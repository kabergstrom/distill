use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

pub use distill_core::attestation::{
    CompiledAttestationDigest, CompiledTypeRow, CompiledTypeTable, RegistryExtrasDigest,
    RegistryExtrasV1,
};
pub use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};
pub use distill_store::state::{InputVersion, SnapshotStamp, StoreInstanceId};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GameModuleEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetDefinitionHash(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LoadPolicyEntry {
    pub type_uuid: TypeUuid,
    pub build_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadPolicyAttestation {
    pub rows: Vec<LoadPolicyEntry>,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub epoch: GameModuleEpoch,
    pub target: String,
    pub target_definition_hash: TargetDefinitionHash,
    pub compiled_registry: Vec<CompiledTypeRow>,
    pub dsca: CompiledAttestationDigest,
    pub load_policy: Vec<LoadPolicyEntry>,
    pub policy_digest: [u8; 32],
    pub protocol: u32,
}

impl ConnectRequest {
    pub fn canonical(
        epoch: GameModuleEpoch,
        target: impl Into<String>,
        target_definition_hash: TargetDefinitionHash,
        compiled_registry: Vec<CompiledTypeRow>,
        mut load_policy: Vec<LoadPolicyEntry>,
    ) -> Result<Self, crate::AttestationShapeError> {
        load_policy.sort_by_key(|row| row.type_uuid);
        let compiled = CompiledTypeTable::canonical(compiled_registry)?;
        let policy_digest = crate::compute_policy_digest(&load_policy)?;
        crate::attestation::validate_attestation_shape(
            &compiled.rows,
            compiled.digest,
            &load_policy,
            policy_digest,
        )?;
        Ok(Self {
            epoch,
            target: target.into(),
            target_definition_hash,
            compiled_registry: compiled.rows,
            dsca: compiled.digest,
            load_policy,
            policy_digest,
            protocol: PROTOCOL_VERSION,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReattestRequest {
    pub epoch: GameModuleEpoch,
    pub base_attestation_generation: u64,
    pub successor_attestation_generation: u64,
    pub target_definition_hash: TargetDefinitionHash,
    pub compiled_registry: Vec<CompiledTypeRow>,
    pub dsca: CompiledAttestationDigest,
    pub load_policy: Vec<LoadPolicyEntry>,
    pub policy_digest: [u8; 32],
}

impl From<ConnectRequest> for ReattestRequest {
    fn from(request: ConnectRequest) -> Self {
        Self {
            epoch: request.epoch,
            base_attestation_generation: 0,
            successor_attestation_generation: 1,
            target_definition_hash: request.target_definition_hash,
            compiled_registry: request.compiled_registry,
            dsca: request.dsca,
            load_policy: request.load_policy,
            policy_digest: request.policy_digest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectError {
    ProtocolMismatch {
        expected: u32,
        got: u32,
    },
    UnknownTarget {
        target: String,
    },
    TargetDefinitionMismatch {
        expected: TargetDefinitionHash,
        got: TargetDefinitionHash,
    },
    AttestationShape(crate::AttestationShapeError),
    MissingCompiledType {
        type_uuid: TypeUuid,
    },
    CompiledTypeMismatch {
        type_uuid: TypeUuid,
    },
    MissingLoadPolicy {
        type_uuid: TypeUuid,
    },
    LoadPolicyMismatch {
        type_uuid: TypeUuid,
        expected: bool,
        got: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttestationFailureCode(pub u16);

impl AttestationFailureCode {
    pub const UNKNOWN_TARGET: Self = Self(0x0200);
    pub const TARGET_DEFINITION_MISMATCH: Self = Self(0x0201);
    pub const COMPILED_REGISTRY_INVALID: Self = Self(0x0202);
    pub const DSCA_AGGREGATE_MISMATCH: Self = Self(0x0203);
    pub const MISSING_COMPILED_TYPE: Self = Self(0x0204);
    pub const COMPILED_TYPE_MISMATCH: Self = Self(0x0205);
    pub const POLICY_PROJECTION_INVALID: Self = Self(0x0206);
    pub const MISSING_LOAD_POLICY: Self = Self(0x0207);
    pub const LOAD_POLICY_MISMATCH: Self = Self(0x0208);
}

/// Typed carrier for what part of the complete attestation failed. No UUID
/// sentinel is used for aggregate/table failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestationSubject {
    SpecificType(TypeUuid),
    TargetDefinition(TargetDefinitionFailureSubject),
    CompiledRegistry,
    DscaAggregate,
    PolicyProjection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetDefinitionFailureSubject {
    UnknownTarget(String),
    DigestMismatch {
        expected: TargetDefinitionHash,
        observed: TargetDefinitionHash,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestationFailure {
    pub code: AttestationFailureCode,
    pub subject: AttestationSubject,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReattestSuccess {
    /// The only legal source for the successor RPC basis generation.
    pub installed_attestation_generation: u64,
}

pub const STALE_ATTESTATION_BASE_CODE: u16 = 0x0100;
pub const ATTESTATION_GENERATION_OVERFLOW_CODE: u16 = 0x0101;
pub const INVALID_ATTESTATION_SUCCESSOR_CODE: u16 = 0x0102;
pub const EPOCH_NOT_SUCCESSOR_CODE: u16 = 0x0103;

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ConnectError {}

impl ConnectError {
    /// Stable connect/reattest wire classification shared by both result
    /// unions. Presentation text is deliberately not the type carrier.
    pub fn attestation_failure(&self) -> AttestationFailure {
        use distill_core::attestation::AttestationError;

        let (code, subject) = match self {
            Self::ProtocolMismatch { .. } => {
                unreachable!("protocol mismatch has its own ConnectResult arm")
            }
            Self::UnknownTarget { target } => (
                AttestationFailureCode::UNKNOWN_TARGET,
                AttestationSubject::TargetDefinition(
                    TargetDefinitionFailureSubject::UnknownTarget(target.clone()),
                ),
            ),
            Self::TargetDefinitionMismatch { expected, got } => (
                AttestationFailureCode::TARGET_DEFINITION_MISMATCH,
                AttestationSubject::TargetDefinition(
                    TargetDefinitionFailureSubject::DigestMismatch {
                        expected: *expected,
                        observed: *got,
                    },
                ),
            ),
            Self::AttestationShape(crate::AttestationShapeError::Compiled(
                AttestationError::CompiledDigestMismatch,
            )) => (
                AttestationFailureCode::DSCA_AGGREGATE_MISMATCH,
                AttestationSubject::DscaAggregate,
            ),
            Self::AttestationShape(crate::AttestationShapeError::Compiled(_)) => (
                AttestationFailureCode::COMPILED_REGISTRY_INVALID,
                AttestationSubject::CompiledRegistry,
            ),
            Self::AttestationShape(crate::AttestationShapeError::DuplicateTarget { target }) => (
                AttestationFailureCode::UNKNOWN_TARGET,
                AttestationSubject::TargetDefinition(
                    TargetDefinitionFailureSubject::UnknownTarget(target.clone()),
                ),
            ),
            Self::AttestationShape(_) => (
                AttestationFailureCode::POLICY_PROJECTION_INVALID,
                AttestationSubject::PolicyProjection,
            ),
            Self::MissingCompiledType { type_uuid } => (
                AttestationFailureCode::MISSING_COMPILED_TYPE,
                AttestationSubject::SpecificType(*type_uuid),
            ),
            Self::CompiledTypeMismatch { type_uuid } => (
                AttestationFailureCode::COMPILED_TYPE_MISMATCH,
                AttestationSubject::SpecificType(*type_uuid),
            ),
            Self::MissingLoadPolicy { type_uuid } => (
                AttestationFailureCode::MISSING_LOAD_POLICY,
                AttestationSubject::SpecificType(*type_uuid),
            ),
            Self::LoadPolicyMismatch { type_uuid, .. } => (
                AttestationFailureCode::LOAD_POLICY_MISMATCH,
                AttestationSubject::SpecificType(*type_uuid),
            ),
        };
        AttestationFailure {
            code,
            subject,
            message: format!("{self:?}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectOutcome {
    Connected(Connected),
    ConfigurationPoisoned(ConfigurationPoison),
    Rejected(ConnectError),
}

#[derive(Debug, Clone)]
pub struct Connected {
    pub hub: crate::Hub,
    pub instance: StoreInstanceId,
    pub policy_generation: u64,
    pub target_generation: u64,
    pub attestation_generation: u64,
}

impl PartialEq for Connected {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
            && self.policy_generation == other.policy_generation
            && self.target_generation == other.target_generation
            && self.attestation_generation == other.attestation_generation
            && self.hub.connection_id() == other.hub.connection_id()
    }
}

impl Eq for Connected {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConfigurationPoisonCode(pub u16);

impl ConfigurationPoisonCode {
    pub const INVALID_CANDIDATE: Self = Self(1);
    pub const NON_LOOPBACK_ADDRESS: Self = Self(2);
    pub const INVALID_TARGET: Self = Self(3);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationPoison {
    pub code: ConfigurationPoisonCode,
    pub reason_hash: [u8; 32],
    pub message: String,
}

impl ConfigurationPoison {
    /// `reason_key` is stable diagnostic identity; the human message may be
    /// edited without changing the fingerprint.
    pub fn new(
        code: ConfigurationPoisonCode,
        reason_key: impl AsRef<[u8]>,
        message: impl Into<String>,
    ) -> Self {
        let key = reason_key.as_ref();
        let reason_hash =
            distill_core::canonical::domain_digest(distill_core::canonical::DSCP, 1, |encoder| {
                encoder.u16(code.0);
                encoder.raw(
                    &u32::try_from(key.len())
                        .expect("configuration poison reason key exceeds u32")
                        .to_le_bytes(),
                );
                encoder.raw(key);
            });
        Self {
            code,
            reason_hash,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationStatus {
    Ready,
    Poisoned(ConfigurationPoison),
}

/// R22/H4 terminology alias. The Cap'n Proto declaration calls the wire
/// struct `ConfigurationStatus`; both names denote the same snapshot-pinned
/// Ready/Poisoned state.
pub type ConfigurationState = ConfigurationStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectReason {
    TargetDefinitionChanged,
    LoadPolicyChanged,
    StoreInstanceChanged,
    ProtocolEpochChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcFailure {
    LeaseExpired,
    InvalidCursor {
        since: InputVersion,
        current: InputVersion,
    },
    ArtifactNotFound {
        hash: ContentHash,
    },
    ForeignSnapshot,
    ClientEpochChanged {
        snapshot: GameModuleEpoch,
        current: GameModuleEpoch,
    },
    InvalidPath {
        path: String,
    },
    EpochNotSuccessor {
        previous: GameModuleEpoch,
        proposed: GameModuleEpoch,
    },
    StaleAttestationBase {
        expected: u64,
        got: u64,
    },
    InvalidAttestationSuccessor {
        base: u64,
        successor: u64,
    },
    AttestationGenerationOverflow {
        base: u64,
    },
    Attestation(ConnectError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcResult<T> {
    Success(T),
    ReconnectRequired { reason: ReconnectReason },
    ConfigurationPoisoned(ConfigurationPoison),
    Failure(RpcFailure),
}

impl<T> RpcResult<T> {
    pub fn success(self) -> Option<T> {
        match self {
            Self::Success(value) => Some(value),
            _ => None,
        }
    }

    pub fn map_success<U>(self, map: impl FnOnce(T) -> U) -> RpcResult<U> {
        match self {
            Self::Success(value) => RpcResult::Success(map(value)),
            Self::ReconnectRequired { reason } => RpcResult::ReconnectRequired { reason },
            Self::ConfigurationPoisoned(poison) => RpcResult::ConfigurationPoisoned(poison),
            Self::Failure(error) => RpcResult::Failure(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcBasis {
    pub snapshot: SnapshotStamp,
    pub load_policy: Arc<LoadPolicyAttestation>,
    pub policy_generation: u64,
    pub target_generation: u64,
    pub attestation_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalEvent<T> {
    pub basis: RpcBasis,
    pub value: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftedInput {
    File(String),
    Asset(AssetUuid),
    Query(String),
    Dylib,
    Tool(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveResult {
    Built {
        content_hash: ContentHash,
    },
    Drifted {
        input: DriftedInput,
        current: SnapshotStamp,
    },
    Failed {
        error: String,
    },
    Missing,
    Deleted {
        at: SnapshotStamp,
    },
    /// The UUID exists, but its authored row is tooling-only and must never
    /// enter the runtime build/load path.
    RoleIneligible {
        observed: AuthoringEntryRole,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredResolve {
    Built { content_hash: ContentHash },
    Drifted { input: DriftedInput },
    Failed { error: String },
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathResolveResult {
    Resolved(AssetUuid),
    Missing,
    Failed(PathResolveFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathResolveFailure {
    Ambiguous { candidates: Vec<AssetUuid> },
}

/// Closed query subset exposed by the pinned authoring RPC capability.
/// `None` is the explicitly tooling-only whole-tree enumeration; ordinary
/// runtime query surfaces do not accept that form.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AssetQuery {
    pub uuid: Option<AssetUuid>,
    pub bundle_path: Option<String>,
    pub local_id: Option<String>,
    pub bundle_uuid: Option<BundleUuid>,
    pub authored_type: Option<TypeUuid>,
    pub terminal_type: Option<TypeUuid>,
    pub tag: Option<TagSelector>,
    pub path_prefix: Option<String>,
    pub path_glob: Option<String>,
    pub authoring_only: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagSelector {
    pub tag: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoringEntryRole {
    Runtime,
    AuthoringOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringValue {
    /// Canonical authored-value bytes. This branded carrier is deliberately
    /// not a `ContentHash` and cannot be fetched as an artifact.
    pub canonical_value: Arc<[u8]>,
    pub blobs: Vec<Arc<[u8]>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringEntry {
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub type_uuid: TypeUuid,
    pub terminal_type: TypeUuid,
    pub schema_hash: LogicalHash,
    pub role: AuthoringEntryRole,
    pub tags: BTreeMap<String, Option<String>>,
    pub value: AuthoringValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringInspection {
    pub stamp: SnapshotStamp,
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub type_uuid: TypeUuid,
    pub schema_hash: LogicalHash,
    pub role: AuthoringEntryRole,
    pub value: AuthoringValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringInspectResult {
    Inspection(AuthoringInspection),
    Missing,
    RoleIneligible { observed: AuthoringEntryRole },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPayload {
    pub structural: Arc<[u8]>,
    pub blobs: Vec<Arc<[u8]>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactChunkKind {
    Structural,
    Blob { index: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactChunk {
    pub kind: ArtifactChunkKind,
    pub offset: u64,
    pub bytes: Arc<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkStream {
    pub(crate) chunks: std::collections::VecDeque<ArtifactChunk>,
}

impl ChunkStream {
    pub fn next_chunk(&mut self) -> Option<ArtifactChunk> {
        self.chunks.pop_front()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetDeltaState {
    Changed,
    Deleted,
    Restored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub basis: RpcBasis,
    pub assets: Vec<(AssetUuid, AssetDeltaState)>,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetEvent {
    Created {
        uuid: AssetUuid,
        bundle: BundleUuid,
        local_id: String,
        type_uuid: TypeUuid,
    },
    Modified {
        uuid: AssetUuid,
        bundle: BundleUuid,
        local_id: String,
    },
    Deleted {
        uuid: AssetUuid,
        bundle: BundleUuid,
        local_id: String,
    },
    Invalidated {
        uuid: AssetUuid,
        version: InputVersion,
    },
    Built {
        uuid: AssetUuid,
        artifact: ContentHash,
        version: InputVersion,
    },
    MigrationPending {
        uuid: AssetUuid,
        from_hash: LogicalHash,
        to_hash: LogicalHash,
    },
    MigrationApplied {
        uuid: AssetUuid,
    },
    Error {
        uuid: Option<AssetUuid>,
        message: String,
    },
    RestartRequired {
        keys: Vec<String>,
    },
    ReconnectRequired {
        reason: ReconnectReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    InitialDelta {
        basis: RpcBasis,
        since: InputVersion,
        installed: InputVersion,
        deltas: Vec<Delta>,
    },
    Delta(Delta),
    ResyncRequired {
        basis: RpcBasis,
        oldest_available: InputVersion,
    },
    Asset {
        basis: RpcBasis,
        event: AssetEvent,
    },
}

impl StreamEvent {
    /// Every message on the stream is basis-tagged, including empty initial
    /// deltas, resync markers, restart prompts, and reconnect prompts.
    pub fn basis(&self) -> &RpcBasis {
        match self {
            Self::InitialDelta { basis, .. }
            | Self::ResyncRequired { basis, .. }
            | Self::Asset { basis, .. } => basis,
            Self::Delta(delta) => &delta.basis,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionInstall {
    pub deltas: crate::DeltaStream,
    pub installed: InputVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetMutation {
    Set {
        uuid: AssetUuid,
        resolution: StoredResolve,
        delta: AssetDeltaState,
    },
    Remove {
        uuid: AssetUuid,
        delta: AssetDeltaState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringMutation {
    Set(AuthoringEntry),
    Remove { uuid: AssetUuid },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathMutation {
    Set {
        path: String,
        candidates: BTreeSet<AssetUuid>,
    },
    Remove {
        path: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Commit {
    pub assets: Vec<AssetMutation>,
    pub authoring: Vec<AuthoringMutation>,
    pub paths: Vec<PathMutation>,
    pub configuration: Option<ConfigurationStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    UnknownTarget { target: String },
    InvalidTargetAttestation(crate::AttestationShapeError),
    DuplicateAssetMutation { uuid: AssetUuid },
    DuplicateAuthoringMutation { uuid: AssetUuid },
    DuplicatePathMutation { path: String },
    InvalidPath { path: String },
    EmptyPathCandidates { path: String },
    ArtifactAlreadyExistsWithDifferentPayload { hash: ContentHash },
}
