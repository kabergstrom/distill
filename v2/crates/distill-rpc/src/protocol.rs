use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

pub use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};
pub use distill_store::state::{InputVersion, SnapshotStamp, StoreInstanceId};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GameModuleEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetDefinitionHash(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LayoutEntry {
    pub type_uuid: TypeUuid,
    pub layout_digest: [u8; 32],
}

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
    pub layout_registry: Vec<LayoutEntry>,
    pub layout_aggregate: [u8; 32],
    pub load_policy: Vec<LoadPolicyEntry>,
    pub policy_digest: [u8; 32],
    pub protocol: u32,
}

impl ConnectRequest {
    pub fn canonical(
        epoch: GameModuleEpoch,
        target: impl Into<String>,
        target_definition_hash: TargetDefinitionHash,
        mut layout_registry: Vec<LayoutEntry>,
        mut load_policy: Vec<LoadPolicyEntry>,
    ) -> Result<Self, crate::AttestationShapeError> {
        layout_registry.sort_by_key(|row| row.type_uuid);
        load_policy.sort_by_key(|row| row.type_uuid);
        let layout_aggregate = crate::compute_layout_aggregate(&layout_registry)?;
        let policy_digest = crate::compute_policy_digest(&load_policy)?;
        crate::attestation::validate_attestation_shape(
            &layout_registry,
            layout_aggregate,
            &load_policy,
            policy_digest,
        )?;
        Ok(Self {
            epoch,
            target: target.into(),
            target_definition_hash,
            layout_registry,
            layout_aggregate,
            load_policy,
            policy_digest,
            protocol: PROTOCOL_VERSION,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReattestRequest {
    pub epoch: GameModuleEpoch,
    pub target_definition_hash: TargetDefinitionHash,
    pub layout_registry: Vec<LayoutEntry>,
    pub layout_aggregate: [u8; 32],
    pub load_policy: Vec<LoadPolicyEntry>,
    pub policy_digest: [u8; 32],
}

impl From<ConnectRequest> for ReattestRequest {
    fn from(request: ConnectRequest) -> Self {
        Self {
            epoch: request.epoch,
            target_definition_hash: request.target_definition_hash,
            layout_registry: request.layout_registry,
            layout_aggregate: request.layout_aggregate,
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
    MissingLayout {
        type_uuid: TypeUuid,
    },
    LayoutMismatch {
        type_uuid: TypeUuid,
        expected: [u8; 32],
        got: [u8; 32],
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

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ConnectError {}

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
}

impl PartialEq for Connected {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance && self.hub.connection_id() == other.hub.connection_id()
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcBasis {
    pub snapshot: SnapshotStamp,
    pub load_policy: Arc<LoadPolicyAttestation>,
    pub policy_generation: u64,
    pub target_generation: u64,
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
    pub paths: Vec<PathMutation>,
    pub configuration: Option<ConfigurationStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    UnknownTarget { target: String },
    InvalidTargetAttestation(crate::AttestationShapeError),
    DuplicateAssetMutation { uuid: AssetUuid },
    DuplicatePathMutation { path: String },
    InvalidPath { path: String },
    EmptyPathCandidates { path: String },
    ArtifactAlreadyExistsWithDifferentPayload { hash: ContentHash },
}
