use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub use distill_core::id::{
    AssetUuid, BundleFileHash, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid,
};
pub use distill_store::state::{
    AssetClaimant, CleanupDisposition, ConfigurationPoison, ConfigurationPoisonCode, DscpV1,
    GlobalBundleReadFailureCode, InputVersion, LineageManifestClaimant, PhysicalPathClaim,
    PhysicalPathFailureCode, PipelineCandidateIdentity, PipelinePoison, PipelinePoisonCode,
    PipelinePoisonOrigin, PlatformPathBytes, ReadableBundleSource, ScanFailureCode, ScanSubject,
    SchemaAcceptanceRequired, SchemaManifestBasis, SchemaRegistryMismatch, SkeletonFailureCode,
    SnapshotStamp, StoreInstanceId, VersionPoison, VersionPoisonCode, VersionPoisonV1,
};
pub use distill_store::RetiredTypeReference;

pub const PROTOCOL_VERSION: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetDefinitionHash(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub target: String,
    pub target_definition_hash: TargetDefinitionHash,
    pub protocol: u32,
}

impl ConnectRequest {
    pub fn new(target: impl Into<String>, target_definition_hash: TargetDefinitionHash) -> Self {
        Self {
            target: target.into(),
            target_definition_hash,
            protocol: PROTOCOL_VERSION,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectOutcome {
    Connected(Connected),
    ConfigurationPoisoned(ConfigurationPoison),
    PipelineUnavailable(PipelineUnavailableDiagnostic),
    Rejected(ConnectError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineUnavailableDiagnostic {
    PipelinePoison(PipelinePoison),
    SchemaAcceptanceRequired(SchemaAcceptanceRequired),
    RetiredTypeReferenced(RetiredTypeReferenced),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataConnectOutcome {
    Connected(MetadataConnected),
    ProtocolMismatch { expected: u32, observed: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OccupiedLineageDestinationKind {
    CanonicalBundle,
    Opaque,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairDestination {
    Absent,
    Occupied {
        file_hash: BundleFileHash,
        kind: OccupiedLineageDestinationKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairState {
    Missing {
        configured_root: String,
        configured_path: String,
        destination: LineageRepairDestination,
    },
    Duplicate {
        claimants: Vec<LineageManifestClaimant>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageRepairInspection {
    pub instance: StoreInstanceId,
    pub stamp: SnapshotStamp,
    pub state: LineageRepairState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairUnavailable {
    ConfigurationReady,
    OtherConfigurationPoison(ConfigurationPoison),
}

#[derive(Debug, Clone)]
pub struct LineageRepairConnected {
    pub repair: crate::LineageRepair,
    pub instance: StoreInstanceId,
    pub protocol_epoch: u32,
}

#[derive(Debug, Clone)]
pub enum LineageRepairConnectOutcome {
    Connected(LineageRepairConnected),
    Unavailable(LineageRepairUnavailable),
    ProtocolMismatch { expected: u32, observed: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageRepairInvalidCode {
    WrongBasisState,
    NonCanonicalBundle,
    MissingManifestEntry,
    NotAuthoringOnly,
    BootstrapTypePresent,
    InvalidLineage,
    SurvivorNotClaimant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageRepairInvalid {
    pub code: LineageRepairInvalidCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageRepairStaleCode {
    StampChanged,
    StateChanged,
    DestinationAppeared,
    ClaimantChanged,
    PreimageChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageRepairStale {
    pub code: LineageRepairStaleCode,
    pub observed_stamp: SnapshotStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineageRepairCommitted {
    pub stamp: SnapshotStamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairInspectOutcome {
    Success(LineageRepairInspection),
    Unavailable(LineageRepairUnavailable),
    ReconnectRequired { reason: MetadataReconnectReason },
    Failure(RpcFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairMutationOutcome {
    Success(LineageRepairCommitted),
    StaleBasis(LineageRepairStale),
    Invalid(LineageRepairInvalid),
    Unavailable(LineageRepairUnavailable),
    ReconnectRequired { reason: MetadataReconnectReason },
    Failure(RpcFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairBackendError {
    /// Descriptor-relative destination/claimant/preimage drift observed by the
    /// durable backend after the RPC coordinator's in-memory basis CAS.
    Stale(LineageRepairStale),
    Invalid(LineageRepairInvalid),
    Failure(RpcFailure),
}

impl MetadataConnectOutcome {
    pub fn connected(self) -> Option<MetadataConnected> {
        match self {
            Self::Connected(connected) => Some(connected),
            Self::ProtocolMismatch { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetadataConnected {
    pub hub: crate::MetadataHub,
    pub instance: StoreInstanceId,
    pub protocol_epoch: u32,
}

impl PartialEq for MetadataConnected {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
            && self.protocol_epoch == other.protocol_epoch
            && self.hub.connection_id() == other.hub.connection_id()
    }
}

impl Eq for MetadataConnected {}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationStatus {
    Ready,
    Poisoned(ConfigurationPoison),
}

/// Closed, poison-safe pipeline diagnostic using shared typed §13 records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineDiagnostic {
    Ready,
    Poisoned(PipelinePoison),
    SchemaAcceptanceRequired(SchemaAcceptanceRequired),
    RetiredTypeReferenced(RetiredTypeReferenced),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredTypeReferenced {
    pub manifest_hash: BundleFileHash,
    pub basis: SnapshotStamp,
    pub type_uuid: TypeUuid,
    pub references: Vec<RetiredTypeReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataDiagnostics {
    pub stamp: SnapshotStamp,
    pub configuration: ConfigurationStatus,
    pub pipeline: PipelineDiagnostic,
    pub version_poison: Option<VersionPoison>,
}

/// R22/H4 terminology alias. The Cap'n Proto declaration calls the wire
/// struct `ConfigurationStatus`; both names denote the same snapshot-pinned
/// Ready/Poisoned state.
pub type ConfigurationState = ConfigurationStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectReason {
    TargetDefinitionChanged,
    StoreInstanceChanged,
    ProtocolEpochChanged,
    PipelineEpochChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataReconnectReason {
    StoreInstanceChanged,
    ProtocolEpochChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcFailure {
    LeaseExpired,
    PipelineUnavailable(Box<PipelineUnavailableDiagnostic>),
    BuildDepthExceeded {
        limit: usize,
        chain: Vec<AssetUuid>,
    },
    InvalidCursor {
        since: InputVersion,
        current: InputVersion,
    },
    ResourceLimit {
        resource: String,
        limit: usize,
    },
    ArtifactNotFound {
        hash: ContentHash,
    },
    AssetNotFound {
        uuid: AssetUuid,
    },
    ForeignSnapshot,
    InvalidPath {
        path: String,
    },
    InvalidQuery {
        detail: String,
    },
    TagIndexPoisoned {
        bundles: Vec<BundleUuid>,
    },
    StaleInputVersion {
        expected: InputVersion,
        got: InputVersion,
    },
    InvalidAuthoringRequest {
        detail: String,
    },
    AuthoringBackendUnavailable {
        operation: String,
    },
    WireTreeNotFound {
        hash: LayoutHash,
    },
}

/// Exact common result grammar for the unbound metadata capability family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCall<T> {
    Success(T),
    ReconnectRequired { reason: MetadataReconnectReason },
    LeaseFailure,
    Error(RpcFailure),
}

impl<T> MetadataCall<T> {
    pub fn success(self) -> Option<T> {
        match self {
            Self::Success(value) => Some(value),
            _ => None,
        }
    }

    pub fn map_success<U>(self, map: impl FnOnce(T) -> U) -> MetadataCall<U> {
        match self {
            Self::Success(value) => MetadataCall::Success(map(value)),
            Self::ReconnectRequired { reason } => MetadataCall::ReconnectRequired { reason },
            Self::LeaseFailure => MetadataCall::LeaseFailure,
            Self::Error(error) => MetadataCall::Error(error),
        }
    }
}

/// Namespace-facing metadata result grammar; version poison is a value arm,
/// never a stringly RPC error or a bootstrap failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataNamespaceCall<T> {
    Success(T),
    ReconnectRequired { reason: MetadataReconnectReason },
    LeaseFailure,
    Error(RpcFailure),
    VersionPoisoned(VersionPoison),
}

impl<T> MetadataNamespaceCall<T> {
    pub fn success(self) -> Option<T> {
        match self {
            Self::Success(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcResult<T> {
    Success(T),
    ReconnectRequired { reason: ReconnectReason },
    ConfigurationPoisoned(ConfigurationPoison),
    VersionPoisoned(VersionPoison),
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
            Self::VersionPoisoned(poison) => RpcResult::VersionPoisoned(poison),
            Self::Failure(error) => RpcResult::Failure(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcBasis {
    pub snapshot: SnapshotStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataBasis {
    pub snapshot: SnapshotStamp,
    pub protocol_epoch: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEvent<T> {
    pub basis: MetadataBasis,
    pub value: T,
}

/// The exact selector subset available without a pipeline/target authority.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PureMetadataQuery {
    pub uuid: Option<AssetUuid>,
    pub bundle: Option<BundleUuid>,
    pub normalized_path_prefix: Option<String>,
    pub authored_type: Option<TypeUuid>,
    pub role: Option<AuthoringEntryRole>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEntry {
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub schema_hash: LogicalHash,
    pub role: AuthoringEntryRole,
    pub tags: BTreeMap<String, Option<String>>,
}

/// Target/pipeline-independent entry projection exposed by `Root.metadata`.
/// It deliberately omits terminal type and extracted tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PureMetadataEntry {
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub authored_type: TypeUuid,
    pub schema_hash: LogicalHash,
    pub role: AuthoringEntryRole,
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

/// Closed authoring mutation grammar accepted by `Hub.write`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum AuthoringOp {
    Set(AuthoringEntry),
    Remove { uuid: AssetUuid },
}

/// Explicit importer request. Settings retain their canonical authored-value
/// bytes; the RPC layer never sniffs sources or reinterprets this payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportRequest {
    pub importer: String,
    pub sources: Vec<String>,
    pub dest: String,
    pub settings: AuthoringValue,
    pub watch: bool,
    pub root: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LongRunningOp {
    RenameWithFixups(Arc<[u8]>),
    DiskMigration(Arc<[u8]>),
    Doctor(Arc<[u8]>),
    SchemaTransition(Arc<[u8]>),
}

/// Canonical request carried by [`LongRunningOp::RenameWithFixups`].  The
/// bundle UUID identifies the inode to move; the destination remains rooted so
/// two configured roots can never be selected by ambient path lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameWithFixupsRequest {
    pub bundle: BundleUuid,
    pub destination_root: String,
    pub destination_path: String,
}

/// Canonical request carried by [`LongRunningOp::DiskMigration`].  Empty means
/// every authored bundle with a pending forward migration; otherwise the list
/// is raw-UUID sorted and unique.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskMigrationRequest {
    pub bundles: Vec<BundleUuid>,
}

/// Closed maintenance request carried by [`LongRunningOp::Doctor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorRequest {
    Verify,
    /// Explicitly remove every currently retained displaced inode while
    /// preserving its audit row. This is distinct from the normal retention
    /// sweep.
    Clean,
    RebuildIndexes,
}

/// Canonical request carried by [`LongRunningOp::SchemaTransition`]. Every
/// action repeats the complete manifest and pending-candidate basis reported
/// by [`SchemaAcceptanceRequired`], so a command cannot be consumed by a
/// different manifest revision or staged module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaTransitionRequest {
    pub manifest: SchemaManifestBasis,
    pub candidate: PipelineCandidateIdentity,
    pub type_uuid: TypeUuid,
    pub action: SchemaTransitionAction,
}

/// The four explicit schema-authority transitions. Ordinary acceptance and
/// rollback name the exact candidate digest to select. Retirement additionally
/// carries the metadata/control snapshot that established its negative proof;
/// reactivation selects the named pending candidate's row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTransitionAction {
    Accept { requested: LogicalHash },
    Rollback { target: LogicalHash },
    Retire { control_basis: SnapshotStamp },
    Reactivate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationPayloadError {
    Truncated,
    TrailingBytes,
    UnsupportedVersion(u8),
    InvalidTag(u8),
    InvalidUtf8,
    CountOverflow,
    NonCanonicalOrder,
}

impl std::fmt::Display for OperationPayloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "long-running operation payload: {self:?}")
    }
}

impl std::error::Error for OperationPayloadError {}

impl RenameWithFixupsRequest {
    pub fn encode(&self) -> Arc<[u8]> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&self.bundle.0);
        encode_operation_text(&mut bytes, &self.destination_root);
        encode_operation_text(&mut bytes, &self.destination_path);
        Arc::from(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OperationPayloadError> {
        let mut reader = OperationPayloadReader::new(bytes)?;
        let request = Self {
            bundle: BundleUuid(reader.array()?),
            destination_root: reader.text()?,
            destination_path: reader.text()?,
        };
        reader.finish()?;
        Ok(request)
    }
}

impl DiskMigrationRequest {
    pub fn encode(&self) -> Result<Arc<[u8]>, OperationPayloadError> {
        if !self.bundles.windows(2).all(|pair| pair[0].0 < pair[1].0) {
            return Err(OperationPayloadError::NonCanonicalOrder);
        }
        let count =
            u32::try_from(self.bundles.len()).map_err(|_| OperationPayloadError::CountOverflow)?;
        let mut bytes = vec![1];
        bytes.extend_from_slice(&count.to_le_bytes());
        for bundle in &self.bundles {
            bytes.extend_from_slice(&bundle.0);
        }
        Ok(Arc::from(bytes))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OperationPayloadError> {
        let mut reader = OperationPayloadReader::new(bytes)?;
        let count =
            usize::try_from(reader.u32()?).map_err(|_| OperationPayloadError::CountOverflow)?;
        if count > reader.remaining() / 16 {
            return Err(OperationPayloadError::CountOverflow);
        }
        let mut bundles = Vec::with_capacity(count);
        for _ in 0..count {
            bundles.push(BundleUuid(reader.array()?));
        }
        reader.finish()?;
        if !bundles.windows(2).all(|pair| pair[0].0 < pair[1].0) {
            return Err(OperationPayloadError::NonCanonicalOrder);
        }
        Ok(Self { bundles })
    }
}

impl DoctorRequest {
    pub fn encode(self) -> Arc<[u8]> {
        Arc::from([1, self.tag()])
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OperationPayloadError> {
        let mut reader = OperationPayloadReader::new(bytes)?;
        let request = match reader.u8()? {
            1 => Self::Verify,
            2 => Self::Clean,
            3 => Self::RebuildIndexes,
            tag => return Err(OperationPayloadError::InvalidTag(tag)),
        };
        reader.finish()?;
        Ok(request)
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Verify => 1,
            Self::Clean => 2,
            Self::RebuildIndexes => 3,
        }
    }
}

impl SchemaTransitionRequest {
    pub fn encode(&self) -> Result<Arc<[u8]>, OperationPayloadError> {
        distill_core::target_set::CanonicalTargetSet::from_canonical(
            self.candidate.target_set.rows.clone(),
        )
        .map_err(|_| OperationPayloadError::NonCanonicalOrder)?;
        let cursor_count = u32::try_from(self.manifest.current_cursors.len())
            .map_err(|_| OperationPayloadError::CountOverflow)?;
        let target_count = u32::try_from(self.candidate.target_set.rows.len())
            .map_err(|_| OperationPayloadError::CountOverflow)?;

        let mut bytes = vec![1, self.action.tag()];
        bytes.extend_from_slice(&self.manifest.manifest_hash.0);
        bytes.extend_from_slice(&cursor_count.to_le_bytes());
        for (type_uuid, logical_hash) in &self.manifest.current_cursors {
            bytes.extend_from_slice(&type_uuid.0);
            bytes.extend_from_slice(&logical_hash.0);
        }
        bytes.extend_from_slice(&self.candidate.dylib_hash);
        bytes.extend_from_slice(&target_count.to_le_bytes());
        for target in &self.candidate.target_set.rows {
            encode_operation_text_checked(&mut bytes, &target.name)?;
            bytes.extend_from_slice(&target.target_definition_hash);
        }
        bytes.extend_from_slice(&self.type_uuid.0);
        match self.action {
            SchemaTransitionAction::Accept { requested } => bytes.extend_from_slice(&requested.0),
            SchemaTransitionAction::Rollback { target } => bytes.extend_from_slice(&target.0),
            SchemaTransitionAction::Retire { control_basis } => {
                bytes.extend_from_slice(&control_basis.instance.0);
                bytes.extend_from_slice(&control_basis.version.0.to_le_bytes());
            }
            SchemaTransitionAction::Reactivate => {}
        }
        Ok(Arc::from(bytes))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OperationPayloadError> {
        let mut reader = OperationPayloadReader::new(bytes)?;
        let action_tag = reader.u8()?;
        let manifest_hash = ContentHash(reader.array()?);
        let cursor_count =
            usize::try_from(reader.u32()?).map_err(|_| OperationPayloadError::CountOverflow)?;
        if cursor_count > reader.remaining() / 48 {
            return Err(OperationPayloadError::CountOverflow);
        }
        let mut current_cursors = BTreeMap::new();
        let mut prior_type_uuid = None;
        for _ in 0..cursor_count {
            let type_uuid = TypeUuid(reader.array()?);
            if prior_type_uuid.is_some_and(|prior| prior >= type_uuid) {
                return Err(OperationPayloadError::NonCanonicalOrder);
            }
            let logical_hash = LogicalHash(reader.array()?);
            current_cursors.insert(type_uuid, logical_hash);
            prior_type_uuid = Some(type_uuid);
        }

        let dylib_hash = reader.array()?;
        let target_count =
            usize::try_from(reader.u32()?).map_err(|_| OperationPayloadError::CountOverflow)?;
        if target_count > reader.remaining() / 36 {
            return Err(OperationPayloadError::CountOverflow);
        }
        let mut target_rows = Vec::with_capacity(target_count);
        for _ in 0..target_count {
            target_rows.push(distill_core::target_set::TargetSetRow {
                name: reader.text()?,
                target_definition_hash: reader.array()?,
            });
        }
        let target_set = distill_core::target_set::CanonicalTargetSet::from_canonical(target_rows)
            .map_err(|_| OperationPayloadError::NonCanonicalOrder)?;
        let type_uuid = TypeUuid(reader.array()?);
        let action = match action_tag {
            1 => SchemaTransitionAction::Accept {
                requested: LogicalHash(reader.array()?),
            },
            2 => SchemaTransitionAction::Rollback {
                target: LogicalHash(reader.array()?),
            },
            3 => SchemaTransitionAction::Retire {
                control_basis: SnapshotStamp {
                    instance: StoreInstanceId(reader.array()?),
                    version: InputVersion(reader.u64()?),
                },
            },
            4 => SchemaTransitionAction::Reactivate,
            tag => return Err(OperationPayloadError::InvalidTag(tag)),
        };
        reader.finish()?;
        Ok(Self {
            manifest: SchemaManifestBasis {
                manifest_hash,
                current_cursors,
            },
            candidate: PipelineCandidateIdentity {
                dylib_hash,
                target_set,
            },
            type_uuid,
            action,
        })
    }
}

impl SchemaTransitionAction {
    const fn tag(self) -> u8 {
        match self {
            Self::Accept { .. } => 1,
            Self::Rollback { .. } => 2,
            Self::Retire { .. } => 3,
            Self::Reactivate => 4,
        }
    }
}

fn encode_operation_text(bytes: &mut Vec<u8>, text: &str) {
    let len = u32::try_from(text.len()).expect("operation text exceeds the RPC message limit");
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(text.as_bytes());
}

fn encode_operation_text_checked(
    bytes: &mut Vec<u8>,
    text: &str,
) -> Result<(), OperationPayloadError> {
    let len = u32::try_from(text.len()).map_err(|_| OperationPayloadError::CountOverflow)?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(text.as_bytes());
    Ok(())
}

struct OperationPayloadReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> OperationPayloadReader<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, OperationPayloadError> {
        let Some((&version, _)) = bytes.split_first() else {
            return Err(OperationPayloadError::Truncated);
        };
        if version != 1 {
            return Err(OperationPayloadError::UnsupportedVersion(version));
        }
        Ok(Self { bytes, position: 1 })
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], OperationPayloadError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(OperationPayloadError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(OperationPayloadError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], OperationPayloadError> {
        self.take(N)?
            .try_into()
            .map_err(|_| OperationPayloadError::Truncated)
    }

    fn u8(&mut self) -> Result<u8, OperationPayloadError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, OperationPayloadError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, OperationPayloadError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn text(&mut self) -> Result<String, OperationPayloadError> {
        let len = usize::try_from(self.u32()?).map_err(|_| OperationPayloadError::CountOverflow)?;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| OperationPayloadError::InvalidUtf8)
    }

    fn finish(self) -> Result<(), OperationPayloadError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(OperationPayloadError::TrailingBytes)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoringProgressState {
    Started,
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl AuthoringProgressState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringProgressEvent {
    pub sequence: u64,
    pub state: AuthoringProgressState,
    pub payload: Arc<[u8]>,
}

/// Pure preparation result returned by an injected authoring backend. The RPC
/// server validates and commits this change set atomically against the caller's
/// input-version precondition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedImportCommit {
    pub bundle: BundleUuid,
    pub commit: Commit,
}

pub trait DeferredOperation: Send + Sync {
    /// Execute the already-validated operation against `base`. The RPC server
    /// invokes this only when the client consumes the terminal Completed
    /// event, on the authority thread.
    fn complete(&self, base: InputVersion) -> Result<DeferredOperationResult, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredOperationResult {
    pub commit: Commit,
    /// A per-file operation may publish earlier successful files before a
    /// later conflict. The version must still commit; this terminal diagnostic
    /// changes Completed to Failed only after that commit is visible.
    pub terminal_error: Option<String>,
}

#[derive(Clone)]
pub enum PreparedOperationPublication {
    Immediate(Box<Commit>),
    Deferred(Arc<dyn DeferredOperation>),
}

impl std::fmt::Debug for PreparedOperationPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Immediate(commit) => formatter.debug_tuple("Immediate").field(commit).finish(),
            Self::Deferred(_) => formatter.write_str("Deferred(..)"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedOperationCommit {
    pub publication: PreparedOperationPublication,
    pub progress: Vec<AuthoringProgressEvent>,
}

impl PreparedOperationCommit {
    pub fn immediate(commit: Commit, progress: Vec<AuthoringProgressEvent>) -> Self {
        Self {
            publication: PreparedOperationPublication::Immediate(Box::new(commit)),
            progress,
        }
    }

    pub fn deferred(
        operation: Arc<dyn DeferredOperation>,
        progress: Vec<AuthoringProgressEvent>,
    ) -> Self {
        Self {
            publication: PreparedOperationPublication::Deferred(operation),
            progress,
        }
    }
}

/// The authority-side step of an import run: publish what ran off the
/// authority, or fail.
pub type ImportJob<'a> =
    Box<dyn FnOnce() -> Result<PreparedImportCommit, RpcFailure> + Send + 'a>;

/// Daemon integration seam for workflows that require importer, filesystem,
/// migration, or doctor services. Implementations prepare a side-effect-free
/// commit; publication remains an atomic RPC-server CAS step.
pub trait AuthoringBackend: Send + Sync {
    /// Execute an ordinary authoring batch against `base` and return the
    /// rescan-proven in-memory projection when the backend owns durable
    /// publication. `Ok(None)` retains the in-memory-only implementation used
    /// by embedders and tests that have no filesystem authority.
    ///
    /// The RPC server invokes this on the authority thread;
    /// production implementations must compare their durable store version
    /// with `base`, publish, rescan, and advance that store exactly once before
    /// returning. They must not call back into the [`crate::Server`].
    fn prepare_write(
        &self,
        _base: InputVersion,
        _operations: &[AuthoringOp],
    ) -> Result<Option<Commit>, RpcFailure> {
        Ok(None)
    }

    fn prepare_import(
        &self,
        base: InputVersion,
        request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure>;

    fn prepare_reimport(
        &self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure>;

    /// Run an import off the authority and return the step that publishes
    /// it, which the authority runs while still at `base`. The default
    /// leaves all the work to that step.
    fn run_import<'a>(
        &'a self,
        base: InputVersion,
        request: ImportRequest,
    ) -> Result<ImportJob<'a>, RpcFailure> {
        Ok(Box::new(move || self.prepare_import(base, &request)))
    }

    /// [`AuthoringBackend::run_import`] for a reimport.
    fn run_reimport<'a>(
        &'a self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<ImportJob<'a>, RpcFailure> {
        Ok(Box::new(move || self.prepare_reimport(base, bundle)))
    }

    fn prepare_operation(
        &self,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure>;

    /// Execute the destination-aware durable repair and return its
    /// rescan-proven publication commit. The RPC coordinator invokes this
    /// only after its second exact inspection CAS, on the authority thread,
    /// and installs the returned commit before the authority runs anything
    /// else. Implementations must not publish through this `Server`.
    fn prepare_create_missing_lineage(
        &self,
        _basis: &LineageRepairInspection,
        _canonical_manifest_bundle: &[u8],
    ) -> Result<Commit, LineageRepairBackendError> {
        Err(LineageRepairBackendError::Failure(
            RpcFailure::AuthoringBackendUnavailable {
                operation: "lineageRepair.createMissing".to_owned(),
            },
        ))
    }

    fn prepare_resolve_duplicate_lineage(
        &self,
        _basis: &LineageRepairInspection,
        _survivor: &LineageManifestClaimant,
    ) -> Result<Commit, LineageRepairBackendError> {
        Err(LineageRepairBackendError::Failure(
            RpcFailure::AuthoringBackendUnavailable {
                operation: "lineageRepair.resolveDuplicate".to_owned(),
            },
        ))
    }
}

/// Snapshot-pinned request issued when runtime resolution reaches a drifted
/// asset. Implementations must build only from `entry` and inputs resolved at
/// `basis`; a newer daemon input version is drift, not an implicit rebase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildWorkClass {
    Interactive,
    Batch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildRequest {
    pub work_class: BuildWorkClass,
    pub basis: SnapshotStamp,
    pub target: String,
    pub target_definition: TargetDefinitionHash,
    /// UUID named by the resolver. For a primary this equals `entry.uuid`;
    /// for a derived output it is UUIDv5(parent, output_key).
    pub requested_asset: AssetUuid,
    /// Empty for the primary, otherwise the statically declared extra key.
    pub output_key: String,
    pub requested_terminal_type: TypeUuid,
    pub entry: AuthoringEntry,
    pub drifted_input: DriftedInput,
}

/// Snapshot- and target-bound schema policy lookup used by offline pack
/// construction. `build_only` is intentionally not part of DSLH, so the
/// published project schema authority is the sole source of this fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTypePolicyRequest {
    pub basis: SnapshotStamp,
    pub target: String,
    pub target_definition: TargetDefinitionHash,
    pub type_uuid: TypeUuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeTypePolicy {
    pub build_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildWireTree {
    pub layout_hash: LayoutHash,
    /// Canonical DSWL body bytes (without the CAS domain/version prefix).
    pub bytes: Arc<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildArtifactPublication {
    pub content_hash: ContentHash,
    pub payload: ArtifactPayload,
}

/// Complete content-addressed publication produced by one lazy build. The
/// root artifact must be present in `artifacts`; dependency artifacts and all
/// referenced wire trees may be published in the same atomic visibility step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildPublication {
    pub root_content_hash: ContentHash,
    pub artifacts: Vec<BuildArtifactPublication>,
    pub wire_trees: Vec<BuildWireTree>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildBackendOutcome {
    Built(BuildPublication),
    Failed { error: String },
    Drifted { input: DriftedInput },
}

/// Daemon integration seam for snapshot-pinned, target-specific lazy builds.
/// The RPC server owns single-flight coordination and artifact publication;
/// implementations must not call back into the [`crate::Server`].
pub trait BuildBackend: Send + Sync {
    fn build(&self, request: &BuildRequest) -> Result<BuildBackendOutcome, RpcFailure>;

    fn runtime_type_policy(
        &self,
        _request: &RuntimeTypePolicyRequest,
    ) -> Result<RuntimeTypePolicy, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "runtime type-policy lookup".to_owned(),
        })
    }

    /// Notification that the server has either installed or rejected the
    /// publication returned by [`Self::build`]. A durable backend uses this
    /// boundary to release its in-flight CAS pins and run maintenance only
    /// after the caller's lease has been pinned.
    fn build_finished(&self, _request: &BuildRequest) -> Result<(), RpcFailure> {
        Ok(())
    }
}

/// Storage hook for §13's pin-before-response rule. The RPC crate owns lease
/// lifetime while the daemon owns the durable CAS, so this deliberately small
/// interface is the only coupling between them.
pub trait ArtifactLeaseBackend: Send + Sync {
    fn pin_lease(&self, holder: u64, hashes: &[[u8; 32]]) -> Result<(), String>;
    fn release_lease(&self, holder: u64);

    /// Pins retained only while a bounded, renewable pack-build session is
    /// alive. Backends that do not distinguish durable pin classes can use
    /// the ordinary lease implementation.
    fn pin_pack_session(&self, holder: u64, hashes: &[[u8; 32]]) -> Result<(), String> {
        self.pin_lease(holder, hashes)
    }

    fn release_pack_session(&self, holder: u64) {
        self.release_lease(holder);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetReferenceQuery {
    Uuid(AssetUuid),
    Path {
        normalized_path: String,
        local_id: Option<String>,
    },
    SameBundleLocalId(String),
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
    /// Canonical §5 logical-schema snapshot bytes authenticated by
    /// `schema_hash` before authored-value decoding.
    pub logical_schema: Arc<[u8]>,
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
    pub logical_schema: Arc<[u8]>,
    pub role: AuthoringEntryRole,
    pub value: AuthoringValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Public wire-domain shape mirrors the typed result union.
pub enum AuthoringInspectResult {
    Inspection(AuthoringInspection),
    Missing,
    RoleIneligible { observed: AuthoringEntryRole },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPayload {
    pub structural: Arc<[u8]>,
    pub blobs: Vec<Arc<[u8]>>,
    /// Direct strong-reference edges. The asset UUID set must exactly match
    /// the authenticated artifact header's `load_deps` set.
    pub load_edges: Vec<ServedLoadEdge>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServedLoadEdge {
    pub asset: AssetUuid,
    pub expected_terminal: TypeUuid,
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
    pub(crate) structural: Arc<[u8]>,
    pub(crate) blobs: Vec<Arc<[u8]>>,
    pub(crate) chunk_size: usize,
    pub(crate) section: usize,
    pub(crate) offset: usize,
    pub(crate) total_bytes: u64,
    pub(crate) load_edges: Vec<ServedLoadEdge>,
}

impl ChunkStream {
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn load_edges(&self) -> &[ServedLoadEdge] {
        &self.load_edges
    }
}

pub(crate) trait ProgressCompletion {
    fn complete(&self) -> Result<(), String>;
    fn cancel(&self) -> bool;
}

pub struct ProgressStream {
    pub(crate) events: std::collections::VecDeque<AuthoringProgressEvent>,
    pub(crate) next_sequence: u64,
    pub(crate) terminal_seen: bool,
    pub(crate) completion: std::rc::Rc<dyn ProgressCompletion>,
}

impl std::fmt::Debug for ProgressStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProgressStream")
            .field("events", &self.events)
            .field("next_sequence", &self.next_sequence)
            .field("terminal_seen", &self.terminal_seen)
            .finish_non_exhaustive()
    }
}

impl ProgressStream {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn cancel(&mut self) -> bool {
        if self.terminal_seen || !self.completion.cancel() {
            return false;
        }
        self.events.clear();
        self.events.push_back(AuthoringProgressEvent {
            sequence: self.next_sequence,
            state: AuthoringProgressState::Cancelled,
            payload: Arc::from([]),
        });
        true
    }
}

impl Iterator for ProgressStream {
    type Item = AuthoringProgressEvent;

    fn next(&mut self) -> Option<Self::Item> {
        let mut event = self.events.pop_front()?;
        if event.state == AuthoringProgressState::Completed {
            if let Err(error) = self.completion.complete() {
                event.state = AuthoringProgressState::Failed;
                event.payload = Arc::from(error.into_bytes());
            }
        } else if matches!(
            event.state,
            AuthoringProgressState::Cancelled | AuthoringProgressState::Failed
        ) {
            self.completion.cancel();
        }
        self.next_sequence = event.sequence.saturating_add(1);
        self.terminal_seen = event.state.is_terminal();
        Some(event)
    }
}

impl ChunkStream {
    pub fn next_chunk(&mut self) -> Option<ArtifactChunk> {
        if self.section > self.blobs.len() {
            return None;
        }
        let (kind, bytes) = if self.section == 0 {
            (ArtifactChunkKind::Structural, self.structural.as_ref())
        } else {
            (
                ArtifactChunkKind::Blob {
                    index: (self.section - 1) as u32,
                },
                self.blobs[self.section - 1].as_ref(),
            )
        };
        if bytes.is_empty() {
            self.section += 1;
            self.offset = 0;
            return Some(ArtifactChunk {
                kind,
                offset: 0,
                bytes: Arc::from([]),
            });
        }
        let start = self.offset;
        let end = start.saturating_add(self.chunk_size).min(bytes.len());
        let chunk = Arc::from(&bytes[start..end]);
        self.offset = end;
        if end == bytes.len() {
            self.section += 1;
            self.offset = 0;
        }
        Some(ArtifactChunk {
            kind,
            offset: start as u64,
            bytes: chunk,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.section > self.blobs.len()
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
#[allow(clippy::large_enum_variant)] // Commit grammar keeps Set strongly typed and allocation policy explicit.
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

/// Snapshot-published derived child authority.  These rows come from the
/// authored asset set crossed with the pinned pipeline map; cached build
/// records never create namespace authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedOutputEntry {
    pub parent: AssetUuid,
    pub output_key: String,
    pub terminal_type: TypeUuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivedOutputMutation {
    Set {
        child: AssetUuid,
        entry: DerivedOutputEntry,
    },
    Remove {
        child: AssetUuid,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagProjectionMutation {
    Set {
        asset: AssetUuid,
        tags: BTreeMap<String, Option<String>>,
    },
    Remove {
        asset: AssetUuid,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagPoisonMutation {
    Set {
        asset: AssetUuid,
        bundle: BundleUuid,
    },
    Remove {
        asset: AssetUuid,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Commit {
    pub assets: Vec<AssetMutation>,
    pub authoring: Vec<AuthoringMutation>,
    pub paths: Vec<PathMutation>,
    /// `Some` replaces the complete derived-output namespace for the new
    /// immutable version. `None` preserves it for metadata-only commits.
    pub derived_outputs: Option<BTreeMap<AssetUuid, DerivedOutputEntry>>,
    /// Bounded ordinary-publication updates applied after an optional full
    /// replacement. Duplicate child keys are rejected.
    pub derived_output_mutations: Vec<DerivedOutputMutation>,
    /// `Some` replaces the complete per-entry §10 tag-poison projection.
    /// A tag query whose other selectors could include one of these assets
    /// fails instead of returning an under-approximation.
    pub tag_poisons: Option<BTreeMap<AssetUuid, BundleUuid>>,
    pub tag_poison_mutations: Vec<TagPoisonMutation>,
    /// Complete value-bearing tag replacement independent of authored-value
    /// mutations. Pipeline-only publications use this to reindex the current
    /// namespace without replaying unrelated identity rows.
    pub tag_projection: Option<BTreeMap<AssetUuid, BTreeMap<String, Option<String>>>>,
    pub tag_projection_mutations: Vec<TagProjectionMutation>,
    pub configuration: Option<ConfigurationStatus>,
    pub pipeline: Option<PipelineDiagnostic>,
    /// The publication installs, retires, or poisons a module epoch even when
    /// the externally visible diagnostic remains `Ready`. Existing target
    /// Hubs must reconnect instead of retaining capabilities across that
    /// boundary.
    pub pipeline_epoch_changed: bool,
    /// `Some(None)` heals version poison; `Some(Some(_))` publishes it.
    pub version_poison: Option<Option<VersionPoison>>,
    /// `Some(None)` clears repair inspection; `Some(Some(_))` publishes the
    /// exact current missing/duplicate lineage basis.
    pub lineage_repair: Option<Option<LineageRepairState>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    UnknownTarget {
        target: String,
    },
    InvalidTargetSet(crate::TargetSetError),
    DuplicateAssetMutation {
        uuid: AssetUuid,
    },
    DuplicateAuthoringMutation {
        uuid: AssetUuid,
    },
    InvalidAuthoringValue {
        uuid: AssetUuid,
        error: AuthoringValueError,
    },
    InvalidAuthoringIdentity {
        uuid: AssetUuid,
        detail: String,
    },
    IncompleteArtifactTypeCoverage {
        missing: Vec<TypeUuid>,
    },
    InvalidServedClosure {
        detail: String,
    },
    InvalidArtifact {
        detail: String,
    },
    InvalidLineageRepairState {
        detail: String,
    },
    InvalidVersionPoison {
        error: distill_store::state::VersionPoisonError,
    },
    InvalidConfigurationPoison {
        error: distill_store::state::DscpError,
    },
    InvalidPipelineDiagnostic {
        detail: String,
    },
    DuplicatePathMutation {
        path: String,
    },
    InvalidPath {
        path: String,
    },
    EmptyPathCandidates {
        path: String,
    },
    ArtifactAlreadyExistsWithDifferentPayload {
        hash: ContentHash,
    },
    WireTreeHashMismatch {
        expected: LayoutHash,
        observed: LayoutHash,
    },
    InvalidWireTree {
        detail: String,
    },
    WireTreeAlreadyExistsWithDifferentPayload {
        hash: LayoutHash,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringValueError {
    LogicalSchemaUtf8,
    LogicalSchemaInvalid(String),
    ValueInvalid(String),
    ValueNotCanonical,
    SchemaValueShape { detail: String },
    BlobIndexOutOfRange { index: u32, blob_count: u32 },
    DuplicateBlobIndex { index: u32 },
    UnusedBlobIndex { index: u32 },
}
