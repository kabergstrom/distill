use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub use distill_core::id::{
    AssetUuid, BundleFileHash, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid,
};
pub use distill_store::state::{
    AssetClaimant, CleanupDisposition, ConfigurationError, ConfigurationErrorCode, DscpV1,
    GlobalBundleReadFailureCode, InputVersion, PhysicalPathClaim, PhysicalPathFailureCode,
    PipelineFailure, PipelineFailureCode, PipelineFailureOrigin, PlatformPathBytes,
    ReadableBundleSource, ScanFailureCode, ScanSubject, SkeletonFailureCode,
    SnapshotStamp, StoreInstanceId, NamespaceError, NamespaceErrorCode, NamespaceErrorV1,
};

/// 10: `Hub.importFailures`. 11: snapshot `runtimeTypePolicy`, batch-class
/// `resolve`, `ASSET_NOT_FOUND` (pack over RPC). 12: snapshot
/// `resolveNamed` (an asset by path and local id).
pub const PROTOCOL_VERSION: u32 = 12;

/// A watched import whose latest attempt failed. The bundle keeps serving its
/// last good contents; the failure clears when a later import succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportFailure {
    pub bundle: BundleUuid,
    /// Asset root name.
    pub root: String,
    /// The bundle's path within `root`.
    pub path: String,
    pub message: String,
}

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
    ConfigurationFailed(ConfigurationError),
    PipelineUnavailable(PipelineUnavailableDiagnostic),
    Rejected(ConnectError),
    /// Past `max_connections`: the request was valid; retry later.
    Refused(RpcFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineUnavailableDiagnostic {
    PipelineFailure(PipelineFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataConnectOutcome {
    Connected(MetadataConnected),
    ProtocolMismatch { expected: u32, observed: u32 },
    /// Past `max_connections`; retry later.
    Refused(RpcFailure),
}

impl MetadataConnectOutcome {
    pub fn connected(self) -> Option<MetadataConnected> {
        match self {
            Self::Connected(connected) => Some(connected),
            Self::ProtocolMismatch { .. } | Self::Refused(_) => None,
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
    Failed(ConfigurationError),
}

/// Closed, failure-safe pipeline diagnostic using shared typed §13 records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineDiagnostic {
    Ready,
    Failed(PipelineFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataDiagnostics {
    pub stamp: SnapshotStamp,
    pub configuration: ConfigurationStatus,
    pub pipeline: PipelineDiagnostic,
    /// Every current namespace error (LOCKLESS.md §4), in canonical order.
    pub namespace_errors: Vec<NamespaceError>,
}

/// R22/H4 terminology alias. The Cap'n Proto declaration calls the wire
/// struct `ConfigurationStatus`; both names denote the same snapshot-pinned
/// Ready/Failed state.
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
    /// The snapshot expired or was released: open a new one.
    SnapshotExpired,
    /// The connection was closed to admit a newer one: reconnect.
    ConnectionClosed,
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
    /// The write would drop data held under the on-disk schema: the
    /// named fields hold non-default values, or no automatic plan or
    /// migration function covers the schema change. `force_lossy` on the
    /// write overrides.
    LossyWrite {
        type_uuid: TypeUuid,
        asset: AssetUuid,
        fields: Vec<String>,
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
    SnapshotExpired,
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
            Self::SnapshotExpired => MetadataCall::SnapshotExpired,
            Self::Error(error) => MetadataCall::Error(error),
        }
    }
}

/// Namespace-facing metadata result grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataNamespaceCall<T> {
    Success(T),
    ReconnectRequired { reason: MetadataReconnectReason },
    SnapshotExpired,
    Error(RpcFailure),
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
    ConfigurationFailed(ConfigurationError),
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
            Self::ConfigurationFailed(error) => RpcResult::ConfigurationFailed(error),
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
    Doctor(Arc<[u8]>),
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

/// Closed maintenance request carried by [`LongRunningOp::Doctor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorRequest {
    Verify,
    RebuildIndexes,
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

impl DoctorRequest {
    pub fn encode(self) -> Arc<[u8]> {
        Arc::from([1, self.tag()])
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OperationPayloadError> {
        let mut reader = OperationPayloadReader::new(bytes)?;
        let request = match reader.u8()? {
            1 => Self::Verify,
            3 => Self::RebuildIndexes,
            tag => return Err(OperationPayloadError::InvalidTag(tag)),
        };
        reader.finish()?;
        Ok(request)
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Verify => 1,
            Self::RebuildIndexes => 3,
        }
    }
}

fn encode_operation_text(bytes: &mut Vec<u8>, text: &str) {
    let len = u32::try_from(text.len()).expect("operation text exceeds the RPC message limit");
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(text.as_bytes());
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
    /// event, inside the input open on `store` that publishes it.
    fn complete(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
    ) -> Result<DeferredOperationResult, String>;
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

/// The publishing step of an import run: publish what ran on a worker, or
/// fail. It runs inside the input open on the writer it is given, which
/// then publishes it.
pub type ImportJob =
    Box<dyn FnOnce(&mut distill_store::Store) -> Result<PreparedImportCommit, RpcFailure> + Send>;

/// Daemon integration seam for workflows that require importer, filesystem,
/// migration, or doctor services. Implementations prepare a side-effect-free
/// commit; publication remains an atomic RPC-server CAS step.
pub trait AuthoringBackend: Send + Sync + 'static {
    /// Execute an ordinary authoring batch against `base` and return the
    /// rescan-proven in-memory projection when the backend owns durable
    /// publication. `Ok(None)` retains the in-memory-only implementation used
    /// by embedders and tests that have no filesystem authority.
    ///
    /// The RPC server invokes this inside the input open on `store` that
    /// publishes it; production implementations must compare that store's
    /// version with `base`, publish, rescan, and advance it exactly once
    /// before returning, all through `store`. They must not call back into
    /// the [`crate::Server`].
    fn prepare_write(
        &self,
        _store: &mut distill_store::Store,
        _base: InputVersion,
        _operations: &[AuthoringOp],
        _force_lossy: bool,
    ) -> Result<Option<Commit>, RpcFailure> {
        Ok(None)
    }

    fn prepare_import(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
        request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure>;

    fn prepare_reimport(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure>;

    /// Run an import on a worker, off the RPC thread (on a reader or writer
    /// of the backend's own), and return the step that publishes it while
    /// still at `base`. The default leaves all the work to that step.
    fn run_import(
        self: Arc<Self>,
        base: InputVersion,
        request: ImportRequest,
    ) -> Result<ImportJob, RpcFailure> {
        Ok(Box::new(move |store| self.prepare_import(store, base, &request)))
    }

    /// [`AuthoringBackend::run_import`] for a reimport.
    fn run_reimport(
        self: Arc<Self>,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<ImportJob, RpcFailure> {
        Ok(Box::new(move |store| self.prepare_reimport(store, base, bundle)))
    }

    /// Check an operation against `store` and describe its progress; a
    /// deferred publication runs later, in its own input.
    fn prepare_operation(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure>;

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildWorkClass {
    Interactive,
    Batch,
}

/// What runtime resolution asks of the build backend when it reaches a
/// drifted asset. A build is a pure function of its inputs, so the request
/// names no input version: the backend keys the build by its static inputs
/// and answers it at the requester's [`BuildView`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildRequest {
    pub work_class: BuildWorkClass,
    pub target: String,
    pub target_definition: TargetDefinitionHash,
    /// UUID named by the resolver. For a primary this equals `entry.uuid`;
    /// for a derived output it is UUIDv5(parent, output_key).
    pub requested_asset: AssetUuid,
    /// Empty for the primary, otherwise the statically declared extra key.
    pub output_key: String,
    pub requested_terminal_type: TypeUuid,
    pub entry: BuildEntry,
    pub drifted_input: DriftedInput,
}

/// The authored entry a build request builds, as the served snapshot
/// records it: all a build takes from the request (it reads the rest
/// itself, at its view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildEntry {
    pub uuid: AssetUuid,
    pub type_uuid: TypeUuid,
    pub terminal_type: TypeUuid,
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

/// Artifacts and wire trees a backend outside the daemon publishes for one
/// build ([`crate::install_build_publication`]). The root artifact must be
/// present in `artifacts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildPublication {
    pub root_content_hash: ContentHash,
    pub artifacts: Vec<BuildArtifactPublication>,
    pub wire_trees: Vec<BuildWireTree>,
}

/// What a requester reads while it starts or answers a build: its own
/// snapshot, whose answers decide whether a result serves it, and the
/// store's latest committed state, where shared build results and artifacts
/// land. Both are readers: nothing that answers a resolve writes, and a
/// thread holding an open input has no view to offer.
#[derive(Clone, Copy)]
pub struct BuildView<'a> {
    pub snapshot: &'a distill_store::StoreReader,
    pub stamp: SnapshotStamp,
    pub latest: &'a distill_store::StoreReader,
}

/// A build's answer for one requester, at that requester's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildAnswer {
    /// The requested artifact is in the CAS.
    Built { content_hash: ContentHash },
    Failed { error: String },
    /// An input the build observed differs at the requester's snapshot; a
    /// fresher snapshot resolves it.
    Drifted { input: DriftedInput },
}

/// A finished build, still to be answered at each waiter's own snapshot.
pub trait BuildCompletion: Send {
    fn answer(self: Box<Self>, view: BuildView<'_>) -> Result<BuildAnswer, RpcFailure>;
}

/// One requester's interest in a submitted build. It resolves once the
/// build finishes; dropping it first withdraws the interest, and a build no
/// requester wants any more is not started.
pub struct BuildTicket(
    std::pin::Pin<Box<dyn std::future::Future<Output = Box<dyn BuildCompletion>> + Send>>,
);

impl BuildTicket {
    pub fn new(
        future: impl std::future::Future<Output = Box<dyn BuildCompletion>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl std::future::Future for BuildTicket {
    type Output = Box<dyn BuildCompletion>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl std::fmt::Debug for BuildTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuildTicket").finish_non_exhaustive()
    }
}

/// How a backend takes up a request.
#[derive(Debug)]
pub enum BuildStart {
    /// Answered at once, at the requester's snapshot (a cache hit, or a
    /// failure found before any build).
    Answered(Result<BuildAnswer, RpcFailure>),
    /// Submitted: the requester awaits the ticket, then asks the completion
    /// for its own answer.
    Submitted(BuildTicket),
}

/// Daemon integration seam for target-specific lazy builds. The backend
/// owns the shared build state: it keys a build by its static inputs,
/// shares one build among every requester of that key, publishes the
/// result to the CAS and notifies the requesters. The RPC server only reads.
/// Implementations must not call back into the [`crate::Server`].
pub trait BuildBackend: Send + Sync {
    /// Answer `request` at `view`, or submit its build and hand back a
    /// ticket. Called on a connection's thread: it reads, never blocks on a
    /// build.
    fn start(&self, view: BuildView<'_>, request: &BuildRequest) -> BuildStart;

    /// The runtime policy of `request`'s type under the compiled state
    /// `snapshot` sees.
    fn runtime_type_policy(
        &self,
        _snapshot: &distill_store::StoreReader,
        _request: &RuntimeTypePolicyRequest,
    ) -> Result<RuntimeTypePolicy, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "runtime type-policy lookup".to_owned(),
        })
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
    /// Check and publish the completion, on the connection's own writer.
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

impl ProgressStream {
    /// The next event. A completion publishes before it is answered: a
    /// failed publication turns it into `Failed`.
    fn next_event(&mut self) -> Option<AuthoringProgressEvent> {
        let event = self.events.pop_front()?;
        if event.state == AuthoringProgressState::Completed {
            let outcome = self.completion.complete();
            return Some(self.finish_completion(event, outcome));
        }
        if matches!(
            event.state,
            AuthoringProgressState::Cancelled | AuthoringProgressState::Failed
        ) {
            self.completion.cancel();
        }
        Some(self.settle(event))
    }

    /// The completion event, once its publication ran.
    fn finish_completion(
        &mut self,
        mut event: AuthoringProgressEvent,
        outcome: Result<(), String>,
    ) -> AuthoringProgressEvent {
        if let Err(error) = outcome {
            event.state = AuthoringProgressState::Failed;
            event.payload = Arc::from(error.into_bytes());
        }
        self.settle(event)
    }

    fn settle(&mut self, event: AuthoringProgressEvent) -> AuthoringProgressEvent {
        self.next_sequence = event.sequence.saturating_add(1);
        self.terminal_seen = event.state.is_terminal();
        event
    }
}

impl Iterator for ProgressStream {
    type Item = AuthoringProgressEvent;

    /// Runs a completion's publication on this thread.
    fn next(&mut self) -> Option<Self::Item> {
        self.next_event()
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
    pub tag_projection_mutations: Vec<TagProjectionMutation>,
    pub configuration: Option<ConfigurationStatus>,
    pub pipeline: Option<PipelineDiagnostic>,
    /// The publication installs, retires, or fails a module epoch even when
    /// the externally visible diagnostic remains `Ready`. Existing target
    /// Hubs must reconnect instead of retaining capabilities across that
    /// boundary.
    pub pipeline_epoch_changed: bool,
    /// `Some` replaces the namespace errors (LOCKLESS.md §4).
    pub namespace_errors: Option<Vec<NamespaceError>>,
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
    InvalidNamespaceError {
        error: distill_store::state::NamespaceErrorDecodeError,
    },
    InvalidConfigurationError {
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
