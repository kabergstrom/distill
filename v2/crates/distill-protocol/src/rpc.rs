use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub use distill_core::id::{
    AssetUuid, BundleFileHash, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid,
};
pub use crate::state::{
    AssetClaimant, CleanupDisposition, ConfigurationError, ConfigurationErrorCode, DscpV1,
    GlobalBundleReadFailureCode, InputVersion, NamespaceError, NamespaceErrorCode,
    NamespaceErrorV1, PhysicalPathClaim, PhysicalPathFailureCode, PipelineFailure,
    PipelineFailureCode, PipelineFailureOrigin, PlatformPathBytes, ReadableBundleSource,
    ScanFailureCode, ScanSubject, SkeletonFailureCode, SnapshotStamp, StoreInstanceId,
};

/// 10: `Hub.importFailures`. 11: snapshot `runtimeTypePolicy`, batch-class
/// `resolve`, `ASSET_NOT_FOUND` (pack over RPC). 12: snapshot
/// `resolveNamed` (an asset by path and local id). 13: the doctor request
/// is `Verify` alone, a read-only report that publishes no version
/// (`RebuildIndexes`, tag 3, is gone). 14: `Hub.write` and a rename's
/// Completed event answer a [`WriteReceipt`] (the files changed on disk)
/// instead of a version, and `AuthoringSnapshot.file` reads a file's
/// published content hash. 15: the reconnect reasons are
/// `storeInstanceChanged` and `pipelineEpochChanged`, and an asset delta is
/// `changed` or `deleted` (`targetDefinitionChanged`, `protocolEpochChanged`
/// and `restored` were never produced). 16: the metadata calls have no
/// `reconnectRequired` arm (a metadata connection never outlives its
/// store: the state directory is locked to the daemon process). 17: the
/// one reconnect reason is `pipelineEpochChanged` (a hub connection never
/// outlives its store either, so nothing could produce
/// `storeInstanceChanged`). 18: `RpcError` says what failed as a typed
/// `failure` union (the [`RpcFailure`], or a request the daemon could not
/// decode) instead of a numeric code, and `importFailures` moves from the hub
/// to the snapshot, naming each failure's bundle at that snapshot. 19:
/// `importFailures` also lists the directory imports whose attempt failed
/// before any bundle existed at their output, with an empty `bundle`. 20:
/// `AuthoringValue` carries `types`, the types its dynamic values name
/// (newgameplus GAP 23 §3; a value naming one fails to parse without it). 21:
/// `AuthoringSnapshot` gains `describeType` and `describableTypes` (§17
/// "Type descriptions"). 22: an authoring entry names its bundle's `root`
/// (a write creates a bundle in the root it names) and an inspection the
/// root it read the bundle from. 23 (newgameplus GAP 23 §12):
/// `AuthoringSnapshot` gains `list` (entry rows with type names and their
/// bundle's origin), `sourceFiles` (the files under a root with the layer
/// claiming each) and `references`; `Hub` gains `release` (an explicit
/// import's bundle follows the rule claiming its sources again). 24: an
/// `AuthoringRow` says whether its entry is its bundle's primary asset (what
/// a reference to the bundle's path without a name resolves to). 25: a
/// `TypeDescription` names its renamed fields (`renames`). 26: `AuthoringSnapshot`
/// gains `authorable` (§17 "Creating": the authored types the pipeline
/// declares, with their defaults, and its importers). 27: a
/// `DescribableType` says whether the type derives `Component` (`component`).
/// 28 (newgameplus GAP 23 §15.3): an `AuthoringValue`'s blobs are
/// [`AuthoringBlob`]s, a [`BlobRef`] (length and blake3) or bytes; an
/// inspection names its blobs by reference only, and a write may name a
/// blob its bundle holds by reference. `AuthoringSnapshot.inspect` takes a
/// list of entries (one snapshot, each bundle read once); `blob` streams an
/// entry's blob bytes.
/// 29 (GAP 23 §15.3): `AuthoringRow` and `AuthoringInspection` carry the
/// entry's `content` key (blake3 over schema hash, canonical text, blob
/// refs and carried types; [`crate::content_key`]).
/// 30 (GAP 23 §15.4): `Hub.subscribeIndex` delivers every published
/// version's `Delta`, unfiltered; a `Delta` names source `files` changed
/// and whether the type files changed (`types`);
/// `AuthoringSnapshot.listEntries` answers the rows of named entries.
/// 31 (GAP 23 §16.2): `TypeMember` carries the editor hints as one
/// canonical JSON text (`editor`) instead of `hasRange`/`rangeMin`/
/// `rangeMax`/`color`/`slider`/`hidden`.
pub const PROTOCOL_VERSION: u32 = 31;

/// A watched import whose latest attempt failed. The bundle keeps serving its
/// last good contents; the failure clears when a later import succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportFailure {
    /// `None` for a directory import whose attempt failed before any bundle
    /// existed at its output: nothing is served there until it succeeds.
    pub bundle: Option<BundleUuid>,
    /// Asset root name.
    pub root: String,
    /// The bundle's path within `root` (the output it would write, with no
    /// bundle).
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
pub enum PipelineUnavailableDiagnostic {
    PipelineFailure(PipelineFailure),
}

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
    /// The pipeline changed: an epoch installed, retired or failed. A
    /// target definition changes only with it (a configuration
    /// publication), and a reconnect under the old definition is rejected.
    PipelineEpochChanged,
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
            Self::SnapshotExpired => MetadataCall::SnapshotExpired,
            Self::Error(error) => MetadataCall::Error(error),
        }
    }

    /// A failure as the failure of another call; panics on a success.
    #[doc(hidden)]
    pub fn retype<U>(self) -> MetadataCall<U> {
        match self {
            Self::Success(_) => unreachable!("only failures are retyped"),
            Self::SnapshotExpired => MetadataCall::SnapshotExpired,
            Self::Error(error) => MetadataCall::Error(error),
        }
    }
}

/// Namespace-facing metadata result grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataNamespaceCall<T> {
    Success(T),
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
    /// The blob table the text's `{"len","offset"}` placements index.
    pub blobs: Vec<AuthoringBlob>,
    /// The types its dynamic values name (newgameplus GAP 23 §3), each
    /// once, in path order.
    pub types: Vec<AuthoringType>,
}

impl AuthoringValue {
    /// The value with every blob by reference: what an inspection of it
    /// answers.
    pub fn by_ref(&self) -> Self {
        Self {
            canonical_value: self.canonical_value.clone(),
            blobs: self.blobs.iter().map(|blob| AuthoringBlob::Ref(blob.blob_ref())).collect(),
            types: self.types.clone(),
        }
    }
}

/// A blob by length and blake3 of its bytes: what an inspection carries
/// instead of the bytes (newgameplus GAP 23 §15.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobRef {
    pub len: u64,
    pub hash: [u8; 32],
}

impl BlobRef {
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            len: bytes.len() as u64,
            hash: *blake3::hash(bytes).as_bytes(),
        }
    }
}

/// One blob of an [`AuthoringValue`]: by reference (inspections; a write
/// naming a blob its bundle already holds) or by its bytes (a write of new
/// content, import settings).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringBlob {
    Ref(BlobRef),
    Bytes(Arc<[u8]>),
}

impl AuthoringBlob {
    pub fn len(&self) -> u64 {
        match self {
            Self::Ref(blob) => blob.len,
            Self::Bytes(bytes) => bytes.len() as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The reference: a `Ref` as is, bytes hashed.
    pub fn blob_ref(&self) -> BlobRef {
        match self {
            Self::Ref(blob) => *blob,
            Self::Bytes(bytes) => BlobRef::of(bytes),
        }
    }

    /// The bytes of a `Bytes` blob.
    pub fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Ref(_) => None,
            Self::Bytes(bytes) => Some(bytes),
        }
    }
}

/// A type a carried value's dynamic values name: the path its text
/// spells and its verified schema snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringType {
    pub path: String,
    pub type_uuid: TypeUuid,
    pub schema_hash: LogicalHash,
    pub logical_schema: Arc<[u8]>,
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
    /// Skip the import when the destination already holds an import by
    /// `importer` of the same sources and watch flag whose `$settings` equal
    /// these settings completed with the importer's defaults; the reply
    /// names the existing bundle.
    pub if_changed: bool,
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

/// Closed request carried by [`LongRunningOp::Doctor`]. Doctor reports;
/// it never changes the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorRequest {
    Verify,
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

/// One file an authoring write changed on disk, by its new content hash
/// (`None`: removed). The store reflects it at the first version whose
/// `AuthoringSnapshot.file(root, path)` answers `content_hash`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenFile {
    pub root: String,
    pub path: String,
    pub content_hash: Option<ContentHash>,
}

/// What an authoring write (`Hub.write`, or a rename's Completed event)
/// did: the files it had atomically changed on disk when it answered. The
/// store follows them through the daemon's watcher like any other edit;
/// a client that needs the result waits for a version reflecting every
/// file. Empty when the write needed no file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteReceipt {
    pub files: Vec<WrittenFile>,
}

impl WriteReceipt {
    pub fn encode(&self) -> Arc<[u8]> {
        let mut bytes = vec![1];
        let count = u32::try_from(self.files.len()).expect("receipt exceeds the RPC message limit");
        bytes.extend_from_slice(&count.to_le_bytes());
        for file in &self.files {
            encode_operation_text(&mut bytes, &file.root);
            encode_operation_text(&mut bytes, &file.path);
            match file.content_hash {
                Some(hash) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&hash.0);
                }
                None => bytes.push(0),
            }
        }
        Arc::from(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OperationPayloadError> {
        let mut reader = OperationPayloadReader::new(bytes)?;
        let count = reader.u32()?;
        let mut files = Vec::new();
        for _ in 0..count {
            let root = reader.text()?;
            let path = reader.text()?;
            let content_hash = match reader.u8()? {
                0 => None,
                1 => Some(ContentHash(reader.array()?)),
                tag => return Err(OperationPayloadError::InvalidTag(tag)),
            };
            files.push(WrittenFile {
                root,
                path,
                content_hash,
            });
        }
        reader.finish()?;
        Ok(Self { files })
    }
}

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
            tag => return Err(OperationPayloadError::InvalidTag(tag)),
        };
        reader.finish()?;
        Ok(request)
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Verify => 1,
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
    /// The asset root of the bundle, by name. Empty: the bundle's own root;
    /// creating a bundle, the one configured root.
    pub root: String,
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
    /// The asset root holding the bundle, by name.
    pub root: String,
    /// The bundle's origin (protocol 23).
    pub origin: BundleOrigin,
    /// The type's path in the served schema; empty when unknown.
    pub type_name: String,
    /// The entry's authoring content key, as its row's (protocol 29).
    pub content: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Public wire-domain shape mirrors the typed result union.
pub enum AuthoringInspectResult {
    Inspection(AuthoringInspection),
    Missing,
    RoleIneligible {
        observed: AuthoringEntryRole,
    },
    /// The entry's bundle file no longer holds the bytes the snapshot
    /// published; a fresher snapshot (`current` or later) inspects it.
    Drifted {
        input: DriftedInput,
        current: SnapshotStamp,
    },
}


/// Who owns a bundle's entries (§8): a human (authored), an explicit
/// import, an authored directory rule or the default layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BundleOrigin {
    /// No `$record`: authored by hand.
    Authored,
    /// An explicit import (`$record` without a directory origin).
    Explicit,
    /// Generated by rule `rule` of rules bundle `rules_bundle`.
    Rule { rules_bundle: BundleUuid, rule: [u8; 16] },
    /// Generated by default import rule `rule` of the bundle's root.
    Default { rule: [u8; 16] },
}

/// One entry an [`AuthoringSnapshot::list`](crate::AuthoringSnapshot::list)
/// selects: its metadata, without its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringRow {
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub root: String,
    pub normalized_path: String,
    pub local_id: String,
    pub type_uuid: TypeUuid,
    /// The authored type's path in the served schema; empty when unknown.
    pub type_name: String,
    pub role: AuthoringEntryRole,
    pub origin: BundleOrigin,
    /// The bundle's primary asset: what a reference to the bundle's path
    /// without a name resolves to.
    pub primary: bool,
    /// The entry's authoring content key (§17 "Content keys"): equal keys
    /// mean equal values.
    pub content: [u8; 32],
}

/// The import layer claiming a source file (§8 "Default imports":
/// explicit > rules bundle > default).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceClaim {
    /// No layer: nothing imports the file.
    Unclaimed,
    /// An explicit import names it as a source.
    Explicit { bundle: BundleUuid },
    /// A directory rule claims it: of rules bundle `rules_bundle`, or of
    /// the root's default layer (`None`). `importer` is `none` for a rule
    /// that claims and imports nothing; `output` the bundle path its group
    /// renders, `bundle` the bundle there now.
    Rule {
        rules_bundle: Option<BundleUuid>,
        rule: [u8; 16],
        importer: String,
        output: String,
        bundle: Option<BundleUuid>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceFile {
    pub root: String,
    pub path: String,
    pub claim: SourceClaim,
}

/// A bundle's reference edges (§13 `bundle_path_refs`): the bundles whose
/// reference fields name its path, and the paths its own fields name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BundleReferences {
    /// (bundle, root, path), path-sorted.
    pub referenced_by: Vec<(BundleUuid, String, String)>,
    pub references: Vec<String>,
}

/// What an editor knows about a game type without loading game code (§17
/// "Type descriptions"): read from the type files a schema walk writes
/// (`ngp_schema::prefab_types`), the default included, all at one
/// snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeDescription {
    pub stamp: SnapshotStamp,
    pub type_uuid: TypeUuid,
    /// The type's path (`game::Spinner`), as prefabs name it.
    pub path: String,
    /// The hash of `logical_schema`.
    pub schema_hash: LogicalHash,
    /// Canonical §5 logical-schema snapshot bytes of the type's value
    /// projection, as [`AuthoringInspection::logical_schema`].
    pub logical_schema: Arc<[u8]>,
    pub default: TypeDefault,
    /// Docs and editor hints by planner path (`$`, `$.rate`,
    /// `$.shape{Ring}`), path-sorted; paths with neither are absent.
    pub members: Vec<TypeMember>,
    /// The renamed fields of the projection (`#[asset(renamed_from)]`):
    /// planner path → the name it was renamed from (protocol 25). An
    /// automatic migration of a value to `logical_schema` follows them
    /// (`distill_migrate::plan_automatic_renamed`).
    pub renames: BTreeMap<String, String>,
}

/// A described type's default value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeDefault {
    /// The value, canonical, conforming to the description's schema.
    Value(AuthoringValue),
    /// No default file: the type has no `Default` thunk in the game
    /// module, or its defaults were not written yet.
    Absent,
    /// The default file was written under another value projection (this
    /// logical hash): the module build and the schema files come from
    /// different walks (one is being rebuilt).
    SchemaMismatch { written: LogicalHash },
    /// The default file does not decode under the schema.
    Invalid(String),
}

/// One described path of a type ([`TypeDescription::members`]).
#[derive(Debug, Clone, PartialEq)]
pub struct TypeMember {
    pub path: String,
    /// The field's or variant's doc comment.
    pub doc: Option<String>,
    /// The doc comment of the struct or enum at the path.
    pub type_doc: Option<String>,
    /// `#[ngp(editor(...))]` hints as canonical JSON
    /// (`ngp_schema::describe::EditorHints::to_json`); empty: none. Distill
    /// carries them opaque: a new key needs no protocol change.
    pub editor: String,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // Mirrors the typed result union.
pub enum DescribeTypeResult {
    Description(TypeDescription),
    /// No type file describes the UUID at this snapshot.
    Missing,
    /// A type file no longer holds the bytes the snapshot observed; a
    /// fresher snapshot (`current` or later) describes it.
    Drifted {
        input: DriftedInput,
        current: SnapshotStamp,
    },
}

/// One describable type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribableType {
    pub type_uuid: TypeUuid,
    pub path: String,
    /// The type derives `Component` (the index's `component`, protocol 27):
    /// a prefab node can hold it.
    pub component: bool,
}

/// What an authoring tool can create (§17 "Creating"): the authored types
/// the serving pipeline declares and the control type
/// `distill::DirectoryImportRules`, and the pipeline's importers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Authorable {
    /// Extension-sorted.
    pub types: Vec<AuthorableType>,
    /// Id-sorted.
    pub importers: Vec<AuthorableImporter>,
}

/// A type a new bundle can hold: `<name>.<extension>.bundle` with one entry
/// `local_id` of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorableType {
    pub type_uuid: TypeUuid,
    /// The type's path in the served schema.
    pub path: String,
    pub extension: String,
    pub local_id: String,
    /// One line on what the type is for.
    pub doc: String,
    pub authoring_only: bool,
    pub schema_hash: LogicalHash,
    pub logical_schema: Arc<[u8]>,
    /// The pipeline's `Default` of the type, under `logical_schema`.
    /// `Absent` for `DirectoryImportRules` (a rules bundle needs a rule);
    /// `SchemaMismatch` while the pipeline module and the served schema
    /// disagree on the type (one is being rebuilt).
    pub default: TypeDefault,
}

/// A registered importer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorableImporter {
    pub id: String,
    pub settings_type: TypeUuid,
    /// The settings type's path in the served schema: what a rule's
    /// settings name (`{"type": <path>, "value": …}`).
    pub settings_path: String,
    pub settings_hash: LogicalHash,
    pub settings_schema: Arc<[u8]>,
    /// Complete default settings, under `settings_schema`.
    pub default_settings: AuthoringValue,
    /// The path globs of the default import rules naming this importer
    /// (§8 "Default imports"), in registration order.
    pub default_globs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribableTypesResult {
    /// The types of the snapshot's type index, path-sorted, and the index
    /// file's content hash (`None`: no index): a changed hash at a later
    /// snapshot is a changed list.
    Types {
        stamp: SnapshotStamp,
        index_hash: Option<ContentHash>,
        types: Vec<DescribableType>,
    },
    Drifted {
        input: DriftedInput,
        current: SnapshotStamp,
    },
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetDeltaState {
    Changed,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub basis: RpcBasis,
    pub assets: Vec<(AssetUuid, AssetDeltaState)>,
    pub paths: Vec<String>,
    /// Source files the scan found added, changed or removed (bundle
    /// files excepted): index subscriptions only (protocol 30).
    pub files: Vec<FileChange>,
    /// A file of the type-files root changed (`describableTypes` and
    /// `describeType` may answer otherwise): index subscriptions only.
    pub types: bool,
}

/// One source file a version changed (protocol 30).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChange {
    pub root: String,
    pub path: String,
    pub state: AssetDeltaState,
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

/// An asset whose resolution a publication changes, with the delta its
/// subscribers see. How it resolves is read from the store's tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetMutation {
    pub uuid: AssetUuid,
    pub delta: AssetDeltaState,
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
    /// The bundle paths of runtime entries the namespace did not hold
    /// before: a named reference (path and local id) may wait on one, so
    /// the path changes too.
    pub new_entry_paths: BTreeSet<String>,
    /// `Some` replaces the complete per-entry §10 tag-poison projection.
    /// A tag query whose other selectors could include one of these assets
    /// fails instead of returning an under-approximation.
    pub tag_poisons: Option<BTreeMap<AssetUuid, BundleUuid>>,
    pub tag_poison_mutations: Vec<TagPoisonMutation>,
    pub tag_projection_mutations: Vec<TagProjectionMutation>,
    /// The publication installs, retires, or fails a module epoch even when
    /// the externally visible diagnostic remains `Ready`. Existing target
    /// Hubs must reconnect instead of retaining capabilities across that
    /// boundary.
    pub pipeline_epoch_changed: bool,
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
    /// Blob `index` names by reference a blob its bundle does not hold.
    UnknownBlobRef { index: u32 },
    /// A carried type is malformed, listed twice, or its schema does not
    /// verify.
    TypeInvalid(String),
    /// A carried type no dynamic value names.
    UnusedType { path: String },
}
