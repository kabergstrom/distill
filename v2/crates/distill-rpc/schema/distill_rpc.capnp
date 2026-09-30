@0xbecdb9786a3f2e31;

# Wire-shaped declaration of the §17 surface. Fixed-size Data fields are
# length-checked by the Rust transport adapter (UUID=16, hashes=32, instance=16).

struct SnapshotStamp {
  instance @0 :Data;
  version @1 :UInt64;
}

struct RpcBasisValue {
  stamp @0 :SnapshotStamp;
}

struct ConfigurationError {
  code @0 :UInt16;
  reasonHash @1 :Data;
  message @2 :Text;
  detailVersion @3 :UInt16;
  detailBytes @4 :Data;
}

enum ReconnectReason {
  targetDefinitionChanged @0;
  storeInstanceChanged @1;
  protocolEpochChanged @2;
  pipelineEpochChanged @3;
}

struct ReconnectRequired {
  reason @0 :ReconnectReason;
}

struct RpcError {
  code @0 :UInt16;
  message @1 :Text;
}

struct ProtocolFailure {
  expected @0 :UInt32;
  observed @1 :UInt32;
  message @2 :Text;
}

enum TargetFailureCode {
  unknownTarget @0;
  definitionMismatch @1;
}

struct TargetFailure {
  code @0 :TargetFailureCode;
  expected @1 :Data;
  observed @2 :Data;
}

struct ConnectCall {
  union {
    success @0 :ConnectSuccess;
    targetFailure @1 :TargetFailure;
    configurationFailed @2 :ConfigurationError;
    protocolFailure @3 :ProtocolFailure;
    error @4 :RpcError;
    pipelineUnavailable @5 :PipelineUnavailableDiagnostic;
  }
}

struct PipelineUnavailableDiagnostic {
  pipelineFailure @0 :PipelineFailure;
}

struct ConnectSuccess {
  hub @0 :Hub;
  instance @1 :Data;
}

struct MetadataConnectResult {
  union {
    success @0 :MetadataConnectSuccess;
    protocolFailure @1 :ProtocolFailure;
    error @2 :RpcError;
  }
}

struct MetadataConnectSuccess {
  hub @0 :MetadataHub;
  instance @1 :Data;
  protocolEpoch @2 :UInt32;
}

enum MetadataReconnectReason {
  storeInstanceChanged @0;
  protocolEpochChanged @1;
}

struct MetadataReconnectRequired {
  reason @0 :MetadataReconnectReason;
}

struct MetadataSnapshotCall {
  union {
    success @0 :MetadataSnapshot;
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
  }
}

struct MetadataAuthoringSnapshotCall {
  union {
    success @0 :MetadataAuthoringSnapshot;
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
  }
}

struct MetadataDiagnosticsCall {
  union {
    success @0 :MetadataDiagnostics;
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
  }
}

struct MetadataUInt64Call {
  union {
    success @0 :UInt64;
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
  }
}

struct MetadataChunkStreamCall {
  union {
    success @0 :ChunkStream;
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
  }
}

struct MetadataUuidListCall {
  union {
    success @0 :List(Uuid);
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
  }
}

struct MetadataEntryMetaCall {
  union {
    success @0 :PureMetadataEntry;
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
  }
}

struct PureMetadataEntry {
  uuid @0 :Uuid;
  bundle @1 :Uuid;
  localId @2 :Text;
  normalizedPath @3 :Text;
  authoredType @4 :Uuid;
  schemaHash @5 :Data;
  role @6 :AuthoringEntryRole;
}

struct MetadataPathResolveCall {
  union {
    success @0 :PathResolveResult;
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
  }
}

struct MetadataAuthoringInspectCall {
  union {
    success @0 :AuthoringInspection;
    reconnectRequired @1 :MetadataReconnectRequired;
    snapshotExpired @2 :Void;
    error @3 :RpcError;
    missing @4 :Void;
    roleIneligible @5 :AuthoringRoleFailure;
  }
}

struct SnapshotCall {
  union {
    success @0 :Snapshot;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct AuthoringSnapshotCall {
  union {
    success @0 :AuthoringSnapshot;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct SubscribeCall {
  union {
    success @0 :Subscription;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct VoidCall {
  union {
    success @0 :Void;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct UInt64Call {
  union {
    success @0 :UInt64;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct Uuid {
  bytes @0 :Data;
}

struct UuidCall {
  union {
    success @0 :Uuid;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

interface ProgressStream {
  next @0 () -> (done :Bool, progress :AuthoringProgressEvent);
  cancel @1 () -> (cancelled :Bool);
}

enum AuthoringProgressState {
  started @0;
  running @1;
  completed @2;
  cancelled @3;
  failed @4;
}

struct AuthoringProgressEvent {
  sequence @0 :UInt64;
  state @1 :AuthoringProgressState;
  payload @2 :Data;
}

struct ProgressCall {
  union {
    success @0 :ProgressStream;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct DataCall {
  union {
    success @0 :Data;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct UuidListCall {
  union {
    success @0 :List(Uuid);
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

# A watched import whose latest attempt failed (see Hub.importFailures).
struct ImportFailure {
  bundle @0 :Data;
  root @1 :Text;
  path @2 :Text;
  message @3 :Text;
}

struct ImportFailuresCall {
  union {
    success @0 :List(ImportFailure);
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct OptionalData {
  union {
    absent @0 :Void;
    value @1 :Data;
  }
}

struct OptionalText {
  union {
    absent @0 :Void;
    value @1 :Text;
  }
}

struct OptionalBool {
  union {
    absent @0 :Void;
    value @1 :Bool;
  }
}

struct TagSelector {
  tag @0 :Text;
  value @1 :OptionalText;
}

struct OptionalTagSelector {
  union {
    absent @0 :Void;
    value @1 :TagSelector;
  }
}

struct AssetQuery {
  uuid @0 :OptionalData;
  bundlePath @1 :OptionalText;
  localId @2 :OptionalText;
  bundleUuid @3 :OptionalData;
  authoredType @4 :OptionalData;
  terminalType @5 :OptionalData;
  tag @6 :OptionalTagSelector;
  pathPrefix @7 :OptionalText;
  pathGlob @8 :OptionalText;
  authoringOnly @9 :OptionalBool;
}

enum AuthoringEntryRole {
  runtime @0;
  authoringOnly @1;
}

struct PureMetadataQuery {
  uuid @0 :Uuid;
  hasUuid @1 :Bool;
  bundle @2 :Uuid;
  hasBundle @3 :Bool;
  normalizedPathPrefix @4 :Text;
  hasPathPrefix @5 :Bool;
  authoredType @6 :Uuid;
  hasAuthoredType @7 :Bool;
  role @8 :AuthoringEntryRole;
  hasRole @9 :Bool;
}

struct BundleSource {
  rootName @0 :Text;
  normalizedPath @1 :Text;
  fileHash @2 :Data;
}

struct AuthoredAssetClaimant {
  source @0 :BundleSource;
  bundle @1 :Uuid;
  localId @2 :Text;
}

struct DerivedAssetClaimant {
  parent @0 :Uuid;
  outputKey @1 :Text;
}

struct AssetClaimant {
  union {
    authored @0 :AuthoredAssetClaimant;
    derived @1 :DerivedAssetClaimant;
  }
}

struct DuplicateAssetError {
  asset @0 :Uuid;
  claimants @1 :List(AssetClaimant);
}

struct DuplicateBundleError {
  bundle @0 :Uuid;
  sources @1 :List(BundleSource);
}

struct PlatformPathBytes {
  union {
    unixBytes @0 :Data;
    windowsUtf16Le @1 :Data;
  }
}

struct PhysicalPathClaim {
  rawRelativePath @0 :PlatformPathBytes;
  fileHash @1 :Data;
}

struct SameRootNormalizedPathError {
  rootName @0 :Text;
  normalizedPath @1 :Text;
  claims @2 :List(PhysicalPathClaim);
}

struct IncompleteSkeletonError {
  source @0 :BundleSource;
  failureCode @1 :UInt16;
}

struct UnreadableGlobalPathError {
  rootName @0 :Text;
  normalizedPath @1 :Text;
  failureCode @2 :UInt16;
}

struct InvalidPhysicalPathError {
  rootName @0 :Text;
  rawRelativePath @1 :PlatformPathBytes;
  failureCode @2 :UInt16;
}

struct ScanRootSubject {
  rootName @0 :Text;
}

struct ScanSubtreeSubject {
  rootName @0 :Text;
  rawRelativePath @1 :PlatformPathBytes;
}

struct ScanSubject {
  union {
    root @0 :ScanRootSubject;
    subtree @1 :ScanSubtreeSubject;
  }
}

enum ScanFailureCodeValue {
  permissionDenied @0;
  notFound @1;
  invalidFileType @2;
  symlinkIdentityChanged @3;
  ioDataLoss @4;
}

struct UnreadableScanSubtreeError {
  subject @0 :ScanSubject;
  failure @1 :ScanFailureCodeValue;
}

struct NamespaceErrorDetail {
  union {
    duplicateAssetUuid @0 :DuplicateAssetError;
    duplicateBundleUuid @1 :DuplicateBundleError;
    sameRootNormalizedPathCollision @2 :SameRootNormalizedPathError;
    incompleteSkeleton @3 :IncompleteSkeletonError;
    unreadableGlobalBundlePath @4 :UnreadableGlobalPathError;
    invalidPhysicalPath @5 :InvalidPhysicalPathError;
    unreadableScanSubtree @6 :UnreadableScanSubtreeError;
  }
}

struct NamespaceError {
  code @0 :UInt16;
  identity @1 :Data;
  detail @2 :NamespaceErrorDetail;
  message @3 :Text;
}

struct ConfigurationDiagnostic {
  union {
    ready @0 :Void;
    failed @1 :ConfigurationError;
  }
}

enum PipelineFailureCode {
  candidateOpen @0;
  candidateAttestation @1;
  candidateRegistration @2;
  candidateValidation @3;
  candidateCleanup @4;
  publishedCallbackPanic @5;
  publishedCallbackRejected @6;
  publishedCleanup @7;
}

enum PipelineFailureOrigin {
  candidateOpen @0;
  publishedRuntime @1;
}

enum CleanupDisposition {
  none @0;
  cleanedAndClosed @1;
  registrationCleanupFailed @2;
  moduleUnloadFailed @3;
  tokenPoisoned @4;
  tokenPinned @5;
  dlcloseFailed @6;
  publishedEpochLeaked @7;
}

struct PipelineFailure {
  code @0 :PipelineFailureCode;
  origin @1 :PipelineFailureOrigin;
  cleanup @2 :CleanupDisposition;
  identity @3 :Data;
  message @4 :Text;
}

struct PipelineDiagnostic {
  union {
    ready @0 :Void;
    failed @1 :PipelineFailure;
  }
}

struct MetadataDiagnostics {
  stamp @0 :SnapshotStampValue;
  configuration @1 :ConfigurationDiagnostic;
  pipeline @2 :PipelineDiagnostic;
  namespaceErrors @3 :List(NamespaceError);
}

struct AuthoringValue {
  canonicalValue @0 :Data;
  blobs @1 :List(Data);
}

struct AuthoringEntryValue {
  uuid @0 :Uuid;
  bundle @1 :Uuid;
  localId @2 :Text;
  normalizedPath @3 :Text;
  typeUuid @4 :Uuid;
  terminalType @5 :Uuid;
  schemaHash @6 :Data;
  logicalSchema @7 :Data;
  role @8 :AuthoringEntryRole;
  tags @9 :List(EntryTag);
  value @10 :AuthoringValue;
}

struct AuthoringOp {
  union {
    set @0 :AuthoringEntryValue;
    remove @1 :Uuid;
  }
}

struct ImportRequest {
  importer @0 :Text;
  sources @1 :List(Text);
  dest @2 :Text;
  settings @3 :AuthoringValue;
  watch @4 :Bool;
  root @5 :Text;
}

struct LongRunningOp {
  union {
    renameWithFixups @0 :Data;
    doctor @1 :Data;
  }
}

struct SnapshotStampValue {
  instance @0 :Data;
  inputVersion @1 :UInt64;
}

struct AuthoringInspection {
  stamp @0 :SnapshotStampValue;
  uuid @1 :Data;
  bundle @2 :Data;
  localId @3 :Text;
  normalizedPath @4 :Text;
  typeUuid @5 :Data;
  schemaHash @6 :Data;
  role @7 :AuthoringEntryRole;
  value @8 :AuthoringValue;
  logicalSchema @9 :Data;
}

struct AuthoringRoleFailure {
  observed @0 :AuthoringEntryRole;
}

struct AuthoringInspectCall {
  union {
    success @0 :AuthoringInspection;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
    missing @5 :Void;
    roleIneligible @6 :AuthoringRoleFailure;
  }
}

struct EntryMeta {
  uuid @0 :Uuid;
  bundle @1 :Uuid;
  localId @2 :Text;
  normalizedPath @3 :Text;
  authoredType @4 :Uuid;
  terminalType @5 :Uuid;
  schemaHash @6 :Data;
  role @7 :AuthoringEntryRole;
  tags @8 :List(EntryTag);
}

struct EntryTag {
  tag @0 :Text;
  hasValue @1 :Bool;
  value @2 :Text;
}

struct EntryMetaCall {
  union {
    success @0 :EntryMeta;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct TerminalResolve {
  basis @0 :RpcBasisValue;
  result @1 :ResolveResult;
}

struct DriftedInputValue {
  union {
    file @0 :Text;
    asset @1 :Data;
    query @2 :Text;
    dylib @3 :Void;
    tool @4 :Text;
  }
}

struct DriftedResolve {
  input @0 :DriftedInputValue;
  current @1 :SnapshotStamp;
}

struct ResolveCall {
  union {
    success @0 :TerminalResolve;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct ResolveResult {
  union {
    built @0 :Data;
    drifted @1 :DriftedResolve;
    failed @2 :Text;
    missing @3 :Void;
    deleted @4 :SnapshotStamp;
    roleIneligible @5 :AuthoringRoleFailure;
  }
}

struct TerminalPathResolve {
  basis @0 :RpcBasisValue;
  result @1 :PathResolveResult;
}

struct PathResolveCall {
  union {
    success @0 :TerminalPathResolve;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct PathResolveResult {
  union {
    resolved @0 :Data;
    missing @1 :Void;
    ambiguous @2 :List(Data);
  }
}

struct TerminalFetch {
  basis @0 :RpcBasisValue;
  chunks @1 :ChunkStream;
  totalBytes @2 :UInt64;
  loadEdges @3 :List(ServedLoadEdge);
}

struct ServedLoadEdge {
  asset @0 :Data;
  expectedTerminal @1 :Data;
}

struct ChunkStreamCall {
  union {
    success @0 :TerminalFetch;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct Subscription {
  deltas @0 :DeltaStream;
  installed @1 :UInt64;
}

interface Root {
  connect @0 (target :Text, targetDefHash :Data, protocol :UInt32)
          -> (result :ConnectCall);
  metadata @1 (protocol :UInt32) -> (result :MetadataConnectResult);
}

interface MetadataHub {
  snapshot @0 () -> (result :MetadataSnapshotCall);
  authoringSnapshot @1 () -> (result :MetadataAuthoringSnapshotCall);
  diagnostics @2 () -> (result :MetadataDiagnosticsCall);
  fetch @3 (hash :Data) -> (result :MetadataChunkStreamCall);
}

interface MetadataSnapshot {
  version @0 () -> (result :MetadataUInt64Call);
  diagnostics @1 () -> (result :MetadataDiagnosticsCall);
  query @2 (q :PureMetadataQuery) -> (result :MetadataUuidListCall);
  entry @3 (uuid :Uuid) -> (result :MetadataEntryMetaCall);
  resolvePath @4 (path :Text) -> (result :MetadataPathResolveCall);
  refresh @5 () -> (result :MetadataSnapshotCall);
}

interface MetadataAuthoringSnapshot {
  version @0 () -> (result :MetadataUInt64Call);
  query @1 (q :PureMetadataQuery) -> (result :MetadataUuidListCall);
  inspect @2 (uuid :Uuid) -> (result :MetadataAuthoringInspectCall);
  refresh @3 () -> (result :MetadataAuthoringSnapshotCall);
}

interface Hub {
  snapshot @0 () -> (result :SnapshotCall);
  subscribe @1 (since :UInt64, assets :List(Data), paths :List(Text))
            -> (result :SubscribeCall);
  # forceLossy writes even when data held under the on-disk schema would
  # be dropped.
  write @2 (base :UInt64, ops :List(AuthoringOp), forceLossy :Bool) -> (result :UInt64Call);
  import @3 (base :UInt64, request :ImportRequest) -> (result :UuidCall);
  reimport @4 (base :UInt64, bundle :Uuid) -> (result :UuidCall);
  operation @5 (base :UInt64, operation :LongRunningOp) -> (result :ProgressCall);
  wireTree @6 (layoutHash :Data) -> (result :DataCall);
  unsubscribe @7 (assets :List(Data), paths :List(Text))
              -> (result :VoidCall);
  authoringSnapshot @8 () -> (result :AuthoringSnapshotCall);
  # Current watched-import failures: memo state, not versioned input, so
  # clients poll it. Protocol 10.
  importFailures @9 () -> (result :ImportFailuresCall);
}

interface Snapshot {
  version @0 () -> (result :UInt64Call);
  query @1 (query :AssetQuery) -> (result :UuidListCall);
  entry @2 (uuid :Data) -> (result :EntryMetaCall);
  resolve @3 (uuid :Data) -> (result :ResolveCall);
  refresh @4 () -> (result :SnapshotCall);
  resolvePath @5 (path :Text) -> (result :PathResolveCall);
  configuration @6 () -> (result :VoidCall);
  fetch @7 (hash :Data) -> (result :ChunkStreamCall);
}

interface AuthoringSnapshot {
  version @0 () -> (result :UInt64Call);
  query @1 (query :AssetQuery) -> (result :UuidListCall);
  inspect @2 (uuid :Data) -> (result :AuthoringInspectCall);
  refresh @3 () -> (result :AuthoringSnapshotCall);
}

interface ChunkStream {
  next @0 () -> (done :Bool, kind :UInt16, index :UInt32,
                 offset :UInt64, bytes :Data);
}

struct AssetDelta {
  uuid @0 :Data;
  state @1 :AssetDeltaState;
}

enum AssetDeltaState {
  changed @0;
  deleted @1;
  restored @2;
}

struct Delta {
  basis @0 :RpcBasisValue;
  assets @1 :List(AssetDelta);
  paths @2 :List(Text);
}

struct StreamEvent {
  basis @0 :RpcBasisValue;
  union {
    initialDelta @1 :List(Delta);
    delta @2 :Delta;
    resyncRequired @3 :UInt64;
    restartRequired @4 :List(Text);
    reconnectRequired @5 :ReconnectReason;
  }
}

interface DeltaStream {
  next @0 () -> (done :Bool, event :StreamEvent);
}
