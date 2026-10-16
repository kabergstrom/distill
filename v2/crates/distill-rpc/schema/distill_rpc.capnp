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
  pipelineEpochChanged @0;
}

struct ReconnectRequired {
  reason @0 :ReconnectReason;
}

# Why a call failed: `failure` says what, for programs (the Rust
# `RpcFailure`, or a request the daemon could not decode); `message` says it
# for people.
struct RpcError {
  message @0 :Text;
  failure :union {
    invalidRequest @1 :InvalidRequest;
    snapshotExpired @2 :Void;
    connectionClosed @3 :Void;
    pipelineUnavailable @4 :PipelineUnavailableDiagnostic;
    buildDepthExceeded @5 :BuildDepthExceeded;
    invalidCursor @6 :InvalidCursor;
    resourceLimit @7 :ResourceLimit;
    artifactNotFound @8 :Data;
    assetNotFound @9 :Data;
    foreignSnapshot @10 :Void;
    invalidPath @11 :Text;
    invalidQuery @12 :Text;
    tagIndexPoisoned @13 :List(Data);
    staleInputVersion @14 :StaleInputVersion;
    invalidAuthoringRequest @15 :Text;
    lossyWrite @16 :LossyWrite;
    authoringBackendUnavailable @17 :Text;
    wireTreeNotFound @18 :Data;
  }
}

# The request field the daemon could not decode, by what it must be.
enum InvalidRequest {
  uuid @0;
  hash @1;
  instance @2;
  utf8 @3;
  value @4;
}

struct BuildDepthExceeded {
  limit @0 :UInt64;
  chain @1 :List(Data);
}

struct InvalidCursor {
  since @0 :UInt64;
  current @1 :UInt64;
}

struct ResourceLimit {
  resource @0 :Text;
  limit @1 :UInt64;
}

struct StaleInputVersion {
  expected @0 :UInt64;
  got @1 :UInt64;
}

struct LossyWrite {
  typeUuid @0 :Data;
  asset @1 :Data;
  fields @2 :List(Text);
  detail @3 :Text;
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

struct MetadataSnapshotCall {
  union {
    success @0 :MetadataSnapshot;
    snapshotExpired @1 :Void;
    error @2 :RpcError;
  }
}

struct MetadataAuthoringSnapshotCall {
  union {
    success @0 :MetadataAuthoringSnapshot;
    snapshotExpired @1 :Void;
    error @2 :RpcError;
  }
}

struct MetadataDiagnosticsCall {
  union {
    success @0 :MetadataDiagnostics;
    snapshotExpired @1 :Void;
    error @2 :RpcError;
  }
}

struct MetadataUInt64Call {
  union {
    success @0 :UInt64;
    snapshotExpired @1 :Void;
    error @2 :RpcError;
  }
}

struct MetadataChunkStreamCall {
  union {
    success @0 :ChunkStream;
    snapshotExpired @1 :Void;
    error @2 :RpcError;
  }
}

struct MetadataUuidListCall {
  union {
    success @0 :List(Uuid);
    snapshotExpired @1 :Void;
    error @2 :RpcError;
  }
}

struct MetadataEntryMetaCall {
  union {
    success @0 :PureMetadataEntry;
    snapshotExpired @1 :Void;
    error @2 :RpcError;
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
    snapshotExpired @1 :Void;
    error @2 :RpcError;
  }
}

struct MetadataAuthoringInspectCall {
  union {
    success @0 :AuthoringInspection;
    snapshotExpired @1 :Void;
    error @2 :RpcError;
    missing @3 :Void;
    roleIneligible @4 :AuthoringRoleFailure;
    drifted @5 :DriftedResolve;
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

# A watched import whose latest attempt failed (see Snapshot.importFailures).
struct ImportFailure {
  # Empty for a directory import that failed before any bundle existed at
  # its output (protocol 19); `path` is then that output.
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

# A blob a value names (protocol 28): its length and blake3 hash, or, in a
# write, its bytes. Inspections always carry refs; the bytes come from
# AuthoringSnapshot.blob. A write naming a ref keeps the bytes the entry
# already holds under it.
struct BlobRef {
  len @0 :UInt64;
  hash @1 :Data;
}

struct AuthoringBlob {
  union {
    ref @0 :BlobRef;
    bytes @1 :Data;
  }
}

struct AuthoringValue {
  canonicalValue @0 :Data;
  blobs @1 :List(AuthoringBlob);
  # The types its dynamic values name (newgameplus GAP 23 §3; protocol 20).
  types @2 :List(AuthoringType);
}

struct AuthoringType {
  path @0 :Text;
  typeUuid @1 :Uuid;
  schemaHash @2 :Data;
  logicalSchema @3 :Data;
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
  # The root of the bundle: empty for the bundle's own root, or, creating a
  # bundle, the one configured root (protocol 22).
  root @11 :Text;
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
  ifChanged @6 :Bool;
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
  # The root holding the entry's bundle (protocol 22).
  root @10 :Text;
  # The bundle's origin and the type's served schema path (protocol 23).
  origin @11 :BundleOrigin;
  typeName @12 :Text;
  # The entry's authoring content key (protocol 29).
  content @13 :Data;
}

struct AuthoringRoleFailure {
  observed @0 :AuthoringEntryRole;
}

# One answer per uuid of AuthoringSnapshot.inspect, in order (protocol 28).
struct AuthoringInspectAnswer {
  union {
    success @0 :AuthoringInspection;
    missing @1 :Void;
    roleIneligible @2 :AuthoringRoleFailure;
    drifted @3 :DriftedResolve;
  }
}

struct AuthoringInspectManyCall {
  union {
    success @0 :List(AuthoringInspectAnswer);
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

# A blob of an authored entry's value as a chunk stream (protocol 28).
struct AuthoringBlobStream {
  stamp @0 :SnapshotStampValue;
  totalBytes @1 :UInt64;
  chunks @2 :ChunkStream;
}

struct AuthoringBlobCall {
  union {
    success @0 :AuthoringBlobStream;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
    missing @5 :Void;
    drifted @6 :DriftedResolve;
  }
}

# §17 "Type descriptions" (protocol 21).
struct TypeMember {
  # Planner path: `$`, `$.rate`, `$.shape{Ring}`.
  path @0 :Text;
  # Empty: none.
  doc @1 :Text;
  typeDoc @2 :Text;
  hasRange @3 :Bool;
  rangeMin @4 :Float64;
  rangeMax @5 :Float64;
  color @6 :Bool;
  slider @7 :Bool;
  hidden @8 :Bool;
}

struct TypeRename {
  # Planner path in the current schema.
  at @0 :Text;
  # The field name it was renamed from.
  from @1 :Text;
}

struct TypeDefault {
  union {
    value @0 :AuthoringValue;
    # No default file for the type.
    absent @1 :Void;
    # The logical hash the default file was written under.
    schemaMismatch @2 :Data;
    invalid @3 :Text;
  }
}

struct TypeDescription {
  stamp @0 :SnapshotStampValue;
  typeUuid @1 :Data;
  path @2 :Text;
  schemaHash @3 :Data;
  logicalSchema @4 :Data;
  defaultValue @5 :TypeDefault;
  members @6 :List(TypeMember);
  # Renamed fields (`#[asset(renamed_from)]`) by planner path, path-sorted
  # (protocol 25): what an automatic migration to `logicalSchema` needs.
  renames @7 :List(TypeRename);
}

struct DescribeTypeCall {
  union {
    success @0 :TypeDescription;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
    missing @5 :Void;
    drifted @6 :DriftedResolve;
  }
}

struct DescribableType {
  typeUuid @0 :Data;
  path @1 :Text;
  # The type derives `Component` (protocol 27).
  component @2 :Bool;
}

struct DescribableTypes {
  stamp @0 :SnapshotStampValue;
  # The index file's content hash; empty without one.
  indexHash @1 :Data;
  types @2 :List(DescribableType);
}

struct DescribableTypesCall {
  union {
    success @0 :DescribableTypes;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
    drifted @5 :DriftedResolve;
  }
}

# §17 "Creating" (protocol 26).
struct AuthorableType {
  typeUuid @0 :Data;
  path @1 :Text;
  # A new bundle is `<name>.<extension>.bundle`, its one entry `localId`.
  extension @2 :Text;
  localId @3 :Text;
  doc @4 :Text;
  authoringOnly @5 :Bool;
  schemaHash @6 :Data;
  logicalSchema @7 :Data;
  defaultValue @8 :TypeDefault;
}

struct AuthorableImporter {
  id @0 :Text;
  settingsType @1 :Data;
  settingsPath @2 :Text;
  settingsHash @3 :Data;
  settingsSchema @4 :Data;
  defaultSettings @5 :AuthoringValue;
  # Globs of the default import rules naming the importer.
  defaultGlobs @6 :List(Text);
}

struct Authorable {
  types @0 :List(AuthorableType);
  importers @1 :List(AuthorableImporter);
}

struct AuthorableCall {
  union {
    success @0 :Authorable;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

# §17 "Browsing" (protocol 23).
struct BundleOrigin {
  union {
    # No $record: authored by hand.
    authored @0 :Void;
    # An explicit import.
    explicit @1 :Void;
    rule :group {
      rulesBundle @2 :Data;
      rule @3 :Data;
    }
    # The root's default import layer.
    default @4 :Data;
  }
}

struct AuthoringRow {
  uuid @0 :Data;
  bundle @1 :Data;
  root @2 :Text;
  normalizedPath @3 :Text;
  localId @4 :Text;
  typeUuid @5 :Data;
  # The authored type's path in the served schema; empty when unknown.
  typeName @6 :Text;
  role @7 :AuthoringEntryRole;
  origin @8 :BundleOrigin;
  # The bundle's primary asset: what a reference to the bundle's path
  # without a name resolves to.
  primary @9 :Bool;
  # The entry's authoring content key: equal keys, equal values (§17
  # "Content keys"; protocol 29).
  content @10 :Data;
}

struct AuthoringListCall {
  union {
    success @0 :List(AuthoringRow);
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct SourceClaim {
  union {
    unclaimed @0 :Void;
    # The explicit import's bundle.
    explicit @1 :Data;
    rule :group {
      # Empty: the root's default layer.
      rulesBundle @2 :Data;
      rule @3 :Data;
      # "none": the rule claims the file and imports nothing.
      importer @4 :Text;
      output @5 :Text;
      # Empty: no bundle at the output.
      bundle @6 :Data;
    }
  }
}

struct SourceFile {
  root @0 :Text;
  path @1 :Text;
  claim @2 :SourceClaim;
}

struct SourceFilesCall {
  union {
    success @0 :List(SourceFile);
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

struct BundleRef {
  bundle @0 :Data;
  root @1 :Text;
  path @2 :Text;
}

struct BundleReferences {
  referencedBy @0 :List(BundleRef);
  references @1 :List(Text);
}

struct ReferencesCall {
  union {
    success @0 :BundleReferences;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
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
  # be dropped. Success is an encoded WriteReceipt: the files the write
  # changed on disk, which the store publishes through its watcher.
  # Protocol 13 (a version before).
  write @2 (base :UInt64, ops :List(AuthoringOp), forceLossy :Bool) -> (result :DataCall);
  import @3 (base :UInt64, request :ImportRequest) -> (result :UuidCall);
  reimport @4 (base :UInt64, bundle :Uuid) -> (result :UuidCall);
  operation @5 (base :UInt64, operation :LongRunningOp) -> (result :ProgressCall);
  wireTree @6 (layoutHash :Data) -> (result :DataCall);
  unsubscribe @7 (assets :List(Data), paths :List(Text))
              -> (result :VoidCall);
  authoringSnapshot @8 () -> (result :AuthoringSnapshotCall);
  # Make explicit import `bundle` follow the rule claiming its sources
  # again: re-imported under the rule's settings and origin, keeping its
  # bundle and entry UUIDs (§17 "Release"). Protocol 23.
  release @9 (base :UInt64, bundle :Uuid) -> (result :UuidCall);
}

interface Snapshot {
  version @0 () -> (result :UInt64Call);
  query @1 (query :AssetQuery) -> (result :UuidListCall);
  entry @2 (uuid :Data) -> (result :EntryMetaCall);
  # batch admits any build as batch work (a pack traversal) instead of
  # interactive; the answer is the same.
  resolve @3 (uuid :Data, batch :Bool) -> (result :ResolveCall);
  refresh @4 () -> (result :SnapshotCall);
  resolvePath @5 (path :Text) -> (result :PathResolveCall);
  configuration @6 () -> (result :VoidCall);
  fetch @7 (hash :Data) -> (result :ChunkStreamCall);
  runtimeTypePolicy @8 (typeUuid :Data) -> (result :RuntimeTypePolicyCall);
  # The runtime asset whose local id is `name` among the assets imported at
  # `path` (a bundle path). Protocol 12.
  resolveNamed @9 (path :Text, name :Text) -> (result :PathResolveCall);
  # The watched-import failures recorded now (memo state, not versioned
  # input, so clients poll it), each named by its bundle's root and path
  # at this snapshot; a failure whose bundle the snapshot lacks is left
  # out. Protocol 18 (on Hub, unnamed by any snapshot, since 10).
  importFailures @10 () -> (result :ImportFailuresCall);
}

struct RuntimeTypePolicy {
  buildOnly @0 :Bool;
}

struct RuntimeTypePolicyCall {
  union {
    success @0 :RuntimeTypePolicy;
    reconnectRequired @1 :ReconnectRequired;
    configurationFailed @2 :ConfigurationError;
    snapshotExpired @3 :Void;
    error @4 :RpcError;
  }
}

interface AuthoringSnapshot {
  version @0 () -> (result :UInt64Call);
  query @1 (query :AssetQuery) -> (result :UuidListCall);
  # Each uuid's inspection, in order; a bundle holding several of them is
  # read and parsed once. Values carry blob refs. Protocol 28 (one uuid
  # before).
  inspect @2 (uuids :List(Data)) -> (result :AuthoringInspectManyCall);
  refresh @3 () -> (result :AuthoringSnapshotCall);
  # The content hash of the file observed at path in root: 32 bytes, or
  # empty for no file there. A WriteReceipt is reflected at the first
  # version whose answers match it. Protocol 13.
  file @4 (root :Text, path :Text) -> (result :DataCall);
  # A game type's description, read at this snapshot from the type files
  # a schema walk and the game module build write (ngp_schema::prefab_types;
  # §17 "Type descriptions"). Protocol 20.
  describeType @5 (typeUuid :Data) -> (result :DescribeTypeCall);
  # The types the snapshot's type index lists. Protocol 20.
  describableTypes @6 () -> (result :DescribableTypesCall);
  # The entries `query` selects as rows: metadata, type name, the bundle's
  # origin; authoringOnly unset selects both roles. Protocol 23.
  list @7 (query :AssetQuery) -> (result :AuthoringListCall);
  # The files under `root` matching `glob` (empty: all), bundle files
  # excepted, each with the import layer claiming it. Protocol 23.
  sourceFiles @8 (root :Text, glob :Text) -> (result :SourceFilesCall);
  # A bundle's path reference edges, both ways. Protocol 23.
  references @9 (bundle :Data) -> (result :ReferencesCall);
  # The authored types the pipeline declares, with their defaults, and its
  # importers. Protocol 26.
  authorable @10 () -> (result :AuthorableCall);
  # Blob `index` of the entry's value table (the order of its refs), as
  # chunks. Protocol 28.
  blob @11 (uuid :Data, index :UInt32) -> (result :AuthoringBlobCall);
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
