@0xbecdb9786a3f2e31;

# Wire-shaped declaration of the §17 surface. Fixed-size Data fields are
# length-checked by the Rust transport adapter (UUID=16, hashes=32, instance=16).

struct CompiledTypeEntry {
  typeUuid @0 :Data;
  logicalHash @1 :Data;
  nativeLayoutDigest @2 :Data;
  buildOnly @3 :Bool;
  registryExtrasDigest @4 :Data;
  registryExtras @5 :Data;
}

struct LoadPolicyEntry {
  typeUuid @0 :Data;
  buildOnly @1 :Bool;
}

struct SnapshotStamp {
  instance @0 :Data;
  version @1 :UInt64;
}

struct RpcBasisValue {
  stamp @0 :SnapshotStamp;
  loadPolicy @1 :List(LoadPolicyEntry);
  policyDigest @2 :Data;
  policyGeneration @3 :UInt64;
  targetGeneration @4 :UInt64;
  attestationGeneration @5 :UInt64;
  daemonCompiledProjection @6 :Data;
}

struct ConfigurationPoison {
  code @0 :UInt16;
  reasonHash @1 :Data;
  message @2 :Text;
  detailVersion @3 :UInt16;
  detailBytes @4 :Data;
}

enum ReconnectReason {
  targetDefinitionChanged @0;
  loadPolicyChanged @1;
  storeInstanceChanged @2;
  protocolEpochChanged @3;
  compiledAttestationChanged @4;
}

struct ReconnectRequired {
  reason @0 :ReconnectReason;
}

struct LeaseFailure {
  code @0 :UInt16;
  message @1 :Text;
}

struct RpcError {
  code @0 :UInt16;
  message @1 :Text;
}

enum AttestationFailureCode {
  malformedTable @0;
  duplicateType @1;
  missingType @2;
  logicalHashMismatch @3;
  nativeLayoutMismatch @4;
  buildOnlyMismatch @5;
  registryExtrasMismatch @6;
  compiledRegistryAggregateMismatch @7;
  targetDefinitionMismatch @8;
  policyProjectionMismatch @9;
  bootstrapAuthorityMismatch @10;
  malformedField @11;
}

enum AttestationProjection {
  compiledRegistry @0;
  policy @1;
}

struct TypeAttestationSubject {
  typeUuid @0 :Uuid;
  projection @1 :AttestationProjection;
}

struct ExpectedObserved {
  expected @0 :Data;
  observed @1 :Data;
}

struct AttestationTableDetail {
  index @0 :UInt32;
  entry @1 :Data;
}

struct MalformedFieldDetail {
  expectedWidth @0 :UInt32;
  observed @1 :Data;
}

struct AttestationFailurePayload {
  union {
    none @0 :Void;
    expectedObserved @1 :ExpectedObserved;
    tableDetail @2 :AttestationTableDetail;
    malformedField @3 :MalformedFieldDetail;
  }
}

struct AttestationFixedFieldSubject {
  union {
    targetDefHash @0 :Void;
    dscaAggregate @1 :Void;
    policyDigest @2 :Void;
    compiledTypeUuid @3 :UInt32;
    compiledLogicalHash @4 :UInt32;
    compiledNativeLayoutDigest @5 :UInt32;
    compiledRegistryExtrasDigest @6 :UInt32;
    policyTypeUuid @7 :UInt32;
  }
}

struct AttestationSubject {
  union {
    specificType @0 :TypeAttestationSubject;
    targetDefinition @1 :Void;
    compiledRegistryTable @2 :Void;
    compiledRegistryAggregate @3 :Void;
    policyProjection @4 :Void;
    bootstrapAuthority @5 :Void;
    fixedField @6 :AttestationFixedFieldSubject;
  }
}

struct AttestationFailure {
  code @0 :AttestationFailureCode;
  subject @1 :AttestationSubject;
  payload @2 :AttestationFailurePayload;
  message @3 :Text;
}

struct ReattestSuccess {
  installedAttestationGeneration @0 :UInt64;
  daemonCompiledProjection @1 :Data;
  loadPolicy @2 :List(LoadPolicyEntry);
  policyDigest @3 :Data;
  policyGeneration @4 :UInt64;
}

struct AttestationExpansionRequired {
  snapshot @0 :SnapshotStamp;
  closureIdentity @1 :Data;
  requiredTypeUuids @2 :List(Data);
}

struct StaleAttestationBase {
  code @0 :UInt16;
  expected @1 :UInt64;
  observed @2 :UInt64;
}

struct AttestationGenerationOverflow {
  code @0 :UInt16;
  base @1 :UInt64;
}

struct ReattestResult {
  union {
    success @0 :ReattestSuccess;
    attestationFailure @1 :AttestationFailure;
    staleAttestationBase @2 :StaleAttestationBase;
    attestationGenerationOverflow @3 :AttestationGenerationOverflow;
    reconnectRequired @4 :ReconnectRequired;
    configurationPoisoned @5 :ConfigurationPoison;
    leaseFailure @6 :LeaseFailure;
    error @7 :RpcError;
  }
}

struct ProtocolFailure {
  expected @0 :UInt32;
  observed @1 :UInt32;
  message @2 :Text;
}

struct ConnectCall {
  union {
    success @0 :ConnectSuccess;
    attestationFailure @1 :AttestationFailure;
    configurationPoisoned @2 :ConfigurationPoison;
    protocolFailure @3 :ProtocolFailure;
    error @4 :RpcError;
    pipelineUnavailable @5 :PipelineUnavailableDiagnostic;
  }
}

struct PipelineUnavailableDiagnostic {
  union {
    pipelinePoison @0 :PipelinePoison;
    schemaAcceptanceRequired @1 :SchemaAcceptanceRequired;
    retiredTypeReferenced @2 :RetiredTypeReferenced;
  }
}

struct ConnectSuccess {
  hub @0 :Hub;
  instance @1 :Data;
  policyGeneration @2 :UInt64;
  targetGeneration @3 :UInt64;
  attestationGeneration @4 :UInt64;
  daemonCompiledProjection @5 :Data;
  loadPolicy @6 :List(LoadPolicyEntry);
  policyDigest @7 :Data;
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
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
  }
}

struct MetadataAuthoringSnapshotCall {
  union {
    success @0 :MetadataAuthoringSnapshot;
    reconnectRequired @1 :MetadataReconnectRequired;
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
  }
}

struct MetadataDiagnosticsCall {
  union {
    success @0 :MetadataDiagnostics;
    reconnectRequired @1 :MetadataReconnectRequired;
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
  }
}

struct MetadataUInt64Call {
  union {
    success @0 :UInt64;
    reconnectRequired @1 :MetadataReconnectRequired;
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
  }
}

struct MetadataChunkStreamCall {
  union {
    success @0 :ChunkStream;
    reconnectRequired @1 :MetadataReconnectRequired;
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
  }
}

struct MetadataUuidListCall {
  union {
    success @0 :List(Uuid);
    reconnectRequired @1 :MetadataReconnectRequired;
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
    versionPoisoned @4 :VersionPoison;
  }
}

struct MetadataEntryMetaCall {
  union {
    success @0 :PureMetadataEntry;
    reconnectRequired @1 :MetadataReconnectRequired;
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
    versionPoisoned @4 :VersionPoison;
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
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
    versionPoisoned @4 :VersionPoison;
  }
}

struct MetadataAuthoringInspectCall {
  union {
    success @0 :AuthoringInspection;
    reconnectRequired @1 :MetadataReconnectRequired;
    leaseFailure @2 :LeaseFailure;
    error @3 :RpcError;
    versionPoisoned @4 :VersionPoison;
    missing @5 :Void;
    roleIneligible @6 :AuthoringRoleFailure;
  }
}

struct SnapshotCall {
  union {
    success @0 :Snapshot;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
  }
}

struct AuthoringSnapshotCall {
  union {
    success @0 :AuthoringSnapshot;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
  }
}

struct SubscribeCall {
  union {
    success @0 :Subscription;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
  }
}

struct VoidCall {
  union {
    success @0 :Void;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
  }
}

struct UInt64Call {
  union {
    success @0 :UInt64;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
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
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
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
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
  }
}

struct DataCall {
  union {
    success @0 :Data;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
    attestationExpansionRequired @5 :AttestationExpansionRequired;
  }
}

struct UuidListCall {
  union {
    success @0 :List(Uuid);
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
    versionPoisoned @5 :VersionPoison;
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

struct VersionPoisonSource {
  rootName @0 :Text;
  normalizedPath @1 :Text;
  fileHash @2 :Data;
}

struct AuthoredAssetClaimant {
  source @0 :VersionPoisonSource;
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

struct DuplicateAssetPoison {
  asset @0 :Uuid;
  claimants @1 :List(AssetClaimant);
}

struct DuplicateBundlePoison {
  bundle @0 :Uuid;
  sources @1 :List(VersionPoisonSource);
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

struct SameRootNormalizedPathPoison {
  rootName @0 :Text;
  normalizedPath @1 :Text;
  claims @2 :List(PhysicalPathClaim);
}

struct IncompleteSkeletonPoison {
  source @0 :VersionPoisonSource;
  failureCode @1 :UInt16;
}

struct UnreadableGlobalPathPoison {
  rootName @0 :Text;
  normalizedPath @1 :Text;
  failureCode @2 :UInt16;
}

struct InvalidPhysicalPathPoison {
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

struct UnreadableScanSubtreePoison {
  subject @0 :ScanSubject;
  failure @1 :ScanFailureCodeValue;
}

struct VersionPoisonDetail {
  union {
    duplicateAssetUuid @0 :DuplicateAssetPoison;
    duplicateBundleUuid @1 :DuplicateBundlePoison;
    sameRootNormalizedPathCollision @2 :SameRootNormalizedPathPoison;
    incompleteSkeleton @3 :IncompleteSkeletonPoison;
    unreadableGlobalBundlePath @4 :UnreadableGlobalPathPoison;
    invalidPhysicalPath @5 :InvalidPhysicalPathPoison;
    unreadableScanSubtree @6 :UnreadableScanSubtreePoison;
  }
}

struct VersionPoison {
  code @0 :UInt16;
  identity @1 :Data;
  detail @2 :VersionPoisonDetail;
  message @3 :Text;
}

struct ConfigurationDiagnostic {
  union {
    ready @0 :Void;
    poisoned @1 :ConfigurationPoison;
  }
}

enum PipelinePoisonCode {
  candidateOpen @0;
  candidateAttestation @1;
  candidateRegistration @2;
  candidateValidation @3;
  candidateCleanup @4;
  publishedCallbackPanic @5;
  publishedCallbackRejected @6;
  publishedCleanup @7;
}

enum PipelinePoisonOrigin {
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

struct PipelinePoison {
  code @0 :PipelinePoisonCode;
  origin @1 :PipelinePoisonOrigin;
  cleanup @2 :CleanupDisposition;
  identity @3 :Data;
  message @4 :Text;
}

struct SchemaManifestCursor {
  typeUuid @0 :Uuid;
  logicalHash @1 :Data;
}

struct SchemaManifestBasis {
  manifestHash @0 :Data;
  currentCursors @1 :List(SchemaManifestCursor);
}

struct PipelineCandidateIdentity {
  dylibHash @0 :Data;
  compiledTypes @1 :Data;
  targetSetHash @2 :Data;
}

struct SchemaRegistryMismatch {
  typeUuid @0 :Uuid;
  candidate @1 :Data;
  hasCandidate @2 :Bool;
  manifest @3 :Data;
  hasManifest @4 :Bool;
}

struct SchemaAcceptanceRequired {
  manifest @0 :SchemaManifestBasis;
  candidate @1 :PipelineCandidateIdentity;
  mismatches @2 :List(SchemaRegistryMismatch);
}

struct RetiredTypeReference { union {
  asset @0 :Uuid;
  migrationEndpoint @1 :Data;
} }

struct RetiredTypeReferenced {
  manifestHash @0 :Data;
  basis @1 :SnapshotStampValue;
  typeUuid @2 :Uuid;
  references @3 :List(RetiredTypeReference);
}

struct PipelineDiagnostic {
  union {
    ready @0 :Void;
    poisoned @1 :PipelinePoison;
    schemaAcceptanceRequired @2 :SchemaAcceptanceRequired;
    retiredTypeReferenced @3 :RetiredTypeReferenced;
  }
}

struct VersionPoisonDiagnostic {
  union {
    healthy @0 :Void;
    poisoned @1 :VersionPoison;
  }
}

struct MetadataDiagnostics {
  stamp @0 :SnapshotStampValue;
  configuration @1 :ConfigurationDiagnostic;
  pipeline @2 :PipelineDiagnostic;
  versionPoison @3 :VersionPoisonDiagnostic;
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
    diskMigration @1 :Data;
    doctor @2 :Data;
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
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
    missing @5 :Void;
    roleIneligible @6 :AuthoringRoleFailure;
    versionPoisoned @7 :VersionPoison;
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
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
    versionPoisoned @5 :VersionPoison;
    attestationExpansionRequired @6 :AttestationExpansionRequired;
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
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
    versionPoisoned @5 :VersionPoison;
    attestationExpansionRequired @6 :AttestationExpansionRequired;
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
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
    versionPoisoned @5 :VersionPoison;
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
}

struct ChunkStreamCall {
  union {
    success @0 :TerminalFetch;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
    attestationExpansionRequired @5 :AttestationExpansionRequired;
  }
}

struct Subscription {
  deltas @0 :DeltaStream;
  installed @1 :UInt64;
}

struct LineageManifestClaimant {
  asset @0 :Uuid;
  rootName @1 :Text;
  normalizedPath @2 :Text;
  fileHash @3 :Data;
  bundle @4 :Uuid;
  localId @5 :Text;
}

enum OccupiedLineageDestinationKind {
  canonicalBundle @0;
  opaque @1;
}

struct OccupiedLineageDestination {
  fileHash @0 :Data;
  kind @1 :OccupiedLineageDestinationKind;
}

struct LineageRepairDestination {
  union {
    absent @0 :Void;
    occupied @1 :OccupiedLineageDestination;
  }
}

struct MissingLineageRepairState {
  configuredRoot @0 :Text;
  configuredPath @1 :Text;
  destination @2 :LineageRepairDestination;
}

struct DuplicateLineageRepairState {
  claimants @0 :List(LineageManifestClaimant);
}

struct LineageRepairState {
  union {
    missing @0 :MissingLineageRepairState;
    duplicate @1 :DuplicateLineageRepairState;
  }
}

struct LineageRepairInspection {
  instance @0 :Data;
  stamp @1 :SnapshotStamp;
  state @2 :LineageRepairState;
}

struct LineageRepairUnavailable {
  union {
    configurationReady @0 :Void;
    otherConfigurationPoison @1 :ConfigurationPoison;
  }
}

struct LineageRepairConnectResult {
  union {
    success @0 :LineageRepair;
    unavailable @1 :LineageRepairUnavailable;
    protocolFailure @2 :ProtocolFailure;
    error @3 :RpcError;
  }
}

enum LineageRepairInvalidCode {
  wrongBasisState @0;
  nonCanonicalBundle @1;
  missingManifestEntry @2;
  notAuthoringOnly @3;
  bootstrapTypePresent @4;
  invalidLineage @5;
  survivorNotClaimant @6;
}

struct LineageRepairInvalid {
  code @0 :LineageRepairInvalidCode;
  message @1 :Text;
}

enum LineageRepairStaleCode {
  stampChanged @0;
  stateChanged @1;
  destinationAppeared @2;
  claimantChanged @3;
  preimageChanged @4;
}

struct LineageRepairStale {
  code @0 :LineageRepairStaleCode;
  observedStamp @1 :SnapshotStamp;
}

struct LineageRepairCommitted {
  stamp @0 :SnapshotStamp;
}

struct LineageRepairInspectResult {
  union {
    success @0 :LineageRepairInspection;
    unavailable @1 :LineageRepairUnavailable;
    reconnectRequired @2 :MetadataReconnectRequired;
    error @3 :RpcError;
  }
}

struct LineageRepairMutationResult {
  union {
    success @0 :LineageRepairCommitted;
    staleBasis @1 :LineageRepairStale;
    invalid @2 :LineageRepairInvalid;
    unavailable @3 :LineageRepairUnavailable;
    reconnectRequired @4 :MetadataReconnectRequired;
    error @5 :RpcError;
  }
}

interface LineageRepair {
  inspect @0 () -> (result :LineageRepairInspectResult);
  createMissing @1 (basis :LineageRepairInspection,
                    canonicalManifestBundle :Data)
                -> (result :LineageRepairMutationResult);
  resolveDuplicate @2 (basis :LineageRepairInspection,
                       survivor :LineageManifestClaimant)
                -> (result :LineageRepairMutationResult);
}

interface Root {
  connect @0 (target :Text, targetDefHash :Data,
              compiledRegistry :List(CompiledTypeEntry), dscaAggregate :Data,
              loadPolicy :List(LoadPolicyEntry), policyDigest :Data,
              protocol :UInt32, gameModuleEpoch :UInt64)
          -> (result :ConnectCall);
  metadata @1 (protocol :UInt32) -> (result :MetadataConnectResult);
  lineageRepair @2 (protocol :UInt32) -> (result :LineageRepairConnectResult);
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
  write @2 (base :UInt64, ops :List(AuthoringOp)) -> (result :UInt64Call);
  import @3 (base :UInt64, request :ImportRequest) -> (result :UuidCall);
  reimport @4 (base :UInt64, bundle :Uuid) -> (result :UuidCall);
  operation @5 (base :UInt64, operation :LongRunningOp) -> (result :ProgressCall);
  fetch @6 (hash :Data) -> (result :ChunkStreamCall);
  wireTree @7 (layoutHash :Data) -> (result :DataCall);
  reattest @8 (epoch :UInt64,
               baseAttestationGeneration :UInt64,
               successorAttestationGeneration :UInt64,
               targetDefHash :Data,
               compiledRegistry :List(CompiledTypeEntry), dscaAggregate :Data,
               loadPolicy :List(LoadPolicyEntry), policyDigest :Data)
           -> (result :ReattestResult);
  unsubscribe @9 (assets :List(Data), paths :List(Text))
              -> (result :VoidCall);
  authoringSnapshot @10 () -> (result :AuthoringSnapshotCall);
}

interface Snapshot {
  version @0 () -> (result :UInt64Call);
  query @1 (query :AssetQuery) -> (result :UuidListCall);
  entry @2 (uuid :Data) -> (result :EntryMetaCall);
  resolve @3 (uuid :Data) -> (result :ResolveCall);
  refresh @4 () -> (result :SnapshotCall);
  reserved5 @5 () -> (result :VoidCall);
  reserved6 @6 () -> (result :VoidCall);
  reserved7 @7 () -> (result :VoidCall);
  reserved8 @8 () -> (result :VoidCall);
  reserved9 @9 () -> (result :VoidCall);
  resolvePath @10 (path :Text) -> (result :PathResolveCall);
  configuration @11 () -> (result :VoidCall);
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
