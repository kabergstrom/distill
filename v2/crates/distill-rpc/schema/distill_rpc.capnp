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

struct ConfigurationPoison {
  code @0 :UInt16;
  reasonHash @1 :Data;
  message @2 :Text;
}

enum ReconnectReason {
  targetDefinitionChanged @0;
  loadPolicyChanged @1;
  storeInstanceChanged @2;
  protocolEpochChanged @3;
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

struct DigestMismatch {
  expected @0 :Data;
  observed @1 :Data;
}

struct TargetDefinitionSubject {
  union {
    unknownTarget @0 :Text;
    digestMismatch @1 :DigestMismatch;
  }
}

struct AttestationSubject {
  union {
    specificType @0 :Data;
    targetDefinition @1 :TargetDefinitionSubject;
    compiledRegistry @2 :Void;
    dscaAggregate @3 :Void;
    policyProjection @4 :Void;
  }
}

struct AttestationFailure {
  code @0 :UInt16;
  subject @1 :AttestationSubject;
  message @2 :Text;
}

struct ReattestSuccess {
  installedAttestationGeneration @0 :UInt64;
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
  }
}

struct ConnectSuccess {
  hub @0 :Hub;
  instance @1 :Data;
  policyGeneration @2 :UInt64;
  targetGeneration @3 :UInt64;
  attestationGeneration @4 :UInt64;
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
  next @0 () -> (done :Bool, progress :Data);
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
  }
}

struct UuidListCall {
  union {
    success @0 :List(Uuid);
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
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

struct AuthoringValue {
  canonicalValue @0 :Data;
  blobs @1 :List(Data);
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
  }
}

struct EntryMeta {
  bytes @0 :Data;
}

struct EntryMetaCall {
  union {
    success @0 :EntryMeta;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
  }
}

struct TerminalResolve {
  basis @0 :SnapshotStamp;
  result @1 :ResolveResult;
}

struct ResolveCall {
  union {
    success @0 :TerminalResolve;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
  }
}

struct ResolveResult {
  union {
    built @0 :Data;
    drifted @1 :Text;
    failed @2 :Text;
    missing @3 :Void;
    deleted @4 :SnapshotStamp;
    roleIneligible @5 :AuthoringRoleFailure;
  }
}

struct TerminalPathResolve {
  basis @0 :SnapshotStamp;
  result @1 :PathResolveResult;
}

struct PathResolveCall {
  union {
    success @0 :TerminalPathResolve;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
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
  basis @0 :SnapshotStamp;
  chunks @1 :ChunkStream;
}

struct ChunkStreamCall {
  union {
    success @0 :TerminalFetch;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    leaseFailure @3 :LeaseFailure;
    error @4 :RpcError;
  }
}

struct Subscription {
  deltas @0 :DeltaStream;
  installed @1 :UInt64;
}

interface Root {
  connect @0 (target :Text, targetDefHash :Data,
              compiledRegistry :List(CompiledTypeEntry), dscaAggregate :Data,
              loadPolicy :List(LoadPolicyEntry), policyDigest :Data,
              protocol :UInt32, gameModuleEpoch :UInt64)
          -> (result :ConnectCall);
}

interface Hub {
  snapshot @0 () -> (result :SnapshotCall);
  subscribe @1 (since :UInt64, assets :List(Data), paths :List(Text))
            -> (result :SubscribeCall);
  write @2 (base :UInt64, ops :Data) -> (result :UInt64Call);
  import @3 (base :UInt64, request :Data) -> (result :UuidCall);
  reimport @4 (base :UInt64, bundle :Data) -> (result :UuidCall);
  operation @5 (base :UInt64, operation :Data) -> (result :ProgressCall);
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
  stamp @0 :SnapshotStamp;
  assets @1 :List(AssetDelta);
  paths @2 :List(Text);
}

struct StreamEvent {
  basis @0 :SnapshotStamp;
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
