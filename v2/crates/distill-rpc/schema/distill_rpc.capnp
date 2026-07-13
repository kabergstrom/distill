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

struct AttestationFailure {
  code @0 :UInt16;
  typeUuid @1 :Data;
  message @2 :Text;
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
           -> (result :UInt64Call);
  unsubscribe @9 (assets :List(Data), paths :List(Text))
              -> (result :VoidCall);
}

interface Snapshot {
  version @0 () -> (result :UInt64Call);
  query @1 (query :Data) -> (result :UuidListCall);
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
