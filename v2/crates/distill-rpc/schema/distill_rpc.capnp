@0xbecdb9786a3f2e31;

# Wire-shaped declaration of the §17 surface. Fixed-size Data fields are
# length-checked by the Rust transport adapter (UUID=16, hashes=32, instance=16).

struct LayoutEntry {
  typeUuid @0 :Data;
  layoutDigest @1 :Data;
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

struct ConfigurationStatus {
  union {
    ready @0 :Void;
    poisoned @1 :ConfigurationPoison;
  }
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

struct RpcFailure {
  code @0 :UInt16;
  message @1 :Text;
}

struct CallStatus {
  union {
    ok @0 :Void;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    failure @3 :RpcFailure;
  }
}

struct ConnectResult {
  union {
    connected @0 :Connected;
    configurationPoisoned @1 :ConfigurationPoison;
    rejected @2 :RpcFailure;
  }
}

struct Connected {
  hub @0 :Hub;
  instance @1 :Data;
}

struct SnapshotResult {
  union {
    snapshot @0 :Snapshot;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    failure @3 :RpcFailure;
  }
}

struct TerminalResolve {
  basis @0 :SnapshotStamp;
  result @1 :ResolveResult;
}

struct ResolveCallResult {
  union {
    terminal @0 :TerminalResolve;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    failure @3 :RpcFailure;
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

struct PathCallResult {
  union {
    terminal @0 :TerminalPathResolve;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    failure @3 :RpcFailure;
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

struct FetchCallResult {
  union {
    terminal @0 :TerminalFetch;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    failure @3 :RpcFailure;
  }
}

struct SubscribeResult {
  union {
    installed @0 :Subscription;
    reconnectRequired @1 :ReconnectRequired;
    configurationPoisoned @2 :ConfigurationPoison;
    failure @3 :RpcFailure;
  }
}

struct Subscription {
  deltas @0 :DeltaStream;
  installed @1 :UInt64;
}

interface Root {
  connect @0 (target :Text, targetDefHash :Data,
              layoutRegistry :List(LayoutEntry), layoutAggregate :Data,
              loadPolicy :List(LoadPolicyEntry), policyDigest :Data,
              protocol :UInt32, gameModuleEpoch :UInt64)
          -> (result :ConnectResult);
}

interface Hub {
  snapshot @0 () -> (result :SnapshotResult);
  subscribe @1 (since :UInt64, assets :List(Data), paths :List(Text))
            -> (result :SubscribeResult);
  write @2 (base :UInt64, ops :Data) -> (status :CallStatus, version :UInt64);
  import @3 (base :UInt64, request :Data) -> (status :CallStatus, bundle :Data);
  reimport @4 (base :UInt64, bundle :Data) -> (status :CallStatus, result :Data);
  operation @5 (base :UInt64, operation :Data) -> (status :CallStatus);
  fetch @6 (hash :Data) -> (result :FetchCallResult);
  wireTree @7 (layoutHash :Data) -> (status :CallStatus, tree :Data);
  reattest @8 (epoch :UInt64, targetDefHash :Data,
               layoutRegistry :List(LayoutEntry), layoutAggregate :Data,
               loadPolicy :List(LoadPolicyEntry), policyDigest :Data)
           -> (result :CallStatus);
  unsubscribe @9 (assets :List(Data), paths :List(Text))
              -> (result :CallStatus);
}

interface Snapshot {
  version @0 () -> (stamp :SnapshotStamp);
  query @1 (query :Data) -> (status :CallStatus, uuids :List(Data));
  entry @2 (uuid :Data) -> (status :CallStatus, metadata :Data);
  resolve @3 (uuid :Data) -> (result :ResolveCallResult);
  refresh @4 () -> (result :SnapshotResult);
  reserved5 @5 () -> (status :CallStatus);
  reserved6 @6 () -> (status :CallStatus);
  reserved7 @7 () -> (status :CallStatus);
  reserved8 @8 () -> (status :CallStatus);
  reserved9 @9 () -> (status :CallStatus);
  resolvePath @10 (path :Text) -> (result :PathCallResult);
  configuration @11 () -> (state :ConfigurationStatus);
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
