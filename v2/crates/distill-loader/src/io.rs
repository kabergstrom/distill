//! The sole runtime IO boundary: RPC in development or pack files in shipping.

use std::sync::Arc;

use distill_build::trace::EntryRole;
use distill_core::id::{AssetUuid, ContentHash};
use distill_store::state::SnapshotStamp;
use distill_wire::exec::Blob;

use crate::basis::IoBasis;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReqId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTarget {
    pub epoch: crate::GameModuleEpoch,
    pub target_definition_hash: [u8; 32],
}

impl RuntimeTarget {
    pub fn new(epoch: crate::GameModuleEpoch, target_definition_hash: [u8; 32]) -> Self {
        Self {
            epoch,
            target_definition_hash,
        }
    }
}

/// A late-bound asset reference: a project path (a bundle path), and
/// optionally the name of one of the assets imported at it (the local id
/// its importer gave it). Without a name, the path's primary asset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AssetPath {
    pub path: String,
    pub name: Option<String>,
}

impl AssetPath {
    /// The primary asset at `path`.
    pub fn primary(path: &str) -> Self {
        Self {
            path: path.to_owned(),
            name: None,
        }
    }

    /// The asset named `name` among those imported at `path`.
    pub fn named(path: &str, name: &str) -> Self {
        Self {
            path: path.to_owned(),
            name: Some(name.to_owned()),
        }
    }
}

impl From<&str> for AssetPath {
    fn from(path: &str) -> Self {
        Self::primary(path)
    }
}

impl std::fmt::Display for AssetPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(name) => write!(f, "{} [{name}]", self.path),
            None => f.write_str(&self.path),
        }
    }
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
    RoleIneligible {
        uuid: AssetUuid,
        role: EntryRole,
    },
    Missing,
    Deleted {
        at: SnapshotStamp,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathResolveResult {
    Resolved(AssetUuid),
    Missing,
    Unsupported,
    Failed { error: String },
}

#[derive(Debug, Clone)]
pub struct FetchedArtifact {
    pub structural: Arc<[u8]>,
    pub blobs: Vec<Blob>,
    pub load_edges: Vec<distill_rpc::ServedLoadEdge>,
    /// Canonical DSWL body authenticated by the artifact header's
    /// `layout_hash`; LoaderIO resolves this before completing the fetch.
    pub wire_layout: Blob,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetDeltaState {
    Changed,
    Deleted,
    Restored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectReason {
    /// The connection to the daemon closed.
    ConnectionLost,
    StoreInstanceChanged,
    PipelineEpochChanged,
}

#[derive(Debug, Clone)]
pub enum IoEvent {
    Resolved {
        req: ReqId,
        uuid: AssetUuid,
        result: ResolveResult,
        basis: IoBasis,
    },
    PathResolved {
        req: ReqId,
        path: AssetPath,
        result: PathResolveResult,
        basis: IoBasis,
    },
    Fetched {
        req: ReqId,
        content_hash: ContentHash,
        artifact: FetchedArtifact,
        basis: IoBasis,
    },
    Delta {
        stamp: SnapshotStamp,
        assets: Vec<(AssetUuid, AssetDeltaState)>,
        paths: Vec<String>,
    },
    RequestError {
        req: ReqId,
        message: String,
        basis: IoBasis,
    },
    /// The request's snapshot expired, or an artifact it resolved left the
    /// daemon's cache: retry the round at a new snapshot.
    SnapshotExpired {
        req: ReqId,
        basis: IoBasis,
    },
    ConnectionError {
        message: String,
    },
    ReconnectRequired {
        reason: ReconnectReason,
    },
    TargetBound {
        target: RuntimeTarget,
        basis: IoBasis,
    },
    TargetRejected {
        message: String,
    },
}

/// The loader's only IO boundary (DESIGN.md §15, Plumbing).
///
/// Every method is called from the engine thread and returns without
/// waiting on IO: requests are answered later through [`Self::poll`], each
/// by exactly one request-terminal event unless its sweep ends first.
/// Implementations bound their own work by admission; they never park the
/// caller to apply backpressure.
pub trait LoaderIO {
    /// Drop everything issued so far and (re)bind the runtime target;
    /// answered by `TargetBound` or `TargetRejected`.
    fn bind_target(&mut self, target: RuntimeTarget);
    /// The basis of a new sweep: the newest basis the IO already holds.
    /// Never waits. Every event that makes the loader start a new round
    /// (a delta, a drift, an expired snapshot) is published only once the
    /// basis returned here answers it.
    fn begin_sweep(&mut self) -> IoBasis;
    /// The loader wants no further answers under `basis`: requests issued
    /// under it are cancelled (no event is delivered for them) and what the
    /// IO held for them is released.
    fn end_sweep(&mut self, basis: &IoBasis);
    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis);
    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis);
    /// Resolve `path` to an asset UUID: its path's primary, or the asset of
    /// that name. Answered by `IoEvent::PathResolved`.
    fn resolve_path(&mut self, req: ReqId, path: &AssetPath, basis: &IoBasis);
    fn subscribe(&mut self, uuid: AssetUuid);
    fn unsubscribe(&mut self, uuid: AssetUuid);
    fn subscribe_path(&mut self, path: &str);
    fn unsubscribe_path(&mut self, path: &str);
    fn poll(&mut self) -> Vec<IoEvent>;
}
