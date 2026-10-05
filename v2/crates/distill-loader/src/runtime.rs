//! Client manifest transitions and request-generation fencing.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, ContentHash};
use distill_store::state::SnapshotStamp;

use crate::basis::IoBasis;
use crate::io::{AssetDeltaState, ReqId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AdoptionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HandleId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub state: ManifestState,
    pub adopted_at: AdoptionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestState {
    Current {
        content_hash: ContentHash,
    },
    Invalidated {
        last: ContentHash,
    },
    StaleLastGood {
        content_hash: ContentHash,
        error: String,
        built_from: SnapshotStamp,
    },
    Missing,
    Dead,
}

impl ManifestEntry {
    /// Apply a typed watch transition. A `Missing` entry has never resolved,
    /// so deletion cannot turn it into `Dead`; that state is client-relative.
    pub fn apply_delta(&mut self, delta: AssetDeltaState) {
        match delta {
            AssetDeltaState::Changed => match &self.state {
                ManifestState::Current { content_hash } => {
                    self.state = ManifestState::Invalidated {
                        last: *content_hash,
                    };
                }
                ManifestState::StaleLastGood { content_hash, .. } => {
                    self.state = ManifestState::Invalidated {
                        last: *content_hash,
                    };
                }
                ManifestState::Invalidated { .. } | ManifestState::Missing => {}
                // The daemon publishes a returning asset as Changed: it has
                // no last-good hash, and resolves afresh.
                ManifestState::Dead => self.state = ManifestState::Missing,
            },
            AssetDeltaState::Deleted => {
                if !matches!(self.state, ManifestState::Missing) {
                    self.state = ManifestState::Dead;
                }
            }
        }
    }

    /// Map an absent resolve using client history. An entry that ever held a
    /// last-good hash becomes `Dead` even when daemon state loss downgraded a
    /// server-side `Deleted` answer to `Missing`.
    pub fn observe_absence(&mut self) {
        if !matches!(self.state, ManifestState::Missing) {
            self.state = ManifestState::Dead;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionEpoch(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RequestOwner {
    Handle(HandleId),
    Path(crate::io::AssetPath),
    Content { asset: AssetUuid, hash: ContentHash },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutstandingPurpose {
    Resolve,
    ResolvePath,
    Fetch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutstandingRequest {
    pub owner: RequestOwner,
    pub purpose: OutstandingPurpose,
    pub basis: IoBasis,
    pub connection: ConnectionEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDisposition {
    Accepted,
    UnknownOrRetired,
    Superseded,
    WrongConnection,
    WrongBasis,
    /// The loader ended the request's sweep (`retire_basis`) in this
    /// `process`; its answer was already delivered.
    Cancelled,
}

#[derive(Debug)]
pub struct RequestTracker {
    next: u64,
    connection: ConnectionEpoch,
    outstanding: BTreeMap<ReqId, OutstandingRequest>,
    newest: BTreeMap<RequestOwner, ReqId>,
    /// Requests retired by `retire_basis` since `forget_cancelled`.
    cancelled: BTreeSet<ReqId>,
    /// Requests the loader stopped wanting while their basis stays open
    /// (a delta keeps the old basis for its in-flight fetches, doc 22 §5):
    /// their answers still arrive and are dropped as `Cancelled`.
    dropped: BTreeMap<ReqId, IoBasis>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            next: 1,
            connection: ConnectionEpoch(1),
            outstanding: BTreeMap::new(),
            newest: BTreeMap::new(),
            cancelled: BTreeSet::new(),
            dropped: BTreeMap::new(),
        }
    }

    pub fn reconnect(&mut self) -> Result<ConnectionEpoch, RequestError> {
        self.connection.0 = self
            .connection
            .0
            .checked_add(1)
            .ok_or(RequestError::Exhausted)?;
        self.outstanding.clear();
        self.newest.clear();
        self.cancelled.clear();
        self.dropped.clear();
        Ok(self.connection)
    }

    pub fn issue(
        &mut self,
        owner: RequestOwner,
        purpose: OutstandingPurpose,
        basis: IoBasis,
    ) -> Result<ReqId, RequestError> {
        let req = ReqId(self.next);
        self.next = self.next.checked_add(1).ok_or(RequestError::Exhausted)?;
        let record = OutstandingRequest {
            owner: owner.clone(),
            purpose,
            basis,
            connection: self.connection,
        };
        self.outstanding.insert(req, record);
        self.newest.insert(owner, req);
        Ok(req)
    }

    /// Consume a terminal completion only when its generation, connection,
    /// and basis all match. Every rejected entry is retired as well: a late
    /// event can never become acceptable after another state transition.
    pub fn complete(&mut self, req: ReqId, event_basis: &IoBasis) -> CompletionDisposition {
        let Some(record) = self.outstanding.remove(&req) else {
            if self.cancelled.remove(&req) || self.dropped.remove(&req).is_some() {
                return CompletionDisposition::Cancelled;
            }
            return CompletionDisposition::UnknownOrRetired;
        };
        if record.connection != self.connection {
            return CompletionDisposition::WrongConnection;
        }
        if self.newest.get(&record.owner) != Some(&req) {
            return CompletionDisposition::Superseded;
        }
        if &record.basis != event_basis {
            return CompletionDisposition::WrongBasis;
        }
        self.newest.remove(&record.owner);
        CompletionDisposition::Accepted
    }

    /// Retire every request issued under `basis`: the IO cancelled them, so
    /// no completion arrives for them after the current batch.
    pub fn retire_basis(&mut self, basis: &IoBasis) {
        let cancelled = &mut self.cancelled;
        self.outstanding.retain(|req, record| {
            let keep = &record.basis != basis;
            if !keep {
                cancelled.insert(*req);
            }
            keep
        });
        self.dropped.retain(|req, dropped| {
            let keep = dropped != basis;
            if !keep {
                cancelled.insert(*req);
            }
            keep
        });
        let outstanding = &self.outstanding;
        self.newest.retain(|_, req| outstanding.contains_key(req));
    }

    /// Stop wanting every request under `basis` except its fetches, which
    /// stay outstanding: the basis stays open for them.
    pub fn drop_basis_except_fetches(&mut self, basis: &IoBasis) {
        let dropped = &mut self.dropped;
        self.outstanding.retain(|req, record| {
            let keep = &record.basis != basis || record.purpose == OutstandingPurpose::Fetch;
            if !keep {
                dropped.insert(*req, record.basis.clone());
            }
            keep
        });
        let outstanding = &self.outstanding;
        self.newest.retain(|_, req| outstanding.contains_key(req));
    }

    /// The (asset, content hash) of each fetch outstanding under `basis`.
    pub fn fetches_under<'a>(
        &'a self,
        basis: &'a IoBasis,
    ) -> impl Iterator<Item = (AssetUuid, ContentHash)> + 'a {
        self.outstanding
            .values()
            .filter(move |record| &record.basis == basis)
            .filter_map(|record| match record.owner {
                RequestOwner::Content { asset, hash } => Some((asset, hash)),
                _ => None,
            })
    }

    /// Forget the requests retired before this batch: their answers can no
    /// longer arrive.
    pub fn forget_cancelled(&mut self) {
        self.cancelled.clear();
    }

    pub fn outstanding(&self, req: ReqId) -> Option<&OutstandingRequest> {
        self.outstanding.get(&req)
    }
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}
