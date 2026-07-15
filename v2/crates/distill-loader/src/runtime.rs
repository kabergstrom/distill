//! Client manifest transitions and request-generation fencing.

use std::collections::BTreeMap;

use distill_core::id::ContentHash;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestTransitionError {
    ChangedDeadWithoutRestoration,
    RestoredLiveAsset,
}

impl ManifestEntry {
    /// Apply a typed watch transition. A `Missing` entry has never resolved,
    /// so deletion cannot turn it into `Dead`; that state is client-relative.
    pub fn apply_delta(&mut self, delta: AssetDeltaState) -> Result<(), ManifestTransitionError> {
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
                ManifestState::Dead => {
                    return Err(ManifestTransitionError::ChangedDeadWithoutRestoration);
                }
            },
            AssetDeltaState::Deleted => {
                if !matches!(self.state, ManifestState::Missing) {
                    self.state = ManifestState::Dead;
                }
            }
            AssetDeltaState::Restored => {
                if !matches!(self.state, ManifestState::Dead) {
                    return Err(ManifestTransitionError::RestoredLiveAsset);
                }
                self.state = ManifestState::Missing;
            }
        }
        Ok(())
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
    Path(String),
    Content(ContentHash),
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
}

#[derive(Debug)]
pub struct RequestTracker {
    next: u64,
    connection: ConnectionEpoch,
    outstanding: BTreeMap<ReqId, OutstandingRequest>,
    newest: BTreeMap<RequestOwner, ReqId>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            next: 1,
            connection: ConnectionEpoch(1),
            outstanding: BTreeMap::new(),
            newest: BTreeMap::new(),
        }
    }

    pub fn connection_epoch(&self) -> ConnectionEpoch {
        self.connection
    }

    pub fn reconnect(&mut self) -> Result<ConnectionEpoch, RequestError> {
        self.connection.0 = self
            .connection
            .0
            .checked_add(1)
            .ok_or(RequestError::Exhausted)?;
        self.outstanding.clear();
        self.newest.clear();
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

    pub fn outstanding(&self, req: ReqId) -> Option<&OutstandingRequest> {
        self.outstanding.get(&req)
    }
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}
