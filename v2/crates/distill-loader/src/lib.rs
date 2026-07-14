//! Runtime loading contracts from DESIGN.md §15.
//!
//! This crate keeps IO bases explicit, computes atomic adoption components
//! over the union of old and candidate dependency graphs, rejects stale
//! completions by request and connection epoch, and makes module poisoning
//! observable at the storage boundary.

pub mod admission;
pub mod basis;
pub mod component;
pub mod io;
pub mod orchestrator;
pub mod rpc_io;
pub mod runtime;
pub mod storage;

pub use admission::{Admission, FetchAdmission};
pub use basis::{IoBasis, LoadPolicyAttestation, LoadPolicyError, LoadPolicyRow, ManifestHash};
pub use component::{
    AdoptionDecision, CandidateAsset, CandidateOutcome, ComponentPlanner, MemberFailure,
};
pub use io::{
    AssetDeltaState, DriftedInput, FetchedArtifact, IoEvent, LoaderIO, PathResolveResult,
    ReconnectReason, ReqId, ResolveResult,
};
pub use orchestrator::{
    FetchedInput, Handle, LoadStatus, Loader, LoaderDiagnostic, LoaderError, PreparedValue,
    ReattestationState, RegistrationError,
};
pub use rpc_io::{RpcIo, RpcIoInitError};
pub use runtime::{
    AdoptionId, CompletionDisposition, ConnectionEpoch, HandleId, ManifestEntry, ManifestState,
    OutstandingPurpose, RequestOwner, RequestTracker,
};
pub use storage::{
    AssetStorage, GameModuleEpoch, PendingState, PendingToken, RuntimeEpochError, RuntimeEpochs,
    StorageError, StoredAdoption, UpdateResult,
};
