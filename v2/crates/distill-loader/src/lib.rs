//! Runtime loading contracts from DESIGN.md §15.
//!
//! This crate keeps IO bases explicit, computes atomic adoption components
//! over the union of old and candidate dependency graphs, rejects stale
//! completions by request and connection epoch, and makes module poisoning
//! observable at the storage boundary.
//!
//! Feature `rpc` (default): the daemon connection, [`RpcIo`] and its
//! authoring calls, over `distill-client` and tokio. Without it the loader
//! reads packs only (`distill-pack`'s `PackfileIO`).

pub mod admission;
#[cfg(feature = "rpc")]
pub mod authoring;
pub mod basis;
#[cfg(feature = "rpc")]
pub mod capnp_io;
pub mod component;
pub mod io;
pub mod layered;
pub mod orchestrator;
#[cfg(feature = "rpc")]
mod rpc_decode;
pub mod runtime;
pub mod stats;
pub mod storage;

pub use admission::{Admit, FetchAdmission, Reservation};
#[cfg(feature = "rpc")]
pub use authoring::{AuthoringAnswer, AuthoringRequest, IndexChange};
pub use basis::{IoBasis, ManifestHash};
#[cfg(feature = "rpc")]
pub use capnp_io::{
    RpcIo, RpcIoConfig, RpcIoInitError, RpcIoStats, RpcIoStatus, StepStats, TypeName,
};
pub use component::{
    dependency_order, load_cycles, AdoptionDecision, CandidateAsset, CandidateOutcome, ComponentPlanner,
    MemberFailure,
};
pub use io::{
    AssetDeltaState, AssetPath, DriftedInput, FetchedArtifact, IoEvent, LoaderIO,
    PathResolveResult, ReconnectReason, ReqId, ResolveResult, RuntimeTarget,
};
pub use layered::{Layer, LayeredIO};
pub use orchestrator::{
    Handle, LoadStatus, Loader, LoaderDiagnostic, LoaderError, RegistrationError,
    TargetBindingState,
};
pub use runtime::{
    AdoptionId, CompletionDisposition, ConnectionEpoch, HandleId, ManifestEntry, ManifestState,
    OutstandingPurpose, RequestOwner, RequestTracker,
};
pub use stats::{
    AcceptParts, AssetTimeline, FetchTiming, LoaderStats, LoaderTotals, Mark, Outcome, Stage,
    StageAggregate,
};
pub use storage::{
    AssetStorage, GameModuleEpoch, PendingState, PendingToken, RuntimeEpochError, RuntimeEpochs,
    StorageError, StoredAdoption, UpdateResult,
};
