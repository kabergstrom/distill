//! DESIGN.md §17's Cap'n Proto RPC contract.
//!
//! The crate keeps its typed state machine transport-neutral, generates the
//! official Rust bindings from `schema/distill_rpc.capnp`, and adapts them to a
//! concrete single-threaded `capnp-rpc` system in [`capnp_transport`].

mod bind;
#[doc(hidden)]
pub mod distill_rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/distill_rpc_capnp.rs"));
}
pub mod capnp_loader;
pub mod capnp_transport;
mod apply;
mod capability;
mod persist;
mod protocol;
mod server;
mod validate;
mod target;

pub use bind::{validate_bind_address, BindStageError};
pub use protocol::*;
pub use apply::{
    apply_commit, apply_commit_served, publish_protocol_epoch, publish_restart_required, publish_runtime_pipeline_failure,
    publish_target, publish_target_set, ApplyError, ApplyMode, RETAINED_HISTORY_VERSIONS,
};
pub use capability::{
    AuthoringSnapshot, DeltaStream, FinishedBuild, Hub, MetadataAuthoringSnapshot, MetadataHub,
    MetadataSnapshot, PackSession, PendingBuild, ResolveStep, Snapshot, import_call_outcome,
    write_call_outcome,
};
pub use server::{
    target_map, run_scoped, AuthorityCall, AuthorityJob, AuthorityStopped, CoordinatedCommitError, ExternalStore, LeasePolicy, Root, Server, ServerHandle,
    MAX_SUBSCRIBED_ASSETS, MAX_SUBSCRIBED_PATHS,
};
pub use validate::{decode_asset_reference_query, decode_authoring_payload};
pub use target::{TargetDefinition, TargetSetError};
