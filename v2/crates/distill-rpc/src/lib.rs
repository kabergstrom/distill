//! DESIGN.md §17's Cap'n Proto RPC contract, server side.
//!
//! The crate keeps its typed state machine transport-neutral and serves it
//! over `capnp-rpc` in [`capnp_transport`], every connection on a thread of
//! its own. The protocol's data is `distill-protocol`'s; the bindings, the
//! codecs and the client are `distill-client`'s, re-exported here for the
//! daemon and its tests.

mod apply;
mod backend;
mod capability;
pub mod capnp_transport;
mod persist;
mod server;
mod validate;

pub use distill_client::{capnp_loader, distill_rpc_capnp, validate_bind_address, BindStageError};

pub use apply::{
    apply_commit, publish_pipeline_fence, publish_target_set, RETAINED_HISTORY_VERSIONS,
};
pub use capability::{
    AuthoringSnapshot, DeltaStream, FinishedBuild, Hub, MetadataAuthoringSnapshot, MetadataHub,
    MetadataSnapshot, PendingBuild, ResolveStep, Snapshot,
};
pub use backend::*;
pub use distill_protocol::*;
pub use server::{
    bundle_authoring_entry, target_map, CoordinatedCommitError, PendingScanRejection,
    ReportSnapshot, Root, Server, ServerHandle, SnapshotPolicy, DEFAULT_SNAPSHOT_TTL,
    MAX_SUBSCRIBED_ASSETS, MAX_SUBSCRIBED_PATHS,
};
pub use validate::{
    authoring_value, authoring_value_by_ref, check_authoring_value, complete_settings, content_key, content_key_of,
    decode_asset_reference_query, names_blob_ref, resolve_blob_refs, resolve_value_blob_refs, settings_value, stored_value, value_blob, value_blobs,
};
