//! DESIGN.md §17's Cap'n Proto RPC contract.
//!
//! The crate keeps its typed state machine transport-neutral, generates the
//! official Rust bindings from `schema/distill_rpc.capnp`, and adapts them to a
//! concrete single-threaded `capnp-rpc` system in [`capnp_transport`].

mod attestation;
mod bind;
#[doc(hidden)]
pub mod distill_rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/distill_rpc_capnp.rs"));
}
pub mod capnp_transport;
mod protocol;
mod server;

pub use attestation::{compute_policy_digest, AttestationShapeError, TargetDefinition};
pub use bind::{validate_bind_address, BindStageError};
pub use protocol::*;
pub use server::{
    decode_asset_reference_query, decode_authoring_payload, AuthoringSnapshot,
    CoordinatedCommitError, DeltaStream, Hub, LineageRepair, MetadataAuthoringSnapshot,
    MetadataHub, MetadataSnapshot, Root, Server, Snapshot,
};
