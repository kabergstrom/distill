//! The client side of DESIGN.md §17's Cap'n Proto RPC: the generated
//! bindings of `schema/distill_rpc.capnp`, the connection
//! ([`capnp_transport::CapnpClient`]) and the typed loader, pack and
//! authoring calls ([`capnp_loader`]). The daemon's server (`distill-rpc`)
//! shares the bindings and the codecs; runtimes link this crate alone.

mod bind;
#[doc(hidden)]
pub mod distill_rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/distill_rpc_capnp.rs"));
}
pub mod capnp_loader;
pub mod capnp_transport;

pub use bind::{validate_bind_address, BindStageError};
pub use distill_protocol::*;
