//! The data both sides of DESIGN.md §17's RPC contract share: the protocol's
//! request, answer and event types, the store's §13 state types they carry
//! (`state`) and target definitions. Pure data: no store, transport or runtime,
//! so a runtime client links it without the daemon's crates.

pub mod codec;
mod rpc;
pub mod state;
mod target;

pub use rpc::*;
pub use target::{TargetDefinition, TargetSetError};

/// The section size a served artifact streams in (`fetch`, `blob`), and so
/// the unit a client sizes its read-ahead window by.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;
