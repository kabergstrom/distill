//! §13 Daemon State & Storage: the SQLite metadata layer, the
//! log-structured content-addressed artifact store, and the state
//! machinery of the consistency contract.
//!
//! All daemon state is disposable (§2) and lives under `state_path`
//! (`.distill/`, §18). Segments are the durable record within daemon
//! state; every index is rebuildable by a segment scan. Version control
//! remains the durable archive.

pub mod artifacts;
pub mod bundles;
pub mod cas;
pub mod codegen;
pub mod config;
pub mod db;
pub mod error;
pub mod files;
pub mod imports;
pub mod journal;
pub mod pipeline;
pub mod state;

pub use config::{parse_byte_size, ByteSizeError, StoreConfig};
pub use db::{InputTxn, Store, StoreReader, SCHEMA_VERSION};
pub use error::{RetiredTypeReference, StoreError};
