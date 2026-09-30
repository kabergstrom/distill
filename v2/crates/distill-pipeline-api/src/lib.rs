//! Everything a pipeline module links against.
//!
//! A module depends on this crate and nothing of the daemon: the callback
//! traits and descriptors, the registration arena, the module table and its
//! export macro, and the leaf types they carry. The daemon and
//! `distill-build` re-export these types at their old paths.

// The Rust-ABI module boundary transfers ownership of standard-library
// allocations in both directions, so the daemon and every module must use
// the System allocator. `export_pipeline_module_v2!` installs it in the
// module and `distilld` in the daemon; a library linking this crate (the
// engine's loader, through distill-build) keeps its own choice.

pub mod callbacks;
pub mod codegen;
pub mod failure;
pub mod import;
pub mod importer;
pub mod module;
pub mod outputs;
pub mod query;
pub mod registration;
pub mod target;
pub mod tool;
