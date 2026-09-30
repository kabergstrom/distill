//! Everything a pipeline module links against.
//!
//! A module depends on this crate and nothing of the daemon: the callback
//! traits and descriptors, the registration arena, the module table and its
//! export macro, and the leaf types they carry. The daemon and
//! `distill-build` re-export these types at their old paths.

// The Rust-ABI module boundary transfers ownership of standard-library
// allocations in both directions. Installing System here makes the declared
// allocator contract a link-time fact for the daemon and every pipeline cdylib
// that consumes this interface; a second `#[global_allocator]` is rejected by
// rustc instead of being able to forge the ABI identity string.
#[global_allocator]
static DISTILL_SYSTEM_ALLOCATOR: std::alloc::System = std::alloc::System;

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
