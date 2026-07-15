//! Long-lived daemon infrastructure: pipeline-module epochs, cooperative
//! scheduling, code-loading policy, and displaced-inode quarantine.

// The Rust-ABI module boundary transfers ownership of standard-library
// allocations in both directions. Installing System here makes the declared
// allocator contract a link-time fact for the daemon and every pipeline cdylib
// that consumes this interface; a second `#[global_allocator]` is rejected by
// rustc instead of being able to forge the ABI identity string.
#[global_allocator]
static DISTILL_SYSTEM_ALLOCATOR: std::alloc::System = std::alloc::System;

pub mod authoring;
mod build;
pub mod callbacks;
pub mod codegen;
pub mod config;
pub mod coordinator;
pub mod epoch;
pub mod importer;
pub mod lineage_repair;
mod migration_control;
pub mod module_loader;
pub mod module_sdk;
mod operations;
pub mod pack_command;
mod pipeline_map;
pub mod process;
pub mod quarantine;
pub mod scanner;
pub mod scheduler;
mod tool_resolver;
pub mod watcher;
