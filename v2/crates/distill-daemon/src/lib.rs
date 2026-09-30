//! Long-lived daemon infrastructure: pipeline-module epochs, cooperative
//! scheduling, code-loading policy, and displaced-inode quarantine.

// The system allocator is installed by distill-pipeline-api.

pub mod authoring;
mod authority;
pub mod bootstrap;
mod build;
pub mod callbacks;
pub mod codegen;
pub mod config;
pub mod coordinator;
pub mod dev;
pub mod epoch;
pub mod importer;
pub mod lineage_repair;
mod migration_control;
pub mod module_loader;
mod operations;
pub mod pack_command;
mod pipeline_map;
pub mod process;
pub mod quarantine;
pub mod scanner;
pub mod scheduler;
pub mod store_cell;
mod tool_resolver;
pub mod watcher;
#[cfg(not(unix))]
compile_error!(
    "distill-daemon currently supports Unix host processes only; Windows remains a cook target"
);
