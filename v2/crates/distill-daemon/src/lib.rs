//! Long-lived daemon infrastructure: pipeline-module epochs, cooperative
//! scheduling, code-loading policy.

// The system allocator is installed by distill-pipeline-api.

pub mod atomic;
pub mod authoring;
pub mod bootstrap;
mod build;
pub mod callbacks;
pub mod codegen;
pub mod config;
pub mod coordinator;
pub mod epoch;
pub mod importer;
pub mod module_loader;
mod operations;
pub mod pack_command;
mod pipeline_map;
pub mod process;
#[cfg(unix)]
pub mod rebuild;
pub mod scanner;
pub mod scheduler;
mod tool_resolver;
pub mod watcher;
#[cfg(not(unix))]
compile_error!(
    "distill-daemon currently supports Unix host processes only; Windows remains a cook target"
);
