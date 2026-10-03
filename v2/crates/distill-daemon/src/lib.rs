//! Long-lived daemon infrastructure: pipeline-module epochs, cooperative
//! scheduling, code-loading policy.

// The system allocator is installed by distill-pipeline-api.

pub mod authoring;
pub mod bootstrap;
mod build;
pub mod callbacks;
pub mod codegen;
pub mod compiled;
pub mod config;
pub mod coordinator;
pub mod epoch;
pub mod importer;
pub mod module_loader;
mod operations;
pub mod pack_command;
mod pipeline_map;
pub mod process;
pub mod rebuild;
pub mod scanner;
pub mod scheduler;
mod settle;
mod tool_resolver;
pub mod watcher;
