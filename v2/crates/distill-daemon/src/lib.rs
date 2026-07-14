//! Long-lived daemon infrastructure: pipeline-module epochs, cooperative
//! scheduling, code-loading policy, and displaced-inode quarantine.

pub mod authoring;
mod build;
pub mod callbacks;
pub mod config;
pub mod coordinator;
pub mod epoch;
pub mod importer;
pub mod lineage_repair;
pub mod logical_node;
mod migration_control;
pub mod module_loader;
mod operations;
mod pipeline_map;
pub mod policy;
pub mod process;
pub mod quarantine;
pub mod scanner;
pub mod scheduler;
mod tool_resolver;
pub mod watcher;
