//! Long-lived daemon infrastructure: pipeline-module epochs, cooperative
//! scheduling, code-loading policy, and displaced-inode quarantine.

pub mod authoring;
pub mod config;
pub mod coordinator;
pub mod epoch;
pub mod importer;
pub mod lineage_repair;
pub mod logical_node;
pub mod module_loader;
mod operations;
pub mod policy;
pub mod process;
pub mod quarantine;
pub mod scanner;
pub mod scheduler;
pub mod watcher;
