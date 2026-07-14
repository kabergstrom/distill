//! Long-lived daemon infrastructure: pipeline-module epochs, cooperative
//! scheduling, code-loading policy, and displaced-inode quarantine.

pub mod epoch;
pub mod policy;
pub mod quarantine;
pub mod scanner;
pub mod scheduler;
