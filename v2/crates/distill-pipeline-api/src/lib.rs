//! Everything a pipeline module links against.
//!
//! A module depends on this crate and nothing of the daemon. The daemon and
//! `distill-build` re-export these types at their old paths.

pub mod codegen;
pub mod failure;
pub mod import;
pub mod outputs;
pub mod query;
pub mod target;
pub mod tool;
