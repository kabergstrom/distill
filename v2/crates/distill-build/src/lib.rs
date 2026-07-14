//! Offline import, processing, dependency-trace, and scheduling core
//! (§§8–10). The crate is deliberately IO-abstract: daemon integration
//! supplies snapshot/file backends while this crate owns deterministic
//! folds, keys, validation, trace revalidation, and stack-safe traversal.

pub mod cache;
pub mod codegen;
pub mod dslf;
pub mod import;
pub mod keys;
pub mod outputs;
pub mod pipeline;
pub mod query;
pub mod scheduler;
pub mod tool;
pub mod trace;
