//! Offline import, processing, and dependency-trace core
//! (§§8–10). The crate is deliberately IO-abstract: daemon integration
//! supplies snapshot/file backends while this crate owns deterministic
//! folds, keys, validation, and trace revalidation.

pub mod artifact_encode;
pub mod cache;
pub mod codegen;
pub mod dslf;
pub mod import;
pub mod keys;
pub mod outputs;
pub mod persist;
pub mod pipeline;
pub mod query;
pub mod tool;
pub mod trace;
