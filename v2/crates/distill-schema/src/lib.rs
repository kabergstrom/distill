//! Daemon-side schema services (§5, §11).
//!
//! The schema MODEL — split model, node grammar, classifier, projection,
//! snapshot codec, `Schema::merge`, migration planning — is shared code and
//! lives in `ngp-schema` (the engine workspace), so source-walk, the
//! engine, and distill can never disagree on it (§19). This crate is what
//! stays distill-side: the daemon's live registry over that model.

pub use ngp_schema;

pub mod registry;

pub use registry::SchemaRegistry;
