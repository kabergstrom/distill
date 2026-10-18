//! Building a shipping pack (§16) from a running daemon's pinned snapshot,
//! publishing its archive and manifest, and activating it. Runtimes read
//! packs through `distill-pack` alone.

pub mod builder;
pub mod publish;

pub use builder::*;
pub use publish::*;
