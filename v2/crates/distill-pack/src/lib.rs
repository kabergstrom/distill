//! Cooked shipping pack format (§16): authenticated manifest tables,
//! independently decompressible structural blocks, mmap-ready blob extents,
//! and reading the durable `pack.current` activation. Building, publishing
//! and activating a pack is `distill-pack-build`'s.

pub mod activation;
pub mod archive;
pub mod diff;
pub mod manifest;
pub mod packfile_io;

pub use activation::*;
pub use archive::*;
pub use diff::*;
pub use manifest::*;
pub use packfile_io::*;
