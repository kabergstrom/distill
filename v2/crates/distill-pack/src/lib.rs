//! Cooked shipping pack format (§16): authenticated manifest tables,
//! independently decompressible structural blocks, mmap-ready blob extents,
//! and durable `pack.current` activation.

pub mod activation;
pub mod archive;
pub mod manifest;
pub mod packfile_io;

pub use activation::*;
pub use archive::*;
pub use manifest::*;
pub use packfile_io::*;
