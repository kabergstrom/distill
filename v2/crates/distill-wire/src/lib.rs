//! Binary artifact format (§12): the DSTL container, wire-layout derivation
//! and hashing, the live native-layout tree, fixup-plan compilation, and the
//! transactional fixup executor.

pub mod artifact;
pub mod derive;
pub mod dswl;
pub mod encode;
pub mod exec;
pub mod native;
pub mod plan;
pub mod wire;
