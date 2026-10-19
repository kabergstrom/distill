//! Distill's test asset types (see `Cargo.toml`). One module per test
//! suite that uses them; each type's doc names its suite.

pub mod defaults;
pub mod loader;
pub mod recursion;
pub mod runtime;
pub mod semantic;
mod ngp_asset_types;

pub use ngp_asset_types::NGP_ASSET_TABLE;
