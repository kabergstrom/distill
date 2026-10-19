//! `distill-loader` (orchestrator, rpc_io_backpressure), `distill-pack`
//! (layered), `distill-daemon` (module_host) and `distill-schema`
//! (project_authority) fixtures.

use std::sync::atomic::{AtomicUsize, Ordering};

/// orchestrator.
#[distill_asset::asset(uuid = "41112233-4455-6677-8899-aabbccddeeff")]
pub struct A;

/// orchestrator.
#[distill_asset::asset(uuid = "42112233-4455-6677-8899-aabbccddeeff")]
pub struct B;

/// orchestrator.
#[distill_asset::asset(uuid = "43112233-4455-6677-8899-aabbccddeeff", build_only)]
pub struct BuildOnly;

/// Drops of [`Counted`].
pub static COUNTED_DROPS: AtomicUsize = AtomicUsize::new(0);

/// orchestrator: counts its drops in [`COUNTED_DROPS`].
#[distill_asset::asset(uuid = "44112233-4455-6677-8899-aabbccddeeff")]
pub struct Counted;

impl Drop for Counted {
    fn drop(&mut self) {
        COUNTED_DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

/// rpc_io_backpressure.
#[distill_asset::asset(uuid = "51112233-4455-6677-8899-aabbccddeeff")]
pub struct RpcA;

/// distill-pack layered.
#[distill_asset::asset(uuid = "61112233-4455-6677-8899-aabbccddeeff")]
pub struct PackA;

/// distill-daemon module_host: its drop panics.
#[distill_asset::asset(uuid = "30112233-4455-6677-8899-aabbccddeeff")]
pub struct PanickingAssetDrop;

impl Drop for PanickingAssetDrop {
    fn drop(&mut self) {
        panic!("module asset drop failed")
    }
}

/// distill-schema project_authority.
#[repr(C)]
#[distill_asset::asset(uuid = "91112233-4455-4677-8899-aabbccddeeff")]
pub struct ProjectRow {
    pub alpha: u32,
    pub beta: u16,
}
