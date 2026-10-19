//! Runtime support: drops, finalize, skip writers.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Counts its drops in its skipped field.
#[distill_asset::asset(uuid = "00112233-4455-6677-8899-aabbccddeeff")]
pub struct Counted {
    #[asset(skip)]
    pub drops: Arc<AtomicUsize>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[distill_asset::asset(uuid = "20112233-4455-6677-8899-aabbccddeeff")]
pub struct Plain;

/// Its drop panics.
#[distill_asset::asset(uuid = "10112233-4455-6677-8899-aabbccddeeff")]
pub struct PanickingDrop;

impl Drop for PanickingDrop {
    fn drop(&mut self) {
        panic!("asset drop failed")
    }
}
