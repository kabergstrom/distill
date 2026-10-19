//! Runtime support: erased values, epochs, the deterministic hasher, and
//! the table's status thunks.

use std::hash::{BuildHasher, Hasher};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use distill_asset::{AssetHashMap, AssetType, DeterministicState, EpochToken, ErasedValue, ModuleEpochPoisonCause};
use distill_test_assets::runtime::{Counted, PanickingDrop};

#[test]
fn deterministic_state_is_reproducible_and_map_defaults_to_it() {
    let state = DeterministicState;
    assert_eq!(state.hash_one("same"), state.hash_one("same"));

    let mut integer = state.build_hasher();
    integer.write_u32(0x0102_0304);
    let mut explicit_le = state.build_hasher();
    explicit_le.write(&0x0102_0304u32.to_le_bytes());
    assert_eq!(integer.finish(), explicit_le.finish());

    let mut map: AssetHashMap<&str, u32> = AssetHashMap::default();
    map.insert("b", 2);
    map.insert("a", 1);
    assert_eq!(map.len(), 2);
}

#[test]
fn erased_value_destroys_once_and_keeps_its_epoch_alive() {
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = EpochToken::new(42);
    let value = ErasedValue::new_in(Counted { drops: drops.clone() }, owner.clone());
    assert_eq!(value.owner_epoch(), 42);
    drop(owner);
    value.destroy().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn failed_erased_drop_is_contained_and_poisons_the_exact_epoch() {
    let owner = EpochToken::new(7);
    let other = EpochToken::new(8);
    let value = ErasedValue::new_in(PanickingDrop, owner.clone());
    assert!(value.destroy().is_err());
    assert!(owner.is_poisoned());
    assert_eq!(owner.poison_cause(), Some(ModuleEpochPoisonCause::CallbackPanic));
    assert!(!other.is_poisoned());
}

#[test]
fn module_epoch_token_latches_the_first_typed_poison_cause() {
    let owner = EpochToken::new(17);
    owner.poison_with(ModuleEpochPoisonCause::CallbackRejected);
    owner.poison_with(ModuleEpochPoisonCause::CallbackPanic);
    assert_eq!(owner.poison_cause(), Some(ModuleEpochPoisonCause::CallbackRejected));
}

struct PanicDefault;

impl Default for PanicDefault {
    fn default() -> Self {
        panic!("default failed")
    }
}

#[test]
fn skip_and_drop_callbacks_return_status_without_unwinding() {
    let skip = distill_asset::reflect::table::skip::<PanicDefault>();
    let mut slot = MaybeUninit::<PanicDefault>::uninit();
    assert!(unsafe { (skip.write)(slot.as_mut_ptr().cast()) }.is_err());

    let descriptor = PanickingDrop::descriptor();
    let drop_fn = descriptor.drops.entries[0];
    let mut slot = MaybeUninit::new(PanickingDrop);
    assert!(unsafe { drop_fn(slot.as_mut_ptr().cast()) }.is_err());
    // MaybeUninit does not drop its contents; the failed value stays leaked.
}

#[test]
fn descriptor_finalize_moves_into_an_epoch_owned_erased_value() {
    let drops = Arc::new(AtomicUsize::new(0));
    let descriptor = Counted::descriptor();
    let mut source = MaybeUninit::new(Counted { drops: drops.clone() });
    let value = unsafe { (descriptor.finalize)(source.as_mut_ptr().cast(), EpochToken::new(55)) }.unwrap();
    assert_eq!(value.owner_epoch(), 55);
    value.destroy().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
