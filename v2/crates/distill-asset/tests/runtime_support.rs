use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::mem::MaybeUninit;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use distill_asset::{
    default_table, placeholder, AssetHashMap, AssetReflect, DeterministicState, EncodeContainer,
    EncodeSink, EpochToken, ErasedValue, ModuleEpochPoisonCause,
};
use distill_core::id::{AssetUuid, TypeUuid};
use distill_json::AuthoredValue;

#[distill_asset::asset(uuid = "00112233-4455-6677-8899-aabbccddeeff")]
struct Counted {
    #[asset(skip)]
    drops: Arc<AtomicUsize>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[distill_asset::asset(uuid = "20112233-4455-6677-8899-aabbccddeeff")]
struct Plain;

#[distill_asset::asset(uuid = "10112233-4455-6677-8899-aabbccddeeff")]
struct PanickingDrop;

#[test]
fn empty_map_authored_shape_is_selected_by_key_type_not_vacuous_entries() {
    let binary = BTreeMap::<[u8; 16], u32>::new().to_authored();
    let strings = BTreeMap::<String, u32>::new().to_authored();
    assert_eq!(binary, AuthoredValue::Array(Vec::new()));
    assert_eq!(strings, AuthoredValue::Object(BTreeMap::new()));
}

impl Drop for PanickingDrop {
    fn drop(&mut self) {
        panic!("drop failed")
    }
}

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
    let value = ErasedValue::new_in(
        Counted {
            drops: drops.clone(),
        },
        owner.clone(),
    );
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
    assert_eq!(
        owner.poison_cause(),
        Some(ModuleEpochPoisonCause::CallbackPanic)
    );
    assert!(!other.is_poisoned());
}

#[test]
fn module_epoch_token_latches_the_first_typed_poison_cause() {
    let owner = EpochToken::new(17);
    owner.poison_with(ModuleEpochPoisonCause::CallbackRejected);
    owner.poison_with(ModuleEpochPoisonCause::CallbackPanic);
    assert_eq!(
        owner.poison_cause(),
        Some(ModuleEpochPoisonCause::CallbackRejected)
    );
}

#[test]
fn placeholder_contains_panics_and_binds_minted_values_to_the_caller_epoch() {
    let okay = placeholder!(Plain, Plain);
    let owner = EpochToken::new(91);
    let value = (okay.make)(owner.clone()).unwrap();
    assert_eq!(value.owner_epoch(), 91);
    value.destroy().unwrap();

    let bad = placeholder!(Plain, panic!("constructor failed"));
    assert!((bad.make)(owner).is_err());
}

#[test]
fn encode_panics_are_statuses() {
    struct PanicSink;
    impl EncodeSink for PanicSink {
        fn flat(&mut self, _: &[u8]) {
            panic!("sink")
        }
        fn begin(&mut self, _: EncodeContainer, _: u32) {
            panic!("sink")
        }
        fn push(&mut self) {}
        fn finish(&mut self) {}
        fn blob(&mut self, _: &[u8]) {}
        fn reference(&mut self, _: bool, _: AssetUuid, _: TypeUuid) {}
    }

    let descriptor = <Plain as distill_asset::AssetType>::descriptor();
    let value = Plain;
    let mut sink = PanicSink;
    let result = unsafe { (descriptor.encode)((&value as *const Plain).cast(), &mut sink) };
    assert!(result.is_err());
}

struct PanicDefault;

impl Default for PanicDefault {
    fn default() -> Self {
        panic!("default failed")
    }
}

#[test]
fn generated_skip_and_drop_callbacks_return_status_without_unwinding() {
    let skip = distill_asset::thunks::skip_entry::<PanicDefault>();
    let mut slot = MaybeUninit::<PanicDefault>::uninit();
    assert!(unsafe { (skip.write)(slot.as_mut_ptr().cast()) }.is_err());

    let descriptor = <PanickingDrop as distill_asset::AssetType>::descriptor();
    let drop_fn = descriptor.drops.entries[0];
    let mut slot = MaybeUninit::new(PanickingDrop);
    assert!(unsafe { drop_fn(slot.as_mut_ptr().cast()) }.is_err());
    // MaybeUninit does not drop its contents; the failed value stays leaked.
}

#[test]
fn descriptor_finalize_moves_into_an_epoch_owned_erased_value() {
    let drops = Arc::new(AtomicUsize::new(0));
    let descriptor = <Counted as distill_asset::AssetType>::descriptor();
    let mut source = MaybeUninit::new(Counted {
        drops: drops.clone(),
    });
    let value =
        unsafe { (descriptor.finalize)(source.as_mut_ptr().cast(), EpochToken::new(55)) }.unwrap();
    assert_eq!(value.owner_epoch(), 55);
    value.destroy().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn non_default_asset_has_no_fabricated_parent_default() {
    assert!(default_table::<Counted>().parent.is_none());
}
