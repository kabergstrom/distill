//! The vocabulary of a walk-generated asset table (newgameplus GAP 23
//! §18.2): `src/ngp_asset_types.rs` in every crate that declares asset
//! types, written by source-walk (`--asset-crate`) beside the crate's
//! pruned schema (`src/ngp_asset_types.bin`). The table holds what only
//! compiled code knows: per type its compiled size and align and its drop,
//! default and finalize thunks, per container instantiation its
//! constructor and read thunks, per skipped field type its default writer.
//! Everything else (offsets, tags, names, the logical schema) is the
//! schema's, which [`super::interp`] reads. The generated file asserts the
//! schema's layout numbers against the compiler's (`size_of`, `offset_of!`,
//! exhaustive patterns), so a table and schema that disagree with the
//! crate's sources fail its build.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use distill_wire::native::{CallbackPanic, CtorEntry, SkipEntry};

use crate::{AssetType, EpochToken, ErasedValue};

/// std names for the generated table.
pub mod std_names {
    pub use std::boxed::Box;
    pub use std::collections::hash_map::RandomState;
    pub use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    pub use std::option::Option;
    pub use std::string::String;
    pub use std::sync::Arc;
    pub use std::vec::Vec;
}

pub type DropThunk = unsafe fn(*mut u8) -> Result<(), CallbackPanic>;
/// Writes `T::default()` into an uninitialized, aligned slot.
pub type DefaultThunk = unsafe fn(*mut u8) -> Result<(), CallbackPanic>;
pub type FinalizeThunk = unsafe fn(*mut u8, EpochToken) -> Result<ErasedValue, CallbackPanic>;

/// One crate's generated table.
pub struct AssetTable {
    /// The crate's pruned schema in the schema file encoding: every type
    /// the crate's asset types reach, one layout table.
    pub schema: &'static [u8],
    pub types: &'static [TypeEntry],
    pub containers: &'static [ContainerEntry],
    pub skips: &'static [SkipTypeEntry],
    /// Descriptors, built from `schema` and the entries on first use.
    pub registry: super::interp::Registry,
}

/// A struct or enum the crate's asset types reach, keyed by its
/// `Schema::instance_key`.
pub struct TypeEntry {
    pub key: &'static str,
    pub size: usize,
    pub align: usize,
    /// `None` when the type has no drop glue (`needs_drop`).
    pub drop: Option<DropThunk>,
    /// The type's `Default`, when it has one.
    pub default: Option<DefaultThunk>,
    /// Asset types (`#[asset(uuid)]`) only.
    pub finalize: Option<FinalizeThunk>,
}

/// A container instantiation (`Vec<T>`, `Option<T>`, `Box<T>`, `Arc<T>`,
/// sets, maps), keyed by its `Schema::instance_key`.
pub struct ContainerEntry {
    pub key: &'static str,
    pub ops: fn() -> ContainerOps,
}

/// The type of a `#[asset(skip)]` field, keyed by its
/// `Schema::instance_key`: its default writer.
pub struct SkipTypeEntry {
    pub key: &'static str,
    pub entry: fn() -> SkipEntry,
}

pub struct ContainerOps {
    pub size: usize,
    pub align: usize,
    pub ctor: CtorEntry,
    pub read: ContainerRead,
}

/// How [`super::interp`] reads a container in place. Pointers are to the
/// container, element pointers into it; nothing is moved or copied.
#[derive(Clone, Copy)]
pub enum ContainerRead {
    /// Element pointer and length.
    Vec(unsafe fn(*const u8) -> (*const u8, usize)),
    /// The payload, or null for `None`.
    Option(unsafe fn(*const u8) -> *const u8),
    /// The pointee (`Box`, `Arc`).
    Deref(unsafe fn(*const u8) -> *const u8),
    /// Every element, in the container's iteration order.
    Set(unsafe fn(*const u8, &mut dyn FnMut(*const u8))),
    /// Every entry (key, value), in the container's iteration order.
    Map(unsafe fn(*const u8, &mut dyn FnMut(*const u8, *const u8))),
}

// --- Entry constructors the generated table calls ---

pub const fn size<T>() -> usize {
    std::mem::size_of::<T>()
}

pub const fn align<T>() -> usize {
    std::mem::align_of::<T>()
}

pub const fn drop<T>() -> Option<DropThunk> {
    if std::mem::needs_drop::<T>() {
        Some(crate::thunks::drop_in_place_thunk::<T>)
    } else {
        None
    }
}

/// # Safety
/// `dst` must be aligned, writable and uninitialized for a `T`.
pub unsafe fn default<T: Default>(dst: *mut u8) -> Result<(), CallbackPanic> {
    let value = catch_unwind(T::default).map_err(|_| CallbackPanic)?;
    std::ptr::write(dst.cast::<T>(), value);
    Ok(())
}

/// Move a complete `T` out of a plan-execution buffer into its erased form
/// (`AssetRuntimeDescriptor::finalize`).
///
/// # Safety
/// `src` must hold a complete `T`; afterwards it is uninitialized.
pub unsafe fn finalize<T: AssetType>(src: *mut u8, owner: EpochToken) -> Result<ErasedValue, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| {
        let value = std::ptr::read(src.cast::<T>());
        ErasedValue::new_in(value, owner)
    }))
    .map_err(|_| CallbackPanic)
}

pub fn skip<T: Default>() -> SkipEntry {
    crate::thunks::skip_entry::<T>()
}

/// [`skip`] for the type of the field `field` reads, which the generated
/// table then never spells.
pub fn skip_of<S, F: Default>(_field: fn(&S) -> &F) -> SkipEntry {
    skip::<F>()
}

unsafe fn vec_view<T>(p: *const u8) -> (*const u8, usize) {
    let v = &*p.cast::<Vec<T>>();
    (v.as_ptr().cast(), v.len())
}

unsafe fn option_some<T>(p: *const u8) -> *const u8 {
    match &*p.cast::<Option<T>>() {
        Some(v) => (v as *const T).cast(),
        None => std::ptr::null(),
    }
}

unsafe fn box_deref<T>(p: *const u8) -> *const u8 {
    (&**p.cast::<Box<T>>() as *const T).cast()
}

unsafe fn arc_deref<T>(p: *const u8) -> *const u8 {
    (&**p.cast::<Arc<T>>() as *const T).cast()
}

unsafe fn hash_set_each<T, S>(p: *const u8, f: &mut dyn FnMut(*const u8)) {
    for v in &*p.cast::<HashSet<T, S>>() {
        f((v as *const T).cast());
    }
}

unsafe fn btree_set_each<T>(p: *const u8, f: &mut dyn FnMut(*const u8)) {
    for v in &*p.cast::<BTreeSet<T>>() {
        f((v as *const T).cast());
    }
}

unsafe fn hash_map_each<K, V, S>(p: *const u8, f: &mut dyn FnMut(*const u8, *const u8)) {
    for (k, v) in &*p.cast::<HashMap<K, V, S>>() {
        f((k as *const K).cast(), (v as *const V).cast());
    }
}

unsafe fn btree_map_each<K, V>(p: *const u8, f: &mut dyn FnMut(*const u8, *const u8)) {
    for (k, v) in &*p.cast::<BTreeMap<K, V>>() {
        f((k as *const K).cast(), (v as *const V).cast());
    }
}

fn ops<C>(ctor: CtorEntry, read: ContainerRead) -> ContainerOps {
    ContainerOps { size: size::<C>(), align: align::<C>(), ctor, read }
}

pub fn vec<T>() -> ContainerOps {
    ops::<Vec<T>>(crate::thunks::vec_ctor::<T>(), ContainerRead::Vec(vec_view::<T>))
}

pub fn option<T>() -> ContainerOps {
    ops::<Option<T>>(crate::thunks::option_ctor::<T>(), ContainerRead::Option(option_some::<T>))
}

pub fn boxed<T>() -> ContainerOps {
    ops::<Box<T>>(crate::thunks::box_ctor::<T>(), ContainerRead::Deref(box_deref::<T>))
}

pub fn arc<T>() -> ContainerOps {
    ops::<Arc<T>>(crate::thunks::arc_ctor::<T>(), ContainerRead::Deref(arc_deref::<T>))
}

pub fn hash_set<T: Eq + Hash, S: BuildHasher + Default>() -> ContainerOps {
    ops::<HashSet<T, S>>(crate::thunks::hash_set_ctor::<T, S>(), ContainerRead::Set(hash_set_each::<T, S>))
}

pub fn btree_set<T: Ord>() -> ContainerOps {
    ops::<BTreeSet<T>>(crate::thunks::btree_set_ctor::<T>(), ContainerRead::Set(btree_set_each::<T>))
}

pub fn hash_map<K: Eq + Hash, V, S: BuildHasher + Default>() -> ContainerOps {
    ops::<HashMap<K, V, S>>(crate::thunks::hash_map_ctor::<K, V, S>(), ContainerRead::Map(hash_map_each::<K, V, S>))
}

pub fn btree_map<K: Ord, V>() -> ContainerOps {
    ops::<BTreeMap<K, V>>(crate::thunks::btree_map_ctor::<K, V>(), ContainerRead::Map(btree_map_each::<K, V>))
}

/// `value` rounded up to `align` (a power of two), for the generated
/// file's enum variant offset checks.
pub const fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}
