//! Generated-table callback implementations. Every entry catches unwind
//! inside the defining module and reports status; no callback lets a
//! user `Default`, `Hash`/`Eq`/`Ord`, or `Drop` panic cross the boundary.
//!
//! One non-generic `catch` runs every callback body: a body is a plain
//! `unsafe fn(*mut u8)` over its family's argument block (`Begin`, `Push`,
//! `Finish`, `Finalize`, or the value pointer itself). Per type, an asset
//! crate then compiles only the bodies and their thin entry wrappers, never
//! the unwinding machinery (GAP 23 §18.1: crates hold hundreds of entries).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::mem::ManuallyDrop;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::null_mut;
use std::sync::Arc;

use distill_wire::native::{CallbackPanic, CtorCursor, CtorEntry, PushError, SkipEntry};

use crate::{AssetType, EpochToken, ErasedValue};

type Body = unsafe fn(*mut u8);

#[inline(never)]
unsafe fn catch(body: Body, args: *mut u8) -> Result<(), CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe { body(args) })).map_err(|_| CallbackPanic)
}

unsafe fn args<'a, A>(p: *mut u8) -> &'a mut A {
    &mut *p.cast::<A>()
}

fn ptr_of<A>(a: &mut A) -> *mut u8 {
    (a as *mut A).cast()
}

// --- Drops, defaults, finalize ---

unsafe fn drop_body<T>(p: *mut u8) {
    std::ptr::drop_in_place(p.cast::<T>())
}

/// Drop a heap-owned erased `T`. A panic is caught and reported; the
/// partially destroyed allocation is intentionally not touched again.
///
/// # Safety
///
/// `ptr` must be the unique pointer returned by `Box::into_raw` for a
/// live `T`, and this callback may be invoked at most once.
pub unsafe fn erased_drop_thunk<T>(ptr: *mut u8) -> Result<(), CallbackPanic> {
    drop_boxed_in_place(ptr.cast::<T>())
}

/// Drop an initialized in-place `T` under panic containment.
///
/// # Safety
///
/// `ptr` must be aligned and point to a live `T`, and the value may not
/// be used or dropped again after this call, including on `Err`.
pub unsafe fn drop_in_place_thunk<T>(ptr: *mut u8) -> Result<(), CallbackPanic> {
    catch(drop_body::<T>, ptr)
}

/// Drop the pointee first, then free its box allocation only on success.
/// Constructing a `Box<T>` before the caught drop would let Box's unwind
/// cleanup deallocate after a panicking `T::drop`, violating the required
/// leak-on-failure contract.
unsafe fn drop_boxed_in_place<T>(ptr: *mut T) -> Result<(), CallbackPanic> {
    catch(drop_body::<T>, ptr.cast())?;
    drop(Box::from_raw(ptr.cast::<ManuallyDrop<T>>()));
    Ok(())
}

unsafe fn default_body<T: Default>(dst: *mut u8) {
    let value = T::default();
    std::ptr::write(dst.cast::<T>(), value);
}

/// Write `T::default()` into `dst`; a panicking `Default` writes nothing.
///
/// # Safety
/// `dst` must be aligned, writable and uninitialized for a `T`.
pub unsafe fn write_default<T: Default>(dst: *mut u8) -> Result<(), CallbackPanic> {
    catch(default_body::<T>, dst)
}

pub fn skip_entry<T: Default>() -> SkipEntry {
    SkipEntry {
        write: write_default::<T>,
        drop_in_place: drop_in_place_thunk::<T>,
    }
}

struct Finalize {
    src: *mut u8,
    owner: Option<EpochToken>,
    out: Option<ErasedValue>,
}

unsafe fn finalize_body<T: AssetType>(p: *mut u8) {
    let a = args::<Finalize>(p);
    if let Some(owner) = a.owner.take() {
        let value = std::ptr::read(a.src.cast::<T>());
        a.out = Some(ErasedValue::new_in(value, owner));
    }
}

/// Move a complete `T` out of `src` into its erased, epoch-owned form.
///
/// # Safety
/// `src` must hold a complete `T`; afterwards it is uninitialized.
pub unsafe fn finalize<T: AssetType>(src: *mut u8, owner: EpochToken) -> Result<ErasedValue, CallbackPanic> {
    let mut a = Finalize { src, owner: Some(owner), out: None };
    catch(finalize_body::<T>, ptr_of(&mut a))?;
    a.out.ok_or(CallbackPanic)
}

// --- Container constructors (§12 `CtorEntry`) ---

struct Begin {
    len: u32,
    state: *mut (),
}

unsafe fn begin(len: u32, body: Body) -> Result<CtorCursor, CallbackPanic> {
    let mut a = Begin { len, state: null_mut() };
    catch(body, ptr_of(&mut a))?;
    Ok(CtorCursor { state: a.state })
}

struct Push {
    state: *mut (),
    elem: *mut u8,
    inserted: bool,
}

unsafe fn push(cur: &mut CtorCursor, elem: *mut u8, body: Body) -> Result<(), PushError> {
    let mut a = Push { state: cur.state, elem, inserted: false };
    catch(body, ptr_of(&mut a)).map_err(PushError::Panic)?;
    if a.inserted {
        Ok(())
    } else {
        Err(PushError::Duplicate)
    }
}

struct Finish {
    state: *mut (),
    dst: *mut u8,
}

unsafe fn finish(cur: &mut CtorCursor, dst: *mut u8, body: Body) -> Result<(), CallbackPanic> {
    let mut a = Finish { state: cur.state, dst };
    let result = catch(body, ptr_of(&mut a));
    cur.state = a.state;
    result
}

fn boxed<C>(value: C) -> *mut () {
    Box::into_raw(Box::new(value)).cast()
}

/// Move the boxed partial container into `dst` as it is.
unsafe fn finish_moved_body<C>(p: *mut u8) {
    let a = args::<Finish>(p);
    let value = *Box::from_raw(a.state.cast::<C>());
    a.state = null_mut();
    std::ptr::write(a.dst.cast::<C>(), value);
}

unsafe fn abort_boxed<C>(cur: CtorCursor) -> Result<(), CallbackPanic> {
    if cur.state.is_null() {
        return Ok(());
    }
    drop_boxed_in_place(cur.state.cast::<C>())
}

/// Insert the element through `insert` (`false`: duplicate key).
unsafe fn push_with<C, E>(p: *mut u8, insert: impl FnOnce(&mut C, E) -> bool) {
    let a = args::<Push>(p);
    let value = std::ptr::read(a.elem.cast::<E>());
    a.inserted = insert(&mut *a.state.cast::<C>(), value);
}

unsafe fn vec_begin_body<T>(p: *mut u8) {
    let a = args::<Begin>(p);
    a.state = boxed(Vec::<T>::with_capacity(a.len as usize));
}

unsafe fn vec_push_body<T>(p: *mut u8) {
    push_with(p, |v: &mut Vec<T>, value: T| {
        v.push(value);
        true
    })
}

unsafe fn vec_begin<T>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    begin(len, vec_begin_body::<T>)
}

unsafe fn vec_push<T>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    push(cur, elem, vec_push_body::<T>)
}

unsafe fn finish_moved<C>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    finish(cur, dst, finish_moved_body::<C>)
}

pub fn vec_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: vec_begin::<T>,
        push: vec_push::<T>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: finish_moved::<Vec<T>>,
        abort: abort_boxed::<Vec<T>>,
        drop_in_place: drop_in_place_thunk::<Vec<T>>,
    }
}

unsafe fn hash_set_begin_body<T, S: BuildHasher + Default>(p: *mut u8) {
    let a = args::<Begin>(p);
    a.state = boxed(HashSet::<T, S>::with_capacity_and_hasher(a.len as usize, S::default()));
}

unsafe fn hash_set_push_body<T: Eq + Hash, S: BuildHasher>(p: *mut u8) {
    push_with(p, |s: &mut HashSet<T, S>, value: T| s.insert(value))
}

unsafe fn hash_set_begin<T, S: BuildHasher + Default>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    begin(len, hash_set_begin_body::<T, S>)
}

unsafe fn hash_set_push<T: Eq + Hash, S: BuildHasher>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    push(cur, elem, hash_set_push_body::<T, S>)
}

pub fn hash_set_ctor<T, S>() -> CtorEntry
where
    T: Eq + Hash,
    S: BuildHasher + Default,
{
    CtorEntry {
        begin: hash_set_begin::<T, S>,
        push: hash_set_push::<T, S>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: finish_moved::<HashSet<T, S>>,
        abort: abort_boxed::<HashSet<T, S>>,
        drop_in_place: drop_in_place_thunk::<HashSet<T, S>>,
    }
}

unsafe fn btree_set_begin_body<T>(p: *mut u8) {
    args::<Begin>(p).state = boxed(BTreeSet::<T>::new());
}

unsafe fn btree_set_push_body<T: Ord>(p: *mut u8) {
    push_with(p, |s: &mut BTreeSet<T>, value: T| s.insert(value))
}

unsafe fn btree_set_begin<T>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    begin(len, btree_set_begin_body::<T>)
}

unsafe fn btree_set_push<T: Ord>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    push(cur, elem, btree_set_push_body::<T>)
}

pub fn btree_set_ctor<T: Ord>() -> CtorEntry {
    CtorEntry {
        begin: btree_set_begin::<T>,
        push: btree_set_push::<T>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: finish_moved::<BTreeSet<T>>,
        abort: abort_boxed::<BTreeSet<T>>,
        drop_in_place: drop_in_place_thunk::<BTreeSet<T>>,
    }
}

unsafe fn hash_map_begin_body<K, V, S: BuildHasher + Default>(p: *mut u8) {
    let a = args::<Begin>(p);
    a.state = boxed(HashMap::<K, V, S>::with_capacity_and_hasher(a.len as usize, S::default()));
}

unsafe fn hash_map_push_body<K: Eq + Hash, V, S: BuildHasher>(p: *mut u8) {
    push_with(p, |map: &mut HashMap<K, V, S>, (key, value): (K, V)| match map.entry(key) {
        std::collections::hash_map::Entry::Occupied(_) => {
            drop(value);
            false
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(value);
            true
        }
    })
}

unsafe fn hash_map_begin<K, V, S: BuildHasher + Default>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    begin(len, hash_map_begin_body::<K, V, S>)
}

unsafe fn hash_map_push<K: Eq + Hash, V, S: BuildHasher>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    push(cur, elem, hash_map_push_body::<K, V, S>)
}

pub fn hash_map_ctor<K, V, S>() -> CtorEntry
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    CtorEntry {
        begin: hash_map_begin::<K, V, S>,
        push: hash_map_push::<K, V, S>,
        elem_size: size_u32::<(K, V)>(),
        elem_align: align_u32::<(K, V)>(),
        key_offset: std::mem::offset_of!((K, V), 0) as u32,
        value_offset: std::mem::offset_of!((K, V), 1) as u32,
        finish: finish_moved::<HashMap<K, V, S>>,
        abort: abort_boxed::<HashMap<K, V, S>>,
        drop_in_place: drop_in_place_thunk::<HashMap<K, V, S>>,
    }
}

unsafe fn btree_map_begin_body<K, V>(p: *mut u8) {
    args::<Begin>(p).state = boxed(BTreeMap::<K, V>::new());
}

unsafe fn btree_map_push_body<K: Ord, V>(p: *mut u8) {
    push_with(p, |map: &mut BTreeMap<K, V>, (key, value): (K, V)| match map.entry(key) {
        std::collections::btree_map::Entry::Occupied(_) => {
            drop(value);
            false
        }
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
            true
        }
    })
}

unsafe fn btree_map_begin<K, V>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    begin(len, btree_map_begin_body::<K, V>)
}

unsafe fn btree_map_push<K: Ord, V>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    push(cur, elem, btree_map_push_body::<K, V>)
}

pub fn btree_map_ctor<K: Ord, V>() -> CtorEntry {
    CtorEntry {
        begin: btree_map_begin::<K, V>,
        push: btree_map_push::<K, V>,
        elem_size: size_u32::<(K, V)>(),
        elem_align: align_u32::<(K, V)>(),
        key_offset: std::mem::offset_of!((K, V), 0) as u32,
        value_offset: std::mem::offset_of!((K, V), 1) as u32,
        finish: finish_moved::<BTreeMap<K, V>>,
        abort: abort_boxed::<BTreeMap<K, V>>,
        drop_in_place: drop_in_place_thunk::<BTreeMap<K, V>>,
    }
}

// Option, Box and Arc build through a partial `Option<T>` (one element).

unsafe fn option_begin_body<T>(p: *mut u8) {
    let a = args::<Begin>(p);
    assert!(a.len <= 1, "single-value ctor length exceeds one");
    a.state = boxed(None::<T>);
}

unsafe fn option_push_body<T>(p: *mut u8) {
    push_with(p, |slot: &mut Option<T>, value: T| {
        if slot.is_some() {
            drop(value);
            false
        } else {
            *slot = Some(value);
            true
        }
    })
}

unsafe fn option_begin<T>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    begin(len, option_begin_body::<T>)
}

unsafe fn option_push<T>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    push(cur, elem, option_push_body::<T>)
}

unsafe fn box_finish_body<T>(p: *mut u8) {
    let a = args::<Finish>(p);
    let value = Box::from_raw(a.state.cast::<Option<T>>()).take().expect("box ctor requires exactly one element");
    a.state = null_mut();
    std::ptr::write(a.dst.cast::<Box<T>>(), Box::new(value));
}

unsafe fn arc_finish_body<T>(p: *mut u8) {
    let a = args::<Finish>(p);
    let value = Box::from_raw(a.state.cast::<Option<T>>()).take().expect("arc ctor requires exactly one element");
    a.state = null_mut();
    std::ptr::write(a.dst.cast::<Arc<T>>(), Arc::new(value));
}

unsafe fn box_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    finish(cur, dst, box_finish_body::<T>)
}

unsafe fn arc_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    finish(cur, dst, arc_finish_body::<T>)
}

pub fn option_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: option_begin::<T>,
        push: option_push::<T>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: finish_moved::<Option<T>>,
        abort: abort_boxed::<Option<T>>,
        drop_in_place: drop_in_place_thunk::<Option<T>>,
    }
}

pub fn box_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: option_begin::<T>,
        push: option_push::<T>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: box_finish::<T>,
        abort: abort_boxed::<Option<T>>,
        drop_in_place: drop_in_place_thunk::<Box<T>>,
    }
}

pub fn arc_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: option_begin::<T>,
        push: option_push::<T>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: arc_finish::<T>,
        abort: abort_boxed::<Option<T>>,
        drop_in_place: drop_in_place_thunk::<Arc<T>>,
    }
}

fn size_u32<T>() -> u32 {
    u32::try_from(std::mem::size_of::<T>()).expect("asset native size exceeds u32")
}

fn align_u32<T>() -> u32 {
    u32::try_from(std::mem::align_of::<T>()).expect("asset native alignment exceeds u32")
}
