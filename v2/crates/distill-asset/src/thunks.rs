//! Generated-table callback implementations. Every entry catches unwind
//! inside the defining module and reports status; no callback lets a
//! user `Default`, `Hash`/`Eq`/`Ord`, or `Drop` panic cross the boundary.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::mem::ManuallyDrop;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use distill_wire::native::{CallbackPanic, CtorCursor, CtorEntry, PushError, SkipEntry};

fn contained<R>(body: impl FnOnce() -> R) -> Result<R, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(body)).map_err(|_| CallbackPanic)
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
    contained(|| std::ptr::drop_in_place(ptr.cast::<T>()))
}

unsafe fn skip_write<T: Default>(dst: *mut u8) -> Result<(), CallbackPanic> {
    match contained(T::default) {
        Ok(value) => {
            std::ptr::write(dst.cast::<T>(), value);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub fn skip_entry<T: Default>() -> SkipEntry {
    SkipEntry {
        write: skip_write::<T>,
        drop_in_place: drop_in_place_thunk::<T>,
    }
}

unsafe fn vec_begin<T>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    contained(|| CtorCursor {
        state: Box::into_raw(Box::new(Vec::<T>::with_capacity(len as usize))).cast(),
    })
}

unsafe fn vec_push<T>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    contained(|| {
        let value = std::ptr::read(elem.cast::<T>());
        (&mut *cur.state.cast::<Vec<T>>()).push(value);
    })
    .map_err(PushError::Panic)
}

unsafe fn vec_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    contained(|| {
        let value = *Box::from_raw(cur.state.cast::<Vec<T>>());
        cur.state = std::ptr::null_mut();
        std::ptr::write(dst.cast::<Vec<T>>(), value);
    })
}

unsafe fn abort_boxed<T>(mut cur: CtorCursor) -> Result<(), CallbackPanic> {
    if cur.state.is_null() {
        return Ok(());
    }
    let state = cur.state;
    cur.state = std::ptr::null_mut();
    drop_boxed_in_place(state.cast::<T>())
}

/// Drop the pointee first, then free its box allocation only on success.
/// Constructing a `Box<T>` before the caught drop would let Box's unwind
/// cleanup deallocate after a panicking `T::drop`, violating the required
/// leak-on-failure contract.
unsafe fn drop_boxed_in_place<T>(ptr: *mut T) -> Result<(), CallbackPanic> {
    contained(|| std::ptr::drop_in_place(ptr))?;
    drop(Box::from_raw(ptr.cast::<ManuallyDrop<T>>()));
    Ok(())
}

pub fn vec_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: vec_begin::<T>,
        push: vec_push::<T>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: vec_finish::<T>,
        abort: abort_boxed::<Vec<T>>,
        drop_in_place: drop_in_place_thunk::<Vec<T>>,
    }
}

unsafe fn hash_set_begin<T, S>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic>
where
    S: BuildHasher + Default,
{
    contained(|| CtorCursor {
        state: Box::into_raw(Box::new(HashSet::<T, S>::with_capacity_and_hasher(
            len as usize,
            S::default(),
        )))
        .cast(),
    })
}

unsafe fn hash_set_push<T, S>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    match contained(|| {
        let value = std::ptr::read(elem.cast::<T>());
        (&mut *cur.state.cast::<HashSet<T, S>>()).insert(value)
    }) {
        Ok(true) => Ok(()),
        Ok(false) => Err(PushError::Duplicate),
        Err(e) => Err(PushError::Panic(e)),
    }
}

unsafe fn hash_set_finish<T, S>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    contained(|| {
        let value = *Box::from_raw(cur.state.cast::<HashSet<T, S>>());
        cur.state = std::ptr::null_mut();
        std::ptr::write(dst.cast::<HashSet<T, S>>(), value);
    })
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
        finish: hash_set_finish::<T, S>,
        abort: abort_boxed::<HashSet<T, S>>,
        drop_in_place: drop_in_place_thunk::<HashSet<T, S>>,
    }
}

unsafe fn btree_set_begin<T>(_: *mut u8, _: u32) -> Result<CtorCursor, CallbackPanic> {
    contained(|| CtorCursor {
        state: Box::into_raw(Box::new(BTreeSet::<T>::new())).cast(),
    })
}

unsafe fn btree_set_push<T: Ord>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    match contained(|| {
        let value = std::ptr::read(elem.cast::<T>());
        (&mut *cur.state.cast::<BTreeSet<T>>()).insert(value)
    }) {
        Ok(true) => Ok(()),
        Ok(false) => Err(PushError::Duplicate),
        Err(e) => Err(PushError::Panic(e)),
    }
}

unsafe fn btree_set_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    contained(|| {
        let value = *Box::from_raw(cur.state.cast::<BTreeSet<T>>());
        cur.state = std::ptr::null_mut();
        std::ptr::write(dst.cast::<BTreeSet<T>>(), value);
    })
}

pub fn btree_set_ctor<T: Ord>() -> CtorEntry {
    CtorEntry {
        begin: btree_set_begin::<T>,
        push: btree_set_push::<T>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: btree_set_finish::<T>,
        abort: abort_boxed::<BTreeSet<T>>,
        drop_in_place: drop_in_place_thunk::<BTreeSet<T>>,
    }
}

unsafe fn hash_map_begin<K, V, S>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic>
where
    S: BuildHasher + Default,
{
    contained(|| CtorCursor {
        state: Box::into_raw(Box::new(HashMap::<K, V, S>::with_capacity_and_hasher(
            len as usize,
            S::default(),
        )))
        .cast(),
    })
}

unsafe fn hash_map_push<K, V, S>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    match contained(|| {
        let (key, value) = std::ptr::read(elem.cast::<(K, V)>());
        let map = &mut *cur.state.cast::<HashMap<K, V, S>>();
        match map.entry(key) {
            std::collections::hash_map::Entry::Occupied(_) => {
                drop(value);
                false
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(value);
                true
            }
        }
    }) {
        Ok(true) => Ok(()),
        Ok(false) => Err(PushError::Duplicate),
        Err(e) => Err(PushError::Panic(e)),
    }
}

unsafe fn hash_map_finish<K, V, S>(
    cur: &mut CtorCursor,
    dst: *mut u8,
) -> Result<(), CallbackPanic> {
    contained(|| {
        let value = *Box::from_raw(cur.state.cast::<HashMap<K, V, S>>());
        cur.state = std::ptr::null_mut();
        std::ptr::write(dst.cast::<HashMap<K, V, S>>(), value);
    })
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
        finish: hash_map_finish::<K, V, S>,
        abort: abort_boxed::<HashMap<K, V, S>>,
        drop_in_place: drop_in_place_thunk::<HashMap<K, V, S>>,
    }
}

unsafe fn btree_map_begin<K, V>(_: *mut u8, _: u32) -> Result<CtorCursor, CallbackPanic> {
    contained(|| CtorCursor {
        state: Box::into_raw(Box::new(BTreeMap::<K, V>::new())).cast(),
    })
}

unsafe fn btree_map_push<K: Ord, V>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    match contained(|| {
        let (key, value) = std::ptr::read(elem.cast::<(K, V)>());
        let map = &mut *cur.state.cast::<BTreeMap<K, V>>();
        match map.entry(key) {
            std::collections::btree_map::Entry::Occupied(_) => {
                drop(value);
                false
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
                true
            }
        }
    }) {
        Ok(true) => Ok(()),
        Ok(false) => Err(PushError::Duplicate),
        Err(e) => Err(PushError::Panic(e)),
    }
}

unsafe fn btree_map_finish<K, V>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    contained(|| {
        let value = *Box::from_raw(cur.state.cast::<BTreeMap<K, V>>());
        cur.state = std::ptr::null_mut();
        std::ptr::write(dst.cast::<BTreeMap<K, V>>(), value);
    })
}

pub fn btree_map_ctor<K: Ord, V>() -> CtorEntry {
    CtorEntry {
        begin: btree_map_begin::<K, V>,
        push: btree_map_push::<K, V>,
        elem_size: size_u32::<(K, V)>(),
        elem_align: align_u32::<(K, V)>(),
        key_offset: std::mem::offset_of!((K, V), 0) as u32,
        value_offset: std::mem::offset_of!((K, V), 1) as u32,
        finish: btree_map_finish::<K, V>,
        abort: abort_boxed::<BTreeMap<K, V>>,
        drop_in_place: drop_in_place_thunk::<BTreeMap<K, V>>,
    }
}

unsafe fn option_begin<T>(_: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    contained(|| {
        assert!(len <= 1, "single-value ctor length exceeds one");
        CtorCursor {
            state: Box::into_raw(Box::new(None::<T>)).cast(),
        }
    })
}

unsafe fn option_push<T>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    contained(|| {
        let slot = &mut *cur.state.cast::<Option<T>>();
        if slot.is_some() {
            drop(std::ptr::read(elem.cast::<T>()));
            false
        } else {
            *slot = Some(std::ptr::read(elem.cast::<T>()));
            true
        }
    })
    .map_err(PushError::Panic)
    .and_then(|inserted| {
        if inserted {
            Ok(())
        } else {
            Err(PushError::Duplicate)
        }
    })
}

unsafe fn box_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    contained(|| {
        let value = Box::from_raw(cur.state.cast::<Option<T>>())
            .take()
            .expect("box ctor requires exactly one element");
        cur.state = std::ptr::null_mut();
        std::ptr::write(dst.cast::<Box<T>>(), Box::new(value));
    })
}

unsafe fn arc_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    contained(|| {
        let value = Box::from_raw(cur.state.cast::<Option<T>>())
            .take()
            .expect("arc ctor requires exactly one element");
        cur.state = std::ptr::null_mut();
        std::ptr::write(dst.cast::<Arc<T>>(), Arc::new(value));
    })
}

unsafe fn option_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    contained(|| {
        let value = *Box::from_raw(cur.state.cast::<Option<T>>());
        cur.state = std::ptr::null_mut();
        std::ptr::write(dst.cast::<Option<T>>(), value);
    })
}

pub fn option_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: option_begin::<T>,
        push: option_push::<T>,
        elem_size: size_u32::<T>(),
        elem_align: align_u32::<T>(),
        key_offset: 0,
        value_offset: 0,
        finish: option_finish::<T>,
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
