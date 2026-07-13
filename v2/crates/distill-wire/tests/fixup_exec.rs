//! The transactional fixup executor (§12): ordered ops over a
//! `MaybeUninit` destination, typed construction through the generated
//! ctor/skip tables, validation before initialization, and the framed
//! constructed-value rollback stack — completing an aggregate disarms
//! its frame's entries and pushes exactly one whole-value entry;
//! failure pops and drops in reverse.

mod common;

use common::*;
use distill_wire::exec::{execute_fixup, Blob, ExecEnv, ExecError, ExecLimits};
use distill_wire::native::{
    CallbackPanic, CtorCursor, CtorEntry, CtorTable, DropTable, DropThunk, NativeLayoutNode,
    NativeTagEncoding, NativeVariantTag, PushError, ScalarKind, SkipEntry, SkipWriterTable,
};
use distill_wire::plan::{compile_plans, PlanId};
use distill_wire::wire::{WireEnumForm, WireNode};
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::sync::Arc;

// --- event log ---------------------------------------------------------------

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn log_event(s: impl Into<String>) {
    EVENTS.with(|e| e.borrow_mut().push(s.into()));
}

fn take_events() -> Vec<String> {
    EVENTS.with(|e| std::mem::take(&mut *e.borrow_mut()))
}

fn count(events: &[String], needle: &str) -> usize {
    events.iter().filter(|e| e.as_str() == needle).count()
}

// --- tracked test types ------------------------------------------------------

/// Layout: one u32 field, plus drop glue that logs.
#[repr(transparent)]
#[derive(Debug, PartialEq)]
struct Tracked(u32);

impl Drop for Tracked {
    fn drop(&mut self) {
        log_event(format!("T:{}", self.0));
    }
}

fn tracked_native(offset: u32) -> NativeLayoutNode {
    // struct Tracked(u32) with drop glue: whole_drop = DropId(TRACKED_DROP).
    nstruct_glue(
        offset,
        4,
        4,
        TRACKED_DROP,
        vec![nfield("0", 0, scalar(0, ScalarKind::U32))],
    )
}

fn tracked_wire(offset: u32) -> WireNode {
    wstruct(
        offset,
        4,
        4,
        vec![wfield("0", 0, wprim(0, ScalarKind::U32))],
    )
}

/// { id: u32, flag: bool } with drop glue that logs — bool gives the
/// element plan a failure point.
#[repr(C)]
#[derive(Debug, PartialEq)]
struct TrackedPair {
    id: u32,
    flag: bool,
}

impl Drop for TrackedPair {
    fn drop(&mut self) {
        log_event(format!("TP:{}", self.id));
    }
}

fn tracked_pair_native(offset: u32) -> NativeLayoutNode {
    nstruct_glue(
        offset,
        8,
        4,
        TRACKED_PAIR_DROP,
        vec![
            nfield("id", 0, scalar(0, ScalarKind::U32)),
            nfield("flag", 1, scalar(4, ScalarKind::Bool)),
        ],
    )
}

fn tracked_pair_wire(offset: u32) -> WireNode {
    wstruct(
        offset,
        8,
        4,
        vec![
            wfield("id", 0, wprim(0, ScalarKind::U32)),
            wfield("flag", 1, wprim(4, ScalarKind::Bool)),
        ],
    )
}

// --- drop table --------------------------------------------------------------

const TRACKED_DROP: u32 = 0;
const TRACKED_PAIR_DROP: u32 = 1;
const INNER_DROP: u32 = 2;
const NODE_DROP: u32 = 3;
const OPTBOX_DROP: u32 = 4;
const ENUM_DROP: u32 = 5;

#[repr(C)]
struct Inner {
    v: Vec<Tracked>,
}

#[repr(C)]
#[derive(Debug, PartialEq)]
struct Node {
    val: u32,
    next: Vec<Node>,
}

/// A fabricated canonical-form enum destination: valid only under tag 0
/// (payload live). Tag byte at 0, Vec payload at 8.
#[repr(C)]
struct FakeEnum {
    tag: u8,
    _pad: [u8; 7],
    payload: Vec<Tracked>,
}

unsafe fn drop_tracked(ptr: *mut u8) -> Result<(), CallbackPanic> {
    std::ptr::drop_in_place(ptr as *mut Tracked);
    Ok(())
}
unsafe fn drop_tracked_pair(ptr: *mut u8) -> Result<(), CallbackPanic> {
    std::ptr::drop_in_place(ptr as *mut TrackedPair);
    Ok(())
}
unsafe fn drop_inner(ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event("whole:Inner");
    std::ptr::drop_in_place(ptr as *mut Inner);
    Ok(())
}
unsafe fn drop_node(ptr: *mut u8) -> Result<(), CallbackPanic> {
    std::ptr::drop_in_place(ptr as *mut Node);
    Ok(())
}
unsafe fn drop_optbox(ptr: *mut u8) -> Result<(), CallbackPanic> {
    std::ptr::drop_in_place(ptr as *mut Option<Box<u32>>);
    Ok(())
}
unsafe fn drop_fake_enum(ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event("whole:Enum");
    let e = &mut *(ptr as *mut FakeEnum);
    if e.tag == 0 {
        std::ptr::drop_in_place(&mut e.payload as *mut Vec<Tracked>);
    }
    Ok(())
}

unsafe fn drop_fails(_ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event("drop:failed");
    Err(CallbackPanic)
}

fn drop_table() -> Vec<DropThunk> {
    vec![
        drop_tracked,
        drop_tracked_pair,
        drop_inner,
        drop_node,
        drop_optbox,
        drop_fake_enum,
        drop_fails,
    ]
}

// --- generic ctor entries ----------------------------------------------------

fn tname<T>() -> &'static str {
    let full = std::any::type_name::<T>();
    full.rsplit("::").next().unwrap_or(full)
}

unsafe fn vec_begin<T>(_dst: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    let b = Box::new(Vec::<T>::with_capacity(len as usize));
    Ok(CtorCursor {
        state: Box::into_raw(b) as *mut (),
    })
}
unsafe fn vec_push<T>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    (*(cur.state as *mut Vec<T>)).push(std::ptr::read(elem as *const T));
    Ok(())
}
unsafe fn vec_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    let v = *Box::from_raw(cur.state as *mut Vec<T>);
    cur.state = std::ptr::null_mut();
    std::ptr::write(dst as *mut Vec<T>, v);
    Ok(())
}
unsafe fn vec_abort<T>(cur: CtorCursor) -> Result<(), CallbackPanic> {
    if !cur.state.is_null() {
        log_event(format!("abort:vec<{}>", tname::<T>()));
        drop(Box::from_raw(cur.state as *mut Vec<T>));
    }
    Ok(())
}
unsafe fn vec_drop<T>(ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event(format!("ctor_drop:vec<{}>", tname::<T>()));
    std::ptr::drop_in_place(ptr as *mut Vec<T>);
    Ok(())
}

fn vec_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: vec_begin::<T>,
        push: vec_push::<T>,
        elem_size: std::mem::size_of::<T>() as u32,
        elem_align: std::mem::align_of::<T>() as u32,
        key_offset: 0,
        value_offset: 0,
        finish: vec_finish::<T>,
        abort: vec_abort::<T>,
        drop_in_place: vec_drop::<T>,
    }
}

unsafe fn set_begin(_dst: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    let b = Box::new(std::collections::HashSet::<u32>::with_capacity(
        len as usize,
    ));
    Ok(CtorCursor {
        state: Box::into_raw(b) as *mut (),
    })
}
unsafe fn set_push(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    let v = std::ptr::read(elem as *const u32);
    if (*(cur.state as *mut std::collections::HashSet<u32>)).insert(v) {
        Ok(())
    } else {
        Err(PushError::Duplicate)
    }
}
unsafe fn set_finish(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    let v = *Box::from_raw(cur.state as *mut std::collections::HashSet<u32>);
    cur.state = std::ptr::null_mut();
    std::ptr::write(dst as *mut std::collections::HashSet<u32>, v);
    Ok(())
}
unsafe fn set_abort(cur: CtorCursor) -> Result<(), CallbackPanic> {
    if !cur.state.is_null() {
        log_event("abort:set<u32>");
        drop(Box::from_raw(
            cur.state as *mut std::collections::HashSet<u32>,
        ));
    }
    Ok(())
}
unsafe fn set_drop(ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event("ctor_drop:set<u32>");
    std::ptr::drop_in_place(ptr as *mut std::collections::HashSet<u32>);
    Ok(())
}

fn set_u32_ctor() -> CtorEntry {
    CtorEntry {
        begin: set_begin,
        push: set_push,
        elem_size: 4,
        elem_align: 4,
        key_offset: 0,
        value_offset: 0,
        finish: set_finish,
        abort: set_abort,
        drop_in_place: set_drop,
    }
}

#[repr(C)]
struct MapPair<K, V> {
    k: K,
    v: V,
}

unsafe fn map_begin<K, V>(_dst: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic>
where
    K: std::hash::Hash + Eq,
{
    let b = Box::new(std::collections::HashMap::<K, V>::with_capacity(
        len as usize,
    ));
    Ok(CtorCursor {
        state: Box::into_raw(b) as *mut (),
    })
}
unsafe fn map_push<K, V>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError>
where
    K: std::hash::Hash + Eq,
{
    let pair = std::ptr::read(elem as *const MapPair<K, V>);
    let map = &mut *(cur.state as *mut std::collections::HashMap<K, V>);
    if map.contains_key(&pair.k) {
        // Consumed either way: pair drops here.
        return Err(PushError::Duplicate);
    }
    map.insert(pair.k, pair.v);
    Ok(())
}
unsafe fn map_finish<K, V>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic>
where
    K: std::hash::Hash + Eq,
{
    let v = *Box::from_raw(cur.state as *mut std::collections::HashMap<K, V>);
    cur.state = std::ptr::null_mut();
    std::ptr::write(dst as *mut std::collections::HashMap<K, V>, v);
    Ok(())
}
unsafe fn map_abort<K, V>(cur: CtorCursor) -> Result<(), CallbackPanic> {
    if !cur.state.is_null() {
        log_event(format!("abort:map<{},{}>", tname::<K>(), tname::<V>()));
        drop(Box::from_raw(
            cur.state as *mut std::collections::HashMap<K, V>,
        ));
    }
    Ok(())
}
unsafe fn map_drop<K, V>(ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event(format!("ctor_drop:map<{},{}>", tname::<K>(), tname::<V>()));
    std::ptr::drop_in_place(ptr as *mut std::collections::HashMap<K, V>);
    Ok(())
}

fn map_ctor<K, V>() -> CtorEntry
where
    K: std::hash::Hash + Eq,
{
    CtorEntry {
        begin: map_begin::<K, V>,
        push: map_push::<K, V>,
        elem_size: std::mem::size_of::<MapPair<K, V>>() as u32,
        elem_align: std::mem::align_of::<MapPair<K, V>>() as u32,
        key_offset: std::mem::offset_of!(MapPair<K, V>, k) as u32,
        value_offset: std::mem::offset_of!(MapPair<K, V>, v) as u32,
        finish: map_finish::<K, V>,
        abort: map_abort::<K, V>,
        drop_in_place: map_drop::<K, V>,
    }
}

unsafe fn once_begin<T>(_dst: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic> {
    assert_eq!(len, 1, "Box/Arc use len = 1");
    let b = Box::new(Option::<T>::None);
    Ok(CtorCursor {
        state: Box::into_raw(b) as *mut (),
    })
}
unsafe fn once_push<T>(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    *(cur.state as *mut Option<T>) = Some(std::ptr::read(elem as *const T));
    Ok(())
}
unsafe fn once_abort<T>(cur: CtorCursor) -> Result<(), CallbackPanic> {
    if !cur.state.is_null() {
        log_event(format!("abort:once<{}>", tname::<T>()));
        drop(Box::from_raw(cur.state as *mut Option<T>));
    }
    Ok(())
}
unsafe fn box_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    let v = (*Box::from_raw(cur.state as *mut Option<T>)).expect("pushed");
    cur.state = std::ptr::null_mut();
    std::ptr::write(dst as *mut Box<T>, Box::new(v));
    Ok(())
}
unsafe fn box_drop<T>(ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event(format!("ctor_drop:box<{}>", tname::<T>()));
    std::ptr::drop_in_place(ptr as *mut Box<T>);
    Ok(())
}
unsafe fn arc_finish<T>(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic> {
    let v = (*Box::from_raw(cur.state as *mut Option<T>)).expect("pushed");
    cur.state = std::ptr::null_mut();
    std::ptr::write(dst as *mut Arc<T>, Arc::new(v));
    Ok(())
}
unsafe fn arc_drop<T>(ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event(format!("ctor_drop:arc<{}>", tname::<T>()));
    std::ptr::drop_in_place(ptr as *mut Arc<T>);
    Ok(())
}

fn box_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: once_begin::<T>,
        push: once_push::<T>,
        elem_size: std::mem::size_of::<T>() as u32,
        elem_align: std::mem::align_of::<T>() as u32,
        key_offset: 0,
        value_offset: 0,
        finish: box_finish::<T>,
        abort: once_abort::<T>,
        drop_in_place: box_drop::<T>,
    }
}

fn arc_ctor<T>() -> CtorEntry {
    CtorEntry {
        begin: once_begin::<T>,
        push: once_push::<T>,
        elem_size: std::mem::size_of::<T>() as u32,
        elem_align: std::mem::align_of::<T>() as u32,
        key_offset: 0,
        value_offset: 0,
        finish: arc_finish::<T>,
        abort: once_abort::<T>,
        drop_in_place: arc_drop::<T>,
    }
}

// Failing ctor variants.
unsafe fn begin_fails(_dst: *mut u8, _len: u32) -> Result<CtorCursor, CallbackPanic> {
    Err(CallbackPanic)
}
unsafe fn finish_fails(_cur: &mut CtorCursor, _dst: *mut u8) -> Result<(), CallbackPanic> {
    Err(CallbackPanic)
}
/// A push that "panics" on Tracked(42): the element was already moved in
/// and dropped by the failing callback — no thunk can restore it.
unsafe fn push_panics_on_42(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError> {
    let v = std::ptr::read(elem as *const Tracked);
    if v.0 == 42 {
        drop(v); // logs T:42 — the callback consumed it
        return Err(PushError::Panic(CallbackPanic));
    }
    (*(cur.state as *mut Vec<Tracked>)).push(v);
    Ok(())
}

fn vec_tracked_begin_fails() -> CtorEntry {
    CtorEntry {
        begin: begin_fails,
        ..vec_ctor::<Tracked>()
    }
}
fn vec_tracked_finish_fails() -> CtorEntry {
    CtorEntry {
        finish: finish_fails,
        ..vec_ctor::<Tracked>()
    }
}
fn vec_tracked_push_panics() -> CtorEntry {
    CtorEntry {
        push: push_panics_on_42,
        ..vec_ctor::<Tracked>()
    }
}

unsafe fn abort_fails(_cur: CtorCursor) -> Result<(), CallbackPanic> {
    log_event("abort:failed");
    Err(CallbackPanic)
}

unsafe fn ctor_drop_fails(_ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event("ctor_drop:failed");
    Err(CallbackPanic)
}

// --- skip entries ------------------------------------------------------------

unsafe fn skip_write_u32(dst: *mut u8) -> Result<(), CallbackPanic> {
    (dst as *mut u32).write_unaligned(0x1122_3344);
    Ok(())
}
unsafe fn skip_drop_noop(_ptr: *mut u8) -> Result<(), CallbackPanic> {
    Ok(())
}
unsafe fn skip_write_tracked(dst: *mut u8) -> Result<(), CallbackPanic> {
    log_event("skip:write");
    std::ptr::write(dst as *mut Tracked, Tracked(99));
    Ok(())
}
unsafe fn skip_drop_tracked(ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event("skip:drop");
    std::ptr::drop_in_place(ptr as *mut Tracked);
    Ok(())
}
unsafe fn skip_drop_fails(_ptr: *mut u8) -> Result<(), CallbackPanic> {
    log_event("skip:drop_failed");
    Err(CallbackPanic)
}
unsafe fn skip_write_fails(_dst: *mut u8) -> Result<(), CallbackPanic> {
    Err(CallbackPanic)
}

fn skip_u32_entry() -> SkipEntry {
    SkipEntry {
        write: skip_write_u32,
        drop_in_place: skip_drop_noop,
    }
}
fn skip_tracked_entry() -> SkipEntry {
    SkipEntry {
        write: skip_write_tracked,
        drop_in_place: skip_drop_tracked,
    }
}
fn skip_fails_entry() -> SkipEntry {
    SkipEntry {
        write: skip_write_fails,
        drop_in_place: skip_drop_noop,
    }
}

// --- env and run helpers -----------------------------------------------------

fn make_env(ctors: Vec<CtorEntry>, skips: Vec<SkipEntry>, blobs: Vec<Blob>) -> ExecEnv<'static> {
    ExecEnv {
        ctors: leak(CtorTable {
            entries: leak_slice(ctors),
        }),
        drops: leak(DropTable {
            entries: leak_slice(drop_table()),
        }),
        skips: leak(SkipWriterTable {
            entries: leak_slice(skips),
        }),
        blobs: leak_slice(blobs),
        limits: ExecLimits::default(),
    }
}

/// Compile and execute into a zeroed `T`; on success the value is
/// returned owned.
fn run_value<T>(
    wire: &WireNode,
    native: &NativeLayoutNode,
    fixed: &[u8],
    var: &[u8],
    env: &ExecEnv,
) -> Result<T, ExecError> {
    let compiled = compile_plans(wire, native).expect("plans compile");
    let mut dst = MaybeUninit::<T>::zeroed();
    unsafe {
        execute_fixup(
            &compiled,
            PlanId(0),
            fixed,
            var,
            env,
            dst.as_mut_ptr() as *mut u8,
        )
        .map(|()| dst.assume_init())
    }
}

fn varref(offset: u32, len: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(8);
    v.extend_from_slice(&offset.to_le_bytes());
    v.extend_from_slice(&len.to_le_bytes());
    v
}

/// A 24-byte indirection slot image: VarRef + zeroed remainder.
fn slot24(offset: u32, len: u32) -> Vec<u8> {
    let mut v = varref(offset, len);
    v.resize(24, 0);
    v
}

/// A 48-byte indirection slot image (sets/maps): VarRef + zeroed remainder.
fn slot48(offset: u32, len: u32) -> Vec<u8> {
    let mut v = varref(offset, len);
    v.resize(48, 0);
    v
}

#[repr(C, align(8))]
#[derive(Clone, Copy, PartialEq, Debug)]
struct Bytes8([u8; 8]);

// --- flat copies and validation ----------------------------------------------

#[test]
fn diverging_offsets_scatter_copy_into_the_destination() {
    // wire: a u16 @0, b u32 @4; native: b u32 @0, a u16 @4.
    let wire = wstruct(
        0,
        8,
        4,
        vec![
            wfield("a", 0, wprim(0, ScalarKind::U16)),
            wfield("b", 1, wprim(4, ScalarKind::U32)),
        ],
    );
    let native = nstruct(
        0,
        8,
        4,
        vec![
            nfield("a", 0, scalar(4, ScalarKind::U16)),
            nfield("b", 1, scalar(0, ScalarKind::U32)),
        ],
    );
    #[repr(C)]
    #[derive(Debug, PartialEq)]
    struct Dst {
        b: u32,
        a: u16,
    }
    let mut fixed = vec![0u8; 8];
    fixed[0..2].copy_from_slice(&0xBEEFu16.to_le_bytes());
    fixed[4..8].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    let env = make_env(vec![], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert_eq!(
        got,
        Dst {
            b: 0xDEAD_BEEF,
            a: 0xBEEF
        }
    );
}

#[test]
fn valid_bool_and_char_bit_patterns_pass() {
    let wire = wstruct(
        0,
        8,
        4,
        vec![
            wfield("flag", 0, wprim(0, ScalarKind::Bool)),
            wfield("c", 1, wprim(4, ScalarKind::Char)),
        ],
    );
    let native = nstruct(
        0,
        8,
        4,
        vec![
            nfield("flag", 0, scalar(0, ScalarKind::Bool)),
            nfield("c", 1, scalar(4, ScalarKind::Char)),
        ],
    );
    #[repr(C)]
    struct Dst {
        flag: bool,
        c: char,
    }
    let mut fixed = vec![0u8; 8];
    fixed[0] = 1;
    fixed[4..8].copy_from_slice(&(0x2603u32).to_le_bytes()); // '☃'
    let env = make_env(vec![], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert!(got.flag);
    assert_eq!(got.c, '\u{2603}');
}

#[test]
fn invalid_bool_bit_pattern_is_an_integrity_error() {
    let wire = wstruct(0, 1, 1, vec![wfield("flag", 0, wprim(0, ScalarKind::Bool))]);
    let native = nstruct(
        0,
        1,
        1,
        vec![nfield("flag", 0, scalar(0, ScalarKind::Bool))],
    );
    let env = make_env(vec![], vec![], vec![]);
    let err = run_value::<u8>(&wire, &native, &[2], &[], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

#[test]
fn invalid_char_bit_pattern_is_an_integrity_error() {
    let wire = wstruct(0, 4, 4, vec![wfield("c", 0, wprim(0, ScalarKind::Char))]);
    let native = nstruct(0, 4, 4, vec![nfield("c", 0, scalar(0, ScalarKind::Char))]);
    let env = make_env(vec![], vec![], vec![]);
    // 0xD800: a surrogate, not a char.
    let err = run_value::<u32>(&wire, &native, &0xD800u32.to_le_bytes(), &[], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

#[test]
fn fixed_section_size_mismatch_is_an_integrity_error() {
    let wire = wstruct(0, 4, 4, vec![wfield("x", 0, wprim(0, ScalarKind::U32))]);
    let native = nstruct(0, 4, 4, vec![nfield("x", 0, scalar(0, ScalarKind::U32))]);
    let env = make_env(vec![], vec![], vec![]);
    let err = run_value::<u32>(&wire, &native, &[0u8; 3], &[], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

// --- strings -------------------------------------------------------------------

#[test]
fn construct_string_from_the_variable_section() {
    let wire = wstruct(0, 24, 8, vec![wfield("s", 0, wstring_slot(0))]);
    let native = nstruct(0, 24, 8, vec![nfield("s", 0, nstr(0))]);
    #[repr(C)]
    struct Dst {
        s: String,
    }
    let fixed = slot24(1, 5);
    let var = b"xhello";
    let env = make_env(vec![], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, var, &env).unwrap();
    assert_eq!(got.s, "hello");
}

#[test]
fn invalid_utf8_is_an_integrity_error() {
    let wire = wstruct(0, 24, 8, vec![wfield("s", 0, wstring_slot(0))]);
    let native = nstruct(0, 24, 8, vec![nfield("s", 0, nstr(0))]);
    let fixed = slot24(0, 2);
    let env = make_env(vec![], vec![], vec![]);
    let err = run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &[0xFF, 0xFE], &env)
        .unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

#[test]
fn varref_out_of_bounds_is_an_integrity_error() {
    let wire = wstruct(0, 24, 8, vec![wfield("s", 0, wstring_slot(0))]);
    let native = nstruct(0, 24, 8, vec![nfield("s", 0, nstr(0))]);
    let fixed = slot24(4, 10); // var section is 8 bytes
    let env = make_env(vec![], vec![], vec![]);
    let err =
        run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &[0u8; 8], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

#[test]
fn huge_varref_length_is_an_integrity_error() {
    // u32::MAX elements of stride 4: checked arithmetic must reject, not wrap.
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 0))],
    );
    let fixed = slot24(0, u32::MAX);
    let env = make_env(vec![vec_ctor::<u32>()], vec![], vec![]);
    let err =
        run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &[0u8; 16], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

#[test]
fn misaligned_varref_offset_is_an_integrity_error() {
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 0))],
    );
    let fixed = slot24(2, 1); // offset 2 is not 4-aligned for a u32 element
    let env = make_env(vec![vec_ctor::<u32>()], vec![], vec![]);
    let err =
        run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &[0u8; 8], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

// --- containers ----------------------------------------------------------------

#[test]
fn construct_vec_of_scalars() {
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 0))],
    );
    #[repr(C)]
    struct Dst {
        v: Vec<u32>,
    }
    let fixed = slot24(0, 3);
    let mut var = Vec::new();
    for x in [10u32, 20, 30] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![vec_ctor::<u32>()], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, &var, &env).unwrap();
    assert_eq!(got.v, vec![10, 20, 30]);
}

#[test]
fn empty_vec_constructs() {
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 0))],
    );
    #[repr(C)]
    struct Dst {
        v: Vec<u32>,
    }
    let fixed = slot24(0, 0);
    let env = make_env(vec![vec_ctor::<u32>()], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert_eq!(got.v, Vec::<u32>::new());
}

#[test]
fn construct_vec_of_strings_via_element_plans() {
    // Element VarRefs are variable-section relative, wherever the element sits.
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, wstring_slot(0)))],
    );
    let native = nstruct(0, 24, 8, vec![nfield("v", 0, nvec(0, nstr(0), 0))]);
    #[repr(C)]
    struct Dst {
        v: Vec<String>,
    }
    let fixed = slot24(0, 2);
    let mut var = Vec::new();
    var.extend_from_slice(&slot24(48, 2)); // "ab"
    var.extend_from_slice(&slot24(50, 3)); // "xyz"
    var.extend_from_slice(b"abxyz");
    let env = make_env(vec![vec_ctor::<String>()], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, &var, &env).unwrap();
    assert_eq!(got.v, vec!["ab".to_string(), "xyz".to_string()]);
}

#[test]
fn construct_set_by_real_insertion() {
    let wire = wstruct(
        0,
        48,
        8,
        vec![wfield("s", 0, wset_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        48,
        8,
        vec![nfield("s", 0, nset(0, scalar(0, ScalarKind::U32), 0))],
    );
    #[repr(C)]
    struct Dst {
        s: std::collections::HashSet<u32>,
    }
    let fixed = slot48(0, 3);
    let mut var = Vec::new();
    for x in [7u32, 8, 9] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![set_u32_ctor()], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, &var, &env).unwrap();
    assert_eq!(got.s, [7, 8, 9].into_iter().collect());
}

#[test]
fn duplicate_set_element_is_an_integrity_error() {
    let wire = wstruct(
        0,
        48,
        8,
        vec![wfield("s", 0, wset_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        48,
        8,
        vec![nfield("s", 0, nset(0, scalar(0, ScalarKind::U32), 0))],
    );
    let fixed = slot48(0, 2);
    let mut var = Vec::new();
    for x in [7u32, 7] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![set_u32_ctor()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 48]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
    // The partial set is aborted through its cursor.
    assert_eq!(count(&take_events(), "abort:set<u32>"), 1);
}

#[test]
fn construct_map_from_flattened_key_value_pairs() {
    // Wire pair layout: key at 0, value at align_up(key size, value align).
    let wire = wstruct(
        0,
        48,
        8,
        vec![wfield(
            "m",
            0,
            wmap_slot(0, wprim(0, ScalarKind::U32), wprim(0, ScalarKind::U64)),
        )],
    );
    let native = nstruct(
        0,
        48,
        8,
        vec![nfield(
            "m",
            0,
            nmap(0, scalar(0, ScalarKind::U32), scalar(0, ScalarKind::U64), 0),
        )],
    );
    #[repr(C)]
    struct Dst {
        m: std::collections::HashMap<u32, u64>,
    }
    let fixed = slot48(0, 2);
    let mut var = Vec::new();
    // pair stride: value at 8, size 16.
    var.extend_from_slice(&1u32.to_le_bytes());
    var.extend_from_slice(&[0u8; 4]);
    var.extend_from_slice(&100u64.to_le_bytes());
    var.extend_from_slice(&2u32.to_le_bytes());
    var.extend_from_slice(&[0u8; 4]);
    var.extend_from_slice(&200u64.to_le_bytes());
    let env = make_env(vec![map_ctor::<u32, u64>()], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, &var, &env).unwrap();
    assert_eq!(got.m, [(1, 100), (2, 200)].into_iter().collect());
}

#[test]
fn duplicate_map_key_is_an_integrity_error_and_partial_state_aborts() {
    let wire = wstruct(
        0,
        48,
        8,
        vec![wfield(
            "m",
            0,
            wmap_slot(0, wprim(0, ScalarKind::U32), tracked_wire(0)),
        )],
    );
    let native = nstruct(
        0,
        48,
        8,
        vec![nfield(
            "m",
            0,
            nmap(0, scalar(0, ScalarKind::U32), tracked_native(0), 0),
        )],
    );
    let fixed = slot48(0, 2);
    let mut var = Vec::new();
    // pair: key u32 @0, Tracked payload u32 @4, stride 8.
    var.extend_from_slice(&1u32.to_le_bytes());
    var.extend_from_slice(&10u32.to_le_bytes());
    var.extend_from_slice(&1u32.to_le_bytes()); // duplicate key
    var.extend_from_slice(&20u32.to_le_bytes());
    let env = make_env(vec![map_ctor::<u32, Tracked>()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 48]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
    let events = take_events();
    // The duplicate pair was consumed by push (T:20), then the partial
    // map aborted through its cursor, dropping the first entry (T:10).
    assert_eq!(count(&events, "T:20"), 1);
    assert_eq!(count(&events, "abort:map<u32,Tracked>"), 1);
    assert_eq!(count(&events, "T:10"), 1);
}

#[test]
fn key_constructed_then_value_failure_unwinds_the_key_and_aborts() {
    // Map<u32, TrackedPair>: entry 2's value has an invalid bool.
    let wire = wstruct(
        0,
        48,
        8,
        vec![wfield(
            "m",
            0,
            wmap_slot(0, wprim(0, ScalarKind::U32), tracked_pair_wire(0)),
        )],
    );
    let native = nstruct(
        0,
        48,
        8,
        vec![nfield(
            "m",
            0,
            nmap(0, scalar(0, ScalarKind::U32), tracked_pair_native(0), 0),
        )],
    );
    let fixed = slot48(0, 2);
    // wire pair: key u32 @0, TrackedPair @4 (size 8, align 4), stride 12.
    let mut var = Vec::new();
    var.extend_from_slice(&1u32.to_le_bytes());
    var.extend_from_slice(&10u32.to_le_bytes());
    var.extend_from_slice(&[1, 0, 0, 0]); // flag=1 ok, padding
    var.extend_from_slice(&2u32.to_le_bytes());
    var.extend_from_slice(&20u32.to_le_bytes());
    var.extend_from_slice(&[2, 0, 0, 0]); // flag=2: invalid bool
    let env = make_env(vec![map_ctor::<u32, TrackedPair>()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 48]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
    let events = take_events();
    // Entry 1's pair is cursor-owned partial state, dropped by abort.
    assert_eq!(count(&events, "abort:map<u32,TrackedPair>"), 1);
    assert_eq!(count(&events, "TP:10"), 1);
    // Entry 2's value never completed construction.
    assert_eq!(count(&events, "TP:20"), 0);
}

// --- box / arc / blob ----------------------------------------------------------

#[test]
fn construct_box_and_arc() {
    let wire = wstruct(
        0,
        16,
        8,
        vec![
            wfield("b", 0, wbox_slot(0, wprim(0, ScalarKind::U32))),
            wfield("a", 1, warc_slot(8, wprim(0, ScalarKind::U16))),
        ],
    );
    let native = nstruct(
        0,
        16,
        8,
        vec![
            nfield("b", 0, nbox(0, scalar(0, ScalarKind::U32), 0)),
            nfield("a", 1, narc(8, scalar(0, ScalarKind::U16), 1)),
        ],
    );
    #[repr(C)]
    struct Dst {
        b: Box<u32>,
        a: Arc<u16>,
    }
    let mut fixed = Vec::new();
    fixed.extend_from_slice(&varref(0, 4)); // box pointee: 4 bytes at var 0
    fixed.extend_from_slice(&varref(4, 2)); // arc pointee: 2 bytes at var 4
    let mut var = Vec::new();
    var.extend_from_slice(&42u32.to_le_bytes());
    var.extend_from_slice(&7u16.to_le_bytes());
    let env = make_env(vec![box_ctor::<u32>(), arc_ctor::<u16>()], vec![], vec![]);
    let got: Dst = run_value(&wire, &native, &fixed, &var, &env).unwrap();
    assert_eq!(*got.b, 42);
    assert_eq!(*got.a, 7);
    assert_eq!(Arc::strong_count(&got.a), 1); // constructed fresh
}

#[test]
fn box_varref_len_must_equal_the_pointee_wire_size() {
    let wire = wstruct(
        0,
        8,
        8,
        vec![wfield("b", 0, wbox_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        8,
        8,
        vec![nfield("b", 0, nbox(0, scalar(0, ScalarKind::U32), 0))],
    );
    let fixed = varref(0, 3); // pointee is 4 bytes, len says 3
    let env = make_env(vec![box_ctor::<u32>()], vec![], vec![]);
    let err =
        run_value::<MaybeUninit<[u8; 8]>>(&wire, &native, &fixed, &[0u8; 4], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

#[test]
fn construct_blob_borrows_the_backing_without_copying() {
    let backing: Arc<Vec<u8>> = Arc::new(vec![0, 1, 2, 3, 4, 5, 6, 7]);
    let base = backing.as_ptr();
    let blob = Blob::new(backing.clone(), 2, 3);
    assert_eq!(std::mem::size_of::<Blob>(), 32);

    let blob_size = std::mem::size_of::<Blob>() as u32;
    let wire = wstruct(
        0,
        blob_size,
        8,
        vec![wfield("b", 0, wblob_slot(0, blob_size))],
    );
    let native = nstruct(0, blob_size, 8, vec![nfield("b", 0, nblob(0, blob_size))]);
    #[repr(C)]
    struct Dst {
        b: Blob,
    }
    // BlobRef { index: 0, zero: 0 } + zeroed slot remainder.
    let fixed = vec![0u8; blob_size as usize];
    let env = make_env(vec![], vec![], vec![blob]);
    let got: Dst = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert_eq!(got.b.as_bytes(), &[2, 3, 4]);
    // A borrow of the same backing, never a copy.
    assert_eq!(got.b.as_bytes().as_ptr(), unsafe { base.add(2) });
}

#[test]
fn blobref_nonzero_pad_is_an_integrity_error() {
    let blob_size = std::mem::size_of::<Blob>() as u32;
    let wire = wstruct(
        0,
        blob_size,
        8,
        vec![wfield("b", 0, wblob_slot(0, blob_size))],
    );
    let native = nstruct(0, blob_size, 8, vec![nfield("b", 0, nblob(0, blob_size))]);
    let mut fixed = vec![0u8; blob_size as usize];
    fixed[4] = 1; // BlobRef.zero != 0
    let blob = Blob::new(Arc::new(vec![1u8, 2]), 0, 2);
    let env = make_env(vec![], vec![], vec![blob]);
    let err = run_value::<MaybeUninit<[u8; 32]>>(&wire, &native, &fixed, &[], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

#[test]
fn blobref_index_out_of_range_is_an_integrity_error() {
    let blob_size = std::mem::size_of::<Blob>() as u32;
    let wire = wstruct(
        0,
        blob_size,
        8,
        vec![wfield("b", 0, wblob_slot(0, blob_size))],
    );
    let native = nstruct(0, blob_size, 8, vec![nfield("b", 0, nblob(0, blob_size))]);
    let mut fixed = vec![0u8; blob_size as usize];
    fixed[0] = 3; // index 3, table has 1 entry
    let blob = Blob::new(Arc::new(vec![1u8, 2]), 0, 2);
    let env = make_env(vec![], vec![], vec![blob]);
    let err = run_value::<MaybeUninit<[u8; 32]>>(&wire, &native, &fixed, &[], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

// --- skip defaults ---------------------------------------------------------------

#[test]
fn skip_defaults_are_written_at_consumer_native_offsets() {
    // wire: { x: u32 } (skip slots are not encoded); native: x @0, skip @4.
    let wire = wstruct(0, 4, 4, vec![wfield("x", 0, wprim(0, ScalarKind::U32))]);
    let native = nstruct(
        0,
        8,
        4,
        vec![
            nfield("x", 0, scalar(0, ScalarKind::U32)),
            nfield("cache", 1, nskip(4, 4, 4, 0)),
        ],
    );
    let env = make_env(vec![], vec![skip_u32_entry()], vec![]);
    let got: Bytes8 = run_value(&wire, &native, &5u32.to_le_bytes(), &[], &env).unwrap();
    let mut want = [0u8; 8];
    want[0..4].copy_from_slice(&5u32.to_le_bytes());
    want[4..8].copy_from_slice(&0x1122_3344u32.to_le_bytes());
    assert_eq!(got, Bytes8(want));
}

#[test]
fn skip_writer_failure_is_a_callback_error() {
    let wire = wstruct(0, 4, 4, vec![wfield("x", 0, wprim(0, ScalarKind::U32))]);
    let native = nstruct(
        0,
        8,
        4,
        vec![
            nfield("x", 0, scalar(0, ScalarKind::U32)),
            nfield("cache", 1, nskip(4, 4, 4, 0)),
        ],
    );
    let env = make_env(vec![], vec![skip_fails_entry()], vec![]);
    let err = run_value::<Bytes8>(&wire, &native, &5u32.to_le_bytes(), &[], &env).unwrap_err();
    assert!(matches!(err, ExecError::Callback { .. }), "{err:?}");
}

#[test]
fn skip_default_rolls_back_through_its_paired_drop() {
    // native: skip Tracked @0, set @8 (duplicate → failure after the skip wrote).
    let wire = wstruct(
        0,
        48,
        8,
        vec![wfield("s", 0, wset_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        56,
        8,
        vec![
            nfield("cache", 0, nskip(0, 4, 4, 0)),
            nfield("s", 1, nset(8, scalar(0, ScalarKind::U32), 0)),
        ],
    );
    let fixed = slot48(0, 2);
    let mut var = Vec::new();
    for x in [7u32, 7] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![set_u32_ctor()], vec![skip_tracked_entry()], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 56]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
    let events = take_events();
    assert_eq!(
        events,
        vec![
            "skip:write".to_string(),
            "abort:set<u32>".to_string(),
            "skip:drop".to_string(),
            "T:99".to_string(),
        ]
    );
}

// --- enums --------------------------------------------------------------------

/// Canonical wire enum over a direct-tagged native enum:
/// native: tag u8 @0 (A=0, B=1), A payload u32 @4; size 8, align 4.
/// wire: tag u32 @0, payload @4; size 8, align 4.
fn canonical_direct_pair() -> (WireNode, NativeLayoutNode) {
    let wire = wenum(
        0,
        8,
        4,
        WireEnumForm::Canonical,
        vec![
            wvariant("A", 0, wprim(4, ScalarKind::U32)),
            wvariant("B", 1, WireNode::Unit { offset: 4 }),
        ],
    );
    let native = nenum(
        0,
        8,
        4,
        NativeTagEncoding::Direct { offset: 0, size: 1 },
        vec![
            nvariant(
                "A",
                0,
                NativeVariantTag::Direct { value: 0 },
                scalar(4, ScalarKind::U32),
            ),
            nvariant(
                "B",
                1,
                NativeVariantTag::Direct { value: 1 },
                NativeLayoutNode::Unit { offset: 4 },
            ),
        ],
    );
    (wire, native)
}

#[test]
fn canonical_tag_selects_the_variant_and_writes_the_native_tag() {
    let (wire, native) = canonical_direct_pair();
    let env = make_env(vec![], vec![], vec![]);

    // Variant A (name-sorted index 0), payload 99.
    let mut fixed = vec![0u8; 8];
    fixed[4..8].copy_from_slice(&99u32.to_le_bytes());
    let got: Bytes8 = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert_eq!(got.0[0], 0, "native direct tag written");
    assert_eq!(&got.0[4..8], &99u32.to_le_bytes());

    // Variant B (index 1), unit payload.
    let mut fixed = vec![0u8; 8];
    fixed[0] = 1;
    let got: Bytes8 = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert_eq!(got.0[0], 1, "native direct tag written");
}

#[test]
fn canonical_tag_out_of_range_is_an_integrity_error() {
    let (wire, native) = canonical_direct_pair();
    let env = make_env(vec![], vec![], vec![]);
    let mut fixed = vec![0u8; 8];
    fixed[0] = 7; // two variants
    let err = run_value::<Bytes8>(&wire, &native, &fixed, &[], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

#[test]
fn canonical_wire_fixes_into_a_native_niche() {
    // Option<Box<u32>>: null-pointer niche. Wire: canonical tag u32 @0,
    // box slot @8; size 16, align 8.
    let wire = wenum(
        0,
        16,
        8,
        WireEnumForm::Canonical,
        vec![
            wvariant("None", 0, WireNode::Unit { offset: 8 }),
            wvariant("Some", 1, wbox_slot(8, wprim(0, ScalarKind::U32))),
        ],
    );
    let native = nenum_glue(
        0,
        8,
        8,
        NativeTagEncoding::Niche {
            offset: 0,
            size: 8,
            niche_start: 0,
        },
        OPTBOX_DROP,
        vec![
            nvariant(
                "None",
                0,
                NativeVariantTag::Niche { index: 0 },
                NativeLayoutNode::Unit { offset: 0 },
            ),
            nvariant(
                "Some",
                1,
                NativeVariantTag::Untagged,
                nbox(0, scalar(0, ScalarKind::U32), 0),
            ),
        ],
    );
    let env = make_env(vec![box_ctor::<u32>()], vec![], vec![]);

    // Some(42): tag 1 (name-sorted [None, Some]), box pointee in var.
    let mut fixed = vec![0u8; 16];
    fixed[0] = 1;
    fixed[8..16].copy_from_slice(&varref(0, 4));
    let got: Option<Box<u32>> =
        run_value(&wire, &native, &fixed, &42u32.to_le_bytes(), &env).unwrap();
    assert_eq!(got, Some(Box::new(42)));

    // None: tag 0, niche value written — never read from wire bytes.
    let fixed = vec![0u8; 16];
    let got: Option<Box<u32>> = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert_eq!(got, None);
}

/// Fully-flat wire enum: in-place u8 discriminants 7 (A) and 9 (B).
fn fully_flat_pair() -> (WireNode, NativeLayoutNode) {
    let wire = wenum(
        0,
        8,
        4,
        WireEnumForm::FullyFlat {
            tag_offset: 0,
            tag_size: 1,
        },
        vec![
            wvariant("A", 7, wprim(4, ScalarKind::U32)),
            wvariant("B", 9, WireNode::Unit { offset: 4 }),
        ],
    );
    let native = nenum(
        0,
        8,
        4,
        NativeTagEncoding::Direct { offset: 0, size: 1 },
        vec![
            nvariant(
                "A",
                0,
                NativeVariantTag::Direct { value: 7 },
                scalar(4, ScalarKind::U32),
            ),
            nvariant(
                "B",
                1,
                NativeVariantTag::Direct { value: 9 },
                NativeLayoutNode::Unit { offset: 4 },
            ),
        ],
    );
    (wire, native)
}

#[test]
fn fully_flat_wire_tag_matches_raw_bits_and_writes_the_native_tag() {
    let (wire, native) = fully_flat_pair();
    let env = make_env(vec![], vec![], vec![]);
    let mut fixed = vec![0u8; 8];
    fixed[0] = 7; // A
    fixed[4..8].copy_from_slice(&123u32.to_le_bytes());
    let got: Bytes8 = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert_eq!(got.0[0], 7);
    assert_eq!(&got.0[4..8], &123u32.to_le_bytes());

    let mut fixed = vec![0u8; 8];
    fixed[0] = 9; // B
    let got: Bytes8 = run_value(&wire, &native, &fixed, &[], &env).unwrap();
    assert_eq!(got.0[0], 9);
}

#[test]
fn unmatched_direct_wire_tag_is_an_integrity_error() {
    let (wire, native) = fully_flat_pair();
    let env = make_env(vec![], vec![], vec![]);
    let mut fixed = vec![0u8; 8];
    fixed[0] = 8; // neither 7 nor 9
    let err = run_value::<Bytes8>(&wire, &native, &fixed, &[], &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
}

// --- rollback ------------------------------------------------------------------

#[test]
fn success_pushes_no_drops_and_ownership_transfers() {
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, tracked_wire(0)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, tracked_native(0), 0))],
    );
    #[repr(C)]
    struct Dst {
        v: Vec<Tracked>,
    }
    let fixed = slot24(0, 2);
    let mut var = Vec::new();
    for x in [1u32, 2] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![vec_ctor::<Tracked>()], vec![], vec![]);
    take_events();
    let got: Dst = run_value(&wire, &native, &fixed, &var, &env).unwrap();
    assert_eq!(take_events(), Vec::<String>::new(), "no drops on success");
    // Compare ids without constructing Tracked values (their drops log).
    assert_eq!(got.v.iter().map(|t| t.0).collect::<Vec<_>>(), vec![1, 2]);
    drop(got);
    let events = take_events();
    assert_eq!(count(&events, "T:1"), 1, "caller owns the value");
    assert_eq!(count(&events, "T:2"), 1);
}

#[test]
fn failure_rolls_back_completed_constructions_in_reverse_order() {
    // a: Vec<Tracked>[1,2] @0, b: Vec<Tracked>[3] @24, s: set(dup) @48.
    let wire = wstruct(
        0,
        96,
        8,
        vec![
            wfield("a", 0, wvec_slot(0, tracked_wire(0))),
            wfield("b", 1, wvec_slot(24, tracked_wire(0))),
            wfield("s", 2, wset_slot(48, wprim(0, ScalarKind::U32))),
        ],
    );
    let native = nstruct(
        0,
        96,
        8,
        vec![
            nfield("a", 0, nvec(0, tracked_native(0), 0)),
            nfield("b", 1, nvec(24, tracked_native(0), 0)),
            nfield("s", 2, nset(48, scalar(0, ScalarKind::U32), 1)),
        ],
    );
    let mut fixed = Vec::new();
    fixed.extend_from_slice(&slot24(0, 2));
    fixed.extend_from_slice(&slot24(8, 1));
    fixed.extend_from_slice(&slot48(12, 2));
    let mut var = Vec::new();
    for x in [1u32, 2, 3, 7, 7] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![vec_ctor::<Tracked>(), set_u32_ctor()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 96]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
    assert_eq!(
        take_events(),
        vec![
            // The failing set aborts through its cursor first,
            "abort:set<u32>".to_string(),
            // then completed constructions unwind in reverse: b, then a.
            "ctor_drop:vec<Tracked>".to_string(),
            "T:3".to_string(),
            "ctor_drop:vec<Tracked>".to_string(),
            "T:1".to_string(),
            "T:2".to_string(),
        ]
    );
}

#[test]
fn completing_an_aggregate_disarms_children_and_pushes_one_whole_drop() {
    // outer { inner: Inner { v: Vec<Tracked> } @0, s: set(dup) @24 }.
    let wire = wstruct(
        0,
        72,
        8,
        vec![
            wfield(
                "inner",
                0,
                wstruct(
                    0,
                    24,
                    8,
                    vec![wfield("v", 0, wvec_slot(0, tracked_wire(0)))],
                ),
            ),
            wfield("s", 1, wset_slot(24, wprim(0, ScalarKind::U32))),
        ],
    );
    let native = nstruct(
        0,
        72,
        8,
        vec![
            nfield(
                "inner",
                0,
                nstruct_glue(
                    0,
                    24,
                    8,
                    INNER_DROP,
                    vec![nfield("v", 0, nvec(0, tracked_native(0), 0))],
                ),
            ),
            nfield("s", 1, nset(24, scalar(0, ScalarKind::U32), 1)),
        ],
    );
    let mut fixed = Vec::new();
    fixed.extend_from_slice(&slot24(0, 2));
    fixed.extend_from_slice(&slot48(8, 2));
    let mut var = Vec::new();
    for x in [1u32, 2, 7, 7] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![vec_ctor::<Tracked>(), set_u32_ctor()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 72]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
    let events = take_events();
    assert_eq!(
        events,
        vec![
            "abort:set<u32>".to_string(),
            // ONE whole-value entry for the completed Inner — its glue
            // drops the vec (and the elements) exactly once.
            "whole:Inner".to_string(),
            "T:1".to_string(),
            "T:2".to_string(),
        ]
    );
    // No double-drop: the vec's own ctor entry was disarmed when Inner completed.
    assert_eq!(count(&events, "ctor_drop:vec<Tracked>"), 0);
}

#[test]
fn completed_enum_rolls_back_through_its_whole_drop() {
    // outer { e: canonical enum { A(Vec<Tracked>), B } @0, s: set(dup) @32 }.
    let blob = std::mem::size_of::<FakeEnum>() as u32;
    assert_eq!(blob, 32);
    let wire = wstruct(
        0,
        80,
        8,
        vec![
            wfield(
                "e",
                0,
                wenum(
                    0,
                    32,
                    8,
                    WireEnumForm::Canonical,
                    vec![
                        wvariant("A", 0, wvec_slot(8, tracked_wire(0))),
                        wvariant("B", 1, WireNode::Unit { offset: 8 }),
                    ],
                ),
            ),
            wfield("s", 1, wset_slot(32, wprim(0, ScalarKind::U32))),
        ],
    );
    let native = nstruct(
        0,
        80,
        8,
        vec![
            nfield(
                "e",
                0,
                nenum_glue(
                    0,
                    32,
                    8,
                    NativeTagEncoding::Direct { offset: 0, size: 1 },
                    ENUM_DROP,
                    vec![
                        nvariant(
                            "A",
                            0,
                            NativeVariantTag::Direct { value: 0 },
                            nvec(8, tracked_native(0), 0),
                        ),
                        nvariant(
                            "B",
                            1,
                            NativeVariantTag::Direct { value: 1 },
                            NativeLayoutNode::Unit { offset: 8 },
                        ),
                    ],
                ),
            ),
            nfield("s", 1, nset(32, scalar(0, ScalarKind::U32), 1)),
        ],
    );
    let mut fixed = Vec::new();
    fixed.extend_from_slice(&[0u8; 8]); // canonical tag 0 = A
    fixed.extend_from_slice(&slot24(0, 2)); // vec payload
    fixed.extend_from_slice(&slot48(8, 2)); // set (duplicate)
    let mut var = Vec::new();
    for x in [1u32, 2, 7, 7] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![vec_ctor::<Tracked>(), set_u32_ctor()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 80]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
    let events = take_events();
    assert_eq!(
        events,
        vec![
            "abort:set<u32>".to_string(),
            "whole:Enum".to_string(),
            "T:1".to_string(),
            "T:2".to_string(),
        ]
    );
    assert_eq!(
        count(&events, "ctor_drop:vec<Tracked>"),
        0,
        "payload children disarmed"
    );
}

#[test]
fn mid_container_failure_aborts_partial_state_through_the_cursor() {
    // Vec<TrackedPair>: element 2's bool is invalid; element 1 is
    // cursor-owned partial state, dropped exactly once by abort.
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, tracked_pair_wire(0)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, tracked_pair_native(0), 0))],
    );
    let fixed = slot24(0, 2);
    let mut var = Vec::new();
    var.extend_from_slice(&1u32.to_le_bytes());
    var.extend_from_slice(&[1, 0, 0, 0]);
    var.extend_from_slice(&2u32.to_le_bytes());
    var.extend_from_slice(&[2, 0, 0, 0]); // invalid bool
    let env = make_env(vec![vec_ctor::<TrackedPair>()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Integrity { .. }), "{err:?}");
    let events = take_events();
    assert_eq!(
        events,
        vec!["abort:vec<TrackedPair>".to_string(), "TP:1".to_string()]
    );
}

#[test]
fn push_owns_the_element_even_on_failure_no_double_drop() {
    // push "panics" on Tracked(42) after consuming it: the executor must
    // NOT roll the element back — the callback already disposed of it.
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, tracked_wire(0)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, tracked_native(0), 0))],
    );
    let fixed = slot24(0, 2);
    let mut var = Vec::new();
    for x in [1u32, 42] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(vec![vec_tracked_push_panics()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Callback { .. }), "{err:?}");
    let events = take_events();
    assert_eq!(
        events,
        vec![
            "T:42".to_string(), // consumed by the failing push, once
            "abort:vec<Tracked>".to_string(),
            "T:1".to_string(), // cursor-owned partial state
        ]
    );
}

#[test]
fn begin_failure_is_a_callback_error_and_prior_fields_unwind() {
    let wire = wstruct(
        0,
        48,
        8,
        vec![
            wfield("a", 0, wvec_slot(0, tracked_wire(0))),
            wfield("b", 1, wvec_slot(24, tracked_wire(0))),
        ],
    );
    let native = nstruct(
        0,
        48,
        8,
        vec![
            nfield("a", 0, nvec(0, tracked_native(0), 0)),
            nfield("b", 1, nvec(24, tracked_native(0), 1)),
        ],
    );
    let mut fixed = Vec::new();
    fixed.extend_from_slice(&slot24(0, 1));
    fixed.extend_from_slice(&slot24(4, 1));
    let mut var = Vec::new();
    for x in [5u32, 6] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let env = make_env(
        vec![vec_ctor::<Tracked>(), vec_tracked_begin_fails()],
        vec![],
        vec![],
    );
    take_events();
    let err = run_value::<MaybeUninit<[u8; 48]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Callback { .. }), "{err:?}");
    assert_eq!(
        take_events(),
        vec!["ctor_drop:vec<Tracked>".to_string(), "T:5".to_string()]
    );
}

#[test]
fn finish_failure_aborts_the_cursor_and_reports_callback() {
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, tracked_wire(0)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, tracked_native(0), 0))],
    );
    let fixed = slot24(0, 1);
    let var = 8u32.to_le_bytes();
    let env = make_env(vec![vec_tracked_finish_fails()], vec![], vec![]);
    take_events();
    let err = run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(matches!(err, ExecError::Callback { .. }), "{err:?}");
    assert_eq!(
        take_events(),
        vec!["abort:vec<Tracked>".to_string(), "T:8".to_string()]
    );
}

// --- recursion, caps, tables -----------------------------------------------------

#[test]
fn recursive_types_execute_through_backref_plans() {
    // struct Node { val: u32 @0, next: Vec<Node> @8 }, vec pointee = backref.
    let wire = wstruct(
        0,
        32,
        8,
        vec![
            wfield("val", 0, wprim(0, ScalarKind::U32)),
            wfield(
                "next",
                1,
                wvec_slot(
                    8,
                    WireNode::BackRef {
                        distance: 0,
                        offset: 0,
                    },
                ),
            ),
        ],
    );
    let native = nstruct_glue(
        0,
        32,
        8,
        NODE_DROP,
        vec![
            nfield("val", 0, scalar(0, ScalarKind::U32)),
            nfield(
                "next",
                1,
                nvec(
                    8,
                    NativeLayoutNode::BackRef {
                        distance: 0,
                        offset: 0,
                    },
                    0,
                ),
            ),
        ],
    );
    let mut fixed = vec![0u8; 32];
    fixed[0..4].copy_from_slice(&1u32.to_le_bytes());
    fixed[8..16].copy_from_slice(&varref(0, 1)); // one child at var 0
    let mut var = vec![0u8; 32];
    var[0..4].copy_from_slice(&2u32.to_le_bytes());
    var[8..16].copy_from_slice(&varref(0, 0)); // leaf
    let env = make_env(vec![vec_ctor::<Node>()], vec![], vec![]);
    let got: Node = run_value(&wire, &native, &fixed, &var, &env).unwrap();
    assert_eq!(
        got,
        Node {
            val: 1,
            next: vec![Node {
                val: 2,
                next: vec![]
            }]
        }
    );
}

#[test]
fn recursion_depth_cap_is_enforced() {
    // Box<Box<u32>> with max_depth 2: the innermost frame is depth 3.
    let wire = wstruct(
        0,
        8,
        8,
        vec![wfield(
            "b",
            0,
            wbox_slot(0, wbox_slot(0, wprim(0, ScalarKind::U32))),
        )],
    );
    let native = nstruct(
        0,
        8,
        8,
        vec![nfield(
            "b",
            0,
            nbox(0, nbox(0, scalar(0, ScalarKind::U32), 1), 0),
        )],
    );
    let fixed = varref(0, 8);
    let mut var = Vec::new();
    var.extend_from_slice(&varref(8, 4));
    var.extend_from_slice(&11u32.to_le_bytes());
    let mut env = make_env(
        vec![box_ctor::<Box<u32>>(), box_ctor::<u32>()],
        vec![],
        vec![],
    );
    env.limits = ExecLimits {
        max_depth: 2,
        ..ExecLimits::default()
    };
    let err = run_value::<MaybeUninit<[u8; 8]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(
        matches!(err, ExecError::DepthExceeded { limit: 2 }),
        "{err:?}"
    );
}

#[test]
fn allocation_cap_is_enforced() {
    // Vec of 3: begin + 3 element temps = 4 allocations > 3.
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 0))],
    );
    let fixed = slot24(0, 3);
    let mut var = Vec::new();
    for x in [1u32, 2, 3] {
        var.extend_from_slice(&x.to_le_bytes());
    }
    let mut env = make_env(vec![vec_ctor::<u32>()], vec![], vec![]);
    env.limits = ExecLimits {
        max_allocations: 3,
        ..ExecLimits::default()
    };
    let err = run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert!(
        matches!(err, ExecError::AllocationsExceeded { limit: 3 }),
        "{err:?}"
    );
}

#[test]
fn a_plan_naming_a_missing_ctor_slot_is_reported() {
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, wprim(0, ScalarKind::U32)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![
            nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 3)), // CtorId(3)
        ],
    );
    let fixed = slot24(0, 0);
    let env = make_env(vec![vec_ctor::<u32>()], vec![], vec![]); // one entry
    let err = run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &[], &env).unwrap_err();
    assert!(
        matches!(
            err,
            ExecError::BadTableIndex {
                table: "ctor",
                index: 3
            }
        ),
        "{err:?}"
    );
}

// --- status-returning rollback callbacks (R22/H3) ---------------------------

#[test]
fn failing_whole_drop_is_reported_and_rollback_continues() {
    let wire = wstruct(
        0,
        32,
        8,
        vec![
            wfield(
                "a",
                0,
                wstruct(0, 4, 4, vec![wfield("0", 0, wprim(0, ScalarKind::U32))]),
            ),
            wfield(
                "b",
                1,
                wstruct(4, 4, 4, vec![wfield("0", 0, wprim(0, ScalarKind::U32))]),
            ),
            wfield("bad", 2, wstring_slot(8)),
        ],
    );
    let native = nstruct(
        0,
        32,
        8,
        vec![
            nfield("a", 0, tracked_native(0)),
            nfield(
                "b",
                1,
                nstruct_glue(4, 4, 4, 6, vec![nfield("0", 0, scalar(0, ScalarKind::U32))]),
            ),
            nfield("bad", 2, nstr(8)),
        ],
    );
    let mut fixed = vec![0u8; 32];
    fixed[0..4].copy_from_slice(&11u32.to_le_bytes());
    fixed[4..8].copy_from_slice(&22u32.to_le_bytes());
    fixed[8..16].copy_from_slice(&varref(0, 1));
    let env = make_env(vec![], vec![], vec![]);

    take_events();
    let err =
        run_value::<MaybeUninit<[u8; 32]>>(&wire, &native, &fixed, &[0xFF], &env).unwrap_err();
    assert_eq!(err, ExecError::Callback { what: "drop table" });
    assert_eq!(take_events(), vec!["drop:failed", "T:11"]);
}

#[test]
fn failing_ctor_abort_is_reported() {
    let wire = wstruct(
        0,
        24,
        8,
        vec![wfield("v", 0, wvec_slot(0, tracked_pair_wire(0)))],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![nfield("v", 0, nvec(0, tracked_pair_native(0), 0))],
    );
    let fixed = slot24(0, 1);
    let mut var = vec![0u8; 8];
    var[0..4].copy_from_slice(&7u32.to_le_bytes());
    var[4] = 2;
    let ctor = CtorEntry {
        abort: abort_fails,
        ..vec_ctor::<TrackedPair>()
    };
    let env = make_env(vec![ctor], vec![], vec![]);

    take_events();
    let err = run_value::<MaybeUninit<[u8; 24]>>(&wire, &native, &fixed, &var, &env).unwrap_err();
    assert_eq!(err, ExecError::Callback { what: "ctor abort" });
    assert_eq!(take_events(), vec!["abort:failed"]);
}

#[test]
fn failing_completed_ctor_drop_is_reported() {
    let wire = wstruct(
        0,
        48,
        8,
        vec![
            wfield("v", 0, wvec_slot(0, wprim(0, ScalarKind::U32))),
            wfield("bad", 1, wstring_slot(24)),
        ],
    );
    let native = nstruct(
        0,
        48,
        8,
        vec![
            nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 0)),
            nfield("bad", 1, nstr(24)),
        ],
    );
    let mut fixed = slot24(0, 0);
    fixed.resize(48, 0);
    fixed[24..32].copy_from_slice(&varref(0, 1));
    let ctor = CtorEntry {
        drop_in_place: ctor_drop_fails,
        ..vec_ctor::<u32>()
    };
    let env = make_env(vec![ctor], vec![], vec![]);

    take_events();
    let err =
        run_value::<MaybeUninit<[u8; 48]>>(&wire, &native, &fixed, &[0xFF], &env).unwrap_err();
    assert_eq!(err, ExecError::Callback { what: "ctor drop" });
    assert_eq!(take_events(), vec!["ctor_drop:failed"]);
}

#[test]
fn failing_skip_drop_is_reported() {
    let wire = wstruct(0, 24, 8, vec![wfield("bad", 0, wstring_slot(0))]);
    let native = nstruct(
        0,
        32,
        8,
        vec![
            nfield("cache", 0, nskip(0, 4, 4, 0)),
            nfield("bad", 1, nstr(8)),
        ],
    );
    let skip = SkipEntry {
        write: skip_write_tracked,
        drop_in_place: skip_drop_fails,
    };
    let env = make_env(vec![], vec![skip], vec![]);

    take_events();
    let err = run_value::<MaybeUninit<[u8; 32]>>(&wire, &native, &slot24(0, 1), &[0xFF], &env)
        .unwrap_err();
    assert_eq!(err, ExecError::Callback { what: "skip drop" });
    assert_eq!(take_events(), vec!["skip:write", "skip:drop_failed"]);
}
