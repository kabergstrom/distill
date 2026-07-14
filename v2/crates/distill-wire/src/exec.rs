//! The transactional fixup executor (§12): runs a compiled plan's
//! ordered ops over a `MaybeUninit` destination — flat copies for the
//! coinciding runs, typed construction through the consuming binary's
//! generated ctor/skip tables for every pointer-shaped slot — validating
//! everything before the destination is treated as initialized: `VarRef`
//! bounds and alignment with checked arithmetic, UTF-8, discriminants,
//! scalar bit patterns, allocation-count and recursion-depth caps.
//!
//! Failure mid-fixup unwinds a **framed constructed-value stack**:
//! beginning an aggregate opens a frame; every completed construction
//! pushes (pointer, drop entry); completing an aggregate whose plan
//! carries a `whole_drop` disarms its frame's entries and pushes exactly
//! one entry for the whole value, so ownership transfers into the
//! enclosing value exactly once. A plan with `whole_drop: None` leaves
//! its entries armed — for aggregates that is vacuous (no drop glue ⇒ no
//! entries), and for single-construct frames (a `String` element, a
//! container pointee) the construction's own entry *is* the whole-value
//! entry. Failure pops and drops in reverse; a partially filled
//! container is aborted through its cursor as the partial state it is.
//! `ConstructString`/`ConstructBlob` values roll back through the
//! executor's built-in drop for the value it itself constructed
//! (normative, not table-supplied).
//!
//! Executor-pinned wire conventions this module is normative for:
//! - Sequence elements (`Vec`/`Set`) sit in the variable section at
//!   stride `align_up(element wire size, element wire align)`.
//! - Map entries are flattened key-then-value: value at
//!   `align_up(key wire size, value wire align)`, pair stride rounded to
//!   `max(key align, value align)`.
//! - `Box`/`Arc` `VarRef.len` is the pointee's flattened byte size and
//!   must equal it exactly.

use crate::native::{CtorCursor, CtorEntry, DropId, PushError, ScalarKind, SkipDefaultId};
use crate::plan::{CompiledPlans, FixupOp, NativeTagWrite, PlanId, PlanMeta, WireTagRead};
use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::sync::Arc;

/// The `#[asset(blob)]` runtime type (§4): an `Arc`-backed borrowed byte
/// range — a pack load borrows the mmap, a fetched artifact borrows the
/// buffer; never a copy. `ConstructBlob` clones one of the environment's
/// resolved per-blob-table-entry values into the destination slot.
pub struct Blob {
    backing: Arc<dyn AsRef<[u8]> + Send + Sync>,
    offset: usize,
    len: usize,
}

impl Blob {
    /// A byte range of `backing`. Panics if the range is out of bounds —
    /// resolving a blob-table entry against its backing is the caller's
    /// checked step, not a latent error.
    pub fn new(backing: Arc<dyn AsRef<[u8]> + Send + Sync>, offset: usize, len: usize) -> Blob {
        let total = (*backing).as_ref().len();
        assert!(
            offset.checked_add(len).is_some_and(|end| end <= total),
            "blob range {offset}+{len} exceeds backing of {total} bytes"
        );
        Blob {
            backing,
            offset,
            len,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &(*self.backing).as_ref()[self.offset..self.offset + self.len]
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Clone for Blob {
    fn clone(&self) -> Blob {
        Blob {
            backing: Arc::clone(&self.backing),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl std::fmt::Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blob")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

/// Validation caps (§12: allocation-count and recursion-depth caps are
/// part of validation before and during construction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecLimits {
    /// Maximum plan-frame nesting depth (root = 1).
    pub max_depth: u32,
    /// Maximum count of executor-driven allocations: container begins,
    /// element temps, strings, blobs.
    pub max_allocations: u64,
}

impl Default for ExecLimits {
    fn default() -> ExecLimits {
        ExecLimits {
            max_depth: 128,
            max_allocations: 1 << 32,
        }
    }
}

/// The consuming binary's side of execution: the generated tables the
/// plan's ids index, the resolved blob-table backings, and the caps.
pub struct ExecEnv<'a> {
    pub ctors: &'a crate::native::CtorTable,
    pub drops: &'a crate::native::DropTable,
    pub skips: &'a crate::native::SkipWriterTable,
    /// One resolved `Blob` per artifact blob-table entry, in table order.
    pub blobs: &'a [Blob],
    pub limits: ExecLimits,
}

/// Execution failures. On error the destination is uninitialized again:
/// everything constructed has been rolled back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    /// Malformed artifact data — bad discriminants, bit patterns, UTF-8,
    /// `VarRef` bounds or alignment, duplicate keys/elements, blob refs.
    Integrity {
        detail: String,
    },
    /// A generated-table callback reported failure (a caught panic).
    Callback {
        what: &'static str,
    },
    /// A plan named a table slot the environment does not have.
    BadTableIndex {
        table: &'static str,
        index: u32,
    },
    DepthExceeded {
        limit: u32,
    },
    AllocationsExceeded {
        limit: u64,
    },
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::Integrity { detail } => write!(f, "artifact integrity error: {detail}"),
            ExecError::Callback { what } => write!(f, "{what} callback failed (caught panic)"),
            ExecError::BadTableIndex { table, index } => {
                write!(
                    f,
                    "plan names {table} table slot {index}, which does not exist"
                )
            }
            ExecError::DepthExceeded { limit } => {
                write!(f, "fixup recursion exceeds the depth cap {limit}")
            }
            ExecError::AllocationsExceeded { limit } => {
                write!(f, "fixup exceeds the allocation cap {limit}")
            }
        }
    }
}

impl std::error::Error for ExecError {}

/// Execute `plans.arena.plans[root]` over the artifact's fixed and
/// variable sections into `dst`.
///
/// On `Ok`, `dst` holds a fully initialized value the caller owns. On
/// `Err`, everything constructed has been dropped in reverse order and
/// `dst` is uninitialized again.
///
/// # Safety
/// - `dst` must be valid for writes of `metas[root].native_size` bytes
///   and aligned to `metas[root].native_align`.
/// - The plans must have been compiled against the native layout tree
///   that describes `dst`'s type, and the environment's tables must be
///   the ones the plan's `CtorId`/`DropId`/`SkipDefaultId` values were
///   assigned against (the §5 fixup-table identity pairs them).
pub unsafe fn execute_fixup(
    plans: &CompiledPlans,
    root: PlanId,
    fixed: &[u8],
    var: &[u8],
    env: &ExecEnv<'_>,
    dst: *mut u8,
) -> Result<(), ExecError> {
    let meta = plans.metas[root.0 as usize];
    if fixed.len() != meta.wire_size as usize {
        return Err(ExecError::Integrity {
            detail: format!(
                "fixed section is {} bytes, the wire layout says {}",
                fixed.len(),
                meta.wire_size
            ),
        });
    }
    let mut ex = Executor {
        plans,
        env,
        var,
        stack: Vec::new(),
        allocations: 0,
    };
    let r = ex.run_plan(root, fixed, dst, 1);
    debug_assert!(r.is_ok() || ex.stack.is_empty(), "failure fully unwinds");
    // On success the stack's entries (the root's whole-value entry, or
    // its armed constructions) are discarded: ownership transfers out.
    r
}

fn integrity(detail: impl Into<String>) -> ExecError {
    ExecError::Integrity {
        detail: detail.into(),
    }
}

fn checked_native_range(
    offset: u64,
    bytes: u64,
    native_size: u32,
    operation: &str,
) -> Result<usize, ExecError> {
    let end = offset
        .checked_add(bytes)
        .ok_or_else(|| integrity(format!("{operation} native range overflows")))?;
    if end > u64::from(native_size) {
        return Err(integrity(format!(
            "{operation} native range {offset}..{end} exceeds the {native_size}-byte native frame"
        )));
    }
    usize::try_from(offset)
        .map_err(|_| integrity(format!("{operation} native offset does not fit usize")))
}

fn align_up(v: u32, align: u32) -> u32 {
    let a = align.max(1);
    let rem = v % a;
    if rem == 0 {
        v
    } else {
        v + (a - rem)
    }
}

/// An armed rollback entry: a constructed value and how to drop it.
struct Entry {
    ptr: *mut u8,
    drop: EntryDrop,
}

enum EntryDrop {
    /// Whole-value glue from the generated drop table.
    Table(DropId),
    /// A completed container, through its ctor entry's `drop_in_place`.
    Ctor(crate::native::CtorId),
    /// A written skip default, through its paired drop.
    Skip(SkipDefaultId),
    /// Executor-built `String` (built-in, normative).
    String,
    /// Executor-cloned `Blob` (built-in, normative).
    Blob,
}

/// An aligned element temp; frees the *memory* on drop (never contents —
/// ownership of the contents moves through push or the rollback stack).
struct Temp {
    ptr: *mut u8,
    layout: Option<Layout>,
}

impl Temp {
    fn new(size: u32, align: u32) -> Result<Temp, ExecError> {
        if size == 0 {
            // ZST slot: an aligned dangling pointer, nothing to free.
            return Ok(Temp {
                ptr: align.max(1) as usize as *mut u8,
                layout: None,
            });
        }
        let layout = Layout::from_size_align(size as usize, align.max(1) as usize)
            .map_err(|_| integrity(format!("ctor entry temp layout {size}/{align} is invalid")))?;
        // Safety: layout has nonzero size.
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Ok(Temp {
            ptr,
            layout: Some(layout),
        })
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        if let Some(layout) = self.layout {
            // Safety: allocated in Temp::new with this exact layout.
            unsafe { dealloc(self.ptr, layout) };
        }
    }
}

struct Executor<'a> {
    plans: &'a CompiledPlans,
    env: &'a ExecEnv<'a>,
    var: &'a [u8],
    stack: Vec<Entry>,
    allocations: u64,
}

impl<'a> Executor<'a> {
    fn meta(&self, plan: PlanId) -> PlanMeta {
        self.plans.metas[plan.0 as usize]
    }

    fn charge(&mut self) -> Result<(), ExecError> {
        self.allocations += 1;
        if self.allocations > self.env.limits.max_allocations {
            Err(ExecError::AllocationsExceeded {
                limit: self.env.limits.max_allocations,
            })
        } else {
            Ok(())
        }
    }

    fn ctor(&self, id: crate::native::CtorId) -> Result<&'static CtorEntry, ExecError> {
        self.env
            .ctors
            .entries
            .get(id.0 as usize)
            .ok_or(ExecError::BadTableIndex {
                table: "ctor",
                index: id.0,
            })
    }

    /// Bounds- and alignment-checked variable-section range, all
    /// arithmetic checked (§12: reject, never wrap).
    fn var_range(&self, offset: u32, bytes: u64, align: u32) -> Result<&'a [u8], ExecError> {
        if align > 1 && !offset.is_multiple_of(align) {
            return Err(integrity(format!(
                "VarRef offset {offset} is not {align}-aligned"
            )));
        }
        let end = offset as u64 + bytes;
        if end > self.var.len() as u64 {
            return Err(integrity(format!(
                "VarRef range {offset}+{bytes} exceeds the {}-byte variable section",
                self.var.len()
            )));
        }
        Ok(&self.var[offset as usize..end as usize])
    }

    /// Disarm without dropping: ownership moved elsewhere (into a
    /// completed aggregate's single entry, or into a cursor).
    fn disarm_to(&mut self, mark: usize) {
        self.stack.truncate(mark);
    }

    /// Pop and drop in reverse down to `mark`. A contained drop panic is
    /// remembered, but rollback continues so every independently armed
    /// value still gets its disposal attempt. The first callback failure
    /// is returned after the stack is empty; callers must poison the
    /// owning module epoch before allowing more work through its tables.
    unsafe fn unwind_to(&mut self, mark: usize) -> Result<(), ExecError> {
        let mut first_failure = None;
        while self.stack.len() > mark {
            let entry = self.stack.pop().expect("len > mark");
            let callback = match entry.drop {
                EntryDrop::Table(id) => (self.env.drops.entries[id.0 as usize])(entry.ptr)
                    .map_err(|_| ExecError::Callback { what: "drop table" }),
                EntryDrop::Ctor(id) => {
                    (self.env.ctors.entries[id.0 as usize].drop_in_place)(entry.ptr)
                        .map_err(|_| ExecError::Callback { what: "ctor drop" })
                }
                EntryDrop::Skip(id) => {
                    (self.env.skips.entries[id.0 as usize].drop_in_place)(entry.ptr)
                        .map_err(|_| ExecError::Callback { what: "skip drop" })
                }
                EntryDrop::String => {
                    std::ptr::drop_in_place(entry.ptr as *mut String);
                    Ok(())
                }
                EntryDrop::Blob => {
                    std::ptr::drop_in_place(entry.ptr as *mut Blob);
                    Ok(())
                }
            };
            if first_failure.is_none() {
                first_failure = callback.err();
            }
        }
        first_failure.map_or(Ok(()), Err)
    }

    /// Run one plan frame: `wire` is the frame's wire image, `dst` its
    /// native origin. On error, everything this frame armed has been
    /// unwound; on success, a `whole_drop` plan has disarmed its frame
    /// and armed the single whole-value entry.
    unsafe fn run_plan(
        &mut self,
        id: PlanId,
        wire: &[u8],
        dst: *mut u8,
        depth: u32,
    ) -> Result<(), ExecError> {
        let native_size = self.meta(id).native_size;
        self.run_plan_in_native_frame(id, wire, dst, depth, native_size)
    }

    /// Variant sub-plans are enum-relative and therefore execute within the
    /// enclosing enum's native frame rather than their payload node's size.
    unsafe fn run_plan_in_native_frame(
        &mut self,
        id: PlanId,
        wire: &[u8],
        dst: *mut u8,
        depth: u32,
        native_size: u32,
    ) -> Result<(), ExecError> {
        if depth > self.env.limits.max_depth {
            return Err(ExecError::DepthExceeded {
                limit: self.env.limits.max_depth,
            });
        }
        let mark = self.stack.len();
        let plan = &self.plans.arena.plans[id.0 as usize];
        match self.run_ops(&plan.ops, wire, dst, depth, native_size) {
            Ok(()) => {
                if let Some(drop_id) = plan.whole_drop {
                    // Validate before disarming — after the swap there
                    // is no precise rollback for a bad id.
                    if self.env.drops.entries.len() <= drop_id.0 as usize {
                        let bad_index = ExecError::BadTableIndex {
                            table: "drop",
                            index: drop_id.0,
                        };
                        return match self.unwind_to(mark) {
                            Ok(()) => Err(bad_index),
                            Err(drop_failure) => Err(drop_failure),
                        };
                    }
                    self.disarm_to(mark);
                    self.stack.push(Entry {
                        ptr: dst,
                        drop: EntryDrop::Table(drop_id),
                    });
                }
                Ok(())
            }
            Err(e) => match self.unwind_to(mark) {
                Ok(()) => Err(e),
                Err(drop_failure) => Err(drop_failure),
            },
        }
    }

    unsafe fn run_ops(
        &mut self,
        ops: &[FixupOp],
        wire: &[u8],
        dst: *mut u8,
        depth: u32,
        native_size: u32,
    ) -> Result<(), ExecError> {
        for op in ops {
            match op {
                FixupOp::FlatCopy {
                    wire: range,
                    native,
                } => {
                    let (start, end) = (range.start as usize, range.end as usize);
                    if start > end || end > wire.len() {
                        return Err(integrity(format!(
                            "flat-copy range {start}..{end} exceeds the {}-byte frame",
                            wire.len()
                        )));
                    }
                    let native = checked_native_range(
                        u64::from(*native),
                        (end - start) as u64,
                        native_size,
                        "flat copy",
                    )?;
                    std::ptr::copy_nonoverlapping(
                        wire.as_ptr().add(start),
                        dst.add(native),
                        end - start,
                    );
                }
                FixupOp::ValidateScalar { wire: offset, kind } => {
                    self.validate_scalar(wire, *offset, *kind)?;
                }
                FixupOp::ConstructString { wire_slot, native } => {
                    let native_offset = checked_native_range(
                        u64::from(*native),
                        std::mem::size_of::<String>() as u64,
                        native_size,
                        "string construction",
                    )?;
                    let (off, len) = read_varref(wire, *wire_slot)?;
                    let bytes = self.var_range(off, len as u64, 1)?;
                    let s = std::str::from_utf8(bytes)
                        .map_err(|_| integrity("string payload is not UTF-8"))?;
                    self.charge()?;
                    std::ptr::write(dst.add(native_offset) as *mut String, s.to_owned());
                    self.stack.push(Entry {
                        ptr: dst.add(native_offset),
                        drop: EntryDrop::String,
                    });
                }
                FixupOp::ConstructBlob { wire_slot, native } => {
                    let native_offset = checked_native_range(
                        u64::from(*native),
                        std::mem::size_of::<Blob>() as u64,
                        native_size,
                        "blob construction",
                    )?;
                    let (index, zero) = read_varref(wire, *wire_slot)?;
                    if zero != 0 {
                        return Err(integrity(format!(
                            "BlobRef pad word is {zero}, must be zero"
                        )));
                    }
                    let blob = self.env.blobs.get(index as usize).ok_or_else(|| {
                        integrity(format!(
                            "BlobRef index {index} exceeds the {}-entry blob table",
                            self.env.blobs.len()
                        ))
                    })?;
                    self.charge()?;
                    std::ptr::write(dst.add(native_offset) as *mut Blob, blob.clone());
                    self.stack.push(Entry {
                        ptr: dst.add(native_offset),
                        drop: EntryDrop::Blob,
                    });
                }
                FixupOp::ConstructVec {
                    wire_slot,
                    native,
                    elem,
                    ctor,
                } => {
                    self.construct_sequence(wire, dst, *wire_slot, *native, *elem, *ctor, depth)?;
                }
                FixupOp::ConstructSet {
                    wire_slot,
                    native,
                    elem,
                    ctor,
                } => {
                    self.construct_sequence(wire, dst, *wire_slot, *native, *elem, *ctor, depth)?;
                }
                FixupOp::ConstructMap {
                    wire_slot,
                    native,
                    key,
                    value,
                    ctor,
                } => {
                    self.construct_map(wire, dst, *wire_slot, *native, *key, *value, *ctor, depth)?;
                }
                FixupOp::ConstructBox {
                    wire_slot,
                    native,
                    inner,
                    ctor,
                } => {
                    self.construct_indirect(wire, dst, *wire_slot, *native, *inner, *ctor, depth)?;
                }
                FixupOp::ConstructArc {
                    wire_slot,
                    native,
                    inner,
                    ctor,
                } => {
                    self.construct_indirect(wire, dst, *wire_slot, *native, *inner, *ctor, depth)?;
                }
                FixupOp::WriteSkipDefault { native, writer } => {
                    let entry = self.env.skips.entries.get(writer.0 as usize).ok_or(
                        ExecError::BadTableIndex {
                            table: "skip",
                            index: writer.0,
                        },
                    )?;
                    (entry.write)(dst.add(*native as usize)).map_err(|_| ExecError::Callback {
                        what: "skip writer",
                    })?;
                    self.stack.push(Entry {
                        ptr: dst.add(*native as usize),
                        drop: EntryDrop::Skip(*writer),
                    });
                }
                FixupOp::SwitchVariant {
                    wire_tag,
                    native,
                    variants,
                } => {
                    self.switch_variant(
                        wire,
                        dst,
                        wire_tag,
                        *native,
                        variants,
                        depth,
                        native_size,
                    )?;
                }
                FixupOp::Recurse {
                    wire: wire_off,
                    native,
                    plan,
                } => {
                    let m = self.meta(*plan);
                    let start = *wire_off as usize;
                    let end = start + m.wire_size as usize;
                    if end > wire.len() {
                        return Err(integrity(format!(
                            "recurse frame {start}..{end} exceeds the {}-byte frame",
                            wire.len()
                        )));
                    }
                    let native = checked_native_range(
                        u64::from(*native),
                        u64::from(m.native_size),
                        native_size,
                        "recurse",
                    )?;
                    self.run_plan(*plan, &wire[start..end], dst.add(native), depth + 1)?;
                }
            }
        }
        Ok(())
    }

    fn validate_scalar(&self, wire: &[u8], offset: u32, kind: ScalarKind) -> Result<(), ExecError> {
        match kind {
            ScalarKind::Bool => {
                let b = *wire
                    .get(offset as usize)
                    .ok_or_else(|| integrity(format!("bool at {offset} exceeds the frame")))?;
                if b > 1 {
                    return Err(integrity(format!("invalid bool bit pattern 0x{b:02X}")));
                }
            }
            ScalarKind::Char => {
                let end = offset as usize + 4;
                if end > wire.len() {
                    return Err(integrity(format!("char at {offset} exceeds the frame")));
                }
                let v = u32::from_le_bytes(wire[offset as usize..end].try_into().expect("4 bytes"));
                if char::from_u32(v).is_none() {
                    return Err(integrity(format!("invalid char bit pattern 0x{v:08X}")));
                }
            }
            other => {
                debug_assert!(false, "ValidateScalar on unrestricted kind {other:?}");
            }
        }
        Ok(())
    }

    /// Vec/Set: elements at `align_up(wire size, wire align)` stride,
    /// built by real insertion through the ctor entry.
    #[allow(clippy::too_many_arguments)]
    unsafe fn construct_sequence(
        &mut self,
        wire: &[u8],
        dst: *mut u8,
        wire_slot: u32,
        native: u32,
        elem: PlanId,
        ctor: crate::native::CtorId,
        depth: u32,
    ) -> Result<(), ExecError> {
        let entry = self.ctor(ctor)?;
        let (off, len) = read_varref(wire, wire_slot)?;
        let m = self.meta(elem);
        let stride = m.wire_stride();
        let region = self.var_range(off, stride as u64 * len as u64, m.wire_align)?;
        self.charge()?;
        let mut cursor = (entry.begin)(dst.add(native as usize), len)
            .map_err(|_| ExecError::Callback { what: "ctor begin" })?;
        let result = self.fill_sequence(&mut cursor, entry, region, stride, len, elem, depth);
        self.seal(cursor, entry, dst.add(native as usize), ctor, result)
    }

    #[allow(clippy::too_many_arguments)]
    unsafe fn fill_sequence(
        &mut self,
        cursor: &mut CtorCursor,
        entry: &CtorEntry,
        region: &[u8],
        stride: u32,
        len: u32,
        elem: PlanId,
        depth: u32,
    ) -> Result<(), ExecError> {
        let m = self.meta(elem);
        for i in 0..len {
            let start = i as usize * stride as usize;
            let ew = &region[start..start + m.wire_size as usize];
            self.charge()?;
            let temp = Temp::new(entry.elem_size, entry.elem_align)?;
            let mark = self.stack.len();
            self.run_plan(elem, ew, temp.ptr, depth + 1)?;
            let pushed = (entry.push)(cursor, temp.ptr);
            // The cursor owns the element now, Ok or Err — never roll
            // the element back after push.
            self.disarm_to(mark);
            match pushed {
                Ok(()) => {}
                Err(PushError::Duplicate) => {
                    return Err(integrity("duplicate element in a set or map"));
                }
                Err(PushError::Panic(_)) => {
                    return Err(ExecError::Callback { what: "ctor push" });
                }
            }
        }
        Ok(())
    }

    /// Map: entries flattened as key/value pairs in the variable section;
    /// the (K, V) temp uses the ctor entry's own pair layout.
    #[allow(clippy::too_many_arguments)]
    unsafe fn construct_map(
        &mut self,
        wire: &[u8],
        dst: *mut u8,
        wire_slot: u32,
        native: u32,
        key: PlanId,
        value: PlanId,
        ctor: crate::native::CtorId,
        depth: u32,
    ) -> Result<(), ExecError> {
        let entry = self.ctor(ctor)?;
        let (off, len) = read_varref(wire, wire_slot)?;
        let km = self.meta(key);
        let vm = self.meta(value);
        let value_off = align_up(km.wire_size, vm.wire_align);
        let pair_align = km.wire_align.max(vm.wire_align);
        let stride = align_up(value_off + vm.wire_size, pair_align);
        let region = self.var_range(off, stride as u64 * len as u64, pair_align)?;
        self.charge()?;
        let mut cursor = (entry.begin)(dst.add(native as usize), len)
            .map_err(|_| ExecError::Callback { what: "ctor begin" })?;
        let result = self.fill_map(
            &mut cursor,
            entry,
            region,
            stride,
            value_off,
            len,
            key,
            value,
            depth,
        );
        self.seal(cursor, entry, dst.add(native as usize), ctor, result)
    }

    #[allow(clippy::too_many_arguments)]
    unsafe fn fill_map(
        &mut self,
        cursor: &mut CtorCursor,
        entry: &CtorEntry,
        region: &[u8],
        stride: u32,
        value_off: u32,
        len: u32,
        key: PlanId,
        value: PlanId,
        depth: u32,
    ) -> Result<(), ExecError> {
        let km = self.meta(key);
        let vm = self.meta(value);
        for i in 0..len {
            let start = i as usize * stride as usize;
            let kw = &region[start..start + km.wire_size as usize];
            let vw = &region
                [start + value_off as usize..start + value_off as usize + vm.wire_size as usize];
            self.charge()?;
            let temp = Temp::new(entry.elem_size, entry.elem_align)?;
            let mark = self.stack.len();
            self.run_plan(key, kw, temp.ptr.add(entry.key_offset as usize), depth + 1)?;
            match self.run_plan(
                value,
                vw,
                temp.ptr.add(entry.value_offset as usize),
                depth + 1,
            ) {
                Ok(()) => {}
                Err(e) => {
                    // The completed key is this frame's armed state.
                    return match self.unwind_to(mark) {
                        Ok(()) => Err(e),
                        Err(drop_failure) => Err(drop_failure),
                    };
                }
            }
            let pushed = (entry.push)(cursor, temp.ptr);
            self.disarm_to(mark);
            match pushed {
                Ok(()) => {}
                Err(PushError::Duplicate) => {
                    return Err(integrity("duplicate key in a map"));
                }
                Err(PushError::Panic(_)) => {
                    return Err(ExecError::Callback { what: "ctor push" });
                }
            }
        }
        Ok(())
    }

    /// Box/Arc: `VarRef.len` is the pointee's flattened byte size,
    /// validated against the layout; `begin` uses len = 1.
    #[allow(clippy::too_many_arguments)]
    unsafe fn construct_indirect(
        &mut self,
        wire: &[u8],
        dst: *mut u8,
        wire_slot: u32,
        native: u32,
        inner: PlanId,
        ctor: crate::native::CtorId,
        depth: u32,
    ) -> Result<(), ExecError> {
        let entry = self.ctor(ctor)?;
        let (off, len) = read_varref(wire, wire_slot)?;
        let m = self.meta(inner);
        if len != m.wire_size {
            return Err(integrity(format!(
                "Box/Arc VarRef len {len} != flattened pointee size {}",
                m.wire_size
            )));
        }
        let region = self.var_range(off, m.wire_size as u64, m.wire_align)?;
        self.charge()?;
        let mut cursor = (entry.begin)(dst.add(native as usize), 1)
            .map_err(|_| ExecError::Callback { what: "ctor begin" })?;
        let result = (|| {
            self.charge()?;
            let temp = Temp::new(entry.elem_size, entry.elem_align)?;
            let mark = self.stack.len();
            self.run_plan(inner, region, temp.ptr, depth + 1)?;
            let pushed = (entry.push)(&mut cursor, temp.ptr);
            self.disarm_to(mark);
            pushed.map_err(|e| match e {
                PushError::Duplicate => integrity("duplicate in a single-value ctor"),
                PushError::Panic(_) => ExecError::Callback { what: "ctor push" },
            })
        })();
        self.seal(cursor, entry, dst.add(native as usize), ctor, result)
    }

    /// Finish-or-abort: on a filled cursor, `finish` into the slot and
    /// arm the completed container's rollback entry; any failure aborts
    /// the cursor — the partial state it owns is disposed of as the
    /// partial state it is.
    unsafe fn seal(
        &mut self,
        mut cursor: CtorCursor,
        entry: &CtorEntry,
        slot: *mut u8,
        ctor: crate::native::CtorId,
        filled: Result<(), ExecError>,
    ) -> Result<(), ExecError> {
        match filled {
            Ok(()) => match (entry.finish)(&mut cursor, slot) {
                Ok(()) => {
                    self.stack.push(Entry {
                        ptr: slot,
                        drop: EntryDrop::Ctor(ctor),
                    });
                    Ok(())
                }
                Err(_) => match (entry.abort)(cursor) {
                    Ok(()) => Err(ExecError::Callback {
                        what: "ctor finish",
                    }),
                    Err(_) => Err(ExecError::Callback { what: "ctor abort" }),
                },
            },
            Err(e) => match (entry.abort)(cursor) {
                Ok(()) => Err(e),
                Err(_) => Err(ExecError::Callback { what: "ctor abort" }),
            },
        }
    }

    /// Read the wire tag, select that variant's sub-plan (enum-relative
    /// on both sides), then write the native discriminant with its
    /// proper encoding — niches are written, never read.
    unsafe fn switch_variant(
        &mut self,
        wire: &[u8],
        dst: *mut u8,
        wire_tag: &WireTagRead,
        native: u32,
        variants: &[(NativeTagWrite, PlanId)],
        depth: u32,
        native_size: u32,
    ) -> Result<(), ExecError> {
        let index = match wire_tag {
            WireTagRead::CanonicalU32 { offset } => {
                let end = *offset as usize + 4;
                if end > wire.len() {
                    return Err(integrity("canonical tag exceeds the frame"));
                }
                let v =
                    u32::from_le_bytes(wire[*offset as usize..end].try_into().expect("4 bytes"));
                if v as usize >= variants.len() {
                    return Err(integrity(format!(
                        "canonical tag {v} exceeds the {}-variant enum",
                        variants.len()
                    )));
                }
                v as usize
            }
            WireTagRead::Direct {
                offset,
                size,
                values,
            } => {
                let end = *offset as usize + *size as usize;
                if *size == 0 || *size > 16 || end > wire.len() {
                    return Err(integrity("direct tag exceeds the frame"));
                }
                let mut raw = [0u8; 16];
                raw[..*size as usize].copy_from_slice(&wire[*offset as usize..end]);
                let raw = u128::from_le_bytes(raw);
                values.iter().position(|v| *v == raw).ok_or_else(|| {
                    integrity(format!("wire discriminant 0x{raw:X} matches no variant"))
                })?
            }
        };
        let (write, plan) = &variants[index];
        // Variant plans are enum-relative on both sides and carry no
        // whole_drop of their own — the enum frame owns the value.
        self.run_plan_in_native_frame(*plan, wire, dst, depth + 1, native_size)?;
        match write {
            NativeTagWrite::Direct {
                offset,
                size,
                value,
            }
            | NativeTagWrite::Niche {
                offset,
                size,
                value,
            } => {
                let bytes = value.to_le_bytes();
                let size = (*size).min(16) as u64;
                let native_offset = checked_native_range(
                    u64::from(native) + u64::from(*offset),
                    size,
                    native_size,
                    "enum tag write",
                )?;
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    dst.add(native_offset),
                    size as usize,
                );
            }
            NativeTagWrite::PayloadImplied | NativeTagWrite::None => {}
        }
        Ok(())
    }
}

/// An 8-byte `VarRef { offset, len }` (or `BlobRef { index, zero }`) at
/// `slot` in the frame's wire image.
fn read_varref(wire: &[u8], slot: u32) -> Result<(u32, u32), ExecError> {
    let start = slot as usize;
    let end = start + 8;
    if end > wire.len() {
        return Err(integrity(format!(
            "VarRef slot at {slot} exceeds the {}-byte frame",
            wire.len()
        )));
    }
    let offset = u32::from_le_bytes(wire[start..start + 4].try_into().expect("4 bytes"));
    let len = u32::from_le_bytes(wire[start + 4..end].try_into().expect("4 bytes"));
    Ok((offset, len))
}
