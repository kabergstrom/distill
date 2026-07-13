//! The measured native layout tree and the generated-table vocabulary
//! (§12) — declared exactly as the design pins them. `#[asset]` generates
//! these as statics in every consuming binary; this crate declares the
//! shapes, hashes the tree (DSNL), and compiles/executes fixup plans
//! against it.

/// Every primitive leaf the native tree can hold — the tree must describe
/// the whole value, not just the validated corners. `Bool` and `Char` are
/// the restricted-bit-pattern kinds (`ValidateScalar` applies to exactly
/// those); the rest are unrestricted and flat-copy freely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarKind {
    Bool,
    Char,
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    F32,
    F64,
}

impl ScalarKind {
    /// The DSNL/DSWL grammar id — declaration order, pinned.
    pub fn grammar_id(self) -> u8 {
        match self {
            ScalarKind::Bool => 0x00,
            ScalarKind::Char => 0x01,
            ScalarKind::U8 => 0x02,
            ScalarKind::U16 => 0x03,
            ScalarKind::U32 => 0x04,
            ScalarKind::U64 => 0x05,
            ScalarKind::U128 => 0x06,
            ScalarKind::I8 => 0x07,
            ScalarKind::I16 => 0x08,
            ScalarKind::I32 => 0x09,
            ScalarKind::I64 => 0x0A,
            ScalarKind::I128 => 0x0B,
            ScalarKind::F32 => 0x0C,
            ScalarKind::F64 => 0x0D,
        }
    }
}

/// Whole-value drop glue for plan-constructed aggregates (§12): one
/// no-unwind entry per aggregate node in the consuming binary, keyed by
/// the same fixup-table identity as the plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DropId(pub u32);

/// The generated whole-value drop table, indexed by `DropId`.
pub type DropThunk = unsafe fn(ptr: *mut u8) -> Result<(), CallbackPanic>;

pub struct DropTable {
    pub entries: &'static [DropThunk],
}

/// Index into the consuming binary's #[asset]-generated constructor
/// table (§12): per monomorphized container instantiation, the typed
/// alloc/insert/finish/drop entry points fixup needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CtorId(pub u32);

/// The generated constructor table, indexed by `CtorId`. §3's thunk rule
/// applies to every fn here: a panic is caught and returned (or, for
/// drop, leaked and reported), never unwound.
pub struct CtorTable {
    pub entries: &'static [CtorEntry],
}

/// A caught callback panic (§3's thunk rule): returned, never unwound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallbackPanic;

/// `push`'s disposition: `Duplicate` is a *data* verdict (malformed
/// artifact — a set/map element already present), `Panic` a *callback*
/// failure. The executor maps `Duplicate` to the §12 integrity error
/// naming the container; both consume the element either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    Duplicate,
    Panic(CallbackPanic),
}

/// Opaque partial-container state owned by a `CtorEntry`'s callbacks:
/// `begin` creates it, `push` grows it, `finish` spends it, `abort`
/// disposes of it. The pointee shape is the generated entry's private
/// business.
#[derive(Debug)]
pub struct CtorCursor {
    pub state: *mut (),
}

/// One generated constructor-table entry (§12).
pub struct CtorEntry {
    /// Begin a container of len elements/entries; dst is the MaybeUninit
    /// native slot, untouched on Err. Scalars/Box/Arc use len = 1.
    pub begin: unsafe fn(dst: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic>,
    /// CONSUMES the element slot on entry, Ok or Err: a panicking user
    /// Hash/Eq mid-insertion may already have dropped or captured the
    /// moved value, which no thunk can restore — so the caller never
    /// rolls elem back after push; the cursor owns all partial state.
    /// For maps, `elem` points to a (K, V) temp the executor constructed
    /// at `key_offset`/`value_offset`; both move on entry.
    pub push: unsafe fn(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError>,
    /// Element temp layout the executor must provide to `push`: the
    /// element type for vec/set (key_offset = 0, value_offset unused),
    /// the monomorphized (K, V) pair for maps — this binary's layout of
    /// that pair, which no plan could otherwise know.
    pub elem_size: u32,
    pub elem_align: u32,
    pub key_offset: u32,
    pub value_offset: u32,
    /// Complete the container into dst. On Ok the cursor is spent (abort
    /// becomes a no-op); on Err dst is untouched and the cursor still
    /// owns the partial value — abort it. &mut, not by value: a caller
    /// cannot be asked to abort a cursor it no longer has.
    pub finish: unsafe fn(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic>,
    /// No-unwind disposal of a partial container (begin without finish,
    /// or failed finish); panicking element Drops leak-and-report (§3).
    pub abort: unsafe fn(cur: CtorCursor) -> Result<(), CallbackPanic>,
    /// Rollback drop for a completed value at ptr — never unwinds: a
    /// panicking Drop leaks the value and reports (§3).
    pub drop_in_place: unsafe fn(ptr: *mut u8) -> Result<(), CallbackPanic>,
}

/// The skip-writer table's entry (`SkipDefaultId`): write is paired with
/// its drop so a skip default constructed before a later failure can be
/// rolled back — the skipped field's type is schema-invisible, so no
/// other table could supply it. Both no-unwind (§3); Err leaves the
/// slot untouched.
pub struct SkipEntry {
    pub write: unsafe fn(dst: *mut u8) -> Result<(), CallbackPanic>,
    pub drop_in_place: unsafe fn(ptr: *mut u8) -> Result<(), CallbackPanic>,
}

/// The generated skip-writer table, indexed by `SkipDefaultId`.
pub struct SkipWriterTable {
    pub entries: &'static [SkipEntry],
}

/// Index into the enclosing type's skip-writer table, generated by
/// #[asset] in the asset-types crate (fixup runs game-side, §15): one
/// native default constructor per #[asset(skip)] field. Skipped fields
/// have no schema presence and no asset TypeUuid, so they key
/// positionally per type; a skipped field whose type lacks Default is a
/// compile error at #[asset] expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SkipDefaultId(pub u32);

/// The measured native layout tree (§12), generated as statics by
/// #[asset] in every consuming binary: the native-side input to
/// fixup-plan compilation and the tree the §5 digests hash. Offsets are
/// frame-relative; skip slots appear with their writers (schema-invisible
/// but plan-visible). Generated statics cannot hold heap collections, so
/// every aggregate is a `&'static` slice.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NativeLayoutNode {
    Scalar {
        offset: u32,
        size: u32,
        align: u32,
        kind: ScalarKind,
    },
    /// `fields` in **physical order**: sorted by (offset ascending,
    /// declaration index ascending). `whole_drop` is the completed
    /// aggregate's rollback entry (None ⇔ no drop glue).
    Struct {
        offset: u32,
        size: u32,
        align: u32,
        whole_drop: Option<DropId>,
        fields: &'static [NativeField], // skip slots included
    },
    /// `variants` are self-contained records: name, declaration index,
    /// payload node, and that variant's own tag info in one place.
    Enum {
        offset: u32,
        size: u32,
        align: u32,
        tag: NativeTagEncoding,
        whole_drop: Option<DropId>,
        variants: &'static [NativeVariant],
    },
    Array {
        offset: u32,
        size: u32,
        align: u32,
        len: u32,
        stride: u32,
        elem: &'static NativeLayoutNode,
    },
    Vec {
        offset: u32,
        size: u32,
        align: u32,
        elem: &'static NativeLayoutNode,
        ctor: CtorId,
    },
    Set {
        offset: u32,
        size: u32,
        align: u32,
        elem: &'static NativeLayoutNode,
        ctor: CtorId,
    },
    Map {
        offset: u32,
        size: u32,
        align: u32,
        key: &'static NativeLayoutNode,
        value: &'static NativeLayoutNode,
        ctor: CtorId,
    },
    BoxPtr {
        offset: u32,
        size: u32,
        align: u32,
        inner: &'static NativeLayoutNode,
        ctor: CtorId,
    },
    ArcPtr {
        offset: u32,
        size: u32,
        align: u32,
        inner: &'static NativeLayoutNode,
        ctor: CtorId,
    },
    /// ConstructString/rollback built in.
    Str { offset: u32, size: u32, align: u32 },
    /// The §4 Blob native form.
    Blob { offset: u32, size: u32, align: u32 },
    /// Skipped slots are schema-invisible but plan-visible: alignment is
    /// measured like every slot's — the DSNL promise that skipped-slot
    /// geometry is measured, not inferred.
    Skip {
        offset: u32,
        size: u32,
        align: u32,
        writer: SkipDefaultId,
    },
    /// §5-style frame distance plus this slot's own frame-relative
    /// origin; size and align are the referenced frame's, by rule —
    /// stated as inferred, never re-stated (they could only disagree).
    BackRef { distance: u32, offset: u32 },
    /// Size 0, align 1 — stated normatively, so the node carries a
    /// well-defined position like every other node.
    Unit { offset: u32 },
}

impl NativeLayoutNode {
    /// The slot's own frame-relative origin — every kind carries one.
    pub fn offset(&self) -> u32 {
        match *self {
            NativeLayoutNode::Scalar { offset, .. }
            | NativeLayoutNode::Struct { offset, .. }
            | NativeLayoutNode::Enum { offset, .. }
            | NativeLayoutNode::Array { offset, .. }
            | NativeLayoutNode::Vec { offset, .. }
            | NativeLayoutNode::Set { offset, .. }
            | NativeLayoutNode::Map { offset, .. }
            | NativeLayoutNode::BoxPtr { offset, .. }
            | NativeLayoutNode::ArcPtr { offset, .. }
            | NativeLayoutNode::Str { offset, .. }
            | NativeLayoutNode::Blob { offset, .. }
            | NativeLayoutNode::Skip { offset, .. }
            | NativeLayoutNode::BackRef { offset, .. }
            | NativeLayoutNode::Unit { offset } => offset,
        }
    }
}

/// A struct field record (§12): the DSNL/DSWL physical order is (offset
/// ascending, declaration index ascending), and after wire repacking the
/// declaration index is the only tie-break for equal offsets — native
/// slice order alone cannot supply it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NativeField {
    /// Decimal for tuples (§5).
    pub name: &'static str,
    /// The source declaration index.
    pub declaration_index: u32,
    pub node: NativeLayoutNode,
}

/// One enum variant, self-contained (§12): discriminant ↔ name
/// association is by construction — the value sits in the same record as
/// the name it belongs to; wire tables derive their name-sorted order
/// (§5) by re-sorting these records.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NativeVariant {
    pub name: &'static str,
    pub declaration_index: u32,
    pub node: &'static NativeLayoutNode,
    pub tag: NativeVariantTag,
}

/// Per-variant tag info, carried in the variant record (never a parallel
/// array). Values are raw bits per `NativeTagEncoding`'s rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeVariantTag {
    /// Raw bits at tag width, zero-extended.
    Direct { value: u128 },
    /// Discriminant = niche_start + index, at tag width.
    Niche { index: u32 },
    /// The niche encoding's untagged variant.
    Untagged,
    /// Single-variant enums: nothing to read or write.
    Single,
}

/// Const-constructible tag encoding (§12). **Discriminants are raw bits,
/// everywhere**: the native discriminant truncated to the tag's byte
/// width (two's complement for signed reprs), zero-extended to `u128`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeTagEncoding {
    Direct {
        offset: u32,
        size: u8,
    },
    Niche {
        offset: u32,
        size: u8,
        niche_start: u128,
    },
    Single,
}

/// Failures serializing a layout tree under the DSNL or DSWL grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayoutHashError {
    /// A back-reference names a frame that is not on the expansion path.
    BackRefOutOfRange { distance: u32, frames: u32 },
    /// Two fields or variants share one (NFC-normalized) name — no
    /// canonical order exists, so no canonical bytes exist.
    DuplicateName(String),
    /// A field or variant name exceeds the grammar's u32 length bound.
    NameTooLong,
}

impl std::fmt::Display for LayoutHashError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LayoutHashError::BackRefOutOfRange { distance, frames } => write!(
                f,
                "backref distance {distance} exceeds the {frames} frames on the expansion path"
            ),
            LayoutHashError::DuplicateName(n) => {
                write!(f, "duplicate field/variant name {n:?}")
            }
            LayoutHashError::NameTooLong => write!(f, "name exceeds u32 byte length"),
        }
    }
}

impl std::error::Error for LayoutHashError {}
