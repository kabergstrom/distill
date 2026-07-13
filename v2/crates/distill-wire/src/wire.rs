//! The wire layout tree (§12): derived deterministically from the native
//! layout by the layout schema — identical except where indirection or
//! niche encoding forces divergence. This is the tree the DSWL hash
//! covers, the tree the daemon persists under its hash, and the wire-side
//! input to fixup-plan compilation.

use crate::native::ScalarKind;

/// Which indirection a wire slot encodes. The slot holds a
/// `VarRef { offset: u32, len: u32 }` (`BlobRef { index: u32, zero: u32 }`
/// for `Blob`) in its first 8 bytes; the rest of the slot is zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotKind {
    Vec,
    String,
    Map,
    Set,
    Box,
    Arc,
    Blob,
}

impl SlotKind {
    /// The DSWL grammar id, pinned.
    pub fn grammar_id(self) -> u8 {
        match self {
            SlotKind::Vec => 0x00,
            SlotKind::String => 0x01,
            SlotKind::Map => 0x02,
            SlotKind::Set => 0x03,
            SlotKind::Box => 0x04,
            SlotKind::Arc => 0x05,
            SlotKind::Blob => 0x06,
        }
    }
}

/// The three wire enum forms (§12), exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireEnumForm {
    /// One variant: wire = the sole variant's payload, no tag.
    Single,
    /// Integer discriminant and every variant payload wire == native:
    /// wire = native byte image, tag bytes in place.
    FullyFlat { tag_offset: u32, tag_size: u8 },
    /// Everything else: `tag: u32` at wire offset 0 holding the variant's
    /// index in name-sorted order; payload union at
    /// `align_up(4, max variant wire align)`.
    Canonical,
}

/// One field of a wire struct. `declaration_index` is the DSWL physical
/// order tie-break for equal wire offsets (§12); it is never hashed.
#[derive(Debug, Clone, PartialEq)]
pub struct WireField {
    pub name: String,
    pub declaration_index: u32,
    pub node: WireNode,
}

/// One wire enum variant. `discriminant` is the wire's native raw-bits
/// discriminant — meaningful (and hashed) only under
/// `WireEnumForm::FullyFlat`.
#[derive(Debug, Clone, PartialEq)]
pub struct WireVariant {
    pub name: String,
    pub discriminant: u128,
    pub node: WireNode,
}

/// A node of the wire layout tree. Offsets are frame-relative, exactly as
/// in the native tree; a slot's pointee nodes are frame-relative to the
/// element.
#[derive(Debug, Clone, PartialEq)]
pub enum WireNode {
    Primitive {
        offset: u32,
        size: u32,
        align: u32,
        kind: ScalarKind,
    },
    Struct {
        offset: u32,
        size: u32,
        align: u32,
        fields: Vec<WireField>,
    },
    Enum {
        offset: u32,
        size: u32,
        align: u32,
        form: WireEnumForm,
        variants: Vec<WireVariant>,
    },
    Array {
        offset: u32,
        size: u32,
        align: u32,
        len: u32,
        stride: u32,
        elem: Box<WireNode>,
    },
    /// An indirection slot: `Vec`, `String`, maps, sets, `Box`, `Arc`
    /// hold a `VarRef`; `#[asset(blob)]` slots hold a `BlobRef`. Pointee
    /// count: 1 for vec/set/box/arc (the element), 2 for map (key then
    /// value), 0 for string/blob.
    Slot {
        offset: u32,
        size: u32,
        align: u32,
        kind: SlotKind,
        pointee: Vec<WireNode>,
    },
    /// Recursion terminator: frame distance (struct/enum wire frames on
    /// the current expansion path, 0 = innermost) plus this slot's own
    /// frame-relative origin; size and align are the referenced frame's,
    /// by rule.
    BackRef { distance: u32, offset: u32 },
    /// Size 0, align 1 by rule.
    Unit { offset: u32 },
}

impl WireNode {
    /// The node's frame-relative origin.
    pub fn offset(&self) -> u32 {
        match *self {
            WireNode::Primitive { offset, .. }
            | WireNode::Struct { offset, .. }
            | WireNode::Enum { offset, .. }
            | WireNode::Array { offset, .. }
            | WireNode::Slot { offset, .. }
            | WireNode::BackRef { offset, .. }
            | WireNode::Unit { offset } => offset,
        }
    }
}
