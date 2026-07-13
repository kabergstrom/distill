//! Fixup plans (§12): compiled once per (type, wire layout hash,
//! fixup-table identity) from the producing wire tree and the consumer's
//! measured native tree. A plan is an ordered op list over a
//! `MaybeUninit` destination — flat-copy ops for the maximal runs where
//! wire and native offsets coincide, then typed construction over every
//! pointer-shaped slot. Construction is typed, never offset-blind: each
//! construct op names a `CtorId` into the consuming binary's generated
//! constructor table.
//!
//! Frame conventions (normative for this compiler/executor pair):
//! - Plan 0 is the root; every sub-plan (Recurse target, container
//!   element, box/arc inner, map key/value) is rooted at its own value
//!   origin — offsets are relative to that origin on both sides.
//! - Non-single enums always compile as their own enum-rooted plan whose
//!   sole op is the `SwitchVariant`; variant sub-plans are enum-relative
//!   on both sides and carry `whole_drop: None` — the enum plan's
//!   completion pushes the enum's whole drop, after the tag write.
//! - Variant payload nodes' own `whole_drop` annotations are ignored: a
//!   variant payload is not a standalone value; the enum's glue covers it.

use crate::dsnl::nfc_of;
use crate::native::{
    CtorId, DropId, NativeLayoutNode, NativeTagEncoding, NativeVariantTag, ScalarKind,
    SkipDefaultId,
};
use crate::wire::{SlotKind, WireEnumForm, WireNode};
use std::collections::HashMap;
use std::ops::Range;

/// Arena of compiled plans, keyed by (type, wire layout hash, fixup-table
/// identity §5). Index 0 = root; `PlanId` indexes the arena, and an op may
/// reference any plan, ancestors included — recursive types close cycles
/// through indirection ops, finite because element counts and presence
/// are data.
#[derive(Debug, Clone, PartialEq)]
pub struct FixupPlanArena {
    pub plans: Vec<FixupPlan>,
}

/// One frame's op list.
#[derive(Debug, Clone, PartialEq)]
pub struct FixupPlan {
    pub ops: Vec<FixupOp>,
    /// The framed-rollback entry pushed when this plan's aggregate
    /// completes — the whole value's drop glue (fields plus any custom
    /// Drop), from the generated drop table. None ⇔ the type has no drop
    /// glue: nothing is pushed, the frame just disarms.
    pub whole_drop: Option<DropId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanId(pub u32);

/// The §12 op vocabulary, exactly.
#[derive(Debug, Clone, PartialEq)]
pub enum FixupOp {
    FlatCopy {
        wire: Range<u32>,
        native: u32,
    },
    ConstructVec {
        wire_slot: u32,
        native: u32,
        elem: PlanId,
        ctor: CtorId,
    },
    ConstructString {
        wire_slot: u32,
        native: u32,
    },
    ConstructMap {
        wire_slot: u32,
        native: u32,
        key: PlanId,
        value: PlanId,
        ctor: CtorId,
    },
    ConstructSet {
        wire_slot: u32,
        native: u32,
        elem: PlanId,
        ctor: CtorId,
    },
    ConstructBox {
        wire_slot: u32,
        native: u32,
        inner: PlanId,
        ctor: CtorId,
    },
    ConstructArc {
        wire_slot: u32,
        native: u32,
        inner: PlanId,
        ctor: CtorId,
    },
    /// BlobRef → blob table.
    ConstructBlob {
        wire_slot: u32,
        native: u32,
    },
    WriteSkipDefault {
        native: u32,
        writer: SkipDefaultId,
    },
    /// bool/char bit patterns.
    ValidateScalar {
        wire: u32,
        kind: ScalarKind,
    },
    /// Variants in name-sorted order (§5).
    SwitchVariant {
        wire_tag: WireTagRead,
        native: u32,
        variants: Vec<(NativeTagWrite, PlanId)>,
    },
    Recurse {
        wire: u32,
        native: u32,
        plan: PlanId,
    },
}

/// How the wire tag is READ — from the *wire* layout tree, a separate
/// axis from `NativeTagWrite`: a wire produced under a fully-flat
/// encoding may fix into a native niche and vice versa. A read value
/// matching no variant is an integrity error (§12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireTagRead {
    /// Name-sorted variant index (§5).
    CanonicalU32 { offset: u32 },
    /// The wire's discriminant per variant, name-sorted order (§5); raw
    /// bits at tag width zero-extended — matching is raw-bit equality,
    /// signedness never enters.
    Direct {
        offset: u32,
        size: u8,
        values: Vec<u128>,
    },
}

/// How a variant's native discriminant is written — from the layout
/// schema's tag encoding, never read back from wire bytes. Values are
/// raw bits at tag width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeTagWrite {
    Direct {
        offset: u32,
        size: u8,
        value: u128,
    },
    /// The niche's reserved value.
    Niche {
        offset: u32,
        size: u8,
        value: u128,
    },
    /// The niche lives inside payload bytes: writing the payload writes
    /// the tag (Option<Box<T>>'s Some).
    PayloadImplied,
    /// Single-variant: nothing to write.
    None,
}

/// Per-plan geometry the executor needs beyond the §12 op fields:
/// element strides in the variable section and temp/len validation come
/// from the plan root's wire and native geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanMeta {
    pub wire_size: u32,
    pub wire_align: u32,
    pub native_size: u32,
    pub native_align: u32,
}

impl PlanMeta {
    /// Element stride in the variable section: size rounded to alignment.
    pub fn wire_stride(&self) -> u32 {
        let a = self.wire_align.max(1);
        let rem = self.wire_size % a;
        if rem == 0 {
            self.wire_size
        } else {
            self.wire_size + (a - rem)
        }
    }
}

/// A compiled arena plus its parallel metadata (index i describes
/// `arena.plans[i]`).
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledPlans {
    pub arena: FixupPlanArena,
    pub metas: Vec<PlanMeta>,
}

/// Plan-compilation failures. The trees authenticate separately (logical
/// hash, DSWL recomputation); a mismatch here means corrupted inputs or a
/// registry disagreement — named, never guessed around.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// The wire and native trees disagree structurally.
    Mismatch { detail: String },
    /// Offset arithmetic left u32 (§12: reject, never wrap).
    Bounds { what: &'static str, value: u64 },
    /// Two flat-copy sources overlap — a malformed wire tree.
    Overlap { at: u32 },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::Mismatch { detail } => write!(f, "wire/native tree mismatch: {detail}"),
            PlanError::Bounds { what, value } => {
                write!(f, "plan {what} {value} exceeds the u32 bound")
            }
            PlanError::Overlap { at } => write!(f, "overlapping flat-copy runs at {at}"),
        }
    }
}

impl std::error::Error for PlanError {}

/// Compile fixup plans for (producing wire tree, consumer native tree).
pub fn compile_plans(
    wire: &WireNode,
    native: &NativeLayoutNode,
) -> Result<CompiledPlans, PlanError> {
    let mut c = Compiler {
        plans: Vec::new(),
        metas: Vec::new(),
        memo: HashMap::new(),
        wire_frames: Vec::new(),
        native_frames: Vec::new(),
    };
    c.compile_frame(wire, native)?;
    let plans = c
        .plans
        .into_iter()
        .map(|p| p.expect("every reserved plan is filled"))
        .collect();
    Ok(CompiledPlans {
        arena: FixupPlanArena { plans },
        metas: c.metas,
    })
}

fn mismatch(detail: impl Into<String>) -> PlanError {
    PlanError::Mismatch {
        detail: detail.into(),
    }
}

fn add32(a: u32, b: u32, what: &'static str) -> Result<u32, PlanError> {
    a.checked_add(b).ok_or(PlanError::Bounds {
        what,
        value: a as u64 + b as u64,
    })
}

fn wire_geom(w: &WireNode) -> (u32, u32) {
    match *w {
        WireNode::Primitive { size, align, .. }
        | WireNode::Struct { size, align, .. }
        | WireNode::Enum { size, align, .. }
        | WireNode::Array { size, align, .. }
        | WireNode::Slot { size, align, .. } => (size, align),
        WireNode::Unit { .. } => (0, 1),
        WireNode::BackRef { .. } => (0, 1),
    }
}

fn native_geom(n: &NativeLayoutNode) -> (u32, u32) {
    match *n {
        NativeLayoutNode::Scalar { size, align, .. }
        | NativeLayoutNode::Struct { size, align, .. }
        | NativeLayoutNode::Enum { size, align, .. }
        | NativeLayoutNode::Array { size, align, .. }
        | NativeLayoutNode::Vec { size, align, .. }
        | NativeLayoutNode::Set { size, align, .. }
        | NativeLayoutNode::Map { size, align, .. }
        | NativeLayoutNode::BoxPtr { size, align, .. }
        | NativeLayoutNode::ArcPtr { size, align, .. }
        | NativeLayoutNode::Str { size, align, .. }
        | NativeLayoutNode::Blob { size, align, .. }
        | NativeLayoutNode::Skip { size, align, .. } => (size, align),
        NativeLayoutNode::Unit { .. } => (0, 1),
        NativeLayoutNode::BackRef { .. } => (0, 1),
    }
}

fn whole_drop_of(n: &NativeLayoutNode) -> Option<DropId> {
    match n {
        NativeLayoutNode::Struct { whole_drop, .. } | NativeLayoutNode::Enum { whole_drop, .. } => {
            *whole_drop
        }
        _ => None,
    }
}

/// Does this native subtree require its own frame when it appears as a
/// struct field — constructions anywhere, or whole-value drop glue?
fn needs_frame(n: &NativeLayoutNode) -> bool {
    match n {
        NativeLayoutNode::Scalar { .. } | NativeLayoutNode::Unit { .. } => false,
        NativeLayoutNode::Str { .. }
        | NativeLayoutNode::Blob { .. }
        | NativeLayoutNode::Vec { .. }
        | NativeLayoutNode::Set { .. }
        | NativeLayoutNode::Map { .. }
        | NativeLayoutNode::BoxPtr { .. }
        | NativeLayoutNode::ArcPtr { .. }
        | NativeLayoutNode::Skip { .. }
        | NativeLayoutNode::BackRef { .. } => true,
        NativeLayoutNode::Array { elem, .. } => needs_frame(elem),
        NativeLayoutNode::Struct {
            whole_drop, fields, ..
        } => whole_drop.is_some() || fields.iter().any(|f| needs_frame(&f.node)),
        NativeLayoutNode::Enum {
            whole_drop,
            tag,
            variants,
            ..
        } => {
            whole_drop.is_some()
                || !matches!(tag, NativeTagEncoding::Single)
                || variants.iter().any(|v| needs_frame(v.node))
        }
    }
}

/// Raw bits at tag width: truncate to `size` bytes, zero-extended.
fn mask_to_width(value: u128, size: u8) -> u128 {
    if size >= 16 {
        value
    } else {
        value & ((1u128 << (size as u32 * 8)) - 1)
    }
}

#[derive(Default)]
struct Acc {
    /// (wire start, len, native start)
    segments: Vec<(u32, u32, u32)>,
    /// (wire offset, kind)
    validates: Vec<(u32, ScalarKind)>,
    constructs: Vec<FixupOp>,
}

struct Compiler<'w, 'n> {
    plans: Vec<Option<FixupPlan>>,
    metas: Vec<PlanMeta>,
    /// (wire node ptr, native node ptr) → plan — pointee/Recurse roots.
    memo: HashMap<(usize, usize), PlanId>,
    /// Struct/enum frames on the expansion path — §5's backref counting.
    wire_frames: Vec<&'w WireNode>,
    native_frames: Vec<&'n NativeLayoutNode>,
}

impl<'w, 'n> Compiler<'w, 'n> {
    fn reserve(&mut self, w: &'w WireNode, n: &'n NativeLayoutNode, memoize: bool) -> PlanId {
        let id = PlanId(self.plans.len() as u32);
        self.plans.push(None);
        let (ws, wa) = wire_geom(w);
        let (ns, na) = native_geom(n);
        self.metas.push(PlanMeta {
            wire_size: ws,
            wire_align: wa,
            native_size: ns,
            native_align: na,
        });
        if memoize {
            let key = (w as *const _ as usize, n as *const _ as usize);
            self.memo.insert(key, id);
        }
        id
    }

    /// Compile an origin-rooted plan for the pair (offsets relative to
    /// the pair's own value origin), memoized by node identity so
    /// recursive types close cycles.
    fn compile_frame(
        &mut self,
        w: &'w WireNode,
        n: &'n NativeLayoutNode,
    ) -> Result<PlanId, PlanError> {
        let key = (w as *const _ as usize, n as *const _ as usize);
        if let Some(id) = self.memo.get(&key) {
            return Ok(*id);
        }
        let id = self.reserve(w, n, true);
        let mut acc = Acc::default();
        self.walk(w, n, 0, 0, &mut acc)?;
        let ops = assemble(acc)?;
        self.plans[id.0 as usize] = Some(FixupPlan {
            ops,
            whole_drop: whole_drop_of(n),
        });
        Ok(id)
    }

    /// Compile a variant payload plan: offsets are ENUM-relative (the
    /// payload nodes carry their enum-frame offsets), whole_drop None —
    /// the enum plan's completion owns the frame entry.
    fn compile_variant_plan(
        &mut self,
        w: &'w WireNode,
        n: &'n NativeLayoutNode,
    ) -> Result<PlanId, PlanError> {
        let id = self.reserve(w, n, false);
        let mut acc = Acc::default();
        self.walk(w, n, w.offset(), n.offset(), &mut acc)?;
        let ops = assemble(acc)?;
        self.plans[id.0 as usize] = Some(FixupPlan {
            ops,
            whole_drop: None,
        });
        Ok(id)
    }

    /// A slot pointee: backref pairs resolve to the ancestor plan; fresh
    /// pairs compile origin-rooted.
    fn pointee_plan(
        &mut self,
        w: &'w WireNode,
        n: &'n NativeLayoutNode,
    ) -> Result<PlanId, PlanError> {
        match (w, n) {
            (
                WireNode::BackRef { distance: dw, .. },
                NativeLayoutNode::BackRef { distance: dn, .. },
            ) => {
                let wi = self
                    .wire_frames
                    .len()
                    .checked_sub(1 + *dw as usize)
                    .ok_or_else(|| mismatch(format!("wire backref distance {dw} out of range")))?;
                let ni = self
                    .native_frames
                    .len()
                    .checked_sub(1 + *dn as usize)
                    .ok_or_else(|| {
                        mismatch(format!("native backref distance {dn} out of range"))
                    })?;
                let wt = self.wire_frames[wi];
                let nt = self.native_frames[ni];
                let key = (wt as *const _ as usize, nt as *const _ as usize);
                self.memo.get(&key).copied().ok_or_else(|| {
                    mismatch("backref target frames do not name one plan root".to_string())
                })
            }
            (WireNode::BackRef { .. }, _) | (_, NativeLayoutNode::BackRef { .. }) => Err(mismatch(
                "backref on one side only: wire and native trees disagree",
            )),
            _ => self.compile_frame(w, n),
        }
    }

    fn walk(
        &mut self,
        w: &'w WireNode,
        n: &'n NativeLayoutNode,
        w_at: u32,
        n_at: u32,
        acc: &mut Acc,
    ) -> Result<(), PlanError> {
        match (w, n) {
            (
                WireNode::Primitive {
                    size: ws, kind: wk, ..
                },
                NativeLayoutNode::Scalar {
                    size: ns, kind: nk, ..
                },
            ) => {
                if wk != nk {
                    return Err(mismatch(format!("scalar kind {wk:?} vs {nk:?}")));
                }
                if ws != ns {
                    return Err(mismatch(format!("scalar size {ws} vs {ns} for {wk:?}")));
                }
                if matches!(wk, ScalarKind::Bool | ScalarKind::Char) {
                    acc.validates.push((w_at, *wk));
                }
                if *ws > 0 {
                    acc.segments.push((w_at, *ws, n_at));
                }
                Ok(())
            }
            (WireNode::Unit { .. }, NativeLayoutNode::Unit { .. }) => Ok(()),
            (WireNode::Struct { .. }, NativeLayoutNode::Struct { .. }) => {
                self.wire_frames.push(w);
                self.native_frames.push(n);
                let r = self.walk_struct(w, n, w_at, n_at, acc);
                self.wire_frames.pop();
                self.native_frames.pop();
                r
            }
            (WireNode::Enum { .. }, NativeLayoutNode::Enum { .. }) => {
                self.walk_enum(w, n, w_at, n_at, acc)
            }
            (
                WireNode::Array {
                    len: wl,
                    stride: wstride,
                    elem: we,
                    ..
                },
                NativeLayoutNode::Array {
                    len: nl,
                    stride: nstride,
                    elem: ne,
                    ..
                },
            ) => {
                if wl != nl {
                    return Err(mismatch(format!("array length {wl} vs {nl}")));
                }
                if needs_frame(ne) {
                    let plan = self.compile_frame(we, ne)?;
                    for i in 0..*wl {
                        let wo = add32(w_at, mul32(i, *wstride, "array offset")?, "array offset")?;
                        let no = add32(n_at, mul32(i, *nstride, "array offset")?, "array offset")?;
                        acc.constructs.push(FixupOp::Recurse {
                            wire: add32(wo, we.offset(), "array offset")?,
                            native: add32(no, ne.offset(), "array offset")?,
                            plan,
                        });
                    }
                } else {
                    for i in 0..*wl {
                        let wo = add32(w_at, mul32(i, *wstride, "array offset")?, "array offset")?;
                        let no = add32(n_at, mul32(i, *nstride, "array offset")?, "array offset")?;
                        self.walk(
                            we,
                            ne,
                            add32(wo, we.offset(), "array offset")?,
                            add32(no, ne.offset(), "array offset")?,
                            acc,
                        )?;
                    }
                }
                Ok(())
            }
            (WireNode::Slot { kind, pointee, .. }, _) => {
                self.walk_slot(*kind, pointee, n, w_at, n_at, acc)
            }
            (WireNode::BackRef { .. }, _) | (_, NativeLayoutNode::BackRef { .. }) => Err(mismatch(
                "backref outside an indirection pointee".to_string(),
            )),
            _ => Err(mismatch(format!(
                "wire {} vs native {} at wire offset {w_at}",
                wire_kind_name(w),
                native_kind_name(n)
            ))),
        }
    }

    fn walk_slot(
        &mut self,
        kind: SlotKind,
        pointee: &'w [WireNode],
        n: &'n NativeLayoutNode,
        w_at: u32,
        n_at: u32,
        acc: &mut Acc,
    ) -> Result<(), PlanError> {
        let expect_pointees = |count: usize| -> Result<(), PlanError> {
            if pointee.len() == count {
                Ok(())
            } else {
                Err(mismatch(format!(
                    "{kind:?} slot with {} pointees, expected {count}",
                    pointee.len()
                )))
            }
        };
        match (kind, n) {
            (SlotKind::String, NativeLayoutNode::Str { .. }) => {
                expect_pointees(0)?;
                acc.constructs.push(FixupOp::ConstructString {
                    wire_slot: w_at,
                    native: n_at,
                });
                Ok(())
            }
            (SlotKind::Blob, NativeLayoutNode::Blob { .. }) => {
                expect_pointees(0)?;
                acc.constructs.push(FixupOp::ConstructBlob {
                    wire_slot: w_at,
                    native: n_at,
                });
                Ok(())
            }
            (SlotKind::Vec, NativeLayoutNode::Vec { elem, ctor, .. }) => {
                expect_pointees(1)?;
                let plan = self.pointee_plan(&pointee[0], elem)?;
                acc.constructs.push(FixupOp::ConstructVec {
                    wire_slot: w_at,
                    native: n_at,
                    elem: plan,
                    ctor: *ctor,
                });
                Ok(())
            }
            (SlotKind::Set, NativeLayoutNode::Set { elem, ctor, .. }) => {
                expect_pointees(1)?;
                let plan = self.pointee_plan(&pointee[0], elem)?;
                acc.constructs.push(FixupOp::ConstructSet {
                    wire_slot: w_at,
                    native: n_at,
                    elem: plan,
                    ctor: *ctor,
                });
                Ok(())
            }
            (
                SlotKind::Map,
                NativeLayoutNode::Map {
                    key, value, ctor, ..
                },
            ) => {
                expect_pointees(2)?;
                let key_plan = self.pointee_plan(&pointee[0], key)?;
                let value_plan = self.pointee_plan(&pointee[1], value)?;
                acc.constructs.push(FixupOp::ConstructMap {
                    wire_slot: w_at,
                    native: n_at,
                    key: key_plan,
                    value: value_plan,
                    ctor: *ctor,
                });
                Ok(())
            }
            (SlotKind::Box, NativeLayoutNode::BoxPtr { inner, ctor, .. }) => {
                expect_pointees(1)?;
                let plan = self.pointee_plan(&pointee[0], inner)?;
                acc.constructs.push(FixupOp::ConstructBox {
                    wire_slot: w_at,
                    native: n_at,
                    inner: plan,
                    ctor: *ctor,
                });
                Ok(())
            }
            (SlotKind::Arc, NativeLayoutNode::ArcPtr { inner, ctor, .. }) => {
                expect_pointees(1)?;
                let plan = self.pointee_plan(&pointee[0], inner)?;
                acc.constructs.push(FixupOp::ConstructArc {
                    wire_slot: w_at,
                    native: n_at,
                    inner: plan,
                    ctor: *ctor,
                });
                Ok(())
            }
            _ => Err(mismatch(format!(
                "{kind:?} slot vs native {}",
                native_kind_name(n)
            ))),
        }
    }

    fn walk_struct(
        &mut self,
        w: &'w WireNode,
        n: &'n NativeLayoutNode,
        w_at: u32,
        n_at: u32,
        acc: &mut Acc,
    ) -> Result<(), PlanError> {
        let (WireNode::Struct { fields: wf, .. }, NativeLayoutNode::Struct { fields: nf, .. }) =
            (w, n)
        else {
            unreachable!("walk_struct called on struct pairs only");
        };

        // Wire fields by NFC name.
        let mut by_name: HashMap<String, &'w crate::wire::WireField> = HashMap::new();
        for field in wf.iter() {
            if by_name
                .insert(nfc_of(&field.name).into_owned(), field)
                .is_some()
            {
                return Err(mismatch(format!("duplicate wire field {:?}", field.name)));
            }
        }

        // Native fields in physical order.
        let mut ordered: Vec<&'n crate::native::NativeField> = nf.iter().collect();
        ordered.sort_by_key(|f| (f.node.offset(), f.declaration_index));

        let mut matched = 0usize;
        for field in ordered {
            if let NativeLayoutNode::Skip { offset, writer, .. } = field.node {
                acc.constructs.push(FixupOp::WriteSkipDefault {
                    native: add32(n_at, offset, "skip offset")?,
                    writer,
                });
                continue;
            }
            let key = nfc_of(field.name).into_owned();
            let wire_field = by_name.get(key.as_str()).ok_or_else(|| {
                mismatch(format!("native field {:?} missing on the wire", field.name))
            })?;
            matched += 1;
            let w_child = &wire_field.node;
            let n_child = &field.node;
            let cw = add32(w_at, w_child.offset(), "field offset")?;
            let cn = add32(n_at, n_child.offset(), "field offset")?;
            let aggregate_pair = matches!(
                (w_child, n_child),
                (WireNode::Struct { .. }, NativeLayoutNode::Struct { .. })
                    | (WireNode::Enum { .. }, NativeLayoutNode::Enum { .. })
            );
            if aggregate_pair && needs_frame(n_child) {
                let plan = self.compile_frame(w_child, n_child)?;
                acc.constructs.push(FixupOp::Recurse {
                    wire: cw,
                    native: cn,
                    plan,
                });
            } else {
                self.walk(w_child, n_child, cw, cn, acc)?;
            }
        }
        // Every wire field must have found a native twin.
        if matched != wf.len() {
            let native_names: Vec<&str> = nf.iter().map(|f| f.name).collect();
            for field in wf.iter() {
                let key = nfc_of(&field.name);
                if !nf.iter().any(|f| nfc_of(f.name) == key) {
                    return Err(mismatch(format!(
                        "wire field {:?} missing natively (native fields: {native_names:?})",
                        field.name
                    )));
                }
            }
        }
        Ok(())
    }

    fn walk_enum(
        &mut self,
        w: &'w WireNode,
        n: &'n NativeLayoutNode,
        w_at: u32,
        n_at: u32,
        acc: &mut Acc,
    ) -> Result<(), PlanError> {
        let (
            WireNode::Enum {
                form, variants: wv, ..
            },
            NativeLayoutNode::Enum {
                tag, variants: nv, ..
            },
        ) = (w, n)
        else {
            unreachable!("walk_enum called on enum pairs only");
        };

        if matches!(form, WireEnumForm::Single) {
            if !matches!(tag, NativeTagEncoding::Single) {
                return Err(mismatch(
                    "wire single-variant enum vs native multi-variant encoding",
                ));
            }
            if wv.len() != 1 || nv.len() != 1 {
                return Err(mismatch(format!(
                    "single-variant enum with {} wire / {} native variants",
                    wv.len(),
                    nv.len()
                )));
            }
            if nfc_of(&wv[0].name) != nfc_of(nv[0].name) {
                return Err(mismatch(format!(
                    "variant name {:?} vs {:?}",
                    wv[0].name, nv[0].name
                )));
            }
            self.wire_frames.push(w);
            self.native_frames.push(n);
            let r = self.walk(
                &wv[0].node,
                nv[0].node,
                add32(w_at, wv[0].node.offset(), "payload offset")?,
                add32(n_at, nv[0].node.offset(), "payload offset")?,
                acc,
            );
            self.wire_frames.pop();
            self.native_frames.pop();
            return r;
        }

        // Non-single enums are always enum-rooted plans: the walk only
        // reaches here at the frame root (children Recurse), so the enum
        // origin is the frame base on both sides.
        debug_assert_eq!(w_at, 0, "non-single enums compile enum-rooted");
        debug_assert_eq!(n_at, 0, "non-single enums compile enum-rooted");

        // Name-sorted pairing (§5's order).
        let mut ws: Vec<_> = wv
            .iter()
            .map(|v| (nfc_of(&v.name).into_owned(), v))
            .collect();
        ws.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let mut ns: Vec<_> = nv
            .iter()
            .map(|v| (nfc_of(v.name).into_owned(), v))
            .collect();
        ns.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        if ws.len() != ns.len() {
            return Err(mismatch(format!(
                "enum with {} wire / {} native variants",
                ws.len(),
                ns.len()
            )));
        }
        for (a, b) in ws.iter().zip(ns.iter()) {
            if a.0 != b.0 {
                return Err(mismatch(format!("variant name {:?} vs {:?}", a.0, b.0)));
            }
        }

        let wire_tag = match form {
            WireEnumForm::Canonical => WireTagRead::CanonicalU32 { offset: 0 },
            WireEnumForm::FullyFlat {
                tag_offset,
                tag_size,
            } => WireTagRead::Direct {
                offset: *tag_offset,
                size: *tag_size,
                values: ws.iter().map(|(_, v)| v.discriminant).collect(),
            },
            WireEnumForm::Single => unreachable!(),
        };

        self.wire_frames.push(w);
        self.native_frames.push(n);
        let mut variants = Vec::with_capacity(ws.len());
        let mut result = Ok(());
        for ((_, wvar), (_, nvar)) in ws.iter().zip(ns.iter()) {
            let write = match (tag, nvar.tag) {
                (
                    NativeTagEncoding::Direct { offset, size },
                    NativeVariantTag::Direct { value },
                ) => NativeTagWrite::Direct {
                    offset: *offset,
                    size: *size,
                    value: mask_to_width(value, *size),
                },
                (
                    NativeTagEncoding::Niche {
                        offset,
                        size,
                        niche_start,
                    },
                    NativeVariantTag::Niche { index },
                ) => NativeTagWrite::Niche {
                    offset: *offset,
                    size: *size,
                    value: mask_to_width(niche_start.wrapping_add(index as u128), *size),
                },
                (NativeTagEncoding::Niche { .. }, NativeVariantTag::Untagged) => {
                    NativeTagWrite::PayloadImplied
                }
                (encoding, vtag) => {
                    result = Err(mismatch(format!(
                        "variant {:?}: tag info {vtag:?} under encoding {encoding:?}",
                        nvar.name
                    )));
                    break;
                }
            };
            match self.compile_variant_plan(&wvar.node, nvar.node) {
                Ok(plan) => variants.push((write, plan)),
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        self.wire_frames.pop();
        self.native_frames.pop();
        result?;

        acc.constructs.push(FixupOp::SwitchVariant {
            wire_tag,
            native: n_at,
            variants,
        });
        Ok(())
    }
}

fn mul32(a: u32, b: u32, what: &'static str) -> Result<u32, PlanError> {
    let v = a as u64 * b as u64;
    u32::try_from(v).map_err(|_| PlanError::Bounds { what, value: v })
}

fn wire_kind_name(w: &WireNode) -> &'static str {
    match w {
        WireNode::Primitive { .. } => "primitive",
        WireNode::Struct { .. } => "struct",
        WireNode::Enum { .. } => "enum",
        WireNode::Array { .. } => "array",
        WireNode::Slot { .. } => "slot",
        WireNode::BackRef { .. } => "backref",
        WireNode::Unit { .. } => "unit",
    }
}

fn native_kind_name(n: &NativeLayoutNode) -> &'static str {
    match n {
        NativeLayoutNode::Scalar { .. } => "scalar",
        NativeLayoutNode::Struct { .. } => "struct",
        NativeLayoutNode::Enum { .. } => "enum",
        NativeLayoutNode::Array { .. } => "array",
        NativeLayoutNode::Vec { .. } => "vec",
        NativeLayoutNode::Set { .. } => "set",
        NativeLayoutNode::Map { .. } => "map",
        NativeLayoutNode::BoxPtr { .. } => "box",
        NativeLayoutNode::ArcPtr { .. } => "arc",
        NativeLayoutNode::Str { .. } => "str",
        NativeLayoutNode::Blob { .. } => "blob",
        NativeLayoutNode::Skip { .. } => "skip",
        NativeLayoutNode::BackRef { .. } => "backref",
        NativeLayoutNode::Unit { .. } => "unit",
    }
}

/// Merge copy segments into maximal runs and order the op list: validated
/// runs first (wire offset ascending, `ValidateScalar` before the run's
/// copy), then constructions in walk order.
fn assemble(acc: Acc) -> Result<Vec<FixupOp>, PlanError> {
    let Acc {
        mut segments,
        mut validates,
        constructs,
    } = acc;
    segments.sort_by_key(|s| s.0);
    validates.sort_by_key(|v| v.0);

    // Merge adjacency on BOTH sides; reject overlapping sources.
    let mut runs: Vec<(u32, u32, u32)> = Vec::new();
    for (start, len, native) in segments {
        if let Some(last) = runs.last_mut() {
            let last_end = last.0 + last.1;
            if start < last_end {
                return Err(PlanError::Overlap { at: start });
            }
            if start == last_end && native == last.2 + last.1 {
                last.1 += len;
                continue;
            }
        }
        runs.push((start, len, native));
    }

    let mut ops = Vec::new();
    let mut vi = 0usize;
    for (start, len, native) in runs {
        let end = start + len;
        while vi < validates.len() && validates[vi].0 < end {
            let (offset, kind) = validates[vi];
            debug_assert!(offset >= start);
            ops.push(FixupOp::ValidateScalar { wire: offset, kind });
            vi += 1;
        }
        ops.push(FixupOp::FlatCopy {
            wire: start..end,
            native,
        });
    }
    debug_assert_eq!(vi, validates.len(), "every validate sits inside a run");
    ops.extend(constructs);
    Ok(ops)
}
