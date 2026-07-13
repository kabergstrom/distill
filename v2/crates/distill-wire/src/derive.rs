//! Wire-layout derivation (§12 "Wire layout, precisely"): the wire tree is
//! derived deterministically from the native layout by the layout schema —
//! identical except where indirection or niche encoding forces divergence.
//!
//! Normative rules implemented here:
//! - Primitives and flat structs/arrays of them: wire = native (the
//!   flat-copy case) — native offsets, rustc gaps included, are kept.
//! - Indirection slots keep native size/alignment; the first 8 bytes are
//!   the `VarRef` (`BlobRef` for `#[asset(blob)]`), the rest zero.
//! - `#[asset(skip)]` slots are not encoded. Their absence must leave the
//!   wire layout independent of skip geometry (§12's consumer-relayout
//!   premise: "an enlarged #[asset(skip)] field shifting later native
//!   offsets" *leaves wire bytes untouched*), so a struct containing any
//!   skip field is repacked over its encoded members.
//! - Structs (and payload unions) containing any size- or alignment-
//!   diverging member: field order preserved (native physical order),
//!   offsets recomputed by align-and-pack over wire sizes/alignments.
//! - Enums take exactly three forms: single-variant (no tag), fully flat
//!   (integer discriminant and every variant payload wire == native — the
//!   whole payload image, offsets included), or the canonical tagged form.
//! - u32 bounds are enforced here, where the u32 grammars begin: any
//!   size, offset, stride, or length beyond u32 is a typed error naming
//!   the type — never truncated, never wrapped.

use crate::native::ScalarKind;
use crate::wire::{SlotKind, WireEnumForm, WireField, WireNode, WireVariant};
use ngp_schema::classify::{classify, Class};
use ngp_schema::node::PrimitiveKind;
use ngp_schema::{FieldIdentifier, LayoutView, SchemaTypeId, TagEncoding};

/// Expansion-depth cap: recursion terminates through backrefs, so only a
/// pathologically deep non-recursive nesting can hit this.
pub const MAX_DERIVE_DEPTH: u32 = 1024;

/// Failures deriving a wire layout. Every error names the type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeriveError {
    /// A required measured fact (size, align, field offset) is absent.
    MissingLayout {
        type_path: String,
        what: &'static str,
    },
    /// An enum's layout record carries no tag encoding.
    MissingTagEncoding { type_path: String },
    /// The type is outside the serializable classifier — named, never
    /// guessed.
    Unsupported { type_path: String, reason: String },
    /// The enum's schema/layout records are inconsistent.
    MalformedEnum { type_path: String, detail: String },
    /// A hashed map/set without the fixed-seed `BuildHasher` (§5).
    NondeterministicHasher { type_path: String },
    /// A `#[asset(blob)]` field beneath a map key or set element (§5).
    BlobUnderOrderedKey { type_path: String, field: String },
    /// A u32 grammar bound was exceeded (§12: reject, never wrap).
    Bounds {
        type_path: String,
        what: &'static str,
        value: u64,
    },
    /// An indirection slot too small to hold its 8-byte `VarRef`.
    SlotTooSmall { type_path: String, size: u32 },
    /// Non-recursive nesting deeper than `MAX_DERIVE_DEPTH`.
    DepthExceeded { limit: u32 },
}

impl std::fmt::Display for DeriveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeriveError::MissingLayout { type_path, what } => {
                write!(f, "{type_path}: measured layout missing {what}")
            }
            DeriveError::MissingTagEncoding { type_path } => {
                write!(f, "{type_path}: enum layout carries no tag encoding")
            }
            DeriveError::Unsupported { type_path, reason } => {
                write!(f, "{type_path}: not serializable: {reason}")
            }
            DeriveError::MalformedEnum { type_path, detail } => {
                write!(f, "{type_path}: malformed enum: {detail}")
            }
            DeriveError::NondeterministicHasher { type_path } => write!(
                f,
                "{type_path}: hashed container without the fixed-seed BuildHasher"
            ),
            DeriveError::BlobUnderOrderedKey { type_path, field } => write!(
                f,
                "{type_path}.{field}: #[asset(blob)] beneath a map key or set element"
            ),
            DeriveError::Bounds {
                type_path,
                what,
                value,
            } => {
                write!(f, "{type_path}: {what} {value} exceeds the u32 bound")
            }
            DeriveError::SlotTooSmall { type_path, size } => {
                write!(
                    f,
                    "{type_path}: indirection slot of {size} bytes cannot hold a VarRef"
                )
            }
            DeriveError::DepthExceeded { limit } => {
                write!(f, "type nesting exceeds the derivation depth cap {limit}")
            }
        }
    }
}

impl std::error::Error for DeriveError {}

/// Derive the wire layout tree for `root` from the layout schema (§12).
pub fn derive_wire(view: LayoutView<'_>, root: SchemaTypeId) -> Result<WireNode, DeriveError> {
    let mut ctx = Ctx {
        view,
        path: Vec::new(),
        depth: 0,
    };
    Ok(derive_type(&mut ctx, root, false)?.node)
}

struct Ctx<'a> {
    view: LayoutView<'a>,
    /// Struct/enum frames on the expansion path (§5's backref rule):
    /// records, enums, and variant payload frames — exactly the nodes the
    /// DSWL hasher counts.
    path: Vec<SchemaTypeId>,
    depth: u32,
}

struct Derived {
    node: WireNode,
    native_size: u32,
    native_align: u32,
    wire_size: u32,
    wire_align: u32,
    /// Whether the wire subtree's geometry differs anywhere from native —
    /// slot interiors excluded (they are separate frames).
    diverged: bool,
}

fn type_path(ctx: &Ctx<'_>, id: SchemaTypeId) -> String {
    match ctx.view.schema.types.get(id.0) {
        Some(def) => {
            let p = def.path.display_path();
            if p.is_empty() {
                format!("type #{}", id.0)
            } else {
                p
            }
        }
        None => format!("type #{}", id.0),
    }
}

fn to_u32(ctx: &Ctx<'_>, id: SchemaTypeId, what: &'static str, v: u64) -> Result<u32, DeriveError> {
    u32::try_from(v).map_err(|_| DeriveError::Bounds {
        type_path: type_path(ctx, id),
        what,
        value: v,
    })
}

fn measured(ctx: &Ctx<'_>, id: SchemaTypeId) -> Result<(u32, u32), DeriveError> {
    let ty = ctx.view.ty(id).ok_or_else(|| DeriveError::Unsupported {
        type_path: type_path(ctx, id),
        reason: "dangling type id".to_string(),
    })?;
    let size = ty.size().ok_or_else(|| DeriveError::MissingLayout {
        type_path: type_path(ctx, id),
        what: "size",
    })?;
    let align = ty.align().ok_or_else(|| DeriveError::MissingLayout {
        type_path: type_path(ctx, id),
        what: "align",
    })?;
    Ok((
        to_u32(ctx, id, "size", size)?,
        to_u32(ctx, id, "align", align)?,
    ))
}

fn align_up(n: u32, align: u32) -> Option<u32> {
    debug_assert!(align >= 1);
    let rem = n % align;
    if rem == 0 {
        Some(n)
    } else {
        n.checked_add(align - rem)
    }
}

fn set_offset(node: &mut WireNode, new_offset: u32) {
    match node {
        WireNode::Primitive { offset, .. }
        | WireNode::Struct { offset, .. }
        | WireNode::Enum { offset, .. }
        | WireNode::Array { offset, .. }
        | WireNode::Slot { offset, .. }
        | WireNode::BackRef { offset, .. }
        | WireNode::Unit { offset } => *offset = new_offset,
    }
}

fn scalar_kind(k: PrimitiveKind) -> ScalarKind {
    match k {
        PrimitiveKind::Bool => ScalarKind::Bool,
        PrimitiveKind::Char => ScalarKind::Char,
        PrimitiveKind::U8 => ScalarKind::U8,
        PrimitiveKind::U16 => ScalarKind::U16,
        PrimitiveKind::U32 => ScalarKind::U32,
        PrimitiveKind::U64 => ScalarKind::U64,
        PrimitiveKind::U128 => ScalarKind::U128,
        PrimitiveKind::I8 => ScalarKind::I8,
        PrimitiveKind::I16 => ScalarKind::I16,
        PrimitiveKind::I32 => ScalarKind::I32,
        PrimitiveKind::I64 => ScalarKind::I64,
        PrimitiveKind::I128 => ScalarKind::I128,
        PrimitiveKind::F32 => ScalarKind::F32,
        PrimitiveKind::F64 => ScalarKind::F64,
    }
}

/// Derive a node for `id` at frame-relative offset 0 (callers position it).
/// `in_key` — the subtree sits beneath a map key or set element, where
/// blobs are barred (§5).
fn derive_type(ctx: &mut Ctx<'_>, id: SchemaTypeId, in_key: bool) -> Result<Derived, DeriveError> {
    if ctx.depth >= MAX_DERIVE_DEPTH {
        return Err(DeriveError::DepthExceeded {
            limit: MAX_DERIVE_DEPTH,
        });
    }
    ctx.depth += 1;
    let result = derive_type_inner(ctx, id, in_key);
    ctx.depth -= 1;
    result
}

fn derive_type_inner(
    ctx: &mut Ctx<'_>,
    id: SchemaTypeId,
    in_key: bool,
) -> Result<Derived, DeriveError> {
    let class = classify(ctx.view.schema, id);
    match class {
        Class::Primitive(k) => {
            let (size, align) = measured(ctx, id)?;
            Ok(Derived {
                node: WireNode::Primitive {
                    offset: 0,
                    size,
                    align,
                    kind: scalar_kind(k),
                },
                native_size: size,
                native_align: align,
                wire_size: size,
                wire_align: align,
                diverged: false,
            })
        }
        Class::Unit => Ok(Derived {
            node: WireNode::Unit { offset: 0 },
            native_size: 0,
            native_align: 1,
            wire_size: 0,
            wire_align: 1,
            diverged: false,
        }),
        Class::StringNode => slot(ctx, id, SlotKind::String, vec![]),
        Class::Boxed(t) => {
            let inner = derive_pointee(ctx, t, in_key)?;
            slot(ctx, id, SlotKind::Box, vec![inner])
        }
        Class::ArcOf(t) => {
            let inner = derive_pointee(ctx, t, in_key)?;
            slot(ctx, id, SlotKind::Arc, vec![inner])
        }
        Class::Vec(t) => {
            let elem = derive_pointee(ctx, t, in_key)?;
            slot(ctx, id, SlotKind::Vec, vec![elem])
        }
        Class::Set {
            elem,
            hasher,
            hashed,
        } => {
            check_hasher(ctx, id, hasher, hashed)?;
            let elem = derive_pointee(ctx, elem, true)?;
            slot(ctx, id, SlotKind::Set, vec![elem])
        }
        Class::Map {
            key,
            value,
            hasher,
            hashed,
        } => {
            check_hasher(ctx, id, hasher, hashed)?;
            let key = derive_pointee(ctx, key, true)?;
            // Values never participate in key ordering, but an enclosing
            // key/element subtree still bars blobs beneath them.
            let value = derive_pointee(ctx, value, in_key)?;
            slot(ctx, id, SlotKind::Map, vec![key, value])
        }
        Class::Array { len, elem } => derive_array(ctx, id, len, elem, in_key),
        Class::Option(_) | Class::Enum => derive_enum(ctx, id, in_key),
        Class::Struct | Class::Tuple | Class::AssetRef(_) | Class::WeakRef(_) => {
            derive_record(ctx, id, in_key, false)
        }
        Class::EnumVariant => Err(DeriveError::Unsupported {
            type_path: type_path(ctx, id),
            reason: "enum variant outside its enum".to_string(),
        }),
        Class::FixedState => Err(DeriveError::Unsupported {
            type_path: type_path(ctx, id),
            reason: "the fixed-seed BuildHasher is not a data type".to_string(),
        }),
        Class::SlotMap { .. } => Err(DeriveError::Unsupported {
            type_path: type_path(ctx, id),
            reason: "engine container (SlotMap)".to_string(),
        }),
        Class::Opaque(o) => Err(DeriveError::Unsupported {
            type_path: type_path(ctx, id),
            reason: format!("{o:?}"),
        }),
        Class::Malformed(m) => Err(DeriveError::Unsupported {
            type_path: type_path(ctx, id),
            reason: format!("malformed schema reference: {m:?}"),
        }),
    }
}

fn check_hasher(
    ctx: &Ctx<'_>,
    id: SchemaTypeId,
    hasher: Option<SchemaTypeId>,
    hashed: bool,
) -> Result<(), DeriveError> {
    if !hashed {
        return Ok(());
    }
    let deterministic = matches!(
        hasher.map(|h| classify(ctx.view.schema, h)),
        Some(Class::FixedState)
    );
    if deterministic {
        Ok(())
    } else {
        Err(DeriveError::NondeterministicHasher {
            type_path: type_path(ctx, id),
        })
    }
}

/// A slot pointee: a fresh element frame at offset 0. The expansion path
/// persists through indirection — that is what terminates recursion.
fn derive_pointee(
    ctx: &mut Ctx<'_>,
    id: SchemaTypeId,
    in_key: bool,
) -> Result<WireNode, DeriveError> {
    if let Some(pos) = ctx.path.iter().position(|p| *p == id) {
        let distance = (ctx.path.len() - 1 - pos) as u32;
        return Ok(WireNode::BackRef {
            distance,
            offset: 0,
        });
    }
    Ok(derive_type(ctx, id, in_key)?.node)
}

fn slot(
    ctx: &Ctx<'_>,
    id: SchemaTypeId,
    kind: SlotKind,
    pointee: Vec<WireNode>,
) -> Result<Derived, DeriveError> {
    let (size, align) = measured(ctx, id)?;
    if size < 8 {
        return Err(DeriveError::SlotTooSmall {
            type_path: type_path(ctx, id),
            size,
        });
    }
    Ok(Derived {
        node: WireNode::Slot {
            offset: 0,
            size,
            align,
            kind,
            pointee,
        },
        native_size: size,
        native_align: align,
        wire_size: size,
        wire_align: align,
        diverged: false,
    })
}

/// A `#[asset(blob)]` slot: the field type's measured geometry, a
/// `BlobRef` in the first 8 bytes, no pointee.
fn blob_slot(ctx: &Ctx<'_>, id: SchemaTypeId) -> Result<Derived, DeriveError> {
    slot(ctx, id, SlotKind::Blob, vec![])
}

fn derive_array(
    ctx: &mut Ctx<'_>,
    id: SchemaTypeId,
    len: u64,
    elem_id: SchemaTypeId,
    in_key: bool,
) -> Result<Derived, DeriveError> {
    let len = to_u32(ctx, id, "array length", len)?;
    let elem = derive_type(ctx, elem_id, in_key)?;
    let native_align = elem.native_align.max(1);
    let wire_align = elem.wire_align.max(1);
    let native_stride =
        align_up(elem.native_size, native_align).ok_or_else(|| DeriveError::Bounds {
            type_path: type_path(ctx, id),
            what: "native stride",
            value: elem.native_size as u64,
        })?;
    let wire_stride = align_up(elem.wire_size, wire_align).ok_or_else(|| DeriveError::Bounds {
        type_path: type_path(ctx, id),
        what: "wire stride",
        value: elem.wire_size as u64,
    })?;
    let native_size64 = native_stride as u64 * len as u64;
    let wire_size64 = wire_stride as u64 * len as u64;
    let native_size = to_u32(ctx, id, "array size", native_size64)?;
    let wire_size = to_u32(ctx, id, "array size", wire_size64)?;
    let diverged = elem.diverged || wire_stride != native_stride || wire_align != native_align;
    Ok(Derived {
        node: WireNode::Array {
            offset: 0,
            size: wire_size,
            align: wire_align,
            len,
            stride: wire_stride,
            elem: Box::new(elem.node),
        },
        native_size,
        native_align,
        wire_size,
        wire_align,
        diverged,
    })
}

/// A struct/tuple/variant-payload record. `force_repack` is the canonical
/// enum form's rule: payloads are re-laid from offset 0 unconditionally.
fn derive_record(
    ctx: &mut Ctx<'_>,
    id: SchemaTypeId,
    in_key: bool,
    force_repack: bool,
) -> Result<Derived, DeriveError> {
    let (native_size, native_align) = measured(ctx, id)?;
    let def = &ctx.view.schema.types[id.0];
    let field_count = def.fields.len();

    struct Member {
        name: String,
        declaration_index: u32,
        native_offset: u32,
        child: Derived,
    }

    ctx.path.push(id);
    let mut members: Vec<Member> = Vec::new();
    let mut skip_present = false;
    let mut result: Result<(), DeriveError> = Ok(());
    for i in 0..field_count {
        let def = &ctx.view.schema.types[id.0];
        let field = def.fields[i].clone();
        if field.attrs.skip {
            skip_present = true;
            continue;
        }
        let name = match &field.id {
            FieldIdentifier::Name(n) => n.clone(),
            FieldIdentifier::Number(n) => n.to_string(),
            FieldIdentifier::Variant(v) => {
                result = Err(DeriveError::Unsupported {
                    type_path: type_path(ctx, id),
                    reason: format!("variant field {v:?} in a record"),
                });
                break;
            }
        };
        let view_ty = ctx.view.ty(id).expect("checked by measured");
        let offset64 = match view_ty.field(i).and_then(|f| f.offset()) {
            Some(o) => o,
            None => {
                result = Err(DeriveError::MissingLayout {
                    type_path: type_path(ctx, id),
                    what: "field offset",
                });
                break;
            }
        };
        let native_offset = match to_u32(ctx, id, "field offset", offset64) {
            Ok(o) => o,
            Err(e) => {
                result = Err(e);
                break;
            }
        };
        let child = if field.attrs.blob {
            if in_key {
                result = Err(DeriveError::BlobUnderOrderedKey {
                    type_path: type_path(ctx, id),
                    field: name,
                });
                break;
            }
            blob_slot(ctx, field.type_id)
        } else {
            derive_type(ctx, field.type_id, in_key)
        };
        match child {
            Ok(child) => members.push(Member {
                name,
                declaration_index: i as u32,
                native_offset,
                child,
            }),
            Err(e) => {
                result = Err(e);
                break;
            }
        }
    }
    ctx.path.pop();
    result?;

    // Physical order: (native offset ascending, declaration index ascending).
    members.sort_by_key(|m| (m.native_offset, m.declaration_index));

    let member_diverges = members.iter().any(|m| {
        m.child.wire_size != m.child.native_size || m.child.wire_align != m.child.native_align
    });
    let repack = force_repack || skip_present || member_diverges;

    let mut any_child_diverged = false;
    let mut fields = Vec::with_capacity(members.len());
    let (wire_size, wire_align);
    if !repack {
        for mut m in members {
            any_child_diverged |= m.child.diverged;
            set_offset(&mut m.child.node, m.native_offset);
            fields.push(WireField {
                name: m.name,
                declaration_index: m.declaration_index,
                node: m.child.node,
            });
        }
        wire_size = native_size;
        wire_align = native_align;
    } else {
        let mut cursor: u32 = 0;
        let mut max_align: u32 = 1;
        let mut geometry_moved = false;
        for mut m in members {
            any_child_diverged |= m.child.diverged;
            let a = m.child.wire_align.max(1);
            max_align = max_align.max(a);
            let off = align_up(cursor, a).ok_or_else(|| DeriveError::Bounds {
                type_path: type_path(ctx, id),
                what: "packed offset",
                value: cursor as u64,
            })?;
            cursor = off
                .checked_add(m.child.wire_size)
                .ok_or_else(|| DeriveError::Bounds {
                    type_path: type_path(ctx, id),
                    what: "packed size",
                    value: off as u64 + m.child.wire_size as u64,
                })?;
            geometry_moved |= off != m.native_offset;
            set_offset(&mut m.child.node, off);
            fields.push(WireField {
                name: m.name,
                declaration_index: m.declaration_index,
                node: m.child.node,
            });
        }
        wire_align = max_align;
        wire_size = align_up(cursor, wire_align).ok_or_else(|| DeriveError::Bounds {
            type_path: type_path(ctx, id),
            what: "packed size",
            value: cursor as u64,
        })?;
        any_child_diverged |= geometry_moved
            || wire_size != native_size
            || wire_align != native_align
            || skip_present;
    }

    Ok(Derived {
        node: WireNode::Struct {
            offset: 0,
            size: wire_size,
            align: wire_align,
            fields,
        },
        native_size,
        native_align,
        wire_size,
        wire_align,
        diverged: any_child_diverged,
    })
}

fn derive_enum(ctx: &mut Ctx<'_>, id: SchemaTypeId, in_key: bool) -> Result<Derived, DeriveError> {
    let (native_size, native_align) = measured(ctx, id)?;
    let view_ty = ctx.view.ty(id).expect("checked by measured");
    let tag = view_ty
        .tag_encoding()
        .cloned()
        .ok_or_else(|| DeriveError::MissingTagEncoding {
            type_path: type_path(ctx, id),
        })?;

    // Collect (name, variant type id) in declaration order.
    let def = &ctx.view.schema.types[id.0];
    let mut variants: Vec<(String, SchemaTypeId)> = Vec::with_capacity(def.fields.len());
    for field in &def.fields {
        match &field.id {
            FieldIdentifier::Variant(name) => variants.push((name.clone(), field.type_id)),
            other => {
                return Err(DeriveError::MalformedEnum {
                    type_path: type_path(ctx, id),
                    detail: format!("non-variant field {other:?} in an enum"),
                })
            }
        }
    }
    for (_, vid) in &variants {
        if !matches!(classify(ctx.view.schema, *vid), Class::EnumVariant) {
            return Err(DeriveError::MalformedEnum {
                type_path: type_path(ctx, id),
                detail: format!(
                    "variant type {} is not an enum variant",
                    type_path(ctx, *vid)
                ),
            });
        }
    }

    ctx.path.push(id);
    let derived = derive_enum_inner(ctx, id, in_key, native_size, native_align, &tag, &variants);
    ctx.path.pop();
    derived
}

#[allow(clippy::too_many_arguments)]
fn derive_enum_inner(
    ctx: &mut Ctx<'_>,
    id: SchemaTypeId,
    in_key: bool,
    native_size: u32,
    native_align: u32,
    tag: &TagEncoding,
    variants: &[(String, SchemaTypeId)],
) -> Result<Derived, DeriveError> {
    match tag {
        TagEncoding::Single { variant_index } => {
            if variants.len() != 1 {
                return Err(DeriveError::MalformedEnum {
                    type_path: type_path(ctx, id),
                    detail: format!(
                        "single-tag enum with {} listed variants (uninhabited variants unsupported)",
                        variants.len()
                    ),
                });
            }
            if *variant_index != 0 {
                return Err(DeriveError::MalformedEnum {
                    type_path: type_path(ctx, id),
                    detail: format!("single-tag variant index {variant_index} out of range"),
                });
            }
            let (name, vid) = &variants[0];
            let payload = derive_record(ctx, *vid, in_key, false)?;
            let diverged = payload.diverged
                || payload.wire_size != native_size
                || payload.wire_align != native_align;
            Ok(Derived {
                node: WireNode::Enum {
                    offset: 0,
                    size: payload.wire_size,
                    align: payload.wire_align,
                    form: WireEnumForm::Single,
                    variants: vec![WireVariant {
                        name: name.clone(),
                        discriminant: 0,
                        node: payload.node,
                    }],
                },
                native_size,
                native_align,
                wire_size: payload.wire_size,
                wire_align: payload.wire_align,
                diverged,
            })
        }
        TagEncoding::Direct {
            tag_size,
            tag_offset,
            variant_values,
        } => {
            if variant_values.len() != variants.len() {
                return Err(DeriveError::MalformedEnum {
                    type_path: type_path(ctx, id),
                    detail: format!(
                        "{} variants but {} discriminant values",
                        variants.len(),
                        variant_values.len()
                    ),
                });
            }
            let tag_size_u8 = u8::try_from(*tag_size)
                .ok()
                .filter(|s| *s <= 16)
                .ok_or_else(|| DeriveError::MalformedEnum {
                    type_path: type_path(ctx, id),
                    detail: format!("tag size {tag_size} out of range"),
                })?;
            let tag_offset = to_u32(ctx, id, "tag offset", *tag_offset)?;

            // Native-style payload derivation: fully flat iff nothing in
            // any payload image diverges.
            let mut payloads = Vec::with_capacity(variants.len());
            for (_, vid) in variants {
                payloads.push(derive_record(ctx, *vid, in_key, false)?);
            }
            if payloads.iter().all(|p| !p.diverged) {
                let wire_variants = variants
                    .iter()
                    .zip(payloads)
                    .zip(variant_values)
                    .map(|(((name, _), payload), value)| WireVariant {
                        name: name.clone(),
                        discriminant: *value,
                        node: payload.node,
                    })
                    .collect();
                Ok(Derived {
                    node: WireNode::Enum {
                        offset: 0,
                        size: native_size,
                        align: native_align,
                        form: WireEnumForm::FullyFlat {
                            tag_offset,
                            tag_size: tag_size_u8,
                        },
                        variants: wire_variants,
                    },
                    native_size,
                    native_align,
                    wire_size: native_size,
                    wire_align: native_align,
                    diverged: false,
                })
            } else {
                derive_canonical(ctx, id, in_key, native_size, native_align, variants)
            }
        }
        TagEncoding::Niche { .. } => {
            derive_canonical(ctx, id, in_key, native_size, native_align, variants)
        }
    }
}

/// The canonical tagged form: `tag: u32` at wire offset 0 holding the
/// variant's index in name-sorted order; payload union at
/// `align_up(4, max variant wire align)`; wire alignment
/// `max(4, max variant wire align)`; wire size = payload offset + max
/// variant wire size, rounded up to alignment. Payloads are repacked
/// from 0 — their native offsets were laid out around a native tag that
/// no longer exists.
fn derive_canonical(
    ctx: &mut Ctx<'_>,
    id: SchemaTypeId,
    in_key: bool,
    native_size: u32,
    native_align: u32,
    variants: &[(String, SchemaTypeId)],
) -> Result<Derived, DeriveError> {
    let mut payloads = Vec::with_capacity(variants.len());
    let mut max_align: u32 = 1;
    let mut max_size: u32 = 0;
    for (_, vid) in variants {
        let payload = derive_record(ctx, *vid, in_key, true)?;
        max_align = max_align.max(payload.wire_align);
        max_size = max_size.max(payload.wire_size);
        payloads.push(payload);
    }
    let payload_offset = align_up(4, max_align).ok_or_else(|| DeriveError::Bounds {
        type_path: type_path(ctx, id),
        what: "payload offset",
        value: max_align as u64,
    })?;
    let wire_align = max_align.max(4);
    let end = payload_offset
        .checked_add(max_size)
        .ok_or_else(|| DeriveError::Bounds {
            type_path: type_path(ctx, id),
            what: "enum size",
            value: payload_offset as u64 + max_size as u64,
        })?;
    let wire_size = align_up(end, wire_align).ok_or_else(|| DeriveError::Bounds {
        type_path: type_path(ctx, id),
        what: "enum size",
        value: end as u64,
    })?;
    let wire_variants = variants
        .iter()
        .zip(payloads)
        .map(|((name, _), mut payload)| {
            set_offset(&mut payload.node, payload_offset);
            WireVariant {
                name: name.clone(),
                discriminant: 0,
                node: payload.node,
            }
        })
        .collect();
    Ok(Derived {
        node: WireNode::Enum {
            offset: 0,
            size: wire_size,
            align: wire_align,
            form: WireEnumForm::Canonical,
            variants: wire_variants,
        },
        native_size,
        native_align,
        wire_size,
        wire_align,
        // The tag moved: the image always differs from native.
        diverged: true,
    })
}
