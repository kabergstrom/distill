//! Owned native-layout projection from the shared `ngp-schema` model.
//!
//! Compiled descriptors expose static `NativeLayoutNode` trees. A daemon
//! watching a schema artifact needs an owned equivalent so rejected reloads do
//! not leak generated statics. `dsnl` serializes both forms with one grammar.

use ngp_schema::classify::{classify, Class};
use ngp_schema::node::PrimitiveKind;
use ngp_schema::{FieldIdentifier, LayoutView, SchemaTypeId, TagEncoding, FIXED_STATE_UUID};

use crate::native::ScalarKind;

pub const MAX_MEASURED_DEPTH: u32 = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeasuredLayoutError {
    MissingLayout {
        type_path: String,
        what: &'static str,
    },
    Unsupported {
        type_path: String,
        reason: String,
    },
    MalformedEnum {
        type_path: String,
        detail: String,
    },
    NondeterministicHasher {
        type_path: String,
    },
    Bounds {
        type_path: String,
        what: &'static str,
        value: u64,
    },
    DepthExceeded {
        limit: u32,
    },
}

impl std::fmt::Display for MeasuredLayoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingLayout { type_path, what } => {
                write!(f, "{type_path}: measured layout missing {what}")
            }
            Self::Unsupported { type_path, reason } => {
                write!(f, "{type_path}: not serializable: {reason}")
            }
            Self::MalformedEnum { type_path, detail } => {
                write!(f, "{type_path}: malformed enum: {detail}")
            }
            Self::NondeterministicHasher { type_path } => write!(
                f,
                "{type_path}: hashed container without the fixed-seed BuildHasher"
            ),
            Self::Bounds {
                type_path,
                what,
                value,
            } => {
                write!(f, "{type_path}: {what} {value} exceeds the DSNL bound")
            }
            Self::DepthExceeded { limit } => {
                write!(
                    f,
                    "type nesting exceeds the measured-layout depth cap {limit}"
                )
            }
        }
    }
}

impl std::error::Error for MeasuredLayoutError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeasuredNativeNode {
    Scalar {
        offset: u32,
        size: u32,
        align: u32,
        kind: ScalarKind,
    },
    Struct {
        offset: u32,
        size: u32,
        align: u32,
        fields: Vec<MeasuredNativeField>,
    },
    Enum {
        offset: u32,
        size: u32,
        align: u32,
        tag: MeasuredNativeTagEncoding,
        variants: Vec<MeasuredNativeVariant>,
    },
    Array {
        offset: u32,
        size: u32,
        align: u32,
        len: u32,
        stride: u32,
        elem: Box<MeasuredNativeNode>,
    },
    Vec {
        offset: u32,
        size: u32,
        align: u32,
        elem: Box<MeasuredNativeNode>,
    },
    Set {
        offset: u32,
        size: u32,
        align: u32,
        elem: Box<MeasuredNativeNode>,
    },
    Map {
        offset: u32,
        size: u32,
        align: u32,
        key: Box<MeasuredNativeNode>,
        value: Box<MeasuredNativeNode>,
    },
    BoxPtr {
        offset: u32,
        size: u32,
        align: u32,
        inner: Box<MeasuredNativeNode>,
    },
    ArcPtr {
        offset: u32,
        size: u32,
        align: u32,
        inner: Box<MeasuredNativeNode>,
    },
    Str {
        offset: u32,
        size: u32,
        align: u32,
    },
    Blob {
        offset: u32,
        size: u32,
        align: u32,
    },
    Skip {
        offset: u32,
        size: u32,
        align: u32,
    },
    BackRef {
        distance: u32,
        offset: u32,
    },
    Unit {
        offset: u32,
    },
}

impl MeasuredNativeNode {
    pub fn offset(&self) -> u32 {
        match self {
            Self::Scalar { offset, .. }
            | Self::Struct { offset, .. }
            | Self::Enum { offset, .. }
            | Self::Array { offset, .. }
            | Self::Vec { offset, .. }
            | Self::Set { offset, .. }
            | Self::Map { offset, .. }
            | Self::BoxPtr { offset, .. }
            | Self::ArcPtr { offset, .. }
            | Self::Str { offset, .. }
            | Self::Blob { offset, .. }
            | Self::Skip { offset, .. }
            | Self::BackRef { offset, .. }
            | Self::Unit { offset } => *offset,
        }
    }

    fn set_offset(&mut self, value: u32) {
        match self {
            Self::Scalar { offset, .. }
            | Self::Struct { offset, .. }
            | Self::Enum { offset, .. }
            | Self::Array { offset, .. }
            | Self::Vec { offset, .. }
            | Self::Set { offset, .. }
            | Self::Map { offset, .. }
            | Self::BoxPtr { offset, .. }
            | Self::ArcPtr { offset, .. }
            | Self::Str { offset, .. }
            | Self::Blob { offset, .. }
            | Self::Skip { offset, .. }
            | Self::BackRef { offset, .. }
            | Self::Unit { offset } => *offset = value,
        }
    }

    pub fn size(&self) -> u32 {
        match self {
            Self::Scalar { size, .. }
            | Self::Struct { size, .. }
            | Self::Enum { size, .. }
            | Self::Array { size, .. }
            | Self::Vec { size, .. }
            | Self::Set { size, .. }
            | Self::Map { size, .. }
            | Self::BoxPtr { size, .. }
            | Self::ArcPtr { size, .. }
            | Self::Str { size, .. }
            | Self::Blob { size, .. }
            | Self::Skip { size, .. } => *size,
            Self::BackRef { .. } | Self::Unit { .. } => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasuredNativeField {
    pub name: String,
    pub declaration_index: u32,
    pub node: MeasuredNativeNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasuredNativeVariant {
    pub name: String,
    pub declaration_index: u32,
    pub node: MeasuredNativeNode,
    pub tag: MeasuredNativeVariantTag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasuredNativeVariantTag {
    Direct { value: u128 },
    Niche { index: u32 },
    Untagged,
    Single,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasuredNativeTagEncoding {
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

pub fn derive_measured_native(
    view: LayoutView<'_>,
    root: SchemaTypeId,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    derive_type(
        &mut Context {
            view,
            frames: Vec::new(),
            depth: 0,
        },
        root,
    )
}

struct Context<'a> {
    view: LayoutView<'a>,
    frames: Vec<SchemaTypeId>,
    depth: u32,
}

fn type_path(context: &Context<'_>, id: SchemaTypeId) -> String {
    context
        .view
        .schema
        .types
        .get(id.0)
        .map(|ty| ty.path.display_path())
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| format!("type #{}", id.0))
}

fn bounded(
    context: &Context<'_>,
    id: SchemaTypeId,
    what: &'static str,
    value: u64,
) -> Result<u32, MeasuredLayoutError> {
    u32::try_from(value).map_err(|_| MeasuredLayoutError::Bounds {
        type_path: type_path(context, id),
        what,
        value,
    })
}

fn measured(context: &Context<'_>, id: SchemaTypeId) -> Result<(u32, u32), MeasuredLayoutError> {
    let ty = context
        .view
        .ty(id)
        .ok_or_else(|| MeasuredLayoutError::Unsupported {
            type_path: type_path(context, id),
            reason: "dangling type id".to_owned(),
        })?;
    if !ty.layout_complete() {
        return Err(MeasuredLayoutError::MissingLayout {
            type_path: type_path(context, id),
            what: "complete layout",
        });
    }
    let size = ty
        .size()
        .ok_or_else(|| MeasuredLayoutError::MissingLayout {
            type_path: type_path(context, id),
            what: "size",
        })?;
    let align = ty
        .align()
        .ok_or_else(|| MeasuredLayoutError::MissingLayout {
            type_path: type_path(context, id),
            what: "align",
        })?;
    let (size, align) = (
        bounded(context, id, "size", size)?,
        bounded(context, id, "align", align)?,
    );
    if align == 0 || !align.is_power_of_two() {
        return Err(MeasuredLayoutError::Unsupported {
            type_path: type_path(context, id),
            reason: format!("invalid alignment {align}"),
        });
    }
    Ok((size, align))
}

fn scalar_kind(kind: PrimitiveKind) -> ScalarKind {
    match kind {
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

fn derive_type(
    context: &mut Context<'_>,
    id: SchemaTypeId,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    if context.depth >= MAX_MEASURED_DEPTH {
        return Err(MeasuredLayoutError::DepthExceeded {
            limit: MAX_MEASURED_DEPTH,
        });
    }
    context.depth += 1;
    let result = derive_type_inner(context, id);
    context.depth -= 1;
    result
}

fn derive_type_inner(
    context: &mut Context<'_>,
    id: SchemaTypeId,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    match classify(context.view.schema, id) {
        Class::Primitive(kind) => {
            let (size, align) = measured(context, id)?;
            Ok(MeasuredNativeNode::Scalar {
                offset: 0,
                size,
                align,
                kind: scalar_kind(kind),
            })
        }
        Class::Unit => {
            let (size, align) = measured(context, id)?;
            if (size, align) != (0, 1) {
                return Err(MeasuredLayoutError::Unsupported {
                    type_path: type_path(context, id),
                    reason: format!("unit measured as size {size}, align {align}"),
                });
            }
            Ok(MeasuredNativeNode::Unit { offset: 0 })
        }
        Class::StringNode => leaf(context, id, |size, align| MeasuredNativeNode::Str {
            offset: 0,
            size,
            align,
        }),
        Class::Vec(child) => one(context, id, child, |size, align, elem| {
            MeasuredNativeNode::Vec {
                offset: 0,
                size,
                align,
                elem: Box::new(elem),
            }
        }),
        Class::Set {
            elem,
            hasher,
            hashed,
        } => {
            check_hasher(context, id, hasher, hashed)?;
            one(context, id, elem, |size, align, elem| {
                MeasuredNativeNode::Set {
                    offset: 0,
                    size,
                    align,
                    elem: Box::new(elem),
                }
            })
        }
        Class::Map {
            key,
            value,
            hasher,
            hashed,
        } => {
            check_hasher(context, id, hasher, hashed)?;
            let (size, align) = measured(context, id)?;
            Ok(MeasuredNativeNode::Map {
                offset: 0,
                size,
                align,
                key: Box::new(derive_pointee(context, key)?),
                value: Box::new(derive_pointee(context, value)?),
            })
        }
        Class::Boxed(child) => one(context, id, child, |size, align, inner| {
            MeasuredNativeNode::BoxPtr {
                offset: 0,
                size,
                align,
                inner: Box::new(inner),
            }
        }),
        Class::ArcOf(child) => one(context, id, child, |size, align, inner| {
            MeasuredNativeNode::ArcPtr {
                offset: 0,
                size,
                align,
                inner: Box::new(inner),
            }
        }),
        Class::Array { len, elem: elem_id } => {
            let (size, align) = measured(context, id)?;
            let (elem_size, elem_align) = measured(context, elem_id)?;
            let stride =
                align_up(elem_size, elem_align).ok_or_else(|| MeasuredLayoutError::Bounds {
                    type_path: type_path(context, id),
                    what: "array stride",
                    value: elem_size as u64,
                })?;
            Ok(MeasuredNativeNode::Array {
                offset: 0,
                size,
                align,
                len: bounded(context, id, "array length", len)?,
                stride,
                elem: Box::new(derive_pointee(context, elem_id)?),
            })
        }
        Class::AssetRef(_) | Class::WeakRef(_) => {
            let (size, align) = measured(context, id)?;
            if (size, align) != (16, 1) {
                return Err(MeasuredLayoutError::Unsupported {
                    type_path: type_path(context, id),
                    reason: format!("asset reference measured as size {size}, align {align}"),
                });
            }
            Ok(MeasuredNativeNode::Array {
                offset: 0,
                size: 16,
                align: 1,
                len: 16,
                stride: 1,
                elem: Box::new(MeasuredNativeNode::Scalar {
                    offset: 0,
                    size: 1,
                    align: 1,
                    kind: ScalarKind::U8,
                }),
            })
        }
        Class::Struct | Class::Tuple => derive_record(context, id),
        Class::Option(_) | Class::Enum => derive_enum(context, id),
        Class::EnumVariant => unsupported(context, id, "enum variant outside its enum"),
        Class::FixedState => {
            unsupported(context, id, "fixed-seed BuildHasher is not an asset value")
        }
        Class::SlotMap { .. } => unsupported(context, id, "engine SlotMap container"),
        Class::Opaque(reason) => unsupported(context, id, &format!("{reason:?}")),
        Class::Malformed(reason) => {
            unsupported(context, id, &format!("malformed schema: {reason:?}"))
        }
    }
}

fn unsupported<T>(
    context: &Context<'_>,
    id: SchemaTypeId,
    reason: &str,
) -> Result<T, MeasuredLayoutError> {
    Err(MeasuredLayoutError::Unsupported {
        type_path: type_path(context, id),
        reason: reason.to_owned(),
    })
}

fn leaf<F>(
    context: &Context<'_>,
    id: SchemaTypeId,
    make: F,
) -> Result<MeasuredNativeNode, MeasuredLayoutError>
where
    F: FnOnce(u32, u32) -> MeasuredNativeNode,
{
    let (size, align) = measured(context, id)?;
    Ok(make(size, align))
}

fn one<F>(
    context: &mut Context<'_>,
    id: SchemaTypeId,
    child: SchemaTypeId,
    make: F,
) -> Result<MeasuredNativeNode, MeasuredLayoutError>
where
    F: FnOnce(u32, u32, MeasuredNativeNode) -> MeasuredNativeNode,
{
    let (size, align) = measured(context, id)?;
    let child = derive_pointee(context, child)?;
    Ok(make(size, align, child))
}

fn derive_pointee(
    context: &mut Context<'_>,
    id: SchemaTypeId,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    if let Some(index) = context
        .frames
        .iter()
        .rposition(|candidate| *candidate == id)
    {
        return Ok(MeasuredNativeNode::BackRef {
            distance: (context.frames.len() - 1 - index) as u32,
            offset: 0,
        });
    }
    derive_type(context, id)
}

fn check_hasher(
    context: &Context<'_>,
    id: SchemaTypeId,
    hasher: Option<SchemaTypeId>,
    hashed: bool,
) -> Result<(), MeasuredLayoutError> {
    if !hashed
        || hasher
            .and_then(|id| context.view.schema.types.get(id.0))
            .and_then(|ty| ty.uuid)
            == Some(FIXED_STATE_UUID)
    {
        Ok(())
    } else {
        Err(MeasuredLayoutError::NondeterministicHasher {
            type_path: type_path(context, id),
        })
    }
}

fn field_name(id: &FieldIdentifier) -> Option<String> {
    match id {
        FieldIdentifier::Name(name) => Some(name.clone()),
        FieldIdentifier::Number(index) => Some(index.to_string()),
        FieldIdentifier::Variant(_) => None,
    }
}

fn derive_record(
    context: &mut Context<'_>,
    id: SchemaTypeId,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    let (size, align) = measured(context, id)?;
    context.frames.push(id);
    let result = record_fields(context, id, 0, size, align);
    context.frames.pop();
    result
}

fn record_fields(
    context: &mut Context<'_>,
    id: SchemaTypeId,
    frame_offset: u32,
    size: u32,
    align: u32,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    let definitions = context.view.schema.types[id.0].fields.clone();
    let mut fields = Vec::with_capacity(definitions.len());
    for (index, field) in definitions.into_iter().enumerate() {
        let name = field_name(&field.id).ok_or_else(|| MeasuredLayoutError::Unsupported {
            type_path: type_path(context, id),
            reason: "variant record found in a struct".to_owned(),
        })?;
        let field_view = context
            .view
            .ty(id)
            .and_then(|ty| ty.field(index))
            .expect("schema field exists");
        let offset = bounded(
            context,
            id,
            "field offset",
            field_view
                .offset()
                .ok_or_else(|| MeasuredLayoutError::MissingLayout {
                    type_path: type_path(context, id),
                    what: "field offset",
                })?,
        )?;
        let mut node = field_node(context, id, index, &field)?;
        node.set_offset(offset);
        fields.push(MeasuredNativeField {
            name,
            declaration_index: index as u32,
            node,
        });
    }
    Ok(MeasuredNativeNode::Struct {
        offset: frame_offset,
        size,
        align,
        fields,
    })
}

fn field_node(
    context: &mut Context<'_>,
    owner: SchemaTypeId,
    index: usize,
    field: &ngp_schema::Field,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    if field.attrs.skip {
        let size = context
            .view
            .ty(owner)
            .and_then(|ty| ty.field(index))
            .and_then(|field| field.field_size())
            .ok_or_else(|| MeasuredLayoutError::MissingLayout {
                type_path: type_path(context, owner),
                what: "skip field size",
            })?;
        let (_, align) = measured(context, field.type_id)?;
        Ok(MeasuredNativeNode::Skip {
            offset: 0,
            size: bounded(context, owner, "skip field size", size)?,
            align,
        })
    } else if field.attrs.blob {
        let (size, align) = measured(context, field.type_id)?;
        Ok(MeasuredNativeNode::Blob {
            offset: 0,
            size,
            align,
        })
    } else {
        derive_type(context, field.type_id)
    }
}

fn derive_enum(
    context: &mut Context<'_>,
    id: SchemaTypeId,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    let (size, align) = measured(context, id)?;
    let tag = context
        .view
        .ty(id)
        .expect("measured type exists")
        .tag_encoding()
        .cloned()
        .ok_or_else(|| MeasuredLayoutError::MissingLayout {
            type_path: type_path(context, id),
            what: "enum tag encoding",
        })?;
    let definitions = context.view.schema.types[id.0].fields.clone();
    context.frames.push(id);
    let result = (|| {
        let mut variants = Vec::with_capacity(definitions.len());
        for (index, field) in definitions.iter().enumerate() {
            let FieldIdentifier::Variant(name) = &field.id else {
                return malformed(context, id, format!("non-variant field {:?}", field.id));
            };
            if !matches!(
                classify(context.view.schema, field.type_id),
                Class::EnumVariant
            ) {
                return malformed(context, id, format!("{name} payload is not an EnumVariant"));
            }
            let payload_align = variant_align(context, field.type_id)?;
            let payload_offset = match &tag {
                TagEncoding::Direct { tag_size, .. } => {
                    align_up(bounded(context, id, "tag size", *tag_size)?, payload_align)
                        .ok_or_else(|| MeasuredLayoutError::Bounds {
                            type_path: type_path(context, id),
                            what: "variant payload offset",
                            value: *tag_size,
                        })?
                }
                TagEncoding::Niche { .. } | TagEncoding::Single { .. } => 0,
            };
            variants.push(MeasuredNativeVariant {
                name: name.clone(),
                declaration_index: index as u32,
                node: derive_variant(context, field.type_id, payload_offset, payload_align)?,
                tag: variant_tag(context, id, &tag, index, definitions.len())?,
            });
        }
        Ok(MeasuredNativeNode::Enum {
            offset: 0,
            size,
            align,
            tag: native_tag(context, id, &tag, definitions.len())?,
            variants,
        })
    })();
    context.frames.pop();
    result
}

fn variant_align(context: &Context<'_>, id: SchemaTypeId) -> Result<u32, MeasuredLayoutError> {
    context.view.schema.types[id.0]
        .fields
        .iter()
        .try_fold(1, |align, field| {
            measured(context, field.type_id).map(|(_, field_align)| align.max(field_align))
        })
}

fn derive_variant(
    context: &mut Context<'_>,
    id: SchemaTypeId,
    payload_offset: u32,
    payload_align: u32,
) -> Result<MeasuredNativeNode, MeasuredLayoutError> {
    context.frames.push(id);
    let result = (|| {
        let definitions = context.view.schema.types[id.0].fields.clone();
        let raw_offsets = definitions
            .iter()
            .enumerate()
            .map(|(index, _)| {
                context
                    .view
                    .ty(id)
                    .and_then(|ty| ty.field(index))
                    .and_then(|field| field.offset())
            })
            .collect::<Vec<_>>();
        let absolute = payload_offset != 0
            && raw_offsets
                .iter()
                .flatten()
                .copied()
                .min()
                .is_some_and(|minimum| minimum >= u64::from(payload_offset));
        let mut fields = Vec::with_capacity(definitions.len());
        let mut end = 0u32;
        for (index, field) in definitions.into_iter().enumerate() {
            let name = field_name(&field.id).ok_or_else(|| MeasuredLayoutError::MalformedEnum {
                type_path: type_path(context, id),
                detail: "nested variant record".to_owned(),
            })?;
            let raw = bounded(
                context,
                id,
                "variant field offset",
                raw_offsets[index].ok_or_else(|| MeasuredLayoutError::MissingLayout {
                    type_path: type_path(context, id),
                    what: "variant field offset",
                })?,
            )?;
            let offset = if absolute {
                raw.checked_sub(payload_offset).ok_or_else(|| {
                    MeasuredLayoutError::MalformedEnum {
                        type_path: type_path(context, id),
                        detail: "variant field precedes payload".to_owned(),
                    }
                })?
            } else {
                raw
            };
            let mut node = field_node(context, id, index, &field)?;
            let field_size = context
                .view
                .ty(id)
                .and_then(|ty| ty.field(index))
                .and_then(|field| field.field_size())
                .map(|value| bounded(context, id, "variant field size", value))
                .transpose()?
                .unwrap_or(node.size());
            end = end.max(offset.checked_add(field_size).ok_or_else(|| {
                MeasuredLayoutError::Bounds {
                    type_path: type_path(context, id),
                    what: "variant payload size",
                    value: u64::from(offset) + u64::from(field_size),
                }
            })?);
            node.set_offset(offset);
            fields.push(MeasuredNativeField {
                name,
                declaration_index: index as u32,
                node,
            });
        }
        let size = align_up(end, payload_align).ok_or_else(|| MeasuredLayoutError::Bounds {
            type_path: type_path(context, id),
            what: "variant payload size",
            value: end as u64,
        })?;
        Ok(MeasuredNativeNode::Struct {
            offset: payload_offset,
            size,
            align: payload_align,
            fields,
        })
    })();
    context.frames.pop();
    result
}

fn native_tag(
    context: &Context<'_>,
    id: SchemaTypeId,
    tag: &TagEncoding,
    variants: usize,
) -> Result<MeasuredNativeTagEncoding, MeasuredLayoutError> {
    match tag {
        TagEncoding::Direct {
            tag_size,
            tag_offset,
            variant_values,
        } => {
            if variant_values.len() != variants {
                return malformed(
                    context,
                    id,
                    "direct discriminant count differs from variants".to_owned(),
                );
            }
            let size = u8::try_from(*tag_size)
                .ok()
                .filter(|size| (1..=16).contains(size))
                .ok_or_else(|| MeasuredLayoutError::MalformedEnum {
                    type_path: type_path(context, id),
                    detail: format!("direct tag size {tag_size} is outside 1..=16"),
                })?;
            Ok(MeasuredNativeTagEncoding::Direct {
                offset: bounded(context, id, "tag offset", *tag_offset)?,
                size,
            })
        }
        TagEncoding::Niche {
            niche_field_offset,
            niche_field_size,
            niche_start,
            ..
        } => {
            let size = u8::try_from(*niche_field_size)
                .ok()
                .filter(|size| (1..=16).contains(size))
                .ok_or_else(|| MeasuredLayoutError::MalformedEnum {
                    type_path: type_path(context, id),
                    detail: format!("niche tag size {niche_field_size} is outside 1..=16"),
                })?;
            Ok(MeasuredNativeTagEncoding::Niche {
                offset: bounded(context, id, "niche offset", *niche_field_offset)?,
                size,
                niche_start: *niche_start,
            })
        }
        TagEncoding::Single { variant_index } if variants == 1 && *variant_index == 0 => {
            Ok(MeasuredNativeTagEncoding::Single)
        }
        TagEncoding::Single { .. } => malformed(
            context,
            id,
            "single tag does not name the sole variant".to_owned(),
        ),
    }
}

fn variant_tag(
    context: &Context<'_>,
    id: SchemaTypeId,
    tag: &TagEncoding,
    index: usize,
    variants: usize,
) -> Result<MeasuredNativeVariantTag, MeasuredLayoutError> {
    match tag {
        TagEncoding::Direct { variant_values, .. } => variant_values
            .get(index)
            .copied()
            .map(|value| MeasuredNativeVariantTag::Direct { value })
            .ok_or_else(|| MeasuredLayoutError::MalformedEnum {
                type_path: type_path(context, id),
                detail: "missing direct discriminant".to_owned(),
            }),
        TagEncoding::Niche {
            untagged_variant,
            niche_variants_start,
            niche_variants_end,
            ..
        } => {
            if *untagged_variant >= variants
                || *niche_variants_start > *niche_variants_end
                || *niche_variants_end >= variants
            {
                return malformed(context, id, "niche variant range is invalid".to_owned());
            }
            if index == *untagged_variant {
                Ok(MeasuredNativeVariantTag::Untagged)
            } else if (*niche_variants_start..=*niche_variants_end).contains(&index) {
                Ok(MeasuredNativeVariantTag::Niche {
                    index: u32::try_from(index - *niche_variants_start).map_err(|_| {
                        MeasuredLayoutError::Bounds {
                            type_path: type_path(context, id),
                            what: "niche variant index",
                            value: index as u64,
                        }
                    })?,
                })
            } else {
                malformed(
                    context,
                    id,
                    "variant is neither untagged nor niche encoded".to_owned(),
                )
            }
        }
        TagEncoding::Single { .. } => Ok(MeasuredNativeVariantTag::Single),
    }
}

fn malformed<T>(
    context: &Context<'_>,
    id: SchemaTypeId,
    detail: String,
) -> Result<T, MeasuredLayoutError> {
    Err(MeasuredLayoutError::MalformedEnum {
        type_path: type_path(context, id),
        detail,
    })
}

fn align_up(value: u32, align: u32) -> Option<u32> {
    let remainder = value % align;
    if remainder == 0 {
        Some(value)
    } else {
        value.checked_add(align - remainder)
    }
}
