//! The asset reflection interpreter (newgameplus GAP 23 §18.2): every
//! asset type's runtime descriptor, logical schema, `Value` reader and
//! migration defaults, built once per crate table from the walked schema
//! ([`super::table::AssetTable::schema`]) and the table's compiled thunks.
//!
//! Physical and logical forms are read separately, as the schema model
//! splits them: the native layout follows `ngp_schema::classify` (what a
//! type is in memory), the logical schema `ngp_schema::project` (what it
//! is to Distill). They differ only for the framework types, which both
//! recognize by attributed UUID:
//! - `DynamicPatch`: physically its one `Vec<u8>` (the value encoding),
//!   logically `SchemaNode::Dynamic { sparse: true }`;
//! - `AssetRef<T>`/`WeakAssetRef<T>`: physically 16 uuid bytes (a `[u8;
//!   16]` native node), logically `AssetRef`/`WeakRef` naming `T`'s uuid;
//! - the fixed-seed hasher state: a zero-sized `BuildHasher`, logically
//!   nothing (it only licenses its hash container);
//! - `#[asset(blob)]` fields (`Blob`): an `Arc`-backed byte range,
//!   logically a `Blob` leaf, whatever the field's type.

use std::collections::HashMap;
use std::sync::OnceLock;

use distill_core::id::{LogicalHash, TypeUuid};
use distill_wire::exec::Blob;
use distill_wire::native::{
    validate_native_descriptor, CallbackPanic, CtorEntry, CtorId, CtorTable, DropId, DropTable,
    NativeField, NativeLayoutNode, NativeTagEncoding, NativeVariant, NativeVariantTag, ScalarKind,
    SkipDefaultId, SkipEntry, SkipWriterTable,
};
use ngp_schema::classify::{classify, Class};
use ngp_schema::node::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use ngp_schema::{AssetRefValue, FieldIdentifier, Schema, SchemaTypeId, TagEncoding, Value};

use super::table::{AssetTable, ContainerOps, ContainerRead, DefaultThunk, DropThunk, TypeEntry};
use crate::AssetRuntimeDescriptor;

/// A table's descriptors, built on first use.
pub struct Registry {
    built: OnceLock<Built>,
}

impl Registry {
    pub const fn new() -> Registry {
        Registry { built: OnceLock::new() }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

struct Built {
    schema: &'static Schema,
    /// Parallel to `AssetTable::types`: `Some` for asset types.
    by_index: Vec<Option<&'static TypeInfo>>,
    by_uuid: HashMap<TypeUuid, &'static TypeInfo>,
}

/// Everything the interpreter knows about one asset type.
pub struct TypeInfo {
    pub descriptor: AssetRuntimeDescriptor,
    /// The type in its table's schema ([`AssetTable::schema`]).
    pub id: SchemaTypeId,
    pub reflect: TypeReflect,
}

/// The logical side of one asset type: its schema, its `Value` reader and
/// its migration defaults.
pub struct TypeReflect {
    pub logical: LogicalSchema,
    plan: &'static Plan,
    root: ReadId,
    defaults: Vec<DefaultContainer>,
}

impl AssetTable {
    fn built(&'static self) -> &'static Built {
        self.registry.built.get_or_init(|| build(self))
    }

    /// The descriptor of the asset type at `index` in `types`.
    pub fn descriptor(&'static self, index: usize) -> &'static AssetRuntimeDescriptor {
        &self.info(index).descriptor
    }

    pub fn info(&'static self, index: usize) -> &'static TypeInfo {
        self.built().by_index[index]
            .unwrap_or_else(|| panic!("asset table entry {} is not an asset type", self.types[index].key))
    }

    /// The asset type with `uuid`, if this table declares it.
    pub fn by_uuid(&'static self, uuid: TypeUuid) -> Option<&'static TypeInfo> {
        self.built().by_uuid.get(&uuid).copied()
    }

    /// The table's schema, decoded.
    pub fn schema(&'static self) -> &'static Schema {
        self.built().schema
    }

    /// Every asset type of this table.
    pub fn asset_types(&'static self) -> impl Iterator<Item = &'static TypeInfo> {
        self.built().by_index.iter().flatten().copied()
    }
}

impl TypeReflect {
    /// The value at `ptr` in the typed model (`ngp_schema::Value`): struct
    /// fields and enum variants in schema (name-sorted) order, map and set
    /// entries in the container's iteration order (writers canonicalize).
    ///
    /// # Safety
    /// `ptr` must point to a live value of this type.
    pub unsafe fn to_value(&self, ptr: *const u8) -> Value {
        self.plan.read(self.root, ptr)
    }

    /// The default of field `at.last()` of the struct (or enum variant
    /// payload) at `at[..len - 1]` below `to_schema`: the field type's
    /// `Default`, from the first type of this asset's closure whose logical
    /// node is that container. `None` when no such type or default exists.
    pub fn field_default(&self, to_schema: &SchemaNode, at: &[String]) -> Option<Result<Value, CallbackPanic>> {
        let (field, node) = container_at(to_schema, at)?;
        self.defaults.iter().filter(|c| c.schema == *node).find_map(|c| {
            let (_, default) = c.fields.iter().find(|(name, _)| name == field)?;
            Some(self.plan.default_value(default))
        })
    }

    /// The logical node of each default container (struct, enum variant
    /// payload) of the closure, in expansion order.
    pub fn default_containers(&self) -> impl Iterator<Item = &SchemaNode> {
        self.defaults.iter().map(|c| &c.schema)
    }

    /// The same field of its container's own `Default`.
    pub fn parent_default(&self, to_schema: &SchemaNode, at: &[String]) -> Option<Result<Value, CallbackPanic>> {
        let (field, node) = container_at(to_schema, at)?;
        self.defaults.iter().filter(|c| c.schema == *node).find_map(|c| {
            match self.plan.default_value(c.own.as_ref()?) {
                Ok(value) => value.field(&c.schema, field).cloned().map(Ok),
                Err(e) => Some(Err(e)),
            }
        })
    }
}

fn container_at<'a>(to_schema: &'a SchemaNode, at: &'a [String]) -> Option<(&'a str, &'a SchemaNode)> {
    let (field, parent) = at.split_last()?;
    let mut node = to_schema;
    for segment in parent {
        let SchemaNode::Struct { fields, .. } = node else {
            return None;
        };
        node = fields.iter().find(|(name, _, _)| name == segment).map(|(_, _, child)| child)?;
    }
    Some((field.as_str(), node))
}

// --- Building ---

/// The table's entries by schema type id.
struct Ops<'s> {
    schema: &'s Schema,
    view: ngp_schema::LayoutView<'s>,
    types: Vec<Option<&'static TypeEntry>>,
    containers: Vec<Option<ContainerOps>>,
    skips: Vec<Option<SkipEntry>>,
}

fn build(table: &'static AssetTable) -> Built {
    let schema: &'static Schema = Box::leak(Box::new(
        Schema::from_file_bytes(table.schema).unwrap_or_else(|e| panic!("asset table schema: {e}")),
    ));
    let view = schema.single_layout().expect("an asset table schema has one layout table");
    let mut by_key: HashMap<String, SchemaTypeId> = HashMap::new();
    for ty in &schema.types {
        by_key.entry(schema.instance_key(ty.id)).or_insert(ty.id);
    }
    let id_of = |key: &str| {
        *by_key.get(key).unwrap_or_else(|| panic!("asset table names {key}, which its schema lacks"))
    };
    let n = schema.types.len();
    let mut ops = Ops {
        schema,
        view,
        types: vec![None; n],
        containers: (0..n).map(|_| None).collect(),
        skips: (0..n).map(|_| None).collect(),
    };
    let mut index_ids = Vec::with_capacity(table.types.len());
    for entry in table.types {
        let id = id_of(entry.key);
        check_layout(&ops, id, entry.size, entry.align, entry.key);
        ops.types[id.0] = Some(entry);
        index_ids.push(id);
    }
    for entry in table.containers {
        let id = id_of(entry.key);
        let container = (entry.ops)();
        check_layout(&ops, id, container.size, container.align, entry.key);
        ops.containers[id.0] = Some(container);
    }
    for entry in table.skips {
        ops.skips[id_of(entry.key).0] = Some((entry.entry)());
    }
    let plan: &'static Plan = Box::leak(Box::new(Plan::new(&ops)));
    let mut by_index = Vec::with_capacity(table.types.len());
    let mut by_uuid = HashMap::new();
    for (entry, &id) in table.types.iter().zip(&index_ids) {
        if entry.finalize.is_none() {
            by_index.push(None);
            continue;
        }
        let info: &'static TypeInfo = Box::leak(Box::new(type_info(&ops, plan, id, entry)));
        by_uuid.insert(info.descriptor.type_uuid, info);
        by_index.push(Some(info));
    }
    Built { schema, by_index, by_uuid }
}

/// The compiled layout must be the schema's: the generated file asserts
/// it at compile time, so this holds for any table that built.
fn check_layout(ops: &Ops, id: SchemaTypeId, size: usize, align: usize, key: &str) {
    let ty = ops.view.ty(id).expect("table type in schema");
    assert!(
        ty.size() == Some(size as u64) && ty.align() == Some(align as u64),
        "asset table: {key} compiled as size {size} align {align}, schema says {:?} {:?}",
        ty.size(),
        ty.align()
    );
}

fn type_info(ops: &Ops, plan: &'static Plan, id: SchemaTypeId, entry: &TypeEntry) -> TypeInfo {
    let def = &ops.schema.types[id.0];
    let type_uuid = def.uuid.unwrap_or_else(|| panic!("asset type {} has no uuid", entry.key));
    let logical = ngp_schema::project(ops.schema, id).unwrap_or_else(|e| panic!("{}: {e}", entry.key));
    let logical_hash: LogicalHash = node_hash(&logical.root).unwrap_or_else(|e| panic!("{}: {e:?}", entry.key));

    let mut layout = LayoutBuild::new(ops);
    let root = layout.node(id, 0);
    let root: &'static NativeLayoutNode = Box::leak(Box::new(root));
    let ctors: &'static CtorTable = Box::leak(Box::new(CtorTable { entries: Box::leak(layout.ctors.into_boxed_slice()) }));
    let drops: &'static DropTable = Box::leak(Box::new(DropTable { entries: Box::leak(layout.drops.into_boxed_slice()) }));
    let skips: &'static SkipWriterTable =
        Box::leak(Box::new(SkipWriterTable { entries: Box::leak(layout.skips.into_boxed_slice()) }));
    validate_native_descriptor(
        root,
        u32(entry.size),
        u32(entry.align),
        ctors.entries.len(),
        drops.entries.len(),
        skips.entries.len(),
    )
    .unwrap_or_else(|e| panic!("{}: invalid native descriptor: {e:?}", entry.key));

    let descriptor = AssetRuntimeDescriptor {
        type_uuid,
        logical_hash,
        build_only: def.attrs.build_only,
        native_layout: root,
        size: entry.size,
        align: entry.align,
        ctors,
        drops,
        skip_writers: skips,
        finalize: entry.finalize.expect("asset type finalize thunk"),
        encode: unsupported_encode,
    };
    let reflect = TypeReflect { logical, plan, root: plan.ids[id.0].expect("asset type read plan"), defaults: defaults(ops, plan, id) };
    TypeInfo { descriptor, id, reflect }
}

unsafe fn unsupported_encode(_: *const u8, _: &mut dyn crate::EncodeSink) -> Result<(), CallbackPanic> {
    Err(CallbackPanic)
}

fn u32(value: impl TryInto<u32>) -> u32 {
    value.try_into().unwrap_or_else(|_| panic!("asset native layout exceeds u32"))
}

fn size_align(ops: &Ops, id: SchemaTypeId) -> (u32, u32) {
    let ty = ops.view.ty(id).expect("type in schema");
    match (ty.size(), ty.align()) {
        (Some(size), Some(align)) => (u32(size), u32(align)),
        _ => panic!("asset table schema: {} has no layout", ops.schema.types[id.0].path.display_path()),
    }
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

fn field_name(id: &FieldIdentifier) -> String {
    match id {
        FieldIdentifier::Name(name) | FieldIdentifier::Variant(name) => name.clone(),
        FieldIdentifier::Number(n) => n.to_string(),
    }
}

fn leak_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// The physical class of `id`: `DynamicPatch` is its byte vector
/// (`classify` already sees through it).
fn physical(ops: &Ops, id: SchemaTypeId) -> Class {
    classify(ops.schema, id)
}

fn is_dynamic_patch(ops: &Ops, id: SchemaTypeId) -> bool {
    ops.schema.types[id.0].uuid == Some(ngp_schema::DYNAMIC_PATCH_UUID)
}

/// One asset type's native layout tree and callback tables (§12), in the
/// order and with the ids a per-type walk assigns: drops when a struct or
/// enum opens (if it needs drop), constructors after their elements, skip
/// writers per skipped field; one id per type.
struct LayoutBuild<'o, 's> {
    ops: &'o Ops<'s>,
    ctors: Vec<CtorEntry>,
    ctor_ids: HashMap<SchemaTypeId, CtorId>,
    drops: Vec<DropThunk>,
    drop_ids: HashMap<SchemaTypeId, DropId>,
    skips: Vec<SkipEntry>,
    /// Struct, enum and `Option` types on the expansion path; `None` for
    /// enum variant payloads (frames of their own, never re-entered).
    frames: Vec<Option<SchemaTypeId>>,
}

static BYTE: NativeLayoutNode = NativeLayoutNode::Scalar { offset: 0, size: 1, align: 1, kind: ScalarKind::U8 };

impl<'o, 's> LayoutBuild<'o, 's> {
    fn new(ops: &'o Ops<'s>) -> Self {
        LayoutBuild {
            ops,
            ctors: Vec::new(),
            ctor_ids: HashMap::new(),
            drops: Vec::new(),
            drop_ids: HashMap::new(),
            skips: Vec::new(),
            frames: Vec::new(),
        }
    }

    fn backref(&self, id: SchemaTypeId, offset: u32) -> Option<NativeLayoutNode> {
        self.frames.iter().rposition(|f| *f == Some(id)).map(|index| NativeLayoutNode::BackRef {
            distance: (self.frames.len() - 1 - index) as u32,
            offset,
        })
    }

    fn ctor(&mut self, id: SchemaTypeId) -> CtorId {
        if let Some(ctor) = self.ctor_ids.get(&id) {
            return *ctor;
        }
        let ops = self.ops.containers[id.0].as_ref().unwrap_or_else(|| {
            panic!("asset table lacks container {}", self.ops.schema.instance_key(id))
        });
        let ctor = CtorId(u32(self.ctors.len()));
        self.ctors.push(ops.ctor);
        self.ctor_ids.insert(id, ctor);
        ctor
    }

    fn drop(&mut self, id: SchemaTypeId) -> Option<DropId> {
        let thunk = self.type_entry(id).drop?;
        if let Some(drop) = self.drop_ids.get(&id) {
            return Some(*drop);
        }
        let drop = DropId(u32(self.drops.len()));
        self.drops.push(thunk);
        self.drop_ids.insert(id, drop);
        Some(drop)
    }

    fn type_entry(&self, id: SchemaTypeId) -> &'static TypeEntry {
        self.ops.types[id.0].unwrap_or_else(|| panic!("asset table lacks type {}", self.ops.schema.instance_key(id)))
    }

    fn leak(node: NativeLayoutNode) -> &'static NativeLayoutNode {
        Box::leak(Box::new(node))
    }

    fn node(&mut self, id: SchemaTypeId, offset: u32) -> NativeLayoutNode {
        let (size, align) = size_align(self.ops, id);
        match physical(self.ops, id) {
            Class::Primitive(kind) => NativeLayoutNode::Scalar { offset, size, align, kind: scalar_kind(kind) },
            Class::Unit => NativeLayoutNode::Unit { offset },
            Class::StringNode => NativeLayoutNode::Str { offset, size, align },
            Class::Vec(elem) => {
                let elem = Self::leak(self.node(elem, 0));
                // `DynamicPatch` is its `Vec<u8>`: that vector's constructor.
                let ctor_type = if is_dynamic_patch(self.ops, id) { self.ops.schema.types[id.0].fields[0].type_id } else { id };
                let ctor = self.ctor(ctor_type);
                NativeLayoutNode::Vec { offset, size, align, elem, ctor }
            }
            Class::Array { len, elem } => {
                let (stride, _) = size_align(self.ops, elem);
                let elem = Self::leak(self.node(elem, 0));
                NativeLayoutNode::Array { offset, size, align, len: u32(len), stride, elem }
            }
            Class::Option(inner) => {
                if let Some(backref) = self.backref(id, offset) {
                    return backref;
                }
                self.frames.push(Some(id));
                let inner = Self::leak(self.node(inner, 0));
                let ctor = self.ctor(id);
                self.frames.pop();
                NativeLayoutNode::Option { offset, size, align, inner, ctor }
            }
            Class::Boxed(inner) => {
                let inner = Self::leak(self.node(inner, 0));
                let ctor = self.ctor(id);
                NativeLayoutNode::BoxPtr { offset, size, align, inner, ctor }
            }
            Class::ArcOf(inner) => {
                let inner = Self::leak(self.node(inner, 0));
                let ctor = self.ctor(id);
                NativeLayoutNode::ArcPtr { offset, size, align, inner, ctor }
            }
            Class::Map { key, value, .. } => {
                let key = Self::leak(self.node(key, 0));
                let value = Self::leak(self.node(value, 0));
                let ctor = self.ctor(id);
                NativeLayoutNode::Map { offset, size, align, key, value, ctor }
            }
            Class::Set { elem, .. } => {
                let elem = Self::leak(self.node(elem, 0));
                let ctor = self.ctor(id);
                NativeLayoutNode::Set { offset, size, align, elem, ctor }
            }
            // 16 unrestricted uuid bytes: a fixed array keeps their exact
            // native and wire geometry.
            Class::AssetRef(_) | Class::WeakRef(_) => {
                NativeLayoutNode::Array { offset, size: 16, align: 1, len: 16, stride: 1, elem: &BYTE }
            }
            Class::Struct | Class::Tuple => {
                if let Some(backref) = self.backref(id, offset) {
                    return backref;
                }
                self.frames.push(Some(id));
                let whole_drop = self.drop(id);
                let def = &self.ops.schema.types[id.0];
                let ty = self.ops.view.ty(id).expect("type layout");
                let mut fields = Vec::with_capacity(def.fields.len());
                for (index, field) in def.fields.iter().enumerate() {
                    let at = ty.field(index).and_then(|f| f.offset()).unwrap_or_else(|| {
                        panic!("asset table schema: {}.{} has no offset", def.path.display_path(), field_name(&field.id))
                    });
                    fields.push(NativeField {
                        name: leak_str(field_name(&field.id)),
                        declaration_index: index as u32,
                        node: self.field(field, u32(at)),
                    });
                }
                self.frames.pop();
                NativeLayoutNode::Struct { offset, size, align, whole_drop, fields: sorted(fields) }
            }
            Class::Enum => {
                if let Some(backref) = self.backref(id, offset) {
                    return backref;
                }
                self.frames.push(Some(id));
                let whole_drop = self.drop(id);
                let def = &self.ops.schema.types[id.0];
                let ty = self.ops.view.ty(id).expect("type layout");
                let single = def.fields.len() == 1;
                let (tag, values, tag_size) = match ty.tag_encoding() {
                    Some(TagEncoding::Direct { tag_size, tag_offset, variant_values }) if !single => (
                        NativeTagEncoding::Direct { offset: u32(*tag_offset), size: *tag_size as u8 },
                        variant_values.clone(),
                        u32(*tag_size),
                    ),
                    _ if single => (NativeTagEncoding::Single, Vec::new(), 0),
                    other => panic!("asset enum {} has no direct tag ({other:?})", def.path.display_path()),
                };
                let mut variants = Vec::with_capacity(def.fields.len());
                for (index, variant) in def.fields.iter().enumerate() {
                    let base = ty.field(index).and_then(|f| f.offset()).unwrap_or(0);
                    let vdef = &self.ops.schema.types[variant.type_id.0];
                    let vty = self.ops.view.ty(variant.type_id).expect("variant layout");
                    self.frames.push(None);
                    let mut cursor = tag_size;
                    let mut payload_align = 1u32;
                    let mut fields = Vec::with_capacity(vdef.fields.len());
                    for (findex, field) in vdef.fields.iter().enumerate() {
                        let at = base
                            + vty.field(findex).and_then(|f| f.offset()).unwrap_or_else(|| {
                                panic!("asset table schema: {} variant field has no offset", def.path.display_path())
                            });
                        let (fsize, falign) = size_align(self.ops, field.type_id);
                        cursor = cursor.max(u32(at) + fsize);
                        payload_align = payload_align.max(falign);
                        fields.push(NativeField {
                            name: leak_str(field_name(&field.id)),
                            declaration_index: findex as u32,
                            node: self.field(field, u32(at)),
                        });
                    }
                    self.frames.pop();
                    let node = Self::leak(NativeLayoutNode::Struct {
                        offset: 0,
                        size: crate::build::align_up(cursor, payload_align),
                        align: payload_align,
                        whole_drop: None,
                        fields: sorted(fields),
                    });
                    variants.push(NativeVariant {
                        name: leak_str(field_name(&variant.id)),
                        declaration_index: index as u32,
                        node,
                        tag: if single { NativeVariantTag::Single } else { NativeVariantTag::Direct { value: values[index] } },
                    });
                }
                self.frames.pop();
                NativeLayoutNode::Enum {
                    offset,
                    size,
                    align,
                    tag,
                    whole_drop,
                    variants: Box::leak(variants.into_boxed_slice()),
                }
            }
            other => panic!(
                "asset table schema: {} is not serializable ({other:?})",
                self.ops.schema.types[id.0].path.display_path()
            ),
        }
    }

    fn field(&mut self, field: &ngp_schema::Field, offset: u32) -> NativeLayoutNode {
        let (size, align) = size_align(self.ops, field.type_id);
        if field.attrs.skip {
            let entry = self.ops.skips[field.type_id.0].as_ref().unwrap_or_else(|| {
                panic!("asset table lacks the skip writer of {}", self.ops.schema.instance_key(field.type_id))
            });
            let writer = SkipDefaultId(u32(self.skips.len()));
            self.skips.push(SkipEntry { write: entry.write, drop_in_place: entry.drop_in_place });
            NativeLayoutNode::Skip { offset, size, align, writer }
        } else if field.attrs.blob {
            NativeLayoutNode::Blob { offset, size, align }
        } else {
            self.node(field.type_id, offset)
        }
    }
}

fn sorted(mut fields: Vec<NativeField>) -> &'static [NativeField] {
    fields.sort_by_key(|field| (field.node.offset(), field.declaration_index));
    Box::leak(fields.into_boxed_slice())
}

// --- Reading values ---

type ReadId = usize;

/// How to read one schema type as a `Value`, by type, shared by every
/// asset type of a table. Recursive types refer back by id.
enum Read {
    /// Filled in once the type's children are planned (recursion).
    Pending,
    Scalar(PrimitiveKind),
    Unit,
    String,
    Blob,
    AssetRef { weak: bool },
    Dynamic,
    /// Non-skipped fields in schema (name-sorted) order.
    Struct(Box<[(usize, ReadId)]>),
    Enum { tag_offset: usize, tag_size: usize, variants: Box<[ReadVariant]> },
    Vec { view: unsafe fn(*const u8) -> (*const u8, usize), stride: usize, elem: ReadId },
    Array { len: usize, stride: usize, elem: ReadId },
    Option { some: unsafe fn(*const u8) -> *const u8, inner: ReadId },
    Deref { deref: unsafe fn(*const u8) -> *const u8, inner: ReadId },
    Set { each: unsafe fn(*const u8, &mut dyn FnMut(*const u8)), elem: ReadId },
    Map { each: unsafe fn(*const u8, &mut dyn FnMut(*const u8, *const u8)), key: ReadId, value: ReadId },
}

struct ReadVariant {
    /// Raw tag bits (`None`: the single variant).
    tag: Option<u128>,
    /// The variant's index in schema (name-sorted) order.
    index: u32,
    fields: Box<[(usize, ReadId)]>,
}

/// A default the interpreter can produce: a known std default, or a
/// type's `Default` read back as a value.
enum DefaultValue {
    Value(Value),
    Thunk { write: DefaultThunk, drop: Option<DropThunk>, size: usize, align: usize, read: ReadId },
}

struct DefaultContainer {
    /// The container type's own logical node (an enum's: one variant's
    /// payload).
    schema: SchemaNode,
    own: Option<DefaultValue>,
    fields: Vec<(String, DefaultValue)>,
}

struct Plan {
    reads: Vec<Read>,
    /// The read of each schema type, once planned.
    ids: Vec<Option<ReadId>>,
    blob: ReadId,
}

impl Plan {
    fn new(ops: &Ops) -> Plan {
        let mut plan = Plan { reads: vec![Read::Blob], ids: vec![None; ops.schema.types.len()], blob: 0 };
        for (index, entry) in ops.types.iter().enumerate() {
            if entry.is_some_and(|e| e.finalize.is_some()) {
                plan.plan(ops, SchemaTypeId(index));
            }
        }
        plan
    }

    fn push(&mut self, read: Read) -> ReadId {
        self.reads.push(read);
        self.reads.len() - 1
    }

    fn plan(&mut self, ops: &Ops, id: SchemaTypeId) -> ReadId {
        if let Some(read) = self.ids[id.0] {
            return read;
        }
        let at = self.push(Read::Pending);
        self.ids[id.0] = Some(at);
        let stride = |id: SchemaTypeId| size_align(ops, id).0 as usize;
        let container = |id: SchemaTypeId| {
            ops.containers[id.0]
                .as_ref()
                .unwrap_or_else(|| panic!("asset table lacks container {}", ops.schema.instance_key(id)))
                .read
        };
        let read = if is_dynamic_patch(ops, id) {
            Read::Dynamic
        } else {
            match classify(ops.schema, id) {
                Class::Primitive(kind) => Read::Scalar(kind),
                Class::Unit => Read::Unit,
                Class::StringNode => Read::String,
                Class::AssetRef(_) => Read::AssetRef { weak: false },
                Class::WeakRef(_) => Read::AssetRef { weak: true },
                Class::Vec(elem) => {
                    let ContainerRead::Vec(view) = container(id) else { panic!("Vec read thunk") };
                    Read::Vec { view, stride: stride(elem), elem: self.plan(ops, elem) }
                }
                Class::Array { len, elem } => Read::Array { len: len as usize, stride: stride(elem), elem: self.plan(ops, elem) },
                Class::Option(inner) => {
                    let ContainerRead::Option(some) = container(id) else { panic!("Option read thunk") };
                    Read::Option { some, inner: self.plan(ops, inner) }
                }
                Class::Boxed(inner) | Class::ArcOf(inner) => {
                    let ContainerRead::Deref(deref) = container(id) else { panic!("Box/Arc read thunk") };
                    Read::Deref { deref, inner: self.plan(ops, inner) }
                }
                Class::Set { elem, .. } => {
                    let ContainerRead::Set(each) = container(id) else { panic!("set read thunk") };
                    Read::Set { each, elem: self.plan(ops, elem) }
                }
                Class::Map { key, value, .. } => {
                    let ContainerRead::Map(each) = container(id) else { panic!("map read thunk") };
                    Read::Map { each, key: self.plan(ops, key), value: self.plan(ops, value) }
                }
                Class::Struct | Class::Tuple => Read::Struct(self.fields(ops, id, 0)),
                Class::Enum => {
                    let def = &ops.schema.types[id.0];
                    let ty = ops.view.ty(id).expect("enum layout");
                    let (tag_offset, tag_size, values) = match ty.tag_encoding() {
                        Some(TagEncoding::Direct { tag_size, tag_offset, variant_values }) if def.fields.len() > 1 => {
                            (*tag_offset as usize, *tag_size as usize, Some(variant_values.clone()))
                        }
                        _ => (0, 0, None),
                    };
                    let mut names: Vec<(String, usize)> =
                        def.fields.iter().enumerate().map(|(i, f)| (nfc(&field_name(&f.id)), i)).collect();
                    names.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                    let mut variants = Vec::with_capacity(names.len());
                    for (sorted_index, (_, decl)) in names.into_iter().enumerate() {
                        let variant = &def.fields[decl];
                        let base = ty.field(decl).and_then(|f| f.offset()).unwrap_or(0) as usize;
                        variants.push(ReadVariant {
                            tag: values.as_ref().map(|v| v[decl]),
                            index: sorted_index as u32,
                            fields: self.fields(ops, variant.type_id, base),
                        });
                    }
                    Read::Enum { tag_offset, tag_size, variants: variants.into_boxed_slice() }
                }
                other => panic!("asset table schema: {} is not serializable ({other:?})", ops.schema.types[id.0].path.display_path()),
            }
        };
        self.reads[at] = read;
        at
    }

    /// A record's non-skipped fields, name-sorted, at `base` + their offsets.
    fn fields(&mut self, ops: &Ops, id: SchemaTypeId, base: usize) -> Box<[(usize, ReadId)]> {
        let def = &ops.schema.types[id.0];
        let ty = ops.view.ty(id).expect("record layout");
        let mut fields: Vec<(String, usize, ReadId)> = Vec::with_capacity(def.fields.len());
        for (index, field) in def.fields.iter().enumerate() {
            if field.attrs.skip {
                continue;
            }
            let offset = base + ty.field(index).and_then(|f| f.offset()).expect("field offset") as usize;
            let read = if field.attrs.blob { self.blob } else { self.plan(ops, field.type_id) };
            fields.push((nfc(&field_name(&field.id)), offset, read));
        }
        fields.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        fields.into_iter().map(|(_, offset, read)| (offset, read)).collect()
    }

    unsafe fn read(&self, id: ReadId, p: *const u8) -> Value {
        match &self.reads[id] {
            Read::Pending => unreachable!("read plan completed at build"),
            Read::Scalar(kind) => match kind {
                PrimitiveKind::Bool => Value::Bool(*p.cast::<bool>()),
                PrimitiveKind::Char => Value::Char(*p.cast::<char>()),
                PrimitiveKind::U8 => Value::U8(*p),
                PrimitiveKind::U16 => Value::U16(p.cast::<u16>().read_unaligned()),
                PrimitiveKind::U32 => Value::U32(p.cast::<u32>().read_unaligned()),
                PrimitiveKind::U64 => Value::U64(p.cast::<u64>().read_unaligned()),
                PrimitiveKind::U128 => Value::U128(p.cast::<u128>().read_unaligned()),
                PrimitiveKind::I8 => Value::I8(*p.cast::<i8>()),
                PrimitiveKind::I16 => Value::I16(p.cast::<i16>().read_unaligned()),
                PrimitiveKind::I32 => Value::I32(p.cast::<i32>().read_unaligned()),
                PrimitiveKind::I64 => Value::I64(p.cast::<i64>().read_unaligned()),
                PrimitiveKind::I128 => Value::I128(p.cast::<i128>().read_unaligned()),
                PrimitiveKind::F32 => Value::F32(p.cast::<f32>().read_unaligned()),
                PrimitiveKind::F64 => Value::F64(p.cast::<f64>().read_unaligned()),
            },
            Read::Unit => Value::Unit,
            Read::String => Value::String((*p.cast::<String>()).clone()),
            Read::Blob => Value::Blob((*p.cast::<Blob>()).as_bytes().to_vec().into()),
            Read::AssetRef { weak } => {
                let uuid = distill_core::id::AssetUuid(p.cast::<[u8; 16]>().read_unaligned());
                let query = AssetRefValue::Path(uuid.to_string());
                if *weak {
                    Value::WeakRef(query)
                } else {
                    Value::AssetRef(query)
                }
            }
            Read::Dynamic => panic!(
                "a DynamicPatch has no value without its bundle's type closure; build the value as ngp_schema::Value"
            ),
            Read::Struct(fields) => Value::Struct(self.record(fields, p)),
            Read::Enum { tag_offset, tag_size, variants } => {
                let variant = if *tag_size == 0 {
                    &variants[0]
                } else {
                    let mut bits = [0u8; 16];
                    std::ptr::copy_nonoverlapping(p.add(*tag_offset), bits.as_mut_ptr(), *tag_size);
                    let tag = u128::from_le_bytes(bits);
                    variants.iter().find(|v| v.tag == Some(tag)).expect("an enum value holds one of its variants' tags")
                };
                Value::Enum { variant: variant.index, payload: self.record(&variant.fields, p) }
            }
            Read::Vec { view, stride, elem } => {
                let (data, len) = view(p);
                Value::Seq((0..len).map(|i| self.read(*elem, data.add(i * stride))).collect())
            }
            Read::Array { len, stride, elem } => Value::Seq((0..*len).map(|i| self.read(*elem, p.add(i * stride))).collect()),
            Read::Option { some, inner } => {
                let value = some(p);
                Value::Option((!value.is_null()).then(|| Box::new(self.read(*inner, value))))
            }
            Read::Deref { deref, inner } => self.read(*inner, deref(p)),
            Read::Set { each, elem } => {
                let mut out = Vec::new();
                each(p, &mut |v| out.push(self.read(*elem, v)));
                Value::Seq(out)
            }
            Read::Map { each, key, value } => {
                let mut out = Vec::new();
                each(p, &mut |k, v| out.push((self.read(*key, k), self.read(*value, v))));
                Value::Map(out)
            }
        }
    }

    unsafe fn record(&self, fields: &[(usize, ReadId)], p: *const u8) -> Box<[Value]> {
        fields.iter().map(|&(offset, read)| self.read(read, p.add(offset))).collect()
    }

    fn default_value(&self, default: &DefaultValue) -> Result<Value, CallbackPanic> {
        match default {
            DefaultValue::Value(value) => Ok(value.clone()),
            DefaultValue::Thunk { write, drop, size, align, read } => {
                let layout = std::alloc::Layout::from_size_align(*size, *align).expect("type layout");
                let buffer = if layout.size() == 0 {
                    std::ptr::without_provenance_mut::<u8>(layout.align())
                } else {
                    // Safety: non-zero size.
                    unsafe { std::alloc::alloc(layout) }
                };
                assert!(!buffer.is_null(), "default buffer allocation failed");
                let free = |buffer: *mut u8| {
                    if layout.size() != 0 {
                        // Safety: allocated above with `layout`.
                        unsafe { std::alloc::dealloc(buffer, layout) }
                    }
                };
                // Safety: an aligned, uninitialized slot of the type.
                if let Err(e) = unsafe { write(buffer) } {
                    free(buffer);
                    return Err(e);
                }
                let value = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // Safety: `write` initialized it.
                    unsafe { self.read(*read, buffer) }
                }));
                let dropped = match drop {
                    // Safety: initialized, dropped once.
                    Some(drop) => unsafe { drop(buffer) },
                    None => Ok(()),
                };
                // A failed drop leaked the value's contents; its slot is ours.
                free(buffer);
                let value = value.map_err(|_| CallbackPanic)?;
                dropped.map(|()| value)
            }
        }
    }
}

fn nfc(s: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    s.nfc().collect()
}

// --- Defaults ---

/// The struct and enum types of `root`'s closure in first-expansion order
/// (fields name-sorted, variants and their fields in declaration order,
/// container elements, map keys before values), each with its logical
/// node and its fields' defaults: what a migration adding a field fills it
/// with (`field_default`, `parent_default`).
fn defaults(ops: &Ops, plan: &Plan, root: SchemaTypeId) -> Vec<DefaultContainer> {
    let mut seen = vec![false; ops.schema.types.len()];
    let mut out = Vec::new();
    visit_defaults(ops, plan, root, &mut seen, &mut out);
    out
}

fn visit_defaults(ops: &Ops, plan: &Plan, id: SchemaTypeId, seen: &mut [bool], out: &mut Vec<DefaultContainer>) {
    if std::mem::replace(&mut seen[id.0], true) || is_dynamic_patch(ops, id) {
        return;
    }
    let def = &ops.schema.types[id.0];
    match classify(ops.schema, id) {
        Class::Struct | Class::Tuple => {
            let logical = ngp_schema::project(ops.schema, id).expect("asset closure projects").root;
            let mut fields: Vec<&ngp_schema::Field> = def.fields.iter().filter(|f| !f.attrs.skip).collect();
            fields.sort_by(|a, b| nfc(&field_name(&a.id)).as_bytes().cmp(nfc(&field_name(&b.id)).as_bytes()));
            out.push(DefaultContainer {
                schema: logical,
                own: type_default(ops, plan, id),
                fields: field_defaults(ops, plan, &fields),
            });
            for field in fields {
                if !field.attrs.blob {
                    visit_defaults(ops, plan, field.type_id, seen, out);
                }
            }
        }
        Class::Enum => {
            let logical = ngp_schema::project(ops.schema, id).expect("asset closure projects").root;
            let SchemaNode::Enum { variants, .. } = logical else { unreachable!("an enum projects to an enum") };
            for variant in &def.fields {
                let name = nfc(&field_name(&variant.id));
                let payload = variants.iter().find(|(n, _, _)| *n == name).map(|(_, _, p)| p.clone()).expect("variant");
                let fields: Vec<&ngp_schema::Field> =
                    ops.schema.types[variant.type_id.0].fields.iter().filter(|f| !f.attrs.skip).collect();
                out.push(DefaultContainer { schema: payload, own: None, fields: field_defaults(ops, plan, &fields) });
            }
            for variant in &def.fields {
                for field in ops.schema.types[variant.type_id.0].fields.iter().filter(|f| !f.attrs.skip) {
                    if !field.attrs.blob {
                        visit_defaults(ops, plan, field.type_id, seen, out);
                    }
                }
            }
        }
        Class::Vec(elem) | Class::Array { elem, .. } | Class::Option(elem) | Class::Set { elem, .. } => {
            visit_defaults(ops, plan, elem, seen, out)
        }
        Class::Boxed(inner) | Class::ArcOf(inner) => visit_defaults(ops, plan, inner, seen, out),
        Class::Map { key, value, .. } => {
            visit_defaults(ops, plan, key, seen, out);
            visit_defaults(ops, plan, value, seen, out);
        }
        _ => {}
    }
}

fn field_defaults(ops: &Ops, plan: &Plan, fields: &[&ngp_schema::Field]) -> Vec<(String, DefaultValue)> {
    fields
        .iter()
        .filter(|f| !f.attrs.blob)
        .filter_map(|f| Some((nfc(&field_name(&f.id)), type_default(ops, plan, f.type_id)?)))
        .collect()
}

/// A type's `Default` as a value: std's known defaults, a struct's or
/// enum's `Default` impl; `None` for types without one the reflection
/// offers (arrays, `Box`, `Arc`, references, blobs, dynamic patches).
fn type_default(ops: &Ops, plan: &Plan, id: SchemaTypeId) -> Option<DefaultValue> {
    if is_dynamic_patch(ops, id) {
        return None;
    }
    let value = match classify(ops.schema, id) {
        Class::Primitive(kind) => match kind {
            PrimitiveKind::Bool => Value::Bool(false),
            PrimitiveKind::Char => Value::Char('\0'),
            PrimitiveKind::U8 => Value::U8(0),
            PrimitiveKind::U16 => Value::U16(0),
            PrimitiveKind::U32 => Value::U32(0),
            PrimitiveKind::U64 => Value::U64(0),
            PrimitiveKind::U128 => Value::U128(0),
            PrimitiveKind::I8 => Value::I8(0),
            PrimitiveKind::I16 => Value::I16(0),
            PrimitiveKind::I32 => Value::I32(0),
            PrimitiveKind::I64 => Value::I64(0),
            PrimitiveKind::I128 => Value::I128(0),
            PrimitiveKind::F32 => Value::F32(0.0),
            PrimitiveKind::F64 => Value::F64(0.0),
        },
        Class::Unit => Value::Unit,
        Class::StringNode => Value::String(String::new()),
        Class::Vec(_) | Class::Set { .. } => Value::Seq(Vec::new()),
        Class::Option(_) => Value::Option(None),
        Class::Map { .. } => Value::Map(Vec::new()),
        Class::Struct | Class::Tuple | Class::Enum => {
            let entry = ops.types[id.0]?;
            return Some(DefaultValue::Thunk {
                write: entry.default?,
                drop: entry.drop,
                size: entry.size,
                align: entry.align,
                read: plan.ids[id.0].expect("closure types are planned"),
            });
        }
        _ => return None,
    };
    Some(DefaultValue::Value(value))
}
