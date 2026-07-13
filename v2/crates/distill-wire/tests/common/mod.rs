//! Shared test scaffolding: leak helpers and native-tree builders (the
//! trees #[asset] would generate as statics; tests leak instead).
#![allow(dead_code)]

use distill_wire::native::{
    CtorId, DropId, NativeField, NativeLayoutNode, NativeTagEncoding, NativeVariant,
    NativeVariantTag, ScalarKind, SkipDefaultId,
};

pub fn leak<T>(v: T) -> &'static T {
    Box::leak(Box::new(v))
}

pub fn leak_slice<T>(v: Vec<T>) -> &'static [T] {
    Box::leak(v.into_boxed_slice())
}

pub fn leak_str(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

pub fn scalar(offset: u32, kind: ScalarKind) -> NativeLayoutNode {
    let (size, align) = scalar_geometry(kind);
    NativeLayoutNode::Scalar {
        offset,
        size,
        align,
        kind,
    }
}

pub fn scalar_geometry(kind: ScalarKind) -> (u32, u32) {
    match kind {
        ScalarKind::Bool | ScalarKind::U8 | ScalarKind::I8 => (1, 1),
        ScalarKind::U16 | ScalarKind::I16 => (2, 2),
        ScalarKind::Char | ScalarKind::U32 | ScalarKind::I32 | ScalarKind::F32 => (4, 4),
        ScalarKind::U64 | ScalarKind::I64 | ScalarKind::F64 => (8, 8),
        ScalarKind::U128 | ScalarKind::I128 => (16, 16),
    }
}

pub fn nfield(name: &str, declaration_index: u32, node: NativeLayoutNode) -> NativeField {
    NativeField {
        name: leak_str(name),
        declaration_index,
        node,
    }
}

pub fn nstruct(offset: u32, size: u32, align: u32, fields: Vec<NativeField>) -> NativeLayoutNode {
    NativeLayoutNode::Struct {
        offset,
        size,
        align,
        whole_drop: None,
        fields: leak_slice(fields),
    }
}

pub fn nenum(
    offset: u32,
    size: u32,
    align: u32,
    tag: NativeTagEncoding,
    variants: Vec<NativeVariant>,
) -> NativeLayoutNode {
    NativeLayoutNode::Enum {
        offset,
        size,
        align,
        tag,
        whole_drop: None,
        variants: leak_slice(variants),
    }
}

pub fn nvariant(
    name: &str,
    declaration_index: u32,
    tag: NativeVariantTag,
    node: NativeLayoutNode,
) -> NativeVariant {
    NativeVariant {
        name: leak_str(name),
        declaration_index,
        node: leak(node),
        tag,
    }
}

/// A struct with whole-value drop glue (`whole_drop: Some`).
pub fn nstruct_glue(
    offset: u32,
    size: u32,
    align: u32,
    whole_drop: u32,
    fields: Vec<NativeField>,
) -> NativeLayoutNode {
    NativeLayoutNode::Struct {
        offset,
        size,
        align,
        whole_drop: Some(DropId(whole_drop)),
        fields: leak_slice(fields),
    }
}

/// An enum with whole-value drop glue (`whole_drop: Some`).
pub fn nenum_glue(
    offset: u32,
    size: u32,
    align: u32,
    tag: NativeTagEncoding,
    whole_drop: u32,
    variants: Vec<NativeVariant>,
) -> NativeLayoutNode {
    NativeLayoutNode::Enum {
        offset,
        size,
        align,
        tag,
        whole_drop: Some(DropId(whole_drop)),
        variants: leak_slice(variants),
    }
}

pub fn nvec(offset: u32, elem: NativeLayoutNode, ctor: u32) -> NativeLayoutNode {
    NativeLayoutNode::Vec {
        offset,
        size: 24,
        align: 8,
        elem: leak(elem),
        ctor: CtorId(ctor),
    }
}

pub fn nbox(offset: u32, inner: NativeLayoutNode, ctor: u32) -> NativeLayoutNode {
    NativeLayoutNode::BoxPtr {
        offset,
        size: 8,
        align: 8,
        inner: leak(inner),
        ctor: CtorId(ctor),
    }
}

pub fn nset(offset: u32, elem: NativeLayoutNode, ctor: u32) -> NativeLayoutNode {
    NativeLayoutNode::Set {
        offset,
        size: 48,
        align: 8,
        elem: leak(elem),
        ctor: CtorId(ctor),
    }
}

pub fn nmap(
    offset: u32,
    key: NativeLayoutNode,
    value: NativeLayoutNode,
    ctor: u32,
) -> NativeLayoutNode {
    NativeLayoutNode::Map {
        offset,
        size: 48,
        align: 8,
        key: leak(key),
        value: leak(value),
        ctor: CtorId(ctor),
    }
}

pub fn narc(offset: u32, inner: NativeLayoutNode, ctor: u32) -> NativeLayoutNode {
    NativeLayoutNode::ArcPtr {
        offset,
        size: 8,
        align: 8,
        inner: leak(inner),
        ctor: CtorId(ctor),
    }
}

pub fn nstr(offset: u32) -> NativeLayoutNode {
    NativeLayoutNode::Str {
        offset,
        size: 24,
        align: 8,
    }
}

pub fn nblob(offset: u32, size: u32) -> NativeLayoutNode {
    NativeLayoutNode::Blob {
        offset,
        size,
        align: 8,
    }
}

pub fn nskip(offset: u32, size: u32, align: u32, writer: u32) -> NativeLayoutNode {
    NativeLayoutNode::Skip {
        offset,
        size,
        align,
        writer: SkipDefaultId(writer),
    }
}

// --- wire-tree builders -----------------------------------------------------

use distill_wire::wire::{SlotKind, WireEnumForm, WireField, WireNode, WireVariant};

pub fn wprim(offset: u32, kind: ScalarKind) -> WireNode {
    let (size, align) = scalar_geometry(kind);
    WireNode::Primitive {
        offset,
        size,
        align,
        kind,
    }
}

pub fn wfield(name: &str, declaration_index: u32, node: WireNode) -> WireField {
    WireField {
        name: name.to_string(),
        declaration_index,
        node,
    }
}

pub fn wstruct(offset: u32, size: u32, align: u32, fields: Vec<WireField>) -> WireNode {
    WireNode::Struct {
        offset,
        size,
        align,
        fields,
    }
}

pub fn wenum(
    offset: u32,
    size: u32,
    align: u32,
    form: WireEnumForm,
    variants: Vec<WireVariant>,
) -> WireNode {
    WireNode::Enum {
        offset,
        size,
        align,
        form,
        variants,
    }
}

pub fn wvariant(name: &str, discriminant: u128, node: WireNode) -> WireVariant {
    WireVariant {
        name: name.to_string(),
        discriminant,
        node,
    }
}

pub fn wslot(
    offset: u32,
    size: u32,
    align: u32,
    kind: SlotKind,
    pointee: Vec<WireNode>,
) -> WireNode {
    WireNode::Slot {
        offset,
        size,
        align,
        kind,
        pointee,
    }
}

pub fn wvec_slot(offset: u32, elem: WireNode) -> WireNode {
    wslot(offset, 24, 8, SlotKind::Vec, vec![elem])
}

pub fn wstring_slot(offset: u32) -> WireNode {
    wslot(offset, 24, 8, SlotKind::String, vec![])
}

pub fn wset_slot(offset: u32, elem: WireNode) -> WireNode {
    wslot(offset, 48, 8, SlotKind::Set, vec![elem])
}

pub fn wmap_slot(offset: u32, key: WireNode, value: WireNode) -> WireNode {
    wslot(offset, 48, 8, SlotKind::Map, vec![key, value])
}

pub fn wbox_slot(offset: u32, inner: WireNode) -> WireNode {
    wslot(offset, 8, 8, SlotKind::Box, vec![inner])
}

pub fn warc_slot(offset: u32, inner: WireNode) -> WireNode {
    wslot(offset, 8, 8, SlotKind::Arc, vec![inner])
}

pub fn wblob_slot(offset: u32, size: u32) -> WireNode {
    wslot(offset, size, 8, SlotKind::Blob, vec![])
}

// --- schema builders (ngp-schema model, §5) ----------------------------------

use ngp_schema::{
    CompilationIdentity, Field, FieldAttrs, FieldIdentifier, FieldLayout, LayoutView,
    PrimitiveType, Schema, SchemaLayouts, SchemaTypeId, TagEncoding, TypeAttrs, TypeDef,
    TypeLayout, TypePath,
};

pub fn test_identity() -> CompilationIdentity {
    CompilationIdentity {
        target_triple: "test-triple".to_string(),
        rustc: "rustc test".to_string(),
        source_fingerprint: [0; 32],
        features: Default::default(),
        cfgs: Default::default(),
        manifest_lock_hash: [0; 32],
        algorithm_version: 1,
    }
}

pub fn ty(id: usize, kind: PrimitiveType, name: Option<&str>, krate: &str) -> TypeDef {
    TypeDef {
        id: SchemaTypeId(id),
        kind,
        path: TypePath {
            name: name.map(|s| s.to_string()),
            containing_type: None,
            modules: vec![],
            krate: krate.to_string(),
        },
        uuid: None,
        attrs: TypeAttrs::default(),
        fields: vec![],
        generic_parameters: vec![],
        generic_argument_ids: vec![],
        has_default: false,
    }
}

pub fn tl(size: u64, align: u64) -> TypeLayout {
    TypeLayout {
        size: Some(size),
        align: Some(align),
        layout_complete: true,
        tag_encoding: None,
        fields: vec![],
    }
}

pub fn named_field(name: &str, type_id: usize) -> Field {
    Field {
        id: FieldIdentifier::Name(name.to_string()),
        type_id: SchemaTypeId(type_id),
        attrs: FieldAttrs::default(),
    }
}

pub fn tuple_field(index: usize, type_id: usize) -> Field {
    Field {
        id: FieldIdentifier::Number(index),
        type_id: SchemaTypeId(type_id),
        attrs: FieldAttrs::default(),
    }
}

pub fn variant_field(name: &str, type_id: usize) -> Field {
    Field {
        id: FieldIdentifier::Variant(name.to_string()),
        type_id: SchemaTypeId(type_id),
        attrs: FieldAttrs::default(),
    }
}

pub fn skip_field(name: &str, type_id: usize) -> Field {
    let mut f = named_field(name, type_id);
    f.attrs.skip = true;
    f
}

pub fn blob_field(name: &str, type_id: usize) -> Field {
    let mut f = named_field(name, type_id);
    f.attrs.blob = true;
    f
}

/// A struct/tuple/variant type with measured field offsets.
pub fn record(
    id: usize,
    kind: PrimitiveType,
    name: Option<&str>,
    krate: &str,
    size: u64,
    align: u64,
    fields: Vec<(Field, u64)>,
) -> (TypeDef, TypeLayout) {
    let mut def = ty(id, kind, name, krate);
    let mut layout = tl(size, align);
    for (field, offset) in fields {
        def.fields.push(field);
        layout.fields.push(FieldLayout {
            offset: Some(offset),
            field_size: None,
        });
    }
    (def, layout)
}

pub fn strukt(
    id: usize,
    name: &str,
    size: u64,
    align: u64,
    fields: Vec<(Field, u64)>,
) -> (TypeDef, TypeLayout) {
    record(
        id,
        PrimitiveType::Struct,
        Some(name),
        "game",
        size,
        align,
        fields,
    )
}

pub fn variant_ty(
    id: usize,
    enum_name: &str,
    name: &str,
    size: u64,
    align: u64,
    fields: Vec<(Field, u64)>,
) -> (TypeDef, TypeLayout) {
    let (mut def, layout) = record(
        id,
        PrimitiveType::EnumVariant,
        Some(name),
        "game",
        size,
        align,
        fields,
    );
    def.path.containing_type = Some(enum_name.to_string());
    (def, layout)
}

pub fn enum_ty(
    id: usize,
    name: &str,
    size: u64,
    align: u64,
    tag: TagEncoding,
    variants: Vec<(&str, usize)>,
) -> (TypeDef, TypeLayout) {
    let mut def = ty(id, PrimitiveType::Enum, Some(name), "game");
    let mut layout = tl(size, align);
    layout.tag_encoding = Some(tag);
    for (vname, vid) in variants {
        def.fields.push(variant_field(vname, vid));
        layout.fields.push(FieldLayout::default());
    }
    (def, layout)
}

pub fn prim(id: usize, kind: PrimitiveType) -> (TypeDef, TypeLayout) {
    let (size, align): (u64, u64) = match kind {
        PrimitiveType::Boolean | PrimitiveType::U8 | PrimitiveType::I8 => (1, 1),
        PrimitiveType::U16 | PrimitiveType::I16 => (2, 2),
        PrimitiveType::Char | PrimitiveType::U32 | PrimitiveType::I32 | PrimitiveType::F32 => {
            (4, 4)
        }
        PrimitiveType::U64 | PrimitiveType::I64 | PrimitiveType::F64 => (8, 8),
        PrimitiveType::U128 | PrimitiveType::I128 => (16, 16),
        PrimitiveType::Unit => (0, 1),
        _ => panic!("not a primitive: {kind:?}"),
    };
    (ty(id, kind, None, ""), tl(size, align))
}

fn container(
    id: usize,
    name: &str,
    krate: &str,
    size: u64,
    align: u64,
    args: Vec<usize>,
) -> (TypeDef, TypeLayout) {
    let mut def = ty(id, PrimitiveType::Struct, Some(name), krate);
    def.generic_argument_ids = args.into_iter().map(SchemaTypeId).collect();
    (def, tl(size, align))
}

pub fn vec_ty(id: usize, elem: usize) -> (TypeDef, TypeLayout) {
    container(id, "Vec", "alloc", 24, 8, vec![elem])
}

pub fn string_ty(id: usize) -> (TypeDef, TypeLayout) {
    (
        ty(id, PrimitiveType::String, Some("String"), "alloc"),
        tl(24, 8),
    )
}

pub fn box_ty(id: usize, inner: usize) -> (TypeDef, TypeLayout) {
    container(id, "Box", "alloc", 8, 8, vec![inner])
}

pub fn arc_ty(id: usize, inner: usize) -> (TypeDef, TypeLayout) {
    container(id, "Arc", "std", 8, 8, vec![inner])
}

pub fn hashmap_ty(
    id: usize,
    key: usize,
    value: usize,
    hasher: Option<usize>,
) -> (TypeDef, TypeLayout) {
    let mut args = vec![key, value];
    args.extend(hasher);
    container(id, "HashMap", "std", 48, 8, args)
}

pub fn btreemap_ty(id: usize, key: usize, value: usize) -> (TypeDef, TypeLayout) {
    container(id, "BTreeMap", "alloc", 24, 8, vec![key, value])
}

pub fn hashset_ty(id: usize, elem: usize, hasher: Option<usize>) -> (TypeDef, TypeLayout) {
    let mut args = vec![elem];
    args.extend(hasher);
    container(id, "HashSet", "std", 48, 8, args)
}

pub fn fixed_state_ty(id: usize) -> (TypeDef, TypeLayout) {
    let mut def = ty(
        id,
        PrimitiveType::Struct,
        Some("FixedState"),
        "distill_support",
    );
    def.uuid = Some(ngp_schema::FIXED_STATE_UUID);
    (def, tl(0, 1))
}

pub fn array_ty(id: usize, len: usize, elem: usize) -> (TypeDef, TypeLayout) {
    let mut def = ty(
        id,
        PrimitiveType::StaticArray(ngp_schema::StaticArray { length: len }),
        None,
        "",
    );
    def.generic_argument_ids = vec![SchemaTypeId(elem)];
    // Size/align computed by the deriver from the element; vacuous here.
    (def, TypeLayout::vacuous(0))
}

pub fn option_ty(
    id: usize,
    size: u64,
    align: u64,
    tag: TagEncoding,
    none_variant: usize,
    some_variant: usize,
) -> (TypeDef, TypeLayout) {
    let mut def = ty(id, PrimitiveType::Enum, Some("Option"), "core");
    let mut layout = tl(size, align);
    layout.tag_encoding = Some(tag);
    def.fields = vec![
        variant_field("None", none_variant),
        variant_field("Some", some_variant),
    ];
    layout.fields = vec![FieldLayout::default(), FieldLayout::default()];
    (def, layout)
}

/// Assemble a schema with one layout table under `test_identity()`.
/// Entries must be supplied in id order (asserted).
pub fn schema(entries: Vec<(TypeDef, TypeLayout)>) -> Schema {
    let mut types = Vec::new();
    let mut layouts = Vec::new();
    for (i, (def, layout)) in entries.into_iter().enumerate() {
        assert_eq!(
            def.id,
            SchemaTypeId(i),
            "schema entries must be in id order"
        );
        types.push(def);
        layouts.push(layout);
    }
    Schema {
        source_hashes: Default::default(),
        types,
        layouts: vec![SchemaLayouts {
            identity: test_identity(),
            layouts,
        }],
    }
}

pub fn view(s: &Schema) -> LayoutView<'_> {
    s.single_layout().expect("one layout table")
}
