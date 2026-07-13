#![allow(dead_code)]

use std::collections::BTreeMap;
use std::mem::{align_of, size_of};

use distill_asset::{
    default_table, AssetHashMap, AssetReflect, AssetType, Blob, EncodeContainer, EncodeSink,
    PathStep,
};
use distill_core::attestation::{ReferenceStrength, RegistryExtraFact, RegistryPathStep};
use distill_core::id::{AssetUuid, TypeUuid};
use distill_json::AuthoredValue;
use distill_wire::dsnl::dsnl_hash;
use distill_wire::native::NativeLayoutNode;

#[derive(Default)]
#[distill_asset_macro::asset(uuid = "11111111-2222-3333-8444-555555555555", rev = 3, build_only)]
struct Example {
    z: u16,
    name: String,
    values: Vec<u32>,
    map: AssetHashMap<u32, String>,
    #[asset(skip)]
    cache: BTreeMap<String, usize>,
}

#[repr(u8)]
#[distill_asset_macro::asset(uuid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")]
enum Choice {
    Zed = 9,
    Alpha(u32) = 3,
}

#[derive(Default)]
#[distill_asset_macro::asset(uuid = "01010101-0202-4303-8404-050505050505")]
struct Inner {
    number: u32,
}

#[derive(Default)]
#[distill_asset_macro::asset(uuid = "11110101-0202-4303-8404-050505050505")]
struct Nested {
    values: Vec<Inner>,
}

#[derive(Default)]
#[distill_asset_macro::asset(uuid = "12110101-0202-4303-8404-050505050505")]
struct Recursive {
    child: Option<Box<Recursive>>,
    number: u32,
}

#[distill_asset_macro::asset(uuid = "20000000-0000-4000-8000-000000000001")]
struct SameShapeA {
    value: u32,
}

#[distill_asset_macro::asset(uuid = "20000000-0000-4000-8000-000000000002")]
struct SameShapeB {
    value: u32,
}

#[distill_asset_macro::asset(uuid = "20000000-0000-4000-8000-000000000003", rev = 1)]
struct RevisedShape {
    value: u32,
}

#[derive(Default)]
struct SkipA(u32);

#[derive(Default)]
struct SkipB(u32);

#[distill_asset_macro::asset(uuid = "30000000-0000-4000-8000-000000000001")]
struct SkipShapeA {
    value: u32,
    #[asset(skip)]
    cache: SkipA,
}

#[distill_asset_macro::asset(uuid = "30000000-0000-4000-8000-000000000002")]
struct SkipShapeB {
    value: u32,
    #[asset(skip)]
    cache: SkipB,
}

#[distill_asset_macro::asset(uuid = "40000000-0000-4000-8000-000000000001")]
struct ReferenceTarget {
    value: u32,
}

#[distill_asset_macro::asset(uuid = "40000000-0000-4000-8000-000000000002", build_only)]
struct SemanticFacts {
    strong: distill_asset::AssetRef<ReferenceTarget>,
    weak: distill_asset::WeakAssetRef<ReferenceTarget>,
    #[asset(blob)]
    payload: Blob,
    #[asset(tag)]
    label: String,
}

#[derive(Default)]
struct Sink(Vec<String>);

impl EncodeSink for Sink {
    fn flat(&mut self, bytes: &[u8]) {
        self.0.push(format!("flat:{bytes:?}"));
    }
    fn begin(&mut self, kind: EncodeContainer, len: u32) {
        self.0.push(format!("begin:{kind:?}:{len}"));
    }
    fn push(&mut self) {
        self.0.push("push".into());
    }
    fn finish(&mut self) {
        self.0.push("finish".into());
    }
    fn blob(&mut self, bytes: &[u8]) {
        self.0.push(format!("blob:{bytes:?}"));
    }
    fn reference(&mut self, _: bool, _: AssetUuid, _: TypeUuid) {
        unreachable!()
    }
}

#[test]
fn macro_builds_a_self_consistent_runtime_descriptor() {
    let d = Example::descriptor();
    assert_eq!(
        d.type_uuid.0,
        [
            0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x84, 0x44, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55
        ]
    );
    assert_eq!(d.size, size_of::<Example>());
    assert_eq!(d.align, align_of::<Example>());
    assert!(d.build_only);
    assert_eq!(d.layout_digest, dsnl_hash(d.native_layout).unwrap());
    assert_ne!(d.layout_digest, d.fixup_identity);
    assert!(std::ptr::eq(d, Example::descriptor()));

    let NativeLayoutNode::Struct { fields, .. } = d.native_layout else {
        panic!("struct")
    };
    assert_eq!(fields.len(), 5);
    assert!(fields.windows(2).all(|w| {
        (w[0].node.offset(), w[0].declaration_index) <= (w[1].node.offset(), w[1].declaration_index)
    }));
    assert!(fields
        .iter()
        .any(|f| matches!(f.node, NativeLayoutNode::Skip { .. })));
    assert!(!d.ctors.entries.is_empty());
    assert_eq!(d.skip_writers.entries.len(), 1);
}

#[test]
fn macro_generates_logical_hash_defaults_and_deterministic_encoding() {
    let table = default_table::<Example>();
    assert!(table.parent.is_some());
    assert!(table
        .nodes
        .iter()
        .any(|(_, path, _)| path == &[PathStep::Field("name")]));
    assert!(!table
        .nodes
        .iter()
        .any(|(_, path, _)| path == &[PathStep::Field("cache")]));
    let parent = (table.parent.unwrap())().unwrap();
    let AuthoredValue::Object(parent) = parent else {
        panic!("object")
    };
    assert!(!parent.contains_key("cache"));

    let mut value = Example {
        z: 4,
        ..Example::default()
    };
    value.map.insert(20, "twenty".into());
    value.map.insert(1, "one".into());
    let mut sink = Sink::default();
    AssetReflect::encode(&value, &mut sink);
    assert!(sink.0.starts_with(&["begin:Struct:4".into()]));
    let one = sink
        .0
        .iter()
        .position(|event| event == "flat:[1, 0, 0, 0]")
        .unwrap();
    let twenty = sink
        .0
        .iter()
        .position(|event| event == "flat:[20, 0, 0, 0]")
        .unwrap();
    assert!(one < twenty, "map keys are emitted in encoded-byte order");
    assert_eq!(
        Example::descriptor().logical_hash,
        Example::descriptor().logical_hash
    );
}

#[test]
fn enum_descriptor_and_encoder_use_name_sorted_wire_variant_indices() {
    let d = Choice::descriptor();
    let NativeLayoutNode::Enum { variants, .. } = d.native_layout else {
        panic!("enum")
    };
    assert_eq!(variants.len(), 2);
    let mut sink = Sink::default();
    AssetReflect::encode(&Choice::Alpha(7), &mut sink);
    assert_eq!(sink.0.first().unwrap(), "begin:Variant(0):1");

    let value = Choice::Alpha(7);
    let field_offset = match &value {
        Choice::Alpha(field) => (field as *const u32 as usize) - (&value as *const Choice as usize),
        Choice::Zed => unreachable!(),
    };
    let alpha = variants
        .iter()
        .find(|variant| variant.name == "Alpha")
        .unwrap();
    let NativeLayoutNode::Struct { fields, .. } = alpha.node else {
        panic!("payload")
    };
    assert_eq!(
        alpha.node.offset() as usize + fields[0].node.offset() as usize,
        field_offset
    );
}

#[test]
fn logical_identity_is_structural_and_revision_sensitive() {
    assert_eq!(
        SameShapeA::descriptor().logical_hash,
        SameShapeB::descriptor().logical_hash
    );
    assert_ne!(
        SameShapeA::descriptor().logical_hash,
        RevisedShape::descriptor().logical_hash
    );
}

#[test]
fn default_table_covers_nested_container_paths() {
    let table = default_table::<Nested>();
    assert!(table
        .nodes
        .iter()
        .any(|(node, path, _)| node.0 == 0 && path == &[PathStep::Field("values")]));
    assert!(table
        .nodes
        .iter()
        .any(|(node, path, _)| node.0 == 1 && path == &[PathStep::Elem]));
    assert!(table
        .nodes
        .iter()
        .any(|(node, path, _)| node.0 == 2 && path == &[PathStep::Field("number")]));
}

#[test]
fn recursive_default_table_reuses_schema_node_ids_and_is_finite() {
    let table = default_table::<Recursive>();
    assert!(table.nodes.len() <= 6, "recursive schema was unrolled");
    assert_eq!(table.nodes.iter().map(|(node, _, _)| node.0).max(), Some(2));
    assert!(table
        .nodes
        .iter()
        .any(|(node, path, _)| node.0 == 0 && path == &[PathStep::Field("child")]));
}

#[test]
fn fixup_identity_commits_to_nominal_skip_table_assignments() {
    let a = SkipShapeA::descriptor();
    let b = SkipShapeB::descriptor();
    assert_eq!(a.layout_digest, b.layout_digest);
    assert_ne!(a.fixup_identity, b.fixup_identity);
}

#[test]
fn descriptor_exposes_typed_complete_registry_facts_and_finite_backrefs() {
    let descriptor = SemanticFacts::descriptor();
    let row = descriptor.compiled_type;
    assert_eq!(row.type_uuid, descriptor.type_uuid);
    assert_eq!(row.logical_hash, descriptor.logical_hash);
    assert_eq!(row.native_layout_digest, descriptor.layout_digest);
    assert!(row.build_only);
    row.validate().unwrap();

    let fact_at = |field: &str| {
        row.registry_extras
            .rows
            .iter()
            .find(|row| row.path == vec![RegistryPathStep::Field(field.to_owned())])
    };
    assert!(matches!(
        fact_at("strong").map(|row| &row.fact),
        Some(RegistryExtraFact::Reference {
            strength: ReferenceStrength::Strong,
            target,
        }) if *target == ReferenceTarget::TYPE_UUID
    ));
    assert!(matches!(
        fact_at("weak").map(|row| &row.fact),
        Some(RegistryExtraFact::Reference {
            strength: ReferenceStrength::Weak,
            target,
        }) if *target == ReferenceTarget::TYPE_UUID
    ));
    assert!(matches!(
        fact_at("payload").map(|row| &row.fact),
        Some(RegistryExtraFact::Blob)
    ));
    assert!(matches!(
        fact_at("label").map(|row| &row.fact),
        Some(RegistryExtraFact::Tag)
    ));
    assert!(row
        .registry_extras
        .rows
        .iter()
        .any(|row| matches!(row.fact, RegistryExtraFact::BuildOnly(true))));

    let recursive = Recursive::descriptor().compiled_type;
    assert!(recursive.registry_extras.rows.len() < 8);
    assert!(recursive.registry_extras.rows.iter().any(
        |row| matches!(row.fact, RegistryExtraFact::BackReference { target } if target.0 == 0)
    ));
}
