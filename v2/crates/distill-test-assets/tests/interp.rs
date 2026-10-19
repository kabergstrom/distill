//! The interpreter over this crate's walked table (newgameplus GAP 23
//! §18.2): descriptors, logical schemas, values, defaults, and a recursive
//! value through encode and fixup.

use std::mem::MaybeUninit;

use distill_asset::{to_value, AssetType, NativeLayoutNode};
use distill_core::id::{AssetUuid, TypeUuid};
use distill_test_assets::recursion::{Child, Kind, Node, Recursive, Tree};
use distill_test_assets::semantic::{
    Example, Inner, Level, Nested, ReferenceTarget, RenamedShape, RevisedShape, SameShapeA, SameShapeB,
    SemanticFacts,
};
use distill_wire::encode::encode_value;
use distill_wire::exec::{execute_fixup, ExecEnv, ExecLimits};
use distill_wire::plan::{compile_plans, PlanId};
use ngp_schema::node::SchemaNode;
use ngp_schema::Value;

fn root<T: AssetType>() -> &'static SchemaNode {
    &distill_asset::logical::<T>().root
}

fn field_names(node: &SchemaNode) -> Vec<&str> {
    match node {
        SchemaNode::Struct { fields, .. } => fields.iter().map(|(n, _, _)| n.as_str()).collect(),
        other => panic!("not a struct: {other:?}"),
    }
}

#[test]
fn descriptors_are_built_once_from_the_table() {
    let d = Example::descriptor();
    assert_eq!(d.type_uuid, Example::TYPE_UUID);
    assert_eq!(
        d.type_uuid.0,
        [0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x84, 0x44, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55]
    );
    assert_eq!((d.size, d.align), (size_of::<Example>(), align_of::<Example>()));
    assert!(d.build_only);
    assert!(std::ptr::eq(d, Example::descriptor()));
    let NativeLayoutNode::Struct { fields, .. } = d.native_layout else { panic!("struct") };
    assert_eq!(fields.len(), 5);
    assert!(fields
        .windows(2)
        .all(|w| (w[0].node.offset(), w[0].declaration_index) <= (w[1].node.offset(), w[1].declaration_index)));
    assert!(fields.iter().any(|f| matches!(f.node, NativeLayoutNode::Skip { .. })));
    assert!(!d.ctors.entries.is_empty());
    assert_eq!(d.skip_writers.entries.len(), 1);
    let NativeLayoutNode::Enum { variants, .. } = Level::descriptor().native_layout else { panic!("enum") };
    assert_eq!(variants.len(), 2);
}

#[test]
fn logical_schemas_follow_the_attributes() {
    // Name-sorted, the skipped field absent, the revision kept.
    let SchemaNode::Struct { rev, .. } = root::<Example>() else { panic!("struct") };
    assert_eq!(*rev, 3);
    assert_eq!(field_names(root::<Example>()), ["map", "name", "values", "z"]);
    let SchemaNode::Struct { fields, .. } = root::<SemanticFacts>() else { panic!("struct") };
    let kinds: Vec<_> = fields.iter().map(|(n, _, node)| (n.as_str(), node.clone())).collect();
    assert_eq!(
        kinds,
        [
            ("label", SchemaNode::String),
            ("payload", SchemaNode::Blob),
            ("strong", SchemaNode::AssetRef(ReferenceTarget::TYPE_UUID)),
            ("weak", SchemaNode::WeakRef(ReferenceTarget::TYPE_UUID)),
        ]
    );
    // Identity is structural and revision sensitive; renames keep it.
    let hash = |d: &distill_asset::AssetRuntimeDescriptor| d.logical_hash;
    assert_eq!(hash(SameShapeA::descriptor()), hash(SameShapeB::descriptor()));
    assert_eq!(hash(SameShapeA::descriptor()), hash(RenamedShape::descriptor()));
    assert_ne!(hash(SameShapeA::descriptor()), hash(RevisedShape::descriptor()));
}

#[test]
fn values_read_in_schema_order() {
    let mut example = Example { z: 4, name: "n".into(), values: vec![1, 2], ..Example::default() };
    example.map.insert(20, "twenty".into());
    example.map.insert(1, "one".into());
    let Value::Struct(fields) = to_value(&example) else { panic!("struct") };
    let [Value::Map(map), name, values, z] = &fields[..] else { panic!("{fields:?}") };
    let mut map = map.clone();
    map.sort_by_key(|(k, _)| match k {
        Value::U32(k) => *k,
        _ => panic!("u32 keys"),
    });
    assert_eq!(
        map,
        [(Value::U32(1), Value::String("one".into())), (Value::U32(20), Value::String("twenty".into()))]
    );
    assert_eq!(name, &Value::String("n".into()));
    assert_eq!(values, &Value::Seq(vec![Value::U32(1), Value::U32(2)]));
    assert_eq!(z, &Value::U16(4));
    assert_eq!(to_value(&Level::High), Value::Enum { variant: 0, payload: Box::new([]) });
    assert_eq!(to_value(&Level::Low), Value::Enum { variant: 1, payload: Box::new([]) });
}

#[test]
fn defaults_answer_per_container() {
    let example = &Example::info().reflect;
    let at = |f: &str| vec![f.to_owned()];
    let ok = |r: Option<Result<Value, _>>| r.map(|r| r.unwrap());
    assert_eq!(ok(example.field_default(root::<Example>(), &at("name"))), Some(Value::String(String::new())));
    assert_eq!(ok(example.parent_default(root::<Example>(), &at("z"))), Some(Value::U16(0)));
    assert_eq!(ok(example.field_default(root::<Example>(), &at("cache"))), None);
    // Nested containers: the element struct of a Vec field.
    let nested = &Nested::info().reflect;
    assert_eq!(ok(nested.field_default(root::<Inner>(), &at("number"))), Some(Value::U32(0)));
    assert_eq!(ok(nested.field_default(root::<Nested>(), &at("values"))), Some(Value::Seq(Vec::new())));
    // A recursive type's containers are finite.
    let recursive = &Recursive::info().reflect;
    assert!(recursive.default_containers().count() <= 2);
    assert_eq!(ok(recursive.field_default(root::<Recursive>(), &at("number"))), Some(Value::U32(0)));
}

fn leaf(id: u32) -> Node {
    Node { kind: Kind::Leaf { id }, gain: id * 10 }
}

fn child(min: u32, node: Node, alt: Option<Node>) -> Child {
    Child { min, node, alt: alt.map(Box::new) }
}

fn sample() -> Tree {
    Tree {
        root: Node {
            kind: Kind::Switch {
                children: vec![
                    child(0, Node { kind: Kind::Many { children: vec![leaf(1), leaf(2)] }, gain: 3 }, Some(leaf(4))),
                    child(5, Node { kind: Kind::Switch { children: vec![child(6, leaf(7), None)] }, gain: 8 }, None),
                ],
            },
            gain: 9,
        },
        release: Some(Node { kind: Kind::Many { children: vec![leaf(10)] }, gain: 11 }),
    }
}

/// A type re-entered inline through a record, through variant payloads and
/// through `Option<Box<_>>`: wire from the table's schema, encode, fixup
/// through the descriptor, read back.
#[test]
fn a_recursive_value_round_trips() {
    let table = &distill_test_assets::NGP_ASSET_TABLE;
    let info = Tree::info();
    let wire = distill_wire::derive::derive_wire(table.schema().single_layout().unwrap(), info.id).unwrap();
    let tree = sample();
    let value = to_value(&tree);
    let mut no_references = |_: &ngp_schema::AssetRefValue,
                             _: TypeUuid,
                             _: bool,
                             _: &[distill_bundle::PathComponent]|
     -> Result<AssetUuid, String> { Err("no references".into()) };
    let encoded = encode_value(&info.reflect.logical.root, &wire, &value, &mut no_references).unwrap();
    let descriptor = Tree::descriptor();
    let plans = compile_plans(&wire, descriptor.native_layout).unwrap();
    let env = ExecEnv {
        ctors: descriptor.ctors,
        drops: descriptor.drops,
        skips: descriptor.skip_writers,
        blobs: &[],
        limits: ExecLimits::default(),
    };
    let mut out = MaybeUninit::<Tree>::uninit();
    let got = unsafe {
        execute_fixup(&plans, PlanId(0), &encoded.fixed, &encoded.variable, &env, out.as_mut_ptr().cast()).unwrap();
        out.assume_init()
    };
    assert_eq!(got, tree);
    assert_eq!(to_value(&got), value);
}
