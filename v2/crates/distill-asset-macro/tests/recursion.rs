//! A recursive asset type end to end: the `#[asset]` logical schema and
//! native layout, the wire layout derived from a measured layout schema
//! (§12), the encoder, and the fixup plan that constructs the value again.
//! The shape is the cue source tree's (newgameplus `CueSource`): a node
//! re-entered inline through another record inside a `Vec`, through enum
//! variant payloads, and through an `Option<Box<_>>`.

#[path = "../../distill-wire/tests/common/mod.rs"]
mod common;

use std::mem::{align_of, offset_of, size_of, MaybeUninit};

use common::*;
use distill_asset::{AssetReflect, AssetType};
use distill_core::id::{AssetUuid, TypeUuid};
use distill_wire::derive::derive_wire;
use distill_wire::encode::encode_value;
use distill_wire::exec::{execute_fixup, ExecEnv, ExecLimits};
use distill_wire::native::NativeLayoutNode;
use distill_wire::plan::{compile_plans, PlanId};
use distill_wire::wire::WireNode;
use ngp_schema::{PrimitiveType, SchemaTypeId, TagEncoding};

#[derive(Debug, Default, PartialEq)]
#[distill_asset_macro::asset(uuid = "70000000-0000-4000-8000-000000000001")]
struct Tree {
    root: Node,
    release: Option<Node>,
}

#[derive(Debug, Default, PartialEq)]
#[distill_asset_macro::asset(uuid = "70000000-0000-4000-8000-000000000002")]
struct Node {
    kind: Kind,
    gain: u32,
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
#[distill_asset_macro::asset(uuid = "70000000-0000-4000-8000-000000000003")]
enum Kind {
    Leaf { id: u32 },
    Many { children: Vec<Node> },
    Switch { children: Vec<Child> },
}

impl Default for Kind {
    fn default() -> Self {
        Kind::Leaf { id: 0 }
    }
}

/// Holds `Node` inline: re-entered through a record, not a slot.
#[derive(Debug, Default, PartialEq)]
#[distill_asset_macro::asset(uuid = "70000000-0000-4000-8000-000000000004")]
struct Child {
    min: u32,
    node: Node,
    alt: Option<Box<Node>>,
}

/// `#[repr(u8)]` lays each variant out as a `#[repr(C)]` struct behind
/// the tag.
#[repr(C)]
struct LeafImage {
    tag: u8,
    id: u32,
}

#[repr(C)]
struct VecImage {
    tag: u8,
    children: Vec<Node>,
}

/// The measured layout schema source-walk would record for `Tree`.
fn layout_schema() -> ngp_schema::Schema {
    let option = |id: usize, size: usize, niche: TagEncoding, none: usize, some: usize, arg: usize| {
        let (mut def, layout) = option_ty(id, size as u64, 8, niche, none, some);
        def.generic_argument_ids = vec![SchemaTypeId(arg)];
        (def, layout)
    };
    let node_size = size_of::<Node>();
    schema(vec![
        prim(0, PrimitiveType::U32),
        strukt(
            1,
            "Tree",
            size_of::<Tree>() as u64,
            align_of::<Tree>() as u64,
            vec![
                (named_field("root", 2), offset_of!(Tree, root) as u64),
                (named_field("release", 8), offset_of!(Tree, release) as u64),
            ],
        ),
        strukt(
            2,
            "Node",
            node_size as u64,
            align_of::<Node>() as u64,
            vec![
                (named_field("kind", 3), offset_of!(Node, kind) as u64),
                (named_field("gain", 0), offset_of!(Node, gain) as u64),
            ],
        ),
        enum_ty(
            3,
            "Kind",
            size_of::<Kind>() as u64,
            align_of::<Kind>() as u64,
            TagEncoding::Direct {
                tag_size: 1,
                tag_offset: 0,
                variant_values: vec![0, 1, 2],
            },
            vec![("Leaf", 4), ("Many", 5), ("Switch", 6)],
        ),
        variant_ty(
            4,
            "Kind",
            "Leaf",
            size_of::<LeafImage>() as u64,
            align_of::<LeafImage>() as u64,
            vec![(named_field("id", 0), offset_of!(LeafImage, id) as u64)],
        ),
        variant_ty(
            5,
            "Kind",
            "Many",
            size_of::<VecImage>() as u64,
            8,
            vec![(named_field("children", 7), offset_of!(VecImage, children) as u64)],
        ),
        variant_ty(
            6,
            "Kind",
            "Switch",
            size_of::<VecImage>() as u64,
            8,
            vec![(named_field("children", 9), offset_of!(VecImage, children) as u64)],
        ),
        vec_ty(7, 2),
        option(
            8,
            size_of::<Option<Node>>(),
            TagEncoding::Niche {
                niche_field_offset: offset_of!(Node, kind) as u64,
                niche_field_size: 1,
                untagged_variant: 1,
                niche_variants_start: 0,
                niche_variants_end: 0,
                niche_start: 3,
            },
            10,
            11,
            2,
        ),
        vec_ty(9, 12),
        variant_ty(10, "Option", "None", 0, 1, vec![]),
        variant_ty(11, "Option", "Some", node_size as u64, 8, vec![(tuple_field(0, 2), 0)]),
        strukt(
            12,
            "Child",
            size_of::<Child>() as u64,
            align_of::<Child>() as u64,
            vec![
                (named_field("min", 0), offset_of!(Child, min) as u64),
                (named_field("node", 2), offset_of!(Child, node) as u64),
                (named_field("alt", 13), offset_of!(Child, alt) as u64),
            ],
        ),
        option(
            13,
            8,
            TagEncoding::Niche {
                niche_field_offset: 0,
                niche_field_size: 8,
                untagged_variant: 1,
                niche_variants_start: 0,
                niche_variants_end: 0,
                niche_start: 0,
            },
            10,
            15,
            14,
        ),
        box_ty(14, 2),
        variant_ty(15, "Option", "Some", 8, 8, vec![(tuple_field(0, 14), 0)]),
    ])
}

fn leaf(id: u32) -> Node {
    Node {
        kind: Kind::Leaf { id },
        gain: id * 10,
    }
}

fn child(min: u32, node: Node, alt: Option<Node>) -> Child {
    Child {
        min,
        node,
        alt: alt.map(Box::new),
    }
}

fn sample() -> Tree {
    Tree {
        root: Node {
            kind: Kind::Switch {
                children: vec![
                    child(
                        0,
                        Node {
                            kind: Kind::Many {
                                children: vec![leaf(1), leaf(2)],
                            },
                            gain: 3,
                        },
                        Some(leaf(4)),
                    ),
                    child(
                        5,
                        Node {
                            kind: Kind::Switch {
                                children: vec![child(6, leaf(7), None)],
                            },
                            gain: 8,
                        },
                        None,
                    ),
                ],
            },
            gain: 9,
        },
        release: Some(Node {
            kind: Kind::Many {
                children: vec![leaf(10)],
            },
            gain: 11,
        }),
    }
}

/// Every struct or enum frame's re-entry is a back-reference, wherever
/// it is reached: `Child.node` re-enters `Node` inline.
fn backref_at<'a>(wire: &'a WireNode, path: &[&str]) -> &'a WireNode {
    let mut node = wire;
    for step in path {
        node = match node {
            WireNode::Struct { fields, .. } => {
                &fields.iter().find(|f| f.name == *step).expect("field").node
            }
            WireNode::Enum { variants, .. } => {
                &variants.iter().find(|v| v.name == *step).expect("variant").node
            }
            WireNode::Slot { pointee, .. } => &pointee[step.parse::<usize>().unwrap()],
            other => panic!("no step {step} into {other:?}"),
        };
    }
    node
}

#[test]
fn a_type_reentered_inline_through_a_record_round_trips() {
    let layouts = layout_schema();
    let wire = derive_wire(layouts.single_layout().unwrap(), SchemaTypeId(1)).unwrap();
    // Tree > Node > Kind > Switch > (Vec) Child: Node is three frames out.
    assert!(matches!(
        backref_at(&wire, &["root", "kind", "Switch", "children", "0", "node"]),
        WireNode::BackRef { distance: 3, .. }
    ));

    let logical = ngp_schema::node_from_bytes(&distill_asset::build::logical_bytes::<Tree>())
        .unwrap()
        .root;
    let tree = sample();
    let mut no_references = |_: &ngp_schema::AssetRefValue,
                             _: TypeUuid,
                             _: bool,
                             _: &[distill_bundle::PathComponent]|
     -> Result<AssetUuid, String> { Err("no references".into()) };
    let encoded = encode_value(&logical, &wire, &tree.to_value(), &mut no_references).unwrap();

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
        execute_fixup(
            &plans,
            PlanId(0),
            &encoded.fixed,
            &encoded.variable,
            &env,
            out.as_mut_ptr().cast(),
        )
        .unwrap();
        out.assume_init()
    };
    assert_eq!(got, tree);
}

/// Native back-reference distances count variant payload frames, as the
/// wire's and the native validator's do: from `Many`'s `Vec<Node>`, the
/// frames out to `Node` are `Kind` and the `Many` payload.
#[test]
fn native_backrefs_count_variant_payload_frames() {
    let NativeLayoutNode::Struct { fields, .. } = Node::descriptor().native_layout else {
        panic!("Node is a struct");
    };
    let kind = fields.iter().find(|f| f.name == "kind").unwrap();
    let NativeLayoutNode::Enum { variants, .. } = kind.node else {
        panic!("kind is an enum");
    };
    let many = variants.iter().find(|v| v.name == "Many").unwrap();
    let NativeLayoutNode::Struct { fields, .. } = many.node else {
        panic!("a payload is a struct");
    };
    let NativeLayoutNode::Vec { elem, .. } = fields[0].node else {
        panic!("children is a Vec");
    };
    assert!(matches!(elem, NativeLayoutNode::BackRef { distance: 2, .. }), "{elem:?}");
}
