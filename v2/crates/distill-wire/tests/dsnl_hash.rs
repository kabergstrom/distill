//! DSNL — the measured native-layout digest (§12): grammar bytes pinned,
//! physical-order sorting, table-id exclusion, backref frames.

mod common;

use common::*;
use distill_wire::dsnl::{
    dsnl_bytes, dsnl_hash, measured_dsnl_bytes, measured_dsnl_hash, DSNL_VERSION,
};
use distill_wire::measured::derive_measured_native;
use distill_wire::native::{
    CtorId, LayoutHashError, NativeLayoutNode, NativeTagEncoding, NativeVariantTag, ScalarKind,
    SkipDefaultId,
};
use ngp_schema::{PrimitiveType, SchemaTypeId};

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn header(out: &mut Vec<u8>, kind: u8, offset: u32, size: u32, align: u32) {
    out.push(kind);
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&align.to_le_bytes());
}

// --- pinned grammar bytes -----------------------------------------------------

#[test]
fn scalar_bytes_pinned() {
    let node = scalar(4, ScalarKind::U32);
    let mut expected = Vec::new();
    header(&mut expected, 0x01, 4, 4, 4);
    expected.push(0x04); // ScalarKind::U32 id
    assert_eq!(dsnl_bytes(&node).unwrap(), expected);
}

#[test]
fn hash_is_domain_prefixed_and_versioned() {
    let node = scalar(0, ScalarKind::Bool);
    let body = dsnl_bytes(&node).unwrap();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DSNL");
    hasher.update(&[DSNL_VERSION]);
    hasher.update(&body);
    assert_eq!(dsnl_hash(&node).unwrap(), *hasher.finalize().as_bytes());
}

#[test]
fn shared_schema_projection_matches_the_static_dsnl_grammar() {
    let schema = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U16),
        strukt(
            2,
            "S",
            8,
            4,
            vec![(named_field("a", 0), 0), (named_field("b", 1), 4)],
        ),
    ]);
    let measured = derive_measured_native(view(&schema), SchemaTypeId(2)).unwrap();
    let static_tree = nstruct(
        0,
        8,
        4,
        vec![
            nfield("a", 0, scalar(0, ScalarKind::U32)),
            nfield("b", 1, scalar(4, ScalarKind::U16)),
        ],
    );
    assert_eq!(
        measured_dsnl_bytes(&measured).unwrap(),
        dsnl_bytes(&static_tree).unwrap()
    );
    assert_eq!(
        measured_dsnl_hash(&measured).unwrap(),
        dsnl_hash(&static_tree).unwrap()
    );
}

#[test]
fn struct_bytes_pinned_with_fields_in_physical_order() {
    // Declared b-then-a in the slice; physical order is by (offset, decl idx).
    let node = nstruct(
        0,
        8,
        4,
        vec![
            nfield("b", 1, scalar(4, ScalarKind::U16)),
            nfield("a", 0, scalar(0, ScalarKind::U32)),
        ],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x02, 0, 8, 4);
    expected.extend_from_slice(&2u32.to_le_bytes()); // field count
    put_str(&mut expected, "a");
    expected.extend_from_slice(&0u32.to_le_bytes()); // declaration_index
    header(&mut expected, 0x01, 0, 4, 4);
    expected.push(0x04); // U32
    put_str(&mut expected, "b");
    expected.extend_from_slice(&1u32.to_le_bytes());
    header(&mut expected, 0x01, 4, 2, 2);
    expected.push(0x03); // U16
    assert_eq!(dsnl_bytes(&node).unwrap(), expected);
}

#[test]
fn equal_offset_zsts_tie_break_by_declaration_index() {
    let a_first = nstruct(
        0,
        0,
        1,
        vec![
            nfield("y", 1, NativeLayoutNode::Unit { offset: 0 }),
            nfield("x", 0, NativeLayoutNode::Unit { offset: 0 }),
        ],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x02, 0, 0, 1);
    expected.extend_from_slice(&2u32.to_le_bytes());
    put_str(&mut expected, "x");
    expected.extend_from_slice(&0u32.to_le_bytes());
    header(&mut expected, 0x0E, 0, 0, 1); // unit: size 0 align 1 by rule
    put_str(&mut expected, "y");
    expected.extend_from_slice(&1u32.to_le_bytes());
    header(&mut expected, 0x0E, 0, 0, 1);
    assert_eq!(dsnl_bytes(&a_first).unwrap(), expected);
}

#[test]
fn enum_bytes_pinned_direct_tag_variants_in_declaration_order() {
    // enum E { A(u32), B } — repr with tag at 0, size 4; payload u32 at 4.
    let payload_a = nstruct(0, 8, 4, vec![nfield("0", 0, scalar(4, ScalarKind::U32))]);
    let payload_b = nstruct(0, 4, 4, vec![]);
    // Slice deliberately out of declaration order.
    let node = nenum(
        0,
        8,
        4,
        NativeTagEncoding::Direct { offset: 0, size: 4 },
        vec![
            nvariant("B", 1, NativeVariantTag::Direct { value: 1 }, payload_b),
            nvariant("A", 0, NativeVariantTag::Direct { value: 0 }, payload_a),
        ],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x03, 0, 8, 4);
    expected.push(0x00); // tag form Direct
    expected.extend_from_slice(&0u32.to_le_bytes()); // tag offset
    expected.push(4); // tag size
    expected.extend_from_slice(&2u32.to_le_bytes()); // variant count
                                                     // Variant A, declaration index 0.
    put_str(&mut expected, "A");
    expected.extend_from_slice(&0u32.to_le_bytes());
    expected.push(0x00); // tag-info kind Direct
    expected.extend_from_slice(&0u128.to_le_bytes());
    header(&mut expected, 0x02, 0, 8, 4);
    expected.extend_from_slice(&1u32.to_le_bytes());
    put_str(&mut expected, "0");
    expected.extend_from_slice(&0u32.to_le_bytes());
    header(&mut expected, 0x01, 4, 4, 4);
    expected.push(0x04);
    // Variant B, declaration index 1.
    put_str(&mut expected, "B");
    expected.extend_from_slice(&1u32.to_le_bytes());
    expected.push(0x00);
    expected.extend_from_slice(&1u128.to_le_bytes());
    header(&mut expected, 0x02, 0, 4, 4);
    expected.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(dsnl_bytes(&node).unwrap(), expected);
}

#[test]
fn niche_enum_bytes_carry_niche_start_and_variant_tag_info() {
    // Option<Box<u32>>-shaped: niche at offset 0, size 8, start 0.
    let some_payload = nstruct(
        0,
        8,
        8,
        vec![nfield("0", 0, nbox(0, scalar(0, ScalarKind::U32), 0))],
    );
    let none_payload = nstruct(0, 0, 1, vec![]);
    let node = nenum(
        0,
        8,
        8,
        NativeTagEncoding::Niche {
            offset: 0,
            size: 8,
            niche_start: 0,
        },
        vec![
            nvariant(
                "None",
                0,
                NativeVariantTag::Niche { index: 0 },
                none_payload,
            ),
            nvariant("Some", 1, NativeVariantTag::Untagged, some_payload),
        ],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x03, 0, 8, 8);
    expected.push(0x01); // tag form Niche
    expected.extend_from_slice(&0u32.to_le_bytes()); // niche offset
    expected.push(8); // niche size
    expected.extend_from_slice(&0u128.to_le_bytes()); // niche_start
    expected.extend_from_slice(&2u32.to_le_bytes());
    put_str(&mut expected, "None");
    expected.extend_from_slice(&0u32.to_le_bytes());
    expected.push(0x01); // tag-info Niche
    expected.extend_from_slice(&0u32.to_le_bytes()); // niche index
    header(&mut expected, 0x02, 0, 0, 1);
    expected.extend_from_slice(&0u32.to_le_bytes());
    put_str(&mut expected, "Some");
    expected.extend_from_slice(&1u32.to_le_bytes());
    expected.push(0x02); // tag-info Untagged
    header(&mut expected, 0x02, 0, 8, 8);
    expected.extend_from_slice(&1u32.to_le_bytes());
    put_str(&mut expected, "0");
    expected.extend_from_slice(&0u32.to_le_bytes());
    header(&mut expected, 0x08, 0, 8, 8); // BoxPtr
    header(&mut expected, 0x01, 0, 4, 4); // inner u32
    expected.push(0x04);
    assert_eq!(dsnl_bytes(&node).unwrap(), expected);
}

#[test]
fn container_and_leaf_kinds_bytes_pinned() {
    // Map<String, Vec<u8>> field plus str/blob/array/set/skip leaves.
    let map = NativeLayoutNode::Map {
        offset: 0,
        size: 48,
        align: 8,
        key: leak(NativeLayoutNode::Str {
            offset: 0,
            size: 24,
            align: 8,
        }),
        value: leak(nvec(0, scalar(0, ScalarKind::U8), 3)),
        ctor: CtorId(7),
    };
    let mut expected = Vec::new();
    header(&mut expected, 0x07, 0, 48, 8);
    header(&mut expected, 0x0A, 0, 24, 8); // key: Str, no payload
    header(&mut expected, 0x05, 0, 24, 8); // value: Vec
    header(&mut expected, 0x01, 0, 1, 1); // elem u8
    expected.push(0x02);
    assert_eq!(dsnl_bytes(&map).unwrap(), expected);

    let arr = NativeLayoutNode::Array {
        offset: 8,
        size: 12,
        align: 4,
        len: 3,
        stride: 4,
        elem: leak(scalar(0, ScalarKind::F32)),
    };
    let mut expected = Vec::new();
    header(&mut expected, 0x04, 8, 12, 4);
    expected.extend_from_slice(&3u32.to_le_bytes());
    expected.extend_from_slice(&4u32.to_le_bytes());
    header(&mut expected, 0x01, 0, 4, 4);
    expected.push(0x0C); // F32
    assert_eq!(dsnl_bytes(&arr).unwrap(), expected);

    let set = NativeLayoutNode::Set {
        offset: 0,
        size: 48,
        align: 8,
        elem: leak(scalar(0, ScalarKind::U64)),
        ctor: CtorId(0),
    };
    let mut expected = Vec::new();
    header(&mut expected, 0x06, 0, 48, 8);
    header(&mut expected, 0x01, 0, 8, 8);
    expected.push(0x05); // U64
    assert_eq!(dsnl_bytes(&set).unwrap(), expected);

    let blob = NativeLayoutNode::Blob {
        offset: 16,
        size: 32,
        align: 8,
    };
    let mut expected = Vec::new();
    header(&mut expected, 0x0B, 16, 32, 8);
    assert_eq!(dsnl_bytes(&blob).unwrap(), expected);

    let skip = nskip(4, 8, 4, 9);
    let mut expected = Vec::new();
    header(&mut expected, 0x0C, 4, 8, 4); // measured align rides in the header
    assert_eq!(dsnl_bytes(&skip).unwrap(), expected); // and NO writer id
}

#[test]
fn opaque_option_bytes_and_backref_frame_are_pinned() {
    let node = noption(
        0,
        8,
        8,
        NativeLayoutNode::BackRef {
            distance: 0,
            offset: 0,
        },
        3,
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x0F, 0, 8, 8);
    header(&mut expected, 0x0D, 0, 8, 8);
    expected.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(dsnl_bytes(&node).unwrap(), expected);
}

// --- exclusion rules ----------------------------------------------------------

#[test]
fn table_ids_are_excluded_from_the_digest() {
    let a = NativeLayoutNode::Struct {
        offset: 0,
        size: 24,
        align: 8,
        whole_drop: Some(distill_wire::native::DropId(1)),
        fields: leak_slice(vec![nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 5))]),
    };
    let b = NativeLayoutNode::Struct {
        offset: 0,
        size: 24,
        align: 8,
        whole_drop: Some(distill_wire::native::DropId(42)),
        fields: leak_slice(vec![nfield(
            "v",
            0,
            nvec(0, scalar(0, ScalarKind::U32), 17),
        )]),
    };
    assert_eq!(dsnl_hash(&a).unwrap(), dsnl_hash(&b).unwrap());

    let c = NativeLayoutNode::Struct {
        offset: 0,
        size: 24,
        align: 8,
        whole_drop: None,
        fields: leak_slice(vec![nfield("v", 0, nvec(0, scalar(0, ScalarKind::U32), 5))]),
    };
    assert_eq!(dsnl_hash(&a).unwrap(), dsnl_hash(&c).unwrap());
}

#[test]
fn skip_writer_id_is_excluded_but_skip_geometry_is_not() {
    let a = nstruct(0, 16, 8, vec![nfield("s", 0, nskip(0, 8, 8, 0))]);
    let b = nstruct(0, 16, 8, vec![nfield("s", 0, nskip(0, 8, 8, 3))]);
    assert_eq!(dsnl_hash(&a).unwrap(), dsnl_hash(&b).unwrap());

    let c = nstruct(0, 16, 8, vec![nfield("s", 0, nskip(0, 4, 4, 0))]);
    assert_ne!(dsnl_hash(&a).unwrap(), dsnl_hash(&c).unwrap());
}

#[test]
fn names_and_declaration_indexes_are_hashed() {
    let a = nstruct(0, 4, 4, vec![nfield("x", 0, scalar(0, ScalarKind::U32))]);
    let b = nstruct(0, 4, 4, vec![nfield("y", 0, scalar(0, ScalarKind::U32))]);
    assert_ne!(dsnl_hash(&a).unwrap(), dsnl_hash(&b).unwrap());

    let c = nstruct(0, 4, 4, vec![nfield("x", 1, scalar(0, ScalarKind::U32))]);
    assert_ne!(dsnl_hash(&a).unwrap(), dsnl_hash(&c).unwrap());
}

#[test]
fn field_names_are_nfc_normalized() {
    let composed = nstruct(
        0,
        4,
        4,
        vec![nfield("\u{e9}", 0, scalar(0, ScalarKind::U32))],
    );
    let decomposed = nstruct(
        0,
        4,
        4,
        vec![nfield("e\u{301}", 0, scalar(0, ScalarKind::U32))],
    );
    assert_eq!(
        dsnl_hash(&composed).unwrap(),
        dsnl_hash(&decomposed).unwrap()
    );
}

// --- backrefs -------------------------------------------------------------------

#[test]
fn backref_header_carries_referenced_frame_geometry() {
    // struct Node { next: Box<Node> } — size 8, align 8.
    let node = nstruct(
        0,
        8,
        8,
        vec![nfield(
            "next",
            0,
            nbox(
                0,
                NativeLayoutNode::BackRef {
                    distance: 0,
                    offset: 0,
                },
                0,
            ),
        )],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x02, 0, 8, 8);
    expected.extend_from_slice(&1u32.to_le_bytes());
    put_str(&mut expected, "next");
    expected.extend_from_slice(&0u32.to_le_bytes());
    header(&mut expected, 0x08, 0, 8, 8); // BoxPtr
                                          // BackRef header: offset = its own frame-relative origin; size and
                                          // align are the referenced frame's (the Node struct: 8, 8), by rule.
    header(&mut expected, 0x0D, 0, 8, 8);
    expected.extend_from_slice(&0u32.to_le_bytes()); // distance
    assert_eq!(dsnl_bytes(&node).unwrap(), expected);
}

#[test]
fn backref_distance_counts_struct_and_enum_frames_only() {
    // struct Outer { opt: Enum { Some(Box<BackRef -> Outer>) } }
    // Path at the backref: Outer(struct) -> Enum -> payload struct.
    // Wrapper (BoxPtr) does not count. distance 2 = Outer.
    let payload = nstruct(
        0,
        8,
        8,
        vec![nfield(
            "0",
            0,
            nbox(
                0,
                NativeLayoutNode::BackRef {
                    distance: 2,
                    offset: 0,
                },
                0,
            ),
        )],
    );
    let inner_enum = nenum(
        0,
        8,
        8,
        NativeTagEncoding::Niche {
            offset: 0,
            size: 8,
            niche_start: 0,
        },
        vec![nvariant("Some", 0, NativeVariantTag::Untagged, payload)],
    );
    let outer = nstruct(0, 8, 8, vec![nfield("opt", 0, inner_enum)]);
    let bytes = dsnl_bytes(&outer).unwrap();
    // The backref header must carry Outer's geometry (8, 8): find the
    // 0x0D node at the tail: kind, offset 0, size 8, align 8, distance 2.
    let tail = &bytes[bytes.len() - 17..];
    assert_eq!(tail[0], 0x0D);
    assert_eq!(&tail[1..5], &0u32.to_le_bytes());
    assert_eq!(&tail[5..9], &8u32.to_le_bytes());
    assert_eq!(&tail[9..13], &8u32.to_le_bytes());
    assert_eq!(&tail[13..17], &2u32.to_le_bytes());
}

#[test]
fn backref_out_of_range_is_an_error() {
    let node = nstruct(
        0,
        8,
        8,
        vec![nfield(
            "next",
            0,
            nbox(
                0,
                NativeLayoutNode::BackRef {
                    distance: 1,
                    offset: 0,
                },
                0,
            ),
        )],
    );
    assert!(matches!(
        dsnl_bytes(&node),
        Err(LayoutHashError::BackRefOutOfRange {
            distance: 1,
            frames: 1
        })
    ));
}

#[test]
fn unit_node_is_size_zero_align_one_by_rule() {
    let node = NativeLayoutNode::Unit { offset: 12 };
    let mut expected = Vec::new();
    header(&mut expected, 0x0E, 12, 0, 1);
    assert_eq!(dsnl_bytes(&node).unwrap(), expected);
}

#[test]
fn skip_default_id_type_exists_with_pinned_shape() {
    // The declared table-id types (§12) — constructible, comparable.
    let s = SkipDefaultId(3);
    assert_eq!(s.0, 3);
}
