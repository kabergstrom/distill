//! DSWL — the wire-layout hash (§12): grammar bytes pinned, physical
//! order with declaration-index tie-break, semantic identity in payloads,
//! all three enum forms, pointee inclusion, backrefs.

mod common;

use common::*;
use distill_wire::dswl::{decode_dswl, dswl_bytes, dswl_hash, DswlDecodeError, DSWL_VERSION};
use distill_wire::native::{LayoutHashError, ScalarKind};
use distill_wire::wire::{SlotKind, WireEnumForm, WireNode};

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

// --- pinned grammar bytes ----------------------------------------------------

#[test]
fn primitive_bytes_pinned_and_hash_domain_prefixed() {
    let node = wprim(4, ScalarKind::U32);
    let mut expected = Vec::new();
    header(&mut expected, 0x01, 4, 4, 4);
    expected.push(0x04); // ScalarKind::U32
    assert_eq!(dswl_bytes(&node).unwrap(), expected);

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DSWL");
    hasher.update(&[DSWL_VERSION]);
    hasher.update(&expected);
    assert_eq!(dswl_hash(&node).unwrap().0, *hasher.finalize().as_bytes());
}

#[test]
fn vec_slot_bytes_carry_slot_kind_and_pointee_node() {
    let node = wvec_slot(8, wprim(0, ScalarKind::U16));
    let mut expected = Vec::new();
    header(&mut expected, 0x05, 8, 24, 8);
    expected.push(0x00); // SlotKind::Vec
    header(&mut expected, 0x01, 0, 2, 2); // pointee, frame-relative to the element
    expected.push(0x03); // U16
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

#[test]
fn map_slot_bytes_carry_key_then_value() {
    let node = wslot(
        0,
        48,
        8,
        SlotKind::Map,
        vec![wprim(0, ScalarKind::U32), wstring_slot(0)],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x05, 0, 48, 8);
    expected.push(0x02); // SlotKind::Map
    header(&mut expected, 0x01, 0, 4, 4);
    expected.push(0x04); // key u32
    header(&mut expected, 0x05, 0, 24, 8);
    expected.push(0x01); // value: SlotKind::String, no pointee
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

#[test]
fn string_and_blob_slots_have_no_pointee() {
    let s = wstring_slot(0);
    let mut expected = Vec::new();
    header(&mut expected, 0x05, 0, 24, 8);
    expected.push(0x01);
    assert_eq!(dswl_bytes(&s).unwrap(), expected);

    let b = wslot(16, 32, 8, SlotKind::Blob, vec![]);
    let mut expected = Vec::new();
    header(&mut expected, 0x05, 16, 32, 8);
    expected.push(0x06);
    assert_eq!(dswl_bytes(&b).unwrap(), expected);
}

#[test]
fn array_bytes_pinned() {
    let node = WireNode::Array {
        offset: 4,
        size: 8,
        align: 2,
        len: 4,
        stride: 2,
        elem: Box::new(wprim(0, ScalarKind::I16)),
    };
    let mut expected = Vec::new();
    header(&mut expected, 0x04, 4, 8, 2);
    expected.extend_from_slice(&4u32.to_le_bytes());
    expected.extend_from_slice(&2u32.to_le_bytes());
    header(&mut expected, 0x01, 0, 2, 2);
    expected.push(0x08); // I16
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

#[test]
fn unit_bytes_pinned() {
    let node = WireNode::Unit { offset: 3 };
    let mut expected = Vec::new();
    header(&mut expected, 0x06, 3, 0, 1);
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

// --- struct ordering and semantic identity ------------------------------------

#[test]
fn struct_fields_serialize_in_wire_offset_order_without_declaration_index() {
    let node = wstruct(
        0,
        8,
        4,
        vec![
            wfield("b", 1, wprim(4, ScalarKind::U16)),
            wfield("a", 0, wprim(0, ScalarKind::U32)),
        ],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x02, 0, 8, 4);
    expected.extend_from_slice(&2u32.to_le_bytes());
    put_str(&mut expected, "a"); // offset 0 first; per-field payload is (name, node)
    header(&mut expected, 0x01, 0, 4, 4);
    expected.push(0x04);
    put_str(&mut expected, "b");
    header(&mut expected, 0x01, 4, 2, 2);
    expected.push(0x03);
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

#[test]
fn equal_wire_offsets_tie_break_by_declaration_index() {
    // Two ZST fields at wire offset 0: order is by declaration index.
    let a = wstruct(
        0,
        0,
        1,
        vec![
            wfield("y", 1, WireNode::Unit { offset: 0 }),
            wfield("x", 0, WireNode::Unit { offset: 0 }),
        ],
    );
    let b = wstruct(
        0,
        0,
        1,
        vec![
            wfield("x", 0, WireNode::Unit { offset: 0 }),
            wfield("y", 1, WireNode::Unit { offset: 0 }),
        ],
    );
    assert_eq!(dswl_bytes(&a).unwrap(), dswl_bytes(&b).unwrap());

    // Swapping the declaration indexes swaps the serialized order.
    let c = wstruct(
        0,
        0,
        1,
        vec![
            wfield("y", 0, WireNode::Unit { offset: 0 }),
            wfield("x", 1, WireNode::Unit { offset: 0 }),
        ],
    );
    assert_ne!(dswl_bytes(&a).unwrap(), dswl_bytes(&c).unwrap());
}

#[test]
fn swapping_two_same_typed_field_names_changes_the_hash() {
    // Same geometry, names swapped: must never hash equal (§12 — a cached
    // artifact would flat-copy with fields silently swapped).
    let a = wstruct(
        0,
        8,
        4,
        vec![
            wfield("hp", 0, wprim(0, ScalarKind::U32)),
            wfield("mp", 1, wprim(4, ScalarKind::U32)),
        ],
    );
    let b = wstruct(
        0,
        8,
        4,
        vec![
            wfield("mp", 0, wprim(0, ScalarKind::U32)),
            wfield("hp", 1, wprim(4, ScalarKind::U32)),
        ],
    );
    assert_ne!(dswl_hash(&a).unwrap(), dswl_hash(&b).unwrap());
}

#[test]
fn pointee_relayout_changes_the_hash_for_vec_and_set() {
    // The slot itself stays 8 opaque bytes; the element node must still
    // participate (§12: a set-element relayout under an unchanged logical
    // hash must change the layout hash).
    let vec_u32 = wvec_slot(0, wprim(0, ScalarKind::U32));
    let vec_i32 = wvec_slot(0, wprim(0, ScalarKind::I32));
    assert_ne!(dswl_hash(&vec_u32).unwrap(), dswl_hash(&vec_i32).unwrap());

    let set_a = wslot(0, 48, 8, SlotKind::Set, vec![wprim(0, ScalarKind::U8)]);
    let set_b = wslot(0, 48, 8, SlotKind::Set, vec![wprim(0, ScalarKind::U16)]);
    assert_ne!(dswl_hash(&set_a).unwrap(), dswl_hash(&set_b).unwrap());
}

// --- enum forms ----------------------------------------------------------------

#[test]
fn single_variant_enum_bytes_pinned() {
    let node = wenum(
        0,
        4,
        4,
        WireEnumForm::Single,
        vec![wvariant(
            "Only",
            0,
            wstruct(0, 4, 4, vec![wfield("0", 0, wprim(0, ScalarKind::F32))]),
        )],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x03, 0, 4, 4);
    expected.push(0x00); // form Single
    expected.extend_from_slice(&1u32.to_le_bytes()); // variant count
    put_str(&mut expected, "Only");
    header(&mut expected, 0x02, 0, 4, 4);
    expected.extend_from_slice(&1u32.to_le_bytes());
    put_str(&mut expected, "0");
    header(&mut expected, 0x01, 0, 4, 4);
    expected.push(0x0C);
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

#[test]
fn fully_flat_enum_bytes_carry_tag_geometry_and_discriminants_name_sorted() {
    // Variants supplied out of name order; serialized name-sorted.
    let node = wenum(
        0,
        8,
        4,
        WireEnumForm::FullyFlat {
            tag_offset: 0,
            tag_size: 4,
        },
        vec![
            wvariant("B", 7, wstruct(0, 4, 4, vec![])),
            wvariant(
                "A",
                3,
                wstruct(0, 8, 4, vec![wfield("0", 0, wprim(4, ScalarKind::U32))]),
            ),
        ],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x03, 0, 8, 4);
    expected.push(0x01); // form FullyFlat
    expected.extend_from_slice(&0u32.to_le_bytes()); // native tag offset
    expected.push(4); // tag size
    expected.extend_from_slice(&2u32.to_le_bytes());
    put_str(&mut expected, "A");
    expected.extend_from_slice(&3u128.to_le_bytes()); // discriminant raw bits
    header(&mut expected, 0x02, 0, 8, 4);
    expected.extend_from_slice(&1u32.to_le_bytes());
    put_str(&mut expected, "0");
    header(&mut expected, 0x01, 4, 4, 4);
    expected.push(0x04);
    put_str(&mut expected, "B");
    expected.extend_from_slice(&7u128.to_le_bytes());
    header(&mut expected, 0x02, 0, 4, 4);
    expected.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

#[test]
fn canonical_enum_bytes_omit_discriminants() {
    let node = wenum(
        0,
        12,
        4,
        WireEnumForm::Canonical,
        vec![
            wvariant("Two", 0, wstruct(4, 8, 4, vec![])),
            wvariant("One", 0, wstruct(4, 4, 4, vec![])),
        ],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x03, 0, 12, 4);
    expected.push(0x02); // form Canonical
    expected.extend_from_slice(&2u32.to_le_bytes());
    put_str(&mut expected, "One"); // name-sorted
    header(&mut expected, 0x02, 4, 4, 4);
    expected.extend_from_slice(&0u32.to_le_bytes());
    put_str(&mut expected, "Two");
    header(&mut expected, 0x02, 4, 8, 4);
    expected.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

#[test]
fn reassigning_discriminants_changes_the_hash_with_geometry_unchanged() {
    let mk = |a: u128, b: u128| {
        wenum(
            0,
            4,
            4,
            WireEnumForm::FullyFlat {
                tag_offset: 0,
                tag_size: 4,
            },
            vec![
                wvariant("A", a, wstruct(0, 4, 4, vec![])),
                wvariant("B", b, wstruct(0, 4, 4, vec![])),
            ],
        )
    };
    assert_ne!(dswl_hash(&mk(0, 1)).unwrap(), dswl_hash(&mk(1, 0)).unwrap());
}

#[test]
fn duplicate_variant_names_are_an_error() {
    let node = wenum(
        0,
        4,
        4,
        WireEnumForm::Canonical,
        vec![
            wvariant("A", 0, wstruct(4, 0, 1, vec![])),
            wvariant("A", 1, wstruct(4, 0, 1, vec![])),
        ],
    );
    assert!(matches!(
        dswl_bytes(&node),
        Err(LayoutHashError::DuplicateName(_))
    ));
}

#[test]
fn variant_names_sort_by_nfc_bytes() {
    let composed = wenum(
        0,
        4,
        4,
        WireEnumForm::Canonical,
        vec![wvariant("\u{e9}", 0, wstruct(4, 0, 1, vec![]))],
    );
    let decomposed = wenum(
        0,
        4,
        4,
        WireEnumForm::Canonical,
        vec![wvariant("e\u{301}", 0, wstruct(4, 0, 1, vec![]))],
    );
    assert_eq!(
        dswl_hash(&composed).unwrap(),
        dswl_hash(&decomposed).unwrap()
    );
}

// --- backrefs -------------------------------------------------------------------

#[test]
fn backref_header_carries_referenced_wire_frame_geometry() {
    // struct Node { next: Box<Node> } on the wire: the pointee re-enters
    // Node, so the box's pointee is a backref to the struct frame.
    let node = wstruct(
        0,
        8,
        8,
        vec![wfield(
            "next",
            0,
            wslot(
                0,
                8,
                8,
                SlotKind::Box,
                vec![WireNode::BackRef {
                    distance: 0,
                    offset: 0,
                }],
            ),
        )],
    );
    let mut expected = Vec::new();
    header(&mut expected, 0x02, 0, 8, 8);
    expected.extend_from_slice(&1u32.to_le_bytes());
    put_str(&mut expected, "next");
    header(&mut expected, 0x05, 0, 8, 8);
    expected.push(0x04); // SlotKind::Box
    header(&mut expected, 0x07, 0, 8, 8); // backref: referenced frame's size/align
    expected.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(dswl_bytes(&node).unwrap(), expected);
}

#[test]
fn backref_out_of_range_is_an_error() {
    let node = WireNode::BackRef {
        distance: 0,
        offset: 0,
    };
    assert!(matches!(
        dswl_bytes(&node),
        Err(LayoutHashError::BackRefOutOfRange {
            distance: 0,
            frames: 0
        })
    ));
}

#[test]
fn authenticated_body_decodes_to_the_same_canonical_tree_bytes() {
    let node = wstruct(
        0,
        32,
        8,
        vec![
            wfield("count", 0, wprim(0, ScalarKind::U32)),
            wfield("items", 1, wvec_slot(8, wprim(0, ScalarKind::U16))),
        ],
    );
    let bytes = dswl_bytes(&node).unwrap();
    let decoded = decode_dswl(&bytes).unwrap();
    assert_eq!(dswl_bytes(&decoded).unwrap(), bytes);
    for end in 0..bytes.len() {
        assert!(
            decode_dswl(&bytes[..end]).is_err(),
            "accepted truncation {end}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(decode_dswl(&trailing), Err(DswlDecodeError::TrailingBytes));
}

#[test]
fn decoder_rejects_unknown_kinds_and_false_backref_geometry() {
    let mut unknown = dswl_bytes(&wprim(0, ScalarKind::U8)).unwrap();
    unknown[0] = 0xff;
    assert_eq!(
        decode_dswl(&unknown),
        Err(DswlDecodeError::UnknownNode(0xff))
    );

    let node = wstruct(
        0,
        8,
        8,
        vec![wfield(
            "next",
            0,
            wslot(
                0,
                8,
                8,
                SlotKind::Box,
                vec![WireNode::BackRef {
                    distance: 0,
                    offset: 0,
                }],
            ),
        )],
    );
    let mut bytes = dswl_bytes(&node).unwrap();
    let backref = bytes.iter().rposition(|byte| *byte == 0x07).unwrap();
    bytes[backref + 5] ^= 1;
    assert_eq!(decode_dswl(&bytes), Err(DswlDecodeError::BackRefGeometry));
}
