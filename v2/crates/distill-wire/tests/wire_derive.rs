//! Wire-layout derivation from the layout schema (§12 "Wire layout,
//! precisely"): flat data keeps native geometry, indirection slots keep
//! size/align, skip slots vanish and force repacking, and enums take
//! exactly one of the three pinned forms.

mod common;

use common::*;
use distill_wire::derive::{derive_wire, DeriveError};
use distill_wire::dswl::dswl_hash;
use distill_wire::native::ScalarKind;
use distill_wire::wire::{SlotKind, WireEnumForm, WireNode};
use ngp_schema::{PrimitiveType, SchemaTypeId, TagEncoding};

fn derive(s: &ngp_schema::Schema, root: usize) -> Result<WireNode, DeriveError> {
    derive_wire(view(s), SchemaTypeId(root))
}

// --- flat data -----------------------------------------------------------------

#[test]
fn flat_struct_keeps_native_offsets_and_geometry() {
    let s = schema(vec![
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
    let wire = derive(&s, 2).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            8,
            4,
            vec![
                wfield("a", 0, wprim(0, ScalarKind::U32)),
                wfield("b", 1, wprim(4, ScalarKind::U16)),
            ]
        )
    );
}

#[test]
fn flat_struct_preserves_rustc_gaps_when_nothing_diverges() {
    // Deliberate native gap at 4..8: without divergence the wire keeps it
    // (gaps are zero on the wire; the encoder writes field-by-field).
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        strukt(
            1,
            "S",
            12,
            4,
            vec![(named_field("a", 0), 0), (named_field("b", 0), 8)],
        ),
    ]);
    let wire = derive(&s, 1).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            12,
            4,
            vec![
                wfield("a", 0, wprim(0, ScalarKind::U32)),
                wfield("b", 1, wprim(8, ScalarKind::U32)),
            ]
        )
    );
}

#[test]
fn tuple_fields_use_decimal_index_names() {
    let s = schema(vec![
        prim(0, PrimitiveType::U8),
        record(
            1,
            PrimitiveType::Tuple,
            None,
            "",
            2,
            1,
            vec![(tuple_field(0, 0), 0), (tuple_field(1, 0), 1)],
        ),
    ]);
    let wire = derive(&s, 1).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            2,
            1,
            vec![
                wfield("0", 0, wprim(0, ScalarKind::U8)),
                wfield("1", 1, wprim(1, ScalarKind::U8)),
            ]
        )
    );
}

#[test]
fn unit_fields_are_unit_nodes() {
    let s = schema(vec![
        prim(0, PrimitiveType::Unit),
        prim(1, PrimitiveType::U32),
        strukt(
            2,
            "S",
            4,
            4,
            vec![(named_field("u", 0), 0), (named_field("x", 1), 0)],
        ),
    ]);
    let wire = derive(&s, 2).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            4,
            4,
            vec![
                wfield("u", 0, WireNode::Unit { offset: 0 }),
                wfield("x", 1, wprim(0, ScalarKind::U32)),
            ]
        )
    );
}

// --- skip slots ------------------------------------------------------------------

#[test]
fn skip_fields_vanish_and_force_repacking() {
    // native: a u32 @0, b u32 @4, s u64 @8 (skip); size 16 align 8.
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U64),
        strukt(
            2,
            "S",
            16,
            8,
            vec![
                (named_field("a", 0), 0),
                (named_field("b", 0), 4),
                (skip_field("s", 1), 8),
            ],
        ),
    ]);
    let wire = derive(&s, 2).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            8,
            4,
            vec![
                wfield("a", 0, wprim(0, ScalarKind::U32)),
                wfield("b", 1, wprim(4, ScalarKind::U32)),
            ]
        )
    );
}

#[test]
fn wire_layout_is_independent_of_skip_slot_geometry() {
    // The §12 consumer-relayout premise: enlarging a skip field leaves
    // wire bytes untouched.
    let small = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U64),
        strukt(
            2,
            "S",
            16,
            8,
            vec![
                (named_field("a", 0), 0),
                (named_field("b", 0), 4),
                (skip_field("s", 1), 8),
            ],
        ),
    ]);
    let big = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U128),
        strukt(
            2,
            "S",
            32,
            16,
            vec![
                (named_field("a", 0), 0),
                (named_field("b", 0), 4),
                (skip_field("s", 1), 16),
            ],
        ),
    ]);
    let wire_small = derive(&small, 2).unwrap();
    let wire_big = derive(&big, 2).unwrap();
    assert_eq!(wire_small, wire_big);
    assert_eq!(
        dswl_hash(&wire_small).unwrap(),
        dswl_hash(&wire_big).unwrap()
    );
}

// --- indirection slots -------------------------------------------------------------

#[test]
fn string_field_is_a_slot_with_native_geometry() {
    let s = schema(vec![
        string_ty(0),
        strukt(1, "S", 24, 8, vec![(named_field("name", 0), 0)]),
    ]);
    let wire = derive(&s, 1).unwrap();
    assert_eq!(
        wire,
        wstruct(0, 24, 8, vec![wfield("name", 0, wstring_slot(0))])
    );
}

#[test]
fn vec_of_struct_derives_pointee_frame_relative_to_the_element() {
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        strukt(
            1,
            "Elem",
            8,
            4,
            vec![(named_field("x", 0), 0), (named_field("y", 0), 4)],
        ),
        vec_ty(2, 1),
        strukt(3, "S", 24, 8, vec![(named_field("items", 2), 0)]),
    ]);
    let wire = derive(&s, 3).unwrap();
    let elem = wstruct(
        0,
        8,
        4,
        vec![
            wfield("x", 0, wprim(0, ScalarKind::U32)),
            wfield("y", 1, wprim(4, ScalarKind::U32)),
        ],
    );
    assert_eq!(
        wire,
        wstruct(0, 24, 8, vec![wfield("items", 0, wvec_slot(0, elem))])
    );
}

#[test]
fn box_and_arc_derive_as_slots_with_inner_pointee() {
    let s = schema(vec![
        prim(0, PrimitiveType::F64),
        box_ty(1, 0),
        arc_ty(2, 0),
        strukt(
            3,
            "S",
            16,
            8,
            vec![(named_field("b", 1), 0), (named_field("a", 2), 8)],
        ),
    ]);
    let wire = derive(&s, 3).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            16,
            8,
            vec![
                wfield(
                    "b",
                    0,
                    wslot(0, 8, 8, SlotKind::Box, vec![wprim(0, ScalarKind::F64)])
                ),
                wfield(
                    "a",
                    1,
                    wslot(8, 8, 8, SlotKind::Arc, vec![wprim(0, ScalarKind::F64)])
                ),
            ]
        )
    );
}

#[test]
fn hashed_map_requires_the_fixed_state_hasher() {
    let ok = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U64),
        fixed_state_ty(2),
        hashmap_ty(3, 0, 1, Some(2)),
    ]);
    let wire = derive(&ok, 3).unwrap();
    assert_eq!(
        wire,
        wslot(
            0,
            48,
            8,
            SlotKind::Map,
            vec![wprim(0, ScalarKind::U32), wprim(0, ScalarKind::U64),]
        )
    );

    let missing = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U64),
        hashmap_ty(2, 0, 1, None),
    ]);
    assert!(matches!(
        derive(&missing, 2),
        Err(DeriveError::NondeterministicHasher { .. })
    ));

    let wrong = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U64),
        hashmap_ty(2, 0, 1, Some(0)),
    ]);
    assert!(matches!(
        derive(&wrong, 2),
        Err(DeriveError::NondeterministicHasher { .. })
    ));
}

#[test]
fn btree_containers_need_no_hasher() {
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        string_ty(1),
        btreemap_ty(2, 0, 1),
    ]);
    let wire = derive(&s, 2).unwrap();
    assert_eq!(
        wire,
        wslot(
            0,
            24,
            8,
            SlotKind::Map,
            vec![wprim(0, ScalarKind::U32), wstring_slot(0),]
        )
    );
}

#[test]
fn hashed_set_requires_the_fixed_state_hasher() {
    let ok = schema(vec![
        prim(0, PrimitiveType::U16),
        fixed_state_ty(1),
        hashset_ty(2, 0, Some(1)),
    ]);
    assert_eq!(
        derive(&ok, 2).unwrap(),
        wslot(0, 48, 8, SlotKind::Set, vec![wprim(0, ScalarKind::U16)])
    );

    let missing = schema(vec![prim(0, PrimitiveType::U16), hashset_ty(1, 0, None)]);
    assert!(matches!(
        derive(&missing, 1),
        Err(DeriveError::NondeterministicHasher { .. })
    ));
}

// --- blobs ---------------------------------------------------------------------------

#[test]
fn blob_fields_become_blob_slots_with_the_field_types_geometry() {
    let s = schema(vec![
        strukt(0, "Blob", 32, 8, vec![]),
        prim(1, PrimitiveType::U32),
        strukt(
            2,
            "S",
            40,
            8,
            vec![(blob_field("data", 0), 0), (named_field("x", 1), 32)],
        ),
    ]);
    let wire = derive(&s, 2).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            40,
            8,
            vec![
                wfield("data", 0, wslot(0, 32, 8, SlotKind::Blob, vec![])),
                wfield("x", 1, wprim(32, ScalarKind::U32)),
            ]
        )
    );
}

#[test]
fn blobs_are_barred_beneath_map_keys_and_set_elements() {
    // Key struct carrying a blob field.
    let keyed = schema(vec![
        strukt(0, "Blob", 32, 8, vec![]),
        strukt(1, "K", 32, 8, vec![(blob_field("data", 0), 0)]),
        prim(2, PrimitiveType::U32),
        fixed_state_ty(3),
        hashmap_ty(4, 1, 2, Some(3)),
    ]);
    assert!(matches!(
        derive(&keyed, 4),
        Err(DeriveError::BlobUnderOrderedKey { .. })
    ));

    let set = schema(vec![
        strukt(0, "Blob", 32, 8, vec![]),
        strukt(1, "E", 32, 8, vec![(blob_field("data", 0), 0)]),
        fixed_state_ty(2),
        hashset_ty(3, 1, Some(2)),
    ]);
    assert!(matches!(
        derive(&set, 3),
        Err(DeriveError::BlobUnderOrderedKey { .. })
    ));

    // Map VALUES may contain blobs freely.
    let valued = schema(vec![
        strukt(0, "Blob", 32, 8, vec![]),
        strukt(1, "V", 32, 8, vec![(blob_field("data", 0), 0)]),
        prim(2, PrimitiveType::U32),
        fixed_state_ty(3),
        hashmap_ty(4, 2, 1, Some(3)),
    ]);
    assert!(derive(&valued, 4).is_ok());

    // ... but not when the map itself sits beneath a set element.
    let nested = schema(vec![
        strukt(0, "Blob", 32, 8, vec![]),
        strukt(1, "V", 32, 8, vec![(blob_field("data", 0), 0)]),
        prim(2, PrimitiveType::U32),
        fixed_state_ty(3),
        hashmap_ty(4, 2, 1, Some(3)),
        strukt(5, "E", 48, 8, vec![(named_field("m", 4), 0)]),
        hashset_ty(6, 5, Some(3)),
    ]);
    assert!(matches!(
        derive(&nested, 6),
        Err(DeriveError::BlobUnderOrderedKey { .. })
    ));
}

// --- arrays -----------------------------------------------------------------------

#[test]
fn flat_array_keeps_native_stride() {
    let s = schema(vec![
        prim(0, PrimitiveType::U16),
        array_ty(1, 3, 0),
        strukt(
            2,
            "S",
            8,
            2,
            vec![(named_field("arr", 1), 0), (named_field("c", 0), 6)],
        ),
    ]);
    let wire = derive(&s, 2).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            8,
            2,
            vec![
                wfield(
                    "arr",
                    0,
                    WireNode::Array {
                        offset: 0,
                        size: 6,
                        align: 2,
                        len: 3,
                        stride: 2,
                        elem: Box::new(wprim(0, ScalarKind::U16)),
                    }
                ),
                wfield("c", 1, wprim(6, ScalarKind::U16)),
            ]
        )
    );
}

#[test]
fn array_of_diverging_elements_recomputes_stride_and_forces_parent_repack() {
    // Element: { x: u32 @0, s: u64 @8 (skip) } native (16, 8) → wire (4, 4).
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U64),
        strukt(
            2,
            "Elem",
            16,
            8,
            vec![(named_field("x", 0), 0), (skip_field("s", 1), 8)],
        ),
        array_ty(3, 2, 2),
        strukt(
            4,
            "S",
            40,
            8,
            vec![(named_field("arr", 3), 0), (named_field("tail", 0), 32)],
        ),
    ]);
    let wire = derive(&s, 4).unwrap();
    let elem = wstruct(0, 4, 4, vec![wfield("x", 0, wprim(0, ScalarKind::U32))]);
    assert_eq!(
        wire,
        wstruct(
            0,
            12,
            4,
            vec![
                wfield(
                    "arr",
                    0,
                    WireNode::Array {
                        offset: 0,
                        size: 8,
                        align: 4,
                        len: 2,
                        stride: 4,
                        elem: Box::new(elem),
                    }
                ),
                wfield("tail", 1, wprim(8, ScalarKind::U32)),
            ]
        )
    );
}

// --- the three enum forms ------------------------------------------------------------

#[test]
fn single_variant_enum_is_the_payload_with_no_tag() {
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        variant_ty(1, "One", "Only", 4, 4, vec![(tuple_field(0, 0), 0)]),
        enum_ty(
            2,
            "One",
            4,
            4,
            TagEncoding::Single { variant_index: 0 },
            vec![("Only", 1)],
        ),
    ]);
    let wire = derive(&s, 2).unwrap();
    assert_eq!(
        wire,
        wenum(
            0,
            4,
            4,
            WireEnumForm::Single,
            vec![wvariant(
                "Only",
                0,
                wstruct(0, 4, 4, vec![wfield("0", 0, wprim(0, ScalarKind::U32))])
            )]
        )
    );
}

#[test]
fn direct_tag_with_flat_payloads_is_fully_flat_in_place() {
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        variant_ty(1, "E", "A", 8, 4, vec![(tuple_field(0, 0), 4)]),
        variant_ty(2, "E", "B", 4, 4, vec![]),
        enum_ty(
            3,
            "E",
            8,
            4,
            TagEncoding::Direct {
                tag_size: 4,
                tag_offset: 0,
                variant_values: vec![5, 9],
            },
            vec![("A", 1), ("B", 2)],
        ),
    ]);
    let wire = derive(&s, 3).unwrap();
    assert_eq!(
        wire,
        wenum(
            0,
            8,
            4,
            WireEnumForm::FullyFlat {
                tag_offset: 0,
                tag_size: 4
            },
            vec![
                wvariant(
                    "A",
                    5,
                    wstruct(0, 8, 4, vec![wfield("0", 0, wprim(4, ScalarKind::U32))])
                ),
                wvariant("B", 9, wstruct(0, 4, 4, vec![])),
            ]
        )
    );
}

#[test]
fn niche_encoded_enum_takes_the_canonical_form() {
    // Option<Box<u32>>: niche in the pointer; canonical wire form is
    // tag u32 @0, payload union at align_up(4, 8) = 8, size 16, align 8.
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        box_ty(1, 0),
        variant_ty(2, "Option", "None", 0, 1, vec![]),
        variant_ty(3, "Option", "Some", 8, 8, vec![(tuple_field(0, 1), 0)]),
        {
            let (mut def, layout) = option_ty(
                4,
                8,
                8,
                TagEncoding::Niche {
                    niche_field_offset: 0,
                    niche_field_size: 8,
                    untagged_variant: 1,
                    niche_variants_start: 0,
                    niche_variants_end: 0,
                    niche_start: 0,
                },
                2,
                3,
            );
            def.generic_argument_ids = vec![SchemaTypeId(1)];
            (def, layout)
        },
    ]);
    let wire = derive(&s, 4).unwrap();
    assert_eq!(
        wire,
        wenum(
            0,
            16,
            8,
            WireEnumForm::Canonical,
            vec![
                wvariant("None", 0, wstruct(8, 0, 1, vec![])),
                wvariant(
                    "Some",
                    0,
                    wstruct(
                        8,
                        8,
                        8,
                        vec![wfield(
                            "0",
                            0,
                            wslot(0, 8, 8, SlotKind::Box, vec![wprim(0, ScalarKind::U32)])
                        )]
                    )
                ),
            ]
        )
    );
}

#[test]
fn direct_tag_with_diverging_payload_takes_the_canonical_form() {
    // Variant A's payload carries a skip field → payload wire != native →
    // canonical despite the integer discriminant.
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        prim(1, PrimitiveType::U64),
        variant_ty(
            2,
            "E",
            "A",
            16,
            8,
            vec![(tuple_field(0, 0), 4), (skip_field("s", 1), 8)],
        ),
        variant_ty(3, "E", "B", 4, 4, vec![]),
        enum_ty(
            4,
            "E",
            16,
            8,
            TagEncoding::Direct {
                tag_size: 4,
                tag_offset: 0,
                variant_values: vec![0, 1],
            },
            vec![("A", 2), ("B", 3)],
        ),
    ]);
    let wire = derive(&s, 4).unwrap();
    assert_eq!(
        wire,
        wenum(
            0,
            8,
            4,
            WireEnumForm::Canonical,
            vec![
                wvariant(
                    "A",
                    0,
                    wstruct(4, 4, 4, vec![wfield("0", 0, wprim(0, ScalarKind::U32))])
                ),
                wvariant("B", 0, wstruct(4, 0, 1, vec![])),
            ]
        )
    );
}

#[test]
fn slot_payloads_do_not_diverge_so_the_enum_stays_fully_flat() {
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        vec_ty(1, 0),
        variant_ty(2, "E", "A", 32, 8, vec![(tuple_field(0, 1), 8)]),
        variant_ty(3, "E", "B", 8, 8, vec![]),
        enum_ty(
            4,
            "E",
            32,
            8,
            TagEncoding::Direct {
                tag_size: 8,
                tag_offset: 0,
                variant_values: vec![0, 1],
            },
            vec![("A", 2), ("B", 3)],
        ),
    ]);
    let wire = derive(&s, 4).unwrap();
    match wire {
        WireNode::Enum {
            form, size, align, ..
        } => {
            assert_eq!(
                form,
                WireEnumForm::FullyFlat {
                    tag_offset: 0,
                    tag_size: 8
                }
            );
            assert_eq!((size, align), (32, 8));
        }
        other => panic!("expected enum, got {other:?}"),
    }
}

// --- recursion ------------------------------------------------------------------------

#[test]
fn self_recursion_through_vec_emits_backref_distance_zero() {
    let s = schema(vec![
        prim(0, PrimitiveType::U32),
        strukt(
            1,
            "Node",
            32,
            8,
            vec![
                (named_field("children", 2), 0),
                (named_field("value", 0), 24),
            ],
        ),
        vec_ty(2, 1),
    ]);
    let wire = derive(&s, 1).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            32,
            8,
            vec![
                wfield(
                    "children",
                    0,
                    wvec_slot(
                        0,
                        WireNode::BackRef {
                            distance: 0,
                            offset: 0
                        }
                    )
                ),
                wfield("value", 1, wprim(24, ScalarKind::U32)),
            ]
        )
    );
}

#[test]
fn mutual_recursion_counts_intervening_frames() {
    // A { b: B }, B { link: Box<A> } — from the box pointee, frames on
    // the path are [A, B]; A is distance 1.
    let s = schema(vec![
        strukt(0, "A", 8, 8, vec![(named_field("b", 1), 0)]),
        strukt(1, "B", 8, 8, vec![(named_field("link", 2), 0)]),
        box_ty(2, 0),
    ]);
    let wire = derive(&s, 0).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            8,
            8,
            vec![wfield(
                "b",
                0,
                wstruct(
                    0,
                    8,
                    8,
                    vec![wfield(
                        "link",
                        0,
                        wslot(
                            0,
                            8,
                            8,
                            SlotKind::Box,
                            vec![WireNode::BackRef {
                                distance: 1,
                                offset: 0
                            }]
                        )
                    )]
                )
            )]
        )
    );
}

// --- framework types --------------------------------------------------------------------

#[test]
fn asset_refs_derive_as_their_ordinary_struct_shape() {
    let s = schema(vec![
        prim(0, PrimitiveType::U128),
        prim(1, PrimitiveType::U32),
        {
            let (mut def, layout) = strukt(2, "AssetRef", 16, 8, vec![(named_field("uuid", 0), 0)]);
            def.uuid = Some(ngp_schema::ASSET_REF_UUID);
            def.generic_argument_ids = vec![SchemaTypeId(1)];
            (def, layout)
        },
    ]);
    let wire = derive(&s, 2).unwrap();
    assert_eq!(
        wire,
        wstruct(
            0,
            16,
            8,
            vec![wfield("uuid", 0, wprim(0, ScalarKind::U128))]
        )
    );
}

// --- errors -------------------------------------------------------------------------------

#[test]
fn missing_layout_is_a_typed_error_naming_the_type() {
    let mut entries = vec![prim(0, PrimitiveType::U32)];
    let (def, mut layout) = strukt(1, "S", 4, 4, vec![(named_field("a", 0), 0)]);
    layout.size = None;
    entries.push((def, layout));
    let s = schema(entries);
    match derive(&s, 1) {
        Err(DeriveError::MissingLayout { type_path, .. }) => {
            assert!(type_path.contains('S'), "path: {type_path}")
        }
        other => panic!("expected MissingLayout, got {other:?}"),
    }
}

#[test]
fn missing_field_offset_is_a_typed_error() {
    let mut entries = vec![prim(0, PrimitiveType::U32)];
    let (def, mut layout) = strukt(1, "S", 4, 4, vec![(named_field("a", 0), 0)]);
    layout.fields[0].offset = None;
    entries.push((def, layout));
    let s = schema(entries);
    assert!(matches!(
        derive(&s, 1),
        Err(DeriveError::MissingLayout { .. })
    ));
}

#[test]
fn missing_tag_encoding_is_a_typed_error() {
    let s = schema(vec![variant_ty(0, "E", "A", 4, 4, vec![]), {
        let (def, mut layout) = enum_ty(
            1,
            "E",
            4,
            4,
            TagEncoding::Single { variant_index: 0 },
            vec![("A", 0)],
        );
        layout.tag_encoding = None;
        (def, layout)
    }]);
    assert!(matches!(
        derive(&s, 1),
        Err(DeriveError::MissingTagEncoding { .. })
    ));
}

#[test]
fn unclassifiable_types_are_rejected_never_guessed() {
    let s = schema(vec![
        (
            ty(
                0,
                PrimitiveType::Pointer(ngp_schema::PointerKind::MutPointer),
                None,
                "",
            ),
            tl(8, 8),
        ),
        strukt(1, "S", 8, 8, vec![(named_field("p", 0), 0)]),
    ]);
    assert!(matches!(
        derive(&s, 1),
        Err(DeriveError::Unsupported { .. })
    ));
}

#[test]
fn u32_bounds_are_enforced_at_the_boundary() {
    // A measured size beyond u32 must be rejected, never truncated.
    let s = schema(vec![prim(0, PrimitiveType::U8), {
        let (def, mut layout) = strukt(1, "Huge", 0, 1, vec![(named_field("a", 0), 0)]);
        layout.size = Some(1u64 << 33);
        layout.align = Some(1);
        (def, layout)
    }]);
    assert!(matches!(derive(&s, 1), Err(DeriveError::Bounds { .. })));
}

#[test]
fn variant_value_count_mismatch_is_a_malformed_enum() {
    let s = schema(vec![
        variant_ty(0, "E", "A", 4, 4, vec![]),
        variant_ty(1, "E", "B", 4, 4, vec![]),
        enum_ty(
            2,
            "E",
            4,
            4,
            TagEncoding::Direct {
                tag_size: 4,
                tag_offset: 0,
                variant_values: vec![0], // two variants, one value
            },
            vec![("A", 0), ("B", 1)],
        ),
    ]);
    assert!(matches!(
        derive(&s, 2),
        Err(DeriveError::MalformedEnum { .. })
    ));
}

#[test]
fn indirection_slot_smaller_than_a_varref_is_rejected() {
    let mut entries = vec![prim(0, PrimitiveType::U32)];
    let (def, mut layout) = vec_ty(1, 0);
    layout.size = Some(4); // cannot hold VarRef { offset, len }
    entries.push((def, layout));
    let s = schema(entries);
    assert!(matches!(
        derive(&s, 1),
        Err(DeriveError::SlotTooSmall { .. })
    ));
}
