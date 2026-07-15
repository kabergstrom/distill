//! Fixup-plan compilation (§12): plans from (wire layout, native layout)
//! pairs — flat-copy runs where offsets coincide, typed construction for
//! every pointer-shaped slot, skip defaults, tag switches, recursion.

mod common;

use common::*;
use distill_wire::native::{
    CtorId, DropId, NativeLayoutNode, NativeTagEncoding, NativeVariantTag, ScalarKind,
    SkipDefaultId,
};
use distill_wire::plan::{compile_plans, FixupOp, NativeTagWrite, PlanError, PlanId, WireTagRead};
use distill_wire::wire::{SlotKind, WireEnumForm, WireNode};

// --- flat copies ----------------------------------------------------------------

#[test]
fn coinciding_offsets_merge_into_one_flat_copy_run() {
    let wire = wstruct(
        0,
        8,
        4,
        vec![
            wfield("a", 0, wprim(0, ScalarKind::U32)),
            wfield("b", 1, wprim(4, ScalarKind::U16)),
        ],
    );
    let native = nstruct(
        0,
        8,
        4,
        vec![
            nfield("a", 0, scalar(0, ScalarKind::U32)),
            nfield("b", 1, scalar(4, ScalarKind::U16)),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(compiled.arena.plans.len(), 1);
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![FixupOp::FlatCopy {
            wire: 0..6,
            native: 0
        }]
    );
    assert_eq!(compiled.arena.plans[0].whole_drop, None);
    assert_eq!(compiled.metas[0].wire_size, 8);
    assert_eq!(compiled.metas[0].wire_align, 4);
    assert_eq!(compiled.metas[0].native_size, 8);
    assert_eq!(compiled.metas[0].native_align, 4);
}

#[test]
fn diverging_native_offsets_scatter_copy() {
    // Consumer reordered the fields: same names, swapped native offsets.
    let wire = wstruct(
        0,
        8,
        4,
        vec![
            wfield("a", 0, wprim(0, ScalarKind::U32)),
            wfield("b", 1, wprim(4, ScalarKind::U32)),
        ],
    );
    let native = nstruct(
        0,
        8,
        4,
        vec![
            nfield("b", 0, scalar(0, ScalarKind::U32)),
            nfield("a", 1, scalar(4, ScalarKind::U32)),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            FixupOp::FlatCopy {
                wire: 0..4,
                native: 4
            },
            FixupOp::FlatCopy {
                wire: 4..8,
                native: 0
            },
        ]
    );
}

#[test]
fn gaps_break_flat_copy_runs() {
    // Wire gap at 4..8 (zero padding): runs must not span it.
    let wire = wstruct(
        0,
        12,
        4,
        vec![
            wfield("a", 0, wprim(0, ScalarKind::U32)),
            wfield("b", 1, wprim(8, ScalarKind::U32)),
        ],
    );
    let native = nstruct(
        0,
        12,
        4,
        vec![
            nfield("a", 0, scalar(0, ScalarKind::U32)),
            nfield("b", 1, scalar(8, ScalarKind::U32)),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            FixupOp::FlatCopy {
                wire: 0..4,
                native: 0
            },
            FixupOp::FlatCopy {
                wire: 8..12,
                native: 8
            },
        ]
    );
}

#[test]
fn bool_and_char_get_validate_ops_before_their_run() {
    let wire = wstruct(
        0,
        8,
        4,
        vec![
            wfield("flag", 0, wprim(0, ScalarKind::Bool)),
            wfield("c", 1, wprim(4, ScalarKind::Char)),
        ],
    );
    let native = nstruct(
        0,
        8,
        4,
        vec![
            nfield("flag", 0, scalar(0, ScalarKind::Bool)),
            nfield("c", 1, scalar(4, ScalarKind::Char)),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            // Runs cover initialized value bytes only (§12: padding is
            // never flat-copied), and each validate precedes the run
            // that would initialize its destination.
            FixupOp::ValidateScalar {
                wire: 0,
                kind: ScalarKind::Bool
            },
            FixupOp::FlatCopy {
                wire: 0..1,
                native: 0
            },
            FixupOp::ValidateScalar {
                wire: 4,
                kind: ScalarKind::Char
            },
            FixupOp::FlatCopy {
                wire: 4..8,
                native: 4
            },
        ]
    );
}

#[test]
fn unit_fields_are_no_ops() {
    let wire = wstruct(
        0,
        4,
        4,
        vec![
            wfield("u", 0, WireNode::Unit { offset: 0 }),
            wfield("x", 1, wprim(0, ScalarKind::U32)),
        ],
    );
    let native = nstruct(
        0,
        4,
        4,
        vec![
            nfield("u", 0, NativeLayoutNode::Unit { offset: 0 }),
            nfield("x", 1, scalar(0, ScalarKind::U32)),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![FixupOp::FlatCopy {
            wire: 0..4,
            native: 0
        }]
    );
}

// --- constructions ---------------------------------------------------------------

#[test]
fn string_and_blob_slots_construct() {
    let wire = wstruct(
        0,
        64,
        8,
        vec![
            wfield("name", 0, wstring_slot(0)),
            wfield("data", 1, wslot(24, 32, 8, SlotKind::Blob, vec![])),
        ],
    );
    let native = NativeLayoutNode::Struct {
        offset: 0,
        size: 64,
        align: 8,
        whole_drop: Some(DropId(3)),
        fields: leak_slice(vec![
            nfield(
                "name",
                0,
                NativeLayoutNode::Str {
                    offset: 0,
                    size: 24,
                    align: 8,
                },
            ),
            nfield(
                "data",
                1,
                NativeLayoutNode::Blob {
                    offset: 24,
                    size: 32,
                    align: 8,
                },
            ),
        ]),
    };
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            FixupOp::ConstructString {
                wire_slot: 0,
                native: 0
            },
            FixupOp::ConstructBlob {
                wire_slot: 24,
                native: 24
            },
        ]
    );
    assert_eq!(compiled.arena.plans[0].whole_drop, Some(DropId(3)));
}

#[test]
fn vec_slot_constructs_with_an_element_plan() {
    let wire = wvec_slot(0, wprim(0, ScalarKind::U32));
    let native = nvec(0, scalar(0, ScalarKind::U32), 7);
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![FixupOp::ConstructVec {
            wire_slot: 0,
            native: 0,
            elem: PlanId(1),
            ctor: CtorId(7),
        }]
    );
    assert_eq!(
        compiled.arena.plans[1].ops,
        vec![FixupOp::FlatCopy {
            wire: 0..4,
            native: 0
        }]
    );
    // Element metas drive stride and temp geometry.
    assert_eq!(compiled.metas[1].wire_size, 4);
    assert_eq!(compiled.metas[1].wire_align, 4);
}

#[test]
fn map_slot_constructs_with_key_and_value_plans() {
    let wire = wslot(
        0,
        48,
        8,
        SlotKind::Map,
        vec![wprim(0, ScalarKind::U32), wstring_slot(0)],
    );
    let native = NativeLayoutNode::Map {
        offset: 0,
        size: 48,
        align: 8,
        key: leak(scalar(0, ScalarKind::U32)),
        value: leak(NativeLayoutNode::Str {
            offset: 0,
            size: 24,
            align: 8,
        }),
        ctor: CtorId(2),
    };
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![FixupOp::ConstructMap {
            wire_slot: 0,
            native: 0,
            key: PlanId(1),
            value: PlanId(2),
            ctor: CtorId(2),
        }]
    );
    assert_eq!(
        compiled.arena.plans[2].ops,
        vec![FixupOp::ConstructString {
            wire_slot: 0,
            native: 0
        }]
    );
}

#[test]
fn box_arc_and_set_slots_construct() {
    let wire = wstruct(
        0,
        64,
        8,
        vec![
            wfield(
                "b",
                0,
                wslot(0, 8, 8, SlotKind::Box, vec![wprim(0, ScalarKind::U64)]),
            ),
            wfield(
                "a",
                1,
                wslot(8, 8, 8, SlotKind::Arc, vec![wprim(0, ScalarKind::U8)]),
            ),
            wfield(
                "s",
                2,
                wslot(16, 48, 8, SlotKind::Set, vec![wprim(0, ScalarKind::U16)]),
            ),
        ],
    );
    let native = NativeLayoutNode::Struct {
        offset: 0,
        size: 64,
        align: 8,
        whole_drop: Some(DropId(0)),
        fields: leak_slice(vec![
            nfield("b", 0, nbox(0, scalar(0, ScalarKind::U64), 4)),
            nfield(
                "a",
                1,
                NativeLayoutNode::ArcPtr {
                    offset: 8,
                    size: 8,
                    align: 8,
                    inner: leak(scalar(0, ScalarKind::U8)),
                    ctor: CtorId(5),
                },
            ),
            nfield(
                "s",
                2,
                NativeLayoutNode::Set {
                    offset: 16,
                    size: 48,
                    align: 8,
                    elem: leak(scalar(0, ScalarKind::U16)),
                    ctor: CtorId(6),
                },
            ),
        ]),
    };
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            FixupOp::ConstructBox {
                wire_slot: 0,
                native: 0,
                inner: PlanId(1),
                ctor: CtorId(4)
            },
            FixupOp::ConstructArc {
                wire_slot: 8,
                native: 8,
                inner: PlanId(2),
                ctor: CtorId(5)
            },
            FixupOp::ConstructSet {
                wire_slot: 16,
                native: 16,
                elem: PlanId(3),
                ctor: CtorId(6)
            },
        ]
    );
}

#[test]
fn skip_slots_write_defaults_at_consumer_native_offsets() {
    // Wire has no skip slot; the consumer's native layout places one, and
    // the wire fields match by NAME to shifted native offsets.
    let wire = wstruct(
        0,
        8,
        4,
        vec![
            wfield("a", 0, wprim(0, ScalarKind::U32)),
            wfield("b", 1, wprim(4, ScalarKind::U32)),
        ],
    );
    let native = nstruct(
        0,
        24,
        8,
        vec![
            nfield("s", 1, nskip(0, 8, 8, 2)),
            nfield("a", 0, scalar(8, ScalarKind::U32)),
            nfield("b", 2, scalar(12, ScalarKind::U32)),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            FixupOp::FlatCopy {
                wire: 0..8,
                native: 8
            },
            FixupOp::WriteSkipDefault {
                native: 0,
                writer: SkipDefaultId(2)
            },
        ]
    );
}

// --- nesting -----------------------------------------------------------------------

#[test]
fn flat_nested_structs_inline_into_the_parent_run() {
    let wire = wstruct(
        0,
        8,
        4,
        vec![
            wfield("head", 0, wprim(0, ScalarKind::U32)),
            wfield(
                "inner",
                1,
                wstruct(4, 4, 4, vec![wfield("x", 0, wprim(0, ScalarKind::U32))]),
            ),
        ],
    );
    let native = nstruct(
        0,
        8,
        4,
        vec![
            nfield("head", 0, scalar(0, ScalarKind::U32)),
            nfield(
                "inner",
                1,
                nstruct(4, 4, 4, vec![nfield("x", 0, scalar(0, ScalarKind::U32))]),
            ),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(compiled.arena.plans.len(), 1);
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![FixupOp::FlatCopy {
            wire: 0..8,
            native: 0
        }]
    );
}

#[test]
fn constructed_nested_structs_recurse_into_a_sub_plan() {
    let wire = wstruct(
        0,
        32,
        8,
        vec![
            wfield("head", 0, wprim(24, ScalarKind::U32)),
            wfield(
                "inner",
                1,
                wstruct(0, 24, 8, vec![wfield("s", 0, wstring_slot(0))]),
            ),
        ],
    );
    let native = nstruct(
        0,
        32,
        8,
        vec![
            nfield("head", 0, scalar(24, ScalarKind::U32)),
            nfield("inner", 1, {
                NativeLayoutNode::Struct {
                    offset: 0,
                    size: 24,
                    align: 8,
                    whole_drop: Some(DropId(1)),
                    fields: leak_slice(vec![nfield(
                        "s",
                        0,
                        NativeLayoutNode::Str {
                            offset: 0,
                            size: 24,
                            align: 8,
                        },
                    )]),
                }
            }),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    // Flat-copy runs first (§12), then constructions in native physical order.
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            FixupOp::FlatCopy {
                wire: 24..28,
                native: 24
            },
            FixupOp::Recurse {
                wire: 0,
                native: 0,
                plan: PlanId(1)
            },
        ]
    );
    assert_eq!(
        compiled.arena.plans[1].ops,
        vec![FixupOp::ConstructString {
            wire_slot: 0,
            native: 0
        }]
    );
    assert_eq!(compiled.arena.plans[1].whole_drop, Some(DropId(1)));
}

#[test]
fn flat_nested_struct_with_drop_glue_still_gets_its_own_frame() {
    // A flat aggregate with custom Drop must complete as one value.
    let wire = wstruct(
        0,
        4,
        4,
        vec![wfield(
            "inner",
            0,
            wstruct(0, 4, 4, vec![wfield("x", 0, wprim(0, ScalarKind::U32))]),
        )],
    );
    let native = nstruct(
        0,
        4,
        4,
        vec![nfield("inner", 0, {
            NativeLayoutNode::Struct {
                offset: 0,
                size: 4,
                align: 4,
                whole_drop: Some(DropId(9)),
                fields: leak_slice(vec![nfield("x", 0, scalar(0, ScalarKind::U32))]),
            }
        })],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![FixupOp::Recurse {
            wire: 0,
            native: 0,
            plan: PlanId(1)
        }]
    );
    assert_eq!(
        compiled.arena.plans[1].ops,
        vec![FixupOp::FlatCopy {
            wire: 0..4,
            native: 0
        }]
    );
    assert_eq!(compiled.arena.plans[1].whole_drop, Some(DropId(9)));
}

#[test]
fn flat_arrays_merge_into_one_run_and_constructed_arrays_recurse_per_element() {
    let flat_wire = WireNode::Array {
        offset: 0,
        size: 6,
        align: 2,
        len: 3,
        stride: 2,
        elem: Box::new(wprim(0, ScalarKind::U16)),
    };
    let flat_native = NativeLayoutNode::Array {
        offset: 0,
        size: 6,
        align: 2,
        len: 3,
        stride: 2,
        elem: leak(scalar(0, ScalarKind::U16)),
    };
    let compiled = compile_plans(&flat_wire, &flat_native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![FixupOp::FlatCopy {
            wire: 0..6,
            native: 0
        }]
    );

    let built_wire = WireNode::Array {
        offset: 0,
        size: 48,
        align: 8,
        len: 2,
        stride: 24,
        elem: Box::new(wstring_slot(0)),
    };
    let built_native = NativeLayoutNode::Array {
        offset: 0,
        size: 48,
        align: 8,
        len: 2,
        stride: 24,
        elem: leak(NativeLayoutNode::Str {
            offset: 0,
            size: 24,
            align: 8,
        }),
    };
    let compiled = compile_plans(&built_wire, &built_native).unwrap();
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            FixupOp::Recurse {
                wire: 0,
                native: 0,
                plan: PlanId(1)
            },
            FixupOp::Recurse {
                wire: 24,
                native: 24,
                plan: PlanId(1)
            },
        ]
    );
    assert_eq!(
        compiled.arena.plans[1].ops,
        vec![FixupOp::ConstructString {
            wire_slot: 0,
            native: 0
        }]
    );
}

// --- enums --------------------------------------------------------------------------

#[test]
fn single_variant_enums_emit_no_switch_op_at_all() {
    let wire = wenum(
        0,
        4,
        4,
        WireEnumForm::Single,
        vec![wvariant(
            "Only",
            0,
            wstruct(0, 4, 4, vec![wfield("0", 0, wprim(0, ScalarKind::U32))]),
        )],
    );
    let native = nenum(
        0,
        4,
        4,
        NativeTagEncoding::Single,
        vec![nvariant(
            "Only",
            0,
            NativeVariantTag::Single,
            nstruct(0, 4, 4, vec![nfield("0", 0, scalar(0, ScalarKind::U32))]),
        )],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(compiled.arena.plans.len(), 1);
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![FixupOp::FlatCopy {
            wire: 0..4,
            native: 0
        }]
    );
}

#[test]
fn canonical_wire_tag_switches_into_native_niche_writes() {
    // Wire: canonical Option<Box<u32>>; native: niche-encoded.
    let wire = wenum(
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
                        wslot(0, 8, 8, SlotKind::Box, vec![wprim(0, ScalarKind::U32)]),
                    )],
                ),
            ),
        ],
    );
    let native = nenum(
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
                nstruct(0, 0, 1, vec![]),
            ),
            nvariant(
                "Some",
                1,
                NativeVariantTag::Untagged,
                nstruct(
                    0,
                    8,
                    8,
                    vec![nfield("0", 0, nbox(0, scalar(0, ScalarKind::U32), 3))],
                ),
            ),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    // Root plan is the enum frame: exactly the switch op.
    assert_eq!(compiled.arena.plans[0].ops.len(), 1);
    match &compiled.arena.plans[0].ops[0] {
        FixupOp::SwitchVariant {
            wire_tag,
            native,
            variants,
        } => {
            assert_eq!(*wire_tag, WireTagRead::CanonicalU32 { offset: 0 });
            assert_eq!(*native, 0);
            assert_eq!(variants.len(), 2);
            // Name-sorted: None then Some.
            assert_eq!(
                variants[0].0,
                NativeTagWrite::Niche {
                    offset: 0,
                    size: 8,
                    value: 0
                }
            );
            assert_eq!(variants[1].0, NativeTagWrite::PayloadImplied);
            // Some's payload constructs the box from the canonical payload
            // offset (8) into native offset 0.
            let some_plan = &compiled.arena.plans[variants[1].1 .0 as usize];
            assert_eq!(
                some_plan.ops,
                vec![FixupOp::ConstructBox {
                    wire_slot: 8,
                    native: 0,
                    inner: PlanId(3),
                    ctor: CtorId(3),
                }]
            );
            assert_eq!(some_plan.whole_drop, None); // the enum plan owns the frame
        }
        other => panic!("expected SwitchVariant, got {other:?}"),
    }
}

#[test]
fn option_plan_uses_typed_construction_instead_of_native_tag_writes() {
    let wire = wenum(
        0,
        8,
        4,
        WireEnumForm::Canonical,
        vec![
            wvariant("None", 0, wstruct(4, 0, 1, vec![])),
            wvariant(
                "Some",
                0,
                wstruct(4, 4, 4, vec![wfield("0", 0, wprim(0, ScalarKind::U32))]),
            ),
        ],
    );
    let native = noption(0, 8, 4, scalar(0, ScalarKind::U32), 7);
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(compiled.arena.plans[0].ops.len(), 1);
    assert!(matches!(
        compiled.arena.plans[0].ops[0],
        FixupOp::ConstructOption {
            wire_tag: WireTagRead::CanonicalU32 { offset: 0 },
            native: 0,
            ctor: CtorId(7),
            ..
        }
    ));
}

#[test]
fn fully_flat_wire_tag_reads_direct_values_in_name_sorted_order() {
    // Wire: fully-flat with declared discriminants; native: direct too.
    let wire = wenum(
        0,
        8,
        4,
        WireEnumForm::FullyFlat {
            tag_offset: 0,
            tag_size: 4,
        },
        vec![
            wvariant("B", 9, wstruct(0, 4, 4, vec![])),
            wvariant(
                "A",
                5,
                wstruct(0, 8, 4, vec![wfield("0", 0, wprim(4, ScalarKind::U32))]),
            ),
        ],
    );
    let native = nenum(
        0,
        8,
        4,
        NativeTagEncoding::Direct { offset: 0, size: 4 },
        vec![
            nvariant(
                "A",
                0,
                NativeVariantTag::Direct { value: 11 },
                nstruct(0, 8, 4, vec![nfield("0", 0, scalar(4, ScalarKind::U32))]),
            ),
            nvariant(
                "B",
                1,
                NativeVariantTag::Direct { value: 12 },
                nstruct(0, 4, 4, vec![]),
            ),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    match &compiled.arena.plans[0].ops[0] {
        FixupOp::SwitchVariant {
            wire_tag, variants, ..
        } => {
            assert_eq!(
                *wire_tag,
                WireTagRead::Direct {
                    offset: 0,
                    size: 4,
                    values: vec![5, 9]
                }
            );
            // The native tag is written from the CONSUMER's values, never
            // read back from wire bytes.
            assert_eq!(
                variants[0].0,
                NativeTagWrite::Direct {
                    offset: 0,
                    size: 4,
                    value: 11
                }
            );
            assert_eq!(
                variants[1].0,
                NativeTagWrite::Direct {
                    offset: 0,
                    size: 4,
                    value: 12
                }
            );
        }
        other => panic!("expected SwitchVariant, got {other:?}"),
    }
}

// --- recursion ------------------------------------------------------------------------

#[test]
fn recursive_types_close_cycles_through_indirection_plans() {
    // struct Node { children: Vec<Node>, value: u32 }
    let wire = wstruct(
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
                        offset: 0,
                    },
                ),
            ),
            wfield("value", 1, wprim(24, ScalarKind::U32)),
        ],
    );
    let native = nstruct(
        0,
        32,
        8,
        vec![
            nfield(
                "children",
                0,
                nvec(
                    0,
                    NativeLayoutNode::BackRef {
                        distance: 0,
                        offset: 0,
                    },
                    1,
                ),
            ),
            nfield("value", 1, scalar(24, ScalarKind::U32)),
        ],
    );
    let compiled = compile_plans(&wire, &native).unwrap();
    assert_eq!(compiled.arena.plans.len(), 1);
    assert_eq!(
        compiled.arena.plans[0].ops,
        vec![
            FixupOp::FlatCopy {
                wire: 24..28,
                native: 24
            },
            FixupOp::ConstructVec {
                wire_slot: 0,
                native: 0,
                elem: PlanId(0), // the cycle: element plan IS the root plan
                ctor: CtorId(1),
            },
        ]
    );
}

// --- mismatches --------------------------------------------------------------------------

#[test]
fn field_name_mismatch_is_an_error() {
    let wire = wstruct(0, 4, 4, vec![wfield("a", 0, wprim(0, ScalarKind::U32))]);
    let native = nstruct(0, 4, 4, vec![nfield("b", 0, scalar(0, ScalarKind::U32))]);
    assert!(matches!(
        compile_plans(&wire, &native),
        Err(PlanError::Mismatch { .. })
    ));
}

#[test]
fn scalar_kind_mismatch_is_an_error() {
    let wire = wprim(0, ScalarKind::U32);
    let native = scalar(0, ScalarKind::I32);
    assert!(matches!(
        compile_plans(&wire, &native),
        Err(PlanError::Mismatch { .. })
    ));
}

#[test]
fn variant_set_mismatch_is_an_error() {
    let wire = wenum(
        0,
        4,
        4,
        WireEnumForm::Canonical,
        vec![wvariant("A", 0, wstruct(4, 0, 1, vec![]))],
    );
    let native = nenum(
        0,
        1,
        1,
        NativeTagEncoding::Direct { offset: 0, size: 1 },
        vec![
            nvariant(
                "A",
                0,
                NativeVariantTag::Direct { value: 0 },
                nstruct(0, 0, 1, vec![]),
            ),
            nvariant(
                "B",
                1,
                NativeVariantTag::Direct { value: 1 },
                nstruct(0, 0, 1, vec![]),
            ),
        ],
    );
    assert!(matches!(
        compile_plans(&wire, &native),
        Err(PlanError::Mismatch { .. })
    ));
}

#[test]
fn array_length_mismatch_is_an_error() {
    let wire = WireNode::Array {
        offset: 0,
        size: 4,
        align: 2,
        len: 2,
        stride: 2,
        elem: Box::new(wprim(0, ScalarKind::U16)),
    };
    let native = NativeLayoutNode::Array {
        offset: 0,
        size: 6,
        align: 2,
        len: 3,
        stride: 2,
        elem: leak(scalar(0, ScalarKind::U16)),
    };
    assert!(matches!(
        compile_plans(&wire, &native),
        Err(PlanError::Mismatch { .. })
    ));
}

#[test]
fn slot_kind_mismatch_is_an_error() {
    let wire = wstring_slot(0);
    let native = nvec(0, scalar(0, ScalarKind::U8), 0);
    assert!(matches!(
        compile_plans(&wire, &native),
        Err(PlanError::Mismatch { .. })
    ));
}
