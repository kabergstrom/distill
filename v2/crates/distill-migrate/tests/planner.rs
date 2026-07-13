//! Automatic planner (§11): structural matching over snapshot ASTs.

mod common;
use common::*;
use distill_migrate::{plan_automatic, MigrationOp};
use ngp_schema::SchemaNode;

fn refuses_at(old: &SchemaNode, new: &SchemaNode, path_part: &str, reason_part: &str) {
    let err = plan_automatic(old, new).expect_err("planner should refuse");
    assert!(
        err.reasons
            .iter()
            .any(|(p, r)| p.contains(path_part) && r.contains(reason_part)),
        "no refusal at path containing {path_part:?} with reason containing \
         {reason_part:?}; got {:?}",
        err.reasons
    );
}

#[test]
fn identical_schemas_plan_one_root_copy() {
    let s = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, SchemaNode::String)]);
    let ops = plan_automatic(&s, &s).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::CopyField {
            from: root(),
            to: root()
        }]
    );
}

#[test]
fn added_plain_field_gets_field_default() {
    let old = strct(0, &[("a", 0, p(PK::U32))]);
    let new = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, SchemaNode::String)]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert!(ops.contains(&MigrationOp::CopyField {
        from: path(&["a"]),
        to: path(&["a"])
    }));
    assert!(ops.contains(&MigrationOp::WriteFieldDefault { to: path(&["b"]) }));
    assert_eq!(ops.len(), 2);
}

#[test]
fn added_option_field_gets_write_none() {
    let old = strct(0, &[("a", 0, p(PK::U32))]);
    let new = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, opt(p(PK::U32)))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert!(ops.contains(&MigrationOp::WriteNone { to: path(&["b"]) }));
}

#[test]
fn removed_field_gets_drop_field() {
    let old = strct(0, &[("a", 0, p(PK::U32)), ("gone", 0, SchemaNode::String)]);
    let new = strct(0, &[("a", 0, p(PK::U32))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert!(ops.contains(&MigrationOp::DropField {
        at: path(&["gone"])
    }));
    assert!(ops.contains(&MigrationOp::CopyField {
        from: path(&["a"]),
        to: path(&["a"])
    }));
    assert_eq!(ops.len(), 2);
}

#[test]
fn field_rev_bump_is_a_hard_stop() {
    // R20/H8: a name-matched field whose rev differs is a planner
    // REFUSAL naming the path and both revs — rev marks same-shape/
    // new-meaning, and both silent options (copy = reinterpretation,
    // drop+default = data loss) are what rev exists to prevent.
    let old = strct(0, &[("a", 0, p(PK::U32))]);
    let new = strct(0, &[("a", 1, p(PK::U32))]);
    let refusal = plan_automatic(&old, &new).unwrap_err();
    assert_eq!(refusal.reasons.len(), 1);
    let (rpath, reason) = &refusal.reasons[0];
    assert!(rpath.contains('a'), "path names the field: {rpath}");
    assert!(
        reason.contains("0") && reason.contains("1") && reason.contains("revision"),
        "reason names both revs: {reason}"
    );
    assert!(
        reason.contains("custom migration edge"),
        "reason demands a custom edge: {reason}"
    );
}

#[test]
fn variant_rev_bump_is_a_hard_stop() {
    // Same R20/H8 rule for name-matched enum variants — even data-free
    // ones (the old rule would have silently dropped them).
    let old = enm(0, &[("A", 0, strct(0, &[])), ("B", 0, strct(0, &[]))]);
    let new = enm(0, &[("A", 1, strct(0, &[])), ("B", 0, strct(0, &[]))]);
    let refusal = plan_automatic(&old, &new).unwrap_err();
    assert_eq!(refusal.reasons.len(), 1);
    let (rpath, reason) = &refusal.reasons[0];
    assert!(rpath.contains("{A}"), "path names the variant: {rpath}");
    assert!(
        reason.contains("revision") && reason.contains("custom migration edge"),
        "reason: {reason}"
    );
}

#[test]
fn widen_unsigned_u16_to_u32() {
    let old = strct(0, &[("a", 0, p(PK::U16))]);
    let new = strct(0, &[("a", 0, p(PK::U32))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::Widen {
            from: path(&["a"]),
            to: path(&["a"])
        }]
    );
}

#[test]
fn widen_signed_i8_to_i64() {
    let old = strct(0, &[("a", 0, p(PK::I8))]);
    let new = strct(0, &[("a", 0, p(PK::I64))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::Widen {
            from: path(&["a"]),
            to: path(&["a"])
        }]
    );
}

#[test]
fn widen_float_f32_to_f64() {
    let old = strct(0, &[("a", 0, p(PK::F32))]);
    let new = strct(0, &[("a", 0, p(PK::F64))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::Widen {
            from: path(&["a"]),
            to: path(&["a"])
        }]
    );
}

#[test]
fn narrowing_refused_naming_path() {
    let old = strct(0, &[("a", 0, p(PK::U32))]);
    let new = strct(0, &[("a", 0, p(PK::U16))]);
    refuses_at(&old, &new, "a", "u32");
}

#[test]
fn cross_sign_refused() {
    let old = strct(0, &[("a", 0, p(PK::I32))]);
    let new = strct(0, &[("a", 0, p(PK::U64))]);
    refuses_at(&old, &new, "a", "i32");
}

#[test]
fn int128_targets_refused() {
    // The shared widening table (ngp_schema::migrate) never targets
    // 128-bit integers.
    let old = strct(0, &[("a", 0, p(PK::I64))]);
    let new = strct(0, &[("a", 0, p(PK::I128))]);
    refuses_at(&old, &new, "a", "i64");
    let old = strct(0, &[("a", 0, p(PK::U64))]);
    let new = strct(0, &[("a", 0, p(PK::U128))]);
    refuses_at(&old, &new, "a", "u64");
}

#[test]
fn int_to_float_refused() {
    let old = strct(0, &[("a", 0, p(PK::U32))]);
    let new = strct(0, &[("a", 0, p(PK::F64))]);
    refuses_at(&old, &new, "a", "u32");
}

#[test]
fn node_kind_change_refused() {
    let old = strct(0, &[("a", 0, SchemaNode::String)]);
    let new = strct(0, &[("a", 0, p(PK::U32))]);
    refuses_at(&old, &new, "a", "");
}

#[test]
fn array_len_change_refused() {
    let old = strct(0, &[("a", 0, arr(3, p(PK::U8)))]);
    let new = strct(0, &[("a", 0, arr(4, p(PK::U8)))]);
    refuses_at(&old, &new, "a", "length");
}

#[test]
fn map_key_and_value_both_changed_refused() {
    let old = strct(0, &[("m", 0, map_of(p(PK::U16), p(PK::U16)))]);
    let new = strct(0, &[("m", 0, map_of(p(PK::U32), p(PK::U32)))]);
    refuses_at(&old, &new, "m", "key");
}

#[test]
fn map_value_only_diff_plans_migrate_elements() {
    let old = strct(0, &[("m", 0, map_of(SchemaNode::String, p(PK::U16)))]);
    let new = strct(0, &[("m", 0, map_of(SchemaNode::String, p(PK::U32)))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::MigrateElements {
            at: path(&["m"]),
            element: vec![MigrationOp::Widen {
                from: root(),
                to: root()
            }]
        }]
    );
}

#[test]
fn map_key_only_diff_plans_migrate_map_keys() {
    let old = strct(0, &[("m", 0, map_of(p(PK::U16), SchemaNode::String))]);
    let new = strct(0, &[("m", 0, map_of(p(PK::U32), SchemaNode::String))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::MigrateMapKeys {
            at: path(&["m"]),
            key: vec![MigrationOp::Widen {
                from: root(),
                to: root()
            }]
        }]
    );
}

#[test]
fn nested_struct_diff_plans_migrate_inline() {
    let old = strct(0, &[("inner", 0, strct(0, &[("x", 0, p(PK::U32))]))]);
    let new = strct(0, &[("inner", 0, strct(0, &[("x", 0, p(PK::U64))]))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::MigrateInline {
            at: path(&["inner"]),
            ops: vec![MigrationOp::Widen {
                from: path(&["x"]),
                to: path(&["x"])
            }]
        }]
    );
}

#[test]
fn struct_type_rev_change_refused() {
    // PINNED: a TYPE-level rev bump on a matched struct node is a refusal
    // (the author declared "not the same type"); only FIELD-entry rev
    // bumps get the drop+default treatment.
    let old = strct(0, &[("inner", 0, strct(0, &[("x", 0, p(PK::U32))]))]);
    let new = strct(0, &[("inner", 0, strct(1, &[("x", 0, p(PK::U32))]))]);
    refuses_at(&old, &new, "inner", "rev");
}

#[test]
fn vec_of_struct_elem_diff_plans_migrate_elements() {
    let elem_old = strct(0, &[("x", 0, p(PK::U32)), ("k", 0, SchemaNode::String)]);
    let elem_new = strct(0, &[("x", 0, p(PK::U64)), ("k", 0, SchemaNode::String)]);
    let old = strct(0, &[("items", 0, vec_of(elem_old))]);
    let new = strct(0, &[("items", 0, vec_of(elem_new))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(ops.len(), 1);
    let MigrationOp::MigrateElements { at, element } = &ops[0] else {
        panic!("expected MigrateElements, got {ops:?}");
    };
    assert_eq!(*at, path(&["items"]));
    assert!(element.contains(&MigrationOp::Widen {
        from: path(&["x"]),
        to: path(&["x"])
    }));
    assert!(element.contains(&MigrationOp::CopyField {
        from: path(&["k"]),
        to: path(&["k"])
    }));
}

#[test]
fn option_inner_diff_plans_migrate_elements() {
    let old = strct(0, &[("o", 0, opt(p(PK::U16)))]);
    let new = strct(0, &[("o", 0, opt(p(PK::U64)))]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::MigrateElements {
            at: path(&["o"]),
            element: vec![MigrationOp::Widen {
                from: root(),
                to: root()
            }]
        }]
    );
}

#[test]
fn enum_variant_payload_diff_plans_map_variant() {
    let old_e = enm(
        0,
        &[
            ("A", 0, unit_payload()),
            ("B", 0, strct(0, &[("n", 0, p(PK::U32))])),
        ],
    );
    let new_e = enm(
        0,
        &[
            ("A", 0, unit_payload()),
            ("B", 0, strct(0, &[("n", 0, p(PK::U64))])),
        ],
    );
    let old = strct(0, &[("e", 0, old_e)]);
    let new = strct(0, &[("e", 0, new_e)]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::MapVariant {
            at: path(&["e"]),
            from: "B".to_string(),
            to: "B".to_string(),
            payload: vec![MigrationOp::Widen {
                from: path(&["n"]),
                to: path(&["n"])
            }]
        }]
    );
}

#[test]
fn unmatched_data_variant_refused() {
    // unmatched + data = error (§11).
    let old_e = enm(
        0,
        &[
            ("A", 0, unit_payload()),
            ("Gone", 0, strct(0, &[("n", 0, p(PK::U32))])),
        ],
    );
    let new_e = enm(0, &[("A", 0, unit_payload())]);
    let old = strct(0, &[("e", 0, old_e)]);
    let new = strct(0, &[("e", 0, new_e)]);
    refuses_at(&old, &new, "e", "Gone");
}

#[test]
fn unmatched_unit_variant_silently_droppable() {
    // A data-free removed variant refuses nothing; surviving variants pass
    // through — the enum leaf is covered by a plain copy (pinned).
    let old_e = enm(0, &[("A", 0, unit_payload()), ("Gone", 0, unit_payload())]);
    let new_e = enm(0, &[("A", 0, unit_payload()), ("New", 0, unit_payload())]);
    let old = strct(0, &[("e", 0, old_e)]);
    let new = strct(0, &[("e", 0, new_e)]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert_eq!(
        ops,
        vec![MigrationOp::CopyField {
            from: path(&["e"]),
            to: path(&["e"])
        }]
    );
}

#[test]
fn variant_rev_bump_with_data_refused() {
    // R20/H8: a name-matched, rev-bumped variant is a hard stop — here
    // it also carries data, so the refusal is doubly mandatory.
    let old_e = enm(0, &[("B", 0, strct(0, &[("n", 0, p(PK::U32))]))]);
    let new_e = enm(0, &[("B", 1, strct(0, &[("n", 0, p(PK::U32))]))]);
    let old = strct(0, &[("e", 0, old_e)]);
    let new = strct(0, &[("e", 0, new_e)]);
    refuses_at(&old, &new, "e{B}", "revision");
}

#[test]
fn backref_recursive_type_unchanged_is_copyable() {
    // Outer changes OUTSIDE the recursion; the recursive subtree is
    // meaning-identical (its backref stays inside the subtree) → CopyField.
    let tree = strct(
        0,
        &[
            ("name", 0, SchemaNode::String),
            ("children", 0, vec_of(SchemaNode::BackRef(0))),
        ],
    );
    let old = strct(0, &[("count", 0, p(PK::U32)), ("tree", 0, tree.clone())]);
    let new = strct(0, &[("count", 0, p(PK::U64)), ("tree", 0, tree)]);
    let ops = plan_automatic(&old, &new).unwrap();
    assert!(ops.contains(&MigrationOp::Widen {
        from: path(&["count"]),
        to: path(&["count"])
    }));
    assert!(ops.contains(&MigrationOp::CopyField {
        from: path(&["tree"]),
        to: path(&["tree"])
    }));
    assert_eq!(ops.len(), 2);
}

#[test]
fn backref_into_changed_type_refused() {
    // The recursive field's AST is `Vec(BackRef(0))` on BOTH sides — plain
    // equality would call it unchanged, but the frame it re-enters changed
    // (meta widened), so a copy would preserve old-shaped subtrees. The
    // planner must refuse (automatic plans cannot recurse).
    let old = strct(
        0,
        &[
            ("meta", 0, p(PK::U32)),
            ("children", 0, vec_of(SchemaNode::BackRef(0))),
        ],
    );
    let new = strct(
        0,
        &[
            ("meta", 0, p(PK::U64)),
            ("children", 0, vec_of(SchemaNode::BackRef(0))),
        ],
    );
    refuses_at(&old, &new, "children", "recurs");
}

#[test]
fn backref_distance_change_refused() {
    let old = strct(0, &[("r", 0, opt(SchemaNode::BackRef(0)))]);
    let inner = strct(0, &[("r", 0, opt(SchemaNode::BackRef(1)))]);
    let new = strct(0, &[("r", 0, opt(SchemaNode::BackRef(0)))]);
    // Sanity: build a real distance change at a matched path.
    let old2 = strct(
        0,
        &[("w", 0, strct(0, &[("q", 0, SchemaNode::BackRef(0))]))],
    );
    let new2 = strct(
        0,
        &[("w", 0, strct(0, &[("q", 0, SchemaNode::BackRef(1))]))],
    );
    let _ = (old, inner, new);
    refuses_at(&old2, &new2, "q", "distance");
}

#[test]
fn asset_ref_retarget_refused() {
    let old = strct(0, &[("r", 0, SchemaNode::AssetRef(tuuid(1)))]);
    let new = strct(0, &[("r", 0, SchemaNode::AssetRef(tuuid(2)))]);
    refuses_at(&old, &new, "r", "");
}
