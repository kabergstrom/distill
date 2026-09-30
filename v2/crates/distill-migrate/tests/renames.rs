//! `#[asset(renamed_from)]`: the planner matches a renamed field to its
//! old name.

mod common;
use common::*;
use distill_migrate::{
    execute_ops, lossy_drops, plan_automatic_renamed, validate_plan, EdgeKind, MigrationOp,
};
use distill_json::AuthoredValue;
use ngp_schema::{Renames, SchemaNode};

fn renames(pairs: &[(&str, &str)]) -> Renames {
    pairs
        .iter()
        .map(|(at, old)| (at.to_string(), old.to_string()))
        .collect()
}

#[test]
fn a_renamed_field_is_copied_from_its_old_name() {
    let old = strct(0, &[("a", 0, p(PK::U32)), ("hp", 0, p(PK::U32))]);
    let new = strct(0, &[("a", 0, p(PK::U32)), ("health", 0, p(PK::U32))]);
    let ops = plan_automatic_renamed(&old, &new, &renames(&[("$.health", "hp")])).unwrap();
    assert!(ops.contains(&MigrationOp::CopyField {
        from: path(&["hp"]),
        to: path(&["health"])
    }));
    assert!(!ops.iter().any(|op| matches!(op, MigrationOp::DropField { .. })));
    validate_plan(&ops, &old, &new, EdgeKind::Automatic).unwrap();
    let input = obj(&[("a", ui(1)), ("hp", ui(40))]);
    assert!(lossy_drops(&ops, &old, &input).is_empty());
    let out = execute_ops(&ops, &input, &old, &new, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("a", ui(1)), ("health", ui(40))]));
}

#[test]
fn a_renamed_field_may_also_widen() {
    let old = strct(0, &[("hp", 0, p(PK::U16))]);
    let new = strct(0, &[("health", 0, p(PK::U32))]);
    let ops = plan_automatic_renamed(&old, &new, &renames(&[("$.health", "hp")])).unwrap();
    assert_eq!(
        ops,
        [MigrationOp::Widen {
            from: path(&["hp"]),
            to: path(&["health"])
        }]
    );
    let out = execute_ops(&ops, &obj(&[("hp", ui(7))]), &old, &new, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("health", ui(7))]));
}

#[test]
fn renames_apply_inside_elements_and_variant_payloads() {
    let item = |name: &str| strct(0, &[(name, 0, SchemaNode::String)]);
    let old = strct(
        0,
        &[
            ("items", 0, vec_of(item("label"))),
            ("shape", 0, enm(0, &[("Circle", 0, strct(0, &[("r", 0, p(PK::F32))]))])),
        ],
    );
    let new = strct(
        0,
        &[
            ("items", 0, vec_of(item("name"))),
            ("shape", 0, enm(0, &[("Circle", 0, strct(0, &[("radius", 0, p(PK::F32))]))])),
        ],
    );
    let table = renames(&[("$.items[].name", "label"), ("$.shape{Circle}.radius", "r")]);
    let ops = plan_automatic_renamed(&old, &new, &table).unwrap();
    validate_plan(&ops, &old, &new, EdgeKind::Automatic).unwrap();
    let input = obj(&[
        ("items", arr_v(&[obj(&[("label", AuthoredValue::Str("one".into()))])])),
        ("shape", obj(&[("Circle", obj(&[("r", fl(2.0))]))])),
    ]);
    let out = execute_ops(&ops, &input, &old, &new, &NoDefaults).unwrap();
    assert_eq!(
        out.value,
        obj(&[
            ("items", arr_v(&[obj(&[("name", AuthoredValue::Str("one".into()))])])),
            ("shape", obj(&[("Circle", obj(&[("radius", fl(2.0))]))])),
        ])
    );
}

#[test]
fn a_field_still_present_under_its_old_name_keeps_its_own_match() {
    let old = strct(0, &[("hp", 0, p(PK::U32))]);
    let new = strct(0, &[("hp", 0, p(PK::U32)), ("health", 0, p(PK::U32))]);
    let ops = plan_automatic_renamed(&old, &new, &renames(&[("$.health", "hp")])).unwrap();
    assert!(ops.contains(&MigrationOp::CopyField {
        from: path(&["hp"]),
        to: path(&["hp"])
    }));
    assert!(ops.contains(&MigrationOp::WriteFieldDefault {
        to: path(&["health"])
    }));
}

#[test]
fn a_rename_that_also_changes_shape_or_rev_is_refused() {
    let old = strct(0, &[("inner", 0, strct(0, &[("a", 0, p(PK::U8))]))]);
    let new = strct(
        0,
        &[("outer", 0, strct(0, &[("a", 0, p(PK::U8)), ("b", 0, p(PK::U8))]))],
    );
    let refusal =
        plan_automatic_renamed(&old, &new, &renames(&[("$.outer", "inner")])).unwrap_err();
    assert_eq!(refusal.reasons.len(), 1);
    assert_eq!(refusal.reasons[0].0, "$.outer");

    let old = strct(0, &[("hp", 0, p(PK::U32))]);
    let new = strct(0, &[("health", 1, p(PK::U32))]);
    let refusal = plan_automatic_renamed(&old, &new, &renames(&[("$.health", "hp")])).unwrap_err();
    assert!(refusal.reasons[0].1.contains("semantic revision"));
}
