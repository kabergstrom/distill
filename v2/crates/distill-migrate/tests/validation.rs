//! Plan validation (§11): path resolution, edge legality, total and
//! disjoint output coverage.

mod common;
use common::*;
use distill_json::AuthoredValue;
use distill_migrate::{validate_plan, EdgeKind, MigrationOp, PlanError};
use ngp_schema::SchemaNode;

fn copy(fp: &[&str], tp: &[&str]) -> MigrationOp {
    MigrationOp::CopyField {
        from: path(fp),
        to: path(tp),
    }
}

fn has_error(errs: &[PlanError], pred: impl Fn(&PlanError) -> bool) -> bool {
    errs.iter().any(pred)
}

#[test]
fn valid_plan_passes_both_edge_kinds() {
    let from = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, SchemaNode::String)]);
    let to = from.clone();
    let ops = vec![copy(&["a"], &["a"]), copy(&["b"], &["b"])];
    validate_plan(&ops, &from, &to, EdgeKind::Automatic).unwrap();
    validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap();
}

#[test]
fn missing_from_path_is_error() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = from.clone();
    let ops = vec![copy(&["nope"], &["a"])];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::MissingFromPath { path } if path.contains("nope")
    )));
}

#[test]
fn missing_to_path_is_error() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = from.clone();
    let ops = vec![copy(&["a"], &["a"]), copy(&["a"], &["nope"])];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::MissingToPath { path } if path.contains("nope")
    )));
}

#[test]
fn missing_at_path_is_error_per_side() {
    // `at` must resolve in BOTH schemas.
    let inner = strct(0, &[("x", 0, p(PK::U32))]);
    let from = strct(0, &[("a", 0, p(PK::U32))]); // no "inner" here
    let to = strct(0, &[("a", 0, p(PK::U32)), ("inner", 0, inner)]);
    let ops = vec![
        copy(&["a"], &["a"]),
        MigrationOp::MigrateInline {
            at: path(&["inner"]),
            ops: vec![MigrationOp::WriteValue {
                to: path(&["x"]),
                value: ui(1),
            }],
        },
    ];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::MissingAtPath { path, .. } if path.contains("inner")
    )));
}

#[test]
fn drop_field_at_resolves_in_from_schema() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = from.clone();
    let ops = vec![
        copy(&["a"], &["a"]),
        MigrationOp::DropField {
            at: path(&["ghost"]),
        },
    ];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::MissingFromPath { path } if path.contains("ghost")
    )));
}

#[test]
fn uncovered_leaf_is_error() {
    let from = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, SchemaNode::String)]);
    let to = from.clone();
    let ops = vec![copy(&["a"], &["a"])]; // "b" never written
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::UncoveredLeaf { path } if path.contains("b")
    )));
}

#[test]
fn double_covered_leaf_equal_paths_is_error() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = from.clone();
    let ops = vec![copy(&["a"], &["a"]), copy(&["a"], &["a"])];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::DoubleCoveredLeaf { path } if path.contains("a")
    )));
}

#[test]
fn double_covered_leaf_prefix_overlap_is_error() {
    // Writing $.inner (whole struct) AND $.inner.x double-covers leaf x.
    let inner = strct(0, &[("x", 0, p(PK::U32))]);
    let from = strct(0, &[("inner", 0, inner)]);
    let to = from.clone();
    let ops = vec![
        copy(&["inner"], &["inner"]),
        MigrationOp::WriteValue {
            to: path(&["inner", "x"]),
            value: ui(1),
        },
    ];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::DoubleCoveredLeaf { path } if path.contains("x")
    )));
}

#[test]
fn default_ops_rejected_in_custom_edge() {
    // §11: edge authoring materializes literals; a violating edge is
    // rejected naming it.
    let from = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, p(PK::U32))]);
    let to = from.clone();
    let ops = vec![
        MigrationOp::WriteFieldDefault { to: path(&["a"]) },
        MigrationOp::WriteParentDefault { to: path(&["b"]) },
    ];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::DefaultOpInCustomEdge { op, .. } if *op == "WriteFieldDefault"
    )));
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::DefaultOpInCustomEdge { op, .. } if *op == "WriteParentDefault"
    )));
}

#[test]
fn default_ops_accepted_in_automatic_segment() {
    let from = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, p(PK::U32))]);
    let to = from.clone();
    let ops = vec![
        MigrationOp::WriteFieldDefault { to: path(&["a"]) },
        MigrationOp::WriteParentDefault { to: path(&["b"]) },
    ];
    validate_plan(&ops, &from, &to, EdgeKind::Automatic).unwrap();
}

#[test]
fn write_value_accepted_in_custom_edge() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = from.clone();
    let ops = vec![MigrationOp::WriteValue {
        to: path(&["a"]),
        value: ui(7),
    }];
    validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap();
}

#[test]
fn duplicate_map_variant_from_is_error() {
    let e = enm(0, &[("A", 0, unit_payload()), ("B", 0, unit_payload())]);
    let from = strct(0, &[("e", 0, e.clone())]);
    let to = from.clone();
    let mv = |to_v: &str| MigrationOp::MapVariant {
        at: path(&["e"]),
        from: "A".to_string(),
        to: to_v.to_string(),
        payload: vec![],
    };
    let ops = vec![mv("A"), mv("B")];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::DuplicateMapVariantFrom { from, .. } if from == "A"
    )));
}

#[test]
fn map_variant_group_is_one_writer() {
    // Two MapVariants with DISTINCT froms at one path: one composite
    // writer, not a double-cover.
    let e = enm(0, &[("A", 0, unit_payload()), ("B", 0, unit_payload())]);
    let from = strct(0, &[("e", 0, e.clone())]);
    let to = from.clone();
    let mv = |f: &str| MigrationOp::MapVariant {
        at: path(&["e"]),
        from: f.to_string(),
        to: f.to_string(),
        payload: vec![],
    };
    validate_plan(&[mv("A"), mv("B")], &from, &to, EdgeKind::Custom).unwrap();
}

// NOTE: `MigrationOp::Custom` was removed from the vocabulary (R20/M21):
// whole-edge `MigrationKind::Function` is the custom-logic carrier, so
// there is no per-op escape hatch left to reject.

#[test]
fn write_none_target_must_be_option() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = from.clone();
    let ops = vec![MigrationOp::WriteNone { to: path(&["a"]) }];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::WriteNoneTargetNotOption { path } if path.contains("a")
    )));
}

#[test]
fn invalid_widen_pair_rejected() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = strct(0, &[("a", 0, p(PK::U16))]);
    let ops = vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::InvalidWiden { .. }
    )));
}

#[test]
fn map_variant_unknown_variant_rejected() {
    let e = enm(0, &[("A", 0, unit_payload())]);
    let from = strct(0, &[("e", 0, e.clone())]);
    let to = from.clone();
    let ops = vec![MigrationOp::MapVariant {
        at: path(&["e"]),
        from: "Nope".to_string(),
        to: "A".to_string(),
        payload: vec![],
    }];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::UnknownVariant { variant, .. } if variant == "Nope"
    )));
}

#[test]
fn nested_frame_coverage_enforced() {
    // Total-and-disjoint applies recursively: MigrateInline's inner ops
    // must cover the inner struct completely.
    let inner = strct(0, &[("x", 0, p(PK::U32)), ("y", 0, p(PK::U32))]);
    let from = strct(0, &[("inner", 0, inner)]);
    let to = from.clone();
    let ops = vec![MigrationOp::MigrateInline {
        at: path(&["inner"]),
        ops: vec![copy(&["x"], &["x"])], // "y" uncovered in the inner frame
    }];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::UncoveredLeaf { path } if path.contains("y")
    )));
}

#[test]
fn migrate_elements_array_len_mismatch_rejected() {
    let from = strct(0, &[("a", 0, arr(3, p(PK::U16)))]);
    let to = strct(0, &[("a", 0, arr(4, p(PK::U32)))]);
    let ops = vec![MigrationOp::MigrateElements {
        at: path(&["a"]),
        element: vec![MigrationOp::Widen {
            from: root(),
            to: root(),
        }],
    }];
    let errs = validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap_err();
    assert!(has_error(&errs, |e| matches!(
        e,
        PlanError::ContainerMismatch { .. }
    )));
}

#[test]
fn zero_field_struct_is_trivially_covered() {
    // A zero-field struct has no coverage leaves: the empty plan is total.
    let from = strct(0, &[]);
    let to = strct(0, &[]);
    let ops: Vec<MigrationOp> = vec![];
    validate_plan(&ops, &from, &to, EdgeKind::Custom).unwrap();
    let _ = AuthoredValue::Null; // silence unused import lints in some cfgs
}
