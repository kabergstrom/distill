//! Executor (§11): immutable input, fresh output, order-independence,
//! fail-hard integrity.

mod common;
use common::*;
use distill_migrate::{
    execute_edge, execute_ops, FieldPath, MigrationError, MigrationKind, MigrationOp,
};
use ngp_schema::SchemaNode;

fn copy(fp: &[&str], tp: &[&str]) -> MigrationOp {
    MigrationOp::CopyField {
        from: path(fp),
        to: path(tp),
    }
}

/// Run `ops` on `input` (text of `from`), returning the output's text.
fn run(ops: &[MigrationOp], input: &str, from: &SchemaNode, to: &SchemaNode) -> String {
    let out = execute_ops(ops, &val(from, input), from, to, &NoDefaults).unwrap();
    txt(to, &out.value)
}

#[test]
fn copy_twice_from_one_source_proves_immutable_input() {
    // CopyField COPIES — the source stays readable to later ops (§11).
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = strct(0, &[("x", 0, p(PK::U32)), ("y", 0, p(PK::U32))]);
    let ops = vec![copy(&["a"], &["x"]), copy(&["a"], &["y"])];
    let out = execute_ops(&ops, &val(&from, r#"{"a":41}"#), &from, &to, &NoDefaults).unwrap();
    assert_eq!(txt(&to, &out.value), r#"{"x":41,"y":41}"#);
    assert!(out.defaulted.is_empty());
}

#[test]
fn op_order_shuffle_produces_identical_output() {
    let from = strct(
        0,
        &[
            ("a", 0, p(PK::U32)),
            ("b", 0, SchemaNode::String),
            ("c", 0, p(PK::U16)),
        ],
    );
    let to = strct(
        0,
        &[
            ("a", 0, p(PK::U32)),
            ("b", 0, SchemaNode::String),
            ("c", 0, p(PK::U32)),
        ],
    );
    let ops1 = vec![
        copy(&["a"], &["a"]),
        copy(&["b"], &["b"]),
        MigrationOp::Widen {
            from: path(&["c"]),
            to: path(&["c"]),
        },
    ];
    let mut ops2 = ops1.clone();
    ops2.reverse();
    let input = val(&from, r#"{"a":1,"b":"hi","c":9}"#);
    let r1 = execute_ops(&ops1, &input, &from, &to, &NoDefaults).unwrap();
    let r2 = execute_ops(&ops2, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(r1.value, r2.value);
}

#[test]
fn widen_converts_to_the_new_leaf_kind() {
    let from = strct(0, &[("a", 0, p(PK::I8))]);
    let to = strct(0, &[("a", 0, p(PK::I64))]);
    let ops = vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }];
    for n in [-3i8, 5] {
        let out = execute_ops(
            &ops,
            &Value::Struct(Box::new([Value::I8(n)])),
            &from,
            &to,
            &NoDefaults,
        )
        .unwrap();
        assert_eq!(out.value, Value::Struct(Box::new([Value::I64(n as i64)])));
    }
}

#[test]
fn widen_float_converts_to_f64() {
    let from = strct(0, &[("a", 0, p(PK::F32))]);
    let to = strct(0, &[("a", 0, p(PK::F64))]);
    let ops = vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }];
    let out = execute_ops(&ops, &val(&from, r#"{"a":1.5}"#), &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, Value::Struct(Box::new([Value::F64(1.5)])));
}

#[test]
fn widen_value_not_of_the_old_leaf_is_corrupt_input() {
    let from = strct(0, &[("a", 0, p(PK::U8))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let ops = vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }];
    // A typed value cannot hold 300 as a u8; a u16 there is the wrong leaf.
    let input = Value::Struct(Box::new([Value::U16(300)]));
    let err = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap_err();
    assert!(matches!(err, MigrationError::InputShape { .. }), "{err:?}");
}

#[test]
fn widen_value_outside_the_new_range_is_corrupt_input() {
    // An unvalidated signed → unsigned "widening": -1 does not fit.
    let from = strct(0, &[("a", 0, p(PK::I8))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let ops = vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }];
    let err = execute_ops(&ops, &val(&from, r#"{"a":-1}"#), &from, &to, &NoDefaults).unwrap_err();
    assert!(
        matches!(err, MigrationError::WidenOutOfRange { .. }),
        "{err:?}"
    );
}

#[test]
fn write_none_and_write_value() {
    let from = strct(0, &[]);
    let to = strct(
        0,
        &[("o", 0, opt(p(PK::U32))), ("v", 0, SchemaNode::String)],
    );
    let ops = vec![
        MigrationOp::WriteNone { to: path(&["o"]) },
        MigrationOp::WriteValue {
            to: path(&["v"]),
            value: Value::String("lit".into()),
        },
    ];
    assert_eq!(run(&ops, "{}", &from, &to), r#"{"o":null,"v":"lit"}"#);
}

#[test]
fn missing_default_fails_hard() {
    // Integrity: a None from the provider is an error, never a fabricated
    // value (§11).
    let from = strct(0, &[]);
    let to = strct(0, &[("b", 0, p(PK::U32))]);
    let ops = vec![MigrationOp::WriteFieldDefault { to: path(&["b"]) }];
    let err = execute_ops(&ops, &val(&from, "{}"), &from, &to, &NoDefaults).unwrap_err();
    assert!(
        matches!(&err, MigrationError::MissingDefault { path, .. } if path.contains('b')),
        "{err:?}"
    );
}

#[test]
fn defaults_populate_value_and_defaulted_list() {
    let from = strct(0, &[]);
    let to = strct(0, &[("b", 0, p(PK::U32)), ("c", 0, p(PK::U32))]);
    let mut defaults = TableDefaults::default();
    defaults.field.insert(path(&["b"]), Value::U32(11));
    defaults.parent.insert(path(&["c"]), Value::U32(22));
    let ops = vec![
        MigrationOp::WriteFieldDefault { to: path(&["b"]) },
        MigrationOp::WriteParentDefault { to: path(&["c"]) },
    ];
    let out = execute_ops(&ops, &val(&from, "{}"), &from, &to, &defaults).unwrap();
    assert_eq!(txt(&to, &out.value), r#"{"b":11,"c":22}"#);
    let mut defaulted = out.defaulted.clone();
    defaulted.sort();
    assert_eq!(defaulted, vec![path(&["b"]), path(&["c"])]);
}

#[test]
fn defaulted_paths_from_element_frames_are_reported() {
    let elem_old = strct(0, &[("x", 0, p(PK::U32))]);
    let elem_new = strct(0, &[("x", 0, p(PK::U32)), ("y", 0, p(PK::U8))]);
    let from = strct(0, &[("items", 0, vec_of(elem_old))]);
    let to = strct(0, &[("items", 0, vec_of(elem_new))]);
    let mut defaults = TableDefaults::default();
    defaults.field.insert(path(&["y"]), Value::U8(0));
    let ops = vec![MigrationOp::MigrateElements {
        at: path(&["items"]),
        element: vec![
            copy(&["x"], &["x"]),
            MigrationOp::WriteFieldDefault { to: path(&["y"]) },
        ],
    }];
    let input = val(&from, r#"{"items":[{"x":1},{"x":2}]}"#);
    let out = execute_ops(&ops, &input, &from, &to, &defaults).unwrap();
    assert_eq!(
        txt(&to, &out.value),
        r#"{"items":[{"x":1,"y":0},{"x":2,"y":0}]}"#
    );
    // One defaulted path per element, with diagnostic pseudo-segments.
    assert_eq!(out.defaulted.len(), 2);
    assert_eq!(out.defaulted[0], FieldPath::of(&["items", "[0]", "y"]));
    assert_eq!(out.defaulted[1], FieldPath::of(&["items", "[1]", "y"]));
}

#[test]
fn map_variant_renames_and_transforms_payload() {
    let old_e = enm(
        0,
        &[
            ("Keep", 0, unit_payload()),
            ("Old", 0, strct(0, &[("n", 0, p(PK::U32))])),
        ],
    );
    let new_e = enm(
        0,
        &[
            ("Keep", 0, unit_payload()),
            ("New", 0, strct(0, &[("n", 0, p(PK::U64))])),
        ],
    );
    let from = strct(0, &[("e", 0, old_e)]);
    let to = strct(0, &[("e", 0, new_e)]);
    let ops = vec![MigrationOp::MapVariant {
        at: path(&["e"]),
        from: "Old".to_string(),
        to: "New".to_string(),
        payload: vec![MigrationOp::Widen {
            from: path(&["n"]),
            to: path(&["n"]),
        }],
    }];
    assert_eq!(
        run(&ops, r#"{"e":{"Old":{"n":5}}}"#, &from, &to),
        r#"{"e":{"New":{"n":5}}}"#
    );
    // A variant no MapVariant names passes through unchanged.
    assert_eq!(
        run(&ops, r#"{"e":{"Keep":{}}}"#, &from, &to),
        r#"{"e":{"Keep":{}}}"#
    );
}

#[test]
fn passthrough_variant_follows_its_name_to_a_new_index() {
    // "Keep" is variant 0 before and 1 after ("Added" sorts first).
    let from = strct(0, &[("e", 0, enm(0, &[("Keep", 0, unit_payload())]))]);
    let to = strct(
        0,
        &[(
            "e",
            0,
            enm(0, &[("Added", 0, unit_payload()), ("Keep", 0, unit_payload())]),
        )],
    );
    let ops = vec![MigrationOp::MapVariant {
        at: path(&["e"]),
        from: "Gone".to_string(),
        to: "Added".to_string(),
        payload: vec![],
    }];
    assert_eq!(
        run(&ops, r#"{"e":{"Keep":{}}}"#, &from, &to),
        r#"{"e":{"Keep":{}}}"#
    );
}

#[test]
fn migrate_elements_over_vec() {
    let from = strct(0, &[("v", 0, vec_of(p(PK::U16)))]);
    let to = strct(0, &[("v", 0, vec_of(p(PK::U32)))]);
    let ops = vec![MigrationOp::MigrateElements {
        at: path(&["v"]),
        element: vec![MigrationOp::Widen {
            from: root(),
            to: root(),
        }],
    }];
    assert_eq!(run(&ops, r#"{"v":[1,2,3]}"#, &from, &to), r#"{"v":[1,2,3]}"#);
}

#[test]
fn migrate_elements_over_option_null_and_some() {
    let from = strct(0, &[("o", 0, opt(p(PK::U16)))]);
    let to = strct(0, &[("o", 0, opt(p(PK::U32)))]);
    let ops = vec![MigrationOp::MigrateElements {
        at: path(&["o"]),
        element: vec![MigrationOp::Widen {
            from: root(),
            to: root(),
        }],
    }];
    assert_eq!(run(&ops, r#"{"o":null}"#, &from, &to), r#"{"o":null}"#);
    assert_eq!(run(&ops, r#"{"o":6}"#, &from, &to), r#"{"o":6}"#);
}

#[test]
fn migrate_elements_over_map_values() {
    let from = strct(0, &[("m", 0, map_of(SchemaNode::String, p(PK::U16)))]);
    let to = strct(0, &[("m", 0, map_of(SchemaNode::String, p(PK::U32)))]);
    let ops = vec![MigrationOp::MigrateElements {
        at: path(&["m"]),
        element: vec![MigrationOp::Widen {
            from: root(),
            to: root(),
        }],
    }];
    assert_eq!(
        run(&ops, r#"{"m":{"k1":1,"k2":2}}"#, &from, &to),
        r#"{"m":{"k1":1,"k2":2}}"#
    );
}

#[test]
fn set_duplicate_after_migration_is_error() {
    // Sets rebuild through real equality: a post-migration duplicate is
    // an error, exactly the map-key collision rule (§11).
    let from = strct(0, &[("s", 0, set_of(p(PK::U32)))]);
    let to = strct(0, &[("s", 0, set_of(p(PK::U32)))]);
    let ops = vec![MigrationOp::MigrateElements {
        at: path(&["s"]),
        element: vec![MigrationOp::WriteValue {
            to: root(),
            value: Value::U32(1),
        }],
    }];
    let input = val(&from, r#"{"s":[1,2]}"#);
    let err = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap_err();
    assert!(
        matches!(err, MigrationError::DuplicateSetElement { .. }),
        "{err:?}"
    );
}

#[test]
fn map_key_collision_after_migrate_map_keys_is_error() {
    let from = strct(0, &[("m", 0, map_of(SchemaNode::String, p(PK::U32)))]);
    let to = strct(0, &[("m", 0, map_of(SchemaNode::String, p(PK::U32)))]);
    let ops = vec![MigrationOp::MigrateMapKeys {
        at: path(&["m"]),
        key: vec![MigrationOp::WriteValue {
            to: root(),
            value: Value::String("same".into()),
        }],
    }];
    let input = val(&from, r#"{"m":{"a":1,"b":2}}"#);
    let err = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap_err();
    assert!(
        matches!(err, MigrationError::MapKeyCollision { .. }),
        "{err:?}"
    );
}

#[test]
fn migrate_map_keys_widens_nonstring_keys() {
    let from = strct(0, &[("m", 0, map_of(p(PK::U16), SchemaNode::String))]);
    let to = strct(0, &[("m", 0, map_of(p(PK::U32), SchemaNode::String))]);
    let ops = vec![MigrationOp::MigrateMapKeys {
        at: path(&["m"]),
        key: vec![MigrationOp::Widen {
            from: root(),
            to: root(),
        }],
    }];
    // Non-string-key maps are [k, v] pair arrays (§6).
    assert_eq!(
        run(&ops, r#"{"m":[[1,"one"],[2,"two"]]}"#, &from, &to),
        r#"{"m":[[1,"one"],[2,"two"]]}"#
    );
}

#[test]
fn migrate_inline_runs_nested_frame() {
    let from = strct(0, &[("inner", 0, strct(0, &[("x", 0, p(PK::U16))]))]);
    let to = strct(0, &[("inner", 0, strct(0, &[("x", 0, p(PK::U64))]))]);
    let ops = vec![MigrationOp::MigrateInline {
        at: path(&["inner"]),
        ops: vec![MigrationOp::Widen {
            from: path(&["x"]),
            to: path(&["x"]),
        }],
    }];
    assert_eq!(
        run(&ops, r#"{"inner":{"x":3}}"#, &from, &to),
        r#"{"inner":{"x":3}}"#
    );
}

#[test]
fn input_struct_of_the_wrong_arity_is_corrupt_input() {
    let from = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, p(PK::U32))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let ops = vec![copy(&["a"], &["a"])];
    let input = Value::Struct(Box::new([Value::U32(1)]));
    let err = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap_err();
    assert!(matches!(err, MigrationError::InputShape { .. }), "{err:?}");
}

// NOTE: `MigrationOp::Custom` was removed from the vocabulary (R20/M21);
// custom logic is whole-edge only (`MigrationKind::Function`).

// --- execute_edge ----------------------------------------------------------

#[test]
fn edge_ops_are_validated_before_execution() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = from.clone();
    // Valid ops but a default-table op: rejected for a CUSTOM edge.
    let kind = MigrationKind::Ops(vec![MigrationOp::WriteFieldDefault { to: path(&["a"]) }]);
    let err = execute_edge(
        &kind,
        &val(&from, r#"{"a":1}"#),
        &from,
        &to,
        &NoDefaults,
        &Fns::default(),
    )
    .unwrap_err();
    assert!(matches!(err, MigrationError::PlanInvalid(_)), "{err:?}");
}

#[test]
fn edge_ops_happy_path_runs_and_conforms() {
    let from = strct(0, &[("a", 0, p(PK::U16))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let kind = MigrationKind::Ops(vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }]);
    let out = execute_edge(
        &kind,
        &val(&from, r#"{"a":9}"#),
        &from,
        &to,
        &NoDefaults,
        &Fns::default(),
    )
    .unwrap();
    assert_eq!(txt(&to, &out.value), r#"{"a":9}"#);
}

#[test]
fn function_edge_runs() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let mut fns = Fns::default();
    fns.table.insert("bump".to_string(), |v| {
        let Value::Struct(fields) = v else {
            return Err("not a struct".into());
        };
        let [Value::U32(n)] = *fields else {
            return Err("a not u32".into());
        };
        Ok(Value::Struct(Box::new([Value::U32(n + 1)])))
    });
    let kind = MigrationKind::Function("bump".to_string());
    let out = execute_edge(&kind, &val(&from, r#"{"a":1}"#), &from, &to, &NoDefaults, &fns).unwrap();
    assert_eq!(txt(&to, &out.value), r#"{"a":2}"#);
    assert!(out.defaulted.is_empty());
}

#[test]
fn function_edge_bad_output_caught_by_conformance_naming_edge() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let mut fns = Fns::default();
    fns.table
        .insert("bad".to_string(), |_| Ok(Value::String("nope".into())));
    let kind = MigrationKind::Function("bad".to_string());
    let err = execute_edge(&kind, &val(&from, r#"{"a":1}"#), &from, &to, &NoDefaults, &fns)
        .unwrap_err();
    let MigrationError::NonConforming { edge, .. } = &err else {
        panic!("expected NonConforming, got {err:?}");
    };
    assert!(edge.contains("bad"), "edge name missing: {edge}");
}

#[test]
fn ops_edge_bad_output_caught_by_conformance() {
    // A validated plan can still produce a non-conforming value (a value
    // of the wrong kind reaches a mismatched leaf via CopyField).
    let from = strct(0, &[("a", 0, SchemaNode::String)]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let kind = MigrationKind::Ops(vec![MigrationOp::CopyField {
        from: path(&["a"]),
        to: path(&["a"]),
    }]);
    let err = execute_edge(
        &kind,
        &val(&from, r#"{"a":"not a number"}"#),
        &from,
        &to,
        &NoDefaults,
        &Fns::default(),
    )
    .unwrap_err();
    assert!(
        matches!(err, MigrationError::NonConforming { .. }),
        "{err:?}"
    );
}

#[test]
fn unknown_fn_key_is_error() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = from.clone();
    let kind = MigrationKind::Function("ghost".to_string());
    let err = execute_edge(
        &kind,
        &val(&from, r#"{"a":1}"#),
        &from,
        &to,
        &NoDefaults,
        &Fns::default(),
    )
    .unwrap_err();
    assert!(
        matches!(&err, MigrationError::FunctionFailed { key, .. } if key == "ghost"),
        "{err:?}"
    );
}

#[test]
fn grow_array_takes_the_added_slots_from_the_field_default() {
    let from = strct(0, &[("a", 0, arr(2, p(PK::U8)))]);
    let to = strct(0, &[("a", 0, arr(4, p(PK::U16)))]);
    let ops = distill_migrate::plan_automatic(&from, &to).unwrap();
    let mut defaults = TableDefaults::default();
    defaults.field.insert(path(&["a"]), val(&arr(4, p(PK::U16)), "[7,8,9,10]"));
    let out = execute_ops(&ops, &val(&from, r#"{"a":[1,2]}"#), &from, &to, &defaults).unwrap();
    assert_eq!(txt(&to, &out.value), r#"{"a":[1,2,9,10]}"#);
    assert_eq!(out.defaulted, vec![path(&["a"])]);
}

#[test]
fn grow_array_without_a_default_fails_hard() {
    let from = strct(0, &[("a", 0, arr(2, p(PK::U8)))]);
    let to = strct(0, &[("a", 0, arr(3, p(PK::U8)))]);
    let ops = distill_migrate::plan_automatic(&from, &to).unwrap();
    let err = execute_ops(&ops, &val(&from, r#"{"a":[1,2]}"#), &from, &to, &NoDefaults).unwrap_err();
    assert!(matches!(&err, MigrationError::MissingDefault { op: "GrowArray", .. }), "{err:?}");
}

#[test]
fn grow_array_is_automatic_segment_only() {
    let from = strct(0, &[("a", 0, arr(2, p(PK::U8)))]);
    let to = strct(0, &[("a", 0, arr(3, p(PK::U8)))]);
    let ops = distill_migrate::plan_automatic(&from, &to).unwrap();
    let errors = distill_migrate::validate_plan(&ops, &from, &to, distill_migrate::EdgeKind::Custom).unwrap_err();
    assert!(
        errors.iter().any(|e| matches!(e, distill_migrate::PlanError::DefaultOpInCustomEdge { op: "GrowArray", .. })),
        "{errors:?}"
    );
    distill_migrate::validate_plan(&ops, &from, &to, distill_migrate::EdgeKind::Automatic).unwrap();
}

#[test]
fn sparse_grow_array_inherits_the_added_slots() {
    let elem = strct(0, &[("x", 0, p(PK::U8)), ("y", 0, p(PK::U8))]);
    let from = strct(0, &[("a", 0, arr(2, elem.clone()))]);
    let to = strct(0, &[("a", 0, arr(4, elem))]);
    let ops = distill_migrate::plan_automatic(&from, &to).unwrap();
    let input = patch(&from, r#"{"a":[{"x":1},{}]}"#);
    let out = distill_migrate::execute_sparse(&ops, &input, &from, &to).unwrap();
    assert_eq!(ptxt(&to, &out), r#"{"a":[{"x":1},{},{},{}]}"#);
}

#[test]
fn an_added_variant_moving_the_others_keeps_values_by_name() {
    // "Body" sorts between "Board" and "Tie": "Tie" moves from 1 to 2. The
    // planner maps it by name; a plain copy would read it as "Body".
    let tie = strct(0, &[("lead", 0, p(PK::F32)), ("turns", 0, p(PK::Bool))]);
    let old = strct(0, &[("kind", 0, enm(0, &[("Board", 0, unit_payload()), ("Tie", 0, tie.clone())]))]);
    let new = strct(
        0,
        &[("kind", 0, enm(0, &[("Board", 0, unit_payload()), ("Body", 0, unit_payload()), ("Tie", 0, tie)]))],
    );
    let ops = distill_migrate::plan_automatic(&old, &new).unwrap();
    for input in [r#"{"kind":{"Tie":{"lead":0.5,"turns":true}}}"#, r#"{"kind":{"Board":{}}}"#] {
        assert_eq!(run(&ops, input, &old, &new), input);
        let out = distill_migrate::execute_sparse(&ops, &patch(&old, input), &old, &new).unwrap();
        assert_eq!(ptxt(&new, &out), input);
    }
}
