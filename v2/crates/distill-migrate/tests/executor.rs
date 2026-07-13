//! Executor (§11): immutable input, fresh output, order-independence,
//! fail-hard integrity.

mod common;
use common::*;
use distill_json::AuthoredValue;
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

#[test]
fn copy_twice_from_one_source_proves_immutable_input() {
    // CopyField COPIES — the source stays readable to later ops (§11).
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = strct(0, &[("x", 0, p(PK::U32)), ("y", 0, p(PK::U32))]);
    let ops = vec![copy(&["a"], &["x"]), copy(&["a"], &["y"])];
    let input = obj(&[("a", ui(41))]);
    let out = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("x", ui(41)), ("y", ui(41))]));
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
    let input = obj(&[("a", ui(1)), ("b", st("hi")), ("c", ui(9))]);
    let r1 = execute_ops(&ops1, &input, &from, &to, &NoDefaults).unwrap();
    let r2 = execute_ops(&ops2, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(r1.value, r2.value);
}

#[test]
fn widen_passes_value_through_with_normalization() {
    let from = strct(0, &[("a", 0, p(PK::I8))]);
    let to = strct(0, &[("a", 0, p(PK::I64))]);
    let ops = vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }];
    // Negative stays Int.
    let out = execute_ops(&ops, &obj(&[("a", int(-3))]), &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("a", int(-3))]));
    // Non-negative normalizes to UInt (the pinned parse-variant split).
    let out = execute_ops(&ops, &obj(&[("a", int(5))]), &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("a", ui(5))]));
    // UInt stays UInt.
    let out = execute_ops(&ops, &obj(&[("a", ui(7))]), &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("a", ui(7))]));
}

#[test]
fn widen_float_passes_through() {
    let from = strct(0, &[("a", 0, p(PK::F32))]);
    let to = strct(0, &[("a", 0, p(PK::F64))]);
    let ops = vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }];
    let out = execute_ops(&ops, &obj(&[("a", fl(1.5))]), &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("a", fl(1.5))]));
}

#[test]
fn widen_value_outside_old_range_is_corrupt_input() {
    let from = strct(0, &[("a", 0, p(PK::U8))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let ops = vec![MigrationOp::Widen {
        from: path(&["a"]),
        to: path(&["a"]),
    }];
    let err = execute_ops(&ops, &obj(&[("a", ui(300))]), &from, &to, &NoDefaults).unwrap_err();
    assert!(
        matches!(err, MigrationError::WidenOutOfRange { .. }),
        "{err:?}"
    );
    // Negative into an unsigned old leaf: same class.
    let err = execute_ops(&ops, &obj(&[("a", int(-1))]), &from, &to, &NoDefaults).unwrap_err();
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
            value: st("lit"),
        },
    ];
    let out = execute_ops(&ops, &obj(&[]), &from, &to, &NoDefaults).unwrap();
    assert_eq!(
        out.value,
        obj(&[("o", AuthoredValue::Null), ("v", st("lit"))])
    );
}

#[test]
fn missing_default_fails_hard() {
    // Integrity: a None from the provider is an error, never a fabricated
    // value (§11).
    let from = strct(0, &[]);
    let to = strct(0, &[("b", 0, p(PK::U32))]);
    let ops = vec![MigrationOp::WriteFieldDefault { to: path(&["b"]) }];
    let err = execute_ops(&ops, &obj(&[]), &from, &to, &NoDefaults).unwrap_err();
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
    defaults.field.insert(path(&["b"]), ui(11));
    defaults.parent.insert(path(&["c"]), ui(22));
    let ops = vec![
        MigrationOp::WriteFieldDefault { to: path(&["b"]) },
        MigrationOp::WriteParentDefault { to: path(&["c"]) },
    ];
    let out = execute_ops(&ops, &obj(&[]), &from, &to, &defaults).unwrap();
    assert_eq!(out.value, obj(&[("b", ui(11)), ("c", ui(22))]));
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
    defaults.field.insert(path(&["y"]), ui(0));
    let ops = vec![MigrationOp::MigrateElements {
        at: path(&["items"]),
        element: vec![
            copy(&["x"], &["x"]),
            MigrationOp::WriteFieldDefault { to: path(&["y"]) },
        ],
    }];
    let input = obj(&[(
        "items",
        arr_v(&[obj(&[("x", ui(1))]), obj(&[("x", ui(2))])]),
    )]);
    let out = execute_ops(&ops, &input, &from, &to, &defaults).unwrap();
    assert_eq!(
        out.value,
        obj(&[(
            "items",
            arr_v(&[
                obj(&[("x", ui(1)), ("y", ui(0))]),
                obj(&[("x", ui(2)), ("y", ui(0))])
            ])
        )])
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
            ("Old", 0, strct(0, &[("n", 0, p(PK::U32))])),
            ("Keep", 0, unit_payload()),
        ],
    );
    let new_e = enm(
        0,
        &[
            ("New", 0, strct(0, &[("n", 0, p(PK::U64))])),
            ("Keep", 0, unit_payload()),
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
    let input = obj(&[("e", obj(&[("Old", obj(&[("n", ui(5))]))]))]);
    let out = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(
        out.value,
        obj(&[("e", obj(&[("New", obj(&[("n", ui(5))]))]))])
    );
    // A variant no MapVariant names passes through unchanged.
    let input = obj(&[("e", obj(&[("Keep", obj(&[]))]))]);
    let out = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("e", obj(&[("Keep", obj(&[]))]))]));
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
    let input = obj(&[("v", arr_v(&[ui(1), ui(2), ui(3)]))]);
    let out = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("v", arr_v(&[ui(1), ui(2), ui(3)]))]));
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
    let out = execute_ops(
        &ops,
        &obj(&[("o", AuthoredValue::Null)]),
        &from,
        &to,
        &NoDefaults,
    )
    .unwrap();
    assert_eq!(out.value, obj(&[("o", AuthoredValue::Null)]));
    let out = execute_ops(&ops, &obj(&[("o", ui(6))]), &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("o", ui(6))]));
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
    let input = obj(&[("m", obj(&[("k1", ui(1)), ("k2", ui(2))]))]);
    let out = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(
        out.value,
        obj(&[("m", obj(&[("k1", ui(1)), ("k2", ui(2))]))])
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
            value: ui(1),
        }],
    }];
    let input = obj(&[("s", arr_v(&[ui(1), ui(2)]))]);
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
            value: st("same"),
        }],
    }];
    let input = obj(&[("m", obj(&[("a", ui(1)), ("b", ui(2))]))]);
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
    let input = obj(&[(
        "m",
        arr_v(&[arr_v(&[ui(1), st("one")]), arr_v(&[ui(2), st("two")])]),
    )]);
    let out = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(
        out.value,
        obj(&[(
            "m",
            arr_v(&[arr_v(&[ui(1), st("one")]), arr_v(&[ui(2), st("two")])])
        )])
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
    let input = obj(&[("inner", obj(&[("x", ui(3))]))]);
    let out = execute_ops(&ops, &input, &from, &to, &NoDefaults).unwrap();
    assert_eq!(out.value, obj(&[("inner", obj(&[("x", ui(3))]))]));
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
        &obj(&[("a", ui(1))]),
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
        &obj(&[("a", ui(9))]),
        &from,
        &to,
        &NoDefaults,
        &Fns::default(),
    )
    .unwrap();
    assert_eq!(out.value, obj(&[("a", ui(9))]));
}

#[test]
fn function_edge_runs() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let mut fns = Fns::default();
    fns.table.insert("bump".to_string(), |v| {
        let AuthoredValue::Object(mut m) = v else {
            return Err("not an object".into());
        };
        let AuthoredValue::UInt(n) = m["a"] else {
            return Err("a not uint".into());
        };
        m.insert("a".to_string(), AuthoredValue::UInt(n + 1));
        Ok(AuthoredValue::Object(m))
    });
    let kind = MigrationKind::Function("bump".to_string());
    let out = execute_edge(&kind, &obj(&[("a", ui(1))]), &from, &to, &NoDefaults, &fns).unwrap();
    assert_eq!(out.value, obj(&[("a", ui(2))]));
    assert!(out.defaulted.is_empty());
}

#[test]
fn function_edge_bad_output_caught_by_conformance_naming_edge() {
    let from = strct(0, &[("a", 0, p(PK::U32))]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let mut fns = Fns::default();
    fns.table
        .insert("bad".to_string(), |_| Ok(AuthoredValue::Str("nope".into())));
    let kind = MigrationKind::Function("bad".to_string());
    let err =
        execute_edge(&kind, &obj(&[("a", ui(1))]), &from, &to, &NoDefaults, &fns).unwrap_err();
    let MigrationError::NonConforming { edge, .. } = &err else {
        panic!("expected NonConforming, got {err:?}");
    };
    assert!(edge.contains("bad"), "edge name missing: {edge}");
}

#[test]
fn ops_edge_bad_output_caught_by_conformance() {
    // A validated plan can still produce a non-conforming value (a literal
    // of the wrong shape reaches a mismatched leaf via CopyField).
    let from = strct(0, &[("a", 0, SchemaNode::String)]);
    let to = strct(0, &[("a", 0, p(PK::U32))]);
    let kind = MigrationKind::Ops(vec![MigrationOp::CopyField {
        from: path(&["a"]),
        to: path(&["a"]),
    }]);
    let err = execute_edge(
        &kind,
        &obj(&[("a", st("not a number"))]),
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
        &obj(&[("a", ui(1))]),
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
