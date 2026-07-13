//! Conformance checker (§11): value-against-schema, every node kind,
//! positive and negative.

mod common;
use common::*;
use distill_json::AuthoredValue;
use distill_migrate::conforms;
use ngp_schema::SchemaNode;

#[test]
fn bool_leaf() {
    conforms(&AuthoredValue::Bool(true), &p(PK::Bool)).unwrap();
    conforms(&ui(1), &p(PK::Bool)).unwrap_err();
}

#[test]
fn int_leaves_are_range_checked() {
    conforms(&ui(255), &p(PK::U8)).unwrap();
    conforms(&ui(256), &p(PK::U8)).unwrap_err();
    conforms(&int(-128), &p(PK::I8)).unwrap();
    conforms(&int(-129), &p(PK::I8)).unwrap_err();
    conforms(&int(-1), &p(PK::U32)).unwrap_err();
    conforms(&ui(u64::MAX as u128), &p(PK::U64)).unwrap();
    conforms(&ui(u64::MAX as u128 + 1), &p(PK::U64)).unwrap_err();
    // PINNED: either integer variant is accepted when in range — the
    // canonical writer prints both identically.
    conforms(&int(5), &p(PK::U8)).unwrap();
    // Non-integer shapes are not integers.
    conforms(&fl(1.0), &p(PK::U8)).unwrap_err();
    conforms(&st("1"), &p(PK::U8)).unwrap_err();
}

#[test]
fn i128_u128_leaves() {
    conforms(&ui(u128::MAX), &p(PK::U128)).unwrap();
    conforms(&int(i128::MIN), &p(PK::I128)).unwrap();
    conforms(&ui(u128::MAX), &p(PK::I128)).unwrap_err();
}

#[test]
fn float_leaves() {
    conforms(&fl(1.5), &p(PK::F64)).unwrap();
    conforms(&fl(1.5), &p(PK::F32)).unwrap();
    // PINNED: an F32 leaf requires an exactly-f32-representable value.
    conforms(&fl(1.1), &p(PK::F32)).unwrap_err();
    conforms(&ui(1), &p(PK::F64)).unwrap_err();
}

#[test]
fn char_is_single_scalar_string() {
    // PINNED: char encodes as a single-scalar-value JSON string.
    conforms(&st("x"), &p(PK::Char)).unwrap();
    conforms(&st("é"), &p(PK::Char)).unwrap();
    conforms(&st(""), &p(PK::Char)).unwrap_err();
    conforms(&st("ab"), &p(PK::Char)).unwrap_err();
    conforms(&ui(65), &p(PK::Char)).unwrap_err();
}

#[test]
fn string_leaf() {
    conforms(&st("hello"), &SchemaNode::String).unwrap();
    conforms(&ui(1), &SchemaNode::String).unwrap_err();
}

#[test]
fn unit_is_null() {
    conforms(&AuthoredValue::Null, &SchemaNode::Unit).unwrap();
    conforms(&obj(&[]), &SchemaNode::Unit).unwrap_err();
}

#[test]
fn option_is_null_or_inner() {
    let s = opt(p(PK::U8));
    conforms(&AuthoredValue::Null, &s).unwrap();
    conforms(&ui(3), &s).unwrap();
    conforms(&st("no"), &s).unwrap_err();
}

#[test]
fn blob_leaf() {
    conforms(&AuthoredValue::Blob(vec![1, 2]), &SchemaNode::Blob).unwrap();
    conforms(&st("AAEC"), &SchemaNode::Blob).unwrap_err();
}

#[test]
fn vec_node() {
    let s = vec_of(p(PK::U8));
    conforms(&arr_v(&[ui(1), ui(2), ui(1)]), &s).unwrap(); // dups fine in Vec
    conforms(&arr_v(&[ui(1), st("x")]), &s).unwrap_err();
    conforms(&obj(&[]), &s).unwrap_err();
}

#[test]
fn array_len_is_exact() {
    let s = arr(2, p(PK::U8));
    conforms(&arr_v(&[ui(1), ui(2)]), &s).unwrap();
    conforms(&arr_v(&[ui(1)]), &s).unwrap_err();
    conforms(&arr_v(&[ui(1), ui(2), ui(3)]), &s).unwrap_err();
}

#[test]
fn set_rejects_duplicate_and_unsorted_elements() {
    let s = set_of(p(PK::U8));
    conforms(&arr_v(&[ui(1), ui(2)]), &s).unwrap();
    conforms(&arr_v(&[ui(1), ui(1)]), &s).unwrap_err();
    // PINNED: canonical set order (by encoded bytes) is part of conformance.
    conforms(&arr_v(&[ui(2), ui(1)]), &s).unwrap_err();
}

#[test]
fn string_key_map_is_object() {
    let s = map_of(SchemaNode::String, p(PK::U8));
    conforms(&obj(&[("a", ui(1)), ("b", ui(2))]), &s).unwrap();
    conforms(&arr_v(&[]), &s).unwrap_err();
    conforms(&obj(&[("a", st("x"))]), &s).unwrap_err();
}

#[test]
fn nonstring_key_map_is_sorted_pair_array() {
    let s = map_of(p(PK::U8), SchemaNode::String);
    conforms(
        &arr_v(&[arr_v(&[ui(1), st("a")]), arr_v(&[ui(2), st("b")])]),
        &s,
    )
    .unwrap();
    // Duplicate encoded keys are an error.
    conforms(
        &arr_v(&[arr_v(&[ui(1), st("a")]), arr_v(&[ui(1), st("b")])]),
        &s,
    )
    .unwrap_err();
    // Malformed pair.
    conforms(&arr_v(&[arr_v(&[ui(1)])]), &s).unwrap_err();
    // Object form is wrong for non-string keys.
    conforms(&obj(&[("1", st("a"))]), &s).unwrap_err();
}

#[test]
fn struct_requires_exact_field_set() {
    let s = strct(0, &[("a", 0, p(PK::U8)), ("b", 0, SchemaNode::String)]);
    conforms(&obj(&[("a", ui(1)), ("b", st("x"))]), &s).unwrap();
    // Missing field.
    let err = conforms(&obj(&[("a", ui(1))]), &s).unwrap_err();
    assert!(
        err.expected.contains('b') || err.path.contains('b'),
        "{err}"
    );
    // Extra field.
    conforms(&obj(&[("a", ui(1)), ("b", st("x")), ("z", ui(9))]), &s).unwrap_err();
    // Not an object.
    conforms(&ui(1), &s).unwrap_err();
}

#[test]
fn enum_is_single_key_object_of_declared_variant() {
    let s = enm(
        0,
        &[
            ("A", 0, unit_payload()),
            ("B", 0, strct(0, &[("n", 0, p(PK::U8))])),
        ],
    );
    conforms(&obj(&[("A", obj(&[]))]), &s).unwrap();
    conforms(&obj(&[("B", obj(&[("n", ui(3))]))]), &s).unwrap();
    // Unknown variant.
    conforms(&obj(&[("C", obj(&[]))]), &s).unwrap_err();
    // Multi-key object.
    conforms(&obj(&[("A", obj(&[])), ("B", obj(&[("n", ui(1))]))]), &s).unwrap_err();
    // Zero-key object.
    conforms(&obj(&[]), &s).unwrap_err();
    // Payload must conform.
    let err = conforms(&obj(&[("B", obj(&[("n", ui(300))]))]), &s).unwrap_err();
    assert!(err.path.contains('B') || err.path.contains('n'), "{err}");
}

#[test]
fn asset_refs_accept_reference_query_forms() {
    // §4 pinned: uuid string, path string, {path, asset}, {asset}.
    for s in [
        SchemaNode::AssetRef(tuuid(7)),
        SchemaNode::WeakRef(tuuid(7)),
    ] {
        conforms(&st("91a2500a-3d63-4d5c-8a0b-6789abcdef01"), &s).unwrap();
        conforms(&st("characters/hero.bundle"), &s).unwrap();
        conforms(
            &obj(&[
                ("path", st("characters/hero.bundle")),
                ("asset", st("mesh/body")),
            ]),
            &s,
        )
        .unwrap();
        conforms(&obj(&[("asset", st("mesh/body"))]), &s).unwrap();
        // Empty string is not a reference.
        conforms(&st(""), &s).unwrap_err();
        // A bare {path} object is spelled as a plain string (§4 lists no
        // such form).
        conforms(&obj(&[("path", st("x.bundle"))]), &s).unwrap_err();
        // Unknown keys refused.
        conforms(&obj(&[("asset", st("a")), ("wat", ui(1))]), &s).unwrap_err();
        conforms(&ui(1), &s).unwrap_err();
    }
}

#[test]
fn backref_conforms_through_recursion() {
    // Tree { name: String, children: Vec<Tree> } via BackRef(0).
    let tree = strct(
        0,
        &[
            ("name", 0, SchemaNode::String),
            ("children", 0, vec_of(SchemaNode::BackRef(0))),
        ],
    );
    let leaf = obj(&[("name", st("kid")), ("children", arr_v(&[]))]);
    let root_v = obj(&[
        ("name", st("root")),
        ("children", arr_v(std::slice::from_ref(&leaf))),
    ]);
    conforms(&root_v, &tree).unwrap();
    // A malformed value two levels deep fails, naming a deep path.
    let bad_leaf = obj(&[("name", ui(1)), ("children", arr_v(&[]))]);
    let bad = obj(&[
        ("name", st("root")),
        (
            "children",
            arr_v(&[obj(&[
                ("name", st("mid")),
                ("children", arr_v(&[bad_leaf])),
            ])]),
        ),
    ]);
    let err = conforms(&bad, &tree).unwrap_err();
    assert!(err.path.contains("children"), "{err}");
}

#[test]
fn backref_through_enum_frames() {
    // Enum recursion: the variant payload does NOT open its own frame
    // (§5) — BackRef(0) inside a payload re-enters the ENUM.
    let e = enm(
        0,
        &[
            ("Leaf", 0, unit_payload()),
            ("Node", 0, strct(0, &[("next", 0, SchemaNode::BackRef(0))])),
        ],
    );
    let v = obj(&[("Node", obj(&[("next", obj(&[("Leaf", obj(&[]))]))]))]);
    conforms(&v, &e).unwrap();
    let bad = obj(&[("Node", obj(&[("next", ui(1))]))]);
    conforms(&bad, &e).unwrap_err();
}

#[test]
fn unresolvable_backref_is_error() {
    let s = strct(0, &[("r", 0, SchemaNode::BackRef(5))]);
    conforms(&obj(&[("r", obj(&[]))]), &s).unwrap_err();
}
