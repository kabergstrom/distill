//! Schema-directed walk negatives: wrong shapes for `data` vs the schema
//! produce errors naming the structural path — canonical bundles are total
//! (§6 Adoption), so nothing is inferred or skipped.

mod common;

use common::*;
use distill_bundle::{parse_bundle, write_bundle, BundleError as E};
use distill_json::AuthoredValue as V;
use ngp_schema::{PrimitiveKind as PK, SchemaNode as N};

/// Parse a plain bundle whose single entry "a" carries `data` under the
/// given schema, expecting an error.
fn walk_err(sc: &ngp_schema::LogicalSchema, data: V) -> E {
    let b = bundle(&[sc], vec![("a", entry(UUID_A, sc, data))], None);
    parse_bundle(&plain_bytes(&b)).unwrap_err()
}

#[test]
fn missing_struct_field() {
    let sc = simple_schema();
    let err = walk_err(&sc, obj(&[("count", u(1))]));
    assert!(
        matches!(&err, E::MissingField { local_id, path, field }
            if local_id == "a" && path == "data" && field == "name"),
        "got {err:?}"
    );
}

#[test]
fn extra_struct_field() {
    let sc = simple_schema();
    let err = walk_err(
        &sc,
        obj(&[("bogus", u(9)), ("count", u(1)), ("name", s("n"))]),
    );
    assert!(
        matches!(&err, E::ExtraField { path, field, .. } if path == "data" && field == "bogus"),
        "got {err:?}"
    );
}

#[test]
fn nested_shape_error_names_full_path() {
    let sc = schema(st(&[(
        "outer",
        st(&[("items", N::Vec(Box::new(N::String)))]),
    )]));
    let err = walk_err(
        &sc,
        obj(&[("outer", obj(&[("items", arr(vec![s("ok"), u(3)]))]))]),
    );
    assert!(
        matches!(&err, E::Shape { path, expected, found, .. }
            if path == "data.outer.items[1]" && expected.contains("string") && found.contains("integer")),
        "got {err:?}"
    );
}

#[test]
fn unknown_enum_variant() {
    let sc = schema(st(&[(
        "e",
        en(&[("A", st(&[("x", N::String)])), ("B", st(&[]))]),
    )]));
    let err = walk_err(&sc, obj(&[("e", obj(&[("C", obj(&[]))]))]));
    assert!(
        matches!(&err, E::UnknownVariant { path, variant, .. }
            if path == "data.e" && variant == "C"),
        "got {err:?}"
    );
}

#[test]
fn enum_value_must_be_single_key_object() {
    let sc = schema(st(&[("e", en(&[("A", st(&[])), ("B", st(&[]))]))]));
    let err = walk_err(&sc, obj(&[("e", obj(&[("A", obj(&[])), ("B", obj(&[]))]))]));
    assert!(
        matches!(&err, E::EnumShape { path, keys: 2, .. } if path == "data.e"),
        "got {err:?}"
    );
    let err = walk_err(&sc, obj(&[("e", obj(&[]))]));
    assert!(matches!(&err, E::EnumShape { keys: 0, .. }), "got {err:?}");
}

#[test]
fn enum_variant_payload_is_walked() {
    let sc = schema(st(&[(
        "e",
        en(&[("A", st(&[("x", N::String)])), ("B", st(&[]))]),
    )]));
    // Missing payload field inside the variant.
    let err = walk_err(&sc, obj(&[("e", obj(&[("A", obj(&[]))]))]));
    assert!(
        matches!(&err, E::MissingField { path, field, .. }
            if path == "data.e.<A>" && field == "x"),
        "got {err:?}"
    );
}

#[test]
fn fixed_array_length_mismatch() {
    let sc = schema(st(&[(
        "a",
        N::Array {
            len: 2,
            elem: Box::new(N::Primitive(PK::U8)),
        },
    )]));
    let err = walk_err(&sc, obj(&[("a", arr(vec![u(1)]))]));
    assert!(
        matches!(&err, E::ArrayLen { path, expected: 2, actual: 1, .. } if path == "data.a"),
        "got {err:?}"
    );
}

#[test]
fn primitive_range_and_shape_errors() {
    let sc = schema(st(&[
        ("b", N::Primitive(PK::U8)),
        ("c", N::Primitive(PK::Char)),
        ("f", N::Primitive(PK::F64)),
        ("un", N::Unit),
    ]));
    let ok = |field: &str, v: V| {
        let mut fields = vec![
            ("b", u(255)),
            ("c", s("x")),
            ("f", V::Float(1.5)),
            ("un", V::Null),
        ];
        for f in &mut fields {
            if f.0 == field {
                f.1 = v.clone();
            }
        }
        obj(&fields)
    };
    // u8 out of range.
    let err = walk_err(&sc, ok("b", u(256)));
    assert!(
        matches!(&err, E::Shape { path, .. } if path == "data.b"),
        "got {err:?}"
    );
    // char must be exactly one scalar.
    let err = walk_err(&sc, ok("c", s("ab")));
    assert!(
        matches!(&err, E::Shape { path, .. } if path == "data.c"),
        "got {err:?}"
    );
    // float leaf can't be a string.
    let err = walk_err(&sc, ok("f", s("1.5")));
    assert!(
        matches!(&err, E::Shape { path, .. } if path == "data.f"),
        "got {err:?}"
    );
    // unit is null, nothing else.
    let err = walk_err(&sc, ok("un", u(0)));
    assert!(
        matches!(&err, E::Shape { path, .. } if path == "data.un"),
        "got {err:?}"
    );
}

#[test]
fn f32_canonicality_is_shortest_ties_even_roundtrip() {
    let sc = schema(st(&[("f", N::Primitive(PK::F32))]));
    let valid = [
        V::Float(0.1),
        V::Float(1.0e-45),
        V::UInt(16_777_216),
        V::Float(-0.0),
    ];
    for value in valid {
        let b = bundle(
            &[&sc],
            vec![("a", entry(UUID_A, &sc, obj(&[("f", value)])))],
            None,
        );
        assert!(parse_bundle(&plain_bytes(&b)).is_ok());
        assert!(write_bundle(&b).is_ok());
    }

    let cases = [
        (V::Float(0.10000000149011612), "0.1"),
        // Exact midpoint between 1.0 and its next binary32 neighbour;
        // ties-to-even selects 1.0.
        (V::Float(1.0000000596046448), "1"),
        (V::UInt(16_777_217), "16777216"),
        // Below half the smallest subnormal, so ties-even parsing yields 0.
        (V::Float(7.0e-46), "0"),
    ];
    for (value, expected) in cases {
        let err = walk_err(&sc, obj(&[("f", value)]));
        assert!(
            matches!(&err, E::NonCanonicalF32 { path, canonical, .. }
                if path == "data.f" && canonical == expected),
            "got {err:?}"
        );
    }
}

#[test]
fn f32_overflow_is_rejected() {
    let sc = schema(st(&[("f", N::Primitive(PK::F32))]));
    let err = walk_err(&sc, obj(&[("f", V::Float(3.5e38))]));
    assert!(matches!(&err, E::F32OutOfRange { path, .. } if path == "data.f"));
}

#[test]
fn option_of_null_is_none_anything_else_walks_inner() {
    let sc = schema(st(&[("o", N::Option(Box::new(N::Primitive(PK::U8))))]));
    // Null = None: fine.
    let b = bundle(
        &[&sc],
        vec![("a", entry(UUID_A, &sc, obj(&[("o", V::Null)])))],
        None,
    );
    assert!(parse_bundle(&plain_bytes(&b)).is_ok());
    // Non-null walks the inner node: a string is a shape error there.
    let err = walk_err(&sc, obj(&[("o", s("no"))]));
    assert!(
        matches!(&err, E::Shape { path, .. } if path == "data.o"),
        "got {err:?}"
    );
}

#[test]
fn string_key_map_needs_object_nonstring_needs_pairs() {
    let string_map = schema(st(&[(
        "m",
        N::Map {
            key: Box::new(N::String),
            value: Box::new(N::Primitive(PK::U8)),
        },
    )]));
    let err = walk_err(&string_map, obj(&[("m", arr(vec![]))]));
    assert!(
        matches!(&err, E::Shape { path, .. } if path == "data.m"),
        "got {err:?}"
    );

    let int_map = schema(st(&[(
        "m",
        N::Map {
            key: Box::new(N::Primitive(PK::I64)),
            value: Box::new(N::String),
        },
    )]));
    let err = walk_err(&int_map, obj(&[("m", obj(&[("1", s("x"))]))]));
    assert!(
        matches!(&err, E::Shape { path, .. } if path == "data.m"),
        "got {err:?}"
    );
    // A pair must be exactly [k, v].
    let err = walk_err(&int_map, obj(&[("m", arr(vec![arr(vec![u(1)])]))]));
    assert!(
        matches!(&err, E::Shape { path, .. } if path == "data.m[0]"),
        "got {err:?}"
    );
}

#[test]
fn set_elements_must_be_strictly_ascending() {
    let sc = schema(st(&[("s", N::Set(Box::new(N::String)))]));
    let err = walk_err(&sc, obj(&[("s", arr(vec![s("b"), s("a")]))]));
    assert!(
        matches!(&err, E::NotSorted { path, what: "set elements", index: 1, .. }
            if path == "data.s"),
        "got {err:?}"
    );
    // Duplicates are "not strictly ascending" too (§6: parse error).
    let err = walk_err(&sc, obj(&[("s", arr(vec![s("a"), s("a")]))]));
    assert!(matches!(&err, E::NotSorted { index: 1, .. }), "got {err:?}");
}

#[test]
fn map_keys_must_be_strictly_ascending_by_encoded_bytes() {
    let sc = schema(st(&[(
        "m",
        N::Map {
            key: Box::new(N::Primitive(PK::I64)),
            value: Box::new(N::String),
        },
    )]));
    // "-1" < "2" byte-wise, so [[2,..],[-1,..]] is unsorted.
    let err = walk_err(
        &sc,
        obj(&[(
            "m",
            arr(vec![arr(vec![u(2), s("x")]), arr(vec![V::Int(-1), s("y")])]),
        )]),
    );
    assert!(
        matches!(&err, E::NotSorted { path, what: "map keys", index: 1, .. }
            if path == "data.m"),
        "got {err:?}"
    );
}

#[test]
fn blob_beneath_map_key_is_barred() {
    // §5: blobs are barred anywhere in the subtree of a map key (the
    // ordering/offset circularity). The schema itself can spell it — the
    // walk rejects it with the path.
    let sc = schema(st(&[(
        "mk",
        N::Map {
            key: Box::new(st(&[("b", N::Blob)])),
            value: Box::new(N::String),
        },
    )]));
    let err = walk_err(
        &sc,
        obj(&[("mk", arr(vec![arr(vec![obj(&[("b", V::Null)]), s("v")])]))]),
    );
    assert!(
        matches!(&err, E::BlobBarred { path, .. } if path.contains("mk")),
        "got {err:?}"
    );
}

#[test]
fn blob_beneath_set_element_is_barred() {
    let sc = schema(st(&[("se", N::Set(Box::new(st(&[("b", N::Blob)]))))]));
    let err = walk_err(&sc, obj(&[("se", arr(vec![obj(&[("b", V::Null)])]))]));
    assert!(
        matches!(&err, E::BlobBarred { path, .. } if path.contains("se")),
        "got {err:?}"
    );
}

#[test]
fn bad_blob_object_keys() {
    // Container encoding, blob leaf that is not exactly {"len","offset"}.
    let sc = schema(st(&[("p", N::Blob)]));
    let cases = [
        obj(&[("len", u(0))]),                                // missing offset
        obj(&[("len", u(0)), ("offset", u(0)), ("x", u(0))]), // extra key
        obj(&[("len", s("0")), ("offset", u(0))]),            // wrong type
        s("blob"),                                            // not an object
    ];
    for data in cases {
        let b = bundle(
            &[&sc],
            vec![("a", entry(UUID_A, &sc, obj(&[("p", data.clone())])))],
            None,
        );
        let bytes = build_container(&plain_bytes(&b), &[]);
        let err = parse_bundle(&bytes).unwrap_err();
        assert!(
            matches!(&err, E::BadBlobObject { local_id, path, .. }
                if local_id == "a" && path == "data.p"),
            "case {data:?}: got {err:?}"
        );
    }
}

// ---- writer-side walk negatives ----

#[test]
fn write_rejects_blob_value_at_non_blob_position() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(
                UUID_A,
                &sc,
                obj(&[("count", blob(b"zz")), ("name", s("n"))]),
            ),
        )],
        None,
    );
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(&err, E::Shape { path, found, .. } if path == "data.count" && found.contains("blob")),
        "got {err:?}"
    );
}

#[test]
fn write_rejects_non_blob_value_at_blob_position() {
    let sc = schema(st(&[("p", N::Blob)]));
    let b = bundle(
        &[&sc],
        vec![("a", entry(UUID_A, &sc, obj(&[("p", s("not bytes"))])))],
        None,
    );
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(&err, E::Shape { path, expected, .. } if path == "data.p" && expected.contains("blob")),
        "got {err:?}"
    );
}

#[test]
fn write_rejects_non_finite_float_with_path() {
    let sc = schema(st(&[("f", N::Primitive(PK::F64))]));
    let b = bundle(
        &[&sc],
        vec![("a", entry(UUID_A, &sc, obj(&[("f", V::Float(f64::NAN))])))],
        None,
    );
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(&err, E::NonFiniteFloat { path, .. } if path == "data.f"),
        "got {err:?}"
    );
}

#[test]
fn write_rejects_backref_past_outermost_frame() {
    // Snapshot decoding rejects this statically; the walker still guards
    // programmatic input.
    let sc = schema(st(&[("x", N::BackRef(3))]));
    let b = bundle(
        &[&sc],
        vec![("a", entry(UUID_A, &sc, obj(&[("x", obj(&[]))])))],
        None,
    );
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(&err, E::BadBackRef { path, distance: 3, frames: 1, .. } if path == "data.x"),
        "got {err:?}"
    );
}

#[test]
fn walk_depth_is_capped_never_a_stack_overflow() {
    // A recursive tree nested far past any legitimate bundle: the walk
    // reports a definite error instead of overflowing the stack. Built by
    // move (obj() clones, and cloning a deep value would itself recurse).
    let sc = tree_schema();
    let mut node = obj(&[("children", arr(vec![])), ("payload", blob(b"x"))]);
    for _ in 0..600 {
        let mut m = std::collections::BTreeMap::new();
        m.insert("children".to_string(), arr(vec![node]));
        m.insert("payload".to_string(), blob(b"x"));
        node = V::Object(m);
    }
    let b = bundle(&[&sc], vec![("t", entry(UUID_A, &sc, node))], None);
    let err = write_bundle(&b).unwrap_err();
    assert!(matches!(err, E::WalkDepth { .. }), "got {err:?}");
}

#[test]
fn hostile_option_chain_schema_hits_walk_cap_on_parse() {
    // Option and BackRef steps consume no data nesting, so a hand-crafted
    // schema can multiply walk depth per data level far past the JSON
    // depth cap. The walker's own cap must fire as a definite error before
    // the stack is at risk.
    let mut chain = N::BackRef(0);
    for _ in 0..300 {
        chain = N::Option(Box::new(chain));
    }
    let sc = schema(st(&[("x", chain)]));
    // 30 data levels x ~302 walk frames per level >> MAX_WALK_DEPTH.
    let mut node = obj(&[("x", V::Null)]);
    for _ in 0..30 {
        let mut m = std::collections::BTreeMap::new();
        m.insert("x".to_string(), node);
        node = V::Object(m);
    }
    let b = bundle(&[&sc], vec![("a", entry(UUID_A, &sc, node))], None);
    let err = parse_bundle(&plain_bytes(&b)).unwrap_err();
    assert!(matches!(err, E::WalkDepth { .. }), "got {err:?}");
}

#[test]
fn write_rejects_unsorted_set() {
    let sc = schema(st(&[("s", N::Set(Box::new(N::String)))]));
    let b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("s", arr(vec![s("b"), s("a")]))])),
        )],
        None,
    );
    let err = write_bundle(&b).unwrap_err();
    assert!(matches!(err, E::NotSorted { .. }), "got {err:?}");
}
