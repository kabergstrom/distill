//! Conformance checker (§11): value-against-schema, every node kind,
//! positive and negative. Values are positional and typed, so shape
//! errors are kind, arity, order and range-of-index errors.

mod common;
use common::*;
use distill_migrate::conforms;
use ngp_schema::{AssetRefValue, SchemaNode};

fn s(v: &str) -> Value {
    Value::String(v.into())
}

fn st(fields: Vec<Value>) -> Value {
    Value::Struct(fields.into())
}

fn seq(items: Vec<Value>) -> Value {
    Value::Seq(items)
}

fn none() -> Value {
    Value::Option(None)
}

fn some(v: Value) -> Value {
    Value::Option(Some(Box::new(v)))
}

#[test]
fn bool_leaf() {
    conforms(&Value::Bool(true), &p(PK::Bool)).unwrap();
    conforms(&Value::U8(1), &p(PK::Bool)).unwrap_err();
}

#[test]
fn int_leaves_take_exactly_their_kind() {
    conforms(&Value::U8(255), &p(PK::U8)).unwrap();
    conforms(&Value::U16(255), &p(PK::U8)).unwrap_err();
    conforms(&Value::I8(-128), &p(PK::I8)).unwrap();
    conforms(&Value::I16(-1), &p(PK::I8)).unwrap_err();
    conforms(&Value::I32(-1), &p(PK::U32)).unwrap_err();
    conforms(&Value::U64(u64::MAX), &p(PK::U64)).unwrap();
    conforms(&Value::U128(u128::MAX), &p(PK::U128)).unwrap();
    conforms(&Value::I128(i128::MIN), &p(PK::I128)).unwrap();
    conforms(&Value::U128(1), &p(PK::I128)).unwrap_err();
    // Non-integer shapes are not integers.
    conforms(&Value::F64(1.0), &p(PK::U8)).unwrap_err();
    conforms(&s("1"), &p(PK::U8)).unwrap_err();
}

#[test]
fn float_leaves_are_finite_and_of_their_width() {
    conforms(&Value::F64(1.5), &p(PK::F64)).unwrap();
    conforms(&Value::F32(1.5), &p(PK::F32)).unwrap();
    conforms(&Value::F64(1.5), &p(PK::F32)).unwrap_err();
    conforms(&Value::F32(f32::NAN), &p(PK::F32)).unwrap_err();
    conforms(&Value::F64(f64::INFINITY), &p(PK::F64)).unwrap_err();
    conforms(&Value::U32(1), &p(PK::F64)).unwrap_err();
}

#[test]
fn char_leaf() {
    conforms(&Value::Char('x'), &p(PK::Char)).unwrap();
    conforms(&Value::Char('é'), &p(PK::Char)).unwrap();
    conforms(&s("x"), &p(PK::Char)).unwrap_err();
    conforms(&Value::U32(65), &p(PK::Char)).unwrap_err();
}

#[test]
fn string_leaf() {
    conforms(&s("hello"), &SchemaNode::String).unwrap();
    conforms(&Value::U8(1), &SchemaNode::String).unwrap_err();
}

#[test]
fn unit_leaf() {
    conforms(&Value::Unit, &SchemaNode::Unit).unwrap();
    conforms(&st(vec![]), &SchemaNode::Unit).unwrap_err();
}

#[test]
fn option_is_none_or_inner() {
    let n = opt(p(PK::U8));
    conforms(&none(), &n).unwrap();
    conforms(&some(Value::U8(3)), &n).unwrap();
    conforms(&some(s("no")), &n).unwrap_err();
    // The inner value alone is not an option.
    conforms(&Value::U8(3), &n).unwrap_err();
}

#[test]
fn blob_leaf() {
    conforms(&Value::Blob(vec![1, 2]), &SchemaNode::Blob).unwrap();
    conforms(&s("AAEC"), &SchemaNode::Blob).unwrap_err();
}

#[test]
fn vec_node() {
    let n = vec_of(p(PK::U8));
    conforms(&seq(vec![Value::U8(1), Value::U8(2), Value::U8(1)]), &n).unwrap(); // dups fine in Vec
    conforms(&seq(vec![Value::U8(1), s("x")]), &n).unwrap_err();
    conforms(&st(vec![]), &n).unwrap_err();
}

#[test]
fn array_len_is_exact() {
    let n = arr(2, p(PK::U8));
    conforms(&seq(vec![Value::U8(1), Value::U8(2)]), &n).unwrap();
    conforms(&seq(vec![Value::U8(1)]), &n).unwrap_err();
    conforms(&seq(vec![Value::U8(1), Value::U8(2), Value::U8(3)]), &n).unwrap_err();
}

#[test]
fn set_rejects_duplicate_elements_in_any_order() {
    let n = set_of(p(PK::U8));
    conforms(&seq(vec![Value::U8(1), Value::U8(2)]), &n).unwrap();
    conforms(&seq(vec![Value::U8(1), Value::U8(1)]), &n).unwrap_err();
    // Order is free (writers canonicalize).
    conforms(&seq(vec![Value::U8(2), Value::U8(1)]), &n).unwrap();
    conforms(&seq(vec![Value::U8(2), Value::U8(1), Value::U8(2)]), &n).unwrap_err();
}

#[test]
fn string_key_map() {
    let n = map_of(SchemaNode::String, p(PK::U8));
    conforms(&val(&n, r#"{"a":1,"b":2}"#), &n).unwrap();
    conforms(&Value::Map(vec![(s("a"), s("x"))]), &n).unwrap_err();
    conforms(&seq(vec![]), &n).unwrap_err();
    // Order is free (writers canonicalize).
    conforms(
        &Value::Map(vec![(s("b"), Value::U8(1)), (s("a"), Value::U8(2))]),
        &n,
    )
    .unwrap();
}

#[test]
fn nonstring_key_map_keys_are_unique() {
    let n = map_of(p(PK::U8), SchemaNode::String);
    conforms(&val(&n, r#"[[1,"a"],[2,"b"]]"#), &n).unwrap();
    // Duplicate keys are an error.
    conforms(
        &Value::Map(vec![(Value::U8(1), s("a")), (Value::U8(1), s("b"))]),
        &n,
    )
    .unwrap_err();
    // Out of order is fine.
    conforms(
        &Value::Map(vec![(Value::U8(2), s("a")), (Value::U8(1), s("b"))]),
        &n,
    )
    .unwrap();
    // Wrong key kind.
    conforms(&Value::Map(vec![(s("1"), s("a"))]), &n).unwrap_err();
}

#[test]
fn struct_requires_exact_field_count() {
    let n = strct(0, &[("a", 0, p(PK::U8)), ("b", 0, SchemaNode::String)]);
    conforms(&val(&n, r#"{"a":1,"b":"x"}"#), &n).unwrap();
    // Missing field.
    let err = conforms(&st(vec![Value::U8(1)]), &n).unwrap_err();
    assert!(err.expected.contains('2'), "{err}");
    // Extra field.
    conforms(&st(vec![Value::U8(1), s("x"), Value::U8(9)]), &n).unwrap_err();
    // A field of the wrong kind names its path.
    let err = conforms(&st(vec![Value::U8(1), Value::U8(2)]), &n).unwrap_err();
    assert_eq!(err.path, "$.b");
    // Not a struct.
    conforms(&Value::U8(1), &n).unwrap_err();
}

#[test]
fn enum_names_a_declared_variant_with_its_payload() {
    let n = enm(
        0,
        &[
            ("A", 0, unit_payload()),
            ("B", 0, strct(0, &[("n", 0, p(PK::U8))])),
        ],
    );
    conforms(&val(&n, r#"{"A":{}}"#), &n).unwrap();
    conforms(&val(&n, r#"{"B":{"n":3}}"#), &n).unwrap();
    // Unknown variant index.
    conforms(
        &Value::Enum {
            variant: 2,
            payload: Box::new([]),
        },
        &n,
    )
    .unwrap_err();
    // Payload arity.
    conforms(
        &Value::Enum {
            variant: 0,
            payload: Box::new([Value::U8(1)]),
        },
        &n,
    )
    .unwrap_err();
    // Payload must conform.
    let err = conforms(
        &Value::Enum {
            variant: 1,
            payload: Box::new([Value::U16(300)]),
        },
        &n,
    )
    .unwrap_err();
    assert!(err.path.contains('B') || err.path.contains('n'), "{err}");
}

#[test]
fn asset_refs_accept_reference_query_forms() {
    // §4 pinned: uuid string, path string, {path, asset}, {asset}.
    let path = |p: &str| AssetRefValue::Path(p.into());
    let query = |p: Option<&str>, a: Option<&str>| AssetRefValue::Query {
        path: p.map(Into::into),
        asset: a.map(Into::into),
    };
    for (n, wrap) in [
        (
            SchemaNode::AssetRef(tuuid(7)),
            Value::AssetRef as fn(AssetRefValue) -> Value,
        ),
        (SchemaNode::WeakRef(tuuid(7)), Value::WeakRef),
    ] {
        conforms(&wrap(path("91a2500a-3d63-4d5c-8a0b-6789abcdef01")), &n).unwrap();
        conforms(&wrap(path("characters/hero.bundle")), &n).unwrap();
        conforms(
            &wrap(query(Some("characters/hero.bundle"), Some("mesh/body"))),
            &n,
        )
        .unwrap();
        conforms(&wrap(query(None, Some("mesh/body"))), &n).unwrap();
        // Empty string is not a reference.
        conforms(&wrap(path("")), &n).unwrap_err();
        // A bare {path} object is spelled as a plain string (§4 lists no
        // such form).
        conforms(&wrap(query(Some("x.bundle"), None)), &n).unwrap_err();
        conforms(&Value::U8(1), &n).unwrap_err();
    }
    // A weak reference is not a strong one.
    conforms(&Value::WeakRef(path("a")), &SchemaNode::AssetRef(tuuid(7))).unwrap_err();
}

#[test]
fn backref_conforms_through_recursion() {
    // Tree { children: Vec<Tree>, name: String } via BackRef(0).
    let tree = strct(
        0,
        &[
            ("children", 0, vec_of(SchemaNode::BackRef(0))),
            ("name", 0, SchemaNode::String),
        ],
    );
    conforms(
        &val(&tree, r#"{"children":[{"children":[],"name":"kid"}],"name":"root"}"#),
        &tree,
    )
    .unwrap();
    // A malformed value two levels deep fails, naming a deep path.
    let bad_leaf = st(vec![seq(vec![]), Value::U8(1)]);
    let bad = st(vec![
        seq(vec![st(vec![seq(vec![bad_leaf]), s("mid")])]),
        s("root"),
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
    conforms(&val(&e, r#"{"Node":{"next":{"Leaf":{}}}}"#), &e).unwrap();
    let bad = Value::Enum {
        variant: 1,
        payload: Box::new([Value::U8(1)]),
    };
    conforms(&bad, &e).unwrap_err();
}

#[test]
fn unresolvable_backref_is_error() {
    let n = strct(0, &[("r", 0, SchemaNode::BackRef(5))]);
    conforms(&st(vec![st(vec![])]), &n).unwrap_err();
}

#[test]
fn backref_reentry_resolves_under_the_targets_own_ancestors() {
    // A { b: Option<B> }, B { a: Option<A>, b: Option<B> }. B.b re-enters
    // B as BackRef(0); inside that B, `a` is BackRef(1) and names A.
    let n = strct(
        0,
        &[(
            "b",
            0,
            opt(strct(
                0,
                &[
                    ("a", 0, opt(SchemaNode::BackRef(1))),
                    ("b", 0, opt(SchemaNode::BackRef(0))),
                ],
            )),
        )],
    );
    let a = |b: Value| st(vec![b]);
    let b = |a: Value, b: Value| some(st(vec![a, b]));
    conforms(&a(b(none(), b(some(a(none())), none()))), &n).unwrap();
    // A B where `a` names A is refused.
    conforms(&a(b(none(), b(b(none(), none()), none()))), &n).unwrap_err();
}

#[test]
fn backref_reentry_resolves_under_the_targets_own_ancestors_three_deep() {
    // A { b: Option<B> }, B { c: Option<C> },
    // C { a: Option<A>, b: Option<B>, c: Option<C> }.
    let n = strct(
        0,
        &[(
            "b",
            0,
            opt(strct(
                0,
                &[(
                    "c",
                    0,
                    opt(strct(
                        0,
                        &[
                            ("a", 0, opt(SchemaNode::BackRef(2))),
                            ("b", 0, opt(SchemaNode::BackRef(1))),
                            ("c", 0, opt(SchemaNode::BackRef(0))),
                        ],
                    )),
                )],
            )),
        )],
    );
    let a = |b: Value| st(vec![b]);
    let b = |c: Value| st(vec![c]);
    let c = |a: Value, b: Value, c: Value| st(vec![a, b, c]);
    let inner = some(b(some(c(some(a(some(b(none())))), none(), some(c(none(), none(), none()))))));
    conforms(
        &a(some(b(some(c(none(), none(), some(c(none(), inner, none()))))))),
        &n,
    )
    .unwrap();
    // Inside the re-entered C, `b` names B: a C there is refused.
    let wrong = some(c(none(), none(), none()));
    conforms(
        &a(some(b(some(c(none(), none(), some(c(none(), wrong, none()))))))),
        &n,
    )
    .unwrap_err();
}
