//! Schema-directed authored-value encoding into canonical §12 wire sections.

mod common;

use std::collections::BTreeMap;

use common::*;
use distill_bundle::PathComponent;
use distill_core::id::{AssetUuid, TypeUuid};
use distill_json::AuthoredValue;
use distill_wire::encode::{encode_authored_value, EncodeError};
use distill_wire::native::ScalarKind;
use distill_wire::wire::{SlotKind, WireEnumForm, WireNode};
use ngp_schema::node::{PrimitiveKind, SchemaNode};

fn object(entries: impl IntoIterator<Item = (&'static str, AuthoredValue)>) -> AuthoredValue {
    AuthoredValue::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

fn no_references(
    _: &AuthoredValue,
    _: TypeUuid,
    _: bool,
    _: &[PathComponent],
) -> Result<AssetUuid, String> {
    Err("unexpected reference".to_owned())
}

#[test]
fn struct_and_string_emit_zeroed_fixed_padding_and_variable_bytes() {
    let schema = SchemaNode::Struct {
        rev: 0,
        fields: vec![
            ("count".into(), 0, SchemaNode::Primitive(PrimitiveKind::U32)),
            ("name".into(), 0, SchemaNode::String),
        ],
    };
    let wire = wstruct(
        0,
        32,
        8,
        vec![
            wfield("count", 0, wprim(0, ScalarKind::U32)),
            wfield("name", 1, wstring_slot(8)),
        ],
    );
    let encoded = encode_authored_value(
        &schema,
        &wire,
        &object([
            ("count", AuthoredValue::UInt(7)),
            ("name", AuthoredValue::Str("hi".into())),
        ]),
        &mut no_references,
    )
    .unwrap();

    let mut fixed = vec![0; 32];
    fixed[..4].copy_from_slice(&7u32.to_le_bytes());
    fixed[8..12].copy_from_slice(&0u32.to_le_bytes());
    fixed[12..16].copy_from_slice(&2u32.to_le_bytes());
    assert_eq!(encoded.fixed, fixed);
    assert_eq!(encoded.variable, b"hi");
    assert!(encoded.blobs.is_empty());
}

#[test]
fn map_entries_sort_by_canonical_key_and_use_padded_pair_stride() {
    let schema = SchemaNode::Map {
        key: Box::new(SchemaNode::Primitive(PrimitiveKind::U32)),
        value: Box::new(SchemaNode::Primitive(PrimitiveKind::U16)),
    };
    let wire = wmap_slot(0, wprim(0, ScalarKind::U32), wprim(0, ScalarKind::U16));
    let value = AuthoredValue::Array(vec![
        AuthoredValue::Array(vec![AuthoredValue::UInt(9), AuthoredValue::UInt(90)]),
        AuthoredValue::Array(vec![AuthoredValue::UInt(2), AuthoredValue::UInt(20)]),
    ]);
    let encoded = encode_authored_value(&schema, &wire, &value, &mut no_references).unwrap();

    assert_eq!(&encoded.fixed[..8], &[0, 0, 0, 0, 2, 0, 0, 0]);
    let mut expected = vec![0; 16];
    expected[0..4].copy_from_slice(&2u32.to_le_bytes());
    expected[4..6].copy_from_slice(&20u16.to_le_bytes());
    expected[8..12].copy_from_slice(&9u32.to_le_bytes());
    expected[12..14].copy_from_slice(&90u16.to_le_bytes());
    assert_eq!(encoded.variable, expected);
}

#[test]
fn set_is_sorted_and_rejects_duplicate_canonical_elements() {
    let schema = SchemaNode::Set(Box::new(SchemaNode::Primitive(PrimitiveKind::U8)));
    let wire = wset_slot(0, wprim(0, ScalarKind::U8));
    let encoded = encode_authored_value(
        &schema,
        &wire,
        &AuthoredValue::Array(vec![AuthoredValue::UInt(9), AuthoredValue::UInt(2)]),
        &mut no_references,
    )
    .unwrap();
    assert_eq!(encoded.variable, [2, 9]);

    let duplicate = AuthoredValue::Array(vec![AuthoredValue::UInt(2), AuthoredValue::UInt(2)]);
    assert!(matches!(
        encode_authored_value(&schema, &wire, &duplicate, &mut no_references),
        Err(EncodeError::DuplicateOrderedValue { .. })
    ));
}

#[test]
fn canonical_enum_writes_name_sorted_tag_and_payload_union() {
    let payload = SchemaNode::Struct {
        rev: 0,
        fields: vec![("x".into(), 0, SchemaNode::Primitive(PrimitiveKind::U32))],
    };
    let schema = SchemaNode::Enum {
        rev: 0,
        variants: vec![
            ("A".into(), 0, payload.clone()),
            (
                "B".into(),
                0,
                SchemaNode::Struct {
                    rev: 0,
                    fields: vec![],
                },
            ),
        ],
    };
    let wire = wenum(
        0,
        12,
        4,
        WireEnumForm::Canonical,
        vec![
            wvariant("B", 0, wstruct(8, 0, 1, vec![])),
            wvariant(
                "A",
                0,
                wstruct(8, 4, 4, vec![wfield("x", 0, wprim(0, ScalarKind::U32))]),
            ),
        ],
    );
    let encoded = encode_authored_value(
        &schema,
        &wire,
        &object([("A", object([("x", AuthoredValue::UInt(77))]))]),
        &mut no_references,
    )
    .unwrap();
    assert_eq!(&encoded.fixed[..4], &0u32.to_le_bytes());
    assert_eq!(&encoded.fixed[8..12], &77u32.to_le_bytes());
}

#[test]
fn option_some_payload_and_transparent_box_use_exact_pointee_length() {
    let schema = SchemaNode::Option(Box::new(SchemaNode::Primitive(PrimitiveKind::U32)));
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
    let encoded =
        encode_authored_value(&schema, &wire, &AuthoredValue::UInt(42), &mut no_references)
            .unwrap();
    assert_eq!(&encoded.fixed[..4], &1u32.to_le_bytes());
    assert_eq!(&encoded.fixed[8..12], &0u32.to_le_bytes());
    assert_eq!(&encoded.fixed[12..16], &4u32.to_le_bytes());
    assert_eq!(encoded.variable, 42u32.to_le_bytes());
}

#[test]
fn recursive_vec_backref_reuses_the_enclosing_record_geometry() {
    let schema = SchemaNode::Struct {
        rev: 0,
        fields: vec![
            (
                "children".into(),
                0,
                SchemaNode::Vec(Box::new(SchemaNode::BackRef(0))),
            ),
            ("value".into(), 0, SchemaNode::Primitive(PrimitiveKind::U32)),
        ],
    };
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
    let child = object([
        ("children", AuthoredValue::Array(vec![])),
        ("value", AuthoredValue::UInt(2)),
    ]);
    let root = object([
        ("children", AuthoredValue::Array(vec![child])),
        ("value", AuthoredValue::UInt(1)),
    ]);
    let encoded = encode_authored_value(&schema, &wire, &root, &mut no_references).unwrap();
    assert_eq!(&encoded.fixed[..8], &[0, 0, 0, 0, 1, 0, 0, 0]);
    assert_eq!(&encoded.fixed[24..28], &1u32.to_le_bytes());
    assert_eq!(&encoded.variable[0..8], &[32, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(&encoded.variable[24..28], &2u32.to_le_bytes());
}

#[test]
fn recursive_enum_uses_distinct_logical_and_wire_backref_distances() {
    let schema = SchemaNode::Enum {
        rev: 0,
        variants: vec![
            (
                "End".into(),
                0,
                SchemaNode::Struct {
                    rev: 0,
                    fields: vec![],
                },
            ),
            (
                "Next".into(),
                0,
                SchemaNode::Struct {
                    rev: 0,
                    fields: vec![("0".into(), 0, SchemaNode::BackRef(0))],
                },
            ),
        ],
    };
    let wire = wenum(
        0,
        16,
        8,
        WireEnumForm::Canonical,
        vec![
            wvariant("End", 0, wstruct(8, 0, 1, vec![])),
            wvariant(
                "Next",
                0,
                wstruct(
                    8,
                    8,
                    8,
                    vec![wfield(
                        "0",
                        0,
                        wslot(
                            0,
                            8,
                            8,
                            SlotKind::Box,
                            vec![WireNode::BackRef {
                                distance: 1,
                                offset: 0,
                            }],
                        ),
                    )],
                ),
            ),
        ],
    );
    let value = object([("Next", object([("0", object([("End", object([]))]))]))]);
    let encoded = encode_authored_value(&schema, &wire, &value, &mut no_references).unwrap();
    assert_eq!(&encoded.fixed[..4], &1u32.to_le_bytes());
    assert_eq!(&encoded.fixed[8..12], &0u32.to_le_bytes());
    assert_eq!(&encoded.fixed[12..16], &16u32.to_le_bytes());
    assert_eq!(encoded.variable.len(), 16);
    assert_eq!(&encoded.variable[..4], &0u32.to_le_bytes());
}

#[test]
fn recursive_option_counts_its_two_wire_only_frames() {
    let schema = SchemaNode::Struct {
        rev: 0,
        fields: vec![(
            "next".into(),
            0,
            SchemaNode::Option(Box::new(SchemaNode::BackRef(0))),
        )],
    };
    let option_wire = wenum(
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
                        wslot(
                            0,
                            8,
                            8,
                            SlotKind::Box,
                            vec![WireNode::BackRef {
                                distance: 2,
                                offset: 0,
                            }],
                        ),
                    )],
                ),
            ),
        ],
    );
    let wire = wstruct(0, 16, 8, vec![wfield("next", 0, option_wire)]);
    let value = object([("next", object([("next", AuthoredValue::Null)]))]);
    let encoded = encode_authored_value(&schema, &wire, &value, &mut no_references).unwrap();
    assert_eq!(&encoded.fixed[..4], &1u32.to_le_bytes());
    assert_eq!(&encoded.fixed[8..12], &0u32.to_le_bytes());
    assert_eq!(&encoded.fixed[12..16], &16u32.to_le_bytes());
    assert_eq!(encoded.variable.len(), 16);
    assert_eq!(&encoded.variable[..4], &0u32.to_le_bytes());
}

fn option_of(inner: SchemaNode) -> SchemaNode {
    SchemaNode::Option(Box::new(inner))
}

fn record(fields: Vec<(&str, SchemaNode)>) -> SchemaNode {
    SchemaNode::Struct {
        rev: 0,
        fields: fields
            .into_iter()
            .map(|(name, node)| (name.to_owned(), 0, node))
            .collect(),
    }
}

/// `Option<Box<T>>` on the wire: a canonical enum whose `Some` record
/// boxes `pointee` — three wire frames above the pointee (§12).
fn option_box_wire(offset: u32, pointee: WireNode) -> WireNode {
    wenum(
        offset,
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
                        wslot(0, 8, 8, SlotKind::Box, vec![pointee]),
                    )],
                ),
            ),
        ],
    )
}

fn wire_backref(distance: u32) -> WireNode {
    WireNode::BackRef {
        distance,
        offset: 0,
    }
}

fn parse(text: &str) -> AuthoredValue {
    distill_json::parse(text).unwrap()
}

#[test]
fn backref_reentry_resolves_under_the_targets_own_ancestors() {
    // A { b: Option<Box<B>> }, B { a: Option<Box<A>>, b: Option<Box<B>> }.
    // B.b re-enters B; inside it, `a` names A on both trees — logical
    // BackRef(1), wire BackRef(5) — not the B the re-entry came from.
    let schema = record(vec![(
        "b",
        option_of(record(vec![
            ("a", option_of(SchemaNode::BackRef(1))),
            ("b", option_of(SchemaNode::BackRef(0))),
        ])),
    )]);
    let b_wire = wstruct(
        0,
        32,
        8,
        vec![
            wfield("a", 0, option_box_wire(0, wire_backref(5))),
            wfield("b", 1, option_box_wire(16, wire_backref(2))),
        ],
    );
    let wire = wstruct(0, 16, 8, vec![wfield("b", 0, option_box_wire(0, b_wire))]);
    let value = parse(r#"{"b":{"a":null,"b":{"a":{"b":null},"b":null}}}"#);
    let encoded = encode_authored_value(&schema, &wire, &value, &mut no_references).unwrap();
    // Variable section: outer B (32), inner B (32), inner A (16).
    assert_eq!(encoded.variable.len(), 80);
    assert_eq!(&encoded.variable[32..36], &1u32.to_le_bytes(), "inner B.a is Some");
    assert_eq!(&encoded.variable[40..44], &64u32.to_le_bytes(), "boxing the A at 64");
    assert_eq!(&encoded.variable[44..48], &16u32.to_le_bytes(), "of 16 bytes");

    // A B where `a` names A is refused.
    let wrong = parse(r#"{"b":{"a":null,"b":{"a":{"a":null,"b":null},"b":null}}}"#);
    encode_authored_value(&schema, &wire, &wrong, &mut no_references).unwrap_err();
}

#[test]
fn backref_reentry_resolves_under_the_targets_own_ancestors_three_deep() {
    // A { b: Option<Box<B>> }, B { c: Option<Box<C>> },
    // C { a: Option<Box<A>>, b: Option<Box<B>>, c: Option<Box<C>> }.
    let schema = record(vec![(
        "b",
        option_of(record(vec![(
            "c",
            option_of(record(vec![
                ("a", option_of(SchemaNode::BackRef(2))),
                ("b", option_of(SchemaNode::BackRef(1))),
                ("c", option_of(SchemaNode::BackRef(0))),
            ])),
        )])),
    )]);
    let c_wire = wstruct(
        0,
        48,
        8,
        vec![
            wfield("a", 0, option_box_wire(0, wire_backref(8))),
            wfield("b", 1, option_box_wire(16, wire_backref(5))),
            wfield("c", 2, option_box_wire(32, wire_backref(2))),
        ],
    );
    let b_wire = wstruct(0, 16, 8, vec![wfield("c", 0, option_box_wire(0, c_wire))]);
    let wire = wstruct(0, 16, 8, vec![wfield("b", 0, option_box_wire(0, b_wire))]);
    let value = parse(
        r#"{"b":{"c":{"a":null,"b":null,"c":{"a":null,"b":{"c":{"a":{"b":{"c":null}},"b":null,"c":{"a":null,"b":null,"c":null}}},"c":null}}}}"#,
    );
    encode_authored_value(&schema, &wire, &value, &mut no_references).unwrap();

    // Inside the re-entered C, `b` names B: a C there is refused.
    let wrong = parse(
        r#"{"b":{"c":{"a":null,"b":null,"c":{"a":null,"b":{"a":null,"b":null,"c":null},"c":null}}}}"#,
    );
    encode_authored_value(&schema, &wire, &wrong, &mut no_references).unwrap_err();
}

#[test]
fn blobs_sort_by_structural_path_and_patch_slots_after_sorting() {
    let schema = SchemaNode::Struct {
        rev: 0,
        fields: vec![
            ("a".into(), 0, SchemaNode::Blob),
            ("z".into(), 0, SchemaNode::Blob),
        ],
    };
    let wire = wstruct(
        0,
        16,
        8,
        vec![
            wfield("z", 0, wblob_slot(0, 8)),
            wfield("a", 1, wblob_slot(8, 8)),
        ],
    );
    let encoded = encode_authored_value(
        &schema,
        &wire,
        &object([
            ("a", AuthoredValue::Blob(vec![1])),
            ("z", AuthoredValue::Blob(vec![2])),
        ]),
        &mut no_references,
    )
    .unwrap();
    assert_eq!(encoded.blobs[0].path, [PathComponent::Field("a".into())]);
    assert_eq!(encoded.blobs[1].path, [PathComponent::Field("z".into())]);
    assert_eq!(&encoded.fixed[0..4], &1u32.to_le_bytes());
    assert_eq!(&encoded.fixed[8..12], &0u32.to_le_bytes());
}

#[test]
fn typed_references_are_resolved_and_recorded_in_traversal_order() {
    let strong_type = TypeUuid([3; 16]);
    let weak_type = TypeUuid([4; 16]);
    let schema = SchemaNode::Struct {
        rev: 0,
        fields: vec![
            ("strong".into(), 0, SchemaNode::AssetRef(strong_type)),
            ("weak".into(), 0, SchemaNode::WeakRef(weak_type)),
        ],
    };
    let reference_wire = || wstruct(0, 16, 8, vec![]);
    let wire = wstruct(
        0,
        32,
        8,
        vec![
            wfield("strong", 0, reference_wire()),
            wfield("weak", 1, {
                let mut node = reference_wire();
                if let WireNode::Struct { offset, .. } = &mut node {
                    *offset = 16;
                }
                node
            }),
        ],
    );
    let mut calls = Vec::new();
    let mut resolver =
        |query: &AuthoredValue, expected: TypeUuid, strong: bool, path: &[PathComponent]| {
            calls.push((query.clone(), expected, strong, path.to_vec()));
            Ok(if strong {
                AssetUuid([7; 16])
            } else {
                AssetUuid([8; 16])
            })
        };
    let encoded = encode_authored_value(
        &schema,
        &wire,
        &object([
            ("strong", AuthoredValue::Str("one".into())),
            ("weak", AuthoredValue::Str("two".into())),
        ]),
        &mut resolver,
    )
    .unwrap();
    assert_eq!(&encoded.fixed[..16], &[7; 16]);
    assert_eq!(&encoded.fixed[16..], &[8; 16]);
    assert_eq!(calls.len(), 2);
    assert_eq!(encoded.references.len(), 2);
    assert!(encoded.references[0].strong);
    assert!(!encoded.references[1].strong);
}

#[test]
fn f32_adoption_is_ties_even_and_floats_normalize_negative_zero() {
    let wire32 = wprim(0, ScalarKind::F32);
    let halfway = 1.0f64 + 2f64.powi(-24);
    let encoded = encode_authored_value(
        &SchemaNode::Primitive(PrimitiveKind::F32),
        &wire32,
        &AuthoredValue::Float(halfway),
        &mut no_references,
    )
    .unwrap();
    assert_eq!(encoded.fixed, 1.0f32.to_bits().to_le_bytes());

    let large = 18_446_741_324_930_481_762u128;
    let encoded = encode_authored_value(
        &SchemaNode::Primitive(PrimitiveKind::F32),
        &wire32,
        &AuthoredValue::UInt(large),
        &mut no_references,
    )
    .unwrap();
    assert_eq!(encoded.fixed, (large as f32).to_bits().to_le_bytes());

    let encoded = encode_authored_value(
        &SchemaNode::Primitive(PrimitiveKind::F64),
        &wprim(0, ScalarKind::F64),
        &AuthoredValue::Float(-0.0),
        &mut no_references,
    )
    .unwrap();
    assert_eq!(encoded.fixed, 0.0f64.to_bits().to_le_bytes());
}

#[test]
fn structs_reject_unknown_fields_instead_of_silently_dropping_them() {
    let schema = SchemaNode::Struct {
        rev: 0,
        fields: vec![("x".into(), 0, SchemaNode::Primitive(PrimitiveKind::U8))],
    };
    let wire = wstruct(0, 1, 1, vec![wfield("x", 0, wprim(0, ScalarKind::U8))]);
    let mut values = BTreeMap::new();
    values.insert("x".into(), AuthoredValue::UInt(1));
    values.insert("unknown".into(), AuthoredValue::UInt(2));
    assert!(matches!(
        encode_authored_value(
            &schema,
            &wire,
            &AuthoredValue::Object(values),
            &mut no_references
        ),
        Err(EncodeError::Shape { .. })
    ));
}
