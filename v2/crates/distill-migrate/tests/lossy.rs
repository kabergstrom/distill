//! Which data an automatic plan drops.

mod common;
use common::*;
use distill_json::AuthoredValue;
use distill_migrate::{conforms, lossy_drops, plan_automatic, zero_value};
use ngp_schema::SchemaNode;

fn drops(old: &SchemaNode, new: &SchemaNode, value: &AuthoredValue) -> Vec<String> {
    lossy_drops(&plan_automatic(old, new).unwrap(), old, value)
}

#[test]
fn added_fields_drop_nothing() {
    let old = strct(0, &[("a", 0, p(PK::U32))]);
    let new = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, SchemaNode::String)]);
    assert!(drops(&old, &new, &obj(&[("a", ui(7))])).is_empty());
}

#[test]
fn a_dropped_field_is_lossy_only_when_it_holds_a_non_default_value() {
    let old = strct(0, &[("a", 0, p(PK::U32)), ("gone", 0, vec_of(p(PK::U8)))]);
    let new = strct(0, &[("a", 0, p(PK::U32))]);
    assert!(drops(&old, &new, &obj(&[("a", ui(1)), ("gone", arr_v(&[]))])).is_empty());
    assert_eq!(
        drops(&old, &new, &obj(&[("a", ui(1)), ("gone", arr_v(&[ui(3)]))])),
        ["$.gone"]
    );
}

#[test]
fn drops_inside_nested_structs_elements_and_variants_name_their_path() {
    let inner_old = strct(0, &[("keep", 0, p(PK::U8)), ("gone", 0, p(PK::U8))]);
    let inner_new = strct(0, &[("keep", 0, p(PK::U8))]);
    let old = strct(
        0,
        &[
            ("inline", 0, inner_old.clone()),
            ("items", 0, vec_of(inner_old.clone())),
            ("choice", 0, enm(0, &[("Some", 0, inner_old)])),
        ],
    );
    let new = strct(
        0,
        &[
            ("inline", 0, inner_new.clone()),
            ("items", 0, vec_of(inner_new.clone())),
            ("choice", 0, enm(0, &[("Some", 0, inner_new)])),
        ],
    );
    let held = obj(&[("keep", ui(1)), ("gone", ui(2))]);
    let value = obj(&[
        ("inline", held.clone()),
        ("items", arr_v(&[obj(&[("keep", ui(1)), ("gone", ui(0))]), held.clone()])),
        ("choice", obj(&[("Some", held)])),
    ]);
    assert_eq!(
        drops(&old, &new, &value),
        ["$.inline.gone", "$.items[].gone", "$.choice{Some}.gone"]
    );
}

#[test]
fn zero_values_conform_to_their_schema() {
    let node = strct(
        0,
        &[
            ("a", 0, p(PK::F32)),
            ("b", 0, p(PK::Char)),
            ("c", 0, arr(3, p(PK::I16))),
            ("d", 0, map_of(SchemaNode::String, SchemaNode::Blob)),
            ("e", 0, enm(0, &[("Empty", 0, unit_payload())])),
            ("f", 0, opt(SchemaNode::AssetRef(tuuid(1)))),
            ("g", 0, SchemaNode::Blob),
        ],
    );
    conforms(&zero_value(&node).unwrap(), &node).unwrap();
    assert!(zero_value(&SchemaNode::AssetRef(tuuid(1))).is_err());
}
