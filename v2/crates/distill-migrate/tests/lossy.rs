//! Which data an automatic plan drops.

mod common;
use common::*;
use distill_migrate::{conforms, lossy_drops, plan_automatic, zero_value};
use ngp_schema::SchemaNode;

fn drops(old: &SchemaNode, new: &SchemaNode, value: &str) -> Vec<String> {
    lossy_drops(&plan_automatic(old, new).unwrap(), old, &val(old, value))
}

#[test]
fn added_fields_drop_nothing() {
    let old = strct(0, &[("a", 0, p(PK::U32))]);
    let new = strct(0, &[("a", 0, p(PK::U32)), ("b", 0, SchemaNode::String)]);
    assert!(drops(&old, &new, r#"{"a":7}"#).is_empty());
}

#[test]
fn a_dropped_field_is_lossy_only_when_it_holds_a_non_default_value() {
    let old = strct(0, &[("a", 0, p(PK::U32)), ("gone", 0, vec_of(p(PK::U8)))]);
    let new = strct(0, &[("a", 0, p(PK::U32))]);
    assert!(drops(&old, &new, r#"{"a":1,"gone":[]}"#).is_empty());
    assert_eq!(drops(&old, &new, r#"{"a":1,"gone":[3]}"#), ["$.gone"]);
}

#[test]
fn drops_inside_nested_structs_elements_and_variants_name_their_path() {
    let inner_old = strct(0, &[("gone", 0, p(PK::U8)), ("keep", 0, p(PK::U8))]);
    let inner_new = strct(0, &[("keep", 0, p(PK::U8))]);
    let old = strct(
        0,
        &[
            ("choice", 0, enm(0, &[("Some", 0, inner_old.clone())])),
            ("inline", 0, inner_old.clone()),
            ("items", 0, vec_of(inner_old)),
        ],
    );
    let new = strct(
        0,
        &[
            ("choice", 0, enm(0, &[("Some", 0, inner_new.clone())])),
            ("inline", 0, inner_new.clone()),
            ("items", 0, vec_of(inner_new)),
        ],
    );
    let held = r#"{"gone":2,"keep":1}"#;
    let value = format!(
        r#"{{"choice":{{"Some":{held}}},"inline":{held},"items":[{{"gone":0,"keep":1}},{held}]}}"#
    );
    assert_eq!(
        drops(&old, &new, &value),
        ["$.choice{Some}.gone", "$.inline.gone", "$.items[].gone"]
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
    assert!(zero_value(&SchemaNode::AssetRef(tuuid(1))).is_none());
}
