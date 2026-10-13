//! Dynamic values (newgameplus GAP 23 §3) in bundles: the `types` closure
//! names each type a dynamic value spells, its schema in `schemas`.

mod common;

use common::*;
use distill_bundle::{parse_bundle, referenced_schema_hashes, write_bundle, BundleError, BundleType};
use distill_core::id::TypeUuid;
use ngp_schema::{DynamicPayload, Patch, PrimitiveKind, SchemaNode as N, Value};

const MARKER_UUID: &str = "6e67702d-7479-7065-0102-030405060708";

fn marker_schema() -> ngp_schema::LogicalSchema {
    schema(st(&[("hp", N::Primitive(PrimitiveKind::U32)), ("tag", N::String)]))
}

fn holder_schema() -> ngp_schema::LogicalSchema {
    schema(st(&[("component", N::Dynamic { sparse: true })]))
}

/// A bundle whose one entry holds `{"component": {"type": "m::Marker",
/// "value": {"hp": 3}}}`.
fn dynamic_bundle() -> distill_bundle::Bundle {
    let (marker, holder) = (marker_schema(), holder_schema());
    let ty: TypeUuid = MARKER_UUID.parse().unwrap();
    let data = Value::Struct(Box::new([Value::Dynamic(Box::new(ngp_schema::DynamicValue {
        ty,
        value: DynamicPayload::Sparse(Patch::Struct(Box::new([Some(Patch::Replace(Value::U32(3))), None]))),
    }))]));
    let mut b = bundle(&[&holder, &marker], vec![("a", typed_entry(UUID_A, &holder, data))], Some("a"));
    b.types.insert("m::Marker".to_owned(), BundleType { type_uuid: ty, schema_hash: lh(&marker) });
    b
}

#[test]
fn a_dynamic_value_round_trips_through_its_type_closure() {
    let b = dynamic_bundle();
    let bytes = write_bundle(&b).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains(r#""data":{"component":{"type":"m::Marker","value":{"hp":3}}}"#), "{text}");
    assert!(text.contains(&format!(
        r#""types":{{"m::Marker":{{"schema_hash":"{}","type_uuid":"{MARKER_UUID}"}}}}"#,
        lh(&marker_schema())
    )));
    let parsed = parse_bundle(&bytes).unwrap();
    assert_eq!(parsed, b);
    assert_eq!(write_bundle(&parsed).unwrap(), bytes);
    assert!(referenced_schema_hashes(&parsed).contains(&lh(&marker_schema())));
}

#[test]
fn a_bundle_without_dynamic_values_writes_no_types_key() {
    let sc = simple_schema();
    let b = bundle(&[&sc], vec![("a", entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("x"))])))], None);
    let bytes = write_bundle(&b).unwrap();
    assert!(!std::str::from_utf8(&bytes).unwrap().contains("\"types\""));
}

#[test]
fn a_type_outside_the_closure_fails_parse_and_write() {
    let b = dynamic_bundle();
    let bytes = write_bundle(&b).unwrap();
    let renamed = mutate_envelope(&bytes, |env| {
        let types = as_obj(as_obj(env).get_mut("types").unwrap());
        let entry = types.remove("m::Marker").unwrap();
        types.insert("m::Other".to_owned(), entry);
    });
    assert!(matches!(parse_bundle(&renamed), Err(BundleError::Value { .. })), "{:?}", parse_bundle(&renamed));

    let mut missing = b.clone();
    missing.types.clear();
    assert!(write_bundle(&missing).is_err());
}

#[test]
fn a_type_schema_outside_schemas_is_a_closure_error() {
    let b = dynamic_bundle();
    let bytes = write_bundle(&b).unwrap();
    let marker = lh(&marker_schema()).to_string();
    let dropped = mutate_envelope(&bytes, |env| {
        as_obj(as_obj(env).get_mut("schemas").unwrap()).remove(&marker);
    });
    assert!(matches!(parse_bundle(&dropped), Err(BundleError::MissingTypeSchema { .. })));
    let unknown_key = mutate_envelope(&bytes, |env| {
        let types = as_obj(as_obj(env).get_mut("types").unwrap());
        as_obj(types.get_mut("m::Marker").unwrap()).insert("extra".to_owned(), V::Null);
    });
    assert!(matches!(parse_bundle(&unknown_key), Err(BundleError::BadType { .. })));
}
