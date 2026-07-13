use distill_build::outputs::*;
use distill_core::id::{AssetUuid, TypeUuid};

fn value(ty: u8) -> EncodedValue {
    EncodedValue {
        type_uuid: TypeUuid([ty; 16]),
        bytes: vec![ty],
        references: vec![],
    }
}

#[test]
fn declared_outputs_bind_exactly_once_and_extras_get_uuid_v5() {
    let parent = AssetUuid([7; 16]);
    let decls = OutputDecls::new(
        TypeUuid([1; 16]),
        vec![("reflection".into(), TypeUuid([2; 16]))],
    )
    .unwrap();
    let mut out = OutputCollector::new(parent, decls);
    out.primary(value(1)).unwrap();
    let child = out.extra("reflection", value(2)).unwrap();
    assert_eq!(child, AssetUuid::v5(parent, "reflection"));
    let bound = out.finish().unwrap();
    assert_eq!(bound.outputs.len(), 2);
    let extra = &bound.outputs[1];
    assert_eq!(extra.encoded_type, TypeUuid([2; 16]));
    assert_eq!(extra.terminal_type, TypeUuid([2; 16]));
}

#[test]
fn missing_duplicate_unknown_and_wrong_typed_bindings_fail() {
    let decls = OutputDecls::new(TypeUuid([1; 16]), vec![("x".into(), TypeUuid([2; 16]))]).unwrap();
    let empty = OutputCollector::new(AssetUuid([1; 16]), decls.clone());
    assert!(empty.finish().is_err());
    let mut out = OutputCollector::new(AssetUuid([1; 16]), decls.clone());
    assert!(out.primary(value(2)).is_err());
    out.primary(value(1)).unwrap();
    assert!(out.primary(value(1)).is_err());
    assert!(out.extra("nope", value(2)).is_err());
    assert!(out.extra("x", value(1)).is_err());
}

#[test]
fn output_keys_are_normalized_before_uniqueness_and_debug_is_bounded() {
    assert!(OutputDecls::new(
        TypeUuid([1; 16]),
        vec![
            ("e\u{301}".into(), TypeUuid([2; 16])),
            ("\u{e9}".into(), TypeUuid([2; 16]))
        ],
    )
    .is_err());
    let decls = OutputDecls::new(TypeUuid([1; 16]), vec![]).unwrap();
    let mut out = OutputCollector::new(AssetUuid([1; 16]), decls);
    out.primary(value(1)).unwrap();
    out.debug("a", vec![1]).unwrap();
    assert!(out.debug("a", vec![2]).is_err());
    assert!(out.debug("", vec![]).is_err());
}
