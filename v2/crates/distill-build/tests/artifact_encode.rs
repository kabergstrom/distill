use std::collections::BTreeMap;

use distill_build::artifact_encode::{encode_artifact_value, ArtifactValueSpec};
use distill_core::id::{AssetUuid, LayoutHash, LogicalHash, TypeUuid};
use distill_json::AuthoredValue;
use distill_schema::ngp_schema::{PrimitiveKind, SchemaNode};
use distill_wire::artifact::{parse_artifact, parse_artifact_parts};
use distill_wire::native::ScalarKind;
use distill_wire::wire::{SlotKind, WireField, WireNode};

fn field(name: &str, declaration_index: u32, node: WireNode) -> WireField {
    WireField {
        name: name.to_owned(),
        declaration_index,
        node,
    }
}

#[test]
fn composite_encoder_binds_refs_blobs_headers_and_split_transport() {
    let expected_type = TypeUuid([9; 16]);
    let schema = SchemaNode::Struct {
        rev: 0,
        fields: vec![
            ("blob".into(), 0, SchemaNode::Blob),
            ("reference".into(), 0, SchemaNode::AssetRef(expected_type)),
            ("value".into(), 0, SchemaNode::Primitive(PrimitiveKind::U32)),
        ],
    };
    let wire = WireNode::Struct {
        offset: 0,
        size: 32,
        align: 8,
        fields: vec![
            field(
                "blob",
                0,
                WireNode::Slot {
                    offset: 0,
                    size: 8,
                    align: 8,
                    kind: SlotKind::Blob,
                    pointee: vec![],
                },
            ),
            field(
                "reference",
                1,
                WireNode::Struct {
                    offset: 8,
                    size: 16,
                    align: 8,
                    fields: vec![],
                },
            ),
            field(
                "value",
                2,
                WireNode::Primitive {
                    offset: 24,
                    size: 4,
                    align: 4,
                    kind: ScalarKind::U32,
                },
            ),
        ],
    };
    let value = AuthoredValue::Object(BTreeMap::from([
        ("blob".into(), AuthoredValue::Blob(vec![1, 2, 3])),
        ("reference".into(), AuthoredValue::Str("query".into())),
        ("value".into(), AuthoredValue::UInt(44)),
    ]));
    let dependency = AssetUuid([7; 16]);
    let mut resolver = |_: &AuthoredValue,
                        expected: TypeUuid,
                        strong: bool,
                        _: &[distill_bundle::PathComponent]| {
        assert_eq!(expected, expected_type);
        assert!(strong);
        Ok(dependency)
    };
    let encoded = encode_artifact_value(
        ArtifactValueSpec {
            asset_uuid: AssetUuid([1; 16]),
            authored_type: TypeUuid([2; 16]),
            terminal_type: TypeUuid([3; 16]),
            encoded_type: TypeUuid([3; 16]),
            logical_hash: LogicalHash([4; 32]),
            layout_hash: LayoutHash([5; 32]),
            schema: &schema,
            wire: &wire,
            value: &value,
        },
        &mut resolver,
    )
    .unwrap();

    let complete = parse_artifact(&encoded.bytes).unwrap();
    assert_eq!(complete.load_deps, [dependency]);
    assert_eq!(complete.blob(0).unwrap(), [1, 2, 3]);
    assert_eq!(encoded.load_deps, [dependency]);
    assert_eq!(encoded.references.len(), 1);
    let blob_refs = encoded.blobs.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let split = parse_artifact_parts(&encoded.structural, &blob_refs).unwrap();
    assert_eq!(split.content_hash, encoded.content_hash);
    assert_eq!(split.asset_uuid, AssetUuid([1; 16]));
}
