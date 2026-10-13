//! An authored PackDefinition bundle, shared by the pack tests.

use std::collections::BTreeMap;

use distill_bundle::{AssetEntry, Bundle, BUNDLE_FORMAT_VERSION};
use distill_core::bootstrap::{
    BootstrapControlSpecV1, BootstrapControlSymbol, PACK_DEFINITION_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid};
use distill_schema::ngp_schema::Value;

/// The bytes of a bundle holding one PackDefinition, `definition`, that
/// packs `roots` (by UUID) for `target`.
pub fn pack_definition_bundle(
    bundle: BundleUuid,
    definition: AssetUuid,
    target: &str,
    roots: &[AssetUuid],
    include_path_table: bool,
) -> Vec<u8> {
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::PackDefinition)
        .unwrap();
    let schema = distill_schema::ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
    // AssetQuery fields in node (name) order: nine `None`s, then `uuid`.
    let query = |uuid: &AssetUuid| {
        let mut fields = vec![Value::Option(None); 9];
        fields.push(Value::Option(Some(Box::new(Value::Seq(
            uuid.0.iter().map(|byte| Value::U8(*byte)).collect(),
        )))));
        Value::Struct(fields.into_boxed_slice())
    };
    // include_path_table, roots, target, zstd_level.
    let data = Value::Struct(Box::new([
        Value::Bool(include_path_table),
        Value::Seq(roots.iter().map(query).collect()),
        Value::String(target.to_owned()),
        Value::I32(1),
    ]));
    distill_bundle::write_bundle(&Bundle {
        format_version: BUNDLE_FORMAT_VERSION,
        uuid: bundle,
        primary: None,
        schemas: BTreeMap::from([(row.logical_hash, schema)]),
        assets: BTreeMap::from([(
            "pack".to_owned(),
            AssetEntry {
                uuid: definition,
                type_uuid: PACK_DEFINITION_TYPE_UUID,
                schema_hash: row.logical_hash,
                authoring_only: true,
                data,
            },
        )]),
    })
    .unwrap()
}
