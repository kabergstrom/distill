//! An authored PackDefinition bundle, shared by the pack tests.

use std::collections::BTreeMap;

use distill_bundle::{AssetEntry, Bundle, BUNDLE_FORMAT_VERSION};
use distill_core::bootstrap::{
    BootstrapControlSpecV1, BootstrapControlSymbol, PACK_DEFINITION_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid};
use distill_json::AuthoredValue;

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
    let query = |uuid: &AssetUuid| {
        let mut fields = BTreeMap::from(
            [
                "authored_type",
                "authoring_only",
                "bundle_path",
                "bundle_uuid",
                "local_id",
                "path_glob",
                "path_prefix",
                "tag",
                "terminal_type",
            ]
            .map(|name| (name.to_owned(), AuthoredValue::Null)),
        );
        fields.insert(
            "uuid".to_owned(),
            AuthoredValue::Array(
                uuid.0
                    .iter()
                    .map(|byte| AuthoredValue::UInt(u128::from(*byte)))
                    .collect(),
            ),
        );
        AuthoredValue::Object(fields)
    };
    let data = AuthoredValue::Object(BTreeMap::from([
        (
            "include_path_table".to_owned(),
            AuthoredValue::Bool(include_path_table),
        ),
        (
            "roots".to_owned(),
            AuthoredValue::Array(roots.iter().map(query).collect()),
        ),
        ("target".to_owned(), AuthoredValue::Str(target.to_owned())),
        ("zstd_level".to_owned(), AuthoredValue::Int(1)),
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
