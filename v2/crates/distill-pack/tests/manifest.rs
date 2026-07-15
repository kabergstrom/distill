use std::collections::BTreeMap;

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_pack::archive::{EKey, ObjectLocation};
use distill_pack::manifest::*;

fn sample() -> PackManifest {
    PackManifest {
        target: PackTarget { name: "dev".into() },
        target_def_hash: [5; 32],
        archives: vec![ArchiveRef {
            generation: 7,
            file_hash: [8; 32],
        }],
        assets: vec![
            ManifestAssetRow {
                asset_uuid: AssetUuid([1; 16]),
                content_hash: ContentHash([11; 32]),
                load_deps: vec![ManifestLoadEdge {
                    asset_uuid: AssetUuid([2; 16]),
                    expected_terminal: TypeUuid([22; 16]),
                }],
            },
            ManifestAssetRow {
                asset_uuid: AssetUuid([2; 16]),
                content_hash: ContentHash([12; 32]),
                load_deps: Vec::new(),
            },
        ],
        encodings: vec![EncodingRow {
            content_hash: ContentHash([11; 32]),
            blocks: vec![EKey([31; 32])],
            blobs: vec![EKey([32; 32])],
        }],
        index: vec![IndexRow {
            ekey: EKey([31; 32]),
            location: ObjectLocation {
                generation: 7,
                offset: 64,
                len: 32,
            },
        }],
        wire_trees: vec![WireTreeRow {
            layout_hash: LayoutHash([41; 32]),
            bytes: vec![1, 2, 3],
        }],
        paths: Some(vec![PathRow {
            path: "assets/root.bundle".into(),
            asset_uuid: AssetUuid([1; 16]),
        }]),
    }
}

#[test]
fn manifest_v2_roundtrips_byte_identically_with_all_tables() {
    let manifest = canonicalize(sample()).unwrap();
    let bytes = encode_manifest(&manifest).unwrap();
    let decoded = decode_manifest(&bytes).unwrap();

    assert_eq!(decoded, manifest);
    assert_eq!(encode_manifest(&decoded).unwrap(), bytes);
}

#[test]
fn optional_path_table_is_directory_membership() {
    let with_paths = encode_manifest(&sample()).unwrap();
    let mut without = sample();
    without.paths = None;
    let without_paths = encode_manifest(&without).unwrap();

    assert!(decode_manifest(&with_paths).unwrap().paths.is_some());
    assert!(decode_manifest(&without_paths).unwrap().paths.is_none());
}

#[test]
fn paths_are_nfc_normalized_before_sorting() {
    let mut manifest = sample();
    manifest.paths = Some(vec![PathRow {
        path: "te\u{301}xtures/a.bundle".into(),
        asset_uuid: AssetUuid([1; 16]),
    }]);

    let decoded = decode_manifest(&encode_manifest(&manifest).unwrap()).unwrap();
    assert_eq!(decoded.paths.unwrap()[0].path, "t\u{e9}xtures/a.bundle");
}

#[test]
fn manifest_rejects_corruption_and_truncation() {
    let bytes = encode_manifest(&sample()).unwrap();
    assert!(matches!(
        decode_manifest(&bytes[..bytes.len() - 1]),
        Err(ManifestError::FileHash)
    ));

    let mut corrupt = bytes;
    corrupt[8] ^= 1;
    assert!(matches!(
        decode_manifest(&corrupt),
        Err(ManifestError::FileHash)
    ));
}

#[test]
fn artifact_header_crosscheck_authenticates_identity_and_dependency_assets() {
    let manifest = sample();
    let row = &manifest.assets[0];
    assert!(verify_artifact_header(row, row.asset_uuid, &[AssetUuid([2; 16])]).is_ok());

    assert_eq!(
        verify_artifact_header(row, row.asset_uuid, &[]),
        Err(ManifestError::MetadataMismatch)
    );
}

#[test]
fn manifest_rejects_missing_dependency_assets() {
    let mut manifest = sample();
    manifest.assets.pop();

    assert_eq!(
        canonicalize(manifest),
        Err(ManifestError::MissingDependency(AssetUuid([2; 16])))
    );
}

#[test]
fn mounted_closure_rejects_typed_edge_terminal_mismatch() {
    let manifest = canonicalize(sample()).unwrap();
    let terminal_types = BTreeMap::from([
        (AssetUuid([1; 16]), TypeUuid([21; 16])),
        (AssetUuid([2; 16]), TypeUuid([99; 16])),
    ]);

    assert_eq!(
        verify_expected_terminals(&manifest, &terminal_types),
        Err(ManifestError::DependencyTypeMismatch(TypeUuid([22; 16])))
    );
}

#[test]
fn duplicate_dependency_assets_are_rejected_even_with_different_types() {
    let mut manifest = sample();
    manifest.assets[0].load_deps.push(ManifestLoadEdge {
        asset_uuid: AssetUuid([2; 16]),
        expected_terminal: TypeUuid([23; 16]),
    });

    assert_eq!(canonicalize(manifest), Err(ManifestError::Duplicate));
}
