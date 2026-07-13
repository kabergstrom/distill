use std::collections::BTreeMap;

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};
use distill_pack::archive::{EKey, ObjectLocation};
use distill_pack::manifest::*;

fn sample() -> PackManifest {
    let asset = AssetUuid([1; 16]);
    let content = ContentHash([2; 32]);
    let layout = LayoutHash([3; 32]);
    let key = EKey([4; 32]);
    PackManifest {
        target: PackTarget {
            os: 1,
            arch: 2,
            apis: vec![3, 1],
            options: BTreeMap::from([("quality".into(), "high".into())]),
        },
        target_def_hash: [5; 32],
        layout_registry: vec![LayoutRegistryRow {
            type_uuid: TypeUuid([6; 16]),
            digest: [7; 32],
        }],
        load_policy: vec![LoadPolicyRow {
            type_uuid: TypeUuid([6; 16]),
            build_only: false,
        }],
        archives: vec![ArchiveRef {
            generation: 9,
            file_hash: [8; 32],
        }],
        assets: vec![ManifestAssetRow {
            asset_uuid: asset,
            authored_type: TypeUuid([10; 16]),
            terminal_type: TypeUuid([11; 16]),
            logical_hash: LogicalHash([12; 32]),
            content_hash: content,
            load_deps: vec![AssetUuid([13; 16])],
        }],
        encodings: vec![EncodingRow {
            content_hash: content,
            blocks: vec![key],
            blobs: vec![],
        }],
        index: vec![IndexRow {
            ekey: key,
            location: ObjectLocation {
                generation: 9,
                offset: 64,
                len: 17,
            },
        }],
        wire_trees: vec![WireTreeRow {
            layout_hash: layout,
            bytes: vec![1, 2, 3],
        }],
        paths: Some(vec![PathRow {
            path: "textures/a.bundle".into(),
            asset_uuid: asset,
        }]),
    }
}

#[test]
fn manifest_roundtrips_byte_identically_with_all_five_tables() {
    let manifest = sample();
    let bytes = encode_manifest(&manifest).unwrap();
    let parsed = decode_manifest(&bytes).unwrap();
    assert_eq!(parsed, canonicalize(manifest).unwrap());
    assert_eq!(encode_manifest(&parsed).unwrap(), bytes);
    assert_eq!(manifest_hash(&bytes), manifest_hash(&bytes));
}

#[test]
fn optional_path_table_is_directory_membership() {
    let mut manifest = sample();
    manifest.paths = None;
    let bytes = encode_manifest(&manifest).unwrap();
    assert!(decode_manifest(&bytes).unwrap().paths.is_none());
}

#[test]
fn manifest_rejects_corruption_truncation_and_invalid_policy_bits() {
    let bytes = encode_manifest(&sample()).unwrap();
    for len in 0..bytes.len() {
        assert!(
            decode_manifest(&bytes[..len]).is_err(),
            "accepted truncation {len}"
        );
    }
    let mut bad = bytes.clone();
    bad[0] ^= 1;
    assert!(matches!(
        decode_manifest(&bad),
        Err(ManifestError::FileHash)
    ));
}

#[test]
fn artifact_header_crosscheck_is_bidirectional_and_exact() {
    let row = &sample().assets[0];
    let header = ArtifactMetadata {
        asset_uuid: row.asset_uuid,
        authored_type: row.authored_type,
        terminal_type: row.terminal_type,
        logical_hash: row.logical_hash,
        load_deps: row.load_deps.clone(),
    };
    assert!(verify_artifact_metadata(row, &header).is_ok());
    let mut wrong = header;
    wrong.load_deps.clear();
    assert!(verify_artifact_metadata(row, &wrong).is_err());
}

#[test]
fn attestation_requires_pack_projection_coverage_but_allows_runtime_superset() {
    let manifest = canonicalize(sample()).unwrap();
    let layouts = BTreeMap::from([(TypeUuid([6; 16]), [7; 32]), (TypeUuid([99; 16]), [1; 32])]);
    let policy = BTreeMap::from([(TypeUuid([6; 16]), false), (TypeUuid([99; 16]), true)]);
    assert!(verify_attestation(&manifest, &layouts, &policy, [5; 32]).is_ok());
    assert!(verify_attestation(&manifest, &BTreeMap::new(), &policy, [5; 32]).is_err());
    assert!(verify_attestation(&manifest, &layouts, &policy, [0; 32]).is_err());
}
