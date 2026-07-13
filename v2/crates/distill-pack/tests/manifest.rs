use std::collections::BTreeMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use distill_core::attestation::{
    CompiledTypeRow, CompiledTypeTable, RegistryExtraFact, RegistryExtraRow, RegistryExtrasV1,
    SchemaNodeId,
};
use distill_core::id::{AssetUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};
use distill_pack::activation::{manifest_filename, publish_manifest, PointerError};
use distill_pack::archive::{EKey, ObjectLocation};
use distill_pack::manifest::*;

fn sample() -> PackManifest {
    let asset = AssetUuid([1; 16]);
    let content = ContentHash([2; 32]);
    let layout = LayoutHash([3; 32]);
    let key = EKey([4; 32]);
    let type_uuid = TypeUuid([6; 16]);
    let logical_hash = LogicalHash([12; 32]);
    let compiled = CompiledTypeRow::new(
        type_uuid,
        logical_hash,
        [7; 32],
        false,
        RegistryExtrasV1::canonical(vec![RegistryExtraRow {
            node: SchemaNodeId(0),
            path: vec![],
            fact: RegistryExtraFact::BuildOnly(false),
        }])
        .unwrap(),
    )
    .unwrap();
    PackManifest {
        target: PackTarget {
            os: 1,
            arch: 2,
            apis: vec![3, 1],
            options: BTreeMap::from([("quality".into(), "high".into())]),
        },
        target_def_hash: [5; 32],
        compiled_types: CompiledTypeTable::canonical(vec![compiled]).unwrap(),
        load_policy: vec![LoadPolicyRow {
            type_uuid,
            build_only: false,
        }],
        archives: vec![ArchiveRef {
            generation: 9,
            file_hash: [8; 32],
        }],
        assets: vec![ManifestAssetRow {
            asset_uuid: asset,
            authored_type: type_uuid,
            terminal_type: type_uuid,
            logical_hash,
            content_hash: content,
            load_deps: vec![],
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
fn manifest_header_places_direct_compiled_rows_before_load_policy() {
    let manifest = canonicalize(sample()).unwrap();
    let bytes = encode_manifest(&manifest).unwrap();
    let mut cursor = 8;
    let target_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4 + target_len + 32;

    assert_eq!(
        u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()),
        1
    );
    cursor += 4;
    let compiled_row = manifest.compiled_types.rows[0].encode().unwrap();
    assert_eq!(
        &bytes[cursor..cursor + compiled_row.len()],
        compiled_row.as_slice(),
        "compiled row must begin directly with TypeUuid, without an outer row length"
    );
    cursor += compiled_row.len();
    assert_eq!(
        &bytes[cursor..cursor + 32],
        &manifest.compiled_types.digest.0
    );
    cursor += 32;
    assert_eq!(
        u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()),
        1,
        "load-policy count must follow DSCA"
    );
    cursor += 4;
    assert_eq!(
        &bytes[cursor..cursor + 16],
        &manifest.load_policy[0].type_uuid.0
    );
    assert_eq!(bytes[cursor + 16], 0);
}

#[test]
fn hash_named_manifest_publication_is_idempotent_and_never_replaces_bytes() {
    let dir = std::env::temp_dir().join(format!(
        "distill-pack-manifest-publish-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&dir).unwrap();
    let bytes = encode_manifest(&sample()).unwrap();
    let hash = publish_manifest(&dir, &bytes).unwrap();
    let expected = manifest_filename(hash);
    assert_eq!(fs::read(dir.join(&expected)).unwrap(), bytes);

    publish_manifest(&dir, &bytes).unwrap();
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);

    fs::remove_file(dir.join(&expected)).unwrap();
    fs::write(dir.join(&expected), b"different immutable bytes").unwrap();
    assert!(matches!(
        publish_manifest(&dir, &bytes),
        Err(PointerError::ImmutableConflict(_))
    ));
    fs::remove_dir_all(dir).unwrap();
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
    wrong.load_deps.push(AssetUuid([13; 16]));
    assert!(verify_artifact_metadata(row, &wrong).is_err());
}

#[test]
fn attestation_requires_pack_projection_coverage_but_allows_runtime_superset() {
    let manifest = canonicalize(sample()).unwrap();
    let extra = CompiledTypeRow::new(
        TypeUuid([99; 16]),
        LogicalHash([1; 32]),
        [1; 32],
        true,
        RegistryExtrasV1::default(),
    )
    .unwrap();
    let runtime =
        CompiledTypeTable::canonical(vec![manifest.compiled_types.rows[0].clone(), extra]).unwrap();
    assert!(verify_attestation(&manifest, &runtime, [5; 32]).is_ok());
    assert!(verify_attestation(
        &manifest,
        &CompiledTypeTable::canonical(vec![]).unwrap(),
        [5; 32]
    )
    .is_err());
    assert!(verify_attestation(&manifest, &runtime, [0; 32]).is_err());
}

#[test]
fn manifest_rejects_forged_unsorted_incomplete_and_semantically_stale_rows() {
    let mut forged = sample();
    forged.compiled_types.rows[0].registry_extras_digest.0[0] ^= 1;
    assert!(matches!(
        encode_manifest(&forged),
        Err(ManifestError::CompiledAttestation(
            distill_core::attestation::AttestationError::ExtrasDigestMismatch
        ))
    ));

    let mut duplicate = sample();
    duplicate
        .compiled_types
        .rows
        .push(duplicate.compiled_types.rows[0].clone());
    assert!(matches!(
        encode_manifest(&duplicate),
        Err(ManifestError::CompiledAttestation(
            distill_core::attestation::AttestationError::DuplicateType(_)
        ))
    ));

    let mut incomplete = sample();
    incomplete.load_policy.clear();
    assert_eq!(
        encode_manifest(&incomplete),
        Err(ManifestError::CompiledCoverage)
    );

    let manifest = canonicalize(sample()).unwrap();
    let pack_row = &manifest.compiled_types.rows[0];
    let stale = CompiledTypeRow::new(
        pack_row.type_uuid,
        LogicalHash([99; 32]),
        pack_row.native_layout_digest,
        pack_row.build_only,
        pack_row.registry_extras.clone(),
    )
    .unwrap();
    let stale_runtime = CompiledTypeTable::canonical(vec![stale]).unwrap();
    assert_eq!(
        verify_attestation(&manifest, &stale_runtime, manifest.target_def_hash),
        Err(ManifestError::CompiledMismatch(pack_row.type_uuid))
    );
}

#[test]
fn manifest_rejects_compiled_rows_outside_the_asset_type_closure() {
    let mut manifest = sample();
    let type_uuid = TypeUuid([99; 16]);
    manifest.compiled_types = CompiledTypeTable::canonical(vec![
        manifest.compiled_types.rows[0].clone(),
        CompiledTypeRow::new(
            type_uuid,
            LogicalHash([99; 32]),
            [99; 32],
            false,
            RegistryExtrasV1::default(),
        )
        .unwrap(),
    ])
    .unwrap();
    manifest.load_policy.push(LoadPolicyRow {
        type_uuid,
        build_only: false,
    });

    assert_eq!(
        encode_manifest(&manifest),
        Err(ManifestError::CompiledCoverage)
    );
}

#[test]
fn manifest_rejects_load_dependencies_absent_from_the_asset_table() {
    let mut manifest = sample();
    let missing = AssetUuid([13; 16]);
    manifest.assets[0].load_deps.push(missing);

    assert_eq!(
        encode_manifest(&manifest),
        Err(ManifestError::MissingDependency(missing))
    );
}
