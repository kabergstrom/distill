use std::collections::BTreeMap;

use distill_bundle::PathComponent;
use distill_core::attestation::{
    CompiledTypeRow, CompiledTypeTable, RegistryExtraFact, RegistryExtraRow, RegistryExtrasV1,
    SchemaNodeId,
};
use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_loader::{
    GameModuleEpoch, IoEvent, LoaderIO, PathResolveResult, ReqId, ResolveResult,
    RuntimeAttestation as LoaderRuntimeAttestation,
};
use distill_pack::archive::{encode_archive, ArtifactPayload};
use distill_pack::manifest::{
    encode_manifest, ArchiveRef, EncodingRow, IndexRow, LoadPolicyRow, ManifestAssetRow,
    PackManifest, PackTarget, PathRow, WireTreeRow,
};
use distill_pack::{MountError, PackfileIO, RuntimeAttestation};
use distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1;
use distill_wire::artifact::{
    content_hash, parse_artifact, write_artifact, ArtifactHeader, ARTIFACT_MAGIC,
};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::native::ScalarKind;
use distill_wire::wire::WireNode;

fn fixture(
    paths: bool,
) -> (
    Vec<u8>,
    Vec<u8>,
    RuntimeAttestation,
    AssetUuid,
    distill_core::id::ContentHash,
) {
    let asset_uuid = AssetUuid([1; 16]);
    let type_uuid = TypeUuid([3; 16]);
    let logical_hash = LogicalHash([6; 32]);
    let wire = WireNode::Primitive {
        offset: 0,
        size: 4,
        align: 4,
        kind: ScalarKind::U32,
    };
    let wire_bytes = dswl_bytes(&wire).unwrap();
    let layout_hash = dswl_hash(&wire).unwrap();
    let blob = b"blob".as_slice();
    let artifact = write_artifact(
        &ArtifactHeader {
            asset_uuid,
            authored_type: type_uuid,
            terminal_type: type_uuid,
            encoded_type: type_uuid,
            logical_hash,
            layout_hash,
        },
        &[],
        &[1, 2, 3, 4],
        &[],
        &[(vec![PathComponent::Field("blob".into())], blob)],
    )
    .unwrap();
    let parsed = parse_artifact(&artifact).unwrap();
    let structural_len = artifact.len() - parsed.blob_section.len();
    let structural = artifact[..structural_len].to_vec();
    let content_hash = content_hash(&artifact);
    let archive = encode_archive(
        7,
        "zstd-test",
        3,
        &[ArtifactPayload {
            content_hash,
            structural,
            blobs: vec![blob.to_vec()],
        }],
    )
    .unwrap();
    let encoding = &archive.encodings[&content_hash];
    let target = PackTarget {
        os: 1,
        arch: 2,
        apis: vec![3],
        options: BTreeMap::new(),
    };
    let runtime_row = CompiledTypeRow::new(
        type_uuid,
        logical_hash,
        [5; 32],
        false,
        RegistryExtrasV1::canonical(vec![RegistryExtraRow {
            node: SchemaNodeId(0),
            path: vec![],
            fact: RegistryExtraFact::BuildOnly(false),
        }])
        .unwrap(),
    )
    .unwrap();
    let bootstrap_authority = consumer_bootstrap_authority_v1().unwrap();
    let bootstrap_rows = bootstrap_authority.rows().to_vec();
    let mut compiled_rows = bootstrap_rows;
    compiled_rows.push(runtime_row);
    let compiled_types = CompiledTypeTable::canonical(compiled_rows).unwrap();
    let load_policy = compiled_types
        .rows
        .iter()
        .map(|row| LoadPolicyRow {
            type_uuid: row.type_uuid,
            build_only: row.build_only,
        })
        .collect();
    let manifest = PackManifest {
        target: target.clone(),
        target_def_hash: [4; 32],
        compiled_types: compiled_types.clone(),
        load_policy,
        archives: vec![ArchiveRef {
            generation: 7,
            file_hash: *blake3::hash(&archive.bytes).as_bytes(),
        }],
        assets: vec![ManifestAssetRow {
            asset_uuid,
            authored_type: type_uuid,
            terminal_type: type_uuid,
            logical_hash,
            content_hash,
            load_deps: vec![],
        }],
        encodings: vec![EncodingRow {
            content_hash,
            blocks: encoding.blocks.clone(),
            blobs: encoding.blobs.clone(),
        }],
        index: archive
            .index
            .iter()
            .map(|(ekey, location)| IndexRow {
                ekey: *ekey,
                location: *location,
            })
            .collect(),
        wire_trees: vec![WireTreeRow {
            layout_hash,
            bytes: wire_bytes,
        }],
        paths: paths.then(|| {
            vec![PathRow {
                path: "assets/a.bundle".into(),
                asset_uuid,
            }]
        }),
    };
    let runtime = RuntimeAttestation {
        target,
        target_def_hash: [4; 32],
        compiled_types,
        bootstrap_authority,
    };
    (
        encode_manifest(&manifest).unwrap(),
        archive.bytes,
        runtime,
        asset_uuid,
        content_hash,
    )
}

#[test]
fn packfile_io_resolves_fetches_and_resolves_paths_under_one_basis() {
    let (manifest, archive, runtime, asset_uuid, content_hash) = fixture(true);
    let mut io = PackfileIO::mount(&manifest, vec![archive], &runtime).unwrap();
    let basis = io.begin_sweep();
    io.resolve(ReqId(1), asset_uuid, &basis);
    io.fetch(ReqId(2), content_hash, &basis);
    io.resolve_path(ReqId(3), "assets/a.bundle", &basis);
    let events = io.poll();
    assert!(matches!(&events[0], IoEvent::Resolved {
        req: ReqId(1), result: ResolveResult::Built { content_hash: got }, basis: outer, ..
    } if *got == content_hash && outer == &basis));
    assert!(
        matches!(&events[1], IoEvent::Fetched { req: ReqId(2), artifact, basis: got, .. }
        if artifact.structural.starts_with(&ARTIFACT_MAGIC)
        && artifact.blobs[0].as_bytes() == b"blob"
        && !artifact.wire_layout.is_empty()
        && got == &basis)
    );
    assert!(matches!(&events[2], IoEvent::PathResolved {
        req: ReqId(3), result: PathResolveResult::Resolved(got), basis: event_basis, ..
    } if *got == asset_uuid && event_basis == &basis));
    assert!(io.poll().is_empty());
}

#[test]
fn packfile_io_reattests_the_registered_runtime_before_progress() {
    let (manifest, archive, runtime, _, _) = fixture(true);
    let mut io = PackfileIO::mount(&manifest, vec![archive], &runtime).unwrap();
    let attestation = LoaderRuntimeAttestation {
        epoch: GameModuleEpoch(1),
        target_definition_hash: runtime.target_def_hash,
        compiled_types: runtime.compiled_types.clone(),
    };

    LoaderIO::reattest(&mut io, attestation);

    assert!(matches!(
        io.poll().as_slice(),
        [IoEvent::Reattested { basis, .. }] if basis == &io.begin_sweep()
    ));
}

#[test]
fn packfile_io_rejects_a_changed_runtime_attestation() {
    let (manifest, archive, runtime, _, _) = fixture(true);
    let mut io = PackfileIO::mount(&manifest, vec![archive], &runtime).unwrap();
    let attestation = LoaderRuntimeAttestation {
        epoch: GameModuleEpoch(1),
        target_definition_hash: [0; 32],
        compiled_types: runtime.compiled_types.clone(),
    };

    LoaderIO::reattest(&mut io, attestation);

    assert!(matches!(
        io.poll().as_slice(),
        [IoEvent::ReattestationFailed { .. }]
    ));
}

#[test]
fn absent_path_table_is_loudly_unsupported() {
    let (manifest, archive, runtime, _, _) = fixture(false);
    let mut io = PackfileIO::mount(&manifest, vec![archive], &runtime).unwrap();
    let basis = io.begin_sweep();
    io.resolve_path(ReqId(1), "assets/a.bundle", &basis);
    assert!(matches!(
        &io.poll()[0],
        IoEvent::PathResolved {
            result: PathResolveResult::Unsupported,
            ..
        }
    ));
}

#[test]
fn packfile_io_normalizes_path_queries_before_lookup() {
    let (manifest, archive, runtime, asset_uuid, _) = fixture(true);
    let mut manifest = distill_pack::manifest::decode_manifest(&manifest).unwrap();
    manifest.paths.as_mut().unwrap()[0].path = "t\u{e9}xtures/a.bundle".into();
    let manifest = encode_manifest(&manifest).unwrap();
    let mut io = PackfileIO::mount(&manifest, vec![archive], &runtime).unwrap();
    let basis = io.begin_sweep();

    io.resolve_path(ReqId(1), "te\u{301}xtures/a.bundle", &basis);

    assert!(matches!(
        &io.poll()[0],
        IoEvent::PathResolved {
            result: PathResolveResult::Resolved(got),
            ..
        } if *got == asset_uuid
    ));
}

#[test]
fn mount_refuses_wrong_runtime_or_archive_identity() {
    let (manifest, archive, mut runtime, _, _) = fixture(true);
    runtime.target_def_hash = [0; 32];
    assert!(matches!(
        PackfileIO::mount(&manifest, vec![archive.clone()], &runtime),
        Err(MountError::Manifest(_))
    ));

    let (_, _, good_runtime, _, _) = fixture(true);
    let mut bad_archive = archive;
    bad_archive[10] ^= 1;
    assert!(matches!(
        PackfileIO::mount(&manifest, vec![bad_archive], &good_runtime),
        Err(MountError::Archive(_))
    ));
}

#[test]
fn mount_refuses_the_internal_trailer_as_the_archive_file_hash() {
    let (manifest_bytes, archive, runtime, _, _) = fixture(true);
    let mut manifest = distill_pack::manifest::decode_manifest(&manifest_bytes).unwrap();
    manifest.archives[0].file_hash = archive[archive.len() - 32..].try_into().unwrap();
    let manifest = encode_manifest(&manifest).unwrap();

    assert!(matches!(
        PackfileIO::mount(&manifest, vec![archive], &runtime),
        Err(MountError::ArchiveFileHash(7))
    ));
}

#[test]
fn mount_rejects_a_compiled_row_outside_exact_artifact_boundary() {
    let (manifest_bytes, archive, mut runtime, _, _) = fixture(true);
    let mut manifest = distill_pack::manifest::decode_manifest(&manifest_bytes).unwrap();
    let extra_type = TypeUuid([0x77; 16]);
    manifest.compiled_types.rows.push(
        CompiledTypeRow::new(
            extra_type,
            LogicalHash([0x77; 32]),
            [0x77; 32],
            false,
            RegistryExtrasV1::default(),
        )
        .unwrap(),
    );
    manifest.compiled_types = CompiledTypeTable::canonical(manifest.compiled_types.rows).unwrap();
    manifest.load_policy.push(LoadPolicyRow {
        type_uuid: extra_type,
        build_only: false,
    });
    runtime.compiled_types = manifest.compiled_types.clone();
    let manifest = encode_manifest(&manifest).unwrap();

    assert!(matches!(
        PackfileIO::mount(&manifest, vec![archive], &runtime),
        Err(MountError::Manifest(
            distill_pack::manifest::ManifestError::CompiledCoverage
        ))
    ));
}

#[test]
fn equal_pack_and_runtime_bootstrap_forgery_fails_independent_local_authority() {
    let (manifest_bytes, archive, mut runtime, _, _) = fixture(true);
    let mut manifest = distill_pack::manifest::decode_manifest(&manifest_bytes).unwrap();
    let bootstrap_uuid = runtime.bootstrap_authority.rows()[0].type_uuid;
    let index = manifest
        .compiled_types
        .rows
        .iter()
        .position(|row| row.type_uuid == bootstrap_uuid)
        .unwrap();
    let original = manifest.compiled_types.rows[index].clone();
    manifest.compiled_types.rows[index] = CompiledTypeRow::new(
        original.type_uuid,
        original.logical_hash,
        [0xee; 32],
        original.build_only,
        original.registry_extras,
    )
    .unwrap();
    manifest.compiled_types = CompiledTypeTable::canonical(manifest.compiled_types.rows).unwrap();
    runtime.compiled_types = manifest.compiled_types.clone();

    let forged_manifest = encode_manifest(&manifest).unwrap();
    assert!(matches!(
        PackfileIO::mount(&forged_manifest, vec![archive], &runtime),
        Err(MountError::BootstrapAuthority(_))
    ));
}

#[test]
fn stale_pack_basis_cannot_read_after_remount() {
    let (manifest, archive, runtime, asset_uuid, _) = fixture(true);
    let mut io = PackfileIO::mount(&manifest, vec![archive], &runtime).unwrap();
    let stale = distill_loader::IoBasis::Pack {
        manifest: distill_loader::ManifestHash([0; 32]),
        load_policy: match io.begin_sweep() {
            distill_loader::IoBasis::Pack { load_policy, .. } => load_policy,
            _ => unreachable!(),
        },
    };
    io.resolve(ReqId(1), asset_uuid, &stale);
    assert!(matches!(
        &io.poll()[0],
        IoEvent::RequestError { req: ReqId(1), .. }
    ));
}
