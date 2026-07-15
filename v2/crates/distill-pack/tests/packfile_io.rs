use distill_bundle::PathComponent;
use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_loader::{
    GameModuleEpoch, IoBasis, IoEvent, LoaderIO, PathResolveResult, ReqId, ResolveResult,
    RuntimeTarget as LoaderRuntimeTarget,
};
use distill_pack::archive::{encode_archive, ArtifactPayload};
use distill_pack::manifest::{
    encode_manifest, ArchiveRef, EncodingRow, IndexRow, ManifestAssetRow, PackManifest, PackTarget,
    PathRow, WireTreeRow,
};
use distill_pack::{
    activate, archive_filename, manifest_filename, manifest_hash, MountError, PackfileIO,
    RuntimeTarget,
};
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
    RuntimeTarget,
    AssetUuid,
    distill_core::id::ContentHash,
) {
    let asset_uuid = AssetUuid([1; 16]);
    let type_uuid = TypeUuid([3; 16]);
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
            logical_hash: LogicalHash([6; 32]),
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
    let content_hash = content_hash(&artifact);
    let archive = encode_archive(
        7,
        "zstd-test",
        3,
        &[ArtifactPayload {
            content_hash,
            structural: artifact[..structural_len].to_vec(),
            blobs: vec![blob.to_vec()],
        }],
    )
    .unwrap();
    let encoding = &archive.encodings[&content_hash];
    let manifest = PackManifest {
        target: PackTarget { name: "dev".into() },
        target_def_hash: [4; 32],
        archives: vec![ArchiveRef {
            generation: 7,
            file_hash: *blake3::hash(&archive.bytes).as_bytes(),
        }],
        assets: vec![ManifestAssetRow {
            asset_uuid,
            content_hash,
            load_deps: Vec::new(),
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
    (
        encode_manifest(&manifest).unwrap(),
        archive.bytes,
        RuntimeTarget {
            target: "dev".into(),
            target_def_hash: [4; 32],
        },
        asset_uuid,
        content_hash,
    )
}

fn mount_pack(
    manifest: &[u8],
    archive: &[u8],
    runtime: &RuntimeTarget,
) -> Result<PackfileIO, MountError> {
    let archive_hash = distill_pack::manifest::decode_manifest(manifest)
        .unwrap()
        .archives[0]
        .file_hash;
    mount_pack_as(manifest, archive_hash, archive, runtime)
}

fn mount_pack_as(
    manifest: &[u8],
    archive_hash: [u8; 32],
    archive: &[u8],
    runtime: &RuntimeTarget,
) -> Result<PackfileIO, MountError> {
    let directory = tempfile::tempdir().unwrap();
    let manifest_hash = manifest_hash(manifest);
    std::fs::write(
        directory.path().join(manifest_filename(manifest_hash)),
        manifest,
    )
    .unwrap();
    std::fs::write(
        directory.path().join(archive_filename(archive_hash)),
        archive,
    )
    .unwrap();
    activate(directory.path(), manifest_hash).unwrap();
    PackfileIO::mount_current(directory.path(), runtime)
}

#[test]
fn resolves_fetches_and_paths_under_one_manifest_basis() {
    let (manifest, archive, runtime, asset_uuid, content_hash) = fixture(true);
    let mut io = mount_pack(&manifest, &archive, &runtime).unwrap();
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
        && artifact.load_edges.is_empty()
        && !artifact.wire_layout.is_empty()
        && got == &basis)
    );
    assert!(matches!(&events[2], IoEvent::PathResolved {
        req: ReqId(3), result: PathResolveResult::Resolved(got), basis: outer, ..
    } if *got == asset_uuid && outer == &basis));
}

#[test]
fn current_mount_keeps_blob_ranges_alive_after_io_drops() {
    let (manifest, archive, runtime, _, content_hash) = fixture(true);
    let mut io = mount_pack(&manifest, &archive, &runtime).unwrap();
    let basis = io.begin_sweep();
    io.fetch(ReqId(1), content_hash, &basis);
    let Some(IoEvent::Fetched { artifact, .. }) = io.poll().pop() else {
        panic!("expected mapped artifact");
    };

    drop(io);
    assert_eq!(artifact.blobs[0].as_bytes(), b"blob");
}

#[test]
fn target_binding_checks_only_the_module_target_definition_hash() {
    let (manifest, archive, runtime, _, _) = fixture(true);
    let mut io = mount_pack(&manifest, &archive, &runtime).unwrap();
    let target = LoaderRuntimeTarget::new(GameModuleEpoch(1), [4; 32]);
    io.bind_target(target.clone());
    assert!(
        matches!(io.poll().as_slice(), [IoEvent::TargetBound { target: got, basis }]
        if got == &target && basis == &io.begin_sweep())
    );

    io.bind_target(LoaderRuntimeTarget::new(GameModuleEpoch(2), [9; 32]));
    assert!(matches!(
        io.poll().as_slice(),
        [IoEvent::TargetRejected { .. }]
    ));
}

#[test]
fn mount_canonicalizes_the_runtime_target_name() {
    let (manifest, archive, mut runtime, _, _) = fixture(true);
    let mut decoded = distill_pack::manifest::decode_manifest(&manifest).unwrap();
    decoded.target.name = "t\u{e9}st".into();
    let manifest = encode_manifest(&decoded).unwrap();
    runtime.target = "te\u{301}st".into();

    mount_pack(&manifest, &archive, &runtime).unwrap();
}

#[test]
fn absent_path_table_is_loudly_unsupported() {
    let (manifest, archive, runtime, _, _) = fixture(false);
    let mut io = mount_pack(&manifest, &archive, &runtime).unwrap();
    let basis = io.begin_sweep();
    io.resolve_path(ReqId(1), "assets/a.bundle", &basis);
    assert!(matches!(
        io.poll().as_slice(),
        [IoEvent::PathResolved {
            result: PathResolveResult::Unsupported,
            ..
        }]
    ));
}

#[test]
fn path_queries_are_normalized_before_lookup() {
    let (manifest, archive, runtime, asset_uuid, _) = fixture(true);
    let mut decoded = distill_pack::manifest::decode_manifest(&manifest).unwrap();
    decoded.paths.as_mut().unwrap()[0].path = "t\u{e9}xtures/a.bundle".into();
    let manifest = encode_manifest(&decoded).unwrap();
    let mut io = mount_pack(&manifest, &archive, &runtime).unwrap();
    let basis = io.begin_sweep();
    io.resolve_path(ReqId(1), "te\u{301}xtures/a.bundle", &basis);

    assert!(matches!(io.poll().as_slice(), [IoEvent::PathResolved {
        result: PathResolveResult::Resolved(got), ..
    }] if *got == asset_uuid));
}

#[test]
fn mount_refuses_wrong_runtime_or_archive_identity() {
    let (manifest, archive, mut runtime, _, _) = fixture(true);
    runtime.target_def_hash = [0; 32];
    assert!(matches!(
        mount_pack(&manifest, &archive, &runtime),
        Err(MountError::TargetMismatch)
    ));

    let (_, _, runtime, _, _) = fixture(true);
    let mut bad_archive = archive;
    bad_archive[10] ^= 1;
    assert!(matches!(
        mount_pack(&manifest, &bad_archive, &runtime),
        Err(MountError::Archive(_))
    ));
}

#[test]
fn mount_authenticates_complete_manifest_before_serving_any_asset() {
    let (manifest, archive, runtime, _, _) = fixture(true);
    let mut missing_encoding = distill_pack::manifest::decode_manifest(&manifest).unwrap();
    let content_hash = missing_encoding.assets[0].content_hash;
    missing_encoding.encodings.clear();
    let missing_encoding = encode_manifest(&missing_encoding).unwrap();
    assert!(matches!(
        mount_pack(&missing_encoding, &archive, &runtime),
        Err(MountError::MissingEncoding(hash)) if hash == content_hash
    ));

    let mut bad_wire = distill_pack::manifest::decode_manifest(&manifest).unwrap();
    let layout_hash = bad_wire.wire_trees[0].layout_hash;
    bad_wire.wire_trees[0].bytes.push(0);
    let bad_wire = encode_manifest(&bad_wire).unwrap();
    assert!(matches!(
        mount_pack(&bad_wire, &archive, &runtime),
        Err(MountError::WireTree(hash)) if hash == layout_hash
    ));

    let mut wrong_header = distill_pack::manifest::decode_manifest(&manifest).unwrap();
    wrong_header.assets[0].asset_uuid = AssetUuid([9; 16]);
    let wrong_header = encode_manifest(&wrong_header).unwrap();
    assert!(matches!(
        mount_pack(&wrong_header, &archive, &runtime),
        Err(MountError::Manifest(
            distill_pack::manifest::ManifestError::MetadataMismatch
        ))
    ));
}

#[test]
fn internal_archive_trailer_is_not_the_archive_file_hash() {
    let (manifest, archive, runtime, _, _) = fixture(true);
    let mut decoded = distill_pack::manifest::decode_manifest(&manifest).unwrap();
    decoded.archives[0].file_hash = archive[archive.len() - 32..].try_into().unwrap();
    let archive_hash = decoded.archives[0].file_hash;
    let manifest = encode_manifest(&decoded).unwrap();

    assert!(matches!(
        mount_pack_as(&manifest, archive_hash, &archive, &runtime),
        Err(MountError::ArchiveFileHash(7))
    ));
}

#[test]
fn stale_pack_basis_cannot_read() {
    let (manifest, archive, runtime, asset_uuid, _) = fixture(true);
    let mut io = mount_pack(&manifest, &archive, &runtime).unwrap();
    let stale = IoBasis::Pack {
        manifest: distill_loader::ManifestHash([0; 32]),
    };
    io.resolve(ReqId(1), asset_uuid, &stale);
    assert!(matches!(
        io.poll().as_slice(),
        [IoEvent::RequestError { req: ReqId(1), .. }]
    ));
}
