use std::collections::BTreeMap;

use distill_bundle::PathComponent;
use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_loader::{IoEvent, LoaderIO, PathResolveResult, ReqId, ResolveResult};
use distill_pack::archive::{encode_archive, ArtifactPayload};
use distill_pack::manifest::{
    encode_manifest, ArchiveRef, EncodingRow, IndexRow, LayoutRegistryRow, LoadPolicyRow,
    ManifestAssetRow, PackManifest, PackTarget, PathRow, WireTreeRow,
};
use distill_pack::{MountError, PackfileIO, RuntimeAttestation};
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
    let manifest = PackManifest {
        target: target.clone(),
        target_def_hash: [4; 32],
        layout_registry: vec![LayoutRegistryRow {
            type_uuid,
            digest: [5; 32],
        }],
        load_policy: vec![LoadPolicyRow {
            type_uuid,
            build_only: false,
        }],
        archives: vec![ArchiveRef {
            generation: 7,
            file_hash: archive.bytes[archive.bytes.len() - 32..]
                .try_into()
                .unwrap(),
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
        layouts: BTreeMap::from([(type_uuid, [5; 32])]),
        load_policy: BTreeMap::from([(type_uuid, false)]),
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
        req: ReqId(1), result: ResolveResult::Built { content_hash: got, basis: inner }, basis: outer, ..
    } if *got == content_hash && inner == &basis && outer == &basis));
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
        IoEvent::IoError {
            req: Some(ReqId(1)),
            ..
        }
    ));
}
