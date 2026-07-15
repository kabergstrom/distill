use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use distill_build::query::AssetQuery as BuildAssetQuery;
use distill_build::trace::PackDefinitionControlValue;
use distill_bundle::PathComponent;
use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_pack::builder::{
    build_pack, build_publish_and_activate_pack, decode_pack_definition, PackBuildError,
    PackBuildTarget,
};
use distill_pack::{
    activate, archive_filename, manifest_filename, manifest_hash, publish_archive,
    publish_manifest, read_current, PackTarget, PackfileIO, RuntimeTarget,
};
use distill_rpc::{
    ArtifactPayload, AssetDeltaState, AssetMutation, AuthoringEntry, AuthoringEntryRole,
    AuthoringMutation, AuthoringValue, Commit, ConnectOutcome, ConnectRequest, PathMutation,
    ServedLoadEdge, Server, StoreInstanceId, StoredResolve, TargetDefinition, TargetDefinitionHash,
};
use distill_schema::ngp_schema::{node_hash, SchemaNode};
use distill_wire::artifact::{content_hash, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::WireNode;

const TARGET_HASH: [u8; 32] = [7; 32];

struct Fixture {
    hub: distill_rpc::Hub,
    snapshot: distill_rpc::Snapshot,
    root: AssetUuid,
    child: AssetUuid,
    target: PackTarget,
}

fn fixture() -> Fixture {
    let runtime_type = TypeUuid([21; 16]);
    let logical_hash = LogicalHash([31; 32]);
    let target_definition = TargetDefinition::new("dev", TargetDefinitionHash(TARGET_HASH));
    let server = Server::new(StoreInstanceId([9; 16]), vec![target_definition]).unwrap();

    let root = AssetUuid([1; 16]);
    let child = AssetUuid([2; 16]);
    let wire = WireNode::Unit { offset: 0 };
    let layout_hash = dswl_hash(&wire).unwrap();
    server
        .install_wire_tree(layout_hash, Arc::from(dswl_bytes(&wire).unwrap()))
        .unwrap();

    let child_row = artifact_row(
        child,
        runtime_type,
        logical_hash,
        layout_hash,
        Vec::new(),
        Some(Arc::from(&b""[..])),
    );
    let root_row = artifact_row(
        root,
        runtime_type,
        logical_hash,
        layout_hash,
        vec![ServedLoadEdge {
            asset: child,
            expected_terminal: runtime_type,
        }],
        None,
    );
    server.install_artifact(child_row.0, child_row.1).unwrap();
    server.install_artifact(root_row.0, root_row.1).unwrap();

    server
        .commit(Commit {
            authoring: vec![
                AuthoringMutation::Set(entry(root, runtime_type, "assets/root.bundle")),
                AuthoringMutation::Set(entry(child, runtime_type, "assets/child.bundle")),
            ],
            assets: vec![
                AssetMutation::Set {
                    uuid: root,
                    resolution: StoredResolve::Built {
                        content_hash: root_row.0,
                    },
                    delta: AssetDeltaState::Changed,
                },
                AssetMutation::Set {
                    uuid: child,
                    resolution: StoredResolve::Built {
                        content_hash: child_row.0,
                    },
                    delta: AssetDeltaState::Changed,
                },
            ],
            paths: vec![
                PathMutation::Set {
                    path: "assets/root.bundle".into(),
                    candidates: BTreeSet::from([root]),
                },
                PathMutation::Set {
                    path: "assets/child.bundle".into(),
                    candidates: BTreeSet::from([child]),
                },
            ],
            ..Commit::default()
        })
        .unwrap();

    let request = ConnectRequest::new("dev", TargetDefinitionHash(TARGET_HASH));
    let hub = match server.root().connect(request) {
        ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("connection failed: {other:?}"),
    };
    let snapshot = hub.snapshot().success().unwrap();
    Fixture {
        hub,
        snapshot,
        root,
        child,
        target: PackTarget { name: "dev".into() },
    }
}

fn artifact_row(
    asset: AssetUuid,
    runtime_type: TypeUuid,
    logical_hash: LogicalHash,
    layout_hash: distill_core::id::LayoutHash,
    load_edges: Vec<ServedLoadEdge>,
    blob: Option<Arc<[u8]>>,
) -> (distill_core::id::ContentHash, ArtifactPayload) {
    let load_deps = load_edges.iter().map(|edge| edge.asset).collect::<Vec<_>>();
    let blob_inputs = blob
        .iter()
        .map(|bytes| (vec![PathComponent::Field("blob".into())], bytes.as_ref()))
        .collect::<Vec<_>>();
    let complete = write_artifact(
        &ArtifactHeader {
            asset_uuid: asset,
            authored_type: runtime_type,
            terminal_type: runtime_type,
            encoded_type: runtime_type,
            logical_hash,
            layout_hash,
        },
        &load_deps,
        &[],
        &[],
        &blob_inputs,
    )
    .unwrap();
    let parsed = distill_wire::artifact::parse_artifact(&complete).unwrap();
    let structural_len = complete.len() - parsed.blob_section.len();
    let hash = content_hash(&complete);
    (
        hash,
        ArtifactPayload {
            structural: Arc::from(complete[..structural_len].to_vec()),
            blobs: blob.into_iter().collect(),
            load_edges,
        },
    )
}

fn entry(uuid: AssetUuid, runtime_type: TypeUuid, path: &str) -> AuthoringEntry {
    let schema_hash = node_hash(&SchemaNode::Blob).unwrap();
    AuthoringEntry {
        uuid,
        bundle: BundleUuid(uuid.0),
        local_id: "main".into(),
        normalized_path: path.into(),
        type_uuid: runtime_type,
        terminal_type: runtime_type,
        schema_hash,
        logical_schema: Arc::from(&b"\"blob\""[..]),
        role: AuthoringEntryRole::Runtime,
        tags: BTreeMap::new(),
        value: AuthoringValue {
            canonical_value: Arc::from(&b"{\"$distill_blob\":0}"[..]),
            blobs: vec![Arc::from(&b"source"[..])],
        },
    }
}

fn definition(root: AssetUuid) -> PackDefinitionControlValue {
    PackDefinitionControlValue {
        roots: vec![BuildAssetQuery {
            uuid: Some(root),
            ..BuildAssetQuery::default()
        }],
        target: "dev".into(),
        zstd_level: 3,
        include_path_table: true,
    }
}

fn byte_array(bytes: &[u8]) -> distill_json::AuthoredValue {
    distill_json::AuthoredValue::Array(
        bytes
            .iter()
            .map(|byte| distill_json::AuthoredValue::UInt(u128::from(*byte)))
            .collect(),
    )
}

fn pack_definition_authoring(root: AssetUuid) -> AuthoringValue {
    use distill_json::AuthoredValue as Value;

    let query = Value::Object(BTreeMap::from([
        ("authored_type".to_owned(), Value::Null),
        ("authoring_only".to_owned(), Value::Null),
        ("bundle_path".to_owned(), Value::Null),
        ("bundle_uuid".to_owned(), Value::Null),
        ("local_id".to_owned(), Value::Null),
        ("path_glob".to_owned(), Value::Null),
        ("path_prefix".to_owned(), Value::Null),
        ("tag".to_owned(), Value::Null),
        ("terminal_type".to_owned(), Value::Null),
        ("uuid".to_owned(), byte_array(&root.0)),
    ]));
    let value = Value::Object(BTreeMap::from([
        ("include_path_table".to_owned(), Value::Bool(true)),
        ("roots".to_owned(), Value::Array(vec![query])),
        ("target".to_owned(), Value::Str("dev".to_owned())),
        ("zstd_level".to_owned(), Value::Int(-2)),
    ]));
    AuthoringValue {
        canonical_value: Arc::from(distill_json::write(&value).unwrap().into_bytes()),
        blobs: Vec::new(),
    }
}

#[test]
fn decodes_the_sealed_pack_definition_authored_shape() {
    let root = AssetUuid([42; 16]);
    assert_eq!(
        decode_pack_definition(&pack_definition_authoring(root)).unwrap(),
        PackDefinitionControlValue {
            roots: vec![BuildAssetQuery {
                uuid: Some(root),
                ..BuildAssetQuery::default()
            }],
            target: "dev".to_owned(),
            zstd_level: -2,
            include_path_table: true,
        }
    );
}

#[test]
fn pack_definition_decoder_rejects_noncanonical_or_blob_backed_values() {
    let mut noncanonical = pack_definition_authoring(AssetUuid([42; 16]));
    let mut bytes = noncanonical.canonical_value.to_vec();
    bytes.push(b' ');
    noncanonical.canonical_value = Arc::from(bytes);
    assert!(matches!(
        decode_pack_definition(&noncanonical),
        Err(PackBuildError::Definition(_))
    ));

    let mut blob_backed = pack_definition_authoring(AssetUuid([42; 16]));
    blob_backed.blobs.push(Arc::from(&b"unexpected"[..]));
    assert!(matches!(
        decode_pack_definition(&blob_backed),
        Err(PackBuildError::Definition(_))
    ));
}

#[test]
fn build_pack_pulls_the_typed_closure_and_emits_mountable_files() {
    let fixture = fixture();
    let output = build_pack(
        &definition(fixture.root),
        &PackBuildTarget {
            name: "dev".into(),
            definition_hash: TARGET_HASH,
        },
        "zstd-test",
        &fixture.snapshot,
        &fixture.hub,
    )
    .unwrap();

    assert_eq!(output.manifest.assets.len(), 2);
    assert_eq!(output.manifest.assets[0].asset_uuid, fixture.root);
    assert_eq!(
        output.manifest.assets[0].load_deps[0].asset_uuid,
        fixture.child
    );
    assert_eq!(output.manifest.paths.as_ref().unwrap().len(), 2);
    assert_eq!(
        output.archive_file_hash,
        *blake3::hash(&output.archive_bytes).as_bytes()
    );

    let directory = tempfile::tempdir().unwrap();
    publish_archive(directory.path(), &output.archive_bytes).unwrap();
    let mounted_manifest = publish_manifest(directory.path(), &output.manifest_bytes).unwrap();
    activate(directory.path(), mounted_manifest).unwrap();
    PackfileIO::mount_current(
        directory.path(),
        &RuntimeTarget {
            target: fixture.target.name,
            target_def_hash: TARGET_HASH,
        },
    )
    .unwrap();
}

#[test]
fn build_publish_and_activate_pack_commits_the_complete_pack() {
    let fixture = fixture();
    let directory = std::env::temp_dir().join(format!(
        "distill-pack-build-publish-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();

    let output = build_publish_and_activate_pack(
        &directory,
        &definition(fixture.root),
        &PackBuildTarget {
            name: "dev".into(),
            definition_hash: TARGET_HASH,
        },
        "zstd-test",
        &fixture.snapshot,
        &fixture.hub,
    )
    .unwrap();
    let manifest_hash = manifest_hash(&output.manifest_bytes);

    assert_eq!(read_current(&directory).unwrap(), manifest_hash);
    assert_eq!(
        fs::read(directory.join(manifest_filename(manifest_hash))).unwrap(),
        output.manifest_bytes
    );
    assert_eq!(
        fs::read(directory.join(archive_filename(output.archive_file_hash))).unwrap(),
        output.archive_bytes
    );
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 3);

    PackfileIO::mount_current(
        &directory,
        &RuntimeTarget {
            target: fixture.target.name,
            target_def_hash: TARGET_HASH,
        },
    )
    .unwrap();

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn build_pack_rejects_an_empty_root_selection() {
    let fixture = fixture();
    assert!(matches!(
        build_pack(
            &definition(AssetUuid([99; 16])),
            &PackBuildTarget {
                name: "dev".into(),
                definition_hash: TARGET_HASH,
            },
            "zstd-test",
            &fixture.snapshot,
            &fixture.hub,
        ),
        Err(PackBuildError::EmptyRoot { index: 0 })
    ));
}

#[test]
fn build_pack_closes_roots_before_rpc_evaluation() {
    let fixture = fixture();
    let mut definition = definition(fixture.root);
    definition.roots[0].authoring_only = Some(true);
    assert!(matches!(
        build_pack(
            &definition,
            &PackBuildTarget {
                name: "dev".into(),
                definition_hash: TARGET_HASH,
            },
            "zstd-test",
            &fixture.snapshot,
            &fixture.hub,
        ),
        Err(PackBuildError::InvalidRoot {
            index: 0,
            error: distill_build::query::IntakeError::AuthoringOnlyRestricted,
        })
    ));
}
