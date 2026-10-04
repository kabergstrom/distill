use std::collections::BTreeMap;
use std::fs;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use distill_build::query::AssetQuery as BuildAssetQuery;
use distill_build::trace::PackDefinitionControlValue;
use distill_bundle::PathComponent;
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_pack::builder::{
    build_pack, build_publish_and_activate_pack, decode_pack_definition, PackBuildError,
    PackBuildOutput, PackBuildTarget,
};
use distill_pack::{
    activate, archive_filename, manifest_filename, manifest_hash, publish_archive,
    publish_manifest, read_current, PackTarget, PackfileIO, RuntimeTarget,
};
use distill_rpc::capnp_loader::{RemoteHub, RemoteSnapshot};
use distill_rpc::capnp_transport::{CapnpClient, StagedListener};
use distill_rpc::{
    ArtifactPayload, AuthoringValue, BuildAnswer, BuildBackend, BuildRequest, BuildStart,
    BuildView, ConnectRequest, RpcFailure, RuntimeTypePolicy, RuntimeTypePolicyRequest,
    ServedLoadEdge, Server, SnapshotPolicy, TargetDefinition, TargetDefinitionHash,
};
use distill_test_project::{Asset, TestProject};
use distill_wire::artifact::{content_hash, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::WireNode;
use tokio::task::LocalSet;

const TARGET_HASH: [u8; 32] = [7; 32];

struct Fixture {
    // The daemon and its files, held for the fixture's lifetime.
    _project: TestProject,
    server: Server,
    root: AssetUuid,
    child: AssetUuid,
    target: PackTarget,
}

/// Answers each build with the artifact installed for its asset, and every
/// type's runtime policy with the one the test chose.
struct TypePolicyBackend {
    build_only: bool,
    built: BTreeMap<AssetUuid, ContentHash>,
}

impl BuildBackend for TypePolicyBackend {
    fn start(&self, _view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        BuildStart::Answered(match self.built.get(&request.requested_asset) {
            Some(content_hash) => Ok(BuildAnswer::Built {
                content_hash: *content_hash,
            }),
            None => Ok(BuildAnswer::Drifted {
                input: request.drifted_input.clone(),
            }),
        })
    }

    fn runtime_type_policy(
        &self,
        _snapshot: &distill_store::StoreReader,
        _request: &RuntimeTypePolicyRequest,
    ) -> Result<RuntimeTypePolicy, RpcFailure> {
        Ok(RuntimeTypePolicy {
            build_only: self.build_only,
        })
    }
}

fn fixture() -> Fixture {
    fixture_with_policy_and_cycle(false, false)
}

fn fixture_with_policy(build_only: bool) -> Fixture {
    fixture_with_policy_and_cycle(build_only, false)
}

/// A daemon serving `dev` over two runtime assets, each the primary
/// (`main`) of its own bundle file: `root` (assets/root.bundle) loads
/// `child` (assets/child.bundle), and with `cycle` the child loads the
/// root back. Their builds answer the artifacts installed here.
fn fixture_with_policy_and_cycle(build_only: bool, cycle: bool) -> Fixture {
    let runtime_type = TypeUuid([21; 16]);
    let logical_hash = LogicalHash([31; 32]);
    let target_definition = TargetDefinition::new("dev", TargetDefinitionHash(TARGET_HASH));
    let mut project = TestProject::new(vec![target_definition]);
    let server = project.server();

    let root = AssetUuid([1; 16]);
    let child = AssetUuid([2; 16]);
    let wire = WireNode::Unit { offset: 0 };
    let layout_hash = dswl_hash(&wire).unwrap();
    assert_eq!(
        distill_test_project::put_wire_tree(&server.handle(), &dswl_bytes(&wire).unwrap()),
        layout_hash
    );

    let child_row = artifact_row(
        child,
        runtime_type,
        logical_hash,
        layout_hash,
        cycle
            .then_some(ServedLoadEdge {
                asset: root,
                expected_terminal: runtime_type,
            })
            .into_iter()
            .collect(),
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
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &child_row.1),
        child_row.0
    );
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &root_row.1),
        root_row.0
    );
    server.install_build_backend(Arc::new(TypePolicyBackend {
        build_only,
        built: BTreeMap::from([(root, root_row.0), (child, child_row.0)]),
    }));

    for (uuid, path) in [(root, "assets/root.bundle"), (child, "assets/child.bundle")] {
        project.write_bundle(
            path,
            BundleUuid(uuid.0),
            Some("main"),
            &[Asset::blob("main", uuid, runtime_type, b"source")],
        );
    }
    project.publish();

    Fixture {
        _project: project,
        server,
        root,
        child,
        target: PackTarget { name: "dev".into() },
    }
}

impl Fixture {
    /// Serve the fixture over a loopback Cap'n Proto listener and run `body`
    /// against a snapshot pinned on a remote `dev` hub.
    fn remote<T>(&self, body: impl AsyncFnOnce(&RemoteSnapshot, &RemoteHub) -> T) -> T {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        LocalSet::new().block_on(&runtime, async {
            let listener = Rc::new(
                StagedListener::bind(self.server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let serving = Rc::clone(&listener);
            let server_task = tokio::task::spawn_local(async move { serving.serve().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let request = ConnectRequest::new("dev", TargetDefinitionHash(TARGET_HASH));
            let hub = RemoteHub::connected(client.connect(&request).await.unwrap()).unwrap();
            let snapshot = hub.snapshot().await.unwrap().success().unwrap();
            let result = body(&snapshot, &hub).await;
            drop(client);
            server_task.abort();
            result
        })
    }

    fn build(
        &self,
        definition: &PackDefinitionControlValue,
    ) -> Result<PackBuildOutput, PackBuildError> {
        self.remote(async |snapshot, hub| {
            build_pack(
                definition,
                &PackBuildTarget {
                    name: "dev".into(),
                    definition_hash: TARGET_HASH,
                },
                "zstd-test",
                snapshot,
                hub,
            )
            .await
        })
    }
}

fn artifact_row(
    asset: AssetUuid,
    runtime_type: TypeUuid,
    logical_hash: LogicalHash,
    layout_hash: distill_core::id::LayoutHash,
    load_edges: Vec<ServedLoadEdge>,
    blob: Option<Arc<[u8]>>,
) -> (ContentHash, ArtifactPayload) {
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
    let output = fixture.build(&definition(fixture.root)).unwrap();

    assert_eq!(output.manifest.assets.len(), 2);
    assert_eq!(output.manifest.assets[0].asset_uuid, fixture.root);
    assert_eq!(
        output.manifest.assets[0].load_deps[0].asset_uuid,
        fixture.child
    );
    // Each packed runtime entry: its path (it is the primary) and its name.
    let paths = output.manifest.paths.as_ref().unwrap();
    assert_eq!(paths.len(), 4);
    assert_eq!(
        paths
            .iter()
            .filter(|row| row.name.as_deref() == Some("main"))
            .count(),
        2
    );
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
    let directory = tempfile::tempdir().unwrap();
    let directory = directory.path();

    let output = fixture
        .remote(async |snapshot, hub| {
            build_publish_and_activate_pack(
                directory,
                &definition(fixture.root),
                &PackBuildTarget {
                    name: "dev".into(),
                    definition_hash: TARGET_HASH,
                },
                "zstd-test",
                snapshot,
                hub,
            )
            .await
        })
        .unwrap();
    let manifest_hash = manifest_hash(&output.manifest_bytes);

    assert_eq!(read_current(directory).unwrap(), manifest_hash);
    assert_eq!(
        fs::read(directory.join(manifest_filename(manifest_hash))).unwrap(),
        output.manifest_bytes
    );
    assert_eq!(
        fs::read(directory.join(archive_filename(output.archive_file_hash))).unwrap(),
        output.archive_bytes
    );
    let staging = distill_store::atomic_file::staging_dir(directory);
    assert_eq!(
        fs::read_dir(&staging).unwrap().count(),
        0,
        "no temp is left behind"
    );
    assert_eq!(
        fs::read_dir(directory).unwrap().count(),
        4,
        "three files and the staging directory"
    );

    PackfileIO::mount_current(
        directory,
        &RuntimeTarget {
            target: fixture.target.name,
            target_def_hash: TARGET_HASH,
        },
    )
    .unwrap();
}

#[test]
fn build_pack_rejects_an_empty_root_selection() {
    let fixture = fixture();
    assert!(matches!(
        fixture.build(&definition(AssetUuid([99; 16]))),
        Err(PackBuildError::EmptyRoot { index: 0 })
    ));
}

#[test]
fn build_pack_closes_roots_before_rpc_evaluation() {
    let fixture = fixture();
    let mut definition = definition(fixture.root);
    definition.roots[0].authoring_only = Some(true);
    assert!(matches!(
        fixture.build(&definition),
        Err(PackBuildError::InvalidRoot {
            index: 0,
            error: distill_build::query::IntakeError::AuthoringOnlyRestricted,
        })
    ));
}

#[test]
fn build_pack_rejects_build_only_terminal_types_from_the_pinned_policy() {
    let fixture = fixture_with_policy(true);
    assert!(matches!(
        fixture.build(&definition(fixture.root)),
        Err(PackBuildError::BuildOnlyType { type_uuid })
            if type_uuid == TypeUuid([21; 16])
    ));
}

#[test]
fn build_pack_rejects_a_strong_reference_cycle_with_the_complete_path() {
    let fixture = fixture_with_policy_and_cycle(false, true);
    assert!(matches!(
        fixture.build(&definition(fixture.root)),
        Err(PackBuildError::LoadCycle { cycle })
            if cycle == vec![fixture.root, fixture.child, fixture.root]
    ));
}

#[test]
fn build_pack_answers_an_expired_snapshot_as_a_cache_miss() {
    let fixture = fixture();
    fixture
        .server
        .install_snapshot_policy(SnapshotPolicy {
            ttl: Duration::from_millis(50),
            max_snapshots: 8,
            max_connections: 8,
        })
        .unwrap();
    let error = fixture
        .remote(async |snapshot, hub| {
            tokio::time::sleep(Duration::from_millis(200)).await;
            build_pack(
                &definition(fixture.root),
                &PackBuildTarget {
                    name: "dev".into(),
                    definition_hash: TARGET_HASH,
                },
                "zstd-test",
                snapshot,
                hub,
            )
            .await
        })
        .unwrap_err();
    assert!(error.is_cache_miss(), "{error:?}");
}
