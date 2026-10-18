use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use distill_schema::ngp_schema::Value;
use distill_rpc::capnp_loader::{RemoteAuthoringBlob, RemoteCall, RemoteHub};
use distill_rpc::capnp_transport::{
    schema, CapnpClient, RemoteConnectOutcome, RemoteMetadataOutcome, StagedListener,
};
use distill_rpc::*;
use distill_schema::ngp_schema::{node_hash, SchemaNode};
use distill_test_project::{
    Asset, TestBuilds, TestProject, PARENT_TYPE, REFLECTION, ROOT, TAGGED_TYPE,
};
use tokio::task::LocalSet;

fn target() -> TargetDefinition {
    TargetDefinition::new("dev", TargetDefinitionHash([7; 16]))
}

fn request() -> ConnectRequest {
    ConnectRequest::new("dev", TargetDefinitionHash([7; 16]))
}

/// An empty project whose daemon serves the target "dev" (definition 7).
fn project() -> TestProject {
    TestProject::new(vec![target()])
}

/// `entry` (see [`authoring_entry`]) as an authored asset.
fn asset_of(entry: &AuthoringEntry) -> Asset {
    let asset = Asset::blob(
        &entry.local_id,
        entry.uuid,
        entry.type_uuid,
        entry.value.blobs[0].bytes().expect("the entry carries its bytes"),
    );
    match entry.role {
        AuthoringEntryRole::Runtime => asset,
        AuthoringEntryRole::AuthoringOnly => asset.authoring_only(),
    }
}

/// Author `entry` as its own bundle file, at its path; a runtime entry is
/// the bundle's primary asset (its path resolves to it).
fn write_entry(project: &mut TestProject, entry: &AuthoringEntry) {
    let primary = (entry.role == AuthoringEntryRole::Runtime).then_some(entry.local_id.as_str());
    project.write_bundle(
        &entry.normalized_path,
        entry.bundle,
        primary,
        &[asset_of(entry)],
    );
}

/// [`write_entry`], published as one version.
fn publish_entry(project: &mut TestProject, entry: &AuthoringEntry) -> SnapshotStamp {
    write_entry(project, entry);
    project.publish()
}

/// An input version that changes nothing these tests watch: an unrelated
/// bundle file appears.
fn publish_unrelated(project: &mut TestProject) -> SnapshotStamp {
    let version = project.server().current_stamp().unwrap().version.0;
    let entry = authoring_entry(
        200u8.wrapping_add(version as u8),
        AuthoringEntryRole::AuthoringOnly,
    );
    publish_entry(project, &entry)
}

/// The configuration error [`reject_configuration`] publishes.
fn configuration_error(file_hash: [u8; 16]) -> ConfigurationError {
    ConfigurationError::from_reason(
        &DscpV1::MalformedConfiguration { file_hash },
        "invalid staged configuration",
    )
}

/// The daemon rejects its configuration source (a malformed configuration
/// file hashing to `file_hash`): no version is published, the current one
/// is served under the failure (its stamp is returned).
fn reject_configuration(project: &TestProject, file_hash: [u8; 16]) -> SnapshotStamp {
    project
        .coordinator()
        .reject_configuration(
            DscpV1::MalformedConfiguration { file_hash },
            "invalid staged configuration",
        )
        .unwrap();
    project.server().current_stamp().unwrap()
}

fn canonical_artifact(
    asset: AssetUuid,
    type_uuid: TypeUuid,
    layout_hash: LayoutHash,
    fixed: &[u8],
) -> (ContentHash, ArtifactPayload) {
    let complete = distill_wire::artifact::write_artifact(
        &distill_wire::artifact::ArtifactHeader {
            asset_uuid: asset,
            authored_type: type_uuid,
            terminal_type: type_uuid,
            encoded_type: type_uuid,
            logical_hash: LogicalHash([77; 16]),
            layout_hash,
        },
        &[],
        fixed,
        &[],
        &[],
    )
    .unwrap();
    let hash = distill_wire::artifact::content_hash(&complete);
    (
        hash,
        ArtifactPayload {
            structural: complete.into(),
            blobs: Vec::new(),
            load_edges: Vec::new(),
        },
    )
}

#[tokio::test(flavor = "current_thread")]
async fn remote_loader_client_preserves_typed_calls() {
    LocalSet::new()
        .run_until(async {
            let mut project = project();
            let server = project.server();
            let asset = AssetUuid([44; 16]);
            let path = "assets/remote.bundle";
            let wire = distill_wire::wire::WireNode::Unit { offset: 0 };
            let layout_hash = distill_wire::dswl::dswl_hash(&wire).unwrap();
            let wire_bytes: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&wire).unwrap());
            assert_eq!(
                distill_test_project::put_wire_tree(&server.handle(), &wire_bytes),
                layout_hash
            );
            let (content_hash, artifact) =
                canonical_artifact(asset, TypeUuid([1; 16]), layout_hash, &[7, 8, 9]);
            assert_eq!(
                distill_test_project::put_artifact(&server.handle(), &artifact),
                content_hash
            );
            // The bundle file at `path`: its primary asset is `asset`,
            // drifted until something builds it.
            project.write_bundle(
                path,
                BundleUuid([45; 16]),
                Some("remote"),
                &[Asset::blob("remote", asset, TypeUuid([1; 16]), &[1])],
            );
            let stamp = project.publish();
            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = Rc::clone(&listener);
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = RemoteHub::connected(client.connect(&request()).await.unwrap()).unwrap();

            let snapshot = match hub.snapshot().await.unwrap() {
                RemoteCall::Success(snapshot) => snapshot,
                other => panic!("snapshot failed: {other:?}"),
            };
            assert_eq!(snapshot.basis().snapshot, stamp);
            let resolved = match snapshot.resolve(asset).await.unwrap() {
                RemoteCall::Success(terminal) => terminal,
                other => panic!("resolve failed: {other:?}"),
            };
            assert!(matches!(
                resolved.value,
                ResolveResult::Drifted {
                    input: DriftedInput::Asset(input),
                    current,
                } if input == asset && current == stamp
            ));
            let path_result = match snapshot.resolve_path(path).await.unwrap() {
                RemoteCall::Success(terminal) => terminal.value,
                other => panic!("path resolve failed: {other:?}"),
            };
            assert_eq!(path_result, PathResolveResult::Resolved(asset));

            let newer = publish_unrelated(&mut project);
            assert_ne!(newer, stamp);
            let mut fetched = match snapshot.fetch(content_hash).await.unwrap() {
                RemoteCall::Success(terminal) => terminal,
                other => panic!("fetch failed: {other:?}"),
            };
            assert_eq!(fetched.basis.snapshot, stamp);
            let fetched_total = fetched.value.total_bytes();
            let mut structural = Vec::new();
            while let Some(chunk) = fetched.value.next_chunk().await.unwrap() {
                assert_eq!(chunk.kind, ArtifactChunkKind::Structural);
                structural.extend_from_slice(&chunk.bytes);
            }
            assert_eq!(
                distill_wire::artifact::content_hash(&structural),
                content_hash
            );
            assert_eq!(fetched_total, structural.len() as u64);
            assert_eq!(
                match hub.wire_tree(layout_hash).await.unwrap() {
                    RemoteCall::Success(bytes) => bytes,
                    other => panic!("wire tree failed: {other:?}"),
                },
                wire_bytes
            );
            assert!(matches!(
                snapshot.import_failures().await.unwrap(),
                RemoteCall::Success(failures) if failures.is_empty()
            ));

            let mut subscription = match hub
                .subscribe(stamp.version, vec![asset], vec![])
                .await
                .unwrap()
            {
                RemoteCall::Success(subscription) => subscription,
                other => panic!("subscription failed: {other:?}"),
            };
            assert!(matches!(
                subscription.next().await.unwrap(),
                Some(StreamEvent::InitialDelta { .. })
            ));

            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn capnp_snapshots_hard_expire_and_a_closed_connection_releases_them() {
    LocalSet::new()
        .run_until(async {
            let project = project();
            let server = project.server();
            server
                .install_snapshot_policy(SnapshotPolicy {
                    ttl: std::time::Duration::from_millis(200),
                    max_snapshots: 8,
                    max_connections: 8,
                })
                .unwrap();
            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = Rc::clone(&listener);
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.connect(&request()).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, .. } => hub,
                other => panic!("expected connected, got {other:?}"),
            };
            let open_snapshot = || async {
                let response = hub.snapshot_request().send().promise.await.unwrap();
                match response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                {
                    schema::snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                    _ => panic!("expected a snapshot"),
                }
            };
            let version = |snapshot: schema::snapshot::Client| async move {
                let response = snapshot.version_request().send().promise.await.unwrap();
                matches!(
                    response
                        .get()
                        .unwrap()
                        .get_result()
                        .unwrap()
                        .which()
                        .unwrap(),
                    schema::u_int64_call::Which::Success(_)
                )
            };

            // Use does not extend a snapshot: it expires at its TTL.
            let first = open_snapshot().await;
            assert!(version(first.clone()).await);
            assert_eq!(server.open_snapshots(), 1);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            assert!(version(first.clone()).await);
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            assert_eq!(server.open_snapshots(), 0);
            let response = first.version_request().send().promise.await.unwrap();
            assert!(matches!(
                response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap(),
                schema::u_int64_call::Which::SnapshotExpired(())
            ));

            // Closing the connection releases the snapshots it held.
            server
                .install_snapshot_policy(SnapshotPolicy {
                    ttl: std::time::Duration::from_secs(60),
                    max_snapshots: 8,
                    max_connections: 8,
                })
                .unwrap();
            let second = open_snapshot().await;
            assert!(version(second.clone()).await);
            assert_eq!(server.open_snapshots(), 1);
            drop((second, first, hub, client));
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(server.open_snapshots(), 0);
        })
        .await;
}

#[derive(Default)]
struct RecordingAuthoringBackend {
    writes: Mutex<Vec<(InputVersion, Vec<AuthoringOp>)>>,
    imports: Mutex<Vec<ImportRequest>>,
    reimports: Mutex<Vec<BundleUuid>>,
    operations: Mutex<Vec<LongRunningOp>>,
}

/// The receipt [`RecordingAuthoringBackend`] answers a write with: the
/// file of each set entry, under [`ROOT`].
fn receipt_of(operations: &[AuthoringOp]) -> WriteReceipt {
    WriteReceipt {
        files: operations
            .iter()
            .filter_map(|operation| match operation {
                AuthoringOp::Set(entry) => Some(WrittenFile {
                    root: ROOT.to_owned(),
                    path: entry.normalized_path.clone(),
                    content_hash: None,
                }),
                AuthoringOp::Remove { .. } => None,
            })
            .collect(),
    }
}

impl AuthoringBackend for RecordingAuthoringBackend {
    fn read_file(
        &self,
        _: &distill_store::StoreReader,
        _: &str,
        _: &str,
    ) -> Result<Vec<u8>, String> {
        unreachable!("never inspects")
    }

    fn write_files(
        &self,
        _store: &mut distill_store::Store,
        base: InputVersion,
        operations: &[AuthoringOp],
        _force_lossy: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        self.writes
            .lock()
            .unwrap()
            .push((base, operations.to_vec()));
        Ok(receipt_of(operations))
    }

    fn prepare_import(
        &self,
        _store: &mut distill_store::Store,
        _base: InputVersion,
        request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        self.imports.lock().unwrap().push(request.clone());
        Ok(PreparedImportCommit {
            bundle: BundleUuid([70; 16]),
            commit: Commit::default(),
        })
    }

    fn prepare_reimport(
        &self,
        _store: &mut distill_store::Store,
        _base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        self.reimports.lock().unwrap().push(bundle);
        Ok(PreparedImportCommit {
            bundle,
            commit: Commit::default(),
        })
    }

    fn prepare_operation(
        &self,
        _store: &mut distill_store::Store,
        _base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        self.operations.lock().unwrap().push(operation.clone());
        let payload = match operation {
            LongRunningOp::RenameWithFixups(payload) | LongRunningOp::Doctor(payload) => {
                payload.clone()
            }
        };
        Ok(PreparedOperationCommit::immediate(
            Commit::default(),
            vec![
                AuthoringProgressEvent {
                    sequence: 0,
                    state: AuthoringProgressState::Started,
                    payload: Arc::from([]),
                },
                AuthoringProgressEvent {
                    sequence: 1,
                    state: AuthoringProgressState::Running,
                    payload,
                },
                AuthoringProgressEvent {
                    sequence: 2,
                    state: AuthoringProgressState::Completed,
                    payload: Arc::from([]),
                },
            ],
        ))
    }
}

/// A finished build, answered the same at every snapshot.
struct Finished(Result<BuildAnswer, RpcFailure>);

impl BuildCompletion for Finished {
    fn answer(self: Box<Self>, _view: BuildView<'_>) -> Result<BuildAnswer, RpcFailure> {
        self.0
    }
}

/// Submits a build that finishes (drifted) once the test releases it.
struct BlockingBuildBackend {
    started: Arc<AtomicBool>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl BuildBackend for BlockingBuildBackend {
    fn start(&self, _view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        self.started.store(true, Ordering::Release);
        let release = Arc::clone(&self.release);
        let input = request.drifted_input.clone();
        BuildStart::Submitted(BuildTicket::new(async move {
            while !*release.0.lock().unwrap() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            Box::new(Finished(Ok(BuildAnswer::Drifted { input }))) as Box<dyn BuildCompletion>
        }))
    }
}

#[test]
fn schema_uses_typed_five_arm_results_for_every_hub_and_snapshot_method() {
    // A Windows checkout may carry CRLF line ends.
    let source = include_str!("../../distill-client/schema/distill_rpc.capnp").replace("\r\n", "\n");
    assert!(!source.contains("CallStatus"));
    assert!(!source.contains("AnyPointer"));
    assert!(!source.contains("-> (status"));
    for name in [
        "SnapshotCall",
        "AuthoringSnapshotCall",
        "SubscribeCall",
        "VoidCall",
        "UInt64Call",
        "UuidCall",
        "ProgressCall",
        "DataCall",
        "UuidListCall",
        "EntryMetaCall",
        "ResolveCall",
        "PathResolveCall",
        "ChunkStreamCall",
        "RuntimeTypePolicyCall",
        "DescribeTypeCall",
        "DescribableTypesCall",
        "AuthoringListCall",
        "SourceFilesCall",
        "ReferencesCall",
        "AuthorableCall",
    ] {
        let marker = format!("struct {name} {{");
        let body = source
            .split_once(marker.as_str())
            .unwrap_or_else(|| panic!("missing {name}"))
            .1
            .split_once("\n  }\n}")
            .unwrap_or_else(|| panic!("unterminated {name}"))
            .0;
        assert!(body.contains("success @0"), "{name} success ordinal");
        assert!(
            body.contains("reconnectRequired @1"),
            "{name} reconnect ordinal"
        );
        assert!(
            body.contains("configurationFailed @2"),
            "{name} configuration ordinal"
        );
        assert!(body.contains("snapshotExpired @3"), "{name} expiry ordinal");
        assert!(body.contains("error @4"), "{name} error ordinal");
    }
    assert!(!source.contains("Reattest"));

    let body = |name: &str| {
        source
            .split_once(&format!("struct {name} {{"))
            .unwrap_or_else(|| panic!("dedicated {name}"))
            .1
            .split_once("\n  }\n}")
            .unwrap_or_else(|| panic!("terminated {name}"))
            .0
    };
    let inspect = body("AuthoringInspectManyCall");
    for arm in [
        "success @0 :List(AuthoringInspectAnswer)",
        "reconnectRequired @1 :ReconnectRequired",
        "configurationFailed @2 :ConfigurationError",
        "snapshotExpired @3 :Void",
        "error @4 :RpcError",
    ] {
        assert!(inspect.contains(arm), "missing authoring inspect arm {arm}");
    }
    let answer = body("AuthoringInspectAnswer");
    for arm in [
        "success @0 :AuthoringInspection",
        "missing @1 :Void",
        "roleIneligible @2 :AuthoringRoleFailure",
        "drifted @3 :DriftedResolve",
    ] {
        assert!(answer.contains(arm), "missing authoring inspect answer arm {arm}");
    }
    assert!(source.contains("authoringSnapshot @8 () -> (result :AuthoringSnapshotCall);"));
    assert!(!source.contains("fetch @6 (hash :Data) -> (result :ChunkStreamCall);"));
    assert!(source.contains("fetch @7 (hash :Data) -> (result :ChunkStreamCall);"));
    assert!(source.contains("interface AuthoringSnapshot {"));
    assert!(source.contains("version @0 () -> (result :UInt64Call);"));
    assert!(source.contains("query @1 (query :AssetQuery) -> (result :UuidListCall);"));
    assert!(source.contains("inspect @2 (uuids :List(Data)) -> (result :AuthoringInspectManyCall);"));
    assert!(source.contains("blob @11 (uuid :Data, index :UInt32) -> (result :AuthoringBlobCall);"));
    assert!(source.contains("refresh @3 () -> (result :AuthoringSnapshotCall);"));
    for field in [
        "uuid @0 :OptionalData",
        "bundlePath @1 :OptionalText",
        "localId @2 :OptionalText",
        "bundleUuid @3 :OptionalData",
        "authoredType @4 :OptionalData",
        "terminalType @5 :OptionalData",
        "tag @6 :OptionalTagSelector",
        "pathPrefix @7 :OptionalText",
        "pathGlob @8 :OptionalText",
        "authoringOnly @9 :OptionalBool",
    ] {
        assert!(source.contains(field), "missing AssetQuery field {field}");
    }
    assert!(source.contains("stamp @0 :SnapshotStampValue"));
    assert!(source.contains("inputVersion @1 :UInt64"));
}

/// A metadata connection never outlives its store (the state directory is
/// locked to the daemon process), so the metadata calls have no
/// reconnect arm: success, expiry, error.
#[test]
fn schema_metadata_calls_have_no_reconnect_arm() {
    let source = include_str!("../../distill-client/schema/distill_rpc.capnp").replace("\r\n", "\n");
    assert!(!source.contains("MetadataReconnect"));
    for name in [
        "MetadataSnapshotCall",
        "MetadataAuthoringSnapshotCall",
        "MetadataDiagnosticsCall",
        "MetadataUInt64Call",
        "MetadataChunkStreamCall",
        "MetadataUuidListCall",
        "MetadataEntryMetaCall",
        "MetadataPathResolveCall",
        "MetadataAuthoringInspectCall",
    ] {
        let marker = format!("struct {name} {{");
        let body = source
            .split_once(marker.as_str())
            .unwrap_or_else(|| panic!("missing {name}"))
            .1
            .split_once("\n  }\n}")
            .unwrap_or_else(|| panic!("unterminated {name}"))
            .0;
        assert!(body.contains("success @0"), "{name} success ordinal");
        assert!(body.contains("snapshotExpired @1"), "{name} expiry ordinal");
        assert!(body.contains("error @2"), "{name} error ordinal");
        assert!(!body.contains("reconnectRequired"), "{name} reconnect arm");
    }
}

/// A hub connection never outlives its store either: the one reason to
/// reconnect is a pipeline change.
#[test]
fn schema_reconnect_reason_is_the_pipeline_epoch_alone() {
    let source = include_str!("../../distill-client/schema/distill_rpc.capnp").replace("\r\n", "\n");
    assert!(
        source.contains("enum ReconnectReason {\n  pipelineEpochChanged @0;\n}\n"),
        "ReconnectReason has one arm"
    );
    assert!(!source.contains("storeInstanceChanged"));
    assert_eq!(schema::ReconnectReason::PipelineEpochChanged as u16, 0);
}

/// The authoring entry the daemon serves for the asset `byte`: one blob
/// (`byte + 2`) in its own bundle file `bundle-{byte}.bundle`, the bundle's
/// primary. The daemon tags nothing without a project schema.
fn authoring_entry(byte: u8, role: AuthoringEntryRole) -> AuthoringEntry {
    let schema_hash = node_hash(&SchemaNode::Blob).unwrap();
    AuthoringEntry {
        root: String::new(),
        uuid: AssetUuid([byte; 16]),
        bundle: BundleUuid([byte.wrapping_add(1); 16]),
        local_id: format!("entry-{byte}"),
        normalized_path: format!("bundle-{byte}.bundle"),
        type_uuid: TypeUuid([1; 16]),
        terminal_type: TypeUuid([1; 16]),
        schema_hash,
        logical_schema: Arc::from(&b"\"blob\""[..]),
        role,
        tags: std::collections::BTreeMap::new(),
        value: AuthoringValue {
            canonical_value: Arc::from(&b"{\"len\":1,\"offset\":0}"[..]),
            blobs: vec![AuthoringBlob::Bytes(Arc::from([byte.wrapping_add(2)]))],
            types: Vec::new(),
        },
    }
}

fn write_authoring_entry_value(
    mut output: schema::authoring_entry_value::Builder<'_>,
    entry: &AuthoringEntry,
) {
    output.reborrow().init_uuid().set_bytes(&entry.uuid.0);
    output.reborrow().init_bundle().set_bytes(&entry.bundle.0);
    output.set_local_id(entry.local_id.as_str());
    output.set_normalized_path(entry.normalized_path.as_str());
    output.set_root(entry.root.as_str());
    output
        .reborrow()
        .init_type_uuid()
        .set_bytes(&entry.type_uuid.0);
    output
        .reborrow()
        .init_terminal_type()
        .set_bytes(&entry.terminal_type.0);
    output.set_schema_hash(&entry.schema_hash.0);
    output.set_logical_schema(&entry.logical_schema);
    output.set_role(match entry.role {
        AuthoringEntryRole::Runtime => schema::AuthoringEntryRole::Runtime,
        AuthoringEntryRole::AuthoringOnly => schema::AuthoringEntryRole::AuthoringOnly,
    });
    let mut tags = output.reborrow().init_tags(entry.tags.len() as u32);
    for (index, (tag, value)) in entry.tags.iter().enumerate() {
        let mut row = tags.reborrow().get(index as u32);
        row.set_tag(tag.as_str());
        if let Some(value) = value {
            row.set_has_value(true);
            row.set_value(value.as_str());
        }
    }
    let mut value = output.init_value();
    value.set_canonical_value(&entry.value.canonical_value);
    let mut blobs = value.init_blobs(entry.value.blobs.len() as u32);
    for (index, blob) in entry.value.blobs.iter().enumerate() {
        let bytes = blob.bytes().expect("a written entry carries its bytes");
        blobs.reborrow().get(index as u32).init_bytes(bytes.len() as u32).copy_from_slice(bytes);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn authoring_snapshot_round_trips_over_real_tcp_with_exact_stamp_and_role_fence() {
    LocalSet::new()
        .run_until(async {
            let mut project = TestProject::configured(false);
            let server = project.server();
            let entry = authoring_entry(3, AuthoringEntryRole::AuthoringOnly);
            // A runtime asset with no authoring entry, for the authoring
            // role fence: the declared output of a processed parent.
            let parent = project.asset(
                "parent",
                AssetUuid([5; 16]),
                PARENT_TYPE,
                Value::Struct(Box::new([Value::U8(5)])),
            );
            let runtime_uuid = AssetUuid::v5(parent.uuid, REFLECTION);
            write_entry(&mut project, &entry);
            project.write_bundle(
                "parent.bundle",
                BundleUuid([60; 16]),
                Some("parent"),
                &[parent],
            );
            let first_stamp = project.publish();
            let request =
                ConnectRequest::new(project.target().name(), project.target().definition_hash());

            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = listener.clone();
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.connect(&request).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, .. } => hub,
                other => panic!("expected connected, got {other:?}"),
            };

            let metadata_hub = match client.metadata(PROTOCOL_VERSION).await.unwrap() {
                RemoteMetadataOutcome::Connected { hub, .. } => hub,
                _ => panic!("expected metadata bootstrap"),
            };
            let snapshot_response = metadata_hub
                .snapshot_request()
                .send()
                .promise
                .await
                .unwrap();
            let snapshot_result = snapshot_response.get().unwrap().get_result().unwrap();
            let metadata_snapshot = match snapshot_result.which().unwrap() {
                schema::metadata_snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("expected metadata snapshot"),
            };
            let mut entry_call = metadata_snapshot.entry_request();
            entry_call
                .get()
                .reborrow()
                .init_uuid()
                .set_bytes(&entry.uuid.0);
            let entry_response = entry_call.send().promise.await.unwrap();
            let entry_result = entry_response.get().unwrap().get_result().unwrap();
            let metadata = match entry_result.which().unwrap() {
                schema::metadata_entry_meta_call::Which::Success(metadata) => metadata.unwrap(),
                _ => panic!("expected typed entry metadata"),
            };
            assert_eq!(
                metadata.get_uuid().unwrap().get_bytes().unwrap(),
                &entry.uuid.0
            );
            assert_eq!(
                metadata.get_authored_type().unwrap().get_bytes().unwrap(),
                &entry.type_uuid.0
            );
            assert_eq!(metadata.get_schema_hash().unwrap(), &entry.schema_hash.0);

            let response = hub
                .authoring_snapshot_request()
                .send()
                .promise
                .await
                .unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            let snapshot = match result.which().unwrap() {
                schema::authoring_snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("expected authoring snapshot"),
            };

            let mut query = snapshot.query_request();
            {
                let mut query_value = query.get().init_query();
                query_value.reborrow().init_authoring_only().set_value(true);
            }
            let query_response = query.send().promise.await.unwrap();
            let query_result = query_response.get().unwrap().get_result().unwrap();
            match query_result.which().unwrap() {
                schema::uuid_list_call::Which::Success(values) => {
                    let values = values.unwrap();
                    assert_eq!(values.len(), 1);
                    assert_eq!(values.get(0).get_bytes().unwrap(), &entry.uuid.0);
                }
                _ => panic!("expected authoring-only query result"),
            }

            let mut inspect = snapshot.inspect_request();
            inspect.get().init_uuids(1).set(0, &entry.uuid.0);
            let inspect_response = inspect.send().promise.await.unwrap();
            let inspect_result = inspect_response.get().unwrap().get_result().unwrap();
            let schema::authoring_inspect_many_call::Which::Success(answers) = inspect_result.which().unwrap() else {
                panic!("expected authoring inspection");
            };
            match answers.unwrap().get(0).which().unwrap() {
                schema::authoring_inspect_answer::Which::Success(inspection) => {
                    let inspection = inspection.unwrap();
                    let stamp = inspection.get_stamp().unwrap();
                    assert_eq!(stamp.get_instance().unwrap(), &first_stamp.instance.0);
                    assert_eq!(stamp.get_input_version(), first_stamp.version.0);
                    assert_eq!(inspection.get_uuid().unwrap(), &entry.uuid.0);
                    assert_eq!(
                        inspection.get_logical_schema().unwrap(),
                        &*entry.logical_schema
                    );
                    assert_eq!(
                        inspection.get_role().unwrap(),
                        schema::AuthoringEntryRole::AuthoringOnly
                    );
                    assert_eq!(
                        inspection
                            .get_value()
                            .unwrap()
                            .get_canonical_value()
                            .unwrap(),
                        &*entry.value.canonical_value
                    );
                }
                _ => panic!("expected authoring inspection"),
            }

            let mut wrong_role = snapshot.inspect_request();
            wrong_role.get().init_uuids(1).set(0, &runtime_uuid.0);
            let wrong_role_response = wrong_role.send().promise.await.unwrap();
            let wrong_role_result = wrong_role_response.get().unwrap().get_result().unwrap();
            let schema::authoring_inspect_many_call::Which::Success(answers) = wrong_role_result.which().unwrap() else {
                panic!("expected typed authoring wrong-role result");
            };
            match answers.unwrap().get(0).which().unwrap() {
                schema::authoring_inspect_answer::Which::RoleIneligible(failure) => {
                    assert_eq!(
                        failure.unwrap().get_observed().unwrap(),
                        schema::AuthoringEntryRole::Runtime
                    );
                }
                _ => panic!("expected typed authoring wrong-role result"),
            }

            let runtime_response = hub.snapshot_request().send().promise.await.unwrap();
            let runtime = match runtime_response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("expected runtime snapshot"),
            };
            let mut resolve = runtime.resolve_request();
            resolve.get().set_uuid(&entry.uuid.0);
            let resolve_response = resolve.send().promise.await.unwrap();
            let terminal = match resolve_response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::resolve_call::Which::Success(terminal) => terminal.unwrap(),
                schema::resolve_call::Which::Error(error) => {
                    let error = error.unwrap();
                    panic!(
                        "unexpected resolve error: {}",
                        error.get_message().unwrap().to_str().unwrap()
                    )
                }
                _ => panic!("expected typed terminal role failure"),
            };
            assert!(matches!(
                terminal.get_result().unwrap().which().unwrap(),
                schema::resolve_result::Which::RoleIneligible(_)
            ));

            let mut replacement = entry;
            replacement.value.blobs = vec![AuthoringBlob::Bytes(Arc::from([9u8]))];
            let second_stamp = publish_entry(&mut project, &replacement);
            assert!(second_stamp.version > first_stamp.version);
            let refresh_response = snapshot.refresh_request().send().promise.await.unwrap();
            let refreshed = match refresh_response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::authoring_snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("expected refreshed authoring snapshot"),
            };
            let version_response = refreshed.version_request().send().promise.await.unwrap();
            match version_response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::u_int64_call::Which::Success(version) => {
                    assert_eq!(version, second_stamp.version.0)
                }
                _ => panic!("expected refreshed version"),
            }

            let failed_stamp = reject_configuration(&project, [7; 16]);
            let failed_refresh = refreshed.refresh_request().send().promise.await.unwrap();
            let failed = match failed_refresh
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::authoring_snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("pure authoring metadata refresh must survive a configuration error"),
            };
            let failed_version = failed.version_request().send().promise.await.unwrap();
            assert!(matches!(
                failed_version
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap(),
                schema::u_int64_call::Which::Success(version)
                    if version == failed_stamp.version.0
            ));

            project.reconfigure(true);
            let fenced = failed.version_request().send().promise.await.unwrap();
            assert!(matches!(
                fenced.get().unwrap().get_result().unwrap().which().unwrap(),
                schema::u_int64_call::Which::ReconnectRequired(_)
            ));

            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn listener_staging_rejects_non_loopback_before_binding() {
    let project = project();
    let root = project.server().root();
    let error = match StagedListener::bind(root, "192.0.2.1:0").await {
        Ok(_) => panic!("non-loopback must fail staging"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        distill_rpc::capnp_transport::TransportError::BindValidation(
            BindStageError::NonLoopbackAddress { .. }
        )
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn listener_staging_binds_ipv4_and_ipv6_loopback() {
    let project = project();
    let ipv4 = StagedListener::bind(project.server().root(), "127.0.0.1:0")
        .await
        .unwrap();
    assert!(ipv4.local_addr().unwrap().ip().is_loopback());

    // Some CI hosts disable IPv6. When available, it must remain loopback.
    if let Ok(ipv6) = StagedListener::bind(project.server().root(), "[::1]:0").await {
        assert!(ipv6.local_addr().unwrap().ip().is_loopback());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unbound_metadata_bootstrap_round_trips_over_tcp_while_failed() {
    LocalSet::new()
        .run_until(async {
            let mut project = project();
            let server = project.server();
            // A malformed bundle file: a namespace error naming it.
            project.write("broken.bundle", b"not a bundle");
            project.publish();
            let error = configuration_error([6; 16]);
            let stamp = reject_configuration(&project, [6; 16]);
            let namespace_errors = match server.root().metadata(PROTOCOL_VERSION) {
                MetadataConnectOutcome::Connected(connected) => {
                    let diagnostics = connected.hub.diagnostics().success().unwrap();
                    assert_eq!(diagnostics.stamp, stamp);
                    diagnostics.namespace_errors
                }
                _ => panic!("expected an in-process metadata bootstrap"),
            };
            let [namespace_error] = &namespace_errors[..] else {
                panic!("expected one namespace error, got {namespace_errors:?}")
            };
            assert!(matches!(
                &namespace_error.detail,
                NamespaceErrorV1::IncompleteSkeleton { source, .. }
                    if source.root_name == ROOT && source.normalized_path == "broken.bundle"
            ));
            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = listener.clone();
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.metadata(PROTOCOL_VERSION).await.unwrap() {
                RemoteMetadataOutcome::Connected {
                    hub,
                    instance,
                    protocol_epoch,
                } => {
                    assert_eq!(instance, stamp.instance);
                    assert_eq!(protocol_epoch, PROTOCOL_VERSION);
                    hub
                }
                _ => panic!("expected metadata bootstrap"),
            };
            let snapshot_response = hub.snapshot_request().send().promise.await.unwrap();
            let snapshot = match snapshot_response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::metadata_snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("expected metadata snapshot under a configuration error"),
            };
            let version = snapshot.version_request().send().promise.await.unwrap();
            assert!(matches!(
                version.get().unwrap().get_result().unwrap().which().unwrap(),
                schema::metadata_u_int64_call::Which::Success(value) if value == stamp.version.0
            ));
            let diagnostics = hub.diagnostics_request().send().promise.await.unwrap();
            let diagnostics = match diagnostics
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::metadata_diagnostics_call::Which::Success(value) => value.unwrap(),
                _ => panic!("expected typed error diagnostics"),
            };
            match diagnostics.get_configuration().unwrap().which().unwrap() {
                schema::configuration_diagnostic::Which::Failed(value) => {
                    let value = value.unwrap();
                    assert_eq!(value.get_code(), error.code as u16);
                    assert_eq!(value.get_reason_hash().unwrap(), error.reason_hash);
                }
                _ => panic!("expected failed diagnostics"),
            }
            let errors = diagnostics.get_namespace_errors().unwrap();
            assert_eq!(errors.len(), 1);
            assert_eq!(
                &distill_rpc::capnp_transport::decode_namespace_error(errors.get(0)).unwrap(),
                namespace_error
            );
            let mut query = snapshot.query_request();
            {
                let mut q = query.get().init_q();
                q.reborrow().init_uuid().set_bytes(&[0; 16]);
                q.reborrow().init_bundle().set_bytes(&[0; 16]);
                q.reborrow().init_authored_type().set_bytes(&[0; 16]);
                q.set_normalized_path_prefix("");
                q.set_role(schema::AuthoringEntryRole::Runtime);
            }
            let response = query.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::metadata_uuid_list_call::Which::Success(value) => {
                    assert!(value.unwrap().is_empty());
                }
                _ => panic!("a namespace error does not gate the namespace query"),
            }
            let mut noncanonical = snapshot.query_request();
            {
                let mut q = noncanonical.get().init_q();
                q.reborrow().init_uuid().set_bytes(&[1; 16]);
                q.reborrow().init_bundle().set_bytes(&[0; 16]);
                q.reborrow().init_authored_type().set_bytes(&[0; 16]);
                q.set_normalized_path_prefix("");
                q.set_role(schema::AuthoringEntryRole::Runtime);
            }
            let response = noncanonical.send().promise.await.unwrap();
            assert!(matches!(
                response.get().unwrap().get_result().unwrap().which().unwrap(),
                schema::metadata_uuid_list_call::Which::Error(error)
                    if invalid_request(error.clone().unwrap()) == Some(schema::InvalidRequest::Uuid)
            ));
            drop(client);
            server_task.await.unwrap().unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn generated_rpc_system_round_trips_connect_snapshot_resolve_fetch_and_delta() {
    LocalSet::new()
        .run_until(async {
            let mut project = project();
            let server = project.server();
            let builds = TestBuilds::install(&server);
            let uuid = AssetUuid([3; 16]);
            let (hash, payload) =
                canonical_artifact(uuid, TypeUuid([1; 16]), LayoutHash([4; 16]), &[10, 11, 12]);
            let expected_structural = payload.structural.clone();
            assert_eq!(
                distill_test_project::put_artifact(&server.handle(), &payload),
                hash
            );
            builds.answer(uuid, Ok(BuildAnswer::Built { content_hash: hash }));
            let asset_bundle = |value: u8| {
                (
                    BundleUuid([30; 16]),
                    [Asset::blob("asset", uuid, TypeUuid([1; 16]), &[value])],
                )
            };
            let (bundle, assets) = asset_bundle(1);
            project.write_bundle("asset.bundle", bundle, Some("asset"), &assets);
            let stamp = project.publish();
            assert_eq!(stamp.version, InputVersion(1));

            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = listener.clone();
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.connect(&request()).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, instance } => {
                    assert_eq!(instance, stamp.instance);
                    hub
                }
                other => panic!("expected connected, got {other:?}"),
            };

            let snapshot_response = hub.snapshot_request().send().promise.await.unwrap();
            let snapshot_result = snapshot_response.get().unwrap().get_result().unwrap();
            let snapshot = match snapshot_result.which().unwrap() {
                schema::snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("expected snapshot capability"),
            };

            let mut resolve = snapshot.resolve_request();
            resolve.get().set_uuid(&uuid.0);
            let resolve_response = resolve.send().promise.await.unwrap();
            let resolve_result = resolve_response.get().unwrap().get_result().unwrap();
            let terminal = match resolve_result.which().unwrap() {
                schema::resolve_call::Which::Success(terminal) => terminal.unwrap(),
                _ => panic!("expected terminal resolve"),
            };
            assert_eq!(
                terminal
                    .get_basis()
                    .unwrap()
                    .get_stamp()
                    .unwrap()
                    .get_version(),
                1
            );
            match terminal.get_result().unwrap().which().unwrap() {
                schema::resolve_result::Which::Built(bytes) => {
                    assert_eq!(bytes.unwrap(), &hash.0)
                }
                _ => panic!("expected built result"),
            }

            let mut fetch = snapshot.fetch_request();
            fetch.get().set_hash(&hash.0);
            let fetch_response = fetch.send().promise.await.unwrap();
            let fetch_result = fetch_response.get().unwrap().get_result().unwrap();
            let terminal = match fetch_result.which().unwrap() {
                schema::chunk_stream_call::Which::Success(terminal) => terminal.unwrap(),
                _ => panic!("expected terminal fetch"),
            };
            assert_eq!(
                terminal
                    .get_basis()
                    .unwrap()
                    .get_stamp()
                    .unwrap()
                    .get_version(),
                1
            );
            assert_eq!(terminal.get_total_bytes(), expected_structural.len() as u64);
            let chunks = terminal.get_chunks().unwrap();
            let chunk_response = chunks.next_request().send().promise.await.unwrap();
            let chunk = chunk_response.get().unwrap();
            assert!(!chunk.get_done());
            assert_eq!(chunk.get_kind(), 0);
            assert_eq!(chunk.get_bytes().unwrap(), expected_structural.as_ref());

            let mut subscribe = hub.subscribe_request();
            {
                let mut params = subscribe.get();
                params.set_since(0);
                params.reborrow().init_assets(1).set(0, &uuid.0);
                params.init_paths(0);
            }
            let subscribe_response = subscribe.send().promise.await.unwrap();
            let subscribe_result = subscribe_response.get().unwrap().get_result().unwrap();
            let installed = match subscribe_result.which().unwrap() {
                schema::subscribe_call::Which::Success(installed) => installed.unwrap(),
                _ => panic!("expected subscription"),
            };
            assert_eq!(installed.get_installed(), 1);
            let deltas = installed.get_deltas().unwrap();
            let delta_response = deltas.next_request().send().promise.await.unwrap();
            let delta = delta_response.get().unwrap();
            assert!(!delta.get_done());
            let event = delta.get_event().unwrap();
            assert_eq!(
                event
                    .get_basis()
                    .unwrap()
                    .get_stamp()
                    .unwrap()
                    .get_version(),
                1
            );
            match event.which().unwrap() {
                schema::stream_event::Which::InitialDelta(initial) => {
                    // The first publication logs nothing: a client reads
                    // its state.
                    assert_eq!(initial.unwrap().len(), 0);
                }
                _ => panic!("expected cursor-bound initial delta"),
            }

            // `DeltaStream.next` is a long-polling capability call. A commit
            // after the call is in flight wakes it without a polling gap.
            let pending_live = deltas.next_request().send().promise;
            let (bundle, assets) = asset_bundle(2);
            project.write_bundle("asset.bundle", bundle, Some("asset"), &assets);
            project.publish();
            let live_response =
                tokio::time::timeout(std::time::Duration::from_secs(2), pending_live)
                    .await
                    .unwrap()
                    .unwrap();
            let live_event = live_response.get().unwrap().get_event().unwrap();
            assert_eq!(
                live_event
                    .get_basis()
                    .unwrap()
                    .get_stamp()
                    .unwrap()
                    .get_version(),
                2
            );
            assert!(matches!(
                live_event.which().unwrap(),
                schema::stream_event::Which::Delta(_)
            ));

            // A configuration error remains a typed result union over the
            // generated transport; refresh itself remains safe.
            let error = configuration_error([8; 16]);
            reject_configuration(&project, [8; 16]);
            let refresh_response = snapshot.refresh_request().send().promise.await.unwrap();
            let refresh_result = refresh_response.get().unwrap().get_result().unwrap();
            let failed_snapshot = match refresh_result.which().unwrap() {
                schema::snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("refresh must remain valid under a configuration error"),
            };
            let mut failed_resolve = failed_snapshot.resolve_request();
            failed_resolve.get().set_uuid(&uuid.0);
            let failed_response = failed_resolve.send().promise.await.unwrap();
            let failed_result = failed_response.get().unwrap().get_result().unwrap();
            match failed_result.which().unwrap() {
                schema::resolve_call::Which::ConfigurationFailed(value) => {
                    let value = value.unwrap();
                    assert_eq!(value.get_code(), error.code as u16);
                    assert_eq!(value.get_reason_hash().unwrap(), &error.reason_hash);
                }
                _ => panic!("resolve must return a typed configuration error"),
            }

            // The stream notification is advisory; stale capabilities are
            // independently fenced by the generated server adapter.
            fence_pipeline(&project);
            let mut fenced_resolve = failed_snapshot.resolve_request();
            fenced_resolve.get().set_uuid(&uuid.0);
            let fenced_response = fenced_resolve.send().promise.await.unwrap();
            let fenced_result = fenced_response.get().unwrap().get_result().unwrap();
            match fenced_result.which().unwrap() {
                schema::resolve_call::Which::ReconnectRequired(reconnect) => {
                    assert_eq!(
                        reconnect.unwrap().get_reason().unwrap(),
                        schema::ReconnectReason::PipelineEpochChanged
                    );
                }
                _ => panic!("stale capability must be generation-fenced"),
            }
            let version = failed_snapshot
                .version_request()
                .send()
                .promise
                .await
                .unwrap();
            assert!(matches!(
                version
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap(),
                schema::u_int64_call::Which::ReconnectRequired(_)
            ));
            let configuration = failed_snapshot
                .configuration_request()
                .send()
                .promise
                .await
                .unwrap();
            assert!(matches!(
                configuration
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap(),
                schema::void_call::Which::ReconnectRequired(_)
            ));

            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_slow_snapshot_build_does_not_stall_the_rpc_io_thread() {
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut project = project();
    let server = project.server();
    server.install_build_backend(Arc::new(BlockingBuildBackend {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    }));
    let entry = authoring_entry(41, AuthoringEntryRole::Runtime);
    let drift_stamp = publish_entry(&mut project, &entry);

    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let root = server.root();
    let server_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        LocalSet::new().block_on(&runtime, async move {
            let listener = StagedListener::bind(root, "127.0.0.1:0").await.unwrap();
            address_tx.send(listener.local_addr().unwrap()).unwrap();
            listener.serve_one().await.unwrap();
        });
    });
    let address = address_rx.recv().unwrap();

    LocalSet::new()
        .run_until(async {
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.connect(&request()).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, .. } => hub,
                other => panic!("expected connected, got {other:?}"),
            };
            let response = hub.snapshot_request().send().promise.await.unwrap();
            let snapshot = match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("expected snapshot capability"),
            };

            let mut resolve = snapshot.resolve_request();
            resolve.get().set_uuid(&entry.uuid.0);
            let resolve_task = tokio::task::spawn_local(resolve.send().promise);
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while !started.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("build callback started");

            let metadata = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                client.metadata(PROTOCOL_VERSION),
            )
            .await;
            {
                let (released, wake) = &*release;
                *released.lock().unwrap() = true;
                wake.notify_all();
            }
            let resolve_response = resolve_task.await.unwrap().unwrap();
            let resolve_terminal = match resolve_response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::resolve_call::Which::Success(terminal) => terminal.unwrap(),
                _ => panic!("expected typed drifted resolve"),
            };
            let drifted = match resolve_terminal.get_result().unwrap().which().unwrap() {
                schema::resolve_result::Which::Drifted(drifted) => drifted.unwrap(),
                _ => panic!("expected drifted result"),
            };
            let drifted_asset = match drifted.get_input().unwrap().which().unwrap() {
                schema::drifted_input_value::Which::Asset(asset) => asset.unwrap(),
                _ => panic!("expected asset drift input"),
            };
            assert_eq!(drifted_asset, &entry.uuid.0);
            let current = drifted.get_current().unwrap();
            assert_eq!(current.get_instance().unwrap(), &drift_stamp.instance.0);
            assert_eq!(current.get_version(), drift_stamp.version.0);
            assert!(matches!(
                metadata,
                Ok(Ok(RemoteMetadataOutcome::Connected { .. }))
            ));
            drop(client);
        })
        .await;
    server_thread.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn wire_rejects_wrong_hash_and_uuid_widths_as_typed_results() {
    LocalSet::new()
        .run_until(async {
            let project = project();
            let listener = Rc::new(
                StagedListener::bind(project.server().root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = listener.clone();
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();

            let mut wrong_protocol = request();
            wrong_protocol.protocol = PROTOCOL_VERSION + 1;
            assert!(matches!(
                client.connect(&wrong_protocol).await.unwrap(),
                RemoteConnectOutcome::ProtocolFailure {
                    expected: PROTOCOL_VERSION,
                    observed,
                    ..
                } if observed == PROTOCOL_VERSION + 1
            ));

            let mut wrong_target = request();
            wrong_target.target_definition_hash = TargetDefinitionHash([8; 16]);
            assert!(matches!(
                client.connect(&wrong_target).await.unwrap(),
                RemoteConnectOutcome::TargetFailure(
                    ConnectError::TargetDefinitionMismatch { expected, got }
                ) if expected == TargetDefinitionHash([7; 16])
                    && got == TargetDefinitionHash([8; 16])
            ));

            let mut malformed = client.root().connect_request();
            {
                let mut params = malformed.get();
                params.set_target("dev");
                params.set_target_def_hash(&[7; 31]);
                params.set_protocol(PROTOCOL_VERSION);
            }
            let malformed_response = malformed.send().promise.await.unwrap();
            let malformed_result = malformed_response.get().unwrap().get_result().unwrap();
            match malformed_result.which().unwrap() {
                schema::connect_call::Which::Error(failure) => {
                    assert_eq!(
                        invalid_request(failure.unwrap()),
                        Some(schema::InvalidRequest::Hash)
                    );
                }
                _ => panic!("wrong target-definition hash width must be rejected"),
            }

            let hub = match client.connect(&request()).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, .. } => hub,
                other => panic!("expected connected, got {other:?}"),
            };
            let snapshot_response = hub.snapshot_request().send().promise.await.unwrap();
            let snapshot_result = snapshot_response.get().unwrap().get_result().unwrap();
            let snapshot = match snapshot_result.which().unwrap() {
                schema::snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("expected snapshot"),
            };
            let mut resolve = snapshot.resolve_request();
            resolve.get().set_uuid(&[1; 15]);
            let response = resolve.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::resolve_call::Which::Error(failure) => {
                    assert_eq!(
                        invalid_request(failure.unwrap()),
                        Some(schema::InvalidRequest::Uuid)
                    )
                }
                _ => panic!("wrong UUID width must be a typed failure"),
            }

            let mut fetch = snapshot.fetch_request();
            fetch.get().set_hash(&[2; 31]);
            let response = fetch.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::chunk_stream_call::Which::Error(failure) => {
                    assert_eq!(
                        invalid_request(failure.unwrap()),
                        Some(schema::InvalidRequest::Hash)
                    )
                }
                _ => panic!("wrong hash width must be a typed failure"),
            }

            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn wire_connect_returns_closed_pipeline_unavailable_diagnostic() {
    LocalSet::new()
        .run_until(async {
            let project = project();
            let server = project.server();
            let failure = PipelineFailure::new(
                PipelineFailureCode::CandidateRegistration,
                PipelineFailureOrigin::CandidateOpen,
                CleanupDisposition::CleanedAndClosed,
                "duplicate processor id",
            )
            .unwrap();
            let mut writer = project.coordinator().open_writer().unwrap();
            project
                .coordinator()
                .publish_pipeline_rejection(&mut writer, failure.clone())
                .unwrap();
            drop(writer);
            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = listener.clone();
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();

            assert!(matches!(
                client.connect(&request()).await.unwrap(),
                RemoteConnectOutcome::PipelineUnavailable(
                    PipelineUnavailableDiagnostic::PipelineFailure(observed)
                ) if observed == failure
            ));

            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn hub_authoring_operation_and_wire_tree_methods_are_live_and_generation_first() {
    LocalSet::new()
        .run_until(async {
            // The subject is the hub's handling of a custom authoring
            // backend: a second server over the daemon's store serves it.
            let project = project();
            let backend = Arc::new(RecordingAuthoringBackend::default());
            let handle = ServerHandle::open(
                backend.clone(),
                Arc::clone(project.coordinator().server_handle().opener()),
            );
            let server = Server::open(&handle);
            let entry = authoring_entry(31, AuthoringEntryRole::Runtime);
            let wire_node = distill_wire::wire::WireNode::Unit { offset: 0 };
            let tree: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&wire_node).unwrap());
            let layout_hash = distill_wire::dswl::dswl_hash(&wire_node).unwrap();
            assert_eq!(
                distill_test_project::put_wire_tree(&server.handle(), &tree),
                layout_hash
            );
            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = listener.clone();
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.connect(&request()).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, .. } => hub,
                other => panic!("expected connected, got {other:?}"),
            };

            let mut write = hub.write_request();
            {
                let mut params = write.get();
                params.set_base(0);
                let mut ops = params.reborrow().init_ops(1);
                write_authoring_entry_value(ops.reborrow().get(0).init_set(), &entry);
            }
            let write = write.send().promise.await.unwrap();
            // The backend writes the files and answers their receipt; the
            // version moves only when the watcher publishes them.
            let expected_ops = vec![AuthoringOp::Set(entry.clone())];
            match write.get().unwrap().get_result().unwrap().which().unwrap() {
                schema::data_call::Which::Success(receipt) => assert_eq!(
                    WriteReceipt::decode(receipt.unwrap()).unwrap(),
                    receipt_of(&expected_ops)
                ),
                _ => panic!("expected a write receipt"),
            }
            assert_eq!(
                *backend.writes.lock().unwrap(),
                vec![(InputVersion(0), expected_ops)]
            );
            assert_eq!(server.current_stamp().unwrap().version, InputVersion(0));

            let mut import = hub.import_request();
            {
                let mut params = import.get();
                params.set_base(0);
                let mut request = params.init_request();
                request.set_importer("image-importer");
                let mut sources = request.reborrow().init_sources(1);
                sources.set(0, "source/image.png");
                request.set_dest("generated/image.bundle");
                let mut settings = request.reborrow().init_settings();
                settings.set_canonical_value(b"{}");
                settings.reborrow().init_blobs(1).get(0).set_bytes(b"settings-blob");
                request.set_watch(true);
                request.set_root("assets");
            }
            let import = import.send().promise.await.unwrap();
            match import.get().unwrap().get_result().unwrap().which().unwrap() {
                schema::uuid_call::Which::Success(uuid) => {
                    assert_eq!(uuid.unwrap().get_bytes().unwrap(), &[70; 16])
                }
                _ => panic!("expected imported bundle identity"),
            }

            let mut reimport = hub.reimport_request();
            reimport.get().set_base(1);
            reimport.get().reborrow().init_bundle().set_bytes(&[70; 16]);
            let reimport = reimport.send().promise.await.unwrap();
            assert!(matches!(
                reimport
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap(),
                schema::uuid_call::Which::Success(_)
            ));

            let mut operation = hub.operation_request();
            operation.get().set_base(2);
            operation
                .get()
                .reborrow()
                .init_operation()
                .set_doctor(b"verify-cas");
            let operation = operation.send().promise.await.unwrap();
            let progress = match operation
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::progress_call::Which::Success(progress) => progress.unwrap(),
                _ => panic!("expected progress capability"),
            };
            let started = progress.next_request().send().promise.await.unwrap();
            assert!(!started.get().unwrap().get_done());
            assert_eq!(
                started
                    .get()
                    .unwrap()
                    .get_progress()
                    .unwrap()
                    .get_state()
                    .unwrap(),
                schema::AuthoringProgressState::Started
            );
            let running = progress.next_request().send().promise.await.unwrap();
            let running = running.get().unwrap().get_progress().unwrap();
            assert_eq!(
                running.get_state().unwrap(),
                schema::AuthoringProgressState::Running
            );
            assert_eq!(running.get_payload().unwrap(), b"verify-cas");
            let completed = progress.next_request().send().promise.await.unwrap();
            assert_eq!(
                completed
                    .get()
                    .unwrap()
                    .get_progress()
                    .unwrap()
                    .get_state()
                    .unwrap(),
                schema::AuthoringProgressState::Completed
            );
            let cancel = progress.cancel_request().send().promise.await.unwrap();
            assert!(!cancel.get().unwrap().get_cancelled());

            let mut cancellable = hub.operation_request();
            cancellable.get().set_base(3);
            cancellable
                .get()
                .reborrow()
                .init_operation()
                .set_doctor(b"cancel-me");
            let cancellable = cancellable.send().promise.await.unwrap();
            let cancellable = match cancellable
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::progress_call::Which::Success(progress) => progress.unwrap(),
                _ => panic!("expected cancellable progress capability"),
            };
            let cancel = cancellable.cancel_request().send().promise.await.unwrap();
            assert!(cancel.get().unwrap().get_cancelled());
            let cancelled = cancellable.next_request().send().promise.await.unwrap();
            assert_eq!(
                cancelled
                    .get()
                    .unwrap()
                    .get_progress()
                    .unwrap()
                    .get_state()
                    .unwrap(),
                schema::AuthoringProgressState::Cancelled
            );

            assert_eq!(backend.imports.lock().unwrap().len(), 1);
            assert_eq!(
                backend.imports.lock().unwrap()[0].settings.blobs[0].bytes().unwrap(),
                b"settings-blob"
            );
            assert_eq!(
                *backend.reimports.lock().unwrap(),
                vec![BundleUuid([70; 16])]
            );
            assert_eq!(
                *backend.operations.lock().unwrap(),
                vec![
                    LongRunningOp::Doctor(Arc::from(&b"verify-cas"[..])),
                    LongRunningOp::Doctor(Arc::from(&b"cancel-me"[..])),
                ]
            );

            let mut wire_tree = hub.wire_tree_request();
            wire_tree.get().set_layout_hash(&layout_hash.0);
            let wire_tree = wire_tree.send().promise.await.unwrap();
            match wire_tree
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::data_call::Which::Success(bytes) => {
                    assert_eq!(bytes.unwrap(), tree.as_ref())
                }
                _ => panic!("expected wire tree bytes"),
            }

            fence_pipeline(&project);
            let mut stale = hub.reimport_request();
            stale.get().set_base(u64::MAX);
            stale.get().reborrow().init_bundle().set_bytes(&[1]);
            let stale = stale.send().promise.await.unwrap();
            match stale.get().unwrap().get_result().unwrap().which().unwrap() {
                schema::uuid_call::Which::ReconnectRequired(reconnect) => assert_eq!(
                    reconnect.unwrap().get_reason().unwrap(),
                    schema::ReconnectReason::PipelineEpochChanged
                ),
                _ => panic!("generation fence must precede malformed payload decoding"),
            }

            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        })
        .await;
}

#[test]
fn pipeline_failure_capnp_decode_rejects_unknown_width_and_matrix_failures() {
    fn initialize(mut root: schema::pipeline_failure::Builder<'_>, identity: &[u8]) {
        root.set_code(schema::PipelineFailureCode::CandidateOpen);
        root.set_origin(schema::PipelineFailureOrigin::CandidateOpen);
        root.set_cleanup(schema::CleanupDisposition::None);
        root.set_identity(identity);
        root.set_message("diagnostic only");
    }

    let mut unknown = capnp::message::Builder::new_default();
    {
        let mut root = unknown.init_root::<schema::pipeline_failure::Builder<'_>>();
        initialize(root.reborrow(), &[0; 16]);
        use capnp::introspect::{Introspect, TypeVariant};
        let TypeVariant::Enum(raw_schema) = schema::PipelineFailureCode::introspect().which()
        else {
            panic!("pipeline failure code must introspect as an enum")
        };
        let enum_schema: capnp::schema::EnumSchema = raw_schema.into();
        let capnp::dynamic_value::Builder::Struct(mut dynamic) =
            capnp::dynamic_value::Builder::from(root.reborrow())
        else {
            panic!("pipeline failure must introspect as a struct")
        };
        dynamic
            .set_named(
                "code",
                capnp::dynamic_value::Enum::new(99, enum_schema).into(),
            )
            .unwrap();
    }
    assert!(distill_rpc::capnp_transport::decode_pipeline_failure(
        unknown
            .get_root_as_reader::<schema::pipeline_failure::Reader<'_>>()
            .unwrap()
    )
    .is_err());

    let mut wrong_width = capnp::message::Builder::new_default();
    initialize(
        wrong_width.init_root::<schema::pipeline_failure::Builder<'_>>(),
        &[0; 31],
    );
    assert!(distill_rpc::capnp_transport::decode_pipeline_failure(
        wrong_width
            .get_root_as_reader::<schema::pipeline_failure::Reader<'_>>()
            .unwrap()
    )
    .is_err());

    let mut wrong_matrix = capnp::message::Builder::new_default();
    {
        let mut root = wrong_matrix.init_root::<schema::pipeline_failure::Builder<'_>>();
        root.set_code(schema::PipelineFailureCode::CandidateCleanup);
        root.set_origin(schema::PipelineFailureOrigin::CandidateOpen);
        root.set_cleanup(schema::CleanupDisposition::None);
        root.set_identity(&[0; 16]);
        root.set_message("diagnostic only");
    }
    assert!(distill_rpc::capnp_transport::decode_pipeline_failure(
        wrong_matrix
            .get_root_as_reader::<schema::pipeline_failure::Reader<'_>>()
            .unwrap()
    )
    .is_err());
}

#[test]
fn configuration_error_wire_authenticates_dscp_version_detail_code_and_digest() {
    let detail = DscpV1::NonLoopbackAddress {
        address: "192.0.2.1:4000".to_owned(),
    };
    let error = ConfigurationError::from_reason(&detail, "loopback required");
    let encode = |version: u16, code: u16, bytes: Vec<u8>, digest: [u8; 16]| {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<schema::configuration_error::Builder<'_>>();
            root.set_code(code);
            root.set_reason_hash(&digest);
            root.set_message("loopback required");
            root.set_detail_version(version);
            root.set_detail_bytes(&bytes);
        }
        message
    };

    let valid = encode(
        1,
        error.code as u16,
        error.detail.canonical_detail_bytes(),
        error.reason_hash,
    );
    assert_eq!(
        distill_rpc::capnp_transport::decode_configuration_error(
            valid
                .get_root_as_reader::<schema::configuration_error::Reader<'_>>()
                .unwrap(),
        )
        .unwrap(),
        error
    );
    for invalid in [
        encode(
            2,
            error.code as u16,
            error.detail.canonical_detail_bytes(),
            error.reason_hash,
        ),
        encode(
            1,
            ConfigurationErrorCode::DuplicateRootName as u16,
            error.detail.canonical_detail_bytes(),
            error.reason_hash,
        ),
        {
            let mut bytes = error.detail.canonical_detail_bytes();
            bytes.push(0);
            encode(1, error.code as u16, bytes, error.reason_hash)
        },
        encode(
            1,
            error.code as u16,
            error.detail.canonical_detail_bytes(),
            [9; 16],
        ),
    ] {
        assert!(distill_rpc::capnp_transport::decode_configuration_error(
            invalid
                .get_root_as_reader::<schema::configuration_error::Reader<'_>>()
                .unwrap(),
        )
        .is_err());
    }
}

#[test]
fn rpc_basis_decoder_authenticates_the_adopted_snapshot_stamp() {
    let mut valid = capnp::message::Builder::new_default();
    {
        let mut root = valid.init_root::<schema::rpc_basis_value::Builder<'_>>();
        let mut stamp = root.reborrow().init_stamp();
        stamp.set_instance(&[9; 16]);
        stamp.set_version(17);
    }
    let adopted = RpcBasis {
        snapshot: SnapshotStamp {
            instance: StoreInstanceId([9; 16]),
            version: InputVersion(17),
        },
    };
    let decoded = distill_rpc::capnp_transport::decode_rpc_basis(
        valid
            .get_root_as_reader::<schema::rpc_basis_value::Reader<'_>>()
            .unwrap(),
        &adopted,
    )
    .unwrap();
    assert_eq!(decoded, adopted);

    let mut invalid = capnp::message::Builder::new_default();
    {
        let mut root = invalid.init_root::<schema::rpc_basis_value::Builder<'_>>();
        let mut stamp = root.reborrow().init_stamp();
        stamp.set_instance(&[8; 16]);
        stamp.set_version(17);
    }
    assert!(distill_rpc::capnp_transport::decode_rpc_basis(
        invalid
            .get_root_as_reader::<schema::rpc_basis_value::Reader<'_>>()
            .unwrap(),
        &adopted,
    )
    .is_err());
}

#[test]
fn namespace_error_capnp_decode_rejects_noncanonical_claimants_and_path_bytes() {
    let mut short_claimant_uuid = capnp::message::Builder::new_default();
    {
        let mut root = short_claimant_uuid.init_root::<schema::namespace_error::Builder<'_>>();
        root.set_code(NamespaceErrorCode::DuplicateAssetUuid as u16);
        root.set_identity(&[0; 16]);
        root.set_message("diagnostic only");
        let mut detail = root.init_detail().init_duplicate_asset_uuid();
        detail.reborrow().init_asset().set_bytes(&[1; 16]);
        let mut claimants = detail.init_claimants(2);
        let mut first = claimants.reborrow().get(0).init_derived();
        first.reborrow().init_parent().set_bytes(&[2; 15]);
        first.set_output_key("a");
        let mut second = claimants.reborrow().get(1).init_derived();
        second.reborrow().init_parent().set_bytes(&[3; 16]);
        second.set_output_key("b");
    }
    assert!(distill_rpc::capnp_transport::decode_namespace_error(
        short_claimant_uuid
            .get_root_as_reader::<schema::namespace_error::Reader<'_>>()
            .unwrap()
    )
    .is_err());

    let mut insufficient_claimants = capnp::message::Builder::new_default();
    {
        let mut root = insufficient_claimants.init_root::<schema::namespace_error::Builder<'_>>();
        root.set_code(NamespaceErrorCode::DuplicateAssetUuid as u16);
        root.set_identity(&[0; 16]);
        root.set_message("diagnostic only");
        let mut detail = root.init_detail().init_duplicate_asset_uuid();
        detail.reborrow().init_asset().set_bytes(&[1; 16]);
        let mut claimant = detail.init_claimants(1).get(0).init_derived();
        claimant.reborrow().init_parent().set_bytes(&[2; 16]);
        claimant.set_output_key("a");
    }
    assert!(distill_rpc::capnp_transport::decode_namespace_error(
        insufficient_claimants
            .get_root_as_reader::<schema::namespace_error::Reader<'_>>()
            .unwrap()
    )
    .is_err());

    let mut odd_windows_path = capnp::message::Builder::new_default();
    {
        let mut root = odd_windows_path.init_root::<schema::namespace_error::Builder<'_>>();
        root.set_code(NamespaceErrorCode::SameRootNormalizedPathCollision as u16);
        root.set_identity(&[0; 16]);
        root.set_message("diagnostic only");
        let mut detail = root
            .init_detail()
            .init_same_root_normalized_path_collision();
        detail.set_root_name("assets");
        detail.set_normalized_path("same/path");
        let mut claims = detail.init_claims(2);
        let mut first = claims.reborrow().get(0);
        first
            .reborrow()
            .init_raw_relative_path()
            .set_windows_utf16_le(&[0x61]);
        first.set_file_hash(&[1; 16]);
        let mut second = claims.reborrow().get(1);
        second
            .reborrow()
            .init_raw_relative_path()
            .set_unix_bytes(b"same/path");
        second.set_file_hash(&[2; 16]);
    }
    assert!(distill_rpc::capnp_transport::decode_namespace_error(
        odd_windows_path
            .get_root_as_reader::<schema::namespace_error::Reader<'_>>()
            .unwrap()
    )
    .is_err());
}

/// Records each build's work class and answers the type policy.
#[derive(Default)]
struct PackBackend {
    work_classes: Mutex<Vec<BuildWorkClass>>,
}

impl BuildBackend for PackBackend {
    fn start(&self, _view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        self.work_classes.lock().unwrap().push(request.work_class);
        BuildStart::Answered(Ok(BuildAnswer::Drifted {
            input: request.drifted_input.clone(),
        }))
    }

    fn runtime_type_policy(
        &self,
        _snapshot: &distill_store::StoreReader,
        request: &RuntimeTypePolicyRequest,
    ) -> Result<RuntimeTypePolicy, RpcFailure> {
        Ok(RuntimeTypePolicy {
            build_only: request.type_uuid == TypeUuid([2; 16]),
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn remote_snapshot_serves_the_pack_surface() {
    LocalSet::new()
        .run_until(async {
            let mut project = TestProject::configured(false);
            let server = project.server();
            let backend = Arc::new(PackBackend::default());
            server.install_build_backend(backend.clone());
            let group = |value: &str| {
                Value::Struct(Box::new([Value::String(value.to_owned())]))
            };
            let entry = project.asset("entry-5", AssetUuid([5; 16]), TAGGED_TYPE, group("group-5"));
            let definition = project
                .asset("entry-6", AssetUuid([6; 16]), TAGGED_TYPE, group("group-6"))
                .authoring_only();
            project.write_bundle(
                "bundle-5.bundle",
                BundleUuid([6; 16]),
                Some("entry-5"),
                &[entry.clone()],
            );
            project.write_bundle(
                "bundle-6.bundle",
                BundleUuid([7; 16]),
                None,
                &[definition.clone()],
            );
            project.publish();
            let request =
                ConnectRequest::new(project.target().name(), project.target().definition_hash());
            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let serving = Rc::clone(&listener);
            let server_task = tokio::task::spawn_local(async move { serving.serve().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = RemoteHub::connected(client.connect(&request).await.unwrap()).unwrap();
            let snapshot = hub.snapshot().await.unwrap().success().unwrap();

            // query and entry carry the runtime namespace.
            let selected = snapshot
                .query(&AssetQuery {
                    tag: Some(TagSelector {
                        tag: "group".into(),
                        value: Some("group-5".into()),
                    }),
                    ..AssetQuery::default()
                })
                .await
                .unwrap()
                .success()
                .unwrap();
            assert_eq!(selected, vec![entry.uuid]);
            let meta = snapshot.entry(entry.uuid).await.unwrap().success().unwrap();
            assert_eq!(meta.normalized_path, "bundle-5.bundle");
            assert_eq!(
                meta.tags,
                [("group".to_owned(), Some("group-5".to_owned()))].into()
            );
            match snapshot.entry(definition.uuid).await.unwrap() {
                RemoteCall::Error(error) => {
                    assert_eq!(
                        error.failure,
                        Some(RpcFailure::AssetNotFound {
                            uuid: definition.uuid
                        })
                    )
                }
                other => panic!("an authoring-only entry is not a runtime entry: {other:?}"),
            }

            // A batch resolve admits its build as batch work.
            snapshot
                .resolve_batch(entry.uuid)
                .await
                .unwrap()
                .success()
                .unwrap();
            assert_eq!(
                *backend.work_classes.lock().unwrap(),
                vec![BuildWorkClass::Batch]
            );

            for (type_uuid, build_only) in [(TypeUuid([1; 16]), false), (TypeUuid([2; 16]), true)] {
                let policy = snapshot
                    .runtime_type_policy(type_uuid)
                    .await
                    .unwrap()
                    .success()
                    .unwrap();
                assert_eq!(policy.build_only, build_only);
            }

            // The metadata hub inspects the authoring-only definition.
            let metadata = distill_rpc::capnp_loader::RemoteMetadataHub::connected(
                client.metadata(PROTOCOL_VERSION).await.unwrap(),
            )
            .ok()
            .expect("metadata connects");
            let authoring = metadata
                .authoring_snapshot()
                .await
                .unwrap()
                .success()
                .unwrap();
            match authoring
                .inspect(definition.uuid)
                .await
                .unwrap()
                .success()
                .unwrap()
            {
                AuthoringInspectResult::Inspection(inspection) => {
                    assert_eq!(inspection.role, AuthoringEntryRole::AuthoringOnly);
                    assert_eq!(inspection.stamp, snapshot.basis().snapshot);
                    assert_eq!(
                        &*inspection.value.canonical_value,
                        br#"{"group":"group-6"}"#
                    );
                }
                other => panic!("expected an inspection, got {other:?}"),
            }
            assert!(matches!(
                authoring
                    .inspect(AssetUuid([99; 16]))
                    .await
                    .unwrap()
                    .success(),
                Some(AuthoringInspectResult::Missing)
            ));
            // A definition file rewritten since the snapshot is drift over
            // the wire too, at the version the daemon has published.
            let mut rewritten = definition.clone();
            rewritten.value = group("group-6b");
            project.write_bundle("bundle-6.bundle", BundleUuid([7; 16]), None, &[rewritten]);
            assert_eq!(
                authoring.inspect(definition.uuid).await.unwrap().success(),
                Some(AuthoringInspectResult::Drifted {
                    input: DriftedInput::File("bundle-6.bundle".to_owned()),
                    current: snapshot.basis().snapshot,
                })
            );
            drop(client);
            server_task.abort();
        })
        .await;
}

/// Fence every connection: the daemon publishes a rejected pipeline
/// candidate, which changes the pipeline generation.
fn fence_pipeline(project: &TestProject) {
    let failure = PipelineFailure::new(
        PipelineFailureCode::CandidateRegistration,
        PipelineFailureOrigin::CandidateOpen,
        CleanupDisposition::CleanedAndClosed,
        "duplicate processor id",
    )
    .unwrap();
    let mut writer = project.coordinator().open_writer().unwrap();
    project
        .coordinator()
        .publish_pipeline_rejection(&mut writer, failure)
        .unwrap();
}

/// A generation fence the server cannot read answers the call's error arm
/// before its payload is decoded; it is no reason to reconnect.
#[tokio::test(flavor = "current_thread")]
async fn an_unreadable_generation_fence_answers_the_error_arm() {
    LocalSet::new()
        .run_until(async {
            let project = project();
            let server = project.server();
            let listener = Rc::new(
                StagedListener::bind(server.root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = listener.clone();
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.connect(&request()).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, .. } => hub,
                other => panic!("expected connected, got {other:?}"),
            };

            rusqlite::Connection::open(project.path().join(".distill/meta.sqlite"))
                .unwrap()
                .execute(
                    "INSERT INTO store_meta(key, value) VALUES ('rpc_pipeline_generation', 'x')
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    [],
                )
                .unwrap();
            let mut reimport = hub.reimport_request();
            reimport.get().set_base(u64::MAX);
            reimport.get().reborrow().init_bundle().set_bytes(&[1]);
            let reimport = reimport.send().promise.await.unwrap();
            match reimport
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::uuid_call::Which::Error(error) => {
                    let error = error.unwrap();
                    assert!(matches!(
                        error.get_failure().which().unwrap(),
                        schema::rpc_error::failure::Which::InvalidQuery(_)
                    ));
                    let message = error.get_message().unwrap().to_str().unwrap();
                    assert!(message.contains("daemon state read failed"), "{message}");
                }
                _ => panic!("an unreadable fence must answer the error arm"),
            }

            drop(client);
            server_task.abort();
        })
        .await;
}

/// The request field an `RpcError` says the daemon could not decode.
fn invalid_request(error: schema::rpc_error::Reader<'_>) -> Option<schema::InvalidRequest> {
    match error.get_failure().which().unwrap() {
        schema::rpc_error::failure::Which::InvalidRequest(kind) => Some(kind.unwrap()),
        _ => None,
    }
}

/// Reads the project's type files from disk; never writes.
struct TypeFilesBackend {
    roots: std::path::PathBuf,
}

impl AuthoringBackend for TypeFilesBackend {
    fn read_file(&self, _: &distill_store::StoreReader, root: &str, path: &str) -> Result<Vec<u8>, String> {
        std::fs::read(self.roots.join(root).join(path)).map_err(|error| error.to_string())
    }

    fn write_files(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &[AuthoringOp],
        _: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        unreachable!("never writes")
    }

    fn prepare_import(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("never imports")
    }

    fn prepare_reimport(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("never imports")
    }

    fn prepare_operation(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        unreachable!("never operates")
    }
}

/// `describeType` and `describableTypes` over TCP: the description's schema
/// and default authenticate on decode, and every default arm crosses, each
/// read from the default file at its snapshot.
#[tokio::test(flavor = "current_thread")]
async fn type_descriptions_round_trip_over_tcp() {
    use distill_schema::ngp_schema::build::{field, field_renamed, SchemaBuilder};
    use distill_schema::ngp_schema::{prefab_types, PrimitiveType};
    LocalSet::new()
        .run_until(async {
            let mut project = TestProject::with_roots(vec![target()], &[ROOT, "types"]);
            let mut b = SchemaBuilder::new();
            let u32_t = b.primitive(PrimitiveType::U32);
            let spinner = b.struct_type("game", "Spinner", vec![field_renamed("rate", u32_t, "speed")]);
            let mut schema = b.finish();
            schema.source_hashes.insert("game".into(), "0".into());
            schema.types[spinner.0].has_default = true;
            schema.types[spinner.0].attrs.component = true;
            schema.types[spinner.0].fields[0].attrs.doc = Some("Degrees per second.".into());
            for (path, contents) in prefab_types::files(&schema) {
                project.write_in("types", &path, contents);
            }
            // The daemon carries hints as written, keys it never interprets
            // (`position`) included.
            project.write_in(
                "types",
                &prefab_types::description_file("game::Spinner"),
                r#"{"members":{"$.rate":{"doc":"Degrees per second.","editor":{"unit":"deg/s","position":true}}}}"#,
            );
            let type_uuid = schema.types[spinner.0].stable_type_uuid();
            let projected = distill_schema::ngp_schema::project_values(&schema, spinner).unwrap().0;
            let schema_hash = node_hash(&projected.root).unwrap();
            let default_file = prefab_types::default_file("game::Spinner");
            let rate = |rate: u32| Value::Struct(Box::new([Value::U32(rate)]));
            project.write_in("types", &default_file, prefab_types::write_default(&projected.root, &rate(90)).unwrap());
            let stamp = project.publish();

            let backend = Arc::new(TypeFilesBackend { roots: project.path().to_owned() });
            let handle = ServerHandle::open(backend.clone(), Arc::clone(project.coordinator().server_handle().opener()));
            let server = Server::open(&handle);
            let listener = Rc::new(StagedListener::bind(server.root(), "127.0.0.1:0").await.unwrap());
            let address = listener.local_addr().unwrap();
            let serving = Rc::clone(&listener);
            let server_task = tokio::task::spawn_local(async move { serving.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = RemoteHub::connected(client.connect(&request()).await.unwrap()).unwrap();
            let snapshot = || async { hub.authoring_snapshot().await.unwrap().success().unwrap() };

            match snapshot().await.describable_types().await.unwrap().success().unwrap() {
                DescribableTypesResult::Types { stamp: at, index_hash, types } => {
                    assert_eq!(at, stamp);
                    assert!(index_hash.is_some());
                    assert_eq!(types, [DescribableType { type_uuid, path: "game::Spinner".into(), component: true }]);
                }
                other => panic!("expected the index, got {other:?}"),
            }
            let describe = || async {
                match snapshot().await.describe_type(type_uuid).await.unwrap().success().unwrap() {
                    DescribeTypeResult::Description(description) => description,
                    other => panic!("expected a description, got {other:?}"),
                }
            };
            let description = describe().await;
            assert_eq!(description.stamp, stamp);
            assert_eq!(description.schema_hash, schema_hash);
            match &description.default {
                TypeDefault::Value(value) => assert_eq!(&*value.canonical_value, br#"{"rate":90}"#),
                other => panic!("expected the default, got {other:?}"),
            }
            assert_eq!(description.members.len(), 1);
            assert_eq!(description.members[0].path, "$.rate");
            assert_eq!(description.members[0].doc.as_deref(), Some("Degrees per second."));
            assert_eq!(description.members[0].editor, r#"{"unit":"deg/s","position":true}"#, "the hints cross as their text");
            assert_eq!(description.renames, [("$.rate".to_owned(), "speed".to_owned())].into());

            // Another walk's default names its own schema hash.
            let mut other = SchemaBuilder::new();
            let u64_t = other.primitive(PrimitiveType::U64);
            let other_spinner = other.struct_type("game", "Spinner", vec![field("rate", u64_t)]);
            let other = other.finish();
            let other_root = distill_schema::ngp_schema::project_values(&other, other_spinner).unwrap().0.root;
            let other_default = Value::Struct(Box::new([Value::U64(90)]));
            project.write_in("types", &default_file, prefab_types::write_default(&other_root, &other_default).unwrap());
            project.publish();
            let written = node_hash(&other_root).unwrap();
            assert_eq!(describe().await.default, TypeDefault::SchemaMismatch { written });
            project.write_in("types", &default_file, format!(r#"{{"schema_hash":"{schema_hash}","value":{{"rate":-1}}}}"#));
            project.publish();
            assert!(matches!(describe().await.default, TypeDefault::Invalid(_)));
            project.remove_in("types", &default_file);
            project.publish();
            assert_eq!(describe().await.default, TypeDefault::Absent);
            assert!(matches!(
                snapshot().await.describe_type(TypeUuid([9; 16])).await.unwrap().success(),
                Some(DescribeTypeResult::Missing)
            ));
            drop(client);
            server_task.abort();
        })
        .await;
}

/// Answers the browsing calls (newgameplus GAP 23 §12.4) with fixed
/// listings and reads the project's files; never writes.
struct BrowsingBackend {
    roots: std::path::PathBuf,
}

impl AuthoringBackend for BrowsingBackend {
    fn read_file(&self, _: &distill_store::StoreReader, root: &str, path: &str) -> Result<Vec<u8>, String> {
        std::fs::read(self.roots.join(root).join(path)).map_err(|error| error.to_string())
    }

    fn write_files(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &[AuthoringOp],
        _: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        unreachable!("never writes")
    }

    fn prepare_import(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("never imports")
    }

    fn prepare_reimport(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("never imports")
    }

    fn prepare_operation(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        unreachable!("never operates")
    }

    fn type_name(&self, _: &distill_store::StoreReader, type_uuid: TypeUuid) -> Option<String> {
        (type_uuid == TypeUuid([1; 16])).then(|| "game::Blob".to_owned())
    }

    fn source_files(
        &self,
        _: &distill_store::StoreReader,
        root: &str,
        glob: Option<&str>,
    ) -> Result<Vec<SourceFile>, RpcFailure> {
        assert_eq!(glob, Some("**/*"));
        Ok(browsing_files(root))
    }

    fn run_release(self: Arc<Self>, _: InputVersion, bundle: BundleUuid) -> Result<ImportJob, RpcFailure> {
        Err(RpcFailure::InvalidAuthoringRequest {
            detail: format!("no rule claims {bundle}"),
        })
    }

    fn authorable(&self, _: &distill_store::StoreReader) -> Result<Authorable, RpcFailure> {
        Ok(browsing_authorable())
    }
}

/// A fixed `authorable` listing: one type with its default, one with a
/// schema mismatch, the rules type without one, one importer.
fn browsing_authorable() -> Authorable {
    use distill_schema::ngp_schema::{snapshot_to_json, LogicalSchema, PrimitiveKind};
    let schema = LogicalSchema {
        root: SchemaNode::Struct {
            rev: 0,
            fields: vec![("value".to_owned(), 0, SchemaNode::Primitive(PrimitiveKind::U8))],
        },
    };
    let schema_hash = node_hash(&schema.root).unwrap();
    let logical_schema: Arc<[u8]> = snapshot_to_json(&schema).unwrap().into_bytes().into();
    let value = |text: &str| AuthoringValue {
        canonical_value: Arc::from(text.as_bytes()),
        blobs: Vec::new(),
        types: Vec::new(),
    };
    let ty = |extension: &str, default: TypeDefault| AuthorableType {
        type_uuid: TypeUuid([extension.len() as u8; 16]),
        path: format!("game::{extension}"),
        extension: extension.to_owned(),
        local_id: "asset".into(),
        doc: format!("A {extension}."),
        authoring_only: extension == "rules",
        schema_hash,
        logical_schema: logical_schema.clone(),
        default,
    };
    Authorable {
        types: vec![
            ty("cue", TypeDefault::Value(value(r#"{"value":3}"#))),
            ty("rules", TypeDefault::Absent),
            ty("stale", TypeDefault::SchemaMismatch { written: LogicalHash([2; 16]) }),
        ],
        importers: vec![AuthorableImporter {
            id: "ngp.image".into(),
            settings_type: TypeUuid([4; 16]),
            settings_path: "game::ImageSettings".into(),
            settings_hash: schema_hash,
            settings_schema: logical_schema.clone(),
            default_settings: value(r#"{"value":0}"#),
            default_globs: vec!["**/*.png".into(), "**/*.jpg".into()],
        }],
    }
}

fn browsing_files(root: &str) -> Vec<SourceFile> {
    vec![
        SourceFile {
            root: root.to_owned(),
            path: "a.png".into(),
            claim: SourceClaim::Rule {
                rules_bundle: None,
                rule: [5; 16],
                importer: "ngp.image".into(),
                output: "a.png.bundle".into(),
                bundle: None,
            },
        },
        SourceFile {
            root: root.to_owned(),
            path: "b.glb".into(),
            claim: SourceClaim::Rule {
                rules_bundle: Some(BundleUuid([6; 16])),
                rule: [7; 16],
                importer: "ngp.gltf".into(),
                output: "b.glb.bundle".into(),
                bundle: Some(BundleUuid([8; 16])),
            },
        },
        SourceFile {
            root: root.to_owned(),
            path: "c.glb".into(),
            claim: SourceClaim::Explicit { bundle: BundleUuid([9; 16]) },
        },
        SourceFile {
            root: root.to_owned(),
            path: "notes.txt".into(),
            claim: SourceClaim::Unclaimed,
        },
    ]
}

/// `list`, `sourceFiles`, `references`, an inspection's origin and type
/// name and `Hub.release` over TCP (protocol 23), and `authorable`
/// (protocol 26): rows carry both roles with their type names and origins, every
/// claim arm crosses, an unknown bundle's references and a refused release
/// answer typed errors.
#[tokio::test(flavor = "current_thread")]
async fn browsing_calls_round_trip_over_tcp() {
    LocalSet::new()
        .run_until(async {
            let mut project = TestProject::with_roots(vec![target()], &[ROOT]);
            let runtime = authoring_entry(3, AuthoringEntryRole::Runtime);
            let control = authoring_entry(11, AuthoringEntryRole::AuthoringOnly);
            write_entry(&mut project, &runtime);
            write_entry(&mut project, &control);
            project.publish();

            let backend = Arc::new(BrowsingBackend { roots: project.path().to_owned() });
            let handle = ServerHandle::open(backend, Arc::clone(project.coordinator().server_handle().opener()));
            let server = Server::open(&handle);
            let listener = Rc::new(StagedListener::bind(server.root(), "127.0.0.1:0").await.unwrap());
            let address = listener.local_addr().unwrap();
            let serving = Rc::clone(&listener);
            let server_task = tokio::task::spawn_local(async move { serving.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = RemoteHub::connected(client.connect(&request()).await.unwrap()).unwrap();
            let snapshot = hub.authoring_snapshot().await.unwrap().success().unwrap();

            let all = AssetQuery {
                path_glob: Some("bundle-*".into()),
                ..AssetQuery::default()
            };
            let rows = snapshot.list(&all).await.unwrap().success().unwrap();
            let row = |entry: &AuthoringEntry| AuthoringRow {
                uuid: entry.uuid,
                bundle: entry.bundle,
                root: ROOT.to_owned(),
                normalized_path: entry.normalized_path.clone(),
                local_id: entry.local_id.clone(),
                type_uuid: entry.type_uuid,
                type_name: "game::Blob".into(),
                role: entry.role,
                origin: BundleOrigin::Authored,
                // A bundle's one runtime entry is its primary.
                primary: entry.role == AuthoringEntryRole::Runtime,
                // The key the daemon published is the carried value's.
                content: content_key_of(entry.schema_hash, &entry.value),
            };
            assert_eq!(rows, [row(&control), row(&runtime)]);
            let runtime_only = AssetQuery { authoring_only: Some(false), ..all.clone() };
            assert_eq!(snapshot.list(&runtime_only).await.unwrap().success().unwrap(), [row(&runtime)]);
            // An inspection names its bundle's origin and its type's name.
            match snapshot.inspect(&[control.uuid]).await.unwrap().success().unwrap().remove(0) {
                AuthoringInspectResult::Inspection(inspection) => {
                    assert_eq!(inspection.origin, BundleOrigin::Authored);
                    assert_eq!(inspection.type_name, "game::Blob");
                }
                other => panic!("expected an inspection, got {other:?}"),
            }

            assert_eq!(
                snapshot.source_files(ROOT, Some("**/*")).await.unwrap().success().unwrap(),
                browsing_files(ROOT)
            );
            assert_eq!(
                snapshot.references(runtime.bundle).await.unwrap().success().unwrap(),
                BundleReferences::default()
            );
            assert!(matches!(snapshot.references(BundleUuid([99; 16])).await.unwrap(), RemoteCall::Error(_)));
            assert_eq!(snapshot.authorable().await.unwrap().success().unwrap(), browsing_authorable());

            let base = project.server().current_stamp().unwrap().version;
            match hub.release(base, runtime.bundle).await.unwrap() {
                RemoteCall::Error(error) => assert!(error.message.contains("no rule claims"), "{error:?}"),
                other => panic!("expected a refusal, got {other:?}"),
            }
            drop(client);
            server_task.abort();
        })
        .await;
}

/// Inspections carry blob refs, never bytes, so an entry whose blob alone
/// is past capnp's 64 MiB traversal limit inspects; `blob` streams the
/// bytes; one `inspect` call reads and parses each bundle once (protocol
/// 28).
#[tokio::test(flavor = "current_thread")]
async fn inspect_batches_by_bundle_and_blobs_travel_by_reference() {
    LocalSet::new()
        .run_until(async {
            let mut project = TestProject::with_roots(vec![target()], &[ROOT]);
            let big: Vec<u8> = (0..100u32 << 20).map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8).collect();
            let (a, b) = (AssetUuid([21; 16]), AssetUuid([22; 16]));
            project.write_bundle(
                "pair.bundle",
                BundleUuid([23; 16]),
                Some("a"),
                &[
                    Asset::blob("a", a, TypeUuid([1; 16]), &big),
                    Asset::blob("b", b, TypeUuid([1; 16]), &[1, 2, 3]).authoring_only(),
                ],
            );
            let other = authoring_entry(24, AuthoringEntryRole::Runtime);
            write_entry(&mut project, &other);
            project.publish();

            let backend = Arc::new(BrowsingBackend { roots: project.path().to_owned() });
            let handle = ServerHandle::open(backend, Arc::clone(project.coordinator().server_handle().opener()));
            let server = Server::open(&handle);
            let listener = Rc::new(StagedListener::bind(server.root(), "127.0.0.1:0").await.unwrap());
            let address = listener.local_addr().unwrap();
            let serving = Rc::clone(&listener);
            let server_task = tokio::task::spawn_local(async move { serving.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = RemoteHub::connected(client.connect(&request()).await.unwrap()).unwrap();
            let snapshot = hub.authoring_snapshot().await.unwrap().success().unwrap();

            let parses = handle.authoring_bundle_parses();
            let answers = snapshot
                .inspect(&[a, b, other.uuid, AssetUuid([99; 16])])
                .await
                .unwrap()
                .success()
                .unwrap();
            assert_eq!(handle.authoring_bundle_parses() - parses, 2, "one parse per bundle");
            let blobs = |answer: &AuthoringInspectResult| match answer {
                AuthoringInspectResult::Inspection(inspection) => inspection.value.blobs.clone(),
                other => panic!("expected an inspection, got {other:?}"),
            };
            assert_eq!(blobs(&answers[0]), [AuthoringBlob::Ref(BlobRef::of(&big))]);
            assert_eq!(blobs(&answers[1]), [AuthoringBlob::Ref(BlobRef::of(&[1, 2, 3]))]);
            assert_eq!(blobs(&answers[2]), other.value.by_ref().blobs);
            assert_eq!(answers[3], AuthoringInspectResult::Missing);

            match snapshot.blob(a, 0).await.unwrap().success().unwrap() {
                RemoteAuthoringBlob::Blob { stamp, chunks } => {
                    let AuthoringInspectResult::Inspection(inspection) = &answers[0] else { unreachable!() };
                    assert_eq!(stamp, inspection.stamp);
                    assert_eq!(chunks.total_bytes(), big.len() as u64);
                    assert!(chunks.read_structural().await.unwrap() == big);
                }
                other => panic!("expected the blob, got {other:?}"),
            }
            assert!(matches!(
                snapshot.blob(a, 1).await.unwrap().success().unwrap(),
                RemoteAuthoringBlob::Missing
            ));
            assert!(matches!(
                snapshot.blob(AssetUuid([99; 16]), 0).await.unwrap().success().unwrap(),
                RemoteAuthoringBlob::Missing
            ));
            drop(client);
            server_task.abort();
        })
        .await;
}

/// Protocol 30 over the wire: `subscribeIndex` installs the connection's
/// stream, whose deltas carry source files; `listEntries` answers rows.
#[tokio::test(flavor = "current_thread")]
async fn capnp_index_subscription_carries_files_and_list_entries_answers_rows() {
    LocalSet::new()
        .run_until(async {
            let mut project = project();
            let server = project.server();
            let asset = AssetUuid([46; 16]);
            let bundle = |value: u8| [Asset::uint("entry", asset, TypeUuid([1; 16]), value)];
            project.write_bundle("assets/indexed.bundle", BundleUuid([47; 16]), Some("entry"), &bundle(1));
            let stamp = project.publish();
            let listener = Rc::new(StagedListener::bind(server.root(), "127.0.0.1:0").await.unwrap());
            let address = listener.local_addr().unwrap();
            let server_listener = Rc::clone(&listener);
            let server_task = tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = RemoteHub::connected(client.connect(&request()).await.unwrap()).unwrap();

            let mut index = match hub.subscribe_index(stamp.version).await.unwrap() {
                RemoteCall::Success(subscription) => subscription,
                other => panic!("index subscription failed: {other:?}"),
            };
            assert!(matches!(
                index.next().await.unwrap(),
                Some(StreamEvent::InitialDelta { deltas, .. }) if deltas.is_empty()
            ));
            project.write_bundle("assets/indexed.bundle", BundleUuid([47; 16]), Some("entry"), &bundle(2));
            project.write("assets/notes.txt", "notes");
            let edit = project.publish();
            let Some(StreamEvent::Delta(delta)) = index.next().await.unwrap() else {
                panic!("the index subscription is told the edit");
            };
            assert_eq!(delta.basis.snapshot, edit);
            assert_eq!(delta.assets, vec![(asset, AssetDeltaState::Changed)]);
            assert_eq!(
                delta.files,
                vec![FileChange {
                    root: ROOT.to_owned(),
                    path: "assets/notes.txt".to_owned(),
                    state: AssetDeltaState::Changed,
                }]
            );
            assert!(!delta.types);

            let authoring = match hub.authoring_snapshot().await.unwrap() {
                RemoteCall::Success(authoring) => authoring,
                other => panic!("authoring snapshot failed: {other:?}"),
            };
            let rows = match authoring.list_entries(&[asset, AssetUuid([0xEE; 16])]).await.unwrap() {
                RemoteCall::Success(rows) => rows,
                other => panic!("listEntries failed: {other:?}"),
            };
            assert_eq!(
                rows.iter().map(|row| (row.uuid, row.normalized_path.as_str())).collect::<Vec<_>>(),
                vec![(asset, "assets/indexed.bundle")]
            );

            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        })
        .await;
}
