use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distill_json::AuthoredValue;
use distill_rpc::*;
use distill_schema::ngp_schema::{
    node_hash, snapshot_to_json, LogicalSchema, PrimitiveKind, SchemaNode,
};
use distill_store::config::RestartOnlyChange;
use distill_test_project::{
    bundle_bytes, Asset, TestBuilds, TestProject, PARENT_TYPE, REFLECTION, REFLECTION_TYPE, ROOT,
    TAGGED_TYPE,
};

fn type_id(byte: u8) -> TypeUuid {
    TypeUuid([byte; 16])
}

fn asset_id(byte: u8) -> AssetUuid {
    AssetUuid([byte; 16])
}

fn content_hash(byte: u8) -> ContentHash {
    ContentHash([byte; 32])
}

#[allow(clippy::too_many_arguments)] // Test fixture mirrors the authenticated DSTL header/parts tuple.
fn canonical_artifact(
    asset: AssetUuid,
    authored_type: TypeUuid,
    encoded_type: TypeUuid,
    terminal_type: TypeUuid,
    layout_hash: LayoutHash,
    load_edges: Vec<ServedLoadEdge>,
    fixed: Vec<u8>,
    blobs: Vec<Arc<[u8]>>,
) -> (ContentHash, ArtifactPayload) {
    assert!(blobs.len() <= 1, "test helper uses one canonical blob path");
    let header = distill_wire::artifact::ArtifactHeader {
        asset_uuid: asset,
        authored_type,
        terminal_type,
        encoded_type,
        logical_hash: LogicalHash([77; 32]),
        layout_hash,
    };
    let load_deps = load_edges.iter().map(|edge| edge.asset).collect::<Vec<_>>();
    let blob_inputs = blobs
        .iter()
        .map(|blob| (Vec::new(), blob.as_ref()))
        .collect::<Vec<_>>();
    let complete =
        distill_wire::artifact::write_artifact(&header, &load_deps, &fixed, &[], &blob_inputs)
            .unwrap();
    let parsed = distill_wire::artifact::parse_artifact(&complete).unwrap();
    let structural_len = complete.len() - parsed.blob_section.len();
    let hash = distill_wire::artifact::content_hash(&complete);
    (
        hash,
        ArtifactPayload {
            structural: Arc::from(complete[..structural_len].to_vec()),
            blobs,
            load_edges,
        },
    )
}

fn target_hash(byte: u8) -> TargetDefinitionHash {
    TargetDefinitionHash([byte; 32])
}

fn target_with(definition_hash: u8, _policies: &[(u8, bool)]) -> TargetDefinition {
    TargetDefinition::new("dev", target_hash(definition_hash))
}

fn request_for(definition_hash: u8, _epoch: u64, _policies: &[(u8, bool)]) -> ConnectRequest {
    ConnectRequest::new("dev", target_hash(definition_hash))
}

/// An empty project whose daemon serves the target "dev" (definition 7).
fn project() -> TestProject {
    TestProject::new(vec![target_with(7, &[])])
}

/// `entry` (see [`authoring_entry`]) as an authored asset.
fn asset_of(entry: &AuthoringEntry) -> Asset {
    let asset = Asset::blob(
        &entry.local_id,
        entry.uuid,
        entry.type_uuid,
        &entry.value.blobs[0],
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

#[derive(Default)]
struct DepthLimitedBuildBackend {
    calls: Mutex<usize>,
}

struct LifecycleBuildBackend {
    inner: Arc<TestBuilds>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl BuildBackend for LifecycleBuildBackend {
    fn start(&self, view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        self.events.lock().unwrap().push("start");
        self.inner.start(view, request)
    }
}

impl BuildBackend for DepthLimitedBuildBackend {
    fn start(&self, _view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        *self.calls.lock().unwrap() += 1;
        BuildStart::Answered(Err(RpcFailure::BuildDepthExceeded {
            limit: 1,
            chain: vec![request.requested_asset, asset_id(99)],
        }))
    }
}

#[test]
fn dependency_depth_exhaustion_is_typed_and_never_memoized() {
    let mut project = project();
    let server = project.server();
    let backend = Arc::new(DepthLimitedBuildBackend::default());
    server.install_build_backend(backend.clone());
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    publish_entry(&mut project, &entry);
    let snapshot = snapshot(&connect(&server, &[(1, false)]));
    let expected = RpcResult::Failure(RpcFailure::BuildDepthExceeded {
        limit: 1,
        chain: vec![entry.uuid, asset_id(99)],
    });

    assert_eq!(snapshot.resolve(entry.uuid), expected);
    assert_eq!(snapshot.resolve(entry.uuid), expected);
    assert_eq!(*backend.calls.lock().unwrap(), 2);
}

#[test]
fn snapshot_clones_share_one_read_transaction() {
    let mut project = project();
    let server = project.server();
    let events = Arc::new(Mutex::new(Vec::new()));
    server.install_build_backend(Arc::new(LifecycleBuildBackend {
        inner: TestBuilds::new(&server),
        events: Arc::clone(&events),
    }));
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    publish_entry(&mut project, &entry);
    let snapshot = snapshot(&connect(&server, &[(1, false)]));
    let clone = snapshot.clone();

    assert!(matches!(
        snapshot.resolve(entry.uuid),
        RpcResult::Success(_)
    ));
    assert_eq!(&events.lock().unwrap()[..], &["start"]);
    clone.expire();
    assert_eq!(
        snapshot.version(),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    );
}

#[test]
fn snapshot_and_connection_bounds_hold_across_connections() {
    let project = project();
    let server = project.server();
    server
        .install_snapshot_policy(SnapshotPolicy {
            ttl: Duration::from_secs(60),
            max_snapshots: 1,
            max_connections: 2,
        })
        .unwrap();
    // Past the bound a connection releases its own oldest snapshot.
    let first_hub = connect(&server, &[]);
    let first_snapshot = snapshot(&first_hub);
    let second_snapshot = snapshot(&first_hub);
    assert_eq!(
        first_snapshot.version(),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    );
    assert_eq!(
        second_snapshot.version(),
        RpcResult::Success(InputVersion(0))
    );
    assert_eq!(server.open_snapshots(), 1);

    // It never reaches into another connection's: one holding none is
    // refused until a snapshot is released.
    let second_hub = connect(&server, &[]);
    assert!(matches!(
        second_hub.snapshot(),
        RpcResult::Failure(RpcFailure::ResourceLimit { limit: 1, .. })
    ));
    assert_eq!(
        second_snapshot.version(),
        RpcResult::Success(InputVersion(0))
    );
    second_snapshot.expire();
    assert!(matches!(second_hub.snapshot(), RpcResult::Success(_)));

    // A connection past the bound is refused; the open ones are untouched.
    assert!(matches!(
        server.root().connect(request_for(7, 1, &[])),
        ConnectOutcome::Refused(RpcFailure::ResourceLimit { limit: 2, .. })
    ));
    assert!(matches!(
        server.root().metadata(PROTOCOL_VERSION),
        MetadataConnectOutcome::Refused(RpcFailure::ResourceLimit { limit: 2, .. })
    ));
    assert!(matches!(first_hub.snapshot(), RpcResult::Success(_)));
    drop((first_hub, first_snapshot, second_snapshot));
    assert!(matches!(
        server.root().connect(request_for(7, 1, &[])),
        ConnectOutcome::Connected(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn a_refused_connection_leaves_a_waiting_delta_stream_alone() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut project = project();
            let server = project.server();
            server
                .install_snapshot_policy(SnapshotPolicy {
                    ttl: Duration::from_secs(60),
                    max_snapshots: 8,
                    max_connections: 1,
                })
                .unwrap();
            let entry = authoring_entry(5, AuthoringEntryRole::Runtime);
            let asset = entry.uuid;
            // The daemon serves only after its first publication.
            publish_unrelated(&mut project);
            let first_hub = connect(&server, &[]);
            let install = first_hub
                .subscribe(InputVersion(0), vec![asset], Vec::new())
                .success()
                .unwrap();
            assert!(matches!(
                install.deltas.next(),
                Some(StreamEvent::InitialDelta { .. })
            ));

            let stream = install.deltas.clone();
            let pending = tokio::task::spawn_local(async move { stream.next_async().await });
            tokio::task::yield_now().await;
            assert!(matches!(
                server.root().connect(request_for(7, 1, &[])),
                ConnectOutcome::Refused(_)
            ));
            publish_entry(&mut project, &entry);

            let event = tokio::time::timeout(Duration::from_secs(1), pending)
                .await
                .expect("the waiting delta stream missed the publication")
                .unwrap();
            assert!(matches!(
                event,
                Some(StreamEvent::Delta(Delta { ref assets, .. }))
                    if assets == &vec![(asset, AssetDeltaState::Changed)]
            ));
            assert!(matches!(first_hub.snapshot(), RpcResult::Success(_)));
        })
        .await;
}

#[test]
fn drifted_resolve_builds_per_resolve_at_the_snapshot_and_publishes_canonical_outputs() {
    let mut project = project();
    let server = project.server();
    let backend = TestBuilds::install(&server);
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let first_stamp = publish_entry(&mut project, &entry);
    let hub = connect(&server, &[(1, false)]);
    let first = snapshot(&hub);

    let first_hash = match first.resolve(entry.uuid).success().unwrap().value {
        ResolveResult::Built { content_hash } => content_hash,
        other => panic!("expected lazy build, got {other:?}"),
    };
    assert_eq!(
        first.resolve(entry.uuid).success().unwrap().value,
        ResolveResult::Built {
            content_hash: first_hash
        }
    );
    assert!(matches!(first.fetch(first_hash), RpcResult::Success(_)));
    // The server keeps no build results: every resolve of a drifted asset
    // asks the backend, at the requester's own snapshot (the daemon's
    // backend answers a repeat from its node cache).
    assert_eq!(backend.requests().len(), 2);
    assert!(backend
        .requests()
        .iter()
        .all(|(_, stamp)| *stamp == first_stamp));

    let second_stamp = publish_unrelated(&mut project);
    let second = first.refresh().success().unwrap();
    assert_eq!(second.stamp(), second_stamp);
    assert_eq!(
        second.resolve(entry.uuid).success().unwrap().value,
        ResolveResult::Built {
            content_hash: first_hash
        }
    );
    let requests = backend.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2].1, second_stamp);
    assert_eq!(requests[2].0.target, "dev");
    assert_eq!(requests[2].0.target_definition, target_hash(7));
    assert_eq!(requests[2].0.requested_asset, entry.uuid);
    assert!(requests[2].0.output_key.is_empty());
}

/// The durable store's version is what a restarted daemon serves first.
#[test]
fn derived_child_resolution_builds_the_parent_and_selects_the_declared_output() {
    let mut project = TestProject::configured(false);
    let server = project.server();
    let builds = TestBuilds::install(&server);
    let parent = asset_id(1);
    let asset = project.asset(
        "parent",
        parent,
        PARENT_TYPE,
        object("value", AuthoredValue::UInt(5)),
    );
    project.write_bundle(
        "parent.bundle",
        BundleUuid([2; 16]),
        Some("parent"),
        &[asset],
    );
    project.publish();
    let child = AssetUuid::v5(parent, REFLECTION);

    let snapshot = snapshot(&connect_to(&server, project.target()));
    let hash = match snapshot.resolve(child).success().unwrap().value {
        ResolveResult::Built { content_hash } => content_hash,
        other => panic!("expected built derived output, got {other:?}"),
    };
    assert!(matches!(snapshot.fetch(hash), RpcResult::Success(_)));
    let requests = builds.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0.entry.uuid, parent);
    assert_eq!(requests[0].0.requested_asset, child);
    assert_eq!(requests[0].0.output_key, REFLECTION);
    assert_eq!(requests[0].0.requested_terminal_type, REFLECTION_TYPE);
}

#[test]
fn production_bootstrap_starts_at_the_durable_store_version() {
    let mut project = project();
    publish_entry(
        &mut project,
        &authoring_entry(1, AuthoringEntryRole::Runtime),
    );
    let durable = publish_entry(
        &mut project,
        &authoring_entry(2, AuthoringEntryRole::Runtime),
    );
    assert_eq!(durable.version, InputVersion(2));

    let project = project.restart();
    let server = project.server();
    assert_eq!(server.current_stamp().unwrap().version, durable.version);
    assert_eq!(
        snapshot(&connect(&server, &[(1, false)])).stamp().version,
        durable.version
    );
}

#[derive(Default)]
struct RecordingAuthoringBackend {
    imports: Mutex<Vec<ImportRequest>>,
    reimports: Mutex<Vec<BundleUuid>>,
    operations: Mutex<Vec<LongRunningOp>>,
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
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &[AuthoringOp],
        _: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        unreachable!("never writes")
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

/// The daemon writes an authoring write's files and answers their receipt;
/// the version does not move until the watcher publishes them. A stale
/// base reaches no file.
#[test]
fn a_file_write_answers_its_receipt_and_publishes_nothing() {
    let mut project = project();
    let server = project.server();
    let entry = authoring_entry(7, AuthoringEntryRole::Runtime);
    let base = publish_entry(&mut project, &entry).version;
    let hub = connect(&server, &[(1, false)]);
    let file = project
        .root(distill_test_project::ROOT)
        .join(&entry.normalized_path);

    let receipt = WriteReceipt {
        files: vec![WrittenFile {
            root: distill_test_project::ROOT.to_owned(),
            path: entry.normalized_path.clone(),
            content_hash: None,
        }],
    };
    assert_eq!(
        hub.write(
            InputVersion(base.0 - 1),
            vec![AuthoringOp::Remove { uuid: entry.uuid }],
            false
        ),
        RpcResult::Failure(RpcFailure::StaleInputVersion {
            expected: base,
            got: InputVersion(base.0 - 1),
        })
    );
    assert!(file.exists());
    assert_eq!(
        hub.write(base, vec![AuthoringOp::Remove { uuid: entry.uuid }], false),
        RpcResult::Success(receipt.clone())
    );
    assert!(!file.exists());
    assert_eq!(server.current_stamp().unwrap().version, base);
    assert_eq!(WriteReceipt::decode(&receipt.encode()), Ok(receipt));
}

#[test]
fn external_coordinator_cas_runs_publication_only_at_the_exact_server_base() {
    let project = project();
    let handle = project.coordinator().server_handle();
    let mut writer = handle.opener().open_writer().unwrap();
    let mut called = false;
    assert_eq!(
        handle.coordinated_commit(&mut writer, InputVersion(9), |_| {
            called = true;
            Ok(Commit::default())
        }),
        Err(CoordinatedCommitError::Stale {
            expected: InputVersion(9),
            observed: InputVersion(0),
        })
    );
    assert!(!called);
    assert_eq!(
        handle
            .coordinated_commit(&mut writer, InputVersion(0), |_| Ok(Commit::default()))
            .unwrap()
            .version,
        InputVersion(1)
    );
}

#[test]
fn coordinator_can_project_daemon_controls_but_hub_cannot_write_them_directly() {
    let mut project = project();
    let server = project.server();
    let mut control = authoring_entry(44, AuthoringEntryRole::AuthoringOnly);
    control.local_id = "$settings".to_owned();
    let stamp = publish_entry(&mut project, &control);
    let hub = connect(&server, &[(1, false)]);
    assert!(matches!(
        authoring_snapshot(&hub).inspect(control.uuid),
        RpcResult::Success(AuthoringInspectResult::Inspection(_))
    ));
    assert!(matches!(
        hub.write(stamp.version, vec![AuthoringOp::Set(control)], false),
        RpcResult::Failure(RpcFailure::InvalidAuthoringRequest { .. })
    ));
    assert_eq!(server.current_stamp().unwrap(), stamp);
}

fn connect(server: &Server, policies: &[(u8, bool)]) -> Hub {
    match server.root().connect(request_for(7, 1, policies)) {
        ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("expected connection, got {other:?}"),
    }
}

/// A hub bound to `target`, as served by a configured project.
fn connect_to(server: &Server, target: &TargetDefinition) -> Hub {
    match server
        .root()
        .connect(ConnectRequest::new(target.name(), target.definition_hash()))
    {
        ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("expected connection, got {other:?}"),
    }
}

/// The authored value of a configured project type of one field.
fn object(field: &str, value: AuthoredValue) -> AuthoredValue {
    AuthoredValue::Object(BTreeMap::from([(field.to_owned(), value)]))
}

fn snapshot(hub: &Hub) -> Snapshot {
    match hub.snapshot() {
        RpcResult::Success(snapshot) => snapshot,
        other => panic!("expected snapshot, got {other:?}"),
    }
}

/// Offer `commit` to the server as the next version: what its commit
/// validation makes of it. A rejected commit publishes nothing.
fn offer(server: &Server, commit: Commit) -> Result<SnapshotStamp, AdminError> {
    let before = server.current_stamp().unwrap();
    let offered = coordinated(server, |handle, store| {
        handle.coordinated_commit(store, before.version, |_| Ok(commit))
    });
    match offered {
        Ok(stamp) => Ok(stamp),
        Err(CoordinatedCommitError::Invalid(error)) => {
            assert_eq!(server.current_stamp().unwrap(), before);
            Err(error)
        }
        Err(other) => panic!("expected a validated commit, got {other:?}"),
    }
}

/// The daemon publishes `failure` as a rejected pipeline candidate.
fn reject_pipeline(project: &TestProject, failure: PipelineFailure) -> SnapshotStamp {
    let mut writer = project.coordinator().open_writer().unwrap();
    project
        .coordinator()
        .publish_pipeline_rejection(&mut writer, failure)
        .unwrap()
}

/// The daemon rejects its configuration source: no version is published,
/// the current one is served under the failure (its stamp is returned).
fn reject_configuration(project: &TestProject, reason: DscpV1, message: &str) -> SnapshotStamp {
    project
        .coordinator()
        .reject_configuration(reason, message)
        .unwrap();
    project.server().current_stamp().unwrap()
}

/// A second RPC server over `project`'s store, with `backend` as its
/// authoring backend.
fn server_over(project: &TestProject, backend: Arc<dyn AuthoringBackend>) -> Server {
    let handle = ServerHandle::open(
        backend,
        Arc::clone(project.coordinator().server_handle().opener()),
    );
    Server::open(&handle)
}

/// Hand the files an authoring write wrote to the watcher: the next
/// [`TestProject::publish`] publishes them.
fn touch_written(project: &mut TestProject, receipt: &WriteReceipt) {
    for file in &receipt.files {
        let bytes = std::fs::read(project.root(&file.root).join(&file.path)).unwrap();
        project.write_in(&file.root, &file.path, bytes);
    }
}

/// A build backend answering every build with the artifact set for the
/// requester's input version.
#[derive(Default)]
struct BuildsByVersion {
    built: Mutex<BTreeMap<InputVersion, ContentHash>>,
}

impl BuildsByVersion {
    fn install(server: &Server) -> Arc<Self> {
        let builds = Arc::new(Self::default());
        server.install_build_backend(builds.clone());
        builds
    }

    fn answer(&self, at: SnapshotStamp, content_hash: ContentHash) {
        self.built.lock().unwrap().insert(at.version, content_hash);
    }
}

impl BuildBackend for BuildsByVersion {
    fn start(&self, view: BuildView<'_>, _request: &BuildRequest) -> BuildStart {
        let content_hash = *self
            .built
            .lock()
            .unwrap()
            .get(&view.stamp.version)
            .expect("an artifact for the requester's version");
        BuildStart::Answered(Ok(BuildAnswer::Built { content_hash }))
    }
}

fn assert_reconnect<T: std::fmt::Debug>(result: RpcResult<T>, reason: ReconnectReason) {
    assert!(
        matches!(
            result,
            RpcResult::ReconnectRequired { reason: actual } if actual == reason
        ),
        "{result:?}"
    );
}

/// The authoring entry the daemon serves for the asset `byte`: one blob
/// (`byte + 2`) in its own bundle file `bundle-{byte}.bundle`, the bundle's
/// primary. The daemon tags nothing without a project schema.
fn authoring_entry(byte: u8, role: AuthoringEntryRole) -> AuthoringEntry {
    let schema_hash = node_hash(&SchemaNode::Blob).unwrap();
    AuthoringEntry {
        uuid: asset_id(byte),
        bundle: BundleUuid([byte.wrapping_add(1); 16]),
        local_id: format!("entry-{byte}"),
        normalized_path: format!("bundle-{byte}.bundle"),
        type_uuid: type_id(byte),
        terminal_type: type_id(byte),
        schema_hash,
        logical_schema: Arc::from(&b"\"blob\""[..]),
        role,
        tags: BTreeMap::new(),
        value: AuthoringValue {
            canonical_value: Arc::from(&b"{\"$distill_blob\":0}"[..]),
            blobs: vec![Arc::from([byte.wrapping_add(2)])],
        },
    }
}

fn authoring_snapshot(hub: &Hub) -> AuthoringSnapshot {
    match hub.authoring_snapshot() {
        RpcResult::Success(snapshot) => snapshot,
        other => panic!("expected authoring snapshot, got {other:?}"),
    }
}

#[test]
fn authoring_payload_decoder_materializes_authenticated_blob_bytes() {
    let entry = authoring_entry(9, AuthoringEntryRole::AuthoringOnly);
    assert_eq!(
        decode_authoring_payload(entry.schema_hash, &entry.logical_schema, &entry.value).unwrap(),
        AuthoredValue::Blob(vec![11])
    );
}

#[test]
fn authoring_payload_decoder_reenters_an_enum_from_its_variant_payload() {
    // { End: {}, Link: { blob: Blob, next: BackRef(0) } }: a variant
    // payload opens no frame of its own (§5), so `next` is the enum again.
    let schema = LogicalSchema {
        root: SchemaNode::Enum {
            rev: 0,
            variants: vec![
                (
                    "End".to_owned(),
                    0,
                    SchemaNode::Struct {
                        rev: 0,
                        fields: Vec::new(),
                    },
                ),
                (
                    "Link".to_owned(),
                    0,
                    SchemaNode::Struct {
                        rev: 0,
                        fields: vec![
                            ("blob".to_owned(), 0, SchemaNode::Blob),
                            ("next".to_owned(), 0, SchemaNode::BackRef(0)),
                        ],
                    },
                ),
            ],
        },
    };
    let value = AuthoringValue {
        canonical_value: Arc::from(
            &br#"{"Link":{"blob":{"$distill_blob":0},"next":{"Link":{"blob":{"$distill_blob":1},"next":{"End":{}}}}}}"#[..],
        ),
        blobs: vec![Arc::from([1u8]), Arc::from([2u8])],
    };
    let link = |byte: u8, next: AuthoredValue| {
        AuthoredValue::Object(BTreeMap::from([(
            "Link".to_owned(),
            AuthoredValue::Object(BTreeMap::from([
                ("blob".to_owned(), AuthoredValue::Blob(vec![byte])),
                ("next".to_owned(), next),
            ])),
        )]))
    };
    let end = AuthoredValue::Object(BTreeMap::from([(
        "End".to_owned(),
        AuthoredValue::Object(BTreeMap::new()),
    )]));
    assert_eq!(
        decode_authoring_payload(
            node_hash(&schema.root).unwrap(),
            snapshot_to_json(&schema).unwrap().as_bytes(),
            &value,
        )
        .unwrap(),
        link(1, link(2, end))
    );
}

fn option_of(inner: SchemaNode) -> SchemaNode {
    SchemaNode::Option(Box::new(inner))
}

fn record(fields: Vec<(&str, SchemaNode)>) -> SchemaNode {
    SchemaNode::Struct {
        rev: 0,
        fields: fields
            .into_iter()
            .map(|(name, node)| (name.to_owned(), 0, node))
            .collect(),
    }
}

fn decode_plain(root: SchemaNode, canonical: &str) -> Result<AuthoredValue, AuthoringValueError> {
    let schema = LogicalSchema { root };
    decode_authoring_payload(
        node_hash(&schema.root).unwrap(),
        snapshot_to_json(&schema).unwrap().as_bytes(),
        &AuthoringValue {
            canonical_value: Arc::from(canonical.as_bytes()),
            blobs: Vec::new(),
        },
    )
}

#[test]
fn authoring_payload_decoder_reenters_a_backref_under_its_own_ancestors() {
    // A { b: Option<B> }, B { a: Option<A>, b: Option<B> }. B.b re-enters
    // B as BackRef(0); inside that B, `a` is BackRef(1) and names A.
    let schema = || {
        record(vec![(
            "b",
            option_of(record(vec![
                ("a", option_of(SchemaNode::BackRef(1))),
                ("b", option_of(SchemaNode::BackRef(0))),
            ])),
        )])
    };
    let text = r#"{"b":{"a":null,"b":{"a":{"b":null},"b":null}}}"#;
    assert_eq!(
        decode_plain(schema(), text).unwrap(),
        distill_json::parse(text).unwrap()
    );
    // A B where `a` names A is refused.
    decode_plain(
        schema(),
        r#"{"b":{"a":null,"b":{"a":{"a":null,"b":null},"b":null}}}"#,
    )
    .unwrap_err();
}

#[test]
fn authoring_payload_decoder_reenters_a_backref_under_its_own_ancestors_three_deep() {
    // A { b: Option<B> }, B { c: Option<C> },
    // C { a: Option<A>, b: Option<B>, c: Option<C> }.
    let schema = || {
        record(vec![(
            "b",
            option_of(record(vec![(
                "c",
                option_of(record(vec![
                    ("a", option_of(SchemaNode::BackRef(2))),
                    ("b", option_of(SchemaNode::BackRef(1))),
                    ("c", option_of(SchemaNode::BackRef(0))),
                ])),
            )])),
        )])
    };
    let text = r#"{"b":{"c":{"a":null,"b":null,"c":{"a":null,"b":{"c":{"a":{"b":{"c":null}},"b":null,"c":{"a":null,"b":null,"c":null}}},"c":null}}}}"#;
    assert_eq!(
        decode_plain(schema(), text).unwrap(),
        distill_json::parse(text).unwrap()
    );
    // Inside the re-entered C, `b` names B: a C there is refused.
    decode_plain(
        schema(),
        r#"{"b":{"c":{"a":null,"b":null,"c":{"a":null,"b":{"a":null,"b":null,"c":null},"c":null}}}}"#,
    )
    .unwrap_err();
}

#[test]
fn tag_queries_fail_when_other_selectors_could_include_a_poisoned_entry() {
    // Without a project schema the daemon's tag index stays pending: the
    // entry's bundle is poisoned.
    let mut project = project();
    let server = project.server();
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    publish_entry(&mut project, &entry);
    let hub = connect(&server, &[(1, false), (2, false)]);
    let pinned = snapshot(&hub);
    assert_eq!(
        pinned.query(AssetQuery {
            tag: Some(TagSelector {
                tag: "group".to_owned(),
                value: Some("group-1".to_owned()),
            }),
            ..AssetQuery::default()
        }),
        RpcResult::Failure(RpcFailure::TagIndexPoisoned {
            bundles: vec![entry.bundle],
        })
    );
    assert_eq!(
        pinned.query(AssetQuery {
            authored_type: Some(type_id(2)),
            tag: Some(TagSelector {
                tag: "group".to_owned(),
                value: None,
            }),
            ..AssetQuery::default()
        }),
        RpcResult::Success(Vec::new())
    );
    assert_eq!(
        pinned.query(AssetQuery {
            authored_type: Some(type_id(1)),
            ..AssetQuery::default()
        }),
        RpcResult::Success(vec![entry.uuid])
    );
}

#[test]
fn authoring_commit_authenticates_schema_and_exact_blob_index_coverage() {
    let mut project = project();
    let server = project.server();
    let valid = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    publish_entry(&mut project, &valid);
    // The daemon serves the valid entry as authored.
    assert!(matches!(
        authoring_snapshot(&connect(&server, &[])).inspect(valid.uuid),
        RpcResult::Success(AuthoringInspectResult::Inspection(inspection))
            if inspection.schema_hash == valid.schema_hash
                && inspection.logical_schema == valid.logical_schema
                && inspection.value == valid.value
    ));

    let mut malformed = valid.clone();
    malformed.value.canonical_value = Arc::from(&b"0"[..]);
    assert!(matches!(
        offer(
            &server,
            Commit {
                authoring: vec![AuthoringMutation::Set(malformed)],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidAuthoringValue {
            error: AuthoringValueError::SchemaValueShape { .. },
            ..
        })
    ));

    let mut duplicate = valid.clone();
    duplicate.logical_schema = Arc::from(&b"{\"array\":{\"elem\":\"blob\",\"len\":2}}"[..]);
    duplicate.schema_hash = node_hash(&SchemaNode::Array {
        len: 2,
        elem: Box::new(SchemaNode::Blob),
    })
    .unwrap();
    duplicate.value.canonical_value =
        Arc::from(&b"[{\"$distill_blob\":0},{\"$distill_blob\":0}]"[..]);
    assert!(matches!(
        offer(
            &server,
            Commit {
                authoring: vec![AuthoringMutation::Set(duplicate)],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidAuthoringValue {
            error: AuthoringValueError::DuplicateBlobIndex { index: 0 },
            ..
        })
    ));

    let mut tampered_schema = valid;
    tampered_schema.schema_hash = LogicalHash([0; 32]);
    assert!(matches!(
        offer(
            &server,
            Commit {
                authoring: vec![AuthoringMutation::Set(tampered_schema)],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidAuthoringValue {
            error: AuthoringValueError::LogicalSchemaInvalid(_),
            ..
        })
    ));
}

#[test]
fn authoring_schema_walk_type_checks_primitive_and_reference_leaves() {
    let project = project();
    let server = project.server();
    let mut entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let primitive = SchemaNode::Primitive(PrimitiveKind::U8);
    entry.schema_hash = node_hash(&primitive).unwrap();
    entry.logical_schema = Arc::from(
        snapshot_to_json(&LogicalSchema { root: primitive })
            .unwrap()
            .into_bytes(),
    );
    entry.value.canonical_value = Arc::from(&b"{\"$distill_blob\":0}"[..]);
    entry.value.blobs.clear();
    assert!(matches!(
        offer(
            &server,
            Commit {
                authoring: vec![AuthoringMutation::Set(entry.clone())],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidAuthoringValue {
            error: AuthoringValueError::SchemaValueShape { .. },
            ..
        })
    ));

    let reference = SchemaNode::AssetRef(type_id(1));
    entry.schema_hash = node_hash(&reference).unwrap();
    entry.logical_schema = Arc::from(
        snapshot_to_json(&LogicalSchema { root: reference })
            .unwrap()
            .into_bytes(),
    );
    entry.value.canonical_value = Arc::from(&b"{\"$distill_blob\":0}"[..]);
    assert!(matches!(
        offer(
            &server,
            Commit {
                authoring: vec![AuthoringMutation::Set(entry)],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidAuthoringValue {
            error: AuthoringValueError::SchemaValueShape { .. },
            ..
        })
    ));
}

#[test]
fn asset_reference_query_uses_uuid_precedence_and_rejects_noncanonical_selectors() {
    let canonical = "abababab-abab-abab-abab-abababababab";
    assert_eq!(
        decode_asset_reference_query(&AuthoredValue::Str(canonical.to_owned())).unwrap(),
        AssetReferenceQuery::Uuid(AssetUuid([0xab; 16]))
    );
    assert!(decode_asset_reference_query(&AuthoredValue::Str(canonical.to_uppercase())).is_err());
    assert!(decode_asset_reference_query(&AuthoredValue::Str("a/../b".to_owned())).is_err());
    assert!(
        decode_asset_reference_query(&AuthoredValue::Str("e\u{301}.asset".to_owned())).is_err()
    );

    let selector = |path: Option<&str>, asset: Option<&str>| {
        let mut fields = BTreeMap::new();
        if let Some(path) = path {
            fields.insert("path".to_owned(), AuthoredValue::Str(path.to_owned()));
        }
        if let Some(asset) = asset {
            fields.insert("asset".to_owned(), AuthoredValue::Str(asset.to_owned()));
        }
        AuthoredValue::Object(fields)
    };
    assert_eq!(
        decode_asset_reference_query(&selector(Some("folder/a.asset"), Some("mesh"))).unwrap(),
        AssetReferenceQuery::Path {
            normalized_path: "folder/a.asset".to_owned(),
            local_id: Some("mesh".to_owned()),
        }
    );
    assert!(decode_asset_reference_query(&selector(Some("a/./b"), None)).is_err());
    assert!(decode_asset_reference_query(&selector(None, Some("$record"))).is_err());
    assert!(decode_asset_reference_query(&selector(None, Some(&"x".repeat(256)))).is_err());
}

/// Whether `errors` is the one namespace error the daemon reports for the
/// malformed bundle file `path` under the root [`distill_test_project::ROOT`].
fn names_malformed_file(errors: &[NamespaceError], path: &str) -> bool {
    matches!(
        errors,
        [NamespaceError {
            detail: NamespaceErrorV1::IncompleteSkeleton { source, .. },
            ..
        }] if source.root_name == distill_test_project::ROOT && source.normalized_path == path
    )
}

fn test_pipeline_failure() -> PipelineFailure {
    PipelineFailure::new(
        PipelineFailureCode::CandidateRegistration,
        PipelineFailureOrigin::CandidateOpen,
        CleanupDisposition::CleanedAndClosed,
        "duplicate processor id",
    )
    .unwrap()
}

#[test]
fn metadata_namespace_calls_serve_around_a_namespace_error() {
    let mut project = project();
    let server = project.server();
    // A runtime entry: a bundle's primary, which its path resolves to, is
    // never authoring-only.
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    write_entry(&mut project, &entry);
    project.write("broken.bundle", b"not a bundle");
    project.publish();
    let failure = test_pipeline_failure();
    let stamp = reject_pipeline(&project, failure.clone());
    let connected = server
        .root()
        .metadata(PROTOCOL_VERSION)
        .connected()
        .unwrap();
    let diagnostics = connected.hub.diagnostics().success().unwrap();
    assert!(
        names_malformed_file(&diagnostics.namespace_errors, "broken.bundle"),
        "{:?}",
        diagnostics.namespace_errors
    );
    assert_eq!(diagnostics.pipeline, PipelineDiagnostic::Failed(failure));
    let snapshot = connected.hub.snapshot().success().unwrap();
    assert_eq!(snapshot.version(), MetadataCall::Success(stamp.version));
    // The error is about one file: the rest of the namespace serves.
    let query = PureMetadataQuery {
        bundle: Some(entry.bundle),
        authored_type: Some(entry.type_uuid),
        role: Some(AuthoringEntryRole::Runtime),
        normalized_path_prefix: Some("bundle-1".to_owned()),
        ..PureMetadataQuery::default()
    };
    assert_eq!(
        snapshot.query(&query),
        MetadataNamespaceCall::Success(vec![entry.uuid])
    );
    assert_eq!(
        snapshot
            .entry(entry.uuid)
            .success()
            .unwrap()
            .normalized_path,
        entry.normalized_path
    );
    assert_eq!(
        snapshot.resolve_path(&entry.normalized_path),
        MetadataNamespaceCall::Success(PathResolveResult::Resolved(entry.uuid))
    );
    let authoring = connected.hub.authoring_snapshot().success().unwrap();
    assert!(authoring.inspect(entry.uuid).success().is_some());

    project.remove("broken.bundle");
    project.publish();
    assert!(connected
        .hub
        .diagnostics()
        .success()
        .unwrap()
        .namespace_errors
        .is_empty());
}

#[test]
fn target_snapshot_namespace_calls_serve_around_a_namespace_error() {
    let mut project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    project.write("broken.bundle", b"not a bundle");
    project.publish();
    let metadata = server
        .root()
        .metadata(PROTOCOL_VERSION)
        .connected()
        .unwrap();
    assert!(names_malformed_file(
        &metadata
            .hub
            .diagnostics()
            .success()
            .unwrap()
            .namespace_errors,
        "broken.bundle"
    ));
    let snapshot = snapshot(&hub);
    let authoring = authoring_snapshot(&hub);
    assert!(snapshot
        .query(AssetQuery {
            uuid: Some(asset_id(1)),
            ..AssetQuery::default()
        })
        .success()
        .is_some());
    assert_eq!(
        snapshot.entry(asset_id(1)),
        RpcResult::Failure(RpcFailure::AssetNotFound { uuid: asset_id(1) })
    );
    assert!(snapshot.resolve_path("a.asset").success().is_some());
    assert!(authoring
        .query(AssetQuery {
            uuid: Some(asset_id(1)),
            ..AssetQuery::default()
        })
        .success()
        .is_some());
}

#[test]
fn unbound_metadata_bootstrap_survives_a_configuration_error_and_has_no_runtime_surface() {
    let mut project = project();
    let server = project.server();
    let entry = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    let (hash, payload) = canonical_artifact(
        entry.uuid,
        type_id(1),
        type_id(1),
        type_id(1),
        LayoutHash([4; 32]),
        Vec::new(),
        vec![1, 2, 3],
        Vec::new(),
    );
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &payload),
        hash
    );
    let reason = DscpV1::MalformedConfiguration { file_hash: [4; 32] };
    let error = ConfigurationError::from_reason(&reason, "invalid staged configuration");
    publish_entry(&mut project, &entry);
    let stamp = reject_configuration(&project, reason, "invalid staged configuration");

    let connected = match server.root().metadata(PROTOCOL_VERSION) {
        MetadataConnectOutcome::Connected(connected) => connected,
        other => panic!("expected metadata bootstrap, got {other:?}"),
    };
    assert_eq!(connected.instance, stamp.instance);
    let snapshot = connected.hub.snapshot().success().unwrap();
    assert_eq!(snapshot.version(), MetadataCall::Success(stamp.version));
    assert_eq!(
        snapshot.diagnostics().success().unwrap().configuration,
        ConfigurationStatus::Failed(error.clone())
    );
    let authoring = connected.hub.authoring_snapshot().success().unwrap();
    assert!(matches!(
        authoring.inspect(entry.uuid),
        MetadataNamespaceCall::Success(AuthoringInspectResult::Inspection(_))
    ));
    assert!(matches!(
        connected.hub.fetch(hash),
        MetadataCall::Success(_)
    ));
}

#[test]
fn authoring_snapshot_pins_role_inclusive_metadata_without_runtime_escape() {
    let mut project = TestProject::configured(false);
    let server = project.server();
    // Each asset is its own bundle file `bundle-{byte}.bundle`.
    let write = |project: &mut TestProject, byte: u8, asset: Asset| {
        let primary = (!asset.authoring_only).then(|| asset.local_id.clone());
        project.write_bundle(
            &format!("bundle-{byte}.bundle"),
            BundleUuid([byte.wrapping_add(1); 16]),
            primary.as_deref(),
            &[asset],
        );
    };
    let tagged = |project: &TestProject, byte: u8, group: &str| {
        project.asset(
            &format!("entry-{byte}"),
            asset_id(byte),
            TAGGED_TYPE,
            object("group", AuthoredValue::Str(group.to_owned())),
        )
    };
    // The asset 99 the namespace deleted is missing, as if never published.
    let removed = tagged(&project, 99, "group-99");
    write(&mut project, 99, removed);
    project.publish();
    project.remove("bundle-99.bundle");
    let runtime = tagged(&project, 1, "group-1");
    write(&mut project, 1, runtime);
    let tooling = tagged(&project, 2, "group-2").authoring_only();
    write(&mut project, 2, tooling);
    // The parent's declared output is a runtime asset with no authoring
    // entry of its own.
    let parent = project.asset(
        "entry-3",
        asset_id(3),
        PARENT_TYPE,
        object("value", AuthoredValue::UInt(3)),
    );
    write(&mut project, 3, parent);
    let derived = AssetUuid::v5(asset_id(3), REFLECTION);
    let stamp = project.publish();
    let hub = connect_to(&server, project.target());
    let pinned = authoring_snapshot(&hub);

    assert_eq!(pinned.stamp(), stamp);
    assert_eq!(pinned.basis().snapshot, stamp);
    assert_eq!(pinned.version(), RpcResult::Success(stamp.version));
    assert_eq!(
        pinned.query(AssetQuery {
            authoring_only: Some(false),
            ..AssetQuery::default()
        }),
        RpcResult::Success(vec![asset_id(1), asset_id(3)])
    );
    assert_eq!(
        pinned.query(AssetQuery {
            authoring_only: Some(true),
            ..AssetQuery::default()
        }),
        RpcResult::Success(vec![asset_id(2)])
    );
    // Every selector at once, the tag value among them.
    assert_eq!(
        pinned.query(AssetQuery {
            uuid: Some(asset_id(2)),
            bundle_path: Some("bundle-2.bundle".to_owned()),
            local_id: Some("entry-2".to_owned()),
            bundle_uuid: Some(BundleUuid([3; 16])),
            authored_type: Some(TAGGED_TYPE),
            terminal_type: Some(TAGGED_TYPE),
            tag: Some(TagSelector {
                tag: "group".to_owned(),
                value: Some("group-2".to_owned()),
            }),
            path_prefix: Some("bundle-".to_owned()),
            path_glob: Some("bundle-?.bundle".to_owned()),
            authoring_only: Some(true),
        }),
        RpcResult::Success(vec![asset_id(2)])
    );
    assert_eq!(
        pinned.query(AssetQuery {
            tag: Some(TagSelector {
                tag: "group".to_owned(),
                value: Some("group-1".to_owned()),
            }),
            ..AssetQuery::default()
        }),
        RpcResult::Success(vec![asset_id(1)])
    );
    match pinned.inspect(asset_id(2)) {
        RpcResult::Success(AuthoringInspectResult::Inspection(inspection)) => {
            assert_eq!(inspection.stamp, stamp);
            assert_eq!(inspection.uuid, asset_id(2));
            assert_eq!(inspection.role, AuthoringEntryRole::AuthoringOnly);
            assert_eq!(
                distill_json::parse(
                    std::str::from_utf8(&inspection.value.canonical_value).unwrap()
                )
                .unwrap(),
                object("group", AuthoredValue::Str("group-2".to_owned()))
            );
        }
        other => panic!("expected pinned inspection, got {other:?}"),
    }
    assert_eq!(
        pinned.inspect(derived),
        RpcResult::Success(AuthoringInspectResult::RoleIneligible {
            observed: AuthoringEntryRole::Runtime,
        })
    );
    assert_eq!(
        pinned.inspect(asset_id(99)),
        RpcResult::Success(AuthoringInspectResult::Missing)
    );
    assert_eq!(
        pinned.inspect(asset_id(98)),
        RpcResult::Success(AuthoringInspectResult::Missing)
    );

    let runtime_snapshot = snapshot(&hub);
    assert!(matches!(
        runtime_snapshot.resolve(asset_id(2)),
        RpcResult::Success(TerminalEvent {
            value: ResolveResult::RoleIneligible {
                observed: AuthoringEntryRole::AuthoringOnly
            },
            ..
        })
    ));
}

#[test]
fn authoring_snapshot_refreshes_to_a_successor_stamp_without_tearing() {
    let mut project = project();
    let server = project.server();
    let first_entry = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    let first_stamp = publish_entry(&mut project, &first_entry);
    let hub = connect(&server, &[(1, false)]);
    let first = authoring_snapshot(&hub);

    assert!(
        matches!(first.inspect(first_entry.uuid).success().unwrap(), AuthoringInspectResult::Inspection(value) if value.stamp == first_stamp && value.value == first_entry.value)
    );

    // The value is the bundle file's, verified against the hash the
    // snapshot published: a file rewritten since is drift, at the current
    // version until the daemon publishes the edit.
    let mut replacement = first_entry.clone();
    replacement.value.blobs = vec![Arc::from([0xAA])];
    assert_ne!(replacement.value, first_entry.value);
    write_entry(&mut project, &replacement);
    let drifted = AuthoringInspectResult::Drifted {
        input: DriftedInput::File(first_entry.normalized_path.clone()),
        current: first_stamp,
    };
    assert_eq!(first.inspect(first_entry.uuid).success().unwrap(), drifted);
    let second_stamp = project.publish();

    let old = first.inspect(first_entry.uuid).success().unwrap();
    let refreshed = first.refresh().success().unwrap();
    let new = refreshed.inspect(first_entry.uuid).success().unwrap();
    assert_eq!(first.stamp(), first_stamp);
    assert_eq!(refreshed.stamp(), second_stamp);
    assert_eq!(first.basis().snapshot, first_stamp);
    assert_eq!(refreshed.basis().snapshot, second_stamp);
    assert_eq!(
        old,
        AuthoringInspectResult::Drifted {
            input: DriftedInput::File(first_entry.normalized_path.clone()),
            current: second_stamp,
        }
    );
    assert!(
        matches!(new, AuthoringInspectResult::Inspection(value) if value.stamp == second_stamp && value.value == replacement.value)
    );
}

#[test]
fn every_authoring_snapshot_method_is_expiry_and_generation_fenced() {
    let mut project = TestProject::configured(false);
    let server = project.server();
    let old = project.target().clone();
    let hub = connect_to(&server, &old);
    let expired = authoring_snapshot(&hub);
    expired.expire();
    assert_eq!(
        expired.version(),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    );
    assert_eq!(
        expired.query(AssetQuery::default()),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    );
    assert_eq!(
        expired.inspect(asset_id(1)),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    );
    assert!(matches!(
        expired.refresh(),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    ));

    let stale = authoring_snapshot(&hub);
    // A configuration that redefines the target publishes with its
    // pipeline fence: there is no target-specific reason.
    project.reconfigure(true);
    assert_ne!(project.target().definition_hash(), old.definition_hash());
    let reason = ReconnectReason::PipelineEpochChanged;
    assert_reconnect(hub.authoring_snapshot(), reason);
    assert_reconnect(stale.version(), reason);
    assert_reconnect(stale.query(AssetQuery::default()), reason);
    assert_reconnect(stale.inspect(asset_id(1)), reason);
    assert_reconnect(stale.refresh(), reason);
}

#[test]
fn an_unreadable_generation_fence_is_an_error_not_a_reconnect() {
    let project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let pinned = snapshot(&hub);
    let authoring = authoring_snapshot(&hub);
    // Break the fence row underneath the server: reading it fails.
    rusqlite::Connection::open(project.path().join(".distill/meta.sqlite"))
        .unwrap()
        .execute(
            "INSERT INTO store_meta(key, value) VALUES ('rpc_pipeline_generation', 'x')
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [],
        )
        .unwrap();
    let read_failure = |result: &dyn std::fmt::Debug| {
        let result = format!("{result:?}");
        assert!(
            result.starts_with("Failure(InvalidQuery")
                && result.contains("daemon state read failed"),
            "{result}"
        );
    };
    read_failure(&hub.snapshot());
    read_failure(&hub.authoring_snapshot());
    read_failure(&pinned.version());
    read_failure(&pinned.refresh());
    read_failure(&authoring.version());
    read_failure(&authoring.refresh());
}

#[test]
fn authoring_inspection_is_a_pinned_pure_metadata_read_under_configuration_error() {
    let mut project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    publish_entry(&mut project, &entry);
    let failed_stamp = reject_configuration(
        &project,
        DscpV1::MalformedConfiguration { file_hash: [9; 32] },
        "invalid staged configuration",
    );

    let pinned = authoring_snapshot(&hub);
    assert_eq!(pinned.stamp(), failed_stamp);
    assert_eq!(
        pinned.query(AssetQuery {
            uuid: Some(entry.uuid),
            authoring_only: Some(true),
            ..AssetQuery::default()
        }),
        RpcResult::Success(vec![entry.uuid])
    );
    assert!(matches!(
        pinned.inspect(entry.uuid),
        RpcResult::Success(AuthoringInspectResult::Inspection(value))
            if value.stamp == failed_stamp
    ));
}

#[test]
fn staging_accepts_only_numeric_loopback_addresses() {
    assert!(validate_bind_address("127.0.0.1:9999").is_ok());
    assert!(validate_bind_address("[::1]:9999").is_ok());
    assert_eq!(
        validate_bind_address("192.0.2.1:9999"),
        Err(BindStageError::NonLoopbackAddress {
            address: "192.0.2.1:9999".to_owned(),
        })
    );
    assert!(matches!(
        validate_bind_address("0.0.0.0:9999"),
        Err(BindStageError::NonLoopbackAddress { .. })
    ));
    assert!(matches!(
        validate_bind_address("localhost:9999"),
        Err(BindStageError::InvalidSocketAddress { .. })
    ));
}

#[test]
fn connect_rejects_protocol_target_and_definition_mismatches() {
    let project = project();
    let server = project.server();
    let mut protocol = request_for(7, 1, &[(1, false)]);
    protocol.protocol += 1;
    assert!(matches!(
        server.root().connect(protocol),
        ConnectOutcome::Rejected(ConnectError::ProtocolMismatch { .. })
    ));

    let mut unknown = request_for(7, 1, &[(1, false)]);
    unknown.target = "ship".to_owned();
    assert!(matches!(
        server.root().connect(unknown),
        ConnectOutcome::Rejected(ConnectError::UnknownTarget { .. })
    ));

    assert!(matches!(
        server.root().connect(request_for(8, 1, &[(1, false)])),
        ConnectOutcome::Rejected(ConnectError::TargetDefinitionMismatch { .. })
    ));
}

#[test]
fn connect_canonicalizes_equivalent_target_names() {
    let project = TestProject::new(vec![TargetDefinition::new(
        "t\u{e9}st",
        TargetDefinitionHash([7; 32]),
    )]);
    let server = project.server();

    assert!(matches!(
        server.root().connect(ConnectRequest::new(
            "te\u{301}st",
            TargetDefinitionHash([7; 32]),
        )),
        ConnectOutcome::Connected(_)
    ));
}

#[test]
fn resolve_path_and_fetch_terminal_outcomes_all_carry_the_snapshot_basis() {
    let mut project = TestProject::with_roots(vec![target_with(7, &[])], &[ROOT, "alt"]);
    let server = project.server();
    let builds = TestBuilds::install(&server);
    let hub = connect(&server, &[(1, false)]);
    let built = asset_id(1);
    let failed = asset_id(2);
    let drifted = asset_id(3);
    let deleted = asset_id(4);
    let (hash, payload) = canonical_artifact(
        built,
        type_id(1),
        type_id(1),
        type_id(1),
        LayoutHash([11; 32]),
        Vec::new(),
        vec![5_u8; 70_000],
        vec![Arc::from(vec![8_u8; 3])],
    );
    let structural_len = payload.structural.len();
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &payload),
        hash
    );
    builds.answer(built, Ok(BuildAnswer::Built { content_hash: hash }));
    builds.answer(
        drifted,
        Ok(BuildAnswer::Drifted {
            input: DriftedInput::File("shader.glsl".to_owned()),
        }),
    );
    let primary = |byte: u8| Asset::blob("main", asset_id(byte), type_id(byte), &[byte]);
    project.write_bundle(
        "deleted.bundle",
        BundleUuid([4; 16]),
        Some("main"),
        &[primary(4)],
    );
    project.publish();
    project.remove("deleted.bundle");
    project.write_bundle(
        "textures/a.bundle",
        BundleUuid([1; 16]),
        Some("main"),
        &[primary(1)],
    );
    // One asset in two bundle files: the claim collision withholds it, and
    // it resolves to its error.
    project.write_bundle(
        "failed-a.bundle",
        BundleUuid([2; 16]),
        Some("main"),
        &[primary(2)],
    );
    project.write_bundle(
        "failed-b.bundle",
        BundleUuid([12; 16]),
        Some("main"),
        &[primary(2)],
    );
    project.write_bundle(
        "drifted.bundle",
        BundleUuid([3; 16]),
        Some("main"),
        &[primary(3)],
    );
    // One path in two roots: ambiguous between their primaries.
    project.write_bundle(
        "ambiguous.bundle",
        BundleUuid([5; 16]),
        Some("main"),
        &[primary(5)],
    );
    project.write_in(
        "alt",
        "ambiguous.bundle",
        bundle_bytes(BundleUuid([6; 16]), Some("main"), &[primary(6)]),
    );
    let stamp = project.publish();
    let snap = snapshot(&hub);

    let built_result = snap.resolve(built).success().unwrap();
    assert_eq!(built_result.basis.snapshot, stamp);
    assert_eq!(
        built_result.value,
        ResolveResult::Built { content_hash: hash }
    );
    assert!(matches!(
        snap.resolve(failed).success().unwrap().value,
        ResolveResult::Failed { .. }
    ));
    assert!(matches!(
        snap.resolve(drifted).success().unwrap().value,
        ResolveResult::Drifted {
            input: DriftedInput::File(_),
            current,
        } if current == stamp
    ));
    assert_eq!(
        snap.resolve(asset_id(99)).success().unwrap().value,
        ResolveResult::Missing
    );
    assert_eq!(
        snap.resolve(deleted).success().unwrap().value,
        ResolveResult::Missing
    );

    let path = snap.resolve_path("textures/a.bundle").success().unwrap();
    assert_eq!(path.basis.snapshot, stamp);
    assert_eq!(path.value, PathResolveResult::Resolved(built));
    assert_eq!(
        snap.resolve_path("missing.bundle").success().unwrap().value,
        PathResolveResult::Missing
    );
    assert_eq!(
        snap.resolve_path("ambiguous.bundle")
            .success()
            .unwrap()
            .value,
        PathResolveResult::Failed(PathResolveFailure::Ambiguous {
            candidates: vec![asset_id(5), asset_id(6)],
        })
    );

    let mut fetched = hub.fetch(&snap, hash).success().unwrap();
    assert_eq!(fetched.basis.snapshot, stamp);
    let first = fetched.value.next_chunk().unwrap();
    let second = fetched.value.next_chunk().unwrap();
    let blob = fetched.value.next_chunk().unwrap();
    assert_eq!(first.kind, ArtifactChunkKind::Structural);
    assert_eq!(first.offset, 0);
    assert_eq!(first.bytes.len(), 65_536);
    assert_eq!(second.offset, 65_536);
    assert_eq!(second.bytes.len(), structural_len - 65_536);
    assert_eq!(blob.kind, ArtifactChunkKind::Blob { index: 0 });
    assert_eq!(blob.offset, 0);
    assert!(fetched.value.is_empty());
    assert!(matches!(
        snap.fetch(content_hash(99)),
        RpcResult::Failure(RpcFailure::ArtifactNotFound { .. })
    ));
}

#[test]
fn snapshots_pin_old_metadata_and_refresh_repins_latest() {
    let mut project = project();
    let server = project.server();
    // Each version's resolve is built at the requester's own snapshot.
    let builds = BuildsByVersion::install(&server);
    let hub = connect(&server, &[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let uuid = entry.uuid;
    let (first, first_payload) = canonical_artifact(
        uuid,
        type_id(1),
        type_id(1),
        type_id(1),
        LayoutHash([1; 32]),
        Vec::new(),
        vec![1],
        Vec::new(),
    );
    let (second, second_payload) = canonical_artifact(
        uuid,
        type_id(1),
        type_id(1),
        type_id(1),
        LayoutHash([1; 32]),
        Vec::new(),
        vec![2],
        Vec::new(),
    );
    for (hash, payload) in [(first, first_payload), (second, second_payload)] {
        assert_eq!(
            distill_test_project::put_artifact(&server.handle(), &payload),
            hash
        );
    }
    let first_stamp = publish_entry(&mut project, &entry);
    builds.answer(first_stamp, first);
    let old = snapshot(&hub);
    // The bundle moves and its value changes.
    let mut moved = entry.clone();
    moved.normalized_path = "moved/bundle-1.bundle".to_owned();
    moved.value.blobs = vec![Arc::from([0xAA])];
    project.remove(&entry.normalized_path);
    let second_stamp = publish_entry(&mut project, &moved);
    builds.answer(second_stamp, second);

    assert_eq!(
        old.entry(uuid).success().unwrap().normalized_path,
        entry.normalized_path
    );
    assert_eq!(
        old.resolve(uuid).success().unwrap().value,
        ResolveResult::Built {
            content_hash: first,
        }
    );
    let refreshed = old.refresh().success().unwrap();
    assert_eq!(
        refreshed.version(),
        RpcResult::Success(second_stamp.version)
    );
    assert_eq!(
        refreshed.entry(uuid).success().unwrap().normalized_path,
        moved.normalized_path
    );
    assert_eq!(
        refreshed.resolve(uuid).success().unwrap().value,
        ResolveResult::Built {
            content_hash: second,
        }
    );
}

#[test]
fn configuration_error_is_snapshot_pinned_and_typed_without_blocking_safe_reads() {
    let project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let (hash, payload) = canonical_artifact(
        asset_id(1),
        type_id(1),
        type_id(1),
        type_id(1),
        LayoutHash([1; 32]),
        Vec::new(),
        vec![1, 2, 3],
        Vec::new(),
    );
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &payload),
        hash
    );
    let reason = DscpV1::NonLoopbackAddress {
        address: "198.51.100.7:7331".to_owned(),
    };
    let error = ConfigurationError::from_reason(&reason, "daemon.address is not loopback");
    let stamp = reject_configuration(&project, reason.clone(), "daemon.address is not loopback");
    let failed = snapshot(&hub);

    assert_eq!(
        failed.configuration(),
        RpcResult::Success(ConfigurationStatus::Failed(error.clone()))
    );
    assert_eq!(
        failed.resolve(asset_id(1)),
        RpcResult::ConfigurationFailed(error.clone())
    );
    assert!(matches!(
        failed.resolve_path("a.asset"),
        RpcResult::Success(_)
    ));
    assert!(matches!(hub.fetch(&failed, hash), RpcResult::Success(_)));
    assert!(matches!(failed.refresh(), RpcResult::Success(_)));
    assert!(matches!(
        hub.subscribe(stamp.version, vec![], vec![]),
        RpcResult::Success(_)
    ));
    assert_eq!(
        server.root().connect(request_for(7, 3, &[(1, false)])),
        ConnectOutcome::ConfigurationFailed(error.clone())
    );

    let same_reason_new_words = ConfigurationError::from_reason(&reason, "translated diagnostic");
    assert_eq!(error.reason_hash, same_reason_new_words.reason_hash);
    assert_ne!(error.message, same_reason_new_words.message);
}

#[test]
fn connect_returns_typed_pipeline_unavailable_without_minting_a_hub() {
    let mut project = TestProject::configured(false);
    let server = project.server();
    let failure = test_pipeline_failure();
    reject_pipeline(&project, failure.clone());
    let request = ConnectRequest::new(project.target().name(), project.target().definition_hash());
    assert_eq!(
        server.root().connect(request.clone()),
        ConnectOutcome::PipelineUnavailable(PipelineUnavailableDiagnostic::PipelineFailure(
            failure
        ))
    );

    // The daemon loads the configuration's pipeline module again: Ready.
    project.reconfigure(false);
    let connected = match server.root().connect(request) {
        ConnectOutcome::Connected(connected) => connected,
        other => panic!("expected recovered connection, got {other:?}"),
    };
    assert_eq!(connected.hub.connection_id(), 1);
}

#[test]
fn published_runtime_failure_fences_shared_epoch_without_minting_a_version() {
    let mut project = project();
    let server = project.server();
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let stamp = publish_entry(&mut project, &entry);
    let hub = connect(&server, &[(1, false)]);
    let pinned = snapshot(&hub);
    let failure = PipelineFailure::new(
        PipelineFailureCode::PublishedCallbackPanic,
        PipelineFailureOrigin::PublishedRuntime,
        CleanupDisposition::PublishedEpochLeaked,
        "processor callback panicked",
    )
    .unwrap();
    // The failure lives on the loaded epoch, which the backend answers for;
    // the daemon then fences every connection once.
    let failed = server_over(&project, Arc::new(RuntimeFailedBackend(failure.clone())));
    coordinated(&server, |handle, store| {
        handle.coordinated_pipeline_fence(store)
    })
    .unwrap();

    assert_eq!(server.current_stamp().unwrap(), stamp);
    let reason = ReconnectReason::PipelineEpochChanged;
    assert_reconnect(pinned.version(), reason);
    assert_reconnect(
        pinned.query(AssetQuery {
            uuid: Some(entry.uuid),
            ..AssetQuery::default()
        }),
        reason,
    );
    assert_reconnect(
        pinned.query(AssetQuery {
            terminal_type: Some(entry.terminal_type),
            ..AssetQuery::default()
        }),
        reason,
    );
    assert_reconnect(pinned.entry(entry.uuid), reason);
    assert_reconnect(pinned.resolve(entry.uuid), reason);
    assert_reconnect(hub.write(stamp.version, Vec::new(), false), reason);
    assert_eq!(
        failed.root().connect(request_for(7, 3, &[(1, false)])),
        ConnectOutcome::PipelineUnavailable(PipelineUnavailableDiagnostic::PipelineFailure(
            failure.clone()
        ))
    );
    assert_eq!(
        failed
            .root()
            .metadata(PROTOCOL_VERSION)
            .connected()
            .unwrap()
            .hub
            .diagnostics()
            .success()
            .unwrap()
            .pipeline,
        PipelineDiagnostic::Failed(failure)
    );
}

/// A backend whose served pipeline epoch has failed at runtime.
struct RuntimeFailedBackend(PipelineFailure);

impl AuthoringBackend for RuntimeFailedBackend {
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
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &[AuthoringOp],
        _: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        unreachable!("never writes")
    }

    fn pipeline_failure(
        &self,
        _snapshot: &distill_store::StoreReader,
    ) -> Result<Option<PipelineFailure>, RpcFailure> {
        Ok(Some(self.0.clone()))
    }

    fn prepare_import(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("no imports")
    }

    fn prepare_reimport(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("no reimports")
    }

    fn prepare_operation(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        unreachable!("no operations")
    }
}

#[test]
fn initial_subscription_delta_is_cursor_bound_ordered_and_filters_assets_and_paths() {
    let mut project = project();
    let server = project.server();
    // The first publication logs nothing: no client holds the version before.
    publish_unrelated(&mut project);
    let hub = connect(&server, &[(1, false)]);
    let watched = asset_id(1);
    let ignored = asset_id(2);
    let primary = |byte: u8| Asset::blob("main", asset_id(byte), type_id(byte), &[byte]);
    project.write_bundle(
        "watched.bundle",
        BundleUuid([1; 16]),
        Some("main"),
        &[primary(1)],
    );
    project.write_bundle(
        "ignored.bundle",
        BundleUuid([2; 16]),
        Some("main"),
        &[Asset::blob("main", ignored, type_id(2), &[2])],
    );
    project.publish();
    project.remove("watched.bundle");
    project.remove("ignored.bundle");
    project.publish();

    let install = hub
        .subscribe(
            InputVersion(0),
            vec![watched],
            vec!["watched.bundle".to_owned()],
        )
        .success()
        .unwrap();
    assert_eq!(install.installed, InputVersion(3));
    let first = install.deltas.next().unwrap();
    assert_eq!(first.basis().snapshot.version, InputVersion(3));
    match first {
        StreamEvent::InitialDelta {
            since,
            installed,
            deltas,
            ..
        } => {
            assert_eq!(since, InputVersion(0));
            assert_eq!(installed, InputVersion(3));
            assert_eq!(deltas.len(), 2);
            assert_eq!(deltas[0].basis.snapshot.version, InputVersion(2));
            assert_eq!(deltas[0].assets, vec![(watched, AssetDeltaState::Changed)]);
            assert_eq!(deltas[0].paths, vec!["watched.bundle"]);
            assert_eq!(deltas[1].basis.snapshot.version, InputVersion(3));
            assert_eq!(deltas[1].assets, vec![(watched, AssetDeltaState::Deleted)]);
            assert_eq!(deltas[1].paths, vec!["watched.bundle"]);
        }
        other => panic!("expected initial delta, got {other:?}"),
    }
    assert!(install.deltas.next().is_none());
}

#[test]
fn live_delta_types_changed_deleted_and_restored_and_every_event_has_a_basis() {
    let mut project = project();
    let server = project.server();
    // The daemon serves only after its first publication, which logs nothing.
    publish_unrelated(&mut project);
    let hub = connect(&server, &[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let uuid = entry.uuid;
    let install = hub
        .subscribe(InputVersion(0), vec![uuid], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::InitialDelta { .. })
    ));

    // The bundle appears, goes and comes back. The daemon publishes a
    // returning asset as Changed: it never publishes Restored.
    for (remove, delta) in [
        (false, AssetDeltaState::Changed),
        (true, AssetDeltaState::Deleted),
        (false, AssetDeltaState::Changed),
    ] {
        let stamp = if remove {
            project.remove(&entry.normalized_path);
            project.publish()
        } else {
            publish_entry(&mut project, &entry)
        };
        let event = install.deltas.next().unwrap();
        assert_eq!(event.basis().snapshot, stamp);
        assert!(
            matches!(&event,
                StreamEvent::Delta(Delta { assets, .. }) if *assets == vec![(uuid, delta)]
            ),
            "{event:?}"
        );
    }
}

#[test]
fn repeated_subscribe_unions_names_on_one_stream_and_unsubscribe_removes_them() {
    let mut project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let first_entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let second_entry = authoring_entry(2, AuthoringEntryRole::Runtime);
    let (first, second) = (first_entry.uuid, second_entry.uuid);
    write_entry(&mut project, &first_entry);
    write_entry(&mut project, &second_entry);
    let since = project.publish().version;
    let first_install = hub.subscribe(since, vec![first], vec![]).success().unwrap();
    first_install.deltas.next().unwrap();
    project.remove(&second_entry.normalized_path);
    let removed = project.publish();
    assert!(first_install.deltas.next().is_none());

    let second_install = hub
        .subscribe(since, vec![second], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        first_install.deltas.next(),
        Some(StreamEvent::Delta(Delta { basis, assets, .. }))
            if basis.snapshot == removed
                && assets == vec![(second, AssetDeltaState::Deleted)]
    ));
    assert!(second_install.deltas.next().is_none());

    assert_eq!(hub.unsubscribe(vec![first], vec![]), RpcResult::Success(()));
    // The first goes, the second comes back (the daemon publishes a
    // returning asset as Changed).
    project.remove(&first_entry.normalized_path);
    write_entry(&mut project, &second_entry);
    project.publish();
    assert!(matches!(
        first_install.deltas.next(),
        Some(StreamEvent::Delta(Delta { assets, .. }))
            if assets == vec![(second, AssetDeltaState::Changed)]
    ));
}

#[test]
fn subscription_asset_and_path_sets_have_typed_cardinality_limits() {
    let project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let assets = (0..MAX_SUBSCRIBED_ASSETS)
        .map(|index| AssetUuid((index as u128).to_le_bytes()))
        .collect();
    assert!(matches!(
        hub.subscribe(InputVersion(0), assets, Vec::new()),
        RpcResult::Success(_)
    ));
    assert!(matches!(
        hub.subscribe(
            InputVersion(0),
            vec![AssetUuid((MAX_SUBSCRIBED_ASSETS as u128).to_le_bytes())],
            Vec::new(),
        ),
        RpcResult::Failure(RpcFailure::ResourceLimit { resource, limit })
            if resource == "subscribed assets" && limit == MAX_SUBSCRIBED_ASSETS
    ));

    let paths = (0..MAX_SUBSCRIBED_PATHS)
        .map(|index| format!("bounded/path-{index}"))
        .collect();
    assert!(matches!(
        hub.subscribe(InputVersion(0), Vec::new(), paths),
        RpcResult::Success(_)
    ));
    assert!(matches!(
        hub.subscribe(
            InputVersion(0),
            Vec::new(),
            vec!["bounded/overflow".to_owned()],
        ),
        RpcResult::Failure(RpcFailure::ResourceLimit { resource, limit })
            if resource == "subscribed paths" && limit == MAX_SUBSCRIBED_PATHS
    ));
}

#[test]
fn old_history_returns_resync_marker_and_future_cursor_is_typed_failure() {
    let mut project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    publish_entry(&mut project, &entry);
    project.remove(&entry.normalized_path);
    assert_eq!(project.publish().version, InputVersion(2));
    // History keeps the last RETAINED_HISTORY_VERSIONS versions: an
    // unrelated file coming and going pushes version 1's history out.
    let churn = authoring_entry(2, AuthoringEntryRole::AuthoringOnly);
    while server.current_stamp().unwrap().version.0 < RETAINED_HISTORY_VERSIONS + 1 {
        publish_entry(&mut project, &churn);
        project.remove(&churn.normalized_path);
        project.publish();
    }
    let current = InputVersion(RETAINED_HISTORY_VERSIONS + 2);
    assert_eq!(server.current_stamp().unwrap().version, current);

    let install = hub
        .subscribe(InputVersion(0), vec![asset_id(1)], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::ResyncRequired {
            oldest_available: InputVersion(2),
            ..
        })
    ));
    assert!(matches!(
        hub.subscribe(InputVersion(current.0 + 1), vec![], vec![]),
        RpcResult::Failure(RpcFailure::InvalidCursor {
            since,
            current: observed,
        }) if since.0 == current.0 + 1 && observed == current
    ));
}

#[test]
fn slow_subscription_queue_is_bounded_by_a_resync_marker() {
    let mut project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let watched = asset_id(1);
    let install = hub
        .subscribe(InputVersion(0), vec![watched], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::InitialDelta { .. })
    ));

    // The watched asset changes in each of 1025 versions after the first,
    // which logs nothing.
    publish_unrelated(&mut project);
    for version in 2..=1026_u16 {
        project.write_bundle(
            "watched.bundle",
            BundleUuid([1; 16]),
            Some("main"),
            &[Asset::blob(
                "main",
                watched,
                type_id(1),
                &version.to_le_bytes(),
            )],
        );
        assert_eq!(project.publish().version, InputVersion(version.into()));
    }

    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::ResyncRequired {
            oldest_available: InputVersion(1026),
            ..
        })
    ));
    assert!(install.deltas.next().is_none());
}

#[test]
fn restart_required_names_sorted_unique_keys_without_advancing_version() {
    let project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();
    let before = server.current_stamp().unwrap();
    announce_restart(
        &project,
        &[
            RestartOnlyChange::StatePath("state".into()),
            RestartOnlyChange::Address(([127, 0, 0, 1], 9000).into()),
            RestartOnlyChange::Address(([127, 0, 0, 1], 9001).into()),
        ],
    );
    assert_eq!(server.current_stamp().unwrap(), before);
    match install.deltas.next().unwrap() {
        StreamEvent::Asset { basis, event } => {
            assert_eq!(basis.snapshot, before);
            assert_eq!(
                event,
                AssetEvent::RestartRequired {
                    keys: vec!["daemon.address".to_owned(), "daemon.state_path".to_owned()]
                }
            );
        }
        other => panic!("expected restart event, got {other:?}"),
    }
}

#[test]
fn restart_required_is_queued_after_the_cursor_bound_first_message() {
    let project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    announce_restart(
        &project,
        &[RestartOnlyChange::Address(([127, 0, 0, 1], 9000).into())],
    );
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::InitialDelta { .. })
    ));
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::Asset {
            event: AssetEvent::RestartRequired { keys },
            ..
        }) if keys == vec!["daemon.address"]
    ));
}

#[test]
fn restart_required_replaces_the_prior_key_set() {
    let project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    announce_restart(
        &project,
        &[RestartOnlyChange::Address(([127, 0, 0, 1], 9000).into())],
    );
    announce_restart(&project, &[RestartOnlyChange::AutoCodegen(true)]);
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::Asset {
            event: AssetEvent::RestartRequired { keys },
            ..
        }) if keys == vec!["codegen.auto_codegen"]
    ));
}

#[test]
fn delta_stream_capability_keeps_the_connection_alive_after_hub_drop() {
    let mut project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let uuid = entry.uuid;
    let since = publish_entry(&mut project, &entry).version;
    let install = hub.subscribe(since, vec![uuid], vec![]).success().unwrap();
    install.deltas.next().unwrap();
    drop(hub);
    project.remove(&entry.normalized_path);
    project.publish();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::Delta(Delta { assets, .. }))
            if assets == vec![(uuid, AssetDeltaState::Deleted)]
    ));
}

#[test]
fn a_configuration_change_fences_every_target_bound_method_and_prompts_stream() {
    let mut project = TestProject::configured(false);
    let server = project.server();
    let old = project.target().clone();
    let hub = connect_to(&server, &old);
    let snap = snapshot(&hub);
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();
    // A configuration that redefines the target publishes with its
    // pipeline fence: there is no target-specific reason.
    project.reconfigure(true);
    assert_ne!(project.target().definition_hash(), old.definition_hash());

    match install.deltas.next().unwrap() {
        StreamEvent::Asset {
            event: AssetEvent::ReconnectRequired { reason },
            ..
        } => assert_eq!(reason, ReconnectReason::PipelineEpochChanged),
        other => panic!("expected reconnect event, got {other:?}"),
    }
    let reason = ReconnectReason::PipelineEpochChanged;
    assert_reconnect(hub.snapshot(), reason);
    assert_reconnect(snap.refresh(), reason);
    assert_reconnect(snap.resolve(asset_id(1)), reason);
    assert_reconnect(snap.resolve_path("a.asset"), reason);
    assert_reconnect(snap.fetch(content_hash(1)), reason);
    assert_reconnect(hub.fetch(&snap, content_hash(1)), reason);
    assert_reconnect(hub.subscribe(InputVersion(0), vec![], vec![]), reason);
    assert_reconnect(hub.unsubscribe(vec![], vec![]), reason);
    assert_reconnect(snap.version(), reason);
    assert_reconnect(snap.configuration(), reason);
    // The reconnect under the stale definition is what names it.
    assert!(matches!(
        server
            .root()
            .connect(ConnectRequest::new(old.name(), old.definition_hash())),
        ConnectOutcome::Rejected(ConnectError::TargetDefinitionMismatch { .. })
    ));
}

#[test]
fn pipeline_epoch_change_fences_every_target_bound_capability_and_prompts_stream() {
    let project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let snapshot = snapshot(&hub);
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();

    // A rejected pipeline candidate changes the pipeline epoch.
    reject_pipeline(&project, test_pipeline_failure());

    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::Asset {
            event: AssetEvent::ReconnectRequired {
                reason: ReconnectReason::PipelineEpochChanged,
            },
            ..
        })
    ));
    assert_reconnect(hub.snapshot(), ReconnectReason::PipelineEpochChanged);
    assert_reconnect(
        snapshot.fetch(content_hash(1)),
        ReconnectReason::PipelineEpochChanged,
    );
}

#[test]
fn unchanged_pipeline_diagnostic_does_not_create_a_false_epoch_fence() {
    let mut project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    // Every scan publication carries the pipeline diagnostic, here the
    // unchanged Ready.
    publish_unrelated(&mut project);
    assert!(matches!(hub.snapshot(), RpcResult::Success(_)));
}

#[test]
fn hub_authoring_and_wire_tree_surface_is_versioned_typed_and_generation_first() {
    let mut project = project();
    let daemon = project.server();
    // The daemon's authoring service writes files; a second server over
    // the same store records imports, reimports and operations.
    let backend = Arc::new(RecordingAuthoringBackend::default());
    let server = server_over(&project, backend.clone());
    let hub = connect(&daemon, &[(1, false)]);
    let recording = connect(&server, &[(1, false)]);
    let mut entry = authoring_entry(21, AuthoringEntryRole::Runtime);
    entry.type_uuid = type_id(1);
    entry.terminal_type = type_id(1);
    // The daemon writes the new bundle file and names it; the watcher
    // publishes it.
    let receipt = hub
        .write(
            InputVersion(0),
            vec![AuthoringOp::Set(entry.clone())],
            false,
        )
        .success()
        .unwrap();
    assert!(matches!(
        &receipt.files[..],
        [WrittenFile { root, path, content_hash: Some(_) }]
            if root == distill_test_project::ROOT && *path == entry.normalized_path
    ));
    assert_eq!(server.current_stamp().unwrap().version, InputVersion(0));
    touch_written(&mut project, &receipt);
    assert_eq!(project.publish().version, InputVersion(1));
    assert_eq!(
        snapshot(&hub)
            .entry(entry.uuid)
            .success()
            .unwrap()
            .terminal_type,
        entry.terminal_type
    );
    assert!(matches!(
        snapshot(&hub).resolve(entry.uuid),
        RpcResult::Success(TerminalEvent {
            value: ResolveResult::Drifted {
                input: DriftedInput::Asset(uuid),
                ..
            },
            ..
        }) if uuid == entry.uuid
    ));
    assert!(matches!(
        hub.write(
            InputVersion(0),
            vec![AuthoringOp::Remove { uuid: entry.uuid }],
            false
        ),
        RpcResult::Failure(RpcFailure::StaleInputVersion {
            expected: InputVersion(1),
            got: InputVersion(0),
        })
    ));
    let import_request = ImportRequest {
        importer: "image-importer".to_owned(),
        sources: vec!["source/image.png".to_owned()],
        dest: "generated/image.bundle".to_owned(),
        settings: AuthoringValue {
            canonical_value: Arc::from(&b"{}"[..]),
            blobs: vec![Arc::from(&b"settings-blob"[..])],
        },
        watch: true,
        root: "assets".to_owned(),
    };
    assert_eq!(
        recording.import(InputVersion(1), import_request.clone()),
        RpcResult::Success(BundleUuid([70; 16]))
    );
    assert_eq!(
        recording.reimport(InputVersion(2), BundleUuid([70; 16])),
        RpcResult::Success(BundleUuid([70; 16]))
    );
    let operation = LongRunningOp::Doctor(Arc::from(&b"verify-cas"[..]));
    let progress = recording
        .operation(InputVersion(3), operation.clone())
        .success()
        .unwrap();
    let events = progress.collect::<Vec<_>>();
    assert_eq!(events.len(), 3);
    assert_eq!(events[1].state, AuthoringProgressState::Running);
    assert_eq!(&*events[1].payload, b"verify-cas");
    assert_eq!(events[2].state, AuthoringProgressState::Completed);
    assert_eq!(server.current_stamp().unwrap().version, InputVersion(4));
    let cancelled_operation = LongRunningOp::Doctor(Arc::from(&b"cancel-me"[..]));
    let mut cancellable = recording
        .operation(InputVersion(4), cancelled_operation.clone())
        .success()
        .unwrap();
    assert_eq!(
        cancellable.next().unwrap().state,
        AuthoringProgressState::Started
    );
    assert!(cancellable.cancel());
    assert_eq!(
        cancellable.next().unwrap().state,
        AuthoringProgressState::Cancelled
    );
    assert!(!cancellable.cancel());
    assert_eq!(server.current_stamp().unwrap().version, InputVersion(4));
    assert_eq!(*backend.imports.lock().unwrap(), vec![import_request]);
    assert_eq!(
        *backend.reimports.lock().unwrap(),
        vec![BundleUuid([70; 16])]
    );
    assert_eq!(
        *backend.operations.lock().unwrap(),
        vec![operation, cancelled_operation]
    );

    let wire_node = distill_wire::wire::WireNode::Unit { offset: 0 };
    let tree: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&wire_node).unwrap());
    let hash = distill_wire::dswl::dswl_hash(&wire_node).unwrap();
    assert_eq!(
        distill_test_project::put_wire_tree(&server.handle(), &tree),
        hash
    );
    let (wire_artifact_hash, wire_artifact) = canonical_artifact(
        asset_id(1),
        type_id(1),
        type_id(1),
        type_id(1),
        hash,
        Vec::new(),
        vec![1],
        Vec::new(),
    );
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &wire_artifact),
        wire_artifact_hash
    );
    assert_eq!(hub.wire_tree(hash), RpcResult::Success(tree));

    let snapshot = snapshot(&hub);
    let authoring = authoring_snapshot(&hub);
    reject_pipeline(&project, test_pipeline_failure());
    let reconnect = ReconnectReason::PipelineEpochChanged;
    assert_reconnect(hub.write(InputVersion(99), vec![], false), reconnect);
    assert_reconnect(
        snapshot.query(AssetQuery {
            path_prefix: Some("../invalid".to_owned()),
            ..AssetQuery::default()
        }),
        reconnect,
    );
    assert_reconnect(
        authoring.query(AssetQuery {
            path_prefix: Some("../invalid".to_owned()),
            ..AssetQuery::default()
        }),
        reconnect,
    );
}

#[test]
fn long_running_operation_payloads_are_canonical_and_closed() {
    let rename = RenameWithFixupsRequest {
        bundle: BundleUuid([9; 16]),
        destination_root: "assets".into(),
        destination_path: "renamed/item.bundle".into(),
    };
    assert_eq!(
        RenameWithFixupsRequest::decode(&rename.encode()).unwrap(),
        rename
    );

    assert_eq!(
        DoctorRequest::decode(&DoctorRequest::Verify.encode()).unwrap(),
        DoctorRequest::Verify
    );
    assert_eq!(
        DoctorRequest::decode(&[1, 99]),
        Err(OperationPayloadError::InvalidTag(99))
    );
    // Tag 2 was the retired displaced-inode clean, tag 3 the retired
    // index rebuild: doctor only reports.
    for retired in [2, 3] {
        assert_eq!(
            DoctorRequest::decode(&[1, retired]),
            Err(OperationPayloadError::InvalidTag(retired))
        );
    }
}

#[test]
fn an_unregistered_importer_is_typed_and_never_advances_the_input_version() {
    // The daemon has no importer the project does not provide: it says so,
    // and publishes nothing.
    let mut project = project();
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let base = publish_entry(&mut project, &entry);
    let server = project.server();
    let daemon = connect(&server, &[(1, false)]);
    let request = ImportRequest {
        importer: "image-importer".to_owned(),
        sources: vec!["source/image.png".to_owned()],
        dest: "generated/image.bundle".to_owned(),
        settings: AuthoringValue {
            canonical_value: Arc::from(&b"{}"[..]),
            blobs: vec![],
        },
        watch: false,
        root: String::new(),
    };
    assert_eq!(
        daemon.import(base.version, request),
        RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
            detail: "importer \"image-importer\" is not registered".to_owned(),
        })
    );
    assert_eq!(server.current_stamp().unwrap(), base);
}

#[test]
fn fence_is_the_guarantee_even_if_reconnect_event_is_not_consumed() {
    let mut project = TestProject::configured(false);
    let server = project.server();
    let hub = connect_to(&server, project.target());
    let snap = snapshot(&hub);
    let _unpolled = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    // A configuration that redefines the target is a new pipeline
    // epoch too, which the fence names first.
    project.reconfigure(true);
    assert_reconnect(
        snap.resolve(asset_id(1)),
        ReconnectReason::PipelineEpochChanged,
    );
}

#[test]
fn expired_and_foreign_snapshots_fail_without_serving_data() {
    let project = project();
    let server = project.server();
    let first_hub = connect(&server, &[(1, false)]);
    let second_hub = connect(&server, &[(1, false)]);
    let first = snapshot(&first_hub);
    let second = snapshot(&second_hub);
    assert_eq!(
        first_hub.fetch(&second, content_hash(1)),
        RpcResult::Failure(RpcFailure::ForeignSnapshot)
    );

    first.expire();
    assert!(matches!(
        first.resolve(asset_id(1)),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    ));
    assert!(matches!(
        first.resolve_path("a.asset"),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    ));
    assert!(matches!(
        first.fetch(content_hash(1)),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    ));
    assert!(matches!(
        first.refresh(),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    ));
}

#[test]
fn commit_validation_is_atomic_for_duplicate_names_and_invalid_paths() {
    let project = project();
    let server = project.server();
    let before = server.current_stamp().unwrap();
    let deleted = AssetMutation {
        uuid: asset_id(1),
        delta: AssetDeltaState::Deleted,
    };
    assert!(matches!(
        offer(
            &server,
            Commit {
                assets: vec![deleted.clone(), deleted],
                ..Commit::default()
            }
        ),
        Err(AdminError::DuplicateAssetMutation { .. })
    ));
    assert!(matches!(
        offer(
            &server,
            Commit {
                paths: vec![PathMutation::Remove {
                    path: "../escape".to_owned(),
                }],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidPath { .. })
    ));
    assert!(matches!(
        offer(
            &server,
            Commit {
                paths: vec![PathMutation::Remove {
                    path: "cafe\u{301}.asset".to_owned(),
                }],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidPath { .. })
    ));
    assert!(matches!(
        offer(
            &server,
            Commit {
                paths: vec![PathMutation::Remove {
                    path: "nul\0.asset".to_owned(),
                }],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidPath { .. })
    ));
    assert_eq!(
        offer(
            &server,
            Commit {
                paths: vec![PathMutation::Set {
                    path: "empty.asset".to_owned(),
                    candidates: BTreeSet::new(),
                }],
                ..Commit::default()
            }
        ),
        Err(AdminError::EmptyPathCandidates {
            path: "empty.asset".to_owned()
        })
    );
    assert_eq!(server.current_stamp().unwrap(), before);
}

#[test]
fn authoring_identity_validation_rejects_reserved_local_ids_and_noncanonical_tags_atomically() {
    let project = project();
    let server = project.server();
    let before = server.current_stamp().unwrap();
    let mut reserved = authoring_entry(1, AuthoringEntryRole::Runtime);
    reserved.local_id = "$generated".to_owned();
    assert!(matches!(
        offer(
            &server,
            Commit {
                authoring: vec![AuthoringMutation::Set(reserved)],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidAuthoringIdentity { .. })
    ));
    let mut bad_tag = authoring_entry(1, AuthoringEntryRole::Runtime);
    bad_tag.tags = std::collections::BTreeMap::from([("bad\0tag".to_owned(), None)]);
    assert!(matches!(
        offer(
            &server,
            Commit {
                authoring: vec![AuthoringMutation::Set(bad_tag)],
                ..Commit::default()
            }
        ),
        Err(AdminError::InvalidAuthoringIdentity { .. })
    ));
    assert_eq!(server.current_stamp().unwrap(), before);
}

#[test]
fn wire_tree_coverage_keeps_pinned_compatible_artifact_references() {
    let mut project = project();
    let server = project.server();
    let hub = connect(&server, &[(1, false)]);
    let node = distill_wire::wire::WireNode::Unit { offset: 0 };
    let tree: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&node).unwrap());
    let layout_hash = distill_wire::dswl::dswl_hash(&node).unwrap();
    assert_eq!(
        distill_test_project::put_wire_tree(&server.handle(), &tree),
        layout_hash
    );
    let asset = asset_id(9);
    let (historical_hash, historical) = canonical_artifact(
        asset,
        type_id(2),
        type_id(2),
        type_id(2),
        layout_hash,
        Vec::new(),
        vec![2],
        Vec::new(),
    );
    let (current_hash, current) = canonical_artifact(
        asset,
        type_id(1),
        type_id(1),
        type_id(1),
        layout_hash,
        Vec::new(),
        vec![1],
        Vec::new(),
    );
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &historical),
        historical_hash
    );
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &current),
        current_hash
    );
    // The asset resolves to the historical artifact, then to the current
    // one, then is deleted.
    let builds = BuildsByVersion::install(&server);
    let entry = authoring_entry(9, AuthoringEntryRole::Runtime);
    let historical_stamp = publish_entry(&mut project, &entry);
    builds.answer(historical_stamp, historical_hash);
    assert_eq!(
        snapshot(&hub).resolve(asset).success().unwrap().value,
        ResolveResult::Built {
            content_hash: historical_hash
        }
    );
    let mut changed = entry.clone();
    changed.value.blobs = vec![Arc::from([0xAA])];
    let current_stamp = publish_entry(&mut project, &changed);
    builds.answer(current_stamp, current_hash);
    assert_eq!(
        snapshot(&hub).resolve(asset).success().unwrap().value,
        ResolveResult::Built {
            content_hash: current_hash
        }
    );

    assert_eq!(hub.wire_tree(layout_hash), RpcResult::Success(tree.clone()));

    project.remove(&entry.normalized_path);
    project.publish();
    assert_eq!(hub.wire_tree(layout_hash), RpcResult::Success(tree));
}

#[test]
fn an_artifact_with_an_unresolved_direct_load_edge_stays_fetchable() {
    let mut project = project();
    let server = project.server();
    let (hash, payload) = canonical_artifact(
        asset_id(1),
        type_id(2),
        type_id(2),
        type_id(2),
        LayoutHash([1; 32]),
        vec![ServedLoadEdge {
            asset: asset_id(2),
            expected_terminal: type_id(2),
        }],
        vec![1],
        Vec::new(),
    );
    assert_eq!(
        distill_test_project::put_artifact(&server.handle(), &payload),
        hash
    );
    // The asset 1 resolves to it; its direct dependency, the asset 2, is
    // not in the namespace.
    let builds = TestBuilds::install(&server);
    builds.answer(asset_id(1), Ok(BuildAnswer::Built { content_hash: hash }));
    let mut entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    entry.type_uuid = type_id(2);
    entry.terminal_type = type_id(2);
    publish_entry(&mut project, &entry);
    let hub = connect(&server, &[(1, false), (2, false)]);
    assert_eq!(
        snapshot(&hub).resolve(asset_id(1)).success().unwrap().value,
        ResolveResult::Built { content_hash: hash }
    );
    let RpcResult::Success(fetched) = snapshot(&hub).fetch(hash) else {
        panic!("an authenticated artifact with unresolved direct edges must remain fetchable");
    };
    assert_eq!(
        fetched.value.load_edges(),
        &[ServedLoadEdge {
            asset: asset_id(2),
            expected_terminal: type_id(2),
        }]
    );
}

/// A backend that writes, then fails: a write, an import whose bundle
/// identity changed, and a deferred operation.
struct PartialFailBackend;

fn write_then_fail(store: &mut distill_store::Store, root: &str) {
    store
        .input_transaction(|txn| txn.intern_root(root).map(drop))
        .unwrap();
}

struct PartialFailOperation;

impl DeferredOperation for PartialFailOperation {
    fn complete(
        &self,
        store: &mut distill_store::Store,
        _base: InputVersion,
    ) -> Result<DeferredOperationResult, String> {
        write_then_fail(store, "partial-operation");
        Err("a later file failed".to_owned())
    }
}

impl AuthoringBackend for PartialFailBackend {
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
        store: &mut distill_store::Store,
        _base: InputVersion,
        _operations: &[AuthoringOp],
        _force_lossy: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        write_then_fail(store, "partial-write");
        Err(RpcFailure::InvalidAuthoringRequest {
            detail: "refinement failed".to_owned(),
        })
    }

    fn prepare_import(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("only reimports")
    }

    fn prepare_reimport(
        &self,
        store: &mut distill_store::Store,
        _: InputVersion,
        _: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        write_then_fail(store, "partial-reimport");
        Ok(PreparedImportCommit {
            bundle: BundleUuid([0xEE; 16]),
            commit: Commit::default(),
        })
    }

    fn prepare_operation(
        &self,
        _: &mut distill_store::Store,
        _: InputVersion,
        _: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        Ok(PreparedOperationCommit::deferred(
            Arc::new(PartialFailOperation),
            vec![
                AuthoringProgressEvent {
                    sequence: 0,
                    state: AuthoringProgressState::Started,
                    payload: Arc::from([]),
                },
                AuthoringProgressEvent {
                    sequence: 1,
                    state: AuthoringProgressState::Completed,
                    payload: Arc::from([]),
                },
            ],
        ))
    }
}

/// A backend step that fails after writing commits none of it: the
/// version does not move and no row of the failed step is visible (a
/// failed input would otherwise commit as a version with no change log).
#[test]
fn a_failed_backend_step_commits_nothing_it_wrote() {
    let mut project = project();
    let base = publish_unrelated(&mut project).version;
    let server = server_over(&project, Arc::new(PartialFailBackend));
    let hub = connect(&server, &[(1, false)]);
    let unchanged = |root: &str| {
        assert_eq!(server.current_stamp().unwrap().version, base, "{root}");
        let store = server.handle().opener().open_reader().unwrap();
        assert_eq!(store.root_id(root).unwrap(), None, "{root} committed");
        assert_eq!(store.input_version().unwrap(), base, "{root}");
    };

    let write = hub.write(base, vec![AuthoringOp::Remove { uuid: asset_id(7) }], false);
    assert!(matches!(write, RpcResult::Failure(_)), "{write:?}");
    unchanged("partial-write");

    let reimport = hub.reimport(base, BundleUuid([0xAA; 16]));
    assert!(matches!(reimport, RpcResult::Failure(_)), "{reimport:?}");
    unchanged("partial-reimport");

    let progress = hub
        .operation(
            base,
            LongRunningOp::RenameWithFixups(Arc::from(&b"rename"[..])),
        )
        .success()
        .unwrap();
    let events = progress.collect::<Vec<_>>();
    assert_eq!(
        events.last().unwrap().state,
        AuthoringProgressState::Failed,
        "{events:?}"
    );
    unchanged("partial-operation");
}

/// `publish` on a writer of `server`'s store, as the daemon coordinator
/// runs its coordinated publications.
fn coordinated<T>(
    server: &Server,
    publish: impl FnOnce(&ServerHandle, &mut distill_store::Store) -> T,
) -> T {
    let handle = server.handle();
    let mut writer = handle.opener().open_writer().unwrap();
    publish(&handle, &mut writer)
}

/// The daemon announces `changes` as the restart it needs, as the process
/// loop does for a restart-only configuration edit.
fn announce_restart(project: &TestProject, changes: &[RestartOnlyChange]) {
    project.coordinator().set_restart_required(changes).unwrap();
}
