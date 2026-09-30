use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distill_core::id::BundleFileHash;
use distill_json::AuthoredValue;
use distill_rpc::*;
use distill_schema::ngp_schema::{
    node_hash, snapshot_to_json, LogicalSchema, PrimitiveKind, SchemaNode,
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

fn server_with(policies: &[(u8, bool)]) -> Server {
    Server::new(StoreInstanceId([9; 16]), vec![target_with(7, policies)]).unwrap()
}

#[derive(Default)]
struct RecordingBuildBackend {
    requests: Mutex<Vec<BuildRequest>>,
}

impl BuildBackend for RecordingBuildBackend {
    fn build(&self, request: &BuildRequest) -> Result<BuildBackendOutcome, RpcFailure> {
        self.requests.lock().unwrap().push(request.clone());
        let wire_node = distill_wire::wire::WireNode::Unit { offset: 0 };
        let wire_bytes: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&wire_node).unwrap());
        let layout_hash = distill_wire::dswl::dswl_hash(&wire_node).unwrap();
        let authored_type = if request.output_key.is_empty() {
            request.entry.type_uuid
        } else {
            request.requested_terminal_type
        };
        let (content_hash, payload) = canonical_artifact(
            request.requested_asset,
            authored_type,
            request.requested_terminal_type,
            request.requested_terminal_type,
            layout_hash,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        Ok(BuildBackendOutcome::Built(BuildPublication {
            root_content_hash: content_hash,
            artifacts: vec![BuildArtifactPublication {
                content_hash,
                payload,
            }],
            wire_trees: vec![BuildWireTree {
                layout_hash,
                bytes: wire_bytes,
            }],
        }))
    }
}

#[derive(Default)]
struct DepthLimitedBuildBackend {
    calls: Mutex<usize>,
}

struct LifecycleBuildBackend {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl BuildBackend for LifecycleBuildBackend {
    fn build(&self, request: &BuildRequest) -> Result<BuildBackendOutcome, RpcFailure> {
        RecordingBuildBackend::default().build(request)
    }

    fn build_finished(&self, _request: &BuildRequest) -> Result<(), RpcFailure> {
        self.events.lock().unwrap().push("finish");
        Ok(())
    }
}

impl BuildBackend for DepthLimitedBuildBackend {
    fn build(&self, request: &BuildRequest) -> Result<BuildBackendOutcome, RpcFailure> {
        *self.calls.lock().unwrap() += 1;
        Err(RpcFailure::BuildDepthExceeded {
            limit: 1,
            chain: vec![request.requested_asset, asset_id(99)],
        })
    }
}

#[test]
fn dependency_depth_exhaustion_is_typed_and_never_memoized() {
    let server = server_with(&[(1, false)]);
    let backend = Arc::new(DepthLimitedBuildBackend::default());
    server.install_build_backend(backend.clone());
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    server
        .commit(Commit {
            assets: vec![set_asset(
                entry.uuid,
                StoredResolve::Drifted {
                    input: DriftedInput::Asset(entry.uuid),
                },
                AssetDeltaState::Changed,
            )],
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            ..Commit::default()
        })
        .unwrap();
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
    let server = server_with(&[(1, false)]);
    let events = Arc::new(Mutex::new(Vec::new()));
    server.install_build_backend(Arc::new(LifecycleBuildBackend {
        events: Arc::clone(&events),
    }));
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    server
        .commit(Commit {
            assets: vec![set_asset(
                entry.uuid,
                StoredResolve::Drifted {
                    input: DriftedInput::Asset(entry.uuid),
                },
                AssetDeltaState::Changed,
            )],
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            ..Commit::default()
        })
        .unwrap();
    let snapshot = snapshot(&connect(&server, &[(1, false)]));
    let clone = snapshot.clone();

    assert!(matches!(
        snapshot.resolve(entry.uuid),
        RpcResult::Success(_)
    ));
    assert_eq!(&events.lock().unwrap()[..], &["finish"]);
    clone.expire();
    assert_eq!(
        snapshot.version(),
        RpcResult::Failure(RpcFailure::SnapshotExpired)
    );
}

#[test]
fn snapshot_and_connection_bounds_release_the_oldest_capabilities() {
    let server = server_with(&[]);
    server
        .install_snapshot_policy(SnapshotPolicy {
            ttl: Duration::from_secs(60),
            max_snapshots: 1,
            max_connections: 2,
        })
        .unwrap();
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

    let second_hub = connect(&server, &[]);
    assert!(matches!(first_hub.snapshot(), RpcResult::Success(_)));
    server
        .install_snapshot_policy(SnapshotPolicy {
            ttl: Duration::from_secs(60),
            max_snapshots: 1,
            max_connections: 1,
        })
        .unwrap();
    assert!(matches!(
        first_hub.snapshot(),
        RpcResult::Failure(RpcFailure::ConnectionClosed)
    ));
    assert!(matches!(second_hub.snapshot(), RpcResult::Success(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn connection_cap_terminates_an_already_waiting_delta_stream() {
    tokio::task::LocalSet::new()
        .run_until(async {
    let server = server_with(&[]);
    server
        .install_snapshot_policy(SnapshotPolicy {
            ttl: Duration::from_secs(60),
            max_snapshots: 8,
            max_connections: 1,
        })
        .unwrap();
    let first_hub = connect(&server, &[]);
    let install = first_hub
        .subscribe(InputVersion(0), Vec::new(), Vec::new())
        .success()
        .unwrap();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::InitialDelta { .. })
    ));

    let stream = install.deltas.clone();
    let pending = tokio::task::spawn_local(async move { stream.next_async().await });
    tokio::task::yield_now().await;
    let second_hub = connect(&server, &[]);

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("evicted delta stream remained blocked")
            .unwrap(),
        None
    );
    assert!(matches!(
        first_hub.snapshot(),
        RpcResult::Failure(RpcFailure::ConnectionClosed)
    ));
    assert!(matches!(second_hub.snapshot(), RpcResult::Success(_)));
        })
        .await;
}

#[test]
fn drifted_resolve_builds_once_per_snapshot_target_and_publishes_canonical_outputs() {
    let server = server_with(&[(1, false)]);
    let backend = Arc::new(RecordingBuildBackend::default());
    server.install_build_backend(backend.clone());
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let first_stamp = server
        .commit(Commit {
            assets: vec![set_asset(
                entry.uuid,
                StoredResolve::Drifted {
                    input: DriftedInput::Asset(entry.uuid),
                },
                AssetDeltaState::Changed,
            )],
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            ..Commit::default()
        })
        .unwrap();
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
    assert_eq!(backend.requests.lock().unwrap().len(), 1);
    assert_eq!(backend.requests.lock().unwrap()[0].basis, first_stamp);

    let second_stamp = server.commit(Commit::default()).unwrap();
    let second = first.refresh().success().unwrap();
    assert_eq!(second.stamp(), second_stamp);
    assert_eq!(
        second.resolve(entry.uuid).success().unwrap().value,
        ResolveResult::Built {
            content_hash: first_hash
        }
    );
    let requests = backend.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].basis, second_stamp);
    assert_eq!(requests[1].target, "dev");
    assert_eq!(requests[1].target_definition, target_hash(7));
    assert_eq!(requests[1].requested_asset, entry.uuid);
    assert!(requests[1].output_key.is_empty());
}

#[test]
fn batch_resolve_marks_lazy_build_work_as_batch() {
    let server = server_with(&[(1, false)]);
    let backend = Arc::new(RecordingBuildBackend::default());
    server.install_build_backend(backend.clone());
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    server
        .commit(Commit {
            assets: vec![set_asset(
                entry.uuid,
                StoredResolve::Drifted {
                    input: DriftedInput::Asset(entry.uuid),
                },
                AssetDeltaState::Changed,
            )],
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            ..Commit::default()
        })
        .unwrap();
    let snapshot = snapshot(&connect(&server, &[(1, false)]));

    assert!(matches!(
        snapshot.resolve_batch(entry.uuid),
        RpcResult::Success(_)
    ));
    assert_eq!(
        backend.requests.lock().unwrap()[0].work_class,
        BuildWorkClass::Batch
    );
}

#[test]
fn derived_child_resolution_builds_the_parent_and_selects_the_declared_output() {
    let server = server_with(&[(1, false), (2, false)]);
    let backend = Arc::new(RecordingBuildBackend::default());
    server.install_build_backend(backend.clone());
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let output_key = "reflection".to_owned();
    let child = AssetUuid::v5(entry.uuid, &output_key);
    server
        .commit(Commit {
            assets: vec![set_asset(
                entry.uuid,
                StoredResolve::Drifted {
                    input: DriftedInput::Asset(entry.uuid),
                },
                AssetDeltaState::Changed,
            )],
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            derived_outputs: Some(BTreeMap::from([(
                child,
                DerivedOutputEntry {
                    parent: entry.uuid,
                    output_key: output_key.clone(),
                    terminal_type: type_id(2),
                },
            )])),
            ..Commit::default()
        })
        .unwrap();

    let snapshot = snapshot(&connect(&server, &[(1, false), (2, false)]));
    let hash = match snapshot.resolve(child).success().unwrap().value {
        ResolveResult::Built { content_hash } => content_hash,
        other => panic!("expected built derived output, got {other:?}"),
    };
    assert!(matches!(snapshot.fetch(hash), RpcResult::Success(_)));
    let requests = backend.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].entry.uuid, entry.uuid);
    assert_eq!(requests[0].requested_asset, child);
    assert_eq!(requests[0].output_key, output_key);
    assert_eq!(requests[0].requested_terminal_type, type_id(2));
}

#[test]
fn production_bootstrap_starts_at_the_durable_store_version() {
    let server = Server::new_at_version_with_authoring_backend(
        StoreInstanceId([9; 16]),
        InputVersion(41),
        vec![target_with(7, &[(1, false)])],
        Arc::new(RecordingAuthoringBackend::default()),
    )
    .unwrap();

    assert_eq!(server.current_stamp().version, InputVersion(41));
    assert_eq!(
        snapshot(&connect(&server, &[(1, false)])).stamp().version,
        InputVersion(41)
    );
}

#[derive(Default)]
struct RecordingAuthoringBackend {
    imports: Mutex<Vec<ImportRequest>>,
    reimports: Mutex<Vec<BundleUuid>>,
    operations: Mutex<Vec<LongRunningOp>>,
}

impl AuthoringBackend for RecordingAuthoringBackend {
    fn prepare_import(
        &self,
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

#[derive(Default)]
struct DurableWriteBackend {
    bases: Mutex<Vec<InputVersion>>,
}

impl AuthoringBackend for DurableWriteBackend {
    fn prepare_write(
        &self,
        base: InputVersion,
        _operations: &[AuthoringOp],
        _force_lossy: bool,
    ) -> Result<Option<Commit>, RpcFailure> {
        self.bases.lock().unwrap().push(base);
        Ok(Some(Commit::default()))
    }

    fn prepare_import(
        &self,
        _base: InputVersion,
        _request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("write test does not import")
    }

    fn prepare_reimport(
        &self,
        _base: InputVersion,
        _bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("write test does not reimport")
    }

    fn prepare_operation(
        &self,
        _base: InputVersion,
        _operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        unreachable!("write test does not run operations")
    }
}

#[test]
fn durable_write_backend_owns_the_committed_projection() {
    let backend = Arc::new(DurableWriteBackend::default());
    let server = Server::new_at_version_with_authoring_backend(
        StoreInstanceId([9; 16]),
        InputVersion(8),
        vec![target_with(7, &[(1, false)])],
        backend.clone(),
    )
    .unwrap();
    let hub = connect(&server, &[(1, false)]);

    assert_eq!(
        hub.write(
            InputVersion(8),
            vec![AuthoringOp::Remove { uuid: asset_id(7) }],
            false
        ),
        RpcResult::Success(InputVersion(9))
    );
    assert_eq!(*backend.bases.lock().unwrap(), [InputVersion(8)]);
    assert_eq!(server.current_stamp().version, InputVersion(9));
}

#[test]
fn external_coordinator_cas_runs_publication_only_at_the_exact_server_base() {
    let server = server_with(&[(1, false)]);
    let mut called = false;
    assert_eq!(
        server.coordinated_commit(InputVersion(9), || {
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
        server
            .coordinated_commit(InputVersion(0), || Ok(Commit::default()))
            .unwrap()
            .version,
        InputVersion(1)
    );
}

#[test]
fn coordinator_can_project_daemon_controls_but_hub_cannot_write_them_directly() {
    let server = server_with(&[(1, false)]);
    let mut control = authoring_entry(44, AuthoringEntryRole::AuthoringOnly);
    control.local_id = "$record".to_owned();
    server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(control.clone())],
            ..Commit::default()
        })
        .unwrap();
    let hub = connect(&server, &[(1, false)]);
    assert!(matches!(
        authoring_snapshot(&hub).inspect(control.uuid),
        RpcResult::Success(AuthoringInspectResult::Inspection(_))
    ));
    assert!(matches!(
        hub.write(InputVersion(1), vec![AuthoringOp::Set(control)], false),
        RpcResult::Failure(RpcFailure::InvalidAuthoringRequest { .. })
    ));
    assert_eq!(server.current_stamp().version, InputVersion(1));
}

fn connect(server: &Server, policies: &[(u8, bool)]) -> Hub {
    match server.root().connect(request_for(7, 1, policies)) {
        ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("expected connection, got {other:?}"),
    }
}

fn snapshot(hub: &Hub) -> Snapshot {
    match hub.snapshot() {
        RpcResult::Success(snapshot) => snapshot,
        other => panic!("expected snapshot, got {other:?}"),
    }
}

fn set_asset(uuid: AssetUuid, resolution: StoredResolve, delta: AssetDeltaState) -> AssetMutation {
    AssetMutation::Set {
        uuid,
        resolution,
        delta,
    }
}

fn commit_one(server: &Server, mutation: AssetMutation) -> SnapshotStamp {
    server
        .commit(Commit {
            assets: vec![mutation],
            ..Commit::default()
        })
        .unwrap()
}

fn assert_reconnect<T: std::fmt::Debug>(result: RpcResult<T>, reason: ReconnectReason) {
    assert!(matches!(
        result,
        RpcResult::ReconnectRequired { reason: actual } if actual == reason
    ));
}

fn authoring_entry(byte: u8, role: AuthoringEntryRole) -> AuthoringEntry {
    let schema_hash = node_hash(&SchemaNode::Blob).unwrap();
    AuthoringEntry {
        uuid: asset_id(byte),
        bundle: BundleUuid([byte.wrapping_add(1); 16]),
        local_id: format!("entry-{byte}"),
        normalized_path: format!("bundle-{byte}.asset"),
        type_uuid: type_id(byte),
        terminal_type: type_id(byte),
        schema_hash,
        logical_schema: Arc::from(&b"\"blob\""[..]),
        role,
        tags: std::collections::BTreeMap::from([(
            "group".to_owned(),
            Some(format!("group-{byte}")),
        )]),
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
fn tag_queries_fail_when_other_selectors_could_include_a_poisoned_entry() {
    let server = server_with(&[(1, false), (2, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            tag_poisons: Some(BTreeMap::from([(entry.uuid, entry.bundle)])),
            ..Commit::default()
        })
        .unwrap();
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
    let server = server_with(&[(1, false)]);
    let valid = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(valid.clone())],
            ..Commit::default()
        })
        .unwrap();

    let mut malformed = valid.clone();
    malformed.value.canonical_value = Arc::from(&b"0"[..]);
    assert!(matches!(
        server.commit(Commit {
            authoring: vec![AuthoringMutation::Set(malformed)],
            ..Commit::default()
        }),
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
        server.commit(Commit {
            authoring: vec![AuthoringMutation::Set(duplicate)],
            ..Commit::default()
        }),
        Err(AdminError::InvalidAuthoringValue {
            error: AuthoringValueError::DuplicateBlobIndex { index: 0 },
            ..
        })
    ));

    let mut tampered_schema = valid;
    tampered_schema.schema_hash = LogicalHash([0; 32]);
    assert!(matches!(
        server.commit(Commit {
            authoring: vec![AuthoringMutation::Set(tampered_schema)],
            ..Commit::default()
        }),
        Err(AdminError::InvalidAuthoringValue {
            error: AuthoringValueError::LogicalSchemaInvalid(_),
            ..
        })
    ));
}

#[test]
fn authoring_schema_walk_type_checks_primitive_and_reference_leaves() {
    let server = server_with(&[(1, false)]);
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
        server.commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            ..Commit::default()
        }),
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
        server.commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry)],
            ..Commit::default()
        }),
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

fn test_namespace_error() -> NamespaceError {
    NamespaceError::new(
        NamespaceErrorV1::IncompleteSkeleton {
            source: ReadableBundleSource {
                root_name: "assets".to_owned(),
                normalized_path: "broken.asset".to_owned(),
                file_hash: BundleFileHash([4; 32]),
            },
            failure: SkeletonFailureCode::EnvelopeMalformed,
        },
        "broken bundle",
    )
    .unwrap()
}

fn test_pipeline_failure() -> PipelineFailure {
    PipelineFailure::new(
        PipelineFailureCode::PublishedCallbackPanic,
        PipelineFailureOrigin::PublishedRuntime,
        CleanupDisposition::PublishedEpochLeaked,
        "processor callback panicked",
    )
    .unwrap()
}

#[test]
fn metadata_namespace_calls_serve_around_a_namespace_error() {
    let server = server_with(&[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    let error = test_namespace_error();
    server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            paths: vec![PathMutation::Set {
                path: entry.normalized_path.clone(),
                candidates: BTreeSet::from([entry.uuid]),
            }],
            pipeline: Some(PipelineDiagnostic::Failed(test_pipeline_failure())),
            namespace_errors: Some(vec![error.clone()]),
            ..Commit::default()
        })
        .unwrap();
    let connected = server
        .root()
        .metadata(PROTOCOL_VERSION)
        .connected()
        .unwrap();
    let diagnostics = connected.hub.diagnostics().success().unwrap();
    assert_eq!(diagnostics.namespace_errors, vec![error]);
    assert!(matches!(
        diagnostics.pipeline,
        PipelineDiagnostic::Failed(_)
    ));
    let snapshot = connected.hub.snapshot().success().unwrap();
    assert!(matches!(
        snapshot.version(),
        MetadataCall::Success(InputVersion(1))
    ));
    // The error is about one file: the rest of the namespace serves.
    let query = PureMetadataQuery {
        bundle: Some(entry.bundle),
        authored_type: Some(entry.type_uuid),
        role: Some(AuthoringEntryRole::AuthoringOnly),
        normalized_path_prefix: Some("bundle-1".to_owned()),
        ..PureMetadataQuery::default()
    };
    assert_eq!(
        snapshot.query(&query),
        MetadataNamespaceCall::Success(vec![entry.uuid])
    );
    assert_eq!(
        snapshot.entry(entry.uuid).success().unwrap().normalized_path,
        entry.normalized_path
    );
    assert_eq!(
        snapshot.resolve_path("bundle-1.asset"),
        MetadataNamespaceCall::Success(PathResolveResult::Resolved(entry.uuid))
    );
    let authoring = connected.hub.authoring_snapshot().success().unwrap();
    assert!(authoring.inspect(entry.uuid).success().is_some());

    server
        .commit(Commit {
            namespace_errors: Some(vec![]),
            ..Commit::default()
        })
        .unwrap();
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
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    server
        .commit(Commit {
            namespace_errors: Some(vec![test_namespace_error()]),
            ..Commit::default()
        })
        .unwrap();
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
    let server = server_with(&[(1, false)]);
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
    server.install_artifact(hash, payload).unwrap();
    let error = ConfigurationError::from_reason(
        &DscpV1::MalformedConfiguration { file_hash: [4; 32] },
        "invalid staged configuration",
    );
    let stamp = server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            configuration: Some(ConfigurationStatus::Failed(error.clone())),
            ..Commit::default()
        })
        .unwrap();

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
    let server = server_with(&[(1, false), (2, false)]);
    let runtime = authoring_entry(1, AuthoringEntryRole::Runtime);
    let tooling = authoring_entry(2, AuthoringEntryRole::AuthoringOnly);
    let stamp = server
        .commit(Commit {
            assets: vec![
                set_asset(
                    tooling.uuid,
                    StoredResolve::Built {
                        content_hash: content_hash(9),
                    },
                    AssetDeltaState::Changed,
                ),
                set_asset(
                    asset_id(99),
                    StoredResolve::Built {
                        content_hash: content_hash(10),
                    },
                    AssetDeltaState::Changed,
                ),
            ],
            authoring: vec![
                AuthoringMutation::Set(runtime.clone()),
                AuthoringMutation::Set(tooling.clone()),
            ],
            ..Commit::default()
        })
        .unwrap();
    let hub = connect(&server, &[(1, false), (2, false)]);
    let pinned = authoring_snapshot(&hub);

    assert_eq!(pinned.stamp(), stamp);
    assert_eq!(pinned.basis().snapshot, stamp);
    assert_eq!(pinned.version(), RpcResult::Success(stamp.version));
    assert_eq!(
        pinned.query(AssetQuery {
            authoring_only: Some(false),
            ..AssetQuery::default()
        }),
        RpcResult::Success(vec![runtime.uuid])
    );
    assert_eq!(
        pinned.query(AssetQuery {
            authoring_only: Some(true),
            ..AssetQuery::default()
        }),
        RpcResult::Success(vec![tooling.uuid])
    );
    assert_eq!(
        pinned.query(AssetQuery {
            uuid: Some(tooling.uuid),
            bundle_path: Some(tooling.normalized_path.clone()),
            local_id: Some(tooling.local_id.clone()),
            bundle_uuid: Some(tooling.bundle),
            authored_type: Some(tooling.type_uuid),
            terminal_type: Some(tooling.terminal_type),
            tag: Some(TagSelector {
                tag: "group".to_owned(),
                value: Some("group-2".to_owned()),
            }),
            path_prefix: Some("bundle-".to_owned()),
            path_glob: Some("bundle-?.asset".to_owned()),
            authoring_only: Some(true),
        }),
        RpcResult::Success(vec![tooling.uuid])
    );
    match pinned.inspect(tooling.uuid) {
        RpcResult::Success(AuthoringInspectResult::Inspection(inspection)) => {
            assert_eq!(inspection.stamp, stamp);
            assert_eq!(inspection.uuid, tooling.uuid);
            assert_eq!(inspection.role, AuthoringEntryRole::AuthoringOnly);
            assert_eq!(inspection.value, tooling.value);
        }
        other => panic!("expected pinned inspection, got {other:?}"),
    }
    assert_eq!(
        pinned.inspect(asset_id(99)),
        RpcResult::Success(AuthoringInspectResult::RoleIneligible {
            observed: AuthoringEntryRole::Runtime,
        })
    );
    assert_eq!(
        pinned.inspect(asset_id(98)),
        RpcResult::Success(AuthoringInspectResult::Missing)
    );

    let runtime_snapshot = snapshot(&hub);
    assert!(matches!(
        runtime_snapshot.resolve(tooling.uuid),
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
    let server = server_with(&[(1, false)]);
    let first_entry = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    let first_stamp = server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(first_entry.clone())],
            ..Commit::default()
        })
        .unwrap();
    let hub = connect(&server, &[(1, false)]);
    let first = authoring_snapshot(&hub);

    let mut replacement = first_entry.clone();
    replacement.value.canonical_value = Arc::from(&b"{\"$distill_blob\":0}"[..]);
    let second_stamp = server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(replacement.clone())],
            ..Commit::default()
        })
        .unwrap();

    let old = first.inspect(first_entry.uuid).success().unwrap();
    let refreshed = first.refresh().success().unwrap();
    let new = refreshed.inspect(first_entry.uuid).success().unwrap();
    assert_eq!(first.stamp(), first_stamp);
    assert_eq!(refreshed.stamp(), second_stamp);
    assert_eq!(first.basis().snapshot, first_stamp);
    assert_eq!(refreshed.basis().snapshot, second_stamp);
    assert!(
        matches!(old, AuthoringInspectResult::Inspection(value) if value.stamp == first_stamp && value.value == first_entry.value)
    );
    assert!(
        matches!(new, AuthoringInspectResult::Inspection(value) if value.stamp == second_stamp && value.value == replacement.value)
    );
}

#[test]
fn every_authoring_snapshot_method_is_expiry_and_generation_fenced() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
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
    server
        .replace_target(target_with(8, &[(1, false)]))
        .unwrap();
    let reason = ReconnectReason::TargetDefinitionChanged;
    assert_reconnect(hub.authoring_snapshot(), reason);
    assert_reconnect(stale.version(), reason);
    assert_reconnect(stale.query(AssetQuery::default()), reason);
    assert_reconnect(stale.inspect(asset_id(1)), reason);
    assert_reconnect(stale.refresh(), reason);
}

#[test]
fn authoring_inspection_is_a_pinned_pure_metadata_read_under_configuration_error() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    let error = ConfigurationError::from_reason(
        &DscpV1::MalformedConfiguration { file_hash: [9; 32] },
        "invalid staged configuration",
    );
    let failed_stamp = server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            configuration: Some(ConfigurationStatus::Failed(error)),
            ..Commit::default()
        })
        .unwrap();

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
fn reconnect_reason_vocabulary_is_shared_and_complete() {
    assert_ne!(
        ReconnectReason::StoreInstanceChanged,
        ReconnectReason::ProtocolEpochChanged
    );
    assert_ne!(
        ReconnectReason::TargetDefinitionChanged,
        ReconnectReason::StoreInstanceChanged
    );
    assert_ne!(
        ReconnectReason::PipelineEpochChanged,
        ReconnectReason::ProtocolEpochChanged
    );
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
    let server = server_with(&[(1, false)]);
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
    let server = Server::new(
        StoreInstanceId([9; 16]),
        vec![TargetDefinition::new(
            "t\u{e9}st",
            TargetDefinitionHash([7; 32]),
        )],
    )
    .unwrap();

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
    let server = server_with(&[(1, false)]);
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
    server.install_artifact(hash, payload).unwrap();
    let mut unique = BTreeSet::new();
    unique.insert(built);
    let mut ambiguous = BTreeSet::new();
    ambiguous.insert(built);
    ambiguous.insert(failed);
    let stamp = server
        .commit(Commit {
            assets: vec![
                set_asset(
                    built,
                    StoredResolve::Built { content_hash: hash },
                    AssetDeltaState::Changed,
                ),
                set_asset(
                    failed,
                    StoredResolve::Failed {
                        error: "compiler failed".to_owned(),
                    },
                    AssetDeltaState::Changed,
                ),
                set_asset(
                    drifted,
                    StoredResolve::Drifted {
                        input: DriftedInput::File("shader.glsl".to_owned()),
                    },
                    AssetDeltaState::Changed,
                ),
                set_asset(deleted, StoredResolve::Deleted, AssetDeltaState::Deleted),
            ],
            authoring: Vec::new(),
            paths: vec![
                PathMutation::Set {
                    path: "textures/a.asset".to_owned(),
                    candidates: unique,
                },
                PathMutation::Set {
                    path: "ambiguous.asset".to_owned(),
                    candidates: ambiguous,
                },
            ],
            configuration: None,
            ..Commit::default()
        })
        .unwrap();
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
        ResolveResult::Deleted { at: stamp }
    );

    let path = snap.resolve_path("textures/a.asset").success().unwrap();
    assert_eq!(path.basis.snapshot, stamp);
    assert_eq!(path.value, PathResolveResult::Resolved(built));
    assert_eq!(
        snap.resolve_path("missing.asset").success().unwrap().value,
        PathResolveResult::Missing
    );
    assert_eq!(
        snap.resolve_path("ambiguous.asset")
            .success()
            .unwrap()
            .value,
        PathResolveResult::Failed(PathResolveFailure::Ambiguous {
            candidates: vec![built, failed],
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
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let uuid = asset_id(1);
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
        server.install_artifact(hash, payload).unwrap();
    }
    commit_one(
        &server,
        set_asset(
            uuid,
            StoredResolve::Built {
                content_hash: first,
            },
            AssetDeltaState::Changed,
        ),
    );
    let old = snapshot(&hub);
    commit_one(
        &server,
        set_asset(
            uuid,
            StoredResolve::Built {
                content_hash: second,
            },
            AssetDeltaState::Changed,
        ),
    );

    assert_eq!(
        old.resolve(uuid).success().unwrap().value,
        ResolveResult::Built {
            content_hash: first,
        }
    );
    let refreshed = old.refresh().success().unwrap();
    assert_eq!(refreshed.version(), RpcResult::Success(InputVersion(2)));
    assert_eq!(
        refreshed.resolve(uuid).success().unwrap().value,
        ResolveResult::Built {
            content_hash: second,
        }
    );
}

#[test]
fn deleted_result_preserves_the_deleting_stamp_across_later_commits() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let uuid = asset_id(1);
    let deleted_at = commit_one(
        &server,
        set_asset(uuid, StoredResolve::Deleted, AssetDeltaState::Deleted),
    );
    server.commit(Commit::default()).unwrap();
    let snap = snapshot(&hub);
    assert_eq!(snap.version(), RpcResult::Success(InputVersion(2)));
    assert_eq!(
        snap.resolve(uuid).success().unwrap().value,
        ResolveResult::Deleted { at: deleted_at }
    );
}

#[test]
fn configuration_error_is_snapshot_pinned_and_typed_without_blocking_safe_reads() {
    let server = server_with(&[(1, false)]);
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
    server.install_artifact(hash, payload).unwrap();
    let reason = DscpV1::NonLoopbackAddress {
        address: "198.51.100.7:7331".to_owned(),
    };
    let error = ConfigurationError::from_reason(&reason, "daemon.address is not loopback");
    server
        .commit(Commit {
            configuration: Some(ConfigurationStatus::Failed(error.clone())),
            ..Commit::default()
        })
        .unwrap();
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
        hub.subscribe(InputVersion(1), vec![], vec![]),
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
    let server = server_with(&[(1, false)]);
    let failure = test_pipeline_failure();
    server
        .commit(Commit {
            pipeline: Some(PipelineDiagnostic::Failed(failure.clone())),
            ..Commit::default()
        })
        .unwrap();

    assert_eq!(
        server.root().connect(request_for(7, 2, &[(1, false)])),
        ConnectOutcome::PipelineUnavailable(PipelineUnavailableDiagnostic::PipelineFailure(failure))
    );

    server
        .commit(Commit {
            pipeline: Some(PipelineDiagnostic::Ready),
            ..Commit::default()
        })
        .unwrap();
    let connected = match server.root().connect(request_for(7, 3, &[(1, false)])) {
        ConnectOutcome::Connected(connected) => connected,
        other => panic!("expected recovered connection, got {other:?}"),
    };
    assert_eq!(connected.hub.connection_id(), 1);
}

#[test]
fn published_runtime_failure_fences_shared_epoch_without_minting_a_version() {
    let server = server_with(&[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    let stamp = server
        .commit(Commit {
            assets: vec![set_asset(
                entry.uuid,
                StoredResolve::Drifted {
                    input: DriftedInput::Asset(entry.uuid),
                },
                AssetDeltaState::Changed,
            )],
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            ..Commit::default()
        })
        .unwrap();
    let hub = connect(&server, &[(1, false)]);
    let pinned = snapshot(&hub);
    let failure = PipelineFailure::new(
        PipelineFailureCode::PublishedCallbackPanic,
        PipelineFailureOrigin::PublishedRuntime,
        CleanupDisposition::PublishedEpochLeaked,
        "processor callback panicked",
    )
    .unwrap();
    let mut persisted = false;

    server
        .coordinated_runtime_pipeline_failure(failure.clone(), || {
            persisted = true;
            Ok(())
        })
        .unwrap();

    assert!(persisted);
    assert_eq!(server.current_stamp(), stamp);
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
        server.root().connect(request_for(7, 3, &[(1, false)])),
        ConnectOutcome::PipelineUnavailable(PipelineUnavailableDiagnostic::PipelineFailure(
            failure.clone()
        ))
    );
    assert_eq!(
        server
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

#[test]
fn commit_rejects_unauthenticated_dscp_and_noncanonical_typed_pipeline_diagnostics() {
    let server = server_with(&[(1, false)]);
    let before = server.current_stamp();
    let mut error = ConfigurationError::from_reason(
        &DscpV1::MalformedConfiguration { file_hash: [1; 32] },
        "bad configuration",
    );
    error.reason_hash = [2; 32];
    assert!(matches!(
        server.commit(Commit {
            configuration: Some(ConfigurationStatus::Failed(error)),
            ..Commit::default()
        }),
        Err(AdminError::InvalidConfigurationError { .. })
    ));

    let mut tampered = test_pipeline_failure();
    tampered.identity = [0; 32];
    assert!(matches!(
        server.commit(Commit {
            pipeline: Some(PipelineDiagnostic::Failed(tampered)),
            ..Commit::default()
        }),
        Err(AdminError::InvalidPipelineDiagnostic { .. })
    ));
    assert_eq!(server.current_stamp(), before);
}

#[test]
fn initial_subscription_delta_is_cursor_bound_ordered_and_filters_assets_and_paths() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let watched = asset_id(1);
    let ignored = asset_id(2);
    server
        .commit(Commit {
            assets: vec![set_asset(
                watched,
                StoredResolve::Failed {
                    error: "first".to_owned(),
                },
                AssetDeltaState::Changed,
            )],
            authoring: Vec::new(),
            paths: vec![PathMutation::Set {
                path: "watched.asset".to_owned(),
                candidates: BTreeSet::from([watched]),
            }],
            configuration: None,
            ..Commit::default()
        })
        .unwrap();
    server
        .commit(Commit {
            assets: vec![
                set_asset(watched, StoredResolve::Deleted, AssetDeltaState::Deleted),
                set_asset(ignored, StoredResolve::Deleted, AssetDeltaState::Deleted),
            ],
            authoring: Vec::new(),
            paths: vec![PathMutation::Remove {
                path: "watched.asset".to_owned(),
            }],
            configuration: None,
            ..Commit::default()
        })
        .unwrap();

    let install = hub
        .subscribe(
            InputVersion(0),
            vec![watched],
            vec!["watched.asset".to_owned()],
        )
        .success()
        .unwrap();
    assert_eq!(install.installed, InputVersion(2));
    let first = install.deltas.next().unwrap();
    assert_eq!(first.basis().snapshot.version, InputVersion(2));
    match first {
        StreamEvent::InitialDelta {
            since,
            installed,
            deltas,
            ..
        } => {
            assert_eq!(since, InputVersion(0));
            assert_eq!(installed, InputVersion(2));
            assert_eq!(deltas.len(), 2);
            assert_eq!(deltas[0].basis.snapshot.version, InputVersion(1));
            assert_eq!(deltas[0].assets, vec![(watched, AssetDeltaState::Changed)]);
            assert_eq!(deltas[0].paths, vec!["watched.asset"]);
            assert_eq!(deltas[1].basis.snapshot.version, InputVersion(2));
            assert_eq!(deltas[1].assets, vec![(watched, AssetDeltaState::Deleted)]);
            assert_eq!(deltas[1].paths, vec!["watched.asset"]);
        }
        other => panic!("expected initial delta, got {other:?}"),
    }
    assert!(install.deltas.next().is_none());
}

#[test]
fn live_delta_types_changed_deleted_and_restored_and_every_event_has_a_basis() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let uuid = asset_id(1);
    let install = hub
        .subscribe(InputVersion(0), vec![uuid], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::InitialDelta { .. })
    ));

    for (version, delta) in [
        AssetDeltaState::Changed,
        AssetDeltaState::Deleted,
        AssetDeltaState::Restored,
    ]
    .into_iter()
    .enumerate()
    {
        commit_one(&server, set_asset(uuid, StoredResolve::Deleted, delta));
        let event = install.deltas.next().unwrap();
        assert_eq!(
            event.basis().snapshot.version,
            InputVersion(version as u64 + 1)
        );
        assert!(matches!(
            event,
            StreamEvent::Delta(Delta { assets, .. }) if assets == vec![(uuid, delta)]
        ));
    }
}

#[test]
fn repeated_subscribe_unions_names_on_one_stream_and_unsubscribe_removes_them() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let first = asset_id(1);
    let second = asset_id(2);
    let first_install = hub
        .subscribe(InputVersion(0), vec![first], vec![])
        .success()
        .unwrap();
    first_install.deltas.next().unwrap();
    commit_one(
        &server,
        set_asset(second, StoredResolve::Deleted, AssetDeltaState::Deleted),
    );
    assert!(first_install.deltas.next().is_none());

    let second_install = hub
        .subscribe(InputVersion(0), vec![second], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        first_install.deltas.next(),
        Some(StreamEvent::Delta(Delta { basis, assets, .. }))
            if basis.snapshot.version == InputVersion(1)
                && assets == vec![(second, AssetDeltaState::Deleted)]
    ));
    assert!(second_install.deltas.next().is_none());

    assert_eq!(hub.unsubscribe(vec![first], vec![]), RpcResult::Success(()));
    server
        .commit(Commit {
            assets: vec![
                set_asset(first, StoredResolve::Deleted, AssetDeltaState::Deleted),
                set_asset(second, StoredResolve::Deleted, AssetDeltaState::Restored),
            ],
            ..Commit::default()
        })
        .unwrap();
    assert!(matches!(
        first_install.deltas.next(),
        Some(StreamEvent::Delta(Delta { assets, .. }))
            if assets == vec![(second, AssetDeltaState::Restored)]
    ));
}

#[test]
fn subscription_asset_and_path_sets_have_typed_cardinality_limits() {
    let server = server_with(&[(1, false)]);
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
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    commit_one(
        &server,
        set_asset(
            asset_id(1),
            StoredResolve::Deleted,
            AssetDeltaState::Deleted,
        ),
    );
    commit_one(
        &server,
        set_asset(
            asset_id(1),
            StoredResolve::Deleted,
            AssetDeltaState::Restored,
        ),
    );
    server.discard_history_before(InputVersion(1));

    let install = hub
        .subscribe(InputVersion(0), vec![asset_id(1)], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::ResyncRequired {
            oldest_available: InputVersion(1),
            ..
        })
    ));
    assert!(matches!(
        hub.subscribe(InputVersion(99), vec![], vec![]),
        RpcResult::Failure(RpcFailure::InvalidCursor {
            since: InputVersion(99),
            current: InputVersion(2),
        })
    ));
}

#[test]
fn slow_subscription_queue_is_bounded_by_a_resync_marker() {
    let server = server_with(&[(1, false)]);
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

    for version in 1..=1025 {
        commit_one(
            &server,
            set_asset(
                watched,
                StoredResolve::Failed {
                    error: version.to_string(),
                },
                AssetDeltaState::Changed,
            ),
        );
    }

    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::ResyncRequired {
            oldest_available: InputVersion(1025),
            ..
        })
    ));
    assert!(install.deltas.next().is_none());
}

#[test]
fn restart_required_names_sorted_unique_keys_without_advancing_version() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();
    let before = server.current_stamp();
    let after = server.restart_required(vec![
        "daemon.state_path".to_owned(),
        "daemon.address".to_owned(),
        "daemon.address".to_owned(),
    ]);
    assert_eq!(before, after);
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
fn pending_restart_state_is_queued_after_the_cursor_bound_first_message() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    server.restart_required(vec!["daemon.address".to_owned()]);
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
fn restart_required_replaces_the_prior_pending_key_set() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    server.restart_required(vec!["daemon.address".to_owned()]);
    server.restart_required(vec!["codegen.auto_codegen".to_owned()]);
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
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let uuid = asset_id(1);
    let install = hub
        .subscribe(InputVersion(0), vec![uuid], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();
    drop(hub);
    commit_one(
        &server,
        set_asset(uuid, StoredResolve::Deleted, AssetDeltaState::Deleted),
    );
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::Delta(Delta { assets, .. }))
            if assets == vec![(uuid, AssetDeltaState::Deleted)]
    ));
}

#[test]
fn target_definition_change_fences_every_target_bound_method_and_prompts_stream() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let snap = snapshot(&hub);
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();
    server
        .replace_target(target_with(8, &[(1, false)]))
        .unwrap();

    match install.deltas.next().unwrap() {
        StreamEvent::Asset {
            event: AssetEvent::ReconnectRequired { reason },
            ..
        } => assert_eq!(reason, ReconnectReason::TargetDefinitionChanged),
        other => panic!("expected reconnect event, got {other:?}"),
    }
    let reason = ReconnectReason::TargetDefinitionChanged;
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
}

#[test]
fn pipeline_epoch_change_fences_every_target_bound_capability_and_prompts_stream() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let snapshot = snapshot(&hub);
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();

    server
        .commit(Commit {
            pipeline: Some(PipelineDiagnostic::Ready),
            pipeline_epoch_changed: true,
            ..Commit::default()
        })
        .unwrap();

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
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    server
        .commit(Commit {
            pipeline: Some(PipelineDiagnostic::Ready),
            ..Commit::default()
        })
        .unwrap();
    assert!(matches!(hub.snapshot(), RpcResult::Success(_)));
}

#[test]
fn hub_authoring_and_wire_tree_surface_is_versioned_typed_and_generation_first() {
    let backend = Arc::new(RecordingAuthoringBackend::default());
    let server = Server::new_with_authoring_backend(
        StoreInstanceId([9; 16]),
        vec![target_with(7, &[(1, false)])],
        backend.clone(),
    )
    .unwrap();
    let hub = connect(&server, &[(1, false)]);
    let mut entry = authoring_entry(21, AuthoringEntryRole::Runtime);
    entry.type_uuid = type_id(1);
    entry.terminal_type = type_id(1);
    assert_eq!(
        hub.write(InputVersion(0), vec![AuthoringOp::Set(entry.clone())], false),
        RpcResult::Success(InputVersion(1))
    );
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
        hub.import(InputVersion(1), import_request.clone()),
        RpcResult::Success(BundleUuid([70; 16]))
    );
    assert_eq!(
        hub.reimport(InputVersion(2), BundleUuid([70; 16])),
        RpcResult::Success(BundleUuid([70; 16]))
    );
    let operation = LongRunningOp::Doctor(Arc::from(&b"verify-cas"[..]));
    let progress = hub
        .operation(InputVersion(3), operation.clone())
        .success()
        .unwrap();
    let events = progress.collect::<Vec<_>>();
    assert_eq!(events.len(), 3);
    assert_eq!(events[1].state, AuthoringProgressState::Running);
    assert_eq!(&*events[1].payload, b"verify-cas");
    assert_eq!(events[2].state, AuthoringProgressState::Completed);
    assert_eq!(server.current_stamp().version, InputVersion(4));
    let cancelled_operation = LongRunningOp::Doctor(Arc::from(&b"cancel-me"[..]));
    let mut cancellable = hub
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
    assert_eq!(server.current_stamp().version, InputVersion(4));
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
    server.install_wire_tree(hash, tree.clone()).unwrap();
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
    server
        .install_artifact(wire_artifact_hash, wire_artifact)
        .unwrap();
    commit_one(
        &server,
        set_asset(
            asset_id(1),
            StoredResolve::Built {
                content_hash: wire_artifact_hash,
            },
            AssetDeltaState::Changed,
        ),
    );
    assert_eq!(hub.wire_tree(hash), RpcResult::Success(tree));

    let snapshot = snapshot(&hub);
    let authoring = authoring_snapshot(&hub);
    server
        .replace_target(target_with(8, &[(2, false)]))
        .unwrap();
    let reconnect = ReconnectReason::TargetDefinitionChanged;
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

    for request in [DoctorRequest::Verify, DoctorRequest::RebuildIndexes] {
        assert_eq!(DoctorRequest::decode(&request.encode()).unwrap(), request);
    }
    assert_eq!(
        DoctorRequest::decode(&[1, 99]),
        Err(OperationPayloadError::InvalidTag(99))
    );
    // Tag 2 was the retired displaced-inode clean.
    assert_eq!(
        DoctorRequest::decode(&[1, 2]),
        Err(OperationPayloadError::InvalidTag(2))
    );
}

#[test]
fn missing_authoring_backend_is_typed_and_never_advances_the_input_version() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
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
        hub.import(InputVersion(0), request),
        RpcResult::Failure(RpcFailure::AuthoringBackendUnavailable {
            operation: "import".to_owned(),
        })
    );
    assert_eq!(server.current_stamp().version, InputVersion(0));
}

#[test]
fn fence_is_the_guarantee_even_if_reconnect_event_is_not_consumed() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let snap = snapshot(&hub);
    let _unpolled = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    server
        .replace_target(target_with(8, &[(1, false)]))
        .unwrap();
    assert_reconnect(
        snap.resolve(asset_id(1)),
        ReconnectReason::TargetDefinitionChanged,
    );
}

#[test]
fn expired_and_foreign_snapshots_fail_without_serving_data() {
    let server = server_with(&[(1, false)]);
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
    let server = server_with(&[(1, false)]);
    let before = server.current_stamp();
    assert!(matches!(
        server.commit(Commit {
            assets: vec![
                set_asset(
                    asset_id(1),
                    StoredResolve::Deleted,
                    AssetDeltaState::Deleted
                ),
                set_asset(
                    asset_id(1),
                    StoredResolve::Deleted,
                    AssetDeltaState::Deleted
                ),
            ],
            ..Commit::default()
        }),
        Err(AdminError::DuplicateAssetMutation { .. })
    ));
    assert!(matches!(
        server.commit(Commit {
            paths: vec![PathMutation::Remove {
                path: "../escape".to_owned(),
            }],
            ..Commit::default()
        }),
        Err(AdminError::InvalidPath { .. })
    ));
    assert!(matches!(
        server.commit(Commit {
            paths: vec![PathMutation::Remove {
                path: "cafe\u{301}.asset".to_owned(),
            }],
            ..Commit::default()
        }),
        Err(AdminError::InvalidPath { .. })
    ));
    assert!(matches!(
        server.commit(Commit {
            paths: vec![PathMutation::Remove {
                path: "nul\0.asset".to_owned(),
            }],
            ..Commit::default()
        }),
        Err(AdminError::InvalidPath { .. })
    ));
    assert_eq!(
        server.commit(Commit {
            paths: vec![PathMutation::Set {
                path: "empty.asset".to_owned(),
                candidates: BTreeSet::new(),
            }],
            ..Commit::default()
        }),
        Err(AdminError::EmptyPathCandidates {
            path: "empty.asset".to_owned()
        })
    );
    assert_eq!(server.current_stamp(), before);
}

#[test]
fn authoring_identity_validation_rejects_reserved_local_ids_and_noncanonical_tags_atomically() {
    let server = server_with(&[(1, false)]);
    let before = server.current_stamp();
    let mut reserved = authoring_entry(1, AuthoringEntryRole::Runtime);
    reserved.local_id = "$generated".to_owned();
    assert!(matches!(
        server.commit(Commit {
            authoring: vec![AuthoringMutation::Set(reserved)],
            ..Commit::default()
        }),
        Err(AdminError::InvalidAuthoringIdentity { .. })
    ));
    let mut bad_tag = authoring_entry(1, AuthoringEntryRole::Runtime);
    bad_tag.tags = std::collections::BTreeMap::from([("bad\0tag".to_owned(), None)]);
    assert!(matches!(
        server.commit(Commit {
            authoring: vec![AuthoringMutation::Set(bad_tag)],
            ..Commit::default()
        }),
        Err(AdminError::InvalidAuthoringIdentity { .. })
    ));
    assert_eq!(server.current_stamp(), before);
}

#[test]
fn wire_tree_coverage_keeps_pinned_compatible_artifact_references() {
    let server = server_with(&[(1, false), (2, false)]);
    let hub = connect(&server, &[(1, false)]);
    let node = distill_wire::wire::WireNode::Unit { offset: 0 };
    let tree: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&node).unwrap());
    let layout_hash = distill_wire::dswl::dswl_hash(&node).unwrap();
    server.install_wire_tree(layout_hash, tree.clone()).unwrap();
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
    server
        .install_artifact(historical_hash, historical)
        .unwrap();
    server.install_artifact(current_hash, current).unwrap();
    commit_one(
        &server,
        set_asset(
            asset,
            StoredResolve::Built {
                content_hash: historical_hash,
            },
            AssetDeltaState::Changed,
        ),
    );
    commit_one(
        &server,
        set_asset(
            asset,
            StoredResolve::Built {
                content_hash: current_hash,
            },
            AssetDeltaState::Changed,
        ),
    );

    assert_eq!(hub.wire_tree(layout_hash), RpcResult::Success(tree.clone()));

    commit_one(
        &server,
        AssetMutation::Remove {
            uuid: asset,
            delta: AssetDeltaState::Deleted,
        },
    );
    assert_eq!(hub.wire_tree(layout_hash), RpcResult::Success(tree));
}

#[test]
fn artifact_install_authenticates_header_hash_and_direct_typed_load_edges() {
    let server = server_with(&[(1, false), (2, false)]);
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
    let mut omitted_dep = payload.clone();
    omitted_dep.load_edges.clear();
    assert!(matches!(
        server.install_artifact(hash, omitted_dep),
        Err(AdminError::InvalidArtifact { .. })
    ));
    assert!(matches!(
        server.install_artifact(content_hash(99), payload.clone()),
        Err(AdminError::InvalidArtifact { .. })
    ));

    server.install_artifact(hash, payload).unwrap();
    commit_one(
        &server,
        set_asset(
            asset_id(1),
            StoredResolve::Built { content_hash: hash },
            AssetDeltaState::Changed,
        ),
    );
    let hub = connect(&server, &[(1, false), (2, false)]);
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

#[test]
fn content_hash_records_are_immutable() {
    let server = server_with(&[(1, false)]);
    let (hash, first) = canonical_artifact(
        asset_id(1),
        type_id(1),
        type_id(1),
        type_id(1),
        LayoutHash([1; 32]),
        vec![ServedLoadEdge {
            asset: asset_id(2),
            expected_terminal: type_id(2),
        }],
        vec![1],
        Vec::new(),
    );
    assert_eq!(server.install_artifact(hash, first.clone()), Ok(()));
    assert_eq!(server.install_artifact(hash, first.clone()), Ok(()));
    let mut different_edges = first;
    different_edges.load_edges[0].expected_terminal = type_id(3);
    assert_eq!(
        server.install_artifact(hash, different_edges),
        Err(AdminError::ArtifactAlreadyExistsWithDifferentPayload { hash })
    );
}

#[test]
fn coordinated_target_set_replacement_advances_once_and_fences_changed_or_removed_hubs() {
    let server = server_with(&[(1, false)]);
    let changed = connect(&server, &[(1, false)]);
    let stamp = server
        .coordinated_replace_target_set(
            InputVersion(0),
            vec![target_with(8, &[(1, false)])],
            || Ok(Commit::default()),
        )
        .unwrap();
    assert_eq!(stamp.version, InputVersion(1));
    assert_reconnect(changed.snapshot(), ReconnectReason::TargetDefinitionChanged);

    let replacement = match server.root().connect(request_for(8, 1, &[(1, false)])) {
        ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("expected replacement connection, got {other:?}"),
    };
    let stamp = server
        .coordinated_replace_target_set(InputVersion(1), Vec::new(), || Ok(Commit::default()))
        .unwrap();
    assert_eq!(stamp.version, InputVersion(2));
    assert_reconnect(
        replacement.snapshot(),
        ReconnectReason::TargetDefinitionChanged,
    );
}
