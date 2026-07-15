use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use distill_core::attestation::{
    AttestationError, ReferenceStrength, RegistryExtraFact, RegistryExtraRow, RegistryExtrasV1,
    RegistryPathStep, SchemaNodeId,
};
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
            encoded_type,
            terminal_type,
            closure_rows: vec![ServedClosureRow {
                asset,
                content_hash: hash,
                authored_type,
                encoded_type,
                terminal_type,
                load_edges,
            }],
        },
    )
}

fn target_hash(byte: u8) -> TargetDefinitionHash {
    TargetDefinitionHash([byte; 32])
}

fn compiled(byte: u8, build_only: bool) -> CompiledTypeRow {
    CompiledTypeRow::new(
        type_id(byte),
        LogicalHash([byte + 10; 32]),
        [byte + 20; 32],
        build_only,
        RegistryExtrasV1::canonical(vec![
            RegistryExtraRow {
                node: SchemaNodeId(0),
                path: vec![],
                fact: RegistryExtraFact::BuildOnly(build_only),
            },
            RegistryExtraRow {
                node: SchemaNodeId(0),
                path: vec![RegistryPathStep::Field("ref".into())],
                fact: RegistryExtraFact::Reference {
                    strength: ReferenceStrength::Strong,
                    target: type_id(byte + 30),
                },
            },
        ])
        .unwrap(),
    )
    .unwrap()
}

fn policy(byte: u8, build_only: bool) -> LoadPolicyEntry {
    LoadPolicyEntry {
        type_uuid: type_id(byte),
        build_only,
    }
}

fn bootstrap_rows() -> Vec<CompiledTypeRow> {
    distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1()
        .unwrap()
        .rows()
        .to_vec()
}

fn boundary(policies: &[(u8, bool)]) -> (Vec<CompiledTypeRow>, Vec<LoadPolicyEntry>) {
    let mut compiled_rows = policies
        .iter()
        .map(|(byte, build_only)| compiled(*byte, *build_only))
        .collect::<Vec<_>>();
    compiled_rows.extend(bootstrap_rows());
    let mut policy_rows = policies
        .iter()
        .map(|(byte, build_only)| policy(*byte, *build_only))
        .collect::<Vec<_>>();
    policy_rows.extend(bootstrap_rows().into_iter().map(|row| LoadPolicyEntry {
        type_uuid: row.type_uuid,
        build_only: true,
    }));
    (compiled_rows, policy_rows)
}

fn target_with(definition_hash: u8, policies: &[(u8, bool)]) -> TargetDefinition {
    let (compiled_rows, policy_rows) = boundary(policies);
    TargetDefinition::canonical(
        "dev",
        target_hash(definition_hash),
        compiled_rows,
        policy_rows,
    )
    .unwrap()
}

fn request_for(definition_hash: u8, epoch: u64, policies: &[(u8, bool)]) -> ConnectRequest {
    let (compiled_rows, policy_rows) = boundary(policies);
    ConnectRequest::canonical(
        GameModuleEpoch(epoch),
        "dev",
        target_hash(definition_hash),
        compiled_rows,
        policy_rows,
    )
    .unwrap()
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

struct RecordingArtifactLeases {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl ArtifactLeaseBackend for RecordingArtifactLeases {
    fn pin_lease(&self, _holder: u64, _hashes: &[[u8; 32]]) -> Result<(), String> {
        self.events.lock().unwrap().push("pin");
        Ok(())
    }

    fn release_lease(&self, _holder: u64) {
        self.events.lock().unwrap().push("release");
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
fn resolve_pins_before_build_release_and_snapshot_clones_share_one_lease() {
    let server = server_with(&[(1, false)]);
    let events = Arc::new(Mutex::new(Vec::new()));
    server.install_build_backend(Arc::new(LifecycleBuildBackend {
        events: Arc::clone(&events),
    }));
    server.install_artifact_lease_backend(Arc::new(RecordingArtifactLeases {
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
    assert_eq!(
        &events.lock().unwrap()[..2],
        &["pin", "finish"],
        "the caller lease must be durable before the in-flight pin is released"
    );
    drop(snapshot);
    assert!(!events.lock().unwrap().contains(&"release"));
    clone.expire_lease();
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "release")
            .count(),
        1
    );
    drop(clone);
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "release")
            .count(),
        1,
        "expiry and final drop release a shared lease only once"
    );
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
    widen_lineage_publication: bool,
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
            LongRunningOp::RenameWithFixups(payload)
            | LongRunningOp::DiskMigration(payload)
            | LongRunningOp::Doctor(payload) => payload.clone(),
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

    fn prepare_resolve_duplicate_lineage(
        &self,
        _basis: &LineageRepairInspection,
        _survivor: &LineageManifestClaimant,
    ) -> Result<Commit, LineageRepairBackendError> {
        Ok(Commit {
            assets: self
                .widen_lineage_publication
                .then_some(AssetMutation::Remove {
                    uuid: AssetUuid([99; 16]),
                    delta: AssetDeltaState::Changed,
                })
                .into_iter()
                .collect(),
            configuration: Some(ConfigurationStatus::Ready),
            lineage_repair: Some(None),
            ..Commit::default()
        })
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
            vec![AuthoringOp::Remove { uuid: asset_id(7) }]
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
        hub.write(InputVersion(1), vec![AuthoringOp::Set(control)]),
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

fn assert_expansion<T: std::fmt::Debug>(
    result: RpcResult<T>,
    snapshot: SnapshotStamp,
    required: &[TypeUuid],
) {
    assert!(matches!(
        result,
        RpcResult::AttestationExpansionRequired(AttestationExpansionRequired {
            snapshot: actual_snapshot,
            closure_identity,
            required: actual_required,
        }) if actual_snapshot == snapshot
            && closure_identity != [0; 32]
            && actual_required == required
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

fn test_version_poison() -> VersionPoison {
    VersionPoison::new(
        VersionPoisonV1::IncompleteSkeleton {
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

#[test]
fn metadata_capabilities_are_poison_safe_but_namespace_calls_return_exact_version_poison() {
    let server = server_with(&[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    let poison = test_version_poison();
    server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            paths: vec![PathMutation::Set {
                path: entry.normalized_path.clone(),
                candidates: BTreeSet::from([entry.uuid]),
            }],
            pipeline: Some(PipelineDiagnostic::SchemaAcceptanceRequired(
                SchemaAcceptanceRequired {
                    manifest: SchemaManifestBasis {
                        manifest_hash: content_hash(41),
                        current_cursors: BTreeMap::new(),
                    },
                    candidate: PipelineCandidateIdentity {
                        dylib_hash: [42; 32],
                        target_set: distill_core::target_set::CanonicalTargetSet::canonical(vec![])
                            .unwrap(),
                    },
                    mismatches: vec![SchemaRegistryMismatch {
                        type_uuid: type_id(1),
                        candidate: Some(LogicalHash([45; 32])),
                        manifest: None,
                    }],
                },
            )),
            version_poison: Some(Some(poison.clone())),
            ..Commit::default()
        })
        .unwrap();
    let connected = server
        .root()
        .metadata(PROTOCOL_VERSION)
        .connected()
        .unwrap();
    let diagnostics = connected.hub.diagnostics().success().unwrap();
    assert_eq!(diagnostics.version_poison, Some(poison.clone()));
    assert!(matches!(
        diagnostics.pipeline,
        PipelineDiagnostic::SchemaAcceptanceRequired(_)
    ));
    let snapshot = connected.hub.snapshot().success().unwrap();
    assert!(matches!(
        snapshot.version(),
        MetadataCall::Success(InputVersion(1))
    ));
    assert_eq!(
        snapshot.query(&PureMetadataQuery::default()),
        MetadataNamespaceCall::VersionPoisoned(poison.clone())
    );
    assert_eq!(
        snapshot.entry(entry.uuid),
        MetadataNamespaceCall::VersionPoisoned(poison.clone())
    );
    assert_eq!(
        snapshot.resolve_path(&entry.normalized_path),
        MetadataNamespaceCall::VersionPoisoned(poison.clone())
    );
    let authoring = connected.hub.authoring_snapshot().success().unwrap();
    assert_eq!(
        authoring.inspect(entry.uuid),
        MetadataNamespaceCall::VersionPoisoned(poison)
    );

    server
        .commit(Commit {
            version_poison: Some(None),
            ..Commit::default()
        })
        .unwrap();
    let healed = snapshot.refresh().success().unwrap();
    let query = PureMetadataQuery {
        bundle: Some(entry.bundle),
        authored_type: Some(entry.type_uuid),
        role: Some(AuthoringEntryRole::AuthoringOnly),
        normalized_path_prefix: Some("bundle-1".to_owned()),
        ..PureMetadataQuery::default()
    };
    assert_eq!(
        healed.query(&query),
        MetadataNamespaceCall::Success(vec![entry.uuid])
    );
    assert_eq!(
        healed.entry(entry.uuid).success().unwrap().normalized_path,
        entry.normalized_path
    );
    assert_eq!(
        healed.resolve_path("bundle-1.asset"),
        MetadataNamespaceCall::Success(PathResolveResult::Resolved(entry.uuid))
    );
}

#[test]
fn target_snapshot_namespace_calls_return_the_exact_pinned_version_poison() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let poison = test_version_poison();
    server
        .commit(Commit {
            version_poison: Some(Some(poison.clone())),
            ..Commit::default()
        })
        .unwrap();
    let snapshot = snapshot(&hub);
    let authoring = authoring_snapshot(&hub);
    assert_eq!(
        snapshot.query(AssetQuery {
            uuid: Some(asset_id(1)),
            ..AssetQuery::default()
        }),
        RpcResult::VersionPoisoned(poison.clone())
    );
    assert_eq!(
        snapshot.entry(asset_id(1)),
        RpcResult::VersionPoisoned(poison.clone())
    );
    assert_eq!(
        snapshot.resolve(asset_id(1)),
        RpcResult::VersionPoisoned(poison.clone())
    );
    assert_eq!(
        snapshot.resolve_path("a.asset"),
        RpcResult::VersionPoisoned(poison.clone())
    );
    assert_eq!(
        authoring.query(AssetQuery {
            uuid: Some(asset_id(1)),
            ..AssetQuery::default()
        }),
        RpcResult::VersionPoisoned(poison.clone())
    );
    assert_eq!(
        authoring.inspect(asset_id(1)),
        RpcResult::VersionPoisoned(poison)
    );
}

#[test]
fn unbound_metadata_bootstrap_survives_poison_and_has_no_runtime_surface() {
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
    let poison = ConfigurationPoison::from_reason(
        &DscpV1::MalformedConfiguration { file_hash: [4; 32] },
        "invalid staged configuration",
    );
    let stamp = server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            configuration: Some(ConfigurationStatus::Poisoned(poison.clone())),
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
        ConfigurationStatus::Poisoned(poison.clone())
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
fn daemon_compiled_projection_fences_only_the_accepted_uuid_set() {
    let server = server_with(&[(1, false)]);
    let request = request_for(7, 1, &[(1, false)]);
    let connected = match server.root().connect(request.clone()) {
        ConnectOutcome::Connected(connected) => connected,
        other => panic!("expected connect, got {other:?}"),
    };
    assert_eq!(connected.daemon_compiled_projection, request.dsca);
    assert_eq!(
        snapshot(&connected.hub).basis().daemon_compiled_projection,
        request.dsca
    );

    server
        .replace_target(target_with(7, &[(1, false), (2, false)]))
        .unwrap();
    assert!(matches!(connected.hub.snapshot(), RpcResult::Success(_)));

    let (mut changed_rows, policies) = boundary(&[(1, false), (2, false)]);
    changed_rows
        .iter_mut()
        .find(|row| row.type_uuid == type_id(1))
        .unwrap()
        .native_layout_digest = [99; 32];
    let replacement =
        TargetDefinition::canonical("dev", target_hash(7), changed_rows, policies).unwrap();
    server.replace_target(replacement).unwrap();
    assert_reconnect(
        connected.hub.snapshot(),
        ReconnectReason::CompiledAttestationChanged,
    );
}

#[test]
fn removing_an_accepted_daemon_type_returns_compiled_reconnect_without_panicking() {
    let server = server_with(&[(1, false), (2, false)]);
    let hub = connect(&server, &[(1, false)]);
    let snapshot = snapshot(&hub);
    let install = hub
        .subscribe(InputVersion(0), vec![], vec![])
        .success()
        .unwrap();
    install.deltas.next().unwrap();

    server
        .replace_target(target_with(7, &[(2, false)]))
        .unwrap();
    assert_reconnect(
        snapshot.query(AssetQuery {
            uuid: Some(asset_id(1)),
            ..AssetQuery::default()
        }),
        ReconnectReason::CompiledAttestationChanged,
    );
    assert_reconnect(hub.snapshot(), ReconnectReason::CompiledAttestationChanged);
    assert!(matches!(
        install.deltas.next(),
        Some(StreamEvent::Asset {
            event: AssetEvent::ReconnectRequired {
                reason: ReconnectReason::CompiledAttestationChanged
            },
            ..
        })
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
    assert_eq!(pinned.basis().target_generation, 0);
    assert_eq!(pinned.basis().policy_generation, 0);
    assert_eq!(pinned.basis().attestation_generation, 0);
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
    assert_eq!(
        refreshed.basis().target_generation,
        first.basis().target_generation
    );
    assert_eq!(
        refreshed.basis().policy_generation,
        first.basis().policy_generation
    );
    assert_eq!(
        refreshed.basis().attestation_generation,
        first.basis().attestation_generation
    );
    assert!(
        matches!(old, AuthoringInspectResult::Inspection(value) if value.stamp == first_stamp && value.value == first_entry.value)
    );
    assert!(
        matches!(new, AuthoringInspectResult::Inspection(value) if value.stamp == second_stamp && value.value == replacement.value)
    );
}

#[test]
fn every_authoring_snapshot_method_is_lease_and_generation_fenced() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let expired = authoring_snapshot(&hub);
    expired.expire_lease();
    assert_eq!(
        expired.version(),
        RpcResult::Failure(RpcFailure::LeaseExpired)
    );
    assert_eq!(
        expired.query(AssetQuery::default()),
        RpcResult::Failure(RpcFailure::LeaseExpired)
    );
    assert_eq!(
        expired.inspect(asset_id(1)),
        RpcResult::Failure(RpcFailure::LeaseExpired)
    );
    assert!(matches!(
        expired.refresh(),
        RpcResult::Failure(RpcFailure::LeaseExpired)
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
fn authoring_inspection_is_a_pinned_pure_metadata_read_under_configuration_poison() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let entry = authoring_entry(1, AuthoringEntryRole::AuthoringOnly);
    let poison = ConfigurationPoison::from_reason(
        &DscpV1::MalformedConfiguration { file_hash: [9; 32] },
        "invalid staged configuration",
    );
    let poisoned_stamp = server
        .commit(Commit {
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            configuration: Some(ConfigurationStatus::Poisoned(poison)),
            ..Commit::default()
        })
        .unwrap();

    let pinned = authoring_snapshot(&hub);
    assert_eq!(pinned.stamp(), poisoned_stamp);
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
            if value.stamp == poisoned_stamp
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
        ReconnectReason::LoadPolicyChanged
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
fn dsca_and_dslp_match_the_shared_typed_grammars() {
    let compiled = vec![compiled(1, false), compiled(2, true)];
    let policies = vec![policy(1, false), policy(2, true)];

    let mut dslp = blake3::Hasher::new();
    dslp.update(b"DSLP");
    dslp.update(&[1]);
    dslp.update(&2_u32.to_le_bytes());
    for row in &policies {
        dslp.update(&row.type_uuid.0);
        dslp.update(&[u8::from(row.build_only)]);
    }

    assert_eq!(
        CompiledTypeTable::canonical(compiled.clone())
            .unwrap()
            .digest,
        distill_core::attestation::compute_compiled_attestation_digest(&compiled).unwrap()
    );
    assert_eq!(
        compute_policy_digest(&policies).unwrap(),
        *dslp.finalize().as_bytes()
    );
}

#[test]
fn canonical_attestations_reject_duplicates_and_different_registered_sets() {
    assert!(matches!(
        ConnectRequest::canonical(
            GameModuleEpoch(1),
            "dev",
            target_hash(7),
            vec![compiled(1, false), compiled(1, false)],
            vec![policy(1, false), policy(1, false)],
        ),
        Err(AttestationShapeError::Compiled(
            AttestationError::DuplicateType(_)
        ))
    ));
    let (compiled_rows, _) = boundary(&[(1, false)]);
    let (_, policy_rows) = boundary(&[(2, false)]);
    assert!(matches!(
        ConnectRequest::canonical(
            GameModuleEpoch(1),
            "dev",
            target_hash(7),
            compiled_rows,
            policy_rows,
        ),
        Err(AttestationShapeError::RegisteredTypeSetMismatch { .. })
    ));
}

#[test]
fn connect_accepts_a_client_subset_and_binds_its_policy_to_every_basis() {
    let server = server_with(&[(1, false), (2, true)]);
    let hub = connect(&server, &[(1, false)]);
    let snap = snapshot(&hub);
    let (_, mut expected_policy) = boundary(&[(1, false)]);
    expected_policy.sort_by_key(|row| row.type_uuid);

    assert_eq!(snap.stamp().instance, StoreInstanceId([9; 16]));
    assert_eq!(snap.basis().load_policy.rows, expected_policy);
    assert_eq!(
        snap.basis().load_policy.digest,
        compute_policy_digest(&snap.basis().load_policy.rows).unwrap()
    );
    assert_eq!(snap.basis().target_generation, 0);
    assert_eq!(snap.basis().policy_generation, 0);
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
fn connect_rejects_unsorted_forged_and_mismatched_compiled_attestations() {
    let server = server_with(&[(1, false), (2, true)]);
    let mut unsorted = request_for(7, 1, &[(1, false), (2, true)]);
    unsorted.compiled_registry.reverse();
    assert!(matches!(
        server.root().connect(unsorted),
        ConnectOutcome::Rejected(ConnectError::AttestationShape(
            AttestationShapeError::Compiled(AttestationError::TypeRowsNotStrictlySorted)
        ))
    ));

    let mut forged = request_for(7, 1, &[(1, false)]);
    forged.dsca.0 = [55; 32];
    assert!(matches!(
        server.root().connect(forged),
        ConnectOutcome::Rejected(ConnectError::CompiledRegistryAggregateMismatch {
            observed,
            ..
        }) if observed == [55; 32]
    ));

    let mut mismatch = request_for(7, 1, &[(1, false)]);
    mismatch.compiled_registry[0] = CompiledTypeRow::new(
        type_id(1),
        LogicalHash([11; 32]),
        [99; 32],
        false,
        mismatch.compiled_registry[0].registry_extras.clone(),
    )
    .unwrap();
    mismatch.dsca = CompiledTypeTable::canonical(mismatch.compiled_registry.clone())
        .unwrap()
        .digest;
    assert!(matches!(
        server.root().connect(mismatch),
        ConnectOutcome::Rejected(ConnectError::LogicalHashMismatch { type_uuid, .. })
            | ConnectOutcome::Rejected(ConnectError::NativeLayoutMismatch { type_uuid, .. })
            | ConnectOutcome::Rejected(ConnectError::RegistryExtrasMismatch { type_uuid, .. })
            if type_uuid == type_id(1)
    ));

    assert!(matches!(
        server.root().connect(request_for(7, 1, &[(3, false)])),
        ConnectOutcome::Rejected(ConnectError::MissingCompiledType { type_uuid })
            if type_uuid == type_id(3)
    ));
}

#[test]
fn connect_rejects_forged_and_mismatched_load_policy_attestations() {
    let server = server_with(&[(1, false)]);
    let mut forged = request_for(7, 1, &[(1, false)]);
    forged.policy_digest = [77; 32];
    assert!(matches!(
        server.root().connect(forged),
        ConnectOutcome::Rejected(ConnectError::AttestationShape(
            AttestationShapeError::PolicyDigestMismatch { .. }
        ))
    ));

    assert!(matches!(
        server.root().connect(request_for(7, 1, &[(1, true)])),
        ConnectOutcome::Rejected(ConnectError::CompiledBuildOnlyMismatch { type_uuid, .. })
            if type_uuid == type_id(1)
    ));
}

#[test]
fn connect_classifies_cross_projection_set_and_build_bit_differences_exactly() {
    let server = server_with(&[(1, false)]);

    let mut missing_policy = request_for(7, 1, &[(1, false)]);
    missing_policy
        .load_policy
        .retain(|row| row.type_uuid != type_id(1));
    missing_policy.policy_digest = compute_policy_digest(&missing_policy.load_policy).unwrap();
    assert!(matches!(
        server.root().connect(missing_policy),
        ConnectOutcome::Rejected(ConnectError::MissingLoadPolicy { type_uuid })
            if type_uuid == type_id(1)
    ));

    let mut missing_compiled = request_for(7, 1, &[(1, false)]);
    missing_compiled
        .compiled_registry
        .retain(|row| row.type_uuid != type_id(1));
    missing_compiled.dsca =
        CompiledTypeTable::canonical(missing_compiled.compiled_registry.clone())
            .unwrap()
            .digest;
    assert!(matches!(
        server.root().connect(missing_compiled),
        ConnectOutcome::Rejected(ConnectError::MissingCompiledType { type_uuid })
            if type_uuid == type_id(1)
    ));

    let mut bit_mismatch = request_for(7, 1, &[(1, false)]);
    bit_mismatch
        .load_policy
        .iter_mut()
        .find(|row| row.type_uuid == type_id(1))
        .unwrap()
        .build_only = true;
    bit_mismatch.policy_digest = compute_policy_digest(&bit_mismatch.load_policy).unwrap();
    assert!(matches!(
        server.root().connect(bit_mismatch),
        ConnectOutcome::Rejected(ConnectError::LoadPolicyMismatch {
            type_uuid,
            expected: false,
            got: true,
        }) if type_uuid == type_id(1)
    ));
}

#[test]
fn equal_dsnl_cannot_hide_logical_reference_or_extras_drift() {
    let server = server_with(&[(1, false)]);
    let base = compiled(1, false);
    let variants = [
        CompiledTypeRow::new(
            base.type_uuid,
            LogicalHash([99; 32]),
            base.native_layout_digest,
            base.build_only,
            base.registry_extras.clone(),
        )
        .unwrap(),
        CompiledTypeRow::new(
            base.type_uuid,
            base.logical_hash,
            base.native_layout_digest,
            base.build_only,
            RegistryExtrasV1::canonical(vec![
                RegistryExtraRow {
                    node: SchemaNodeId(0),
                    path: vec![],
                    fact: RegistryExtraFact::BuildOnly(false),
                },
                RegistryExtraRow {
                    node: SchemaNodeId(0),
                    path: vec![RegistryPathStep::Field("ref".into())],
                    fact: RegistryExtraFact::Reference {
                        strength: ReferenceStrength::Weak,
                        target: type_id(42),
                    },
                },
            ])
            .unwrap(),
        )
        .unwrap(),
        CompiledTypeRow::new(
            base.type_uuid,
            base.logical_hash,
            base.native_layout_digest,
            base.build_only,
            RegistryExtrasV1::canonical(vec![RegistryExtraRow {
                node: SchemaNodeId(0),
                path: vec![],
                fact: RegistryExtraFact::BuildOnly(false),
            }])
            .unwrap(),
        )
        .unwrap(),
    ];

    for changed in variants {
        assert_eq!(changed.native_layout_digest, base.native_layout_digest);
        let mut request = request_for(7, 1, &[(1, false)]);
        let runtime = request
            .compiled_registry
            .iter()
            .position(|row| row.type_uuid == type_id(1))
            .unwrap();
        request.compiled_registry[runtime] = changed;
        request.dsca = CompiledTypeTable::canonical(request.compiled_registry.clone())
            .unwrap()
            .digest;
        assert!(matches!(
            server.root().connect(request),
            ConnectOutcome::Rejected(ConnectError::LogicalHashMismatch { type_uuid, .. })
                | ConnectOutcome::Rejected(ConnectError::RegistryExtrasMismatch { type_uuid, .. })
                if type_uuid == type_id(1)
        ));
    }
}

#[test]
fn reattest_rechecks_the_entire_identity_and_requires_a_successor_epoch() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let old_snapshot = snapshot(&hub);

    let same_epoch = ReattestRequest::from(request_for(7, 1, &[(1, false)]));
    assert!(matches!(
        hub.reattest(same_epoch),
        RpcResult::Failure(RpcFailure::EpochNotSuccessor { .. })
    ));

    let bad_definition = ReattestRequest::from(request_for(8, 2, &[(1, false)]));
    assert!(matches!(
        hub.reattest(bad_definition),
        RpcResult::Failure(RpcFailure::Attestation(
            ConnectError::TargetDefinitionMismatch { .. }
        ))
    ));

    assert_eq!(
        hub.reattest(ReattestRequest::from(request_for(7, 2, &[(1, false)]))),
        RpcResult::Success(ReattestSuccess {
            installed_attestation_generation: 1,
            daemon_compiled_projection: request_for(7, 2, &[(1, false)]).dsca,
            load_policy: Arc::new(LoadPolicyAttestation {
                rows: request_for(7, 2, &[(1, false)]).load_policy,
                digest: request_for(7, 2, &[(1, false)]).policy_digest,
            }),
            policy_generation: 0,
        })
    );
    assert!(matches!(
        old_snapshot.resolve(asset_id(1)),
        RpcResult::Failure(RpcFailure::ClientEpochChanged {
            snapshot: GameModuleEpoch(1),
            current: GameModuleEpoch(2),
        })
    ));
    assert!(matches!(old_snapshot.refresh(), RpcResult::Success(_)));
}

#[test]
fn connect_and_reattest_share_stable_typed_attestation_failure_subjects() {
    let type_uuid = type_id(1);
    let cases = [
        (
            ConnectError::MissingCompiledType { type_uuid },
            AttestationFailureCode::MissingType,
            AttestationSubject::SpecificType {
                type_uuid,
                projection: AttestationProjection::CompiledRegistry,
            },
        ),
        (
            ConnectError::LogicalHashMismatch {
                type_uuid,
                expected: [1; 32],
                observed: [2; 32],
            },
            AttestationFailureCode::LogicalHashMismatch,
            AttestationSubject::SpecificType {
                type_uuid,
                projection: AttestationProjection::CompiledRegistry,
            },
        ),
        (
            ConnectError::MissingLoadPolicy { type_uuid },
            AttestationFailureCode::MissingType,
            AttestationSubject::SpecificType {
                type_uuid,
                projection: AttestationProjection::Policy,
            },
        ),
        (
            ConnectError::LoadPolicyMismatch {
                type_uuid,
                expected: false,
                got: true,
            },
            AttestationFailureCode::BuildOnlyMismatch,
            AttestationSubject::SpecificType {
                type_uuid,
                projection: AttestationProjection::Policy,
            },
        ),
        (
            ConnectError::TargetDefinitionMismatch {
                expected: target_hash(7),
                got: target_hash(8),
            },
            AttestationFailureCode::TargetDefinitionMismatch,
            AttestationSubject::TargetDefinition,
        ),
        (
            ConnectError::AttestationShape(AttestationShapeError::Compiled(
                AttestationError::TrailingBytes,
            )),
            AttestationFailureCode::MalformedTable,
            AttestationSubject::CompiledRegistryTable,
        ),
        (
            ConnectError::AttestationShape(AttestationShapeError::Compiled(
                AttestationError::CompiledDigestMismatch,
            )),
            AttestationFailureCode::CompiledRegistryAggregateMismatch,
            AttestationSubject::CompiledRegistryAggregate,
        ),
        (
            ConnectError::AttestationShape(AttestationShapeError::PolicyDigestMismatch {
                expected: [1; 32],
                got: [2; 32],
            }),
            AttestationFailureCode::PolicyProjectionMismatch,
            AttestationSubject::PolicyProjection,
        ),
    ];
    for (error, code, subject) in cases {
        let failure = error.attestation_failure().unwrap();
        assert_eq!(failure.code(), code);
        assert_eq!(failure.subject(), &subject);
        assert!(!failure.message().is_empty());
    }
    assert!(ConnectError::UnknownTarget {
        target: "missing".into()
    }
    .attestation_failure()
    .is_none());
}

#[test]
fn connect_returns_all_basis_generations_and_reattest_is_generation_cas() {
    let server = server_with(&[(1, false)]);
    let connected = match server.root().connect(request_for(7, 1, &[(1, false)])) {
        ConnectOutcome::Connected(connected) => connected,
        other => panic!("expected connection, got {other:?}"),
    };
    assert_eq!(connected.instance, StoreInstanceId([9; 16]));
    assert_eq!(connected.policy_generation, 0);
    assert_eq!(connected.target_generation, 0);
    assert_eq!(connected.attestation_generation, 0);
    assert_eq!(
        snapshot(&connected.hub).basis().attestation_generation,
        connected.attestation_generation
    );

    let barrier = Arc::new(std::sync::Barrier::new(3));
    let run = |epoch| {
        let hub = connected.hub.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let mut request = ReattestRequest::from(request_for(7, epoch, &[(1, false)]));
            request.base_attestation_generation = 0;
            request.successor_attestation_generation = 1;
            barrier.wait();
            hub.reattest(request)
        })
    };
    let first = run(2);
    let second = run(3);
    barrier.wait();
    let outcomes = [first.join().unwrap(), second.join().unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                RpcResult::Success(ReattestSuccess {
                    installed_attestation_generation: 1,
                    ..
                })
            ))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                RpcResult::Failure(RpcFailure::StaleAttestationBase {
                    expected: 1,
                    got: 0
                })
            ))
            .count(),
        1
    );
    assert_eq!(snapshot(&connected.hub).basis().attestation_generation, 1);

    let mut successor = ReattestRequest::from(request_for(7, 4, &[(1, false)]));
    successor.base_attestation_generation = 1;
    successor.successor_attestation_generation = 2;
    assert_eq!(
        connected.hub.reattest(successor),
        RpcResult::Success(ReattestSuccess {
            installed_attestation_generation: 2,
            daemon_compiled_projection: request_for(7, 4, &[(1, false)]).dsca,
            load_policy: Arc::new(LoadPolicyAttestation {
                rows: request_for(7, 4, &[(1, false)]).load_policy,
                digest: request_for(7, 4, &[(1, false)]).policy_digest,
            }),
            policy_generation: 0,
        })
    );
    assert_eq!(snapshot(&connected.hub).basis().attestation_generation, 2);
}

#[test]
fn reattest_echo_rotates_the_basis_and_discards_old_generation_events() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let old_snapshot = snapshot(&hub);
    let install = hub
        .subscribe(InputVersion(0), vec![asset_id(1)], vec![])
        .success()
        .unwrap();
    install.deltas.next().expect("initial install event");
    server
        .commit(Commit {
            assets: vec![AssetMutation::Remove {
                uuid: asset_id(1),
                delta: AssetDeltaState::Changed,
            }],
            ..Commit::default()
        })
        .unwrap();

    let success = hub
        .reattest(ReattestRequest::from(request_for(7, 2, &[(1, false)])))
        .success()
        .unwrap();
    assert_eq!(success.installed_attestation_generation, 1);
    assert_eq!(
        snapshot(&hub).basis().attestation_generation,
        success.installed_attestation_generation
    );
    assert!(matches!(
        old_snapshot.resolve(asset_id(1)),
        RpcResult::Failure(RpcFailure::ClientEpochChanged { .. })
    ));
    let replacement = install
        .deltas
        .next()
        .expect("old queued delta is replaced by a successor-basis resync");
    assert!(matches!(replacement, StreamEvent::ResyncRequired { .. }));
    assert_eq!(
        replacement.basis().attestation_generation,
        success.installed_attestation_generation
    );
    assert!(install.deltas.next().is_none());
}

#[test]
fn reattest_rejects_nonconsecutive_and_overflowing_generations_without_mutation() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let mut skipped = ReattestRequest::from(request_for(7, 2, &[(1, false)]));
    skipped.successor_attestation_generation = 2;
    assert_eq!(
        hub.reattest(skipped),
        RpcResult::Failure(RpcFailure::InvalidAttestationSuccessor {
            base: 0,
            successor: 2,
        })
    );
    assert_eq!(snapshot(&hub).basis().attestation_generation, 0);

    let mut overflow = ReattestRequest::from(request_for(7, 2, &[(1, false)]));
    overflow.base_attestation_generation = u64::MAX;
    overflow.successor_attestation_generation = 0;
    assert!(matches!(
        hub.reattest(overflow),
        RpcResult::Failure(RpcFailure::AttestationGenerationOverflow { base: u64::MAX })
    ));
    assert_eq!(snapshot(&hub).basis().attestation_generation, 0);
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
fn configuration_poison_is_snapshot_pinned_and_typed_without_blocking_safe_reads() {
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
    let poison = ConfigurationPoison::from_reason(&reason, "daemon.address is not loopback");
    server
        .commit(Commit {
            configuration: Some(ConfigurationStatus::Poisoned(poison.clone())),
            ..Commit::default()
        })
        .unwrap();
    let poisoned = snapshot(&hub);

    assert_eq!(
        poisoned.configuration(),
        RpcResult::Success(ConfigurationStatus::Poisoned(poison.clone()))
    );
    assert_eq!(
        poisoned.resolve(asset_id(1)),
        RpcResult::ConfigurationPoisoned(poison.clone())
    );
    assert!(matches!(
        poisoned.resolve_path("a.asset"),
        RpcResult::Success(_)
    ));
    assert!(matches!(hub.fetch(&poisoned, hash), RpcResult::Success(_)));
    assert!(matches!(poisoned.refresh(), RpcResult::Success(_)));
    assert!(matches!(
        hub.subscribe(InputVersion(1), vec![], vec![]),
        RpcResult::Success(_)
    ));
    assert_eq!(
        hub.reattest(ReattestRequest::from(request_for(7, 2, &[(1, false)]))),
        RpcResult::ConfigurationPoisoned(poison.clone())
    );
    assert_eq!(
        server.root().connect(request_for(7, 3, &[(1, false)])),
        ConnectOutcome::ConfigurationPoisoned(poison.clone())
    );

    let same_reason_new_words = ConfigurationPoison::from_reason(&reason, "translated diagnostic");
    assert_eq!(poison.reason_hash, same_reason_new_words.reason_hash);
    assert_ne!(poison.message, same_reason_new_words.message);
}

#[test]
fn connect_returns_typed_pipeline_unavailable_without_minting_a_hub() {
    let server = server_with(&[(1, false)]);
    let required = SchemaAcceptanceRequired {
        manifest: SchemaManifestBasis {
            manifest_hash: content_hash(41),
            current_cursors: BTreeMap::new(),
        },
        candidate: PipelineCandidateIdentity {
            dylib_hash: [42; 32],
            target_set: distill_core::target_set::CanonicalTargetSet::canonical(vec![]).unwrap(),
        },
        mismatches: vec![SchemaRegistryMismatch {
            type_uuid: type_id(1),
            candidate: Some(LogicalHash([45; 32])),
            manifest: None,
        }],
    };
    server
        .commit(Commit {
            pipeline: Some(PipelineDiagnostic::SchemaAcceptanceRequired(
                required.clone(),
            )),
            ..Commit::default()
        })
        .unwrap();

    assert_eq!(
        server.root().connect(request_for(7, 2, &[(1, false)])),
        ConnectOutcome::PipelineUnavailable(
            PipelineUnavailableDiagnostic::SchemaAcceptanceRequired(required)
        )
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
fn published_runtime_poison_fences_shared_epoch_without_minting_a_version() {
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
    let poison = PipelinePoison::new(
        PipelinePoisonCode::PublishedCallbackPanic,
        PipelinePoisonOrigin::PublishedRuntime,
        CleanupDisposition::PublishedEpochLeaked,
        "processor callback panicked",
    )
    .unwrap();
    let unavailable = RpcFailure::PipelineUnavailable(Box::new(
        PipelineUnavailableDiagnostic::PipelinePoison(poison.clone()),
    ));
    let mut persisted = false;

    server
        .coordinated_runtime_pipeline_poison(poison.clone(), || {
            persisted = true;
            Ok(())
        })
        .unwrap();

    assert!(persisted);
    assert_eq!(server.current_stamp(), stamp);
    assert_eq!(pinned.version(), RpcResult::Success(stamp.version));
    assert!(matches!(
        pinned.query(AssetQuery {
            uuid: Some(entry.uuid),
            ..AssetQuery::default()
        }),
        RpcResult::Success(_)
    ));
    assert_eq!(
        pinned.query(AssetQuery {
            terminal_type: Some(entry.terminal_type),
            ..AssetQuery::default()
        }),
        RpcResult::Failure(unavailable.clone())
    );
    assert_eq!(
        pinned.entry(entry.uuid),
        RpcResult::Failure(unavailable.clone())
    );
    assert_eq!(
        pinned.resolve(entry.uuid),
        RpcResult::Failure(unavailable.clone())
    );
    assert_eq!(
        hub.write(stamp.version, Vec::new()),
        RpcResult::Failure(unavailable.clone())
    );
    assert_eq!(
        hub.reattest(ReattestRequest::from(request_for(7, 2, &[(1, false)]))),
        RpcResult::Failure(unavailable)
    );
    assert_eq!(
        server.root().connect(request_for(7, 3, &[(1, false)])),
        ConnectOutcome::PipelineUnavailable(PipelineUnavailableDiagnostic::PipelinePoison(
            poison.clone()
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
        PipelineDiagnostic::Poisoned(poison)
    );
}

#[test]
fn commit_rejects_unauthenticated_dscp_and_noncanonical_typed_pipeline_diagnostics() {
    let server = server_with(&[(1, false)]);
    let before = server.current_stamp();
    let mut poison = ConfigurationPoison::from_reason(
        &DscpV1::MalformedConfiguration { file_hash: [1; 32] },
        "bad configuration",
    );
    poison.reason_hash = [2; 32];
    assert!(matches!(
        server.commit(Commit {
            configuration: Some(ConfigurationStatus::Poisoned(poison)),
            ..Commit::default()
        }),
        Err(AdminError::InvalidConfigurationPoison { .. })
    ));

    let empty_schema = PipelineDiagnostic::SchemaAcceptanceRequired(SchemaAcceptanceRequired {
        manifest: SchemaManifestBasis {
            manifest_hash: content_hash(3),
            current_cursors: BTreeMap::new(),
        },
        candidate: PipelineCandidateIdentity {
            dylib_hash: [4; 32],
            target_set: distill_core::target_set::CanonicalTargetSet::canonical(vec![]).unwrap(),
        },
        mismatches: vec![],
    });
    assert!(matches!(
        server.commit(Commit {
            pipeline: Some(empty_schema),
            ..Commit::default()
        }),
        Err(AdminError::InvalidPipelineDiagnostic { .. })
    ));

    let empty_retired = PipelineDiagnostic::RetiredTypeReferenced(RetiredTypeReferenced {
        manifest_hash: BundleFileHash([7; 32]),
        basis: before,
        type_uuid: type_id(1),
        references: vec![],
    });
    assert!(matches!(
        server.commit(Commit {
            pipeline: Some(empty_retired),
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
    match first_install.deltas.next().unwrap() {
        StreamEvent::InitialDelta { deltas, .. } => {
            assert_eq!(deltas.len(), 1);
            assert_eq!(deltas[0].assets, vec![(second, AssetDeltaState::Deleted)]);
        }
        other => panic!("expected initial delta, got {other:?}"),
    }
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
    assert_reconnect(hub.fetch_latest(content_hash(1)), reason);
    assert_reconnect(hub.subscribe(InputVersion(0), vec![], vec![]), reason);
    assert_reconnect(hub.unsubscribe(vec![], vec![]), reason);
    assert_reconnect(
        hub.reattest(ReattestRequest::from(request_for(8, 2, &[(1, false)]))),
        reason,
    );
    assert_eq!(hub.attestation_generation(), 0);

    assert_reconnect(snap.version(), reason);
    assert_reconnect(snap.configuration(), reason);
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
        hub.write(InputVersion(0), vec![AuthoringOp::Set(entry.clone())]),
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
            vec![AuthoringOp::Remove { uuid: entry.uuid }]
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
        .replace_target(target_with(7, &[(2, false)]))
        .unwrap();
    let reconnect = ReconnectReason::CompiledAttestationChanged;
    assert_reconnect(hub.write(InputVersion(99), vec![]), reconnect);
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

    let migration = DiskMigrationRequest {
        bundles: vec![BundleUuid([1; 16]), BundleUuid([2; 16])],
    };
    let encoded = migration.encode().unwrap();
    assert_eq!(DiskMigrationRequest::decode(&encoded).unwrap(), migration);
    assert_eq!(
        DiskMigrationRequest {
            bundles: vec![BundleUuid([2; 16]), BundleUuid([1; 16])],
        }
        .encode(),
        Err(OperationPayloadError::NonCanonicalOrder)
    );

    for request in [
        DoctorRequest::Verify,
        DoctorRequest::Clean,
        DoctorRequest::RebuildIndexes,
    ] {
        assert_eq!(DoctorRequest::decode(&request.encode()).unwrap(), request);
    }
    assert_eq!(
        DoctorRequest::decode(&[1, 99]),
        Err(OperationPayloadError::InvalidTag(99))
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
fn store_and_protocol_components_of_the_reattest_fence_never_mutate_generation() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);

    server.replace_store_instance(StoreInstanceId([10; 16]));
    assert_reconnect(
        hub.reattest(ReattestRequest::from(request_for(7, 2, &[(1, false)]))),
        ReconnectReason::StoreInstanceChanged,
    );
    assert_eq!(hub.attestation_generation(), 0);
    server.replace_store_instance(StoreInstanceId([9; 16]));
    assert_eq!(snapshot(&hub).basis().attestation_generation, 0);

    server.replace_protocol_epoch(PROTOCOL_VERSION + 1);
    assert_reconnect(
        hub.reattest(ReattestRequest::from(request_for(7, 2, &[(1, false)]))),
        ReconnectReason::ProtocolEpochChanged,
    );
    assert_eq!(hub.attestation_generation(), 0);
    server.replace_protocol_epoch(PROTOCOL_VERSION);
    assert_eq!(snapshot(&hub).basis().attestation_generation, 0);
}

#[test]
fn load_policy_change_has_its_own_fence_reason_and_target_reason_wins_if_both_change() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let snap = snapshot(&hub);
    server.replace_target(target_with(7, &[(1, true)])).unwrap();
    assert_reconnect(
        snap.resolve(asset_id(1)),
        ReconnectReason::LoadPolicyChanged,
    );
    assert_reconnect(
        hub.reattest(ReattestRequest::from(request_for(7, 2, &[(1, true)]))),
        ReconnectReason::LoadPolicyChanged,
    );
    assert_eq!(hub.attestation_generation(), 0);

    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let snap = snapshot(&hub);
    server.replace_target(target_with(8, &[(1, true)])).unwrap();
    assert_reconnect(
        snap.resolve(asset_id(1)),
        ReconnectReason::TargetDefinitionChanged,
    );
}

#[test]
fn policy_generation_and_live_fence_ignore_daemon_rows_outside_the_hub_projection() {
    let server = server_with(&[(1, false), (2, false)]);
    let first = match server.root().connect(request_for(7, 1, &[(1, false)])) {
        ConnectOutcome::Connected(connected) => connected,
        other => panic!("expected subset connection, got {other:?}"),
    };
    let pinned = snapshot(&first.hub);
    assert_eq!(first.policy_generation, 0);
    server
        .replace_target(target_with(7, &[(1, false), (2, true)]))
        .unwrap();
    assert_eq!(pinned.version(), RpcResult::Success(InputVersion(0)));
    assert_eq!(pinned.basis().policy_generation, 0);

    let fresh = match server.root().connect(request_for(7, 2, &[(1, false)])) {
        ConnectOutcome::Connected(connected) => connected,
        other => panic!("expected fresh subset connection, got {other:?}"),
    };
    assert_eq!(fresh.policy_generation, 0);
    assert_eq!(snapshot(&fresh.hub).basis().policy_generation, 0);
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

    first.expire_lease();
    assert!(matches!(
        first.resolve(asset_id(1)),
        RpcResult::Failure(RpcFailure::LeaseExpired)
    ));
    assert!(matches!(
        first.resolve_path("a.asset"),
        RpcResult::Failure(RpcFailure::LeaseExpired)
    ));
    assert!(matches!(
        first.fetch(content_hash(1)),
        RpcResult::Failure(RpcFailure::LeaseExpired)
    ));
    assert!(matches!(
        first.refresh(),
        RpcResult::Failure(RpcFailure::LeaseExpired)
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
fn target_bound_data_coverage_never_serves_runtime_types_outside_the_accepted_set() {
    let server = server_with(&[(1, false), (2, false)]);
    let hub = connect(&server, &[(1, false)]);
    let (dependency_hash, dependency_payload) = canonical_artifact(
        asset_id(2),
        type_id(2),
        type_id(2),
        type_id(2),
        LayoutHash([42; 32]),
        Vec::new(),
        vec![4, 2],
        Vec::new(),
    );
    let dependency_row = dependency_payload.closure_rows[0].clone();
    server
        .install_artifact(dependency_hash, dependency_payload)
        .unwrap();
    let (hash, mut payload) = canonical_artifact(
        asset_id(1),
        type_id(1),
        type_id(1),
        type_id(1),
        LayoutHash([41; 32]),
        vec![ServedLoadEdge {
            asset: asset_id(2),
            expected_terminal: type_id(2),
        }],
        vec![4, 1],
        Vec::new(),
    );
    payload.closure_rows.push(dependency_row);
    server.install_artifact(hash, payload.clone()).unwrap();
    let mut entry = authoring_entry(1, AuthoringEntryRole::Runtime);
    entry.terminal_type = type_id(1);
    server
        .commit(Commit {
            assets: vec![set_asset(
                entry.uuid,
                StoredResolve::Built { content_hash: hash },
                AssetDeltaState::Changed,
            )],
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            paths: vec![PathMutation::Set {
                path: entry.normalized_path.clone(),
                candidates: BTreeSet::from([entry.uuid]),
            }],
            ..Commit::default()
        })
        .unwrap();
    let snap = snapshot(&hub);

    assert!(matches!(
        snap.query(AssetQuery {
            terminal_type: Some(type_id(2)),
            ..AssetQuery::default()
        }),
        RpcResult::Failure(RpcFailure::InvalidQuery { .. })
    ));
    assert!(matches!(hub.snapshot(), RpcResult::Success(_)));
    assert_expansion(snap.entry(entry.uuid), snap.stamp(), &[type_id(2)]);
    assert_expansion(snap.resolve(entry.uuid), snap.stamp(), &[type_id(2)]);
    assert!(matches!(
        snap.resolve_path(&entry.normalized_path),
        RpcResult::Success(TerminalEvent {
            value: PathResolveResult::Resolved(uuid),
            ..
        }) if uuid == entry.uuid
    ));
    assert_expansion(snap.fetch(hash), snap.stamp(), &[type_id(2)]);
    assert_expansion(hub.fetch(&snap, hash), snap.stamp(), &[type_id(2)]);
    assert_expansion(
        hub.fetch_latest(hash),
        server.current_stamp(),
        &[type_id(2)],
    );

    let artifact_only_uuid = asset_id(42);
    let (artifact_only_hash, artifact_only_payload) = canonical_artifact(
        artifact_only_uuid,
        type_id(2),
        type_id(2),
        type_id(2),
        LayoutHash([43; 32]),
        Vec::new(),
        vec![4, 2],
        Vec::new(),
    );
    server
        .install_artifact(artifact_only_hash, artifact_only_payload)
        .unwrap();
    commit_one(
        &server,
        set_asset(
            artifact_only_uuid,
            StoredResolve::Built {
                content_hash: artifact_only_hash,
            },
            AssetDeltaState::Changed,
        ),
    );
    let latest = snapshot(&hub);
    assert_expansion(
        latest.resolve(artifact_only_uuid),
        latest.stamp(),
        &[type_id(2)],
    );

    let expanded_request = ReattestRequest::from(request_for(7, 1, &[(1, false), (2, false)]));
    assert!(matches!(
        hub.reattest(expanded_request),
        RpcResult::Success(ReattestSuccess {
            installed_attestation_generation: 1,
            policy_generation: 1,
            ..
        })
    ));
    assert_eq!(
        snap.fetch(hash),
        RpcResult::Failure(RpcFailure::StaleAttestationBase {
            expected: 1,
            got: 0,
        })
    );
    assert_eq!(
        latest.resolve(artifact_only_uuid),
        RpcResult::Failure(RpcFailure::StaleAttestationBase {
            expected: 1,
            got: 0,
        })
    );
    let expanded = snapshot(&hub);
    assert!(matches!(expanded.fetch(hash), RpcResult::Success(_)));
    assert!(matches!(
        expanded.resolve(artifact_only_uuid),
        RpcResult::Success(TerminalEvent {
            value: ResolveResult::Built { .. },
            ..
        })
    ));

    let metadata = server
        .root()
        .metadata(PROTOCOL_VERSION)
        .connected()
        .unwrap();
    assert!(matches!(metadata.hub.fetch(hash), MetadataCall::Success(_)));
    assert!(!payload.structural.is_empty());
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
fn artifact_install_and_serving_authenticate_header_hash_and_complete_load_closure() {
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
    let mut forged_header = payload.clone();
    forged_header.encoded_type = type_id(1);
    forged_header.terminal_type = type_id(1);
    forged_header.closure_rows[0].authored_type = type_id(1);
    forged_header.closure_rows[0].encoded_type = type_id(1);
    forged_header.closure_rows[0].terminal_type = type_id(1);
    assert!(matches!(
        server.install_artifact(hash, forged_header),
        Err(AdminError::InvalidArtifact { .. })
    ));
    let mut omitted_dep = payload.clone();
    omitted_dep.closure_rows[0].load_edges.clear();
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
    assert!(matches!(
        snapshot(&hub).fetch(hash),
        RpcResult::Failure(RpcFailure::InvalidQuery { .. })
    ));
}

#[test]
fn lineage_repair_is_a_narrow_exact_basis_capability_with_typed_cas_outcomes() {
    let backend = Arc::new(RecordingAuthoringBackend::default());
    let server = Server::new_with_authoring_backend(
        StoreInstanceId([9; 16]),
        vec![target_with(7, &[(1, false)])],
        backend,
    )
    .unwrap();
    assert!(matches!(
        server.root().lineage_repair(PROTOCOL_VERSION),
        LineageRepairConnectOutcome::Unavailable(LineageRepairUnavailable::ConfigurationReady)
    ));
    assert!(matches!(
        server.root().lineage_repair(PROTOCOL_VERSION + 1),
        LineageRepairConnectOutcome::ProtocolMismatch {
            expected: PROTOCOL_VERSION,
            observed,
        } if observed == PROTOCOL_VERSION + 1
    ));

    let claimants = vec![
        LineageManifestClaimant {
            root_name: "assets".to_owned(),
            normalized_path: "a.bundle".to_owned(),
            bundle: BundleUuid([1; 16]),
            local_id: "manifest-a".to_owned(),
            asset: AssetUuid([1; 16]),
            file_hash: BundleFileHash([1; 32]),
        },
        LineageManifestClaimant {
            root_name: "assets".to_owned(),
            normalized_path: "b.bundle".to_owned(),
            bundle: BundleUuid([2; 16]),
            local_id: "manifest-b".to_owned(),
            asset: AssetUuid([2; 16]),
            file_hash: BundleFileHash([2; 32]),
        },
    ];
    let poison = ConfigurationPoison::from_reason(
        &DscpV1::DuplicateLineageManifest {
            entries: claimants.clone(),
        },
        "two lineage manifests",
    );
    let poisoned_stamp = server
        .commit(Commit {
            configuration: Some(ConfigurationStatus::Poisoned(poison)),
            lineage_repair: Some(Some(LineageRepairState::Duplicate {
                claimants: claimants.clone(),
            })),
            ..Commit::default()
        })
        .unwrap();
    let repair = match server.root().lineage_repair(PROTOCOL_VERSION) {
        LineageRepairConnectOutcome::Connected(connected) => connected.repair,
        other => panic!("expected lineage repair capability, got {other:?}"),
    };
    let first = match repair.inspect() {
        LineageRepairInspectOutcome::Success(inspection) => inspection,
        other => panic!("expected exact repair inspection, got {other:?}"),
    };
    assert_eq!(first.instance, StoreInstanceId([9; 16]));
    assert_eq!(first.stamp, poisoned_stamp);
    assert_eq!(
        repair.resolve_duplicate(
            first.clone(),
            LineageManifestClaimant {
                asset: AssetUuid([9; 16]),
                ..claimants[0].clone()
            }
        ),
        LineageRepairMutationOutcome::Invalid(LineageRepairInvalid {
            code: LineageRepairInvalidCode::SurvivorNotClaimant,
            message: "selected survivor is not an exact current claimant".to_owned(),
        })
    );

    server.commit(Commit::default()).unwrap();
    assert!(matches!(
        repair.resolve_duplicate(first, claimants[0].clone()),
        LineageRepairMutationOutcome::StaleBasis(LineageRepairStale {
            code: LineageRepairStaleCode::StampChanged,
            ..
        })
    ));
    let current = match repair.inspect() {
        LineageRepairInspectOutcome::Success(inspection) => inspection,
        other => panic!("expected refreshed repair inspection, got {other:?}"),
    };
    let committed = repair.resolve_duplicate(current, claimants[0].clone());
    assert!(matches!(
        committed,
        LineageRepairMutationOutcome::Success(LineageRepairCommitted { stamp })
            if stamp == server.current_stamp()
    ));
    assert!(matches!(
        repair.inspect(),
        LineageRepairInspectOutcome::Unavailable(LineageRepairUnavailable::ConfigurationReady)
    ));
}

#[test]
fn lineage_repair_rejects_backend_publication_outside_the_repair_boundary() {
    let backend = Arc::new(RecordingAuthoringBackend {
        widen_lineage_publication: true,
        ..RecordingAuthoringBackend::default()
    });
    let server = Server::new_with_authoring_backend(
        StoreInstanceId([9; 16]),
        vec![target_with(7, &[(1, false)])],
        backend,
    )
    .unwrap();
    let claimants = vec![
        LineageManifestClaimant {
            root_name: "assets".to_owned(),
            normalized_path: "a.bundle".to_owned(),
            bundle: BundleUuid([1; 16]),
            local_id: "manifest-a".to_owned(),
            asset: AssetUuid([1; 16]),
            file_hash: BundleFileHash([1; 32]),
        },
        LineageManifestClaimant {
            root_name: "assets".to_owned(),
            normalized_path: "b.bundle".to_owned(),
            bundle: BundleUuid([2; 16]),
            local_id: "manifest-b".to_owned(),
            asset: AssetUuid([2; 16]),
            file_hash: BundleFileHash([2; 32]),
        },
    ];
    let poison = ConfigurationPoison::from_reason(
        &DscpV1::DuplicateLineageManifest {
            entries: claimants.clone(),
        },
        "two lineage manifests",
    );
    server
        .commit(Commit {
            configuration: Some(ConfigurationStatus::Poisoned(poison)),
            lineage_repair: Some(Some(LineageRepairState::Duplicate {
                claimants: claimants.clone(),
            })),
            ..Commit::default()
        })
        .unwrap();
    let repair = match server.root().lineage_repair(PROTOCOL_VERSION) {
        LineageRepairConnectOutcome::Connected(connected) => connected.repair,
        other => panic!("expected repair connection, got {other:?}"),
    };
    let basis = match repair.inspect() {
        LineageRepairInspectOutcome::Success(basis) => basis,
        other => panic!("expected repair inspection, got {other:?}"),
    };
    let before = server.current_stamp();
    assert!(matches!(
        repair.resolve_duplicate(basis, claimants[0].clone()),
        LineageRepairMutationOutcome::Invalid(LineageRepairInvalid {
            code: LineageRepairInvalidCode::WrongBasisState,
            ..
        })
    ));
    assert_eq!(server.current_stamp(), before);
    assert!(matches!(
        repair.inspect(),
        LineageRepairInspectOutcome::Success(_)
    ));
}

#[test]
fn target_definition_construction_seals_bootstrap_authority_and_policy_shape() {
    let server = server_with(&[(1, false)]);
    let before = server.current_stamp();
    let valid = target_with(8, &[(1, false)]);
    let mut rows = valid.compiled_registry().to_vec();
    let index = rows
        .iter()
        .position(|row| distill_core::attestation::is_bootstrap_control_type(row.type_uuid))
        .unwrap();
    let row = rows[index].clone();
    rows[index] = CompiledTypeRow::new(
        row.type_uuid,
        row.logical_hash,
        [77; 32],
        row.build_only,
        row.registry_extras,
    )
    .unwrap();
    assert!(matches!(
        TargetDefinition::canonical("dev", target_hash(8), rows, valid.load_policy().to_vec(),),
        Err(AttestationShapeError::Bootstrap(_))
    ));
    assert_eq!(server.current_stamp(), before);
    assert!(matches!(
        TargetDefinition::canonical(
            "dev",
            target_hash(8),
            valid.compiled_registry().to_vec(),
            vec![policy(1, true)],
        ),
        Err(AttestationShapeError::RegisteredTypeSetMismatch { .. })
    ));
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
    let mut different_closure = first;
    different_closure.closure_rows[0].load_edges[0].expected_terminal = type_id(3);
    assert_eq!(
        server.install_artifact(hash, different_closure),
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
