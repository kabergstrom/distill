use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use distill_core::attestation::{
    CompiledTypeRow, RegistryExtraFact, RegistryExtraRow, RegistryExtrasV1, SchemaNodeId,
};
use distill_core::id::BundleFileHash;
use distill_rpc::capnp_loader::{RemoteCall, RemoteHub};
use distill_rpc::capnp_transport::{
    schema, CapnpClient, RemoteConnectOutcome, RemoteMetadataOutcome, StagedListener,
};
use distill_rpc::*;
use distill_schema::ngp_schema::{node_hash, SchemaNode};
use tokio::task::LocalSet;

fn compiled(definition: u8) -> CompiledTypeRow {
    CompiledTypeRow::new(
        TypeUuid([1; 16]),
        LogicalHash([3; 32]),
        [2; 32],
        false,
        RegistryExtrasV1::canonical(vec![RegistryExtraRow {
            node: SchemaNodeId(0),
            path: vec![],
            fact: RegistryExtraFact::ControlRole(if definition == 7 {
                distill_core::attestation::ControlRole::Unrestricted
            } else {
                distill_core::attestation::ControlRole::AuthoringOnlyRequired
            }),
        }])
        .unwrap(),
    )
    .unwrap()
}

fn target() -> TargetDefinition {
    target_with_definition(7)
}

fn target_with_definition(definition: u8) -> TargetDefinition {
    let mut rows = vec![compiled(definition)];
    rows.extend_from_slice(
        distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1()
            .unwrap()
            .rows(),
    );
    let mut policies = vec![LoadPolicyEntry {
        type_uuid: TypeUuid([1; 16]),
        build_only: false,
    }];
    policies.extend(rows.iter().filter_map(|row| {
        distill_core::attestation::is_bootstrap_control_type(row.type_uuid).then_some(
            LoadPolicyEntry {
                type_uuid: row.type_uuid,
                build_only: true,
            },
        )
    }));
    TargetDefinition::canonical(
        "dev",
        TargetDefinitionHash([definition; 32]),
        rows,
        policies,
    )
    .unwrap()
}

fn request() -> ConnectRequest {
    let target = target();
    ConnectRequest::canonical(
        GameModuleEpoch(1),
        "dev",
        TargetDefinitionHash([7; 32]),
        target.compiled_registry().to_vec(),
        target.load_policy().to_vec(),
    )
    .unwrap()
}

fn server() -> Server {
    Server::new(StoreInstanceId([9; 16]), vec![target()]).unwrap()
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
            logical_hash: LogicalHash([77; 32]),
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
            encoded_type: type_uuid,
            terminal_type: type_uuid,
            closure_rows: vec![ServedClosureRow {
                asset,
                content_hash: hash,
                authored_type: type_uuid,
                encoded_type: type_uuid,
                terminal_type: type_uuid,
                load_edges: Vec::new(),
            }],
        },
    )
}

#[tokio::test(flavor = "current_thread")]
async fn remote_loader_client_preserves_typed_calls_and_rotates_reattestation() {
    LocalSet::new()
        .run_until(async {
            let server = server();
            let asset = AssetUuid([44; 16]);
            let path = "assets/remote.bundle";
            let wire = distill_wire::wire::WireNode::Unit { offset: 0 };
            let layout_hash = distill_wire::dswl::dswl_hash(&wire).unwrap();
            let wire_bytes: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&wire).unwrap());
            server
                .install_wire_tree(layout_hash, Arc::clone(&wire_bytes))
                .unwrap();
            let (content_hash, artifact) =
                canonical_artifact(asset, TypeUuid([1; 16]), layout_hash, &[7, 8, 9]);
            server.install_artifact(content_hash, artifact).unwrap();
            let stamp = server
                .commit(Commit {
                    assets: vec![AssetMutation::Set {
                        uuid: asset,
                        resolution: StoredResolve::Drifted {
                            input: DriftedInput::File("source.asset".into()),
                        },
                        delta: AssetDeltaState::Changed,
                    }],
                    paths: vec![PathMutation::Set {
                        path: path.into(),
                        candidates: std::collections::BTreeSet::from([asset]),
                    }],
                    ..Commit::default()
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
            let mut hub = RemoteHub::connected(client.connect(&request()).await.unwrap()).unwrap();

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
                    input: DriftedInput::File(ref value),
                    current,
                } if value == "source.asset" && current == stamp
            ));
            let path_result = match snapshot.resolve_path(path).await.unwrap() {
                RemoteCall::Success(terminal) => terminal.value,
                other => panic!("path resolve failed: {other:?}"),
            };
            assert_eq!(path_result, PathResolveResult::Resolved(asset));

            let mut fetched = match hub.fetch(content_hash).await.unwrap() {
                RemoteCall::Success(terminal) => terminal,
                other => panic!("fetch failed: {other:?}"),
            };
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

            let mut next = request();
            next.epoch = GameModuleEpoch(2);
            let reattest = ReattestRequest {
                epoch: next.epoch,
                base_attestation_generation: 0,
                successor_attestation_generation: 1,
                target_definition_hash: next.target_definition_hash,
                compiled_registry: next.compiled_registry,
                dsca: next.dsca,
                load_policy: next.load_policy,
                policy_digest: next.policy_digest,
            };
            assert!(matches!(
                hub.reattest(&reattest).await.unwrap(),
                RemoteCall::Success(ReattestSuccess {
                    installed_attestation_generation: 1,
                    ..
                })
            ));
            assert_eq!(hub.attestation_generation(), 1);

            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        })
        .await;
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
}

struct BlockingBuildBackend {
    started: Arc<AtomicBool>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl BuildBackend for BlockingBuildBackend {
    fn build(&self, request: &BuildRequest) -> Result<BuildBackendOutcome, RpcFailure> {
        self.started.store(true, Ordering::Release);
        let (released, wake) = &*self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = wake.wait(released).unwrap();
        }
        Ok(BuildBackendOutcome::Drifted {
            input: request.drifted_input.clone(),
        })
    }
}

fn write_reattest(
    mut params: schema::hub::reattest_params::Builder<'_>,
    epoch: u64,
    base: u64,
    successor: u64,
) {
    let next = request();
    params.set_epoch(epoch);
    params.set_base_attestation_generation(base);
    params.set_successor_attestation_generation(successor);
    params.set_target_def_hash(&next.target_definition_hash.0);
    let mut rows = params
        .reborrow()
        .init_compiled_registry(next.compiled_registry.len() as u32);
    for (index, row) in next.compiled_registry.iter().enumerate() {
        let mut wire = rows.reborrow().get(index as u32);
        wire.set_type_uuid(&row.type_uuid.0);
        wire.set_logical_hash(&row.logical_hash.0);
        wire.set_native_layout_digest(&row.native_layout_digest);
        wire.set_build_only(row.build_only);
        wire.set_registry_extras_digest(&row.registry_extras_digest.0);
        wire.set_registry_extras(&row.registry_extras.encode().unwrap());
    }
    params.set_dsca_aggregate(&next.dsca.0);
    let mut policies = params
        .reborrow()
        .init_load_policy(next.load_policy.len() as u32);
    for (index, row) in next.load_policy.iter().enumerate() {
        let mut wire = policies.reborrow().get(index as u32);
        wire.set_type_uuid(&row.type_uuid.0);
        wire.set_build_only(row.build_only);
    }
    params.set_policy_digest(&next.policy_digest);
}

#[test]
fn schema_uses_typed_five_arm_results_for_every_hub_and_snapshot_method() {
    let source = include_str!("../schema/distill_rpc.capnp");
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
            body.contains("configurationPoisoned @2"),
            "{name} poison ordinal"
        );
        assert!(body.contains("leaseFailure @3"), "{name} lease ordinal");
        assert!(body.contains("error @4"), "{name} error ordinal");
    }
    let reattest = source
        .split_once("struct ReattestResult {")
        .expect("dedicated ReattestResult")
        .1
        .split_once("\n  }\n}")
        .expect("terminated ReattestResult")
        .0;
    for arm in [
        "success @0 :ReattestSuccess",
        "attestationFailure @1 :AttestationFailure",
        "staleAttestationBase @2 :StaleAttestationBase",
        "attestationGenerationOverflow @3 :AttestationGenerationOverflow",
        "reconnectRequired @4 :ReconnectRequired",
        "configurationPoisoned @5 :ConfigurationPoison",
        "leaseFailure @6 :LeaseFailure",
        "error @7 :RpcError",
    ] {
        assert!(reattest.contains(arm), "missing reattest arm {arm}");
    }
    assert!(source.contains("-> (result :ReattestResult);"));

    let inspect = source
        .split_once("struct AuthoringInspectCall {")
        .expect("dedicated AuthoringInspectCall")
        .1
        .split_once("\n  }\n}")
        .expect("terminated AuthoringInspectCall")
        .0;
    for arm in [
        "success @0 :AuthoringInspection",
        "reconnectRequired @1 :ReconnectRequired",
        "configurationPoisoned @2 :ConfigurationPoison",
        "leaseFailure @3 :LeaseFailure",
        "error @4 :RpcError",
        "missing @5 :Void",
        "roleIneligible @6 :AuthoringRoleFailure",
    ] {
        assert!(inspect.contains(arm), "missing authoring inspect arm {arm}");
    }
    assert!(source.contains("authoringSnapshot @10 () -> (result :AuthoringSnapshotCall);"));
    assert!(source.contains("interface AuthoringSnapshot {"));
    assert!(source.contains("version @0 () -> (result :UInt64Call);"));
    assert!(source.contains("query @1 (query :AssetQuery) -> (result :UuidListCall);"));
    assert!(source.contains("inspect @2 (uuid :Data) -> (result :AuthoringInspectCall);"));
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

fn authoring_entry(byte: u8, role: AuthoringEntryRole) -> AuthoringEntry {
    let schema_hash = node_hash(&SchemaNode::Blob).unwrap();
    AuthoringEntry {
        uuid: AssetUuid([byte; 16]),
        bundle: BundleUuid([byte.wrapping_add(1); 16]),
        local_id: format!("entry-{byte}"),
        normalized_path: format!("bundle-{byte}.asset"),
        type_uuid: TypeUuid([1; 16]),
        terminal_type: TypeUuid([1; 16]),
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

fn write_authoring_entry_value(
    mut output: schema::authoring_entry_value::Builder<'_>,
    entry: &AuthoringEntry,
) {
    output.reborrow().init_uuid().set_bytes(&entry.uuid.0);
    output.reborrow().init_bundle().set_bytes(&entry.bundle.0);
    output.set_local_id(entry.local_id.as_str());
    output.set_normalized_path(entry.normalized_path.as_str());
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
        blobs.set(index as u32, blob);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn authoring_snapshot_round_trips_over_real_tcp_with_exact_stamp_and_role_fence() {
    LocalSet::new()
        .run_until(async {
            let server = server();
            let entry = authoring_entry(3, AuthoringEntryRole::AuthoringOnly);
            let derived_uuid = AssetUuid([5; 16]);
            let hash = ContentHash([4; 32]);
            let first_stamp = server
                .commit(Commit {
                    assets: vec![
                        AssetMutation::Set {
                            uuid: entry.uuid,
                            resolution: StoredResolve::Built { content_hash: hash },
                            delta: AssetDeltaState::Changed,
                        },
                        AssetMutation::Set {
                            uuid: derived_uuid,
                            resolution: StoredResolve::Built { content_hash: hash },
                            delta: AssetDeltaState::Changed,
                        },
                    ],
                    authoring: vec![AuthoringMutation::Set(entry.clone())],
                    ..Commit::default()
                })
                .unwrap();

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
            inspect.get().set_uuid(&entry.uuid.0);
            let inspect_response = inspect.send().promise.await.unwrap();
            let inspect_result = inspect_response.get().unwrap().get_result().unwrap();
            match inspect_result.which().unwrap() {
                schema::authoring_inspect_call::Which::Success(inspection) => {
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
            wrong_role.get().set_uuid(&derived_uuid.0);
            let wrong_role_response = wrong_role.send().promise.await.unwrap();
            match wrong_role_response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::authoring_inspect_call::Which::RoleIneligible(failure) => {
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
                schema::resolve_call::Which::AttestationExpansionRequired(_) => {
                    panic!("unexpected resolve attestation expansion")
                }
                _ => panic!("expected typed terminal role failure"),
            };
            assert!(matches!(
                terminal.get_result().unwrap().which().unwrap(),
                schema::resolve_result::Which::RoleIneligible(_)
            ));

            let mut replacement = entry;
            replacement.value.canonical_value = Arc::from(&b"{\"$distill_blob\":0}"[..]);
            let second_stamp = server
                .commit(Commit {
                    authoring: vec![AuthoringMutation::Set(replacement)],
                    ..Commit::default()
                })
                .unwrap();
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

            let poison = ConfigurationPoison::from_reason(
                &DscpV1::MalformedConfiguration { file_hash: [7; 32] },
                "invalid staged configuration",
            );
            let poisoned_stamp = server
                .commit(Commit {
                    configuration: Some(ConfigurationStatus::Poisoned(poison)),
                    ..Commit::default()
                })
                .unwrap();
            let poisoned_refresh = refreshed.refresh_request().send().promise.await.unwrap();
            let poisoned = match poisoned_refresh
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::authoring_snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("pure authoring metadata refresh must survive configuration poison"),
            };
            let poisoned_version = poisoned.version_request().send().promise.await.unwrap();
            assert!(matches!(
                poisoned_version
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap(),
                schema::u_int64_call::Which::Success(version)
                    if version == poisoned_stamp.version.0
            ));

            server.replace_target(target_with_definition(8)).unwrap();
            let fenced = poisoned.version_request().send().promise.await.unwrap();
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
    let root = server().root();
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
    let ipv4 = StagedListener::bind(server().root(), "127.0.0.1:0")
        .await
        .unwrap();
    assert!(ipv4.local_addr().unwrap().ip().is_loopback());

    // Some CI hosts disable IPv6. When available, it must remain loopback.
    if let Ok(ipv6) = StagedListener::bind(server().root(), "[::1]:0").await {
        assert!(ipv6.local_addr().unwrap().ip().is_loopback());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unbound_metadata_bootstrap_round_trips_over_tcp_while_poisoned() {
    LocalSet::new()
        .run_until(async {
            let server = server();
            let poison = ConfigurationPoison::from_reason(
                &DscpV1::MalformedConfiguration { file_hash: [6; 32] },
                "invalid staged configuration",
            );
            let version_poison = VersionPoison::new(
                VersionPoisonV1::UnreadableScanSubtree {
                    subject: ScanSubject::Subtree {
                        root_name: "assets".to_owned(),
                        raw_relative_path: PlatformPathBytes::Unix(b"unreadable".to_vec()),
                    },
                    failure: ScanFailureCode::PermissionDenied,
                },
                "unreadable scan subtree",
            )
            .unwrap();
            let stamp = server
                .commit(Commit {
                    configuration: Some(ConfigurationStatus::Poisoned(poison.clone())),
                    version_poison: Some(Some(version_poison.clone())),
                    ..Commit::default()
                })
                .unwrap();
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
                _ => panic!("expected metadata snapshot under poison"),
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
                _ => panic!("expected typed poison diagnostics"),
            };
            match diagnostics.get_configuration().unwrap().which().unwrap() {
                schema::configuration_diagnostic::Which::Poisoned(value) => {
                    let value = value.unwrap();
                    assert_eq!(value.get_code(), poison.code as u16);
                    assert_eq!(value.get_reason_hash().unwrap(), poison.reason_hash);
                }
                _ => panic!("expected poisoned diagnostics"),
            }
            match diagnostics.get_version_poison().unwrap().which().unwrap() {
                schema::version_poison_diagnostic::Which::Poisoned(value) => {
                    let value = value.unwrap();
                    assert_eq!(value.get_code(), version_poison.code as u16);
                    assert_eq!(value.get_identity().unwrap(), version_poison.identity);
                }
                _ => panic!("expected version poison diagnostics"),
            }
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
                schema::metadata_uuid_list_call::Which::VersionPoisoned(value) => {
                    assert_eq!(
                        distill_rpc::capnp_transport::decode_version_poison(value.unwrap())
                            .unwrap(),
                        version_poison
                    );
                }
                _ => panic!("namespace query must carry the exact version poison"),
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
                    if error.clone().unwrap().get_code() == 1001
            ));
            drop(client);
            server_task.await.unwrap().unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lineage_repair_bootstrap_and_exact_inspection_round_trip_over_tcp() {
    LocalSet::new()
        .run_until(async {
            let server = server();
            let poison = ConfigurationPoison::from_reason(
                &DscpV1::MissingLineageManifest,
                "lineage manifest is missing",
            );
            let destination_hash = BundleFileHash([8; 32]);
            let configured_path = format!("control/{}.bundle", "a".repeat(300));
            let stamp = server
                .commit(Commit {
                    configuration: Some(ConfigurationStatus::Poisoned(poison)),
                    lineage_repair: Some(Some(LineageRepairState::Missing {
                        configured_root: "assets".to_owned(),
                        configured_path: configured_path.clone(),
                        destination: LineageRepairDestination::Occupied {
                            file_hash: destination_hash,
                            kind: OccupiedLineageDestinationKind::Opaque,
                        },
                    })),
                    ..Commit::default()
                })
                .unwrap();
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
            let mut connect = client.root().lineage_repair_request();
            connect.get().set_protocol(PROTOCOL_VERSION);
            let response = connect.send().promise.await.unwrap();
            let repair = match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::lineage_repair_connect_result::Which::Success(repair) => repair.unwrap(),
                _ => panic!("expected lineage repair capability"),
            };
            let response = repair.inspect_request().send().promise.await.unwrap();
            let inspection = match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::lineage_repair_inspect_result::Which::Success(inspection) => {
                    inspection.unwrap()
                }
                _ => panic!("expected lineage repair inspection"),
            };
            assert_eq!(inspection.get_instance().unwrap(), &stamp.instance.0);
            let observed_stamp = inspection.get_stamp().unwrap();
            assert_eq!(observed_stamp.get_instance().unwrap(), &stamp.instance.0);
            assert_eq!(observed_stamp.get_version(), stamp.version.0);
            let missing = match inspection.get_state().unwrap().which().unwrap() {
                schema::lineage_repair_state::Which::Missing(missing) => missing.unwrap(),
                _ => panic!("expected missing-lineage inspection"),
            };
            assert_eq!(missing.get_configured_root().unwrap(), "assets");
            assert_eq!(missing.get_configured_path().unwrap(), configured_path);
            let occupied = match missing.get_destination().unwrap().which().unwrap() {
                schema::lineage_repair_destination::Which::Occupied(occupied) => occupied.unwrap(),
                _ => panic!("expected occupied destination basis"),
            };
            assert_eq!(occupied.get_file_hash().unwrap(), &destination_hash.0);
            assert_eq!(
                occupied.get_kind().unwrap(),
                schema::OccupiedLineageDestinationKind::Opaque
            );
            drop(client);
            server_task.await.unwrap().unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn generated_rpc_system_round_trips_connect_snapshot_resolve_fetch_and_delta() {
    LocalSet::new()
        .run_until(async {
            let server = server();
            let uuid = AssetUuid([3; 16]);
            let (hash, payload) =
                canonical_artifact(uuid, TypeUuid([1; 16]), LayoutHash([4; 32]), &[10, 11, 12]);
            let expected_structural = payload.structural.clone();
            server.install_artifact(hash, payload).unwrap();
            server
                .commit(Commit {
                    assets: vec![AssetMutation::Set {
                        uuid,
                        resolution: StoredResolve::Built { content_hash: hash },
                        delta: AssetDeltaState::Changed,
                    }],
                    ..Commit::default()
                })
                .unwrap();

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
                RemoteConnectOutcome::Connected {
                    hub,
                    instance,
                    policy_generation,
                    target_generation,
                    attestation_generation,
                    load_policy,
                    daemon_compiled_projection,
                } => {
                    assert_eq!(instance, StoreInstanceId([9; 16]));
                    assert_eq!(policy_generation, 0);
                    assert_eq!(target_generation, 0);
                    assert_eq!(attestation_generation, 0);
                    assert_eq!(load_policy.rows, request().load_policy);
                    assert_eq!(load_policy.digest, request().policy_digest);
                    assert_eq!(daemon_compiled_projection, request().dsca);
                    hub
                }
                other => panic!("expected connected, got {other:?}"),
            };

            // Reattestation carries and revalidates the same full typed rows,
            // not a layout-only projection.
            let mut reattest = hub.reattest_request();
            write_reattest(reattest.get(), 2, 0, 1);
            let reattested = reattest.send().promise.await.unwrap();
            match reattested
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::reattest_result::Which::Success(success) => {
                    let success = success.unwrap();
                    assert_eq!(success.get_installed_attestation_generation(), 1);
                    assert_eq!(
                        success.get_daemon_compiled_projection().unwrap(),
                        request().dsca.0
                    );
                }
                _ => panic!("expected typed reattestation success"),
            }

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

            let mut fetch = hub.fetch_request();
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
                    let initial = initial.unwrap();
                    assert_eq!(initial.len(), 1);
                    assert_eq!(initial.get(0).get_assets().unwrap().len(), 1);
                }
                _ => panic!("expected cursor-bound initial delta"),
            }

            // `DeltaStream.next` is a long-polling capability call. A commit
            // after the call is in flight wakes it without a polling gap.
            let pending_live = deltas.next_request().send().promise;
            server
                .commit(Commit {
                    assets: vec![AssetMutation::Set {
                        uuid,
                        resolution: StoredResolve::Built { content_hash: hash },
                        delta: AssetDeltaState::Restored,
                    }],
                    ..Commit::default()
                })
                .unwrap();
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

            // Configuration poison remains a typed result union over the
            // generated transport; refresh itself remains safe.
            let poison = ConfigurationPoison::from_reason(
                &DscpV1::MalformedConfiguration { file_hash: [8; 32] },
                "invalid staged configuration",
            );
            server
                .commit(Commit {
                    configuration: Some(ConfigurationStatus::Poisoned(poison.clone())),
                    ..Commit::default()
                })
                .unwrap();
            let refresh_response = snapshot.refresh_request().send().promise.await.unwrap();
            let refresh_result = refresh_response.get().unwrap().get_result().unwrap();
            let poisoned_snapshot = match refresh_result.which().unwrap() {
                schema::snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("refresh must remain valid under configuration poison"),
            };
            let mut poisoned_resolve = poisoned_snapshot.resolve_request();
            poisoned_resolve.get().set_uuid(&uuid.0);
            let poisoned_response = poisoned_resolve.send().promise.await.unwrap();
            let poisoned_result = poisoned_response.get().unwrap().get_result().unwrap();
            match poisoned_result.which().unwrap() {
                schema::resolve_call::Which::ConfigurationPoisoned(value) => {
                    let value = value.unwrap();
                    assert_eq!(value.get_code(), poison.code as u16);
                    assert_eq!(value.get_reason_hash().unwrap(), &poison.reason_hash);
                }
                _ => panic!("resolve must return typed configuration poison"),
            }

            // The stream notification is advisory; stale capabilities are
            // independently fenced by the generated server adapter.
            server.replace_target(target_with_definition(8)).unwrap();
            let mut fenced_resolve = poisoned_snapshot.resolve_request();
            fenced_resolve.get().set_uuid(&uuid.0);
            let fenced_response = fenced_resolve.send().promise.await.unwrap();
            let fenced_result = fenced_response.get().unwrap().get_result().unwrap();
            match fenced_result.which().unwrap() {
                schema::resolve_call::Which::ReconnectRequired(reconnect) => {
                    assert_eq!(
                        reconnect.unwrap().get_reason().unwrap(),
                        schema::ReconnectReason::TargetDefinitionChanged
                    );
                }
                _ => panic!("stale capability must be generation-fenced"),
            }
            let version = poisoned_snapshot
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
            let configuration = poisoned_snapshot
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
    let server = server();
    server.install_build_backend(Arc::new(BlockingBuildBackend {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    }));
    let entry = authoring_entry(41, AuthoringEntryRole::Runtime);
    let drift_stamp = server
        .commit(Commit {
            assets: vec![AssetMutation::Set {
                uuid: entry.uuid,
                resolution: StoredResolve::Drifted {
                    input: DriftedInput::Asset(entry.uuid),
                },
                delta: AssetDeltaState::Changed,
            }],
            authoring: vec![AuthoringMutation::Set(entry.clone())],
            ..Commit::default()
        })
        .unwrap();

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
            let listener = Rc::new(
                StagedListener::bind(server().root(), "127.0.0.1:0")
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

            let mut wrong_attestation = request();
            wrong_attestation.target_definition_hash = TargetDefinitionHash([8; 32]);
            assert!(matches!(
                client.connect(&wrong_attestation).await.unwrap(),
                RemoteConnectOutcome::AttestationFailure(failure)
                    if failure.code() == AttestationFailureCode::TargetDefinitionMismatch
                        && *failure.subject() == AttestationSubject::TargetDefinition
            ));

            let mut wrong_type = request();
            wrong_type.compiled_registry[0].logical_hash = LogicalHash([8; 32]);
            wrong_type.dsca = CompiledTypeTable::canonical(wrong_type.compiled_registry.clone())
                .unwrap()
                .digest;
            assert!(matches!(
                client.connect(&wrong_type).await.unwrap(),
                RemoteConnectOutcome::AttestationFailure(failure)
                    if failure.code() == AttestationFailureCode::LogicalHashMismatch
                        && *failure.subject() == AttestationSubject::SpecificType {
                            type_uuid: TypeUuid([1; 16]),
                            projection: AttestationProjection::CompiledRegistry,
                        }
            ));

            let mut malformed = client.root().connect_request();
            {
                let mut params = malformed.get();
                params.set_target("dev");
                params.set_target_def_hash(&[7; 31]);
                params.reborrow().init_compiled_registry(0);
                params.set_dsca_aggregate(&[0; 32]);
                params.reborrow().init_load_policy(0);
                params.set_policy_digest(&[0; 32]);
                params.set_protocol(PROTOCOL_VERSION);
                params.set_game_module_epoch(1);
            }
            let malformed_response = malformed.send().promise.await.unwrap();
            let malformed_result = malformed_response.get().unwrap().get_result().unwrap();
            match malformed_result.which().unwrap() {
                schema::connect_call::Which::AttestationFailure(failure) => {
                    let failure = failure.unwrap();
                    assert_eq!(
                        failure.get_code().unwrap(),
                        schema::AttestationFailureCode::MalformedField
                    );
                    let fixed = match failure.get_subject().unwrap().which().unwrap() {
                        schema::attestation_subject::Which::FixedField(fixed) => fixed.unwrap(),
                        _ => panic!("wrong fixed width must identify the field"),
                    };
                    assert!(matches!(
                        fixed.which().unwrap(),
                        schema::attestation_fixed_field_subject::Which::TargetDefHash(())
                    ));
                    let detail = match failure.get_payload().unwrap().which().unwrap() {
                        schema::attestation_failure_payload::Which::MalformedField(detail) => {
                            detail.unwrap()
                        }
                        _ => panic!("wrong fixed width must carry the observed bytes"),
                    };
                    assert_eq!(detail.get_expected_width(), 32);
                    assert_eq!(detail.get_observed().unwrap(), &[7; 31]);
                }
                _ => panic!("wrong hash width must be rejected"),
            }

            let mut forged_extras = client.root().connect_request();
            {
                let valid = request();
                let row = &valid.compiled_registry[0];
                let mut params = forged_extras.get();
                params.set_target("dev");
                params.set_target_def_hash(&valid.target_definition_hash.0);
                let mut rows = params.reborrow().init_compiled_registry(1);
                let mut wire = rows.reborrow().get(0);
                wire.set_type_uuid(&row.type_uuid.0);
                wire.set_logical_hash(&row.logical_hash.0);
                wire.set_native_layout_digest(&row.native_layout_digest);
                wire.set_build_only(row.build_only);
                wire.set_registry_extras_digest(&row.registry_extras_digest.0);
                wire.set_registry_extras(&[2, 0, 0, 0, 0]);
                params.set_dsca_aggregate(&valid.dsca.0);
                let mut policies = params.reborrow().init_load_policy(1);
                let mut policy = policies.reborrow().get(0);
                policy.set_type_uuid(&valid.load_policy[0].type_uuid.0);
                policy.set_build_only(false);
                params.set_policy_digest(&valid.policy_digest);
                params.set_protocol(PROTOCOL_VERSION);
                params.set_game_module_epoch(1);
            }
            let forged_response = forged_extras.send().promise.await.unwrap();
            let forged_result = forged_response.get().unwrap().get_result().unwrap();
            match forged_result.which().unwrap() {
                schema::connect_call::Which::AttestationFailure(failure) => {
                    let failure = failure.unwrap();
                    assert_eq!(
                        failure.get_code().unwrap(),
                        schema::AttestationFailureCode::MalformedTable
                    );
                    assert!(matches!(
                        failure.get_subject().unwrap().which().unwrap(),
                        schema::attestation_subject::Which::CompiledRegistryTable(())
                    ));
                    let detail = match failure.get_payload().unwrap().which().unwrap() {
                        schema::attestation_failure_payload::Which::TableDetail(detail) => {
                            detail.unwrap()
                        }
                        _ => panic!("malformed row must carry table detail"),
                    };
                    assert_eq!(detail.get_index(), 0);
                    assert!(!detail.get_entry().unwrap().is_empty());
                }
                _ => panic!("unknown DSRE version must be a typed attestation rejection"),
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
                    assert_eq!(failure.unwrap().get_code(), 1001)
                }
                _ => panic!("wrong UUID width must be a typed failure"),
            }

            let mut fetch = hub.fetch_request();
            fetch.get().set_hash(&[2; 31]);
            let response = fetch.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::chunk_stream_call::Which::Error(failure) => {
                    assert_eq!(failure.unwrap().get_code(), 1002)
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
            let server = server();
            let required = SchemaAcceptanceRequired {
                manifest: SchemaManifestBasis {
                    manifest_hash: ContentHash([41; 32]),
                    current_cursors: std::collections::BTreeMap::new(),
                },
                candidate: PipelineCandidateIdentity {
                    dylib_hash: [42; 32],
                    compiled_types: CompiledAttestationDigest([43; 32]),
                    target_set: distill_core::target_set::CanonicalTargetSet::canonical(vec![])
                        .unwrap(),
                },
                mismatches: vec![SchemaRegistryMismatch {
                    type_uuid: TypeUuid([1; 16]),
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
                    PipelineUnavailableDiagnostic::SchemaAcceptanceRequired(observed)
                ) if observed == required
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
async fn concurrent_wire_reattests_cas_the_generation_without_late_overwrite() {
    LocalSet::new()
        .run_until(async {
            let listener = Rc::new(
                StagedListener::bind(server().root(), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
            let address = listener.local_addr().unwrap();
            let server_listener = listener.clone();
            let server_task =
                tokio::task::spawn_local(async move { server_listener.serve_one().await });
            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.connect(&request()).await.unwrap() {
                RemoteConnectOutcome::Connected {
                    hub,
                    attestation_generation,
                    ..
                } => {
                    assert_eq!(attestation_generation, 0);
                    hub
                }
                other => panic!("expected connected, got {other:?}"),
            };

            let mut first = hub.reattest_request();
            write_reattest(first.get(), 2, 0, 1);
            let mut second = hub.reattest_request();
            write_reattest(second.get(), 3, 0, 1);
            let (first, second) = futures::join!(first.send().promise, second.send().promise);
            let first = first.unwrap();
            let second = second.unwrap();
            let outcomes = [
                first.get().unwrap().get_result().unwrap().which().unwrap(),
                second.get().unwrap().get_result().unwrap().which().unwrap(),
            ];
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| match outcome {
                        schema::reattest_result::Which::Success(success) =>
                            success.as_ref().is_ok_and(|success| {
                                success.get_installed_attestation_generation() == 1
                            }),
                        _ => false,
                    })
                    .count(),
                1
            );
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| match outcome {
                        schema::reattest_result::Which::StaleAttestationBase(stale) =>
                            stale.as_ref().is_ok_and(|stale| {
                                stale.get_code() == STALE_ATTESTATION_BASE_CODE
                                    && stale.get_expected() == 1
                                    && stale.get_observed() == 0
                            }),
                        _ => false,
                    })
                    .count(),
                1
            );

            let mut successor = hub.reattest_request();
            write_reattest(successor.get(), 4, 1, 2);
            let successor = successor.send().promise.await.unwrap();
            match successor
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::reattest_result::Which::Success(success) => {
                    assert_eq!(success.unwrap().get_installed_attestation_generation(), 2)
                }
                _ => panic!("expected second typed reattestation success"),
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
async fn hub_authoring_operation_and_wire_tree_methods_are_live_and_generation_first() {
    LocalSet::new()
        .run_until(async {
            let backend = Arc::new(RecordingAuthoringBackend::default());
            let server = Server::new_with_authoring_backend(
                StoreInstanceId([9; 16]),
                vec![target()],
                backend.clone(),
            )
            .unwrap();
            let entry = authoring_entry(31, AuthoringEntryRole::Runtime);
            let wire_node = distill_wire::wire::WireNode::Unit { offset: 0 };
            let tree: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&wire_node).unwrap());
            let layout_hash = distill_wire::dswl::dswl_hash(&wire_node).unwrap();
            server.install_wire_tree(layout_hash, tree.clone()).unwrap();
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
            assert!(matches!(
                write.get().unwrap().get_result().unwrap().which().unwrap(),
                schema::u_int64_call::Which::Success(1)
            ));

            let mut import = hub.import_request();
            {
                let mut params = import.get();
                params.set_base(1);
                let mut request = params.init_request();
                request.set_importer("image-importer");
                let mut sources = request.reborrow().init_sources(1);
                sources.set(0, "source/image.png");
                request.set_dest("generated/image.bundle");
                let mut settings = request.reborrow().init_settings();
                settings.set_canonical_value(b"{}");
                settings.reborrow().init_blobs(1).set(0, b"settings-blob");
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
            reimport.get().set_base(2);
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
            operation.get().set_base(3);
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
            cancellable.get().set_base(4);
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
                backend.imports.lock().unwrap()[0].settings.blobs[0].as_ref(),
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

            let (wire_artifact_hash, wire_artifact) =
                canonical_artifact(entry.uuid, TypeUuid([1; 16]), layout_hash, &[31]);
            server
                .install_artifact(wire_artifact_hash, wire_artifact)
                .unwrap();
            server
                .commit(Commit {
                    assets: vec![AssetMutation::Set {
                        uuid: entry.uuid,
                        resolution: StoredResolve::Built {
                            content_hash: wire_artifact_hash,
                        },
                        delta: AssetDeltaState::Changed,
                    }],
                    ..Commit::default()
                })
                .unwrap();

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

            let expansion_node = distill_wire::wire::WireNode::Unit { offset: 8 };
            let expansion_tree: Arc<[u8]> =
                Arc::from(distill_wire::dswl::dswl_bytes(&expansion_node).unwrap());
            let expansion_hash = distill_wire::dswl::dswl_hash(&expansion_node).unwrap();
            server
                .install_wire_tree(expansion_hash, expansion_tree)
                .unwrap();
            let expansion_asset = AssetUuid([2; 16]);
            let (expansion_artifact_hash, expansion_artifact) =
                canonical_artifact(expansion_asset, TypeUuid([2; 16]), expansion_hash, &[2]);
            server
                .install_artifact(expansion_artifact_hash, expansion_artifact)
                .unwrap();
            server
                .commit(Commit {
                    assets: vec![AssetMutation::Set {
                        uuid: expansion_asset,
                        resolution: StoredResolve::Built {
                            content_hash: expansion_artifact_hash,
                        },
                        delta: AssetDeltaState::Changed,
                    }],
                    ..Commit::default()
                })
                .unwrap();
            let mut expansion = hub.wire_tree_request();
            expansion.get().set_layout_hash(&expansion_hash.0);
            let expansion = expansion.send().promise.await.unwrap();
            let challenge = match expansion
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                schema::data_call::Which::AttestationExpansionRequired(challenge) => {
                    challenge.unwrap()
                }
                _ => panic!("expected typed no-data attestation expansion"),
            };
            assert_eq!(challenge.get_closure_identity().unwrap().len(), 32);
            assert_ne!(challenge.get_closure_identity().unwrap(), &[0; 32]);
            let required = challenge.get_required_type_uuids().unwrap();
            assert_eq!(required.len(), 1);
            assert_eq!(required.get(0).unwrap(), &[2; 16]);

            server.replace_target(target_with_definition(8)).unwrap();
            let mut stale = hub.reimport_request();
            stale.get().set_base(u64::MAX);
            stale.get().reborrow().init_bundle().set_bytes(&[1]);
            let stale = stale.send().promise.await.unwrap();
            match stale.get().unwrap().get_result().unwrap().which().unwrap() {
                schema::uuid_call::Which::ReconnectRequired(reconnect) => assert_eq!(
                    reconnect.unwrap().get_reason().unwrap(),
                    schema::ReconnectReason::TargetDefinitionChanged
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

#[tokio::test(flavor = "current_thread")]
async fn wire_reattest_has_typed_failure_subjects_generations_and_reconnect() {
    LocalSet::new()
        .run_until(async {
            let server = server();
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

            let mut bad_target = hub.reattest_request();
            {
                let mut params = bad_target.get();
                write_reattest(params.reborrow(), 2, 0, 1);
                params.set_target_def_hash(&[8; 32]);
            }
            let response = bad_target.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            let failure = match result.which().unwrap() {
                schema::reattest_result::Which::AttestationFailure(failure) => failure.unwrap(),
                _ => panic!("target mismatch must use the attestation arm"),
            };
            assert_eq!(
                failure.get_code().unwrap(),
                schema::AttestationFailureCode::TargetDefinitionMismatch
            );
            let subject = failure.get_subject().unwrap();
            match subject.which().unwrap() {
                schema::attestation_subject::Which::TargetDefinition(()) => {}
                _ => panic!("target mismatch must use the target-definition subject"),
            };
            let payload = failure.get_payload().unwrap();
            let mismatch = match payload.which().unwrap() {
                schema::attestation_failure_payload::Which::ExpectedObserved(mismatch) => {
                    mismatch.unwrap()
                }
                _ => panic!("target mismatch must carry both digests"),
            };
            assert_eq!(mismatch.get_expected().unwrap(), &[7; 32]);
            assert_eq!(mismatch.get_observed().unwrap(), &[8; 32]);

            let mut duplicate_policy = hub.reattest_request();
            {
                let mut params = duplicate_policy.get();
                write_reattest(params.reborrow(), 2, 0, 1);
                let mut rows = params.reborrow().get_load_policy().unwrap();
                rows.reborrow()
                    .get(1)
                    .set_type_uuid(&request().load_policy[0].type_uuid.0);
            }
            let response = duplicate_policy.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            let failure = match result.which().unwrap() {
                schema::reattest_result::Which::AttestationFailure(failure) => failure.unwrap(),
                _ => panic!("duplicate policy row must use the attestation arm"),
            };
            assert_eq!(
                failure.get_code().unwrap(),
                schema::AttestationFailureCode::DuplicateType
            );
            let subject = match failure.get_subject().unwrap().which().unwrap() {
                schema::attestation_subject::Which::SpecificType(subject) => subject.unwrap(),
                _ => panic!("duplicate policy row must identify the repeated type"),
            };
            assert_eq!(
                subject.get_type_uuid().unwrap().get_bytes().unwrap(),
                &request().load_policy[0].type_uuid.0
            );
            assert_eq!(
                subject.get_projection().unwrap(),
                schema::AttestationProjection::Policy
            );
            let detail = match failure.get_payload().unwrap().which().unwrap() {
                schema::attestation_failure_payload::Which::TableDetail(detail) => detail.unwrap(),
                _ => panic!("duplicate policy row must carry table detail"),
            };
            assert_eq!(detail.get_index(), 1);
            assert_eq!(detail.get_entry().unwrap().len(), 17);

            let mut malformed_policy = hub.reattest_request();
            {
                let mut params = malformed_policy.get();
                write_reattest(params.reborrow(), 2, 0, 1);
                params
                    .reborrow()
                    .get_load_policy()
                    .unwrap()
                    .get(0)
                    .set_type_uuid(&[4; 15]);
            }
            let response = malformed_policy.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            let failure = match result.which().unwrap() {
                schema::reattest_result::Which::AttestationFailure(failure) => failure.unwrap(),
                _ => panic!("malformed policy UUID must use the attestation arm"),
            };
            assert_eq!(
                failure.get_code().unwrap(),
                schema::AttestationFailureCode::MalformedField
            );
            let fixed = match failure.get_subject().unwrap().which().unwrap() {
                schema::attestation_subject::Which::FixedField(fixed) => fixed.unwrap(),
                _ => panic!("malformed policy UUID must identify its row field"),
            };
            assert!(matches!(
                fixed.which().unwrap(),
                schema::attestation_fixed_field_subject::Which::PolicyTypeUuid(0)
            ));
            let malformed = match failure.get_payload().unwrap().which().unwrap() {
                schema::attestation_failure_payload::Which::MalformedField(detail) => {
                    detail.unwrap()
                }
                _ => panic!("malformed policy UUID must carry the observed bytes"),
            };
            assert_eq!(malformed.get_expected_width(), 16);
            assert_eq!(malformed.get_observed().unwrap(), &[4; 15]);

            let mut overflow = hub.reattest_request();
            write_reattest(overflow.get(), 2, u64::MAX, 0);
            let response = overflow.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::reattest_result::Which::AttestationGenerationOverflow(overflow) => {
                    let overflow = overflow.unwrap();
                    assert_eq!(overflow.get_code(), ATTESTATION_GENERATION_OVERFLOW_CODE);
                    assert_eq!(overflow.get_base(), u64::MAX);
                }
                _ => panic!("generation overflow must have its dedicated arm"),
            }

            // Overflow did not mutate: base zero remains installable.
            let mut valid = hub.reattest_request();
            write_reattest(valid.get(), 2, 0, 1);
            let response = valid.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::reattest_result::Which::Success(success) => {
                    assert_eq!(success.unwrap().get_installed_attestation_generation(), 1)
                }
                _ => panic!("expected successor installation"),
            }

            let mut stale = hub.reattest_request();
            write_reattest(stale.get(), 3, 0, 1);
            let response = stale.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::reattest_result::Which::StaleAttestationBase(stale) => {
                    let stale = stale.unwrap();
                    assert_eq!(stale.get_code(), STALE_ATTESTATION_BASE_CODE);
                    assert_eq!(stale.get_expected(), 1);
                    assert_eq!(stale.get_observed(), 0);
                }
                _ => panic!("stale base must have its dedicated arm"),
            }

            server.replace_target(target_with_definition(8)).unwrap();
            let mut fenced = hub.reattest_request();
            write_reattest(fenced.get(), 3, 1, 2);
            let response = fenced.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::reattest_result::Which::ReconnectRequired(reconnect) => assert_eq!(
                    reconnect.unwrap().get_reason().unwrap(),
                    schema::ReconnectReason::TargetDefinitionChanged
                ),
                _ => panic!("target drift must reconnect without installing"),
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
fn pipeline_poison_capnp_decode_rejects_unknown_width_and_matrix_failures() {
    fn initialize(mut root: schema::pipeline_poison::Builder<'_>, identity: &[u8]) {
        root.set_code(schema::PipelinePoisonCode::CandidateOpen);
        root.set_origin(schema::PipelinePoisonOrigin::CandidateOpen);
        root.set_cleanup(schema::CleanupDisposition::None);
        root.set_identity(identity);
        root.set_message("diagnostic only");
    }

    let mut unknown = capnp::message::Builder::new_default();
    {
        let mut root = unknown.init_root::<schema::pipeline_poison::Builder<'_>>();
        initialize(root.reborrow(), &[0; 32]);
        use capnp::introspect::{Introspect, TypeVariant};
        let TypeVariant::Enum(raw_schema) = schema::PipelinePoisonCode::introspect().which() else {
            panic!("pipeline poison code must introspect as an enum")
        };
        let enum_schema: capnp::schema::EnumSchema = raw_schema.into();
        let capnp::dynamic_value::Builder::Struct(mut dynamic) =
            capnp::dynamic_value::Builder::from(root.reborrow())
        else {
            panic!("pipeline poison must introspect as a struct")
        };
        dynamic
            .set_named(
                "code",
                capnp::dynamic_value::Enum::new(99, enum_schema).into(),
            )
            .unwrap();
    }
    assert!(distill_rpc::capnp_transport::decode_pipeline_poison(
        unknown
            .get_root_as_reader::<schema::pipeline_poison::Reader<'_>>()
            .unwrap()
    )
    .is_err());

    let mut wrong_width = capnp::message::Builder::new_default();
    initialize(
        wrong_width.init_root::<schema::pipeline_poison::Builder<'_>>(),
        &[0; 31],
    );
    assert!(distill_rpc::capnp_transport::decode_pipeline_poison(
        wrong_width
            .get_root_as_reader::<schema::pipeline_poison::Reader<'_>>()
            .unwrap()
    )
    .is_err());

    let mut wrong_matrix = capnp::message::Builder::new_default();
    {
        let mut root = wrong_matrix.init_root::<schema::pipeline_poison::Builder<'_>>();
        root.set_code(schema::PipelinePoisonCode::CandidateCleanup);
        root.set_origin(schema::PipelinePoisonOrigin::CandidateOpen);
        root.set_cleanup(schema::CleanupDisposition::None);
        root.set_identity(&[0; 32]);
        root.set_message("diagnostic only");
    }
    assert!(distill_rpc::capnp_transport::decode_pipeline_poison(
        wrong_matrix
            .get_root_as_reader::<schema::pipeline_poison::Reader<'_>>()
            .unwrap()
    )
    .is_err());
}

#[test]
fn configuration_poison_wire_authenticates_dscp_version_detail_code_and_digest() {
    let detail = DscpV1::NonLoopbackAddress {
        address: "192.0.2.1:4000".to_owned(),
    };
    let poison = ConfigurationPoison::from_reason(&detail, "loopback required");
    let encode = |version: u16, code: u16, bytes: Vec<u8>, digest: [u8; 32]| {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<schema::configuration_poison::Builder<'_>>();
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
        poison.code as u16,
        poison.detail.canonical_detail_bytes(),
        poison.reason_hash,
    );
    assert_eq!(
        distill_rpc::capnp_transport::decode_configuration_poison(
            valid
                .get_root_as_reader::<schema::configuration_poison::Reader<'_>>()
                .unwrap(),
        )
        .unwrap(),
        poison
    );
    for invalid in [
        encode(
            2,
            poison.code as u16,
            poison.detail.canonical_detail_bytes(),
            poison.reason_hash,
        ),
        encode(
            1,
            ConfigurationPoisonCode::DuplicateRootName as u16,
            poison.detail.canonical_detail_bytes(),
            poison.reason_hash,
        ),
        {
            let mut bytes = poison.detail.canonical_detail_bytes();
            bytes.push(0);
            encode(1, poison.code as u16, bytes, poison.reason_hash)
        },
        encode(
            1,
            poison.code as u16,
            poison.detail.canonical_detail_bytes(),
            [9; 32],
        ),
    ] {
        assert!(distill_rpc::capnp_transport::decode_configuration_poison(
            invalid
                .get_root_as_reader::<schema::configuration_poison::Reader<'_>>()
                .unwrap(),
        )
        .is_err());
    }
}

#[test]
fn typed_pipeline_diagnostic_codecs_reject_empty_and_noncanonical_tables() {
    let mut schema_message = capnp::message::Builder::new_default();
    {
        let mut root =
            schema_message.init_root::<schema::schema_acceptance_required::Builder<'_>>();
        let mut manifest = root.reborrow().init_manifest();
        manifest.set_manifest_hash(&[1; 32]);
        let mut cursor = manifest.reborrow().init_current_cursors(1).get(0);
        cursor.reborrow().init_type_uuid().set_bytes(&[2; 16]);
        cursor.set_logical_hash(&[3; 32]);
        let mut candidate = root.reborrow().init_candidate();
        candidate.set_dylib_hash(&[4; 32]);
        candidate.set_compiled_types(&[5; 32]);
        candidate.init_target_rows(0);
        let mut mismatch = root.init_mismatches(1).get(0);
        mismatch.reborrow().init_type_uuid().set_bytes(&[7; 16]);
        mismatch.set_candidate(&[8; 32]);
        mismatch.set_has_candidate(true);
        mismatch.set_has_manifest(false);
    }
    let decoded = distill_rpc::capnp_transport::decode_schema_acceptance_required(
        schema_message
            .get_root_as_reader::<schema::schema_acceptance_required::Reader<'_>>()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(decoded.mismatches.len(), 1);
    assert_eq!(decoded.mismatches[0].type_uuid, TypeUuid([7; 16]));

    let mut empty = capnp::message::Builder::new_default();
    empty
        .init_root::<schema::schema_acceptance_required::Builder<'_>>()
        .init_mismatches(0);
    assert!(
        distill_rpc::capnp_transport::decode_schema_acceptance_required(
            empty
                .get_root_as_reader::<schema::schema_acceptance_required::Reader<'_>>()
                .unwrap(),
        )
        .is_err()
    );

    let mut retired_message = capnp::message::Builder::new_default();
    {
        let mut root = retired_message.init_root::<schema::retired_type_referenced::Builder<'_>>();
        root.set_manifest_hash(&[9; 32]);
        let mut basis = root.reborrow().init_basis();
        basis.set_instance(&[10; 16]);
        basis.set_input_version(11);
        root.reborrow().init_type_uuid().set_bytes(&[12; 16]);
        let mut references = root.init_references(2);
        references
            .reborrow()
            .get(0)
            .init_asset()
            .set_bytes(&[13; 16]);
        references.get(1).set_migration_endpoint(&[14; 32]);
    }
    let retired = distill_rpc::capnp_transport::decode_retired_type_referenced(
        retired_message
            .get_root_as_reader::<schema::retired_type_referenced::Reader<'_>>()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(retired.references.len(), 2);

    let mut duplicate = capnp::message::Builder::new_default();
    {
        let mut root = duplicate.init_root::<schema::retired_type_referenced::Builder<'_>>();
        root.set_manifest_hash(&[9; 32]);
        root.reborrow().init_basis().set_instance(&[10; 16]);
        root.reborrow().init_type_uuid().set_bytes(&[12; 16]);
        let mut references = root.init_references(2);
        references
            .reborrow()
            .get(0)
            .init_asset()
            .set_bytes(&[13; 16]);
        references.get(1).init_asset().set_bytes(&[13; 16]);
    }
    assert!(
        distill_rpc::capnp_transport::decode_retired_type_referenced(
            duplicate
                .get_root_as_reader::<schema::retired_type_referenced::Reader<'_>>()
                .unwrap(),
        )
        .is_err()
    );
}

#[test]
fn rpc_basis_decoder_authenticates_complete_policy_and_projection_fields() {
    let request = request();
    let encode = |digest: [u8; 32]| {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<schema::rpc_basis_value::Builder<'_>>();
            let mut stamp = root.reborrow().init_stamp();
            stamp.set_instance(&[9; 16]);
            stamp.set_version(17);
            let mut policies = root
                .reborrow()
                .init_load_policy(request.load_policy.len() as u32);
            for (index, row) in request.load_policy.iter().enumerate() {
                let mut policy = policies.reborrow().get(index as u32);
                policy.set_type_uuid(&row.type_uuid.0);
                policy.set_build_only(row.build_only);
            }
            root.set_policy_digest(&digest);
            root.set_policy_generation(18);
            root.set_target_generation(19);
            root.set_attestation_generation(20);
            root.set_daemon_compiled_projection(&request.dsca.0);
        }
        message
    };
    let valid = encode(request.policy_digest);
    let adopted = RpcBasis {
        snapshot: SnapshotStamp {
            instance: StoreInstanceId([9; 16]),
            version: InputVersion(17),
        },
        load_policy: Arc::new(LoadPolicyAttestation {
            rows: request.load_policy.clone(),
            digest: request.policy_digest,
        }),
        policy_generation: 18,
        target_generation: 19,
        attestation_generation: 20,
        daemon_compiled_projection: request.dsca,
    };
    let decoded = distill_rpc::capnp_transport::decode_rpc_basis(
        valid
            .get_root_as_reader::<schema::rpc_basis_value::Reader<'_>>()
            .unwrap(),
        &adopted,
    )
    .unwrap();
    assert_eq!(decoded.snapshot.version, InputVersion(17));
    assert_eq!(decoded.load_policy.rows, request.load_policy);
    assert_eq!(decoded.policy_generation, 18);
    assert_eq!(decoded.target_generation, 19);
    assert_eq!(decoded.attestation_generation, 20);
    assert_eq!(decoded.daemon_compiled_projection, request.dsca);

    let invalid = encode([0; 32]);
    assert!(distill_rpc::capnp_transport::decode_rpc_basis(
        invalid
            .get_root_as_reader::<schema::rpc_basis_value::Reader<'_>>()
            .unwrap(),
        &adopted,
    )
    .is_err());
}

#[test]
fn version_poison_capnp_decode_rejects_noncanonical_claimants_and_path_bytes() {
    let mut short_claimant_uuid = capnp::message::Builder::new_default();
    {
        let mut root = short_claimant_uuid.init_root::<schema::version_poison::Builder<'_>>();
        root.set_code(VersionPoisonCode::DuplicateAssetUuid as u16);
        root.set_identity(&[0; 32]);
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
    assert!(distill_rpc::capnp_transport::decode_version_poison(
        short_claimant_uuid
            .get_root_as_reader::<schema::version_poison::Reader<'_>>()
            .unwrap()
    )
    .is_err());

    let mut insufficient_claimants = capnp::message::Builder::new_default();
    {
        let mut root = insufficient_claimants.init_root::<schema::version_poison::Builder<'_>>();
        root.set_code(VersionPoisonCode::DuplicateAssetUuid as u16);
        root.set_identity(&[0; 32]);
        root.set_message("diagnostic only");
        let mut detail = root.init_detail().init_duplicate_asset_uuid();
        detail.reborrow().init_asset().set_bytes(&[1; 16]);
        let mut claimant = detail.init_claimants(1).get(0).init_derived();
        claimant.reborrow().init_parent().set_bytes(&[2; 16]);
        claimant.set_output_key("a");
    }
    assert!(distill_rpc::capnp_transport::decode_version_poison(
        insufficient_claimants
            .get_root_as_reader::<schema::version_poison::Reader<'_>>()
            .unwrap()
    )
    .is_err());

    let mut odd_windows_path = capnp::message::Builder::new_default();
    {
        let mut root = odd_windows_path.init_root::<schema::version_poison::Builder<'_>>();
        root.set_code(VersionPoisonCode::SameRootNormalizedPathCollision as u16);
        root.set_identity(&[0; 32]);
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
        first.set_file_hash(&[1; 32]);
        let mut second = claims.reborrow().get(1);
        second
            .reborrow()
            .init_raw_relative_path()
            .set_unix_bytes(b"same/path");
        second.set_file_hash(&[2; 32]);
    }
    assert!(distill_rpc::capnp_transport::decode_version_poison(
        odd_windows_path
            .get_root_as_reader::<schema::version_poison::Reader<'_>>()
            .unwrap()
    )
    .is_err());
}
