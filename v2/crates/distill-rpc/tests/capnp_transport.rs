use std::rc::Rc;
use std::sync::Arc;

use distill_core::attestation::{
    CompiledTypeRow, RegistryExtraFact, RegistryExtraRow, RegistryExtrasV1, SchemaNodeId,
};
use distill_rpc::capnp_transport::{schema, CapnpClient, RemoteConnectOutcome, StagedListener};
use distill_rpc::*;
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
    TargetDefinition::canonical(
        "dev",
        TargetDefinitionHash([definition; 32]),
        vec![compiled(definition)],
        vec![LoadPolicyEntry {
            type_uuid: TypeUuid([1; 16]),
            build_only: false,
        }],
    )
    .unwrap()
}

fn request() -> ConnectRequest {
    ConnectRequest::canonical(
        GameModuleEpoch(1),
        "dev",
        TargetDefinitionHash([7; 32]),
        target().compiled_registry,
        target().load_policy,
    )
    .unwrap()
}

fn server() -> Server {
    Server::new(StoreInstanceId([9; 16]), vec![target()]).unwrap()
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
async fn generated_rpc_system_round_trips_connect_snapshot_resolve_fetch_and_delta() {
    LocalSet::new()
        .run_until(async {
            let server = server();
            let uuid = AssetUuid([3; 16]);
            let hash = ContentHash([4; 32]);
            server
                .install_artifact(
                    hash,
                    ArtifactPayload {
                        structural: Arc::from([10_u8, 11, 12]),
                        blobs: Vec::new(),
                    },
                )
                .unwrap();
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
                } => {
                    assert_eq!(instance, StoreInstanceId([9; 16]));
                    assert_eq!(policy_generation, 0);
                    assert_eq!(target_generation, 0);
                    assert_eq!(attestation_generation, 0);
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
                    assert_eq!(success.unwrap().get_installed_attestation_generation(), 1)
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
            assert_eq!(terminal.get_basis().unwrap().get_version(), 1);
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
            assert_eq!(terminal.get_basis().unwrap().get_version(), 1);
            let chunks = terminal.get_chunks().unwrap();
            let chunk_response = chunks.next_request().send().promise.await.unwrap();
            let chunk = chunk_response.get().unwrap();
            assert!(!chunk.get_done());
            assert_eq!(chunk.get_kind(), 0);
            assert_eq!(chunk.get_bytes().unwrap(), &[10, 11, 12]);

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
            assert_eq!(event.get_basis().unwrap().get_version(), 1);
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
            assert_eq!(live_event.get_basis().unwrap().get_version(), 2);
            assert!(matches!(
                live_event.which().unwrap(),
                schema::stream_event::Which::Delta(_)
            ));

            // Configuration poison remains a typed result union over the
            // generated transport; refresh itself remains safe.
            let poison = ConfigurationPoison::new(
                ConfigurationPoisonCode::INVALID_CANDIDATE,
                "test/poison",
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
                    assert_eq!(value.get_code(), poison.code.0);
                    assert_eq!(value.get_reason_hash().unwrap(), &poison.reason_hash);
                }
                _ => panic!("resolve must return typed configuration poison"),
            }

            // The stream notification is advisory; stale capabilities are
            // independently fenced by the generated server adapter.
            server
                .replace_target(
                    TargetDefinition::canonical(
                        "dev",
                        TargetDefinitionHash([8; 32]),
                        vec![compiled(8)],
                        vec![LoadPolicyEntry {
                            type_uuid: TypeUuid([1; 16]),
                            build_only: false,
                        }],
                    )
                    .unwrap(),
                )
                .unwrap();
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
                    if failure.code == AttestationFailureCode::TARGET_DEFINITION_MISMATCH
                        && failure.subject
                            == AttestationSubject::TargetDefinition(
                                TargetDefinitionFailureSubject::DigestMismatch {
                                    expected: TargetDefinitionHash([7; 32]),
                                    observed: TargetDefinitionHash([8; 32]),
                                }
                            )
            ));

            let wrong_type = ConnectRequest::canonical(
                GameModuleEpoch(1),
                "dev",
                TargetDefinitionHash([7; 32]),
                vec![compiled(8)],
                vec![LoadPolicyEntry {
                    type_uuid: TypeUuid([1; 16]),
                    build_only: false,
                }],
            )
            .unwrap();
            assert!(matches!(
                client.connect(&wrong_type).await.unwrap(),
                RemoteConnectOutcome::AttestationFailure(failure)
                    if failure.code == AttestationFailureCode::COMPILED_TYPE_MISMATCH
                        && failure.subject
                            == AttestationSubject::SpecificType(TypeUuid([1; 16]))
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
                schema::connect_call::Which::Error(failure) => {
                    let failure = failure.unwrap();
                    assert_eq!(failure.get_code(), 1002);
                    assert!(failure
                        .get_message()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .contains("32 bytes"));
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
                schema::connect_call::Which::Error(failure) => {
                    assert_eq!(failure.unwrap().get_code(), 1005);
                }
                _ => panic!("unknown DSRE version must be a typed wire rejection"),
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
                failure.get_code(),
                AttestationFailureCode::TARGET_DEFINITION_MISMATCH.0
            );
            let subject = failure.get_subject().unwrap();
            let target = match subject.which().unwrap() {
                schema::attestation_subject::Which::TargetDefinition(target) => target.unwrap(),
                _ => panic!("target mismatch must use the target-definition subject"),
            };
            let mismatch = match target.which().unwrap() {
                schema::target_definition_subject::Which::DigestMismatch(mismatch) => {
                    mismatch.unwrap()
                }
                _ => panic!("target subject must carry both digests"),
            };
            assert_eq!(mismatch.get_expected().unwrap(), &[7; 32]);
            assert_eq!(mismatch.get_observed().unwrap(), &[8; 32]);

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
