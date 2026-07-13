use std::rc::Rc;
use std::sync::Arc;

use distill_rpc::capnp_transport::{schema, CapnpClient, RemoteConnectOutcome, StagedListener};
use distill_rpc::*;
use tokio::task::LocalSet;

fn target() -> TargetDefinition {
    TargetDefinition::canonical(
        "dev",
        TargetDefinitionHash([7; 32]),
        vec![LayoutEntry {
            type_uuid: TypeUuid([1; 16]),
            layout_digest: [2; 32],
        }],
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
        target().layout_registry,
        target().load_policy,
    )
    .unwrap()
}

fn server() -> Server {
    Server::new(StoreInstanceId([9; 16]), vec![target()]).unwrap()
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
                RemoteConnectOutcome::Connected { hub, instance } => {
                    assert_eq!(instance, StoreInstanceId([9; 16]));
                    hub
                }
                other => panic!("expected connected, got {other:?}"),
            };

            let snapshot_response = hub.snapshot_request().send().promise.await.unwrap();
            let snapshot_result = snapshot_response.get().unwrap().get_result().unwrap();
            let snapshot = match snapshot_result.which().unwrap() {
                schema::snapshot_result::Which::Snapshot(snapshot) => snapshot.unwrap(),
                _ => panic!("expected snapshot capability"),
            };

            let mut resolve = snapshot.resolve_request();
            resolve.get().set_uuid(&uuid.0);
            let resolve_response = resolve.send().promise.await.unwrap();
            let resolve_result = resolve_response.get().unwrap().get_result().unwrap();
            let terminal = match resolve_result.which().unwrap() {
                schema::resolve_call_result::Which::Terminal(terminal) => terminal.unwrap(),
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
                schema::fetch_call_result::Which::Terminal(terminal) => terminal.unwrap(),
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
                schema::subscribe_result::Which::Installed(installed) => installed.unwrap(),
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
                schema::snapshot_result::Which::Snapshot(snapshot) => snapshot.unwrap(),
                _ => panic!("refresh must remain valid under configuration poison"),
            };
            let mut poisoned_resolve = poisoned_snapshot.resolve_request();
            poisoned_resolve.get().set_uuid(&uuid.0);
            let poisoned_response = poisoned_resolve.send().promise.await.unwrap();
            let poisoned_result = poisoned_response.get().unwrap().get_result().unwrap();
            match poisoned_result.which().unwrap() {
                schema::resolve_call_result::Which::ConfigurationPoisoned(value) => {
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
                        vec![LayoutEntry {
                            type_uuid: TypeUuid([1; 16]),
                            layout_digest: [2; 32],
                        }],
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
                schema::resolve_call_result::Which::ReconnectRequired(reconnect) => {
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

            let mut malformed = client.root().connect_request();
            {
                let mut params = malformed.get();
                params.set_target("dev");
                params.set_target_def_hash(&[7; 31]);
                params.reborrow().init_layout_registry(0);
                params.set_layout_aggregate(&[0; 32]);
                params.reborrow().init_load_policy(0);
                params.set_policy_digest(&[0; 32]);
                params.set_protocol(PROTOCOL_VERSION);
                params.set_game_module_epoch(1);
            }
            let malformed_response = malformed.send().promise.await.unwrap();
            let malformed_result = malformed_response.get().unwrap().get_result().unwrap();
            match malformed_result.which().unwrap() {
                schema::connect_result::Which::Rejected(failure) => {
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

            let hub = match client.connect(&request()).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, .. } => hub,
                other => panic!("expected connected, got {other:?}"),
            };
            let snapshot_response = hub.snapshot_request().send().promise.await.unwrap();
            let snapshot_result = snapshot_response.get().unwrap().get_result().unwrap();
            let snapshot = match snapshot_result.which().unwrap() {
                schema::snapshot_result::Which::Snapshot(snapshot) => snapshot.unwrap(),
                _ => panic!("expected snapshot"),
            };
            let mut resolve = snapshot.resolve_request();
            resolve.get().set_uuid(&[1; 15]);
            let response = resolve.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::resolve_call_result::Which::Failure(failure) => {
                    assert_eq!(failure.unwrap().get_code(), 1001)
                }
                _ => panic!("wrong UUID width must be a typed failure"),
            }

            let mut fetch = hub.fetch_request();
            fetch.get().set_hash(&[2; 31]);
            let response = fetch.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            match result.which().unwrap() {
                schema::fetch_call_result::Which::Failure(failure) => {
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
