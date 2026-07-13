use std::collections::BTreeSet;
use std::sync::Arc;

use distill_rpc::*;

fn type_id(byte: u8) -> TypeUuid {
    TypeUuid([byte; 16])
}

fn asset_id(byte: u8) -> AssetUuid {
    AssetUuid([byte; 16])
}

fn content_hash(byte: u8) -> ContentHash {
    ContentHash([byte; 32])
}

fn target_hash(byte: u8) -> TargetDefinitionHash {
    TargetDefinitionHash([byte; 32])
}

fn layout(byte: u8) -> LayoutEntry {
    LayoutEntry {
        type_uuid: type_id(byte),
        layout_digest: [byte + 20; 32],
    }
}

fn policy(byte: u8, build_only: bool) -> LoadPolicyEntry {
    LoadPolicyEntry {
        type_uuid: type_id(byte),
        build_only,
    }
}

fn target_with(definition_hash: u8, policies: &[(u8, bool)]) -> TargetDefinition {
    TargetDefinition::canonical(
        "dev",
        target_hash(definition_hash),
        policies.iter().map(|(byte, _)| layout(*byte)).collect(),
        policies
            .iter()
            .map(|(byte, build_only)| policy(*byte, *build_only))
            .collect(),
    )
    .unwrap()
}

fn request_for(definition_hash: u8, epoch: u64, policies: &[(u8, bool)]) -> ConnectRequest {
    ConnectRequest::canonical(
        GameModuleEpoch(epoch),
        "dev",
        target_hash(definition_hash),
        policies.iter().map(|(byte, _)| layout(*byte)).collect(),
        policies
            .iter()
            .map(|(byte, build_only)| policy(*byte, *build_only))
            .collect(),
    )
    .unwrap()
}

fn server_with(policies: &[(u8, bool)]) -> Server {
    Server::new(StoreInstanceId([9; 16]), vec![target_with(7, policies)]).unwrap()
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
fn dsla_and_dslp_match_the_pinned_byte_grammar() {
    let layouts = vec![layout(1), layout(2)];
    let policies = vec![policy(1, false), policy(2, true)];

    let mut dsla = blake3::Hasher::new();
    dsla.update(b"DSLA");
    dsla.update(&[1]);
    dsla.update(&2_u32.to_le_bytes());
    for row in &layouts {
        dsla.update(&row.type_uuid.0);
        dsla.update(&row.layout_digest);
    }
    let mut dslp = blake3::Hasher::new();
    dslp.update(b"DSLP");
    dslp.update(&[1]);
    dslp.update(&2_u32.to_le_bytes());
    for row in &policies {
        dslp.update(&row.type_uuid.0);
        dslp.update(&[u8::from(row.build_only)]);
    }

    assert_eq!(
        compute_layout_aggregate(&layouts).unwrap(),
        *dsla.finalize().as_bytes()
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
            vec![layout(1), layout(1)],
            vec![policy(1, false), policy(1, false)],
        ),
        Err(AttestationShapeError::LayoutRowsNotStrictlySorted { .. })
    ));
    assert!(matches!(
        ConnectRequest::canonical(
            GameModuleEpoch(1),
            "dev",
            target_hash(7),
            vec![layout(1)],
            vec![policy(2, false)],
        ),
        Err(AttestationShapeError::RegisteredTypeSetMismatch { .. })
    ));
}

#[test]
fn connect_accepts_a_client_subset_and_binds_its_policy_to_every_basis() {
    let server = server_with(&[(1, false), (2, true)]);
    let hub = connect(&server, &[(1, false)]);
    let snap = snapshot(&hub);

    assert_eq!(snap.stamp().instance, StoreInstanceId([9; 16]));
    assert_eq!(snap.basis().load_policy.rows, vec![policy(1, false)]);
    assert_eq!(
        snap.basis().load_policy.digest,
        compute_policy_digest(&[policy(1, false)]).unwrap()
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
fn connect_rejects_unsorted_forged_and_mismatched_layout_attestations() {
    let server = server_with(&[(1, false), (2, true)]);
    let mut unsorted = request_for(7, 1, &[(1, false), (2, true)]);
    unsorted.layout_registry.reverse();
    assert!(matches!(
        server.root().connect(unsorted),
        ConnectOutcome::Rejected(ConnectError::AttestationShape(
            AttestationShapeError::LayoutRowsNotStrictlySorted { .. }
        ))
    ));

    let mut forged = request_for(7, 1, &[(1, false)]);
    forged.layout_aggregate = [55; 32];
    assert!(matches!(
        server.root().connect(forged),
        ConnectOutcome::Rejected(ConnectError::AttestationShape(
            AttestationShapeError::LayoutAggregateMismatch { .. }
        ))
    ));

    let mut mismatch = request_for(7, 1, &[(1, false)]);
    mismatch.layout_registry[0].layout_digest = [99; 32];
    mismatch.layout_aggregate = compute_layout_aggregate(&mismatch.layout_registry).unwrap();
    assert!(matches!(
        server.root().connect(mismatch),
        ConnectOutcome::Rejected(ConnectError::LayoutMismatch {
            type_uuid,
            ..
        }) if type_uuid == type_id(1)
    ));

    assert!(matches!(
        server.root().connect(request_for(7, 1, &[(3, false)])),
        ConnectOutcome::Rejected(ConnectError::MissingLayout { type_uuid })
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
        ConnectOutcome::Rejected(ConnectError::LoadPolicyMismatch {
            type_uuid,
            expected: false,
            got: true,
        }) if type_uuid == type_id(1)
    ));
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
        RpcResult::Success(())
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
fn resolve_path_and_fetch_terminal_outcomes_all_carry_the_snapshot_basis() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let built = asset_id(1);
    let failed = asset_id(2);
    let drifted = asset_id(3);
    let deleted = asset_id(4);
    let hash = content_hash(11);
    server
        .install_artifact(
            hash,
            ArtifactPayload {
                structural: Arc::from(vec![5_u8; 70_000]),
                blobs: vec![Arc::from(vec![8_u8; 3])],
            },
        )
        .unwrap();
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
    assert_eq!(second.bytes.len(), 4_464);
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
    let first = content_hash(1);
    let second = content_hash(2);
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
    assert_eq!(refreshed.version(), InputVersion(2));
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
    assert_eq!(snap.version(), InputVersion(2));
    assert_eq!(
        snap.resolve(uuid).success().unwrap().value,
        ResolveResult::Deleted { at: deleted_at }
    );
}

#[test]
fn configuration_poison_is_snapshot_pinned_and_typed_without_blocking_safe_reads() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let hash = content_hash(1);
    server
        .install_artifact(
            hash,
            ArtifactPayload {
                structural: Arc::from([1_u8, 2, 3]),
                blobs: Vec::new(),
            },
        )
        .unwrap();
    let poison = ConfigurationPoison::new(
        ConfigurationPoisonCode::NON_LOOPBACK_ADDRESS,
        "daemon.address/non-loopback",
        "daemon.address is not loopback",
    );
    server
        .commit(Commit {
            configuration: Some(ConfigurationStatus::Poisoned(poison.clone())),
            ..Commit::default()
        })
        .unwrap();
    let poisoned = snapshot(&hub);

    assert_eq!(
        poisoned.configuration(),
        ConfigurationStatus::Poisoned(poison.clone())
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

    let same_reason_new_words = ConfigurationPoison::new(
        ConfigurationPoisonCode::NON_LOOPBACK_ADDRESS,
        "daemon.address/non-loopback",
        "translated diagnostic",
    );
    assert_eq!(poison.reason_hash, same_reason_new_words.reason_hash);
    assert_ne!(poison.message, same_reason_new_words.message);
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
            paths: vec![PathMutation::Set {
                path: "watched.asset".to_owned(),
                candidates: BTreeSet::from([watched]),
            }],
            configuration: None,
        })
        .unwrap();
    server
        .commit(Commit {
            assets: vec![
                set_asset(watched, StoredResolve::Deleted, AssetDeltaState::Deleted),
                set_asset(ignored, StoredResolve::Deleted, AssetDeltaState::Deleted),
            ],
            paths: vec![PathMutation::Remove {
                path: "watched.asset".to_owned(),
            }],
            configuration: None,
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

    // Pure snapshot metadata remains inspectable; it is not target-bound data.
    assert_eq!(snap.version(), InputVersion(0));
    assert_eq!(snap.configuration(), ConfigurationStatus::Ready);
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
fn forged_target_replacements_are_rejected_before_generation_or_version_changes() {
    let server = server_with(&[(1, false)]);
    let hub = connect(&server, &[(1, false)]);
    let snap = snapshot(&hub);
    let before = server.current_stamp();
    let mut forged = target_with(8, &[(1, false)]);
    forged.layout_aggregate = [77; 32];
    assert!(matches!(
        server.replace_target(forged),
        Err(AdminError::InvalidTargetAttestation(
            AttestationShapeError::LayoutAggregateMismatch { .. }
        ))
    ));
    assert_eq!(server.current_stamp(), before);
    assert!(matches!(snap.resolve(asset_id(1)), RpcResult::Success(_)));

    let mut forged_initial = target_with(7, &[(1, false)]);
    forged_initial.policy_digest = [88; 32];
    assert!(matches!(
        Server::new(StoreInstanceId([1; 16]), vec![forged_initial]),
        Err(AttestationShapeError::PolicyDigestMismatch { .. })
    ));
}

#[test]
fn content_hash_records_are_immutable() {
    let server = server_with(&[(1, false)]);
    let hash = content_hash(1);
    let first = ArtifactPayload {
        structural: Arc::from([1_u8]),
        blobs: vec![],
    };
    assert_eq!(server.install_artifact(hash, first.clone()), Ok(()));
    assert_eq!(server.install_artifact(hash, first), Ok(()));
    assert_eq!(
        server.install_artifact(
            hash,
            ArtifactPayload {
                structural: Arc::from([2_u8]),
                blobs: vec![],
            }
        ),
        Err(AdminError::ArtifactAlreadyExistsWithDifferentPayload { hash })
    );
}
