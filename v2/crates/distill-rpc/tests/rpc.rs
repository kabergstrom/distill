use std::collections::BTreeSet;
use std::sync::Arc;

use distill_core::attestation::{
    AttestationError, ReferenceStrength, RegistryExtraFact, RegistryExtraRow, RegistryExtrasV1,
    RegistryPathStep, SchemaNodeId,
};
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

fn target_with(definition_hash: u8, policies: &[(u8, bool)]) -> TargetDefinition {
    TargetDefinition::canonical(
        "dev",
        target_hash(definition_hash),
        policies
            .iter()
            .map(|(byte, build_only)| compiled(*byte, *build_only))
            .collect(),
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
        policies
            .iter()
            .map(|(byte, build_only)| compiled(*byte, *build_only))
            .collect(),
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

fn authoring_entry(byte: u8, role: AuthoringEntryRole) -> AuthoringEntry {
    AuthoringEntry {
        uuid: asset_id(byte),
        bundle: BundleUuid([byte.wrapping_add(1); 16]),
        local_id: format!("entry-{byte}"),
        normalized_path: format!("bundle-{byte}.asset"),
        type_uuid: type_id(byte),
        terminal_type: type_id(byte),
        schema_hash: LogicalHash([byte.wrapping_add(10); 32]),
        role,
        tags: std::collections::BTreeMap::from([(
            "group".to_owned(),
            Some(format!("group-{byte}")),
        )]),
        value: AuthoringValue {
            canonical_value: Arc::from(&b"[0]"[..]),
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
    replacement.value.canonical_value = Arc::from(&b"{\"blob\":0}"[..]);
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
    let poison = ConfigurationPoison::new(
        ConfigurationPoisonCode::INVALID_CANDIDATE,
        "authoring-test/poison",
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
    assert!(matches!(
        ConnectRequest::canonical(
            GameModuleEpoch(1),
            "dev",
            target_hash(7),
            vec![compiled(1, false)],
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
        ConnectOutcome::Rejected(ConnectError::AttestationShape(
            AttestationShapeError::Compiled(AttestationError::CompiledDigestMismatch)
        ))
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
        ConnectOutcome::Rejected(ConnectError::CompiledTypeMismatch { type_uuid })
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
        ConnectOutcome::Rejected(ConnectError::CompiledTypeMismatch { type_uuid })
            if type_uuid == type_id(1)
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
        request.compiled_registry = vec![changed];
        request.dsca = CompiledTypeTable::canonical(request.compiled_registry.clone())
            .unwrap()
            .digest;
        assert!(matches!(
            server.root().connect(request),
            ConnectOutcome::Rejected(ConnectError::CompiledTypeMismatch { type_uuid })
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
            ConnectError::UnknownTarget {
                target: "missing".into(),
            },
            AttestationFailureCode::UNKNOWN_TARGET,
            AttestationSubject::TargetDefinition(TargetDefinitionFailureSubject::UnknownTarget(
                "missing".into(),
            )),
        ),
        (
            ConnectError::MissingCompiledType { type_uuid },
            AttestationFailureCode::MISSING_COMPILED_TYPE,
            AttestationSubject::SpecificType(type_uuid),
        ),
        (
            ConnectError::CompiledTypeMismatch { type_uuid },
            AttestationFailureCode::COMPILED_TYPE_MISMATCH,
            AttestationSubject::SpecificType(type_uuid),
        ),
        (
            ConnectError::MissingLoadPolicy { type_uuid },
            AttestationFailureCode::MISSING_LOAD_POLICY,
            AttestationSubject::SpecificType(type_uuid),
        ),
        (
            ConnectError::LoadPolicyMismatch {
                type_uuid,
                expected: false,
                got: true,
            },
            AttestationFailureCode::LOAD_POLICY_MISMATCH,
            AttestationSubject::SpecificType(type_uuid),
        ),
        (
            ConnectError::TargetDefinitionMismatch {
                expected: target_hash(7),
                got: target_hash(8),
            },
            AttestationFailureCode::TARGET_DEFINITION_MISMATCH,
            AttestationSubject::TargetDefinition(TargetDefinitionFailureSubject::DigestMismatch {
                expected: target_hash(7),
                observed: target_hash(8),
            }),
        ),
        (
            ConnectError::AttestationShape(AttestationShapeError::Compiled(
                AttestationError::TrailingBytes,
            )),
            AttestationFailureCode::COMPILED_REGISTRY_INVALID,
            AttestationSubject::CompiledRegistry,
        ),
        (
            ConnectError::AttestationShape(AttestationShapeError::Compiled(
                AttestationError::CompiledDigestMismatch,
            )),
            AttestationFailureCode::DSCA_AGGREGATE_MISMATCH,
            AttestationSubject::DscaAggregate,
        ),
        (
            ConnectError::AttestationShape(AttestationShapeError::PolicyDigestMismatch {
                expected: [1; 32],
                got: [2; 32],
            }),
            AttestationFailureCode::POLICY_PROJECTION_INVALID,
            AttestationSubject::PolicyProjection,
        ),
    ];
    for (error, code, subject) in cases {
        let failure = error.attestation_failure();
        assert_eq!(failure.code, code);
        assert_eq!(failure.subject, subject);
        assert!(!failure.message.is_empty());
    }
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
                    installed_attestation_generation: 1
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
            authoring: Vec::new(),
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
            authoring: Vec::new(),
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
    assert_eq!(hub.attestation_generation(), 0);

    // Pure snapshot metadata remains inspectable; it is not target-bound data.
    assert_eq!(snap.version(), InputVersion(0));
    assert_eq!(snap.configuration(), ConfigurationStatus::Ready);
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
    forged.dsca.0 = [77; 32];
    assert!(matches!(
        server.replace_target(forged),
        Err(AdminError::InvalidTargetAttestation(
            AttestationShapeError::Compiled(AttestationError::CompiledDigestMismatch)
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
