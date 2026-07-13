//! §13 pipeline-side metadata: the `pipeline_state` row (dylib hash,
//! load-policy digest, staged-candidate poison), the `tools` ToolEpoch
//! table, and the source-controlled schema-lineage projection that gates
//! automatic migration diffs (§11).

use std::collections::BTreeMap;
use std::sync::Arc;

use distill_core::attestation::CompiledAttestationDigest;
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
use distill_store::bundles::AssetRecord;
use distill_store::pipeline::{
    AcceptedSchemaEpoch, AcceptedTypeLineage, HardStopReason, LineageClass, LineageStamp,
    ReverseMigrationEdge, SchemaLineageManifest, SchemaReactivationRequest, SchemaRollbackRequest,
    TypeAuthorityState, VerifiedSchemaLineageManifest,
};
use distill_store::state::{
    PipelineEpoch, PipelineState, PipelineUnavailable, Registration, RegistrationKind,
};
use distill_store::{RetiredTypeReference, Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

fn epoch(n: u8) -> PipelineEpoch {
    PipelineEpoch {
        dylib_hash: [n; 32],
        load_policy_digest: [n.wrapping_add(1); 32],
        compiled_types: CompiledAttestationDigest([n.wrapping_add(2); 32]),
        target_set: CanonicalTargetSet::canonical(vec![TargetSetRow {
            name: format!("target-{n}"),
            target_definition_hash: [n.wrapping_add(3); 32],
        }])
        .unwrap(),
        schema_registry: BTreeMap::new(),
        registrations: vec![
            Registration {
                kind: RegistrationKind::Importer,
                id: "gltf".into(),
                version: 2,
            },
            Registration {
                kind: RegistrationKind::Processor,
                id: "tex".into(),
                version: 5,
            },
        ],
    }
}

fn epoch_with_registry(n: u8, rows: &[(TypeUuid, LogicalHash)]) -> PipelineEpoch {
    let mut epoch = epoch(n);
    epoch.schema_registry = rows.iter().copied().collect();
    epoch
}

fn project_empty(store: &mut Store) {
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(1, SchemaLineageManifest::default()))
        })
        .unwrap();
}

fn verified(n: u8, manifest: SchemaLineageManifest) -> VerifiedSchemaLineageManifest {
    VerifiedSchemaLineageManifest::from_verified_source(ContentHash([n; 32]), manifest)
}

fn require_candidate(
    store: &mut Store,
    candidate: &PipelineEpoch,
) -> distill_store::state::SchemaManifestBasis {
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(candidate))
        .unwrap();
    match store.pipeline_state().unwrap().unwrap() {
        PipelineState::SchemaAcceptanceRequired { required, .. } => required.manifest,
        other => panic!("candidate unexpectedly ready: {other:?}"),
    }
}

fn h(n: u8) -> LogicalHash {
    LogicalHash([n; 32])
}

const T: TypeUuid = TypeUuid([4u8; 16]);

// ---- pipeline_state row ----

#[test]
fn no_pipeline_state_until_first_publication() {
    let (_d, store) = store();
    assert!(store.pipeline_state().unwrap().is_none());
}

#[test]
fn publishing_an_epoch_roundtrips_identity_and_registrations() {
    let (_d, mut store) = store();
    project_empty(&mut store);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch(3)))
        .unwrap();
    let state = store.pipeline_state().unwrap().expect("published");
    let got = state.epoch().expect("ready");
    assert_eq!(got.dylib_hash, [3u8; 32]);
    assert_eq!(got.load_policy_digest, [4u8; 32]);
    assert_eq!(got.target_set, epoch(3).target_set);
    let mut regs = got.registrations.clone();
    regs.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(regs.len(), 2);
    assert_eq!(regs[0].id, "gltf");
    assert_eq!(regs[0].kind, RegistrationKind::Importer);
    assert_eq!(regs[1].version, 5);
}

#[test]
fn a_rejected_candidate_still_publishes_as_poison() {
    // §13: failure publishes the version carrying a pipeline poison —
    // the prior epoch is never silently retained as the new version's
    // code, and the version is never dropped.
    let (_d, mut store) = store();
    project_empty(&mut store);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch(3)))
        .unwrap();
    store
        .input_transaction(|txn| txn.publish_pipeline_poison("dup processor id `tex`"))
        .unwrap();

    let state = store.pipeline_state().unwrap().expect("still published");
    match &state {
        PipelineState::Poisoned { error, last_good } => {
            assert!(error.error.contains("dup processor id"));
            // last_good is residency bookkeeping only — present, but
            // epoch() still refuses.
            let last: &Arc<PipelineEpoch> = last_good.as_ref().expect("prior epoch recorded");
            assert_eq!(last.dylib_hash, [3u8; 32]);
        }
        other => panic!("expected Poisoned, got {other:?}"),
    }
    assert!(state.epoch().is_err());
}

#[test]
fn poison_with_no_prior_epoch_has_no_last_good() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| txn.publish_pipeline_poison("first candidate invalid"))
        .unwrap();
    match store.pipeline_state().unwrap().expect("published") {
        PipelineState::Poisoned { last_good, .. } => assert!(last_good.is_none()),
        other => panic!("expected Poisoned, got {other:?}"),
    }
}

#[test]
fn the_next_successful_swap_publishes_over_the_poison() {
    let (_d, mut store) = store();
    project_empty(&mut store);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch(3)))
        .unwrap();
    store
        .input_transaction(|txn| txn.publish_pipeline_poison("bad candidate"))
        .unwrap();
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch(7)))
        .unwrap();
    let state = store.pipeline_state().unwrap().expect("published");
    assert_eq!(state.epoch().expect("healed").dylib_hash, [7u8; 32]);
}

#[test]
fn ready_requires_exact_candidate_registry_and_manifest_cursor_equality() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                2,
                manifest(&[(T, accepted(&[h(1)], 0))]),
            ))
        })
        .unwrap();
    let candidate = epoch_with_registry(3, &[(T, h(1))]);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&candidate))
        .unwrap();

    let state = store.pipeline_state().unwrap().expect("published");
    let ready = state.epoch().expect("exact registry is ready");
    assert_eq!(ready.schema_registry, BTreeMap::from([(T, h(1))]));
}

#[test]
fn candidate_publication_requires_a_verified_source_manifest() {
    let (_d, mut store) = store();
    let candidate = epoch_with_registry(2, &[]);
    let err = store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&candidate))
        .unwrap_err();
    assert!(matches!(err, StoreError::LineageManifestUnavailable));
    assert_eq!(store.input_version().0, 0);
    assert!(store.pipeline_state().unwrap().is_none());
}

#[test]
fn missing_extra_and_unequal_registry_rows_publish_stable_acceptance_required() {
    let other = TypeUuid([5u8; 16]);
    let cases = [
        (
            manifest(&[(T, accepted(&[h(1)], 0))]),
            epoch_with_registry(1, &[]),
            (T, None, Some(h(1))),
        ),
        (
            SchemaLineageManifest::default(),
            epoch_with_registry(2, &[(T, h(1))]),
            (T, Some(h(1)), None),
        ),
        (
            manifest(&[(T, accepted(&[h(1)], 0)), (other, accepted(&[h(7)], 0))]),
            epoch_with_registry(3, &[(T, h(2)), (other, h(7))]),
            (T, Some(h(2)), Some(h(1))),
        ),
    ];

    for (authority, candidate, expected) in cases {
        let (_d, mut store) = store();
        store
            .input_transaction(|txn| {
                txn.project_verified_lineage_manifest(&verified(3, authority.clone()))
            })
            .unwrap();
        store
            .input_transaction(|txn| txn.publish_pipeline_epoch(&candidate))
            .unwrap();

        let first = store.pipeline_state().unwrap().expect("published");
        let second = store.pipeline_state().unwrap().expect("stable reread");
        match (&first, &second) {
            (
                PipelineState::SchemaAcceptanceRequired { required: a, .. },
                PipelineState::SchemaAcceptanceRequired { required: b, .. },
            ) => {
                assert_eq!(a, b);
                assert_eq!(a.candidate.dylib_hash, candidate.dylib_hash);
                assert!(a
                    .mismatches
                    .iter()
                    .any(|row| { (row.type_uuid, row.candidate, row.manifest) == expected }));
            }
            other => panic!("expected stable SchemaAcceptanceRequired, got {other:?}"),
        }
        assert!(matches!(
            first.epoch(),
            Err(PipelineUnavailable::SchemaAcceptanceRequired(_))
        ));
    }
}

#[test]
fn candidate_bound_accept_rejects_stale_base_and_then_publishes_atomically() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                4,
                manifest(&[(T, accepted(&[h(1)], 0))]),
            ))
        })
        .unwrap();
    let candidate = epoch_with_registry(4, &[(T, h(2))]);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&candidate))
        .unwrap();
    let required = match store.pipeline_state().unwrap().unwrap() {
        PipelineState::SchemaAcceptanceRequired { required, .. } => required,
        other => panic!("expected acceptance requirement, got {other:?}"),
    };
    let base = required.manifest;
    let stale = distill_store::state::SchemaManifestBasis {
        manifest_hash: ContentHash([99; 32]),
        current_cursors: base.current_cursors.clone(),
    };
    let stale_cursors = distill_store::state::SchemaManifestBasis {
        manifest_hash: base.manifest_hash,
        current_cursors: BTreeMap::from([(T, h(99))]),
    };
    let proposed = verified(7, manifest(&[(T, accepted(&[h(1), h(2)], 1))]));
    let before_version = store.input_version();

    let err = store
        .input_transaction(|txn| {
            txn.accept_schema_candidate(&candidate, &stale, &proposed, T, h(2))
        })
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleSchemaManifestBase { expected, actual }
            if *expected == stale && actual.as_deref() == Some(&base)
    ));
    let err = store
        .input_transaction(|txn| {
            txn.accept_schema_candidate(&candidate, &stale_cursors, &proposed, T, h(2))
        })
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleSchemaManifestBase { expected, actual }
            if *expected == stale_cursors && actual.as_deref() == Some(&base)
    ));
    assert_eq!(store.input_version(), before_version);
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(1)));
    assert!(matches!(
        store.pipeline_state().unwrap().unwrap(),
        PipelineState::SchemaAcceptanceRequired { .. }
    ));

    store
        .input_transaction(|txn| txn.accept_schema_candidate(&candidate, &base, &proposed, T, h(2)))
        .unwrap();
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(2)));
    assert_eq!(store.lineage(T).unwrap().len(), 2);
    assert_eq!(
        store
            .pipeline_state()
            .unwrap()
            .unwrap()
            .epoch()
            .unwrap()
            .dylib_hash,
        candidate.dylib_hash
    );
}

#[test]
fn verified_proposed_manifest_is_the_only_authority_for_acceptance() {
    let other = TypeUuid([8u8; 16]);
    let (_d, mut store) = store();
    let initial = manifest(&[(T, accepted(&[h(1)], 0)), (other, accepted(&[h(7)], 0))]);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(40, initial.clone()))
        })
        .unwrap();
    let candidate = epoch_with_registry(12, &[(T, h(2)), (other, h(7))]);
    let base = require_candidate(&mut store, &candidate);
    let before_version = store.input_version();
    // T's requested append is present, but the source handoff also sneaks in
    // an unrelated accepted epoch. The store must reject the whole verified
    // proposal instead of treating its SQLite projection as authority.
    let overbroad = verified(
        41,
        manifest(&[
            (T, accepted(&[h(1), h(2)], 1)),
            (other, accepted(&[h(7), h(8)], 1)),
        ]),
    );
    let err = store
        .input_transaction(|txn| {
            txn.accept_schema_candidate(&candidate, &base, &overbroad, T, h(2))
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::InvalidLineageManifest { .. }));
    assert_eq!(store.input_version(), before_version);
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(1)));
    assert_eq!(store.lineage(T).unwrap().len(), 1);
    assert_eq!(store.lineage(other).unwrap().len(), 1);
    assert!(matches!(
        store.pipeline_state().unwrap().unwrap(),
        PipelineState::SchemaAcceptanceRequired { .. }
    ));
}

#[test]
fn acceptance_is_bound_to_the_pending_candidate_and_never_partially_publishes() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                5,
                manifest(&[(T, accepted(&[h(1)], 0))]),
            ))
        })
        .unwrap();
    let pending = epoch_with_registry(6, &[(T, h(2))]);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&pending))
        .unwrap();
    let required = match store.pipeline_state().unwrap().unwrap() {
        PipelineState::SchemaAcceptanceRequired { required, .. } => required,
        other => panic!("expected acceptance requirement, got {other:?}"),
    };
    let base = required.manifest;
    let proposed = verified(8, manifest(&[(T, accepted(&[h(1), h(2)], 1))]));
    let wrong_candidate = epoch_with_registry(7, &[(T, h(2))]);
    let before_version = store.input_version();

    let err = store
        .input_transaction(|txn| {
            txn.accept_schema_candidate(&wrong_candidate, &base, &proposed, T, h(2))
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::StaleSchemaCandidate { .. }));
    assert_eq!(store.input_version(), before_version);
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(1)));
    assert_eq!(store.lineage(T).unwrap().len(), 1);
    match store.pipeline_state().unwrap().unwrap() {
        PipelineState::SchemaAcceptanceRequired { required, .. } => {
            assert_eq!(required.candidate.dylib_hash, pending.dylib_hash)
        }
        other => panic!("pending state changed after rejected accept: {other:?}"),
    }
}

#[test]
fn candidate_bound_rollback_rejects_stale_base_then_moves_only_the_cursor() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                6,
                manifest(&[(T, accepted(&[h(1), h(2)], 1))]),
            ))
        })
        .unwrap();
    let candidate = epoch_with_registry(8, &[(T, h(1))]);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&candidate))
        .unwrap();
    let required = match store.pipeline_state().unwrap().unwrap() {
        PipelineState::SchemaAcceptanceRequired { required, .. } => required,
        other => panic!("expected acceptance requirement, got {other:?}"),
    };
    let base = required.manifest;
    let stale = distill_store::state::SchemaManifestBasis {
        manifest_hash: ContentHash([98; 32]),
        current_cursors: base.current_cursors.clone(),
    };
    let proposed = verified(9, manifest(&[(T, accepted(&[h(1), h(2)], 0))]));
    let reverse = [ReverseMigrationEdge {
        from: h(2),
        to: h(1),
    }];

    let err = store
        .input_transaction(|txn| {
            txn.rollback_schema_candidate(
                &candidate,
                &stale,
                &proposed,
                SchemaRollbackRequest {
                    type_uuid: T,
                    target: h(1),
                    live_schema_hashes: &[h(2)],
                    reverse_edges: &reverse,
                },
            )
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::StaleSchemaManifestBase { .. }));
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(2)));

    store
        .input_transaction(|txn| {
            txn.rollback_schema_candidate(
                &candidate,
                &base,
                &proposed,
                SchemaRollbackRequest {
                    type_uuid: T,
                    target: h(1),
                    live_schema_hashes: &[h(2)],
                    reverse_edges: &reverse,
                },
            )
        })
        .unwrap();
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(1)));
    assert_eq!(store.lineage(T).unwrap().len(), 2);
    assert!(store.pipeline_state().unwrap().unwrap().epoch().is_ok());
}

#[test]
fn retirement_and_exact_current_reactivation_preserve_history_and_gate_ready() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                50,
                manifest(&[(T, accepted(&[h(1), h(2)], 1))]),
            ))
        })
        .unwrap();
    let active_stamp = store.current_lineage_stamp(T).unwrap().unwrap();

    let retirement_candidate = epoch_with_registry(20, &[]);
    let base = require_candidate(&mut store, &retirement_candidate);
    let retired_source = verified(51, manifest(&[(T, retired(&[h(1), h(2)], 1))]));
    let stale = distill_store::state::SchemaManifestBasis {
        manifest_hash: ContentHash([99; 32]),
        current_cursors: base.current_cursors.clone(),
    };
    let err = store
        .input_transaction(|txn| {
            txn.retire_schema_candidate(
                &retirement_candidate,
                &stale,
                txn.base_stamp(),
                &retired_source,
                T,
                &[],
            )
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::StaleSchemaManifestBase { .. }));
    assert_eq!(store.lineage(T).unwrap().len(), 2);
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(2)));
    assert_eq!(
        store.current_lineage_stamp(T).unwrap().unwrap(),
        active_stamp,
        "authority is deliberately outside DSSL"
    );

    store
        .input_transaction(|txn| {
            txn.retire_schema_candidate(
                &retirement_candidate,
                &base,
                txn.base_stamp(),
                &retired_source,
                T,
                &[],
            )
        })
        .unwrap();
    assert_eq!(store.lineage(T).unwrap().len(), 2);
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(2)));
    assert_eq!(
        store.current_lineage_stamp(T).unwrap().unwrap(),
        active_stamp,
        "authority is deliberately outside DSSL"
    );
    assert!(store.pipeline_state().unwrap().unwrap().epoch().is_ok());

    let reactivation_candidate = epoch_with_registry(21, &[(T, h(2))]);
    let retired_base = require_candidate(&mut store, &reactivation_candidate);
    assert!(retired_base.current_cursors.is_empty());
    let active_source = verified(52, manifest(&[(T, accepted(&[h(1), h(2)], 1))]));
    store
        .input_transaction(|txn| {
            txn.reactivate_schema_candidate(
                &reactivation_candidate,
                &retired_base,
                &active_source,
                SchemaReactivationRequest {
                    type_uuid: T,
                    live_schema_hashes: &[],
                    reverse_edges: &[],
                },
            )
        })
        .unwrap();
    assert_eq!(store.lineage(T).unwrap().len(), 2);
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(2)));
    assert!(store.pipeline_state().unwrap().unwrap().epoch().is_ok());

    // The exact authority state, including retired_from, survived SQLite
    // projection and was consumed by the verified transition.
    store
        .input_transaction(|txn| txn.project_verified_lineage_manifest(&active_source))
        .unwrap();
}

#[test]
fn retirement_is_blocked_by_migration_endpoints_and_live_authored_entries() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                53,
                manifest(&[(T, accepted(&[h(1)], 0))]),
            ))
        })
        .unwrap();
    let candidate = epoch_with_registry(22, &[]);
    let base = require_candidate(&mut store, &candidate);
    let proposed = verified(54, manifest(&[(T, retired(&[h(1)], 0))]));

    let err = store
        .input_transaction(|txn| {
            txn.retire_schema_candidate(&candidate, &base, txn.base_stamp(), &proposed, T, &[h(1)])
        })
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::SchemaRetirementBlocked {
            live_assets: 0,
            live_migration_endpoints: 1,
            ..
        }
    ));

    store
        .input_transaction(|txn| {
            txn.upsert_asset(&AssetRecord {
                asset: AssetUuid([77; 16]),
                bundle: BundleUuid([78; 16]),
                local_id: "live".into(),
                type_uuid: T,
                logical_hash: h(1),
                authoring_only: false,
                tags: vec![],
            })
        })
        .unwrap();
    let err = store
        .input_transaction(|txn| {
            txn.retire_schema_candidate(&candidate, &base, txn.base_stamp(), &proposed, T, &[])
        })
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::SchemaRetirementBlocked {
            live_assets: 1,
            live_migration_endpoints: 0,
            ..
        }
    ));
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(1)));
}

#[test]
fn retirement_requires_exact_control_basis_and_blocks_later_type_references() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                61,
                manifest(&[(T, accepted(&[h(1)], 0))]),
            ))
        })
        .unwrap();

    let stale_control_basis = store.stamp();
    let candidate = epoch_with_registry(27, &[]);
    let base = require_candidate(&mut store, &candidate);
    let proposed = verified(62, manifest(&[(T, retired(&[h(1)], 0))]));
    let current_control_basis = store.stamp();

    let err = store
        .input_transaction(|txn| {
            txn.retire_schema_candidate(&candidate, &base, stale_control_basis, &proposed, T, &[])
        })
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleControlSnapshotBasis {
            provided,
            current
        } if provided == stale_control_basis && current == current_control_basis
    ));
    assert_eq!(store.stamp(), current_control_basis);

    store
        .input_transaction(|txn| {
            txn.retire_schema_candidate(&candidate, &base, current_control_basis, &proposed, T, &[])
        })
        .unwrap();

    let retired_stamp = store.stamp();
    let asset = AssetUuid([79; 16]);
    let err = store
        .input_transaction(|txn| {
            txn.upsert_asset(&AssetRecord {
                asset,
                bundle: BundleUuid([80; 16]),
                local_id: "retired".into(),
                type_uuid: T,
                logical_hash: h(1),
                authoring_only: false,
                tags: vec![],
            })
        })
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::RetiredTypeReferenced {
            type_uuid: T,
            reference: RetiredTypeReference::Asset(found),
        } if found == asset
    ));
    assert_eq!(store.stamp(), retired_stamp);

    let err = store
        .input_transaction(|txn| txn.ensure_migration_endpoint_type_active(T, h(9)))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::RetiredTypeReferenced {
            type_uuid: T,
            reference: RetiredTypeReference::MigrationEndpoint(endpoint),
        } if endpoint == h(9)
    ));
    assert_eq!(store.stamp(), retired_stamp);
}

#[test]
fn reactivation_of_existing_noncurrent_digest_requires_rollback_coverage() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                55,
                manifest(&[(T, retired(&[h(1), h(2)], 1))]),
            ))
        })
        .unwrap();
    let candidate = epoch_with_registry(23, &[(T, h(1))]);
    let base = require_candidate(&mut store, &candidate);
    let proposed = verified(56, manifest(&[(T, accepted(&[h(1), h(2)], 0))]));

    let err = store
        .input_transaction(|txn| {
            txn.reactivate_schema_candidate(
                &candidate,
                &base,
                &proposed,
                SchemaReactivationRequest {
                    type_uuid: T,
                    live_schema_hashes: &[],
                    reverse_edges: &[],
                },
            )
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::IncompleteRollbackCoverage { .. }));

    let reverse = [ReverseMigrationEdge {
        from: h(2),
        to: h(1),
    }];
    store
        .input_transaction(|txn| {
            txn.reactivate_schema_candidate(
                &candidate,
                &base,
                &proposed,
                SchemaReactivationRequest {
                    type_uuid: T,
                    live_schema_hashes: &[],
                    reverse_edges: &reverse,
                },
            )
        })
        .unwrap();
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(1)));
    assert_eq!(store.lineage(T).unwrap().len(), 2);
}

#[test]
fn reactivation_appends_a_genuinely_new_candidate_digest() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                59,
                manifest(&[(T, retired(&[h(1)], 0))]),
            ))
        })
        .unwrap();
    let candidate = epoch_with_registry(26, &[(T, h(2))]);
    let base = require_candidate(&mut store, &candidate);
    let proposed = verified(60, manifest(&[(T, accepted(&[h(1), h(2)], 1))]));

    store
        .input_transaction(|txn| {
            txn.reactivate_schema_candidate(
                &candidate,
                &base,
                &proposed,
                SchemaReactivationRequest {
                    type_uuid: T,
                    live_schema_hashes: &[],
                    reverse_edges: &[],
                },
            )
        })
        .unwrap();
    let lineage = store.lineage(T).unwrap();
    assert_eq!(lineage.len(), 2);
    assert_eq!(lineage[1].schema_hash, h(2));
    assert_eq!(lineage[1].forward_parent, Some(0));
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(2)));
}

#[test]
fn forged_dsts_is_rejected_at_publish_and_again_at_schema_commit() {
    {
        let (_d, mut store) = store();
        project_empty(&mut store);
        let mut forged = epoch(24);
        forged.target_set.digest.0[0] ^= 1;
        let err = store
            .input_transaction(|txn| txn.publish_pipeline_epoch(&forged))
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidTargetSet(_)));
        let mut forged_rows = epoch(24);
        forged_rows.target_set.rows[0].name = "targe\u{301}t-24".into();
        let err = store
            .input_transaction(|txn| txn.publish_pipeline_epoch(&forged_rows))
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidTargetSet(_)));
        assert!(store.pipeline_state().unwrap().is_none());
    }

    let (d, mut store2) = store();
    store2
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                57,
                manifest(&[(T, accepted(&[h(1)], 0))]),
            ))
        })
        .unwrap();
    let candidate = epoch_with_registry(25, &[(T, h(2))]);
    let base = require_candidate(&mut store2, &candidate);
    let proposed = verified(58, manifest(&[(T, accepted(&[h(1), h(2)], 1))]));
    let conn = rusqlite::Connection::open(d.path().join(".distill/meta.sqlite")).unwrap();
    conn.execute(
        "UPDATE pipeline_candidate_target_set SET target_definition_hash = ?1",
        [[0u8; 32].as_slice()],
    )
    .unwrap();
    drop(conn);
    let before = store2.input_version();
    let err = store2
        .input_transaction(|txn| txn.accept_schema_candidate(&candidate, &base, &proposed, T, h(2)))
        .unwrap_err();
    assert!(matches!(err, StoreError::InvalidTargetSet(_)));
    assert_eq!(store2.input_version(), before);
    assert_eq!(store2.lineage_current(T).unwrap(), Some(h(1)));
}

// ---- tools: the ToolEpoch table ----

#[test]
fn staging_a_tool_is_content_addressed_and_input_versioned() {
    // §13: tool key → (staged copy path, content hash) — registering
    // stages a content-addressed copy under daemon state and publishes
    // the mapping at an input version.
    let (_d, mut store) = store();
    let binary = b"#!/bin/sh\necho v1\n";
    let expected_hash = *blake3::hash(binary).as_bytes();

    let (staged, v) = store
        .input_transaction(|txn| txn.stage_tool("shaderc", binary))
        .unwrap();
    assert_eq!(staged.content_hash, expected_hash);
    assert_eq!(staged.input_version, v);
    assert!(staged.path.is_file(), "the staged copy exists");
    assert_eq!(std::fs::read(&staged.path).unwrap(), binary);

    let resolved = store.tool("shaderc").unwrap().expect("registered");
    assert_eq!(resolved.content_hash, expected_hash);
    assert_eq!(resolved.path, staged.path);
    assert!(store.tool("unknown-tool").unwrap().is_none());
}

#[test]
fn replacing_a_tool_republishes_and_staged_copies_coexist() {
    // §9: staged versions coexist — a swap mid-epoch invalidates traces
    // into rebuilds that run the new copy at the new version; an old
    // snapshot's staged bytes stay where its ToolEpoch mapping put them.
    let (_d, mut store) = store();
    let (v1, _) = store
        .input_transaction(|txn| txn.stage_tool("shaderc", b"tool v1"))
        .unwrap();
    let (v2, ver2) = store
        .input_transaction(|txn| txn.stage_tool("shaderc", b"tool v2"))
        .unwrap();
    assert_ne!(v1.content_hash, v2.content_hash);
    assert_ne!(
        v1.path, v2.path,
        "content-addressed: different bytes, different path"
    );
    assert!(v1.path.is_file(), "the old staged copy coexists");
    assert!(v2.path.is_file());

    let current = store.tool("shaderc").unwrap().unwrap();
    assert_eq!(current.content_hash, v2.content_hash);
    assert_eq!(current.input_version, ver2);
}

#[test]
fn a_failed_transaction_publishes_no_tool_mapping() {
    let (_d, mut store) = store();
    let err = store
        .input_transaction::<(), _>(|txn| {
            txn.stage_tool("shaderc", b"tool v1")?;
            Err(StoreError::Poisoned {
                error: "abort".into(),
            })
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::Poisoned { .. }));
    // The mapping never published; the content-addressed orphan file is
    // inert (unreferenced by any row).
    assert!(store.tool("shaderc").unwrap().is_none());
}

// ---- schema lineage (§6, §11, §13) ----

fn epochs(hashes: &[LogicalHash]) -> Vec<AcceptedSchemaEpoch> {
    hashes
        .iter()
        .enumerate()
        .map(|(index, digest)| AcceptedSchemaEpoch {
            digest: *digest,
            forward_parent: index.checked_sub(1).map(|parent| parent as u32),
        })
        .collect()
}

fn accepted(hashes: &[LogicalHash], current: u32) -> AcceptedTypeLineage {
    AcceptedTypeLineage {
        epochs: epochs(hashes),
        current,
        authority: TypeAuthorityState::Active,
    }
}

fn retired(hashes: &[LogicalHash], current: u32) -> AcceptedTypeLineage {
    AcceptedTypeLineage {
        epochs: epochs(hashes),
        current,
        authority: TypeAuthorityState::Retired {
            retired_from: current,
        },
    }
}

fn manifest(types: &[(TypeUuid, AcceptedTypeLineage)]) -> SchemaLineageManifest {
    SchemaLineageManifest {
        types: types.iter().cloned().collect::<BTreeMap<_, _>>(),
    }
}

/// The compact commitment to the explicit accepted-epoch prefix and cursor.
fn dssl(t: TypeUuid, accepted: &[AcceptedSchemaEpoch], cursor: u32) -> [u8; 32] {
    let mut pre_image = Vec::new();
    pre_image.extend_from_slice(b"DSSL");
    pre_image.push(1); // version
    pre_image.extend_from_slice(&t.0);
    pre_image.extend_from_slice(&(accepted.len() as u32).to_le_bytes());
    for epoch in accepted {
        pre_image.extend_from_slice(&epoch.digest.0);
        match epoch.forward_parent {
            None => pre_image.push(0),
            Some(parent) => {
                pre_image.push(1);
                pre_image.extend_from_slice(&parent.to_le_bytes());
            }
        }
    }
    pre_image.extend_from_slice(&cursor.to_le_bytes());
    *blake3::hash(&pre_image).as_bytes()
}

fn stamp(t: TypeUuid, accepted: Vec<AcceptedSchemaEpoch>, cursor: u32) -> LineageStamp {
    LineageStamp {
        chain: dssl(t, &accepted, cursor),
        epochs: accepted,
        cursor,
    }
}

#[test]
fn manifest_projection_records_epochs_even_when_no_bundle_was_written() {
    // B was accepted but no authored bundle happened to be written while
    // it was current. The source-controlled manifest still names it, so a
    // later C projection cannot collapse the history to [A, C].
    let (_d, mut store) = store();
    let source = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 2))]);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(20, source.clone()))
        })
        .unwrap();

    let entries = store.lineage(T).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].generation, 1);
    assert_eq!(entries[0].schema_hash, h(1));
    assert_eq!(entries[0].forward_parent, None);
    assert_eq!(entries[1].generation, 2);
    assert_eq!(entries[1].schema_hash, h(2));
    assert_eq!(entries[1].forward_parent, Some(0));
    assert_eq!(entries[2].forward_parent, Some(1));
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(3)));

    assert_eq!(
        store.current_lineage_stamp(T).unwrap(),
        Some(stamp(T, epochs(&[h(1), h(2), h(3)]), 2))
    );
}

#[test]
fn manifest_validation_rejects_bad_parents_duplicates_and_cursors() {
    let (_d, mut store) = store();
    let invalid = [
        AcceptedTypeLineage {
            epochs: vec![AcceptedSchemaEpoch {
                digest: h(1),
                forward_parent: Some(0),
            }],
            current: 0,
            authority: TypeAuthorityState::Active,
        },
        AcceptedTypeLineage {
            epochs: vec![
                AcceptedSchemaEpoch {
                    digest: h(1),
                    forward_parent: None,
                },
                AcceptedSchemaEpoch {
                    digest: h(1),
                    forward_parent: Some(0),
                },
            ],
            current: 1,
            authority: TypeAuthorityState::Active,
        },
        accepted(&[h(1), h(2)], 2),
    ];
    for lineage in invalid {
        let err = store
            .input_transaction(|txn| {
                txn.project_verified_lineage_manifest(&verified(21, manifest(&[(T, lineage)])))
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidLineageManifest { type_uuid: Some(t), .. } if t == T)
        );
        assert!(!store.lineage_manifest_available().unwrap());
    }
}

#[test]
fn live_projection_is_append_only_and_cannot_bypass_rollback_validation() {
    let (_d, mut store) = store();
    let original = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 2))]);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(22, original.clone()))
        })
        .unwrap();

    for invalid_update in [manifest(&[(T, accepted(&[h(1), h(2)], 1))]), manifest(&[])] {
        let err = store
            .input_transaction(|txn| {
                txn.project_verified_lineage_manifest(&verified(23, invalid_update))
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::LineageMutationRequiresCandidate));
        assert_eq!(store.lineage_current(T).unwrap(), Some(h(3)));
        assert_eq!(store.lineage(T).unwrap().len(), 3);
    }

    let rollback_bypass = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 0))]);
    let err = store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(24, rollback_bypass))
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::LineageMutationRequiresCandidate));
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(3)));

    // Even an append-only extension cannot bypass the candidate-bound,
    // verified-source transition API.
    let extended = manifest(&[(T, accepted(&[h(1), h(2), h(3), h(4)], 3))]);
    let err = store
        .input_transaction(|txn| txn.project_verified_lineage_manifest(&verified(25, extended)))
        .unwrap_err();
    assert!(matches!(err, StoreError::LineageMutationRequiresCandidate));
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(3)));
    assert_eq!(store.lineage(T).unwrap().len(), 3);
}

#[test]
fn state_loss_never_treats_a_bundle_stamp_as_forward_authority() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join(".distill"));
    let mut store = Store::open(config.clone()).unwrap();
    let source = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 2))]);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(26, source.clone()))
        })
        .unwrap();
    let old_stamp = stamp(T, epochs(&[h(1)]), 0);

    let mut store = Store::recreate(config).unwrap();
    assert!(!store.lineage_manifest_available().unwrap());
    assert_eq!(
        store
            .classify_lineage(T, h(1), Some(old_stamp.clone()), h(3))
            .unwrap(),
        LineageClass::HardStop(HardStopReason::MissingManifest)
    );

    // Startup rebuilds only from the source-controlled manifest, never by
    // unioning the observed bundle stamp into authority.
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(26, source.clone()))
        })
        .unwrap();
    assert_eq!(
        store
            .classify_lineage(T, h(1), Some(old_stamp), h(3))
            .unwrap(),
        LineageClass::ForwardOnChain
    );
}

#[test]
fn prefix_and_parent_proof_alone_permits_a_forward_automatic_diff() {
    let (_d, mut store) = store();
    let source = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 2))]);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(27, source.clone()))
        })
        .unwrap();
    let s1 = stamp(T, epochs(&[h(1)]), 0);
    assert_eq!(
        store.classify_lineage(T, h(1), Some(s1), h(3)).unwrap(),
        LineageClass::ForwardOnChain
    );
    assert_eq!(
        store.classify_lineage(T, h(1), None, h(3)).unwrap(),
        LineageClass::HardStop(HardStopReason::Unstamped)
    );
}

#[test]
fn vector_order_never_substitutes_for_parent_reachability() {
    // A→B→C was followed by an accepted rollback to A and then A→D.
    // B and C precede D in append-only history, but are not ancestors of D.
    let (_d, mut store) = store();
    let branch = vec![
        AcceptedSchemaEpoch {
            digest: h(1),
            forward_parent: None,
        },
        AcceptedSchemaEpoch {
            digest: h(2),
            forward_parent: Some(0),
        },
        AcceptedSchemaEpoch {
            digest: h(3),
            forward_parent: Some(1),
        },
        AcceptedSchemaEpoch {
            digest: h(4),
            forward_parent: Some(0),
        },
    ];
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                28,
                manifest(&[(
                    T,
                    AcceptedTypeLineage {
                        epochs: branch.clone(),
                        current: 3,
                        authority: TypeAuthorityState::Active,
                    },
                )]),
            ))
        })
        .unwrap();

    assert_eq!(
        store
            .classify_lineage(T, h(1), Some(stamp(T, branch[..1].to_vec(), 0)), h(4))
            .unwrap(),
        LineageClass::ForwardOnChain
    );
    for (data, cursor) in [(h(2), 1), (h(3), 2)] {
        assert_eq!(
            store
                .classify_lineage(
                    T,
                    data,
                    Some(stamp(T, branch[..=cursor].to_vec(), cursor as u32)),
                    h(4),
                )
                .unwrap(),
            LineageClass::HardStop(HardStopReason::Divergent)
        );
    }
}

#[test]
fn divergent_forged_or_non_manifest_stamps_never_prove_ancestry() {
    let (_d, mut store) = store();
    let source = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 2))]);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(29, source.clone()))
        })
        .unwrap();
    let mut bad_parent = epochs(&[h(1), h(2)]);
    bad_parent[1].forward_parent = None;
    for bad in [
        stamp(T, epochs(&[h(1), h(9)]), 1),
        stamp(T, bad_parent, 1),
        LineageStamp {
            epochs: epochs(&[h(1)]),
            cursor: 0,
            chain: [0u8; 32],
        },
    ] {
        assert_eq!(
            store.classify_lineage(T, h(9), Some(bad), h(3)).unwrap(),
            LineageClass::HardStop(HardStopReason::Divergent)
        );
    }
}

#[test]
fn rollback_moves_only_the_cursor_after_complete_reverse_edge_validation() {
    let (_d, mut store) = store();
    let source = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 2))]);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(30, source.clone()))
        })
        .unwrap();
    let candidate = epoch_with_registry(10, &[(T, h(1))]);
    let base = require_candidate(&mut store, &candidate);
    let reverse = [
        ReverseMigrationEdge {
            from: h(3),
            to: h(2),
        },
        ReverseMigrationEdge {
            from: h(2),
            to: h(1),
        },
    ];
    let proposed = verified(33, manifest(&[(T, accepted(&[h(1), h(2), h(3)], 0))]));
    let (rolled_back, _) = store
        .input_transaction(|txn| {
            txn.rollback_schema_candidate(
                &candidate,
                &base,
                &proposed,
                SchemaRollbackRequest {
                    type_uuid: T,
                    target: h(1),
                    live_schema_hashes: &[h(2), h(3)],
                    reverse_edges: &reverse,
                },
            )
        })
        .unwrap();
    assert_eq!(rolled_back, stamp(T, epochs(&[h(1), h(2), h(3)]), 0));
    assert_eq!(
        store
            .lineage(T)
            .unwrap()
            .iter()
            .map(|entry| entry.schema_hash)
            .collect::<Vec<_>>(),
        [h(1), h(2), h(3)],
        "rollback never truncates append-only history"
    );
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(1)));

    // Parent traversal, not vector order, now governs direction.
    assert_eq!(
        store
            .classify_lineage(
                T,
                h(3),
                Some(stamp(T, epochs(&[h(1), h(2), h(3)]), 2)),
                h(1)
            )
            .unwrap(),
        LineageClass::RegistryBehindData
    );
}

#[test]
fn rollback_rejects_missing_ambiguous_cyclic_and_unknown_coverage_atomically() {
    let (_d, mut store) = store();
    let source = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 2))]);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(31, source.clone()))
        })
        .unwrap();
    let candidate = epoch_with_registry(11, &[(T, h(1))]);
    let base = require_candidate(&mut store, &candidate);
    let proposed = verified(34, manifest(&[(T, accepted(&[h(1), h(2), h(3)], 0))]));

    let bad_cases: &[(&[LogicalHash], &[ReverseMigrationEdge])] = &[
        (
            &[h(3)],
            &[ReverseMigrationEdge {
                from: h(3),
                to: h(2),
            }],
        ),
        (
            &[h(3)],
            &[
                ReverseMigrationEdge {
                    from: h(3),
                    to: h(2),
                },
                ReverseMigrationEdge {
                    from: h(3),
                    to: h(1),
                },
                ReverseMigrationEdge {
                    from: h(2),
                    to: h(1),
                },
            ],
        ),
        (
            &[h(3)],
            &[
                ReverseMigrationEdge {
                    from: h(3),
                    to: h(2),
                },
                ReverseMigrationEdge {
                    from: h(2),
                    to: h(3),
                },
            ],
        ),
        (
            &[h(9)],
            &[
                ReverseMigrationEdge {
                    from: h(3),
                    to: h(2),
                },
                ReverseMigrationEdge {
                    from: h(2),
                    to: h(1),
                },
            ],
        ),
    ];
    for (live, edges) in bad_cases {
        let err = store
            .input_transaction(|txn| {
                txn.rollback_schema_candidate(
                    &candidate,
                    &base,
                    &proposed,
                    SchemaRollbackRequest {
                        type_uuid: T,
                        target: h(1),
                        live_schema_hashes: live,
                        reverse_edges: edges,
                    },
                )
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::IncompleteRollbackCoverage { type_uuid, target, .. } if type_uuid == T && target == h(1))
        );
        assert_eq!(store.lineage_current(T).unwrap(), Some(h(3)));
    }
}

#[test]
fn projection_is_per_type_and_chain_commitments_bind_the_type() {
    let (_d, mut store) = store();
    let other = TypeUuid([5u8; 16]);
    let first = accepted(&[h(1)], 0);
    let second = accepted(&[h(7)], 0);
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(&verified(
                32,
                manifest(&[(T, first), (other, second)]),
            ))
        })
        .unwrap();
    assert_eq!(store.lineage(T).unwrap().len(), 1);
    assert_eq!(store.lineage(other).unwrap().len(), 1);
    assert_ne!(
        dssl(T, &epochs(&[h(1)]), 0),
        dssl(other, &epochs(&[h(1)]), 0),
        "the type uuid is in the DSSL pre-image"
    );
    assert_eq!(
        store
            .classify_lineage(other, h(1), Some(stamp(T, epochs(&[h(1)]), 0)), h(7))
            .unwrap(),
        LineageClass::HardStop(HardStopReason::Divergent),
        "another type's chain never leaks"
    );
}
