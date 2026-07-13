//! §13 pipeline-side metadata: the `pipeline_state` row (dylib hash,
//! load-policy digest, staged-candidate poison), the `tools` ToolEpoch
//! table, and the source-controlled schema-lineage projection that gates
//! automatic migration diffs (§11).

use std::collections::BTreeMap;
use std::sync::Arc;

use distill_core::id::{LogicalHash, TypeUuid};
use distill_store::pipeline::{
    AcceptedSchemaEpoch, AcceptedTypeLineage, HardStopReason, LineageClass, LineageStamp,
    ReverseMigrationEdge, SchemaLineageManifest,
};
use distill_store::state::{PipelineEpoch, PipelineState, Registration, RegistrationKind};
use distill_store::{Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

fn epoch(n: u8) -> PipelineEpoch {
    PipelineEpoch {
        dylib_hash: [n; 32],
        load_policy_digest: [n.wrapping_add(1); 32],
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
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch(3)))
        .unwrap();
    let state = store.pipeline_state().unwrap().expect("published");
    let got = state.epoch().expect("ready");
    assert_eq!(got.dylib_hash, [3u8; 32]);
    assert_eq!(got.load_policy_digest, [4u8; 32]);
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
        .input_transaction(|txn| txn.project_lineage_manifest(&source))
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
        },
        accepted(&[h(1), h(2)], 2),
    ];
    for lineage in invalid {
        let err = store
            .input_transaction(|txn| txn.project_lineage_manifest(&manifest(&[(T, lineage)])))
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
        .input_transaction(|txn| txn.project_lineage_manifest(&original))
        .unwrap();

    for invalid_update in [manifest(&[(T, accepted(&[h(1), h(2)], 1))]), manifest(&[])] {
        let err = store
            .input_transaction(|txn| txn.project_lineage_manifest(&invalid_update))
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidLineageManifest { .. }));
        assert_eq!(store.lineage_current(T).unwrap(), Some(h(3)));
        assert_eq!(store.lineage(T).unwrap().len(), 3);
    }

    let rollback_bypass = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 0))]);
    let err = store
        .input_transaction(|txn| txn.project_lineage_manifest(&rollback_bypass))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::LineageRollback {
            type_uuid,
            candidate,
            current,
        } if type_uuid == T && candidate == h(1) && current == h(3)
    ));
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(3)));

    // Ordinary acceptance is the only general projection transition: it
    // appends and advances, preserving the complete prior vector.
    let extended = manifest(&[(T, accepted(&[h(1), h(2), h(3), h(4)], 3))]);
    store
        .input_transaction(|txn| txn.project_lineage_manifest(&extended))
        .unwrap();
    assert_eq!(store.lineage_current(T).unwrap(), Some(h(4)));
    assert_eq!(store.lineage(T).unwrap().len(), 4);
}

#[test]
fn state_loss_never_treats_a_bundle_stamp_as_forward_authority() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join(".distill"));
    let mut store = Store::open(config.clone()).unwrap();
    let source = manifest(&[(T, accepted(&[h(1), h(2), h(3)], 2))]);
    store
        .input_transaction(|txn| txn.project_lineage_manifest(&source))
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
        .input_transaction(|txn| txn.project_lineage_manifest(&source))
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
        .input_transaction(|txn| txn.project_lineage_manifest(&source))
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
            txn.project_lineage_manifest(&manifest(&[(
                T,
                AcceptedTypeLineage {
                    epochs: branch.clone(),
                    current: 3,
                },
            )]))
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
        .input_transaction(|txn| txn.project_lineage_manifest(&source))
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
        .input_transaction(|txn| txn.project_lineage_manifest(&source))
        .unwrap();
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
    let (rolled_back, _) = store
        .input_transaction(|txn| txn.rollback_lineage(T, h(1), &[h(2), h(3)], &reverse))
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
        .input_transaction(|txn| txn.project_lineage_manifest(&source))
        .unwrap();

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
            .input_transaction(|txn| txn.rollback_lineage(T, h(1), live, edges))
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
            txn.project_lineage_manifest(&manifest(&[(T, first), (other, second)]))
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
