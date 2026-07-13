//! §13 pipeline-side metadata: the `pipeline_state` row (dylib hash,
//! load-policy digest, staged-candidate poison), the `tools` ToolEpoch
//! table, and the `schema_lineage` forward chain that gates automatic
//! migration diffs (§11).

use std::sync::Arc;

use distill_core::id::{LogicalHash, TypeUuid};
use distill_store::pipeline::{HardStopReason, LineageClass, LineageStamp};
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

/// The compact commitment to the explicit ordered digest list.
fn dssl(t: TypeUuid, hashes: &[LogicalHash]) -> [u8; 32] {
    let mut pre_image = Vec::new();
    pre_image.extend_from_slice(b"DSSL");
    pre_image.push(1); // version
    pre_image.extend_from_slice(&t.0);
    pre_image.extend_from_slice(&(hashes.len() as u32).to_le_bytes());
    for h in hashes {
        pre_image.extend_from_slice(&h.0);
    }
    *blake3::hash(&pre_image).as_bytes()
}

fn stamp(t: TypeUuid, hashes: &[LogicalHash]) -> LineageStamp {
    LineageStamp {
        digests: hashes.to_vec(),
        chain_digest: dssl(t, hashes),
    }
}

#[test]
fn appends_stamp_the_full_ordered_list_and_dssl_commitment() {
    // R22/C1: generation is derived from list length. The authored stamp
    // carries every predecessor, while DSSL remains the compact
    // commitment. An opaque digest alone is not an ancestry proof.
    let (_d, mut store) = store();
    let (stamps, _) = store
        .input_transaction(|txn| {
            let s1 = txn.append_lineage(T, h(1))?;
            let s2 = txn.append_lineage(T, h(2))?;
            Ok((s1, s2))
        })
        .unwrap();
    assert_eq!(stamps.0, stamp(T, &[h(1)]));
    assert_eq!(stamps.1, stamp(T, &[h(1), h(2)]));
    assert_eq!(stamps.0.generation(), 1);
    assert_eq!(stamps.1.generation(), 2);

    let entries = store.lineage(T).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].generation, 1);
    assert_eq!(entries[0].schema_hash, h(1));
    assert_eq!(entries[0].chain_digest, dssl(T, &[h(1)]));
    assert_eq!(entries[1].generation, 2);
    assert_eq!(entries[1].schema_hash, h(2));
    assert_eq!(entries[1].chain_digest, dssl(T, &[h(1), h(2)]));
    assert!(store.lineage(TypeUuid([9u8; 16])).unwrap().is_empty());

    assert_eq!(
        store.lineage_stamp(T, h(1)).unwrap(),
        Some(stamp(T, &[h(1)]))
    );
    assert_eq!(store.lineage_stamp(T, h(9)).unwrap(), None);
}

#[test]
fn restaging_the_head_is_idempotent() {
    // A candidate whose digest IS the head is not a rollback — the
    // schema is unchanged, and no duplicate entry appends (§13's staging
    // rule guarantees a digest never re-enters a chain).
    let (_d, mut store) = store();
    let (stamps, _) = store
        .input_transaction(|txn| {
            let first = txn.append_lineage(T, h(1))?;
            let again = txn.append_lineage(T, h(1))?;
            Ok((first, again))
        })
        .unwrap();
    assert_eq!(stamps.0, stamps.1);
    assert_eq!(store.lineage(T).unwrap().len(), 1);
}

#[test]
fn staging_a_non_head_chain_entry_is_a_rollback_refusal() {
    // §13: staging rejects a candidate whose digest is a non-head chain
    // entry — a rollback. Hard stop, no append: schema-writing services
    // refuse for the type until an explicit reverse edge lands.
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.append_lineage(T, h(1))?;
            txn.append_lineage(T, h(2))?;
            txn.append_lineage(T, h(3))?;
            Ok(())
        })
        .unwrap();
    let err = store
        .input_transaction(|txn| txn.append_lineage(T, h(2)))
        .unwrap_err();
    match err {
        StoreError::LineageRollback {
            type_uuid,
            candidate,
            head,
        } => {
            assert_eq!(type_uuid, T);
            assert_eq!(candidate, h(2));
            assert_eq!(head, h(3));
        }
        other => panic!("expected LineageRollback, got {other:?}"),
    }
    // Nothing appended: the chain still ends at h(3).
    assert_eq!(store.lineage(T).unwrap().len(), 3);
    assert_eq!(store.lineage(T).unwrap().last().unwrap().schema_hash, h(3));
}

#[test]
fn classification_at_current() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| txn.append_lineage(T, h(2)))
        .unwrap();
    assert_eq!(
        store.classify_lineage(T, h(2), None, h(2)).unwrap(),
        LineageClass::AtCurrent
    );
}

#[test]
fn only_an_explicit_prefix_proof_permits_the_automatic_diff() {
    let (_d, mut store) = store();
    let (stamps, _) = store
        .input_transaction(|txn| {
            let s1 = txn.append_lineage(T, h(1))?;
            let s2 = txn.append_lineage(T, h(2))?;
            let s3 = txn.append_lineage(T, h(3))?;
            Ok((s1, s2, s3))
        })
        .unwrap();

    // Even a hash present in disposable store state needs the authored
    // list. Store position is an accelerator, not ancestry proof.
    assert_eq!(
        store.classify_lineage(T, h(1), None, h(3)).unwrap(),
        LineageClass::HardStop(HardStopReason::Unstamped)
    );
    let class = store
        .classify_lineage(T, h(1), Some(stamps.0), h(3))
        .unwrap();
    assert_eq!(class, LineageClass::ForwardOnChain);
    assert!(class.permits_automatic_diff());

    assert_eq!(
        store
            .classify_lineage(T, h(2), Some(stamps.1), h(3))
            .unwrap(),
        LineageClass::ForwardOnChain
    );
}

#[test]
fn registry_prefix_of_data_is_a_rollback_hard_stop() {
    let (_d, mut store) = store();
    // The serving registry is at [h1,h2], while authored data proves it
    // was written by the descendant history [h1,h2,h3].
    store
        .input_transaction(|txn| txn.reestablish_lineage(T, h(2), stamp(T, &[h(1), h(2)])))
        .unwrap();
    let class = store
        .classify_lineage(T, h(3), Some(stamp(T, &[h(1), h(2), h(3)])), h(2))
        .unwrap();
    assert_eq!(class, LineageClass::RegistryBehindData);
    assert!(!class.permits_automatic_diff());
}

#[test]
fn an_unknown_unstamped_hash_is_a_hard_stop() {
    // §11: an unknown or unstamped schema is a hard stop requiring an
    // explicit edge — first sight is NOT trivially forward (the R21
    // supersession of the old NoLineage rule).
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| txn.append_lineage(T, h(2)))
        .unwrap();
    let class = store.classify_lineage(T, h(9), None, h(2)).unwrap();
    assert_eq!(class, LineageClass::HardStop(HardStopReason::Unstamped));
    assert!(!class.permits_automatic_diff());
}

#[test]
fn an_empty_chain_with_no_position_is_a_hard_stop() {
    // With no recorded lineage AND no re-established position, the
    // registry cannot judge direction: never an automatic diff (the old
    // "no lineage yet, trivially forward" rule is superseded).
    let (_d, store) = store();
    let class = store.classify_lineage(T, h(1), None, h(2)).unwrap();
    assert_eq!(
        class,
        LineageClass::HardStop(HardStopReason::UnknownPosition)
    );
    assert!(!class.permits_automatic_diff());

    let stamped = store
        .classify_lineage(T, h(1), Some(stamp(T, &[h(1)])), h(2))
        .unwrap();
    assert!(!stamped.permits_automatic_diff());
}

#[test]
fn one_full_stamp_reconstructs_missing_intermediate_positions_after_state_loss() {
    let dir = tempfile::tempdir().unwrap();
    let config = distill_store::StoreConfig::new(dir.path().join(".distill"));
    let mut store = Store::open(config.clone()).unwrap();
    let (current_stamp, _) = store
        .input_transaction(|txn| {
            txn.append_lineage(T, h(1))?;
            txn.append_lineage(T, h(2))?;
            txn.append_lineage(T, h(3))
        })
        .unwrap();

    // State loss: the chain is gone.
    let mut store = Store::recreate(config).unwrap();
    assert!(store.lineage(T).unwrap().is_empty());

    store
        .input_transaction(|txn| txn.reestablish_lineage(T, h(3), current_stamp.clone()))
        .unwrap();

    assert_eq!(
        store
            .lineage(T)
            .unwrap()
            .iter()
            .map(|e| e.schema_hash)
            .collect::<Vec<_>>(),
        [h(1), h(2), h(3)]
    );
    assert_eq!(
        store
            .classify_lineage(T, h(1), Some(stamp(T, &[h(1)])), h(3))
            .unwrap(),
        LineageClass::ForwardOnChain
    );
}

#[test]
fn divergent_or_forged_lists_are_never_automatic_ancestry() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| txn.reestablish_lineage(T, h(3), stamp(T, &[h(1), h(2), h(3)])))
        .unwrap();
    for bad in [
        stamp(T, &[h(1), h(9)]), // diverges from registry
        LineageStamp {
            digests: vec![h(1)],
            chain_digest: [0u8; 32],
        }, // forged commitment
        stamp(T, &[h(1), h(2)]), // head does not equal claimed data hash
    ] {
        assert_eq!(
            store.classify_lineage(T, h(9), Some(bad), h(3)).unwrap(),
            LineageClass::HardStop(HardStopReason::Divergent)
        );
    }
}

#[test]
fn reconstruction_unions_only_prefix_consistent_lists() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.reestablish_lineage(T, h(2), stamp(T, &[h(1), h(2)]))?;
            txn.reestablish_lineage(T, h(3), stamp(T, &[h(1), h(2), h(3)]))
        })
        .unwrap();

    let err = store
        .input_transaction(|txn| txn.reestablish_lineage(T, h(9), stamp(T, &[h(1), h(9)])))
        .unwrap_err();
    assert!(
        matches!(err, StoreError::LineageStampConflict { type_uuid, generation: 2, .. } if type_uuid == T)
    );
    assert_eq!(
        store.lineage(T).unwrap().len(),
        3,
        "conflicting union publishes nothing"
    );
}

#[test]
fn appends_extend_a_reestablished_chain() {
    let (_d, mut store) = store();
    let (got, _) = store
        .input_transaction(|txn| {
            txn.reestablish_lineage(T, h(3), stamp(T, &[h(1), h(2), h(3)]))?;
            txn.append_lineage(T, h(4))
        })
        .unwrap();
    assert_eq!(got, stamp(T, &[h(1), h(2), h(3), h(4)]));
    let err = store
        .input_transaction(|txn| txn.append_lineage(T, h(3)))
        .unwrap_err();
    assert!(matches!(err, StoreError::LineageRollback { .. }));
}

#[test]
fn lineage_is_per_type() {
    let (_d, mut store) = store();
    let other = TypeUuid([5u8; 16]);
    store
        .input_transaction(|txn| {
            txn.append_lineage(T, h(1))?;
            txn.append_lineage(other, h(7))?;
            Ok(())
        })
        .unwrap();
    assert_eq!(store.lineage(T).unwrap().len(), 1);
    assert_eq!(store.lineage(other).unwrap().len(), 1);
    // The digests bind the type uuid: identical histories for two types
    // never share a chain digest.
    assert_ne!(
        dssl(T, &[h(1)]),
        dssl(other, &[h(1)]),
        "the type uuid is in the DSSL pre-image"
    );
    assert_eq!(
        store
            .classify_lineage(other, h(1), Some(stamp(T, &[h(1)])), h(7))
            .unwrap(),
        LineageClass::HardStop(HardStopReason::Divergent),
        "another type's chain never leaks"
    );
}
