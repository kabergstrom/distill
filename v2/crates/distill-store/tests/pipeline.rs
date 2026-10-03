//! §13 pipeline-side metadata: the `pipeline_state` row (dylib hash,
//! compiled schema registry and staged-candidate failure) and the `tools`
//! ToolEpoch table.

use std::collections::BTreeMap;
use std::sync::Arc;

use distill_core::bootstrap::bootstrap_control_logical_registry_v1;
use distill_core::id::{LogicalHash, TypeUuid};
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
use distill_core::tool::ToolCwdPolicy;
use distill_store::pipeline::{
    ResolvedToolPackageFile, ResolvedToolSourceV2, ToolRegistrationV2, ValidatedPipelineEpoch,
};
use distill_store::state::{
    CleanupDisposition, PipelineEpoch, PipelineFailure, PipelineFailureCode, PipelineFailureOrigin,
    PipelineState, Registration, RegistrationKind,
};
use distill_store::{Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

fn tool_package(launcher: &[u8], resource: &[u8]) -> ToolRegistrationV2 {
    ToolRegistrationV2 {
        source: ResolvedToolSourceV2::Package {
            launcher: "bin/tool".into(),
            files: vec![
                ResolvedToolPackageFile {
                    path: "bin/tool".into(),
                    executable: true,
                    bytes: launcher.to_vec(),
                },
                ResolvedToolPackageFile {
                    path: "share/config".into(),
                    executable: false,
                    bytes: resource.to_vec(),
                },
            ],
        },
        environment: vec![("LANG".into(), "C.UTF-8".into())],
        cwd_policy: ToolCwdPolicy::EmptyScratch,
    }
}

fn raw_epoch(n: u8, rows: &[(TypeUuid, LogicalHash)]) -> PipelineEpoch {
    let mut schema_registry = bootstrap_control_logical_registry_v1().unwrap();
    schema_registry.extend(rows.iter().copied());
    PipelineEpoch {
        dylib_hash: [n; 32],
        target_set: CanonicalTargetSet::canonical(vec![TargetSetRow {
            name: format!("target-{n}"),
            target_definition_hash: [n.wrapping_add(3); 32],
        }])
        .unwrap(),
        schema_registry,
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

fn epoch(n: u8) -> ValidatedPipelineEpoch {
    epoch_with_registry(n, &[])
}

fn epoch_with_registry(n: u8, rows: &[(TypeUuid, LogicalHash)]) -> ValidatedPipelineEpoch {
    ValidatedPipelineEpoch::validate(raw_epoch(n, rows)).unwrap()
}


#[test]
fn published_runtime_failure_is_durable_without_a_new_input_version() {
    let (_dir, mut store) = store();
    let ready = epoch(41);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&ready))
        .unwrap();
    let version = store.input_version();
    let failure = PipelineFailure::new(
        PipelineFailureCode::PublishedCallbackPanic,
        PipelineFailureOrigin::PublishedRuntime,
        CleanupDisposition::PublishedEpochLeaked,
        "drop thunk panicked",
    )
    .unwrap();
    store
        .fail_published_pipeline_epoch(ready.dylib_hash, &failure)
        .unwrap();
    assert_eq!(store.input_version(), version);
    assert!(matches!(
        store.pipeline_state().unwrap(),
        Some(PipelineState::Failed {
            error,
            last_good: Some(_),
        }) if error == failure
    ));
    assert!(matches!(
        store.fail_published_pipeline_epoch(ready.dylib_hash, &failure),
        Err(StoreError::StalePublishedPipeline {
            already_unavailable: true,
            ..
        })
    ));
}

#[test]
fn runtime_failure_cas_cannot_fence_another_epoch_or_use_candidate_origin() {
    let (_dir, mut store) = store();
    let ready = epoch(42);
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&ready))
        .unwrap();
    let runtime = PipelineFailure::new(
        PipelineFailureCode::PublishedCallbackRejected,
        PipelineFailureOrigin::PublishedRuntime,
        CleanupDisposition::PublishedEpochLeaked,
        "callback rejected",
    )
    .unwrap();
    assert!(matches!(
        store.fail_published_pipeline_epoch([0xff; 32], &runtime),
        Err(StoreError::StalePublishedPipeline { .. })
    ));
    assert!(matches!(
        store.pipeline_state().unwrap(),
        Some(PipelineState::Ready(_))
    ));

    let candidate = PipelineFailure::new(
        PipelineFailureCode::CandidateOpen,
        PipelineFailureOrigin::CandidateOpen,
        CleanupDisposition::None,
        "open failed",
    )
    .unwrap();
    assert!(matches!(
        store.fail_published_pipeline_epoch(ready.dylib_hash, &candidate),
        Err(StoreError::InvalidPipelineFailure(_))
    ));
}

#[test]
fn ready_requires_exact_bootstrap_logical_projection() {
    let type_uuid = distill_core::bootstrap::BOOTSTRAP_CONTROL_TYPE_UUIDS[0];

    let mut missing = raw_epoch(1, &[]);
    missing.schema_registry.remove(&type_uuid);
    assert!(matches!(
        ValidatedPipelineEpoch::validate(missing),
        Err(StoreError::InvalidBootstrapRegistry { .. })
    ));

    let mut changed = raw_epoch(2, &[]);
    changed
        .schema_registry
        .insert(type_uuid, LogicalHash([9; 32]));
    assert!(matches!(
        ValidatedPipelineEpoch::validate(changed),
        Err(StoreError::InvalidBootstrapRegistry { .. })
    ));
}

fn h(n: u8) -> LogicalHash {
    LogicalHash([n; 32])
}

const T: TypeUuid = TypeUuid([4u8; 16]);

fn candidate_failure(message: &str) -> PipelineFailure {
    PipelineFailure::new(
        PipelineFailureCode::CandidateRegistration,
        PipelineFailureOrigin::CandidateOpen,
        CleanupDisposition::CleanedAndClosed,
        message,
    )
    .unwrap()
}

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
    assert_eq!(got.dylib_hash, epoch(3).dylib_hash);
    assert_eq!(got.target_set, epoch(3).target_set);
    let mut regs = got.registrations.clone();
    regs.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(regs.len(), 2);
    assert_eq!(regs[0].id, "gltf");
    assert_eq!(regs[0].kind, RegistrationKind::Importer);
    assert_eq!(regs[1].version, 5);
}

#[test]
fn a_rejected_candidate_still_publishes_as_a_failure() {
    // §13: failure publishes the version carrying a pipeline failure —
    // the prior epoch is never silently retained as the new version's
    // code, and the version is never dropped.
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch(3)))
        .unwrap();
    store
        .input_transaction(|txn| {
            txn.publish_pipeline_failure(&candidate_failure("dup processor id `tex`"))
        })
        .unwrap();

    let state = store.pipeline_state().unwrap().expect("still published");
    match &state {
        PipelineState::Failed { error, last_good } => {
            assert!(error.message.contains("dup processor id"));
            // last_good is residency bookkeeping only — present, but
            // epoch() still refuses.
            let last: &Arc<PipelineEpoch> = last_good.as_ref().expect("prior epoch recorded");
            assert_eq!(last.dylib_hash, [3u8; 32]);
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    assert!(state.epoch().is_err());
}

#[test]
fn poison_with_no_prior_epoch_has_no_last_good() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.publish_pipeline_failure(&candidate_failure("first candidate invalid"))
        })
        .unwrap();
    match store.pipeline_state().unwrap().expect("published") {
        PipelineState::Failed { last_good, .. } => assert!(last_good.is_none()),
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn the_next_successful_swap_publishes_over_the_failure() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch(3)))
        .unwrap();
    store
        .input_transaction(|txn| txn.publish_pipeline_failure(&candidate_failure("bad candidate")))
        .unwrap();
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch(7)))
        .unwrap();
    let state = store.pipeline_state().unwrap().expect("published");
    assert_eq!(state.epoch().expect("healed").dylib_hash, [7u8; 32]);
}


#[test]
fn a_candidate_registry_publishes_ready_and_roundtrips() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| txn.publish_pipeline_epoch(&epoch_with_registry(5, &[(T, h(2))])))
        .unwrap();
    let state = store.pipeline_state().unwrap().expect("published");
    let got = state.epoch().expect("ready");
    assert_eq!(got.schema_registry.get(&T), Some(&h(2)));
    assert_eq!(got.schema_registry, raw_epoch(5, &[(T, h(2))]).schema_registry);
}

#[test]
fn noncanonical_target_rows_are_rejected() {
    let (_d, store) = store();
    let mut forged_rows = raw_epoch(24, &[]);
    forged_rows.target_set.rows[0].name = "targe\u{301}t-24".into();
    let err = ValidatedPipelineEpoch::validate(forged_rows).unwrap_err();
    assert!(matches!(err, StoreError::InvalidTargetSet(_)));
    assert!(store.pipeline_state().unwrap().is_none());
}

// ---- tools: the ToolEpoch table ----

#[test]
fn registering_a_package_tool_is_content_addressed_and_input_versioned() {
    // §13: tool key → (complete staged package, DSCT) at an input version.
    let (_d, mut store) = store();
    let binary = b"#!/bin/sh\necho v1\n";

    let (registered, v) = store
        .input_transaction(|txn| txn.register_tool("shaderc", tool_package(binary, b"cfg v1")))
        .unwrap();
    assert_eq!(registered.input_version, v);
    let root = registered.root.as_ref().expect("package root");
    assert!(root.join("bin/tool").is_file());
    assert_eq!(std::fs::read(root.join("bin/tool")).unwrap(), binary);
    assert_eq!(std::fs::read(root.join("share/config")).unwrap(), b"cfg v1");
    assert_ne!(registered.tool_hash, *blake3::hash(binary).as_bytes());

    let resolved = store.tool("shaderc").unwrap().expect("registered");
    assert_eq!(resolved.tool_hash, registered.tool_hash);
    assert_eq!(resolved.identity, registered.identity);
    assert_eq!(resolved.root, registered.root);
    assert!(store.tool("unknown-tool").unwrap().is_none());
}

#[test]
fn replacing_a_tool_republishes_and_package_copies_coexist() {
    // §9: staged versions coexist — a swap mid-epoch invalidates traces
    // into rebuilds that run the new package at the new version.
    let (_d, mut store) = store();
    let (v1, ver1) = store
        .input_transaction(|txn| {
            txn.register_tool("shaderc", tool_package(b"same launcher", b"resource v1"))
        })
        .unwrap();
    let (v2, ver2) = store
        .input_transaction(|txn| {
            txn.register_tool("shaderc", tool_package(b"same launcher", b"resource v2"))
        })
        .unwrap();
    assert_ne!(v1.tool_hash, v2.tool_hash);
    assert_ne!(v1.root, v2.root);
    assert!(v1.root.as_ref().unwrap().join("share/config").is_file());
    assert!(v2.root.as_ref().unwrap().join("share/config").is_file());

    let current = store.tool("shaderc").unwrap().unwrap();
    assert_eq!(current.tool_hash, v2.tool_hash);
    assert_eq!(current.input_version, ver2);
    let pinned = store.tool_at("shaderc", ver1).unwrap().unwrap();
    assert_eq!(pinned.tool_hash, v1.tool_hash);
    assert_eq!(pinned.input_version, ver1);
    assert!(store
        .tool_at("shaderc", distill_store::state::InputVersion(0))
        .unwrap()
        .is_none());
}

#[test]
fn complete_tool_epoch_tombstones_removed_keys_without_hiding_old_snapshots() {
    let (_d, mut store) = store();
    let first = BTreeMap::from([
        (
            "compiler".to_owned(),
            tool_package(b"compiler", b"compiler config"),
        ),
        (
            "linker".to_owned(),
            tool_package(b"linker", b"linker config"),
        ),
    ]);
    let (_, version_one) = store
        .input_transaction(|txn| txn.publish_tool_epoch(&first))
        .unwrap();

    let second = BTreeMap::from([(
        "compiler".to_owned(),
        tool_package(b"compiler v2", b"compiler config v2"),
    )]);
    let (_, version_two) = store
        .input_transaction(|txn| txn.publish_tool_epoch(&second))
        .unwrap();

    assert!(store.tool_at("linker", version_one).unwrap().is_some());
    assert!(store.tool_at("linker", version_two).unwrap().is_none());
    assert!(store.tool("linker").unwrap().is_none());
    assert_eq!(
        store.tool("compiler").unwrap().unwrap().input_version,
        version_two
    );
}

#[test]
fn a_failed_transaction_publishes_no_tool_mapping() {
    let (_d, mut store) = store();
    let err = store
        .input_transaction::<(), _>(|txn| {
            txn.register_tool("shaderc", tool_package(b"tool v1", b"resource"))?;
            Err(StoreError::InvalidConfiguration {
                error: "abort".into(),
            })
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::InvalidConfiguration { .. }));
    // The mapping never published; the content-addressed orphan file is
    // inert (unreferenced by any row).
    assert!(store.tool("shaderc").unwrap().is_none());
}

#[test]
fn invalid_package_registration_publishes_nothing() {
    let (_d, mut store) = store();
    let mut package = tool_package(b"tool", b"resource");
    let ResolvedToolSourceV2::Package { files, .. } = &mut package.source else {
        unreachable!();
    };
    files[0].path = "../tool".into();
    let before = store.input_version();
    assert!(matches!(
        store.input_transaction(|txn| txn.register_tool("tool", package)),
        Err(StoreError::InvalidToolIdentity(_))
    ));
    assert_eq!(store.input_version(), before);
    assert!(store.tool("tool").unwrap().is_none());
}

#[test]
fn staged_package_is_revalidated_immediately_before_launch() {
    let (_d, mut store) = store();
    let (registered, _) = store
        .input_transaction(|txn| txn.register_tool("tool", tool_package(b"launcher", b"resource")))
        .unwrap();
    std::fs::remove_file(registered.root.as_ref().unwrap().join("share/config")).unwrap();
    assert!(matches!(
        registered.revalidate(),
        Err(StoreError::ToolUnavailable { .. })
    ));
    // Lookup still returns the snapshot's sealed identity. Package drift is
    // classified only by the immediate pre-launch revalidation so the caller
    // can discard the successful Tool trace observation as transient.
    let resolved = store.tool("tool").unwrap().expect("published mapping");
    assert_eq!(resolved.tool_hash, registered.tool_hash);
    assert!(matches!(
        resolved.revalidate(),
        Err(StoreError::ToolUnavailable { .. })
    ));
}

#[test]
fn ambient_registration_is_not_staged_and_trust_controls_cacheability() {
    let (_d, mut store) = store();
    let launcher = std::env::current_exe().unwrap();
    let untrusted = ToolRegistrationV2 {
        source: ResolvedToolSourceV2::Ambient {
            launcher: launcher.to_string_lossy().into_owned(),
            toolchain_id: "developer-tools".into(),
            trusted_fingerprint: None,
        },
        environment: vec![],
        cwd_policy: ToolCwdPolicy::EmptyScratch,
    };
    let (registered, _) = store
        .input_transaction(|txn| txn.register_tool("ambient", untrusted))
        .unwrap();
    assert!(registered.root.is_none());
    assert!(!registered.is_cacheable());
    registered.revalidate().unwrap();

    let trusted = ToolRegistrationV2 {
        source: ResolvedToolSourceV2::Ambient {
            launcher: launcher.to_string_lossy().into_owned(),
            toolchain_id: "developer-tools".into(),
            trusted_fingerprint: Some([7; 32]),
        },
        environment: vec![],
        cwd_policy: ToolCwdPolicy::EmptyScratch,
    };
    let (registered, _) = store
        .input_transaction(|txn| txn.register_tool("ambient", trusted))
        .unwrap();
    assert!(registered.root.is_none());
    assert!(registered.is_cacheable());
}



#[test]
fn a_rolled_back_registrations_package_is_collected_at_open() {
    // A package is staged on disk inside the publishing input; an input
    // that rolls back leaves files no `tools` row names. The next open
    // collects them and keeps every registered package.
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join(".distill"));
    let mut store = Store::open(config.clone()).unwrap();
    let (kept, _) = store
        .input_transaction(|txn| txn.register_tool("kept", tool_package(b"kept tool", b"shared")))
        .unwrap();
    let mut rolled_back = None;
    let out: Result<((), _), StoreError> = store.input_transaction(|txn| {
        rolled_back = Some(txn.register_tool("dropped", tool_package(b"dropped tool", b"shared"))?);
        Err(StoreError::Rejected { detail: "the publication failed".into() })
    });
    assert!(out.is_err());
    let dropped_root = rolled_back.unwrap().root.unwrap();
    assert!(dropped_root.join("bin/tool").is_file());
    drop(store);

    let store = Store::open(config.clone()).unwrap();
    assert!(!dropped_root.exists(), "a rolled-back package outlived its registration");
    let objects: Vec<String> = std::fs::read_dir(config.state_path.join("tools/objects"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(objects.len(), 2, "the kept package's two objects: {objects:?}");
    let root = kept.root.unwrap();
    assert_eq!(std::fs::read(root.join("bin/tool")).unwrap(), b"kept tool");
    assert_eq!(std::fs::read(root.join("share/config")).unwrap(), b"shared");
    assert!(store.tool("kept").unwrap().is_some());
}
