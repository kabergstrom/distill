//! §13 pipeline-side metadata: epoch validation and the `tools` ToolEpoch
//! table.

use std::collections::BTreeMap;

use distill_core::bootstrap::bootstrap_control_logical_registry_v1;
use distill_core::id::LogicalHash;
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
use distill_core::tool::ToolCwdPolicy;
use distill_store::pipeline::{
    ResolvedToolPackageFile, ResolvedToolSourceV2, ToolRegistrationV2, ValidatedPipelineEpoch,
};
use distill_store::state::PipelineEpoch;
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

fn raw_epoch(n: u8) -> PipelineEpoch {
    PipelineEpoch {
        target_set: CanonicalTargetSet::canonical(vec![TargetSetRow {
            name: format!("target-{n}"),
            target_definition_hash: [n.wrapping_add(3); 32],
        }])
        .unwrap(),
        schema_registry: bootstrap_control_logical_registry_v1().unwrap(),
    }
}

#[test]
fn ready_requires_exact_bootstrap_logical_projection() {
    let type_uuid = distill_core::bootstrap::BOOTSTRAP_CONTROL_TYPE_UUIDS[0];

    let mut missing = raw_epoch(1);
    missing.schema_registry.remove(&type_uuid);
    assert!(matches!(
        ValidatedPipelineEpoch::validate(missing),
        Err(StoreError::InvalidBootstrapRegistry { .. })
    ));

    let mut changed = raw_epoch(2);
    changed
        .schema_registry
        .insert(type_uuid, LogicalHash([9; 32]));
    assert!(matches!(
        ValidatedPipelineEpoch::validate(changed),
        Err(StoreError::InvalidBootstrapRegistry { .. })
    ));
}

#[test]
fn noncanonical_target_rows_are_rejected() {
    let mut forged_rows = raw_epoch(24);
    forged_rows.target_set.rows[0].name = "targe\u{301}t-24".into();
    let err = ValidatedPipelineEpoch::validate(forged_rows).unwrap_err();
    assert!(matches!(err, StoreError::InvalidTargetSet(_)));
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

    // The same epoch again writes no row: the compiler's is version two's.
    let (_, version_three) = store
        .input_transaction(|txn| txn.publish_tool_epoch(&second))
        .unwrap();
    assert!(version_three > version_two);
    assert_eq!(
        store.tool("compiler").unwrap().unwrap().input_version,
        version_two
    );
    assert!(store.tool("linker").unwrap().is_none());
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
    let before = store.input_version().unwrap();
    assert!(matches!(
        store.input_transaction(|txn| txn.register_tool("tool", package)),
        Err(StoreError::InvalidToolIdentity(_))
    ));
    assert_eq!(store.input_version().unwrap(), before);
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
fn a_rolled_back_registration_leaves_no_package() {
    // A registration stages its package tree in the tool store's staging
    // directory; only its input's commit publishes it. An input that rolls
    // back leaves no package, and nothing is collected at open.
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join(".distill"));
    let mut store = Store::open(config.clone()).unwrap();
    let (kept, _) = store
        .input_transaction(|txn| txn.register_tool("kept", tool_package(b"kept tool", b"shared")))
        .unwrap();
    let mut rolled_back = None;
    let out: Result<((), _), StoreError> = store.input_transaction(|txn| {
        let registered = txn.register_tool("dropped", tool_package(b"dropped tool", b"shared"))?;
        // Uncommitted: the tree is staged, not published.
        assert!(!registered.root.as_ref().unwrap().exists());
        rolled_back = Some(registered);
        Err(StoreError::Rejected {
            detail: "the publication failed".into(),
        })
    });
    assert!(out.is_err());
    let dropped_root = rolled_back.unwrap().root.unwrap();
    assert!(
        !dropped_root.exists(),
        "a rolled-back registration published its package"
    );
    let tools = config.state_path.join("tools");
    let staging = distill_store::atomic_file::staging_dir(&tools);
    let names = |dir: &std::path::Path| -> Vec<String> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    };
    assert!(
        names(&staging).is_empty(),
        "a rolled-back stage outlived its input"
    );
    let root = kept.root.unwrap();
    assert_eq!(
        names(&tools.join("packages")),
        [root.file_name().unwrap().to_string_lossy().into_owned()]
    );
    assert_eq!(std::fs::read(root.join("bin/tool")).unwrap(), b"kept tool");
    assert_eq!(std::fs::read(root.join("share/config")).unwrap(), b"shared");
    drop(store);

    let store = Store::open(config.clone()).unwrap();
    assert!(store.tool("kept").unwrap().unwrap().revalidate().is_ok());
    assert!(store.tool("dropped").unwrap().is_none());
}

/// A savepoint that rolls back inside an input that commits drops the
/// packages it staged and keeps the input's own.
#[test]
fn a_failed_nested_input_fails_the_input_and_drops_its_packages() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join(".distill"));
    let mut store = Store::open(config.clone()).unwrap();
    store.open_input().unwrap();
    let (outer, _) = store
        .input_transaction(|txn| txn.register_tool("outer", tool_package(b"outer tool", b"a")))
        .unwrap();
    let inner: Result<((), _), StoreError> = store.input_transaction(|txn| {
        txn.register_tool("inner", tool_package(b"inner tool", b"b"))?;
        Err(StoreError::Rejected {
            detail: "nested failure".into(),
        })
    });
    assert!(inner.is_err());
    assert!(store.finish_input(true).is_err());
    let packages =
        std::fs::read_dir(config.state_path.join("tools/packages")).map_or(0, Iterator::count);
    assert_eq!(packages, 0);
    assert!(!outer.root.unwrap().exists());
    assert!(store.tool("outer").unwrap().is_none());
    assert!(store.tool("inner").unwrap().is_none());
}
