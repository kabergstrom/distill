#![cfg(unix)]

use distill_build::tool::{
    ProcessContext, StoreToolEpochSnapshot, ToolEpochSnapshot, ToolOutput, ToolRunError,
    ToolRuntimeBinding,
};
use distill_build::trace::{
    CapabilityKey, Observed, StableFailureFingerprint, ToolLaunchFailureClass, TraceOp,
};
use distill_core::tool::ToolCwdPolicy;
use distill_store::pipeline::{ResolvedToolPackageFile, ResolvedToolSourceV2, ToolRegistrationV2};
use distill_store::{Store, StoreConfig};

fn new_store() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path().join(".distill"))).unwrap();
    (directory, store)
}

fn package_registration(cwd_policy: ToolCwdPolicy) -> ToolRegistrationV2 {
    let script = br#"#!/bin/sh
printf '%s|%s|%s|' "$FOO" "${HOME-unset}" "$1"
IFS= read -r input || :
printf '%s' "$input"
"#;
    ToolRegistrationV2 {
        source: ResolvedToolSourceV2::Package {
            launcher: "bin/launcher".into(),
            files: vec![
                ResolvedToolPackageFile {
                    path: "bin/launcher".into(),
                    executable: true,
                    bytes: script.to_vec(),
                },
                ResolvedToolPackageFile {
                    path: "work/resource".into(),
                    executable: false,
                    bytes: b"resource".to_vec(),
                },
            ],
        },
        environment: vec![("FOO".into(), "sealed".into())],
        cwd_policy,
    }
}

fn ambient_registration(trusted: bool) -> ToolRegistrationV2 {
    let launcher = std::fs::canonicalize("/bin/sh").unwrap();
    ToolRegistrationV2 {
        source: ResolvedToolSourceV2::Ambient {
            launcher: launcher.to_string_lossy().into_owned(),
            toolchain_id: "system-shell".into(),
            trusted_fingerprint: trusted.then_some([9; 32]),
        },
        environment: vec![("FOO".into(), "ambient".into())],
        cwd_policy: ToolCwdPolicy::EmptyScratch,
    }
}

fn context<'a, S: ToolEpochSnapshot>(
    snapshot: &'a S,
    execution_root: &'a std::path::Path,
) -> ProcessContext<'a, S> {
    ProcessContext::new(snapshot, ToolRuntimeBinding { execution_root })
}

#[test]
fn missing_tool_is_a_terminal_memoizable_capability_observation() {
    let (directory, store) = new_store();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = context(&snapshot, directory.path());
    let error = process.run_tool("missing", &[], b"").unwrap_err();
    let expected = StableFailureFingerprint::MissingCapability {
        key: CapabilityKey::Tool("missing".into()),
    };
    assert_eq!(error, ToolRunError::Stable(expected.clone()));
    assert_eq!(
        process.trace(),
        Some(
            [TraceOp::Tool {
                id: "missing".into(),
                observed: Observed::Err(expected),
            }]
            .as_slice()
        )
    );
    assert!(matches!(
        process.run_tool("missing", &[], b""),
        Err(ToolRunError::AttemptStopped)
    ));
}

#[test]
fn package_tool_uses_the_registered_tree_and_only_the_sealed_environment() {
    let (directory, mut store) = new_store();
    let (registered, _) = store
        .input_transaction(|txn| {
            txn.register_tool(
                "compiler",
                package_registration(ToolCwdPolicy::EmptyScratch),
            )
        })
        .unwrap();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = context(&snapshot, directory.path());
    let ToolOutput {
        status,
        stdout,
        stderr,
    } = process
        .run_tool("compiler", &["argument"], b"stdin\n")
        .unwrap();
    assert_eq!(status, 0);
    assert_eq!(stdout, b"sealed|unset|argument|stdin");
    assert!(stderr.is_empty());
    assert_eq!(
        process.trace(),
        Some(
            [TraceOp::Tool {
                id: "compiler".into(),
                observed: Observed::Ok(registered.tool_hash),
            }]
            .as_slice()
        )
    );
    assert!(process.into_trace().unwrap().1);
}

#[test]
fn package_drift_discards_the_entire_attempt_trace() {
    let (directory, mut store) = new_store();
    let (registered, _) = store
        .input_transaction(|txn| {
            txn.register_tool(
                "compiler",
                package_registration(ToolCwdPolicy::EmptyScratch),
            )
        })
        .unwrap();
    std::fs::remove_file(registered.root.as_ref().unwrap().join("work/resource")).unwrap();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = context(&snapshot, directory.path());
    let error = process.run_tool("compiler", &[], b"").unwrap_err();
    assert!(matches!(
        error,
        ToolRunError::Transient(ref diagnostic)
            if diagnostic.id == "compiler"
                && diagnostic.tool_hash == registered.tool_hash
                && diagnostic.class == ToolLaunchFailureClass::PackageUnavailable
    ));
    assert_eq!(process.trace(), None);
    assert!(process.into_trace().is_err());
}

#[test]
fn package_subdirectory_is_the_process_cwd_inside_a_private_copy() {
    let (directory, mut store) = new_store();
    store
        .input_transaction(|txn| {
            txn.register_tool(
                "compiler",
                package_registration(ToolCwdPolicy::ReadOnlyPackageSubdir("work".into())),
            )
        })
        .unwrap();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = context(&snapshot, directory.path());
    let output = process.run_tool("compiler", &["ok"], b"\n").unwrap();
    assert_eq!(output.stdout, b"sealed|unset|ok|");
}

#[test]
fn process_context_never_substitutes_a_newer_tool_epoch_for_its_pinned_basis() {
    let (directory, mut store) = new_store();
    let mut first = package_registration(ToolCwdPolicy::EmptyScratch);
    first.environment = vec![("FOO".into(), "first".into())];
    let (_, first_version) = store
        .input_transaction(|txn| txn.register_tool("compiler", first))
        .unwrap();
    let mut second = package_registration(ToolCwdPolicy::EmptyScratch);
    second.environment = vec![("FOO".into(), "second".into())];
    store
        .input_transaction(|txn| txn.register_tool("compiler", second))
        .unwrap();

    let snapshot = StoreToolEpochSnapshot::new(&store, first_version);
    let mut process = context(&snapshot, directory.path());
    let output = process.run_tool("compiler", &["arg"], b"\n").unwrap();
    assert_eq!(output.stdout, b"first|unset|arg|");
}

#[test]
fn ambient_tool_runs_directly_and_trust_controls_memoization() {
    for trusted in [false, true] {
        let (directory, mut store) = new_store();
        store
            .input_transaction(|txn| txn.register_tool("shell", ambient_registration(trusted)))
            .unwrap();
        let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
        let mut process = context(&snapshot, directory.path());
        let output = process
            .run_tool("shell", &["-c", "printf '%s' \"$FOO\""], b"")
            .unwrap();
        assert_eq!(output.stdout, b"ambient");
        let (_, cacheable) = process.into_trace().unwrap();
        assert_eq!(cacheable, trusted);
    }
}

#[test]
fn tool_output_is_drained_while_large_stdin_is_written() {
    let (directory, mut store) = new_store();
    let script = br#"#!/bin/sh
head -c 262144 /dev/zero
cat >/dev/null
"#;
    store
        .input_transaction(|txn| {
            txn.register_tool(
                "duplex",
                ToolRegistrationV2 {
                    source: ResolvedToolSourceV2::Package {
                        launcher: "bin/duplex".into(),
                        files: vec![ResolvedToolPackageFile {
                            path: "bin/duplex".into(),
                            executable: true,
                            bytes: script.to_vec(),
                        }],
                    },
                    environment: Vec::new(),
                    cwd_policy: ToolCwdPolicy::EmptyScratch,
                },
            )
        })
        .unwrap();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = context(&snapshot, directory.path());
    let output = process.run_tool("duplex", &[], &vec![7; 262_144]).unwrap();
    assert_eq!(output.status, 0);
    assert_eq!(output.stdout.len(), 262_144);
    assert!(output.stderr.is_empty());
}
