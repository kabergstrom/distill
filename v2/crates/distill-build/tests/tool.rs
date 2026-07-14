#![cfg(unix)]

use distill_build::tool::{
    ProcessContext, StoreToolEpochSnapshot, ToolEpochSnapshot, ToolOutput, ToolRunError,
    ToolRuntimeBinding,
};
use distill_build::trace::{
    CapabilityKey, Observed, StableFailureFingerprint, ToolLaunchFailureClass, TraceOp,
};
use distill_core::tool::{
    ToolCapsuleFileRole, ToolCwdPolicy, ToolLaunchMetadataV1, ToolPlatformBinding,
};
use distill_store::pipeline::{ResolvedToolCapsuleFile, ToolCapsuleRegistrationV1};
use distill_store::{Store, StoreConfig};

fn new_store() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path().join(".distill"))).unwrap();
    (directory, store)
}

fn registration(cwd_policy: ToolCwdPolicy) -> ToolCapsuleRegistrationV1 {
    let script = br#"printf '%s|%s|%s|' "$FOO" "${HOME-unset}" "$1"; cat"#;
    ToolCapsuleRegistrationV1 {
        files: vec![
            ResolvedToolCapsuleFile {
                path: "bin/launcher".into(),
                role: ToolCapsuleFileRole::Launcher,
                executable: false,
                bytes: script.to_vec(),
            },
            ResolvedToolCapsuleFile {
                path: "bin/sh".into(),
                role: ToolCapsuleFileRole::Interpreter,
                executable: true,
                bytes: std::fs::read("/bin/sh").unwrap(),
            },
            ResolvedToolCapsuleFile {
                path: "work/declared".into(),
                role: ToolCapsuleFileRole::DeclaredResource,
                executable: false,
                bytes: b"resource".to_vec(),
            },
        ],
        resolved_interpreter: Some("bin/sh".into()),
        launch: ToolLaunchMetadataV1 {
            argv0: "bin/launcher".into(),
            interpreter_args: Vec::new(),
        },
        environment: vec![("FOO".into(), "sealed".into())],
        cwd_policy,
        platform: ToolPlatformBinding::Pinned {
            platform_id: "test-platform".into(),
            system_runtime_id: "test-runtime".into(),
        },
    }
}

fn context<'a, S: ToolEpochSnapshot>(
    snapshot: &'a S,
    execution_root: &'a std::path::Path,
) -> ProcessContext<'a, S> {
    ProcessContext::new(
        snapshot,
        ToolRuntimeBinding {
            platform_id: "test-platform",
            system_runtime_id: "test-runtime",
            execution_root,
        },
    )
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
fn run_tool_launches_the_exact_capsule_with_only_sealed_environment() {
    let (directory, mut store) = new_store();
    let (staged, _) = store
        .input_transaction(|txn| {
            txn.stage_tool("compiler", registration(ToolCwdPolicy::EmptyScratch))
        })
        .unwrap();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = context(&snapshot, directory.path());
    let ToolOutput {
        status,
        stdout,
        stderr,
    } = process
        .run_tool("compiler", &["argument"], b"stdin")
        .unwrap();
    assert_eq!(status, 0);
    assert_eq!(stdout, b"sealed|unset|argument|stdin");
    assert!(stderr.is_empty());
    assert_eq!(
        process.trace(),
        Some(
            [TraceOp::Tool {
                id: "compiler".into(),
                observed: Observed::Ok(staged.capsule_hash),
            }]
            .as_slice()
        )
    );
}

#[test]
fn closure_or_platform_drift_discards_the_entire_attempt_trace() {
    let (directory, mut store) = new_store();
    let (staged, _) = store
        .input_transaction(|txn| {
            txn.stage_tool("compiler", registration(ToolCwdPolicy::EmptyScratch))
        })
        .unwrap();
    std::fs::remove_file(staged.root.join("work/declared")).unwrap();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = context(&snapshot, directory.path());
    let error = process.run_tool("compiler", &[], b"").unwrap_err();
    assert!(matches!(
        error,
        ToolRunError::Transient(ref diagnostic)
            if diagnostic.id == "compiler"
                && diagnostic.capsule_hash == staged.capsule_hash
                && diagnostic.class == ToolLaunchFailureClass::CapsuleClosureUnavailable
    ));
    assert_eq!(process.trace(), None);
    assert!(process.into_trace().is_err());

    let (directory, mut store) = new_store();
    store
        .input_transaction(|txn| {
            txn.stage_tool("compiler", registration(ToolCwdPolicy::EmptyScratch))
        })
        .unwrap();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = ProcessContext::new(
        &snapshot,
        ToolRuntimeBinding {
            platform_id: "wrong-platform",
            system_runtime_id: "test-runtime",
            execution_root: directory.path(),
        },
    );
    assert!(matches!(
        process.run_tool("compiler", &[], b""),
        Err(ToolRunError::Transient(ref diagnostic))
            if diagnostic.class == ToolLaunchFailureClass::CapsuleClosureUnavailable
    ));
    assert_eq!(process.trace(), None);
}

#[test]
fn declared_subdirectory_is_the_process_cwd_inside_a_private_capsule_copy() {
    let (directory, mut store) = new_store();
    store
        .input_transaction(|txn| {
            txn.stage_tool(
                "compiler",
                registration(ToolCwdPolicy::ReadOnlyDeclaredSubdir("work".into())),
            )
        })
        .unwrap();
    let snapshot = StoreToolEpochSnapshot::new(&store, store.input_version());
    let mut process = context(&snapshot, directory.path());
    let output = process.run_tool("compiler", &["ok"], b"").unwrap();
    assert_eq!(output.stdout, b"sealed|unset|ok|");
}

#[test]
fn process_context_never_substitutes_a_newer_tool_epoch_for_its_pinned_basis() {
    let (directory, mut store) = new_store();
    let mut first = registration(ToolCwdPolicy::EmptyScratch);
    first.environment = vec![("FOO".into(), "first".into())];
    let (_, first_version) = store
        .input_transaction(|txn| txn.stage_tool("compiler", first))
        .unwrap();
    let mut second = registration(ToolCwdPolicy::EmptyScratch);
    second.environment = vec![("FOO".into(), "second".into())];
    store
        .input_transaction(|txn| txn.stage_tool("compiler", second))
        .unwrap();

    let snapshot = StoreToolEpochSnapshot::new(&store, first_version);
    let mut process = context(&snapshot, directory.path());
    let output = process.run_tool("compiler", &["arg"], b"").unwrap();
    assert_eq!(output.stdout, b"first|unset|arg|");
}
