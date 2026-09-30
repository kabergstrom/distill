use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use distill_core::id::ContentHash;
use distill_daemon::config::DaemonConfig;
use distill_daemon::process::DaemonProcess;
use distill_daemon::scanner::{DaemonOwnedDirectoryKind, ScanDiagnostic};
use distill_schema::ngp_schema::{LayoutIdentity, Schema, SchemaLayouts};
use distill_store::state::{ConfigurationState, DscpV1, PipelineState};

fn test_layout_identity() -> LayoutIdentity {
    LayoutIdentity {
        target_triple: "aarch64-apple-darwin".into(),
        rustc: "rustc test".into(),
        algorithm_version: 1,
    }
}

fn config_source(temp: &tempfile::TempDir) -> String {
    let assets = temp.path().join("assets");
    format!(
        r#"
[daemon]
address = "127.0.0.1:0"
state_path = "{}"
[assets]
roots = {{ main = "{}" }}
schema_path = "{}"
[modules]
pipeline_dylib = "{}"
[targets.dev]
os = "macos"
arch = "aarch64"
apis = ["vulkan"]
optimize = false
[codegen]
rs_mod_path = "{}"
auto_codegen = false
[pipeline]
parallelism = 2
max_dependency_depth = 64
batch_reserved_workers = 1
[cas]
segment_size = "1MiB"
cache_limit = "8MiB"
"#,
        temp.path().join("state").display(),
        assets.display(),
        temp.path().join("schema.json").display(),
        temp.path().join("pipeline.so").display(),
        temp.path().join("generated").display(),
    )
}

fn config(temp: &tempfile::TempDir) -> DaemonConfig {
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let path = temp.path().join("distill.toml");
    std::fs::write(&path, config_source(temp)).unwrap();
    write_schema(temp, "initial");
    DaemonConfig::load(path).unwrap()
}

fn write_schema(temp: &tempfile::TempDir, marker: &str) {
    write_schema_path(&temp.path().join("schema.json"), marker);
}

fn write_schema_path(path: &std::path::Path, marker: &str) {
    let schema = Schema {
        source_hashes: [("test-marker".to_owned(), marker.to_owned())]
            .into_iter()
            .collect(),
        type_ops_hash: String::new(),
        layout_hashes: Default::default(),
        rustc_version: String::new(),
        types: Vec::new(),
        layouts: vec![SchemaLayouts {
            identity: test_layout_identity(),
            layouts: Vec::new(),
        }],
    };
    std::fs::write(path, serde_json::to_vec(&schema).unwrap()).unwrap();
}

fn wait_until(mut predicate: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !predicate() {
        assert!(Instant::now() < deadline, "{message}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn process_serves_rpc_and_consumes_watcher_changes_until_drop() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let mut socket = TcpStream::connect(process.rpc_address()).unwrap();
    socket.write_all(&[]).unwrap();

    let before = process.coordinator().server().current_stamp().version.0;
    assert!(matches!(
        process
            .coordinator()
            .store()
            .read()
            .pipeline_state()
            .unwrap(),
        Some(PipelineState::Failed { .. })
    ));

    std::fs::write(temp.path().join("assets/source.txt"), b"source").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while process.coordinator().server().current_stamp().version.0 <= before {
        assert!(Instant::now() < deadline, "watcher batch did not publish");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(matches!(
        process
            .coordinator()
            .store()
            .read()
            .pipeline_state()
            .unwrap(),
        Some(PipelineState::Failed { .. })
    ));
    assert!(process.last_background_error().is_none());
    assert!(
        !temp.path().join("generated").exists(),
        "disabled codegen must not create its output directory"
    );
    drop(process);
}

#[test]
fn a_write_whose_rows_were_never_committed_is_adopted_on_restart() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    let config = config(&temp);
    let target = assets.join("pending.txt");
    std::fs::write(&target, b"old").unwrap();
    let old_hash = ContentHash(*blake3::hash(b"old").as_bytes());
    let new_hash = ContentHash(*blake3::hash(b"new").as_bytes());
    let indexed = |process: &DaemonProcess, hash: ContentHash| {
        process
            .coordinator()
            .store()
            .read()
            .all_files()
            .unwrap()
            .iter()
            .any(|(_, path, state)| path == "pending.txt" && state.content_hash == Some(hash))
    };
    let process = DaemonProcess::start(config.clone()).unwrap();
    assert!(indexed(&process, old_hash));
    drop(process);

    // A publication wrote the file and stopped before its store commit.
    distill_daemon::atomic::atomic_write(&target, b"new").unwrap();

    let process = DaemonProcess::start(config).unwrap();
    assert!(indexed(&process, new_hash));
    assert_eq!(
        std::fs::read_dir(&assets).unwrap().count(),
        1,
        "no temp file is left next to the target"
    );
}

#[cfg(unix)]
#[test]
fn disabled_existing_codegen_output_is_still_excluded() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp);
    let output = temp.path().join("generated");
    std::fs::create_dir_all(&output).unwrap();
    std::fs::write(output.join("owned.rs"), b"daemon owned").unwrap();
    let alias = temp.path().join("assets/generated-alias");
    symlink(&output, &alias).unwrap();

    let process = DaemonProcess::start(config).unwrap();
    assert!(process
        .coordinator()
        .scan_diagnostics()
        .unwrap()
        .iter()
        .any(|diagnostic| matches!(
            diagnostic,
            ScanDiagnostic::DaemonOwnedDirectoryAlias {
                normalized_path,
                kind: DaemonOwnedDirectoryKind::CodegenOutput,
                ..
            } if normalized_path == "generated-alias"
        )));
    assert!(process
        .coordinator()
        .store()
        .read()
        .all_files()
        .unwrap()
        .iter()
        .all(|(_, path, _)| !path.starts_with("generated-alias")));
    assert!(output.join("owned.rs").is_file());
}

#[test]
fn malformed_configuration_publishes_once_and_a_valid_edit_heals_it() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let path = temp.path().join("distill.toml");
    std::fs::write(&path, "not = [valid").unwrap();

    wait_until(
        || {
            matches!(
                process
                    .coordinator()
                    .store()
                    .read()
                    .configuration_state()
                    .unwrap(),
                ConfigurationState::Failed { reason, .. }
                    if matches!(reason.detail.as_ref(), DscpV1::MalformedConfiguration { .. })
            )
        },
        "malformed configuration was not published",
    );
    let failed = process.coordinator().server().current_stamp().version;
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        failed
    );

    std::fs::write(&path, config_source(&temp)).unwrap();
    wait_until(
        || {
            if let Some(error) = process.last_background_error() {
                panic!("background reconciliation failed: {error}");
            }
            let version = process.coordinator().server().current_stamp().version;
            let state = process
                .coordinator()
                .store()
                .read()
                .configuration_state()
                .unwrap();
            version > failed && matches!(state, ConfigurationState::Ready(_))
        },
        "valid configuration did not clear its malformed-source error",
    );
}

#[test]
fn valid_configuration_with_malformed_schema_fails_only_the_pipeline() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let config_path = temp.path().join("distill.toml");
    std::fs::write(&config_path, "not = [valid").unwrap();
    wait_until(
        || {
            matches!(
                process
                    .coordinator()
                    .store()
                    .read()
                    .configuration_state()
                    .unwrap(),
                ConfigurationState::Failed { reason, .. }
                    if matches!(reason.detail.as_ref(), DscpV1::MalformedConfiguration { .. })
            )
        },
        "malformed configuration was not published",
    );

    std::fs::write(temp.path().join("schema.json"), b"not-json").unwrap();
    std::fs::write(&config_path, config_source(&temp)).unwrap();
    wait_until(
        || {
            let store = process.coordinator().store();
            let store = store.read();
            matches!(
                store.configuration_state().unwrap(),
                ConfigurationState::Ready(_)
            ) && matches!(
                store.pipeline_state().unwrap(),
                Some(PipelineState::Failed { error, .. })
                    if error.message.contains("schema authority")
            )
        },
        "valid configuration with a malformed schema did not fail only the pipeline",
    );
    assert!(
        process.last_background_error().is_none(),
        "background reconciliation failed: {:?}",
        process.last_background_error()
    );
}

#[test]
fn simultaneous_configuration_defects_choose_canonical_authority() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let source = config_source(&temp)
        .replace("apis = [\"vulkan\"]", "apis = []")
        .replace("parallelism = 2", "parallelism = 0");
    std::fs::write(temp.path().join("distill.toml"), source).unwrap();

    wait_until(
        || {
            matches!(
                process
                    .coordinator()
                    .store()
                    .read()
                    .configuration_state()
                    .unwrap(),
                ConfigurationState::Failed { reason, .. }
                    if matches!(
                        reason.detail.as_ref(),
                        DscpV1::EmptyTargetApis { target } if target == "dev"
                    )
            )
        },
        "canonical configuration defect was not selected",
    );
    assert!(process.last_background_error().is_none());
}

#[test]
fn schema_bound_target_mismatches_publish_configuration_error() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp);
    let process = DaemonProcess::start(config).unwrap();
    let source = config_source(&temp).replace("os = \"macos\"", "os = \"linux\"");
    std::fs::write(temp.path().join("distill.toml"), source).unwrap();

    wait_until(
        || {
            matches!(
                process
                    .coordinator()
                    .store()
                    .read()
                    .configuration_state()
                    .unwrap(),
                ConfigurationState::Failed { reason, .. }
                    if matches!(
                        reason.detail.as_ref(),
                        DscpV1::UnsupportedTargetIdentity { target, .. } if target == "dev"
                    )
            )
        },
        "schema-bound target mismatch was not published",
    );
    assert!(process.last_background_error().is_none());
}

#[test]
fn operational_configuration_applies_live_without_an_input_version() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let before = process.coordinator().server().current_stamp().version;
    let edited = config_source(&temp).replace("parallelism = 2", "parallelism = 3");
    std::fs::write(temp.path().join("distill.toml"), edited).unwrap();

    wait_until(
        || {
            process
                .coordinator()
                .operational_configuration()
                .parallelism
                == 3
        },
        "operational parallelism was not applied",
    );
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        before
    );
    assert_eq!(
        process.coordinator().store().config().parallelism,
        3
    );
}

#[test]
fn restart_only_configuration_is_staged_without_an_input_version() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let before = process.coordinator().server().current_stamp().version;
    let edited = config_source(&temp).replace("auto_codegen = false", "auto_codegen = true");
    std::fs::write(temp.path().join("distill.toml"), edited).unwrap();

    wait_until(
        || {
            process
                .coordinator()
                .store()
                .read()
                .pending_restart()
                .unwrap()
                .is_some_and(|pending| pending.keys == ["codegen.auto_codegen"])
        },
        "restart-only edit was not staged",
    );
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        before
    );
    assert!(
        !temp.path().join("generated").exists(),
        "a staged restart-only edit must not activate codegen in this process"
    );
}

#[test]
fn startup_auto_codegen_activates_the_service_and_surfaces_pipeline_unavailability() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let path = temp.path().join("distill.toml");
    std::fs::write(
        &path,
        config_source(&temp).replace("auto_codegen = false", "auto_codegen = true"),
    )
    .unwrap();
    write_schema(&temp, "initial");

    let process = DaemonProcess::start(DaemonConfig::load(path).unwrap()).unwrap();

    assert!(temp.path().join("generated").is_dir());
    assert!(
        process.last_background_error().is_some(),
        "the enabled service must report that the intentionally missing pipeline cannot run"
    );
}

#[test]
fn target_configuration_and_pipeline_validation_publish_as_one_version() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    wait_until(
        || process.coordinator().build_target("dev").is_some(),
        "initial build target was not retained",
    );
    assert!(
        !process
            .coordinator()
            .build_target("dev")
            .expect("initial target is retained for build execution")
            .optimize
    );
    let before = process.coordinator().server().current_stamp().version;
    let edited = config_source(&temp).replace("optimize = false", "optimize = true");
    std::fs::write(temp.path().join("distill.toml"), edited).unwrap();

    wait_until(
        || process.coordinator().server().current_stamp().version > before,
        "target configuration did not publish",
    );
    let after = process.coordinator().server().current_stamp().version;
    assert_eq!(after.0, before.0 + 1);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        after
    );
    assert!(
        process
            .coordinator()
            .build_target("dev")
            .expect("published target is retained for build execution")
            .optimize
    );
    assert!(process.last_background_error().is_none());
}

#[test]
fn root_configuration_reconciles_new_namespace_in_the_same_version() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let before = process.coordinator().server().current_stamp().version;
    let second = temp.path().join("second-assets");
    std::fs::create_dir(&second).unwrap();
    std::fs::write(second.join("new.txt"), b"new root").unwrap();
    let edited = config_source(&temp).replace(
        &temp.path().join("assets").display().to_string(),
        &second.display().to_string(),
    );
    std::fs::write(temp.path().join("distill.toml"), edited).unwrap();

    wait_until(
        || {
            process.coordinator().server().current_stamp().version > before
                && process
                    .coordinator()
                    .store()
                    .read()
                    .all_files()
                    .unwrap()
                    .iter()
                    .any(|(_, path, _)| path == "new.txt")
        },
        "new root was not reconciled",
    );
    let after = process.coordinator().server().current_stamp().version;
    assert_eq!(after.0, before.0 + 1);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        after
    );
    assert_eq!(
        process
            .coordinator()
            .scanner()
            .physical_path("main", "new.txt")
            .unwrap(),
        std::fs::canonicalize(&second).unwrap().join("new.txt")
    );
}

#[test]
fn same_path_schema_edits_publish_exactly_one_atomic_candidate_version() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let before = process.coordinator().server().current_stamp().version;

    write_schema(&temp, "changed");
    wait_until(
        || process.coordinator().server().current_stamp().version > before,
        "same-path schema edit did not publish",
    );
    let after = process.coordinator().server().current_stamp().version;
    assert_eq!(after.0, before.0 + 1);
    let expected_schema_hash =
        *blake3::hash(&std::fs::read(temp.path().join("schema.json")).unwrap()).as_bytes();
    assert_eq!(
        process
            .coordinator()
            .schema_authority()
            .unwrap()
            .source_hash(),
        expected_schema_hash
    );
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        after
    );
    assert!(process.last_background_error().is_none());
}

#[test]
fn config_retargets_native_schema_watch_without_polling_the_old_path() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let old_schema = temp.path().join("schema.json");
    let next_schema = temp.path().join("schema-next.json");
    write_schema_path(&next_schema, "next-initial");
    let before = process.coordinator().server().current_stamp().version;
    let edited = config_source(&temp).replace(
        &old_schema.display().to_string(),
        &next_schema.display().to_string(),
    );
    std::fs::write(temp.path().join("distill.toml"), edited).unwrap();
    wait_until(
        || process.coordinator().server().current_stamp().version > before,
        "schema watch path replacement did not publish",
    );
    let replaced = process.coordinator().server().current_stamp().version;

    write_schema_path(&old_schema, "obsolete-path");
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        replaced,
        "the retired schema path must no longer drive candidates"
    );

    write_schema_path(&next_schema, "next-edited");
    wait_until(
        || process.coordinator().server().current_stamp().version > replaced,
        "replacement schema path was not watched",
    );
    assert_eq!(
        process.coordinator().server().current_stamp().version.0,
        replaced.0 + 1
    );
}

#[test]
fn malformed_schema_is_a_stable_pipeline_failure_and_a_valid_edit_retries() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let before = process.coordinator().server().current_stamp().version;
    std::fs::write(temp.path().join("schema.json"), b"not-json").unwrap();

    wait_until(
        || {
            matches!(
                process
                    .coordinator()
                    .store()
                    .read()
                    .pipeline_state()
                    .unwrap(),
                Some(PipelineState::Failed { error, .. })
                    if error.message.contains("schema authority")
            )
        },
        "malformed schema was not published as a pipeline failure",
    );
    let failed = process.coordinator().server().current_stamp().version;
    assert_eq!(failed.0, before.0 + 1);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        failed
    );

    write_schema(&temp, "healed");
    wait_until(
        || process.coordinator().server().current_stamp().version > failed,
        "valid schema edit did not retry the atomic candidate",
    );
    assert_eq!(
        process.coordinator().server().current_stamp().version.0,
        failed.0 + 1
    );
    assert!(process.last_background_error().is_none());
}
