use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use distill_daemon::config::DaemonConfig;
use distill_daemon::process::DaemonProcess;
use distill_schema::bootstrap_gen_v1::consumer_compilation_identity_v1;
use distill_schema::ngp_schema::{Schema, SchemaLayouts};
use distill_store::state::{ConfigurationState, DscpV1, PipelineState};

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
lineage_manifest = {{ root = "main", path = "schema/lineage.bundle" }}
[modules]
pipeline_dylib = "{}"
[targets.dev]
os = "linux"
arch = "x86_64"
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
    let schema = Schema {
        source_hashes: [("test-marker".to_owned(), marker.to_owned())]
            .into_iter()
            .collect(),
        types: Vec::new(),
        layouts: vec![SchemaLayouts {
            identity: consumer_compilation_identity_v1().clone(),
            layouts: Vec::new(),
        }],
    };
    std::fs::write(
        temp.path().join("schema.json"),
        serde_json::to_vec(&schema).unwrap(),
    )
    .unwrap();
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
            .lock()
            .unwrap()
            .pipeline_state()
            .unwrap(),
        Some(PipelineState::Poisoned { .. })
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
            .lock()
            .unwrap()
            .pipeline_state()
            .unwrap(),
        Some(PipelineState::Poisoned { .. })
    ));
    assert!(process.last_background_error().is_none());
    drop(process);
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
                    .lock()
                    .unwrap()
                    .configuration_state()
                    .unwrap(),
                ConfigurationState::Poisoned { reason, .. }
                    if matches!(reason.detail.as_ref(), DscpV1::MalformedConfiguration { .. })
            )
        },
        "malformed configuration was not published",
    );
    let poisoned = process.coordinator().server().current_stamp().version;
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        poisoned
    );

    std::fs::write(&path, config_source(&temp)).unwrap();
    wait_until(
        || {
            process.coordinator().server().current_stamp().version > poisoned
                && matches!(
                    process
                        .coordinator()
                        .store()
                        .lock()
                        .unwrap()
                        .configuration_state()
                        .unwrap(),
                    ConfigurationState::Poisoned { reason, .. }
                        if matches!(reason.detail.as_ref(), DscpV1::MissingLineageManifest)
                )
        },
        "valid configuration did not clear its malformed-source poison",
    );
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
        process
            .coordinator()
            .store()
            .lock()
            .unwrap()
            .operational_config()
            .parallelism,
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
                .lock()
                .unwrap()
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
}

#[test]
fn target_configuration_and_pipeline_validation_publish_as_one_version() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
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
                    .lock()
                    .unwrap()
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
fn malformed_schema_is_a_stable_pipeline_poison_and_a_valid_edit_retries() {
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
                    .lock()
                    .unwrap()
                    .pipeline_state()
                    .unwrap(),
                Some(PipelineState::Poisoned { error, .. })
                    if error.message.contains("schema authority")
            )
        },
        "malformed schema was not published as pipeline poison",
    );
    let poisoned = process.coordinator().server().current_stamp().version;
    assert_eq!(poisoned.0, before.0 + 1);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        process.coordinator().server().current_stamp().version,
        poisoned
    );

    write_schema(&temp, "healed");
    wait_until(
        || process.coordinator().server().current_stamp().version > poisoned,
        "valid schema edit did not retry the atomic candidate",
    );
    assert_eq!(
        process.coordinator().server().current_stamp().version.0,
        poisoned.0 + 1
    );
    assert!(process.last_background_error().is_none());
}
