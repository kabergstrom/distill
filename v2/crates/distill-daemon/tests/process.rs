use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distill_core::id::ContentHash;
use distill_daemon::compiled::Compiled;
use distill_daemon::config::DaemonConfig;
use distill_daemon::process::DaemonProcess;
use distill_daemon::scanner::{DaemonOwnedDirectoryKind, ScanDiagnostic};
use distill_schema::ngp_schema::{LayoutIdentity, Schema, SchemaLayouts};
use distill_rpc::ConfigurationStatus;
use distill_store::state::DscpV1;

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
state_path = '{}'
[assets]
roots = {{ main = '{}' }}
schema_path = '{}'
[modules]
pipeline_dylib = '{}'
[targets.dev]
os = "macos"
arch = "aarch64"
apis = ["vulkan"]
optimize = false
[codegen]
rs_mod_path = '{}'
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

/// The compiled state the process's latest committed version sees.
fn compiled(process: &DaemonProcess) -> Option<Arc<Compiled>> {
    let coordinator = process.coordinator();
    let reader = coordinator.open_reader().unwrap();
    coordinator.compiled_at(&reader).ok()
}

fn wait_until(mut predicate: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !predicate() {
        assert!(Instant::now() < deadline, "{message}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Long enough after a change for any pass it causes to have run: twice
/// the default 250 ms quiet window.
const SETTLED: Duration = Duration::from_millis(500);

#[test]
fn watcher_changes_wait_for_the_configured_quiet_window() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("assets")).unwrap();
    let path = temp.path().join("distill.toml");
    let source = format!("{}[watch]\nquiet_ms = 1000\n", config_source(&temp));
    std::fs::write(&path, source).unwrap();
    write_schema(&temp, "initial");
    let process = DaemonProcess::start(DaemonConfig::load(path).unwrap()).unwrap();
    // The pass that follows startup.
    std::thread::sleep(SETTLED);
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;

    // Three writes 300 ms apart: each restarts the 1 s window.
    let asset = temp.path().join("assets/source.txt");
    let mut last_write = Instant::now();
    for content in ["one", "two", "three"] {
        // Taken before the write: its event cannot arrive earlier.
        last_write = Instant::now();
        std::fs::write(&asset, content).unwrap();
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version,
            before,
            "nothing reconciles while changes keep arriving"
        );
    }
    wait_until(
        || {
            process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version
                > before
        },
        "the burst was not reconciled",
    );
    assert!(
        last_write.elapsed() >= Duration::from_secs(1),
        "reconciled {:?} after the last write",
        last_write.elapsed()
    );
    assert!(process
        .coordinator()
        .open_reader()
        .unwrap()
        .observed_files()
        .unwrap()
        .iter()
        .any(|row| row.path == "source.txt"));
    assert!(process.last_background_error().is_none());
}

#[test]
fn process_serves_rpc_and_consumes_watcher_changes_until_drop() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let mut socket = TcpStream::connect(process.rpc_address()).unwrap();
    socket.write_all(&[]).unwrap();

    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version
        .0;
    assert!(matches!(
        process
            .coordinator()
            .pipeline_failure(&process.coordinator().open_reader().unwrap())
            .unwrap(),
        Some(_)
    ));

    std::fs::write(temp.path().join("assets/source.txt"), b"source").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version
        .0
        <= before
    {
        assert!(Instant::now() < deadline, "watcher batch did not publish");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(matches!(
        process
            .coordinator()
            .pipeline_failure(&process.coordinator().open_reader().unwrap())
            .unwrap(),
        Some(_)
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
            .open_reader()
            .unwrap()
            .observed_files()
            .unwrap()
            .iter()
            .any(|row| row.path == "pending.txt" && row.file.state.content_hash == Some(hash))
    };
    let process = DaemonProcess::start(config.clone()).unwrap();
    assert!(indexed(&process, old_hash));
    drop(process);

    // A publication wrote the file and stopped before its store commit.
    distill_store::atomic_file::write(
        &assets,
        &target,
        b"new",
        distill_store::atomic_file::Expected::Any,
    )
    .unwrap();

    let process = DaemonProcess::start(config).unwrap();
    assert!(indexed(&process, new_hash));
    let names = std::fs::read_dir(&assets)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(
        names.len(),
        2,
        "only the target and the staging directory: {names:?}"
    );
    assert_eq!(
        std::fs::read_dir(distill_store::atomic_file::staging_dir(&assets))
            .unwrap()
            .count(),
        0,
        "no temp file is left behind"
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
        .scanner()
        .scan()
        .unwrap()
        .diagnostic_rows()
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
        .open_reader()
        .unwrap()
        .observed_files()
        .unwrap()
        .iter()
        .all(|row| !row.path.starts_with("generated-alias")));
    assert!(output.join("owned.rs").is_file());
}

/// Directory-import rules generating one `collide.bundle` from each `*.src`:
/// a collision no pass can reconcile while two sources exist.
fn colliding_directory_rules_bundle() -> Vec<u8> {
    use distill_core::bootstrap::{
        BootstrapControlSpecV1, BootstrapControlSymbol, DIRECTORY_IMPORT_RULES_TYPE_UUID,
    };
    use distill_json::AuthoredValue;
    let object = |fields: Vec<(&str, AuthoredValue)>| {
        AuthoredValue::Object(
            fields
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect(),
        )
    };
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::DirectoryImportRules)
        .unwrap();
    let schema = distill_schema::ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
    let query = || {
        object(vec![
            ("path_glob", AuthoredValue::Str("*.src".into())),
            ("path_prefix", AuthoredValue::Null),
        ])
    };
    let rule = |id: u8| {
        object(vec![
            ("group", object(vec![("PerFile", object(Vec::new()))])),
            (
                "id",
                AuthoredValue::Array(vec![AuthoredValue::UInt(id.into()); 16]),
            ),
            ("importer", AuthoredValue::Str("missing-importer".into())),
            ("matches", query()),
            ("output", AuthoredValue::Str("collide.bundle".into())),
            (
                "settings",
                object(vec![(
                    "UInt",
                    object(vec![("value", AuthoredValue::UInt(5))]),
                )]),
            ),
        ])
    };
    let data = object(vec![
        ("listing", query()),
        ("rules", AuthoredValue::Array(vec![rule(1)])),
    ]);
    distill_bundle::write_bundle(&distill_bundle::Bundle {
        format_version: 1,
        uuid: distill_core::id::BundleUuid([31; 16]),
        primary: None,
        schemas: [(row.logical_hash, schema)].into_iter().collect(),
        assets: [(
            "rules".to_owned(),
            distill_bundle::AssetEntry {
                uuid: distill_core::id::AssetUuid([32; 16]),
                type_uuid: DIRECTORY_IMPORT_RULES_TYPE_UUID,
                schema_hash: row.logical_hash,
                authoring_only: true,
                data,
            },
        )]
        .into_iter()
        .collect(),
    })
    .unwrap()
}

/// A failed pass reports its error, and keeps retrying, until a later pass
/// succeeds: that pass clears the error.
#[test]
fn a_background_error_clears_once_a_later_pass_succeeds() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    std::fs::write(
        temp.path().join("assets/rules.bundle"),
        colliding_directory_rules_bundle(),
    )
    .unwrap();
    std::fs::write(temp.path().join("assets/foo.src"), b"1").unwrap();
    let source = temp.path().join("assets/bar.src");
    std::fs::write(&source, b"2").unwrap();
    wait_until(
        || {
            process
                .last_background_error()
                .is_some_and(|error| error.contains("directory import rules collide"))
        },
        "the colliding rules did not fail the pass",
    );

    std::fs::remove_file(&source).unwrap();
    wait_until(
        || process.last_background_error().is_none(),
        "the pass that succeeded did not clear the error",
    );
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
                    .configuration_status()
                    .unwrap(),
                ConfigurationStatus::Failed(reason)
                    if matches!(reason.detail.as_ref(), DscpV1::MalformedConfiguration { .. })
            )
        },
        "malformed configuration was not published",
    );
    let failed = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    std::thread::sleep(SETTLED);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        failed
    );

    std::fs::write(&path, config_source(&temp)).unwrap();
    wait_until(
        || {
            if let Some(error) = process.last_background_error() {
                panic!("background reconciliation failed: {error}");
            }
            let version = process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version;
            let state = process
                .coordinator()
                .configuration_status()
                .unwrap();
            version > failed && matches!(state, ConfigurationStatus::Ready)
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
                    .configuration_status()
                    .unwrap(),
                ConfigurationStatus::Failed(reason)
                    if matches!(reason.detail.as_ref(), DscpV1::MalformedConfiguration { .. })
            )
        },
        "malformed configuration was not published",
    );

    std::fs::write(temp.path().join("schema.json"), b"not-json").unwrap();
    std::fs::write(&config_path, config_source(&temp)).unwrap();
    wait_until(
        || {
            let store = process.coordinator().open_reader().unwrap();
            matches!(
                process.coordinator().configuration_status().unwrap(),
                ConfigurationStatus::Ready
            ) && matches!(
                process.coordinator().pipeline_failure(&store).unwrap(),
                Some(error) if error.message.contains("schema authority")
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
                    .configuration_status()
                    .unwrap(),
                ConfigurationStatus::Failed(reason)
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
                    .configuration_status()
                    .unwrap(),
                ConfigurationStatus::Failed(reason)
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
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
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
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        before
    );
    assert_eq!(
        process
            .coordinator()
            .server_handle()
            .opener()
            .config()
            .parallelism,
        3
    );
}

#[test]
fn restart_only_configuration_is_announced_without_an_input_version() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    let edited = config_source(&temp).replace("auto_codegen = false", "auto_codegen = true");
    std::fs::write(temp.path().join("distill.toml"), edited).unwrap();

    wait_until(
        || {
            process.coordinator().server_handle().restart_required().1 == ["codegen.auto_codegen"]
        },
        "restart-only edit was not announced",
    );
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        before
    );
    assert!(
        !temp.path().join("generated").exists(),
        "a restart-only edit must not activate codegen in this process"
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
        || {
            compiled(&process)
                .and_then(|compiled| compiled.build_target("dev"))
                .is_some()
        },
        "initial build target was not retained",
    );
    assert!(
        !compiled(&process)
            .and_then(|compiled| compiled.build_target("dev"))
            .expect("initial target is retained for build execution")
            .optimize
    );
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    let edited = config_source(&temp).replace("optimize = false", "optimize = true");
    std::fs::write(temp.path().join("distill.toml"), edited).unwrap();

    wait_until(
        || {
            process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version
                > before
        },
        "target configuration did not publish",
    );
    let after = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    assert_eq!(after.0, before.0 + 1);
    std::thread::sleep(SETTLED);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        after
    );
    assert!(
        compiled(&process)
            .and_then(|compiled| compiled.build_target("dev"))
            .expect("published target is retained for build execution")
            .optimize
    );
    assert!(process.last_background_error().is_none());
}

#[test]
fn root_configuration_reconciles_new_namespace_in_the_same_version() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
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
            process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version
                > before
                && process
                    .coordinator()
                    .open_reader()
                    .unwrap()
                    .observed_files()
                    .unwrap()
                    .iter()
                    .any(|row| row.path == "new.txt")
        },
        "new root was not reconciled",
    );
    let after = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    assert_eq!(after.0, before.0 + 1);
    std::thread::sleep(SETTLED);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
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
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;

    write_schema(&temp, "changed");
    wait_until(
        || {
            process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version
                > before
        },
        "same-path schema edit did not publish",
    );
    let after = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    assert_eq!(after.0, before.0 + 1);
    let expected_schema_hash =
        *blake3::hash(&std::fs::read(temp.path().join("schema.json")).unwrap()).as_bytes();
    assert_eq!(
        compiled(&process)
            .and_then(|compiled| compiled.schema_authority())
            .unwrap()
            .source_hash(),
        expected_schema_hash
    );
    std::thread::sleep(SETTLED);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
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
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    let edited = config_source(&temp).replace(
        &old_schema.display().to_string(),
        &next_schema.display().to_string(),
    );
    std::fs::write(temp.path().join("distill.toml"), edited).unwrap();
    wait_until(
        || {
            process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version
                > before
        },
        "schema watch path replacement did not publish",
    );
    let replaced = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;

    write_schema_path(&old_schema, "obsolete-path");
    std::thread::sleep(SETTLED);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        replaced,
        "the retired schema path must no longer drive candidates"
    );

    write_schema_path(&next_schema, "next-edited");
    wait_until(
        || {
            process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version
                > replaced
        },
        "replacement schema path was not watched",
    );
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version
            .0,
        replaced.0 + 1
    );
}

#[test]
fn malformed_schema_is_a_stable_pipeline_failure_and_a_valid_edit_retries() {
    let temp = tempfile::tempdir().unwrap();
    let process = DaemonProcess::start(config(&temp)).unwrap();
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    std::fs::write(temp.path().join("schema.json"), b"not-json").unwrap();

    wait_until(
        || {
            matches!(
                process
                    .coordinator()
                    .pipeline_failure(&process.coordinator().open_reader().unwrap())
                    .unwrap(),
                Some(error) if error.message.contains("schema authority")
            )
        },
        "malformed schema was not published as a pipeline failure",
    );
    let failed = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;
    assert_eq!(failed.0, before.0 + 1);
    std::thread::sleep(SETTLED);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        failed
    );

    write_schema(&temp, "healed");
    wait_until(
        || {
            process
                .coordinator()
                .server()
                .current_stamp()
                .unwrap()
                .version
                > failed
        },
        "valid schema edit did not retry the atomic candidate",
    );
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version
            .0,
        failed.0 + 1
    );
    assert!(process.last_background_error().is_none());
}

// ---------------------------------------------------------------------------
// Pipeline swaps gated on the watched schema (ngp_module_host::SourceGate).
//
// One standalone pipeline crate is built in three variants, in the same
// directory so their source and layout hashes are comparable:
// - `v1`: importer version 1;
// - `v2`: importer version 2 and a new struct (a layout change);
// - `v3`: importer version 3, otherwise `v2` (a fn-body-only change: same
//   layout hash as `v2`).
// The importer writes its version into the imported value, so a capability-
// driven reimport is visible in the bundle.

const GATE_SETTINGS_TYPE: [u8; 16] = [0xa0; 16];
const GATE_SOURCE_TYPE: [u8; 16] = [0xa1; 16];
const GATE_CRATE: &str = "distill_gate_fixture";

struct GateVariant {
    dylib: std::path::PathBuf,
    source_hash: String,
    layout_hash: String,
}

struct GateVariants {
    v1: GateVariant,
    v2: GateVariant,
    v3: GateVariant,
}

fn gate_fixture_source(version: u32, layout_marker: bool) -> String {
    let marker = if layout_marker {
        "pub struct LayoutMarker {\n    pub value: u32,\n}\n"
    } else {
        ""
    };
    format!(
        r#"use std::collections::{{BTreeMap, BTreeSet}};

use distill_core::id::TypeUuid;
use distill_json::AuthoredValue;
use distill_pipeline_api::callbacks::ImporterDescriptor;
use distill_pipeline_api::import::ImportOutput;
use distill_pipeline_api::importer::{{
    AuthoringImportContext, AuthoringImporter, AuthoringImporterError,
}};
use distill_pipeline_api::registration::{{ModuleCallError, RegistrationArena, TargetDefinition}};
use distill_schema::ngp_schema::{{LogicalSchema, PrimitiveKind, SchemaNode}};

const SETTINGS_TYPE: TypeUuid = TypeUuid([0xa0; 16]);
const SOURCE_TYPE: TypeUuid = TypeUuid([0xa1; 16]);

#[derive(newgameplus_api_macros::NgpSourceIdentity)]
pub struct SourceIdentity;

{marker}
fn importer_version() -> u32 {{
    {version}
}}

fn settings_schema() -> SchemaNode {{
    SchemaNode::Struct {{
        rev: 0,
        fields: vec![("value".to_owned(), 0, SchemaNode::Primitive(PrimitiveKind::U8))],
    }}
}}

fn settings_value() -> AuthoredValue {{
    AuthoredValue::Object(BTreeMap::from([("value".to_owned(), AuthoredValue::UInt(0))]))
}}

struct GateImporter;

impl AuthoringImporter for GateImporter {{
    fn id(&self) -> &str {{
        "gate-fixture"
    }}

    fn version(&self) -> u32 {{
        importer_version()
    }}

    fn settings_type_uuid(&self) -> TypeUuid {{
        SETTINGS_TYPE
    }}

    fn settings_schema(&self) -> &LogicalSchema {{
        static SETTINGS: std::sync::OnceLock<LogicalSchema> = std::sync::OnceLock::new();
        SETTINGS.get_or_init(|| LogicalSchema {{ root: settings_schema() }})
    }}

    fn default_settings(&self) -> AuthoredValue {{
        settings_value()
    }}

    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        _settings: &AuthoredValue,
    ) -> Result<ImportOutput, AuthoringImporterError> {{
        let source = context
            .sources()
            .first()
            .map(|source| source.path.clone())
            .ok_or_else(|| AuthoringImporterError::rejected(1, "no source"))?;
        let bytes = context.read(&source)?;
        let text = String::from_utf8_lossy(&bytes);
        let mut output = ImportOutput::new();
        output
            .entry(
                "main",
                SOURCE_TYPE,
                AuthoredValue::Object(BTreeMap::from([(
                    "value".to_owned(),
                    AuthoredValue::Str(format!("v{{}}:{{text}}", importer_version())),
                )])),
            )
            .map_err(|error| AuthoringImporterError::rejected(2, format!("{{error:?}}")))?;
        output
            .primary("main")
            .map_err(|error| AuthoringImporterError::rejected(3, format!("{{error:?}}")))?;
        Ok(output)
    }}
}}

fn register(
    targets: &[TargetDefinition],
    arena: &mut RegistrationArena,
) -> Result<BTreeSet<String>, ModuleCallError> {{
    arena
        .register_importer(
            ImporterDescriptor {{
                id: "gate-fixture".to_owned(),
                version: importer_version(),
                settings_type_uuid: SETTINGS_TYPE,
                settings_schema: LogicalSchema {{ root: settings_schema() }},
                default_settings: settings_value(),
            }},
            GateImporter,
        )
        .into_result()?;
    Ok(targets.iter().map(|target| target.name.clone()).collect())
}}

fn unload() -> Result<(), ModuleCallError> {{
    Ok(())
}}

distill_pipeline_api::export_pipeline_module_v2!(register = register, unload = unload);
"#
    )
}

/// Build the three variants once per test binary (they share one cargo
/// output path, so building is serialized here).
fn gate_variants() -> &'static GateVariants {
    static VARIANTS: std::sync::OnceLock<GateVariants> = std::sync::OnceLock::new();
    VARIANTS.get_or_init(|| {
        let daemon = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let crates = daemon.parent().unwrap();
        let workspace = crates.parent().unwrap();
        let newgameplus = workspace.join("../../newgameplus");
        let target_dir = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| workspace.join("target/gate-fixture"));
        let root = target_dir.join("distill-gate-fixture");
        let crate_dir = root.join("crate");
        std::fs::create_dir_all(crate_dir.join("src")).unwrap();
        let path = |p: std::path::PathBuf| std::fs::canonicalize(p).unwrap().display().to_string();
        std::fs::write(
            crate_dir.join("Cargo.toml"),
            format!(
                r#"[package]
name = "distill-gate-fixture"
version = "0.1.0"
edition = "2024"
publish = false

[workspace]

[lib]
crate-type = ["cdylib"]

[dependencies]
distill-core = {{ path = '{}' }}
distill-json = {{ path = '{}' }}
distill-pipeline-api = {{ path = '{}' }}
distill-schema = {{ path = '{}' }}
newgameplus-api-macros = {{ path = '{}' }}

[build-dependencies]
ngp-source-hash = {{ path = '{}' }}
"#,
                path(crates.join("distill-core")),
                path(crates.join("distill-json")),
                path(crates.join("distill-pipeline-api")),
                path(crates.join("distill-schema")),
                path(newgameplus.join("newgameplus-api-macros")),
                path(newgameplus.join("ngp-source-hash")),
            ),
        )
        .unwrap();
        std::fs::write(
            crate_dir.join("build.rs"),
            "fn main() {\n    ngp_source_hash::build_script_main();\n}\n",
        )
        .unwrap();
        // Resolve offline to the versions the workspace already uses.
        if !crate_dir.join("Cargo.lock").exists() {
            std::fs::copy(workspace.join("Cargo.lock"), crate_dir.join("Cargo.lock")).unwrap();
        }
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let built = if cfg!(target_os = "windows") {
            "distill_gate_fixture.dll"
        } else if cfg!(target_os = "macos") {
            "libdistill_gate_fixture.dylib"
        } else {
            "libdistill_gate_fixture.so"
        };
        let build = |name: &str, version: u32, layout_marker: bool| {
            std::fs::write(
                crate_dir.join("src/lib.rs"),
                gate_fixture_source(version, layout_marker),
            )
            .unwrap();
            let output = std::process::Command::new(&cargo)
                .current_dir(&crate_dir)
                .env("CARGO_TARGET_DIR", &target_dir)
                .args(["build", "--offline"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "gate fixture build failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let dylib = root.join(format!("{name}-{built}"));
            std::fs::copy(target_dir.join("debug").join(built), &dylib).unwrap();
            let staged = ngp_module_host::stage_copy_to(
                &dylib,
                &root.join(format!("{name}-identity-{}-{built}", std::process::id())),
            )
            .unwrap();
            // SAFETY: the fixture was compiled just above from the source
            // written here; it is opened only to read its identity exports.
            let library = unsafe { ngp_module_host::HostedLibrary::open(staged.clone()) }.unwrap();
            // SAFETY: the fixture derives NgpSourceIdentity.
            let identity = unsafe { ngp_module_host::read_reload_identity(&library) }.unwrap();
            library.close();
            let _ = std::fs::remove_file(staged.path());
            assert_eq!(identity.source.crate_name, GATE_CRATE);
            assert!(
                !identity.layout_hash.is_empty(),
                "NgpSourceIdentity exports a layout hash"
            );
            GateVariant {
                dylib,
                source_hash: identity.source.source_hash,
                layout_hash: identity.layout_hash,
            }
        };
        let v1 = build("v1", 1, false);
        let v2 = build("v2", 2, true);
        let v3 = build("v3", 3, true);
        assert_ne!(v1.source_hash, v2.source_hash);
        assert_ne!(
            v1.layout_hash, v2.layout_hash,
            "a new struct changes the layout hash"
        );
        assert_ne!(v2.source_hash, v3.source_hash);
        assert_eq!(
            v2.layout_hash, v3.layout_hash,
            "a fn body keeps the layout hash"
        );
        GateVariants { v1, v2, v3 }
    })
}

fn write_gate_schema(temp: &tempfile::TempDir, variant: &GateVariant) {
    use distill_core::id::TypeUuid;
    use distill_schema::ngp_schema::{
        Field, FieldAttrs, FieldIdentifier, FieldLayout, PrimitiveType, SchemaTypeId, TypeAttrs,
        TypeDef, TypeLayout, TypePath,
    };
    let path = |krate: &str, name: &str| TypePath {
        name: Some(name.into()),
        containing_type: None,
        modules: Vec::new(),
        krate: krate.into(),
    };
    let type_def = |id, kind, path, uuid, attrs, fields| TypeDef {
        id: SchemaTypeId(id),
        kind,
        path,
        uuid,
        attrs,
        fields,
        generic_parameters: Vec::new(),
        generic_argument_ids: Vec::new(),
        has_default: true,
        generic_const_arguments: Vec::new(),
        has_explicit_discriminants: false,
    };
    let field = |name: &str, type_id| Field {
        id: FieldIdentifier::Name(name.into()),
        type_id: SchemaTypeId(type_id),
        attrs: FieldAttrs::default(),
    };
    let layout = |size: usize, align: usize, fields: Vec<(usize, usize)>| TypeLayout {
        size: Some(size as u64),
        align: Some(align as u64),
        layout_complete: true,
        tag_encoding: None,
        fields: fields
            .into_iter()
            .map(|(offset, size)| FieldLayout {
                offset: Some(offset as u64),
                field_size: Some(size as u64),
            })
            .collect(),
    };
    let string = std::mem::size_of::<String>();
    let schema = Schema {
        source_hashes: [(GATE_CRATE.to_owned(), variant.source_hash.clone())]
            .into_iter()
            .collect(),
        type_ops_hash: String::new(),
        layout_hashes: [(GATE_CRATE.to_owned(), variant.layout_hash.clone())]
            .into_iter()
            .collect(),
        rustc_version: String::new(),
        types: vec![
            type_def(
                0,
                PrimitiveType::U8,
                path("core", "u8"),
                None,
                TypeAttrs::default(),
                Vec::new(),
            ),
            type_def(
                1,
                PrimitiveType::Struct,
                path(GATE_CRATE, "GateSettings"),
                Some(TypeUuid(GATE_SETTINGS_TYPE)),
                TypeAttrs {
                    build_only: true,
                    ..TypeAttrs::default()
                },
                vec![field("value", 0)],
            ),
            type_def(
                2,
                PrimitiveType::String,
                path("alloc", "String"),
                None,
                TypeAttrs::default(),
                Vec::new(),
            ),
            type_def(
                3,
                PrimitiveType::Struct,
                path(GATE_CRATE, "GateSource"),
                Some(TypeUuid(GATE_SOURCE_TYPE)),
                TypeAttrs::default(),
                vec![field("value", 2)],
            ),
        ],
        layouts: vec![SchemaLayouts {
            identity: test_layout_identity(),
            layouts: vec![
                layout(1, 1, Vec::new()),
                layout(1, 1, vec![(0, 1)]),
                layout(string, std::mem::align_of::<String>(), Vec::new()),
                layout(string, std::mem::align_of::<String>(), vec![(0, string)]),
            ],
        }],
    };
    let schema_path = temp.path().join("schema.json");
    let staging = temp.path().join("schema.json.next");
    std::fs::write(&staging, serde_json::to_vec(&schema).unwrap()).unwrap();
    std::fs::rename(staging, schema_path).unwrap();
}

/// Replace the watched pipeline as cargo would: a complete new file.
fn install_gate_pipeline(temp: &tempfile::TempDir, variant: &GateVariant) {
    let staging = temp.path().join("pipeline.so.next");
    std::fs::copy(&variant.dylib, &staging).unwrap();
    std::fs::rename(staging, temp.path().join("pipeline.so")).unwrap();
}

fn start_gate_daemon(temp: &tempfile::TempDir, variant: &GateVariant) -> DaemonProcess {
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(assets.join("note.txt"), b"note").unwrap();
    install_gate_pipeline(temp, variant);
    write_gate_schema(temp, variant);
    let path = temp.path().join("distill.toml");
    std::fs::write(&path, config_source(temp)).unwrap();
    let config = DaemonConfig::load(path).unwrap();
    let process = DaemonProcess::start(config.clone()).unwrap();
    assert_eq!(ready_dylib_hash(&process), Some(dylib_hash(variant)));
    distill_daemon::bootstrap::import(
        &config,
        process.rpc_address(),
        None,
        &distill_rpc::ImportRequest {
            importer: "gate-fixture".into(),
            sources: vec!["note.txt".into()],
            dest: "note.bundle".into(),
            settings: distill_rpc::AuthoringValue {
                canonical_value: std::sync::Arc::from(&b"{\"value\":0}"[..]),
                blobs: Vec::new(),
            },
            watch: true,
            root: "main".into(),
        },
        Duration::from_secs(30),
    )
    .unwrap();
    process
}

fn dylib_hash(variant: &GateVariant) -> [u8; 32] {
    *blake3::hash(&std::fs::read(&variant.dylib).unwrap()).as_bytes()
}

fn ready_dylib_hash(process: &DaemonProcess) -> Option<[u8; 32]> {
    let store = process.coordinator().open_reader().unwrap();
    match process.coordinator().pipeline_failure(&store).unwrap() {
        None => process.coordinator().ready_dylib_hash(),
        Some(_) => None,
    }
}

fn pipeline_generation(process: &DaemonProcess) -> u64 {
    process
        .coordinator()
        .open_reader()
        .unwrap()
        .rpc_pipeline_generation()
        .unwrap()
}

fn imported_value(temp: &tempfile::TempDir) -> Option<String> {
    let bytes = std::fs::read(temp.path().join("assets/note.bundle")).ok()?;
    let bundle = distill_bundle::parse_bundle(&bytes).ok()?;
    match &bundle.assets.get("main")?.data {
        distill_json::AuthoredValue::Object(fields) => match fields.get("value") {
            Some(distill_json::AuthoredValue::Str(value)) => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn wait_long(mut predicate: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !predicate() {
        assert!(Instant::now() < deadline, "{message}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn pipeline_ahead_of_its_schema_keeps_serving_until_the_schema_catches_up() {
    let variants = gate_variants();
    let temp = tempfile::tempdir().unwrap();
    let process = start_gate_daemon(&temp, &variants.v1);
    assert_eq!(imported_value(&temp).as_deref(), Some("v1:note"));
    // Let the watcher publish the import's own bundle write first.
    std::thread::sleep(SETTLED);
    let generation = pipeline_generation(&process);
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;

    // Cargo rewrote the dylib; source-walk hasn't rewritten the schema yet,
    // and the layout changed: the candidate waits, nothing is published.
    install_gate_pipeline(&temp, &variants.v2);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        before
    );
    assert_eq!(pipeline_generation(&process), generation);
    assert_eq!(ready_dylib_hash(&process), Some(dylib_hash(&variants.v1)));
    assert_eq!(process.last_background_error(), None);
    assert_eq!(imported_value(&temp).as_deref(), Some("v1:note"));

    // Source-walk catches up: exactly one pipeline epoch change, and the
    // new importer version reimports the watched import.
    write_gate_schema(&temp, &variants.v2);
    wait_long(
        || ready_dylib_hash(&process) == Some(dylib_hash(&variants.v2)),
        "the matching schema did not adopt the waiting pipeline",
    );
    wait_long(
        || imported_value(&temp).as_deref() == Some("v2:note"),
        "the new importer version did not reimport the watched import",
    );
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(pipeline_generation(&process), generation + 1);
    assert_eq!(process.last_background_error(), None);
}

#[test]
fn pipeline_with_an_unchanged_layout_is_adopted_ahead_of_source_walk() {
    let variants = gate_variants();
    let temp = tempfile::tempdir().unwrap();
    let process = start_gate_daemon(&temp, &variants.v2);
    assert_eq!(imported_value(&temp).as_deref(), Some("v2:note"));
    let generation = pipeline_generation(&process);

    // Only fn bodies changed: the schema already describes this build. The
    // swap can share a pass with the watcher work of the import's own bundle
    // write, which the capability-driven reimport rewrites: more work for
    // the next pass, never an error.
    install_gate_pipeline(&temp, &variants.v3);
    let no_error = || {
        assert_eq!(process.last_background_error(), None);
    };
    wait_long(
        || {
            no_error();
            ready_dylib_hash(&process) == Some(dylib_hash(&variants.v3))
        },
        "a layout-preserving pipeline was not adopted ahead of source-walk",
    );
    wait_long(
        || {
            no_error();
            imported_value(&temp).as_deref() == Some("v3:note")
        },
        "the new importer version did not reimport the watched import",
    );
    assert_eq!(pipeline_generation(&process), generation + 1);
    assert_eq!(process.last_background_error(), None);
}

#[test]
fn source_walk_catching_up_after_an_ahead_of_walk_adoption_is_not_republished() {
    let variants = gate_variants();
    let temp = tempfile::tempdir().unwrap();
    let process = start_gate_daemon(&temp, &variants.v2);
    assert_eq!(imported_value(&temp).as_deref(), Some("v2:note"));
    // Let the watcher publish the import's own bundle write first.
    std::thread::sleep(SETTLED);
    let generation = pipeline_generation(&process);
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;

    // The order a rebuild job produces. cargo removes the dylib and links
    // the new one; a pass in between sees no module and keeps serving.
    std::fs::remove_file(temp.path().join("pipeline.so")).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(pipeline_generation(&process), generation);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        before
    );
    assert_eq!(ready_dylib_hash(&process), Some(dylib_hash(&variants.v2)));
    assert_eq!(process.last_background_error(), None);

    // The new dylib is adopted against the schema source-walk has not
    // refreshed yet.
    install_gate_pipeline(&temp, &variants.v3);
    wait_long(
        || ready_dylib_hash(&process) == Some(dylib_hash(&variants.v3)),
        "a layout-preserving pipeline was not adopted ahead of source-walk",
    );
    wait_long(
        || imported_value(&temp).as_deref() == Some("v3:note"),
        "the new importer version did not reimport the watched import",
    );
    // Let the watcher publish the reimport's own bundle write first.
    std::thread::sleep(SETTLED);
    assert_eq!(pipeline_generation(&process), generation + 1);
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;

    // Then source-walk's fast path refreshes only the pipeline crate's
    // source hash, to the adopted module's. Exactly one epoch change for
    // the edit.
    write_gate_schema(&temp, &variants.v3);
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(pipeline_generation(&process), generation + 1);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        before
    );
    assert_eq!(ready_dylib_hash(&process), Some(dylib_hash(&variants.v3)));
    assert_eq!(process.last_background_error(), None);

    // The observed schema still gates the next build: another code-only
    // change is adopted ahead of the walk as before.
    install_gate_pipeline(&temp, &variants.v2);
    wait_long(
        || ready_dylib_hash(&process) == Some(dylib_hash(&variants.v2)),
        "the next layout-preserving pipeline was not adopted",
    );
    wait_long(
        || imported_value(&temp).as_deref() == Some("v2:note"),
        "the next importer version did not reimport the watched import",
    );
    assert_eq!(pipeline_generation(&process), generation + 2);
    assert_eq!(process.last_background_error(), None);
}

#[test]
fn a_schema_refresh_observed_before_its_dylib_gives_one_epoch_change() {
    let variants = gate_variants();
    let temp = tempfile::tempdir().unwrap();
    let process = start_gate_daemon(&temp, &variants.v2);
    assert_eq!(imported_value(&temp).as_deref(), Some("v2:note"));
    std::thread::sleep(SETTLED);
    let generation = pipeline_generation(&process);
    let before = process
        .coordinator()
        .server()
        .current_stamp()
        .unwrap()
        .version;

    // The schema already names v3's source while v2 still serves: v2 would
    // load against it as it is, so nothing is published.
    write_gate_schema(&temp, &variants.v3);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(pipeline_generation(&process), generation);
    assert_eq!(
        process
            .coordinator()
            .server()
            .current_stamp()
            .unwrap()
            .version,
        before
    );
    assert_eq!(ready_dylib_hash(&process), Some(dylib_hash(&variants.v2)));
    assert_eq!(process.last_background_error(), None);

    // The dylib follows and matches the schema: one epoch change.
    install_gate_pipeline(&temp, &variants.v3);
    wait_long(
        || ready_dylib_hash(&process) == Some(dylib_hash(&variants.v3)),
        "the dylib matching the refreshed schema was not adopted",
    );
    wait_long(
        || imported_value(&temp).as_deref() == Some("v3:note"),
        "the new importer version did not reimport the watched import",
    );
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(pipeline_generation(&process), generation + 1);
    assert_eq!(process.last_background_error(), None);
}
