use std::collections::BTreeMap;
use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::bootstrap::{BootstrapControlSpecV1, BootstrapControlSymbol};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash};
use distill_daemon::config::DaemonConfig;
use distill_daemon::process::DaemonProcess;
use distill_daemon::quarantine::{QuarantineDriver, QuarantineRoot};
use distill_daemon::scanner::{DaemonOwnedDirectoryKind, ScanDiagnostic};
use distill_schema::ngp_schema::{LayoutIdentity, Schema, SchemaLayouts};
use distill_store::journal::{JournalIntentPlan, PublicationGroupKind};
use distill_store::state::{ConfigurationState, DscpV1, PipelineState};
use distill_store::Store;

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
lineage_manifest = {{ root = "main", path = "schema/lineage.bundle" }}
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
        types: Vec::new(),
        layouts: vec![SchemaLayouts {
            identity: test_layout_identity(),
            layouts: Vec::new(),
        }],
    };
    std::fs::write(path, serde_json::to_vec(&schema).unwrap()).unwrap();
}

fn write_empty_lineage_manifest(temp: &tempfile::TempDir) {
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::SchemaLineageManifest)
        .unwrap();
    let schema = distill_schema::ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
    let bytes = distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: BundleUuid([93; 16]),
        primary: None,
        schemas: BTreeMap::from([(row.logical_hash, schema)]),
        assets: BTreeMap::from([(
            "manifest".to_owned(),
            AssetEntry {
                uuid: AssetUuid([94; 16]),
                type_uuid: row.type_uuid,
                schema_hash: row.logical_hash,
                lineage: EntryLineageV1::Bootstrap {
                    bundle_format_version: 1,
                },
                authoring_only: true,
                data: distill_json::AuthoredValue::Object(BTreeMap::from([(
                    "types".to_owned(),
                    distill_json::AuthoredValue::Array(Vec::new()),
                )])),
            },
        )]),
    })
    .unwrap();
    let path = temp.path().join("assets/schema/lineage.bundle");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
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
    assert!(
        !temp.path().join("generated").exists(),
        "disabled codegen must not create its output directory"
    );
    drop(process);
}

#[test]
fn startup_recovers_non_codegen_publication_before_the_initial_scan() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp);
    let assets = std::fs::canonicalize(temp.path().join("assets")).unwrap();
    let target = assets.join("pending.txt");
    let proposal = assets.join(".pending.proposed");
    let conflict = assets.join(".pending.conflict");
    std::fs::write(&target, b"old").unwrap();
    let old_hash = ContentHash(*blake3::hash(b"old").as_bytes());
    let new_hash = ContentHash(*blake3::hash(b"new").as_bytes());
    {
        let mut store = Store::open(config.store_config()).unwrap();
        let group = store
            .record_publication_group(
                PublicationGroupKind::AuthoringWrite,
                b"interrupted authoring publication",
                &[JournalIntentPlan {
                    target_path: target.to_string_lossy().into_owned(),
                    temp_path: proposal.to_string_lossy().into_owned(),
                    conflict_path: conflict.to_string_lossy().into_owned(),
                    pre_image_hash: Some(old_hash),
                    proposed_hash: new_hash,
                }],
            )
            .unwrap();
        // The group is durable before the proposal temp exists.
        std::fs::write(&proposal, b"new").unwrap();
        store.arm_publication_group(group.group_id).unwrap();
    }

    let process = DaemonProcess::start(config).unwrap();

    assert_eq!(std::fs::read(&target).unwrap(), b"new");
    let store = process.coordinator().store();
    let store = store.lock().unwrap();
    assert!(store.unfinished_publication_groups().unwrap().is_empty());
    assert!(store
        .all_files()
        .unwrap()
        .iter()
        .any(|(_, path, state)| path == "pending.txt" && state.content_hash == Some(new_hash)));
    drop(store);
    drop(process);
}

#[test]
fn startup_surfaces_an_abandoned_unarmed_authoring_group() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp);
    let target = temp.path().join("assets/never-published.txt");
    let proposal = temp.path().join("assets/.never-published.proposed");
    let proposed_hash = ContentHash(*blake3::hash(b"new").as_bytes());
    {
        let mut store = Store::open(config.store_config()).unwrap();
        store
            .record_publication_group(
                PublicationGroupKind::AuthoringWrite,
                b"interrupted unarmed publication",
                &[JournalIntentPlan {
                    target_path: target.to_string_lossy().into_owned(),
                    temp_path: proposal.to_string_lossy().into_owned(),
                    conflict_path: temp
                        .path()
                        .join("assets/.never-published.conflict")
                        .to_string_lossy()
                        .into_owned(),
                    pre_image_hash: None,
                    proposed_hash,
                }],
            )
            .unwrap();
    }

    let process = DaemonProcess::start(config).unwrap();

    let diagnostic = process
        .last_background_error()
        .expect("material startup recovery is surfaced");
    assert!(diagnostic.contains("AuthoringWrite"));
    assert!(diagnostic.contains("never-published.txt"));
    assert!(diagnostic.contains("AbandonedUnarmed"));
    assert!(!target.exists());
    assert!(!proposal.exists());
    assert!(process
        .coordinator()
        .store()
        .lock()
        .unwrap()
        .unfinished_publication_groups()
        .unwrap()
        .is_empty());
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
        .lock()
        .unwrap()
        .all_files()
        .unwrap()
        .iter()
        .all(|(_, path, _)| !path.starts_with("generated-alias")));
    assert!(output.join("owned.rs").is_file());
}

#[test]
fn startup_runs_the_journaled_displacement_retention_sweep() {
    let temp = tempfile::tempdir().unwrap();
    let _ = config(&temp);
    let config_path = temp.path().join("distill.toml");
    let source = std::fs::read_to_string(&config_path).unwrap().replace(
        "address = \"127.0.0.1:0\"",
        "address = \"127.0.0.1:0\"\ndisplaced_retention_days = 0",
    );
    std::fs::write(&config_path, source).unwrap();
    let config = DaemonConfig::load(&config_path).unwrap();
    let assets = temp.path().join("assets");
    let quarantine = assets.join(".distill-displaced");
    let target = assets.join("old.asset");
    std::fs::write(&target, b"old").unwrap();
    let retained = {
        let mut store = Store::open(config.store_config()).unwrap();
        let driver = QuarantineDriver::new([QuarantineRoot::new(&assets, &quarantine)]).unwrap();
        let mut publication = driver.admit_publication(&mut store).unwrap();
        let group = publication
            .record_group(
                PublicationGroupKind::AuthoringWrite,
                b"retention test",
                &[JournalIntentPlan {
                    target_path: target.to_string_lossy().into_owned(),
                    temp_path: String::new(),
                    conflict_path: assets.join("old.conflict").to_string_lossy().into_owned(),
                    pre_image_hash: Some(ContentHash(*blake3::hash(b"old").as_bytes())),
                    proposed_hash: ContentHash(*blake3::hash(b"").as_bytes()),
                }],
            )
            .unwrap();
        publication.arm_group(group.group_id).unwrap();
        publication
            .resume_group_delete(group.child_intents[0], &target)
            .unwrap();
        publication.retire_group(group.group_id).unwrap();
        drop(publication);
        store
            .quarantined_entries()
            .unwrap()
            .into_iter()
            .find(|entry| entry.intent_id == group.child_intents[0])
            .unwrap()
            .path
    };
    std::thread::sleep(Duration::from_millis(1_100));

    let process = DaemonProcess::start(config).unwrap();

    assert!(!retained.exists());
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
            if let Some(error) = process.last_background_error() {
                panic!("background reconciliation failed: {error}");
            }
            let version = process.coordinator().server().current_stamp().version;
            let state = process
                .coordinator()
                .store()
                .lock()
                .unwrap()
                .configuration_state()
                .unwrap();
            version > poisoned
                && matches!(
                    state,
                    ConfigurationState::Poisoned { reason, .. }
                        if matches!(reason.detail.as_ref(), DscpV1::MissingLineageManifest)
                )
        },
        "valid configuration did not clear its malformed-source poison",
    );
}

#[test]
fn valid_configuration_with_malformed_schema_retains_lineage_configuration_poison() {
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

    std::fs::write(temp.path().join("schema.json"), b"not-json").unwrap();
    std::fs::write(&config_path, config_source(&temp)).unwrap();
    wait_until(
        || {
            let store = process.coordinator().store();
            let store = store.lock().unwrap();
            matches!(
                store.configuration_state().unwrap(),
                ConfigurationState::Poisoned { reason, .. }
                    if matches!(reason.detail.as_ref(), DscpV1::MissingLineageManifest)
            ) && matches!(
                store.pipeline_state().unwrap(),
                Some(PipelineState::Poisoned { error, .. })
                    if error.message.contains("schema authority")
            )
        },
        "valid configuration erased independent lineage configuration poison",
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
                    .lock()
                    .unwrap()
                    .configuration_state()
                    .unwrap(),
                ConfigurationState::Poisoned { reason, .. }
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
fn schema_bound_target_mismatches_publish_configuration_poison() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp);
    write_empty_lineage_manifest(&temp);
    let process = DaemonProcess::start(config).unwrap();
    let source = config_source(&temp).replace("os = \"macos\"", "os = \"linux\"");
    std::fs::write(temp.path().join("distill.toml"), source).unwrap();

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
