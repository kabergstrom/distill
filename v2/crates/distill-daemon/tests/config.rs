use distill_core::attestation::CompiledTypeTable;
use distill_daemon::config::{DaemonConfig, DaemonConfigError};
use distill_schema::bootstrap_gen_v1::consumer_compilation_identity_v1;

fn compiled_table() -> CompiledTypeTable {
    CompiledTypeTable::canonical(Vec::new()).unwrap()
}

fn valid_config(temp: &tempfile::TempDir) -> String {
    std::fs::create_dir_all(temp.path().join("assets")).unwrap();
    format!(
        r#"
[daemon]
address = "127.0.0.1:0"
state_path = "{}"
displaced_retention_days = 7

[assets]
roots = {{ main = "{}" }}
schema_path = "{}"
lineage_manifest = {{ root = "main", path = "schema/schema-lineage.bundle" }}

[modules]
pipeline_dylib = "{}"

[targets.dev]
os = "macos"
arch = "aarch64"
apis = ["vulkan"]
optimize = false
debug_info = true

[codegen]
rs_mod_path = "{}"
auto_codegen = true

[pipeline]
parallelism = 8
max_dependency_depth = 256
batch_reserved_workers = 1

[cas]
segment_size = "256MiB"
cache_limit = "20GiB"
"#,
        temp.path().join("state").display(),
        temp.path().join("assets").display(),
        temp.path().join("schema.json").display(),
        temp.path().join("pipeline.dylib").display(),
        temp.path().join("generated").display(),
    )
}

#[test]
fn parses_and_validates_the_complete_configuration_surface() {
    let temp = tempfile::tempdir().unwrap();
    let config =
        DaemonConfig::parse(temp.path().join("distill.toml"), &valid_config(&temp)).unwrap();
    assert!(config.daemon.address.ip().is_loopback());
    assert_eq!(config.assets.roots.len(), 1);
    assert_eq!(config.store_config().segment_size, 256 * 1024 * 1024);
    assert_eq!(config.store_config().cache_limit, 20 * 1024 * 1024 * 1024);
    let table = compiled_table();
    let definitions = config
        .target_definitions(&table, consumer_compilation_identity_v1())
        .unwrap();
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name(), "dev");
}

#[test]
fn rejects_unknown_keys_nonloopback_and_invalid_scheduler_bounds() {
    let temp = tempfile::tempdir().unwrap();
    let source = valid_config(&temp).replace(
        "displaced_retention_days = 7",
        "displaced_retention_days = 7\nunknown = true",
    );
    assert!(matches!(
        DaemonConfig::parse(temp.path().join("distill.toml"), &source),
        Err(DaemonConfigError::Toml(_))
    ));

    let source = valid_config(&temp).replace("127.0.0.1:0", "192.0.2.1:9999");
    assert!(matches!(
        DaemonConfig::parse(temp.path().join("distill.toml"), &source),
        Err(DaemonConfigError::NonLoopbackAddress(_))
    ));

    let source =
        valid_config(&temp).replace("batch_reserved_workers = 1", "batch_reserved_workers = 8");
    assert!(matches!(
        DaemonConfig::parse(temp.path().join("distill.toml"), &source),
        Err(DaemonConfigError::Scheduler(_))
    ));
}

#[test]
fn rejects_empty_target_apis_unknown_lineage_root_and_watched_output_nesting() {
    let temp = tempfile::tempdir().unwrap();
    let source = valid_config(&temp).replace("apis = [\"vulkan\"]", "apis = []");
    assert!(matches!(
        DaemonConfig::parse(temp.path().join("distill.toml"), &source),
        Err(DaemonConfigError::EmptyTargetApis(name)) if name == "dev"
    ));

    let source = valid_config(&temp).replace(
        "lineage_manifest = { root = \"main\"",
        "lineage_manifest = { root = \"missing\"",
    );
    assert!(matches!(
        DaemonConfig::parse(temp.path().join("distill.toml"), &source),
        Err(DaemonConfigError::UnknownLineageRoot(name)) if name == "missing"
    ));

    let source = valid_config(&temp).replace(
        &temp.path().join("generated").display().to_string(),
        &temp.path().join("assets/generated").display().to_string(),
    );
    let result = DaemonConfig::parse(temp.path().join("distill.toml"), &source);
    assert!(
        matches!(result, Err(DaemonConfigError::PathOverlap { .. })),
        "unexpected result: {result:?}"
    );
}

#[test]
fn target_definition_hash_changes_for_a_bound_target_edit() {
    let temp = tempfile::tempdir().unwrap();
    let a = DaemonConfig::parse(temp.path().join("distill.toml"), &valid_config(&temp)).unwrap();
    let b = DaemonConfig::parse(
        temp.path().join("distill.toml"),
        &valid_config(&temp).replace("optimize = false", "optimize = true"),
    )
    .unwrap();
    let table = compiled_table();
    assert_ne!(
        a.target_definitions(&table, consumer_compilation_identity_v1())
            .unwrap()[0]
            .definition_hash(),
        b.target_definitions(&table, consumer_compilation_identity_v1())
            .unwrap()[0]
            .definition_hash()
    );
}

#[test]
fn targets_bind_layout_identity_while_module_requirements_bind_source_identity() {
    let temp = tempfile::tempdir().unwrap();
    let config =
        DaemonConfig::parse(temp.path().join("distill.toml"), &valid_config(&temp)).unwrap();
    let table = compiled_table();
    let first = consumer_compilation_identity_v1().clone();
    let mut second = first.clone();
    second.algorithm_version += 1;

    let first_target = config.target_definitions(&table, &first).unwrap();
    let second_target = config.target_definitions(&table, &second).unwrap();
    assert_ne!(
        first_target[0].definition_hash(),
        second_target[0].definition_hash()
    );
    let source_hashes =
        std::collections::BTreeMap::from([("pipeline".to_owned(), "0123456789abcdef".to_owned())]);
    let first_requirements = config
        .candidate_requirements(&table, &first, &source_hashes)
        .unwrap();
    let second_requirements = config
        .candidate_requirements(&table, &second, &source_hashes)
        .unwrap();
    assert_eq!(
        first_requirements.module_abi,
        second_requirements.module_abi
    );
    assert_eq!(second_requirements.source_hashes, source_hashes);
}

#[test]
fn rejects_target_without_an_exact_schema_compilation_layout() {
    let temp = tempfile::tempdir().unwrap();
    let source = valid_config(&temp)
        .replace("os = \"macos\"", "os = \"linux\"")
        .replace("arch = \"aarch64\"", "arch = \"x86_64\"");
    let config = DaemonConfig::parse(temp.path().join("distill.toml"), &source).unwrap();
    let table = compiled_table();

    let error = config
        .candidate_requirements(
            &table,
            consumer_compilation_identity_v1(),
            &std::collections::BTreeMap::new(),
        )
        .unwrap_err();

    assert!(matches!(error, DaemonConfigError::Target(_)));
}
