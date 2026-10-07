use std::collections::BTreeMap;
use std::sync::Arc;

use distill_bundle::{AssetEntry, Bundle};
use distill_core::bootstrap::{
    BootstrapControlSpecV1, BootstrapControlSymbol, DIRECTORY_IMPORT_RULES_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_daemon::coordinator::DaemonCoordinator;
use distill_daemon::scanner::AssetRoot;
use distill_daemon::watcher::WatcherBatch;
use distill_json::AuthoredValue;
use distill_pipeline_fixture::{
    default_settings, settings, value as byte, value_of as byte_of, BYTE_IMPORTER, CHAIN_IMPORTER,
    SETTINGS_IMPORTER, FLOAT_IMPORTER, OPTIONAL_IMPORTER, REQUIRE_IMPORTER, float_settings,
};
use distill_rpc::{
    AuthoringValue, ImportRequest, InputVersion, TargetDefinition, TargetDefinitionHash,
};
use distill_schema::ngp_schema::LogicalSchema;
use distill_store::StoreConfig;

/// The output type of the pipeline fixture's importers, a project type of
/// the shared test configuration: a struct of one `u8` field `value`.
const TYPE_UUID: TypeUuid = distill_test_project::VALUE_TYPE;

fn ordinary_bundle() -> (Vec<u8>, LogicalSchema, distill_core::id::LogicalHash) {
    let authority = distill_test_project::project_authority();
    let project = authority.project_type(TYPE_UUID).unwrap();
    let schema = project.logical_schema.clone();
    let schema_hash = project.logical_hash;
    let entry = AssetEntry {
        uuid: AssetUuid([72; 16]),
        type_uuid: TYPE_UUID,
        schema_hash,
        authoring_only: false,
        data: byte(1),
    };
    (
        distill_bundle::write_bundle(&Bundle {
            format_version: 1,
            uuid: BundleUuid([73; 16]),
            primary: Some("entry".into()),
            schemas: BTreeMap::from([(schema_hash, schema.clone())]),
            assets: BTreeMap::from([("entry".into(), entry)]),
        })
        .unwrap(),
        schema,
        schema_hash,
    )
}

fn target() -> TargetDefinition {
    TargetDefinition::new("dev", TargetDefinitionHash([4; 32]))
}

/// Configure the project at `dir`, whose root "main" is `root`, with the
/// shared test configuration (whose schema has the project type
/// `TYPE_UUID`), and publish it as the daemon's process loop does:
/// importer outputs are written at that schema.
fn configure(coordinator: &DaemonCoordinator, dir: &std::path::Path, root: &std::path::Path) {
    let config = distill_test_project::write_configuration(dir, root, false);
    let mut writer = coordinator.open_writer().unwrap();
    distill_test_project::publish_configuration(coordinator, &mut writer, &config);
}

/// A hub bound to the target the daemon serves.
fn connect(coordinator: &DaemonCoordinator) -> distill_rpc::Hub {
    let target = coordinator
        .open_reader()
        .unwrap()
        .rpc_targets()
        .unwrap()
        .remove(0);
    match coordinator
        .server()
        .root()
        .connect(distill_rpc::ConnectRequest::new(
            &target.name,
            TargetDefinitionHash(target.definition_hash),
        )) {
        distill_rpc::ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("expected connection, got {other:?}"),
    }
}

/// Import `dest` from `sources` with `importer` at its default settings,
/// watched, through the RPC hub as a client does, at the current version;
/// returns the imported bundle.
fn import(
    coordinator: &DaemonCoordinator,
    importer: &str,
    sources: &[&str],
    dest: &str,
) -> BundleUuid {
    import_with(coordinator, importer, sources, dest, &default_settings())
}

/// [`import`] with the importer settings `settings`.
fn import_with(
    coordinator: &DaemonCoordinator,
    importer: &str,
    sources: &[&str],
    dest: &str,
    settings: &AuthoredValue,
) -> BundleUuid {
    let base = coordinator.server().current_stamp().unwrap().version;
    let request = ImportRequest {
        importer: importer.into(),
        sources: sources.iter().map(|source| (*source).to_owned()).collect(),
        dest: dest.into(),
        settings: AuthoringValue {
            canonical_value: Arc::from(distill_json::write(settings).unwrap().into_bytes()),
            blobs: Vec::new(),
        },
        watch: true,
        root: "main".into(),
        if_changed: false,
    };
    match connect(coordinator).import(base, request) {
        distill_rpc::RpcResult::Success(bundle) => bundle,
        other => panic!("expected an import, got {other:?}"),
    }
}

/// The watched-import failures a runtime client polls, as (path, message).
fn import_failures(coordinator: &DaemonCoordinator) -> Vec<(String, String)> {
    let snapshot = match connect(coordinator).snapshot() {
        distill_rpc::RpcResult::Success(snapshot) => snapshot,
        other => panic!("expected a snapshot, got {other:?}"),
    };
    match snapshot.import_failures() {
        distill_rpc::RpcResult::Success(failures) => failures
            .into_iter()
            .map(|failure| {
                assert_eq!(failure.root, "main");
                (failure.path, failure.message)
            })
            .collect(),
        other => panic!("expected import failures, got {other:?}"),
    }
}

fn object<const N: usize>(fields: [(&str, AuthoredValue); N]) -> AuthoredValue {
    AuthoredValue::Object(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

fn bytes(value: [u8; 16]) -> AuthoredValue {
    AuthoredValue::Array(
        value
            .into_iter()
            .map(|value| AuthoredValue::UInt(value.into()))
            .collect(),
    )
}

/// `value` as a rules asset spells an importer's settings: a self-describing
/// `AuthoredValueV1` enum, each variant's payload `{ value }`.
fn wrapped(value: &AuthoredValue) -> AuthoredValue {
    let (variant, payload) = match value {
        AuthoredValue::Null => return object([("Null", object([]))]),
        AuthoredValue::Bool(_) => ("Bool", value.clone()),
        AuthoredValue::Int(_) => ("Int", value.clone()),
        AuthoredValue::UInt(_) => ("UInt", value.clone()),
        AuthoredValue::Float(_) => ("Float", value.clone()),
        AuthoredValue::Str(_) => ("Str", value.clone()),
        AuthoredValue::Blob(_) => ("Blob", value.clone()),
        AuthoredValue::Array(items) => (
            "Array",
            AuthoredValue::Array(items.iter().map(wrapped).collect()),
        ),
        AuthoredValue::Object(fields) => (
            "Object",
            AuthoredValue::Object(
                fields
                    .iter()
                    .map(|(name, value)| (name.clone(), wrapped(value)))
                    .collect(),
            ),
        ),
    };
    object([(variant, object([("value", payload)]))])
}

/// A rules bundle whose one rule imports each `*.src` file with
/// [`BYTE_IMPORTER`] at its default settings.
fn directory_rules_bundle() -> Vec<u8> {
    directory_rules_bundle_with(true, &default_settings())
}

fn directory_rules_bundle_with_rule(include_rule: bool) -> Vec<u8> {
    directory_rules_bundle_with(include_rule, &default_settings())
}

/// A rules bundle whose one rule imports with [`BYTE_IMPORTER`] at
/// `settings`: each `*.src` file when `include_rule`, else only `*.other`
/// files, under another rule id.
fn directory_rules_bundle_with(include_rule: bool, settings: &AuthoredValue) -> Vec<u8> {
    directory_rules_bundle_for(include_rule, BYTE_IMPORTER, settings)
}

/// [`directory_rules_bundle_with`], its rule importing with `importer`.
fn directory_rules_bundle_for(
    include_rule: bool,
    importer: &str,
    settings: &AuthoredValue,
) -> Vec<u8> {
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::DirectoryImportRules)
        .unwrap();
    let schema = distill_schema::ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
    let query = || {
        object([
            ("path_glob", AuthoredValue::Str("*.src".into())),
            ("path_prefix", AuthoredValue::Null),
        ])
    };
    let matches = if include_rule {
        query()
    } else {
        object([
            ("path_glob", AuthoredValue::Str("*.other".into())),
            ("path_prefix", AuthoredValue::Null),
        ])
    };
    let rules = vec![object([
        ("group", object([("PerFile", object([]))])),
        ("id", bytes(if include_rule { [97; 16] } else { [100; 16] })),
        ("importer", AuthoredValue::Str(importer.into())),
        ("matches", matches),
        ("output", AuthoredValue::Str("{stem}.bundle".into())),
        ("settings", wrapped(settings)),
    ])];
    let data = object([("listing", query()), ("rules", AuthoredValue::Array(rules))]);
    distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: BundleUuid([98; 16]),
        primary: None,
        schemas: BTreeMap::from([(row.logical_hash, schema)]),
        assets: BTreeMap::from([(
            "rules".into(),
            AssetEntry {
                uuid: AssetUuid([99; 16]),
                type_uuid: DIRECTORY_IMPORT_RULES_TYPE_UUID,
                schema_hash: row.logical_hash,
                authoring_only: true,
                data,
            },
        )]),
    })
    .unwrap()
}

#[test]
fn explicit_import_and_reimport_publish_controls_read_set_and_stable_identities() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, _) = ordinary_bundle();
    std::fs::write(assets.join("ordinary.bundle"), ordinary).unwrap();
    std::fs::write(assets.join("source.txt"), b"7").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();

    configure(&coordinator, temp.path(), &assets);

    let imported_bundle = import(
        &coordinator,
        BYTE_IMPORTER,
        &["source.txt"],
        "imported.bundle",
    );
    let path = assets.join("imported.bundle");
    let first = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    let first_asset = first.assets["asset"].uuid;
    assert_eq!(first.assets["asset"].data, byte(7));
    assert_eq!(first.assets["$settings"].data, default_settings());
    assert!(first.assets.contains_key("$record"));
    assert!(coordinator
        .authoring_service()
        .watched_imports_needing_reimport(&mut writer)
        .unwrap()
        .is_empty());

    std::fs::write(assets.join("source.txt"), b"8").unwrap();
    assert_eq!(
        coordinator
            .authoring_service()
            .watched_imports_needing_reimport(&mut writer)
            .unwrap(),
        vec![imported_bundle]
    );
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![event_root(&assets).join("source.txt")],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(
        coordinator.reconcile_watched_imports(&mut writer).unwrap(),
        vec![imported_bundle]
    );
    let second = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(second.uuid, imported_bundle);
    assert_eq!(second.assets["asset"].uuid, first_asset);
    assert_eq!(second.assets["asset"].data, byte(8));
    assert_eq!(second.assets["$settings"].data, default_settings());
    assert_eq!(
        coordinator.open_reader().unwrap().input_version().unwrap(),
        InputVersion(5)
    );

    std::fs::write(assets.join("source.txt"), b"not-a-byte").unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![event_root(&assets).join("source.txt")],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert!(coordinator
        .reconcile_watched_imports(&mut writer)
        .unwrap()
        .is_empty());
    let failed = coordinator
        .open_reader()
        .unwrap()
        .watched_import_failure(imported_bundle)
        .unwrap()
        .expect("stable failed attempt is retained");
    assert_eq!(
        failed.terminal,
        distill_store::imports::WatchedImportTerminal::Importer { code: 3 }
    );
    assert_eq!(
        coordinator.open_reader().unwrap().input_version().unwrap(),
        InputVersion(6),
        "memoizing a failure is not an input event"
    );
    let failures = import_failures(&coordinator);
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, "imported.bundle");
    assert!(failures[0].1.contains("invalid digit"), "{failures:?}");
    let failed_memo = failed.memo_seq;
    assert!(coordinator
        .authoring_service()
        .watched_imports_needing_reimport(&mut writer)
        .unwrap()
        .is_empty());
    assert!(coordinator
        .reconcile_watched_imports(&mut writer)
        .unwrap()
        .is_empty());
    assert_eq!(
        coordinator.open_reader().unwrap().memo_seq().unwrap(),
        failed_memo,
        "an unchanged failure must not spin"
    );

    std::fs::write(assets.join("source.txt"), b"9").unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![event_root(&assets).join("source.txt")],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(
        coordinator.reconcile_watched_imports(&mut writer).unwrap(),
        vec![imported_bundle]
    );
    assert!(coordinator
        .open_reader()
        .unwrap()
        .watched_import_failure(imported_bundle)
        .unwrap()
        .is_none());
    let healed = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(healed.assets["asset"].data, byte(9));
    assert!(import_failures(&coordinator).is_empty());
    assert_eq!(
        coordinator.open_reader().unwrap().input_version().unwrap(),
        InputVersion(8)
    );

    std::fs::remove_file(assets.join("source.txt")).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![event_root(&assets).join("source.txt")],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert!(coordinator
        .reconcile_watched_imports(&mut writer)
        .unwrap()
        .is_empty());
    assert_eq!(
        coordinator
            .open_reader()
            .unwrap()
            .watched_import_failure(imported_bundle)
            .unwrap()
            .unwrap()
            .terminal,
        distill_store::imports::WatchedImportTerminal::Dependency
    );
    assert!(coordinator
        .authoring_service()
        .watched_imports_needing_reimport(&mut writer)
        .unwrap()
        .is_empty());
    std::fs::write(assets.join("source.txt"), b"10").unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![event_root(&assets).join("source.txt")],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(
        coordinator.reconcile_watched_imports(&mut writer).unwrap(),
        vec![imported_bundle]
    );
    let healed = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(healed.assets["asset"].data, byte(10));
    assert_eq!(
        coordinator.open_reader().unwrap().input_version().unwrap(),
        InputVersion(11)
    );
}

#[test]
fn directory_rules_publish_owned_bundles_and_listing_loss_only_orphans_them() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, _) = ordinary_bundle();
    std::fs::write(assets.join("ordinary.bundle"), ordinary).unwrap();
    std::fs::write(assets.join("rules.bundle"), directory_rules_bundle()).unwrap();
    std::fs::write(assets.join("foo.src"), b"9").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);

    let imported = coordinator
        .reconcile_directory_imports(&mut writer)
        .unwrap();
    assert_eq!(imported.len(), 1);
    let generated_path = assets.join("foo.bundle");
    let generated = distill_bundle::parse_bundle(&std::fs::read(&generated_path).unwrap()).unwrap();
    assert_eq!(generated.assets["asset"].data, byte(9));
    assert_eq!(generated.assets["$settings"].data, default_settings());
    let meta = coordinator
        .open_reader()
        .unwrap()
        .bundle(generated.uuid)
        .unwrap()
        .unwrap();
    let origin = meta.origin.unwrap();
    assert_eq!(origin.rules_bundle, BundleUuid([98; 16]));
    assert_eq!(origin.rule.0, [97; 16]);
    assert_eq!(origin.group_root, "main");
    assert_eq!(origin.group_path, "foo.src");
    assert!(coordinator
        .reconcile_directory_imports(&mut writer)
        .unwrap()
        .is_empty());

    std::fs::remove_file(assets.join("foo.src")).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![event_root(&assets).join("foo.src")],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let work = coordinator.pending_file_work(&mut writer).unwrap();
    assert!(coordinator
        .reconcile_directory_imports_affected(&mut writer, &work, false)
        .unwrap()
        .is_empty());
    coordinator
        .acknowledge_file_work(&mut writer, &work)
        .unwrap();
    let failure = coordinator
        .open_reader()
        .unwrap()
        .watched_import_failure(generated.uuid)
        .unwrap()
        .expect("listing loss is durable orphan state");
    assert_eq!(
        failure.terminal,
        distill_store::imports::WatchedImportTerminal::DirectoryOrphan
    );
    let orphan_memo = failure.memo_seq;
    assert!(coordinator
        .reconcile_directory_imports(&mut writer)
        .unwrap()
        .is_empty());
    assert_eq!(
        coordinator.open_reader().unwrap().memo_seq().unwrap(),
        orphan_memo,
        "an unchanged orphan must not spin memo state"
    );
    assert!(
        generated_path.exists(),
        "lost groups are orphaned, never deleted"
    );

    std::fs::write(assets.join("foo.src"), b"9").unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    coordinator
        .reconcile_directory_imports(&mut writer)
        .unwrap();
    assert!(coordinator
        .open_reader()
        .unwrap()
        .watched_import_failure(generated.uuid)
        .unwrap()
        .is_none());

    std::fs::write(
        assets.join("rules.bundle"),
        directory_rules_bundle_with_rule(false),
    )
    .unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert!(coordinator
        .reconcile_directory_imports(&mut writer)
        .unwrap()
        .is_empty());
    assert_eq!(
        coordinator
            .open_reader()
            .unwrap()
            .watched_import_failure(generated.uuid)
            .unwrap()
            .unwrap()
            .terminal,
        distill_store::imports::WatchedImportTerminal::DirectoryOrphan,
        "deleting a stable rule id orphans its existing outputs"
    );

    std::fs::write(assets.join("rules.bundle"), directory_rules_bundle()).unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    coordinator
        .reconcile_directory_imports(&mut writer)
        .unwrap();
    assert!(coordinator
        .open_reader()
        .unwrap()
        .watched_import_failure(generated.uuid)
        .unwrap()
        .is_none());
}

/// A rule's settings are any authored value: an object holding an array
/// and a nested object reaches the importer as authored, end to end from
/// the rules file to the generated bundle's `$settings`.
#[test]
fn directory_rule_settings_holding_nested_values_reach_the_importer() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let rule_settings = settings([2, 3], 2);
    std::fs::write(
        assets.join("rules.bundle"),
        directory_rules_bundle_with(true, &rule_settings),
    )
    .unwrap();
    std::fs::write(assets.join("foo.src"), b"4").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);

    assert_eq!(
        coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .len(),
        1
    );
    let generated =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("foo.bundle")).unwrap()).unwrap();
    assert_eq!(generated.assets["$settings"].data, rule_settings);
    assert_eq!(generated.assets["asset"].data, byte((4 + 2 + 3) * 2));
}

/// An explicit import's settings reach the importer and its `$settings`.
#[test]
fn explicit_import_settings_reach_the_importer() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(assets.join("source.txt"), b"4").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);

    let import_settings = settings([1, 0], 3);
    import_with(
        &coordinator,
        BYTE_IMPORTER,
        &["source.txt"],
        "imported.bundle",
        &import_settings,
    );
    let imported =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("imported.bundle")).unwrap())
            .unwrap();
    assert_eq!(imported.assets["$settings"].data, import_settings);
    assert_eq!(imported.assets["asset"].data, byte((4 + 1) * 3));
}

/// Explicit import settings that leave out fields take the importer's
/// defaults, a nested struct field by field; `$settings` records the whole
/// value the importer ran with.
#[test]
fn explicit_import_settings_leaving_out_fields_take_the_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(assets.join("empty.txt"), b"4").unwrap();
    std::fs::write(assets.join("partial.txt"), b"4").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);

    import_with(
        &coordinator,
        BYTE_IMPORTER,
        &["empty.txt"],
        "empty.bundle",
        &object([]),
    );
    let partial = object([("scale", object([]))]);
    import_with(
        &coordinator,
        BYTE_IMPORTER,
        &["partial.txt"],
        "partial.bundle",
        &partial,
    );
    for dest in ["empty.bundle", "partial.bundle"] {
        let imported =
            distill_bundle::parse_bundle(&std::fs::read(assets.join(dest)).unwrap()).unwrap();
        assert_eq!(imported.assets["$settings"].data, default_settings());
        assert_eq!(imported.assets["asset"].data, byte(4));
    }
}

/// A directory rule's settings that leave out fields take the importer's
/// defaults, so a rule names only what it changes.
#[test]
fn directory_rule_settings_leaving_out_fields_take_the_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let rule_settings = object([("scale", object([("by", AuthoredValue::UInt(2))]))]);
    std::fs::write(
        assets.join("rules.bundle"),
        directory_rules_bundle_with(true, &rule_settings),
    )
    .unwrap();
    std::fs::write(assets.join("foo.src"), b"4").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);

    assert_eq!(
        coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .len(),
        1
    );
    let generated =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("foo.bundle")).unwrap()).unwrap();
    assert_eq!(generated.assets["$settings"].data, settings([0, 0], 2));
    assert_eq!(generated.assets["asset"].data, byte(4 * 2));
    // Unchanged rules and sources: the completed settings match what the
    // output recorded, so nothing reimports.
    assert!(coordinator
        .reconcile_directory_imports(&mut writer)
        .unwrap()
        .is_empty());
}

/// A directory rule importing with `rule_settings` through
/// [`FLOAT_IMPORTER`], whose settings hold an integral float (canonical
/// JSON writes `30.0` as `30`, which the output's `$settings` read back as
/// an integer): once its output settled, a quiet daemon (a full rescan and
/// the generated bundle's own watcher pass) imports nothing, leaves the
/// output unwritten and publishes no new version.
fn assert_integral_float_settings_stay_quiet(rule_settings: &AuthoredValue) {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("rules.bundle"),
        directory_rules_bundle_for(true, FLOAT_IMPORTER, rule_settings),
    )
    .unwrap();
    std::fs::write(assets.join("foo.src"), b"4").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    assert_eq!(
        coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .len(),
        1
    );
    let generated = assets.join("foo.bundle");
    assert_eq!(
        distill_bundle::parse_bundle(&std::fs::read(&generated).unwrap())
            .unwrap()
            .assets["asset"]
            .data,
        byte(4)
    );
    coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["foo.bundle"]), false)
        .unwrap();
    let version = writer.input_version().unwrap();
    let written = std::fs::metadata(&generated).unwrap().modified().unwrap();

    for _ in 0..3 {
        coordinator.reconcile_full_scan(&mut writer).unwrap();
        assert!(coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .is_empty());
        coordinator
            .reconcile_batch(&mut writer, &batch(&assets, &["foo.bundle"]), false)
            .unwrap();
    }
    assert_eq!(writer.input_version().unwrap(), version);
    assert_eq!(
        std::fs::metadata(&generated).unwrap().modified().unwrap(),
        written
    );
}

/// [`assert_integral_float_settings_stay_quiet`], the rule leaving `rate`
/// to the importer's default `30.0`.
#[test]
fn a_rule_at_integral_float_defaults_leaves_a_quiet_daemon_quiet() {
    assert_integral_float_settings_stay_quiet(&object([]));
}

/// [`assert_integral_float_settings_stay_quiet`], the rule authoring `rate`
/// `30.0` itself.
#[test]
fn a_rule_authoring_an_integral_float_leaves_a_quiet_daemon_quiet() {
    assert_integral_float_settings_stay_quiet(&float_settings(30.0));
}

/// An [`OPTIONAL_IMPORTER`] `skeleton` value: `{ path, name }`, `name`
/// left out when `None` is given.
fn skeleton(path: Option<&str>, name: Option<Option<&str>>) -> AuthoredValue {
    let mut fields = BTreeMap::new();
    if let Some(path) = path {
        fields.insert("path".to_owned(), AuthoredValue::Str(path.into()));
    }
    if let Some(name) = name {
        fields.insert(
            "name".to_owned(),
            name.map_or(AuthoredValue::Null, |name| AuthoredValue::Str(name.into())),
        );
    }
    object([("skeleton", AuthoredValue::Object(fields))])
}

/// A directory rule through [`OPTIONAL_IMPORTER`] with `rule_settings`,
/// over one source `foo.src`: the coordinator after reconciling the scan,
/// and the asset root.
fn optional_rule_project(
    temp: &tempfile::TempDir,
    rule_settings: &AuthoredValue,
) -> (DaemonCoordinator, distill_store::StoreWriter, std::path::PathBuf) {
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("rules.bundle"),
        directory_rules_bundle_for(true, OPTIONAL_IMPORTER, rule_settings),
    )
    .unwrap();
    std::fs::write(assets.join("foo.src"), b"4").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    (coordinator, writer, assets)
}

/// A rule's object for an `Option` struct whose default is `None` (doc 21
/// §4.2, sparse settings): the fields it leaves out that are `Option`s are
/// `None`, so `{ "path": … }` imports as `{ "path": …, "name": null }`, and
/// an unchanged rule then matches what its output recorded: a quiet daemon
/// stays quiet.
#[test]
fn a_rule_object_over_a_none_default_leaves_absent_option_fields_none() {
    let temp = tempfile::tempdir().unwrap();
    let (coordinator, mut writer, assets) =
        optional_rule_project(&temp, &skeleton(Some("rig.gltf"), None));
    assert_eq!(
        coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .len(),
        1
    );
    let generated = assets.join("foo.bundle");
    let bundle = distill_bundle::parse_bundle(&std::fs::read(&generated).unwrap()).unwrap();
    assert_eq!(
        bundle.assets["$settings"].data,
        skeleton(Some("rig.gltf"), Some(None))
    );
    assert_eq!(bundle.assets["asset"].data, byte(4));
    coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["foo.bundle"]), false)
        .unwrap();
    let version = writer.input_version().unwrap();
    let written = std::fs::metadata(&generated).unwrap().modified().unwrap();
    for _ in 0..3 {
        coordinator.reconcile_full_scan(&mut writer).unwrap();
        assert!(coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .is_empty());
        coordinator
            .reconcile_batch(&mut writer, &batch(&assets, &["foo.bundle"]), false)
            .unwrap();
    }
    assert_eq!(writer.input_version().unwrap(), version);
    assert_eq!(
        std::fs::metadata(&generated).unwrap().modified().unwrap(),
        written
    );
}

/// The same rule spelling `name` whole imports the same way: a full
/// object is taken as authored.
#[test]
fn a_rule_spelling_the_whole_option_struct_imports_it_as_authored() {
    for name in [None, Some("Armature")] {
        let temp = tempfile::tempdir().unwrap();
        let rule_settings = skeleton(Some("rig.gltf"), Some(name));
        let (coordinator, mut writer, assets) = optional_rule_project(&temp, &rule_settings);
        assert_eq!(
            coordinator
                .reconcile_directory_imports(&mut writer)
                .unwrap()
                .len(),
            1
        );
        let bundle =
            distill_bundle::parse_bundle(&std::fs::read(assets.join("foo.bundle")).unwrap())
                .unwrap();
        assert_eq!(bundle.assets["$settings"].data, rule_settings);
        assert!(coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .is_empty());
    }
}

/// A rule's object over a `None` default that leaves out a required field
/// does not import: the error names the field, its path in the settings,
/// the importer and the rule.
#[test]
fn a_rule_object_over_a_none_default_missing_a_required_field_names_it() {
    let temp = tempfile::tempdir().unwrap();
    let (coordinator, mut writer, assets) =
        optional_rule_project(&temp, &skeleton(None, Some(Some("Armature"))));
    let error = coordinator
        .reconcile_directory_imports(&mut writer)
        .unwrap_err()
        .to_string()
        // The detail arrives Debug-quoted, once per wrapping.
        .replace('\\', "");
    for part in [
        r#"missing struct field "path""#,
        "data.skeleton",
        OPTIONAL_IMPORTER,
        "directory rule",
    ] {
        assert!(error.contains(part), "{part:?} not in {error:?}");
    }
    assert!(!assets.join("foo.bundle").exists());
}

/// Editing a rules source's settings while the daemon watches re-imports
/// what the rule generated with the new settings, through the watcher
/// pass the daemon runs (`reconcile_batch`).
#[test]
fn editing_a_rules_settings_reimports_its_outputs_in_a_watcher_pass() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(assets.join("rules.bundle"), directory_rules_bundle_with(true, &settings([0, 0], 2))).unwrap();
    std::fs::write(assets.join("foo.src"), b"4").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    coordinator.reconcile_directory_imports(&mut writer).unwrap();
    let read = || distill_bundle::parse_bundle(&std::fs::read(assets.join("foo.bundle")).unwrap()).unwrap();
    assert_eq!(read().assets["asset"].data, byte(8));
    let generated = identities(&assets.join("foo.bundle"));
    // The generated bundle's publication settles first, as in the daemon.
    coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["foo.bundle"]), false)
        .unwrap();

    std::fs::write(assets.join("rules.bundle"), directory_rules_bundle_with(true, &settings([0, 0], 3))).unwrap();
    coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["rules.bundle"]), false)
        .unwrap();
    assert_eq!(read().assets["asset"].data, byte(12));
    // The regenerated output folds over its prior: every UUID is kept.
    assert_eq!(identities(&assets.join("foo.bundle")), generated);
}
/// Deleting a rules source orphans what its rules generated, found from
/// the work alone: the removed rules bundle no longer has a row, and the
/// bundles it generated still name it.
#[test]
fn removing_a_rules_source_orphans_its_outputs_incrementally() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, _) = ordinary_bundle();
    std::fs::write(assets.join("ordinary.bundle"), ordinary).unwrap();
    std::fs::write(assets.join("rules.bundle"), directory_rules_bundle()).unwrap();
    std::fs::write(assets.join("foo.src"), b"9").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    assert_eq!(
        coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .len(),
        1
    );
    let generated =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("foo.bundle")).unwrap()).unwrap();
    // The generated bundle's own publication is work of its own.
    let work = coordinator.pending_file_work(&mut writer).unwrap();
    coordinator
        .reconcile_directory_imports_affected(&mut writer, &work, false)
        .unwrap();
    coordinator
        .acknowledge_file_work(&mut writer, &work)
        .unwrap();

    std::fs::remove_file(assets.join("rules.bundle")).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![event_root(&assets).join("rules.bundle")],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let work = coordinator.pending_file_work(&mut writer).unwrap();
    assert!(coordinator
        .reconcile_directory_imports_affected(&mut writer, &work, false)
        .unwrap()
        .is_empty());
    coordinator
        .acknowledge_file_work(&mut writer, &work)
        .unwrap();
    assert_eq!(
        coordinator
            .open_reader()
            .unwrap()
            .watched_import_failure(generated.uuid)
            .unwrap()
            .expect("a removed rules source orphans its outputs")
            .terminal,
        distill_store::imports::WatchedImportTerminal::DirectoryOrphan
    );
}

/// A restart whose pipeline has not installed (or was rejected) finds its
/// watched imports' importer unregistered. Reconciliation defers them instead
/// of failing the startup pass, and runs them once the importer registers.
#[test]
fn watched_imports_defer_while_their_importer_is_unregistered() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, _) = ordinary_bundle();
    std::fs::write(assets.join("ordinary.bundle"), ordinary).unwrap();
    std::fs::write(assets.join("source.txt"), b"7").unwrap();
    let open = || {
        DaemonCoordinator::open(
            StoreConfig::new(temp.path().join(".distill")),
            vec![AssetRoot::new("main", &assets)],
            vec![target()],
            64,
        )
        .unwrap()
    };
    let imported_bundle = {
        let coordinator = open();
        let mut writer = coordinator.open_writer().unwrap();
        coordinator.reconcile_full_scan(&mut writer).unwrap();
        configure(&coordinator, temp.path(), &assets);
        import(
            &coordinator,
            BYTE_IMPORTER,
            &["source.txt"],
            "imported.bundle",
        )
    };
    let path = assets.join("imported.bundle");

    // Restart with the source edited, before the configuration (and so the
    // pipeline, whose importer it is) is published again.
    std::fs::write(assets.join("source.txt"), b"8").unwrap();
    let coordinator = open();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert_eq!(
        coordinator
            .authoring_service()
            .watched_imports_needing_reimport(&mut writer)
            .unwrap(),
        vec![imported_bundle]
    );
    assert!(coordinator
        .reconcile_watched_imports(&mut writer)
        .unwrap()
        .is_empty());
    let unchanged = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(unchanged.assets["asset"].data, byte(7));
    assert!(
        coordinator
            .open_reader()
            .unwrap()
            .watched_import_failure(imported_bundle)
            .unwrap()
            .is_none(),
        "a deferred import is not a memoized failure"
    );

    configure(&coordinator, temp.path(), &assets);
    assert_eq!(
        coordinator.reconcile_watched_imports(&mut writer).unwrap(),
        vec![imported_bundle]
    );
    let healed = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(healed.assets["asset"].data, byte(8));
}

/// Reverting a broken source to its last good content retries the import on
/// the incremental (watcher) path and clears the failure, though the last
/// success's read set revalidates again.
#[test]
fn reverting_a_failed_watched_import_clears_its_failure_incrementally() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, _) = ordinary_bundle();
    std::fs::write(assets.join("ordinary.bundle"), ordinary).unwrap();
    std::fs::write(assets.join("source.txt"), b"7").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    import(
        &coordinator,
        BYTE_IMPORTER,
        &["source.txt"],
        "imported.bundle",
    );
    // Build the import index as the startup pass does.
    coordinator.reconcile_watched_imports(&mut writer).unwrap();

    let mut edit = |content: &[u8]| {
        std::fs::write(assets.join("source.txt"), content).unwrap();
        coordinator
            .reconcile_incremental(
                &mut writer,
                &WatcherBatch {
                    paths: vec![event_root(&assets).join("source.txt")],
                    renames: Vec::new(),
                },
            )
            .unwrap();
        let work = coordinator.pending_file_work(&mut writer).unwrap();
        coordinator
            .reconcile_watched_imports_affected(&mut writer, &work, false)
            .unwrap();
        coordinator
            .acknowledge_file_work(&mut writer, &work)
            .unwrap();
    };
    edit(b"broken");
    assert_eq!(import_failures(&coordinator).len(), 1);
    edit(b"7");
    assert!(
        import_failures(&coordinator).is_empty(),
        "the revert reran the import and cleared its failure"
    );
}

/// A project with directory rules generating `{stem}.bundle` from each of
/// `sources` (stem, contents), imported.
fn imported_sources(
    sources: &[(&str, &str)],
) -> (tempfile::TempDir, std::path::PathBuf, DaemonCoordinator) {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, _) = ordinary_bundle();
    std::fs::write(assets.join("ordinary.bundle"), ordinary).unwrap();
    std::fs::write(assets.join("rules.bundle"), directory_rules_bundle()).unwrap();
    std::fs::write(assets.join("other.txt"), b"7").unwrap();
    for (stem, contents) in sources {
        std::fs::write(assets.join(format!("{stem}.src")), contents).unwrap();
    }
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    assert_eq!(
        coordinator
            .reconcile_directory_imports(&mut writer)
            .unwrap()
            .len(),
        sources.len()
    );
    // Earlier passes consumed the setup's watcher work.
    let work = coordinator.pending_file_work(&mut writer).unwrap();
    assert!(coordinator
        .acknowledge_file_work(&mut writer, &work)
        .unwrap());
    (temp, assets, coordinator)
}

/// The content hash each `{stem}.bundle` has in `store`'s version, `None`
/// while it has none: the store records a file's identity, never its bytes.
fn generated_hashes(
    store: &distill_store::StoreReader,
    stems: &[&str],
) -> Vec<Option<distill_core::id::ContentHash>> {
    stems
        .iter()
        .map(|stem| {
            store
                .file_content_hash("main", &format!("{stem}.bundle"))
                .unwrap()
        })
        .collect()
}

/// The value each `{stem}.bundle` holds in `store`'s version, `None` while
/// it has none: read from the file, which must be the one that version
/// observed.
fn generated_values(
    store: &distill_store::StoreReader,
    assets: &std::path::Path,
    stems: &[&str],
) -> Vec<Option<u128>> {
    stems
        .iter()
        .zip(generated_hashes(store, stems))
        .map(|(stem, hash)| {
            let hash = hash?;
            let bytes = std::fs::read(assets.join(format!("{stem}.bundle"))).unwrap();
            assert_eq!(
                *blake3::hash(&bytes).as_bytes(),
                hash.0,
                "{stem}.bundle is the published file"
            );
            let data = &distill_bundle::parse_bundle(&bytes).unwrap().assets["asset"].data;
            Some(byte_of(data).unwrap_or_else(|| panic!("unexpected value {data:?}")))
        })
        .collect()
}

/// Every asset of the generated `{stem}.bundle`s, sorted.
fn generated_assets(assets: &std::path::Path, stems: &[&str]) -> Vec<AssetUuid> {
    let mut uuids = stems
        .iter()
        .flat_map(|stem| {
            let bytes = std::fs::read(assets.join(format!("{stem}.bundle"))).unwrap();
            distill_bundle::parse_bundle(&bytes)
                .unwrap()
                .assets
                .into_values()
                .map(|entry| entry.uuid)
        })
        .collect::<Vec<_>>();
    uuids.sort();
    uuids
}

/// `changed` holds each `{stem}.bundle`'s imported asset once, and nothing
/// outside those bundles.
fn assert_changes_exactly(changed: &[AssetUuid], assets: &std::path::Path, stems: &[&str]) {
    let owned = generated_assets(assets, stems);
    for asset in changed {
        assert!(owned.contains(asset), "{asset:?} changed outside {stems:?}");
    }
    for stem in stems {
        let bytes = std::fs::read(assets.join(format!("{stem}.bundle"))).unwrap();
        let asset = distill_bundle::parse_bundle(&bytes).unwrap().assets["asset"].uuid;
        assert_eq!(
            changed.iter().filter(|changed| **changed == asset).count(),
            1,
            "{stem}'s asset changes once"
        );
    }
}

/// The asset changes the change log holds for `version`.
fn changed_assets(store: &distill_store::StoreReader, version: InputVersion) -> Vec<AssetUuid> {
    store
        .change_log_after(0)
        .unwrap()
        .into_iter()
        .filter(|entry| entry.version == version)
        .filter_map(|entry| match entry.change {
            distill_store::served::Change::Asset { asset, .. } => Some(asset),
            _ => None,
        })
        .collect()
}

/// `assets` as the watcher reports events under it: the configured root,
/// which the configuration canonicalizes (on Windows a `\\?\` verbatim
/// path, which a plain path under the same directory does not match).
fn event_root(assets: &std::path::Path) -> std::path::PathBuf {
    std::fs::canonicalize(assets).unwrap()
}

fn batch(assets: &std::path::Path, files: &[&str]) -> WatcherBatch {
    let assets = event_root(assets);
    WatcherBatch {
        paths: files.iter().map(|file| assets.join(file)).collect(),
        renames: Vec::new(),
    }
}

/// A burst touching three generated bundles, whose imports take different
/// times, publishes as one version: a reader sees every bundle old or every
/// bundle new, never a mix.
#[test]
fn a_burst_across_bundles_publishes_one_version() {
    let stems = ["a", "b", "c"];
    let (_temp, assets, coordinator) = imported_sources(&[("a", "1"), ("b", "2"), ("c", "3")]);
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();
    let old_hashes = generated_hashes(&writer, &stems);
    assert_eq!(
        generated_values(&writer, &assets, &stems),
        [Some(1), Some(2), Some(3)]
    );

    std::fs::write(assets.join("a.src"), b"9").unwrap();
    std::fs::write(assets.join("b.src"), b"5").unwrap();
    std::fs::write(assets.join("c.src"), b"1").unwrap();
    let done = std::sync::atomic::AtomicBool::new(false);
    let (outcome, seen) = std::thread::scope(|scope| {
        let observer = scope.spawn(|| {
            let mut seen = Vec::new();
            loop {
                let finished = done.load(std::sync::atomic::Ordering::Acquire);
                let snapshot = coordinator.open_reader().unwrap().begin_snapshot().unwrap();
                seen.push((
                    snapshot.input_version().unwrap(),
                    generated_hashes(&snapshot, &stems),
                ));
                if finished {
                    return seen;
                }
            }
        });
        let outcome = coordinator
            .reconcile_batch(
                &mut writer,
                &batch(&assets, &["a.src", "b.src", "c.src"]),
                false,
            )
            .unwrap();
        done.store(true, std::sync::atomic::Ordering::Release);
        (outcome, observer.join().unwrap())
    });

    let version = InputVersion(base.0 + 1);
    assert_eq!(outcome.stamp.version, version);
    assert_eq!(outcome.imported.len(), 3);
    assert!(!outcome.more_work);
    assert!(outcome.failures.is_empty());
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(
        generated_values(&reader, &assets, &stems),
        [Some(9), Some(5), Some(1)]
    );
    let new_hashes = generated_hashes(&reader, &stems);
    assert!(old_hashes
        .iter()
        .zip(&new_hashes)
        .all(|(old, new)| old != new));
    for (observed, hashes) in &seen {
        if *observed == base {
            assert_eq!(hashes, &old_hashes, "the base holds every old bundle");
        } else {
            assert_eq!(*observed, version);
            assert_eq!(
                hashes, &new_hashes,
                "the pass's version holds every new bundle"
            );
        }
    }
    assert_eq!(seen.last().unwrap().0, version);

    assert_changes_exactly(&changed_assets(&reader, version), &assets, &stems);
    // The sources' work is acknowledged with the pass; the outputs it wrote
    // are the next pass's work.
    let mut pending = reader
        .committed_file_work()
        .unwrap()
        .dirty
        .into_iter()
        .map(|entry| entry.path)
        .collect::<Vec<_>>();
    pending.sort();
    assert_eq!(pending, ["a.bundle", "b.bundle", "c.bundle"]);
}

/// An RPC publication that lands while a pass's imports run makes the pass
/// stale: it publishes nothing, and its retry recomputes everything from the
/// new base, so both land, once each.
#[test]
fn an_rpc_write_during_a_pass_makes_it_stale_and_its_retry_applies_everything_once() {
    let (temp, assets, coordinator) = imported_sources(&[("a", "1"), ("b", "2"), ("c", "3")]);
    // The run of a.src waits at a gate: it reports `started` and waits for
    // `release`.
    let gate = temp.path().join("gate");
    std::fs::create_dir_all(&gate).unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();

    std::fs::write(assets.join("a.src"), format!("4 {}", gate.display())).unwrap();
    std::fs::write(assets.join("b.src"), b"5").unwrap();
    std::fs::write(assets.join("c.src"), b"6").unwrap();
    let burst = batch(&assets, &["a.src", "b.src", "c.src"]);
    let stale = std::thread::scope(|scope| {
        let pass = scope.spawn(|| {
            let mut writer = coordinator.open_writer().unwrap();
            coordinator.reconcile_batch(&mut writer, &burst, false)
        });
        while !gate.join("started").exists() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        // The RPC import commits while the pass's import is running.
        import(
            &coordinator,
            BYTE_IMPORTER,
            &["other.txt"],
            "explicit.bundle",
        );
        std::fs::write(gate.join("release"), b"").unwrap();
        pass.join().unwrap()
    });
    assert!(
        matches!(
            stale,
            Err(distill_daemon::coordinator::CoordinatorError::Coordinated(
                distill_rpc::CoordinatedCommitError::Stale { .. }
            ))
        ),
        "{stale:?}"
    );
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(reader.input_version().unwrap(), InputVersion(base.0 + 1));
    assert_eq!(
        generated_values(&reader, &assets, &["a", "b", "c"]),
        [Some(1), Some(2), Some(3)],
        "the stale pass published nothing"
    );

    let outcome = coordinator
        .reconcile_batch(&mut writer, &burst, false)
        .unwrap();
    let version = InputVersion(base.0 + 2);
    assert_eq!(outcome.stamp.version, version);
    assert_eq!(outcome.imported.len(), 3);
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(
        generated_values(&reader, &assets, &["a", "b", "c", "explicit"]),
        [Some(4), Some(5), Some(6), Some(7)]
    );
    // Each publication's assets change once, in its own version.
    assert_changes_exactly(
        &changed_assets(&reader, InputVersion(base.0 + 1)),
        &assets,
        &["explicit"],
    );
    assert_changes_exactly(&changed_assets(&reader, version), &assets, &["a", "b", "c"]);
}

/// One failing import in a multi-bundle pass: the others publish, the
/// failing bundle keeps its last good contents and records the failure, and
/// a new output whose importer fails is reported, all in one version.
#[test]
fn an_import_failure_in_a_pass_keeps_its_last_good_bundle_while_the_rest_publish() {
    let (_temp, assets, coordinator) = imported_sources(&[("a", "1"), ("b", "2"), ("c", "3")]);
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();

    std::fs::write(assets.join("a.src"), b"4").unwrap();
    std::fs::write(assets.join("b.src"), b"broken").unwrap();
    std::fs::write(assets.join("c.src"), b"6").unwrap();
    std::fs::write(assets.join("d.src"), b"new and broken").unwrap();
    let outcome = coordinator
        .reconcile_batch(
            &mut writer,
            &batch(&assets, &["a.src", "b.src", "c.src", "d.src"]),
            false,
        )
        .unwrap();

    let version = InputVersion(base.0 + 1);
    assert_eq!(outcome.stamp.version, version);
    assert_eq!(outcome.imported.len(), 2);
    assert_eq!(outcome.failures.len(), 1, "{:?}", outcome.failures);
    assert!(!outcome.more_work);
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(
        generated_values(&reader, &assets, &["a", "b", "c", "d"]),
        [Some(4), Some(2), Some(6), None]
    );
    let failures = import_failures(&coordinator);
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, "b.bundle");
    assert_changes_exactly(&changed_assets(&reader, version), &assets, &["a", "c"]);
}

/// Import `dest` from `sources` with [`CHAIN_IMPORTER`], watched, as one
/// RPC publication, and pass over the watcher work it leaves.
fn chain_import(
    coordinator: &DaemonCoordinator,
    assets: &std::path::Path,
    dest: &str,
    sources: &[&str],
) {
    let mut writer = coordinator.open_writer().unwrap();
    import(coordinator, CHAIN_IMPORTER, sources, dest);
    // The watcher's echo of the output: a pass that indexes its import
    // record and consumes its work.
    let echo = coordinator
        .reconcile_batch(&mut writer, &batch(assets, &[dest]), false)
        .unwrap();
    assert!(!echo.more_work);
}

/// `a.src` imported to `a.bundle` by directory rules, and `stems` each
/// imported, watched, from the bundle of the stem before it: a chain whose
/// every level reads the output of the one before.
fn chained_imports(stems: &[&str]) -> (tempfile::TempDir, std::path::PathBuf, DaemonCoordinator) {
    let (temp, assets, coordinator) = imported_sources(&[("a", "1")]);
    let mut previous = "a".to_owned();
    for stem in stems {
        chain_import(
            &coordinator,
            &assets,
            &format!("{stem}.bundle"),
            &[&format!("{previous}.bundle")],
        );
        previous = (*stem).to_owned();
    }
    (temp, assets, coordinator)
}

/// An edit at the head of a chain of `levels` imports publishes every level
/// in the pass's one version, each asset changed once, and leaves nothing
/// for the next pass.
fn assert_chain_publishes_in_one_version(stems: &[&str]) {
    let (_temp, assets, coordinator) = chained_imports(stems);
    let mut all = vec!["a"];
    all.extend_from_slice(stems);
    let reader = coordinator.open_reader().unwrap();
    let before = generated_values(&reader, &assets, &all);
    assert_eq!(
        before,
        (1..=all.len() as u128).map(Some).collect::<Vec<_>>(),
        "each level is one more than the level it reads"
    );
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();

    std::fs::write(assets.join("a.src"), b"5").unwrap();
    let outcome = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["a.src"]), false)
        .unwrap();

    let version = InputVersion(base.0 + 1);
    assert_eq!(outcome.stamp.version, version);
    assert_eq!(outcome.imported.len(), all.len());
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    assert!(!outcome.more_work);
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(reader.input_version().unwrap(), version);
    assert_eq!(
        generated_values(&reader, &assets, &all),
        (5..5 + all.len() as u128).map(Some).collect::<Vec<_>>(),
        "every level of the chain is in the pass's version"
    );
    assert_changes_exactly(&changed_assets(&reader, version), &assets, &all);

    // The outputs' own watcher work finds nothing left to import.
    let outputs = all
        .iter()
        .map(|stem| format!("{stem}.bundle"))
        .collect::<Vec<_>>();
    let outputs = outputs.iter().map(String::as_str).collect::<Vec<_>>();
    let echo = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &outputs), false)
        .unwrap();
    assert_eq!(
        echo.stamp.version, version,
        "nothing drifted to another pass"
    );
    assert!(echo.imported.is_empty());
    assert!(!echo.more_work);
}

#[test]
fn a_two_level_import_chain_publishes_in_one_version() {
    assert_chain_publishes_in_one_version(&["b"]);
}

#[test]
fn a_three_level_import_chain_publishes_in_one_version() {
    assert_chain_publishes_in_one_version(&["b", "c"]);
}

/// A chain deeper than a pass's bound publishes its first levels in one
/// version, reports the bound, and leaves the rest to the next pass, which
/// publishes it.
#[test]
fn a_chain_deeper_than_the_bound_continues_in_the_next_pass() {
    let stems = ["l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8", "l9", "l10"];
    let (_temp, assets, coordinator) = chained_imports(&stems);
    let mut all = vec!["a"];
    all.extend_from_slice(&stems);
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();

    std::fs::write(assets.join("a.src"), b"20").unwrap();
    let first = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["a.src"]), false)
        .unwrap();
    assert_eq!(first.stamp.version, InputVersion(base.0 + 1));
    assert_eq!(first.imported.len(), 9, "the head and eight chained levels");
    assert!(first.more_work);
    assert_eq!(first.failures.len(), 1, "{:?}", first.failures);
    assert!(
        first.failures[0].contains("deeper than 8 levels")
            && first.failures[0].contains("l9.bundle"),
        "{:?}",
        first.failures
    );
    let reader = coordinator.open_reader().unwrap();
    let mut expected = (20..29).map(Some).collect::<Vec<_>>();
    expected.extend([Some(10), Some(11)]);
    assert_eq!(generated_values(&reader, &assets, &all), expected);

    let second = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &[]), false)
        .unwrap();
    assert_eq!(second.stamp.version, InputVersion(base.0 + 2));
    assert_eq!(second.imported.len(), 2);
    assert!(second.failures.is_empty(), "{:?}", second.failures);
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(
        generated_values(&reader, &assets, &all),
        (20..31).map(Some).collect::<Vec<_>>()
    );
}

/// Imports that read each other's outputs (a cycle that never settles) run
/// once each per pass: the one that would read its own output again is cut
/// and reported, and the pass still publishes the rest in one version.
#[test]
fn an_import_cycle_is_cut_and_reported() {
    let (_temp, assets, coordinator) = chained_imports(&[]);
    // x reads a's output and y's; y reads x's.
    chain_import(&coordinator, &assets, "x.bundle", &["a.bundle", "y.bundle"]);
    chain_import(&coordinator, &assets, "y.bundle", &["x.bundle"]);
    let mut writer = coordinator.open_writer().unwrap();
    // x now reads y: the cycle runs a pass of its own.
    coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["y.bundle"]), false)
        .unwrap();
    let reader = coordinator.open_reader().unwrap();
    let before = generated_values(&reader, &assets, &["a", "x", "y"]);
    let base = writer.input_version().unwrap();

    std::fs::write(assets.join("a.src"), b"50").unwrap();
    let outcome = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["a.src"]), false)
        .unwrap();
    let version = InputVersion(base.0 + 1);
    assert_eq!(outcome.stamp.version, version);
    assert_eq!(outcome.failures.len(), 1, "{:?}", outcome.failures);
    assert!(
        outcome.failures[0].contains("import cycle") && outcome.failures[0].contains("x.bundle"),
        "{:?}",
        outcome.failures
    );
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(generated_values(&reader, &assets, &["a"]), [Some(50)]);
    let [_, Some(x), Some(_)] = generated_values(&reader, &assets, &["a", "x", "y"])[..] else {
        panic!("{before:?}");
    };
    assert_eq!(x, 51, "x read a's new output");
    assert_changes_exactly(&changed_assets(&reader, version), &assets, &["a", "x", "y"]);
}

/// Pages a whole-namespace watched-import check reads beside `filler`
/// ordinary bundles, once their publication's work is indexed and
/// acknowledged.
fn import_index_pages(filler: usize) -> u64 {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (_, schema, schema_hash) = ordinary_bundle();
    for index in 0..filler {
        let mut uuid = [0; 16];
        uuid[..8].copy_from_slice(&(index as u64 + 1).to_le_bytes());
        let entry = AssetEntry {
            uuid: AssetUuid(uuid),
            type_uuid: TYPE_UUID,
            schema_hash,
            authoring_only: false,
            data: byte(1),
        };
        let bytes = distill_bundle::write_bundle(&Bundle {
            format_version: 1,
            uuid: BundleUuid(uuid),
            primary: Some("entry".into()),
            schemas: BTreeMap::from([(schema_hash, schema.clone())]),
            assets: BTreeMap::from([("entry".into(), entry)]),
        })
        .unwrap();
        let directory = assets.join(format!("filler/d{}", index % 40));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("b{index}.bundle")), bytes).unwrap();
    }
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    let authoring = coordinator.authoring_service();
    assert!(authoring
        .watched_imports_needing_reimport(&mut writer)
        .unwrap()
        .is_empty());
    let work = coordinator.pending_file_work(&mut writer).unwrap();
    assert!(coordinator
        .acknowledge_file_work(&mut writer, &work)
        .unwrap());

    let before = writer.pages_fetched().unwrap();
    assert!(authoring
        .watched_imports_needing_reimport(&mut writer)
        .unwrap()
        .is_empty());
    writer.pages_fetched().unwrap() - before
}

/// The import index is kept by the dirty work bundle publication queues: a
/// whole-namespace import check with no pending work reindexes nothing.
#[test]
fn an_indexed_namespace_is_not_reindexed() {
    let small = import_index_pages(20);
    let large = import_index_pages(2000);
    println!("watched-import check: {small} pages beside 20 bundles, {large} beside 2000");
    assert!(
        large <= small + 16,
        "{small} pages beside 20 bundles, {large} beside 2000"
    );
}

/// An RPC reimport whose importer fails memoizes the failure: the memo
/// commits, with no new version, and the client gets the failure.
#[test]
fn a_failed_rpc_reimport_commits_only_its_memo() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, _) = ordinary_bundle();
    std::fs::write(assets.join("ordinary.bundle"), ordinary).unwrap();
    std::fs::write(assets.join("source.txt"), b"7").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    let bundle = import(
        &coordinator,
        BYTE_IMPORTER,
        &["source.txt"],
        "imported.bundle",
    );
    std::fs::write(assets.join("source.txt"), b"broken").unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![event_root(&assets).join("source.txt")],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert!(import_failures(&coordinator).is_empty());

    let base = coordinator.server().current_stamp().unwrap().version;
    let hub = connect(&coordinator);
    let reimport = hub.reimport(base, bundle);
    assert!(
        matches!(reimport, distill_rpc::RpcResult::Failure(_)),
        "{reimport:?}"
    );
    assert_eq!(coordinator.server().current_stamp().unwrap().version, base);
    assert_eq!(
        import_failures(&coordinator).len(),
        1,
        "the failure memo committed"
    );
}

/// Import through the hub as [`import_with`] does, with `if_changed`.
fn import_if_changed(
    coordinator: &DaemonCoordinator,
    sources: &[&str],
    dest: &str,
    settings: &AuthoredValue,
) -> BundleUuid {
    let base = coordinator.server().current_stamp().unwrap().version;
    let request = ImportRequest {
        importer: BYTE_IMPORTER.into(),
        sources: sources.iter().map(|source| (*source).to_owned()).collect(),
        dest: dest.into(),
        settings: AuthoringValue {
            canonical_value: Arc::from(distill_json::write(settings).unwrap().into_bytes()),
            blobs: Vec::new(),
        },
        watch: true,
        root: "main".into(),
        if_changed: true,
    };
    match connect(coordinator).import(base, request) {
        distill_rpc::RpcResult::Success(bundle) => bundle,
        other => panic!("expected an import, got {other:?}"),
    }
}

/// `if_changed` skips an explicit import whose importer, sources, watch
/// flag and completed settings equal what the destination's `$settings`
/// and `$record` hold, and reimports when the settings differ, including a
/// field left out that now takes the importer's default.
#[test]
fn if_changed_imports_skip_unchanged_settings_and_rerun_edited_ones() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(assets.join("source.txt"), b"4").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    let read = || {
        distill_bundle::parse_bundle(&std::fs::read(assets.join("imported.bundle")).unwrap())
            .unwrap()
    };

    // A missing destination imports.
    let tripled = settings([0, 0], 3);
    let bundle = import_if_changed(&coordinator, &["source.txt"], "imported.bundle", &tripled);
    assert_eq!(read().assets["asset"].data, byte(4 * 3));
    let bytes = std::fs::read(assets.join("imported.bundle")).unwrap();

    // The same line again: skipped, so the importer never sees the source
    // change the watcher has not published yet. A sparse spelling
    // completing to the same value is the same line.
    std::fs::write(assets.join("source.txt"), b"5").unwrap();
    let sparse = object([("scale", object([("by", AuthoredValue::UInt(3))]))]);
    for same in [&tripled, &sparse] {
        assert_eq!(
            import_if_changed(&coordinator, &["source.txt"], "imported.bundle", same),
            bundle
        );
    }
    assert_eq!(std::fs::read(assets.join("imported.bundle")).unwrap(), bytes);
    assert_eq!(read().assets["asset"].data, byte(4 * 3));
    let skipped_to = coordinator.server().current_stamp().unwrap().version;
    assert!(changed_assets(&coordinator.open_reader().unwrap(), skipped_to).is_empty());
    std::fs::write(assets.join("source.txt"), b"4").unwrap();

    // Edited settings reimport, keeping the bundle identity.
    let doubled = settings([1, 0], 2);
    assert_eq!(
        import_if_changed(&coordinator, &["source.txt"], "imported.bundle", &doubled),
        bundle
    );
    assert_eq!(read().assets["$settings"].data, doubled);
    assert_eq!(read().assets["asset"].data, byte((4 + 1) * 2));

    // Settings left out take the defaults, which differ: reimport.
    import_if_changed(&coordinator, &["source.txt"], "imported.bundle", &object([]));
    assert_eq!(read().assets["$settings"].data, default_settings());
    assert_eq!(read().assets["asset"].data, byte(4));
}

/// `read_settings` gives an importer the `$settings` of the import of a
/// path, and a settings edit of that import re-runs the reading import in
/// the pass that publishes the edit.
#[test]
fn read_settings_returns_the_bundle_settings_and_an_edit_reruns_the_reader() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(assets.join("image.txt"), b"4").unwrap();
    std::fs::write(assets.join("loose.txt"), b"9").unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, temp.path(), &assets);
    let value = |dest: &str| {
        let bundle =
            distill_bundle::parse_bundle(&std::fs::read(assets.join(dest)).unwrap()).unwrap();
        byte_of(&bundle.assets["asset"].data)
    };

    import_with(
        &coordinator,
        BYTE_IMPORTER,
        &["image.txt"],
        "image.txt.bundle",
        &settings([0, 0], 3),
    );
    let echo = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["image.txt.bundle"]), false)
        .unwrap();
    assert!(!echo.more_work);
    // `image.txt`'s import has `scale.by` 3; `loose.txt` has no import.
    import(
        &coordinator,
        SETTINGS_IMPORTER,
        &["image.txt", "loose.txt"],
        "reader.bundle",
    );
    let echo = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["reader.bundle"]), false)
        .unwrap();
    assert!(!echo.more_work);
    assert_eq!(value("reader.bundle"), Some(3));

    // Edit the image import's settings: publishing its bundle re-runs the
    // reader.
    import_with(
        &coordinator,
        BYTE_IMPORTER,
        &["image.txt"],
        "image.txt.bundle",
        &settings([0, 0], 5),
    );
    assert_eq!(value("image.txt.bundle"), Some(20));
    let outcome = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["image.txt.bundle"]), false)
        .unwrap();
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    assert_eq!(outcome.imported.len(), 1, "the reader, and only it");
    assert_eq!(value("reader.bundle"), Some(5));
}

/// The bundle UUID and each entry's UUID by local id of the bundle at `path`.
fn identities(path: &std::path::Path) -> (BundleUuid, BTreeMap<String, AssetUuid>) {
    let bundle = distill_bundle::parse_bundle(&std::fs::read(path).unwrap()).unwrap();
    let entries = bundle
        .assets
        .iter()
        .map(|(local_id, entry)| (local_id.clone(), entry.uuid))
        .collect();
    (bundle.uuid, entries)
}

/// A daemon over a fresh store at `dir/.distill` whose root "main" is
/// `assets`, scanned and configured.
fn open_configured(dir: &std::path::Path, assets: &std::path::Path) -> DaemonCoordinator {
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(dir.join(".distill")),
        vec![AssetRoot::new("main", assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    configure(&coordinator, dir, assets);
    coordinator
}

/// A first import mints random UUIDs that depend on nothing it read:
/// identical sources at two paths get distinct ones. The bundle then keeps
/// them through every reimport, also from a source moved to another path
/// together with the bundle.
#[test]
fn first_imports_mint_fresh_identities_that_reimports_and_moves_keep() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(assets.join("copy")).unwrap();
    std::fs::write(assets.join("source.txt"), b"7").unwrap();
    std::fs::write(assets.join("copy/source.txt"), b"7").unwrap();
    let coordinator = open_configured(temp.path(), &assets);
    let mut writer = coordinator.open_writer().unwrap();

    let bundle = import(&coordinator, BYTE_IMPORTER, &["source.txt"], "imported.bundle");
    let copy = import(
        &coordinator,
        BYTE_IMPORTER,
        &["copy/source.txt"],
        "copy/imported.bundle",
    );
    let first = identities(&assets.join("imported.bundle"));
    let copied = identities(&assets.join("copy/imported.bundle"));
    assert_eq!(first.0, bundle);
    assert_eq!(copied.0, copy);
    assert_ne!(bundle, copy);
    assert_eq!(
        first.1.keys().collect::<Vec<_>>(),
        ["$record", "$settings", "asset"]
    );
    for uuid in first.1.values() {
        assert!(
            !copied.1.values().any(|other| other == uuid),
            "identical sources at two paths share {uuid:?}"
        );
    }

    // An explicit reimport over the bundle, and a watched one.
    assert_eq!(
        import(&coordinator, BYTE_IMPORTER, &["source.txt"], "imported.bundle"),
        bundle
    );
    assert_eq!(identities(&assets.join("imported.bundle")), first);
    std::fs::write(assets.join("source.txt"), b"8").unwrap();
    coordinator
        .reconcile_incremental(&mut writer, &batch(&assets, &["source.txt"]))
        .unwrap();
    assert_eq!(
        coordinator.reconcile_watched_imports(&mut writer).unwrap(),
        vec![bundle]
    );
    assert_eq!(identities(&assets.join("imported.bundle")), first);

    // The source and its bundle move to another directory; the import from
    // the moved source over the moved bundle keeps every UUID.
    std::fs::create_dir_all(assets.join("moved")).unwrap();
    std::fs::rename(assets.join("source.txt"), assets.join("moved/source.txt")).unwrap();
    std::fs::rename(
        assets.join("imported.bundle"),
        assets.join("moved/imported.bundle"),
    )
    .unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert_eq!(
        import(
            &coordinator,
            BYTE_IMPORTER,
            &["moved/source.txt"],
            "moved/imported.bundle",
        ),
        bundle
    );
    let moved = identities(&assets.join("moved/imported.bundle"));
    assert_eq!(moved, first);
    let moved = distill_bundle::parse_bundle(
        &std::fs::read(assets.join("moved/imported.bundle")).unwrap(),
    )
    .unwrap();
    assert_eq!(moved.assets["asset"].data, byte(8));
}

/// A first import whose bundle was written but whose commit the daemon lost
/// (a crash before the store committed; here the whole store is lost) does
/// not mint twice: the retried import finds the written bundle in the scan
/// and folds over it.
#[test]
fn a_first_import_retried_after_its_commit_was_lost_keeps_its_identities() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(assets.join("source.txt"), b"7").unwrap();
    let coordinator = open_configured(temp.path(), &assets);
    let bundle = import(&coordinator, BYTE_IMPORTER, &["source.txt"], "imported.bundle");
    let first = identities(&assets.join("imported.bundle"));
    drop(coordinator);

    std::fs::remove_dir_all(temp.path().join(".distill")).unwrap();
    let coordinator = open_configured(temp.path(), &assets);
    assert_eq!(
        import(&coordinator, BYTE_IMPORTER, &["source.txt"], "imported.bundle"),
        bundle
    );
    assert_eq!(identities(&assets.join("imported.bundle")), first);
}

/// A pass folds a first import twice: once as the output the imports
/// chained to it read, once to publish it. Both folds mint the same UUIDs,
/// so the chained import's read of the output still holds at publication
/// and the whole chain publishes in one version.
#[test]
fn a_first_import_chained_in_a_pass_publishes_the_identities_its_readers_read() {
    let (_temp, assets, coordinator) = chained_imports(&["b"]);
    let before = identities(&assets.join("a.bundle"));
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();

    // The generated bundle is deleted and its source edited: its rules
    // import it anew, and the import reading it runs on that output.
    std::fs::remove_file(assets.join("a.bundle")).unwrap();
    std::fs::write(assets.join("a.src"), b"5").unwrap();
    let outcome = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["a.bundle", "a.src"]), false)
        .unwrap();
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    // b published with the regenerated a it read: had the publication
    // minted other UUIDs than the preview b read, b would have drifted.
    assert_eq!(outcome.stamp.version, InputVersion(base.0 + 1));
    assert_eq!(outcome.imported.len(), 2, "{:?}", outcome.imported);
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(
        generated_values(&reader, &assets, &["a", "b"]),
        [Some(5), Some(6)]
    );
    // The outputs' own watcher work finds nothing left to import.
    let echo = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["a.bundle", "b.bundle"]), false)
        .unwrap();
    assert!(echo.imported.is_empty(), "{:?}", echo.imported);
    assert!(echo.failures.is_empty(), "{:?}", echo.failures);
    let after = identities(&assets.join("a.bundle"));
    for uuid in after.1.values().chain([&AssetUuid(after.0 .0)]) {
        assert!(
            !before.1.values().any(|old| old == uuid) && before.0 .0 != uuid.0,
            "a deleted bundle's identity is not minted again"
        );
    }
}

/// A watched import rejected because a bundle it reads went away re-runs
/// when the bundle comes back: the memoized failure keeps the probe of the
/// missing path in its read set (newgameplus doc 21 §7.4: a clip file
/// bound to a skeleton file's bundle).
#[test]
fn an_import_rejected_for_a_removed_bundle_reruns_when_it_returns() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("rules.bundle"),
        directory_rules_bundle_for(true, REQUIRE_IMPORTER, &default_settings()),
    )
    .unwrap();
    std::fs::write(assets.join("rig.txt"), b"4").unwrap();
    std::fs::write(assets.join("clip.src"), b"rig.txt").unwrap();
    let coordinator = open_configured(temp.path(), &assets);
    let mut writer = coordinator.open_writer().unwrap();
    import_with(&coordinator, BYTE_IMPORTER, &["rig.txt"], "rig.txt.bundle", &settings([0, 0], 3));
    coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["rig.txt.bundle"]), false)
        .unwrap();
    coordinator.reconcile_directory_imports(&mut writer).unwrap();
    let clip = || {
        let bundle =
            distill_bundle::parse_bundle(&std::fs::read(assets.join("clip.bundle")).unwrap())
                .unwrap();
        byte_of(&bundle.assets["asset"].data)
    };
    assert_eq!(clip(), Some(3));
    coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["clip.bundle"]), false)
        .unwrap();

    // The rig's bundle goes: the clip's re-import is rejected and listed,
    // keeping its last good bundle.
    let rig = std::fs::read(assets.join("rig.txt.bundle")).unwrap();
    std::fs::remove_file(assets.join("rig.txt.bundle")).unwrap();
    let outcome = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["rig.txt.bundle"]), false)
        .unwrap();
    assert!(outcome.imported.is_empty());
    let failures = import_failures(&coordinator);
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].0, "clip.bundle");
    assert!(failures[0].1.contains("rig.txt has no import"), "{failures:?}");
    assert_eq!(clip(), Some(3));

    // An unrelated change does not re-run it.
    std::fs::write(assets.join("other.txt"), b"1").unwrap();
    let outcome = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["other.txt"]), false)
        .unwrap();
    assert!(outcome.imported.is_empty() && outcome.failures.is_empty());

    // It comes back: the clip re-imports and its failure clears.
    std::fs::write(assets.join("rig.txt.bundle"), rig).unwrap();
    let outcome = coordinator
        .reconcile_batch(&mut writer, &batch(&assets, &["rig.txt.bundle"]), false)
        .unwrap();
    assert_eq!(outcome.imported.len(), 1, "the clip");
    assert!(import_failures(&coordinator).is_empty());
    assert_eq!(clip(), Some(3));
}
