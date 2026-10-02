use std::collections::BTreeMap;
use std::sync::Arc;

use distill_bundle::{AssetEntry, Bundle};
use distill_core::bootstrap::{
    bootstrap_control_logical_registry_v1, BootstrapControlSpecV1, BootstrapControlSymbol,
    DIRECTORY_IMPORT_RULES_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
use distill_daemon::coordinator::DaemonCoordinator;
use distill_daemon::importer::{AuthoringImportContext, AuthoringImporter, AuthoringImporterError};
use distill_daemon::scanner::AssetRoot;
use distill_daemon::watcher::WatcherBatch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, AuthoringValue, Commit, ImportRequest, InputVersion, TargetDefinition,
    TargetDefinitionHash,
};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use distill_store::pipeline::ValidatedPipelineEpoch;
use distill_store::state::PipelineEpoch;
use distill_store::StoreConfig;

const TYPE_UUID: TypeUuid = TypeUuid([71; 16]);

struct ByteImporter {
    schema: LogicalSchema,
}

impl AuthoringImporter for ByteImporter {
    fn id(&self) -> &str {
        "byte-importer"
    }

    fn version(&self) -> u32 {
        1
    }

    fn settings_type_uuid(&self) -> TypeUuid {
        TYPE_UUID
    }

    fn settings_schema(&self) -> &LogicalSchema {
        &self.schema
    }

    fn default_settings(&self) -> AuthoredValue {
        AuthoredValue::UInt(0)
    }

    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        _settings: &AuthoredValue,
    ) -> Result<distill_build::import::ImportOutput, AuthoringImporterError> {
        let source = context
            .sources()
            .first()
            .ok_or_else(|| AuthoringImporterError::rejected(1, "one source is required"))?
            .path
            .clone();
        let bytes = context.read(&source)?;
        let value = std::str::from_utf8(&bytes)
            .map_err(|error| AuthoringImporterError::rejected(2, error.to_string()))?
            .parse::<u8>()
            .map_err(|error| AuthoringImporterError::rejected(3, error.to_string()))?;
        let mut output = distill_build::import::ImportOutput::new();
        output
            .entry("asset", TYPE_UUID, AuthoredValue::UInt(value.into()))
            .map_err(|error| AuthoringImporterError::rejected(4, format!("{error:?}")))?;
        Ok(output)
    }
}

fn ordinary_bundle() -> (Vec<u8>, LogicalSchema, distill_core::id::LogicalHash) {
    let schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    };
    let schema_hash = node_hash(&schema.root).unwrap();
    let entry = AssetEntry {
        uuid: AssetUuid([72; 16]),
        type_uuid: TYPE_UUID,
        schema_hash,
        authoring_only: false,
        data: AuthoredValue::UInt(1),
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

/// Publish a Ready pipeline epoch whose registry names `schema_hash` as the
/// current schema of `TYPE_UUID`: importer outputs are written at it.
fn publish_schema_registry(coordinator: &DaemonCoordinator, schema_hash: LogicalHash) {
    let mut writer = coordinator.open_writer().unwrap();
    let mut schema_registry = bootstrap_control_logical_registry_v1().unwrap();
    schema_registry.insert(TYPE_UUID, schema_hash);
    let epoch = ValidatedPipelineEpoch::validate(PipelineEpoch {
        dylib_hash: [9; 32],
        target_set: CanonicalTargetSet::canonical(vec![TargetSetRow {
            name: "dev".into(),
            target_definition_hash: [4; 32],
        }])
        .unwrap(),
        schema_registry,
        registrations: Vec::new(),
    })
    .unwrap();
    let base = coordinator.server().current_stamp().version;
    coordinator
        .coordinated_commit(&mut writer, base, |store| {
            store
                .input_transaction(|transaction| transaction.publish_pipeline_epoch(&epoch))
                .map_err(|error| error.to_string())?;
            Ok(Commit::default())
        })
        .unwrap();
}

/// The watched-import failures a runtime client polls, as (path, message).
fn import_failures(coordinator: &DaemonCoordinator) -> Vec<(String, String)> {
    let hub = match coordinator
        .server()
        .root()
        .connect(distill_rpc::ConnectRequest::new("dev", TargetDefinitionHash([4; 32])))
    {
        distill_rpc::ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("expected connection, got {other:?}"),
    };
    match hub.import_failures() {
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

fn directory_rules_bundle() -> Vec<u8> {
    directory_rules_bundle_with_rule(true)
}

fn directory_rules_bundle_with_rule(include_rule: bool) -> Vec<u8> {
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
        ("importer", AuthoredValue::Str("byte-importer".into())),
        ("matches", matches),
        ("output", AuthoredValue::Str("{stem}.bundle".into())),
        (
            "settings",
            object([("UInt", object([("value", AuthoredValue::UInt(5))]))]),
        ),
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
    let (ordinary, schema, schema_hash) = ordinary_bundle();
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

    publish_schema_registry(&coordinator, schema_hash);

    coordinator
        .authoring_service()
        .register_importer(Arc::new(ByteImporter { schema }))
        .unwrap();
    let backend = Arc::clone(coordinator.authoring_service());
    let imported_bundle = Arc::new(std::sync::Mutex::new(None));
    let captured = Arc::clone(&imported_bundle);
    coordinator
        .coordinated_commit(&mut writer, InputVersion(2), |store| {
            let prepared = backend
                .prepare_import(store, 
                    InputVersion(2),
                    &ImportRequest {
                        importer: "byte-importer".into(),
                        sources: vec!["source.txt".into()],
                        dest: "imported.bundle".into(),
                        settings: AuthoringValue {
                            canonical_value: Arc::from(&b"3"[..]),
                            blobs: Vec::new(),
                        },
                        watch: true,
                        root: "main".into(),
                    },
                )
                .map_err(|error| format!("{error:?}"))?;
            *captured.lock().unwrap() = Some(prepared.bundle);
            Ok(prepared.commit)
        })
        .unwrap();
    let imported_bundle = imported_bundle.lock().unwrap().unwrap();
    let path = assets.join("imported.bundle");
    let first = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    let first_asset = first.assets["asset"].uuid;
    assert_eq!(first.assets["asset"].data, AuthoredValue::UInt(7));
    assert_eq!(first.assets["$settings"].data, AuthoredValue::UInt(3));
    assert!(first.assets.contains_key("$record"));
    assert!(coordinator.authoring_service().watched_imports_needing_reimport(&mut writer)
        .unwrap()
        .is_empty());

    std::fs::write(assets.join("source.txt"), b"8").unwrap();
    assert_eq!(
        coordinator.authoring_service().watched_imports_needing_reimport(&mut writer)
            .unwrap(),
        vec![imported_bundle]
    );
    coordinator
        .reconcile_incremental(&mut writer, &WatcherBatch {
            paths: vec![assets.join("source.txt")],
            renames: Vec::new(),
        })
        .unwrap();
    assert_eq!(
        coordinator.reconcile_watched_imports(&mut writer).unwrap(),
        vec![imported_bundle]
    );
    let second = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(second.uuid, imported_bundle);
    assert_eq!(second.assets["asset"].uuid, first_asset);
    assert_eq!(second.assets["asset"].data, AuthoredValue::UInt(8));
    assert_eq!(second.assets["$settings"].data, AuthoredValue::UInt(3));
    assert_eq!(
        coordinator.open_reader().unwrap().input_version(),
        InputVersion(5)
    );

    std::fs::write(assets.join("source.txt"), b"not-a-byte").unwrap();
    coordinator
        .reconcile_incremental(&mut writer, &WatcherBatch {
            paths: vec![assets.join("source.txt")],
            renames: Vec::new(),
        })
        .unwrap();
    assert!(coordinator.reconcile_watched_imports(&mut writer).unwrap().is_empty());
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
        coordinator.open_reader().unwrap().input_version(),
        InputVersion(6),
        "memoizing a failure is not an input event"
    );
    let failures = import_failures(&coordinator);
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, "imported.bundle");
    assert!(failures[0].1.contains("invalid digit"), "{failures:?}");
    let failed_memo = failed.memo_seq;
    assert!(coordinator.authoring_service().watched_imports_needing_reimport(&mut writer)
        .unwrap()
        .is_empty());
    assert!(coordinator.reconcile_watched_imports(&mut writer).unwrap().is_empty());
    assert_eq!(
        coordinator.open_reader().unwrap().memo_seq(),
        failed_memo,
        "an unchanged failure must not spin"
    );

    std::fs::write(assets.join("source.txt"), b"9").unwrap();
    coordinator
        .reconcile_incremental(&mut writer, &WatcherBatch {
            paths: vec![assets.join("source.txt")],
            renames: Vec::new(),
        })
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
    assert_eq!(healed.assets["asset"].data, AuthoredValue::UInt(9));
    assert!(import_failures(&coordinator).is_empty());
    assert_eq!(
        coordinator.open_reader().unwrap().input_version(),
        InputVersion(8)
    );

    std::fs::remove_file(assets.join("source.txt")).unwrap();
    coordinator
        .reconcile_incremental(&mut writer, &WatcherBatch {
            paths: vec![assets.join("source.txt")],
            renames: Vec::new(),
        })
        .unwrap();
    assert!(coordinator.reconcile_watched_imports(&mut writer).unwrap().is_empty());
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
    assert!(coordinator.authoring_service().watched_imports_needing_reimport(&mut writer)
        .unwrap()
        .is_empty());
    std::fs::write(assets.join("source.txt"), b"10").unwrap();
    coordinator
        .reconcile_incremental(&mut writer, &WatcherBatch {
            paths: vec![assets.join("source.txt")],
            renames: Vec::new(),
        })
        .unwrap();
    assert_eq!(
        coordinator.reconcile_watched_imports(&mut writer).unwrap(),
        vec![imported_bundle]
    );
    let healed = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(healed.assets["asset"].data, AuthoredValue::UInt(10));
    assert_eq!(
        coordinator.open_reader().unwrap().input_version(),
        InputVersion(11)
    );
}

#[test]
fn directory_rules_publish_owned_bundles_and_listing_loss_only_orphans_them() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, schema, schema_hash) = ordinary_bundle();
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
    publish_schema_registry(&coordinator, schema_hash);
    coordinator
        .authoring_service()
        .register_importer(Arc::new(ByteImporter { schema }))
        .unwrap();

    let imported = coordinator.reconcile_directory_imports(&mut writer).unwrap();
    assert_eq!(imported.len(), 1);
    let generated_path = assets.join("foo.bundle");
    let generated = distill_bundle::parse_bundle(&std::fs::read(&generated_path).unwrap()).unwrap();
    assert_eq!(generated.assets["asset"].data, AuthoredValue::UInt(9));
    assert_eq!(generated.assets["$settings"].data, AuthoredValue::UInt(5));
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
        .reconcile_incremental(&mut writer, &WatcherBatch {
            paths: vec![assets.join("foo.src")],
            renames: Vec::new(),
        })
        .unwrap();
    let work = coordinator.pending_file_work(&mut writer).unwrap();
    assert!(coordinator
        .reconcile_directory_imports_affected(&mut writer, &work, false)
        .unwrap()
        .is_empty());
    coordinator.acknowledge_file_work(&mut writer, &work).unwrap();
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
        coordinator.open_reader().unwrap().memo_seq(),
        orphan_memo,
        "an unchanged orphan must not spin memo state"
    );
    assert!(
        generated_path.exists(),
        "lost groups are orphaned, never deleted"
    );

    std::fs::write(assets.join("foo.src"), b"9").unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    coordinator.reconcile_directory_imports(&mut writer).unwrap();
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
    coordinator.reconcile_directory_imports(&mut writer).unwrap();
    assert!(coordinator
        .open_reader()
        .unwrap()
        .watched_import_failure(generated.uuid)
        .unwrap()
        .is_none());
}

/// A restart whose pipeline has not installed (or was rejected) finds its
/// watched imports' importer unregistered. Reconciliation defers them instead
/// of failing the startup pass, and runs them once the importer registers.
#[test]
fn watched_imports_defer_while_their_importer_is_unregistered() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, schema, schema_hash) = ordinary_bundle();
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
        publish_schema_registry(&coordinator, schema_hash);
        coordinator
            .authoring_service()
            .register_importer(Arc::new(ByteImporter {
                schema: schema.clone(),
            }))
            .unwrap();
        let backend = Arc::clone(coordinator.authoring_service());
        let base = coordinator.server().current_stamp().version;
        let imported = Arc::new(std::sync::Mutex::new(None));
        let captured = Arc::clone(&imported);
        coordinator
            .coordinated_commit(&mut writer, base, |store| {
                let prepared = backend
                    .prepare_import(store, 
                        base,
                        &ImportRequest {
                            importer: "byte-importer".into(),
                            sources: vec!["source.txt".into()],
                            dest: "imported.bundle".into(),
                            settings: AuthoringValue {
                                canonical_value: Arc::from(&b"3"[..]),
                                blobs: Vec::new(),
                            },
                            watch: true,
                            root: "main".into(),
                        },
                    )
                    .map_err(|error| format!("{error:?}"))?;
                *captured.lock().unwrap() = Some(prepared.bundle);
                Ok(prepared.commit)
            })
            .unwrap();
        let bundle = imported.lock().unwrap().unwrap();
        bundle
    };
    let path = assets.join("imported.bundle");

    // Restart with the source edited and no importer registered.
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
    assert!(coordinator.reconcile_watched_imports(&mut writer).unwrap().is_empty());
    let unchanged = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(unchanged.assets["asset"].data, AuthoredValue::UInt(7));
    assert!(
        coordinator
            .open_reader()
            .unwrap()
            .watched_import_failure(imported_bundle)
            .unwrap()
            .is_none(),
        "a deferred import is not a memoized failure"
    );

    coordinator
        .authoring_service()
        .register_importer(Arc::new(ByteImporter { schema }))
        .unwrap();
    assert_eq!(
        coordinator.reconcile_watched_imports(&mut writer).unwrap(),
        vec![imported_bundle]
    );
    let healed = distill_bundle::parse_bundle(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(healed.assets["asset"].data, AuthoredValue::UInt(8));
}

/// Reverting a broken source to its last good content retries the import on
/// the incremental (watcher) path and clears the failure, though the last
/// success's read set revalidates again.
#[test]
fn reverting_a_failed_watched_import_clears_its_failure_incrementally() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, schema, schema_hash) = ordinary_bundle();
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
    publish_schema_registry(&coordinator, schema_hash);
    coordinator
        .authoring_service()
        .register_importer(Arc::new(ByteImporter { schema }))
        .unwrap();
    let backend = Arc::clone(coordinator.authoring_service());
    let base = coordinator.server().current_stamp().version;
    coordinator
        .coordinated_commit(&mut writer, base, |store| {
            let prepared = backend
                .prepare_import(store, 
                    base,
                    &ImportRequest {
                        importer: "byte-importer".into(),
                        sources: vec!["source.txt".into()],
                        dest: "imported.bundle".into(),
                        settings: AuthoringValue {
                            canonical_value: Arc::from(&b"3"[..]),
                            blobs: Vec::new(),
                        },
                        watch: true,
                        root: "main".into(),
                    },
                )
                .map_err(|error| format!("{error:?}"))?;
            Ok(prepared.commit)
        })
        .unwrap();
    // Build the import index as the startup pass does.
    coordinator.reconcile_watched_imports(&mut writer).unwrap();

    let mut edit = |content: &[u8]| {
        std::fs::write(assets.join("source.txt"), content).unwrap();
        coordinator
            .reconcile_incremental(&mut writer, &WatcherBatch {
                paths: vec![assets.join("source.txt")],
                renames: Vec::new(),
            })
            .unwrap();
        let work = coordinator.pending_file_work(&mut writer).unwrap();
        coordinator
            .reconcile_watched_imports_affected(&mut writer, &work, false)
            .unwrap();
        coordinator.acknowledge_file_work(&mut writer, &work).unwrap();
    };
    edit(b"broken");
    assert_eq!(import_failures(&coordinator).len(), 1);
    edit(b"7");
    assert!(
        import_failures(&coordinator).is_empty(),
        "the revert reran the import and cleared its failure"
    );
}
