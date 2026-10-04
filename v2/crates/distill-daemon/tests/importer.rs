use std::collections::BTreeMap;
use std::sync::Arc;

use distill_bundle::{AssetEntry, Bundle};
use distill_core::bootstrap::{
    BootstrapControlSpecV1, BootstrapControlSymbol, DIRECTORY_IMPORT_RULES_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_daemon::coordinator::DaemonCoordinator;
use distill_daemon::importer::{AuthoringImportContext, AuthoringImporter, AuthoringImporterError};
use distill_daemon::scanner::AssetRoot;
use distill_daemon::watcher::WatcherBatch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, AuthoringValue, Commit, ImportRequest, InputVersion, TargetDefinition,
    TargetDefinitionHash,
};
use distill_schema::ngp_schema::{
    Field, FieldAttrs, FieldIdentifier, FieldLayout, LayoutIdentity, LogicalSchema, PrimitiveKind,
    PrimitiveType, Schema, SchemaLayouts, SchemaNode, SchemaTypeId, TypeAttrs, TypeDef, TypeLayout,
    TypePath,
};
use distill_schema::ProjectSchemaAuthority;
use distill_store::StoreConfig;

const TYPE_UUID: TypeUuid = TypeUuid([71; 16]);
/// The importers' settings type: a `u8` the importer registers itself.
const SETTINGS_TYPE_UUID: TypeUuid = TypeUuid([70; 16]);

fn settings_schema() -> LogicalSchema {
    LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    }
}

/// The importers' output value: a `TYPE_UUID` struct of one `u8`.
fn byte(value: u128) -> AuthoredValue {
    AuthoredValue::Object(BTreeMap::from([("value".to_owned(), AuthoredValue::UInt(value))]))
}

/// The `u8` of an importer output value.
fn byte_of(value: &AuthoredValue) -> Option<u128> {
    match value {
        AuthoredValue::Object(fields) => match fields.get("value") {
            Some(AuthoredValue::UInt(value)) => Some(*value),
            _ => None,
        },
        _ => None,
    }
}

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
        SETTINGS_TYPE_UUID
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
            .entry("asset", TYPE_UUID, byte(value.into()))
            .map_err(|error| AuthoringImporterError::rejected(4, format!("{error:?}")))?;
        Ok(output)
    }
}

fn ordinary_bundle() -> (Vec<u8>, LogicalSchema, distill_core::id::LogicalHash) {
    let authority = byte_authority();
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

/// Publish a version whose compiled schema authority names `schema_hash` as
/// the current schema of `TYPE_UUID` (a project type of one byte), as a
/// configuration with that schema would: importer outputs are written at
/// it.
fn publish_schema_registry(coordinator: &DaemonCoordinator, schema_hash: LogicalHash) {
    let mut writer = coordinator.open_writer().unwrap();
    let base = coordinator.server().current_stamp().unwrap().version;
    coordinator
        .coordinated_commit(&mut writer, base, |store| {
            store
                .input_transaction(|_| Ok(()))
                .map_err(|error| error.to_string())?;
            Ok(Commit::default())
        })
        .unwrap();
    let authority = byte_authority();
    assert_eq!(
        authority.project_type(TYPE_UUID).unwrap().logical_hash,
        schema_hash
    );
    coordinator.install_schema_authority_for_test(Arc::new(authority));
}

/// A project schema whose one asset type, `TYPE_UUID`, is a struct of one
/// `u8` field `value`.
fn type_def(
    id: usize,
    kind: PrimitiveType,
    krate: &str,
    name: &str,
    uuid: Option<TypeUuid>,
    fields: Vec<Field>,
) -> TypeDef {
    TypeDef {
        id: SchemaTypeId(id),
        kind,
        path: TypePath {
            name: Some(name.to_owned()),
            containing_type: None,
            modules: Vec::new(),
            krate: krate.to_owned(),
        },
        uuid,
        attrs: TypeAttrs::default(),
        fields,
        generic_parameters: Vec::new(),
        generic_argument_ids: Vec::new(),
        has_default: true,
        generic_const_arguments: Vec::new(),
        has_explicit_discriminants: false,
    }
}

fn byte_authority() -> ProjectSchemaAuthority {
    ProjectSchemaAuthority::from_schema(
        Schema {
            source_hashes: BTreeMap::new(),
            type_ops_hash: String::new(),
            layout_hashes: Default::default(),
            rustc_version: String::new(),
            types: vec![
                type_def(
                    0,
                    PrimitiveType::Struct,
                    "game",
                    "Byte",
                    Some(TYPE_UUID),
                    vec![Field {
                        id: FieldIdentifier::Name("value".to_owned()),
                        type_id: SchemaTypeId(1),
                        attrs: FieldAttrs::default(),
                    }],
                ),
                type_def(1, PrimitiveType::U8, "core", "u8", None, Vec::new()),
            ],
            layouts: vec![SchemaLayouts {
                identity: LayoutIdentity {
                    target_triple: "x86_64-unknown-linux-gnu".into(),
                    rustc: "rustc test".into(),
                    algorithm_version: 1,
                },
                layouts: vec![
                    TypeLayout {
                        size: Some(1),
                        align: Some(1),
                        layout_complete: true,
                        tag_encoding: None,
                        fields: vec![FieldLayout {
                            offset: Some(0),
                            field_size: Some(1),
                        }],
                    },
                    TypeLayout {
                        size: Some(1),
                        align: Some(1),
                        layout_complete: true,
                        tag_encoding: None,
                        fields: Vec::new(),
                    },
                ],
            }],
        },
        [10; 32],
    )
    .unwrap()
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
    let (ordinary, _, schema_hash) = ordinary_bundle();
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
        .register_importer(Arc::new(ByteImporter {
            schema: settings_schema(),
        }))
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
    assert_eq!(first.assets["asset"].data, byte(7));
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
    assert_eq!(second.assets["asset"].data, byte(8));
    assert_eq!(second.assets["$settings"].data, AuthoredValue::UInt(3));
    assert_eq!(
        coordinator.open_reader().unwrap().input_version().unwrap(),
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
        coordinator.open_reader().unwrap().input_version().unwrap(),
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
        coordinator.open_reader().unwrap().memo_seq().unwrap(),
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
    assert_eq!(healed.assets["asset"].data, byte(9));
    assert!(import_failures(&coordinator).is_empty());
    assert_eq!(
        coordinator.open_reader().unwrap().input_version().unwrap(),
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
    let (ordinary, _, schema_hash) = ordinary_bundle();
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
        .register_importer(Arc::new(ByteImporter {
            schema: settings_schema(),
        }))
        .unwrap();

    let imported = coordinator.reconcile_directory_imports(&mut writer).unwrap();
    assert_eq!(imported.len(), 1);
    let generated_path = assets.join("foo.bundle");
    let generated = distill_bundle::parse_bundle(&std::fs::read(&generated_path).unwrap()).unwrap();
    assert_eq!(generated.assets["asset"].data, byte(9));
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
    let (ordinary, _, schema_hash) = ordinary_bundle();
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
                schema: settings_schema(),
            }))
            .unwrap();
        let backend = Arc::clone(coordinator.authoring_service());
        let base = coordinator.server().current_stamp().unwrap().version;
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
    // The schema authority is the configuration's, compiled again on open.
    coordinator.install_schema_authority_for_test(Arc::new(byte_authority()));
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

    coordinator
        .authoring_service()
        .register_importer(Arc::new(ByteImporter {
            schema: settings_schema(),
        }))
        .unwrap();
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
    let (ordinary, _, schema_hash) = ordinary_bundle();
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
        .register_importer(Arc::new(ByteImporter {
            schema: settings_schema(),
        }))
        .unwrap();
    let backend = Arc::clone(coordinator.authoring_service());
    let base = coordinator.server().current_stamp().unwrap().version;
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

/// [`ByteImporter`] that takes `value` × 10 ms to import. While `gate` holds
/// its channels, the run of `gated` reports that it started and waits to be
/// released, once.
struct PacedImporter {
    schema: LogicalSchema,
    gated: u8,
    gate: std::sync::Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

impl PacedImporter {
    fn new(schema: LogicalSchema) -> Self {
        Self {
            schema,
            gated: 0,
            gate: std::sync::Mutex::new(None),
        }
    }

    /// Hold the run of `value` until the returned sender releases it; the
    /// returned receiver reports that it started.
    fn gated(
        schema: LogicalSchema,
        value: u8,
    ) -> (Self, std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (started, entered) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        (
            Self {
                schema,
                gated: value,
                gate: std::sync::Mutex::new(Some((started, released))),
            },
            entered,
            release,
        )
    }
}

impl AuthoringImporter for PacedImporter {
    fn id(&self) -> &str {
        "byte-importer"
    }

    fn version(&self) -> u32 {
        1
    }

    fn settings_type_uuid(&self) -> TypeUuid {
        SETTINGS_TYPE_UUID
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
        settings: &AuthoredValue,
    ) -> Result<distill_build::import::ImportOutput, AuthoringImporterError> {
        let source = context.sources()[0].path.clone();
        let value = std::str::from_utf8(&context.read(&source)?)
            .ok()
            .and_then(|text| text.parse::<u8>().ok());
        if let Some(value) = value {
            std::thread::sleep(std::time::Duration::from_millis(u64::from(value) * 10));
            if value == self.gated {
                let gate = self.gate.lock().unwrap().take();
                if let Some((started, release)) = gate {
                    started.send(()).unwrap();
                    release.recv().unwrap();
                }
            }
        }
        ByteImporter {
            schema: self.schema.clone(),
        }
        .import(context, settings)
    }
}

/// A project with directory rules generating `{stem}.bundle` from each of
/// `sources` (stem, contents), imported.
fn imported_sources(
    sources: &[(&str, &str)],
    importer: impl FnOnce(LogicalSchema) -> PacedImporter,
) -> (tempfile::TempDir, std::path::PathBuf, DaemonCoordinator) {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, schema_hash) = ordinary_bundle();
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
    publish_schema_registry(&coordinator, schema_hash);
    coordinator
        .authoring_service()
        .register_importer(Arc::new(importer(settings_schema())))
        .unwrap();
    assert_eq!(
        coordinator.reconcile_directory_imports(&mut writer).unwrap().len(),
        sources.len()
    );
    // Earlier passes consumed the setup's watcher work.
    let work = coordinator.pending_file_work(&mut writer).unwrap();
    assert!(coordinator.acknowledge_file_work(&mut writer, &work).unwrap());
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
        .map(|stem| store.file_content_hash("main", &format!("{stem}.bundle")).unwrap())
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
            assert_eq!(*blake3::hash(&bytes).as_bytes(), hash.0, "{stem}.bundle is the published file");
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

fn batch(assets: &std::path::Path, files: &[&str]) -> WatcherBatch {
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
    let (_temp, assets, coordinator) =
        imported_sources(&[("a", "1"), ("b", "2"), ("c", "3")], PacedImporter::new);
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();
    let old_hashes = generated_hashes(&writer, &stems);
    assert_eq!(generated_values(&writer, &assets, &stems), [Some(1), Some(2), Some(3)]);

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
                seen.push((snapshot.input_version().unwrap(), generated_hashes(&snapshot, &stems)));
                if finished {
                    return seen;
                }
            }
        });
        let outcome = coordinator
            .reconcile_batch(&mut writer, &batch(&assets, &["a.src", "b.src", "c.src"]), false)
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
    assert_eq!(generated_values(&reader, &assets, &stems), [Some(9), Some(5), Some(1)]);
    let new_hashes = generated_hashes(&reader, &stems);
    assert!(old_hashes.iter().zip(&new_hashes).all(|(old, new)| old != new));
    for (observed, hashes) in &seen {
        if *observed == base {
            assert_eq!(hashes, &old_hashes, "the base holds every old bundle");
        } else {
            assert_eq!(*observed, version);
            assert_eq!(hashes, &new_hashes, "the pass's version holds every new bundle");
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
    let (_temp, assets, coordinator) =
        imported_sources(&[("a", "1"), ("b", "2"), ("c", "3")], |schema| {
            let (importer, entered, release) = PacedImporter::gated(schema, 4);
            GATE.with(|gate| *gate.borrow_mut() = Some((entered, release)));
            importer
        });
    let (entered, release) = GATE.with(|gate| gate.borrow_mut().take()).unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();

    std::fs::write(assets.join("a.src"), b"4").unwrap();
    std::fs::write(assets.join("b.src"), b"5").unwrap();
    std::fs::write(assets.join("c.src"), b"6").unwrap();
    let burst = batch(&assets, &["a.src", "b.src", "c.src"]);
    let stale = std::thread::scope(|scope| {
        let pass = scope.spawn(|| {
            let mut writer = coordinator.open_writer().unwrap();
            coordinator.reconcile_batch(&mut writer, &burst, false)
        });
        entered.recv().unwrap();
        // The RPC import commits while the pass's import is running.
        let backend = Arc::clone(coordinator.authoring_service());
        coordinator
            .coordinated_commit(&mut writer, base, |store| {
                backend
                    .prepare_import(
                        store,
                        base,
                        &ImportRequest {
                            importer: "byte-importer".into(),
                            sources: vec!["other.txt".into()],
                            dest: "explicit.bundle".into(),
                            settings: AuthoringValue {
                                canonical_value: Arc::from(&b"3"[..]),
                                blobs: Vec::new(),
                            },
                            watch: true,
                            root: "main".into(),
                        },
                    )
                    .map(|prepared| prepared.commit)
                    .map_err(|error| format!("{error:?}"))
            })
            .unwrap();
        release.send(()).unwrap();
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

    let outcome = coordinator.reconcile_batch(&mut writer, &burst, false).unwrap();
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

thread_local! {
    static GATE: std::cell::RefCell<
        Option<(std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>)>,
    > = const { std::cell::RefCell::new(None) };
}

/// One failing import in a multi-bundle pass: the others publish, the
/// failing bundle keeps its last good contents and records the failure, and
/// a new output whose importer fails is reported, all in one version.
#[test]
fn an_import_failure_in_a_pass_keeps_its_last_good_bundle_while_the_rest_publish() {
    let (_temp, assets, coordinator) =
        imported_sources(&[("a", "1"), ("b", "2"), ("c", "3")], PacedImporter::new);
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

/// An importer whose output is one more than the largest value among its
/// sources that exist: a text source's number, or a bundle source's
/// `asset` entry, so its sources may be other imports' outputs.
struct ChainImporter {
    schema: LogicalSchema,
}

impl AuthoringImporter for ChainImporter {
    fn id(&self) -> &str {
        "chain-importer"
    }

    fn version(&self) -> u32 {
        1
    }

    fn settings_type_uuid(&self) -> TypeUuid {
        SETTINGS_TYPE_UUID
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
        let mut value = 0;
        for source in context.sources().to_vec() {
            if !context.probe(&source.path)? {
                continue;
            }
            let bytes = context.read(&source.path)?;
            let source_value = if source.path.ends_with(".bundle") {
                let bundle = distill_bundle::parse_bundle(&bytes)
                    .map_err(|error| AuthoringImporterError::rejected(5, format!("{error:?}")))?;
                let data = &bundle.assets["asset"].data;
                byte_of(data)
                    .ok_or_else(|| AuthoringImporterError::rejected(6, format!("{data:?}")))?
            } else {
                std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|text| text.parse::<u128>().ok())
                    .ok_or_else(|| AuthoringImporterError::rejected(3, "not a number"))?
            };
            value = value.max(source_value);
        }
        let mut output = distill_build::import::ImportOutput::new();
        output
            .entry("asset", TYPE_UUID, byte(value + 1))
            .map_err(|error| AuthoringImporterError::rejected(4, format!("{error:?}")))?;
        Ok(output)
    }
}

/// Import `dest` from `sources` with [`ChainImporter`], watched, as one
/// RPC publication, and pass over the watcher work it leaves.
fn chain_import(coordinator: &DaemonCoordinator, assets: &std::path::Path, dest: &str, sources: &[&str]) {
    let mut writer = coordinator.open_writer().unwrap();
    let base = writer.input_version().unwrap();
    let backend = Arc::clone(coordinator.authoring_service());
    coordinator
        .coordinated_commit(&mut writer, base, |store| {
            backend
                .prepare_import(
                    store,
                    base,
                    &ImportRequest {
                        importer: "chain-importer".into(),
                        sources: sources.iter().map(|source| (*source).to_owned()).collect(),
                        dest: dest.into(),
                        settings: AuthoringValue {
                            canonical_value: Arc::from(&b"0"[..]),
                            blobs: Vec::new(),
                        },
                        watch: true,
                        root: "main".into(),
                    },
                )
                .map(|prepared| prepared.commit)
                .map_err(|error| format!("{error:?}"))
        })
        .unwrap();
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
    let (temp, assets, coordinator) = imported_sources(&[("a", "1")], PacedImporter::new);
    coordinator
        .authoring_service()
        .register_importer(Arc::new(ChainImporter { schema: settings_schema() }))
        .unwrap();
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
    assert_eq!(echo.stamp.version, version, "nothing drifted to another pass");
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
        first.failures[0].contains("deeper than 8 levels") && first.failures[0].contains("l9.bundle"),
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
    assert!(authoring.watched_imports_needing_reimport(&mut writer).unwrap().is_empty());
    let work = coordinator.pending_file_work(&mut writer).unwrap();
    assert!(coordinator.acknowledge_file_work(&mut writer, &work).unwrap());

    let before = writer.pages_fetched().unwrap();
    assert!(authoring.watched_imports_needing_reimport(&mut writer).unwrap().is_empty());
    writer.pages_fetched().unwrap() - before
}

/// The import index is kept by the dirty work bundle publication queues: a
/// whole-namespace import check with no pending work reindexes nothing.
#[test]
fn an_indexed_namespace_is_not_reindexed() {
    let small = import_index_pages(20);
    let large = import_index_pages(2000);
    println!("watched-import check: {small} pages beside 20 bundles, {large} beside 2000");
    assert!(large <= small + 16, "{small} pages beside 20 bundles, {large} beside 2000");
}

/// An RPC reimport whose importer fails memoizes the failure: the memo
/// commits, with no new version, and the client gets the failure.
#[test]
fn a_failed_rpc_reimport_commits_only_its_memo() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (ordinary, _, schema_hash) = ordinary_bundle();
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
        .register_importer(Arc::new(ByteImporter {
            schema: settings_schema(),
        }))
        .unwrap();
    let backend = Arc::clone(coordinator.authoring_service());
    let base = coordinator.server().current_stamp().unwrap().version;
    let mut bundle = None;
    coordinator
        .coordinated_commit(&mut writer, base, |store| {
            let prepared = backend
                .prepare_import(
                    store,
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
            bundle = Some(prepared.bundle);
            Ok(prepared.commit)
        })
        .unwrap();
    std::fs::write(assets.join("source.txt"), b"broken").unwrap();
    coordinator
        .reconcile_incremental(&mut writer, &WatcherBatch {
            paths: vec![assets.join("source.txt")],
            renames: Vec::new(),
        })
        .unwrap();
    assert!(import_failures(&coordinator).is_empty());

    let base = coordinator.server().current_stamp().unwrap().version;
    let hub = match coordinator
        .server()
        .root()
        .connect(distill_rpc::ConnectRequest::new("dev", TargetDefinitionHash([4; 32])))
    {
        distill_rpc::ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("expected connection, got {other:?}"),
    };
    let reimport = hub.reimport(base, bundle.unwrap());
    assert!(matches!(reimport, distill_rpc::RpcResult::Failure(_)), "{reimport:?}");
    assert_eq!(coordinator.server().current_stamp().unwrap().version, base);
    assert_eq!(import_failures(&coordinator).len(), 1, "the failure memo committed");
}
