use std::collections::BTreeMap;
use std::sync::Arc;

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::attestation::{
    BootstrapControlSpecV1, BootstrapControlSymbol, DIRECTORY_IMPORT_RULES_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_daemon::coordinator::{DaemonCoordinator, LineageDestination};
use distill_daemon::importer::{AuthoringImportContext, AuthoringImporter};
use distill_daemon::scanner::AssetRoot;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, AuthoringValue, Commit, ImportRequest, InputVersion, LoadPolicyEntry,
    TargetDefinition, TargetDefinitionHash,
};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use distill_store::pipeline::{
    AcceptedTypeLineage, SchemaLineageManifest, TypeAuthorityState, VerifiedSchemaLineageManifest,
};
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
    ) -> Result<distill_build::import::ImportOutput, String> {
        let source = context
            .sources()
            .first()
            .ok_or_else(|| "one source is required".to_owned())?
            .path
            .clone();
        let bytes = context
            .read(&source)
            .map_err(|error| format!("{error:?}"))?;
        let value = std::str::from_utf8(&bytes)
            .map_err(|error| error.to_string())?
            .parse::<u8>()
            .map_err(|error| error.to_string())?;
        let mut output = distill_build::import::ImportOutput::new();
        output
            .entry("asset", TYPE_UUID, AuthoredValue::UInt(value.into()))
            .map_err(|error| format!("{error:?}"))?;
        Ok(output)
    }
}

fn ordinary_bundle() -> (Vec<u8>, LogicalSchema, distill_core::id::LogicalHash) {
    let schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    };
    let schema_hash = node_hash(&schema.root).unwrap();
    let epochs = vec![AcceptedSchemaEpoch {
        digest: schema_hash,
        forward_parent: None,
    }];
    let entry = AssetEntry {
        uuid: AssetUuid([72; 16]),
        type_uuid: TYPE_UUID,
        schema_hash,
        lineage: EntryLineageV1::Manifest(LineageStamp {
            chain: lineage_chain_digest(TYPE_UUID, &epochs, 0),
            epochs,
            cursor: 0,
        }),
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
    let rows = distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1()
        .unwrap()
        .rows()
        .to_vec();
    let policy = rows
        .iter()
        .map(|row| LoadPolicyEntry {
            type_uuid: row.type_uuid,
            build_only: row.build_only,
        })
        .collect();
    TargetDefinition::canonical("dev", TargetDefinitionHash([4; 32]), rows, policy).unwrap()
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
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::DirectoryImportRules)
        .unwrap();
    let schema =
        distill_daemon::logical_node::decode_logical_schema_bytes(&row.logical_schema).unwrap();
    let query = || {
        object([
            ("path_glob", AuthoredValue::Str("*.src".into())),
            ("path_prefix", AuthoredValue::Null),
        ])
    };
    let data = object([
        ("listing", query()),
        (
            "rules",
            AuthoredValue::Array(vec![object([
                ("group", object([("PerFile", object([]))])),
                ("id", bytes([97; 16])),
                ("importer", AuthoredValue::Str("byte-importer".into())),
                ("matches", query()),
                ("output", AuthoredValue::Str("{stem}.bundle".into())),
                (
                    "settings",
                    object([("UInt", object([("value", AuthoredValue::UInt(5))]))]),
                ),
            ])]),
        ),
    ]);
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
                lineage: EntryLineageV1::Bootstrap {
                    bundle_format_version: 1,
                },
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
        vec![AssetRoot::new(
            "main",
            &assets,
            assets.join(".distill-displaced"),
        )],
        LineageDestination {
            root: "main".into(),
            path: "schema/schema-lineage.bundle".into(),
        },
        vec![target()],
    )
    .unwrap();
    coordinator.reconcile_full_scan().unwrap();

    let store = coordinator.store();
    let manifest = VerifiedSchemaLineageManifest::from_verified_source(
        ContentHash([9; 32]),
        SchemaLineageManifest {
            types: BTreeMap::from([(
                TYPE_UUID,
                AcceptedTypeLineage {
                    epochs: vec![AcceptedSchemaEpoch {
                        digest: schema_hash,
                        forward_parent: None,
                    }],
                    current: 0,
                    authority: TypeAuthorityState::Active,
                },
            )]),
        },
    );
    coordinator
        .server()
        .coordinated_commit(InputVersion(1), || {
            store
                .lock()
                .unwrap()
                .input_transaction(|transaction| {
                    transaction.project_verified_lineage_manifest(&manifest)
                })
                .map_err(|error| error.to_string())?;
            Ok(Commit::default())
        })
        .unwrap();

    coordinator
        .authoring_service()
        .register_importer(Arc::new(ByteImporter { schema }))
        .unwrap();
    let backend = Arc::clone(coordinator.authoring_service());
    let imported_bundle = Arc::new(std::sync::Mutex::new(None));
    let captured = Arc::clone(&imported_bundle);
    coordinator
        .server()
        .coordinated_commit(InputVersion(2), || {
            let prepared = backend
                .prepare_import(
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
    assert!(coordinator
        .authoring_service()
        .watched_imports_needing_reimport()
        .unwrap()
        .is_empty());

    std::fs::write(assets.join("source.txt"), b"8").unwrap();
    assert_eq!(
        coordinator
            .authoring_service()
            .watched_imports_needing_reimport()
            .unwrap(),
        vec![imported_bundle]
    );
    assert_eq!(
        coordinator.reconcile_watched_imports().unwrap(),
        vec![imported_bundle]
    );
    let second = distill_bundle::parse_bundle(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(second.uuid, imported_bundle);
    assert_eq!(second.assets["asset"].uuid, first_asset);
    assert_eq!(second.assets["asset"].data, AuthoredValue::UInt(8));
    assert_eq!(second.assets["$settings"].data, AuthoredValue::UInt(3));
    assert_eq!(
        coordinator.store().lock().unwrap().input_version(),
        InputVersion(4)
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
        vec![AssetRoot::new(
            "main",
            &assets,
            assets.join(".distill-displaced"),
        )],
        LineageDestination {
            root: "main".into(),
            path: "schema/schema-lineage.bundle".into(),
        },
        vec![target()],
    )
    .unwrap();
    coordinator.reconcile_full_scan().unwrap();
    let store = coordinator.store();
    let manifest = VerifiedSchemaLineageManifest::from_verified_source(
        ContentHash([9; 32]),
        SchemaLineageManifest {
            types: BTreeMap::from([(
                TYPE_UUID,
                AcceptedTypeLineage {
                    epochs: vec![AcceptedSchemaEpoch {
                        digest: schema_hash,
                        forward_parent: None,
                    }],
                    current: 0,
                    authority: TypeAuthorityState::Active,
                },
            )]),
        },
    );
    coordinator
        .server()
        .coordinated_commit(InputVersion(1), || {
            store
                .lock()
                .unwrap()
                .input_transaction(|transaction| {
                    transaction.project_verified_lineage_manifest(&manifest)
                })
                .map_err(|error| error.to_string())?;
            Ok(Commit::default())
        })
        .unwrap();
    coordinator
        .authoring_service()
        .register_importer(Arc::new(ByteImporter { schema }))
        .unwrap();

    let imported = coordinator.reconcile_directory_imports().unwrap();
    assert_eq!(imported.len(), 1);
    let generated_path = assets.join("foo.bundle");
    let generated = distill_bundle::parse_bundle(&std::fs::read(&generated_path).unwrap()).unwrap();
    assert_eq!(generated.assets["asset"].data, AuthoredValue::UInt(9));
    assert_eq!(generated.assets["$settings"].data, AuthoredValue::UInt(5));
    let meta = coordinator
        .store()
        .lock()
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
        .reconcile_directory_imports()
        .unwrap()
        .is_empty());

    std::fs::remove_file(assets.join("foo.src")).unwrap();
    coordinator.reconcile_full_scan().unwrap();
    assert!(coordinator
        .reconcile_directory_imports()
        .unwrap()
        .is_empty());
    assert!(
        generated_path.exists(),
        "lost groups are orphaned, never deleted"
    );
}
