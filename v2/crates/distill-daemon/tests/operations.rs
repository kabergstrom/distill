use std::collections::BTreeMap;
use std::sync::Arc;

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::bootstrap::{BootstrapControlSpecV1, BootstrapControlSymbol};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_daemon::coordinator::{DaemonCoordinator, LineageDestination};
use distill_daemon::scanner::AssetRoot;
use distill_json::AuthoredValue;
use distill_migrate::FieldPath;
use distill_rpc::{
    AuthoringBackend, Commit, DeferredOperationResult, DiskMigrationRequest, InputVersion,
    LongRunningOp, PreparedOperationPublication, RenameWithFixupsRequest, TargetDefinition,
    TargetDefinitionHash,
};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use distill_store::pipeline::{
    AcceptedTypeLineage, SchemaLineageManifest, TypeAuthorityState, VerifiedSchemaLineageManifest,
};
use distill_store::StoreConfig;

const VALUE_TYPE: TypeUuid = TypeUuid([81; 16]);
const REF_TYPE: TypeUuid = TypeUuid([82; 16]);
const MIGRATION_BUNDLE: BundleUuid = BundleUuid([93; 16]);
const MIGRATION_ASSET: AssetUuid = AssetUuid([94; 16]);

fn lineage(type_uuid: TypeUuid, schema: distill_core::id::LogicalHash) -> EntryLineageV1 {
    let epochs = vec![AcceptedSchemaEpoch {
        digest: schema,
        forward_parent: None,
    }];
    EntryLineageV1::Manifest(LineageStamp {
        chain: lineage_chain_digest(type_uuid, &epochs, 0),
        epochs,
        cursor: 0,
    })
}

fn bundle(
    bundle: BundleUuid,
    asset: AssetUuid,
    type_uuid: TypeUuid,
    schema: LogicalSchema,
    value: AuthoredValue,
) -> Vec<u8> {
    let hash = node_hash(&schema.root).unwrap();
    distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: bundle,
        primary: Some("entry".into()),
        schemas: BTreeMap::from([(hash, schema)]),
        assets: BTreeMap::from([(
            "entry".into(),
            AssetEntry {
                uuid: asset,
                type_uuid,
                schema_hash: hash,
                lineage: lineage(type_uuid, hash),
                authoring_only: false,
                data: value,
            },
        )]),
    })
    .unwrap()
}

fn object(fields: impl IntoIterator<Item = (&'static str, AuthoredValue)>) -> AuthoredValue {
    AuthoredValue::Object(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

fn variant(name: &'static str, fields: Vec<(&'static str, AuthoredValue)>) -> AuthoredValue {
    object([(name, object(fields))])
}

fn bytes(value: &[u8]) -> AuthoredValue {
    AuthoredValue::Array(
        value
            .iter()
            .map(|byte| AuthoredValue::UInt(u128::from(*byte)))
            .collect(),
    )
}

fn path(value: &FieldPath) -> AuthoredValue {
    object([(
        "segments",
        AuthoredValue::Array(
            value
                .0
                .iter()
                .map(|segment| AuthoredValue::Str(segment.clone()))
                .collect(),
        ),
    )])
}

fn neutral(value: &AuthoredValue) -> AuthoredValue {
    match value {
        AuthoredValue::UInt(value) => variant("UInt", vec![("value", AuthoredValue::UInt(*value))]),
        _ => panic!("operation test encodes only unsigned migration literals"),
    }
}

fn authored_lineage(
    type_uuid: TypeUuid,
    epochs: &[AcceptedSchemaEpoch],
    cursor: u32,
) -> AuthoredValue {
    object([
        (
            "chain",
            bytes(&lineage_chain_digest(type_uuid, epochs, cursor)),
        ),
        ("cursor", AuthoredValue::UInt(u128::from(cursor))),
        (
            "epochs",
            AuthoredValue::Array(
                epochs
                    .iter()
                    .map(|epoch| {
                        object([
                            ("digest", bytes(&epoch.digest.0)),
                            (
                                "forward_parent",
                                epoch.forward_parent.map_or(AuthoredValue::Null, |parent| {
                                    AuthoredValue::UInt(u128::from(parent))
                                }),
                            ),
                        ])
                    })
                    .collect(),
            ),
        ),
    ])
}

fn custom_migration_bundle(
    old_hash: LogicalHash,
    old_schema: &LogicalSchema,
    new_hash: LogicalHash,
    new_schema: &LogicalSchema,
) -> Vec<u8> {
    let old_epochs = vec![AcceptedSchemaEpoch {
        digest: old_hash,
        forward_parent: None,
    }];
    let new_epochs = vec![
        old_epochs[0],
        AcceptedSchemaEpoch {
            digest: new_hash,
            forward_parent: Some(0),
        },
    ];
    let at = FieldPath(vec!["value".to_owned()]);
    let ops = AuthoredValue::Array(vec![
        variant("DropField", vec![("at", path(&at))]),
        variant(
            "WriteValue",
            vec![
                ("to", path(&at)),
                ("value", neutral(&AuthoredValue::UInt(42))),
            ],
        ),
    ]);
    let migration = object([
        ("from_hash", bytes(&old_hash.0)),
        ("from_lineage", authored_lineage(VALUE_TYPE, &old_epochs, 0)),
        ("kind", variant("Ops", vec![("ops", ops)])),
        ("target_type_uuid", bytes(&VALUE_TYPE.0)),
        ("to_hash", bytes(&new_hash.0)),
        ("to_lineage", authored_lineage(VALUE_TYPE, &new_epochs, 1)),
    ]);
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::Migration)
        .unwrap();
    let migration_schema =
        distill_schema::ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
    distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: MIGRATION_BUNDLE,
        primary: None,
        schemas: BTreeMap::from([
            (old_hash, old_schema.clone()),
            (new_hash, new_schema.clone()),
            (row.logical_hash, migration_schema),
        ]),
        assets: BTreeMap::from([(
            "migration".to_owned(),
            AssetEntry {
                uuid: MIGRATION_ASSET,
                type_uuid: row.type_uuid,
                schema_hash: row.logical_hash,
                lineage: EntryLineageV1::Bootstrap {
                    bundle_format_version: 1,
                },
                authoring_only: true,
                data: migration,
            },
        )]),
    })
    .unwrap()
}

fn target() -> TargetDefinition {
    TargetDefinition::new("dev", TargetDefinitionHash([8; 32]))
}

fn complete(
    publication: PreparedOperationPublication,
    base: InputVersion,
) -> DeferredOperationResult {
    match publication {
        PreparedOperationPublication::Deferred(operation) => operation.complete(base).unwrap(),
        PreparedOperationPublication::Immediate(_) => panic!("production operations are deferred"),
    }
}

#[test]
fn rename_with_fixups_is_deferred_journaled_and_rescanned_as_one_version() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("old.bundle"),
        bundle(
            BundleUuid([83; 16]),
            AssetUuid([84; 16]),
            VALUE_TYPE,
            LogicalSchema {
                root: SchemaNode::Primitive(PrimitiveKind::U8),
            },
            AuthoredValue::UInt(7),
        ),
    )
    .unwrap();
    std::fs::write(
        assets.join("consumer.bundle"),
        bundle(
            BundleUuid([85; 16]),
            AssetUuid([86; 16]),
            REF_TYPE,
            LogicalSchema {
                root: SchemaNode::AssetRef(VALUE_TYPE),
            },
            AuthoredValue::Object(BTreeMap::from([
                ("asset".into(), AuthoredValue::Str("entry".into())),
                ("path".into(), AuthoredValue::Str("old.bundle".into())),
            ])),
        ),
    )
    .unwrap();

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
        64,
    )
    .unwrap();
    coordinator.reconcile_full_scan().unwrap();
    let base = InputVersion(1);
    let request = RenameWithFixupsRequest {
        bundle: BundleUuid([83; 16]),
        destination_root: "main".into(),
        destination_path: "renamed.bundle".into(),
    };
    let prepared = coordinator
        .authoring_service()
        .prepare_operation(base, &LongRunningOp::RenameWithFixups(request.encode()))
        .unwrap();

    assert!(assets.join("old.bundle").exists());
    assert!(!assets.join("renamed.bundle").exists());
    let completed = complete(prepared.publication, base);
    assert_eq!(completed.terminal_error, None);
    coordinator
        .server()
        .coordinated_commit(base, || Ok(completed.commit))
        .unwrap();

    assert!(!assets.join("old.bundle").exists());
    assert!(assets.join("renamed.bundle").exists());
    let consumer =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("consumer.bundle")).unwrap())
            .unwrap();
    let AuthoredValue::Object(reference) = &consumer.assets["entry"].data else {
        panic!("reference remains an object")
    };
    assert_eq!(
        reference["path"],
        AuthoredValue::Str("renamed.bundle".into())
    );
    assert_eq!(
        coordinator.store().lock().unwrap().input_version(),
        InputVersion(2)
    );
    assert_eq!(
        coordinator.server().current_stamp().version,
        InputVersion(2)
    );
}

#[test]
fn disk_migration_uses_the_shared_loader_and_prefers_a_custom_edge() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let old_schema = LogicalSchema {
        root: SchemaNode::Struct {
            rev: 0,
            fields: vec![(
                "value".to_owned(),
                0,
                SchemaNode::Primitive(PrimitiveKind::U8),
            )],
        },
    };
    let new_schema = LogicalSchema {
        root: SchemaNode::Struct {
            rev: 0,
            fields: vec![(
                "value".to_owned(),
                0,
                SchemaNode::Primitive(PrimitiveKind::U16),
            )],
        },
    };
    let old_hash = node_hash(&old_schema.root).unwrap();
    let new_hash = node_hash(&new_schema.root).unwrap();
    // Keep the custom edge in the same bundle as the authored value. Disk
    // migration compacts this bundle's schema table after rewriting the value,
    // so the MigrationV1 endpoint snapshots must survive that compaction.
    let mut value_bundle = distill_bundle::parse_bundle(&bundle(
        BundleUuid([91; 16]),
        AssetUuid([92; 16]),
        VALUE_TYPE,
        old_schema.clone(),
        object([("value", AuthoredValue::UInt(7))]),
    ))
    .unwrap();
    let migration_bundle = distill_bundle::parse_bundle(&custom_migration_bundle(
        old_hash,
        &old_schema,
        new_hash,
        &new_schema,
    ))
    .unwrap();
    value_bundle.schemas.extend(migration_bundle.schemas);
    value_bundle.assets.extend(migration_bundle.assets);
    std::fs::write(
        assets.join("value.bundle"),
        distill_bundle::write_bundle(&value_bundle).unwrap(),
    )
    .unwrap();
    let coordinator = Arc::new(
        DaemonCoordinator::open(
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
            64,
        )
        .unwrap(),
    );
    coordinator.attach_build_backend();
    coordinator.reconcile_full_scan().unwrap();

    let manifest = VerifiedSchemaLineageManifest::from_verified_source(
        ContentHash([3; 32]),
        SchemaLineageManifest {
            types: BTreeMap::from([(
                VALUE_TYPE,
                AcceptedTypeLineage {
                    epochs: vec![
                        AcceptedSchemaEpoch {
                            digest: old_hash,
                            forward_parent: None,
                        },
                        AcceptedSchemaEpoch {
                            digest: new_hash,
                            forward_parent: Some(0),
                        },
                    ],
                    current: 1,
                    authority: TypeAuthorityState::Active,
                },
            )]),
        },
    );
    let store = coordinator.store();
    let new_snapshot = distill_schema::ngp_schema::snapshot_to_json(&new_schema).unwrap();
    coordinator
        .server()
        .coordinated_commit(InputVersion(1), || {
            store
                .lock()
                .unwrap()
                .input_transaction(|transaction| {
                    transaction.put_schema(new_hash, &new_snapshot)?;
                    transaction.project_verified_lineage_manifest(&manifest)
                })
                .map_err(|error| error.to_string())?;
            Ok(Commit::default())
        })
        .unwrap();

    let base = InputVersion(2);
    let request = DiskMigrationRequest {
        bundles: vec![BundleUuid([91; 16])],
    };
    let prepared = coordinator
        .authoring_service()
        .prepare_operation(
            base,
            &LongRunningOp::DiskMigration(request.encode().unwrap()),
        )
        .unwrap();
    let completed = complete(prepared.publication, base);
    assert_eq!(completed.terminal_error, None);
    coordinator
        .server()
        .coordinated_commit(base, || Ok(completed.commit))
        .unwrap();

    let migrated =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("value.bundle")).unwrap()).unwrap();
    let entry = &migrated.assets["entry"];
    assert_eq!(entry.schema_hash, new_hash);
    assert_eq!(entry.data, object([("value", AuthoredValue::UInt(42))]));
    assert!(matches!(
        &entry.lineage,
        EntryLineageV1::Manifest(stamp) if stamp.cursor == 1 && stamp.selected_digest() == Some(new_hash)
    ));
    assert!(migrated.schemas.contains_key(&old_hash));
    assert!(migrated.schemas.contains_key(&new_hash));
    assert_eq!(
        coordinator.store().lock().unwrap().input_version(),
        InputVersion(3)
    );
}

#[test]
fn disk_migration_reports_one_bundle_failure_and_still_rewrites_later_bundles() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let old_schema = LogicalSchema {
        root: SchemaNode::Struct {
            rev: 0,
            fields: vec![(
                "value".to_owned(),
                0,
                SchemaNode::Primitive(PrimitiveKind::U8),
            )],
        },
    };
    let new_schema = LogicalSchema {
        root: SchemaNode::Struct {
            rev: 0,
            fields: vec![(
                "value".to_owned(),
                0,
                SchemaNode::Primitive(PrimitiveKind::U16),
            )],
        },
    };
    let old_hash = node_hash(&old_schema.root).unwrap();
    let new_hash = node_hash(&new_schema.root).unwrap();
    let failing_bundle = BundleUuid([90; 16]);
    let migrating_bundle = BundleUuid([91; 16]);
    let failing_bytes = bundle(
        failing_bundle,
        AssetUuid([89; 16]),
        REF_TYPE,
        old_schema.clone(),
        object([("value", AuthoredValue::UInt(3))]),
    );
    std::fs::write(assets.join("failing.bundle"), &failing_bytes).unwrap();

    let mut migrating = distill_bundle::parse_bundle(&bundle(
        migrating_bundle,
        AssetUuid([92; 16]),
        VALUE_TYPE,
        old_schema.clone(),
        object([("value", AuthoredValue::UInt(7))]),
    ))
    .unwrap();
    let migration = distill_bundle::parse_bundle(&custom_migration_bundle(
        old_hash,
        &old_schema,
        new_hash,
        &new_schema,
    ))
    .unwrap();
    migrating.schemas.extend(migration.schemas);
    migrating.assets.extend(migration.assets);
    std::fs::write(
        assets.join("migrating.bundle"),
        distill_bundle::write_bundle(&migrating).unwrap(),
    )
    .unwrap();

    let coordinator = Arc::new(
        DaemonCoordinator::open(
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
            64,
        )
        .unwrap(),
    );
    coordinator.attach_build_backend();
    coordinator.reconcile_full_scan().unwrap();

    let manifest = VerifiedSchemaLineageManifest::from_verified_source(
        ContentHash([3; 32]),
        SchemaLineageManifest {
            types: BTreeMap::from([(
                VALUE_TYPE,
                AcceptedTypeLineage {
                    epochs: vec![
                        AcceptedSchemaEpoch {
                            digest: old_hash,
                            forward_parent: None,
                        },
                        AcceptedSchemaEpoch {
                            digest: new_hash,
                            forward_parent: Some(0),
                        },
                    ],
                    current: 1,
                    authority: TypeAuthorityState::Active,
                },
            )]),
        },
    );
    let store = coordinator.store();
    let new_snapshot = distill_schema::ngp_schema::snapshot_to_json(&new_schema).unwrap();
    coordinator
        .server()
        .coordinated_commit(InputVersion(1), || {
            store
                .lock()
                .unwrap()
                .input_transaction(|transaction| {
                    transaction.put_schema(new_hash, &new_snapshot)?;
                    transaction.project_verified_lineage_manifest(&manifest)
                })
                .map_err(|error| error.to_string())?;
            Ok(Commit::default())
        })
        .unwrap();

    let base = InputVersion(2);
    let request = DiskMigrationRequest {
        bundles: vec![failing_bundle, migrating_bundle],
    };
    let prepared = coordinator
        .authoring_service()
        .prepare_operation(
            base,
            &LongRunningOp::DiskMigration(request.encode().unwrap()),
        )
        .unwrap();
    let completed = complete(prepared.publication, base);
    let terminal_error = completed
        .terminal_error
        .as_deref()
        .expect("the failed bundle is reported");
    assert!(terminal_error.contains(&failing_bundle.to_string()));
    assert!(terminal_error.contains("failing.bundle"));
    assert!(terminal_error.contains("has no accepted lineage"));
    assert!(!terminal_error.contains(&migrating_bundle.to_string()));
    coordinator
        .server()
        .coordinated_commit(base, || Ok(completed.commit))
        .unwrap();

    assert_eq!(
        std::fs::read(assets.join("failing.bundle")).unwrap(),
        failing_bytes
    );
    let failed = distill_bundle::parse_bundle(&failing_bytes).unwrap();
    assert_eq!(failed.assets["entry"].schema_hash, old_hash);
    let migrated =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("migrating.bundle")).unwrap())
            .unwrap();
    assert_eq!(migrated.assets["entry"].schema_hash, new_hash);
    assert_eq!(
        migrated.assets["entry"].data,
        object([("value", AuthoredValue::UInt(42))])
    );
    assert_eq!(
        coordinator.store().lock().unwrap().input_version(),
        InputVersion(3)
    );
}
