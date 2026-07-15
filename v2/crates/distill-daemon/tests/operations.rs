use std::collections::BTreeMap;

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_daemon::coordinator::{DaemonCoordinator, LineageDestination};
use distill_daemon::scanner::AssetRoot;
use distill_json::AuthoredValue;
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
        256,
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
fn disk_migration_applies_a_verified_forward_automatic_plan() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let old_schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    };
    let new_schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U16),
    };
    let old_hash = node_hash(&old_schema.root).unwrap();
    let new_hash = node_hash(&new_schema.root).unwrap();
    std::fs::write(
        assets.join("value.bundle"),
        bundle(
            BundleUuid([91; 16]),
            AssetUuid([92; 16]),
            VALUE_TYPE,
            old_schema,
            AuthoredValue::UInt(7),
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
        256,
    )
    .unwrap();
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
    assert_eq!(entry.data, AuthoredValue::UInt(7));
    assert!(matches!(
        &entry.lineage,
        EntryLineageV1::Manifest(stamp) if stamp.cursor == 1 && stamp.selected_digest() == Some(new_hash)
    ));
    assert_eq!(
        coordinator.store().lock().unwrap().input_version(),
        InputVersion(3)
    );
}
