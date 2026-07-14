use std::collections::BTreeMap;

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_daemon::coordinator::{DaemonCoordinator, LineageDestination, WatcherPathEvent};
use distill_daemon::scanner::AssetRoot;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringInspectResult, LoadPolicyEntry, MetadataCall, MetadataNamespaceCall, TargetDefinition,
    TargetDefinitionHash,
};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use distill_store::state::{ConfigurationState, DscpV1, InputVersion, VersionPoisonV1};
use distill_store::StoreConfig;

fn ordinary_bundle() -> (Vec<u8>, BundleUuid, AssetUuid) {
    let type_uuid = TypeUuid([71; 16]);
    let schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    };
    let schema_hash = node_hash(&schema.root).unwrap();
    let epochs = vec![AcceptedSchemaEpoch {
        digest: schema_hash,
        forward_parent: None,
    }];
    let asset = AssetUuid([72; 16]);
    let bundle = BundleUuid([73; 16]);
    let entry = AssetEntry {
        uuid: asset,
        type_uuid,
        schema_hash,
        lineage: EntryLineageV1::Manifest(LineageStamp {
            chain: lineage_chain_digest(type_uuid, &epochs, 0),
            epochs,
            cursor: 0,
        }),
        authoring_only: false,
        data: AuthoredValue::UInt(7),
    };
    (
        distill_bundle::write_bundle(&Bundle {
            format_version: 1,
            uuid: bundle,
            primary: Some("entry".into()),
            schemas: BTreeMap::from([(schema_hash, schema)]),
            assets: BTreeMap::from([("entry".into(), entry)]),
        })
        .unwrap(),
        bundle,
        asset,
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

fn coordinator(temp: &tempfile::TempDir) -> DaemonCoordinator {
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new(
            "main",
            &assets,
            assets.join(".distill-displaced"),
        )],
        LineageDestination {
            root: "main".to_owned(),
            path: "schema/schema-lineage.bundle".to_owned(),
        },
        vec![target()],
    )
    .unwrap()
}

#[test]
fn full_scan_publishes_one_store_and_rpc_version_with_missing_lineage_repair_basis() {
    let temp = tempfile::tempdir().unwrap();
    let (bytes, bundle, asset) = ordinary_bundle();
    let coordinator = coordinator(&temp);
    std::fs::write(temp.path().join("assets/ordinary.bundle"), bytes).unwrap();

    let stamp = coordinator.reconcile_full_scan().unwrap();
    assert_eq!(stamp.version, InputVersion(1));
    assert_eq!(coordinator.server().current_stamp(), stamp);
    let store = coordinator.store();
    let store = store.lock().unwrap();
    assert_eq!(store.input_version(), InputVersion(1));
    assert!(store.bundle(bundle).unwrap().is_some());
    assert_eq!(store.entry(asset).unwrap().unwrap().local_id, "entry");
    assert!(matches!(
        store.configuration_state().unwrap(),
        ConfigurationState::Poisoned { reason, .. }
            if matches!(reason.detail.as_ref(), DscpV1::MissingLineageManifest)
    ));
    drop(store);

    let metadata = coordinator
        .server()
        .root()
        .metadata(distill_rpc::PROTOCOL_VERSION)
        .connected()
        .unwrap();
    let snapshot = metadata.hub.snapshot().success().unwrap();
    assert_eq!(snapshot.version(), MetadataCall::Success(InputVersion(1)));
    let authoring = metadata.hub.authoring_snapshot().success().unwrap();
    assert!(matches!(
        authoring.inspect(asset),
        MetadataNamespaceCall::Success(AuthoringInspectResult::Inspection(_))
    ));
}

#[test]
fn watcher_union_reconciles_an_offline_delete_in_exactly_one_version() {
    let temp = tempfile::tempdir().unwrap();
    let (bytes, bundle, asset) = ordinary_bundle();
    let coordinator = coordinator(&temp);
    let path = temp.path().join("assets/ordinary.bundle");
    std::fs::write(&path, bytes).unwrap();
    coordinator.reconcile_full_scan().unwrap();

    std::fs::remove_file(path).unwrap();
    let stamp = coordinator
        .apply_watcher_batch([
            WatcherPathEvent {
                root: "main".to_owned(),
                path: "ordinary.bundle".to_owned(),
                exists: false,
            },
            WatcherPathEvent {
                root: "main".to_owned(),
                path: "ordinary.bundle".to_owned(),
                exists: false,
            },
        ])
        .unwrap();
    assert_eq!(stamp.version, InputVersion(2));

    let store = coordinator.store();
    let store = store.lock().unwrap();
    assert!(store.bundle(bundle).unwrap().is_none());
    assert!(store.entry(asset).unwrap().is_none());
    assert_eq!(store.input_version(), InputVersion(2));
}

#[cfg(unix)]
#[test]
fn unreadable_scan_state_publishes_a_typed_version_and_heals() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let coordinator = coordinator(&temp);
    let outside = temp.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    let link = temp.path().join("assets/escape");
    symlink(&outside, &link).unwrap();

    assert_eq!(
        coordinator.reconcile_full_scan().unwrap().version,
        InputVersion(1)
    );
    let store = coordinator.store();
    assert!(matches!(
        store
            .lock()
            .unwrap()
            .version_poison()
            .unwrap()
            .unwrap()
            .detail,
        VersionPoisonV1::UnreadableScanSubtree { .. }
    ));

    std::fs::remove_file(link).unwrap();
    assert_eq!(
        coordinator.reconcile_full_scan().unwrap().version,
        InputVersion(2)
    );
    assert!(store.lock().unwrap().version_poison().unwrap().is_none());
}

#[cfg(unix)]
#[test]
fn directory_alias_publishes_configuration_poison_without_aborting_the_version() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("assets/real");
    std::fs::create_dir_all(&directory).unwrap();
    symlink(&directory, temp.path().join("assets/alias")).unwrap();
    let coordinator = coordinator(&temp);

    assert_eq!(
        coordinator.reconcile_full_scan().unwrap().version,
        InputVersion(1)
    );
    let store = coordinator.store();
    assert!(matches!(
        store.lock().unwrap().configuration_state().unwrap(),
        ConfigurationState::Poisoned { reason, .. }
            if matches!(reason.detail.as_ref(), DscpV1::DirectoryAlias { .. })
    ));
}
