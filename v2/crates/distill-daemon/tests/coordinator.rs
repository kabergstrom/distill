use std::collections::BTreeMap;
use std::sync::Arc;

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::bootstrap::{BootstrapControlSpecV1, BootstrapControlSymbol};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_daemon::coordinator::{DaemonCoordinator, LineageDestination};
use distill_daemon::scanner::{AssetRoot, ScanDiagnostic};
use distill_daemon::watcher::WatcherBatch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, AuthoringEntry, AuthoringEntryRole, AuthoringInspectResult, AuthoringOp,
    AuthoringValue as RpcAuthoringValue, ConnectOutcome, ConnectRequest, Delta, DoctorRequest,
    LongRunningOp, MetadataCall, MetadataNamespaceCall, PreparedOperationPublication, StreamEvent,
    TargetDefinition, TargetDefinitionHash,
};
use distill_schema::ngp_schema::{
    node_hash, snapshot_to_json, LogicalSchema, PrimitiveKind, SchemaNode,
};
use distill_store::config::RestartOnlyChange;
use distill_store::state::{ConfigurationState, DscpV1, InputVersion, VersionPoisonV1};
use distill_store::{Store, StoreConfig, StoreError};

fn ordinary_bundle() -> (Vec<u8>, BundleUuid, AssetUuid) {
    ordinary_bundle_with(73, 72, 7)
}

fn ordinary_bundle_with(
    bundle_byte: u8,
    asset_byte: u8,
    value: u64,
) -> (Vec<u8>, BundleUuid, AssetUuid) {
    let type_uuid = TypeUuid([71; 16]);
    let schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    };
    let schema_hash = node_hash(&schema.root).unwrap();
    let epochs = vec![AcceptedSchemaEpoch {
        digest: schema_hash,
        forward_parent: None,
    }];
    let asset = AssetUuid([asset_byte; 16]);
    let bundle = BundleUuid([bundle_byte; 16]);
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
        data: AuthoredValue::UInt(value.into()),
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

fn bytes(value: &[u8]) -> AuthoredValue {
    AuthoredValue::Array(
        value
            .iter()
            .map(|byte| AuthoredValue::UInt((*byte).into()))
            .collect(),
    )
}

fn lineage_manifest_bundle(
    type_uuid: TypeUuid,
    schema_hash: distill_core::id::LogicalHash,
) -> Vec<u8> {
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::SchemaLineageManifest)
        .unwrap();
    let schema = distill_schema::ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
    let data = AuthoredValue::Object(BTreeMap::from([(
        "types".to_owned(),
        AuthoredValue::Array(vec![AuthoredValue::Array(vec![
            bytes(&type_uuid.0),
            AuthoredValue::Object(BTreeMap::from([
                (
                    "authority".to_owned(),
                    AuthoredValue::Object(BTreeMap::from([(
                        "Active".to_owned(),
                        AuthoredValue::Object(BTreeMap::new()),
                    )])),
                ),
                ("current".to_owned(), AuthoredValue::UInt(0)),
                (
                    "epochs".to_owned(),
                    AuthoredValue::Array(vec![AuthoredValue::Object(BTreeMap::from([
                        ("digest".to_owned(), bytes(&schema_hash.0)),
                        ("forward_parent".to_owned(), AuthoredValue::Null),
                    ]))]),
                ),
            ])),
        ])]),
    )]));
    distill_bundle::write_bundle(&Bundle {
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
                data,
            },
        )]),
    })
    .unwrap()
}

fn add_unknown_envelope_key(bytes: &[u8]) -> Vec<u8> {
    let mut value = distill_json::parse(std::str::from_utf8(bytes).unwrap()).unwrap();
    let AuthoredValue::Object(envelope) = &mut value else {
        panic!("bundle envelope must be an object");
    };
    envelope.insert("future-extension".to_owned(), AuthoredValue::UInt(1));
    distill_json::write(&value).unwrap().into_bytes()
}

#[test]
fn incremental_bundle_edit_does_not_invalidate_an_unrelated_bundle() {
    let temp = tempfile::tempdir().unwrap();
    let (first_bytes, _, first_asset) = ordinary_bundle();
    let (second_bytes, _, second_asset) = ordinary_bundle_with(83, 82, 8);
    let first_path = temp.path().join("assets/first.bundle");
    let second_path = temp.path().join("assets/second.bundle");
    std::fs::create_dir_all(temp.path().join("assets")).unwrap();
    std::fs::write(&first_path, first_bytes).unwrap();
    std::fs::write(&second_path, second_bytes).unwrap();
    let schema_hash = distill_bundle::parse_bundle(&std::fs::read(&first_path).unwrap())
        .unwrap()
        .assets["entry"]
        .schema_hash;
    std::fs::create_dir_all(temp.path().join("assets/schema")).unwrap();
    std::fs::write(
        temp.path().join("assets/schema/schema-lineage.bundle"),
        lineage_manifest_bundle(TypeUuid([71; 16]), schema_hash),
    )
    .unwrap();
    let coordinator = coordinator(&temp);
    let startup = coordinator.reconcile_full_scan().unwrap();

    let hub = match coordinator
        .server()
        .root()
        .connect(ConnectRequest::new("dev", TargetDefinitionHash([4; 32])))
    {
        ConnectOutcome::Connected(connected) => connected.hub,
        outcome => panic!("target connection failed: {outcome:?}"),
    };
    let subscription = hub
        .subscribe(startup.version, vec![first_asset, second_asset], vec![])
        .success()
        .unwrap();
    assert!(matches!(
        subscription.deltas.next(),
        Some(StreamEvent::InitialDelta { .. })
    ));

    let mut edited = distill_bundle::parse_bundle(&std::fs::read(&first_path).unwrap()).unwrap();
    edited.assets.get_mut("entry").unwrap().data = AuthoredValue::UInt(9);
    std::fs::write(&first_path, distill_bundle::write_bundle(&edited).unwrap()).unwrap();
    let published = coordinator
        .reconcile_incremental(&WatcherBatch {
            paths: vec![first_path],
            renames: Vec::new(),
        })
        .unwrap();
    assert_eq!(published.version.0, startup.version.0 + 1);
    assert!(matches!(
        subscription.deltas.next(),
        Some(StreamEvent::Delta(Delta { assets, .. }))
            if assets == vec![(first_asset, distill_rpc::AssetDeltaState::Changed)]
    ));
}

#[test]
fn complete_malformed_skeleton_is_bundle_scoped_and_heals_incrementally() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(assets.join("schema")).unwrap();
    let (ordinary, _, ordinary_asset) = ordinary_bundle();
    std::fs::write(assets.join("ordinary.bundle"), ordinary).unwrap();
    let schema_hash =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("ordinary.bundle")).unwrap())
            .unwrap()
            .assets["entry"]
            .schema_hash;
    let manifest = lineage_manifest_bundle(TypeUuid([71; 16]), schema_hash);
    let manifest_path = assets.join("schema/schema-lineage.bundle");
    std::fs::write(&manifest_path, &manifest).unwrap();
    let coordinator = coordinator(&temp);
    coordinator.reconcile_full_scan().unwrap();

    std::fs::write(&manifest_path, add_unknown_envelope_key(&manifest)).unwrap();
    coordinator
        .reconcile_incremental(&WatcherBatch {
            paths: vec![manifest_path.clone()],
            renames: Vec::new(),
        })
        .unwrap();

    {
        let store = coordinator.store();
        let store = store.lock().unwrap();
        assert!(store.version_poison().unwrap().is_none());
        assert!(matches!(
            store.entry(AssetUuid([94; 16])).unwrap_err(),
            StoreError::BundlePoisoned { bundle, .. } if bundle == BundleUuid([93; 16])
        ));
        assert!(store.entry(ordinary_asset).unwrap().is_some());
    }

    std::fs::write(&manifest_path, manifest).unwrap();
    coordinator
        .reconcile_incremental(&WatcherBatch {
            paths: vec![manifest_path],
            renames: Vec::new(),
        })
        .unwrap();
    let store = coordinator.store();
    let store = store.lock().unwrap();
    assert!(store.version_poison().unwrap().is_none());
    assert!(store.entry(AssetUuid([94; 16])).unwrap().is_some());
}

fn target() -> TargetDefinition {
    TargetDefinition::new("dev", TargetDefinitionHash([4; 32]))
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
        64,
    )
    .unwrap()
}

#[test]
fn startup_adopts_the_pending_restart_generation_before_rpc_construction() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join(".distill");
    let pending_generation = {
        let mut store = Store::open(StoreConfig::new(&state)).unwrap();
        store
            .stage_pending_restart(&[RestartOnlyChange::AutoCodegen(true)])
            .unwrap()
            .generation
    };

    let coordinator = coordinator(&temp);
    let store = coordinator.store();
    let store = store.lock().unwrap();
    assert_eq!(store.input_version(), InputVersion(1));
    assert!(store.pending_restart().unwrap().is_none());
    assert!(matches!(
        store.configuration_state().unwrap(),
        ConfigurationState::Ready(epoch) if epoch.generation == pending_generation
    ));
    assert_eq!(
        coordinator.server().current_stamp().version,
        InputVersion(1)
    );
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
fn watcher_trigger_reconciles_an_offline_delete_in_exactly_one_version() {
    let temp = tempfile::tempdir().unwrap();
    let (bytes, bundle, asset) = ordinary_bundle();
    let coordinator = coordinator(&temp);
    let path = temp.path().join("assets/ordinary.bundle");
    std::fs::write(&path, bytes).unwrap();
    coordinator.reconcile_full_scan().unwrap();

    std::fs::remove_file(path).unwrap();
    let stamp = coordinator.reconcile_full_scan().unwrap();
    assert_eq!(stamp.version, InputVersion(2));

    let store = coordinator.store();
    let store = store.lock().unwrap();
    assert!(store.bundle(bundle).unwrap().is_none());
    assert!(store.entry(asset).unwrap().is_none());
    assert_eq!(store.input_version(), InputVersion(2));
}

#[test]
fn direct_authoring_rewrites_and_deletes_the_bundle_durably() {
    let temp = tempfile::tempdir().unwrap();
    let (bytes, bundle_uuid, asset_uuid) = ordinary_bundle();
    let coordinator = coordinator(&temp);
    let bundle_path = temp.path().join("assets/ordinary.bundle");
    std::fs::write(&bundle_path, bytes).unwrap();
    coordinator.reconcile_full_scan().unwrap();

    let parsed = distill_bundle::parse_bundle(&std::fs::read(&bundle_path).unwrap()).unwrap();
    let original = &parsed.assets["entry"];
    let logical_schema = snapshot_to_json(&parsed.schemas[&original.schema_hash]).unwrap();
    let operation = AuthoringOp::Set(AuthoringEntry {
        uuid: asset_uuid,
        bundle: bundle_uuid,
        local_id: "entry".into(),
        normalized_path: "ordinary.bundle".into(),
        type_uuid: original.type_uuid,
        terminal_type: original.type_uuid,
        schema_hash: original.schema_hash,
        logical_schema: Arc::from(logical_schema.into_bytes()),
        role: AuthoringEntryRole::Runtime,
        tags: BTreeMap::new(),
        value: RpcAuthoringValue {
            canonical_value: Arc::from(&b"9"[..]),
            blobs: Vec::new(),
        },
    });
    let backend = Arc::clone(coordinator.authoring_service());
    let stamp = coordinator
        .server()
        .coordinated_commit(InputVersion(1), || {
            backend
                .prepare_write(InputVersion(1), &[operation])
                .map_err(|error| format!("{error:?}"))?
                .ok_or_else(|| "production authoring returned no commit".to_owned())
        })
        .unwrap();
    assert_eq!(stamp.version, InputVersion(2));
    let rewritten = distill_bundle::parse_bundle(&std::fs::read(&bundle_path).unwrap()).unwrap();
    assert_eq!(rewritten.assets["entry"].data, AuthoredValue::UInt(9));
    assert_eq!(
        coordinator.store().lock().unwrap().input_version(),
        InputVersion(2)
    );

    let backend = Arc::clone(coordinator.authoring_service());
    let stamp = coordinator
        .server()
        .coordinated_commit(InputVersion(2), || {
            backend
                .prepare_write(InputVersion(2), &[AuthoringOp::Remove { uuid: asset_uuid }])
                .map_err(|error| format!("{error:?}"))?
                .ok_or_else(|| "production authoring returned no commit".to_owned())
        })
        .unwrap();
    assert_eq!(stamp.version, InputVersion(3));
    assert!(!bundle_path.exists());
    let store = coordinator.store();
    let store = store.lock().unwrap();
    assert_eq!(store.input_version(), InputVersion(3));
    assert!(store.bundle(bundle_uuid).unwrap().is_none());
    assert!(store.entry(asset_uuid).unwrap().is_none());
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
fn incremental_scan_poison_heals_when_observation_returns_to_last_good() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let coordinator = coordinator(&temp);
    assert_eq!(
        coordinator.reconcile_full_scan().unwrap().version,
        InputVersion(1)
    );
    let outside = temp.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    let link = temp.path().join("assets/escape");
    symlink(&outside, &link).unwrap();
    assert_eq!(
        coordinator
            .reconcile_incremental(&WatcherBatch {
                paths: vec![link.clone()],
                renames: Vec::new(),
            })
            .unwrap()
            .version,
        InputVersion(2)
    );

    std::fs::remove_file(&link).unwrap();
    assert_eq!(
        coordinator
            .reconcile_incremental(&WatcherBatch {
                paths: vec![link],
                renames: Vec::new(),
            })
            .unwrap()
            .version,
        InputVersion(3)
    );
    assert!(coordinator
        .store()
        .lock()
        .unwrap()
        .version_poison()
        .unwrap()
        .is_none());
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

#[cfg(unix)]
#[test]
fn retained_daemon_state_alias_is_diagnosed_and_never_scanned() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(assets.join("schema")).unwrap();
    let (ordinary, _, ordinary_asset) = ordinary_bundle();
    let ordinary_path = assets.join("ordinary.bundle");
    std::fs::write(&ordinary_path, ordinary).unwrap();
    let schema_hash = distill_bundle::parse_bundle(&std::fs::read(&ordinary_path).unwrap())
        .unwrap()
        .assets["entry"]
        .schema_hash;
    std::fs::write(
        assets.join("schema/schema-lineage.bundle"),
        lineage_manifest_bundle(TypeUuid([71; 16]), schema_hash),
    )
    .unwrap();
    let coordinator = Arc::new(coordinator(&temp));
    coordinator.attach_build_backend();
    let alias = assets.join("daemon-state-alias");
    symlink(temp.path().join(".distill"), &alias).unwrap();

    coordinator.reconcile_full_scan().unwrap();
    assert!(matches!(
        coordinator.scan_diagnostics().as_slice(),
        [ScanDiagnostic::DaemonOwnedDirectoryAlias {
            root_name,
            normalized_path,
            ..
        }] if root_name == "main" && normalized_path == "daemon-state-alias"
    ));
    let store = coordinator.store();
    let store = store.lock().unwrap();
    assert!(matches!(
        store.configuration_state().unwrap(),
        ConfigurationState::Ready(_)
    ));
    assert!(store.entry(ordinary_asset).unwrap().is_some());
    assert!(store
        .all_files()
        .unwrap()
        .iter()
        .all(|(_, path, _)| !path.starts_with("daemon-state-alias")));
    drop(store);
    let version = coordinator.server().current_stamp().version;

    std::fs::remove_file(&alias).unwrap();
    coordinator
        .reconcile_incremental(&WatcherBatch {
            paths: vec![alias.clone()],
            renames: Vec::new(),
        })
        .unwrap();
    assert!(coordinator.scan_diagnostics().is_empty());
    assert_eq!(coordinator.server().current_stamp().version, version);
    symlink(temp.path().join(".distill"), &alias).unwrap();
    coordinator
        .reconcile_incremental(&WatcherBatch {
            paths: vec![alias],
            renames: Vec::new(),
        })
        .unwrap();
    assert_eq!(coordinator.scan_diagnostics().len(), 1);
    assert_eq!(coordinator.server().current_stamp().version, version);

    let base = coordinator.server().current_stamp().version;
    let prepared = coordinator
        .authoring_service()
        .prepare_operation(base, &LongRunningOp::Doctor(DoctorRequest::Verify.encode()))
        .unwrap();
    let PreparedOperationPublication::Deferred(operation) = prepared.publication else {
        panic!("doctor verify must be deferred")
    };
    let completed = operation.complete(base).unwrap();
    assert!(completed
        .terminal_error
        .as_deref()
        .is_some_and(|error| error.contains("daemon-owned-directory-alias")));
}
