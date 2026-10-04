use std::collections::BTreeMap;
use std::sync::Arc;

use distill_bundle::{AssetEntry, Bundle};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_daemon::coordinator::DaemonCoordinator;
use distill_daemon::scanner::{AssetRoot, ScanDiagnostic};
use distill_daemon::watcher::WatcherBatch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, AuthoringEntry, AuthoringEntryRole, AuthoringInspectResult, AuthoringOp,
    AuthoringProgressState, AuthoringValue as RpcAuthoringValue, ConnectOutcome, ConnectRequest,
    ContentHash, Delta, DoctorRequest, LongRunningOp, MetadataCall, MetadataNamespaceCall,
    ConfigurationStatus, RpcFailure, StreamEvent, TargetDefinition, TargetDefinitionHash, WriteReceipt, WrittenFile,
};
use distill_schema::ngp_schema::{
    node_hash, snapshot_to_json, LogicalSchema, PrimitiveKind, SchemaNode,
};
use distill_store::config::RestartOnlyChange;
use distill_store::served::AssetResolution;
use distill_store::state::{DscpV1, InputVersion, NamespaceErrorV1};
use distill_store::{Store, StoreConfig, StoreReader};

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
    let asset = AssetUuid([asset_byte; 16]);
    let bundle = BundleUuid([bundle_byte; 16]);
    let entry = AssetEntry {
        uuid: asset,
        type_uuid,
        schema_hash,
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
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    let startup = coordinator.reconcile_full_scan(&mut writer).unwrap();

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
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![first_path],
                renames: Vec::new(),
            },
        )
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
    let (manifest, _, _) = ordinary_bundle_with(93, 94, 5);
    let manifest_path = assets.join("schema/sample.bundle");
    std::fs::write(&manifest_path, &manifest).unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();

    std::fs::write(&manifest_path, add_unknown_envelope_key(&manifest)).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![manifest_path.clone()],
                renames: Vec::new(),
            },
        )
        .unwrap();

    {
        let store = coordinator.open_reader().unwrap();
        let errors = store.namespace_errors().unwrap();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0].detail,
            NamespaceErrorV1::IncompleteSkeleton { source, .. }
                if source.normalized_path == "schema/sample.bundle"
        ));
        // No project authority knows its type, so the bundle drops out of
        // the namespace instead of being indexed as a poisoned skeleton.
        assert!(store.entry(AssetUuid([94; 16])).unwrap().is_none());
        assert!(store.entry(ordinary_asset).unwrap().is_some());
    }

    std::fs::write(&manifest_path, manifest).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![manifest_path],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let store = coordinator.open_reader().unwrap();
    assert!(store.namespace_errors().unwrap().is_empty());
    assert!(store.entry(AssetUuid([94; 16])).unwrap().is_some());
}

/// A bundle holding its own asset plus `shared`, which another bundle may
/// claim too.
fn bundle_sharing(bundle_byte: u8, own_byte: u8, shared: AssetUuid) -> Vec<u8> {
    let (bytes, _, _) = ordinary_bundle_with(bundle_byte, own_byte, 1);
    let mut bundle = distill_bundle::parse_bundle(&bytes).unwrap();
    let mut entry = bundle.assets["entry"].clone();
    entry.uuid = shared;
    bundle.assets.insert("shared".into(), entry);
    distill_bundle::write_bundle(&bundle).unwrap()
}

/// Two roots' worth of fixture: `first.bundle`
/// (bundle 31, asset 32 + the shared 40) and optionally `second.bundle`
/// (bundle 33, asset 34 + the shared 40).
fn collision_fixture(temp: &tempfile::TempDir, second: bool) -> DaemonCoordinator {
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(assets.join("schema")).unwrap();
    let shared = AssetUuid([40; 16]);
    let first = bundle_sharing(31, 32, shared);
    std::fs::write(assets.join("first.bundle"), first).unwrap();
    if second {
        std::fs::write(assets.join("second.bundle"), bundle_sharing(33, 34, shared)).unwrap();
    }
    coordinator(temp)
}

fn assert_only_the_shared_asset_is_withheld(store: &StoreReader) {
    let [error] = <[_; 1]>::try_from(store.namespace_errors().unwrap()).unwrap();
    assert!(matches!(
        error.detail,
        NamespaceErrorV1::DuplicateAssetUuid { asset, .. } if asset == AssetUuid([40; 16])
    ));
    assert!(store.entry(AssetUuid([32; 16])).unwrap().is_some());
    assert!(store.entry(AssetUuid([34; 16])).unwrap().is_some());
    assert!(store.entry(AssetUuid([40; 16])).unwrap().is_none());
    assert_eq!(
        store.asset_resolution(AssetUuid([40; 16])).unwrap(),
        Some(AssetResolution::Failed(error.message))
    );
}

#[test]
fn a_colliding_asset_is_withheld_alone_and_heals_when_one_claimant_leaves() {
    let temp = tempfile::tempdir().unwrap();
    let coordinator = collision_fixture(&temp, true);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert_only_the_shared_asset_is_withheld(&coordinator.open_reader().unwrap());

    let second = temp.path().join("assets/second.bundle");
    std::fs::remove_file(&second).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![second],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let store = coordinator.open_reader().unwrap();
    assert!(store.namespace_errors().unwrap().is_empty());
    assert!(store.entry(AssetUuid([32; 16])).unwrap().is_some());
    assert!(store.entry(AssetUuid([34; 16])).unwrap().is_none());
    assert!(store.entry(AssetUuid([40; 16])).unwrap().is_some());
}

#[test]
fn an_incremental_collision_withholds_the_asset_and_republishes_the_survivor_on_heal() {
    let temp = tempfile::tempdir().unwrap();
    let coordinator = collision_fixture(&temp, false);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert!(coordinator
        .open_reader()
        .unwrap()
        .entry(AssetUuid([40; 16]))
        .unwrap()
        .is_some());

    let second = temp.path().join("assets/second.bundle");
    std::fs::write(&second, bundle_sharing(33, 34, AssetUuid([40; 16]))).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![second.clone()],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert_only_the_shared_asset_is_withheld(&coordinator.open_reader().unwrap());

    // The second bundle drops the shared asset: the first bundle, whose
    // bytes never changed, publishes it again.
    std::fs::write(&second, ordinary_bundle_with(33, 34, 1).0).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![second],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let store = coordinator.open_reader().unwrap();
    assert!(store.namespace_errors().unwrap().is_empty());
    assert!(store.entry(AssetUuid([34; 16])).unwrap().is_some());
    assert!(store.entry(AssetUuid([40; 16])).unwrap().is_some());
}

/// Claims are input state the publication that derived them commits, not
/// process state: a restart keeps them. When the first scan after the
/// restart is rejected, its namespace errors still name the stored
/// collision, and an incremental heal of one claimant republishes the
/// survivor from the stored claims of a bundle no scan of this process
/// read.
#[cfg(unix)]
#[test]
fn claims_survive_a_restart_whose_first_scan_is_rejected() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    {
        let coordinator = collision_fixture(&temp, true);
        let mut writer = coordinator.open_writer().unwrap();
        coordinator.reconcile_full_scan(&mut writer).unwrap();
        assert_only_the_shared_asset_is_withheld(&coordinator.open_reader().unwrap());
    }
    let outside = temp.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    let link = temp.path().join("assets/escape");
    symlink(&outside, &link).unwrap();

    let coordinator = coordinator(&temp);
    let reader = coordinator.open_reader().unwrap();
    let [stored] = <[_; 1]>::try_from(reader.namespace_errors().unwrap()).unwrap();
    assert!(matches!(
        stored.detail,
        NamespaceErrorV1::DuplicateAssetUuid { asset, .. } if asset == AssetUuid([40; 16])
    ));
    drop(reader);

    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    let reader = coordinator.open_reader().unwrap();
    let hub = coordinator.server_handle();
    assert!(
        hub.scan_rejection().is_some(),
        "the first scan is rejected"
    );
    let errors = hub.namespace_errors(&reader).unwrap();
    assert!(
        errors.iter().any(|error| matches!(
            error.detail,
            NamespaceErrorV1::DuplicateAssetUuid { asset, .. } if asset == AssetUuid([40; 16])
        )),
        "{errors:?}"
    );
    assert!(errors
        .iter()
        .any(|error| matches!(error.detail, NamespaceErrorV1::UnreadableScanSubtree { .. })));
    assert!(reader.entry(AssetUuid([40; 16])).unwrap().is_none());
    drop(reader);

    // One claimant leaves; the other's claims are the stored ones.
    let second = temp.path().join("assets/second.bundle");
    std::fs::remove_file(&second).unwrap();
    std::fs::remove_file(&link).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![second, link],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let store = coordinator.open_reader().unwrap();
    assert_eq!(hub.scan_rejection(), None);
    assert!(
        hub.namespace_errors(&store).unwrap().is_empty(),
        "{:?}",
        hub.namespace_errors(&store)
    );
    assert!(store.entry(AssetUuid([32; 16])).unwrap().is_some());
    assert!(store.entry(AssetUuid([34; 16])).unwrap().is_none());
    assert!(store.entry(AssetUuid([40; 16])).unwrap().is_some());
}

fn target() -> TargetDefinition {
    TargetDefinition::new("dev", TargetDefinitionHash([4; 32]))
}

fn coordinator(temp: &tempfile::TempDir) -> DaemonCoordinator {
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap()
}

/// An RPC authoring write as the server runs it: the backend writes the
/// files inside an input at `base` that is then rolled back.
fn write_files(
    coordinator: &DaemonCoordinator,
    writer: &mut Store,
    base: InputVersion,
    operations: &[AuthoringOp],
    force_lossy: bool,
) -> Result<WriteReceipt, RpcFailure> {
    assert_eq!(writer.open_input().unwrap(), base);
    let written =
        coordinator
            .authoring_service()
            .write_files(writer, base, operations, force_lossy);
    writer.finish_input(false).unwrap();
    written
}

/// [`write_files`], then the watcher's batch for the written files. The
/// version it publishes reflects the receipt.
fn write_and_publish(
    coordinator: &DaemonCoordinator,
    writer: &mut Store,
    base: InputVersion,
    operations: &[AuthoringOp],
    force_lossy: bool,
) -> Result<(WriteReceipt, InputVersion), RpcFailure> {
    let receipt = write_files(coordinator, writer, base, operations, force_lossy)?;
    let scanner = coordinator.scanner();
    let paths = receipt
        .files
        .iter()
        .map(|file| scanner.physical_path(&file.root, &file.path).unwrap())
        .collect();
    let version = coordinator
        .reconcile_incremental(
            writer,
            &WatcherBatch {
                paths,
                renames: Vec::new(),
            },
        )
        .unwrap()
        .version;
    let reader = coordinator.open_reader().unwrap();
    for file in &receipt.files {
        assert_eq!(
            reader.file_content_hash(&file.root, &file.path).unwrap(),
            file.content_hash,
            "the published version reflects {file:?}"
        );
    }
    Ok((receipt, version))
}

#[test]
fn full_scan_publishes_one_store_and_rpc_version() {
    let temp = tempfile::tempdir().unwrap();
    let (bytes, bundle, asset) = ordinary_bundle();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    std::fs::write(temp.path().join("assets/ordinary.bundle"), bytes).unwrap();

    let stamp = coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert_eq!(stamp.version, InputVersion(1));
    assert_eq!(coordinator.server().current_stamp().unwrap(), stamp);
    let store = coordinator.open_reader().unwrap();
    assert_eq!(store.input_version().unwrap(), InputVersion(1));
    assert!(store.bundle(bundle).unwrap().is_some());
    assert_eq!(store.entry(asset).unwrap().unwrap().local_id, "entry");
    assert!(matches!(
        coordinator.configuration_status().unwrap(),
        ConfigurationStatus::Ready
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
    let mut writer = coordinator.open_writer().unwrap();
    let path = temp.path().join("assets/ordinary.bundle");
    std::fs::write(&path, bytes).unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();

    std::fs::remove_file(path).unwrap();
    let stamp = coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert_eq!(stamp.version, InputVersion(2));

    let store = coordinator.open_reader().unwrap();
    assert!(store.bundle(bundle).unwrap().is_none());
    assert!(store.entry(asset).unwrap().is_none());
    assert_eq!(store.input_version().unwrap(), InputVersion(2));
}

#[test]
fn direct_authoring_rewrites_and_deletes_the_bundle_durably() {
    let temp = tempfile::tempdir().unwrap();
    let (bytes, bundle_uuid, asset_uuid) = ordinary_bundle();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    let bundle_path = temp.path().join("assets/ordinary.bundle");
    std::fs::write(&bundle_path, bytes).unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();

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
    let (receipt, version) = write_and_publish(
        &coordinator,
        &mut writer,
        InputVersion(1),
        &[operation],
        false,
    )
    .unwrap();
    assert_eq!(version, InputVersion(2));
    let bytes = std::fs::read(&bundle_path).unwrap();
    assert_eq!(
        receipt.files,
        [WrittenFile {
            root: "main".into(),
            path: "ordinary.bundle".into(),
            content_hash: Some(ContentHash(*blake3::hash(&bytes).as_bytes())),
        }]
    );
    let rewritten = distill_bundle::parse_bundle(&bytes).unwrap();
    assert_eq!(rewritten.assets["entry"].data, AuthoredValue::UInt(9));

    let (receipt, version) = write_and_publish(
        &coordinator,
        &mut writer,
        InputVersion(2),
        &[AuthoringOp::Remove { uuid: asset_uuid }],
        false,
    )
    .unwrap();
    assert_eq!(version, InputVersion(3));
    assert_eq!(receipt.files[0].content_hash, None);
    assert!(!bundle_path.exists());
    let store = coordinator.open_reader().unwrap();
    assert_eq!(store.input_version().unwrap(), InputVersion(3));
    assert!(store.bundle(bundle_uuid).unwrap().is_none());
    assert!(store.entry(asset_uuid).unwrap().is_none());
}

/// A direct write changes the file and commits nothing: the store keeps
/// the old version until the watcher publishes the file. A write that fails
/// changes no file.
#[test]
fn a_direct_write_changes_the_file_and_commits_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let (bytes, bundle_uuid, asset_uuid) = ordinary_bundle();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    let bundle_path = temp.path().join("assets/ordinary.bundle");
    std::fs::write(&bundle_path, &bytes).unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();

    // A stale base fails before any file changes.
    writer.open_input().unwrap();
    let stale = coordinator.authoring_service().write_files(
        &mut writer,
        InputVersion(0),
        &[AuthoringOp::Remove { uuid: asset_uuid }],
        false,
    );
    writer.finish_input(false).unwrap();
    assert!(matches!(stale, Err(RpcFailure::StaleInputVersion { .. })));
    assert_eq!(std::fs::read(&bundle_path).unwrap(), bytes);

    write_files(
        &coordinator,
        &mut writer,
        InputVersion(1),
        &[AuthoringOp::Remove { uuid: asset_uuid }],
        false,
    )
    .unwrap();
    assert!(!bundle_path.exists());
    let outside = StoreReader::open(StoreConfig::new(temp.path().join(".distill"))).unwrap();
    assert_eq!(outside.input_version().unwrap(), InputVersion(1));
    assert!(outside.entry(asset_uuid).unwrap().is_some());
    assert!(outside.bundle(bundle_uuid).unwrap().is_some());

    // A second write planned against the same version finds the file it
    // read gone, and changes nothing.
    std::fs::write(&bundle_path, b"not the published bytes").unwrap();
    assert!(write_files(
        &coordinator,
        &mut writer,
        InputVersion(1),
        &[AuthoringOp::Remove { uuid: asset_uuid }],
        false,
    )
    .is_err());
    assert_eq!(
        std::fs::read(&bundle_path).unwrap(),
        b"not the published bytes"
    );
}

#[cfg(unix)]
#[test]
fn unreadable_scan_state_is_diagnosed_without_a_version_and_heals() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    let outside = temp.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    let link = temp.path().join("assets/escape");
    symlink(&outside, &link).unwrap();

    // The rejected scan publishes nothing; its error is the process's.
    assert_eq!(
        coordinator
            .reconcile_full_scan(&mut writer)
            .unwrap()
            .version,
        InputVersion(0)
    );
    let hub = coordinator.server_handle();
    let store = coordinator.open_reader().unwrap();
    assert!(matches!(
        hub.namespace_errors(&store).unwrap()[0].detail,
        NamespaceErrorV1::UnreadableScanSubtree { .. }
    ));

    std::fs::remove_file(link).unwrap();
    assert_eq!(
        coordinator
            .reconcile_full_scan(&mut writer)
            .unwrap()
            .version,
        InputVersion(1)
    );
    assert!(hub.namespace_errors(&store).unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn incremental_scan_error_heals_when_observation_returns_to_last_good() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    assert_eq!(
        coordinator
            .reconcile_full_scan(&mut writer)
            .unwrap()
            .version,
        InputVersion(1)
    );
    let outside = temp.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    let link = temp.path().join("assets/escape");
    symlink(&outside, &link).unwrap();
    // The rejection publishes no version; the process diagnoses it.
    assert_eq!(
        coordinator
            .reconcile_incremental(
                &mut writer,
                &WatcherBatch {
                    paths: vec![link.clone()],
                    renames: Vec::new(),
                }
            )
            .unwrap()
            .version,
        InputVersion(1)
    );
    let hub = coordinator.server_handle();
    assert!(hub.scan_rejection().is_some());

    // The healing batch publishes what it observed.
    std::fs::remove_file(&link).unwrap();
    assert_eq!(
        coordinator
            .reconcile_incremental(
                &mut writer,
                &WatcherBatch {
                    paths: vec![link],
                    renames: Vec::new(),
                }
            )
            .unwrap()
            .version,
        InputVersion(2)
    );
    assert_eq!(hub.scan_rejection(), None);
}

#[cfg(unix)]
#[test]
fn unrelated_incremental_observation_does_not_heal_pending_scan_error() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    assert_eq!(
        coordinator
            .reconcile_full_scan(&mut writer)
            .unwrap()
            .version,
        InputVersion(1)
    );
    let outside = temp.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    let link = temp.path().join("assets/escape");
    symlink(&outside, &link).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![link.clone()],
                renames: Vec::new(),
            },
        )
        .unwrap();

    let unrelated = temp.path().join("assets/unrelated.txt");
    std::fs::write(&unrelated, b"new observation").unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![unrelated],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let hub = coordinator.server_handle();
    let store = coordinator.open_reader().unwrap();
    assert!(!hub.namespace_errors(&store).unwrap().is_empty());
    drop(store);

    std::fs::remove_file(&link).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![link],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let store = coordinator.open_reader().unwrap();
    assert!(hub.namespace_errors(&store).unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn configuration_scan_rejection_preserves_existing_namespace_errors() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    let real = assets.join("real");
    std::fs::create_dir_all(&real).unwrap();
    let (bytes, _, _) = ordinary_bundle();
    std::fs::write(assets.join("first.bundle"), &bytes).unwrap();
    std::fs::write(assets.join("second.bundle"), &bytes).unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    let initial = coordinator
        .open_reader()
        .unwrap()
        .namespace_errors()
        .unwrap();

    let alias = assets.join("alias");
    symlink(&real, &alias).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![alias],
                renames: Vec::new(),
            },
        )
        .unwrap();

    assert_eq!(
        coordinator
            .open_reader()
            .unwrap()
            .namespace_errors()
            .unwrap(),
        initial
    );
}

#[cfg(unix)]
#[test]
fn directory_alias_is_a_configuration_error_without_a_version() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("assets/real");
    std::fs::create_dir_all(&directory).unwrap();
    symlink(&directory, temp.path().join("assets/alias")).unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();

    assert_eq!(
        coordinator
            .reconcile_full_scan(&mut writer)
            .unwrap()
            .version,
        InputVersion(0)
    );
    assert!(matches!(
        coordinator.configuration_status().unwrap(),
        ConfigurationStatus::Failed(reason)
            if matches!(reason.detail.as_ref(), DscpV1::DirectoryAlias { .. })
    ));
}

/// What a fresh scan of the coordinator's roots diagnoses.
#[cfg(unix)]
fn diagnostics(coordinator: &DaemonCoordinator) -> Vec<ScanDiagnostic> {
    coordinator
        .scanner()
        .scan()
        .unwrap()
        .diagnostic_rows()
        .cloned()
        .collect()
}

#[cfg(unix)]
#[test]
fn daemon_state_alias_is_diagnosed_and_never_scanned() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(assets.join("schema")).unwrap();
    let (ordinary, _, ordinary_asset) = ordinary_bundle();
    let ordinary_path = assets.join("ordinary.bundle");
    std::fs::write(&ordinary_path, ordinary).unwrap();
    let coordinator = Arc::new(coordinator(&temp));
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.attach_build_backend();
    let alias = assets.join("daemon-state-alias");
    symlink(temp.path().join(".distill"), &alias).unwrap();

    coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert!(matches!(
        diagnostics(&coordinator).as_slice(),
        [ScanDiagnostic::DaemonOwnedDirectoryAlias {
            root_name,
            normalized_path,
            ..
        }] if root_name == "main" && normalized_path == "daemon-state-alias"
    ));
    let store = coordinator.open_reader().unwrap();
    assert!(matches!(
        coordinator.configuration_status().unwrap(),
        ConfigurationStatus::Ready
    ));
    assert!(store.entry(ordinary_asset).unwrap().is_some());
    assert!(store
        .observed_files()
        .unwrap()
        .iter()
        .all(|row| !row.path.starts_with("daemon-state-alias")));
    drop(store);
    let version = coordinator.server().current_stamp().unwrap().version;

    std::fs::remove_file(&alias).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![alias.clone()],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert!(diagnostics(&coordinator).is_empty());
    assert_eq!(
        coordinator.server().current_stamp().unwrap().version,
        version
    );
    symlink(temp.path().join(".distill"), &alias).unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![alias],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(diagnostics(&coordinator).len(), 1);
    assert_eq!(
        coordinator.server().current_stamp().unwrap().version,
        version
    );

    let base = coordinator.server().current_stamp().unwrap().version;
    let hub = match coordinator
        .server()
        .root()
        .connect(ConnectRequest::new("dev", TargetDefinitionHash([4; 32])))
    {
        ConnectOutcome::Connected(connected) => connected.hub,
        outcome => panic!("target connection failed: {outcome:?}"),
    };
    let mut cancelled = hub
        .operation(base, LongRunningOp::Doctor(DoctorRequest::Verify.encode()))
        .success()
        .unwrap();
    assert_eq!(
        cancelled.next().unwrap().state,
        AuthoringProgressState::Started
    );
    assert!(cancelled.cancel());
    assert_eq!(
        cancelled.next().unwrap().state,
        AuthoringProgressState::Cancelled
    );
    assert_eq!(coordinator.server().current_stamp().unwrap().version, base);

    // Doctor verify is a report on a read snapshot: it completes while
    // another writer holds an open input (the write lock), and publishes no
    // version.
    writer.open_input().unwrap();
    let events = hub
        .operation(base, LongRunningOp::Doctor(DoctorRequest::Verify.encode()))
        .success()
        .unwrap()
        .collect::<Vec<_>>();
    writer.finish_input(false).unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].state, AuthoringProgressState::Started);
    assert_eq!(events[1].state, AuthoringProgressState::Running);
    assert_eq!(events[2].state, AuthoringProgressState::Failed);
    assert!(std::str::from_utf8(&events[2].payload)
        .unwrap()
        .contains("daemon-owned-directory-alias"));
    assert_eq!(coordinator.server().current_stamp().unwrap().version, base);
}

fn u8_struct(fields: &[&str]) -> LogicalSchema {
    LogicalSchema {
        root: SchemaNode::Struct {
            rev: 0,
            fields: fields
                .iter()
                .map(|name| {
                    (
                        name.to_string(),
                        0,
                        SchemaNode::Primitive(PrimitiveKind::U8),
                    )
                })
                .collect(),
        },
    }
}

fn u8_object(fields: &[(&str, u64)]) -> AuthoredValue {
    AuthoredValue::Object(
        fields
            .iter()
            .map(|(name, value)| (name.to_string(), AuthoredValue::UInt((*value).into())))
            .collect(),
    )
}

/// Stores `stored` under its schema, then writes `written` over it under
/// another one.
fn rewrite_under_a_new_schema(
    stored: (LogicalSchema, AuthoredValue),
    written: (LogicalSchema, AuthoredValue),
    force_lossy: bool,
) -> Result<AuthoredValue, distill_rpc::RpcFailure> {
    let temp = tempfile::tempdir().unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    let type_uuid = TypeUuid([71; 16]);
    let (bundle_uuid, asset_uuid) = (BundleUuid([73; 16]), AssetUuid([72; 16]));
    let (schema, data) = stored;
    let schema_hash = node_hash(&schema.root).unwrap();
    let bundle_path = temp.path().join("assets/ordinary.bundle");
    let bytes = distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: bundle_uuid,
        primary: Some("entry".into()),
        schemas: BTreeMap::from([(schema_hash, schema)]),
        assets: BTreeMap::from([(
            "entry".into(),
            AssetEntry {
                uuid: asset_uuid,
                type_uuid,
                schema_hash,
                authoring_only: false,
                data,
            },
        )]),
    })
    .unwrap();
    std::fs::write(&bundle_path, bytes).unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();

    let (schema, value) = written;
    let operation = AuthoringOp::Set(AuthoringEntry {
        uuid: asset_uuid,
        bundle: bundle_uuid,
        local_id: "entry".into(),
        normalized_path: "ordinary.bundle".into(),
        type_uuid,
        terminal_type: type_uuid,
        schema_hash: node_hash(&schema.root).unwrap(),
        logical_schema: Arc::from(snapshot_to_json(&schema).unwrap().into_bytes()),
        role: AuthoringEntryRole::Runtime,
        tags: BTreeMap::new(),
        value: RpcAuthoringValue {
            canonical_value: Arc::from(distill_json::write(&value).unwrap().into_bytes()),
            blobs: Vec::new(),
        },
    });
    let (_, version) = write_and_publish(
        &coordinator,
        &mut writer,
        InputVersion(1),
        &[operation],
        force_lossy,
    )?;
    assert_eq!(version, InputVersion(2));
    let rewritten = distill_bundle::parse_bundle(&std::fs::read(&bundle_path).unwrap()).unwrap();
    Ok(rewritten.assets["entry"].data.clone())
}

#[test]
fn a_write_that_adds_a_field_is_lossless() {
    let written = u8_object(&[("a", 1), ("b", 2)]);
    let result = rewrite_under_a_new_schema(
        (u8_struct(&["a"]), u8_object(&[("a", 1)])),
        (u8_struct(&["a", "b"]), written.clone()),
        false,
    );
    assert_eq!(result.unwrap(), written);
}

#[test]
fn a_write_may_drop_a_field_that_holds_its_default() {
    let result = rewrite_under_a_new_schema(
        (u8_struct(&["a", "b"]), u8_object(&[("a", 1), ("b", 0)])),
        (u8_struct(&["a"]), u8_object(&[("a", 1)])),
        false,
    );
    assert_eq!(result.unwrap(), u8_object(&[("a", 1)]));
}

#[test]
fn a_write_that_drops_data_is_refused_unless_forced() {
    let stored = (u8_struct(&["a", "b"]), u8_object(&[("a", 1), ("b", 5)]));
    let written = (u8_struct(&["a"]), u8_object(&[("a", 1)]));
    match rewrite_under_a_new_schema(stored.clone(), written.clone(), false) {
        Err(distill_rpc::RpcFailure::LossyWrite {
            type_uuid,
            asset,
            fields,
            ..
        }) => {
            assert_eq!(type_uuid, TypeUuid([71; 16]));
            assert_eq!(asset, AssetUuid([72; 16]));
            assert_eq!(fields, ["$.b"]);
        }
        other => panic!("expected a lossy-write refusal, got {other:?}"),
    }
    assert_eq!(
        rewrite_under_a_new_schema(stored, written, true).unwrap(),
        u8_object(&[("a", 1)])
    );
}

#[test]
fn a_write_the_planner_refuses_needs_a_migration_function() {
    let stored = (u8_struct(&["a"]), u8_object(&[("a", 1)]));
    let written = (
        LogicalSchema {
            root: SchemaNode::Struct {
                rev: 0,
                fields: vec![("a".into(), 0, SchemaNode::String)],
            },
        },
        AuthoredValue::Object(BTreeMap::from([(
            "a".to_owned(),
            AuthoredValue::Str("one".into()),
        )])),
    );
    assert!(matches!(
        rewrite_under_a_new_schema(stored, written, false),
        Err(distill_rpc::RpcFailure::LossyWrite { .. })
    ));
}

#[test]
fn the_echo_of_an_atomic_write_through_an_unobserved_temporary_file_publishes_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let (bytes, _, _) = ordinary_bundle();
    let coordinator = coordinator(&temp);
    let path = temp.path().join("assets/ordinary.bundle");
    std::fs::write(&path, &bytes).unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    let startup = coordinator.reconcile_full_scan(&mut writer).unwrap();

    // An atomic write: a temporary file renamed onto its target.
    let (edited, _, _) = ordinary_bundle_with(73, 72, 9);
    let temporary = temp.path().join("assets/.ordinary.bundle.distill-1.tmp");
    std::fs::write(&temporary, edited).unwrap();
    std::fs::rename(&temporary, &path).unwrap();
    let batch = WatcherBatch {
        paths: vec![temporary.clone(), path.clone()],
        renames: vec![distill_daemon::watcher::WatcherRename {
            from: temporary,
            to: path,
        }],
    };
    let published = coordinator
        .reconcile_incremental(&mut writer, &batch)
        .unwrap();
    assert_eq!(published.version.0, startup.version.0 + 1);

    // The same events again, as the watcher delivers the echo of a write the
    // daemon already observed: the namespace is unchanged and the rename
    // moved nothing, so no version is published.
    let echo = coordinator
        .reconcile_incremental(&mut writer, &batch)
        .unwrap();
    assert_eq!(echo.version, published.version);
}

/// A pending scan rejection is process state: an RPC authoring write keeps
/// its errors, only the scan that revalidates its subjects heals it, and a
/// restarted process knows it again once its first scan finds it.
#[cfg(unix)]
#[test]
fn a_pending_scan_rejection_survives_an_authoring_write_and_a_rescan() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let (bytes, bundle_uuid, asset_uuid) = ordinary_bundle();
    std::fs::create_dir_all(temp.path().join("assets")).unwrap();
    let bundle_path = temp.path().join("assets/ordinary.bundle");
    std::fs::write(&bundle_path, bytes).unwrap();
    let outside = temp.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    let link = temp.path().join("assets/escape");

    let pending = {
        let coordinator = coordinator(&temp);
        let mut writer = coordinator.open_writer().unwrap();
        coordinator.reconcile_full_scan(&mut writer).unwrap();
        symlink(&outside, &link).unwrap();
        coordinator
            .reconcile_incremental(
                &mut writer,
                &WatcherBatch {
                    paths: vec![link.clone()],
                    renames: Vec::new(),
                },
            )
            .unwrap();
        let hub = coordinator.server_handle();
        let pending = hub
            .scan_rejection()
            .expect("the unreadable subtree leaves a pending rejection");
        assert!(matches!(
            pending.errors[..],
            [ref error] if matches!(error.detail, NamespaceErrorV1::UnreadableScanSubtree { .. })
        ));
        assert!(!pending.subjects.is_empty());

        // An RPC authoring write, published by the watcher, keeps it.
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
        let base = coordinator.server().current_stamp().unwrap().version;
        let (_, version) =
            write_and_publish(&coordinator, &mut writer, base, &[operation], false).unwrap();
        assert_eq!(version, InputVersion(base.0 + 1));
        let store = coordinator.open_reader().unwrap();
        assert_eq!(hub.scan_rejection().as_ref(), Some(&pending));
        assert_eq!(hub.namespace_errors(&store).unwrap(), pending.errors);
        pending
    };

    // A restart starts without it; its first scan finds it again.
    let coordinator = coordinator(&temp);
    let hub = coordinator.server_handle();
    assert_eq!(hub.scan_rejection(), None);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    let store = coordinator.open_reader().unwrap();
    assert_eq!(hub.namespace_errors(&store).unwrap(), pending.errors);
    drop(store);

    // An unrelated observation does not heal it; revalidating its subject
    // does.
    std::fs::remove_file(&link).unwrap();
    let unrelated = temp.path().join("assets/unrelated.txt");
    std::fs::write(&unrelated, b"new observation").unwrap();
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![unrelated],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(
        hub.scan_rejection().map(|rejection| rejection.errors),
        Some(pending.errors)
    );
    coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![link],
                renames: Vec::new(),
            },
        )
        .unwrap();
    let store = coordinator.open_reader().unwrap();
    assert_eq!(hub.scan_rejection(), None);
    assert!(hub.namespace_errors(&store).unwrap().is_empty());
}

/// What one single-bundle edit's incremental publication reads, in pages
/// the loop's writer fetched, beside `filler` other files in the root.
fn single_edit_pages(filler: usize) -> u64 {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(assets.join("edit")).unwrap();
    for index in 0..filler {
        let directory = assets.join(format!("filler/d{}", index % 40));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("f{index}.txt")), index.to_string()).unwrap();
    }
    let (bytes, _, _) = ordinary_bundle();
    let edited = assets.join("edit/first.bundle");
    std::fs::write(&edited, bytes).unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();

    let mut bundle = distill_bundle::parse_bundle(&std::fs::read(&edited).unwrap()).unwrap();
    bundle.assets.get_mut("entry").unwrap().data = AuthoredValue::UInt(9);
    std::fs::write(&edited, distill_bundle::write_bundle(&bundle).unwrap()).unwrap();
    let before = writer.pages_fetched().unwrap();
    let base = writer.input_version().unwrap();
    let published = coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![edited],
                renames: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(published.version.0, base.0 + 1);
    writer.pages_fetched().unwrap() - before
}

/// An edit's publication reads its own subtree, never the root's other rows:
/// sixty times the files cost it no more than the deeper B-trees do (a read of
/// the whole root's file rows alone would cost about 50 pages more).
#[test]
fn a_single_bundle_edit_reads_independent_of_namespace_size() {
    let small = single_edit_pages(100);
    let large = single_edit_pages(6000);
    println!("single bundle edit: {small} pages beside 100 files, {large} beside 6000");
    assert!(
        large <= small + 16,
        "{small} pages beside 100 files, {large} beside 6000"
    );
}

/// A configuration source error and its heal are process state: they
/// publish no version and read no page of the store.
#[test]
fn a_configuration_error_publishes_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let (bytes, _, _) = ordinary_bundle();
    std::fs::write(assets.join("first.bundle"), bytes).unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    let ready = coordinator.configuration_status().unwrap();

    let base = writer.input_version().unwrap();
    let before = writer.pages_fetched().unwrap();
    coordinator
        .reject_configuration(
            DscpV1::MalformedConfiguration { file_hash: [5; 32] },
            "malformed",
        )
        .unwrap();
    assert!(matches!(
        coordinator.configuration_status().unwrap(),
        ConfigurationStatus::Failed(_)
    ));
    coordinator.heal_configuration_rejection();
    assert_eq!(
        format!("{:?}", coordinator.configuration_status().unwrap()),
        format!("{ready:?}")
    );
    assert_eq!(writer.pages_fetched().unwrap(), before);
    assert_eq!(writer.input_version().unwrap(), base);
}

static EDIT_STATEMENTS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

fn record_edit_statement(sql: &str) {
    EDIT_STATEMENTS.lock().unwrap().push(sql.to_owned());
}

/// One bundle edited beside `n` others, reconciled by a watcher pass: the
/// statements it ran and the pages it fetched.
fn edit_cost(n: u32) -> (Vec<String>, u64) {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    let bundle = |index: u32, value: u64| {
        let mut bytes = [0u8; 16];
        bytes[..4].copy_from_slice(&index.to_le_bytes());
        let (data, ..) = ordinary_bundle_with(0, 0, value);
        // Distinct bundle and asset ids per index.
        let mut parsed = distill_bundle::parse_bundle(&data).unwrap();
        parsed.uuid = BundleUuid({
            let mut uuid = bytes;
            uuid[15] = 0xB0;
            uuid
        });
        for entry in parsed.assets.values_mut() {
            entry.uuid = AssetUuid({
                let mut uuid = bytes;
                uuid[15] = 0xA0;
                uuid
            });
        }
        distill_bundle::write_bundle(&parsed).unwrap()
    };
    for index in 0..n {
        let dir = assets.join(format!("d{}", index % 20));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("b{index}.bundle")), bundle(index, 1)).unwrap();
    }
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator
        .reconcile_rescan(
            &mut writer,
            &mut distill_daemon::watcher::WatcherQueue::new(),
        )
        .unwrap();
    let edited = assets.join("d7/b7.bundle");
    let batch = WatcherBatch {
        paths: vec![edited.clone()],
        renames: Vec::new(),
    };
    // The first edit warms the connection's statement cache.
    std::fs::write(&edited, bundle(7, 2)).unwrap();
    coordinator
        .reconcile_batch(&mut writer, &batch, false)
        .unwrap();
    std::fs::write(&edited, bundle(7, 3)).unwrap();
    let pages = writer.pages_fetched().unwrap();
    EDIT_STATEMENTS.lock().unwrap().clear();
    writer.trace_statements(Some(record_edit_statement));
    coordinator
        .reconcile_batch(&mut writer, &batch, false)
        .unwrap();
    writer.trace_statements(None);
    let pages = writer.pages_fetched().unwrap() - pages;
    (std::mem::take(&mut *EDIT_STATEMENTS.lock().unwrap()), pages)
}

/// An edit's pass costs what the edit does, not what the namespace holds:
/// the same statements beside 40 bundles as beside 1200, and pages that
/// grow only with the B-trees' depth (237 and 332 when written).
#[test]
fn an_edit_pass_runs_the_same_statements_at_any_namespace_size() {
    let (small, small_pages) = edit_cost(40);
    let (large, large_pages) = edit_cost(1200);
    println!(
        "statements {} / {}, pages {small_pages} / {large_pages}",
        small.len(),
        large.len()
    );
    assert_eq!(small.len(), large.len(), "{small:#?}\n{large:#?}");
    assert!(
        2 * large_pages <= 3 * small_pages,
        "{small_pages} -> {large_pages} pages"
    );
}

/// The restart-only changes are the process's own state: a subscriber is
/// told the keys when its stream installs, again whenever their keys or
/// values change, and an empty set once they clear. A restart starts with
/// the file's values, so nothing is required of it.
#[test]
fn restart_required_is_told_as_it_changes_and_clears() {
    let temp = tempfile::tempdir().unwrap();
    let connect = |coordinator: &DaemonCoordinator| {
        let hub = match coordinator
            .server()
            .root()
            .connect(ConnectRequest::new("dev", TargetDefinitionHash([4; 32])))
        {
            ConnectOutcome::Connected(connected) => connected.hub,
            outcome => panic!("target connection failed: {outcome:?}"),
        };
        hub.subscribe(InputVersion(0), vec![], vec![])
            .success()
            .unwrap()
    };
    let restart_events = |subscription: &distill_rpc::SubscriptionInstall| {
        let mut keys = Vec::new();
        while let Some(event) = subscription.deltas.next() {
            if let StreamEvent::Asset {
                event: distill_rpc::AssetEvent::RestartRequired { keys: event },
                ..
            } = event
            {
                keys.push(event);
            }
        }
        keys
    };
    let auto_codegen = vec!["codegen.auto_codegen".to_owned()];
    {
        let coordinator = coordinator(&temp);
        let mut writer = coordinator.open_writer().unwrap();
        let version = coordinator.reconcile_full_scan(&mut writer).unwrap().version;
        let installed = connect(&coordinator);
        assert_eq!(restart_events(&installed), Vec::<Vec<String>>::new());
        coordinator
            .set_restart_required(&[RestartOnlyChange::AutoCodegen(true)])
            .unwrap();
        assert_eq!(restart_events(&installed), vec![auto_codegen.clone()]);
        // The same values again are no change.
        coordinator
            .set_restart_required(&[RestartOnlyChange::AutoCodegen(true)])
            .unwrap();
        assert_eq!(restart_events(&installed), Vec::<Vec<String>>::new());
        // A stream installed now is told the keys as they are.
        let late = connect(&coordinator);
        assert_eq!(restart_events(&late), vec![auto_codegen.clone()]);
        // Another value of the same key is a change.
        coordinator
            .set_restart_required(&[RestartOnlyChange::AutoCodegen(false)])
            .unwrap();
        assert_eq!(restart_events(&installed), vec![auto_codegen]);
        // The file back at the running values clears it.
        coordinator.set_restart_required(&[]).unwrap();
        assert_eq!(restart_events(&installed), vec![Vec::<String>::new()]);
        // A stream that missed changes is told only where they ended.
        assert_eq!(restart_events(&late), vec![Vec::<String>::new()]);
        // A non-loopback address is rejected and changes nothing.
        let non_loopback = "10.0.0.5:9999".parse().unwrap();
        assert!(coordinator
            .set_restart_required(&[RestartOnlyChange::Address(non_loopback)])
            .is_err());
        assert_eq!(restart_events(&installed), Vec::<Vec<String>>::new());
        assert_eq!(
            writer.input_version().unwrap(),
            version,
            "restart-only changes publish nothing"
        );
        coordinator
            .set_restart_required(&[RestartOnlyChange::AutoCodegen(true)])
            .unwrap();
    }
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    assert_eq!(
        restart_events(&connect(&coordinator)),
        Vec::<Vec<String>>::new()
    );
}

/// The change log holds only real changes: the first publication on an
/// empty store logs nothing (no client holds the version before it), and a
/// full rescan logs only the assets and paths that changed.
#[test]
fn a_full_rescan_logs_only_what_changed() {
    use distill_store::served::Change;
    let temp = tempfile::tempdir().unwrap();
    let coordinator = coordinator(&temp);
    let mut writer = coordinator.open_writer().unwrap();
    let assets = temp.path().join("assets");
    let (edited, _, edited_asset) = ordinary_bundle_with(73, 72, 7);
    let (kept, _, kept_asset) = ordinary_bundle_with(83, 82, 7);
    std::fs::write(assets.join("edited.bundle"), edited).unwrap();
    std::fs::write(assets.join("kept.bundle"), kept).unwrap();
    let log = |version: InputVersion| {
        coordinator
            .open_reader()
            .unwrap()
            .change_log_after(0)
            .unwrap()
            .into_iter()
            .filter(|entry| entry.version == version)
            .map(|entry| entry.change)
            .collect::<Vec<_>>()
    };

    let first = coordinator
        .reconcile_full_scan(&mut writer)
        .unwrap()
        .version;
    assert_eq!(first, InputVersion(1));
    assert_eq!(log(first), []);

    let (edited, _, _) = ordinary_bundle_with(73, 72, 8);
    std::fs::write(assets.join("edited.bundle"), edited).unwrap();
    let edit = coordinator
        .reconcile_full_scan(&mut writer)
        .unwrap()
        .version;
    assert_eq!(edit, InputVersion(2));
    assert_eq!(
        log(edit),
        [Change::Asset {
            asset: edited_asset,
            state: 0
        }]
    );

    std::fs::remove_file(assets.join("kept.bundle")).unwrap();
    let removal = coordinator
        .reconcile_full_scan(&mut writer)
        .unwrap()
        .version;
    assert_eq!(
        log(removal),
        [
            Change::Asset {
                asset: kept_asset,
                state: 1
            },
            Change::Path {
                path: "kept.bundle".to_owned()
            },
        ]
    );
}
