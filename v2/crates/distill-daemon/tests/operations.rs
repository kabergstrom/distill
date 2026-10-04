use std::collections::BTreeMap;

use distill_bundle::{AssetEntry, Bundle};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_daemon::coordinator::DaemonCoordinator;
use distill_daemon::scanner::AssetRoot;
use distill_daemon::watcher::WatcherBatch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, AuthoringProgressState, DeferredOperationResult, InputVersion, LongRunningOp,
    PreparedOperationCommit, PreparedOperationPublication, RenameWithFixupsRequest,
    TargetDefinition, TargetDefinitionHash, WriteReceipt,
};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use distill_store::atomic_file::{self, Expected};
use distill_store::{Store, StoreConfig, StoreWriter};

const VALUE_TYPE: TypeUuid = TypeUuid([81; 16]);
const REF_TYPE: TypeUuid = TypeUuid([82; 16]);

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

/// Complete a deferred operation as the server does, inside an input at
/// `base`, and return its result. A rename publishes nothing.
fn complete(
    coordinator: &DaemonCoordinator,
    writer: &mut Store,
    publication: PreparedOperationPublication,
    base: InputVersion,
) -> Result<DeferredOperationResult, String> {
    let PreparedOperationPublication::Deferred(operation) = publication else {
        panic!("production operations are deferred")
    };
    let mut completed = None;
    coordinator
        .server_handle()
        .coordinated_maybe_commit(writer, base, |store| {
            let result = operation.complete(store, base);
            let commit = result
                .as_ref()
                .ok()
                .and_then(|result| result.commit.clone());
            completed = Some(result);
            Ok(commit)
        })
        .unwrap();
    completed.unwrap()
}

/// The watcher's batch for a receipt's files: the version it publishes
/// reflects every file.
fn publish_receipt(
    coordinator: &DaemonCoordinator,
    writer: &mut Store,
    receipt: &WriteReceipt,
) -> InputVersion {
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
            "{file:?}"
        );
    }
    version
}

fn value_bundle() -> Vec<u8> {
    bundle(
        BundleUuid([83; 16]),
        AssetUuid([84; 16]),
        VALUE_TYPE,
        LogicalSchema {
            root: SchemaNode::Primitive(PrimitiveKind::U8),
        },
        AuthoredValue::UInt(7),
    )
}

/// The consumer bundle, referencing the value bundle at `path`.
fn consumer_bundle(path: &str) -> Vec<u8> {
    bundle(
        BundleUuid([85; 16]),
        AssetUuid([86; 16]),
        REF_TYPE,
        LogicalSchema {
            root: SchemaNode::AssetRef(VALUE_TYPE),
        },
        AuthoredValue::Object(BTreeMap::from([
            ("asset".into(), AuthoredValue::Str("entry".into())),
            ("path".into(), AuthoredValue::Str(path.into())),
        ])),
    )
}

/// `old.bundle` and a consumer referencing it, published at version 1.
fn rename_world(temp: &tempfile::TempDir) -> (std::path::PathBuf, DaemonCoordinator, StoreWriter) {
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(assets.join("old.bundle"), value_bundle()).unwrap();
    std::fs::write(
        assets.join("consumer.bundle"),
        consumer_bundle("old.bundle"),
    )
    .unwrap();
    let coordinator = DaemonCoordinator::open(
        StoreConfig::new(temp.path().join(".distill")),
        vec![AssetRoot::new("main", &assets)],
        vec![target()],
        64,
    )
    .unwrap();
    let mut writer = coordinator.open_writer().unwrap();
    coordinator.reconcile_full_scan(&mut writer).unwrap();
    (assets, coordinator, writer)
}

fn rename_request() -> LongRunningOp {
    LongRunningOp::RenameWithFixups(
        RenameWithFixupsRequest {
            bundle: BundleUuid([83; 16]),
            destination_root: "main".into(),
            destination_path: "renamed.bundle".into(),
        }
        .encode(),
    )
}

/// The receipt a prepared operation's Completed event carries.
fn completed_receipt(prepared: &PreparedOperationCommit) -> WriteReceipt {
    let completed = prepared
        .progress
        .iter()
        .find(|event| event.state == AuthoringProgressState::Completed)
        .unwrap();
    WriteReceipt::decode(&completed.payload).unwrap()
}

#[test]
fn rename_with_fixups_writes_files_and_the_watcher_publishes_them() {
    let temp = tempfile::tempdir().unwrap();
    let (assets, coordinator, mut writer) = rename_world(&temp);
    let base = InputVersion(1);
    let prepared = coordinator
        .authoring_service()
        .prepare_operation(&mut writer, base, &rename_request())
        .unwrap();
    let receipt = completed_receipt(&prepared);
    assert_eq!(
        receipt
            .files
            .iter()
            .map(|file| (file.path.as_str(), file.content_hash.is_some()))
            .collect::<Vec<_>>(),
        [
            ("consumer.bundle", true),
            ("old.bundle", false),
            ("renamed.bundle", true)
        ]
    );

    assert!(assets.join("old.bundle").exists());
    assert!(!assets.join("renamed.bundle").exists());
    let completed = complete(&coordinator, &mut writer, prepared.publication, base).unwrap();
    assert_eq!(completed.commit, None);
    assert_eq!(completed.terminal_error, None);

    assert!(!assets.join("old.bundle").exists());
    assert_eq!(
        std::fs::read(assets.join("renamed.bundle")).unwrap(),
        value_bundle()
    );
    assert_eq!(
        std::fs::read(assets.join("consumer.bundle")).unwrap(),
        consumer_bundle("renamed.bundle")
    );
    assert!(std::fs::read_dir(assets.join(".distill-staging"))
        .unwrap()
        .next()
        .is_none());
    // The completion published nothing; the watcher publishes the files.
    assert_eq!(coordinator.server().current_stamp().unwrap().version, base);
    assert_eq!(
        publish_receipt(&coordinator, &mut writer, &receipt),
        InputVersion(2)
    );
    assert_eq!(
        coordinator.server().current_stamp().unwrap().version,
        InputVersion(2)
    );
}

/// Every pre-image and the destination are checked before the first
/// rename: a conflict then fails the rename and changes no file.
#[test]
fn a_rename_conflict_found_before_the_first_rename_changes_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let (assets, coordinator, mut writer) = rename_world(&temp);
    let base = InputVersion(1);
    let prepared = coordinator
        .authoring_service()
        .prepare_operation(&mut writer, base, &rename_request())
        .unwrap();
    // The destination appears after the plan.
    std::fs::write(assets.join("renamed.bundle"), b"someone else's").unwrap();

    let error = complete(&coordinator, &mut writer, prepared.publication, base).unwrap_err();
    assert!(error.contains("renamed.bundle"), "{error}");
    assert_eq!(
        std::fs::read(assets.join("old.bundle")).unwrap(),
        value_bundle()
    );
    assert_eq!(
        std::fs::read(assets.join("consumer.bundle")).unwrap(),
        consumer_bundle("old.bundle")
    );
    assert_eq!(
        std::fs::read(assets.join("renamed.bundle")).unwrap(),
        b"someone else's"
    );
    assert!(std::fs::read_dir(assets.join(".distill-staging"))
        .unwrap()
        .next()
        .is_none());
}

/// A crash between the renames: the consumer already references the
/// destination, the bundle has not moved. That is ordinary authored input
/// (the consumer's reference does not resolve until the move), the pass
/// publishes it with no namespace error, and the same request, retried at
/// the new version, plans and completes only the move.
#[test]
fn a_rename_cut_short_between_its_renames_is_finished_by_a_retry() {
    let temp = tempfile::tempdir().unwrap();
    let (assets, coordinator, mut writer) = rename_world(&temp);
    // Step 1 of the rename landed, as one atomic replace; then the crash.
    let consumer = assets.join("consumer.bundle");
    atomic_file::write(
        &assets,
        &consumer,
        &consumer_bundle("renamed.bundle"),
        Expected::Any,
    )
    .unwrap();
    let version = coordinator
        .reconcile_incremental(
            &mut writer,
            &WatcherBatch {
                paths: vec![consumer.clone()],
                renames: Vec::new(),
            },
        )
        .unwrap()
        .version;
    assert_eq!(version, InputVersion(2));
    let reader = coordinator.open_reader().unwrap();
    assert!(reader.namespace_errors().unwrap().is_empty());
    assert!(reader
        .bundles_referencing_path("old.bundle")
        .unwrap()
        .is_empty());
    assert_eq!(
        reader.bundles_referencing_path("renamed.bundle").unwrap(),
        [BundleUuid([85; 16])]
    );
    drop(reader);

    let prepared = coordinator
        .authoring_service()
        .prepare_operation(&mut writer, version, &rename_request())
        .unwrap();
    let receipt = completed_receipt(&prepared);
    assert_eq!(
        receipt
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        ["old.bundle", "renamed.bundle"],
        "only the move remains"
    );
    let completed = complete(&coordinator, &mut writer, prepared.publication, version).unwrap();
    assert_eq!(completed.terminal_error, None);
    assert_eq!(
        publish_receipt(&coordinator, &mut writer, &receipt),
        InputVersion(3)
    );
    assert!(!assets.join("old.bundle").exists());
    assert_eq!(
        std::fs::read(assets.join("renamed.bundle")).unwrap(),
        value_bundle()
    );
    assert_eq!(
        std::fs::read(&consumer).unwrap(),
        consumer_bundle("renamed.bundle")
    );
}
