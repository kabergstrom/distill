use std::collections::BTreeMap;

use distill_bundle::{AssetEntry, Bundle};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_daemon::coordinator::DaemonCoordinator;
use distill_daemon::scanner::AssetRoot;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, DeferredOperationResult, InputVersion, LongRunningOp,
    PreparedOperationPublication, RenameWithFixupsRequest, TargetDefinition, TargetDefinitionHash,
};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use distill_store::StoreConfig;

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

fn complete(
    publication: PreparedOperationPublication,
    base: InputVersion,
) -> DeferredOperationResult {
    match publication {
        PreparedOperationPublication::Deferred(operation) => operation.complete(base).unwrap(),
        PreparedOperationPublication::Immediate(_) => panic!("production operations are deferred"),
    }
}

/// Complete a deferred operation as the durable step of its coordinated
/// publication (the server's completion path); returns its terminal error.
fn complete_and_publish(
    coordinator: &DaemonCoordinator,
    publication: PreparedOperationPublication,
    base: InputVersion,
) -> Option<String> {
    let mut terminal_error = None;
    coordinator
        .coordinated_commit(base, || {
            let completed = complete(publication, base);
            terminal_error = completed.terminal_error;
            Ok(completed.commit)
        })
        .unwrap();
    terminal_error
}

#[test]
fn rename_with_fixups_is_deferred_and_rescanned_as_one_version() {
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
        vec![AssetRoot::new("main", &assets)],
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
    let prepared = coordinator.authoring_service().prepare_operation(base, &LongRunningOp::RenameWithFixups(request.encode()))
        .unwrap();

    assert!(assets.join("old.bundle").exists());
    assert!(!assets.join("renamed.bundle").exists());
    assert_eq!(
        complete_and_publish(&coordinator, prepared.publication, base),
        None
    );

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
        coordinator.store().read().input_version(),
        InputVersion(2)
    );
    assert_eq!(
        coordinator.server().current_stamp().version,
        InputVersion(2)
    );
}

