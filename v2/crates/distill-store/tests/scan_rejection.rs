//! The pending scan rejection and the configuration source's error are
//! durable rows: they survive a reopen, a write of the scan's own namespace
//! errors leaves them alone, and the configuration status is selected from
//! them.

use distill_store::errors::ScanRejectionRecord;
use distill_store::state::{
    ConfigurationError, ConfigurationState, DirectoryAliasSide, DscpV1, NamespaceError,
    NamespaceErrorV1, ScanFailureCode, ScanSubject,
};
use distill_store::{Store, StoreConfig};

fn config(dir: &tempfile::TempDir) -> StoreConfig {
    StoreConfig::new(dir.path().join(".distill"))
}

fn unreadable(root: &str) -> NamespaceError {
    NamespaceError::new(
        NamespaceErrorV1::UnreadableScanSubtree {
            subject: ScanSubject::Root {
                root_name: root.to_owned(),
            },
            failure: ScanFailureCode::PermissionDenied,
        },
        format!("{root} is unreadable"),
    )
    .unwrap()
}

fn alias() -> ConfigurationError {
    ConfigurationError::from_reason(
        &DscpV1::DirectoryAlias {
            first: DirectoryAliasSide {
                normalized_path: "a".into(),
            },
            second: DirectoryAliasSide {
                normalized_path: "b".into(),
            },
        },
        "a and b alias",
    )
}

fn malformed() -> ConfigurationError {
    ConfigurationError::from_reason(
        &DscpV1::MalformedConfiguration { file_hash: [7; 32] },
        "distill.toml is malformed",
    )
}

#[test]
fn a_pending_rejection_survives_reopen_and_namespace_rewrites() {
    let dir = tempfile::tempdir().unwrap();
    let rejection = ScanRejectionRecord {
        errors: vec![unreadable("main")],
        configuration: Some(alias()),
        subjects: vec![b"/assets/locked".to_vec(), b"/assets/a".to_vec()],
    };
    {
        let mut store = Store::open(config(&dir)).unwrap();
        assert_eq!(store.scan_rejection().unwrap(), None);
        store
            .input_transaction(|txn| txn.set_scan_rejection(Some(&rejection)))
            .unwrap();
        // Another writer replaces the scan's own namespace errors.
        store
            .input_transaction(|txn| txn.set_namespace_errors(Vec::new()).map(|_| ()))
            .unwrap();
    }
    let mut store = Store::open(config(&dir)).unwrap();
    let stored = store.scan_rejection().unwrap().unwrap();
    assert_eq!(stored.errors, rejection.errors);
    assert_eq!(stored.configuration, rejection.configuration);
    assert_eq!(
        stored.subjects,
        vec![b"/assets/a".to_vec(), b"/assets/locked".to_vec()]
    );
    // Readers see the rejection's namespace errors beside the scan's.
    assert_eq!(store.namespace_errors().unwrap(), rejection.errors);

    store
        .input_transaction(|txn| txn.set_scan_rejection(None))
        .unwrap();
    assert_eq!(store.scan_rejection().unwrap(), None);
    assert!(store.namespace_errors().unwrap().is_empty());
}

#[test]
fn the_configuration_status_selects_from_the_stored_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(config(&dir)).unwrap();
    store
        .input_transaction(|txn| {
            txn.set_scan_rejection(Some(&ScanRejectionRecord {
                configuration: Some(alias()),
                ..ScanRejectionRecord::default()
            }))?;
            txn.set_configuration_source_error(Some(&malformed()))?;
            txn.publish_configuration_status(3).map(|_| ())
        })
        .unwrap();
    // Malformed (code 1) orders before the alias (code 9).
    assert_eq!(store.configuration_source_error().unwrap(), Some(malformed()));
    assert_eq!(store.configuration_error().unwrap(), Some(malformed()));
    match store.configuration_state().unwrap() {
        ConfigurationState::Failed { reason, .. } => assert_eq!(reason, malformed()),
        state => panic!("expected the source error, got {state:?}"),
    }

    store
        .input_transaction(|txn| {
            txn.set_configuration_source_error(None)?;
            txn.publish_configuration_status(3).map(|_| ())
        })
        .unwrap();
    assert_eq!(store.configuration_error().unwrap(), Some(alias()));

    store
        .input_transaction(|txn| {
            txn.set_scan_rejection(None)?;
            txn.publish_configuration_status(3).map(|_| ())
        })
        .unwrap();
    assert_eq!(store.configuration_error().unwrap(), None);
    match store.configuration_state().unwrap() {
        ConfigurationState::Ready(epoch) => assert_eq!(epoch.generation, 3),
        state => panic!("expected ready, got {state:?}"),
    }
}

#[test]
fn the_compiled_version_is_the_marking_input_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let marked = {
        let mut store = Store::open(config(&dir)).unwrap();
        assert_eq!(store.compiled_version().unwrap(), None);
        let ((), marked) = store.input_transaction(|txn| txn.mark_compiled()).unwrap();
        // A later input that compiles nothing keeps the marker.
        store.input_transaction(|_| Ok(())).unwrap();
        // A rolled-back marking leaves it.
        let rolled_back = store.input_transaction(|txn| {
            txn.mark_compiled()?;
            Err::<(), _>(distill_store::StoreError::Rejected {
                detail: "test".to_owned(),
            })
        });
        assert!(rolled_back.is_err());
        marked
    };
    let store = Store::open(config(&dir)).unwrap();
    assert_eq!(store.compiled_version().unwrap(), Some(marked));
}
