//! §18 configuration state: scheduler bounds/live resize, restart-only
//! changes, and configuration errors.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use distill_store::config::{ConfigValidationError, RestartOnlyChange};
use distill_store::state::{ConfigurationError, DscpV1};
use distill_store::{Store, StoreConfig};

fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, store)
}

#[test]
fn scheduler_bounds_preserve_progress_for_both_classes() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path().join(".distill"));
    cfg.parallelism = 0;
    assert_eq!(
        cfg.validate_scheduler(),
        Err(ConfigValidationError::ParallelismZero)
    );

    cfg.parallelism = 4;
    cfg.batch_reserved_workers = 0;
    assert!(matches!(
        cfg.validate_scheduler(),
        Err(ConfigValidationError::BatchReservationOutOfBounds { .. })
    ));
    cfg.batch_reserved_workers = 4;
    assert!(matches!(
        cfg.validate_scheduler(),
        Err(ConfigValidationError::BatchReservationOutOfBounds { max: 3, .. })
    ));

    cfg.parallelism = 1;
    cfg.batch_reserved_workers = 1;
    cfg.validate_scheduler().unwrap();
}

#[test]
fn operational_store_values_apply_without_copying_restart_state() {
    let (_dir, mut store) = open();
    let original_state_path = store.config().clone().state_path;
    let mut candidate = StoreConfig::new("/a/restart-only/path");
    candidate.segment_size = 4096;
    candidate.cache_limit = 8192;
    candidate.parallelism = 3;
    candidate.batch_reserved_workers = 2;

    store.apply_operational_config(&candidate).unwrap();
    let applied = store.config().clone();
    assert_eq!(applied.state_path, original_state_path);
    assert_eq!(applied.segment_size, 4096);
    assert_eq!(applied.cache_limit, 8192);
    assert_eq!(applied.parallelism, 3);
    assert_eq!(applied.batch_reserved_workers, 2);
    assert_eq!(store.input_version().unwrap().0, 0);
}

#[test]
fn restart_only_changes_name_their_keys_once_in_key_order() {
    let rows = RestartOnlyChange::key_values(&[
        RestartOnlyChange::Address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999)),
        RestartOnlyChange::AutoCodegen(false),
        RestartOnlyChange::AutoCodegen(false),
    ])
    .unwrap();
    assert_eq!(
        rows,
        [
            ("codegen.auto_codegen", "false".to_owned()),
            ("daemon.address", "127.0.0.1:9999".to_owned()),
        ]
    );
    assert_eq!(RestartOnlyChange::key_values(&[]).unwrap(), []);
}

#[test]
fn invalid_restart_value_is_rejected() {
    let non_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 9999);
    let err = RestartOnlyChange::key_values(&[RestartOnlyChange::Address(non_loopback)])
        .unwrap_err();
    assert!(matches!(err, ConfigValidationError::NonLoopbackAddress(a) if a == non_loopback));
}

#[test]
fn invalid_configuration_candidate_publishes_typed_snapshot_error() {
    let (_dir, mut store) = open();
    store
        .input_transaction(|txn| {
            txn.set_configuration_source_error(Some(&ConfigurationError::from_reason(
                &DscpV1::NonLoopbackAddress {
                    address: "10.0.0.5:9999".to_owned(),
                },
                "non-loopback daemon address",
            )))
        })
        .unwrap();
    let err = store.configuration_error().unwrap().unwrap();
    assert!(err.message.contains("non-loopback"));
}
