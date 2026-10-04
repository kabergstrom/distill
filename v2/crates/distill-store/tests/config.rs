//! §18 configuration state: scheduler bounds/live resize, pending-restart
//! generations, and configuration errors.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use distill_store::config::{ConfigValidationError, RestartOnlyChange};
use distill_store::state::{ConfigurationError, ConfigurationState, DscpV1};
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
fn restart_only_changes_stage_without_advancing_or_replacing_active_values() {
    let (_dir, mut store) = open();
    let before = store.input_version().unwrap();
    let pending = store
        .stage_pending_restart(&[
            RestartOnlyChange::Address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999)),
            RestartOnlyChange::AutoCodegen(false),
        ])
        .unwrap();
    assert_eq!(store.input_version().unwrap(), before);
    assert_eq!(pending.generation, 1);
    assert_eq!(pending.keys, ["codegen.auto_codegen", "daemon.address"]);
    assert_eq!(store.pending_restart().unwrap(), Some(pending.clone()));
    assert!(matches!(
        store.configuration_state().unwrap(),
        ConfigurationState::Ready(ref epoch) if epoch.generation == 0
    ));

    // Restart adoption is the input event: only now does the active
    // generation change and the pending marker clear.
    let (_, adopted_at) = store
        .input_transaction(|txn| txn.adopt_pending_restart())
        .unwrap();
    assert_eq!(adopted_at.0, before.0 + 1);
    assert!(store.pending_restart().unwrap().is_none());
    assert!(matches!(
        store.configuration_state().unwrap(),
        ConfigurationState::Ready(ref epoch) if epoch.generation == 1
    ));
}

#[test]
fn a_reverted_restart_candidate_clears_pending_state_without_advancing() {
    let (_dir, mut store) = open();
    store
        .stage_pending_restart(&[RestartOnlyChange::AutoCodegen(true)])
        .unwrap();
    let before = store.input_version().unwrap();
    store.clear_pending_restart().unwrap();
    assert_eq!(store.input_version().unwrap(), before);
    assert!(store.pending_restart().unwrap().is_none());
}

#[test]
fn invalid_restart_value_is_rejected_before_pending_state_exists() {
    let (_dir, mut store) = open();
    let non_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 9999);
    let err = store
        .stage_pending_restart(&[RestartOnlyChange::Address(non_loopback)])
        .unwrap_err();
    assert!(matches!(err, ConfigValidationError::NonLoopbackAddress(a) if a == non_loopback));
    assert!(store.pending_restart().unwrap().is_none());
    assert_eq!(store.input_version().unwrap().0, 0);
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
    let state = store.configuration_state().unwrap();
    assert!(matches!(state, ConfigurationState::Failed { .. }));
    let err = state.epoch().unwrap_err();
    assert!(err.message.contains("non-loopback"));
}
