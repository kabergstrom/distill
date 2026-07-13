//! §18 configuration state: scheduler bounds/live resize, declared change
//! classes, pending-restart generations, and configuration poison.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use distill_store::config::{change_class, ChangeClass, ConfigValidationError, RestartOnlyChange};
use distill_store::state::{ConfigurationState, OperationKind};
use distill_store::{Store, StoreConfig};

fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, store)
}

#[test]
fn batch_reservation_has_a_declared_operational_live_change_class() {
    assert_eq!(
        change_class("pipeline.batch_reserved_workers"),
        Some(ChangeClass::OperationalLive)
    );
    assert_eq!(
        change_class("daemon.address"),
        Some(ChangeClass::RestartOnly)
    );
    assert_eq!(
        change_class("targets"),
        Some(ChangeClass::InputVersionedEpoch)
    );
    assert_eq!(change_class("future.unclassified"), None);
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
fn live_resize_reclamps_reservation_and_drains_excess_active_slots() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path().join(".distill"));
    cfg.batch_reserved_workers = 4;
    let resize = cfg.resize_parallelism(3, 8).unwrap();
    assert_eq!(cfg.parallelism, 3);
    assert_eq!(
        cfg.batch_reserved_workers, 2,
        "one interactive slot remains"
    );
    assert_eq!(resize.active_slots_to_drain, 5);
    assert!(!resize.single_worker_alternates);

    let one = cfg.resize_parallelism(1, 3).unwrap();
    assert_eq!(cfg.batch_reserved_workers, 1);
    assert_eq!(one.active_slots_to_drain, 2);
    assert!(
        one.single_worker_alternates,
        "the sole slot alternates oldest batch/interactive"
    );
}

#[test]
fn restart_only_changes_stage_without_advancing_or_replacing_active_values() {
    let (_dir, mut store) = open();
    let before = store.input_version();
    let pending = store
        .stage_pending_restart(&[
            RestartOnlyChange::Address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999)),
            RestartOnlyChange::AutoCodegen(false),
        ])
        .unwrap();
    assert_eq!(store.input_version(), before);
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
fn invalid_restart_value_is_rejected_before_pending_state_exists() {
    let (_dir, mut store) = open();
    let non_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 9999);
    let err = store
        .stage_pending_restart(&[RestartOnlyChange::Address(non_loopback)])
        .unwrap_err();
    assert!(matches!(err, ConfigValidationError::NonLoopbackAddress(a) if a == non_loopback));
    assert!(store.pending_restart().unwrap().is_none());
    assert_eq!(store.input_version().0, 0);
}

#[test]
fn invalid_configuration_candidate_publishes_typed_snapshot_poison() {
    let (_dir, mut store) = open();
    store
        .input_transaction(|txn| txn.publish_configuration_poison("non-loopback daemon address"))
        .unwrap();
    let state = store.configuration_state().unwrap();
    assert!(matches!(state, ConfigurationState::Poisoned { .. }));
    let err = state.check(OperationKind::TargetBoundRpc).unwrap_err();
    assert!(err.error.contains("non-loopback"));
    assert!(state.check(OperationKind::SnapshotRead).is_ok());
}
