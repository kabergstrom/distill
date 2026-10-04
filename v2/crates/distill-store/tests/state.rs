//! §13 consistency-contract state machinery: version counters, the
//! snapshot stamp (RPC-side realization of `IoBasis::Rpc`, §15), the
//! operation classification.

use std::sync::Arc;

use distill_store::state::{
    ConfigurationEpoch, ConfigurationError, ConfigurationState, DscpV1, InputVersion, MemoSeq,
    OperationKind, SnapshotStamp, StoreInstanceId,
};

fn configuration() -> Arc<ConfigurationEpoch> {
    Arc::new(ConfigurationEpoch { generation: 7 })
}

fn configuration_error() -> ConfigurationError {
    ConfigurationError::from_reason(
        &DscpV1::NonLoopbackAddress {
            address: "10.0.0.5:9999".to_owned(),
        },
        "daemon.address is not loopback",
    )
}

// ---- version counters ----

#[test]
fn input_version_and_memo_seq_are_ordered_u64_newtypes() {
    assert!(InputVersion(1) < InputVersion(2));
    assert!(MemoSeq(41) < MemoSeq(42));
    assert_eq!(InputVersion(7), InputVersion(7));
    assert_eq!(MemoSeq(7), MemoSeq(7));
}

// ---- StoreInstanceId / SnapshotStamp ----

#[test]
fn minted_instance_ids_are_distinct() {
    let a = StoreInstanceId::mint();
    let b = StoreInstanceId::mint();
    assert_ne!(a, b, "two mints must not alias");
    assert_ne!(
        a.0, [0u8; 16],
        "a mint of all zeroes is vanishingly unlikely random output"
    );
}

#[test]
fn snapshot_stamps_compare_only_within_one_instance() {
    let inst_a = StoreInstanceId::mint();
    let inst_b = StoreInstanceId::mint();
    let s1 = SnapshotStamp {
        instance: inst_a,
        version: InputVersion(4),
    };
    let s2 = SnapshotStamp {
        instance: inst_a,
        version: InputVersion(4),
    };
    let s3 = SnapshotStamp {
        instance: inst_b,
        version: InputVersion(4),
    };
    assert_eq!(s1, s2);
    // Same bare u64, different instance: never equal — an InputVersion is
    // ordered only within one store instance (§13).
    assert_ne!(s1, s3);
    assert!(s1.same_instance(&s2));
    assert!(!s1.same_instance(&s3));
}

// ---- OperationKind ----

#[test]
fn operations_are_classified_by_their_need_for_the_pipeline() {
    // §13: "pure-metadata reads — the path index, input versions, CAS
    // reads, lease pinning — remain valid under poison"; anything needing
    // the pipeline map, registry, defaults or migration fns fails.
    for op in [
        OperationKind::PathIndex,
        OperationKind::InputVersionRead,
        OperationKind::CasRead,
        OperationKind::LeasePin,
    ] {
        assert!(!op.requires_epoch(), "{op:?} is pure metadata");
    }
    for op in [
        OperationKind::LoadCurrent,
        OperationKind::TerminalTypeQuery,
        OperationKind::DerivedOutputNamespace,
        OperationKind::Build,
    ] {
        assert!(op.requires_epoch(), "{op:?} needs the pipeline");
    }
}

// ---- ConfigurationState (R22/H4) ----

#[test]
fn configuration_error_never_serves_last_good_as_current() {
    let state = ConfigurationState::Failed {
        reason: configuration_error(),
        last_good: Some(configuration()),
    };
    assert!(state.epoch().is_err());
    for op in [
        OperationKind::Authoring,
        OperationKind::TargetBoundRpc,
        OperationKind::TerminalTypeQuery,
        OperationKind::DerivedOutputNamespace,
        OperationKind::Build,
    ] {
        let err = state
            .check(op)
            .expect_err("configuration-dependent operation must fail");
        assert!(err.message.contains("loopback"));
    }
}

#[test]
fn configuration_error_keeps_only_explicitly_pure_operations_available() {
    let state = ConfigurationState::Failed {
        reason: configuration_error(),
        last_good: Some(configuration()),
    };
    for op in [
        OperationKind::SnapshotRead,
        OperationKind::PathIndex,
        OperationKind::InputVersionRead,
        OperationKind::CasRead,
        OperationKind::LeasePin,
        OperationKind::LoadCurrent,
    ] {
        assert!(!op.requires_configuration());
        assert!(state.check(op).unwrap().is_none());
    }
}

#[test]
fn ready_configuration_supplies_its_snapshot_pinned_epoch() {
    let state = ConfigurationState::Ready(configuration());
    let got = state
        .check(OperationKind::Build)
        .unwrap()
        .expect("configuration-dependent operations consume the epoch");
    assert_eq!(got.generation, 7);
    assert!(state.check(OperationKind::CasRead).unwrap().is_none());
}
