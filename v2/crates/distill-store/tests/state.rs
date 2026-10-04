//! §13 consistency-contract state machinery: version counters and the
//! snapshot stamp (RPC-side realization of `IoBasis::Rpc`, §15).

use distill_store::state::{
    InputVersion, MemoSeq, SnapshotStamp, StoreInstanceId,
};

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
}
