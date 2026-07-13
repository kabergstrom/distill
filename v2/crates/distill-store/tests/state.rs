//! §13 consistency-contract state machinery: version counters, the
//! snapshot stamp (RPC-side realization of `IoBasis::Rpc`, §15), the
//! `PipelineState` valid/invalid operation classification, and the
//! load-policy digest.

use std::collections::BTreeMap;
use std::sync::Arc;

use distill_core::attestation::CompiledAttestationDigest;
use distill_core::id::TypeUuid;
use distill_core::target_set::CanonicalTargetSet;
use distill_store::state::{
    load_policy_digest, CleanupDisposition, ConfigurationEpoch, ConfigurationPoison,
    ConfigurationState, DscpV1, InputVersion, MemoSeq, OperationKind, PipelineEpoch,
    PipelinePoison, PipelinePoisonCode, PipelinePoisonOrigin, PipelineState, Registration,
    RegistrationKind, SnapshotStamp, StoreInstanceId,
};

fn epoch() -> Arc<PipelineEpoch> {
    Arc::new(PipelineEpoch {
        dylib_hash: [7u8; 32],
        load_policy_digest: [9u8; 32],
        compiled_types: CompiledAttestationDigest([10u8; 32]),
        target_set: CanonicalTargetSet::canonical(vec![]).unwrap(),
        schema_registry: BTreeMap::new(),
        registrations: vec![Registration {
            kind: RegistrationKind::Processor,
            id: "tex-compress".to_owned(),
            version: 3,
        }],
    })
}

fn poison() -> PipelinePoison {
    PipelinePoison::new(
        PipelinePoisonCode::CandidateRegistration,
        PipelinePoisonOrigin::CandidateOpen,
        CleanupDisposition::CleanedAndClosed,
        "duplicate processor id `tex-compress`",
    )
    .unwrap()
}

fn configuration() -> Arc<ConfigurationEpoch> {
    Arc::new(ConfigurationEpoch { generation: 7 })
}

fn configuration_poison() -> ConfigurationPoison {
    ConfigurationPoison::from_reason(
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

// ---- PipelineState ----

#[test]
fn ready_state_yields_its_epoch() {
    let e = epoch();
    let state = PipelineState::Ready(e.clone());
    let got = state.epoch().expect("ready state has an epoch");
    assert_eq!(got.dylib_hash, e.dylib_hash);
}

#[test]
fn poisoned_state_epoch_is_the_named_error() {
    let state = PipelineState::Poisoned {
        error: poison(),
        last_good: None,
    };
    let err = state.epoch().expect_err("poisoned version has no epoch");
    assert!(err.to_string().contains("tex-compress"));
}

#[test]
fn last_good_is_residency_bookkeeping_never_served() {
    // §13: "the prior epoch is never silently retained as the new
    // version's code" — even with last_good resident, epoch() fails.
    let state = PipelineState::Poisoned {
        error: poison(),
        last_good: Some(epoch()),
    };
    assert!(state.epoch().is_err());
    assert!(state.check(OperationKind::Build).is_err());
    assert!(state.check(OperationKind::LoadCurrent).is_err());
}

#[test]
fn pure_metadata_reads_remain_valid_under_poison() {
    // §13: "pure-metadata reads — the path index, input versions, CAS
    // reads, lease pinning — remain valid under poison".
    let state = PipelineState::Poisoned {
        error: poison(),
        last_good: None,
    };
    for op in [
        OperationKind::PathIndex,
        OperationKind::InputVersionRead,
        OperationKind::CasRead,
        OperationKind::LeasePin,
    ] {
        assert!(!op.requires_epoch(), "{op:?} is pure metadata");
        let got = state
            .check(op)
            .unwrap_or_else(|_| panic!("{op:?} must survive poison"));
        assert!(got.is_none(), "pure metadata ops consume no epoch");
    }
}

#[test]
fn pipeline_dependent_ops_fail_deterministically_under_poison() {
    // §13: "anything needing the pipeline map, registry, defaults, or
    // migration fns (load_current, terminal-type queries, the
    // derived-output namespace, builds) fails deterministically with a
    // stable Failed naming the registration error".
    let state = PipelineState::Poisoned {
        error: poison(),
        last_good: None,
    };
    for op in [
        OperationKind::LoadCurrent,
        OperationKind::TerminalTypeQuery,
        OperationKind::DerivedOutputNamespace,
        OperationKind::Build,
    ] {
        assert!(op.requires_epoch(), "{op:?} needs the pipeline");
        let err = state.check(op).expect_err("must fail under poison");
        assert!(
            err.to_string().contains("tex-compress"),
            "the error names the failure"
        );
    }
}

#[test]
fn ready_state_supplies_the_epoch_to_pipeline_ops() {
    let state = PipelineState::Ready(epoch());
    let got = state
        .check(OperationKind::Build)
        .expect("ready state permits builds")
        .expect("pipeline ops receive the epoch");
    assert_eq!(got.load_policy_digest, [9u8; 32]);
    // Pure ops are also valid, without an epoch requirement.
    assert!(state.check(OperationKind::CasRead).is_ok());
}

// ---- ConfigurationState (R22/H4) ----

#[test]
fn configuration_poison_never_serves_last_good_as_current() {
    let state = ConfigurationState::Poisoned {
        reason: configuration_poison(),
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
fn configuration_poison_keeps_only_explicitly_pure_operations_available() {
    let state = ConfigurationState::Poisoned {
        reason: configuration_poison(),
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

// ---- load-policy digest ----

fn t(n: u8) -> TypeUuid {
    TypeUuid([n; 16])
}

#[test]
fn load_policy_digest_is_order_independent() {
    // §13: "blake3 over the sorted (type_uuid, build_only) pairs".
    let a = load_policy_digest(&[(t(1), false), (t(2), true)]);
    let b = load_policy_digest(&[(t(2), true), (t(1), false)]);
    assert_eq!(a, b);
}

#[test]
fn load_policy_digest_bytes_are_pinned_under_the_dslp_domain() {
    // §13 pins the construction: blake3("DSLP" ‖ version:u8 ‖ count:u32 ‖
    // (type_uuid:16 ‖ build_only:u8)*) over the sorted pairs — §5's
    // domain table registers "DSLP", so the digest can never alias
    // another meaning.
    let mut pre_image = Vec::new();
    pre_image.extend_from_slice(b"DSLP");
    pre_image.push(1); // version
    pre_image.extend_from_slice(&2u32.to_le_bytes());
    pre_image.extend_from_slice(&[1u8; 16]);
    pre_image.push(0);
    pre_image.extend_from_slice(&[2u8; 16]);
    pre_image.push(1);
    let expected = *blake3::hash(&pre_image).as_bytes();
    // Unsorted input: the encoding sorts by uuid bytes.
    assert_eq!(load_policy_digest(&[(t(2), true), (t(1), false)]), expected);
}

#[test]
fn load_policy_digest_sees_a_flipped_bit() {
    let a = load_policy_digest(&[(t(1), false), (t(2), true)]);
    let b = load_policy_digest(&[(t(1), false), (t(2), false)]);
    assert_ne!(a, b, "toggling build_only must publish a new digest");
}

#[test]
fn load_policy_digest_sees_membership() {
    let a = load_policy_digest(&[(t(1), false)]);
    let b = load_policy_digest(&[(t(1), false), (t(2), false)]);
    assert_ne!(a, b);
    let empty = load_policy_digest(&[]);
    assert_ne!(a, empty);
}
