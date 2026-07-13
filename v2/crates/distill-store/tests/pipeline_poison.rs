use distill_core::canonical::{CanonicalEncoder, DSPP};
use distill_store::state::{
    CleanupDisposition, PipelinePoison, PipelinePoisonCode, PipelinePoisonError,
    PipelinePoisonOrigin, PipelineState,
};
use distill_store::{Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, store)
}

#[test]
fn dspp_v1_discriminants_are_pinned() {
    assert_eq!(PipelinePoisonCode::CandidateOpen as u16, 1);
    assert_eq!(PipelinePoisonCode::CandidateAttestation as u16, 2);
    assert_eq!(PipelinePoisonCode::CandidateRegistration as u16, 3);
    assert_eq!(PipelinePoisonCode::CandidateValidation as u16, 4);
    assert_eq!(PipelinePoisonCode::CandidateCleanup as u16, 5);
    assert_eq!(PipelinePoisonCode::PublishedCallbackPanic as u16, 6);
    assert_eq!(PipelinePoisonCode::PublishedCallbackRejected as u16, 7);
    assert_eq!(PipelinePoisonCode::PublishedCleanup as u16, 8);

    assert_eq!(PipelinePoisonOrigin::CandidateOpen as u16, 1);
    assert_eq!(PipelinePoisonOrigin::PublishedRuntime as u16, 2);

    assert_eq!(CleanupDisposition::None as u16, 0);
    assert_eq!(CleanupDisposition::CleanedAndClosed as u16, 1);
    assert_eq!(CleanupDisposition::RegistrationCleanupFailed as u16, 2);
    assert_eq!(CleanupDisposition::ModuleUnloadFailed as u16, 3);
    assert_eq!(CleanupDisposition::TokenPoisoned as u16, 4);
    assert_eq!(CleanupDisposition::TokenPinned as u16, 5);
    assert_eq!(CleanupDisposition::DlcloseFailed as u16, 6);
    assert_eq!(CleanupDisposition::PublishedEpochLeaked as u16, 7);
}

#[test]
fn identity_pins_dspp_v1_and_excludes_message() {
    let first = PipelinePoison::new(
        PipelinePoisonCode::CandidateAttestation,
        PipelinePoisonOrigin::CandidateOpen,
        CleanupDisposition::CleanedAndClosed,
        "first wording",
    )
    .unwrap();
    let second = PipelinePoison::new(
        PipelinePoisonCode::CandidateAttestation,
        PipelinePoisonOrigin::CandidateOpen,
        CleanupDisposition::CleanedAndClosed,
        "different wording",
    )
    .unwrap();
    assert_eq!(first.identity, second.identity);

    let mut expected = CanonicalEncoder::new();
    expected.raw(&DSPP);
    expected.u8(1);
    expected.u16(2);
    expected.u16(1);
    expected.u16(1);
    assert_eq!(
        first.identity,
        *blake3::hash(&expected.into_bytes()).as_bytes()
    );
}

#[test]
fn closed_matrix_accepts_each_cleanup_class_and_rejects_cross_origin_rows() {
    for code in [
        PipelinePoisonCode::CandidateOpen,
        PipelinePoisonCode::CandidateAttestation,
        PipelinePoisonCode::CandidateRegistration,
        PipelinePoisonCode::CandidateValidation,
    ] {
        for cleanup in [
            CleanupDisposition::None,
            CleanupDisposition::CleanedAndClosed,
        ] {
            PipelinePoison::new(
                code,
                PipelinePoisonOrigin::CandidateOpen,
                cleanup,
                "candidate",
            )
            .unwrap();
        }
    }
    for cleanup in [
        CleanupDisposition::RegistrationCleanupFailed,
        CleanupDisposition::ModuleUnloadFailed,
        CleanupDisposition::TokenPoisoned,
        CleanupDisposition::TokenPinned,
        CleanupDisposition::DlcloseFailed,
    ] {
        PipelinePoison::new(
            PipelinePoisonCode::CandidateCleanup,
            PipelinePoisonOrigin::CandidateOpen,
            cleanup,
            "cleanup",
        )
        .unwrap();
    }
    for code in [
        PipelinePoisonCode::PublishedCallbackPanic,
        PipelinePoisonCode::PublishedCallbackRejected,
        PipelinePoisonCode::PublishedCleanup,
    ] {
        PipelinePoison::new(
            code,
            PipelinePoisonOrigin::PublishedRuntime,
            CleanupDisposition::PublishedEpochLeaked,
            "runtime",
        )
        .unwrap();
    }

    assert_eq!(
        PipelinePoison::new(
            PipelinePoisonCode::PublishedCallbackPanic,
            PipelinePoisonOrigin::CandidateOpen,
            CleanupDisposition::CleanedAndClosed,
            "wrong origin",
        )
        .unwrap_err(),
        PipelinePoisonError::InvalidMatrix
    );
    assert_eq!(
        PipelinePoison::new(
            PipelinePoisonCode::CandidateCleanup,
            PipelinePoisonOrigin::CandidateOpen,
            CleanupDisposition::None,
            "missing cleanup failure",
        )
        .unwrap_err(),
        PipelinePoisonError::InvalidMatrix
    );
}

#[test]
fn wire_decoder_rejects_unknown_tags_and_identity_mismatch() {
    assert_eq!(
        PipelinePoison::from_wire(99, 1, 0, [0; 32], "unknown").unwrap_err(),
        PipelinePoisonError::UnknownCode(99)
    );
    assert_eq!(
        PipelinePoison::from_wire(1, 99, 0, [0; 32], "unknown").unwrap_err(),
        PipelinePoisonError::UnknownOrigin(99)
    );
    assert_eq!(
        PipelinePoison::from_wire(1, 1, 99, [0; 32], "unknown").unwrap_err(),
        PipelinePoisonError::UnknownCleanup(99)
    );
    let poison = PipelinePoison::new(
        PipelinePoisonCode::CandidateOpen,
        PipelinePoisonOrigin::CandidateOpen,
        CleanupDisposition::None,
        "open failed",
    )
    .unwrap();
    assert_eq!(
        PipelinePoison::from_wire(1, 1, 0, [7; 32], poison.message).unwrap_err(),
        PipelinePoisonError::IdentityMismatch
    );
}

#[test]
fn typed_pipeline_poison_roundtrips_through_store_and_invalid_identity_rolls_back() {
    let (_dir, mut store) = store();
    let poison = PipelinePoison::new(
        PipelinePoisonCode::PublishedCallbackRejected,
        PipelinePoisonOrigin::PublishedRuntime,
        CleanupDisposition::PublishedEpochLeaked,
        "callback rejected the request",
    )
    .unwrap();
    store
        .input_transaction(|txn| txn.publish_pipeline_poison(&poison))
        .unwrap();
    match store.pipeline_state().unwrap().unwrap() {
        PipelineState::Poisoned { error, last_good } => {
            assert_eq!(error, poison);
            assert!(last_good.is_none());
        }
        other => panic!("expected poison, got {other:?}"),
    }

    let mut invalid = poison;
    invalid.identity[0] ^= 1;
    let before = store.input_version();
    let error = store
        .input_transaction(|txn| txn.publish_pipeline_poison(&invalid))
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::InvalidPipelinePoison(PipelinePoisonError::IdentityMismatch)
    ));
    assert_eq!(store.input_version(), before);
}
