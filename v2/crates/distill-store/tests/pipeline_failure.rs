use distill_core::canonical::{CanonicalEncoder, DSPP};
use distill_store::state::{
    CleanupDisposition, PipelineFailure, PipelineFailureCode, PipelineFailureDecodeError,
    PipelineFailureOrigin, PipelineState,
};
use distill_store::{Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, store)
}

#[test]
fn dspp_v1_discriminants_are_pinned() {
    assert_eq!(PipelineFailureCode::CandidateOpen as u16, 1);
    assert_eq!(PipelineFailureCode::CandidateAttestation as u16, 2);
    assert_eq!(PipelineFailureCode::CandidateRegistration as u16, 3);
    assert_eq!(PipelineFailureCode::CandidateValidation as u16, 4);
    assert_eq!(PipelineFailureCode::CandidateCleanup as u16, 5);
    assert_eq!(PipelineFailureCode::PublishedCallbackPanic as u16, 6);
    assert_eq!(PipelineFailureCode::PublishedCallbackRejected as u16, 7);
    assert_eq!(PipelineFailureCode::PublishedCleanup as u16, 8);

    assert_eq!(PipelineFailureOrigin::CandidateOpen as u16, 1);
    assert_eq!(PipelineFailureOrigin::PublishedRuntime as u16, 2);

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
    let first = PipelineFailure::new(
        PipelineFailureCode::CandidateAttestation,
        PipelineFailureOrigin::CandidateOpen,
        CleanupDisposition::CleanedAndClosed,
        "first wording",
    )
    .unwrap();
    let second = PipelineFailure::new(
        PipelineFailureCode::CandidateAttestation,
        PipelineFailureOrigin::CandidateOpen,
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
        PipelineFailureCode::CandidateOpen,
        PipelineFailureCode::CandidateAttestation,
        PipelineFailureCode::CandidateRegistration,
        PipelineFailureCode::CandidateValidation,
    ] {
        for cleanup in [
            CleanupDisposition::None,
            CleanupDisposition::CleanedAndClosed,
        ] {
            PipelineFailure::new(
                code,
                PipelineFailureOrigin::CandidateOpen,
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
        PipelineFailure::new(
            PipelineFailureCode::CandidateCleanup,
            PipelineFailureOrigin::CandidateOpen,
            cleanup,
            "cleanup",
        )
        .unwrap();
    }
    for code in [
        PipelineFailureCode::PublishedCallbackPanic,
        PipelineFailureCode::PublishedCallbackRejected,
        PipelineFailureCode::PublishedCleanup,
    ] {
        PipelineFailure::new(
            code,
            PipelineFailureOrigin::PublishedRuntime,
            CleanupDisposition::PublishedEpochLeaked,
            "runtime",
        )
        .unwrap();
    }

    assert_eq!(
        PipelineFailure::new(
            PipelineFailureCode::PublishedCallbackPanic,
            PipelineFailureOrigin::CandidateOpen,
            CleanupDisposition::CleanedAndClosed,
            "wrong origin",
        )
        .unwrap_err(),
        PipelineFailureDecodeError::InvalidMatrix
    );
    assert_eq!(
        PipelineFailure::new(
            PipelineFailureCode::CandidateCleanup,
            PipelineFailureOrigin::CandidateOpen,
            CleanupDisposition::None,
            "missing cleanup failure",
        )
        .unwrap_err(),
        PipelineFailureDecodeError::InvalidMatrix
    );
}

#[test]
fn wire_decoder_rejects_unknown_tags_and_identity_mismatch() {
    assert_eq!(
        PipelineFailure::from_wire(99, 1, 0, [0; 32], "unknown").unwrap_err(),
        PipelineFailureDecodeError::UnknownCode(99)
    );
    assert_eq!(
        PipelineFailure::from_wire(1, 99, 0, [0; 32], "unknown").unwrap_err(),
        PipelineFailureDecodeError::UnknownOrigin(99)
    );
    assert_eq!(
        PipelineFailure::from_wire(1, 1, 99, [0; 32], "unknown").unwrap_err(),
        PipelineFailureDecodeError::UnknownCleanup(99)
    );
    let failure = PipelineFailure::new(
        PipelineFailureCode::CandidateOpen,
        PipelineFailureOrigin::CandidateOpen,
        CleanupDisposition::None,
        "open failed",
    )
    .unwrap();
    assert_eq!(
        PipelineFailure::from_wire(1, 1, 0, [7; 32], failure.message).unwrap_err(),
        PipelineFailureDecodeError::IdentityMismatch
    );
}

#[test]
fn typed_pipeline_failure_roundtrips_through_store_and_invalid_identity_rolls_back() {
    let (_dir, mut store) = store();
    let failure = PipelineFailure::new(
        PipelineFailureCode::PublishedCallbackRejected,
        PipelineFailureOrigin::PublishedRuntime,
        CleanupDisposition::PublishedEpochLeaked,
        "callback rejected the request",
    )
    .unwrap();
    store
        .input_transaction(|txn| txn.publish_pipeline_failure(&failure))
        .unwrap();
    match store.pipeline_state().unwrap().unwrap() {
        PipelineState::Failed { error, last_good } => {
            assert_eq!(error, failure);
            assert!(last_good.is_none());
        }
        other => panic!("expected a failure, got {other:?}"),
    }

    let mut invalid = failure;
    invalid.identity[0] ^= 1;
    let before = store.input_version().unwrap();
    let error = store
        .input_transaction(|txn| txn.publish_pipeline_failure(&invalid))
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::InvalidPipelineFailure(PipelineFailureDecodeError::IdentityMismatch)
    ));
    assert_eq!(store.input_version().unwrap(), before);
}
