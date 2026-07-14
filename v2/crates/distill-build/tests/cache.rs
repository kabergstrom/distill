use distill_build::cache::*;
use distill_build::query::AssetQuery;
use distill_build::trace::*;
use distill_core::id::{AssetUuid, BundleFileHash, ContentHash, TypeUuid};

struct Source(ContentHash);
impl TraceSource for Source {
    fn authoring_read(&self, _: AssetUuid) -> Observed<Option<BundleFileHash>> {
        Observed::Ok(None)
    }
    fn read(&self, _: AssetUuid) -> Observed<ContentHash> {
        Observed::Ok(self.0)
    }
    fn resolve(&self, _: &str) -> Observed<Option<AssetUuid>> {
        Observed::Ok(None)
    }
    fn query(&self, _: &AssetQuery) -> Observed<[u8; 32]> {
        Observed::Ok([0; 32])
    }
    fn tool(&self, _: &str) -> Observed<[u8; 32]> {
        Observed::Ok([0; 32])
    }
    fn capability(&self, _: &CapabilityKey) -> Observed<[u8; 32]> {
        Observed::Ok([0; 32])
    }
    fn ref_check(&self, _: AssetUuid, _: TypeUuid) -> Observed<Option<TypeUuid>> {
        Observed::Ok(None)
    }
    fn role_check(&self, _: AssetUuid) -> Observed<Option<EntryRole>> {
        Observed::Ok(None)
    }
    fn control(&self, _: &ControlQuery) -> Observed<[u8; 32]> {
        Observed::Ok([0; 32])
    }
    fn control_read(&self, _: &ControlSubject) -> Observed<ControlValueHash> {
        Observed::Ok(ControlValueHash([0; 32]))
    }
}

#[test]
fn bucket_is_monotone_and_looks_up_newest_valid_candidate() {
    let asset = AssetUuid([1; 16]);
    let mut bucket = CandidateBucket::new();
    bucket.commit(Candidate {
        basis_version: 1,
        trace: vec![TraceOp::Read {
            asset,
            observed: Observed::Ok(ContentHash([1; 32])),
        }],
        outcome: CandidateOutcome::Success("old"),
    });
    bucket.commit(Candidate {
        basis_version: 2,
        trace: vec![TraceOp::Read {
            asset,
            observed: Observed::Ok(ContentHash([2; 32])),
        }],
        outcome: CandidateOutcome::Success("new"),
    });
    assert_eq!(bucket.len(), 2);
    assert_eq!(
        bucket
            .lookup(&Source(ContentHash([2; 32])))
            .unwrap()
            .outcome,
        CandidateOutcome::Success("new")
    );
    assert_eq!(
        bucket
            .lookup(&Source(ContentHash([1; 32])))
            .unwrap()
            .outcome,
        CandidateOutcome::Success("old")
    );
}

#[test]
fn incoherent_failures_never_enter_a_cross_snapshot_bucket() {
    let mut bucket = CandidateBucket::<()>::new();
    assert!(matches!(
        bucket.try_commit(Candidate {
            basis_version: 1,
            trace: vec![],
            outcome: CandidateOutcome::Failure(FailureRecord {
                trace: vec![],
                cause: FailureCause::Op
            }),
        }),
        Err(CacheError::InvalidFailure(_))
    ));
    assert!(bucket.is_empty());
}
