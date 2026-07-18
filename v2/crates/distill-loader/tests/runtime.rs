use distill_core::id::{AssetUuid, ContentHash};
use distill_loader::runtime::ManifestTransitionError;
use distill_loader::{
    AdoptionId, AssetDeltaState, CompletionDisposition, ConnectionEpoch, HandleId, IoBasis,
    ManifestEntry, ManifestHash, ManifestState, OutstandingPurpose, RequestOwner, RequestTracker,
};

fn basis(seed: u8) -> IoBasis {
    IoBasis::Pack {
        manifest: ManifestHash([seed; 32]),
    }
}

#[test]
fn requests_are_fenced_by_generation_basis_and_connection_epoch() {
    let mut tracker = RequestTracker::new();
    let owner = RequestOwner::Handle(HandleId(7));
    let first = tracker
        .issue(owner.clone(), OutstandingPurpose::Resolve, basis(1))
        .unwrap();
    let second = tracker
        .issue(owner.clone(), OutstandingPurpose::Resolve, basis(1))
        .unwrap();
    assert_eq!(
        tracker.complete(first, &basis(1)),
        CompletionDisposition::Superseded
    );
    assert_eq!(
        tracker.complete(second, &basis(2)),
        CompletionDisposition::WrongBasis
    );
    assert_eq!(
        tracker.complete(second, &basis(1)),
        CompletionDisposition::UnknownOrRetired
    );

    let old_connection = tracker
        .issue(owner, OutstandingPurpose::Resolve, basis(3))
        .unwrap();
    assert_eq!(tracker.reconnect().unwrap(), ConnectionEpoch(2));
    assert_eq!(
        tracker.complete(old_connection, &basis(3)),
        CompletionDisposition::UnknownOrRetired
    );
}

#[test]
fn successful_completion_is_consumed_exactly_once() {
    let mut tracker = RequestTracker::new();
    let request = tracker
        .issue(
            RequestOwner::Content {
                asset: AssetUuid([2; 16]),
                hash: ContentHash([1; 32]),
            },
            OutstandingPurpose::Fetch,
            basis(1),
        )
        .unwrap();
    assert_eq!(
        tracker.complete(request, &basis(1)),
        CompletionDisposition::Accepted
    );
    assert_eq!(
        tracker.complete(request, &basis(1)),
        CompletionDisposition::UnknownOrRetired
    );
}

#[test]
fn typed_deletion_and_restoration_preserve_client_relative_semantics() {
    let hash = ContentHash([1; 32]);
    let mut live = ManifestEntry {
        state: ManifestState::Current { content_hash: hash },
        adopted_at: AdoptionId(1),
    };
    live.apply_delta(AssetDeltaState::Deleted).unwrap();
    assert_eq!(live.state, ManifestState::Dead);
    assert_eq!(
        live.apply_delta(AssetDeltaState::Changed),
        Err(ManifestTransitionError::ChangedDeadWithoutRestoration)
    );
    live.apply_delta(AssetDeltaState::Restored).unwrap();
    assert_eq!(live.state, ManifestState::Missing);

    let mut never_resolved = ManifestEntry {
        state: ManifestState::Missing,
        adopted_at: AdoptionId(0),
    };
    never_resolved
        .apply_delta(AssetDeltaState::Deleted)
        .unwrap();
    assert_eq!(never_resolved.state, ManifestState::Missing);
    never_resolved.observe_absence();
    assert_eq!(never_resolved.state, ManifestState::Missing);
}

#[test]
fn missing_after_daemon_state_loss_is_deleted_for_a_previously_resolved_handle() {
    let hash = ContentHash([5; 32]);
    let mut entry = ManifestEntry {
        state: ManifestState::Invalidated { last: hash },
        adopted_at: AdoptionId(9),
    };
    entry.observe_absence();
    assert_eq!(entry.state, ManifestState::Dead);
}
