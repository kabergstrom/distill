use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use distill_core::id::{AssetUuid, ContentHash, TypeUuid};
use distill_loader::{
    AdoptionDecision, CandidateAsset, CandidateOutcome, ComponentPlanner, IoBasis,
    LoadPolicyAttestation, LoadPolicyRow, ManifestHash, MemberFailure,
};

fn id(n: u8) -> AssetUuid {
    AssetUuid([n; 16])
}
fn ty(n: u8) -> TypeUuid {
    TypeUuid([n; 16])
}

fn basis(seed: u8, build_only: &[u8]) -> IoBasis {
    let rows = (1..=9)
        .map(|n| LoadPolicyRow {
            type_uuid: ty(n),
            build_only: build_only.contains(&n),
        })
        .collect();
    IoBasis::Pack {
        manifest: ManifestHash([seed; 32]),
        load_policy: Arc::new(LoadPolicyAttestation::from_rows(rows).unwrap()),
    }
}

fn ready(uuid: u8, deps: &[u8], basis: &IoBasis) -> CandidateAsset {
    CandidateAsset {
        uuid: id(uuid),
        type_uuid: ty(uuid),
        basis: basis.clone(),
        load_deps: deps.iter().copied().map(id).collect(),
        outcome: CandidateOutcome::Ready {
            content_hash: ContentHash([uuid; 32]),
        },
    }
}

fn map(values: Vec<CandidateAsset>) -> BTreeMap<AssetUuid, CandidateAsset> {
    values
        .into_iter()
        .map(|value| (value.uuid, value))
        .collect()
}

#[test]
fn shared_child_weakly_connects_reverse_dependents() {
    let basis = basis(1, &[]);
    let candidates = map(vec![
        ready(1, &[3], &basis),
        ready(2, &[3], &basis),
        ready(3, &[], &basis),
    ]);
    let decisions = ComponentPlanner::default().plan(&BTreeSet::from([id(1), id(2)]), &candidates);
    assert_eq!(decisions.len(), 1);
    assert!(matches!(
        &decisions[0],
        AdoptionDecision::Ready { members, .. } if members == &vec![id(1), id(2), id(3)]
    ));
}

#[test]
fn old_and_candidate_edges_are_unioned_before_any_swap() {
    let basis = basis(1, &[]);
    let current = BTreeMap::from([(id(1), BTreeSet::from([id(2)]))]);
    let candidates = map(vec![
        ready(1, &[3], &basis),
        ready(2, &[], &basis),
        ready(3, &[], &basis),
    ]);
    let decisions =
        ComponentPlanner::new(current).plan(&BTreeSet::from([id(1), id(2)]), &candidates);
    assert_eq!(decisions.len(), 1);
    assert!(matches!(&decisions[0], AdoptionDecision::Ready { members, .. } if members.len() == 3));
}

#[test]
fn failure_freezes_only_its_component() {
    let basis = basis(1, &[]);
    let mut failed = ready(2, &[], &basis);
    failed.outcome = CandidateOutcome::Failed {
        error: "shader failed".into(),
    };
    let candidates = map(vec![ready(1, &[2], &basis), failed, ready(8, &[], &basis)]);
    let decisions = ComponentPlanner::default().plan(&BTreeSet::from([id(1), id(8)]), &candidates);
    assert!(decisions.iter().any(|decision| matches!(
        decision,
        AdoptionDecision::Poisoned { members, failures }
            if members == &vec![id(1), id(2)]
            && failures == &vec![(id(2), MemberFailure::Failed("shader failed".into()))]
    )));
    assert!(decisions.iter().any(|decision| matches!(
        decision,
        AdoptionDecision::Ready { members, .. } if members == &vec![id(8)]
    )));
}

#[test]
fn mixed_basis_component_is_reresolved_whole() {
    let old = basis(1, &[]);
    let new = basis(2, &[]);
    let candidates = map(vec![ready(1, &[2], &old), ready(2, &[], &new)]);
    assert_eq!(
        ComponentPlanner::default().plan(&BTreeSet::from([id(1)]), &candidates),
        vec![AdoptionDecision::Reresolve {
            members: vec![id(1), id(2)]
        }]
    );
}

#[test]
fn build_only_and_deletion_policy_are_component_failures() {
    let policy = basis(1, &[2]);
    let mut deleted = ready(3, &[], &policy);
    deleted.outcome = CandidateOutcome::Deleted {
        placeholder_ready: false,
    };
    let candidates = map(vec![
        ready(1, &[2, 3], &policy),
        ready(2, &[], &policy),
        deleted,
    ]);
    let decisions = ComponentPlanner::default().plan(&BTreeSet::from([id(1)]), &candidates);
    assert!(
        matches!(&decisions[0], AdoptionDecision::Poisoned { failures, .. }
        if failures.iter().any(|(uuid, failure)| *uuid == id(2) && matches!(failure, MemberFailure::LoadPolicy(_)))
        && failures.contains(&(id(3), MemberFailure::DeletedWithoutPlaceholder)))
    );

    let clean = basis(2, &[]);
    let mut placeholder = ready(2, &[], &clean);
    placeholder.outcome = CandidateOutcome::Deleted {
        placeholder_ready: true,
    };
    assert!(matches!(
        &ComponentPlanner::default().plan(
            &BTreeSet::from([id(1)]),
            &map(vec![ready(1, &[2], &clean), placeholder]),
        )[0],
        AdoptionDecision::Ready { .. }
    ));
}

#[test]
fn component_discovery_is_stack_safe_for_deep_closures() {
    let basis = basis(1, &[]);
    let mut candidates = BTreeMap::new();
    for n in 0..20_000u32 {
        let uuid = AssetUuid((n as u128).to_le_bytes());
        let next = AssetUuid(((n + 1) as u128).to_le_bytes());
        candidates.insert(
            uuid,
            CandidateAsset {
                uuid,
                type_uuid: ty(1),
                basis: basis.clone(),
                load_deps: if n + 1 == 20_000 { vec![] } else { vec![next] },
                outcome: CandidateOutcome::Ready {
                    content_hash: ContentHash([1; 32]),
                },
            },
        );
    }
    let decisions = ComponentPlanner::default().plan(
        &BTreeSet::from([AssetUuid(0u128.to_le_bytes())]),
        &candidates,
    );
    assert!(
        matches!(&decisions[0], AdoptionDecision::Ready { members, .. } if members.len() == 20_000)
    );
}
