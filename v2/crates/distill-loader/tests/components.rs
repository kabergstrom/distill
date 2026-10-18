use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, ContentHash};
use distill_loader::{
    load_cycles, AdoptionDecision, CandidateAsset, CandidateOutcome, ComponentPlanner, IoBasis,
    ManifestHash, MemberFailure,
};

fn id(n: u8) -> AssetUuid {
    AssetUuid([n; 16])
}
fn basis(seed: u8, _build_only: &[u8]) -> IoBasis {
    IoBasis::Pack {
        manifest: ManifestHash([seed; 16]),
    }
}

fn ready(uuid: u8, deps: &[u8], basis: &IoBasis) -> CandidateAsset {
    CandidateAsset {
        uuid: id(uuid),
        basis: basis.clone(),
        load_deps: deps.iter().copied().map(id).collect(),
        outcome: CandidateOutcome::Ready {
            content_hash: ContentHash([uuid; 16]),
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
fn a_deleted_member_adopts_and_a_dangling_one_freezes() {
    let basis = basis(1, &[]);
    let mut deleted = ready(3, &[], &basis);
    deleted.outcome = CandidateOutcome::Deleted;
    let candidates = map(vec![ready(1, &[2, 3], &basis), ready(2, &[], &basis), deleted]);
    let decisions = ComponentPlanner::default().plan(&BTreeSet::from([id(1)]), &candidates);
    assert!(matches!(
        &decisions[0],
        AdoptionDecision::Ready { members, .. } if members == &vec![id(1), id(2), id(3)]
    ));

    let mut missing = ready(3, &[], &basis);
    missing.outcome = CandidateOutcome::Missing;
    let candidates = map(vec![ready(1, &[3], &basis), missing]);
    assert!(matches!(
        &ComponentPlanner::default().plan(&BTreeSet::from([id(1)]), &candidates)[0],
        AdoptionDecision::Poisoned { failures, .. } if failures == &vec![(id(3), MemberFailure::Missing)]
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
                basis: basis.clone(),
                load_deps: if n + 1 == 20_000 { vec![] } else { vec![next] },
                outcome: CandidateOutcome::Ready {
                    content_hash: ContentHash([1; 16]),
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

#[test]
fn load_cycle_reports_the_complete_path_and_is_stack_safe() {
    let graph = BTreeMap::from([
        (id(1), vec![id(2)]),
        (id(2), vec![id(3)]),
        (id(3), vec![id(1)]),
    ]);
    assert_eq!(load_cycles(&graph), vec![vec![id(1), id(2), id(3), id(1)]]);

    let deep = (0..20_000u32)
        .map(|index| {
            let node = AssetUuid((index as u128).to_le_bytes());
            let dependencies = (index + 1 < 20_000)
                .then(|| AssetUuid(((index + 1) as u128).to_le_bytes()))
                .into_iter()
                .collect();
            (node, dependencies)
        })
        .collect();
    assert!(load_cycles(&deep).is_empty());
}
