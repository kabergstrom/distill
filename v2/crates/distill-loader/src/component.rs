//! Atomic adoption components over weak connectivity of old ∪ candidate edges.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, ContentHash, TypeUuid};

use crate::basis::{IoBasis, LoadPolicyError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateOutcome {
    Ready {
        content_hash: ContentHash,
    },
    Failed {
        error: String,
    },
    Missing,
    /// Deletion can adopt only when a placeholder was minted successfully.
    Deleted {
        placeholder_ready: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateAsset {
    pub uuid: AssetUuid,
    pub type_uuid: TypeUuid,
    pub basis: IoBasis,
    pub load_deps: Vec<AssetUuid>,
    pub outcome: CandidateOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberFailure {
    Unresolved,
    Failed(String),
    Missing,
    DeletedWithoutPlaceholder,
    LoadPolicy(LoadPolicyError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptionDecision {
    Ready {
        members: Vec<AssetUuid>,
        basis: IoBasis,
    },
    Poisoned {
        members: Vec<AssetUuid>,
        failures: Vec<(AssetUuid, MemberFailure)>,
    },
    /// Outcomes from multiple bases are never relabelled. The entire merged
    /// component must be issued again under a fresh `begin_sweep` basis.
    Reresolve { members: Vec<AssetUuid> },
}

#[derive(Debug, Clone, Default)]
pub struct ComponentPlanner {
    current: BTreeMap<AssetUuid, BTreeSet<AssetUuid>>,
}

impl ComponentPlanner {
    pub fn new(current: BTreeMap<AssetUuid, BTreeSet<AssetUuid>>) -> Self {
        Self { current }
    }

    pub fn plan(
        &self,
        held: &BTreeSet<AssetUuid>,
        candidates: &BTreeMap<AssetUuid, CandidateAsset>,
    ) -> Vec<AdoptionDecision> {
        let mut universe = held.clone();
        universe.extend(candidates.keys().copied());
        for candidate in candidates.values() {
            universe.extend(candidate.load_deps.iter().copied());
        }
        for (parent, children) in &self.current {
            if held.contains(parent) || candidates.contains_key(parent) {
                universe.insert(*parent);
                universe.extend(children.iter().copied());
            }
        }

        let nodes: Vec<_> = universe.into_iter().collect();
        let indices: BTreeMap<_, _> = nodes
            .iter()
            .copied()
            .enumerate()
            .map(|(index, uuid)| (uuid, index))
            .collect();
        let mut sets = DisjointSets::new(nodes.len());
        for (parent, children) in &self.current {
            union_edges(*parent, children, &indices, &mut sets);
        }
        for candidate in candidates.values() {
            union_edges(
                candidate.uuid,
                &candidate.load_deps.iter().copied().collect(),
                &indices,
                &mut sets,
            );
        }

        let mut components: BTreeMap<usize, Vec<AssetUuid>> = BTreeMap::new();
        for (index, uuid) in nodes.into_iter().enumerate() {
            components.entry(sets.find(index)).or_default().push(uuid);
        }
        components
            .into_values()
            .map(|members| decide(members, candidates))
            .collect()
    }
}

fn union_edges(
    parent: AssetUuid,
    children: &BTreeSet<AssetUuid>,
    indices: &BTreeMap<AssetUuid, usize>,
    sets: &mut DisjointSets,
) {
    let Some(&parent_index) = indices.get(&parent) else {
        return;
    };
    for child in children {
        if let Some(&child_index) = indices.get(child) {
            sets.union(parent_index, child_index);
        }
    }
}

fn decide(
    members: Vec<AssetUuid>,
    candidates: &BTreeMap<AssetUuid, CandidateAsset>,
) -> AdoptionDecision {
    let first_basis = members
        .iter()
        .find_map(|uuid| candidates.get(uuid).map(|candidate| &candidate.basis));
    if let Some(first) = first_basis {
        if members
            .iter()
            .filter_map(|uuid| candidates.get(uuid))
            .any(|candidate| &candidate.basis != first)
        {
            return AdoptionDecision::Reresolve { members };
        }
    }

    let mut failures = Vec::new();
    for uuid in &members {
        let Some(candidate) = candidates.get(uuid) else {
            failures.push((*uuid, MemberFailure::Unresolved));
            continue;
        };
        if let Err(error) = candidate
            .basis
            .load_policy()
            .require_runtime(candidate.type_uuid)
        {
            failures.push((*uuid, MemberFailure::LoadPolicy(error)));
            continue;
        }
        let failure = match &candidate.outcome {
            CandidateOutcome::Ready { .. }
            | CandidateOutcome::Deleted {
                placeholder_ready: true,
            } => None,
            CandidateOutcome::Failed { error } => Some(MemberFailure::Failed(error.clone())),
            CandidateOutcome::Missing => Some(MemberFailure::Missing),
            CandidateOutcome::Deleted {
                placeholder_ready: false,
            } => Some(MemberFailure::DeletedWithoutPlaceholder),
        };
        if let Some(failure) = failure {
            failures.push((*uuid, failure));
        }
    }
    if failures.is_empty() {
        AdoptionDecision::Ready {
            members,
            basis: first_basis
                .expect("nonempty component has candidates")
                .clone(),
        }
    } else {
        AdoptionDecision::Poisoned { members, failures }
    }
}

#[derive(Debug)]
struct DisjointSets {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl DisjointSets {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
            rank: vec![0; len],
        }
    }

    fn find(&mut self, mut node: usize) -> usize {
        let mut root = node;
        while self.parent[root] != root {
            root = self.parent[root];
        }
        while self.parent[node] != node {
            let next = self.parent[node];
            self.parent[node] = root;
            node = next;
        }
        root
    }

    fn union(&mut self, left: usize, right: usize) {
        let left = self.find(left);
        let right = self.find(right);
        if left == right {
            return;
        }
        match self.rank[left].cmp(&self.rank[right]) {
            std::cmp::Ordering::Less => self.parent[left] = right,
            std::cmp::Ordering::Greater => self.parent[right] = left,
            std::cmp::Ordering::Equal => {
                self.parent[right] = left;
                self.rank[left] = self.rank[left].saturating_add(1);
            }
        }
    }
}
