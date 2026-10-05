//! Atomic adoption components over weak connectivity of old ∪ candidate edges.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, ContentHash};

use crate::basis::IoBasis;

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
        let edges = candidates
            .values()
            .map(|candidate| (candidate.uuid, candidate.load_deps.clone()))
            .collect();
        self.components(held, &edges)
            .into_iter()
            .map(|members| decide(members, candidates))
            .collect()
    }

    /// The load-dependency components (weakly connected over old ∪
    /// candidate edges) of `held` and the candidates in `edges`, each
    /// sorted. A candidate whose load edges are not known yet has an empty
    /// edge list: its component is not closed (doc 22 §5), and only it can
    /// grow it.
    pub fn components(
        &self,
        held: &BTreeSet<AssetUuid>,
        edges: &BTreeMap<AssetUuid, Vec<AssetUuid>>,
    ) -> Vec<Vec<AssetUuid>> {
        let mut universe = held.clone();
        universe.extend(edges.keys().copied());
        for deps in edges.values() {
            universe.extend(deps.iter().copied());
        }
        for (parent, children) in &self.current {
            if held.contains(parent) || edges.contains_key(parent) {
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
        for (uuid, deps) in edges {
            union_edges(*uuid, deps.iter(), &indices, &mut sets);
        }

        let mut components: BTreeMap<usize, Vec<AssetUuid>> = BTreeMap::new();
        for (index, uuid) in nodes.into_iter().enumerate() {
            components.entry(sets.find(index)).or_default().push(uuid);
        }
        components.into_values().collect()
    }
}

/// Find every back-edge cycle in a load-dependency graph without using the
/// native stack. Each returned path repeats its first member at the end, so
/// diagnostics preserve the complete cycle (`A -> B -> ... -> A`).
pub fn load_cycles(graph: &BTreeMap<AssetUuid, Vec<AssetUuid>>) -> Vec<Vec<AssetUuid>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Color {
        Gray,
        Black,
    }

    let mut colors = BTreeMap::<AssetUuid, Color>::new();
    let mut cycles = Vec::new();
    for root in graph.keys().copied() {
        if colors.contains_key(&root) {
            continue;
        }
        let mut path = vec![root];
        let mut positions = BTreeMap::from([(root, 0usize)]);
        let mut stack = vec![(root, 0usize)];
        colors.insert(root, Color::Gray);

        while let Some((node, next_dependency)) = stack.last_mut() {
            let dependencies = graph.get(node).map(Vec::as_slice).unwrap_or_default();
            if *next_dependency == dependencies.len() {
                let node = *node;
                stack.pop();
                path.pop();
                positions.remove(&node);
                colors.insert(node, Color::Black);
                continue;
            }

            let dependency = dependencies[*next_dependency];
            *next_dependency += 1;
            if !graph.contains_key(&dependency) {
                continue;
            }
            match colors.get(&dependency).copied() {
                None => {
                    colors.insert(dependency, Color::Gray);
                    positions.insert(dependency, path.len());
                    path.push(dependency);
                    stack.push((dependency, 0));
                }
                Some(Color::Gray) => {
                    let start = positions[&dependency];
                    let mut cycle = path[start..].to_vec();
                    cycle.push(dependency);
                    cycles.push(cycle);
                }
                Some(Color::Black) => {}
            }
        }
    }
    cycles
}

fn union_edges<'a>(
    parent: AssetUuid,
    children: impl IntoIterator<Item = &'a AssetUuid>,
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

/// The adoption decision for one component's `members`.
pub fn decide(
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
