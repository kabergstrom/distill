//! Graph walk (§11): greedy, custom edges mandatory, current tested
//! before edges are consulted.

mod common;
use common::*;
use distill_core::id::LogicalHash;
use distill_migrate::{select_chain, EdgeRef, WalkError};
use std::cell::RefCell;
use std::collections::BTreeMap;

fn edge(b: u8, from: LogicalHash, to: LogicalHash) -> EdgeRef {
    EdgeRef {
        bundle: bundle(b),
        from,
        to,
    }
}

/// An edges fn over a static map that logs every node it is asked about.
struct Graph {
    map: BTreeMap<LogicalHash, Vec<EdgeRef>>,
    log: RefCell<Vec<LogicalHash>>,
}

impl Graph {
    fn new(entries: &[(LogicalHash, Vec<EdgeRef>)]) -> Self {
        Graph {
            map: entries.iter().cloned().collect(),
            log: RefCell::new(Vec::new()),
        }
    }
    fn edges_fn(&self) -> impl FnMut(LogicalHash) -> Vec<EdgeRef> + '_ {
        move |h| {
            self.log.borrow_mut().push(h);
            self.map.get(&h).cloned().unwrap_or_default()
        }
    }
}

#[test]
fn start_equals_current_terminates_with_no_queries() {
    // Equality is tested BEFORE querying edges: no query is recorded, even
    // though an edge OUT of current exists.
    let cur = hash(1);
    let g = Graph::new(&[(cur, vec![edge(9, cur, hash(2))])]);
    let chain = select_chain(cur, cur, &mut g.edges_fn()).unwrap();
    assert!(chain.edges.is_empty());
    assert!(chain.queried.is_empty());
    assert!(!chain.needs_automatic_tail);
    assert!(g.log.borrow().is_empty(), "edges fn must never be called");
}

#[test]
fn one_custom_edge_to_current() {
    let (a, cur) = (hash(1), hash(2));
    let e = edge(3, a, cur);
    let g = Graph::new(&[(a, vec![e.clone()])]);
    let chain = select_chain(a, cur, &mut g.edges_fn()).unwrap();
    assert_eq!(chain.edges, vec![e.clone()]);
    assert_eq!(chain.queried, vec![(a, vec![e])]);
    assert!(!chain.needs_automatic_tail);
    assert_eq!(*g.log.borrow(), vec![a]);
}

#[test]
fn custom_chain_then_dead_end_needs_tail_with_empty_query_recorded() {
    // A→B custom; B has no outgoing edge; current is C. The empty set at
    // B is recorded — it is a migration_edge dep (§10, §11).
    let (a, b, cur) = (hash(1), hash(2), hash(3));
    let e = edge(4, a, b);
    let g = Graph::new(&[(a, vec![e.clone()])]);
    let chain = select_chain(a, cur, &mut g.edges_fn()).unwrap();
    assert_eq!(chain.edges, vec![e.clone()]);
    assert_eq!(chain.queried, vec![(a, vec![e]), (b, vec![])]);
    assert!(chain.needs_automatic_tail);
}

#[test]
fn two_outgoing_edges_is_ambiguity_naming_bundles() {
    let (a, cur) = (hash(1), hash(9));
    let e1 = edge(5, a, hash(2));
    let e2 = edge(6, a, hash(3));
    let g = Graph::new(&[(a, vec![e1, e2])]);
    let err = select_chain(a, cur, &mut g.edges_fn()).unwrap_err();
    let WalkError::Ambiguous { node, bundles } = err else {
        panic!("expected Ambiguous, got {err:?}");
    };
    assert_eq!(node, a);
    assert_eq!(bundles, vec![bundle(5), bundle(6)]);
}

#[test]
fn revisited_node_is_cycle() {
    let (a, b, cur) = (hash(1), hash(2), hash(9));
    let g = Graph::new(&[(a, vec![edge(3, a, b)]), (b, vec![edge(4, b, a)])]);
    let err = select_chain(a, cur, &mut g.edges_fn()).unwrap_err();
    assert_eq!(err, WalkError::Cycle { node: a });
}

#[test]
fn self_loop_is_cycle() {
    let (a, cur) = (hash(1), hash(9));
    let g = Graph::new(&[(a, vec![edge(3, a, a)])]);
    let err = select_chain(a, cur, &mut g.edges_fn()).unwrap_err();
    assert_eq!(err, WalkError::Cycle { node: a });
}

#[test]
fn edge_out_of_current_is_never_consulted() {
    // A→B→C(current); C has an outgoing edge C→D that must never be
    // queried or followed: the walk terminates AT current, recording no
    // query for it.
    let (a, b, cur, d) = (hash(1), hash(2), hash(3), hash(4));
    let e1 = edge(5, a, b);
    let e2 = edge(6, b, cur);
    let g = Graph::new(&[
        (a, vec![e1.clone()]),
        (b, vec![e2.clone()]),
        (cur, vec![edge(7, cur, d)]),
    ]);
    let chain = select_chain(a, cur, &mut g.edges_fn()).unwrap();
    assert_eq!(chain.edges, vec![e1.clone(), e2.clone()]);
    assert_eq!(chain.queried, vec![(a, vec![e1]), (b, vec![e2])]);
    assert!(!chain.needs_automatic_tail);
    assert!(
        !g.log.borrow().contains(&cur),
        "edges fn was consulted for target_current: {:?}",
        g.log.borrow()
    );
}
