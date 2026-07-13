//! The migration-graph walk (§11): a greedy walk in which custom edges
//! are mandatory. Nodes are logical hashes; edges are custom migration
//! bundles only — automatic diffs are never edges between arbitrary
//! historical schemas.

use distill_core::id::{BundleUuid, LogicalHash};
use std::fmt;

/// One custom migration edge as the metadata index surfaces it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeRef {
    pub bundle: BundleUuid,
    pub from: LogicalHash,
    pub to: LogicalHash,
}

/// The selected chain plus everything selection consumed: one `queried`
/// entry per node whose outgoing edges were consulted — INCLUDING empty
/// sets — these become §10 migration_edge query deps. The terminating
/// current node records NO entry: its edges are never consulted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedChain {
    pub edges: Vec<EdgeRef>,
    pub queried: Vec<(LogicalHash, Vec<EdgeRef>)>,
    /// True when the walk ended at a non-current node with no outgoing
    /// edge: exactly one automatic diff to current happens there.
    pub needs_automatic_tail: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalkError {
    /// Two+ outgoing custom edges from one node — a human edit resolves
    /// it, never a tie-break heuristic (§11).
    Ambiguous {
        node: LogicalHash,
        bundles: Vec<BundleUuid>,
    },
    /// A revisited node.
    Cycle { node: LogicalHash },
}

impl fmt::Display for WalkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalkError::Ambiguous { node, bundles } => {
                write!(f, "ambiguous migration graph at {node}: bundles")?;
                for b in bundles {
                    write!(f, " {b}")?;
                }
                Ok(())
            }
            WalkError::Cycle { node } => write!(f, "migration graph cycle revisits {node}"),
        }
    }
}

impl std::error::Error for WalkError {}

/// Walk from `start` toward `target_current`. At every node the walk
/// FIRST tests `node == target_current` and terminates immediately on
/// equality — an edge out of the current schema is never consulted or
/// followed. Only then are outgoing edges queried (and recorded).
pub fn select_chain(
    start: LogicalHash,
    target_current: LogicalHash,
    edges: &mut dyn FnMut(LogicalHash) -> Vec<EdgeRef>,
) -> Result<SelectedChain, WalkError> {
    let mut chain = SelectedChain {
        edges: Vec::new(),
        queried: Vec::new(),
        needs_automatic_tail: false,
    };
    let mut visited = vec![start];
    let mut node = start;
    loop {
        // FIRST: the current-schema test — before any edge query, so the
        // terminating node records no migration_edge dep and an edge out
        // of current can never transform data already at current (§11).
        if node == target_current {
            return Ok(chain);
        }
        let out = edges(node);
        chain.queried.push((node, out.clone()));
        match out.len() {
            // No outgoing edge at a non-current node: exactly one
            // automatic diff from here to current (§11).
            0 => {
                chain.needs_automatic_tail = true;
                return Ok(chain);
            }
            // A custom edge was authored for data at exactly this schema
            // and is never bypassed (§11).
            1 => {
                let e = out.into_iter().next().expect("len checked");
                let next = e.to;
                chain.edges.push(e);
                if visited.contains(&next) {
                    return Err(WalkError::Cycle { node: next });
                }
                visited.push(next);
                node = next;
            }
            // Ambiguity is an error, never a guess (§11).
            _ => {
                return Err(WalkError::Ambiguous {
                    node,
                    bundles: out.into_iter().map(|e| e.bundle).collect(),
                })
            }
        }
    }
}
