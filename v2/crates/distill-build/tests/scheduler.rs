use std::collections::{BTreeMap, BTreeSet};

use distill_build::scheduler::*;
use distill_core::id::AssetUuid;

struct Graph {
    deps: BTreeMap<AssetUuid, Vec<AssetUuid>>,
    cached: BTreeSet<AssetUuid>,
    built: Vec<AssetUuid>,
}

impl BuildGraph for Graph {
    type Error = ();
    fn is_cached(&mut self, asset: AssetUuid) -> bool {
        self.cached.contains(&asset)
    }
    fn dependencies(&mut self, asset: AssetUuid) -> Result<Vec<AssetUuid>, Self::Error> {
        Ok(self.deps.get(&asset).cloned().unwrap_or_default())
    }
    fn execute(&mut self, asset: AssetUuid) -> Result<(), Self::Error> {
        self.built.push(asset);
        self.cached.insert(asset);
        Ok(())
    }
}

fn id(n: u32) -> AssetUuid {
    let mut bytes = [0; 16];
    bytes[..4].copy_from_slice(&n.to_le_bytes());
    AssetUuid(bytes)
}

#[test]
fn trampoline_handles_a_deep_chain_without_native_recursion() {
    let depth = 20_000;
    let mut deps = BTreeMap::new();
    for n in 0..depth - 1 {
        deps.insert(id(n), vec![id(n + 1)]);
    }
    let mut graph = Graph {
        deps,
        cached: BTreeSet::new(),
        built: vec![],
    };
    build_trampolined(id(0), depth as usize, &mut graph).unwrap();
    assert_eq!(graph.built.len(), depth as usize);
    assert_eq!(graph.built.first(), Some(&id(depth - 1)));
    assert_eq!(graph.built.last(), Some(&id(0)));
}

#[test]
fn cycles_and_depth_exhaustion_are_named_scheduler_outcomes() {
    let mut graph = Graph {
        deps: BTreeMap::from([(id(1), vec![id(2)]), (id(2), vec![id(1)])]),
        cached: BTreeSet::new(),
        built: vec![],
    };
    assert!(
        matches!(build_trampolined(id(1), 10, &mut graph), Err(SchedulerError::Cycle { chain }) if chain == vec![id(1), id(2), id(1)])
    );

    let mut graph = Graph {
        deps: BTreeMap::from([(id(1), vec![id(2)]), (id(2), vec![id(3)])]),
        cached: BTreeSet::new(),
        built: vec![],
    };
    assert!(matches!(
        build_trampolined(id(1), 2, &mut graph),
        Err(SchedulerError::DepthExceeded { .. })
    ));
    assert!(graph.cached.is_empty(), "depth outcome was not memoized");
    build_trampolined(id(1), 3, &mut graph).unwrap();
}

#[test]
fn wait_for_graph_rejects_cycle_closing_join() {
    let mut waits = WaitForGraph::new();
    waits.add_wait(id(1), id(2)).unwrap();
    waits.add_wait(id(2), id(3)).unwrap();
    assert!(matches!(
        waits.add_wait(id(3), id(1)),
        Err(WaitCycle { .. })
    ));
    waits.remove_job(id(2));
    assert!(waits.add_wait(id(3), id(1)).is_ok());
}

#[test]
fn reservation_bounds_and_single_worker_alternation_are_pinned() {
    assert!(SchedulerConfig::new(0, 1).is_err());
    assert!(SchedulerConfig::new(4, 0).is_err());
    assert!(SchedulerConfig::new(4, 4).is_err());
    let mut cfg = SchedulerConfig::new(8, 3).unwrap();
    cfg.resize(2).unwrap();
    assert_eq!(cfg.batch_reserved_workers, 1);

    let mut queue = FairQueue::new(SchedulerConfig::new(1, 1).unwrap());
    queue.push(WorkClass::Batch, "b1");
    queue.push(WorkClass::Interactive, "i1");
    queue.push(WorkClass::Batch, "b2");
    queue.push(WorkClass::Interactive, "i2");
    assert_eq!(queue.pop_next(0, 0), Some((WorkClass::Batch, "b1")));
    assert_eq!(queue.pop_next(0, 0), Some((WorkClass::Interactive, "i1")));
    assert_eq!(queue.pop_next(0, 0), Some((WorkClass::Batch, "b2")));
    assert_eq!(queue.pop_next(0, 0), Some((WorkClass::Interactive, "i2")));
}
