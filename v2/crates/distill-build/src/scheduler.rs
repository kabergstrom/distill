//! Stack-safe dependency traversal, global wait-for cycle checks, and
//! interactive/batch reservation policy (§§9,13,18).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use distill_core::id::AssetUuid;

pub trait BuildGraph {
    type Error;
    fn is_cached(&mut self, asset: AssetUuid) -> bool;
    fn dependencies(&mut self, asset: AssetUuid) -> Result<Vec<AssetUuid>, Self::Error>;
    fn execute(&mut self, asset: AssetUuid) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError<E> {
    Graph(E),
    Cycle { chain: Vec<AssetUuid> },
    DepthExceeded { limit: usize, chain: Vec<AssetUuid> },
}

struct Frame {
    asset: AssetUuid,
    dependencies: Vec<AssetUuid>,
    next: usize,
}

/// Iterative post-order build. No function call is nested by dependency
/// depth; the configured cap is policy, independent of native stack safety.
pub fn build_trampolined<G: BuildGraph>(
    root: AssetUuid,
    max_depth: usize,
    graph: &mut G,
) -> Result<(), SchedulerError<G::Error>> {
    if max_depth == 0 {
        return Err(SchedulerError::DepthExceeded {
            limit: 0,
            chain: vec![root],
        });
    }
    if graph.is_cached(root) {
        return Ok(());
    }
    let deps = graph.dependencies(root).map_err(SchedulerError::Graph)?;
    let mut stack = vec![Frame {
        asset: root,
        dependencies: deps,
        next: 0,
    }];
    let mut completed = BTreeSet::new();

    while !stack.is_empty() {
        let last = stack.len() - 1;
        if stack[last].next < stack[last].dependencies.len() {
            let child = stack[last].dependencies[stack[last].next];
            stack[last].next += 1;
            if completed.contains(&child) || graph.is_cached(child) {
                completed.insert(child);
                continue;
            }
            if let Some(start) = stack.iter().position(|frame| frame.asset == child) {
                let mut chain: Vec<_> = stack[start..].iter().map(|frame| frame.asset).collect();
                chain.push(child);
                return Err(SchedulerError::Cycle { chain });
            }
            if stack.len() + 1 > max_depth {
                let mut chain: Vec<_> = stack.iter().map(|frame| frame.asset).collect();
                chain.push(child);
                return Err(SchedulerError::DepthExceeded {
                    limit: max_depth,
                    chain,
                });
            }
            let dependencies = graph.dependencies(child).map_err(SchedulerError::Graph)?;
            stack.push(Frame {
                asset: child,
                dependencies,
                next: 0,
            });
        } else {
            let frame = stack.pop().expect("not empty");
            graph.execute(frame.asset).map_err(SchedulerError::Graph)?;
            completed.insert(frame.asset);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitCycle {
    pub chain: Vec<AssetUuid>,
}

#[derive(Default)]
pub struct WaitForGraph {
    edges: BTreeMap<AssetUuid, BTreeSet<AssetUuid>>,
}

impl WaitForGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_wait(&mut self, waiter: AssetUuid, owner: AssetUuid) -> Result<(), WaitCycle> {
        if waiter == owner {
            return Err(WaitCycle {
                chain: vec![waiter, owner],
            });
        }
        if let Some(mut path) = self.path(owner, waiter) {
            path.insert(0, waiter);
            return Err(WaitCycle { chain: path });
        }
        self.edges.entry(waiter).or_default().insert(owner);
        Ok(())
    }

    pub fn remove_job(&mut self, job: AssetUuid) {
        self.edges.remove(&job);
        for owners in self.edges.values_mut() {
            owners.remove(&job);
        }
    }

    fn path(&self, from: AssetUuid, to: AssetUuid) -> Option<Vec<AssetUuid>> {
        let mut queue = VecDeque::from([(from, vec![from])]);
        let mut seen = BTreeSet::new();
        while let Some((node, path)) = queue.pop_front() {
            if !seen.insert(node) {
                continue;
            }
            if node == to {
                return Some(path);
            }
            if let Some(next) = self.edges.get(&node) {
                for child in next {
                    let mut path = path.clone();
                    path.push(*child);
                    queue.push_back((*child, path));
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub parallelism: usize,
    pub batch_reserved_workers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerConfigError {
    ParallelismZero,
    ReservationOutOfBounds,
}

impl SchedulerConfig {
    pub fn new(
        parallelism: usize,
        batch_reserved_workers: usize,
    ) -> Result<Self, SchedulerConfigError> {
        if parallelism == 0 {
            return Err(SchedulerConfigError::ParallelismZero);
        }
        let max = parallelism.saturating_sub(1).max(1);
        if !(1..=max).contains(&batch_reserved_workers) {
            return Err(SchedulerConfigError::ReservationOutOfBounds);
        }
        Ok(Self {
            parallelism,
            batch_reserved_workers,
        })
    }

    /// Live resize: active slots drain externally; future admission uses
    /// the re-clamped reservation immediately.
    pub fn resize(&mut self, parallelism: usize) -> Result<(), SchedulerConfigError> {
        if parallelism == 0 {
            return Err(SchedulerConfigError::ParallelismZero);
        }
        self.parallelism = parallelism;
        self.batch_reserved_workers = self
            .batch_reserved_workers
            .clamp(1, parallelism.saturating_sub(1).max(1));
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkClass {
    Interactive,
    Batch,
}

pub struct FairQueue<T> {
    config: SchedulerConfig,
    interactive: VecDeque<T>,
    batch: VecDeque<T>,
    single_next: WorkClass,
}

impl<T> FairQueue<T> {
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            interactive: VecDeque::new(),
            batch: VecDeque::new(),
            single_next: WorkClass::Batch,
        }
    }

    pub fn push(&mut self, class: WorkClass, work: T) {
        match class {
            WorkClass::Interactive => self.interactive.push_back(work),
            WorkClass::Batch => self.batch.push_back(work),
        }
    }

    pub fn pop_next(
        &mut self,
        active_interactive: usize,
        active_batch: usize,
    ) -> Option<(WorkClass, T)> {
        if active_interactive + active_batch >= self.config.parallelism {
            return None;
        }
        if self.config.parallelism == 1 && !self.interactive.is_empty() && !self.batch.is_empty() {
            let class = self.single_next;
            self.single_next = match class {
                WorkClass::Batch => WorkClass::Interactive,
                WorkClass::Interactive => WorkClass::Batch,
            };
            return match class {
                WorkClass::Interactive => self.interactive.pop_front().map(|v| (class, v)),
                WorkClass::Batch => self.batch.pop_front().map(|v| (class, v)),
            };
        }
        if active_batch < self.config.batch_reserved_workers {
            if let Some(work) = self.batch.pop_front() {
                return Some((WorkClass::Batch, work));
            }
        }
        if let Some(work) = self.interactive.pop_front() {
            return Some((WorkClass::Interactive, work));
        }
        self.batch.pop_front().map(|work| (WorkClass::Batch, work))
    }
}
