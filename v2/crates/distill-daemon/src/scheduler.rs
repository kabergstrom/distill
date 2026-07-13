//! Coordinator-side admission and cooperative descendant execution.

use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkClass {
    Interactive,
    Batch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub parallelism: usize,
    pub batch_reserved_workers: usize,
    pub max_dependency_depth: usize,
}

impl SchedulerConfig {
    pub fn validate(self) -> Result<(), SchedulerConfigError> {
        if self.parallelism == 0 {
            return Err(SchedulerConfigError::ParallelismZero);
        }
        let max = self.parallelism.saturating_sub(1).max(1);
        if self.batch_reserved_workers == 0 || self.batch_reserved_workers > max {
            return Err(SchedulerConfigError::BatchReservationOutOfBounds {
                got: self.batch_reserved_workers,
                max,
            });
        }
        if self.max_dependency_depth == 0 {
            return Err(SchedulerConfigError::DependencyDepthZero);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerConfigError {
    ParallelismZero,
    BatchReservationOutOfBounds { got: usize, max: usize },
    DependencyDepthZero,
}

impl std::fmt::Display for SchedulerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParallelismZero => f.write_str("pipeline.parallelism must be at least 1"),
            Self::BatchReservationOutOfBounds { got, max } => write!(
                f,
                "pipeline.batch_reserved_workers must be in 1..={max}, got {got}"
            ),
            Self::DependencyDepthZero => {
                f.write_str("pipeline.max_dependency_depth must be at least 1")
            }
        }
    }
}

impl std::error::Error for SchedulerConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerError {
    UnknownActiveJob(u64),
    DuplicateJob(u64),
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownActiveJob(id) => write!(f, "job {id} is not active"),
            Self::DuplicateJob(id) => write!(f, "job {id} is already queued or active"),
        }
    }
}

impl std::error::Error for SchedulerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerResize {
    pub active_slots_to_drain: usize,
    pub batch_reserved_workers: usize,
    pub single_worker_alternates: bool,
}

/// FIFO within each class. At two or more workers, batch work owns a reserved
/// minimum while every other slot retains strict interactive priority. At one
/// worker the selected class alternates whenever both are pending.
pub struct Scheduler {
    config: SchedulerConfig,
    interactive: VecDeque<u64>,
    batch: VecDeque<u64>,
    active: BTreeMap<u64, WorkClass>,
    single_next: WorkClass,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Result<Self, SchedulerConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            interactive: VecDeque::new(),
            batch: VecDeque::new(),
            active: BTreeMap::new(),
            single_next: WorkClass::Interactive,
        })
    }

    pub fn config(&self) -> SchedulerConfig {
        self.config
    }

    pub fn enqueue(&mut self, id: u64, class: WorkClass) {
        assert!(!self.contains(id), "{}", SchedulerError::DuplicateJob(id));
        match class {
            WorkClass::Interactive => self.interactive.push_back(id),
            WorkClass::Batch => self.batch.push_back(id),
        }
    }

    pub fn try_enqueue(&mut self, id: u64, class: WorkClass) -> Result<(), SchedulerError> {
        if self.contains(id) {
            return Err(SchedulerError::DuplicateJob(id));
        }
        match class {
            WorkClass::Interactive => self.interactive.push_back(id),
            WorkClass::Batch => self.batch.push_back(id),
        }
        Ok(())
    }

    pub fn admit(&mut self) -> Vec<u64> {
        let mut admitted = Vec::new();
        while self.active.len() < self.config.parallelism {
            let Some((id, class)) = self.pop_next() else {
                break;
            };
            self.active.insert(id, class);
            admitted.push(id);
        }
        admitted
    }

    pub fn complete(&mut self, id: u64) -> Result<(), SchedulerError> {
        if self.active.remove(&id).is_none() {
            return Err(SchedulerError::UnknownActiveJob(id));
        }
        Ok(())
    }

    /// Operational-live resize. Work above the new limit remains active and
    /// simply drains; no admission occurs until active count falls below it.
    pub fn resize(
        &mut self,
        parallelism: usize,
        batch_reserved_workers: usize,
    ) -> Result<(), SchedulerConfigError> {
        let next = SchedulerConfig {
            parallelism,
            batch_reserved_workers,
            max_dependency_depth: self.config.max_dependency_depth,
        };
        next.validate()?;
        self.config = next;
        Ok(())
    }

    /// Resize only the worker pool and re-clamp the live reservation. This is
    /// the operational-live `pipeline.parallelism` transition: active excess
    /// work drains naturally and is never cancelled.
    pub fn resize_parallelism(
        &mut self,
        parallelism: usize,
    ) -> Result<SchedulerResize, SchedulerConfigError> {
        if parallelism == 0 {
            return Err(SchedulerConfigError::ParallelismZero);
        }
        let reservation_max = parallelism.saturating_sub(1).max(1);
        self.config.parallelism = parallelism;
        self.config.batch_reserved_workers =
            self.config.batch_reserved_workers.clamp(1, reservation_max);
        Ok(SchedulerResize {
            active_slots_to_drain: self.active.len().saturating_sub(parallelism),
            batch_reserved_workers: self.config.batch_reserved_workers,
            single_worker_alternates: parallelism == 1,
        })
    }

    pub fn active_len(&self) -> usize {
        self.active.len()
    }

    fn contains(&self, id: u64) -> bool {
        self.active.contains_key(&id) || self.interactive.contains(&id) || self.batch.contains(&id)
    }

    fn pop_next(&mut self) -> Option<(u64, WorkClass)> {
        if self.config.parallelism == 1 {
            return self.pop_single_worker();
        }
        let active_batch = self
            .active
            .values()
            .filter(|class| **class == WorkClass::Batch)
            .count();
        if !self.batch.is_empty() && active_batch < self.config.batch_reserved_workers {
            return self.batch.pop_front().map(|id| (id, WorkClass::Batch));
        }
        self.interactive
            .pop_front()
            .map(|id| (id, WorkClass::Interactive))
            .or_else(|| self.batch.pop_front().map(|id| (id, WorkClass::Batch)))
    }

    fn pop_single_worker(&mut self) -> Option<(u64, WorkClass)> {
        let selection = match (self.interactive.is_empty(), self.batch.is_empty()) {
            (false, false) => self.single_next,
            (false, true) => WorkClass::Interactive,
            (true, false) => WorkClass::Batch,
            (true, true) => return None,
        };
        self.single_next = match selection {
            WorkClass::Interactive => WorkClass::Batch,
            WorkClass::Batch => WorkClass::Interactive,
        };
        match selection {
            WorkClass::Interactive => self
                .interactive
                .pop_front()
                .map(|id| (id, WorkClass::Interactive)),
            WorkClass::Batch => self.batch.pop_front().map(|id| (id, WorkClass::Batch)),
        }
    }
}

pub struct ChainFrame<T> {
    name: String,
    run: Box<dyn FnOnce() -> ChainStep<T> + Send>,
}

impl<T> ChainFrame<T> {
    pub fn new(
        name: impl Into<String>,
        run: impl FnOnce() -> ChainStep<T> + Send + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            run: Box::new(run),
        }
    }
}

pub enum ChainStep<T> {
    Complete(T),
    Continue(ChainFrame<T>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainDepthExceeded {
    pub limit: usize,
    pub chain: Vec<String>,
}

impl ChainDepthExceeded {
    /// Depth is caller-local resource policy, so this outcome must never enter
    /// the build-failure memo table.
    pub const fn memoizable(&self) -> bool {
        false
    }
}

impl std::fmt::Display for ChainDepthExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "dependency depth {} exceeded: {}",
            self.limit,
            self.chain.join(" -> ")
        )
    }
}

impl std::error::Error for ChainDepthExceeded {}

/// Execute an arbitrarily deep cooperative chain with one native call frame.
/// A cache consult is represented by one frame which immediately completes,
/// so no historical subtree contributes to this request's live depth.
pub fn run_trampolined<T>(
    mut frame: ChainFrame<T>,
    max_depth: usize,
) -> Result<T, ChainDepthExceeded> {
    let mut chain = Vec::new();
    loop {
        chain.push(frame.name);
        if chain.len() > max_depth {
            return Err(ChainDepthExceeded {
                limit: max_depth,
                chain,
            });
        }
        frame = match (frame.run)() {
            ChainStep::Complete(value) => return Ok(value),
            ChainStep::Continue(next) => next,
        };
    }
}
