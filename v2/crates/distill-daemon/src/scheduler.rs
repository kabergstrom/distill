//! Coordinator-side admission and cooperative descendant execution.

use std::collections::{BTreeMap, VecDeque};

pub const DEFAULT_DEPENDENCY_DEPTH: usize = 32;
pub const MAX_DEPENDENCY_DEPTH: usize = 64;

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
        if !(1..=MAX_DEPENDENCY_DEPTH).contains(&self.max_dependency_depth) {
            return Err(SchedulerConfigError::DependencyDepthOutOfBounds {
                got: self.max_dependency_depth,
                max: MAX_DEPENDENCY_DEPTH,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerConfigError {
    ParallelismZero,
    BatchReservationOutOfBounds { got: usize, max: usize },
    DependencyDepthOutOfBounds { got: usize, max: usize },
}

impl std::fmt::Display for SchedulerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParallelismZero => f.write_str("pipeline.parallelism must be at least 1"),
            Self::BatchReservationOutOfBounds { got, max } => write!(
                f,
                "pipeline.batch_reserved_workers must be in 1..={max}, got {got}"
            ),
            Self::DependencyDepthOutOfBounds { got, max } => write!(
                f,
                "pipeline.max_dependency_depth must be in 1..={max}, got {got}"
            ),
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

    /// Apply the complete operational-live scheduler configuration without
    /// discarding queued or active jobs. Active excess drains naturally.
    pub fn reconfigure(&mut self, config: SchedulerConfig) -> Result<(), SchedulerConfigError> {
        config.validate()?;
        self.config = config;
        Ok(())
    }

    pub fn is_active(&self, id: u64) -> bool {
        self.active.contains_key(&id)
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
