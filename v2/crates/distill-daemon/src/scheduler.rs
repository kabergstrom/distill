//! Coordinator-side admission and cooperative descendant execution.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};

use rayon::ThreadPool;

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

type Job = Box<dyn FnOnce() + Send>;

enum Message {
    Submit { id: u64, class: WorkClass, job: Job },
    Complete(u64),
    Reconfigure {
        config: SchedulerConfig,
        pool: Option<Arc<ThreadPool>>,
        reply: mpsc::SyncSender<Result<(), SchedulerConfigError>>,
    },
    Config(mpsc::SyncSender<SchedulerConfig>),
}

/// A [`Scheduler`] on a thread of its own, admitting jobs onto a worker
/// pool. Jobs and completions arrive as messages; the thread stops once
/// every handle and every running job is gone.
pub(crate) struct ScheduledPool {
    inbox: mpsc::Sender<Message>,
    next_id: AtomicU64,
}

impl ScheduledPool {
    pub(crate) fn start(scheduler: Scheduler, pool: Arc<ThreadPool>) -> std::io::Result<Self> {
        let (inbox, messages) = mpsc::channel();
        std::thread::Builder::new()
            .name("distill-scheduler".to_owned())
            .spawn(move || admit_jobs(scheduler, pool, messages))?;
        Ok(Self {
            inbox,
            next_id: AtomicU64::new(1),
        })
    }

    /// Queue `job` in `class`; it runs on the pool once admitted.
    pub(crate) fn submit(&self, class: WorkClass, job: impl FnOnce() + Send + 'static) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let completion = Completion {
            id,
            inbox: self.inbox.clone(),
        };
        let job: Job = Box::new(move || {
            let _completion = completion;
            job();
        });
        self.inbox
            .send(Message::Submit { id, class, job })
            .expect("the scheduler thread outlives its handles");
    }

    pub(crate) fn config(&self) -> SchedulerConfig {
        let (reply, answer) = mpsc::sync_channel(1);
        self.inbox
            .send(Message::Config(reply))
            .expect("the scheduler thread outlives its handles");
        answer.recv().expect("the scheduler thread answers")
    }

    /// Apply `config`, and move future jobs to `pool` when given.
    pub(crate) fn reconfigure(
        &self,
        config: SchedulerConfig,
        pool: Option<Arc<ThreadPool>>,
    ) -> Result<(), SchedulerConfigError> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.inbox
            .send(Message::Reconfigure {
                config,
                pool,
                reply,
            })
            .expect("the scheduler thread outlives its handles");
        answer.recv().expect("the scheduler thread answers")
    }
}

/// Reports a job complete when dropped, so a panicking job frees its slot too.
struct Completion {
    id: u64,
    inbox: mpsc::Sender<Message>,
}

impl Drop for Completion {
    fn drop(&mut self) {
        let _ = self.inbox.send(Message::Complete(self.id));
    }
}

fn admit_jobs(mut scheduler: Scheduler, mut pool: Arc<ThreadPool>, messages: mpsc::Receiver<Message>) {
    let mut queued = BTreeMap::<u64, Job>::new();
    for message in messages {
        match message {
            Message::Submit { id, class, job } => {
                scheduler
                    .try_enqueue(id, class)
                    .expect("scheduler job identities are unique");
                queued.insert(id, job);
            }
            Message::Complete(id) => scheduler
                .complete(id)
                .expect("scheduled job remains active until its worker returns"),
            Message::Reconfigure {
                config,
                pool: replacement,
                reply,
            } => {
                let result = scheduler.reconfigure(config);
                if result.is_ok() {
                    if let Some(replacement) = replacement {
                        pool = replacement;
                    }
                }
                let _ = reply.send(result);
            }
            Message::Config(reply) => {
                let _ = reply.send(scheduler.config());
            }
        }
        for id in scheduler.admit() {
            let job = queued.remove(&id).expect("an admitted job was queued");
            pool.spawn_fifo(job);
        }
    }
}
