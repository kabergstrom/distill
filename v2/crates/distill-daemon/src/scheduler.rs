//! Coordinator-side admission, and the build cells that let many requesters
//! share one build.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};

use distill_store::{Store, StoreOpener, StoreWriter};
use rayon::ThreadPool;
use tokio::sync::oneshot;

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

    /// Take a queued job out of its queue; `false` when it is not queued.
    pub fn remove_queued(&mut self, id: u64) -> bool {
        for queue in [&mut self.interactive, &mut self.batch] {
            if let Some(position) = queue.iter().position(|queued| *queued == id) {
                queue.remove(position);
                return true;
            }
        }
        false
    }

    /// Move a queued job to the back of `class`'s queue: a build cell runs
    /// at the highest class among its waiters.
    pub fn reclassify(&mut self, id: u64, class: WorkClass) -> bool {
        if !self.remove_queued(id) {
            return false;
        }
        match class {
            WorkClass::Interactive => self.interactive.push_back(id),
            WorkClass::Batch => self.batch.push_back(id),
        }
        true
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

/// A job runs on the writer the scheduler lends it.
type Job = Box<dyn FnOnce(&mut Store) + Send>;

// ---------------------------------------------------------------------------
// Build cells

/// The key of a build cell: the static-input key of the build it runs.
pub(crate) type CellKey = [u8; 32];

/// What a build cell produces. Every waiter of one cell shares one outcome.
pub(crate) trait CellOutcome: Send + Sync + 'static {
    /// The outcome waiters see when the worker running their cell is lost
    /// (it panicked, or the pool stopped) before it reported one.
    fn lost() -> Self;
}

/// A queued cell's build: it runs on a pool worker, on the writer the
/// scheduler lends it, and may claim the cells its dependencies need.
pub(crate) type CellJob<O> = Box<dyn FnOnce(&mut Store, &CellWorker<O>) -> O + Send>;

enum Message<O> {
    #[cfg_attr(not(test), allow(dead_code))]
    Submit {
        id: u64,
        class: WorkClass,
        job: Job,
        completion: Completion<O>,
    },
    /// A job finished; its writer comes back unless it was lost.
    Complete(u64, Option<StoreWriter>),
    Reconfigure {
        config: SchedulerConfig,
        pool: Option<Arc<ThreadPool>>,
        reply: mpsc::SyncSender<Result<(), SchedulerConfigError>>,
    },
    Config(mpsc::SyncSender<SchedulerConfig>),
    /// A requester's interest in the cell `key`: it joins the cell in
    /// flight, or queues `job` as a new one. `id` names the ticket, and the
    /// cell when this request creates it.
    Request {
        id: u64,
        key: CellKey,
        class: WorkClass,
        job: CellJob<O>,
        reply: oneshot::Sender<Arc<O>>,
    },
    /// A ticket was dropped before its cell finished.
    Release { ticket: u64, key: CellKey },
    /// A worker needs the cell `key` for a dependency.
    Claim {
        worker: u64,
        id: u64,
        key: CellKey,
        reply: mpsc::SyncSender<ClaimReply<O>>,
    },
    /// The cell `key`, running on `worker`, is done.
    Finished {
        worker: u64,
        key: CellKey,
        outcome: Arc<O>,
    },
    #[cfg_attr(not(test), allow(dead_code))]
    CellsInFlight(mpsc::SyncSender<usize>),
    /// The pool's handle is gone: stop once no job is running.
    Closed,
}

enum ClaimReply<O> {
    Run,
    Wait(oneshot::Receiver<Arc<O>>),
    Private,
}

/// A [`Scheduler`] on a thread of its own, admitting jobs onto a worker
/// pool. Requests, claims and completions arrive as messages; the thread
/// stops once every handle, ticket and running job is gone.
///
/// The scheduler thread owns its jobs' store writers: it lends one to each
/// job it admits, which hands it back on completion. A writer is only ever
/// in one place, and idle ones beyond the pool's parallelism are closed.
///
/// It also owns the build cells: one per static-input key while that build
/// is queued or running, holding every waiter's interest. A cell is queued
/// at the highest class among its tickets, dequeued when its last ticket is
/// dropped before it runs, and removed once it has notified its waiters.
pub(crate) struct ScheduledPool<O> {
    inbox: mpsc::Sender<Message<O>>,
    ids: Arc<AtomicU64>,
}

impl<O: CellOutcome> ScheduledPool<O> {
    pub(crate) fn start(
        scheduler: Scheduler,
        pool: Arc<ThreadPool>,
        opener: Arc<StoreOpener>,
    ) -> std::io::Result<Self> {
        let (inbox, messages) = mpsc::channel();
        let ids = Arc::new(AtomicU64::new(1));
        let actor = Actor {
            scheduler,
            pool,
            opener,
            inbox: inbox.clone(),
            ids: Arc::clone(&ids),
            queued_jobs: BTreeMap::new(),
            idle: Vec::new(),
            cells: HashMap::new(),
            queued_cells: HashMap::new(),
            waiting: HashMap::new(),
        };
        // The actor keeps a sender for the workers it spawns; it stops once
        // the handle is gone and no job is running. Cells still queued then
        // are dropped, and their tickets resolve as lost.
        std::thread::Builder::new()
            .name("distill-scheduler".to_owned())
            .spawn(move || actor.run(messages))?;
        Ok(Self { inbox, ids })
    }

    fn next_id(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::Relaxed)
    }

    /// Queue `job` in `class`; it runs on the pool once admitted, on a
    /// writer the scheduler lends it.
    #[cfg(test)]
    pub(crate) fn submit(&self, class: WorkClass, job: impl FnOnce(&mut Store) + Send + 'static) {
        let id = self.next_id();
        let completion = Completion {
            id,
            inbox: self.inbox.clone(),
            writer: None,
        };
        self.inbox
            .send(Message::Submit {
                id,
                class,
                job: Box::new(job),
                completion,
            })
            .expect("the scheduler thread outlives its handles");
    }

    /// Register interest in the cell `key` at `class`. When no cell for the
    /// key is in flight, `job` becomes one and is queued; otherwise `job` is
    /// dropped and the ticket joins the running or queued cell, promoting a
    /// queued one to `class` when that is higher. Dropping the ticket
    /// withdraws the interest.
    pub(crate) fn request(
        &self,
        key: CellKey,
        class: WorkClass,
        job: impl FnOnce(&mut Store, &CellWorker<O>) -> O + Send + 'static,
    ) -> CellTicket<O> {
        let id = self.next_id();
        let (reply, receiver) = oneshot::channel();
        let sent = self.inbox.send(Message::Request {
            id,
            key,
            class,
            job: Box::new(job),
            reply,
        });
        CellTicket {
            id,
            key,
            inbox: self.inbox.clone(),
            receiver,
            done: sent.is_err(),
        }
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

    /// How many cells are queued or running.
    #[cfg(test)]
    pub(crate) fn cells_in_flight(&self) -> usize {
        let (reply, answer) = mpsc::sync_channel(1);
        self.inbox
            .send(Message::CellsInFlight(reply))
            .expect("the scheduler thread outlives its handles");
        answer.recv().expect("the scheduler thread answers")
    }
}

impl<O> Drop for ScheduledPool<O> {
    fn drop(&mut self) {
        let _ = self.inbox.send(Message::Closed);
    }
}

/// A requester's interest in one cell. It resolves to the cell's shared
/// outcome; dropping it first withdraws the interest, and a queued cell no
/// ticket wants any more never runs.
pub(crate) struct CellTicket<O> {
    id: u64,
    key: CellKey,
    inbox: mpsc::Sender<Message<O>>,
    receiver: oneshot::Receiver<Arc<O>>,
    done: bool,
}

impl<O: CellOutcome> std::future::Future for CellTicket<O> {
    type Output = Arc<O>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Arc<O>> {
        match std::pin::Pin::new(&mut self.receiver).poll(cx) {
            std::task::Poll::Ready(outcome) => {
                self.done = true;
                std::task::Poll::Ready(outcome.unwrap_or_else(|_| Arc::new(O::lost())))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<O> Drop for CellTicket<O> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.inbox.send(Message::Release {
                ticket: self.id,
                key: self.key,
            });
        }
    }
}

/// The worker a cell's build runs on. Its dependencies' cells are claimed
/// through it: a worker never waits on a queued cell (it takes the build
/// over and runs it inline), and waits on a running one only when that
/// cannot close a cycle of waiting workers.
pub(crate) struct CellWorker<O> {
    id: u64,
    inbox: mpsc::Sender<Message<O>>,
    ids: Arc<AtomicU64>,
}

/// What a worker does about a dependency's cell.
pub(crate) enum Claim<O: CellOutcome> {
    /// The cell is this worker's now (it was absent or queued): build it
    /// inline and [`CellRun::finish`] it. Its waiters, if any, share the
    /// result.
    Run(CellRun<O>),
    /// Another worker is running the cell: wait for its outcome.
    Wait(CellWait<O>),
    /// Waiting would close a cycle of waiting workers: build the
    /// dependency inline without the cell.
    Private,
}

impl<O: CellOutcome> CellWorker<O> {
    pub(crate) fn claim(&self, key: CellKey) -> Claim<O> {
        let (reply, answer) = mpsc::sync_channel(1);
        let message = Message::Claim {
            worker: self.id,
            id: self.ids.fetch_add(1, Ordering::Relaxed),
            key,
            reply,
        };
        if self.inbox.send(message).is_err() {
            return Claim::Private;
        }
        match answer.recv() {
            Ok(ClaimReply::Run) => Claim::Run(CellRun {
                worker: self.id,
                key,
                inbox: self.inbox.clone(),
                finished: false,
            }),
            Ok(ClaimReply::Wait(receiver)) => Claim::Wait(CellWait(receiver)),
            Ok(ClaimReply::Private) | Err(_) => Claim::Private,
        }
    }
}

/// A cell a worker claimed and runs inline. Dropping it unfinished reports
/// the cell lost, so its waiters never hang.
pub(crate) struct CellRun<O: CellOutcome> {
    worker: u64,
    key: CellKey,
    inbox: mpsc::Sender<Message<O>>,
    finished: bool,
}

impl<O: CellOutcome> CellRun<O> {
    pub(crate) fn finish(mut self, outcome: Arc<O>) {
        self.finished = true;
        let _ = self.inbox.send(Message::Finished {
            worker: self.worker,
            key: self.key,
            outcome,
        });
    }
}

impl<O: CellOutcome> Drop for CellRun<O> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.inbox.send(Message::Finished {
                worker: self.worker,
                key: self.key,
                outcome: Arc::new(O::lost()),
            });
        }
    }
}

/// A running cell another worker waits on.
pub(crate) struct CellWait<O>(oneshot::Receiver<Arc<O>>);

impl<O: CellOutcome> CellWait<O> {
    /// Block this worker until the cell finishes. Only a pool worker waits
    /// this way, never a thread holding a write transaction.
    pub(crate) fn wait(self) -> Arc<O> {
        self.0
            .blocking_recv()
            .unwrap_or_else(|_| Arc::new(O::lost()))
    }
}

/// Reports a job complete when dropped, so a panicking job frees its slot
/// too, and hands back the writer it was lent.
struct Completion<O> {
    id: u64,
    inbox: mpsc::Sender<Message<O>>,
    writer: Option<StoreWriter>,
}

impl<O> Completion<O> {
    /// Report the job complete and hand its writer back.
    fn finish(mut self, writer: StoreWriter) {
        self.writer = Some(writer);
    }
}

impl<O> Drop for Completion<O> {
    fn drop(&mut self) {
        let _ = self
            .inbox
            .send(Message::Complete(self.id, self.writer.take()));
    }
}

struct Cell<O> {
    /// The cell's id in the scheduler's queues while it is queued.
    id: u64,
    class: WorkClass,
    state: CellState<O>,
    tickets: BTreeMap<u64, (WorkClass, oneshot::Sender<Arc<O>>)>,
    /// Workers blocked on this cell.
    workers: Vec<(u64, oneshot::Sender<Arc<O>>)>,
}

enum CellState<O> {
    Queued(CellJob<O>),
    Running { worker: u64 },
}

struct Actor<O> {
    scheduler: Scheduler,
    pool: Arc<ThreadPool>,
    opener: Arc<StoreOpener>,
    inbox: mpsc::Sender<Message<O>>,
    ids: Arc<AtomicU64>,
    queued_jobs: BTreeMap<u64, (Job, Completion<O>)>,
    idle: Vec<StoreWriter>,
    cells: HashMap<CellKey, Cell<O>>,
    /// Queued cells by scheduler id.
    queued_cells: HashMap<u64, CellKey>,
    /// The wait-for graph: a blocked worker and the worker running the cell
    /// it waits on. Each worker waits on at most one cell.
    waiting: HashMap<u64, u64>,
}

fn rank(class: WorkClass) -> u8 {
    match class {
        WorkClass::Batch => 0,
        WorkClass::Interactive => 1,
    }
}

impl<O: CellOutcome> Actor<O> {
    fn run(mut self, messages: mpsc::Receiver<Message<O>>) {
        let mut closed = false;
        // The actor's own sender keeps the channel open: it stops once the
        // handle is closed and nothing runs.
        loop {
            let Ok(message) = messages.recv() else {
                return;
            };
            if matches!(message, Message::Closed) {
                closed = true;
            } else {
                self.handle(message);
            }
            if closed {
                if self.scheduler.active.is_empty() {
                    return;
                }
            } else {
                self.admit();
            }
        }
    }

    fn handle(&mut self, message: Message<O>) {
        match message {
            Message::Submit {
                id,
                class,
                job,
                completion,
            } => {
                self.scheduler
                    .try_enqueue(id, class)
                    .expect("scheduler job identities are unique");
                self.queued_jobs.insert(id, (job, completion));
            }
            Message::Complete(id, writer) => {
                self.scheduler
                    .complete(id)
                    .expect("scheduled job remains active until its worker returns");
                // A writer left inside a transaction is not lent again.
                if let Some(writer) = writer.filter(|writer| !writer.in_transaction()) {
                    if self.idle.len() < self.scheduler.config().parallelism {
                        self.idle.push(writer);
                    }
                }
                // Cells the job left running were lost with it.
                let lost = self
                    .cells
                    .iter()
                    .filter(|(_, cell)| {
                        matches!(cell.state, CellState::Running { worker } if worker == id)
                    })
                    .map(|(key, _)| *key)
                    .collect::<Vec<_>>();
                for key in lost {
                    self.finish(id, key, Arc::new(O::lost()));
                }
                self.waiting.remove(&id);
            }
            Message::Reconfigure {
                config,
                pool: replacement,
                reply,
            } => {
                let result = self.scheduler.reconfigure(config);
                if result.is_ok() {
                    if let Some(replacement) = replacement {
                        self.pool = replacement;
                    }
                }
                let _ = reply.send(result);
            }
            Message::Config(reply) => {
                let _ = reply.send(self.scheduler.config());
            }
            Message::Request {
                id,
                key,
                class,
                job,
                reply,
            } => match self.cells.get_mut(&key) {
                Some(cell) => {
                    cell.tickets.insert(id, (class, reply));
                    if matches!(cell.state, CellState::Queued(_)) && rank(class) > rank(cell.class)
                    {
                        self.scheduler.reclassify(cell.id, class);
                        cell.class = class;
                    }
                }
                None => {
                    self.cells.insert(
                        key,
                        Cell {
                            id,
                            class,
                            state: CellState::Queued(job),
                            tickets: BTreeMap::from([(id, (class, reply))]),
                            workers: Vec::new(),
                        },
                    );
                    self.queued_cells.insert(id, key);
                    self.scheduler
                        .try_enqueue(id, class)
                        .expect("scheduler job identities are unique");
                }
            },
            Message::Release { ticket, key } => {
                let Some(cell) = self.cells.get_mut(&key) else {
                    return;
                };
                if cell.tickets.remove(&ticket).is_none()
                    || !matches!(cell.state, CellState::Queued(_))
                {
                    return;
                }
                // Only tickets wait on a queued cell: a worker takes a
                // queued cell over instead of waiting on it.
                match cell.tickets.values().map(|(class, _)| *class).max_by_key(|c| rank(*c)) {
                    None => {
                        let id = cell.id;
                        self.scheduler.remove_queued(id);
                        self.queued_cells.remove(&id);
                        self.cells.remove(&key);
                    }
                    Some(class) if class != cell.class => {
                        self.scheduler.reclassify(cell.id, class);
                        cell.class = class;
                    }
                    Some(_) => {}
                }
            }
            Message::Claim {
                worker,
                id,
                key,
                reply,
            } => {
                let answer = match self.cells.get_mut(&key) {
                    None => {
                        self.cells.insert(
                            key,
                            Cell {
                                id,
                                class: WorkClass::Interactive,
                                state: CellState::Running { worker },
                                tickets: BTreeMap::new(),
                                workers: Vec::new(),
                            },
                        );
                        ClaimReply::Run
                    }
                    Some(cell) => match cell.state {
                        CellState::Queued(_) => {
                            self.scheduler.remove_queued(cell.id);
                            self.queued_cells.remove(&cell.id);
                            cell.state = CellState::Running { worker };
                            ClaimReply::Run
                        }
                        CellState::Running { worker: runner } => {
                            if would_cycle(&self.waiting, worker, runner) {
                                ClaimReply::Private
                            } else {
                                let (sender, receiver) = oneshot::channel();
                                cell.workers.push((worker, sender));
                                self.waiting.insert(worker, runner);
                                ClaimReply::Wait(receiver)
                            }
                        }
                    },
                };
                match reply.send(answer) {
                    // The claiming worker is gone: nothing will run the cell.
                    Err(mpsc::SendError(ClaimReply::Run)) => {
                        self.finish(worker, key, Arc::new(O::lost()))
                    }
                    Err(mpsc::SendError(ClaimReply::Wait(_))) => {
                        self.waiting.remove(&worker);
                    }
                    _ => {}
                }
            }
            Message::Finished {
                worker,
                key,
                outcome,
            } => self.finish(worker, key, outcome),
            Message::CellsInFlight(reply) => {
                let _ = reply.send(self.cells.len());
            }
            Message::Closed => {}
        }
    }

    /// Notify every waiter of the cell `key` running on `worker`, and
    /// remove it.
    fn finish(&mut self, worker: u64, key: CellKey, outcome: Arc<O>) {
        let running_here = self.cells.get(&key).is_some_and(|cell| {
            matches!(cell.state, CellState::Running { worker: runner } if runner == worker)
        });
        if !running_here {
            return;
        }
        let cell = self.cells.remove(&key).expect("checked above");
        for (_, (_, ticket)) in cell.tickets {
            let _ = ticket.send(Arc::clone(&outcome));
        }
        for (waiter, sender) in cell.workers {
            self.waiting.remove(&waiter);
            let _ = sender.send(Arc::clone(&outcome));
        }
    }

    fn admit(&mut self) {
        for id in self.scheduler.admit() {
            let writer = self
                .idle
                .pop()
                .map_or_else(|| self.opener.open_writer(), Ok);
            if let Some((job, completion)) = self.queued_jobs.remove(&id) {
                self.pool.spawn_fifo(move || {
                    let mut writer = writer.unwrap_or_else(|error| {
                        panic!("cannot open a writer for a build job: {error}")
                    });
                    job(&mut writer);
                    completion.finish(writer);
                });
                continue;
            }
            let key = self
                .queued_cells
                .remove(&id)
                .expect("an admitted job was queued");
            let cell = self.cells.get_mut(&key).expect("a queued cell is in flight");
            let CellState::Queued(job) =
                std::mem::replace(&mut cell.state, CellState::Running { worker: id })
            else {
                unreachable!("an admitted cell was queued");
            };
            let completion = Completion {
                id,
                inbox: self.inbox.clone(),
                writer: None,
            };
            let worker = CellWorker {
                id,
                inbox: self.inbox.clone(),
                ids: Arc::clone(&self.ids),
            };
            self.pool.spawn_fifo(move || {
                // Without a writer the completion reports the cell lost.
                let Ok(mut writer) = writer.inspect_err(|error| {
                    tracing::error!(%error, "cannot open a writer for a build cell");
                }) else {
                    return;
                };
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    job(&mut writer, &worker)
                }))
                .map_or_else(|_| Arc::new(O::lost()), Arc::new);
                let _ = worker.inbox.send(Message::Finished {
                    worker: id,
                    key,
                    outcome,
                });
                completion.finish(writer);
            });
        }
    }
}

/// Whether `worker` waiting on `runner` would close a cycle: following the
/// workers `runner` transitively waits on reaches `worker`.
fn would_cycle(waiting: &HashMap<u64, u64>, worker: u64, runner: u64) -> bool {
    let mut current = runner;
    // Each worker waits on at most one other: the chain is a path, at most
    // as long as the graph.
    for _ in 0..=waiting.len() {
        if current == worker {
            return true;
        }
        match waiting.get(&current) {
            Some(next) => current = *next,
            None => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[derive(Debug, PartialEq)]
    enum TestOutcome {
        Value(u32),
        Failed(String),
        Lost,
    }

    impl CellOutcome for TestOutcome {
        fn lost() -> Self {
            TestOutcome::Lost
        }
    }

    const WATCHDOG: Duration = Duration::from_secs(20);

    fn pool(
        parallelism: usize,
    ) -> (tempfile::TempDir, ScheduledPool<TestOutcome>) {
        let temp = tempfile::tempdir().unwrap();
        let store =
            Store::open(distill_store::StoreConfig::new(temp.path().join("state"))).unwrap();
        let (opener, _writer) = StoreOpener::new(store);
        let config = SchedulerConfig {
            parallelism,
            batch_reserved_workers: 1,
            max_dependency_depth: 8,
        };
        let threads = rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism)
            .build()
            .map(Arc::new)
            .unwrap();
        let pool = ScheduledPool::start(Scheduler::new(config).unwrap(), threads, opener).unwrap();
        (temp, pool)
    }

    /// Block a worker until the returned sender sends or drops.
    fn occupy(pool: &ScheduledPool<TestOutcome>, class: WorkClass) -> mpsc::Sender<()> {
        let (release, released) = mpsc::channel::<()>();
        let (started, running) = mpsc::sync_channel(1);
        pool.submit(class, move |_| {
            started.send(()).unwrap();
            let _ = released.recv();
        });
        running.recv_timeout(WATCHDOG).expect("the blocker started");
        release
    }

    /// Run `future` on this thread, bounded by the watchdog.
    fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime
            .block_on(async { tokio::time::timeout(WATCHDOG, future).await })
            .expect("watchdog: the cell never finished")
    }

    fn wait_until(mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + WATCHDOG;
        while !done() {
            assert!(std::time::Instant::now() < deadline, "watchdog");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn concurrent_requests_for_one_key_run_one_build_and_share_its_outcome() {
        let (_temp, pool) = pool(2);
        let runs = Arc::new(AtomicUsize::new(0));
        let blocker = occupy(&pool, WorkClass::Interactive);
        let blocker_batch = occupy(&pool, WorkClass::Batch);
        let tickets = (0..8)
            .map(|_| {
                let runs = Arc::clone(&runs);
                pool.request([1; 32], WorkClass::Interactive, move |_, _| {
                    runs.fetch_add(1, Ordering::SeqCst);
                    TestOutcome::Value(7)
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(pool.cells_in_flight(), 1);
        drop((blocker, blocker_batch));
        let outcomes = tickets.into_iter().map(block_on).collect::<Vec<_>>();
        assert!(outcomes.iter().all(|outcome| **outcome == TestOutcome::Value(7)));
        assert!(outcomes.windows(2).all(|pair| Arc::ptr_eq(&pair[0], &pair[1])));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        wait_until(|| pool.cells_in_flight() == 0);
    }

    #[test]
    fn a_running_cell_outlives_one_dropped_ticket_and_a_queued_one_no_ticket_wants_never_runs() {
        let (_temp, pool) = pool(1);
        let (release, released) = mpsc::channel::<()>();
        let (started, running) = mpsc::sync_channel(1);
        let first = pool.request([1; 32], WorkClass::Interactive, move |_, _| {
            started.send(()).unwrap();
            let _ = released.recv();
            TestOutcome::Value(1)
        });
        running.recv_timeout(WATCHDOG).unwrap();
        let second = pool.request([1; 32], WorkClass::Interactive, |_, _| {
            unreachable!("joined the running cell")
        });
        drop(first);
        // The single worker is busy: this cell stays queued.
        let ran = Arc::new(AtomicUsize::new(0));
        let queued = {
            let ran = Arc::clone(&ran);
            pool.request([2; 32], WorkClass::Interactive, move |_, _| {
                ran.fetch_add(1, Ordering::SeqCst);
                TestOutcome::Value(2)
            })
        };
        assert_eq!(pool.cells_in_flight(), 2);
        drop(queued);
        wait_until(|| pool.cells_in_flight() == 1);
        release.send(()).unwrap();
        assert_eq!(*block_on(second), TestOutcome::Value(1));
        // A later job runs after anything that was queued before it.
        let (done, finished) = mpsc::sync_channel(1);
        pool.submit(WorkClass::Interactive, move |_| done.send(()).unwrap());
        finished.recv_timeout(WATCHDOG).unwrap();
        assert_eq!(ran.load(Ordering::SeqCst), 0, "the abandoned cell never ran");
        assert_eq!(pool.cells_in_flight(), 0);
    }

    #[test]
    fn an_interactive_waiter_promotes_a_queued_batch_cell() {
        let (_temp, pool) = pool(1);
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let blocker = occupy(&pool, WorkClass::Interactive);
        let record = |name: &'static str| {
            let order = Arc::clone(&order);
            move |_: &mut Store| order.lock().unwrap().push(name)
        };
        pool.submit(WorkClass::Batch, record("batch-1"));
        pool.submit(WorkClass::Batch, record("batch-2"));
        let cell_order = Arc::clone(&order);
        let batch = pool.request([3; 32], WorkClass::Batch, move |_, _| {
            cell_order.lock().unwrap().push("cell");
            TestOutcome::Value(3)
        });
        let interactive = pool.request([3; 32], WorkClass::Interactive, |_, _| {
            unreachable!("joined the queued cell")
        });
        drop(blocker);
        assert_eq!(*block_on(interactive), TestOutcome::Value(3));
        assert_eq!(*block_on(batch), TestOutcome::Value(3));
        wait_until(|| order.lock().unwrap().len() == 3);
        // At one worker the classes alternate; promoted, the cell runs in
        // the interactive turn ahead of the second batch job.
        assert_eq!(&order.lock().unwrap()[..], &["batch-1", "cell", "batch-2"]);
    }

    #[test]
    fn a_dependency_chain_completes_on_one_worker() {
        let (_temp, pool) = pool(1);
        // The dependencies are queued behind the root on the only worker;
        // claiming them takes them over instead of waiting.
        let blocker = occupy(&pool, WorkClass::Interactive);
        let root = pool.request([10; 32], WorkClass::Interactive, |_, worker| {
            let mut total = 0;
            for key in [[11; 32], [12; 32]] {
                match worker.claim(key) {
                    Claim::Run(run) => {
                        total += 1;
                        run.finish(Arc::new(TestOutcome::Value(1)));
                    }
                    Claim::Wait(_) | Claim::Private => panic!("a lone worker never waits"),
                }
            }
            TestOutcome::Value(total)
        });
        let dependency = pool.request([11; 32], WorkClass::Interactive, |_, _| {
            unreachable!("the root's worker took the queued dependency over")
        });
        drop(blocker);
        assert_eq!(*block_on(root), TestOutcome::Value(2));
        assert_eq!(*block_on(dependency), TestOutcome::Value(1));
        assert_eq!(pool.cells_in_flight(), 0);
    }

    #[test]
    fn a_worker_waits_on_a_running_dependency_unless_that_closes_a_cycle() {
        let (_temp, pool) = pool(2);
        let (b_claimed, b_claim) = mpsc::sync_channel(1);
        let (a_waiting, a_wait) = mpsc::sync_channel::<()>(1);
        // Worker A runs [20] and needs [21]; worker B runs [21] and needs
        // [20]. A waits on B; B waiting on A would close the cycle, so B
        // builds [20] privately and both finish.
        let a = pool.request([20; 32], WorkClass::Interactive, move |_, worker| {
            b_claim.recv_timeout(WATCHDOG).unwrap();
            match worker.claim([21; 32]) {
                Claim::Wait(wait) => {
                    a_waiting.send(()).unwrap();
                    match &*wait.wait() {
                        TestOutcome::Value(value) => TestOutcome::Value(value + 100),
                        other => TestOutcome::Failed(format!("{other:?}")),
                    }
                }
                _ => TestOutcome::Failed("expected to wait".to_owned()),
            }
        });
        let b = pool.request([21; 32], WorkClass::Batch, move |_, worker| {
            b_claimed.send(()).unwrap();
            a_wait.recv_timeout(WATCHDOG).unwrap();
            match worker.claim([20; 32]) {
                Claim::Private => TestOutcome::Value(21),
                _ => TestOutcome::Failed("expected a private build".to_owned()),
            }
        });
        assert_eq!(*block_on(b), TestOutcome::Value(21));
        assert_eq!(*block_on(a), TestOutcome::Value(121));
    }

    #[test]
    fn a_failure_reaches_every_waiter_and_a_lost_worker_is_reported() {
        let (_temp, pool) = pool(1);
        let blocker = occupy(&pool, WorkClass::Interactive);
        let tickets = (0..3)
            .map(|_| {
                pool.request([30; 32], WorkClass::Interactive, |_, _| {
                    TestOutcome::Failed("rejected".to_owned())
                })
            })
            .collect::<Vec<_>>();
        let panicking = pool.request([31; 32], WorkClass::Interactive, |_, _| {
            panic!("the build panicked")
        });
        drop(blocker);
        for ticket in tickets {
            assert_eq!(*block_on(ticket), TestOutcome::Failed("rejected".to_owned()));
        }
        assert_eq!(*block_on(panicking), TestOutcome::Lost);
        wait_until(|| pool.cells_in_flight() == 0);
    }
}
