//! Development `LoaderIO`: Cap'n Proto to the local daemon, driven on the
//! caller's thread. There is no IO thread.
//!
//! # Ownership
//!
//! `RpcIo` owns a tokio `current_thread` runtime and a `LocalSet`
//! (capnp-rpc clients are `!Send`). Every request, the delta stream, the
//! subscription calls, the reconnect attempts, and capnp's `RpcSystem` are
//! tasks on that `LocalSet`. They run only while `RpcIo` steps the runtime,
//! which it does from [`LoaderIO::poll`]; every other method only records
//! work or cancels it. Tasks hand their results to a plain queue in
//! [`Shared`]; `poll` drains it. Nothing crosses a thread, so nothing can
//! wait for the engine and the engine never waits for IO.
//!
//! # Stepping
//!
//! One *turn* is `runtime.block_on(f)`, where `f`
//!
//! 1. polls the `LocalSet` once, through a waker that records whether it
//!    was woken: the `LocalSet` runs up to its per-tick budget of ready
//!    tasks (tasks woken while it runs, such as capnp's `RpcSystem` waking a
//!    request whose answer it just read, run in the same tick); then
//! 2. awaits `tokio::task::yield_now()`. Inside a runtime that defers its
//!    wake-up to the scheduler, so `block_on` finds no ready task and a
//!    pending deferred wake and calls `park_yield`: the IO driver polls
//!    epoll with a **zero** timeout and the time driver fires due timers.
//!    Readiness wakes the tasks waiting on it, which schedules them on the
//!    `LocalSet` and calls its registered waker (step 1's), which records
//!    the wake. The deferred wake then completes `f`.
//!
//! A turn therefore never blocks: it runs what is ready and collects what
//! the kernel has ready. [`RpcIo::step`] repeats turns while the previous
//! turn reported a wake (more work is ready) or admission started new work,
//! and stops when quiescent or when `RpcIoConfig::step_budget` has elapsed.
//! The budget is checked between turns; one turn is bounded by the
//! `LocalSet`'s tick budget (61 task polls), and each task poll does bounded
//! work (one RPC answer, one 64 KiB chunk, or one artifact's final checks).
//!
//! # Snapshots and sweeps
//!
//! `RpcIo` keeps the newest snapshot capability it holds (`current`), plus
//! the snapshot of every sweep begun and not yet ended. `begin_sweep`
//! returns `current`'s stamp without IO. `current` is kept fresh by the
//! tasks that learn it is stale, *before* they publish what they learned: a
//! delta, a `Drifted` resolve, or an expired snapshot is pushed only after a
//! snapshot that answers it is installed. So the round the loader starts
//! in response always gets a basis that answers it. A periodic refresh
//! keeps `current` well inside the daemon's snapshot TTL while idle.
//!
//! # Admission and cancellation
//!
//! Requests queue in `RpcIo` and start (as tasks) only while the in-flight
//! count is under `max_in_flight_requests`, and fetches also under
//! `max_in_flight_fetches`. A started fetch learns its payload size from
//! the daemon's answer and reserves it against `fetch_memory_budget`
//! ([`crate::admission`]) before it reads the payload stream; a reservation
//! that does not fit waits in FIFO order, holding its fetch slot but
//! reading nothing (the chunk stream is pulled, so the daemon sends nothing
//! meanwhile). A reservation ends when the loader takes the payload from
//! `poll`, which admits the next waiters; the engine itself never waits on
//! admission. Payloads are held only in memory.
//! `end_sweep` cancels by dropping: queued requests are removed, running
//! tasks are aborted (the `LocalSet` drops them at its next turn, releasing
//! their admission slot, their place in the admission queue or their
//! reservation, their snapshot reference, and their partial payload), and
//! undelivered answers are discarded. `bind_target` does the
//! same for everything of the old connection, which fences the old
//! connection: no event from it can follow a rebind. Dropping `RpcIo` drops
//! every task without running it.
//!
//! # Without a connection
//!
//! [`RpcIo::connect_in_background`] starts with no connection; the first
//! `bind_target` starts connecting, and a failed attempt retries with
//! backoff (25 ms doubling to 1 s), as every rebind does. While no
//! connection is bound, requests are answered at once as by a daemon that
//! names nothing, so the layers under it in a `LayeredIO` answer instead:
//! a resolve or path this IO never named is `Missing`; one it named fails
//! (`RequestError`), so what is resident keeps its value instead of
//! turning `Dead`; a fetch fails. Losing a connection publishes no delta.
//! [`RpcIo::status`] tells whether a connection is bound and the last
//! connection error.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use distill_build::trace::EntryRole;
use distill_core::id::{AssetUuid, ContentHash, TypeUuid};
use distill_rpc::capnp_loader::{RemoteCall, RemoteHub, RemoteSnapshot, RemoteSubscription};
use distill_rpc::capnp_transport::{CapnpClient, RemoteConnectOutcome, RemoteError};
use distill_rpc::{
    AssetEvent, ConnectRequest, DescribableTypesResult, DescribeTypeResult, DriftedInput as RpcDriftedInput,
    ImportFailure, RpcFailure, StreamEvent, TypeDescription,
};
use distill_store::state::{InputVersion, SnapshotStamp};
use tokio::task::{AbortHandle, LocalSet};

use crate::admission::{FetchAdmission, Reservation};
use crate::authoring::{AuthoringAnswer, AuthoringRequest, IndexChange};
use crate::io::{
    AssetDeltaState, AssetPath, DriftedInput, IoEvent, LoaderIO, PathResolveResult,
    ReconnectReason, ReqId, ResolveResult, RuntimeTarget,
};
use crate::rpc_decode::{fetched_artifact, io_basis};
use crate::IoBasis;

const DEFAULT_FETCH_MEMORY_BUDGET: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 256;
const DEFAULT_MAX_IN_FLIGHT_FETCHES: usize = 32;
const DEFAULT_STEP_BUDGET: Duration = Duration::from_millis(2);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Well inside the daemon's default snapshot TTL (30 s).
const DEFAULT_SNAPSHOT_REFRESH_AFTER: Duration = Duration::from_secs(10);
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(25);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(1);
const RECONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
/// Watched-import failures publish no version; RpcIO polls them.
const IMPORT_FAILURE_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Consecutive failed rebind attempts before the game hears of them. A
/// pipeline swap briefly fences connections (the daemon answers
/// `PipelineUnavailable` until the new epoch serves); the backoff rides that
/// out without a diagnostic.
const DEFAULT_TARGET_REJECTION_AFTER: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcIoConfig {
    /// Fetched payload bytes RpcIO holds in memory, in flight or awaiting
    /// `poll`. A fetch whose payload does not fit waits, without reading
    /// it, until earlier payloads are taken; a payload larger than the
    /// whole budget is admitted once nothing else is held.
    pub fetch_memory_budget: usize,
    /// Consecutive failed rebind attempts after which `TargetRejected` is
    /// reported (once per rebind). Retries continue either way.
    pub target_rejection_after: u32,
    /// Resolve, path, and fetch requests running at once; the rest queue.
    pub max_in_flight_requests: usize,
    /// Fetches running at once, within `max_in_flight_requests`.
    pub max_in_flight_fetches: usize,
    /// How long one `poll` may keep stepping the runtime while work is
    /// ready. Checked between turns.
    pub step_budget: Duration,
    /// How long `connect` may wait for the daemon before failing.
    pub connect_timeout: Duration,
    /// Age after which the held snapshot is reopened in the background.
    pub snapshot_refresh_after: Duration,
}

impl Default for RpcIoConfig {
    fn default() -> Self {
        Self {
            fetch_memory_budget: DEFAULT_FETCH_MEMORY_BUDGET,
            target_rejection_after: DEFAULT_TARGET_REJECTION_AFTER,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
            max_in_flight_fetches: DEFAULT_MAX_IN_FLIGHT_FETCHES,
            step_budget: DEFAULT_STEP_BUDGET,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            snapshot_refresh_after: DEFAULT_SNAPSHOT_REFRESH_AFTER,
        }
    }
}

#[derive(Debug)]
pub enum RpcIoInitError {
    ReconnectRequired(ReconnectReason),
    Unavailable(String),
}

impl std::fmt::Display for RpcIoInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "RPC loader initialization failed: {self:?}")
    }
}

impl std::error::Error for RpcIoInitError {}

impl RpcIoInitError {
    fn message(self) -> String {
        match self {
            Self::Unavailable(message) => message,
            Self::ReconnectRequired(reason) => format!("RPC reconnection required: {reason:?}"),
        }
    }
}

/// What RpcIO holds right now; every count is bounded by admission or by
/// what the loader asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RpcIoStats {
    /// Requests waiting for admission.
    pub queued_requests: usize,
    /// Request tasks alive, fetches included.
    pub in_flight_requests: usize,
    /// Fetches running, those waiting for admission included.
    pub in_flight_fetches: usize,
    /// Fetches waiting for their payload to fit the memory budget.
    pub waiting_fetches: usize,
    /// Events waiting for the next `poll`.
    pub undelivered_events: usize,
    /// Fetched payload bytes reserved in memory.
    pub resident_fetch_bytes: usize,
    /// Snapshot capabilities held: the current one plus live sweeps'.
    pub held_snapshots: usize,
    /// Subscription, stream, and maintenance tasks alive.
    pub control_tasks: usize,
    /// Artifact payload bytes received since the IO was created.
    pub received_bytes: u64,
    /// Artifact payload chunks received since the IO was created.
    pub received_chunks: u64,
}

/// Whether an [`RpcIo`] has a connection; the engine shows "Distill
/// unreachable" from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcIoStatus {
    pub address: SocketAddr,
    /// A connection is bound to the loader's target.
    pub connected: bool,
    /// Why the last connection attempt failed or the last connection
    /// ended; `None` once a connection binds.
    pub last_error: Option<String>,
}

/// What one [`RpcIo::step`] did: the work a frame's `poll` may do, which
/// the module docs bound ("Stepping") by the step budget plus one turn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StepStats {
    /// Turns the step ran: one, then more only while work was ready and
    /// the budget not spent.
    pub turns: usize,
    /// Polls of the IO's own tasks (requests, control, rebind) in the step.
    pub task_polls: usize,
    /// Polls of the IO's own tasks in its busiest turn. The `LocalSet` runs
    /// at most 61 task polls a turn, capnp's `RpcSystem` among them.
    pub most_task_polls_in_a_turn: usize,
}

pub struct RpcIo {
    shared: Rc<Shared>,
    max_in_flight_requests: usize,
    max_in_flight_fetches: usize,
    step_budget: Duration,
    queued: VecDeque<Queued>,
    last_step: StepStats,
    queued_fetches: VecDeque<Queued>,
    /// The last basis `begin_sweep` returned, for a sweep begun while no
    /// connection is bound (`IoBasis::Disconnected` before the first).
    last_basis: IoBasis,
    // Dropped after everything above, the tasks before the runtime.
    local: LocalSet,
    runtime: tokio::runtime::Runtime,
}

impl RpcIo {
    pub fn connect(address: SocketAddr, request: ConnectRequest) -> Result<Self, RpcIoInitError> {
        Self::connect_with_config(address, request, RpcIoConfig::default())
    }

    /// Open the initial connection, waiting at most `connect_timeout`. This
    /// is the only call that waits on the daemon.
    pub fn connect_with_config(
        address: SocketAddr,
        request: ConnectRequest,
        config: RpcIoConfig,
    ) -> Result<Self, RpcIoInitError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| RpcIoInitError::Unavailable(error.to_string()))?;
        let local = LocalSet::new();
        let connect_timeout = config.connect_timeout;
        let opened = runtime.block_on(local.run_until(async {
            tokio::time::timeout(
                connect_timeout,
                open_connection(address, &request, Vec::new(), Vec::new()),
            )
            .await
        }));
        let opened = match opened {
            Ok(Ok(opened)) => opened,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(RpcIoInitError::Unavailable(format!(
                    "the daemon at {address} did not answer within {} ms",
                    connect_timeout.as_millis()
                )))
            }
        };
        let shared = Rc::new(Shared::new(address, request.target, &config));
        let basis = {
            let _local = local.enter();
            shared.commit(opened)
        };
        Ok(Self {
            shared,
            max_in_flight_requests: config.max_in_flight_requests.max(1),
            max_in_flight_fetches: config.max_in_flight_fetches.max(1),
            step_budget: config.step_budget,
            queued: VecDeque::new(),
            last_step: StepStats::default(),
            queued_fetches: VecDeque::new(),
            last_basis: basis,
            local,
            runtime,
        })
    }

    /// Start without a connection: the first [`LoaderIO::bind_target`]
    /// starts connecting in the background and retries until a connection
    /// binds (module docs, "Without a connection"). Never waits on the
    /// daemon; fails only when the IO's runtime cannot be built.
    pub fn connect_in_background(
        address: SocketAddr,
        request: ConnectRequest,
    ) -> Result<Self, RpcIoInitError> {
        Self::connect_in_background_with_config(address, request, RpcIoConfig::default())
    }

    pub fn connect_in_background_with_config(
        address: SocketAddr,
        request: ConnectRequest,
        config: RpcIoConfig,
    ) -> Result<Self, RpcIoInitError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| RpcIoInitError::Unavailable(error.to_string()))?;
        Ok(Self {
            shared: Rc::new(Shared::new(address, request.target, &config)),
            max_in_flight_requests: config.max_in_flight_requests.max(1),
            max_in_flight_fetches: config.max_in_flight_fetches.max(1),
            step_budget: config.step_budget,
            queued: VecDeque::new(),
            last_step: StepStats::default(),
            queued_fetches: VecDeque::new(),
            last_basis: IoBasis::Disconnected,
            local: LocalSet::new(),
            runtime,
        })
    }

    /// Whether a connection is bound, and the last connection error.
    pub fn status(&self) -> RpcIoStatus {
        RpcIoStatus {
            address: self.shared.address,
            connected: self.shared.connection().is_some(),
            last_error: self.shared.last_error.borrow().clone(),
        }
    }

    /// The daemon's current watched-import failures when they changed since
    /// the last call; `None` when unchanged or not yet polled. Each names a
    /// bundle still serving its last good contents.
    pub fn take_import_failures(&mut self) -> Option<Vec<ImportFailure>> {
        if !self.shared.import_failures_changed.replace(false) {
            return None;
        }
        self.shared.import_failures.borrow().clone()
    }

    /// Ask the daemon to describe a game type (§17 "Type descriptions") at
    /// its current version, by path (through the type index) or UUID. The
    /// call runs as the IO steps; its answer is taken with
    /// [`Self::take_type_description`] under the returned ticket.
    pub fn describe_type(&mut self, name: TypeName) -> u64 {
        let ticket = self.shared.next_description.get();
        self.shared.next_description.set(ticket + 1);
        let Some(connection) = self.shared.connection() else {
            self.shared
                .descriptions
                .borrow_mut()
                .insert(ticket, Err("not connected to the daemon".to_owned()));
            return ticket;
        };
        let shared = Rc::clone(&self.shared);
        let _local = self.local.enter();
        drop(self.shared.spawn(async move {
            let answer = describe(&connection.hub, name).await;
            shared.descriptions.borrow_mut().insert(ticket, answer);
        }));
        ticket
    }

    /// The answer to [`Self::describe_type`]'s `ticket`, once it arrived.
    pub fn take_type_description(&mut self, ticket: u64) -> Option<Result<TypeDescription, String>> {
        self.shared.descriptions.borrow_mut().remove(&ticket)
    }

    /// Run an authoring call (`crate::authoring`) on the current
    /// connection; its answer is taken with [`Self::take_authoring`] under
    /// the returned ticket.
    pub fn authoring(&mut self, request: AuthoringRequest) -> u64 {
        let ticket = self.shared.next_description.get();
        self.shared.next_description.set(ticket + 1);
        let Some(connection) = self.shared.connection() else {
            self.shared
                .authoring_answers
                .borrow_mut()
                .insert(ticket, Err("not connected to the daemon".to_owned()));
            return ticket;
        };
        let shared = Rc::clone(&self.shared);
        let _local = self.local.enter();
        drop(self.shared.spawn(async move {
            let answer = crate::authoring::run(&connection.hub, request).await;
            shared.authoring_answers.borrow_mut().insert(ticket, answer);
        }));
        ticket
    }

    /// The answer to [`Self::authoring`]'s `ticket`, once it arrived.
    pub fn take_authoring(&mut self, ticket: u64) -> Option<Result<AuthoringAnswer, String>> {
        self.shared.authoring_answers.borrow_mut().remove(&ticket)
    }

    /// Subscribe this connection's index (`Hub.subscribeIndex`, protocol
    /// 30): every published version reaches [`Self::take_index_changes`]
    /// whole, and the loader's own deltas are taken from the same stream.
    /// Kept across reconnects; each new connection reports one resync.
    pub fn subscribe_index(&mut self) {
        if !std::mem::replace(&mut self.shared.subscriptions.borrow_mut().index, true) {
            let _local = self.local.enter();
            self.shared.subscribe_index();
        }
    }

    /// Index changes since the last call, in stream order.
    pub fn take_index_changes(&mut self) -> Vec<IndexChange> {
        std::mem::take(&mut *self.shared.index_changes.borrow_mut())
    }

    /// What the last [`Self::step`] (or `poll`) did.
    pub fn last_step(&self) -> StepStats {
        self.last_step
    }

    pub fn stats(&self) -> RpcIoStats {
        let shared = &self.shared;
        RpcIoStats {
            queued_requests: self.queued.len() + self.queued_fetches.len(),
            in_flight_requests: shared.in_flight.borrow().len(),
            in_flight_fetches: shared.in_flight_fetches.get(),
            undelivered_events: shared.completions.borrow().len(),
            waiting_fetches: shared.admission.waiting(),
            resident_fetch_bytes: shared.admission.resident(),
            held_snapshots: shared.snapshots.borrow().held.len(),
            control_tasks: shared
                .control
                .borrow()
                .iter()
                .filter(|task| !task.is_finished())
                .count(),
            received_bytes: shared.received_bytes.get(),
            received_chunks: shared.received_chunks.get(),
        }
    }

    /// Run ready IO without blocking: turns until nothing is ready or the
    /// step budget is spent. [`LoaderIO::poll`] calls this.
    pub fn step(&mut self) {
        let started = Instant::now();
        self.last_step = StepStats::default();
        self.admit();
        loop {
            let polls = self.shared.task_polls.get();
            let woken = self.turn();
            let polled = self.shared.task_polls.get() - polls;
            self.last_step.turns += 1;
            self.last_step.task_polls += polled;
            self.last_step.most_task_polls_in_a_turn =
                self.last_step.most_task_polls_in_a_turn.max(polled);
            if started.elapsed() >= self.step_budget {
                break;
            }
            let admitted = self.admit();
            if !woken && admitted == 0 {
                break;
            }
        }
    }

    /// One non-blocking pass over the runtime (module docs, "Stepping"):
    /// true when a `LocalSet` task was woken, i.e. more work is ready.
    fn turn(&mut self) -> bool {
        let Self { runtime, local, .. } = self;
        let woken = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&woken);
        runtime.block_on(async move {
            std::future::poll_fn(|cx| {
                let waker = Waker::from(Arc::new(TurnWaker {
                    woken: Arc::clone(&flag),
                    inner: cx.waker().clone(),
                }));
                let _ = Pin::new(&mut *local).poll(&mut Context::from_waker(&waker));
                Poll::Ready(())
            })
            .await;
            tokio::task::yield_now().await;
        });
        woken.load(Ordering::Relaxed)
    }

    /// Start queued requests while admission allows; how many started.
    fn admit(&mut self) -> usize {
        let _local = self.local.enter();
        let mut started = 0;
        loop {
            if self.shared.in_flight.borrow().len() >= self.max_in_flight_requests {
                break;
            }
            let next = if self.shared.in_flight_fetches.get() < self.max_in_flight_fetches
                && !self.queued_fetches.is_empty()
            {
                self.queued_fetches.pop_front()
            } else {
                self.queued.pop_front()
            };
            let Some(queued) = next else {
                break;
            };
            self.shared.start(queued);
            started += 1;
        }
        started
    }

    fn enqueue(&mut self, req: ReqId, basis: &IoBasis, kind: RequestKind) {
        if self.shared.connection().is_none() {
            let event = self.shared.unreachable_answer(req, basis.clone(), kind);
            self.shared.push_request(basis.rpc_snapshot(), event, None);
            return;
        }
        let held = match basis {
            IoBasis::Rpc { snapshot } => self
                .shared
                .snapshots
                .borrow()
                .held
                .contains_key(snapshot)
                .then_some(*snapshot),
            _ => None,
        };
        let Some(stamp) = held else {
            self.shared.push_request(
                basis.rpc_snapshot(),
                request_error(req, basis.clone(), "stale RPC basis".into()),
                None,
            );
            return;
        };
        let queued = Queued { req, stamp, kind };
        if matches!(queued.kind, RequestKind::Fetch(_)) {
            self.queued_fetches.push_back(queued);
        } else {
            self.queued.push_back(queued);
        }
    }

    /// Drop everything of the current connection: queued and running
    /// requests, undelivered events, stream and control tasks, snapshots,
    /// and a pending rebind. Aborted tasks are dropped at the next turn.
    fn discard(&mut self) {
        self.queued.clear();
        self.queued_fetches.clear();
        let shared = &self.shared;
        let mut aborts = shared
            .in_flight
            .borrow()
            .values()
            .map(|in_flight| in_flight.abort.clone())
            .collect::<Vec<_>>();
        aborts.append(&mut shared.control.borrow_mut());
        let link = shared.link.replace(Link::Closed);
        if let Link::Rebinding { task, .. } = &link {
            aborts.push(task.clone());
        }
        for abort in aborts {
            abort.abort();
        }
        let completions = std::mem::take(&mut *shared.completions.borrow_mut());
        let snapshots = shared.snapshots.replace(Snapshots::default());
        drop((link, completions, snapshots));
    }
}

impl Drop for RpcIo {
    fn drop(&mut self) {
        // Nothing runs again: the LocalSet drops every task unpolled, which
        // closes the connection. No join, no wait on the daemon.
        self.discard();
    }
}

impl LoaderIO for RpcIo {
    fn bind_target(&mut self, target: RuntimeTarget) {
        // A rebind to the same target is already underway: keep its backoff
        // (several requests can report one lost connection).
        if matches!(&*self.shared.link.borrow(), Link::Rebinding { target: pending, .. } if *pending == target)
        {
            return;
        }
        self.discard();
        let _local = self.local.enter();
        let task = self
            .shared
            .spawn(rebind(Rc::clone(&self.shared), target.clone()));
        *self.shared.link.borrow_mut() = Link::Rebinding {
            target,
            task: task.abort_handle(),
        };
    }

    fn begin_sweep(&mut self) -> IoBasis {
        let mut snapshots = self.shared.snapshots.borrow_mut();
        if let Some(stamp) = snapshots.current {
            snapshots.sweeps.insert(stamp);
            self.last_basis = IoBasis::Rpc { snapshot: stamp };
        }
        self.last_basis.clone()
    }

    fn end_sweep(&mut self, basis: &IoBasis) {
        let Some(stamp) = basis.rpc_snapshot() else {
            return;
        };
        let released = {
            let mut snapshots = self.shared.snapshots.borrow_mut();
            snapshots.sweeps.remove(&stamp);
            snapshots.release_unused(stamp)
        };
        self.queued.retain(|queued| queued.stamp != stamp);
        self.queued_fetches.retain(|queued| queued.stamp != stamp);
        let aborts = self
            .shared
            .in_flight
            .borrow()
            .values()
            .filter(|in_flight| in_flight.stamp == stamp)
            .map(|in_flight| in_flight.abort.clone())
            .collect::<Vec<_>>();
        for abort in aborts {
            abort.abort();
        }
        let purged = {
            let mut completions = self.shared.completions.borrow_mut();
            let (purged, kept) = std::mem::take(&mut *completions)
                .into_iter()
                .partition::<VecDeque<_>, _>(|completion| completion.request == Some(stamp));
            *completions = kept;
            purged
        };
        drop((released, purged));
    }

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        self.enqueue(req, basis, RequestKind::Resolve(uuid));
    }

    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis) {
        self.enqueue(req, basis, RequestKind::Fetch(content_hash));
    }

    fn resolve_path(&mut self, req: ReqId, path: &AssetPath, basis: &IoBasis) {
        self.enqueue(req, basis, RequestKind::Path(path.to_owned()));
    }

    fn subscribe(&mut self, uuid: AssetUuid) {
        let mut subscriptions = self.shared.subscriptions.borrow_mut();
        if subscriptions.assets.insert(uuid) {
            drop(subscriptions);
            let _local = self.local.enter();
            self.shared.subscribe(vec![uuid], Vec::new());
        }
    }

    fn unsubscribe(&mut self, uuid: AssetUuid) {
        let mut subscriptions = self.shared.subscriptions.borrow_mut();
        if subscriptions.assets.remove(&uuid) {
            drop(subscriptions);
            let _local = self.local.enter();
            self.shared.unsubscribe(vec![uuid], Vec::new());
        }
    }

    fn subscribe_path(&mut self, path: &str) {
        if self
            .shared
            .subscriptions
            .borrow_mut()
            .paths
            .insert(path.to_owned())
        {
            let _local = self.local.enter();
            self.shared.subscribe(Vec::new(), vec![path.to_owned()]);
        }
    }

    fn unsubscribe_path(&mut self, path: &str) {
        if self.shared.subscriptions.borrow_mut().paths.remove(path) {
            let _local = self.local.enter();
            self.shared.unsubscribe(Vec::new(), vec![path.to_owned()]);
        }
    }

    fn poll(&mut self) -> Vec<IoEvent> {
        self.step();
        let completions = std::mem::take(&mut *self.shared.completions.borrow_mut());
        // Each payload's memory reservation ends here (the loader owns it);
        // the fetches it admits run at the next step.
        completions
            .into_iter()
            .map(|completion| completion.event)
            .collect()
    }
}

/// Records that the `LocalSet` was woken during a turn.
struct TurnWaker {
    woken: Arc<AtomicBool>,
    inner: Waker,
}

impl Wake for TurnWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Relaxed);
        self.inner.wake_by_ref();
    }
}

/// A type to describe ([`RpcIo::describe_type`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeName {
    /// As prefabs name it (`game::Spinner`).
    Path(String),
    Uuid(TypeUuid),
}

pub(crate) fn remote<T>(call: Result<RemoteCall<T>, impl std::fmt::Display>) -> Result<T, String> {
    match call {
        Ok(RemoteCall::Success(value)) => Ok(value),
        Ok(RemoteCall::ReconnectRequired(reason)) => Err(format!("reconnect required: {reason:?}")),
        Ok(RemoteCall::ConfigurationFailed(error)) => Err(format!("configuration failed: {error:?}")),
        Ok(RemoteCall::SnapshotExpired) => Err("snapshot expired; ask again".to_owned()),
        Ok(RemoteCall::Error(error)) => Err(format!("{error:?}")),
        Err(error) => Err(error.to_string()),
    }
}

/// One authoring snapshot: the type index (for a path), then the type.
pub(crate) async fn describe(hub: &RemoteHub, name: TypeName) -> Result<TypeDescription, String> {
    let snapshot = remote(hub.authoring_snapshot().await)?;
    let type_uuid = match name {
        TypeName::Uuid(type_uuid) => type_uuid,
        TypeName::Path(path) => match remote(snapshot.describable_types().await)? {
            DescribableTypesResult::Types { types, .. } => types
                .into_iter()
                .find(|described| described.path == path)
                .map(|described| described.type_uuid)
                .ok_or_else(|| format!("no describable type {path}"))?,
            DescribableTypesResult::Drifted { input, .. } => {
                return Err(format!("{input:?} changed while it was read; ask again"))
            }
        },
    };
    match remote(snapshot.describe_type(type_uuid).await)? {
        DescribeTypeResult::Description(description) => Ok(description),
        DescribeTypeResult::Missing => Err(format!("no description of type {type_uuid}")),
        DescribeTypeResult::Drifted { input, .. } => Err(format!("{input:?} changed while it was read; ask again")),
    }
}

/// State shared by `RpcIo` and its tasks. Every borrow is short and never
/// held across an `.await` or a turn.
struct Shared {
    address: SocketAddr,
    target_name: String,
    admission: FetchAdmission,
    target_rejection_after: u32,
    snapshot_refresh_after: Duration,
    completions: RefCell<VecDeque<Completion>>,
    in_flight: RefCell<HashMap<ReqId, InFlight>>,
    in_flight_fetches: Cell<usize>,
    snapshots: RefCell<Snapshots>,
    link: RefCell<Link>,
    subscriptions: RefCell<Subscriptions>,
    /// Stream, subscription, and maintenance tasks of the current
    /// connection.
    control: RefCell<Vec<AbortHandle>>,
    import_failures: RefCell<Option<Vec<ImportFailure>>>,
    import_failures_changed: Cell<bool>,
    /// Answers of `RpcIo::describe_type` by ticket, until taken.
    descriptions: RefCell<HashMap<u64, Result<TypeDescription, String>>>,
    /// Answers of `RpcIo::authoring` by ticket, until taken.
    authoring_answers: RefCell<HashMap<u64, Result<AuthoringAnswer, String>>>,
    /// Index changes, until taken.
    index_changes: RefCell<Vec<IndexChange>>,
    next_description: Cell<u64>,
    /// Signalled whenever a snapshot refresh ends.
    refreshed: tokio::sync::Notify,
    next_serial: Cell<u64>,
    /// Polls of the tasks this IO spawned, ever (see [`StepStats`]).
    task_polls: Rc<Cell<usize>>,
    /// Artifact payload bytes and chunks received, ever.
    received_bytes: Cell<u64>,
    received_chunks: Cell<u64>,
    /// Assets and paths whose last answer named them (anything but
    /// `Missing`/`Deleted`); without a connection they fail instead of
    /// answering `Missing` (module docs, "Without a connection").
    named_assets: RefCell<HashSet<AssetUuid>>,
    named_paths: RefCell<HashSet<AssetPath>>,
    last_error: RefCell<Option<String>>,
}

enum Link {
    Connected(Rc<Connection>),
    Rebinding {
        target: RuntimeTarget,
        task: AbortHandle,
    },
    Closed,
}

struct Connection {
    _client: CapnpClient,
    hub: RemoteHub,
    /// At most one `ReconnectRequired` per connection reaches the loader.
    reconnect_reported: Cell<bool>,
    refreshing: Cell<bool>,
    stream_installed: Cell<bool>,
}

#[derive(Default)]
struct Snapshots {
    /// The newest snapshot held: what `begin_sweep` returns.
    current: Option<SnapshotStamp>,
    held: HashMap<SnapshotStamp, Held>,
    /// Bases of sweeps begun and not yet ended.
    sweeps: HashSet<SnapshotStamp>,
}

struct Held {
    snapshot: RemoteSnapshot,
    serial: u64,
    opened: Instant,
}

impl Snapshots {
    /// Hold `snapshot`; it becomes current unless it is older than current.
    fn install(&mut self, snapshot: RemoteSnapshot, serial: u64) -> Option<Held> {
        let stamp = snapshot.basis().snapshot;
        if let Some(current) = self.current {
            if current.instance == stamp.instance && stamp.version < current.version {
                return None;
            }
        }
        let replaced = self.held.insert(
            stamp,
            Held {
                snapshot,
                serial,
                opened: Instant::now(),
            },
        );
        match self.current.replace(stamp) {
            Some(previous) if previous != stamp => self.release_unused(previous),
            _ => replaced,
        }
    }

    /// Drop `stamp`'s snapshot unless it is current or a live sweep's.
    fn release_unused(&mut self, stamp: SnapshotStamp) -> Option<Held> {
        if self.current == Some(stamp) || self.sweeps.contains(&stamp) {
            return None;
        }
        self.held.remove(&stamp)
    }

    fn satisfies(&self, need: &Need) -> bool {
        let Some(current) = self
            .current
            .and_then(|stamp| Some((stamp, self.held.get(&stamp)?)))
        else {
            return false;
        };
        match need {
            Need::AtLeast(stamp) => {
                current.0.instance == stamp.instance && current.0.version >= stamp.version
            }
            Need::Replace(serial) => current.1.serial != *serial,
        }
    }

    /// The cursor a new subscription names: no later than any basis the
    /// loader may still be resolving at, so no delta after it is missed.
    fn subscription_cursor(&self) -> InputVersion {
        let Some(current) = self.current else {
            return InputVersion(0);
        };
        self.sweeps
            .iter()
            .filter(|stamp| stamp.instance == current.instance)
            .map(|stamp| stamp.version)
            .chain([current.version])
            .min()
            .unwrap_or(current.version)
    }
}

/// What the current snapshot must satisfy before an event is published.
enum Need {
    /// At least this version: a delta or drift reported it.
    AtLeast(SnapshotStamp),
    /// Any capability but this one: it expired.
    Replace(u64),
}

#[derive(Default)]
struct Subscriptions {
    /// The loader's.
    assets: BTreeSet<AssetUuid>,
    paths: BTreeSet<String>,
    /// The connection's index is subscribed (`RpcIo::subscribe_index`):
    /// the stream carries every delta, the loader's filtered from them.
    index: bool,
}

impl Subscriptions {
    /// What the connection subscribes: the loader's.
    fn lists(&self) -> (Vec<AssetUuid>, Vec<String>) {
        (self.assets.iter().copied().collect(), self.paths.iter().cloned().collect())
    }
}

struct Queued {
    req: ReqId,
    stamp: SnapshotStamp,
    kind: RequestKind,
}

enum RequestKind {
    Resolve(AssetUuid),
    Path(AssetPath),
    Fetch(ContentHash),
}

struct InFlight {
    stamp: SnapshotStamp,
    abort: AbortHandle,
}

/// One event for the next `poll`.
struct Completion {
    event: IoEvent,
    /// The basis a request-terminal event's request was issued under.
    request: Option<SnapshotStamp>,
    _reservation: Option<Reservation>,
}

/// Removes its request from the in-flight table when the task ends,
/// whether it completed or was dropped.
struct InFlightGuard {
    shared: Rc<Shared>,
    req: ReqId,
    fetch: bool,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.shared.in_flight.borrow_mut().remove(&self.req);
        if self.fetch {
            let fetches = &self.shared.in_flight_fetches;
            fetches.set(fetches.get().saturating_sub(1));
        }
    }
}

/// Marks a snapshot refresh in flight on a connection; waiters re-check
/// when it ends, however it ends.
struct Refreshing<'a> {
    shared: &'a Shared,
    connection: &'a Connection,
}

impl Drop for Refreshing<'_> {
    fn drop(&mut self) {
        self.connection.refreshing.set(false);
        self.shared.refreshed.notify_waiters();
    }
}

struct Opened {
    client: CapnpClient,
    hub: RemoteHub,
    snapshot: RemoteSnapshot,
    subscription: Option<RemoteSubscription>,
    subscribed: (Vec<AssetUuid>, Vec<String>),
}

impl Shared {
    fn new(address: SocketAddr, target_name: String, config: &RpcIoConfig) -> Self {
        Self {
            address,
            target_name,
            admission: FetchAdmission::new(config.fetch_memory_budget),
            target_rejection_after: config.target_rejection_after.max(1),
            snapshot_refresh_after: config.snapshot_refresh_after,
            completions: RefCell::new(VecDeque::new()),
            in_flight: RefCell::new(HashMap::new()),
            in_flight_fetches: Cell::new(0),
            snapshots: RefCell::new(Snapshots::default()),
            link: RefCell::new(Link::Closed),
            subscriptions: RefCell::new(Subscriptions::default()),
            control: RefCell::new(Vec::new()),
            import_failures: RefCell::new(None),
            import_failures_changed: Cell::new(false),
            descriptions: RefCell::new(HashMap::new()),
            authoring_answers: RefCell::new(HashMap::new()),
            index_changes: RefCell::new(Vec::new()),
            next_description: Cell::new(1),
            refreshed: tokio::sync::Notify::new(),
            next_serial: Cell::new(1),
            task_polls: Rc::new(Cell::new(0)),
            received_bytes: Cell::new(0),
            received_chunks: Cell::new(0),
            named_assets: RefCell::new(HashSet::new()),
            named_paths: RefCell::new(HashSet::new()),
            last_error: RefCell::new(None),
        }
    }

    /// The answer to a request made while no connection is bound.
    fn unreachable_answer(&self, req: ReqId, basis: IoBasis, kind: RequestKind) -> IoEvent {
        match kind {
            RequestKind::Resolve(uuid) if !self.named_assets.borrow().contains(&uuid) => {
                IoEvent::Resolved {
                    req,
                    uuid,
                    result: ResolveResult::Missing,
                    basis,
                }
            }
            RequestKind::Path(path) if !self.named_paths.borrow().contains(&path) => {
                IoEvent::PathResolved {
                    req,
                    path,
                    result: PathResolveResult::Missing,
                    basis,
                }
            }
            _ => request_error(
                req,
                basis,
                format!("the daemon at {} is unreachable", self.address),
            ),
        }
    }

    /// Remember whether an answer named its asset or path.
    fn note_named(&self, event: &IoEvent) {
        match event {
            IoEvent::Resolved { uuid, result, .. } => {
                let mut named = self.named_assets.borrow_mut();
                if matches!(result, ResolveResult::Missing | ResolveResult::Deleted { .. }) {
                    named.remove(uuid);
                } else {
                    named.insert(*uuid);
                }
            }
            IoEvent::PathResolved { path, result, .. } => {
                let mut named = self.named_paths.borrow_mut();
                if matches!(result, PathResolveResult::Missing) {
                    named.remove(path);
                } else {
                    named.insert(path.clone());
                }
            }
            _ => {}
        }
    }

    /// Spawn `task` on the `LocalSet`, counting its polls.
    fn spawn<T: 'static>(
        &self,
        task: impl Future<Output = T> + 'static,
    ) -> tokio::task::JoinHandle<T> {
        tokio::task::spawn_local(Counted {
            polls: Rc::clone(&self.task_polls),
            task: Box::pin(task),
        })
    }

    /// The bound connection, unless it reported `ReconnectRequired`: a lost
    /// or fenced connection serves nothing until the rebind.
    fn connection(&self) -> Option<Rc<Connection>> {
        match &*self.link.borrow() {
            Link::Connected(connection) if !connection.reconnect_reported.get() => {
                Some(Rc::clone(connection))
            }
            _ => None,
        }
    }

    fn push(&self, event: IoEvent) {
        self.completions.borrow_mut().push_back(Completion {
            event,
            request: None,
            _reservation: None,
        });
    }

    fn push_request(
        &self,
        request: Option<SnapshotStamp>,
        event: IoEvent,
        reservation: Option<Reservation>,
    ) {
        self.completions.borrow_mut().push_back(Completion {
            event,
            request,
            _reservation: reservation,
        });
    }

    /// Connection-level events: `ReconnectRequired` once per connection.
    fn push_connection(&self, connection: &Connection, event: IoEvent) {
        if let IoEvent::ReconnectRequired { reason } = &event {
            if connection.reconnect_reported.replace(true) {
                return;
            }
            if *reason == ReconnectReason::ConnectionLost {
                *self.last_error.borrow_mut() =
                    Some(format!("lost the connection to the daemon at {}", self.address));
            }
        }
        self.push(event);
    }

    fn install(&self, snapshot: RemoteSnapshot) {
        let serial = self.next_serial.get();
        self.next_serial.set(serial + 1);
        let released = self.snapshots.borrow_mut().install(snapshot, serial);
        drop(released);
    }

    fn spawn_control(&self, task: impl Future<Output = ()> + 'static) {
        let handle = self.spawn(task).abort_handle();
        let mut control = self.control.borrow_mut();
        control.retain(|task| !task.is_finished());
        control.push(handle);
    }

    /// Make `opened` the connection: its snapshot becomes current, its
    /// stream and maintenance start, and subscriptions changed since it
    /// subscribed are reconciled. Must run inside the `LocalSet`.
    fn commit(self: &Rc<Self>, opened: Opened) -> IoBasis {
        let basis = io_basis(opened.snapshot.basis());
        let connection = Rc::new(Connection {
            _client: opened.client,
            hub: opened.hub,
            reconnect_reported: Cell::new(false),
            refreshing: Cell::new(false),
            stream_installed: Cell::new(false),
        });
        *self.link.borrow_mut() = Link::Connected(Rc::clone(&connection));
        *self.last_error.borrow_mut() = None;
        self.install(opened.snapshot);
        if let Some(subscription) = opened.subscription {
            connection.stream_installed.set(true);
            self.spawn_control(delta_stream(
                Rc::clone(self),
                Rc::clone(&connection),
                subscription,
            ));
        }
        let (assets, paths) = self.subscriptions.borrow().lists();
        let (subscribed_assets, subscribed_paths) = opened.subscribed;
        let added_assets = assets
            .iter()
            .filter(|asset| !subscribed_assets.contains(asset))
            .copied()
            .collect::<Vec<_>>();
        let added_paths = paths
            .iter()
            .filter(|path| !subscribed_paths.contains(path))
            .cloned()
            .collect::<Vec<_>>();
        let removed_assets = subscribed_assets
            .into_iter()
            .filter(|asset| !assets.contains(asset))
            .collect::<Vec<_>>();
        let removed_paths = subscribed_paths
            .into_iter()
            .filter(|path| !paths.contains(path))
            .collect::<Vec<_>>();
        if !added_assets.is_empty() || !added_paths.is_empty() {
            self.subscribe(added_assets, added_paths);
        }
        if !removed_assets.is_empty() || !removed_paths.is_empty() {
            self.unsubscribe(removed_assets, removed_paths);
        }
        if self.subscriptions.borrow().index {
            self.subscribe_index();
        }
        self.spawn_control(maintain(Rc::clone(self), connection));
        basis
    }

    /// Start a queued request as a task. Must run inside the `LocalSet`.
    fn start(self: &Rc<Self>, queued: Queued) {
        let Queued { req, stamp, kind } = queued;
        let basis = IoBasis::Rpc { snapshot: stamp };
        let held = self
            .snapshots
            .borrow()
            .held
            .get(&stamp)
            .map(|held| (held.snapshot.clone(), held.serial));
        let (Some(connection), Some((snapshot, serial))) = (self.connection(), held) else {
            self.push_request(
                Some(stamp),
                request_error(req, basis, "stale RPC basis".into()),
                None,
            );
            return;
        };
        let fetch = matches!(kind, RequestKind::Fetch(_));
        let guard = InFlightGuard {
            shared: Rc::clone(self),
            req,
            fetch,
        };
        let task = self.spawn(run_request(
            Rc::clone(self),
            connection,
            snapshot,
            serial,
            req,
            stamp,
            kind,
            guard,
        ));
        if fetch {
            self.in_flight_fetches.set(self.in_flight_fetches.get() + 1);
        }
        self.in_flight.borrow_mut().insert(
            req,
            InFlight {
                stamp,
                abort: task.abort_handle(),
            },
        );
    }

    /// Subscribe on the current connection; with none, the next connection
    /// subscribes the whole set. Must run inside the `LocalSet`.
    fn subscribe(self: &Rc<Self>, assets: Vec<AssetUuid>, paths: Vec<String>) {
        let Some(connection) = self.connection() else {
            return;
        };
        let since = self.snapshots.borrow().subscription_cursor();
        let shared = Rc::clone(self);
        self.spawn_control(async move {
            match connection.hub.subscribe(since, assets, paths).await {
                Ok(RemoteCall::Success(subscription)) => {
                    // The connection carries one stream for every subscription.
                    if !connection.stream_installed.replace(true) {
                        let stream =
                            delta_stream(Rc::clone(&shared), Rc::clone(&connection), subscription);
                        shared.spawn_control(stream);
                    }
                }
                Ok(call) => shared.push_connection(&connection, connection_event(call)),
                Err(_) => shared.push_connection(&connection, connection_lost()),
            }
        });
    }

    /// Subscribe the current connection's index; with none, the next
    /// connection does. Must run inside the `LocalSet`.
    fn subscribe_index(self: &Rc<Self>) {
        let Some(connection) = self.connection() else {
            return;
        };
        let since = self.snapshots.borrow().subscription_cursor();
        let shared = Rc::clone(self);
        self.spawn_control(async move {
            match connection.hub.subscribe_index(since).await {
                Ok(RemoteCall::Success(subscription)) => {
                    if !connection.stream_installed.replace(true) {
                        let stream =
                            delta_stream(Rc::clone(&shared), Rc::clone(&connection), subscription);
                        shared.spawn_control(stream);
                    }
                }
                Ok(call) => shared.push_connection(&connection, connection_event(call)),
                Err(_) => shared.push_connection(&connection, connection_lost()),
            }
        });
    }

    fn unsubscribe(self: &Rc<Self>, assets: Vec<AssetUuid>, paths: Vec<String>) {
        let Some(connection) = self.connection() else {
            return;
        };
        let shared = Rc::clone(self);
        self.spawn_control(async move {
            match connection.hub.unsubscribe(assets, paths).await {
                Ok(RemoteCall::Success(())) => {}
                Ok(call) => shared.push_connection(&connection, connection_event(call)),
                Err(error) => shared.push(IoEvent::ConnectionError {
                    message: error.to_string(),
                }),
            }
        });
    }

    fn set_import_failures(&self, failures: Vec<ImportFailure>) {
        let mut current = self.import_failures.borrow_mut();
        if current.as_ref() != Some(&failures) {
            *current = Some(failures);
            self.import_failures_changed.set(true);
        }
    }
}

/// Make the current snapshot satisfy `need`, refreshing it when it does
/// not. Concurrent callers share one refresh per connection.
async fn ensure_current(
    shared: &Shared,
    connection: &Connection,
    need: Need,
) -> Result<(), IoEvent> {
    loop {
        let notified = shared.refreshed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if shared.snapshots.borrow().satisfies(&need) {
            return Ok(());
        }
        if !connection.refreshing.get() {
            break;
        }
        notified.await;
    }
    connection.refreshing.set(true);
    let _refreshing = Refreshing { shared, connection };
    match connection.hub.snapshot().await {
        Ok(RemoteCall::Success(snapshot)) => {
            shared.install(snapshot);
            Ok(())
        }
        Ok(call) => Err(connection_event(call)),
        Err(_) => Err(connection_lost()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_request(
    shared: Rc<Shared>,
    connection: Rc<Connection>,
    snapshot: RemoteSnapshot,
    serial: u64,
    req: ReqId,
    stamp: SnapshotStamp,
    kind: RequestKind,
    _guard: InFlightGuard,
) {
    let basis = IoBasis::Rpc { snapshot: stamp };
    let (event, reservation) = match kind {
        RequestKind::Resolve(uuid) => (resolve_event(&snapshot, req, uuid, basis).await, None),
        RequestKind::Path(path) => (path_event(&snapshot, req, path, basis).await, None),
        RequestKind::Fetch(content_hash) => {
            fetch_event(
                &shared,
                &connection.hub,
                &snapshot,
                req,
                content_hash,
                basis,
            )
            .await
        }
    };
    // The loader answers these with a new round: publish them only once the
    // basis that round will get answers them.
    let need = match &event {
        IoEvent::Resolved {
            result: ResolveResult::Drifted { current, .. },
            ..
        } => Some(Need::AtLeast(*current)),
        IoEvent::SnapshotExpired { .. } => Some(Need::Replace(serial)),
        _ => None,
    };
    if let Some(need) = need {
        if let Err(event) = ensure_current(&shared, &connection, need).await {
            shared.push_connection(&connection, event);
        }
    }
    match event {
        IoEvent::ReconnectRequired { .. }
        | IoEvent::ConnectionError { .. }
        | IoEvent::RestartRequired { .. } => shared.push_connection(&connection, event),
        event => {
            shared.note_named(&event);
            shared.push_request(Some(stamp), event, reservation)
        }
    }
}

/// Deltas in stream order, each published once the current snapshot is at
/// least as new as it.
async fn delta_stream(
    shared: Rc<Shared>,
    connection: Rc<Connection>,
    mut subscription: RemoteSubscription,
) {
    loop {
        match subscription.next().await {
            Ok(Some(event)) => {
                let (events, changes) = stream_events(event, &shared.subscriptions.borrow());
                shared.index_changes.borrow_mut().extend(changes);
                let newest = events
                    .iter()
                    .filter_map(|event| match event {
                        IoEvent::Delta { stamp, .. } => Some(*stamp),
                        _ => None,
                    })
                    .max_by_key(|stamp| stamp.version);
                if let Some(stamp) = newest {
                    if let Err(event) =
                        ensure_current(&shared, &connection, Need::AtLeast(stamp)).await
                    {
                        shared.push_connection(&connection, event);
                    }
                }
                for event in events {
                    shared.push_connection(&connection, event);
                }
            }
            Ok(None) | Err(_) => {
                shared.push_connection(&connection, connection_lost());
                return;
            }
        }
    }
}

/// Per connection: poll watched-import failures, named at the current
/// snapshot, and keep that snapshot well inside the daemon's TTL.
async fn maintain(shared: Rc<Shared>, connection: Rc<Connection>) {
    loop {
        let current = {
            let snapshots = shared.snapshots.borrow();
            snapshots
                .current
                .and_then(|stamp| snapshots.held.get(&stamp))
                .map(|held| held.snapshot.clone())
        };
        if let Some(snapshot) = current {
            if let Ok(RemoteCall::Success(failures)) = snapshot.import_failures().await {
                shared.set_import_failures(failures);
            }
        }
        let stale = shared.snapshots.borrow().current.and_then(|stamp| {
            let held = shared
                .snapshots
                .borrow()
                .held
                .get(&stamp)
                .map(|held| (held.serial, held.opened))?;
            (held.1.elapsed() >= shared.snapshot_refresh_after).then_some(held.0)
        });
        if let Some(serial) = stale {
            if let Err(event) = ensure_current(&shared, &connection, Need::Replace(serial)).await {
                shared.push_connection(&connection, event);
            }
        }
        tokio::time::sleep(IMPORT_FAILURE_POLL_INTERVAL).await;
    }
}

/// Rebind to `target`: attempt, back off, report after repeated failure,
/// and commit the first connection that binds.
async fn rebind(shared: Rc<Shared>, target: RuntimeTarget) {
    let mut backoff = RECONNECT_INITIAL_BACKOFF;
    let mut failures = 0u32;
    let mut reported = false;
    loop {
        let (assets, paths) = shared.subscriptions.borrow().lists();
        let request = connect_request(&shared.target_name, &target);
        let attempt = tokio::time::timeout(
            RECONNECT_ATTEMPT_TIMEOUT,
            open_connection(shared.address, &request, assets, paths),
        )
        .await;
        let message = match attempt {
            Ok(Ok(opened)) => {
                let basis = shared.commit(opened);
                // Changes while disconnected are unknown.
                let version = basis.rpc_snapshot().map_or(InputVersion(0), |stamp| stamp.version);
                if shared.subscriptions.borrow().index {
                    shared.index_changes.borrow_mut().push(IndexChange::resync(version));
                }
                shared.push(IoEvent::TargetBound { target, basis });
                return;
            }
            Ok(Err(error)) => error.message(),
            Err(_) => format!(
                "RPC reconnection attempt timed out after {} ms",
                RECONNECT_ATTEMPT_TIMEOUT.as_millis()
            ),
        };
        *shared.last_error.borrow_mut() = Some(message.clone());
        failures = failures.saturating_add(1);
        if !reported && failures >= shared.target_rejection_after {
            reported = true;
            shared.push(IoEvent::TargetRejected { message });
        }
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2).min(RECONNECT_MAX_BACKOFF);
    }
}

/// Connect, bind, open a snapshot, and subscribe `assets`/`paths` at it.
async fn open_connection(
    address: SocketAddr,
    request: &ConnectRequest,
    assets: Vec<AssetUuid>,
    paths: Vec<String>,
) -> Result<Opened, RpcIoInitError> {
    let unavailable =
        |error: &dyn std::fmt::Display| RpcIoInitError::Unavailable(error.to_string());
    let client = CapnpClient::connect_local(address)
        .await
        .map_err(|error| unavailable(&error))?;
    let outcome = client
        .connect(request)
        .await
        .map_err(|error| unavailable(&error))?;
    let hub = RemoteHub::connected(outcome).map_err(|outcome| {
        RpcIoInitError::Unavailable(match *outcome {
            RemoteConnectOutcome::ConfigurationFailed(error) => {
                format!("daemon configuration failed: {}", error.message)
            }
            other => format!("RPC connection rejected: {other:?}"),
        })
    })?;
    let snapshot = match hub.snapshot().await {
        Ok(RemoteCall::Success(snapshot)) => snapshot,
        Ok(call) => return Err(init_remote_failure(call)),
        Err(error) => return Err(unavailable(&error)),
    };
    let subscription = if assets.is_empty() && paths.is_empty() {
        None
    } else {
        match hub
            .subscribe(
                snapshot.basis().snapshot.version,
                assets.clone(),
                paths.clone(),
            )
            .await
        {
            Ok(RemoteCall::Success(subscription)) => Some(subscription),
            Ok(call) => return Err(init_remote_failure(call)),
            Err(error) => return Err(unavailable(&error)),
        }
    };
    Ok(Opened {
        client,
        hub,
        snapshot,
        subscription,
        subscribed: (assets, paths),
    })
}

async fn resolve_event(
    snapshot: &RemoteSnapshot,
    req: ReqId,
    uuid: AssetUuid,
    request_basis: IoBasis,
) -> IoEvent {
    match snapshot.resolve(uuid).await {
        Ok(RemoteCall::Success(terminal)) => IoEvent::Resolved {
            req,
            uuid,
            result: match terminal.value {
                distill_rpc::ResolveResult::Built { content_hash } => {
                    ResolveResult::Built { content_hash }
                }
                distill_rpc::ResolveResult::Drifted { input, current } => ResolveResult::Drifted {
                    input: drifted_input(input),
                    current,
                },
                distill_rpc::ResolveResult::Failed { error } => ResolveResult::Failed { error },
                distill_rpc::ResolveResult::Missing => ResolveResult::Missing,
                distill_rpc::ResolveResult::Deleted { at } => ResolveResult::Deleted { at },
                distill_rpc::ResolveResult::RoleIneligible { observed } => {
                    ResolveResult::RoleIneligible {
                        uuid,
                        role: entry_role(observed),
                    }
                }
            },
            basis: io_basis(&terminal.basis),
        },
        Ok(call) => remote_request_event(call, req, request_basis),
        Err(error) => request_error(req, request_basis, error.to_string()),
    }
}

async fn path_event(
    snapshot: &RemoteSnapshot,
    req: ReqId,
    path: AssetPath,
    request_basis: IoBasis,
) -> IoEvent {
    let call = match &path.name {
        None => snapshot.resolve_path(&path.path).await,
        Some(name) => snapshot.resolve_named(&path.path, name).await,
    };
    match call {
        Ok(RemoteCall::Success(terminal)) => IoEvent::PathResolved {
            req,
            path,
            result: match terminal.value {
                distill_rpc::PathResolveResult::Resolved(uuid) => PathResolveResult::Resolved(uuid),
                distill_rpc::PathResolveResult::Missing => PathResolveResult::Missing,
                distill_rpc::PathResolveResult::Failed(error) => PathResolveResult::Failed {
                    error: format!("{error:?}"),
                },
            },
            basis: io_basis(&terminal.basis),
        },
        Ok(call) => remote_request_event(call, req, request_basis),
        Err(error) => request_error(req, request_basis, error.to_string()),
    }
}

/// Fetch one artifact and its wire tree. The payload's size is reserved
/// before its stream is read, waiting unread while it does not fit; the
/// reservation grows by the wire tree once that arrives and travels with
/// the event until the loader takes it.
async fn fetch_event(
    shared: &Shared,
    hub: &RemoteHub,
    snapshot: &RemoteSnapshot,
    req: ReqId,
    content_hash: ContentHash,
    request_basis: IoBasis,
) -> (IoEvent, Option<Reservation>) {
    let fail = |message: String| (request_error(req, request_basis.clone(), message), None);
    let mut timing = crate::stats::FetchTiming {
        started: Some(Instant::now()),
        ..Default::default()
    };
    let mut terminal = match snapshot.fetch(content_hash).await {
        Ok(RemoteCall::Success(terminal)) => terminal,
        Ok(call) => return (remote_request_event(call, req, request_basis.clone()), None),
        Err(error) => return fail(error.to_string()),
    };
    let basis = io_basis(&terminal.basis);
    let load_edges = terminal.value.load_edges().to_vec();
    let Ok(total_bytes) = usize::try_from(terminal.value.total_bytes()) else {
        return fail("fetched artifact is too large for this client".into());
    };
    timing.answered = Some(Instant::now());
    let mut reservation = shared.admission.admit(total_bytes).await;
    timing.admitted = Some(Instant::now());
    let payload =
        match collect_remote_chunks(shared, &mut terminal.value, total_bytes, &mut timing).await {
            Ok(payload) => payload,
            Err(error) => return fail(error),
        };
    timing.received = Some(Instant::now());
    // The layout hash the artifact header declares. The loader
    // authenticates the artifact itself (content hash, structure) when it
    // parses it; hashing every byte here as well would double that cost on
    // the engine thread.
    let structural = &payload.bytes[payload.structural.clone()];
    let layout_hash = match distill_wire::artifact::artifact_header_layout_hash(structural) {
        Ok(layout_hash) => layout_hash,
        Err(error) => return fail(format!("invalid fetched artifact: {error}")),
    };
    let wire_layout = match hub.wire_tree(layout_hash).await {
        Ok(RemoteCall::Success(bytes)) => bytes,
        Ok(call) => return (remote_request_event(call, req, request_basis.clone()), None),
        Err(error) => return fail(error.to_string()),
    };
    reservation.grow(wire_layout.len());
    let wire_layout = memory_wire_blob(wire_layout);
    timing.completed = Some(Instant::now());
    match fetched_artifact(layout_hash, payload, load_edges, wire_layout, timing) {
        Ok(artifact) => (
            IoEvent::Fetched {
                req,
                content_hash,
                artifact,
                basis,
            },
            Some(reservation),
        ),
        Err(error) => fail(error),
    }
}

/// A fetched payload: every section in one buffer, sized from the
/// authenticated total before the first chunk, each section starting at a
/// [`SECTION_ALIGN`]-byte offset.
pub(crate) struct RemotePayload {
    pub(crate) bytes: Vec<u8>,
    pub(crate) structural: std::ops::Range<usize>,
    pub(crate) blobs: Vec<std::ops::Range<usize>>,
}

/// Section start alignment in a [`RemotePayload`]: what a separately
/// allocated section had from the allocator.
const SECTION_ALIGN: usize = 16;

/// The payload's chunks, collected into one buffer sized up front (no
/// growth reallocations); counts them into `shared`'s totals and `timing`
/// (chunks, bytes, copy time). The server sends the structural section,
/// then blob 0, 1, ... in order, each section's chunks contiguous.
async fn collect_remote_chunks(
    shared: &Shared,
    stream: &mut distill_rpc::capnp_loader::RemoteChunkStream,
    total_bytes: usize,
    timing: &mut crate::stats::FetchTiming,
) -> Result<RemotePayload, String> {
    // Room for the alignment padding of up to 64 sections; more sections
    // grow the buffer once.
    let mut bytes = Vec::<u8>::with_capacity(total_bytes.saturating_add(64 * SECTION_ALIGN));
    // [0] structural, [k + 1] blob k; the last one's end is `bytes.len()`.
    let mut sections = Vec::<std::ops::Range<usize>>::new();
    let mut total = 0usize;
    loop {
        match stream.next_chunk().await {
            Ok(Some(chunk)) => {
                let section = match chunk.kind {
                    distill_rpc::ArtifactChunkKind::Structural => 0usize,
                    distill_rpc::ArtifactChunkKind::Blob { index } => index as usize + 1,
                };
                if sections.is_empty() && section == 1 {
                    sections.push(0..0); // no structural chunk: an empty section
                }
                if section == sections.len() && chunk.offset == 0 {
                    if let Some(last) = sections.last_mut() {
                        last.end = bytes.len();
                    }
                    let start = bytes.len().next_multiple_of(SECTION_ALIGN);
                    bytes.resize(start, 0);
                    sections.push(start..start);
                } else if section + 1 != sections.len()
                    || chunk.offset != (bytes.len() - sections[section].start) as u64
                {
                    return Err("artifact chunks are not contiguous".into());
                }
                total = total
                    .checked_add(chunk.bytes.len())
                    .ok_or_else(|| "artifact stream length overflow".to_owned())?;
                if total > total_bytes {
                    return Err("artifact stream exceeds its authenticated total".into());
                }
                let copy_started = Instant::now();
                let len = chunk.bytes.len();
                bytes.reserve(len);
                let at = bytes.len();
                // Ordinary stores. Streaming stores were measured slower into
                // this fresh buffer (doc 22 §1.7, nt-copy); phase 2 measures
                // them on pooled, prefaulted buffers and adds them only if the
                // engine-side gain is real.
                // SAFETY: `reserve` made room for `len` bytes past `at`; the
                // copy initialises all of them before `set_len`.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        chunk.bytes.as_ptr(),
                        bytes.as_mut_ptr().add(at),
                        len,
                    );
                    bytes.set_len(at + len);
                }
                timing.copy_ns += copy_started.elapsed().as_nanos() as u64;
                timing.chunks += 1;
                timing.bytes += len as u64;
                shared.received_chunks.set(shared.received_chunks.get() + 1);
                shared
                    .received_bytes
                    .set(shared.received_bytes.get() + len as u64);
            }
            Ok(None) => {
                if total != total_bytes {
                    return Err(
                        "artifact stream length differs from its authenticated total".into(),
                    );
                }
                if let Some(last) = sections.last_mut() {
                    last.end = bytes.len();
                }
                let mut sections = sections.into_iter();
                return Ok(RemotePayload {
                    structural: sections.next().unwrap_or(0..0),
                    blobs: sections.collect(),
                    bytes,
                });
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

fn memory_wire_blob(bytes: std::sync::Arc<[u8]>) -> distill_wire::exec::Blob {
    let len = bytes.len();
    let backing: std::sync::Arc<dyn AsRef<[u8]> + Send + Sync> = std::sync::Arc::new(bytes);
    distill_wire::exec::Blob::new(backing, 0, len)
}

fn connection_lost() -> IoEvent {
    IoEvent::ReconnectRequired {
        reason: ReconnectReason::ConnectionLost,
    }
}

fn remote_request_event<T: std::fmt::Debug>(
    call: RemoteCall<T>,
    req: ReqId,
    basis: IoBasis,
) -> IoEvent {
    match call {
        RemoteCall::ReconnectRequired(reason) => IoEvent::ReconnectRequired {
            reason: reconnect_reason(reason),
        },
        RemoteCall::SnapshotExpired => IoEvent::SnapshotExpired { req, basis },
        // An artifact that left the CAS is a cache miss at this snapshot.
        RemoteCall::Error(RemoteError {
            failure: Some(RpcFailure::ArtifactNotFound { .. }),
            ..
        }) => IoEvent::SnapshotExpired { req, basis },
        RemoteCall::Error(RemoteError {
            failure: Some(RpcFailure::ConnectionClosed),
            ..
        }) => connection_lost(),
        other => request_error(req, basis, remote_message(other)),
    }
}

fn connection_event<T: std::fmt::Debug>(call: RemoteCall<T>) -> IoEvent {
    match call {
        RemoteCall::ReconnectRequired(reason) => IoEvent::ReconnectRequired {
            reason: reconnect_reason(reason),
        },
        RemoteCall::Error(RemoteError {
            failure: Some(RpcFailure::ConnectionClosed),
            ..
        }) => connection_lost(),
        other => IoEvent::ConnectionError {
            message: remote_message(other),
        },
    }
}

fn remote_message<T: std::fmt::Debug>(call: RemoteCall<T>) -> String {
    match call {
        RemoteCall::ConfigurationFailed(error) => {
            format!("daemon configuration failed: {}", error.message)
        }
        RemoteCall::SnapshotExpired => "the snapshot expired".to_owned(),
        RemoteCall::Error(error) => error.message,
        other => format!("unexpected RPC result: {other:?}"),
    }
}

fn request_error(req: ReqId, basis: IoBasis, message: String) -> IoEvent {
    IoEvent::RequestError {
        req,
        message,
        basis,
    }
}

fn init_remote_failure<T: std::fmt::Debug>(call: RemoteCall<T>) -> RpcIoInitError {
    match call {
        RemoteCall::ReconnectRequired(reason) => {
            RpcIoInitError::ReconnectRequired(reconnect_reason(reason))
        }
        other => RpcIoInitError::Unavailable(remote_message(other)),
    }
}

fn connect_request(target: &str, binding: &RuntimeTarget) -> ConnectRequest {
    ConnectRequest::new(
        target,
        distill_rpc::TargetDefinitionHash(binding.target_definition_hash),
    )
}

/// The loader's events of one stream event, and the index changes it
/// tells. With the index subscribed the stream carries every delta: the
/// loader's are the assets and paths it subscribed.
fn stream_events(event: StreamEvent, subscriptions: &Subscriptions) -> (Vec<IoEvent>, Vec<IndexChange>) {
    let mut changes = Vec::new();
    let mut delta = |delta: distill_rpc::Delta| {
        if subscriptions.index {
            changes.push(IndexChange {
                version: delta.basis.snapshot.version,
                entries: delta
                    .assets
                    .iter()
                    .map(|(asset, state)| (*asset, *state == distill_rpc::AssetDeltaState::Deleted))
                    .collect(),
                files: delta
                    .files
                    .iter()
                    .map(|file| {
                        let deleted = file.state == distill_rpc::AssetDeltaState::Deleted;
                        (file.root.clone(), file.path.clone(), deleted)
                    })
                    .collect(),
                types: delta.types,
                resync: false,
            });
        }
        let assets = delta
            .assets
            .into_iter()
            .filter(|(asset, _)| subscriptions.assets.contains(asset))
            .map(|(asset, state)| (asset, delta_state(state)))
            .collect::<Vec<_>>();
        let paths = delta
            .paths
            .into_iter()
            .filter(|path| subscriptions.paths.contains(path))
            .collect::<Vec<_>>();
        (!assets.is_empty() || !paths.is_empty()).then_some(IoEvent::Delta {
            stamp: delta.basis.snapshot,
            assets,
            paths,
        })
    };
    let events = match event {
        StreamEvent::InitialDelta { deltas, .. } => deltas.into_iter().filter_map(&mut delta).collect(),
        StreamEvent::Delta(value) => delta(value).into_iter().collect(),
        StreamEvent::ResyncRequired { basis, .. } => {
            if subscriptions.index {
                changes.push(IndexChange::resync(basis.snapshot.version));
            }
            vec![IoEvent::Delta {
                stamp: basis.snapshot,
                assets: subscriptions
                    .assets
                    .iter()
                    .copied()
                    .map(|asset| (asset, AssetDeltaState::Changed))
                    .collect(),
                paths: subscriptions.paths.iter().cloned().collect(),
            }]
        }
        StreamEvent::Asset { event, .. } => match event {
            AssetEvent::ReconnectRequired { reason } => vec![IoEvent::ReconnectRequired {
                reason: reconnect_reason(reason),
            }],
            AssetEvent::RestartRequired { keys } => vec![IoEvent::RestartRequired { keys }],
            AssetEvent::Error { message, .. } => vec![IoEvent::ConnectionError { message }],
            _ => Vec::new(),
        },
    };
    (events, changes)
}

fn reconnect_reason(reason: distill_rpc::ReconnectReason) -> ReconnectReason {
    match reason {
        distill_rpc::ReconnectReason::PipelineEpochChanged => ReconnectReason::PipelineEpochChanged,
    }
}

fn delta_state(state: distill_rpc::AssetDeltaState) -> AssetDeltaState {
    match state {
        distill_rpc::AssetDeltaState::Changed => AssetDeltaState::Changed,
        distill_rpc::AssetDeltaState::Deleted => AssetDeltaState::Deleted,
    }
}

fn entry_role(role: distill_rpc::AuthoringEntryRole) -> EntryRole {
    match role {
        distill_rpc::AuthoringEntryRole::Runtime => EntryRole::Runtime,
        distill_rpc::AuthoringEntryRole::AuthoringOnly => EntryRole::AuthoringOnly,
    }
}

fn drifted_input(input: RpcDriftedInput) -> DriftedInput {
    match input {
        RpcDriftedInput::File(path) => DriftedInput::File(path),
        RpcDriftedInput::Asset(asset) => DriftedInput::Asset(asset),
        RpcDriftedInput::Query(query) => DriftedInput::Query(query),
        RpcDriftedInput::Dylib => DriftedInput::Dylib,
        RpcDriftedInput::Tool(tool) => DriftedInput::Tool(tool),
    }
}

/// A task that counts its polls.
struct Counted<T> {
    polls: Rc<Cell<usize>>,
    task: Pin<Box<dyn Future<Output = T>>>,
}

impl<T> Future for Counted<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        self.polls.set(self.polls.get() + 1);
        self.task.as_mut().poll(cx)
    }
}
