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
use distill_core::id::{AssetUuid, ContentHash};
use distill_rpc::capnp_loader::{RemoteCall, RemoteHub, RemoteSnapshot, RemoteSubscription};
use distill_rpc::capnp_transport::{
    CapnpClient, RemoteConnectOutcome, ARTIFACT_NOT_FOUND, CONNECTION_CLOSED,
};
use distill_rpc::{
    AssetEvent, ConnectRequest, DriftedInput as RpcDriftedInput, ImportFailure, StreamEvent,
};
use distill_store::state::{InputVersion, SnapshotStamp};
use tokio::task::{AbortHandle, LocalSet};

use crate::admission::{FetchAdmission, Reservation};
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
    /// connection is bound: requests under it fail as stale.
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

    /// The daemon's current watched-import failures when they changed since
    /// the last call; `None` when unchanged or not yet polled. Each names a
    /// bundle still serving its last good contents.
    pub fn take_import_failures(&mut self) -> Option<Vec<ImportFailure>> {
        if !self.shared.import_failures_changed.replace(false) {
            return None;
        }
        self.shared.import_failures.borrow().clone()
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
        let held = match basis {
            IoBasis::Rpc { snapshot } => self
                .shared
                .snapshots
                .borrow()
                .held
                .contains_key(snapshot)
                .then_some(*snapshot),
            IoBasis::Pack { .. } => None,
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
        let task = self.shared.spawn(rebind(Rc::clone(&self.shared), target.clone()));
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
        if self.shared.subscriptions.borrow_mut().assets.insert(uuid) {
            let _local = self.local.enter();
            self.shared.subscribe(vec![uuid], Vec::new());
        }
    }

    fn unsubscribe(&mut self, uuid: AssetUuid) {
        if self.shared.subscriptions.borrow_mut().assets.remove(&uuid) {
            let _local = self.local.enter();
            self.shared.unsubscribe(vec![uuid], Vec::new());
        }
    }

    fn subscribe_path(&mut self, path: &str) {
        if self.shared.subscriptions.borrow_mut().paths.insert(path.to_owned()) {
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
    /// Signalled whenever a snapshot refresh ends.
    refreshed: tokio::sync::Notify,
    next_serial: Cell<u64>,
    /// Polls of the tasks this IO spawned, ever (see [`StepStats`]).
    task_polls: Rc<Cell<usize>>,
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
        let Some(current) = self.current.and_then(|stamp| Some((stamp, self.held.get(&stamp)?)))
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
    assets: BTreeSet<AssetUuid>,
    paths: BTreeSet<String>,
}

impl Subscriptions {
    fn lists(&self) -> (Vec<AssetUuid>, Vec<String>) {
        (
            self.assets.iter().copied().collect(),
            self.paths.iter().cloned().collect(),
        )
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
            refreshed: tokio::sync::Notify::new(),
            next_serial: Cell::new(1),
            task_polls: Rc::new(Cell::new(0)),
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

    fn connection(&self) -> Option<Rc<Connection>> {
        match &*self.link.borrow() {
            Link::Connected(connection) => Some(Rc::clone(connection)),
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
        if matches!(event, IoEvent::ReconnectRequired { .. })
            && connection.reconnect_reported.replace(true)
        {
            return;
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
            fetch_event(&shared, &connection.hub, &snapshot, req, content_hash, basis).await
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
        IoEvent::ReconnectRequired { .. } | IoEvent::ConnectionError { .. } => {
            shared.push_connection(&connection, event)
        }
        event => shared.push_request(Some(stamp), event, reservation),
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
                let events = {
                    let subscriptions = shared.subscriptions.borrow();
                    stream_events(event, &subscriptions.assets, &subscriptions.paths)
                };
                let newest = events
                    .iter()
                    .filter_map(|event| match event {
                        IoEvent::Delta { stamp, .. } => Some(*stamp),
                        _ => None,
                    })
                    .max_by_key(|stamp| stamp.version);
                if let Some(stamp) = newest {
                    if let Err(event) = ensure_current(&shared, &connection, Need::AtLeast(stamp)).await
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

/// Per connection: poll watched-import failures and keep the current
/// snapshot well inside the daemon's TTL.
async fn maintain(shared: Rc<Shared>, connection: Rc<Connection>) {
    loop {
        if let Ok(RemoteCall::Success(failures)) = connection.hub.import_failures().await {
            shared.set_import_failures(failures);
        }
        let stale = shared.snapshots.borrow().current.and_then(|stamp| {
            let held = shared.snapshots.borrow().held.get(&stamp).map(|held| (held.serial, held.opened))?;
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
                shared.push(IoEvent::TargetBound { target, basis });
                return;
            }
            Ok(Err(error)) => error.message(),
            Err(_) => format!(
                "RPC reconnection attempt timed out after {} ms",
                RECONNECT_ATTEMPT_TIMEOUT.as_millis()
            ),
        };
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
    let unavailable = |error: &dyn std::fmt::Display| RpcIoInitError::Unavailable(error.to_string());
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
            .subscribe(snapshot.basis().snapshot.version, assets.clone(), paths.clone())
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

/// Fetch one artifact and its DSWL tree. The payload's size is reserved
/// before its stream is read, waiting unread while it does not fit; the
/// reservation grows by the DSWL tree once that arrives and travels with
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
    let mut reservation = shared.admission.admit(total_bytes).await;
    let (structural, blobs) = match collect_remote_chunks(&mut terminal.value, total_bytes).await {
        Ok(payload) => payload,
        Err(error) => return fail(error),
    };
    // The layout hash the artifact header declares. The loader
    // authenticates the artifact itself (content hash, structure) when it
    // parses it; hashing every byte here as well would double that cost on
    // the engine thread.
    let layout_hash = match distill_wire::artifact::artifact_header_layout_hash(&structural) {
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
    match fetched_artifact(layout_hash, structural, blobs, load_edges, wire_layout) {
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

async fn collect_remote_chunks(
    stream: &mut distill_rpc::capnp_loader::RemoteChunkStream,
    total_bytes: usize,
) -> Result<(Vec<u8>, Vec<Vec<u8>>), String> {
    let mut structural = Vec::new();
    let mut blobs = std::collections::BTreeMap::<u32, Vec<u8>>::new();
    let mut total = 0usize;
    loop {
        match stream.next_chunk().await {
            Ok(Some(chunk)) => {
                let output = match chunk.kind {
                    distill_rpc::ArtifactChunkKind::Structural => &mut structural,
                    distill_rpc::ArtifactChunkKind::Blob { index } => {
                        blobs.entry(index).or_default()
                    }
                };
                if chunk.offset != output.len() as u64 {
                    return Err("artifact chunks are not contiguous".into());
                }
                total = total
                    .checked_add(chunk.bytes.len())
                    .ok_or_else(|| "artifact stream length overflow".to_owned())?;
                if total > total_bytes {
                    return Err("artifact stream exceeds its authenticated total".into());
                }
                output.extend_from_slice(&chunk.bytes);
            }
            Ok(None) => {
                if blobs.keys().copied().ne(0..blobs.len() as u32) {
                    return Err("artifact blob chunk indices are not contiguous".into());
                }
                if total != total_bytes {
                    return Err(
                        "artifact stream length differs from its authenticated total".into(),
                    );
                }
                return Ok((structural, blobs.into_values().collect()));
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
        RemoteCall::Error(error) if error.code == ARTIFACT_NOT_FOUND => {
            IoEvent::SnapshotExpired { req, basis }
        }
        RemoteCall::Error(error) if error.code == CONNECTION_CLOSED => connection_lost(),
        other => request_error(req, basis, remote_message(other)),
    }
}

fn connection_event<T: std::fmt::Debug>(call: RemoteCall<T>) -> IoEvent {
    match call {
        RemoteCall::ReconnectRequired(reason) => IoEvent::ReconnectRequired {
            reason: reconnect_reason(reason),
        },
        RemoteCall::Error(error) if error.code == CONNECTION_CLOSED => connection_lost(),
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

fn stream_events(
    event: StreamEvent,
    subscribed_assets: &BTreeSet<AssetUuid>,
    subscribed_paths: &BTreeSet<String>,
) -> Vec<IoEvent> {
    match event {
        StreamEvent::InitialDelta { deltas, .. } => deltas
            .into_iter()
            .map(|delta| IoEvent::Delta {
                stamp: delta.basis.snapshot,
                assets: delta
                    .assets
                    .into_iter()
                    .map(|(asset, state)| (asset, delta_state(state)))
                    .collect(),
                paths: delta.paths,
            })
            .collect(),
        StreamEvent::Delta(delta) => vec![IoEvent::Delta {
            stamp: delta.basis.snapshot,
            assets: delta
                .assets
                .into_iter()
                .map(|(asset, state)| (asset, delta_state(state)))
                .collect(),
            paths: delta.paths,
        }],
        StreamEvent::ResyncRequired { basis, .. } => vec![IoEvent::Delta {
            stamp: basis.snapshot,
            assets: subscribed_assets
                .iter()
                .copied()
                .map(|asset| (asset, AssetDeltaState::Changed))
                .collect(),
            paths: subscribed_paths.iter().cloned().collect(),
        }],
        StreamEvent::Asset { event, .. } => match event {
            AssetEvent::ReconnectRequired { reason } => vec![IoEvent::ReconnectRequired {
                reason: reconnect_reason(reason),
            }],
            AssetEvent::RestartRequired { keys } => vec![IoEvent::ConnectionError {
                message: format!("daemon restart required for {}", keys.join(", ")),
            }],
            AssetEvent::Error { message, .. } => vec![IoEvent::ConnectionError { message }],
            _ => Vec::new(),
        },
    }
}

fn reconnect_reason(reason: distill_rpc::ReconnectReason) -> ReconnectReason {
    match reason {
        distill_rpc::ReconnectReason::StoreInstanceChanged => ReconnectReason::StoreInstanceChanged,
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
