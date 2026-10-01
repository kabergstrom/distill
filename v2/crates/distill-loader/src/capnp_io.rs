//! Development `LoaderIO` driven by Cap'n Proto on a dedicated local IO thread.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::Write;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc as sync_mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use distill_build::trace::EntryRole;
use distill_core::id::{AssetUuid, ContentHash};
use distill_rpc::capnp_loader::{RemoteCall, RemoteHub, RemoteSnapshot, RemoteSubscription};
use distill_rpc::capnp_transport::{
    CapnpClient, RemoteConnectOutcome, ARTIFACT_NOT_FOUND, CONNECTION_CLOSED,
};
use distill_rpc::{
    AssetEvent, ConnectRequest, DriftedInput as RpcDriftedInput, ImportFailure, StreamEvent,
};
use tokio::sync::{mpsc, watch};

use crate::admission::{Admission, FetchAdmission};
use crate::io::{
    AssetDeltaState, DriftedInput, IoEvent, LoaderIO, PathResolveResult, ReconnectReason, ReqId,
    ResolveResult, RuntimeTarget,
};
use crate::rpc_decode::{
    artifact_layout_hash, artifact_layout_hash_backed, fetched_artifact, fetched_artifact_backed,
    io_basis,
};
use crate::IoBasis;

const DEFAULT_FETCH_MEMORY_BUDGET: usize = 64 * 1024 * 1024;
const DEFAULT_SPOOL_THRESHOLD: usize = 8 * 1024 * 1024;
const COMMAND_CHANNEL_CAPACITY: usize = 256;
const COMPLETION_CHANNEL_CAPACITY: usize = 256;
const IN_FLIGHT_REQUEST_LIMIT: usize = 256;
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(25);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(1);
const RECONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
/// Watched-import failures publish no version; the driver polls them.
const IMPORT_FAILURE_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Consecutive failed rebind attempts before the game hears of them. A
/// pipeline swap briefly fences connections (the daemon answers
/// `PipelineUnavailable` until the new epoch serves); the backoff rides that
/// out without a diagnostic.
const DEFAULT_TARGET_REJECTION_AFTER: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcIoConfig {
    pub fetch_memory_budget: usize,
    pub spool_threshold: usize,
    pub spool_directory: Option<PathBuf>,
    /// Consecutive failed rebind attempts after which `TargetRejected` is
    /// reported (once per rebind). Retries continue either way.
    pub target_rejection_after: u32,
}

impl Default for RpcIoConfig {
    fn default() -> Self {
        Self {
            fetch_memory_budget: DEFAULT_FETCH_MEMORY_BUDGET,
            spool_threshold: DEFAULT_SPOOL_THRESHOLD,
            spool_directory: None,
            target_rejection_after: DEFAULT_TARGET_REJECTION_AFTER,
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

pub struct RpcIo {
    commands: mpsc::Sender<Command>,
    shutdown: watch::Sender<bool>,
    events: mpsc::Receiver<Completion>,
    pending: Vec<IoEvent>,
    delivered_fetches: Vec<FetchPermit>,
    basis: IoBasis,
    import_failures: watch::Receiver<Option<Vec<ImportFailure>>>,
    thread: Option<JoinHandle<()>>,
}

impl RpcIo {
    pub fn connect(address: SocketAddr, request: ConnectRequest) -> Result<Self, RpcIoInitError> {
        Self::connect_with_config(address, request, RpcIoConfig::default())
    }

    pub fn connect_with_config(
        address: SocketAddr,
        request: ConnectRequest,
        config: RpcIoConfig,
    ) -> Result<Self, RpcIoInitError> {
        let (commands, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (event_tx, events) = mpsc::channel(COMPLETION_CHANNEL_CAPACITY);
        let (init_tx, init_rx) = sync_mpsc::sync_channel(1);
        let (import_failures_tx, import_failures) = watch::channel(None);
        let thread = std::thread::Builder::new()
            .name("distill-rpc-io".into())
            .spawn(move || {
                run_thread(
                    address,
                    request,
                    config,
                    command_rx,
                    shutdown_rx,
                    event_tx,
                    init_tx,
                    import_failures_tx,
                )
            })
            .map_err(|error| RpcIoInitError::Unavailable(error.to_string()))?;
        let basis = match init_rx.recv() {
            Ok(Ok(basis)) => basis,
            Ok(Err(error)) => {
                let _ = thread.join();
                return Err(error);
            }
            Err(_) => {
                let _ = thread.join();
                return Err(RpcIoInitError::Unavailable(
                    "RPC IO thread stopped during initialization".into(),
                ));
            }
        };
        Ok(Self {
            commands,
            shutdown,
            events,
            pending: Vec::new(),
            delivered_fetches: Vec::new(),
            basis,
            import_failures,
            thread: Some(thread),
        })
    }

    /// The daemon's current watched-import failures when they changed since
    /// the last call; `None` when unchanged or not yet polled. Each names a
    /// bundle still serving its last good contents.
    pub fn take_import_failures(&mut self) -> Option<Vec<ImportFailure>> {
        if !self.import_failures.has_changed().unwrap_or(false) {
            return None;
        }
        self.import_failures.borrow_and_update().clone()
    }

    fn send(&mut self, command: Command) {
        if self.commands.blocking_send(command).is_err() {
            self.pending.push(IoEvent::ConnectionError {
                message: "RPC IO thread is unavailable".into(),
            });
        }
    }
}

impl Drop for RpcIo {
    fn drop(&mut self) {
        self.delivered_fetches.clear();
        let (_closed_sender, replacement) = mpsc::channel(1);
        drop(std::mem::replace(&mut self.events, replacement));
        let _ = self.shutdown.send(true);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl LoaderIO for RpcIo {
    fn bind_target(&mut self, target: RuntimeTarget) {
        self.send(Command::BindTarget(target));
    }

    fn begin_sweep(&mut self) -> IoBasis {
        let (reply, receive) = sync_mpsc::sync_channel(1);
        if self
            .commands
            .blocking_send(Command::BeginSweep { reply })
            .is_err()
        {
            self.pending.push(IoEvent::ConnectionError {
                message: "RPC IO thread is unavailable".into(),
            });
            return self.basis.clone();
        }
        match receive.recv() {
            Ok(Some(basis)) => {
                self.basis = basis;
                self.basis.clone()
            }
            Ok(None) => self.basis.clone(),
            Err(_) => {
                self.pending.push(IoEvent::ConnectionError {
                    message: "RPC IO thread stopped while refreshing its snapshot".into(),
                });
                self.basis.clone()
            }
        }
    }

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        self.send(Command::Resolve {
            req,
            uuid,
            basis: basis.clone(),
        });
    }

    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis) {
        self.send(Command::Fetch {
            req,
            content_hash,
            basis: basis.clone(),
        });
    }

    fn resolve_path(&mut self, req: ReqId, path: &str, basis: &IoBasis) {
        self.send(Command::ResolvePath {
            req,
            path: path.to_owned(),
            basis: basis.clone(),
        });
    }

    fn subscribe(&mut self, uuid: AssetUuid) {
        self.send(Command::SubscribeAsset(uuid));
    }

    fn unsubscribe(&mut self, uuid: AssetUuid) {
        self.send(Command::UnsubscribeAsset(uuid));
    }

    fn subscribe_path(&mut self, path: &str) {
        self.send(Command::SubscribePath(path.to_owned()));
    }

    fn unsubscribe_path(&mut self, path: &str) {
        self.send(Command::UnsubscribePath(path.to_owned()));
    }

    fn poll(&mut self) -> Vec<IoEvent> {
        self.delivered_fetches.clear();
        while let Ok(completion) = self.events.try_recv() {
            if let Some(permit) = completion.fetch_permit {
                self.delivered_fetches.push(permit);
            }
            self.pending.push(completion.event);
        }
        std::mem::take(&mut self.pending)
    }
}

struct Completion {
    event: IoEvent,
    fetch_permit: Option<FetchPermit>,
}

struct FetchPermit {
    _slot: tokio::sync::OwnedSemaphorePermit,
}

async fn send_event(events: &mpsc::Sender<Completion>, event: IoEvent) -> bool {
    events.send(Completion::event(event)).await.is_ok()
}

impl Completion {
    fn event(event: IoEvent) -> Self {
        Self {
            event,
            fetch_permit: None,
        }
    }
}

enum Command {
    BindTarget(RuntimeTarget),
    BeginSweep {
        reply: sync_mpsc::SyncSender<Option<IoBasis>>,
    },
    Resolve {
        req: ReqId,
        uuid: AssetUuid,
        basis: IoBasis,
    },
    Fetch {
        req: ReqId,
        content_hash: ContentHash,
        basis: IoBasis,
    },
    ResolvePath {
        req: ReqId,
        path: String,
        basis: IoBasis,
    },
    SubscribeAsset(AssetUuid),
    UnsubscribeAsset(AssetUuid),
    SubscribePath(String),
    UnsubscribePath(String),
}

fn run_thread(
    address: SocketAddr,
    request: ConnectRequest,
    config: RpcIoConfig,
    commands: mpsc::Receiver<Command>,
    shutdown: watch::Receiver<bool>,
    events: mpsc::Sender<Completion>,
    init: sync_mpsc::SyncSender<Result<IoBasis, RpcIoInitError>>,
    import_failures: watch::Sender<Option<Vec<ImportFailure>>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = init.send(Err(RpcIoInitError::Unavailable(error.to_string())));
            return;
        }
    };
    tokio::task::LocalSet::new().block_on(&runtime, async move {
        let client = match CapnpClient::connect_local(address).await {
            Ok(client) => client,
            Err(error) => {
                let _ = init.send(Err(RpcIoInitError::Unavailable(error.to_string())));
                return;
            }
        };
        let outcome = match client.connect(&request).await {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = init.send(Err(RpcIoInitError::Unavailable(error.to_string())));
                return;
            }
        };
        let hub = match RemoteHub::connected(outcome) {
            Ok(hub) => hub,
            Err(outcome) => {
                let error = match *outcome {
                    RemoteConnectOutcome::ConfigurationFailed(error) => {
                        format!("daemon configuration failed: {}", error.message)
                    }
                    other => format!("RPC connection rejected: {other:?}"),
                };
                let _ = init.send(Err(RpcIoInitError::Unavailable(error)));
                return;
            }
        };
        let snapshot = match hub.snapshot().await {
            Ok(RemoteCall::Success(snapshot)) => snapshot,
            Ok(call) => {
                let _ = init.send(Err(init_remote_failure(call)));
                return;
            }
            Err(error) => {
                let _ = init.send(Err(RpcIoInitError::Unavailable(error.to_string())));
                return;
            }
        };
        let basis = io_basis(snapshot.basis());
        if init.send(Ok(basis)).is_err() {
            return;
        }
        Driver {
            address,
            target: request.target,
            client,
            hub,
            snapshot,
            commands,
            shutdown,
            events,
            subscriptions: Rc::new(RefCell::new(Subscriptions::default())),
            delta_task: None,
            rebind: None,
            fetch_admission: FetchAdmission::new(
                config.fetch_memory_budget,
                config.spool_threshold,
            ),
            request_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(
                IN_FLIGHT_REQUEST_LIMIT,
            )),
            fetch_slot: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            spool_directory: config.spool_directory,
            import_failures,
            next_import_failure_poll: tokio::time::Instant::now(),
            target_rejection_after: config.target_rejection_after.max(1),
        }
        .run()
        .await;
    });
}

struct Driver {
    address: SocketAddr,
    target: String,
    client: CapnpClient,
    hub: RemoteHub,
    snapshot: RemoteSnapshot,
    commands: mpsc::Receiver<Command>,
    shutdown: watch::Receiver<bool>,
    events: mpsc::Sender<Completion>,
    subscriptions: Rc<RefCell<Subscriptions>>,
    delta_task: Option<tokio::task::JoinHandle<()>>,
    rebind: Option<RebindState>,
    fetch_admission: FetchAdmission,
    request_slots: std::sync::Arc<tokio::sync::Semaphore>,
    fetch_slot: std::sync::Arc<tokio::sync::Semaphore>,
    spool_directory: Option<PathBuf>,
    import_failures: watch::Sender<Option<Vec<ImportFailure>>>,
    next_import_failure_poll: tokio::time::Instant,
    target_rejection_after: u32,
}

struct RebindState {
    target: RuntimeTarget,
    next_attempt: tokio::time::Instant,
    backoff: Duration,
    failures: u32,
    reported: bool,
}

impl RebindState {
    /// Count a failed attempt; true exactly when this failure is the one to
    /// report (the `report_after`th in a row).
    fn record_failure(&mut self, report_after: u32) -> bool {
        self.failures = self.failures.saturating_add(1);
        let report = !self.reported && self.failures >= report_after;
        self.reported |= report;
        report
    }
}

struct ReconnectCandidate {
    target: RuntimeTarget,
    client: CapnpClient,
    hub: RemoteHub,
    snapshot: RemoteSnapshot,
    subscription: Option<RemoteSubscription>,
}

#[derive(Default)]
struct Subscriptions {
    assets: BTreeSet<AssetUuid>,
    paths: BTreeSet<String>,
}

impl Driver {
    async fn run(mut self) {
        'driver: loop {
            if *self.shutdown.borrow() {
                break;
            }
            let retry_at = self.rebind.as_ref().map(|rebind| rebind.next_attempt);
            let poll_at = self.next_import_failure_poll;
            let wake = if let Some(retry_at) = retry_at {
                tokio::select! {
                    biased;
                    _ = self.shutdown.changed() => DriverWake::Shutdown,
                    command = self.commands.recv() => DriverWake::Command(command),
                    () = tokio::time::sleep_until(retry_at) => DriverWake::Reconnect,
                }
            } else {
                tokio::select! {
                    biased;
                    _ = self.shutdown.changed() => DriverWake::Shutdown,
                    command = self.commands.recv() => DriverWake::Command(command),
                    () = tokio::time::sleep_until(poll_at) => DriverWake::PollImportFailures,
                }
            };
            match wake {
                DriverWake::Shutdown => break,
                DriverWake::PollImportFailures => self.poll_import_failures().await,
                DriverWake::Command(Some(command)) => {
                    if !self.handle(command).await {
                        break;
                    }
                }
                DriverWake::Command(None) => break,
                DriverWake::Reconnect => {
                    let target = self
                        .rebind
                        .as_ref()
                        .expect("reconnect wake requires pending state")
                        .target
                        .clone();
                    let attempt = {
                        let mut shutdown = self.shutdown.clone();
                        let attempt = tokio::time::timeout(
                            RECONNECT_ATTEMPT_TIMEOUT,
                            self.prepare_reconnect(target),
                        );
                        tokio::pin!(attempt);
                        tokio::select! {
                            biased;
                            _ = shutdown.changed() => break 'driver,
                            result = &mut attempt => result,
                        }
                    };
                    match attempt {
                        Ok(Ok(candidate)) => self.commit_reconnect(candidate).await,
                        Ok(Err(message)) => self.defer_reconnect(message).await,
                        Err(_) => {
                            self.defer_reconnect(format!(
                                "RPC reconnection attempt timed out after {} ms",
                                RECONNECT_ATTEMPT_TIMEOUT.as_millis(),
                            ))
                            .await;
                        }
                    }
                }
            }
        }
        if let Some(task) = self.delta_task.take() {
            task.abort();
        }
    }

    /// Publish the daemon's watched-import failures when they changed. A
    /// failed poll keeps the last list; reconnection is the command path's
    /// job.
    async fn poll_import_failures(&mut self) {
        self.next_import_failure_poll =
            tokio::time::Instant::now() + IMPORT_FAILURE_POLL_INTERVAL;
        if let Ok(RemoteCall::Success(failures)) = self.hub.import_failures().await {
            self.import_failures.send_if_modified(|current| {
                if current.as_ref() == Some(&failures) {
                    return false;
                }
                *current = Some(failures);
                true
            });
        }
    }

    async fn handle(&mut self, command: Command) -> bool {
        match command {
            Command::BindTarget(target) => self.begin_reconnect(target),
            Command::BeginSweep { reply } => {
                // Each round reads one snapshot of its own.
                let basis = match self.hub.snapshot().await {
                    Ok(RemoteCall::Success(snapshot)) => {
                        self.snapshot = snapshot;
                        Some(io_basis(self.snapshot.basis()))
                    }
                    Ok(call) => {
                        let _ = send_event(&self.events, connection_event(call)).await;
                        None
                    }
                    Err(_error) => {
                        let _ = send_event(
                            &self.events,
                            IoEvent::ReconnectRequired {
                                reason: ReconnectReason::ConnectionLost,
                            },
                        )
                        .await;
                        None
                    }
                };
                let _ = reply.send(basis);
            }
            Command::Resolve { req, uuid, basis } => {
                if !self.basis_matches(&basis) {
                    let _ = send_event(
                        &self.events,
                        request_error(req, basis, "stale RPC basis".into()),
                    )
                    .await;
                    return true;
                }
                let request_slots = std::sync::Arc::clone(&self.request_slots);
                let Ok(request_slot) = request_slots.acquire_owned().await else {
                    return false;
                };
                let snapshot = self.snapshot.clone();
                let events = self.events.clone();
                tokio::task::spawn_local(async move {
                    let _request_slot = request_slot;
                    let event = resolve_event(snapshot, req, uuid, basis).await;
                    let _ = send_event(&events, event).await;
                });
            }
            Command::Fetch {
                req,
                content_hash,
                basis,
            } => {
                if !self.basis_matches(&basis) {
                    let _ = send_event(
                        &self.events,
                        request_error(req, basis, "stale RPC basis".into()),
                    )
                    .await;
                    return true;
                }
                let request_slots = std::sync::Arc::clone(&self.request_slots);
                let Ok(request_slot) = request_slots.acquire_owned().await else {
                    return false;
                };
                let fetch_slot = std::sync::Arc::clone(&self.fetch_slot);
                let hub = self.hub.clone();
                let snapshot = self.snapshot.clone();
                let events = self.events.clone();
                let admission = self.fetch_admission;
                let spool_directory = self.spool_directory.clone();
                tokio::task::spawn_local(async move {
                    let _request_slot = request_slot;
                    let Ok(fetch_slot) = fetch_slot.acquire_owned().await else {
                        return;
                    };
                    let completion = fetch_event(
                        (hub, snapshot),
                        req,
                        content_hash,
                        basis,
                        admission,
                        FetchPermit { _slot: fetch_slot },
                        spool_directory,
                    )
                    .await;
                    let _ = events.send(completion).await;
                });
            }
            Command::ResolvePath { req, path, basis } => {
                if !self.basis_matches(&basis) {
                    let _ = send_event(
                        &self.events,
                        request_error(req, basis, "stale RPC basis".into()),
                    )
                    .await;
                    return true;
                }
                let request_slots = std::sync::Arc::clone(&self.request_slots);
                let Ok(request_slot) = request_slots.acquire_owned().await else {
                    return false;
                };
                let snapshot = self.snapshot.clone();
                let events = self.events.clone();
                tokio::task::spawn_local(async move {
                    let _request_slot = request_slot;
                    let event = path_event(snapshot, req, path, basis).await;
                    let _ = send_event(&events, event).await;
                });
            }
            Command::SubscribeAsset(uuid) => {
                let inserted = self.subscriptions.borrow_mut().assets.insert(uuid);
                if inserted && self.rebind.is_none() {
                    let _ = self.subscribe(vec![uuid], Vec::new()).await;
                }
            }
            Command::UnsubscribeAsset(uuid) => {
                let removed = self.subscriptions.borrow_mut().assets.remove(&uuid);
                if removed && self.rebind.is_none() {
                    self.unsubscribe(vec![uuid], Vec::new()).await;
                }
            }
            Command::SubscribePath(path) => {
                let inserted = self.subscriptions.borrow_mut().paths.insert(path.clone());
                if inserted && self.rebind.is_none() {
                    let _ = self.subscribe(Vec::new(), vec![path]).await;
                }
            }
            Command::UnsubscribePath(path) => {
                let removed = self.subscriptions.borrow_mut().paths.remove(&path);
                if removed && self.rebind.is_none() {
                    self.unsubscribe(Vec::new(), vec![path]).await;
                }
            }
        }
        true
    }

    fn begin_reconnect(&mut self, target: RuntimeTarget) {
        if let Some(task) = self.delta_task.take() {
            task.abort();
        }
        self.rebind = Some(RebindState {
            target,
            next_attempt: tokio::time::Instant::now(),
            backoff: RECONNECT_INITIAL_BACKOFF,
            failures: 0,
            reported: false,
        });
    }

    async fn prepare_reconnect(&self, target: RuntimeTarget) -> Result<ReconnectCandidate, String> {
        let request = connect_request(&self.target, &target);
        let client = CapnpClient::connect_local(self.address)
            .await
            .map_err(|error| error.to_string())?;
        let outcome = client
            .connect(&request)
            .await
            .map_err(|error| error.to_string())?;
        let hub = RemoteHub::connected(outcome)
            .map_err(|outcome| format!("RPC reconnection rejected: {outcome:?}"))?;
        let snapshot = match hub.snapshot().await {
            Ok(RemoteCall::Success(snapshot)) => snapshot,
            Ok(call) => return Err(remote_message(call)),
            Err(error) => return Err(error.to_string()),
        };
        let (assets, paths) = {
            let subscriptions = self.subscriptions.borrow();
            (
                subscriptions.assets.iter().copied().collect::<Vec<_>>(),
                subscriptions.paths.iter().cloned().collect::<Vec<_>>(),
            )
        };
        let subscription = if assets.is_empty() && paths.is_empty() {
            None
        } else {
            match hub
                .subscribe(snapshot.basis().snapshot.version, assets, paths)
                .await
            {
                Ok(RemoteCall::Success(subscription)) => Some(subscription),
                Ok(call) => return Err(remote_message(call)),
                Err(error) => return Err(error.to_string()),
            }
        };

        Ok(ReconnectCandidate {
            target,
            client,
            hub,
            snapshot,
            subscription,
        })
    }

    async fn commit_reconnect(&mut self, candidate: ReconnectCandidate) {
        let target_bound = IoEvent::TargetBound {
            target: candidate.target,
            basis: io_basis(candidate.snapshot.basis()),
        };
        // Queue the acknowledgment while the previous connection and durable
        // rebind state still own the driver. A full completion channel may
        // delay this local publication, but it is not a failed peer handshake
        // and must not consume the reconnect timeout or expose candidate state.
        if !send_event(&self.events, target_bound).await {
            return;
        }
        if let Some(task) = self.delta_task.take() {
            task.abort();
        }
        self.client = candidate.client;
        self.hub = candidate.hub;
        self.snapshot = candidate.snapshot;
        if let Some(subscription) = candidate.subscription {
            self.install_delta_stream(subscription);
        }
        self.rebind = None;
    }

    async fn defer_reconnect(&mut self, message: String) {
        let report_after = self.target_rejection_after;
        let report = self
            .rebind
            .as_mut()
            .is_some_and(|state| state.record_failure(report_after));
        if report {
            let _ = send_event(&self.events, IoEvent::TargetRejected { message }).await;
        }
        if let Some(state) = &mut self.rebind {
            state.next_attempt = tokio::time::Instant::now() + state.backoff;
            state.backoff = state.backoff.saturating_mul(2).min(RECONNECT_MAX_BACKOFF);
        }
    }

    fn basis_matches(&self, basis: &IoBasis) -> bool {
        io_basis(self.snapshot.basis()) == *basis
    }

    async fn subscribe(&mut self, assets: Vec<AssetUuid>, paths: Vec<String>) -> bool {
        match self
            .hub
            .subscribe(self.snapshot.basis().snapshot.version, assets, paths)
            .await
        {
            Ok(RemoteCall::Success(subscription)) => {
                if self.delta_task.is_none() {
                    self.install_delta_stream(subscription);
                }
                true
            }
            Ok(call) => {
                let _ = send_event(&self.events, connection_event(call)).await;
                false
            }
            Err(_error) => {
                let _ = send_event(
                    &self.events,
                    IoEvent::ReconnectRequired {
                        reason: ReconnectReason::ConnectionLost,
                    },
                )
                .await;
                false
            }
        }
    }

    fn install_delta_stream(&mut self, mut subscription: RemoteSubscription) {
        let events = self.events.clone();
        let subscriptions = Rc::clone(&self.subscriptions);
        self.delta_task = Some(tokio::task::spawn_local(async move {
            loop {
                match subscription.next().await {
                    Ok(Some(event)) => {
                        let filtered = {
                            let subscriptions = subscriptions.borrow();
                            stream_events(event, &subscriptions.assets, &subscriptions.paths)
                        };
                        for event in filtered {
                            if !send_event(&events, event).await {
                                return;
                            }
                        }
                    }
                    Ok(None) | Err(_) => {
                        let _ = send_event(
                            &events,
                            IoEvent::ReconnectRequired {
                                reason: ReconnectReason::ConnectionLost,
                            },
                        )
                        .await;
                        return;
                    }
                }
            }
        }));
    }

    async fn unsubscribe(&self, assets: Vec<AssetUuid>, paths: Vec<String>) {
        match self.hub.unsubscribe(assets, paths).await {
            Ok(RemoteCall::Success(())) => {}
            Ok(call) => {
                let _ = send_event(&self.events, connection_event(call)).await;
            }
            Err(error) => {
                let _ = send_event(
                    &self.events,
                    IoEvent::ConnectionError {
                        message: error.to_string(),
                    },
                )
                .await;
            }
        }
    }
}

enum DriverWake {
    Command(Option<Command>),
    Reconnect,
    PollImportFailures,
    Shutdown,
}

async fn resolve_event(
    snapshot: RemoteSnapshot,
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
    snapshot: RemoteSnapshot,
    req: ReqId,
    path: String,
    request_basis: IoBasis,
) -> IoEvent {
    match snapshot.resolve_path(&path).await {
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

async fn fetch_event(
    remote: (RemoteHub, RemoteSnapshot),
    req: ReqId,
    content_hash: ContentHash,
    request_basis: IoBasis,
    admission: FetchAdmission,
    permit: FetchPermit,
    spool_directory: Option<PathBuf>,
) -> Completion {
    let (hub, snapshot) = remote;
    let mut terminal = match snapshot.fetch(content_hash).await {
        Ok(RemoteCall::Success(terminal)) => terminal,
        Ok(call) => return Completion::event(remote_request_event(call, req, request_basis)),
        Err(error) => {
            return Completion::event(request_error(req, request_basis, error.to_string()))
        }
    };
    let basis = io_basis(&terminal.basis);
    let load_edges = terminal.value.load_edges().to_vec();
    let total_bytes = match usize::try_from(terminal.value.total_bytes()) {
        Ok(total_bytes) => total_bytes,
        Err(_) => {
            return Completion::event(request_error(
                req,
                request_basis,
                "fetched artifact is too large for this client".into(),
            ))
        }
    };
    let payload = match admission.admit(total_bytes) {
        Admission::Memory => match collect_remote_chunks(&mut terminal.value, total_bytes).await {
            Ok((structural, blobs)) => {
                let observed = blobs.iter().try_fold(structural.len(), |total, blob| {
                    total.checked_add(blob.len())
                });
                if observed != Some(total_bytes) {
                    return Completion::event(request_error(
                        req,
                        request_basis,
                        "artifact stream length differs from its authenticated total".into(),
                    ));
                }
                FetchPayload::Memory { structural, blobs }
            }
            Err(error) => {
                return Completion::event(request_error(req, request_basis, error));
            }
        },
        Admission::Spool => {
            match spool_remote_chunks(&mut terminal.value, total_bytes, spool_directory.as_deref())
                .await
            {
                Ok(payload) => payload,
                Err(error) => {
                    return Completion::event(request_error(req, request_basis, error));
                }
            }
        }
    };
    let layout_hash = match payload.layout_hash(content_hash) {
        Ok(layout_hash) => layout_hash,
        Err(error) => return Completion::event(request_error(req, request_basis, error)),
    };
    let wire_layout = match hub.wire_tree(layout_hash).await {
        Ok(RemoteCall::Success(bytes)) => bytes,
        Ok(call) => return Completion::event(remote_request_event(call, req, request_basis)),
        Err(error) => {
            return Completion::event(request_error(req, request_basis, error.to_string()))
        }
    };
    let admitted_bytes = match total_bytes.checked_add(wire_layout.len()) {
        Some(bytes) => bytes,
        None => {
            return Completion::event(request_error(
                req,
                request_basis,
                "artifact plus DSWL length overflows this client".into(),
            ))
        }
    };
    let spool = admission.should_spool(admitted_bytes);
    match payload.finish(
        layout_hash,
        load_edges,
        wire_layout,
        spool,
        spool_directory.as_deref(),
    ) {
        Ok(artifact) => Completion {
            event: IoEvent::Fetched {
                req,
                content_hash,
                artifact,
                basis,
            },
            fetch_permit: Some(permit),
        },
        Err(error) => Completion::event(request_error(req, request_basis, error)),
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

enum FetchPayload {
    Memory {
        structural: Vec<u8>,
        blobs: Vec<Vec<u8>>,
    },
    Spool {
        file: tempfile::NamedTempFile,
        structural: Range<usize>,
        blobs: Vec<Range<usize>>,
    },
}

type ArcMappedSpool = std::sync::Arc<dyn AsRef<[u8]> + Send + Sync>;

impl FetchPayload {
    fn layout_hash(
        &self,
        content_hash: ContentHash,
    ) -> Result<distill_core::id::LayoutHash, String> {
        match self {
            Self::Memory { structural, blobs } => {
                artifact_layout_hash(content_hash, structural, blobs)
            }
            Self::Spool {
                file,
                structural,
                blobs,
            } => {
                // Safety: the temporary is flushed before this point and no
                // writer runs while this short-lived validation map exists.
                let mapping = unsafe { memmap2::MmapOptions::new().map(file.as_file()) }
                    .map_err(|error| format!("cannot map fetch spool: {error}"))?;
                artifact_layout_hash_backed(content_hash, &mapping, structural, blobs)
            }
        }
    }

    fn finish(
        self,
        layout_hash: distill_core::id::LayoutHash,
        load_edges: Vec<distill_rpc::ServedLoadEdge>,
        wire_layout: std::sync::Arc<[u8]>,
        spool: bool,
        spool_directory: Option<&std::path::Path>,
    ) -> Result<crate::FetchedArtifact, String> {
        match self {
            Self::Memory { structural, blobs } if !spool => fetched_artifact(
                layout_hash,
                structural,
                blobs,
                load_edges,
                memory_wire_blob(wire_layout),
            ),
            Self::Memory { structural, blobs } => spool_complete_payload(
                layout_hash,
                structural,
                blobs,
                load_edges,
                &wire_layout,
                spool_directory,
            ),
            Self::Spool {
                mut file,
                structural,
                blobs,
            } => {
                let wire_start = structural.len() + blobs.iter().map(Range::len).sum::<usize>();
                file.write_all(&wire_layout)
                    .map_err(|error| format!("cannot write DSWL fetch spool: {error}"))?;
                file.flush()
                    .map_err(|error| format!("cannot flush fetch spool: {error}"))?;
                let backing = map_spool(file)?;
                let wire = distill_wire::exec::Blob::new(
                    std::sync::Arc::clone(&backing),
                    wire_start,
                    wire_layout.len(),
                );
                fetched_artifact_backed(layout_hash, backing, structural, blobs, load_edges, wire)
            }
        }
    }
}

fn memory_wire_blob(bytes: std::sync::Arc<[u8]>) -> distill_wire::exec::Blob {
    let len = bytes.len();
    let backing: ArcMappedSpool = std::sync::Arc::new(bytes);
    distill_wire::exec::Blob::new(backing, 0, len)
}

fn spool_complete_payload(
    layout_hash: distill_core::id::LayoutHash,
    structural: Vec<u8>,
    blobs: Vec<Vec<u8>>,
    load_edges: Vec<distill_rpc::ServedLoadEdge>,
    wire_layout: &[u8],
    directory: Option<&std::path::Path>,
) -> Result<crate::FetchedArtifact, String> {
    let mut file = create_spool(directory)?;
    file.write_all(&structural)
        .map_err(|error| format!("cannot write fetch spool: {error}"))?;
    let structural_range = 0..structural.len();
    let mut cursor = structural.len();
    let mut blob_ranges = Vec::with_capacity(blobs.len());
    for blob in blobs {
        let end = cursor
            .checked_add(blob.len())
            .ok_or_else(|| "fetch spool length overflow".to_owned())?;
        file.write_all(&blob)
            .map_err(|error| format!("cannot write fetch spool: {error}"))?;
        blob_ranges.push(cursor..end);
        cursor = end;
    }
    let wire_start = cursor;
    file.write_all(wire_layout)
        .map_err(|error| format!("cannot write DSWL fetch spool: {error}"))?;
    file.flush()
        .map_err(|error| format!("cannot flush fetch spool: {error}"))?;
    let backing = map_spool(file)?;
    let wire = distill_wire::exec::Blob::new(
        std::sync::Arc::clone(&backing),
        wire_start,
        wire_layout.len(),
    );
    fetched_artifact_backed(
        layout_hash,
        backing,
        structural_range,
        blob_ranges,
        load_edges,
        wire,
    )
}

struct MappedSpool {
    mapping: memmap2::Mmap,
    _file: tempfile::NamedTempFile,
}

impl AsRef<[u8]> for MappedSpool {
    fn as_ref(&self) -> &[u8] {
        &self.mapping
    }
}

async fn spool_remote_chunks(
    stream: &mut distill_rpc::capnp_loader::RemoteChunkStream,
    total_bytes: usize,
    directory: Option<&std::path::Path>,
) -> Result<FetchPayload, String> {
    if total_bytes == 0 {
        return Err("artifact stream cannot be empty".into());
    }
    let mut file = create_spool(directory)?;
    let mut structural = 0usize..0usize;
    let mut blobs = Vec::<Range<usize>>::new();
    let mut total = 0usize;
    let mut current_blob = None;
    loop {
        let chunk = match stream.next_chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(error) => return Err(error.to_string()),
        };
        let bytes = chunk.bytes.as_ref();
        match chunk.kind {
            distill_rpc::ArtifactChunkKind::Structural => {
                if current_blob.is_some() || chunk.offset != structural.len() as u64 {
                    return Err("artifact chunks are not contiguous".into());
                }
                structural.end = structural
                    .end
                    .checked_add(bytes.len())
                    .ok_or_else(|| "artifact stream length overflow".to_owned())?;
            }
            distill_rpc::ArtifactChunkKind::Blob { index } => {
                let index = usize::try_from(index)
                    .map_err(|_| "artifact blob index does not fit this client")?;
                if current_blob.is_none() || current_blob != Some(index) {
                    if index != blobs.len() {
                        return Err("artifact blob chunk indices are not contiguous".into());
                    }
                    current_blob = Some(index);
                    blobs.push(total..total);
                }
                let range = &mut blobs[index];
                if chunk.offset != range.len() as u64 {
                    return Err("artifact chunks are not contiguous".into());
                }
                range.end = range
                    .end
                    .checked_add(bytes.len())
                    .ok_or_else(|| "artifact stream length overflow".to_owned())?;
            }
        }
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| "artifact stream length overflow".to_owned())?;
        if total > total_bytes {
            return Err("artifact stream exceeds its authenticated total".into());
        }
        file.write_all(bytes)
            .map_err(|error| format!("cannot write fetch spool: {error}"))?;
    }
    if total != total_bytes {
        return Err("artifact stream length differs from its authenticated total".into());
    }
    file.flush()
        .map_err(|error| format!("cannot flush fetch spool: {error}"))?;
    Ok(FetchPayload::Spool {
        file,
        structural,
        blobs,
    })
}

fn create_spool(directory: Option<&std::path::Path>) -> Result<tempfile::NamedTempFile, String> {
    match directory {
        Some(directory) => tempfile::NamedTempFile::new_in(directory),
        None => tempfile::NamedTempFile::new(),
    }
    .map_err(|error| format!("cannot create fetch spool: {error}"))
}

fn map_spool(file: tempfile::NamedTempFile) -> Result<ArcMappedSpool, String> {
    // Safety: the file is retained by MappedSpool and is never mutated after
    // this point. Every exposed range is checked before Blob construction.
    let mapping = unsafe { memmap2::MmapOptions::new().map(file.as_file()) }
        .map_err(|error| format!("cannot map fetch spool: {error}"))?;
    Ok(std::sync::Arc::new(MappedSpool {
        mapping,
        _file: file,
    }))
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
        RemoteCall::Error(error) if error.code == CONNECTION_CLOSED => IoEvent::ReconnectRequired {
            reason: ReconnectReason::ConnectionLost,
        },
        other => request_error(req, basis, remote_message(other)),
    }
}

fn connection_event<T: std::fmt::Debug>(call: RemoteCall<T>) -> IoEvent {
    match call {
        RemoteCall::ReconnectRequired(reason) => IoEvent::ReconnectRequired {
            reason: reconnect_reason(reason),
        },
        RemoteCall::Error(error) if error.code == CONNECTION_CLOSED => IoEvent::ReconnectRequired {
            reason: ReconnectReason::ConnectionLost,
        },
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
        distill_rpc::ReconnectReason::TargetDefinitionChanged => {
            ReconnectReason::TargetDefinitionChanged
        }
        distill_rpc::ReconnectReason::StoreInstanceChanged => ReconnectReason::StoreInstanceChanged,
        distill_rpc::ReconnectReason::ProtocolEpochChanged => ReconnectReason::ProtocolEpochChanged,
        distill_rpc::ReconnectReason::PipelineEpochChanged => ReconnectReason::PipelineEpochChanged,
    }
}

fn delta_state(state: distill_rpc::AssetDeltaState) -> AssetDeltaState {
    match state {
        distill_rpc::AssetDeltaState::Changed => AssetDeltaState::Changed,
        distill_rpc::AssetDeltaState::Deleted => AssetDeltaState::Deleted,
        distill_rpc::AssetDeltaState::Restored => AssetDeltaState::Restored,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rebind_reports_once_after_consecutive_failures() {
        let mut state = RebindState {
            target: RuntimeTarget {
                epoch: crate::GameModuleEpoch(1),
                target_definition_hash: [0; 32],
            },
            next_attempt: tokio::time::Instant::now(),
            backoff: RECONNECT_INITIAL_BACKOFF,
            failures: 0,
            reported: false,
        };
        let reports = (0..6)
            .map(|_| state.record_failure(DEFAULT_TARGET_REJECTION_AFTER))
            .collect::<Vec<_>>();
        assert_eq!(reports, [false, false, true, false, false, false]);
    }
}
