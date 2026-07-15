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

use distill_build::trace::EntryRole;
use distill_core::id::{AssetUuid, ContentHash};
use distill_rpc::capnp_loader::{RemoteCall, RemoteHub, RemoteSnapshot};
use distill_rpc::capnp_transport::{CapnpClient, RemoteConnectOutcome};
use distill_rpc::{AssetEvent, ConnectRequest, DriftedInput as RpcDriftedInput, StreamEvent};
use tokio::sync::mpsc;
use tokio::sync::Notify;

use crate::admission::{Admission, FetchAdmission, FetchPermit};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcIoConfig {
    pub fetch_memory_budget: usize,
    pub spool_threshold: usize,
    pub spool_directory: Option<PathBuf>,
}

impl Default for RpcIoConfig {
    fn default() -> Self {
        Self {
            fetch_memory_budget: DEFAULT_FETCH_MEMORY_BUDGET,
            spool_threshold: DEFAULT_SPOOL_THRESHOLD,
            spool_directory: None,
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
    events: mpsc::Receiver<Completion>,
    pending: Vec<IoEvent>,
    delivered_fetches: Vec<FetchPermit>,
    basis: IoBasis,
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
        let (event_tx, events) = mpsc::channel(COMPLETION_CHANNEL_CAPACITY);
        let (init_tx, init_rx) = sync_mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("distill-rpc-io".into())
            .spawn(move || run_thread(address, request, config, command_rx, event_tx, init_tx))
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
            events,
            pending: Vec::new(),
            delivered_fetches: Vec::new(),
            basis,
            thread: Some(thread),
        })
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
        for permit in std::mem::take(&mut self.delivered_fetches) {
            let _ = self.commands.blocking_send(Command::ReleaseFetch(permit));
        }
        let _ = self.commands.blocking_send(Command::Shutdown);
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
        for permit in std::mem::take(&mut self.delivered_fetches) {
            self.send(Command::ReleaseFetch(permit));
        }
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
    ReleaseFetch(FetchPermit),
    Shutdown,
}

fn run_thread(
    address: SocketAddr,
    request: ConnectRequest,
    config: RpcIoConfig,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Completion>,
    init: sync_mpsc::SyncSender<Result<IoBasis, RpcIoInitError>>,
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
                    RemoteConnectOutcome::ConfigurationPoisoned(poison) => {
                        format!("daemon configuration poisoned: {}", poison.message)
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
            events,
            subscriptions: Rc::new(RefCell::new(Subscriptions::default())),
            delta_task: None,
            fetch_admission: Rc::new(RefCell::new(FetchAdmission::new(
                config.fetch_memory_budget,
                config.spool_threshold,
            ))),
            fetch_wake: Rc::new(Notify::new()),
            request_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(
                IN_FLIGHT_REQUEST_LIMIT,
            )),
            spool_directory: config.spool_directory,
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
    events: mpsc::Sender<Completion>,
    subscriptions: Rc<RefCell<Subscriptions>>,
    delta_task: Option<tokio::task::JoinHandle<()>>,
    fetch_admission: Rc<RefCell<FetchAdmission>>,
    fetch_wake: Rc<Notify>,
    request_slots: std::sync::Arc<tokio::sync::Semaphore>,
    spool_directory: Option<PathBuf>,
}

#[derive(Default)]
struct Subscriptions {
    assets: BTreeSet<AssetUuid>,
    paths: BTreeSet<String>,
}

impl Driver {
    async fn run(mut self) {
        while let Some(command) = self.commands.recv().await {
            if !self.handle(command).await {
                break;
            }
        }
        if let Some(task) = self.delta_task.take() {
            task.abort();
        }
    }

    async fn handle(&mut self, command: Command) -> bool {
        match command {
            Command::BindTarget(target) => self.reconnect(target).await,
            Command::BeginSweep { reply } => {
                let basis = match self.snapshot.refresh().await {
                    Ok(RemoteCall::Success(snapshot)) => {
                        self.snapshot = snapshot;
                        Some(io_basis(self.snapshot.basis()))
                    }
                    Ok(call) => {
                        let _ = send_event(&self.events, connection_event(call)).await;
                        None
                    }
                    Err(error) => {
                        let _ = send_event(
                            &self.events,
                            IoEvent::ConnectionError {
                                message: error.to_string(),
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
                let Ok(request_slot) = std::sync::Arc::clone(&self.request_slots)
                    .acquire_owned()
                    .await
                else {
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
                let Ok(request_slot) = std::sync::Arc::clone(&self.request_slots)
                    .acquire_owned()
                    .await
                else {
                    return false;
                };
                let hub = self.hub.clone();
                let events = self.events.clone();
                let admission = Rc::clone(&self.fetch_admission);
                let wake = Rc::clone(&self.fetch_wake);
                let spool_directory = self.spool_directory.clone();
                tokio::task::spawn_local(async move {
                    let _request_slot = request_slot;
                    let completion = fetch_event(
                        hub,
                        req,
                        content_hash,
                        basis,
                        admission,
                        wake,
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
                let Ok(request_slot) = std::sync::Arc::clone(&self.request_slots)
                    .acquire_owned()
                    .await
                else {
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
                if inserted {
                    self.subscribe(vec![uuid], Vec::new()).await;
                }
            }
            Command::UnsubscribeAsset(uuid) => {
                let removed = self.subscriptions.borrow_mut().assets.remove(&uuid);
                if removed {
                    self.unsubscribe(vec![uuid], Vec::new()).await;
                }
            }
            Command::SubscribePath(path) => {
                let inserted = self.subscriptions.borrow_mut().paths.insert(path.clone());
                if inserted {
                    self.subscribe(Vec::new(), vec![path]).await;
                }
            }
            Command::UnsubscribePath(path) => {
                let removed = self.subscriptions.borrow_mut().paths.remove(&path);
                if removed {
                    self.unsubscribe(Vec::new(), vec![path]).await;
                }
            }
            Command::ReleaseFetch(permit) => {
                let _ = self.fetch_admission.borrow_mut().release(permit);
                self.fetch_wake.notify_waiters();
            }
            Command::Shutdown => return false,
        }
        true
    }

    async fn reconnect(&mut self, target: RuntimeTarget) {
        let request = connect_request(&self.target, &target);
        let client = match CapnpClient::connect_local(self.address).await {
            Ok(client) => client,
            Err(error) => {
                let _ = send_event(
                    &self.events,
                    IoEvent::TargetRejected {
                        message: error.to_string(),
                    },
                )
                .await;
                return;
            }
        };
        let outcome = match client.connect(&request).await {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = send_event(
                    &self.events,
                    IoEvent::TargetRejected {
                        message: error.to_string(),
                    },
                )
                .await;
                return;
            }
        };
        let hub = match RemoteHub::connected(outcome) {
            Ok(hub) => hub,
            Err(outcome) => {
                let _ = send_event(
                    &self.events,
                    IoEvent::TargetRejected {
                        message: format!("RPC reconnection rejected: {outcome:?}"),
                    },
                )
                .await;
                return;
            }
        };
        let snapshot = match hub.snapshot().await {
            Ok(RemoteCall::Success(snapshot)) => snapshot,
            Ok(call) => {
                let _ = send_event(
                    &self.events,
                    IoEvent::TargetRejected {
                        message: remote_message(call),
                    },
                )
                .await;
                return;
            }
            Err(error) => {
                let _ = send_event(
                    &self.events,
                    IoEvent::TargetRejected {
                        message: error.to_string(),
                    },
                )
                .await;
                return;
            }
        };
        if let Some(task) = self.delta_task.take() {
            task.abort();
        }
        self.client = client;
        self.hub = hub;
        self.snapshot = snapshot;
        self.restart_subscription().await;
        self.publish_target_bound(target).await;
    }

    async fn publish_target_bound(&self, target: RuntimeTarget) {
        let _ = send_event(
            &self.events,
            IoEvent::TargetBound {
                target,
                basis: io_basis(self.snapshot.basis()),
            },
        )
        .await;
    }

    async fn restart_subscription(&mut self) {
        if let Some(task) = self.delta_task.take() {
            task.abort();
        }
        let (assets, paths) = {
            let subscriptions = self.subscriptions.borrow();
            (
                subscriptions.assets.iter().copied().collect::<Vec<_>>(),
                subscriptions.paths.iter().cloned().collect::<Vec<_>>(),
            )
        };
        if !assets.is_empty() || !paths.is_empty() {
            self.subscribe(assets, paths).await;
        }
    }

    fn basis_matches(&self, basis: &IoBasis) -> bool {
        io_basis(self.snapshot.basis()) == *basis
    }

    async fn subscribe(&mut self, assets: Vec<AssetUuid>, paths: Vec<String>) {
        match self
            .hub
            .subscribe(self.snapshot.basis().snapshot.version, assets, paths)
            .await
        {
            Ok(RemoteCall::Success(mut subscription)) => {
                if self.delta_task.is_none() {
                    let events = self.events.clone();
                    let subscriptions = Rc::clone(&self.subscriptions);
                    self.delta_task = Some(tokio::task::spawn_local(async move {
                        loop {
                            match subscription.next().await {
                                Ok(Some(event)) => {
                                    let filtered = {
                                        let subscriptions = subscriptions.borrow();
                                        stream_events(
                                            event,
                                            &subscriptions.assets,
                                            &subscriptions.paths,
                                        )
                                    };
                                    for event in filtered {
                                        if !send_event(&events, event).await {
                                            return;
                                        }
                                    }
                                }
                                Ok(None) => return,
                                Err(error) => {
                                    let _ = send_event(
                                        &events,
                                        IoEvent::ConnectionError {
                                            message: error.to_string(),
                                        },
                                    )
                                    .await;
                                    return;
                                }
                            }
                        }
                    }));
                }
            }
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
    hub: RemoteHub,
    req: ReqId,
    content_hash: ContentHash,
    request_basis: IoBasis,
    admission: Rc<RefCell<FetchAdmission>>,
    wake: Rc<Notify>,
    spool_directory: Option<PathBuf>,
) -> Completion {
    let mut terminal = match hub.fetch(content_hash).await {
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
    let permit = match acquire_fetch(admission, wake, total_bytes).await {
        Ok(permit) => permit,
        Err(error) => return Completion::event(request_error(req, request_basis, error)),
    };
    let payload = match permit.storage {
        FetchStorage::Memory => match collect_remote_chunks(&mut terminal.value).await {
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
        FetchStorage::Spool => {
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
    match payload.finish(layout_hash, load_edges, wire_layout) {
        Ok(artifact) => Completion {
            event: IoEvent::Fetched {
                req,
                content_hash,
                artifact,
                basis,
            },
            fetch_permit: Some(permit.into_permit()),
        },
        Err(error) => Completion::event(request_error(req, request_basis, error)),
    }
}

async fn collect_remote_chunks(
    stream: &mut distill_rpc::capnp_loader::RemoteChunkStream,
) -> Result<(Vec<u8>, Vec<Vec<u8>>), String> {
    let mut structural = Vec::new();
    let mut blobs = std::collections::BTreeMap::<u32, Vec<u8>>::new();
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
                output.extend_from_slice(&chunk.bytes);
            }
            Ok(None) => {
                if blobs.keys().copied().ne(0..blobs.len() as u32) {
                    return Err("artifact blob chunk indices are not contiguous".into());
                }
                return Ok((structural, blobs.into_values().collect()));
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchStorage {
    Memory,
    Spool,
}

struct FetchAdmissionGuard {
    admission: Rc<RefCell<FetchAdmission>>,
    wake: Rc<Notify>,
    permit: Option<FetchPermit>,
    storage: FetchStorage,
}

impl Drop for FetchAdmissionGuard {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            let _ = self.admission.borrow_mut().release(permit);
            self.wake.notify_waiters();
        }
    }
}

impl FetchAdmissionGuard {
    fn into_permit(mut self) -> FetchPermit {
        self.permit
            .take()
            .expect("an admitted fetch guard always owns its permit")
    }
}

async fn acquire_fetch(
    admission: Rc<RefCell<FetchAdmission>>,
    wake: Rc<Notify>,
    bytes: usize,
) -> Result<FetchAdmissionGuard, String> {
    loop {
        let notified = wake.notified();
        let decision = admission
            .borrow_mut()
            .admit(bytes)
            .map_err(|error| format!("fetch admission failed: {error:?}"))?;
        match decision {
            Admission::Memory(permit) => {
                return Ok(FetchAdmissionGuard {
                    admission,
                    wake: Rc::clone(&wake),
                    permit: Some(permit),
                    storage: FetchStorage::Memory,
                })
            }
            Admission::Spool(permit) => {
                return Ok(FetchAdmissionGuard {
                    admission,
                    wake: Rc::clone(&wake),
                    permit: Some(permit),
                    storage: FetchStorage::Spool,
                })
            }
            Admission::Wait => notified.await,
        }
    }
}

enum FetchPayload {
    Memory {
        structural: Vec<u8>,
        blobs: Vec<Vec<u8>>,
    },
    Spool {
        backing: ArcMappedSpool,
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
                backing,
                structural,
                blobs,
            } => artifact_layout_hash_backed(content_hash, backing.as_ref(), structural, blobs),
        }
    }

    fn finish(
        self,
        layout_hash: distill_core::id::LayoutHash,
        load_edges: Vec<distill_rpc::ServedLoadEdge>,
        wire_layout: std::sync::Arc<[u8]>,
    ) -> Result<crate::FetchedArtifact, String> {
        match self {
            Self::Memory { structural, blobs } => {
                fetched_artifact(layout_hash, structural, blobs, load_edges, wire_layout)
            }
            Self::Spool {
                backing,
                structural,
                blobs,
            } => fetched_artifact_backed(
                layout_hash,
                backing,
                structural,
                blobs,
                load_edges,
                wire_layout,
            ),
        }
    }
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
    let mut file = match directory {
        Some(directory) => tempfile::NamedTempFile::new_in(directory),
        None => tempfile::NamedTempFile::new(),
    }
    .map_err(|error| format!("cannot create fetch spool: {error}"))?;
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
    // Safety: the file is retained by MappedSpool and is never mutated after
    // this point. Every exposed range is checked before Blob construction.
    let mapping = unsafe { memmap2::MmapOptions::new().map(file.as_file()) }
        .map_err(|error| format!("cannot map fetch spool: {error}"))?;
    Ok(FetchPayload::Spool {
        backing: std::sync::Arc::new(MappedSpool {
            mapping,
            _file: file,
        }),
        structural,
        blobs,
    })
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
        other => request_error(req, basis, remote_message(other)),
    }
}

fn connection_event<T: std::fmt::Debug>(call: RemoteCall<T>) -> IoEvent {
    match call {
        RemoteCall::ReconnectRequired(reason) => IoEvent::ReconnectRequired {
            reason: reconnect_reason(reason),
        },
        other => IoEvent::ConnectionError {
            message: remote_message(other),
        },
    }
}

fn remote_message<T: std::fmt::Debug>(call: RemoteCall<T>) -> String {
    match call {
        RemoteCall::ConfigurationPoisoned(poison) => {
            format!("daemon configuration poisoned: {}", poison.message)
        }
        RemoteCall::VersionPoisoned(poison) => format!("daemon version poisoned: {poison}"),
        RemoteCall::LeaseFailure(error) | RemoteCall::Error(error) => error.message,
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
