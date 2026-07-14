//! Development `LoaderIO` driven by Cap'n Proto on a dedicated local IO thread.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::mpsc as sync_mpsc;
use std::thread::JoinHandle;

use distill_build::trace::EntryRole;
use distill_core::id::{AssetUuid, ContentHash};
use distill_rpc::capnp_loader::{RemoteCall, RemoteHub, RemoteSnapshot};
use distill_rpc::capnp_transport::{CapnpClient, RemoteConnectOutcome};
use distill_rpc::{AssetEvent, ConnectRequest, DriftedInput as RpcDriftedInput, StreamEvent};
use tokio::sync::mpsc;

use crate::io::{
    AssetDeltaState, DriftedInput, IoEvent, LoaderIO, PathResolveResult, ReconnectReason, ReqId,
    ResolveResult,
};
use crate::rpc_decode::{
    artifact_layout_hash, collect_artifact_chunks, fetched_artifact, io_basis,
};
use crate::IoBasis;

#[derive(Debug)]
pub enum RpcIoInitError {
    ReconnectRequired(ReconnectReason),
    Unavailable(String),
    LoadPolicy(crate::LoadPolicyError),
}

impl std::fmt::Display for RpcIoInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "RPC loader initialization failed: {self:?}")
    }
}

impl std::error::Error for RpcIoInitError {}

pub struct RpcIo {
    commands: mpsc::UnboundedSender<Command>,
    events: sync_mpsc::Receiver<IoEvent>,
    pending: Vec<IoEvent>,
    basis: IoBasis,
    thread: Option<JoinHandle<()>>,
}

impl RpcIo {
    pub fn connect(address: SocketAddr, request: ConnectRequest) -> Result<Self, RpcIoInitError> {
        let (commands, command_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = sync_mpsc::channel();
        let (init_tx, init_rx) = sync_mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("distill-rpc-io".into())
            .spawn(move || run_thread(address, request, command_rx, event_tx, init_tx))
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
            basis,
            thread: Some(thread),
        })
    }

    fn send(&mut self, command: Command) {
        if self.commands.send(command).is_err() {
            self.pending.push(IoEvent::ConnectionError {
                message: "RPC IO thread is unavailable".into(),
            });
        }
    }
}

impl Drop for RpcIo {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl LoaderIO for RpcIo {
    fn begin_sweep(&mut self) -> IoBasis {
        let (reply, receive) = sync_mpsc::sync_channel(1);
        if self.commands.send(Command::BeginSweep { reply }).is_err() {
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
        self.pending.extend(self.events.try_iter());
        std::mem::take(&mut self.pending)
    }
}

enum Command {
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
    Shutdown,
}

fn run_thread(
    address: SocketAddr,
    request: ConnectRequest,
    commands: mpsc::UnboundedReceiver<Command>,
    events: sync_mpsc::Sender<IoEvent>,
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
        let basis = match io_basis(snapshot.basis()) {
            Ok(basis) => basis,
            Err(error) => {
                let _ = init.send(Err(RpcIoInitError::LoadPolicy(error)));
                return;
            }
        };
        if init.send(Ok(basis)).is_err() {
            return;
        }
        Driver {
            _client: client,
            hub,
            snapshot,
            commands,
            events,
            subscriptions: Rc::new(RefCell::new(Subscriptions::default())),
            delta_task: None,
        }
        .run()
        .await;
    });
}

struct Driver {
    _client: CapnpClient,
    hub: RemoteHub,
    snapshot: RemoteSnapshot,
    commands: mpsc::UnboundedReceiver<Command>,
    events: sync_mpsc::Sender<IoEvent>,
    subscriptions: Rc<RefCell<Subscriptions>>,
    delta_task: Option<tokio::task::JoinHandle<()>>,
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
            Command::BeginSweep { reply } => {
                let basis = match self.snapshot.refresh().await {
                    Ok(RemoteCall::Success(snapshot)) => {
                        self.snapshot = snapshot;
                        match io_basis(self.snapshot.basis()) {
                            Ok(basis) => Some(basis),
                            Err(error) => {
                                let _ = self.events.send(IoEvent::ConnectionError {
                                    message: format!("invalid RPC load policy: {error:?}"),
                                });
                                None
                            }
                        }
                    }
                    Ok(call) => {
                        let _ = self.events.send(connection_event(call));
                        None
                    }
                    Err(error) => {
                        let _ = self.events.send(IoEvent::ConnectionError {
                            message: error.to_string(),
                        });
                        None
                    }
                };
                let _ = reply.send(basis);
            }
            Command::Resolve { req, uuid, basis } => {
                if !self.basis_matches(&basis) {
                    let _ = self
                        .events
                        .send(request_error(req, basis, "stale RPC basis".into()));
                    return true;
                }
                let snapshot = self.snapshot.clone();
                let events = self.events.clone();
                tokio::task::spawn_local(async move {
                    let event = resolve_event(snapshot, req, uuid, basis).await;
                    let _ = events.send(event);
                });
            }
            Command::Fetch {
                req,
                content_hash,
                basis,
            } => {
                if !self.basis_matches(&basis) {
                    let _ = self
                        .events
                        .send(request_error(req, basis, "stale RPC basis".into()));
                    return true;
                }
                let hub = self.hub.clone();
                let events = self.events.clone();
                tokio::task::spawn_local(async move {
                    let event = fetch_event(hub, req, content_hash, basis).await;
                    let _ = events.send(event);
                });
            }
            Command::ResolvePath { req, path, basis } => {
                if !self.basis_matches(&basis) {
                    let _ = self
                        .events
                        .send(request_error(req, basis, "stale RPC basis".into()));
                    return true;
                }
                let snapshot = self.snapshot.clone();
                let events = self.events.clone();
                tokio::task::spawn_local(async move {
                    let event = path_event(snapshot, req, path, basis).await;
                    let _ = events.send(event);
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
            Command::Shutdown => return false,
        }
        true
    }

    fn basis_matches(&self, basis: &IoBasis) -> bool {
        io_basis(self.snapshot.basis()).as_ref() == Ok(basis)
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
                                    let subscriptions = subscriptions.borrow();
                                    for event in stream_events(
                                        event,
                                        &subscriptions.assets,
                                        &subscriptions.paths,
                                    ) {
                                        if events.send(event).is_err() {
                                            return;
                                        }
                                    }
                                }
                                Ok(None) => return,
                                Err(error) => {
                                    let _ = events.send(IoEvent::ConnectionError {
                                        message: error.to_string(),
                                    });
                                    return;
                                }
                            }
                        }
                    }));
                }
            }
            Ok(call) => {
                let _ = self.events.send(connection_event(call));
            }
            Err(error) => {
                let _ = self.events.send(IoEvent::ConnectionError {
                    message: error.to_string(),
                });
            }
        }
    }

    async fn unsubscribe(&self, assets: Vec<AssetUuid>, paths: Vec<String>) {
        match self.hub.unsubscribe(assets, paths).await {
            Ok(RemoteCall::Success(())) => {}
            Ok(call) => {
                let _ = self.events.send(connection_event(call));
            }
            Err(error) => {
                let _ = self.events.send(IoEvent::ConnectionError {
                    message: error.to_string(),
                });
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
        Ok(RemoteCall::Success(terminal)) => match io_basis(&terminal.basis) {
            Ok(basis) => IoEvent::Resolved {
                req,
                uuid,
                result: match terminal.value {
                    distill_rpc::ResolveResult::Built { content_hash } => {
                        ResolveResult::Built { content_hash }
                    }
                    distill_rpc::ResolveResult::Drifted { input, current } => {
                        ResolveResult::Drifted {
                            input: drifted_input(input),
                            current,
                        }
                    }
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
                basis,
            },
            Err(error) => {
                request_error(req, request_basis, format!("invalid RPC basis: {error:?}"))
            }
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
        Ok(RemoteCall::Success(terminal)) => match io_basis(&terminal.basis) {
            Ok(basis) => IoEvent::PathResolved {
                req,
                path,
                result: match terminal.value {
                    distill_rpc::PathResolveResult::Resolved(uuid) => {
                        PathResolveResult::Resolved(uuid)
                    }
                    distill_rpc::PathResolveResult::Missing => PathResolveResult::Missing,
                    distill_rpc::PathResolveResult::Failed(error) => PathResolveResult::Failed {
                        error: format!("{error:?}"),
                    },
                },
                basis,
            },
            Err(error) => {
                request_error(req, request_basis, format!("invalid RPC basis: {error:?}"))
            }
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
) -> IoEvent {
    let mut terminal = match hub.fetch(content_hash).await {
        Ok(RemoteCall::Success(terminal)) => terminal,
        Ok(call) => return remote_request_event(call, req, request_basis),
        Err(error) => return request_error(req, request_basis, error.to_string()),
    };
    let basis = match io_basis(&terminal.basis) {
        Ok(basis) => basis,
        Err(error) => {
            return request_error(req, request_basis, format!("invalid RPC basis: {error:?}"))
        }
    };
    let mut chunks = Vec::new();
    loop {
        match terminal.value.next_chunk().await {
            Ok(Some(chunk)) => chunks.push(chunk),
            Ok(None) => break,
            Err(error) => return request_error(req, request_basis, error.to_string()),
        }
    }
    let (structural, blobs) = match collect_artifact_chunks(chunks) {
        Ok(parts) => parts,
        Err(error) => return request_error(req, request_basis, error),
    };
    let layout_hash = match artifact_layout_hash(content_hash, &structural, &blobs) {
        Ok(layout_hash) => layout_hash,
        Err(error) => return request_error(req, request_basis, error),
    };
    let wire_layout = match hub.wire_tree(layout_hash).await {
        Ok(RemoteCall::Success(bytes)) => bytes,
        Ok(call) => return remote_request_event(call, req, request_basis),
        Err(error) => return request_error(req, request_basis, error.to_string()),
    };
    match fetched_artifact(layout_hash, structural, blobs, wire_layout) {
        Ok(artifact) => IoEvent::Fetched {
            req,
            content_hash,
            artifact,
            basis,
        },
        Err(error) => request_error(req, request_basis, error),
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
        RemoteCall::AttestationExpansionRequired(expansion) => {
            format!(
                "RPC attestation expansion required for {:?}",
                expansion.required
            )
        }
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
        distill_rpc::ReconnectReason::LoadPolicyChanged => ReconnectReason::LoadPolicyChanged,
        distill_rpc::ReconnectReason::CompiledAttestationChanged => {
            ReconnectReason::CompiledAttestationChanged
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
