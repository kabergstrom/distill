//! Concrete Cap'n Proto transport adapter.
//!
//! Protocol state remains in the transport-neutral [`crate::Server`]. This
//! module validates wire widths, translates typed result unions, and drives
//! capnp-rpc.
//!
//! Server side: one [`StagedListener`] accepts connections, and each one is
//! served on a thread of its own, with its own current-thread Tokio runtime
//! and [`tokio::task::LocalSet`] running that connection's `RpcSystem`.
//! `RpcSystem` and every capability are `!Send` and stay on that thread,
//! with the connection's front end (store reader, snapshots, subscription
//! queue, and the store writer it opens on its first write): nothing a
//! connection does, a slow SQLite read or a write waiting on the write lock
//! included, waits on another connection. Writes, import publications and
//! operation completions run inline on that writer; importer runs and lazy
//! builds go to a blocking worker so the connection keeps serving. The
//! listener itself needs no `LocalSet`. Threads are bounded by
//! `max_connections` (one past it is closed unserved), exit when their
//! connection closes, and are told to close by
//! [`StagedListener::shutdown`]. Closing shuts the socket down and lets
//! capnp-rpc run its disconnect, which releases every capability the
//! connection's pending calls hold; dropping a live `RpcSystem` would leak
//! them. A panic ends only its own thread.
//!
//! Client side: [`CapnpClient`] runs its `RpcSystem` as a task on the
//! caller's `LocalSet`; callers drive it on that one thread.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch, Notify};
use tokio::task::JoinHandle;
use tokio_util::compat::TokioAsyncReadCompatExt;
use unicode_normalization::UnicodeNormalization;

use crate::{
    AssetDeltaState, AssetEvent, AssetQuery, AssetUuid, AuthoringEntryRole, AuthoringInspectResult,
    AuthoringInspection, AuthoringSnapshot, BundleUuid, ChunkStream,
    ConfigurationError, ConfigurationStatus, ConnectError, ConnectOutcome, ConnectRequest,
    ContentHash, Delta, DeltaStream, DriftedInput, Hub, InputVersion, LayoutHash,
    MetadataAuthoringSnapshot, MetadataCall, MetadataConnectOutcome, MetadataDiagnostics,
    MetadataEntry, MetadataHub, MetadataNamespaceCall, MetadataReconnectReason, MetadataSnapshot,
    PathResolveFailure, PathResolveResult, ProgressStream,
    PureMetadataEntry, PureMetadataQuery, ReconnectReason, ResolveResult, Root, RpcBasis,
    RpcFailure, RpcResult, Snapshot, SnapshotStamp, StoreInstanceId, TagSelector,
    TargetDefinitionHash, TypeUuid, NamespaceError, NamespaceErrorV1,
};

pub use crate::distill_rpc_capnp as schema;

const WIRE_INVALID_UUID: u16 = 1001;
const WIRE_INVALID_HASH: u16 = 1002;
const WIRE_INVALID_INSTANCE: u16 = 1003;
const WIRE_INVALID_UTF8: u16 = 1004;
const WIRE_INVALID_VALUE: u16 = 1005;
const RPC_FAILURE: u16 = 3000;
/// `RpcError.code` of a fetch whose artifact left the CAS: a cache miss,
/// retried at a new snapshot.
pub const ARTIFACT_NOT_FOUND: u16 = 3001;
/// `RpcError.code` of a call on a connection closed to admit a newer one.
pub const CONNECTION_CLOSED: u16 = 3002;
/// `RpcError.code` of an `entry` whose asset has no runtime entry (absent,
/// authoring-only, or a derived output).
pub const ASSET_NOT_FOUND: u16 = 3003;
/// `RpcError.code` of a `Root.connect` or `Root.metadata` refused because
/// `max_connections` connections are open: retry later.
pub const CONNECTION_LIMIT: u16 = 3004;

fn failure_code(error: &RpcFailure) -> u16 {
    match error {
        RpcFailure::ArtifactNotFound { .. } => ARTIFACT_NOT_FOUND,
        RpcFailure::ConnectionClosed => CONNECTION_CLOSED,
        RpcFailure::AssetNotFound { .. } => ASSET_NOT_FOUND,
        _ => RPC_FAILURE,
    }
}

#[derive(Debug)]
pub enum TransportError {
    BindValidation(crate::BindStageError),
    Io(io::Error),
    Capnp(capnp::Error),
    /// The listener is serving `limit` connections, or shutting down.
    Refused { limit: usize },
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BindValidation(error) => write!(f, "RPC bind validation failed: {error}"),
            Self::Io(error) => write!(f, "RPC I/O failed: {error}"),
            Self::Capnp(error) => write!(f, "Cap'n Proto RPC failed: {error}"),
            Self::Refused { limit } => {
                write!(f, "RPC connection refused: {limit} connections are open or the listener is stopping")
            }
        }
    }
}

impl std::error::Error for TransportError {}

impl From<io::Error> for TransportError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<capnp::Error> for TransportError {
    fn from(value: capnp::Error) -> Self {
        Self::Capnp(value)
    }
}

/// How long [`StagedListener::serve_until`] waits, after its shutdown
/// signal, for connection threads to exit before it leaves the rest
/// running.
pub const CONNECTION_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

/// How long a connection being closed may take to run capnp-rpc's
/// disconnect once its socket is shut down.
const CONNECTION_DISCONNECT_GRACE: Duration = Duration::from_millis(250);

/// A listener whose address passed the loopback-only configuration staging
/// gate before any socket was opened. Every connection it accepts is served
/// on a thread of its own.
pub struct StagedListener {
    listener: TcpListener,
    root: Root,
    connections: Arc<ConnectionThreads>,
}

/// The connection threads of one listener.
struct ConnectionThreads {
    /// How many are running. Admission increments it under
    /// `max_connections`; each thread decrements it as it exits.
    live: watch::Sender<usize>,
    /// Set once, by shutdown: every thread closes its connection and exits.
    stop: watch::Sender<bool>,
}

/// One running connection thread's place among its listener's.
struct LiveThread(Arc<ConnectionThreads>);

impl Drop for LiveThread {
    fn drop(&mut self) {
        self.0.live.send_modify(|live| *live -= 1);
    }
}

/// A connection thread panicked; the daemon and every other connection
/// carry on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionPanicked(pub String);

type ConnectionOutcome = Result<Result<(), capnp::Error>, ConnectionPanicked>;

/// A connection served on its own thread. Await it for the connection's
/// outcome, reported once the thread has released everything the
/// connection held; dropping it detaches the thread.
pub struct ConnectionHandle {
    done: oneshot::Receiver<ConnectionOutcome>,
    close: Arc<Notify>,
}

impl ConnectionHandle {
    /// Close the connection: its thread drops the RPC system, closing the
    /// socket, and exits.
    pub fn close(&self) {
        self.close.notify_one();
    }
}

impl Future for ConnectionHandle {
    type Output = ConnectionOutcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.done).poll(cx).map(|done| {
            done.unwrap_or_else(|_| {
                Err(ConnectionPanicked(
                    "the connection thread exited without an outcome".to_owned(),
                ))
            })
        })
    }
}

impl StagedListener {
    pub async fn bind(root: Root, address: &str) -> Result<Self, TransportError> {
        let address =
            crate::validate_bind_address(address).map_err(TransportError::BindValidation)?;
        let listener = TcpListener::bind(address).await?;
        Ok(Self {
            listener,
            root,
            connections: Arc::new(ConnectionThreads {
                live: watch::Sender::new(0),
                stop: watch::Sender::new(false),
            }),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// How many connection threads are running.
    pub fn connections(&self) -> usize {
        *self.connections.live.borrow()
    }

    /// Accept one connection and serve it on a thread of its own.
    pub async fn accept_one(&self) -> Result<ConnectionHandle, TransportError> {
        let (stream, peer) = self.listener.accept().await?;
        self.start(stream, peer)
    }

    /// Serve an accepted connection on a new thread with its own
    /// current-thread runtime and `LocalSet`: its `RpcSystem` and every
    /// capability it hands out live and die there. Past `max_connections`
    /// running threads, or after shutdown, the connection is closed
    /// unserved.
    fn start(&self, stream: TcpStream, peer: SocketAddr) -> Result<ConnectionHandle, TransportError> {
        if !peer.ip().is_loopback() {
            return Err(TransportError::BindValidation(
                crate::BindStageError::NonLoopbackAddress {
                    address: peer.to_string(),
                },
            ));
        }
        let limit = self.root.handle.snapshot_policy().max_connections;
        let stopped = *self.connections.stop.borrow();
        let admitted = !stopped
            && self.connections.live.send_if_modified(|live| {
                let admit = *live < limit;
                *live += usize::from(admit);
                admit
            });
        if !admitted {
            return Err(TransportError::Refused { limit });
        }
        let live = LiveThread(Arc::clone(&self.connections));
        stream.set_nodelay(true)?;
        let stream = stream.into_std()?;
        let (done_tx, done) = oneshot::channel();
        let close = Arc::new(Notify::new());
        let thread_close = Arc::clone(&close);
        let stop = self.connections.stop.subscribe();
        let root = self.root.clone();
        std::thread::Builder::new()
            .name(format!("distill-rpc {peer}"))
            .spawn(move || {
                let outcome = serve_connection_thread(stream, root, stop, thread_close);
                if let Err(ConnectionPanicked(message)) = &outcome {
                    tracing::error!(%peer, %message, "RPC connection thread panicked");
                }
                drop(live);
                let _ = done_tx.send(outcome);
            })?;
        Ok(ConnectionHandle { done, close })
    }

    /// Accept and drive one connection to completion.
    pub async fn serve_one(&self) -> Result<(), TransportError> {
        match self.accept_one().await?.await {
            Ok(result) => result.map_err(TransportError::Capnp),
            Err(ConnectionPanicked(message)) => Err(TransportError::Capnp(capnp::Error::failed(
                format!("RPC connection thread panicked: {message}"),
            ))),
        }
    }

    /// Production accept loop: every connection on a thread of its own.
    pub async fn serve(&self) -> Result<(), TransportError> {
        self.serve_until(std::future::pending()).await
    }

    /// Production accept loop with supervisor-owned shutdown. A connection
    /// that cannot be served (refused past `max_connections`, or failing to
    /// start) is logged and closed; only a failing listener ends the loop.
    /// On shutdown the listener stops accepting and closes every
    /// connection, waiting at most [`CONNECTION_SHUTDOWN_GRACE`] for their
    /// threads.
    pub async fn serve_until<F>(&self, shutdown: F) -> Result<(), TransportError>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let result = loop {
            tokio::select! {
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, peer)) => match self.start(stream, peer) {
                        // The thread runs detached; shutdown reaches it.
                        Ok(connection) => drop(connection),
                        Err(error) => tracing::warn!(%peer, %error, "RPC connection not served"),
                    },
                    Err(error) => break Err(TransportError::Io(error)),
                },
                () = &mut shutdown => break Ok(()),
            }
        };
        self.shutdown(CONNECTION_SHUTDOWN_GRACE).await;
        result
    }

    /// Close every connection this listener serves and refuse new ones,
    /// then wait up to `grace` for their threads to exit. A thread blocked
    /// in a synchronous call exits when the call returns; returns how many
    /// were still running.
    pub async fn shutdown(&self, grace: Duration) -> usize {
        self.connections.stop.send_replace(true);
        let mut live = self.connections.live.subscribe();
        let _ = tokio::time::timeout(grace, live.wait_for(|live| *live == 0)).await;
        let remaining = *live.borrow();
        if remaining > 0 {
            tracing::warn!(remaining, "RPC connection threads still running after shutdown");
        }
        remaining
    }
}

/// The body of one connection thread: serve the connection until it
/// closes, the listener shuts down, or its handle closes it. Everything
/// the connection held is released before this returns; blocking workers
/// still running (a lazy build) finish on their own.
fn serve_connection_thread(
    stream: std::net::TcpStream,
    root: Root,
    mut stop: watch::Receiver<bool>,
    close: Arc<Notify>,
) -> ConnectionOutcome {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return Ok(Err(capnp::Error::failed(format!(
                "cannot start the connection runtime: {error}"
            ))))
        }
    };
    let local = tokio::task::LocalSet::new();
    let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        local.block_on(&runtime, async move {
            let socket = stream
                .try_clone()
                .map_err(|error| capnp::Error::failed(format!("connection socket: {error}")))?;
            let stream = TcpStream::from_std(stream)
                .map_err(|error| capnp::Error::failed(format!("connection socket: {error}")))?;
            let root_client: schema::root::Client = capnp_rpc::new_client(RootService { root });
            let stopped = async {
                // A dropped listener stops nothing: its threads keep serving.
                if stop.wait_for(|stopped| *stopped).await.is_err() {
                    std::future::pending::<()>().await;
                }
            };
            let rpc = run_rpc_system(stream, Some(root_client.client));
            tokio::pin!(rpc);
            tokio::select! {
                result = &mut rpc => return result,
                () = stopped => {}
                () = close.notified() => {}
            }
            // Close by ending the stream, not by dropping the RPC system:
            // a dropped system leaks the capabilities its pending calls
            // hold (and with them this connection's reader and admission),
            // while end-of-stream runs capnp-rpc's disconnect, which
            // releases them.
            let _ = socket.shutdown(std::net::Shutdown::Both);
            let _ = tokio::time::timeout(CONNECTION_DISCONNECT_GRACE, rpc).await;
            Ok(())
        })
    }));
    drop(local);
    runtime.shutdown_background();
    served.map_err(|payload| {
        ConnectionPanicked(
            payload
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_owned()),
        )
    })
}

/// Remote bootstrap client plus the local task driving its single-threaded
/// capnp-rpc system.
pub struct CapnpClient {
    root: schema::root::Client,
    rpc_task: JoinHandle<Result<(), capnp::Error>>,
}

impl CapnpClient {
    pub async fn connect_local(address: SocketAddr) -> Result<Self, TransportError> {
        if !address.ip().is_loopback() {
            return Err(TransportError::BindValidation(
                crate::BindStageError::NonLoopbackAddress {
                    address: address.to_string(),
                },
            ));
        }
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.compat().split();
        let network = Box::new(twoparty::VatNetwork::new(
            BufReader::new(reader),
            BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let mut rpc_system = RpcSystem::new(network, None);
        let root = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
        let rpc_task = tokio::task::spawn_local(rpc_system);
        Ok(Self { root, rpc_task })
    }

    pub fn root(&self) -> &schema::root::Client {
        &self.root
    }

    pub async fn connect(
        &self,
        request: &ConnectRequest,
    ) -> Result<RemoteConnectOutcome, capnp::Error> {
        let mut call = self.root.connect_request();
        write_connect_request(call.get(), request);
        let response = call.send().promise.await?;
        decode_connect_response(response.get()?.get_result()?, request)
    }

    pub async fn metadata(&self, protocol: u32) -> Result<RemoteMetadataOutcome, capnp::Error> {
        let mut call = self.root.metadata_request();
        call.get().set_protocol(protocol);
        let response = call.send().promise.await?;
        decode_metadata_response(response.get()?.get_result()?)
    }
}

impl Drop for CapnpClient {
    fn drop(&mut self) {
        self.rpc_task.abort();
    }
}

pub enum RemoteConnectOutcome {
    Connected {
        hub: schema::hub::Client,
        instance: StoreInstanceId,
    },
    ConfigurationFailed(ConfigurationError),
    PipelineUnavailable(crate::PipelineUnavailableDiagnostic),
    TargetFailure(ConnectError),
    ProtocolFailure {
        expected: u32,
        observed: u32,
        message: String,
    },
    Error {
        code: u16,
        message: String,
    },
}

pub enum RemoteMetadataOutcome {
    Connected {
        hub: schema::metadata_hub::Client,
        instance: StoreInstanceId,
        protocol_epoch: u32,
    },
    ProtocolFailure {
        expected: u32,
        observed: u32,
        message: String,
    },
    Error {
        code: u16,
        message: String,
    },
}

impl fmt::Debug for RemoteConnectOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected { instance, .. } => f
                .debug_struct("Connected")
                .field("instance", instance)
                .finish_non_exhaustive(),
            Self::ConfigurationFailed(error) => f
                .debug_tuple("ConfigurationFailed")
                .field(error)
                .finish(),
            Self::PipelineUnavailable(diagnostic) => f
                .debug_tuple("PipelineUnavailable")
                .field(diagnostic)
                .finish(),
            Self::TargetFailure(failure) => f.debug_tuple("TargetFailure").field(failure).finish(),
            Self::ProtocolFailure {
                expected,
                observed,
                message,
            } => f
                .debug_struct("ProtocolFailure")
                .field("expected", expected)
                .field("observed", observed)
                .field("message", message)
                .finish(),
            Self::Error { code, message } => f
                .debug_struct("Error")
                .field("code", code)
                .field("message", message)
                .finish(),
        }
    }
}

async fn run_rpc_system(
    stream: TcpStream,
    bootstrap: Option<capnp::capability::Client>,
) -> Result<(), capnp::Error> {
    let (reader, writer) = stream.compat().split();
    let network = Box::new(twoparty::VatNetwork::new(
        BufReader::new(reader),
        BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Server,
        Default::default(),
    ));
    RpcSystem::new(network, bootstrap).await
}

struct RootService {
    root: Root,
}

// Keep generated RPITIT signatures verbatim; this makes schema/runtime version
// drift a compile error instead of relying on async-fn desugaring equivalence.
#[allow(clippy::manual_async_fn)]
impl schema::root::Server for RootService {
    fn connect(
        self: capnp::capability::Rc<Self>,
        params: schema::root::ConnectParams,
        mut results: schema::root::ConnectResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let reader = params.get()?;
            let decoded = decode_connect_request(reader);
            let result = results.get().init_result();
            let request = match decoded {
                Ok(request) => request,
                Err(DecodeError::Wire(error)) => {
                    write_wire_error(result.init_error(), &error);
                    return Ok(());
                }
                Err(DecodeError::Capnp(error)) => return Err(error),
            };
            match self.root.connect(request) {
                ConnectOutcome::Connected(connected) => {
                    let mut output = result.init_success();
                    let hub: schema::hub::Client =
                        capnp_rpc::new_client(HubService { hub: connected.hub });
                    output.set_hub(hub);
                    output.set_instance(&connected.instance.0);
                }
                ConnectOutcome::ConfigurationFailed(error) => {
                    write_configuration_error(result.init_configuration_failed(), &error);
                }
                ConnectOutcome::PipelineUnavailable(diagnostic) => {
                    write_pipeline_unavailable(result.init_pipeline_unavailable(), &diagnostic)?;
                }
                ConnectOutcome::Rejected(error) => {
                    write_connect_error(result, &error);
                }
                ConnectOutcome::Refused(failure) => {
                    write_error(result.init_error(), CONNECTION_LIMIT, &format!("{failure:?}"));
                }
            }
            Ok(())
        }
    }

    fn metadata(
        self: capnp::capability::Rc<Self>,
        params: schema::root::MetadataParams,
        mut results: schema::root::MetadataResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let protocol = params.get()?.get_protocol();
            let mut result = results.get().init_result();
            match self.root.metadata(protocol) {
                MetadataConnectOutcome::Connected(connected) => {
                    let mut output = result.reborrow().init_success();
                    let hub: schema::metadata_hub::Client =
                        capnp_rpc::new_client(MetadataHubService { hub: connected.hub });
                    output.set_hub(hub);
                    output.set_instance(&connected.instance.0);
                    output.set_protocol_epoch(connected.protocol_epoch);
                }
                MetadataConnectOutcome::ProtocolMismatch { expected, observed } => {
                    let mut failure = result.init_protocol_failure();
                    failure.set_expected(expected);
                    failure.set_observed(observed);
                    failure.set_message("metadata bootstrap protocol mismatch");
                }
                MetadataConnectOutcome::Refused(failure) => {
                    write_error(result.init_error(), CONNECTION_LIMIT, &format!("{failure:?}"));
                }
            }
            Ok(())
        }
    }

}

struct MetadataHubService {
    hub: MetadataHub,
}

#[allow(clippy::manual_async_fn)]
impl schema::metadata_hub::Server for MetadataHubService {
    fn snapshot(
        self: capnp::capability::Rc<Self>,
        _params: schema::metadata_hub::SnapshotParams,
        mut results: schema::metadata_hub::SnapshotResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_metadata_snapshot_result(results.get().init_result(), self.hub.snapshot());
            Ok(())
        }
    }

    fn authoring_snapshot(
        self: capnp::capability::Rc<Self>,
        _params: schema::metadata_hub::AuthoringSnapshotParams,
        mut results: schema::metadata_hub::AuthoringSnapshotResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_metadata_authoring_snapshot_result(
                results.get().init_result(),
                self.hub.authoring_snapshot(),
            );
            Ok(())
        }
    }

    fn diagnostics(
        self: capnp::capability::Rc<Self>,
        _params: schema::metadata_hub::DiagnosticsParams,
        mut results: schema::metadata_hub::DiagnosticsResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_metadata_diagnostics_result(results.get().init_result(), self.hub.diagnostics())?;
            Ok(())
        }
    }

    fn fetch(
        self: capnp::capability::Rc<Self>,
        params: schema::metadata_hub::FetchParams,
        mut results: schema::metadata_hub::FetchResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let hash = match decode_hash(params.get()?.get_hash()?, "hash") {
                Ok(hash) => ContentHash(hash),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_metadata_fetch_result(results.get().init_result(), self.hub.fetch(hash));
            Ok(())
        }
    }

}

struct HubService {
    hub: Hub,
}

#[allow(clippy::manual_async_fn)]
impl schema::hub::Server for HubService {
    fn snapshot(
        self: capnp::capability::Rc<Self>,
        _params: schema::hub::SnapshotParams,
        mut results: schema::hub::SnapshotResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            write_snapshot_result(results.get().init_result(), self.hub.snapshot());
            Ok(())
        }
    }

    fn subscribe(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::SubscribeParams,
        mut results: schema::hub::SubscribeResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let reader = params.get()?;
            let assets = match decode_uuid_list(reader.get_assets()?, "assets") {
                Ok(assets) => assets,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            let paths = match decode_text_list(reader.get_paths()?, "paths") {
                Ok(paths) => paths,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            let result = self
                .hub
                .subscribe(InputVersion(reader.get_since()), assets, paths);
            write_subscribe_result(results.get().init_result(), result);
            Ok(())
        }
    }

    fn write(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::WriteParams,
        mut results: schema::hub::WriteResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let params = params.get()?;
            let ops = match decode_authoring_ops(params.get_ops()?) {
                Ok(ops) => ops,
                Err(error) => {
                    write_error(
                        results.get().init_result().init_error(),
                        WIRE_INVALID_VALUE,
                        &format!("invalid authoring operations: {error}"),
                    );
                    return Ok(());
                }
            };
            // A publication waits on SQLite's write lock on this
            // connection's own thread; no other connection waits with it.
            let result = self.hub.write(
                InputVersion(params.get_base()),
                ops,
                params.get_force_lossy(),
            );
            write_uint64_result(
                results.get().init_result(),
                result.map_success(|version| version.0),
            );
            Ok(())
        }
    }

    fn import(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::ImportParams,
        mut results: schema::hub::ImportResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let params = params.get()?;
            let request = match decode_import_request(params.get_request()?) {
                Ok(request) => request,
                Err(error) => {
                    write_error(
                        results.get().init_result().init_error(),
                        WIRE_INVALID_VALUE,
                        &format!("invalid import request: {error}"),
                    );
                    return Ok(());
                }
            };
            let result = match self.hub.import_prepare(InputVersion(params.get_base()), request) {
                // The importer runs on a blocking worker, never on the
                // single-threaded capnp-rpc driver.
                Ok(pending) => {
                    let finished = tokio::task::spawn_blocking(move || pending.run())
                        .await
                        .map_err(|error| {
                            capnp::Error::failed(format!("import worker failed: {error}"))
                        })?;
                    self.hub.import_finish(finished)
                }
                Err(result) => result,
            };
            write_bundle_uuid_result(results.get().init_result(), result);
            Ok(())
        }
    }

    fn reimport(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::ReimportParams,
        mut results: schema::hub::ReimportResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let params = params.get()?;
            let bundle = match decode_uuid(params.get_bundle()?.get_bytes()?, "bundle") {
                Ok(bundle) => BundleUuid(bundle),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            let result = match self.hub.reimport_prepare(InputVersion(params.get_base()), bundle) {
                // The importer runs on a blocking worker, never on the
                // single-threaded capnp-rpc driver.
                Ok(pending) => {
                    let finished = tokio::task::spawn_blocking(move || pending.run())
                        .await
                        .map_err(|error| {
                            capnp::Error::failed(format!("import worker failed: {error}"))
                        })?;
                    self.hub.import_finish(finished)
                }
                Err(result) => result,
            };
            write_bundle_uuid_result(results.get().init_result(), result);
            Ok(())
        }
    }

    fn operation(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::OperationParams,
        mut results: schema::hub::OperationResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let params = params.get()?;
            let operation = match decode_long_running_op(params.get_operation()?) {
                Ok(operation) => operation,
                Err(error) => {
                    write_error(
                        results.get().init_result().init_error(),
                        WIRE_INVALID_VALUE,
                        &format!("invalid long-running operation: {error}"),
                    );
                    return Ok(());
                }
            };
            write_progress_result(
                results.get().init_result(),
                self.hub
                    .operation(InputVersion(params.get_base()), operation),
            );
            Ok(())
        }
    }

    fn wire_tree(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::WireTreeParams,
        mut results: schema::hub::WireTreeResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let hash = match decode_hash(params.get()?.get_layout_hash()?, "layoutHash") {
                Ok(hash) => LayoutHash(hash),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_data_result(results.get().init_result(), self.hub.wire_tree(hash));
            Ok(())
        }
    }

    fn unsubscribe(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::UnsubscribeParams,
        mut results: schema::hub::UnsubscribeResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let reader = params.get()?;
            let assets = match decode_uuid_list(reader.get_assets()?, "assets") {
                Ok(assets) => assets,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            let paths = match decode_text_list(reader.get_paths()?, "paths") {
                Ok(paths) => paths,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_void_result(
                results.get().init_result(),
                self.hub.unsubscribe(assets, paths),
            );
            Ok(())
        }
    }

    fn authoring_snapshot(
        self: capnp::capability::Rc<Self>,
        _params: schema::hub::AuthoringSnapshotParams,
        mut results: schema::hub::AuthoringSnapshotResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            write_authoring_snapshot_result(
                results.get().init_result(),
                self.hub.authoring_snapshot(),
            );
            Ok(())
        }
    }

    fn import_failures(
        self: capnp::capability::Rc<Self>,
        _params: schema::hub::ImportFailuresParams,
        mut results: schema::hub::ImportFailuresResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            write_import_failures_result(results.get().init_result(), self.hub.import_failures());
            Ok(())
        }
    }
}

struct SnapshotService {
    snapshot: Snapshot,
}

#[allow(clippy::manual_async_fn)]
impl schema::snapshot::Server for SnapshotService {
    fn version(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::VersionParams,
        mut results: schema::snapshot::VersionResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            write_uint64_result(
                results.get().init_result(),
                self.snapshot.version().map_success(|version| version.0),
            );
            Ok(())
        }
    }

    fn query(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::QueryParams,
        mut results: schema::snapshot::QueryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let query = match decode_asset_query(params.get()?.get_query()?) {
                Ok(query) => query,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_uuid_list_result(results.get().init_result(), self.snapshot.query(query));
            Ok(())
        }
    }

    fn entry(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::EntryParams,
        mut results: schema::snapshot::EntryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let uuid = match decode_uuid(params.get()?.get_uuid()?, "uuid") {
                Ok(uuid) => AssetUuid(uuid),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_target_entry_result(results.get().init_result(), self.snapshot.entry(uuid));
            Ok(())
        }
    }

    fn resolve(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::ResolveParams,
        mut results: schema::snapshot::ResolveResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let params = params.get()?;
            let uuid = match decode_uuid(params.get_uuid()?, "uuid") {
                Ok(uuid) => AssetUuid(uuid),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            let work_class = if params.get_batch() {
                crate::BuildWorkClass::Batch
            } else {
                crate::BuildWorkClass::Interactive
            };
            // Lazy resolution may synchronously execute a complete processor
            // chain. Run it on a blocking worker so this connection's other
            // calls (pipelined resolves, fetches, its delta stream) keep
            // moving; the daemon build scheduler bounds the actual work.
            let outcome = match self.snapshot.resolve_prepare(uuid, work_class) {
                crate::ResolveStep::Done(outcome) => outcome,
                crate::ResolveStep::Build(build) => {
                    let finished = tokio::task::spawn_blocking(move || build.run())
                        .await
                        .map_err(|error| {
                            capnp::Error::failed(format!(
                                "snapshot resolve worker failed: {error}"
                            ))
                        })?;
                    self.snapshot.resolve_finish(uuid, finished)
                }
            };
            write_resolve_result(results.get().init_result(), outcome);
            Ok(())
        }
    }

    fn refresh(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::RefreshParams,
        mut results: schema::snapshot::RefreshResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            write_snapshot_result(results.get().init_result(), self.snapshot.refresh());
            Ok(())
        }
    }

    fn resolve_path(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::ResolvePathParams,
        mut results: schema::snapshot::ResolvePathResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let path = match decode_text(params.get()?.get_path()?, "path") {
                Ok(path) => path,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_path_result(
                results.get().init_result(),
                self.snapshot.resolve_path(&path),
            );
            Ok(())
        }
    }

    fn configuration(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::ConfigurationParams,
        mut results: schema::snapshot::ConfigurationResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            write_snapshot_configuration_result(
                results.get().init_result(),
                self.snapshot.configuration(),
            );
            Ok(())
        }
    }

    fn fetch(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::FetchParams,
        mut results: schema::snapshot::FetchResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let hash = match decode_hash(params.get()?.get_hash()?, "hash") {
                Ok(hash) => ContentHash(hash),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_fetch_result(results.get().init_result(), self.snapshot.fetch(hash));
            Ok(())
        }
    }

    fn runtime_type_policy(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::RuntimeTypePolicyParams,
        mut results: schema::snapshot::RuntimeTypePolicyResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let type_uuid = match decode_uuid(params.get()?.get_type_uuid()?, "typeUuid") {
                Ok(uuid) => TypeUuid(uuid),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_runtime_type_policy_result(
                results.get().init_result(),
                self.snapshot.runtime_type_policy(type_uuid),
            );
            Ok(())
        }
    }

    fn resolve_named(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::ResolveNamedParams,
        mut results: schema::snapshot::ResolveNamedResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let params = params.get()?;
            let (path, name) = (params.get_path()?, params.get_name()?);
            let decoded = decode_text(path, "path")
                .and_then(|path| Ok((path, decode_text(name, "name")?)));
            let (path, name) = match decoded {
                Ok(decoded) => decoded,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_path_result(
                results.get().init_result(),
                self.snapshot.resolve_named(&path, &name),
            );
            Ok(())
        }
    }
}

struct AuthoringSnapshotService {
    snapshot: AuthoringSnapshot,
}

#[allow(clippy::manual_async_fn)]
impl schema::authoring_snapshot::Server for AuthoringSnapshotService {
    fn version(
        self: capnp::capability::Rc<Self>,
        _params: schema::authoring_snapshot::VersionParams,
        mut results: schema::authoring_snapshot::VersionResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            write_uint64_result(
                results.get().init_result(),
                self.snapshot.version().map_success(|version| version.0),
            );
            Ok(())
        }
    }

    fn query(
        self: capnp::capability::Rc<Self>,
        params: schema::authoring_snapshot::QueryParams,
        mut results: schema::authoring_snapshot::QueryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let query = match decode_asset_query(params.get()?.get_query()?) {
                Ok(query) => query,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_uuid_list_result(results.get().init_result(), self.snapshot.query(query));
            Ok(())
        }
    }

    fn inspect(
        self: capnp::capability::Rc<Self>,
        params: schema::authoring_snapshot::InspectParams,
        mut results: schema::authoring_snapshot::InspectResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            let uuid = match decode_uuid(params.get()?.get_uuid()?, "uuid") {
                Ok(uuid) => AssetUuid(uuid),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_authoring_inspect_result(
                results.get().init_result(),
                self.snapshot.inspect(uuid),
            );
            Ok(())
        }
    }

    fn refresh(
        self: capnp::capability::Rc<Self>,
        _params: schema::authoring_snapshot::RefreshParams,
        mut results: schema::authoring_snapshot::RefreshResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.snapshot.generation_reconnect() {
                write_reconnect(
                    results.get().init_result().init_reconnect_required(),
                    reason,
                );
                return Ok(());
            }
            write_authoring_snapshot_result(results.get().init_result(), self.snapshot.refresh());
            Ok(())
        }
    }
}

struct MetadataSnapshotService {
    snapshot: MetadataSnapshot,
}

#[allow(clippy::manual_async_fn)]
impl schema::metadata_snapshot::Server for MetadataSnapshotService {
    fn version(
        self: capnp::capability::Rc<Self>,
        _params: schema::metadata_snapshot::VersionParams,
        mut results: schema::metadata_snapshot::VersionResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_metadata_uint64_result(
                results.get().init_result(),
                self.snapshot.version().map_success(|version| version.0),
            );
            Ok(())
        }
    }

    fn diagnostics(
        self: capnp::capability::Rc<Self>,
        _params: schema::metadata_snapshot::DiagnosticsParams,
        mut results: schema::metadata_snapshot::DiagnosticsResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_metadata_diagnostics_result(
                results.get().init_result(),
                self.snapshot.diagnostics(),
            )?;
            Ok(())
        }
    }

    fn query(
        self: capnp::capability::Rc<Self>,
        params: schema::metadata_snapshot::QueryParams,
        mut results: schema::metadata_snapshot::QueryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let query = match decode_pure_metadata_query(params.get()?.get_q()?) {
                Ok(query) => query,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_metadata_uuid_list_result(
                results.get().init_result(),
                self.snapshot.query(&query),
            );
            Ok(())
        }
    }

    fn entry(
        self: capnp::capability::Rc<Self>,
        params: schema::metadata_snapshot::EntryParams,
        mut results: schema::metadata_snapshot::EntryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let uuid = match decode_uuid(params.get()?.get_uuid()?.get_bytes()?, "uuid") {
                Ok(uuid) => AssetUuid(uuid),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_metadata_entry_result(results.get().init_result(), self.snapshot.entry(uuid));
            Ok(())
        }
    }

    fn resolve_path(
        self: capnp::capability::Rc<Self>,
        params: schema::metadata_snapshot::ResolvePathParams,
        mut results: schema::metadata_snapshot::ResolvePathResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let path = match decode_text(params.get()?.get_path()?, "path") {
                Ok(path) => path,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_metadata_path_result(
                results.get().init_result(),
                self.snapshot.resolve_path(&path),
            );
            Ok(())
        }
    }

    fn refresh(
        self: capnp::capability::Rc<Self>,
        _params: schema::metadata_snapshot::RefreshParams,
        mut results: schema::metadata_snapshot::RefreshResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_metadata_snapshot_result(results.get().init_result(), self.snapshot.refresh());
            Ok(())
        }
    }
}

struct MetadataAuthoringSnapshotService {
    snapshot: MetadataAuthoringSnapshot,
}

#[allow(clippy::manual_async_fn)]
impl schema::metadata_authoring_snapshot::Server for MetadataAuthoringSnapshotService {
    fn version(
        self: capnp::capability::Rc<Self>,
        _params: schema::metadata_authoring_snapshot::VersionParams,
        mut results: schema::metadata_authoring_snapshot::VersionResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_metadata_uint64_result(
                results.get().init_result(),
                self.snapshot.version().map_success(|version| version.0),
            );
            Ok(())
        }
    }

    fn query(
        self: capnp::capability::Rc<Self>,
        params: schema::metadata_authoring_snapshot::QueryParams,
        mut results: schema::metadata_authoring_snapshot::QueryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let query = match decode_pure_metadata_query(params.get()?.get_q()?) {
                Ok(query) => query,
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_metadata_uuid_list_result(
                results.get().init_result(),
                self.snapshot.query(&query),
            );
            Ok(())
        }
    }

    fn inspect(
        self: capnp::capability::Rc<Self>,
        params: schema::metadata_authoring_snapshot::InspectParams,
        mut results: schema::metadata_authoring_snapshot::InspectResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let uuid = match decode_uuid(params.get()?.get_uuid()?.get_bytes()?, "uuid") {
                Ok(uuid) => AssetUuid(uuid),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_metadata_authoring_inspect_result(
                results.get().init_result(),
                self.snapshot.inspect(uuid),
            );
            Ok(())
        }
    }

    fn refresh(
        self: capnp::capability::Rc<Self>,
        _params: schema::metadata_authoring_snapshot::RefreshParams,
        mut results: schema::metadata_authoring_snapshot::RefreshResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_metadata_authoring_refresh_result(
                results.get().init_result(),
                self.snapshot.refresh(),
            );
            Ok(())
        }
    }
}

struct ChunkStreamService {
    stream: RefCell<ChunkStream>,
}

struct ProgressStreamService {
    stream: RefCell<ProgressStream>,
}

#[allow(clippy::manual_async_fn)]
impl schema::progress_stream::Server for ProgressStreamService {
    fn next(
        self: capnp::capability::Rc<Self>,
        _params: schema::progress_stream::NextParams,
        mut results: schema::progress_stream::NextResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            // A completion publishes here, on this connection's writer.
            let progress = self.stream.borrow_mut().next();
            let mut output = results.get();
            match progress {
                Some(progress) => {
                    output.set_done(false);
                    write_authoring_progress(output.reborrow().init_progress(), &progress);
                }
                None => output.set_done(true),
            }
            Ok(())
        }
    }

    fn cancel(
        self: capnp::capability::Rc<Self>,
        _params: schema::progress_stream::CancelParams,
        mut results: schema::progress_stream::CancelResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let cancelled = self
                .stream
                .borrow_mut()
                .cancel();
            results.get().set_cancelled(cancelled);
            Ok(())
        }
    }
}

#[allow(clippy::manual_async_fn)]
impl schema::chunk_stream::Server for ChunkStreamService {
    fn next(
        self: capnp::capability::Rc<Self>,
        _params: schema::chunk_stream::NextParams,
        mut results: schema::chunk_stream::NextResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let chunk = self
                .stream
                .borrow_mut()
                .next_chunk();
            let mut output = results.get();
            match chunk {
                None => output.set_done(true),
                Some(chunk) => {
                    output.set_done(false);
                    match chunk.kind {
                        crate::ArtifactChunkKind::Structural => {
                            output.set_kind(0);
                            output.set_index(0);
                        }
                        crate::ArtifactChunkKind::Blob { index } => {
                            output.set_kind(1);
                            output.set_index(index);
                        }
                    }
                    output.set_offset(chunk.offset);
                    output.set_bytes(&chunk.bytes);
                }
            }
            Ok(())
        }
    }
}

struct DeltaStreamService {
    stream: DeltaStream,
}

#[allow(clippy::manual_async_fn)]
impl schema::delta_stream::Server for DeltaStreamService {
    fn next(
        self: capnp::capability::Rc<Self>,
        _params: schema::delta_stream::NextParams,
        mut results: schema::delta_stream::NextResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let mut output = results.get();
            match self.stream.next_async().await {
                Some(event) => {
                    output.set_done(false);
                    write_stream_event(output.init_event(), &event)?;
                }
                None => output.set_done(true),
            }
            Ok(())
        }
    }
}

#[derive(Debug)]
struct WireFailure {
    code: u16,
    message: String,
}

impl From<capnp::Error> for WireFailure {
    fn from(error: capnp::Error) -> Self {
        Self {
            code: WIRE_INVALID_VALUE,
            message: error.to_string(),
        }
    }
}

#[derive(Debug)]
enum DecodeError {
    Wire(WireFailure),
    Capnp(capnp::Error),
}

impl From<capnp::Error> for DecodeError {
    fn from(value: capnp::Error) -> Self {
        Self::Capnp(value)
    }
}

fn decode_connect_request(
    reader: schema::root::connect_params::Reader<'_>,
) -> Result<ConnectRequest, DecodeError> {
    let target = decode_text(reader.get_target()?, "target").map_err(DecodeError::Wire)?;
    let target_definition_hash = TargetDefinitionHash(
        decode_hash(reader.get_target_def_hash()?, "targetDefHash").map_err(DecodeError::Wire)?,
    );
    Ok(ConnectRequest {
        target,
        target_definition_hash,
        protocol: reader.get_protocol(),
    })
}

fn decode_uuid_list(
    values: capnp::data_list::Reader<'_>,
    field: &str,
) -> Result<Vec<AssetUuid>, WireFailure> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map_err(|error| WireFailure {
                    code: WIRE_INVALID_UUID,
                    message: format!("{field}[{index}] could not be read: {error}"),
                })
                .and_then(|value| decode_uuid(value, &format!("{field}[{index}]")).map(AssetUuid))
        })
        .collect()
}

fn authoring_decode_error(error: impl fmt::Display) -> String {
    error.to_string()
}

fn decode_authoring_value(
    value: schema::authoring_value::Reader<'_>,
) -> Result<crate::AuthoringValue, String> {
    Ok(crate::AuthoringValue {
        canonical_value: value
            .get_canonical_value()
            .map_err(authoring_decode_error)?
            .to_vec()
            .into(),
        blobs: value
            .get_blobs()
            .map_err(authoring_decode_error)?
            .iter()
            .map(|blob| {
                blob.map(|bytes| bytes.to_vec().into())
                    .map_err(authoring_decode_error)
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn decode_authoring_entry(
    value: schema::authoring_entry_value::Reader<'_>,
) -> Result<crate::AuthoringEntry, String> {
    let mut tags = std::collections::BTreeMap::new();
    let mut previous: Option<String> = None;
    for row in value.get_tags().map_err(authoring_decode_error)?.iter() {
        let tag = decode_text(
            row.get_tag().map_err(authoring_decode_error)?,
            "authoringOp.tag",
        )
        .map_err(|error| error.message)?;
        if previous.as_ref().is_some_and(|prior| prior >= &tag) {
            return Err("authoring tags must be strictly byte-sorted".to_owned());
        }
        previous = Some(tag.clone());
        let raw_value = decode_text(
            row.get_value().map_err(authoring_decode_error)?,
            "authoringOp.tag.value",
        )
        .map_err(|error| error.message)?;
        let tag_value = if row.get_has_value() {
            Some(raw_value)
        } else {
            if !raw_value.is_empty() {
                return Err("valueless authoring tag carries a value".to_owned());
            }
            None
        };
        tags.insert(tag, tag_value);
    }
    Ok(crate::AuthoringEntry {
        uuid: AssetUuid(
            decode_uuid(
                value
                    .get_uuid()
                    .map_err(authoring_decode_error)?
                    .get_bytes()
                    .map_err(authoring_decode_error)?,
                "authoringOp.uuid",
            )
            .map_err(|error| error.message)?,
        ),
        bundle: BundleUuid(
            decode_uuid(
                value
                    .get_bundle()
                    .map_err(authoring_decode_error)?
                    .get_bytes()
                    .map_err(authoring_decode_error)?,
                "authoringOp.bundle",
            )
            .map_err(|error| error.message)?,
        ),
        local_id: decode_text(
            value.get_local_id().map_err(authoring_decode_error)?,
            "authoringOp.localId",
        )
        .map_err(|error| error.message)?,
        normalized_path: decode_text(
            value
                .get_normalized_path()
                .map_err(authoring_decode_error)?,
            "authoringOp.normalizedPath",
        )
        .map_err(|error| error.message)?,
        type_uuid: TypeUuid(
            decode_uuid(
                value
                    .get_type_uuid()
                    .map_err(authoring_decode_error)?
                    .get_bytes()
                    .map_err(authoring_decode_error)?,
                "authoringOp.typeUuid",
            )
            .map_err(|error| error.message)?,
        ),
        terminal_type: TypeUuid(
            decode_uuid(
                value
                    .get_terminal_type()
                    .map_err(authoring_decode_error)?
                    .get_bytes()
                    .map_err(authoring_decode_error)?,
                "authoringOp.terminalType",
            )
            .map_err(|error| error.message)?,
        ),
        schema_hash: crate::LogicalHash(
            decode_hash(
                value.get_schema_hash().map_err(authoring_decode_error)?,
                "authoringOp.schemaHash",
            )
            .map_err(|error| error.message)?,
        ),
        logical_schema: value
            .get_logical_schema()
            .map_err(authoring_decode_error)?
            .to_vec()
            .into(),
        role: match value.get_role().map_err(authoring_decode_error)? {
            schema::AuthoringEntryRole::Runtime => AuthoringEntryRole::Runtime,
            schema::AuthoringEntryRole::AuthoringOnly => AuthoringEntryRole::AuthoringOnly,
        },
        tags,
        value: decode_authoring_value(value.get_value().map_err(authoring_decode_error)?)?,
    })
}

fn decode_authoring_ops(
    ops: capnp::struct_list::Reader<'_, schema::authoring_op::Owned>,
) -> Result<Vec<crate::AuthoringOp>, String> {
    ops.iter()
        .map(|op| match op.which().map_err(authoring_decode_error)? {
            schema::authoring_op::Which::Set(entry) => {
                decode_authoring_entry(entry.map_err(authoring_decode_error)?)
                    .map(crate::AuthoringOp::Set)
            }
            schema::authoring_op::Which::Remove(uuid) => Ok(crate::AuthoringOp::Remove {
                uuid: AssetUuid(
                    decode_uuid(
                        uuid.map_err(authoring_decode_error)?
                            .get_bytes()
                            .map_err(authoring_decode_error)?,
                        "authoringOp.remove",
                    )
                    .map_err(|error| error.message)?,
                ),
            }),
        })
        .collect()
}

fn decode_import_request(
    request: schema::import_request::Reader<'_>,
) -> Result<crate::ImportRequest, String> {
    let sources = request
        .get_sources()
        .map_err(authoring_decode_error)?
        .iter()
        .map(|source| {
            decode_text(source.map_err(authoring_decode_error)?, "import.sources")
                .map_err(|error| error.message)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(crate::ImportRequest {
        importer: decode_text(
            request.get_importer().map_err(authoring_decode_error)?,
            "import.importer",
        )
        .map_err(|error| error.message)?,
        sources,
        dest: decode_text(
            request.get_dest().map_err(authoring_decode_error)?,
            "import.dest",
        )
        .map_err(|error| error.message)?,
        settings: decode_authoring_value(request.get_settings().map_err(authoring_decode_error)?)?,
        watch: request.get_watch(),
        root: decode_text(
            request.get_root().map_err(authoring_decode_error)?,
            "import.root",
        )
        .map_err(|error| error.message)?,
    })
}

fn decode_long_running_op(
    operation: schema::long_running_op::Reader<'_>,
) -> Result<crate::LongRunningOp, String> {
    match operation.which().map_err(authoring_decode_error)? {
        schema::long_running_op::Which::RenameWithFixups(payload) => {
            Ok(crate::LongRunningOp::RenameWithFixups(
                payload.map_err(authoring_decode_error)?.to_vec().into(),
            ))
        }
        schema::long_running_op::Which::Doctor(payload) => Ok(crate::LongRunningOp::Doctor(
            payload.map_err(authoring_decode_error)?.to_vec().into(),
        )),
    }
}

fn decode_asset_query(reader: schema::asset_query::Reader<'_>) -> Result<AssetQuery, WireFailure> {
    Ok(AssetQuery {
        uuid: decode_optional_uuid(reader.get_uuid()?, "query.uuid")?.map(AssetUuid),
        bundle_path: decode_optional_text(reader.get_bundle_path()?, "query.bundlePath")?,
        local_id: decode_optional_text(reader.get_local_id()?, "query.localId")?,
        bundle_uuid: decode_optional_uuid(reader.get_bundle_uuid()?, "query.bundleUuid")?
            .map(crate::BundleUuid),
        authored_type: decode_optional_uuid(reader.get_authored_type()?, "query.authoredType")?
            .map(TypeUuid),
        terminal_type: decode_optional_uuid(reader.get_terminal_type()?, "query.terminalType")?
            .map(TypeUuid),
        tag: decode_optional_tag(reader.get_tag()?)?,
        path_prefix: decode_optional_text(reader.get_path_prefix()?, "query.pathPrefix")?,
        path_glob: decode_optional_text(reader.get_path_glob()?, "query.pathGlob")?,
        authoring_only: decode_optional_bool(reader.get_authoring_only()?)?,
    })
}

fn decode_pure_metadata_query(
    reader: schema::pure_metadata_query::Reader<'_>,
) -> Result<PureMetadataQuery, WireFailure> {
    let uuid = decode_uuid(reader.get_uuid()?.get_bytes()?, "q.uuid")?;
    let bundle = decode_uuid(reader.get_bundle()?.get_bytes()?, "q.bundle")?;
    let authored_type = decode_uuid(reader.get_authored_type()?.get_bytes()?, "q.authoredType")?;
    let path = decode_text(
        reader.get_normalized_path_prefix()?,
        "q.normalizedPathPrefix",
    )?;
    let role = reader.get_role().map_err(|error| WireFailure {
        code: WIRE_INVALID_VALUE,
        message: format!("q.role is invalid: {error}"),
    })?;

    let uuid = decode_present_uuid(reader.get_has_uuid(), uuid, "q.uuid")?.map(AssetUuid);
    let bundle =
        decode_present_uuid(reader.get_has_bundle(), bundle, "q.bundle")?.map(crate::BundleUuid);
    let authored_type = decode_present_uuid(
        reader.get_has_authored_type(),
        authored_type,
        "q.authoredType",
    )?
    .map(TypeUuid);
    let normalized_path_prefix = if reader.get_has_path_prefix() {
        if !valid_metadata_path_prefix(&path) {
            return Err(WireFailure {
                code: WIRE_INVALID_UTF8,
                message: "q.normalizedPathPrefix is not canonical".to_owned(),
            });
        }
        Some(path)
    } else if path.is_empty() {
        None
    } else {
        return Err(WireFailure {
            code: WIRE_INVALID_UTF8,
            message: "q.normalizedPathPrefix has non-empty unused payload".to_owned(),
        });
    };
    let role = if reader.get_has_role() {
        Some(match role {
            schema::AuthoringEntryRole::Runtime => AuthoringEntryRole::Runtime,
            schema::AuthoringEntryRole::AuthoringOnly => AuthoringEntryRole::AuthoringOnly,
        })
    } else if role == schema::AuthoringEntryRole::Runtime {
        None
    } else {
        return Err(WireFailure {
            code: WIRE_INVALID_VALUE,
            message: "q.role has non-default unused payload".to_owned(),
        });
    };
    Ok(PureMetadataQuery {
        uuid,
        bundle,
        normalized_path_prefix,
        authored_type,
        role,
    })
}

fn decode_present_uuid(
    present: bool,
    value: [u8; 16],
    field: &str,
) -> Result<Option<[u8; 16]>, WireFailure> {
    if present {
        Ok(Some(value))
    } else if value == [0; 16] {
        Ok(None)
    } else {
        Err(WireFailure {
            code: WIRE_INVALID_UUID,
            message: format!("{field} has nonzero unused payload"),
        })
    }
}

fn valid_metadata_path_prefix(path: &str) -> bool {
    path.nfc().eq(path.chars())
        && !path.starts_with('/')
        && !path.contains('\\')
        && path
            .split('/')
            .all(|component| !matches!(component, "." | ".."))
}

fn decode_optional_uuid(
    reader: schema::optional_data::Reader<'_>,
    field: &str,
) -> Result<Option<[u8; 16]>, WireFailure> {
    match reader.which().map_err(|error| WireFailure {
        code: WIRE_INVALID_UUID,
        message: format!("{field} selector is invalid: {error}"),
    })? {
        schema::optional_data::Which::Absent(()) => Ok(None),
        schema::optional_data::Which::Value(value) => {
            let value = value.map_err(|error| WireFailure {
                code: WIRE_INVALID_UUID,
                message: format!("{field} could not be read: {error}"),
            })?;
            decode_uuid(value, field).map(Some)
        }
    }
}

fn decode_optional_text(
    reader: schema::optional_text::Reader<'_>,
    field: &str,
) -> Result<Option<String>, WireFailure> {
    match reader.which().map_err(|error| WireFailure {
        code: WIRE_INVALID_UTF8,
        message: format!("{field} selector is invalid: {error}"),
    })? {
        schema::optional_text::Which::Absent(()) => Ok(None),
        schema::optional_text::Which::Value(value) => value
            .map_err(|error| WireFailure {
                code: WIRE_INVALID_UTF8,
                message: format!("{field} could not be read: {error}"),
            })
            .and_then(|value| decode_text(value, field))
            .map(Some),
    }
}

fn decode_optional_bool(
    reader: schema::optional_bool::Reader<'_>,
) -> Result<Option<bool>, WireFailure> {
    match reader.which().map_err(|error| WireFailure {
        code: WIRE_INVALID_VALUE,
        message: format!("query.authoringOnly selector is invalid: {error}"),
    })? {
        schema::optional_bool::Which::Absent(()) => Ok(None),
        schema::optional_bool::Which::Value(value) => Ok(Some(value)),
    }
}

fn decode_optional_tag(
    reader: schema::optional_tag_selector::Reader<'_>,
) -> Result<Option<TagSelector>, WireFailure> {
    match reader.which().map_err(|error| WireFailure {
        code: WIRE_INVALID_UTF8,
        message: format!("query.tag selector is invalid: {error}"),
    })? {
        schema::optional_tag_selector::Which::Absent(()) => Ok(None),
        schema::optional_tag_selector::Which::Value(value) => {
            let value = value.map_err(|error| WireFailure {
                code: WIRE_INVALID_UTF8,
                message: format!("query.tag could not be read: {error}"),
            })?;
            let tag = value.get_tag().map_err(|error| WireFailure {
                code: WIRE_INVALID_UTF8,
                message: format!("query.tag.tag could not be read: {error}"),
            })?;
            let optional_value = value.get_value().map_err(|error| WireFailure {
                code: WIRE_INVALID_UTF8,
                message: format!("query.tag.value could not be read: {error}"),
            })?;
            Ok(Some(TagSelector {
                tag: decode_text(tag, "query.tag.tag")?,
                value: decode_optional_text(optional_value, "query.tag.value")?,
            }))
        }
    }
}

fn decode_text_list(
    values: capnp::text_list::Reader<'_>,
    field: &str,
) -> Result<Vec<String>, WireFailure> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map_err(|error| WireFailure {
                    code: WIRE_INVALID_UTF8,
                    message: format!("{field}[{index}] could not be read: {error}"),
                })
                .and_then(|value| decode_text(value, &format!("{field}[{index}]")))
        })
        .collect()
}

fn decode_uuid(bytes: &[u8], field: &str) -> Result<[u8; 16], WireFailure> {
    fixed(bytes, field, WIRE_INVALID_UUID)
}

fn decode_hash(bytes: &[u8], field: &str) -> Result<[u8; 32], WireFailure> {
    fixed(bytes, field, WIRE_INVALID_HASH)
}

fn decode_instance(bytes: &[u8], field: &str) -> Result<[u8; 16], WireFailure> {
    fixed(bytes, field, WIRE_INVALID_INSTANCE)
}

fn fixed<const N: usize>(bytes: &[u8], field: &str, code: u16) -> Result<[u8; N], WireFailure> {
    bytes.try_into().map_err(|_| WireFailure {
        code,
        message: format!("{field} must be {N} bytes, got {}", bytes.len()),
    })
}

fn decode_text(value: capnp::text::Reader<'_>, field: &str) -> Result<String, WireFailure> {
    value.to_str().map(str::to_owned).map_err(|_| WireFailure {
        code: WIRE_INVALID_UTF8,
        message: format!("{field} is not valid UTF-8"),
    })
}

fn write_connect_request(
    mut output: schema::root::connect_params::Builder<'_>,
    request: &ConnectRequest,
) {
    output.set_target(request.target.as_str());
    output.set_target_def_hash(&request.target_definition_hash.0);
    output.set_protocol(request.protocol);
}

fn decode_connect_response(
    response: schema::connect_call::Reader<'_>,
    request: &ConnectRequest,
) -> Result<RemoteConnectOutcome, capnp::Error> {
    use schema::connect_call::Which;
    match response
        .which()
        .map_err(|error| capnp::Error::failed(error.to_string()))?
    {
        Which::Success(connected) => {
            let connected = connected?;
            let instance = decode_instance(connected.get_instance()?, "instance")
                .map_err(|error| capnp::Error::failed(error.message))?;
            Ok(RemoteConnectOutcome::Connected {
                hub: connected.get_hub()?,
                instance: StoreInstanceId(instance),
            })
        }
        Which::ConfigurationFailed(error) => Ok(RemoteConnectOutcome::ConfigurationFailed(
            decode_configuration_error(error?)?,
        )),
        Which::PipelineUnavailable(diagnostic) => {
            let diagnostic = diagnostic?;
            let diagnostic = crate::PipelineUnavailableDiagnostic::PipelineFailure(
                decode_pipeline_failure(diagnostic.get_pipeline_failure()?)?,
            );
            Ok(RemoteConnectOutcome::PipelineUnavailable(diagnostic))
        }
        Which::TargetFailure(failure) => {
            let failure = failure?;
            let failure = match failure.get_code()? {
                schema::TargetFailureCode::UnknownTarget => {
                    if !failure.get_expected()?.is_empty() || !failure.get_observed()?.is_empty() {
                        return Err(capnp::Error::failed(
                            "unknown-target failure must not carry hashes".to_owned(),
                        ));
                    }
                    ConnectError::UnknownTarget {
                        target: request.target.clone(),
                    }
                }
                schema::TargetFailureCode::DefinitionMismatch => {
                    let expected = TargetDefinitionHash(
                        decode_hash(failure.get_expected()?, "targetFailure.expected")
                            .map_err(|error| capnp::Error::failed(error.message))?,
                    );
                    let got = TargetDefinitionHash(
                        decode_hash(failure.get_observed()?, "targetFailure.observed")
                            .map_err(|error| capnp::Error::failed(error.message))?,
                    );
                    if got != request.target_definition_hash {
                        return Err(capnp::Error::failed(
                            "server definition mismatch does not describe the submitted hash"
                                .to_owned(),
                        ));
                    }
                    ConnectError::TargetDefinitionMismatch { expected, got }
                }
            };
            Ok(RemoteConnectOutcome::TargetFailure(failure))
        }
        Which::ProtocolFailure(failure) => {
            let failure = failure?;
            Ok(RemoteConnectOutcome::ProtocolFailure {
                expected: failure.get_expected(),
                observed: failure.get_observed(),
                message: decode_text(failure.get_message()?, "protocolFailure.message")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            })
        }
        Which::Error(failure) => {
            let failure = failure?;
            Ok(RemoteConnectOutcome::Error {
                code: failure.get_code(),
                message: decode_text(failure.get_message()?, "error.message")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            })
        }
    }
}

fn decode_metadata_response(
    response: schema::metadata_connect_result::Reader<'_>,
) -> Result<RemoteMetadataOutcome, capnp::Error> {
    use schema::metadata_connect_result::Which;
    match response
        .which()
        .map_err(|error| capnp::Error::failed(error.to_string()))?
    {
        Which::Success(connected) => {
            let connected = connected?;
            let instance = decode_instance(connected.get_instance()?, "instance")
                .map_err(|error| capnp::Error::failed(error.message))?;
            Ok(RemoteMetadataOutcome::Connected {
                hub: connected.get_hub()?,
                instance: StoreInstanceId(instance),
                protocol_epoch: connected.get_protocol_epoch(),
            })
        }
        Which::ProtocolFailure(failure) => {
            let failure = failure?;
            Ok(RemoteMetadataOutcome::ProtocolFailure {
                expected: failure.get_expected(),
                observed: failure.get_observed(),
                message: decode_text(failure.get_message()?, "protocolFailure.message")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            })
        }
        Which::Error(failure) => {
            let failure = failure?;
            Ok(RemoteMetadataOutcome::Error {
                code: failure.get_code(),
                message: decode_text(failure.get_message()?, "error.message")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            })
        }
    }
}

/// Decode and authenticate a wire authoring inspection before handing it to
/// tooling. This is the sole high-level boundary for the branded raw fields:
/// logical-schema DSLH and the complete schema/value/blob walk are verified
/// before success is returned.
pub fn decode_authoring_inspection(
    value: schema::authoring_inspection::Reader<'_>,
) -> Result<AuthoringInspection, capnp::Error> {
    let stamp = value.get_stamp()?;
    let stamp = SnapshotStamp {
        instance: StoreInstanceId(
            decode_instance(stamp.get_instance()?, "authoringInspection.stamp.instance")
                .map_err(|error| capnp::Error::failed(error.message))?,
        ),
        version: InputVersion(stamp.get_input_version()),
    };
    let uuid = AssetUuid(
        decode_uuid(value.get_uuid()?, "authoringInspection.uuid")
            .map_err(|error| capnp::Error::failed(error.message))?,
    );
    let bundle = crate::BundleUuid(
        decode_uuid(value.get_bundle()?, "authoringInspection.bundle")
            .map_err(|error| capnp::Error::failed(error.message))?,
    );
    let type_uuid = TypeUuid(
        decode_uuid(value.get_type_uuid()?, "authoringInspection.typeUuid")
            .map_err(|error| capnp::Error::failed(error.message))?,
    );
    let schema_hash = crate::LogicalHash(
        decode_hash(value.get_schema_hash()?, "authoringInspection.schemaHash")
            .map_err(|error| capnp::Error::failed(error.message))?,
    );
    let wire_value = value.get_value()?;
    let authored_value = crate::AuthoringValue {
        canonical_value: wire_value.get_canonical_value()?.to_vec().into(),
        blobs: wire_value
            .get_blobs()?
            .iter()
            .map(|blob| blob.map(|bytes| bytes.to_vec().into()))
            .collect::<Result<Vec<_>, _>>()?,
    };
    let logical_schema: std::sync::Arc<[u8]> = value.get_logical_schema()?.to_vec().into();
    crate::validate::decode_authoring_payload(schema_hash, &logical_schema, &authored_value)
        .map(drop)
        .map_err(|error| {
            capnp::Error::failed(format!("invalid authoring inspection: {error:?}"))
        })?;
    Ok(AuthoringInspection {
        stamp,
        uuid,
        bundle,
        local_id: decode_text(value.get_local_id()?, "authoringInspection.localId")
            .map_err(|error| capnp::Error::failed(error.message))?,
        normalized_path: decode_text(
            value.get_normalized_path()?,
            "authoringInspection.normalizedPath",
        )
        .map_err(|error| capnp::Error::failed(error.message))?,
        type_uuid,
        schema_hash,
        logical_schema,
        role: match value.get_role()? {
            schema::AuthoringEntryRole::Runtime => AuthoringEntryRole::Runtime,
            schema::AuthoringEntryRole::AuthoringOnly => AuthoringEntryRole::AuthoringOnly,
        },
        value: authored_value,
    })
}

pub fn decode_configuration_error(
    reader: schema::configuration_error::Reader<'_>,
) -> Result<ConfigurationError, capnp::Error> {
    if reader.get_detail_version() != 1 {
        return Err(capnp::Error::failed(format!(
            "unsupported configuration error detail version {}",
            reader.get_detail_version()
        )));
    }
    let code = crate::ConfigurationErrorCode::try_from(reader.get_code())
        .map_err(|error| capnp::Error::failed(error.to_string()))?;
    let detail = crate::DscpV1::from_canonical_detail_bytes(code, reader.get_detail_bytes()?)
        .map_err(|error| capnp::Error::failed(format!("invalid DSCP v1 detail: {error}")))?;
    let value = ConfigurationError {
        code,
        reason_hash: decode_hash(reader.get_reason_hash()?, "reasonHash")
            .map_err(|error| capnp::Error::failed(error.message))?,
        detail: Box::new(detail),
        message: decode_text(reader.get_message()?, "configurationError.message")
            .map_err(|error| capnp::Error::failed(error.message))?,
    };
    value
        .validate()
        .map_err(|error| capnp::Error::failed(format!("invalid configuration error: {error}")))?;
    Ok(value)
}

pub fn decode_namespace_error(
    reader: schema::namespace_error::Reader<'_>,
) -> Result<NamespaceError, capnp::Error> {
    let detail = reader.get_detail()?;
    let detail = match detail
        .which()
        .map_err(|error| capnp::Error::failed(error.to_string()))?
    {
        schema::namespace_error_detail::Which::DuplicateAssetUuid(value) => {
            let value = value?;
            let asset = AssetUuid(
                decode_uuid(value.get_asset()?.get_bytes()?, "namespaceError.asset")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            );
            let claimants = value
                .get_claimants()?
                .iter()
                .map(|claimant| {
                    Ok(
                        match claimant
                            .which()
                            .map_err(|error| capnp::Error::failed(error.to_string()))?
                        {
                            schema::asset_claimant::Which::Authored(authored) => {
                                let authored = authored?;
                                crate::AssetClaimant::Authored {
                                    source: read_bundle_source(authored.get_source()?)?,
                                    bundle: crate::BundleUuid(
                                        decode_uuid(
                                            authored.get_bundle()?.get_bytes()?,
                                            "namespaceError.claimant.bundle",
                                        )
                                        .map_err(|error| capnp::Error::failed(error.message))?,
                                    ),
                                    local_id: decode_text(
                                        authored.get_local_id()?,
                                        "namespaceError.claimant.localId",
                                    )
                                    .map_err(|error| capnp::Error::failed(error.message))?,
                                }
                            }
                            schema::asset_claimant::Which::Derived(derived) => {
                                let derived = derived?;
                                crate::AssetClaimant::Derived {
                                    parent: AssetUuid(
                                        decode_uuid(
                                            derived.get_parent()?.get_bytes()?,
                                            "namespaceError.claimant.parent",
                                        )
                                        .map_err(|error| capnp::Error::failed(error.message))?,
                                    ),
                                    output_key: decode_text(
                                        derived.get_output_key()?,
                                        "namespaceError.claimant.outputKey",
                                    )
                                    .map_err(|error| capnp::Error::failed(error.message))?,
                                }
                            }
                        },
                    )
                })
                .collect::<Result<Vec<_>, capnp::Error>>()?;
            NamespaceErrorV1::DuplicateAssetUuid { asset, claimants }
        }
        schema::namespace_error_detail::Which::DuplicateBundleUuid(value) => {
            let value = value?;
            NamespaceErrorV1::DuplicateBundleUuid {
                bundle: crate::BundleUuid(
                    decode_uuid(value.get_bundle()?.get_bytes()?, "namespaceError.bundle")
                        .map_err(|error| capnp::Error::failed(error.message))?,
                ),
                sources: value
                    .get_sources()?
                    .iter()
                    .map(read_bundle_source)
                    .collect::<Result<Vec<_>, _>>()?,
            }
        }
        schema::namespace_error_detail::Which::SameRootNormalizedPathCollision(value) => {
            let value = value?;
            let claims = value
                .get_claims()?
                .iter()
                .map(|claim| {
                    let path = claim.get_raw_relative_path()?;
                    let raw_relative_path = match path
                        .which()
                        .map_err(|error| capnp::Error::failed(error.to_string()))?
                    {
                        schema::platform_path_bytes::Which::UnixBytes(bytes) => {
                            crate::PlatformPathBytes::Unix(bytes?.to_vec())
                        }
                        schema::platform_path_bytes::Which::WindowsUtf16Le(bytes) => {
                            let bytes = bytes?;
                            if bytes.len() % 2 != 0 {
                                return Err(capnp::Error::failed(
                                    "namespace error Windows path has odd byte length".to_owned(),
                                ));
                            }
                            crate::PlatformPathBytes::Windows(
                                bytes
                                    .chunks_exact(2)
                                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                                    .collect(),
                            )
                        }
                    };
                    Ok(crate::PhysicalPathClaim {
                        raw_relative_path,
                        file_hash: distill_core::id::BundleFileHash(
                            decode_hash(claim.get_file_hash()?, "namespaceError.claim.fileHash")
                                .map_err(|error| capnp::Error::failed(error.message))?,
                        ),
                    })
                })
                .collect::<Result<Vec<_>, capnp::Error>>()?;
            NamespaceErrorV1::SameRootNormalizedPathCollision {
                root_name: decode_text(value.get_root_name()?, "namespaceError.rootName")
                    .map_err(|error| capnp::Error::failed(error.message))?,
                normalized_path: decode_text(
                    value.get_normalized_path()?,
                    "namespaceError.normalizedPath",
                )
                .map_err(|error| capnp::Error::failed(error.message))?,
                claims,
            }
        }
        schema::namespace_error_detail::Which::IncompleteSkeleton(value) => {
            let value = value?;
            NamespaceErrorV1::IncompleteSkeleton {
                source: read_bundle_source(value.get_source()?)?,
                failure: match value.get_failure_code() {
                    1 => crate::SkeletonFailureCode::EnvelopeMalformed,
                    2 => crate::SkeletonFailureCode::MissingFormatVersion,
                    3 => crate::SkeletonFailureCode::InvalidBundleUuid,
                    4 => crate::SkeletonFailureCode::IncompleteAssetIdentity,
                    5 => crate::SkeletonFailureCode::IncompleteTypeIdentity,
                    6 => crate::SkeletonFailureCode::IncompleteTagIdentity,
                    other => {
                        return Err(capnp::Error::failed(format!(
                            "unknown skeleton failure code {other}"
                        )))
                    }
                },
            }
        }
        schema::namespace_error_detail::Which::UnreadableGlobalBundlePath(value) => {
            let value = value?;
            NamespaceErrorV1::UnreadableGlobalBundlePath {
                root_name: decode_text(value.get_root_name()?, "namespaceError.rootName")
                    .map_err(|error| capnp::Error::failed(error.message))?,
                normalized_path: decode_text(
                    value.get_normalized_path()?,
                    "namespaceError.normalizedPath",
                )
                .map_err(|error| capnp::Error::failed(error.message))?,
                failure: match value.get_failure_code() {
                    1 => crate::GlobalBundleReadFailureCode::PermissionDenied,
                    2 => crate::GlobalBundleReadFailureCode::InvalidFileType,
                    3 => crate::GlobalBundleReadFailureCode::SymlinkIdentityChanged,
                    4 => crate::GlobalBundleReadFailureCode::IoDataLoss,
                    other => {
                        return Err(capnp::Error::failed(format!(
                            "unknown global read failure code {other}"
                        )))
                    }
                },
            }
        }
        schema::namespace_error_detail::Which::InvalidPhysicalPath(value) => {
            let value = value?;
            let path = value.get_raw_relative_path()?;
            let raw_relative_path = match path
                .which()
                .map_err(|error| capnp::Error::failed(error.to_string()))?
            {
                schema::platform_path_bytes::Which::UnixBytes(bytes) => {
                    crate::PlatformPathBytes::Unix(bytes?.to_vec())
                }
                schema::platform_path_bytes::Which::WindowsUtf16Le(bytes) => {
                    let bytes = bytes?;
                    if bytes.len() % 2 != 0 {
                        return Err(capnp::Error::failed(
                            "namespace error Windows path has odd byte length".to_owned(),
                        ));
                    }
                    crate::PlatformPathBytes::Windows(
                        bytes
                            .chunks_exact(2)
                            .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                            .collect(),
                    )
                }
            };
            NamespaceErrorV1::InvalidPhysicalPath {
                root_name: decode_text(value.get_root_name()?, "namespaceError.rootName")
                    .map_err(|error| capnp::Error::failed(error.message))?,
                raw_relative_path,
                failure: match value.get_failure_code() {
                    1 => crate::PhysicalPathFailureCode::InvalidUnixUtf8,
                    2 => crate::PhysicalPathFailureCode::UnpairedWindowsUtf16,
                    3 => crate::PhysicalPathFailureCode::Absolute,
                    4 => crate::PhysicalPathFailureCode::EmptyComponent,
                    5 => crate::PhysicalPathFailureCode::DotComponent,
                    6 => crate::PhysicalPathFailureCode::ParentComponent,
                    7 => crate::PhysicalPathFailureCode::ForbiddenCharacter,
                    other => {
                        return Err(capnp::Error::failed(format!(
                            "unknown physical path failure code {other}"
                        )))
                    }
                },
            }
        }
        schema::namespace_error_detail::Which::UnreadableScanSubtree(value) => {
            let value = value?;
            let subject = match value
                .get_subject()?
                .which()
                .map_err(|error| capnp::Error::failed(error.to_string()))?
            {
                schema::scan_subject::Which::Root(root) => crate::ScanSubject::Root {
                    root_name: decode_text(root?.get_root_name()?, "namespaceError.scan.rootName")
                        .map_err(|error| capnp::Error::failed(error.message))?,
                },
                schema::scan_subject::Which::Subtree(subtree) => {
                    let subtree = subtree?;
                    let path = subtree.get_raw_relative_path()?;
                    let raw_relative_path = match path
                        .which()
                        .map_err(|error| capnp::Error::failed(error.to_string()))?
                    {
                        schema::platform_path_bytes::Which::UnixBytes(bytes) => {
                            crate::PlatformPathBytes::Unix(bytes?.to_vec())
                        }
                        schema::platform_path_bytes::Which::WindowsUtf16Le(bytes) => {
                            let bytes = bytes?;
                            if bytes.len() % 2 != 0 {
                                return Err(capnp::Error::failed(
                                    "namespace error Windows scan path has odd byte length"
                                        .to_owned(),
                                ));
                            }
                            crate::PlatformPathBytes::Windows(
                                bytes
                                    .chunks_exact(2)
                                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                                    .collect(),
                            )
                        }
                    };
                    crate::ScanSubject::Subtree {
                        root_name: decode_text(
                            subtree.get_root_name()?,
                            "namespaceError.scan.rootName",
                        )
                        .map_err(|error| capnp::Error::failed(error.message))?,
                        raw_relative_path,
                    }
                }
            };
            let failure = match value
                .get_failure()
                .map_err(|error| capnp::Error::failed(error.to_string()))?
            {
                schema::ScanFailureCodeValue::PermissionDenied => {
                    crate::ScanFailureCode::PermissionDenied
                }
                schema::ScanFailureCodeValue::NotFound => crate::ScanFailureCode::NotFound,
                schema::ScanFailureCodeValue::InvalidFileType => {
                    crate::ScanFailureCode::InvalidFileType
                }
                schema::ScanFailureCodeValue::SymlinkIdentityChanged => {
                    crate::ScanFailureCode::SymlinkIdentityChanged
                }
                schema::ScanFailureCodeValue::IoDataLoss => crate::ScanFailureCode::IoDataLoss,
            };
            NamespaceErrorV1::UnreadableScanSubtree { subject, failure }
        }
    };
    let message = decode_text(reader.get_message()?, "namespaceError.message")
        .map_err(|error| capnp::Error::failed(error.message))?;
    let decoded = NamespaceError::new(detail, message)
        .map_err(|error| capnp::Error::failed(error.to_string()))?;
    let identity = decode_hash(reader.get_identity()?, "namespaceError.identity")
        .map_err(|error| capnp::Error::failed(error.message))?;
    if decoded.code as u16 != reader.get_code() || decoded.identity != identity {
        return Err(capnp::Error::failed(
            "namespace error code/detail/identity mismatch".to_owned(),
        ));
    }
    Ok(decoded)
}

pub fn decode_pipeline_failure(
    reader: schema::pipeline_failure::Reader<'_>,
) -> Result<crate::PipelineFailure, capnp::Error> {
    let code = reader.get_code()? as u16 + 1;
    let origin = reader.get_origin()? as u16 + 1;
    let cleanup = reader.get_cleanup()? as u16;
    let identity = decode_hash(reader.get_identity()?, "pipelineFailure.identity")
        .map_err(|error| capnp::Error::failed(error.message))?;
    let message = decode_text(reader.get_message()?, "pipelineFailure.message")
        .map_err(|error| capnp::Error::failed(error.message))?;
    crate::PipelineFailure::from_wire(code, origin, cleanup, identity, message)
        .map_err(|error| capnp::Error::failed(error.to_string()))
}

fn read_bundle_source(
    source: schema::bundle_source::Reader<'_>,
) -> Result<crate::ReadableBundleSource, capnp::Error> {
    Ok(crate::ReadableBundleSource {
        root_name: decode_text(source.get_root_name()?, "namespaceError.source.rootName")
            .map_err(|error| capnp::Error::failed(error.message))?,
        normalized_path: decode_text(
            source.get_normalized_path()?,
            "namespaceError.source.normalizedPath",
        )
        .map_err(|error| capnp::Error::failed(error.message))?,
        file_hash: distill_core::id::BundleFileHash(
            decode_hash(source.get_file_hash()?, "namespaceError.source.fileHash")
                .map_err(|error| capnp::Error::failed(error.message))?,
        ),
    })
}

fn write_snapshot_result(result: schema::snapshot_call::Builder<'_>, outcome: RpcResult<Snapshot>) {
    match outcome {
        RpcResult::Success(snapshot) => {
            snapshot.expire_later();
            let client: schema::snapshot::Client =
                capnp_rpc::new_client(SnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(error) => write_rpc_result_error_snapshot(result, error),
    }
}

fn write_authoring_snapshot_result(
    mut result: schema::authoring_snapshot_call::Builder<'_>,
    outcome: RpcResult<AuthoringSnapshot>,
) {
    match outcome {
        RpcResult::Success(snapshot) => {
            snapshot.expire_later();
            let client: schema::authoring_snapshot::Client =
                capnp_rpc::new_client(AuthoringSnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => result.set_snapshot_expired(()),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_snapshot_result(
    mut result: schema::metadata_snapshot_call::Builder<'_>,
    outcome: MetadataCall<MetadataSnapshot>,
) {
    match outcome {
        MetadataCall::Success(snapshot) => {
            snapshot.expire_later();
            let client: schema::metadata_snapshot::Client =
                capnp_rpc::new_client(MetadataSnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_authoring_snapshot_result(
    mut result: schema::metadata_authoring_snapshot_call::Builder<'_>,
    outcome: MetadataCall<MetadataAuthoringSnapshot>,
) {
    match outcome {
        MetadataCall::Success(snapshot) => {
            snapshot.expire_later();
            let client: schema::metadata_authoring_snapshot::Client =
                capnp_rpc::new_client(MetadataAuthoringSnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_authoring_refresh_result(
    mut result: schema::metadata_authoring_snapshot_call::Builder<'_>,
    outcome: MetadataCall<MetadataAuthoringSnapshot>,
) {
    match outcome {
        MetadataCall::Success(snapshot) => {
            snapshot.expire_later();
            let client: schema::metadata_authoring_snapshot::Client =
                capnp_rpc::new_client(MetadataAuthoringSnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_diagnostics_result(
    mut result: schema::metadata_diagnostics_call::Builder<'_>,
    outcome: MetadataCall<MetadataDiagnostics>,
) -> Result<(), capnp::Error> {
    match outcome {
        MetadataCall::Success(diagnostics) => {
            let mut output = result.reborrow().init_success();
            write_authoring_stamp(output.reborrow().init_stamp(), diagnostics.stamp);
            let mut configuration = output.reborrow().init_configuration();
            match &diagnostics.configuration {
                ConfigurationStatus::Ready => configuration.set_ready(()),
                ConfigurationStatus::Failed(error) => {
                    write_configuration_error(configuration.init_failed(), error)
                }
            }
            let mut pipeline = output.reborrow().init_pipeline();
            match &diagnostics.pipeline {
                crate::PipelineDiagnostic::Ready => pipeline.set_ready(()),
                crate::PipelineDiagnostic::Failed(failure) => {
                    write_pipeline_failure(pipeline.init_failed(), failure)?
                }
            }
            let mut errors = output.init_namespace_errors(diagnostics.namespace_errors.len() as u32);
            for (index, error) in diagnostics.namespace_errors.iter().enumerate() {
                write_namespace_error(errors.reborrow().get(index as u32), error);
            }
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
    Ok(())
}

fn write_metadata_uint64_result(
    mut result: schema::metadata_u_int64_call::Builder<'_>,
    outcome: MetadataCall<u64>,
) {
    match outcome {
        MetadataCall::Success(value) => result.set_success(value),
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_uuid_list_result(
    mut result: schema::metadata_uuid_list_call::Builder<'_>,
    outcome: MetadataNamespaceCall<Vec<AssetUuid>>,
) {
    match outcome {
        MetadataNamespaceCall::Success(uuids) => {
            let mut list = result.reborrow().init_success(uuids.len() as u32);
            for (index, uuid) in uuids.iter().enumerate() {
                list.reborrow().get(index as u32).set_bytes(&uuid.0);
            }
        }
        MetadataNamespaceCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataNamespaceCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataNamespaceCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_entry_result(
    mut result: schema::metadata_entry_meta_call::Builder<'_>,
    outcome: MetadataNamespaceCall<PureMetadataEntry>,
) {
    match outcome {
        MetadataNamespaceCall::Success(entry) => {
            write_pure_metadata_entry(result.reborrow().init_success(), &entry);
        }
        MetadataNamespaceCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataNamespaceCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataNamespaceCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_path_result(
    mut result: schema::metadata_path_resolve_call::Builder<'_>,
    outcome: MetadataNamespaceCall<PathResolveResult>,
) {
    match outcome {
        MetadataNamespaceCall::Success(value) => {
            let mut output = result.init_success();
            match value {
                PathResolveResult::Resolved(uuid) => output.set_resolved(&uuid.0),
                PathResolveResult::Missing => output.set_missing(()),
                PathResolveResult::Failed(PathResolveFailure::Ambiguous { candidates }) => {
                    let mut list = output.init_ambiguous(candidates.len() as u32);
                    for (index, uuid) in candidates.iter().enumerate() {
                        list.set(index as u32, &uuid.0);
                    }
                }
            }
        }
        MetadataNamespaceCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataNamespaceCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataNamespaceCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_authoring_inspect_result(
    mut result: schema::metadata_authoring_inspect_call::Builder<'_>,
    outcome: MetadataNamespaceCall<AuthoringInspectResult>,
) {
    match outcome {
        MetadataNamespaceCall::Success(AuthoringInspectResult::Inspection(inspection)) => {
            write_authoring_inspection(result.reborrow().init_success(), &inspection);
        }
        MetadataNamespaceCall::Success(AuthoringInspectResult::Missing) => result.set_missing(()),
        MetadataNamespaceCall::Success(AuthoringInspectResult::RoleIneligible { observed }) => {
            result
                .init_role_ineligible()
                .set_observed(wire_authoring_role(observed));
        }
        MetadataNamespaceCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataNamespaceCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataNamespaceCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_pure_metadata_entry(
    mut output: schema::pure_metadata_entry::Builder<'_>,
    entry: &PureMetadataEntry,
) {
    output.reborrow().init_uuid().set_bytes(&entry.uuid.0);
    output.reborrow().init_bundle().set_bytes(&entry.bundle.0);
    output.set_local_id(entry.local_id.as_str());
    output.set_normalized_path(entry.normalized_path.as_str());
    output
        .reborrow()
        .init_authored_type()
        .set_bytes(&entry.authored_type.0);
    output.set_schema_hash(&entry.schema_hash.0);
    output.set_role(wire_authoring_role(entry.role));
}

fn write_metadata_entry(mut output: schema::entry_meta::Builder<'_>, entry: &MetadataEntry) {
    output.reborrow().init_uuid().set_bytes(&entry.uuid.0);
    output.reborrow().init_bundle().set_bytes(&entry.bundle.0);
    output.set_local_id(entry.local_id.as_str());
    output.set_normalized_path(entry.normalized_path.as_str());
    output
        .reborrow()
        .init_authored_type()
        .set_bytes(&entry.authored_type.0);
    output
        .reborrow()
        .init_terminal_type()
        .set_bytes(&entry.terminal_type.0);
    output.set_schema_hash(&entry.schema_hash.0);
    output.set_role(wire_authoring_role(entry.role));
    let mut tags = output.init_tags(entry.tags.len() as u32);
    for (index, (tag, value)) in entry.tags.iter().enumerate() {
        let mut row = tags.reborrow().get(index as u32);
        row.set_tag(tag.as_str());
        if let Some(value) = value {
            row.set_has_value(true);
            row.set_value(value.as_str());
        }
    }
}

fn write_uuid_list_result(
    mut result: schema::uuid_list_call::Builder<'_>,
    outcome: RpcResult<Vec<AssetUuid>>,
) {
    match outcome {
        RpcResult::Success(uuids) => {
            let mut list = result.reborrow().init_success(uuids.len() as u32);
            for (index, uuid) in uuids.iter().enumerate() {
                list.reborrow().get(index as u32).set_bytes(&uuid.0);
            }
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => result.set_snapshot_expired(()),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_target_entry_result(
    mut result: schema::entry_meta_call::Builder<'_>,
    outcome: RpcResult<MetadataEntry>,
) {
    match outcome {
        RpcResult::Success(entry) => write_metadata_entry(result.reborrow().init_success(), &entry),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_authoring_inspect_result(
    mut result: schema::authoring_inspect_call::Builder<'_>,
    outcome: RpcResult<AuthoringInspectResult>,
) {
    match outcome {
        RpcResult::Success(AuthoringInspectResult::Inspection(inspection)) => {
            write_authoring_inspection(result.reborrow().init_success(), &inspection);
        }
        RpcResult::Success(AuthoringInspectResult::Missing) => result.set_missing(()),
        RpcResult::Success(AuthoringInspectResult::RoleIneligible { observed }) => {
            result
                .init_role_ineligible()
                .set_observed(wire_authoring_role(observed));
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => result.set_snapshot_expired(()),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_authoring_inspection(
    mut output: schema::authoring_inspection::Builder<'_>,
    inspection: &AuthoringInspection,
) {
    write_authoring_stamp(output.reborrow().init_stamp(), inspection.stamp);
    output.set_uuid(&inspection.uuid.0);
    output.set_bundle(&inspection.bundle.0);
    output.set_local_id(inspection.local_id.as_str());
    output.set_normalized_path(inspection.normalized_path.as_str());
    output.set_type_uuid(&inspection.type_uuid.0);
    output.set_schema_hash(&inspection.schema_hash.0);
    output.set_logical_schema(&inspection.logical_schema);
    output.set_role(wire_authoring_role(inspection.role));
    let mut value = output.init_value();
    value.set_canonical_value(&inspection.value.canonical_value);
    let mut blobs = value.init_blobs(inspection.value.blobs.len() as u32);
    for (index, blob) in inspection.value.blobs.iter().enumerate() {
        blobs.set(index as u32, blob);
    }
}

fn wire_authoring_role(role: AuthoringEntryRole) -> schema::AuthoringEntryRole {
    match role {
        AuthoringEntryRole::Runtime => schema::AuthoringEntryRole::Runtime,
        AuthoringEntryRole::AuthoringOnly => schema::AuthoringEntryRole::AuthoringOnly,
    }
}

fn write_authoring_progress(
    mut output: schema::authoring_progress_event::Builder<'_>,
    event: &crate::AuthoringProgressEvent,
) {
    output.set_sequence(event.sequence);
    output.set_state(match event.state {
        crate::AuthoringProgressState::Started => schema::AuthoringProgressState::Started,
        crate::AuthoringProgressState::Running => schema::AuthoringProgressState::Running,
        crate::AuthoringProgressState::Completed => schema::AuthoringProgressState::Completed,
        crate::AuthoringProgressState::Cancelled => schema::AuthoringProgressState::Cancelled,
        crate::AuthoringProgressState::Failed => schema::AuthoringProgressState::Failed,
    });
    output.set_payload(&event.payload);
}

fn write_subscribe_result(
    result: schema::subscribe_call::Builder<'_>,
    outcome: RpcResult<crate::SubscriptionInstall>,
) {
    match outcome {
        RpcResult::Success(subscription) => {
            let mut installed = result.init_success();
            let client: schema::delta_stream::Client = capnp_rpc::new_client(DeltaStreamService {
                stream: subscription.deltas,
            });
            installed.set_deltas(client);
            installed.set_installed(subscription.installed.0);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(error) => write_rpc_result_error_subscribe(result, error),
    }
}

fn write_void_result(mut result: schema::void_call::Builder<'_>, outcome: RpcResult<()>) {
    match outcome {
        RpcResult::Success(()) => result.set_success(()),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_uint64_result(mut result: schema::u_int64_call::Builder<'_>, outcome: RpcResult<u64>) {
    match outcome {
        RpcResult::Success(value) => result.set_success(value),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_bundle_uuid_result(
    mut result: schema::uuid_call::Builder<'_>,
    outcome: RpcResult<BundleUuid>,
) {
    match outcome {
        RpcResult::Success(uuid) => result.reborrow().init_success().set_bytes(&uuid.0),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_progress_result(
    mut result: schema::progress_call::Builder<'_>,
    outcome: RpcResult<ProgressStream>,
) {
    match outcome {
        RpcResult::Success(stream) => {
            let client: schema::progress_stream::Client =
                capnp_rpc::new_client(ProgressStreamService {
                    stream: RefCell::new(stream),
                });
            result.set_success(client);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_import_failures_result(
    mut result: schema::import_failures_call::Builder<'_>,
    outcome: RpcResult<Vec<crate::ImportFailure>>,
) {
    match outcome {
        RpcResult::Success(failures) => {
            let mut list = result.init_success(failures.len() as u32);
            for (index, failure) in failures.iter().enumerate() {
                let mut entry = list.reborrow().get(index as u32);
                entry.set_bundle(&failure.bundle.0);
                entry.set_root(failure.root.as_str());
                entry.set_path(failure.path.as_str());
                entry.set_message(failure.message.as_str());
            }
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => result.set_snapshot_expired(()),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_data_result(mut result: schema::data_call::Builder<'_>, outcome: RpcResult<Arc<[u8]>>) {
    match outcome {
        RpcResult::Success(bytes) => result.set_success(&bytes),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_resolve_result(
    mut result: schema::resolve_call::Builder<'_>,
    outcome: RpcResult<crate::TerminalEvent<ResolveResult>>,
) {
    match outcome {
        RpcResult::Success(terminal) => {
            let mut output = result.init_success();
            write_rpc_basis(output.reborrow().init_basis(), &terminal.basis);
            let mut value = output.init_result();
            match terminal.value {
                ResolveResult::Built { content_hash } => value.set_built(&content_hash.0),
                ResolveResult::Drifted { input, current } => {
                    let mut drifted = value.init_drifted();
                    let mut wire_input = drifted.reborrow().init_input();
                    match input {
                        DriftedInput::File(path) => wire_input.set_file(path.as_str()),
                        DriftedInput::Asset(asset) => wire_input.set_asset(&asset.0),
                        DriftedInput::Query(query) => wire_input.set_query(query.as_str()),
                        DriftedInput::Dylib => wire_input.set_dylib(()),
                        DriftedInput::Tool(tool) => wire_input.set_tool(tool.as_str()),
                    }
                    write_stamp(drifted.init_current(), current);
                }
                ResolveResult::Failed { error } => value.set_failed(error.as_str()),
                ResolveResult::Missing => value.set_missing(()),
                ResolveResult::Deleted { at } => write_stamp(value.init_deleted(), at),
                ResolveResult::RoleIneligible { observed } => value
                    .init_role_ineligible()
                    .set_observed(wire_authoring_role(observed)),
            }
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_runtime_type_policy_result(
    mut result: schema::runtime_type_policy_call::Builder<'_>,
    outcome: RpcResult<crate::RuntimeTypePolicy>,
) {
    match outcome {
        RpcResult::Success(policy) => result.init_success().set_build_only(policy.build_only),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => result.set_snapshot_expired(()),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_path_result(
    mut result: schema::path_resolve_call::Builder<'_>,
    outcome: RpcResult<crate::TerminalEvent<PathResolveResult>>,
) {
    match outcome {
        RpcResult::Success(terminal) => {
            let mut output = result.init_success();
            write_rpc_basis(output.reborrow().init_basis(), &terminal.basis);
            let mut value = output.init_result();
            match terminal.value {
                PathResolveResult::Resolved(uuid) => value.set_resolved(&uuid.0),
                PathResolveResult::Missing => value.set_missing(()),
                PathResolveResult::Failed(PathResolveFailure::Ambiguous { candidates }) => {
                    let mut list = value.init_ambiguous(candidates.len() as u32);
                    for (index, uuid) in candidates.iter().enumerate() {
                        list.set(index as u32, &uuid.0);
                    }
                }
            }
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_fetch_result(
    mut result: schema::chunk_stream_call::Builder<'_>,
    outcome: RpcResult<crate::TerminalEvent<ChunkStream>>,
) {
    match outcome {
        RpcResult::Success(terminal) => {
            let mut output = result.init_success();
            write_rpc_basis(output.reborrow().init_basis(), &terminal.basis);
            output.set_total_bytes(terminal.value.total_bytes());
            {
                let edges = terminal.value.load_edges();
                let mut wire = output.reborrow().init_load_edges(edges.len() as u32);
                for (index, edge) in edges.iter().enumerate() {
                    let mut value = wire.reborrow().get(index as u32);
                    value.set_asset(&edge.asset.0);
                    value.set_expected_terminal(&edge.expected_terminal.0);
                }
            }
            let client: schema::chunk_stream::Client = capnp_rpc::new_client(ChunkStreamService {
                stream: RefCell::new(terminal.value),
            });
            output.set_chunks(client);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(result.init_configuration_failed(), &error)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            result.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_metadata_fetch_result(
    mut result: schema::metadata_chunk_stream_call::Builder<'_>,
    outcome: MetadataCall<ChunkStream>,
) {
    match outcome {
        MetadataCall::Success(stream) => {
            let client: schema::chunk_stream::Client = capnp_rpc::new_client(ChunkStreamService {
                stream: RefCell::new(stream),
            });
            let mut result = result;
            result.set_success(client);
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::SnapshotExpired => result.set_snapshot_expired(()),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_stream_event(
    mut output: schema::stream_event::Builder<'_>,
    event: &crate::StreamEvent,
) -> Result<(), capnp::Error> {
    write_rpc_basis(output.reborrow().init_basis(), event.basis());
    match event {
        crate::StreamEvent::InitialDelta { deltas, .. } => {
            let mut list = output.init_initial_delta(deltas.len() as u32);
            for (index, delta) in deltas.iter().enumerate() {
                write_delta(list.reborrow().get(index as u32), delta);
            }
        }
        crate::StreamEvent::Delta(delta) => write_delta(output.init_delta(), delta),
        crate::StreamEvent::ResyncRequired {
            oldest_available, ..
        } => output.set_resync_required(oldest_available.0),
        crate::StreamEvent::Asset {
            event: AssetEvent::RestartRequired { keys },
            ..
        } => {
            let mut list = output.init_restart_required(keys.len() as u32);
            for (index, key) in keys.iter().enumerate() {
                list.set(index as u32, key.as_str());
            }
        }
        crate::StreamEvent::Asset {
            event: AssetEvent::ReconnectRequired { reason },
            ..
        } => output.set_reconnect_required(wire_reconnect(*reason)),
        crate::StreamEvent::Asset { event, .. } => {
            return Err(capnp::Error::failed(format!(
                "asset event has no DeltaStream wire arm: {event:?}"
            )))
        }
    }
    Ok(())
}

fn write_delta(mut output: schema::delta::Builder<'_>, delta: &Delta) {
    write_rpc_basis(output.reborrow().init_basis(), &delta.basis);
    {
        let mut assets = output.reborrow().init_assets(delta.assets.len() as u32);
        for (index, (uuid, state)) in delta.assets.iter().enumerate() {
            let mut asset = assets.reborrow().get(index as u32);
            asset.set_uuid(&uuid.0);
            asset.set_state(match state {
                AssetDeltaState::Changed => schema::AssetDeltaState::Changed,
                AssetDeltaState::Deleted => schema::AssetDeltaState::Deleted,
                AssetDeltaState::Restored => schema::AssetDeltaState::Restored,
            });
        }
    }
    let mut paths = output.init_paths(delta.paths.len() as u32);
    for (index, path) in delta.paths.iter().enumerate() {
        paths.set(index as u32, path.as_str());
    }
}

fn write_snapshot_configuration_result(
    mut output: schema::void_call::Builder<'_>,
    outcome: RpcResult<ConfigurationStatus>,
) {
    match outcome {
        RpcResult::Success(ConfigurationStatus::Ready) => output.set_success(()),
        RpcResult::Success(ConfigurationStatus::Failed(error))
        | RpcResult::ConfigurationFailed(error) => {
            write_configuration_error(output.init_configuration_failed(), &error)
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(output.init_reconnect_required(), reason)
        }
        RpcResult::Failure(RpcFailure::SnapshotExpired) => {
            output.set_snapshot_expired(())
        }
        RpcResult::Failure(error) => {
            write_error(output.init_error(), failure_code(&error), &format!("{error:?}"))
        }
    }
}

fn write_stamp(mut output: schema::snapshot_stamp::Builder<'_>, stamp: SnapshotStamp) {
    output.set_instance(&stamp.instance.0);
    output.set_version(stamp.version.0);
}

fn write_rpc_basis(mut output: schema::rpc_basis_value::Builder<'_>, basis: &RpcBasis) {
    write_stamp(output.reborrow().init_stamp(), basis.snapshot);
}

pub fn decode_rpc_basis(
    input: schema::rpc_basis_value::Reader<'_>,
    adopted: &RpcBasis,
) -> Result<RpcBasis, capnp::Error> {
    let stamp = input.get_stamp()?;
    let decoded = RpcBasis {
        snapshot: SnapshotStamp {
            instance: StoreInstanceId(
                decode_instance(stamp.get_instance()?, "basis.stamp.instance")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            ),
            version: InputVersion(stamp.get_version()),
        },
    };
    if &decoded != adopted {
        return Err(capnp::Error::failed(
            "RPC terminal basis differs from the adopted request basis".to_owned(),
        ));
    }
    Ok(decoded)
}

fn write_authoring_stamp(
    mut output: schema::snapshot_stamp_value::Builder<'_>,
    stamp: SnapshotStamp,
) {
    output.set_instance(&stamp.instance.0);
    output.set_input_version(stamp.version.0);
}

fn write_configuration_error(
    mut output: schema::configuration_error::Builder<'_>,
    error: &ConfigurationError,
) {
    output.set_code(error.code as u16);
    output.set_reason_hash(&error.reason_hash);
    output.set_message(error.message.as_str());
    output.set_detail_version(1);
    output.set_detail_bytes(&error.detail.canonical_detail_bytes());
}

fn write_pipeline_unavailable(
    mut output: schema::pipeline_unavailable_diagnostic::Builder<'_>,
    diagnostic: &crate::PipelineUnavailableDiagnostic,
) -> Result<(), capnp::Error> {
    match diagnostic {
        crate::PipelineUnavailableDiagnostic::PipelineFailure(failure) => {
            write_pipeline_failure(output.reborrow().init_pipeline_failure(), failure)
        }
    }
}

fn write_pipeline_failure(
    mut output: schema::pipeline_failure::Builder<'_>,
    failure: &crate::PipelineFailure,
) -> Result<(), capnp::Error> {
    failure
        .validate()
        .map_err(|error| capnp::Error::failed(format!("invalid DSPP diagnostic: {error}")))?;
    output.set_code(match failure.code {
        crate::PipelineFailureCode::CandidateOpen => schema::PipelineFailureCode::CandidateOpen,
        crate::PipelineFailureCode::CandidateAttestation => {
            schema::PipelineFailureCode::CandidateAttestation
        }
        crate::PipelineFailureCode::CandidateRegistration => {
            schema::PipelineFailureCode::CandidateRegistration
        }
        crate::PipelineFailureCode::CandidateValidation => {
            schema::PipelineFailureCode::CandidateValidation
        }
        crate::PipelineFailureCode::CandidateCleanup => {
            schema::PipelineFailureCode::CandidateCleanup
        }
        crate::PipelineFailureCode::PublishedCallbackPanic => {
            schema::PipelineFailureCode::PublishedCallbackPanic
        }
        crate::PipelineFailureCode::PublishedCallbackRejected => {
            schema::PipelineFailureCode::PublishedCallbackRejected
        }
        crate::PipelineFailureCode::PublishedCleanup => {
            schema::PipelineFailureCode::PublishedCleanup
        }
    });
    output.set_origin(match failure.origin {
        crate::PipelineFailureOrigin::CandidateOpen => schema::PipelineFailureOrigin::CandidateOpen,
        crate::PipelineFailureOrigin::PublishedRuntime => {
            schema::PipelineFailureOrigin::PublishedRuntime
        }
    });
    output.set_cleanup(match failure.cleanup {
        crate::CleanupDisposition::None => schema::CleanupDisposition::None,
        crate::CleanupDisposition::CleanedAndClosed => schema::CleanupDisposition::CleanedAndClosed,
        crate::CleanupDisposition::RegistrationCleanupFailed => {
            schema::CleanupDisposition::RegistrationCleanupFailed
        }
        crate::CleanupDisposition::ModuleUnloadFailed => {
            schema::CleanupDisposition::ModuleUnloadFailed
        }
        crate::CleanupDisposition::TokenPoisoned => schema::CleanupDisposition::TokenPoisoned,
        crate::CleanupDisposition::TokenPinned => schema::CleanupDisposition::TokenPinned,
        crate::CleanupDisposition::DlcloseFailed => schema::CleanupDisposition::DlcloseFailed,
        crate::CleanupDisposition::PublishedEpochLeaked => {
            schema::CleanupDisposition::PublishedEpochLeaked
        }
    });
    output.set_identity(&failure.identity);
    output.set_message(failure.message.as_str());
    Ok(())
}

fn write_namespace_error(mut output: schema::namespace_error::Builder<'_>, error: &NamespaceError) {
    debug_assert!(error.validate().is_ok());
    output.set_code(error.code as u16);
    output.set_identity(&error.identity);
    output.set_message(error.message.as_str());
    let detail = output.init_detail();
    match &error.detail {
        NamespaceErrorV1::DuplicateAssetUuid { asset, claimants } => {
            let mut value = detail.init_duplicate_asset_uuid();
            value.reborrow().init_asset().set_bytes(&asset.0);
            let mut rows = value.init_claimants(claimants.len() as u32);
            for (index, claimant) in claimants.iter().enumerate() {
                let mut row = rows.reborrow().get(index as u32);
                match claimant {
                    crate::AssetClaimant::Authored {
                        source,
                        bundle,
                        local_id,
                    } => {
                        let mut authored = row.reborrow().init_authored();
                        write_bundle_source(authored.reborrow().init_source(), source);
                        authored.reborrow().init_bundle().set_bytes(&bundle.0);
                        authored.set_local_id(local_id.as_str());
                    }
                    crate::AssetClaimant::Derived { parent, output_key } => {
                        let mut derived = row.init_derived();
                        derived.reborrow().init_parent().set_bytes(&parent.0);
                        derived.set_output_key(output_key.as_str());
                    }
                }
            }
        }
        NamespaceErrorV1::DuplicateBundleUuid { bundle, sources } => {
            let mut value = detail.init_duplicate_bundle_uuid();
            value.reborrow().init_bundle().set_bytes(&bundle.0);
            write_bundle_sources(value.init_sources(sources.len() as u32), sources);
        }
        NamespaceErrorV1::SameRootNormalizedPathCollision {
            root_name,
            normalized_path,
            claims,
        } => {
            let mut value = detail.init_same_root_normalized_path_collision();
            value.set_root_name(root_name.as_str());
            value.set_normalized_path(normalized_path.as_str());
            let mut rows = value.init_claims(claims.len() as u32);
            for (index, claim) in claims.iter().enumerate() {
                let mut row = rows.reborrow().get(index as u32);
                row.set_file_hash(&claim.file_hash.0);
                let mut path = row.init_raw_relative_path();
                match &claim.raw_relative_path {
                    crate::PlatformPathBytes::Unix(bytes) => path.set_unix_bytes(bytes),
                    crate::PlatformPathBytes::Windows(units) => {
                        let mut bytes = Vec::with_capacity(units.len() * 2);
                        for unit in units {
                            bytes.extend_from_slice(&unit.to_le_bytes());
                        }
                        path.set_windows_utf16_le(&bytes);
                    }
                }
            }
        }
        NamespaceErrorV1::IncompleteSkeleton { source, failure } => {
            let mut value = detail.init_incomplete_skeleton();
            write_bundle_source(value.reborrow().init_source(), source);
            value.set_failure_code(*failure as u16);
        }
        NamespaceErrorV1::UnreadableGlobalBundlePath {
            root_name,
            normalized_path,
            failure,
        } => {
            let mut value = detail.init_unreadable_global_bundle_path();
            value.set_root_name(root_name.as_str());
            value.set_normalized_path(normalized_path.as_str());
            value.set_failure_code(*failure as u16);
        }
        NamespaceErrorV1::InvalidPhysicalPath {
            root_name,
            raw_relative_path,
            failure,
        } => {
            let mut value = detail.init_invalid_physical_path();
            value.set_root_name(root_name.as_str());
            let mut path = value.reborrow().init_raw_relative_path();
            match raw_relative_path {
                crate::PlatformPathBytes::Unix(bytes) => path.set_unix_bytes(bytes),
                crate::PlatformPathBytes::Windows(units) => {
                    let mut bytes = Vec::with_capacity(units.len() * 2);
                    for unit in units {
                        bytes.extend_from_slice(&unit.to_le_bytes());
                    }
                    path.set_windows_utf16_le(&bytes);
                }
            }
            value.set_failure_code(*failure as u16);
        }
        NamespaceErrorV1::UnreadableScanSubtree { subject, failure } => {
            let mut value = detail.init_unreadable_scan_subtree();
            let subject_output = value.reborrow().init_subject();
            match subject {
                crate::ScanSubject::Root { root_name } => {
                    subject_output.init_root().set_root_name(root_name.as_str());
                }
                crate::ScanSubject::Subtree {
                    root_name,
                    raw_relative_path,
                } => {
                    let mut subtree = subject_output.init_subtree();
                    subtree.set_root_name(root_name.as_str());
                    let mut path = subtree.init_raw_relative_path();
                    match raw_relative_path {
                        crate::PlatformPathBytes::Unix(bytes) => path.set_unix_bytes(bytes),
                        crate::PlatformPathBytes::Windows(units) => {
                            let mut bytes = Vec::with_capacity(units.len() * 2);
                            for unit in units {
                                bytes.extend_from_slice(&unit.to_le_bytes());
                            }
                            path.set_windows_utf16_le(&bytes);
                        }
                    }
                }
            }
            value.set_failure(match failure {
                crate::ScanFailureCode::PermissionDenied => {
                    schema::ScanFailureCodeValue::PermissionDenied
                }
                crate::ScanFailureCode::NotFound => schema::ScanFailureCodeValue::NotFound,
                crate::ScanFailureCode::InvalidFileType => {
                    schema::ScanFailureCodeValue::InvalidFileType
                }
                crate::ScanFailureCode::SymlinkIdentityChanged => {
                    schema::ScanFailureCodeValue::SymlinkIdentityChanged
                }
                crate::ScanFailureCode::IoDataLoss => schema::ScanFailureCodeValue::IoDataLoss,
            });
        }
    }
}

fn write_bundle_sources(
    mut output: capnp::struct_list::Builder<'_, schema::bundle_source::Owned>,
    sources: &[crate::ReadableBundleSource],
) {
    for (index, source) in sources.iter().enumerate() {
        write_bundle_source(output.reborrow().get(index as u32), source);
    }
}

fn write_bundle_source(
    mut output: schema::bundle_source::Builder<'_>,
    source: &crate::ReadableBundleSource,
) {
    output.set_root_name(source.root_name.as_str());
    output.set_normalized_path(source.normalized_path.as_str());
    output.set_file_hash(&source.file_hash.0);
}

fn write_error(mut output: schema::rpc_error::Builder<'_>, code: u16, message: &str) {
    output.set_code(code);
    output.set_message(message);
}

fn write_wire_error(output: schema::rpc_error::Builder<'_>, failure: &WireFailure) {
    write_error(output, failure.code, failure.message.as_str());
}

fn write_connect_error(mut result: schema::connect_call::Builder<'_>, error: &crate::ConnectError) {
    match error {
        crate::ConnectError::ProtocolMismatch { expected, got } => {
            let mut failure = result.reborrow().init_protocol_failure();
            failure.set_expected(*expected);
            failure.set_observed(*got);
            failure.set_message(format!("{error:?}").as_str());
        }
        crate::ConnectError::UnknownTarget { .. } => {
            let mut failure = result.reborrow().init_target_failure();
            failure.set_code(schema::TargetFailureCode::UnknownTarget);
            failure.set_expected(&[]);
            failure.set_observed(&[]);
        }
        crate::ConnectError::TargetDefinitionMismatch { expected, got } => {
            let mut failure = result.reborrow().init_target_failure();
            failure.set_code(schema::TargetFailureCode::DefinitionMismatch);
            failure.set_expected(&expected.0);
            failure.set_observed(&got.0);
        }
    }
}

fn write_rpc_result_error_snapshot(
    mut result: schema::snapshot_call::Builder<'_>,
    error: RpcFailure,
) {
    if error == RpcFailure::SnapshotExpired {
        result.reborrow().set_snapshot_expired(());
    } else {
        write_error(
            result.init_error(),
            failure_code(&error),
            format!("{error:?}").as_str(),
        );
    }
}

fn write_rpc_result_error_subscribe(
    mut result: schema::subscribe_call::Builder<'_>,
    error: RpcFailure,
) {
    if error == RpcFailure::SnapshotExpired {
        result.reborrow().set_snapshot_expired(());
    } else {
        write_error(
            result.init_error(),
            failure_code(&error),
            format!("{error:?}").as_str(),
        );
    }
}

fn write_reconnect(mut output: schema::reconnect_required::Builder<'_>, reason: ReconnectReason) {
    output.set_reason(wire_reconnect(reason));
}

fn write_metadata_reconnect(
    mut output: schema::metadata_reconnect_required::Builder<'_>,
    reason: MetadataReconnectReason,
) {
    output.set_reason(match reason {
        MetadataReconnectReason::StoreInstanceChanged => {
            schema::MetadataReconnectReason::StoreInstanceChanged
        }
        MetadataReconnectReason::ProtocolEpochChanged => {
            schema::MetadataReconnectReason::ProtocolEpochChanged
        }
    });
}

fn wire_reconnect(reason: ReconnectReason) -> schema::ReconnectReason {
    match reason {
        ReconnectReason::TargetDefinitionChanged => {
            schema::ReconnectReason::TargetDefinitionChanged
        }
        ReconnectReason::StoreInstanceChanged => schema::ReconnectReason::StoreInstanceChanged,
        ReconnectReason::ProtocolEpochChanged => schema::ReconnectReason::ProtocolEpochChanged,
        ReconnectReason::PipelineEpochChanged => schema::ReconnectReason::PipelineEpochChanged,
    }
}
