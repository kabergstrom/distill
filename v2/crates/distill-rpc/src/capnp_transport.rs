//! Concrete Cap'n Proto transport adapter.
//!
//! Protocol state remains in the transport-neutral [`crate::Server`]. This
//! module validates wire widths, translates typed result unions, and drives
//! capnp-rpc on a Tokio [`tokio::task::LocalSet`]. `RpcSystem` is deliberately
//! `!Send`; callers must run listener and client tasks on that one IO thread.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::compat::TokioAsyncReadCompatExt;
use unicode_normalization::UnicodeNormalization;

use crate::{
    AssetDeltaState, AssetEvent, AssetQuery, AssetUuid, AuthoringEntryRole, AuthoringInspectResult,
    AuthoringInspection, AuthoringSnapshot, BundleFileHash, BundleUuid, ChunkStream,
    ConfigurationPoison, ConfigurationStatus, ConnectError, ConnectOutcome, ConnectRequest,
    ContentHash, Delta, DeltaStream, DriftedInput, Hub, InputVersion, LayoutHash,
    LineageManifestClaimant, LineageRepair, LineageRepairConnectOutcome, LineageRepairDestination,
    LineageRepairInspectOutcome, LineageRepairInspection, LineageRepairInvalid,
    LineageRepairInvalidCode, LineageRepairMutationOutcome, LineageRepairStale,
    LineageRepairStaleCode, LineageRepairState, LineageRepairUnavailable,
    MetadataAuthoringSnapshot, MetadataCall, MetadataConnectOutcome, MetadataDiagnostics,
    MetadataEntry, MetadataHub, MetadataNamespaceCall, MetadataReconnectReason, MetadataSnapshot,
    OccupiedLineageDestinationKind, PathResolveFailure, PathResolveResult, ProgressStream,
    PureMetadataEntry, PureMetadataQuery, ReconnectReason, ResolveResult, Root, RpcBasis,
    RpcFailure, RpcResult, Snapshot, SnapshotStamp, StoreInstanceId, TagSelector,
    TargetDefinitionHash, TypeUuid, VersionPoison, VersionPoisonV1,
};

pub use crate::distill_rpc_capnp as schema;

const WIRE_INVALID_UUID: u16 = 1001;
const WIRE_INVALID_HASH: u16 = 1002;
const WIRE_INVALID_INSTANCE: u16 = 1003;
const WIRE_INVALID_UTF8: u16 = 1004;
const WIRE_INVALID_VALUE: u16 = 1005;
const RPC_FAILURE: u16 = 3000;

#[derive(Debug)]
pub enum TransportError {
    BindValidation(crate::BindStageError),
    Io(io::Error),
    Capnp(capnp::Error),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BindValidation(error) => write!(f, "RPC bind validation failed: {error}"),
            Self::Io(error) => write!(f, "RPC I/O failed: {error}"),
            Self::Capnp(error) => write!(f, "Cap'n Proto RPC failed: {error}"),
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

/// A listener whose address passed the loopback-only configuration staging
/// gate before any socket was opened.
pub struct StagedListener {
    listener: TcpListener,
    root: Root,
}

impl StagedListener {
    pub async fn bind(root: Root, address: &str) -> Result<Self, TransportError> {
        let address =
            crate::validate_bind_address(address).map_err(TransportError::BindValidation)?;
        let listener = TcpListener::bind(address).await?;
        Ok(Self { listener, root })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept one connection and spawn its `RpcSystem` on the current
    /// [`tokio::task::LocalSet`]. The returned handle exposes connection-level
    /// protocol failure to supervisors that want to observe it.
    pub async fn accept_one(&self) -> Result<JoinHandle<Result<(), capnp::Error>>, TransportError> {
        let (stream, peer) = self.listener.accept().await?;
        if !peer.ip().is_loopback() {
            return Err(TransportError::BindValidation(
                crate::BindStageError::NonLoopbackAddress {
                    address: peer.to_string(),
                },
            ));
        }
        stream.set_nodelay(true)?;
        let root_client: schema::root::Client = capnp_rpc::new_client(RootService {
            root: self.root.clone(),
        });
        Ok(tokio::task::spawn_local(run_rpc_system(
            stream,
            Some(root_client.client),
        )))
    }

    /// Accept and drive one connection to completion.
    pub async fn serve_one(&self) -> Result<(), TransportError> {
        match self.accept_one().await?.await {
            Ok(result) => result.map_err(TransportError::Capnp),
            Err(error) => Err(TransportError::Capnp(capnp::Error::failed(format!(
                "RPC connection task failed: {error}"
            )))),
        }
    }

    /// Production accept loop. Each connection remains on the same local IO
    /// thread while independent `RpcSystem`s make progress concurrently.
    pub async fn serve(&self) -> Result<(), TransportError> {
        loop {
            // Dropping a Tokio JoinHandle detaches the task; the connection's
            // own RpcSystem continues until disconnect.
            drop(self.accept_one().await?);
        }
    }

    /// Production accept loop with supervisor-owned shutdown. Existing
    /// connection tasks are detached and finish independently; shutdown stops
    /// admitting new unauthenticated loopback peers immediately.
    pub async fn serve_until<F>(&self, shutdown: F) -> Result<(), TransportError>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                result = self.accept_one() => drop(result?),
                () = &mut shutdown => return Ok(()),
            }
        }
    }
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
    ConfigurationPoisoned(ConfigurationPoison),
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
            Self::ConfigurationPoisoned(poison) => f
                .debug_tuple("ConfigurationPoisoned")
                .field(poison)
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
                ConnectOutcome::ConfigurationPoisoned(poison) => {
                    write_poison(result.init_configuration_poisoned(), &poison);
                }
                ConnectOutcome::PipelineUnavailable(diagnostic) => {
                    write_pipeline_unavailable(result.init_pipeline_unavailable(), &diagnostic)?;
                }
                ConnectOutcome::Rejected(error) => {
                    write_connect_error(result, &error);
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
            }
            Ok(())
        }
    }

    fn lineage_repair(
        self: capnp::capability::Rc<Self>,
        params: schema::root::LineageRepairParams,
        mut results: schema::root::LineageRepairResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let protocol = params.get()?.get_protocol();
            let mut result = results.get().init_result();
            match self.root.lineage_repair(protocol) {
                LineageRepairConnectOutcome::Connected(connected) => {
                    let client: schema::lineage_repair::Client =
                        capnp_rpc::new_client(LineageRepairService {
                            repair: connected.repair,
                        });
                    result.set_success(client);
                }
                LineageRepairConnectOutcome::Unavailable(unavailable) => {
                    write_lineage_unavailable(result.init_unavailable(), &unavailable);
                }
                LineageRepairConnectOutcome::ProtocolMismatch { expected, observed } => {
                    let mut failure = result.init_protocol_failure();
                    failure.set_expected(expected);
                    failure.set_observed(observed);
                    failure.set_message("lineage repair bootstrap protocol mismatch");
                }
            }
            Ok(())
        }
    }
}

struct LineageRepairService {
    repair: LineageRepair,
}

#[allow(clippy::manual_async_fn)]
impl schema::lineage_repair::Server for LineageRepairService {
    fn inspect(
        self: capnp::capability::Rc<Self>,
        _params: schema::lineage_repair::InspectParams,
        mut results: schema::lineage_repair::InspectResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_lineage_inspect_result(results.get().init_result(), self.repair.inspect());
            Ok(())
        }
    }

    fn create_missing(
        self: capnp::capability::Rc<Self>,
        params: schema::lineage_repair::CreateMissingParams,
        mut results: schema::lineage_repair::CreateMissingResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let params = params.get()?;
            let basis = match decode_lineage_inspection(params.get_basis()?) {
                Ok(basis) => basis,
                Err(error) => {
                    write_error(
                        results.get().init_result().init_error(),
                        error.code,
                        &error.message,
                    );
                    return Ok(());
                }
            };
            let bytes: Arc<[u8]> = Arc::from(params.get_canonical_manifest_bundle()?);
            write_lineage_mutation_result(
                results.get().init_result(),
                self.repair.create_missing(basis, bytes),
            );
            Ok(())
        }
    }

    fn resolve_duplicate(
        self: capnp::capability::Rc<Self>,
        params: schema::lineage_repair::ResolveDuplicateParams,
        mut results: schema::lineage_repair::ResolveDuplicateResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let params = params.get()?;
            let basis = match decode_lineage_inspection(params.get_basis()?) {
                Ok(basis) => basis,
                Err(error) => {
                    write_error(
                        results.get().init_result().init_error(),
                        error.code,
                        &error.message,
                    );
                    return Ok(());
                }
            };
            let survivor = match decode_lineage_claimant(params.get_survivor()?) {
                Ok(survivor) => survivor,
                Err(error) => {
                    write_error(
                        results.get().init_result().init_error(),
                        error.code,
                        &error.message,
                    );
                    return Ok(());
                }
            };
            write_lineage_mutation_result(
                results.get().init_result(),
                self.repair.resolve_duplicate(basis, survivor),
            );
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
            write_uint64_result(
                results.get().init_result(),
                self.hub
                    .write(InputVersion(params.get_base()), ops)
                    .map_success(|version| version.0),
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
            write_bundle_uuid_result(
                results.get().init_result(),
                self.hub.import(InputVersion(params.get_base()), request),
            );
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
            write_bundle_uuid_result(
                results.get().init_result(),
                self.hub.reimport(InputVersion(params.get_base()), bundle),
            );
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

    fn fetch(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::FetchParams,
        mut results: schema::hub::FetchResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            if let Some(reason) = self.hub.generation_reconnect() {
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
            write_fetch_result(results.get().init_result(), self.hub.fetch_latest(hash));
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
            let uuid = match decode_uuid(params.get()?.get_uuid()?, "uuid") {
                Ok(uuid) => AssetUuid(uuid),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            // Lazy resolution may synchronously execute a complete processor
            // chain. Keep that work off the single-threaded capnp-rpc driver;
            // the daemon build scheduler provides the actual admission bound
            // while this future yields so unrelated connections keep moving.
            let snapshot = self.snapshot.clone();
            let outcome = tokio::task::spawn_blocking(move || snapshot.resolve(uuid))
                .await
                .map_err(|error| {
                    capnp::Error::failed(format!("snapshot resolve worker failed: {error}"))
                })?;
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
    stream: Mutex<ChunkStream>,
}

struct ProgressStreamService {
    stream: Mutex<ProgressStream>,
}

#[allow(clippy::manual_async_fn)]
impl schema::progress_stream::Server for ProgressStreamService {
    fn next(
        self: capnp::capability::Rc<Self>,
        _params: schema::progress_stream::NextParams,
        mut results: schema::progress_stream::NextResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let progress = self
                .stream
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .next();
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
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
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
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
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
            let event = self.stream.next_async().await;
            let mut output = results.get();
            output.set_done(false);
            write_stream_event(output.init_event(), &event)?;
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
        schema::long_running_op::Which::DiskMigration(payload) => {
            Ok(crate::LongRunningOp::DiskMigration(
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
        Which::ConfigurationPoisoned(poison) => Ok(RemoteConnectOutcome::ConfigurationPoisoned(
            decode_configuration_poison(poison?)?,
        )),
        Which::PipelineUnavailable(diagnostic) => {
            let diagnostic = diagnostic?;
            let diagnostic = match diagnostic
                .which()
                .map_err(|error| capnp::Error::failed(error.to_string()))?
            {
                schema::pipeline_unavailable_diagnostic::Which::PipelinePoison(poison) => {
                    crate::PipelineUnavailableDiagnostic::PipelinePoison(decode_pipeline_poison(
                        poison?,
                    )?)
                }
                schema::pipeline_unavailable_diagnostic::Which::SchemaAcceptanceRequired(
                    required,
                ) => crate::PipelineUnavailableDiagnostic::SchemaAcceptanceRequired(
                    decode_schema_acceptance_required(required?)?,
                ),
                schema::pipeline_unavailable_diagnostic::Which::RetiredTypeReferenced(retired) => {
                    crate::PipelineUnavailableDiagnostic::RetiredTypeReferenced(
                        decode_retired_type_referenced(retired?)?,
                    )
                }
            };
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
    crate::server::decode_authoring_payload(schema_hash, &logical_schema, &authored_value)
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

pub fn decode_configuration_poison(
    poison: schema::configuration_poison::Reader<'_>,
) -> Result<ConfigurationPoison, capnp::Error> {
    if poison.get_detail_version() != 1 {
        return Err(capnp::Error::failed(format!(
            "unsupported configuration poison detail version {}",
            poison.get_detail_version()
        )));
    }
    let code = crate::ConfigurationPoisonCode::try_from(poison.get_code())
        .map_err(|error| capnp::Error::failed(error.to_string()))?;
    let detail = crate::DscpV1::from_canonical_detail_bytes(code, poison.get_detail_bytes()?)
        .map_err(|error| capnp::Error::failed(format!("invalid DSCP v1 detail: {error}")))?;
    let value = ConfigurationPoison {
        code,
        reason_hash: decode_hash(poison.get_reason_hash()?, "reasonHash")
            .map_err(|error| capnp::Error::failed(error.message))?,
        detail: Box::new(detail),
        message: decode_text(poison.get_message()?, "poison.message")
            .map_err(|error| capnp::Error::failed(error.message))?,
    };
    value
        .validate()
        .map_err(|error| capnp::Error::failed(format!("invalid configuration poison: {error}")))?;
    Ok(value)
}

pub fn decode_version_poison(
    poison: schema::version_poison::Reader<'_>,
) -> Result<VersionPoison, capnp::Error> {
    let detail = poison.get_detail()?;
    let detail = match detail
        .which()
        .map_err(|error| capnp::Error::failed(error.to_string()))?
    {
        schema::version_poison_detail::Which::DuplicateAssetUuid(value) => {
            let value = value?;
            let asset = AssetUuid(
                decode_uuid(value.get_asset()?.get_bytes()?, "versionPoison.asset")
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
                                    source: read_version_poison_source(authored.get_source()?)?,
                                    bundle: crate::BundleUuid(
                                        decode_uuid(
                                            authored.get_bundle()?.get_bytes()?,
                                            "versionPoison.claimant.bundle",
                                        )
                                        .map_err(|error| capnp::Error::failed(error.message))?,
                                    ),
                                    local_id: decode_text(
                                        authored.get_local_id()?,
                                        "versionPoison.claimant.localId",
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
                                            "versionPoison.claimant.parent",
                                        )
                                        .map_err(|error| capnp::Error::failed(error.message))?,
                                    ),
                                    output_key: decode_text(
                                        derived.get_output_key()?,
                                        "versionPoison.claimant.outputKey",
                                    )
                                    .map_err(|error| capnp::Error::failed(error.message))?,
                                }
                            }
                        },
                    )
                })
                .collect::<Result<Vec<_>, capnp::Error>>()?;
            VersionPoisonV1::DuplicateAssetUuid { asset, claimants }
        }
        schema::version_poison_detail::Which::DuplicateBundleUuid(value) => {
            let value = value?;
            VersionPoisonV1::DuplicateBundleUuid {
                bundle: crate::BundleUuid(
                    decode_uuid(value.get_bundle()?.get_bytes()?, "versionPoison.bundle")
                        .map_err(|error| capnp::Error::failed(error.message))?,
                ),
                sources: value
                    .get_sources()?
                    .iter()
                    .map(read_version_poison_source)
                    .collect::<Result<Vec<_>, _>>()?,
            }
        }
        schema::version_poison_detail::Which::SameRootNormalizedPathCollision(value) => {
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
                                    "version poison Windows path has odd byte length".to_owned(),
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
                            decode_hash(claim.get_file_hash()?, "versionPoison.claim.fileHash")
                                .map_err(|error| capnp::Error::failed(error.message))?,
                        ),
                    })
                })
                .collect::<Result<Vec<_>, capnp::Error>>()?;
            VersionPoisonV1::SameRootNormalizedPathCollision {
                root_name: decode_text(value.get_root_name()?, "versionPoison.rootName")
                    .map_err(|error| capnp::Error::failed(error.message))?,
                normalized_path: decode_text(
                    value.get_normalized_path()?,
                    "versionPoison.normalizedPath",
                )
                .map_err(|error| capnp::Error::failed(error.message))?,
                claims,
            }
        }
        schema::version_poison_detail::Which::IncompleteSkeleton(value) => {
            let value = value?;
            VersionPoisonV1::IncompleteSkeleton {
                source: read_version_poison_source(value.get_source()?)?,
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
        schema::version_poison_detail::Which::UnreadableGlobalBundlePath(value) => {
            let value = value?;
            VersionPoisonV1::UnreadableGlobalBundlePath {
                root_name: decode_text(value.get_root_name()?, "versionPoison.rootName")
                    .map_err(|error| capnp::Error::failed(error.message))?,
                normalized_path: decode_text(
                    value.get_normalized_path()?,
                    "versionPoison.normalizedPath",
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
        schema::version_poison_detail::Which::InvalidPhysicalPath(value) => {
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
                            "version poison Windows path has odd byte length".to_owned(),
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
            VersionPoisonV1::InvalidPhysicalPath {
                root_name: decode_text(value.get_root_name()?, "versionPoison.rootName")
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
        schema::version_poison_detail::Which::UnreadableScanSubtree(value) => {
            let value = value?;
            let subject = match value
                .get_subject()?
                .which()
                .map_err(|error| capnp::Error::failed(error.to_string()))?
            {
                schema::scan_subject::Which::Root(root) => crate::ScanSubject::Root {
                    root_name: decode_text(root?.get_root_name()?, "versionPoison.scan.rootName")
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
                                    "version poison Windows scan path has odd byte length"
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
                            "versionPoison.scan.rootName",
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
            VersionPoisonV1::UnreadableScanSubtree { subject, failure }
        }
    };
    let message = decode_text(poison.get_message()?, "versionPoison.message")
        .map_err(|error| capnp::Error::failed(error.message))?;
    let decoded = VersionPoison::new(detail, message)
        .map_err(|error| capnp::Error::failed(error.to_string()))?;
    let identity = decode_hash(poison.get_identity()?, "versionPoison.identity")
        .map_err(|error| capnp::Error::failed(error.message))?;
    if decoded.code as u16 != poison.get_code() || decoded.identity != identity {
        return Err(capnp::Error::failed(
            "version poison code/detail/identity mismatch".to_owned(),
        ));
    }
    Ok(decoded)
}

pub fn decode_pipeline_poison(
    poison: schema::pipeline_poison::Reader<'_>,
) -> Result<crate::PipelinePoison, capnp::Error> {
    let code = poison.get_code()? as u16 + 1;
    let origin = poison.get_origin()? as u16 + 1;
    let cleanup = poison.get_cleanup()? as u16;
    let identity = decode_hash(poison.get_identity()?, "pipelinePoison.identity")
        .map_err(|error| capnp::Error::failed(error.message))?;
    let message = decode_text(poison.get_message()?, "pipelinePoison.message")
        .map_err(|error| capnp::Error::failed(error.message))?;
    crate::PipelinePoison::from_wire(code, origin, cleanup, identity, message)
        .map_err(|error| capnp::Error::failed(error.to_string()))
}

pub fn decode_schema_acceptance_required(
    value: schema::schema_acceptance_required::Reader<'_>,
) -> Result<crate::SchemaAcceptanceRequired, capnp::Error> {
    let manifest = value.get_manifest()?;
    let mut current_cursors = std::collections::BTreeMap::new();
    let mut previous = None;
    for (index, row) in manifest.get_current_cursors()?.iter().enumerate() {
        let type_uuid = TypeUuid(
            decode_uuid(
                row.get_type_uuid()?.get_bytes()?,
                &format!("schemaAcceptance.manifest.currentCursors[{index}].typeUuid"),
            )
            .map_err(|error| capnp::Error::failed(error.message))?,
        );
        if previous.is_some_and(|prior| prior >= type_uuid) {
            return Err(capnp::Error::failed(
                "schema manifest cursors are not strictly TypeUuid-sorted".to_owned(),
            ));
        }
        previous = Some(type_uuid);
        let logical_hash = crate::LogicalHash(
            decode_hash(
                row.get_logical_hash()?,
                &format!("schemaAcceptance.manifest.currentCursors[{index}].logicalHash"),
            )
            .map_err(|error| capnp::Error::failed(error.message))?,
        );
        current_cursors.insert(type_uuid, logical_hash);
    }
    let candidate = value.get_candidate()?;
    let mut mismatches = Vec::with_capacity(value.get_mismatches()?.len() as usize);
    let mut previous = None;
    for (index, row) in value.get_mismatches()?.iter().enumerate() {
        let type_uuid = TypeUuid(
            decode_uuid(
                row.get_type_uuid()?.get_bytes()?,
                &format!("schemaAcceptance.mismatches[{index}].typeUuid"),
            )
            .map_err(|error| capnp::Error::failed(error.message))?,
        );
        if previous.is_some_and(|prior| prior >= type_uuid)
            || distill_core::bootstrap::is_bootstrap_control_type(type_uuid)
        {
            return Err(capnp::Error::failed(
                "schema mismatches are not strict non-bootstrap rows".to_owned(),
            ));
        }
        previous = Some(type_uuid);
        let candidate_bytes = row.get_candidate()?;
        let candidate_hash = if row.get_has_candidate() {
            Some(crate::LogicalHash(
                decode_hash(candidate_bytes, "schemaAcceptance.candidate")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            ))
        } else {
            if !candidate_bytes.is_empty() {
                return Err(capnp::Error::failed(
                    "schema mismatch hasCandidate=false with payload".to_owned(),
                ));
            }
            None
        };
        let manifest_bytes = row.get_manifest()?;
        let manifest_hash = if row.get_has_manifest() {
            Some(crate::LogicalHash(
                decode_hash(manifest_bytes, "schemaAcceptance.manifest")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            ))
        } else {
            if !manifest_bytes.is_empty() {
                return Err(capnp::Error::failed(
                    "schema mismatch hasManifest=false with payload".to_owned(),
                ));
            }
            None
        };
        if candidate_hash == manifest_hash {
            return Err(capnp::Error::failed(
                "schema mismatch row does not disagree".to_owned(),
            ));
        }
        mismatches.push(crate::SchemaRegistryMismatch {
            type_uuid,
            candidate: candidate_hash,
            manifest: manifest_hash,
        });
    }
    if mismatches.is_empty() {
        return Err(capnp::Error::failed(
            "schema mismatch table must not be empty".to_owned(),
        ));
    }
    Ok(crate::SchemaAcceptanceRequired {
        manifest: crate::SchemaManifestBasis {
            manifest_hash: ContentHash(
                decode_hash(
                    manifest.get_manifest_hash()?,
                    "schemaAcceptance.manifestHash",
                )
                .map_err(|error| capnp::Error::failed(error.message))?,
            ),
            current_cursors,
        },
        candidate: crate::PipelineCandidateIdentity {
            dylib_hash: decode_hash(candidate.get_dylib_hash()?, "schemaAcceptance.dylibHash")
                .map_err(|error| capnp::Error::failed(error.message))?,
            target_set: {
                let rows = candidate.get_target_rows()?;
                let mut decoded = Vec::with_capacity(rows.len() as usize);
                for row in rows {
                    decoded.push(distill_core::target_set::TargetSetRow {
                        name: row.get_name()?.to_str()?.to_owned(),
                        target_definition_hash: decode_hash(
                            row.get_target_definition_hash()?,
                            "schemaAcceptance.targetDefinitionHash",
                        )
                        .map_err(|error| capnp::Error::failed(error.message))?,
                    });
                }
                distill_core::target_set::CanonicalTargetSet::from_canonical(decoded).map_err(
                    |error| capnp::Error::failed(format!("invalid target rows: {error}")),
                )?
            },
        },
        mismatches,
    })
}

pub fn decode_retired_type_referenced(
    value: schema::retired_type_referenced::Reader<'_>,
) -> Result<crate::RetiredTypeReferenced, capnp::Error> {
    let basis = value.get_basis()?;
    let basis = SnapshotStamp {
        instance: StoreInstanceId(
            decode_instance(basis.get_instance()?, "retiredType.basis.instance")
                .map_err(|error| capnp::Error::failed(error.message))?,
        ),
        version: InputVersion(basis.get_input_version()),
    };
    let mut references = Vec::with_capacity(value.get_references()?.len() as usize);
    let mut previous: Option<Vec<u8>> = None;
    for row in value.get_references()?.iter() {
        let reference = match row
            .which()
            .map_err(|error| capnp::Error::failed(error.to_string()))?
        {
            schema::retired_type_reference::Which::Asset(asset) => {
                crate::RetiredTypeReference::Asset(AssetUuid(
                    decode_uuid(asset?.get_bytes()?, "retiredType.reference.asset")
                        .map_err(|error| capnp::Error::failed(error.message))?,
                ))
            }
            schema::retired_type_reference::Which::MigrationEndpoint(hash) => {
                crate::RetiredTypeReference::MigrationEndpoint(crate::LogicalHash(
                    decode_hash(hash?, "retiredType.reference.migrationEndpoint")
                        .map_err(|error| capnp::Error::failed(error.message))?,
                ))
            }
        };
        let mut encoded = Vec::with_capacity(33);
        match reference {
            crate::RetiredTypeReference::Asset(asset) => {
                encoded.push(1);
                encoded.extend_from_slice(&asset.0);
            }
            crate::RetiredTypeReference::MigrationEndpoint(hash) => {
                encoded.push(2);
                encoded.extend_from_slice(&hash.0);
            }
        }
        if previous.as_ref().is_some_and(|prior| prior >= &encoded) {
            return Err(capnp::Error::failed(
                "retired references are not strictly canonical".to_owned(),
            ));
        }
        previous = Some(encoded);
        references.push(reference);
    }
    if references.is_empty() {
        return Err(capnp::Error::failed(
            "retired reference table must not be empty".to_owned(),
        ));
    }
    Ok(crate::RetiredTypeReferenced {
        manifest_hash: crate::BundleFileHash(
            decode_hash(value.get_manifest_hash()?, "retiredType.manifestHash")
                .map_err(|error| capnp::Error::failed(error.message))?,
        ),
        basis,
        type_uuid: TypeUuid(
            decode_uuid(value.get_type_uuid()?.get_bytes()?, "retiredType.typeUuid")
                .map_err(|error| capnp::Error::failed(error.message))?,
        ),
        references,
    })
}

fn read_version_poison_source(
    source: schema::version_poison_source::Reader<'_>,
) -> Result<crate::ReadableBundleSource, capnp::Error> {
    Ok(crate::ReadableBundleSource {
        root_name: decode_text(source.get_root_name()?, "versionPoison.source.rootName")
            .map_err(|error| capnp::Error::failed(error.message))?,
        normalized_path: decode_text(
            source.get_normalized_path()?,
            "versionPoison.source.normalizedPath",
        )
        .map_err(|error| capnp::Error::failed(error.message))?,
        file_hash: distill_core::id::BundleFileHash(
            decode_hash(source.get_file_hash()?, "versionPoison.source.fileHash")
                .map_err(|error| capnp::Error::failed(error.message))?,
        ),
    })
}

fn write_snapshot_result(result: schema::snapshot_call::Builder<'_>, outcome: RpcResult<Snapshot>) {
    match outcome {
        RpcResult::Success(snapshot) => {
            let client: schema::snapshot::Client =
                capnp_rpc::new_client(SnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on capability acquisition: {poison}"),
        ),
        RpcResult::Failure(error) => write_rpc_result_error_snapshot(result, error),
    }
}

fn write_authoring_snapshot_result(
    result: schema::authoring_snapshot_call::Builder<'_>,
    outcome: RpcResult<AuthoringSnapshot>,
) {
    match outcome {
        RpcResult::Success(snapshot) => {
            let client: schema::authoring_snapshot::Client =
                capnp_rpc::new_client(AuthoringSnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on capability acquisition: {poison}"),
        ),
        RpcResult::Failure(RpcFailure::LeaseExpired) => write_lease_failure(
            result.init_lease_failure(),
            "authoring snapshot lease expired",
        ),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_metadata_snapshot_result(
    result: schema::metadata_snapshot_call::Builder<'_>,
    outcome: MetadataCall<MetadataSnapshot>,
) {
    match outcome {
        MetadataCall::Success(snapshot) => {
            let client: schema::metadata_snapshot::Client =
                capnp_rpc::new_client(MetadataSnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata snapshot lease expired",
        ),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_metadata_authoring_snapshot_result(
    result: schema::metadata_authoring_snapshot_call::Builder<'_>,
    outcome: MetadataCall<MetadataAuthoringSnapshot>,
) {
    match outcome {
        MetadataCall::Success(snapshot) => {
            let client: schema::metadata_authoring_snapshot::Client =
                capnp_rpc::new_client(MetadataAuthoringSnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata authoring snapshot lease expired",
        ),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_metadata_authoring_refresh_result(
    result: schema::metadata_authoring_snapshot_call::Builder<'_>,
    outcome: MetadataCall<MetadataAuthoringSnapshot>,
) {
    match outcome {
        MetadataCall::Success(snapshot) => {
            let client: schema::metadata_authoring_snapshot::Client =
                capnp_rpc::new_client(MetadataAuthoringSnapshotService { snapshot });
            let mut result = result;
            result.set_success(client);
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata authoring snapshot lease expired",
        ),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
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
                ConfigurationStatus::Poisoned(poison) => {
                    write_poison(configuration.init_poisoned(), poison)
                }
            }
            let mut pipeline = output.reborrow().init_pipeline();
            match &diagnostics.pipeline {
                crate::PipelineDiagnostic::Ready => pipeline.set_ready(()),
                crate::PipelineDiagnostic::Poisoned(poison) => {
                    write_pipeline_poison(pipeline.init_poisoned(), poison)?
                }
                crate::PipelineDiagnostic::SchemaAcceptanceRequired(diagnostic) => {
                    write_schema_acceptance_required(
                        pipeline.init_schema_acceptance_required(),
                        diagnostic,
                    )
                }
                crate::PipelineDiagnostic::RetiredTypeReferenced(diagnostic) => {
                    write_retired_type_referenced(
                        pipeline.init_retired_type_referenced(),
                        diagnostic,
                    )
                }
            }
            let mut version = output.init_version_poison();
            match &diagnostics.version_poison {
                None => version.set_healthy(()),
                Some(poison) => write_version_poison(version.init_poisoned(), poison),
            }
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata snapshot lease expired",
        ),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
    Ok(())
}

fn write_schema_acceptance_required(
    mut output: schema::schema_acceptance_required::Builder<'_>,
    diagnostic: &crate::SchemaAcceptanceRequired,
) {
    let mut manifest = output.reborrow().init_manifest();
    manifest.set_manifest_hash(&diagnostic.manifest.manifest_hash.0);
    let mut cursors = manifest
        .reborrow()
        .init_current_cursors(diagnostic.manifest.current_cursors.len() as u32);
    for (index, (type_uuid, logical_hash)) in diagnostic.manifest.current_cursors.iter().enumerate()
    {
        let mut row = cursors.reborrow().get(index as u32);
        row.reborrow().init_type_uuid().set_bytes(&type_uuid.0);
        row.set_logical_hash(&logical_hash.0);
    }
    let mut candidate = output.reborrow().init_candidate();
    candidate.set_dylib_hash(&diagnostic.candidate.dylib_hash);
    candidate.set_reserved_compiled_types(());
    let mut targets = candidate
        .reborrow()
        .init_target_rows(diagnostic.candidate.target_set.rows.len() as u32);
    for (index, target) in diagnostic.candidate.target_set.rows.iter().enumerate() {
        let mut row = targets.reborrow().get(index as u32);
        row.set_name(&target.name);
        row.set_target_definition_hash(&target.target_definition_hash);
    }
    let mut mismatches = output.init_mismatches(diagnostic.mismatches.len() as u32);
    for (index, mismatch) in diagnostic.mismatches.iter().enumerate() {
        let mut row = mismatches.reborrow().get(index as u32);
        row.reborrow()
            .init_type_uuid()
            .set_bytes(&mismatch.type_uuid.0);
        if let Some(hash) = mismatch.candidate {
            row.set_candidate(&hash.0);
            row.set_has_candidate(true);
        }
        if let Some(hash) = mismatch.manifest {
            row.set_manifest(&hash.0);
            row.set_has_manifest(true);
        }
    }
}

fn write_retired_type_referenced(
    mut output: schema::retired_type_referenced::Builder<'_>,
    diagnostic: &crate::RetiredTypeReferenced,
) {
    output.set_manifest_hash(&diagnostic.manifest_hash.0);
    write_authoring_stamp(output.reborrow().init_basis(), diagnostic.basis);
    output
        .reborrow()
        .init_type_uuid()
        .set_bytes(&diagnostic.type_uuid.0);
    let mut references = output.init_references(diagnostic.references.len() as u32);
    for (index, reference) in diagnostic.references.iter().enumerate() {
        let mut row = references.reborrow().get(index as u32);
        match reference {
            crate::RetiredTypeReference::Asset(asset) => {
                row.reborrow().init_asset().set_bytes(&asset.0)
            }
            crate::RetiredTypeReference::MigrationEndpoint(endpoint) => {
                row.set_migration_endpoint(&endpoint.0)
            }
        }
    }
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
        MetadataCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata snapshot lease expired",
        ),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
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
        MetadataNamespaceCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata snapshot lease expired",
        ),
        MetadataNamespaceCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
        MetadataNamespaceCall::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
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
        MetadataNamespaceCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata snapshot lease expired",
        ),
        MetadataNamespaceCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
        MetadataNamespaceCall::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
        }
    }
}

fn write_metadata_path_result(
    result: schema::metadata_path_resolve_call::Builder<'_>,
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
        MetadataNamespaceCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata snapshot lease expired",
        ),
        MetadataNamespaceCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
        MetadataNamespaceCall::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
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
        MetadataNamespaceCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata snapshot lease expired",
        ),
        MetadataNamespaceCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
        MetadataNamespaceCall::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
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
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
        }
        RpcResult::Failure(RpcFailure::LeaseExpired) => write_lease_failure(
            result.init_lease_failure(),
            "authoring snapshot lease expired",
        ),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
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
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
        }
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(result.init_lease_failure(), "snapshot lease expired")
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
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
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
        }
        RpcResult::Failure(RpcFailure::LeaseExpired) => write_lease_failure(
            result.init_lease_failure(),
            "authoring snapshot lease expired",
        ),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
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
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on subscription install: {poison}"),
        ),
        RpcResult::Failure(error) => write_rpc_result_error_subscribe(result, error),
    }
}

fn write_void_result(mut result: schema::void_call::Builder<'_>, outcome: RpcResult<()>) {
    match outcome {
        RpcResult::Success(()) => result.set_success(()),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on target-global call: {poison}"),
        ),
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(result.init_lease_failure(), "snapshot lease expired")
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_uint64_result(mut result: schema::u_int64_call::Builder<'_>, outcome: RpcResult<u64>) {
    match outcome {
        RpcResult::Success(value) => result.set_success(value),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on target-global call: {poison}"),
        ),
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(result.init_lease_failure(), "snapshot lease expired")
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
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
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on authoring operation: {poison}"),
        ),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
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
                    stream: Mutex::new(stream),
                });
            result.set_success(client);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on authoring operation: {poison}"),
        ),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_data_result(mut result: schema::data_call::Builder<'_>, outcome: RpcResult<Arc<[u8]>>) {
    match outcome {
        RpcResult::Success(bytes) => result.set_success(&bytes),
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on wire-tree fetch: {poison}"),
        ),
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_resolve_result(
    result: schema::resolve_call::Builder<'_>,
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
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
        }
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(result.init_lease_failure(), "snapshot lease expired")
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_path_result(
    result: schema::path_resolve_call::Builder<'_>,
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
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => {
            write_version_poison(result.init_version_poisoned(), &poison)
        }
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(result.init_lease_failure(), "snapshot lease expired")
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_fetch_result(
    result: schema::chunk_stream_call::Builder<'_>,
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
                stream: Mutex::new(terminal.value),
            });
            output.set_chunks(client);
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            result.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on immutable fetch: {poison}"),
        ),
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(result.init_lease_failure(), "snapshot lease expired")
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_metadata_fetch_result(
    result: schema::metadata_chunk_stream_call::Builder<'_>,
    outcome: MetadataCall<ChunkStream>,
) {
    match outcome {
        MetadataCall::Success(stream) => {
            let client: schema::chunk_stream::Client = capnp_rpc::new_client(ChunkStreamService {
                stream: Mutex::new(stream),
            });
            let mut result = result;
            result.set_success(client);
        }
        MetadataCall::ReconnectRequired { reason } => {
            write_metadata_reconnect(result.init_reconnect_required(), reason)
        }
        MetadataCall::LeaseFailure => write_lease_failure(
            result.init_lease_failure(),
            "metadata snapshot lease expired",
        ),
        MetadataCall::Error(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
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
        RpcResult::Success(ConfigurationStatus::Poisoned(poison))
        | RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(output.init_configuration_poisoned(), &poison)
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(output.init_reconnect_required(), reason)
        }
        RpcResult::VersionPoisoned(poison) => write_error(
            output.init_error(),
            RPC_FAILURE,
            &format!("unexpected version poison on configuration query: {poison}"),
        ),
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(output.init_lease_failure(), "snapshot lease expired")
        }
        RpcResult::Failure(error) => {
            write_error(output.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_stamp(mut output: schema::snapshot_stamp::Builder<'_>, stamp: SnapshotStamp) {
    output.set_instance(&stamp.instance.0);
    output.set_version(stamp.version.0);
}

fn write_lineage_claimant(
    mut output: schema::lineage_manifest_claimant::Builder<'_>,
    claimant: &LineageManifestClaimant,
) {
    output.reborrow().init_asset().set_bytes(&claimant.asset.0);
    output.set_root_name(claimant.root_name.as_str());
    output.set_normalized_path(claimant.normalized_path.as_str());
    output.set_file_hash(&claimant.file_hash.0);
    output
        .reborrow()
        .init_bundle()
        .set_bytes(&claimant.bundle.0);
    output.set_local_id(claimant.local_id.as_str());
}

fn write_lineage_state(
    mut output: schema::lineage_repair_state::Builder<'_>,
    state: &LineageRepairState,
) {
    match state {
        LineageRepairState::Missing {
            configured_root,
            configured_path,
            destination,
        } => {
            let mut missing = output.reborrow().init_missing();
            missing.set_configured_root(configured_root.as_str());
            missing.set_configured_path(configured_path.as_str());
            let mut destination_output = missing.init_destination();
            match destination {
                LineageRepairDestination::Absent => destination_output.set_absent(()),
                LineageRepairDestination::Occupied { file_hash, kind } => {
                    let mut occupied = destination_output.init_occupied();
                    occupied.set_file_hash(&file_hash.0);
                    occupied.set_kind(match kind {
                        OccupiedLineageDestinationKind::CanonicalBundle => {
                            schema::OccupiedLineageDestinationKind::CanonicalBundle
                        }
                        OccupiedLineageDestinationKind::Opaque => {
                            schema::OccupiedLineageDestinationKind::Opaque
                        }
                    });
                }
            }
        }
        LineageRepairState::Duplicate { claimants } => {
            let duplicate = output.reborrow().init_duplicate();
            let mut rows = duplicate.init_claimants(claimants.len() as u32);
            for (index, claimant) in claimants.iter().enumerate() {
                write_lineage_claimant(rows.reborrow().get(index as u32), claimant);
            }
        }
    }
}

fn write_lineage_inspection(
    mut output: schema::lineage_repair_inspection::Builder<'_>,
    inspection: &LineageRepairInspection,
) {
    output.set_instance(&inspection.instance.0);
    write_stamp(output.reborrow().init_stamp(), inspection.stamp);
    write_lineage_state(output.init_state(), &inspection.state);
}

fn write_lineage_unavailable(
    mut output: schema::lineage_repair_unavailable::Builder<'_>,
    unavailable: &LineageRepairUnavailable,
) {
    match unavailable {
        LineageRepairUnavailable::ConfigurationReady => output.set_configuration_ready(()),
        LineageRepairUnavailable::OtherConfigurationPoison(poison) => {
            write_poison(output.init_other_configuration_poison(), poison)
        }
    }
}

fn write_lineage_invalid(
    mut output: schema::lineage_repair_invalid::Builder<'_>,
    invalid: &LineageRepairInvalid,
) {
    output.set_code(match invalid.code {
        LineageRepairInvalidCode::WrongBasisState => {
            schema::LineageRepairInvalidCode::WrongBasisState
        }
        LineageRepairInvalidCode::NonCanonicalBundle => {
            schema::LineageRepairInvalidCode::NonCanonicalBundle
        }
        LineageRepairInvalidCode::MissingManifestEntry => {
            schema::LineageRepairInvalidCode::MissingManifestEntry
        }
        LineageRepairInvalidCode::NotAuthoringOnly => {
            schema::LineageRepairInvalidCode::NotAuthoringOnly
        }
        LineageRepairInvalidCode::BootstrapTypePresent => {
            schema::LineageRepairInvalidCode::BootstrapTypePresent
        }
        LineageRepairInvalidCode::InvalidLineage => {
            schema::LineageRepairInvalidCode::InvalidLineage
        }
        LineageRepairInvalidCode::SurvivorNotClaimant => {
            schema::LineageRepairInvalidCode::SurvivorNotClaimant
        }
    });
    output.set_message(invalid.message.as_str());
}

fn write_lineage_stale(
    mut output: schema::lineage_repair_stale::Builder<'_>,
    stale: &LineageRepairStale,
) {
    output.set_code(match stale.code {
        LineageRepairStaleCode::StampChanged => schema::LineageRepairStaleCode::StampChanged,
        LineageRepairStaleCode::StateChanged => schema::LineageRepairStaleCode::StateChanged,
        LineageRepairStaleCode::DestinationAppeared => {
            schema::LineageRepairStaleCode::DestinationAppeared
        }
        LineageRepairStaleCode::ClaimantChanged => schema::LineageRepairStaleCode::ClaimantChanged,
        LineageRepairStaleCode::PreimageChanged => schema::LineageRepairStaleCode::PreimageChanged,
    });
    write_stamp(output.init_observed_stamp(), stale.observed_stamp);
}

fn write_lineage_inspect_result(
    mut output: schema::lineage_repair_inspect_result::Builder<'_>,
    outcome: LineageRepairInspectOutcome,
) {
    match outcome {
        LineageRepairInspectOutcome::Success(inspection) => {
            write_lineage_inspection(output.reborrow().init_success(), &inspection)
        }
        LineageRepairInspectOutcome::Unavailable(unavailable) => {
            write_lineage_unavailable(output.reborrow().init_unavailable(), &unavailable)
        }
        LineageRepairInspectOutcome::ReconnectRequired { reason } => {
            write_metadata_reconnect(output.reborrow().init_reconnect_required(), reason)
        }
        LineageRepairInspectOutcome::Failure(error) => {
            write_error(output.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_lineage_mutation_result(
    mut output: schema::lineage_repair_mutation_result::Builder<'_>,
    outcome: LineageRepairMutationOutcome,
) {
    match outcome {
        LineageRepairMutationOutcome::Success(committed) => write_stamp(
            output.reborrow().init_success().init_stamp(),
            committed.stamp,
        ),
        LineageRepairMutationOutcome::StaleBasis(stale) => {
            write_lineage_stale(output.reborrow().init_stale_basis(), &stale)
        }
        LineageRepairMutationOutcome::Invalid(invalid) => {
            write_lineage_invalid(output.reborrow().init_invalid(), &invalid)
        }
        LineageRepairMutationOutcome::Unavailable(unavailable) => {
            write_lineage_unavailable(output.reborrow().init_unavailable(), &unavailable)
        }
        LineageRepairMutationOutcome::ReconnectRequired { reason } => {
            write_metadata_reconnect(output.reborrow().init_reconnect_required(), reason)
        }
        LineageRepairMutationOutcome::Failure(error) => {
            write_error(output.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn decode_lineage_claimant(
    input: schema::lineage_manifest_claimant::Reader<'_>,
) -> Result<LineageManifestClaimant, WireFailure> {
    let root_name = decode_text(input.get_root_name()?, "lineage.claimant.rootName")?;
    let normalized_path = decode_text(
        input.get_normalized_path()?,
        "lineage.claimant.normalizedPath",
    )?;
    let local_id = decode_text(input.get_local_id()?, "lineage.claimant.localId")?;
    if !canonical_lineage_identifier(&root_name)
        || !canonical_lineage_path(&normalized_path)
        || !canonical_lineage_identifier(&local_id)
        || local_id.starts_with('$')
    {
        return Err(WireFailure {
            code: WIRE_INVALID_UTF8,
            message: "lineage claimant strings are not canonical".to_owned(),
        });
    }
    Ok(LineageManifestClaimant {
        asset: AssetUuid(decode_uuid(
            input.get_asset()?.get_bytes()?,
            "lineage.claimant.asset",
        )?),
        root_name,
        normalized_path,
        file_hash: BundleFileHash(decode_hash(
            input.get_file_hash()?,
            "lineage.claimant.fileHash",
        )?),
        bundle: BundleUuid(decode_uuid(
            input.get_bundle()?.get_bytes()?,
            "lineage.claimant.bundle",
        )?),
        local_id,
    })
}

fn decode_lineage_inspection(
    input: schema::lineage_repair_inspection::Reader<'_>,
) -> Result<LineageRepairInspection, WireFailure> {
    let instance = StoreInstanceId(decode_instance(
        input.get_instance()?,
        "lineage.inspection.instance",
    )?);
    let stamp_input = input.get_stamp()?;
    let stamp = SnapshotStamp {
        instance: StoreInstanceId(decode_instance(
            stamp_input.get_instance()?,
            "lineage.inspection.stamp.instance",
        )?),
        version: InputVersion(stamp_input.get_version()),
    };
    if instance != stamp.instance {
        return Err(WireFailure {
            code: WIRE_INVALID_INSTANCE,
            message: "lineage inspection instance must equal stamp.instance".to_owned(),
        });
    }
    let unknown_union = |field: &str| WireFailure {
        code: WIRE_INVALID_VALUE,
        message: format!("{field} has an unknown union discriminant"),
    };
    let state = match input
        .get_state()?
        .which()
        .map_err(|_| unknown_union("lineage.inspection.state"))?
    {
        schema::lineage_repair_state::Which::Missing(missing) => {
            let missing = missing?;
            let configured_root = decode_text(
                missing.get_configured_root()?,
                "lineage.missing.configuredRoot",
            )?;
            let configured_path = decode_text(
                missing.get_configured_path()?,
                "lineage.missing.configuredPath",
            )?;
            if !canonical_lineage_identifier(&configured_root)
                || !canonical_lineage_path(&configured_path)
            {
                return Err(WireFailure {
                    code: WIRE_INVALID_UTF8,
                    message: "lineage missing destination is not canonical".to_owned(),
                });
            }
            let destination = match missing
                .get_destination()?
                .which()
                .map_err(|_| unknown_union("lineage.missing.destination"))?
            {
                schema::lineage_repair_destination::Which::Absent(()) => {
                    LineageRepairDestination::Absent
                }
                schema::lineage_repair_destination::Which::Occupied(occupied) => {
                    let occupied = occupied?;
                    let kind = match occupied
                        .get_kind()
                        .map_err(|_| unknown_union("lineage.missing.destination.kind"))?
                    {
                        schema::OccupiedLineageDestinationKind::CanonicalBundle => {
                            OccupiedLineageDestinationKind::CanonicalBundle
                        }
                        schema::OccupiedLineageDestinationKind::Opaque => {
                            OccupiedLineageDestinationKind::Opaque
                        }
                    };
                    LineageRepairDestination::Occupied {
                        file_hash: BundleFileHash(decode_hash(
                            occupied.get_file_hash()?,
                            "lineage.missing.destination.fileHash",
                        )?),
                        kind,
                    }
                }
            };
            LineageRepairState::Missing {
                configured_root,
                configured_path,
                destination,
            }
        }
        schema::lineage_repair_state::Which::Duplicate(duplicate) => {
            let rows = duplicate?.get_claimants()?;
            let mut claimants = Vec::with_capacity(rows.len() as usize);
            for row in rows.iter() {
                claimants.push(decode_lineage_claimant(row)?);
            }
            if claimants.len() < 2 || claimants.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(WireFailure {
                    code: WIRE_INVALID_VALUE,
                    message: "lineage duplicate claimants must contain at least two strict rows"
                        .to_owned(),
                });
            }
            LineageRepairState::Duplicate { claimants }
        }
    };
    Ok(LineageRepairInspection {
        instance,
        stamp,
        state,
    })
}

fn canonical_lineage_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.nfc().eq(value.chars())
        && !value.contains('\0')
}

fn canonical_lineage_path(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.contains('\\')
        && !value.contains('\0')
        && value
            .split('/')
            .all(|part| !matches!(part, "" | "." | "..") && part.nfc().eq(part.chars()))
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

fn write_poison(
    mut output: schema::configuration_poison::Builder<'_>,
    poison: &ConfigurationPoison,
) {
    output.set_code(poison.code as u16);
    output.set_reason_hash(&poison.reason_hash);
    output.set_message(poison.message.as_str());
    output.set_detail_version(1);
    output.set_detail_bytes(&poison.detail.canonical_detail_bytes());
}

fn write_pipeline_unavailable(
    mut output: schema::pipeline_unavailable_diagnostic::Builder<'_>,
    diagnostic: &crate::PipelineUnavailableDiagnostic,
) -> Result<(), capnp::Error> {
    match diagnostic {
        crate::PipelineUnavailableDiagnostic::PipelinePoison(poison) => {
            write_pipeline_poison(output.reborrow().init_pipeline_poison(), poison)
        }
        crate::PipelineUnavailableDiagnostic::SchemaAcceptanceRequired(required) => {
            write_schema_acceptance_required(
                output.reborrow().init_schema_acceptance_required(),
                required,
            );
            Ok(())
        }
        crate::PipelineUnavailableDiagnostic::RetiredTypeReferenced(retired) => {
            write_retired_type_referenced(
                output.reborrow().init_retired_type_referenced(),
                retired,
            );
            Ok(())
        }
    }
}

fn write_pipeline_poison(
    mut output: schema::pipeline_poison::Builder<'_>,
    poison: &crate::PipelinePoison,
) -> Result<(), capnp::Error> {
    poison
        .validate()
        .map_err(|error| capnp::Error::failed(format!("invalid DSPP diagnostic: {error}")))?;
    output.set_code(match poison.code {
        crate::PipelinePoisonCode::CandidateOpen => schema::PipelinePoisonCode::CandidateOpen,
        crate::PipelinePoisonCode::CandidateAttestation => {
            schema::PipelinePoisonCode::CandidateAttestation
        }
        crate::PipelinePoisonCode::CandidateRegistration => {
            schema::PipelinePoisonCode::CandidateRegistration
        }
        crate::PipelinePoisonCode::CandidateValidation => {
            schema::PipelinePoisonCode::CandidateValidation
        }
        crate::PipelinePoisonCode::CandidateCleanup => schema::PipelinePoisonCode::CandidateCleanup,
        crate::PipelinePoisonCode::PublishedCallbackPanic => {
            schema::PipelinePoisonCode::PublishedCallbackPanic
        }
        crate::PipelinePoisonCode::PublishedCallbackRejected => {
            schema::PipelinePoisonCode::PublishedCallbackRejected
        }
        crate::PipelinePoisonCode::PublishedCleanup => schema::PipelinePoisonCode::PublishedCleanup,
    });
    output.set_origin(match poison.origin {
        crate::PipelinePoisonOrigin::CandidateOpen => schema::PipelinePoisonOrigin::CandidateOpen,
        crate::PipelinePoisonOrigin::PublishedRuntime => {
            schema::PipelinePoisonOrigin::PublishedRuntime
        }
    });
    output.set_cleanup(match poison.cleanup {
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
    output.set_identity(&poison.identity);
    output.set_message(poison.message.as_str());
    Ok(())
}

fn write_version_poison(mut output: schema::version_poison::Builder<'_>, poison: &VersionPoison) {
    debug_assert!(poison.validate().is_ok());
    output.set_code(poison.code as u16);
    output.set_identity(&poison.identity);
    output.set_message(poison.message.as_str());
    let detail = output.init_detail();
    match &poison.detail {
        VersionPoisonV1::DuplicateAssetUuid { asset, claimants } => {
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
                        write_version_poison_source(authored.reborrow().init_source(), source);
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
        VersionPoisonV1::DuplicateBundleUuid { bundle, sources } => {
            let mut value = detail.init_duplicate_bundle_uuid();
            value.reborrow().init_bundle().set_bytes(&bundle.0);
            write_version_poison_sources(value.init_sources(sources.len() as u32), sources);
        }
        VersionPoisonV1::SameRootNormalizedPathCollision {
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
        VersionPoisonV1::IncompleteSkeleton { source, failure } => {
            let mut value = detail.init_incomplete_skeleton();
            write_version_poison_source(value.reborrow().init_source(), source);
            value.set_failure_code(*failure as u16);
        }
        VersionPoisonV1::UnreadableGlobalBundlePath {
            root_name,
            normalized_path,
            failure,
        } => {
            let mut value = detail.init_unreadable_global_bundle_path();
            value.set_root_name(root_name.as_str());
            value.set_normalized_path(normalized_path.as_str());
            value.set_failure_code(*failure as u16);
        }
        VersionPoisonV1::InvalidPhysicalPath {
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
        VersionPoisonV1::UnreadableScanSubtree { subject, failure } => {
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

fn write_version_poison_sources(
    mut output: capnp::struct_list::Builder<'_, schema::version_poison_source::Owned>,
    sources: &[crate::ReadableBundleSource],
) {
    for (index, source) in sources.iter().enumerate() {
        write_version_poison_source(output.reborrow().get(index as u32), source);
    }
}

fn write_version_poison_source(
    mut output: schema::version_poison_source::Builder<'_>,
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

fn write_lease_failure(mut output: schema::lease_failure::Builder<'_>, message: &str) {
    output.set_code(1);
    output.set_message(message);
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
    if error == RpcFailure::LeaseExpired {
        write_lease_failure(
            result.reborrow().init_lease_failure(),
            "snapshot lease expired",
        );
    } else {
        write_error(
            result.init_error(),
            RPC_FAILURE,
            format!("{error:?}").as_str(),
        );
    }
}

fn write_rpc_result_error_subscribe(
    mut result: schema::subscribe_call::Builder<'_>,
    error: RpcFailure,
) {
    if error == RpcFailure::LeaseExpired {
        write_lease_failure(
            result.reborrow().init_lease_failure(),
            "snapshot lease expired",
        );
    } else {
        write_error(
            result.init_error(),
            RPC_FAILURE,
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
    }
}
