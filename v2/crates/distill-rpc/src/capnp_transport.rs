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
use std::sync::Mutex;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::{
    AssetDeltaState, AssetEvent, AssetUuid, ChunkStream, CompiledAttestationDigest,
    CompiledTypeRow, ConfigurationPoison, ConfigurationStatus, ConnectOutcome, ConnectRequest,
    ContentHash, Delta, DeltaStream, DriftedInput, GameModuleEpoch, Hub, InputVersion,
    LoadPolicyEntry, PathResolveFailure, PathResolveResult, ReattestRequest, ReconnectReason,
    RegistryExtrasDigest, RegistryExtrasV1, ResolveResult, Root, RpcFailure, RpcResult, Snapshot,
    SnapshotStamp, StoreInstanceId, TargetDefinitionHash, TypeUuid,
};

pub use crate::distill_rpc_capnp as schema;

const WIRE_INVALID_UUID: u16 = 1001;
const WIRE_INVALID_HASH: u16 = 1002;
const WIRE_INVALID_INSTANCE: u16 = 1003;
const WIRE_INVALID_UTF8: u16 = 1004;
const WIRE_INVALID_ATTESTATION: u16 = 1005;
const CONNECT_REJECTED: u16 = 2000;
const RPC_FAILURE: u16 = 3000;
const UNSUPPORTED_METHOD: u16 = 4000;

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
        decode_connect_response(response.get()?.get_result()?)
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
        policy_generation: u64,
        target_generation: u64,
        attestation_generation: u64,
    },
    ConfigurationPoisoned(ConfigurationPoison),
    AttestationFailure {
        code: u16,
        type_uuid: Option<TypeUuid>,
        message: String,
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
            Self::Connected {
                instance,
                policy_generation,
                target_generation,
                attestation_generation,
                ..
            } => f
                .debug_struct("Connected")
                .field("instance", instance)
                .field("policy_generation", policy_generation)
                .field("target_generation", target_generation)
                .field("attestation_generation", attestation_generation)
                .finish_non_exhaustive(),
            Self::ConfigurationPoisoned(poison) => f
                .debug_tuple("ConfigurationPoisoned")
                .field(poison)
                .finish(),
            Self::AttestationFailure {
                code,
                type_uuid,
                message,
            } => f
                .debug_struct("AttestationFailure")
                .field("code", code)
                .field("type_uuid", type_uuid)
                .field("message", message)
                .finish(),
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
                    output.set_policy_generation(connected.policy_generation);
                    output.set_target_generation(connected.target_generation);
                    output.set_attestation_generation(connected.attestation_generation);
                }
                ConnectOutcome::ConfigurationPoisoned(poison) => {
                    write_poison(result.init_configuration_poisoned(), &poison);
                }
                ConnectOutcome::Rejected(error) => {
                    write_connect_error(result, &error);
                }
            }
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
        _params: schema::hub::WriteParams,
        mut results: schema::hub::WriteResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(results.get().init_result().init_error(), "Hub.write");
            Ok(())
        }
    }

    fn import(
        self: capnp::capability::Rc<Self>,
        _params: schema::hub::ImportParams,
        mut results: schema::hub::ImportResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(results.get().init_result().init_error(), "Hub.import");
            Ok(())
        }
    }

    fn reimport(
        self: capnp::capability::Rc<Self>,
        _params: schema::hub::ReimportParams,
        mut results: schema::hub::ReimportResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(results.get().init_result().init_error(), "Hub.reimport");
            Ok(())
        }
    }

    fn operation(
        self: capnp::capability::Rc<Self>,
        _params: schema::hub::OperationParams,
        mut results: schema::hub::OperationResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(results.get().init_result().init_error(), "Hub.operation");
            Ok(())
        }
    }

    fn fetch(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::FetchParams,
        mut results: schema::hub::FetchResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
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
        _params: schema::hub::WireTreeParams,
        mut results: schema::hub::WireTreeResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(results.get().init_result().init_error(), "Hub.wireTree");
            Ok(())
        }
    }

    fn reattest(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::ReattestParams,
        mut results: schema::hub::ReattestResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let reader = params.get()?;
            let request = match decode_reattest_request(reader) {
                Ok(request) => request,
                Err(DecodeError::Wire(error)) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
                Err(DecodeError::Capnp(error)) => return Err(error),
            };
            write_uint64_result(results.get().init_result(), self.hub.reattest(request));
            Ok(())
        }
    }

    fn unsubscribe(
        self: capnp::capability::Rc<Self>,
        params: schema::hub::UnsubscribeParams,
        mut results: schema::hub::UnsubscribeResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
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
            write_uint64_result(
                results.get().init_result(),
                RpcResult::Success(self.snapshot.version().0),
            );
            Ok(())
        }
    }

    fn query(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::QueryParams,
        mut results: schema::snapshot::QueryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(results.get().init_result().init_error(), "Snapshot.query");
            Ok(())
        }
    }

    fn entry(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::EntryParams,
        mut results: schema::snapshot::EntryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(results.get().init_result().init_error(), "Snapshot.entry");
            Ok(())
        }
    }

    fn resolve(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::ResolveParams,
        mut results: schema::snapshot::ResolveResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            let uuid = match decode_uuid(params.get()?.get_uuid()?, "uuid") {
                Ok(uuid) => AssetUuid(uuid),
                Err(error) => {
                    write_wire_error(results.get().init_result().init_error(), &error);
                    return Ok(());
                }
            };
            write_resolve_result(results.get().init_result(), self.snapshot.resolve(uuid));
            Ok(())
        }
    }

    fn refresh(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::RefreshParams,
        mut results: schema::snapshot::RefreshResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_snapshot_result(results.get().init_result(), self.snapshot.refresh());
            Ok(())
        }
    }

    fn reserved5(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::Reserved5Params,
        mut results: schema::snapshot::Reserved5Results,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(
                results.get().init_result().init_error(),
                "Snapshot.reserved5",
            );
            Ok(())
        }
    }

    fn reserved6(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::Reserved6Params,
        mut results: schema::snapshot::Reserved6Results,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(
                results.get().init_result().init_error(),
                "Snapshot.reserved6",
            );
            Ok(())
        }
    }

    fn reserved7(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::Reserved7Params,
        mut results: schema::snapshot::Reserved7Results,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(
                results.get().init_result().init_error(),
                "Snapshot.reserved7",
            );
            Ok(())
        }
    }

    fn reserved8(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::Reserved8Params,
        mut results: schema::snapshot::Reserved8Results,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(
                results.get().init_result().init_error(),
                "Snapshot.reserved8",
            );
            Ok(())
        }
    }

    fn reserved9(
        self: capnp::capability::Rc<Self>,
        _params: schema::snapshot::Reserved9Params,
        mut results: schema::snapshot::Reserved9Results,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
            write_unsupported(
                results.get().init_result().init_error(),
                "Snapshot.reserved9",
            );
            Ok(())
        }
    }

    fn resolve_path(
        self: capnp::capability::Rc<Self>,
        params: schema::snapshot::ResolvePathParams,
        mut results: schema::snapshot::ResolvePathResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        async move {
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
            write_configuration_result(results.get().init_result(), &self.snapshot.configuration());
            Ok(())
        }
    }
}

struct ChunkStreamService {
    stream: Mutex<ChunkStream>,
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
    let compiled_registry = decode_compiled(reader.get_compiled_registry()?)?;
    let dsca = CompiledAttestationDigest(
        decode_hash(reader.get_dsca_aggregate()?, "dscaAggregate").map_err(DecodeError::Wire)?,
    );
    let load_policy = decode_policies(reader.get_load_policy()?)?;
    let policy_digest =
        decode_hash(reader.get_policy_digest()?, "policyDigest").map_err(DecodeError::Wire)?;
    Ok(ConnectRequest {
        epoch: GameModuleEpoch(reader.get_game_module_epoch()),
        target,
        target_definition_hash,
        compiled_registry,
        dsca,
        load_policy,
        policy_digest,
        protocol: reader.get_protocol(),
    })
}

fn decode_reattest_request(
    reader: schema::hub::reattest_params::Reader<'_>,
) -> Result<ReattestRequest, DecodeError> {
    Ok(ReattestRequest {
        epoch: GameModuleEpoch(reader.get_epoch()),
        base_attestation_generation: reader.get_base_attestation_generation(),
        successor_attestation_generation: reader.get_successor_attestation_generation(),
        target_definition_hash: TargetDefinitionHash(
            decode_hash(reader.get_target_def_hash()?, "targetDefHash")
                .map_err(DecodeError::Wire)?,
        ),
        compiled_registry: decode_compiled(reader.get_compiled_registry()?)?,
        dsca: CompiledAttestationDigest(
            decode_hash(reader.get_dsca_aggregate()?, "dscaAggregate")
                .map_err(DecodeError::Wire)?,
        ),
        load_policy: decode_policies(reader.get_load_policy()?)?,
        policy_digest: decode_hash(reader.get_policy_digest()?, "policyDigest")
            .map_err(DecodeError::Wire)?,
    })
}

fn decode_compiled(
    rows: capnp::struct_list::Reader<'_, schema::compiled_type_entry::Owned>,
) -> Result<Vec<CompiledTypeRow>, DecodeError> {
    rows.iter()
        .map(|row| {
            let registry_extras =
                RegistryExtrasV1::decode(row.get_registry_extras()?).map_err(|error| {
                    DecodeError::Wire(WireFailure {
                        code: WIRE_INVALID_ATTESTATION,
                        message: format!("compiledRegistry.registryExtras is invalid: {error}"),
                    })
                })?;
            let value = CompiledTypeRow {
                type_uuid: TypeUuid(
                    decode_uuid(row.get_type_uuid()?, "compiledRegistry.typeUuid")
                        .map_err(DecodeError::Wire)?,
                ),
                logical_hash: crate::LogicalHash(
                    decode_hash(row.get_logical_hash()?, "compiledRegistry.logicalHash")
                        .map_err(DecodeError::Wire)?,
                ),
                native_layout_digest: decode_hash(
                    row.get_native_layout_digest()?,
                    "compiledRegistry.nativeLayoutDigest",
                )
                .map_err(DecodeError::Wire)?,
                build_only: row.get_build_only(),
                registry_extras_digest: RegistryExtrasDigest(
                    decode_hash(
                        row.get_registry_extras_digest()?,
                        "compiledRegistry.registryExtrasDigest",
                    )
                    .map_err(DecodeError::Wire)?,
                ),
                registry_extras,
            };
            value.validate().map_err(|error| {
                DecodeError::Wire(WireFailure {
                    code: WIRE_INVALID_ATTESTATION,
                    message: format!("compiledRegistry row is invalid: {error}"),
                })
            })?;
            Ok(value)
        })
        .collect()
}

fn decode_policies(
    rows: capnp::struct_list::Reader<'_, schema::load_policy_entry::Owned>,
) -> Result<Vec<LoadPolicyEntry>, DecodeError> {
    rows.iter()
        .map(|row| {
            Ok(LoadPolicyEntry {
                type_uuid: TypeUuid(
                    decode_uuid(row.get_type_uuid()?, "loadPolicy.typeUuid")
                        .map_err(DecodeError::Wire)?,
                ),
                build_only: row.get_build_only(),
            })
        })
        .collect()
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
    {
        let mut rows = output
            .reborrow()
            .init_compiled_registry(request.compiled_registry.len() as u32);
        for (index, row) in request.compiled_registry.iter().enumerate() {
            let mut item = rows.reborrow().get(index as u32);
            item.set_type_uuid(&row.type_uuid.0);
            item.set_logical_hash(&row.logical_hash.0);
            item.set_native_layout_digest(&row.native_layout_digest);
            item.set_build_only(row.build_only);
            item.set_registry_extras_digest(&row.registry_extras_digest.0);
            item.set_registry_extras(
                &row.registry_extras
                    .encode()
                    .expect("validated ConnectRequest carries canonical DSRE"),
            );
        }
    }
    output.set_dsca_aggregate(&request.dsca.0);
    {
        let mut rows = output
            .reborrow()
            .init_load_policy(request.load_policy.len() as u32);
        for (index, row) in request.load_policy.iter().enumerate() {
            let mut item = rows.reborrow().get(index as u32);
            item.set_type_uuid(&row.type_uuid.0);
            item.set_build_only(row.build_only);
        }
    }
    output.set_policy_digest(&request.policy_digest);
    output.set_protocol(request.protocol);
    output.set_game_module_epoch(request.epoch.0);
}

fn decode_connect_response(
    response: schema::connect_call::Reader<'_>,
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
                policy_generation: connected.get_policy_generation(),
                target_generation: connected.get_target_generation(),
                attestation_generation: connected.get_attestation_generation(),
            })
        }
        Which::ConfigurationPoisoned(poison) => Ok(RemoteConnectOutcome::ConfigurationPoisoned(
            read_poison(poison?)?,
        )),
        Which::AttestationFailure(failure) => {
            let failure = failure?;
            let type_uuid = decode_uuid(failure.get_type_uuid()?, "attestationFailure.typeUuid")
                .map_err(|error| capnp::Error::failed(error.message))?;
            Ok(RemoteConnectOutcome::AttestationFailure {
                code: failure.get_code(),
                type_uuid: (type_uuid != [0; 16]).then_some(TypeUuid(type_uuid)),
                message: decode_text(failure.get_message()?, "attestationFailure.message")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            })
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

fn read_poison(
    poison: schema::configuration_poison::Reader<'_>,
) -> Result<ConfigurationPoison, capnp::Error> {
    Ok(ConfigurationPoison {
        code: crate::ConfigurationPoisonCode(poison.get_code()),
        reason_hash: decode_hash(poison.get_reason_hash()?, "reasonHash")
            .map_err(|error| capnp::Error::failed(error.message))?,
        message: decode_text(poison.get_message()?, "poison.message")
            .map_err(|error| capnp::Error::failed(error.message))?,
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
        RpcResult::Failure(error) => write_rpc_result_error_snapshot(result, error),
    }
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
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(result.init_lease_failure(), "snapshot lease expired")
        }
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
            write_stamp(output.reborrow().init_basis(), terminal.basis.snapshot);
            let mut value = output.init_result();
            match terminal.value {
                ResolveResult::Built { content_hash } => value.set_built(&content_hash.0),
                ResolveResult::Drifted { input, current } => value.set_drifted(
                    format!("{} at {}", describe_drift(&input), current.version.0).as_str(),
                ),
                ResolveResult::Failed { error } => value.set_failed(error.as_str()),
                ResolveResult::Missing => value.set_missing(()),
                ResolveResult::Deleted { at } => write_stamp(value.init_deleted(), at),
            }
        }
        RpcResult::ReconnectRequired { reason } => {
            write_reconnect(result.init_reconnect_required(), reason)
        }
        RpcResult::ConfigurationPoisoned(poison) => {
            write_poison(result.init_configuration_poisoned(), &poison)
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
            write_stamp(output.reborrow().init_basis(), terminal.basis.snapshot);
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
            write_stamp(output.reborrow().init_basis(), terminal.basis.snapshot);
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
        RpcResult::Failure(RpcFailure::LeaseExpired) => {
            write_lease_failure(result.init_lease_failure(), "snapshot lease expired")
        }
        RpcResult::Failure(error) => {
            write_error(result.init_error(), RPC_FAILURE, &format!("{error:?}"))
        }
    }
}

fn write_stream_event(
    mut output: schema::stream_event::Builder<'_>,
    event: &crate::StreamEvent,
) -> Result<(), capnp::Error> {
    write_stamp(output.reborrow().init_basis(), event.basis().snapshot);
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
    write_stamp(output.reborrow().init_stamp(), delta.basis.snapshot);
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

fn write_configuration_result(
    mut output: schema::void_call::Builder<'_>,
    state: &ConfigurationStatus,
) {
    match state {
        ConfigurationStatus::Ready => output.set_success(()),
        ConfigurationStatus::Poisoned(poison) => {
            write_poison(output.init_configuration_poisoned(), poison)
        }
    }
}

fn write_stamp(mut output: schema::snapshot_stamp::Builder<'_>, stamp: SnapshotStamp) {
    output.set_instance(&stamp.instance.0);
    output.set_version(stamp.version.0);
}

fn write_poison(
    mut output: schema::configuration_poison::Builder<'_>,
    poison: &ConfigurationPoison,
) {
    output.set_code(poison.code.0);
    output.set_reason_hash(&poison.reason_hash);
    output.set_message(poison.message.as_str());
}

fn write_error(mut output: schema::rpc_error::Builder<'_>, code: u16, message: &str) {
    output.set_code(code);
    output.set_message(message);
}

fn write_wire_error(output: schema::rpc_error::Builder<'_>, failure: &WireFailure) {
    write_error(output, failure.code, failure.message.as_str());
}

fn write_unsupported(output: schema::rpc_error::Builder<'_>, method: &str) {
    write_error(
        output,
        UNSUPPORTED_METHOD,
        &format!("{method} is outside the loader RPC state model"),
    );
}

fn write_lease_failure(mut output: schema::lease_failure::Builder<'_>, message: &str) {
    output.set_code(1);
    output.set_message(message);
}

fn write_connect_error(mut result: schema::connect_call::Builder<'_>, error: &crate::ConnectError) {
    if let crate::ConnectError::ProtocolMismatch { expected, got } = error {
        let mut failure = result.reborrow().init_protocol_failure();
        failure.set_expected(*expected);
        failure.set_observed(*got);
        failure.set_message(format!("{error:?}").as_str());
        return;
    }
    let mut failure = result.init_attestation_failure();
    failure.set_code(CONNECT_REJECTED);
    let type_uuid = match error {
        crate::ConnectError::MissingCompiledType { type_uuid }
        | crate::ConnectError::CompiledTypeMismatch { type_uuid }
        | crate::ConnectError::MissingLoadPolicy { type_uuid }
        | crate::ConnectError::LoadPolicyMismatch { type_uuid, .. } => type_uuid.0,
        _ => [0; 16],
    };
    failure.set_type_uuid(&type_uuid);
    failure.set_message(format!("{error:?}").as_str());
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

fn wire_reconnect(reason: ReconnectReason) -> schema::ReconnectReason {
    match reason {
        ReconnectReason::TargetDefinitionChanged => {
            schema::ReconnectReason::TargetDefinitionChanged
        }
        ReconnectReason::LoadPolicyChanged => schema::ReconnectReason::LoadPolicyChanged,
        ReconnectReason::StoreInstanceChanged => schema::ReconnectReason::StoreInstanceChanged,
        ReconnectReason::ProtocolEpochChanged => schema::ReconnectReason::ProtocolEpochChanged,
    }
}

fn describe_drift(input: &DriftedInput) -> String {
    match input {
        DriftedInput::File(path) => format!("file:{path}"),
        DriftedInput::Asset(uuid) => format!("asset:{uuid}"),
        DriftedInput::Query(query) => format!("query:{query}"),
        DriftedInput::Dylib => "dylib".to_owned(),
        DriftedInput::Tool(tool) => format!("tool:{tool}"),
    }
}
