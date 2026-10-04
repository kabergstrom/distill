//! Typed client-side realization of the loader and pack subset of the Cap'n
//! Proto RPC.

use std::collections::BTreeMap;
use std::sync::Arc;

use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};
use distill_store::state::{InputVersion, SnapshotStamp, StoreInstanceId};

pub use crate::capnp_transport::RemoteError;
use crate::capnp_transport::{
    decode_authoring_inspection, decode_configuration_error, decode_rpc_basis, decode_rpc_error,
    schema, RemoteConnectOutcome, RemoteMetadataOutcome,
};
use crate::{
    ArtifactChunk, ArtifactChunkKind, AssetDeltaState, AssetEvent, AssetQuery, AuthoringEntryRole,
    AuthoringInspectResult, ConfigurationError, Delta, DriftedInput, ImportFailure, ImportRequest,
    MetadataEntry, PathResolveFailure, PathResolveResult, ReconnectReason, ResolveResult, RpcBasis,
    RuntimeTypePolicy, ServedLoadEdge, StreamEvent, TerminalEvent,
};

#[derive(Debug)]
pub enum RemoteCall<T> {
    Success(T),
    ReconnectRequired(ReconnectReason),
    ConfigurationFailed(ConfigurationError),
    /// The snapshot expired or was released: open a new one.
    SnapshotExpired,
    Error(RemoteError),
}

impl<T> RemoteCall<T> {
    pub fn success(self) -> Option<T> {
        match self {
            Self::Success(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct RemoteHub {
    client: schema::hub::Client,
    basis: RpcBasis,
}

impl std::fmt::Debug for RemoteHub {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteHub")
            .field("basis", &self.basis)
            .finish_non_exhaustive()
    }
}

impl RemoteHub {
    pub fn connected(outcome: RemoteConnectOutcome) -> Result<Self, Box<RemoteConnectOutcome>> {
        match outcome {
            RemoteConnectOutcome::Connected { hub, instance } => Ok(Self {
                client: hub,
                basis: RpcBasis {
                    snapshot: SnapshotStamp {
                        instance,
                        version: InputVersion(0),
                    },
                },
            }),
            other => Err(Box::new(other)),
        }
    }

    pub async fn snapshot(&self) -> Result<RemoteCall<RemoteSnapshot>, capnp::Error> {
        let response = self.client.snapshot_request().send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::snapshot_call::Which::Success(snapshot) => {
                RemoteSnapshot::open(snapshot?, self.basis.clone()).await
            }
            schema::snapshot_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::snapshot_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::snapshot_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::snapshot_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    pub async fn wire_tree(
        &self,
        layout_hash: LayoutHash,
    ) -> Result<RemoteCall<Arc<[u8]>>, capnp::Error> {
        let mut request = self.client.wire_tree_request();
        request.get().set_layout_hash(&layout_hash.0);
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::data_call::Which::Success(value) => {
                Ok(RemoteCall::Success(Arc::from(value?.to_vec())))
            }
            schema::data_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::data_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::data_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::data_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    pub async fn subscribe(
        &self,
        since: InputVersion,
        assets: Vec<AssetUuid>,
        paths: Vec<String>,
    ) -> Result<RemoteCall<RemoteSubscription>, capnp::Error> {
        let mut request = self.client.subscribe_request();
        {
            let mut params = request.get();
            params.set_since(since.0);
            let mut wire_assets = params.reborrow().init_assets(assets.len() as u32);
            for (index, asset) in assets.iter().enumerate() {
                wire_assets.set(index as u32, &asset.0);
            }
            let mut wire_paths = params.init_paths(paths.len() as u32);
            for (index, path) in paths.iter().enumerate() {
                wire_paths.set(index as u32, path.as_str());
            }
        }
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::subscribe_call::Which::Success(value) => {
                let value = value?;
                Ok(RemoteCall::Success(RemoteSubscription {
                    client: value.get_deltas()?,
                    basis: self.basis.clone(),
                    since,
                    installed: InputVersion(value.get_installed()),
                    initial: true,
                }))
            }
            schema::subscribe_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::subscribe_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::subscribe_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::subscribe_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    pub async fn unsubscribe(
        &self,
        assets: Vec<AssetUuid>,
        paths: Vec<String>,
    ) -> Result<RemoteCall<()>, capnp::Error> {
        let mut request = self.client.unsubscribe_request();
        {
            let mut params = request.get();
            let mut wire_assets = params.reborrow().init_assets(assets.len() as u32);
            for (index, asset) in assets.iter().enumerate() {
                wire_assets.set(index as u32, &asset.0);
            }
            let mut wire_paths = params.init_paths(paths.len() as u32);
            for (index, path) in paths.iter().enumerate() {
                wire_paths.set(index as u32, path.as_str());
            }
        }
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::void_call::Which::Success(()) => Ok(RemoteCall::Success(())),
            schema::void_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::void_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::void_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::void_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    /// The current watched-import failures (protocol 10). They publish no
    /// version, so a client polls this.
    pub async fn import_failures(&self) -> Result<RemoteCall<Vec<ImportFailure>>, capnp::Error> {
        let response = self.client.import_failures_request().send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::import_failures_call::Which::Success(list) => {
                let mut failures = Vec::new();
                for entry in list? {
                    failures.push(ImportFailure {
                        bundle: BundleUuid(fixed::<16>(
                            entry.get_bundle()?,
                            "importFailures.bundle",
                        )?),
                        root: entry.get_root()?.to_string()?,
                        path: entry.get_path()?.to_string()?,
                        message: entry.get_message()?.to_string()?,
                    });
                }
                Ok(RemoteCall::Success(failures))
            }
            schema::import_failures_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::import_failures_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::import_failures_call::Which::SnapshotExpired(()) => {
                Ok(RemoteCall::SnapshotExpired)
            }
            schema::import_failures_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    /// Run `request` through the hub's authoring `import` at `base`; the
    /// destination bundle's identity on success.
    pub async fn import(
        &self,
        base: InputVersion,
        request: &ImportRequest,
    ) -> Result<RemoteCall<BundleUuid>, capnp::Error> {
        let mut call = self.client.import_request();
        {
            let mut params = call.get();
            params.set_base(base.0);
            let mut wire = params.init_request();
            wire.set_importer(request.importer.as_str());
            let mut sources = wire.reborrow().init_sources(request.sources.len() as u32);
            for (index, source) in request.sources.iter().enumerate() {
                sources.set(index as u32, source.as_str());
            }
            wire.set_dest(request.dest.as_str());
            wire.set_watch(request.watch);
            wire.set_root(request.root.as_str());
            let mut settings = wire.init_settings();
            settings.set_canonical_value(&request.settings.canonical_value);
            let mut blobs = settings.init_blobs(request.settings.blobs.len() as u32);
            for (index, blob) in request.settings.blobs.iter().enumerate() {
                blobs.set(index as u32, blob);
            }
        }
        let response = call.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::uuid_call::Which::Success(uuid) => Ok(RemoteCall::Success(BundleUuid(
                fixed::<16>(uuid?.get_bytes()?, "import.bundle")?,
            ))),
            schema::uuid_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::uuid_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::uuid_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::uuid_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }
}

#[derive(Clone)]
pub struct RemoteSnapshot {
    client: schema::snapshot::Client,
    basis: RpcBasis,
}

impl std::fmt::Debug for RemoteSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteSnapshot")
            .field("basis", &self.basis)
            .finish_non_exhaustive()
    }
}

impl RemoteSnapshot {
    async fn open(
        client: schema::snapshot::Client,
        mut basis: RpcBasis,
    ) -> Result<RemoteCall<Self>, capnp::Error> {
        let response = client.version_request().send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::u_int64_call::Which::Success(version) => {
                basis.snapshot.version = InputVersion(version);
                Ok(RemoteCall::Success(Self { client, basis }))
            }
            schema::u_int64_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::u_int64_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::u_int64_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::u_int64_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    pub fn basis(&self) -> &RpcBasis {
        &self.basis
    }

    pub async fn refresh(&self) -> Result<RemoteCall<Self>, capnp::Error> {
        let response = self.client.refresh_request().send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::snapshot_call::Which::Success(snapshot) => {
                Self::open(snapshot?, self.basis.clone()).await
            }
            schema::snapshot_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::snapshot_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::snapshot_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::snapshot_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    pub async fn fetch(
        &self,
        content_hash: ContentHash,
    ) -> Result<RemoteCall<TerminalEvent<RemoteChunkStream>>, capnp::Error> {
        let mut request = self.client.fetch_request();
        request.get().set_hash(&content_hash.0);
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::chunk_stream_call::Which::Success(value) => {
                let value = value?;
                let basis = decode_rpc_basis(value.get_basis()?, &self.basis)?;
                let mut load_edges = Vec::with_capacity(value.get_load_edges()?.len() as usize);
                for edge in value.get_load_edges()?.iter() {
                    load_edges.push(ServedLoadEdge {
                        asset: AssetUuid(fixed::<16>(edge.get_asset()?, "fetch.loadEdge.asset")?),
                        expected_terminal: distill_core::id::TypeUuid(fixed::<16>(
                            edge.get_expected_terminal()?,
                            "fetch.loadEdge.expectedTerminal",
                        )?),
                    });
                }
                if load_edges.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(capnp::Error::failed(
                        "fetch load edges are not strictly sorted".into(),
                    ));
                }
                Ok(RemoteCall::Success(TerminalEvent {
                    basis,
                    value: RemoteChunkStream {
                        client: value.get_chunks()?,
                        total_bytes: value.get_total_bytes(),
                        load_edges,
                    },
                }))
            }
            schema::chunk_stream_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::chunk_stream_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::chunk_stream_call::Which::SnapshotExpired(()) => {
                Ok(RemoteCall::SnapshotExpired)
            }
            schema::chunk_stream_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    pub async fn resolve(
        &self,
        uuid: AssetUuid,
    ) -> Result<RemoteCall<TerminalEvent<ResolveResult>>, capnp::Error> {
        self.resolve_with(uuid, false).await
    }

    /// [`Self::resolve`] for a pack traversal: any build it needs is
    /// admitted as batch work. The answer is the same.
    pub async fn resolve_batch(
        &self,
        uuid: AssetUuid,
    ) -> Result<RemoteCall<TerminalEvent<ResolveResult>>, capnp::Error> {
        self.resolve_with(uuid, true).await
    }

    async fn resolve_with(
        &self,
        uuid: AssetUuid,
        batch: bool,
    ) -> Result<RemoteCall<TerminalEvent<ResolveResult>>, capnp::Error> {
        let mut request = self.client.resolve_request();
        request.get().set_uuid(&uuid.0);
        request.get().set_batch(batch);
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::resolve_call::Which::Success(value) => {
                let value = value?;
                let basis = decode_rpc_basis(value.get_basis()?, &self.basis)?;
                let resolved = decode_resolve(value.get_result()?)?;
                Ok(RemoteCall::Success(TerminalEvent {
                    basis,
                    value: resolved,
                }))
            }
            schema::resolve_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::resolve_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::resolve_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::resolve_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    pub async fn resolve_path(
        &self,
        path: &str,
    ) -> Result<RemoteCall<TerminalEvent<PathResolveResult>>, capnp::Error> {
        let mut request = self.client.resolve_path_request();
        request.get().set_path(path);
        let response = request.send().promise.await?;
        self.path_call(response.get()?.get_result()?)
    }

    /// The runtime asset whose local id is `name` among those imported at
    /// `path` (protocol 12).
    pub async fn resolve_named(
        &self,
        path: &str,
        name: &str,
    ) -> Result<RemoteCall<TerminalEvent<PathResolveResult>>, capnp::Error> {
        let mut request = self.client.resolve_named_request();
        request.get().set_path(path);
        request.get().set_name(name);
        let response = request.send().promise.await?;
        self.path_call(response.get()?.get_result()?)
    }

    fn path_call(
        &self,
        result: schema::path_resolve_call::Reader<'_>,
    ) -> Result<RemoteCall<TerminalEvent<PathResolveResult>>, capnp::Error> {
        match result.which()? {
            schema::path_resolve_call::Which::Success(value) => {
                let value = value?;
                let basis = decode_rpc_basis(value.get_basis()?, &self.basis)?;
                let resolved = decode_path(value.get_result()?)?;
                Ok(RemoteCall::Success(TerminalEvent {
                    basis,
                    value: resolved,
                }))
            }
            schema::path_resolve_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::path_resolve_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::path_resolve_call::Which::SnapshotExpired(()) => {
                Ok(RemoteCall::SnapshotExpired)
            }
            schema::path_resolve_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    /// The runtime assets `query` selects, UUID-sorted.
    pub async fn query(
        &self,
        query: &AssetQuery,
    ) -> Result<RemoteCall<Vec<AssetUuid>>, capnp::Error> {
        let mut request = self.client.query_request();
        write_asset_query(request.get().init_query(), query);
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::uuid_list_call::Which::Success(values) => {
                let mut assets = Vec::new();
                for value in values?.iter() {
                    assets.push(AssetUuid(fixed::<16>(value.get_bytes()?, "query.uuid")?));
                }
                Ok(RemoteCall::Success(assets))
            }
            schema::uuid_list_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::uuid_list_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::uuid_list_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::uuid_list_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    /// The runtime entry of `uuid`; an authoring-only or absent one answers
    /// an `AssetNotFound` error.
    pub async fn entry(&self, uuid: AssetUuid) -> Result<RemoteCall<MetadataEntry>, capnp::Error> {
        let mut request = self.client.entry_request();
        request.get().set_uuid(&uuid.0);
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::entry_meta_call::Which::Success(value) => {
                Ok(RemoteCall::Success(decode_entry(value?)?))
            }
            schema::entry_meta_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::entry_meta_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::entry_meta_call::Which::SnapshotExpired(()) => Ok(RemoteCall::SnapshotExpired),
            schema::entry_meta_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }

    /// The published schema's runtime policy for terminal type `type_uuid`,
    /// at this snapshot's target and pipeline epoch.
    pub async fn runtime_type_policy(
        &self,
        type_uuid: TypeUuid,
    ) -> Result<RemoteCall<RuntimeTypePolicy>, capnp::Error> {
        let mut request = self.client.runtime_type_policy_request();
        request.get().set_type_uuid(&type_uuid.0);
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::runtime_type_policy_call::Which::Success(value) => {
                Ok(RemoteCall::Success(RuntimeTypePolicy {
                    build_only: value?.get_build_only(),
                }))
            }
            schema::runtime_type_policy_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::runtime_type_policy_call::Which::ConfigurationFailed(value) => Ok(
                RemoteCall::ConfigurationFailed(decode_configuration_error(value?)?),
            ),
            schema::runtime_type_policy_call::Which::SnapshotExpired(()) => {
                Ok(RemoteCall::SnapshotExpired)
            }
            schema::runtime_type_policy_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }
}

/// The target-independent metadata hub of `Root.metadata`.
#[derive(Clone)]
pub struct RemoteMetadataHub {
    client: schema::metadata_hub::Client,
}

impl std::fmt::Debug for RemoteMetadataHub {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteMetadataHub")
            .finish_non_exhaustive()
    }
}

impl RemoteMetadataHub {
    pub fn connected(outcome: RemoteMetadataOutcome) -> Result<Self, Box<RemoteMetadataOutcome>> {
        match outcome {
            RemoteMetadataOutcome::Connected { hub, .. } => Ok(Self { client: hub }),
            other => Err(Box::new(other)),
        }
    }

    /// Pin an authoring view at the current store stamp.
    pub async fn authoring_snapshot(
        &self,
    ) -> Result<RemoteCall<RemoteMetadataAuthoringSnapshot>, capnp::Error> {
        let response = self
            .client
            .authoring_snapshot_request()
            .send()
            .promise
            .await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::metadata_authoring_snapshot_call::Which::Success(client) => {
                Ok(RemoteCall::Success(RemoteMetadataAuthoringSnapshot {
                    client: client?,
                }))
            }
            schema::metadata_authoring_snapshot_call::Which::SnapshotExpired(()) => {
                Ok(RemoteCall::SnapshotExpired)
            }
            schema::metadata_authoring_snapshot_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }
}

/// An authoring view of the metadata hub: authoring-only entries are
/// visible to `inspect`.
#[derive(Clone)]
pub struct RemoteMetadataAuthoringSnapshot {
    client: schema::metadata_authoring_snapshot::Client,
}

impl std::fmt::Debug for RemoteMetadataAuthoringSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteMetadataAuthoringSnapshot")
            .finish_non_exhaustive()
    }
}

impl RemoteMetadataAuthoringSnapshot {
    /// The authored entry of `uuid` with its value, authenticated against
    /// its logical schema. The inspection carries the snapshot's stamp.
    pub async fn inspect(
        &self,
        uuid: AssetUuid,
    ) -> Result<RemoteCall<AuthoringInspectResult>, capnp::Error> {
        let mut request = self.client.inspect_request();
        request.get().init_uuid().set_bytes(&uuid.0);
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::metadata_authoring_inspect_call::Which::Success(value) => {
                Ok(RemoteCall::Success(AuthoringInspectResult::Inspection(
                    decode_authoring_inspection(value?)?,
                )))
            }
            schema::metadata_authoring_inspect_call::Which::Missing(()) => {
                Ok(RemoteCall::Success(AuthoringInspectResult::Missing))
            }
            schema::metadata_authoring_inspect_call::Which::RoleIneligible(value) => Ok(
                RemoteCall::Success(AuthoringInspectResult::RoleIneligible {
                    observed: decode_role(value?.get_observed()?),
                }),
            ),
            schema::metadata_authoring_inspect_call::Which::Drifted(value) => {
                let (input, current) = decode_drifted(value?)?;
                Ok(RemoteCall::Success(AuthoringInspectResult::Drifted {
                    input,
                    current,
                }))
            }
            schema::metadata_authoring_inspect_call::Which::SnapshotExpired(()) => {
                Ok(RemoteCall::SnapshotExpired)
            }
            schema::metadata_authoring_inspect_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_rpc_error(value?)?))
            }
        }
    }
}

pub struct RemoteChunkStream {
    client: schema::chunk_stream::Client,
    total_bytes: u64,
    load_edges: Vec<ServedLoadEdge>,
}

impl std::fmt::Debug for RemoteChunkStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteChunkStream")
            .finish_non_exhaustive()
    }
}

impl RemoteChunkStream {
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn load_edges(&self) -> &[ServedLoadEdge] {
        &self.load_edges
    }

    pub async fn next_chunk(&mut self) -> Result<Option<ArtifactChunk>, capnp::Error> {
        let response = self.client.next_request().send().promise.await?;
        let value = response.get()?;
        if value.get_done() {
            return Ok(None);
        }
        let kind = match value.get_kind() {
            0 if value.get_index() == 0 => ArtifactChunkKind::Structural,
            1 => ArtifactChunkKind::Blob {
                index: value.get_index(),
            },
            kind => {
                return Err(capnp::Error::failed(format!(
                    "invalid artifact chunk kind/index {kind}/{}",
                    value.get_index()
                )))
            }
        };
        Ok(Some(ArtifactChunk {
            kind,
            offset: value.get_offset(),
            bytes: Arc::from(value.get_bytes()?.to_vec()),
        }))
    }
}

pub struct RemoteSubscription {
    client: schema::delta_stream::Client,
    basis: RpcBasis,
    since: InputVersion,
    installed: InputVersion,
    initial: bool,
}

impl std::fmt::Debug for RemoteSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteSubscription")
            .field("since", &self.since)
            .field("installed", &self.installed)
            .finish_non_exhaustive()
    }
}

impl RemoteSubscription {
    pub async fn next(&mut self) -> Result<Option<StreamEvent>, capnp::Error> {
        let response = self.client.next_request().send().promise.await?;
        let value = response.get()?;
        if value.get_done() {
            return Ok(None);
        }
        let event = value.get_event()?;
        let basis = decode_connection_basis(event.get_basis()?, &self.basis)?;
        let decoded = match event.which()? {
            schema::stream_event::Which::InitialDelta(deltas) => {
                if !self.initial {
                    return Err(capnp::Error::failed(
                        "delta stream repeated its initial event".into(),
                    ));
                }
                let mut output = Vec::new();
                for delta in deltas?.iter() {
                    output.push(decode_delta(delta, &self.basis)?);
                }
                StreamEvent::InitialDelta {
                    basis,
                    since: self.since,
                    installed: self.installed,
                    deltas: output,
                }
            }
            schema::stream_event::Which::Delta(delta) => {
                StreamEvent::Delta(decode_delta(delta?, &self.basis)?)
            }
            schema::stream_event::Which::ResyncRequired(oldest) => StreamEvent::ResyncRequired {
                basis,
                oldest_available: InputVersion(oldest),
            },
            schema::stream_event::Which::RestartRequired(keys) => StreamEvent::Asset {
                basis,
                event: AssetEvent::RestartRequired {
                    keys: decode_text_list(keys?)?,
                },
            },
            schema::stream_event::Which::ReconnectRequired(reason) => StreamEvent::Asset {
                basis,
                event: AssetEvent::ReconnectRequired {
                    reason: decode_reconnect(reason?),
                },
            },
        };
        self.initial = false;
        Ok(Some(decoded))
    }
}

fn decode_drifted(
    drifted: schema::drifted_resolve::Reader<'_>,
) -> Result<(DriftedInput, SnapshotStamp), capnp::Error> {
    let input = match drifted.get_input()?.which()? {
        schema::drifted_input_value::Which::File(value) => {
            DriftedInput::File(text(value?, "resolve.drifted.file")?)
        }
        schema::drifted_input_value::Which::Asset(value) => {
            DriftedInput::Asset(AssetUuid(fixed::<16>(value?, "resolve.drifted.asset")?))
        }
        schema::drifted_input_value::Which::Query(value) => {
            DriftedInput::Query(text(value?, "resolve.drifted.query")?)
        }
        schema::drifted_input_value::Which::Dylib(()) => DriftedInput::Dylib,
        schema::drifted_input_value::Which::Tool(value) => {
            DriftedInput::Tool(text(value?, "resolve.drifted.tool")?)
        }
    };
    Ok((input, decode_stamp(drifted.get_current()?)?))
}

fn decode_resolve(
    value: schema::resolve_result::Reader<'_>,
) -> Result<ResolveResult, capnp::Error> {
    Ok(match value.which()? {
        schema::resolve_result::Which::Built(hash) => ResolveResult::Built {
            content_hash: ContentHash(fixed::<32>(hash?, "resolve.built")?),
        },
        schema::resolve_result::Which::Drifted(drifted) => {
            let (input, current) = decode_drifted(drifted?)?;
            ResolveResult::Drifted { input, current }
        }
        schema::resolve_result::Which::Failed(error) => ResolveResult::Failed {
            error: text(error?, "resolve.failed")?,
        },
        schema::resolve_result::Which::Missing(()) => ResolveResult::Missing,
        schema::resolve_result::Which::Deleted(stamp) => ResolveResult::Deleted {
            at: decode_stamp(stamp?)?,
        },
        schema::resolve_result::Which::RoleIneligible(value) => ResolveResult::RoleIneligible {
            observed: decode_role(value?.get_observed()?),
        },
    })
}

fn write_asset_query(mut output: schema::asset_query::Builder<'_>, query: &AssetQuery) {
    // Every selector's union defaults to `absent`.
    if let Some(uuid) = query.uuid {
        output.reborrow().init_uuid().set_value(&uuid.0);
    }
    if let Some(path) = &query.bundle_path {
        output
            .reborrow()
            .init_bundle_path()
            .set_value(path.as_str());
    }
    if let Some(local_id) = &query.local_id {
        output
            .reborrow()
            .init_local_id()
            .set_value(local_id.as_str());
    }
    if let Some(bundle) = query.bundle_uuid {
        output.reborrow().init_bundle_uuid().set_value(&bundle.0);
    }
    if let Some(authored) = query.authored_type {
        output
            .reborrow()
            .init_authored_type()
            .set_value(&authored.0);
    }
    if let Some(terminal) = query.terminal_type {
        output
            .reborrow()
            .init_terminal_type()
            .set_value(&terminal.0);
    }
    if let Some(tag) = &query.tag {
        let mut selector = output.reborrow().init_tag().init_value();
        selector.set_tag(tag.tag.as_str());
        if let Some(value) = &tag.value {
            selector.init_value().set_value(value.as_str());
        }
    }
    if let Some(prefix) = &query.path_prefix {
        output
            .reborrow()
            .init_path_prefix()
            .set_value(prefix.as_str());
    }
    if let Some(glob) = &query.path_glob {
        output.reborrow().init_path_glob().set_value(glob.as_str());
    }
    if let Some(authoring_only) = query.authoring_only {
        output.init_authoring_only().set_value(authoring_only);
    }
}

fn decode_entry(value: schema::entry_meta::Reader<'_>) -> Result<MetadataEntry, capnp::Error> {
    let mut tags = BTreeMap::new();
    for tag in value.get_tags()?.iter() {
        let name = text(tag.get_tag()?, "entry.tag")?;
        let tag_value = if tag.get_has_value() {
            Some(text(tag.get_value()?, "entry.tag.value")?)
        } else {
            None
        };
        tags.insert(name, tag_value);
    }
    Ok(MetadataEntry {
        uuid: AssetUuid(fixed::<16>(value.get_uuid()?.get_bytes()?, "entry.uuid")?),
        bundle: BundleUuid(fixed::<16>(
            value.get_bundle()?.get_bytes()?,
            "entry.bundle",
        )?),
        local_id: text(value.get_local_id()?, "entry.localId")?,
        normalized_path: text(value.get_normalized_path()?, "entry.normalizedPath")?,
        authored_type: TypeUuid(fixed::<16>(
            value.get_authored_type()?.get_bytes()?,
            "entry.authoredType",
        )?),
        terminal_type: TypeUuid(fixed::<16>(
            value.get_terminal_type()?.get_bytes()?,
            "entry.terminalType",
        )?),
        schema_hash: LogicalHash(fixed::<32>(value.get_schema_hash()?, "entry.schemaHash")?),
        role: decode_role(value.get_role()?),
        tags,
    })
}

fn decode_path(
    value: schema::path_resolve_result::Reader<'_>,
) -> Result<PathResolveResult, capnp::Error> {
    Ok(match value.which()? {
        schema::path_resolve_result::Which::Resolved(uuid) => {
            PathResolveResult::Resolved(AssetUuid(fixed::<16>(uuid?, "path.resolved")?))
        }
        schema::path_resolve_result::Which::Missing(()) => PathResolveResult::Missing,
        schema::path_resolve_result::Which::Ambiguous(values) => {
            let mut candidates = Vec::new();
            for value in values?.iter() {
                candidates.push(AssetUuid(fixed::<16>(value?, "path.ambiguous")?));
            }
            if candidates.len() < 2 || candidates.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(capnp::Error::failed(
                    "ambiguous path candidates are not canonical".into(),
                ));
            }
            PathResolveResult::Failed(PathResolveFailure::Ambiguous { candidates })
        }
    })
}

fn decode_delta(
    value: schema::delta::Reader<'_>,
    template: &RpcBasis,
) -> Result<Delta, capnp::Error> {
    let basis = decode_connection_basis(value.get_basis()?, template)?;
    let mut assets = Vec::new();
    for asset in value.get_assets()?.iter() {
        let uuid = AssetUuid(fixed::<16>(asset.get_uuid()?, "delta.asset")?);
        let state = match asset.get_state()? {
            schema::AssetDeltaState::Changed => AssetDeltaState::Changed,
            schema::AssetDeltaState::Deleted => AssetDeltaState::Deleted,
        };
        assets.push((uuid, state));
    }
    if assets.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(capnp::Error::failed(
            "delta assets are not strictly UUID-sorted".into(),
        ));
    }
    Ok(Delta {
        basis,
        assets,
        paths: decode_text_list(value.get_paths()?)?,
    })
}

fn decode_connection_basis(
    input: schema::rpc_basis_value::Reader<'_>,
    template: &RpcBasis,
) -> Result<RpcBasis, capnp::Error> {
    let mut expected = template.clone();
    expected.snapshot.version = InputVersion(input.get_stamp()?.get_version());
    decode_rpc_basis(input, &expected)
}

fn decode_stamp(value: schema::snapshot_stamp::Reader<'_>) -> Result<SnapshotStamp, capnp::Error> {
    Ok(SnapshotStamp {
        instance: StoreInstanceId(fixed::<16>(value.get_instance()?, "stamp.instance")?),
        version: InputVersion(value.get_version()),
    })
}

fn decode_reconnect(value: schema::ReconnectReason) -> ReconnectReason {
    match value {
        schema::ReconnectReason::PipelineEpochChanged => ReconnectReason::PipelineEpochChanged,
    }
}

fn decode_role(value: schema::AuthoringEntryRole) -> AuthoringEntryRole {
    match value {
        schema::AuthoringEntryRole::Runtime => AuthoringEntryRole::Runtime,
        schema::AuthoringEntryRole::AuthoringOnly => AuthoringEntryRole::AuthoringOnly,
    }
}

fn decode_text_list(value: capnp::text_list::Reader<'_>) -> Result<Vec<String>, capnp::Error> {
    value
        .iter()
        .map(|value| text(value?, "text list value"))
        .collect()
}

fn text(value: capnp::text::Reader<'_>, field: &str) -> Result<String, capnp::Error> {
    value
        .to_str()
        .map(str::to_owned)
        .map_err(|_| capnp::Error::failed(format!("{field} is not valid UTF-8")))
}

fn fixed<const N: usize>(value: &[u8], field: &str) -> Result<[u8; N], capnp::Error> {
    value
        .try_into()
        .map_err(|_| capnp::Error::failed(format!("{field} must be exactly {N} bytes")))
}
