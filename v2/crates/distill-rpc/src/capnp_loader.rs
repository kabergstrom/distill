//! Typed client-side realization of the loader subset of the Cap'n Proto RPC.

use std::sync::Arc;

use distill_core::attestation::CompiledAttestationDigest;
use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_store::state::{InputVersion, SnapshotStamp, StoreInstanceId};

use crate::capnp_transport::{
    decode_configuration_poison, decode_rpc_basis, decode_version_poison, read_attestation_failure,
    schema, validate_reattest_failure_context, RemoteConnectOutcome,
};
use crate::{
    compute_policy_digest, ArtifactChunk, ArtifactChunkKind, AssetDeltaState, AssetEvent,
    AttestationExpansionRequired, AttestationFailure, AuthoringEntryRole, ConfigurationPoison,
    Delta, DriftedInput, LoadPolicyAttestation, LoadPolicyEntry, PathResolveFailure,
    PathResolveResult, ReattestRequest, ReattestSuccess, ReconnectReason, ResolveResult, RpcBasis,
    StreamEvent, TerminalEvent, VersionPoison,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteError {
    pub code: u16,
    pub message: String,
}

#[derive(Debug)]
pub enum RemoteCall<T> {
    Success(T),
    ReconnectRequired(ReconnectReason),
    AttestationExpansionRequired(AttestationExpansionRequired),
    AttestationFailure(AttestationFailure),
    StaleAttestationBase { expected: u64, observed: u64 },
    AttestationGenerationOverflow { base: u64 },
    ConfigurationPoisoned(ConfigurationPoison),
    VersionPoisoned(VersionPoison),
    LeaseFailure(RemoteError),
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
            RemoteConnectOutcome::Connected {
                hub,
                instance,
                policy_generation,
                target_generation,
                attestation_generation,
                load_policy,
                daemon_compiled_projection,
            } => Ok(Self {
                client: hub,
                basis: RpcBasis {
                    snapshot: SnapshotStamp {
                        instance,
                        version: InputVersion(0),
                    },
                    load_policy,
                    policy_generation,
                    target_generation,
                    attestation_generation,
                    daemon_compiled_projection,
                },
            }),
            other => Err(Box::new(other)),
        }
    }

    pub fn attestation_generation(&self) -> u64 {
        self.basis.attestation_generation
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
            schema::snapshot_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::snapshot_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::snapshot_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_error(value?)?))
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
                let basis = decode_connection_basis(value.get_basis()?, &self.basis)?;
                Ok(RemoteCall::Success(TerminalEvent {
                    basis,
                    value: RemoteChunkStream {
                        client: value.get_chunks()?,
                        total_bytes: value.get_total_bytes(),
                    },
                }))
            }
            schema::chunk_stream_call::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::chunk_stream_call::Which::AttestationExpansionRequired(value) => Ok(
                RemoteCall::AttestationExpansionRequired(decode_expansion(value?)?),
            ),
            schema::chunk_stream_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::chunk_stream_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::chunk_stream_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_error(value?)?))
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
            schema::data_call::Which::AttestationExpansionRequired(value) => Ok(
                RemoteCall::AttestationExpansionRequired(decode_expansion(value?)?),
            ),
            schema::data_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::data_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::data_call::Which::Error(value) => Ok(RemoteCall::Error(decode_error(value?)?)),
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
            schema::subscribe_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::subscribe_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::subscribe_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_error(value?)?))
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
            schema::void_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::void_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::void_call::Which::Error(value) => Ok(RemoteCall::Error(decode_error(value?)?)),
        }
    }

    pub async fn reattest(
        &mut self,
        reattest: &ReattestRequest,
    ) -> Result<RemoteCall<ReattestSuccess>, capnp::Error> {
        validate_reattest_request(reattest)?;
        if reattest.base_attestation_generation != self.basis.attestation_generation {
            return Err(capnp::Error::failed(
                "reattest base differs from the adopted remote Hub generation".into(),
            ));
        }
        let mut request = self.client.reattest_request();
        write_reattest(request.get(), reattest);
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;
        match result.which()? {
            schema::reattest_result::Which::Success(value) => {
                let value = value?;
                let installed = value.get_installed_attestation_generation();
                if installed != reattest.successor_attestation_generation {
                    return Err(capnp::Error::failed(
                        "reattest success returned an unexpected generation".into(),
                    ));
                }
                let rows = decode_policy_rows(value.get_load_policy()?)?;
                let digest = fixed::<32>(value.get_policy_digest()?, "reattest.policyDigest")?;
                if compute_policy_digest(&rows)
                    .map_err(|error| capnp::Error::failed(error.to_string()))?
                    != digest
                    || rows != reattest.load_policy
                    || digest != reattest.policy_digest
                {
                    return Err(capnp::Error::failed(
                        "reattest policy response does not authenticate the proposed set".into(),
                    ));
                }
                let daemon_compiled_projection = CompiledAttestationDigest(fixed::<32>(
                    value.get_daemon_compiled_projection()?,
                    "reattest.daemonCompiledProjection",
                )?);
                if daemon_compiled_projection != reattest.dsca {
                    return Err(capnp::Error::failed(
                        "reattest compiled projection differs from the proposed set".into(),
                    ));
                }
                let expected_policy_generation = if rows == self.basis.load_policy.rows {
                    self.basis.policy_generation
                } else {
                    self.basis.policy_generation.checked_add(1).ok_or_else(|| {
                        capnp::Error::failed("local policy generation overflow".into())
                    })?
                };
                if value.get_policy_generation() != expected_policy_generation {
                    return Err(capnp::Error::failed(
                        "reattest success returned an unexpected policy generation".into(),
                    ));
                }
                let success = ReattestSuccess {
                    installed_attestation_generation: installed,
                    daemon_compiled_projection,
                    load_policy: Arc::new(LoadPolicyAttestation { rows, digest }),
                    policy_generation: value.get_policy_generation(),
                };
                self.basis.attestation_generation = installed;
                self.basis.daemon_compiled_projection = daemon_compiled_projection;
                self.basis.load_policy = Arc::clone(&success.load_policy);
                self.basis.policy_generation = success.policy_generation;
                Ok(RemoteCall::Success(success))
            }
            schema::reattest_result::Which::AttestationFailure(value) => {
                let failure = read_attestation_failure(value?)?;
                validate_reattest_failure_context(&failure, reattest)?;
                Ok(RemoteCall::AttestationFailure(failure))
            }
            schema::reattest_result::Which::StaleAttestationBase(value) => {
                let value = value?;
                if value.get_code() != crate::STALE_ATTESTATION_BASE_CODE {
                    return Err(capnp::Error::failed(
                        "invalid stale-attestation-base code".into(),
                    ));
                }
                Ok(RemoteCall::StaleAttestationBase {
                    expected: value.get_expected(),
                    observed: value.get_observed(),
                })
            }
            schema::reattest_result::Which::AttestationGenerationOverflow(value) => {
                let value = value?;
                if value.get_code() != crate::ATTESTATION_GENERATION_OVERFLOW_CODE {
                    return Err(capnp::Error::failed(
                        "invalid attestation-generation-overflow code".into(),
                    ));
                }
                Ok(RemoteCall::AttestationGenerationOverflow {
                    base: value.get_base(),
                })
            }
            schema::reattest_result::Which::ReconnectRequired(value) => Ok(
                RemoteCall::ReconnectRequired(decode_reconnect(value?.get_reason()?)),
            ),
            schema::reattest_result::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::reattest_result::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::reattest_result::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_error(value?)?))
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
            schema::u_int64_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::u_int64_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::u_int64_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_error(value?)?))
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
            schema::snapshot_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::snapshot_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::snapshot_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_error(value?)?))
            }
        }
    }

    pub async fn resolve(
        &self,
        uuid: AssetUuid,
    ) -> Result<RemoteCall<TerminalEvent<ResolveResult>>, capnp::Error> {
        let mut request = self.client.resolve_request();
        request.get().set_uuid(&uuid.0);
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
            schema::resolve_call::Which::AttestationExpansionRequired(value) => Ok(
                RemoteCall::AttestationExpansionRequired(decode_expansion(value?)?),
            ),
            schema::resolve_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::resolve_call::Which::VersionPoisoned(value) => {
                Ok(RemoteCall::VersionPoisoned(decode_version_poison(value?)?))
            }
            schema::resolve_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::resolve_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_error(value?)?))
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
        let result = response.get()?.get_result()?;
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
            schema::path_resolve_call::Which::ConfigurationPoisoned(value) => Ok(
                RemoteCall::ConfigurationPoisoned(decode_configuration_poison(value?)?),
            ),
            schema::path_resolve_call::Which::VersionPoisoned(value) => {
                Ok(RemoteCall::VersionPoisoned(decode_version_poison(value?)?))
            }
            schema::path_resolve_call::Which::LeaseFailure(value) => {
                Ok(RemoteCall::LeaseFailure(decode_lease(value?)?))
            }
            schema::path_resolve_call::Which::Error(value) => {
                Ok(RemoteCall::Error(decode_error(value?)?))
            }
        }
    }
}

pub struct RemoteChunkStream {
    client: schema::chunk_stream::Client,
    total_bytes: u64,
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

fn decode_resolve(
    value: schema::resolve_result::Reader<'_>,
) -> Result<ResolveResult, capnp::Error> {
    Ok(match value.which()? {
        schema::resolve_result::Which::Built(hash) => ResolveResult::Built {
            content_hash: ContentHash(fixed::<32>(hash?, "resolve.built")?),
        },
        schema::resolve_result::Which::Drifted(drifted) => {
            let drifted = drifted?;
            let input = drifted.get_input()?;
            let input = match input.which()? {
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
            ResolveResult::Drifted {
                input,
                current: decode_stamp(drifted.get_current()?)?,
            }
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
            schema::AssetDeltaState::Restored => AssetDeltaState::Restored,
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

fn decode_expansion(
    value: schema::attestation_expansion_required::Reader<'_>,
) -> Result<AttestationExpansionRequired, capnp::Error> {
    let required = value.get_required_type_uuids()?;
    let mut types = Vec::with_capacity(required.len() as usize);
    for value in required.iter() {
        types.push(TypeUuid(fixed::<16>(value?, "expansion.required")?));
    }
    if types.is_empty() || types.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(capnp::Error::failed(
            "attestation expansion set is not canonical".into(),
        ));
    }
    Ok(AttestationExpansionRequired {
        snapshot: decode_stamp(value.get_snapshot()?)?,
        closure_identity: fixed::<32>(value.get_closure_identity()?, "expansion.closureIdentity")?,
        required: types,
    })
}

fn validate_reattest_request(request: &ReattestRequest) -> Result<(), capnp::Error> {
    crate::attestation::validate_attestation_shape(
        &request.compiled_registry,
        request.dsca,
        &request.load_policy,
        request.policy_digest,
    )
    .map_err(|error| capnp::Error::failed(format!("invalid reattestation: {error}")))
}

fn write_reattest(
    mut output: schema::hub::reattest_params::Builder<'_>,
    request: &ReattestRequest,
) {
    output.set_epoch(request.epoch.0);
    output.set_base_attestation_generation(request.base_attestation_generation);
    output.set_successor_attestation_generation(request.successor_attestation_generation);
    output.set_target_def_hash(&request.target_definition_hash.0);
    let mut compiled = output
        .reborrow()
        .init_compiled_registry(request.compiled_registry.len() as u32);
    for (index, row) in request.compiled_registry.iter().enumerate() {
        let mut wire = compiled.reborrow().get(index as u32);
        wire.set_type_uuid(&row.type_uuid.0);
        wire.set_logical_hash(&row.logical_hash.0);
        wire.set_native_layout_digest(&row.native_layout_digest);
        wire.set_build_only(row.build_only);
        wire.set_registry_extras_digest(&row.registry_extras_digest.0);
        wire.set_registry_extras(
            &row.registry_extras
                .encode()
                .expect("validated reattestation carries canonical extras"),
        );
    }
    output.set_dsca_aggregate(&request.dsca.0);
    let mut policies = output
        .reborrow()
        .init_load_policy(request.load_policy.len() as u32);
    for (index, row) in request.load_policy.iter().enumerate() {
        let mut wire = policies.reborrow().get(index as u32);
        wire.set_type_uuid(&row.type_uuid.0);
        wire.set_build_only(row.build_only);
    }
    output.set_policy_digest(&request.policy_digest);
}

fn decode_policy_rows(
    values: capnp::struct_list::Reader<'_, schema::load_policy_entry::Owned>,
) -> Result<Vec<LoadPolicyEntry>, capnp::Error> {
    let mut rows = Vec::with_capacity(values.len() as usize);
    for value in values.iter() {
        rows.push(LoadPolicyEntry {
            type_uuid: TypeUuid(fixed::<16>(value.get_type_uuid()?, "policy.typeUuid")?),
            build_only: value.get_build_only(),
        });
    }
    if rows
        .windows(2)
        .any(|pair| pair[0].type_uuid >= pair[1].type_uuid)
    {
        return Err(capnp::Error::failed(
            "load-policy rows are not strictly TypeUuid-sorted".into(),
        ));
    }
    Ok(rows)
}

fn decode_stamp(value: schema::snapshot_stamp::Reader<'_>) -> Result<SnapshotStamp, capnp::Error> {
    Ok(SnapshotStamp {
        instance: StoreInstanceId(fixed::<16>(value.get_instance()?, "stamp.instance")?),
        version: InputVersion(value.get_version()),
    })
}

fn decode_reconnect(value: schema::ReconnectReason) -> ReconnectReason {
    match value {
        schema::ReconnectReason::TargetDefinitionChanged => {
            ReconnectReason::TargetDefinitionChanged
        }
        schema::ReconnectReason::LoadPolicyChanged => ReconnectReason::LoadPolicyChanged,
        schema::ReconnectReason::CompiledAttestationChanged => {
            ReconnectReason::CompiledAttestationChanged
        }
        schema::ReconnectReason::StoreInstanceChanged => ReconnectReason::StoreInstanceChanged,
        schema::ReconnectReason::ProtocolEpochChanged => ReconnectReason::ProtocolEpochChanged,
    }
}

fn decode_role(value: schema::AuthoringEntryRole) -> AuthoringEntryRole {
    match value {
        schema::AuthoringEntryRole::Runtime => AuthoringEntryRole::Runtime,
        schema::AuthoringEntryRole::AuthoringOnly => AuthoringEntryRole::AuthoringOnly,
    }
}

fn decode_error(value: schema::rpc_error::Reader<'_>) -> Result<RemoteError, capnp::Error> {
    Ok(RemoteError {
        code: value.get_code(),
        message: text(value.get_message()?, "rpc.error")?,
    })
}

fn decode_lease(value: schema::lease_failure::Reader<'_>) -> Result<RemoteError, capnp::Error> {
    Ok(RemoteError {
        code: value.get_code(),
        message: text(value.get_message()?, "rpc.leaseFailure")?,
    })
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
