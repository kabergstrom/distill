//! Development `LoaderIO` backed by one target-bound daemon Hub.

use std::collections::BTreeSet;
use std::sync::Arc;

use distill_build::trace::EntryRole;
use distill_core::id::{AssetUuid, ContentHash};
use distill_rpc::{
    ArtifactChunkKind, AssetEvent, DeltaStream, Hub, RpcBasis, RpcFailure, RpcResult, Snapshot,
    StreamEvent,
};
use distill_wire::artifact::parse_artifact_parts;
use distill_wire::exec::Blob;

use crate::basis::{IoBasis, LoadPolicyAttestation, LoadPolicyError, LoadPolicyRow};
use crate::io::{
    AssetDeltaState, DriftedInput, FetchedArtifact, IoEvent, LoaderIO, PathResolveResult,
    ReconnectReason, ReqId, ResolveResult,
};

#[derive(Debug)]
pub enum RpcIoInitError {
    ReconnectRequired(ReconnectReason),
    Unavailable(String),
    LoadPolicy(LoadPolicyError),
}

impl std::fmt::Display for RpcIoInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "RPC loader initialization failed: {self:?}")
    }
}

impl std::error::Error for RpcIoInitError {}

/// Synchronous loader adapter for the transport-neutral in-process Hub.
pub struct RpcIo {
    hub: Hub,
    snapshot: Snapshot,
    basis: IoBasis,
    events: Vec<IoEvent>,
    delta_stream: Option<DeltaStream>,
    subscribed_assets: BTreeSet<AssetUuid>,
    subscribed_paths: BTreeSet<String>,
}

impl RpcIo {
    pub fn new(hub: Hub) -> Result<Self, RpcIoInitError> {
        let snapshot = match hub.snapshot() {
            RpcResult::Success(snapshot) => snapshot,
            result => return Err(init_failure(result)),
        };
        let basis = io_basis(snapshot.basis()).map_err(RpcIoInitError::LoadPolicy)?;
        Ok(Self {
            hub,
            snapshot,
            basis,
            events: Vec::new(),
            delta_stream: None,
            subscribed_assets: BTreeSet::new(),
            subscribed_paths: BTreeSet::new(),
        })
    }

    pub fn basis(&self) -> &IoBasis {
        &self.basis
    }

    fn request_failure(&mut self, req: ReqId, basis: &IoBasis, failure: RpcIoFailure) {
        match failure {
            RpcIoFailure::Reconnect(reason) => {
                self.events.push(IoEvent::ReconnectRequired { reason });
            }
            RpcIoFailure::Message(message) => self.events.push(IoEvent::RequestError {
                req,
                message,
                basis: basis.clone(),
            }),
        }
    }

    fn connection_failure(&mut self, failure: RpcIoFailure) {
        match failure {
            RpcIoFailure::Reconnect(reason) => {
                self.events.push(IoEvent::ReconnectRequired { reason });
            }
            RpcIoFailure::Message(message) => {
                self.events.push(IoEvent::ConnectionError { message });
            }
        }
    }

    fn refresh_snapshot(&mut self) {
        let snapshot = match rpc_result(self.hub.snapshot()) {
            Ok(snapshot) => snapshot,
            Err(failure) => {
                self.connection_failure(failure);
                return;
            }
        };
        let basis = match io_basis(snapshot.basis()) {
            Ok(basis) => basis,
            Err(error) => {
                self.events.push(IoEvent::ConnectionError {
                    message: format!("invalid RPC load-policy attestation: {error:?}"),
                });
                return;
            }
        };
        self.snapshot = snapshot;
        self.basis = basis;
    }

    fn drain_deltas(&mut self) {
        loop {
            let Some(event) = self.delta_stream.as_ref().and_then(DeltaStream::next) else {
                break;
            };
            self.accept_stream_event(event);
        }
    }

    fn accept_stream_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::InitialDelta { deltas, .. } => {
                for delta in deltas {
                    self.push_delta(delta);
                }
            }
            StreamEvent::Delta(delta) => self.push_delta(delta),
            StreamEvent::ResyncRequired { basis, .. } => {
                self.events.push(IoEvent::Delta {
                    stamp: basis.snapshot,
                    assets: self
                        .subscribed_assets
                        .iter()
                        .copied()
                        .map(|asset| (asset, AssetDeltaState::Changed))
                        .collect(),
                    paths: self.subscribed_paths.iter().cloned().collect(),
                });
            }
            StreamEvent::Asset { event, .. } => match event {
                AssetEvent::ReconnectRequired { reason } => {
                    self.events.push(IoEvent::ReconnectRequired {
                        reason: reconnect_reason(reason),
                    });
                }
                AssetEvent::Error { message, .. } => {
                    self.events.push(IoEvent::ConnectionError { message });
                }
                AssetEvent::RestartRequired { keys } => {
                    self.events.push(IoEvent::ConnectionError {
                        message: format!("daemon restart required for {}", keys.join(", ")),
                    });
                }
                _ => {}
            },
        }
    }

    fn push_delta(&mut self, delta: distill_rpc::Delta) {
        self.events.push(IoEvent::Delta {
            stamp: delta.basis.snapshot,
            assets: delta
                .assets
                .into_iter()
                .map(|(asset, state)| (asset, delta_state(state)))
                .collect(),
            paths: delta.paths,
        });
    }

    fn install_subscription(&mut self, result: RpcResult<distill_rpc::SubscriptionInstall>) {
        match rpc_result(result) {
            Ok(install) => self.delta_stream = Some(install.deltas),
            Err(failure) => self.connection_failure(failure),
        }
    }
}

impl LoaderIO for RpcIo {
    fn begin_sweep(&mut self) -> IoBasis {
        self.refresh_snapshot();
        self.basis.clone()
    }

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        if basis != &self.basis {
            self.request_failure(req, basis, RpcIoFailure::Message("stale RPC basis".into()));
            return;
        }
        let terminal = match rpc_result(self.snapshot.resolve(uuid)) {
            Ok(terminal) => terminal,
            Err(failure) => {
                self.request_failure(req, basis, failure);
                return;
            }
        };
        let event_basis = match io_basis(&terminal.basis) {
            Ok(basis) => basis,
            Err(error) => {
                self.request_failure(
                    req,
                    basis,
                    RpcIoFailure::Message(format!("invalid RPC basis: {error:?}")),
                );
                return;
            }
        };
        let result = match terminal.value {
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
        };
        self.events.push(IoEvent::Resolved {
            req,
            uuid,
            result,
            basis: event_basis,
        });
    }

    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis) {
        if basis != &self.basis {
            self.request_failure(req, basis, RpcIoFailure::Message("stale RPC basis".into()));
            return;
        }
        let terminal = match rpc_result(self.snapshot.fetch(content_hash)) {
            Ok(terminal) => terminal,
            Err(failure) => {
                self.request_failure(req, basis, failure);
                return;
            }
        };
        let event_basis = match io_basis(&terminal.basis) {
            Ok(basis) => basis,
            Err(error) => {
                self.request_failure(
                    req,
                    basis,
                    RpcIoFailure::Message(format!("invalid RPC basis: {error:?}")),
                );
                return;
            }
        };
        let (structural, raw_blobs) = match collect_chunks(terminal.value) {
            Ok(parts) => parts,
            Err(message) => {
                self.request_failure(req, basis, RpcIoFailure::Message(message));
                return;
            }
        };
        let blob_parts = raw_blobs.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let parsed = match parse_artifact_parts(&structural, &blob_parts) {
            Ok(parsed) if parsed.content_hash == content_hash => parsed,
            Ok(_) => {
                self.request_failure(
                    req,
                    basis,
                    RpcIoFailure::Message("fetched artifact ContentHash mismatch".into()),
                );
                return;
            }
            Err(error) => {
                self.request_failure(
                    req,
                    basis,
                    RpcIoFailure::Message(format!("invalid fetched artifact: {error}")),
                );
                return;
            }
        };
        let layout_hash = parsed.layout_hash;
        let wire_layout = match rpc_result(self.hub.wire_tree(layout_hash)) {
            Ok(bytes) => bytes,
            Err(failure) => {
                self.request_failure(req, basis, failure);
                return;
            }
        };
        let wire = match distill_wire::dswl::decode_dswl(&wire_layout) {
            Ok(wire) => wire,
            Err(error) => {
                self.request_failure(
                    req,
                    basis,
                    RpcIoFailure::Message(format!("invalid DSWL wire tree: {error:?}")),
                );
                return;
            }
        };
        if distill_wire::dswl::dswl_hash(&wire).ok() != Some(layout_hash) {
            self.request_failure(
                req,
                basis,
                RpcIoFailure::Message("DSWL wire-tree hash mismatch".into()),
            );
            return;
        }
        let blobs = raw_blobs
            .into_iter()
            .map(|bytes| {
                let len = bytes.len();
                let backing: Arc<dyn AsRef<[u8]> + Send + Sync> = Arc::new(bytes);
                Blob::new(backing, 0, len)
            })
            .collect();
        self.events.push(IoEvent::Fetched {
            req,
            content_hash,
            artifact: FetchedArtifact {
                structural: Arc::from(structural),
                blobs,
                wire_layout,
            },
            basis: event_basis,
        });
    }

    fn resolve_path(&mut self, req: ReqId, path: &str, basis: &IoBasis) {
        if basis != &self.basis {
            self.request_failure(req, basis, RpcIoFailure::Message("stale RPC basis".into()));
            return;
        }
        let terminal = match rpc_result(self.snapshot.resolve_path(path)) {
            Ok(terminal) => terminal,
            Err(failure) => {
                self.request_failure(req, basis, failure);
                return;
            }
        };
        let event_basis = match io_basis(&terminal.basis) {
            Ok(basis) => basis,
            Err(error) => {
                self.request_failure(
                    req,
                    basis,
                    RpcIoFailure::Message(format!("invalid RPC basis: {error:?}")),
                );
                return;
            }
        };
        let result = match terminal.value {
            distill_rpc::PathResolveResult::Resolved(uuid) => PathResolveResult::Resolved(uuid),
            distill_rpc::PathResolveResult::Missing => PathResolveResult::Missing,
            distill_rpc::PathResolveResult::Failed(error) => PathResolveResult::Failed {
                error: format!("{error:?}"),
            },
        };
        self.events.push(IoEvent::PathResolved {
            req,
            path: path.to_owned(),
            result,
            basis: event_basis,
        });
    }

    fn subscribe(&mut self, uuid: AssetUuid) {
        if self.subscribed_assets.insert(uuid) {
            self.install_subscription(self.hub.subscribe(
                self.snapshot.stamp().version,
                vec![uuid],
                Vec::new(),
            ));
        }
    }

    fn unsubscribe(&mut self, uuid: AssetUuid) {
        if self.subscribed_assets.remove(&uuid) {
            if let Err(failure) = rpc_result(self.hub.unsubscribe(vec![uuid], Vec::new())) {
                self.connection_failure(failure);
            }
        }
    }

    fn subscribe_path(&mut self, path: &str) {
        if self.subscribed_paths.insert(path.to_owned()) {
            self.install_subscription(self.hub.subscribe(
                self.snapshot.stamp().version,
                Vec::new(),
                vec![path.to_owned()],
            ));
        }
    }

    fn unsubscribe_path(&mut self, path: &str) {
        if self.subscribed_paths.remove(path) {
            if let Err(failure) =
                rpc_result(self.hub.unsubscribe(Vec::new(), vec![path.to_owned()]))
            {
                self.connection_failure(failure);
            }
        }
    }

    fn poll(&mut self) -> Vec<IoEvent> {
        self.drain_deltas();
        std::mem::take(&mut self.events)
    }
}

enum RpcIoFailure {
    Reconnect(ReconnectReason),
    Message(String),
}

fn rpc_result<T>(result: RpcResult<T>) -> Result<T, RpcIoFailure> {
    match result {
        RpcResult::Success(value) => Ok(value),
        RpcResult::ReconnectRequired { reason } => {
            Err(RpcIoFailure::Reconnect(reconnect_reason(reason)))
        }
        RpcResult::AttestationExpansionRequired(expansion) => Err(RpcIoFailure::Message(format!(
            "RPC attestation expansion required for {:?}",
            expansion.required
        ))),
        RpcResult::ConfigurationPoisoned(poison) => Err(RpcIoFailure::Message(format!(
            "daemon configuration poisoned: {}",
            poison.message
        ))),
        RpcResult::VersionPoisoned(poison) => Err(RpcIoFailure::Message(format!(
            "daemon version poisoned: {}",
            poison.message
        ))),
        RpcResult::Failure(error) => Err(RpcIoFailure::Message(rpc_failure(error))),
    }
}

fn init_failure<T>(result: RpcResult<T>) -> RpcIoInitError {
    match rpc_result(result) {
        Ok(_) => unreachable!("only non-success results are passed to init_failure"),
        Err(RpcIoFailure::Reconnect(reason)) => RpcIoInitError::ReconnectRequired(reason),
        Err(RpcIoFailure::Message(message)) => RpcIoInitError::Unavailable(message),
    }
}

fn io_basis(basis: &RpcBasis) -> Result<IoBasis, LoadPolicyError> {
    let rows = basis
        .load_policy
        .rows
        .iter()
        .map(|row| LoadPolicyRow {
            type_uuid: row.type_uuid,
            build_only: row.build_only,
        })
        .collect();
    let load_policy = Arc::new(LoadPolicyAttestation::try_from_parts(
        rows,
        basis.load_policy.digest,
    )?);
    Ok(IoBasis::Rpc {
        snapshot: basis.snapshot,
        load_policy,
        daemon_compiled_projection: basis.daemon_compiled_projection,
        policy_generation: basis.policy_generation,
        target_generation: basis.target_generation,
        attestation_generation: basis.attestation_generation,
    })
}

fn collect_chunks(mut stream: distill_rpc::ChunkStream) -> Result<(Vec<u8>, Vec<Vec<u8>>), String> {
    let mut structural = Vec::new();
    let mut blobs = std::collections::BTreeMap::<u32, Vec<u8>>::new();
    while let Some(chunk) = stream.next_chunk() {
        let output = match chunk.kind {
            ArtifactChunkKind::Structural => &mut structural,
            ArtifactChunkKind::Blob { index } => blobs.entry(index).or_default(),
        };
        if chunk.offset != output.len() as u64 {
            return Err("artifact chunks are not contiguous".into());
        }
        output.extend_from_slice(&chunk.bytes);
    }
    if blobs.keys().copied().ne(0..blobs.len() as u32) {
        return Err("artifact blob chunk indices are not contiguous".into());
    }
    Ok((structural, blobs.into_values().collect()))
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

fn drifted_input(input: distill_rpc::DriftedInput) -> DriftedInput {
    match input {
        distill_rpc::DriftedInput::File(path) => DriftedInput::File(path),
        distill_rpc::DriftedInput::Asset(asset) => DriftedInput::Asset(asset),
        distill_rpc::DriftedInput::Query(query) => DriftedInput::Query(query),
        distill_rpc::DriftedInput::Dylib => DriftedInput::Dylib,
        distill_rpc::DriftedInput::Tool(tool) => DriftedInput::Tool(tool),
    }
}

fn rpc_failure(error: RpcFailure) -> String {
    format!("{error:?}")
}
