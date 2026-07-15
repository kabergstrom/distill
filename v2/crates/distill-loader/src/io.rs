//! The sole runtime IO boundary: RPC in development or pack files in shipping.

use std::sync::Arc;

use distill_build::trace::EntryRole;
use distill_core::attestation::CompiledTypeTable;
use distill_core::id::{AssetUuid, ContentHash};
use distill_store::state::SnapshotStamp;
use distill_wire::exec::Blob;

use crate::basis::IoBasis;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReqId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAttestation {
    pub epoch: crate::GameModuleEpoch,
    pub target_definition_hash: [u8; 32],
    pub compiled_types: CompiledTypeTable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeAttestationError {
    Compiled(distill_core::attestation::AttestationError),
    Rpc(distill_rpc::AttestationShapeError),
}

impl std::fmt::Display for RuntimeAttestationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "runtime attestation: {self:?}")
    }
}

impl std::error::Error for RuntimeAttestationError {}

impl RuntimeAttestation {
    pub fn from_descriptors(
        epoch: crate::GameModuleEpoch,
        target_definition_hash: [u8; 32],
        descriptors: &[&'static distill_asset::AssetRuntimeDescriptor],
    ) -> Result<Self, RuntimeAttestationError> {
        let rows = descriptors
            .iter()
            .map(|descriptor| (*descriptor.compiled_type).clone())
            .collect();
        let compiled_types =
            CompiledTypeTable::canonical(rows).map_err(RuntimeAttestationError::Compiled)?;
        Ok(Self {
            epoch,
            target_definition_hash,
            compiled_types,
        })
    }

    pub fn connect_request(
        &self,
        target: &str,
    ) -> Result<distill_rpc::ConnectRequest, RuntimeAttestationError> {
        let policy = self
            .compiled_types
            .rows
            .iter()
            .map(|row| distill_rpc::LoadPolicyEntry {
                type_uuid: row.type_uuid,
                build_only: row.build_only,
            })
            .collect();
        distill_rpc::ConnectRequest::canonical(
            distill_rpc::GameModuleEpoch(self.epoch.0),
            target,
            distill_rpc::TargetDefinitionHash(self.target_definition_hash),
            self.compiled_types.rows.clone(),
            policy,
        )
        .map_err(RuntimeAttestationError::Rpc)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftedInput {
    File(String),
    Asset(AssetUuid),
    Query(String),
    Dylib,
    Tool(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveResult {
    Built {
        content_hash: ContentHash,
    },
    Drifted {
        input: DriftedInput,
        current: SnapshotStamp,
    },
    Failed {
        error: String,
    },
    RoleIneligible {
        uuid: AssetUuid,
        role: EntryRole,
    },
    Missing,
    Deleted {
        at: SnapshotStamp,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathResolveResult {
    Resolved(AssetUuid),
    Missing,
    Unsupported,
    Failed { error: String },
}

#[derive(Debug, Clone)]
pub struct FetchedArtifact {
    pub structural: Arc<[u8]>,
    pub blobs: Vec<Blob>,
    /// Canonical DSWL body authenticated by the artifact header's
    /// `layout_hash`; LoaderIO resolves this before completing the fetch.
    pub wire_layout: Arc<[u8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetDeltaState {
    Changed,
    Deleted,
    Restored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectReason {
    TargetDefinitionChanged,
    LoadPolicyChanged,
    CompiledAttestationChanged,
    StoreInstanceChanged,
    ProtocolEpochChanged,
}

#[derive(Debug, Clone)]
pub enum IoEvent {
    Resolved {
        req: ReqId,
        uuid: AssetUuid,
        result: ResolveResult,
        basis: IoBasis,
    },
    PathResolved {
        req: ReqId,
        path: String,
        result: PathResolveResult,
        basis: IoBasis,
    },
    Fetched {
        req: ReqId,
        content_hash: ContentHash,
        artifact: FetchedArtifact,
        basis: IoBasis,
    },
    Delta {
        stamp: SnapshotStamp,
        assets: Vec<(AssetUuid, AssetDeltaState)>,
        paths: Vec<String>,
    },
    RequestError {
        req: ReqId,
        message: String,
        basis: IoBasis,
    },
    ConnectionError {
        message: String,
    },
    ReconnectRequired {
        reason: ReconnectReason,
    },
    Reattested {
        attestation: RuntimeAttestation,
        basis: IoBasis,
    },
    ReattestationFailed {
        message: String,
    },
}

pub trait LoaderIO {
    fn reattest(&mut self, attestation: RuntimeAttestation);
    fn begin_sweep(&mut self) -> IoBasis;
    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis);
    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis);
    fn resolve_path(&mut self, req: ReqId, path: &str, basis: &IoBasis);
    fn subscribe(&mut self, uuid: AssetUuid);
    fn unsubscribe(&mut self, uuid: AssetUuid);
    fn subscribe_path(&mut self, path: &str);
    fn unsubscribe_path(&mut self, path: &str);
    fn poll(&mut self) -> Vec<IoEvent>;
}
