//! Production one-shot pack construction through the daemon's own schema,
//! pipeline, lazy-build, snapshot, and CAS authorities.

use std::fmt;
use std::path::Path;

use distill_core::bootstrap::{bootstrap_control_logical_registry_v1, PACK_DEFINITION_TYPE_UUID};
use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_pack::builder::{
    build_publish_and_activate_pack, decode_pack_definition, encoder_identity, PackBuildError,
    PackBuildOutput, PackBuildTarget,
};
use distill_rpc::{
    AuthoringEntryRole, AuthoringInspectResult, ConnectOutcome, ConnectRequest, MetadataCall,
    MetadataConnectOutcome, MetadataNamespaceCall, RpcResult, SnapshotStamp, TargetDefinitionHash,
    PROTOCOL_VERSION,
};

use crate::config::DaemonConfig;
use crate::process::{DaemonProcess, DaemonProcessError};

const MAX_BASIS_RETRIES: usize = 32;

#[derive(Debug)]
pub enum PackCommandError {
    Daemon(DaemonProcessError),
    Bootstrap(String),
    Metadata(String),
    MissingDefinition(AssetUuid),
    IneligibleDefinition {
        asset: AssetUuid,
        observed: AuthoringEntryRole,
    },
    WrongDefinitionType {
        expected: TypeUuid,
        observed: TypeUuid,
    },
    WrongDefinitionSchema {
        expected: LogicalHash,
        observed: LogicalHash,
    },
    UnknownTarget(String),
    Connect(String),
    UnstableBasis,
    Build(PackBuildError),
}

impl fmt::Display for PackCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "pack command failed: {self:?}")
    }
}

impl std::error::Error for PackCommandError {}

impl From<DaemonProcessError> for PackCommandError {
    fn from(value: DaemonProcessError) -> Self {
        Self::Daemon(value)
    }
}

impl From<PackBuildError> for PackCommandError {
    fn from(value: PackBuildError) -> Self {
        Self::Build(value)
    }
}

pub fn build_configured_pack(
    config: DaemonConfig,
    definition_asset: AssetUuid,
    destination: &Path,
) -> Result<PackBuildOutput, PackCommandError> {
    let process = DaemonProcess::start_for_pack(config, destination)?;
    build_pack_from_process(&process, definition_asset, destination)
}

fn build_pack_from_process(
    process: &DaemonProcess,
    definition_asset: AssetUuid,
    destination: &Path,
) -> Result<PackBuildOutput, PackCommandError> {
    let coordinator = process.coordinator();
    let root = coordinator.server().root();
    let metadata_hub = match root.metadata(PROTOCOL_VERSION) {
        MetadataConnectOutcome::Connected(connected) => connected.hub,
        outcome => {
            return Err(PackCommandError::Metadata(format!(
                "metadata bootstrap rejected: {outcome:?}"
            )))
        }
    };
    let expected_schema = bootstrap_control_logical_registry_v1()
        .map_err(|error| PackCommandError::Bootstrap(error.to_string()))?
        .get(&PACK_DEFINITION_TYPE_UUID)
        .copied()
        .ok_or_else(|| PackCommandError::Bootstrap("PackDefinition row is absent".to_owned()))?;

    for _ in 0..MAX_BASIS_RETRIES {
        let (definition_stamp, definition) =
            inspect_definition(&metadata_hub, definition_asset, expected_schema)?;
        let target = coordinator
            .build_target(&definition.target)
            .ok_or_else(|| PackCommandError::UnknownTarget(definition.target.clone()))?;
        let definition_hash = distill_build::keys::target_definition_hash(&target);
        let hub = match root.connect(ConnectRequest::new(
            &definition.target,
            TargetDefinitionHash(definition_hash),
        )) {
            ConnectOutcome::Connected(connected) => connected.hub,
            outcome => {
                return Err(PackCommandError::Connect(format!(
                    "target connection rejected: {outcome:?}"
                )))
            }
        };
        let snapshot = rpc_value(hub.snapshot(), "pin runtime snapshot")?;
        if snapshot.stamp() != definition_stamp {
            continue;
        }
        return build_publish_and_activate_pack(
            destination,
            &definition,
            &PackBuildTarget {
                name: definition.target.clone(),
                definition_hash,
            },
            &encoder_identity(),
            &snapshot,
            &hub,
        )
        .map_err(Into::into);
    }
    Err(PackCommandError::UnstableBasis)
}

fn inspect_definition(
    hub: &distill_rpc::MetadataHub,
    asset: AssetUuid,
    expected_schema: LogicalHash,
) -> Result<
    (
        SnapshotStamp,
        distill_build::trace::PackDefinitionControlValue,
    ),
    PackCommandError,
> {
    let snapshot = metadata_value(hub.authoring_snapshot(), "pin authoring snapshot")?;
    let inspection = metadata_namespace_value(snapshot.inspect(asset), "inspect PackDefinition")?;
    let inspection = match inspection {
        AuthoringInspectResult::Inspection(inspection) => inspection,
        AuthoringInspectResult::Missing => return Err(PackCommandError::MissingDefinition(asset)),
        AuthoringInspectResult::RoleIneligible { observed } => {
            return Err(PackCommandError::IneligibleDefinition { asset, observed })
        }
    };
    if inspection.role != AuthoringEntryRole::AuthoringOnly {
        return Err(PackCommandError::IneligibleDefinition {
            asset,
            observed: inspection.role,
        });
    }
    if inspection.type_uuid != PACK_DEFINITION_TYPE_UUID {
        return Err(PackCommandError::WrongDefinitionType {
            expected: PACK_DEFINITION_TYPE_UUID,
            observed: inspection.type_uuid,
        });
    }
    if inspection.schema_hash != expected_schema {
        return Err(PackCommandError::WrongDefinitionSchema {
            expected: expected_schema,
            observed: inspection.schema_hash,
        });
    }
    let definition = decode_pack_definition(&inspection.value)?;
    Ok((inspection.stamp, definition))
}

fn metadata_value<T>(result: MetadataCall<T>, operation: &str) -> Result<T, PackCommandError> {
    match result {
        MetadataCall::Success(value) => Ok(value),
        MetadataCall::ReconnectRequired { reason } => Err(PackCommandError::Metadata(format!(
            "{operation}: reconnect required: {reason:?}"
        ))),
        MetadataCall::LeaseFailure => Err(PackCommandError::Metadata(format!(
            "{operation}: snapshot lease expired"
        ))),
        MetadataCall::Error(error) => Err(PackCommandError::Metadata(format!(
            "{operation}: {error:?}"
        ))),
    }
}

fn metadata_namespace_value<T>(
    result: MetadataNamespaceCall<T>,
    operation: &str,
) -> Result<T, PackCommandError> {
    match result {
        MetadataNamespaceCall::Success(value) => Ok(value),
        MetadataNamespaceCall::ReconnectRequired { reason } => Err(PackCommandError::Metadata(
            format!("{operation}: reconnect required: {reason:?}"),
        )),
        MetadataNamespaceCall::LeaseFailure => Err(PackCommandError::Metadata(format!(
            "{operation}: snapshot lease expired"
        ))),
        MetadataNamespaceCall::Error(error) => Err(PackCommandError::Metadata(format!(
            "{operation}: {error:?}"
        ))),
    }
}

fn rpc_value<T>(result: RpcResult<T>, operation: &str) -> Result<T, PackCommandError> {
    match result {
        RpcResult::Success(value) => Ok(value),
        RpcResult::ReconnectRequired { reason } => Err(PackCommandError::Connect(format!(
            "{operation}: reconnect required: {reason:?}"
        ))),
        RpcResult::ConfigurationPoisoned(poison) => Err(PackCommandError::Connect(format!(
            "{operation}: configuration poisoned: {poison:?}"
        ))),
        RpcResult::Failure(error) => {
            Err(PackCommandError::Connect(format!("{operation}: {error:?}")))
        }
    }
}
