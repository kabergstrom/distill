//! `distilld pack`: build a pack through a running daemon's RPC.
//!
//! The daemon owns the state directory, so the command is a client of it:
//! it inspects the PackDefinition on the metadata hub, connects to the
//! definition's target and builds from one pinned snapshot there.

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use distill_core::bootstrap::{bootstrap_control_logical_registry_v1, PACK_DEFINITION_TYPE_UUID};
use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_pack::builder::{
    build_publish_and_activate_pack, decode_pack_definition, encoder_identity, PackBuildError,
    PackBuildOutput, PackBuildTarget,
};
use distill_rpc::capnp_loader::{
    RemoteCall, RemoteHub, RemoteMetadataAuthoringSnapshot, RemoteMetadataHub,
};
use distill_rpc::capnp_transport::{CapnpClient, RemoteMetadataOutcome};
use distill_rpc::{
    AuthoringEntryRole, AuthoringInspectResult, ConnectRequest, SnapshotStamp,
    TargetDefinitionHash, PROTOCOL_VERSION,
};

use crate::bootstrap;
use crate::config::DaemonConfig;

const MAX_BASIS_RETRIES: usize = 32;

#[derive(Debug)]
pub enum PackCommandError {
    /// Nothing answered at the configured daemon address.
    NoDaemon {
        address: SocketAddr,
        error: String,
    },
    Config(String),
    /// The output directory is missing or not a directory.
    OutputDirectory {
        path: PathBuf,
        error: std::io::Error,
    },
    /// The output directory lies inside an asset root. The running daemon
    /// does not own it, so it would scan the pack files as assets.
    OutputInsideAssetRoot {
        output: PathBuf,
        root: String,
    },
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
    Connect(String),
    UnstableBasis,
    Build(PackBuildError),
}

impl fmt::Display for PackCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDaemon { address, error } => {
                write!(formatter, "no distilld at {address}; start it first ({error})")
            }
            Self::OutputInsideAssetRoot { output, root } => write!(
                formatter,
                "pack output {} is inside asset root `{root}`; \
                 use a directory outside every asset root",
                output.display()
            ),
            Self::OutputDirectory { path, error } => {
                write!(formatter, "pack output {}: {error}", path.display())
            }
            other => write!(formatter, "pack command failed: {other:?}"),
        }
    }
}

impl std::error::Error for PackCommandError {}

impl From<PackBuildError> for PackCommandError {
    fn from(value: PackBuildError) -> Self {
        Self::Build(value)
    }
}

/// Build, publish and activate the pack `definition_asset` describes into
/// `destination`, through the daemon serving `config`.
pub fn build_configured_pack(
    config: &DaemonConfig,
    definition_asset: AssetUuid,
    destination: &Path,
) -> Result<PackBuildOutput, PackCommandError> {
    check_output_directory(config, destination)?;
    let address = config.daemon.address;
    if address.port() == 0 {
        return Err(PackCommandError::Config(
            "daemon.address needs a fixed port for pack to find the daemon".into(),
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| PackCommandError::Connect(error.to_string()))?;
    let local = tokio::task::LocalSet::new();
    local.block_on(
        &runtime,
        build_pack_at(config, address, definition_asset, destination),
    )
}

/// The daemon does not own the output directory, so it must not scan it:
/// the directory has to exist outside every asset root.
fn check_output_directory(
    config: &DaemonConfig,
    destination: &Path,
) -> Result<(), PackCommandError> {
    let output_error = |error| PackCommandError::OutputDirectory {
        path: destination.to_path_buf(),
        error,
    };
    let output = std::fs::canonicalize(destination).map_err(output_error)?;
    if !output.is_dir() {
        return Err(output_error(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            "not a directory",
        )));
    }
    for (name, root) in &config.assets.roots {
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.clone());
        if output.starts_with(&root) {
            return Err(PackCommandError::OutputInsideAssetRoot {
                output,
                root: name.clone(),
            });
        }
    }
    Ok(())
}

async fn build_pack_at(
    config: &DaemonConfig,
    address: SocketAddr,
    definition_asset: AssetUuid,
    destination: &Path,
) -> Result<PackBuildOutput, PackCommandError> {
    let client = CapnpClient::connect_local(address)
        .await
        .map_err(|error| PackCommandError::NoDaemon {
            address,
            error: error.to_string(),
        })?;
    let metadata = client
        .metadata(PROTOCOL_VERSION)
        .await
        .map_err(|error| PackCommandError::Metadata(error.to_string()))?;
    let metadata = RemoteMetadataHub::connected(metadata).map_err(|outcome| {
        PackCommandError::Metadata(match *outcome {
            RemoteMetadataOutcome::ProtocolFailure {
                expected,
                observed,
                message,
            } => format!("daemon speaks protocol {expected}, this distilld {observed}: {message}"),
            RemoteMetadataOutcome::Error { code, message } => format!("{message} ({code})"),
            RemoteMetadataOutcome::Connected { .. } => unreachable!("connected is Ok"),
        })
    })?;
    let expected_schema = bootstrap_control_logical_registry_v1()
        .map_err(|error| PackCommandError::Bootstrap(error.to_string()))?
        .get(&PACK_DEFINITION_TYPE_UUID)
        .copied()
        .ok_or_else(|| PackCommandError::Bootstrap("PackDefinition row is absent".to_owned()))?;
    let authority = bootstrap::load_authority(config)
        .map_err(|error| PackCommandError::Config(error.to_string()))?;

    for _ in 0..MAX_BASIS_RETRIES {
        let authoring = remote(metadata.authoring_snapshot().await, "pin authoring snapshot")?;
        // A definition file that changed since the snapshot is read at the
        // next one.
        let Some((definition_stamp, definition)) =
            inspect_definition(&authoring, definition_asset, expected_schema).await?
        else {
            continue;
        };
        let definition_hash = bootstrap::target_hash(config, &authority, &definition.target)
            .map_err(|error| PackCommandError::Config(error.to_string()))?;
        let outcome = client
            .connect(&ConnectRequest::new(
                &definition.target,
                TargetDefinitionHash(definition_hash),
            ))
            .await
            .map_err(|error| PackCommandError::Connect(error.to_string()))?;
        let hub = RemoteHub::connected(outcome).map_err(|outcome| {
            PackCommandError::Connect(format!("target connection rejected: {outcome:?}"))
        })?;
        let snapshot = remote(hub.snapshot().await, "pin runtime snapshot")?;
        if snapshot.basis().snapshot != definition_stamp {
            continue;
        }
        match build_publish_and_activate_pack(
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
        .await
        {
            // An artifact that left the CAS, or a snapshot that expired,
            // mid-build is a cache miss: build again at a new snapshot.
            Err(error) if error.is_cache_miss() => continue,
            result => return result.map_err(Into::into),
        }
    }
    Err(PackCommandError::UnstableBasis)
}

async fn inspect_definition(
    snapshot: &RemoteMetadataAuthoringSnapshot,
    asset: AssetUuid,
    expected_schema: LogicalHash,
) -> Result<
    Option<(
        SnapshotStamp,
        distill_build::trace::PackDefinitionControlValue,
    )>,
    PackCommandError,
> {
    let inspection = match remote(snapshot.inspect(asset).await, "inspect PackDefinition")? {
        AuthoringInspectResult::Inspection(inspection) => inspection,
        AuthoringInspectResult::Missing => return Err(PackCommandError::MissingDefinition(asset)),
        AuthoringInspectResult::RoleIneligible { observed } => {
            return Err(PackCommandError::IneligibleDefinition { asset, observed })
        }
        AuthoringInspectResult::Drifted { .. } => return Ok(None),
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
    Ok(Some((inspection.stamp, definition)))
}

fn remote<T: fmt::Debug, E: fmt::Display>(
    result: Result<RemoteCall<T>, E>,
    operation: &str,
) -> Result<T, PackCommandError> {
    match result {
        Ok(RemoteCall::Success(value)) => Ok(value),
        Ok(other) => Err(PackCommandError::Connect(format!("{operation}: {other:?}"))),
        Err(error) => Err(PackCommandError::Connect(format!("{operation}: {error}"))),
    }
}
