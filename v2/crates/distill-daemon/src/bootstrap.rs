//! Project bootstrap behind `distilld init`, `distilld import` and
//! `distilld engine-args`, shared with the tests.
//!
//! - [`init`] writes the control bundles an asset root needs before the
//!   daemon can publish against its project schema: the schema-lineage
//!   manifest (one accepted epoch per project type, at its current logical
//!   hash) and the schema seed (one authoring-only entry per project type,
//!   holding the type's zero value, which puts every logical schema in the
//!   namespace).
//! - [`import`] runs an authoring import through a running daemon's RPC hub.
//! - [`engine_args`] prints what a game needs to connect to a target.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use distill_build::keys::target_definition_hash;
use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::bootstrap::{BootstrapControlSpecV1, BootstrapControlSymbol};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_json::AuthoredValue;
use distill_rpc::capnp_loader::{RemoteCall, RemoteHub};
use distill_rpc::capnp_transport::{CapnpClient, RemoteConnectOutcome};
use distill_rpc::{ConnectRequest, ImportRequest, TargetDefinitionHash};
use distill_schema::ngp_schema::{PrimitiveKind, SchemaNode};
use distill_schema::ProjectSchemaAuthority;
use distill_store::state::InputVersion;

use crate::config::DaemonConfig;
use crate::coordinator::LineageDestination;

/// The schema seed's file name. It sits beside the lineage manifest.
pub const SCHEMA_SEED_FILE: &str = "project-schema-cache.bundle";

#[derive(Debug)]
pub enum BootstrapError {
    Io { path: PathBuf, source: std::io::Error },
    Schema(String),
    Bundle(String),
    /// A project type whose value cannot be empty (a required asset
    /// reference, or a required recursive field).
    NoZeroValue { type_uuid: TypeUuid, detail: String },
    /// The manifest on disk has schema-transition history for this type,
    /// which a regenerated single-epoch manifest would drop.
    ManifestHistory(TypeUuid),
    Config(String),
    Rpc(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "bootstrap: {self:?}")
    }
}

impl std::error::Error for BootstrapError {}

/// What [`init`] wrote; files whose bytes already matched are `unchanged`.
#[derive(Debug, Default)]
pub struct InitReport {
    pub written: Vec<PathBuf>,
    pub unchanged: Vec<PathBuf>,
}

/// The project schema authority for the schema file `config` names.
pub fn load_authority(config: &DaemonConfig) -> Result<ProjectSchemaAuthority, BootstrapError> {
    let path = &config.assets.schema_path;
    let bytes = std::fs::read(path).map_err(|source| BootstrapError::Io {
        path: path.clone(),
        source,
    })?;
    ProjectSchemaAuthority::from_json(&bytes)
        .map_err(|error| BootstrapError::Schema(format!("{}: {error}", path.display())))
}

/// Write the lineage manifest and schema seed for `config`'s schema. Only
/// changed bytes are written: identical rewrites would still wake a running
/// daemon's watcher.
pub fn init(config: &DaemonConfig) -> Result<InitReport, BootstrapError> {
    let authority = load_authority(config)?;
    let destination = &config.assets.lineage_manifest;
    let root = config.assets.roots.get(&destination.root).ok_or_else(|| {
        BootstrapError::Config(format!("unknown lineage root {}", destination.root))
    })?;
    let manifest_path = root.join(&destination.path);
    let seed_path = manifest_path.with_file_name(SCHEMA_SEED_FILE);
    refuse_manifest_history(&manifest_path)?;
    let manifest = lineage_manifest_bundle(&authority, destination)?;
    let seed = schema_seed_bundle(&authority, destination)?;
    let mut report = InitReport::default();
    for (path, bundle) in [(manifest_path, manifest), (seed_path, seed)] {
        let bytes = distill_bundle::write_bundle(&bundle)
            .map_err(|error| BootstrapError::Bundle(format!("{}: {error:?}", path.display())))?;
        if write_if_changed(&path, &bytes)? {
            report.written.push(path);
        } else {
            report.unchanged.push(path);
        }
    }
    Ok(report)
}

/// The schema-lineage manifest: every project type active at one epoch, its
/// current logical hash.
pub fn lineage_manifest_bundle(
    authority: &ProjectSchemaAuthority,
    destination: &LineageDestination,
) -> Result<Bundle, BootstrapError> {
    let row = BootstrapControlSpecV1::embedded()
        .map_err(|error| BootstrapError::Schema(error.to_string()))?
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::SchemaLineageManifest)
        .ok_or_else(|| BootstrapError::Schema("no lineage manifest control row".into()))?;
    let schema = distill_schema::ngp_schema::node_from_bytes(&row.logical_schema)
        .map_err(|error| BootstrapError::Schema(error.to_string()))?;
    let types = authority
        .project_types()
        .iter()
        .map(|(type_uuid, project)| {
            AuthoredValue::Array(vec![
                bytes(&type_uuid.0),
                AuthoredValue::Object(BTreeMap::from([
                    (
                        "authority".into(),
                        AuthoredValue::Object(BTreeMap::from([(
                            "Active".into(),
                            AuthoredValue::Object(BTreeMap::new()),
                        )])),
                    ),
                    ("current".into(), AuthoredValue::UInt(0)),
                    (
                        "epochs".into(),
                        AuthoredValue::Array(vec![AuthoredValue::Object(BTreeMap::from([
                            ("digest".into(), bytes(&project.logical_hash.0)),
                            ("forward_parent".into(), AuthoredValue::Null),
                        ]))]),
                    ),
                ])),
            ])
        })
        .collect();
    Ok(Bundle {
        format_version: 1,
        uuid: BundleUuid(derived_id("lineage manifest bundle", destination, &[])),
        primary: None,
        schemas: BTreeMap::from([(row.logical_hash, schema)]),
        assets: BTreeMap::from([(
            "manifest".into(),
            AssetEntry {
                uuid: AssetUuid(derived_id("lineage manifest", destination, &[])),
                type_uuid: row.type_uuid,
                schema_hash: row.logical_hash,
                lineage: EntryLineageV1::Bootstrap {
                    bundle_format_version: 1,
                },
                authoring_only: true,
                data: AuthoredValue::Object(BTreeMap::from([(
                    "types".into(),
                    AuthoredValue::Array(types),
                )])),
            },
        )]),
    })
}

/// The schema seed: one authoring-only zero value per project type, stamped
/// at the manifest's single epoch.
pub fn schema_seed_bundle(
    authority: &ProjectSchemaAuthority,
    destination: &LineageDestination,
) -> Result<Bundle, BootstrapError> {
    let mut schemas = BTreeMap::new();
    let mut entries = BTreeMap::new();
    for (type_uuid, project) in authority.project_types() {
        schemas.insert(project.logical_hash, project.logical_schema.clone());
        let epochs = vec![AcceptedSchemaEpoch {
            digest: project.logical_hash,
            forward_parent: None,
        }];
        let data = zero_value(&project.logical_schema.root).map_err(|detail| {
            BootstrapError::NoZeroValue {
                type_uuid: *type_uuid,
                detail,
            }
        })?;
        entries.insert(
            format!("schema-{}", hex(&type_uuid.0)),
            AssetEntry {
                uuid: AssetUuid(derived_id("schema seed", destination, &type_uuid.0)),
                type_uuid: *type_uuid,
                schema_hash: project.logical_hash,
                lineage: EntryLineageV1::Manifest(LineageStamp {
                    chain: lineage_chain_digest(*type_uuid, &epochs, 0),
                    epochs,
                    cursor: 0,
                }),
                authoring_only: true,
                data,
            },
        );
    }
    Ok(Bundle {
        format_version: 1,
        uuid: BundleUuid(derived_id("schema seed bundle", destination, &[])),
        primary: None,
        schemas,
        assets: entries,
    })
}

/// The empty value of a logical schema: zero numbers, empty strings and
/// containers, `None`, the first variant of an enum. Fails for what has no
/// empty value (a required asset reference or recursive field).
pub fn zero_value(node: &SchemaNode) -> Result<AuthoredValue, String> {
    Ok(match node {
        SchemaNode::Primitive(kind) => match kind {
            PrimitiveKind::Bool => AuthoredValue::Bool(false),
            PrimitiveKind::F32 | PrimitiveKind::F64 => AuthoredValue::Float(0.0),
            PrimitiveKind::Char => AuthoredValue::Str("\0".into()),
            _ => AuthoredValue::UInt(0),
        },
        SchemaNode::Struct { fields, .. } => AuthoredValue::Object(
            fields
                .iter()
                .map(|(name, _, field)| {
                    zero_value(field)
                        .map(|value| (name.clone(), value))
                        .map_err(|detail| format!("{name}: {detail}"))
                })
                .collect::<Result<_, _>>()?,
        ),
        SchemaNode::Enum { variants, .. } => {
            let (name, _, payload) = variants.first().ok_or("an enum without variants")?;
            let payload = zero_value(payload).map_err(|detail| format!("{name}: {detail}"))?;
            AuthoredValue::Object(BTreeMap::from([(name.clone(), payload)]))
        }
        SchemaNode::Vec(_) | SchemaNode::Set(_) => AuthoredValue::Array(Vec::new()),
        SchemaNode::Map { key, .. } if matches!(**key, SchemaNode::String) => {
            AuthoredValue::Object(BTreeMap::new())
        }
        SchemaNode::Map { .. } => AuthoredValue::Array(Vec::new()),
        SchemaNode::Array { len, elem } => {
            let elem = zero_value(elem)?;
            AuthoredValue::Array(vec![elem; *len as usize])
        }
        SchemaNode::Option(_) | SchemaNode::Unit => AuthoredValue::Null,
        SchemaNode::String => AuthoredValue::Str(String::new()),
        SchemaNode::Blob => AuthoredValue::Blob(Vec::new()),
        SchemaNode::AssetRef(_) | SchemaNode::WeakRef(_) => {
            return Err("a required asset reference".into())
        }
        SchemaNode::BackRef(_) => return Err("a required recursive field".into()),
    })
}

/// A regenerated manifest has one epoch per type. Refuse to replace one that
/// recorded schema transitions.
fn refuse_manifest_history(path: &Path) -> Result<(), BootstrapError> {
    let Ok(bytes) = std::fs::read(path) else {
        return Ok(());
    };
    let Ok(bundle) = distill_bundle::parse_bundle(&bytes) else {
        return Ok(());
    };
    for entry in bundle.assets.values() {
        let AuthoredValue::Object(data) = &entry.data else {
            continue;
        };
        let Some(AuthoredValue::Array(types)) = data.get("types") else {
            continue;
        };
        for row in types {
            let AuthoredValue::Array(pair) = row else {
                continue;
            };
            let [uuid, AuthoredValue::Object(lineage)] = pair.as_slice() else {
                continue;
            };
            if matches!(lineage.get("epochs"), Some(AuthoredValue::Array(epochs)) if epochs.len() > 1)
            {
                return Err(BootstrapError::ManifestHistory(TypeUuid(uuid_bytes(uuid))));
            }
        }
    }
    Ok(())
}

/// Write `bytes` to `path` through a sibling temporary file unless it
/// already holds them. Returns whether it wrote.
pub fn write_if_changed(path: &Path, bytes: &[u8]) -> Result<bool, BootstrapError> {
    if std::fs::read(path).ok().as_deref() == Some(bytes) {
        return Ok(false);
    }
    let io = |source| BootstrapError::Io {
        path: path.to_path_buf(),
        source,
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(io)?;
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let temp = parent.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let result = std::fs::File::create(&temp)
        .and_then(|mut file| {
            file.write_all(bytes)?;
            file.sync_all()
        })
        .and_then(|()| std::fs::rename(&temp, path));
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result.map_err(io)?;
    Ok(true)
}

/// The target a command addresses: the named one, else the only (or first)
/// configured target.
fn target_name<'a>(config: &'a DaemonConfig, target: Option<&'a str>) -> Result<&'a str, BootstrapError> {
    match target {
        Some(name) if config.targets.contains_key(name) => Ok(name),
        Some(name) => Err(BootstrapError::Config(format!("unknown target {name}"))),
        None => config
            .targets
            .keys()
            .next()
            .map(String::as_str)
            .ok_or_else(|| BootstrapError::Config("no targets configured".into())),
    }
}

fn target_hash(
    config: &DaemonConfig,
    authority: &ProjectSchemaAuthority,
    target: &str,
) -> Result<[u8; 32], BootstrapError> {
    let targets = config
        .build_targets(authority.identity())
        .map_err(|error| BootstrapError::Config(error.to_string()))?;
    let target = targets
        .get(target)
        .ok_or_else(|| BootstrapError::Config(format!("unknown target {target}")))?;
    Ok(target_definition_hash(target))
}

/// The engine's `--distill-rpc`, `--distill-target` and
/// `--distill-target-hash` arguments for `target` of `config`.
pub fn engine_args(
    config: &DaemonConfig,
    target: Option<&str>,
) -> Result<Vec<String>, BootstrapError> {
    if config.daemon.address.port() == 0 {
        return Err(BootstrapError::Config(
            "daemon.address needs a fixed port for a client to find it".into(),
        ));
    }
    let authority = load_authority(config)?;
    let target = target_name(config, target)?;
    let hash = target_hash(config, &authority, target)?;
    Ok(vec![
        "--distill-rpc".into(),
        config.daemon.address.to_string(),
        "--distill-target".into(),
        target.into(),
        "--distill-target-hash".into(),
        hex(&hash),
    ])
}

/// Import through the daemon serving `config` at `address`, connected as
/// `target`. Retries, for up to `wait`, while the daemon is not listening
/// yet, its pipeline is not ready, or another publication moved the base.
pub fn import(
    config: &DaemonConfig,
    address: std::net::SocketAddr,
    target: Option<&str>,
    request: &ImportRequest,
    wait: Duration,
) -> Result<BundleUuid, BootstrapError> {
    let authority = load_authority(config)?;
    let target = target_name(config, target)?;
    let connect = ConnectRequest::new(
        target,
        TargetDefinitionHash(target_hash(config, &authority, target)?),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| BootstrapError::Rpc(error.to_string()))?;
    let local = tokio::task::LocalSet::new();
    let deadline = Instant::now() + wait;
    local.block_on(&runtime, async move {
        loop {
            let error = match import_once(address, &connect, request).await {
                Ok(bundle) => return Ok(bundle),
                Err(error) => error,
            };
            if Instant::now() >= deadline {
                return Err(BootstrapError::Rpc(error.message));
            }
            if !error.retry {
                return Err(BootstrapError::Rpc(error.message));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
}

struct ImportAttemptError {
    message: String,
    retry: bool,
}

async fn import_once(
    address: std::net::SocketAddr,
    connect: &ConnectRequest,
    request: &ImportRequest,
) -> Result<BundleUuid, ImportAttemptError> {
    let fail = |retry: bool| move |message: String| ImportAttemptError { message, retry };
    let client = CapnpClient::connect_local(address)
        .await
        .map_err(|error| fail(true)(format!("connect {address}: {error:?}")))?;
    let outcome = client
        .connect(connect)
        .await
        .map_err(|error| fail(true)(error.to_string()))?;
    let hub = match RemoteHub::connected(outcome) {
        Ok(hub) => hub,
        Err(outcome) => {
            let retry = matches!(*outcome, RemoteConnectOutcome::PipelineUnavailable(_));
            return Err(fail(retry)(format!("connect: {outcome:?}")));
        }
    };
    let base = match hub.snapshot().await {
        Ok(RemoteCall::Success(snapshot)) => snapshot.basis().snapshot.version,
        Ok(other) => return Err(fail(true)(format!("snapshot: {other:?}"))),
        Err(error) => return Err(fail(true)(error.to_string())),
    };
    import_at(&hub, base, request).await
}

async fn import_at(
    hub: &RemoteHub,
    base: InputVersion,
    request: &ImportRequest,
) -> Result<BundleUuid, ImportAttemptError> {
    match hub.import(base, request).await {
        Ok(RemoteCall::Success(bundle)) => Ok(bundle),
        Ok(RemoteCall::Error(error)) => Err(ImportAttemptError {
            retry: error.message.starts_with("StaleInputVersion"),
            message: format!("import: {} ({})", error.message, error.code),
        }),
        Ok(other) => Err(ImportAttemptError {
            message: format!("import: {other:?}"),
            retry: matches!(other, RemoteCall::ReconnectRequired(_)),
        }),
        Err(error) => Err(ImportAttemptError {
            message: error.to_string(),
            retry: false,
        }),
    }
}

/// A stable identity for one bootstrap entry of one lineage destination, so
/// rerunning `init` reproduces the same bytes.
fn derived_id(label: &str, destination: &LineageDestination, salt: &[u8]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    for part in [
        b"distill bootstrap".as_slice(),
        label.as_bytes(),
        destination.root.as_bytes(),
        destination.path.as_bytes(),
        salt,
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    id
}

fn bytes(bytes: &[u8]) -> AuthoredValue {
    AuthoredValue::Array(
        bytes
            .iter()
            .map(|byte| AuthoredValue::UInt(u128::from(*byte)))
            .collect(),
    )
}

fn uuid_bytes(value: &AuthoredValue) -> [u8; 16] {
    let mut out = [0; 16];
    if let AuthoredValue::Array(items) = value {
        for (slot, item) in out.iter_mut().zip(items) {
            if let AuthoredValue::UInt(byte) = item {
                *slot = *byte as u8;
            }
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_values_conform_to_their_schema() {
        let node = SchemaNode::Struct {
            rev: 0,
            fields: vec![
                ("a".into(), 0, SchemaNode::Primitive(PrimitiveKind::F32)),
                ("b".into(), 0, SchemaNode::Primitive(PrimitiveKind::Char)),
                (
                    "c".into(),
                    0,
                    SchemaNode::Array {
                        len: 3,
                        elem: Box::new(SchemaNode::Primitive(PrimitiveKind::I16)),
                    },
                ),
                (
                    "d".into(),
                    0,
                    SchemaNode::Map {
                        key: Box::new(SchemaNode::String),
                        value: Box::new(SchemaNode::Blob),
                    },
                ),
                (
                    "e".into(),
                    0,
                    SchemaNode::Enum {
                        rev: 0,
                        variants: vec![(
                            "Empty".into(),
                            0,
                            SchemaNode::Struct {
                                rev: 0,
                                fields: Vec::new(),
                            },
                        )],
                    },
                ),
                (
                    "f".into(),
                    0,
                    SchemaNode::Option(Box::new(SchemaNode::AssetRef(TypeUuid([1; 16])))),
                ),
                ("g".into(), 0, SchemaNode::Blob),
            ],
        };
        let value = zero_value(&node).unwrap();
        distill_migrate::conforms(&value, &node).unwrap();
        assert!(zero_value(&SchemaNode::AssetRef(TypeUuid([1; 16]))).is_err());
    }
}
