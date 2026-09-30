//! Project bootstrap behind `distilld import` and `distilld engine-args`,
//! shared with the tests.
//!
//! - [`import`] runs an authoring import through a running daemon's RPC hub.
//! - [`engine_args`] prints what a game needs to connect to a target.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use distill_build::keys::target_definition_hash;
use distill_core::id::BundleUuid;
use distill_json::AuthoredValue;
use distill_rpc::capnp_loader::{RemoteCall, RemoteHub};
use distill_rpc::capnp_transport::{CapnpClient, RemoteConnectOutcome};
use distill_rpc::{ConnectRequest, ImportRequest, TargetDefinitionHash};
use distill_schema::ngp_schema::{PrimitiveKind, SchemaNode};
use distill_schema::ProjectSchemaAuthority;
use distill_store::state::InputVersion;

use crate::config::DaemonConfig;

#[derive(Debug)]
pub enum BootstrapError {
    Io { path: PathBuf, source: std::io::Error },
    Schema(String),
    Config(String),
    Rpc(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "bootstrap: {self:?}")
    }
}

impl std::error::Error for BootstrapError {}

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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use distill_core::id::TypeUuid;

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
