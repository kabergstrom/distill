//! Concrete Cap'n Proto transport adapter.
//!
//! Protocol state remains in the transport-neutral [`crate::Server`]. This
//! module validates wire widths, translates typed result unions, and drives
//! capnp-rpc.
//!
//! Server side: one [`StagedListener`] accepts connections, and each one is
//! served on a thread of its own, with its own current-thread Tokio runtime
//! and [`tokio::task::LocalSet`] running that connection's `RpcSystem`.
//! `RpcSystem` and every capability are `!Send` and stay on that thread,
//! with the connection's front end (store reader, snapshots, subscription
//! queue, and the store writer it opens on its first write): nothing a
//! connection does, a slow SQLite read or a write waiting on the write lock
//! included, waits on another connection. Writes, import publications and
//! operation completions run inline on that writer; importer runs and lazy
//! builds go to a blocking worker so the connection keeps serving. The
//! listener itself needs no `LocalSet`. Threads are bounded by
//! `max_connections` (one past it is closed unserved), exit when their
//! connection closes, and are told to close by
//! [`StagedListener::shutdown`]. Closing shuts the socket down and lets
//! capnp-rpc run its disconnect, which releases every capability the
//! connection's pending calls hold; dropping a live `RpcSystem` would leak
//! them. A panic ends only its own thread.
//!
//! Client side: [`CapnpClient`] runs its `RpcSystem` as a task on the
//! caller's `LocalSet`; callers drive it on that one thread.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::{
    AssetUuid, AuthoringEntryRole, AuthoringInspection, AuthoringRow, Authorable,
    AuthorableImporter, AuthorableType, BundleOrigin, BundleReferences, BundleUuid,
    ConfigurationError, ConnectError, ConnectRequest, ContentHash, DescribableType,
    DescribableTypesResult, InputVersion, LayoutHash, NamespaceError, NamespaceErrorV1, RpcBasis,
    RpcFailure, SnapshotStamp, SourceClaim, SourceFile, StoreInstanceId, TargetDefinitionHash,
    TypeDefault, TypeDescription, TypeMember, TypeUuid,
};

pub use crate::distill_rpc_capnp as schema;

use schema::InvalidRequest;

#[derive(Debug)]
pub enum TransportError {
    BindValidation(crate::BindStageError),
    Io(io::Error),
    Capnp(capnp::Error),
    /// The listener is serving `limit` connections, or shutting down.
    Refused {
        limit: usize,
    },
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BindValidation(error) => write!(f, "RPC bind validation failed: {error}"),
            Self::Io(error) => write!(f, "RPC I/O failed: {error}"),
            Self::Capnp(error) => write!(f, "Cap'n Proto RPC failed: {error}"),
            Self::Refused { limit } => {
                write!(f, "RPC connection refused: {limit} connections are open or the listener is stopping")
            }
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
    ConfigurationFailed(ConfigurationError),
    PipelineUnavailable(crate::PipelineUnavailableDiagnostic),
    TargetFailure(ConnectError),
    ProtocolFailure {
        expected: u32,
        observed: u32,
        message: String,
    },
    Error(RemoteError),
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
    Error(RemoteError),
}

impl fmt::Debug for RemoteConnectOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected { instance, .. } => f
                .debug_struct("Connected")
                .field("instance", instance)
                .finish_non_exhaustive(),
            Self::ConfigurationFailed(error) => {
                f.debug_tuple("ConfigurationFailed").field(error).finish()
            }
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
            Self::Error(error) => f.debug_tuple("Error").field(error).finish(),
        }
    }
}

/// A request or answer that does not decode, and why (the server answers
/// it as an `InvalidRequest`).
#[doc(hidden)]
#[derive(Debug)]
pub struct WireFailure {
    pub kind: InvalidRequest,
    pub message: String,
}

impl From<capnp::Error> for WireFailure {
    fn from(error: capnp::Error) -> Self {
        Self {
            kind: InvalidRequest::Value,
            message: error.to_string(),
        }
    }
}

#[doc(hidden)]
pub fn decode_uuid_list(
    values: capnp::data_list::Reader<'_>,
    field: &str,
) -> Result<Vec<AssetUuid>, WireFailure> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map_err(|error| WireFailure {
                    kind: InvalidRequest::Uuid,
                    message: format!("{field}[{index}] could not be read: {error}"),
                })
                .and_then(|value| decode_uuid(value, &format!("{field}[{index}]")).map(AssetUuid))
        })
        .collect()
}

#[doc(hidden)]
pub fn authoring_decode_error(error: impl fmt::Display) -> String {
    error.to_string()
}

/// A schema named by `hash`, read from its schema encoding and refused
/// unless it hashes to `hash` (protocol 33).
#[doc(hidden)]
pub fn decode_schema(hash: &[u8], bytes: &[u8], field: &str) -> Result<(crate::LogicalHash, Arc<crate::LogicalSchema>), String> {
    let hash = crate::LogicalHash(decode_hash(hash, field).map_err(|error| error.message)?);
    let schema = crate::codec::read_schema(hash, bytes).map_err(|error| format!("{field}: {error}"))?;
    Ok((hash, schema))
}

/// A carrier's value under `schema`, decoded with the types it carries
/// (protocol 33: the value encoding, blobs inside by bytes or reference).
#[doc(hidden)]
pub fn decode_authoring_value(
    schema: &crate::LogicalSchema,
    value: schema::authoring_value::Reader<'_>,
) -> Result<crate::AuthoringValue, String> {
    let types = decode_authoring_types(value.get_types().map_err(authoring_decode_error)?)?;
    crate::codec::read_value(schema, types, value.get_value().map_err(authoring_decode_error)?)
        .map_err(|error| error.to_string())
}

/// A carrier's patch of `schema`, decoded with the types it carries.
#[doc(hidden)]
pub fn decode_authoring_patch(
    schema: &crate::LogicalSchema,
    patch: schema::authoring_patch::Reader<'_>,
) -> Result<crate::AuthoringPatch, String> {
    let types = decode_authoring_types(patch.get_types().map_err(authoring_decode_error)?)?;
    crate::codec::read_patch(schema, types, patch.get_patch().map_err(authoring_decode_error)?)
        .map_err(|error| error.to_string())
}

/// Write `value`, a value of `schema`, and the types it carries.
#[doc(hidden)]
pub fn write_authoring_value(
    mut output: schema::authoring_value::Builder<'_>,
    schema: &crate::LogicalSchema,
    value: &crate::AuthoringValue,
) -> Result<(), crate::AuthoringValueError> {
    output.set_value(&crate::codec::value_bytes(schema, value)?);
    write_authoring_types(output.init_types(value.types.len() as u32), &value.types);
    Ok(())
}

/// Write `patch`, a patch of `schema`, and the types it carries.
#[doc(hidden)]
pub fn write_authoring_patch(
    mut output: schema::authoring_patch::Builder<'_>,
    schema: &crate::LogicalSchema,
    patch: &crate::AuthoringPatch,
) -> Result<(), crate::AuthoringValueError> {
    output.set_patch(&crate::codec::patch_bytes(schema, patch)?);
    write_authoring_types(output.init_types(patch.types.len() as u32), &patch.types);
    Ok(())
}

/// Write a carrier's types, each with its schema.
#[doc(hidden)]
pub fn write_authoring_types(
    mut output: capnp::struct_list::Builder<'_, schema::authoring_type::Owned>,
    types: &[crate::AuthoringType],
) {
    for (index, ty) in types.iter().enumerate() {
        let mut row = output.reborrow().get(index as u32);
        row.set_path(ty.path.as_str());
        row.reborrow().init_type_uuid().set_bytes(&ty.type_uuid.0);
        row.set_schema_hash(&ty.schema_hash.0);
        row.set_schema(&crate::codec::schema_bytes(&ty.schema));
    }
}

/// A carrier's types (GAP 23 §3), each schema verified against its hash.
#[doc(hidden)]
pub fn decode_authoring_types(
    types: capnp::struct_list::Reader<'_, schema::authoring_type::Owned>,
) -> Result<Vec<crate::AuthoringType>, String> {
    types
        .iter()
        .map(|ty| {
            let (schema_hash, schema) = decode_schema(
                ty.get_schema_hash().map_err(authoring_decode_error)?,
                ty.get_schema().map_err(authoring_decode_error)?,
                "authoringType.schemaHash",
            )?;
            Ok(crate::AuthoringType {
                path: decode_text(ty.get_path().map_err(authoring_decode_error)?, "authoringType.path")
                    .map_err(|error| error.message)?,
                type_uuid: TypeUuid(
                    decode_uuid(
                        ty.get_type_uuid()
                            .map_err(authoring_decode_error)?
                            .get_bytes()
                            .map_err(authoring_decode_error)?,
                        "authoringType.typeUuid",
                    )
                    .map_err(|error| error.message)?,
                ),
                schema_hash,
                schema,
            })
        })
        .collect()
}

/// A blob reference: its length and content hash.
#[doc(hidden)]
pub fn decode_blob_ref(value: schema::blob_ref::Reader<'_>, field: &str) -> Result<crate::BlobRef, String> {
    Ok(crate::BlobRef {
        len: value.get_len(),
        hash: decode_hash(value.get_hash().map_err(authoring_decode_error)?, field).map_err(|error| error.message)?,
    })
}

/// Write the editor hints of a type member.
#[doc(hidden)]
pub fn write_editor_hints(mut output: schema::editor_hints::Builder<'_>, hints: &crate::EditorHints) {
    let text = |value: &Option<String>| value.as_deref().unwrap_or("").to_string();
    output.set_label(text(&hints.label).as_str());
    output.set_group(text(&hints.group).as_str());
    output.set_collapsed(hints.collapsed);
    output.set_read_only(hints.read_only);
    output.set_hidden(hints.hidden);
    if let Some([min, max]) = hints.range {
        output.set_has_range(true);
        output.set_range_min(min);
        output.set_range_max(max);
    }
    if let Some(min) = hints.min {
        output.set_has_min(true);
        output.set_min(min);
    }
    if let Some(max) = hints.max {
        output.set_has_max(true);
        output.set_max(max);
    }
    if let Some(step) = hints.step {
        output.set_has_step(true);
        output.set_step(step);
    }
    output.set_unit(text(&hints.unit).as_str());
    output.set_degrees(hints.degrees);
    output.set_percent(hints.percent);
    if let Some(precision) = hints.precision {
        output.set_has_precision(true);
        output.set_precision(precision);
    }
    output.set_slider(hints.slider);
    output.set_multiline(hints.multiline);
    if let Some(max_len) = hints.max_len {
        output.set_has_max_len(true);
        output.set_max_len(max_len);
    }
    output.set_flags(hints.flags);
    output.set_color(match hints.color {
        None => schema::ColorHint::None,
        Some(crate::ColorSpace::Srgb) => schema::ColorHint::Srgb,
        Some(crate::ColorSpace::Linear) => schema::ColorHint::Linear,
    });
    output.set_hdr(hints.hdr);
    output.set_alpha(match hints.alpha {
        None => schema::AlphaHint::Unset,
        Some(false) => schema::AlphaHint::Off,
        Some(true) => schema::AlphaHint::On,
    });
    output.set_filter(text(&hints.filter).as_str());
    if let Some(node) = &hints.node {
        output.set_has_node(true);
        output.set_node_component(node.component.as_str());
    }
    output.set_normalized(hints.normalized);
    output.set_uniform(hints.uniform);
    output.set_position(hints.position);
    output.set_rotation(hints.rotation);
}

/// A type member's editor hints.
#[doc(hidden)]
pub fn decode_editor_hints(value: schema::editor_hints::Reader<'_>) -> Result<crate::EditorHints, capnp::Error> {
    let text = |value: capnp::text::Reader<'_>, field: &str| -> Result<Option<String>, capnp::Error> {
        let value = decode_text(value, field).map_err(|error| capnp::Error::failed(error.message))?;
        Ok((!value.is_empty()).then_some(value))
    };
    let flag = |has: bool, value| has.then_some(value);
    Ok(crate::EditorHints {
        label: text(value.get_label()?, "editorHints.label")?,
        group: text(value.get_group()?, "editorHints.group")?,
        collapsed: value.get_collapsed(),
        read_only: value.get_read_only(),
        hidden: value.get_hidden(),
        range: value.get_has_range().then(|| [value.get_range_min(), value.get_range_max()]),
        min: flag(value.get_has_min(), value.get_min()),
        max: flag(value.get_has_max(), value.get_max()),
        step: flag(value.get_has_step(), value.get_step()),
        unit: text(value.get_unit()?, "editorHints.unit")?,
        degrees: value.get_degrees(),
        percent: value.get_percent(),
        precision: value.get_has_precision().then(|| value.get_precision()),
        slider: value.get_slider(),
        multiline: value.get_multiline(),
        max_len: value.get_has_max_len().then(|| value.get_max_len()),
        flags: value.get_flags(),
        color: match value.get_color()? {
            schema::ColorHint::None => None,
            schema::ColorHint::Srgb => Some(crate::ColorSpace::Srgb),
            schema::ColorHint::Linear => Some(crate::ColorSpace::Linear),
        },
        hdr: value.get_hdr(),
        alpha: match value.get_alpha()? {
            schema::AlphaHint::Unset => None,
            schema::AlphaHint::Off => Some(false),
            schema::AlphaHint::On => Some(true),
        },
        filter: text(value.get_filter()?, "editorHints.filter")?,
        node: if value.get_has_node() {
            Some(crate::NodeHint {
                component: decode_text(value.get_node_component()?, "editorHints.nodeComponent")
                    .map_err(|error| capnp::Error::failed(error.message))?,
            })
        } else {
            None
        },
        normalized: value.get_normalized(),
        uniform: value.get_uniform(),
        position: value.get_position(),
        rotation: value.get_rotation(),
    })
}

#[doc(hidden)]
pub fn decode_text_list(
    values: capnp::text_list::Reader<'_>,
    field: &str,
) -> Result<Vec<String>, WireFailure> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map_err(|error| WireFailure {
                    kind: InvalidRequest::Utf8,
                    message: format!("{field}[{index}] could not be read: {error}"),
                })
                .and_then(|value| decode_text(value, &format!("{field}[{index}]")))
        })
        .collect()
}

#[doc(hidden)]
pub fn decode_uuid(bytes: &[u8], field: &str) -> Result<[u8; 16], WireFailure> {
    fixed(bytes, field, InvalidRequest::Uuid)
}

#[doc(hidden)]
pub fn decode_hash(bytes: &[u8], field: &str) -> Result<[u8; 16], WireFailure> {
    fixed(bytes, field, InvalidRequest::Hash)
}

#[doc(hidden)]
pub fn decode_instance(bytes: &[u8], field: &str) -> Result<[u8; 16], WireFailure> {
    fixed(bytes, field, InvalidRequest::Instance)
}

#[doc(hidden)]
pub fn fixed<const N: usize>(
    bytes: &[u8],
    field: &str,
    kind: InvalidRequest,
) -> Result<[u8; N], WireFailure> {
    bytes.try_into().map_err(|_| WireFailure {
        kind,
        message: format!("{field} must be {N} bytes, got {}", bytes.len()),
    })
}

#[doc(hidden)]
pub fn decode_text(value: capnp::text::Reader<'_>, field: &str) -> Result<String, WireFailure> {
    value.to_str().map(str::to_owned).map_err(|_| WireFailure {
        kind: InvalidRequest::Utf8,
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
        Which::ConfigurationFailed(error) => Ok(RemoteConnectOutcome::ConfigurationFailed(
            decode_configuration_error(error?)?,
        )),
        Which::PipelineUnavailable(diagnostic) => {
            let diagnostic = diagnostic?;
            let diagnostic = crate::PipelineUnavailableDiagnostic::PipelineFailure(
                decode_pipeline_failure(diagnostic.get_pipeline_failure()?)?,
            );
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
        Which::Error(failure) => Ok(RemoteConnectOutcome::Error(decode_rpc_error(failure?)?)),
    }
}

/// What a remote call's `RpcError` says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteError {
    /// What failed; `None` for a request the daemon could not decode
    /// (`message` names the field).
    pub failure: Option<RpcFailure>,
    pub message: String,
}

#[doc(hidden)]
pub fn decode_rpc_error(
    value: schema::rpc_error::Reader<'_>,
) -> Result<RemoteError, capnp::Error> {
    use schema::rpc_error::failure::Which;
    let wire = |error: WireFailure| capnp::Error::failed(error.message);
    let text =
        |value: capnp::text::Reader<'_>, field: &str| decode_text(value, field).map_err(wire);
    let limit = |value: u64| {
        usize::try_from(value).map_err(|_| capnp::Error::failed(format!("limit {value} overflows")))
    };
    let failure = match value.get_failure().which()? {
        Which::InvalidRequest(_) => None,
        Which::SnapshotExpired(()) => Some(RpcFailure::SnapshotExpired),
        Which::ConnectionClosed(()) => Some(RpcFailure::ConnectionClosed),
        Which::PipelineUnavailable(diagnostic) => Some(RpcFailure::PipelineUnavailable(Box::new(
            crate::PipelineUnavailableDiagnostic::PipelineFailure(decode_pipeline_failure(
                diagnostic?.get_pipeline_failure()?,
            )?),
        ))),
        Which::BuildDepthExceeded(value) => {
            let value = value?;
            Some(RpcFailure::BuildDepthExceeded {
                limit: limit(value.get_limit())?,
                chain: decode_uuid_list(value.get_chain()?, "buildDepthExceeded.chain")
                    .map_err(wire)?,
            })
        }
        Which::InvalidCursor(value) => {
            let value = value?;
            Some(RpcFailure::InvalidCursor {
                since: InputVersion(value.get_since()),
                current: InputVersion(value.get_current()),
            })
        }
        Which::ResourceLimit(value) => {
            let value = value?;
            Some(RpcFailure::ResourceLimit {
                resource: text(value.get_resource()?, "resourceLimit.resource")?,
                limit: limit(value.get_limit())?,
            })
        }
        Which::ArtifactNotFound(hash) => Some(RpcFailure::ArtifactNotFound {
            hash: ContentHash(decode_hash(hash?, "artifactNotFound").map_err(wire)?),
        }),
        Which::AssetNotFound(uuid) => Some(RpcFailure::AssetNotFound {
            uuid: AssetUuid(decode_uuid(uuid?, "assetNotFound").map_err(wire)?),
        }),
        Which::ForeignSnapshot(()) => Some(RpcFailure::ForeignSnapshot),
        Which::InvalidPath(path) => Some(RpcFailure::InvalidPath {
            path: text(path?, "invalidPath")?,
        }),
        Which::InvalidQuery(detail) => Some(RpcFailure::InvalidQuery {
            detail: text(detail?, "invalidQuery")?,
        }),
        Which::TagIndexPoisoned(bundles) => Some(RpcFailure::TagIndexPoisoned {
            bundles: decode_uuid_list(bundles?, "tagIndexPoisoned")
                .map_err(wire)?
                .into_iter()
                .map(|uuid| BundleUuid(uuid.0))
                .collect(),
        }),
        Which::StaleInputVersion(value) => {
            let value = value?;
            Some(RpcFailure::StaleInputVersion {
                expected: InputVersion(value.get_expected()),
                got: InputVersion(value.get_got()),
            })
        }
        Which::InvalidAuthoringRequest(detail) => Some(RpcFailure::InvalidAuthoringRequest {
            detail: text(detail?, "invalidAuthoringRequest")?,
        }),
        Which::LossyWrite(value) => {
            let value = value?;
            Some(RpcFailure::LossyWrite {
                type_uuid: TypeUuid(
                    decode_uuid(value.get_type_uuid()?, "lossyWrite.typeUuid").map_err(wire)?,
                ),
                asset: AssetUuid(
                    decode_uuid(value.get_asset()?, "lossyWrite.asset").map_err(wire)?,
                ),
                fields: decode_text_list(value.get_fields()?, "lossyWrite.fields").map_err(wire)?,
                detail: text(value.get_detail()?, "lossyWrite.detail")?,
            })
        }
        Which::AuthoringBackendUnavailable(operation) => {
            Some(RpcFailure::AuthoringBackendUnavailable {
                operation: text(operation?, "authoringBackendUnavailable")?,
            })
        }
        Which::WireTreeNotFound(hash) => Some(RpcFailure::WireTreeNotFound {
            hash: LayoutHash(decode_hash(hash?, "wireTreeNotFound").map_err(wire)?),
        }),
    };
    Ok(RemoteError {
        failure,
        message: text(value.get_message()?, "error.message")?,
    })
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
        Which::Error(failure) => Ok(RemoteMetadataOutcome::Error(decode_rpc_error(failure?)?)),
    }
}

/// Decode and authenticate a wire authoring inspection before handing it to
/// tooling. This is the sole high-level boundary for the branded raw fields:
/// logical schema hash and the complete schema/value/blob walk are verified
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
    let (schema_hash, logical) =
        decode_schema(value.get_schema_hash()?, value.get_schema()?, "authoringInspection.schemaHash")
            .map_err(capnp::Error::failed)?;
    let authored_value = decode_authoring_value(&logical, value.get_value()?).map_err(capnp::Error::failed)?;
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
        schema: logical,
        role: match value.get_role()? {
            schema::AuthoringEntryRole::Runtime => AuthoringEntryRole::Runtime,
            schema::AuthoringEntryRole::AuthoringOnly => AuthoringEntryRole::AuthoringOnly,
        },
        value: authored_value,
        root: decode_text(value.get_root()?, "authoringInspection.root")
            .map_err(|error| capnp::Error::failed(error.message))?,
        origin: decode_bundle_origin(value.get_origin()?, "authoringInspection.origin")?,
        type_name: decode_text(value.get_type_name()?, "authoringInspection.typeName")
            .map_err(|error| capnp::Error::failed(error.message))?,
        content: decode_hash(value.get_content()?, "authoringInspection.content")
            .map_err(|error| capnp::Error::failed(error.message))?,
    })
}

pub fn decode_configuration_error(
    reader: schema::configuration_error::Reader<'_>,
) -> Result<ConfigurationError, capnp::Error> {
    if reader.get_detail_version() != 1 {
        return Err(capnp::Error::failed(format!(
            "unsupported configuration error detail version {}",
            reader.get_detail_version()
        )));
    }
    let code = crate::ConfigurationErrorCode::try_from(reader.get_code())
        .map_err(|error| capnp::Error::failed(error.to_string()))?;
    let detail = crate::DscpV1::from_canonical_detail_bytes(code, reader.get_detail_bytes()?)
        .map_err(|error| capnp::Error::failed(format!("invalid configuration-poison detail: {error}")))?;
    let value = ConfigurationError {
        code,
        reason_hash: decode_hash(reader.get_reason_hash()?, "reasonHash")
            .map_err(|error| capnp::Error::failed(error.message))?,
        detail: Box::new(detail),
        message: decode_text(reader.get_message()?, "configurationError.message")
            .map_err(|error| capnp::Error::failed(error.message))?,
    };
    value
        .validate()
        .map_err(|error| capnp::Error::failed(format!("invalid configuration error: {error}")))?;
    Ok(value)
}

pub fn decode_namespace_error(
    reader: schema::namespace_error::Reader<'_>,
) -> Result<NamespaceError, capnp::Error> {
    let detail = reader.get_detail()?;
    let detail = match detail
        .which()
        .map_err(|error| capnp::Error::failed(error.to_string()))?
    {
        schema::namespace_error_detail::Which::DuplicateAssetUuid(value) => {
            let value = value?;
            let asset = AssetUuid(
                decode_uuid(value.get_asset()?.get_bytes()?, "namespaceError.asset")
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
                                    source: read_bundle_source(authored.get_source()?)?,
                                    bundle: crate::BundleUuid(
                                        decode_uuid(
                                            authored.get_bundle()?.get_bytes()?,
                                            "namespaceError.claimant.bundle",
                                        )
                                        .map_err(|error| capnp::Error::failed(error.message))?,
                                    ),
                                    local_id: decode_text(
                                        authored.get_local_id()?,
                                        "namespaceError.claimant.localId",
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
                                            "namespaceError.claimant.parent",
                                        )
                                        .map_err(|error| capnp::Error::failed(error.message))?,
                                    ),
                                    output_key: decode_text(
                                        derived.get_output_key()?,
                                        "namespaceError.claimant.outputKey",
                                    )
                                    .map_err(|error| capnp::Error::failed(error.message))?,
                                }
                            }
                        },
                    )
                })
                .collect::<Result<Vec<_>, capnp::Error>>()?;
            NamespaceErrorV1::DuplicateAssetUuid { asset, claimants }
        }
        schema::namespace_error_detail::Which::DuplicateBundleUuid(value) => {
            let value = value?;
            NamespaceErrorV1::DuplicateBundleUuid {
                bundle: crate::BundleUuid(
                    decode_uuid(value.get_bundle()?.get_bytes()?, "namespaceError.bundle")
                        .map_err(|error| capnp::Error::failed(error.message))?,
                ),
                sources: value
                    .get_sources()?
                    .iter()
                    .map(read_bundle_source)
                    .collect::<Result<Vec<_>, _>>()?,
            }
        }
        schema::namespace_error_detail::Which::SameRootNormalizedPathCollision(value) => {
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
                                    "namespace error Windows path has odd byte length".to_owned(),
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
                            decode_hash(claim.get_file_hash()?, "namespaceError.claim.fileHash")
                                .map_err(|error| capnp::Error::failed(error.message))?,
                        ),
                    })
                })
                .collect::<Result<Vec<_>, capnp::Error>>()?;
            NamespaceErrorV1::SameRootNormalizedPathCollision {
                root_name: decode_text(value.get_root_name()?, "namespaceError.rootName")
                    .map_err(|error| capnp::Error::failed(error.message))?,
                normalized_path: decode_text(
                    value.get_normalized_path()?,
                    "namespaceError.normalizedPath",
                )
                .map_err(|error| capnp::Error::failed(error.message))?,
                claims,
            }
        }
        schema::namespace_error_detail::Which::IncompleteSkeleton(value) => {
            let value = value?;
            NamespaceErrorV1::IncompleteSkeleton {
                source: read_bundle_source(value.get_source()?)?,
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
        schema::namespace_error_detail::Which::UnreadableGlobalBundlePath(value) => {
            let value = value?;
            NamespaceErrorV1::UnreadableGlobalBundlePath {
                root_name: decode_text(value.get_root_name()?, "namespaceError.rootName")
                    .map_err(|error| capnp::Error::failed(error.message))?,
                normalized_path: decode_text(
                    value.get_normalized_path()?,
                    "namespaceError.normalizedPath",
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
        schema::namespace_error_detail::Which::InvalidPhysicalPath(value) => {
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
                            "namespace error Windows path has odd byte length".to_owned(),
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
            NamespaceErrorV1::InvalidPhysicalPath {
                root_name: decode_text(value.get_root_name()?, "namespaceError.rootName")
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
        schema::namespace_error_detail::Which::UnreadableScanSubtree(value) => {
            let value = value?;
            let subject = match value
                .get_subject()?
                .which()
                .map_err(|error| capnp::Error::failed(error.to_string()))?
            {
                schema::scan_subject::Which::Root(root) => crate::ScanSubject::Root {
                    root_name: decode_text(root?.get_root_name()?, "namespaceError.scan.rootName")
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
                                    "namespace error Windows scan path has odd byte length"
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
                            "namespaceError.scan.rootName",
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
            NamespaceErrorV1::UnreadableScanSubtree { subject, failure }
        }
    };
    let message = decode_text(reader.get_message()?, "namespaceError.message")
        .map_err(|error| capnp::Error::failed(error.message))?;
    let decoded = NamespaceError::new(detail, message)
        .map_err(|error| capnp::Error::failed(error.to_string()))?;
    let identity = decode_hash(reader.get_identity()?, "namespaceError.identity")
        .map_err(|error| capnp::Error::failed(error.message))?;
    if decoded.code as u16 != reader.get_code() || decoded.identity != identity {
        return Err(capnp::Error::failed(
            "namespace error code/detail/identity mismatch".to_owned(),
        ));
    }
    Ok(decoded)
}

pub fn decode_pipeline_failure(
    reader: schema::pipeline_failure::Reader<'_>,
) -> Result<crate::PipelineFailure, capnp::Error> {
    let code = reader.get_code()? as u16 + 1;
    let origin = reader.get_origin()? as u16 + 1;
    let cleanup = reader.get_cleanup()? as u16;
    let identity = decode_hash(reader.get_identity()?, "pipelineFailure.identity")
        .map_err(|error| capnp::Error::failed(error.message))?;
    let message = decode_text(reader.get_message()?, "pipelineFailure.message")
        .map_err(|error| capnp::Error::failed(error.message))?;
    crate::PipelineFailure::from_wire(code, origin, cleanup, identity, message)
        .map_err(|error| capnp::Error::failed(error.to_string()))
}

#[doc(hidden)]
pub fn read_bundle_source(
    source: schema::bundle_source::Reader<'_>,
) -> Result<crate::ReadableBundleSource, capnp::Error> {
    Ok(crate::ReadableBundleSource {
        root_name: decode_text(source.get_root_name()?, "namespaceError.source.rootName")
            .map_err(|error| capnp::Error::failed(error.message))?,
        normalized_path: decode_text(
            source.get_normalized_path()?,
            "namespaceError.source.normalizedPath",
        )
        .map_err(|error| capnp::Error::failed(error.message))?,
        file_hash: distill_core::id::BundleFileHash(
            decode_hash(source.get_file_hash()?, "namespaceError.source.fileHash")
                .map_err(|error| capnp::Error::failed(error.message))?,
        ),
    })
}

#[doc(hidden)]
pub fn decode_bundle_origin(
    origin: schema::bundle_origin::Reader<'_>,
    what: &str,
) -> Result<BundleOrigin, capnp::Error> {
    let failed = |error: WireFailure| capnp::Error::failed(error.message);
    Ok(match origin.which()? {
        schema::bundle_origin::Which::Authored(()) => BundleOrigin::Authored,
        schema::bundle_origin::Which::Explicit(()) => BundleOrigin::Explicit,
        schema::bundle_origin::Which::Rule(group) => BundleOrigin::Rule {
            rules_bundle: BundleUuid(
                decode_uuid(group.get_rules_bundle()?, &format!("{what}.rulesBundle")).map_err(failed)?,
            ),
            rule: decode_uuid(group.get_rule()?, &format!("{what}.rule")).map_err(failed)?,
        },
        schema::bundle_origin::Which::Default(rule) => BundleOrigin::Default {
            rule: decode_uuid(rule?, &format!("{what}.default")).map_err(failed)?,
        },
    })
}

/// Decode wire authoring rows (an `AuthoringListCall` success).
pub fn decode_authoring_rows(
    rows: capnp::struct_list::Reader<'_, schema::authoring_row::Owned>,
) -> Result<Vec<AuthoringRow>, capnp::Error> {
    let failed = |error: WireFailure| capnp::Error::failed(error.message);
    let mut out = Vec::with_capacity(rows.len() as usize);
    for row in rows {
        let origin = decode_bundle_origin(row.get_origin()?, "authoringRow.origin")?;
        out.push(AuthoringRow {
            uuid: AssetUuid(decode_uuid(row.get_uuid()?, "authoringRow.uuid").map_err(failed)?),
            bundle: BundleUuid(decode_uuid(row.get_bundle()?, "authoringRow.bundle").map_err(failed)?),
            root: decode_text(row.get_root()?, "authoringRow.root").map_err(failed)?,
            normalized_path: decode_text(row.get_normalized_path()?, "authoringRow.normalizedPath")
                .map_err(failed)?,
            local_id: decode_text(row.get_local_id()?, "authoringRow.localId").map_err(failed)?,
            type_uuid: TypeUuid(decode_uuid(row.get_type_uuid()?, "authoringRow.typeUuid").map_err(failed)?),
            type_name: decode_text(row.get_type_name()?, "authoringRow.typeName").map_err(failed)?,
            role: match row.get_role()? {
                schema::AuthoringEntryRole::Runtime => AuthoringEntryRole::Runtime,
                schema::AuthoringEntryRole::AuthoringOnly => AuthoringEntryRole::AuthoringOnly,
            },
            origin,
            primary: row.get_primary(),
            content: decode_hash(row.get_content()?, "authoringRow.content").map_err(failed)?,
        });
    }
    Ok(out)
}

/// Decode a wire source-file listing (a `SourceFilesCall` success).
pub fn decode_source_files(
    files: capnp::struct_list::Reader<'_, schema::source_file::Owned>,
) -> Result<Vec<SourceFile>, capnp::Error> {
    let failed = |error: WireFailure| capnp::Error::failed(error.message);
    let optional = |bytes: &[u8], field: &str| -> Result<Option<BundleUuid>, capnp::Error> {
        match bytes {
            [] => Ok(None),
            bytes => Ok(Some(BundleUuid(decode_uuid(bytes, field).map_err(failed)?))),
        }
    };
    let mut out = Vec::with_capacity(files.len() as usize);
    for file in files {
        let claim = match file.get_claim()?.which()? {
            schema::source_claim::Which::Unclaimed(()) => SourceClaim::Unclaimed,
            schema::source_claim::Which::Explicit(bundle) => SourceClaim::Explicit {
                bundle: BundleUuid(decode_uuid(bundle?, "sourceFile.claim.explicit").map_err(failed)?),
            },
            schema::source_claim::Which::Rule(group) => SourceClaim::Rule {
                rules_bundle: optional(group.get_rules_bundle()?, "sourceFile.claim.rulesBundle")?,
                rule: decode_uuid(group.get_rule()?, "sourceFile.claim.rule").map_err(failed)?,
                importer: decode_text(group.get_importer()?, "sourceFile.claim.importer").map_err(failed)?,
                output: decode_text(group.get_output()?, "sourceFile.claim.output").map_err(failed)?,
                bundle: optional(group.get_bundle()?, "sourceFile.claim.bundle")?,
            },
        };
        out.push(SourceFile {
            root: decode_text(file.get_root()?, "sourceFile.root").map_err(failed)?,
            path: decode_text(file.get_path()?, "sourceFile.path").map_err(failed)?,
            claim,
        });
    }
    Ok(out)
}

/// Decode wire bundle references (a `ReferencesCall` success).
pub fn decode_bundle_references(
    value: schema::bundle_references::Reader<'_>,
) -> Result<BundleReferences, capnp::Error> {
    let failed = |error: WireFailure| capnp::Error::failed(error.message);
    let mut referenced_by = Vec::new();
    for row in value.get_referenced_by()? {
        referenced_by.push((
            BundleUuid(decode_uuid(row.get_bundle()?, "bundleRef.bundle").map_err(failed)?),
            decode_text(row.get_root()?, "bundleRef.root").map_err(failed)?,
            decode_text(row.get_path()?, "bundleRef.path").map_err(failed)?,
        ));
    }
    let mut references = Vec::new();
    for path in value.get_references()? {
        references.push(decode_text(path?, "bundleReferences.references").map_err(failed)?);
    }
    Ok(BundleReferences { referenced_by, references })
}

#[doc(hidden)]
pub fn decode_stamp_value(
    stamp: schema::snapshot_stamp_value::Reader<'_>,
    field: &str,
) -> Result<SnapshotStamp, capnp::Error> {
    Ok(SnapshotStamp {
        instance: StoreInstanceId(
            decode_instance(stamp.get_instance()?, field).map_err(|error| capnp::Error::failed(error.message))?,
        ),
        version: InputVersion(stamp.get_input_version()),
    })
}

/// Decode a wire type description, authenticating its logical schema
/// against its hash and its default value against the schema, as
/// [`decode_authoring_inspection`] does for an entry.
pub fn decode_type_description(
    value: schema::type_description::Reader<'_>,
) -> Result<TypeDescription, capnp::Error> {
    let failed = |error: WireFailure| capnp::Error::failed(error.message);
    let stamp = decode_stamp_value(value.get_stamp()?, "typeDescription.stamp.instance")?;
    let type_uuid = TypeUuid(decode_uuid(value.get_type_uuid()?, "typeDescription.typeUuid").map_err(failed)?);
    let (schema_hash, logical) =
        decode_schema(value.get_schema_hash()?, value.get_schema()?, "typeDescription.schemaHash")
            .map_err(capnp::Error::failed)?;
    let default = decode_type_default(&logical, value.get_default_value()?, "typeDescription.defaultValue")?;
    let optional = |text: String| (!text.is_empty()).then_some(text);
    let mut members = Vec::new();
    for member in value.get_members()? {
        members.push(TypeMember {
            path: decode_text(member.get_path()?, "typeMember.path").map_err(failed)?,
            doc: optional(decode_text(member.get_doc()?, "typeMember.doc").map_err(failed)?),
            type_doc: optional(decode_text(member.get_type_doc()?, "typeMember.typeDoc").map_err(failed)?),
            editor: decode_editor_hints(member.get_editor()?)?,
        });
    }
    let mut renames = BTreeMap::new();
    for rename in value.get_renames()? {
        renames.insert(
            decode_text(rename.get_at()?, "typeRename.at").map_err(failed)?,
            decode_text(rename.get_from()?, "typeRename.from").map_err(failed)?,
        );
    }
    Ok(TypeDescription {
        stamp,
        type_uuid,
        path: decode_text(value.get_path()?, "typeDescription.path").map_err(failed)?,
        schema_hash,
        schema: logical,
        default,
        members,
        renames,
    })
}

/// Decode a type default under its (verified) schema.
#[doc(hidden)]
pub fn decode_type_default(
    logical: &crate::LogicalSchema,
    value: schema::type_default::Reader<'_>, field: &str) -> Result<TypeDefault, capnp::Error> {
    let failed = |error: WireFailure| capnp::Error::failed(error.message);
    Ok(match value.which()? {
        schema::type_default::Which::Value(wire) => {
            TypeDefault::Value(decode_authoring_value(logical, wire?).map_err(capnp::Error::failed)?)
        }
        schema::type_default::Which::Absent(()) => TypeDefault::Absent,
        schema::type_default::Which::SchemaMismatch(hash) => TypeDefault::SchemaMismatch {
            written: crate::LogicalHash(decode_hash(hash?, field).map_err(failed)?),
        },
        schema::type_default::Which::Invalid(detail) => {
            TypeDefault::Invalid(decode_text(detail?, field).map_err(failed)?)
        }
    })
}

/// Decode a wire `authorable` answer (its success arm), authenticating
/// each schema against its hash and each default against its schema.
pub fn decode_authorable(value: schema::authorable::Reader<'_>) -> Result<Authorable, capnp::Error> {
    let failed = |error: WireFailure| capnp::Error::failed(error.message);
    let hashed = |hash: &[u8], bytes: &[u8], field: &str| decode_schema(hash, bytes, field).map_err(capnp::Error::failed);
    let mut types = Vec::new();
    for ty in value.get_types()? {
        let (schema_hash, logical) = hashed(ty.get_schema_hash()?, ty.get_schema()?, "authorableType.schemaHash")?;
        types.push(AuthorableType {
            type_uuid: TypeUuid(decode_uuid(ty.get_type_uuid()?, "authorableType.typeUuid").map_err(failed)?),
            path: decode_text(ty.get_path()?, "authorableType.path").map_err(failed)?,
            extension: decode_text(ty.get_extension()?, "authorableType.extension").map_err(failed)?,
            local_id: decode_text(ty.get_local_id()?, "authorableType.localId").map_err(failed)?,
            doc: decode_text(ty.get_doc()?, "authorableType.doc").map_err(failed)?,
            authoring_only: ty.get_authoring_only(),
            default: decode_type_default(&logical, ty.get_default_value()?, "authorableType.defaultValue")?,
            schema_hash,
            schema: logical,
        });
    }
    let mut importers = Vec::new();
    for importer in value.get_importers()? {
        let (settings_hash, settings_schema) = hashed(
            importer.get_settings_hash()?,
            importer.get_settings_schema()?,
            "authorableImporter.settingsHash",
        )?;
        let default_settings =
            decode_authoring_value(&settings_schema, importer.get_default_settings()?).map_err(capnp::Error::failed)?;
        let mut default_globs = Vec::new();
        for glob in importer.get_default_globs()? {
            default_globs.push(decode_text(glob?, "authorableImporter.defaultGlobs").map_err(failed)?);
        }
        importers.push(AuthorableImporter {
            id: decode_text(importer.get_id()?, "authorableImporter.id").map_err(failed)?,
            settings_type: TypeUuid(
                decode_uuid(importer.get_settings_type()?, "authorableImporter.settingsType").map_err(failed)?,
            ),
            settings_path: decode_text(importer.get_settings_path()?, "authorableImporter.settingsPath")
                .map_err(failed)?,
            settings_hash,
            settings_schema,
            default_settings,
            default_globs,
        });
    }
    Ok(Authorable { types, importers })
}

/// Decode a wire describable-types answer (its success arm).
pub fn decode_describable_types(
    value: schema::describable_types::Reader<'_>,
) -> Result<DescribableTypesResult, capnp::Error> {
    let failed = |error: WireFailure| capnp::Error::failed(error.message);
    let stamp = decode_stamp_value(value.get_stamp()?, "describableTypes.stamp.instance")?;
    let index_hash = match value.get_index_hash()? {
        [] => None,
        hash => Some(ContentHash(decode_hash(hash, "describableTypes.indexHash").map_err(failed)?)),
    };
    let mut types = Vec::new();
    for row in value.get_types()? {
        types.push(DescribableType {
            type_uuid: TypeUuid(decode_uuid(row.get_type_uuid()?, "describableType.typeUuid").map_err(failed)?),
            path: decode_text(row.get_path()?, "describableType.path").map_err(failed)?,
            component: row.get_component(),
        });
    }
    Ok(DescribableTypesResult::Types { stamp, index_hash, types })
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
