//! One-shot construction of a v2 pack from one pinned RPC snapshot.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use distill_build::query::{
    AssetQuery as BuildAssetQuery, IntakeError, TagSelector as BuildTagSelector,
};
use distill_build::trace::PackDefinitionControlValue;
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, TypeUuid};
use distill_json::AuthoredValue;
use distill_rpc::{
    ArtifactChunkKind, AuthoringValue, ConfigurationPoison, Hub, PackSession, PathResolveResult,
    ReconnectReason, ResolveResult, RpcFailure, RpcResult, Snapshot, TagSelector, VersionPoison,
};
use distill_wire::artifact::{parse_artifact_parts, ArtifactError};
use unicode_normalization::UnicodeNormalization;

use crate::activation::{activate, publish_archive, publish_manifest, PointerError};
use crate::archive::{encode_archive, ArchiveError, ArtifactPayload};
use crate::manifest::{
    canonicalize, encode_manifest, verify_expected_terminals, ArchiveRef, EncodingRow, IndexRow,
    ManifestAssetRow, ManifestError, ManifestLoadEdge, PackManifest, PackTarget, PathRow,
    WireTreeRow,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackBuildTarget {
    pub name: String,
    pub definition_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackBuildOutput {
    pub manifest: PackManifest,
    pub manifest_bytes: Vec<u8>,
    pub archive_bytes: Vec<u8>,
    pub archive_file_hash: [u8; 32],
}

#[derive(Debug)]
pub enum PackBuildError {
    Definition(String),
    TargetMismatch {
        definition: String,
        requested: String,
    },
    NoRoots,
    InvalidRoot {
        index: usize,
        error: IntakeError,
    },
    EmptyRoot {
        index: usize,
    },
    ReconnectRequired(ReconnectReason),
    ConfigurationPoisoned(Box<ConfigurationPoison>),
    VersionPoisoned(Box<VersionPoison>),
    Rpc(Box<RpcFailure>),
    BasisMismatch,
    Resolve {
        asset: AssetUuid,
        result: Box<ResolveResult>,
    },
    ChunkSequence {
        hash: ContentHash,
    },
    Artifact {
        asset: AssetUuid,
        error: ArtifactError,
    },
    ArtifactIdentity {
        asset: AssetUuid,
    },
    BuildOnlyType {
        type_uuid: TypeUuid,
    },
    InvalidWireTree(LayoutHash),
    LoadCycle {
        cycle: Vec<AssetUuid>,
    },
    Path {
        path: String,
        result: PathResolveResult,
    },
    Archive(ArchiveError),
    Manifest(ManifestError),
    Publication(PointerError),
}

impl fmt::Display for PackBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "pack build failed: {self:?}")
    }
}

impl std::error::Error for PackBuildError {}

impl From<ArchiveError> for PackBuildError {
    fn from(value: ArchiveError) -> Self {
        Self::Archive(value)
    }
}

impl From<ManifestError> for PackBuildError {
    fn from(value: ManifestError) -> Self {
        Self::Manifest(value)
    }
}

impl From<PointerError> for PackBuildError {
    fn from(value: PointerError) -> Self {
        Self::Publication(value)
    }
}

/// Decode the sealed PackDefinition authored value carried by the metadata
/// RPC. The daemon has already authenticated its bootstrap type/schema; this
/// function still requires canonical JSON and the exact closed value shape.
pub fn decode_pack_definition(
    authored: &AuthoringValue,
) -> Result<PackDefinitionControlValue, PackBuildError> {
    if !authored.blobs.is_empty() {
        return Err(definition_error("PackDefinition cannot contain blobs"));
    }
    let text = std::str::from_utf8(&authored.canonical_value)
        .map_err(|error| definition_error(format!("PackDefinition is not UTF-8: {error}")))?;
    let value = distill_json::parse(text)
        .map_err(|error| definition_error(format!("PackDefinition JSON is invalid: {error}")))?;
    if distill_json::write(&value)
        .map_err(|error| definition_error(format!("PackDefinition is not writable: {error}")))?
        .as_bytes()
        != authored.canonical_value.as_ref()
    {
        return Err(definition_error("PackDefinition JSON is not canonical"));
    }
    decode_pack_definition_value(&value)
}

fn decode_pack_definition_value(
    value: &AuthoredValue,
) -> Result<PackDefinitionControlValue, PackBuildError> {
    let fields = exact_object(
        value,
        "PackDefinition",
        &["include_path_table", "roots", "target", "zstd_level"],
    )?;
    let roots = as_array(field(fields, "roots")?, "PackDefinition.roots")?
        .iter()
        .enumerate()
        .map(|(index, value)| decode_asset_query(value, index))
        .collect::<Result<Vec<_>, _>>()?;
    let zstd_level = i32::try_from(as_i128(
        field(fields, "zstd_level")?,
        "PackDefinition.zstd_level",
    )?)
    .map_err(|_| definition_error("PackDefinition.zstd_level is outside i32 range"))?;
    Ok(PackDefinitionControlValue {
        roots,
        target: as_string(field(fields, "target")?, "PackDefinition.target")?.to_owned(),
        zstd_level,
        include_path_table: as_bool(
            field(fields, "include_path_table")?,
            "PackDefinition.include_path_table",
        )?,
    })
}

fn decode_asset_query(
    value: &AuthoredValue,
    index: usize,
) -> Result<BuildAssetQuery, PackBuildError> {
    let context = format!("PackDefinition.roots[{index}]");
    let fields = exact_object(
        value,
        &context,
        &[
            "authored_type",
            "authoring_only",
            "bundle_path",
            "bundle_uuid",
            "local_id",
            "path_glob",
            "path_prefix",
            "tag",
            "terminal_type",
            "uuid",
        ],
    )?;
    Ok(BuildAssetQuery {
        uuid: optional(field(fields, "uuid")?, |value| {
            fixed_bytes(value, &format!("{context}.uuid")).map(AssetUuid)
        })?,
        bundle_path: optional(field(fields, "bundle_path")?, |value| {
            as_string(value, &format!("{context}.bundle_path")).map(str::to_owned)
        })?,
        local_id: optional(field(fields, "local_id")?, |value| {
            as_string(value, &format!("{context}.local_id")).map(str::to_owned)
        })?,
        bundle_uuid: optional(field(fields, "bundle_uuid")?, |value| {
            fixed_bytes(value, &format!("{context}.bundle_uuid")).map(BundleUuid)
        })?,
        authored_type: optional(field(fields, "authored_type")?, |value| {
            fixed_bytes(value, &format!("{context}.authored_type")).map(TypeUuid)
        })?,
        terminal_type: optional(field(fields, "terminal_type")?, |value| {
            fixed_bytes(value, &format!("{context}.terminal_type")).map(TypeUuid)
        })?,
        tag: optional(field(fields, "tag")?, |value| {
            decode_tag_selector(value, &format!("{context}.tag"))
        })?,
        path_prefix: optional(field(fields, "path_prefix")?, |value| {
            as_string(value, &format!("{context}.path_prefix")).map(str::to_owned)
        })?,
        path_glob: optional(field(fields, "path_glob")?, |value| {
            as_string(value, &format!("{context}.path_glob")).map(str::to_owned)
        })?,
        authoring_only: optional(field(fields, "authoring_only")?, |value| {
            as_bool(value, &format!("{context}.authoring_only"))
        })?,
    })
}

fn decode_tag_selector(
    value: &AuthoredValue,
    context: &str,
) -> Result<BuildTagSelector, PackBuildError> {
    let fields = exact_object(value, context, &["tag", "value"])?;
    Ok(BuildTagSelector {
        tag: as_string(field(fields, "tag")?, &format!("{context}.tag"))?.to_owned(),
        value: optional(field(fields, "value")?, |value| {
            as_string(value, &format!("{context}.value")).map(str::to_owned)
        })?,
    })
}

fn definition_error(message: impl Into<String>) -> PackBuildError {
    PackBuildError::Definition(message.into())
}

fn exact_object<'a>(
    value: &'a AuthoredValue,
    context: &str,
    expected: &[&str],
) -> Result<&'a BTreeMap<String, AuthoredValue>, PackBuildError> {
    let AuthoredValue::Object(fields) = value else {
        return Err(definition_error(format!("{context} must be an object")));
    };
    if fields.len() != expected.len() || expected.iter().any(|name| !fields.contains_key(*name)) {
        return Err(definition_error(format!(
            "{context} does not have the sealed field set"
        )));
    }
    Ok(fields)
}

fn field<'a>(
    fields: &'a BTreeMap<String, AuthoredValue>,
    name: &str,
) -> Result<&'a AuthoredValue, PackBuildError> {
    fields
        .get(name)
        .ok_or_else(|| definition_error(format!("missing field {name:?}")))
}

fn as_array<'a>(
    value: &'a AuthoredValue,
    context: &str,
) -> Result<&'a [AuthoredValue], PackBuildError> {
    match value {
        AuthoredValue::Array(values) => Ok(values),
        _ => Err(definition_error(format!("{context} must be an array"))),
    }
}

fn as_string<'a>(value: &'a AuthoredValue, context: &str) -> Result<&'a str, PackBuildError> {
    match value {
        AuthoredValue::Str(value) => Ok(value),
        _ => Err(definition_error(format!("{context} must be text"))),
    }
}

fn as_bool(value: &AuthoredValue, context: &str) -> Result<bool, PackBuildError> {
    match value {
        AuthoredValue::Bool(value) => Ok(*value),
        _ => Err(definition_error(format!("{context} must be a boolean"))),
    }
}

fn as_i128(value: &AuthoredValue, context: &str) -> Result<i128, PackBuildError> {
    match value {
        AuthoredValue::Int(value) => Ok(*value),
        AuthoredValue::UInt(value) => i128::try_from(*value)
            .map_err(|_| definition_error(format!("{context} is outside i128 range"))),
        _ => Err(definition_error(format!("{context} must be an integer"))),
    }
}

fn optional<T>(
    value: &AuthoredValue,
    decode: impl FnOnce(&AuthoredValue) -> Result<T, PackBuildError>,
) -> Result<Option<T>, PackBuildError> {
    match value {
        AuthoredValue::Null => Ok(None),
        value => decode(value).map(Some),
    }
}

fn fixed_bytes<const N: usize>(
    value: &AuthoredValue,
    context: &str,
) -> Result<[u8; N], PackBuildError> {
    let values = as_array(value, context)?;
    if values.len() != N {
        return Err(definition_error(format!(
            "{context} must contain exactly {N} bytes"
        )));
    }
    let mut bytes = [0; N];
    for (target, value) in bytes.iter_mut().zip(values) {
        let AuthoredValue::UInt(value) = value else {
            return Err(definition_error(format!(
                "{context} contains a non-byte value"
            )));
        };
        *target = u8::try_from(*value)
            .map_err(|_| definition_error(format!("{context} contains an out-of-range byte")))?;
    }
    Ok(bytes)
}

pub fn encoder_identity() -> String {
    format!(
        "distill-pack/{},zstd/{}",
        env!("CARGO_PKG_VERSION"),
        zstd::zstd_safe::version_string()
    )
}

struct FetchedArtifact {
    content_hash: ContentHash,
    structural: Vec<u8>,
    blobs: Vec<Vec<u8>>,
    terminal_type: TypeUuid,
    layout_hash: LayoutHash,
    load_edges: Vec<distill_rpc::ServedLoadEdge>,
}

/// Build the single-archive v2 pack described by `definition` from exactly
/// `snapshot`. Drift and every other non-built terminal result are fatal.
pub fn build_pack(
    definition: &PackDefinitionControlValue,
    target: &PackBuildTarget,
    encoder_identity: &str,
    snapshot: &Snapshot,
    hub: &Hub,
) -> Result<PackBuildOutput, PackBuildError> {
    let target_name = target.name.nfc().collect::<String>();
    let definition_target = definition.target.nfc().collect::<String>();
    if definition_target != target_name {
        return Err(PackBuildError::TargetMismatch {
            definition: definition_target,
            requested: target_name,
        });
    }
    if definition.roots.is_empty() {
        return Err(PackBuildError::NoRoots);
    }
    let pack_session = rpc_success(snapshot.open_pack_session())?;
    let mut pending = BTreeSet::new();
    for (index, root) in definition.roots.iter().enumerate() {
        rpc_success(pack_session.keep_alive())?;
        let root = root
            .clone()
            .close(None)
            .map_err(|error| PackBuildError::InvalidRoot { index, error })?;
        let results = rpc_success(snapshot.query(to_rpc_query(root)))?;
        if results.is_empty() {
            return Err(PackBuildError::EmptyRoot { index });
        }
        pending.extend(results);
    }

    let mut artifacts = BTreeMap::new();
    while let Some(asset) = pending.pop_first() {
        if artifacts.contains_key(&asset) {
            continue;
        }
        rpc_success(pack_session.keep_alive())?;
        let resolved = terminal(snapshot, rpc_success(snapshot.resolve_batch(asset))?)?;
        let content_hash = match resolved {
            ResolveResult::Built { content_hash } => content_hash,
            result => {
                return Err(PackBuildError::Resolve {
                    asset,
                    result: Box::new(result),
                });
            }
        };
        let chunks = terminal(snapshot, rpc_success(snapshot.fetch(content_hash))?)?;
        let load_edges = chunks.load_edges().to_vec();
        let (structural, blobs) = collect_chunks(content_hash, chunks)?;
        let blob_parts = blobs.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let parsed = parse_artifact_parts(&structural, &blob_parts)
            .map_err(|error| PackBuildError::Artifact { asset, error })?;
        if parsed.asset_uuid != asset || parsed.content_hash != content_hash {
            return Err(PackBuildError::ArtifactIdentity { asset });
        }
        let load_deps = parsed.load_deps.clone();
        if load_edges
            .iter()
            .map(|edge| edge.asset)
            .ne(load_deps.iter().copied())
        {
            return Err(PackBuildError::ArtifactIdentity { asset });
        }
        let terminal_type = parsed.terminal_type;
        let layout_hash = parsed.layout_hash;
        drop(parsed);
        rpc_success(pack_session.pin(&[content_hash.0, layout_hash.0]))?;
        pending.extend(load_deps.iter().copied());
        artifacts.insert(
            asset,
            FetchedArtifact {
                content_hash,
                structural,
                blobs,
                terminal_type,
                layout_hash,
                load_edges,
            },
        );
    }

    let graph = artifacts
        .iter()
        .map(|(asset, artifact)| {
            (
                *asset,
                artifact
                    .load_edges
                    .iter()
                    .map(|edge| edge.asset)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(cycle) = distill_loader::load_cycles(&graph).into_iter().next() {
        return Err(PackBuildError::LoadCycle { cycle });
    }

    for type_uuid in artifacts
        .values()
        .map(|artifact| artifact.terminal_type)
        .collect::<BTreeSet<_>>()
    {
        rpc_success(pack_session.keep_alive())?;
        if rpc_success(snapshot.runtime_type_policy(type_uuid))?.build_only {
            return Err(PackBuildError::BuildOnlyType { type_uuid });
        }
    }

    let mut wire_trees = Vec::new();
    for layout_hash in artifacts
        .values()
        .map(|artifact| artifact.layout_hash)
        .collect::<BTreeSet<_>>()
    {
        rpc_success(pack_session.keep_alive())?;
        let bytes = rpc_success(hub.wire_tree(layout_hash))?.to_vec();
        let root = distill_wire::dswl::decode_dswl(&bytes)
            .map_err(|_| PackBuildError::InvalidWireTree(layout_hash))?;
        if distill_wire::dswl::dswl_hash(&root)
            .map_err(|_| PackBuildError::InvalidWireTree(layout_hash))?
            != layout_hash
        {
            return Err(PackBuildError::InvalidWireTree(layout_hash));
        }
        wire_trees.push(WireTreeRow { layout_hash, bytes });
    }

    let manifest_assets = artifacts
        .iter()
        .map(|(asset, artifact)| ManifestAssetRow {
            asset_uuid: *asset,
            content_hash: artifact.content_hash,
            load_deps: artifact
                .load_edges
                .iter()
                .map(|edge| ManifestLoadEdge {
                    asset_uuid: edge.asset,
                    expected_terminal: edge.expected_terminal,
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    let terminal_types = artifacts
        .iter()
        .map(|(asset, artifact)| (*asset, artifact.terminal_type))
        .collect::<BTreeMap<_, _>>();

    let paths = if definition.include_path_table {
        Some(build_paths(
            snapshot,
            &pack_session,
            artifacts.keys().copied(),
        )?)
    } else {
        None
    };
    let archive = encode_archive(
        0,
        encoder_identity,
        definition.zstd_level,
        &artifacts
            .values()
            .map(|artifact| ArtifactPayload {
                content_hash: artifact.content_hash,
                structural: artifact.structural.clone(),
                blobs: artifact.blobs.clone(),
            })
            .collect::<Vec<_>>(),
    )?;
    let archive_file_hash = *blake3::hash(&archive.bytes).as_bytes();
    let manifest = canonicalize(PackManifest {
        target: PackTarget { name: target_name },
        target_def_hash: target.definition_hash,
        archives: vec![ArchiveRef {
            generation: 0,
            file_hash: archive_file_hash,
        }],
        assets: manifest_assets,
        encodings: archive
            .encodings
            .iter()
            .map(|(content_hash, encoding)| EncodingRow {
                content_hash: *content_hash,
                blocks: encoding.blocks.clone(),
                blobs: encoding.blobs.clone(),
            })
            .collect(),
        index: archive
            .index
            .iter()
            .map(|(ekey, location)| IndexRow {
                ekey: *ekey,
                location: *location,
            })
            .collect(),
        wire_trees,
        paths,
    })?;
    verify_expected_terminals(&manifest, &terminal_types)?;
    let manifest_bytes = encode_manifest(&manifest)?;
    Ok(PackBuildOutput {
        manifest,
        manifest_bytes,
        archive_bytes: archive.bytes,
        archive_file_hash,
    })
}

/// Build, durably publish, and activate one v2 pack in the required order.
/// The destination directory must already exist. If publication fails before
/// activation, the old `pack.current` remains authoritative.
#[allow(clippy::too_many_arguments)]
pub fn build_publish_and_activate_pack(
    directory: &Path,
    definition: &PackDefinitionControlValue,
    target: &PackBuildTarget,
    encoder_identity: &str,
    snapshot: &Snapshot,
    hub: &Hub,
) -> Result<PackBuildOutput, PackBuildError> {
    let output = build_pack(definition, target, encoder_identity, snapshot, hub)?;
    let archive_hash = publish_archive(directory, &output.archive_bytes)?;
    debug_assert_eq!(archive_hash, output.archive_file_hash);
    let manifest_hash = publish_manifest(directory, &output.manifest_bytes)?;
    activate(directory, manifest_hash)?;
    Ok(output)
}

fn to_rpc_query(query: BuildAssetQuery) -> distill_rpc::AssetQuery {
    distill_rpc::AssetQuery {
        uuid: query.uuid,
        bundle_path: query.bundle_path,
        local_id: query.local_id,
        bundle_uuid: query.bundle_uuid,
        authored_type: query.authored_type,
        terminal_type: query.terminal_type,
        tag: query.tag.map(|tag| TagSelector {
            tag: tag.tag,
            value: tag.value,
        }),
        path_prefix: query.path_prefix,
        path_glob: query.path_glob,
        authoring_only: query.authoring_only,
    }
}

fn rpc_success<T>(result: RpcResult<T>) -> Result<T, PackBuildError> {
    match result {
        RpcResult::Success(value) => Ok(value),
        RpcResult::ReconnectRequired { reason } => Err(PackBuildError::ReconnectRequired(reason)),
        RpcResult::ConfigurationPoisoned(poison) => {
            Err(PackBuildError::ConfigurationPoisoned(Box::new(poison)))
        }
        RpcResult::VersionPoisoned(poison) => {
            Err(PackBuildError::VersionPoisoned(Box::new(poison)))
        }
        RpcResult::Failure(error) => Err(PackBuildError::Rpc(Box::new(error))),
    }
}

fn terminal<T>(
    snapshot: &Snapshot,
    event: distill_rpc::TerminalEvent<T>,
) -> Result<T, PackBuildError> {
    if &event.basis != snapshot.basis() {
        return Err(PackBuildError::BasisMismatch);
    }
    Ok(event.value)
}

fn collect_chunks(
    hash: ContentHash,
    mut stream: distill_rpc::ChunkStream,
) -> Result<(Vec<u8>, Vec<Vec<u8>>), PackBuildError> {
    let mut structural = Vec::new();
    let mut blobs = BTreeMap::<u32, Vec<u8>>::new();
    while let Some(chunk) = stream.next_chunk() {
        let output = match chunk.kind {
            ArtifactChunkKind::Structural => &mut structural,
            ArtifactChunkKind::Blob { index } => blobs.entry(index).or_default(),
        };
        if chunk.offset != output.len() as u64 {
            return Err(PackBuildError::ChunkSequence { hash });
        }
        output.extend_from_slice(&chunk.bytes);
    }
    if blobs.keys().copied().ne(0..blobs.len() as u32) {
        return Err(PackBuildError::ChunkSequence { hash });
    }
    Ok((structural, blobs.into_values().collect()))
}

fn build_paths(
    snapshot: &Snapshot,
    pack_session: &PackSession,
    assets: impl IntoIterator<Item = AssetUuid>,
) -> Result<Vec<PathRow>, PackBuildError> {
    let mut paths = Vec::new();
    for asset in assets {
        rpc_success(pack_session.keep_alive())?;
        let entry = match snapshot.entry(asset) {
            RpcResult::Failure(RpcFailure::AssetNotFound { uuid }) if uuid == asset => continue,
            result => rpc_success(result)?,
        };
        let path = entry.normalized_path;
        match terminal(snapshot, rpc_success(snapshot.resolve_path(&path))?)? {
            PathResolveResult::Resolved(primary) if primary == asset => paths.push(PathRow {
                path,
                asset_uuid: asset,
            }),
            PathResolveResult::Resolved(_) | PathResolveResult::Missing => {}
            result => return Err(PackBuildError::Path { path, result }),
        }
    }
    Ok(paths)
}
