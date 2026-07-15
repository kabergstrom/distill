//! One-shot construction of a v2 pack from one pinned RPC snapshot.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use distill_build::query::{AssetQuery as BuildAssetQuery, IntakeError};
use distill_build::trace::PackDefinitionControlValue;
use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_rpc::{
    ArtifactChunkKind, ConfigurationPoison, Hub, PathResolveResult, ReconnectReason, ResolveResult,
    RpcFailure, RpcResult, Snapshot, TagSelector, VersionPoison,
};
use distill_wire::artifact::{parse_artifact_parts, ArtifactError};

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
    InvalidWireTree(LayoutHash),
    BuildOnlyType(TypeUuid),
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
    build_only_types: &BTreeSet<TypeUuid>,
    encoder_identity: &str,
    snapshot: &Snapshot,
    hub: &Hub,
) -> Result<PackBuildOutput, PackBuildError> {
    if definition.target != target.name {
        return Err(PackBuildError::TargetMismatch {
            definition: definition.target.clone(),
            requested: target.name.clone(),
        });
    }
    if definition.roots.is_empty() {
        return Err(PackBuildError::NoRoots);
    }
    let mut pending = BTreeSet::new();
    for (index, root) in definition.roots.iter().enumerate() {
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
        for type_uuid in [
            parsed.authored_type,
            parsed.encoded_type,
            parsed.terminal_type,
        ] {
            if build_only_types.contains(&type_uuid) {
                return Err(PackBuildError::BuildOnlyType(type_uuid));
            }
        }
        let terminal_type = parsed.terminal_type;
        let layout_hash = parsed.layout_hash;
        drop(parsed);
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

    let mut wire_trees = Vec::new();
    for layout_hash in artifacts
        .values()
        .map(|artifact| artifact.layout_hash)
        .collect::<BTreeSet<_>>()
    {
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
        Some(build_paths(snapshot, artifacts.keys().copied())?)
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
        target: PackTarget {
            name: target.name.clone(),
        },
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
    build_only_types: &BTreeSet<TypeUuid>,
    encoder_identity: &str,
    snapshot: &Snapshot,
    hub: &Hub,
) -> Result<PackBuildOutput, PackBuildError> {
    let output = build_pack(
        definition,
        target,
        build_only_types,
        encoder_identity,
        snapshot,
        hub,
    )?;
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
    assets: impl IntoIterator<Item = AssetUuid>,
) -> Result<Vec<PathRow>, PackBuildError> {
    let mut paths = Vec::new();
    for asset in assets {
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
