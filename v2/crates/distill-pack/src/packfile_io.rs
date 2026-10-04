//! Frozen pack realization of §15's `LoaderIO` boundary.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use distill_core::id::{AssetUuid, ContentHash, LayoutHash};
use distill_loader::{
    AssetPath, FetchedArtifact, IoBasis, IoEvent, LoaderIO, ManifestHash, PathResolveResult, ReqId,
    ResolveResult, RuntimeTarget as LoaderRuntimeTarget,
};
use distill_wire::artifact::{parse_artifact_parts, ArtifactError};
use distill_wire::dswl::{decode_dswl, dswl_hash};
use distill_wire::exec::Blob;
use unicode_normalization::UnicodeNormalization;

use crate::activation::{archive_filename, manifest_filename, read_current, PointerError};
use crate::archive::{
    decode_structural, scan_archive, ArchiveError, ArchiveObjectKind, EKey, ScannedArchive,
};
use crate::manifest::{
    decode_manifest, manifest_hash, verify_artifact_header, verify_expected_terminals,
    ManifestAssetRow, ManifestError, PackManifest,
};

#[derive(Debug, Clone)]
pub struct RuntimeTarget {
    pub target: String,
    pub target_def_hash: [u8; 32],
}

#[derive(Debug)]
pub enum MountError {
    Pointer(PointerError),
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Manifest(ManifestError),
    ManifestFileHash,
    Archive(ArchiveError),
    TargetMismatch,
    MissingArchive(u32),
    DuplicateArchive(u32),
    ArchiveFileHash(u32),
    IndexMismatch(EKey),
    MissingObject(EKey),
    ObjectKind(EKey),
    Artifact(ArtifactError),
    ContentHash(ContentHash),
    MissingEncoding(ContentHash),
    UnreferencedEncoding(ContentHash),
    DuplicateContentHash(ContentHash),
    MissingWireTree(LayoutHash),
    WireTree(LayoutHash),
}

impl From<ManifestError> for MountError {
    fn from(value: ManifestError) -> Self {
        Self::Manifest(value)
    }
}

impl From<PointerError> for MountError {
    fn from(value: PointerError) -> Self {
        Self::Pointer(value)
    }
}

impl From<ArchiveError> for MountError {
    fn from(value: ArchiveError) -> Self {
        Self::Archive(value)
    }
}

impl From<ArtifactError> for MountError {
    fn from(value: ArtifactError) -> Self {
        Self::Artifact(value)
    }
}

pub struct PackfileIO {
    manifest: PackManifest,
    archives: BTreeMap<u32, MountedArchive>,
    basis: IoBasis,
    events: VecDeque<IoEvent>,
}

type ArchiveBacking = Arc<dyn AsRef<[u8]> + Send + Sync>;

struct MountedArchive {
    scanned: ScannedArchive,
    backing: ArchiveBacking,
}

impl MountedArchive {
    fn payload_range(&self, key: EKey) -> Result<(usize, usize), MountError> {
        let object = self
            .scanned
            .objects
            .get(&key)
            .ok_or(MountError::MissingObject(key))?;
        let offset =
            usize::try_from(object.location.offset).map_err(|_| MountError::IndexMismatch(key))?;
        let len =
            usize::try_from(object.location.len).map_err(|_| MountError::IndexMismatch(key))?;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= self.backing.as_ref().as_ref().len())
            .ok_or(MountError::IndexMismatch(key))?;
        let _ = end;
        Ok((offset, len))
    }

    fn payload(&self, key: EKey) -> Result<&[u8], MountError> {
        let (offset, len) = self.payload_range(key)?;
        Ok(&self.backing.as_ref().as_ref()[offset..offset + len])
    }
}

impl PackfileIO {
    /// Mount the pack selected by `pack.current`. The pointer authenticates
    /// the exact manifest name; that manifest in turn selects every immutable
    /// hash-named archive. Archive mappings remain alive through returned
    /// [`Blob`] values, so blob extents are borrowed without copying.
    pub fn mount_current(directory: &Path, runtime: &RuntimeTarget) -> Result<Self, MountError> {
        let expected_manifest_hash = read_current(directory)?;
        let manifest_path = directory.join(manifest_filename(expected_manifest_hash));
        let manifest = map_file(&manifest_path)?;
        let manifest_bytes = manifest.as_ref();
        if manifest_hash(manifest_bytes) != expected_manifest_hash {
            return Err(MountError::ManifestFileHash);
        }
        let manifest = decode_manifest(manifest_bytes)?;
        let archives = manifest
            .archives
            .iter()
            .map(|archive| {
                map_file(&directory.join(archive_filename(archive.file_hash)))
                    .map(|mapping| mapping as ArchiveBacking)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::mount_backings(manifest, expected_manifest_hash, archives, runtime)
    }

    fn mount_backings(
        manifest: PackManifest,
        manifest_hash: [u8; 32],
        archive_files: Vec<ArchiveBacking>,
        runtime: &RuntimeTarget,
    ) -> Result<Self, MountError> {
        Self::verify_runtime(&manifest, runtime)?;

        let mut archives = BTreeMap::new();
        let mut raw_hashes = BTreeMap::new();
        for backing in archive_files {
            let bytes = backing.as_ref().as_ref();
            let scanned = scan_archive(bytes)?;
            let generation = scanned.generation;
            let file_hash = *blake3::hash(bytes).as_bytes();
            if archives
                .insert(generation, MountedArchive { scanned, backing })
                .is_some()
            {
                return Err(MountError::DuplicateArchive(generation));
            }
            raw_hashes.insert(generation, file_hash);
        }
        for archive_ref in &manifest.archives {
            if raw_hashes.get(&archive_ref.generation) != Some(&archive_ref.file_hash) {
                return Err(if raw_hashes.contains_key(&archive_ref.generation) {
                    MountError::ArchiveFileHash(archive_ref.generation)
                } else {
                    MountError::MissingArchive(archive_ref.generation)
                });
            }
        }

        verify_index(&manifest, &archives)?;
        let referenced: std::collections::BTreeSet<_> =
            manifest.assets.iter().map(|row| row.content_hash).collect();
        if referenced.len() != manifest.assets.len() {
            let mut seen = std::collections::BTreeSet::new();
            let duplicate = manifest
                .assets
                .iter()
                .map(|row| row.content_hash)
                .find(|hash| !seen.insert(*hash))
                .expect("set cardinality proved a duplicate content hash");
            return Err(MountError::DuplicateContentHash(duplicate));
        }
        let encoded = manifest
            .encodings
            .iter()
            .map(|row| row.content_hash)
            .collect::<std::collections::BTreeSet<_>>();
        if let Some(missing) = referenced.difference(&encoded).next() {
            return Err(MountError::MissingEncoding(*missing));
        }
        for encoding in &manifest.encodings {
            if !referenced.contains(&encoding.content_hash) {
                return Err(MountError::UnreferencedEncoding(encoding.content_hash));
            }
        }
        for row in &manifest.wire_trees {
            let wire =
                decode_dswl(&row.bytes).map_err(|_| MountError::WireTree(row.layout_hash))?;
            if dswl_hash(&wire).map_err(|_| MountError::WireTree(row.layout_hash))?
                != row.layout_hash
            {
                return Err(MountError::WireTree(row.layout_hash));
            }
        }
        let mut terminal_types = BTreeMap::new();
        for row in &manifest.assets {
            let decoded = decode_fetched(&manifest, &archives, row)?;
            terminal_types.insert(row.asset_uuid, decoded.terminal_type);
        }
        verify_expected_terminals(&manifest, &terminal_types)?;
        let basis = IoBasis::Pack {
            manifest: ManifestHash(manifest_hash),
        };
        Ok(Self {
            manifest,
            archives,
            basis,
            events: VecDeque::new(),
        })
    }

    fn verify_runtime(manifest: &PackManifest, runtime: &RuntimeTarget) -> Result<(), MountError> {
        if manifest.target.name != runtime.target.nfc().collect::<String>()
            || manifest.target_def_hash != runtime.target_def_hash
        {
            return Err(MountError::TargetMismatch);
        }
        Ok(())
    }

    pub fn manifest(&self) -> &PackManifest {
        &self.manifest
    }

    fn basis_matches(&self, basis: &IoBasis) -> bool {
        basis == &self.basis
    }
}

fn verify_index(
    manifest: &PackManifest,
    archives: &BTreeMap<u32, MountedArchive>,
) -> Result<(), MountError> {
    let index: BTreeMap<_, _> = manifest
        .index
        .iter()
        .map(|row| (row.ekey, row.location))
        .collect();
    for (key, location) in &index {
        let object = archives
            .get(&location.generation)
            .and_then(|archive| archive.scanned.objects.get(key))
            .ok_or(MountError::MissingObject(*key))?;
        if object.location != *location {
            return Err(MountError::IndexMismatch(*key));
        }
    }
    for encoding in &manifest.encodings {
        for key in &encoding.blocks {
            let location = index.get(key).ok_or(MountError::MissingObject(*key))?;
            let object = archives
                .get(&location.generation)
                .and_then(|archive| archive.scanned.objects.get(key))
                .ok_or(MountError::MissingObject(*key))?;
            if object.kind != ArchiveObjectKind::Structural {
                return Err(MountError::ObjectKind(*key));
            }
        }
        for key in &encoding.blobs {
            let location = index.get(key).ok_or(MountError::MissingObject(*key))?;
            let object = archives
                .get(&location.generation)
                .and_then(|archive| archive.scanned.objects.get(key))
                .ok_or(MountError::MissingObject(*key))?;
            if object.kind != ArchiveObjectKind::Blob {
                return Err(MountError::ObjectKind(*key));
            }
        }
    }
    Ok(())
}

struct DecodedFetched {
    artifact: FetchedArtifact,
    terminal_type: distill_core::id::TypeUuid,
}

fn decode_fetched(
    manifest: &PackManifest,
    archives: &BTreeMap<u32, MountedArchive>,
    row: &ManifestAssetRow,
) -> Result<DecodedFetched, MountError> {
    let content_hash = row.content_hash;
    let encoding_index = manifest
        .encodings
        .binary_search_by_key(&content_hash, |row| row.content_hash)
        .map_err(|_| MountError::MissingEncoding(content_hash))?;
    let encoding = &manifest.encodings[encoding_index];
    let index: BTreeMap<_, _> = manifest
        .index
        .iter()
        .map(|row| (row.ekey, row.location))
        .collect();
    let object = |key: EKey| {
        let location = index.get(&key).ok_or(MountError::MissingObject(key))?;
        let archive = archives
            .get(&location.generation)
            .ok_or(MountError::MissingObject(key))?;
        let object = archive
            .scanned
            .objects
            .get(&key)
            .ok_or(MountError::MissingObject(key))?;
        Ok::<_, MountError>((archive, object))
    };

    let mut structural = Vec::new();
    for key in &encoding.blocks {
        let (archive, value) = object(*key)?;
        if value.kind != ArchiveObjectKind::Structural {
            return Err(MountError::ObjectKind(*key));
        }
        let raw = decode_structural(archive.payload(*key)?, value.raw_len)?;
        structural.extend_from_slice(&raw);
    }
    let mut blob_ranges = Vec::new();
    for key in &encoding.blobs {
        let (archive, value) = object(*key)?;
        if value.kind != ArchiveObjectKind::Blob {
            return Err(MountError::ObjectKind(*key));
        }
        let (offset, len) = archive.payload_range(*key)?;
        blob_ranges.push((Arc::clone(&archive.backing), offset, len));
    }
    let blob_refs = encoding
        .blobs
        .iter()
        .map(|key| {
            let (archive, _) = object(*key)?;
            archive.payload(*key)
        })
        .collect::<Result<Vec<_>, MountError>>()?;
    let parts = parse_artifact_parts(&structural, &blob_refs)?;
    if parts.content_hash != content_hash {
        return Err(MountError::ContentHash(content_hash));
    }
    let wire_index = manifest
        .wire_trees
        .binary_search_by_key(&parts.layout_hash, |row| row.layout_hash)
        .map_err(|_| MountError::MissingWireTree(parts.layout_hash))?;
    let wire_layout = manifest.wire_trees[wire_index].bytes.clone();
    let wire = decode_dswl(&wire_layout).map_err(|_| MountError::WireTree(parts.layout_hash))?;
    if dswl_hash(&wire).map_err(|_| MountError::WireTree(parts.layout_hash))? != parts.layout_hash {
        return Err(MountError::WireTree(parts.layout_hash));
    }
    verify_artifact_header(row, parts.asset_uuid, &parts.load_deps)?;
    let terminal_type = parts.terminal_type;
    let load_edges = row
        .load_deps
        .iter()
        .map(|edge| distill_rpc::ServedLoadEdge {
            asset: edge.asset_uuid,
            expected_terminal: edge.expected_terminal,
        })
        .collect();
    let blobs = blob_ranges
        .into_iter()
        .map(|(backing, offset, len)| Blob::new(backing, offset, len))
        .collect();
    let wire_len = wire_layout.len();
    let wire_backing: Arc<dyn AsRef<[u8]> + Send + Sync> = Arc::new(wire_layout);
    Ok(DecodedFetched {
        artifact: FetchedArtifact {
            structural: Arc::from(structural),
            blobs,
            load_edges,
            wire_layout: Blob::new(wire_backing, 0, wire_len),
        },
        terminal_type,
    })
}

fn map_file(path: &Path) -> Result<Arc<memmap2::Mmap>, MountError> {
    let file = std::fs::File::open(path).map_err(|source| MountError::Io {
        path: path.to_owned(),
        source,
    })?;
    // Pack files are immutable, content-addressed publications. Mapping the
    // retained file descriptor therefore preserves the bytes authenticated by
    // mount while allowing Blob to retain an Arc-backed range.
    let mapping =
        unsafe { memmap2::MmapOptions::new().map(&file) }.map_err(|source| MountError::Io {
            path: path.to_owned(),
            source,
        })?;
    Ok(Arc::new(mapping))
}

impl LoaderIO for PackfileIO {
    fn bind_target(&mut self, target: LoaderRuntimeTarget) {
        if self.manifest.target_def_hash == target.target_definition_hash {
            self.events.push_back(IoEvent::TargetBound {
                target,
                basis: self.basis.clone(),
            });
        } else {
            self.events.push_back(IoEvent::TargetRejected {
                message: "pack target-definition hash differs from the module target".into(),
            });
        }
    }

    fn begin_sweep(&mut self) -> IoBasis {
        self.basis.clone()
    }

    /// Answers are computed when requested, so cancelling drops the queued
    /// answers to requests under `basis`.
    fn end_sweep(&mut self, basis: &IoBasis) {
        self.events.retain(|event| match event {
            IoEvent::Resolved { basis: answered, .. }
            | IoEvent::PathResolved { basis: answered, .. }
            | IoEvent::Fetched { basis: answered, .. }
            | IoEvent::RequestError { basis: answered, .. }
            | IoEvent::SnapshotExpired { basis: answered, .. } => answered != basis,
            _ => true,
        });
    }

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        if !self.basis_matches(basis) {
            self.events.push_back(IoEvent::RequestError {
                req,
                message: "pack remounted: stale manifest basis".into(),
                basis: self.basis.clone(),
            });
            return;
        }
        let result = self
            .manifest
            .assets
            .binary_search_by_key(&uuid, |row| row.asset_uuid)
            .ok()
            .map(|index| ResolveResult::Built {
                content_hash: self.manifest.assets[index].content_hash,
            })
            .unwrap_or(ResolveResult::Missing);
        self.events.push_back(IoEvent::Resolved {
            req,
            uuid,
            result,
            basis: self.basis.clone(),
        });
    }

    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis) {
        if !self.basis_matches(basis) {
            self.events.push_back(IoEvent::RequestError {
                req,
                message: "pack remounted: stale manifest basis".into(),
                basis: self.basis.clone(),
            });
            return;
        }
        let result = self
            .manifest
            .assets
            .iter()
            .find(|row| row.content_hash == content_hash)
            .ok_or(MountError::MissingEncoding(content_hash))
            .and_then(|row| decode_fetched(&self.manifest, &self.archives, row));
        match result {
            Ok(decoded) => self.events.push_back(IoEvent::Fetched {
                req,
                content_hash,
                artifact: decoded.artifact,
                basis: self.basis.clone(),
            }),
            Err(error) => self.events.push_back(IoEvent::RequestError {
                req,
                message: format!("mounted pack artifact integrity failure: {error:?}"),
                basis: self.basis.clone(),
            }),
        }
    }

    fn resolve_path(&mut self, req: ReqId, path: &AssetPath, basis: &IoBasis) {
        let result = if !self.basis_matches(basis) {
            PathResolveResult::Failed {
                error: "pack remounted: stale manifest basis".into(),
            }
        } else if let Some(paths) = &self.manifest.paths {
            let key = distill_build::query::normalize_path(&path.path).and_then(|normalized| {
                let name = path
                    .name
                    .as_deref()
                    .map(distill_build::query::normalize_identifier)
                    .transpose()?;
                Ok((normalized, name))
            });
            match key {
                Ok((normalized, name)) => paths
                    .binary_search_by(|row| {
                        (row.path.as_str(), row.name.as_deref())
                            .cmp(&(normalized.as_str(), name.as_deref()))
                    })
                    .ok()
                    .map(|index| PathResolveResult::Resolved(paths[index].asset_uuid))
                    .unwrap_or(PathResolveResult::Missing),
                Err(error) => PathResolveResult::Failed {
                    error: error.to_string(),
                },
            }
        } else {
            PathResolveResult::Unsupported
        };
        self.events.push_back(IoEvent::PathResolved {
            req,
            path: path.clone(),
            result,
            basis: self.basis.clone(),
        });
    }

    fn subscribe(&mut self, _uuid: AssetUuid) {}
    fn unsubscribe(&mut self, _uuid: AssetUuid) {}
    fn subscribe_path(&mut self, _path: &str) {}
    fn unsubscribe_path(&mut self, _path: &str) {}

    fn poll(&mut self) -> Vec<IoEvent> {
        self.events.drain(..).collect()
    }
}
