//! Frozen pack realization of §15's `LoaderIO` boundary.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_loader::{
    FetchedArtifact, IoBasis, IoEvent, LoadPolicyAttestation, LoadPolicyRow, LoaderIO,
    ManifestHash, PathResolveResult, ReqId, ResolveResult,
};
use distill_wire::artifact::{parse_artifact_parts, ArtifactError};
use distill_wire::dswl::{decode_dswl, dswl_hash};
use distill_wire::exec::Blob;

use crate::archive::{decode_archive, ArchiveError, ArchiveObjectKind, DecodedArchive, EKey};
use crate::manifest::{
    decode_manifest, manifest_hash, verify_artifact_metadata, verify_attestation, ArtifactMetadata,
    ManifestError, PackManifest, PackTarget,
};

#[derive(Debug, Clone)]
pub struct RuntimeAttestation {
    pub target: PackTarget,
    pub target_def_hash: [u8; 32],
    pub layouts: BTreeMap<TypeUuid, [u8; 32]>,
    pub load_policy: BTreeMap<TypeUuid, bool>,
}

#[derive(Debug)]
pub enum MountError {
    Manifest(ManifestError),
    Archive(ArchiveError),
    TargetMismatch,
    MissingArchive(u32),
    DuplicateArchive(u32),
    ArchiveFileHash(u32),
    IndexMismatch(EKey),
    MissingObject(EKey),
    ObjectKind(EKey),
    LoadPolicy,
    Artifact(ArtifactError),
    ContentHash(ContentHash),
    MissingEncoding(ContentHash),
    UnreferencedEncoding(ContentHash),
    MissingWireTree(LayoutHash),
    WireTree(LayoutHash),
}

impl From<ManifestError> for MountError {
    fn from(value: ManifestError) -> Self {
        Self::Manifest(value)
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
    archives: BTreeMap<u32, DecodedArchive>,
    basis: IoBasis,
    events: VecDeque<IoEvent>,
}

impl PackfileIO {
    pub fn mount(
        manifest_bytes: &[u8],
        archive_files: Vec<Vec<u8>>,
        runtime: &RuntimeAttestation,
    ) -> Result<Self, MountError> {
        let manifest = decode_manifest(manifest_bytes)?;
        Self::verify_runtime(&manifest, runtime)?;

        let mut archives = BTreeMap::new();
        let mut raw_hashes = BTreeMap::new();
        for bytes in archive_files {
            let decoded = decode_archive(&bytes)?;
            let generation = decoded.generation;
            let trailer: [u8; 32] = bytes[bytes.len() - 32..]
                .try_into()
                .expect("a decoded archive has a trailer");
            if archives.insert(generation, decoded).is_some() {
                return Err(MountError::DuplicateArchive(generation));
            }
            raw_hashes.insert(generation, trailer);
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
        for encoding in &manifest.encodings {
            if !referenced.contains(&encoding.content_hash) {
                return Err(MountError::UnreferencedEncoding(encoding.content_hash));
            }
        }
        for row in &manifest.assets {
            let (_, metadata) = decode_fetched(&manifest, &archives, row.content_hash)?;
            verify_artifact_metadata(row, &metadata)?;
        }
        let rows = manifest
            .load_policy
            .iter()
            .map(|row| LoadPolicyRow {
                type_uuid: row.type_uuid,
                build_only: row.build_only,
            })
            .collect();
        let load_policy =
            Arc::new(LoadPolicyAttestation::from_rows(rows).map_err(|_| MountError::LoadPolicy)?);
        let basis = IoBasis::Pack {
            manifest: ManifestHash(manifest_hash(manifest_bytes)),
            load_policy,
        };
        Ok(Self {
            manifest,
            archives,
            basis,
            events: VecDeque::new(),
        })
    }

    pub fn reattest(&self, runtime: &RuntimeAttestation) -> Result<(), MountError> {
        Self::verify_runtime(&self.manifest, runtime)
    }

    fn verify_runtime(
        manifest: &PackManifest,
        runtime: &RuntimeAttestation,
    ) -> Result<(), MountError> {
        if manifest.target != runtime.target {
            return Err(MountError::TargetMismatch);
        }
        verify_attestation(
            manifest,
            &runtime.layouts,
            &runtime.load_policy,
            runtime.target_def_hash,
        )?;
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
    archives: &BTreeMap<u32, DecodedArchive>,
) -> Result<(), MountError> {
    let index: BTreeMap<_, _> = manifest
        .index
        .iter()
        .map(|row| (row.ekey, row.location))
        .collect();
    for (key, location) in &index {
        let object = archives
            .get(&location.generation)
            .and_then(|archive| archive.objects.get(key))
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
                .and_then(|archive| archive.objects.get(key))
                .ok_or(MountError::MissingObject(*key))?;
            if object.kind != ArchiveObjectKind::Structural {
                return Err(MountError::ObjectKind(*key));
            }
        }
        for key in &encoding.blobs {
            let location = index.get(key).ok_or(MountError::MissingObject(*key))?;
            let object = archives
                .get(&location.generation)
                .and_then(|archive| archive.objects.get(key))
                .ok_or(MountError::MissingObject(*key))?;
            if object.kind != ArchiveObjectKind::Blob {
                return Err(MountError::ObjectKind(*key));
            }
        }
    }
    Ok(())
}

fn decode_fetched(
    manifest: &PackManifest,
    archives: &BTreeMap<u32, DecodedArchive>,
    content_hash: ContentHash,
) -> Result<(FetchedArtifact, ArtifactMetadata), MountError> {
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
        archives
            .get(&location.generation)
            .and_then(|archive| archive.objects.get(&key))
            .ok_or(MountError::MissingObject(key))
    };

    let mut structural = Vec::new();
    for key in &encoding.blocks {
        let value = object(*key)?;
        if value.kind != ArchiveObjectKind::Structural {
            return Err(MountError::ObjectKind(*key));
        }
        structural.extend_from_slice(&value.raw);
    }
    let mut blob_bytes = Vec::new();
    for key in &encoding.blobs {
        let value = object(*key)?;
        if value.kind != ArchiveObjectKind::Blob {
            return Err(MountError::ObjectKind(*key));
        }
        blob_bytes.push(value.raw.clone());
    }
    let blob_refs: Vec<_> = blob_bytes.iter().map(Vec::as_slice).collect();
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
    let metadata = ArtifactMetadata {
        asset_uuid: parts.asset_uuid,
        authored_type: parts.authored_type,
        terminal_type: parts.terminal_type,
        logical_hash: parts.logical_hash,
        load_deps: parts.load_deps.clone(),
    };
    let blobs = blob_bytes
        .into_iter()
        .map(|raw| {
            let len = raw.len();
            let backing: Arc<dyn AsRef<[u8]> + Send + Sync> = Arc::new(raw);
            Blob::new(backing, 0, len)
        })
        .collect();
    Ok((
        FetchedArtifact {
            structural: Arc::from(structural),
            blobs,
            wire_layout: Arc::from(wire_layout),
        },
        metadata,
    ))
}

impl LoaderIO for PackfileIO {
    fn begin_sweep(&mut self) -> IoBasis {
        self.basis.clone()
    }

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        if !self.basis_matches(basis) {
            self.events.push_back(IoEvent::IoError {
                req: Some(req),
                message: "pack remounted: stale manifest basis".into(),
                basis: Some(self.basis.clone()),
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
                basis: self.basis.clone(),
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
            self.events.push_back(IoEvent::IoError {
                req: Some(req),
                message: "pack remounted: stale manifest basis".into(),
                basis: Some(self.basis.clone()),
            });
            return;
        }
        let result = decode_fetched(&self.manifest, &self.archives, content_hash)
            .map(|(artifact, _)| artifact);
        match result {
            Ok(artifact) => self.events.push_back(IoEvent::Fetched {
                req,
                content_hash,
                artifact,
                basis: self.basis.clone(),
            }),
            Err(error) => self.events.push_back(IoEvent::IoError {
                req: Some(req),
                message: format!("mounted pack artifact integrity failure: {error:?}"),
                basis: Some(self.basis.clone()),
            }),
        }
    }

    fn resolve_path(&mut self, req: ReqId, path: &str, basis: &IoBasis) {
        let result = if !self.basis_matches(basis) {
            PathResolveResult::Failed {
                error: "pack remounted: stale manifest basis".into(),
            }
        } else if let Some(paths) = &self.manifest.paths {
            paths
                .binary_search_by(|row| row.path.as_str().cmp(path))
                .ok()
                .map(|index| PathResolveResult::Resolved(paths[index].asset_uuid))
                .unwrap_or(PathResolveResult::Missing)
        } else {
            PathResolveResult::Unsupported
        };
        self.events.push_back(IoEvent::PathResolved {
            req,
            path: path.to_owned(),
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
