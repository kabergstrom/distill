//! DPK1 manifest file and its five authenticated tables (§16).

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};

use crate::archive::{append_trailer, EKey, ObjectLocation, PACK_MAGIC, PACK_VERSION};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackTarget {
    pub os: u8,
    pub arch: u8,
    pub apis: Vec<u8>,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LayoutRegistryRow {
    pub type_uuid: TypeUuid,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LoadPolicyRow {
    pub type_uuid: TypeUuid,
    pub build_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ArchiveRef {
    pub generation: u32,
    pub file_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestAssetRow {
    pub asset_uuid: AssetUuid,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub logical_hash: LogicalHash,
    pub content_hash: ContentHash,
    pub load_deps: Vec<AssetUuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EncodingRow {
    pub content_hash: ContentHash,
    pub blocks: Vec<EKey>,
    pub blobs: Vec<EKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexRow {
    pub ekey: EKey,
    pub location: ObjectLocation,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WireTreeRow {
    pub layout_hash: LayoutHash,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PathRow {
    pub path: String,
    pub asset_uuid: AssetUuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackManifest {
    pub target: PackTarget,
    pub target_def_hash: [u8; 32],
    pub layout_registry: Vec<LayoutRegistryRow>,
    pub load_policy: Vec<LoadPolicyRow>,
    pub archives: Vec<ArchiveRef>,
    pub assets: Vec<ManifestAssetRow>,
    pub encodings: Vec<EncodingRow>,
    pub index: Vec<IndexRow>,
    pub wire_trees: Vec<WireTreeRow>,
    pub paths: Option<Vec<PathRow>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    TooShort,
    FileHash,
    Magic,
    Version,
    Truncated,
    Utf8,
    Duplicate,
    InvalidBool,
    InvalidDirectory,
    UnknownTable,
    MissingTable,
    Unsorted,
    BadDigest,
    BadPath,
    MetadataMismatch,
    TargetMismatch,
    MissingRuntimeType(TypeUuid),
    LayoutMismatch(TypeUuid),
    PolicyMismatch(TypeUuid),
}

pub fn canonicalize(mut manifest: PackManifest) -> Result<PackManifest, ManifestError> {
    manifest.target.apis.sort_unstable();
    manifest.target.apis.dedup();
    sort_unique(&mut manifest.layout_registry, |v| v.type_uuid)?;
    sort_unique(&mut manifest.load_policy, |v| v.type_uuid)?;
    sort_unique(&mut manifest.archives, |v| v.generation)?;
    for row in &mut manifest.assets {
        row.load_deps.sort_unstable();
        row.load_deps.dedup();
    }
    sort_unique(&mut manifest.assets, |v| v.asset_uuid)?;
    sort_unique(&mut manifest.encodings, |v| v.content_hash)?;
    sort_unique(&mut manifest.index, |v| v.ekey)?;
    sort_unique(&mut manifest.wire_trees, |v| v.layout_hash)?;
    if let Some(paths) = &mut manifest.paths {
        for row in paths.iter() {
            distill_build::query::normalize_path(&row.path).map_err(|_| ManifestError::BadPath)?;
        }
        sort_unique(paths, |v| v.path.clone())?;
    }
    Ok(manifest)
}

fn sort_unique<T, K: Ord>(values: &mut [T], key: impl Fn(&T) -> K) -> Result<(), ManifestError> {
    values.sort_by_key(&key);
    if values.windows(2).any(|w| key(&w[0]) == key(&w[1])) {
        return Err(ManifestError::Duplicate);
    }
    Ok(())
}

pub fn encode_manifest(manifest: &PackManifest) -> Result<Vec<u8>, ManifestError> {
    let manifest = canonicalize(manifest.clone())?;
    let target = encode_target(&manifest.target);
    let mut header = Vec::new();
    header.extend_from_slice(&(target.len() as u32).to_le_bytes());
    header.extend_from_slice(&target);
    header.extend_from_slice(&manifest.target_def_hash);
    header.extend_from_slice(&(manifest.layout_registry.len() as u32).to_le_bytes());
    for row in &manifest.layout_registry {
        header.extend_from_slice(&row.type_uuid.0);
        header.extend_from_slice(&row.digest);
    }
    header.extend_from_slice(&layout_registry_digest(&manifest.layout_registry));
    header.extend_from_slice(&(manifest.load_policy.len() as u32).to_le_bytes());
    for row in &manifest.load_policy {
        header.extend_from_slice(&row.type_uuid.0);
        header.push(row.build_only as u8);
    }
    header.extend_from_slice(&load_policy_digest(&manifest.load_policy));
    header.extend_from_slice(&(manifest.archives.len() as u32).to_le_bytes());
    for row in &manifest.archives {
        header.extend_from_slice(&row.generation.to_le_bytes());
        header.extend_from_slice(&row.file_hash);
    }

    let mut tables = vec![
        (1u8, encode_assets(&manifest.assets)),
        (2, encode_encodings(&manifest.encodings)),
        (3, encode_index(&manifest.index)),
        (4, encode_wire_trees(&manifest.wire_trees)),
    ];
    if let Some(paths) = &manifest.paths {
        tables.push((5, encode_paths(paths)));
    }
    let directory_len = 4 + tables.len() * 17;
    let mut offset = 8 + header.len() + directory_len;
    let mut directory = Vec::with_capacity(directory_len);
    directory.extend_from_slice(&(tables.len() as u32).to_le_bytes());
    for (kind, table) in &tables {
        directory.push(*kind);
        directory.extend_from_slice(&(offset as u64).to_le_bytes());
        directory.extend_from_slice(&(table.len() as u64).to_le_bytes());
        offset += table.len();
    }

    let mut out = Vec::with_capacity(offset + 32);
    out.extend_from_slice(&PACK_MAGIC);
    out.extend_from_slice(&PACK_VERSION.to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&directory);
    for (_, table) in tables {
        out.extend_from_slice(&table);
    }
    append_trailer(&mut out);
    Ok(out)
}

pub fn decode_manifest(bytes: &[u8]) -> Result<PackManifest, ManifestError> {
    if bytes.len() < 40 {
        return Err(ManifestError::TooShort);
    }
    let split = bytes.len() - 32;
    if blake3::hash(&bytes[..split]).as_bytes() != &bytes[split..] {
        return Err(ManifestError::FileHash);
    }
    let mut r = Reader {
        bytes: &bytes[..split],
        pos: 0,
    };
    if r.take(4)? != PACK_MAGIC {
        return Err(ManifestError::Magic);
    }
    if r.u32()? != PACK_VERSION {
        return Err(ManifestError::Version);
    }
    let target_len = r.u32()? as usize;
    let target = decode_target(r.take(target_len)?)?;
    let target_def_hash = r.a32()?;
    let layout_count = r.u32()? as usize;
    let mut layout_registry = Vec::with_capacity(layout_count);
    for _ in 0..layout_count {
        layout_registry.push(LayoutRegistryRow {
            type_uuid: TypeUuid(r.a16()?),
            digest: r.a32()?,
        });
    }
    check_sorted(&layout_registry, |v| v.type_uuid)?;
    if r.a32()? != layout_registry_digest(&layout_registry) {
        return Err(ManifestError::BadDigest);
    }
    let policy_count = r.u32()? as usize;
    let mut load_policy = Vec::with_capacity(policy_count);
    for _ in 0..policy_count {
        let type_uuid = TypeUuid(r.a16()?);
        let build_only = match r.u8()? {
            0 => false,
            1 => true,
            _ => return Err(ManifestError::InvalidBool),
        };
        load_policy.push(LoadPolicyRow {
            type_uuid,
            build_only,
        });
    }
    check_sorted(&load_policy, |v| v.type_uuid)?;
    if r.a32()? != load_policy_digest(&load_policy) {
        return Err(ManifestError::BadDigest);
    }
    let archive_count = r.u32()? as usize;
    let mut archives = Vec::with_capacity(archive_count);
    for _ in 0..archive_count {
        archives.push(ArchiveRef {
            generation: r.u32()?,
            file_hash: r.a32()?,
        });
    }
    check_sorted(&archives, |v| v.generation)?;

    let dir_count = r.u32()? as usize;
    if !(4..=5).contains(&dir_count) {
        return Err(ManifestError::InvalidDirectory);
    }
    let mut dirs = Vec::with_capacity(dir_count);
    for _ in 0..dir_count {
        dirs.push((r.u8()?, r.u64()? as usize, r.u64()? as usize));
    }
    let table_start = r.pos;
    let mut expected_kind = 1u8;
    let mut expected_offset = table_start;
    let mut seen = BTreeSet::new();
    for (kind, offset, len) in &dirs {
        if *kind != expected_kind || !seen.insert(*kind) || *offset != expected_offset {
            return Err(ManifestError::InvalidDirectory);
        }
        if *kind > 5 {
            return Err(ManifestError::UnknownTable);
        }
        expected_kind += 1;
        expected_offset = offset.checked_add(*len).ok_or(ManifestError::Truncated)?;
    }
    if !seen.is_superset(&BTreeSet::from([1, 2, 3, 4])) || expected_offset != split {
        return Err(ManifestError::MissingTable);
    }

    let table = |kind: u8| -> Result<&[u8], ManifestError> {
        let (_, offset, len) = dirs
            .iter()
            .find(|v| v.0 == kind)
            .ok_or(ManifestError::MissingTable)?;
        bytes
            .get(*offset..offset + len)
            .ok_or(ManifestError::Truncated)
    };
    let assets = decode_assets(table(1)?)?;
    let encodings = decode_encodings(table(2)?)?;
    let index = decode_index(table(3)?)?;
    let wire_trees = decode_wire_trees(table(4)?)?;
    let paths = if seen.contains(&5) {
        Some(decode_paths(table(5)?)?)
    } else {
        None
    };
    Ok(PackManifest {
        target,
        target_def_hash,
        layout_registry,
        load_policy,
        archives,
        assets,
        encodings,
        index,
        wire_trees,
        paths,
    })
}

fn encode_target(target: &PackTarget) -> Vec<u8> {
    let mut out = vec![target.os, target.arch];
    let mut apis = target.apis.clone();
    apis.sort_unstable();
    apis.dedup();
    out.extend_from_slice(&(apis.len() as u32).to_le_bytes());
    out.extend_from_slice(&apis);
    out.extend_from_slice(&(target.options.len() as u32).to_le_bytes());
    for (key, value) in &target.options {
        string(&mut out, key);
        string(&mut out, value);
    }
    out
}

fn decode_target(bytes: &[u8]) -> Result<PackTarget, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let os = r.u8()?;
    let arch = r.u8()?;
    let count = r.u32()? as usize;
    let mut apis = r.take(count)?.to_vec();
    if apis.windows(2).any(|w| w[0] >= w[1]) {
        return Err(ManifestError::Unsorted);
    }
    let options_count = r.u32()? as usize;
    let mut options = BTreeMap::new();
    for _ in 0..options_count {
        let key = r.string()?;
        let value = r.string()?;
        if options.insert(key, value).is_some() {
            return Err(ManifestError::Duplicate);
        }
    }
    if r.pos != bytes.len() {
        return Err(ManifestError::InvalidDirectory);
    }
    Ok(PackTarget {
        os,
        arch,
        apis: std::mem::take(&mut apis),
        options,
    })
}

fn table_start(out: &mut Vec<u8>, count: usize) {
    out.extend_from_slice(&(count as u32).to_le_bytes());
}
fn encode_assets(rows: &[ManifestAssetRow]) -> Vec<u8> {
    let mut out = Vec::new();
    table_start(&mut out, rows.len());
    for row in rows {
        out.extend_from_slice(&row.asset_uuid.0);
        out.extend_from_slice(&row.authored_type.0);
        out.extend_from_slice(&row.terminal_type.0);
        out.extend_from_slice(&row.logical_hash.0);
        out.extend_from_slice(&row.content_hash.0);
        out.extend_from_slice(&(row.load_deps.len() as u32).to_le_bytes());
        for dep in &row.load_deps {
            out.extend_from_slice(&dep.0);
        }
    }
    out
}
fn encode_encodings(rows: &[EncodingRow]) -> Vec<u8> {
    let mut out = Vec::new();
    table_start(&mut out, rows.len());
    for row in rows {
        out.extend_from_slice(&row.content_hash.0);
        table_start(&mut out, row.blocks.len());
        for v in &row.blocks {
            out.extend_from_slice(&v.0);
        }
        table_start(&mut out, row.blobs.len());
        for v in &row.blobs {
            out.extend_from_slice(&v.0);
        }
    }
    out
}
fn encode_index(rows: &[IndexRow]) -> Vec<u8> {
    let mut out = Vec::new();
    table_start(&mut out, rows.len());
    for row in rows {
        out.extend_from_slice(&row.ekey.0);
        out.extend_from_slice(&row.location.generation.to_le_bytes());
        out.extend_from_slice(&row.location.offset.to_le_bytes());
        out.extend_from_slice(&row.location.len.to_le_bytes());
    }
    out
}
fn encode_wire_trees(rows: &[WireTreeRow]) -> Vec<u8> {
    let mut out = Vec::new();
    table_start(&mut out, rows.len());
    for row in rows {
        out.extend_from_slice(&row.layout_hash.0);
        table_start(&mut out, row.bytes.len());
        out.extend_from_slice(&row.bytes);
    }
    out
}
fn encode_paths(rows: &[PathRow]) -> Vec<u8> {
    let mut out = Vec::new();
    table_start(&mut out, rows.len());
    for row in rows {
        string(&mut out, &row.path);
        out.extend_from_slice(&row.asset_uuid.0);
    }
    out
}

fn decode_assets(bytes: &[u8]) -> Result<Vec<ManifestAssetRow>, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let count = r.u32()? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let asset_uuid = AssetUuid(r.a16()?);
        let authored_type = TypeUuid(r.a16()?);
        let terminal_type = TypeUuid(r.a16()?);
        let logical_hash = LogicalHash(r.a32()?);
        let content_hash = ContentHash(r.a32()?);
        let n = r.u32()? as usize;
        let mut load_deps = Vec::with_capacity(n);
        for _ in 0..n {
            load_deps.push(AssetUuid(r.a16()?));
        }
        check_strict(&load_deps)?;
        rows.push(ManifestAssetRow {
            asset_uuid,
            authored_type,
            terminal_type,
            logical_hash,
            content_hash,
            load_deps,
        });
    }
    finish_table(r, rows, |v| v.asset_uuid)
}
fn decode_encodings(bytes: &[u8]) -> Result<Vec<EncodingRow>, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let count = r.u32()? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let content_hash = ContentHash(r.a32()?);
        let bn = r.u32()? as usize;
        let mut blocks = Vec::with_capacity(bn);
        for _ in 0..bn {
            blocks.push(EKey(r.a32()?));
        }
        let nn = r.u32()? as usize;
        let mut blobs = Vec::with_capacity(nn);
        for _ in 0..nn {
            blobs.push(EKey(r.a32()?));
        }
        rows.push(EncodingRow {
            content_hash,
            blocks,
            blobs,
        });
    }
    finish_table(r, rows, |v| v.content_hash)
}
fn decode_index(bytes: &[u8]) -> Result<Vec<IndexRow>, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let count = r.u32()? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        rows.push(IndexRow {
            ekey: EKey(r.a32()?),
            location: ObjectLocation {
                generation: r.u32()?,
                offset: r.u64()?,
                len: r.u64()?,
            },
        });
    }
    finish_table(r, rows, |v| v.ekey)
}
fn decode_wire_trees(bytes: &[u8]) -> Result<Vec<WireTreeRow>, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let count = r.u32()? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let layout_hash = LayoutHash(r.a32()?);
        let len = r.u32()? as usize;
        rows.push(WireTreeRow {
            layout_hash,
            bytes: r.take(len)?.to_vec(),
        });
    }
    finish_table(r, rows, |v| v.layout_hash)
}
fn decode_paths(bytes: &[u8]) -> Result<Vec<PathRow>, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let count = r.u32()? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let path = r.string()?;
        distill_build::query::normalize_path(&path).map_err(|_| ManifestError::BadPath)?;
        rows.push(PathRow {
            path,
            asset_uuid: AssetUuid(r.a16()?),
        });
    }
    finish_table(r, rows, |v| v.path.clone())
}
fn finish_table<T, K: Ord>(
    r: Reader<'_>,
    rows: Vec<T>,
    key: impl Fn(&T) -> K,
) -> Result<Vec<T>, ManifestError> {
    if r.pos != r.bytes.len() {
        return Err(ManifestError::InvalidDirectory);
    }
    check_sorted(&rows, key)?;
    Ok(rows)
}
fn check_strict<T: Ord>(v: &[T]) -> Result<(), ManifestError> {
    if v.windows(2).any(|w| w[0] >= w[1]) {
        Err(ManifestError::Unsorted)
    } else {
        Ok(())
    }
}
fn check_sorted<T, K: Ord>(v: &[T], key: impl Fn(&T) -> K) -> Result<(), ManifestError> {
    if v.windows(2).any(|w| key(&w[0]) >= key(&w[1])) {
        Err(ManifestError::Unsorted)
    } else {
        Ok(())
    }
}

fn string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}
pub fn layout_registry_digest(rows: &[LayoutRegistryRow]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"DSLA");
    h.update(&[1]);
    h.update(&(rows.len() as u32).to_le_bytes());
    for r in rows {
        h.update(&r.type_uuid.0);
        h.update(&r.digest);
    }
    *h.finalize().as_bytes()
}
pub fn load_policy_digest(rows: &[LoadPolicyRow]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"DSLP");
    h.update(&[1]);
    h.update(&(rows.len() as u32).to_le_bytes());
    for r in rows {
        h.update(&r.type_uuid.0);
        h.update(&[r.build_only as u8]);
    }
    *h.finalize().as_bytes()
}
pub fn manifest_hash(bytes: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"DSPM");
    h.update(&[1]);
    h.update(bytes);
    *h.finalize().as_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactMetadata {
    pub asset_uuid: AssetUuid,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub logical_hash: LogicalHash,
    pub load_deps: Vec<AssetUuid>,
}
pub fn verify_artifact_metadata(
    row: &ManifestAssetRow,
    header: &ArtifactMetadata,
) -> Result<(), ManifestError> {
    if row.asset_uuid == header.asset_uuid
        && row.authored_type == header.authored_type
        && row.terminal_type == header.terminal_type
        && row.logical_hash == header.logical_hash
        && row.load_deps == header.load_deps
    {
        Ok(())
    } else {
        Err(ManifestError::MetadataMismatch)
    }
}
pub fn verify_attestation(
    manifest: &PackManifest,
    layouts: &BTreeMap<TypeUuid, [u8; 32]>,
    policy: &BTreeMap<TypeUuid, bool>,
    target_def_hash: [u8; 32],
) -> Result<(), ManifestError> {
    if manifest.target_def_hash != target_def_hash {
        return Err(ManifestError::TargetMismatch);
    }
    for row in &manifest.layout_registry {
        match layouts.get(&row.type_uuid) {
            None => return Err(ManifestError::MissingRuntimeType(row.type_uuid)),
            Some(v) if v != &row.digest => {
                return Err(ManifestError::LayoutMismatch(row.type_uuid))
            }
            _ => {}
        }
    }
    for row in &manifest.load_policy {
        match policy.get(&row.type_uuid) {
            None => return Err(ManifestError::MissingRuntimeType(row.type_uuid)),
            Some(v) if v != &row.build_only => {
                return Err(ManifestError::PolicyMismatch(row.type_uuid))
            }
            _ => {}
        }
    }
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ManifestError> {
        let end = self.pos.checked_add(n).ok_or(ManifestError::Truncated)?;
        let v = self
            .bytes
            .get(self.pos..end)
            .ok_or(ManifestError::Truncated)?;
        self.pos = end;
        Ok(v)
    }
    fn u8(&mut self) -> Result<u8, ManifestError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, ManifestError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ManifestError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn a16(&mut self) -> Result<[u8; 16], ManifestError> {
        Ok(self.take(16)?.try_into().unwrap())
    }
    fn a32(&mut self) -> Result<[u8; 32], ManifestError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn string(&mut self) -> Result<String, ManifestError> {
        let n = self.u32()? as usize;
        Ok(std::str::from_utf8(self.take(n)?)
            .map_err(|_| ManifestError::Utf8)?
            .to_owned())
    }
}
