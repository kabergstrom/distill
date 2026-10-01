//! DPK1 manifest file and its five authenticated tables (§16).

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use unicode_normalization::{is_nfc, UnicodeNormalization};

use crate::archive::{append_trailer, EKey, ObjectLocation, PACK_MAGIC, PACK_VERSION};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackTarget {
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ArchiveRef {
    pub generation: u32,
    pub file_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestLoadEdge {
    pub asset_uuid: AssetUuid,
    pub expected_terminal: TypeUuid,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestAssetRow {
    pub asset_uuid: AssetUuid,
    pub content_hash: ContentHash,
    pub load_deps: Vec<ManifestLoadEdge>,
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
    /// `None`: the path's primary asset. `Some`: the asset of that local id
    /// imported at `path` (pack version 3). Rows sort by path, then name,
    /// the primary first.
    pub name: Option<String>,
    pub asset_uuid: AssetUuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackManifest {
    pub target: PackTarget,
    pub target_def_hash: [u8; 32],
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
    InvalidDirectory,
    UnknownTable,
    MissingTable,
    Unsorted,
    BadPath,
    MetadataMismatch,
    TargetMismatch,
    MissingDependency(AssetUuid),
    DependencyTypeMismatch(TypeUuid),
}

pub fn canonicalize(mut manifest: PackManifest) -> Result<PackManifest, ManifestError> {
    manifest.target.name = manifest.target.name.nfc().collect();
    if manifest.target.name.is_empty() || manifest.target.name.contains('\0') {
        return Err(ManifestError::TargetMismatch);
    }
    sort_unique(&mut manifest.archives, |v| v.generation)?;
    for row in &mut manifest.assets {
        row.load_deps.sort_unstable();
        row.load_deps.dedup();
        if row
            .load_deps
            .windows(2)
            .any(|pair| pair[0].asset_uuid == pair[1].asset_uuid)
        {
            return Err(ManifestError::Duplicate);
        }
    }
    sort_unique(&mut manifest.assets, |v| v.asset_uuid)?;
    sort_unique(&mut manifest.encodings, |v| v.content_hash)?;
    sort_unique(&mut manifest.index, |v| v.ekey)?;
    sort_unique(&mut manifest.wire_trees, |v| v.layout_hash)?;
    if let Some(paths) = &mut manifest.paths {
        for row in paths.iter_mut() {
            row.path = distill_build::query::normalize_path(&row.path)
                .map_err(|_| ManifestError::BadPath)?;
            if let Some(name) = &mut row.name {
                *name = distill_build::query::normalize_identifier(name)
                    .map_err(|_| ManifestError::BadPath)?;
            }
        }
        sort_unique(paths, |v| (v.path.clone(), v.name.clone()))?;
    }
    verify_manifest_closure(&manifest)?;
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
    let target_len = usize::try_from(r.u32()?).map_err(|_| ManifestError::Truncated)?;
    let target = decode_target(r.take(target_len)?)?;
    let target_def_hash = r.a32()?;
    let archive_count = r.bounded_count(36)?;
    let mut archives = Vec::with_capacity(archive_count);
    for _ in 0..archive_count {
        archives.push(ArchiveRef {
            generation: r.u32()?,
            file_hash: r.a32()?,
        });
    }
    check_sorted(&archives, |v| v.generation)?;

    let dir_count = r.bounded_count(17)?;
    if !(4..=5).contains(&dir_count) {
        return Err(ManifestError::InvalidDirectory);
    }
    let mut dirs = Vec::with_capacity(dir_count);
    for _ in 0..dir_count {
        dirs.push((
            r.u8()?,
            usize::try_from(r.u64()?).map_err(|_| ManifestError::InvalidDirectory)?,
            usize::try_from(r.u64()?).map_err(|_| ManifestError::InvalidDirectory)?,
        ));
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
    let manifest = PackManifest {
        target,
        target_def_hash,
        archives,
        assets,
        encodings,
        index,
        wire_trees,
        paths,
    };
    verify_manifest_closure(&manifest)?;
    Ok(manifest)
}

fn encode_target(target: &PackTarget) -> Vec<u8> {
    let mut out = Vec::new();
    string(&mut out, &target.name);
    out
}

fn decode_target(bytes: &[u8]) -> Result<PackTarget, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let name = r.string()?;
    if name.is_empty() || name.contains('\0') || !is_nfc(&name) {
        return Err(ManifestError::TargetMismatch);
    }
    if r.pos != bytes.len() {
        return Err(ManifestError::InvalidDirectory);
    }
    Ok(PackTarget { name })
}

fn table_start(out: &mut Vec<u8>, count: usize) {
    out.extend_from_slice(&(count as u32).to_le_bytes());
}
fn encode_assets(rows: &[ManifestAssetRow]) -> Vec<u8> {
    let mut out = Vec::new();
    table_start(&mut out, rows.len());
    for row in rows {
        out.extend_from_slice(&row.asset_uuid.0);
        out.extend_from_slice(&row.content_hash.0);
        out.extend_from_slice(&(row.load_deps.len() as u32).to_le_bytes());
        for dep in &row.load_deps {
            out.extend_from_slice(&dep.asset_uuid.0);
            out.extend_from_slice(&dep.expected_terminal.0);
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
        // A local id is never empty: the empty name is the primary.
        string(&mut out, row.name.as_deref().unwrap_or(""));
        out.extend_from_slice(&row.asset_uuid.0);
    }
    out
}

fn decode_assets(bytes: &[u8]) -> Result<Vec<ManifestAssetRow>, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let count = r.bounded_count(52)?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let asset_uuid = AssetUuid(r.a16()?);
        let content_hash = ContentHash(r.a32()?);
        let n = r.bounded_count(32)?;
        let mut load_deps = Vec::with_capacity(n);
        for _ in 0..n {
            load_deps.push(ManifestLoadEdge {
                asset_uuid: AssetUuid(r.a16()?),
                expected_terminal: TypeUuid(r.a16()?),
            });
        }
        check_strict(&load_deps)?;
        rows.push(ManifestAssetRow {
            asset_uuid,
            content_hash,
            load_deps,
        });
    }
    finish_table(r, rows, |v| v.asset_uuid)
}
fn decode_encodings(bytes: &[u8]) -> Result<Vec<EncodingRow>, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let count = r.bounded_count(40)?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let content_hash = ContentHash(r.a32()?);
        let bn = r.bounded_count(32)?;
        let mut blocks = Vec::with_capacity(bn);
        for _ in 0..bn {
            blocks.push(EKey(r.a32()?));
        }
        let nn = r.bounded_count(32)?;
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
    let count = r.bounded_count(52)?;
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
    let count = r.bounded_count(36)?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let layout_hash = LayoutHash(r.a32()?);
        let len = usize::try_from(r.u32()?).map_err(|_| ManifestError::Truncated)?;
        rows.push(WireTreeRow {
            layout_hash,
            bytes: r.take(len)?.to_vec(),
        });
    }
    finish_table(r, rows, |v| v.layout_hash)
}
fn decode_paths(bytes: &[u8]) -> Result<Vec<PathRow>, ManifestError> {
    let mut r = Reader { bytes, pos: 0 };
    let count = r.bounded_count(24)?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let path = r.string()?;
        let normalized =
            distill_build::query::normalize_path(&path).map_err(|_| ManifestError::BadPath)?;
        if normalized != path {
            return Err(ManifestError::BadPath);
        }
        let name = r.string()?;
        let name = if name.is_empty() {
            None
        } else {
            let normalized = distill_build::query::normalize_identifier(&name)
                .map_err(|_| ManifestError::BadPath)?;
            if normalized != name {
                return Err(ManifestError::BadPath);
            }
            Some(name)
        };
        rows.push(PathRow {
            path,
            name,
            asset_uuid: AssetUuid(r.a16()?),
        });
    }
    finish_table(r, rows, |v| (v.path.clone(), v.name.clone()))
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

pub fn manifest_hash(bytes: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"DSPM");
    h.update(&[2]);
    h.update(bytes);
    *h.finalize().as_bytes()
}

pub fn verify_artifact_header(
    row: &ManifestAssetRow,
    asset_uuid: AssetUuid,
    load_deps: &[AssetUuid],
) -> Result<(), ManifestError> {
    if row.asset_uuid == asset_uuid
        && row
            .load_deps
            .iter()
            .map(|edge| edge.asset_uuid)
            .eq(load_deps.iter().copied())
    {
        Ok(())
    } else {
        Err(ManifestError::MetadataMismatch)
    }
}
fn verify_manifest_closure(manifest: &PackManifest) -> Result<(), ManifestError> {
    let asset_uuids = manifest
        .assets
        .iter()
        .map(|asset| asset.asset_uuid)
        .collect::<BTreeSet<_>>();
    for asset in &manifest.assets {
        for dependency in &asset.load_deps {
            if !asset_uuids.contains(&dependency.asset_uuid) {
                return Err(ManifestError::MissingDependency(dependency.asset_uuid));
            }
        }
    }
    Ok(())
}

/// Confirm each manifest edge's expected type against the terminal types of
/// the ContentHash-authenticated artifacts selected for the pack.
pub fn verify_expected_terminals(
    manifest: &PackManifest,
    terminal_types: &BTreeMap<AssetUuid, TypeUuid>,
) -> Result<(), ManifestError> {
    if terminal_types.len() != manifest.assets.len() {
        return Err(ManifestError::MetadataMismatch);
    }
    for row in &manifest.assets {
        terminal_types
            .get(&row.asset_uuid)
            .ok_or(ManifestError::MetadataMismatch)?;
        for edge in &row.load_deps {
            let dependency_type = terminal_types
                .get(&edge.asset_uuid)
                .ok_or(ManifestError::MissingDependency(edge.asset_uuid))?;
            if *dependency_type != edge.expected_terminal {
                return Err(ManifestError::DependencyTypeMismatch(
                    edge.expected_terminal,
                ));
            }
        }
    }
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn bounded_count(&mut self, minimum_row_bytes: usize) -> Result<usize, ManifestError> {
        let count = usize::try_from(self.u32()?).map_err(|_| ManifestError::Truncated)?;
        if count > self.remaining() / minimum_row_bytes {
            return Err(ManifestError::Truncated);
        }
        Ok(count)
    }

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

#[cfg(test)]
mod tests {
    use super::{decode_paths, ManifestError};

    #[test]
    fn decoder_rejects_noncanonical_path_bytes() {
        let path = "te\u{301}xtures/a.bundle";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&(path.len() as u32).to_le_bytes());
        bytes.extend_from_slice(path.as_bytes());
        bytes.extend_from_slice(&[1; 16]);

        assert!(matches!(decode_paths(&bytes), Err(ManifestError::BadPath)));
    }

    #[test]
    fn decoder_rejects_noncanonical_name_bytes() {
        let path = "textures/a.bundle";
        let name = "Surve\u{301}y";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&(path.len() as u32).to_le_bytes());
        bytes.extend_from_slice(path.as_bytes());
        bytes.extend_from_slice(&(name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(&[1; 16]);

        assert!(matches!(decode_paths(&bytes), Err(ManifestError::BadPath)));
    }
}
