//! The DSTL artifact container (§12).
//!
//! Layout, pinned (all integers LE):
//!
//! ```text
//! magic         [u8; 8]     89 44 53 54 4C 0D 1A 0A  ("\x89DSTL\r\x1a\n")
//! version       u32
//! asset_uuid    [u8; 16]
//! authored_type [u8; 16]
//! terminal_type [u8; 16]
//! encoded_type  [u8; 16]
//! logical_hash  [u8; 32]
//! layout_hash   [u8; 32]
//! dep_count     u32
//! blob_count    u32
//! fixed_len     u32
//! var_len       u64          must be < 2^32 (VarRef offsets are u32)
//! load_deps     [u8; 16] × dep_count      sorted lexicographically, deduped
//! blob_table    (offset u64, len u64) × blob_count — into the blob section
//! pad           zeros to 16-byte alignment
//! fixed         [u8; fixed_len]
//! pad           zeros to 16-byte alignment
//! variable      [u8; var_len]
//! pad           zeros to 16-byte alignment
//! blobs         each blob 16-byte aligned; offsets ascending, gaps only
//!               alignment-sized and zero
//! ```
//!
//! Total size must match the header exactly; all arithmetic is checked;
//! padding must be zero. Blob-table entry order is the canonical structural
//! path key (§6): entries sorted by `distill_bundle::encode_path` bytes.

use distill_bundle::{encode_path, PathComponent};
use distill_core::id::{AssetUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};

/// `"\x89DSTL\r\x1a\n"` (§12).
pub const ARTIFACT_MAGIC: [u8; 8] = [0x89, 0x44, 0x53, 0x54, 0x4C, 0x0D, 0x1A, 0x0A];

/// Artifact format version — an input-hash input (§12).
pub const ARTIFACT_FORMAT_VERSION: u32 = 1;

/// Fixed header size in bytes (magic through `var_len`), pinned.
pub const ARTIFACT_HEADER_LEN: usize = 160;

/// Identity fields of the header (§12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactHeader {
    pub asset_uuid: AssetUuid,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub encoded_type: TypeUuid,
    pub logical_hash: LogicalHash,
    pub layout_hash: LayoutHash,
}

/// Everything that can go wrong writing or parsing an artifact. The reader
/// is strict: any deviation from the canonical form is an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactError {
    BadMagic,
    UnsupportedVersion {
        got: u32,
    },
    /// Fewer bytes than the header/tables/sections claim.
    Truncated {
        needed: u64,
        have: u64,
    },
    /// More bytes than the header accounts for.
    TrailingBytes {
        expected: u64,
        got: u64,
    },
    /// Checked size arithmetic overflowed — a corrupt header, not a panic.
    Overflow,
    /// `var_len` at or above 2^32: `VarRef` offsets are u32 (§12).
    VarLenTooLarge {
        var_len: u64,
    },
    /// A u32 grammar boundary was exceeded on write (§12: reject, never wrap).
    CountExceedsU32 {
        what: &'static str,
        count: u64,
    },
    /// `load_deps` not sorted-and-deduplicated at `index`.
    DepsNotCanonical {
        index: u32,
    },
    /// A padding byte (section pad or blob gap) was nonzero.
    NonzeroPadding {
        offset: u64,
    },
    /// Blob-table entry `index` is not at its canonical offset
    /// (`align_up(previous end, 16)`) or its extent overflows.
    BlobTableInvalid {
        index: u32,
        expected: u64,
        got: u64,
    },
    /// Two blobs share one structural path — the key must be injective (§6).
    DuplicateBlobPath {
        path: String,
    },
    BlobCountMismatch {
        expected: u32,
        got: usize,
    },
    BlobLengthMismatch {
        index: u32,
        expected: u64,
        got: usize,
    },
}

impl std::fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtifactError::BadMagic => write!(f, "bad artifact magic"),
            ArtifactError::UnsupportedVersion { got } => {
                write!(f, "unsupported artifact format version {got}")
            }
            ArtifactError::Truncated { needed, have } => {
                write!(f, "artifact truncated: need {needed} bytes, have {have}")
            }
            ArtifactError::TrailingBytes { expected, got } => {
                write!(
                    f,
                    "artifact has trailing bytes: expected {expected}, got {got}"
                )
            }
            ArtifactError::Overflow => write!(f, "artifact size arithmetic overflowed"),
            ArtifactError::VarLenTooLarge { var_len } => {
                write!(
                    f,
                    "variable section length {var_len} exceeds the u32 VarRef bound"
                )
            }
            ArtifactError::CountExceedsU32 { what, count } => {
                write!(f, "{what} count {count} exceeds u32")
            }
            ArtifactError::DepsNotCanonical { index } => {
                write!(f, "load_deps not sorted/deduplicated at index {index}")
            }
            ArtifactError::NonzeroPadding { offset } => {
                write!(f, "nonzero padding byte at file offset {offset}")
            }
            ArtifactError::BlobTableInvalid {
                index,
                expected,
                got,
            } => write!(
                f,
                "blob table entry {index}: offset {got}, canonical offset {expected}"
            ),
            ArtifactError::DuplicateBlobPath { path } => {
                write!(f, "duplicate blob structural path {path}")
            }
            ArtifactError::BlobCountMismatch { expected, got } => {
                write!(
                    f,
                    "artifact expects {expected} blob extents, fetch carried {got}"
                )
            }
            ArtifactError::BlobLengthMismatch {
                index,
                expected,
                got,
            } => {
                write!(
                    f,
                    "blob extent {index} has {got} bytes, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ArtifactError {}

/// blake3 over the whole raw file — the artifact's identity (§12).
pub fn content_hash(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

fn align_up_u64(n: u64, align: u64) -> Option<u64> {
    let rem = n % align;
    if rem == 0 {
        Some(n)
    } else {
        n.checked_add(align - rem)
    }
}

/// Assign canonical `BlobRef` indices: for each input path, its position in
/// the blob table (entries sorted by encoded structural-path bytes, §6/§12).
/// A duplicate path is an error — the key must be injective.
pub fn canonical_blob_order(paths: &[Vec<PathComponent>]) -> Result<Vec<u32>, ArtifactError> {
    if paths.len() as u64 > u32::MAX as u64 {
        return Err(ArtifactError::CountExceedsU32 {
            what: "blob",
            count: paths.len() as u64,
        });
    }
    let mut keyed: Vec<(Vec<u8>, usize)> = paths
        .iter()
        .enumerate()
        .map(|(i, p)| (encode_path(p), i))
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    for w in keyed.windows(2) {
        if w[0].0 == w[1].0 {
            return Err(ArtifactError::DuplicateBlobPath {
                path: format!("{:?}", paths[w[0].1]),
            });
        }
    }
    let mut order = vec![0u32; paths.len()];
    for (table_index, (_, input_index)) in keyed.iter().enumerate() {
        order[*input_index] = table_index as u32;
    }
    Ok(order)
}

fn pad_to_16(out: &mut Vec<u8>) {
    while !out.len().is_multiple_of(16) {
        out.push(0);
    }
}

/// Write a canonical artifact. `load_deps` are canonicalized (sorted
/// lexicographically, deduplicated); blobs are sorted by encoded structural
/// path — the caller must have assigned `BlobRef` indices with
/// [`canonical_blob_order`] over the same paths.
pub fn write_artifact(
    header: &ArtifactHeader,
    load_deps: &[AssetUuid],
    fixed: &[u8],
    variable: &[u8],
    blobs: &[(Vec<PathComponent>, &[u8])],
) -> Result<Vec<u8>, ArtifactError> {
    let mut deps: Vec<AssetUuid> = load_deps.to_vec();
    deps.sort();
    deps.dedup();
    if deps.len() as u64 > u32::MAX as u64 {
        return Err(ArtifactError::CountExceedsU32 {
            what: "load_dep",
            count: deps.len() as u64,
        });
    }
    if fixed.len() as u64 > u32::MAX as u64 {
        return Err(ArtifactError::CountExceedsU32 {
            what: "fixed byte",
            count: fixed.len() as u64,
        });
    }
    if variable.len() as u64 >= 1u64 << 32 {
        return Err(ArtifactError::VarLenTooLarge {
            var_len: variable.len() as u64,
        });
    }

    // Canonical blob order: sort by encoded structural path bytes.
    let paths: Vec<Vec<PathComponent>> = blobs.iter().map(|(p, _)| p.clone()).collect();
    let order = canonical_blob_order(&paths)?;
    let mut sorted: Vec<&[u8]> = vec![&[]; blobs.len()];
    for (input_index, (_, bytes)) in blobs.iter().enumerate() {
        sorted[order[input_index] as usize] = bytes;
    }

    // Blob table: each blob at align_up(previous end, 16), first at 0.
    let mut table = Vec::with_capacity(sorted.len());
    let mut cursor: u64 = 0;
    for bytes in &sorted {
        let offset = align_up_u64(cursor, 16).ok_or(ArtifactError::Overflow)?;
        let len = bytes.len() as u64;
        cursor = offset.checked_add(len).ok_or(ArtifactError::Overflow)?;
        table.push((offset, len));
    }

    let mut out = Vec::new();
    out.extend_from_slice(&ARTIFACT_MAGIC);
    out.extend_from_slice(&ARTIFACT_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&header.asset_uuid.0);
    out.extend_from_slice(&header.authored_type.0);
    out.extend_from_slice(&header.terminal_type.0);
    out.extend_from_slice(&header.encoded_type.0);
    out.extend_from_slice(&header.logical_hash.0);
    out.extend_from_slice(&header.layout_hash.0);
    out.extend_from_slice(&(deps.len() as u32).to_le_bytes());
    out.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
    out.extend_from_slice(&(fixed.len() as u32).to_le_bytes());
    out.extend_from_slice(&(variable.len() as u64).to_le_bytes());
    debug_assert_eq!(out.len(), ARTIFACT_HEADER_LEN);
    for dep in &deps {
        out.extend_from_slice(&dep.0);
    }
    for (offset, len) in &table {
        out.extend_from_slice(&offset.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    pad_to_16(&mut out);
    out.extend_from_slice(fixed);
    pad_to_16(&mut out);
    out.extend_from_slice(variable);
    pad_to_16(&mut out);
    let blob_section_base = out.len() as u64;
    for (i, bytes) in sorted.iter().enumerate() {
        // Gap to this blob's aligned offset — zeros.
        while (out.len() as u64) < blob_section_base + table[i].0 {
            out.push(0);
        }
        out.extend_from_slice(bytes);
    }
    Ok(out)
}

/// A parsed artifact: the strict reader's view, borrowing the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactView<'a> {
    pub version: u32,
    pub asset_uuid: AssetUuid,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub encoded_type: TypeUuid,
    pub logical_hash: LogicalHash,
    pub layout_hash: LayoutHash,
    pub load_deps: Vec<AssetUuid>,
    /// (offset, len) into the blob section, in canonical structural-path
    /// order — `BlobRef.index` indexes this table.
    pub blob_table: Vec<(u64, u64)>,
    pub fixed: &'a [u8],
    pub variable: &'a [u8],
    /// The raw blob section (from the end of the variable pad to EOF).
    pub blob_section: &'a [u8],
}

impl<'a> ArtifactView<'a> {
    /// The backing bytes of blob-table entry `index`.
    pub fn blob(&self, index: u32) -> Option<&'a [u8]> {
        let (offset, len) = *self.blob_table.get(index as usize)?;
        self.blob_section
            .get(offset as usize..(offset + len) as usize)
    }
}

/// The structural prefix of a fetched artifact, with blob extents validated
/// separately in canonical table order. Packs and RPC transport blobs out of
/// line, but their identity and metadata are still those of the complete DSTL
/// file; this view is the bridge back to the fixup executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPartsView<'a> {
    pub version: u32,
    pub asset_uuid: AssetUuid,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub encoded_type: TypeUuid,
    pub logical_hash: LogicalHash,
    pub layout_hash: LayoutHash,
    pub load_deps: Vec<AssetUuid>,
    pub blob_table: Vec<(u64, u64)>,
    pub fixed: &'a [u8],
    pub variable: &'a [u8],
    pub content_hash: ContentHash,
}

/// Parse a fetched artifact whose canonical structural prefix and blob
/// extents arrived separately. The blob list must be in blob-table order.
pub fn parse_artifact_parts<'a>(
    structural: &'a [u8],
    blobs: &[&[u8]],
) -> Result<ArtifactPartsView<'a>, ArtifactError> {
    let mut r = Reader {
        bytes: structural,
        pos: 0,
    };
    if r.take(8)? != ARTIFACT_MAGIC {
        return Err(ArtifactError::BadMagic);
    }
    let version = r.u32()?;
    if version != ARTIFACT_FORMAT_VERSION {
        return Err(ArtifactError::UnsupportedVersion { got: version });
    }
    let asset_uuid = AssetUuid(r.bytes16()?);
    let authored_type = TypeUuid(r.bytes16()?);
    let terminal_type = TypeUuid(r.bytes16()?);
    let encoded_type = TypeUuid(r.bytes16()?);
    let logical_hash = LogicalHash(r.bytes32()?);
    let layout_hash = LayoutHash(r.bytes32()?);
    let dep_count = r.u32()?;
    let blob_count = r.u32()?;
    let fixed_len = r.u32()?;
    let var_len = r.u64()?;
    if var_len >= 1u64 << 32 {
        return Err(ArtifactError::VarLenTooLarge { var_len });
    }

    let mut load_deps = Vec::new();
    load_deps
        .try_reserve_exact(dep_count.min(4096) as usize)
        .ok();
    for index in 0..dep_count {
        let dep = AssetUuid(r.bytes16()?);
        if load_deps.last().is_some_and(|previous| *previous >= dep) {
            return Err(ArtifactError::DepsNotCanonical { index });
        }
        load_deps.push(dep);
    }

    let mut blob_table = Vec::new();
    let mut expected_offset = 0u64;
    for index in 0..blob_count {
        let offset = r.u64()?;
        let len = r.u64()?;
        if offset != expected_offset {
            return Err(ArtifactError::BlobTableInvalid {
                index,
                expected: expected_offset,
                got: offset,
            });
        }
        let end = offset.checked_add(len).ok_or(ArtifactError::Overflow)?;
        expected_offset = align_up_u64(end, 16).ok_or(ArtifactError::Overflow)?;
        blob_table.push((offset, len));
    }
    r.pad_to_16()?;
    let fixed = r.take(fixed_len as u64)?;
    r.pad_to_16()?;
    let variable = r.take(var_len)?;
    r.pad_to_16()?;
    if r.pos != structural.len() {
        return Err(ArtifactError::TrailingBytes {
            expected: r.pos as u64,
            got: structural.len() as u64,
        });
    }
    if blobs.len() != blob_count as usize {
        return Err(ArtifactError::BlobCountMismatch {
            expected: blob_count,
            got: blobs.len(),
        });
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(structural);
    let mut cursor = 0u64;
    const ZERO_PAD: [u8; 16] = [0; 16];
    for (index, ((offset, len), blob)) in blob_table.iter().zip(blobs).enumerate() {
        if blob.len() as u64 != *len {
            return Err(ArtifactError::BlobLengthMismatch {
                index: index as u32,
                expected: *len,
                got: blob.len(),
            });
        }
        let gap = offset.checked_sub(cursor).ok_or(ArtifactError::Overflow)? as usize;
        hasher.update(&ZERO_PAD[..gap]);
        hasher.update(blob);
        cursor = offset + len;
    }

    Ok(ArtifactPartsView {
        version,
        asset_uuid,
        authored_type,
        terminal_type,
        encoded_type,
        logical_hash,
        layout_hash,
        load_deps,
        blob_table,
        fixed,
        variable,
        content_hash: ContentHash(*hasher.finalize().as_bytes()),
    })
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: u64) -> Result<&'a [u8], ArtifactError> {
        let end = (self.pos as u64)
            .checked_add(n)
            .ok_or(ArtifactError::Overflow)?;
        if end > self.bytes.len() as u64 {
            return Err(ArtifactError::Truncated {
                needed: end,
                have: self.bytes.len() as u64,
            });
        }
        let slice = &self.bytes[self.pos..end as usize];
        self.pos = end as usize;
        Ok(slice)
    }

    fn u32(&mut self) -> Result<u32, ArtifactError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, ArtifactError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes16(&mut self) -> Result<[u8; 16], ArtifactError> {
        Ok(self.take(16)?.try_into().unwrap())
    }

    fn bytes32(&mut self) -> Result<[u8; 32], ArtifactError> {
        Ok(self.take(32)?.try_into().unwrap())
    }

    fn pad_to_16(&mut self) -> Result<(), ArtifactError> {
        while !self.pos.is_multiple_of(16) {
            let at = self.pos as u64;
            let byte = self.take(1)?[0];
            if byte != 0 {
                return Err(ArtifactError::NonzeroPadding { offset: at });
            }
        }
        Ok(())
    }
}

/// Rejoin a structural prefix and its blobs (in blob-table order) into the
/// complete artifact file: each blob at the next 16-aligned offset from the
/// blob-section base, gaps zero. The inverse of [`split_artifact`]; callers
/// validate the parts with [`parse_artifact_parts`] first.
pub fn assemble_artifact(structural: &[u8], blobs: &[&[u8]]) -> Vec<u8> {
    let total = blobs
        .iter()
        .fold(structural.len(), |len, blob| len + 15 + blob.len());
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(structural);
    let base = out.len();
    for blob in blobs {
        let offset = (out.len() - base).next_multiple_of(16);
        out.resize(base + offset, 0);
        out.extend_from_slice(blob);
    }
    out
}

/// Split a complete artifact file into its structural prefix and its blobs
/// in blob-table order, validating it with [`parse_artifact`].
pub fn split_artifact(bytes: &[u8]) -> Result<(&[u8], Vec<&[u8]>), ArtifactError> {
    let view = parse_artifact(bytes)?;
    let structural_len = bytes.len() - view.blob_section.len();
    let blobs = (0..view.blob_table.len() as u32)
        .map(|index| view.blob(index).ok_or(ArtifactError::Overflow))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((&bytes[..structural_len], blobs))
}

/// Parse and strictly validate an artifact (§12): exact total size, zero
/// padding, canonical `load_deps`, canonical blob-table offsets.
pub fn parse_artifact(bytes: &[u8]) -> Result<ArtifactView<'_>, ArtifactError> {
    let mut r = Reader { bytes, pos: 0 };

    if r.take(8)? != ARTIFACT_MAGIC {
        return Err(ArtifactError::BadMagic);
    }
    let version = r.u32()?;
    if version != ARTIFACT_FORMAT_VERSION {
        return Err(ArtifactError::UnsupportedVersion { got: version });
    }
    let asset_uuid = AssetUuid(r.bytes16()?);
    let authored_type = TypeUuid(r.bytes16()?);
    let terminal_type = TypeUuid(r.bytes16()?);
    let encoded_type = TypeUuid(r.bytes16()?);
    let logical_hash = LogicalHash(r.bytes32()?);
    let layout_hash = LayoutHash(r.bytes32()?);
    let dep_count = r.u32()?;
    let blob_count = r.u32()?;
    let fixed_len = r.u32()?;
    let var_len = r.u64()?;
    if var_len >= 1u64 << 32 {
        return Err(ArtifactError::VarLenTooLarge { var_len });
    }

    let mut load_deps = Vec::new();
    load_deps
        .try_reserve_exact(dep_count.min(4096) as usize)
        .ok();
    for i in 0..dep_count {
        let dep = AssetUuid(r.bytes16()?);
        if let Some(prev) = load_deps.last() {
            if *prev >= dep {
                return Err(ArtifactError::DepsNotCanonical { index: i });
            }
        }
        load_deps.push(dep);
    }

    // Blob table: offsets must be exactly align_up(previous end, 16).
    let mut blob_table = Vec::new();
    let mut expected_offset: u64 = 0;
    let mut blob_section_len: u64 = 0;
    for i in 0..blob_count {
        let offset = r.u64()?;
        let len = r.u64()?;
        if offset != expected_offset {
            return Err(ArtifactError::BlobTableInvalid {
                index: i,
                expected: expected_offset,
                got: offset,
            });
        }
        let end = offset.checked_add(len).ok_or(ArtifactError::Overflow)?;
        blob_section_len = end;
        expected_offset = align_up_u64(end, 16).ok_or(ArtifactError::Overflow)?;
        blob_table.push((offset, len));
    }

    r.pad_to_16()?;
    let fixed = r.take(fixed_len as u64)?;
    r.pad_to_16()?;
    let variable = r.take(var_len)?;
    r.pad_to_16()?;
    let blob_section_start = r.pos;
    let blob_section = r.take(blob_section_len)?;

    // Total size must match the header exactly.
    if r.pos as u64 != bytes.len() as u64 {
        return Err(ArtifactError::TrailingBytes {
            expected: r.pos as u64,
            got: bytes.len() as u64,
        });
    }

    // Blob gaps (alignment-sized by construction) must be zero.
    let mut cursor: u64 = 0;
    for &(offset, len) in &blob_table {
        for gap in cursor..offset {
            if blob_section[gap as usize] != 0 {
                return Err(ArtifactError::NonzeroPadding {
                    offset: blob_section_start as u64 + gap,
                });
            }
        }
        cursor = offset + len;
    }

    Ok(ArtifactView {
        version,
        asset_uuid,
        authored_type,
        terminal_type,
        encoded_type,
        logical_hash,
        layout_hash,
        load_deps,
        blob_table,
        fixed,
        variable,
        blob_section,
    })
}
