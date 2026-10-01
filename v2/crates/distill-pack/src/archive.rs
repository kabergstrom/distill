//! Authenticated DPK1 archive records (§16).

use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};

use distill_bundle::crc32c;
use distill_core::id::ContentHash;

pub const PACK_MAGIC: [u8; 4] = *b"DPK1";
/// 3: path-table rows carry the asset name (a local id; empty for the
/// primary).
pub const PACK_VERSION: u32 = 3;
pub const STRUCTURAL_CHUNK_SIZE: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EKey(pub [u8; 32]);

pub fn ekey(stored_payload: &[u8]) -> EKey {
    let mut h = blake3::Hasher::new();
    h.update(b"DSEK");
    h.update(&[1]);
    h.update(stored_payload);
    EKey(*h.finalize().as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPayload {
    pub content_hash: ContentHash,
    pub structural: Vec<u8>,
    pub blobs: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactEncoding {
    pub blocks: Vec<EKey>,
    pub blobs: Vec<EKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectLocation {
    pub generation: u32,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveBuild {
    pub bytes: Vec<u8>,
    pub encodings: BTreeMap<ContentHash, ArtifactEncoding>,
    pub index: BTreeMap<EKey, ObjectLocation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveObjectKind {
    Structural,
    Blob,
}

/// Validated archive metadata without retaining any payload copy. PackfileIO
/// keeps this beside an mmap and decodes structural frames on demand; blob
/// extents remain ranges of the mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScannedObject {
    pub kind: ArchiveObjectKind,
    pub raw_len: u64,
    pub location: ObjectLocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScannedArchive {
    pub generation: u32,
    pub encoder: String,
    pub zstd_level: i32,
    pub objects: BTreeMap<EKey, ScannedObject>,
}

#[derive(Default)]
struct EncodedObjectIndex {
    locations: BTreeMap<EKey, ObjectLocation>,
    kinds: BTreeMap<EKey, ArchiveObjectKind>,
}

#[derive(Debug)]
pub enum ArchiveError {
    TooShort,
    FileHash,
    Magic,
    Version,
    Utf8,
    Truncated,
    UnknownKind,
    BadPadding,
    BadLength,
    Crc,
    EKey,
    Zstd(std::io::Error),
    DuplicateEKey,
    CrossKindEKey(EKey),
    ExternalDictionary,
    MultipleFrames,
}

impl From<std::io::Error> for ArchiveError {
    fn from(value: std::io::Error) -> Self {
        Self::Zstd(value)
    }
}

pub fn encode_archive(
    generation: u32,
    encoder: &str,
    zstd_level: i32,
    artifacts: &[ArtifactPayload],
) -> Result<ArchiveBuild, ArchiveError> {
    let mut out = Vec::new();
    out.extend_from_slice(&PACK_MAGIC);
    out.extend_from_slice(&PACK_VERSION.to_le_bytes());
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&(encoder.len() as u32).to_le_bytes());
    out.extend_from_slice(encoder.as_bytes());
    out.extend_from_slice(&zstd_level.to_le_bytes());

    let mut encodings = BTreeMap::new();
    let mut objects = EncodedObjectIndex::default();
    let mut artifacts = artifacts.to_vec();
    artifacts.sort_by_key(|artifact| artifact.content_hash);
    for artifact in artifacts {
        let mut encoding = ArtifactEncoding {
            blocks: Vec::new(),
            blobs: Vec::new(),
        };
        for raw in artifact.structural.chunks(STRUCTURAL_CHUNK_SIZE) {
            let stored = encode_structural(raw, zstd_level)?;
            let key = ekey(&stored);
            encoding.blocks.push(key);
            write_object(
                &mut out,
                generation,
                key,
                ArchiveObjectKind::Structural,
                raw.len() as u64,
                &stored,
                &mut objects,
            )?;
        }
        for blob in artifact.blobs {
            let key = ekey(&blob);
            encoding.blobs.push(key);
            write_object(
                &mut out,
                generation,
                key,
                ArchiveObjectKind::Blob,
                blob.len() as u64,
                &blob,
                &mut objects,
            )?;
        }
        encodings.insert(artifact.content_hash, encoding);
    }
    append_trailer(&mut out);
    Ok(ArchiveBuild {
        bytes: out,
        encodings,
        index: objects.locations,
    })
}

fn encode_structural(raw: &[u8], level: i32) -> Result<Vec<u8>, ArchiveError> {
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), level)?;
    encoder.window_log(18)?; // 2^18 = the canonical 256 KiB block bound.
    encoder.include_contentsize(true)?;
    encoder.set_pledged_src_size(Some(raw.len() as u64))?;
    encoder.write_all(raw)?;
    Ok(encoder.finish()?)
}

pub(crate) fn decode_structural(stored: &[u8], expected_len: u64) -> Result<Vec<u8>, ArchiveError> {
    if expected_len > STRUCTURAL_CHUNK_SIZE as u64 {
        return Err(ArchiveError::BadLength);
    }
    let frame_len = zstd::zstd_safe::get_frame_content_size(stored)
        .map_err(|_| ArchiveError::BadLength)?
        .ok_or(ArchiveError::BadLength)?;
    if frame_len != expected_len {
        return Err(ArchiveError::BadLength);
    }
    if zstd::zstd_safe::get_dict_id_from_frame(stored).is_some() {
        return Err(ArchiveError::ExternalDictionary);
    }
    if zstd::zstd_safe::find_frame_compressed_size(stored).map_err(|_| ArchiveError::BadLength)?
        != stored.len()
    {
        return Err(ArchiveError::MultipleFrames);
    }
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(stored))?;
    decoder.window_log_max(18)?;
    let capacity = usize::try_from(expected_len).map_err(|_| ArchiveError::BadLength)?;
    let mut raw = Vec::with_capacity(capacity);
    decoder
        .take(expected_len.saturating_add(1))
        .read_to_end(&mut raw)?;
    if raw.len() as u64 != expected_len {
        return Err(ArchiveError::BadLength);
    }
    Ok(raw)
}

fn write_object(
    out: &mut Vec<u8>,
    generation: u32,
    key: EKey,
    kind: ArchiveObjectKind,
    raw_len: u64,
    stored: &[u8],
    objects: &mut EncodedObjectIndex,
) -> Result<(), ArchiveError> {
    if let Some(existing) = objects.kinds.get(&key) {
        if *existing != kind {
            return Err(ArchiveError::CrossKindEKey(key));
        }
        return Ok(());
    }
    out.push(match kind {
        ArchiveObjectKind::Structural => 0x01,
        ArchiveObjectKind::Blob => 0x02,
    });
    out.extend_from_slice(&key.0);
    out.extend_from_slice(&raw_len.to_le_bytes());
    out.extend_from_slice(&(stored.len() as u64).to_le_bytes());
    out.extend_from_slice(&crc32c(stored).to_le_bytes());
    pad16(out);
    let offset = out.len() as u64;
    out.extend_from_slice(stored);
    let len = stored.len() as u64;
    pad16(out);
    objects.locations.insert(
        key,
        ObjectLocation {
            generation,
            offset,
            len,
        },
    );
    objects.kinds.insert(key, kind);
    Ok(())
}

pub fn validate_archive(bytes: &[u8]) -> Result<(), ArchiveError> {
    scan_archive(bytes).map(|_| ())
}

pub(crate) fn scan_archive(bytes: &[u8]) -> Result<ScannedArchive, ArchiveError> {
    if bytes.len() < 8 + 4 + 4 + 4 + 32 {
        return Err(ArchiveError::TooShort);
    }
    verify_trailer(bytes)?;
    let body_end = bytes.len() - 32;
    let mut r = Reader {
        bytes: &bytes[..body_end],
        pos: 0,
    };
    if r.take(4)? != PACK_MAGIC {
        return Err(ArchiveError::Magic);
    }
    if r.u32()? != PACK_VERSION {
        return Err(ArchiveError::Version);
    }
    let generation = r.u32()?;
    let encoder_len = r.u32()? as usize;
    let encoder = std::str::from_utf8(r.take(encoder_len)?)
        .map_err(|_| ArchiveError::Utf8)?
        .to_owned();
    let zstd_level = r.i32()?;
    let mut objects = BTreeMap::new();
    while r.pos < body_end {
        let kind = match r.u8()? {
            0x01 => ArchiveObjectKind::Structural,
            0x02 => ArchiveObjectKind::Blob,
            _ => return Err(ArchiveError::UnknownKind),
        };
        let key = EKey(r.array32()?);
        let raw_len = r.u64()?;
        let stored_len = usize::try_from(r.u64()?).map_err(|_| ArchiveError::BadLength)?;
        let crc = r.u32()?;
        r.align16_zero()?;
        let offset = r.pos as u64;
        let stored = r.take(stored_len)?;
        if crc32c(stored) != crc {
            return Err(ArchiveError::Crc);
        }
        if ekey(stored) != key {
            return Err(ArchiveError::EKey);
        }
        match kind {
            ArchiveObjectKind::Structural => {
                if raw_len > STRUCTURAL_CHUNK_SIZE as u64 {
                    return Err(ArchiveError::BadLength);
                }
                let _ = decode_structural(stored, raw_len)?;
            }
            ArchiveObjectKind::Blob => {
                if raw_len != stored.len() as u64 {
                    return Err(ArchiveError::BadLength);
                }
            }
        }
        r.align16_zero()?;
        let object = ScannedObject {
            kind,
            raw_len,
            location: ObjectLocation {
                generation,
                offset,
                len: stored_len as u64,
            },
        };
        if objects.insert(key, object).is_some() {
            return Err(ArchiveError::DuplicateEKey);
        }
    }
    Ok(ScannedArchive {
        generation,
        encoder,
        zstd_level,
        objects,
    })
}

fn pad16(out: &mut Vec<u8>) {
    out.resize((out.len() + 15) & !15, 0);
}

pub(crate) fn append_trailer(out: &mut Vec<u8>) {
    let hash = *blake3::hash(out).as_bytes();
    out.extend_from_slice(&hash);
}

pub(crate) fn verify_trailer(bytes: &[u8]) -> Result<(), ArchiveError> {
    if bytes.len() < 32 {
        return Err(ArchiveError::TooShort);
    }
    let split = bytes.len() - 32;
    if blake3::hash(&bytes[..split]).as_bytes() != &bytes[split..] {
        return Err(ArchiveError::FileHash);
    }
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], ArchiveError> {
        let end = self.pos.checked_add(len).ok_or(ArchiveError::Truncated)?;
        let out = self
            .bytes
            .get(self.pos..end)
            .ok_or(ArchiveError::Truncated)?;
        self.pos = end;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, ArchiveError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, ArchiveError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, ArchiveError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ArchiveError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array32(&mut self) -> Result<[u8; 32], ArchiveError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn align16_zero(&mut self) -> Result<(), ArchiveError> {
        let end = (self.pos + 15) & !15;
        if self.take(end - self.pos)?.iter().any(|b| *b != 0) {
            return Err(ArchiveError::BadPadding);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structural_frames_are_single_dictionary_free_and_window_bounded() {
        let raw = vec![7u8; STRUCTURAL_CHUNK_SIZE];
        let frame = encode_structural(&raw, 3).unwrap();
        assert!(zstd::zstd_safe::get_dict_id_from_frame(&frame).is_none());
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&frame).unwrap(),
            Some(raw.len() as u64)
        );
        assert_eq!(decode_structural(&frame, raw.len() as u64).unwrap(), raw);

        let mut concatenated = frame.clone();
        concatenated.extend_from_slice(&frame);
        assert!(matches!(
            decode_structural(&concatenated, raw.len() as u64),
            Err(ArchiveError::MultipleFrames)
        ));

        let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
        encoder.include_contentsize(false).unwrap();
        encoder.write_all(&raw).unwrap();
        let missing_size = encoder.finish().unwrap();
        assert!(matches!(
            decode_structural(&missing_size, raw.len() as u64),
            Err(ArchiveError::BadLength)
        ));
    }

    #[test]
    fn encoder_rejects_one_ekey_used_as_both_structural_and_blob() {
        let structural = b"structural payload".to_vec();
        let frame = encode_structural(&structural, 3).unwrap();
        let error = encode_archive(
            0,
            "test",
            3,
            &[ArtifactPayload {
                content_hash: ContentHash([1; 32]),
                structural,
                blobs: vec![frame.clone()],
            }],
        )
        .unwrap_err();
        assert!(matches!(error, ArchiveError::CrossKindEKey(key) if key == ekey(&frame)));
    }
}
