//! The container encoding (§6) and the writer for both encodings.
//!
//! ```text
//! magic     [u8;8] = 89 44 53 42 0D 0A 1A 0A
//! version   u32 LE   (pinned = 1)
//! json_len  u64 LE
//! blob_len  u64 LE
//! json_crc  u32 LE   CRC-32C of the json bytes
//! blob_crc  u32 LE   CRC-32C of the blob chunk bytes
//! json      [json_len]   envelope JSON; blob leaves are {"len","offset"}
//! pad       zeros to 16-byte alignment of (36 + json_len)
//! blob      [blob_len]
//! ```
//!
//! Parse-side validation runs before any large allocation: header length,
//! magic (with loud line-ending-mangling detection), version, lengths
//! checked against the actual file size with overflow-checked arithmetic
//! (the file must be exactly `36 + json_len + pad + blob_len`), CRCs, pad
//! bytes. Blob placement is then validated globally: offsets 16-aligned,
//! non-overlapping, at exactly the canonical position (the next 16-aligned
//! offset in `(local_id, encoded path)` order — zero-length blobs take the
//! current position and may share the next blob's offset), inter-blob gap
//! bytes zero, and `blob_len` equal to the end of the last blob.
//!
//! The JSON chunk is the same canonical text the plain encoding uses,
//! trailing newline included (the §6 whole-file byte); every byte of the
//! container is deterministic.

use std::collections::BTreeMap;

use ngp_schema::{blob_sites, encode_path, node_hash, write_value, SnapshotError, WriteOptions};

use crate::crc32c::crc32c;
use crate::envelope;
use crate::error::BundleError;
use crate::data::{display, map_error, text_depth, BlobSite, Blobs, LayoutSink, MAX_DATA_DEPTH};
use crate::envelope::Strictness;
use crate::{Bundle, BundleError as E, CONTAINER_MAGIC, CONTAINER_VERSION};

const HEADER_LEN: u64 = 36;

fn align16(n: u64) -> Option<u64> {
    n.checked_add(15).map(|x| x & !15)
}

fn le_u32(bytes: &[u8], at: usize) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(buf)
}

fn le_u64(bytes: &[u8], at: usize) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(buf)
}

/// Does a non-matching magic look like the real magic after line-ending
/// translation? LF→CRLF doubles the `\n` bytes; CRLF→LF collapses the
/// `\r\n`. The PNG-style magic exists to catch exactly this, loudly.
fn looks_line_ending_mangled(found: &[u8; 8]) -> bool {
    found[..4] == CONTAINER_MAGIC[..4]
        && (found[4..8] == [0x0D, 0x0D, 0x0A, 0x1A] || found[4..7] == [0x0A, 0x1A, 0x0A])
}

pub(crate) fn parse(bytes: &[u8]) -> Result<Bundle, BundleError> {
    parse_with(bytes, Strictness::Full)
}

pub(crate) fn parse_namespace(bytes: &[u8]) -> Result<Bundle, BundleError> {
    parse_with(bytes, Strictness::Namespace)
}

fn parse_with(
    bytes: &[u8],
    strictness: Strictness,
) -> Result<Bundle, BundleError> {
    // ---- framing: everything here runs before any allocation ----
    if bytes.len() < HEADER_LEN as usize {
        return Err(E::TooShortForHeader { len: bytes.len() });
    }
    let mut magic = [0u8; 8];
    magic.copy_from_slice(&bytes[0..8]);
    if magic != CONTAINER_MAGIC {
        return Err(E::BadMagic {
            found: magic,
            line_ending_mangled: looks_line_ending_mangled(&magic),
        });
    }
    let version = le_u32(bytes, 8);
    if version != CONTAINER_VERSION {
        return Err(E::UnsupportedContainerVersion { found: version });
    }
    let json_len = le_u64(bytes, 12);
    let blob_len = le_u64(bytes, 20);
    let json_crc = le_u32(bytes, 28);
    let blob_crc = le_u32(bytes, 32);

    let overflow = E::HeaderOverflow { json_len, blob_len };
    let json_end = HEADER_LEN.checked_add(json_len).ok_or(overflow.clone())?;
    let chunk_start = align16(json_end).ok_or(overflow.clone())?;
    let expected_total = chunk_start.checked_add(blob_len).ok_or(overflow)?;
    let actual_total = bytes.len() as u64;
    if actual_total < expected_total {
        return Err(E::Truncated {
            expected: expected_total,
            actual: actual_total,
        });
    }
    if actual_total > expected_total {
        return Err(E::TrailingBytes {
            expected: expected_total,
            actual: actual_total,
        });
    }
    // Lengths are proven consistent with the in-memory slice; indexing
    // below cannot go out of bounds.
    let json = &bytes[HEADER_LEN as usize..json_end as usize];
    let pad = &bytes[json_end as usize..chunk_start as usize];
    let chunk = &bytes[chunk_start as usize..];

    let actual_json_crc = crc32c(json);
    if actual_json_crc != json_crc {
        return Err(E::JsonCrcMismatch {
            expected: json_crc,
            actual: actual_json_crc,
        });
    }
    let actual_blob_crc = crc32c(chunk);
    if actual_blob_crc != blob_crc {
        return Err(E::BlobCrcMismatch {
            expected: blob_crc,
            actual: actual_blob_crc,
        });
    }
    if let Some(i) = pad.iter().position(|&b| b != 0) {
        return Err(E::NonzeroPadByte {
            file_offset: json_end + i as u64,
        });
    }

    // ---- envelope ----
    let text = envelope::utf8(json)?;
    // ---- typed parse: placement leaves bounds-checked and filled ----
    let mut sites: Vec<BlobSite> = Vec::new();
    let bundle = envelope::parse(
        text,
        strictness,
        Blobs::Container {
            chunk,
            sites: &mut sites,
        },
    )?;

    // ---- global placement checks, in canonical (local_id, path) order ----
    sites.sort_by(|a, b| {
        (a.local_id.as_bytes(), &a.path_bytes).cmp(&(b.local_id.as_bytes(), &b.path_bytes))
    });
    let mut prev_end: u64 = 0;
    let mut expected: u64 = 0;
    for site in &sites {
        if site.offset % 16 != 0 {
            return Err(E::MisalignedBlob {
                local_id: site.local_id.clone(),
                path: site.path_display.clone(),
                offset: site.offset,
            });
        }
        if site.offset < prev_end {
            return Err(E::OverlappingBlobs {
                local_id: site.local_id.clone(),
                path: site.path_display.clone(),
                offset: site.offset,
                prev_end,
            });
        }
        if site.offset != expected {
            return Err(E::BlobNotAtCanonicalOffset {
                local_id: site.local_id.clone(),
                path: site.path_display.clone(),
                expected,
                actual: site.offset,
            });
        }
        // The alignment gap before this blob must be zero bytes.
        if let Some(i) = chunk[prev_end as usize..site.offset as usize]
            .iter()
            .position(|&b| b != 0)
        {
            return Err(E::NonzeroGapByte {
                chunk_offset: prev_end + i as u64,
            });
        }
        // In-bounds and overflow-free per pass A: offset + len <= blob_len.
        prev_end = site.offset + site.len;
        expected = align16(prev_end).ok_or(E::HeaderOverflow { json_len, blob_len })?;
    }
    if prev_end != blob_len {
        return Err(E::BlobChunkLength {
            expected: prev_end,
            actual: blob_len,
        });
    }

    Ok(bundle)
}

/// Write a bundle canonically: plain JSON iff the schema walk collects no
/// blobs, the container otherwise (§6 — the writer never produces a
/// zero-blob container).
pub(crate) fn write(bundle: &Bundle) -> Result<Vec<u8>, BundleError> {
    // ---- envelope-level validation, mirroring the parser's ----
    if bundle.format_version != crate::BUNDLE_FORMAT_VERSION {
        return Err(E::UnsupportedFormatVersion {
            found: bundle.format_version as u128,
        });
    }
    for (hash, schema) in &bundle.schemas {
        let actual = node_hash(&schema.root).map_err(|e| E::Schema {
            hash: *hash,
            error: SnapshotError::Node(e),
        })?;
        if actual != *hash {
            return Err(E::Schema {
                hash: *hash,
                error: SnapshotError::HashMismatch {
                    expected: *hash,
                    actual,
                },
            });
        }
    }
    for (local_id, entry) in &bundle.assets {
        envelope::check_local_id(local_id)?;
        if local_id.starts_with('$') && !entry.authoring_only {
            return Err(E::ReservedEntryMustBeAuthoringOnly {
                local_id: local_id.clone(),
            });
        }
        for schema_hash in envelope::schema_references(entry) {
            if !bundle.schemas.contains_key(&schema_hash) {
                return Err(E::MissingSchema {
                    local_id: local_id.clone(),
                    schema_hash,
                });
            }
        }
        envelope::validate_entry_role(
            local_id,
            bundle.format_version,
            entry,
            &bundle.schemas[&entry.schema_hash],
        )?;
    }
    if let Some(p) = &bundle.primary {
        match bundle.assets.get(p) {
            None => return Err(E::PrimaryNotFound { primary: p.clone() }),
            Some(entry) if entry.authoring_only => {
                return Err(E::PrimaryIsAuthoringOnly { primary: p.clone() })
            }
            Some(_) => {}
        }
    }

    let table = bundle.type_table()?;

    // ---- pass A: validate data against schemas, collect blob lengths ----
    let mut collected: Vec<(&str, Vec<u8>, u64)> = Vec::new(); // (local_id, path, len)
    for (local_id, entry) in &bundle.assets {
        let schema = &bundle.schemas[&entry.schema_hash]; // presence checked above
        let sites =
            blob_sites(&schema.root, &entry.data, Some(&mut &table)).map_err(|e| map_error(local_id, e, None))?;
        for (path, len) in sites {
            collected.push((local_id.as_str(), encode_path(&path), len));
        }
    }

    // ---- layout: successive 16-aligned offsets in canonical order ----
    collected.sort_by(|a, b| (a.0.as_bytes(), &a.1).cmp(&(b.0.as_bytes(), &b.1)));
    let overflow = || E::Internal {
        detail: "blob layout exceeds u64".to_string(),
    };
    let mut layouts: BTreeMap<&str, BTreeMap<Vec<u8>, u64>> = BTreeMap::new();
    let mut cursor: u64 = 0;
    for (local_id, path, len) in &collected {
        let offset = align16(cursor).ok_or_else(overflow)?;
        cursor = offset.checked_add(*len).ok_or_else(overflow)?;
        layouts.entry(local_id).or_default().insert(path.clone(), offset);
    }
    let blob_len = usize::try_from(cursor).map_err(|_| overflow())?;

    // ---- pass B: write each entry's data, blobs placed into the chunk ----
    let empty_layout = BTreeMap::new();
    let mut chunk = vec![0u8; blob_len];
    let mut datas: BTreeMap<&str, String> = BTreeMap::new();
    for (local_id, entry) in &bundle.assets {
        let schema = &bundle.schemas[&entry.schema_hash];
        let mut sink = LayoutSink {
            layout: layouts.get(local_id.as_str()).unwrap_or(&empty_layout),
            chunk: &mut chunk,
        };
        let text = write_value(
            &schema.root,
            &entry.data,
            WriteOptions {
                blobs: Some(&mut sink),
                types: Some(&mut &table),
            },
        )
        .map_err(|e| map_error(local_id, e, None))?;
        // The parser's nesting cap must accept what the writer emits.
        if text_depth(&text) > MAX_DATA_DEPTH {
            return Err(E::WalkDepth {
                local_id: local_id.clone(),
                path: display(&[]),
            });
        }
        datas.insert(local_id.as_str(), text);
    }

    // ---- plain JSON iff no blobs ----
    let mut text = envelope::write(bundle, &datas)?;
    text.push('\n'); // the §6 whole-file byte: exactly one trailing \n
    if collected.is_empty() {
        return Ok(text.into_bytes());
    }
    let json = text.into_bytes();

    // ---- header + pad + chunk ----
    let mut out = Vec::with_capacity(HEADER_LEN as usize + json.len() + 15 + chunk.len());
    out.extend_from_slice(&CONTAINER_MAGIC);
    out.extend_from_slice(&CONTAINER_VERSION.to_le_bytes());
    out.extend_from_slice(&(json.len() as u64).to_le_bytes());
    out.extend_from_slice(&(chunk.len() as u64).to_le_bytes());
    out.extend_from_slice(&crc32c(&json).to_le_bytes());
    out.extend_from_slice(&crc32c(&chunk).to_le_bytes());
    out.extend_from_slice(&json);
    let padded = align16(out.len() as u64).ok_or_else(overflow)?;
    out.resize(padded as usize, 0);
    out.extend_from_slice(&chunk);
    Ok(out)
}
