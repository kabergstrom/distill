//! §6 Bundle File Format: one self-contained file holding one or more
//! assets — authored data plus deterministic snapshots (UUIDs, schemas),
//! no sidecar files.
//!
//! Two physical encodings, one parser dispatching on the first byte:
//!
//! - **Plain JSON** — whenever the bundle has no blobs; diffable text.
//! - **Container** — whenever blobs are present: a fixed 36-byte header
//!   (magic, version, lengths, CRC-32C checksums), the envelope JSON with
//!   every blob leaf rewritten as `{"len": n, "offset": m}`, zero padding
//!   to 16-byte alignment, then the blob chunk.
//!
//! Writer determinism is part of the spec (§6): canonical JSON, zero
//! padding, blobs laid out at successive 16-aligned offsets in canonical
//! `(local_id, structural path)` order. `write_bundle` picks plain JSON iff
//! the bundle has no blobs; `write_bundle(parse_bundle(bytes)) == bytes`
//! for canonically written files of either encoding.

use std::collections::BTreeMap;

use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_json::AuthoredValue;
use ngp_schema::LogicalSchema;

mod container;
mod crc32c;
mod envelope;
mod error;
mod path;
mod walk;

pub use crc32c::crc32c;
pub use error::BundleError;
pub use path::{encode_path, PathComponent};

/// The envelope semantics version this crate reads and writes (§6): the
/// JSON `format_version` field. Pinned to exactly 1.
pub const BUNDLE_FORMAT_VERSION: u32 = 1;

/// The container encoding version this crate reads and writes (§6): the
/// header `version` field. Pinned to exactly 1.
pub const CONTAINER_VERSION: u32 = 1;

/// PNG-style container magic: `\x89DSB\r\n\x1a\n`. The high-bit first byte
/// is invalid as UTF-8/JSON at byte zero, and the embedded `\r\n` / `\n`
/// detect line-ending mangling loudly at open time (§6).
pub const CONTAINER_MAGIC: [u8; 8] = [0x89, 0x44, 0x53, 0x42, 0x0D, 0x0A, 0x1A, 0x0A];

/// Walk nesting cap for the schema-directed walk — a definite error, never
/// a stack overflow (§12's cap discipline applied to the walk). JSON text
/// nesting is already capped at `distill_json::MAX_DEPTH` (512), and every
/// struct/enum/container step consumes a JSON level, so legitimate walks
/// stay within ~512 frames plus short schema chains; the headroom above
/// that covers Option chains and BackRef expansions, which add schema
/// steps without consuming data nesting. Only a hostile hand-crafted
/// schema (e.g. a hundreds-deep Option chain re-entered through a BackRef)
/// can approach the cap.
pub const MAX_WALK_DEPTH: usize = 1024;

/// A parsed bundle (§6). `assets` maps `local_id → entry`; `schemas` holds
/// every logical hash the bundle references (bundles are schema-closed).
#[derive(Debug, Clone, PartialEq)]
pub struct Bundle {
    pub format_version: u32,
    pub uuid: BundleUuid,
    /// Local ID resolved by plain-path references, if declared. Must name
    /// an existing entry.
    pub primary: Option<String>,
    pub schemas: BTreeMap<LogicalHash, LogicalSchema>,
    pub assets: BTreeMap<String, AssetEntry>,
}

/// One asset entry (§6). `data` is the schema-shaped authored value; in a
/// parsed container bundle, blob leaves are `AuthoredValue::Blob` carrying
/// the chunk bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct AssetEntry {
    pub uuid: AssetUuid,
    pub type_uuid: TypeUuid,
    pub schema_hash: LogicalHash,
    /// Authoring/control metadata is never eligible as a runtime primary,
    /// processor input, reference target, query result, or pack member.
    pub authoring_only: bool,
    pub data: AuthoredValue,
}

/// Stack reserved for the recursive phases (schema walk, JSON
/// encode/decode, snapshot codec). `MAX_WALK_DEPTH` frames cost several KB
/// each in unoptimized builds; reserving explicitly makes the depth caps
/// definite errors on every caller thread, never stack overflows. The
/// reservation is virtual — untouched pages cost nothing.
const RESERVED_STACK: usize = 32 * 1024 * 1024;

/// Run `f` on a thread whose stack is guaranteed to accommodate
/// `MAX_WALK_DEPTH` recursion — a bundle op's stack budget must not depend
/// on the caller's thread.
fn on_reserved_stack<T, F>(f: F) -> Result<T, BundleError>
where
    T: Send,
    F: FnOnce() -> Result<T, BundleError> + Send,
{
    std::thread::scope(|scope| {
        match std::thread::Builder::new()
            .name("distill-bundle".to_string())
            .stack_size(RESERVED_STACK)
            .spawn_scoped(scope, f)
        {
            Ok(handle) => handle.join().unwrap_or_else(|_| {
                Err(BundleError::Internal {
                    detail: "bundle worker panicked".to_string(),
                })
            }),
            Err(e) => Err(BundleError::Internal {
                detail: format!("could not spawn bundle worker thread: {e}"),
            }),
        }
    })
}

/// Parse a bundle from file bytes, dispatching on the first byte: 0x89 →
/// container, anything else → plain-JSON envelope (§6). All validation
/// (framing, CRCs, envelope shape, schema closure, the schema-directed
/// walk, blob placement) happens here; malformed input is a definite
/// [`BundleError`], never a panic.
pub fn parse_bundle(bytes: &[u8]) -> Result<Bundle, BundleError> {
    on_reserved_stack(|| {
        if bytes.first() == Some(&0x89) {
            container::parse(bytes)
        } else {
            envelope::parse_plain(bytes)
        }
    })
}

/// Write a bundle to canonical file bytes (§6): plain JSON iff the schema
/// walk finds no blobs, the container encoding otherwise. Every byte is
/// deterministic: canonical JSON, zero padding, blobs at successive
/// 16-aligned offsets in `(local_id, encoded structural path)` order.
pub fn write_bundle(bundle: &Bundle) -> Result<Vec<u8>, BundleError> {
    on_reserved_stack(|| container::write(bundle))
}
