//! `BundleError`: every way a bundle can be malformed, named precisely —
//! WHAT failed and WHERE (local_id, structural path, offsets, expected vs
//! actual). Two implementers reading only the error must agree on the
//! defect. Malformed input is always a definite error, never a panic.

use std::fmt;

use distill_core::id::LogicalHash;
use ngp_schema::SnapshotError;

#[derive(Debug, Clone, PartialEq)]
pub enum BundleError {
    // ---- container framing (§6) ----
    /// Shorter than the fixed 36-byte container header.
    TooShortForHeader {
        len: usize,
    },
    /// First byte was 0x89 (container dispatch) but the 8-byte magic does
    /// not match. `line_ending_mangled` is set when the bytes look like the
    /// magic after LF→CRLF or CRLF→LF translation — the PNG-style magic
    /// exists to detect exactly that, loudly.
    BadMagic {
        found: [u8; 8],
        line_ending_mangled: bool,
    },
    /// Header `version` field is not a supported container encoding version.
    UnsupportedContainerVersion {
        found: u32,
    },
    /// `36 + json_len + pad + blob_len` overflowed u64 — the header lengths
    /// are garbage. Checked before any allocation.
    HeaderOverflow {
        json_len: u64,
        blob_len: u64,
    },
    /// File is shorter than the header's `36 + json_len + pad + blob_len`.
    /// Checked before any allocation.
    Truncated {
        expected: u64,
        actual: u64,
    },
    /// File is longer than the header's `36 + json_len + pad + blob_len` —
    /// exact size is normative.
    TrailingBytes {
        expected: u64,
        actual: u64,
    },
    /// CRC-32C of the JSON chunk does not match the header field.
    JsonCrcMismatch {
        expected: u32,
        actual: u32,
    },
    /// CRC-32C of the blob chunk does not match the header field.
    BlobCrcMismatch {
        expected: u32,
        actual: u32,
    },
    /// A pad byte between the JSON chunk and the blob chunk is nonzero.
    /// `file_offset` is absolute in the file.
    NonzeroPadByte {
        file_offset: u64,
    },
    /// A byte in an inter-blob alignment gap is nonzero. `chunk_offset` is
    /// relative to the blob chunk start.
    NonzeroGapByte {
        chunk_offset: u64,
    },

    // ---- text ----
    /// The envelope text (whole file for plain, JSON chunk for container)
    /// is not valid UTF-8; `offset` is where decoding failed.
    NotUtf8 {
        offset: usize,
    },
    /// The envelope text is not valid JSON (duplicate keys included).
    Json(distill_json::ParseError),
    /// Defensive: re-serializing a parsed subtree failed. Unreachable from
    /// parser-produced values (they contain no blobs or non-finite floats).
    JsonWrite(distill_json::WriteError),

    // ---- envelope (§6) ----
    EnvelopeNotObject {
        found: &'static str,
    },
    /// Canonical bundles carry exactly {assets, format_version, primary?,
    /// schemas, uuid}; anything else is an error.
    UnknownEnvelopeKey {
        key: String,
    },
    MissingEnvelopeKey {
        key: &'static str,
    },
    FormatVersionNotUInt {
        found: &'static str,
    },
    /// `format_version` parsed but is not a version this crate supports
    /// (supported: exactly 1).
    UnsupportedFormatVersion {
        found: u128,
    },
    BadBundleUuid {
        found: String,
    },
    PrimaryNotString {
        found: &'static str,
    },
    /// `primary` names a local_id with no entry in `assets`.
    PrimaryNotFound {
        primary: String,
    },
    /// A bundle primary must be runtime content.
    PrimaryIsAuthoringOnly {
        primary: String,
    },
    SchemasNotObject {
        found: &'static str,
    },
    AssetsNotObject {
        found: &'static str,
    },
    /// A `schemas` key is not a 64-char hex LogicalHash.
    BadSchemaKey {
        key: String,
    },
    /// Two `schemas` keys spell the same hash (e.g. case variants).
    DuplicateSchema {
        hash: LogicalHash,
    },
    /// A schema snapshot failed to verify against its map key — includes
    /// `SnapshotError::HashMismatch { expected, actual }` when the key is
    /// not the snapshot's own hash.
    Schema {
        hash: LogicalHash,
        error: SnapshotError,
    },
    /// A `$`-prefixed local_id other than exactly `$settings` or `$record`
    /// (§6 reserved namespace) — an integrity error at every boundary.
    ReservedLocalId {
        local_id: String,
    },
    EntryNotObject {
        local_id: String,
        found: &'static str,
    },
    /// Entries carry exactly {data, schema_hash, type_uuid, uuid}.
    UnknownEntryKey {
        local_id: String,
        key: String,
    },
    MissingEntryKey {
        local_id: String,
        key: &'static str,
    },
    AuthoringOnlyNotBool {
        local_id: String,
        found: &'static str,
    },
    ReservedEntryMustBeAuthoringOnly {
        local_id: String,
    },
    /// `uuid` / `type_uuid` / `schema_hash` failed to parse; `field` names
    /// which.
    BadEntryId {
        local_id: String,
        field: &'static str,
        found: String,
    },
    /// Schema-closure violation (§6): the entry's `schema_hash` does not
    /// resolve in the bundle's own `schemas`.
    MissingSchema {
        local_id: String,
        schema_hash: LogicalHash,
    },
    EntryLineage {
        local_id: String,
        detail: &'static str,
    },

    // ---- schema-directed walk ----
    /// The data value has the wrong JSON shape for the schema node at
    /// `path` (paths are rooted at the entry's `data`).
    Shape {
        local_id: String,
        path: String,
        expected: String,
        found: String,
    },
    /// Struct field required by the schema, absent in the data — canonical
    /// bundles are total (§6 Adoption).
    MissingField {
        local_id: String,
        path: String,
        field: String,
    },
    /// Data object key with no corresponding schema field.
    ExtraField {
        local_id: String,
        path: String,
        field: String,
    },
    UnknownVariant {
        local_id: String,
        path: String,
        variant: String,
    },
    /// Enum values are single-key `{ "Variant": payload }` objects; this
    /// one had `keys` keys.
    EnumShape {
        local_id: String,
        path: String,
        keys: usize,
    },
    ArrayLen {
        local_id: String,
        path: String,
        expected: u64,
        actual: u64,
    },
    /// Set elements / non-string map keys not strictly ascending by their
    /// canonical encoded bytes (duplicates included). `index` is the first
    /// offending element/pair.
    NotSorted {
        local_id: String,
        path: String,
        what: &'static str,
        index: usize,
    },
    /// A set element or map key could not be canonically encoded (blob
    /// bytes or non-finite float inside).
    KeyNotEncodable {
        local_id: String,
        path: String,
        what: &'static str,
    },
    /// NaN/±Inf at a float leaf (write side; the parser can't produce one).
    NonFiniteFloat {
        local_id: String,
        path: String,
    },
    /// A finite authored number does not fit in IEEE-754 binary32.
    F32OutOfRange {
        local_id: String,
        path: String,
    },
    /// The authored numeric value rounds to binary32, but its canonical
    /// JSON text is not the shortest decimal that reparses to those same
    /// bits under round-to-nearest-ties-even.
    NonCanonicalF32 {
        local_id: String,
        path: String,
        found: String,
        canonical: String,
    },
    /// The schema walk reached a Blob node in a plain-JSON bundle — blobs
    /// require the container encoding (§6).
    BlobInPlainBundle {
        local_id: String,
        path: String,
    },
    /// A Blob node beneath a map key or set element — barred by §5 (the
    /// ordering/offset circularity).
    BlobBarred {
        local_id: String,
        path: String,
    },
    /// BackRef distance points past the outermost expanded frame.
    BadBackRef {
        local_id: String,
        path: String,
        distance: u32,
        frames: usize,
    },
    /// Walk nesting exceeded `MAX_WALK_DEPTH` — a definite error, never a
    /// stack overflow (§12's cap discipline).
    WalkDepth {
        local_id: String,
        path: String,
    },

    // ---- blob placement (container, §6) ----
    /// The value at a Blob node is not exactly `{"len": n, "offset": m}`
    /// with u64 values.
    BadBlobObject {
        local_id: String,
        path: String,
        detail: String,
    },
    /// offset + len exceeds the blob chunk (checked arithmetic).
    BlobOutOfBounds {
        local_id: String,
        path: String,
        offset: u64,
        len: u64,
        blob_len: u64,
    },
    /// Blob offset is not 16-byte aligned.
    MisalignedBlob {
        local_id: String,
        path: String,
        offset: u64,
    },
    /// Blob starts before the previous blob (in canonical (local_id, path)
    /// order) ends.
    OverlappingBlobs {
        local_id: String,
        path: String,
        offset: u64,
        prev_end: u64,
    },
    /// Blob is not at the canonical position: the next 16-aligned offset
    /// after the previous blob in (local_id, path) order.
    BlobNotAtCanonicalOffset {
        local_id: String,
        path: String,
        expected: u64,
        actual: u64,
    },
    /// `blob_len` must equal the end of the last blob exactly (0 with no
    /// blobs).
    BlobChunkLength {
        expected: u64,
        actual: u64,
    },

    /// An internal invariant broke (e.g. a writer pass disagreeing with
    /// its own collection pass). Unreachable by construction; surfaced as
    /// an error rather than a panic.
    Internal {
        detail: String,
    },
}

impl fmt::Display for BundleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use BundleError as E;
        match self {
            E::TooShortForHeader { len } => write!(
                f,
                "container is {len} bytes, shorter than the 36-byte header"
            ),
            E::BadMagic {
                found,
                line_ending_mangled,
            } => {
                write!(f, "bad container magic {found:02x?}")?;
                if *line_ending_mangled {
                    write!(f, " — looks like line-ending translation mangled the file")?;
                }
                Ok(())
            }
            E::UnsupportedContainerVersion { found } => {
                write!(f, "unsupported container version {found} (supported: 1)")
            }
            E::HeaderOverflow { json_len, blob_len } => write!(
                f,
                "header lengths overflow: json_len {json_len} + blob_len {blob_len}"
            ),
            E::Truncated { expected, actual } => write!(
                f,
                "file truncated or header lengths exceed it: header demands {expected} bytes, file has {actual}"
            ),
            E::TrailingBytes { expected, actual } => write!(
                f,
                "file too long: header accounts for {expected} bytes, file has {actual}"
            ),
            E::JsonCrcMismatch { expected, actual } => write!(
                f,
                "json chunk CRC-32C mismatch: header says {expected:#010x}, bytes hash to {actual:#010x}"
            ),
            E::BlobCrcMismatch { expected, actual } => write!(
                f,
                "blob chunk CRC-32C mismatch: header says {expected:#010x}, bytes hash to {actual:#010x}"
            ),
            E::NonzeroPadByte { file_offset } => {
                write!(f, "nonzero pad byte at file offset {file_offset}")
            }
            E::NonzeroGapByte { chunk_offset } => write!(
                f,
                "nonzero inter-blob gap byte at blob-chunk offset {chunk_offset}"
            ),
            E::NotUtf8 { offset } => {
                write!(f, "envelope text is not UTF-8 (at byte {offset})")
            }
            E::Json(e) => write!(f, "envelope: {e}"),
            E::JsonWrite(e) => write!(f, "internal JSON re-serialization failed: {e}"),
            E::EnvelopeNotObject { found } => {
                write!(f, "envelope must be a JSON object, found {found}")
            }
            E::UnknownEnvelopeKey { key } => write!(f, "unknown envelope key {key:?}"),
            E::MissingEnvelopeKey { key } => write!(f, "missing envelope key {key:?}"),
            E::FormatVersionNotUInt { found } => write!(
                f,
                "format_version must be an unsigned integer, found {found}"
            ),
            E::UnsupportedFormatVersion { found } => {
                write!(f, "unsupported format_version {found} (supported: 1)")
            }
            E::BadBundleUuid { found } => {
                write!(f, "bundle uuid {found:?} is not a hyphenated uuid")
            }
            E::PrimaryNotString { found } => {
                write!(f, "primary must be a string, found {found}")
            }
            E::PrimaryNotFound { primary } => {
                write!(f, "primary {primary:?} names no entry in assets")
            }
            E::PrimaryIsAuthoringOnly { primary } => {
                write!(f, "primary {primary:?} is authoring-only runtime-ineligible metadata")
            }
            E::SchemasNotObject { found } => {
                write!(f, "schemas must be a JSON object, found {found}")
            }
            E::AssetsNotObject { found } => {
                write!(f, "assets must be a JSON object, found {found}")
            }
            E::BadSchemaKey { key } => write!(
                f,
                "schemas key {key:?} is not a 64-char hex logical hash"
            ),
            E::DuplicateSchema { hash } => {
                write!(f, "schemas spells hash {hash} twice")
            }
            E::Schema { hash, error } => {
                write!(f, "schema snapshot under key {hash}: {error}")
            }
            E::ReservedLocalId { local_id } => write!(
                f,
                "local_id {local_id:?}: the $ namespace is reserved; only \"$settings\" and \"$record\" are valid"
            ),
            E::EntryNotObject { local_id, found } => {
                write!(f, "asset {local_id:?}: entry must be an object, found {found}")
            }
            E::UnknownEntryKey { local_id, key } => {
                write!(f, "asset {local_id:?}: unknown entry key {key:?}")
            }
            E::MissingEntryKey { local_id, key } => {
                write!(f, "asset {local_id:?}: missing entry key {key:?}")
            }
            E::AuthoringOnlyNotBool { local_id, found } => write!(
                f,
                "asset {local_id:?}: authoring_only must be a boolean, found {found}"
            ),
            E::ReservedEntryMustBeAuthoringOnly { local_id } => write!(
                f,
                "reserved control entry {local_id:?} must set authoring_only=true"
            ),
            E::BadEntryId {
                local_id,
                field,
                found,
            } => write!(f, "asset {local_id:?}: {field} {found:?} is malformed"),
            E::MissingSchema {
                local_id,
                schema_hash,
            } => write!(
                f,
                "asset {local_id:?}: schema_hash {schema_hash} does not resolve in this bundle's schemas (bundles are schema-closed)"
            ),
            E::EntryLineage { local_id, detail } => {
                write!(f, "asset {local_id:?} has invalid entry lineage: {detail}")
            }
            E::Shape {
                local_id,
                path,
                expected,
                found,
            } => write!(
                f,
                "asset {local_id:?} at {path}: expected {expected}, found {found}"
            ),
            E::MissingField {
                local_id,
                path,
                field,
            } => write!(
                f,
                "asset {local_id:?} at {path}: missing struct field {field:?}"
            ),
            E::ExtraField {
                local_id,
                path,
                field,
            } => write!(
                f,
                "asset {local_id:?} at {path}: field {field:?} is not in the schema"
            ),
            E::UnknownVariant {
                local_id,
                path,
                variant,
            } => write!(
                f,
                "asset {local_id:?} at {path}: unknown enum variant {variant:?}"
            ),
            E::EnumShape {
                local_id,
                path,
                keys,
            } => write!(
                f,
                "asset {local_id:?} at {path}: enum value must be a single-key {{\"Variant\": payload}} object, found {keys} keys"
            ),
            E::ArrayLen {
                local_id,
                path,
                expected,
                actual,
            } => write!(
                f,
                "asset {local_id:?} at {path}: fixed array expects {expected} elements, found {actual}"
            ),
            E::NotSorted {
                local_id,
                path,
                what,
                index,
            } => write!(
                f,
                "asset {local_id:?} at {path}: {what} not strictly ascending by encoded bytes (element {index})"
            ),
            E::KeyNotEncodable {
                local_id,
                path,
                what,
            } => write!(
                f,
                "asset {local_id:?} at {path}: {what} has no canonical JSON encoding (blob or non-finite float inside)"
            ),
            E::NonFiniteFloat { local_id, path } => write!(
                f,
                "asset {local_id:?} at {path}: NaN/Inf is not authorable (§6)"
            ),
            E::F32OutOfRange { local_id, path } => write!(
                f,
                "asset {local_id:?} at {path}: number is outside finite binary32 range"
            ),
            E::NonCanonicalF32 {
                local_id,
                path,
                found,
                canonical,
            } => write!(
                f,
                "asset {local_id:?} at {path}: f32 value {found} must use shortest ties-even roundtrip decimal {canonical}"
            ),
            E::BlobInPlainBundle { local_id, path } => write!(
                f,
                "asset {local_id:?} at {path}: blob field in a plain-JSON bundle — blobs require the container encoding"
            ),
            E::BlobBarred { local_id, path } => write!(
                f,
                "asset {local_id:?} at {path}: blob beneath a map key or set element is unserializable (§5)"
            ),
            E::BadBackRef {
                local_id,
                path,
                distance,
                frames,
            } => write!(
                f,
                "asset {local_id:?} at {path}: backref distance {distance} exceeds {frames} open frames"
            ),
            E::WalkDepth { local_id, path } => write!(
                f,
                "asset {local_id:?} at {path}: schema walk exceeded the nesting cap"
            ),
            E::BadBlobObject {
                local_id,
                path,
                detail,
            } => write!(
                f,
                "asset {local_id:?} at {path}: blob field must be {{\"len\": n, \"offset\": m}}: {detail}"
            ),
            E::BlobOutOfBounds {
                local_id,
                path,
                offset,
                len,
                blob_len,
            } => write!(
                f,
                "asset {local_id:?} at {path}: blob [{offset}, {offset}+{len}) exceeds blob chunk of {blob_len} bytes"
            ),
            E::MisalignedBlob {
                local_id,
                path,
                offset,
            } => write!(
                f,
                "asset {local_id:?} at {path}: blob offset {offset} is not 16-byte aligned"
            ),
            E::OverlappingBlobs {
                local_id,
                path,
                offset,
                prev_end,
            } => write!(
                f,
                "asset {local_id:?} at {path}: blob at offset {offset} overlaps the previous blob ending at {prev_end}"
            ),
            E::BlobNotAtCanonicalOffset {
                local_id,
                path,
                expected,
                actual,
            } => write!(
                f,
                "asset {local_id:?} at {path}: blob at offset {actual}, canonical layout demands {expected}"
            ),
            E::BlobChunkLength { expected, actual } => write!(
                f,
                "blob_len is {actual} but the last blob ends at {expected} — blob_len must equal the end of the last blob"
            ),
            E::Internal { detail } => write!(f, "internal invariant broke: {detail}"),
        }
    }
}

impl std::error::Error for BundleError {}
