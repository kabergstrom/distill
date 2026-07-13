//! `StoreError`: every way the store can fail, named precisely — WHAT
//! failed and WHERE. Malformed or torn input is always a definite error,
//! never a panic; lengths are checked before allocation (§13).

use std::fmt;
use std::path::PathBuf;

#[derive(Debug)]
pub enum StoreError {
    /// An underlying SQLite error.
    Sqlite(rusqlite::Error),
    /// An I/O error naming the path it struck.
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The metadata database was written by a different store schema
    /// version. Daemon state is disposable (§2): the caller's move is
    /// `Store::recreate`, never a silent adopt.
    SchemaVersionMismatch { found: u32, supported: u32 },
    /// §7/§13's version-global poison: the current version advanced
    /// carrying an identity-validation failure, and every
    /// namespace-facing operation fails with this same error.
    Poisoned { error: String },
    /// A path resolvable in more than one asset root (§13/§18): an
    /// ambiguity error, never a tiebreak.
    AmbiguousPath { path: String, roots: Vec<String> },
    /// A resolve against a poisoned bundle's UUIDs: a stable `Failed`
    /// naming the parse or index error (§13).
    BundlePoisoned {
        bundle: distill_core::id::BundleUuid,
        error: String,
    },
    /// General manifest projection refused a cursor-only move to an existing
    /// accepted epoch. It is a rollback (§11, §13), so it must use the
    /// explicit reverse-coverage validation path instead.
    LineageRollback {
        type_uuid: distill_core::id::TypeUuid,
        candidate: distill_core::id::LogicalHash,
        current: distill_core::id::LogicalHash,
    },
    /// The unique source-controlled lineage manifest is absent, so the
    /// disposable projection has no authority. Observed bundle stamps may
    /// diagnose this state but can never rebuild it.
    LineageManifestUnavailable,
    /// The source-controlled manifest or its disposable projection violates
    /// the accepted-history/current-cursor invariants.
    InvalidLineageManifest {
        type_uuid: Option<distill_core::id::TypeUuid>,
        detail: String,
    },
    /// An explicit rollback lacked one unambiguous, total custom migration
    /// path from a required live source schema to the requested cursor.
    IncompleteRollbackCoverage {
        type_uuid: distill_core::id::TypeUuid,
        target: distill_core::id::LogicalHash,
        source: distill_core::id::LogicalHash,
        detail: String,
    },
    /// A configuration transition was structurally invalid.
    InvalidConfiguration { error: String },
    /// The `CURRENT` generation manifest is malformed or unreadable.
    BadGenerationManifest { path: PathBuf, detail: String },
    /// A CAS frame failed validation at the stated segment offset:
    /// framing, CRC, or blake3 (§13's recovery verification).
    BadRecord {
        segment: u64,
        offset: u64,
        detail: String,
    },
    /// A read named a hash the extent index does not hold.
    NotFound { hash: [u8; 32] },
    /// The bytes read back for a hash no longer verify against it —
    /// corruption caught at read time, never returned.
    CorruptExtent { segment: u64, offset: u64 },
    /// A record's declared lengths exceed the store's caps — checked
    /// before allocation (§13).
    OversizedRecord {
        segment: u64,
        offset: u64,
        payload_len: u64,
    },
    /// A commit's derived-output assertion did not verify against the
    /// input-versioned namespace index (§9, §13) — reported, never
    /// silently recorded.
    DerivedOutputUnverified {
        child: distill_core::id::AssetUuid,
        parent: distill_core::id::AssetUuid,
        output_key: String,
    },
    /// A build-import result must carry exactly one output row (§13).
    BuildImportOutputArity { got: usize },
    /// Eviction refused: the result's outputs are pinned by a manifest
    /// entry, live lease, in-flight build, or open pack-build session —
    /// the observability rule (§13).
    Pinned { hash: [u8; 32] },
    /// A malformed result payload (decode failure on lookup or rebuild).
    BadResultPayload { detail: String },
    /// The write-intent journal was asked about an intent it never
    /// recorded, or an intent transitioned illegally.
    BadIntent { intent_id: i64, detail: String },
    /// A quarantine destination is not on the displaced inode's
    /// filesystem; copying would lose open-descriptor preservation.
    CrossFilesystemQuarantine {
        source: PathBuf,
        quarantine: PathBuf,
    },
    /// Journaled deletion found bytes other than its expected pre-image
    /// and restored/preserved them instead of publishing deletion.
    DeleteConflict {
        intent_id: i64,
        expected: [u8; 32],
        actual: [u8; 32],
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Sqlite(e) => write!(f, "sqlite: {e}"),
            StoreError::Io { path, source } => {
                write!(f, "io error at {}: {source}", path.display())
            }
            StoreError::SchemaVersionMismatch { found, supported } => write!(
                f,
                "metadata schema version {found} unsupported (this store supports {supported}); \
                 daemon state is disposable — recreate it"
            ),
            StoreError::Poisoned { error } => write!(f, "version poison: {error}"),
            StoreError::AmbiguousPath { path, roots } => write!(
                f,
                "path `{path}` resolves in more than one asset root: {}",
                roots.join(", ")
            ),
            StoreError::BundlePoisoned { bundle, error } => {
                write!(f, "bundle {bundle} is poisoned: {error}")
            }
            StoreError::LineageRollback {
                type_uuid,
                candidate,
                current,
            } => write!(
                f,
                "schema lineage for {type_uuid}: candidate current {candidate} differs from \
                 current {current} without an appended epoch — explicit rollback validation is required"
            ),
            StoreError::LineageManifestUnavailable => write!(
                f,
                "schema lineage manifest is unavailable; bundle stamps cannot establish authority"
            ),
            StoreError::InvalidLineageManifest { type_uuid, detail } => match type_uuid {
                Some(type_uuid) => {
                    write!(f, "schema lineage manifest for {type_uuid} is invalid: {detail}")
                }
                None => write!(f, "schema lineage manifest is invalid: {detail}"),
            },
            StoreError::IncompleteRollbackCoverage {
                type_uuid,
                target,
                source,
                detail,
            } => write!(
                f,
                "schema rollback for {type_uuid} to {target} lacks complete reverse coverage from {source}: {detail}"
            ),
            StoreError::InvalidConfiguration { error } => {
                write!(f, "invalid configuration transition: {error}")
            }
            StoreError::BadGenerationManifest { path, detail } => {
                write!(f, "bad CURRENT manifest at {}: {detail}", path.display())
            }
            StoreError::BadRecord { segment, offset, detail } => {
                write!(f, "bad CAS record in segment {segment} at offset {offset}: {detail}")
            }
            StoreError::NotFound { hash } => {
                write!(f, "no CAS extent for hash {}", hex(hash))
            }
            StoreError::CorruptExtent { segment, offset } => write!(
                f,
                "CAS extent in segment {segment} at offset {offset} fails hash verification"
            ),
            StoreError::OversizedRecord { segment, offset, payload_len } => write!(
                f,
                "CAS record in segment {segment} at offset {offset} declares payload_len {payload_len} \
                 beyond the segment bounds"
            ),
            StoreError::DerivedOutputUnverified { child, parent, output_key } => write!(
                f,
                "derived-output assertion {child} → ({parent}, `{output_key}`) does not verify \
                 against the namespace index"
            ),
            StoreError::BuildImportOutputArity { got } => {
                write!(f, "a build-import result carries exactly one output row, got {got}")
            }
            StoreError::Pinned { hash } => {
                write!(f, "hash {} is pinned and may not be evicted", hex(hash))
            }
            StoreError::BadResultPayload { detail } => {
                write!(f, "malformed result payload: {detail}")
            }
            StoreError::BadIntent { intent_id, detail } => {
                write!(f, "write intent {intent_id}: {detail}")
            }
            StoreError::CrossFilesystemQuarantine { source, quarantine } => write!(
                f,
                "quarantine {} is on a different filesystem from displaced inode {}",
                quarantine.display(),
                source.display()
            ),
            StoreError::DeleteConflict { intent_id, expected, actual } => write!(
                f,
                "delete intent {intent_id} displaced bytes {} instead of expected {} and was restored",
                hex(actual),
                hex(expected)
            ),
        }
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Sqlite(e) => Some(e),
            StoreError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Sqlite(e)
    }
}
