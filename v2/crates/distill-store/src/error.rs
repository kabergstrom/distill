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
    /// A served-state publication failed validation inside its
    /// transaction, which rolled back.
    Rejected { detail: String },
    /// A file changed on disk since the version a read was answering for
    /// observed it: its bytes no longer hash to what that version recorded.
    /// The store holds no copy of file content; the watcher reports the
    /// change, and the read is retried once the change is published.
    Drifted { root: String, path: String },
    /// Persisted DSVP bytes were malformed or non-canonical.
    InvalidNamespaceError(crate::state::NamespaceErrorDecodeError),
    /// Persisted DSPP fields were unknown, noncanonical, or inconsistent.
    InvalidPipelineFailure(crate::state::PipelineFailureDecodeError),
    /// The checked-in DSB format authority could not be parsed. A binary
    /// built in this state cannot advertise bundle format v1 or reach Ready.
    InvalidBootstrapSpec(distill_core::bootstrap::BootstrapSpecError),
    /// A store-side epoch has invalid identity or registration metadata.
    InvalidPipelineEpoch { detail: &'static str },
    /// A candidate omitted or changed one of the format-owned logical
    /// control rows.
    InvalidBootstrapRegistry {
        type_uuid: distill_core::id::TypeUuid,
        expected: distill_core::id::LogicalHash,
        observed: Option<distill_core::id::LogicalHash>,
    },
    /// A path resolvable in more than one asset root (§13/§18): an
    /// ambiguity error, never a tiebreak.
    AmbiguousPath { path: String, roots: Vec<String> },
    /// A resolve against a poisoned bundle's UUIDs: a stable `Failed`
    /// naming the parse or index error (§13).
    BundlePoisoned {
        bundle: distill_core::id::BundleUuid,
        error: String,
    },
    /// A §10 tag query could include entries whose current-schema
    /// `load_current` indexing failed. Returning a smaller result would be
    /// unsound, so the complete canonical poisoned-bundle set is reported.
    TagIndexPoisoned {
        bundles: Vec<distill_core::id::BundleUuid>,
    },
    /// A runtime/query/pack surface attempted to select an authoring-only
    /// control entry. Tooling metadata inspection uses a separate API.
    RoleIneligible { asset: distill_core::id::AssetUuid },
    /// An authority command was derived from control reads of a different
    /// committed store snapshot than the transaction it attempted to mutate.
    StaleControlSnapshotBasis {
        provided: crate::state::SnapshotStamp,
        current: crate::state::SnapshotStamp,
    },
    /// Target-set rows/digest were forged or non-canonical. The store
    /// validates exact canonical rows at every publication and schema command.
    InvalidTargetSet(distill_core::target_set::TargetSetError),
    /// A proposed ToolEpoch identity was incomplete or noncanonical.
    InvalidToolIdentity(distill_core::tool::ToolIdentityError),
    /// Tool keys are nonempty NFC text and cannot carry NUL.
    InvalidToolKey,
    /// A published package or ambient executable is unavailable at launch.
    /// This is a transient refusal, never memoized.
    ToolUnavailable {
        key: String,
        path: PathBuf,
        detail: &'static str,
    },
    /// A persisted pipeline-state row is malformed or inconsistent.
    InvalidPipelineState { detail: String },
    /// A configuration transition was structurally invalid.
    InvalidConfiguration { error: String },
    /// Another process holds the state directory.
    StateLocked { path: PathBuf },
    /// A CAS frame failed validation at the stated segment offset:
    /// framing, CRC, or blake3 (§13's recovery verification).
    BadRecord {
        segment: u64,
        offset: u64,
        detail: String,
    },
    /// A read named a hash the extent index does not hold.
    NotFound { hash: [u8; 32] },
    /// A wire-tree body or stored DSWL preimage was malformed, noncanonical,
    /// or did not authenticate to the requested LayoutHash.
    InvalidWireTree { detail: String },
    /// A disposable exact-hash schema cache row is malformed. Rebuilding the
    /// projection is safe; doctor must not use the row as repair input.
    InvalidSchemaCache { detail: String },
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
    /// A build-import result must carry exactly one output row (§13).
    BuildImportOutputArity { got: usize },
    /// A build commit names a wire tree the CAS does not hold.
    MissingWireTree { hash: [u8; 32] },
    /// A malformed result payload (decode failure on lookup or rebuild).
    BadResultPayload { detail: String },
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
            StoreError::Rejected { detail } => write!(f, "publication rejected: {detail}"),
            StoreError::Drifted { root, path } => {
                write!(f, "{root}:{path} changed on disk since it was published")
            }
            StoreError::InvalidNamespaceError(error) => error.fmt(f),
            StoreError::InvalidPipelineFailure(error) => error.fmt(f),
            StoreError::InvalidBootstrapSpec(error) => {
                write!(f, "bundle-format bootstrap authority is invalid: {error}")
            }
            StoreError::InvalidPipelineEpoch { detail } => {
                write!(f, "pipeline epoch is invalid: {detail}")
            }
            StoreError::InvalidBootstrapRegistry {
                type_uuid,
                expected,
                observed,
            } => write!(
                f,
                "candidate bootstrap control {type_uuid} expected logical hash {expected}, got {observed:?}"
            ),
            StoreError::AmbiguousPath { path, roots } => write!(
                f,
                "path `{path}` resolves in more than one asset root: {}",
                roots.join(", ")
            ),
            StoreError::BundlePoisoned { bundle, error } => {
                write!(f, "bundle {bundle} is poisoned: {error}")
            }
            StoreError::TagIndexPoisoned { bundles } => write!(
                f,
                "tag index is poisoned for bundles: {}",
                bundles
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            StoreError::RoleIneligible { asset } => write!(
                f,
                "asset {asset} is authoring-only and ineligible for runtime selection"
            ),
            StoreError::StaleControlSnapshotBasis { provided, current } => write!(
                f,
                "schema authority command control basis {provided:?} is stale; current transaction basis is {current:?}"
            ),
            StoreError::InvalidTargetSet(error) => {
                write!(f, "candidate target rows are invalid: {error}")
            }
            StoreError::InvalidToolIdentity(error) => {
                write!(f, "tool execution identity is invalid: {error}")
            }
            StoreError::InvalidToolKey => {
                write!(f, "tool key must be nonempty NFC text without NUL")
            }
            StoreError::ToolUnavailable { key, path, detail } => write!(
                f,
                "tool {key:?} is unavailable at {}: {detail}",
                path.display()
            ),
            StoreError::InvalidPipelineState { detail } => {
                write!(f, "persisted pipeline state is invalid: {detail}")
            }
            StoreError::InvalidConfiguration { error } => {
                write!(f, "invalid configuration transition: {error}")
            }
            StoreError::StateLocked { path } => {
                write!(f, "state directory is in use by another process ({})", path.display())
            }
            StoreError::BadRecord { segment, offset, detail } => {
                write!(f, "bad CAS record in segment {segment} at offset {offset}: {detail}")
            }
            StoreError::NotFound { hash } => {
                write!(f, "no CAS extent for hash {}", hex(hash))
            }
            StoreError::InvalidWireTree { detail } => {
                write!(f, "invalid DSWL wire tree: {detail}")
            }
            StoreError::InvalidSchemaCache { detail } => {
                write!(f, "invalid cached schema snapshot: {detail}")
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
            StoreError::BuildImportOutputArity { got } => {
                write!(f, "a build-import result carries exactly one output row, got {got}")
            }
            StoreError::MissingWireTree { hash } => {
                write!(f, "wire tree {} is not in the CAS", hex(hash))
            }
            StoreError::BadResultPayload { detail } => {
                write!(f, "malformed result payload: {detail}")
            }
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
            StoreError::InvalidTargetSet(error) => Some(error),
            StoreError::InvalidToolIdentity(error) => Some(error),
            StoreError::InvalidPipelineFailure(error) => Some(error),
            StoreError::InvalidNamespaceError(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Sqlite(e)
    }
}
