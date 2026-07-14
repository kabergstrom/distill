//! `StoreError`: every way the store can fail, named precisely — WHAT
//! failed and WHERE. Malformed or torn input is always a definite error,
//! never a panic; lengths are checked before allocation (§13).

use std::fmt;
use std::path::PathBuf;

/// The concrete publication that attempted to use retired schema authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetiredTypeReference {
    Asset(distill_core::id::AssetUuid),
    MigrationEndpoint(distill_core::id::LogicalHash),
}

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
    /// Persisted DSVP bytes were malformed or non-canonical.
    InvalidVersionPoison(crate::state::VersionPoisonError),
    /// Persisted DSPP fields were unknown, noncanonical, or inconsistent.
    InvalidPipelinePoison(crate::state::PipelinePoisonError),
    /// The checked-in DSB format authority could not be parsed. A binary
    /// built in this state cannot advertise bundle format v1 or reach Ready.
    InvalidBootstrapSpec(distill_core::attestation::BootstrapSpecError),
    /// A full compiled table failed its canonical DSRE/DSCA validation.
    InvalidCompiledAttestation(distill_core::attestation::AttestationError),
    /// The sealed consumer bootstrap rows did not match the candidate table.
    InvalidBootstrapAuthority(distill_core::attestation::BootstrapAuthorityMismatch),
    /// A store-side epoch summary did not equal the independently validated
    /// full compiled table from which it must be derived.
    InvalidPipelineEpoch { detail: &'static str },
    /// A candidate omitted or changed one of the five format-owned logical
    /// control rows before active lineage equality was evaluated.
    InvalidBootstrapRegistry {
        type_uuid: distill_core::id::TypeUuid,
        expected: distill_core::id::LogicalHash,
        observed: Option<distill_core::id::LogicalHash>,
    },
    /// A published-runtime poison attempted to fence a different or already
    /// unavailable epoch. The first durable transition remains authority.
    StalePublishedPipeline {
        expected: [u8; 32],
        actual: Option<[u8; 32]>,
        already_unavailable: bool,
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
    /// An explicit schema accept/rollback command named a lineage
    /// projection epoch that is no longer current (or expected one before
    /// the authoritative manifest had been projected).
    StaleSchemaManifestBase {
        expected: Box<crate::state::SchemaManifestBasis>,
        actual: Option<Box<crate::state::SchemaManifestBasis>>,
    },
    /// An explicit schema command attempted to consume a different staged
    /// candidate from the one that published `SchemaAcceptanceRequired`.
    StaleSchemaCandidate {
        expected: Box<crate::state::PipelineCandidateIdentity>,
        actual: Box<crate::state::PipelineCandidateIdentity>,
    },
    /// The command's requested cursor is not the digest compiled into the
    /// named candidate registry row (or that row is absent).
    SchemaCandidateCursorMismatch {
        type_uuid: distill_core::id::TypeUuid,
        requested: distill_core::id::LogicalHash,
        candidate: Option<distill_core::id::LogicalHash>,
    },
    /// Retirement requires the pending candidate to omit the type entirely.
    SchemaCandidateRetirementMismatch {
        type_uuid: distill_core::id::TypeUuid,
        candidate: Option<distill_core::id::LogicalHash>,
    },
    /// Reactivation requires the pending candidate to include the type.
    SchemaCandidateReactivationMismatch {
        type_uuid: distill_core::id::TypeUuid,
    },
    /// Retirement cannot strand authored entries or migration endpoints.
    SchemaRetirementBlocked {
        type_uuid: distill_core::id::TypeUuid,
        live_assets: u64,
        live_migration_endpoints: usize,
    },
    /// An authority command was derived from control reads of a different
    /// committed store snapshot than the transaction it attempted to mutate.
    StaleControlSnapshotBasis {
        provided: crate::state::SnapshotStamp,
        current: crate::state::SnapshotStamp,
    },
    /// A scanner/importer/CRUD publication attempted to reintroduce a type
    /// whose retained lineage authority is explicitly retired.
    RetiredTypeReferenced {
        type_uuid: distill_core::id::TypeUuid,
        reference: RetiredTypeReference,
    },
    /// An explicit authority command named a row in an ineligible state.
    InvalidAuthorityTransition {
        type_uuid: distill_core::id::TypeUuid,
        detail: String,
    },
    /// Target-set rows/digest were forged or non-canonical. The store
    /// recomputes DSTS at every publication and schema command.
    InvalidTargetSet(distill_core::target_set::TargetSetError),
    /// A proposed ToolEpoch capsule was incomplete or noncanonical.
    InvalidToolCapsule(distill_core::tool::ToolCapsuleError),
    /// Tool keys are nonempty NFC text and cannot carry NUL.
    InvalidToolKey,
    /// A published capsule object can no longer be revalidated against its
    /// staged closure. This is a transient launch refusal, never memoized.
    ToolCapsuleUnavailable {
        key: String,
        path: PathBuf,
        detail: &'static str,
    },
    /// Once initialized, authoritative lineage changes may only occur
    /// through candidate-bound accept or rollback APIs.
    LineageMutationRequiresCandidate,
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
    /// A wire-tree body or stored DSWL preimage was malformed, noncanonical,
    /// or did not authenticate to the requested LayoutHash.
    InvalidWireTree { detail: String },
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
            StoreError::InvalidVersionPoison(error) => error.fmt(f),
            StoreError::InvalidPipelinePoison(error) => error.fmt(f),
            StoreError::InvalidBootstrapSpec(error) => {
                write!(f, "bundle-format bootstrap authority is invalid: {error}")
            }
            StoreError::InvalidCompiledAttestation(error) => {
                write!(f, "compiled pipeline attestation is invalid: {error}")
            }
            StoreError::InvalidBootstrapAuthority(error) => {
                write!(f, "compiled pipeline bootstrap authority is invalid: {error}")
            }
            StoreError::InvalidPipelineEpoch { detail } => {
                write!(f, "pipeline epoch summary is invalid: {detail}")
            }
            StoreError::InvalidBootstrapRegistry {
                type_uuid,
                expected,
                observed,
            } => write!(
                f,
                "candidate bootstrap control {type_uuid} expected logical hash {expected}, got {observed:?}"
            ),
            StoreError::StalePublishedPipeline {
                expected,
                actual,
                already_unavailable,
            } => write!(
                f,
                "published pipeline changed before runtime poison: expected {}, actual {}, already unavailable={already_unavailable}",
                hex(expected),
                actual.map_or_else(|| "none".to_owned(), |hash| hex(&hash)),
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
            StoreError::StaleSchemaManifestBase { expected, actual } => match actual {
                Some(actual) => write!(
                    f,
                    "schema command manifest base {} is stale; current source manifest is {}",
                    expected.manifest_hash, actual.manifest_hash
                ),
                None => write!(
                    f,
                    "schema command manifest base {} is stale; no authoritative manifest is projected",
                    expected.manifest_hash
                ),
            },
            StoreError::StaleSchemaCandidate { expected, actual } => write!(
                f,
                "schema command names stale candidate dylib {:02x?}; pending candidate is {:02x?}",
                actual.dylib_hash, expected.dylib_hash
            ),
            StoreError::SchemaCandidateCursorMismatch {
                type_uuid,
                requested,
                candidate,
            } => match candidate {
                Some(candidate) => write!(
                    f,
                    "schema command requests cursor {requested} for {type_uuid}, but candidate compiled {candidate}"
                ),
                None => write!(
                    f,
                    "schema command requests cursor {requested} for {type_uuid}, but candidate has no such registry row"
                ),
            },
            StoreError::SchemaCandidateRetirementMismatch {
                type_uuid,
                candidate,
            } => write!(
                f,
                "schema retirement for {type_uuid} requires candidate omission, got {candidate:?}"
            ),
            StoreError::SchemaCandidateReactivationMismatch { type_uuid } => write!(
                f,
                "schema reactivation for {type_uuid} requires the pending candidate to include it"
            ),
            StoreError::SchemaRetirementBlocked {
                type_uuid,
                live_assets,
                live_migration_endpoints,
            } => write!(
                f,
                "schema retirement for {type_uuid} is blocked by {live_assets} live authored entries and {live_migration_endpoints} live migration endpoints"
            ),
            StoreError::StaleControlSnapshotBasis { provided, current } => write!(
                f,
                "schema authority command control basis {provided:?} is stale; current transaction basis is {current:?}"
            ),
            StoreError::RetiredTypeReferenced {
                type_uuid,
                reference,
            } => match reference {
                RetiredTypeReference::Asset(asset) => write!(
                    f,
                    "asset {asset} references retired schema authority {type_uuid}; explicit reactivation is required"
                ),
                RetiredTypeReference::MigrationEndpoint(endpoint) => write!(
                    f,
                    "migration endpoint {endpoint} references retired schema authority {type_uuid}; explicit reactivation is required"
                ),
            },
            StoreError::InvalidAuthorityTransition { type_uuid, detail } => {
                write!(f, "schema authority transition for {type_uuid} is invalid: {detail}")
            }
            StoreError::InvalidTargetSet(error) => {
                write!(f, "candidate target set fails DSTS verification: {error}")
            }
            StoreError::InvalidToolCapsule(error) => {
                write!(f, "tool execution capsule is invalid: {error}")
            }
            StoreError::InvalidToolKey => {
                write!(f, "tool key must be nonempty NFC text without NUL")
            }
            StoreError::ToolCapsuleUnavailable { key, path, detail } => write!(
                f,
                "tool capsule {key:?} is unavailable at {}: {detail}",
                path.display()
            ),
            StoreError::LineageMutationRequiresCandidate => write!(
                f,
                "an initialized schema-lineage projection may change only through a stale-base-checked candidate accept or rollback"
            ),
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
            StoreError::InvalidWireTree { detail } => {
                write!(f, "invalid DSWL wire tree: {detail}")
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
            StoreError::InvalidTargetSet(error) => Some(error),
            StoreError::InvalidToolCapsule(error) => Some(error),
            StoreError::InvalidPipelinePoison(error) => Some(error),
            StoreError::InvalidVersionPoison(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Sqlite(e)
    }
}
