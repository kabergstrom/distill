//! §13 consistency-contract state machinery.
//!
//! Two sequencing domains, deliberately distinct: watcher batches and
//! authoring operations advance the **input version** (what snapshots pin
//! and `Drifted` compares against); build results advance only the **memo
//! sequence**, attaching outputs to an input basis without advancing any
//! input version.
//!
//! Boundary note: the full `PipelineEpoch` of §3/§9 holds module-owned
//! trait objects and function pointers, and `dlclose` of a retired module
//! is gated on the epoch `Arc`'s strong count — that machinery lives with
//! the module host, outside this crate. The store-side contract needs the
//! epoch's *identity*: the pipeline dylib content hash (an input-hash
//! input wherever pipeline code runs), the importer/processor
//! registrations and versions, and the load-policy digest — exactly the
//! `pipeline_state` row (§13). That is what [`PipelineEpoch`] here
//! carries; residency is still expressed the spec's way (`Arc`), so pin
//! counting composes when the module host wraps it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use distill_core::attestation::CompiledAttestationDigest;
use distill_core::canonical::{domain_digest, CanonicalEncoder, DSCP, DSPP, DSVP};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::target_set::{CanonicalTargetSet, TargetSetError, TargetSetHash};
use ngp_schema::identity::CompilationIdentity;

/// Advanced by watcher batches + authoring ops — module/schema artifact
/// swaps and config edits arrive as watcher events, so epoch rotation is
/// an input event. Ordered only WITHIN one store instance: never a
/// universal identity (§13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InputVersion(pub u64);

/// Advanced by build-result commits (§13) — the memo sequence, separate
/// from the input version by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemoSeq(pub u64);

/// Minted randomly when `.distill/` is created — and re-minted whenever
/// daemon state is rebuilt from scratch (§13). InputVersion counters
/// restart after state loss, so a bare u64 could alias two unrelated
/// versions across a client reconnect; every version that crosses the RPC
/// boundary is instance-qualified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoreInstanceId(pub [u8; 16]);

impl StoreInstanceId {
    /// Mint a fresh random instance id (creation / state rebuild, §13).
    pub fn mint() -> Self {
        let mut bytes = [0u8; 16];
        getrandom::getrandom(&mut bytes).expect("OS randomness unavailable");
        StoreInstanceId(bytes)
    }
}

impl fmt::Display for StoreInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// The instance-qualified version pair (§13): the **snapshot stamp**. On
/// the loader's IO surface this is the **RPC-side realization** of the
/// IO-neutral basis token (`IoBasis::Rpc`, §15); PackfileIO's realization
/// is the mounted manifest hash, and the stamp never appears in pack
/// outcomes. A client observing a changed instance discards every held
/// version comparison and re-resolves from scratch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotStamp {
    pub instance: StoreInstanceId,
    pub version: InputVersion,
}

impl SnapshotStamp {
    /// Whether `other`'s version numbers are comparable with this stamp's
    /// at all: bare `InputVersion`s are ordered only within one instance.
    pub fn same_instance(&self, other: &SnapshotStamp) -> bool {
        self.instance == other.instance
    }
}

/// An importer or processor registration recorded in `pipeline_state`
/// (§13): id + version, the identity inputs of §9's input hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub kind: RegistrationKind,
    pub id: String,
    pub version: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationKind {
    Importer,
    Processor,
}

/// Store-side pipeline-epoch identity — the `pipeline_state` row (§13).
/// See the module docs for the boundary with §3/§9's full epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineEpoch {
    /// The pipeline dylib content hash — an input-hash input wherever
    /// pipeline code runs (§9, §13).
    pub dylib_hash: [u8; 32],
    /// blake3 over the sorted `(type_uuid, build_only)` pairs of the
    /// current registry (§9, §13) — input-versioned change tracking for
    /// the deliberately unhashed `build_only` bit (§5).
    pub load_policy_digest: [u8; 32],
    /// Aggregate over the complete compiled type rows. This is part of the
    /// staged-candidate identity used by explicit schema commands.
    pub compiled_types: CompiledAttestationDigest,
    /// Complete canonical target-definition set used to construct the
    /// candidate pipeline map. The store independently recomputes DSTS from
    /// these rows before publishing and before every schema command.
    pub target_set: CanonicalTargetSet,
    /// The candidate's complete compiled registry projection. `Ready`
    /// requires exact key/value equality with the authoritative lineage
    /// manifest's current cursors; a missing, extra, or unequal row is a
    /// typed schema-acceptance requirement instead.
    pub schema_registry: BTreeMap<TypeUuid, LogicalHash>,
    /// Importer/processor registrations and versions.
    pub registrations: Vec<Registration>,
}

/// Store-side identity of a candidate whose compiled schema projection is
/// awaiting explicit acceptance or rollback. The staged dylib binds the
/// registration/code identity, DSCA binds the complete compiled type table,
/// and the target-set hash prevents a candidate built for different targets
/// from consuming the pending command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineCandidateIdentity {
    pub dylib_hash: [u8; 32],
    pub compiled_types: CompiledAttestationDigest,
    pub target_set_hash: TargetSetHash,
}

impl TryFrom<&PipelineEpoch> for PipelineCandidateIdentity {
    type Error = TargetSetError;

    fn try_from(epoch: &PipelineEpoch) -> Result<Self, Self::Error> {
        let target_set = CanonicalTargetSet::from_canonical(
            epoch.target_set.rows.clone(),
            epoch.target_set.digest,
        )?;
        Ok(Self {
            dylib_hash: epoch.dylib_hash,
            compiled_types: epoch.compiled_types,
            target_set_hash: target_set.digest,
        })
    }
}

/// One exact registry-versus-authority disagreement. `None` names a row
/// missing from that side, so missing, extra, and unequal cases share one
/// stable typed representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaRegistryMismatch {
    pub type_uuid: TypeUuid,
    pub candidate: Option<LogicalHash>,
    pub manifest: Option<LogicalHash>,
}

/// Exact stale-base identity of the verified source-controlled manifest.
/// The file hash prevents ABA across append-only history changes; the full
/// sorted cursor projection makes the requested selection explicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaManifestBasis {
    pub manifest_hash: ContentHash,
    pub current_cursors: BTreeMap<TypeUuid, LogicalHash>,
}

/// A candidate that cannot become `Ready` until explicit schema acceptance
/// or rollback publishes another verified source-controlled manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaAcceptanceRequired {
    pub manifest: SchemaManifestBasis,
    pub candidate: PipelineCandidateIdentity,
    pub mismatches: Vec<SchemaRegistryMismatch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PipelinePoisonCode {
    CandidateOpen = 1,
    CandidateAttestation = 2,
    CandidateRegistration = 3,
    CandidateValidation = 4,
    CandidateCleanup = 5,
    PublishedCallbackPanic = 6,
    PublishedCallbackRejected = 7,
    PublishedCleanup = 8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PipelinePoisonOrigin {
    CandidateOpen = 1,
    PublishedRuntime = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum CleanupDisposition {
    None = 0,
    CleanedAndClosed = 1,
    RegistrationCleanupFailed = 2,
    ModuleUnloadFailed = 3,
    TokenPoisoned = 4,
    TokenPinned = 5,
    DlcloseFailed = 6,
    PublishedEpochLeaked = 7,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelinePoison {
    pub code: PipelinePoisonCode,
    pub origin: PipelinePoisonOrigin,
    pub cleanup: CleanupDisposition,
    pub identity: [u8; 32],
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelinePoisonError {
    UnknownCode(u16),
    UnknownOrigin(u16),
    UnknownCleanup(u16),
    InvalidMatrix,
    IdentityMismatch,
}

impl PipelinePoison {
    pub fn new(
        code: PipelinePoisonCode,
        origin: PipelinePoisonOrigin,
        cleanup: CleanupDisposition,
        message: impl Into<String>,
    ) -> Result<Self, PipelinePoisonError> {
        validate_pipeline_poison_matrix(code, origin, cleanup)?;
        Ok(Self {
            code,
            origin,
            cleanup,
            identity: pipeline_poison_identity(code, origin, cleanup),
            message: message.into(),
        })
    }

    pub fn from_wire(
        code: u16,
        origin: u16,
        cleanup: u16,
        identity: [u8; 32],
        message: impl Into<String>,
    ) -> Result<Self, PipelinePoisonError> {
        let value = Self::new(
            PipelinePoisonCode::try_from(code)?,
            PipelinePoisonOrigin::try_from(origin)?,
            CleanupDisposition::try_from(cleanup)?,
            message,
        )?;
        if value.identity != identity {
            return Err(PipelinePoisonError::IdentityMismatch);
        }
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), PipelinePoisonError> {
        validate_pipeline_poison_matrix(self.code, self.origin, self.cleanup)?;
        if self.identity != pipeline_poison_identity(self.code, self.origin, self.cleanup) {
            return Err(PipelinePoisonError::IdentityMismatch);
        }
        Ok(())
    }
}

fn pipeline_poison_identity(
    code: PipelinePoisonCode,
    origin: PipelinePoisonOrigin,
    cleanup: CleanupDisposition,
) -> [u8; 32] {
    let mut encoder = CanonicalEncoder::new();
    encoder.raw(&DSPP);
    encoder.u8(1);
    encoder.u16(code as u16);
    encoder.u16(origin as u16);
    encoder.u16(cleanup as u16);
    *blake3::hash(&encoder.into_bytes()).as_bytes()
}

fn validate_pipeline_poison_matrix(
    code: PipelinePoisonCode,
    origin: PipelinePoisonOrigin,
    cleanup: CleanupDisposition,
) -> Result<(), PipelinePoisonError> {
    let valid = match origin {
        PipelinePoisonOrigin::CandidateOpen => match code {
            PipelinePoisonCode::CandidateOpen
            | PipelinePoisonCode::CandidateAttestation
            | PipelinePoisonCode::CandidateRegistration
            | PipelinePoisonCode::CandidateValidation => matches!(
                cleanup,
                CleanupDisposition::None | CleanupDisposition::CleanedAndClosed
            ),
            PipelinePoisonCode::CandidateCleanup => matches!(
                cleanup,
                CleanupDisposition::RegistrationCleanupFailed
                    | CleanupDisposition::ModuleUnloadFailed
                    | CleanupDisposition::TokenPoisoned
                    | CleanupDisposition::TokenPinned
                    | CleanupDisposition::DlcloseFailed
            ),
            PipelinePoisonCode::PublishedCallbackPanic
            | PipelinePoisonCode::PublishedCallbackRejected
            | PipelinePoisonCode::PublishedCleanup => false,
        },
        PipelinePoisonOrigin::PublishedRuntime => {
            matches!(
                code,
                PipelinePoisonCode::PublishedCallbackPanic
                    | PipelinePoisonCode::PublishedCallbackRejected
                    | PipelinePoisonCode::PublishedCleanup
            ) && cleanup == CleanupDisposition::PublishedEpochLeaked
        }
    };
    valid
        .then_some(())
        .ok_or(PipelinePoisonError::InvalidMatrix)
}

macro_rules! pipeline_poison_try_from {
    ($type:ty, $error:ident, {$($value:literal => $variant:ident),+ $(,)?}) => {
        impl TryFrom<u16> for $type {
            type Error = PipelinePoisonError;
            fn try_from(value: u16) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    other => Err(PipelinePoisonError::$error(other)),
                }
            }
        }
    };
}

pipeline_poison_try_from!(PipelinePoisonCode, UnknownCode, {
    1 => CandidateOpen, 2 => CandidateAttestation, 3 => CandidateRegistration,
    4 => CandidateValidation, 5 => CandidateCleanup, 6 => PublishedCallbackPanic,
    7 => PublishedCallbackRejected, 8 => PublishedCleanup,
});
pipeline_poison_try_from!(PipelinePoisonOrigin, UnknownOrigin, {
    1 => CandidateOpen, 2 => PublishedRuntime,
});
pipeline_poison_try_from!(CleanupDisposition, UnknownCleanup, {
    0 => None, 1 => CleanedAndClosed, 2 => RegistrationCleanupFailed,
    3 => ModuleUnloadFailed, 4 => TokenPoisoned, 5 => TokenPinned,
    6 => DlcloseFailed, 7 => PublishedEpochLeaked,
});

/// Snapshot-pinned identity of a validated configuration generation.
/// Full values are owned by the daemon configuration layer; the store
/// persists the generation that served a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationEpoch {
    pub generation: u64,
}

/// Closed DSCP v1 discriminants. Persisted/wire values outside this set
/// reject; there is deliberately no extensible `Other` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ConfigurationPoisonCode {
    MalformedConfiguration = 1,
    NonLoopbackAddress = 2,
    DuplicateRootName = 3,
    InvalidPath = 4,
    OwnedPathOverlap = 5,
    EmptyTargetApis = 6,
    InvalidParallelism = 7,
    InvalidBatchReservation = 8,
    DirectoryAlias = 9,
    MissingLineageManifest = 10,
    DuplicateLineageManifest = 11,
    UnsupportedTargetIdentity = 12,
    DuplicateTargetName = 13,
}

impl TryFrom<u16> for ConfigurationPoisonCode {
    type Error = UnknownConfigurationPoisonCode;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::MalformedConfiguration),
            2 => Ok(Self::NonLoopbackAddress),
            3 => Ok(Self::DuplicateRootName),
            4 => Ok(Self::InvalidPath),
            5 => Ok(Self::OwnedPathOverlap),
            6 => Ok(Self::EmptyTargetApis),
            7 => Ok(Self::InvalidParallelism),
            8 => Ok(Self::InvalidBatchReservation),
            9 => Ok(Self::DirectoryAlias),
            10 => Ok(Self::MissingLineageManifest),
            11 => Ok(Self::DuplicateLineageManifest),
            12 => Ok(Self::UnsupportedTargetIdentity),
            13 => Ok(Self::DuplicateTargetName),
            unknown => Err(UnknownConfigurationPoisonCode(unknown)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownConfigurationPoisonCode(pub u16);

impl fmt::Display for UnknownConfigurationPoisonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown ConfigurationPoisonCode {}", self.0)
    }
}

impl std::error::Error for UnknownConfigurationPoisonCode {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DscpError {
    UnsupportedVersion(u8),
    CodeDetailMismatch,
    UnknownPathKey(u8),
    UnknownOwnedPathKind(u8),
    Truncated,
    TrailingBytes,
    InvalidUtf8,
    NonCanonical,
    InvalidText,
}

impl fmt::Display for DscpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid DSCP detail: {self:?}")
    }
}

impl std::error::Error for DscpError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConfigurationPathKey {
    AssetRoot = 1,
    StatePath = 2,
    SchemaArtifact = 3,
    PipelineModule = 4,
    CodegenOutput = 5,
    Quarantine = 6,
    ImportDestination = 7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OwnedPathKind {
    AssetRoot = 1,
    DaemonState = 2,
    SchemaArtifact = 3,
    PipelineModule = 4,
    CodegenOutput = 5,
    Quarantine = 6,
    ImportDestination = 7,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedPathSide {
    pub kind: OwnedPathKind,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryAliasSide {
    pub normalized_path: String,
    pub device: u64,
    pub inode: u64,
}

/// Exact, closed typed facts hashed by DSCP v1. Presentation prose never
/// enters this value; callers supply it separately when publishing poison.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // DSCP's public variant field types are protocol grammar.
pub enum DscpV1 {
    MalformedConfiguration {
        file_hash: [u8; 32],
    },
    NonLoopbackAddress {
        address: String,
    },
    DuplicateRootName {
        normalized_name: String,
    },
    InvalidPath {
        key: ConfigurationPathKey,
        normalized_or_raw_path: String,
    },
    OwnedPathOverlap {
        first: OwnedPathSide,
        second: OwnedPathSide,
    },
    EmptyTargetApis {
        target: String,
    },
    InvalidParallelism {
        value: u32,
    },
    InvalidBatchReservation {
        parallelism: u32,
        reservation: u32,
    },
    DirectoryAlias {
        first: DirectoryAliasSide,
        second: DirectoryAliasSide,
    },
    MissingLineageManifest,
    DuplicateLineageManifest {
        entries: Vec<AssetUuid>,
    },
    UnsupportedTargetIdentity {
        target: String,
        expected: CompilationIdentity,
        observed: CompilationIdentity,
    },
    DuplicateTargetName {
        normalized_name: String,
    },
}

impl DscpV1 {
    pub fn code(&self) -> ConfigurationPoisonCode {
        match self {
            Self::MalformedConfiguration { .. } => ConfigurationPoisonCode::MalformedConfiguration,
            Self::NonLoopbackAddress { .. } => ConfigurationPoisonCode::NonLoopbackAddress,
            Self::DuplicateRootName { .. } => ConfigurationPoisonCode::DuplicateRootName,
            Self::InvalidPath { .. } => ConfigurationPoisonCode::InvalidPath,
            Self::OwnedPathOverlap { .. } => ConfigurationPoisonCode::OwnedPathOverlap,
            Self::EmptyTargetApis { .. } => ConfigurationPoisonCode::EmptyTargetApis,
            Self::InvalidParallelism { .. } => ConfigurationPoisonCode::InvalidParallelism,
            Self::InvalidBatchReservation { .. } => {
                ConfigurationPoisonCode::InvalidBatchReservation
            }
            Self::DirectoryAlias { .. } => ConfigurationPoisonCode::DirectoryAlias,
            Self::MissingLineageManifest => ConfigurationPoisonCode::MissingLineageManifest,
            Self::DuplicateLineageManifest { .. } => {
                ConfigurationPoisonCode::DuplicateLineageManifest
            }
            Self::UnsupportedTargetIdentity { .. } => {
                ConfigurationPoisonCode::UnsupportedTargetIdentity
            }
            Self::DuplicateTargetName { .. } => ConfigurationPoisonCode::DuplicateTargetName,
        }
    }

    /// `blake3("DSCP" || 0x01 || code:u16 || exact variant fields)`.
    /// Symmetric records and unordered lineage UUIDs are canonicalized here,
    /// so callers cannot publish an order-dependent state.
    pub fn reason_hash(&self) -> [u8; 32] {
        domain_digest(DSCP, 1, |encoder| {
            encoder.u16(self.code() as u16);
            encoder.raw(&self.canonical_detail_bytes());
        })
    }

    /// Exact bytes stored beside the explicit DSCP version and code.
    pub fn canonical_detail_bytes(&self) -> Vec<u8> {
        let mut encoder = CanonicalEncoder::new();
        encode_dscp_detail(&mut encoder, self);
        encoder.into_bytes()
    }

    pub fn from_canonical_detail_bytes(
        code: ConfigurationPoisonCode,
        bytes: &[u8],
    ) -> Result<Self, DscpError> {
        let mut decoder = DscpDecoder { bytes, cursor: 0 };
        let detail = decoder.detail(code)?;
        if decoder.cursor != bytes.len() {
            return Err(DscpError::TrailingBytes);
        }
        validate_dscp_text(&detail)?;
        if detail.code() != code {
            return Err(DscpError::CodeDetailMismatch);
        }
        if detail.canonical_detail_bytes() != bytes {
            return Err(DscpError::NonCanonical);
        }
        Ok(detail)
    }
}

fn encode_dscp_detail(encoder: &mut CanonicalEncoder, detail: &DscpV1) {
    match detail {
        DscpV1::MalformedConfiguration { file_hash } => encoder.raw(file_hash),
        DscpV1::NonLoopbackAddress { address } => encoder.str(address),
        DscpV1::DuplicateRootName { normalized_name } => encoder.str(normalized_name),
        DscpV1::InvalidPath {
            key,
            normalized_or_raw_path,
        } => {
            encoder.u8(*key as u8);
            encoder.str(normalized_or_raw_path);
        }
        DscpV1::OwnedPathOverlap { first, second } => {
            encode_symmetric_pair(encoder, first, second, encode_owned_path_side);
        }
        DscpV1::EmptyTargetApis { target } => encoder.str(target),
        DscpV1::InvalidParallelism { value } => encoder.u32(*value),
        DscpV1::InvalidBatchReservation {
            parallelism,
            reservation,
        } => {
            encoder.u32(*parallelism);
            encoder.u32(*reservation);
        }
        DscpV1::DirectoryAlias { first, second } => {
            encode_symmetric_pair(encoder, first, second, encode_directory_alias_side);
        }
        DscpV1::MissingLineageManifest => {}
        DscpV1::DuplicateLineageManifest { entries } => {
            encoder.set(entries, |encoder, entry| encoder.raw(&entry.0));
        }
        DscpV1::UnsupportedTargetIdentity {
            target,
            expected,
            observed,
        } => {
            encoder.str(target);
            encode_compilation_identity(encoder, expected);
            encode_compilation_identity(encoder, observed);
        }
        DscpV1::DuplicateTargetName { normalized_name } => encoder.str(normalized_name),
    }
}

fn encode_symmetric_pair<T>(
    encoder: &mut CanonicalEncoder,
    first: &T,
    second: &T,
    encode: fn(&mut CanonicalEncoder, &T),
) {
    let encode_one = |side: &T| {
        let mut encoded = CanonicalEncoder::new();
        encode(&mut encoded, side);
        encoded.into_bytes()
    };
    let mut sides = [encode_one(first), encode_one(second)];
    sides.sort();
    encoder.raw(&sides[0]);
    encoder.raw(&sides[1]);
}

fn encode_owned_path_side(encoder: &mut CanonicalEncoder, side: &OwnedPathSide) {
    encoder.u8(side.kind as u8);
    encoder.str(&side.path);
}

fn encode_directory_alias_side(encoder: &mut CanonicalEncoder, side: &DirectoryAliasSide) {
    encoder.str(&side.normalized_path);
    encoder.u64(side.device);
    encoder.u64(side.inode);
}

fn encode_compilation_identity(encoder: &mut CanonicalEncoder, identity: &CompilationIdentity) {
    encoder.str(&identity.target_triple);
    encoder.str(&identity.rustc);
    encoder.raw(&identity.source_fingerprint);
    encoder.set(identity.features.iter(), |encoder, (package, feature)| {
        encoder.str(package);
        encoder.str(feature);
    });
    encoder.set(identity.cfgs.iter(), |encoder, cfg| encoder.str(cfg));
    encoder.raw(&identity.manifest_lock_hash);
    encoder.u32(identity.algorithm_version);
}

struct DscpDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> DscpDecoder<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], DscpError> {
        let end = self.cursor.checked_add(count).ok_or(DscpError::Truncated)?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or(DscpError::Truncated)?;
        self.cursor = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, DscpError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, DscpError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DscpError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DscpError> {
        Ok(self.take(N)?.try_into().unwrap())
    }

    fn string(&mut self) -> Result<String, DscpError> {
        let len = usize::try_from(self.u32()?).map_err(|_| DscpError::Truncated)?;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| DscpError::InvalidUtf8)
    }

    fn count(&mut self, minimum_row_size: usize) -> Result<usize, DscpError> {
        let count = usize::try_from(self.u32()?).map_err(|_| DscpError::Truncated)?;
        if count > self.bytes.len().saturating_sub(self.cursor) / minimum_row_size.max(1) {
            return Err(DscpError::Truncated);
        }
        Ok(count)
    }

    fn owned_path_side(&mut self) -> Result<OwnedPathSide, DscpError> {
        let kind = match self.u8()? {
            1 => OwnedPathKind::AssetRoot,
            2 => OwnedPathKind::DaemonState,
            3 => OwnedPathKind::SchemaArtifact,
            4 => OwnedPathKind::PipelineModule,
            5 => OwnedPathKind::CodegenOutput,
            6 => OwnedPathKind::Quarantine,
            7 => OwnedPathKind::ImportDestination,
            unknown => return Err(DscpError::UnknownOwnedPathKind(unknown)),
        };
        Ok(OwnedPathSide {
            kind,
            path: self.string()?,
        })
    }

    fn directory_alias_side(&mut self) -> Result<DirectoryAliasSide, DscpError> {
        Ok(DirectoryAliasSide {
            normalized_path: self.string()?,
            device: self.u64()?,
            inode: self.u64()?,
        })
    }

    fn compilation_identity(&mut self) -> Result<CompilationIdentity, DscpError> {
        let target_triple = self.string()?;
        let rustc = self.string()?;
        let source_fingerprint = self.array()?;
        let mut features = BTreeSet::new();
        for _ in 0..self.count(8)? {
            if !features.insert((self.string()?, self.string()?)) {
                return Err(DscpError::NonCanonical);
            }
        }
        let mut cfgs = BTreeSet::new();
        for _ in 0..self.count(4)? {
            if !cfgs.insert(self.string()?) {
                return Err(DscpError::NonCanonical);
            }
        }
        Ok(CompilationIdentity {
            target_triple,
            rustc,
            source_fingerprint,
            features,
            cfgs,
            manifest_lock_hash: self.array()?,
            algorithm_version: self.u32()?,
        })
    }

    fn detail(&mut self, code: ConfigurationPoisonCode) -> Result<DscpV1, DscpError> {
        Ok(match code {
            ConfigurationPoisonCode::MalformedConfiguration => DscpV1::MalformedConfiguration {
                file_hash: self.array()?,
            },
            ConfigurationPoisonCode::NonLoopbackAddress => DscpV1::NonLoopbackAddress {
                address: self.string()?,
            },
            ConfigurationPoisonCode::DuplicateRootName => DscpV1::DuplicateRootName {
                normalized_name: self.string()?,
            },
            ConfigurationPoisonCode::InvalidPath => DscpV1::InvalidPath {
                key: match self.u8()? {
                    1 => ConfigurationPathKey::AssetRoot,
                    2 => ConfigurationPathKey::StatePath,
                    3 => ConfigurationPathKey::SchemaArtifact,
                    4 => ConfigurationPathKey::PipelineModule,
                    5 => ConfigurationPathKey::CodegenOutput,
                    6 => ConfigurationPathKey::Quarantine,
                    7 => ConfigurationPathKey::ImportDestination,
                    unknown => return Err(DscpError::UnknownPathKey(unknown)),
                },
                normalized_or_raw_path: self.string()?,
            },
            ConfigurationPoisonCode::OwnedPathOverlap => DscpV1::OwnedPathOverlap {
                first: self.owned_path_side()?,
                second: self.owned_path_side()?,
            },
            ConfigurationPoisonCode::EmptyTargetApis => DscpV1::EmptyTargetApis {
                target: self.string()?,
            },
            ConfigurationPoisonCode::InvalidParallelism => {
                DscpV1::InvalidParallelism { value: self.u32()? }
            }
            ConfigurationPoisonCode::InvalidBatchReservation => DscpV1::InvalidBatchReservation {
                parallelism: self.u32()?,
                reservation: self.u32()?,
            },
            ConfigurationPoisonCode::DirectoryAlias => DscpV1::DirectoryAlias {
                first: self.directory_alias_side()?,
                second: self.directory_alias_side()?,
            },
            ConfigurationPoisonCode::MissingLineageManifest => DscpV1::MissingLineageManifest,
            ConfigurationPoisonCode::DuplicateLineageManifest => {
                let count = self.count(16)?;
                DscpV1::DuplicateLineageManifest {
                    entries: (0..count)
                        .map(|_| self.array().map(AssetUuid))
                        .collect::<Result<Vec<_>, _>>()?,
                }
            }
            ConfigurationPoisonCode::UnsupportedTargetIdentity => {
                DscpV1::UnsupportedTargetIdentity {
                    target: self.string()?,
                    expected: self.compilation_identity()?,
                    observed: self.compilation_identity()?,
                }
            }
            ConfigurationPoisonCode::DuplicateTargetName => DscpV1::DuplicateTargetName {
                normalized_name: self.string()?,
            },
        })
    }
}

fn validate_dscp_text(detail: &DscpV1) -> Result<(), DscpError> {
    use unicode_normalization::UnicodeNormalization;

    let is_nfc = |value: &str| value.nfc().collect::<String>() == value;
    let mut values = Vec::new();
    match detail {
        DscpV1::MalformedConfiguration { .. }
        | DscpV1::InvalidParallelism { .. }
        | DscpV1::InvalidBatchReservation { .. }
        | DscpV1::MissingLineageManifest
        | DscpV1::DuplicateLineageManifest { .. } => {}
        DscpV1::NonLoopbackAddress { address } => values.push(address.as_str()),
        DscpV1::DuplicateRootName { normalized_name }
        | DscpV1::DuplicateTargetName { normalized_name } => {
            values.push(normalized_name.as_str());
        }
        DscpV1::InvalidPath {
            normalized_or_raw_path,
            ..
        } => values.push(normalized_or_raw_path.as_str()),
        DscpV1::OwnedPathOverlap { first, second } => {
            values.extend([first.path.as_str(), second.path.as_str()]);
        }
        DscpV1::EmptyTargetApis { target } => values.push(target.as_str()),
        DscpV1::DirectoryAlias { first, second } => values.extend([
            first.normalized_path.as_str(),
            second.normalized_path.as_str(),
        ]),
        DscpV1::UnsupportedTargetIdentity {
            target,
            expected,
            observed,
        } => {
            values.push(target.as_str());
            for identity in [expected, observed] {
                values.extend([identity.target_triple.as_str(), identity.rustc.as_str()]);
                values.extend(
                    identity
                        .features
                        .iter()
                        .flat_map(|(package, feature)| [package.as_str(), feature.as_str()]),
                );
                values.extend(identity.cfgs.iter().map(String::as_str));
            }
        }
    }
    if values.into_iter().all(is_nfc) {
        Ok(())
    } else {
        Err(DscpError::InvalidText)
    }
}

/// Stable typed failure carried by a configuration-poisoned input version.
/// `message` is presentation-only; `reason_hash` is exclusively DSCP v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationPoison {
    pub code: ConfigurationPoisonCode,
    pub reason_hash: [u8; 32],
    pub detail: Box<DscpV1>,
    pub message: String,
}

impl ConfigurationPoison {
    pub fn from_reason(reason: &DscpV1, message: impl Into<String>) -> Self {
        Self {
            code: reason.code(),
            reason_hash: reason.reason_hash(),
            detail: Box::new(reason.clone()),
            message: message.into(),
        }
    }

    pub fn validate(&self) -> Result<(), DscpError> {
        validate_dscp_text(&self.detail)?;
        if self.code != self.detail.code() || self.reason_hash != self.detail.reason_hash() {
            return Err(DscpError::CodeDetailMismatch);
        }
        Ok(())
    }

    /// Select the authoritative defect independently of discovery order.
    /// Typed facts order by `(code, canonical detail bytes)`; prose only
    /// chooses a stable representative of an otherwise duplicate fact.
    pub fn select_canonical(
        poisons: impl IntoIterator<Item = Self>,
    ) -> Result<Option<Self>, DscpError> {
        Ok(Self::canonical_set(poisons)?.into_iter().next())
    }

    /// Complete canonically ordered set retained for doctor diagnostics.
    pub fn canonical_set(poisons: impl IntoIterator<Item = Self>) -> Result<Vec<Self>, DscpError> {
        let mut keyed = poisons
            .into_iter()
            .map(|poison| {
                poison.validate()?;
                let key = (poison.code as u16, poison.detail.canonical_detail_bytes());
                Ok((key, poison))
            })
            .collect::<Result<Vec<_>, DscpError>>()?;
        keyed.sort_by(|(left_key, left), (right_key, right)| {
            left_key
                .cmp(right_key)
                .then_with(|| left.message.cmp(&right.message))
        });
        keyed.dedup_by(|(left_key, _), (right_key, _)| left_key == right_key);
        Ok(keyed.into_iter().map(|(_, poison)| poison).collect())
    }
}

impl fmt::Display for ConfigurationPoison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "configuration poison {:?} ({:02x?}): {}",
            self.code, self.reason_hash, self.message
        )
    }
}

impl std::error::Error for ConfigurationPoison {}

impl fmt::Display for PipelinePoison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pipeline poison: {}", self.message)
    }
}

impl std::error::Error for PipelinePoison {}

impl fmt::Display for PipelinePoisonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid pipeline poison: {self:?}")
    }
}

impl std::error::Error for PipelinePoisonError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum VersionPoisonCode {
    DuplicateAssetUuid = 1,
    DuplicateBundleUuid = 2,
    SameRootNormalizedPathCollision = 3,
    IncompleteSkeleton = 4,
    UnreadableGlobalBundlePath = 5,
    InvalidPhysicalPath = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum SkeletonFailureCode {
    EnvelopeMalformed = 1,
    MissingFormatVersion = 2,
    InvalidBundleUuid = 3,
    IncompleteAssetIdentity = 4,
    IncompleteTypeIdentity = 5,
    IncompleteTagIdentity = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum GlobalBundleReadFailureCode {
    PermissionDenied = 1,
    InvalidFileType = 2,
    SymlinkIdentityChanged = 3,
    IoDataLoss = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PhysicalPathFailureCode {
    InvalidUnixUtf8 = 1,
    UnpairedWindowsUtf16 = 2,
    Absolute = 3,
    EmptyComponent = 4,
    DotComponent = 5,
    ParentComponent = 6,
    ForbiddenCharacter = 7,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReadableBundleSource {
    pub root_name: String,
    pub normalized_path: String,
    pub file_hash: BundleFileHash,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AssetClaimant {
    Authored {
        source: ReadableBundleSource,
        bundle: BundleUuid,
        local_id: String,
    },
    Derived {
        parent: AssetUuid,
        output_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PlatformPathBytes {
    Unix(Vec<u8>),
    Windows(Vec<u16>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PhysicalPathClaim {
    pub raw_relative_path: PlatformPathBytes,
    pub file_hash: BundleFileHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionPoisonV1 {
    DuplicateAssetUuid {
        asset: AssetUuid,
        claimants: Vec<AssetClaimant>,
    },
    DuplicateBundleUuid {
        bundle: BundleUuid,
        sources: Vec<ReadableBundleSource>,
    },
    SameRootNormalizedPathCollision {
        root_name: String,
        normalized_path: String,
        claims: Vec<PhysicalPathClaim>,
    },
    IncompleteSkeleton {
        source: ReadableBundleSource,
        failure: SkeletonFailureCode,
    },
    UnreadableGlobalBundlePath {
        root_name: String,
        normalized_path: String,
        failure: GlobalBundleReadFailureCode,
    },
    InvalidPhysicalPath {
        root_name: String,
        raw_relative_path: PlatformPathBytes,
        failure: PhysicalPathFailureCode,
    },
}

/// §7/§13's closed version-global poison record. `identity` commits only to
/// the typed facts; `message` is presentation text and cannot affect healing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionPoison {
    pub code: VersionPoisonCode,
    pub identity: [u8; 32],
    pub detail: VersionPoisonV1,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionPoisonError {
    UnsupportedVersion(u8),
    UnknownCode(u16),
    UnknownFailureCode(u16),
    UnknownClaimantTag(u8),
    UnknownPlatformPathTag(u8),
    Truncated,
    TrailingBytes,
    InvalidUtf8,
    InvalidRootName,
    InvalidPath,
    InvalidClaimant,
    InvalidRawPath,
    NonCanonicalSources,
    InsufficientSources,
    IdentityMismatch,
}

impl fmt::Display for VersionPoisonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid version poison: {self:?}")
    }
}

impl std::error::Error for VersionPoisonError {}

impl VersionPoisonV1 {
    pub fn code(&self) -> VersionPoisonCode {
        match self {
            Self::DuplicateAssetUuid { .. } => VersionPoisonCode::DuplicateAssetUuid,
            Self::DuplicateBundleUuid { .. } => VersionPoisonCode::DuplicateBundleUuid,
            Self::SameRootNormalizedPathCollision { .. } => {
                VersionPoisonCode::SameRootNormalizedPathCollision
            }
            Self::IncompleteSkeleton { .. } => VersionPoisonCode::IncompleteSkeleton,
            Self::UnreadableGlobalBundlePath { .. } => {
                VersionPoisonCode::UnreadableGlobalBundlePath
            }
            Self::InvalidPhysicalPath { .. } => VersionPoisonCode::InvalidPhysicalPath,
        }
    }
}

impl VersionPoison {
    pub fn new(
        detail: VersionPoisonV1,
        message: impl Into<String>,
    ) -> Result<Self, VersionPoisonError> {
        validate_version_poison_detail(&detail)?;
        let code = detail.code();
        let identity = version_poison_identity(code, &detail);
        Ok(Self {
            code,
            identity,
            detail,
            message: message.into(),
        })
    }

    pub fn validate(&self) -> Result<(), VersionPoisonError> {
        if self.code != self.detail.code() {
            return Err(VersionPoisonError::IdentityMismatch);
        }
        validate_version_poison_detail(&self.detail)?;
        if self.identity != version_poison_identity(self.code, &self.detail) {
            return Err(VersionPoisonError::IdentityMismatch);
        }
        Ok(())
    }

    pub fn persisted_bytes(&self) -> Result<Vec<u8>, VersionPoisonError> {
        self.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.raw(&DSVP);
        encoder.u8(1);
        encoder.u16(self.code as u16);
        encode_version_poison_detail(&mut encoder, &self.detail);
        encoder.str(&self.message);
        Ok(encoder.into_bytes())
    }

    pub fn from_persisted_bytes(bytes: &[u8]) -> Result<Self, VersionPoisonError> {
        let mut decoder = VersionPoisonDecoder { bytes, cursor: 0 };
        if decoder.take(4)? != DSVP {
            return Err(VersionPoisonError::UnknownCode(0));
        }
        let version = decoder.u8()?;
        if version != 1 {
            return Err(VersionPoisonError::UnsupportedVersion(version));
        }
        let code_raw = decoder.u16()?;
        let code = VersionPoisonCode::try_from(code_raw)?;
        let detail = decoder.detail(code)?;
        let message = decoder.string()?;
        if decoder.cursor != bytes.len() {
            return Err(VersionPoisonError::TrailingBytes);
        }
        Self::new(detail, message)
    }

    /// Selects the one authoritative version poison independently of scan
    /// discovery order. Typed identity is ordered by `(code, canonical
    /// detail bytes)`; presentation text only resolves an otherwise identical
    /// typed record so duplicate diagnostics cannot reintroduce ordering.
    pub fn select_canonical(
        poisons: impl IntoIterator<Item = Self>,
    ) -> Result<Option<Self>, VersionPoisonError> {
        Ok(Self::canonical_set(poisons)?.into_iter().next())
    }

    /// Returns the complete doctor-diagnostic set in the same canonical
    /// order used to select publication authority. Duplicate typed details
    /// collapse even when their presentation messages differ.
    pub fn canonical_set(
        poisons: impl IntoIterator<Item = Self>,
    ) -> Result<Vec<Self>, VersionPoisonError> {
        let mut keyed = poisons
            .into_iter()
            .map(|poison| {
                poison.validate()?;
                let mut detail = CanonicalEncoder::new();
                encode_version_poison_detail(&mut detail, &poison.detail);
                Ok(((poison.code as u16, detail.into_bytes()), poison))
            })
            .collect::<Result<Vec<_>, VersionPoisonError>>()?;
        keyed.sort_by(|(left_key, left), (right_key, right)| {
            left_key
                .cmp(right_key)
                .then_with(|| left.message.cmp(&right.message))
        });
        keyed.dedup_by(|(left_key, _), (right_key, _)| left_key == right_key);
        Ok(keyed.into_iter().map(|(_, poison)| poison).collect())
    }
}

impl TryFrom<u16> for VersionPoisonCode {
    type Error = VersionPoisonError;
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::DuplicateAssetUuid),
            2 => Ok(Self::DuplicateBundleUuid),
            3 => Ok(Self::SameRootNormalizedPathCollision),
            4 => Ok(Self::IncompleteSkeleton),
            5 => Ok(Self::UnreadableGlobalBundlePath),
            6 => Ok(Self::InvalidPhysicalPath),
            other => Err(VersionPoisonError::UnknownCode(other)),
        }
    }
}

struct VersionPoisonDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> VersionPoisonDecoder<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], VersionPoisonError> {
        let end = self
            .cursor
            .checked_add(count)
            .ok_or(VersionPoisonError::Truncated)?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or(VersionPoisonError::Truncated)?;
        self.cursor = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, VersionPoisonError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, VersionPoisonError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, VersionPoisonError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], VersionPoisonError> {
        Ok(self.take(N)?.try_into().unwrap())
    }

    fn string(&mut self) -> Result<String, VersionPoisonError> {
        let len = usize::try_from(self.u32()?).map_err(|_| VersionPoisonError::Truncated)?;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| VersionPoisonError::InvalidUtf8)
    }

    fn source(&mut self) -> Result<ReadableBundleSource, VersionPoisonError> {
        Ok(ReadableBundleSource {
            root_name: self.string()?,
            normalized_path: self.string()?,
            file_hash: BundleFileHash(self.array()?),
        })
    }

    fn sources(&mut self) -> Result<Vec<ReadableBundleSource>, VersionPoisonError> {
        let count = usize::try_from(self.u32()?).map_err(|_| VersionPoisonError::Truncated)?;
        if count > self.bytes.len().saturating_sub(self.cursor) / 40 {
            return Err(VersionPoisonError::Truncated);
        }
        (0..count).map(|_| self.source()).collect()
    }

    fn claimants(&mut self) -> Result<Vec<AssetClaimant>, VersionPoisonError> {
        let count = usize::try_from(self.u32()?).map_err(|_| VersionPoisonError::Truncated)?;
        if count > self.bytes.len().saturating_sub(self.cursor) / 2 {
            return Err(VersionPoisonError::Truncated);
        }
        (0..count)
            .map(|_| match self.u8()? {
                1 => Ok(AssetClaimant::Authored {
                    source: self.source()?,
                    bundle: BundleUuid(self.array()?),
                    local_id: self.string()?,
                }),
                2 => Ok(AssetClaimant::Derived {
                    parent: AssetUuid(self.array()?),
                    output_key: self.string()?,
                }),
                other => Err(VersionPoisonError::UnknownClaimantTag(other)),
            })
            .collect()
    }

    fn path_claims(&mut self) -> Result<Vec<PhysicalPathClaim>, VersionPoisonError> {
        let count = usize::try_from(self.u32()?).map_err(|_| VersionPoisonError::Truncated)?;
        if count > self.bytes.len().saturating_sub(self.cursor) / 6 {
            return Err(VersionPoisonError::Truncated);
        }
        (0..count)
            .map(|_| {
                Ok(PhysicalPathClaim {
                    raw_relative_path: self.platform_path()?,
                    file_hash: BundleFileHash(self.array()?),
                })
            })
            .collect()
    }

    fn platform_path(&mut self) -> Result<PlatformPathBytes, VersionPoisonError> {
        match self.u8()? {
            1 => {
                let len =
                    usize::try_from(self.u32()?).map_err(|_| VersionPoisonError::Truncated)?;
                Ok(PlatformPathBytes::Unix(self.take(len)?.to_vec()))
            }
            2 => {
                let count =
                    usize::try_from(self.u32()?).map_err(|_| VersionPoisonError::Truncated)?;
                if count > self.bytes.len().saturating_sub(self.cursor) / 2 {
                    return Err(VersionPoisonError::Truncated);
                }
                let units = (0..count)
                    .map(|_| self.u16())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlatformPathBytes::Windows(units))
            }
            other => Err(VersionPoisonError::UnknownPlatformPathTag(other)),
        }
    }

    fn detail(&mut self, code: VersionPoisonCode) -> Result<VersionPoisonV1, VersionPoisonError> {
        Ok(match code {
            VersionPoisonCode::DuplicateAssetUuid => VersionPoisonV1::DuplicateAssetUuid {
                asset: AssetUuid(self.array()?),
                claimants: self.claimants()?,
            },
            VersionPoisonCode::DuplicateBundleUuid => VersionPoisonV1::DuplicateBundleUuid {
                bundle: BundleUuid(self.array()?),
                sources: self.sources()?,
            },
            VersionPoisonCode::SameRootNormalizedPathCollision => {
                VersionPoisonV1::SameRootNormalizedPathCollision {
                    root_name: self.string()?,
                    normalized_path: self.string()?,
                    claims: self.path_claims()?,
                }
            }
            VersionPoisonCode::IncompleteSkeleton => VersionPoisonV1::IncompleteSkeleton {
                source: self.source()?,
                failure: match self.u16()? {
                    1 => SkeletonFailureCode::EnvelopeMalformed,
                    2 => SkeletonFailureCode::MissingFormatVersion,
                    3 => SkeletonFailureCode::InvalidBundleUuid,
                    4 => SkeletonFailureCode::IncompleteAssetIdentity,
                    5 => SkeletonFailureCode::IncompleteTypeIdentity,
                    6 => SkeletonFailureCode::IncompleteTagIdentity,
                    other => return Err(VersionPoisonError::UnknownFailureCode(other)),
                },
            },
            VersionPoisonCode::UnreadableGlobalBundlePath => {
                VersionPoisonV1::UnreadableGlobalBundlePath {
                    root_name: self.string()?,
                    normalized_path: self.string()?,
                    failure: match self.u16()? {
                        1 => GlobalBundleReadFailureCode::PermissionDenied,
                        2 => GlobalBundleReadFailureCode::InvalidFileType,
                        3 => GlobalBundleReadFailureCode::SymlinkIdentityChanged,
                        4 => GlobalBundleReadFailureCode::IoDataLoss,
                        other => return Err(VersionPoisonError::UnknownFailureCode(other)),
                    },
                }
            }
            VersionPoisonCode::InvalidPhysicalPath => VersionPoisonV1::InvalidPhysicalPath {
                root_name: self.string()?,
                raw_relative_path: self.platform_path()?,
                failure: match self.u16()? {
                    1 => PhysicalPathFailureCode::InvalidUnixUtf8,
                    2 => PhysicalPathFailureCode::UnpairedWindowsUtf16,
                    3 => PhysicalPathFailureCode::Absolute,
                    4 => PhysicalPathFailureCode::EmptyComponent,
                    5 => PhysicalPathFailureCode::DotComponent,
                    6 => PhysicalPathFailureCode::ParentComponent,
                    7 => PhysicalPathFailureCode::ForbiddenCharacter,
                    other => return Err(VersionPoisonError::UnknownFailureCode(other)),
                },
            },
        })
    }
}

fn version_poison_identity(code: VersionPoisonCode, detail: &VersionPoisonV1) -> [u8; 32] {
    let mut encoder = CanonicalEncoder::new();
    encoder.raw(&DSVP);
    encoder.u8(1);
    encoder.u16(code as u16);
    encode_version_poison_detail(&mut encoder, detail);
    *blake3::hash(&encoder.into_bytes()).as_bytes()
}

fn encode_version_poison_detail(encoder: &mut CanonicalEncoder, detail: &VersionPoisonV1) {
    match detail {
        VersionPoisonV1::DuplicateAssetUuid { asset, claimants } => {
            encoder.raw(&asset.0);
            encoder.seq(claimants, encode_asset_claimant);
        }
        VersionPoisonV1::DuplicateBundleUuid { bundle, sources } => {
            encoder.raw(&bundle.0);
            encode_bundle_sources(encoder, sources);
        }
        VersionPoisonV1::SameRootNormalizedPathCollision {
            root_name,
            normalized_path,
            claims,
        } => {
            encoder.str(root_name);
            encoder.str(normalized_path);
            encoder.seq(claims, encode_physical_path_claim);
        }
        VersionPoisonV1::IncompleteSkeleton { source, failure } => {
            encode_bundle_source(encoder, source);
            encoder.u16(*failure as u16);
        }
        VersionPoisonV1::UnreadableGlobalBundlePath {
            root_name,
            normalized_path,
            failure,
        } => {
            encoder.str(root_name);
            encoder.str(normalized_path);
            encoder.u16(*failure as u16);
        }
        VersionPoisonV1::InvalidPhysicalPath {
            root_name,
            raw_relative_path,
            failure,
        } => {
            encoder.str(root_name);
            encode_platform_path(encoder, raw_relative_path);
            encoder.u16(*failure as u16);
        }
    }
}

fn encode_asset_claimant(encoder: &mut CanonicalEncoder, claimant: &AssetClaimant) {
    match claimant {
        AssetClaimant::Authored {
            source,
            bundle,
            local_id,
        } => {
            encoder.u8(1);
            encode_bundle_source(encoder, source);
            encoder.raw(&bundle.0);
            encoder.str(local_id);
        }
        AssetClaimant::Derived { parent, output_key } => {
            encoder.u8(2);
            encoder.raw(&parent.0);
            encoder.str(output_key);
        }
    }
}

fn encode_physical_path_claim(encoder: &mut CanonicalEncoder, claim: &PhysicalPathClaim) {
    encode_platform_path(encoder, &claim.raw_relative_path);
    encoder.raw(&claim.file_hash.0);
}

fn encode_platform_path(encoder: &mut CanonicalEncoder, path: &PlatformPathBytes) {
    match path {
        PlatformPathBytes::Unix(bytes) => {
            encoder.u8(1);
            encoder.u32(u32::try_from(bytes.len()).expect("raw Unix path exceeds u32 length"));
            encoder.raw(bytes);
        }
        PlatformPathBytes::Windows(units) => {
            encoder.u8(2);
            encoder.u32(u32::try_from(units.len()).expect("raw Windows path exceeds u32 length"));
            for unit in units {
                encoder.u16(*unit);
            }
        }
    }
}

fn encode_bundle_sources(encoder: &mut CanonicalEncoder, sources: &[ReadableBundleSource]) {
    encoder.seq(sources, encode_bundle_source);
}

fn encode_bundle_source(encoder: &mut CanonicalEncoder, source: &ReadableBundleSource) {
    encoder.str(&source.root_name);
    encoder.str(&source.normalized_path);
    encoder.raw(&source.file_hash.0);
}

fn validate_version_poison_detail(detail: &VersionPoisonV1) -> Result<(), VersionPoisonError> {
    let validate_source = |source: &ReadableBundleSource| {
        validate_root_and_path(&source.root_name, &source.normalized_path)
    };
    let validate_collision_sources = |sources: &[ReadableBundleSource]| {
        if sources.len() < 2 {
            return Err(VersionPoisonError::InsufficientSources);
        }
        if sources.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(VersionPoisonError::NonCanonicalSources);
        }
        sources.iter().try_for_each(validate_source)
    };

    match detail {
        VersionPoisonV1::DuplicateAssetUuid { claimants, .. } => {
            validate_strict_two(claimants)?;
            for claimant in claimants {
                match claimant {
                    AssetClaimant::Authored {
                        source, local_id, ..
                    } => {
                        validate_source(source)?;
                        validate_identifier(local_id)?;
                    }
                    AssetClaimant::Derived { output_key, .. } => {
                        validate_identifier(output_key)?;
                    }
                }
            }
        }
        VersionPoisonV1::DuplicateBundleUuid { sources, .. } => {
            validate_collision_sources(sources)?;
        }
        VersionPoisonV1::SameRootNormalizedPathCollision {
            root_name,
            normalized_path,
            claims,
        } => {
            validate_root_and_path(root_name, normalized_path)?;
            validate_strict_two(claims)?;
            claims.iter().try_for_each(validate_physical_path_claim)?;
        }
        VersionPoisonV1::IncompleteSkeleton { source, .. } => validate_source(source)?,
        VersionPoisonV1::UnreadableGlobalBundlePath {
            root_name,
            normalized_path,
            ..
        } => {
            validate_root_and_path(root_name, normalized_path)?;
        }
        VersionPoisonV1::InvalidPhysicalPath {
            root_name,
            raw_relative_path,
            failure,
        } => {
            validate_root_name(root_name)?;
            if classify_invalid_physical_path(raw_relative_path) != Some(*failure) {
                return Err(VersionPoisonError::InvalidRawPath);
            }
        }
    }
    Ok(())
}

fn classify_invalid_physical_path(path: &PlatformPathBytes) -> Option<PhysicalPathFailureCode> {
    match path {
        PlatformPathBytes::Unix(bytes) => {
            if std::str::from_utf8(bytes).is_err() {
                return Some(PhysicalPathFailureCode::InvalidUnixUtf8);
            }
            if bytes.starts_with(b"/") {
                return Some(PhysicalPathFailureCode::Absolute);
            }
            if bytes.is_empty()
                || bytes.ends_with(b"/")
                || bytes.windows(2).any(|pair| pair == b"//")
            {
                return Some(PhysicalPathFailureCode::EmptyComponent);
            }
            if bytes.split(|byte| *byte == b'/').any(|part| part == b".") {
                return Some(PhysicalPathFailureCode::DotComponent);
            }
            if bytes.split(|byte| *byte == b'/').any(|part| part == b"..") {
                return Some(PhysicalPathFailureCode::ParentComponent);
            }
            if bytes.iter().any(|byte| matches!(*byte, 0 | b'\\')) {
                return Some(PhysicalPathFailureCode::ForbiddenCharacter);
            }
            None
        }
        PlatformPathBytes::Windows(units) => {
            if String::from_utf16(units).is_err() {
                return Some(PhysicalPathFailureCode::UnpairedWindowsUtf16);
            }
            let separator = |unit: u16| unit == u16::from(b'/') || unit == u16::from(b'\\');
            let ascii_letter = |unit: u16| {
                (u16::from(b'a')..=u16::from(b'z')).contains(&unit)
                    || (u16::from(b'A')..=u16::from(b'Z')).contains(&unit)
            };
            let drive_prefix =
                units.len() >= 2 && ascii_letter(units[0]) && units[1] == u16::from(b':');
            if units.first().is_some_and(|unit| separator(*unit)) || drive_prefix {
                return Some(PhysicalPathFailureCode::Absolute);
            }
            if units.is_empty()
                || units.last().is_some_and(|unit| separator(*unit))
                || units
                    .windows(2)
                    .any(|pair| separator(pair[0]) && separator(pair[1]))
            {
                return Some(PhysicalPathFailureCode::EmptyComponent);
            }
            if units
                .split(|unit| separator(*unit))
                .any(|part| part == [b'.' as u16])
            {
                return Some(PhysicalPathFailureCode::DotComponent);
            }
            if units
                .split(|unit| separator(*unit))
                .any(|part| part == [b'.' as u16, b'.' as u16])
            {
                return Some(PhysicalPathFailureCode::ParentComponent);
            }
            if units.iter().any(|unit| {
                *unit <= 0x1f || matches!(*unit, 0x22 | 0x2a | 0x3a | 0x3c | 0x3e | 0x3f | 0x7c)
            }) {
                return Some(PhysicalPathFailureCode::ForbiddenCharacter);
            }
            None
        }
    }
}

fn validate_strict_two<T: Ord>(values: &[T]) -> Result<(), VersionPoisonError> {
    if values.len() < 2 {
        return Err(VersionPoisonError::InsufficientSources);
    }
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(VersionPoisonError::NonCanonicalSources);
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), VersionPoisonError> {
    use unicode_normalization::UnicodeNormalization;
    if value.is_empty()
        || value.nfc().collect::<String>() != value
        || value.contains(['/', '\\', '\0'])
    {
        return Err(VersionPoisonError::InvalidClaimant);
    }
    Ok(())
}

fn validate_physical_path_claim(claim: &PhysicalPathClaim) -> Result<(), VersionPoisonError> {
    let valid = match &claim.raw_relative_path {
        PlatformPathBytes::Unix(bytes) => {
            !bytes.is_empty()
                && !bytes.contains(&0)
                && !bytes.starts_with(b"/")
                && !bytes
                    .split(|byte| *byte == b'/')
                    .any(|part| part.is_empty() || part == b"." || part == b"..")
        }
        PlatformPathBytes::Windows(units) => {
            !units.is_empty()
                && !units.contains(&0)
                && !matches!(units.first(), Some(47 | 92))
                && !units
                    .split(|unit| matches!(*unit, 47 | 92))
                    .any(|part| part.is_empty() || part == [46_u16] || part == [46_u16, 46_u16])
        }
    };
    if valid {
        Ok(())
    } else {
        Err(VersionPoisonError::InvalidRawPath)
    }
}

fn validate_root_and_path(
    root_name: &str,
    normalized_path: &str,
) -> Result<(), VersionPoisonError> {
    validate_root_name(root_name)?;
    validate_normalized_path(normalized_path)
}

fn validate_root_name(root_name: &str) -> Result<(), VersionPoisonError> {
    use unicode_normalization::UnicodeNormalization;

    if root_name.is_empty()
        || root_name.nfc().collect::<String>() != root_name
        || root_name.contains(['/', '\\', '\0'])
    {
        return Err(VersionPoisonError::InvalidRootName);
    }
    Ok(())
}

fn validate_normalized_path(normalized_path: &str) -> Result<(), VersionPoisonError> {
    use unicode_normalization::UnicodeNormalization;

    if normalized_path.is_empty()
        || normalized_path.nfc().collect::<String>() != normalized_path
        || normalized_path.contains(['\\', '\0'])
        || normalized_path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(VersionPoisonError::InvalidPath);
    }
    Ok(())
}

impl fmt::Display for VersionPoison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "version poison: {}", self.message)
    }
}

impl std::error::Error for VersionPoison {}

/// What a snapshot carries (§13): the validated epoch, or the named
/// failure that kept a candidate from becoming one. `last_good` is
/// residency bookkeeping only — the prior epoch stays loaded for the
/// snapshots that pin it, but is never served as this version's code
/// (§3, §13): obsolete processor code must not cook new bytes.
#[derive(Debug, Clone)]
pub enum PipelineState {
    Ready(Arc<PipelineEpoch>),
    SchemaAcceptanceRequired {
        required: SchemaAcceptanceRequired,
        last_good: Option<Arc<PipelineEpoch>>,
    },
    Poisoned {
        error: PipelinePoison,
        last_good: Option<Arc<PipelineEpoch>>,
    },
}

/// Typed reason a snapshot has no usable pipeline epoch. Schema acceptance
/// is deliberately not collapsed into generic pipeline poison: authoring can
/// inspect its manifest/candidate basis and issue the explicit bound command.
#[derive(Debug, Clone, Copy)]
pub enum PipelineUnavailable<'a> {
    Poisoned(&'a PipelinePoison),
    SchemaAcceptanceRequired(&'a SchemaAcceptanceRequired),
}

impl fmt::Display for PipelineUnavailable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineUnavailable::Poisoned(error) => error.fmt(f),
            PipelineUnavailable::SchemaAcceptanceRequired(required) => write!(
                f,
                "schema acceptance required for candidate dylib {:02x?} against source manifest {} ({} registry mismatch(es))",
                required.candidate.dylib_hash,
                required.manifest.manifest_hash,
                required.mismatches.len()
            ),
        }
    }
}

impl PipelineState {
    /// The pipeline state current at a snapshot's version. Fallible,
    /// because a pipeline-poisoned version has no `PipelineEpoch` to
    /// return (§3: a failed candidate never becomes one, and the prior
    /// epoch may not stand in).
    pub fn epoch(&self) -> Result<&Arc<PipelineEpoch>, PipelineUnavailable<'_>> {
        match self {
            PipelineState::Ready(epoch) => Ok(epoch),
            PipelineState::SchemaAcceptanceRequired { required, .. } => {
                Err(PipelineUnavailable::SchemaAcceptanceRequired(required))
            }
            PipelineState::Poisoned { error, .. } => Err(PipelineUnavailable::Poisoned(error)),
        }
    }

    /// The §13 operation classification: pure-metadata reads remain valid
    /// under poison (`Ok(None)` — no epoch consumed); pipeline-dependent
    /// operations receive the epoch when ready (`Ok(Some(_))`) and fail
    /// deterministically with its typed poison or schema-acceptance reason
    /// otherwise.
    pub fn check(
        &self,
        op: OperationKind,
    ) -> Result<Option<&Arc<PipelineEpoch>>, PipelineUnavailable<'_>> {
        if !op.requires_epoch() {
            return Ok(None);
        }
        self.epoch().map(Some)
    }
}

/// The configuration state pinned by a snapshot. A rejected candidate
/// is representable independently from pipeline poison: the prior valid
/// configuration is residency/bookkeeping only and is never served as
/// the poisoned version's active values.
#[derive(Debug, Clone)]
pub enum ConfigurationState {
    Ready(Arc<ConfigurationEpoch>),
    Poisoned {
        reason: ConfigurationPoison,
        last_good: Option<Arc<ConfigurationEpoch>>,
    },
}

impl ConfigurationState {
    pub fn epoch(&self) -> Result<&Arc<ConfigurationEpoch>, &ConfigurationPoison> {
        match self {
            ConfigurationState::Ready(epoch) => Ok(epoch),
            ConfigurationState::Poisoned { reason, .. } => Err(reason),
        }
    }

    pub fn check(
        &self,
        op: OperationKind,
    ) -> Result<Option<&Arc<ConfigurationEpoch>>, &ConfigurationPoison> {
        if !op.requires_configuration() {
            return Ok(None);
        }
        self.epoch().map(Some)
    }
}

/// The operations §13's consistency contract classifies against a
/// poisoned version. Pure-metadata reads never consult the epoch;
/// everything needing the pipeline map, registry, defaults, or migration
/// fns does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    /// Creating or reading a metadata snapshot is valid under config
    /// poison; its state fields carry the poison explicitly.
    SnapshotRead,
    /// Path-index resolution — pure metadata.
    PathIndex,
    /// Reading input versions / snapshot stamps — pure metadata.
    InputVersionRead,
    /// CAS reads by ContentHash/LayoutHash — pure metadata.
    CasRead,
    /// Pinning hashes to a lease — pure metadata.
    LeasePin,
    /// `load_current` — needs migration fns and the registry.
    LoadCurrent,
    /// Terminal-type queries — need the pipeline map.
    TerminalTypeQuery,
    /// The derived-output namespace — derived from assets × pinned
    /// pipeline map (§9).
    DerivedOutputNamespace,
    /// Builds — need everything above.
    Build,
    /// Any authoring write depends on validated roots/output paths and
    /// is refused while configuration is poisoned.
    Authoring,
    /// Target-bound RPC methods require a validated target definition
    /// and expose configuration poison as a stable typed result.
    TargetBoundRpc,
}

impl OperationKind {
    /// Whether the operation needs the pipeline epoch — the exact §13
    /// split between "remains valid under poison" and "fails
    /// deterministically".
    pub fn requires_epoch(self) -> bool {
        match self {
            OperationKind::PathIndex
            | OperationKind::SnapshotRead
            | OperationKind::InputVersionRead
            | OperationKind::CasRead
            | OperationKind::LeasePin
            | OperationKind::TargetBoundRpc => false,
            OperationKind::LoadCurrent
            | OperationKind::TerminalTypeQuery
            | OperationKind::DerivedOutputNamespace
            | OperationKind::Build
            | OperationKind::Authoring => true,
        }
    }

    /// Whether the operation requires a validated configuration epoch.
    /// Pure metadata and target-independent schema loading remain valid;
    /// authoring, target/pipeline-map surfaces, and builds do not.
    pub fn requires_configuration(self) -> bool {
        match self {
            OperationKind::SnapshotRead
            | OperationKind::PathIndex
            | OperationKind::InputVersionRead
            | OperationKind::CasRead
            | OperationKind::LeasePin
            | OperationKind::LoadCurrent => false,
            OperationKind::TerminalTypeQuery
            | OperationKind::DerivedOutputNamespace
            | OperationKind::Build
            | OperationKind::Authoring
            | OperationKind::TargetBoundRpc => true,
        }
    }
}

/// The **load-policy digest** (§9, §13): `blake3("DSLP" ‖ version:u8 ‖
/// count:u32 ‖ (type_uuid:16 ‖ build_only:u8)*)` over the sorted
/// `(type_uuid, build_only)` pairs of the current registry — §5's
/// canonical set encoding (fixed 16-byte uuid ‖ bool byte, sorted by
/// encoded bytes — uuid order — and deduplicated) under the `"DSLP"`
/// domain from §5's table (packs carry the same grammar projected onto
/// their closure, §16). `build_only` is deliberately unhashed in the
/// logical schema (§5 — toggling policy must not mint migrations), so
/// this digest is its change tracking.
pub fn load_policy_digest(pairs: &[(TypeUuid, bool)]) -> [u8; 32] {
    distill_core::canonical::domain_digest(distill_core::canonical::DSLP, 1, |e| {
        e.set(pairs.iter(), |e, (uuid, build_only)| {
            e.raw(&uuid.0);
            e.bool(*build_only);
        });
    })
}
