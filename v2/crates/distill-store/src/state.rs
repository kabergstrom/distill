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
//! input wherever pipeline code runs, and the only part stored), the exact
//! logical schema map and canonical target rows the store validates before
//! publishing it (§13). That is what [`PipelineEpoch`] here carries; residency is still expressed the spec's way (`Arc`), so pin
//! counting composes when the module host wraps it.

use std::collections::BTreeMap;
use std::fmt;

use distill_core::canonical::{domain_digest, CanonicalEncoder, DSCP, DSPP, DSVP};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, LogicalHash, TypeUuid};
use distill_core::target_set::CanonicalTargetSet;
use ngp_schema::identity::LayoutIdentity;

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

/// What a pipeline candidate publishes (§13): the projections the store
/// validates before publishing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineEpoch {
    /// Complete canonical target-definition set used to construct the
    /// candidate pipeline map. The store validates and compares these exact
    /// canonical rows before publishing.
    pub target_set: CanonicalTargetSet,
    /// The candidate's complete compiled registry projection: the current
    /// logical schema of every registered type.
    pub schema_registry: BTreeMap<TypeUuid, LogicalHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PipelineFailureCode {
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
pub enum PipelineFailureOrigin {
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
pub struct PipelineFailure {
    pub code: PipelineFailureCode,
    pub origin: PipelineFailureOrigin,
    pub cleanup: CleanupDisposition,
    pub identity: [u8; 32],
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineFailureDecodeError {
    UnknownCode(u16),
    UnknownOrigin(u16),
    UnknownCleanup(u16),
    InvalidMatrix,
    IdentityMismatch,
}

impl PipelineFailure {
    pub fn new(
        code: PipelineFailureCode,
        origin: PipelineFailureOrigin,
        cleanup: CleanupDisposition,
        message: impl Into<String>,
    ) -> Result<Self, PipelineFailureDecodeError> {
        validate_pipeline_failure_matrix(code, origin, cleanup)?;
        Ok(Self {
            code,
            origin,
            cleanup,
            identity: pipeline_failure_identity(code, origin, cleanup),
            message: message.into(),
        })
    }

    pub fn from_wire(
        code: u16,
        origin: u16,
        cleanup: u16,
        identity: [u8; 32],
        message: impl Into<String>,
    ) -> Result<Self, PipelineFailureDecodeError> {
        let value = Self::new(
            PipelineFailureCode::try_from(code)?,
            PipelineFailureOrigin::try_from(origin)?,
            CleanupDisposition::try_from(cleanup)?,
            message,
        )?;
        if value.identity != identity {
            return Err(PipelineFailureDecodeError::IdentityMismatch);
        }
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), PipelineFailureDecodeError> {
        validate_pipeline_failure_matrix(self.code, self.origin, self.cleanup)?;
        if self.identity != pipeline_failure_identity(self.code, self.origin, self.cleanup) {
            return Err(PipelineFailureDecodeError::IdentityMismatch);
        }
        Ok(())
    }
}

fn pipeline_failure_identity(
    code: PipelineFailureCode,
    origin: PipelineFailureOrigin,
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

fn validate_pipeline_failure_matrix(
    code: PipelineFailureCode,
    origin: PipelineFailureOrigin,
    cleanup: CleanupDisposition,
) -> Result<(), PipelineFailureDecodeError> {
    let valid = match origin {
        PipelineFailureOrigin::CandidateOpen => match code {
            PipelineFailureCode::CandidateOpen
            | PipelineFailureCode::CandidateAttestation
            | PipelineFailureCode::CandidateRegistration
            | PipelineFailureCode::CandidateValidation => matches!(
                cleanup,
                CleanupDisposition::None | CleanupDisposition::CleanedAndClosed
            ),
            PipelineFailureCode::CandidateCleanup => matches!(
                cleanup,
                CleanupDisposition::RegistrationCleanupFailed
                    | CleanupDisposition::ModuleUnloadFailed
                    | CleanupDisposition::TokenPoisoned
                    | CleanupDisposition::TokenPinned
                    | CleanupDisposition::DlcloseFailed
            ),
            PipelineFailureCode::PublishedCallbackPanic
            | PipelineFailureCode::PublishedCallbackRejected
            | PipelineFailureCode::PublishedCleanup => false,
        },
        PipelineFailureOrigin::PublishedRuntime => {
            matches!(
                code,
                PipelineFailureCode::PublishedCallbackPanic
                    | PipelineFailureCode::PublishedCallbackRejected
                    | PipelineFailureCode::PublishedCleanup
            ) && cleanup == CleanupDisposition::PublishedEpochLeaked
        }
    };
    valid
        .then_some(())
        .ok_or(PipelineFailureDecodeError::InvalidMatrix)
}

macro_rules! pipeline_failure_try_from {
    ($type:ty, $error:ident, {$($value:literal => $variant:ident),+ $(,)?}) => {
        impl TryFrom<u16> for $type {
            type Error = PipelineFailureDecodeError;
            fn try_from(value: u16) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    other => Err(PipelineFailureDecodeError::$error(other)),
                }
            }
        }
    };
}

pipeline_failure_try_from!(PipelineFailureCode, UnknownCode, {
    1 => CandidateOpen, 2 => CandidateAttestation, 3 => CandidateRegistration,
    4 => CandidateValidation, 5 => CandidateCleanup, 6 => PublishedCallbackPanic,
    7 => PublishedCallbackRejected, 8 => PublishedCleanup,
});
pipeline_failure_try_from!(PipelineFailureOrigin, UnknownOrigin, {
    1 => CandidateOpen, 2 => PublishedRuntime,
});
pipeline_failure_try_from!(CleanupDisposition, UnknownCleanup, {
    0 => None, 1 => CleanedAndClosed, 2 => RegistrationCleanupFailed,
    3 => ModuleUnloadFailed, 4 => TokenPoisoned, 5 => TokenPinned,
    6 => DlcloseFailed, 7 => PublishedEpochLeaked,
});

/// Closed DSCP v1 discriminants. Persisted/wire values outside this set
/// reject; there is deliberately no extensible `Other` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ConfigurationErrorCode {
    MalformedConfiguration = 1,
    NonLoopbackAddress = 2,
    DuplicateRootName = 3,
    InvalidPath = 4,
    OwnedPathOverlap = 5,
    EmptyTargetApis = 6,
    InvalidParallelism = 7,
    InvalidBatchReservation = 8,
    DirectoryAlias = 9,
    UnsupportedTargetIdentity = 12,
    DuplicateTargetName = 13,
    ConfigurationSourceUnavailable = 14,
}

impl TryFrom<u16> for ConfigurationErrorCode {
    type Error = UnknownConfigurationErrorCode;

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
            12 => Ok(Self::UnsupportedTargetIdentity),
            13 => Ok(Self::DuplicateTargetName),
            14 => Ok(Self::ConfigurationSourceUnavailable),
            unknown => Err(UnknownConfigurationErrorCode(unknown)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownConfigurationErrorCode(pub u16);

impl fmt::Display for UnknownConfigurationErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown ConfigurationErrorCode {}", self.0)
    }
}

impl std::error::Error for UnknownConfigurationErrorCode {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DscpError {
    UnsupportedVersion(u8),
    CodeDetailMismatch,
    UnknownPathKey(u8),
    UnknownOwnedPathKind(u8),
    UnknownConfigurationSourcePath(u8),
    UnknownConfigurationSourceFailure(u16),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationSourcePath {
    Unix(Vec<u8>),
    Windows(Vec<u16>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ConfigurationSourceFailureCode {
    Missing = 1,
    PermissionDenied = 2,
    InvalidFileType = 3,
    IoDataLoss = 4,
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
}

/// Exact, closed typed facts hashed by DSCP v1. Presentation prose never
/// enters this value; callers supply it separately when publishing an error.
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
    UnsupportedTargetIdentity {
        target: String,
        expected: LayoutIdentity,
        observed: LayoutIdentity,
    },
    DuplicateTargetName {
        normalized_name: String,
    },
    ConfigurationSourceUnavailable {
        path: ConfigurationSourcePath,
        failure: ConfigurationSourceFailureCode,
    },
}

impl DscpV1 {
    pub fn code(&self) -> ConfigurationErrorCode {
        match self {
            Self::MalformedConfiguration { .. } => ConfigurationErrorCode::MalformedConfiguration,
            Self::NonLoopbackAddress { .. } => ConfigurationErrorCode::NonLoopbackAddress,
            Self::DuplicateRootName { .. } => ConfigurationErrorCode::DuplicateRootName,
            Self::InvalidPath { .. } => ConfigurationErrorCode::InvalidPath,
            Self::OwnedPathOverlap { .. } => ConfigurationErrorCode::OwnedPathOverlap,
            Self::EmptyTargetApis { .. } => ConfigurationErrorCode::EmptyTargetApis,
            Self::InvalidParallelism { .. } => ConfigurationErrorCode::InvalidParallelism,
            Self::InvalidBatchReservation { .. } => ConfigurationErrorCode::InvalidBatchReservation,
            Self::DirectoryAlias { .. } => ConfigurationErrorCode::DirectoryAlias,
            Self::UnsupportedTargetIdentity { .. } => {
                ConfigurationErrorCode::UnsupportedTargetIdentity
            }
            Self::DuplicateTargetName { .. } => ConfigurationErrorCode::DuplicateTargetName,
            Self::ConfigurationSourceUnavailable { .. } => {
                ConfigurationErrorCode::ConfigurationSourceUnavailable
            }
        }
    }

    /// `blake3("DSCP" || 0x01 || code:u16 || exact variant fields)`.
    /// Symmetric records are canonicalized here,
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
        code: ConfigurationErrorCode,
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
        DscpV1::UnsupportedTargetIdentity {
            target,
            expected,
            observed,
        } => {
            encoder.str(target);
            encode_layout_identity(encoder, expected);
            encode_layout_identity(encoder, observed);
        }
        DscpV1::DuplicateTargetName { normalized_name } => encoder.str(normalized_name),
        DscpV1::ConfigurationSourceUnavailable { path, failure } => {
            encode_configuration_source_path(encoder, path);
            encoder.u16(*failure as u16);
        }
    }
}

fn encode_configuration_source_path(
    encoder: &mut CanonicalEncoder,
    path: &ConfigurationSourcePath,
) {
    match path {
        ConfigurationSourcePath::Unix(bytes) => {
            encoder.u8(1);
            encoder.u32(u32::try_from(bytes.len()).expect("configuration path exceeds u32"));
            encoder.raw(bytes);
        }
        ConfigurationSourcePath::Windows(units) => {
            encoder.u8(2);
            encoder.u32(u32::try_from(units.len()).expect("configuration path exceeds u32"));
            for unit in units {
                encoder.u16(*unit);
            }
        }
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
}

fn encode_layout_identity(encoder: &mut CanonicalEncoder, identity: &LayoutIdentity) {
    encoder.str(&identity.target_triple);
    encoder.str(&identity.rustc);
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

    fn u16(&mut self) -> Result<u16, DscpError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
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
        })
    }

    fn configuration_source_path(&mut self) -> Result<ConfigurationSourcePath, DscpError> {
        match self.u8()? {
            1 => {
                let len = self.count(1)?;
                Ok(ConfigurationSourcePath::Unix(self.take(len)?.to_vec()))
            }
            2 => {
                let count = self.count(2)?;
                Ok(ConfigurationSourcePath::Windows(
                    (0..count)
                        .map(|_| self.u16())
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            unknown => Err(DscpError::UnknownConfigurationSourcePath(unknown)),
        }
    }

    fn layout_identity(&mut self) -> Result<LayoutIdentity, DscpError> {
        Ok(LayoutIdentity {
            target_triple: self.string()?,
            rustc: self.string()?,
            algorithm_version: self.u32()?,
        })
    }

    fn detail(&mut self, code: ConfigurationErrorCode) -> Result<DscpV1, DscpError> {
        Ok(match code {
            ConfigurationErrorCode::MalformedConfiguration => DscpV1::MalformedConfiguration {
                file_hash: self.array()?,
            },
            ConfigurationErrorCode::NonLoopbackAddress => DscpV1::NonLoopbackAddress {
                address: self.string()?,
            },
            ConfigurationErrorCode::DuplicateRootName => DscpV1::DuplicateRootName {
                normalized_name: self.string()?,
            },
            ConfigurationErrorCode::InvalidPath => DscpV1::InvalidPath {
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
            ConfigurationErrorCode::OwnedPathOverlap => DscpV1::OwnedPathOverlap {
                first: self.owned_path_side()?,
                second: self.owned_path_side()?,
            },
            ConfigurationErrorCode::EmptyTargetApis => DscpV1::EmptyTargetApis {
                target: self.string()?,
            },
            ConfigurationErrorCode::InvalidParallelism => {
                DscpV1::InvalidParallelism { value: self.u32()? }
            }
            ConfigurationErrorCode::InvalidBatchReservation => DscpV1::InvalidBatchReservation {
                parallelism: self.u32()?,
                reservation: self.u32()?,
            },
            ConfigurationErrorCode::DirectoryAlias => DscpV1::DirectoryAlias {
                first: self.directory_alias_side()?,
                second: self.directory_alias_side()?,
            },
            ConfigurationErrorCode::UnsupportedTargetIdentity => {
                DscpV1::UnsupportedTargetIdentity {
                    target: self.string()?,
                    expected: self.layout_identity()?,
                    observed: self.layout_identity()?,
                }
            }
            ConfigurationErrorCode::DuplicateTargetName => DscpV1::DuplicateTargetName {
                normalized_name: self.string()?,
            },
            ConfigurationErrorCode::ConfigurationSourceUnavailable => {
                DscpV1::ConfigurationSourceUnavailable {
                    path: self.configuration_source_path()?,
                    failure: match self.u16()? {
                        1 => ConfigurationSourceFailureCode::Missing,
                        2 => ConfigurationSourceFailureCode::PermissionDenied,
                        3 => ConfigurationSourceFailureCode::InvalidFileType,
                        4 => ConfigurationSourceFailureCode::IoDataLoss,
                        unknown => {
                            return Err(DscpError::UnknownConfigurationSourceFailure(unknown))
                        }
                    },
                }
            }
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
        | DscpV1::ConfigurationSourceUnavailable { .. } => {}
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
            }
        }
    }
    if values.into_iter().all(is_nfc) {
        Ok(())
    } else {
        Err(DscpError::InvalidText)
    }
}

/// Stable typed failure carried by a configuration-failed input version.
/// `message` is presentation-only; `reason_hash` is exclusively DSCP v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationError {
    pub code: ConfigurationErrorCode,
    pub reason_hash: [u8; 32],
    pub detail: Box<DscpV1>,
    pub message: String,
}

impl ConfigurationError {
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
        errors: impl IntoIterator<Item = Self>,
    ) -> Result<Option<Self>, DscpError> {
        Ok(Self::canonical_set(errors)?.into_iter().next())
    }

    /// Complete canonically ordered set retained for doctor diagnostics.
    pub fn canonical_set(errors: impl IntoIterator<Item = Self>) -> Result<Vec<Self>, DscpError> {
        let mut keyed = errors
            .into_iter()
            .map(|error| {
                error.validate()?;
                let key = (error.code as u16, error.detail.canonical_detail_bytes());
                Ok((key, error))
            })
            .collect::<Result<Vec<_>, DscpError>>()?;
        keyed.sort_by(|(left_key, left), (right_key, right)| {
            left_key
                .cmp(right_key)
                .then_with(|| left.message.cmp(&right.message))
        });
        keyed.dedup_by(|(left_key, _), (right_key, _)| left_key == right_key);
        Ok(keyed.into_iter().map(|(_, error)| error).collect())
    }
}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "configuration error {:?} ({:02x?}): {}",
            self.code, self.reason_hash, self.message
        )
    }
}

impl std::error::Error for ConfigurationError {}

impl fmt::Display for PipelineFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pipeline failure: {}", self.message)
    }
}

impl std::error::Error for PipelineFailure {}

impl fmt::Display for PipelineFailureDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid pipeline failure: {self:?}")
    }
}

impl std::error::Error for PipelineFailureDecodeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum NamespaceErrorCode {
    DuplicateAssetUuid = 1,
    DuplicateBundleUuid = 2,
    SameRootNormalizedPathCollision = 3,
    IncompleteSkeleton = 4,
    UnreadableGlobalBundlePath = 5,
    InvalidPhysicalPath = 6,
    UnreadableScanSubtree = 7,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ScanFailureCode {
    PermissionDenied = 1,
    NotFound = 2,
    InvalidFileType = 3,
    SymlinkIdentityChanged = 4,
    IoDataLoss = 5,
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
pub enum ScanSubject {
    Root {
        root_name: String,
    },
    Subtree {
        root_name: String,
        raw_relative_path: PlatformPathBytes,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PhysicalPathClaim {
    pub raw_relative_path: PlatformPathBytes,
    pub file_hash: BundleFileHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceErrorV1 {
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
    UnreadableScanSubtree {
        subject: ScanSubject,
        failure: ScanFailureCode,
    },
}

/// §7/§13's closed namespace error record. `identity` commits only to
/// the typed facts; `message` is presentation text and cannot affect healing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceError {
    pub code: NamespaceErrorCode,
    pub identity: [u8; 32],
    pub detail: NamespaceErrorV1,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceErrorDecodeError {
    UnsupportedVersion(u8),
    UnknownCode(u16),
    UnknownFailureCode(u16),
    UnknownClaimantTag(u8),
    UnknownPlatformPathTag(u8),
    UnknownScanSubjectTag(u8),
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

impl fmt::Display for NamespaceErrorDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid namespace error: {self:?}")
    }
}

impl std::error::Error for NamespaceErrorDecodeError {}

impl NamespaceErrorV1 {
    pub fn code(&self) -> NamespaceErrorCode {
        match self {
            Self::DuplicateAssetUuid { .. } => NamespaceErrorCode::DuplicateAssetUuid,
            Self::DuplicateBundleUuid { .. } => NamespaceErrorCode::DuplicateBundleUuid,
            Self::SameRootNormalizedPathCollision { .. } => {
                NamespaceErrorCode::SameRootNormalizedPathCollision
            }
            Self::IncompleteSkeleton { .. } => NamespaceErrorCode::IncompleteSkeleton,
            Self::UnreadableGlobalBundlePath { .. } => {
                NamespaceErrorCode::UnreadableGlobalBundlePath
            }
            Self::InvalidPhysicalPath { .. } => NamespaceErrorCode::InvalidPhysicalPath,
            Self::UnreadableScanSubtree { .. } => NamespaceErrorCode::UnreadableScanSubtree,
        }
    }
}

impl NamespaceError {
    pub fn new(
        detail: NamespaceErrorV1,
        message: impl Into<String>,
    ) -> Result<Self, NamespaceErrorDecodeError> {
        validate_namespace_error_detail(&detail)?;
        let code = detail.code();
        let identity = namespace_error_identity(code, &detail);
        Ok(Self {
            code,
            identity,
            detail,
            message: message.into(),
        })
    }

    pub fn validate(&self) -> Result<(), NamespaceErrorDecodeError> {
        if self.code != self.detail.code() {
            return Err(NamespaceErrorDecodeError::IdentityMismatch);
        }
        validate_namespace_error_detail(&self.detail)?;
        if self.identity != namespace_error_identity(self.code, &self.detail) {
            return Err(NamespaceErrorDecodeError::IdentityMismatch);
        }
        Ok(())
    }

    pub fn persisted_bytes(&self) -> Result<Vec<u8>, NamespaceErrorDecodeError> {
        self.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.raw(&DSVP);
        encoder.u8(1);
        encoder.u16(self.code as u16);
        encode_namespace_error_detail(&mut encoder, &self.detail);
        encoder.str(&self.message);
        Ok(encoder.into_bytes())
    }

    pub fn from_persisted_bytes(bytes: &[u8]) -> Result<Self, NamespaceErrorDecodeError> {
        let mut decoder = NamespaceErrorDecoder { bytes, cursor: 0 };
        if decoder.take(4)? != DSVP {
            return Err(NamespaceErrorDecodeError::UnknownCode(0));
        }
        let version = decoder.u8()?;
        if version != 1 {
            return Err(NamespaceErrorDecodeError::UnsupportedVersion(version));
        }
        let code_raw = decoder.u16()?;
        let code = NamespaceErrorCode::try_from(code_raw)?;
        let detail = decoder.detail(code)?;
        let message = decoder.string()?;
        if decoder.cursor != bytes.len() {
            return Err(NamespaceErrorDecodeError::TrailingBytes);
        }
        Self::new(detail, message)
    }

    /// Selects the one authoritative namespace error independently of scan
    /// discovery order. Typed identity is ordered by `(code, canonical
    /// detail bytes)`; presentation text only resolves an otherwise identical
    /// typed record so duplicate diagnostics cannot reintroduce ordering.
    pub fn select_canonical(
        errors: impl IntoIterator<Item = Self>,
    ) -> Result<Option<Self>, NamespaceErrorDecodeError> {
        Ok(Self::canonical_set(errors)?.into_iter().next())
    }

    /// Returns the complete doctor-diagnostic set in the same canonical
    /// order used to select publication authority. Duplicate typed details
    /// collapse even when their presentation messages differ.
    pub fn canonical_set(
        errors: impl IntoIterator<Item = Self>,
    ) -> Result<Vec<Self>, NamespaceErrorDecodeError> {
        let mut keyed = errors
            .into_iter()
            .map(|error| {
                error.validate()?;
                let mut detail = CanonicalEncoder::new();
                encode_namespace_error_detail(&mut detail, &error.detail);
                Ok(((error.code as u16, detail.into_bytes()), error))
            })
            .collect::<Result<Vec<_>, NamespaceErrorDecodeError>>()?;
        keyed.sort_by(|(left_key, left), (right_key, right)| {
            left_key
                .cmp(right_key)
                .then_with(|| left.message.cmp(&right.message))
        });
        keyed.dedup_by(|(left_key, _), (right_key, _)| left_key == right_key);
        Ok(keyed.into_iter().map(|(_, error)| error).collect())
    }
}

impl TryFrom<u16> for NamespaceErrorCode {
    type Error = NamespaceErrorDecodeError;
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::DuplicateAssetUuid),
            2 => Ok(Self::DuplicateBundleUuid),
            3 => Ok(Self::SameRootNormalizedPathCollision),
            4 => Ok(Self::IncompleteSkeleton),
            5 => Ok(Self::UnreadableGlobalBundlePath),
            6 => Ok(Self::InvalidPhysicalPath),
            7 => Ok(Self::UnreadableScanSubtree),
            other => Err(NamespaceErrorDecodeError::UnknownCode(other)),
        }
    }
}

pub(crate) struct NamespaceErrorDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> NamespaceErrorDecoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    /// Fails unless every byte was consumed.
    pub(crate) fn finish(&self) -> Result<(), NamespaceErrorDecodeError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(NamespaceErrorDecodeError::TrailingBytes)
        }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], NamespaceErrorDecodeError> {
        let end = self
            .cursor
            .checked_add(count)
            .ok_or(NamespaceErrorDecodeError::Truncated)?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or(NamespaceErrorDecodeError::Truncated)?;
        self.cursor = end;
        Ok(value)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, NamespaceErrorDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, NamespaceErrorDecodeError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, NamespaceErrorDecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], NamespaceErrorDecodeError> {
        Ok(self.take(N)?.try_into().unwrap())
    }

    pub(crate) fn string(&mut self) -> Result<String, NamespaceErrorDecodeError> {
        let len = usize::try_from(self.u32()?).map_err(|_| NamespaceErrorDecodeError::Truncated)?;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| NamespaceErrorDecodeError::InvalidUtf8)
    }

    pub(crate) fn source(&mut self) -> Result<ReadableBundleSource, NamespaceErrorDecodeError> {
        Ok(ReadableBundleSource {
            root_name: self.string()?,
            normalized_path: self.string()?,
            file_hash: BundleFileHash(self.array()?),
        })
    }

    fn sources(&mut self) -> Result<Vec<ReadableBundleSource>, NamespaceErrorDecodeError> {
        let count =
            usize::try_from(self.u32()?).map_err(|_| NamespaceErrorDecodeError::Truncated)?;
        if count > self.bytes.len().saturating_sub(self.cursor) / 40 {
            return Err(NamespaceErrorDecodeError::Truncated);
        }
        (0..count).map(|_| self.source()).collect()
    }

    fn claimants(&mut self) -> Result<Vec<AssetClaimant>, NamespaceErrorDecodeError> {
        let count =
            usize::try_from(self.u32()?).map_err(|_| NamespaceErrorDecodeError::Truncated)?;
        if count > self.bytes.len().saturating_sub(self.cursor) / 2 {
            return Err(NamespaceErrorDecodeError::Truncated);
        }
        (0..count).map(|_| self.claimant()).collect()
    }

    pub(crate) fn claimant(&mut self) -> Result<AssetClaimant, NamespaceErrorDecodeError> {
        match self.u8()? {
            1 => Ok(AssetClaimant::Authored {
                source: self.source()?,
                bundle: BundleUuid(self.array()?),
                local_id: self.string()?,
            }),
            2 => Ok(AssetClaimant::Derived {
                parent: AssetUuid(self.array()?),
                output_key: self.string()?,
            }),
            other => Err(NamespaceErrorDecodeError::UnknownClaimantTag(other)),
        }
    }

    fn path_claims(&mut self) -> Result<Vec<PhysicalPathClaim>, NamespaceErrorDecodeError> {
        let count =
            usize::try_from(self.u32()?).map_err(|_| NamespaceErrorDecodeError::Truncated)?;
        if count > self.bytes.len().saturating_sub(self.cursor) / 6 {
            return Err(NamespaceErrorDecodeError::Truncated);
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

    fn platform_path(&mut self) -> Result<PlatformPathBytes, NamespaceErrorDecodeError> {
        match self.u8()? {
            1 => {
                let len = usize::try_from(self.u32()?)
                    .map_err(|_| NamespaceErrorDecodeError::Truncated)?;
                Ok(PlatformPathBytes::Unix(self.take(len)?.to_vec()))
            }
            2 => {
                let count = usize::try_from(self.u32()?)
                    .map_err(|_| NamespaceErrorDecodeError::Truncated)?;
                if count > self.bytes.len().saturating_sub(self.cursor) / 2 {
                    return Err(NamespaceErrorDecodeError::Truncated);
                }
                let units = (0..count)
                    .map(|_| self.u16())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlatformPathBytes::Windows(units))
            }
            other => Err(NamespaceErrorDecodeError::UnknownPlatformPathTag(other)),
        }
    }

    fn detail(
        &mut self,
        code: NamespaceErrorCode,
    ) -> Result<NamespaceErrorV1, NamespaceErrorDecodeError> {
        Ok(match code {
            NamespaceErrorCode::DuplicateAssetUuid => NamespaceErrorV1::DuplicateAssetUuid {
                asset: AssetUuid(self.array()?),
                claimants: self.claimants()?,
            },
            NamespaceErrorCode::DuplicateBundleUuid => NamespaceErrorV1::DuplicateBundleUuid {
                bundle: BundleUuid(self.array()?),
                sources: self.sources()?,
            },
            NamespaceErrorCode::SameRootNormalizedPathCollision => {
                NamespaceErrorV1::SameRootNormalizedPathCollision {
                    root_name: self.string()?,
                    normalized_path: self.string()?,
                    claims: self.path_claims()?,
                }
            }
            NamespaceErrorCode::IncompleteSkeleton => NamespaceErrorV1::IncompleteSkeleton {
                source: self.source()?,
                failure: match self.u16()? {
                    1 => SkeletonFailureCode::EnvelopeMalformed,
                    2 => SkeletonFailureCode::MissingFormatVersion,
                    3 => SkeletonFailureCode::InvalidBundleUuid,
                    4 => SkeletonFailureCode::IncompleteAssetIdentity,
                    5 => SkeletonFailureCode::IncompleteTypeIdentity,
                    6 => SkeletonFailureCode::IncompleteTagIdentity,
                    other => return Err(NamespaceErrorDecodeError::UnknownFailureCode(other)),
                },
            },
            NamespaceErrorCode::UnreadableGlobalBundlePath => {
                NamespaceErrorV1::UnreadableGlobalBundlePath {
                    root_name: self.string()?,
                    normalized_path: self.string()?,
                    failure: match self.u16()? {
                        1 => GlobalBundleReadFailureCode::PermissionDenied,
                        2 => GlobalBundleReadFailureCode::InvalidFileType,
                        3 => GlobalBundleReadFailureCode::SymlinkIdentityChanged,
                        4 => GlobalBundleReadFailureCode::IoDataLoss,
                        other => return Err(NamespaceErrorDecodeError::UnknownFailureCode(other)),
                    },
                }
            }
            NamespaceErrorCode::InvalidPhysicalPath => NamespaceErrorV1::InvalidPhysicalPath {
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
                    other => return Err(NamespaceErrorDecodeError::UnknownFailureCode(other)),
                },
            },
            NamespaceErrorCode::UnreadableScanSubtree => NamespaceErrorV1::UnreadableScanSubtree {
                subject: match self.u8()? {
                    1 => ScanSubject::Root {
                        root_name: self.string()?,
                    },
                    2 => ScanSubject::Subtree {
                        root_name: self.string()?,
                        raw_relative_path: self.platform_path()?,
                    },
                    other => return Err(NamespaceErrorDecodeError::UnknownScanSubjectTag(other)),
                },
                failure: match self.u16()? {
                    1 => ScanFailureCode::PermissionDenied,
                    2 => ScanFailureCode::NotFound,
                    3 => ScanFailureCode::InvalidFileType,
                    4 => ScanFailureCode::SymlinkIdentityChanged,
                    5 => ScanFailureCode::IoDataLoss,
                    other => return Err(NamespaceErrorDecodeError::UnknownFailureCode(other)),
                },
            },
        })
    }
}

fn namespace_error_identity(code: NamespaceErrorCode, detail: &NamespaceErrorV1) -> [u8; 32] {
    let mut encoder = CanonicalEncoder::new();
    encoder.raw(&DSVP);
    encoder.u8(1);
    encoder.u16(code as u16);
    encode_namespace_error_detail(&mut encoder, detail);
    *blake3::hash(&encoder.into_bytes()).as_bytes()
}

fn encode_namespace_error_detail(encoder: &mut CanonicalEncoder, detail: &NamespaceErrorV1) {
    match detail {
        NamespaceErrorV1::DuplicateAssetUuid { asset, claimants } => {
            encoder.raw(&asset.0);
            encoder.seq(claimants, encode_asset_claimant);
        }
        NamespaceErrorV1::DuplicateBundleUuid { bundle, sources } => {
            encoder.raw(&bundle.0);
            encode_bundle_sources(encoder, sources);
        }
        NamespaceErrorV1::SameRootNormalizedPathCollision {
            root_name,
            normalized_path,
            claims,
        } => {
            encoder.str(root_name);
            encoder.str(normalized_path);
            encoder.seq(claims, encode_physical_path_claim);
        }
        NamespaceErrorV1::IncompleteSkeleton { source, failure } => {
            encode_bundle_source(encoder, source);
            encoder.u16(*failure as u16);
        }
        NamespaceErrorV1::UnreadableGlobalBundlePath {
            root_name,
            normalized_path,
            failure,
        } => {
            encoder.str(root_name);
            encoder.str(normalized_path);
            encoder.u16(*failure as u16);
        }
        NamespaceErrorV1::InvalidPhysicalPath {
            root_name,
            raw_relative_path,
            failure,
        } => {
            encoder.str(root_name);
            encode_platform_path(encoder, raw_relative_path);
            encoder.u16(*failure as u16);
        }
        NamespaceErrorV1::UnreadableScanSubtree { subject, failure } => {
            match subject {
                ScanSubject::Root { root_name } => {
                    encoder.u8(1);
                    encoder.str(root_name);
                }
                ScanSubject::Subtree {
                    root_name,
                    raw_relative_path,
                } => {
                    encoder.u8(2);
                    encoder.str(root_name);
                    encode_platform_path(encoder, raw_relative_path);
                }
            }
            encoder.u16(*failure as u16);
        }
    }
}

pub(crate) fn encode_asset_claimant(encoder: &mut CanonicalEncoder, claimant: &AssetClaimant) {
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

pub(crate) fn encode_bundle_source(encoder: &mut CanonicalEncoder, source: &ReadableBundleSource) {
    encoder.str(&source.root_name);
    encoder.str(&source.normalized_path);
    encoder.raw(&source.file_hash.0);
}

fn validate_namespace_error_detail(
    detail: &NamespaceErrorV1,
) -> Result<(), NamespaceErrorDecodeError> {
    let validate_source = |source: &ReadableBundleSource| {
        validate_root_and_path(&source.root_name, &source.normalized_path)
    };
    let validate_collision_sources = |sources: &[ReadableBundleSource]| {
        if sources.len() < 2 {
            return Err(NamespaceErrorDecodeError::InsufficientSources);
        }
        if sources.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(NamespaceErrorDecodeError::NonCanonicalSources);
        }
        sources.iter().try_for_each(validate_source)
    };

    match detail {
        NamespaceErrorV1::DuplicateAssetUuid { claimants, .. } => {
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
        NamespaceErrorV1::DuplicateBundleUuid { sources, .. } => {
            validate_collision_sources(sources)?;
        }
        NamespaceErrorV1::SameRootNormalizedPathCollision {
            root_name,
            normalized_path,
            claims,
        } => {
            validate_root_and_path(root_name, normalized_path)?;
            validate_strict_two(claims)?;
            claims.iter().try_for_each(validate_physical_path_claim)?;
        }
        NamespaceErrorV1::IncompleteSkeleton { source, .. } => validate_source(source)?,
        NamespaceErrorV1::UnreadableGlobalBundlePath {
            root_name,
            normalized_path,
            ..
        } => {
            validate_root_and_path(root_name, normalized_path)?;
        }
        NamespaceErrorV1::InvalidPhysicalPath {
            root_name,
            raw_relative_path,
            failure,
        } => {
            validate_root_name(root_name)?;
            if classify_invalid_physical_path(raw_relative_path) != Some(*failure) {
                return Err(NamespaceErrorDecodeError::InvalidRawPath);
            }
        }
        NamespaceErrorV1::UnreadableScanSubtree { subject, .. } => match subject {
            ScanSubject::Root { root_name } => validate_root_name(root_name)?,
            ScanSubject::Subtree {
                root_name,
                raw_relative_path,
            } => {
                validate_root_name(root_name)?;
                if classify_invalid_physical_path(raw_relative_path).is_some() {
                    return Err(NamespaceErrorDecodeError::InvalidRawPath);
                }
            }
        },
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

fn validate_strict_two<T: Ord>(values: &[T]) -> Result<(), NamespaceErrorDecodeError> {
    if values.len() < 2 {
        return Err(NamespaceErrorDecodeError::InsufficientSources);
    }
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(NamespaceErrorDecodeError::NonCanonicalSources);
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), NamespaceErrorDecodeError> {
    use unicode_normalization::UnicodeNormalization;
    if value.is_empty()
        || value.nfc().collect::<String>() != value
        || value.contains(['/', '\\', '\0'])
    {
        return Err(NamespaceErrorDecodeError::InvalidClaimant);
    }
    Ok(())
}

fn validate_physical_path_claim(
    claim: &PhysicalPathClaim,
) -> Result<(), NamespaceErrorDecodeError> {
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
        Err(NamespaceErrorDecodeError::InvalidRawPath)
    }
}

fn validate_root_and_path(
    root_name: &str,
    normalized_path: &str,
) -> Result<(), NamespaceErrorDecodeError> {
    validate_root_name(root_name)?;
    validate_normalized_path(normalized_path)
}

fn validate_root_name(root_name: &str) -> Result<(), NamespaceErrorDecodeError> {
    use unicode_normalization::UnicodeNormalization;

    if root_name.is_empty()
        || root_name.nfc().collect::<String>() != root_name
        || root_name.contains(['/', '\\', '\0'])
    {
        return Err(NamespaceErrorDecodeError::InvalidRootName);
    }
    Ok(())
}

fn validate_normalized_path(normalized_path: &str) -> Result<(), NamespaceErrorDecodeError> {
    use unicode_normalization::UnicodeNormalization;

    if normalized_path.is_empty()
        || normalized_path.nfc().collect::<String>() != normalized_path
        || normalized_path.contains(['\\', '\0'])
        || normalized_path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(NamespaceErrorDecodeError::InvalidPath);
    }
    Ok(())
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "namespace error: {}", self.message)
    }
}

impl std::error::Error for NamespaceError {}

/// What an `errors` row is about (LOCKLESS.md §4).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorScope {
    /// A file, or with an empty or directory `path`, a subtree.
    File {
        root_name: String,
        path: String,
    },
    Bundle(BundleUuid),
    Asset(AssetUuid),
    Target(String),
    Pipeline,
    Configuration,
    Daemon,
}

impl ErrorScope {
    pub(crate) fn kind(&self) -> i64 {
        match self {
            Self::File { .. } => 1,
            Self::Bundle(_) => 2,
            Self::Asset(_) => 3,
            Self::Target(_) => 4,
            Self::Pipeline => 5,
            Self::Configuration => 6,
            Self::Daemon => 7,
        }
    }

    pub(crate) fn id(&self) -> Vec<u8> {
        match self {
            Self::File { root_name, path } => {
                let mut id = root_name.as_bytes().to_vec();
                id.push(0);
                id.extend_from_slice(path.as_bytes());
                id
            }
            Self::Bundle(bundle) => bundle.0.to_vec(),
            Self::Asset(asset) => asset.0.to_vec(),
            Self::Target(name) => name.as_bytes().to_vec(),
            Self::Pipeline | Self::Configuration | Self::Daemon => Vec::new(),
        }
    }
}

fn lossy_path(path: &PlatformPathBytes) -> String {
    match path {
        PlatformPathBytes::Unix(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        PlatformPathBytes::Windows(units) => String::from_utf16_lossy(units),
    }
}

impl NamespaceError {
    /// The one entity this error is about. A collision is about the
    /// colliding UUID; every other namespace error about a file or subtree.
    pub fn scope(&self) -> ErrorScope {
        let file = |root_name: &str, path: String| ErrorScope::File {
            root_name: root_name.to_owned(),
            path,
        };
        match &self.detail {
            NamespaceErrorV1::DuplicateAssetUuid { asset, .. } => ErrorScope::Asset(*asset),
            NamespaceErrorV1::DuplicateBundleUuid { bundle, .. } => ErrorScope::Bundle(*bundle),
            NamespaceErrorV1::SameRootNormalizedPathCollision {
                root_name,
                normalized_path,
                ..
            }
            | NamespaceErrorV1::UnreadableGlobalBundlePath {
                root_name,
                normalized_path,
                ..
            } => file(root_name, normalized_path.clone()),
            NamespaceErrorV1::IncompleteSkeleton { source, .. } => {
                file(&source.root_name, source.normalized_path.clone())
            }
            NamespaceErrorV1::InvalidPhysicalPath {
                root_name,
                raw_relative_path,
                ..
            } => file(root_name, lossy_path(raw_relative_path)),
            NamespaceErrorV1::UnreadableScanSubtree { subject, .. } => match subject {
                ScanSubject::Root { root_name } => file(root_name, String::new()),
                ScanSubject::Subtree {
                    root_name,
                    raw_relative_path,
                } => file(root_name, lossy_path(raw_relative_path)),
            },
        }
    }
}
