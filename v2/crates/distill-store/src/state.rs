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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use distill_core::attestation::CompiledAttestationDigest;
use distill_core::canonical::{domain_digest, CanonicalEncoder, DSCP};
use distill_core::id::{AssetUuid, ContentHash, LogicalHash, TypeUuid};
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

/// The named failure that kept a candidate epoch from publishing (§3,
/// §13): identity checks, registration, configuration validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelinePoison {
    pub error: String,
}

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
            match self {
                Self::MalformedConfiguration { file_hash } => encoder.raw(file_hash),
                Self::NonLoopbackAddress { address } => encoder.str(address),
                Self::DuplicateRootName { normalized_name } => encoder.str(normalized_name),
                Self::InvalidPath {
                    key,
                    normalized_or_raw_path,
                } => {
                    encoder.u8(*key as u8);
                    encoder.str(normalized_or_raw_path);
                }
                Self::OwnedPathOverlap { first, second } => {
                    encode_symmetric_pair(encoder, first, second, encode_owned_path_side);
                }
                Self::EmptyTargetApis { target } => encoder.str(target),
                Self::InvalidParallelism { value } => encoder.u32(*value),
                Self::InvalidBatchReservation {
                    parallelism,
                    reservation,
                } => {
                    encoder.u32(*parallelism);
                    encoder.u32(*reservation);
                }
                Self::DirectoryAlias { first, second } => {
                    encode_symmetric_pair(encoder, first, second, encode_directory_alias_side);
                }
                Self::MissingLineageManifest => {}
                Self::DuplicateLineageManifest { entries } => {
                    encoder.set(entries, |encoder, entry| encoder.raw(&entry.0));
                }
                Self::UnsupportedTargetIdentity {
                    target,
                    expected,
                    observed,
                } => {
                    encoder.str(target);
                    encode_compilation_identity(encoder, expected);
                    encode_compilation_identity(encoder, observed);
                }
                Self::DuplicateTargetName { normalized_name } => encoder.str(normalized_name),
            }
        })
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

/// Stable typed failure carried by a configuration-poisoned input version.
/// `message` is presentation-only; `reason_hash` is exclusively DSCP v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationPoison {
    pub code: ConfigurationPoisonCode,
    pub reason_hash: [u8; 32],
    pub message: String,
}

impl ConfigurationPoison {
    pub fn from_reason(reason: &DscpV1, message: impl Into<String>) -> Self {
        Self {
            code: reason.code(),
            reason_hash: reason.reason_hash(),
            message: message.into(),
        }
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
        write!(f, "pipeline poison: {}", self.error)
    }
}

impl std::error::Error for PipelinePoison {}

/// §7/§13's **version-global** poison: `current` advanced carrying an
/// identity-validation failure. Uniform — every namespace-facing
/// operation fails with this same error, never one surviving duplicate,
/// never last-good metadata from a projection that happens not to touch
/// the colliding rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionPoison {
    pub error: String,
}

impl fmt::Display for VersionPoison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "version poison: {}", self.error)
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
