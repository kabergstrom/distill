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
use distill_core::id::{ContentHash, LogicalHash, TypeUuid};
use distill_core::target_set::{CanonicalTargetSet, TargetSetError, TargetSetHash};

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

/// Stable typed failure carried by a configuration-poisoned input
/// version (R22/H4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationPoison {
    pub error: String,
}

impl fmt::Display for ConfigurationPoison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "configuration poison: {}", self.error)
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
        error: ConfigurationPoison,
        last_good: Option<Arc<ConfigurationEpoch>>,
    },
}

impl ConfigurationState {
    pub fn epoch(&self) -> Result<&Arc<ConfigurationEpoch>, &ConfigurationPoison> {
        match self {
            ConfigurationState::Ready(epoch) => Ok(epoch),
            ConfigurationState::Poisoned { error, .. } => Err(error),
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
