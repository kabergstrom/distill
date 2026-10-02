//! DSBI/DSSI/DSIH/DSNK key construction (§§8–9).

use distill_core::canonical::{domain_digest, CanonicalEncoder, DSSI, DSTG};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};

use crate::pipeline::{Target, TargetArch, TargetOs};
use crate::trace::{trace_digest, TraceOp};

const DSBI: [u8; 4] = *b"DSBI";
const DSIH: [u8; 4] = *b"DSIH";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputHash {
    pub key: String,
    pub logical: LogicalHash,
    pub layout: LayoutHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticInputs {
    pub asset: AssetUuid,
    pub stage: u16,
    pub input_hash: ContentHash,
    pub target_def_hash: [u8; 32],
    pub processor_id: String,
    pub processor_version: u32,
    pub dylib_hash: [u8; 32],
    pub output_hashes: Vec<OutputHash>,
    pub artifact_format_version: u32,
}

pub fn static_inputs_digest(inputs: &StaticInputs) -> [u8; 32] {
    domain_digest(DSSI, 1, |e| encode_static(e, inputs))
}

/// Canonical DSSI body retained in result records for audit/recovery.  The
/// CAS index still keys only on [`static_inputs_digest`].
pub fn static_inputs_canonical_bytes(inputs: &StaticInputs) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encode_static(&mut encoder, inputs);
    encoder.into_bytes()
}

fn encode_static(e: &mut CanonicalEncoder, inputs: &StaticInputs) {
    e.raw(&inputs.asset.0);
    e.u16(inputs.stage);
    e.raw(&inputs.input_hash.0);
    e.raw(&inputs.target_def_hash);
    e.str(&inputs.processor_id);
    e.u32(inputs.processor_version);
    e.raw(&inputs.dylib_hash);
    let mut outputs = inputs.output_hashes.clone();
    outputs.sort_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));
    e.seq(&outputs, |e, output| {
        e.str(&output.key);
        e.raw(&output.logical.0);
        e.raw(&output.layout.0);
    });
    e.u32(inputs.artifact_format_version);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMigration {
    pub bundle_hash: [u8; 32],
    pub planner_version: u32,
    pub dylib_hash: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticMigration {
    pub from: LogicalHash,
    pub to: LogicalHash,
    pub planner_version: u32,
    /// Present only when the selected automatic plan executes a registered
    /// default materializer from the pinned pipeline epoch.
    pub dylib_hash: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildImportInputs {
    pub asset: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub canonical_bundle_bytes: Vec<u8>,
    pub logical: LogicalHash,
    pub layout: LayoutHash,
    pub migrations: Vec<AppliedMigration>,
    pub automatic_migration: Option<AutomaticMigration>,
    /// Validator code participates only when this authored type has at least
    /// one registered validator in the pinned epoch.
    pub validator_dylib_hash: Option<[u8; 32]>,
    pub artifact_format_version: u32,
}

pub fn build_import_digest(inputs: &BuildImportInputs) -> [u8; 32] {
    domain_digest(DSBI, 1, |e| {
        e.raw(&inputs.asset.0);
        e.raw(&inputs.bundle.0);
        e.str(&inputs.local_id);
        e.raw(&inputs.authored_type.0);
        e.raw(&inputs.terminal_type.0);
        e.u64(inputs.canonical_bundle_bytes.len() as u64);
        e.raw(&inputs.canonical_bundle_bytes);
        e.raw(&inputs.logical.0);
        e.raw(&inputs.layout.0);
        e.seq(&inputs.migrations, |e, migration| {
            e.raw(&migration.bundle_hash);
            e.u32(migration.planner_version);
            e.option(migration.dylib_hash, |e, hash| e.raw(hash));
        });
        e.option(inputs.automatic_migration.as_ref(), |e, migration| {
            e.raw(&migration.from.0);
            e.raw(&migration.to.0);
            e.u32(migration.planner_version);
            e.option(migration.dylib_hash, |e, hash| e.raw(hash));
        });
        e.option(inputs.validator_dylib_hash, |e, hash| e.raw(hash));
        e.u32(inputs.artifact_format_version);
    })
}

pub fn full_input_hash(inputs: &StaticInputs, trace: &[TraceOp]) -> [u8; 32] {
    domain_digest(DSIH, 1, |e| {
        encode_static(e, inputs);
        e.raw(&trace_digest(trace));
    })
}

/// Canonical target identity. Names are data, never config ordinals.
pub fn target_definition_hash(target: &Target) -> [u8; 32] {
    domain_digest(DSTG, 1, |e| {
        e.u8(match target.os {
            TargetOs::Linux => 0,
            TargetOs::MacOs => 1,
            TargetOs::Windows => 2,
        });
        e.u8(match target.arch {
            TargetArch::Aarch64 => 0,
            TargetArch::X86_64 => 1,
        });
        e.set(&target.apis, |e, api| e.str(&api.0));
        e.bool(target.optimize);
        e.bool(target.debug_info);
        let identity = &target.layout_identity;
        e.str(&identity.target_triple);
        e.str(&identity.rustc);
        e.u32(identity.algorithm_version);
    })
}

const DSNK: [u8; 4] = *b"DSNK";

/// One processor stage of a node's chain: its identity and the closed
/// output table it declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStage {
    pub processor_id: String,
    pub processor_version: u32,
    pub primary: TypeUuid,
    pub extras: Vec<(String, TypeUuid)>,
}

/// One type a node's chain names, with the schema identity its artifacts
/// encode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeType {
    pub type_uuid: TypeUuid,
    pub logical: LogicalHash,
    pub layout: LayoutHash,
    /// The schema authority's build-only policy: a runtime closure that
    /// names a build-only type fails, so a policy change is a new node.
    pub build_only: bool,
}

/// The static inputs of one asset node: everything its served outputs are a
/// function of besides the answers to the queries its build traces. Nothing
/// here names an input version: two snapshots whose node inputs agree share
/// one key, and the traced answers decide whether a result serves both.
///
/// The asset uuid is an input — the artifact header encodes it and derived
/// outputs are named `UUIDv5(asset, key)`. The bundle is keyed by the
/// recorded hash of its raw bytes, so keying reads no file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInputs {
    pub asset: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub bundle_hash: ContentHash,
    pub authored_type: TypeUuid,
    pub authored_logical: LogicalHash,
    pub target_def_hash: [u8; 32],
    pub dylib_hash: [u8; 32],
    /// Whether the pinned epoch registers a validator for the authored type.
    pub validated: bool,
    pub terminal_type: TypeUuid,
    pub extras: Vec<(String, TypeUuid)>,
    pub stages: Vec<NodeStage>,
    pub types: Vec<NodeType>,
    pub migration_planner_version: u32,
    pub artifact_format_version: u32,
}

/// The `"DSNK"` node key.
pub fn node_digest(inputs: &NodeInputs) -> [u8; 32] {
    domain_digest(DSNK, 2, |e| encode_node(e, inputs))
}

/// Canonical DSNK body retained in the node's result record.
pub fn node_canonical_bytes(inputs: &NodeInputs) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encode_node(&mut encoder, inputs);
    encoder.into_bytes()
}

fn encode_node(e: &mut CanonicalEncoder, inputs: &NodeInputs) {
    e.raw(&inputs.asset.0);
    e.raw(&inputs.bundle.0);
    e.str(&inputs.local_id);
    e.raw(&inputs.bundle_hash.0);
    e.raw(&inputs.authored_type.0);
    e.raw(&inputs.authored_logical.0);
    e.raw(&inputs.target_def_hash);
    e.raw(&inputs.dylib_hash);
    e.bool(inputs.validated);
    e.raw(&inputs.terminal_type.0);
    let mut extras = inputs.extras.clone();
    extras.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    e.seq(&extras, |e, (key, type_uuid)| {
        e.str(key);
        e.raw(&type_uuid.0);
    });
    e.seq(&inputs.stages, |e, stage| {
        e.str(&stage.processor_id);
        e.u32(stage.processor_version);
        e.raw(&stage.primary.0);
        let mut extras = stage.extras.clone();
        extras.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        e.seq(&extras, |e, (key, type_uuid)| {
            e.str(key);
            e.raw(&type_uuid.0);
        });
    });
    let mut types = inputs.types.clone();
    types.sort_by_key(|node_type| node_type.type_uuid);
    types.dedup();
    e.seq(&types, |e, node_type| {
        e.raw(&node_type.type_uuid.0);
        e.raw(&node_type.logical.0);
        e.raw(&node_type.layout.0);
        e.bool(node_type.build_only);
    });
    e.u32(inputs.migration_planner_version);
    e.u32(inputs.artifact_format_version);
}
