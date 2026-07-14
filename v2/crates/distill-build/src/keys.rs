//! DSBI/DSSI/DSIH key construction (§§8–9).

use distill_core::canonical::{domain_digest, CanonicalEncoder, DSSI};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};

use crate::pipeline::{Target, TargetArch, TargetOs};
use crate::trace::{trace_digest, TraceOp};

const DSBI: [u8; 4] = *b"DSBI";
const DSIH: [u8; 4] = *b"DSIH";
const DSTG: [u8; 4] = *b"DSTG";

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
        e.u32(inputs.artifact_format_version);
    })
}

pub fn full_input_hash(inputs: &StaticInputs, trace: &[TraceOp]) -> [u8; 32] {
    domain_digest(DSIH, 1, |e| {
        encode_static(e, inputs);
        e.raw(&trace_digest(trace));
    })
}

/// Canonical target identity plus caller-provided target options (already
/// canonical key/value pairs). Names are data, never config ordinals.
pub fn target_definition_hash(target: &Target, options: &[(String, String)]) -> [u8; 32] {
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
        let identity = &target.compilation_identity;
        e.str(&identity.target_triple);
        e.str(&identity.rustc);
        e.raw(&identity.source_fingerprint);
        e.set(identity.features.iter(), |e, (package, feature)| {
            e.str(package);
            e.str(feature);
        });
        e.set(identity.cfgs.iter(), |e, cfg| e.str(cfg));
        e.raw(&identity.manifest_lock_hash);
        e.u32(identity.algorithm_version);
        let mut options = options.to_vec();
        options.sort();
        options.dedup();
        e.seq(&options, |e, (key, value)| {
            e.str(key);
            e.str(value);
        });
    })
}
