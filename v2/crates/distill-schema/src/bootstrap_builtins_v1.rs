//! Checked-in Rust representation of the five bundle-format-v1 controls.
//!
//! This file is deliberately boring generated source.  Its logical walks are
//! checked against the byte-authoritative DSB and its target-native layouts
//! are the only measurements accepted by the bootstrap generator.

#![allow(dead_code)]

use std::collections::BTreeMap;

use distill_asset::{asset, AssetReflect, AssetType, Blob};
use distill_core::attestation::{
    BootstrapControlSpecRowV1, BootstrapControlSpecV1, BootstrapControlSymbol, CompiledTypeRow,
    ControlRole, RegistryExtraFact, RegistryExtraRow, RegistryExtrasV1, RegistryPathStep,
    SchemaNodeId, BOOTSTRAP_CONTROL_COUNT,
};
use distill_core::id::TypeUuid;
use distill_wire::native::NativeLayoutNode;

#[asset(uuid = "367ce24c-cec3-5b17-b40f-362f900cb5b0", build_only)]
pub struct PackDefinitionV1 {
    pub roots: Vec<AssetQueryV1>,
    pub target: String,
    pub zstd_level: i32,
    pub include_path_table: bool,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000001")]
pub struct AssetQueryV1 {
    pub uuid: Option<[u8; 16]>,
    pub bundle_path: Option<String>,
    pub local_id: Option<String>,
    pub bundle_uuid: Option<[u8; 16]>,
    pub authored_type: Option<[u8; 16]>,
    pub terminal_type: Option<[u8; 16]>,
    pub tag: Option<TagSelectorV1>,
    pub path_prefix: Option<String>,
    pub path_glob: Option<String>,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000002")]
pub struct TagSelectorV1 {
    pub tag: String,
    pub value: Option<String>,
}

#[asset(uuid = "46800ec6-0726-5d36-9a6c-c5c6d3c25337", build_only)]
pub struct SchemaLineageManifestV1 {
    pub types: BTreeMap<[u8; 16], AcceptedTypeLineageV1>,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000003")]
pub struct AcceptedTypeLineageV1 {
    pub epochs: Vec<AcceptedSchemaEpochV1>,
    pub current: u32,
    pub authority: TypeAuthorityStateV1,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000004")]
pub struct AcceptedSchemaEpochV1 {
    pub digest: [u8; 32],
    pub forward_parent: Option<u32>,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000005")]
#[repr(u8)]
pub enum TypeAuthorityStateV1 {
    Active {},
    Retired { retired_from: u32 },
}

#[asset(uuid = "5705382c-ee65-5a35-a8ea-d530f98a7712", build_only)]
pub struct ImportRecordV1 {
    pub importer: String,
    pub sources: Vec<RootedPathV1>,
    pub watch: bool,
    pub read_set: Vec<FileDepV1>,
    pub settings: String,
    pub origin: Option<DirectoryOriginV1>,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000006")]
pub struct RootedPathV1 {
    pub root: String,
    pub path: String,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000007")]
pub struct DirectoryOriginV1 {
    pub rules_bundle: [u8; 16],
    pub rule: [u8; 16],
    pub group: RootedPathV1,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000008")]
pub struct FileQueryV1 {
    pub path_prefix: Option<String>,
    pub path_glob: Option<String>,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000009")]
pub struct FileContentObservationV1 {
    pub path: RootedPathV1,
    pub hash: [u8; 32],
}

#[asset(uuid = "b0000000-0000-4000-8000-00000000000a")]
#[repr(u8)]
pub enum CapabilityKeyV1 {
    MigrationFn { key: String },
    DefaultTable { type_uuid: [u8; 16] },
    Importer { id: String },
    Processor { input: [u8; 16] },
    Tool { id: String },
}

#[asset(uuid = "b0000000-0000-4000-8000-00000000000b")]
#[repr(u8)]
pub enum StableFailureFingerprintV1 {
    RawFile { class: u16, detail: [u8; 32] },
    Tool { tool: String, class: u16 },
    Poisoned { bundle: [u8; 16] },
    Local { class: u16, detail: [u8; 32] },
}

#[asset(uuid = "b0000000-0000-4000-8000-00000000000c")]
#[repr(u8)]
pub enum ObservedHashV1 {
    Ok { value: [u8; 32] },
    Err { failure: StableFailureFingerprintV1 },
}

#[asset(uuid = "b0000000-0000-4000-8000-00000000000d")]
#[repr(u8)]
pub enum ObservedRootV1 {
    Ok { value: Option<String> },
    Err { failure: StableFailureFingerprintV1 },
}

#[asset(uuid = "b0000000-0000-4000-8000-00000000000e")]
#[repr(u8)]
pub enum ObservedContentV1 {
    Ok { value: FileContentObservationV1 },
    Err { failure: StableFailureFingerprintV1 },
}

#[asset(uuid = "b0000000-0000-4000-8000-00000000000f")]
#[repr(u8)]
pub enum FileDepV1 {
    Read {
        path: String,
        observed: ObservedContentV1,
    },
    Probe {
        path: String,
        observed: ObservedRootV1,
    },
    Listing {
        query: FileQueryV1,
        observed: ObservedHashV1,
    },
    Capability {
        key: CapabilityKeyV1,
        observed: ObservedHashV1,
    },
}

#[asset(uuid = "7c9cfdbf-d0ca-5933-b8de-39a858eb06b3", build_only)]
pub struct MigrationV1 {
    pub target_type_uuid: [u8; 16],
    pub from_hash: [u8; 32],
    pub to_hash: [u8; 32],
    pub from_lineage: LineageStampV1,
    pub to_lineage: LineageStampV1,
    pub kind: MigrationKindV1,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000010")]
pub struct LineageStampV1 {
    pub epochs: Vec<AcceptedSchemaEpochV1>,
    pub cursor: u32,
    pub chain: [u8; 32],
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000011")]
#[repr(u8)]
pub enum MigrationKindV1 {
    Ops { ops: Vec<MigrationOpV1> },
    Function { key: String },
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000012")]
pub struct FieldPathV1 {
    pub segments: Vec<String>,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000013")]
#[repr(u8)]
pub enum AuthoredValueV1 {
    Null {},
    Bool {
        value: bool,
    },
    Int {
        value: i128,
    },
    UInt {
        value: u128,
    },
    Float {
        value: f64,
    },
    Str {
        value: String,
    },
    Array {
        value: Vec<AuthoredValueV1>,
    },
    Object {
        value: BTreeMap<String, AuthoredValueV1>,
    },
    Blob {
        #[asset(blob)]
        value: Blob,
    },
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000014")]
#[repr(u8)]
pub enum MigrationOpV1 {
    CopyField {
        from: FieldPathV1,
        to: FieldPathV1,
    },
    Widen {
        from: FieldPathV1,
        to: FieldPathV1,
    },
    WriteValue {
        to: FieldPathV1,
        value: AuthoredValueV1,
    },
    WriteFieldDefault {
        to: FieldPathV1,
    },
    WriteParentDefault {
        to: FieldPathV1,
    },
    WriteNone {
        to: FieldPathV1,
    },
    DropField {
        at: FieldPathV1,
    },
    MapVariant {
        at: FieldPathV1,
        from: String,
        to: String,
        payload: Vec<MigrationOpV1>,
    },
    MigrateElements {
        at: FieldPathV1,
        element: Vec<MigrationOpV1>,
    },
    MigrateMapKeys {
        at: FieldPathV1,
        key: Vec<MigrationOpV1>,
    },
    MigrateInline {
        at: FieldPathV1,
        ops: Vec<MigrationOpV1>,
    },
}

#[asset(uuid = "f82aecaf-0bdf-526f-af12-5cb69581c83b", build_only)]
pub struct DirectoryImportRulesV1 {
    pub listing: FileQueryV1,
    pub rules: Vec<ImportRuleV1>,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000015")]
pub struct ImportRuleV1 {
    pub id: [u8; 16],
    pub matches: FileQueryV1,
    pub group: GroupingV1,
    pub importer: String,
    pub settings: AuthoredValueV1,
    pub output: String,
}

#[asset(uuid = "b0000000-0000-4000-8000-000000000016")]
#[repr(u8)]
pub enum GroupingV1 {
    PerFile {},
    ByStem {},
}

/// The five real, target-native compiled measurements in UUID order.
pub fn locally_compiled_bootstrap_rows_v1() -> Result<[CompiledTypeRow; 5], String> {
    let mut rows = vec![
        control_row::<PackDefinitionV1>(false)?,
        control_row::<SchemaLineageManifestV1>(false)?,
        control_row::<ImportRecordV1>(false)?,
        control_row::<MigrationV1>(true)?,
        control_row::<DirectoryImportRulesV1>(false)?,
    ];
    rows.sort_by_key(|row| row.type_uuid);
    rows.try_into().map_err(|rows: Vec<_>| {
        format!(
            "expected {BOOTSTRAP_CONTROL_COUNT} rows, got {}",
            rows.len()
        )
    })
}

/// Regenerate the byte-authoritative target-invariant table from exactly the
/// same concrete walks that produce the local compiled measurements.
pub fn generated_bootstrap_control_spec_v1() -> Result<BootstrapControlSpecV1, String> {
    let mut rows = vec![
        control_spec_row::<PackDefinitionV1>(BootstrapControlSymbol::PackDefinition, false)?,
        control_spec_row::<SchemaLineageManifestV1>(
            BootstrapControlSymbol::SchemaLineageManifest,
            false,
        )?,
        control_spec_row::<ImportRecordV1>(BootstrapControlSymbol::ImportRecord, false)?,
        control_spec_row::<MigrationV1>(BootstrapControlSymbol::Migration, true)?,
        control_spec_row::<DirectoryImportRulesV1>(
            BootstrapControlSymbol::DirectoryImportRules,
            false,
        )?,
    ];
    rows.sort_by_key(|row| row.type_uuid);
    Ok(BootstrapControlSpecV1(rows.try_into().map_err(
        |rows: Vec<_>| {
            format!(
                "expected {BOOTSTRAP_CONTROL_COUNT} spec rows, got {}",
                rows.len()
            )
        },
    )?))
}

/// The five concrete DSNL roots, borrowed from the same descriptors as the
/// logical/extras check.  No registry or filesystem participates.
pub fn locally_measured_bootstrap_layouts_v1(
) -> [(TypeUuid, &'static NativeLayoutNode); BOOTSTRAP_CONTROL_COUNT] {
    let mut rows = [
        measured_layout::<PackDefinitionV1>(),
        measured_layout::<SchemaLineageManifestV1>(),
        measured_layout::<ImportRecordV1>(),
        measured_layout::<MigrationV1>(),
        measured_layout::<DirectoryImportRulesV1>(),
    ];
    rows.sort_by_key(|row| row.0);
    rows
}

fn measured_layout<T: AssetType>() -> (TypeUuid, &'static NativeLayoutNode) {
    let descriptor = T::descriptor();
    (descriptor.type_uuid, descriptor.native_layout)
}

fn control_spec_row<T: AssetType + AssetReflect>(
    symbol: BootstrapControlSymbol,
    migration_tags: bool,
) -> Result<BootstrapControlSpecRowV1, String> {
    let compiled = control_row::<T>(migration_tags)?;
    Ok(BootstrapControlSpecRowV1 {
        symbol,
        type_uuid: compiled.type_uuid,
        logical_schema: distill_asset::build::logical_schema_bytes::<T>(),
        logical_hash: compiled.logical_hash,
        registry_extras: compiled.registry_extras,
    })
}

fn control_row<T: AssetType>(migration_tags: bool) -> Result<CompiledTypeRow, String> {
    let measured = T::descriptor().compiled_type;
    let mut extras = measured.registry_extras.rows.clone();
    extras.push(RegistryExtraRow {
        node: SchemaNodeId(0),
        path: Vec::new(),
        fact: RegistryExtraFact::ControlRole(ControlRole::AuthoringOnlyRequired),
    });
    if migration_tags {
        for name in ["from_hash", "target_type_uuid"] {
            extras.push(RegistryExtraRow {
                node: SchemaNodeId(0),
                path: vec![RegistryPathStep::Field(name.to_owned())],
                fact: RegistryExtraFact::Tag,
            });
        }
    }
    let extras = RegistryExtrasV1::canonical(extras)
        .map_err(|error| format!("invalid generated registry extras: {error}"))?;
    CompiledTypeRow::new(
        measured.type_uuid,
        measured.logical_hash,
        measured.native_layout_digest,
        true,
        extras,
    )
    .map_err(|error| format!("invalid generated bootstrap row: {error}"))
}

#[cfg(test)]
mod tests {
    use distill_core::attestation::BOOTSTRAP_CONTROL_SPEC_V1_BYTES;

    use super::*;

    #[test]
    fn generated_rust_logical_walks_match_the_embedded_dsb() {
        let generated = generated_bootstrap_control_spec_v1().unwrap();
        assert_eq!(generated.encode().unwrap(), BOOTSTRAP_CONTROL_SPEC_V1_BYTES);
    }
}
