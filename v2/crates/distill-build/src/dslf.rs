//! Closed DSLF v1 deterministic local-failure grammar (§5/§9).
//!
//! Presentation text is deliberately absent.  Every memoizable producer has
//! one typed arm whose required fields are encoded in a pinned order.

use distill_core::canonical::{domain_digest, CanonicalEncoder, DSLF};
use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_migrate::FieldPath;

use crate::query::RootedPath;
use crate::trace::EntryRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum LocalFailureClass {
    Validator = 1,
    MigrationPlan = 2,
    Processor = 3,
    MigrationFunction = 4,
    OutputBinding = 5,
    Importer = 6,
    ImportIntake = 7,
    ArtifactEncoding = 8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DslfError {
    DuplicateImporterSource(RootedPath),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MigrationPlanFailureV1 {
    MissingPath {
        path: FieldPath,
    },
    UnwrittenDestination {
        path: FieldPath,
    },
    DuplicateDestination {
        path: FieldPath,
    },
    RevisionMismatch {
        path: FieldPath,
        expected_revision: u32,
        observed_revision: u32,
    },
    AmbiguousEdge {
        conflicting_edges: Vec<AssetUuid>,
    },
    Cycle {
        cycle_edges: Vec<AssetUuid>,
    },
    MissingReverseEdge {
        missing_from: LogicalHash,
        missing_to: LogicalHash,
    },
    NonConformingOutput {
        edge: AssetUuid,
        path: FieldPath,
    },
    MapKeyCollision {
        path: FieldPath,
    },
    SetElementCollision {
        path: FieldPath,
    },
    MissingDefault {
        path: FieldPath,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OutputBindingSlotV1 {
    Primary,
    Extra { output_key: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OutputBindingFailureV1 {
    MissingPrimary,
    DuplicatePrimary {
        observed_count: u32,
    },
    MissingExtra {
        output_key: String,
        expected_type: TypeUuid,
    },
    DuplicateExtra {
        output_key: String,
        expected_type: TypeUuid,
        observed_count: u32,
    },
    UndeclaredExtra {
        output_key: String,
        observed_type: TypeUuid,
    },
    TypeMismatch {
        slot: OutputBindingSlotV1,
        expected_type: TypeUuid,
        observed_type: TypeUuid,
    },
    InvalidOutputKey {
        output_key: String,
    },
    DuplicateDebugKey {
        debug_key: String,
    },
    EncodeRejected {
        slot: OutputBindingSlotV1,
        encoded_type: TypeUuid,
        failure: ArtifactEncodingFailureV1,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ImportIntakeFailureV1 {
    DuplicateLocalId {
        local_id: String,
    },
    ReservedLocalId {
        local_id: String,
    },
    PrimaryAlreadyDeclared {
        first_local_id: String,
        attempted_local_id: String,
    },
    PrimaryMissing {
        local_id: String,
    },
    VanishedPriorPrimary {
        bundle: BundleUuid,
        local_id: String,
    },
    RoleViolation {
        local_id: String,
        expected_role: EntryRole,
        observed_role: EntryRole,
    },
    TypeMismatch {
        local_id: String,
        expected_type: TypeUuid,
        observed_type: TypeUuid,
    },
    NonCanonicalValue {
        local_id: String,
        type_uuid: TypeUuid,
        path: FieldPath,
    },
    SettingsInvalid {
        settings_type: TypeUuid,
        path: FieldPath,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReferenceObservationV1 {
    Missing,
    Found { observed_terminal: TypeUuid },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArtifactEncodingFailureV1 {
    SizeLimit {
        limit: u64,
        observed: u64,
    },
    NonCanonicalOrder {
        path: FieldPath,
    },
    ValueSchemaMismatch {
        path: FieldPath,
        expected_type: TypeUuid,
        observed_type: TypeUuid,
    },
    LayoutUnavailable,
    InvalidScalar {
        path: FieldPath,
    },
    InvalidReference {
        path: FieldPath,
        expected_terminal: TypeUuid,
        observed: ReferenceObservationV1,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DslfV1 {
    Validator {
        asset: AssetUuid,
        type_uuid: TypeUuid,
        error_paths: Vec<FieldPath>,
    },
    MigrationPlan {
        type_uuid: TypeUuid,
        from: LogicalHash,
        to: LogicalHash,
        failure: MigrationPlanFailureV1,
    },
    Processor {
        asset: AssetUuid,
        processor_id: String,
        processor_version: u32,
        stage: u16,
        build_error_code: u32,
    },
    MigrationFunction {
        asset: AssetUuid,
        type_uuid: TypeUuid,
        from: LogicalHash,
        to: LogicalHash,
        function_key: String,
        migration_error_code: u32,
    },
    OutputBinding {
        asset: AssetUuid,
        processor_id: String,
        processor_version: u32,
        stage: u16,
        failure: OutputBindingFailureV1,
    },
    Importer {
        importer_id: String,
        importer_error_code: u32,
        sources: Vec<RootedPath>,
    },
    ImportIntake {
        importer_id: String,
        failure: ImportIntakeFailureV1,
    },
    ArtifactEncoding {
        asset: AssetUuid,
        encoded_type: TypeUuid,
        failure: ArtifactEncodingFailureV1,
    },
}

impl DslfV1 {
    pub fn class(&self) -> LocalFailureClass {
        match self {
            Self::Validator { .. } => LocalFailureClass::Validator,
            Self::MigrationPlan { .. } => LocalFailureClass::MigrationPlan,
            Self::Processor { .. } => LocalFailureClass::Processor,
            Self::MigrationFunction { .. } => LocalFailureClass::MigrationFunction,
            Self::OutputBinding { .. } => LocalFailureClass::OutputBinding,
            Self::Importer { .. } => LocalFailureClass::Importer,
            Self::ImportIntake { .. } => LocalFailureClass::ImportIntake,
            Self::ArtifactEncoding { .. } => LocalFailureClass::ArtifactEncoding,
        }
    }

    pub fn digest(&self) -> Result<[u8; 32], DslfError> {
        self.validate()?;
        Ok(domain_digest(DSLF, 1, |encoder| {
            encoder.u16(self.class() as u16);
            self.encode_fields(encoder);
        }))
    }

    fn validate(&self) -> Result<(), DslfError> {
        if let Self::Importer { sources, .. } = self {
            let mut sorted = sources.clone();
            sorted.sort();
            for pair in sorted.windows(2) {
                if pair[0] == pair[1] {
                    return Err(DslfError::DuplicateImporterSource(pair[0].clone()));
                }
            }
        }
        Ok(())
    }

    fn encode_fields(&self, e: &mut CanonicalEncoder) {
        match self {
            Self::Validator {
                asset,
                type_uuid,
                error_paths,
            } => {
                e.raw(&asset.0);
                e.raw(&type_uuid.0);
                let mut paths = error_paths.clone();
                paths.sort_by_key(canonical_path_bytes);
                e.seq(&paths, encode_path);
            }
            Self::MigrationPlan {
                type_uuid,
                from,
                to,
                failure,
            } => {
                e.raw(&type_uuid.0);
                e.raw(&from.0);
                e.raw(&to.0);
                encode_migration_plan_failure(e, failure);
            }
            Self::Processor {
                asset,
                processor_id,
                processor_version,
                stage,
                build_error_code,
            } => {
                e.raw(&asset.0);
                e.str(processor_id);
                e.u32(*processor_version);
                e.u16(*stage);
                e.u32(*build_error_code);
            }
            Self::MigrationFunction {
                asset,
                type_uuid,
                from,
                to,
                function_key,
                migration_error_code,
            } => {
                e.raw(&asset.0);
                e.raw(&type_uuid.0);
                e.raw(&from.0);
                e.raw(&to.0);
                e.str(function_key);
                e.u32(*migration_error_code);
            }
            Self::OutputBinding {
                asset,
                processor_id,
                processor_version,
                stage,
                failure,
            } => {
                e.raw(&asset.0);
                e.str(processor_id);
                e.u32(*processor_version);
                e.u16(*stage);
                encode_output_binding_failure(e, failure);
            }
            Self::Importer {
                importer_id,
                importer_error_code,
                sources,
            } => {
                e.str(importer_id);
                e.u32(*importer_error_code);
                let mut sources = sources.clone();
                sources.sort();
                e.seq(&sources, |e, source| {
                    e.str(&source.root.0);
                    e.str(&source.path);
                });
            }
            Self::ImportIntake {
                importer_id,
                failure,
            } => {
                e.str(importer_id);
                encode_import_intake_failure(e, failure);
            }
            Self::ArtifactEncoding {
                asset,
                encoded_type,
                failure,
            } => {
                e.raw(&asset.0);
                e.raw(&encoded_type.0);
                encode_artifact_encoding_failure(e, failure);
            }
        }
    }
}

fn canonical_path_bytes(path: &FieldPath) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    encode_path(&mut e, path);
    e.into_bytes()
}

fn encode_path(e: &mut CanonicalEncoder, path: &FieldPath) {
    e.seq(&path.0, |e, segment| e.str(segment));
}

fn sorted_unique_assets(entries: &[AssetUuid]) -> Vec<AssetUuid> {
    let mut entries = entries.to_vec();
    entries.sort_unstable();
    entries.dedup();
    entries
}

fn encode_migration_plan_failure(e: &mut CanonicalEncoder, failure: &MigrationPlanFailureV1) {
    match failure {
        MigrationPlanFailureV1::MissingPath { path } => tagged_path(e, 1, path),
        MigrationPlanFailureV1::UnwrittenDestination { path } => tagged_path(e, 2, path),
        MigrationPlanFailureV1::DuplicateDestination { path } => tagged_path(e, 3, path),
        MigrationPlanFailureV1::RevisionMismatch {
            path,
            expected_revision,
            observed_revision,
        } => {
            e.u16(4);
            encode_path(e, path);
            e.u32(*expected_revision);
            e.u32(*observed_revision);
        }
        MigrationPlanFailureV1::AmbiguousEdge { conflicting_edges } => {
            e.u16(5);
            e.seq(&sorted_unique_assets(conflicting_edges), |e, id| {
                e.raw(&id.0)
            });
        }
        MigrationPlanFailureV1::Cycle { cycle_edges } => {
            e.u16(6);
            e.seq(&sorted_unique_assets(cycle_edges), |e, id| e.raw(&id.0));
        }
        MigrationPlanFailureV1::MissingReverseEdge {
            missing_from,
            missing_to,
        } => {
            e.u16(7);
            e.raw(&missing_from.0);
            e.raw(&missing_to.0);
        }
        MigrationPlanFailureV1::NonConformingOutput { edge, path } => {
            e.u16(8);
            e.raw(&edge.0);
            encode_path(e, path);
        }
        MigrationPlanFailureV1::MapKeyCollision { path } => tagged_path(e, 9, path),
        MigrationPlanFailureV1::SetElementCollision { path } => tagged_path(e, 10, path),
        MigrationPlanFailureV1::MissingDefault { path } => tagged_path(e, 11, path),
    }
}

fn tagged_path(e: &mut CanonicalEncoder, tag: u16, path: &FieldPath) {
    e.u16(tag);
    encode_path(e, path);
}

fn encode_output_binding_slot(e: &mut CanonicalEncoder, slot: &OutputBindingSlotV1) {
    match slot {
        OutputBindingSlotV1::Primary => e.u16(1),
        OutputBindingSlotV1::Extra { output_key } => {
            e.u16(2);
            e.str(output_key);
        }
    }
}

fn encode_output_binding_failure(e: &mut CanonicalEncoder, failure: &OutputBindingFailureV1) {
    match failure {
        OutputBindingFailureV1::MissingPrimary => e.u16(1),
        OutputBindingFailureV1::DuplicatePrimary { observed_count } => {
            e.u16(2);
            e.u32(*observed_count);
        }
        OutputBindingFailureV1::MissingExtra {
            output_key,
            expected_type,
        } => {
            e.u16(3);
            e.str(output_key);
            e.raw(&expected_type.0);
        }
        OutputBindingFailureV1::DuplicateExtra {
            output_key,
            expected_type,
            observed_count,
        } => {
            e.u16(4);
            e.str(output_key);
            e.raw(&expected_type.0);
            e.u32(*observed_count);
        }
        OutputBindingFailureV1::UndeclaredExtra {
            output_key,
            observed_type,
        } => {
            e.u16(5);
            e.str(output_key);
            e.raw(&observed_type.0);
        }
        OutputBindingFailureV1::TypeMismatch {
            slot,
            expected_type,
            observed_type,
        } => {
            e.u16(6);
            encode_output_binding_slot(e, slot);
            e.raw(&expected_type.0);
            e.raw(&observed_type.0);
        }
        OutputBindingFailureV1::InvalidOutputKey { output_key } => {
            e.u16(7);
            e.str(output_key);
        }
        OutputBindingFailureV1::DuplicateDebugKey { debug_key } => {
            e.u16(8);
            e.str(debug_key);
        }
        OutputBindingFailureV1::EncodeRejected {
            slot,
            encoded_type,
            failure,
        } => {
            e.u16(9);
            encode_output_binding_slot(e, slot);
            e.raw(&encoded_type.0);
            encode_artifact_encoding_failure(e, failure);
        }
    }
}

fn encode_import_intake_failure(e: &mut CanonicalEncoder, failure: &ImportIntakeFailureV1) {
    match failure {
        ImportIntakeFailureV1::DuplicateLocalId { local_id } => tagged_string(e, 1, local_id),
        ImportIntakeFailureV1::ReservedLocalId { local_id } => tagged_string(e, 2, local_id),
        ImportIntakeFailureV1::PrimaryAlreadyDeclared {
            first_local_id,
            attempted_local_id,
        } => {
            e.u16(3);
            e.str(first_local_id);
            e.str(attempted_local_id);
        }
        ImportIntakeFailureV1::PrimaryMissing { local_id } => tagged_string(e, 4, local_id),
        ImportIntakeFailureV1::VanishedPriorPrimary { bundle, local_id } => {
            e.u16(5);
            e.raw(&bundle.0);
            e.str(local_id);
        }
        ImportIntakeFailureV1::RoleViolation {
            local_id,
            expected_role,
            observed_role,
        } => {
            e.u16(6);
            e.str(local_id);
            e.u8(*expected_role as u8);
            e.u8(*observed_role as u8);
        }
        ImportIntakeFailureV1::TypeMismatch {
            local_id,
            expected_type,
            observed_type,
        } => {
            e.u16(7);
            e.str(local_id);
            e.raw(&expected_type.0);
            e.raw(&observed_type.0);
        }
        ImportIntakeFailureV1::NonCanonicalValue {
            local_id,
            type_uuid,
            path,
        } => {
            e.u16(8);
            e.str(local_id);
            e.raw(&type_uuid.0);
            encode_path(e, path);
        }
        ImportIntakeFailureV1::SettingsInvalid {
            settings_type,
            path,
        } => {
            e.u16(9);
            e.raw(&settings_type.0);
            encode_path(e, path);
        }
    }
}

fn tagged_string(e: &mut CanonicalEncoder, tag: u16, value: &str) {
    e.u16(tag);
    e.str(value);
}

fn encode_reference_observation(e: &mut CanonicalEncoder, observed: &ReferenceObservationV1) {
    match observed {
        ReferenceObservationV1::Missing => e.u16(1),
        ReferenceObservationV1::Found { observed_terminal } => {
            e.u16(2);
            e.raw(&observed_terminal.0);
        }
    }
}

fn encode_artifact_encoding_failure(e: &mut CanonicalEncoder, failure: &ArtifactEncodingFailureV1) {
    match failure {
        ArtifactEncodingFailureV1::SizeLimit { limit, observed } => {
            e.u16(1);
            e.u64(*limit);
            e.u64(*observed);
        }
        ArtifactEncodingFailureV1::NonCanonicalOrder { path } => tagged_path(e, 2, path),
        ArtifactEncodingFailureV1::ValueSchemaMismatch {
            path,
            expected_type,
            observed_type,
        } => {
            e.u16(3);
            encode_path(e, path);
            e.raw(&expected_type.0);
            e.raw(&observed_type.0);
        }
        ArtifactEncodingFailureV1::LayoutUnavailable => e.u16(4),
        ArtifactEncodingFailureV1::InvalidScalar { path } => tagged_path(e, 5, path),
        ArtifactEncodingFailureV1::InvalidReference {
            path,
            expected_terminal,
            observed,
        } => {
            e.u16(6);
            encode_path(e, path);
            e.raw(&expected_terminal.0);
            encode_reference_observation(e, observed);
        }
    }
}
