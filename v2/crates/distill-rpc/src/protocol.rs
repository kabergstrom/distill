use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

pub use distill_core::attestation::{
    CompiledAttestationDigest, CompiledTypeRow, CompiledTypeTable, RegistryExtrasDigest,
    RegistryExtrasV1,
};
pub use distill_core::id::{
    AssetUuid, BundleFileHash, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid,
};
pub use distill_store::state::{
    AssetClaimant, CleanupDisposition, ConfigurationPoison, ConfigurationPoisonCode, DscpV1,
    GlobalBundleReadFailureCode, InputVersion, LineageManifestClaimant, PhysicalPathClaim,
    PhysicalPathFailureCode, PipelineCandidateIdentity, PipelinePoison, PipelinePoisonCode,
    PipelinePoisonOrigin, PlatformPathBytes, ReadableBundleSource, ScanFailureCode, ScanSubject,
    SchemaAcceptanceRequired, SchemaManifestBasis, SchemaRegistryMismatch, SkeletonFailureCode,
    SnapshotStamp, StoreInstanceId, VersionPoison, VersionPoisonCode, VersionPoisonV1,
};
pub use distill_store::RetiredTypeReference;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GameModuleEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetDefinitionHash(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LoadPolicyEntry {
    pub type_uuid: TypeUuid,
    pub build_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadPolicyAttestation {
    pub rows: Vec<LoadPolicyEntry>,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub epoch: GameModuleEpoch,
    pub target: String,
    pub target_definition_hash: TargetDefinitionHash,
    pub compiled_registry: Vec<CompiledTypeRow>,
    pub dsca: CompiledAttestationDigest,
    pub load_policy: Vec<LoadPolicyEntry>,
    pub policy_digest: [u8; 32],
    pub protocol: u32,
}

impl ConnectRequest {
    pub fn canonical(
        epoch: GameModuleEpoch,
        target: impl Into<String>,
        target_definition_hash: TargetDefinitionHash,
        compiled_registry: Vec<CompiledTypeRow>,
        mut load_policy: Vec<LoadPolicyEntry>,
    ) -> Result<Self, crate::AttestationShapeError> {
        load_policy.sort_by_key(|row| row.type_uuid);
        let compiled = CompiledTypeTable::canonical(compiled_registry)?;
        distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1()
            .map_err(|error| crate::AttestationShapeError::BootstrapAuthorityUnavailable(error.0))?
            .validate_boundary_rows(
                &compiled.rows,
                distill_core::attestation::BundleFormatVersion::V1,
            )
            .map_err(crate::AttestationShapeError::Bootstrap)?;
        let policy_digest = crate::compute_policy_digest(&load_policy)?;
        crate::attestation::validate_attestation_shape(
            &compiled.rows,
            compiled.digest,
            &load_policy,
            policy_digest,
        )?;
        Ok(Self {
            epoch,
            target: target.into(),
            target_definition_hash,
            compiled_registry: compiled.rows,
            dsca: compiled.digest,
            load_policy,
            policy_digest,
            protocol: PROTOCOL_VERSION,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReattestRequest {
    pub epoch: GameModuleEpoch,
    pub base_attestation_generation: u64,
    pub successor_attestation_generation: u64,
    pub target_definition_hash: TargetDefinitionHash,
    pub compiled_registry: Vec<CompiledTypeRow>,
    pub dsca: CompiledAttestationDigest,
    pub load_policy: Vec<LoadPolicyEntry>,
    pub policy_digest: [u8; 32],
}

impl From<ConnectRequest> for ReattestRequest {
    fn from(request: ConnectRequest) -> Self {
        Self {
            epoch: request.epoch,
            base_attestation_generation: 0,
            successor_attestation_generation: 1,
            target_definition_hash: request.target_definition_hash,
            compiled_registry: request.compiled_registry,
            dsca: request.dsca,
            load_policy: request.load_policy,
            policy_digest: request.policy_digest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectError {
    ProtocolMismatch {
        expected: u32,
        got: u32,
    },
    UnknownTarget {
        target: String,
    },
    TargetDefinitionMismatch {
        expected: TargetDefinitionHash,
        got: TargetDefinitionHash,
    },
    BootstrapAuthorityMismatch {
        expected: Vec<u8>,
        observed: Vec<u8>,
    },
    AttestationShape(crate::AttestationShapeError),
    CompiledRegistryAggregateMismatch {
        expected: [u8; 32],
        observed: [u8; 32],
    },
    MissingCompiledType {
        type_uuid: TypeUuid,
    },
    LogicalHashMismatch {
        type_uuid: TypeUuid,
        expected: [u8; 32],
        observed: [u8; 32],
    },
    NativeLayoutMismatch {
        type_uuid: TypeUuid,
        expected: [u8; 32],
        observed: [u8; 32],
    },
    CompiledBuildOnlyMismatch {
        type_uuid: TypeUuid,
        expected: bool,
        observed: bool,
    },
    RegistryExtrasMismatch {
        type_uuid: TypeUuid,
        expected: Vec<u8>,
        observed: Vec<u8>,
    },
    MissingLoadPolicy {
        type_uuid: TypeUuid,
    },
    LoadPolicyMismatch {
        type_uuid: TypeUuid,
        expected: bool,
        got: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum AttestationFailureCode {
    MalformedTable = 0,
    DuplicateType = 1,
    MissingType = 2,
    LogicalHashMismatch = 3,
    NativeLayoutMismatch = 4,
    BuildOnlyMismatch = 5,
    RegistryExtrasMismatch = 6,
    CompiledRegistryAggregateMismatch = 7,
    TargetDefinitionMismatch = 8,
    PolicyProjectionMismatch = 9,
    BootstrapAuthorityMismatch = 10,
    MalformedField = 11,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttestationProjection {
    CompiledRegistry,
    Policy,
}

/// Typed carrier for what part of the complete attestation failed. No UUID
/// sentinel is used for aggregate/table failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestationSubject {
    SpecificType {
        type_uuid: TypeUuid,
        projection: AttestationProjection,
    },
    TargetDefinition,
    CompiledRegistryTable,
    CompiledRegistryAggregate,
    PolicyProjection,
    BootstrapAuthority,
    FixedField(AttestationFixedFieldSubject),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttestationFixedFieldSubject {
    TargetDefHash,
    DscaAggregate,
    PolicyDigest,
    CompiledTypeUuid(u32),
    CompiledLogicalHash(u32),
    CompiledNativeLayoutDigest(u32),
    CompiledRegistryExtrasDigest(u32),
    PolicyTypeUuid(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestationFailurePayload {
    None,
    ExpectedObserved {
        expected: Vec<u8>,
        observed: Vec<u8>,
    },
    TableDetail {
        index: u32,
        entry: Vec<u8>,
    },
    MalformedField {
        expected_width: u32,
        observed: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestationFailure {
    code: AttestationFailureCode,
    subject: AttestationSubject,
    payload: AttestationFailurePayload,
    message: String,
}

impl AttestationFailure {
    pub fn code(&self) -> AttestationFailureCode {
        self.code
    }

    pub fn subject(&self) -> &AttestationSubject {
        &self.subject
    }

    pub fn payload(&self) -> &AttestationFailurePayload {
        &self.payload
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn from_wire(
        code: AttestationFailureCode,
        subject: AttestationSubject,
        payload: AttestationFailurePayload,
        message: String,
    ) -> Option<Self> {
        let eo = |width: usize| matches!(&payload, AttestationFailurePayload::ExpectedObserved { expected, observed } if expected.len() == width && observed.len() == width && expected != observed);
        let none = matches!(&payload, AttestationFailurePayload::None);
        let canonical = match (code, &subject) {
            (AttestationFailureCode::MalformedField, AttestationSubject::FixedField(subject)) => {
                let expected_width = match subject {
                    AttestationFixedFieldSubject::CompiledTypeUuid(_)
                    | AttestationFixedFieldSubject::PolicyTypeUuid(_) => 16,
                    AttestationFixedFieldSubject::TargetDefHash
                    | AttestationFixedFieldSubject::DscaAggregate
                    | AttestationFixedFieldSubject::PolicyDigest
                    | AttestationFixedFieldSubject::CompiledLogicalHash(_)
                    | AttestationFixedFieldSubject::CompiledNativeLayoutDigest(_)
                    | AttestationFixedFieldSubject::CompiledRegistryExtrasDigest(_) => 32,
                };
                matches!(&payload, AttestationFailurePayload::MalformedField {
                    expected_width: width,
                    observed,
                } if *width == expected_width && observed.len() != expected_width as usize)
            }
            (AttestationFailureCode::MalformedTable, AttestationSubject::CompiledRegistryTable) => {
                matches!(&payload, AttestationFailurePayload::TableDetail { entry, .. }
                if valid_received_compiled_entry(entry))
            }
            (AttestationFailureCode::MalformedTable, AttestationSubject::PolicyProjection) => {
                matches!(&payload, AttestationFailurePayload::TableDetail { entry, .. }
                if entry.len() == 17 && matches!(entry[16], 0 | 1))
            }
            (
                AttestationFailureCode::DuplicateType,
                AttestationSubject::SpecificType {
                    type_uuid,
                    projection,
                },
            ) => match (&payload, projection) {
                (
                    AttestationFailurePayload::TableDetail { entry, .. },
                    AttestationProjection::CompiledRegistry,
                ) => received_compiled_entry_type(entry) == Some(*type_uuid),
                (
                    AttestationFailurePayload::TableDetail { entry, .. },
                    AttestationProjection::Policy,
                ) => entry.len() == 17 && entry[..16] == type_uuid.0 && entry[16] <= 1,
                _ => false,
            },
            (AttestationFailureCode::MissingType, AttestationSubject::SpecificType { .. }) => none,
            (
                AttestationFailureCode::LogicalHashMismatch,
                AttestationSubject::SpecificType {
                    projection: AttestationProjection::CompiledRegistry,
                    ..
                },
            )
            | (
                AttestationFailureCode::NativeLayoutMismatch,
                AttestationSubject::SpecificType {
                    projection: AttestationProjection::CompiledRegistry,
                    ..
                },
            ) => eo(32),
            (
                AttestationFailureCode::BuildOnlyMismatch,
                AttestationSubject::SpecificType { .. },
            ) => {
                matches!(&payload, AttestationFailurePayload::ExpectedObserved { expected, observed }
                if matches!(expected.as_slice(), [0] | [1])
                    && matches!(observed.as_slice(), [0] | [1])
                    && expected != observed)
            }
            (
                AttestationFailureCode::RegistryExtrasMismatch,
                AttestationSubject::SpecificType {
                    projection: AttestationProjection::CompiledRegistry,
                    ..
                },
            ) => {
                matches!(&payload, AttestationFailurePayload::ExpectedObserved { expected, observed }
                    if expected != observed
                        && RegistryExtrasV1::decode(expected).is_ok()
                        && RegistryExtrasV1::decode(observed).is_ok())
            }
            (
                AttestationFailureCode::CompiledRegistryAggregateMismatch,
                AttestationSubject::CompiledRegistryAggregate,
            )
            | (
                AttestationFailureCode::TargetDefinitionMismatch,
                AttestationSubject::TargetDefinition,
            )
            | (
                AttestationFailureCode::PolicyProjectionMismatch,
                AttestationSubject::PolicyProjection,
            ) => eo(32),
            (
                AttestationFailureCode::BootstrapAuthorityMismatch,
                AttestationSubject::BootstrapAuthority,
            ) => {
                matches!(&payload, AttestationFailurePayload::ExpectedObserved { expected, observed }
                    if expected != observed
                        && valid_bootstrap_projection(expected)
                        && valid_bootstrap_projection(observed))
            }
            _ => false,
        };
        canonical.then_some(Self {
            code,
            subject,
            payload,
            message,
        })
    }

    pub(crate) fn malformed_field(
        subject: AttestationFixedFieldSubject,
        expected_width: u32,
        observed: Vec<u8>,
        message: String,
    ) -> Self {
        Self {
            code: AttestationFailureCode::MalformedField,
            subject: AttestationSubject::FixedField(subject),
            payload: AttestationFailurePayload::MalformedField {
                expected_width,
                observed,
            },
            message,
        }
    }

    pub(crate) fn malformed_table(
        projection: AttestationProjection,
        index: u32,
        entry: Vec<u8>,
        message: String,
    ) -> Self {
        let subject = match projection {
            AttestationProjection::CompiledRegistry => AttestationSubject::CompiledRegistryTable,
            AttestationProjection::Policy => AttestationSubject::PolicyProjection,
        };
        Self {
            code: AttestationFailureCode::MalformedTable,
            subject,
            payload: AttestationFailurePayload::TableDetail { index, entry },
            message,
        }
    }

    pub(crate) fn duplicate_type(
        type_uuid: TypeUuid,
        projection: AttestationProjection,
        index: u32,
        entry: Vec<u8>,
        message: String,
    ) -> Self {
        Self {
            code: AttestationFailureCode::DuplicateType,
            subject: AttestationSubject::SpecificType {
                type_uuid,
                projection,
            },
            payload: AttestationFailurePayload::TableDetail { index, entry },
            message,
        }
    }
}

fn valid_bootstrap_projection(bytes: &[u8]) -> bool {
    let Some(count_bytes) = bytes.get(..4) else {
        return false;
    };
    let count = u32::from_le_bytes(count_bytes.try_into().expect("four bytes")) as usize;
    if count != distill_core::attestation::BOOTSTRAP_CONTROL_TYPE_UUIDS.len() {
        return false;
    }
    let mut cursor = 4usize;
    let mut previous = None;
    for _ in 0..count {
        let Some(len_bytes) = bytes.get(cursor..cursor + 4) else {
            return false;
        };
        cursor += 4;
        let len = u32::from_le_bytes(len_bytes.try_into().expect("four bytes")) as usize;
        let Some(end) = cursor.checked_add(len) else {
            return false;
        };
        let Some(encoded) = bytes.get(cursor..end) else {
            return false;
        };
        let Ok(row) = CompiledTypeRow::decode(encoded) else {
            return false;
        };
        if !distill_core::attestation::is_bootstrap_control_type(row.type_uuid) {
            return false;
        }
        if previous.is_some_and(|uuid| uuid >= row.type_uuid) {
            return false;
        }
        previous = Some(row.type_uuid);
        cursor = end;
    }
    cursor == bytes.len()
}

fn valid_received_compiled_entry(bytes: &[u8]) -> bool {
    received_compiled_entry_type(bytes).is_some()
}

/// Validate the raw Cap'n Proto row image used in malformed/duplicate-table
/// failures. This intentionally does not decode the registry extras: invalid
/// extras are the reason a structurally well-formed row can be reported as a
/// malformed table entry.
fn received_compiled_entry_type(bytes: &[u8]) -> Option<TypeUuid> {
    let mut cursor = 0usize;
    let mut type_uuid = None;
    for (field, expected_width) in [Some(16usize), Some(32), Some(32), Some(32), None]
        .into_iter()
        .enumerate()
    {
        let width = u32::from_le_bytes(bytes.get(cursor..cursor.checked_add(4)?)?.try_into().ok()?)
            as usize;
        cursor = cursor.checked_add(4)?;
        if expected_width.is_some_and(|expected| expected != width) {
            return None;
        }
        let end = cursor.checked_add(width)?;
        let value = bytes.get(cursor..end)?;
        if field == 0 {
            type_uuid = Some(TypeUuid(value.try_into().ok()?));
        }
        cursor = end;
    }
    if bytes.get(cursor).copied().is_some_and(|value| value <= 1)
        && cursor.checked_add(1) == Some(bytes.len())
    {
        type_uuid
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReattestSuccess {
    /// The only legal source for the successor RPC basis generation.
    pub installed_attestation_generation: u64,
    pub daemon_compiled_projection: CompiledAttestationDigest,
    pub load_policy: Arc<LoadPolicyAttestation>,
    pub policy_generation: u64,
}

pub const STALE_ATTESTATION_BASE_CODE: u16 = 0x0100;
pub const ATTESTATION_GENERATION_OVERFLOW_CODE: u16 = 0x0101;
pub const INVALID_ATTESTATION_SUCCESSOR_CODE: u16 = 0x0102;
pub const EPOCH_NOT_SUCCESSOR_CODE: u16 = 0x0103;

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ConnectError {}

impl ConnectError {
    /// Stable connect/reattest wire classification shared by both result
    /// unions. Presentation text is deliberately not the type carrier.
    pub fn attestation_failure(&self) -> Option<AttestationFailure> {
        use distill_core::attestation::AttestationError;

        let (code, subject, payload) = match self {
            Self::ProtocolMismatch { .. } | Self::UnknownTarget { .. } => return None,
            Self::TargetDefinitionMismatch { expected, got } => (
                AttestationFailureCode::TargetDefinitionMismatch,
                AttestationSubject::TargetDefinition,
                AttestationFailurePayload::ExpectedObserved {
                    expected: expected.0.to_vec(),
                    observed: got.0.to_vec(),
                },
            ),
            Self::CompiledRegistryAggregateMismatch { expected, observed } => (
                AttestationFailureCode::CompiledRegistryAggregateMismatch,
                AttestationSubject::CompiledRegistryAggregate,
                AttestationFailurePayload::ExpectedObserved {
                    expected: expected.to_vec(),
                    observed: observed.to_vec(),
                },
            ),
            Self::BootstrapAuthorityMismatch { expected, observed } => (
                AttestationFailureCode::BootstrapAuthorityMismatch,
                AttestationSubject::BootstrapAuthority,
                AttestationFailurePayload::ExpectedObserved {
                    expected: expected.clone(),
                    observed: observed.clone(),
                },
            ),
            Self::AttestationShape(crate::AttestationShapeError::Compiled(
                AttestationError::CompiledDigestMismatch,
            )) => (
                AttestationFailureCode::CompiledRegistryAggregateMismatch,
                AttestationSubject::CompiledRegistryAggregate,
                AttestationFailurePayload::ExpectedObserved {
                    expected: vec![0; 32],
                    observed: vec![0; 32],
                },
            ),
            Self::AttestationShape(crate::AttestationShapeError::Compiled(
                AttestationError::DuplicateType(type_uuid),
            )) => (
                AttestationFailureCode::DuplicateType,
                AttestationSubject::SpecificType {
                    type_uuid: *type_uuid,
                    projection: AttestationProjection::CompiledRegistry,
                },
                AttestationFailurePayload::TableDetail {
                    index: 0,
                    entry: type_uuid.0.to_vec(),
                },
            ),
            Self::AttestationShape(crate::AttestationShapeError::Compiled(_)) => (
                AttestationFailureCode::MalformedTable,
                AttestationSubject::CompiledRegistryTable,
                AttestationFailurePayload::TableDetail {
                    index: 0,
                    entry: Vec::new(),
                },
            ),
            Self::AttestationShape(crate::AttestationShapeError::PolicyDigestMismatch {
                expected,
                got,
            }) => (
                AttestationFailureCode::PolicyProjectionMismatch,
                AttestationSubject::PolicyProjection,
                AttestationFailurePayload::ExpectedObserved {
                    expected: expected.to_vec(),
                    observed: got.to_vec(),
                },
            ),
            Self::AttestationShape(_) => (
                AttestationFailureCode::MalformedTable,
                AttestationSubject::PolicyProjection,
                AttestationFailurePayload::TableDetail {
                    index: 0,
                    entry: Vec::new(),
                },
            ),
            Self::MissingCompiledType { type_uuid } => (
                AttestationFailureCode::MissingType,
                AttestationSubject::SpecificType {
                    type_uuid: *type_uuid,
                    projection: AttestationProjection::CompiledRegistry,
                },
                AttestationFailurePayload::None,
            ),
            Self::LogicalHashMismatch {
                type_uuid,
                expected,
                observed,
            } => (
                AttestationFailureCode::LogicalHashMismatch,
                AttestationSubject::SpecificType {
                    type_uuid: *type_uuid,
                    projection: AttestationProjection::CompiledRegistry,
                },
                AttestationFailurePayload::ExpectedObserved {
                    expected: expected.to_vec(),
                    observed: observed.to_vec(),
                },
            ),
            Self::NativeLayoutMismatch {
                type_uuid,
                expected,
                observed,
            } => (
                AttestationFailureCode::NativeLayoutMismatch,
                AttestationSubject::SpecificType {
                    type_uuid: *type_uuid,
                    projection: AttestationProjection::CompiledRegistry,
                },
                AttestationFailurePayload::ExpectedObserved {
                    expected: expected.to_vec(),
                    observed: observed.to_vec(),
                },
            ),
            Self::CompiledBuildOnlyMismatch {
                type_uuid,
                expected,
                observed,
            } => (
                AttestationFailureCode::BuildOnlyMismatch,
                AttestationSubject::SpecificType {
                    type_uuid: *type_uuid,
                    projection: AttestationProjection::CompiledRegistry,
                },
                AttestationFailurePayload::ExpectedObserved {
                    expected: vec![u8::from(*expected)],
                    observed: vec![u8::from(*observed)],
                },
            ),
            Self::RegistryExtrasMismatch {
                type_uuid,
                expected,
                observed,
            } => (
                AttestationFailureCode::RegistryExtrasMismatch,
                AttestationSubject::SpecificType {
                    type_uuid: *type_uuid,
                    projection: AttestationProjection::CompiledRegistry,
                },
                AttestationFailurePayload::ExpectedObserved {
                    expected: expected.clone(),
                    observed: observed.clone(),
                },
            ),
            Self::MissingLoadPolicy { type_uuid } => (
                AttestationFailureCode::MissingType,
                AttestationSubject::SpecificType {
                    type_uuid: *type_uuid,
                    projection: AttestationProjection::Policy,
                },
                AttestationFailurePayload::None,
            ),
            Self::LoadPolicyMismatch {
                type_uuid,
                expected,
                got,
            } => (
                AttestationFailureCode::BuildOnlyMismatch,
                AttestationSubject::SpecificType {
                    type_uuid: *type_uuid,
                    projection: AttestationProjection::Policy,
                },
                AttestationFailurePayload::ExpectedObserved {
                    expected: vec![u8::from(*expected)],
                    observed: vec![u8::from(*got)],
                },
            ),
        };
        Some(AttestationFailure {
            code,
            subject,
            payload,
            message: format!("{self:?}"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectOutcome {
    Connected(Connected),
    ConfigurationPoisoned(ConfigurationPoison),
    PipelineUnavailable(PipelineUnavailableDiagnostic),
    Rejected(ConnectError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineUnavailableDiagnostic {
    PipelinePoison(PipelinePoison),
    SchemaAcceptanceRequired(SchemaAcceptanceRequired),
    RetiredTypeReferenced(RetiredTypeReferenced),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataConnectOutcome {
    Connected(MetadataConnected),
    ProtocolMismatch { expected: u32, observed: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OccupiedLineageDestinationKind {
    CanonicalBundle,
    Opaque,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairDestination {
    Absent,
    Occupied {
        file_hash: BundleFileHash,
        kind: OccupiedLineageDestinationKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairState {
    Missing {
        configured_root: String,
        configured_path: String,
        destination: LineageRepairDestination,
    },
    Duplicate {
        claimants: Vec<LineageManifestClaimant>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageRepairInspection {
    pub instance: StoreInstanceId,
    pub stamp: SnapshotStamp,
    pub state: LineageRepairState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairUnavailable {
    ConfigurationReady,
    OtherConfigurationPoison(ConfigurationPoison),
}

#[derive(Debug, Clone)]
pub struct LineageRepairConnected {
    pub repair: crate::LineageRepair,
    pub instance: StoreInstanceId,
    pub protocol_epoch: u32,
}

#[derive(Debug, Clone)]
pub enum LineageRepairConnectOutcome {
    Connected(LineageRepairConnected),
    Unavailable(LineageRepairUnavailable),
    ProtocolMismatch { expected: u32, observed: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageRepairInvalidCode {
    WrongBasisState,
    NonCanonicalBundle,
    MissingManifestEntry,
    NotAuthoringOnly,
    BootstrapTypePresent,
    InvalidLineage,
    SurvivorNotClaimant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageRepairInvalid {
    pub code: LineageRepairInvalidCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageRepairStaleCode {
    StampChanged,
    StateChanged,
    DestinationAppeared,
    ClaimantChanged,
    PreimageChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageRepairStale {
    pub code: LineageRepairStaleCode,
    pub observed_stamp: SnapshotStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineageRepairCommitted {
    pub stamp: SnapshotStamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairInspectOutcome {
    Success(LineageRepairInspection),
    Unavailable(LineageRepairUnavailable),
    ReconnectRequired { reason: MetadataReconnectReason },
    Failure(RpcFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairMutationOutcome {
    Success(LineageRepairCommitted),
    StaleBasis(LineageRepairStale),
    Invalid(LineageRepairInvalid),
    Unavailable(LineageRepairUnavailable),
    ReconnectRequired { reason: MetadataReconnectReason },
    Failure(RpcFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageRepairBackendError {
    /// Descriptor-relative destination/claimant/preimage drift observed by the
    /// durable backend after the RPC coordinator's in-memory basis CAS.
    Stale(LineageRepairStale),
    Invalid(LineageRepairInvalid),
    Failure(RpcFailure),
}

impl MetadataConnectOutcome {
    pub fn connected(self) -> Option<MetadataConnected> {
        match self {
            Self::Connected(connected) => Some(connected),
            Self::ProtocolMismatch { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetadataConnected {
    pub hub: crate::MetadataHub,
    pub instance: StoreInstanceId,
    pub protocol_epoch: u32,
}

impl PartialEq for MetadataConnected {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
            && self.protocol_epoch == other.protocol_epoch
            && self.hub.connection_id() == other.hub.connection_id()
    }
}

impl Eq for MetadataConnected {}

#[derive(Debug, Clone)]
pub struct Connected {
    pub hub: crate::Hub,
    pub instance: StoreInstanceId,
    pub policy_generation: u64,
    pub target_generation: u64,
    pub attestation_generation: u64,
    /// Exact server-recomputed accepted-set projection. The connect success
    /// is the sole initial source of this policy basis.
    pub load_policy: Arc<LoadPolicyAttestation>,
    /// DSCA recomputed by the daemon over its current rows projected onto
    /// the client's accepted UUID set. It is never copied from the request.
    pub daemon_compiled_projection: CompiledAttestationDigest,
}

impl PartialEq for Connected {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
            && self.policy_generation == other.policy_generation
            && self.target_generation == other.target_generation
            && self.attestation_generation == other.attestation_generation
            && self.load_policy == other.load_policy
            && self.daemon_compiled_projection == other.daemon_compiled_projection
            && self.hub.connection_id() == other.hub.connection_id()
    }
}

impl Eq for Connected {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationStatus {
    Ready,
    Poisoned(ConfigurationPoison),
}

/// Closed, poison-safe pipeline diagnostic using shared typed §13 records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineDiagnostic {
    Ready,
    Poisoned(PipelinePoison),
    SchemaAcceptanceRequired(SchemaAcceptanceRequired),
    RetiredTypeReferenced(RetiredTypeReferenced),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredTypeReferenced {
    pub manifest_hash: BundleFileHash,
    pub basis: SnapshotStamp,
    pub type_uuid: TypeUuid,
    pub references: Vec<RetiredTypeReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataDiagnostics {
    pub stamp: SnapshotStamp,
    pub configuration: ConfigurationStatus,
    pub pipeline: PipelineDiagnostic,
    pub version_poison: Option<VersionPoison>,
}

/// R22/H4 terminology alias. The Cap'n Proto declaration calls the wire
/// struct `ConfigurationStatus`; both names denote the same snapshot-pinned
/// Ready/Poisoned state.
pub type ConfigurationState = ConfigurationStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectReason {
    TargetDefinitionChanged,
    LoadPolicyChanged,
    CompiledAttestationChanged,
    StoreInstanceChanged,
    ProtocolEpochChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataReconnectReason {
    StoreInstanceChanged,
    ProtocolEpochChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcFailure {
    LeaseExpired,
    InvalidCursor {
        since: InputVersion,
        current: InputVersion,
    },
    ArtifactNotFound {
        hash: ContentHash,
    },
    AssetNotFound {
        uuid: AssetUuid,
    },
    ForeignSnapshot,
    ClientEpochChanged {
        snapshot: GameModuleEpoch,
        current: GameModuleEpoch,
    },
    InvalidPath {
        path: String,
    },
    InvalidQuery {
        detail: String,
    },
    StaleInputVersion {
        expected: InputVersion,
        got: InputVersion,
    },
    InvalidAuthoringRequest {
        detail: String,
    },
    AuthoringBackendUnavailable {
        operation: String,
    },
    WireTreeNotFound {
        hash: LayoutHash,
    },
    EpochNotSuccessor {
        previous: GameModuleEpoch,
        proposed: GameModuleEpoch,
    },
    StaleAttestationBase {
        expected: u64,
        got: u64,
    },
    InvalidAttestationSuccessor {
        base: u64,
        successor: u64,
    },
    AttestationGenerationOverflow {
        base: u64,
    },
    PolicyGenerationOverflow {
        base: u64,
    },
    Attestation(ConnectError),
}

/// Exact common result grammar for the unbound metadata capability family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCall<T> {
    Success(T),
    ReconnectRequired { reason: MetadataReconnectReason },
    LeaseFailure,
    Error(RpcFailure),
}

impl<T> MetadataCall<T> {
    pub fn success(self) -> Option<T> {
        match self {
            Self::Success(value) => Some(value),
            _ => None,
        }
    }

    pub fn map_success<U>(self, map: impl FnOnce(T) -> U) -> MetadataCall<U> {
        match self {
            Self::Success(value) => MetadataCall::Success(map(value)),
            Self::ReconnectRequired { reason } => MetadataCall::ReconnectRequired { reason },
            Self::LeaseFailure => MetadataCall::LeaseFailure,
            Self::Error(error) => MetadataCall::Error(error),
        }
    }
}

/// Namespace-facing metadata result grammar; version poison is a value arm,
/// never a stringly RPC error or a bootstrap failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataNamespaceCall<T> {
    Success(T),
    ReconnectRequired { reason: MetadataReconnectReason },
    LeaseFailure,
    Error(RpcFailure),
    VersionPoisoned(VersionPoison),
}

impl<T> MetadataNamespaceCall<T> {
    pub fn success(self) -> Option<T> {
        match self {
            Self::Success(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcResult<T> {
    Success(T),
    ReconnectRequired { reason: ReconnectReason },
    AttestationExpansionRequired(AttestationExpansionRequired),
    ConfigurationPoisoned(ConfigurationPoison),
    VersionPoisoned(VersionPoison),
    Failure(RpcFailure),
}

impl<T> RpcResult<T> {
    pub fn success(self) -> Option<T> {
        match self {
            Self::Success(value) => Some(value),
            _ => None,
        }
    }

    pub fn map_success<U>(self, map: impl FnOnce(T) -> U) -> RpcResult<U> {
        match self {
            Self::Success(value) => RpcResult::Success(map(value)),
            Self::ReconnectRequired { reason } => RpcResult::ReconnectRequired { reason },
            Self::AttestationExpansionRequired(expansion) => {
                RpcResult::AttestationExpansionRequired(expansion)
            }
            Self::ConfigurationPoisoned(poison) => RpcResult::ConfigurationPoisoned(poison),
            Self::VersionPoisoned(poison) => RpcResult::VersionPoisoned(poison),
            Self::Failure(error) => RpcResult::Failure(error),
        }
    }
}

/// Typed, no-data challenge emitted when the actual served closure requires
/// runtime types outside the Hub's accepted attestation set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestationExpansionRequired {
    pub snapshot: SnapshotStamp,
    pub closure_identity: [u8; 32],
    /// Exact sorted, unique, nonempty `B(served) - A`.
    pub required: Vec<TypeUuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcBasis {
    pub snapshot: SnapshotStamp,
    pub load_policy: Arc<LoadPolicyAttestation>,
    pub policy_generation: u64,
    pub target_generation: u64,
    pub attestation_generation: u64,
    pub daemon_compiled_projection: CompiledAttestationDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataBasis {
    pub snapshot: SnapshotStamp,
    pub protocol_epoch: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEvent<T> {
    pub basis: MetadataBasis,
    pub value: T,
}

/// The exact selector subset available without a pipeline/target authority.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PureMetadataQuery {
    pub uuid: Option<AssetUuid>,
    pub bundle: Option<BundleUuid>,
    pub normalized_path_prefix: Option<String>,
    pub authored_type: Option<TypeUuid>,
    pub role: Option<AuthoringEntryRole>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEntry {
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub schema_hash: LogicalHash,
    pub role: AuthoringEntryRole,
    pub tags: BTreeMap<String, Option<String>>,
}

/// Target/pipeline-independent entry projection exposed by `Root.metadata`.
/// It deliberately omits terminal type and extracted tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PureMetadataEntry {
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub authored_type: TypeUuid,
    pub schema_hash: LogicalHash,
    pub role: AuthoringEntryRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalEvent<T> {
    pub basis: RpcBasis,
    pub value: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftedInput {
    File(String),
    Asset(AssetUuid),
    Query(String),
    Dylib,
    Tool(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveResult {
    Built {
        content_hash: ContentHash,
    },
    Drifted {
        input: DriftedInput,
        current: SnapshotStamp,
    },
    Failed {
        error: String,
    },
    Missing,
    Deleted {
        at: SnapshotStamp,
    },
    /// The UUID exists, but its authored row is tooling-only and must never
    /// enter the runtime build/load path.
    RoleIneligible {
        observed: AuthoringEntryRole,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredResolve {
    Built { content_hash: ContentHash },
    Drifted { input: DriftedInput },
    Failed { error: String },
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathResolveResult {
    Resolved(AssetUuid),
    Missing,
    Failed(PathResolveFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathResolveFailure {
    Ambiguous { candidates: Vec<AssetUuid> },
}

/// Closed query subset exposed by the pinned authoring RPC capability.
/// `None` is the explicitly tooling-only whole-tree enumeration; ordinary
/// runtime query surfaces do not accept that form.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AssetQuery {
    pub uuid: Option<AssetUuid>,
    pub bundle_path: Option<String>,
    pub local_id: Option<String>,
    pub bundle_uuid: Option<BundleUuid>,
    pub authored_type: Option<TypeUuid>,
    pub terminal_type: Option<TypeUuid>,
    pub tag: Option<TagSelector>,
    pub path_prefix: Option<String>,
    pub path_glob: Option<String>,
    pub authoring_only: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagSelector {
    pub tag: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoringEntryRole {
    Runtime,
    AuthoringOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringValue {
    /// Canonical authored-value bytes. This branded carrier is deliberately
    /// not a `ContentHash` and cannot be fetched as an artifact.
    pub canonical_value: Arc<[u8]>,
    pub blobs: Vec<Arc<[u8]>>,
}

/// Closed authoring mutation grammar accepted by `Hub.write`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum AuthoringOp {
    Set(AuthoringEntry),
    Remove { uuid: AssetUuid },
}

/// Explicit importer request. Settings retain their canonical authored-value
/// bytes; the RPC layer never sniffs sources or reinterprets this payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportRequest {
    pub importer: String,
    pub sources: Vec<String>,
    pub dest: String,
    pub settings: AuthoringValue,
    pub watch: bool,
    pub root: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LongRunningOp {
    RenameWithFixups(Arc<[u8]>),
    DiskMigration(Arc<[u8]>),
    Doctor(Arc<[u8]>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoringProgressState {
    Started,
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl AuthoringProgressState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringProgressEvent {
    pub sequence: u64,
    pub state: AuthoringProgressState,
    pub payload: Arc<[u8]>,
}

/// Pure preparation result returned by an injected authoring backend. The RPC
/// server validates and commits this change set atomically against the caller's
/// input-version precondition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedImportCommit {
    pub bundle: BundleUuid,
    pub commit: Commit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedOperationCommit {
    pub commit: Commit,
    pub progress: Vec<AuthoringProgressEvent>,
}

/// Daemon integration seam for workflows that require importer, filesystem,
/// migration, or doctor services. Implementations prepare a side-effect-free
/// commit; publication remains an atomic RPC-server CAS step.
pub trait AuthoringBackend: Send + Sync {
    /// Execute an ordinary authoring batch against `base` and return the
    /// rescan-proven in-memory projection when the backend owns durable
    /// publication. `Ok(None)` retains the in-memory-only implementation used
    /// by embedders and tests that have no filesystem authority.
    ///
    /// The RPC coordinator invokes this while holding its publication lock;
    /// production implementations must compare their durable store version
    /// with `base`, publish, rescan, and advance that store exactly once before
    /// returning. They must not call back into the [`crate::Server`].
    fn prepare_write(
        &self,
        _base: InputVersion,
        _operations: &[AuthoringOp],
    ) -> Result<Option<Commit>, RpcFailure> {
        Ok(None)
    }

    fn prepare_import(
        &self,
        base: InputVersion,
        request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure>;

    fn prepare_reimport(
        &self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure>;

    fn prepare_operation(
        &self,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure>;

    /// Execute the destination-aware durable repair and return its
    /// rescan-proven publication commit. The RPC coordinator invokes this
    /// only after its second exact inspection CAS and holds publication
    /// serialization until the returned commit is installed. Implementations
    /// must not call back into this `Server` while that coordinator lock is
    /// held.
    fn prepare_create_missing_lineage(
        &self,
        _basis: &LineageRepairInspection,
        _canonical_manifest_bundle: &[u8],
    ) -> Result<Commit, LineageRepairBackendError> {
        Err(LineageRepairBackendError::Failure(
            RpcFailure::AuthoringBackendUnavailable {
                operation: "lineageRepair.createMissing".to_owned(),
            },
        ))
    }

    fn prepare_resolve_duplicate_lineage(
        &self,
        _basis: &LineageRepairInspection,
        _survivor: &LineageManifestClaimant,
    ) -> Result<Commit, LineageRepairBackendError> {
        Err(LineageRepairBackendError::Failure(
            RpcFailure::AuthoringBackendUnavailable {
                operation: "lineageRepair.resolveDuplicate".to_owned(),
            },
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetReferenceQuery {
    Uuid(AssetUuid),
    Path {
        normalized_path: String,
        local_id: Option<String>,
    },
    SameBundleLocalId(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringEntry {
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub type_uuid: TypeUuid,
    pub terminal_type: TypeUuid,
    pub schema_hash: LogicalHash,
    /// Canonical §5 logical-schema snapshot bytes authenticated by
    /// `schema_hash` before authored-value decoding.
    pub logical_schema: Arc<[u8]>,
    pub role: AuthoringEntryRole,
    pub tags: BTreeMap<String, Option<String>>,
    pub value: AuthoringValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoringInspection {
    pub stamp: SnapshotStamp,
    pub uuid: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub type_uuid: TypeUuid,
    pub schema_hash: LogicalHash,
    pub logical_schema: Arc<[u8]>,
    pub role: AuthoringEntryRole,
    pub value: AuthoringValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Public wire-domain shape mirrors the typed result union.
pub enum AuthoringInspectResult {
    Inspection(AuthoringInspection),
    Missing,
    RoleIneligible { observed: AuthoringEntryRole },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPayload {
    pub structural: Arc<[u8]>,
    pub blobs: Vec<Arc<[u8]>>,
    /// Verified artifact-header encoded type.
    pub encoded_type: TypeUuid,
    /// Verified artifact-header terminal type.
    pub terminal_type: TypeUuid,
    /// Complete, verified closure rows used to derive `B(served)` and DSAE.
    /// Rows and each row's load edges are strict canonical sets.
    pub closure_rows: Vec<ServedClosureRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServedLoadEdge {
    pub asset: AssetUuid,
    pub expected_terminal: TypeUuid,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServedClosureRow {
    pub asset: AssetUuid,
    pub content_hash: ContentHash,
    pub authored_type: TypeUuid,
    pub encoded_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub load_edges: Vec<ServedLoadEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactChunkKind {
    Structural,
    Blob { index: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactChunk {
    pub kind: ArtifactChunkKind,
    pub offset: u64,
    pub bytes: Arc<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkStream {
    pub(crate) chunks: std::collections::VecDeque<ArtifactChunk>,
}

pub(crate) trait ProgressCompletion: Send + Sync {
    fn complete(&self) -> Result<(), String>;
    fn cancel(&self) -> bool;
}

pub struct ProgressStream {
    pub(crate) events: std::collections::VecDeque<AuthoringProgressEvent>,
    pub(crate) next_sequence: u64,
    pub(crate) terminal_seen: bool,
    pub(crate) completion: Arc<dyn ProgressCompletion>,
}

impl std::fmt::Debug for ProgressStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProgressStream")
            .field("events", &self.events)
            .field("next_sequence", &self.next_sequence)
            .field("terminal_seen", &self.terminal_seen)
            .finish_non_exhaustive()
    }
}

impl ProgressStream {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn cancel(&mut self) -> bool {
        if self.terminal_seen || !self.completion.cancel() {
            return false;
        }
        self.events.clear();
        self.events.push_back(AuthoringProgressEvent {
            sequence: self.next_sequence,
            state: AuthoringProgressState::Cancelled,
            payload: Arc::from([]),
        });
        true
    }
}

impl Iterator for ProgressStream {
    type Item = AuthoringProgressEvent;

    fn next(&mut self) -> Option<Self::Item> {
        let mut event = self.events.pop_front()?;
        if event.state == AuthoringProgressState::Completed {
            if let Err(error) = self.completion.complete() {
                event.state = AuthoringProgressState::Failed;
                event.payload = Arc::from(error.into_bytes());
            }
        } else if matches!(
            event.state,
            AuthoringProgressState::Cancelled | AuthoringProgressState::Failed
        ) {
            self.completion.cancel();
        }
        self.next_sequence = event.sequence.saturating_add(1);
        self.terminal_seen = event.state.is_terminal();
        Some(event)
    }
}

impl ChunkStream {
    pub fn next_chunk(&mut self) -> Option<ArtifactChunk> {
        self.chunks.pop_front()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetDeltaState {
    Changed,
    Deleted,
    Restored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub basis: RpcBasis,
    pub assets: Vec<(AssetUuid, AssetDeltaState)>,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetEvent {
    Created {
        uuid: AssetUuid,
        bundle: BundleUuid,
        local_id: String,
        type_uuid: TypeUuid,
    },
    Modified {
        uuid: AssetUuid,
        bundle: BundleUuid,
        local_id: String,
    },
    Deleted {
        uuid: AssetUuid,
        bundle: BundleUuid,
        local_id: String,
    },
    Invalidated {
        uuid: AssetUuid,
        version: InputVersion,
    },
    Built {
        uuid: AssetUuid,
        artifact: ContentHash,
        version: InputVersion,
    },
    MigrationPending {
        uuid: AssetUuid,
        from_hash: LogicalHash,
        to_hash: LogicalHash,
    },
    MigrationApplied {
        uuid: AssetUuid,
    },
    Error {
        uuid: Option<AssetUuid>,
        message: String,
    },
    RestartRequired {
        keys: Vec<String>,
    },
    ReconnectRequired {
        reason: ReconnectReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    InitialDelta {
        basis: RpcBasis,
        since: InputVersion,
        installed: InputVersion,
        deltas: Vec<Delta>,
    },
    Delta(Delta),
    ResyncRequired {
        basis: RpcBasis,
        oldest_available: InputVersion,
    },
    Asset {
        basis: RpcBasis,
        event: AssetEvent,
    },
}

impl StreamEvent {
    /// Every message on the stream is basis-tagged, including empty initial
    /// deltas, resync markers, restart prompts, and reconnect prompts.
    pub fn basis(&self) -> &RpcBasis {
        match self {
            Self::InitialDelta { basis, .. }
            | Self::ResyncRequired { basis, .. }
            | Self::Asset { basis, .. } => basis,
            Self::Delta(delta) => &delta.basis,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionInstall {
    pub deltas: crate::DeltaStream,
    pub installed: InputVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetMutation {
    Set {
        uuid: AssetUuid,
        resolution: StoredResolve,
        delta: AssetDeltaState,
    },
    Remove {
        uuid: AssetUuid,
        delta: AssetDeltaState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Commit grammar keeps Set strongly typed and allocation policy explicit.
pub enum AuthoringMutation {
    Set(AuthoringEntry),
    Remove { uuid: AssetUuid },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathMutation {
    Set {
        path: String,
        candidates: BTreeSet<AssetUuid>,
    },
    Remove {
        path: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Commit {
    pub assets: Vec<AssetMutation>,
    pub authoring: Vec<AuthoringMutation>,
    pub paths: Vec<PathMutation>,
    pub configuration: Option<ConfigurationStatus>,
    pub pipeline: Option<PipelineDiagnostic>,
    /// `Some(None)` heals version poison; `Some(Some(_))` publishes it.
    pub version_poison: Option<Option<VersionPoison>>,
    /// `Some(None)` clears repair inspection; `Some(Some(_))` publishes the
    /// exact current missing/duplicate lineage basis.
    pub lineage_repair: Option<Option<LineageRepairState>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    UnknownTarget {
        target: String,
    },
    InvalidTargetAttestation(crate::AttestationShapeError),
    DuplicateAssetMutation {
        uuid: AssetUuid,
    },
    DuplicateAuthoringMutation {
        uuid: AssetUuid,
    },
    InvalidAuthoringValue {
        uuid: AssetUuid,
        error: AuthoringValueError,
    },
    InvalidAuthoringIdentity {
        uuid: AssetUuid,
        detail: String,
    },
    IncompleteArtifactTypeCoverage {
        missing: Vec<TypeUuid>,
    },
    InvalidServedClosure {
        detail: String,
    },
    InvalidArtifact {
        detail: String,
    },
    InvalidLineageRepairState {
        detail: String,
    },
    InvalidVersionPoison {
        error: distill_store::state::VersionPoisonError,
    },
    InvalidConfigurationPoison {
        error: distill_store::state::DscpError,
    },
    InvalidPipelineDiagnostic {
        detail: String,
    },
    DuplicatePathMutation {
        path: String,
    },
    InvalidPath {
        path: String,
    },
    EmptyPathCandidates {
        path: String,
    },
    ArtifactAlreadyExistsWithDifferentPayload {
        hash: ContentHash,
    },
    WireTreeHashMismatch {
        expected: LayoutHash,
        observed: LayoutHash,
    },
    InvalidWireTree {
        detail: String,
    },
    WireTreeAlreadyExistsWithDifferentPayload {
        hash: LayoutHash,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringValueError {
    LogicalSchemaUtf8,
    LogicalSchemaInvalid(String),
    ValueInvalid(String),
    ValueNotCanonical,
    SchemaValueShape { detail: String },
    BlobIndexOutOfRange { index: u32, blob_count: u32 },
    DuplicateBlobIndex { index: u32 },
    UnusedBlobIndex { index: u32 },
}
