//! Stable failure fingerprints (§§8–10) and the closed vocabulary they carry.

use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};

use crate::query::{AssetQuery, FileQuery};

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

impl LocalFailureClass {
    pub fn from_u16(value: u16) -> Option<Self> {
        Some(match value {
            1 => Self::Validator,
            2 => Self::MigrationPlan,
            3 => Self::Processor,
            4 => Self::MigrationFunction,
            5 => Self::OutputBinding,
            6 => Self::Importer,
            7 => Self::ImportIntake,
            8 => Self::ArtifactEncoding,
            _ => return None,
        })
    }
}

/// Diagnostic for an execution failure after a successful, traced tool
/// lookup. This type is intentionally absent from
/// [`StableFailureFingerprint`]: launch outcomes are transient and an
/// attempted build/import result carrying one must be discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolLaunchDiagnostic {
    pub id: String,
    pub tool_hash: [u8; 32],
    pub class: ToolLaunchFailureClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLaunchFailureClass {
    NotExecutable,
    SpawnDenied,
    PackageUnavailable,
    AmbientUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EntryRole {
    Runtime = 0,
    AuthoringOnly = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RawFileOp {
    Read,
    Probe,
    Enumerate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RawFileFailureClass {
    NotFound,
    PermissionDenied,
    ListingFailed,
    OtherStable,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RawFileSubject {
    Path(String),
    Query(FileQuery),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CapabilityKey {
    MigrationFn(String),
    DefaultTable(TypeUuid),
    Importer(String),
    Processor { input: TypeUuid },
    Tool(String),
}

/// Coordinator-private control-plane queries. This is deliberately a
/// separate vocabulary from [`AssetQuery`], so an ordinary runtime/process
/// query cannot opt authoring-only control entries into its result set.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlQuery {
    /// Enumerate all directory-import-rule controls at the pinned basis.
    DirectoryImportRuleSet,
}

/// The exact non-artifact control value a coordinator read attempted.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlSubject {
    PackDefinition(AssetUuid),
    DirectoryImportRules(AssetUuid),
    ImportSettings {
        bundle: BundleUuid,
        local_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlFailureSubject {
    Query(ControlQuery),
    Read(ControlSubject),
}

/// Stable coordinator-control failure codes. Zero is permanently reserved;
/// these numeric values are part of the DSTR v1 grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ControlFailureCode {
    RoleViolation = 1,
    Missing = 2,
    Ambiguous = 3,
    Poisoned = 4,
    Malformed = 5,
    WrongBuiltInType = 6,
    WrongRole = 7,
    UnsupportedFormat = 8,
    SchemaClosure = 9,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlFailureError {
    EntriesRequired { code: ControlFailureCode },
    AmbiguousNeedsTwoEntries { observed: usize },
    EntriesForbidden { code: ControlFailureCode },
}

/// Canonical conflicting/duplicate entry identities carried by a control
/// failure. Construction sorts and deduplicates the entries so equality and
/// revalidation have the same semantics as their DSTR encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ControlFailureEntries(Vec<AssetUuid>);

impl ControlFailureEntries {
    pub fn new(mut entries: Vec<AssetUuid>) -> Self {
        entries.sort_unstable();
        entries.dedup();
        Self(entries)
    }

    pub fn as_slice(&self) -> &[AssetUuid] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<AssetUuid>> for ControlFailureEntries {
    fn from(entries: Vec<AssetUuid>) -> Self {
        Self::new(entries)
    }
}

/// Canonical, cardinality-checked control failure payload. Its fields are
/// private so a DSTR fingerprint cannot represent a forbidden code/entry
/// combination.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlFailureFingerprint {
    subject: ControlFailureSubject,
    code: ControlFailureCode,
    entries: ControlFailureEntries,
}

impl ControlFailureFingerprint {
    pub fn subject(&self) -> &ControlFailureSubject {
        &self.subject
    }

    pub fn code(&self) -> ControlFailureCode {
        self.code
    }

    pub fn entries(&self) -> &[AssetUuid] {
        self.entries.as_slice()
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StableFailureFingerprint {
    Ambiguous {
        conflicting: Vec<AssetUuid>,
    },
    Poisoned {
        bundle: BundleUuid,
    },
    MissingRef {
        query: Box<AssetQuery>,
        expected_terminal: TypeUuid,
    },
    RoleIneligible {
        asset: AssetUuid,
        observed_role: EntryRole,
    },
    Control(ControlFailureFingerprint),
    Descendant {
        asset: AssetUuid,
        fingerprint: Box<Self>,
    },
    RawFile {
        op: RawFileOp,
        subject: RawFileSubject,
        class: RawFileFailureClass,
    },
    MissingCapability {
        key: CapabilityKey,
    },
    Local {
        class: LocalFailureClass,
        detail: [u8; 32],
    },
}

pub fn control_failure_fingerprint(
    subject: ControlFailureSubject,
    code: ControlFailureCode,
    entries: impl Into<ControlFailureEntries>,
) -> Result<StableFailureFingerprint, ControlFailureError> {
    let entries = entries.into();
    let count = entries.as_slice().len();
    match code {
        ControlFailureCode::RoleViolation | ControlFailureCode::Poisoned if count == 0 => {
            return Err(ControlFailureError::EntriesRequired { code });
        }
        ControlFailureCode::Ambiguous if count < 2 => {
            return Err(ControlFailureError::AmbiguousNeedsTwoEntries { observed: count });
        }
        ControlFailureCode::Missing
        | ControlFailureCode::Malformed
        | ControlFailureCode::WrongBuiltInType
        | ControlFailureCode::WrongRole
        | ControlFailureCode::UnsupportedFormat
        | ControlFailureCode::SchemaClosure
            if count != 0 =>
        {
            return Err(ControlFailureError::EntriesForbidden { code });
        }
        _ => {}
    }
    Ok(StableFailureFingerprint::Control(
        ControlFailureFingerprint {
            subject,
            code,
            entries,
        },
    ))
}
