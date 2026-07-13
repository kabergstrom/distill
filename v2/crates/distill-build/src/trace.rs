//! Outcome-bearing build/import traces and verifying-trace lookup (§§8–10).

use distill_core::canonical::{CanonicalEncoder, DSTR};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};

pub use crate::dslf::LocalFailureClass;
use crate::dslf::{DslfError, DslfV1};
use crate::query::{AssetQuery, FileQuery};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Observed<T> {
    Ok(T),
    Err(StableFailureFingerprint),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ToolErrorClass {
    NotExecutable,
    MissingInterpreter,
    SpawnDenied,
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
}

/// Coordinator-private control-plane queries. This is deliberately a
/// separate vocabulary from [`AssetQuery`], so an ordinary runtime/process
/// query cannot opt authoring-only control entries into its result set.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlQuery {
    MigrationEdges {
        type_uuid: TypeUuid,
        from_hash: LogicalHash,
    },
    /// Enumerate all directory-import-rule controls at the pinned basis.
    DirectoryImportRuleSet,
}

/// The exact non-artifact control value a coordinator read attempted.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlSubject {
    Migration(AssetUuid),
    PackDefinition(AssetUuid),
    DirectoryImportRules(AssetUuid),
    ImportSettings {
        bundle: BundleUuid,
        local_id: String,
    },
    SchemaLineageManifest,
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

/// Identity of the exact canonical bundle bytes from which a control value
/// was decoded. A successful ControlRead traces this identity, not an
/// artifact/content dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ControlValueHash(pub [u8; 32]);

macro_rules! control_metadata {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name(Box<[u8]>);

        impl $name {
            /// Wrap already-validated canonical metadata for this one
            /// built-in control shape.
            pub fn from_canonical_metadata(bytes: impl Into<Box<[u8]>>) -> Self {
                Self(bytes.into())
            }

            pub fn canonical_metadata(&self) -> &[u8] {
                &self.0
            }
        }
    };
}

control_metadata!(MigrationControlValue);
control_metadata!(PackDefinitionControlValue);
control_metadata!(DirectoryImportRulesControlValue);
control_metadata!(ImportSettingsControlValue);
control_metadata!(SchemaLineageManifestControlValue);

/// Closed decoded control values. The variant brands otherwise opaque
/// canonical metadata without exposing `AuthoredValue`, artifacts, or any
/// dependency carrier to control-plane consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlValue {
    Migration(MigrationControlValue),
    PackDefinition(PackDefinitionControlValue),
    DirectoryImportRules(DirectoryImportRulesControlValue),
    ImportSettings(ImportSettingsControlValue),
    SchemaLineageManifest(SchemaLineageManifestControlValue),
}

impl ControlValue {
    pub fn matches_subject(&self, subject: &ControlSubject) -> bool {
        matches!(
            (self, subject),
            (Self::Migration(_), ControlSubject::Migration(_))
                | (Self::PackDefinition(_), ControlSubject::PackDefinition(_))
                | (
                    Self::DirectoryImportRules(_),
                    ControlSubject::DirectoryImportRules(_)
                )
                | (
                    Self::ImportSettings(_),
                    ControlSubject::ImportSettings { .. }
                )
                | (
                    Self::SchemaLineageManifest(_),
                    ControlSubject::SchemaLineageManifest
                )
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedControlValue {
    pub identity: ControlValueHash,
    pub value: ControlValue,
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
    ToolLaunch {
        id: String,
        class: ToolErrorClass,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FailureCause {
    Op,
    Local(StableFailureFingerprint),
}

/// Construct a local stable fingerprint from the closed DSLF v1 grammar.
/// Human presentation text and optional catch-all fact bags are impossible.
pub fn local_failure_fingerprint(facts: &DslfV1) -> Result<StableFailureFingerprint, DslfError> {
    Ok(StableFailureFingerprint::Local {
        class: facts.class(),
        detail: facts.digest()?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TraceOp {
    Read {
        asset: AssetUuid,
        observed: Observed<ContentHash>,
    },
    Resolve {
        path: String,
        observed: Observed<Option<AssetUuid>>,
    },
    Query {
        query: Box<AssetQuery>,
        observed: Observed<[u8; 32]>,
    },
    Tool {
        id: String,
        observed: Observed<[u8; 32]>,
    },
    Capability {
        key: CapabilityKey,
        observed: Observed<[u8; 32]>,
    },
    RefCheck {
        asset: AssetUuid,
        expected_terminal: TypeUuid,
        observed: Observed<Option<TypeUuid>>,
    },
    RoleCheck {
        asset: AssetUuid,
        observed: Observed<Option<EntryRole>>,
    },
    Control {
        query: ControlQuery,
        observed: Observed<[u8; 32]>,
    },
    ControlRead {
        subject: ControlSubject,
        observed: Observed<ControlValueHash>,
    },
}

impl TraceOp {
    pub fn failed(&self) -> bool {
        match self {
            Self::Read { observed, .. } => matches!(observed, Observed::Err(_)),
            Self::Resolve { observed, .. } => matches!(observed, Observed::Err(_)),
            Self::Query { observed, .. } => matches!(observed, Observed::Err(_)),
            Self::Tool { observed, .. } => matches!(observed, Observed::Err(_)),
            Self::Capability { observed, .. } => matches!(observed, Observed::Err(_)),
            Self::RefCheck { observed, .. } => matches!(observed, Observed::Err(_)),
            Self::RoleCheck { observed, .. } => matches!(observed, Observed::Err(_)),
            Self::Control { observed, .. } => matches!(observed, Observed::Err(_)),
            Self::ControlRead { observed, .. } => matches!(observed, Observed::Err(_)),
        }
    }
}

/// Construction errors for a coordinator's focused control attempted basis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptedControlBasisError {
    QueryRequired,
    QueryAlreadyRecorded,
    HardStopped,
    ObservedFailure(StableFailureFingerprint),
}

/// A control operation sequence that cannot silently discard failed reads.
/// Exactly one enumeration/query begins the attempt; every decoded read is
/// appended through `read`, and the first stable failure is retained as the
/// terminal op and hard-stops the attempt.
#[derive(Debug, Default)]
pub struct AttemptedControlBasis {
    trace: Vec<TraceOp>,
    has_query: bool,
    stopped: bool,
}

impl AttemptedControlBasis {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn query(
        &mut self,
        query: ControlQuery,
        observed: Observed<[u8; 32]>,
    ) -> Result<[u8; 32], AttemptedControlBasisError> {
        if self.stopped {
            return Err(AttemptedControlBasisError::HardStopped);
        }
        if self.has_query {
            return Err(AttemptedControlBasisError::QueryAlreadyRecorded);
        }
        self.has_query = true;
        let result = match &observed {
            Observed::Ok(hash) => Ok(*hash),
            Observed::Err(failure) => {
                self.stopped = true;
                Err(AttemptedControlBasisError::ObservedFailure(failure.clone()))
            }
        };
        self.trace.push(TraceOp::Control { query, observed });
        result
    }

    pub fn read(
        &mut self,
        subject: ControlSubject,
        observed: Observed<DecodedControlValue>,
    ) -> Result<ControlValue, AttemptedControlBasisError> {
        if !self.has_query {
            return Err(AttemptedControlBasisError::QueryRequired);
        }
        if self.stopped {
            return Err(AttemptedControlBasisError::HardStopped);
        }

        let (trace_observed, result) = match observed {
            Observed::Ok(decoded) if decoded.value.matches_subject(&subject) => {
                (Observed::Ok(decoded.identity), Ok(decoded.value))
            }
            Observed::Ok(_) => {
                let failure = control_failure_fingerprint(
                    ControlFailureSubject::Read(subject.clone()),
                    ControlFailureCode::WrongBuiltInType,
                    Vec::new(),
                )
                .expect("WrongBuiltInType carries no entry identities");
                (
                    Observed::Err(failure.clone()),
                    Err(AttemptedControlBasisError::ObservedFailure(failure)),
                )
            }
            Observed::Err(failure) => (
                Observed::Err(failure.clone()),
                Err(AttemptedControlBasisError::ObservedFailure(failure)),
            ),
        };
        if result.is_err() {
            self.stopped = true;
        }
        self.trace.push(TraceOp::ControlRead {
            subject,
            observed: trace_observed,
        });
        result
    }

    pub fn trace(&self) -> &[TraceOp] {
        &self.trace
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// Finalize both successful and failed attempts. A failed attempt still
    /// returns its complete terminal-failure trace; only omission of the
    /// required query is rejected.
    pub fn into_trace(self) -> Result<Vec<TraceOp>, AttemptedControlBasisError> {
        if !self.has_query {
            return Err(AttemptedControlBasisError::QueryRequired);
        }
        Ok(self.trace)
    }
}

pub trait TraceSource {
    fn read(&self, asset: AssetUuid) -> Observed<ContentHash>;
    fn resolve(&self, path: &str) -> Observed<Option<AssetUuid>>;
    fn query(&self, query: &AssetQuery) -> Observed<[u8; 32]>;
    fn tool(&self, id: &str) -> Observed<[u8; 32]>;
    fn capability(&self, key: &CapabilityKey) -> Observed<[u8; 32]>;
    fn ref_check(&self, asset: AssetUuid, expected: TypeUuid) -> Observed<Option<TypeUuid>>;
    fn role_check(&self, asset: AssetUuid) -> Observed<Option<EntryRole>>;
    fn control(&self, query: &ControlQuery) -> Observed<[u8; 32]>;
    fn control_read(&self, subject: &ControlSubject) -> Observed<ControlValueHash>;
}

pub fn revalidate(trace: &[TraceOp], source: &impl TraceSource) -> bool {
    trace.iter().all(|op| match op {
        TraceOp::Read { asset, observed } => &source.read(*asset) == observed,
        TraceOp::Resolve { path, observed } => &source.resolve(path) == observed,
        TraceOp::Query { query, observed } => &source.query(query) == observed,
        TraceOp::Tool { id, observed } => &source.tool(id) == observed,
        TraceOp::Capability { key, observed } => &source.capability(key) == observed,
        TraceOp::RefCheck {
            asset,
            expected_terminal,
            observed,
        } => &source.ref_check(*asset, *expected_terminal) == observed,
        TraceOp::RoleCheck { asset, observed } => &source.role_check(*asset) == observed,
        TraceOp::Control { query, observed } => &source.control(query) == observed,
        TraceOp::ControlRead { subject, observed } => &source.control_read(subject) == observed,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureRecord {
    pub trace: Vec<TraceOp>,
    pub cause: FailureCause,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureRecordError {
    MissingTerminalOp,
    TerminalOpDidNotFail,
    LocalCauseIsNotLocal,
}

impl FailureRecord {
    pub fn validate(&self) -> Result<(), FailureRecordError> {
        match &self.cause {
            FailureCause::Op => match self.trace.last() {
                None => Err(FailureRecordError::MissingTerminalOp),
                Some(op) if !op.failed() => Err(FailureRecordError::TerminalOpDidNotFail),
                Some(_) => Ok(()),
            },
            FailureCause::Local(StableFailureFingerprint::Local { .. }) => Ok(()),
            FailureCause::Local(_) => Err(FailureRecordError::LocalCauseIsNotLocal),
        }
    }
}

pub fn trace_digest(trace: &[TraceOp]) -> [u8; 32] {
    *blake3::hash(&trace_canonical_bytes(trace)).as_bytes()
}

/// The exact DSTR v1 hash preimage. Exposing the bytes makes protocol tests
/// pin enum tags, integer widths, and variable-length framing directly.
pub fn trace_canonical_bytes(trace: &[TraceOp]) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.raw(&DSTR);
    encoder.u8(1);
    encoder.seq(trace, encode_trace_op);
    encoder.into_bytes()
}

fn observed<T>(
    e: &mut CanonicalEncoder,
    value: &Observed<T>,
    ok: impl Fn(&mut CanonicalEncoder, &T),
) {
    match value {
        Observed::Ok(v) => {
            e.enum_variant(0);
            ok(e, v);
        }
        Observed::Err(f) => {
            e.enum_variant(1);
            encode_failure(e, f, 0);
        }
    }
}

fn encode_trace_op(e: &mut CanonicalEncoder, op: &TraceOp) {
    match op {
        TraceOp::Read { asset, observed: o } => {
            e.enum_variant(0);
            e.raw(&asset.0);
            observed(e, o, |e, h| e.raw(&h.0));
        }
        TraceOp::Resolve { path, observed: o } => {
            e.enum_variant(1);
            e.str(path);
            observed(e, o, |e, id| e.option(*id, |e, id| e.raw(&id.0)));
        }
        TraceOp::Query { query, observed: o } => {
            e.enum_variant(2);
            encode_query(e, query);
            observed(e, o, |e, h| e.raw(h));
        }
        TraceOp::Tool { id, observed: o } => {
            e.enum_variant(3);
            e.str(id);
            observed(e, o, |e, h| e.raw(h));
        }
        TraceOp::Capability { key, observed: o } => {
            e.enum_variant(4);
            encode_capability(e, key);
            observed(e, o, |e, h| e.raw(h));
        }
        TraceOp::RefCheck {
            asset,
            expected_terminal,
            observed: o,
        } => {
            e.enum_variant(5);
            e.raw(&asset.0);
            e.raw(&expected_terminal.0);
            observed(e, o, |e, ty| e.option(*ty, |e, ty| e.raw(&ty.0)));
        }
        TraceOp::RoleCheck { asset, observed: o } => {
            e.enum_variant(6);
            e.raw(&asset.0);
            observed(e, o, |e, role| e.option(*role, |e, role| e.u8(*role as u8)));
        }
        TraceOp::Control { query, observed: o } => {
            e.enum_variant(7);
            encode_control_query(e, query);
            observed(e, o, |e, hash| e.raw(hash));
        }
        TraceOp::ControlRead {
            subject,
            observed: o,
        } => {
            e.enum_variant(8);
            encode_control_subject(e, subject);
            observed(e, o, |e, hash| e.raw(&hash.0));
        }
    }
}

fn encode_control_query(e: &mut CanonicalEncoder, query: &ControlQuery) {
    match query {
        ControlQuery::MigrationEdges {
            type_uuid,
            from_hash,
        } => {
            e.enum_variant(1);
            e.raw(&type_uuid.0);
            e.raw(&from_hash.0);
        }
        ControlQuery::DirectoryImportRuleSet => e.enum_variant(2),
    }
}

fn encode_control_subject(e: &mut CanonicalEncoder, subject: &ControlSubject) {
    match subject {
        ControlSubject::Migration(asset) => {
            e.enum_variant(1);
            e.raw(&asset.0);
        }
        ControlSubject::PackDefinition(asset) => {
            e.enum_variant(2);
            e.raw(&asset.0);
        }
        ControlSubject::DirectoryImportRules(asset) => {
            e.enum_variant(3);
            e.raw(&asset.0);
        }
        ControlSubject::ImportSettings { bundle, local_id } => {
            e.enum_variant(4);
            e.raw(&bundle.0);
            e.str(local_id);
        }
        ControlSubject::SchemaLineageManifest => e.enum_variant(5),
    }
}

fn encode_control_failure_subject(e: &mut CanonicalEncoder, subject: &ControlFailureSubject) {
    match subject {
        ControlFailureSubject::Query(query) => {
            e.enum_variant(1);
            encode_control_query(e, query);
        }
        ControlFailureSubject::Read(subject) => {
            e.enum_variant(2);
            encode_control_subject(e, subject);
        }
    }
}

fn encode_capability(e: &mut CanonicalEncoder, key: &CapabilityKey) {
    match key {
        CapabilityKey::MigrationFn(v) => {
            e.enum_variant(0);
            e.str(v);
        }
        CapabilityKey::DefaultTable(v) => {
            e.enum_variant(1);
            e.raw(&v.0);
        }
        CapabilityKey::Importer(v) => {
            e.enum_variant(2);
            e.str(v);
        }
        CapabilityKey::Processor { input } => {
            e.enum_variant(3);
            e.raw(&input.0);
        }
    }
}

fn encode_query(e: &mut CanonicalEncoder, q: &AssetQuery) {
    e.option(q.uuid, |e, v| e.raw(&v.0));
    e.option(q.bundle_path.as_deref(), |e, v| e.str(v));
    e.option(q.local_id.as_deref(), |e, v| e.str(v));
    e.option(q.bundle_uuid, |e, v| e.raw(&v.0));
    e.option(q.authored_type, |e, v| e.raw(&v.0));
    e.option(q.terminal_type, |e, v| e.raw(&v.0));
    e.option(q.tag.as_ref(), |e, v| {
        e.str(&v.tag);
        e.option(v.value.as_deref(), |e, v| e.str(v));
    });
    e.option(q.path_prefix.as_deref(), |e, v| e.str(v));
    e.option(q.path_glob.as_deref(), |e, v| e.str(v));
}

fn encode_failure(e: &mut CanonicalEncoder, failure: &StableFailureFingerprint, depth: usize) {
    assert!(depth < 256, "failure fingerprint nesting exceeds cap");
    match failure {
        StableFailureFingerprint::Ambiguous { conflicting } => {
            e.enum_variant(0);
            e.set(conflicting, |e, id| e.raw(&id.0));
        }
        StableFailureFingerprint::Poisoned { bundle } => {
            e.enum_variant(1);
            e.raw(&bundle.0);
        }
        StableFailureFingerprint::MissingRef {
            query,
            expected_terminal,
        } => {
            e.enum_variant(2);
            encode_query(e, query);
            e.raw(&expected_terminal.0);
        }
        StableFailureFingerprint::RoleIneligible {
            asset,
            observed_role,
        } => {
            e.enum_variant(8);
            e.raw(&asset.0);
            e.u8(*observed_role as u8);
        }
        StableFailureFingerprint::Control(failure) => {
            e.enum_variant(9);
            encode_control_failure_subject(e, failure.subject());
            e.u16(failure.code() as u16);
            e.set(failure.entries(), |e, id| e.raw(&id.0));
        }
        StableFailureFingerprint::Descendant { asset, fingerprint } => {
            e.enum_variant(3);
            e.raw(&asset.0);
            encode_failure(e, fingerprint, depth + 1);
        }
        StableFailureFingerprint::ToolLaunch { id, class } => {
            e.enum_variant(4);
            e.str(id);
            e.u8(*class as u8);
        }
        StableFailureFingerprint::RawFile { op, subject, class } => {
            e.enum_variant(5);
            e.u8(*op as u8);
            match subject {
                RawFileSubject::Path(path) => {
                    e.enum_variant(0);
                    e.str(path);
                }
                RawFileSubject::Query(q) => {
                    e.enum_variant(1);
                    e.option(q.path_prefix.as_deref(), |e, v| e.str(v));
                    e.option(q.path_glob.as_deref(), |e, v| e.str(v));
                }
            }
            e.u8(*class as u8);
        }
        StableFailureFingerprint::MissingCapability { key } => {
            e.enum_variant(6);
            encode_capability(e, key);
        }
        StableFailureFingerprint::Local { class, detail } => {
            e.enum_variant(7);
            e.u16(*class as u16);
            e.raw(detail);
        }
    }
}
