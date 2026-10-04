//! Outcome-bearing build/import traces and verifying-trace lookup (§§8–10).

use distill_core::canonical::{CanonicalEncoder, DSTR};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_json::AuthoredValue;

pub use distill_pipeline_api::failure::{
    control_failure_fingerprint, CapabilityKey, ControlFailureCode, ControlFailureEntries,
    ControlFailureError, ControlFailureFingerprint, ControlFailureSubject, ControlQuery,
    ControlSubject, EntryRole, LocalFailureClass, RawFileFailureClass, RawFileOp, RawFileSubject,
    StableFailureFingerprint, ToolLaunchDiagnostic, ToolLaunchFailureClass,
};
use crate::dslf::{DslfError, DslfV1};
use crate::import::ImportRuleId;
use crate::query::{AssetQuery, FileQuery};

#[path = "trace_decode.rs"]
mod decode;
pub use decode::{decode_trace_canonical_bytes, decode_trace_payload_bytes, TraceDecodeError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Observed<T> {
    Ok(T),
    Err(StableFailureFingerprint),
}

/// Identity of the exact canonical bundle bytes from which a control value
/// was decoded. A successful ControlRead traces this identity, not an
/// artifact/content dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ControlValueHash(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackDefinitionControlValue {
    pub roots: Vec<AssetQuery>,
    pub target: String,
    pub zstd_level: i32,
    pub include_path_table: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DirectoryGrouping {
    PerFile,
    ByStem,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectoryImportRule {
    pub id: ImportRuleId,
    pub matches: FileQuery,
    pub group: DirectoryGrouping,
    pub importer: String,
    pub settings: AuthoredValue,
    pub output: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectoryImportRulesControlValue {
    pub listing: FileQuery,
    pub rules: Vec<DirectoryImportRule>,
}

/// Branded, schema-validated import settings. There is intentionally no
/// generic `AuthoredValue` accessor; the importer bridge consumes these
/// canonical bytes only after matching the two exposed schema identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSettingsControlValue {
    pub settings_type: TypeUuid,
    pub schema_hash: LogicalHash,
    canonical_bytes: Box<[u8]>,
}

impl ImportSettingsControlValue {
    pub fn from_validated(
        settings_type: TypeUuid,
        schema_hash: LogicalHash,
        canonical_bytes: impl Into<Box<[u8]>>,
    ) -> Self {
        Self {
            settings_type,
            schema_hash,
            canonical_bytes: canonical_bytes.into(),
        }
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

/// Closed decoded control values. The variant brands otherwise opaque
/// canonical metadata without exposing `AuthoredValue`, artifacts, or any
/// dependency carrier to control-plane consumers.
#[derive(Debug, Clone, PartialEq)]
pub enum ControlValue {
    PackDefinition(PackDefinitionControlValue),
    DirectoryImportRules(DirectoryImportRulesControlValue),
    ImportSettings(ImportSettingsControlValue),
}

impl ControlValue {
    pub fn matches_subject(&self, subject: &ControlSubject) -> bool {
        matches!(
            (self, subject),
            (Self::PackDefinition(_), ControlSubject::PackDefinition(_))
                | (
                    Self::DirectoryImportRules(_),
                    ControlSubject::DirectoryImportRules(_)
                )
                | (
                    Self::ImportSettings(_),
                    ControlSubject::ImportSettings { .. }
                )
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedControlValue {
    pub identity: ControlValueHash,
    pub value: ControlValue,
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
    /// Authoring-service read of one source entry. The whole owning bundle's
    /// byte identity is observed because one bundle is the atomic authored
    /// file; `Ok(None)` is a first-class missing read.
    AuthoringRead {
        asset: AssetUuid,
        observed: Observed<Option<BundleFileHash>>,
    },
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
            Self::AuthoringRead { observed, .. } => matches!(observed, Observed::Err(_)),
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
pub trait TraceSource {
    fn authoring_read(&self, asset: AssetUuid) -> Observed<Option<BundleFileHash>>;
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
        TraceOp::AuthoringRead { asset, observed } => &source.authoring_read(*asset) == observed,
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

pub fn trace_digest(trace: &[TraceOp]) -> [u8; 32] {
    distill_core::canonical::domain_digest(DSTR, 1, |encoder| {
        encoder.raw(&trace_payload_bytes(trace));
    })
}

/// The exact DSTR v1 hash preimage. Exposing the bytes makes protocol tests
/// pin enum tags, integer widths, and variable-length framing directly.
pub fn trace_canonical_bytes(trace: &[TraceOp]) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.raw(&DSTR);
    encoder.u8(1);
    encoder.raw(&trace_payload_bytes(trace));
    encoder.into_bytes()
}

/// The serialized trace body stored in a `results` row. The CAS applies the
/// `DSTR` domain and version to these bytes when deriving the candidate's
/// secondary key, so storing the already-prefixed preimage would apply the
/// domain twice.
pub fn trace_payload_bytes(trace: &[TraceOp]) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
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
            e.enum_variant(1);
            ok(e, v);
        }
        Observed::Err(f) => {
            e.enum_variant(2);
            encode_failure(e, f, 0);
        }
    }
}

fn encode_trace_op(e: &mut CanonicalEncoder, op: &TraceOp) {
    match op {
        TraceOp::AuthoringRead { asset, observed: o } => {
            e.enum_variant(11);
            e.raw(&asset.0);
            observed(e, o, |e, hash| e.option(*hash, |e, hash| e.raw(&hash.0)));
        }
        TraceOp::Read { asset, observed: o } => {
            e.enum_variant(1);
            e.raw(&asset.0);
            observed(e, o, |e, h| e.raw(&h.0));
        }
        TraceOp::Resolve { path, observed: o } => {
            e.enum_variant(2);
            e.str(path);
            observed(e, o, |e, id| e.option(*id, |e, id| e.raw(&id.0)));
        }
        TraceOp::Query { query, observed: o } => {
            e.enum_variant(3);
            encode_query(e, query);
            observed(e, o, |e, h| e.raw(h));
        }
        TraceOp::Tool { id, observed: o } => {
            // Tag 4 is permanently reserved for the removed ToolLaunch
            // grammar. ToolEpoch observations use the closed v1 tag 10.
            e.enum_variant(10);
            e.str(id);
            observed(e, o, |e, h| e.raw(h));
        }
        TraceOp::Capability { key, observed: o } => {
            e.enum_variant(5);
            encode_capability(e, key);
            observed(e, o, |e, h| e.raw(h));
        }
        TraceOp::RefCheck {
            asset,
            expected_terminal,
            observed: o,
        } => {
            e.enum_variant(6);
            e.raw(&asset.0);
            e.raw(&expected_terminal.0);
            observed(e, o, |e, ty| e.option(*ty, |e, ty| e.raw(&ty.0)));
        }
        TraceOp::RoleCheck { asset, observed: o } => {
            e.enum_variant(7);
            e.raw(&asset.0);
            observed(e, o, |e, role| e.option(*role, |e, role| e.u8(*role as u8)));
        }
        TraceOp::Control { query, observed: o } => {
            e.enum_variant(8);
            encode_control_query(e, query);
            observed(e, o, |e, hash| e.raw(hash));
        }
        TraceOp::ControlRead {
            subject,
            observed: o,
        } => {
            e.enum_variant(9);
            encode_control_subject(e, subject);
            observed(e, o, |e, hash| e.raw(&hash.0));
        }
    }
}

fn encode_control_query(e: &mut CanonicalEncoder, query: &ControlQuery) {
    match query {
        ControlQuery::DirectoryImportRuleSet => e.enum_variant(2),
    }
}

fn encode_control_subject(e: &mut CanonicalEncoder, subject: &ControlSubject) {
    match subject {
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
            e.enum_variant(1);
            e.str(v);
        }
        CapabilityKey::DefaultTable(v) => {
            e.enum_variant(2);
            e.raw(&v.0);
        }
        CapabilityKey::Importer(v) => {
            e.enum_variant(3);
            e.str(v);
        }
        CapabilityKey::Processor { input } => {
            e.enum_variant(4);
            e.raw(&input.0);
        }
        CapabilityKey::Tool(id) => {
            e.enum_variant(5);
            e.str(id);
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
    e.option(q.authoring_only, |e, v| e.bool(*v));
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
