//! Outcome-bearing build/import traces and verifying-trace lookup (§§8–10).

use distill_core::canonical::{domain_digest, CanonicalEncoder, DSLF, DSTR};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, TypeUuid};

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
pub enum LocalFailureClass {
    Validator,
    MigrationPlan,
    Processor,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FailureCause {
    Op,
    Local(StableFailureFingerprint),
}

/// Construct a local stable fingerprint from typed canonical facts. Human
/// presentation text is deliberately not accepted by this API.
pub fn local_failure_fingerprint(
    class: LocalFailureClass,
    facts: impl FnOnce(&mut CanonicalEncoder),
) -> StableFailureFingerprint {
    StableFailureFingerprint::Local {
        class,
        detail: domain_digest(DSLF, 1, |encoder| {
            encoder.u16(class as u16);
            facts(encoder);
        }),
    }
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
        }
    }
}

pub trait TraceSource {
    fn read(&self, asset: AssetUuid) -> Observed<ContentHash>;
    fn resolve(&self, path: &str) -> Observed<Option<AssetUuid>>;
    fn query(&self, query: &AssetQuery) -> Observed<[u8; 32]>;
    fn tool(&self, id: &str) -> Observed<[u8; 32]>;
    fn capability(&self, key: &CapabilityKey) -> Observed<[u8; 32]>;
    fn ref_check(&self, asset: AssetUuid, expected: TypeUuid) -> Observed<Option<TypeUuid>>;
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
    domain_digest(DSTR, 1, |e| e.seq(trace, encode_trace_op))
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
    e.option(q.migration_edge, |e, v| {
        e.raw(&v.0 .0);
        e.raw(&v.1 .0);
    });
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
            e.u8(*class as u8);
            e.raw(detail);
        }
    }
}
