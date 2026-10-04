//! §13's result row: the typed outcome a build result stores in
//! `results` / `result_outputs`, and the pinned encoding of a
//! deterministic failure's cause (`results.failure`).
//!
//! The CAS segments hold only content-addressed bytes; everything a
//! result says about them is these rows.

use distill_core::canonical::{domain_digest, DSTR};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, TypeUuid};

use crate::error::StoreError;

/// Decode nesting cap for `FailureFingerprint::Descendant` chains — a
/// definite error, never a stack overflow.
pub const MAX_FINGERPRINT_DEPTH: usize = 256;

// ---------------------------------------------------------------------
// Result payload grammar
// ---------------------------------------------------------------------

/// §13: result rows are tagged by key kind — the lookup keys have
/// different shapes, and none is shoehorned into another's grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    /// `"DSSI"` — a processor result keyed by the StaticInputs digest.
    Processor = 0,
    /// `"DSBI"` — a build-import result keyed by the §8 pre-key digest.
    BuildImport = 1,
    /// `"DSNK"` — one asset node's served outputs (its import, processor
    /// chain and the strong closure it reads), keyed by the node's static
    /// inputs.
    Node = 2,
}

impl KeyKind {
    pub(crate) fn from_byte(b: u8) -> Option<KeyKind> {
        Some(match b {
            0 => KeyKind::Processor,
            1 => KeyKind::BuildImport,
            2 => KeyKind::Node,
            _ => return None,
        })
    }
}

/// One row of the typed output table: `output_key → type uuids,
/// ContentHash` (§13). Physical placement lives solely in the extent
/// index — never here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRow {
    pub output_key: String,
    pub type_uuids: Vec<TypeUuid>,
    pub content_hash: ContentHash,
}

/// One auxiliary-payload row: `debug key → ContentHash` (§13's
/// cache-internal debug records). Pins and evicts with the result, never
/// appears in derived-output rows, manifests, or load-dep closures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxRow {
    pub debug_key: String,
    pub content_hash: ContentHash,
}

/// Store-side mirror of §9's `StableFailureFingerprint`: a typed,
/// content-derived encoding of a deterministic failure — stable across
/// runs, so revalidation compares it like a result hash. The `query`
/// field carries the canonical `AssetQuery` bytes (§10's type lives with
/// the query machinery, outside this crate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureFingerprint {
    /// Ambiguous resolution or query: the sorted conflicting ids.
    Ambiguous { conflicting: Vec<AssetUuid> },
    /// A poisoned bundle or version (§7, §13): the poison row's identity.
    Poisoned { bundle: BundleUuid },
    /// A strong reference that failed to resolve.
    MissingRef {
        query: Vec<u8>,
        expected_terminal: TypeUuid,
    },
    /// Exact resolution found an entry whose observed role cannot enter the
    /// attempted runtime/dependency carrier. Revalidation consults the role
    /// index so a role edit heals the memo.
    RoleIneligible {
        asset: AssetUuid,
        observed_role: EntryRole,
    },
    /// A descendant build failed: the child's own fingerprint.
    Descendant {
        asset: AssetUuid,
        fingerprint: Box<FailureFingerprint>,
    },
    /// A required pipeline capability was not registered
    /// (§9's `TraceOp::Capability`): the requested key. Revalidates
    /// against the snapshot's epoch, so the record heals on the first
    /// epoch that supplies the registration.
    MissingCapability { key: CapabilityKey },
    /// A deterministic LOCAL failure that arose from no context
    /// operation (`FailureCause::Local`): fingerprinted by class plus a
    /// stable content hash of the typed diagnostic.
    Local {
        class: LocalFailureClass,
        detail: [u8; 32],
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryRole {
    Runtime = 0,
    AuthoringOnly = 1,
}

impl EntryRole {
    fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::Runtime,
            1 => Self::AuthoringOnly,
            _ => return None,
        })
    }
}

/// What a capability lookup asked the pipeline epoch for (§9) — the
/// identity a recorded miss carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityKey {
    MigrationFn(String),
    DefaultTable(TypeUuid),
    Importer(String),
    Processor { input: TypeUuid },
    Tool(String),
}

/// The class of a deterministic local failure (§9): validator error
/// diagnostics, migration-plan validation, a processor `BuildError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    fn from_u16(value: u16) -> Option<LocalFailureClass> {
        Some(match value {
            1 => LocalFailureClass::Validator,
            2 => LocalFailureClass::MigrationPlan,
            3 => LocalFailureClass::Processor,
            4 => LocalFailureClass::MigrationFunction,
            5 => LocalFailureClass::OutputBinding,
            6 => LocalFailureClass::Importer,
            7 => LocalFailureClass::ImportIntake,
            8 => LocalFailureClass::ArtifactEncoding,
            _ => return None,
        })
    }
}

/// A failure record's terminal cause (§9, §13): a failure memoizes as a
/// dependency trace (possibly empty) plus exactly one `FailureCause`.
/// Either the trace's own terminal entry is the failing operation (an
/// `Observed::Err` op — the record ends in it, and no standalone
/// fingerprint rides beside the trace), or the failure is LOCAL and
/// deterministic — arising from no context operation — and carries its
/// fingerprint here, with no synthetic trace op invented for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureCause {
    /// The trace's terminal entry (an `Observed::Err` op) is the cause.
    Op,
    Local(FailureFingerprint),
}

/// A build's outcome as its result rows store it: a success carries
/// the typed output table and the auxiliary-payload table; a
/// deterministic failure carries its terminal `FailureCause` and **no
/// output rows** (§13). Transient infrastructure errors are typed apart
/// and are not representable here — they are never memoized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultOutcome {
    Success {
        outputs: Vec<OutputRow>,
        aux: Vec<AuxRow>,
    },
    Failure {
        cause: FailureCause,
    },
}

/// A committed result as its `results` and `result_outputs` rows hold it
/// (§13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultPayload {
    pub key_kind: KeyKind,
    /// The serialized dependency trace: canonical trace-op bytes as §9's
    /// machinery encodes them (up to and including the failing operation
    /// for failure outcomes).
    pub trace: Vec<u8>,
    pub outcome: ResultOutcome,
}

impl ResultPayload {
    /// The trace digest (`"DSTR"`, §5) — the candidate bucket's
    /// secondary key.
    pub fn trace_digest(&self) -> [u8; 32] {
        trace_digest(&self.trace)
    }
}

/// The digest (`"DSTR"`, §5) of canonical trace-op bytes.
pub fn trace_digest(trace: &[u8]) -> [u8; 32] {
    domain_digest(DSTR, 1, |e| e.raw(trace))
}

/// `results.failure`: a deterministic failure's cause.
///
/// Pinned grammar (little-endian, u32 length prefixes, exact — trailing
/// bytes are an error):
///
/// ```text
/// cause tag u8 (0 = Op — nothing follows; 1 = Local — fingerprint
/// follows: tag u8 + fields; Descendant chains linear, depth-capped at
/// decode)
/// ```
impl FailureCause {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            FailureCause::Op => out.push(0),
            FailureCause::Local(fingerprint) => {
                out.push(1);
                // Descendant chains encode iteratively — linear,
                // never encoder recursion.
                let mut fp = fingerprint;
                loop {
                    match fp {
                        FailureFingerprint::Ambiguous { conflicting } => {
                            out.push(0);
                            out.extend_from_slice(&(conflicting.len() as u32).to_le_bytes());
                            for id in conflicting {
                                out.extend_from_slice(&id.0);
                            }
                            break;
                        }
                        FailureFingerprint::Poisoned { bundle } => {
                            out.push(1);
                            out.extend_from_slice(&bundle.0);
                            break;
                        }
                        FailureFingerprint::MissingRef {
                            query,
                            expected_terminal,
                        } => {
                            out.push(2);
                            put_bytes(&mut out, query);
                            out.extend_from_slice(&expected_terminal.0);
                            break;
                        }
                        FailureFingerprint::Descendant { asset, fingerprint } => {
                            out.push(3);
                            out.extend_from_slice(&asset.0);
                            fp = fingerprint;
                        }
                        FailureFingerprint::MissingCapability { key } => {
                            out.push(5);
                            match key {
                                CapabilityKey::MigrationFn(name) => {
                                    out.push(1);
                                    put_bytes(&mut out, name.as_bytes());
                                }
                                CapabilityKey::DefaultTable(ty) => {
                                    out.push(2);
                                    out.extend_from_slice(&ty.0);
                                }
                                CapabilityKey::Importer(name) => {
                                    out.push(3);
                                    put_bytes(&mut out, name.as_bytes());
                                }
                                CapabilityKey::Processor { input } => {
                                    out.push(4);
                                    out.extend_from_slice(&input.0);
                                }
                                CapabilityKey::Tool(id) => {
                                    out.push(5);
                                    put_bytes(&mut out, id.as_bytes());
                                }
                            }
                            break;
                        }
                        FailureFingerprint::Local { class, detail } => {
                            out.push(6);
                            out.extend_from_slice(&(*class as u16).to_le_bytes());
                            out.extend_from_slice(detail);
                            break;
                        }
                        FailureFingerprint::RoleIneligible {
                            asset,
                            observed_role,
                        } => {
                            out.push(7);
                            out.extend_from_slice(&asset.0);
                            out.push(*observed_role as u8);
                            break;
                        }
                    }
                }
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<FailureCause, StoreError> {
        let mut r = Reader { buf: bytes, pos: 0 };
        let cause = match r.u8()? {
            0 => FailureCause::Op,
            1 => {
                // Decode the Descendant chain iteratively,
                // depth-capped.
                let mut ancestors: Vec<AssetUuid> = Vec::new();
                let terminal = loop {
                    if ancestors.len() > MAX_FINGERPRINT_DEPTH {
                        return Err(bad_payload("fingerprint nesting exceeds cap"));
                    }
                    match r.u8()? {
                        0 => {
                            let n = r.count()?;
                            let mut conflicting = Vec::with_capacity(n.min(1024));
                            for _ in 0..n {
                                conflicting.push(AssetUuid(r.array16()?));
                            }
                            break FailureFingerprint::Ambiguous { conflicting };
                        }
                        1 => {
                            break FailureFingerprint::Poisoned {
                                bundle: BundleUuid(r.array16()?),
                            }
                        }
                        2 => {
                            let query = r.bytes()?.to_vec();
                            break FailureFingerprint::MissingRef {
                                query,
                                expected_terminal: TypeUuid(r.array16()?),
                            };
                        }
                        3 => ancestors.push(AssetUuid(r.array16()?)),
                        4 => {
                            return Err(bad_payload(
                                "fingerprint tag 4 is permanently reserved",
                            ));
                        }
                        5 => {
                            let key = match r.u8()? {
                                1 => CapabilityKey::MigrationFn(r.string()?),
                                2 => CapabilityKey::DefaultTable(TypeUuid(r.array16()?)),
                                3 => CapabilityKey::Importer(r.string()?),
                                4 => CapabilityKey::Processor {
                                    input: TypeUuid(r.array16()?),
                                },
                                5 => CapabilityKey::Tool(r.string()?),
                                _ => return Err(bad_payload("unknown capability-key tag")),
                            };
                            break FailureFingerprint::MissingCapability { key };
                        }
                        6 => {
                            let class =
                                LocalFailureClass::from_u16(r.u16()?).ok_or_else(|| {
                                    bad_payload("unknown local failure class")
                                })?;
                            break FailureFingerprint::Local {
                                class,
                                detail: r.array32()?,
                            };
                        }
                        7 => {
                            let asset = AssetUuid(r.array16()?);
                            let observed_role = EntryRole::from_byte(r.u8()?)
                                .ok_or_else(|| bad_payload("unknown entry role"))?;
                            break FailureFingerprint::RoleIneligible {
                                asset,
                                observed_role,
                            };
                        }
                        _ => return Err(bad_payload("unknown fingerprint tag")),
                    }
                };
                let fingerprint =
                    ancestors.into_iter().rev().fold(terminal, |inner, asset| {
                        FailureFingerprint::Descendant {
                            asset,
                            fingerprint: Box::new(inner),
                        }
                    });
                FailureCause::Local(fingerprint)
            }
            _ => return Err(bad_payload("unknown failure-cause tag")),
        };
        if r.pos != bytes.len() {
            return Err(bad_payload("trailing bytes after a failure cause"));
        }
        Ok(cause)
    }
}

impl Drop for FailureFingerprint {
    fn drop(&mut self) {
        // Unwind Descendant chains iteratively: a hostile or merely deep
        // chain must not recurse the default drop glue off the stack.
        if let FailureFingerprint::Descendant { fingerprint, .. } = self {
            let mut next = std::mem::replace(
                fingerprint.as_mut(),
                FailureFingerprint::Poisoned {
                    bundle: BundleUuid([0u8; 16]),
                },
            );
            while let FailureFingerprint::Descendant { fingerprint, .. } = &mut next {
                next = std::mem::replace(
                    fingerprint.as_mut(),
                    FailureFingerprint::Poisoned {
                        bundle: BundleUuid([0u8; 16]),
                    },
                );
            }
        }
    }
}

fn bad_payload(detail: &str) -> StoreError {
    StoreError::BadResultPayload {
        detail: detail.to_owned(),
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], StoreError> {
        // Bounds first — never allocate or slice past the buffer.
        if self.buf.len() - self.pos < n {
            return Err(bad_payload("truncated result payload"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, StoreError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StoreError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u16(&mut self) -> Result<u16, StoreError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    /// A count whose elements each occupy at least one byte: bounded by
    /// the remaining buffer, so `count × min-size` can never demand an
    /// absurd allocation.
    fn count(&mut self) -> Result<usize, StoreError> {
        let n = self.u32()? as usize;
        if n > self.buf.len() - self.pos {
            return Err(bad_payload("count exceeds remaining payload"));
        }
        Ok(n)
    }

    fn bytes(&mut self) -> Result<&'a [u8], StoreError> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    fn string(&mut self) -> Result<String, StoreError> {
        let bytes = self.bytes()?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| bad_payload("string field is not UTF-8"))
    }

    fn array16(&mut self) -> Result<[u8; 16], StoreError> {
        Ok(self.take(16)?.try_into().unwrap())
    }

    fn array32(&mut self) -> Result<[u8; 32], StoreError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
}
