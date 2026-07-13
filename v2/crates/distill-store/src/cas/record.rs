//! §13's pinned CAS record frame and the result-payload grammar.
//!
//! Frame (all integers little-endian):
//!
//! ```text
//! magic        u32 LE  "DSR1"
//! version      u8
//! kind         u8      payload (import encoding / processor output /
//!                      debug / wire tree) or result (commit marker)
//! key_len      u16 LE
//! out_key_len  u16 LE
//! asset_uuid   [u8; 16]
//! payload_len  u64 LE
//! content_hash [u8; 32]  blake3 of payload
//! crc32c       u32 LE    over kind…payload
//! static_input_key [u8; key_len]
//! output_key   [u8; out_key_len]  UTF-8; empty for the primary output
//! payload      [u8; payload_len]
//! pad          zeros to 16-byte alignment
//! ```
//!
//! "over kind…payload" is pinned as: the header bytes from `kind`
//! through `content_hash` inclusive, then the three variable sections —
//! the CRC field itself (and the magic/version prefix) excluded.
//!
//! Result records carry the parent/entry UUID in the frame's
//! `asset_uuid` field and the lookup-key digest in `static_input_key`;
//! the payload is the result-payload grammar below, tagged by key kind
//! (§13: DSSI = processor, DSBI = build import).

use distill_core::canonical::{domain_digest, DSTR};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, TypeUuid};

use crate::error::StoreError;

/// Fixed frame-header length in bytes.
pub const RECORD_HEADER_LEN: usize = 70;

/// The frame encoding version this crate reads and writes.
pub const RECORD_VERSION: u8 = 1;

pub const RECORD_MAGIC: [u8; 4] = *b"DSR1";

/// Decode nesting cap for `FailureFingerprint::Descendant` chains — a
/// definite error, never a stack overflow.
pub const MAX_FINGERPRINT_DEPTH: usize = 256;

/// Record kinds (§13), byte values pinned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    ImportEncoding = 0,
    ProcessorOutput = 1,
    Debug = 2,
    WireTree = 3,
    /// The commit marker (§13): recovery ignores payload records not
    /// covered by a committed result record.
    Result = 4,
}

impl RecordKind {
    pub fn is_payload(self) -> bool {
        !matches!(self, RecordKind::Result)
    }

    fn from_byte(b: u8) -> Option<RecordKind> {
        Some(match b {
            0 => RecordKind::ImportEncoding,
            1 => RecordKind::ProcessorOutput,
            2 => RecordKind::Debug,
            3 => RecordKind::WireTree,
            4 => RecordKind::Result,
            _ => return None,
        })
    }
}

/// One logical record. `content_hash` is derived (blake3 of payload) at
/// encode time and verified at decode time — never caller-supplied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub kind: RecordKind,
    /// Payload records carry the **parent** asset UUID (§13); result
    /// records the parent/entry UUID.
    pub asset_uuid: AssetUuid,
    pub static_input_key: Vec<u8>,
    /// Empty for the primary output (§13).
    pub output_key: String,
    pub payload: Vec<u8>,
}

/// A decoded record plus its physical extent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord {
    pub record: Record,
    /// blake3 of the payload, as stored and verified.
    pub content_hash: [u8; 32],
    /// Total encoded length including pad — the scan cursor advance.
    pub encoded_len: u64,
}

fn align16(n: usize) -> usize {
    (n + 15) & !15
}

/// CRC-32C (Castagnoli) — the same construction §6 pins and §12/§13
/// reuse.
pub fn crc32c(bytes: &[u8]) -> u32 {
    const POLY: u32 = 0x82F6_3B78;
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut table = [0u32; 256];
        for (i, slot) in table.iter_mut().enumerate() {
            let mut crc = i as u32;
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ POLY
                } else {
                    crc >> 1
                };
            }
            *slot = crc;
        }
        table
    });
    let mut crc = !0u32;
    for &b in bytes {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

/// Encode one record: computes the content hash and CRC, pads to 16.
///
/// Panics if `static_input_key` or `output_key` exceed u16 — those are
/// store-internal inputs (32-byte digests and declared output keys),
/// never external data.
pub fn encode_record(rec: &Record) -> Vec<u8> {
    let key_len = u16::try_from(rec.static_input_key.len()).expect("static_input_key > u16");
    let out_key = rec.output_key.as_bytes();
    let out_key_len = u16::try_from(out_key.len()).expect("output_key > u16");
    let payload_len = rec.payload.len() as u64;
    let content_hash = *blake3::hash(&rec.payload).as_bytes();

    let content_len =
        RECORD_HEADER_LEN + rec.static_input_key.len() + out_key.len() + rec.payload.len();
    let total = align16(content_len);
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&RECORD_MAGIC);
    out.push(RECORD_VERSION);
    out.push(rec.kind as u8);
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(&out_key_len.to_le_bytes());
    out.extend_from_slice(&rec.asset_uuid.0);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&content_hash);
    out.extend_from_slice(&[0u8; 4]); // crc placeholder
    out.extend_from_slice(&rec.static_input_key);
    out.extend_from_slice(out_key);
    out.extend_from_slice(&rec.payload);
    out.resize(total, 0);

    let crc = {
        let mut covered = Vec::with_capacity(61 + content_len - RECORD_HEADER_LEN);
        covered.extend_from_slice(&out[5..66]);
        covered.extend_from_slice(&out[RECORD_HEADER_LEN..content_len]);
        crc32c(&covered)
    };
    out[66..70].copy_from_slice(&crc.to_le_bytes());
    out
}

/// Decode one record from the start of `buf`. `segment`/`offset` are
/// diagnostic coordinates. Every length is validated against `buf`
/// before any allocation (§13).
pub fn decode_record(buf: &[u8], segment: u64, offset: u64) -> Result<DecodedRecord, StoreError> {
    let bad = |detail: &str| StoreError::BadRecord {
        segment,
        offset,
        detail: detail.to_owned(),
    };
    if buf.len() < RECORD_HEADER_LEN {
        return Err(bad("truncated header"));
    }
    if buf[0..4] != RECORD_MAGIC {
        return Err(bad("bad magic"));
    }
    if buf[4] != RECORD_VERSION {
        return Err(bad("unsupported record version"));
    }
    let kind = RecordKind::from_byte(buf[5]).ok_or_else(|| bad("unknown record kind"))?;
    let key_len = u16::from_le_bytes([buf[6], buf[7]]) as u64;
    let out_key_len = u16::from_le_bytes([buf[8], buf[9]]) as u64;
    let mut asset_uuid = [0u8; 16];
    asset_uuid.copy_from_slice(&buf[10..26]);
    let payload_len = u64::from_le_bytes(buf[26..34].try_into().unwrap());
    let mut content_hash = [0u8; 32];
    content_hash.copy_from_slice(&buf[34..66]);
    let crc_stored = u32::from_le_bytes(buf[66..70].try_into().unwrap());

    // Length arithmetic in u64, checked — before allocation.
    let content_len = (RECORD_HEADER_LEN as u64)
        .checked_add(key_len)
        .and_then(|n| n.checked_add(out_key_len))
        .and_then(|n| n.checked_add(payload_len))
        .ok_or(StoreError::OversizedRecord {
            segment,
            offset,
            payload_len,
        })?;
    let total =
        content_len
            .checked_add(15)
            .map(|n| n & !15u64)
            .ok_or(StoreError::OversizedRecord {
                segment,
                offset,
                payload_len,
            })?;
    if total > buf.len() as u64 {
        return Err(StoreError::OversizedRecord {
            segment,
            offset,
            payload_len,
        });
    }
    let content_len = content_len as usize;
    let total = total as usize;

    // CRC over kind…payload, the crc field skipped.
    let crc_actual = {
        let mut covered = Vec::with_capacity(61 + content_len - RECORD_HEADER_LEN);
        covered.extend_from_slice(&buf[5..66]);
        covered.extend_from_slice(&buf[RECORD_HEADER_LEN..content_len]);
        crc32c(&covered)
    };
    if crc_actual != crc_stored {
        return Err(bad("crc32c mismatch"));
    }

    let key_end = RECORD_HEADER_LEN + key_len as usize;
    let out_end = key_end + out_key_len as usize;
    let payload = &buf[out_end..content_len];
    // Verify against the stored blake3, not just CRC (§13).
    if *blake3::hash(payload).as_bytes() != content_hash {
        return Err(bad("blake3 content hash mismatch"));
    }
    let output_key = std::str::from_utf8(&buf[key_end..out_end])
        .map_err(|_| bad("output_key is not UTF-8"))?
        .to_owned();
    if buf[content_len..total].iter().any(|&b| b != 0) {
        return Err(bad("nonzero pad byte"));
    }

    Ok(DecodedRecord {
        record: Record {
            kind,
            asset_uuid: AssetUuid(asset_uuid),
            static_input_key: buf[RECORD_HEADER_LEN..key_end].to_vec(),
            output_key,
            payload: payload.to_vec(),
        },
        content_hash,
        encoded_len: total as u64,
    })
}

// ---------------------------------------------------------------------
// Result payload grammar
// ---------------------------------------------------------------------

/// §13: result records are tagged by key kind — the two lookup keys have
/// different shapes, and neither is shoehorned into the other's grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    /// `"DSSI"` — a processor result keyed by the StaticInputs digest.
    Processor = 0,
    /// `"DSBI"` — a build-import result keyed by the §8 pre-key digest.
    BuildImport = 1,
}

impl KeyKind {
    fn from_byte(b: u8) -> Option<KeyKind> {
        Some(match b {
            0 => KeyKind::Processor,
            1 => KeyKind::BuildImport,
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

/// A build's outcome as the result record stores it: a success carries
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

/// The decoded result-record payload (§13).
///
/// Pinned grammar (little-endian, u32 length prefixes, exact — trailing
/// bytes are an error):
///
/// ```text
/// key_kind  u8   (0 = DSSI processor, 1 = DSBI build import)
/// outcome   u8   (0 = success, 1 = failure)
/// static_inputs_len u32 + bytes   (the full canonical StaticInputs
///                                  encoding for index rebuild — DSSI;
///                                  empty for DSBI)
/// trace_len u32 + bytes           (canonical DSTR trace-op bytes)
/// success: output_count u32 × { key_len u32 + UTF-8, type_count u32 ×
///          [u8;16], hash [u8;32] }, aux_count u32 × { key_len u32 +
///          UTF-8, hash [u8;32] }
/// failure: cause tag u8 (0 = Op — nothing follows; 1 = Local —
///          fingerprint follows: tag u8 + fields; Descendant chains
///          linear, depth-capped at decode)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultPayload {
    pub key_kind: KeyKind,
    /// The full canonical `StaticInputs` encoding (§9) — raw
    /// `StaticInputs` are unbounded composites that would burst
    /// `key_len`, so this rides in the payload for index rebuild.
    pub static_inputs_canonical: Vec<u8>,
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
        domain_digest(DSTR, 1, |e| e.raw(&self.trace))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(self.key_kind as u8);
        match &self.outcome {
            ResultOutcome::Success { .. } => out.push(0),
            ResultOutcome::Failure { .. } => out.push(1),
        }
        put_bytes(&mut out, &self.static_inputs_canonical);
        put_bytes(&mut out, &self.trace);
        match &self.outcome {
            ResultOutcome::Success { outputs, aux } => {
                out.extend_from_slice(&(outputs.len() as u32).to_le_bytes());
                for row in outputs {
                    put_bytes(&mut out, row.output_key.as_bytes());
                    out.extend_from_slice(&(row.type_uuids.len() as u32).to_le_bytes());
                    for t in &row.type_uuids {
                        out.extend_from_slice(&t.0);
                    }
                    out.extend_from_slice(&row.content_hash.0);
                }
                out.extend_from_slice(&(aux.len() as u32).to_le_bytes());
                for row in aux {
                    put_bytes(&mut out, row.debug_key.as_bytes());
                    out.extend_from_slice(&row.content_hash.0);
                }
            }
            ResultOutcome::Failure { cause } => match cause {
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
            },
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<ResultPayload, StoreError> {
        let mut r = Reader { buf: bytes, pos: 0 };
        let key_kind =
            KeyKind::from_byte(r.u8()?).ok_or_else(|| bad_payload("unknown key_kind byte"))?;
        let outcome_tag = r.u8()?;
        let static_inputs_canonical = r.bytes()?.to_vec();
        let trace = r.bytes()?.to_vec();
        let outcome = match outcome_tag {
            0 => {
                let output_count = r.count()?;
                let mut outputs = Vec::with_capacity(output_count.min(1024));
                for _ in 0..output_count {
                    let output_key = r.string()?;
                    let type_count = r.count()?;
                    let mut type_uuids = Vec::with_capacity(type_count.min(1024));
                    for _ in 0..type_count {
                        type_uuids.push(TypeUuid(r.array16()?));
                    }
                    outputs.push(OutputRow {
                        output_key,
                        type_uuids,
                        content_hash: ContentHash(r.array32()?),
                    });
                }
                let aux_count = r.count()?;
                let mut aux = Vec::with_capacity(aux_count.min(1024));
                for _ in 0..aux_count {
                    aux.push(AuxRow {
                        debug_key: r.string()?,
                        content_hash: ContentHash(r.array32()?),
                    });
                }
                ResultOutcome::Success { outputs, aux }
            }
            1 => {
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
                ResultOutcome::Failure { cause }
            }
            _ => return Err(bad_payload("unknown outcome tag")),
        };
        if r.pos != bytes.len() {
            return Err(bad_payload("trailing bytes after result payload"));
        }
        Ok(ResultPayload {
            key_kind,
            static_inputs_canonical,
            trace,
            outcome,
        })
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
