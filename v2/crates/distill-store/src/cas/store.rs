//! Append/commit/read machinery for the log-structured CAS (§13).
//!
//! Write order (pinned): append the build's payload records first and
//! one result record last, fsync each touched segment in that order,
//! then insert the index rows in one memo transaction. **The result
//! record is the commit marker** — a crash after output 2 of 3 publishes
//! nothing. Segment creation and deletion also fsync the directory.
//!
//! Each writer appends to a segment of its own; ordinary commit groups
//! roll as a unit. A record larger than the regular segment cap is
//! instead written alone in a typed oversize segment; recovery resolves
//! coverage across all segments, so a group may cross segment boundaries
//! without weakening the result marker rule. Writers never coordinate
//! beyond SQLite: a transaction that rolls back leaves its appended bytes
//! as dead space.

use std::io::Write;
use std::path::PathBuf;

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_wire::dswl::{decode_dswl, dswl_bytes, dswl_hash, DSWL_VERSION};

use crate::cas::record::{
    decode_record, encode_record, AuxRow, DecodedRecord, FailureCause, KeyKind, OutputRow, Record,
    RecordKind, ResultOutcome, ResultPayload, RECORD_HEADER_LEN,
};
use crate::db::{meta_get_u64, meta_set_u64, Store, StoreReader};
use crate::error::StoreError;
use crate::state::MemoSeq;

/// A segment file's kind. An oversize segment holds exactly one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Regular = 0,
    Oversize = 1,
}

impl SegmentKind {
    pub(crate) fn from_i64(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::Regular),
            1 => Some(Self::Oversize),
            _ => None,
        }
    }
}

/// The payload-record kind a successful build's outputs carry (§13's
/// payload kinds, minus the store-managed debug and wire-tree kinds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    ImportEncoding,
    ProcessorOutput,
}

impl PayloadKind {
    fn record_kind(self) -> RecordKind {
        match self {
            PayloadKind::ImportEncoding => RecordKind::ImportEncoding,
            PayloadKind::ProcessorOutput => RecordKind::ProcessorOutput,
        }
    }
}

/// One output to commit: key, declared type uuids, artifact bytes.
#[derive(Debug, Clone)]
pub struct OutputSpec {
    /// Empty for the primary output (§13).
    pub output_key: String,
    pub type_uuids: Vec<TypeUuid>,
    pub bytes: Vec<u8>,
}

/// One auxiliary debug payload to commit (§13: pins and evicts with the
/// result, never appears in derived-output rows, manifests, or load-dep
/// closures).
#[derive(Debug, Clone)]
pub struct AuxSpec {
    pub debug_key: String,
    pub bytes: Vec<u8>,
}

/// A build's outcome at commit time. Transient infrastructure errors are
/// deliberately unrepresentable: they are typed apart and never memoized
/// (§13) — there is no API to write one.
#[derive(Debug, Clone)]
pub enum CommitOutcome {
    Success {
        payload_kind: PayloadKind,
        outputs: Vec<OutputSpec>,
        aux: Vec<AuxSpec>,
    },
    Failure {
        /// The terminal cause (§9, §13): `Op` when the trace's own
        /// terminal entry is the failing operation, `Local` for a
        /// deterministic local failure no operation produced.
        cause: FailureCause,
    },
}

/// One build result to commit (§13): payload records first, one result
/// record last, one memo transaction for the index rows.
#[derive(Debug, Clone)]
pub struct BuildCommit {
    pub key_kind: KeyKind,
    /// The `"DSSI"` StaticInputs digest, the `"DSBI"` pre-key digest or the
    /// `"DSNK"` node key.
    pub static_input_key: [u8; 32],
    /// The parent asset (processor results) or entry (build imports).
    pub asset_uuid: AssetUuid,
    /// The full canonical `StaticInputs` encoding for index rebuild
    /// (DSSI); empty for DSBI.
    pub static_inputs_canonical: Vec<u8>,
    /// Canonical trace-op bytes, §9's encoding (up to and including the
    /// failing op for failures).
    pub trace: Vec<u8>,
    pub outcome: CommitOutcome,
    /// Canonical DSWL bodies of the wire trees the outputs name, committed
    /// with the result. A tree already in the CAS may be left out.
    pub wire_trees: Vec<Vec<u8>>,
}

/// What a commit returns.
#[derive(Debug, Clone)]
pub struct CommitReceipt {
    pub memo_seq: MemoSeq,
    pub trace_digest: [u8; 32],
    /// `output_key → ContentHash` for every committed output.
    pub outputs: Vec<(String, ContentHash)>,
    /// `debug_key → ContentHash` for every committed aux payload.
    pub aux: Vec<(String, ContentHash)>,
}

/// Where one bucket candidate's record lies (§13), by its row in
/// `result_candidates`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateRow {
    pub trace_digest: [u8; 32],
    pub memo_seq: MemoSeq,
    segment: u64,
    offset: u64,
    len: u64,
}

/// A candidate bucket's rows, newest first (the primary key).
pub(crate) const CANDIDATE_ROWS: &str =
    "SELECT trace_digest, memo_seq, segment, offset, len FROM result_candidates
     WHERE key_kind = ?1 AND static_key = ?2 ORDER BY memo_seq DESC";

/// One bucket candidate, most-recently-committed-first (§13).
#[derive(Debug, Clone)]
pub struct Candidate {
    pub trace_digest: [u8; 32],
    pub memo_seq: MemoSeq,
    pub asset_uuid: AssetUuid,
    pub payload: ResultPayload,
    pub segment: u64,
    pub offset: u64,
}

/// This writer's CAS append state. Every writer appends to a segment of
/// its own, so no two writers share a file offset. Which segment that is
/// lives in `cas_segments` alone: the newest open regular segment whose
/// `owner` is this writer ([`ACTIVE_SEGMENT`]), read in the transaction
/// that appends. A transaction or savepoint that rolls back takes its
/// allocations with it, and the next append reads what survived.
#[derive(Debug)]
pub(crate) struct CasInner {
    pub(crate) dir: PathBuf,
    /// This writer's `cas_segments.owner`: unique among the process's
    /// writers. Startup recovery seals every open segment, so no open
    /// segment of an earlier process carries it either.
    pub(crate) owner: i64,
}

impl CasInner {
    pub(crate) fn new(dir: PathBuf) -> Self {
        static NEXT_OWNER: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);
        Self {
            dir,
            owner: NEXT_OWNER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }
}

/// The segment writer `?1` appends to: its newest open regular segment.
/// The state and kind are literals so the plan walks the open segments'
/// partial index backwards.
pub(crate) const ACTIVE_SEGMENT: &str = "SELECT segment_id FROM cas_segments
     WHERE owner = ?1 AND state = 0 AND segment_kind = 0
     ORDER BY segment_id DESC LIMIT 1";
const _: () = assert!(SegmentKind::Regular as i64 == 0);

/// Seal the open segments writer `?1` allocated below id `?2`. The states
/// are literals so the plan can walk the open segments' partial index.
pub(crate) const SEAL_OWN_SEGMENTS: &str = "UPDATE cas_segments SET state = 1
     WHERE owner = ?1 AND segment_id < ?2 AND state = 0";
const _: () = assert!(SEGMENT_OPEN == 0 && SEGMENT_SEALED == 1);

/// `cas_segments.state`: a writer may still append.
pub(crate) const SEGMENT_OPEN: i64 = 0;
/// `cas_segments.state`: no writer appends any more.
pub(crate) const SEGMENT_SEALED: i64 = 1;
/// `cas_segments.state`: nothing in the index points here; the file goes
/// once no read can still reach it.
pub(crate) const SEGMENT_DEAD: i64 = 2;

pub(crate) fn segment_file_name(id: u64, kind: SegmentKind) -> String {
    let prefix = match kind {
        SegmentKind::Regular => "seg",
        SegmentKind::Oversize => "oversize",
    };
    format!("{prefix}-{id:016x}.dsr")
}

pub(crate) fn parse_segment_id(name: &str, kind: SegmentKind) -> Option<u64> {
    let prefix = match kind {
        SegmentKind::Regular => "seg-",
        SegmentKind::Oversize => "oversize-",
    };
    let hex = name.strip_prefix(prefix)?.strip_suffix(".dsr")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Options for opening a segment file. On Windows a segment is opened with
/// `FILE_SHARE_DELETE`, so the sweeper can delete a dead segment a slow
/// reader still has open.
pub(crate) fn segment_open_options() -> std::fs::OpenOptions {
    #[allow(unused_mut)]
    let mut options = std::fs::OpenOptions::new();
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
        options.share_mode(0x1 | 0x2 | 0x4);
    }
    options
}

/// Read a whole segment file.
pub(crate) fn read_segment(path: &std::path::Path) -> Result<Vec<u8>, StoreError> {
    use std::io::Read;
    let mut bytes = Vec::new();
    segment_open_options()
        .read(true)
        .open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(io_err(path))?;
    Ok(bytes)
}

/// fsync a directory: segment creation and deletion also fsync the
/// directory (§13). Windows has no directory fsync (a directory opens only
/// with backup semantics, and NTFS journals its entries); there it is a
/// no-op.
#[cfg(windows)]
pub fn fsync_dir(_dir: &std::path::Path) -> Result<(), StoreError> {
    Ok(())
}

/// fsync a directory: segment creation and deletion also fsync the
/// directory (§13).
#[cfg(not(windows))]
pub fn fsync_dir(dir: &std::path::Path) -> Result<(), StoreError> {
    let f = std::fs::File::open(dir).map_err(|source| StoreError::Io {
        path: dir.to_path_buf(),
        source,
    })?;
    f.sync_all().map_err(|source| StoreError::Io {
        path: dir.to_path_buf(),
        source,
    })
}

fn io_err(path: &std::path::Path) -> impl Fn(std::io::Error) -> StoreError + '_ {
    move |source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// The `cas_refs` holder of one result.
pub(crate) fn result_holder(
    key_kind: KeyKind,
    static_key: &[u8; 32],
    trace_digest: &[u8; 32],
) -> Vec<u8> {
    let mut holder = Vec::with_capacity(65);
    holder.push(key_kind as u8);
    holder.extend_from_slice(static_key);
    holder.extend_from_slice(trace_digest);
    holder
}

pub(crate) const HOLDER_RESULT: i64 = 0;
pub(crate) const HOLDER_INSTALLED: i64 = 1;

pub(crate) fn insert_ref(
    txn: &rusqlite::Connection,
    holder_kind: i64,
    holder: &[u8],
    hash: &[u8; 32],
) -> Result<(), StoreError> {
    txn.execute(
        "INSERT OR IGNORE INTO cas_refs(holder_kind, holder, content_hash) VALUES (?1, ?2, ?3)",
        rusqlite::params![holder_kind, holder, hash.as_slice()],
    )?;
    Ok(())
}

pub(crate) fn extent_exists(txn: &rusqlite::Connection, hash: &[u8; 32]) -> Result<bool, StoreError> {
    use rusqlite::OptionalExtension;
    Ok(txn
        .query_row(
            "SELECT 1 FROM cas_extents WHERE content_hash = ?1",
            [hash.as_slice()],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// The wire tree an artifact's bytes name, if they are an artifact.
pub(crate) fn artifact_layout(bytes: &[u8]) -> Option<[u8; 32]> {
    use distill_wire::artifact::{parse_artifact, ARTIFACT_MAGIC};
    if !bytes.starts_with(&ARTIFACT_MAGIC) {
        return None;
    }
    parse_artifact(bytes).ok().map(|view| view.layout_hash.0)
}

/// Validate a canonical DSWL body and return its LayoutHash and the
/// digest preimage the CAS stores.
fn wire_tree_preimage(tree_bytes: &[u8]) -> Result<(LayoutHash, Vec<u8>), StoreError> {
    let root = decode_dswl(tree_bytes).map_err(|error| StoreError::InvalidWireTree {
        detail: format!("invalid canonical body: {error:?}"),
    })?;
    let canonical = dswl_bytes(&root).map_err(|error| StoreError::InvalidWireTree {
        detail: format!("cannot re-encode body: {error}"),
    })?;
    if canonical != tree_bytes {
        return Err(StoreError::InvalidWireTree {
            detail: "decoded body does not reproduce byte-for-byte".to_owned(),
        });
    }
    let layout_hash = dswl_hash(&root).map_err(|error| StoreError::InvalidWireTree {
        detail: format!("cannot authenticate body: {error}"),
    })?;
    // The generic CAS authenticates raw payload bytes. Persist the exact
    // DSWL digest preimage so its raw blake3 is the semantic LayoutHash;
    // typed reads strip the domain/version prefix before serving DSWL.
    let mut preimage = Vec::with_capacity(5 + tree_bytes.len());
    preimage.extend_from_slice(b"DSWL");
    preimage.push(DSWL_VERSION);
    preimage.extend_from_slice(tree_bytes);
    debug_assert_eq!(*blake3::hash(&preimage).as_bytes(), layout_hash.0);
    Ok((layout_hash, preimage))
}

/// One appended record group: where each record landed, and the length of
/// every touched segment after the append.
struct Appended {
    locations: Vec<(u64, u64)>,
    touched: Vec<(u64, u64)>,
}

impl Store {
    /// Allocate a segment in the open write transaction: its row, then its
    /// file, fsynced with the directory, before the row can commit. A
    /// rolled-back allocation leaves a file no row names: the next
    /// allocation of its id truncates it, and startup recovery deletes it.
    /// No writer appends to it meanwhile, since writers find their segment
    /// by its row ([`ACTIVE_SEGMENT`]).
    pub(crate) fn create_segment(&mut self, kind: SegmentKind) -> Result<u64, StoreError> {
        debug_assert!(!self.conn.is_autocommit(), "segments are allocated in a write transaction");
        let id = meta_get_u64(&self.conn, "next_segment_id")?.unwrap_or(0);
        meta_set_u64(&self.conn, "next_segment_id", id + 1)?;
        let name = segment_file_name(id, kind);
        self.conn.execute(
            "INSERT INTO cas_segments(segment_id, file_name, segment_kind, indexed_len, state, owner)
             VALUES (?1, ?2, ?3, 0, ?4, ?5)",
            rusqlite::params![id as i64, name, kind as i64, SEGMENT_OPEN, self.cas.owner],
        )?;
        let path = self.cas.dir.join(&name);
        let f = std::fs::File::create(&path).map_err(io_err(&path))?;
        f.sync_all().map_err(io_err(&path))?;
        fsync_dir(&self.cas.dir)?;
        Ok(id)
    }

    /// Seal this writer's open segments: it stops appending to them, for
    /// good.
    pub fn seal_active(&mut self) -> Result<(), StoreError> {
        self.write_txn(|store| {
            store
                .conn
                .prepare_cached(SEAL_OWN_SEGMENTS)?
                .execute(rusqlite::params![store.cas.owner, i64::MAX])?;
            Ok(())
        })
    }

    /// The segment this writer appends to and its length, if it has one
    /// open ([`ACTIVE_SEGMENT`]). The length is the file's: bytes a
    /// rolled-back transaction appended are dead space before the next
    /// record.
    fn active_segment(&self) -> Result<Option<(u64, u64)>, StoreError> {
        use rusqlite::OptionalExtension;
        let Some(id) = self
            .conn
            .prepare_cached(ACTIVE_SEGMENT)?
            .query_row([self.cas.owner], |row| row.get::<_, i64>(0))
            .optional()?
        else {
            return Ok(None);
        };
        let path = self
            .cas
            .dir
            .join(segment_file_name(id as u64, SegmentKind::Regular));
        let len = std::fs::metadata(&path).map_err(io_err(&path))?.len();
        Ok(Some((id as u64, len)))
    }

    /// Append a group of encoded records in the open write transaction,
    /// and fsync each touched segment once, in first-write order. The
    /// group's index rows commit with that transaction, which is the
    /// group's commit: bytes past a segment's `indexed_len` belong to no
    /// committed group. A regular record goes to this writer's segment,
    /// rolling at the size cap (§18's `cas.segment_size`); the new
    /// segment's allocation seals every older one this writer has open. A
    /// record larger than the cap gets an oversize segment of its own.
    fn append_records(&mut self, encoded: &[Vec<u8>]) -> Result<Appended, StoreError> {
        debug_assert!(!self.conn.is_autocommit(), "records are appended in a write transaction");
        let mut locations = Vec::with_capacity(encoded.len());
        let mut touched: Vec<(u64, SegmentKind, u64)> = Vec::new();
        let mut active = self.active_segment()?;
        for bytes in encoded {
            let len = bytes.len() as u64;
            let (segment, kind, offset) = if len > self.config.segment_size {
                (self.create_segment(SegmentKind::Oversize)?, SegmentKind::Oversize, 0)
            } else {
                let (id, offset) = match active {
                    Some((id, end)) if end == 0 || end + len <= self.config.segment_size => {
                        (id, end)
                    }
                    _ => {
                        let id = self.create_segment(SegmentKind::Regular)?;
                        self.conn
                            .prepare_cached(SEAL_OWN_SEGMENTS)?
                            .execute(rusqlite::params![self.cas.owner, id as i64])?;
                        (id, 0)
                    }
                };
                active = Some((id, offset + len));
                (id, SegmentKind::Regular, offset)
            };
            let path = self.cas.dir.join(segment_file_name(segment, kind));
            let mut f = segment_open_options()
                .append(true)
                .open(&path)
                .map_err(io_err(&path))?;
            f.write_all(bytes).map_err(io_err(&path))?;
            match touched.iter_mut().find(|(id, _, _)| *id == segment) {
                Some(entry) => entry.2 = offset + len,
                None => touched.push((segment, kind, offset + len)),
            }
            locations.push((segment, offset));
        }
        for (segment, kind, _) in &touched {
            let path = self.cas.dir.join(segment_file_name(*segment, *kind));
            let f = segment_open_options()
                .write(true)
                .open(&path)
                .map_err(io_err(&path))?;
            f.sync_all().map_err(io_err(&path))?;
        }
        Ok(Appended {
            locations,
            touched: touched
                .into_iter()
                .map(|(segment, _, len)| (segment, len))
                .collect(),
        })
    }

    /// Store `payload` as the extent `hash`, held by the installed holder
    /// `hash`, and run `also`, in one write transaction: the extent is
    /// appended only when the index does not hold it.
    fn put_installed(
        &mut self,
        hash: [u8; 32],
        record: Record,
        also: impl FnOnce(&mut Store) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let payload_offset = RECORD_HEADER_LEN as u64
            + record.static_input_key.len() as u64
            + record.output_key.len() as u64;
        self.write_txn(|store| {
            if !extent_exists(&store.conn, &hash)? {
                let appended = store.append_records(std::slice::from_ref(&encode_record(&record)))?;
                let (segment, offset) = appended.locations[0];
                upsert_extent(
                    &store.conn,
                    &hash,
                    segment,
                    offset + payload_offset,
                    record.payload.len() as u64,
                )?;
                index_segments(&store.conn, &appended.touched)?;
            }
            insert_ref(&store.conn, HOLDER_INSTALLED, &hash, &hash)?;
            also(store)
        })
    }

    /// Install a wire tree (§13: a first-class CAS record). Idempotent:
    /// duplicate content is byte-identical by definition. A build commit
    /// carries its own wire trees ([`BuildCommit::wire_trees`]).
    pub fn put_wire_tree(&mut self, tree_bytes: &[u8]) -> Result<LayoutHash, StoreError> {
        let (layout_hash, preimage) = wire_tree_preimage(tree_bytes)?;
        let record = Record {
            kind: RecordKind::WireTree,
            asset_uuid: AssetUuid([0u8; 16]),
            static_input_key: Vec::new(),
            output_key: String::new(),
            payload: preimage,
        };
        self.put_installed(layout_hash.0, record, |_| Ok(()))?;
        Ok(layout_hash)
    }

    /// Store raw artifact bytes outside a build (embedded RPC stores and
    /// explicit installs), indexed by their blake3 content hash, together
    /// with the artifact's typed direct load edges. Idempotent. The install
    /// also holds the artifact's wire tree when the CAS has it.
    pub fn put_artifact(
        &mut self,
        asset: AssetUuid,
        bytes: &[u8],
        load_edges: &[(AssetUuid, distill_core::id::TypeUuid)],
    ) -> Result<ContentHash, StoreError> {
        use crate::served::ServedWrite;
        let hash = ContentHash(*blake3::hash(bytes).as_bytes());
        let layout = artifact_layout(bytes);
        let record = Record {
            kind: RecordKind::ProcessorOutput,
            asset_uuid: asset,
            static_input_key: Vec::new(),
            output_key: String::new(),
            payload: bytes.to_vec(),
        };
        self.put_installed(hash.0, record, |store| {
            if let Some(layout) = layout {
                if extent_exists(&store.conn, &layout)? {
                    insert_ref(&store.conn, HOLDER_INSTALLED, &hash.0, &layout)?;
                }
            }
            store.served_transaction(|txn| txn.record_artifact_load_edges(hash, load_edges))
        })?;
        Ok(hash)
    }

    /// Commit one build result (§13), in one write transaction (the
    /// caller's, when one is open): payloads first, the result record
    /// last, one fsync per touched segment in record order, then the
    /// extent rows, the result's references and the candidate-bucket row.
    /// Advances only the memo sequence. The group is committed exactly when
    /// that transaction is.
    ///
    /// Every wire tree an output artifact names must be in
    /// `commit.wire_trees` or already in the CAS.
    pub fn commit_build(&mut self, commit: BuildCommit) -> Result<CommitReceipt, StoreError> {
        // Shape checks before any byte lands.
        if let CommitOutcome::Success { outputs, .. } = &commit.outcome {
            if commit.key_kind == KeyKind::BuildImport && outputs.len() != 1 {
                return Err(StoreError::BuildImportOutputArity { got: outputs.len() });
            }
        }
        let mut trees = Vec::with_capacity(commit.wire_trees.len());
        for tree in &commit.wire_trees {
            trees.push(wire_tree_preimage(tree)?);
        }

        // Build the record group.
        let mut records: Vec<Record> = Vec::new();
        let mut output_rows: Vec<OutputRow> = Vec::new();
        let mut aux_rows: Vec<AuxRow> = Vec::new();
        let mut layouts: Vec<[u8; 32]> = Vec::new();
        let outcome = match &commit.outcome {
            CommitOutcome::Success {
                payload_kind,
                outputs,
                aux,
            } => {
                for out in outputs {
                    output_rows.push(OutputRow {
                        output_key: out.output_key.clone(),
                        type_uuids: out.type_uuids.clone(),
                        content_hash: ContentHash(*blake3::hash(&out.bytes).as_bytes()),
                    });
                    layouts.extend(artifact_layout(&out.bytes));
                    records.push(Record {
                        kind: payload_kind.record_kind(),
                        asset_uuid: commit.asset_uuid,
                        static_input_key: Vec::new(),
                        output_key: out.output_key.clone(),
                        payload: out.bytes.clone(),
                    });
                }
                for a in aux {
                    aux_rows.push(AuxRow {
                        debug_key: a.debug_key.clone(),
                        content_hash: ContentHash(*blake3::hash(&a.bytes).as_bytes()),
                    });
                    records.push(Record {
                        kind: RecordKind::Debug,
                        asset_uuid: commit.asset_uuid,
                        static_input_key: Vec::new(),
                        output_key: a.debug_key.clone(),
                        payload: a.bytes.clone(),
                    });
                }
                ResultOutcome::Success {
                    outputs: output_rows.clone(),
                    aux: aux_rows.clone(),
                }
            }
            CommitOutcome::Failure { cause } => ResultOutcome::Failure {
                cause: cause.clone(),
            },
        };
        let result_payload = ResultPayload {
            key_kind: commit.key_kind,
            static_inputs_canonical: commit.static_inputs_canonical.clone(),
            trace: commit.trace.clone(),
            outcome,
        };
        let trace_digest = result_payload.trace_digest();
        let result_record = Record {
            kind: RecordKind::Result,
            asset_uuid: commit.asset_uuid,
            static_input_key: commit.static_input_key.to_vec(),
            output_key: String::new(),
            payload: result_payload.encode(),
        };
        let key_kind = commit.key_kind;
        let static_key = commit.static_input_key;
        let holder = result_holder(key_kind, &static_key, &trace_digest);
        let mut unit: Vec<[u8; 32]> = output_rows.iter().map(|row| row.content_hash.0).collect();
        unit.extend(aux_rows.iter().map(|row| row.content_hash.0));
        unit.extend(layouts.iter().copied());

        // A wire tree no output names would be indexed with nothing
        // referencing it: it is not committed.
        trees.retain(|(hash, _)| layouts.contains(&hash.0));
        self.write_txn(|store| {
            // Wire trees and payloads the index holds are not appended
            // again: a node result names the bytes its last stage committed.
            let mut group: Vec<Record> = Vec::new();
            for (hash, preimage) in &trees {
                if !extent_exists(&store.conn, &hash.0)? {
                    group.push(Record {
                        kind: RecordKind::WireTree,
                        asset_uuid: AssetUuid([0u8; 16]),
                        static_input_key: Vec::new(),
                        output_key: String::new(),
                        payload: preimage.clone(),
                    });
                }
            }
            for record in &records {
                if !extent_exists(&store.conn, blake3::hash(&record.payload).as_bytes())? {
                    group.push(record.clone());
                }
            }
            for hash in &layouts {
                if !extent_exists(&store.conn, hash)?
                    && !trees.iter().any(|(tree, _)| tree.0 == *hash)
                {
                    return Err(StoreError::MissingWireTree { hash: *hash });
                }
            }
            group.push(result_record.clone());
            let encoded: Vec<Vec<u8>> = group.iter().map(encode_record).collect();
            let appended = store.append_records(&encoded)?;
            let result_index = group.len() - 1;
            let (result_segment, result_offset) = appended.locations[result_index];
            let result_len = encoded[result_index].len() as u64;

            #[cfg(test)]
            if let Some(hook) = store.before_commit.as_mut() {
                hook();
            }
            let ((), memo_seq) = store.memo_transaction(|txn, memo_seq| {
                for (i, rec) in group.iter().enumerate().take(result_index) {
                    let (segment, offset) = appended.locations[i];
                    let payload_offset = offset
                        + RECORD_HEADER_LEN as u64
                        + rec.static_input_key.len() as u64
                        + rec.output_key.len() as u64;
                    let hash = *blake3::hash(&rec.payload).as_bytes();
                    upsert_extent(txn, &hash, segment, payload_offset, rec.payload.len() as u64)?;
                }
                upsert_candidate(
                    txn,
                    key_kind,
                    &static_key,
                    &trace_digest,
                    memo_seq,
                    result_segment,
                    result_offset,
                    result_len,
                )?;
                for hash in &unit {
                    insert_ref(txn, HOLDER_RESULT, &holder, hash)?;
                }
                index_segments(txn, &appended.touched)?;
                Ok(())
            })?;
            Ok(CommitReceipt {
                memo_seq,
                trace_digest,
                outputs: output_rows
                    .iter()
                    .map(|row| (row.output_key.clone(), row.content_hash))
                    .collect(),
                aux: aux_rows
                    .iter()
                    .map(|row| (row.debug_key.clone(), row.content_hash))
                    .collect(),
            })
        })
    }
}

/// Count one change to the CAS index (`store_meta.cas_writes`), in its write
/// transaction: the CAS pass skips its sweeps while the count stands still.
pub(crate) fn count_cas_write(txn: &rusqlite::Connection) -> Result<(), StoreError> {
    txn.prepare_cached(
        "INSERT INTO store_meta(key, value) VALUES ('cas_writes', 1)
         ON CONFLICT(key) DO UPDATE SET value = value + 1",
    )?
    .execute([])?;
    Ok(())
}

/// Record how far each touched segment is indexed, and seal an oversize
/// segment once its one record is.
fn index_segments(txn: &rusqlite::Connection, touched: &[(u64, u64)]) -> Result<(), StoreError> {
    count_cas_write(txn)?;
    for (segment, len) in touched {
        txn.execute(
            "UPDATE cas_segments SET indexed_len = ?2,
               state = CASE WHEN segment_kind = ?3 THEN ?4 ELSE state END
             WHERE segment_id = ?1",
            rusqlite::params![
                *segment as i64,
                *len as i64,
                SegmentKind::Oversize as i64,
                SEGMENT_SEALED
            ],
        )?;
    }
    Ok(())
}

impl StoreReader {
    /// How many changes the CAS index has counted ([`count_cas_write`]):
    /// an unchanged count is an unchanged CAS index. One primary-key read.
    pub fn cas_writes(&self) -> Result<u64, StoreError> {
        Ok(crate::db::meta_get_u64(&self.conn, "cas_writes")?.unwrap_or(0))
    }

    /// The candidate bucket for a static-input key, most recently
    /// committed first (§13), each candidate read and decoded. A pure read;
    /// a caller that stops at the first candidate that holds reads the
    /// bucket's rows by [`Self::candidate_rows`] and each record by
    /// [`Self::read_candidate`] as it reaches it.
    pub fn lookup_candidates(
        &self,
        key_kind: KeyKind,
        static_key: &[u8; 32],
    ) -> Result<Vec<Candidate>, StoreError> {
        let mut out = Vec::new();
        for row in self.candidate_rows(key_kind, static_key)? {
            out.extend(self.read_candidate(&row)?);
        }
        Ok(out)
    }

    /// The candidate bucket's rows for a static-input key, most recently
    /// committed first: where each record lies, nothing read from it.
    pub fn candidate_rows(
        &self,
        key_kind: KeyKind,
        static_key: &[u8; 32],
    ) -> Result<Vec<CandidateRow>, StoreError> {
        let mut stmt = self.conn.prepare_cached(CANDIDATE_ROWS)?;
        let rows = stmt.query_map(
            rusqlite::params![key_kind as i64, static_key.as_slice()],
            |r| {
                Ok(CandidateRow {
                    trace_digest: crate::bundles::blob32(r.get(0)?),
                    memo_seq: MemoSeq(r.get::<_, i64>(1)? as u64),
                    segment: r.get::<_, i64>(2)? as u64,
                    offset: r.get::<_, i64>(3)? as u64,
                    len: r.get::<_, i64>(4)? as u64,
                })
            },
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The candidate `row` locates, read and decoded; `None` when its
    /// segment was deleted since the rows were read (a cache miss).
    pub fn read_candidate(&self, row: &CandidateRow) -> Result<Option<Candidate>, StoreError> {
        let bytes = match self.read_extent(row.segment, row.offset, row.len) {
            Ok(bytes) => bytes,
            Err(StoreError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None)
            }
            Err(error) => return Err(error),
        };
        let decoded: DecodedRecord = decode_record(&bytes, row.segment, row.offset)?;
        let payload = ResultPayload::decode(&decoded.record.payload)?;
        Ok(Some(Candidate {
            trace_digest: row.trace_digest,
            memo_seq: row.memo_seq,
            asset_uuid: decoded.record.asset_uuid,
            payload,
            segment: row.segment,
            offset: row.offset,
        }))
    }

    pub(crate) fn segment_path(&self, segment_id: u64) -> PathBuf {
        // `cas_segments` names every segment that can hold an indexed
        // extent; a location whose row is gone names a deleted segment, and
        // its read fails as a cache miss.
        use rusqlite::OptionalExtension;
        let name: Option<String> = self
            .conn
            .query_row(
                "SELECT file_name FROM cas_segments WHERE segment_id = ?1",
                [segment_id as i64],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten();
        self.config
            .state_path
            .join("cas")
            .join(name.unwrap_or_else(|| segment_file_name(segment_id, SegmentKind::Regular)))
    }

    pub(crate) fn extent_of(&self, hash: &[u8; 32]) -> Result<Option<(u64, u64, u64)>, StoreError> {
        use rusqlite::OptionalExtension;
        Ok(self
            .conn
            .query_row(
                "SELECT segment, offset, len FROM cas_extents WHERE content_hash = ?1",
                [hash.as_slice()],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)? as u64,
                        r.get::<_, i64>(1)? as u64,
                        r.get::<_, i64>(2)? as u64,
                    ))
                },
            )
            .optional()?)
    }

    /// Read an artifact (or wire tree) by hash: extent lookup, read,
    /// verify against the requested hash — corruption is caught, never
    /// returned. Valid under namespace errors and pipeline failures (§13's
    /// pure-metadata classification).
    pub fn cas_read(&self, hash: &[u8; 32]) -> Result<Vec<u8>, StoreError> {
        let (segment, offset, len) = self
            .extent_of(hash)?
            .ok_or(StoreError::NotFound { hash: *hash })?;
        // A segment deleted since the lookup is a cache miss.
        let bytes = match self.read_extent(segment, offset, len) {
            Err(StoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                return Err(StoreError::NotFound { hash: *hash })
            }
            bytes => bytes?,
        };
        if *blake3::hash(&bytes).as_bytes() != *hash {
            return Err(StoreError::CorruptExtent { segment, offset });
        }
        Ok(bytes)
    }

    /// Read a first-class wire tree by its semantic LayoutHash, returning
    /// the canonical DSWL body (without the stored digest preimage prefix).
    pub fn wire_tree_read(&self, hash: LayoutHash) -> Result<Vec<u8>, StoreError> {
        let preimage = self.cas_read(&hash.0)?;
        let Some(body) = preimage.strip_prefix(b"DSWL") else {
            return Err(StoreError::InvalidWireTree {
                detail: "stored extent lacks the DSWL domain prefix".to_owned(),
            });
        };
        let Some((&version, body)) = body.split_first() else {
            return Err(StoreError::InvalidWireTree {
                detail: "stored extent lacks the DSWL version".to_owned(),
            });
        };
        if version != DSWL_VERSION {
            return Err(StoreError::InvalidWireTree {
                detail: format!("stored DSWL version {version} is unsupported"),
            });
        }
        let root = decode_dswl(body).map_err(|error| StoreError::InvalidWireTree {
            detail: format!("stored body is invalid: {error:?}"),
        })?;
        let observed = dswl_hash(&root).map_err(|error| StoreError::InvalidWireTree {
            detail: format!("stored body cannot be authenticated: {error}"),
        })?;
        if observed != hash {
            return Err(StoreError::InvalidWireTree {
                detail: format!("requested {hash:?}, observed {observed:?}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Full doctor verification of every indexed CAS extent. Each record is
    /// read through the ordinary hash-checking path, so an index/segment drift
    /// cannot be reported healthy merely because its framing still decodes.
    pub fn verify_all_cas_extents(&self) -> Result<usize, StoreError> {
        let hashes = {
            let mut statement = self
                .conn
                .prepare("SELECT content_hash FROM cas_extents ORDER BY content_hash")?;
            let hashes = statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                .map(|row| {
                    let bytes = row?;
                    bytes.try_into().map_err(|_| rusqlite::Error::InvalidQuery)
                })
                .collect::<Result<Vec<[u8; 32]>, _>>()?;
            hashes
        };
        for hash in &hashes {
            self.cas_read(hash)?;
        }
        Ok(hashes.len())
    }

    pub(crate) fn read_extent(
        &self,
        segment: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StoreError> {
        use std::io::{Read, Seek, SeekFrom};
        let path = self.segment_path(segment);
        let mut f = segment_open_options()
            .read(true)
            .open(&path)
            .map_err(io_err(&path))?;
        f.seek(SeekFrom::Start(offset))
            .map_err(io_err(&path))?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf).map_err(io_err(&path))?;
        Ok(buf)
    }
}

pub(crate) fn upsert_extent(
    txn: &rusqlite::Connection,
    hash: &[u8; 32],
    segment: u64,
    offset: u64,
    len: u64,
) -> Result<(), StoreError> {
    txn.execute(
        "INSERT INTO cas_extents(content_hash, segment, offset, len) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(content_hash) DO UPDATE SET
           segment = excluded.segment, offset = excluded.offset, len = excluded.len",
        rusqlite::params![hash.as_slice(), segment as i64, offset as i64, len as i64],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn upsert_candidate(
    txn: &rusqlite::Connection,
    key_kind: KeyKind,
    static_key: &[u8; 32],
    trace_digest: &[u8; 32],
    memo_seq: MemoSeq,
    segment: u64,
    offset: u64,
    len: u64,
) -> Result<(), StoreError> {
    txn.execute(
        "INSERT INTO result_candidates(key_kind, static_key, trace_digest, memo_seq,
                                       segment, offset, len)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(key_kind, static_key, trace_digest) DO UPDATE SET
           memo_seq = excluded.memo_seq, segment = excluded.segment,
           offset = excluded.offset, len = excluded.len",
        rusqlite::params![
            key_kind as i64,
            static_key.as_slice(),
            trace_digest.as_slice(),
            memo_seq.0 as i64,
            segment as i64,
            offset as i64,
            len as i64,
        ],
    )?;
    Ok(())
}

// ---- derived-output namespace (input-versioned, §9/§13) ----

impl crate::db::InputTxn<'_> {
    /// Replace-style publications clear the previous version's projection
    /// before installing the successor rows in the same input transaction.
    pub fn clear_derived_outputs(&mut self) -> Result<(), StoreError> {
        self.txn.execute("DELETE FROM derived_outputs", [])?;
        Ok(())
    }

    /// Publish one derived-output namespace row: `child uuid → (parent
    /// uuid, output key)` — derived per published version from its
    /// assets × pinned pipeline map (§9), the only authority for child
    /// resolution.
    pub fn set_derived_output(
        &mut self,
        child: AssetUuid,
        parent: AssetUuid,
        output_key: &str,
        terminal_type: TypeUuid,
    ) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO derived_outputs(child_uuid, parent_uuid, output_key, terminal_type)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(child_uuid) DO UPDATE SET
               parent_uuid = excluded.parent_uuid, output_key = excluded.output_key,
               terminal_type = excluded.terminal_type",
            rusqlite::params![
                child.0.as_slice(),
                parent.0.as_slice(),
                output_key,
                terminal_type.0.as_slice()
            ],
        )?;
        Ok(())
    }

    /// Retire a derived-output namespace row.
    pub fn remove_derived_output(&mut self, child: AssetUuid) -> Result<bool, StoreError> {
        let n = self.txn.execute(
            "DELETE FROM derived_outputs WHERE child_uuid = ?1",
            [child.0.as_slice()],
        )?;
        Ok(n > 0)
    }
}

impl StoreReader {
    /// Raw bounded lookup used while preparing an unpublished successor.
    pub fn derived_output_row(
        &self,
        child: AssetUuid,
    ) -> Result<Option<(AssetUuid, String)>, StoreError> {
        use rusqlite::OptionalExtension;
        Ok(self
            .conn
            .query_row(
                "SELECT parent_uuid, output_key FROM derived_outputs WHERE child_uuid = ?1",
                [child.0.as_slice()],
                |row| {
                    Ok((
                        AssetUuid(crate::bundles::blob16(row.get::<_, Vec<u8>>(0)?)),
                        row.get::<_, String>(1)?,
                    ))
                },
            )
            .optional()?)
    }

    pub fn all_derived_outputs(&self) -> Result<Vec<(AssetUuid, AssetUuid, String)>, StoreError> {
        let mut statement = self.conn.prepare(
            "SELECT child_uuid, parent_uuid, output_key FROM derived_outputs ORDER BY child_uuid",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (child, parent, key) = row?;
            Ok((
                AssetUuid(crate::bundles::blob16(child)),
                AssetUuid(crate::bundles::blob16(parent)),
                key,
            ))
        })
        .collect()
    }

    /// Resolve a derived child through the namespace index — the only
    /// authority (§9): historical result records never resurrect a
    /// retired child.
    pub fn resolve_child(
        &self,
        child: AssetUuid,
    ) -> Result<Option<(AssetUuid, String)>, StoreError> {
        use rusqlite::OptionalExtension;
        Ok(self
            .conn
            .query_row(
                "SELECT parent_uuid, output_key FROM derived_outputs WHERE child_uuid = ?1",
                [child.0.as_slice()],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?
            .map(|(p, k)| (AssetUuid(crate::bundles::blob16(p)), k)))
    }
}
