//! Append/commit/read machinery for the log-structured CAS (§13).
//!
//! A segment holds content-addressed bytes only: each extent is the raw
//! bytes its hash names. What a build result says about them is its
//! `results` and `result_outputs` rows.
//!
//! Write order (pinned): append the bytes the index does not hold yet,
//! fsync each touched segment, then insert the index rows, all in one
//! write transaction. **That transaction is the commit** — a rollback or
//! a crash before COMMIT publishes nothing, and recovery cuts the bytes it
//! appended (past the segment's `indexed_len`). Segment creation and
//! deletion also fsync the directory.
//!
//! Each writer appends to a segment of its own, rolling at the size cap.
//! An extent larger than the cap is instead written alone in a typed
//! oversize segment. Writers never coordinate beyond SQLite: a
//! transaction that rolls back leaves its appended bytes as dead space.

use std::io::Write;
use std::path::PathBuf;

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_wire::dswl::{decode_dswl, dswl_bytes, dswl_hash, DSWL_VERSION};

use crate::cas::record::{
    trace_digest, AuxRow, FailureCause, KeyKind, OutputRow, ResultOutcome, ResultPayload,
};
use crate::db::{meta_get_u64, meta_set_u64, Store, StoreReader};
use crate::error::StoreError;
use crate::state::MemoSeq;

/// A segment file's kind. An oversize segment holds exactly one extent.
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

/// One build result to commit (§13): the bytes, then its rows, in one
/// memo transaction.
#[derive(Debug, Clone)]
pub struct BuildCommit {
    pub key_kind: KeyKind,
    /// The `"DSSI"` StaticInputs digest, the `"DSBI"` pre-key digest or the
    /// `"DSNK"` node key.
    pub static_input_key: [u8; 32],
    /// The parent asset (processor results) or entry (build imports).
    pub asset_uuid: AssetUuid,
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

/// One candidate of a bucket (§13): its `results` key, nothing read
/// from it yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateRow {
    pub trace_digest: [u8; 32],
    pub memo_seq: MemoSeq,
    key_kind: KeyKind,
    static_key: [u8; 32],
}

/// A candidate bucket's rows, newest first.
pub(crate) const CANDIDATE_ROWS: &str = "SELECT trace_digest, memo_seq FROM results
     WHERE key_kind = ?1 AND static_key = ?2 ORDER BY memo_seq DESC";

/// One result with what it names, its outputs in role and name order.
pub(crate) const CANDIDATE: &str =
    "SELECT r.asset_uuid, r.trace, r.failure, o.role, o.name, o.types, o.content_hash
     FROM results r LEFT JOIN result_outputs o
       ON o.key_kind = r.key_kind AND o.static_key = r.static_key
      AND o.trace_digest = r.trace_digest
     WHERE r.key_kind = ?1 AND r.static_key = ?2 AND r.trace_digest = ?3
     ORDER BY o.role, o.name";

/// `result_outputs.role`.
pub(crate) const ROLE_OUTPUT: i64 = 0;
pub(crate) const ROLE_AUX: i64 = 1;
pub(crate) const ROLE_WIRE_TREE: i64 = 2;

/// One bucket candidate, most-recently-committed-first (§13).
#[derive(Debug, Clone)]
pub struct Candidate {
    pub trace_digest: [u8; 32],
    pub memo_seq: MemoSeq,
    pub asset_uuid: AssetUuid,
    pub payload: ResultPayload,
}

/// This writer's CAS append state. Every writer appends to a segment of
/// its own, so no two writers share a file offset. Which segment that is
/// lives in `cas_segments` alone: the one open regular segment whose
/// `owner` is this writer ([`ACTIVE_SEGMENT`]; the unique partial index
/// `cas_segments_open` allows no second), read in the transaction that
/// appends. A transaction or savepoint that rolls back takes its
/// allocations with it, and the next append reads what survived.
#[derive(Debug)]
pub(crate) struct CasInner {
    pub(crate) dir: PathBuf,
    /// This writer's `cas_segments.owner`: unique among the process's
    /// writers. Opening the store ends every earlier process's writers,
    /// sealing their open segments, so no open segment of an earlier
    /// process carries it either.
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

/// The segment writer `?1` appends to: its one open regular segment. The
/// state and kind are literals so the plan searches the open segments'
/// unique partial index.
pub(crate) const ACTIVE_SEGMENT: &str = "SELECT segment_id FROM cas_segments
     WHERE owner = ?1 AND state = 0 AND segment_kind = 0";
const _: () = assert!(SegmentKind::Regular as i64 == 0);

/// Every segment, for doctor verification.
pub(crate) const VERIFY_SEGMENTS: &str = "SELECT segment_id, file_name FROM cas_segments";
/// The extents segment `?1` holds.
pub(crate) const VERIFY_SEGMENT_EXTENTS: &str =
    "SELECT content_hash, offset, len FROM cas_extents WHERE segment = ?1";

/// Seal writer `?1`'s open regular segment, when it closes: at most one
/// row, by the unique partial index the literals select.
pub(crate) const SEAL_ACTIVE_SEGMENT: &str = "UPDATE cas_segments SET state = 1
     WHERE owner = ?1 AND state = 0 AND segment_kind = 0";
/// Seal segment `?1`: the segment a writer rolls off, in the transaction
/// that allocates its next one.
pub(crate) const SEAL_SEGMENT: &str = "UPDATE cas_segments SET state = 1 WHERE segment_id = ?1";
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

/// Install `holder` as a holder of the extent `hash`.
pub(crate) fn insert_ref(
    txn: &rusqlite::Connection,
    holder: &[u8; 32],
    hash: &[u8; 32],
) -> Result<(), StoreError> {
    txn.prepare_cached("INSERT OR IGNORE INTO cas_refs(holder, content_hash) VALUES (?1, ?2)")?
        .execute(rusqlite::params![holder.as_slice(), hash.as_slice()])?;
    Ok(())
}

pub(crate) fn extent_exists(
    txn: &rusqlite::Connection,
    hash: &[u8; 32],
) -> Result<bool, StoreError> {
    use rusqlite::OptionalExtension;
    Ok(txn
        .prepare_cached("SELECT 1 FROM cas_extents WHERE content_hash = ?1")?
        .query_row([hash.as_slice()], |_| Ok(()))
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

/// One appended group of extents: where each landed, and the length of
/// every touched segment after the append.
struct Appended {
    locations: Vec<(u64, u64)>,
    touched: Vec<(u64, u64)>,
}

impl Store {
    /// Allocate a segment in `state` in the open write transaction: its row
    /// and `next_segment_id`, then its file, created empty (truncating any
    /// file of that name) and fsynced with the directory, before the row
    /// can commit.
    ///
    /// `next_segment_id` is the allocation's commit point. A file whose id
    /// is at or past it was created by an allocation that rolled back (or
    /// that a crash interrupted): it is uncommitted by definition, no row
    /// names it, no reader reaches it, and the next allocation of its id
    /// truncates it. Nothing scans for such files.
    pub(crate) fn create_segment(
        &mut self,
        kind: SegmentKind,
        state: i64,
    ) -> Result<u64, StoreError> {
        debug_assert!(
            !self.conn.is_autocommit(),
            "segments are allocated in a write transaction"
        );
        let id = meta_get_u64(&self.conn, "next_segment_id")?.unwrap_or(0);
        meta_set_u64(&self.conn, "next_segment_id", id + 1)?;
        let name = segment_file_name(id, kind);
        self.conn
            .prepare_cached(
                "INSERT INTO cas_segments(segment_id, file_name, segment_kind, indexed_len, state, owner)
             VALUES (?1, ?2, ?3, 0, ?4, ?5)",
            )?
            .execute(rusqlite::params![id as i64, name, kind as i64, state, self.cas.owner])?;
        let path = self.cas.dir.join(&name);
        let f = std::fs::File::create(&path).map_err(io_err(&path))?;
        f.sync_all().map_err(io_err(&path))?;
        fsync_dir(&self.cas.dir)?;
        Ok(id)
    }

    /// Seal this writer's open segment: it stops appending to it, for good.
    pub fn seal_active(&mut self) -> Result<(), StoreError> {
        self.write_txn(|store| {
            store
                .conn
                .prepare_cached(SEAL_ACTIVE_SEGMENT)?
                .execute([store.cas.owner])?;
            Ok(())
        })
    }

    /// The segment this writer appends to and its length, if it has one
    /// open ([`ACTIVE_SEGMENT`]). The length is the file's: bytes a
    /// rolled-back transaction appended are dead space before the next
    /// extent.
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

    /// Append a group of extents in the open write transaction, and fsync
    /// each touched segment once, in first-write order. The group's index
    /// rows commit with that transaction, which is the group's commit:
    /// bytes past a segment's `indexed_len` belong to no committed group.
    /// An extent goes to this writer's segment, rolling at the size cap
    /// (§18's `cas.segment_size`): the segment it rolls off is sealed in
    /// the transaction that allocates the next. An extent larger than the
    /// cap gets an oversize segment of its own.
    fn append_extents(&mut self, encoded: &[&[u8]]) -> Result<Appended, StoreError> {
        debug_assert!(
            !self.conn.is_autocommit(),
            "extents are appended in a write transaction"
        );
        if encoded.is_empty() {
            return Ok(Appended {
                locations: Vec::new(),
                touched: Vec::new(),
            });
        }
        let mut locations = Vec::with_capacity(encoded.len());
        let mut touched: Vec<(u64, SegmentKind, u64)> = Vec::new();
        let mut active = self.active_segment()?;
        for bytes in encoded {
            let len = bytes.len() as u64;
            let (segment, kind, offset) = if len > self.config.segment_size {
                let id = self.create_segment(SegmentKind::Oversize, SEGMENT_OPEN)?;
                (id, SegmentKind::Oversize, 0)
            } else {
                let (id, offset) = match active {
                    Some((id, end)) if end == 0 || end + len <= self.config.segment_size => {
                        (id, end)
                    }
                    _ => {
                        if let Some((full, _)) = active {
                            self.conn
                                .prepare_cached(SEAL_SEGMENT)?
                                .execute([full as i64])?;
                        }
                        (self.create_segment(SegmentKind::Regular, SEGMENT_OPEN)?, 0)
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

    /// Store `bytes` as the extent `hash`, held by the installed holder
    /// `hash`, and run `also`, in one write transaction: the extent is
    /// appended only when the index does not hold it.
    fn put_installed(
        &mut self,
        hash: [u8; 32],
        bytes: &[u8],
        also: impl FnOnce(&mut Store) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.write_txn(|store| {
            if !extent_exists(&store.conn, &hash)? {
                let appended = store.append_extents(&[bytes])?;
                let (segment, offset) = appended.locations[0];
                upsert_extent(&store.conn, &hash, segment, offset, bytes.len() as u64)?;
                index_segments(&store.conn, &appended.touched)?;
            }
            insert_ref(&store.conn, &hash, &hash)?;
            also(store)
        })
    }

    /// Install a wire tree (§13: a first-class CAS extent). Idempotent:
    /// duplicate content is byte-identical by definition. A build commit
    /// carries its own wire trees ([`BuildCommit::wire_trees`]).
    pub fn put_wire_tree(&mut self, tree_bytes: &[u8]) -> Result<LayoutHash, StoreError> {
        let (layout_hash, preimage) = wire_tree_preimage(tree_bytes)?;
        self.put_installed(layout_hash.0, &preimage, |_| Ok(()))?;
        Ok(layout_hash)
    }

    /// Store raw artifact bytes outside a build (explicit installs), indexed by their blake3 content hash, together
    /// with the artifact's typed direct load edges. Idempotent. The install
    /// also holds the artifact's wire tree when the CAS has it.
    pub fn put_artifact(
        &mut self,
        bytes: &[u8],
        load_edges: &[(AssetUuid, distill_core::id::TypeUuid)],
    ) -> Result<ContentHash, StoreError> {
        use crate::served::ServedWrite;
        let hash = ContentHash(*blake3::hash(bytes).as_bytes());
        let layout = artifact_layout(bytes);
        self.put_installed(hash.0, bytes, |store| {
            if let Some(layout) = layout {
                if extent_exists(&store.conn, &layout)? {
                    insert_ref(&store.conn, &hash.0, &layout)?;
                }
            }
            store.served_transaction(|txn| txn.record_artifact_load_edges(hash, load_edges))
        })?;
        Ok(hash)
    }

    /// Record the typed direct load edges of `hash`, an extent a result
    /// holds, replacing any earlier ones. The edges go with the extent.
    pub fn record_load_edges(
        &mut self,
        hash: ContentHash,
        load_edges: &[(AssetUuid, distill_core::id::TypeUuid)],
    ) -> Result<(), StoreError> {
        use crate::served::ServedWrite;
        self.write_txn(|store| {
            store.served_transaction(|txn| txn.record_artifact_load_edges(hash, load_edges))
        })
    }

    /// Commit one build result (§13), in one write transaction (the
    /// caller's, when one is open): the bytes not yet in the CAS first,
    /// one fsync per touched segment, then the extent rows, the `results`
    /// row and its `result_outputs` rows. A re-commit of the same key and
    /// trace replaces the earlier result in the same transaction.
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

        // The result's rows, and the bytes they name.
        let mut extents: Vec<([u8; 32], &[u8])> = Vec::new();
        let mut output_rows: Vec<OutputRow> = Vec::new();
        let mut aux_rows: Vec<AuxRow> = Vec::new();
        // (output key, wire tree) for each output that names one.
        let mut layouts: Vec<(String, [u8; 32])> = Vec::new();
        let failure = match &commit.outcome {
            CommitOutcome::Success { outputs, aux } => {
                for out in outputs {
                    let hash = *blake3::hash(&out.bytes).as_bytes();
                    output_rows.push(OutputRow {
                        output_key: out.output_key.clone(),
                        type_uuids: out.type_uuids.clone(),
                        content_hash: ContentHash(hash),
                    });
                    if let Some(layout) = artifact_layout(&out.bytes) {
                        layouts.push((out.output_key.clone(), layout));
                    }
                    extents.push((hash, &out.bytes));
                }
                for a in aux {
                    let hash = *blake3::hash(&a.bytes).as_bytes();
                    aux_rows.push(AuxRow {
                        debug_key: a.debug_key.clone(),
                        content_hash: ContentHash(hash),
                    });
                    extents.push((hash, &a.bytes));
                }
                None
            }
            CommitOutcome::Failure { cause } => Some(cause.encode()),
        };
        let trace_digest = trace_digest(&commit.trace);
        let key_kind = commit.key_kind;
        let static_key = commit.static_input_key;

        // A wire tree no output names would be indexed with nothing
        // holding it: it is not committed.
        trees.retain(|(hash, _)| layouts.iter().any(|(_, layout)| *layout == hash.0));
        self.write_txn(|store| {
            // A result committed again replaces the one it had: what only
            // the old one held goes before the new one is laid out.
            crate::cas::gc::evict_result_rows(&store.conn, key_kind, &static_key, &trace_digest)?;
            // Bytes the index holds are not appended again: a node result
            // names the bytes its last stage committed.
            let mut group: Vec<([u8; 32], &[u8])> = Vec::new();
            for (hash, preimage) in &trees {
                if !extent_exists(&store.conn, &hash.0)?
                    && !group.iter().any(|(held, _)| *held == hash.0)
                {
                    group.push((hash.0, preimage));
                }
            }
            for (hash, bytes) in &extents {
                if !extent_exists(&store.conn, hash)? && !group.iter().any(|(held, _)| held == hash)
                {
                    group.push((*hash, bytes));
                }
            }
            for (_, layout) in &layouts {
                if !extent_exists(&store.conn, layout)?
                    && !trees.iter().any(|(tree, _)| tree.0 == *layout)
                {
                    return Err(StoreError::MissingWireTree { hash: *layout });
                }
            }
            let bytes: Vec<&[u8]> = group.iter().map(|(_, bytes)| *bytes).collect();
            let appended = store.append_extents(&bytes)?;

            #[cfg(test)]
            if let Some(hook) = store.before_commit.as_mut() {
                hook();
            }
            let ((), memo_seq) = store.memo_transaction(|txn, memo_seq| {
                for ((hash, bytes), (segment, offset)) in group.iter().zip(&appended.locations) {
                    upsert_extent(txn, hash, *segment, *offset, bytes.len() as u64)?;
                }
                txn.prepare_cached(
                    "INSERT INTO results(key_kind, static_key, trace_digest, memo_seq,
                                         asset_uuid, trace, failure)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )?
                .execute(rusqlite::params![
                    key_kind as i64,
                    static_key.as_slice(),
                    trace_digest.as_slice(),
                    memo_seq.0 as i64,
                    commit.asset_uuid.0.as_slice(),
                    commit.trace,
                    failure,
                ])?;
                let mut named = txn.prepare_cached(
                    "INSERT INTO result_outputs(key_kind, static_key, trace_digest, role, name,
                                                types, content_hash)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )?;
                let mut name = |role: i64, name: &str, types: Option<Vec<u8>>, hash: &[u8; 32]| {
                    named.execute(rusqlite::params![
                        key_kind as i64,
                        static_key.as_slice(),
                        trace_digest.as_slice(),
                        role,
                        name,
                        types,
                        hash.as_slice(),
                    ])
                };
                for row in &output_rows {
                    let types = row.type_uuids.iter().flat_map(|uuid| uuid.0).collect();
                    name(
                        ROLE_OUTPUT,
                        &row.output_key,
                        Some(types),
                        &row.content_hash.0,
                    )?;
                }
                for row in &aux_rows {
                    name(ROLE_AUX, &row.debug_key, None, &row.content_hash.0)?;
                }
                for (output_key, layout) in &layouts {
                    name(ROLE_WIRE_TREE, output_key, None, layout)?;
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
    #[cfg(any(test, feature = "test-hooks"))]
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
    /// committed first: their keys, nothing read from them. One search of
    /// the bucket.
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
                    key_kind,
                    static_key: *static_key,
                })
            },
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The candidate `row` names, with its outputs; `None` when it was
    /// evicted since the rows were read (a cache miss). One statement.
    pub fn read_candidate(&self, row: &CandidateRow) -> Result<Option<Candidate>, StoreError> {
        let mut stmt = self.conn.prepare_cached(CANDIDATE)?;
        let mut rows = stmt.query(rusqlite::params![
            row.key_kind as i64,
            row.static_key.as_slice(),
            row.trace_digest.as_slice(),
        ])?;
        let bad = |detail: &str| StoreError::BadResultPayload {
            detail: detail.to_owned(),
        };
        let mut found = None;
        let mut outputs = Vec::new();
        let mut aux = Vec::new();
        while let Some(r) = rows.next()? {
            if found.is_none() {
                let asset: Vec<u8> = r.get(0)?;
                let asset = AssetUuid(
                    asset
                        .try_into()
                        .map_err(|_| bad("asset uuid is not 16 bytes"))?,
                );
                found = Some((
                    asset,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Option<Vec<u8>>>(2)?,
                ));
            }
            let Some(role) = r.get::<_, Option<i64>>(3)? else {
                continue;
            };
            let name: String = r.get(4)?;
            let hash = ContentHash(crate::bundles::blob32(r.get(6)?));
            match role {
                ROLE_OUTPUT => {
                    let types: Vec<u8> = r.get::<_, Option<Vec<u8>>>(5)?.unwrap_or_default();
                    if types.len() % 16 != 0 {
                        return Err(bad("output types are not 16-byte uuids"));
                    }
                    outputs.push(OutputRow {
                        output_key: name,
                        type_uuids: types
                            .chunks_exact(16)
                            .map(|uuid| TypeUuid(uuid.try_into().expect("16-byte chunk")))
                            .collect(),
                        content_hash: hash,
                    });
                }
                ROLE_AUX => aux.push(AuxRow {
                    debug_key: name,
                    content_hash: hash,
                }),
                _ => {}
            }
        }
        let Some((asset_uuid, trace, failure)) = found else {
            return Ok(None);
        };
        let outcome = match failure {
            None => ResultOutcome::Success { outputs, aux },
            Some(cause) => ResultOutcome::Failure {
                cause: FailureCause::decode(&cause)?,
            },
        };
        Ok(Some(Candidate {
            trace_digest: row.trace_digest,
            memo_seq: row.memo_seq,
            asset_uuid,
            payload: ResultPayload {
                key_kind: row.key_kind,
                trace,
                outcome,
            },
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

    /// Full doctor verification of every indexed CAS extent: each one's
    /// bytes are read and checked against its hash, so an index/segment
    /// drift cannot be reported healthy merely because its framing still
    /// decodes. Segment by segment: one statement and one file open each
    /// ([`VERIFY_SEGMENTS`], [`VERIFY_SEGMENT_EXTENTS`]).
    pub fn verify_all_cas_extents(&self) -> Result<usize, StoreError> {
        use std::io::{Read, Seek, SeekFrom};
        let segments: Vec<(i64, String)> = {
            let mut statement = self.conn.prepare(VERIFY_SEGMENTS)?;
            let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect::<Result<_, _>>()?
        };
        let mut extents = self.conn.prepare(VERIFY_SEGMENT_EXTENTS)?;
        let mut verified = 0;
        for (segment, name) in segments {
            let mut rows = extents.query([segment])?;
            let Some(mut row) = rows.next()? else {
                continue;
            };
            let path = self.config.state_path.join("cas").join(name);
            let mut file = segment_open_options()
                .read(true)
                .open(&path)
                .map_err(io_err(&path))?;
            let mut bytes = Vec::new();
            loop {
                let hash: Vec<u8> = row.get(0)?;
                let offset = row.get::<_, i64>(1)? as u64;
                let len = row.get::<_, i64>(2)? as usize;
                bytes.resize(len, 0);
                file.seek(SeekFrom::Start(offset)).map_err(io_err(&path))?;
                file.read_exact(&mut bytes).map_err(io_err(&path))?;
                if blake3::hash(&bytes).as_bytes().as_slice() != hash.as_slice() {
                    return Err(StoreError::CorruptExtent {
                        segment: segment as u64,
                        offset,
                    });
                }
                verified += 1;
                match rows.next()? {
                    Some(next) => row = next,
                    None => break,
                }
            }
        }
        Ok(verified)
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
        f.seek(SeekFrom::Start(offset)).map_err(io_err(&path))?;
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

// ---- derived-output namespace (§9/§13) ----

impl StoreReader {
    /// A derived child as the namespace serves it — the only authority
    /// (§9): historical result rows never resurrect a retired child. A
    /// child is its one derived-output claim while neither it nor its
    /// parent is withheld.
    pub fn derived_output(
        &self,
        child: AssetUuid,
    ) -> Result<Option<crate::claims::DerivedOutputClaim>, StoreError> {
        let mut claims = self.derived_output_claims(child)?;
        if claims.len() != 1
            || self.withholding(child)?.is_some()
            || self.withholding(claims[0].parent)?.is_some()
        {
            return Ok(None);
        }
        Ok(claims.pop())
    }

    /// [`Self::derived_output`]'s `(parent, output key)`.
    pub fn resolve_child(
        &self,
        child: AssetUuid,
    ) -> Result<Option<(AssetUuid, String)>, StoreError> {
        Ok(self
            .derived_output(child)?
            .map(|claim| (claim.parent, claim.output_key)))
    }
}
