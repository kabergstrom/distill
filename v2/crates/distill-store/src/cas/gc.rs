//! GC (§13): eviction, the cache-limit sweep, compaction, and deleting
//! dead segment files.
//!
//! Eviction is always *safe*: everything in the CAS is rebuildable. It
//! deletes index rows only; a reader that loses the race sees a cache
//! miss. What keeps an extent indexed is a holder naming it: a result (its
//! `result_outputs` rows) or an install (its `cas_refs` rows), each with a
//! foreign key to `cas_extents`. Releasing a holder deletes, in the same
//! transaction, each extent nothing names any more, and the keys refuse to
//! drop an extent something still names. No extent outlives its last
//! holder and no holder outlives its extent, so nothing is ever pruned.
//!
//! Compaction copies the live extents of mostly-dead segments into new,
//! sealed segments and repoints the index, all in one write transaction:
//! it allocates, writes, fsyncs and indexes the copies and kills the old
//! segments together, or (rolling back) none of it. It never rewrites a
//! segment in place. The old segment is then dead, and [`SegmentSweeper`]
//! deletes its file once no read can still reach it (see the read bound
//! in [`crate::cas`]).

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use crate::cas::record::KeyKind;
use crate::cas::store::{
    count_cas_write, fsync_dir, segment_file_name, segment_open_options, SegmentKind, SEGMENT_DEAD,
    SEGMENT_OPEN, SEGMENT_SEALED,
};
use crate::db::Store;
use crate::error::StoreError;

/// What a cache-limit sweep did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionSweep {
    /// Units evicted (whole results or installs).
    pub evicted: usize,
    /// Extent bytes still indexed after the sweep.
    pub live_bytes: u64,
}

/// What a compaction did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    /// Live extents copied into new segments.
    pub extents_copied: usize,
    /// Indexed bytes of the segments that died, less the bytes copied.
    pub reclaimed_bytes: u64,
    /// Segments that died. Their files stay until [`SegmentSweeper`]
    /// deletes them.
    pub dead_segments: Vec<u64>,
}

/// Delete the extents of `hashes` nothing holds any more; the bytes that
/// freed.
fn release_extents(txn: &rusqlite::Connection, hashes: Vec<Vec<u8>>) -> Result<u64, StoreError> {
    use rusqlite::OptionalExtension;
    let mut freed = 0;
    for hash in hashes {
        freed += txn
            .prepare_cached(RELEASE_EXTENT)?
            .query_row([hash], |row| row.get::<_, i64>(0))
            .optional()?
            .map_or(0, |len| len as u64);
    }
    Ok(freed)
}

/// Delete extent `?1` when nothing holds it.
pub(crate) const RELEASE_EXTENT: &str = "DELETE FROM cas_extents WHERE content_hash = ?1
       AND NOT EXISTS (SELECT 1 FROM cas_refs WHERE content_hash = ?1)
       AND NOT EXISTS (SELECT 1 FROM result_outputs WHERE content_hash = ?1)
     RETURNING len";

/// Drop one install's references and the extents only it held; the
/// extent bytes that freed.
fn release_install(txn: &rusqlite::Connection, holder: &[u8]) -> Result<u64, StoreError> {
    let hashes: Vec<Vec<u8>> = {
        let mut statement =
            txn.prepare_cached("DELETE FROM cas_refs WHERE holder = ?1 RETURNING content_hash")?;
        let rows = statement.query_map([holder], |row| row.get(0))?;
        rows.collect::<Result<_, _>>()?
    };
    release_extents(txn, hashes)
}

/// The extents one result names: deleted with it (by the cascade).
pub(crate) const RESULT_EXTENTS: &str = "SELECT content_hash FROM result_outputs
     WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3";
/// Delete one result; its `result_outputs` rows go with it.
pub(crate) const EVICT_RESULT_ROW: &str = "DELETE FROM results
     WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3 RETURNING 1";

/// Evict one result and everything only it held; `None` when there is no
/// such result, else the extent bytes that freed.
pub(crate) fn evict_result_rows(
    txn: &rusqlite::Connection,
    key_kind: KeyKind,
    static_key: &[u8; 32],
    trace_digest: &[u8; 32],
) -> Result<Option<u64>, StoreError> {
    use rusqlite::OptionalExtension;
    let key = rusqlite::params![
        key_kind as i64,
        static_key.as_slice(),
        trace_digest.as_slice()
    ];
    let hashes: Vec<Vec<u8>> = {
        let mut statement = txn.prepare_cached(RESULT_EXTENTS)?;
        let rows = statement.query_map(key, |row| row.get(0))?;
        rows.collect::<Result<_, _>>()?
    };
    if txn
        .prepare_cached(EVICT_RESULT_ROW)?
        .query_row(key, |_| Ok(()))
        .optional()?
        .is_none()
    {
        return Ok(None);
    }
    release_extents(txn, hashes).map(Some)
}

/// One eviction victim, sampled: the holder of the first extent at or
/// after content hash `?1` — a result (`key_kind`, `static_key`,
/// `trace_digest`) or an install (`static_key` = the holder, the others
/// NULL) — so a holder is drawn about in proportion to the extents it
/// holds. Bound to a random hash, then, past the last, to the empty blob
/// (which sorts first).
pub(crate) const SAMPLE_HOLDER: &str = "SELECT * FROM (
       SELECT content_hash, key_kind, static_key, trace_digest FROM result_outputs
       WHERE content_hash >= ?1 ORDER BY content_hash LIMIT 1)
     UNION ALL
     SELECT * FROM (
       SELECT content_hash, NULL, holder, NULL FROM cas_refs
       WHERE content_hash >= ?1 ORDER BY content_hash LIMIT 1)
     ORDER BY 1 LIMIT 1";
/// The extent bytes the index holds, summed over the `(segment, len)`
/// covering index. Run only when the CAS index changed since the last
/// pass (`store_meta.cas_writes`).
pub(crate) const LIVE_BYTES: &str = "SELECT COALESCE(SUM(len), 0) FROM cas_extents";
/// The segments compaction considers, each with the bytes the index holds
/// in it (a covering-index range): the sealed ones (state `?1`, a key
/// range of `cas_segments_by_state`) and the compacting writer `?2`'s open
/// segment (the unique partial index the literals select).
pub(crate) const COMPACTION_CANDIDATES: &str =
    "SELECT segment_id, file_name, segment_kind, indexed_len,
       (SELECT COALESCE(SUM(len), 0) FROM cas_extents WHERE segment = s.segment_id)
     FROM cas_segments s WHERE state = ?1
     UNION ALL
     SELECT segment_id, file_name, segment_kind, indexed_len,
       (SELECT COALESCE(SUM(len), 0) FROM cas_extents WHERE segment = s.segment_id)
     FROM cas_segments s WHERE owner = ?2 AND state = 0 AND segment_kind = 0
     ORDER BY segment_id";
const _: () = assert!(SEGMENT_OPEN == 0);
/// The segments in state `?1`.
pub(crate) const SEGMENTS_IN_STATE: &str =
    "SELECT segment_id, file_name FROM cas_segments WHERE state = ?1";
/// The extents segment `?1` holds, where they lie.
pub(crate) const SEGMENT_EXTENTS: &str =
    "SELECT content_hash, offset, len FROM cas_extents WHERE segment = ?1";
/// Most victims one sweep samples; a sweep that stops short leaves the rest
/// to the next pass.
const MAX_VICTIMS: usize = 4096;

impl Store {
    /// Evict one committed result as a whole unit. `Ok(false)` when the
    /// candidate does not exist. Shared extents survive while anything
    /// else still references them.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn evict_result(
        &mut self,
        key_kind: KeyKind,
        static_key: &[u8; 32],
        trace_digest: &[u8; 32],
    ) -> Result<bool, StoreError> {
        self.write_txn(|store| {
            count_cas_write(&store.conn)?;
            Ok(evict_result_rows(&store.conn, key_kind, static_key, trace_digest)?.is_some())
        })
    }

    /// Evict one installed artifact or wire tree, and what only it held.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn evict_installed(&mut self, hash: &[u8; 32]) -> Result<(), StoreError> {
        self.write_txn(|store| {
            count_cas_write(&store.conn)?;
            release_install(&store.conn, hash).map(drop)
        })
    }

    /// The cache-limit sweep (§18's `cas.cache_limit`, operational-live):
    /// evict whole units (results and installs) in random order until the
    /// indexed extent bytes fit the cap. Random rather than LRU: with a
    /// working set larger than the cap, LRU evicts each result just before
    /// its next use and the hit rate falls to zero; random eviction degrades
    /// gracefully. One write transaction; no segment bytes are read. The
    /// live bytes are summed once and each victim's freed bytes subtracted;
    /// victims are sampled one index probe each, at most [`MAX_VICTIMS`] a
    /// sweep (a sweep that stops short reports bytes above the cap).
    pub fn enforce_cache_limit(&mut self) -> Result<EvictionSweep, StoreError> {
        use rusqlite::OptionalExtension;
        let cache_limit = self.config.cache_limit;
        self.write_txn(|store| {
            let txn = &*store.conn;
            let mut live_bytes = txn
                .prepare_cached(LIVE_BYTES)?
                .query_row([], |r| r.get::<_, i64>(0))? as u64;
            let mut evicted = 0usize;
            while live_bytes > cache_limit && evicted < MAX_VICTIMS {
                type Victim = (Option<i64>, Vec<u8>, Option<Vec<u8>>);
                let sample = |from: &[u8]| -> Result<Option<Victim>, StoreError> {
                    Ok(txn
                        .prepare_cached(SAMPLE_HOLDER)?
                        .query_row([from], |r| Ok((r.get(1)?, r.get(2)?, r.get(3)?)))
                        .optional()?)
                };
                let mut from = [0u8; 32];
                getrandom::getrandom(&mut from).expect("OS randomness unavailable");
                let victim = match sample(&from)? {
                    Some(victim) => Some(victim),
                    None => sample(&[])?,
                };
                let freed = match victim {
                    None => break,
                    Some((Some(key_kind), static_key, Some(trace_digest))) => {
                        let key_kind = u8::try_from(key_kind)
                            .ok()
                            .and_then(KeyKind::from_byte)
                            .ok_or_else(|| StoreError::BadResultPayload {
                                detail: format!("unknown key kind {key_kind}"),
                            })?;
                        evict_result_rows(
                            txn,
                            key_kind,
                            &crate::bundles::blob32(static_key),
                            &crate::bundles::blob32(trace_digest),
                        )?
                        .unwrap_or(0)
                    }
                    Some((_, holder, _)) => release_install(txn, &holder)?,
                };
                evicted += 1;
                live_bytes = live_bytes.saturating_sub(freed);
            }
            Ok(EvictionSweep {
                evicted,
                live_bytes,
            })
        })
    }

    /// The segments compaction acts on: those nothing references (they
    /// die) and regular ones whose live extents fill at most half of them
    /// (copied, then they die). A sealed segment, or this writer's open one.
    fn compaction_candidates(&self) -> Result<Vec<CompactionCandidate>, StoreError> {
        let mut statement = self.conn.prepare_cached(COMPACTION_CANDIDATES)?;
        let rows =
            statement.query_map(rusqlite::params![SEGMENT_SEALED, self.cas.owner], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, name, kind, indexed_len, live) = row?;
            let candidate = CompactionCandidate {
                id: id as u64,
                name,
                kind: SegmentKind::from_i64(kind).unwrap_or(SegmentKind::Regular),
                indexed_len: indexed_len as u64,
                live: live as u64,
            };
            let copied = candidate.kind == SegmentKind::Regular
                && candidate.live * 2 <= candidate.indexed_len;
            if candidate.live == 0 || copied {
                out.push(candidate);
            }
        }
        Ok(out)
    }

    /// Compact the CAS. A sealed segment (or this writer's open one) that
    /// nothing references any more dies; a regular one whose live extents
    /// fill at most half of it has them copied into new sealed segments,
    /// the index repointed, and then dies. Everything after the first read
    /// is one write transaction: the copies' segments are allocated,
    /// written, fsynced and indexed, and the old ones killed, together. A
    /// compaction that fails leaves no segment behind but files past
    /// `next_segment_id`, which are uncommitted by definition (see
    /// `create_segment`). Dead segments are returned, not deleted: their
    /// files go through [`SegmentSweeper`].
    pub fn compact(&mut self) -> Result<CompactionReport, StoreError> {
        // Nothing to compact is answered without the write lock.
        if self.compaction_candidates()?.is_empty() {
            return Ok(CompactionReport {
                extents_copied: 0,
                reclaimed_bytes: 0,
                dead_segments: Vec::new(),
            });
        }
        self.write_txn(Store::compact_locked)
    }

    fn compact_locked(&mut self) -> Result<CompactionReport, StoreError> {
        let candidates = self.compaction_candidates()?;
        let mut victims = Vec::new();
        // (extent bytes, old segment, old offset, hash).
        let mut extents: Vec<(Vec<u8>, u64, u64, Vec<u8>)> = Vec::new();
        for candidate in &candidates {
            victims.push(candidate.id);
            if candidate.live == 0 {
                continue;
            }
            let path = self.cas.dir.join(&candidate.name);
            let mut file = segment_open_options()
                .read(true)
                .open(&path)
                .map_err(|source| StoreError::Io {
                    path: path.clone(),
                    source,
                })?;
            let mut statement = self.conn.prepare_cached(SEGMENT_EXTENTS)?;
            let mut rows = statement.query([candidate.id as i64])?;
            while let Some(row) = rows.next()? {
                let hash: Vec<u8> = row.get(0)?;
                let offset = row.get::<_, i64>(1)? as u64;
                let mut bytes = vec![0u8; row.get::<_, i64>(2)? as usize];
                file.seek(SeekFrom::Start(offset))
                    .and_then(|_| file.read_exact(&mut bytes))
                    .map_err(|source| StoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                extents.push((bytes, candidate.id, offset, hash));
            }
        }
        // Lay the copies out in new segments, rolling at the cap.
        let segment_size = self.config.segment_size;
        let mut written: Vec<Vec<u8>> = Vec::new();
        // (destination index in `written`, offset, old segment, old
        // offset, hash).
        let mut moves = Vec::new();
        for (bytes, old_segment, old_offset, hash) in extents {
            let roll = match written.last() {
                None => true,
                Some(segment) => {
                    !segment.is_empty() && segment.len() as u64 + bytes.len() as u64 > segment_size
                }
            };
            if roll {
                written.push(Vec::new());
            }
            let index = written.len() - 1;
            let segment = &mut written[index];
            let offset = segment.len() as u64;
            segment.extend_from_slice(&bytes);
            moves.push((index, offset, old_segment, old_offset, hash));
        }
        // Each copy's segment is allocated sealed (no writer appends to
        // it), then written and fsynced before the index names it.
        let mut destinations = Vec::with_capacity(written.len());
        for bytes in &written {
            let id = self.create_segment(SegmentKind::Regular, SEGMENT_SEALED)?;
            let path = self
                .cas
                .dir
                .join(segment_file_name(id, SegmentKind::Regular));
            segment_open_options()
                .write(true)
                .open(&path)
                .and_then(|mut file| {
                    file.write_all(bytes)?;
                    file.sync_all()
                })
                .map_err(|source| StoreError::Io { path, source })?;
            destinations.push(id);
        }

        let extents_copied = moves.len();
        let copied_bytes: u64 = written.iter().map(|bytes| bytes.len() as u64).sum();
        let txn = &*self.conn;
        count_cas_write(txn)?;
        for (index, offset, old_segment, old_offset, hash) in &moves {
            txn.prepare_cached(
                "UPDATE cas_extents SET segment = ?2, offset = ?3
                 WHERE content_hash = ?1 AND segment = ?4 AND offset = ?5",
            )?
            .execute(rusqlite::params![
                hash.as_slice(),
                destinations[*index] as i64,
                *offset as i64,
                *old_segment as i64,
                *old_offset as i64
            ])?;
        }
        for (id, bytes) in destinations.iter().zip(&written) {
            txn.prepare_cached("UPDATE cas_segments SET indexed_len = ?2 WHERE segment_id = ?1")?
                .execute(rusqlite::params![*id as i64, bytes.len() as i64])?;
        }
        // Every live extent of a victim moved in this transaction, which
        // holds the write lock: nothing references a victim now.
        for victim in &victims {
            txn.prepare_cached("UPDATE cas_segments SET state = ?2 WHERE segment_id = ?1")?
                .execute(rusqlite::params![*victim as i64, SEGMENT_DEAD])?;
        }
        let dead_bytes: u64 = candidates
            .iter()
            .map(|candidate| candidate.indexed_len)
            .sum();
        Ok(CompactionReport {
            extents_copied,
            reclaimed_bytes: dead_bytes.saturating_sub(copied_bytes),
            dead_segments: victims,
        })
    }
}

/// A segment compaction acts on, with the bytes the index holds in it.
struct CompactionCandidate {
    id: u64,
    name: String,
    kind: SegmentKind,
    indexed_len: u64,
    live: u64,
}

/// Deletes dead segment files once no read can still reach them. One
/// sweeper runs per store, on one thread (the daemon's loop). A segment is
/// deleted `grace` after the sweep that first saw it dead; `grace` must
/// exceed the read bound (see [`crate::cas`]). A delete that fails (a
/// reader on Windows still has the file open) is retried on the next
/// sweep.
#[derive(Debug)]
pub struct SegmentSweeper {
    grace: Duration,
    dead_since: HashMap<u64, Instant>,
}

impl SegmentSweeper {
    pub fn new(grace: Duration) -> Self {
        Self {
            grace,
            dead_since: HashMap::new(),
        }
    }

    /// Delete the dead segments whose grace has passed; returns how many.
    pub fn sweep(&mut self, store: &mut Store) -> Result<usize, StoreError> {
        let now = Instant::now();
        let dead: Vec<(u64, String)> = {
            let mut statement = store.conn.prepare_cached(SEGMENTS_IN_STATE)?;
            let rows = statement.query_map([SEGMENT_DEAD], |row| {
                Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?))
            })?;
            rows.collect::<Result<_, _>>()?
        };
        self.dead_since
            .retain(|id, _| dead.iter().any(|(dead, _)| dead == id));
        let mut deleted = Vec::new();
        for (id, name) in dead {
            let since = *self.dead_since.entry(id).or_insert(now);
            if now.duration_since(since) < self.grace {
                continue;
            }
            let path = store.cas.dir.join(&name);
            match std::fs::remove_file(&path) {
                Ok(()) => deleted.push(id),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => deleted.push(id),
                Err(error) => {
                    tracing::debug!(path = %path.display(), %error, "dead segment kept for the next sweep");
                }
            }
        }
        if deleted.is_empty() {
            return Ok(0);
        }
        fsync_dir(&store.cas.dir)?;
        store.write_txn(|store| {
            for id in &deleted {
                store
                    .conn
                    .prepare_cached(
                        "DELETE FROM cas_segments WHERE segment_id = ?1 AND state = ?2",
                    )?
                    .execute(rusqlite::params![*id as i64, SEGMENT_DEAD])?;
            }
            Ok(())
        })?;
        for id in &deleted {
            self.dead_since.remove(id);
        }
        Ok(deleted.len())
    }
}

#[cfg(test)]
mod tests {
    use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
    use distill_wire::artifact::{write_artifact, ArtifactHeader};
    use distill_wire::dswl::{dswl_bytes, dswl_hash};
    use distill_wire::wire::WireNode;

    use crate::cas::record::KeyKind;
    use crate::cas::{BuildCommit, CommitOutcome, OutputSpec};
    use crate::{Store, StoreConfig, StoreError};

    use super::{LIVE_BYTES, SAMPLE_HOLDER};

    #[test]
    fn eviction_reads_are_planned_on_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        assert_eq!(
            store.query_plan_details(SAMPLE_HOLDER).unwrap(),
            [
                "MERGE (UNION ALL)",
                "LEFT",
                "CO-ROUTINE (subquery-1)",
                "SEARCH result_outputs USING COVERING INDEX result_outputs_by_hash (content_hash>?)",
                "SCAN (subquery-1)",
                "USE TEMP B-TREE FOR ORDER BY",
                "RIGHT",
                "CO-ROUTINE (subquery-3)",
                "SEARCH cas_refs USING COVERING INDEX cas_refs_by_hash (content_hash>?)",
                "SCAN (subquery-3)",
                "USE TEMP B-TREE FOR ORDER BY",
            ]
        );
    }

    static STATEMENTS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    fn record_statement(sql: &str) {
        STATEMENTS.lock().unwrap().push(sql.to_owned());
    }

    fn commit_result(store: &mut Store, index: u32) {
        let mut key = [0u8; 32];
        key[..4].copy_from_slice(&index.to_le_bytes());
        store
            .commit_build(BuildCommit {
                wire_trees: Vec::new(),
                key_kind: KeyKind::Processor,
                static_input_key: key,
                asset_uuid: AssetUuid([7; 16]),
                trace: vec![1],
                outcome: CommitOutcome::Success {
                    outputs: vec![OutputSpec {
                        output_key: String::new(),
                        type_uuids: vec![],
                        bytes: format!("output {index}").into_bytes(),
                    }],
                    aux: vec![],
                },
            })
            .unwrap();
    }

    /// The statements one sweep runs, with the cap `under` bytes below the
    /// live bytes; `(evicted, statements, live sums)`.
    fn sweep_statements(results: u32, under: u64) -> (usize, usize, usize) {
        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path().join("state"));
        let live = {
            let mut store = Store::open(config.clone()).unwrap();
            for index in 0..results {
                commit_result(&mut store, index);
            }
            store
                .conn
                .query_row(LIVE_BYTES, [], |r| r.get::<_, i64>(0))
                .unwrap() as u64
        };
        let mut config = config;
        config.cache_limit = live - under;
        let mut store = Store::open(config).unwrap();
        store.trace_statements(Some(record_statement));
        STATEMENTS.lock().unwrap().clear();
        let sweep = store.enforce_cache_limit().unwrap();
        store.trace_statements(None);
        let statements = std::mem::take(&mut *STATEMENTS.lock().unwrap());
        // A sample past the last hash falls back to the first: one more.
        let fallbacks = statements
            .iter()
            .filter(|sql| sql.contains("content_hash >= zeroblob(0)"))
            .count();
        let sums = statements
            .iter()
            .filter(|sql| sql.as_str() == LIVE_BYTES)
            .count();
        (sweep.evicted, statements.len() - fallbacks, sums)
    }

    /// A sweep sums the live bytes once and then costs per victim, never
    /// per extent: within the cap it is one statement, and one eviction
    /// costs the same over 20 results as over 400.
    #[test]
    fn a_sweep_costs_its_victims_not_the_cas() {
        let within: Vec<_> = [20, 400].map(|results| sweep_statements(results, 0)).into();
        assert_eq!(within[0], within[1], "{within:?}");
        assert_eq!(within[0].0, 0);
        assert_eq!(within[0].2, 1);
        let one: Vec<_> = [20, 400].map(|results| sweep_statements(results, 1)).into();
        assert_eq!(one[0], one[1], "{one:?}");
        assert_eq!(one[0].0, 1);
        assert_eq!(one[0].2, 1);
    }

    /// What a CAS pass within the cap (sweep and compaction) and a reopen
    /// with nothing to recover fetch over `results` committed results:
    /// `(pass pages, open pages, covering-index pages, table pages)`, the
    /// last two for `cas_extents`.
    fn pass_and_open_pages(results: u32) -> (u64, u64, u64, u64) {
        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path().join("state"));
        let mut store = Store::open(config.clone()).unwrap();
        for index in 0..results {
            commit_result(&mut store, index);
        }
        let before = store.pages_fetched().unwrap();
        assert_eq!(store.enforce_cache_limit().unwrap().evicted, 0);
        assert!(store.compact().unwrap().dead_segments.is_empty());
        let pass = store.pages_fetched().unwrap() - before;
        let pages = |names: &str| -> u64 {
            store
                .conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM dbstat WHERE name IN ({names})"),
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap() as u64
        };
        let index_pages = pages("'cas_extents_by_segment'");
        let table_pages = pages("'cas_extents'");
        drop(store);
        let (store, recovery) = Store::open_with_recovery(config).unwrap();
        assert_eq!(recovery, crate::cas::RecoveryReport::default());
        (
            pass,
            store.pages_fetched().unwrap(),
            index_pages,
            table_pages,
        )
    }

    /// The live bytes and each compactable segment's are sums over the
    /// covering index, read once each: the pass grows with those indexes'
    /// pages and reads no table page. Recovery reads segment rows only and
    /// does not grow with the CAS at all.
    #[test]
    fn the_cas_pass_reads_covering_indexes_and_open_reads_segments() {
        let small = pass_and_open_pages(20);
        let large = pass_and_open_pages(2000);
        assert_eq!(small.1, large.1, "{small:?} {large:?}");
        let growth = large.0 - small.0;
        assert!(growth <= 2 * large.2, "{small:?} {large:?}");
        assert!(
            large.2 * 2 < large.3,
            "the covering indexes are the smaller read: {large:?}"
        );
    }

    /// Every extent is held, whatever wrote, moved or released it: a
    /// release deletes the extents only it held in its own transaction, so
    /// nothing is ever left for a prune. Compaction, which allocates its
    /// copies' segments sealed in its one transaction, leaves a writer at
    /// most its one open segment.
    #[test]
    fn eviction_and_compaction_leave_no_unheld_extent_and_one_open_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StoreConfig::new(dir.path().join("state"));
        config.segment_size = 4096;
        let mut store = Store::open(config).unwrap();
        let mut digests = Vec::new();
        for index in 0..200u32 {
            let mut key = [0u8; 32];
            key[..4].copy_from_slice(&index.to_le_bytes());
            digests.push(key);
            commit_result(&mut store, index);
        }
        let check = |store: &Store| {
            let unheld: i64 = store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM cas_extents e WHERE NOT EXISTS
                       (SELECT 1 FROM cas_refs r WHERE r.content_hash = e.content_hash)
                     AND NOT EXISTS
                       (SELECT 1 FROM result_outputs o WHERE o.content_hash = e.content_hash)",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(unheld, 0);
            assert!(open_segments(store) <= 1);
        };
        check(&store);
        // Two in three go: the survivors fill under half of each segment.
        for key in digests
            .iter()
            .enumerate()
            .filter(|(index, _)| index % 3 != 1)
            .map(|(_, key)| key)
        {
            let digest = store.lookup_candidates(KeyKind::Processor, key).unwrap()[0].trace_digest;
            assert!(store
                .evict_result(KeyKind::Processor, key, &digest)
                .unwrap());
        }
        check(&store);
        let compaction = store.compact().unwrap();
        assert!(compaction.extents_copied > 0);
        assert!(!compaction.dead_segments.is_empty());
        check(&store);
        for key in digests.iter().skip(1).step_by(3) {
            assert_eq!(
                store
                    .lookup_candidates(KeyKind::Processor, key)
                    .unwrap()
                    .len(),
                1
            );
        }
        let mut capped = store.config().clone();
        capped.cache_limit = 0;
        drop(store);
        let mut store = Store::open(capped).unwrap();
        store.enforce_cache_limit().unwrap();
        check(&store);
        let live: i64 = store
            .conn
            .query_row(LIVE_BYTES, [], |row| row.get(0))
            .unwrap();
        assert_eq!(live, 0);
    }

    /// One open regular segment per writer is a constraint, not a sweep.
    #[test]
    fn a_writer_cannot_hold_two_open_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        commit_result(&mut store, 1);
        assert_eq!(open_segments(&store), 1);
        let second = store.write_txn(|store| {
            store.create_segment(
                crate::cas::store::SegmentKind::Regular,
                crate::cas::store::SEGMENT_OPEN,
            )
        });
        assert!(
            matches!(&second, Err(StoreError::Sqlite(error)) if error.to_string().contains("UNIQUE")),
            "{second:?}"
        );
        // Sealed (as compaction allocates) and oversize segments are not
        // a writer's append target, and are not constrained.
        store
            .write_txn(|store| {
                store.create_segment(
                    crate::cas::store::SegmentKind::Regular,
                    crate::cas::store::SEGMENT_SEALED,
                )?;
                store.create_segment(
                    crate::cas::store::SegmentKind::Oversize,
                    crate::cas::store::SEGMENT_OPEN,
                )
            })
            .unwrap();
    }

    #[test]
    fn the_cas_write_count_moves_only_with_the_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        let start = store.cas_writes().unwrap();
        commit_result(&mut store, 1);
        let committed = store.cas_writes().unwrap();
        assert!(committed > start);
        store.enforce_cache_limit().unwrap();
        store.compact().unwrap();
        store
            .lookup_candidates(KeyKind::Processor, &[0; 32])
            .unwrap();
        assert_eq!(store.cas_writes().unwrap(), committed);
    }

    #[test]
    fn a_commit_never_leaves_a_dangling_reference() {
        // A commit checks which bytes the index holds in its own write
        // transaction, so no eviction lands between the check and the index
        // rows: a tree evicted before the commit is appended again, and the
        // result's every reference resolves.
        let dir = tempfile::tempdir().unwrap();
        let mut a = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        let node = WireNode::Unit { offset: 0 };
        let wire_bytes = dswl_bytes(&node).unwrap();
        let layout = dswl_hash(&node).unwrap();
        a.put_wire_tree(&wire_bytes).unwrap();
        let artifact = write_artifact(
            &ArtifactHeader {
                asset_uuid: AssetUuid([7; 16]),
                authored_type: TypeUuid([1; 16]),
                terminal_type: TypeUuid([2; 16]),
                encoded_type: TypeUuid([3; 16]),
                logical_hash: LogicalHash([4; 32]),
                layout_hash: layout,
            },
            &[],
            &[],
            &[],
            &[],
        )
        .unwrap();

        a.open_writer().unwrap().evict_installed(&layout.0).unwrap();
        assert!(matches!(
            a.wire_tree_read(layout),
            Err(StoreError::NotFound { .. })
        ));
        let receipt = a
            .commit_build(BuildCommit {
                wire_trees: vec![wire_bytes.clone()],
                key_kind: KeyKind::Processor,
                static_input_key: [1; 32],
                asset_uuid: AssetUuid([7; 16]),
                trace: vec![1],
                outcome: CommitOutcome::Success {
                    outputs: vec![OutputSpec {
                        output_key: String::new(),
                        type_uuids: vec![],
                        bytes: artifact,
                    }],
                    aux: vec![],
                },
            })
            .unwrap();
        assert_eq!(a.wire_tree_read(layout).unwrap(), wire_bytes);

        let dangling: i64 = a
            .conn
            .query_row(
                "SELECT COUNT(*) FROM result_outputs r WHERE NOT EXISTS
                   (SELECT 1 FROM cas_extents e WHERE e.content_hash = r.content_hash)",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dangling, 0);
        // The foreign key refuses to drop an extent something references.
        assert!(a
            .conn
            .execute(
                "DELETE FROM cas_extents WHERE content_hash = ?1",
                [layout.0.as_slice()],
            )
            .is_err());
        assert!(a
            .evict_result(KeyKind::Processor, &[1; 32], &receipt.trace_digest)
            .unwrap());
        assert!(matches!(
            a.wire_tree_read(layout),
            Err(StoreError::NotFound { .. })
        ));
    }

    fn open_segments(store: &Store) -> i64 {
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM cas_segments WHERE state = 0",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn compaction_never_kills_a_segment_a_group_is_being_written_to() {
        // Writer `b` commits a group that rolls its segment mid-group: the
        // first output lands in S0, the second in S1. Before `b`'s index
        // rows commit, writer `a` runs the CAS pass. S0 must not look like a
        // sealed segment nothing references.
        let dir = tempfile::tempdir().unwrap();
        let mut config = StoreConfig::new(dir.path().join("state"));
        config.segment_size = 400;
        let mut b = Store::open(config).unwrap();
        let mut a = Some(b.open_writer().unwrap());
        let dead = std::sync::Arc::new(std::sync::Mutex::new(None));
        let seen = std::sync::Arc::clone(&dead);
        b.before_commit = Some(Box::new(move || {
            if let Some(mut a) = a.take() {
                let compaction = a.compact().unwrap();
                crate::cas::SegmentSweeper::new(std::time::Duration::ZERO)
                    .sweep(&mut a)
                    .unwrap();
                *seen.lock().unwrap() = Some(compaction.dead_segments);
            }
        }));
        let output = vec![9u8; 300];
        let hash = *blake3::hash(&output).as_bytes();
        let second = vec![8u8; 300];
        let second_hash = *blake3::hash(&second).as_bytes();
        b.commit_build(BuildCommit {
            wire_trees: Vec::new(),
            key_kind: KeyKind::Processor,
            static_input_key: [1; 32],
            asset_uuid: AssetUuid([7; 16]),
            trace: vec![1],
            outcome: CommitOutcome::Success {
                outputs: vec![
                    OutputSpec {
                        output_key: String::new(),
                        type_uuids: vec![],
                        bytes: output.clone(),
                    },
                    OutputSpec {
                        output_key: "second".to_owned(),
                        type_uuids: vec![],
                        bytes: second.clone(),
                    },
                ],
                aux: vec![],
            },
        })
        .unwrap();
        b.before_commit = None;
        assert_eq!(b.cas_read(&second_hash).unwrap(), second);
        let read = b.cas_read(&hash);
        assert!(
            read.as_deref().ok() == Some(output.as_slice()),
            "a committed output is unreadable: {:?}; segments compaction killed mid-commit: {:?}",
            read.map(|bytes| bytes.len()),
            dead.lock().unwrap()
        );
    }

    #[test]
    fn a_rolled_back_savepoint_takes_its_segment_allocation_with_it() {
        // A savepoint that allocated this writer's segment rolls back; the
        // enclosing transaction commits. The row and its id are gone, so a
        // later allocation reuses the id. Nothing may still append there.
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        store
            .write_txn(|store| {
                store.conn.execute_batch("SAVEPOINT inner")?;
                store.put_artifact(b"rolled back", &[])?;
                store
                    .conn
                    .execute_batch("ROLLBACK TO inner; RELEASE inner")?;
                Ok(())
            })
            .unwrap();
        store.put_artifact(b"kept", &[]).unwrap();
        let mut other = store.open_writer().unwrap();
        other.put_artifact(b"another writer's", &[]).unwrap();
        let kept = *blake3::hash(b"kept").as_bytes();
        let read = store.cas_read(&kept);
        assert!(
            read.as_deref().ok() == Some(b"kept".as_slice()),
            "an artifact committed after the rolled-back savepoint: {read:?}, extent {:?}",
            store.extent_of(&kept)
        );
        assert_eq!(
            other
                .cas_read(blake3::hash(b"another writer's").as_bytes())
                .unwrap(),
            b"another writer's"
        );
    }

    /// The open segments `store`'s writer owns, and every segment's
    /// (id, state, indexed_len, file length).
    fn segments(store: &Store) -> (Vec<u64>, Vec<(u64, i64, u64, Option<u64>)>) {
        use crate::cas::store::{segment_file_name, SegmentKind};
        let mut open = store
            .conn
            .prepare("SELECT segment_id FROM cas_segments WHERE owner = ?1 AND state = 0 ORDER BY segment_id")
            .unwrap();
        let open = open
            .query_map([store.cas.owner], |row| row.get::<_, i64>(0))
            .unwrap()
            .map(|id| id.unwrap() as u64)
            .collect();
        let mut all = store
            .conn
            .prepare("SELECT segment_id, state, indexed_len, segment_kind FROM cas_segments ORDER BY segment_id")
            .unwrap();
        let all = all
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .unwrap()
            .map(|row| {
                let (id, state, indexed, kind) = row.unwrap();
                let kind = SegmentKind::from_i64(kind).unwrap();
                let path = store.cas.dir.join(segment_file_name(id as u64, kind));
                (
                    id as u64,
                    state,
                    indexed as u64,
                    std::fs::metadata(path).ok().map(|m| m.len()),
                )
            })
            .collect();
        (open, all)
    }

    fn rejected() -> StoreError {
        StoreError::Rejected {
            detail: "the nested step failed".to_owned(),
        }
    }

    fn not_found(store: &Store, bytes: &[u8]) -> bool {
        matches!(
            store.cas_read(blake3::hash(bytes).as_bytes()),
            Err(StoreError::NotFound { .. })
        )
    }

    #[test]
    fn a_failed_write_savepoint_on_an_earlier_segment_leaks_and_truncates_nothing() {
        // The segment is allocated in the enclosing transaction, before the
        // nested write transaction (a savepoint) appends to it and fails.
        // The segment stays this writer's one open segment; the next append
        // goes past the dead bytes, and the index covers the whole file.
        let dir = tempfile::tempdir().unwrap();
        let mut config = StoreConfig::new(dir.path().join("state"));
        config.segment_size = 256;
        let mut store = Store::open(config.clone()).unwrap();
        store
            .write_txn(|store| {
                store.put_artifact(b"before", &[])?;
                let failed = store.isolated_write_transaction(|store| {
                    store.put_artifact(b"rolled back", &[])?;
                    Err::<(), _>(rejected())
                });
                assert!(matches!(failed, Err(StoreError::Rejected { .. })));
                assert!(not_found(store, b"rolled back"));
                let (open, all) = segments(store);
                assert_eq!(open, [0]);
                assert_eq!(all.len(), 1, "{all:?}");
                store.put_artifact(b"after", &[])?;
                Ok(())
            })
            .unwrap();
        let (open, all) = segments(&store);
        assert_eq!(open, [0]);
        let [(0, 0, indexed, Some(file_len))] = all[..] else {
            panic!("{all:?}")
        };
        assert_eq!(
            indexed, file_len,
            "the index covers the dead bytes and the record after them"
        );
        assert_eq!(
            store.cas_read(blake3::hash(b"before").as_bytes()).unwrap(),
            b"before"
        );
        assert_eq!(
            store.cas_read(blake3::hash(b"after").as_bytes()).unwrap(),
            b"after"
        );
        assert!(not_found(&store, b"rolled back"));
        drop(store);

        let (store, recovery) = Store::open_with_recovery(config).unwrap();
        assert_eq!(recovery.truncated_tails, []);
        assert_eq!(recovery.lost_tails, []);
        assert_eq!(
            store.cas_read(blake3::hash(b"before").as_bytes()).unwrap(),
            b"before"
        );
        assert_eq!(
            store.cas_read(blake3::hash(b"after").as_bytes()).unwrap(),
            b"after"
        );
    }

    #[test]
    fn a_failed_write_savepoint_takes_the_segment_it_allocated() {
        // The nested write transaction (a savepoint) rolls this writer onto
        // a new segment, sealing the old one, and fails. Its row, its id and
        // the seal roll back: the old segment is open again, nothing leaks,
        // and the id's next allocation truncates the file the savepoint
        // left. A savepoint whose allocation no later one reuses leaves only
        // a file past `next_segment_id`: nothing deletes it, and the next
        // allocation of its id truncates it.
        let payload = |byte: u8| vec![byte; 200];
        let dir = tempfile::tempdir().unwrap();
        let mut config = StoreConfig::new(dir.path().join("state"));
        config.segment_size = 256;
        let mut store = Store::open(config.clone()).unwrap();
        store.put_artifact(&payload(1), &[]).unwrap();
        let (_, first) = segments(&store);
        store
            .write_txn(|store| {
                let failed = store.isolated_write_transaction(|store| {
                    store.put_artifact(&payload(2), &[])?;
                    assert_eq!(
                        segments(store).0,
                        [1],
                        "the savepoint rolled onto segment 1"
                    );
                    Err::<(), _>(rejected())
                });
                assert!(matches!(failed, Err(StoreError::Rejected { .. })));
                let (open, all) = segments(store);
                assert_eq!(open, [0], "the seal rolled back with the allocation");
                assert_eq!(all, first, "segment 1's row rolled back");
                assert_eq!(
                    crate::db::meta_get_u64(&store.conn, "next_segment_id")?,
                    Some(1)
                );
                let stray = store.cas.dir.join(crate::cas::store::segment_file_name(
                    1,
                    crate::cas::store::SegmentKind::Regular,
                ));
                assert!(
                    std::fs::metadata(&stray).unwrap().len() > 0,
                    "the rolled-back allocation left its file"
                );
                store.put_artifact(&payload(3), &[])?;
                Ok(())
            })
            .unwrap();
        let (open, all) = segments(&store);
        assert_eq!(open, [1]);
        let [(0, 1, indexed_0, Some(len_0)), (1, 0, indexed_1, Some(len_1))] = all[..] else {
            panic!("{all:?}")
        };
        assert_eq!(
            (indexed_0, indexed_1),
            (len_0, len_1),
            "the reused id's file holds only its committed record"
        );
        assert_eq!(
            store
                .cas_read(blake3::hash(&payload(1)).as_bytes())
                .unwrap(),
            payload(1)
        );
        assert_eq!(
            store
                .cas_read(blake3::hash(&payload(3)).as_bytes())
                .unwrap(),
            payload(3)
        );
        assert!(not_found(&store, &payload(2)));

        // A failed savepoint's allocation that nothing reuses before the
        // enclosing transaction commits.
        store
            .write_txn(|store| {
                let failed = store.isolated_write_transaction(|store| {
                    store.put_artifact(&payload(4), &[])?;
                    Err::<(), _>(rejected())
                });
                assert!(failed.is_err());
                Ok(())
            })
            .unwrap();
        let (open, after) = segments(&store);
        assert_eq!(open, [1]);
        assert_eq!(after, all);
        drop(store);

        let (mut store, recovery) = Store::open_with_recovery(config).unwrap();
        assert_eq!(recovery.truncated_tails, []);
        assert_eq!(recovery.lost_tails, []);
        assert_eq!(
            store
                .cas_read(blake3::hash(&payload(1)).as_bytes())
                .unwrap(),
            payload(1)
        );
        assert_eq!(
            store
                .cas_read(blake3::hash(&payload(3)).as_bytes())
                .unwrap(),
            payload(3)
        );
        assert!(not_found(&store, &payload(4)));
        store.put_artifact(&payload(5), &[]).unwrap();
        assert_eq!(
            store
                .cas_read(blake3::hash(&payload(5)).as_bytes())
                .unwrap(),
            payload(5)
        );
        let (open, all) = segments(&store);
        assert_eq!(open, [2]);
        let (2, 0, indexed, Some(file_len)) = all[2] else {
            panic!("{all:?}")
        };
        assert_eq!(
            indexed, file_len,
            "the allocation truncated the uncommitted file"
        );
    }

    #[test]
    fn a_writer_closed_mid_transaction_seals_its_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        let (opener, first) = crate::opener::StoreOpener::new(store);
        let mut writer = opener.open_writer().unwrap();
        writer.put_artifact(b"committed", &[]).unwrap();
        assert_eq!(open_segments(&*first), 1);
        writer.open_input().unwrap();
        drop(writer);
        assert_eq!(
            open_segments(&*first),
            0,
            "a closed writer's segment stays open"
        );
    }
}
