//! GC (§13): eviction, the cache-limit sweep, compaction, and deleting
//! dead segment files.
//!
//! Eviction is always *safe*: everything in the CAS is rebuildable. It
//! deletes index rows only; a reader that loses the race sees a cache
//! miss. `cas_refs` says what keeps each extent indexed, with a foreign
//! key to `cas_extents`, so pruning is one statement inside the write
//! transaction and can never leave a reference dangling.
//!
//! Compaction copies the live records of a mostly-dead sealed segment into
//! a new segment and repoints the index in one write transaction; it
//! never rewrites a segment in place. The old segment is then dead, and
//! [`SegmentSweeper`] deletes its file once no read can still reach it
//! (see the read bound in [`crate::cas`]).

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};

use crate::cas::record::{decode_record, KeyKind, RecordKind, RECORD_HEADER_LEN};
use crate::cas::store::{
    fsync_dir, read_segment, segment_file_name, segment_open_options, SegmentKind,
    HOLDER_INSTALLED, HOLDER_RESULT, SEGMENT_DEAD, SEGMENT_OPEN, SEGMENT_SEALED,
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
    /// Live records copied into new segments.
    pub records_copied: usize,
    /// Indexed bytes of the segments that died, less the bytes copied.
    pub reclaimed_bytes: u64,
    /// Segments that died. Their files stay until [`SegmentSweeper`]
    /// deletes them.
    pub dead_segments: Vec<u64>,
}

/// Delete every extent no `cas_refs` row names. One statement, run in the
/// caller's write transaction.
fn prune(txn: &rusqlite::Connection) -> Result<usize, StoreError> {
    Ok(txn.execute(
        "DELETE FROM cas_extents WHERE NOT EXISTS
           (SELECT 1 FROM cas_refs WHERE cas_refs.content_hash = cas_extents.content_hash)",
        [],
    )?)
}

/// Drop one holder's references and the extents only it held.
fn release_holder(
    txn: &rusqlite::Connection,
    holder_kind: i64,
    holder: &[u8],
) -> Result<(), StoreError> {
    let hashes: Vec<Vec<u8>> = {
        let mut statement = txn.prepare(
            "SELECT content_hash FROM cas_refs WHERE holder_kind = ?1 AND holder = ?2",
        )?;
        let rows = statement.query_map(rusqlite::params![holder_kind, holder], |row| row.get(0))?;
        rows.collect::<Result<_, _>>()?
    };
    txn.execute(
        "DELETE FROM cas_refs WHERE holder_kind = ?1 AND holder = ?2",
        rusqlite::params![holder_kind, holder],
    )?;
    for hash in hashes {
        txn.execute(
            "DELETE FROM cas_extents WHERE content_hash = ?1
               AND NOT EXISTS (SELECT 1 FROM cas_refs WHERE content_hash = ?1)",
            [hash],
        )?;
    }
    Ok(())
}

/// Evict one result row and everything only it held.
fn evict_result_rows(
    txn: &rusqlite::Connection,
    key_kind: i64,
    static_key: &[u8],
    trace_digest: &[u8],
) -> Result<bool, StoreError> {
    use rusqlite::OptionalExtension;
    let memo_seq: Option<i64> = txn
        .query_row(
            "SELECT memo_seq FROM result_candidates
             WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3",
            rusqlite::params![key_kind, static_key, trace_digest],
            |row| row.get(0),
        )
        .optional()?;
    let Some(memo_seq) = memo_seq else {
        return Ok(false);
    };
    txn.execute(
        "DELETE FROM result_candidates
         WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3",
        rusqlite::params![key_kind, static_key, trace_digest],
    )?;
    txn.execute("DELETE FROM derived_assertions WHERE memo_seq = ?1", [memo_seq])?;
    let mut holder = Vec::with_capacity(65);
    holder.push(key_kind as u8);
    holder.extend_from_slice(static_key);
    holder.extend_from_slice(trace_digest);
    release_holder(txn, HOLDER_RESULT, &holder)?;
    Ok(true)
}

/// A record compaction copies, and the index row that points at it.
enum Moved {
    /// An extent, and its payload's offset within the record.
    Extent([u8; 32], u64),
    Result(i64, Vec<u8>, Vec<u8>),
}

impl Store {
    /// Remove payload extents nothing references: bytes appended by a
    /// transaction that rolled back, or left by an eviction.
    pub(crate) fn prune_unreferenced_extents(&mut self) -> Result<usize, StoreError> {
        self.write_txn(|store| prune(&store.conn))
    }

    /// Evict one committed result as a whole unit. `Ok(false)` when the
    /// candidate does not exist. Shared extents survive while anything
    /// else still references them.
    pub fn evict_result(
        &mut self,
        key_kind: KeyKind,
        static_key: &[u8; 32],
        trace_digest: &[u8; 32],
    ) -> Result<bool, StoreError> {
        self.write_txn(|store| {
            evict_result_rows(
                &store.conn,
                key_kind as i64,
                static_key.as_slice(),
                trace_digest.as_slice(),
            )
        })
    }

    /// Evict one installed artifact or wire tree, and what only it held.
    pub fn evict_installed(&mut self, hash: &[u8; 32]) -> Result<(), StoreError> {
        self.write_txn(|store| release_holder(&store.conn, HOLDER_INSTALLED, hash))
    }

    /// The cache-limit sweep (§18's `cas.cache_limit`, operational-live):
    /// evict whole units (results and installs) in random order until the
    /// indexed extent bytes fit the cap. Random rather than LRU: with a
    /// working set larger than the cap, LRU evicts each result just before
    /// its next use and the hit rate falls to zero; random eviction degrades
    /// gracefully. One write transaction; no segment bytes are read.
    pub fn enforce_cache_limit(&mut self) -> Result<EvictionSweep, StoreError> {
        let cache_limit = self.config.cache_limit;
        self.write_txn(|store| {
            let txn = &*store.conn;
            prune(txn)?;
            let live = || -> Result<u64, StoreError> {
                Ok(txn.query_row("SELECT COALESCE(SUM(len), 0) FROM cas_extents", [], |r| {
                    r.get::<_, i64>(0)
                })? as u64)
            };
            let mut live_bytes = live()?;
            let mut evicted = 0usize;
            if live_bytes <= cache_limit {
                return Ok(EvictionSweep {
                    evicted,
                    live_bytes,
                });
            }
            let victims: Vec<(i64, Vec<u8>)> = {
                let mut statement = txn.prepare(
                    "SELECT holder_kind, holder FROM
                       (SELECT DISTINCT holder_kind, holder FROM cas_refs)
                     ORDER BY random()",
                )?;
                let rows = statement.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
                rows.collect::<Result<_, _>>()?
            };
            for (holder_kind, holder) in victims {
                if live_bytes <= cache_limit {
                    break;
                }
                if holder_kind == HOLDER_RESULT {
                    if holder.len() != 65 {
                        continue;
                    }
                    evict_result_rows(txn, i64::from(holder[0]), &holder[1..33], &holder[33..65])?;
                } else {
                    release_holder(txn, holder_kind, &holder)?;
                }
                evicted += 1;
                live_bytes = live()?;
            }
            Ok(EvictionSweep {
                evicted,
                live_bytes,
            })
        })
    }

    /// Compact the CAS. A sealed segment (or this writer's own active one)
    /// that nothing references any more dies; one whose live records fill
    /// less than half of it has them copied into new segments, the index
    /// repointed in one write transaction, and then dies. Dead segments are
    /// returned, not deleted: their files go through [`SegmentSweeper`].
    pub fn compact(&mut self) -> Result<CompactionReport, StoreError> {
        struct Candidate {
            id: u64,
            name: String,
            kind: SegmentKind,
            indexed_len: u64,
            live: u64,
        }
        let own = self.cas.active_id();
        let candidates: Vec<Candidate> = {
            let mut statement = self.conn.prepare(
                "SELECT s.segment_id, s.file_name, s.segment_kind, s.indexed_len,
                   (SELECT COALESCE(SUM(len), 0) FROM cas_extents e
                      WHERE e.segment = s.segment_id)
                 + (SELECT COALESCE(SUM(len), 0) FROM result_candidates r
                      WHERE r.segment = s.segment_id)
                 FROM cas_segments s
                 WHERE s.state = ?1 OR s.segment_id = ?2
                 ORDER BY s.segment_id",
            )?;
            let rows = statement.query_map(
                rusqlite::params![SEGMENT_SEALED, own.map_or(-1, |id| id as i64)],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                let (id, name, kind, indexed_len, live) = row?;
                out.push(Candidate {
                    id: id as u64,
                    name,
                    kind: SegmentKind::from_i64(kind).unwrap_or(SegmentKind::Regular),
                    indexed_len: indexed_len as u64,
                    live: live as u64,
                });
            }
            out
        };

        let mut victims = Vec::new();
        // (record bytes, old segment, old record offset, index row).
        let mut payloads: Vec<(Vec<u8>, u64, u64, Moved)> = Vec::new();
        let mut results: Vec<(Vec<u8>, u64, u64, Moved)> = Vec::new();
        for candidate in &candidates {
            if candidate.live == 0 {
                victims.push(candidate.id);
                continue;
            }
            if candidate.kind != SegmentKind::Regular || candidate.live * 2 >= candidate.indexed_len
            {
                continue;
            }
            let extents: HashMap<[u8; 32], u64> = {
                let mut statement = self
                    .conn
                    .prepare("SELECT content_hash, offset FROM cas_extents WHERE segment = ?1")?;
                let rows = statement.query_map([candidate.id as i64], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                })?;
                let mut out = HashMap::new();
                for row in rows {
                    let (hash, offset) = row?;
                    out.insert(crate::bundles::blob32(hash), offset as u64);
                }
                out
            };
            type ResultKey = (i64, Vec<u8>, Vec<u8>);
            let result_rows: HashMap<u64, ResultKey> = {
                let mut statement = self.conn.prepare(
                    "SELECT offset, key_kind, static_key, trace_digest FROM result_candidates
                     WHERE segment = ?1",
                )?;
                let rows = statement.query_map([candidate.id as i64], |row| {
                    Ok((row.get::<_, i64>(0)?, (row.get(1)?, row.get(2)?, row.get(3)?)))
                })?;
                let mut out = HashMap::new();
                for row in rows {
                    let (offset, key) = row?;
                    out.insert(offset as u64, key);
                }
                out
            };
            let data = read_segment(&self.cas.dir.join(&candidate.name))?;
            let end = (candidate.indexed_len as usize).min(data.len());
            let mut position = 0u64;
            while (position as usize) < end {
                let decoded = decode_record(&data[position as usize..end], candidate.id, position)?;
                let record = &decoded.record;
                let bytes = data[position as usize..(position + decoded.encoded_len) as usize].to_vec();
                if record.kind == RecordKind::Result {
                    if let Some((kind, static_key, trace)) = result_rows.get(&position) {
                        let moved = Moved::Result(*kind, static_key.clone(), trace.clone());
                        results.push((bytes, candidate.id, position, moved));
                    }
                } else {
                    let payload_start = RECORD_HEADER_LEN as u64
                        + record.static_input_key.len() as u64
                        + record.output_key.len() as u64;
                    if extents.get(&decoded.content_hash) == Some(&(position + payload_start)) {
                        let moved = Moved::Extent(decoded.content_hash, payload_start);
                        payloads.push((bytes, candidate.id, position, moved));
                    }
                }
                position += decoded.encoded_len;
            }
            victims.push(candidate.id);
        }
        if victims.is_empty() {
            return Ok(CompactionReport {
                records_copied: 0,
                reclaimed_bytes: 0,
                dead_segments: Vec::new(),
            });
        }
        if own.is_some_and(|own| victims.contains(&own)) {
            self.cas.forget_active();
        }

        // Write the copies into new segments, rolling at the cap. Payloads
        // precede every result, so the new segments alone rebuild.
        let segment_size = self.config.segment_size;
        let mut written: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut moves = Vec::new();
        for (bytes, old_segment, old_offset, moved) in payloads.into_iter().chain(results) {
            let roll = match written.last() {
                None => true,
                Some((_, segment)) => {
                    !segment.is_empty() && segment.len() as u64 + bytes.len() as u64 > segment_size
                }
            };
            if roll {
                let id = self.write_txn(|store| {
                    let id = store.create_segment(SegmentKind::Regular)?;
                    store.conn.execute(
                        "UPDATE cas_segments SET state = ?2 WHERE segment_id = ?1 AND state = ?3",
                        rusqlite::params![id as i64, SEGMENT_SEALED, SEGMENT_OPEN],
                    )?;
                    Ok(id)
                })?;
                written.push((id, Vec::new()));
            }
            let (id, segment) = written.last_mut().expect("a destination segment");
            let offset = segment.len() as u64;
            segment.extend_from_slice(&bytes);
            moves.push((*id, offset, bytes.len() as u64, old_segment, old_offset, moved));
        }
        for (id, bytes) in &written {
            let path = self.cas.dir.join(segment_file_name(*id, SegmentKind::Regular));
            segment_open_options()
                .write(true)
                .open(&path)
                .and_then(|mut file| {
                    file.write_all(bytes)?;
                    file.sync_all()
                })
                .map_err(|source| StoreError::Io { path, source })?;
        }

        let records_copied = moves.len();
        let copied_bytes: u64 = written.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        let dead = self.write_txn(|store| {
            let txn = &*store.conn;
            for (segment, offset, len, old_segment, old_offset, moved) in &moves {
                match moved {
                    Moved::Extent(hash, payload_start) => {
                        txn.execute(
                            "UPDATE cas_extents SET segment = ?2, offset = ?3
                             WHERE content_hash = ?1 AND segment = ?4 AND offset = ?5",
                            rusqlite::params![
                                hash.as_slice(),
                                *segment as i64,
                                (*offset + *payload_start) as i64,
                                *old_segment as i64,
                                (*old_offset + *payload_start) as i64
                            ],
                        )?;
                    }
                    Moved::Result(kind, static_key, trace) => {
                        txn.execute(
                            "UPDATE result_candidates SET segment = ?4, offset = ?5, len = ?6
                             WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3
                               AND segment = ?7 AND offset = ?8",
                            rusqlite::params![
                                kind,
                                static_key.as_slice(),
                                trace.as_slice(),
                                *segment as i64,
                                *offset as i64,
                                *len as i64,
                                *old_segment as i64,
                                *old_offset as i64
                            ],
                        )?;
                    }
                }
            }
            for (id, bytes) in &written {
                txn.execute(
                    "UPDATE cas_segments SET indexed_len = ?2 WHERE segment_id = ?1",
                    rusqlite::params![*id as i64, bytes.len() as i64],
                )?;
            }
            let mut dead = Vec::new();
            for victim in &victims {
                let changed = txn.execute(
                    "UPDATE cas_segments SET state = ?2 WHERE segment_id = ?1
                       AND NOT EXISTS (SELECT 1 FROM cas_extents WHERE segment = ?1)
                       AND NOT EXISTS (SELECT 1 FROM result_candidates WHERE segment = ?1)",
                    rusqlite::params![*victim as i64, SEGMENT_DEAD],
                )?;
                if changed != 0 {
                    dead.push(*victim);
                }
            }
            Ok(dead)
        })?;
        let dead_bytes: u64 = candidates
            .iter()
            .filter(|candidate| dead.contains(&candidate.id))
            .map(|candidate| candidate.indexed_len)
            .sum();
        Ok(CompactionReport {
            records_copied,
            reclaimed_bytes: dead_bytes.saturating_sub(copied_bytes),
            dead_segments: dead,
        })
    }
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
            let mut statement = store
                .conn
                .prepare("SELECT segment_id, file_name FROM cas_segments WHERE state = ?1")?;
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
                store.conn.execute(
                    "DELETE FROM cas_segments WHERE segment_id = ?1 AND state = ?2",
                    rusqlite::params![*id as i64, SEGMENT_DEAD],
                )?;
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
    use crate::cas::{BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
    use crate::{Store, StoreConfig, StoreError};

    #[test]
    fn a_prune_racing_a_commit_never_leaves_a_dangling_reference() {
        // Writer `a` finds the wire tree installed and does not append it;
        // before its index transaction, writer `b` evicts the install, which
        // prunes the extent. `a`'s transaction checks again, appends the tree
        // after all, and commits a result whose every reference resolves.
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

        let mut b = Some(a.open_writer().unwrap());
        a.before_commit = Some(Box::new(move || {
            if let Some(mut b) = b.take() {
                b.evict_installed(&layout.0).unwrap();
                assert!(matches!(b.wire_tree_read(layout), Err(StoreError::NotFound { .. })));
            }
        }));
        let receipt = a
            .commit_build(BuildCommit {
                wire_trees: vec![wire_bytes.clone()],
                key_kind: KeyKind::Processor,
                static_input_key: [1; 32],
                asset_uuid: AssetUuid([7; 16]),
                static_inputs_canonical: vec![],
                trace: vec![1],
                outcome: CommitOutcome::Success {
                    payload_kind: PayloadKind::ProcessorOutput,
                    outputs: vec![OutputSpec {
                        output_key: String::new(),
                        type_uuids: vec![],
                        bytes: artifact,
                    }],
                    aux: vec![],
                },
            })
            .unwrap();
        a.before_commit = None;
        assert_eq!(a.wire_tree_read(layout).unwrap(), wire_bytes);

        let dangling: i64 = a
            .conn
            .query_row(
                "SELECT COUNT(*) FROM cas_refs r WHERE NOT EXISTS
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
        assert!(matches!(a.wire_tree_read(layout), Err(StoreError::NotFound { .. })));
    }
}
