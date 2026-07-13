//! GC (§13): pins, eviction under the observability rule, the
//! cache-limit LRU sweep, and Bitcask-style compaction.
//!
//! Eviction is always *safe* — everything in the CAS is rebuildable —
//! but never *observable*: no ContentHash referenced by a manifest
//! entry, live lease, in-flight build, or open pack-build session may be
//! removed, the check runs inside the same transaction that deletes the
//! index rows, and pins hold a build result's whole output table (aux
//! payloads included) as one unit.
//!
//! Compaction writes new segments durably, atomically replaces
//! `CURRENT`, then flips the index in one SQLite transaction; old
//! segments are deleted last. An interrupted compaction leaves one
//! generation or the other intact, never a mix: a crash between the
//! `CURRENT` replace and the index flip is exactly the
//! generation-mismatch rebuild path. This store performs only transient
//! per-call reads (no long-lived mmap readers), so no generation pins
//! outlive the compaction call itself; a future long-lived reader must
//! take a generation pin before old files may go.

use std::collections::{HashMap, HashSet};
use std::io::Write;

use crate::artifacts::PinKind;
use crate::cas::manifest::{self, GenerationManifest, ManifestSegment, SegmentKind};
use crate::cas::record::{
    decode_record, KeyKind, RecordKind, ResultOutcome, ResultPayload, RECORD_HEADER_LEN,
};
use crate::cas::store::{segment_file_name, CasInner, SegmentInfo};
use crate::db::{meta_set_u64, Store};
use crate::error::StoreError;

/// What a cache-limit sweep did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionSweep {
    /// Results evicted (whole units).
    pub evicted: usize,
    /// Extent bytes still indexed after the sweep.
    pub live_bytes: u64,
}

/// What a compaction did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    pub old_generation: u64,
    pub new_generation: u64,
    /// Live records carried into the new generation.
    pub records_copied: usize,
    /// Segment bytes reclaimed.
    pub reclaimed_bytes: u64,
}

impl Store {
    /// Register pins for a holder (§13's observability sources). Lease
    /// pins back `resolve`'s pin-before-response rule; manifest pins
    /// persist across restarts, the rest are ephemeral.
    pub fn pin(
        &mut self,
        kind: PinKind,
        holder: &str,
        hashes: &[[u8; 32]],
    ) -> Result<(), StoreError> {
        let txn = self.conn.transaction()?;
        for hash in hashes {
            txn.execute(
                "INSERT OR IGNORE INTO pins(kind, holder, content_hash) VALUES (?1, ?2, ?3)",
                rusqlite::params![kind as i64, holder, hash.as_slice()],
            )?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Release every pin a holder registered under `kind`.
    pub fn unpin_holder(&mut self, kind: PinKind, holder: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "DELETE FROM pins WHERE kind = ?1 AND holder = ?2",
            rusqlite::params![kind as i64, holder],
        )?;
        Ok(())
    }

    /// Evict one committed result as a whole unit. `Ok(false)` when the
    /// candidate does not exist; `Err(Pinned)` — with nothing deleted —
    /// when any of its output or aux hashes is pinned. Shared extents
    /// survive while any other candidate still references them.
    pub fn evict_result(
        &mut self,
        key_kind: KeyKind,
        static_key: &[u8; 32],
        trace_digest: &[u8; 32],
    ) -> Result<bool, StoreError> {
        use rusqlite::OptionalExtension;
        // Read phase (single-writer discipline: the coordinator owns the
        // store, so nothing commits between these reads and the
        // transaction below).
        let row: Option<(i64, i64, i64, i64)> = self
            .conn
            .query_row(
                "SELECT memo_seq, segment, offset, len FROM result_candidates
                 WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3",
                rusqlite::params![
                    key_kind as i64,
                    static_key.as_slice(),
                    trace_digest.as_slice()
                ],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some((memo_seq, segment, offset, len)) = row else {
            return Ok(false);
        };
        let unit = self.result_unit_hashes(segment as u64, offset as u64, len as u64)?;
        let mut still_referenced: HashSet<[u8; 32]> = HashSet::new();
        let others: Vec<(i64, i64, i64)> = {
            let mut stmt = self.conn.prepare(
                "SELECT segment, offset, len FROM result_candidates
                 WHERE NOT (key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3)",
            )?;
            let mapped = stmt.query_map(
                rusqlite::params![
                    key_kind as i64,
                    static_key.as_slice(),
                    trace_digest.as_slice()
                ],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            mapped.collect::<Result<_, _>>()?
        };
        for (seg, off, l) in others {
            still_referenced.extend(self.result_unit_hashes(seg as u64, off as u64, l as u64)?);
        }

        // Delete phase: the pin check runs inside the same transaction
        // that deletes the index rows (§13).
        let txn = self.conn.transaction()?;
        for hash in &unit {
            let pinned: Option<i64> = txn
                .query_row(
                    "SELECT 1 FROM pins WHERE content_hash = ?1 LIMIT 1",
                    [hash.as_slice()],
                    |r| r.get(0),
                )
                .optional()?;
            if pinned.is_some() {
                return Err(StoreError::Pinned { hash: *hash });
            }
        }
        txn.execute(
            "DELETE FROM result_candidates
             WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3",
            rusqlite::params![
                key_kind as i64,
                static_key.as_slice(),
                trace_digest.as_slice()
            ],
        )?;
        txn.execute(
            "DELETE FROM derived_assertions WHERE memo_seq = ?1",
            [memo_seq],
        )?;
        for hash in &unit {
            if !still_referenced.contains(hash) {
                txn.execute(
                    "DELETE FROM cas_extents WHERE content_hash = ?1",
                    [hash.as_slice()],
                )?;
            }
        }
        txn.commit()?;
        Ok(true)
    }

    /// The whole pin/evict unit of one result record: every output and
    /// aux ContentHash (§13).
    fn result_unit_hashes(
        &self,
        segment: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<[u8; 32]>, StoreError> {
        let bytes = self.read_extent(segment, offset, len)?;
        let decoded = decode_record(&bytes, segment, offset)?;
        let payload = ResultPayload::decode(&decoded.record.payload)?;
        let mut hashes = Vec::new();
        if let ResultOutcome::Success { outputs, aux } = &payload.outcome {
            for row in outputs {
                hashes.push(row.content_hash.0);
            }
            for row in aux {
                hashes.push(row.content_hash.0);
            }
        }
        Ok(hashes)
    }

    /// The LRU sweep (§18's `cas.cache_limit`, operational-live): evict
    /// least-recently-used unpinned results until the indexed extent
    /// bytes fit the cap. Pinned units are skipped — the observability
    /// rules are unaffected by the cap.
    pub fn enforce_cache_limit(&mut self) -> Result<EvictionSweep, StoreError> {
        let live = |store: &Store| -> Result<u64, StoreError> {
            Ok(store
                .conn
                .query_row("SELECT COALESCE(SUM(len), 0) FROM cas_extents", [], |r| {
                    r.get::<_, i64>(0)
                })? as u64)
        };
        let mut live_bytes = live(self)?;
        let mut evicted = 0usize;
        if live_bytes <= self.config.cache_limit {
            return Ok(EvictionSweep {
                evicted,
                live_bytes,
            });
        }
        let lru: Vec<(i64, Vec<u8>, Vec<u8>)> = {
            let mut stmt = self.conn.prepare(
                "SELECT key_kind, static_key, trace_digest FROM result_candidates
                 ORDER BY last_used ASC, memo_seq ASC",
            )?;
            let mapped = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            mapped.collect::<Result<_, _>>()?
        };
        for (kind, static_key, trace_digest) in lru {
            if live_bytes <= self.config.cache_limit {
                break;
            }
            let key_kind = if kind == KeyKind::BuildImport as i64 {
                KeyKind::BuildImport
            } else {
                KeyKind::Processor
            };
            let sk = crate::bundles::blob32(static_key);
            let td = crate::bundles::blob32(trace_digest);
            match self.evict_result(key_kind, &sk, &td) {
                Ok(true) => {
                    evicted += 1;
                    live_bytes = live(self)?;
                }
                Ok(false) => {}
                Err(StoreError::Pinned { .. }) => {} // never observable: skip
                Err(e) => return Err(e),
            }
        }
        Ok(EvictionSweep {
            evicted,
            live_bytes,
        })
    }

    /// Bitcask-style compaction (§13): copy live records into a new
    /// generation, flip `CURRENT`, flip the index in one transaction,
    /// then delete the old segments.
    pub fn compact(&mut self) -> Result<CompactionReport, StoreError> {
        let old_generation = self.cas.generation;
        let new_generation = old_generation + 1;
        let old_segments = self.cas.segments.clone();
        let old_bytes: u64 = old_segments
            .iter()
            .map(|segment| {
                let p = self.cas.dir.join(&segment.name);
                std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0)
            })
            .sum();

        // Liveness maps from the index.
        let mut live_extents: HashMap<[u8; 32], (u64, u64)> = HashMap::new();
        {
            let mut stmt = self
                .conn
                .prepare("SELECT content_hash, segment, offset FROM cas_extents")?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })?;
            for row in rows {
                let (hash, seg, off) = row?;
                live_extents.insert(crate::bundles::blob32(hash), (seg as u64, off as u64));
            }
        }
        // (key_kind, static_key, trace_digest) keyed by (segment, offset).
        type CandidateKey = (i64, Vec<u8>, Vec<u8>);
        let mut live_results: HashMap<(u64, u64), CandidateKey> = HashMap::new();
        {
            let mut stmt = self.conn.prepare(
                "SELECT segment, offset, key_kind, static_key, trace_digest FROM result_candidates",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, Vec<u8>>(3)?,
                    r.get::<_, Vec<u8>>(4)?,
                ))
            })?;
            for row in rows {
                let (seg, off, kind, sk, td) = row?;
                live_results.insert((seg as u64, off as u64), (kind, sk, td));
            }
        }

        // Copy live records into new segment buffers, rolling at the cap.
        struct NewSegment {
            id: u64,
            kind: SegmentKind,
            bytes: Vec<u8>,
        }
        let mut new_segments: Vec<NewSegment> = Vec::new();
        let mut next_id = self.cas.next_segment_id;
        let mut extent_moves: Vec<([u8; 32], u64, u64)> = Vec::new(); // hash, new seg, new payload offset
                                                                      // A candidate's key plus its new (segment, offset, len).
        type CandidateMove = ((i64, Vec<u8>, Vec<u8>), (u64, u64, u64));
        let mut candidate_moves: Vec<CandidateMove> = Vec::new();
        let mut records_copied = 0usize;

        for old_segment in &old_segments {
            let seg_id = old_segment.id;
            let path = self.cas.dir.join(&old_segment.name);
            let data = std::fs::read(&path).map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
            let mut pos: u64 = 0;
            while (pos as usize) < data.len() {
                let decoded = decode_record(&data[pos as usize..], seg_id, pos)?;
                let rec = &decoded.record;
                let payload_offset = pos
                    + RECORD_HEADER_LEN as u64
                    + rec.static_input_key.len() as u64
                    + rec.output_key.len() as u64;
                let live = match rec.kind {
                    RecordKind::Result => live_results.contains_key(&(seg_id, pos)),
                    _ => live_extents.get(&decoded.content_hash) == Some(&(seg_id, payload_offset)),
                };
                if live {
                    let oversize = decoded.encoded_len > self.config.segment_size;
                    let kind = if oversize {
                        SegmentKind::Oversize
                    } else {
                        SegmentKind::Regular
                    };
                    let roll = oversize
                        || match new_segments.last() {
                            None => true,
                            Some(s) => {
                                s.kind != SegmentKind::Regular
                                    || (!s.bytes.is_empty()
                                        && s.bytes.len() as u64 + decoded.encoded_len
                                            > self.config.segment_size)
                            }
                        };
                    if roll {
                        new_segments.push(NewSegment {
                            id: next_id,
                            kind,
                            bytes: Vec::new(),
                        });
                        next_id += 1;
                    }
                    let dst = new_segments.last_mut().expect("destination segment");
                    let new_offset = dst.bytes.len() as u64;
                    dst.bytes.extend_from_slice(
                        &data[pos as usize..(pos + decoded.encoded_len) as usize],
                    );
                    records_copied += 1;
                    match rec.kind {
                        RecordKind::Result => {
                            let key = live_results[&(seg_id, pos)].clone();
                            candidate_moves.push((key, (dst.id, new_offset, decoded.encoded_len)));
                        }
                        _ => {
                            let new_payload_offset = new_offset
                                + RECORD_HEADER_LEN as u64
                                + rec.static_input_key.len() as u64
                                + rec.output_key.len() as u64;
                            extent_moves.push((decoded.content_hash, dst.id, new_payload_offset));
                        }
                    }
                }
                pos += decoded.encoded_len;
            }
        }

        // Write the new segments durably (§13: new segments durable
        // before the index flips).
        for seg in &new_segments {
            let path = self.cas.dir.join(segment_file_name(seg.id, seg.kind));
            let mut f = std::fs::File::create(&path).map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
            f.write_all(&seg.bytes).map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
            f.sync_all().map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
        }
        manifest::fsync_dir(&self.cas.dir)?;

        // Atomically flip CURRENT — the single authority. A crash after
        // this point and before the SQLite transaction is the
        // generation-mismatch rebuild path.
        let new_names: Vec<ManifestSegment> = new_segments
            .iter()
            .map(|s| ManifestSegment {
                kind: s.kind,
                name: segment_file_name(s.id, s.kind),
            })
            .collect();
        manifest::write_current(
            &self.cas.dir,
            &GenerationManifest {
                generation: new_generation,
                segments: new_names,
            },
        )?;

        // One SQLite transaction flips the index.
        let txn = self.conn.transaction()?;
        txn.execute("DELETE FROM cas_segments", [])?;
        for seg in &new_segments {
            txn.execute(
                "INSERT INTO cas_segments(segment_id, file_name, segment_kind, indexed_len)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    seg.id as i64,
                    segment_file_name(seg.id, seg.kind),
                    seg.kind as i64,
                    seg.bytes.len() as i64
                ],
            )?;
        }
        for (hash, seg, offset) in &extent_moves {
            txn.execute(
                "UPDATE cas_extents SET segment = ?2, offset = ?3 WHERE content_hash = ?1",
                rusqlite::params![hash.as_slice(), *seg as i64, *offset as i64],
            )?;
        }
        for ((kind, sk, td), (seg, offset, len)) in &candidate_moves {
            txn.execute(
                "UPDATE result_candidates SET segment = ?4, offset = ?5, len = ?6
                 WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3",
                rusqlite::params![
                    kind,
                    sk.as_slice(),
                    td.as_slice(),
                    *seg as i64,
                    *offset as i64,
                    *len as i64
                ],
            )?;
        }
        meta_set_u64(&txn, "cas_generation", new_generation)?;
        txn.commit()?;

        // Old segments go last — no long-lived readers pin the old
        // generation in this store (module docs).
        for segment in &old_segments {
            let path = self.cas.dir.join(&segment.name);
            if path.exists() {
                std::fs::remove_file(&path).map_err(|source| StoreError::Io { path, source })?;
            }
        }
        manifest::fsync_dir(&self.cas.dir)?;

        let active_len = new_segments
            .last()
            .filter(|s| s.kind == SegmentKind::Regular)
            .map(|s| s.bytes.len() as u64)
            .unwrap_or(0);
        let new_bytes: u64 = new_segments.iter().map(|s| s.bytes.len() as u64).sum();
        self.cas = CasInner {
            dir: self.cas.dir.clone(),
            generation: new_generation,
            segments: new_segments
                .iter()
                .map(|s| SegmentInfo {
                    id: s.id,
                    name: segment_file_name(s.id, s.kind),
                    kind: s.kind,
                })
                .collect(),
            active_len,
            next_segment_id: next_id,
        };

        Ok(CompactionReport {
            old_generation,
            new_generation,
            records_copied,
            reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
        })
    }
}
