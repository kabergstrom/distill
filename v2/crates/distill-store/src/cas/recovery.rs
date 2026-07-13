//! Startup recovery (§13): `CURRENT` is the single authority — SQLite
//! records the generation it indexed, and a mismatch discards the SQLite
//! artifact index and rebuilds it from the `CURRENT` generation's
//! segments before any read. Recovery scans forward from the last
//! indexed offset, verifies each payload against its stored blake3 (not
//! just CRC), and adopts committed groups or truncates the tail;
//! payload records not covered by a committed result record publish
//! nothing; duplicate content hashes keep the last and mark the rest
//! garbage.

use std::collections::HashMap;

use distill_core::id::AssetUuid;

use crate::cas::manifest;
use crate::cas::record::{
    decode_record, RecordKind, ResultOutcome, ResultPayload, RECORD_HEADER_LEN,
};
use crate::cas::store::{
    derived_row_matches, parse_segment_id, upsert_candidate, upsert_extent, CasInner, SegmentInfo,
};
use crate::db::{meta_get_u64, meta_set_u64, Store};
use crate::error::StoreError;
use crate::state::MemoSeq;

/// What startup recovery found and did — the §13 classification, typed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// The SQLite-recorded generation disagreed with `CURRENT`: the
    /// artifact index was discarded and rebuilt from a full segment scan.
    pub rebuilt_index: bool,
    /// Committed groups adopted from unindexed segment tails (or the
    /// whole log on rebuild).
    pub adopted_results: usize,
    /// Valid payload records covered by no committed result record —
    /// garbage, published nowhere (§13's crash-after-output-2-of-3).
    pub orphaned_payloads: usize,
    /// Segments whose tail failed verification, truncated to the last
    /// valid record boundary: (segment id, valid length).
    pub truncated_tails: Vec<(u64, u64)>,
    /// Duplicate content hashes encountered — byte-identical by
    /// definition; the last occurrence wins, the rest are garbage.
    pub duplicate_payloads: usize,
    /// Segment files on disk but absent from `CURRENT` (interrupted
    /// roll or compaction leftovers) — deleted; `CURRENT` is authority.
    pub removed_stray_segments: Vec<String>,
}

impl Store {
    /// Initialize the in-memory CAS cursor from `CURRENT`.
    pub(crate) fn init_cas(&mut self) -> Result<(), StoreError> {
        let dir = self.config.state_path.join("cas");
        let current = manifest::read_current(&dir)?;
        let mut segments = Vec::with_capacity(current.segments.len());
        for segment in &current.segments {
            let id = parse_segment_id(&segment.name, segment.kind).ok_or_else(|| {
                StoreError::BadGenerationManifest {
                    path: manifest::current_path(&dir),
                    detail: format!(
                        "unparseable {:?} segment name `{}`",
                        segment.kind, segment.name
                    ),
                }
            })?;
            segments.push(SegmentInfo {
                id,
                name: segment.name.clone(),
                kind: segment.kind,
            });
        }
        let next_segment_id = segments
            .iter()
            .map(|s| s.id)
            .max()
            .map(|id| id + 1)
            .unwrap_or(0);
        let active_len = match segments.last() {
            Some(segment) if segment.kind == manifest::SegmentKind::Regular => {
                let path = dir.join(&segment.name);
                std::fs::metadata(&path)
                    .map_err(|source| StoreError::Io { path, source })?
                    .len()
            }
            _ => 0,
        };
        self.cas = CasInner {
            dir,
            generation: current.generation,
            segments,
            active_len,
            next_segment_id,
        };
        Ok(())
    }

    /// §13 startup recovery. Runs before any read; leaves the index
    /// consistent with the (possibly truncated) segments.
    pub(crate) fn recover_cas(&mut self) -> Result<RecoveryReport, StoreError> {
        let mut report = RecoveryReport::default();

        // 1. CURRENT is the single authority: a generation mismatch
        //    discards the whole artifact index before any read.
        let indexed_generation = meta_get_u64(&self.conn, "cas_generation")?.unwrap_or(0);
        if indexed_generation != self.cas.generation {
            report.rebuilt_index = true;
            let txn = self.conn.transaction()?;
            txn.execute("DELETE FROM cas_extents", [])?;
            txn.execute("DELETE FROM result_candidates", [])?;
            txn.execute("DELETE FROM derived_assertions", [])?;
            txn.execute("DELETE FROM cas_segments", [])?;
            meta_set_u64(&txn, "cas_generation", self.cas.generation)?;
            txn.commit()?;
        }

        // 2. Remove stray segment files CURRENT does not name — an
        //    interrupted roll or a completed compaction's leftovers.
        //    Deletion fsyncs the directory (§13).
        let named: std::collections::HashSet<&str> =
            self.cas.segments.iter().map(|s| s.name.as_str()).collect();
        let entries = std::fs::read_dir(&self.cas.dir).map_err(|source| StoreError::Io {
            path: self.cas.dir.clone(),
            source,
        })?;
        let mut removed_any = false;
        for entry in entries {
            let entry = entry.map_err(|source| StoreError::Io {
                path: self.cas.dir.clone(),
                source,
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let is_stray_segment = name.ends_with(".dsr") && !named.contains(name.as_str());
            let is_stale_tmp = name == "CURRENT.tmp";
            if is_stray_segment || is_stale_tmp {
                let path = entry.path();
                std::fs::remove_file(&path).map_err(|source| StoreError::Io { path, source })?;
                removed_any = true;
                if is_stray_segment {
                    report.removed_stray_segments.push(name);
                }
            }
        }
        if removed_any {
            manifest::fsync_dir(&self.cas.dir)?;
        }
        report.removed_stray_segments.sort();

        // 3. Scan each CURRENT segment forward from its last indexed
        //    offset, adopting committed groups and truncating torn tails.
        let segments = self.cas.segments.clone();
        let mut memo_counter = self.memo_seq().0;
        // Payload records seen in the unindexed region, keyed by content
        // hash, valid until a result record covers them (last wins).
        let mut pending: HashMap<[u8; 32], (u64, u64, u64)> = HashMap::new();
        let mut seen_hashes: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();

        for segment in &segments {
            let segment_id = segment.id;
            let name = &segment.name;
            let path = self.cas.dir.join(name);
            use rusqlite::OptionalExtension;
            let indexed: Option<i64> = self
                .conn
                .query_row(
                    "SELECT indexed_len FROM cas_segments WHERE segment_id = ?1",
                    [segment_id as i64],
                    |r| r.get(0),
                )
                .optional()?;
            let mut indexed = indexed.unwrap_or(0) as u64;
            let file_len = std::fs::metadata(&path)
                .map_err(|source| StoreError::Io {
                    path: path.clone(),
                    source,
                })?
                .len();
            if indexed > file_len {
                // The index claims more than the file holds — the two
                // stores disagree below the generation level. Never
                // trust them to agree on their own: rescan this segment
                // from scratch after dropping its index rows.
                let txn = self.conn.transaction()?;
                txn.execute(
                    "DELETE FROM cas_extents WHERE segment = ?1",
                    [segment_id as i64],
                )?;
                txn.execute(
                    "DELETE FROM result_candidates WHERE segment = ?1",
                    [segment_id as i64],
                )?;
                txn.commit()?;
                indexed = 0;
            }
            if indexed == file_len {
                continue;
            }

            let bytes = std::fs::read(&path).map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
            let mut pos = indexed;
            let mut valid_end = indexed;
            let txn = self.conn.transaction()?;
            while pos < file_len {
                if segment.kind == manifest::SegmentKind::Oversize && pos > 0 {
                    break;
                }
                match decode_record(&bytes[pos as usize..], segment_id, pos) {
                    Ok(decoded) => {
                        let rec = &decoded.record;
                        let payload_offset = pos
                            + RECORD_HEADER_LEN as u64
                            + rec.static_input_key.len() as u64
                            + rec.output_key.len() as u64;
                        match rec.kind {
                            RecordKind::WireTree => {
                                // Wire trees index from the scan like
                                // every extent (§13): content-addressed
                                // and idempotent; their lifecycle is the
                                // observability rule, not result
                                // coverage.
                                if !seen_hashes.insert(decoded.content_hash) {
                                    report.duplicate_payloads += 1;
                                }
                                upsert_extent(
                                    &txn,
                                    &decoded.content_hash,
                                    segment_id,
                                    payload_offset,
                                    rec.payload.len() as u64,
                                )?;
                            }
                            RecordKind::Result => {
                                let Ok(payload) = ResultPayload::decode(&rec.payload) else {
                                    // A result record whose payload does
                                    // not parse is torn garbage: the
                                    // commit marker never became valid.
                                    break;
                                };
                                memo_counter += 1;
                                report.adopted_results += 1;
                                let mut covered: Vec<[u8; 32]> = Vec::new();
                                if let ResultOutcome::Success { outputs, aux } = &payload.outcome {
                                    for row in outputs {
                                        covered.push(row.content_hash.0);
                                    }
                                    for row in aux {
                                        covered.push(row.content_hash.0);
                                    }
                                }
                                for hash in covered {
                                    if let Some((seg, off, len)) = pending.remove(&hash) {
                                        upsert_extent(&txn, &hash, seg, off, len)?;
                                    }
                                }
                                upsert_candidate(
                                    &txn,
                                    payload.key_kind,
                                    &crate::bundles::blob32(rec.static_input_key.clone()),
                                    &payload.trace_digest(),
                                    MemoSeq(memo_counter),
                                    segment_id,
                                    pos,
                                    decoded.encoded_len,
                                )?;
                                // Re-verify derived assertions against
                                // the namespace index — memo data, never
                                // a claim (§9).
                                if let ResultOutcome::Success { outputs, .. } = &payload.outcome {
                                    for row in outputs {
                                        if row.output_key.is_empty() {
                                            continue;
                                        }
                                        let child = AssetUuid::v5(rec.asset_uuid, &row.output_key);
                                        if derived_row_matches(
                                            &txn,
                                            child,
                                            rec.asset_uuid,
                                            &row.output_key,
                                        )? {
                                            txn.execute(
                                                "INSERT INTO derived_assertions(child_uuid, parent_uuid, output_key, memo_seq)
                                                 VALUES (?1, ?2, ?3, ?4)
                                                 ON CONFLICT(child_uuid, memo_seq) DO NOTHING",
                                                rusqlite::params![
                                                    child.0.as_slice(),
                                                    rec.asset_uuid.0.as_slice(),
                                                    row.output_key,
                                                    memo_counter as i64,
                                                ],
                                            )?;
                                        }
                                    }
                                }
                            }
                            _ => {
                                // Import encoding / processor output /
                                // debug: held pending until a committed
                                // result covers them.
                                if !seen_hashes.insert(decoded.content_hash) {
                                    report.duplicate_payloads += 1;
                                }
                                if pending
                                    .insert(
                                        decoded.content_hash,
                                        (segment_id, payload_offset, rec.payload.len() as u64),
                                    )
                                    .is_some()
                                {
                                    // last wins; the displaced entry was
                                    // already counted as duplicate above.
                                }
                            }
                        }
                        pos += decoded.encoded_len;
                        valid_end = pos;
                    }
                    Err(_) => break,
                }
            }
            txn.execute(
                "INSERT INTO cas_segments(segment_id, file_name, segment_kind, indexed_len)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(segment_id) DO UPDATE SET
                   file_name = excluded.file_name, segment_kind = excluded.segment_kind,
                   indexed_len = excluded.indexed_len",
                rusqlite::params![
                    segment_id as i64,
                    name,
                    segment.kind as i64,
                    valid_end as i64
                ],
            )?;
            meta_set_u64(&txn, "memo_seq", memo_counter)?;
            txn.commit()?;

            if valid_end < file_len {
                // Torn tail: truncate to the last valid record boundary.
                report.truncated_tails.push((segment_id, valid_end));
                let f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(|source| StoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                f.set_len(valid_end).map_err(|source| StoreError::Io {
                    path: path.clone(),
                    source,
                })?;
                f.sync_all().map_err(|source| StoreError::Io {
                    path: path.clone(),
                    source,
                })?;
                if segments.last().map(|s| s.id) == Some(segment_id)
                    && segment.kind == manifest::SegmentKind::Regular
                {
                    self.cas.active_len = valid_end;
                }
            }
        }
        report.orphaned_payloads = pending.len();
        self.set_memo_seq(memo_counter);

        Ok(report)
    }
}
