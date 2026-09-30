//! Startup recovery (§13): `CURRENT` is the single authority — SQLite
//! records the generation it indexed, and a mismatch discards the SQLite
//! artifact index and rebuilds it from the `CURRENT` generation's
//! segments before any read. Recovery scans forward from the last
//! indexed offset, verifies each payload against its stored blake3 (not
//! just CRC), and adopts committed groups or truncates the tail;
//! payload records not covered by a committed result record publish
//! nothing; duplicate content hashes keep the last and mark the rest
//! garbage.

use std::collections::{HashMap, HashSet};

use distill_core::id::AssetUuid;
use distill_wire::artifact::{parse_artifact, ARTIFACT_MAGIC};

use crate::cas::manifest;
use crate::cas::record::{
    decode_record, Record, RecordKind, ResultOutcome, ResultPayload, RECORD_HEADER_LEN,
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
            let txn = self.read.conn.savepoint()?;
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

        // 3. Scan every unindexed suffix first, then checkpoint all suffixes
        //    in one SQLite transaction. A recovery crash can therefore never
        //    advance an earlier segment past payloads whose result marker is
        //    in a later segment.
        struct ScanRange {
            segment: SegmentInfo,
            path: std::path::PathBuf,
            start: u64,
            file_len: u64,
            valid_end: u64,
        }
        struct ScannedRecord {
            segment: u64,
            offset: u64,
            payload_offset: u64,
            encoded_len: u64,
            content_hash: [u8; 32],
            record: Record,
            result: Option<ResultPayload>,
        }

        use rusqlite::OptionalExtension;
        let segments = self.cas.segments.clone();
        let mut ranges = Vec::with_capacity(segments.len());
        let mut rebuild_for_cursor_drift = false;
        for segment in &segments {
            let path = self.cas.dir.join(&segment.name);
            let file_len = std::fs::metadata(&path)
                .map_err(|source| StoreError::Io {
                    path: path.clone(),
                    source,
                })?
                .len();
            let start = self
                .conn
                .query_row(
                    "SELECT indexed_len FROM cas_segments WHERE segment_id = ?1",
                    [segment.id as i64],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .unwrap_or(0) as u64;
            rebuild_for_cursor_drift |= start > file_len;
            ranges.push(ScanRange {
                segment: segment.clone(),
                path,
                start,
                file_len,
                valid_end: start.min(file_len),
            });
        }
        if rebuild_for_cursor_drift {
            report.rebuilt_index = true;
            let transaction = self.read.conn.savepoint()?;
            transaction.execute("DELETE FROM cas_extents", [])?;
            transaction.execute("DELETE FROM result_candidates", [])?;
            transaction.execute("DELETE FROM derived_assertions", [])?;
            transaction.execute("DELETE FROM cas_segments", [])?;
            meta_set_u64(&transaction, "cas_generation", self.cas.generation)?;
            transaction.commit()?;
            for range in &mut ranges {
                range.start = 0;
                range.valid_end = 0;
            }
        }

        let mut scanned = Vec::new();
        let mut seen_hashes = HashSet::new();
        for range in &mut ranges {
            if range.start == range.file_len {
                continue;
            }
            let bytes = std::fs::read(&range.path).map_err(|source| StoreError::Io {
                path: range.path.clone(),
                source,
            })?;
            let mut position = range.start;
            while position < range.file_len {
                if range.segment.kind == manifest::SegmentKind::Oversize && position > 0 {
                    break;
                }
                let Ok(decoded) =
                    decode_record(&bytes[position as usize..], range.segment.id, position)
                else {
                    break;
                };
                let result = if decoded.record.kind == RecordKind::Result {
                    let Ok(payload) = ResultPayload::decode(&decoded.record.payload) else {
                        break;
                    };
                    Some(payload)
                } else {
                    if !seen_hashes.insert(decoded.content_hash) {
                        report.duplicate_payloads += 1;
                    }
                    None
                };
                let payload_offset = position
                    + RECORD_HEADER_LEN as u64
                    + decoded.record.static_input_key.len() as u64
                    + decoded.record.output_key.len() as u64;
                let encoded_len = decoded.encoded_len;
                scanned.push(ScannedRecord {
                    segment: range.segment.id,
                    offset: position,
                    payload_offset,
                    encoded_len,
                    content_hash: decoded.content_hash,
                    record: decoded.record,
                    result,
                });
                position += encoded_len;
                range.valid_end = position;
            }
        }

        // Build global coverage before indexing. This accepts a compacted log
        // whose selected duplicate payload appears after an older result, and
        // it makes wire-tree ownership follow the artifacts that name it.
        let mut last_payload = HashMap::new();
        let mut covered = HashSet::new();
        for (index, row) in scanned.iter().enumerate() {
            if let Some(result) = &row.result {
                if let ResultOutcome::Success { outputs, aux } = &result.outcome {
                    covered.extend(outputs.iter().map(|output| output.content_hash.0));
                    covered.extend(aux.iter().map(|auxiliary| auxiliary.content_hash.0));
                }
            } else {
                last_payload.insert(row.content_hash, index);
            }
        }
        let direct_outputs: Vec<_> = covered.iter().copied().collect();
        for hash in direct_outputs {
            let Some(index) = last_payload.get(&hash) else {
                continue;
            };
            let bytes = &scanned[*index].record.payload;
            if bytes.starts_with(&ARTIFACT_MAGIC) {
                if let Ok(artifact) = parse_artifact(bytes) {
                    covered.insert(artifact.layout_hash.0);
                }
            }
        }
        report.orphaned_payloads = last_payload
            .keys()
            .filter(|hash| !covered.contains(*hash))
            .count();

        let mut memo_counter = self.memo_seq().0;
        let transaction = self.read.conn.savepoint()?;
        for (hash, index) in &last_payload {
            if !covered.contains(hash) {
                continue;
            }
            let row = &scanned[*index];
            upsert_extent(
                &transaction,
                hash,
                row.segment,
                row.payload_offset,
                row.record.payload.len() as u64,
            )?;
        }
        for row in &scanned {
            let Some(payload) = &row.result else {
                continue;
            };
            memo_counter += 1;
            report.adopted_results += 1;
            upsert_candidate(
                &transaction,
                payload.key_kind,
                &crate::bundles::blob32(row.record.static_input_key.clone()),
                &payload.trace_digest(),
                MemoSeq(memo_counter),
                row.segment,
                row.offset,
                row.encoded_len,
            )?;
            if let ResultOutcome::Success { outputs, .. } = &payload.outcome {
                for output in outputs {
                    if output.output_key.is_empty() {
                        continue;
                    }
                    let child = AssetUuid::v5(row.record.asset_uuid, &output.output_key);
                    if derived_row_matches(
                        &transaction,
                        child,
                        row.record.asset_uuid,
                        &output.output_key,
                    )? {
                        transaction.execute(
                            "INSERT INTO derived_assertions(child_uuid, parent_uuid, output_key, memo_seq)
                             VALUES (?1, ?2, ?3, ?4)
                             ON CONFLICT(child_uuid, memo_seq) DO NOTHING",
                            rusqlite::params![
                                child.0.as_slice(),
                                row.record.asset_uuid.0.as_slice(),
                                output.output_key,
                                memo_counter as i64,
                            ],
                        )?;
                    }
                }
            }
        }
        for range in &ranges {
            if range.start == range.file_len {
                continue;
            }
            transaction.execute(
                "INSERT INTO cas_segments(segment_id, file_name, segment_kind, indexed_len)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(segment_id) DO UPDATE SET
                   file_name = excluded.file_name, segment_kind = excluded.segment_kind,
                   indexed_len = excluded.indexed_len",
                rusqlite::params![
                    range.segment.id as i64,
                    range.segment.name,
                    range.segment.kind as i64,
                    range.valid_end as i64,
                ],
            )?;
        }
        meta_set_u64(&transaction, "memo_seq", memo_counter)?;
        transaction.commit()?;

        for range in &ranges {
            if range.valid_end >= range.file_len {
                continue;
            }
            report
                .truncated_tails
                .push((range.segment.id, range.valid_end));
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&range.path)
                .map_err(|source| StoreError::Io {
                    path: range.path.clone(),
                    source,
                })?;
            file.set_len(range.valid_end)
                .map_err(|source| StoreError::Io {
                    path: range.path.clone(),
                    source,
                })?;
            file.sync_all().map_err(|source| StoreError::Io {
                path: range.path.clone(),
                source,
            })?;
            if segments.last().map(|segment| segment.id) == Some(range.segment.id)
                && range.segment.kind == manifest::SegmentKind::Regular
            {
                self.cas.active_len = range.valid_end;
            }
        }
        report.orphaned_payloads += self.prune_unreferenced_extents()?;

        Ok(report)
    }
}
