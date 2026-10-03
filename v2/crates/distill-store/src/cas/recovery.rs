//! Startup recovery (§13). `cas_segments` names every segment; recovery
//! runs at open with the state directory's process lock held, so no other
//! process reads or writes the CAS and every segment is this process's.
//! Dead segments are deleted at once, open segments are sealed (writers
//! start new ones), and files no row names — a rolled-back allocation —
//! are deleted.
//!
//! Recovery then scans each segment forward from its last indexed offset,
//! verifies each payload against its stored blake3 (not just CRC), and
//! adopts committed groups or truncates the tail; payload records not
//! covered by a committed result record publish nothing; duplicate
//! content hashes keep the last and mark the rest garbage. An index that
//! claims more of a segment than the file holds is discarded and rebuilt
//! from a full scan.

use std::collections::{HashMap, HashSet};

use crate::cas::record::{decode_record, Record, RecordKind, ResultOutcome, ResultPayload, RECORD_HEADER_LEN};
use crate::cas::store::{
    artifact_layout, extent_exists, fsync_dir, insert_ref, parse_segment_id,
    read_segment, result_holder, upsert_candidate, upsert_extent, SegmentKind, HOLDER_RESULT,
    SEGMENT_DEAD, SEGMENT_OPEN, SEGMENT_SEALED,
};
use crate::db::{meta_set_u64, Store};
use crate::error::StoreError;
use crate::state::MemoSeq;

/// What startup recovery found and did — the §13 classification, typed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// The index claimed more of a segment than its file holds: the
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
    /// Segment files no `cas_segments` row names (a rolled-back
    /// allocation) — deleted.
    pub removed_stray_segments: Vec<String>,
    /// Dead segments deleted at startup.
    pub removed_dead_segments: Vec<u64>,
}

struct SegmentRow {
    id: u64,
    name: String,
    kind: SegmentKind,
    indexed_len: u64,
    state: i64,
}

impl Store {
    /// §13 startup recovery. Runs before any read; leaves the index
    /// consistent with the (possibly truncated) segments.
    pub(crate) fn recover_cas(&mut self) -> Result<RecoveryReport, StoreError> {
        let mut report = RecoveryReport::default();
        let dir = self.cas.dir.clone();
        let io = |path: &std::path::Path| {
            let path = path.to_path_buf();
            move |source| StoreError::Io { path, source }
        };

        let rows: Vec<SegmentRow> = {
            let mut statement = self.conn.prepare(
                "SELECT segment_id, file_name, segment_kind, indexed_len, state
                 FROM cas_segments ORDER BY segment_id",
            )?;
            let rows = statement.query_map([], |row| {
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
                let (id, name, kind, indexed_len, state) = row?;
                out.push(SegmentRow {
                    id: id as u64,
                    kind: SegmentKind::from_i64(kind)
                        .filter(|kind| parse_segment_id(&name, *kind) == Some(id as u64))
                        .ok_or_else(|| StoreError::BadRecord {
                            segment: id as u64,
                            offset: 0,
                            detail: format!("segment row names an invalid file `{name}`"),
                        })?,
                    name,
                    indexed_len: indexed_len as u64,
                    state,
                });
            }
            out
        };

        // 1. Dead segments go now: no other process can be reading them.
        //    Open segments are sealed; this process's writers start new ones.
        let mut live = Vec::new();
        for row in rows {
            if row.state == SEGMENT_DEAD {
                let path = dir.join(&row.name);
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(io(&path)(error)),
                }
                report.removed_dead_segments.push(row.id);
            } else {
                live.push(row);
            }
        }
        let dead = report.removed_dead_segments.clone();
        self.write_txn(|store| {
            for id in &dead {
                store
                    .conn
                    .execute("DELETE FROM cas_segments WHERE segment_id = ?1", [*id as i64])?;
            }
            store.conn.execute(
                "UPDATE cas_segments SET state = ?1 WHERE state = ?2",
                [SEGMENT_SEALED, SEGMENT_OPEN],
            )?;
            Ok(())
        })?;

        // 2. Remove segment files no row names. Deletion fsyncs the
        //    directory (§13).
        let named: HashSet<&str> = live.iter().map(|row| row.name.as_str()).collect();
        let mut removed_any = !report.removed_dead_segments.is_empty();
        for entry in std::fs::read_dir(&dir).map_err(io(&dir))? {
            let entry = entry.map_err(io(&dir))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".dsr") && !named.contains(name.as_str()) {
                let path = entry.path();
                std::fs::remove_file(&path).map_err(io(&path))?;
                removed_any = true;
                report.removed_stray_segments.push(name);
            }
        }
        if removed_any {
            fsync_dir(&dir)?;
        }
        report.removed_stray_segments.sort();

        // 3. Scan every unindexed suffix first, then checkpoint all suffixes
        //    in one SQLite transaction. A recovery crash can therefore never
        //    advance an earlier segment past payloads whose result marker is
        //    in a later segment.
        struct ScanRange {
            id: u64,
            kind: SegmentKind,
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

        let mut ranges = Vec::with_capacity(live.len());
        let mut rebuild_for_cursor_drift = false;
        for row in &live {
            let path = dir.join(&row.name);
            let file_len = match std::fs::metadata(&path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(io(&path)(error)),
            };
            rebuild_for_cursor_drift |= row.indexed_len > file_len;
            ranges.push(ScanRange {
                id: row.id,
                kind: row.kind,
                path,
                start: row.indexed_len,
                file_len,
                valid_end: row.indexed_len.min(file_len),
            });
        }
        if rebuild_for_cursor_drift {
            report.rebuilt_index = true;
            self.write_txn(|store| {
                let txn = &*store.conn;
                txn.execute("DELETE FROM cas_refs", [])?;
                txn.execute("DELETE FROM cas_extents", [])?;
                txn.execute("DELETE FROM result_candidates", [])?;
                txn.execute("UPDATE cas_segments SET indexed_len = 0", [])?;
                Ok(())
            })?;
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
            let bytes = read_segment(&range.path)?;
            let mut position = range.start;
            while position < range.file_len {
                if range.kind == SegmentKind::Oversize && position > 0 {
                    break;
                }
                let Ok(decoded) = decode_record(&bytes[position as usize..], range.id, position)
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
                    segment: range.id,
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

        // Build global coverage before indexing. Wire-tree ownership follows
        // the artifacts that name it.
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
        // Each adopted result's unit: outputs, aux, and the outputs' wire
        // trees, read from this scan or from the existing index.
        let mut units = Vec::new();
        for row in &scanned {
            let Some(result) = &row.result else {
                units.push(Vec::new());
                continue;
            };
            let mut unit = Vec::new();
            if let ResultOutcome::Success { outputs, aux } = &result.outcome {
                for output in outputs {
                    unit.push(output.content_hash.0);
                    let layout = match last_payload.get(&output.content_hash.0) {
                        Some(index) => artifact_layout(&scanned[*index].record.payload),
                        None => self
                            .cas_read(&output.content_hash.0)
                            .ok()
                            .and_then(|bytes| artifact_layout(&bytes)),
                    };
                    unit.extend(layout);
                }
                unit.extend(aux.iter().map(|auxiliary| auxiliary.content_hash.0));
            }
            covered.extend(unit.iter().copied());
            units.push(unit);
        }
        report.orphaned_payloads = last_payload
            .keys()
            .filter(|hash| !covered.contains(*hash))
            .count();

        let mut memo_counter = self.memo_seq().0;
        let mut adopted = 0usize;
        self.write_txn(|store| {
            let transaction = &*store.conn;
            for (hash, index) in &last_payload {
                if !covered.contains(hash) {
                    continue;
                }
                let row = &scanned[*index];
                upsert_extent(
                    transaction,
                    hash,
                    row.segment,
                    row.payload_offset,
                    row.record.payload.len() as u64,
                )?;
            }
            for (row, unit) in scanned.iter().zip(&units) {
                let Some(payload) = &row.result else {
                    continue;
                };
                let mut complete = true;
                for hash in unit {
                    complete &= extent_exists(transaction, hash)?;
                }
                if !complete {
                    // A result whose outputs are gone publishes nothing.
                    continue;
                }
                memo_counter += 1;
                adopted += 1;
                let static_key = crate::bundles::blob32(row.record.static_input_key.clone());
                let trace_digest = payload.trace_digest();
                upsert_candidate(
                    transaction,
                    payload.key_kind,
                    &static_key,
                    &trace_digest,
                    MemoSeq(memo_counter),
                    row.segment,
                    row.offset,
                    row.encoded_len,
                )?;
                let holder = result_holder(payload.key_kind, &static_key, &trace_digest);
                for hash in unit {
                    insert_ref(transaction, HOLDER_RESULT, &holder, hash)?;
                }
            }
            for range in &ranges {
                if range.start == range.file_len {
                    continue;
                }
                transaction.execute(
                    "UPDATE cas_segments SET indexed_len = ?2 WHERE segment_id = ?1",
                    rusqlite::params![range.id as i64, range.valid_end as i64],
                )?;
            }
            meta_set_u64(transaction, "memo_seq", memo_counter)
        })?;
        report.adopted_results = adopted;

        for range in &ranges {
            if range.valid_end >= range.file_len {
                continue;
            }
            report.truncated_tails.push((range.id, range.valid_end));
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&range.path)
                .map_err(io(&range.path))?;
            file.set_len(range.valid_end).map_err(io(&range.path))?;
            file.sync_all().map_err(io(&range.path))?;
        }
        report.orphaned_payloads += self.prune_unreferenced_extents()?;

        Ok(report)
    }
}
