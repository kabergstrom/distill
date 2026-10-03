//! Startup recovery (§13). `cas_segments` names every segment; recovery
//! runs at open with the state directory's process lock held, so no other
//! process reads or writes the CAS and every segment is this process's.
//! Dead segments are deleted at once, open segments are sealed (writers
//! start new ones), and files no row names — a rolled-back allocation —
//! are deleted.
//!
//! The write transaction that indexes a record group is the group's
//! commit, and the index is the authority on what committed. Recovery
//! therefore reads no records:
//! - bytes past a segment's `indexed_len` belong to no committed group (a
//!   transaction that rolled back, or that a crash interrupted before
//!   COMMIT): the tail is truncated;
//! - a segment file shorter than its `indexed_len` lost committed records
//!   (an external truncation, a lying fsync): the index rows in the lost
//!   range go with everything that held them, whole results and installs,
//!   and the segment is indexed to its length. Every other segment's rows
//!   stay as they are.

use crate::cas::gc::evict_holder;
use crate::cas::store::{
    count_cas_write, fsync_dir, parse_segment_id, SegmentKind, HOLDER_RESULT, SEGMENT_DEAD,
    SEGMENT_OPEN, SEGMENT_SEALED,
};
use crate::db::Store;
use crate::error::StoreError;

/// What startup recovery found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Segments whose bytes past the index were cut off: (segment id,
    /// indexed length).
    pub truncated_tails: Vec<(u64, u64)>,
    /// Segments shorter than the index claimed: (segment id, file length).
    pub lost_tails: Vec<(u64, u64)>,
    /// Results and installs evicted because a lost tail held their bytes.
    pub evicted: usize,
    /// Segment files no `cas_segments` row names (a rolled-back
    /// allocation) — deleted.
    pub removed_stray_segments: Vec<String>,
    /// Dead segments deleted at startup.
    pub removed_dead_segments: Vec<u64>,
}

/// The holders of the extents segment `?1` holds at or past byte `?2`.
pub(crate) const LOST_EXTENT_HOLDERS: &str = "SELECT DISTINCT cas_refs.holder_kind, cas_refs.holder
     FROM cas_extents JOIN cas_refs ON cas_refs.content_hash = cas_extents.content_hash
     WHERE cas_extents.segment = ?1 AND cas_extents.offset + cas_extents.len > ?2";
/// The results whose records segment `?1` holds at or past byte `?2`.
pub(crate) const LOST_RESULTS: &str =
    "SELECT key_kind, static_key, trace_digest FROM result_candidates
     WHERE segment = ?1 AND offset + len > ?2";
/// The extents of segment `?1` at or past byte `?2`, once nothing holds
/// them.
pub(crate) const DROP_LOST_EXTENTS: &str =
    "DELETE FROM cas_extents WHERE segment = ?1 AND offset + len > ?2";

/// A result's `cas_refs` holder ([`crate::cas::store::result_holder`])
/// from its candidate row.
fn result_holder_of(key_kind: i64, static_key: &[u8], trace_digest: &[u8]) -> Vec<u8> {
    let mut holder = Vec::with_capacity(65);
    holder.push(key_kind as u8);
    holder.extend_from_slice(static_key);
    holder.extend_from_slice(trace_digest);
    holder
}

struct SegmentRow {
    id: u64,
    name: String,
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
                SegmentKind::from_i64(kind)
                    .filter(|kind| parse_segment_id(&name, *kind) == Some(id as u64))
                    .ok_or_else(|| StoreError::BadRecord {
                        segment: id as u64,
                        offset: 0,
                        detail: format!("segment row names an invalid file `{name}`"),
                    })?;
                out.push(SegmentRow {
                    id: id as u64,
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
        let named: std::collections::HashSet<&str> =
            live.iter().map(|row| row.name.as_str()).collect();
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

        // 3. Hold every segment to its index: cut uncommitted tails, and
        //    drop what lost tails held.
        let mut uncommitted = Vec::new();
        for row in &live {
            let path = dir.join(&row.name);
            let file_len = match std::fs::metadata(&path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(io(&path)(error)),
            };
            if file_len > row.indexed_len {
                uncommitted.push((path, row.id, row.indexed_len));
            } else if file_len < row.indexed_len {
                report.lost_tails.push((row.id, file_len));
            }
        }
        if !report.lost_tails.is_empty() {
            let lost = report.lost_tails.clone();
            report.evicted = self.write_txn(|store| {
                let txn = &*store.conn;
                count_cas_write(txn)?;
                let mut evicted = 0;
                for (segment, len) in &lost {
                    let params = rusqlite::params![*segment as i64, *len as i64];
                    let mut holders: Vec<(i64, Vec<u8>)> = Vec::new();
                    let mut extents = txn.prepare_cached(LOST_EXTENT_HOLDERS)?;
                    for row in extents.query_map(params, |row| Ok((row.get(0)?, row.get(1)?)))? {
                        holders.push(row?);
                    }
                    let mut results = txn.prepare_cached(LOST_RESULTS)?;
                    let rows = results.query_map(params, |row| {
                        Ok(result_holder_of(
                            row.get(0)?,
                            &row.get::<_, Vec<u8>>(1)?,
                            &row.get::<_, Vec<u8>>(2)?,
                        ))
                    })?;
                    for row in rows {
                        let row = (HOLDER_RESULT, row?);
                        if !holders.contains(&row) {
                            holders.push(row);
                        }
                    }
                    for (holder_kind, holder) in &holders {
                        evict_holder(txn, *holder_kind, holder)?;
                    }
                    evicted += holders.len();
                    txn.prepare_cached(DROP_LOST_EXTENTS)?.execute(params)?;
                    txn.execute(
                        "UPDATE cas_segments SET indexed_len = ?2 WHERE segment_id = ?1",
                        params,
                    )?;
                }
                Ok(evicted)
            })?;
        }
        for (path, id, indexed_len) in uncommitted {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(io(&path))?;
            file.set_len(indexed_len).map_err(io(&path))?;
            file.sync_all().map_err(io(&path))?;
            report.truncated_tails.push((id, indexed_len));
        }
        Ok(report)
    }
}
