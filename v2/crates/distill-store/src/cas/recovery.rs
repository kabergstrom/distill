//! Startup recovery (§13). `cas_segments` names every segment; recovery
//! runs at open with the state directory's process lock held, so no other
//! process reads or writes the CAS and every segment is this process's.
//! Taking the lock ends every earlier writer, so its open segments are
//! sealed (this process's writers start new ones), and nothing can still
//! read a dead segment, so dead segments are deleted at once.
//!
//! The write transaction that indexes a record group is the group's
//! commit, and the index is the authority on what committed. Recovery
//! therefore reads no records:
//! - bytes past a segment's `indexed_len` belong to no committed group (a
//!   transaction that rolled back, or that a crash interrupted before
//!   COMMIT): the tail is truncated;
//! - a segment file shorter than its `indexed_len` lost committed records
//!   to something outside the store (an external truncation, a lying
//!   fsync). That breaks the filesystem's contract, not the store's: it is
//!   reported, and nothing is changed. A read of a lost record fails.
//!
//! Nothing looks for segment files no row names. `next_segment_id` is the
//! allocation's commit point: such a file belongs to an allocation that
//! never committed, and the next allocation of its id truncates it.

use crate::cas::store::{
    fsync_dir, segment_row_file_name, SEGMENT_DEAD, SEGMENT_OPEN, SEGMENT_SEALED,
};
use crate::db::Store;
use crate::error::StoreError;

/// What startup recovery found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Segments whose bytes past the index were cut off: (segment id,
    /// indexed length).
    pub truncated_tails: Vec<(u64, u64)>,
    /// Segments shorter than the index claims: (segment id, file
    /// length). Reported only; see the module docs.
    pub lost_tails: Vec<(u64, u64)>,
    /// Dead segments deleted at startup.
    pub removed_dead_segments: Vec<u64>,
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
                "SELECT segment_id, segment_kind, indexed_len, state
                 FROM cas_segments ORDER BY segment_id",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (id, kind, indexed_len, state) = row?;
                out.push(SegmentRow {
                    id: id as u64,
                    name: segment_row_file_name(id, kind)?,
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
                store.conn.execute(
                    "DELETE FROM cas_segments WHERE segment_id = ?1",
                    [*id as i64],
                )?;
            }
            store
                .conn
                .prepare_cached("UPDATE cas_segments SET state = ?1 WHERE state = ?2")?
                .execute([SEGMENT_SEALED, SEGMENT_OPEN])?;
            Ok(())
        })?;

        if !report.removed_dead_segments.is_empty() {
            fsync_dir(&dir)?;
        }

        // 2. Cut every uncommitted tail; report every lost one.
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
        for (segment, file_len) in &report.lost_tails {
            tracing::error!(
                segment,
                file_len,
                "a CAS segment file is shorter than its committed index: the filesystem lost \
                 committed records, and reads of them fail"
            );
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
