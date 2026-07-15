//! File-tracking metadata (§13): `files` (per-root physical rows),
//! `dirty_files`, `rename_events`, root interning, and the derived
//! logical path index.
//!
//! Physical tracking is per root: multiple roots form one *logical*
//! namespace (§18), and a single-path key could hold only one of two
//! same-path observations, silently choosing a root. The logical index
//! derives as a multimap with three states — `Missing`, `Unique(root)`,
//! `Ambiguous(roots)` — and ambiguity is representable, not
//! pre-collapsed.
//!
//! Root ids are process-local interning (§18): committed records and
//! FILQ hashes carry the normalized root *name*; the numeric id never
//! leaves this store instance (the interning table lives in the same
//! disposable state and dies with it).

use distill_core::id::ContentHash;
use rusqlite::OptionalExtension;

use crate::db::{InputTxn, Store};
use crate::error::StoreError;
use crate::state::InputVersion;

/// A process-local interned root id (§18) — never serialized beyond this
/// store instance's disposable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    File,
    Directory,
    Symlink,
}

impl FileKind {
    fn to_i64(self) -> i64 {
        match self {
            FileKind::File => 0,
            FileKind::Directory => 1,
            FileKind::Symlink => 2,
        }
    }

    fn from_i64(v: i64) -> FileKind {
        match v {
            1 => FileKind::Directory,
            2 => FileKind::Symlink,
            _ => FileKind::File,
        }
    }
}

/// Last-known tree state for one (root, path) (§13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileState {
    pub mtime: i64,
    pub size: u64,
    pub kind: FileKind,
    pub content_hash: Option<ContentHash>,
}

/// The derived logical path index's three states (§13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPathState {
    Missing,
    Unique(RootId),
    Ambiguous(Vec<RootId>),
}

/// One pending-work entry (§13's `dirty_files`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyEntry {
    pub seq: i64,
    pub root: RootId,
    pub path: String,
    /// `true` = the path exists (create/update); `false` = deleted.
    pub exists: bool,
    /// Input version whose file observation this row represents.
    pub observation: InputVersion,
}

/// One ordered rename event (§13's `rename_events`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameEvent {
    pub seq: i64,
    pub root: RootId,
    pub from_path: String,
    pub to_path: String,
}

/// One stable snapshot of unacknowledged watcher work. A consumer may do
/// fallible work without holding the store mutex, then acknowledge exactly
/// this sequence prefix; rows appended meanwhile remain pending.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingFileWork {
    pub dirty: Vec<DirtyEntry>,
    pub renames: Vec<RenameEvent>,
}

impl PendingFileWork {
    pub fn is_empty(&self) -> bool {
        self.dirty.is_empty() && self.renames.is_empty()
    }
}

impl InputTxn<'_> {
    /// Intern a root name to its process-local id, creating it if new.
    pub fn intern_root(&mut self, name: &str) -> Result<RootId, StoreError> {
        if let Some(id) = self
            .txn
            .query_row("SELECT root_id FROM roots WHERE name = ?1", [name], |r| {
                r.get(0)
            })
            .optional()?
        {
            return Ok(RootId(id));
        }
        self.txn
            .execute("INSERT INTO roots(name) VALUES (?1)", [name])?;
        Ok(RootId(self.txn.last_insert_rowid()))
    }

    /// Record last-known tree state for one (root, path).
    pub fn upsert_file(
        &mut self,
        root: RootId,
        path: &str,
        state: &FileState,
        observation: InputVersion,
    ) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO files(root_id, path, mtime, size, kind, content_hash, observation)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(root_id, path) DO UPDATE SET
               mtime = excluded.mtime, size = excluded.size,
               kind = excluded.kind, content_hash = excluded.content_hash,
               observation = excluded.observation",
            rusqlite::params![
                root.0,
                path,
                state.mtime,
                state.size as i64,
                state.kind.to_i64(),
                state.content_hash.as_ref().map(|h| h.0.as_slice()),
                observation.0 as i64,
            ],
        )?;
        Ok(())
    }

    /// Remove a (root, path) row; `Ok(false)` when it was absent.
    pub fn remove_file(&mut self, root: RootId, path: &str) -> Result<bool, StoreError> {
        let n = self.txn.execute(
            "DELETE FROM files WHERE root_id = ?1 AND path = ?2",
            rusqlite::params![root.0, path],
        )?;
        Ok(n > 0)
    }

    /// Queue pending work (§13's `dirty_files`).
    pub fn push_dirty(
        &mut self,
        root: RootId,
        path: &str,
        exists: bool,
        observation: InputVersion,
    ) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO dirty_files(root_id, path, exists_flag, observation)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![root.0, path, exists as i64, observation.0 as i64],
        )?;
        Ok(())
    }

    /// Append to the ordered rename log (§13's `rename_events`).
    pub fn push_rename(&mut self, root: RootId, from: &str, to: &str) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO rename_events(root_id, from_path, to_path) VALUES (?1, ?2, ?3)",
            rusqlite::params![root.0, from, to],
        )?;
        Ok(())
    }

    /// Complete one captured watcher-work fold in this input transaction.
    /// The dirty prefix is removed only when every path still has the exact
    /// observation generation selected by the consumer. Rows appended after
    /// the captured prefix survive.
    pub fn acknowledge_file_work(&mut self, work: &PendingFileWork) -> Result<bool, StoreError> {
        let mut latest = std::collections::BTreeMap::new();
        for entry in &work.dirty {
            latest.insert((entry.root, entry.path.as_str()), entry);
        }
        for ((root, path), entry) in latest {
            let current = self
                .txn
                .query_row(
                    "SELECT observation FROM files WHERE root_id = ?1 AND path = ?2",
                    rusqlite::params![root.0, path],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            let matches = match (entry.exists, current) {
                (true, Some(observation)) => observation as u64 == entry.observation.0,
                (false, None) => true,
                _ => false,
            };
            if !matches {
                return Ok(false);
            }
        }
        if let Some(last) = work.dirty.last() {
            self.txn
                .execute("DELETE FROM dirty_files WHERE seq <= ?1", [last.seq])?;
        }
        if let Some(last) = work.renames.last() {
            self.txn
                .execute("DELETE FROM rename_events WHERE seq <= ?1", [last.seq])?;
        }
        Ok(true)
    }
}

impl Store {
    /// Snapshot all pending watcher work without consuming it. Downstream
    /// processing can fail or race a later publication without losing rows.
    pub fn pending_file_work(&self) -> Result<PendingFileWork, StoreError> {
        let mut dirty = Vec::new();
        {
            let mut statement = self.conn.prepare(
                "SELECT seq, root_id, path, exists_flag, observation
                 FROM dirty_files ORDER BY seq ASC",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(DirtyEntry {
                    seq: row.get(0)?,
                    root: RootId(row.get(1)?),
                    path: row.get(2)?,
                    exists: row.get::<_, i64>(3)? != 0,
                    observation: InputVersion(row.get::<_, i64>(4)? as u64),
                })
            })?;
            for row in rows {
                dirty.push(row?);
            }
        }
        let mut renames = Vec::new();
        {
            let mut statement = self.conn.prepare(
                "SELECT seq, root_id, from_path, to_path FROM rename_events ORDER BY seq ASC",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(RenameEvent {
                    seq: row.get(0)?,
                    root: RootId(row.get(1)?),
                    from_path: row.get(2)?,
                    to_path: row.get(3)?,
                })
            })?;
            for row in rows {
                renames.push(row?);
            }
        }
        Ok(PendingFileWork { dirty, renames })
    }

    /// Complete deterministic raw-tree projection used by startup
    /// reconciliation. Root ids remain process-local; callers cross the
    /// persistence boundary through [`Store::root_name`].
    pub fn all_files(&self) -> Result<Vec<(RootId, String, FileState)>, StoreError> {
        let mut statement = self.conn.prepare(
            "SELECT root_id, path, mtime, size, kind, content_hash
             FROM files ORDER BY root_id, path",
        )?;
        let rows = statement.query_map([], |row| {
            let content_hash = row.get::<_, Option<Vec<u8>>>(5)?.map(|bytes| {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&bytes);
                ContentHash(hash)
            });
            Ok((
                RootId(row.get(0)?),
                row.get(1)?,
                FileState {
                    mtime: row.get(2)?,
                    size: row.get::<_, i64>(3)? as u64,
                    kind: FileKind::from_i64(row.get(4)?),
                    content_hash,
                },
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    /// The name a root id was interned from.
    pub fn root_name(&self, root: RootId) -> Result<Option<String>, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT name FROM roots WHERE root_id = ?1", [root.0], |r| {
                r.get(0)
            })
            .optional()?)
    }

    /// Last-known state of one (root, path).
    pub fn file(&self, root: RootId, path: &str) -> Result<Option<FileState>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT mtime, size, kind, content_hash FROM files
                 WHERE root_id = ?1 AND path = ?2",
                rusqlite::params![root.0, path],
                |r| {
                    Ok(FileState {
                        mtime: r.get(0)?,
                        size: r.get::<_, i64>(1)? as u64,
                        kind: FileKind::from_i64(r.get(2)?),
                        content_hash: r.get::<_, Option<Vec<u8>>>(3)?.map(|b| {
                            let mut h = [0u8; 32];
                            h.copy_from_slice(&b);
                            ContentHash(h)
                        }),
                    })
                },
            )
            .optional()?)
    }

    /// The derived logical path index (§13): which roots hold `path`.
    pub fn logical_path(&self, path: &str) -> Result<LogicalPathState, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT root_id FROM files WHERE path = ?1 ORDER BY root_id")?;
        let roots: Vec<RootId> = stmt
            .query_map([path], |r| r.get::<_, i64>(0).map(RootId))?
            .collect::<Result<_, _>>()?;
        Ok(match roots.len() {
            0 => LogicalPathState::Missing,
            1 => LogicalPathState::Unique(roots[0]),
            _ => LogicalPathState::Ambiguous(roots),
        })
    }
}
