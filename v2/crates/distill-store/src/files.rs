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

use crate::db::{InputTxn, Store, StoreReader};
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

/// The scanner's complete record of one (root, path): its tree state plus
/// the on-disk spelling and, for a symlink, its canonical target. Both paths
/// are in the daemon's platform path encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileObservation {
    pub state: FileState,
    pub raw_path: Vec<u8>,
    pub symlink_target: Option<Vec<u8>>,
}

impl From<FileState> for FileObservation {
    /// A row with no recorded spelling or symlink target.
    fn from(state: FileState) -> Self {
        Self {
            state,
            raw_path: Vec::new(),
            symlink_target: None,
        }
    }
}

/// One `files` row by root name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedFile {
    pub root_name: String,
    pub path: String,
    pub file: FileObservation,
}

/// One traversed directory (§13 `directories`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedDirectory {
    pub root_name: String,
    pub path: String,
    pub canonical_path: Vec<u8>,
    pub physical_path: Vec<u8>,
}

/// One non-fatal scan exclusion (§13 `scan_diagnostics`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedDiagnostic {
    pub root_name: String,
    pub path: String,
    pub detail: Vec<u8>,
}

/// The bytes the scan read for one `.bundle` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedBundleFile {
    pub root_name: String,
    pub path: String,
    pub bytes: Vec<u8>,
}

impl InputTxn<'_> {
    /// Intern a root name to its process-local id, creating it if new.
    pub fn intern_root(&mut self, name: &str) -> Result<RootId, StoreError> {
        intern_root(&self.txn, name)
    }

    /// Record the scanner's observation of one (root, path).
    pub fn upsert_file(
        &mut self,
        root: RootId,
        path: &str,
        file: &FileObservation,
        observation: InputVersion,
    ) -> Result<(), StoreError> {
        let state = &file.state;
        self.txn.execute(
            "INSERT INTO files(root_id, path, mtime, size, kind, content_hash, observation,
                               raw_path, symlink_target)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(root_id, path) DO UPDATE SET
               mtime = excluded.mtime, size = excluded.size,
               kind = excluded.kind, content_hash = excluded.content_hash,
               observation = excluded.observation, raw_path = excluded.raw_path,
               symlink_target = excluded.symlink_target",
            rusqlite::params![
                root.0,
                path,
                state.mtime,
                state.size as i64,
                state.kind.to_i64(),
                state.content_hash.as_ref().map(|h| h.0.as_slice()),
                observation.0 as i64,
                file.raw_path,
                file.symlink_target,
            ],
        )?;
        Ok(())
    }

    /// Remove a (root, path) row and its bundle bytes; `Ok(false)` when it
    /// was absent.
    pub fn remove_file(&mut self, root: RootId, path: &str) -> Result<bool, StoreError> {
        self.txn.execute(
            "DELETE FROM bundle_files WHERE root_id = ?1 AND path = ?2",
            rusqlite::params![root.0, path],
        )?;
        let n = self.txn.execute(
            "DELETE FROM files WHERE root_id = ?1 AND path = ?2",
            rusqlite::params![root.0, path],
        )?;
        Ok(n > 0)
    }

    /// Record the bytes the scan read for a `.bundle` file's row.
    pub fn set_bundle_file(
        &mut self,
        root: RootId,
        path: &str,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO bundle_files(root_id, path, bytes, hash) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(root_id, path) DO UPDATE SET bytes = excluded.bytes, hash = excluded.hash",
            rusqlite::params![root.0, path, bytes, blake3::hash(bytes).as_bytes().as_slice()],
        )?;
        Ok(())
    }

    /// Replace the directory and diagnostic rows under `under` (every row
    /// when `None`) with a new observation of that subtree.
    pub fn replace_scan_structure(
        &mut self,
        under: Option<&[(String, String)]>,
        directories: &[ObservedDirectory],
        diagnostics: &[ObservedDiagnostic],
    ) -> Result<(), StoreError> {
        clear_structure(&self.txn, under, true)?;
        for directory in directories {
            let root = intern_root(&self.txn, &directory.root_name)?;
            self.txn.execute(
                "INSERT INTO directories(root_id, path, canonical_path, physical_path)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    root.0,
                    directory.path,
                    directory.canonical_path,
                    directory.physical_path
                ],
            )?;
        }
        insert_diagnostics(&self.txn, diagnostics)
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
}

pub(crate) fn intern_root(conn: &rusqlite::Connection, name: &str) -> Result<RootId, StoreError> {
    if let Some(id) = conn
        .query_row("SELECT root_id FROM roots WHERE name = ?1", [name], |r| {
            r.get(0)
        })
        .optional()?
    {
        return Ok(RootId(id));
    }
    conn.execute("INSERT INTO roots(name) VALUES (?1)", [name])?;
    Ok(RootId(conn.last_insert_rowid()))
}

/// Rows of `table` (aliased `t`, joined to `roots` as `r`) under one
/// (root name, prefix) subtree bound as `?1`, `?2`.
pub(crate) const UNDER: &str = "r.name = ?1 AND (?2 = '' OR t.path = ?2
     OR (t.path >= ?2 || '/' AND t.path < ?2 || '0'))";

fn clear_structure(
    conn: &rusqlite::Connection,
    under: Option<&[(String, String)]>,
    directories: bool,
) -> Result<(), StoreError> {
    let tables: &[&str] = if directories {
        &["directories", "scan_diagnostics"]
    } else {
        &["scan_diagnostics"]
    };
    for table in tables {
        match under {
            None => {
                conn.execute(&format!("DELETE FROM {table}"), [])?;
            }
            Some(prefixes) => {
                for (root, prefix) in prefixes {
                    conn.execute(
                        &format!(
                            "DELETE FROM {table} WHERE rowid IN (
                               SELECT t.rowid FROM {table} t JOIN roots r USING (root_id)
                               WHERE {UNDER})"
                        ),
                        rusqlite::params![root, prefix],
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn insert_diagnostics(
    conn: &rusqlite::Connection,
    diagnostics: &[ObservedDiagnostic],
) -> Result<(), StoreError> {
    for diagnostic in diagnostics {
        let root = intern_root(conn, &diagnostic.root_name)?;
        conn.execute(
            "INSERT INTO scan_diagnostics(root_id, path, detail) VALUES (?1, ?2, ?3)
             ON CONFLICT(root_id, path) DO UPDATE SET detail = excluded.detail",
            rusqlite::params![root.0, diagnostic.path, diagnostic.detail],
        )?;
    }
    Ok(())
}

impl Store {
    /// Replace the diagnostic rows under `under` (every row when `None`)
    /// without publishing an input version: diagnostics are scanner state,
    /// not authored input.
    pub fn replace_scan_diagnostics(
        &mut self,
        under: Option<&[(String, String)]>,
        diagnostics: &[ObservedDiagnostic],
    ) -> Result<(), StoreError> {
        self.write_txn(|store| {
            let transaction = store.read.conn.savepoint()?;
            clear_structure(&transaction, under, false)?;
            insert_diagnostics(&transaction, diagnostics)?;
            transaction.commit()?;
            Ok(())
        })
    }


    /// Clear the watcher work a pass has completed without fabricating a new
    /// input snapshot. Each captured path is compared on its own: when the
    /// stored observation still matches the newest captured entry for that
    /// path, its rows up to that entry are cleared; when it does not, more
    /// work arrived for the path and every row for it stays pending for the
    /// next pass. Rows appended after the capture survive either way, and the
    /// captured rename events, which the pass has applied, are cleared.
    /// Returns whether every captured path was cleared.
    pub fn acknowledge_file_work(&mut self, work: &PendingFileWork) -> Result<bool, StoreError> {
        self.write_txn(|store| {
            let transaction = store.read.conn.savepoint()?;
            let mut latest = std::collections::BTreeMap::new();
            for entry in &work.dirty {
                latest.insert((entry.root, entry.path.as_str()), entry);
            }
            let mut complete = true;
            for ((root, path), entry) in latest {
                let current = transaction
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
                if matches {
                    transaction.execute(
                        "DELETE FROM dirty_files WHERE root_id = ?1 AND path = ?2 AND seq <= ?3",
                        rusqlite::params![root.0, path, entry.seq],
                    )?;
                } else {
                    complete = false;
                }
            }
            if let Some(last) = work.renames.last() {
                transaction.execute("DELETE FROM rename_events WHERE seq <= ?1", [last.seq])?;
            }
            transaction.commit()?;
            Ok(complete)
        })
    }
}

impl StoreReader {
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

    /// The id a root name was interned as, if it was.
    pub fn root_id(&self, name: &str) -> Result<Option<RootId>, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT root_id FROM roots WHERE name = ?1", [name], |r| r.get(0))
            .optional()?
            .map(RootId))
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

const OBSERVED_FILE: &str = "SELECT r.name, t.path, t.mtime, t.size, t.kind, t.content_hash,
            t.raw_path, t.symlink_target
     FROM files t JOIN roots r USING (root_id)";

fn observed_file_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservedFile> {
    Ok(ObservedFile {
        root_name: row.get(0)?,
        path: row.get(1)?,
        file: FileObservation {
            state: FileState {
                mtime: row.get(2)?,
                size: row.get::<_, i64>(3)? as u64,
                kind: FileKind::from_i64(row.get(4)?),
                content_hash: row.get::<_, Option<Vec<u8>>>(5)?.map(|bytes| {
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(&bytes);
                    ContentHash(hash)
                }),
            },
            raw_path: row.get(6)?,
            symlink_target: row.get(7)?,
        },
    })
}

fn observed_directory_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservedDirectory> {
    Ok(ObservedDirectory {
        root_name: row.get(0)?,
        path: row.get(1)?,
        canonical_path: row.get(2)?,
        physical_path: row.get(3)?,
    })
}

impl StoreReader {
    pub(crate) fn query_rows<T, P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>, StoreError> {
        let mut statement = self.conn.prepare_cached(sql)?;
        let rows = statement.query_map(params, map)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    /// Every scanned file row, in (root name, path) order.
    pub fn observed_files(&self) -> Result<Vec<ObservedFile>, StoreError> {
        self.query_rows(
            &format!("{OBSERVED_FILE} ORDER BY r.name, t.path"),
            [],
            observed_file_row,
        )
    }

    /// The scanned rows at or below `prefix` in `root_name` (the whole root
    /// when `prefix` is empty), in path order.
    pub fn observed_files_under(
        &self,
        root_name: &str,
        prefix: &str,
    ) -> Result<Vec<ObservedFile>, StoreError> {
        self.query_rows(
            &format!("{OBSERVED_FILE} WHERE {UNDER} ORDER BY t.path"),
            rusqlite::params![root_name, prefix],
            observed_file_row,
        )
    }

    /// The scanned rows at logical `path`, one per root holding it.
    pub fn observed_files_at(&self, path: &str) -> Result<Vec<ObservedFile>, StoreError> {
        self.query_rows(
            &format!("{OBSERVED_FILE} WHERE t.path = ?1 ORDER BY r.name"),
            [path],
            observed_file_row,
        )
    }

    /// One scanned row.
    pub fn observed_file(
        &self,
        root_name: &str,
        path: &str,
    ) -> Result<Option<ObservedFile>, StoreError> {
        Ok(self
            .conn
            .prepare_cached(&format!("{OBSERVED_FILE} WHERE r.name = ?1 AND t.path = ?2"))?
            .query_row(rusqlite::params![root_name, path], observed_file_row)
            .optional()?)
    }

    /// Symlinked file rows whose canonical target starts with the bytes of
    /// `prefix`, in target order.
    pub fn symlinks_targeting(&self, prefix: &[u8]) -> Result<Vec<ObservedFile>, StoreError> {
        let mut rows = self.query_rows(
            &format!(
                "{OBSERVED_FILE} WHERE t.symlink_target >= ?1 AND t.symlink_target IS NOT NULL
                 ORDER BY t.symlink_target"
            ),
            [prefix],
            observed_file_row,
        )?;
        let end = rows
            .iter()
            .position(|row| {
                !row.file
                    .symlink_target
                    .as_deref()
                    .is_some_and(|target| target.starts_with(prefix))
            })
            .unwrap_or(rows.len());
        rows.truncate(end);
        Ok(rows)
    }

    /// Every traversed directory, in (root name, path) order.
    pub fn observed_directories(&self) -> Result<Vec<ObservedDirectory>, StoreError> {
        self.query_rows(
            "SELECT r.name, t.path, t.canonical_path, t.physical_path
             FROM directories t JOIN roots r USING (root_id) ORDER BY r.name, t.path",
            [],
            observed_directory_row,
        )
    }

    /// The traversed directories at or below `prefix` in `root_name`.
    pub fn observed_directories_under(
        &self,
        root_name: &str,
        prefix: &str,
    ) -> Result<Vec<ObservedDirectory>, StoreError> {
        self.query_rows(
            &format!(
                "SELECT r.name, t.path, t.canonical_path, t.physical_path
                 FROM directories t JOIN roots r USING (root_id)
                 WHERE {UNDER} ORDER BY t.path"
            ),
            rusqlite::params![root_name, prefix],
            observed_directory_row,
        )
    }

    /// The first traversed directory (by root name, path) whose canonical
    /// path is `canonical`.
    pub fn directory_by_canonical(
        &self,
        canonical: &[u8],
    ) -> Result<Option<ObservedDirectory>, StoreError> {
        Ok(self
            .conn
            .prepare_cached(
                "SELECT r.name, t.path, t.canonical_path, t.physical_path
                 FROM directories t JOIN roots r USING (root_id)
                 WHERE t.canonical_path = ?1 ORDER BY r.name, t.path LIMIT 1",
            )?
            .query_row([canonical], observed_directory_row)
            .optional()?)
    }

    /// Every scan diagnostic, in (root name, path) order.
    pub fn scan_diagnostics(&self) -> Result<Vec<ObservedDiagnostic>, StoreError> {
        self.query_rows(
            "SELECT r.name, t.path, t.detail
             FROM scan_diagnostics t JOIN roots r USING (root_id) ORDER BY r.name, t.path",
            [],
            |row| {
                Ok(ObservedDiagnostic {
                    root_name: row.get(0)?,
                    path: row.get(1)?,
                    detail: row.get(2)?,
                })
            },
        )
    }

    /// The scan diagnostics at or below `prefix` in `root_name`.
    pub fn scan_diagnostics_under(
        &self,
        root_name: &str,
        prefix: &str,
    ) -> Result<Vec<ObservedDiagnostic>, StoreError> {
        self.query_rows(
            &format!(
                "SELECT r.name, t.path, t.detail
                 FROM scan_diagnostics t JOIN roots r USING (root_id)
                 WHERE {UNDER} ORDER BY t.path"
            ),
            rusqlite::params![root_name, prefix],
            |row| {
                Ok(ObservedDiagnostic {
                    root_name: row.get(0)?,
                    path: row.get(1)?,
                    detail: row.get(2)?,
                })
            },
        )
    }

    /// Every observed `.bundle` file's bytes, in (root name, path) order.
    pub fn bundle_files(&self) -> Result<Vec<ObservedBundleFile>, StoreError> {
        self.query_rows(
            "SELECT r.name, t.path, t.bytes
             FROM bundle_files t JOIN roots r USING (root_id) ORDER BY r.name, t.path",
            [],
            |row| {
                Ok(ObservedBundleFile {
                    root_name: row.get(0)?,
                    path: row.get(1)?,
                    bytes: row.get(2)?,
                })
            },
        )
    }

    /// The observed `.bundle` files at or below `prefix` in `root_name`.
    pub fn bundle_files_under(
        &self,
        root_name: &str,
        prefix: &str,
    ) -> Result<Vec<ObservedBundleFile>, StoreError> {
        self.query_rows(
            &format!(
                "SELECT r.name, t.path, t.bytes
                 FROM bundle_files t JOIN roots r USING (root_id)
                 WHERE {UNDER} ORDER BY t.path"
            ),
            rusqlite::params![root_name, prefix],
            |row| {
                Ok(ObservedBundleFile {
                    root_name: row.get(0)?,
                    path: row.get(1)?,
                    bytes: row.get(2)?,
                })
            },
        )
    }

    /// One observed `.bundle` file's bytes.
    pub fn bundle_file(&self, root_name: &str, path: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .conn
            .prepare_cached(
                "SELECT t.bytes FROM bundle_files t JOIN roots r USING (root_id)
                 WHERE r.name = ?1 AND t.path = ?2",
            )?
            .query_row(rusqlite::params![root_name, path], |row| row.get(0))
            .optional()?)
    }
}

/// Which logical paths a cross-root `files` query selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathSelection<'a> {
    /// Every row.
    All,
    /// The path itself and every path below it (`path/…`).
    Subtree(&'a str),
    /// Every path that starts with the string.
    Prefix(&'a str),
}

impl PathSelection<'_> {
    /// Whether `path` is selected.
    pub fn contains(&self, path: &str) -> bool {
        match self {
            Self::All => true,
            Self::Subtree(prefix) => {
                path == *prefix
                    || path
                        .strip_prefix(prefix)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            }
            Self::Prefix(prefix) => path.starts_with(prefix),
        }
    }
}

/// The SQL range over `column` that selects exactly the paths starting with
/// `?{param}`: valid UTF-8 never holds the bytes `F4 90`, so every string
/// with the prefix sorts below the prefix followed by them.
pub(crate) fn starts_with_sql(column: &str, param: usize) -> String {
    format!("({column} >= ?{param} AND {column} < ?{param} || CAST(x'F490' AS TEXT))")
}

/// The SQL that selects `?{param}` itself and every path below it.
pub(crate) fn subtree_sql(column: &str, param: usize) -> String {
    format!(
        "({column} = ?{param} OR ({column} >= ?{param} || '/' AND {column} < ?{param} || '0'))"
    )
}

impl StoreReader {
    /// The scanned rows of every root that `selection` selects, in (root
    /// name, path) order. Answered from the `files_by_path` index.
    pub fn observed_files_in(
        &self,
        selection: PathSelection<'_>,
    ) -> Result<Vec<ObservedFile>, StoreError> {
        match selection {
            PathSelection::All => self.observed_files(),
            PathSelection::Subtree(path) => self.query_rows(
                &format!(
                    "{OBSERVED_FILE} WHERE {} ORDER BY r.name, t.path",
                    subtree_sql("t.path", 1)
                ),
                [path],
                observed_file_row,
            ),
            PathSelection::Prefix(prefix) => self.query_rows(
                &format!(
                    "{OBSERVED_FILE} WHERE {} ORDER BY r.name, t.path",
                    starts_with_sql("t.path", 1)
                ),
                [prefix],
                observed_file_row,
            ),
        }
    }

    /// Visit every scanned file row in (root name, path) order without
    /// collecting them.
    pub fn for_each_observed_file(
        &self,
        mut visit: impl FnMut(ObservedFile) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut statement = self
            .conn
            .prepare_cached(&format!("{OBSERVED_FILE} ORDER BY r.name, t.path"))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            visit(observed_file_row(row)?)?;
        }
        Ok(())
    }

    /// Visit every traversed directory in (root name, path) order.
    pub fn for_each_observed_directory(
        &self,
        mut visit: impl FnMut(ObservedDirectory) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT r.name, t.path, t.canonical_path, t.physical_path
             FROM directories t JOIN roots r USING (root_id) ORDER BY r.name, t.path",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            visit(observed_directory_row(row)?)?;
        }
        Ok(())
    }

    /// Visit every observed `.bundle` file's (root name, path, blake3 hash)
    /// in (root name, path) order, without reading its bytes.
    pub fn for_each_bundle_file_hash(
        &self,
        mut visit: impl FnMut(String, String, [u8; 32]) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT r.name, t.path, t.hash
             FROM bundle_files t JOIN roots r USING (root_id) ORDER BY r.name, t.path",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            visit(
                row.get(0)?,
                row.get(1)?,
                crate::bundles::blob32(row.get(2)?),
            )?;
        }
        Ok(())
    }

    /// Whether two traversed directories share a canonical path: the scan
    /// tables then hold a directory alias.
    pub fn has_directory_alias(&self) -> Result<bool, StoreError> {
        Ok(self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM directories
                           GROUP BY canonical_path HAVING count(*) > 1)",
            [],
            |row| row.get(0),
        )?)
    }
}
