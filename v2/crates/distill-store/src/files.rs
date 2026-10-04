//! File-tracking metadata (§13): `files` (per-root physical rows), the
//! `file_work` queue, root interning, and the derived logical path index.
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

/// [`StoreReader::file_content_hash`]'s statement.
pub(crate) const FILE_CONTENT_HASH: &str = "SELECT t.content_hash FROM files t JOIN roots r USING (root_id)
     WHERE r.name = ?1 AND t.path = ?2";

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

/// One changed path of the watcher work (§13's `file_work`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyEntry {
    pub root: RootId,
    /// The name `root` was interned from.
    pub root_name: String,
    pub path: String,
    /// `true` = the path exists (create/update); `false` = deleted.
    pub exists: bool,
    /// Input version whose file observation this row represents.
    pub observation: InputVersion,
}

/// One rename of the watcher work, in order (§13's `file_work`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameEvent {
    pub root: RootId,
    /// The name `root` was interned from.
    pub root_name: String,
    pub from_path: String,
    pub to_path: String,
}

/// One stable snapshot of unacknowledged watcher work. A consumer may do
/// fallible work without holding the store mutex, then acknowledge exactly
/// this set; work queued meanwhile remains pending.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingFileWork {
    pub dirty: Vec<DirtyEntry>,
    pub renames: Vec<RenameEvent>,
    /// The `file_work` rows this snapshot holds: every row with `seq` at
    /// most this (0: none).
    through: i64,
    /// The work the open transaction queued that this snapshot holds:
    /// `(generation, count)` of [`QueuedWork`].
    queued: Option<(u64, usize)>,
}

impl PendingFileWork {
    /// Work no queue holds (what a pass derives itself): acknowledging it
    /// consumes nothing.
    pub fn unqueued(dirty: Vec<DirtyEntry>, renames: Vec<RenameEvent>) -> Self {
        Self {
            dirty,
            renames,
            ..Self::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.dirty.is_empty() && self.renames.is_empty()
    }
}

/// One entry of the work a transaction queues.
#[derive(Debug, Clone, PartialEq, Eq)]
enum QueuedEntry {
    Dirty {
        root: RootId,
        path: String,
        exists: bool,
        observation: InputVersion,
    },
    Rename {
        root: RootId,
        from: String,
        to: String,
    },
}

/// The watcher work the open write transaction queued, not yet written: a
/// scratch list of that transaction only. Its outermost commit writes the
/// entries no pass consumed to `file_work`, so work queued and
/// acknowledged in one transaction never becomes a row; a rollback, or a
/// savepoint's, drops what it queued and restores what it consumed.
#[derive(Debug, Default)]
pub(crate) struct QueuedWork {
    /// Advances at each outermost commit or rollback: a snapshot taken in
    /// an earlier transaction consumes nothing here.
    generation: u64,
    entries: Vec<QueuedEntry>,
    /// The leading entries a pass consumed.
    consumed: usize,
}

/// A [`QueuedWork`] state a savepoint returns to on rollback.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueuedMark {
    len: usize,
    consumed: usize,
}

impl QueuedWork {
    pub(crate) fn mark(&self) -> QueuedMark {
        QueuedMark {
            len: self.entries.len(),
            consumed: self.consumed,
        }
    }

    pub(crate) fn restore(&mut self, mark: QueuedMark) {
        self.entries.truncate(mark.len);
        self.consumed = mark.consumed;
    }

    /// Forget everything: the outermost transaction ended.
    pub(crate) fn reset(&mut self) {
        self.generation += 1;
        self.entries.clear();
        self.consumed = 0;
    }

    /// Write the entries no pass consumed, in order, inside the outermost
    /// transaction about to commit.
    pub(crate) fn flush(&self, conn: &rusqlite::Connection) -> Result<(), StoreError> {
        if self.consumed == self.entries.len() {
            return Ok(());
        }
        let mut insert = conn.prepare_cached(
            "INSERT INTO file_work(kind, root_id, path, to_path, observation)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for entry in &self.entries[self.consumed..] {
            match entry {
                QueuedEntry::Dirty {
                    root,
                    path,
                    exists,
                    observation,
                } => insert.execute(rusqlite::params![
                    *exists as i64,
                    root.0,
                    path,
                    None::<String>,
                    observation.0 as i64
                ])?,
                QueuedEntry::Rename { root, from, to } => insert.execute(rusqlite::params![
                    WORK_RENAME,
                    root.0,
                    from,
                    to,
                    None::<i64>
                ])?,
            };
        }
        Ok(())
    }
}

/// `file_work.kind` of a rename (0 and 1 are a dirty path's `exists`).
const WORK_RENAME: i64 = 2;

/// The scanner's complete record of one (root, path): its tree state plus
/// the on-disk spelling, for a symlink its canonical target, and for a
/// traversed directory its canonical path. The paths are in the daemon's
/// platform path encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileObservation {
    pub state: FileState,
    pub raw_path: Vec<u8>,
    pub symlink_target: Option<Vec<u8>>,
    pub canonical_path: Option<Vec<u8>>,
}

impl From<FileState> for FileObservation {
    /// A row with no recorded spelling or symlink target.
    fn from(state: FileState) -> Self {
        Self {
            state,
            raw_path: Vec::new(),
            symlink_target: None,
            canonical_path: None,
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

impl InputTxn<'_> {
    /// Intern a root name to its process-local id, creating it if new.
    pub fn intern_root(&mut self, name: &str) -> Result<RootId, StoreError> {
        interned_root(&self.txn, &mut self.roots, name)
    }

    /// Record the scanner's observation of one (root, path). A directory
    /// taking the canonical path another row holds fails (an alias).
    pub fn upsert_file(
        &mut self,
        root: RootId,
        path: &str,
        file: &FileObservation,
        observation: InputVersion,
    ) -> Result<(), StoreError> {
        let state = &file.state;
        self.txn
            .prepare_cached(
                "INSERT INTO files(root_id, path, mtime, size, kind, content_hash, observation,
                               raw_path, symlink_target, canonical_path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(root_id, path) DO UPDATE SET
               mtime = excluded.mtime, size = excluded.size,
               kind = excluded.kind, content_hash = excluded.content_hash,
               observation = excluded.observation, raw_path = excluded.raw_path,
               symlink_target = excluded.symlink_target,
               canonical_path = excluded.canonical_path",
            )?
            .execute(
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
                    file.canonical_path,
                ],
            )?;
        Ok(())
    }

    /// Remove a (root, path) row; `Ok(false)` when it was absent.
    pub fn remove_file(&mut self, root: RootId, path: &str) -> Result<bool, StoreError> {
        let n = self.txn
            .prepare_cached("DELETE FROM files WHERE root_id = ?1 AND path = ?2")?
            .execute(rusqlite::params![root.0, path])?;
        Ok(n > 0)
    }


    /// Queue pending work (§13's `file_work`): written when the
    /// outermost transaction commits, unless a pass consumes it first.
    pub fn push_dirty(
        &mut self,
        root: RootId,
        path: &str,
        exists: bool,
        observation: InputVersion,
    ) -> Result<(), StoreError> {
        self.queued_work.entries.push(QueuedEntry::Dirty {
            root,
            path: path.to_owned(),
            exists,
            observation,
        });
        Ok(())
    }

    /// Queue a rename, in order with the rest of the work.
    pub fn push_rename(&mut self, root: RootId, from: &str, to: &str) -> Result<(), StoreError> {
        self.queued_work.entries.push(QueuedEntry::Rename {
            root,
            from: from.to_owned(),
            to: to.to_owned(),
        });
        Ok(())
    }
}

pub(crate) fn intern_root(conn: &rusqlite::Connection, name: &str) -> Result<RootId, StoreError> {
    if let Some(id) = conn
        .prepare_cached("SELECT root_id FROM roots WHERE name = ?1")?
        .query_row([name], |r| r.get(0))
        .optional()?
    {
        return Ok(RootId(id));
    }
    conn
        .prepare_cached("INSERT INTO roots(name) VALUES (?1)")?
        .execute([name])?;
    Ok(RootId(conn.last_insert_rowid()))
}

/// [`intern_root`] through `memo`, the ids one transaction already holds.
pub(crate) fn interned_root(
    conn: &rusqlite::Connection,
    memo: &mut std::collections::BTreeMap<String, RootId>,
    name: &str,
) -> Result<RootId, StoreError> {
    if let Some(id) = memo.get(name) {
        return Ok(*id);
    }
    let id = intern_root(conn, name)?;
    memo.insert(name.to_owned(), id);
    Ok(id)
}

/// The predicate selecting the rows of a table (aliased `t`, joined to
/// `roots` as `r`) under one (root name, prefix) subtree bound as `?1`,
/// `?2`: the whole root when the prefix is empty, else the prefix itself and
/// every path below it. Each form is one search of the table's
/// `(root_id, path)` key. A subtree is the range `[prefix, prefix || '0')`
/// (`'0'` follows `'/'`) less the paths in it that only extend the prefix's
/// last segment (`prefix.txt`), which sort before `prefix || '/'`.
pub(crate) fn under_sql(prefix: &str) -> &'static str {
    if prefix.is_empty() {
        // `?2` is bound either way.
        "r.name = ?1 AND ?2 = ''"
    } else {
        "r.name = ?1 AND t.path >= ?2 AND t.path < ?2 || '0'
         AND (t.path = ?2 OR t.path >= ?2 || '/')"
    }
}

impl Store {
    /// Every unacknowledged piece of watcher work, in order: the queue's
    /// rows, then what the open transaction queued.
    pub fn pending_file_work(&self) -> Result<PendingFileWork, StoreError> {
        let mut work = self.read.committed_file_work()?;
        let queued = &self.queued_work;
        let mut names = std::collections::BTreeMap::new();
        let mut name = |root: RootId| -> Result<String, StoreError> {
            if let Some(name) = names.get(&root) {
                return Ok(String::clone(name));
            }
            let found = self.read.root_name(root)?.ok_or_else(|| StoreError::Rejected {
                detail: format!("queued work names unknown root {}", root.0),
            })?;
            names.insert(root, found.clone());
            Ok(found)
        };
        for entry in &queued.entries[queued.consumed..] {
            match entry {
                QueuedEntry::Dirty {
                    root,
                    path,
                    exists,
                    observation,
                } => work.dirty.push(DirtyEntry {
                    root: *root,
                    root_name: name(*root)?,
                    path: path.clone(),
                    exists: *exists,
                    observation: *observation,
                }),
                QueuedEntry::Rename { root, from, to } => work.renames.push(RenameEvent {
                    root: *root,
                    root_name: name(*root)?,
                    from_path: from.clone(),
                    to_path: to.clone(),
                }),
            }
        }
        work.queued = Some((queued.generation, queued.entries.len()));
        Ok(work)
    }

    /// Clear the watcher work a pass has completed, exactly the set `work`
    /// captured, without fabricating a new input snapshot. Returns whether
    /// none of its paths has newer work queued behind it (work queued
    /// meanwhile stays pending either way).
    pub fn acknowledge_file_work(&mut self, work: &PendingFileWork) -> Result<bool, StoreError> {
        self.write_txn(|store| {
            let mut newer = std::collections::BTreeSet::new();
            if work.through > 0 {
                store
                    .read
                    .conn
                    .prepare_cached("DELETE FROM file_work WHERE seq <= ?1")?
                    .execute([work.through])?;
            }
            {
                let mut later = store.read.conn.prepare_cached(
                    "SELECT root_id, path FROM file_work WHERE seq > ?1 AND kind != 2",
                )?;
                let rows = later.query_map([work.through], |row| {
                    Ok((RootId(row.get(0)?), row.get::<_, String>(1)?))
                })?;
                for row in rows {
                    newer.insert(row?);
                }
            }
            let queued = &mut store.queued_work;
            let mut from = queued.consumed;
            if let Some((generation, count)) = work.queued {
                if generation == queued.generation && count > queued.consumed {
                    queued.consumed = count;
                    from = count;
                }
            }
            for entry in &queued.entries[from..] {
                if let QueuedEntry::Dirty { root, path, .. } = entry {
                    newer.insert((*root, path.clone()));
                }
            }
            Ok(!work
                .dirty
                .iter()
                .any(|entry| newer.contains(&(entry.root, entry.path.clone()))))
        })
    }
}

impl StoreReader {
    /// The committed watcher work, in order, without consuming it.
    /// Downstream processing can fail or race a later publication without
    /// losing rows.
    pub fn committed_file_work(&self) -> Result<PendingFileWork, StoreError> {
        let mut work = PendingFileWork::default();
        let mut statement = self.conn.prepare_cached(
            "SELECT w.seq, w.kind, w.root_id, r.name, w.path, w.to_path, w.observation
             FROM file_work w JOIN roots r USING (root_id) ORDER BY w.seq",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            work.through = row.get(0)?;
            let kind: i64 = row.get(1)?;
            let root = RootId(row.get(2)?);
            let root_name: String = row.get(3)?;
            let path: String = row.get(4)?;
            if kind == WORK_RENAME {
                work.renames.push(RenameEvent {
                    root,
                    root_name,
                    from_path: path,
                    to_path: row.get(5)?,
                });
            } else {
                work.dirty.push(DirtyEntry {
                    root,
                    root_name,
                    path,
                    exists: kind != 0,
                    observation: InputVersion(row.get::<_, i64>(6)? as u64),
                });
            }
        }
        Ok(work)
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
            t.raw_path, t.symlink_target, t.canonical_path
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
            canonical_path: row.get(8)?,
        },
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
            &format!("{OBSERVED_FILE} WHERE {} ORDER BY t.path", under_sql(prefix)),
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
        // Targets are platform path bytes, compared bytewise: those starting
        // with `prefix` are the range up to its successor, the least byte
        // string above every extension of it (what `under_sql`'s
        // `prefix || '0'` is for `/`-separated text).
        match bytes_successor(prefix) {
            Some(end) => self.query_rows(
                &format!(
                    "{OBSERVED_FILE} WHERE t.symlink_target >= ?1 AND t.symlink_target < ?2
                     ORDER BY t.symlink_target"
                ),
                rusqlite::params![prefix, end],
                observed_file_row,
            ),
            None => self.query_rows(
                &format!("{OBSERVED_FILE} WHERE t.symlink_target >= ?1 ORDER BY t.symlink_target"),
                [prefix],
                observed_file_row,
            ),
        }
    }

    /// The traversed directory whose canonical path is `canonical`; the
    /// unique index admits at most one.
    pub fn directory_by_canonical(
        &self,
        canonical: &[u8],
    ) -> Result<Option<ObservedFile>, StoreError> {
        Ok(self
            .conn
            .prepare_cached(&format!("{OBSERVED_FILE} WHERE t.canonical_path = ?1"))?
            .query_row([canonical], observed_file_row)
            .optional()?)
    }

    /// The (path, blake3 hash) of each observed `.bundle` file at or below
    /// `prefix` in `root_name`, in path order: its `files` content hash.
    pub fn bundle_file_hashes_under(
        &self,
        root_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, [u8; 32])>, StoreError> {
        self.query_rows(
            &format!(
                "SELECT t.path, t.content_hash FROM files t JOIN roots r USING (root_id)
                 WHERE {} AND +t.ext = 'bundle' AND t.content_hash IS NOT NULL
                 ORDER BY t.path",
                under_sql(prefix)
            ),
            rusqlite::params![root_name, prefix],
            |row| Ok((row.get(0)?, crate::bundles::blob32(row.get(1)?))),
        )
    }

    /// Whether a file or traversed directory is observed at or below
    /// `prefix` in `root_name`.
    pub fn observes_under(&self, root_name: &str, prefix: &str) -> Result<bool, StoreError> {
        let under = under_sql(prefix);
        Ok(self.conn.prepare_cached(&format!(
            "SELECT EXISTS(SELECT 1 FROM files t JOIN roots r USING (root_id) WHERE {under})"
        ))?
        .query_row(rusqlite::params![root_name, prefix], |row| row.get(0))?)
    }

    /// The content hash of the file observed at (`root_name`, `path`):
    /// `None` for no row, or a row with no content (a directory). One
    /// search of each primary key.
    pub fn file_content_hash(
        &self,
        root_name: &str,
        path: &str,
    ) -> Result<Option<ContentHash>, StoreError> {
        let hash: Option<Option<Vec<u8>>> = self
            .conn
            .prepare_cached(FILE_CONTENT_HASH)?
            .query_row(rusqlite::params![root_name, path], |row| row.get(0))
            .optional()?;
        Ok(hash.flatten().map(|bytes| {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&bytes);
            ContentHash(hash)
        }))
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
    /// Every path whose final segment is the string (`files_by_name`).
    Name(&'a str),
    /// Every path whose final segment's text after its last `.` is the
    /// string (`files_by_ext`).
    Extension(&'a str),
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
            Self::Name(name) => path_name(path) == *name,
            Self::Extension(extension) => path_extension(path) == Some(*extension),
        }
    }
}

/// A logical path's final segment: the `name` column of `files` and
/// `bundles`.
pub fn path_name(path: &str) -> &str {
    path.rsplit_once('/').map_or(path, |(_, name)| name)
}

/// The text after the last `.` of a logical path's final segment, if it
/// has one: the `ext` column of `files`.
pub fn path_extension(path: &str) -> Option<&str> {
    path_name(path).rsplit_once('.').map(|(_, extension)| extension)
}

/// `globset`'s metacharacters, its `\` escape among them.
pub const GLOBSET_META: &[char] = &['*', '?', '[', ']', '{', '}', '\\'];

/// What a path glob's literal text says about every path it matches, in
/// a dialect whose metacharacters are `meta` and whose wildcards may match
/// `/` (as `globset`'s and the RPC's do): the keys an index can find its
/// candidates by. A bare `*` or `**` has none: it selects every path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GlobKeys<'a> {
    /// The text before the first metacharacter: every match starts with it.
    pub prefix: &'a str,
    /// The final segment, when the glob's is literal (`**/name.ext`):
    /// every match's final segment.
    pub name: Option<&'a str>,
    /// The extension of a literal tail holding a `.` (`*.ext`): every
    /// match's extension, since the tail ends every match's final segment.
    /// A wildcard may match `/`, so a literal stem after one (`**/name.*`)
    /// says nothing about the final segment.
    pub extension: Option<&'a str>,
}

impl<'a> GlobKeys<'a> {
    pub fn of(pattern: &'a str, meta: &[char]) -> Self {
        let first = pattern.find(meta).unwrap_or(pattern.len());
        // The literal text after the last metacharacter ends every match.
        let tail = match pattern.char_indices().rev().find(|(_, c)| meta.contains(c)) {
            Some((last, c)) => &pattern[last + c.len_utf8()..],
            None => pattern,
        };
        let literal_segment = tail.rsplit_once('/').map(|(_, name)| name);
        let name = if first == pattern.len() {
            Some(path_name(pattern))
        } else {
            literal_segment
        };
        let segment_tail = literal_segment.unwrap_or(tail);
        Self {
            prefix: &pattern[..first],
            name: name.filter(|name| !name.is_empty()),
            extension: segment_tail.rsplit_once('.').map(|(_, extension)| extension),
        }
    }
}

/// The least byte string greater than every string starting with `prefix`:
/// `prefix` without its trailing `FF` bytes, its last byte incremented;
/// `None` when it has no other byte, and nothing bounds its extensions.
fn bytes_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|&byte| byte != 0xFF)?;
    let mut end = prefix[..=last].to_vec();
    end[last] += 1;
    Some(end)
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
    /// name, path) order. Answered from the `files_by_path` index,
    /// or `files_by_name` or `files_by_ext` for a name or an extension.
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
            PathSelection::Name(name) => self.query_rows(
                &format!("{OBSERVED_FILE} WHERE t.name = ?1 ORDER BY r.name, t.path"),
                [name],
                observed_file_row,
            ),
            PathSelection::Extension(extension) => self.query_rows(
                &format!("{OBSERVED_FILE} WHERE t.ext = ?1 ORDER BY r.name, t.path"),
                [extension],
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


    /// Visit every observed `.bundle` file's (root name, path, blake3 hash)
    /// in (root name, path) order: its `files` content hash.
    pub fn for_each_bundle_file_hash(
        &self,
        mut visit: impl FnMut(String, String, [u8; 32]) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT r.name, t.path, t.content_hash
             FROM files t JOIN roots r USING (root_id)
             WHERE t.ext = 'bundle' AND t.content_hash IS NOT NULL
             ORDER BY r.name, t.path",
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

}
