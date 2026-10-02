//! The SQLite metadata layer (§13): schema creation, the two version
//! counters, and the transaction discipline of the consistency contract —
//! input transactions advance the input version, memo transactions
//! advance the memo sequence, readers only ever observe a complete
//! version.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};

use crate::config::StoreConfig;
use crate::error::StoreError;
use crate::state::{InputVersion, MemoSeq, SnapshotStamp, StoreInstanceId};

/// The metadata schema version this crate reads and writes, recorded as
/// SQLite's `user_version`. There is deliberately no in-place migration
/// story: daemon state is disposable (§2), so a mismatch is a typed error
/// and the remedy is [`Store::recreate`].
pub const SCHEMA_VERSION: u32 = 36;

/// §13's table inventory. Physical placement (`segment, offset, len`)
/// lives solely in `cas_extents` — every other row references artifacts
/// by hash only.
const DDL: &str = "
CREATE TABLE store_meta (
    key   TEXT NOT NULL PRIMARY KEY,
    value BLOB
);
CREATE TABLE roots (
    root_id INTEGER PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE
);
CREATE TABLE files (
    root_id      INTEGER NOT NULL,
    path         TEXT NOT NULL,
    mtime        INTEGER NOT NULL,
    size         INTEGER NOT NULL,
    kind         INTEGER NOT NULL,
    content_hash BLOB,
    observation  INTEGER NOT NULL,
    -- The scanner's on-disk spelling of `path`, and a symlink's canonical
    -- target, in the daemon's platform path encoding.
    raw_path       BLOB NOT NULL,
    symlink_target BLOB,
    PRIMARY KEY (root_id, path)
);
CREATE INDEX files_by_path ON files(path);
CREATE INDEX files_by_symlink_target ON files(symlink_target)
    WHERE symlink_target IS NOT NULL;
-- The bytes of every observed `.bundle` file, as read by the scan that
-- recorded its `files` row.
CREATE TABLE bundle_files (
    root_id INTEGER NOT NULL,
    path    TEXT NOT NULL,
    bytes   BLOB NOT NULL,
    PRIMARY KEY (root_id, path)
);
-- Every traversed directory (the root itself at path ''), for alias checks.
CREATE TABLE directories (
    root_id        INTEGER NOT NULL,
    path           TEXT NOT NULL,
    canonical_path BLOB NOT NULL,
    physical_path  BLOB NOT NULL,
    PRIMARY KEY (root_id, path)
);
CREATE INDEX directories_by_canonical ON directories(canonical_path);
-- Non-fatal scan exclusions, keyed by rooted path; `detail` is the
-- daemon's encoding.
CREATE TABLE scan_diagnostics (
    root_id INTEGER NOT NULL,
    path    TEXT NOT NULL,
    detail  BLOB NOT NULL,
    PRIMARY KEY (root_id, path)
);
-- What each scanned bundle claims (bundle and asset UUIDs, derived
-- outputs, primary paths, malformed skeletons), keyed
-- by the claiming source. See `claims`.
CREATE TABLE source_claims (
    root_id  INTEGER NOT NULL,
    path     TEXT NOT NULL,
    kind     INTEGER NOT NULL,
    subject  BLOB NOT NULL,
    claimant BLOB NOT NULL,
    detail   BLOB NOT NULL,
    PRIMARY KEY (root_id, path, kind, subject, claimant)
);
CREATE INDEX source_claims_by_subject ON source_claims(kind, subject);
-- Per-entity errors (see `errors`): one row per current defect. `family`
-- is the producer that owns the row (1 scan namespace, 2 the pending scan
-- rejection's namespace errors, 3 its configuration error, 4 the
-- configuration source's error); `scope_kind` 1 file, 2 bundle, 3 asset,
-- 4 target, 5 pipeline, 6 configuration, 7 daemon.
CREATE TABLE errors (
    family     INTEGER NOT NULL,
    scope_kind INTEGER NOT NULL CHECK (scope_kind BETWEEN 1 AND 7),
    scope_id   BLOB NOT NULL,
    identity   BLOB NOT NULL CHECK (length(identity) = 32),
    code       INTEGER NOT NULL,
    record     BLOB NOT NULL,
    message    TEXT NOT NULL,
    PRIMARY KEY (family, identity)
);
CREATE INDEX errors_by_scope ON errors(scope_kind, scope_id);
-- The physical subjects (platform path encoding) whose revalidation heals
-- the pending scan rejection.
CREATE TABLE scan_rejection_subjects (
    path BLOB NOT NULL PRIMARY KEY
);
-- Subjects with more than one distinct claimant (group 0 bundles, 1 assets).
CREATE TABLE claim_collisions (
    grp     INTEGER NOT NULL,
    subject BLOB NOT NULL,
    PRIMARY KEY (grp, subject)
);
-- Claim subjects changed since the last clean publication.
CREATE TABLE claim_pending (
    kind    INTEGER NOT NULL,
    subject BLOB NOT NULL,
    PRIMARY KEY (kind, subject)
);
-- The import index, derived from each bundle source's import record and
-- directory rules (see `imports`). `basis` is the daemon's read-set
-- encoding; `import_reads` names what it observed: kind 0 a path (key),
-- 1 a listing, 2 an importer capability.
CREATE TABLE import_records (
    bundle_uuid BLOB NOT NULL PRIMARY KEY,
    root_id     INTEGER NOT NULL,
    path        TEXT NOT NULL,
    basis       BLOB NOT NULL
);
CREATE INDEX import_records_by_source ON import_records(root_id, path);
CREATE TABLE import_reads (
    bundle_uuid BLOB NOT NULL,
    kind        INTEGER NOT NULL,
    key         TEXT NOT NULL,
    PRIMARY KEY (bundle_uuid, kind, key)
);
CREATE INDEX import_reads_by_key ON import_reads(kind, key);
CREATE TABLE directory_rule_sources (
    rules_bundle BLOB NOT NULL,
    rules_asset  BLOB NOT NULL,
    root_id      INTEGER NOT NULL,
    path         TEXT NOT NULL,
    PRIMARY KEY (rules_bundle, rules_asset)
);
CREATE INDEX directory_rule_sources_by_source ON directory_rule_sources(root_id, path);
CREATE TABLE dirty_files (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    root_id     INTEGER NOT NULL,
    path        TEXT NOT NULL,
    exists_flag INTEGER NOT NULL,
    observation INTEGER NOT NULL
);
CREATE TABLE rename_events (
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    root_id   INTEGER NOT NULL,
    from_path TEXT NOT NULL,
    to_path   TEXT NOT NULL
);
CREATE TABLE bundles (
    bundle_uuid    BLOB NOT NULL PRIMARY KEY,
    root_id        INTEGER NOT NULL,
    path           TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    content_hash   BLOB NOT NULL,
    poison         TEXT,
    -- Directory-import ownership (§8, §13): derived at scan from the
    -- generated bundle's DirectoryOrigin record, never precious. NULL =
    -- explicit import. The group root is the persistent root NAME (§8),
    -- never a process-local ordinal.
    origin_rules_bundle BLOB,
    origin_rule         BLOB,
    origin_group_root   TEXT,
    origin_group_path   TEXT
);
-- Build traces resolve bundle paths and path prefixes (§9).
CREATE INDEX bundles_by_path ON bundles(path);
CREATE INDEX bundles_by_origin ON bundles(origin_rules_bundle)
    WHERE origin_rules_bundle IS NOT NULL;
CREATE TABLE assets (
    asset_uuid   BLOB NOT NULL PRIMARY KEY,
    bundle_uuid  BLOB NOT NULL,
    local_id     TEXT NOT NULL,
    type_uuid    BLOB NOT NULL,
    authoring_only INTEGER NOT NULL CHECK (authoring_only IN (0, 1)),
    -- NULL only for a poisoned bundle's skeleton rows (§7, §13): the
    -- schema closure may be exactly what failed, and no read path
    -- serves a skeleton row's metadata while the poison stands.
    logical_hash BLOB,
    -- The RPC-served authored value (canonical JSON + blob table, see
    -- `served::encode_authored_value`) and terminal type. NULL for rows the
    -- RPC namespace does not serve (skeleton rows, daemon-private rows).
    authored_value BLOB,
    terminal_type  BLOB
);
-- An asset by bundle and local id is a reference (§9) a build traces.
CREATE INDEX assets_by_bundle ON assets(bundle_uuid, local_id);
-- Build traces query assets by authored and terminal type (§9).
CREATE INDEX assets_by_type ON assets(type_uuid);
CREATE TABLE asset_tags (
    asset_uuid BLOB NOT NULL,
    tag        TEXT NOT NULL,
    value      TEXT,
    PRIMARY KEY (asset_uuid, tag)
);
CREATE INDEX asset_tags_by_tag ON asset_tags(tag, value);
CREATE TABLE asset_tag_index (
    asset_uuid       BLOB NOT NULL PRIMARY KEY,
    tag_epoch        BLOB NOT NULL,
    planner_version  INTEGER,
    dylib_hash       BLOB,
    trace             BLOB NOT NULL,
    poison            TEXT
);
-- A tag query fails naming the least poisoned bundle among its
-- candidates; this lists exactly the poisoned rows.
CREATE INDEX asset_tag_index_poisoned ON asset_tag_index(asset_uuid)
    WHERE poison IS NOT NULL;
CREATE TABLE path_index (
    path       TEXT NOT NULL,
    root_id    INTEGER NOT NULL,
    asset_uuid BLOB NOT NULL,
    PRIMARY KEY (path, root_id)
);
CREATE TABLE deps (
    src_uuid BLOB NOT NULL,
    kind     INTEGER NOT NULL,
    target   TEXT NOT NULL,
    PRIMARY KEY (src_uuid, kind, target)
);
CREATE INDEX deps_by_target ON deps(kind, target);
CREATE TABLE schemas (
    logical_hash BLOB NOT NULL PRIMARY KEY,
    schema_json  TEXT NOT NULL
);
CREATE TABLE result_candidates (
    key_kind     INTEGER NOT NULL,
    static_key   BLOB NOT NULL,
    trace_digest BLOB NOT NULL,
    memo_seq     INTEGER NOT NULL,
    segment      INTEGER NOT NULL,
    offset       INTEGER NOT NULL,
    len          INTEGER NOT NULL,
    PRIMARY KEY (key_kind, static_key, trace_digest)
);
CREATE TABLE derived_outputs (
    child_uuid  BLOB NOT NULL PRIMARY KEY,
    parent_uuid BLOB NOT NULL,
    output_key  TEXT NOT NULL,
    terminal_type BLOB
);
CREATE TABLE derived_assertions (
    child_uuid  BLOB NOT NULL,
    parent_uuid BLOB NOT NULL,
    output_key  TEXT NOT NULL,
    memo_seq    INTEGER NOT NULL,
    PRIMARY KEY (child_uuid, memo_seq)
);
CREATE INDEX result_candidates_by_segment ON result_candidates(segment);
CREATE TABLE cas_extents (
    content_hash BLOB NOT NULL PRIMARY KEY,
    segment      INTEGER NOT NULL,
    offset       INTEGER NOT NULL,
    len          INTEGER NOT NULL
);
CREATE INDEX cas_extents_by_segment ON cas_extents(segment);
-- What keeps an extent indexed. holder_kind 0: a result (holder = key_kind
-- byte, static key, trace digest) names its outputs, aux payloads and
-- output wire trees. holder_kind 1: an installed artifact or wire tree
-- (holder = its own hash). An extent no row names is pruned.
CREATE TABLE cas_refs (
    holder_kind  INTEGER NOT NULL CHECK (holder_kind IN (0, 1)),
    holder       BLOB NOT NULL,
    content_hash BLOB NOT NULL REFERENCES cas_extents(content_hash),
    PRIMARY KEY (holder_kind, holder, content_hash)
) WITHOUT ROWID;
CREATE INDEX cas_refs_by_hash ON cas_refs(content_hash);
-- Every segment file. state 0: a writer may still append; 1: sealed;
-- 2: dead, its file deleted once no read can still reach it (see `cas`).
CREATE TABLE cas_segments (
    segment_id  INTEGER PRIMARY KEY,
    file_name   TEXT NOT NULL,
    segment_kind INTEGER NOT NULL,
    indexed_len INTEGER NOT NULL,
    state       INTEGER NOT NULL CHECK (state IN (0, 1, 2))
);
CREATE TABLE pipeline_state (
    id                 INTEGER PRIMARY KEY CHECK (id = 0),
    dylib_hash         BLOB,
    input_version      INTEGER NOT NULL,
    poison_code        INTEGER CHECK (poison_code BETWEEN 1 AND 8),
    poison_origin      INTEGER CHECK (poison_origin BETWEEN 1 AND 2),
    poison_cleanup     INTEGER CHECK (poison_cleanup BETWEEN 0 AND 7),
    poison_identity    BLOB,
    poison_message     TEXT,
    CHECK ((poison_code IS NULL AND poison_origin IS NULL AND poison_cleanup IS NULL
            AND poison_identity IS NULL AND poison_message IS NULL)
        OR (poison_code IS NOT NULL AND poison_origin IS NOT NULL AND poison_cleanup IS NOT NULL
            AND poison_identity IS NOT NULL AND poison_message IS NOT NULL))
);
CREATE TABLE registrations (
    kind    INTEGER NOT NULL,
    reg_id  TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (kind, reg_id)
);
CREATE TABLE pipeline_schema_registry (
    type_uuid   BLOB NOT NULL PRIMARY KEY,
    logical_hash BLOB NOT NULL
);
CREATE TABLE pipeline_target_set (
    name                   TEXT NOT NULL PRIMARY KEY,
    target_definition_hash BLOB NOT NULL
);
CREATE TABLE configuration_state (
    id                 INTEGER PRIMARY KEY CHECK (id = 0),
    active_generation  INTEGER NOT NULL,
    input_version      INTEGER NOT NULL,
    poison_code        INTEGER CHECK (poison_code IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14)),
    poison_detail_version INTEGER CHECK (poison_detail_version IS NULL OR poison_detail_version = 1),
    poison_detail      BLOB,
    poison_reason_hash BLOB CHECK (poison_reason_hash IS NULL OR length(poison_reason_hash) = 32),
    poison_message     TEXT,
    CHECK ((poison_code IS NULL AND poison_detail_version IS NULL AND poison_detail IS NULL
            AND poison_reason_hash IS NULL AND poison_message IS NULL)
        OR (poison_code IS NOT NULL AND poison_detail_version IS NOT NULL AND poison_detail IS NOT NULL
            AND poison_reason_hash IS NOT NULL AND poison_message IS NOT NULL))
);
CREATE TABLE pending_restart (
    generation   INTEGER NOT NULL,
    config_key   TEXT NOT NULL,
    config_value TEXT NOT NULL,
    PRIMARY KEY (generation, config_key)
);
CREATE TABLE tools (
    tool_key       TEXT NOT NULL,
    present        INTEGER NOT NULL CHECK (present IN (0, 1)),
    identity_object BLOB NOT NULL,
    tool_hash      BLOB NOT NULL CHECK (length(tool_hash) = 32),
    input_version  INTEGER NOT NULL,
    PRIMARY KEY (tool_key, input_version)
);
CREATE TABLE codegen_outputs (
    relative_path TEXT NOT NULL PRIMARY KEY,
    content_hash  BLOB NOT NULL CHECK (length(content_hash) = 32)
);
CREATE TABLE watched_import_failures (
    bundle_uuid             BLOB NOT NULL PRIMARY KEY,
    attempted_input_version INTEGER NOT NULL,
    basis                   BLOB NOT NULL,
    terminal_kind           INTEGER NOT NULL CHECK (terminal_kind IN (1, 2, 3)),
    terminal_code           INTEGER,
    message                 TEXT NOT NULL,
    memo_seq                INTEGER NOT NULL,
    CHECK ((terminal_kind = 1 AND terminal_code IS NULL)
        OR (terminal_kind = 2 AND terminal_code IS NOT NULL
            AND terminal_code BETWEEN 1 AND 4294967295)
        OR (terminal_kind = 3 AND terminal_code IS NULL))
);
-- Served RPC state (LOCKLESS.md §2.2). Current-state only: a snapshot
-- lease reads these inside its own read transaction.
--
-- Explicit resolutions. kind: 0 missing, 1 built, 2 drifted, 3 failed,
-- 4 deleted. An asset with no row resolves Missing.
CREATE TABLE asset_resolutions (
    asset_uuid      BLOB NOT NULL PRIMARY KEY,
    kind            INTEGER NOT NULL CHECK (kind BETWEEN 0 AND 4),
    content_hash    BLOB,
    detail          BLOB,
    deleted_version INTEGER
);
-- Publication deltas and fence events, read by every RPC front end after
-- its cursor. Rows are trimmed by version; `change_log_oldest` in
-- store_meta is the oldest cursor a subscriber may resume from.
CREATE TABLE change_log (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    version    INTEGER NOT NULL,
    kind       INTEGER NOT NULL,
    asset_uuid BLOB,
    state      INTEGER,
    subject    TEXT,
    detail     BLOB
);
CREATE INDEX change_log_by_version ON change_log(version);
-- The RPC target set and each target's reconnect generation.
CREATE TABLE rpc_targets (
    name            TEXT NOT NULL PRIMARY KEY,
    definition_hash BLOB NOT NULL,
    generation      INTEGER NOT NULL
);
-- The one piece of artifact metadata the DSTL bytes do not carry: each
-- direct load edge's expected terminal type. Written with the CAS index.
CREATE TABLE artifact_load_edges (
    content_hash      BLOB NOT NULL,
    asset_uuid        BLOB NOT NULL,
    expected_terminal BLOB NOT NULL,
    PRIMARY KEY (content_hash, asset_uuid)
);
";

/// A read-only view of the store over one SQLite connection. Every thread
/// that reads opens its own ([`StoreReader::open`]); the writer's [`Store`]
/// derefs to the reader over its write connection. Nothing here caches
/// database content: every accessor reads the committed state visible to
/// this connection.
pub struct StoreReader {
    pub(crate) conn: ReaderConn,
    pub(crate) config: Arc<StoreConfig>,
    instance_id: StoreInstanceId,
}

/// The connection a [`StoreReader`] reads through: its own, or the open
/// transaction a [`ReadView`] borrows from the writer.
pub(crate) enum ReaderConn {
    Owned(Connection),
    /// Valid while the [`ReadView`] holding this reader lives.
    Lent(NonNull<Connection>),
}

// SAFETY: a lent connection exists only inside a `ReadView`, which is not
// `Send` (it holds a borrow of the `!Sync` connection), so only the owned
// case ever crosses threads.
unsafe impl Send for ReaderConn {}

impl std::ops::Deref for ReaderConn {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        match self {
            Self::Owned(conn) => conn,
            // SAFETY: see `Lent`.
            Self::Lent(conn) => unsafe { conn.as_ref() },
        }
    }
}

impl std::ops::DerefMut for ReaderConn {
    fn deref_mut(&mut self) -> &mut Connection {
        match self {
            Self::Owned(conn) => conn,
            Self::Lent(_) => unreachable!("a writer always owns its connection"),
        }
    }
}

/// Every [`StoreReader`] query, run inside an open [`InputTxn`]: reads see
/// the transaction's own uncommitted writes.
pub struct ReadView<'a> {
    reader: StoreReader,
    _txn: PhantomData<&'a Connection>,
}

impl std::ops::Deref for ReadView<'_> {
    type Target = StoreReader;

    fn deref(&self) -> &StoreReader {
        &self.reader
    }
}
impl std::fmt::Debug for StoreReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreReader")
            .field("state_path", &self.config.state_path)
            .field("instance_id", &self.instance_id)
            .finish()
    }
}

/// The store writer: the SQLite metadata layer plus the log-structured CAS,
/// opened from one `state_path`. Exactly one writer exists per state
/// directory; it owns the CAS append state. Reads go through the
/// [`StoreReader`] it derefs to.
pub struct Store {
    pub(crate) read: StoreReader,
    pub(crate) cas: crate::cas::store::CasInner,
    input: InputState,
    /// The operational configuration this writer follows
    /// ([`crate::StoreOpener::apply_operational_config`]), reloaded as each
    /// transaction begins.
    pub(crate) config_source: Option<Arc<crate::Current<StoreConfig>>>,
    /// Held for as long as any writer of this process is open: one process
    /// per state directory.
    pub(crate) _state_lock: Arc<std::fs::File>,
    /// Runs in `commit_build` between the append and the index transaction.
    #[cfg(test)]
    pub(crate) before_commit: Option<Box<dyn FnMut() + Send>>,
}

/// Whether writes join one input ([`Store::arm_input`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputState {
    Closed,
    /// The next input transaction begins the input.
    Armed,
    /// Writes join the input begun at `base`.
    Begun { base: InputVersion },
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").field("read", &self.read).finish()
    }
}

impl std::ops::Deref for Store {
    type Target = StoreReader;

    fn deref(&self) -> &StoreReader {
        &self.read
    }
}

impl Store {
    /// Open (creating if absent) the daemon state under
    /// `config.state_path`, run CAS recovery, and return the writer
    /// together with what recovery found and did (§13's classification).
    pub fn open(config: StoreConfig) -> Result<Store, StoreError> {
        Self::open_with_recovery(config).map(|(store, _)| store)
    }

    /// [`Store::open`], also returning the recovery report.
    pub fn open_with_recovery(
        config: StoreConfig,
    ) -> Result<(Store, crate::cas::RecoveryReport), StoreError> {
        let state_path = &config.state_path;
        let cas_dir = state_path.join("cas");
        create_dir(state_path)?;
        create_dir(&state_path.join("cas"))?;
        create_dir(&state_path.join("tools"))?;
        crate::pipeline::cleanup_staged_tool_temps(state_path)?;

        let state_lock = lock_state_dir(state_path)?;
        let db_path = state_path.join("meta.sqlite");
        let conn = open_writer_connection(&db_path)?;

        let found: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if found == 0 {
            conn.execute_batch(DDL)?;
            conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        } else if found != SCHEMA_VERSION {
            return Err(StoreError::SchemaVersionMismatch {
                found,
                supported: SCHEMA_VERSION,
            });
        }

        // Bootstrap identity + counters.
        let instance_id = match meta_get_blob(&conn, "instance_id")? {
            Some(bytes) if bytes.len() == 16 => {
                let mut id = [0u8; 16];
                id.copy_from_slice(&bytes);
                StoreInstanceId(id)
            }
            _ => {
                let id = StoreInstanceId::mint();
                meta_set_blob(&conn, "instance_id", &id.0)?;
                id
            }
        };
        meta_get_u64_or_init(&conn, "input_version")?;
        meta_get_u64_or_init(&conn, "memo_seq")?;

        let mut store = Store {
            read: StoreReader {
                conn: ReaderConn::Owned(conn),
                config: Arc::new(config),
                instance_id,
            },
            cas: crate::cas::store::CasInner::new(cas_dir),
            input: InputState::Closed,
            config_source: None,
            _state_lock: state_lock,
            #[cfg(test)]
            before_commit: None,
        };
        let recovery = store.recover_cas()?;
        tracing::info!(
            path = %store.config.state_path.display(),
            input_version = store.input_version().0,
            memo_seq = store.memo_seq().0,
            recovery = ?recovery,
            "store opened"
        );
        Ok((store, recovery))
    }

    /// Open another writer on this store's state directory, for another
    /// thread: its own connection and its own CAS segment. Recovery ran
    /// when this store opened; nothing runs here.
    pub fn open_writer(&self) -> Result<Store, StoreError> {
        let mut writer = Self::open_sibling(
            Arc::new((*self.config).clone()),
            self.instance_id(),
            self.cas.dir.clone(),
            Arc::clone(&self._state_lock),
        )?;
        writer.config_source = self.config_source.clone();
        Ok(writer)
    }

    pub(crate) fn open_sibling(
        config: Arc<StoreConfig>,
        instance_id: StoreInstanceId,
        cas_dir: PathBuf,
        state_lock: Arc<std::fs::File>,
    ) -> Result<Store, StoreError> {
        let conn = open_writer_connection(&config.state_path.join("meta.sqlite"))?;
        Ok(Store {
            read: StoreReader {
                conn: ReaderConn::Owned(conn),
                config,
                instance_id,
            },
            cas: crate::cas::store::CasInner::new(cas_dir),
            input: InputState::Closed,
            config_source: None,
            _state_lock: state_lock,
            #[cfg(test)]
            before_commit: None,
        })
    }

    /// Open a reader on the same state directory as this writer.
    pub fn reader(&self) -> Result<StoreReader, StoreError> {
        StoreReader::open((*self.config).clone())
    }

    /// Give a fresh, never-published store a caller-chosen instance id and
    /// starting input version. Embedded RPC stores use this so their stamps
    /// match the identity their callers were built against; readers opened
    /// afterwards see it.
    pub fn adopt_embedded_identity(
        &mut self,
        instance: StoreInstanceId,
        version: InputVersion,
    ) -> Result<(), StoreError> {
        self.write_txn(|store| {
            let txn = &*store.read.conn;
            if meta_get_u64(txn, "input_version")?.unwrap_or(0) != 0 {
                return Err(StoreError::InvalidConfiguration {
                    error: "only a never-published store can adopt an embedded identity"
                        .to_owned(),
                });
            }
            meta_set_blob(txn, "instance_id", &instance.0)?;
            meta_set_u64(txn, "input_version", version.0)?;
            meta_set_u64(txn, "change_log_oldest", version.0)
        })?;
        self.read.instance_id = instance;
        Ok(())
    }

    /// Wipe the daemon state and start over: state is disposable (§2).
    /// Re-mints the instance id, so stale version comparisons can never
    /// alias (§13).
    pub fn recreate(config: StoreConfig) -> Result<Store, StoreError> {
        if config.state_path.exists() {
            std::fs::remove_dir_all(&config.state_path).map_err(|source| StoreError::Io {
                path: config.state_path.clone(),
                source,
            })?;
        }
        Store::open(config)
    }

    /// Apply one watcher batch or authoring operation as an atomic
    /// transaction advancing the input version (§13). On any error the
    /// whole transaction rolls back and the version does not advance:
    /// readers only ever observe a complete input version.
    ///
    /// Inside an armed input ([`Store::arm_input`]) this begins or joins it: its
    /// writes, and the version it names, commit with that input.
    pub fn input_transaction<T, F>(&mut self, f: F) -> Result<(T, InputVersion), StoreError>
    where
        F: FnOnce(&mut InputTxn<'_>) -> Result<T, StoreError>,
    {
        match self.input {
            InputState::Begun { .. } => return self.joined_input_transaction(f, true),
            InputState::Armed => {
                self.begin_input()?;
                return self.joined_input_transaction(f, true);
            }
            InputState::Closed => {}
        }
        self.arm_input();
        self.begin_input()?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.joined_input_transaction(f, true)
        }));
        match result {
            Ok(Ok(out)) => {
                self.finish_input(true)?;
                Ok(out)
            }
            Ok(Err(error)) => {
                self.finish_input(false)?;
                Err(error)
            }
            Err(panic) => {
                let _ = self.finish_input(false);
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Run the same exact-basis validation surface as an input transaction,
    /// then roll every database mutation back. Coordinators use this before a
    /// journaled filesystem swap when the authoritative store transition is
    /// intentionally checked a second time in the publishing transaction.
    pub fn preview_input_transaction<T, F>(&mut self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&mut InputTxn<'_>) -> Result<T, StoreError>,
    {
        if !self.read.conn.is_autocommit() {
            return self.joined_input_transaction(f, false).map(|(out, _)| out);
        }
        self.refresh_config();
        self.read.conn.execute_batch("BEGIN IMMEDIATE")?;
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.joined_input_transaction(f, false)
        }));
        let _ = self.read.conn.execute_batch("ROLLBACK");
        match out {
            Ok(out) => out.map(|(out, _)| out),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    /// `f` as a savepoint in the open input (or, with none open, in a
    /// transaction of its own), kept only when `keep`.
    fn joined_input_transaction<T, F>(
        &mut self,
        f: F,
        keep: bool,
    ) -> Result<(T, InputVersion), StoreError>
    where
        F: FnOnce(&mut InputTxn<'_>) -> Result<T, StoreError>,
    {
        let instance = self.instance_id();
        let config = Arc::clone(&self.config);
        let state_path = config.state_path.clone();
        let open = self.input;
        let txn = self.read.conn.savepoint()?;
        let base = match open {
            InputState::Begun { base } => base,
            _ => InputVersion(meta_get_u64(&txn, "input_version")?.unwrap_or(0)),
        };
        let version = InputVersion(base.0 + 1);
        let mut input_txn = InputTxn {
            txn,
            base_stamp: SnapshotStamp {
                instance,
                version: base,
            },
            version,
            state_path,
            config,
        };
        let out = f(&mut input_txn)?;
        if keep {
            meta_set_u64(&input_txn.txn, "input_version", version.0)?;
            input_txn.txn.commit()?;
        }
        Ok((out, version))
    }

    /// Arm one input: the next input transaction begins it, and the writes
    /// after that until [`Store::finish_input`] join it (input transactions,
    /// memo and served writes). Other connections see none of it until it
    /// commits, as one version. Writes before it begins stand alone.
    pub fn arm_input(&mut self) {
        assert_eq!(self.input, InputState::Closed, "an input is already armed");
        self.input = InputState::Armed;
    }

    /// Arm an input and begin it now, with `BEGIN IMMEDIATE`: every read
    /// its writes depend on happens inside it. Returns its base.
    pub fn open_input(&mut self) -> Result<InputVersion, StoreError> {
        self.arm_input();
        if let Err(error) = self.begin_input() {
            self.input = InputState::Closed;
            return Err(error);
        }
        match self.input {
            InputState::Begun { base } => Ok(base),
            _ => unreachable!("an input begun has a base"),
        }
    }

    fn begin_input(&mut self) -> Result<(), StoreError> {
        assert_eq!(self.input, InputState::Armed, "an input begins once armed");
        self.refresh_config();
        self.read.conn.execute_batch("BEGIN IMMEDIATE")?;
        let base = match meta_get_u64(&self.read.conn, "input_version") {
            Ok(version) => InputVersion(version.unwrap_or(0)),
            Err(error) => {
                let _ = self.read.conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        };
        self.input = InputState::Begun { base };
        Ok(())
    }

    /// Whether an input is armed ([`Store::arm_input`]).
    pub fn input_open(&self) -> bool {
        self.input != InputState::Closed
    }

    /// Commit (`keep`) or roll back the armed input. Returns the version
    /// the store is at afterwards.
    pub fn finish_input(&mut self, keep: bool) -> Result<InputVersion, StoreError> {
        let state = std::mem::replace(&mut self.input, InputState::Closed);
        let InputState::Begun { base } = state else {
            assert_eq!(state, InputState::Armed, "an input is armed");
            return Ok(self.input_version());
        };
        if keep {
            match self.read.conn.execute_batch("COMMIT") {
                Ok(()) => return Ok(self.input_version()),
                Err(error) => {
                    let _ = self.read.conn.execute_batch("ROLLBACK");
                    self.cas.forget_active();
                    return Err(error.into());
                }
            }
        }
        self.read.conn.execute_batch("ROLLBACK")?;
        self.cas.forget_active();
        Ok(base)
    }

    /// Run `f` as one write transaction: `BEGIN IMMEDIATE` when none is
    /// open, so the transaction holds SQLite's write lock from its first
    /// read and never upgrades a read snapshot; inside an open one `f` joins
    /// it. On failure everything `f` wrote rolls back with the enclosing
    /// transaction, and this writer forgets its active segment, whose row
    /// may have been part of it.
    pub(crate) fn write_txn<T>(
        &mut self,
        f: impl FnOnce(&mut Store) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        if !self.read.conn.is_autocommit() {
            let out = f(self);
            if out.is_err() {
                self.cas.forget_active();
            }
            return out;
        }
        self.refresh_config();
        self.read.conn.execute_batch("BEGIN IMMEDIATE")?;
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self)));
        let out = match out {
            Ok(Ok(value)) => match self.read.conn.execute_batch("COMMIT") {
                Ok(()) => return Ok(value),
                Err(error) => Err(error.into()),
            },
            Ok(Err(error)) => Err(error),
            Err(panic) => {
                let _ = self.read.conn.execute_batch("ROLLBACK");
                self.cas.forget_active();
                std::panic::resume_unwind(panic)
            }
        };
        let _ = self.read.conn.execute_batch("ROLLBACK");
        self.cas.forget_active();
        out
    }

    /// [`Store::write_txn`] for callers outside this crate: reads that decide
    /// a write belong inside it.
    pub fn write_transaction<T>(
        &mut self,
        f: impl FnOnce(&mut Store) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.write_txn(f)
    }

    /// [`Store::write_transaction`] for a caller with its own error type:
    /// `lift` converts the transaction's own failures.
    pub fn write_transaction_with<T, E>(
        &mut self,
        lift: impl FnOnce(StoreError) -> E,
        f: impl FnOnce(&mut Store) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut failed = None;
        let out = self.write_txn(|store| {
            f(store).map_err(|error| {
                failed = Some(error);
                StoreError::Rejected {
                    detail: "the transaction's step failed".to_owned(),
                }
            })
        });
        out.map_err(|error| failed.take().unwrap_or_else(|| lift(error)))
    }

    /// Follow the opener's current operational configuration. Called as a
    /// transaction begins, so one transaction sees one configuration.
    fn refresh_config(&mut self) {
        if let Some(source) = &self.config_source {
            let config = source.load();
            if !Arc::ptr_eq(&self.read.config, &config) {
                self.read.config = config;
            }
        }
    }

    /// Whether this writer has a transaction open or an input armed: its
    /// uncommitted state is visible only through it.
    pub fn in_transaction(&self) -> bool {
        self.input_open() || !self.read.conn.is_autocommit()
    }

    /// Attach memo state to an input basis without advancing any input
    /// version (§13): build results move only the memo sequence.
    pub(crate) fn memo_transaction<T, F>(&mut self, f: F) -> Result<(T, MemoSeq), StoreError>
    where
        F: FnOnce(&rusqlite::Savepoint<'_>, MemoSeq) -> Result<T, StoreError>,
    {
        self.write_txn(|store| {
            let txn = store.read.conn.savepoint()?;
            let seq = MemoSeq(meta_get_u64(&txn, "memo_seq")?.unwrap_or(0) + 1);
            let out = f(&txn, seq)?;
            meta_set_u64(&txn, "memo_seq", seq.0)?;
            txn.commit()?;
            Ok((out, seq))
        })
    }
}

impl StoreReader {
    /// Open a read-only connection on an existing store. The writer must
    /// have created the state directory ([`Store::open`]); readers never
    /// create, migrate, or recover anything.
    pub fn open(config: StoreConfig) -> Result<StoreReader, StoreError> {
        let db_path = config.state_path.join("meta.sqlite");
        let conn = Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let found: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if found != SCHEMA_VERSION {
            return Err(StoreError::SchemaVersionMismatch {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        let instance_id = match meta_get_blob(&conn, "instance_id")? {
            Some(bytes) if bytes.len() == 16 => {
                let mut id = [0u8; 16];
                id.copy_from_slice(&bytes);
                StoreInstanceId(id)
            }
            _ => {
                return Err(StoreError::Io {
                    path: db_path,
                    source: std::io::Error::other("store has no instance id"),
                })
            }
        };
        Ok(StoreReader {
            conn: ReaderConn::Owned(conn),
            config: Arc::new(config),
            instance_id,
        })
    }

    pub fn state_path(&self) -> &Path {
        &self.config.state_path
    }

    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    /// Rebuild every SQLite index from authoritative table rows. This is a
    /// maintenance action only; callers publish its input-version event in the
    /// same transaction boundary as their other doctor result state.
    pub fn rebuild_indexes(&self) -> Result<(), StoreError> {
        self.conn.execute_batch("REINDEX")?;
        Ok(())
    }

    /// Test hook: call `hook` with each SQL statement this reader's own
    /// connection runs, its parameters expanded; `None` stops.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn trace_statements(&mut self, hook: Option<fn(&str)>) {
        self.conn.trace(hook);
    }

    /// Test hook: the detail lines of SQLite's `EXPLAIN QUERY PLAN` for
    /// `sql`, in plan order.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn query_plan_details(&self, sql: &str) -> Result<Vec<String>, StoreError> {
        let mut statement = self.conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        // Unbound parameters plan as NULLs; the plan does not depend on them.
        let mut rows = statement.raw_query();
        let mut details = Vec::new();
        while let Some(row) = rows.next()? {
            details.push(row.get::<_, String>(3)?);
        }
        Ok(details)
    }

    pub fn instance_id(&self) -> StoreInstanceId {
        self.instance_id
    }

    /// The committed input version visible to this connection.
    ///
    /// Infallible for now: the phase-3+ rewrites of its callers make it
    /// return `Result`. A failing single-row read of `store_meta` on an
    /// open connection means the database is gone or corrupt.
    pub fn input_version(&self) -> InputVersion {
        InputVersion(
            meta_get_u64(&self.conn, "input_version")
                .expect("store_meta.input_version is readable")
                .unwrap_or(0),
        )
    }

    /// The committed memo sequence visible to this connection. See
    /// [`StoreReader::input_version`] on infallibility.
    pub fn memo_seq(&self) -> MemoSeq {
        MemoSeq(
            meta_get_u64(&self.conn, "memo_seq")
                .expect("store_meta.memo_seq is readable")
                .unwrap_or(0),
        )
    }

    /// The instance-qualified current version (§13): what crosses the
    /// RPC boundary.
    pub fn stamp(&self) -> SnapshotStamp {
        SnapshotStamp {
            instance: self.instance_id,
            version: self.input_version(),
        }
    }

    /// §14's clean watermark: the newest mtime observed under active
    /// watch, recorded durably per session; reconciliation content-hashes
    /// anything not strictly older.
    pub fn clean_watermark(&self) -> Result<Option<i64>, StoreError> {
        meta_get_i64(&self.conn, "clean_watermark")
    }

    /// The input version that last published the daemon's compiled
    /// configuration state (schema authority, targets, pipeline epoch,
    /// roots): what in-memory state derived from it is keyed by. `None`
    /// until a publication records one.
    pub fn compiled_version(&self) -> Result<Option<InputVersion>, StoreError> {
        Ok(meta_get_u64(&self.conn, "compiled_version")?.map(InputVersion))
    }

    /// Whether the complete import index has been built: written in the
    /// transaction that writes the index rows, so it rolls back with them.
    pub fn import_index_built(&self) -> Result<bool, StoreError> {
        Ok(meta_get_u64(&self.conn, "import_index_built")?.is_some_and(|built| built != 0))
    }
}


/// One atomic input-version transaction (§13). Every input-versioned
/// table writes through methods on this; dropping without commit rolls
/// everything back.
pub struct InputTxn<'a> {
    pub(crate) txn: rusqlite::Savepoint<'a>,
    base_stamp: SnapshotStamp,
    version: InputVersion,
    pub(crate) state_path: PathBuf,
    config: Arc<StoreConfig>,
}

impl InputTxn<'_> {
    /// The exact committed snapshot against which this transaction began.
    /// Coordinator control reads used by authority commands must name this
    /// stamp; a bare or stale version can never authorize publication.
    pub fn base_stamp(&self) -> SnapshotStamp {
        self.base_stamp
    }

    /// Read through this transaction, seeing its writes so far.
    pub fn reader(&self) -> ReadView<'_> {
        ReadView {
            reader: StoreReader {
                conn: ReaderConn::Lent(NonNull::from(&*self.txn)),
                config: Arc::clone(&self.config),
                instance_id: self.base_stamp.instance,
            },
            _txn: PhantomData,
        }
    }

    /// The version this transaction will publish on commit.
    pub fn version(&self) -> InputVersion {
        self.version
    }

    /// Record §14's clean watermark.
    pub fn set_clean_watermark(&mut self, mtime: i64) -> Result<(), StoreError> {
        meta_set_i64(&self.txn, "clean_watermark", mtime)
    }

    /// Record that this input publishes the daemon's compiled configuration
    /// state (see [`StoreReader::compiled_version`]).
    pub fn mark_compiled(&mut self) -> Result<(), StoreError> {
        meta_set_u64(&self.txn, "compiled_version", self.version.0)
    }
}

/// A write connection: WAL, a busy timeout (writers queue on SQLite's write
/// lock), foreign keys, and FULL sync.
fn open_writer_connection(db_path: &Path) -> Result<Connection, StoreError> {
    let conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "wal")?;
    conn.busy_timeout(WRITER_BUSY_TIMEOUT)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    // FULL: every committed transaction is durable; §13's index rows
    // must never lead the segment fsync they follow.
    conn.pragma_update(None, "synchronous", "FULL")?;
    Ok(conn)
}

/// How long a writer waits for SQLite's write lock before failing.
pub(crate) const WRITER_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Take the state directory's process lock: one process opens a state
/// directory at a time, so startup recovery may treat every segment as
/// its own.
fn lock_state_dir(state_path: &Path) -> Result<Arc<std::fs::File>, StoreError> {
    let path = state_path.join("lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|source| StoreError::Io {
            path: path.clone(),
            source,
        })?;
    match file.try_lock() {
        Ok(()) => Ok(Arc::new(file)),
        Err(std::fs::TryLockError::WouldBlock) => Err(StoreError::StateLocked { path }),
        Err(std::fs::TryLockError::Error(source)) => Err(StoreError::Io { path, source }),
    }
}

fn create_dir(path: &Path) -> Result<(), StoreError> {
    std::fs::create_dir_all(path).map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })
}

// ---- store_meta helpers ----

pub(crate) fn meta_get_blob(conn: &Connection, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
    Ok(conn
        .query_row("SELECT value FROM store_meta WHERE key = ?1", [key], |r| {
            r.get(0)
        })
        .optional()?)
}

pub(crate) fn meta_set_blob(conn: &Connection, key: &str, value: &[u8]) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO store_meta(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

pub(crate) fn meta_get_i64(conn: &Connection, key: &str) -> Result<Option<i64>, StoreError> {
    Ok(conn
        .query_row("SELECT value FROM store_meta WHERE key = ?1", [key], |r| {
            r.get(0)
        })
        .optional()?)
}

pub(crate) fn meta_set_i64(conn: &Connection, key: &str, value: i64) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO store_meta(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

pub(crate) fn meta_get_u64(conn: &Connection, key: &str) -> Result<Option<u64>, StoreError> {
    Ok(meta_get_i64(conn, key)?.map(|v| v as u64))
}

pub(crate) fn meta_set_u64(conn: &Connection, key: &str, value: u64) -> Result<(), StoreError> {
    meta_set_i64(conn, key, value as i64)
}

fn meta_get_u64_or_init(conn: &Connection, key: &str) -> Result<u64, StoreError> {
    match meta_get_u64(conn, key)? {
        Some(v) => Ok(v),
        None => {
            meta_set_u64(conn, key, 0)?;
            Ok(0)
        }
    }
}
