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
pub const SCHEMA_VERSION: u32 = 49;

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
    -- The final path segment, and the text after its last `.` (NULL
    -- without one): what a glob with no literal prefix (`**/name.ext`,
    -- `*.ext`) is looked up by. Derived from `path` on write.
    name TEXT GENERATED ALWAYS AS (substr(path, length(rtrim(path, replace(path, '/', ''))) + 1)) VIRTUAL,
    ext  TEXT GENERATED ALWAYS AS (CASE WHEN instr(name, '.') > 0
        THEN substr(name, length(rtrim(name, replace(name, '.', ''))) + 1) END) VIRTUAL,
    PRIMARY KEY (root_id, path)
);
CREATE INDEX files_by_path ON files(path);
CREATE INDEX files_by_name ON files(name);
CREATE INDEX files_by_ext ON files(ext) WHERE ext IS NOT NULL;
CREATE INDEX files_by_symlink_target ON files(symlink_target)
    WHERE symlink_target IS NOT NULL;
-- Every traversed directory (the root itself at path ''), for alias checks.
-- Two directories never share a canonical path: the scanner rejects an
-- alias before publishing, and the unique index enforces it at write.
CREATE TABLE directories (
    root_id        INTEGER NOT NULL,
    path           TEXT NOT NULL,
    canonical_path BLOB NOT NULL,
    physical_path  BLOB NOT NULL,
    PRIMARY KEY (root_id, path)
);
CREATE UNIQUE INDEX directories_by_canonical ON directories(canonical_path);
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
-- Schema 39: the claims an asset makes (a primary path's claimant), for
-- the sources an asset change makes pending. Led by the claimant: led by
-- `kind`, it would serve a `DISTINCT claimant` of one kind in order, and
-- the planner would walk the kind's every claim instead of searching
-- `source_claims_by_subject`.
CREATE INDEX source_claims_by_claimant ON source_claims(claimant, kind);
-- Per-entity errors (see `errors`): one row per current defect. `family`
-- is the producer that owns the row (1 scan namespace, 2 the pending scan
-- rejection's namespace errors, 3 its configuration error, 4 the
-- configuration source's error, 5 the pipeline candidate's failure);
-- `scope_kind` 1 file, 2 bundle, 3 asset, 4 target, 5 pipeline,
-- 6 configuration, 7 daemon.
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
    -- The directory, ending in `/` (`''` for the whole root), that every
    -- path the rules' listing matches is under.
    listing_dir  TEXT NOT NULL,
    PRIMARY KEY (rules_bundle, rules_asset)
);
CREATE INDEX directory_rule_sources_by_source ON directory_rule_sources(root_id, path);
CREATE INDEX directory_rule_sources_by_listing ON directory_rule_sources(listing_dir);
-- The watcher work no pass has consumed yet, in order. kind 0: `path` was
-- deleted, 1: `path` exists as observed at input version `observation`,
-- 2: `path` was renamed to `to_path`. A pass acknowledges the rows it
-- captured by `seq` range (see `files::QueuedWork`).
CREATE TABLE file_work (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        INTEGER NOT NULL CHECK (kind IN (0, 1, 2)),
    root_id     INTEGER NOT NULL,
    path        TEXT NOT NULL,
    to_path     TEXT,
    observation INTEGER,
    CHECK ((kind = 2) = (to_path IS NOT NULL)),
    CHECK ((kind = 2) = (observation IS NULL))
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
    origin_group_path   TEXT,
    -- Schema 38 (reads-query): whether the bundle's `$record` import
    -- record is watched (§8), derived at publication from the parsed
    -- record so readers need not parse the bundle. 0 for a poisoned
    -- bundle.
    import_watched      INTEGER NOT NULL DEFAULT 0,
    -- The runtime entry the bundle's path resolves to (§9), one of its
    -- own assets; NULL when it names none or the bundle is poisoned.
    -- Every write of the row clears it; the publication that writes the
    -- assets sets it.
    primary_asset       BLOB,
    -- The final segment of `path`: what a glob whose last segment is
    -- literal (`**/name.bundle`) is looked up by. Derived on write. (No
    -- extension column: every bundle path ends in `.bundle`.)
    name TEXT GENERATED ALWAYS AS (substr(path, length(rtrim(path, replace(path, '/', ''))) + 1)) VIRTUAL
);
CREATE INDEX bundles_by_origin ON bundles(origin_rules_bundle)
    WHERE origin_rules_bundle IS NOT NULL;
-- Bundles by logical path (exact, and string-prefix ranges), across roots:
-- what build traces resolve bundle paths and path prefixes by (§9).
CREATE INDEX bundles_by_path ON bundles(path, root_id);
CREATE INDEX bundles_by_name ON bundles(name);
CREATE INDEX bundles_poisoned ON bundles(bundle_uuid) WHERE poison IS NOT NULL;
-- Schema 38 (reads-query): the watched imports' bundles.
CREATE INDEX bundles_import_watched ON bundles(bundle_uuid) WHERE import_watched;
-- The logical path strings each bundle's AssetRef/WeakRef fields name
-- (a bare string or an object's `path` field): what a rename rewrites.
-- Derived at scan from the published bundle, with its `bundles` row.
CREATE TABLE bundle_path_refs (
    bundle_uuid BLOB NOT NULL,
    target      TEXT NOT NULL,
    PRIMARY KEY (bundle_uuid, target)
) WITHOUT ROWID;
CREATE INDEX bundle_path_refs_by_target ON bundle_path_refs(target);
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
    -- The RPC-served terminal type. NULL for rows the RPC namespace does
    -- not serve (skeleton rows, daemon-private rows). The authored value
    -- is read from the bundle file, verified against its published hash.
    terminal_type  BLOB,
    -- The asset's search tags (§10) as `asset_tags` holds them: NULL when
    -- they are its current-schema tags; otherwise why a tag query cannot
    -- answer from them ('tag indexing pending' until a refinement in the
    -- publishing input computes them, or the refinement's failure).
    tag_poison     TEXT,
    -- The pipeline module whose migration produced the tags (NULL: none
    -- ran): a refinement under another module redoes them.
    tag_module     BLOB
);
-- A bundle's assets, and an asset by bundle and local id (a reference a
-- build traces, §9). Covering for (bundle, asset): local ids are unique
-- within a bundle, so walks in (bundle, asset) order sort one bundle at a
-- time.
CREATE INDEX assets_by_bundle ON assets(bundle_uuid, local_id, asset_uuid);
-- Build traces query assets by authored and terminal type (§9).
CREATE INDEX assets_by_type ON assets(type_uuid);
CREATE INDEX assets_by_terminal_type ON assets(terminal_type) WHERE terminal_type IS NOT NULL;
-- Assets by local id alone: a reserved entry (`$record`, `$settings`)
-- across bundles, or a query naming only a local id.
CREATE INDEX assets_by_local_id ON assets(local_id);
-- The authoring-only rows (control entries and tooling-only values).
CREATE INDEX assets_authoring ON assets(asset_uuid) WHERE authoring_only = 1;
CREATE TABLE asset_tags (
    asset_uuid BLOB NOT NULL,
    tag        TEXT NOT NULL,
    value      TEXT,
    PRIMARY KEY (asset_uuid, tag)
);
CREATE INDEX asset_tags_by_tag ON asset_tags(tag, value);
-- A tag query fails naming the least poisoned bundle among its
-- candidates; this lists exactly the tag-poisoned rows.
CREATE INDEX assets_tag_poisoned ON assets(asset_uuid) WHERE tag_poison IS NOT NULL;
-- A typed query's poison check walks the poisoned rows of its types only.
CREATE INDEX assets_tag_poisoned_by_type ON assets(type_uuid) WHERE tag_poison IS NOT NULL;
-- A refinement redoes the migrated rows of another module.
CREATE INDEX assets_tag_migrated ON assets(tag_module) WHERE tag_module IS NOT NULL;
-- Per authored type, the tag epoch its tag rows were refined under: a
-- digest of what the schema authority says about the type (see the
-- daemon's `type_tag_epochs`). A type whose epoch changes has every row
-- of its assets marked pending in the input that changes it.
CREATE TABLE tag_epochs (
    type_uuid BLOB NOT NULL PRIMARY KEY,
    epoch     BLOB NOT NULL
) WITHOUT ROWID;
-- A committed build result (§13): one candidate of the bucket its
-- static-input key names, the memo of one dependency trace. `failure` is
-- the encoded `FailureCause` of a deterministic failure, NULL for a
-- success. Its outputs are its `result_outputs` rows.
CREATE TABLE results (
    key_kind     INTEGER NOT NULL,
    static_key   BLOB NOT NULL,
    trace_digest BLOB NOT NULL,
    memo_seq     INTEGER NOT NULL,
    asset_uuid   BLOB NOT NULL,
    trace        BLOB NOT NULL,
    failure      BLOB,
    PRIMARY KEY (key_kind, static_key, trace_digest)
);
-- What a result names in the CAS, and so holds there. role 0: an output
-- (name = its output key, types = its type uuids, 16 bytes each); 1: an
-- aux payload (name = its debug key); 2: the wire tree output `name`
-- names. The foreign keys go with the result and refuse to drop an
-- extent a result still names.
CREATE TABLE result_outputs (
    key_kind     INTEGER NOT NULL,
    static_key   BLOB NOT NULL,
    trace_digest BLOB NOT NULL,
    role         INTEGER NOT NULL CHECK (role IN (0, 1, 2)),
    name         TEXT NOT NULL,
    types        BLOB,
    content_hash BLOB NOT NULL REFERENCES cas_extents(content_hash),
    PRIMARY KEY (key_kind, static_key, trace_digest, role, name),
    FOREIGN KEY (key_kind, static_key, trace_digest)
        REFERENCES results(key_kind, static_key, trace_digest) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX result_outputs_by_hash ON result_outputs(content_hash);
CREATE TABLE derived_outputs (
    child_uuid  BLOB NOT NULL PRIMARY KEY,
    parent_uuid BLOB NOT NULL,
    output_key  TEXT NOT NULL,
    terminal_type BLOB
);
CREATE TABLE cas_extents (
    content_hash BLOB NOT NULL PRIMARY KEY,
    segment      INTEGER NOT NULL,
    offset       INTEGER NOT NULL,
    len          INTEGER NOT NULL
);
-- Schema 40: a segment's extents with their lengths: the bytes a segment
-- holds, and the CAS's live bytes, are covering-index sums.
CREATE INDEX cas_extents_by_segment ON cas_extents(segment, len);
-- What an install holds: an installed artifact or wire tree (holder = its
-- own hash) holds itself and the wire tree an artifact names. Releasing
-- a holder (or a result, see `result_outputs`) deletes, in the same
-- transaction, each extent it held that nothing else references; the
-- foreign key refuses to drop an extent something still references.
CREATE TABLE cas_refs (
    holder       BLOB NOT NULL,
    content_hash BLOB NOT NULL REFERENCES cas_extents(content_hash),
    PRIMARY KEY (holder, content_hash)
) WITHOUT ROWID;
CREATE INDEX cas_refs_by_hash ON cas_refs(content_hash);
-- Every segment file. state 0: a writer may still append; 1: sealed;
-- 2: dead, its file deleted once no read can still reach it (see `cas`).
CREATE TABLE cas_segments (
    segment_id  INTEGER PRIMARY KEY,
    file_name   TEXT NOT NULL,
    segment_kind INTEGER NOT NULL,
    indexed_len INTEGER NOT NULL,
    state       INTEGER NOT NULL CHECK (state IN (0, 1, 2)),
    -- Schema 39 (fix-cas): the writer that allocated the segment
    -- (`CasInner::owner`).
    owner       INTEGER
);
-- Schema 40: a writer's one open regular segment, the one it appends to.
-- Unique: rolling to a new segment seals the old one in the same
-- transaction, so a writer can never hold two.
CREATE UNIQUE INDEX cas_segments_open ON cas_segments(owner)
    WHERE state = 0 AND segment_kind = 0;
-- Segments by state: the dead ones the sweeper deletes, and the sealed ones
-- compaction considers.
CREATE INDEX cas_segments_by_state ON cas_segments(state);
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
-- Schema 39 (fix-cas): a subscriber's history, one subject at a time.
CREATE INDEX change_log_assets ON change_log(asset_uuid, version) WHERE kind = 1;
CREATE INDEX change_log_paths ON change_log(subject, version) WHERE kind = 2;
-- The RPC target set and each target's reconnect generation.
CREATE TABLE rpc_targets (
    name            TEXT NOT NULL PRIMARY KEY,
    definition_hash BLOB NOT NULL,
    generation      INTEGER NOT NULL
);
-- The one piece of artifact metadata the DSTL bytes do not carry: each
-- direct load edge's expected terminal type. Written with the CAS index,
-- by the artifact's latest install; deleted with its extent.
CREATE TABLE artifact_load_edges (
    content_hash      BLOB NOT NULL
                      REFERENCES cas_extents(content_hash) ON DELETE CASCADE,
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
    /// Tool package trees the open input staged, published right before it
    /// commits ([`crate::pipeline::StagedPackage`]); a rollback drops them.
    staged_packages: Vec<crate::pipeline::StagedPackage>,
    /// The watcher work the open transaction queued
    /// ([`crate::files::QueuedWork`]).
    pub(crate) queued_work: crate::files::QueuedWork,
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
    /// Test hook: [`StoreReader::trace_statements`] on the writer's
    /// connection.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn trace_statements(&mut self, hook: Option<fn(&str)>) {
        self.read.trace_statements(hook);
    }

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
        let state_lock = lock_state_dir(state_path)?;
        crate::pipeline::open_tool_staging(state_path)?;

        let db_path = state_path.join("meta.sqlite");
        let conn = open_writer_connection(&db_path)?;

        // The schema and the store's identity are created in one
        // transaction (SQLite DDL is transactional): a first open that
        // fails part-way leaves no partial schema behind.
        conn.execute_batch("BEGIN IMMEDIATE")?;
        let instance_id = match create_schema(&conn) {
            Ok(instance_id) => {
                conn.execute_batch("COMMIT")?;
                instance_id
            }
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        };

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
            staged_packages: Vec::new(),
            queued_work: Default::default(),
            #[cfg(test)]
            before_commit: None,
        };
        let recovery = store.recover_cas()?;
        tracing::info!(
            path = %store.config.state_path.display(),
            input_version = store.input_version()?.0,
            memo_seq = store.memo_seq()?.0,
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
            staged_packages: Vec::new(),
            queued_work: Default::default(),
            #[cfg(test)]
            before_commit: None,
        })
    }

    /// Open a reader on the same state directory as this writer.
    pub fn reader(&self) -> Result<StoreReader, StoreError> {
        StoreReader::open((*self.config).clone())
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
        let staged = self.staged_packages.len();
        let queued = self.queued_work.mark();
        let txn = self.read.conn.savepoint()?;
        let base = match open {
            InputState::Begun { base } => base,
            _ => InputVersion(meta_get_u64(&txn, "input_version")?.unwrap_or(0)),
        };
        let version = InputVersion(base.0 + 1);
        let out = {
            let mut input_txn = InputTxn {
            txn,
            base_stamp: SnapshotStamp {
                instance,
                version: base,
            },
            version,
            state_path,
            config,
            roots: std::collections::BTreeMap::new(),
                staged_packages: &mut self.staged_packages,
                queued_work: &mut self.queued_work,
            };
            f(&mut input_txn).and_then(|out| {
                if keep {
                    meta_set_u64(&input_txn.txn, "input_version", version.0)?;
                    input_txn.txn.commit()?;
                }
                Ok(out)
            })
        };
        if out.is_err() || !keep {
            // The savepoint rolled back: so do the packages it staged and
            // the work it queued or consumed.
            self.staged_packages.truncate(staged);
            self.queued_work.restore(queued);
        }
        Ok((out?, version))
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
            return Ok(self.input_version()?);
        };
        let staged = std::mem::take(&mut self.staged_packages);
        if keep {
            if let Err(error) = self.queued_work.flush(&self.read.conn) {
                self.queued_work.reset();
                let _ = self.read.conn.execute_batch("ROLLBACK");
                return Err(error);
            }
            self.queued_work.reset();
            // The input's tool packages are published right before its
            // commit, and removed again if the commit fails: a `tools` row
            // never names a missing package, and a rolled-back input leaves
            // none behind (a crash between the two leaves one, under its
            // content address, for the next registration of it to reuse).
            let mut created = Vec::new();
            let published = staged.into_iter().try_for_each(|package| {
                created.extend(package.publish()?);
                Ok::<_, StoreError>(())
            });
            let committed = published.and_then(|()| {
                self.read
                    .conn
                    .execute_batch("COMMIT")
                    .map_err(StoreError::from)
            });
            match committed {
                Ok(()) => return Ok(self.input_version()?),
                Err(error) => {
                    for root in created {
                        let _ = std::fs::remove_dir_all(root);
                    }
                    let _ = self.read.conn.execute_batch("ROLLBACK");
                    return Err(error);
                }
            }
        }
        drop(staged);
        self.queued_work.reset();
        self.read.conn.execute_batch("ROLLBACK")?;
        Ok(base)
    }

    /// Run `f` as one write transaction: `BEGIN IMMEDIATE` when none is
    /// open, so the transaction holds SQLite's write lock from its first
    /// read and never upgrades a read snapshot; inside an open one, a
    /// savepoint in it. On failure everything `f` wrote rolls back (a nested
    /// one only its own writes, so a caller that handles the error commits
    /// none of them). The CAS needs no fixup: a writer finds its segment by
    /// its `cas_segments` row, so a rolled-back allocation goes with its row
    /// (and its id with `next_segment_id`), and bytes a rolled-back append
    /// left in a surviving segment are dead space past its `indexed_len`.
    pub(crate) fn write_txn<T>(
        &mut self,
        f: impl FnOnce(&mut Store) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        if !self.read.conn.is_autocommit() {
            let queued = self.queued_work.mark();
            self.read.conn.execute_batch("SAVEPOINT write_txn")?;
            let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self)));
            let out = match out {
                Ok(Ok(value)) => match self.read.conn.execute_batch("RELEASE write_txn") {
                    Ok(()) => return Ok(value),
                    Err(error) => Err(error.into()),
                },
                Ok(Err(error)) => Err(error),
                Err(panic) => {
                    self.queued_work.restore(queued);
                    let _ = self
                        .read
                        .conn
                        .execute_batch("ROLLBACK TO write_txn; RELEASE write_txn");
                    std::panic::resume_unwind(panic)
                }
            };
            self.queued_work.restore(queued);
            let _ = self
                .read
                .conn
                .execute_batch("ROLLBACK TO write_txn; RELEASE write_txn");
            return out;
        }
        self.refresh_config();
        self.read.conn.execute_batch("BEGIN IMMEDIATE")?;
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self)));
        let out = match out {
            Ok(Ok(value)) => match self
                .queued_work
                .flush(&self.read.conn)
                .and_then(|()| Ok(self.read.conn.execute_batch("COMMIT")?))
            {
                Ok(()) => {
                    self.queued_work.reset();
                    return Ok(value);
                }
                Err(error) => Err(error),
            },
            Ok(Err(error)) => Err(error),
            Err(panic) => {
                self.queued_work.reset();
                let _ = self.read.conn.execute_batch("ROLLBACK");
                std::panic::resume_unwind(panic)
            }
        };
        self.queued_work.reset();
        let _ = self.read.conn.execute_batch("ROLLBACK");
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

/// Create the schema on a fresh database (or check its version), and the
/// store's identity and counters, inside the caller's transaction.
fn create_schema(conn: &Connection) -> Result<StoreInstanceId, StoreError> {
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
    let instance_id = match meta_get_blob(conn, "instance_id")? {
        Some(bytes) if bytes.len() == 16 => {
            let mut id = [0u8; 16];
            id.copy_from_slice(&bytes);
            StoreInstanceId(id)
        }
        _ => {
            let id = StoreInstanceId::mint();
            meta_set_blob(conn, "instance_id", &id.0)?;
            id
        }
    };
    meta_get_u64_or_init(conn, "input_version")?;
    meta_get_u64_or_init(conn, "memo_seq")?;
    Ok(instance_id)
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
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
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

    /// Test hook: every row of `table`, each column debug-formatted, in
    /// sorted order: two stores compare equal table by table.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn table_rows(&self, table: &str) -> Result<Vec<String>, StoreError> {
        let mut statement = self.conn.prepare(&format!("SELECT * FROM {table}"))?;
        let columns = statement.column_count();
        let mut rows = statement.raw_query();
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let values = (0..columns)
                .map(|index| {
                    row.get::<_, rusqlite::types::Value>(index)
                        .map(|value| format!("{value:?}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            out.push(values.join(" | "));
        }
        out.sort();
        Ok(out)
    }

    pub fn instance_id(&self) -> StoreInstanceId {
        self.instance_id
    }

    /// How many database pages this connection has fetched since it opened
    /// (page-cache hits plus misses): a deterministic measure of how much of
    /// the database its reads touched, for tests that pin a query's cost.
    pub fn pages_fetched(&self) -> Result<u64, StoreError> {
        use rusqlite::ffi;
        let mut total = 0;
        for op in [ffi::SQLITE_DBSTATUS_CACHE_HIT, ffi::SQLITE_DBSTATUS_CACHE_MISS] {
            let (mut current, mut highwater) = (0, 0);
            // SAFETY: the handle is this reader's open connection, used on
            // this thread; db_status only reads its counters.
            let code = unsafe {
                ffi::sqlite3_db_status(self.conn.handle(), op, &mut current, &mut highwater, 0)
            };
            if code != ffi::SQLITE_OK {
                return Err(StoreError::Sqlite(rusqlite::Error::SqliteFailure(
                    ffi::Error::new(code),
                    Some("sqlite3_db_status".to_owned()),
                )));
            }
            total += current as u64;
        }
        Ok(total)
    }

    /// The committed input version visible to this connection.
    pub fn input_version(&self) -> Result<InputVersion, StoreError> {
        Ok(InputVersion(meta_get_u64(&self.conn, "input_version")?.unwrap_or(0)))
    }

    /// The committed memo sequence visible to this connection.
    pub fn memo_seq(&self) -> Result<MemoSeq, StoreError> {
        Ok(MemoSeq(meta_get_u64(&self.conn, "memo_seq")?.unwrap_or(0)))
    }

    /// The instance-qualified current version (§13): what crosses the
    /// RPC boundary.
    pub fn stamp(&self) -> Result<SnapshotStamp, StoreError> {
        Ok(SnapshotStamp {
            instance: self.instance_id,
            version: self.input_version()?,
        })
    }

    /// The input version that last published the daemon's compiled
    /// configuration state (schema authority, targets, pipeline epoch,
    /// roots): what in-memory state derived from it is keyed by. `None`
    /// until a publication records one.
    pub fn compiled_version(&self) -> Result<Option<InputVersion>, StoreError> {
        Ok(meta_get_u64(&self.conn, "compiled_version")?.map(InputVersion))
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
    /// The root ids this transaction interned or looked up, by name: a
    /// memo of this transaction only (it rolls back with it), so a
    /// publication's rows name their root without a lookup each.
    pub(crate) roots: std::collections::BTreeMap<String, crate::files::RootId>,
    /// The tool package trees this input staged so far.
    pub(crate) staged_packages: &'a mut Vec<crate::pipeline::StagedPackage>,
    /// The watcher work the open transaction queued so far.
    pub(crate) queued_work: &'a mut crate::files::QueuedWork,
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

    /// Record that this input publishes the daemon's compiled configuration
    /// state (see [`StoreReader::compiled_version`]).
    pub fn mark_compiled(&mut self) -> Result<(), StoreError> {
        meta_set_u64(&self.txn, "compiled_version", self.version.0)
    }
}

/// The writer's page cache: 64 MiB (a negative `cache_size` counts KiB).
const WRITER_PAGE_CACHE_SIZE: i64 = -64 * 1024;

/// Prepared statements a connection keeps. Every fixed statement the store
/// runs is prepared once per connection and reused: a per-row write in a
/// publication loop costs its execution, not a parse. Larger than the
/// store's distinct fixed statements, so a loop's statements never evict
/// one another.
const STATEMENT_CACHE_CAPACITY: usize = 512;

/// A write connection: WAL, a busy timeout (writers queue on SQLite's write
/// lock), foreign keys, and FULL sync.
fn open_writer_connection(db_path: &Path) -> Result<Connection, StoreError> {
    let conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "wal")?;
    conn.busy_timeout(WRITER_BUSY_TIMEOUT)?;
    conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    // A publication writes in one transaction; past the default 2 MiB page
    // cache SQLite spills dirty pages to the WAL mid-transaction (a cold
    // 20k-bundle scan: 8.2 s with the default, 6.3-6.7 s with this). An
    // upper bound, allocated as pages are used.
    conn.pragma_update(None, "cache_size", WRITER_PAGE_CACHE_SIZE)?;
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
        .prepare_cached("SELECT value FROM store_meta WHERE key = ?1")?
        .query_row([key], |r| r.get(0))
        .optional()?)
}

pub(crate) fn meta_set_blob(conn: &Connection, key: &str, value: &[u8]) -> Result<(), StoreError> {
    conn.prepare_cached(
        "INSERT INTO store_meta(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )?
    .execute(rusqlite::params![key, value])?;
    Ok(())
}

pub(crate) fn meta_get_i64(conn: &Connection, key: &str) -> Result<Option<i64>, StoreError> {
    Ok(conn
        .prepare_cached("SELECT value FROM store_meta WHERE key = ?1")?
        .query_row([key], |r| r.get(0))
        .optional()?)
}

pub(crate) fn meta_set_i64(conn: &Connection, key: &str, value: i64) -> Result<(), StoreError> {
    conn.prepare_cached(
        "INSERT INTO store_meta(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )?
    .execute(rusqlite::params![key, value])?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A reader is a read-only SQLite connection, not a second writer.
    #[test]
    fn a_reader_connection_cannot_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        let reader = store.reader().unwrap();
        assert!(reader.conn.execute_batch("CREATE TABLE written (x)").is_err());
    }
}
