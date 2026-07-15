//! The SQLite metadata layer (§13): schema creation, the two version
//! counters, and the transaction discipline of the consistency contract —
//! input transactions advance the input version, memo transactions
//! advance the memo sequence, readers only ever observe a complete
//! version.

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};

use crate::cas::manifest::{self, GenerationManifest};
use crate::config::StoreConfig;
use crate::error::StoreError;
use crate::state::{InputVersion, MemoSeq, SnapshotStamp, StoreInstanceId};

/// The metadata schema version this crate reads and writes, recorded as
/// SQLite's `user_version`. There is deliberately no in-place migration
/// story: daemon state is disposable (§2), so a mismatch is a typed error
/// and the remedy is [`Store::recreate`].
pub const SCHEMA_VERSION: u32 = 22;

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
    PRIMARY KEY (root_id, path)
);
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
    logical_hash BLOB
);
CREATE INDEX assets_by_bundle ON assets(bundle_uuid);
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
    last_used    INTEGER NOT NULL,
    PRIMARY KEY (key_kind, static_key, trace_digest)
);
CREATE TABLE derived_outputs (
    child_uuid  BLOB NOT NULL PRIMARY KEY,
    parent_uuid BLOB NOT NULL,
    output_key  TEXT NOT NULL
);
CREATE TABLE derived_assertions (
    child_uuid  BLOB NOT NULL,
    parent_uuid BLOB NOT NULL,
    output_key  TEXT NOT NULL,
    memo_seq    INTEGER NOT NULL,
    PRIMARY KEY (child_uuid, memo_seq)
);
CREATE TABLE cas_extents (
    content_hash BLOB NOT NULL PRIMARY KEY,
    segment      INTEGER NOT NULL,
    offset       INTEGER NOT NULL,
    len          INTEGER NOT NULL
);
CREATE TABLE cas_segments (
    segment_id  INTEGER PRIMARY KEY,
    file_name   TEXT NOT NULL,
    segment_kind INTEGER NOT NULL,
    indexed_len INTEGER NOT NULL
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
    acceptance_candidate_dylib_hash BLOB,
    acceptance_manifest_hash BLOB,
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
CREATE TABLE pipeline_candidate_schema_registry (
    type_uuid   BLOB NOT NULL PRIMARY KEY,
    logical_hash BLOB NOT NULL
);
CREATE TABLE pipeline_target_set (
    name                   TEXT NOT NULL PRIMARY KEY,
    target_definition_hash BLOB NOT NULL
);
CREATE TABLE pipeline_candidate_target_set (
    name                   TEXT NOT NULL PRIMARY KEY,
    target_definition_hash BLOB NOT NULL
);
CREATE TABLE configuration_state (
    id                 INTEGER PRIMARY KEY CHECK (id = 0),
    active_generation  INTEGER NOT NULL,
    input_version      INTEGER NOT NULL,
    poison_code        INTEGER CHECK (poison_code IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)),
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
CREATE TABLE schema_lineage (
    type_uuid     BLOB NOT NULL,
    generation    INTEGER NOT NULL,
    schema_hash   BLOB NOT NULL,
    forward_parent INTEGER,
    input_version INTEGER NOT NULL,
    PRIMARY KEY (type_uuid, generation)
);
CREATE UNIQUE INDEX lineage_by_hash ON schema_lineage(type_uuid, schema_hash);
CREATE TABLE schema_lineage_current (
    type_uuid     BLOB NOT NULL PRIMARY KEY,
    current_cursor INTEGER NOT NULL,
    chain_digest  BLOB NOT NULL,
    authority     INTEGER NOT NULL CHECK (authority IN (0, 1)),
    retired_from  INTEGER,
    input_version INTEGER NOT NULL
);
CREATE TABLE schema_lineage_state (
    id            INTEGER PRIMARY KEY CHECK (id = 0),
    input_version INTEGER NOT NULL,
    manifest_hash BLOB NOT NULL
);
CREATE TABLE pins (
    kind         INTEGER NOT NULL,
    holder       TEXT NOT NULL,
    content_hash BLOB NOT NULL,
    PRIMARY KEY (kind, holder, content_hash)
);
CREATE INDEX pins_by_hash ON pins(content_hash);
CREATE TABLE write_intents (
    intent_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    target_path     TEXT NOT NULL,
    temp_path       TEXT NOT NULL,
    conflict_path   TEXT NOT NULL,
    pre_image_hash  BLOB,
    proposed_hash   BLOB NOT NULL,
    rename_aside_state INTEGER NOT NULL DEFAULT 0
        CHECK (rename_aside_state IN (0, 1, 2, 3, 4, 5, 6, 7)),
    retired         INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE displaced (
    displacement_id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id       INTEGER NOT NULL,
    ordinal         INTEGER NOT NULL,
    content_hash    BLOB NOT NULL,
    origin_path     TEXT NOT NULL,
    quarantine_path TEXT NOT NULL UNIQUE,
    quarantined_at  INTEGER NOT NULL,
    restored        INTEGER NOT NULL DEFAULT 0,
    cleaned_at      INTEGER,
    cleanup_reason  TEXT,
    UNIQUE(intent_id, ordinal)
);
CREATE TABLE publication_groups (
    group_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        INTEGER NOT NULL CHECK (kind IN (1, 2, 3, 4, 5, 6)),
    basis       BLOB NOT NULL,
    retired     INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1))
);
CREATE TABLE publication_group_children (
    group_id    INTEGER NOT NULL,
    ordinal     INTEGER NOT NULL,
    intent_id   INTEGER NOT NULL UNIQUE,
    PRIMARY KEY (group_id, ordinal)
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
";

/// The store: the SQLite metadata layer plus the log-structured CAS,
/// opened from one `state_path`. One writer, many snapshot readers (§13):
/// the coordinator owns the `Store`; every write goes through
/// [`Store::input_transaction`] or the memo-side commit APIs.
pub struct Store {
    pub(crate) conn: Connection,
    // (no Debug derive: Connection is not Debug-friendly; see impl below)
    pub(crate) config: StoreConfig,
    pub(crate) cas: crate::cas::store::CasInner,
    instance_id: StoreInstanceId,
    input_version: u64,
    memo_seq: u64,
    last_recovery: crate::cas::RecoveryReport,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("state_path", &self.config.state_path)
            .field("instance_id", &self.instance_id)
            .field("input_version", &self.input_version)
            .field("memo_seq", &self.memo_seq)
            .finish()
    }
}

impl Store {
    pub fn state_path(&self) -> &Path {
        &self.config.state_path
    }

    /// Open (creating if absent) the daemon state under
    /// `config.state_path`.
    pub fn open(config: StoreConfig) -> Result<Store, StoreError> {
        let state_path = &config.state_path;
        create_dir(state_path)?;
        create_dir(&state_path.join("cas"))?;
        create_dir(&state_path.join("tools"))?;
        crate::pipeline::cleanup_staged_tool_temps(state_path)?;

        let db_path = state_path.join("meta.sqlite");
        let conn = Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        // FULL: every committed transaction is durable — §14's journal
        // demands "fsynced before the first rename", and §13's index
        // rows must never lead the segment fsync they follow.
        conn.pragma_update(None, "synchronous", "FULL")?;

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
        let input_version = meta_get_u64_or_init(&conn, "input_version")?;
        let memo_seq = meta_get_u64_or_init(&conn, "memo_seq")?;

        // Ephemeral state carries no version and rebuilds from scratch
        // (§13): lease, in-flight, and pack-session pins die with the
        // process; only manifest pins persist.
        conn.execute(
            "DELETE FROM pins WHERE kind != ?1",
            [crate::artifacts::PinKind::Manifest as i64],
        )?;

        // Bootstrap the CAS generation manifest: CURRENT is the single
        // authority for the active segment set (§13).
        let cas_dir = state_path.join("cas");
        if !cas_dir.join("CURRENT").exists() {
            manifest::write_current(
                &cas_dir,
                &GenerationManifest {
                    generation: 0,
                    segments: Vec::new(),
                },
            )?;
        }
        if meta_get_u64(&conn, "cas_generation")?.is_none() {
            meta_set_u64(&conn, "cas_generation", 0)?;
        }

        let mut store = Store {
            conn,
            config,
            cas: Default::default(),
            instance_id,
            input_version,
            memo_seq,
            last_recovery: Default::default(),
        };
        store.init_cas()?;
        store.last_recovery = store.recover_cas()?;
        Ok(store)
    }

    /// What startup recovery found and did (§13's classification).
    pub fn recovery_report(&self) -> &crate::cas::RecoveryReport {
        &self.last_recovery
    }

    /// Rebuild every SQLite index from authoritative table rows. This is a
    /// maintenance action only; callers publish its input-version event in the
    /// same transaction boundary as their other doctor result state.
    pub fn rebuild_indexes(&self) -> Result<(), StoreError> {
        self.conn.execute_batch("REINDEX")?;
        Ok(())
    }

    /// Recovery adopted committed groups and advanced the persisted memo
    /// counter inside its own transactions; sync the cached value.
    pub(crate) fn set_memo_seq(&mut self, seq: u64) {
        self.memo_seq = seq;
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

    pub fn instance_id(&self) -> StoreInstanceId {
        self.instance_id
    }

    pub fn input_version(&self) -> InputVersion {
        InputVersion(self.input_version)
    }

    pub fn memo_seq(&self) -> MemoSeq {
        MemoSeq(self.memo_seq)
    }

    /// The instance-qualified current version (§13): what crosses the
    /// RPC boundary.
    pub fn stamp(&self) -> SnapshotStamp {
        SnapshotStamp {
            instance: self.instance_id,
            version: self.input_version(),
        }
    }

    /// Apply one watcher batch or authoring operation as an atomic
    /// transaction advancing the input version (§13). On any error the
    /// whole transaction rolls back and the version does not advance:
    /// readers only ever observe a complete input version.
    pub fn input_transaction<T, F>(&mut self, f: F) -> Result<(T, InputVersion), StoreError>
    where
        F: FnOnce(&mut InputTxn<'_>) -> Result<T, StoreError>,
    {
        let base_stamp = self.stamp();
        let version = InputVersion(self.input_version + 1);
        let state_path = self.config.state_path.clone();
        let txn = self.conn.transaction()?;
        let mut input_txn = InputTxn {
            txn,
            base_stamp,
            version,
            state_path,
        };
        let out = f(&mut input_txn)?;
        meta_set_u64(&input_txn.txn, "input_version", version.0)?;
        input_txn.txn.commit()?;
        self.input_version = version.0;
        Ok((out, version))
    }

    /// Attach memo state to an input basis without advancing any input
    /// version (§13): build results move only the memo sequence.
    pub(crate) fn memo_transaction<T, F>(&mut self, f: F) -> Result<(T, MemoSeq), StoreError>
    where
        F: FnOnce(&rusqlite::Transaction<'_>, MemoSeq) -> Result<T, StoreError>,
    {
        let seq = MemoSeq(self.memo_seq + 1);
        let txn = self.conn.transaction()?;
        let out = f(&txn, seq)?;
        meta_set_u64(&txn, "memo_seq", seq.0)?;
        txn.commit()?;
        self.memo_seq = seq.0;
        Ok((out, seq))
    }

    /// §14's clean watermark: the newest mtime observed under active
    /// watch, recorded durably per session; reconciliation content-hashes
    /// anything not strictly older.
    pub fn clean_watermark(&self) -> Result<Option<i64>, StoreError> {
        meta_get_i64(&self.conn, "clean_watermark")
    }
}

/// One atomic input-version transaction (§13). Every input-versioned
/// table writes through methods on this; dropping without commit rolls
/// everything back.
pub struct InputTxn<'a> {
    pub(crate) txn: rusqlite::Transaction<'a>,
    base_stamp: SnapshotStamp,
    version: InputVersion,
    pub(crate) state_path: PathBuf,
}

impl InputTxn<'_> {
    /// The exact committed snapshot against which this transaction began.
    /// Coordinator control reads used by authority commands must name this
    /// stamp; a bare or stale version can never authorize publication.
    pub fn base_stamp(&self) -> SnapshotStamp {
        self.base_stamp
    }

    /// The version this transaction will publish on commit.
    pub fn version(&self) -> InputVersion {
        self.version
    }

    /// Record §14's clean watermark.
    pub fn set_clean_watermark(&mut self, mtime: i64) -> Result<(), StoreError> {
        meta_set_i64(&self.txn, "clean_watermark", mtime)
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
