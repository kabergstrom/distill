//! Served RPC state (LOCKLESS.md §2.2): what the RPC front ends answer
//! from, as current-state rows.
//!
//! The namespace itself is the ordinary `bundles` / `assets` / `asset_tags`
//! / `schemas` rows and the derived outputs `source_claims` names. This
//! module adds the served-only facts next to them: the change log that
//! subscriptions and reconnect fences read, the RPC target set, the
//! published pipeline diagnostic, and the typed load edges of stored
//! artifacts.
//!
//! A snapshot is a read transaction over these tables
//! ([`StoreReader::begin_snapshot`]); nothing here is versioned.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use rusqlite::{Connection, OptionalExtension};

use crate::bundles::{blob16, blob32};
use crate::db::{meta_get_u64, meta_set_u64, InputTxn, StoreReader};
use crate::error::StoreError;
use crate::state::{InputVersion, SnapshotStamp};

const RPC_PIPELINE_GENERATION: &str = "rpc_pipeline_generation";
const CHANGE_LOG_OLDEST: &str = "change_log_oldest";
/// Install target `?1` with definition `?2`, or replace its definition.
pub(crate) const SET_RPC_TARGET: &str =
    "INSERT INTO rpc_targets(name, definition_hash) VALUES (?1, ?2)
     ON CONFLICT(name) DO UPDATE SET definition_hash = excluded.definition_hash";
/// A pipeline fence row at version `?1` (kind `?2`: [`CHANGE_RECONNECT_ALL`]).
pub(crate) const APPEND_RECONNECT_ALL: &str =
    "INSERT INTO change_log(version, kind) VALUES (?1, ?2)";
/// Drop an artifact's load edges ahead of recording its latest install's.
pub(crate) const DELETE_LOAD_EDGES: &str =
    "DELETE FROM artifact_load_edges WHERE content_hash = ?1";

/// One served authoring entry without its schema and value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedEntryMeta {
    pub asset: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub normalized_path: String,
    pub type_uuid: TypeUuid,
    pub terminal_type: TypeUuid,
    pub logical_hash: LogicalHash,
    pub authoring_only: bool,
    pub tags: BTreeMap<String, Option<String>>,
}

/// How a published asset resolves (see [`StoreReader::asset_resolution`]);
/// an asset with none is missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetResolution {
    /// Its row is published: it builds on demand.
    Published,
    /// It fails with its bundle's poison or the error withholding it.
    Failed(String),
}

/// A published asset's row and its bundle's poison.
pub(crate) const ASSET_POISON: &str = "SELECT b.poison FROM assets a
     JOIN bundles b ON b.bundle_uuid = a.bundle_uuid WHERE a.asset_uuid = ?1";
/// The collision withholding asset `?1`: its own UUID's (scope 3).
pub(crate) const ASSET_COLLISION: &str =
    "SELECT message FROM errors WHERE scope_kind = 3 AND scope_id = ?1 AND family = ?2";
/// The collision withholding asset `?1`'s bundle: the bundle UUID (scope 2)
/// a source authoring the asset claims. The joins are ordered (`CROSS
/// JOIN`) so errors are searched by their full scope, not scanned per kind.
pub(crate) const ASSET_BUNDLE_COLLISION: &str = "SELECT e.message FROM source_claims a
     CROSS JOIN source_claims b ON b.root_id = a.root_id AND b.path = a.path AND b.kind = 0
     CROSS JOIN errors e ON e.scope_kind = 2 AND e.scope_id = b.subject AND e.family = ?2
     WHERE a.kind = 1 AND a.subject = ?1 LIMIT 1";
/// Every asset a namespace error withholds (see
/// [`ASSET_COLLISION`], [`ASSET_BUNDLE_COLLISION`]): each colliding asset
/// UUID, and each asset a source claiming a colliding bundle UUID authors.
/// Searches from the collision rows, so it costs the defects.
pub(crate) const WITHHELD_ASSETS: &str = "SELECT scope_id FROM errors
     WHERE scope_kind = 3 AND family = ?1
     UNION SELECT a.subject FROM errors e
     CROSS JOIN source_claims b ON b.kind = 0 AND b.subject = e.scope_id
     CROSS JOIN source_claims a ON a.root_id = b.root_id AND a.path = b.path AND a.kind = 1
     WHERE e.scope_kind = 2 AND e.family = ?1";

/// One `change_log` payload. Reason and state codes belong to the RPC layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// A published asset delta.
    Asset { asset: AssetUuid, state: u8 },
    /// A published path delta.
    Path { path: String },
    /// The pipeline changed: every connection must reconnect.
    ReconnectAll,
}

const CHANGE_ASSET: i64 = 1;
const CHANGE_PATH: i64 = 2;
const CHANGE_RECONNECT_ALL: i64 = 3;

/// Asset `?1`'s published deltas with `?2 < version <= ?3`, on the asset
/// deltas' partial index (kind literal: [`CHANGE_ASSET`]).
pub(crate) const ASSET_HISTORY: &str =
    "SELECT seq, version, kind, asset_uuid, state, subject
     FROM change_log WHERE kind = 1 AND asset_uuid = ?1 AND version > ?2 AND version <= ?3";
/// Path `?1`'s published deltas with `?2 < version <= ?3`, on the path
/// deltas' partial index (kind literal: [`CHANGE_PATH`]).
pub(crate) const PATH_HISTORY: &str =
    "SELECT seq, version, kind, asset_uuid, state, subject
     FROM change_log WHERE kind = 2 AND subject = ?1 AND version > ?2 AND version <= ?3";
const _: () = assert!(CHANGE_ASSET == 1 && CHANGE_PATH == 2);

/// One `change_log` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeEntry {
    pub seq: i64,
    pub version: InputVersion,
    pub change: Change,
}

/// One `rpc_targets` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcTargetRow {
    pub name: String,
    pub definition_hash: [u8; 32],
}

/// A read transaction held open on its own connection: every read sees the
/// one committed input version current when it began. Dropping it ends the
/// transaction.
pub struct StoreSnapshot {
    reader: Option<StoreReader>,
    stamp: SnapshotStamp,
}

impl std::fmt::Debug for StoreSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreSnapshot")
            .field("stamp", &self.stamp)
            .finish()
    }
}

impl StoreReader {
    /// Begin a read transaction on this connection and pin the current
    /// committed version.
    pub fn begin_snapshot(self) -> Result<StoreSnapshot, StoreError> {
        self.conn.execute_batch("BEGIN DEFERRED")?;
        self.counters.begin();
        // The first read establishes the WAL read mark.
        let stamp = match self.input_version() {
            Ok(version) => SnapshotStamp {
                instance: self.instance_id(),
                version,
            },
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                self.counters.end();
                return Err(error);
            }
        };
        Ok(StoreSnapshot {
            reader: Some(self),
            stamp,
        })
    }
}

impl StoreSnapshot {
    pub fn stamp(&self) -> SnapshotStamp {
        self.stamp
    }

    /// End the read transaction and return the connection for reuse.
    pub fn into_reader(mut self) -> Result<StoreReader, StoreError> {
        let reader = self.reader.take().expect("snapshot owns its reader");
        let ended = reader.conn.execute_batch("ROLLBACK");
        reader.counters.end();
        ended?;
        Ok(reader)
    }
}

impl std::ops::Deref for StoreSnapshot {
    type Target = StoreReader;

    fn deref(&self) -> &StoreReader {
        self.reader.as_ref().expect("snapshot owns its reader")
    }
}

impl Drop for StoreSnapshot {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.take() {
            let _ = reader.conn.execute_batch("ROLLBACK");
            reader.counters.end();
        }
    }
}

const SERVED_ENTRY_COLUMNS: &str = "a.asset_uuid, a.bundle_uuid, a.local_id, b.path, a.type_uuid,
     a.terminal_type, a.logical_hash, a.authoring_only";
macro_rules! served_entry_where {
    () => {
        "a.terminal_type IS NOT NULL AND a.logical_hash IS NOT NULL AND b.poison IS NULL"
    };
}
pub(crate) const SERVED_ENTRY_WHERE: &str = served_entry_where!();
const SERVED_ENTRY_FROM: &str = concat!(
    "FROM assets a JOIN bundles b ON b.bundle_uuid = a.bundle_uuid WHERE ",
    served_entry_where!()
);
/// Every served runtime entry's UUID, authored type and terminal type.
pub(crate) const SERVED_RUNTIME_ENTRY_TYPES: &str = concat!(
    "SELECT a.asset_uuid, a.type_uuid, a.terminal_type
     FROM assets a JOIN bundles b ON b.bundle_uuid = a.bundle_uuid WHERE ",
    served_entry_where!(),
    " AND a.authoring_only = 0 ORDER BY a.asset_uuid"
);

fn served_entry_meta_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ServedEntryMeta> {
    Ok(ServedEntryMeta {
        asset: AssetUuid(blob16(row.get(0)?)),
        bundle: BundleUuid(blob16(row.get(1)?)),
        local_id: row.get(2)?,
        normalized_path: row.get(3)?,
        type_uuid: TypeUuid(blob16(row.get(4)?)),
        terminal_type: TypeUuid(blob16(row.get(5)?)),
        logical_hash: LogicalHash(blob32(row.get(6)?)),
        authoring_only: row.get::<_, i64>(7)? != 0,
        tags: BTreeMap::new(),
    })
}

impl StoreReader {
    /// Every served authoring entry, ordered by asset, with tags.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn served_entries(&self) -> Result<Vec<ServedEntryMeta>, StoreError> {
        let mut statement = self.conn.prepare_cached(&format!(
            "SELECT {SERVED_ENTRY_COLUMNS} {SERVED_ENTRY_FROM} ORDER BY a.asset_uuid"
        ))?;
        let mut entries = statement
            .query_map([], served_entry_meta_row)?
            .collect::<Result<Vec<_>, _>>()?;
        let mut tags = self.conn.prepare_cached(
            "SELECT asset_uuid, tag, value FROM asset_tags ORDER BY asset_uuid, tag",
        )?;
        let mut by_asset = BTreeMap::<AssetUuid, BTreeMap<String, Option<String>>>::new();
        for row in tags.query_map([], |row| {
            Ok((
                AssetUuid(blob16(row.get(0)?)),
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })? {
            let (asset, tag, value) = row?;
            by_asset.entry(asset).or_default().insert(tag, value);
        }
        for entry in &mut entries {
            if let Some(tags) = by_asset.remove(&entry.asset) {
                entry.tags = tags;
            }
        }
        Ok(entries)
    }

    /// The served authoring entries `filter` selects and whose bundle's path
    /// `glob` matches, with bundle and logical path, by asset UUID; or the
    /// poisoned bundles whose rows the query could select (see
    /// [`StoreReader::namespace_assets_matching`]).
    pub fn served_assets_matching(
        &self,
        filter: &crate::bundles::AssetFilter,
        glob: impl Fn(&str) -> bool,
    ) -> Result<crate::bundles::AssetAnswer, StoreError> {
        self.assets_matching(filter, SERVED_ENTRY_WHERE, glob)
    }

    /// Every served runtime (not authoring-only) entry's UUID, authored type
    /// and terminal type, by asset UUID, in one statement: what a
    /// verification build request names of its entry.
    pub fn served_runtime_entry_types(
        &self,
    ) -> Result<Vec<(AssetUuid, TypeUuid, TypeUuid)>, StoreError> {
        self.query_rows(SERVED_RUNTIME_ENTRY_TYPES, [], |row| {
            Ok((
                AssetUuid(blob16(row.get(0)?)),
                TypeUuid(blob16(row.get(1)?)),
                TypeUuid(blob16(row.get(2)?)),
            ))
        })
    }

    /// One served authoring entry's metadata.
    pub fn served_entry_meta(
        &self,
        asset: AssetUuid,
    ) -> Result<Option<ServedEntryMeta>, StoreError> {
        let mut statement = self.conn.prepare_cached(&format!(
            "SELECT {SERVED_ENTRY_COLUMNS} {SERVED_ENTRY_FROM} AND a.asset_uuid = ?1"
        ))?;
        let Some(mut meta) = statement
            .query_row([asset.0.as_slice()], served_entry_meta_row)
            .optional()?
        else {
            return Ok(None);
        };
        meta.tags = self.asset_tag_map(asset)?;
        Ok(Some(meta))
    }

    fn asset_tag_map(
        &self,
        asset: AssetUuid,
    ) -> Result<BTreeMap<String, Option<String>>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT tag, value FROM asset_tags WHERE asset_uuid = ?1 ORDER BY tag",
        )?;
        let rows = statement.query_map([asset.0.as_slice()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// How `asset` resolves, by point reads: its published row (failed
    /// with its bundle's poison), else the namespace error withholding it
    /// (its own UUID's collision, else its bundle UUID's). `None` is
    /// missing: never published, or deleted.
    pub fn asset_resolution(
        &self,
        asset: AssetUuid,
    ) -> Result<Option<AssetResolution>, StoreError> {
        let id = asset.0.as_slice();
        if let Some(poison) = self
            .conn
            .prepare_cached(ASSET_POISON)?
            .query_row([id], |row| row.get::<_, Option<String>>(0))
            .optional()?
        {
            return Ok(Some(
                poison.map_or(AssetResolution::Published, AssetResolution::Failed),
            ));
        }
        Ok(self.withholding(asset)?.map(AssetResolution::Failed))
    }

    /// The namespace error withholding `asset`: its own UUID's collision,
    /// else its bundle UUID's.
    pub(crate) fn withholding(&self, asset: AssetUuid) -> Result<Option<String>, StoreError> {
        for sql in [ASSET_COLLISION, ASSET_BUNDLE_COLLISION] {
            if let Some(message) = self
                .conn
                .prepare_cached(sql)?
                .query_row(
                    rusqlite::params![asset.0.as_slice(), crate::errors::NAMESPACE],
                    |row| row.get(0),
                )
                .optional()?
            {
                return Ok(Some(message));
            }
        }
        Ok(None)
    }

    /// Every asset a namespace error withholds (see [`Self::withholding`]).
    pub fn withheld_assets(&self) -> Result<BTreeSet<AssetUuid>, StoreError> {
        let mut statement = self.conn.prepare_cached(WITHHELD_ASSETS)?;
        let rows =
            statement.query_map([crate::errors::NAMESPACE], |row| row.get::<_, Vec<u8>>(0))?;
        rows.map(|row| Ok(AssetUuid(blob16(row?)))).collect()
    }

    /// Every asset a logical path names; more than one is an ambiguity.
    pub fn served_path_candidates(&self, path: &str) -> Result<BTreeSet<AssetUuid>, StoreError> {
        let mut statement = self.conn.prepare_cached(crate::bundles::PATH_PRIMARIES)?;
        let rows = statement.query_map([path], |row| row.get::<_, Vec<u8>>(0))?;
        rows.map(|row| row.map(|bytes| AssetUuid(blob16(bytes))))
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(StoreError::from)
    }

    /// Every runtime asset named `name` (its local id) in a bundle at
    /// `path`; more than one (bundles of that path under several roots) is
    /// an ambiguity.
    pub fn served_named_candidates(
        &self,
        path: &str,
        name: &str,
    ) -> Result<BTreeSet<AssetUuid>, StoreError> {
        let mut statement = self.conn.prepare_cached(&format!(
            "SELECT a.asset_uuid {SERVED_ENTRY_FROM}
               AND b.path = ?1 AND a.local_id = ?2 AND a.authoring_only = 0"
        ))?;
        let rows = statement.query_map([path, name], |row| row.get::<_, Vec<u8>>(0))?;
        rows.map(|row| row.map(|bytes| AssetUuid(blob16(bytes))))
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(StoreError::from)
    }

    /// Served assets whose tag index is poisoned (pending or failed), with
    /// the owning bundle.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn tag_poisoned_assets(&self) -> Result<Vec<(AssetUuid, BundleUuid)>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT asset_uuid, bundle_uuid FROM assets INDEXED BY assets_tag_poisoned
             WHERE tag_poison IS NOT NULL ORDER BY asset_uuid",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                AssetUuid(blob16(row.get(0)?)),
                BundleUuid(blob16(row.get(1)?)),
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    /// The pipeline reconnect generation every connection records at
    /// connect time.
    pub fn rpc_pipeline_generation(&self) -> Result<u64, StoreError> {
        Ok(meta_get_u64(&self.conn, RPC_PIPELINE_GENERATION)?.unwrap_or(0))
    }

    pub fn rpc_targets(&self) -> Result<Vec<RpcTargetRow>, StoreError> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT name, definition_hash FROM rpc_targets ORDER BY name")?;
        let rows = statement.query_map([], rpc_target_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn rpc_target(&self, name: &str) -> Result<Option<RpcTargetRow>, StoreError> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT name, definition_hash FROM rpc_targets WHERE name = ?1")?;
        Ok(statement.query_row([name], rpc_target_row).optional()?)
    }

    /// The newest change-log sequence, or 0.
    pub fn change_log_head(&self) -> Result<i64, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT COALESCE(MAX(seq), 0) FROM change_log", [], |row| {
                row.get(0)
            })?)
    }

    /// Change-log rows after `seq`, oldest first.
    pub fn change_log_after(&self, seq: i64) -> Result<Vec<ChangeEntry>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT seq, version, kind, asset_uuid, state, subject
             FROM change_log WHERE seq > ?1 ORDER BY seq",
        )?;
        let rows = statement.query_map([seq], change_entry_row)?;
        rows.collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
    }

    /// The published deltas of `assets` and `paths` with
    /// `since < version <= upto`, ordered by version then sequence: one
    /// search per subject ([`ASSET_HISTORY`], [`PATH_HISTORY`]), never the
    /// whole window.
    pub fn change_log_history(
        &self,
        since: InputVersion,
        upto: InputVersion,
        assets: &BTreeSet<AssetUuid>,
        paths: &BTreeSet<String>,
    ) -> Result<Vec<ChangeEntry>, StoreError> {
        let window = [since.0 as i64, upto.0 as i64];
        let mut entries = Vec::new();
        let mut by_asset = self.conn.prepare_cached(ASSET_HISTORY)?;
        for asset in assets {
            let rows = by_asset.query_map(
                rusqlite::params![asset.0.as_slice(), window[0], window[1]],
                change_entry_row,
            )?;
            for row in rows {
                entries.push(row??);
            }
        }
        let mut by_path = self.conn.prepare_cached(PATH_HISTORY)?;
        for path in paths {
            let rows = by_path.query_map(
                rusqlite::params![path, window[0], window[1]],
                change_entry_row,
            )?;
            for row in rows {
                entries.push(row??);
            }
        }
        // Sequence order is version order: versions commit in sequence.
        entries.sort_by_key(|entry| entry.seq);
        Ok(entries)
    }

    /// The oldest version a subscription cursor may resume from.
    pub fn change_log_oldest(&self) -> Result<InputVersion, StoreError> {
        Ok(InputVersion(
            meta_get_u64(&self.conn, CHANGE_LOG_OLDEST)?.unwrap_or(0),
        ))
    }

    /// The typed direct load edges recorded for an artifact, sorted.
    pub fn artifact_load_edges(
        &self,
        hash: ContentHash,
    ) -> Result<Vec<(AssetUuid, TypeUuid)>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT asset_uuid, expected_terminal FROM artifact_load_edges
             WHERE content_hash = ?1 ORDER BY asset_uuid",
        )?;
        let rows = statement.query_map([hash.0.as_slice()], |row| {
            Ok((
                AssetUuid(blob16(row.get(0)?)),
                TypeUuid(blob16(row.get(1)?)),
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }
}

fn rpc_target_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RpcTargetRow> {
    Ok(RpcTargetRow {
        name: row.get(0)?,
        definition_hash: blob32(row.get(1)?),
    })
}

fn change_entry_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Result<ChangeEntry, StoreError>> {
    let seq: i64 = row.get(0)?;
    let version = InputVersion(row.get::<_, i64>(1)? as u64);
    let kind: i64 = row.get(2)?;
    let asset: Option<Vec<u8>> = row.get(3)?;
    let state: Option<i64> = row.get(4)?;
    let subject: Option<String> = row.get(5)?;
    let corrupt = || {
        StoreError::Sqlite(rusqlite::Error::InvalidColumnType(
            2,
            "change_log".to_owned(),
            rusqlite::types::Type::Integer,
        ))
    };
    let change = match (kind, asset, state, subject) {
        (CHANGE_ASSET, Some(asset), Some(state), _) => Change::Asset {
            asset: AssetUuid(blob16(asset)),
            state: state as u8,
        },
        (CHANGE_PATH, _, _, Some(path)) => Change::Path { path },
        (CHANGE_RECONNECT_ALL, _, _, _) => Change::ReconnectAll,
        _ => return Ok(Err(corrupt())),
    };
    Ok(Ok(ChangeEntry {
        seq,
        version,
        change,
    }))
}

/// Served-state writes shared by input transactions and served-only
/// transactions (those that change fences without a new input version).
pub trait ServedWrite {
    #[doc(hidden)]
    fn served_conn(&self) -> &Connection;

    /// The input version this transaction's change-log rows belong to: the
    /// version being published, or the current one for served-only writes.
    fn change_version(&self) -> InputVersion;

    /// Every RPC target row as this transaction sees them.
    fn txn_rpc_targets(&self) -> Result<Vec<RpcTargetRow>, StoreError> {
        let mut statement = self
            .served_conn()
            .prepare_cached("SELECT name, definition_hash FROM rpc_targets ORDER BY name")?;
        let rows = statement.query_map([], rpc_target_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    /// Append one change-log row at `version`.
    fn append_change(&mut self, version: InputVersion, change: &Change) -> Result<(), StoreError> {
        let conn = self.served_conn();
        let version = version.0 as i64;
        match change {
            Change::Asset { asset, state } => conn
                .prepare_cached(
                    "INSERT INTO change_log(version, kind, asset_uuid, state) VALUES (?1, ?2, ?3, ?4)",
                )?
                .execute(rusqlite::params![version, CHANGE_ASSET, asset.0.as_slice(), *state as i64])?,
            Change::Path { path } => conn
                .prepare_cached(
                    "INSERT INTO change_log(version, kind, subject) VALUES (?1, ?2, ?3)",
                )?
                .execute(rusqlite::params![version, CHANGE_PATH, path])?,
            Change::ReconnectAll => conn
                .prepare_cached(APPEND_RECONNECT_ALL)?
                .execute(rusqlite::params![version, CHANGE_RECONNECT_ALL])?,
        };
        Ok(())
    }

    /// Keep at most `retained` versions of history before `current`; older
    /// rows go and the oldest resumable cursor advances.
    fn trim_change_log(&mut self, current: InputVersion, retained: u64) -> Result<(), StoreError> {
        let conn = self.served_conn();
        let oldest = meta_get_u64(conn, CHANGE_LOG_OLDEST)?.unwrap_or(0);
        let floor = current.0.saturating_sub(retained);
        if floor > oldest {
            conn.prepare_cached("DELETE FROM change_log WHERE version <= ?1")?
                .execute([floor as i64])?;
            meta_set_u64(conn, CHANGE_LOG_OLDEST, floor)?;
        }
        Ok(())
    }

    /// Initialise the oldest resumable cursor if the store has none yet.
    fn init_change_log_oldest(&mut self, oldest: InputVersion) -> Result<(), StoreError> {
        let conn = self.served_conn();
        if meta_get_u64(conn, CHANGE_LOG_OLDEST)?.is_none() {
            meta_set_u64(conn, CHANGE_LOG_OLDEST, oldest.0)?;
        }
        Ok(())
    }

    /// Advance the pipeline reconnect generation and return the new value.
    fn bump_rpc_pipeline_generation(&mut self) -> Result<u64, StoreError> {
        let conn = self.served_conn();
        let next = meta_get_u64(conn, RPC_PIPELINE_GENERATION)?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| StoreError::InvalidConfiguration {
                error: "pipeline generation exhausted".to_owned(),
            })?;
        meta_set_u64(conn, RPC_PIPELINE_GENERATION, next)?;
        Ok(next)
    }

    /// Install one RPC target, or replace its definition.
    fn set_rpc_target(&mut self, name: &str, definition_hash: [u8; 32]) -> Result<(), StoreError> {
        self.served_conn()
            .prepare_cached(SET_RPC_TARGET)?
            .execute(rusqlite::params![name, definition_hash.as_slice()])?;
        Ok(())
    }

    fn remove_rpc_target(&mut self, name: &str) -> Result<(), StoreError> {
        self.served_conn()
            .prepare_cached("DELETE FROM rpc_targets WHERE name = ?1")?
            .execute([name])?;
        Ok(())
    }

    /// Record an artifact's typed direct load edges, replacing any an
    /// earlier install recorded. The DSTL bytes carry the edges' assets but
    /// not their expected terminals, so the same bytes rebuilt after a
    /// dependency's terminal type changed carry other edges: the latest
    /// install's are the artifact's. The rows go with the artifact's
    /// extent (`ON DELETE CASCADE`).
    fn record_artifact_load_edges(
        &mut self,
        hash: ContentHash,
        edges: &[(AssetUuid, TypeUuid)],
    ) -> Result<(), StoreError> {
        let conn = self.served_conn();
        conn.prepare_cached(DELETE_LOAD_EDGES)?
            .execute([hash.0.as_slice()])?;
        let mut insert = conn.prepare_cached(
            "INSERT INTO artifact_load_edges(content_hash, asset_uuid, expected_terminal)
             VALUES (?1, ?2, ?3)",
        )?;
        for (asset, terminal) in edges {
            insert.execute(rusqlite::params![
                hash.0.as_slice(),
                asset.0.as_slice(),
                terminal.0.as_slice()
            ])?;
        }
        Ok(())
    }
}

impl ServedWrite for InputTxn<'_> {
    fn served_conn(&self) -> &Connection {
        self.txn
    }

    fn change_version(&self) -> InputVersion {
        self.version()
    }
}

/// A transaction that changes only served state (fences, diagnostics,
/// artifact edges) without publishing a new input version.
pub struct ServedTxn<'a> {
    txn: &'a Connection,
    version: InputVersion,
}

impl ServedTxn<'_> {
    /// The current input version this transaction annotates.
    pub fn version(&self) -> InputVersion {
        self.version
    }
}

impl ServedWrite for ServedTxn<'_> {
    fn served_conn(&self) -> &Connection {
        self.txn
    }

    fn change_version(&self) -> InputVersion {
        self.version
    }
}

impl crate::db::Store {
    /// Change served state at the current input version in one transaction.
    pub fn served_transaction<T, F>(&mut self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&mut ServedTxn<'_>) -> Result<T, StoreError>,
    {
        self.write_txn(|store| {
            let txn: &Connection = &store.read.conn;
            let version = store.read.input_version()?;
            f(&mut ServedTxn { txn, version })
        })
    }
}
