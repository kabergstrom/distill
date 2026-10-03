//! Served RPC state (LOCKLESS.md §2.2): what the RPC front ends answer
//! from, as current-state rows.
//!
//! The namespace itself is the ordinary `bundles` / `assets` / `asset_tags`
//! / `schemas` / `path_index` / `derived_outputs` rows. This module adds the
//! served-only facts next to them: explicit resolutions, the change log that
//! subscriptions and reconnect fences read, the RPC target generations, the
//! published pipeline diagnostic, and the typed load
//! edges of stored artifacts.
//!
//! A snapshot is a read transaction over these tables
//! ([`StoreReader::begin_snapshot`]); nothing here is versioned.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use rusqlite::{Connection, OptionalExtension};

use crate::bundles::{blob16, blob32};
use crate::db::{meta_get_blob, meta_get_u64, meta_set_blob, meta_set_u64, InputTxn, StoreReader};
use crate::error::StoreError;
use crate::state::{InputVersion, SnapshotStamp};

/// `store_meta` key of the published pipeline diagnostic.
pub const SERVED_PIPELINE: &str = "served_pipeline";
/// `store_meta` key of the staged restart-required configuration keys.
pub const SERVED_RESTART_KEYS: &str = "served_restart_keys";

const RPC_PROTOCOL_EPOCH: &str = "rpc_protocol_epoch";
const RPC_PIPELINE_GENERATION: &str = "rpc_pipeline_generation";
const CHANGE_LOG_OLDEST: &str = "change_log_oldest";

/// Encode an RPC authored value (canonical JSON plus its blob table) for
/// `assets.authored_value`.
pub fn encode_authored_value(canonical_value: &[u8], blobs: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        12 + canonical_value.len() + blobs.iter().map(|blob| 8 + blob.len()).sum::<usize>(),
    );
    out.extend_from_slice(&(canonical_value.len() as u64).to_le_bytes());
    out.extend_from_slice(canonical_value);
    out.extend_from_slice(&(blobs.len() as u32).to_le_bytes());
    for blob in blobs {
        out.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        out.extend_from_slice(blob);
    }
    out
}

/// Inverse of [`encode_authored_value`]: `(canonical JSON, blobs)`.
pub fn decode_authored_value(bytes: &[u8]) -> Result<(Vec<u8>, Vec<Vec<u8>>), StoreError> {
    fn corrupt() -> StoreError {
        StoreError::Sqlite(rusqlite::Error::InvalidColumnType(
            0,
            "authored_value".to_owned(),
            rusqlite::types::Type::Blob,
        ))
    }
    fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], StoreError> {
        if bytes.len() < len {
            return Err(corrupt());
        }
        let (head, tail) = bytes.split_at(len);
        *bytes = tail;
        Ok(head)
    }
    fn length(bytes: &mut &[u8]) -> Result<usize, StoreError> {
        let raw = take(bytes, 8)?;
        usize::try_from(u64::from_le_bytes(raw.try_into().expect("8 bytes"))).map_err(|_| corrupt())
    }
    let mut rest = bytes;
    let json_len = length(&mut rest)?;
    let json = take(&mut rest, json_len)?.to_vec();
    let count = u32::from_le_bytes(take(&mut rest, 4)?.try_into().expect("4 bytes"));
    let mut blobs = Vec::with_capacity(count.min(4096) as usize);
    for _ in 0..count {
        let len = length(&mut rest)?;
        blobs.push(take(&mut rest, len)?.to_vec());
    }
    if !rest.is_empty() {
        return Err(corrupt());
    }
    Ok((json, blobs))
}

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

/// One served authoring entry with its logical schema and authored value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedEntry {
    pub meta: ServedEntryMeta,
    pub schema_json: String,
    /// [`encode_authored_value`] bytes.
    pub authored_value: Vec<u8>,
}

/// An explicit `asset_resolutions` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionRow {
    Missing,
    Built(ContentHash),
    /// The RPC layer's encoded drift input.
    Drifted(Vec<u8>),
    Failed(String),
    Deleted(InputVersion),
}

/// One served derived-output row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedOutputRow {
    pub parent: AssetUuid,
    pub output_key: String,
    pub terminal_type: TypeUuid,
}

/// One `change_log` payload. Reason and state codes belong to the RPC layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// A published asset delta.
    Asset { asset: AssetUuid, state: u8 },
    /// A published path delta.
    Path { path: String },
    /// Every connection must reconnect.
    ReconnectAll { reason: u8 },
    /// Connections bound to `target` must reconnect.
    ReconnectTarget { target: String, reason: u8 },
    /// The staged restart-required key set changed.
    RestartRequired { keys: Vec<String> },
}

const CHANGE_ASSET: i64 = 1;
const CHANGE_PATH: i64 = 2;
const CHANGE_RECONNECT_ALL: i64 = 3;
const CHANGE_RECONNECT_TARGET: i64 = 4;
const CHANGE_RESTART: i64 = 5;

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
    pub generation: u64,
}

/// A connection's view of every reconnect fence at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcFences {
    pub protocol_epoch: Option<u32>,
    pub pipeline_generation: u64,
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
        // The first read establishes the WAL read mark.
        let stamp = match meta_get_u64(&self.conn, "input_version") {
            Ok(version) => SnapshotStamp {
                instance: self.instance_id(),
                version: InputVersion(version.unwrap_or(0)),
            },
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
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
        reader.conn.execute_batch("ROLLBACK")?;
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
        }
    }
}

const SERVED_ENTRY_COLUMNS: &str = "a.asset_uuid, a.bundle_uuid, a.local_id, b.path, a.type_uuid,
     a.terminal_type, a.logical_hash, a.authoring_only";
macro_rules! served_entry_where {
    () => {
        "a.authored_value IS NOT NULL AND a.terminal_type IS NOT NULL
       AND a.logical_hash IS NOT NULL AND b.poison IS NULL"
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
    pub fn served_entry_meta(&self, asset: AssetUuid) -> Result<Option<ServedEntryMeta>, StoreError> {
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

    /// One served authoring entry with schema and value.
    pub fn served_entry(&self, asset: AssetUuid) -> Result<Option<ServedEntry>, StoreError> {
        let Some(meta) = self.served_entry_meta(asset)? else {
            return Ok(None);
        };
        let (schema_json, authored_value) = self.conn.query_row(
            "SELECT s.schema_json, a.authored_value
             FROM assets a JOIN schemas s ON s.logical_hash = a.logical_hash
             WHERE a.asset_uuid = ?1",
            [asset.0.as_slice()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?;
        Ok(Some(ServedEntry {
            meta,
            schema_json,
            authored_value,
        }))
    }

    fn asset_tag_map(
        &self,
        asset: AssetUuid,
    ) -> Result<BTreeMap<String, Option<String>>, StoreError> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT tag, value FROM asset_tags WHERE asset_uuid = ?1 ORDER BY tag")?;
        let rows = statement.query_map([asset.0.as_slice()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The explicit resolution row of `asset`, if any.
    pub fn asset_resolution(&self, asset: AssetUuid) -> Result<Option<ResolutionRow>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT kind, content_hash, detail, deleted_version
             FROM asset_resolutions WHERE asset_uuid = ?1",
        )?;
        let row = statement
            .query_row([asset.0.as_slice()], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })
            .optional()?;
        let Some((kind, hash, detail, deleted)) = row else {
            return Ok(None);
        };
        let corrupt = || {
            StoreError::Sqlite(rusqlite::Error::InvalidColumnType(
                0,
                "asset_resolutions".to_owned(),
                rusqlite::types::Type::Blob,
            ))
        };
        Ok(Some(match kind {
            0 => ResolutionRow::Missing,
            1 => ResolutionRow::Built(ContentHash(blob32(hash.ok_or_else(corrupt)?))),
            2 => ResolutionRow::Drifted(detail.ok_or_else(corrupt)?),
            3 => ResolutionRow::Failed(
                String::from_utf8(detail.ok_or_else(corrupt)?).map_err(|_| corrupt())?,
            ),
            4 => ResolutionRow::Deleted(InputVersion(deleted.ok_or_else(corrupt)? as u64)),
            _ => return Err(corrupt()),
        }))
    }

    /// One served derived-output row.
    pub fn served_derived_output(
        &self,
        child: AssetUuid,
    ) -> Result<Option<DerivedOutputRow>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT parent_uuid, output_key, terminal_type FROM derived_outputs
             WHERE child_uuid = ?1 AND terminal_type IS NOT NULL",
        )?;
        Ok(statement
            .query_row([child.0.as_slice()], |row| {
                Ok(DerivedOutputRow {
                    parent: AssetUuid(blob16(row.get(0)?)),
                    output_key: row.get(1)?,
                    terminal_type: TypeUuid(blob16(row.get(2)?)),
                })
            })
            .optional()?)
    }

    /// Every asset a logical path names; more than one is an ambiguity.
    pub fn served_path_candidates(&self, path: &str) -> Result<BTreeSet<AssetUuid>, StoreError> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT asset_uuid FROM path_index WHERE path = ?1")?;
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

    /// Every logical path that names `asset`.
    pub fn served_paths_of(&self, asset: AssetUuid) -> Result<BTreeSet<String>, StoreError> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT path FROM path_index WHERE asset_uuid = ?1")?;
        let rows = statement.query_map([asset.0.as_slice()], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<BTreeSet<_>, _>>()
            .map_err(StoreError::from)
    }

    /// Served assets whose tag index is poisoned (pending or failed), with
    /// the owning bundle.
    pub fn tag_poisoned_assets(&self) -> Result<Vec<(AssetUuid, BundleUuid)>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT i.asset_uuid, a.bundle_uuid
             FROM asset_tag_index i JOIN assets a ON a.asset_uuid = i.asset_uuid
             WHERE i.poison IS NOT NULL ORDER BY i.asset_uuid",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                AssetUuid(blob16(row.get(0)?)),
                BundleUuid(blob16(row.get(1)?)),
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
    }

    /// A served diagnostic blob ([`SERVED_PIPELINE`], ...).
    pub fn served_blob(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        meta_get_blob(&self.conn, key)
    }

    /// The reconnect fences every connection records at connect time.
    pub fn rpc_fences(&self) -> Result<RpcFences, StoreError> {
        Ok(RpcFences {
            protocol_epoch: meta_get_u64(&self.conn, RPC_PROTOCOL_EPOCH)?.map(|epoch| epoch as u32),
            pipeline_generation: meta_get_u64(&self.conn, RPC_PIPELINE_GENERATION)?.unwrap_or(0),
        })
    }

    pub fn rpc_targets(&self) -> Result<Vec<RpcTargetRow>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT name, definition_hash, generation FROM rpc_targets ORDER BY name",
        )?;
        let rows = statement.query_map([], rpc_target_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
    }

    pub fn rpc_target(&self, name: &str) -> Result<Option<RpcTargetRow>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT name, definition_hash, generation FROM rpc_targets WHERE name = ?1",
        )?;
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
            "SELECT seq, version, kind, asset_uuid, state, subject, detail
             FROM change_log WHERE seq > ?1 ORDER BY seq",
        )?;
        let rows = statement.query_map([seq], change_entry_row)?;
        rows.collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
    }

    /// Published asset and path deltas with `since < version <= upto`,
    /// ordered by version then sequence.
    pub fn change_log_history(
        &self,
        since: InputVersion,
        upto: InputVersion,
    ) -> Result<Vec<ChangeEntry>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT seq, version, kind, asset_uuid, state, subject, detail
             FROM change_log WHERE version > ?1 AND version <= ?2 AND kind IN (1, 2)
             ORDER BY version, seq",
        )?;
        let rows = statement.query_map([since.0 as i64, upto.0 as i64], change_entry_row)?;
        rows.collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
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
        rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
    }

    /// Whether the CAS indexes this hash.
    pub fn cas_contains(&self, hash: &[u8; 32]) -> Result<bool, StoreError> {
        Ok(self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM cas_extents WHERE content_hash = ?1)",
            [hash.as_slice()],
            |row| row.get(0),
        )?)
    }
}

fn rpc_target_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RpcTargetRow> {
    Ok(RpcTargetRow {
        name: row.get(0)?,
        definition_hash: blob32(row.get(1)?),
        generation: row.get::<_, i64>(2)? as u64,
    })
}

fn change_entry_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<ChangeEntry, StoreError>> {
    let seq: i64 = row.get(0)?;
    let version = InputVersion(row.get::<_, i64>(1)? as u64);
    let kind: i64 = row.get(2)?;
    let asset: Option<Vec<u8>> = row.get(3)?;
    let state: Option<i64> = row.get(4)?;
    let subject: Option<String> = row.get(5)?;
    let detail: Option<Vec<u8>> = row.get(6)?;
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
        (CHANGE_RECONNECT_ALL, _, Some(reason), _) => Change::ReconnectAll {
            reason: reason as u8,
        },
        (CHANGE_RECONNECT_TARGET, _, Some(reason), Some(target)) => Change::ReconnectTarget {
            target,
            reason: reason as u8,
        },
        (CHANGE_RESTART, _, _, _) => {
            let keys = match detail {
                Some(bytes) => match decode_keys(&bytes) {
                    Some(keys) => keys,
                    None => return Ok(Err(corrupt())),
                },
                None => Vec::new(),
            };
            Change::RestartRequired { keys }
        }
        _ => return Ok(Err(corrupt())),
    };
    Ok(Ok(ChangeEntry {
        seq,
        version,
        change,
    }))
}

/// Encode a sorted key list (restart-required keys) as NUL-separated UTF-8.
pub fn encode_keys(keys: &[String]) -> Vec<u8> {
    keys.join("\0").into_bytes()
}

/// Inverse of [`encode_keys`].
pub fn decode_keys(bytes: &[u8]) -> Option<Vec<String>> {
    if bytes.is_empty() {
        return Some(Vec::new());
    }
    let text = std::str::from_utf8(bytes).ok()?;
    Some(text.split('\0').map(str::to_owned).collect())
}

/// Served-state writes shared by input transactions and served-only
/// transactions (those that change fences or diagnostics without a new
/// input version).
pub trait ServedWrite {
    #[doc(hidden)]
    fn served_conn(&self) -> &Connection;

    /// The input version this transaction's change-log rows belong to: the
    /// version being published, or the current one for served-only writes.
    fn change_version(&self) -> InputVersion;

    /// A served diagnostic blob as this transaction sees it.
    fn txn_served_blob(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        meta_get_blob(self.served_conn(), key)
    }

    /// The configuration state as this transaction sees it.
    fn txn_configuration_state(&self) -> Result<crate::state::ConfigurationState, StoreError> {
        crate::config::read_configuration_state(self.served_conn())
    }

    /// The reconnect fences as this transaction sees them.
    fn txn_rpc_fences(&self) -> Result<RpcFences, StoreError> {
        let conn = self.served_conn();
        Ok(RpcFences {
            protocol_epoch: meta_get_u64(conn, RPC_PROTOCOL_EPOCH)?.map(|epoch| epoch as u32),
            pipeline_generation: meta_get_u64(conn, RPC_PIPELINE_GENERATION)?.unwrap_or(0),
        })
    }

    /// Every RPC target row as this transaction sees them.
    fn txn_rpc_targets(&self) -> Result<Vec<RpcTargetRow>, StoreError> {
        let mut statement = self.served_conn().prepare_cached(
            "SELECT name, definition_hash, generation FROM rpc_targets ORDER BY name",
        )?;
        let rows = statement.query_map([], rpc_target_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
    }

    /// Whether `asset` carries a served authoring value.
    fn txn_is_served_asset(&self, asset: AssetUuid) -> Result<bool, StoreError> {
        Ok(self.served_conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM assets WHERE asset_uuid = ?1 AND authored_value IS NOT NULL)",
            [asset.0.as_slice()],
            |row| row.get(0),
        )?)
    }

    /// The tag-index poison text of `asset`, if any.
    fn txn_tag_poison(&self, asset: AssetUuid) -> Result<Option<String>, StoreError> {
        Ok(self
            .served_conn()
            .query_row(
                "SELECT poison FROM asset_tag_index WHERE asset_uuid = ?1",
                [asset.0.as_slice()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    /// Whether `asset` has a resolution row other than `Deleted`: a runtime
    /// entry the served namespace already announced.
    fn txn_has_live_resolution(&self, asset: AssetUuid) -> Result<bool, StoreError> {
        Ok(self.served_conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM asset_resolutions WHERE asset_uuid = ?1 AND kind != 4)",
            [asset.0.as_slice()],
            |row| row.get(0),
        )?)
    }

    /// Replace (`Some`) or remove (`None`) an explicit resolution row.
    fn set_asset_resolution(
        &mut self,
        asset: AssetUuid,
        row: Option<&ResolutionRow>,
    ) -> Result<(), StoreError> {
        let conn = self.served_conn();
        let Some(row) = row else {
            conn.execute(
                "DELETE FROM asset_resolutions WHERE asset_uuid = ?1",
                [asset.0.as_slice()],
            )?;
            return Ok(());
        };
        let (kind, hash, detail, deleted): (i64, Option<Vec<u8>>, Option<Vec<u8>>, Option<i64>) =
            match row {
                ResolutionRow::Missing => (0, None, None, None),
                ResolutionRow::Built(hash) => (1, Some(hash.0.to_vec()), None, None),
                ResolutionRow::Drifted(input) => (2, None, Some(input.clone()), None),
                ResolutionRow::Failed(error) => (3, None, Some(error.clone().into_bytes()), None),
                ResolutionRow::Deleted(version) => (4, None, None, Some(version.0 as i64)),
            };
        conn.execute(
            "INSERT INTO asset_resolutions(asset_uuid, kind, content_hash, detail, deleted_version)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(asset_uuid) DO UPDATE SET
               kind = excluded.kind, content_hash = excluded.content_hash,
               detail = excluded.detail, deleted_version = excluded.deleted_version",
            rusqlite::params![asset.0.as_slice(), kind, hash, detail, deleted],
        )?;
        Ok(())
    }

    /// Append one change-log row at `version`.
    fn append_change(&mut self, version: InputVersion, change: &Change) -> Result<(), StoreError> {
        let conn = self.served_conn();
        let version = version.0 as i64;
        match change {
            Change::Asset { asset, state } => conn.execute(
                "INSERT INTO change_log(version, kind, asset_uuid, state) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![version, CHANGE_ASSET, asset.0.as_slice(), *state as i64],
            )?,
            Change::Path { path } => conn.execute(
                "INSERT INTO change_log(version, kind, subject) VALUES (?1, ?2, ?3)",
                rusqlite::params![version, CHANGE_PATH, path],
            )?,
            Change::ReconnectAll { reason } => conn.execute(
                "INSERT INTO change_log(version, kind, state) VALUES (?1, ?2, ?3)",
                rusqlite::params![version, CHANGE_RECONNECT_ALL, *reason as i64],
            )?,
            Change::ReconnectTarget { target, reason } => conn.execute(
                "INSERT INTO change_log(version, kind, state, subject) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![version, CHANGE_RECONNECT_TARGET, *reason as i64, target],
            )?,
            Change::RestartRequired { keys } => conn.execute(
                "INSERT INTO change_log(version, kind, detail) VALUES (?1, ?2, ?3)",
                rusqlite::params![version, CHANGE_RESTART, encode_keys(keys)],
            )?,
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
            conn.execute("DELETE FROM change_log WHERE version <= ?1", [floor as i64])?;
            meta_set_u64(conn, CHANGE_LOG_OLDEST, floor)?;
        }
        Ok(())
    }

    /// Drop history at or before `oldest` and make it the oldest resumable
    /// cursor.
    fn discard_change_log_before(&mut self, oldest: InputVersion) -> Result<(), StoreError> {
        let conn = self.served_conn();
        conn.execute(
            "DELETE FROM change_log WHERE version <= ?1",
            [oldest.0 as i64],
        )?;
        meta_set_u64(conn, CHANGE_LOG_OLDEST, oldest.0)
    }

    /// Initialise the oldest resumable cursor if the store has none yet.
    fn init_change_log_oldest(&mut self, oldest: InputVersion) -> Result<(), StoreError> {
        let conn = self.served_conn();
        if meta_get_u64(conn, CHANGE_LOG_OLDEST)?.is_none() {
            meta_set_u64(conn, CHANGE_LOG_OLDEST, oldest.0)?;
        }
        Ok(())
    }

    /// Replace (`Some`) or remove (`None`) a served diagnostic blob.
    fn set_served_blob(&mut self, key: &str, value: Option<&[u8]>) -> Result<(), StoreError> {
        let conn = self.served_conn();
        match value {
            Some(value) => meta_set_blob(conn, key, value),
            None => {
                conn.execute("DELETE FROM store_meta WHERE key = ?1", [key])?;
                Ok(())
            }
        }
    }

    fn set_rpc_protocol_epoch(&mut self, epoch: u32) -> Result<(), StoreError> {
        meta_set_u64(self.served_conn(), RPC_PROTOCOL_EPOCH, u64::from(epoch))
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

    /// Install or replace one RPC target. A changed definition advances its
    /// generation. Returns whether it changed.
    fn set_rpc_target(&mut self, name: &str, definition_hash: [u8; 32]) -> Result<bool, StoreError> {
        let conn = self.served_conn();
        let existing = conn
            .query_row(
                "SELECT definition_hash, generation FROM rpc_targets WHERE name = ?1",
                [name],
                |row| Ok((blob32(row.get(0)?), row.get::<_, i64>(1)?)),
            )
            .optional()?;
        match existing {
            Some((hash, _)) if hash == definition_hash => Ok(false),
            Some((_, generation)) => {
                conn.execute(
                    "UPDATE rpc_targets SET definition_hash = ?2, generation = ?3 WHERE name = ?1",
                    rusqlite::params![name, definition_hash.as_slice(), generation + 1],
                )?;
                Ok(true)
            }
            None => {
                conn.execute(
                    "INSERT INTO rpc_targets(name, definition_hash, generation) VALUES (?1, ?2, 0)",
                    rusqlite::params![name, definition_hash.as_slice()],
                )?;
                Ok(false)
            }
        }
    }

    fn remove_rpc_target(&mut self, name: &str) -> Result<bool, StoreError> {
        Ok(self
            .served_conn()
            .execute("DELETE FROM rpc_targets WHERE name = ?1", [name])?
            > 0)
    }

    /// Record an artifact's typed direct load edges. Idempotent; a
    /// different edge set for the same artifact is an error.
    fn record_artifact_load_edges(
        &mut self,
        hash: ContentHash,
        edges: &[(AssetUuid, TypeUuid)],
    ) -> Result<(), StoreError> {
        let conn = self.served_conn();
        let mut statement = conn.prepare_cached(
            "SELECT asset_uuid, expected_terminal FROM artifact_load_edges
             WHERE content_hash = ?1 ORDER BY asset_uuid",
        )?;
        let existing = statement
            .query_map([hash.0.as_slice()], |row| {
                Ok((
                    AssetUuid(blob16(row.get(0)?)),
                    TypeUuid(blob16(row.get(1)?)),
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if !existing.is_empty() {
            let mut sorted = edges.to_vec();
            sorted.sort();
            if existing != sorted {
                return Err(StoreError::InvalidConfiguration {
                    error: format!("artifact {hash:?} already has different load edges"),
                });
            }
            return Ok(());
        }
        for (asset, terminal) in edges {
            conn.execute(
                "INSERT INTO artifact_load_edges(content_hash, asset_uuid, expected_terminal)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![hash.0.as_slice(), asset.0.as_slice(), terminal.0.as_slice()],
            )?;
        }
        Ok(())
    }

    /// Remove one served asset row and its tags. Its tag-index poison and
    /// path rows are separate served state and stay.
    fn remove_served_asset(&mut self, asset: AssetUuid) -> Result<(), StoreError> {
        let conn = self.served_conn();
        for table in ["asset_tags", "assets"] {
            conn.execute(
                &format!("DELETE FROM {table} WHERE asset_uuid = ?1"),
                [asset.0.as_slice()],
            )?;
        }
        Ok(())
    }

    /// Replace the candidates of one logical path. Each candidate gets its
    /// own synthetic root row; no roles are checked (embedded RPC stores).
    fn set_served_path(&mut self, path: &str, candidates: &BTreeSet<AssetUuid>) -> Result<(), StoreError> {
        let conn = self.served_conn();
        conn.execute("DELETE FROM path_index WHERE path = ?1", [path])?;
        for (root, asset) in candidates.iter().enumerate() {
            conn.execute(
                "INSERT INTO path_index(path, root_id, asset_uuid) VALUES (?1, ?2, ?3)",
                rusqlite::params![path, root as i64, asset.0.as_slice()],
            )?;
        }
        Ok(())
    }

    /// Replace (`Some`) or remove (`None`) one served derived output.
    fn set_served_derived_output(
        &mut self,
        child: AssetUuid,
        row: Option<&DerivedOutputRow>,
    ) -> Result<(), StoreError> {
        let conn = self.served_conn();
        match row {
            Some(row) => conn.execute(
                "INSERT INTO derived_outputs(child_uuid, parent_uuid, output_key, terminal_type)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(child_uuid) DO UPDATE SET
                   parent_uuid = excluded.parent_uuid, output_key = excluded.output_key,
                   terminal_type = excluded.terminal_type",
                rusqlite::params![
                    child.0.as_slice(),
                    row.parent.0.as_slice(),
                    row.output_key,
                    row.terminal_type.0.as_slice()
                ],
            )?,
            None => conn.execute(
                "DELETE FROM derived_outputs WHERE child_uuid = ?1",
                [child.0.as_slice()],
            )?,
        };
        Ok(())
    }

    fn clear_served_derived_outputs(&mut self) -> Result<(), StoreError> {
        self.served_conn().execute("DELETE FROM derived_outputs", [])?;
        Ok(())
    }

    /// Replace an asset's tags without touching its tag index.
    fn set_served_tags(
        &mut self,
        asset: AssetUuid,
        tags: &BTreeMap<String, Option<String>>,
    ) -> Result<(), StoreError> {
        let conn = self.served_conn();
        conn.execute(
            "DELETE FROM asset_tags WHERE asset_uuid = ?1",
            [asset.0.as_slice()],
        )?;
        for (tag, value) in tags {
            conn.execute(
                "INSERT INTO asset_tags(asset_uuid, tag, value) VALUES (?1, ?2, ?3)",
                rusqlite::params![asset.0.as_slice(), tag, value],
            )?;
        }
        Ok(())
    }

    /// Mark (`Some(reason)`) or clear (`None`) an asset's tag-index poison.
    fn set_served_tag_poison(
        &mut self,
        asset: AssetUuid,
        poison: Option<&str>,
    ) -> Result<(), StoreError> {
        let conn = self.served_conn();
        match poison {
            Some(poison) => conn.execute(
                "INSERT INTO asset_tag_index(asset_uuid, tag_epoch, trace, poison)
                 VALUES (?1, zeroblob(32), X'', ?2)
                 ON CONFLICT(asset_uuid) DO UPDATE SET poison = excluded.poison",
                rusqlite::params![asset.0.as_slice(), poison],
            )?,
            None => conn.execute(
                "UPDATE asset_tag_index SET poison = NULL WHERE asset_uuid = ?1",
                [asset.0.as_slice()],
            )?,
        };
        Ok(())
    }

    fn clear_served_tag_poisons(&mut self) -> Result<(), StoreError> {
        self.served_conn()
            .execute("UPDATE asset_tag_index SET poison = NULL", [])?;
        Ok(())
    }
}

impl ServedWrite for InputTxn<'_> {
    fn served_conn(&self) -> &Connection {
        &self.txn
    }

    fn change_version(&self) -> InputVersion {
        self.version()
    }
}

/// A transaction that changes only served state (fences, diagnostics,
/// artifact edges) without publishing a new input version.
pub struct ServedTxn<'a> {
    txn: rusqlite::Savepoint<'a>,
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
        &self.txn
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
            let txn = store.read.conn.savepoint()?;
            let version = InputVersion(meta_get_u64(&txn, "input_version")?.unwrap_or(0));
            let mut served = ServedTxn { txn, version };
            let out = f(&mut served)?;
            served.txn.commit()?;
            Ok(out)
        })
    }
}
