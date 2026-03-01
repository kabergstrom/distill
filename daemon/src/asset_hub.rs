use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use async_channel::Sender;
use distill_core::{utils, AssetRef, AssetUuid};
use distill_importer::AssetMetadata;
use distill_schema::{
    build_asset_metadata_message,
    data::{
        self, asset_change_log_entry,
        asset_metadata::{self, latest_artifact},
    },
};
use rusqlite::Connection;

use crate::{
    db::{queries, OwnedMessageReader, RwTransaction},
    error::Result,
};

const TABLE_ASSET_METADATA: &str = "asset_metadata";

pub type ListenerID = u64;

pub struct AssetHub {
    id_gen: AtomicU64,
    listeners: Mutex<HashMap<ListenerID, Sender<AssetBatchEvent>>>,
}

struct AssetContentUpdateEvent {
    id: AssetUuid,
    import_hash: Option<Vec<u8>>,
    build_dep_hash: Option<Vec<u8>>,
}

enum ChangeEvent {
    ContentUpdate(AssetContentUpdateEvent),
    Remove(AssetUuid),
    PathRemove(PathBuf),
    PathUpdate(PathBuf),
}

#[derive(Debug)]
pub enum AssetBatchEvent {
    Commit,
}

pub struct ChangeBatch {
    content_changes: Vec<AssetUuid>,
    path_events: Vec<ChangeEvent>,
}

impl ChangeBatch {
    pub fn new() -> ChangeBatch {
        ChangeBatch {
            content_changes: Vec::new(),
            path_events: Vec::new(),
        }
    }
}

// Utility function called from add_changes — uses autoincrement.
// Reserves a seq via a placeholder row, then builds the capnp message once with the real seq.
fn add_asset_changelog_entry(
    conn: &Connection,
    change: &ChangeEvent,
) -> Result<()> {
    // Reserve the autoincrement seq with a placeholder
    conn.execute(
        "INSERT INTO asset_changes (value) VALUES (X'')",
        [],
    )?;
    let seq = conn.last_insert_rowid() as u64;

    // Build the capnp message once with the correct seq
    let mut value_builder = capnp::message::Builder::new_default();
    let mut value = value_builder.init_root::<asset_change_log_entry::Builder<'_>>();
    value.reborrow().set_num(seq);
    {
        let value = value.reborrow().init_event();
        match change {
            ChangeEvent::ContentUpdate(evt) => {
                let mut db_evt = value.init_content_update_event();
                db_evt.reborrow().init_id().set_id(&evt.id.0);
                if let Some(ref import_hash) = evt.import_hash {
                    db_evt.reborrow().set_import_hash(import_hash);
                }
                if let Some(ref build_dep_hash) = evt.build_dep_hash {
                    db_evt.reborrow().set_build_dep_hash(build_dep_hash);
                }
            }
            ChangeEvent::Remove(id) => {
                value.init_remove_event().init_id().set_id(&id.0);
            }
            ChangeEvent::PathRemove(path) => {
                value
                    .init_path_remove_event()
                    .set_path(path.to_string_lossy().as_bytes());
            }
            ChangeEvent::PathUpdate(path) => {
                value
                    .init_path_update_event()
                    .set_path(path.to_string_lossy().as_bytes());
            }
        }
    }
    let mut value_bytes = Vec::new();
    capnp::serialize::write_message(&mut value_bytes, &value_builder)
        .expect("capnp: failed to serialize message");

    // Update the placeholder with the real value
    conn.execute(
        "UPDATE asset_changes SET value = ?1 WHERE seq = ?2",
        rusqlite::params![value_bytes, seq as i64],
    )?;

    Ok(())
}

/// Set all build deps for an asset. Replaces existing deps.
fn set_build_deps(conn: &Connection, asset_id: &AssetUuid, deps: &[AssetRef]) -> Result<()> {
    conn.execute(
        "DELETE FROM build_deps WHERE asset_id = ?1",
        rusqlite::params![asset_id.0.as_ref()],
    )?;
    let mut stmt = conn.prepare_cached(
        "INSERT INTO build_deps (asset_id, dep_id) VALUES (?1, ?2)",
    )?;
    for dep in deps {
        if let AssetRef::Uuid(dep_uuid) = dep {
            stmt.execute(rusqlite::params![
                asset_id.0.as_ref(),
                dep_uuid.0.as_ref()
            ])?;
        }
    }
    Ok(())
}

impl AssetHub {
    pub fn new(_db: Arc<crate::db::Database>) -> Result<AssetHub> {
        Ok(AssetHub {
            id_gen: AtomicU64::new(1),
            listeners: Mutex::new(HashMap::new()),
        })
    }

    pub fn get_all_asset_metadata(
        &self,
        conn: &Connection,
    ) -> Result<Vec<OwnedMessageReader<asset_metadata::Owned>>> {
        let mut stmt = conn.prepare_cached("SELECT value FROM asset_metadata")?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let value: Vec<u8> = row.get(0)?;
            let reader = capnp::serialize::read_message(
                &mut value.as_slice(),
                distill_schema::default_capnp_reader_options(),
            )?;
            result.push(reader.into_typed::<asset_metadata::Owned>());
        }
        Ok(result)
    }

    pub fn get_asset_metadata(
        &self,
        conn: &Connection,
        id: &AssetUuid,
    ) -> Option<OwnedMessageReader<asset_metadata::Owned>> {
        queries::get_capnp::<asset_metadata::Owned>(conn, TABLE_ASSET_METADATA, &id.0)
            .expect("db: failed to get asset_metadata")
    }

    // Writes the asset metadata to the DB. For any build dependency, we update the
    // build_deps table. The indexed reverse lookup replaces the old blob-based reverse index.
    pub fn update_asset(
        &self,
        txn: &mut RwTransaction,
        metadata: &AssetMetadata,
        source: data::AssetSource,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        let conn = txn.conn();
        let existing_metadata: Option<OwnedMessageReader<asset_metadata::Owned>> =
            queries::get_capnp(conn, TABLE_ASSET_METADATA, &metadata.id.0)?;
        let new_metadata = build_asset_metadata_message::<&[u8; 8]>(metadata, source);
        let mut artifact_changed = true;
        if let Some(artifact_metadata) = &metadata.artifact {
            if let Some(existing_metadata) = existing_metadata {
                let existing_metadata = existing_metadata.get()?;
                let latest_artifact = existing_metadata.get_latest_artifact();
                if let latest_artifact::Artifact(Ok(artifact)) = latest_artifact.which()? {
                    artifact_changed =
                        artifact_metadata.id.0.to_le_bytes() != artifact.get_hash()?;
                }
            }
            // Simply replace all deps for this asset — the indexed table handles reverse lookups
            set_build_deps(
                conn,
                &metadata.id,
                &artifact_metadata.build_deps,
            )?;
        }
        // Insert the asset metadata
        queries::put_capnp(conn, TABLE_ASSET_METADATA, &metadata.id.0, &new_metadata)?;
        txn.dirty = true;
        if artifact_changed {
            change_batch.content_changes.push(metadata.id);
        }
        Ok(())
    }

    pub fn remove_asset(
        &self,
        txn: &mut RwTransaction,
        id: &AssetUuid,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        let conn = txn.conn();
        // Clean up all build deps for this asset
        conn.execute(
            "DELETE FROM build_deps WHERE asset_id = ?1",
            rusqlite::params![id.0.as_ref()],
        )?;
        // Delete this asset's metadata
        if queries::delete(conn, TABLE_ASSET_METADATA, &id.0)? {
            change_batch.content_changes.push(*id);
        }
        txn.dirty = true;
        Ok(())
    }

    // Adds a PathRemove ChangeEvent to the batch
    pub fn remove_path(
        &self,
        _txn: &mut RwTransaction,
        relative_path: &Path,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        change_batch
            .path_events
            .push(ChangeEvent::PathRemove(relative_path.to_path_buf()));
        Ok(())
    }

    // Adds a PathUpdate ChangeEvent to the batch
    pub fn update_path(
        &self,
        _txn: &mut RwTransaction,
        relative_path: &Path,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        change_batch
            .path_events
            .push(ChangeEvent::PathUpdate(relative_path.to_path_buf()));
        Ok(())
    }

    // Deep search of dependency tree to find all affected assets
    pub fn add_changes(
        &self,
        txn: &mut RwTransaction,
        change_batch: ChangeBatch,
    ) -> Result<bool> {
        let conn = txn.conn();
        let seeds = &change_batch.content_changes;
        if !seeds.is_empty() {
            log::info!("{} assets changed content", seeds.len());
        }

        // Find all transitively affected assets via a recursive CTE on build_deps.
        // Seeds are the directly changed assets; the CTE walks downstream dependees.
        let affected_assets: Vec<AssetUuid> = if seeds.is_empty() {
            Vec::new()
        } else {
            let placeholders: String = (1..=seeds.len())
                .map(|i| format!("(?{})", i))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "WITH RECURSIVE affected(id) AS ( \
                     VALUES {} \
                     UNION \
                     SELECT bd.asset_id FROM build_deps bd \
                     JOIN affected a ON bd.dep_id = a.id \
                 ) SELECT DISTINCT id FROM affected",
                placeholders
            );
            let mut stmt = conn.prepare(&sql)?;
            let params: Vec<&[u8]> = seeds.iter().map(|id| id.0.as_ref()).collect();
            let mut rows = stmt.query(rusqlite::params_from_iter(&params))?;
            let mut result = Vec::new();
            while let Some(row) = rows.next()? {
                let id_bytes: Vec<u8> = row.get(0)?;
                if let Some(uuid) = utils::uuid_from_slice(&id_bytes) {
                    result.push(uuid);
                }
            }
            result
        };

        // For each affected asset, find its transitive upstream build dependencies
        // via a recursive CTE, fetch their metadata, and compute a combined hash.
        let mut events = Vec::new();
        for asset in &affected_assets {
            let metadata = self.get_asset_metadata(conn, asset);
            if let Some(metadata) = metadata {
                let metadata = metadata.get()?;

                // Recursive CTE: walk upstream build deps, return metadata blobs sorted by key
                let mut upstream_stmt = conn.prepare_cached(
                    "WITH RECURSIVE upstream(id) AS ( \
                         VALUES (?1) \
                         UNION \
                         SELECT bd.dep_id FROM build_deps bd \
                         JOIN upstream u ON bd.asset_id = u.id \
                     ) SELECT am.key, am.value FROM asset_metadata am \
                       JOIN upstream u ON am.key = u.id \
                       ORDER BY am.key",
                )?;
                let mut dep_rows = upstream_stmt.query(rusqlite::params![asset.0.as_ref()])?;
                let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
                while let Some(row) = dep_rows.next()? {
                    let value: Vec<u8> = row.get(1)?;
                    let reader = capnp::serialize::read_message(
                        &mut value.as_slice(),
                        distill_schema::default_capnp_reader_options(),
                    )?;
                    let dep_metadata = reader.into_typed::<asset_metadata::Owned>();
                    if let latest_artifact::Artifact(Ok(artifact)) =
                        dep_metadata.get()?.get_latest_artifact().which()?
                    {
                        artifact.get_hash()?.hash(&mut hasher);
                    }
                }
                let build_dep_hash = hasher.finish();

                let import_hash = {
                    if let latest_artifact::Artifact(Ok(artifact)) =
                        metadata.get_latest_artifact().which()?
                    {
                        Vec::from(artifact.get_hash()?)
                    } else {
                        Vec::new()
                    }
                };
                events.push(ChangeEvent::ContentUpdate(AssetContentUpdateEvent {
                    id: *asset,
                    import_hash: Some(import_hash),
                    build_dep_hash: Some(Vec::from(&build_dep_hash.to_le_bytes() as &[u8])),
                }));
            } else {
                events.push(ChangeEvent::Remove(*asset));
            }
        }
        if !events.is_empty() {
            log::info!("{} asset events generated", events.len());
        }
        for event in events.iter() {
            add_asset_changelog_entry(conn, event)?;
        }
        for event in change_batch.path_events.iter() {
            add_asset_changelog_entry(conn, event)?;
        }
        txn.dirty = true;
        Ok(!events.is_empty())
    }

    // Get the key of the last asset change entry
    pub fn get_latest_asset_change(
        &self,
        conn: &Connection,
    ) -> Result<u64> {
        let mut stmt = conn.prepare_cached(
            "SELECT COALESCE(MAX(seq), 0) FROM asset_changes",
        )?;
        let seq: u64 = stmt.query_row([], |row| row.get(0))?;
        Ok(seq)
    }

    pub fn get_asset_changes(
        &self,
        conn: &Connection,
        start: u64,
        count: usize,
    ) -> Result<Vec<(u64, OwnedMessageReader<asset_change_log_entry::Owned>)>> {
        let limit = if count == 0 {
            i64::MAX
        } else {
            count as i64
        };
        let mut stmt = conn.prepare_cached(
            "SELECT seq, value FROM asset_changes WHERE seq >= ?1 ORDER BY seq LIMIT ?2",
        )?;
        let mut rows = stmt.query(rusqlite::params![start as i64, limit])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let seq: u64 = row.get(0)?;
            let value: Vec<u8> = row.get(1)?;
            let reader = capnp::serialize::read_message(
                &mut value.as_slice(),
                distill_schema::default_capnp_reader_options(),
            )?;
            result.push((seq, reader.into_typed::<asset_change_log_entry::Owned>()));
        }
        Ok(result)
    }

    pub fn notify_listeners(&self) {
        let listeners = &mut *self.listeners.lock().unwrap();
        let mut to_remove = Vec::new();
        for (id, listener) in listeners.iter_mut() {
            if listener.try_send(AssetBatchEvent::Commit).is_err() {
                to_remove.push(*id);
            }
        }
        for id in to_remove {
            listeners.remove(&id);
        }
    }

    pub fn register_listener(&self, listener: Sender<AssetBatchEvent>) -> ListenerID {
        let id = self.id_gen.fetch_add(1, Ordering::Relaxed);
        self.listeners.lock().unwrap().insert(id, listener);
        id
    }

    pub fn drop_listener(&self, listener: ListenerID) -> Option<Sender<AssetBatchEvent>> {
        self.listeners.lock().unwrap().remove(&listener)
    }
}
