use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    str,
    sync::Arc,
    time::Instant,
};

use bincode::config::Options;
use distill_core::{
    utils::{self, canonicalize_path},
    ArtifactId, AssetRef, AssetTypeId, AssetUuid, CompressionType,
};
use distill_importer::{
    ArtifactMetadata, AssetMetadata, BoxedImporter, ImportSource, ImporterContext, SerializedAsset,
};
use distill_schema::{
    build_asset_metadata,
    data::{self, source_metadata},
    parse_db_metadata,
};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver},
    lock::Mutex,
    stream::StreamExt,
};
use log::{debug, error, info};
#[cfg(feature = "rayon")]
use rayon::prelude::*;
use rusqlite::Connection;

use crate::{
    artifact_cache::ArtifactCache,
    asset_hub::{self, AssetHub},
    db::{queries, Database, OwnedMessageReader, RwTransaction},
    daemon::ImporterMap,
    error::{Error, Result},
    file_tracker::{FileState, FileTracker, FileTrackerEvent},
    source_pair_import::{
        self, hash_file, HashedSourcePair, ImportResultMetadata, SourceMetadata, SourcePair,
        SourcePairImport,
    },
};

const TABLE_PATH_TO_METADATA: &str = "path_to_metadata";

pub(crate) struct FileAssetSource {
    hub: Arc<AssetHub>,
    tracker: Arc<FileTracker>,
    db: Arc<Database>,
    artifact_cache: Arc<ArtifactCache>,
    importers: Arc<ImporterMap>,
    importer_contexts: Arc<Vec<Box<dyn ImporterContext>>>,
    runtime: bevy_tasks::IoTaskPool,
}

#[derive(Debug)]
struct AssetImportResultMetadata {
    pub metadata: AssetMetadata,
    pub unresolved_load_refs: Vec<AssetRef>,
    pub unresolved_build_refs: Vec<AssetRef>,
}
struct PairImportResultMetadata<'a> {
    pub import_state: SourcePairImport<'a>,
    pub assets: Vec<AssetImportResultMetadata>,
}

type SerializedAssetVec = SerializedAsset<Vec<u8>>;

// Creates HashedSourcePair from SourcePairs
#[cfg(feature = "parallel_hash")]
fn hash_files<'a, T, I>(pairs: I) -> Vec<Result<HashedSourcePair>>
where
    I: IntoParallelIterator<Item = &'a SourcePair, Iter = T>,
    T: ParallelIterator<Item = &'a SourcePair>,
{
    Vec::from_par_iter(pairs.into_par_iter().map(|s| {
        let mut hashed_pair = HashedSourcePair {
            meta: s.meta.clone(),
            source: s.source.clone(),
            source_hash: None,
            meta_hash: None,
        };
        match s.meta {
            Some(ref state) if state.state == data::FileState::Exists => {
                let (state, hash) = hash_file(state)?;
                hashed_pair.meta = Some(state);
                hashed_pair.meta_hash = hash;
            }
            _ => {}
        };
        match s.source {
            Some(ref state) if state.state == data::FileState::Exists => {
                let (state, hash) = hash_file(state)?;
                hashed_pair.source = Some(state);
                hashed_pair.source_hash = hash;
            }
            _ => {}
        };
        Ok(hashed_pair)
    }))
}

// Creates HashedSourcePair from SourcePairs
#[cfg(not(feature = "parallel_hash"))]
fn hash_files<'a, T, I>(pairs: I) -> Vec<Result<HashedSourcePair>>
where
    I: IntoIterator<Item = &'a SourcePair, IntoIter = T>,
    T: Iterator<Item = &'a SourcePair>,
{
    pairs
        .into_iter()
        .map(|s| {
            let mut hashed_pair = HashedSourcePair {
                meta: s.meta.clone(),
                source: s.source.clone(),
                source_hash: None,
                meta_hash: None,
            };
            match s.meta {
                Some(ref state) if state.state == data::FileState::Exists => {
                    let (state, hash) = hash_file(state)?;
                    hashed_pair.meta = Some(state);
                    hashed_pair.meta_hash = hash;
                }
                _ => {}
            };
            match s.source {
                Some(ref state) if state.state == data::FileState::Exists => {
                    let (state, hash) = hash_file(state)?;
                    hashed_pair.source = Some(state);
                    hashed_pair.source_hash = hash;
                }
                _ => {}
            };
            Ok(hashed_pair)
        })
        .collect()
}

// converts a relative path in one asset file pointing at another asset file to a canonicalized path
// to that file from the root of the asset folder
fn resolve_source_path(abs_source_path: &Path, path: &Path) -> PathBuf {
    let absolute_path = if path.is_relative() {
        // TODO check from root of asset folder as well?
        let mut parent_path = abs_source_path.to_path_buf();
        parent_path.pop();
        parent_path.push(path);
        parent_path
    } else {
        path.to_path_buf()
    };
    canonicalize_path(&absolute_path)
}

impl FileAssetSource {
    pub fn new(
        tracker: &Arc<FileTracker>,
        hub: &Arc<AssetHub>,
        db: &Arc<Database>,
        importers: &Arc<ImporterMap>,
        artifact_cache: &Arc<ArtifactCache>,
        importer_contexts: Arc<Vec<Box<dyn ImporterContext>>>,
    ) -> Result<FileAssetSource> {
        Ok(FileAssetSource {
            tracker: tracker.clone(),
            hub: hub.clone(),
            db: db.clone(),
            artifact_cache: artifact_cache.clone(),
            runtime: bevy_tasks::IoTaskPool(bevy_tasks::TaskPoolBuilder::default().build()),
            importers: importers.clone(),
            importer_contexts,
        })
    }

    // called from process_metadata_changes, inserts the given data, deleting anything that was
    // previously inserted and now no longer needed. Returns a list of all affected assets (deleted
    // and inserted assets)
    fn put_source_metadata(
        &self,
        txn: &mut RwTransaction,
        path: &Path,
        metadata: &SourceMetadata,
        result_metadata: &ImportResultMetadata,
    ) -> Result<Vec<AssetUuid>> {
        let metadata_assets = &result_metadata.assets;
        let mut affected_assets = Vec::new();

        // Parse out the asset UUIDs from capnp message SourceMetadata.assets and the paths from
        // SourceMetadata.pathRefs
        let (assets_to_remove, path_refs_to_remove): (Vec<AssetUuid>, Vec<PathBuf>) = self
            .get_source_metadata(txn.conn(), path)
            .map(|existing| {
                let existing = existing.get().expect("capnp: Failed to read metadata");
                let path_refs = existing
                    .get_path_refs()
                    .expect("capnp: Failed to get path refs")
                    .iter()
                    .map(|r| {
                        PathBuf::from(
                            str::from_utf8(r.expect("cpnp: Failed to read path ref"))
                                .expect("Failed to parse path ref as utf8"),
                        )
                    })
                    .collect();
                let assets = existing.get_assets().expect("capnp: Failed to get assets");

                let asset_ids = assets
                    .iter()
                    .map(|asset| {
                        asset
                            .get_id()
                            .and_then(|id| id.get_id())
                            .map_err(Error::Capnp)
                            .and_then(|slice| {
                                utils::uuid_from_slice(slice).ok_or(Error::UuidLength)
                            })
                            .expect("capnp: Failed to read uuid")
                    })
                    .filter(|id| metadata_assets.iter().all(|a| a.id != *id))
                    .collect();

                (asset_ids, path_refs)
            })
            .unwrap_or_default();

        // Delete all the asset paths that were inserted by the previous metadata
        if !assets_to_remove.is_empty() {
            {
                let placeholders: String = (1..=assets_to_remove.len())
                    .map(|i| format!("?{}", i))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "DELETE FROM asset_id_to_path WHERE key IN ({})",
                    placeholders
                );
                let params: Vec<&[u8]> = assets_to_remove.iter().map(|id| id.0.as_ref()).collect();
                txn.conn().prepare(&sql)
                    .expect("db: Failed to prepare batch delete")
                    .execute(rusqlite::params_from_iter(&params))
                    .expect("db: Failed to batch delete from asset_id_to_path");
            }
            txn.dirty = true;
            for asset in assets_to_remove {
                debug!("removing deleted asset {:?}", asset);
                affected_assets.push(asset);
            }
        }

        let mut deduped_path_refs = HashSet::new();

        // Iterate through all the assets in the new metadata
        for asset in metadata_assets.iter() {
            debug!("updating asset {:?}", asset.id);

            match self.get_asset_path(txn.conn(), &asset.id) {
                // This asset ID is already located somewhere else
                Some(ref old_path) if old_path != path => {
                    error!(
                        "asset {:?} already in DB with path {} expected {}",
                        asset.id,
                        old_path.to_string_lossy(),
                        path.to_string_lossy(),
                    );
                }
                Some(_) => {} // asset already in DB with correct path
                _ => self.put_asset_path(txn, &asset.id, path),
            }

            affected_assets.push(asset.id);
        }

        // Delete all the path refs that were inserted by the previous metadata
        for path_ref in path_refs_to_remove {
            self.remove_path_ref(txn, path, &path_ref);
        }

        // Find every path that exists in the new metadata (load_deps, build_deps). (Don't include
        // references by UUID)
        let new_path_refs: Vec<_> = metadata_assets
            .iter()
            .filter_map(|x| x.artifact.as_ref())
            .flat_map(|x| &x.load_deps)
            .chain(
                metadata_assets
                    .iter()
                    .filter_map(|x| x.artifact.as_ref())
                    .flat_map(|x| &x.build_deps),
            )
            .filter_map(|x| {
                if let AssetRef::Path(path) = x {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        // Insert all of them (avoiding duplicate insertions)
        for path_ref in new_path_refs {
            if deduped_path_refs.insert(path_ref.clone()) {
                self.add_path_ref(txn, path, path_ref);
            }
        }

        // Save the final SourceMetadata capnp object to the DB
        let mut value_builder = capnp::message::Builder::new_default();
        {
            let mut value = value_builder.init_root::<source_metadata::Builder<'_>>();

            {
                value.set_importer_version(result_metadata.importer_version);
                value.set_importer_type(&result_metadata.importer_type.0);
                value.set_importer_state_type(&metadata.importer_state.uuid());
                let mut state_buf = Vec::new();
                bincode::serialize_into(&mut state_buf, &metadata.importer_state)?;
                value.set_importer_state(&state_buf);
                value.set_importer_options_type(&metadata.importer_options.uuid());
                let mut options_buf = Vec::new();
                bincode::serialize_into(&mut options_buf, &metadata.importer_options)?;
                value.set_importer_options(&options_buf);
                let hash_bytes = result_metadata
                    .import_hash
                    .expect("import hash not present")
                    .to_le_bytes();
                value.set_import_hash(&hash_bytes);
            }
            let mut path_refs = value
                .reborrow()
                .init_path_refs(deduped_path_refs.len() as u32);
            for (idx, path_ref) in deduped_path_refs.into_iter().enumerate() {
                path_refs
                    .reborrow()
                    .set(idx as u32, path_ref.to_string_lossy().as_bytes());
            }

            let mut assets = value.reborrow().init_assets(metadata_assets.len() as u32);
            for (idx, asset) in metadata_assets.iter().enumerate() {
                let mut builder = assets.reborrow().get(idx as u32);
                build_asset_metadata(asset, &mut builder, data::AssetSource::File);
            }
            let assets_with_pipelines: Vec<&AssetMetadata> = metadata_assets
                .iter()
                .filter(|a| a.build_pipeline.is_some())
                .collect();

            let mut build_pipelines = value
                .reborrow()
                .init_build_pipelines(assets_with_pipelines.len() as u32);

            for (idx, asset) in assets_with_pipelines.iter().enumerate() {
                build_pipelines
                    .reborrow()
                    .get(idx as u32)
                    .init_key()
                    .set_id(&asset.id.0);
                build_pipelines
                    .reborrow()
                    .get(idx as u32)
                    .init_value()
                    .set_id(&asset.build_pipeline.unwrap().0);
            }
        }

        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        queries::put_capnp(txn.conn(), TABLE_PATH_TO_METADATA, key, &value_builder)
            .expect("db: Failed to put value to path_to_metadata");
        txn.dirty = true;

        Ok(affected_assets)
    }

    pub fn get_source_metadata(
        &self,
        conn: &Connection,
        path: &Path,
    ) -> Option<OwnedMessageReader<source_metadata::Owned>> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        queries::get_capnp::<source_metadata::Owned>(conn, TABLE_PATH_TO_METADATA, key)
            .expect("db: Failed to get source metadata from path_to_metadata table")
    }

    #[allow(dead_code)]
    pub fn iter_source_metadata(
        &self,
        conn: &Connection,
    ) -> Vec<(PathBuf, OwnedMessageReader<source_metadata::Owned>)> {
        queries::iter_all::<source_metadata::Owned>(conn, TABLE_PATH_TO_METADATA)
            .expect("db: Failed to iterate path_to_metadata table")
            .into_iter()
            .filter_map(|(key, value)| {
                let path = PathBuf::from(str::from_utf8(&key).ok()?);
                Some((path, value))
            })
            .collect()
    }

    fn delete_source_metadata(&self, txn: &mut RwTransaction, path: &Path) -> Vec<AssetUuid> {
        // Get all assets that we know to be located at the given path
        let to_remove: Vec<AssetUuid> = self
            .get_source_metadata(txn.conn(), path)
            .map(|existing| {
                let metadata = existing.get().expect("capnp: Failed to read metadata");
                metadata
                    .get_assets()
                    .expect("capnp: Failed to get assets")
                    .iter()
                    .map(|asset| {
                        asset
                            .get_id()
                            .and_then(|id| id.get_id())
                            .map_err(Error::Capnp)
                            .and_then(|slice| {
                                utils::uuid_from_slice(slice).ok_or(Error::UuidLength)
                            })
                            .expect("capnp: Failed to read uuid")
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Batch-delete the path/asset associations
        if !to_remove.is_empty() {
            for asset in to_remove.iter() {
                debug!("remove asset {:?}", asset);
            }
            {
                let placeholders: String = (1..=to_remove.len())
                    .map(|i| format!("?{}", i))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "DELETE FROM asset_id_to_path WHERE key IN ({})",
                    placeholders
                );
                let params: Vec<&[u8]> = to_remove.iter().map(|id| id.0.as_ref()).collect();
                txn.conn().prepare(&sql)
                    .expect("db: Failed to prepare batch delete")
                    .execute(rusqlite::params_from_iter(&params))
                    .expect("db: Failed to batch delete from asset_id_to_path");
            }
            txn.dirty = true;
        }

        // Delete the path/source metadata association
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        queries::delete(txn.conn(), TABLE_PATH_TO_METADATA, key)
            .expect("db: Failed to delete metadata from path_to_metadata table");
        txn.dirty = true;
        to_remove
    }

    // Given a "current directory" and a reference (that might be a relative path), return the
    // appropriate imported asset ID.
    pub fn resolve_asset_ref(
        &self,
        conn: &Connection,
        source_path: &Path,
        asset_ref: &AssetRef,
    ) -> Option<AssetUuid> {
        match asset_ref {
            AssetRef::Uuid(uuid) => Some(*uuid),
            AssetRef::Path(path) => {
                let canon_path = resolve_source_path(source_path, path);
                if let Some(metadata) = self.get_source_metadata(conn, &canon_path) {
                    let assets = metadata
                        .get()
                        .map_err(crate::error::Error::Capnp)
                        .and_then(|metadata| {
                            let mut assets = Vec::new();
                            for asset in metadata.get_assets()? {
                                assets.push(
                                    utils::uuid_from_slice(asset.get_id()?.get_id()?)
                                        .ok_or(Error::UuidLength)?,
                                );
                            }
                            Ok(assets)
                        })
                        .expect("capnp: failed to read asset list");
                    // Resolve the path into asset with index 0, if it exists
                    assets.into_iter().next()
                } else {
                    log::error!(
                        "Failed to resolve path {:?} at {:?}: could not find metadata for file",
                        canon_path.to_string_lossy(),
                        source_path.to_string_lossy(),
                    );
                    None
                }
            }
        }
    }

    // Associate a path with an asset ID
    fn put_asset_path(
        &self,
        txn: &mut RwTransaction,
        asset_id: &AssetUuid,
        path: &Path,
    ) {
        txn.conn()
            .prepare_cached("INSERT OR REPLACE INTO asset_id_to_path (key, path) VALUES (?1, ?2)")
            .expect("db: Failed to prepare asset_id_to_path insert")
            .execute(rusqlite::params![&asset_id.0[..], path.to_string_lossy().as_ref()])
            .expect("db: Failed to put asset path to asset_id_to_path table");
        txn.dirty = true;
    }

    // Given an asset ID, return the path to the source data that generated it
    pub fn get_asset_path(
        &self,
        conn: &Connection,
        asset_id: &AssetUuid,
    ) -> Option<PathBuf> {
        let mut stmt = conn
            .prepare_cached("SELECT path FROM asset_id_to_path WHERE key = ?1")
            .expect("db: Failed to prepare asset_id_to_path select");
        stmt.query_row(rusqlite::params![&asset_id.0[..]], |row| {
            let path: String = row.get(0)?;
            Ok(PathBuf::from(path))
        })
        .ok()
    }

    fn add_path_ref(
        &self,
        txn: &mut RwTransaction,
        source: &Path,
        path_ref: &Path,
    ) -> bool {
        let path_ref = resolve_source_path(source, path_ref);
        let source_str = source.to_string_lossy();
        let ref_str = path_ref.to_string_lossy();
        let result = txn.conn().execute(
            "INSERT OR IGNORE INTO path_refs (source_path, ref_path) VALUES (?1, ?2)",
            rusqlite::params![source_str.as_ref(), ref_str.as_ref()],
        ).expect("db: failed to insert path ref");
        if result > 0 {
            txn.dirty = true;
            true
        } else {
            false
        }
    }

    pub fn get_path_refs(
        &self,
        conn: &Connection,
        path: &Path,
    ) -> Vec<PathBuf> {
        let key_str = path.to_string_lossy();
        let mut stmt = conn
            .prepare_cached("SELECT source_path FROM path_refs WHERE ref_path = ?1")
            .expect("db: failed to prepare path_refs query");
        let mut rows = stmt
            .query(rusqlite::params![key_str.as_ref()])
            .expect("db: failed to query path_refs");
        let mut result = Vec::new();
        while let Some(row) = rows.next().expect("db: failed to iterate path_refs") {
            let source: String = row.get(0).expect("db: failed to get source_path");
            result.push(PathBuf::from(source));
        }
        result
    }

    fn remove_path_ref(&self, txn: &mut RwTransaction, source: &Path, path_ref: &Path) -> bool {
        let path_ref = resolve_source_path(source, path_ref);
        let source_str = source.to_string_lossy();
        let ref_str = path_ref.to_string_lossy();
        let count = txn.conn().execute(
            "DELETE FROM path_refs WHERE source_path = ?1 AND ref_path = ?2",
            rusqlite::params![source_str.as_ref(), ref_str.as_ref()],
        ).expect("db: failed to delete path ref");
        if count > 0 {
            txn.dirty = true;
            true
        } else {
            false
        }
    }

    // Given an asset ID, reimports the source file from which it was generated
    pub async fn regenerate_import_artifact(
        &self,
        conn: &Connection,
        id: &AssetUuid,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<(u64, SerializedAssetVec)> {
        // Find the path the asset is located at
        log::trace!("regenerate_import_artifact id {:?}", id);
        let path = self
            .get_asset_path(conn, id)
            .ok_or_else(|| Error::Custom("Could not find asset".to_string()))?;

        log::trace!("path of id {:?} is {:?}", id, path);
        let cache = DBSourceMetadataCache {
            conn,
            file_asset_source: self,
            owner_thread: std::thread::current().id(),
        };

        // The asset could be one of many coming from the same source file, we need to re-import
        // the full source file
        let mut import = SourcePairImport::new(path.clone());
        import.set_importer_from_map(&self.importers);
        import.set_importer_contexts(&self.importer_contexts);
        import.generate_source_metadata(&cache, ImportSource::File(&path));
        import.hash_source();

        log::trace!("Importing source for {:?}", id);
        let imported_assets = import.import_source(scratch_buf).await?;
        if let Some(import_op) = imported_assets.import_op {
            // TODO store reported errors and warnings in metadata
            for error in &import_op.errors {
                log::error!("Import erorr {:?}: {:?}", path, error);
            }
            for warning in &import_op.warnings {
                log::warn!("Import warning {:?}: {:?}", path, warning);
            }
        }
        let mut context_set = imported_assets
            .importer_context_set
            .expect("importer context set required");
        let mut this_asset = None;
        let mut rw_txn = self.artifact_cache.rw_txn().await?;

        // Get all the imported asset IDs from the source file
        let asset_ids = imported_assets
            .assets
            .iter()
            .map(|a| a.metadata.id)
            .collect::<Vec<_>>();
        for asset in imported_assets.assets {
            // Get all the UUID build deps
            let mut build_deps = asset
                .metadata
                .artifact
                .as_ref()
                .map(|artifact| {
                    artifact
                        .build_deps
                        .iter()
                        .filter(|dep| !dep.is_path())
                        .map(|dep| dep.expect_uuid())
                        .cloned()
                        .collect()
                })
                .unwrap_or_else(Vec::new);
            // Get all the UUID load deps
            let mut load_deps = asset
                .metadata
                .artifact
                .as_ref()
                .map(|artifact| {
                    artifact
                        .load_deps
                        .iter()
                        .filter(|dep| !dep.is_path())
                        .map(|dep| dep.expect_uuid())
                        .cloned()
                        .collect()
                })
                .unwrap_or_else(Vec::new);

            // Get all the unresolved build/load refs (currently those based on paths)
            for unresolved_ref in asset.unresolved_build_refs.iter() {
                if let Some(uuid) = self.resolve_asset_ref(conn, &path, unresolved_ref) {
                    context_set.resolve_ref(unresolved_ref, uuid);
                    build_deps.push(uuid);
                }
            }
            for unresolved_ref in asset.unresolved_load_refs.iter() {
                if let Some(uuid) = self.resolve_asset_ref(conn, &path, unresolved_ref) {
                    context_set.resolve_ref(unresolved_ref, uuid);
                    load_deps.push(uuid);
                }
            }

            let import_hash = import
                .import_hash()
                .expect("Invalid: Import path should exist");

            context_set.begin_serialize_asset(asset.metadata.id);
            let asset_id = asset.metadata.id;

            let pair: Result<(u64, SerializedAssetVec)> = context_set
                .scope(async {
                    // Hash the asset/artifact
                    let hash = utils::calc_import_artifact_hash(
                        &asset.metadata.id,
                        import_hash,
                        load_deps.iter().chain(build_deps.iter()),
                    );
                    // Create a serializable form of the asset
                    let serialized_asset = crate::serialized_asset::create(
                        hash,
                        asset.metadata.id,
                        build_deps.into_iter().map(AssetRef::Uuid).collect(),
                        load_deps.into_iter().map(AssetRef::Uuid).collect(),
                        &*asset
                            .asset
                            .expect("expected asset obj when regenerating artifact"),
                        CompressionType::None,
                        scratch_buf,
                    )?;
                    // Store it in the cache
                    self.artifact_cache.insert(&mut rw_txn, &serialized_asset);
                    Ok((hash, serialized_asset))
                })
                .await;
            let pair = pair?;

            // If this is the asset we were looking for, set this_asset so we can return it later
            if asset_id == *id {
                this_asset = Some(pair);
            }

            context_set.end_serialize_asset(asset_id);
        }
        rw_txn.commit()?;
        if let Some(asset) = this_asset {
            Ok(asset)
        } else {
            Err(Error::Custom(format!(
                "Asset {} does not exist in source file {:?}. Found assets: {:?}",
                *id, path, asset_ids
            )))
        }
    }

    // Replaces any build_deps/load_deps that is unresolved (for example, because it's a path rather
    // than a UUID) with a ref that is UUID-based
    fn resolve_metadata_asset_refs(
        &self,
        conn: &Connection,
        path: &Path,
        asset_import_result: &AssetImportResultMetadata,
        artifact: &mut ArtifactMetadata,
    ) {
        for unresolved_build_ref in asset_import_result.unresolved_build_refs.iter() {
            if let Some(build_ref) = self.resolve_asset_ref(conn, path, unresolved_build_ref) {
                let uuid_ref = AssetRef::Uuid(build_ref);
                if !artifact.build_deps.contains(&uuid_ref) {
                    artifact.build_deps.push(uuid_ref);
                }
                // remove the AssetRef that was resolved
                let ref_idx = artifact
                    .build_deps
                    .iter()
                    .position(|x| x == unresolved_build_ref);
                if let Some(ref_idx) = ref_idx {
                    artifact.build_deps.remove(ref_idx);
                }
            }
        }
        for unresolved_load_ref in asset_import_result.unresolved_load_refs.iter() {
            if let Some(load_ref) = self.resolve_asset_ref(conn, path, unresolved_load_ref) {
                let uuid_ref = AssetRef::Uuid(load_ref);
                if !artifact.load_deps.contains(&uuid_ref) {
                    artifact.load_deps.push(uuid_ref);
                }
                let ref_idx = artifact
                    .load_deps
                    .iter()
                    .position(|x| x == unresolved_load_ref);
                if let Some(ref_idx) = ref_idx {
                    artifact.load_deps.remove(ref_idx);
                }
            }
        }
    }

    // Given the result of importing some assets, update the DB.
    fn process_metadata_changes(
        &self,
        txn: &mut RwTransaction,
        changes: &HashMap<PathBuf, Option<PairImportResultMetadata<'_>>>,
        change_batch: &mut asset_hub::ChangeBatch,
    ) {
        let mut affected_assets = HashMap::new();

        // delete metadata for deleted source pairs
        for (path, _) in changes.iter().filter(|(_, change)| change.is_none()) {
            debug!("deleting metadata for {}", path.to_string_lossy());
            for asset in self.delete_source_metadata(txn, path) {
                affected_assets.entry(asset).or_insert(None);
            }
            if let Some(relative_path) = self.tracker.make_relative_path(path) {
                self.hub
                    .remove_path(txn, &relative_path, change_batch)
                    .unwrap_or_else(|e| {
                        panic!("Failed to remove path {:?} in asset_hub: {:?}", path, e)
                    });
            }
        }

        // update or insert metadata for changed source pairs
        for (path, metadata) in changes.iter().filter(|(_, change)| change.is_some()) {
            if let Some(relative_path) = self.tracker.make_relative_path(path) {
                self.hub
                    .update_path(txn, &relative_path, change_batch)
                    .unwrap_or_else(|e| {
                        panic!("Failed to update path {:?} in asset_hub: {:?}", path, e)
                    });
            }
            let import_state = &metadata.as_ref().unwrap().import_state;
            if import_state.source_metadata().is_none() {
                continue;
            }
            let source_metadata = import_state
                .source_metadata()
                .unwrap_or_else(|| panic!("Change for {:?} has no SourceMetadata", path));
            debug!("imported {}", path.to_string_lossy());

            let result_metadata = ImportResultMetadata::default();
            let result_metadata = if let Some(result_metadata) = import_state.result_metadata() {
                result_metadata
            } else {
                &result_metadata
            };
            let changed_assets = self
                .put_source_metadata(txn, path, source_metadata, result_metadata)
                .expect("Failed to put metadata");

            for asset in changed_assets {
                affected_assets.entry(asset).or_insert(None);
            }

            for asset in &result_metadata.assets {
                affected_assets.insert(asset.id, Some(asset.clone()));
            }
        }

        // resolve unresolved path AssetRefs into UUIDs before updating asset metadata.
        for (path, metadata) in changes.iter().filter(|(_, change)| change.is_some()) {
            let metadata = metadata.as_ref().unwrap();
            for asset in metadata.assets.iter() {
                let asset_metadata = affected_assets
                    .get_mut(&asset.metadata.id)
                    .expect("asset in changes but not in affected_assets")
                    .as_mut()
                    .expect("asset None in affected_assets");
                if let Some(artifact) = asset_metadata.artifact.as_mut() {
                    self.resolve_metadata_asset_refs(txn.conn(), path, asset, artifact);
                }
            }
        }

        // push removals and updates into AssetHub database
        for (asset, maybe_metadata) in affected_assets.iter_mut() {
            match self.get_asset_path(txn.conn(), asset) {
                Some(ref path) => {
                    let asset_metadata = maybe_metadata
                        .as_mut()
                        .expect("metadata exists in DB but not in hashmap");
                    let import_hash = changes
                        .get(path)
                        .expect("path in affected set but no change in hashmap")
                        .as_ref()
                        .expect("path changed but no import result present")
                        .import_state
                        .import_hash()
                        .expect("path changed but no import hash present");
                    if let Some(a) = asset_metadata.artifact.as_mut() {
                        a.load_deps = a
                            .load_deps
                            .iter()
                            .filter(|x| x.is_uuid())
                            .cloned()
                            .collect();
                        a.build_deps = a
                            .build_deps
                            .iter()
                            .filter(|x| x.is_uuid())
                            .cloned()
                            .collect();
                        a.load_deps.sort_unstable();
                        a.build_deps.sort_unstable();
                        a.id = ArtifactId(utils::calc_import_artifact_hash(
                            asset,
                            import_hash,
                            a.load_deps
                                .iter()
                                .chain(a.build_deps.iter())
                                .map(|dep| dep.expect_uuid()),
                        ))
                    }

                    self.hub
                        .update_asset(txn, asset_metadata, data::AssetSource::File, change_batch)
                        .expect("hub: Failed to update asset in hub");
                }
                None => {
                    self.hub
                        .remove_asset(txn, asset, change_batch)
                        .expect("hub: Failed to remove asset");
                }
            }
        }

        // update asset hashes for the reverse path refs of all changes
        for (path, _) in changes.iter() {
            let reverse_path_refs = self.get_path_refs(txn.conn(), path);
            for path_ref_source in reverse_path_refs.iter() {
                // First, check if the path has already been processed
                if changes.contains_key(path_ref_source) {
                    continue;
                }
                // Then we look in the database for assets affected by the change
                let cache = DBSourceMetadataCache {
                    conn: txn.conn(),
                    file_asset_source: self,
                    owner_thread: std::thread::current().id(),
                };
                let mut import = SourcePairImport::new(path_ref_source.clone());
                if !import.set_importer_from_map(&self.importers) {
                    log::warn!("failed to set importer from map for path {:?} when updating path ref dependencies", path_ref_source);
                } else {
                    import.generate_source_metadata(&cache, ImportSource::File(&path.clone()));
                    import
                        .get_result_metadata_from_cache(&cache)
                        .expect("error fetching import result metadata from cache");
                    let import_hash = import
                        .result_metadata()
                        .expect("expected result metadata")
                        .import_hash
                        .expect("expected import hash in source metadata");
                    match import.import_result_from_cached_data() {
                        Ok(import_result) => {
                            for mut asset in import_result.assets {
                                let result_metadata = AssetImportResultMetadata {
                                    metadata: asset.metadata.clone(),
                                    unresolved_load_refs: asset.unresolved_load_refs,
                                    unresolved_build_refs: asset.unresolved_build_refs,
                                };
                                if let Some(artifact) = &mut asset.metadata.artifact {
                                    self.resolve_metadata_asset_refs(
                                        txn.conn(),
                                        path_ref_source,
                                        &result_metadata,
                                        artifact,
                                    );
                                    artifact.load_deps = artifact
                                        .load_deps
                                        .iter()
                                        .filter(|x| x.is_uuid())
                                        .cloned()
                                        .collect();
                                    artifact.build_deps = artifact
                                        .build_deps
                                        .iter()
                                        .filter(|x| x.is_uuid())
                                        .cloned()
                                        .collect();
                                    artifact.id = ArtifactId(utils::calc_import_artifact_hash(
                                        &asset.metadata.id,
                                        import_hash,
                                        artifact
                                            .load_deps
                                            .iter()
                                            .chain(artifact.build_deps.iter())
                                            .map(|dep| dep.expect_uuid()),
                                    ));
                                    self.hub
                                        .update_asset(
                                            txn,
                                            &asset.metadata,
                                            data::AssetSource::File,
                                            change_batch,
                                        )
                                        .expect("hub: Failed to update asset in hub");
                                }
                            }
                        }
                        Err(err) => {
                            log::error!("failed to get import result from metadata when updating path ref for asset: {}", err);
                        }
                    }
                }
            }
        }
    }

    // If the file state in the dirty files table matches the data we just imported, we can clear
    // the dirty state.
    fn ack_dirty_file_states(&self, txn: &mut RwTransaction, pair: &HashedSourcePair) {
        let mut skip_ack_dirty = false;

        {
            let check_file_state = |f: &Option<&FileState>| -> bool {
                match f {
                    Some(f) => {
                        let file_state = self.tracker.get_file_state(txn.conn(), &f.path);
                        file_state.map_or(false, |s| s != **f)
                    }
                    None => false,
                }
            };

            skip_ack_dirty |= check_file_state(&pair.source.as_ref());
            skip_ack_dirty |= check_file_state(&pair.meta.as_ref());
        }
        if !skip_ack_dirty {
            if pair.source.is_some() {
                self.tracker
                    .delete_dirty_file_state(txn, pair.source.as_ref().map(|p| &p.path).unwrap());
            }

            if pair.meta.is_some() {
                self.tracker
                    .delete_dirty_file_state(txn, pair.meta.as_ref().map(|p| &p.path).unwrap());
            }
        }
    }

    // If we detect a file being renamed, update asset_id_to_path and path_to_metadata tables.
    fn handle_rename_events(&self, txn: &mut RwTransaction) {
        let rename_events = self.tracker.read_rename_events(txn.conn());
        debug!("rename events");

        for (_, evt) in rename_events.iter() {
            let dst_str = evt.dst.to_string_lossy();
            let dst = dst_str.as_bytes();
            let mut asset_ids = Vec::new();
            let mut existing_metadata = None;

            {
                let metadata = self.get_source_metadata(txn.conn(), &evt.src);
                if let Some(metadata) = metadata {
                    let metadata_reader = metadata.get().expect("capnp: Failed to get metadata");
                    let mut copy = capnp::message::Builder::new_default();
                    copy.set_root(metadata_reader)
                        .expect("capnp: Failed to set root for metadata");

                    existing_metadata = Some(copy);
                    for asset in metadata_reader
                        .get_assets()
                        .expect("capnp: Failed to get assets")
                    {
                        let id = asset
                            .get_id()
                            .and_then(|a| a.get_id())
                            .expect("capnp: Failed to get asset id");
                        asset_ids.push(Vec::from(id));
                    }
                }
            }

            // Update the asset_id_to_path table
            {
                let mut stmt = txn.conn()
                    .prepare_cached("INSERT OR REPLACE INTO asset_id_to_path (key, path) VALUES (?1, ?2)")
                    .expect("db: Failed to prepare asset_id_to_path upsert");
                let dst_str = str::from_utf8(dst).expect("utf8: Failed to parse dst path");
                for asset in asset_ids {
                    stmt.execute(rusqlite::params![asset, dst_str])
                        .expect("db: Failed to update asset_id_to_path table");
                }
            }

            // Update the path_to_metadata table, if a metadata exists for this path
            if let Some(existing_metadata) = existing_metadata {
                self.delete_source_metadata(txn, &evt.src);
                queries::put_capnp(txn.conn(), TABLE_PATH_TO_METADATA, dst, &existing_metadata)
                    .expect("db: Failed to put to path_to_metadata table");
            }
            txn.dirty = true;
        }

        if !rename_events.is_empty() {
            self.tracker.clear_rename_events(txn);
        }
    }

    // Scan all file metadata and compare them with the default importer state.
    async fn check_for_importer_changes(&self) -> bool {
        let changed_paths: Vec<PathBuf> = {
            let txn = self.db.ro_txn().await.expect("db: Failed to open ro txn");

            // Single LEFT JOIN: get all source file paths with their metadata (if any)
            let mut stmt = txn.conn().prepare_cached(
                "SELECT sf.key, pm.value FROM source_files sf \
                 LEFT JOIN path_to_metadata pm ON sf.key = pm.key",
            ).expect("db: Failed to prepare importer change query");
            let mut rows = stmt.query([]).expect("db: Failed to query for importer changes");

            let mut result = Vec::new();
            while let Some(row) = rows.next().expect("db: Failed to iterate rows") {
                let key: Vec<u8> = row.get(0).expect("db: Failed to get key");
                let path = PathBuf::from(
                    str::from_utf8(&key).expect("utf8: Failed to parse file path"),
                );
                let metadata_bytes: Option<Vec<u8>> = row.get(1).ok();
                let importer = self.importers.get_by_path(&path);

                let changed = match (importer, metadata_bytes) {
                    (None, None) => false,
                    (None, Some(_)) => true,
                    (Some(_), None) => true,
                    (Some(importer), Some(bytes)) => {
                        let reader = capnp::serialize::read_message(
                            &mut bytes.as_slice(),
                            distill_schema::default_capnp_reader_options(),
                        ).expect("capnp: Failed to read metadata");
                        let typed = reader.into_typed::<data::source_metadata::Owned>();
                        let metadata = typed.get().expect("capnp: Failed to get metadata");

                        metadata.get_importer_version() != importer.version()
                            || metadata.get_importer_options_type()
                                .expect("capnp: Failed to get importer options type")
                                != importer.options_type_uuid()
                            || metadata.get_importer_state_type()
                                .expect("capnp: Failed to get importer state type")
                                != importer.default_state().uuid()
                            || metadata.get_importer_type()
                                .expect("capnp: Failed to get importer type")
                                != importer.uuid()
                    }
                };

                if changed {
                    result.push(path);
                }
            }
            result
        };
        let has_changed_paths = !changed_paths.is_empty();
        if has_changed_paths {
            log::debug!(
                "Found {} paths with importer changes, marking as dirty",
                changed_paths.len()
            );
            let mut txn = self.db.rw_txn().await.expect("Failed to open rw txn");
            for p in changed_paths.iter() {
                self.tracker
                    .add_dirty_file(&mut txn, p)
                    .await
                    .unwrap_or_else(|err| error!("Failed to add dirty file, {}", err));
            }
            txn.commit().expect("Failed to commit txn");
        }

        has_changed_paths
    }

    // Scan the dirty files list in the DB for source/meta file pairs.
    fn handle_dirty_files(&self, txn: &mut RwTransaction) -> HashMap<PathBuf, SourcePair> {
        let dirty_files = self.tracker.read_dirty_files(txn.conn());
        let mut source_meta_pairs: HashMap<PathBuf, SourcePair> = HashMap::new();
        log::trace!("Found {} dirty files", dirty_files.len());

        if !dirty_files.is_empty() {
            for state in dirty_files.into_iter() {
                if state.ty == data::FileType::Symlink {
                    continue;
                }
                let mut is_meta = false;
                if let Some(ext) = state.path.extension() {
                    if let Some("meta") = ext.to_str() {
                        is_meta = true;
                    }
                }
                let base_path = if is_meta {
                    state.path.with_file_name(state.path.file_stem().unwrap())
                } else {
                    state.path.clone()
                };
                let pair = source_meta_pairs.entry(base_path).or_insert(SourcePair {
                    source: Option::None,
                    meta: Option::None,
                });
                if is_meta {
                    pair.meta = Some(state.clone());
                } else {
                    pair.source = Some(state.clone());
                }
            }

            for (path, pair) in source_meta_pairs.iter_mut() {
                if pair.meta.is_none() {
                    let path = utils::to_meta_path(path);
                    pair.meta = self.tracker.get_file_state(txn.conn(), &path);
                }

                if pair.source.is_none() {
                    pair.source = self.tracker.get_file_state(txn.conn(), path);
                }
            }

            debug!("Processing {} changed file pairs", source_meta_pairs.len());
        }

        source_meta_pairs
    }

    async fn process_asset_metadata(
        &self,
        txn: &mut RwTransaction,
        hashed_files: &[HashedSourcePair],
    ) -> bool {
        let txn = Mutex::new(txn);
        let metadata_changes = Mutex::new(HashMap::new());
        let metadata_changes_ref = &metadata_changes;

        self.runtime.scope(|scope| {
            let local = async_executor::LocalExecutor::new();
            async_io::block_on(local.run(async {
                let (sender, mut receiver) = unbounded();

                for p in hashed_files {
                    let processed_pair = p.clone();
                    let sender = sender.clone();

                    scope.spawn(async move {
                        let read_txn = self
                            .db
                            .ro_txn()
                            .await
                            .expect("failed to open RO transaction");

                        let cache = DBSourceMetadataCache {
                            conn: read_txn.conn(),
                            file_asset_source: self,
                            owner_thread: std::thread::current().id(),
                        };

                        let result = source_pair_import::import_pair(
                            &cache,
                            &self.importers,
                            &self.importer_contexts,
                            &processed_pair,
                            &mut Vec::new(),
                        ).await;

                        let result = match result {
                            Err(e) => {
                                sender.unbounded_send((processed_pair.clone(), Err(e))).expect("failed to send");
                                return;
                            },
                            Ok(result) => result,
                        };

                        if let Some((import, import_output)) = result {
                            let metadata = if let Some(mut import_output) = import_output {
                                if let Some(import_op) = import_output.import_op {
                                    for error in &import_op.errors {
                                        log::error!("Import errors {:?}: {:?}", p.source, error);
                                    }
                                    for warning in &import_op.warnings {
                                        log::warn!("Import warning {:?}: {:?}", p.source, warning);
                                    }
                                }
                                if !import_output.assets.is_empty() {
                                    let mut txn = self
                                        .artifact_cache
                                        .rw_txn()
                                        .await
                                        .expect("failed to get cache txn");

                                    for asset in import_output.assets.iter_mut() {
                                        if asset.is_fully_resolved() {
                                            if let Some(serialized_asset) = asset.serialized_asset.as_mut()
                                            {
                                                serialized_asset.metadata.id =
                                                    ArtifactId(utils::calc_import_artifact_hash(
                                                        &asset.metadata.id,
                                                        import.import_hash().unwrap(),
                                                        serialized_asset
                                                            .metadata
                                                            .load_deps
                                                            .iter()
                                                            .chain(
                                                                serialized_asset.metadata.build_deps.iter(),
                                                            )
                                                            .map(|dep| dep.expect_uuid()),
                                                    ));
                                                log::trace!(
                                                    "caching asset {:?} from file {:?} with hash {:?}",
                                                    asset.metadata.id,
                                                    p.source,
                                                    serialized_asset.metadata.id
                                                );
                                                self.artifact_cache.insert(&mut txn, serialized_asset);
                                            } else {
                                                log::trace!("asset {:?} from file {:?} did not return serialized asset: cannot cache", asset.metadata.id, p.source);
                                            }
                                        } else {
                                            log::trace!("asset {:?} from file {:?} not fully resolved: cannot cache", asset.metadata.id, p.source);
                                        }
                                    }
                                    txn.commit().expect("failed to commit cache txn");
                                }

                                Some(PairImportResultMetadata {
                                    import_state: import,
                                    assets: import_output
                                        .assets
                                        .into_iter()
                                        .map(|a| AssetImportResultMetadata {
                                            metadata: a.metadata,
                                            unresolved_load_refs: a.unresolved_load_refs,
                                            unresolved_build_refs: a.unresolved_build_refs,
                                        })
                                        .collect(),
                                })
                            } else {
                                None
                            };

                            let path = &processed_pair
                                .source
                                .as_ref()
                                .or_else(|| processed_pair.meta.as_ref())
                                .expect("a successful import must have a source or meta FileState")
                                .path;

                            metadata_changes_ref
                                .lock()
                                .await
                                .insert(path.clone(), metadata);
                        };

                        sender.unbounded_send((processed_pair.clone(), Ok(()))).expect("failed to send");
                    });
                }

                std::mem::drop(sender);

                while let Some((pair, maybe_result)) = receiver.next().await {
                    match maybe_result {
                        Ok(()) => {
                            let mut txn = txn.lock().await;
                            self.ack_dirty_file_states(&mut txn, &pair);
                        }
                        Err(e) => {
                            error!(
                                "Error processing pair at {:?}: {}",
                                pair.source.as_ref().map(|s| &s.path),
                                e
                            )
                        }
                    }
                }
            }))
        });

        let mut change_batch = asset_hub::ChangeBatch::new();
        let txn = txn.into_inner();

        self.process_metadata_changes(txn, &metadata_changes.into_inner(), &mut change_batch);
        self.hub
            .add_changes(txn, change_batch)
            .expect("Failed to process metadata changes")
    }

    async fn handle_update(&self) {
        let start_time = Instant::now();
        let mut changed_files = Vec::new();

        log::trace!("handle_update acquiring rw txn");
        let mut txn = self.db.rw_txn().await.expect("Failed to open rw txn");
        log::trace!("handle_update acquired rw txn, checking rename events");

        self.handle_rename_events(&mut txn);
        log::trace!("handle_update handle_dirty_files");
        let source_meta_pairs = self.handle_dirty_files(&mut txn);

        changed_files.extend(source_meta_pairs.into_iter().map(|(_, v)| v));

        log::trace!("handle_update committing");
        txn.commit().expect("Failed to commit txn");
        log::trace!("handle_update committed");

        let hashed_files = hash_files(&changed_files);
        debug!("Hashed {}", hashed_files.len());

        let hashed_files: Vec<HashedSourcePair> = hashed_files
            .into_iter()
            .filter_map(|f| match f {
                Ok(hashed_file) => Some(hashed_file),
                Err(err) => {
                    error!("Hashing error: {}", err);
                    None
                }
            })
            .collect();

        let elapsed = Instant::now().duration_since(start_time);
        debug!(
            "Hashed {} pairs in {}",
            hashed_files.len(),
            elapsed.as_secs_f32()
        );

        let mut txn = self.db.rw_txn().await.expect("Failed to open rw txn");
        let asset_metadata_changed = self.process_asset_metadata(&mut txn, &hashed_files).await;

        txn.commit().expect("Failed to commit txn");
        if asset_metadata_changed {
            self.hub.notify_listeners();
        }

        let elapsed = Instant::now().duration_since(start_time);
        info!(
            "Processed {} pairs in {}",
            hashed_files.len(),
            elapsed.as_secs_f32()
        );
    }

    pub async fn run(&self, mut rx: UnboundedReceiver<FileTrackerEvent>) {
        let mut update = false;
        let mut unscanned_dirs = self.tracker.get_watch_dirs();

        while let Some(evt) = rx.next().await {
            log::debug!("Received file tracker event {:?}", evt);
            match evt {
                FileTrackerEvent::ScanStarted(_path) => {}
                FileTrackerEvent::ScanFinished(path) => {
                    unscanned_dirs.retain(|p| p != &path);
                    if (update && unscanned_dirs.is_empty())
                        || self.check_for_importer_changes().await
                    {
                        self.handle_update().await;
                    }
                }
                FileTrackerEvent::Update => {
                    update = true;
                    println!("UPDATE DISTILL");
                    if unscanned_dirs.is_empty() {
                        self.handle_update().await;
                    }
                }
            }
        }
    }

    pub async fn export_source(
        &self,
        path: PathBuf,
        assets: Vec<SerializedAssetVec>,
    ) -> Result<Vec<AssetMetadata>> {
        let mut txn = self
            .db
            .rw_txn()
            .await
            .expect("failed to open RW transaction");
        let cache = DBSourceMetadataCache {
            conn: txn.conn(),
            file_asset_source: self,
            owner_thread: std::thread::current().id(),
        };
        let meta_path = utils::to_meta_path(&path);
        let result = source_pair_import::export_pair(
            assets,
            &cache,
            &self.importers,
            &self.importer_contexts,
            path.clone(),
            meta_path,
            &mut Vec::new(),
        )
        .await?;
        let new_asset_metadata: Vec<AssetImportResultMetadata> = result
            .1
            .assets
            .into_iter()
            .map(|a| AssetImportResultMetadata {
                metadata: a.metadata,
                unresolved_load_refs: a.unresolved_load_refs,
                unresolved_build_refs: a.unresolved_build_refs,
            })
            .collect();
        let mut changes = HashMap::new();
        changes.insert(
            path.clone(),
            Some(PairImportResultMetadata {
                import_state: result.0,
                assets: new_asset_metadata,
            }),
        );
        let new_asset_metadata = changes[&path].as_ref().unwrap();
        let mut change_batch = asset_hub::ChangeBatch::new();
        self.process_metadata_changes(&mut txn, &changes, &mut change_batch);
        let asset_metadata_changed = self.hub.add_changes(&mut txn, change_batch)?;
        let new_asset_metadata: Vec<AssetMetadata> = new_asset_metadata
            .assets
            .iter()
            .map(|a| {
                parse_db_metadata(
                    &self
                        .hub
                        .get_asset_metadata(txn.conn(), &a.metadata.id)
                        .expect("Expected asset metadata in DB after metadata update")
                        .get()
                        .expect("capnp: metadata read failed"),
                )
            })
            .collect();
        if txn.dirty {
            txn.commit().expect("Failed to commit txn");

            if asset_metadata_changed {
                self.hub.notify_listeners();
            }
        }
        Ok(new_asset_metadata)
    }
}

struct DBSourceMetadataCache<'a> {
    conn: &'a Connection,
    file_asset_source: &'a FileAssetSource,
    /// Thread that created this cache. Debug-assert on every access to verify
    /// the `unsafe impl Sync` invariant: the connection never crosses threads.
    owner_thread: std::thread::ThreadId,
}

// Safety: Each spawned task creates its own Connection (via RoTransaction) and
// never shares it with other threads. The &Connection is used only within
// the owning task. The `owner_thread` assert guards this invariant at runtime.
unsafe impl Send for DBSourceMetadataCache<'_> {}
unsafe impl Sync for DBSourceMetadataCache<'_> {}

impl DBSourceMetadataCache<'_> {
    fn assert_owner(&self) {
        debug_assert_eq!(
            std::thread::current().id(),
            self.owner_thread,
            "DBSourceMetadataCache accessed from a different thread than the one that created it"
        );
    }
}

impl<'a> source_pair_import::SourceMetadataCache for DBSourceMetadataCache<'a> {
    fn restore_source_metadata(
        &self,
        path: &Path,
        importer: &dyn BoxedImporter,
        metadata: &mut SourceMetadata,
    ) -> Result<()> {
        self.assert_owner();
        let saved_metadata = self.file_asset_source.get_source_metadata(self.conn, path);
        if let Some(saved_metadata) = saved_metadata {
            let saved_metadata = saved_metadata.get()?;
            metadata.version = saved_metadata.get_version();
            if saved_metadata.get_importer_options_type()? == metadata.importer_options.uuid() {
                let mut deserializer = bincode::Deserializer::from_slice(
                    saved_metadata.get_importer_options()?,
                    bincode::options()
                        .with_fixint_encoding()
                        .allow_trailing_bytes(),
                );
                let mut deserializer =
                    <dyn erased_serde::Deserializer<'_>>::erase(&mut deserializer);

                if let Ok(options) = importer.deserialize_options(&mut deserializer) {
                    metadata.importer_options = options;
                }
            }
            if saved_metadata.get_importer_state_type()? == metadata.importer_state.uuid() {
                let mut deserializer = bincode::Deserializer::from_slice(
                    saved_metadata.get_importer_state()?,
                    bincode::options()
                        .with_fixint_encoding()
                        .allow_trailing_bytes(),
                );
                let mut deserializer =
                    <dyn erased_serde::Deserializer<'_>>::erase(&mut deserializer);
                if let Ok(state) = importer.deserialize_state(&mut deserializer) {
                    metadata.importer_state = state;
                }
            }
        }
        Ok(())
    }

    fn get_cached_metadata(&self, path: &Path) -> Result<Option<ImportResultMetadata>> {
        self.assert_owner();
        let saved_metadata = self.file_asset_source.get_source_metadata(self.conn, path);
        if let Some(saved_metadata) = saved_metadata {
            let saved_metadata = saved_metadata.get()?;
            let import_hash = Some(u64::from_le_bytes(utils::make_array(
                saved_metadata.get_import_hash()?,
            )));
            let importer_version = saved_metadata.get_importer_version();
            let importer_type = AssetTypeId(utils::make_array(saved_metadata.get_importer_type()?));
            let assets = saved_metadata
                .get_assets()?
                .iter()
                .map(|a| parse_db_metadata(&a))
                .collect();
            Ok(Some(ImportResultMetadata {
                import_hash,
                importer_version,
                importer_type,
                assets,
            }))
        } else {
            Ok(None)
        }
    }
}
