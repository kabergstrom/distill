use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    thread::JoinHandle,
};

use asset_hub::AssetHub;
use asset_hub_service::AssetHubService;
use distill_core::distill_signal;
use distill_importer::{BoxedImporter, ImporterContext};
use distill_loader::if_handle_enabled;
use distill_schema::data;
use file_asset_source::FileAssetSource;
use futures::{channel::mpsc::unbounded, future::FutureExt};
use std::rc::Rc;

use crate::{
    artifact_cache::ArtifactCache, asset_hub, asset_hub_service, db::Database,
    error::Result, extension_map::ExtensionMap, file_asset_source, file_tracker::FileTracker,
};

// Container of all importers, can return an importer by a given path's filename extension
#[derive(Default)]
pub struct ImporterMap(ExtensionMap<Box<dyn BoxedImporter>>);

impl ImporterMap {
    pub fn insert(&mut self, ext: &[&str], importer: Box<dyn BoxedImporter>) {
        self.0.insert(ext, importer);
    }

    pub fn get_by_path<'a>(&'a self, path: &Path) -> Option<&'a dyn BoxedImporter> {
        let importer = self.0.get(path)?;
        Some(&**importer)
    }
}

const DAEMON_VERSION: u32 = 3;
pub struct AssetDaemon {
    pub db_dir: PathBuf,
    pub address: SocketAddr,
    pub address_websocket: SocketAddr,
    pub importers: ImporterMap,
    pub importer_contexts: Vec<Box<dyn ImporterContext>>,
    pub asset_dirs: Vec<PathBuf>,
    pub clear_db_on_start: bool,
}

#[allow(unused_mut)]
#[allow(clippy::vec_init_then_push)]
pub fn default_importer_contexts() -> Vec<Box<dyn ImporterContext + 'static>> {
    let mut importers = Vec::new();
    if_handle_enabled!(importers
        .push(Box::new(distill_loader::handle::HandleSerdeContextProvider)
            as Box<dyn ImporterContext + 'static>));
    importers
}

#[allow(unused_mut)]
#[allow(clippy::vec_init_then_push)]
pub fn default_importers() -> Vec<(&'static str, Box<dyn BoxedImporter>)> {
    let mut importers: Vec<(&'static str, Box<dyn BoxedImporter>)> = vec![];

    distill_importer::if_serde_importers!(
        importers.push(("ron", Box::new(distill_importer::RonImporter::default())))
    );
    importers
}
impl Default for AssetDaemon {
    fn default() -> Self {
        let mut importer_map = ImporterMap::default();
        for (ext, importer) in default_importers() {
            importer_map.insert(&[ext], importer);
        }
        Self {
            db_dir: PathBuf::from(".assets_db"),
            address: "127.0.0.1:9999".parse().unwrap(),
            address_websocket: "127.0.0.1:9998".parse().unwrap(),
            importers: importer_map,
            importer_contexts: default_importer_contexts(),
            asset_dirs: vec![PathBuf::from("assets")],
            clear_db_on_start: false,
        }
    }
}

impl AssetDaemon {
    pub fn with_db_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.db_dir = path.as_ref().to_owned();
        self
    }

    pub fn with_address(mut self, address: SocketAddr) -> Self {
        self.address = address;
        self
    }

    pub fn with_importer<B>(mut self, exts: &[&str], importer: B) -> Self
    where
        B: BoxedImporter + 'static,
    {
        self.importers.insert(exts, Box::new(importer));
        self
    }

    pub fn add_importer<B>(&mut self, exts: &[&str], importer: B)
    where
        B: BoxedImporter + 'static,
    {
        self.importers.insert(exts, Box::new(importer));
    }

    pub fn with_importers<B, I>(self, importers: I) -> Self
    where
        B: BoxedImporter + 'static,
        I: IntoIterator<Item = (&'static [&'static str], B)>,
    {
        importers.into_iter().fold(self, |this, (ext, importer)| {
            this.with_importer(ext, importer)
        })
    }

    pub fn with_importers_boxed<I>(mut self, importers: I) -> Self
    where
        I: IntoIterator<Item = (&'static [&'static str], Box<dyn BoxedImporter + 'static>)>,
    {
        for (ext, importer) in importers.into_iter() {
            self.importers.insert(ext, importer)
        }
        self
    }

    pub fn add_importers<B, I>(&mut self, importers: I)
    where
        B: BoxedImporter + 'static,
        I: IntoIterator<Item = (&'static [&'static str], B)>,
    {
        for (ext, importer) in importers {
            self.add_importer(ext, importer)
        }
    }

    pub fn with_importer_context(mut self, context: Box<dyn ImporterContext>) -> Self {
        self.importer_contexts.push(context);
        self
    }

    pub fn with_importer_contexts<I>(mut self, contexts: I) -> Self
    where
        I: IntoIterator<Item = Box<dyn ImporterContext>>,
    {
        self.importer_contexts.extend(contexts);
        self
    }

    pub fn with_asset_dirs(mut self, dirs: Vec<PathBuf>) -> Self {
        self.asset_dirs = dirs;
        self
    }

    /// Force the daemon to clean the cache on start
    pub fn with_clear_db_on_start(mut self) -> Self {
        self.clear_db_on_start = true;
        self
    }

    pub fn run(self) -> (JoinHandle<()>, distill_signal::Sender<bool>) {
        let (tx, rx) = distill_signal::oneshot();

        let handle = thread::spawn(|| {
            let local = Rc::new(async_executor::LocalExecutor::new());
            async_io::block_on(local.run(self.run_rpc_runtime(&local, rx)))
        });

        (handle, tx)
    }

    async fn run_rpc_runtime(
        self,
        local: &async_executor::LocalExecutor<'_>,
        rx: distill_signal::Receiver<bool>,
    ) {
        let cache_dir = self.db_dir.join("cache");
        let _ = fs::create_dir(&self.db_dir);
        let _ = fs::create_dir(&cache_dir);

        for dir in self.asset_dirs.iter() {
            let _ = fs::create_dir_all(dir);
        }

        let asset_db = Database::new(&self.db_dir).expect("failed to create asset db");
        let asset_db = Arc::new(asset_db);

        try_clear_db(&asset_db, self.clear_db_on_start)
            .await
            .expect("failed to clear asset db");
        set_db_version(&asset_db)
            .await
            .expect("failed to check daemon version in asset db");

        let to_watch = self.asset_dirs.iter().map(|p| p.to_str().unwrap());
        let tracker = FileTracker::new(asset_db.clone(), to_watch);
        let tracker = Arc::new(tracker);

        let hub = AssetHub::new(asset_db.clone()).expect("failed to create asset hub");
        let hub = Arc::new(hub);

        let importers = Arc::new(self.importers);
        let ctxs = Arc::new(self.importer_contexts);
        let cache_db = Database::new(&cache_dir).expect("failed to create cache db");
        let cache_db = Arc::new(cache_db);
        try_clear_db(&cache_db, self.clear_db_on_start)
            .await
            .expect("failed to clear cache db");
        set_db_version(&cache_db)
            .await
            .expect("failed to check daemon version in cache db");
        let artifact_cache =
            ArtifactCache::new(&cache_db).expect("failed to create artifact cache");
        let artifact_cache = Arc::new(artifact_cache);

        let asset_source =
            FileAssetSource::new(&tracker, &hub, &asset_db, &importers, &artifact_cache, ctxs)
                .expect("failed to create asset source");

        let asset_source = Arc::new(asset_source);

        let service = AssetHubService::new(
            asset_db.clone(),
            hub.clone(),
            asset_source.clone(),
            tracker.clone(),
            artifact_cache.clone(),
        );
        let service = Arc::new(service);

        let addr = self.address;

        let shutdown_tracker = tracker.clone();

        #[cfg(feature = "ws")]
        let mut service_ws_handle = {
            let addr_ws = self.address_websocket;

            let service_clone = Arc::clone(&service);
            local
                .spawn(async move { service_clone.run_on_websocket(addr_ws).await.unwrap() })
                .fuse()
        };
        #[cfg(not(feature = "ws"))]
        let mut service_ws_handle = futures::future::pending::<()>().fuse();

        let service_handle = local.spawn(async move { service.run(addr).await }).fuse();

        let (file_events_tx, file_events_rx) = unbounded();
        tracker.register_listener(file_events_tx);

        let asset_source_handle = local
            .spawn(async move { asset_source.run(file_events_rx).await })
            .fuse();

        let tracker_handle = local.spawn(async move { tracker.run().await }).fuse();

        let rx_fuse = rx.fuse();

        futures::pin_mut!(service_handle, tracker_handle, asset_source_handle, rx_fuse);

        log::info!("Starting Daemon Loop");
        loop {
            futures::select! {
                _done = &mut service_handle => panic!("ServiceHandle panicked"),
                _done = &mut service_ws_handle => panic!("ServiceWebsocketHandle panicked"),
                _done = &mut tracker_handle => panic!("FileTracker panicked"),
                _done = &mut asset_source_handle => panic!("AssetSource panicked"),
                done = &mut rx_fuse => match done {
                    Ok(_) => {
                        log::warn!("Shutting Down!");
                        shutdown_tracker.stop().await;
                        return;
                    }
                    Err(_) => continue,
                }
            };
        }
    }
}

/// All tables to clear when the DB version doesn't match
const ALL_TABLES: &[&str] = &[
    "source_files",
    "dirty_files",
    "asset_metadata",
    "path_to_metadata",
    "asset_id_to_path",
    "daemon_info",
    "hash_to_artifact",
    "build_deps",
    "path_refs",
    "rename_file_events",
    "asset_changes",
];

#[allow(clippy::string_lit_as_bytes)]
async fn try_clear_db(db: &Database, force_clear_db: bool) -> Result<()> {
    use crate::db::queries;
    let txn = db.ro_txn().await?;
    let info_key = "daemon_info".as_bytes();

    let mut clear_db = true;
    let daemon_info =
        queries::get_capnp::<data::daemon_info::Owned>(txn.conn(), "daemon_info", info_key)?;
    if let Some(info) = daemon_info {
        let info = info.get()?;
        if info.get_version() == DAEMON_VERSION {
            clear_db = false;
        }
    }
    drop(txn);

    if clear_db || force_clear_db {
        let mut txn = db.rw_txn().await?;
        for table in ALL_TABLES {
            queries::clear_table(txn.conn(), table)?;
        }
        // Also reset the autoincrement counters
        txn.conn().execute(
            "DELETE FROM sqlite_sequence WHERE name IN ('rename_file_events', 'asset_changes')",
            [],
        ).ok(); // sqlite_sequence may not exist if no inserts happened yet
        txn.dirty = true;
        txn.commit()?;
    }
    Ok(())
}

#[allow(clippy::string_lit_as_bytes)]
async fn set_db_version(db: &Database) -> Result<()> {
    use crate::db::queries;
    let info_key = "daemon_info".as_bytes();
    let mut txn = db.rw_txn().await?;
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<data::daemon_info::Builder<'_>>();
        m.set_version(DAEMON_VERSION);
    }
    queries::put_capnp(txn.conn(), "daemon_info", info_key, &value_builder)?;
    txn.dirty = true;
    txn.commit()?;

    Ok(())
}
