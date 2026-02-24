use std::{
    cell::Cell,
    cmp::PartialEq,
    collections::{HashMap, HashSet},
    fs,
    ops::IndexMut,
    path::{Path, PathBuf},
    str,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    time::Duration,
};

use distill_core::utils::canonicalize_path;
use distill_schema::data::{self, dirty_file_info, source_file_info, FileType};
use event_listener::Event;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    lock::Mutex,
    stream::StreamExt,
    FutureExt,
};
use log::{debug, info};
use rusqlite::Connection;

use crate::{
    db::{queries, Database, OwnedMessageReader, RwTransaction},
    error::{Error, Result},
    watcher::{self, FileEvent, FileMetadata},
};

const TABLE_SOURCE_FILES: &str = "source_files";
const TABLE_DIRTY_FILES: &str = "dirty_files";

#[derive(Clone, Debug)]
pub enum FileTrackerEvent {
    // Sent when we start scanning a directory
    ScanStarted(PathBuf),
    // Sent when we finish scanning a directroy on startup, meaning we can drop any data in the
    // db that wasn't found during the scan
    ScanFinished(PathBuf),
    // Debounced event that indicates there are dirty files ready for processing
    Update,
}

// Starts a thread to watch the file system (via DirWatcher). Receives events about changes to file
// system and populates SQLite tables. Downstream users of this struct can clear the dirty/rename
// records as they are processed
pub struct FileTracker {
    // The single SQLite database
    db: Arc<Database>,

    // Channel that allows registering new listeners while running
    listener_rx: Mutex<Cell<UnboundedReceiver<UnboundedSender<FileTrackerEvent>>>>,
    listener_tx: UnboundedSender<UnboundedSender<FileTrackerEvent>>,

    // Flag indicates we are running, ensures we do not start the task if it's already running
    is_running: AtomicBool,
    // Used to signal the running task to stop
    stopping_event: event_listener::Event,

    // The directories behing watched, this can change when symlinks are created in watched dirs
    watch_dirs: RwLock<Vec<PathBuf>>,
}

// Path to file and description of its state when it was last found by the file watcher
#[derive(Clone)]
pub struct FileState {
    pub path: PathBuf,
    // Indicates if file still exists or was deleted
    pub state: data::FileState,
    pub last_modified: u64,
    pub length: u64,
    pub ty: data::FileType,
}

impl std::fmt::Debug for FileState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use capnp::traits::ToU16;
        f.debug_struct("FileState")
            .field("path", &self.path)
            .field("state", &self.state)
            .field("last_modified", &self.last_modified)
            .field("length", &self.length)
            .field("ty", &self.ty.to_u16())
            .finish()
    }
}

impl PartialEq for FileState {
    fn eq(&self, other: &FileState) -> bool {
        self.path == other.path
            && self.state == other.state
            && self.last_modified == other.last_modified
            && self.length == other.length
    }
}
#[derive(Clone, Debug)]
pub struct RenameFileEvent {
    pub src: PathBuf,
    pub dst: PathBuf,
}

// We keep a stack of these during directory scans and add files that we encounter to the
// ScanContext on the top of the stack
struct ScanContext {
    path: PathBuf,
    files: HashMap<PathBuf, FileMetadata>,
}

pub fn db_file_type(t: fs::FileType) -> FileType {
    if t.is_dir() {
        FileType::Directory
    } else if t.is_symlink() {
        FileType::Symlink
    } else {
        FileType::File
    }
}

struct ListenersList {
    listeners: Vec<UnboundedSender<FileTrackerEvent>>,
}

impl ListenersList {
    fn new() -> Self {
        Self {
            listeners: Vec::new(),
        }
    }

    fn register(&mut self, new_listener: Option<UnboundedSender<FileTrackerEvent>>) {
        if let Some(new_listener) = new_listener {
            self.listeners.push(new_listener);
        }
    }

    fn send_event(&mut self, event: FileTrackerEvent) {
        self.listeners.retain(move |listener| {
            match listener.unbounded_send(event.clone()) {
                Ok(()) => {
                    debug!("Sent to listener");
                    true
                }
                // channel was closed, drop the listener
                Err(_) => {
                    debug!("Listener dropped");
                    false
                }
            }
        })
    }
}

// Adds a new record to rename_file_events using autoincrement
fn add_rename_event(
    conn: &Connection,
    src: &[u8],
    dst: &[u8],
) -> Result<()> {
    let src_str = str::from_utf8(src).expect("utf8: Failed to parse src");
    let dst_str = str::from_utf8(dst).expect("utf8: Failed to parse dst");
    conn.execute(
        "INSERT INTO rename_file_events (src, dst) VALUES (?1, ?2)",
        rusqlite::params![src_str, dst_str],
    )?;
    Ok(())
}

// Creates a DirtyFileInfo capn proto message
fn build_dirty_file_info(
    state: data::FileState,
    source_info: source_file_info::Reader<'_>,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<dirty_file_info::Builder<'_>>();
        value.set_state(state);
        value
            .set_source_info(source_info)
            .expect("failed to set source info");
    }
    value_builder
}

// Creates a SourceFileInfo capn proto message
fn build_source_info(
    metadata: &watcher::FileMetadata,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<source_file_info::Builder<'_>>();
        value.set_last_modified(metadata.last_modified);
        value.set_length(metadata.length);
        value.set_type(db_file_type(metadata.file_type));
    }
    value_builder
}

// Checks if the file is in source_files. If it is, write a delete entry to the dirty_files table
fn update_deleted_dirty_entry(
    conn: &Connection,
    key: &[u8],
) -> Result<()> {
    let dirty_value = {
        queries::get_capnp::<source_file_info::Owned>(conn, TABLE_SOURCE_FILES, key)?
            .map(|v| {
                let info = v.get().expect("failed to get source_file_info");
                build_dirty_file_info(data::FileState::Deleted, info)
            })
    };
    if let Some(dirty_value) = dirty_value {
        queries::put_capnp(conn, TABLE_DIRTY_FILES, key, &dirty_value)?;
    }
    Ok(())
}

fn get_file_state_from_conn(
    conn: &Connection,
    table: &str,
    path: &Path,
) -> Option<FileState> {
    let key_str = path.to_string_lossy();
    let key = key_str.as_bytes();
    if table == TABLE_SOURCE_FILES {
        queries::get_capnp::<source_file_info::Owned>(conn, table, key)
            .expect("db: Failed to get entry from source_files table")
            .map(|value| {
                let info = value.get().expect("capnp: Failed to get source file info");
                FileState {
                    path: path.to_path_buf(),
                    state: data::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                    ty: info
                        .get_type()
                        .expect("Failed to read type in source file info"),
                }
            })
    } else {
        queries::get_capnp::<dirty_file_info::Owned>(conn, table, key)
            .expect("db: Failed to get entry from dirty_files table")
            .map(|value| {
                let value = value.get().expect("capnp: Failed to get dirty file info");
                let info = value
                    .get_source_info()
                    .expect("capnp: Failed to get source info");
                FileState {
                    path: path.to_path_buf(),
                    state: data::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                    ty: info.get_type().expect("Failed to read type in source info"),
                }
            })
    }
}

// TODO(happens): Improve error handling for event handlers
mod events {
    use std::path::Path;

    use super::*;

    // Called from handle_file_event
    fn handle_update(
        txn: &mut RwTransaction,
        path: &Path,
        metadata: &watcher::FileMetadata,
        scan_stack: &mut Vec<ScanContext>,
    ) -> Result<()> {
        let path_str = path.to_string_lossy();
        let key = path_str.as_bytes();
        let mut changed = true;
        {
            let maybe_msg: Option<OwnedMessageReader<source_file_info::Owned>> =
                queries::get_capnp(txn.conn(), TABLE_SOURCE_FILES, key)?;
            if let Some(msg) = maybe_msg {
                let info = msg.get()?;
                if info.get_length() == metadata.length
                    && info.get_last_modified() == metadata.last_modified
                    && info.get_type()? == db_file_type(metadata.file_type)
                {
                    // The file's metadata matches what we already stored
                    changed = false;
                } else {
                    debug!("CHANGED {} metadata {:?}", path_str, metadata);
                }
            }
        }
        // Add the files
        if !scan_stack.is_empty() {
            let head_idx = scan_stack.len() - 1;
            let scan_ctx = scan_stack.index_mut(head_idx);
            scan_ctx.files.insert(path.to_path_buf(), metadata.clone());
        }
        if changed {
            let value = build_source_info(metadata);
            let dirty_value = build_dirty_file_info(
                data::FileState::Exists,
                value.get_root_as_reader::<source_file_info::Reader<'_>>()?,
            );
            queries::put_capnp(txn.conn(), TABLE_SOURCE_FILES, key, &value)?;
            queries::put_capnp(txn.conn(), TABLE_DIRTY_FILES, key, &dirty_value)?;
            txn.dirty = true;
        }
        Ok(())
    }

    // Called from inner loop of run
    pub(super) fn handle_file_event(
        txn: &mut RwTransaction,
        evt: watcher::FileEvent,
        scan_stack: &mut Vec<ScanContext>,
        watch_dirs: &RwLock<Vec<PathBuf>>,
    ) -> Result<Option<FileTrackerEvent>> {
        match evt {
            FileEvent::Updated(path, metadata) => {
                handle_update(txn, &path, &metadata, scan_stack)?;
            }
            FileEvent::Renamed(src, dst, metadata) => {
                if !scan_stack.is_empty() {
                    let head_idx = scan_stack.len() - 1;
                    let scan_ctx = scan_stack.index_mut(head_idx);
                    scan_ctx.files.insert(dst.clone(), metadata.clone());
                    scan_ctx.files.remove(&src);
                }
                let src_str = src.to_string_lossy();
                let src_key = src_str.as_bytes();
                let dst_str = dst.to_string_lossy();
                let dst_key = dst_str.as_bytes();
                debug!("rename {} to {} metadata {:?}", src_str, dst_str, metadata);
                let value = build_source_info(&metadata);
                queries::delete(txn.conn(), TABLE_SOURCE_FILES, src_key)?;
                queries::put_capnp(txn.conn(), TABLE_SOURCE_FILES, dst_key, &value)?;
                let dirty_value_new = build_dirty_file_info(
                    data::FileState::Exists,
                    value.get_root_as_reader::<source_file_info::Reader<'_>>()?,
                );
                let dirty_value_old = build_dirty_file_info(
                    data::FileState::Deleted,
                    value.get_root_as_reader::<source_file_info::Reader<'_>>()?,
                );
                queries::put_capnp(txn.conn(), TABLE_DIRTY_FILES, src_key, &dirty_value_old)?;
                queries::put_capnp(txn.conn(), TABLE_DIRTY_FILES, dst_key, &dirty_value_new)?;
                add_rename_event(txn.conn(), src_key, dst_key)?;
                txn.dirty = true;
            }
            FileEvent::Removed(path) => {
                if !scan_stack.is_empty() {
                    let head_idx = scan_stack.len() - 1;
                    let scan_ctx = scan_stack.index_mut(head_idx);
                    scan_ctx.files.remove(&path);
                }
                let path_str = path.to_string_lossy();
                let key = path_str.as_bytes();
                debug!("removed {}", path_str);
                update_deleted_dirty_entry(txn.conn(), key)?;
                queries::delete(txn.conn(), TABLE_SOURCE_FILES, key)?;
                txn.dirty = true;
            }
            FileEvent::FileError(err) => {
                debug!("file event error: {}", err);
                return Err(err);
            }
            FileEvent::ScanStart(path) => {
                debug!("scan start: {}", path.to_string_lossy());
                scan_stack.push(ScanContext {
                    path: path.clone(),
                    files: HashMap::new(),
                });
                return Ok(Some(FileTrackerEvent::ScanStarted(path)));
            }
            FileEvent::ScanEnd(path, watched_dirs) => {
                // When we finish a scan, we know which files exist in the subdirectories of the
                // current scan. We can now scan our DB for files we've tracked and delete removed
                // files from DB
                let scan_ctx = scan_stack.pop().unwrap();

                // Find all the files that start with the base directory of this scan
                let mut db_file_set = HashSet::new();
                {
                    let path_str = path.to_string_lossy();
                    let key = path_str.as_bytes();
                    let path_string = scan_ctx.path.to_string_lossy().into_owned();
                    let rows = queries::iter_prefix::<source_file_info::Owned>(
                        txn.conn(),
                        TABLE_SOURCE_FILES,
                        key,
                    )?;
                    for (key_bytes, _) in rows {
                        let key = str::from_utf8(&key_bytes).expect("Encoded key was invalid utf8");
                        if !key.starts_with(&path_string) {
                            break;
                        }
                        db_file_set.insert(PathBuf::from(key));
                    }
                }

                // Collapse the files hashmap into a set of keys
                let scan_ctx_set: HashSet<PathBuf> = scan_ctx.files.keys().cloned().collect();

                // Determine what keys in the DB exist that no longer exist on disk and remove them
                let to_remove = db_file_set.difference(&scan_ctx_set);
                for p in to_remove {
                    let p_str = p.to_string_lossy();
                    let p_key = p_str.as_bytes();
                    update_deleted_dirty_entry(txn.conn(), p_key)?;
                    queries::delete(txn.conn(), TABLE_SOURCE_FILES, p_key)?;
                    txn.dirty = true;
                }
                info!(
                    "Scanned and compared {} + {}, deleted {}",
                    scan_ctx_set.len(),
                    db_file_set.len(),
                    db_file_set.difference(&scan_ctx_set).count()
                );

                // If this is the top-level scan, we have a final set of watched directories,
                // so we can delete any files that are not in any watched directories from the DB.
                if scan_stack.is_empty() {
                    let mut to_delete = Vec::new();
                    {
                        let rows = queries::iter_all_raw(txn.conn(), TABLE_SOURCE_FILES)?;
                        let dirs_as_strings: Vec<String> = watched_dirs
                            .into_iter()
                            .map(|f| f.to_string_lossy().into_owned())
                            .collect();
                        for (key_bytes, _) in rows {
                            let key = str::from_utf8(&key_bytes)
                                .expect("Encoded key was invalid utf8");
                            if !dirs_as_strings.iter().any(|dir| key.starts_with(dir)) {
                                to_delete.push(key_bytes);
                            }
                        }
                    }
                    for key_bytes in to_delete {
                        queries::delete(txn.conn(), TABLE_SOURCE_FILES, &key_bytes)?;
                        update_deleted_dirty_entry(txn.conn(), &key_bytes)?;
                        txn.dirty = true;
                    }
                }
                debug!("scan end: {}", path.to_string_lossy());
                return Ok(Some(FileTrackerEvent::ScanFinished(path)));
            }
            FileEvent::Watch(path) => {
                let mut watch_dirs = watch_dirs.write().expect("watch_dirs lock poisoned");
                watch_dirs.push(path);
            }
            FileEvent::Unwatch(path) => {
                let mut watch_dirs = watch_dirs.write().expect("watch_dirs lock poisoned");
                watch_dirs.retain(|p| p != &path);
            }
        }
        Ok(None)
    }
}

impl FileTracker {
    // Creates tables in the provided DB, sanitizes passed paths.
    pub fn new<'a, I, T>(db: Arc<Database>, to_watch: I) -> FileTracker
    where
        I: IntoIterator<Item = &'a str, IntoIter = T>,
        T: Iterator<Item = &'a str>,
    {
        let watch_dirs: Vec<PathBuf> = to_watch
            .into_iter()
            .map(|s| {
                let path = PathBuf::from(s);
                let path = if path.is_relative() {
                    std::env::current_dir()
                        .expect("failed to get current dir")
                        .join(path)
                } else {
                    path
                };
                canonicalize_path(&path)
            })
            .collect();

        let (listener_tx, listener_rx) = unbounded();

        FileTracker {
            is_running: AtomicBool::new(false),
            stopping_event: Event::new(),
            db,
            listener_rx: Mutex::new(Cell::new(listener_rx)),
            listener_tx,
            watch_dirs: RwLock::new(watch_dirs),
        }
    }

    // Find if this file is contained within any of the watched directories. If it is, switch to
    // a relative path starting from that watched directory.
    pub fn make_relative_path(&self, absolute_path: &Path) -> Option<PathBuf> {
        for dir in self.get_watch_dirs() {
            let canonicalized_dir = canonicalize_path(&dir);
            if absolute_path.starts_with(&canonicalized_dir) {
                let relative_path = absolute_path
                    .strip_prefix(canonicalized_dir)
                    .expect("error stripping prefix")
                    .to_path_buf();
                let relative_path = canonicalize_path(&relative_path)
                    .to_string_lossy()
                    .replace("\\", "/");
                return Some(PathBuf::from(relative_path));
            }
        }
        None
    }

    pub fn get_watch_dirs(&self) -> Vec<PathBuf> {
        self.watch_dirs
            .read()
            .expect("watch_dirs lock poisoned")
            .iter()
            .cloned()
            .collect()
    }

    pub async fn get_rw_txn(&self) -> RwTransaction {
        self.db.rw_txn().await.expect("db: Failed to open rw txn")
    }

    // Returns all data from rename_file_events table
    pub fn read_rename_events(
        &self,
        conn: &Connection,
    ) -> Vec<(u64, RenameFileEvent)> {
        let mut stmt = conn
            .prepare_cached("SELECT seq, src, dst FROM rename_file_events ORDER BY seq")
            .expect("db: Failed to prepare rename_file_events query");
        let mut rows = stmt.query([]).expect("db: Failed to query rename_file_events");
        let mut result = Vec::new();
        while let Some(row) = rows.next().expect("db: Failed to iterate rename_file_events") {
            let seq: u64 = row.get(0).expect("db: Failed to get seq");
            let src: String = row.get(1).expect("db: Failed to get src");
            let dst: String = row.get(2).expect("db: Failed to get dst");
            result.push((
                seq,
                RenameFileEvent {
                    src: PathBuf::from(src),
                    dst: PathBuf::from(dst),
                },
            ));
        }
        result
    }

    // Clears the rename_file_events table. To avoid race conditions, read all the rename events,
    // process them, and clear them in the same transaction
    pub fn clear_rename_events(&self, txn: &mut RwTransaction) {
        queries::clear_table(txn.conn(), "rename_file_events")
            .expect("db: Failed to clear rename_file_events table");
    }

    // Checks if the file exists. If it exists, populate source_files and dirty_files tables.
    // Otherwise, write a deleted message to the dirty_files table
    pub async fn add_dirty_file(&self, txn: &mut RwTransaction, path: &Path) -> Result<()> {
        let metadata = match async_fs::metadata(path).await {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(Error::IO(e)),
            Ok(metadata) => Some(watcher::file_metadata(&metadata)),
        };
        let path_str = path.to_string_lossy();
        let key = path_str.as_bytes();
        if let Some(metadata) = metadata {
            let source_info = build_source_info(&metadata);
            let dirty_file_info = build_dirty_file_info(
                data::FileState::Exists,
                source_info.get_root_as_reader::<source_file_info::Reader<'_>>()?,
            );
            queries::put_capnp(txn.conn(), TABLE_SOURCE_FILES, key, &source_info)?;
            queries::put_capnp(txn.conn(), TABLE_DIRTY_FILES, key, &dirty_file_info)?;
            txn.dirty = true;
        } else {
            update_deleted_dirty_entry(txn.conn(), key)?;
        }
        Ok(())
    }

    // Returns file state from dirty_files table
    pub fn read_dirty_files(
        &self,
        conn: &Connection,
    ) -> Vec<FileState> {
        let rows = queries::iter_all::<dirty_file_info::Owned>(conn, TABLE_DIRTY_FILES)
            .expect("db: Failed to read dirty_files");
        rows.into_iter()
            .filter_map(|(key, val)| {
                let key = str::from_utf8(&key).expect("utf8: Failed to parse file path");
                let info = val.get().ok()?;
                let source_info = info
                    .get_source_info()
                    .expect("capnp: Failed to get source info");

                Some(FileState {
                    path: PathBuf::from(key),
                    state: info.get_state().ok()?,
                    last_modified: source_info.get_last_modified(),
                    length: source_info.get_length(),
                    ty: source_info
                        .get_type()
                        .expect("Failed to read type in source file info"),
                })
            })
            .collect()
    }

    // Returns file state from source_files table
    pub fn read_all_files(&self, conn: &Connection) -> Vec<FileState> {
        let rows = queries::iter_all::<source_file_info::Owned>(conn, TABLE_SOURCE_FILES)
            .expect("db: Failed to read source_files");
        rows.into_iter()
            .filter_map(|(key, val)| {
                let key = str::from_utf8(&key).expect("utf8: Failed to parse file path");
                let info = val.get().ok()?;

                Some(FileState {
                    path: PathBuf::from(key),
                    state: data::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                    ty: info
                        .get_type()
                        .expect("Failed to read type in source file info"),
                })
            })
            .collect()
    }

    // Deletes an item from dirty_files table. This function must be used carefully to avoid a race
    // condition; check that the SourceFileInfo stored in the table matches the file that was read
    // and processed
    pub fn delete_dirty_file_state(&self, txn: &mut RwTransaction, path: &Path) -> bool {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        queries::delete(txn.conn(), TABLE_DIRTY_FILES, key)
            .expect("db: Failed to delete entry from dirty_files table")
    }

    #[cfg(not(target_os = "macos"))] // FIXME: these tests fail in macos CI
    #[cfg(test)]
    pub fn get_dirty_file_state(
        &self,
        conn: &Connection,
        path: &Path,
    ) -> Option<FileState> {
        get_file_state_from_conn(conn, TABLE_DIRTY_FILES, path)
    }

    // Gets state from source_files table
    pub fn get_file_state(
        &self,
        conn: &Connection,
        path: &Path,
    ) -> Option<FileState> {
        get_file_state_from_conn(conn, TABLE_SOURCE_FILES, path)
    }

    pub fn register_listener(&self, sender: UnboundedSender<FileTrackerEvent>) {
        self.listener_tx
            .unbounded_send(sender)
            .expect("Failed registering listener")
    }

    pub async fn stop(&self) {
        if self.is_running() {
            self.stopping_event.notify(std::usize::MAX);
            self.listener_rx.lock().await;
            assert!(!self.is_running.load(Ordering::Acquire));
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    pub async fn run(&self) {
        if self
            .is_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let (watcher_tx, watcher_rx) = unbounded();
        let to_watch: Vec<PathBuf> = self.get_watch_dirs();

        // NOTE(happens): If we can't watch the dir, we want to abort
        let mut watcher =
            watcher::DirWatcher::from_path_iter(to_watch.iter().map(|p| Path::new(p)), watcher_tx)
                .expect("watcher: Failed to watch specified path");

        let stop_handle = watcher.stop_handle();
        thread::spawn(move || watcher.run());

        let mut listeners = ListenersList::new();
        let mut scan_stack = Vec::new();

        let stopping = self.stopping_event.listen().fuse();

        let mut listener_rx_guard = self.listener_rx.lock().await;

        let listener_rx = listener_rx_guard.get_mut().fuse();
        let watcher_rx = watcher_rx.fuse();

        futures::pin_mut!(watcher_rx, listener_rx, stopping);

        let mut dirty = false;

        #[allow(unused_mut)]
        loop {
            let delay = futures::FutureExt::fuse(async_io::Timer::after(Duration::from_millis(40)));

            futures::pin_mut!(delay);

            futures::select! {
                // Received a new listener, register it
                new_listener = listener_rx.next() => listeners.register(new_listener),

                // some time has passed since our previous writes to the DB, send the update event
                _ = delay => {
                    if dirty {
                        listeners.send_event(FileTrackerEvent::Update);
                        dirty = false;
                    }
                },
                // Gather all events from the watcher into a transaction and commit it. This will
                // set the dirty flag
                mut maybe_file_event = watcher_rx.next() => {
                    if maybe_file_event.is_none() {
                        debug!("FileTracker: stopping due to exhausted watcher");
                        break;
                    }

                    let mut txn = self.get_rw_txn().await;

                    // batch watcher events into single transaction and update
                    while let Some(file_event) = maybe_file_event {
                        match events::handle_file_event(&mut txn, file_event, &mut scan_stack, &self.watch_dirs) {
                            Ok(Some(evt)) => listeners.send_event(evt),
                            Ok(None) => {},
                            Err(err) => panic!("Error while handling file event: {}", err),
                        }

                        maybe_file_event = watcher_rx.next().now_or_never().flatten();
                    }

                    // If there were any operations that caused real changes, commit the
                    // transaction and set the dirty flag, which will kick an update when we stop
                    // receiving events
                    if txn.dirty {
                        txn.commit().expect("Failed to commit");
                        dirty = true;
                    }
                }
                // Terminate this task
                _ = &mut stopping => {
                    debug!("FileTracker: stopping due to stop() notification");
                    break;
                }
            }
        }

        listeners.send_event(FileTrackerEvent::Update);
        drop(stop_handle);
        self.is_running.store(false, Ordering::Release);
    }
}

#[cfg(not(target_os = "macos"))] // FIXME: these tests fail in macos CI
#[cfg(test)]
pub mod tests {

    use std::{
        fs,
        future::Future,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };

    use super::*;
    use crate::{
        db::Database,
        file_tracker::{FileTracker, FileTrackerEvent},
        timeout::timeout,
    };

    pub async fn with_tracker<F, T>(f: F)
    where
        T: Future<Output = ()>,
        F: FnOnce(Arc<FileTracker>, UnboundedReceiver<FileTrackerEvent>, PathBuf) -> T,
    {
        let _ = crate::init_logging();
        let db_dir = tempfile::tempdir().unwrap();
        let asset_dir = tempfile::tempdir().unwrap();

        let _ = fs::create_dir(db_dir.path());
        let asset_paths = vec![asset_dir.path().to_str().unwrap()];
        let db = Arc::new(
            Database::new(db_dir.path()).unwrap_or_else(|_| {
                panic!(
                    "failed to create db {}",
                    db_dir.path().to_string_lossy()
                )
            }),
        );
        let tracker = Arc::new(FileTracker::new(db, asset_paths));
        let (tx, mut rx) = unbounded();
        tracker.register_listener(tx);

        let tracker_clone = tracker.clone();

        let runtime = async_executor::Executor::new();
        let handle = runtime.spawn(async move { tracker_clone.run().await });
        expect_event(&mut rx).await;

        f(tracker.clone(), rx, asset_dir.into_path()).await;

        tracker.stop().await;
        handle.await;
    }

    async fn expect_no_event(rx: &mut UnboundedReceiver<FileTrackerEvent>) {
        match timeout(Duration::from_millis(1000), rx.next()).await {
            Err(_) => {}
            Ok(evt) => panic!("Received unexpected event {:?}", evt),
        }
    }

    async fn expect_event(rx: &mut UnboundedReceiver<FileTrackerEvent>) -> FileTrackerEvent {
        match timeout(Duration::from_millis(10000), rx.next()).await {
            Err(_) => panic!("Timed out waiting for file event"),
            Ok(evt) => evt.unwrap(),
        }
    }

    async fn expect_no_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_rw_txn().await;
        let canonical_path = canonicalize_path(&asset_dir.join(name));

        assert!(
            t.get_file_state(txn.conn(), &canonical_path).is_none(),
            "expected no file state for file {}",
            name
        );
    }

    async fn expect_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_rw_txn().await;
        let canonical_path = canonicalize_path(&asset_dir.join(name));

        t.get_file_state(txn.conn(), &canonical_path)
            .unwrap_or_else(|| panic!("expected file state for file {}", name));
    }

    pub async fn add_test_dir(asset_dir: &Path, name: &str) -> PathBuf {
        let path = PathBuf::from(asset_dir).join(name);
        async_fs::create_dir(&path).await.expect("create dir");
        path
    }

    pub async fn add_test_file(asset_dir: &Path, name: &str) {
        async_fs::copy(
            PathBuf::from("tests/file_tracker/").join(name),
            asset_dir.join(name),
        )
        .await
        .expect("copy test file");
    }

    #[cfg(target_family = "windows")]
    pub fn add_symlink_file<P: AsRef<Path>, Q: AsRef<Path>, R: AsRef<Path>>(
        asset_dir: &P,
        name: &Q,
        target: &R,
    ) {
        match std::os::windows::fs::symlink_file(
            target,
            PathBuf::from(asset_dir.as_ref()).join(name),
        ) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            err => panic!("failed to create symlink file: {:?}", err),
        }
    }

    #[cfg(target_family = "unix")]
    pub fn add_symlink_file<P: AsRef<Path>, Q: AsRef<Path>, R: AsRef<Path>>(
        asset_dir: &P,
        name: &Q,
        target: &R,
    ) {
        match std::os::unix::fs::symlink(target, PathBuf::from(asset_dir.as_ref()).join(name)) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            err => panic!("failed to create symlink file: {:?}", err),
        }
    }

    #[cfg(target_family = "windows")]
    pub fn add_symlink_dir<P: AsRef<Path>, Q: AsRef<Path>, R: AsRef<Path>>(
        asset_dir: &P,
        name: &Q,
        target: &R,
    ) {
        match std::os::windows::fs::symlink_dir(
            target,
            PathBuf::from(asset_dir.as_ref()).join(name),
        ) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            err => panic!("failed to create symlink file: {:?}", err),
        }
    }

    #[cfg(target_family = "unix")]
    pub fn add_symlink_dir<P: AsRef<Path>, Q: AsRef<Path>, R: AsRef<Path>>(
        asset_dir: &P,
        name: &Q,
        target: &R,
    ) {
        match std::os::unix::fs::symlink(target, PathBuf::from(asset_dir.as_ref()).join(name)) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            err => panic!("failed to create symlink file: {:?}", err),
        }
    }

    async fn expect_dirty_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_rw_txn().await;
        let path = canonicalize_path(&PathBuf::from(asset_dir));
        let canonical_path = path.join(name);
        t.get_dirty_file_state(txn.conn(), &canonical_path)
            .unwrap_or_else(|| panic!("expected dirty file state for file {}", name));
    }

    async fn clear_dirty_file_state(t: &FileTracker) {
        let mut txn = t.get_rw_txn().await;
        for f in t.read_dirty_files(txn.conn()) {
            t.delete_dirty_file_state(&mut txn, &f.path);
        }
    }

    #[futures_test::test]
    #[ignore]
    async fn test_create_file() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_test_file(&asset_dir, "test.txt").await;
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
        })
        .await;
    }

    #[futures_test::test]
    #[ignore]
    async fn test_modify_file() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_test_file(&asset_dir, "test.txt").await;
            expect_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
            clear_dirty_file_state(&t).await;

            async_fs::File::create(asset_dir.join("test.txt"))
                .await
                .expect("truncate test file");

            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
        })
        .await;
    }

    #[futures_test::test]
    #[ignore]
    async fn test_delete_file() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_test_file(&asset_dir, "test.txt").await;
            expect_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
            clear_dirty_file_state(&t).await;

            async_fs::remove_file(asset_dir.join("test.txt"))
                .await
                .expect("test file could not be deleted");

            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_no_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
        })
        .await;
    }

    #[ignore]
    #[futures_test::test]
    async fn test_create_dir() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_test_dir(&asset_dir, "testdir").await;
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "testdir").await;
            expect_dirty_file_state(&t, &asset_dir, "testdir").await;
        })
        .await;
    }

    #[futures_test::test]
    #[ignore]
    async fn test_create_file_in_dir() {
        with_tracker(|t, mut rx, asset_dir| async move {
            let dir = add_test_dir(&asset_dir, "testdir").await;

            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "testdir").await;
            expect_dirty_file_state(&t, &asset_dir, "testdir").await;

            add_test_file(&dir, "test.txt").await;
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &dir, "test.txt").await;
            expect_dirty_file_state(&t, &dir, "test.txt").await;
        })
        .await;
    }

    #[futures_test::test]
    #[ignore]
    async fn test_create_emacs_lockfile() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_symlink_file(
                &asset_dir,
                &"emacs.symlink".to_string(),
                &"emacs@lock.file:buffer".to_string(),
            );
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "emacs.symlink").await;
            expect_dirty_file_state(&t, &asset_dir, "emacs.symlink").await;
            assert!(t.get_watch_dirs() == vec![asset_dir])
        })
        .await;
    }

    #[futures_test::test]
    #[ignore]
    async fn test_create_symlink_dir() {
        with_tracker(|t, mut rx, asset_dir| {
            async move {
                let watch_dir = tempfile::tempdir().unwrap();
                add_symlink_dir(&asset_dir, &"dir_symlink".to_string(), &watch_dir);
                expect_event(&mut rx).await;
                expect_event(&mut rx).await;
                expect_no_event(&mut rx).await;
                expect_file_state(&t, &asset_dir, "dir_symlink").await;
                expect_dirty_file_state(&t, &asset_dir, "dir_symlink").await;
                assert!(t.get_watch_dirs() == vec![asset_dir, watch_dir.path().to_path_buf()]);

                // add file in the newly watched dir
                add_test_file(watch_dir.path(), "test.txt").await;
                expect_event(&mut rx).await;
                expect_no_event(&mut rx).await;
                expect_file_state(&t, watch_dir.path(), "test.txt").await;
                expect_dirty_file_state(&t, watch_dir.path(), "test.txt").await;
            }
        })
        .await;
    }

    #[futures_test::test]
    #[ignore]
    async fn test_delete_symlink_dir() {
        with_tracker(|t, mut rx, asset_dir| async move {
            let watch_dir = tempfile::tempdir().unwrap();
            add_symlink_dir(&asset_dir, &"dir_symlink".to_string(), &watch_dir);
            expect_event(&mut rx).await;
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "dir_symlink").await;
            expect_dirty_file_state(&t, &asset_dir, "dir_symlink").await;
            assert!(t.get_watch_dirs() == vec![asset_dir.clone(), watch_dir.path().to_path_buf()]);

            #[cfg(target_family = "windows")]
            async_fs::remove_dir(asset_dir.join("dir_symlink"))
                .await
                .expect("test file could not be deleted");
            #[cfg(target_family = "unix")]
            async_fs::remove_file(asset_dir.join("dir_symlink"))
                .await
                .expect("test file could not be deleted");

            expect_event(&mut rx).await;
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_no_file_state(&t, &asset_dir, "dir_symlink").await;
            expect_dirty_file_state(&t, &asset_dir, "dir_symlink").await;
            assert!(t.get_watch_dirs() == vec![asset_dir]);
        })
        .await;
    }
}
