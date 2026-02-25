use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
    sync::mpsc::{channel, Receiver, Sender},
    time::UNIX_EPOCH,
};

use distill_core::utils::canonicalize_path;
use notify::{
    event::{ModifyKind, RenameMode},
    Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};

use crate::error::{Error, Result};
use futures::channel::mpsc::UnboundedSender;

// Internal filesystem operation, replacing notify v4's DebouncedEvent
enum FsOp {
    Create(PathBuf),
    Write(PathBuf),
    Rename(PathBuf, PathBuf),
    Remove(PathBuf),
    Rescan,
}

// Message type for the internal channel
enum WatcherMsg {
    FsOp(FsOp),
    Stop,
}

fn convert_notify_event(event: notify::Event) -> Vec<FsOp> {
    match event.kind {
        EventKind::Create(_) => event.paths.into_iter().map(FsOp::Create).collect(),
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
            if event.paths.len() >= 2 {
                vec![FsOp::Rename(
                    event.paths[0].clone(),
                    event.paths[1].clone(),
                )]
            } else {
                vec![]
            }
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
            event.paths.into_iter().map(FsOp::Remove).collect()
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
            event.paths.into_iter().map(FsOp::Create).collect()
        }
        EventKind::Modify(_) => event.paths.into_iter().map(FsOp::Write).collect(),
        EventKind::Remove(_) => event.paths.into_iter().map(FsOp::Remove).collect(),
        EventKind::Any => event.paths.into_iter().map(FsOp::Create).collect(),
        EventKind::Access(_) | EventKind::Other => vec![],
    }
}

// Wraps the notify crate:
// - supports symlinks
// - encapsulates notify so other systems don't use it directly
// - generates update messages for all files on startup
/// The purpose of DirWatcher is to provide enough information to
/// determine which files may be candidates for going through the asset import process.
/// It handles updating watches for directories behind symlinks and scans directories on create/delete.
pub struct DirWatcher {
    // the watcher object from the notify crate
    watcher: RecommendedWatcher,

    // Symlinks we have discovered and where they point
    symlink_map: HashMap<PathBuf, PathBuf>,

    // Refcount of all dirs being watched. We call unwatch on the watcher when it hits 0
    watch_refs: HashMap<PathBuf, i32>,

    // All dirs being watched
    dirs: Vec<PathBuf>,

    // Channel for events from the watcher (and we insert events ourselves to indicate shutdown)
    rx: Receiver<WatcherMsg>,
    tx: Sender<WatcherMsg>,

    // The final output of this struct goes here
    asset_tx: UnboundedSender<FileEvent>,
}

// When dropped, sends a stop message via the same channel we receive messages from notify
pub struct StopHandle {
    tx: Sender<WatcherMsg>,
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_type: fs::FileType,
    pub last_modified: u64,
    pub length: u64,
}
#[derive(Debug)]
pub enum FileEvent {
    Updated(PathBuf, FileMetadata),
    Renamed(PathBuf, PathBuf, FileMetadata),
    Removed(PathBuf),
    FileError(Error),
    // ScanStart is called when a directory is about to be scanned.
    // Scanning can be recursive
    ScanStart(PathBuf),
    // ScanEnd indicates the end of a scan. The set of all watched directories is also sent
    ScanEnd(PathBuf, Vec<PathBuf>),
    // Watch indicates that a new directory is being watched, probably due to a symlink being created
    Watch(PathBuf),
    // Unwatch indicates that a new directory has stopped being watched, probably due to a symlink being removed
    Unwatch(PathBuf),
}

// Sanitizes file metadata
pub(crate) fn file_metadata(metadata: &fs::Metadata) -> FileMetadata {
    let modify_time = metadata.modified().unwrap_or(UNIX_EPOCH);
    let since_epoch = modify_time
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let in_ms = since_epoch.as_secs() * 1000 + u64::from(since_epoch.subsec_nanos()) / 1_000_000;
    FileMetadata {
        file_type: metadata.file_type(),
        length: metadata.len(),
        last_modified: in_ms,
    }
}

/// Returns true if the error indicates the path cannot be watched
/// (doesn't exist, not a file/dir, etc.)
fn is_not_watchable(err: &Error) -> bool {
    match err {
        Error::Notify(e) => matches!(
            &e.kind,
            notify::ErrorKind::PathNotFound
                | notify::ErrorKind::Generic(_)
                | notify::ErrorKind::Io(_)
        ),
        Error::IO(e) => e.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

impl DirWatcher {
    // Starts a watcher running on the given paths
    pub fn from_path_iter<'a, T>(paths: T, chan: UnboundedSender<FileEvent>) -> Result<DirWatcher>
    where
        T: IntoIterator<Item = &'a Path>,
    {
        let (tx, rx) = channel();
        let tx_clone = tx.clone();
        let watcher = RecommendedWatcher::new(
            move |res: notify::Result<notify::Event>| match res {
                Ok(event) => {
                    for op in convert_notify_event(event) {
                        let _ = tx_clone.send(WatcherMsg::FsOp(op));
                    }
                }
                Err(_) => {
                    let _ = tx_clone.send(WatcherMsg::FsOp(FsOp::Rescan));
                }
            },
            Config::default(),
        )?;

        let mut asset_watcher = DirWatcher {
            watcher,
            symlink_map: HashMap::new(),
            watch_refs: HashMap::new(),
            dirs: Vec::new(),
            rx,
            tx,
            asset_tx: chan,
        };

        for path in paths {
            let path = PathBuf::from(path);
            let path = if path.is_relative() {
                std::env::current_dir()?.join(path)
            } else {
                path
            };
            let path = canonicalize_path(&path);
            asset_watcher.watch(&path)?;
        }

        Ok(asset_watcher)
    }

    // Returns a StopHandle that will halt watching when it's dropped
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            tx: self.tx.clone(),
        }
    }

    // Visit all files, call handle_fs_op() passing the event created by the provided callback
    fn scan_directory<F>(&mut self, dir: &Path, evt_create: &F) -> Result<()>
    where
        F: Fn(PathBuf) -> FsOp,
    {
        let canonical_dir = canonicalize_path(dir);
        self.asset_tx
            .unbounded_send(FileEvent::ScanStart(canonical_dir.clone()))
            .map_err(|_| Error::SendError)?;
        let result = self.scan_directory_recurse(&canonical_dir, evt_create);
        self.asset_tx
            .unbounded_send(FileEvent::ScanEnd(canonical_dir, self.dirs.clone()))
            .map_err(|_| Error::SendError)?;
        result
    }

    fn scan_directory_recurse<F>(&mut self, dir: &Path, evt_create: &F) -> Result<()>
    where
        F: Fn(PathBuf) -> FsOp,
    {
        match fs::read_dir(dir) {
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::IO(e)),
            Ok(dir_entry) => {
                for entry in dir_entry {
                    match entry {
                        Err(ref e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(Error::IO(e)),
                        Ok(entry) => {
                            let evt = self.handle_fs_op(evt_create(entry.path()), true)?;
                            if let Some(evt) = evt {
                                self.asset_tx
                                    .unbounded_send(evt)
                                    .map_err(|_| Error::SendError)?;
                            }
                            let metadata;
                            match entry.metadata() {
                                Err(ref e) if e.kind() == io::ErrorKind::NotFound => continue,
                                Err(e) => return Err(Error::IO(e)),
                                Ok(m) => metadata = m,
                            }
                            let is_dir = metadata.is_dir();
                            if is_dir {
                                self.scan_directory_recurse(&entry.path(), evt_create)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn run(&mut self) {
        // Emit a Create for every path recursively
        for dir in &self.dirs.clone() {
            if let Err(err) = self.scan_directory(dir, &FsOp::Create) {
                self.asset_tx
                    .unbounded_send(FileEvent::FileError(err))
                    .expect("Failed to send file error event. Ironic...");
            }
        }

        loop {
            match self.rx.recv() {
                Ok(WatcherMsg::FsOp(op)) => match self.handle_fs_op(op, false) {
                    // By default, just proxy the event into the channel
                    Ok(maybe_event) => {
                        if let Some(evt) = maybe_event {
                            log::debug!("File event: {:?}", evt);
                            self.asset_tx.unbounded_send(evt).unwrap();
                        }
                    }
                    Err(err) => match err {
                        // If notify cannot assure correct modification events (sometimes due to overflowing a queue,
                        // may be a limit on the OS) we have to reset to good state by scanning everything
                        Error::RescanRequired => {
                            for dir in &self.dirs.clone() {
                                if let Err(err) =
                                    self.scan_directory(dir, &FsOp::Create)
                                {
                                    self.asset_tx
                                        .unbounded_send(FileEvent::FileError(err))
                                        .expect("Failed to send file error event");
                                }
                            }
                        }
                        Error::Exit => break,
                        _ => self
                            .asset_tx
                            .unbounded_send(FileEvent::FileError(err))
                            .expect("Failed to send file error event"),
                    },
                },
                Ok(WatcherMsg::Stop) => break,
                Err(_) => {
                    self.asset_tx
                        .unbounded_send(FileEvent::FileError(Error::RecvError))
                        .expect("Failed to send file error event");

                    return;
                }
            }
        }
    }

    fn watch(&mut self, path: &Path) -> Result<bool> {
        let refs = *self.watch_refs.get(path).unwrap_or(&0);
        match refs {
            0 => {
                self.watcher.watch(path, RecursiveMode::Recursive)?;
                self.dirs.push(path.to_path_buf());
                self.watch_refs.insert(path.to_path_buf(), 1);
                return Ok(true);
            }
            refs if refs > 0 => {
                self.watch_refs
                    .entry(path.to_path_buf())
                    .and_modify(|r| *r += 1);
            }
            _ => {}
        }
        Ok(false)
    }

    fn unwatch(&mut self, path: &Path) -> Result<bool> {
        let refs = *self.watch_refs.get(path).unwrap_or(&0);
        if refs == 1 {
            self.watcher.unwatch(path)?;
            for i in 0..self.dirs.len() {
                if *path == self.dirs[i] {
                    self.dirs.remove(i);
                    break;
                }
            }
            self.watch_refs.remove(path);
            return Ok(true);
        } else if refs > 0 {
            self.watch_refs
                .entry(path.to_path_buf())
                .and_modify(|r| *r -= 1);
        }
        Ok(false)
    }

    // Called when we receive about changes, most of the time we will detect that the path is not
    // a symlink and do nothing
    fn handle_updated_symlink(
        &mut self,
        src: Option<&PathBuf>,
        dst: Option<&PathBuf>,
    ) -> Result<bool> {
        let mut recognized_symlink = false;
        if let Some(src) = src {
            if self.symlink_map.contains_key(src) {
                // This was previous a symlink. Remove the watch, and maybe clean up the watch
                let to_unwatch = self.symlink_map[src].clone();
                match self.unwatch(&to_unwatch) {
                    Ok(unwatched) => {
                        if unwatched {
                            self.scan_directory(&to_unwatch, &FsOp::Remove)?;
                        }
                        self.symlink_map.remove(src);
                        self.asset_tx
                            .unbounded_send(FileEvent::Unwatch(to_unwatch))
                            .map_err(|_| Error::SendError)?;
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
                recognized_symlink = true;
            }
        }
        if let Some(dst) = dst {
            let link = fs::read_link(dst);
            if let Ok(link_path) = link {
                let link_path = canonicalize_path(&dst.join(link_path));
                match self.watch(&link_path) {
                    Ok(watched) => {
                        if watched {
                            self.scan_directory(&link_path, &FsOp::Create)?;
                        }
                        self.symlink_map.insert(dst.clone(), link_path.clone());
                        self.asset_tx
                            .unbounded_send(FileEvent::Watch(link_path))
                            .map_err(|_| Error::SendError)?;
                    }
                    Err(err) if is_not_watchable(&err) => {
                        // skip the symlink if it's not a valid path or no longer exists
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
                recognized_symlink = true;
            }
        }
        Ok(recognized_symlink)
    }

    // Handles filesystem operations from notify or from calling scan_directory()
    fn handle_fs_op(
        &mut self,
        op: FsOp,
        is_scanning: bool,
    ) -> Result<Option<FileEvent>> {
        match op {
            FsOp::Create(path) | FsOp::Write(path) => {
                let path = canonicalize_path(&path);
                self.handle_updated_symlink(Option::None, Some(&path))?;

                // Try to get info on this file, following the symlink if necessary
                let metadata_result = match fs::metadata(&path) {
                    Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(ref e) if e.kind() == io::ErrorKind::PermissionDenied => {
                        log::warn!(
                            "Permission denied when fetching metadata for create event {:?}",
                            path,
                        );
                        Ok(None)
                    }
                    Err(e) => Err(Error::IO(e)),
                    Ok(metadata) => Ok(Some(FileEvent::Updated(
                        path.clone(),
                        file_metadata(&metadata),
                    ))),
                };

                // If metadata hit NotFound/PermissionDenied, get metadata without trying to follow symlink
                match metadata_result {
                    Ok(None) => {
                        if let Ok(metadata) = fs::symlink_metadata(&path) {
                            Ok(Some(FileEvent::Updated(path, file_metadata(&metadata))))
                        } else {
                            Ok(None)
                        }
                    }
                    result => result,
                }
            }
            FsOp::Rename(src, dest) => {
                let src = canonicalize_path(&src);
                let dest = canonicalize_path(&dest);
                self.handle_updated_symlink(Some(&src), Some(&dest))?;
                match fs::metadata(&dest) {
                    Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(ref e) if e.kind() == io::ErrorKind::PermissionDenied => {
                        log::warn!(
                            "Permission denied when fetching metadata for rename event {:?}->{:?}",
                            src,
                            dest
                        );
                        Ok(None)
                    }
                    Err(e) => Err(Error::IO(e)),
                    Ok(metadata) => {
                        if metadata.is_dir() && !is_scanning {
                            self.scan_directory(&dest, &|p| {
                                let replaced = canonicalize_path(&src.join(
                                    p.strip_prefix(&dest).expect("Failed to strip prefix dir"),
                                ));
                                FsOp::Rename(replaced, p)
                            })?;
                        }
                        Ok(Some(FileEvent::Renamed(
                            src,
                            dest,
                            file_metadata(&metadata),
                        )))
                    }
                }
            }
            FsOp::Remove(path) => {
                let path = canonicalize_path(&path);
                self.handle_updated_symlink(Some(&path), Option::None)?;
                Ok(Some(FileEvent::Removed(path)))
            }
            FsOp::Rescan => Err(Error::RescanRequired),
        }
    }
}

impl Drop for StopHandle {
    fn drop(&mut self) {
        let _ = self.tx.send(WatcherMsg::Stop);
    }
}
