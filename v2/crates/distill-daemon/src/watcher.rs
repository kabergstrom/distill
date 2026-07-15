//! Lossless native watcher batching for incremental reconciliation.
//!
//! Native paths are invalidation addresses, never namespace authority. The
//! coordinator reopens every affected path through `RootedScanner`. Events
//! arriving during startup remain owned by that scan until it commits; only a
//! native overflow or incomplete event discards the batch and requests a full
//! recovery scan.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use notify::event::{ModifyKind, RenameMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::scanner::RootedScanner;

const DEFAULT_CAPACITY: usize = 65_536;
const ROOT_RECONFIGURE_POLL: Duration = Duration::from_millis(40);
const INCOMPLETE_RENAME_BATCH_LIMIT: u8 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatcherRename {
    pub from: PathBuf,
    pub to: PathBuf,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatcherBatch {
    pub paths: Vec<PathBuf>,
    pub renames: Vec<WatcherRename>,
}

impl WatcherBatch {
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty() && self.renames.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatcherAction {
    None,
    Batch(WatcherBatch),
    FullRescan,
    Failed(String),
}

pub struct WatcherQueue {
    capacity: usize,
    scanning: bool,
    overflowed: bool,
    failed: Option<String>,
    paths: BTreeSet<PathBuf>,
    renames: Vec<WatcherRename>,
    rename_from: BTreeMap<usize, PathBuf>,
    incomplete_rename_batches: u8,
}

impl Default for WatcherQueue {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }
}

impl WatcherQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            scanning: false,
            overflowed: false,
            failed: None,
            paths: BTreeSet::new(),
            renames: Vec::new(),
            rename_from: BTreeMap::new(),
            incomplete_rename_batches: 0,
        }
    }

    /// Begin startup/recovery traversal without clearing events already seen
    /// by the synchronously armed native watcher.
    pub fn arm_scan(&mut self) {
        assert!(!self.scanning, "watcher scan already armed");
        self.scanning = true;
    }

    /// Finish a startup/recovery traversal. The live loop cannot consume this
    /// batch while `scanning` is true.
    pub fn finish_scan(&mut self) -> WatcherAction {
        assert!(self.scanning, "no watcher scan armed");
        self.scanning = false;
        self.take_action()
    }

    pub fn take_live_action(&mut self) -> WatcherAction {
        if self.scanning {
            WatcherAction::None
        } else {
            self.take_action()
        }
    }

    /// Native overflow, an event without usable paths, or incomplete watcher
    /// reconfiguration invalidates the whole event union.
    pub fn force_rescan(&mut self) {
        self.paths.clear();
        self.renames.clear();
        self.rename_from.clear();
        self.incomplete_rename_batches = 0;
        self.overflowed = true;
    }

    /// A native watch can no longer provide complete coverage. This is a hard
    /// failure, not a request to spin on complete scans while edits go unseen.
    pub fn fail(&mut self, message: impl Into<String>) {
        self.paths.clear();
        self.renames.clear();
        self.rename_from.clear();
        self.incomplete_rename_batches = 0;
        self.overflowed = false;
        self.failed = Some(message.into());
    }

    pub fn requeue_action(&mut self, action: WatcherAction) {
        match action {
            WatcherAction::None => {}
            WatcherAction::FullRescan => self.force_rescan(),
            WatcherAction::Failed(message) => self.fail(message),
            WatcherAction::Batch(batch) => {
                self.paths.extend(batch.paths);
                // A failed older batch must remain before any events that
                // arrived while it was being processed.
                let mut restored = batch.renames;
                restored.append(&mut self.renames);
                self.renames = restored;
                for rename in &self.renames {
                    self.paths.insert(rename.from.clone());
                    self.paths.insert(rename.to.clone());
                }
                self.check_capacity();
            }
        }
    }

    /// Testable native-event ingestion. Access-only events are ignored;
    /// imprecise mutation events without a path become recovery scans.
    pub fn push_native(&mut self, event: Event) {
        if event.need_rescan() {
            self.force_rescan();
            return;
        }
        if matches!(event.kind, EventKind::Access(_)) {
            return;
        }
        if event.paths.is_empty() {
            self.force_rescan();
            return;
        }

        let tracker = event.tracker();
        match event.kind {
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)) if event.paths.len() >= 2 => {
                self.push_rename(event.paths[0].clone(), event.paths[1].clone());
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                match (tracker, event.paths.first()) {
                    (Some(tracker), Some(path)) => {
                        self.rename_from.insert(tracker, path.clone());
                        self.incomplete_rename_batches = 0;
                    }
                    _ => self.force_rescan(),
                }
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                match (tracker, event.paths.first()) {
                    (Some(tracker), Some(to)) => match self.rename_from.remove(&tracker) {
                        Some(from) => {
                            self.push_rename(from, to.clone());
                            self.incomplete_rename_batches = 0;
                        }
                        None => self.force_rescan(),
                    },
                    _ => self.force_rescan(),
                }
            }
            _ => {}
        }
        for path in event.paths {
            self.paths.insert(path);
        }
        self.check_capacity();
    }

    fn push_invalidations(&mut self, paths: impl IntoIterator<Item = PathBuf>) {
        self.paths.extend(paths);
        self.check_capacity();
    }

    fn push_rename(&mut self, from: PathBuf, to: PathBuf) {
        self.paths.insert(from.clone());
        self.paths.insert(to.clone());
        self.renames.push(WatcherRename { from, to });
    }

    fn check_capacity(&mut self) {
        let retained = self.paths.len() + self.renames.len() + self.rename_from.len();
        if retained > self.capacity {
            self.force_rescan();
        }
    }

    fn take_action(&mut self) -> WatcherAction {
        if let Some(message) = self.failed.take() {
            return WatcherAction::Failed(message);
        }
        if std::mem::take(&mut self.overflowed) {
            self.paths.clear();
            self.renames.clear();
            self.rename_from.clear();
            return WatcherAction::FullRescan;
        }
        if !self.rename_from.is_empty() {
            self.incomplete_rename_batches = self.incomplete_rename_batches.saturating_add(1);
            if self.incomplete_rename_batches >= INCOMPLETE_RENAME_BATCH_LIMIT {
                self.force_rescan();
                return self.take_action();
            }
            return WatcherAction::None;
        }
        self.incomplete_rename_batches = 0;
        let batch = WatcherBatch {
            paths: std::mem::take(&mut self.paths).into_iter().collect(),
            renames: std::mem::take(&mut self.renames),
        };
        if batch.is_empty() {
            WatcherAction::None
        } else {
            WatcherAction::Batch(batch)
        }
    }
}

#[derive(Debug)]
pub enum WatcherStartError {
    Notify(notify::Error),
    Thread(std::io::Error),
}

impl std::fmt::Display for WatcherStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Notify(error) => write!(formatter, "native watcher: {error}"),
            Self::Thread(error) => write!(formatter, "native watcher thread: {error}"),
        }
    }
}

impl std::error::Error for WatcherStartError {}

impl From<notify::Error> for WatcherStartError {
    fn from(error: notify::Error) -> Self {
        Self::Notify(error)
    }
}

/// Owns the native watcher and its root-reconfiguration monitor. Initial roots
/// are registered synchronously before `start` returns.
pub struct WatcherThread {
    stop: Arc<AtomicBool>,
    control: WatcherControl,
    thread: Option<JoinHandle<()>>,
}

enum WatcherCommand {
    ReplaceControlPaths {
        paths: BTreeSet<PathBuf>,
        reply: mpsc::SyncSender<Result<(), String>>,
    },
    Stop,
}

#[derive(Clone)]
pub(crate) struct WatcherControl {
    commands: mpsc::SyncSender<WatcherCommand>,
}

impl WatcherControl {
    pub(crate) fn replace_paths(
        &self,
        paths: impl IntoIterator<Item = PathBuf>,
    ) -> Result<(), String> {
        let (reply, result) = mpsc::sync_channel(1);
        self.commands
            .send(WatcherCommand::ReplaceControlPaths {
                paths: paths.into_iter().collect(),
                reply,
            })
            .map_err(|_| "native watcher control thread stopped".to_owned())?;
        result
            .recv()
            .map_err(|_| "native watcher control reply was dropped".to_owned())?
    }
}

#[derive(Debug, Default)]
struct WatchCoverage {
    asset_roots: BTreeSet<PathBuf>,
    quarantine_prefixes: BTreeSet<PathBuf>,
    control_paths: ControlPathMap,
}

type ControlPathMap = BTreeMap<PathBuf, BTreeSet<PathBuf>>;

struct ControlCoverage {
    directories: BTreeSet<PathBuf>,
    paths: ControlPathMap,
}

impl WatchCoverage {
    fn asset_path(&self, path: &Path) -> bool {
        self.asset_roots.iter().any(|root| path.starts_with(root))
            && !self
                .quarantine_prefixes
                .iter()
                .any(|quarantine| path.starts_with(quarantine))
    }
}

impl WatcherThread {
    pub fn start(
        scanner: RootedScanner,
        control_paths: impl IntoIterator<Item = PathBuf>,
        queue: Arc<Mutex<WatcherQueue>>,
    ) -> Result<Self, WatcherStartError> {
        let control_paths = control_paths.into_iter().collect::<BTreeSet<_>>();
        let (asset_roots, quarantine_prefixes) = scanner.watch_coverage();
        let asset_roots = asset_roots.into_iter().collect::<BTreeSet<_>>();
        let quarantine_prefixes = quarantine_prefixes.into_iter().collect::<BTreeSet<_>>();
        let initial_control = control_coverage(&control_paths)
            .map_err(|message| WatcherStartError::Thread(std::io::Error::other(message)))?;
        let coverage = Arc::new(Mutex::new(WatchCoverage {
            asset_roots: asset_roots.clone(),
            quarantine_prefixes,
            control_paths: initial_control.paths,
        }));
        let callback_queue = Arc::clone(&queue);
        let callback_coverage = Arc::clone(&coverage);
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<Event>| match result {
                Ok(event) => ingest_native(&callback_queue, &callback_coverage, event),
                // notify represents incomplete event delivery with an explicit
                // Rescan flag on an Event.  Callback errors instead mean the
                // backend can no longer prove coverage (watch loss, resource
                // exhaustion, or I/O failure); a root scan cannot repair that
                // native subscription.
                Err(error) => lock_queue(&callback_queue)
                    .fail(format!("native watcher coverage failed: {error}")),
            },
            Config::default().with_follow_symlinks(false),
        )?;
        let mut watched_assets = asset_roots;
        let mut watched_controls = initial_control.directories;
        for root in &watched_assets {
            watcher.watch(root, RecursiveMode::Recursive)?;
        }
        for root in &watched_controls {
            // FSEvents exposes directory watches recursively; exact-path
            // filtering below keeps sibling events out of the work queue.
            watcher.watch(root, RecursiveMode::Recursive)?;
        }
        let mut revision = scanner.revision();
        let (commands, command_rx) = mpsc::sync_channel(1);
        let control = WatcherControl { commands };

        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("distill-watcher".to_owned())
            .spawn(move || {
                let mut watcher = watcher;
                while !thread_stop.load(Ordering::Acquire) {
                    match command_rx.recv_timeout(ROOT_RECONFIGURE_POLL) {
                        Ok(WatcherCommand::Stop) => break,
                        Ok(WatcherCommand::ReplaceControlPaths { paths, reply }) => {
                            let result = control_coverage(&paths).and_then(|next| {
                                replace_watched_directories(
                                    &mut watcher,
                                    &watched_assets,
                                    &watched_controls,
                                    &watched_assets,
                                    &next.directories,
                                )?;
                                watched_controls = next.directories;
                                lock_coverage(&coverage).control_paths = next.paths;
                                Ok(())
                            });
                            let _ = reply.send(result);
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                    let next_revision = scanner.revision();
                    if next_revision == revision {
                        continue;
                    }
                    let (next_assets, next_quarantine) = scanner.watch_coverage();
                    let next_assets = next_assets.into_iter().collect::<BTreeSet<_>>();
                    if let Err(error) = replace_watched_directories(
                        &mut watcher,
                        &watched_assets,
                        &watched_controls,
                        &next_assets,
                        &watched_controls,
                    ) {
                        lock_queue(&queue).fail(format!(
                            "native watcher cannot cover configured asset roots: {error}"
                        ));
                        break;
                    }
                    watched_assets = next_assets.clone();
                    {
                        let mut coverage = lock_coverage(&coverage);
                        coverage.asset_roots = next_assets;
                        coverage.quarantine_prefixes = next_quarantine.into_iter().collect();
                    }
                    revision = next_revision;
                    // The candidate scan preceded watcher reconfiguration.
                    // Force one armed catch-up scan to cover that bounded gap.
                    lock_queue(&queue).force_rescan();
                }
            })
            .map_err(WatcherStartError::Thread)?;
        Ok(Self {
            stop,
            control,
            thread: Some(thread),
        })
    }

    pub(crate) fn control(&self) -> WatcherControl {
        self.control.clone()
    }
}

impl Drop for WatcherThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.control.commands.try_send(WatcherCommand::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn ingest_native(queue: &Mutex<WatcherQueue>, coverage: &Mutex<WatchCoverage>, mut event: Event) {
    if event.need_rescan() {
        lock_queue(queue).force_rescan();
        return;
    }
    if matches!(event.kind, EventKind::Access(_)) {
        return;
    }
    let coverage = lock_coverage(coverage);
    let original_len = event.paths.len();
    let mut assets = Vec::new();
    let mut controls = Vec::new();
    for path in &event.paths {
        if let Some(authority_paths) = coverage.control_paths.get(path) {
            controls.extend(authority_paths.iter().cloned());
        }
        if coverage.asset_path(path) {
            assets.push(path.clone());
        }
    }
    drop(coverage);
    let preserve_native = controls.is_empty() && assets.len() == original_len;
    let mut queue = lock_queue(queue);
    if preserve_native {
        event.paths = assets;
        queue.push_native(event);
    } else {
        assets.extend(controls);
        assets.sort_unstable();
        assets.dedup();
        queue.push_invalidations(assets);
    }
}

fn control_coverage(paths: &BTreeSet<PathBuf>) -> Result<ControlCoverage, String> {
    let mut directories = BTreeSet::new();
    let mut observed = BTreeMap::<PathBuf, BTreeSet<PathBuf>>::new();
    for path in paths {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| format!("watched control path {} has no parent", path.display()))?;
        let mut ancestor = parent;
        let mut suffix = Vec::new();
        while !ancestor.exists() {
            let name = ancestor.file_name().ok_or_else(|| {
                format!(
                    "watched control path {} has no existing ancestor",
                    path.display()
                )
            })?;
            suffix.push(name.to_owned());
            ancestor = ancestor.parent().ok_or_else(|| {
                format!(
                    "watched control path {} has no existing ancestor",
                    path.display()
                )
            })?;
        }
        let directory = std::fs::canonicalize(ancestor)
            .map_err(|error| format!("resolve watch directory {}: {error}", ancestor.display()))?;
        let mut event_path = directory.clone();
        for component in suffix.into_iter().rev() {
            event_path.push(component);
        }
        event_path.push(
            path.file_name().ok_or_else(|| {
                format!("watched control path {} has no file name", path.display())
            })?,
        );
        directories.insert(directory);
        observed.entry(event_path).or_default().insert(path.clone());
    }
    Ok(ControlCoverage {
        directories,
        paths: observed,
    })
}

fn replace_watched_directories(
    watcher: &mut RecommendedWatcher,
    current_assets: &BTreeSet<PathBuf>,
    current_controls: &BTreeSet<PathBuf>,
    next_assets: &BTreeSet<PathBuf>,
    next_controls: &BTreeSet<PathBuf>,
) -> Result<(), String> {
    let current = current_assets
        .union(current_controls)
        .cloned()
        .collect::<BTreeSet<_>>();
    let next = next_assets
        .union(next_controls)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut added = Vec::<PathBuf>::new();
    for root in next.difference(&current) {
        if let Err(error) = watcher.watch(root, RecursiveMode::Recursive) {
            for rollback in added {
                let _ = watcher.unwatch(&rollback);
            }
            return Err(format!("watch {}: {error}", root.display()));
        }
        added.push(root.clone());
    }
    for root in current.difference(&next) {
        let _ = watcher.unwatch(root);
    }
    Ok(())
}

fn lock_queue(queue: &Mutex<WatcherQueue>) -> std::sync::MutexGuard<'_, WatcherQueue> {
    queue
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_coverage(coverage: &Mutex<WatchCoverage>) -> std::sync::MutexGuard<'_, WatchCoverage> {
    coverage
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
