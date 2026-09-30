//! Lossless native watcher batching for incremental reconciliation.
//!
//! Native paths are invalidation addresses, never namespace authority. The
//! coordinator reopens every affected path through `RootedScanner`. Events
//! arriving during startup remain owned by that scan until it commits; only a
//! native overflow or incomplete event discards the batch and requests a full
//! recovery scan.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};

use notify::event::{ModifyKind, RenameMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::scanner::RootedScanner;

const DEFAULT_CAPACITY: usize = 65_536;
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

/// One watcher observation, in arrival order.
#[derive(Debug, Clone)]
pub enum WatcherEvent {
    /// A native event whose paths are all asset paths.
    Native(Event),
    /// Paths to reopen: control files, or an event only partly inside the
    /// asset roots.
    Invalidate(Vec<PathBuf>),
    /// Coverage is incomplete; rescan everything.
    Rescan,
    /// The native watch can no longer prove coverage.
    Failed(String),
}

/// Where the watcher delivers its events (the process loop's inbox).
pub type WatcherSink = Arc<dyn Fn(WatcherEvent) + Send + Sync>;

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

    /// Fold one watcher event into the pending union.
    pub fn push(&mut self, event: WatcherEvent) {
        match event {
            WatcherEvent::Native(event) => self.push_native(event),
            WatcherEvent::Invalidate(paths) => self.push_invalidations(paths),
            WatcherEvent::Rescan => self.force_rescan(),
            WatcherEvent::Failed(message) => self.fail(message),
        }
    }

    /// Whether anything waits to be reconciled.
    pub fn has_pending(&self) -> bool {
        self.overflowed
            || self.failed.is_some()
            || !self.paths.is_empty()
            || !self.renames.is_empty()
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
/// are registered synchronously before `start` returns. The monitor thread
/// owns the watch coverage; native callbacks forward raw events to it.
pub struct WatcherThread {
    control: WatcherControl,
    thread: Option<JoinHandle<()>>,
}

enum WatcherCommand {
    Native(notify::Result<Event>),
    ReplaceControlPaths {
        paths: BTreeSet<PathBuf>,
        reply: mpsc::SyncSender<Result<(), String>>,
    },
    ReplaceAssetRoots {
        assets: BTreeSet<PathBuf>,
        reply: mpsc::SyncSender<Result<(), String>>,
    },
    Stop,
}

#[derive(Clone)]
pub(crate) struct WatcherControl {
    commands: mpsc::Sender<WatcherCommand>,
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

    /// Cover `scanner`'s current roots. When they changed, the watcher
    /// requests one catch-up scan: the candidate scan that installed them
    /// ran before this coverage.
    pub(crate) fn replace_roots(&self, scanner: &RootedScanner) -> Result<(), String> {
        let assets = scanner.watch_coverage();
        let (reply, result) = mpsc::sync_channel(1);
        self.commands
            .send(WatcherCommand::ReplaceAssetRoots {
                assets: assets.into_iter().collect(),
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
    }
}

impl WatcherThread {
    pub fn start(
        scanner: RootedScanner,
        control_paths: impl IntoIterator<Item = PathBuf>,
        sink: WatcherSink,
    ) -> Result<Self, WatcherStartError> {
        let control_paths = control_paths.into_iter().collect::<BTreeSet<_>>();
        let asset_roots = scanner.watch_coverage().into_iter().collect::<BTreeSet<_>>();
        let initial_control = control_coverage(&control_paths)
            .map_err(|message| WatcherStartError::Thread(std::io::Error::other(message)))?;
        let mut coverage = WatchCoverage {
            asset_roots: asset_roots.clone(),
            control_paths: initial_control.paths,
        };
        let (commands, command_rx) = mpsc::channel();
        let native = commands.clone();
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<Event>| {
                let _ = native.send(WatcherCommand::Native(result));
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
        let control = WatcherControl { commands };

        let thread = thread::Builder::new()
            .name("distill-watcher".to_owned())
            .spawn(move || {
                let mut watcher = watcher;
                loop {
                    match command_rx.recv() {
                        Ok(WatcherCommand::Stop) => break,
                        Ok(WatcherCommand::Native(Ok(event))) => {
                            if let Some(event) = admit_native(&coverage, event) {
                                sink(event);
                            }
                        }
                        // notify represents incomplete event delivery with an
                        // explicit Rescan flag on an Event. Callback errors
                        // instead mean the backend can no longer prove
                        // coverage (watch loss, resource exhaustion, or I/O
                        // failure); a root scan cannot repair that native
                        // subscription.
                        Ok(WatcherCommand::Native(Err(error))) => sink(WatcherEvent::Failed(
                            format!("native watcher coverage failed: {error}"),
                        )),
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
                                coverage.control_paths = next.paths;
                                Ok(())
                            });
                            let _ = reply.send(result);
                        }
                        Ok(WatcherCommand::ReplaceAssetRoots { assets, reply }) => {
                            if assets == watched_assets {
                                let _ = reply.send(Ok(()));
                                continue;
                            }
                            if let Err(error) = replace_watched_directories(
                                &mut watcher,
                                &watched_assets,
                                &watched_controls,
                                &assets,
                                &watched_controls,
                            ) {
                                let message = format!(
                                    "native watcher cannot cover configured asset roots: {error}"
                                );
                                sink(WatcherEvent::Failed(message.clone()));
                                let _ = reply.send(Err(message));
                                break;
                            }
                            watched_assets = assets.clone();
                            coverage.asset_roots = assets;
                            // The candidate scan preceded watcher
                            // reconfiguration. Force one armed catch-up scan
                            // to cover that bounded gap.
                            sink(WatcherEvent::Rescan);
                            let _ = reply.send(Ok(()));
                        }
                        Err(mpsc::RecvError) => break,
                    }
                }
            })
            .map_err(WatcherStartError::Thread)?;
        Ok(Self {
            control,
            thread: Some(thread),
        })
    }

    pub(crate) fn control(&self) -> WatcherControl {
        self.control.clone()
    }

    /// See [`WatcherControl::replace_roots`].
    pub fn replace_roots(&self, scanner: &RootedScanner) -> Result<(), String> {
        self.control.replace_roots(scanner)
    }
}

impl Drop for WatcherThread {
    fn drop(&mut self) {
        let _ = self.control.commands.send(WatcherCommand::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Filter one native event through the watch coverage.
fn admit_native(coverage: &WatchCoverage, mut event: Event) -> Option<WatcherEvent> {
    if event.need_rescan() {
        return Some(WatcherEvent::Rescan);
    }
    if matches!(event.kind, EventKind::Access(_)) {
        return None;
    }
    let original_len = event.paths.len();
    let mut assets = Vec::new();
    let mut controls = Vec::new();
    for path in &event.paths {
        for (observed_path, authority_paths) in &coverage.control_paths {
            // A missing control file is watched at its nearest existing
            // ancestor. Creating/replacing that ancestor is just as
            // authoritative as an event naming the eventual file, while a
            // descendant event covers backend-specific recursive reports.
            if path == observed_path
                || path.starts_with(observed_path)
                || observed_path.starts_with(path)
            {
                controls.extend(authority_paths.iter().cloned());
            }
        }
        if coverage.asset_path(path) {
            assets.push(path.clone());
        }
    }
    if controls.is_empty() && assets.len() == original_len {
        event.paths = assets;
        return Some(WatcherEvent::Native(event));
    }
    assets.extend(controls);
    assets.sort_unstable();
    assets.dedup();
    Some(WatcherEvent::Invalidate(assets))
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

