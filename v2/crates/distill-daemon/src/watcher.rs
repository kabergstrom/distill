//! Lossless native watcher batching for incremental reconciliation.
//!
//! Native paths are invalidation addresses, never namespace authority. The
//! coordinator reopens every affected path through `RootedScanner`. Events
//! arriving during startup remain owned by that scan until it commits; only a
//! native overflow or incomplete event discards the batch and requests a full
//! recovery scan.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use notify::event::{ModifyKind, RenameMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::scanner::RootedScanner;

const DEFAULT_CAPACITY: usize = 65_536;
const ROOT_RECONFIGURE_POLL: Duration = Duration::from_millis(40);

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
}

pub struct WatcherQueue {
    capacity: usize,
    scanning: bool,
    overflowed: bool,
    paths: BTreeSet<PathBuf>,
    renames: Vec<WatcherRename>,
    rename_from: BTreeMap<usize, PathBuf>,
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
            paths: BTreeSet::new(),
            renames: Vec::new(),
            rename_from: BTreeMap::new(),
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
        self.overflowed = true;
    }

    pub fn requeue_action(&mut self, action: WatcherAction) {
        match action {
            WatcherAction::None => {}
            WatcherAction::FullRescan => self.force_rescan(),
            WatcherAction::Batch(batch) => {
                self.paths.extend(batch.paths);
                for rename in batch.renames {
                    self.push_rename(rename.from, rename.to);
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
                if let (Some(tracker), Some(path)) = (tracker, event.paths.first()) {
                    self.rename_from.insert(tracker, path.clone());
                }
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                if let (Some(tracker), Some(to)) = (tracker, event.paths.first()) {
                    if let Some(from) = self.rename_from.remove(&tracker) {
                        self.push_rename(from, to.clone());
                    }
                }
            }
            _ => {}
        }
        for path in event.paths {
            self.paths.insert(path);
        }
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
        if std::mem::take(&mut self.overflowed) {
            self.paths.clear();
            self.renames.clear();
            self.rename_from.clear();
            return WatcherAction::FullRescan;
        }
        let batch = WatcherBatch {
            paths: std::mem::take(&mut self.paths).into_iter().collect(),
            renames: std::mem::take(&mut self.renames),
        };
        self.rename_from.clear();
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
    thread: Option<JoinHandle<()>>,
}

impl WatcherThread {
    pub fn start(
        scanner: RootedScanner,
        queue: Arc<Mutex<WatcherQueue>>,
    ) -> Result<Self, WatcherStartError> {
        let callback_queue = Arc::clone(&queue);
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<Event>| match result {
                Ok(event) => lock_queue(&callback_queue).push_native(event),
                Err(_) => lock_queue(&callback_queue).force_rescan(),
            },
            Config::default().with_follow_symlinks(false),
        )?;
        let mut watched = scanner.watch_roots().into_iter().collect::<BTreeSet<_>>();
        for root in &watched {
            watcher.watch(root, RecursiveMode::Recursive)?;
        }
        let mut revision = scanner.revision();

        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("distill-watcher".to_owned())
            .spawn(move || {
                let mut watcher = watcher;
                while !thread_stop.load(Ordering::Acquire) {
                    thread::sleep(ROOT_RECONFIGURE_POLL);
                    let next_revision = scanner.revision();
                    if next_revision == revision {
                        continue;
                    }
                    let next = scanner.watch_roots().into_iter().collect::<BTreeSet<_>>();
                    let mut complete = true;
                    for root in next.difference(&watched) {
                        if watcher.watch(root, RecursiveMode::Recursive).is_err() {
                            complete = false;
                        }
                    }
                    if !complete {
                        lock_queue(&queue).force_rescan();
                        continue;
                    }
                    for root in watched.difference(&next) {
                        let _ = watcher.unwatch(root);
                    }
                    watched = next;
                    revision = next_revision;
                    // The candidate scan preceded watcher reconfiguration.
                    // Force one armed catch-up scan to cover that bounded gap.
                    lock_queue(&queue).force_rescan();
                }
            })
            .map_err(WatcherStartError::Thread)?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for WatcherThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn lock_queue(queue: &Mutex<WatcherQueue>) -> std::sync::MutexGuard<'_, WatcherQueue> {
    queue
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
