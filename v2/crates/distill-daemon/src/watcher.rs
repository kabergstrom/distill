//! Lossless watcher-generation and debounce queue.
//!
//! The platform watcher is armed before startup traversal. Every event that
//! arrives while the scan is in flight remains tagged with that scan
//! generation and is replayed after its transaction commits. A bounded queue
//! never drops an event silently: overflow converts the generation into an
//! explicit full-rescan request.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::coordinator::WatcherPathEvent;
use crate::scanner::{RootedScanner, ScanError, ScannedFile, ScannedFileKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanGeneration(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationReplay {
    Events(Vec<WatcherPathEvent>),
    FullRescan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatcherQueueError {
    ScanAlreadyArmed,
    NoScanArmed,
    StaleGeneration,
    GenerationExhausted,
}

impl std::fmt::Display for WatcherQueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "watcher queue: {self:?}")
    }
}

impl std::error::Error for WatcherQueueError {}

/// Queue capacity counts distinct `(root, path)` keys. Repeated atomic-save
/// transitions replace the prior state for that path and therefore cannot
/// manufacture overflow by themselves.
pub struct WatcherQueue {
    capacity: usize,
    next_generation: u64,
    scanning: Option<ScanGeneration>,
    overflowed: bool,
    pending: BTreeMap<(String, String), bool>,
}

impl WatcherQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_generation: 1,
            scanning: None,
            overflowed: false,
            pending: BTreeMap::new(),
        }
    }

    /// Begin a scan generation without discarding events already queued by an
    /// armed watcher. Those events are part of the scan/event union too.
    pub fn arm_scan(&mut self) -> Result<ScanGeneration, WatcherQueueError> {
        if self.scanning.is_some() {
            return Err(WatcherQueueError::ScanAlreadyArmed);
        }
        let generation = ScanGeneration(self.next_generation);
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .ok_or(WatcherQueueError::GenerationExhausted)?;
        self.scanning = Some(generation);
        Ok(generation)
    }

    pub fn push(&mut self, event: WatcherPathEvent) -> Result<(), WatcherQueueError> {
        if self.overflowed {
            return Ok(());
        }
        self.pending.insert((event.root, event.path), event.exists);
        if self.pending.len() > self.capacity {
            self.pending.clear();
            self.overflowed = true;
        }
        Ok(())
    }

    pub fn finish_scan(
        &mut self,
        generation: ScanGeneration,
    ) -> Result<GenerationReplay, WatcherQueueError> {
        match self.scanning {
            None => return Err(WatcherQueueError::NoScanArmed),
            Some(current) if current != generation => {
                return Err(WatcherQueueError::StaleGeneration)
            }
            Some(_) => {}
        }
        self.scanning = None;
        if std::mem::take(&mut self.overflowed) {
            self.pending.clear();
            return Ok(GenerationReplay::FullRescan);
        }
        Ok(GenerationReplay::Events(self.drain()))
    }

    /// Consume one debounced live batch. During a scan, only
    /// `finish_scan(generation)` may consume the generation-tagged union.
    pub fn take_live_batch(&mut self) -> Vec<WatcherPathEvent> {
        if self.scanning.is_some() || self.overflowed {
            return Vec::new();
        }
        self.drain()
    }

    pub fn live_overflowed(&self) -> bool {
        self.scanning.is_none() && self.overflowed
    }

    pub fn take_live_overflow(&mut self) -> bool {
        if !self.live_overflowed() {
            return false;
        }
        self.overflowed = false;
        self.pending.clear();
        true
    }

    /// Platform watcher backends call this when their native queue overflows
    /// or an observation pass becomes incomplete. The coordinator responds
    /// only with a complete, newly armed rescan.
    pub fn force_overflow(&mut self) {
        self.pending.clear();
        self.overflowed = true;
    }

    fn drain(&mut self) -> Vec<WatcherPathEvent> {
        std::mem::take(&mut self.pending)
            .into_iter()
            .map(|((root, path), exists)| WatcherPathEvent { root, path, exists })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedPath {
    kind: ScannedFileKind,
    modified_nanos: i64,
    size: u64,
    content_hash: Option<distill_core::id::ContentHash>,
}

impl From<&ScannedFile> for ObservedPath {
    fn from(file: &ScannedFile) -> Self {
        Self {
            kind: file.kind,
            modified_nanos: file.modified_nanos,
            size: file.size,
            content_hash: file.content_hash,
        }
    }
}

/// Portable watcher backend used by the daemon process. It observes the same
/// identity-checked namespace as startup reconciliation, so polling is a
/// correctness-preserving platform fallback rather than a second path parser.
/// Native watcher adapters may feed the same [`WatcherQueue`].
pub struct PollingWatchSource {
    scanner: RootedScanner,
    observed: BTreeMap<(String, String), ObservedPath>,
}

impl PollingWatchSource {
    /// The initial observation is synchronous: when this returns, the source
    /// is armed. A caller can therefore start it before `reconcile_startup`
    /// without a watcher-activation gap.
    pub fn arm(scanner: RootedScanner) -> Result<Self, ScanError> {
        let observed = observe(&scanner)?;
        Ok(Self { scanner, observed })
    }

    pub fn poll_once(&mut self, queue: &Mutex<WatcherQueue>) -> Result<usize, ScanError> {
        let current = match observe(&self.scanner) {
            Ok(current) => current,
            Err(error) => {
                lock_queue(queue).force_overflow();
                return Err(error);
            }
        };
        let mut changed = Vec::new();
        for ((root, path), state) in &current {
            if self.observed.get(&(root.clone(), path.clone())) != Some(state) {
                changed.push(WatcherPathEvent {
                    root: root.clone(),
                    path: path.clone(),
                    exists: true,
                });
            }
        }
        for (root, path) in self.observed.keys() {
            if !current.contains_key(&(root.clone(), path.clone())) {
                changed.push(WatcherPathEvent {
                    root: root.clone(),
                    path: path.clone(),
                    exists: false,
                });
            }
        }
        changed.sort();
        let count = changed.len();
        {
            let mut queue = lock_queue(queue);
            for event in changed {
                // Queue insertion is infallible outside generation exhaustion;
                // overflow is represented as state, never as dropped input.
                let _ = queue.push(event);
            }
        }
        self.observed = current;
        Ok(count)
    }
}

fn observe(scanner: &RootedScanner) -> Result<BTreeMap<(String, String), ObservedPath>, ScanError> {
    Ok(scanner
        .scan()?
        .files
        .into_iter()
        .map(|file| {
            (
                (file.root_name.clone(), file.normalized_path.clone()),
                ObservedPath::from(&file),
            )
        })
        .collect())
}

/// Owned watcher thread. Dropping it requests shutdown and joins the thread,
/// so no observer can outlive the coordinator state it feeds.
pub struct WatcherThread {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl WatcherThread {
    pub fn start(
        scanner: RootedScanner,
        queue: Arc<Mutex<WatcherQueue>>,
        debounce: Duration,
    ) -> Result<Self, ScanError> {
        let mut source = PollingWatchSource::arm(scanner)?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("distill-watcher".to_owned())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    thread::sleep(debounce);
                    let _ = source.poll_once(&queue);
                }
            })
            .expect("failed to start distill watcher thread");
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
