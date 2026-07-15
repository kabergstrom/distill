//! Sticky watcher invalidation for complete namespace rescans.
//!
//! The platform watcher is armed before startup traversal. Any event arriving
//! during a scan leaves the queue dirty, forcing another fully armed scan after
//! the current transaction commits. Paths are deliberately not queued because
//! the coordinator never trusts or consumes a partial path set.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::scanner::{RootedScanner, ScanError, ScannedFile, ScannedFileKind};

#[derive(Default)]
pub struct WatcherQueue {
    scanning: bool,
    dirty: bool,
}

impl WatcherQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin a scan without clearing an invalidation already observed by the
    /// armed watcher.
    pub fn arm_scan(&mut self) {
        assert!(!self.scanning, "watcher scan already armed");
        self.scanning = true;
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Finish the current scan and consume whether another scan is required.
    pub fn finish_scan(&mut self) -> bool {
        assert!(self.scanning, "no watcher scan armed");
        self.scanning = false;
        std::mem::take(&mut self.dirty)
    }

    /// Consume one debounced live invalidation. A scan in flight owns the bit
    /// until `finish_scan` so no event can be lost between traversal and commit.
    pub fn take_live_dirty(&mut self) -> bool {
        if self.scanning {
            return false;
        }
        std::mem::take(&mut self.dirty)
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
    scanner_revision: u64,
    observed: BTreeMap<(String, String), ObservedPath>,
    last_failure: Option<String>,
}

impl PollingWatchSource {
    /// The initial observation is synchronous: when this returns, the source
    /// is armed. A caller can therefore start it before `reconcile_startup`
    /// without a watcher-activation gap.
    pub fn arm(scanner: RootedScanner) -> Result<Self, ScanError> {
        let observed = observe(&scanner)?;
        Ok(Self {
            scanner_revision: scanner.revision(),
            scanner,
            observed,
            last_failure: None,
        })
    }

    pub fn poll_once(&mut self, queue: &Mutex<WatcherQueue>) -> Result<usize, ScanError> {
        let revision = self.scanner.revision();
        if revision != self.scanner_revision {
            self.observed = observe(&self.scanner)?;
            self.scanner_revision = revision;
            self.last_failure = None;
            return Ok(0);
        }
        let current = match observe(&self.scanner) {
            Ok(current) => current,
            Err(error) => {
                let detail = error.to_string();
                if self.last_failure.as_ref() != Some(&detail) {
                    lock_queue(queue).mark_dirty();
                    self.last_failure = Some(detail);
                }
                return Err(error);
            }
        };
        let healed = self.last_failure.take().is_some();
        let updated = current
            .iter()
            .filter(|(path, state)| self.observed.get(*path) != Some(*state))
            .count();
        let deleted = self
            .observed
            .keys()
            .filter(|path| !current.contains_key(*path))
            .count();
        let count = updated + deleted;
        if healed || count != 0 {
            lock_queue(queue).mark_dirty();
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
