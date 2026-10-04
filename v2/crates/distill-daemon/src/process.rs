//! Long-lived daemon process supervisor.
//!
//! The process loop runs on a thread of its own, fed by the watcher over a
//! channel; every reconciliation it starts publishes from that thread. It
//! reconciles watcher changes once the filesystem has settled: each change
//! restarts a trailing quiet window (`watch.quiet_ms`, `crate::settle`),
//! and no pass runs while the window is open (`Schedule`). Each pass
//! publishes one input version. A pass that leaves work behind (a drifted
//! import, a path with newer work) is retried after a delay; a `Stale`
//! pass, overtaken by an outside write, is retried without an error; and
//! the background error is replaced by each pass's outcome, so it clears
//! once a later pass succeeds.

use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use distill_schema::{ProjectSchemaAuthority, SchemaAuthorityError};
use distill_store::state::{
    CleanupDisposition, ConfigurationError, PipelineFailure, PipelineFailureCode,
    PipelineFailureOrigin,
};

use crate::codegen::CodegenService;
use crate::config::{candidate_error_reason, config_error_reason, DaemonConfig, DaemonConfigError};
use crate::coordinator::{CoordinatorError, CoordinatorInitError, DaemonCoordinator, PassOutcome};
use crate::settle::{HeldOpen, QuietWindow};
use crate::watcher::{
    WatcherAction, WatcherBatch, WatcherControl, WatcherEvent, WatcherQueue, WatcherSink,
    WatcherStartError,
    WatcherThread,
};
use distill_store::config::RestartOnlyChange;
use distill_store::cas::SegmentSweeper;
use distill_store::{Store, StoreReader, StoreWriter};
use distill_store::state::{ConfigurationSourceFailureCode, ConfigurationSourcePath, DscpV1};

/// A failed pass, or a failed codegen run, is retried this much later.
const RETRY_DELAY: Duration = Duration::from_millis(40);
/// An idle daemon still runs a pass this often.
const IDLE_PASS_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// How often the loop runs its CAS pass (eviction, compaction, dead
/// segment deletion).
const CAS_PASS_INTERVAL: Duration = Duration::from_secs(10);
/// How much longer than a snapshot's TTL a dead segment's file stays: no
/// snapshot that could read it is left by then.
const CAS_DELETE_MARGIN: Duration = Duration::from_secs(10);

/// When the process loop runs a pass, and when its CAS pass.
///
/// Two kinds of deadline combine. Watcher changes hold a trailing quiet
/// window ([`QuietWindow`], `watch.quiet_ms`): each change pushes it to
/// that long after itself, so a burst (a git checkout, a branch switch) is
/// reconciled once, after its last change. Every other reason for a pass (a
/// failed pass's requeue, a codegen retry, a publication from elsewhere
/// that needs codegen, the idle pass) asks for one by some time, and the
/// earliest such request wins.
///
/// A pass never starts while the window is open: one asked for during a
/// burst waits for the burst's end, which then runs one pass for both, so
/// nothing is reconciled, imported or generated from a filesystem part way
/// through a change. The one exception is a watcher failure, which stops
/// the loop at once. The CAS pass reads no source file and keeps its own
/// timer.
struct Schedule {
    window: QuietWindow,
    /// The earliest time something other than a watcher change asked for a
    /// pass by; `None` until something asks.
    due: Option<Instant>,
    /// The watcher failed: the next pass stops the loop, without waiting.
    failed: bool,
    next_idle_pass: Instant,
    next_cas_pass: Instant,
}

impl Schedule {
    fn new(now: Instant, quiet: Duration) -> Self {
        Self {
            window: QuietWindow::new(quiet, "reconciliation"),
            due: None,
            failed: false,
            next_idle_pass: now + IDLE_PASS_INTERVAL,
            next_cas_pass: now + CAS_PASS_INTERVAL,
        }
    }

    /// A watcher change to `paths` arrived at `now`. Returns the warning
    /// (already logged) when the window has been held open too long.
    fn change<'a>(
        &mut self,
        now: Instant,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> Option<HeldOpen> {
        self.window.change(now, paths)
    }

    /// The watcher failed: pass (and stop) now.
    fn fail(&mut self, now: Instant) {
        self.failed = true;
        self.request(now);
    }

    /// Run a pass by `at` at the latest, unless watcher changes are still
    /// arriving then.
    fn request(&mut self, at: Instant) {
        self.due = Some(self.due.map_or(at, |due| due.min(at)));
    }

    /// When the next pass starts: when the watch window closes if watcher
    /// changes wait, else at the earliest request (the idle pass at the
    /// latest).
    fn pass_at(&self) -> Instant {
        let requested = self
            .due
            .map_or(self.next_idle_pass, |due| due.min(self.next_idle_pass));
        match self.window.closes_at() {
            Some(closes) if !self.failed => closes,
            _ => requested,
        }
    }

    /// When the loop next has work.
    fn deadline(&self) -> Instant {
        self.pass_at().min(self.next_cas_pass)
    }

    /// Whether the CAS pass is due at `now`; if it is, the next is
    /// scheduled.
    fn take_cas_pass(&mut self, now: Instant) -> bool {
        if now < self.next_cas_pass {
            return false;
        }
        self.next_cas_pass = now + CAS_PASS_INTERVAL;
        true
    }

    /// Whether a pass is due at `now`; if it is, it starts: every request
    /// and the watch window's changes are taken by it.
    fn take_pass(&mut self, now: Instant) -> bool {
        if now < self.pass_at() {
            return false;
        }
        self.window.close();
        self.due = None;
        self.failed = false;
        self.next_idle_pass = now + IDLE_PASS_INTERVAL;
        true
    }
}

pub struct DaemonProcess {
    coordinator: Arc<DaemonCoordinator>,
    rpc_address: SocketAddr,
    stop: Arc<watch::Sender<bool>>,
    watcher: Option<WatcherThread>,
    inbox: mpsc::Sender<LoopMessage>,
    loop_thread: Option<JoinHandle<()>>,
    rpc_thread: Option<JoinHandle<Result<(), String>>>,
    last_background_error: watch::Receiver<Option<String>>,
}

impl DaemonProcess {
    /// Start from the configured shared schema artifact. Production never
    /// substitutes the bootstrap-only table for `assets.schema_path`.
    pub fn start(config: DaemonConfig) -> Result<Self, DaemonProcessError> {
        let schema_bytes = std::fs::read(&config.assets.schema_path).map_err(|source| {
            DaemonProcessError::SchemaRead {
                path: config.assets.schema_path.clone(),
                source,
            }
        })?;
        let authority = ProjectSchemaAuthority::from_json(&schema_bytes)?;
        Self::start_with_authority(config, authority)
    }

    pub fn start_with_authority(
        config: DaemonConfig,
        authority: ProjectSchemaAuthority,
    ) -> Result<Self, DaemonProcessError> {
        let targets = config.target_definitions(authority.identity())?;
        let coordinator = Arc::new(DaemonCoordinator::open(
            config.store_config(),
            config.asset_roots(),
            targets,
            config.pipeline.max_dependency_depth,
        )?);
        // Under the store lock, before anything writes into a root: what an
        // earlier process staged and never renamed is uncommitted.
        for root in config.asset_roots() {
            distill_store::atomic_file::open_staging(&root.path).map_err(|source| {
                DaemonProcessError::Staging {
                    path: root.path.clone(),
                    source,
                }
            })?;
        }
        coordinator.attach_build_backend();
        let mut config_watch = ConfigWatch::new(config.clone());
        config_watch.rebuilder = Some(crate::rebuild::Rebuilder::start(
            config.rebuild.clone(),
            config.watch.quiet,
        ));
        let (inbox, messages) = mpsc::channel();
        let sink: WatcherSink = {
            let inbox = inbox.clone();
            Arc::new(move |event| {
                let _ = inbox.send(LoopMessage::Watch(event));
            })
        };
        {
            // Publications from elsewhere (an RPC write, an import) still
            // need codegen.
            let inbox = inbox.clone();
            coordinator
                .server_handle()
                .install_publication_hook(Box::new(move || {
                    let _ = inbox.send(LoopMessage::Published);
                }));
        }
        // Arm both asset and control-file coverage before any candidate scan.
        // Root replacement is synchronously scanned by candidate publication;
        // the watcher then installs the new root and requests one catch-up scan.
        let watcher = WatcherThread::start(coordinator.scanner(), config_watch.control_paths(), sink)?;
        let watcher_control = watcher.control();
        let codegen = CodegenService::new(
            &coordinator,
            &config.codegen.rs_mod_path,
            config.codegen.auto_codegen,
        )
        .map_err(DaemonProcessError::Codegen)?;

        let stop = Arc::new(watch::Sender::new(false));
        let (errors, last_background_error) = watch::channel(None);
        let (ready, started) = mpsc::sync_channel(1);
        let mut process_loop = ProcessLoop {
            store: coordinator
                .open_writer()
                .map_err(CoordinatorInitError::Store)?,
            coordinator: Arc::clone(&coordinator),
            queue: WatcherQueue::new(),
            config_watch,
            watcher_control,
            codegen,
            stop: Arc::clone(&stop),
            errors,
            schedule: {
                let now = Instant::now();
                let mut schedule = Schedule::new(now, config.watch.quiet);
                // The first pass follows startup at once.
                schedule.request(now);
                schedule
            },
            publications: 0,
            cas_sweeper: SegmentSweeper::new(
                coordinator.server_handle().snapshot_policy().ttl + CAS_DELETE_MARGIN,
            ),
            cas_swept: None,
            capabilities_pending: false,
            work_pending: false,
        };
        // Startup runs on the loop thread before the loop: events the
        // watcher sends meanwhile wait in its inbox.
        let loop_thread = thread::Builder::new()
            .name("distill-process-loop".to_owned())
            .spawn(move || match process_loop.startup() {
                Ok(()) => {
                    let _ = ready.send(Ok(()));
                    process_loop.run(messages);
                }
                Err(error) => {
                    let _ = ready.send(Err(error));
                }
            })
            .expect("failed to start the distill process loop");
        let startup = started.recv().unwrap_or_else(|_| {
            Err(DaemonProcessError::Rpc(
                "the process loop stopped during startup".to_owned(),
            ))
        });
        let mut process = Self {
            coordinator,
            rpc_address: config.daemon.address,
            stop,
            watcher: Some(watcher),
            inbox,
            loop_thread: Some(loop_thread),
            rpc_thread: None,
            last_background_error,
        };
        startup?;

        let (address_tx, address_rx) = mpsc::sync_channel(1);
        let rpc_thread = spawn_rpc_loop(
            process.coordinator.server().root(),
            config.daemon.address,
            Arc::clone(&process.stop),
            address_tx,
        );
        process.rpc_thread = Some(rpc_thread);
        process.rpc_address = match address_rx.recv() {
            Ok(Ok(address)) => address,
            Ok(Err(error)) => return Err(DaemonProcessError::Rpc(error)),
            Err(error) => {
                return Err(DaemonProcessError::Rpc(format!(
                    "RPC startup channel closed: {error}"
                )))
            }
        };
        Ok(process)
    }

}

impl DaemonProcess {
    pub fn coordinator(&self) -> &Arc<DaemonCoordinator> {
        &self.coordinator
    }

    pub fn rpc_address(&self) -> SocketAddr {
        self.rpc_address
    }

    pub fn last_background_error(&self) -> Option<String> {
        self.last_background_error.borrow().clone()
    }

    /// Whether a fatal watcher/coordinator failure has stopped the serving
    /// loops. The development supervisor uses this to tear down its producer
    /// children instead of remaining alive around a dead daemon.
    pub fn has_stopped(&self) -> bool {
        *self.stop.borrow()
    }

    pub fn wait(self) -> ! {
        loop {
            thread::park();
        }
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        self.watcher.take();
        let _ = self.inbox.send(LoopMessage::Stop);
        if let Some(thread) = self.loop_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.rpc_thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Debug)]
pub enum DaemonProcessError {
    Config(DaemonConfigError),
    CoordinatorInit(CoordinatorInitError),
    Coordinator(CoordinatorError),
    Watch(WatcherStartError),
    SchemaRead {
        path: PathBuf,
        source: std::io::Error,
    },
    Schema(SchemaAuthorityError),
    /// An asset root's staging directory could not be emptied.
    Staging {
        path: PathBuf,
        source: std::io::Error,
    },
    Codegen(String),
    Rpc(String),
}

impl std::fmt::Display for DaemonProcessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "daemon process: {self:?}")
    }
}

impl std::error::Error for DaemonProcessError {}

impl From<DaemonConfigError> for DaemonProcessError {
    fn from(error: DaemonConfigError) -> Self {
        Self::Config(error)
    }
}
impl From<CoordinatorInitError> for DaemonProcessError {
    fn from(error: CoordinatorInitError) -> Self {
        Self::CoordinatorInit(error)
    }
}
impl From<CoordinatorError> for DaemonProcessError {
    fn from(error: CoordinatorError) -> Self {
        Self::Coordinator(error)
    }
}
impl From<WatcherStartError> for DaemonProcessError {
    fn from(error: WatcherStartError) -> Self {
        Self::Watch(error)
    }
}
impl From<SchemaAuthorityError> for DaemonProcessError {
    fn from(error: SchemaAuthorityError) -> Self {
        Self::Schema(error)
    }
}

enum LoopMessage {
    Watch(WatcherEvent),
    Published,
    Stop,
}

/// The process loop, on a thread of its own. It runs the scan-driven
/// publications; builds, imports and RPC writes publish from wherever they
/// run.
struct ProcessLoop {
    /// The loop's own writer: every reconciliation, configuration change,
    /// codegen publication and CAS pass writes through it.
    store: StoreWriter,
    coordinator: Arc<DaemonCoordinator>,
    queue: WatcherQueue,
    config_watch: ConfigWatch,
    watcher_control: WatcherControl,
    codegen: CodegenService,
    stop: Arc<watch::Sender<bool>>,
    errors: watch::Sender<Option<String>>,
    /// When the next pass and CAS pass run.
    schedule: Schedule,
    /// The server's publication count the last pass saw.
    publications: u64,
    cas_sweeper: SegmentSweeper,
    /// The CAS write count and cache limit the last complete CAS sweep ran
    /// at: with neither changed there is nothing to evict or compact.
    cas_swept: Option<(u64, u64)>,
    /// An accepted configuration candidate changed the importer registry or
    /// its capabilities, and the capability-driven reimport has not yet
    /// completed. A requeued pass keeps it: the configuration watch reports
    /// the change only once.
    capabilities_pending: bool,
    /// A pass left watcher work in the store for another pass (see
    /// `PassOutcome::more_work`).
    work_pending: bool,
}

impl ProcessLoop {
    fn startup(&mut self) -> Result<(), DaemonProcessError> {
        // Retain an existing daemon-owned output before the first candidate
        // performs its mandatory startup scan. Otherwise an authored symlink
        // to that output can be misclassified as a root escape during the
        // narrow gap between candidate publication and service construction.
        let store = &mut *self.store;
        self.config_watch.reconcile(
            &self.coordinator,
            store,
            &self.watcher_control,
            ControlInvalidation::all(),
        )?;
        self.watcher_control
            .replace_roots(&self.coordinator.scanner())
            .map_err(CoordinatorError::InvalidManifest)?;
        let started = Instant::now();
        // The scan and every import it makes due publish as one version.
        let outcome = self.coordinator.reconcile_rescan(store, &mut self.queue)?;
        tracing::info!(elapsed = ?started.elapsed(), "startup reconciled");
        if outcome.more_work {
            self.queue.requeue_action(WatcherAction::FullRescan);
        }
        let mut error = pass_failures(&outcome);
        if let Err(codegen) = self.codegen.run(&self.coordinator, store) {
            error = Some(codegen);
        }
        self.errors.send_replace(error);
        Ok(())
    }

    /// One pass of the loop: reconcile what the watcher queued, and what an
    /// earlier pass left pending, as one input version.
    fn tick(&mut self) -> bool {
        let action = self.queue.take_live_action();
        if let WatcherAction::Failed(message) = &action {
            tracing::error!(%message, "watcher failed; stopping the coordinator");
            self.errors.send_replace(Some(message.clone()));
            self.stop.send_replace(true);
            return false;
        }
        let control_invalidation = match &action {
            WatcherAction::Batch(batch) => self.config_watch.invalidation_for(batch),
            WatcherAction::FullRescan => Some(ControlInvalidation::all()),
            WatcherAction::None | WatcherAction::Failed(_) => None,
        };
        // Work an earlier pass left in the store needs a pass of its own
        // even when the watcher has nothing new.
        let action = match action {
            WatcherAction::None if self.work_pending => WatcherAction::Batch(WatcherBatch::default()),
            action => action,
        };
        let retry_action = action.clone();
        let started = Instant::now();
        match &action {
            WatcherAction::Batch(batch) => {
                tracing::debug!(batch = ?batch, "reconciling watcher batch")
            }
            WatcherAction::FullRescan => tracing::info!("reconciling full rescan"),
            WatcherAction::None | WatcherAction::Failed(_) => {}
        }
        let coordinator = &self.coordinator;
        let store = &mut *self.store;
        let result = match control_invalidation {
            Some(invalidation) => {
                self.config_watch
                    .reconcile(coordinator, store, &self.watcher_control, invalidation)
                    .and_then(|capabilities_changed| {
                        // A configuration may have replaced the roots.
                        self.watcher_control
                            .replace_roots(&coordinator.scanner())
                            .map_err(CoordinatorError::InvalidManifest)?;
                        Ok(capabilities_changed)
                    })
            }
            None => Ok(false),
        }
        .map(|changed| {
            self.capabilities_pending |= changed;
            self.capabilities_pending
        })
        .and_then(|capabilities_changed| match action {
            WatcherAction::None => coordinator
                .sync_runtime_pipeline_failure(store)
                .map(|_| None),
            WatcherAction::Batch(batch) => coordinator
                .reconcile_batch(store, &batch, capabilities_changed)
                .map(Some),
            WatcherAction::FullRescan => coordinator
                .reconcile_rescan(store, &mut self.queue)
                .map(Some),
            WatcherAction::Failed(_) => unreachable!("handled before reconciliation"),
        });
        match result {
            // Another publication (an RPC import, say) moved the input
            // version between this pass reading its base and committing.
            // Nothing was published; the retry recomputes the whole pass
            // from the new base.
            Err(CoordinatorError::Coordinated(distill_rpc::CoordinatedCommitError::Stale {
                expected,
                observed,
            })) => {
                tracing::debug!(?expected, ?observed, "reconciliation raced a publication; requeued");
                self.queue.requeue_action(retry_action);
            }
            // A file the pass read changed since its version observed it:
            // nothing was published. The change is the watcher's to report;
            // invalidating the path makes sure the retry rescans it.
            Err(CoordinatorError::Drifted { root, path }) => {
                tracing::debug!(%root, %path, "reconciliation read a drifted file; requeued");
                self.queue.requeue_action(retry_action);
                if let Ok(physical) = self.coordinator.scanner().physical_path(&root, &path) {
                    self.queue.push(WatcherEvent::Invalidate(vec![physical]));
                }
            }
            Err(error) => {
                tracing::warn!(%error, "reconciliation failed; requeued");
                self.errors.send_replace(Some(error.to_string()));
                self.queue.requeue_action(retry_action);
            }
            Ok(outcome) => {
                let more_work = outcome.as_ref().is_some_and(|outcome| outcome.more_work);
                if let Some(outcome) = &outcome {
                    tracing::info!(
                        elapsed = ?started.elapsed(),
                        version = ?outcome.stamp.version,
                        more_work,
                        "reconciled"
                    );
                }
                if more_work {
                    // Imports this pass could not finish rerun in the next
                    // one, with the same capability change if it had one. A
                    // full pass reruns whole: its imports were not limited to
                    // the work.
                    match retry_action {
                        WatcherAction::FullRescan => self.queue.requeue_action(retry_action),
                        _ => self.work_pending = true,
                    }
                    self.schedule.request(Instant::now() + RETRY_DELAY);
                } else {
                    self.work_pending = false;
                    self.capabilities_pending = false;
                }
                // A pass that succeeded clears the last background error;
                // what still fails is reported again.
                let mut error = outcome.as_ref().and_then(pass_failures);
                if let Err(codegen) = self.codegen.run(&self.coordinator, store) {
                    tracing::warn!(error = %codegen, "codegen failed");
                    error = Some(codegen);
                    self.schedule.request(Instant::now() + RETRY_DELAY);
                }
                self.errors.send_replace(error);
            }
        }
        true
    }

    /// The loop: fold watcher events into the queue, and reconcile once
    /// they have settled (see [`Schedule`]).
    fn run(mut self, messages: mpsc::Receiver<LoopMessage>) {
        loop {
            let wait = self
                .schedule
                .deadline()
                .saturating_duration_since(Instant::now());
            match messages.recv_timeout(wait) {
                Ok(LoopMessage::Watch(event)) => self.watch(event),
                Ok(LoopMessage::Published) => self.published(),
                Ok(LoopMessage::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if !self.fire() {
                        break;
                    }
                }
            }
        }
    }

    fn watch(&mut self, event: WatcherEvent) {
        let now = Instant::now();
        // A window held open too long has logged its warning.
        match &event {
            WatcherEvent::Native(native) => {
                self.schedule
                    .change(now, native.paths.iter().map(PathBuf::as_path));
            }
            // An event wholly outside the asset roots and control files (the
            // daemon's own state writes, under a watched control directory)
            // invalidates nothing, and is no change to wait for.
            WatcherEvent::Invalidate(paths) if paths.is_empty() => {}
            WatcherEvent::Invalidate(paths) => {
                self.schedule.change(now, paths.iter().map(PathBuf::as_path));
            }
            WatcherEvent::Rescan => {
                self.schedule.change(now, []);
            }
            WatcherEvent::Failed(_) => self.schedule.fail(now),
        }
        self.queue.push(event);
    }

    /// Run the due work. `false` stops the loop.
    fn fire(&mut self) -> bool {
        let now = Instant::now();
        if self.schedule.take_cas_pass(now) {
            if let Err(error) = crate::coordinator::maintain_cas(
                &mut self.store,
                &mut self.cas_sweeper,
                &mut self.cas_swept,
            ) {
                tracing::warn!(%error, "CAS maintenance failed");
            }
        }
        if !self.schedule.take_pass(now) {
            return true;
        }
        let keep = self.tick();
        self.publications = self.coordinator.server_handle().publication_count();
        // The pass may have accepted a configuration with another window.
        self.schedule
            .window
            .set_quiet(self.config_watch.active.watch.quiet);
        if self.queue.has_pending() {
            // A failed pass requeued its work.
            self.schedule.request(Instant::now() + RETRY_DELAY);
        }
        keep
    }

    /// Something published; one from elsewhere (an RPC write, an import)
    /// still needs codegen.
    fn published(&mut self) {
        if self.coordinator.server_handle().publication_count() != self.publications {
            self.schedule.request(Instant::now());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ArtifactSourceState {
    Missing,
    Unreadable(std::io::ErrorKind),
    Bytes([u8; 32]),
}

#[derive(Clone)]
struct SchemaObservation {
    state: ArtifactSourceState,
    outcome: Result<Arc<ProjectSchemaAuthority>, String>,
}

fn observe_artifact_source(path: &Path) -> ArtifactSourceState {
    match std::fs::read(path) {
        Ok(bytes) => ArtifactSourceState::Bytes(*blake3::hash(&bytes).as_bytes()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ArtifactSourceState::Missing,
        Err(error) => ArtifactSourceState::Unreadable(error.kind()),
    }
}

fn observe_schema_source(path: &Path) -> SchemaObservation {
    match std::fs::read(path) {
        Ok(bytes) => SchemaObservation {
            state: ArtifactSourceState::Bytes(*blake3::hash(&bytes).as_bytes()),
            outcome: ProjectSchemaAuthority::from_json(&bytes)
                .map(Arc::new)
                .map_err(|error| error.to_string()),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => SchemaObservation {
            state: ArtifactSourceState::Missing,
            outcome: Err(format!("schema artifact {} is missing", path.display())),
        },
        Err(error) => SchemaObservation {
            state: ArtifactSourceState::Unreadable(error.kind()),
            outcome: Err(format!(
                "cannot read schema artifact {}: {error}",
                path.display()
            )),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConfigSourceState {
    Failure(Box<DscpV1>),
    Bytes([u8; 32]),
}

struct ConfigObservation {
    state: ConfigSourceState,
    outcome: Result<DaemonConfig, (DscpV1, String)>,
}

#[derive(Debug, Clone, Copy)]
struct ControlInvalidation {
    configuration: bool,
    schema: bool,
    pipeline: bool,
}

impl ControlInvalidation {
    const fn all() -> Self {
        Self {
            configuration: true,
            schema: true,
            pipeline: true,
        }
    }
}

struct ConfigWatch {
    path: PathBuf,
    active: DaemonConfig,
    staged: DaemonConfig,
    observed: Option<ConfigSourceState>,
    observed_schema: Option<ArtifactSourceState>,
    observed_pipeline: Option<ArtifactSourceState>,
    cached_schema: Option<SchemaObservation>,
    cached_pipeline: Option<ArtifactSourceState>,
    /// Whether the configuration source error the store holds (see
    /// [`rejected`]) came from observing the source itself rather than
    /// from staging a parsed candidate: `staged` then is not the source's.
    source_rejected: bool,
    /// Runs the active configuration's `[[rebuild]]` jobs.
    rebuilder: Option<crate::rebuild::Rebuilder>,
}

impl ConfigWatch {
    fn new(active: DaemonConfig) -> Self {
        Self {
            path: active.source_path.clone(),
            staged: active.clone(),
            active,
            observed: None,
            observed_schema: None,
            observed_pipeline: None,
            cached_schema: None,
            cached_pipeline: None,
            source_rejected: false,
            rebuilder: None,
        }
    }

    /// Adopt the `[watch]` settings and `[[rebuild]]` jobs of an accepted
    /// configuration. They depend on neither the schema nor the pipeline
    /// module (the jobs build those), so they follow the configuration even
    /// while either is rejected.
    fn adopt_watch_and_rebuild(&mut self, candidate: &DaemonConfig) {
        if self.active.watch != candidate.watch {
            self.active.watch = candidate.watch.clone();
            if let Some(rebuilder) = &self.rebuilder {
                rebuilder.jobs().set_quiet(candidate.watch.quiet);
            }
        }
        if self.active.rebuild == candidate.rebuild {
            return;
        }
        self.active.rebuild = candidate.rebuild.clone();
        if let Some(rebuilder) = &self.rebuilder {
            rebuilder.jobs().replace(candidate.rebuild.clone());
        }
    }

    fn control_paths(&self) -> [PathBuf; 3] {
        [
            self.path.clone(),
            self.staged.assets.schema_path.clone(),
            self.staged.modules.pipeline_dylib.clone(),
        ]
    }

    fn invalidation_for(
        &self,
        batch: &crate::watcher::WatcherBatch,
    ) -> Option<ControlInvalidation> {
        control_invalidation_for(
            batch,
            &self.path,
            &self.staged.assets.schema_path,
            &self.staged.modules.pipeline_dylib,
        )
    }

    fn reconcile(
        &mut self,
        coordinator: &DaemonCoordinator,
        store: &mut Store,
        watcher: &WatcherControl,
        invalidation: ControlInvalidation,
    ) -> Result<bool, CoordinatorError> {
        // An ancestor event may have introduced a previously missing parent.
        // Recompute native coverage synchronously before reading authority so
        // subsequent edits stay incremental and do not depend on a rescan.
        watcher
            .replace_paths(self.control_paths())
            .map_err(CoordinatorError::InvalidManifest)?;
        if !invalidation.configuration {
            if self.source_rejected && rejected(store)? {
                self.refresh_cached_artifacts(invalidation);
                return Ok(false);
            }
            let state = self
                .observed
                .clone()
                .expect("a non-configuration invalidation follows initial reconciliation");
            return self.reconcile_valid(
                coordinator,
                store,
                watcher,
                state,
                self.staged.clone(),
                invalidation,
            );
        }

        let observation = observe_configuration(&self.path);
        match observation.outcome {
            Err((reason, message)) => {
                self.refresh_cached_artifacts(ControlInvalidation::all());
                if self.observed.as_ref() == Some(&observation.state) {
                    return Ok(false);
                }
                tracing::warn!(
                    %message,
                    "configuration rejected; the active one (and its rebuild jobs) stays"
                );
                coordinator.publish_configuration_rejection(store, reason, message)?;
                self.source_rejected = true;
                self.observed = Some(observation.state);
                Ok(false)
            }
            Ok(candidate) => self.reconcile_valid(
                coordinator,
                store,
                watcher,
                observation.state,
                candidate,
                ControlInvalidation::all(),
            ),
        }
    }

    fn refresh_cached_artifacts(&mut self, invalidation: ControlInvalidation) {
        if invalidation.schema {
            let schema = observe_schema_source(&self.staged.assets.schema_path);
            self.observed_schema = Some(schema.state.clone());
            self.cached_schema = Some(schema);
        }
        if invalidation.pipeline {
            let pipeline = observe_artifact_source(&self.staged.modules.pipeline_dylib);
            self.observed_pipeline = Some(pipeline.clone());
            self.cached_pipeline = Some(pipeline);
        }
    }

    fn reconcile_valid(
        &mut self,
        coordinator: &DaemonCoordinator,
        store: &mut Store,
        watcher: &WatcherControl,
        config_state: ConfigSourceState,
        candidate: DaemonConfig,
        invalidation: ControlInvalidation,
    ) -> Result<bool, CoordinatorError> {
        let rejected = rejected(store)?;
        if invalidation.configuration {
            self.adopt_watch_and_rebuild(&candidate);
            watcher
                .replace_paths([
                    self.path.clone(),
                    candidate.assets.schema_path.clone(),
                    candidate.modules.pipeline_dylib.clone(),
                ])
                .map_err(CoordinatorError::InvalidManifest)?;
        }
        let schema = if invalidation.schema || self.cached_schema.is_none() {
            observe_schema_source(&candidate.assets.schema_path)
        } else {
            self.cached_schema
                .clone()
                .expect("cached schema presence was checked")
        };
        let pipeline_state = if invalidation.pipeline || self.cached_pipeline.is_none() {
            observe_artifact_source(&candidate.modules.pipeline_dylib)
        } else {
            self.cached_pipeline
                .clone()
                .expect("cached pipeline presence was checked")
        };
        let config_changed = self.observed.as_ref() != Some(&config_state);
        let schema_changed = self.observed_schema.as_ref() != Some(&schema.state);
        let pipeline_changed = self.observed_pipeline.as_ref() != Some(&pipeline_state);
        if !config_changed && !schema_changed && !pipeline_changed && !rejected {
            return Ok(false);
        }
        tracing::debug!(
            config_changed,
            schema_changed,
            pipeline_changed,
            rejected = rejected,
            ?invalidation,
            ?pipeline_state,
            "configuration inputs observed"
        );
        // Schema or pipeline writes that leave the Ready pipeline epoch as it
        // is (the same module bytes, and a schema with the same version key:
        // source-walk catching up after an ahead-of-walk adoption, in either
        // order with the dylib event) are observed without republishing: no
        // second epoch, no loader reconnects, no reimport.
        if !config_changed && !rejected {
            if let (Ok(authority), ArtifactSourceState::Bytes(dylib_hash)) =
                (&schema.outcome, &pipeline_state)
            {
                if coordinator.ready_pipeline_serves(authority, *dylib_hash) {
                    tracing::info!("the Ready pipeline epoch already serves these inputs");
                    self.observed_schema = Some(schema.state.clone());
                    self.observed_pipeline = Some(pipeline_state.clone());
                    self.cached_schema = Some(schema);
                    self.cached_pipeline = Some(pipeline_state);
                    return Ok(false);
                }
            }
        }
        // cargo replaces the dylib by removing it and linking the new one;
        // a pass between the two sees no module. With a Ready epoch to keep
        // serving, that is not a failure: the observed state stays, so the
        // module's reappearance (or any other write) retries the candidate.
        if !config_changed
            && !rejected
            && pipeline_state == ArtifactSourceState::Missing
            && coordinator.has_ready_pipeline()
        {
            tracing::info!("pipeline module is missing; the Ready epoch serves until it reappears");
            self.cached_schema = Some(schema);
            self.cached_pipeline = Some(pipeline_state);
            return Ok(false);
        }

        let input_changed = self.observed.is_none()
            || input_configuration_changed(&self.active, &candidate)
            || schema_changed
            || pipeline_changed
            || rejected;

        match &schema.outcome {
            Err(message) if input_changed => {
                let failure = PipelineFailure::new(
                    PipelineFailureCode::CandidateValidation,
                    PipelineFailureOrigin::CandidateOpen,
                    CleanupDisposition::None,
                    message.clone(),
                )
                .expect("schema candidate failure tuple is valid");
                if rejected {
                    coordinator.publish_pipeline_rejection_healing_configuration(store, failure)?;
                } else {
                    coordinator.publish_pipeline_rejection(store, failure)?;
                }
                if rejected {
                    self.staged = candidate;
                    self.observed = Some(config_state);
                    self.observed_schema = Some(schema.state.clone());
                    self.observed_pipeline = Some(pipeline_state.clone());
                    self.cached_schema = Some(schema);
                    self.cached_pipeline = Some(pipeline_state);
                    self.source_rejected = false;
                    return Ok(true);
                }
            }
            Err(_) => {}
            Ok(authority) if input_changed => {
                let staged = match candidate.configuration_candidate(authority) {
                    Ok(staged) => staged,
                    Err(errors) => {
                        let file_hash = match &config_state {
                            ConfigSourceState::Bytes(hash) => *hash,
                            ConfigSourceState::Failure(reason) => reason.reason_hash(),
                        };
                        let selected =
                            ConfigurationError::select_canonical(errors.iter().map(|error| {
                                ConfigurationError::from_reason(
                                    &candidate_error_reason(error, file_hash),
                                    error.to_string(),
                                )
                            }))
                            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?
                            .expect("execution staging returned at least one defect");
                        coordinator
                            .publish_configuration_rejection(store, *selected.detail, selected.message)?;
                        self.staged = candidate;
                        self.observed = Some(config_state);
                        self.observed_schema = Some(schema.state.clone());
                        self.observed_pipeline = Some(pipeline_state.clone());
                        self.cached_schema = Some(schema);
                        self.cached_pipeline = Some(pipeline_state);
                        self.source_rejected = false;
                        return Ok(false);
                    }
                };
                let published = coordinator.publish_configuration_candidate(store, staged);
                match published {
                    Err(CoordinatorError::PipelineAwaitingSchema(detail)) => {
                        // Not a failure: the Ready epoch keeps serving. The
                        // observed state stays, so the schema write
                        // source-walk is about to make re-runs this whole
                        // candidate; the caches hold what was just read.
                        tracing::warn!(%detail, "pipeline candidate waits for source-walk");
                        self.cached_schema = Some(schema.clone());
                        self.cached_pipeline = Some(pipeline_state);
                        return Ok(false);
                    }
                    published => {
                        published?;
                    }
                }
            }
            Ok(_) if rejected => {
                coordinator.heal_configuration_rejection(store)?;
            }
            Ok(_) => {}
        }

        if operational_configuration_changed(&self.active, &candidate) {
            coordinator.apply_operational_configuration(
                &candidate.store_config(),
                candidate.pipeline.max_dependency_depth,
            )?;
        }

        let restart = restart_changes(&self.active, &candidate);
        if !restart.is_empty() {
            coordinator.stage_restart_configuration(store, &restart)?;
        } else {
            coordinator.clear_restart_configuration(store)?;
        }

        apply_live_values(&mut self.active, &candidate);
        self.staged = candidate;
        self.observed = Some(config_state);
        self.observed_schema = Some(schema.state.clone());
        self.observed_pipeline = Some(pipeline_state.clone());
        self.cached_schema = Some(schema);
        self.cached_pipeline = Some(pipeline_state);
        self.source_rejected = false;
        // Any accepted input candidate can replace the effective importer
        // registry or its capabilities, including a candidate whose module
        // fails to open. Revalidate capability deps without treating an
        // operational-only edit as an asset-filesystem change.
        Ok(input_changed)
    }
}

/// Whether the store holds a configuration source error: the last observed
/// configuration, or its staging, was rejected.
fn rejected(store: &StoreReader) -> Result<bool, CoordinatorError> {
    store
        .configuration_source_error()
        .map(|error| error.is_some())
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
}

fn control_invalidation_for(
    batch: &crate::watcher::WatcherBatch,
    configuration: &Path,
    schema: &Path,
    pipeline: &Path,
) -> Option<ControlInvalidation> {
    let touches = |wanted: &Path| {
        batch
            .paths
            .iter()
            .any(|path| path == wanted || path.starts_with(wanted) || wanted.starts_with(path))
            || batch.renames.iter().any(|rename| {
                rename.from == wanted
                    || rename.to == wanted
                    || rename.from.starts_with(wanted)
                    || rename.to.starts_with(wanted)
                    || wanted.starts_with(&rename.from)
                    || wanted.starts_with(&rename.to)
            })
    };
    if touches(configuration) {
        return Some(ControlInvalidation::all());
    }
    let invalidation = ControlInvalidation {
        configuration: false,
        schema: touches(schema),
        pipeline: touches(pipeline),
    };
    (invalidation.schema || invalidation.pipeline).then_some(invalidation)
}

fn observe_configuration(path: &Path) -> ConfigObservation {
    let unavailable = |failure: ConfigurationSourceFailureCode, message: String| {
        let reason = DscpV1::ConfigurationSourceUnavailable {
            path: configuration_source_path(path),
            failure,
        };
        ConfigObservation {
            state: ConfigSourceState::Failure(Box::new(reason.clone())),
            outcome: Err((reason, message)),
        }
    };
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            let failure = match error.kind() {
                std::io::ErrorKind::NotFound => ConfigurationSourceFailureCode::Missing,
                std::io::ErrorKind::PermissionDenied => {
                    ConfigurationSourceFailureCode::PermissionDenied
                }
                _ => ConfigurationSourceFailureCode::IoDataLoss,
            };
            return unavailable(
                failure,
                format!("cannot inspect {}: {error}", path.display()),
            );
        }
    };
    if !metadata.is_file() {
        return unavailable(
            ConfigurationSourceFailureCode::InvalidFileType,
            format!(
                "configuration source {} is not a regular file",
                path.display()
            ),
        );
    }
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) => {
            let failure = if error.kind() == std::io::ErrorKind::PermissionDenied {
                ConfigurationSourceFailureCode::PermissionDenied
            } else {
                ConfigurationSourceFailureCode::IoDataLoss
            };
            return unavailable(failure, format!("cannot open {}: {error}", path.display()));
        }
    };
    let mut bytes = Vec::new();
    if let Err(error) = file.read_to_end(&mut bytes) {
        return unavailable(
            ConfigurationSourceFailureCode::IoDataLoss,
            format!("cannot read {}: {error}", path.display()),
        );
    }
    let hash = *blake3::hash(&bytes).as_bytes();
    let outcome = match std::str::from_utf8(&bytes) {
        Ok(source) => DaemonConfig::parse_staged(path, source).map_err(|errors| {
            let selected = ConfigurationError::select_canonical(errors.iter().map(|error| {
                ConfigurationError::from_reason(
                    &config_error_reason(error, &bytes),
                    error.to_string(),
                )
            }))
            .expect("configuration staging defects produce valid DSCP rows")
            .expect("parse_staged returns at least one defect");
            (*selected.detail, selected.message)
        }),
        Err(error) => Err((
            DscpV1::MalformedConfiguration { file_hash: hash },
            format!("configuration is not UTF-8: {error}"),
        )),
    };
    ConfigObservation {
        state: ConfigSourceState::Bytes(hash),
        outcome,
    }
}

fn input_configuration_changed(current: &DaemonConfig, candidate: &DaemonConfig) -> bool {
    current.assets != candidate.assets
        || current.modules != candidate.modules
        || current.targets != candidate.targets
}

fn operational_configuration_changed(current: &DaemonConfig, candidate: &DaemonConfig) -> bool {
    current.pipeline != candidate.pipeline
        || current.cas != candidate.cas
}

fn restart_changes(current: &DaemonConfig, candidate: &DaemonConfig) -> Vec<RestartOnlyChange> {
    let mut changes = Vec::new();
    if current.daemon.state_path != candidate.daemon.state_path {
        changes.push(RestartOnlyChange::StatePath(
            candidate.daemon.state_path.clone(),
        ));
    }
    if current.daemon.address != candidate.daemon.address {
        changes.push(RestartOnlyChange::Address(candidate.daemon.address));
    }
    if current.codegen.rs_mod_path != candidate.codegen.rs_mod_path {
        changes.push(RestartOnlyChange::RsModPath(
            candidate.codegen.rs_mod_path.clone(),
        ));
    }
    if current.codegen.auto_codegen != candidate.codegen.auto_codegen {
        changes.push(RestartOnlyChange::AutoCodegen(
            candidate.codegen.auto_codegen,
        ));
    }
    changes
}

fn apply_live_values(active: &mut DaemonConfig, candidate: &DaemonConfig) {
    active.assets = candidate.assets.clone();
    active.modules = candidate.modules.clone();
    active.targets = candidate.targets.clone();
    active.pipeline = candidate.pipeline.clone();
    active.cas = candidate.cas.clone();
    active.watch = candidate.watch.clone();
}

#[cfg(unix)]
fn configuration_source_path(path: &Path) -> ConfigurationSourcePath {
    use std::os::unix::ffi::OsStrExt;
    ConfigurationSourcePath::Unix(path.as_os_str().as_bytes().to_vec())
}

#[cfg(windows)]
fn configuration_source_path(path: &Path) -> ConfigurationSourcePath {
    use std::os::windows::ffi::OsStrExt;
    ConfigurationSourcePath::Windows(path.as_os_str().encode_wide().collect())
}

/// The failures a pass published around, as the loop's background error.
fn pass_failures(outcome: &PassOutcome) -> Option<String> {
    (!outcome.failures.is_empty()).then(|| outcome.failures.join("; "))
}

/// The RPC listener thread. It only accepts: every connection is served on a
/// thread of its own (`distill_rpc::capnp_transport`), and on `stop` the
/// listener closes them and waits a bounded grace for their threads.
fn spawn_rpc_loop(
    root: distill_rpc::Root,
    address: SocketAddr,
    stop: Arc<watch::Sender<bool>>,
    startup: mpsc::SyncSender<Result<SocketAddr, String>>,
) -> JoinHandle<Result<(), String>> {
    thread::Builder::new()
        .name("distill-rpc".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&runtime, async move {
                let listener = match distill_rpc::capnp_transport::StagedListener::bind(
                    root,
                    &address.to_string(),
                )
                .await
                {
                    Ok(listener) => listener,
                    Err(error) => {
                        let detail = error.to_string();
                        let _ = startup.send(Err(detail.clone()));
                        return Err(detail);
                    }
                };
                let local_address = listener.local_addr().map_err(|error| error.to_string())?;
                startup
                    .send(Ok(local_address))
                    .map_err(|error| error.to_string())?;
                listener
                    .serve_until(async move {
                        let _ = stop.subscribe().wait_for(|stopped| *stopped).await;
                    })
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .expect("failed to start distill RPC thread")
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUIET: Duration = Duration::from_millis(250);

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    /// Drive `schedule` as `ProcessLoop::run` does, on a simulated clock: a
    /// watcher change to `path` at each of `changes` (ms after `start`)
    /// arrives unless the loop's deadline comes first; at a deadline the due
    /// work starts, `pass` standing in for each pass. Runs until `end`;
    /// returns when the passes started, and the held-open warnings.
    fn drive(
        schedule: &mut Schedule,
        start: Instant,
        changes: &[u64],
        path: &Path,
        end: u64,
        mut pass: impl FnMut(&mut Schedule, Instant),
    ) -> (Vec<u64>, Vec<HeldOpen>) {
        let mut changes = changes.iter().copied().peekable();
        let mut passes = Vec::new();
        let mut warnings = Vec::new();
        loop {
            let deadline = schedule.deadline();
            match changes.peek() {
                Some(&at) if start + ms(at) < deadline => {
                    warnings.extend(schedule.change(start + ms(at), [path]));
                    changes.next();
                }
                _ if deadline > start + ms(end) => break,
                _ => {
                    schedule.take_cas_pass(deadline);
                    if schedule.take_pass(deadline) {
                        passes.push(deadline.duration_since(start).as_millis() as u64);
                        pass(schedule, deadline);
                    }
                }
            }
        }
        (passes, warnings)
    }

    fn every(step: u64, from: u64, to: u64) -> Vec<u64> {
        (from..=to).step_by(step as usize).collect()
    }

    const ASSET: &str = "/project/assets/a.png";

    #[test]
    fn a_burst_longer_than_the_window_reconciles_once_after_it_ends() {
        let start = Instant::now();
        let mut schedule = Schedule::new(start, QUIET);
        // A change every 100 ms for 2 s: eight windows long.
        let (passes, warnings) = drive(
            &mut schedule,
            start,
            &every(100, 0, 2000),
            Path::new(ASSET),
            5000,
            |_, _| {},
        );
        assert_eq!(passes, vec![2250], "one pass, a window after the last change");
        assert!(warnings.is_empty());
    }

    #[test]
    fn a_single_change_reconciles_after_the_quiet_window() {
        let start = Instant::now();
        let mut schedule = Schedule::new(start, QUIET);
        let (passes, _) = drive(&mut schedule, start, &[0], Path::new(ASSET), 5000, |_, _| {});
        assert_eq!(passes, vec![250]);
    }

    #[test]
    fn the_configured_quiet_window_is_honoured() {
        let start = Instant::now();
        let mut schedule = Schedule::new(start, ms(1000));
        let (passes, _) = drive(
            &mut schedule,
            start,
            &[0, 900, 3000],
            Path::new(ASSET),
            8000,
            |schedule, _| schedule.window.set_quiet(ms(60)),
        );
        // 900 ms apart is one burst under a 1 s window; a pass then adopts
        // a 60 ms window (as an accepted configuration would).
        assert_eq!(passes, vec![1900, 3060]);
    }

    #[test]
    fn a_failed_pass_retries_but_not_while_changes_arrive() {
        let start = Instant::now();
        let mut schedule = Schedule::new(start, QUIET);
        let mut failures = 1;
        let (passes, _) = drive(
            &mut schedule,
            start,
            &[0],
            Path::new(ASSET),
            5000,
            |schedule, now| {
                if failures > 0 {
                    failures -= 1;
                    schedule.request(now + RETRY_DELAY);
                }
            },
        );
        assert_eq!(passes, vec![250, 290], "the requeued pass retries");

        // The retry is due at 290, but changes resume at 260: it waits for
        // them to settle.
        let mut schedule = Schedule::new(start, QUIET);
        let mut failures = 1;
        let (passes, _) = drive(
            &mut schedule,
            start,
            &[0, 260, 360, 460],
            Path::new(ASSET),
            5000,
            |schedule, now| {
                if failures > 0 {
                    failures -= 1;
                    schedule.request(now + RETRY_DELAY);
                }
            },
        );
        assert_eq!(passes, vec![250, 710]);
    }

    #[test]
    fn a_requested_pass_waits_for_an_open_window() {
        // A codegen retry or an outside publication asks for a pass at 150
        // ms, during a burst: the burst's end runs one pass for both.
        let start = Instant::now();
        let mut schedule = Schedule::new(start, QUIET);
        schedule.request(start + ms(150));
        let (passes, _) = drive(
            &mut schedule,
            start,
            &every(100, 0, 1000),
            Path::new(ASSET),
            5000,
            |_, _| {},
        );
        assert_eq!(passes, vec![1250]);

        // With no change waiting, a request runs when asked.
        let mut schedule = Schedule::new(start, QUIET);
        schedule.request(start + ms(150));
        let (passes, _) = drive(&mut schedule, start, &[], Path::new(ASSET), 5000, |_, _| {});
        assert_eq!(passes, vec![150]);
    }

    #[test]
    fn a_watcher_failure_does_not_wait_for_the_window() {
        let start = Instant::now();
        let mut schedule = Schedule::new(start, QUIET);
        schedule.change(start, [Path::new(ASSET)]);
        schedule.fail(start + ms(100));
        assert_eq!(schedule.deadline(), start + ms(100));
        assert!(schedule.take_pass(start + ms(100)));
    }

    #[test]
    fn a_window_held_open_warns_naming_the_noisy_path_and_still_waits() {
        let start = Instant::now();
        let mut schedule = Schedule::new(start, QUIET);
        let noisy = Path::new("/project/assets/editor.log");
        // Rewritten every 100 ms for 7 s.
        let (passes, warnings) =
            drive(&mut schedule, start, &every(100, 0, 7000), noisy, 10_000, |_, _| {});
        assert_eq!(passes, vec![7250], "no cap: the pass waits for the end");
        assert_eq!(warnings.len(), 1, "once per episode: {warnings:?}");
        assert_eq!(warnings[0].open_for, ms(5000));
        assert_eq!(warnings[0].paths, vec![(noisy.to_path_buf(), 51)]);
    }

    #[test]
    fn the_cas_pass_runs_during_an_open_window() {
        let start = Instant::now();
        let mut schedule = Schedule::new(start, QUIET);
        let mut cas_passes = 0;
        let mut clock = start;
        // Changes every 100 ms across the 10 s CAS deadline.
        for at in every(100, 9800, 10_400) {
            let now = start + ms(at);
            while schedule.deadline() < now {
                clock = schedule.deadline();
                if schedule.take_cas_pass(clock) {
                    cas_passes += 1;
                }
                assert!(!schedule.take_pass(clock), "no pass while changes arrive");
            }
            schedule.change(now, [Path::new(ASSET)]);
        }
        assert_eq!(cas_passes, 1);
        assert_eq!(clock, start + CAS_PASS_INTERVAL);
        assert_eq!(schedule.deadline(), start + ms(10_650));
    }

    #[test]
    fn control_invalidation_rereads_only_the_named_source() {
        let config = PathBuf::from("/project/distill.toml");
        let schema = PathBuf::from("/project/schema.json");
        let pipeline = PathBuf::from("/project/pipeline.so");

        let schema_only = control_invalidation_for(
            &crate::watcher::WatcherBatch {
                paths: vec![schema.clone()],
                renames: Vec::new(),
            },
            &config,
            &schema,
            &pipeline,
        )
        .unwrap();
        assert!(!schema_only.configuration);
        assert!(schema_only.schema);
        assert!(!schema_only.pipeline);

        let module_only = control_invalidation_for(
            &crate::watcher::WatcherBatch {
                paths: Vec::new(),
                renames: vec![crate::watcher::WatcherRename {
                    from: pipeline.with_extension("old"),
                    to: pipeline.clone(),
                }],
            },
            &config,
            &schema,
            &pipeline,
        )
        .unwrap();
        assert!(!module_only.configuration);
        assert!(!module_only.schema);
        assert!(module_only.pipeline);

        let configuration = control_invalidation_for(
            &crate::watcher::WatcherBatch {
                paths: vec![config.clone()],
                renames: Vec::new(),
            },
            &config,
            &schema,
            &pipeline,
        )
        .unwrap();
        assert!(configuration.configuration);
        assert!(configuration.schema);
        assert!(configuration.pipeline);

        let nested_schema = PathBuf::from("/project/schema/schema.json");
        let schema_parent = control_invalidation_for(
            &crate::watcher::WatcherBatch {
                paths: vec![nested_schema.parent().unwrap().to_path_buf()],
                renames: Vec::new(),
            },
            &config,
            &nested_schema,
            &pipeline,
        )
        .unwrap();
        assert!(!schema_parent.configuration);
        assert!(schema_parent.schema);
        assert!(!schema_parent.pipeline);
    }
}
