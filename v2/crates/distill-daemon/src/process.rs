//! Long-lived daemon process supervisor.
//!
//! The process loop runs on the coordinator's authority thread as its
//! [`Driver`]: the watcher feeds it through the authority inbox, and every
//! reconciliation it starts publishes from that thread.

use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use distill_schema::{ProjectSchemaAuthority, SchemaAuthorityError};
use distill_store::state::{
    CleanupDisposition, ConfigurationError, PipelineFailure, PipelineFailureCode,
    PipelineFailureOrigin,
};

use crate::authority::Driver;
use crate::codegen::CodegenService;
use crate::config::{candidate_error_reason, config_error_reason, DaemonConfig, DaemonConfigError};
use crate::coordinator::{CoordinatorError, CoordinatorInitError, DaemonCoordinator};
use crate::scanner::DaemonOwnedDirectoryKind;
use crate::watcher::{
    WatcherAction, WatcherControl, WatcherEvent, WatcherQueue, WatcherSink, WatcherStartError,
    WatcherThread,
};
use distill_store::config::RestartOnlyChange;
use distill_store::state::{ConfigurationSourceFailureCode, ConfigurationSourcePath, DscpV1};

/// How often the process loop takes the watcher queue (the debounce) and
/// checks for a runtime pipeline failure or a drained retired epoch. The
/// last two become authority messages when builds become jobs (phase 6).
const DEBOUNCE: Duration = Duration::from_millis(40);
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub struct DaemonProcess {
    coordinator: Arc<DaemonCoordinator>,
    rpc_address: SocketAddr,
    stop: Arc<watch::Sender<bool>>,
    watcher: Option<WatcherThread>,
    rpc_thread: Option<JoinHandle<Result<(), String>>>,
    last_background_error: watch::Receiver<Option<String>>,
}

impl DaemonProcess {
    /// Start from the configured shared schema artifact. Production never
    /// substitutes the bootstrap-only table for `assets.schema_path`.
    pub fn start(config: DaemonConfig) -> Result<Self, DaemonProcessError> {
        Self::start_internal(config, true, Vec::new())
    }

    pub(crate) fn start_for_pack(
        config: DaemonConfig,
        package_output: &Path,
    ) -> Result<Self, DaemonProcessError> {
        Self::start_internal(
            config,
            false,
            vec![(
                DaemonOwnedDirectoryKind::PackageOutput,
                package_output.to_path_buf(),
            )],
        )
    }

    fn start_internal(
        config: DaemonConfig,
        serve_rpc: bool,
        daemon_owned: Vec<(DaemonOwnedDirectoryKind, PathBuf)>,
    ) -> Result<Self, DaemonProcessError> {
        let schema_bytes = std::fs::read(&config.assets.schema_path).map_err(|source| {
            DaemonProcessError::SchemaRead {
                path: config.assets.schema_path.clone(),
                source,
            }
        })?;
        let authority = ProjectSchemaAuthority::from_json(&schema_bytes)?;
        Self::start_with_authority_internal(config, authority, serve_rpc, daemon_owned)
    }

    pub fn start_with_authority(
        config: DaemonConfig,
        authority: ProjectSchemaAuthority,
    ) -> Result<Self, DaemonProcessError> {
        Self::start_with_authority_internal(config, authority, true, Vec::new())
    }

    fn start_with_authority_internal(
        config: DaemonConfig,
        authority: ProjectSchemaAuthority,
        serve_rpc: bool,
        daemon_owned: Vec<(DaemonOwnedDirectoryKind, PathBuf)>,
    ) -> Result<Self, DaemonProcessError> {
        let targets = config.target_definitions(authority.identity())?;
        let coordinator = Arc::new(DaemonCoordinator::open(
            config.store_config(),
            config.asset_roots(),
            targets,
            config.pipeline.max_dependency_depth,
        )?);
        for (kind, path) in daemon_owned {
            coordinator
                .scanner()
                .retain_daemon_owned_directory(kind, path)
                .map_err(|error| {
                    DaemonProcessError::CoordinatorInit(CoordinatorInitError::Scan(error))
                })?;
        }
        coordinator.attach_build_backend();
        let config_watch = ConfigWatch::new(config.clone());
        let inbox = coordinator.authority_sender().clone();
        let sink: WatcherSink = {
            let inbox = inbox.clone();
            Arc::new(move |event| inbox.watch(event))
        };
        // Arm both asset and control-file coverage before any candidate scan.
        // Root replacement is synchronously scanned by candidate publication;
        // the watcher then installs the new root and requests one catch-up scan.
        let watcher = WatcherThread::start(coordinator.scanner(), config_watch.control_paths(), sink)?;
        let watcher_control = watcher.control();
        // Codegen recovery writes the store: it starts on the authority.
        let codegen = coordinator
            .on_authority(|| {
                CodegenService::new(
                    &coordinator,
                    &config.codegen.rs_mod_path,
                    config.codegen.auto_codegen,
                )
            })
            .map_err(DaemonProcessError::Codegen)?;

        let stop = Arc::new(watch::Sender::new(false));
        let (errors, last_background_error) = watch::channel(None);
        let (ready, started) = mpsc::sync_channel(1);
        let mut driver = ProcessDriver {
            coordinator: Arc::clone(&coordinator),
            queue: WatcherQueue::new(),
            config_watch,
            watcher_control,
            codegen,
            stop: Arc::clone(&stop),
            errors,
            due: Some(Instant::now()),
            publications: 0,
            next_retention_sweep: Instant::now() + RETENTION_SWEEP_INTERVAL,
        };
        // Startup runs on the authority before the loop: events the watcher
        // sends meanwhile wait in the inbox for the driver.
        inbox.attach(Box::new(move || match driver.startup() {
            Ok(()) => {
                let _ = ready.send(Ok(()));
                Some(Box::new(driver) as Box<dyn Driver>)
            }
            Err(error) => {
                let _ = ready.send(Err(error));
                None
            }
        }));
        started.recv().map_err(|_| {
            DaemonProcessError::Rpc("the authority stopped during startup".to_owned())
        })??;

        let (rpc_address, rpc_thread) = if serve_rpc {
            let (address_tx, address_rx) = mpsc::sync_channel(1);
            let rpc_thread = spawn_rpc_loop(
                coordinator.server().root(),
                config.daemon.address,
                Arc::clone(&stop),
                address_tx,
            );
            let rpc_address = match address_rx.recv() {
                Ok(Ok(address)) => address,
                Ok(Err(error)) => {
                    stop.send_replace(true);
                    inbox.detach();
                    let _ = rpc_thread.join();
                    return Err(DaemonProcessError::Rpc(error));
                }
                Err(error) => {
                    stop.send_replace(true);
                    inbox.detach();
                    let _ = rpc_thread.join();
                    return Err(DaemonProcessError::Rpc(format!(
                        "RPC startup channel closed: {error}"
                    )));
                }
            };
            (rpc_address, Some(rpc_thread))
        } else {
            (config.daemon.address, None)
        };

        Ok(Self {
            coordinator,
            rpc_address,
            stop,
            watcher: Some(watcher),
            rpc_thread,
            last_background_error,
        })
    }

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
        self.coordinator.authority_sender().detach();
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

/// The process loop, run by the authority.
struct ProcessDriver {
    coordinator: Arc<DaemonCoordinator>,
    queue: WatcherQueue,
    config_watch: ConfigWatch,
    watcher_control: WatcherControl,
    codegen: CodegenService,
    stop: Arc<watch::Sender<bool>>,
    errors: watch::Sender<Option<String>>,
    /// When the next pass is due; `None` until something asks for one.
    due: Option<Instant>,
    /// The server's publication count the last pass saw.
    publications: u64,
    next_retention_sweep: Instant,
}

impl ProcessDriver {
    fn startup(&mut self) -> Result<(), DaemonProcessError> {
        // Retain an existing daemon-owned output before the first candidate
        // performs its mandatory startup scan. Otherwise an authored symlink
        // to that output can be misclassified as a root escape during the
        // narrow gap between candidate publication and service construction.
        self.config_watch.reconcile(
            &self.coordinator,
            &self.watcher_control,
            ControlInvalidation::all(),
        )?;
        self.watcher_control
            .replace_roots(&self.coordinator.scanner())
            .map_err(CoordinatorError::InvalidManifest)?;
        let started = Instant::now();
        self.coordinator.reconcile_startup(&mut self.queue)?;
        tracing::info!(elapsed = ?started.elapsed(), "startup scan reconciled");
        reconcile_imports(&self.coordinator, true, false)?;
        tracing::info!(elapsed = ?started.elapsed(), "startup imports reconciled");
        self.coordinator.sweep_displaced_retention(unix_seconds())?;

        let recovery_diagnostic = [
            self.coordinator
                .authoring_service()
                .take_startup_recovery_diagnostic(),
            self.codegen.take_startup_recovery_diagnostic(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        if !recovery_diagnostic.is_empty() {
            self.errors.send_replace(Some(recovery_diagnostic.join("; ")));
        }
        if let Err(error) = self.codegen.run(&self.coordinator) {
            self.errors.send_replace(Some(error));
        }
        Ok(())
    }

    /// One pass of the loop: reconcile what the watcher queued.
    fn tick(&mut self) -> bool {
        let action = self.queue.take_live_action();
        if let WatcherAction::Failed(message) = &action {
            tracing::error!(%message, "watcher failed; stopping the coordinator");
            self.errors.send_replace(Some(message.clone()));
            self.stop.send_replace(true);
            return false;
        }
        let retry_action = action.clone();
        let started = Instant::now();
        match &action {
            WatcherAction::Batch(batch) => {
                tracing::debug!(batch = ?batch, "reconciling watcher batch")
            }
            WatcherAction::FullRescan => tracing::info!("reconciling full rescan"),
            WatcherAction::None | WatcherAction::Failed(_) => {}
        }
        let reconciled = !matches!(action, WatcherAction::None);
        let control_invalidation = match &action {
            WatcherAction::Batch(batch) => self.config_watch.invalidation_for(batch),
            WatcherAction::FullRescan => Some(ControlInvalidation::all()),
            WatcherAction::None | WatcherAction::Failed(_) => None,
        };
        let coordinator = &self.coordinator;
        let result = match control_invalidation {
            Some(invalidation) => {
                self.config_watch
                    .reconcile(coordinator, &self.watcher_control, invalidation)
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
        .and_then(|capabilities_changed| match action {
            WatcherAction::None => Ok(()),
            WatcherAction::Batch(batch) => coordinator
                .reconcile_incremental(&batch)
                .and_then(|_| reconcile_imports(coordinator, false, capabilities_changed)),
            WatcherAction::FullRescan => coordinator
                .reconcile_startup(&mut self.queue)
                .and_then(|_| reconcile_imports(coordinator, true, false)),
            WatcherAction::Failed(_) => unreachable!("handled before reconciliation"),
        });
        let failure_result = coordinator.sync_runtime_pipeline_failure().map(|_| ());
        let retention_result = if Instant::now() >= self.next_retention_sweep {
            self.next_retention_sweep = Instant::now() + RETENTION_SWEEP_INTERVAL;
            coordinator
                .sweep_displaced_retention(unix_seconds())
                .map(|_| ())
        } else {
            Ok(())
        };
        let result = result.and(failure_result).and(retention_result);
        let _ = coordinator.reap_retired_pipeline_epochs();
        match result {
            Err(error) => {
                tracing::warn!(%error, "reconciliation failed; requeued");
                self.errors.send_replace(Some(error.to_string()));
                self.queue.requeue_action(retry_action);
            }
            Ok(()) => {
                if reconciled {
                    tracing::info!(elapsed = ?started.elapsed(), "reconciled");
                }
                if let Err(error) = self.codegen.run(&self.coordinator) {
                    tracing::warn!(%error, "codegen failed");
                    self.errors.send_replace(Some(error));
                    self.schedule(Instant::now() + DEBOUNCE);
                }
            }
        }
        true
    }

    /// Run a pass by `at` at the latest.
    fn schedule(&mut self, at: Instant) {
        self.due = Some(self.due.map_or(at, |due| due.min(at)));
    }
}

impl Driver for ProcessDriver {
    fn watch(&mut self, event: WatcherEvent) {
        self.queue.push(event);
        // Debounce: a burst of events is reconciled together.
        self.schedule(Instant::now() + DEBOUNCE);
    }

    fn deadline(&self) -> Instant {
        self.due
            .map_or(self.next_retention_sweep, |due| due.min(self.next_retention_sweep))
    }

    fn fire(&mut self) -> bool {
        self.due = None;
        let keep = self.tick();
        self.publications = self.coordinator.server_handle().publication_count();
        if self.queue.has_pending() {
            // A failed pass requeued its work.
            self.schedule(Instant::now() + DEBOUNCE);
        }
        keep
    }

    fn poke(&mut self) {
        self.schedule(Instant::now());
    }

    fn ran_job(&mut self) {
        // A publication from elsewhere (an RPC write, an import) still needs
        // codegen.
        if self.coordinator.server_handle().publication_count() != self.publications {
            self.schedule(Instant::now());
        }
    }
}

fn unix_seconds() -> i64 {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    i64::try_from(seconds).unwrap_or(i64::MAX)
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
    rejected: bool,
    source_rejected: bool,
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
            rejected: false,
            source_rejected: false,
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
            if self.rejected && self.source_rejected {
                self.refresh_cached_artifacts(invalidation);
                return Ok(false);
            }
            let state = self
                .observed
                .clone()
                .expect("a non-configuration invalidation follows initial reconciliation");
            return self.reconcile_valid(
                coordinator,
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
                coordinator.publish_configuration_rejection(reason, message)?;
                self.rejected = true;
                self.source_rejected = true;
                self.observed = Some(observation.state);
                Ok(false)
            }
            Ok(candidate) => self.reconcile_valid(
                coordinator,
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
        watcher: &WatcherControl,
        config_state: ConfigSourceState,
        candidate: DaemonConfig,
        invalidation: ControlInvalidation,
    ) -> Result<bool, CoordinatorError> {
        if invalidation.configuration {
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
        if !config_changed && !schema_changed && !pipeline_changed && !self.rejected {
            return Ok(false);
        }

        let input_changed = self.observed.is_none()
            || input_configuration_changed(&self.active, &candidate)
            || schema_changed
            || pipeline_changed
            || self.rejected;

        match &schema.outcome {
            Err(message) if input_changed => {
                let failure = PipelineFailure::new(
                    PipelineFailureCode::CandidateValidation,
                    PipelineFailureOrigin::CandidateOpen,
                    CleanupDisposition::None,
                    message.clone(),
                )
                .expect("schema candidate failure tuple is valid");
                if self.rejected {
                    coordinator.publish_pipeline_rejection_healing_configuration(failure)?;
                } else {
                    coordinator.publish_pipeline_rejection(failure)?;
                }
                if self.rejected {
                    self.staged = candidate;
                    self.observed = Some(config_state);
                    self.observed_schema = Some(schema.state.clone());
                    self.observed_pipeline = Some(pipeline_state.clone());
                    self.cached_schema = Some(schema);
                    self.cached_pipeline = Some(pipeline_state);
                    self.rejected = false;
                    self.source_rejected = false;
                    return Ok(true);
                }
            }
            Err(_) => {}
            Ok(authority) if input_changed => {
                let staged = match candidate.stage_execution_candidate(authority) {
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
                            .publish_configuration_rejection(*selected.detail, selected.message)?;
                        self.staged = candidate;
                        self.observed = Some(config_state);
                        self.observed_schema = Some(schema.state.clone());
                        self.observed_pipeline = Some(pipeline_state.clone());
                        self.cached_schema = Some(schema);
                        self.cached_pipeline = Some(pipeline_state);
                        self.rejected = true;
                        self.source_rejected = false;
                        return Ok(false);
                    }
                };
                coordinator.publish_configuration_candidate(
                    crate::coordinator::ConfigurationCandidate {
                        roots: candidate.asset_roots(),
                        targets: staged.targets,
                        build_targets: staged.build_targets,
                        pipeline_source: candidate.modules.pipeline_dylib.clone(),
                        requirements: staged.requirements,
                        schema_authority: Arc::clone(authority),
                    },
                )?;
            }
            Ok(_) if self.rejected => {
                coordinator.heal_configuration_rejection()?;
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
            coordinator.stage_restart_configuration(&restart)?;
        } else {
            coordinator.clear_restart_configuration()?;
        }

        apply_live_values(&mut self.active, &candidate);
        self.staged = candidate;
        self.observed = Some(config_state);
        self.observed_schema = Some(schema.state.clone());
        self.observed_pipeline = Some(pipeline_state.clone());
        self.cached_schema = Some(schema);
        self.cached_pipeline = Some(pipeline_state);
        self.rejected = false;
        self.source_rejected = false;
        // Any accepted input candidate can replace the effective importer
        // registry or its capabilities, including a candidate whose module
        // fails to open. Revalidate capability deps without treating an
        // operational-only edit as an asset-filesystem change.
        Ok(input_changed)
    }
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
        || current.daemon.displaced_retention_days != candidate.daemon.displaced_retention_days
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
    active.daemon.displaced_retention_days = candidate.daemon.displaced_retention_days;
}

fn configuration_source_path(path: &Path) -> ConfigurationSourcePath {
    use std::os::unix::ffi::OsStrExt;
    ConfigurationSourcePath::Unix(path.as_os_str().as_bytes().to_vec())
}

fn reconcile_imports(
    coordinator: &DaemonCoordinator,
    revalidate_all: bool,
    capabilities_changed: bool,
) -> Result<(), CoordinatorError> {
    let work = coordinator.pending_file_work()?;
    let directories = if revalidate_all {
        coordinator.reconcile_directory_imports()
    } else {
        coordinator.reconcile_directory_imports_affected(&work, capabilities_changed)
    };
    let reconcile = directories.and_then(|_| {
        if revalidate_all {
            coordinator.reconcile_watched_imports()
        } else {
            coordinator.reconcile_watched_imports_affected(&work, capabilities_changed)
        }
    });
    if reconcile.is_ok() {
        coordinator.acknowledge_file_work(&work)?;
    }
    let failure = coordinator.sync_runtime_pipeline_failure().map(|_| ());
    reconcile.and(failure)
}

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
