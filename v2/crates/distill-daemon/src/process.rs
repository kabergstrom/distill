//! Long-lived daemon process supervisor.

use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use distill_schema::{ProjectSchemaAuthority, SchemaAuthorityError};
use distill_store::state::{
    CleanupDisposition, PipelinePoison, PipelinePoisonCode, PipelinePoisonOrigin,
};

use crate::codegen::CodegenService;
use crate::config::{config_error_reason, DaemonConfig, DaemonConfigError};
use crate::coordinator::{CoordinatorError, CoordinatorInitError, DaemonCoordinator};
use crate::watcher::{
    WatcherAction, WatcherControl, WatcherQueue, WatcherStartError, WatcherThread,
};
use distill_store::config::RestartOnlyChange;
use distill_store::state::{ConfigurationSourceFailureCode, ConfigurationSourcePath, DscpV1};

const DEBOUNCE: Duration = Duration::from_millis(40);
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub struct DaemonProcess {
    coordinator: Arc<DaemonCoordinator>,
    rpc_address: SocketAddr,
    stop: Arc<AtomicBool>,
    watcher: Option<WatcherThread>,
    coordinator_thread: Option<JoinHandle<()>>,
    rpc_thread: Option<JoinHandle<Result<(), String>>>,
    last_background_error: Arc<Mutex<Option<String>>>,
}

impl DaemonProcess {
    /// Start from the configured shared schema artifact. Production never
    /// substitutes the bootstrap-only table for `assets.schema_path`.
    pub fn start(config: DaemonConfig) -> Result<Self, DaemonProcessError> {
        Self::start_internal(config, true)
    }

    pub(crate) fn start_for_pack(config: DaemonConfig) -> Result<Self, DaemonProcessError> {
        Self::start_internal(config, false)
    }

    fn start_internal(config: DaemonConfig, serve_rpc: bool) -> Result<Self, DaemonProcessError> {
        let schema_bytes = std::fs::read(&config.assets.schema_path).map_err(|source| {
            DaemonProcessError::SchemaRead {
                path: config.assets.schema_path.clone(),
                source,
            }
        })?;
        let authority = ProjectSchemaAuthority::from_json(&schema_bytes)?;
        Self::start_with_authority_internal(config, authority, serve_rpc)
    }

    pub fn start_with_authority(
        config: DaemonConfig,
        authority: ProjectSchemaAuthority,
    ) -> Result<Self, DaemonProcessError> {
        Self::start_with_authority_internal(config, authority, true)
    }

    fn start_with_authority_internal(
        config: DaemonConfig,
        authority: ProjectSchemaAuthority,
        serve_rpc: bool,
    ) -> Result<Self, DaemonProcessError> {
        let targets = config.target_definitions(authority.identity())?;
        let coordinator = Arc::new(DaemonCoordinator::open(
            config.store_config(),
            config.asset_roots(),
            config.assets.lineage_manifest.clone(),
            targets,
            config.pipeline.max_dependency_depth,
        )?);
        coordinator.attach_build_backend();
        let mut config_watch = ConfigWatch::new(config.clone());
        let watcher_queue = Arc::new(Mutex::new(WatcherQueue::new()));
        // Arm both asset and control-file coverage before any candidate scan.
        // Root replacement is synchronously scanned by candidate publication;
        // the watcher then installs the new root and requests one catch-up scan.
        let watcher = WatcherThread::start(
            coordinator.scanner(),
            config_watch.control_paths(),
            Arc::clone(&watcher_queue),
        )?;
        let watcher_control = watcher.control();
        config_watch.reconcile(&coordinator, &watcher_control)?;
        let mut codegen = CodegenService::new(
            &coordinator,
            &config.codegen.rs_mod_path,
            config.codegen.auto_codegen,
        )
        .map_err(DaemonProcessError::Codegen)?;
        coordinator.reconcile_startup(&watcher_queue)?;
        reconcile_imports(&coordinator, true)?;
        coordinator.sweep_displaced_retention(unix_seconds())?;

        let stop = Arc::new(AtomicBool::new(false));
        let last_background_error = Arc::new(Mutex::new(None));
        if let Err(error) = codegen.run(&coordinator) {
            *lock(&last_background_error) = Some(error);
        }
        let coordinator_thread = Some(spawn_coordinator_loop(
            Arc::clone(&coordinator),
            Arc::clone(&watcher_queue),
            Arc::clone(&stop),
            Arc::clone(&last_background_error),
            config_watch,
            watcher_control,
            codegen,
        ));

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
                    stop.store(true, Ordering::Release);
                    let _ = rpc_thread.join();
                    return Err(DaemonProcessError::Rpc(error));
                }
                Err(error) => {
                    stop.store(true, Ordering::Release);
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
            coordinator_thread,
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
        lock(&self.last_background_error).clone()
    }

    pub fn wait(self) -> ! {
        loop {
            thread::park();
        }
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.watcher.take();
        if let Some(thread) = self.coordinator_thread.take() {
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

fn spawn_coordinator_loop(
    coordinator: Arc<DaemonCoordinator>,
    watcher: Arc<Mutex<WatcherQueue>>,
    stop: Arc<AtomicBool>,
    last_error: Arc<Mutex<Option<String>>>,
    mut config_watch: ConfigWatch,
    watcher_control: WatcherControl,
    mut codegen: CodegenService,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("distill-coordinator".to_owned())
        .spawn(move || {
            let mut next_retention_sweep = Instant::now() + RETENTION_SWEEP_INTERVAL;
            while !stop.load(Ordering::Acquire) {
                thread::sleep(DEBOUNCE);
                let action = lock(&watcher).take_live_action();
                if let WatcherAction::Failed(message) = &action {
                    *lock(&last_error) = Some(message.clone());
                    stop.store(true, Ordering::Release);
                    break;
                }
                let retry_action = action.clone();
                let reconcile_control = match &action {
                    WatcherAction::Batch(batch) => config_watch.affected_by(batch),
                    WatcherAction::FullRescan => true,
                    WatcherAction::None | WatcherAction::Failed(_) => false,
                };
                let result = (if reconcile_control {
                    config_watch.reconcile(&coordinator, &watcher_control)
                } else {
                    Ok(())
                })
                .and_then(|()| match action {
                    WatcherAction::None => Ok(()),
                    WatcherAction::Batch(batch) => coordinator
                        .reconcile_incremental(&batch)
                        .and_then(|_| reconcile_imports(&coordinator, false)),
                    WatcherAction::FullRescan => coordinator
                        .reconcile_startup(&watcher)
                        .and_then(|_| reconcile_imports(&coordinator, true)),
                    WatcherAction::Failed(_) => unreachable!("handled before reconciliation"),
                });
                let poison_result = coordinator.sync_runtime_pipeline_poison().map(|_| ());
                let retention_result = if Instant::now() >= next_retention_sweep {
                    next_retention_sweep = Instant::now() + RETENTION_SWEEP_INTERVAL;
                    coordinator
                        .sweep_displaced_retention(unix_seconds())
                        .map(|_| ())
                } else {
                    Ok(())
                };
                let result = result.and(poison_result).and(retention_result);
                let _ = coordinator.reap_retired_pipeline_epochs();
                match result {
                    Err(error) => {
                        *lock(&last_error) = Some(error.to_string());
                        lock(&watcher).requeue_action(retry_action);
                    }
                    Ok(()) => {
                        if let Err(error) = codegen.run(&coordinator) {
                            *lock(&last_error) = Some(error);
                        }
                    }
                }
            }
        })
        .expect("failed to start distill coordinator thread")
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

struct SchemaObservation {
    state: ArtifactSourceState,
    outcome: Result<ProjectSchemaAuthority, String>,
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
            outcome: ProjectSchemaAuthority::from_json(&bytes).map_err(|error| error.to_string()),
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

struct ConfigWatch {
    path: PathBuf,
    active: DaemonConfig,
    observed: Option<ConfigSourceState>,
    observed_schema: Option<ArtifactSourceState>,
    observed_pipeline: Option<ArtifactSourceState>,
    rejected: bool,
}

impl ConfigWatch {
    fn new(active: DaemonConfig) -> Self {
        Self {
            path: active.source_path.clone(),
            active,
            observed: None,
            observed_schema: None,
            observed_pipeline: None,
            rejected: false,
        }
    }

    fn control_paths(&self) -> [PathBuf; 3] {
        [
            self.path.clone(),
            self.active.assets.schema_path.clone(),
            self.active.modules.pipeline_dylib.clone(),
        ]
    }

    fn affected_by(&self, batch: &crate::watcher::WatcherBatch) -> bool {
        let paths = self
            .control_paths()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        batch.paths.iter().any(|path| paths.contains(path))
            || batch
                .renames
                .iter()
                .any(|rename| paths.contains(&rename.from) || paths.contains(&rename.to))
    }

    fn reconcile(
        &mut self,
        coordinator: &DaemonCoordinator,
        watcher: &WatcherControl,
    ) -> Result<(), CoordinatorError> {
        let observation = observe_configuration(&self.path);
        match observation.outcome {
            Err((reason, message)) => {
                if self.observed.as_ref() == Some(&observation.state) {
                    return Ok(());
                }
                coordinator.publish_configuration_rejection(reason, message)?;
                self.rejected = true;
                self.observed = Some(observation.state);
                Ok(())
            }
            Ok(candidate) => {
                self.reconcile_valid(coordinator, watcher, observation.state, candidate)
            }
        }
    }

    fn reconcile_valid(
        &mut self,
        coordinator: &DaemonCoordinator,
        watcher: &WatcherControl,
        config_state: ConfigSourceState,
        candidate: DaemonConfig,
    ) -> Result<(), CoordinatorError> {
        watcher
            .replace_paths([
                self.path.clone(),
                candidate.assets.schema_path.clone(),
                candidate.modules.pipeline_dylib.clone(),
            ])
            .map_err(CoordinatorError::InvalidManifest)?;
        let schema = observe_schema_source(&candidate.assets.schema_path);
        let pipeline_state = observe_artifact_source(&candidate.modules.pipeline_dylib);
        let config_changed = self.observed.as_ref() != Some(&config_state);
        let schema_changed = self.observed_schema.as_ref() != Some(&schema.state);
        let pipeline_changed = self.observed_pipeline.as_ref() != Some(&pipeline_state);
        if !config_changed && !schema_changed && !pipeline_changed && !self.rejected {
            return Ok(());
        }

        let input_changed = self.observed.is_none()
            || input_configuration_changed(&self.active, &candidate)
            || schema_changed
            || pipeline_changed
            || self.rejected;

        match schema.outcome {
            Err(message) if input_changed => {
                let poison = PipelinePoison::new(
                    PipelinePoisonCode::CandidateValidation,
                    PipelinePoisonOrigin::CandidateOpen,
                    CleanupDisposition::None,
                    message,
                )
                .expect("schema candidate poison tuple is valid");
                coordinator.publish_pipeline_rejection(poison)?;
            }
            Err(_) => {}
            Ok(authority) if input_changed => {
                let authority = Arc::new(authority);
                let requirements = candidate
                    .candidate_requirements(&authority)
                    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                let targets = candidate
                    .target_definitions(authority.identity())
                    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                let build_targets = candidate
                    .build_targets(authority.identity())
                    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                coordinator.publish_configuration_candidate(
                    crate::coordinator::ConfigurationCandidate {
                        roots: candidate.asset_roots(),
                        lineage_destination: candidate.assets.lineage_manifest.clone(),
                        targets,
                        build_targets,
                        pipeline_source: candidate.modules.pipeline_dylib.clone(),
                        requirements,
                        schema_authority: Arc::clone(&authority),
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
        self.observed = Some(config_state);
        self.observed_schema = Some(schema.state);
        self.observed_pipeline = Some(pipeline_state);
        self.rejected = false;
        Ok(())
    }
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
        Ok(source) => DaemonConfig::parse(path, source).map_err(|error| {
            let reason = config_error_reason(&error, &bytes);
            (reason, error.to_string())
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

#[cfg(not(any(unix, windows)))]
fn configuration_source_path(path: &Path) -> ConfigurationSourcePath {
    ConfigurationSourcePath::Unix(path.to_string_lossy().as_bytes().to_vec())
}

fn reconcile_imports(
    coordinator: &DaemonCoordinator,
    revalidate_all: bool,
) -> Result<(), CoordinatorError> {
    let work = coordinator.pending_file_work()?;
    let directories = if revalidate_all {
        coordinator.reconcile_directory_imports()
    } else {
        coordinator.reconcile_directory_imports_affected(&work)
    };
    let reconcile = directories.and_then(|_| {
        if revalidate_all {
            coordinator.reconcile_watched_imports()
        } else {
            coordinator.reconcile_watched_imports_affected(&work)
        }
    });
    if reconcile.is_ok() {
        coordinator.acknowledge_file_work(&work)?;
    }
    let poison = coordinator.sync_runtime_pipeline_poison().map(|_| ());
    reconcile.and(poison)
}

fn spawn_rpc_loop(
    root: distill_rpc::Root,
    address: SocketAddr,
    stop: Arc<AtomicBool>,
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
                        while !stop.load(Ordering::Acquire) {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    })
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .expect("failed to start distill RPC thread")
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
