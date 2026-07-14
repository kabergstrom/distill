//! Long-lived daemon process supervisor.

use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use distill_core::attestation::CompiledTypeTable;

use crate::config::{config_error_reason, DaemonConfig, DaemonConfigError};
use crate::coordinator::{CoordinatorError, CoordinatorInitError, DaemonCoordinator};
use crate::epoch::CandidateRequirements;
use crate::watcher::{WatcherQueue, WatcherThread};
use distill_store::config::RestartOnlyChange;
use distill_store::state::{ConfigurationSourceFailureCode, ConfigurationSourcePath, DscpV1};

const WATCH_CAPACITY: usize = 65_536;
const DEBOUNCE: Duration = Duration::from_millis(40);

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
    pub fn start(
        config: DaemonConfig,
        compiled: CompiledTypeTable,
    ) -> Result<Self, DaemonProcessError> {
        let requirements = config.candidate_requirements(&compiled)?;
        let targets = config.target_definitions(&compiled)?;
        let coordinator = Arc::new(DaemonCoordinator::open(
            config.store_config(),
            config.asset_roots(),
            config.assets.lineage_manifest.clone(),
            targets,
            config.pipeline.max_dependency_depth,
        )?);
        let watcher_queue = Arc::new(Mutex::new(WatcherQueue::new(WATCH_CAPACITY)));
        let watcher =
            WatcherThread::start(coordinator.scanner(), Arc::clone(&watcher_queue), DEBOUNCE)?;
        coordinator.reconcile_startup(&watcher_queue)?;
        let mut config_watch = ConfigWatch::new(config.clone(), compiled.clone());
        config_watch.reconcile(&coordinator)?;
        let mut pipeline_watch =
            PipelineWatch::new(config.modules.pipeline_dylib.clone(), requirements);
        pipeline_watch.reconcile(&coordinator)?;
        reconcile_imports(&coordinator)?;

        let stop = Arc::new(AtomicBool::new(false));
        let last_background_error = Arc::new(Mutex::new(None));
        let coordinator_thread = Some(spawn_coordinator_loop(
            Arc::clone(&coordinator),
            Arc::clone(&watcher_queue),
            Arc::clone(&stop),
            Arc::clone(&last_background_error),
            config_watch,
            pipeline_watch,
        ));

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

        Ok(Self {
            coordinator,
            rpc_address,
            stop,
            watcher: Some(watcher),
            coordinator_thread,
            rpc_thread: Some(rpc_thread),
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
    Watch(crate::scanner::ScanError),
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
impl From<crate::scanner::ScanError> for DaemonProcessError {
    fn from(error: crate::scanner::ScanError) -> Self {
        Self::Watch(error)
    }
}

fn spawn_coordinator_loop(
    coordinator: Arc<DaemonCoordinator>,
    watcher: Arc<Mutex<WatcherQueue>>,
    stop: Arc<AtomicBool>,
    last_error: Arc<Mutex<Option<String>>>,
    mut config_watch: ConfigWatch,
    mut pipeline_watch: PipelineWatch,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("distill-coordinator".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                thread::sleep(DEBOUNCE);
                let action = {
                    let mut watcher = lock(&watcher);
                    if watcher.take_live_overflow() {
                        WatchAction::FullRescan
                    } else {
                        WatchAction::Events(watcher.take_live_batch())
                    }
                };
                let result = config_watch.reconcile(&coordinator).and_then(|update| {
                    if let Some((path, requirements, observed)) = update {
                        pipeline_watch.reconfigure_published(path, requirements, observed);
                    }
                    match action {
                        WatchAction::FullRescan => coordinator
                            .reconcile_full_scan()
                            .and_then(|_| reconcile_imports(&coordinator)),
                        WatchAction::Events(events) if events.is_empty() => Ok(()),
                        WatchAction::Events(events) => coordinator
                            .apply_watcher_batch(events)
                            .and_then(|_| reconcile_imports(&coordinator)),
                    }
                });
                let result = result.and_then(|_| pipeline_watch.reconcile(&coordinator));
                let _ = coordinator.reap_retired_pipeline_epochs();
                if let Err(error) = result {
                    *lock(&last_error) = Some(error.to_string());
                    lock(&watcher).force_overflow();
                }
            }
        })
        .expect("failed to start distill coordinator thread")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PipelineSourceState {
    Missing,
    Unreadable(std::io::ErrorKind),
    Bytes([u8; 32]),
}

struct PipelineWatch {
    path: std::path::PathBuf,
    requirements: CandidateRequirements,
    observed: Option<PipelineSourceState>,
}

impl PipelineWatch {
    fn new(path: std::path::PathBuf, requirements: CandidateRequirements) -> Self {
        Self {
            path,
            requirements,
            observed: None,
        }
    }

    fn reconcile(&mut self, coordinator: &DaemonCoordinator) -> Result<(), CoordinatorError> {
        let state = observe_pipeline_source(&self.path);
        if self.observed.as_ref() == Some(&state) {
            return Ok(());
        }
        coordinator.publish_pipeline_candidate(&self.path, self.requirements.clone())?;
        self.observed = Some(state);
        Ok(())
    }

    fn reconfigure_published(
        &mut self,
        path: PathBuf,
        requirements: CandidateRequirements,
        observed: PipelineSourceState,
    ) {
        self.path = path;
        self.requirements = requirements;
        self.observed = Some(observed);
    }
}

fn observe_pipeline_source(path: &Path) -> PipelineSourceState {
    match std::fs::read(path) {
        Ok(bytes) => PipelineSourceState::Bytes(*blake3::hash(&bytes).as_bytes()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => PipelineSourceState::Missing,
        Err(error) => PipelineSourceState::Unreadable(error.kind()),
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
    compiled: CompiledTypeTable,
    active: DaemonConfig,
    last_valid: DaemonConfig,
    observed: Option<ConfigSourceState>,
    rejected: bool,
}

impl ConfigWatch {
    fn new(active: DaemonConfig, compiled: CompiledTypeTable) -> Self {
        Self {
            path: active.source_path.clone(),
            compiled,
            last_valid: active.clone(),
            active,
            observed: None,
            rejected: false,
        }
    }

    fn reconcile(
        &mut self,
        coordinator: &DaemonCoordinator,
    ) -> Result<Option<(PathBuf, CandidateRequirements, PipelineSourceState)>, CoordinatorError>
    {
        let observation = observe_configuration(&self.path);
        if self.observed.as_ref() == Some(&observation.state) {
            return Ok(None);
        }
        let update = match observation.outcome {
            Err((reason, message)) => {
                coordinator.publish_configuration_rejection(reason, message)?;
                self.rejected = true;
                None
            }
            Ok(candidate) => {
                let input_changed = input_configuration_changed(&self.active, &candidate);
                let pipeline = if input_changed {
                    let requirements = candidate
                        .candidate_requirements(&self.compiled)
                        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                    let targets = candidate
                        .target_definitions(&self.compiled)
                        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                    let path = candidate.modules.pipeline_dylib.clone();
                    let observed = observe_pipeline_source(&path);
                    coordinator.publish_configuration_candidate(
                        candidate.asset_roots(),
                        candidate.assets.lineage_manifest.clone(),
                        targets,
                        &path,
                        requirements.clone(),
                    )?;
                    Some((path, requirements, observed))
                } else if self.rejected {
                    coordinator.heal_configuration_rejection()?;
                    None
                } else {
                    None
                };

                if operational_configuration_changed(&self.active, &candidate) {
                    coordinator.apply_operational_configuration(
                        &candidate.store_config(),
                        candidate.pipeline.max_dependency_depth,
                    )?;
                }

                let restart = restart_changes(&self.last_valid, &candidate);
                if !restart.is_empty() {
                    coordinator.stage_restart_configuration(&restart)?;
                }

                apply_live_values(&mut self.active, &candidate);
                self.last_valid = candidate;
                self.rejected = false;
                pipeline
            }
        };
        self.observed = Some(observation.state);
        Ok(update)
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

fn reconcile_imports(coordinator: &DaemonCoordinator) -> Result<(), CoordinatorError> {
    coordinator.reconcile_directory_imports()?;
    coordinator.reconcile_watched_imports()?;
    Ok(())
}

enum WatchAction {
    FullRescan,
    Events(Vec<crate::coordinator::WatcherPathEvent>),
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
