//! Long-lived daemon process supervisor.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use distill_core::attestation::CompiledTypeTable;

use crate::config::{DaemonConfig, DaemonConfigError};
use crate::coordinator::{CoordinatorError, CoordinatorInitError, DaemonCoordinator};
use crate::watcher::{WatcherQueue, WatcherThread};

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
        let targets = config.target_definitions(&compiled)?;
        let coordinator = Arc::new(DaemonCoordinator::open(
            config.store_config(),
            config.asset_roots(),
            config.assets.lineage_manifest.clone(),
            targets,
        )?);
        let watcher_queue = Arc::new(Mutex::new(WatcherQueue::new(WATCH_CAPACITY)));
        let watcher =
            WatcherThread::start(coordinator.scanner(), Arc::clone(&watcher_queue), DEBOUNCE)?;
        coordinator.reconcile_startup(&watcher_queue)?;

        let stop = Arc::new(AtomicBool::new(false));
        let last_background_error = Arc::new(Mutex::new(None));
        let coordinator_thread = Some(spawn_coordinator_loop(
            Arc::clone(&coordinator),
            Arc::clone(&watcher_queue),
            Arc::clone(&stop),
            Arc::clone(&last_background_error),
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
                let result = match action {
                    WatchAction::FullRescan => coordinator
                        .reconcile_full_scan()
                        .and_then(|_| reconcile_imports(&coordinator)),
                    WatchAction::Events(events) if events.is_empty() => Ok(()),
                    WatchAction::Events(events) => coordinator
                        .apply_watcher_batch(events)
                        .and_then(|_| reconcile_imports(&coordinator)),
                };
                if let Err(error) = result {
                    *lock(&last_error) = Some(error.to_string());
                    lock(&watcher).force_overflow();
                }
            }
        })
        .expect("failed to start distill coordinator thread")
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
