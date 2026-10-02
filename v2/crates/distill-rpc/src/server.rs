//! The RPC server over the store's current-state tables (LOCKLESS.md §3).
//!
//! A [`ServerHandle`] is the shared, `Send + Sync` identity of one served
//! store: its configuration, snapshot policy, publication signal, backends,
//! and two admission counters. It is the only state connections share, and
//! none of it is per-connection: the counters and the policy are atomics,
//! and every thread writes through its own writer of the store
//! ([`SharedStore`]), whose write lock orders them.
//!
//! Every client connection ([`Root::connect`], [`Root::metadata`]) owns a
//! private front end ([`Server`]): its own store reader, its own snapshots
//! (open read transactions), and for a target connection its subscription
//! queue and change-log cursor. Nothing of it is reachable from another
//! connection, so a connection lives on one thread and needs no locks; the
//! Cap'n Proto transport gives each one a thread of its own. A connection
//! learns of publications from the handle's watch signal and reads the
//! change log itself, from its own cursor. Admin work (commits, installs)
//! goes through a per-thread front end ([`Server::attach`]) that holds no
//! connection state.
//!
//! Embedded servers (tests, tools) own a private store in a temporary
//! directory. Daemon servers share the daemon's store: the daemon's
//! durable step and the served projection of each [`Commit`] commit as one
//! input ([`crate::apply_commit`]), and then the handle is signalled.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};
use unicode_normalization::UnicodeNormalization;

use distill_store::served::{
    Change, ChangeEntry, ServedWrite, SERVED_PIPELINE,
};
use distill_store::{SharedStore, Store, StoreConfig, StoreError, StoreReader};

use crate::apply::{
    apply_commit, apply_commit_served, configuration_status, publish_protocol_epoch, publish_restart_required,
    publish_runtime_pipeline_failure, publish_target, publish_target_set,
    read_served_pipeline, ApplyError, ApplyMode,
};
use crate::persist::{delta_state, reconnect_reason};
use crate::*;

pub(crate) const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;
const MAX_PENDING_STREAM_EVENTS: usize = 1024;
/// How long a snapshot capability served over the wire lives.
pub const DEFAULT_SNAPSHOT_TTL: Duration = Duration::from_secs(30);
const DEFAULT_MAX_SNAPSHOTS: usize = 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 256;
pub const MAX_SUBSCRIBED_ASSETS: usize = 4096;
pub const MAX_SUBSCRIBED_PATHS: usize = 4096;

/// Bounds on the capabilities clients hold. A snapshot served over the wire
/// expires `ttl` after it opens, whatever its use; clients open another.
///
/// `max_snapshots` bounds the snapshots open across all connections. A
/// connection opening one past it releases its own oldest snapshot, and is
/// refused ([`RpcFailure::ResourceLimit`]) when it holds none: a connection
/// never reaches into another's. `max_connections` bounds the connections
/// served at once; one past it is refused. Hubs and metadata hubs count
/// against it, and so, separately, do a listener's transport connections
/// (`capnp_transport`). A policy applies to what opens after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPolicy {
    pub ttl: Duration,
    pub max_snapshots: usize,
    pub max_connections: usize,
}

/// A [`SnapshotPolicy`] readable from any thread without a lock. Each bound
/// is its own atomic: a reader racing a replacement may combine old and new
/// bounds, each of them valid, and the next capability opened sees the new
/// policy whole.
struct PolicyCell {
    ttl_nanos: AtomicU64,
    max_snapshots: AtomicUsize,
    max_connections: AtomicUsize,
}

impl PolicyCell {
    fn new(policy: SnapshotPolicy) -> Self {
        let cell = Self {
            ttl_nanos: AtomicU64::new(0),
            max_snapshots: AtomicUsize::new(0),
            max_connections: AtomicUsize::new(0),
        };
        cell.set(policy);
        cell
    }

    fn get(&self) -> SnapshotPolicy {
        SnapshotPolicy {
            ttl: Duration::from_nanos(self.ttl_nanos.load(Ordering::Acquire)),
            max_snapshots: self.max_snapshots.load(Ordering::Acquire),
            max_connections: self.max_connections.load(Ordering::Acquire),
        }
    }

    fn set(&self, policy: SnapshotPolicy) {
        let ttl = u64::try_from(policy.ttl.as_nanos()).unwrap_or(u64::MAX);
        self.ttl_nanos.store(ttl, Ordering::Release);
        self.max_snapshots.store(policy.max_snapshots, Ordering::Release);
        self.max_connections.store(policy.max_connections, Ordering::Release);
    }
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_SNAPSHOT_TTL,
            max_snapshots: DEFAULT_MAX_SNAPSHOTS,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

impl SnapshotPolicy {
    fn validate(self) -> Result<Self, &'static str> {
        if self.ttl.is_zero() {
            return Err("RPC snapshot TTL must be nonzero");
        }
        if Instant::now().checked_add(self.ttl).is_none() {
            return Err("RPC snapshot TTL is too large");
        }
        if self.max_snapshots == 0 {
            return Err("RPC snapshot bound must be nonzero");
        }
        if self.max_connections == 0 {
            return Err("RPC connection bound must be nonzero");
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatedCommitError {
    Stale {
        expected: InputVersion,
        observed: InputVersion,
    },
    Publication(String),
    Invalid(AdminError),
}

// ---------------------------------------------------------------------------
// Handle

/// A publication a transport runs on a blocking thread, so its event loop
/// never waits on SQLite's write lock ([`WriteCall::run`]).
pub struct WriteCall<T> {
    handle: Arc<ServerHandle>,
    step: Box<dyn FnOnce(&Server) -> T + Send>,
}

impl<T> WriteCall<T> {
    /// Run the step on this thread's front end.
    pub fn run(self) -> T {
        let Self { handle, step } = self;
        step(&Server::attach(&handle))
    }
}

/// An input the daemon's coordinated step joins: rolled back unless
/// finished.
struct OpenInput<'a> {
    handle: &'a ServerHandle,
    open: bool,
}

impl OpenInput<'_> {
    fn finish(mut self) -> Result<(), CoordinatedCommitError> {
        self.open = false;
        self.handle
            .with_store(|store| store.finish_input(true))
            .map(|_| ())
            .map_err(|error| CoordinatedCommitError::Publication(error.to_string()))
    }
}

impl Drop for OpenInput<'_> {
    fn drop(&mut self) {
        if self.open {
            let _ = self.handle.with_store(|store| store.finish_input(false));
        }
    }
}

enum PublishError {
    Stale {
        expected: InputVersion,
        observed: InputVersion,
    },
    Invalid(AdminError),
    Store(StoreError),
}

static NEXT_HANDLE_ID: AtomicU64 = AtomicU64::new(1);

/// The shared identity of one served store. Cheap front ends attach to it
/// per thread ([`Server::attach`]).
pub struct ServerHandle {
    id: u64,
    instance: StoreInstanceId,
    config: StoreConfig,
    published: watch::Sender<u64>,
    authoring_backend: Arc<dyn AuthoringBackend>,
    build_backend: OnceLock<Arc<dyn BuildBackend>>,
    on_publish: OnceLock<Box<dyn Fn() + Send + Sync>>,
    /// The bounds connections read when they open a capability.
    policy: PolicyCell,
    /// Snapshot capabilities holding a read transaction, over every
    /// connection ([`SnapshotPolicy::max_snapshots`]).
    open_snapshots: AtomicUsize,
    /// Admitted hubs and metadata hubs ([`SnapshotPolicy::max_connections`]).
    open_connections: AtomicUsize,
    next_connection_id: AtomicU64,
    /// Every thread writes through its own writer of this store.
    store: Option<Arc<SharedStore>>,
    embedded_dir: Option<PathBuf>,
}

impl fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerHandle")
            .field("instance", &self.instance)
            .field("embedded", &self.embedded())
            .finish_non_exhaustive()
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.store.take();
        if let Some(dir) = self.embedded_dir.take() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

impl ServerHandle {
    fn new(
        instance: StoreInstanceId,
        config: StoreConfig,
        authoring_backend: Arc<dyn AuthoringBackend>,
        store: Arc<SharedStore>,
        embedded_dir: Option<PathBuf>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_HANDLE_ID.fetch_add(1, Ordering::Relaxed),
            instance,
            config,
            published: watch::Sender::new(0),
            authoring_backend,
            build_backend: OnceLock::new(),
            on_publish: OnceLock::new(),
            policy: PolicyCell::new(SnapshotPolicy::default()),
            open_snapshots: AtomicUsize::new(0),
            open_connections: AtomicUsize::new(0),
            next_connection_id: AtomicU64::new(1),
            store: Some(store),
            embedded_dir,
        })
    }

    /// Serve a daemon store. The target set must already be recorded
    /// ([`crate::publish_target_set`]).
    pub fn open(authoring_backend: Arc<dyn AuthoringBackend>, store: Arc<SharedStore>) -> Arc<Self> {
        let config = StoreConfig::clone(&store.config());
        Self::new(store.instance_id(), config, authoring_backend, store, None)
    }

    pub fn instance(&self) -> StoreInstanceId {
        self.instance
    }

    /// Run `job` on this thread's writer.
    fn with_store<T>(&self, job: impl FnOnce(&mut Store) -> T) -> T {
        let store = self.store.as_ref().expect("the store lives as long as the handle");
        job(&mut store.write())
    }

    pub(crate) fn embedded(&self) -> bool {
        self.embedded_dir.is_some()
    }

    /// Wake every front end: the store published a new version or fence.
    pub fn notify_published(&self) {
        self.published.send_modify(|count| *count = count.wrapping_add(1));
        if let Some(hook) = self.on_publish.get() {
            hook();
        }
    }

    /// Call `hook` after every publication, from the publishing thread.
    /// Install it once.
    pub fn install_publication_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        if self.on_publish.set(hook).is_err() {
            panic!("the publication hook is already installed");
        }
    }

    /// How many publications readers have been told about.
    pub fn publication_count(&self) -> u64 {
        *self.published.borrow()
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.published.subscribe()
    }

    /// The snapshot policy capabilities open under. A snapshot never
    /// outlives its TTL, so storage it reads stays for that long.
    pub fn snapshot_policy(&self) -> SnapshotPolicy {
        self.policy.get()
    }

    fn set_snapshot_policy(&self, policy: SnapshotPolicy) {
        self.policy.set(policy);
    }

    /// Snapshot capabilities holding a read transaction, over every
    /// connection.
    pub fn open_snapshots(&self) -> usize {
        self.open_snapshots.load(Ordering::Acquire)
    }

    /// Hubs and metadata hubs currently admitted.
    pub fn open_connections(&self) -> usize {
        self.open_connections.load(Ordering::Acquire)
    }

    fn connection_id(&self) -> u64 {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Count one more open snapshot unless `max` are open.
    fn claim_snapshot(self: &Arc<Self>, max: usize) -> Option<SnapshotClaim> {
        self.open_snapshots
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                (open < max).then_some(open + 1)
            })
            .ok()
            .map(|_| SnapshotClaim {
                handle: Arc::clone(self),
            })
    }

    /// Admit one more connection unless `max_connections` are open.
    fn admit_connection(self: &Arc<Self>) -> Option<Admission> {
        let max = self.snapshot_policy().max_connections;
        self.open_connections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                (open < max).then_some(open + 1)
            })
            .ok()
            .map(|_| Admission {
                handle: Arc::clone(self),
            })
    }

    pub(crate) fn authoring_backend(&self) -> Arc<dyn AuthoringBackend> {
        Arc::clone(&self.authoring_backend)
    }

    pub(crate) fn build_backend(&self) -> Arc<dyn BuildBackend> {
        self.build_backend
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(UnavailableBuildBackend))
    }

    /// Publish `commit` as the version after `base` (when given).
    ///
    /// Embedded: one input transaction checks the base and applies the whole
    /// commit. Daemon: the caller's durable step has already committed the
    /// namespace as `base + 1` (or published nothing, and this transaction
    /// becomes that version); the served projection is applied on top.
    fn publish(
        &self,
        base: Option<InputVersion>,
        commit: Commit,
        targets: Option<BTreeMap<String, TargetDefinitionHash>>,
    ) -> Result<SnapshotStamp, PublishError> {
        let full = self.embedded();
        let result = self.with_store(|store| {
            let mut rejection = None;
            let mut stale = None;
            let mut reject = |error: ApplyError| match error {
                ApplyError::Invalid(error) => {
                    let detail = format!("{error:?}");
                    rejection = Some(PublishError::Invalid(error));
                    StoreError::Rejected { detail }
                }
                ApplyError::Store(error) => error,
            };
            let observed = store.input_version();
            let base = base.unwrap_or(observed);
            let result = if full || observed == base {
                let mode = if full { ApplyMode::Full } else { ApplyMode::Delta };
                store
                    .input_transaction(|txn| {
                        let observed = txn.base_stamp().version;
                        if observed != base {
                            stale = Some(observed);
                            return Err(StoreError::Rejected {
                                detail: "stale publication base".to_owned(),
                            });
                        }
                        apply_commit(txn, &commit, mode).map_err(&mut reject)?;
                        if let Some(targets) = &targets {
                            publish_target_set(txn, targets)?;
                        }
                        Ok(())
                    })
                    .map(|((), version)| version)
            } else if observed.0 == base.0 + 1 {
                store
                    .served_transaction(|txn| {
                        apply_commit_served(txn, &commit).map_err(&mut reject)?;
                        if let Some(targets) = &targets {
                            publish_target_set(txn, targets)?;
                        }
                        Ok(txn.version())
                    })
            } else {
                return Err(PublishError::Stale {
                    expected: base,
                    observed,
                });
            };
            match result {
                Ok(version) => Ok(SnapshotStamp {
                    instance: store.instance_id(),
                    version,
                }),
                Err(error) => Err(rejection.unwrap_or(match stale {
                    Some(observed) => PublishError::Stale {
                        expected: base,
                        observed,
                    },
                    None => PublishError::Store(error),
                })),
            }
        });
        // Inside an open input, readers are told once it commits.
        if result.is_ok() && !self.with_store(|store| store.input_open()) {
            self.notify_published();
        }
        result
    }

    /// Change served state in one transaction, optionally as a new (empty)
    /// input version.
    fn write_served<T: Send + 'static>(
        &self,
        new_version: bool,
        job: impl FnOnce(&mut dyn ServedWriteObj) -> Result<T, StoreError> + Send + 'static,
    ) -> T {
        let result = self.with_store(|store| {
            if new_version {
                store
                    .input_transaction(|txn| job(txn))
                    .map(|(value, _)| value)
            } else {
                store.served_transaction(|txn| job(txn))
            }
        });
        let value = result.unwrap_or_else(|error| panic!("RPC store write failed: {error}"));
        self.notify_published();
        value
    }
}

/// [`ServedWrite`] made object safe by delegation.
trait ServedWriteObj {
    fn runtime_failure(&mut self, failure: PipelineFailure) -> Result<bool, StoreError>;
    fn restart(&mut self, keys: &[String]) -> Result<bool, StoreError>;
    fn target(
        &mut self,
        name: &str,
        hash: TargetDefinitionHash,
    ) -> Result<Option<bool>, StoreError>;
    fn protocol_epoch(&mut self, epoch: u32) -> Result<bool, StoreError>;
    fn discard_before(&mut self, oldest: InputVersion) -> Result<(), StoreError>;
}

impl<W: ServedWrite> ServedWriteObj for W {
    fn runtime_failure(&mut self, failure: PipelineFailure) -> Result<bool, StoreError> {
        publish_runtime_pipeline_failure(self, failure)
    }

    fn restart(&mut self, keys: &[String]) -> Result<bool, StoreError> {
        publish_restart_required(self, keys)
    }

    fn target(
        &mut self,
        name: &str,
        hash: TargetDefinitionHash,
    ) -> Result<Option<bool>, StoreError> {
        publish_target(self, name, hash)
    }

    fn protocol_epoch(&mut self, epoch: u32) -> Result<bool, StoreError> {
        publish_protocol_epoch(self, epoch)
    }

    fn discard_before(&mut self, oldest: InputVersion) -> Result<(), StoreError> {
        let current = self.change_version();
        self.discard_change_log_before(InputVersion(oldest.0.min(current.0)))
    }
}

struct UnavailableBuildBackend;

struct UnavailableAuthoringBackend;

impl BuildBackend for UnavailableBuildBackend {
    fn build(&self, request: &BuildRequest) -> Result<BuildBackendOutcome, RpcFailure> {
        Ok(BuildBackendOutcome::Drifted {
            input: request.drifted_input.clone(),
        })
    }
}

impl AuthoringBackend for UnavailableAuthoringBackend {
    fn prepare_import(
        &self,
        _base: InputVersion,
        _request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "import".to_owned(),
        })
    }

    fn prepare_reimport(
        &self,
        _base: InputVersion,
        _bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "reimport".to_owned(),
        })
    }

    fn prepare_operation(
        &self,
        _base: InputVersion,
        _operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "operation".to_owned(),
        })
    }
}

fn embedded_state_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    std::env::temp_dir().join(format!(
        "distill-rpc-embedded-{}-{}-{nanos}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

// ---------------------------------------------------------------------------
// Front end

thread_local! {
    /// Each thread's admin front end, by handle ([`Server::attach`]).
    static FRONT_ENDS: RefCell<HashMap<u64, Weak<Inner>>> = RefCell::new(HashMap::new());
}

/// A front end of one served store: one connection's, or a thread's admin
/// front end ([`Server::attach`]). Cheap to clone; not `Send`.
/// [`Server::root`] hands out the `Send` bootstrap for other threads.
#[derive(Clone)]
pub struct Server {
    pub(crate) inner: Rc<Inner>,
}

/// One front end's state. A connection's is reachable only from that
/// connection's capabilities, and so only from the thread serving it.
pub(crate) struct Inner {
    /// Current-state reads: fences, the change log, CAS bytes, artifacts.
    pub(crate) reader: StoreReader,
    /// This front end's snapshots of one version share a read transaction.
    current_txn: RefCell<Weak<SnapshotTxn>>,
    /// This front end's open snapshots, oldest first.
    snapshots: RefCell<VecDeque<Weak<SnapshotHold>>>,
    pub(crate) handle: Arc<ServerHandle>,
    /// A connection's place under `max_connections`; none for an admin
    /// front end.
    _admission: Option<Admission>,
}

/// One admitted connection, released when its front end drops.
struct Admission {
    handle: Arc<ServerHandle>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.handle.open_connections.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One open snapshot counted against `max_snapshots`, released with the
/// snapshot's read transaction.
pub(crate) struct SnapshotClaim {
    handle: Arc<ServerHandle>,
}

impl Drop for SnapshotClaim {
    fn drop(&mut self) {
        self.handle.open_snapshots.fetch_sub(1, Ordering::AcqRel);
    }
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server")
            .field("instance", &self.inner.handle.instance)
            .finish_non_exhaustive()
    }
}

/// The `Send` bootstrap of a served store: the entry point on any thread.
#[derive(Clone)]
pub struct Root {
    pub(crate) handle: Arc<ServerHandle>,
}

impl fmt::Debug for Root {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Root").finish_non_exhaustive()
    }
}

impl Server {
    pub fn new(
        instance: StoreInstanceId,
        targets: Vec<TargetDefinition>,
    ) -> Result<Self, TargetSetError> {
        Self::new_with_authoring_backend(instance, targets, Arc::new(UnavailableAuthoringBackend))
    }

    pub fn new_with_authoring_backend(
        instance: StoreInstanceId,
        targets: Vec<TargetDefinition>,
        authoring_backend: Arc<dyn AuthoringBackend>,
    ) -> Result<Self, TargetSetError> {
        Self::new_at_version_with_authoring_backend(
            instance,
            InputVersion(0),
            targets,
            authoring_backend,
        )
    }

    /// An embedded server over a fresh private store whose identity is
    /// `instance` at `version`.
    pub fn new_at_version_with_authoring_backend(
        instance: StoreInstanceId,
        version: InputVersion,
        targets: Vec<TargetDefinition>,
        authoring_backend: Arc<dyn AuthoringBackend>,
    ) -> Result<Self, TargetSetError> {
        let target_map = target_map(targets)?;
        let dir = embedded_state_dir();
        let config = StoreConfig::new(dir.join(".distill"));
        let mut store = Store::open(config.clone()).expect("embedded RPC store opens");
        store
            .adopt_embedded_identity(instance, version)
            .expect("embedded RPC store is fresh");
        store
            .served_transaction(|txn| publish_target_set(txn, &target_map))
            .expect("embedded RPC store records its targets");
        let handle = ServerHandle::new(
            instance,
            config,
            authoring_backend,
            Arc::new(SharedStore::new(store)),
            Some(dir),
        );
        Ok(Self::attach(&handle))
    }

    /// This thread's admin front end of `handle`: commits, installs, and
    /// current-state reads. It holds no connection state; every connection
    /// gets a front end of its own ([`Root::connect`]).
    pub fn attach(handle: &Arc<ServerHandle>) -> Self {
        FRONT_ENDS.with(|front_ends| {
            let mut front_ends = front_ends.borrow_mut();
            if let Some(inner) = front_ends.get(&handle.id).and_then(Weak::upgrade) {
                return Self { inner };
            }
            front_ends.retain(|_, inner| inner.strong_count() > 0);
            let inner = Rc::new(Inner::open(handle, None));
            front_ends.insert(handle.id, Rc::downgrade(&inner));
            Self { inner }
        })
    }

    /// A front end of its own for one admitted connection.
    fn connection(handle: &Arc<ServerHandle>, admission: Admission) -> Self {
        Self {
            inner: Rc::new(Inner::open(handle, Some(admission))),
        }
    }

    pub fn handle(&self) -> Arc<ServerHandle> {
        Arc::clone(&self.inner.handle)
    }

    /// Replace the snapshot policy. It applies to snapshots and connections
    /// opened afterwards; what is open keeps its TTL and is not released.
    pub fn install_snapshot_policy(&self, policy: SnapshotPolicy) -> Result<(), &'static str> {
        self.inner.handle.set_snapshot_policy(policy.validate()?);
        Ok(())
    }

    /// How many snapshots are open, over every connection.
    pub fn open_snapshots(&self) -> usize {
        self.inner.handle.open_snapshots()
    }

    /// Install the lazy-build implementation. Install backends once, before
    /// accepting clients.
    pub fn install_build_backend(&self, backend: Arc<dyn BuildBackend>) {
        if self.inner.handle.build_backend.set(backend).is_err() {
            panic!("the RPC build backend is already installed");
        }
    }

    pub fn root(&self) -> Root {
        Root {
            handle: Arc::clone(&self.inner.handle),
        }
    }

    pub fn instance(&self) -> StoreInstanceId {
        self.inner.handle.instance
    }

    pub fn current_stamp(&self) -> SnapshotStamp {
        self.inner.current_stamp()
    }

    /// Snapshot-pinned batch requests used by `doctor verify`. This bypasses
    /// transport capabilities but not configuration or pipeline errors;
    /// the daemon still executes each request through the ordinary build core.
    pub fn verification_build_requests(&self) -> Result<Vec<BuildRequest>, RpcFailure> {
        let txn = self.inner.current_snapshot().map_err(store_failure)?;
        if let ConfigurationStatus::Failed(error) = &txn.configuration {
            return Err(RpcFailure::InvalidQuery {
                detail: format!("cannot verify failed configuration: {error:?}"),
            });
        }
        if let Some(error) = pipeline_failure(&self.inner.effective_pipeline(&txn)) {
            return Err(error);
        }
        let snapshot = txn.snapshot();
        let targets = snapshot.rpc_targets().map_err(store_failure)?;
        let mut entries = Vec::new();
        for meta in snapshot.served_entries().map_err(store_failure)? {
            if meta.authoring_only {
                continue;
            }
            let entry = snapshot
                .served_entry(meta.asset)
                .map_err(store_failure)?
                .ok_or(RpcFailure::AssetNotFound { uuid: meta.asset })?;
            entries.push(authoring_entry(entry).map_err(store_failure)?);
        }
        let mut requests = Vec::new();
        for target in &targets {
            for entry in &entries {
                requests.push(BuildRequest {
                    work_class: BuildWorkClass::Batch,
                    basis: txn.stamp,
                    target: target.name.clone(),
                    target_definition: TargetDefinitionHash(target.definition_hash),
                    requested_asset: entry.uuid,
                    output_key: String::new(),
                    requested_terminal_type: entry.terminal_type,
                    entry: entry.clone(),
                    drifted_input: DriftedInput::Asset(entry.uuid),
                });
            }
        }
        Ok(requests)
    }

    /// Publish a runtime failure of the current pipeline epoch. Every
    /// snapshot that pinned the epoch sees it and every connection must
    /// reconnect. `persist` records the daemon's own durable failure first.
    pub fn coordinated_runtime_pipeline_failure(
        &self,
        failure: PipelineFailure,
        persist: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        self.runtime_pipeline_failure_locked(failure, persist)
    }

    fn runtime_pipeline_failure_locked(
        &self,
        failure: PipelineFailure,
        persist: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        failure
            .validate()
            .map_err(|error| format!("invalid runtime pipeline failure: {error:?}"))?;
        if failure.origin != PipelineFailureOrigin::PublishedRuntime {
            return Err("runtime failure publication requires PublishedRuntime origin".to_owned());
        }
        let (_, current) = self
            .inner
            .current_pipeline()
            .map_err(|error| error.to_string())?;
        match &current {
            PipelineDiagnostic::Ready => {}
            PipelineDiagnostic::Failed(existing) if existing == &failure => return Ok(()),
            other => {
                return Err(format!(
                    "current RPC pipeline is not the observed ready epoch: {other:?}"
                ))
            }
        }
        persist()?;
        self.inner
            .handle
            .write_served(false, move |txn| txn.runtime_failure(failure));
        Ok(())
    }

    /// Advance the protocol epoch and fence every existing connection.
    pub fn replace_protocol_epoch(&self, protocol_epoch: u32) -> SnapshotStamp {
        self.inner
            .handle
            .write_served(true, move |txn| txn.protocol_epoch(protocol_epoch));
        self.current_stamp()
    }

    /// Validate and publish one artifact with its typed direct load edges.
    pub fn install_artifact(
        &self,
        hash: ContentHash,
        payload: ArtifactPayload,
    ) -> Result<(), AdminError> {
        let blob_parts = payload
            .blobs
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>();
        let parsed = distill_wire::artifact::parse_artifact_parts(&payload.structural, &blob_parts)
            .map_err(|error| AdminError::InvalidArtifact {
                detail: format!("invalid canonical DSTL artifact: {error}"),
            })?;
        if parsed.content_hash != hash {
            return Err(AdminError::InvalidArtifact {
                detail: format!(
                    "artifact ContentHash mismatch: expected {hash:?}, observed {:?}",
                    parsed.content_hash
                ),
            });
        }
        if payload.load_edges.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(AdminError::InvalidArtifact {
                detail: "direct typed load edges must be strictly sorted and unique".to_owned(),
            });
        }
        let edge_assets = payload
            .load_edges
            .iter()
            .map(|edge| edge.asset)
            .collect::<Vec<_>>();
        if edge_assets != parsed.load_deps {
            return Err(AdminError::InvalidArtifact {
                detail: "direct typed load edges do not match authenticated DSTL load_deps"
                    .to_owned(),
            });
        }
        let asset = parsed.asset_uuid;
        let edges = payload
            .load_edges
            .iter()
            .map(|edge| (edge.asset, edge.expected_terminal))
            .collect::<Vec<_>>();
        if self.inner.reader.cas_contains(&hash.0).unwrap_or(false) {
            let existing = self
                .inner
                .reader
                .artifact_load_edges(hash)
                .map_err(|error| AdminError::InvalidArtifact {
                    detail: format!("cannot read recorded load edges: {error}"),
                })?;
            if existing == edges && (!edges.is_empty() || !existing.is_empty()) {
                return Ok(());
            }
            if !existing.is_empty() {
                return Err(AdminError::ArtifactAlreadyExistsWithDifferentPayload { hash });
            }
        }
        let bytes = distill_wire::artifact::assemble_artifact(&payload.structural, &blob_parts);
        let stored = self
            .inner
            .handle
            .with_store(|store| store.put_artifact(asset, &bytes, &edges));
        match stored {
            Ok(stored) if stored == hash => Ok(()),
            Ok(stored) => Err(AdminError::InvalidArtifact {
                detail: format!("stored artifact hash {stored:?} differs from {hash:?}"),
            }),
            Err(StoreError::InvalidConfiguration { .. }) => {
                Err(AdminError::ArtifactAlreadyExistsWithDifferentPayload { hash })
            }
            Err(error) => Err(AdminError::InvalidArtifact {
                detail: format!("the store rejected the artifact: {error}"),
            }),
        }
    }

    /// Validate and publish one canonical DSWL body.
    pub fn install_wire_tree(&self, hash: LayoutHash, bytes: Arc<[u8]>) -> Result<(), AdminError> {
        let root = distill_wire::dswl::decode_dswl(&bytes).map_err(|error| {
            AdminError::InvalidWireTree {
                detail: format!("invalid canonical DSWL body: {error:?}"),
            }
        })?;
        let observed =
            distill_wire::dswl::dswl_hash(&root).map_err(|error| AdminError::InvalidWireTree {
                detail: format!("cannot authenticate DSWL body: {error:?}"),
            })?;
        if observed != hash {
            return Err(AdminError::WireTreeHashMismatch {
                expected: hash,
                observed,
            });
        }
        if self.inner.reader.wire_tree_read(hash).is_ok() {
            return Ok(());
        }
        let body = bytes.to_vec();
        let stored = self
            .inner
            .handle
            .with_store(|store| store.put_wire_tree(&body))
            .map_err(|error| AdminError::InvalidWireTree {
                detail: format!("the store rejected the wire tree: {error}"),
            })?;
        if stored != hash {
            return Err(AdminError::WireTreeHashMismatch {
                expected: hash,
                observed: stored,
            });
        }
        Ok(())
    }

    /// Publish a build's artifacts and wire trees; return its root hash.
    pub(crate) fn install_build_publication(
        &self,
        asset: AssetUuid,
        publication: BuildPublication,
    ) -> Result<ContentHash, RpcFailure> {
        let root_hash = publication.root_content_hash;
        let roots = publication
            .artifacts
            .iter()
            .filter(|artifact| artifact.content_hash == root_hash)
            .collect::<Vec<_>>();
        if roots.len() != 1 {
            return Err(RpcFailure::InvalidQuery {
                detail: "lazy-build publication must contain its root artifact exactly once"
                    .to_owned(),
            });
        }
        let root = roots[0];
        let root_blobs = root
            .payload
            .blobs
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>();
        let parsed =
            distill_wire::artifact::parse_artifact_parts(&root.payload.structural, &root_blobs)
                .map_err(|error| RpcFailure::InvalidQuery {
                    detail: format!("lazy-build root is not canonical DSTL: {error}"),
                })?;
        if parsed.asset_uuid != asset {
            return Err(RpcFailure::InvalidQuery {
                detail: "lazy-build root asset does not match the requested asset".to_owned(),
            });
        }
        drop(parsed);

        let mut wire_hashes = BTreeSet::new();
        for tree in publication.wire_trees {
            if !wire_hashes.insert(tree.layout_hash) {
                return Err(RpcFailure::InvalidQuery {
                    detail: "lazy-build publication contains a duplicate wire tree".to_owned(),
                });
            }
            self.install_wire_tree(tree.layout_hash, tree.bytes)
                .map_err(|error| RpcFailure::InvalidQuery {
                    detail: format!("lazy-build wire-tree publication rejected: {error:?}"),
                })?;
        }
        let mut artifact_hashes = BTreeSet::new();
        for artifact in publication.artifacts {
            if !artifact_hashes.insert(artifact.content_hash) {
                return Err(RpcFailure::InvalidQuery {
                    detail: "lazy-build publication contains a duplicate artifact".to_owned(),
                });
            }
            self.install_artifact(artifact.content_hash, artifact.payload)
                .map_err(|error| RpcFailure::InvalidQuery {
                    detail: format!("lazy-build artifact publication rejected: {error:?}"),
                })?;
        }
        Ok(root_hash)
    }

    /// Embedded: publish an input-version commit.
    pub fn commit(&self, commit: Commit) -> Result<SnapshotStamp, AdminError> {
        assert!(
            self.inner.handle.embedded(),
            "the daemon publishes through coordinated commits"
        );
        match self.inner.handle.publish(None, commit, None) {
            Ok(stamp) => Ok(stamp),
            Err(PublishError::Invalid(error)) => Err(error),
            Err(PublishError::Stale { .. }) => unreachable!("an unconditioned commit is never stale"),
            Err(PublishError::Store(error)) => panic!("embedded RPC store write failed: {error}"),
        }
    }

    /// Publish one coordinator step against `base`. `publish` runs the
    /// daemon's durable step (if any) and returns the RPC delta, which is
    /// then applied as the version after `base`. No other publication
    /// interleaves.
    pub fn coordinated_commit(
        &self,
        base: InputVersion,
        publish: impl FnOnce() -> Result<Commit, String>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        self.coordinated_maybe_commit(base, || publish().map(Some))
            .map(|stamp| stamp.expect("a coordinated commit always publishes"))
    }

    /// `step` as a job a transport runs on its blocking pool, never on its
    /// event loop.
    pub(crate) fn write_call<T: Send + 'static>(
        &self,
        step: impl FnOnce(&Server) -> T + Send + 'static,
    ) -> WriteCall<T> {
        WriteCall {
            handle: Arc::clone(&self.inner.handle),
            step: Box::new(step),
        }
    }

    /// A coordinated publication that may terminate in durable memo state
    /// only; `None` leaves the input version untouched.
    pub fn coordinated_maybe_commit(
        &self,
        base: InputVersion,
        publish: impl FnOnce() -> Result<Option<Commit>, String>,
    ) -> Result<Option<SnapshotStamp>, CoordinatedCommitError> {
        self.coordinated_locked(base, publish, None)
    }

    fn coordinated_locked(
        &self,
        base: InputVersion,
        publish: impl FnOnce() -> Result<Option<Commit>, String>,
        targets: Option<BTreeMap<String, TargetDefinitionHash>>,
    ) -> Result<Option<SnapshotStamp>, CoordinatedCommitError> {
        let handle = &self.inner.handle;
        // The step, its own writes and the served projection commit as one
        // input, begun here: the base is checked inside it, and no reader
        // sees the version before its rows.
        let observed = handle
            .with_store(|store| store.open_input())
            .map_err(|error| CoordinatedCommitError::Publication(error.to_string()))?;
        let open = OpenInput { handle, open: true };
        if observed != base {
            return Err(CoordinatedCommitError::Stale {
                expected: base,
                observed,
            });
        }
        let published = match publish().map_err(CoordinatedCommitError::Publication)? {
            Some(commit) => Some(self.publish_locked(base, commit, targets)?),
            None => None,
        };
        open.finish()?;
        handle.notify_published();
        Ok(published)
    }

    /// Apply `commit` as the version after `base`, inside the caller's
    /// input.
    pub(crate) fn publish_locked(
        &self,
        base: InputVersion,
        commit: Commit,
        targets: Option<BTreeMap<String, TargetDefinitionHash>>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        self.inner
            .handle
            .publish(Some(base), commit, targets)
            .map_err(|error| match error {
                PublishError::Stale { expected, observed } => {
                    CoordinatedCommitError::Stale { expected, observed }
                }
                PublishError::Invalid(error) => CoordinatedCommitError::Invalid(error),
                PublishError::Store(error) => CoordinatedCommitError::Publication(error.to_string()),
            })
    }

    /// Replace the complete target set in the same transaction as the
    /// coordinator commit's projection.
    pub fn coordinated_replace_target_set(
        &self,
        base: InputVersion,
        replacements: Vec<TargetDefinition>,
        publish: impl FnOnce() -> Result<Commit, String>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        let targets = target_map(replacements)
            .map_err(|error| CoordinatedCommitError::Publication(error.to_string()))?;
        self.coordinated_locked(base, || publish().map(Some), Some(targets))
        .map(|stamp| stamp.expect("a coordinated commit always publishes"))
    }

    /// Discard cursor history strictly before `oldest_available`.
    pub fn discard_history_before(&self, oldest_available: InputVersion) {
        self.inner
            .handle
            .write_served(false, move |txn| txn.discard_before(oldest_available));
    }

    /// Replace a staged target definition and fence every bound Hub.
    pub fn replace_target(
        &self,
        replacement: TargetDefinition,
    ) -> Result<SnapshotStamp, AdminError> {
        let name = replacement.name().to_owned();
        let hash = replacement.definition_hash();
        let known = self.inner.reader.rpc_target(&name).map_err(|_| AdminError::UnknownTarget {
            target: name.clone(),
        })?;
        match known {
            None => return Err(AdminError::UnknownTarget { target: name }),
            Some(row) if row.definition_hash == hash.0 => return Ok(self.current_stamp()),
            Some(_) => {}
        }
        let changed = self
            .inner
            .handle
            .write_served(true, move |txn| txn.target(&name, hash));
        debug_assert_eq!(changed, Some(true));
        Ok(self.current_stamp())
    }

    /// Stage a valid restart-only edit. This does not advance the input
    /// version or mutate active configuration values.
    pub fn restart_required(&self, keys: Vec<String>) -> SnapshotStamp {
        self.inner
            .handle
            .write_served(false, move |txn| txn.restart(&keys));
        self.current_stamp()
    }
}

/// The name-to-definition map of a target set, rejecting duplicate names.
pub fn target_map(
    targets: Vec<TargetDefinition>,
) -> Result<BTreeMap<String, TargetDefinitionHash>, TargetSetError> {
    let mut map = BTreeMap::new();
    for target in targets {
        let name = target.name().to_owned();
        if map.insert(name.clone(), target.definition_hash()).is_some() {
            return Err(TargetSetError::DuplicateTarget { target: name });
        }
    }
    Ok(map)
}

pub(crate) fn store_failure(error: StoreError) -> RpcFailure {
    RpcFailure::InvalidQuery {
        detail: format!("daemon state read failed: {error}"),
    }
}

/// Decode a served entry into the RPC authoring carrier.
pub(crate) fn authoring_entry(
    entry: distill_store::served::ServedEntry,
) -> Result<AuthoringEntry, StoreError> {
    let (canonical, blobs) = distill_store::served::decode_authored_value(&entry.authored_value)?;
    let meta = entry.meta;
    Ok(AuthoringEntry {
        uuid: meta.asset,
        bundle: meta.bundle,
        local_id: meta.local_id,
        normalized_path: meta.normalized_path,
        type_uuid: meta.type_uuid,
        terminal_type: meta.terminal_type,
        schema_hash: meta.logical_hash,
        logical_schema: Arc::from(entry.schema_json.into_bytes()),
        role: entry_role(meta.authoring_only),
        tags: meta.tags,
        value: AuthoringValue {
            canonical_value: Arc::from(canonical),
            blobs: blobs.into_iter().map(Arc::from).collect(),
        },
    })
}

pub(crate) fn entry_role(authoring_only: bool) -> AuthoringEntryRole {
    if authoring_only {
        AuthoringEntryRole::AuthoringOnly
    } else {
        AuthoringEntryRole::Runtime
    }
}

pub(crate) fn pipeline_failure(diagnostic: &PipelineDiagnostic) -> Option<RpcFailure> {
    match diagnostic.clone() {
        PipelineDiagnostic::Ready => None,
        PipelineDiagnostic::Failed(failure) => Some(RpcFailure::PipelineUnavailable(Box::new(
            PipelineUnavailableDiagnostic::PipelineFailure(failure),
        ))),
    }
}

// ---------------------------------------------------------------------------
// Snapshots

/// One read transaction pinning one input version, on a connection of its
/// own, shared by every snapshot of that version on one front end. The
/// immutable facts are read once.
pub(crate) struct SnapshotTxn {
    snapshot: distill_store::served::StoreSnapshot,
    pub(crate) stamp: SnapshotStamp,
    pub(crate) configuration: ConfigurationStatus,
    pipeline_installed_at: InputVersion,
    pipeline: PipelineDiagnostic,
}

impl SnapshotTxn {
    pub(crate) fn snapshot(&self) -> &StoreReader {
        &self.snapshot
    }
}

#[derive(Debug, Clone)]
pub(crate) enum BuildResolution {
    Built(ContentHash),
    Failed(String),
    Drifted(DriftedInput),
}

/// A held read transaction and its place under `max_snapshots`.
struct HeldTxn {
    txn: Rc<SnapshotTxn>,
    _claim: SnapshotClaim,
}

/// A snapshot capability's read transaction, until it is released: on drop,
/// on eviction past the snapshot bound, or when its expiry timer fires.
type SnapshotSlot = Rc<RefCell<Option<HeldTxn>>>;

/// What one snapshot capability (and its clones) holds.
pub(crate) struct SnapshotHold {
    slot: SnapshotSlot,
    ttl: Duration,
}

impl SnapshotHold {
    pub(crate) fn alive(&self) -> bool {
        self.slot.borrow().is_some()
    }

    pub(crate) fn txn(&self) -> Option<Rc<SnapshotTxn>> {
        self.slot.borrow().as_ref().map(|held| Rc::clone(&held.txn))
    }

    pub(crate) fn expire(&self) {
        let held = self.slot.borrow_mut().take();
        drop(held);
    }

    /// Release the snapshot once its TTL passes. Call on the `LocalSet` of
    /// the thread serving the connection: the timer runs there.
    pub(crate) fn expire_later(&self) {
        let slot = Rc::clone(&self.slot);
        let ttl = self.ttl;
        tokio::task::spawn_local(async move {
            tokio::time::sleep(ttl).await;
            let held = slot.borrow_mut().take();
            drop(held);
        });
    }
}

impl Drop for SnapshotHold {
    fn drop(&mut self) {
        self.expire();
    }
}

// ---------------------------------------------------------------------------
// Connections

/// A target connection's own state: its fences, subscriptions, and delta
/// queue. Only its connection's capabilities reach it.
pub(crate) struct ConnectionState {
    pub(crate) id: u64,
    pub(crate) target: String,
    target_generation: u64,
    protocol_epoch: u32,
    pipeline_generation: u64,
    /// The change-log cursor: rows up to here are delivered or predate the
    /// connection.
    seen_seq: i64,
    /// A reconnect event was delivered; no further deltas are.
    fenced: bool,
    pub(crate) subscribed_assets: BTreeSet<AssetUuid>,
    pub(crate) subscribed_paths: BTreeSet<String>,
    /// Undelivered stream events, at most `MAX_PENDING_STREAM_EVENTS`: past
    /// that the queue collapses into one `ResyncRequired` (a reconnect
    /// prompt survives on its own).
    pub(crate) queue: VecDeque<StreamEvent>,
    pub(crate) stream_installed: bool,
    pub(crate) notify: Rc<Notify>,
}

impl ConnectionState {
    pub(crate) fn enqueue(&mut self, event: StreamEvent) {
        if self.queue.len() >= MAX_PENDING_STREAM_EVENTS {
            let basis = event.basis().clone();
            let terminal = matches!(
                event,
                StreamEvent::Asset {
                    event: AssetEvent::ReconnectRequired { .. },
                    ..
                } | StreamEvent::ResyncRequired { .. }
            );
            self.queue.clear();
            if !terminal {
                self.queue.push_back(StreamEvent::ResyncRequired {
                    oldest_available: basis.snapshot.version,
                    basis,
                });
                self.notify.notify_one();
                return;
            }
        }
        self.queue.push_back(event);
        self.notify.notify_one();
    }
}

/// One published version's deltas, as the change log records them.
#[derive(Clone)]
pub(crate) struct HistoryDelta {
    pub(crate) stamp: SnapshotStamp,
    pub(crate) assets: Vec<(AssetUuid, AssetDeltaState)>,
    pub(crate) paths: Vec<String>,
}

impl HistoryDelta {
    pub(crate) fn filtered(
        &self,
        assets: &BTreeSet<AssetUuid>,
        paths: &BTreeSet<String>,
    ) -> Option<Delta> {
        let assets = self
            .assets
            .iter()
            .filter(|(uuid, _)| assets.contains(uuid))
            .copied()
            .collect::<Vec<_>>();
        let paths = self
            .paths
            .iter()
            .filter(|path| paths.contains(*path))
            .cloned()
            .collect::<Vec<_>>();
        if assets.is_empty() && paths.is_empty() {
            return None;
        }
        Some(Delta {
            basis: RpcBasis {
                snapshot: self.stamp,
            },
            assets,
            paths,
        })
    }
}

/// Group consecutive asset and path rows into per-version deltas.
pub(crate) fn history_deltas(instance: StoreInstanceId, rows: &[ChangeEntry]) -> Vec<HistoryDelta> {
    let mut deltas: Vec<HistoryDelta> = Vec::new();
    for row in rows {
        let stamp = SnapshotStamp {
            instance,
            version: row.version,
        };
        if deltas.last().is_none_or(|delta| delta.stamp != stamp) {
            deltas.push(HistoryDelta {
                stamp,
                assets: Vec::new(),
                paths: Vec::new(),
            });
        }
        let delta = deltas.last_mut().expect("a delta was just pushed");
        match &row.change {
            Change::Asset { asset, state } => match delta_state(*state) {
                Ok(state) => delta.assets.push((*asset, state)),
                Err(error) => tracing::error!(%error, "skipping a corrupt change-log row"),
            },
            Change::Path { path } => delta.paths.push(path.clone()),
            _ => {}
        }
    }
    deltas
}

/// Read several facts from one committed version, on a reader of its own.
fn read_consistent<T>(
    config: &StoreConfig,
    read: impl FnOnce(&StoreReader) -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    let snapshot = StoreReader::open(config.clone())?.begin_snapshot()?;
    read(&snapshot)
}

impl Inner {
    fn open(handle: &Arc<ServerHandle>, admission: Option<Admission>) -> Self {
        let reader = StoreReader::open(handle.config.clone())
            .unwrap_or_else(|error| panic!("cannot read the served store: {error}"));
        Self {
            reader,
            current_txn: RefCell::new(Weak::new()),
            snapshots: RefCell::new(VecDeque::new()),
            handle: Arc::clone(handle),
            _admission: admission,
        }
    }

    pub(crate) fn current_stamp(&self) -> SnapshotStamp {
        SnapshotStamp {
            instance: self.handle.instance,
            version: self.reader.input_version(),
        }
    }

    /// Read several facts from one committed version.
    pub(crate) fn read_consistent<T>(
        &self,
        read: impl FnOnce(&StoreReader) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        read_consistent(&self.handle.config, read)
    }

    /// The read transaction pinning the current version. Snapshots of one
    /// version on this front end share it; each new version opens a read
    /// transaction on a connection of its own.
    pub(crate) fn current_snapshot(&self) -> Result<Rc<SnapshotTxn>, StoreError> {
        let current = self.reader.input_version();
        if let Some(txn) = self.current_txn.borrow().upgrade() {
            if txn.stamp.version == current {
                return Ok(txn);
            }
        }
        let snapshot = StoreReader::open(self.handle.config.clone())?.begin_snapshot()?;
        let configuration = configuration_status(&snapshot.configuration_state()?);
        let (pipeline_installed_at, pipeline) =
            read_served_pipeline(snapshot.served_blob(SERVED_PIPELINE)?)?;
        let txn = Rc::new(SnapshotTxn {
            stamp: snapshot.stamp(),
            snapshot,
            configuration,
            pipeline_installed_at,
            pipeline,
        });
        *self.current_txn.borrow_mut() = Rc::downgrade(&txn);
        Ok(txn)
    }

    /// The current served pipeline and its installing version.
    pub(crate) fn current_pipeline(&self) -> Result<(InputVersion, PipelineDiagnostic), StoreError> {
        read_served_pipeline(self.reader.served_blob(SERVED_PIPELINE)?)
    }

    /// A snapshot's pipeline: the current diagnostic while the epoch it
    /// pinned is still installed (a runtime failure reaches it), else its own.
    pub(crate) fn effective_pipeline(&self, txn: &SnapshotTxn) -> PipelineDiagnostic {
        match self.current_pipeline() {
            Ok((installed_at, current)) if installed_at == txn.pipeline_installed_at => current,
            Ok(_) => txn.pipeline.clone(),
            Err(error) => {
                tracing::error!(%error, "cannot read the served pipeline");
                txn.pipeline.clone()
            }
        }
    }

    pub(crate) fn protocol_epoch(&self) -> u32 {
        match self.reader.rpc_fences() {
            Ok(fences) => fences.protocol_epoch.unwrap_or(PROTOCOL_VERSION),
            Err(error) => {
                tracing::error!(%error, "cannot read the RPC protocol epoch");
                PROTOCOL_VERSION
            }
        }
    }

    /// Hold one snapshot of the current version under the snapshot bound:
    /// past it this front end releases its own oldest snapshots, and is
    /// refused when it has none left to release.
    pub(crate) fn register_snapshot(
        &self,
        txn: Rc<SnapshotTxn>,
    ) -> Result<Rc<SnapshotHold>, RpcFailure> {
        let policy = self.handle.snapshot_policy();
        self.release_snapshots_beyond(policy.max_snapshots - 1);
        let claim = loop {
            if let Some(claim) = self.handle.claim_snapshot(policy.max_snapshots) {
                break claim;
            }
            if !self.release_oldest_snapshot() {
                return Err(RpcFailure::ResourceLimit {
                    resource: "open snapshots".to_owned(),
                    limit: policy.max_snapshots,
                });
            }
        };
        let hold = Rc::new(SnapshotHold {
            slot: Rc::new(RefCell::new(Some(HeldTxn { txn, _claim: claim }))),
            ttl: policy.ttl,
        });
        self.snapshots.borrow_mut().push_back(Rc::downgrade(&hold));
        Ok(hold)
    }

    /// This front end's open snapshots, oldest first; released ones are
    /// forgotten.
    fn live_snapshots(&self) -> std::cell::RefMut<'_, VecDeque<Weak<SnapshotHold>>> {
        let mut snapshots = self.snapshots.borrow_mut();
        snapshots.retain(|hold| hold.upgrade().is_some_and(|hold| hold.alive()));
        snapshots
    }

    fn release_oldest_snapshot(&self) -> bool {
        let oldest = self.live_snapshots().pop_front();
        match oldest.and_then(|hold| hold.upgrade()) {
            Some(hold) => {
                hold.expire();
                true
            }
            None => false,
        }
    }

    fn release_snapshots_beyond(&self, keep: usize) {
        while self.live_snapshots().len() > keep {
            self.release_oldest_snapshot();
        }
    }

    /// Open one target connection with its fences read at one version;
    /// change-log rows after `head` are its to deliver.
    pub(crate) fn open_connection(
        &self,
        target: String,
        target_generation: u64,
        fences: &distill_store::served::RpcFences,
        head: i64,
    ) -> Rc<RefCell<ConnectionState>> {
        Rc::new(RefCell::new(ConnectionState {
            id: self.handle.connection_id(),
            target,
            target_generation,
            protocol_epoch: fences.protocol_epoch.unwrap_or(PROTOCOL_VERSION),
            pipeline_generation: fences.pipeline_generation,
            seen_seq: head,
            fenced: false,
            subscribed_assets: BTreeSet::new(),
            subscribed_paths: BTreeSet::new(),
            queue: VecDeque::new(),
            stream_installed: false,
            notify: Rc::new(Notify::new()),
        }))
    }

    /// Why `connection` must reconnect, if it must.
    pub(crate) fn generation_fence(&self, connection: &ConnectionState) -> Option<ReconnectReason> {
        let fences = match self.reader.rpc_fences() {
            Ok(fences) => fences,
            Err(error) => {
                tracing::error!(%error, "cannot read RPC fences");
                return Some(ReconnectReason::StoreInstanceChanged);
            }
        };
        if fences.protocol_epoch.unwrap_or(PROTOCOL_VERSION) != connection.protocol_epoch {
            return Some(ReconnectReason::ProtocolEpochChanged);
        }
        if fences.pipeline_generation != connection.pipeline_generation {
            return Some(ReconnectReason::PipelineEpochChanged);
        }
        match self.reader.rpc_target(&connection.target) {
            Ok(Some(row)) if row.generation == connection.target_generation => None,
            Ok(_) => Some(ReconnectReason::TargetDefinitionChanged),
            Err(error) => {
                tracing::error!(%error, "cannot read the RPC target");
                Some(ReconnectReason::StoreInstanceChanged)
            }
        }
    }

    /// Deliver to `connection` every change-log row published since its
    /// cursor.
    pub(crate) fn pump(&self, connection: &RefCell<ConnectionState>) {
        self.pump_until(connection, None);
    }

    /// Deliver to `connection` the change-log rows after its cursor, up to
    /// `limit` (all when `None`), and advance the cursor past them. Each
    /// connection reads the log itself: a publication costs the publisher
    /// one watch signal, whatever the number of connections.
    pub(crate) fn pump_until(&self, connection: &RefCell<ConnectionState>, limit: Option<i64>) {
        let cursor = connection.borrow().seen_seq;
        if limit.is_some_and(|limit| limit <= cursor) {
            return;
        }
        let mut rows = match self.reader.change_log_after(cursor) {
            Ok(rows) => rows,
            Err(error) => {
                tracing::error!(%error, "cannot read the change log");
                return;
            }
        };
        if let Some(limit) = limit {
            rows.retain(|row| row.seq <= limit);
        }
        let Some(last) = rows.last().map(|row| row.seq) else {
            return;
        };
        let mut connection = connection.borrow_mut();
        let connection = &mut *connection;
        connection.seen_seq = last;
        let instance = self.handle.instance;
        let mut index = 0;
        while index < rows.len() {
            let row = &rows[index];
            let stamp = SnapshotStamp {
                instance,
                version: row.version,
            };
            match &row.change {
                Change::Asset { .. } | Change::Path { .. } => {
                    let start = index;
                    while index < rows.len()
                        && rows[index].version == row.version
                        && matches!(rows[index].change, Change::Asset { .. } | Change::Path { .. })
                    {
                        index += 1;
                    }
                    if connection.fenced {
                        continue;
                    }
                    for delta in history_deltas(instance, &rows[start..index]) {
                        if let Some(delta) =
                            delta.filtered(&connection.subscribed_assets, &connection.subscribed_paths)
                        {
                            connection.enqueue(StreamEvent::Delta(delta));
                        }
                    }
                    continue;
                }
                Change::ReconnectAll { reason } | Change::ReconnectTarget { reason, .. } => {
                    let target = match &row.change {
                        Change::ReconnectTarget { target, .. } => Some(target),
                        _ => None,
                    };
                    match reconnect_reason(*reason) {
                        Ok(_) if target.is_some_and(|target| &connection.target != target) => {}
                        Ok(reason) => {
                            connection.fenced = true;
                            connection.enqueue(StreamEvent::Asset {
                                basis: RpcBasis { snapshot: stamp },
                                event: AssetEvent::ReconnectRequired { reason },
                            });
                        }
                        Err(error) => tracing::error!(%error, "skipping a corrupt change-log row"),
                    }
                }
                Change::RestartRequired { keys } => {
                    if !keys.is_empty() && connection.stream_installed {
                        connection.enqueue(StreamEvent::Asset {
                            basis: RpcBasis { snapshot: stamp },
                            event: AssetEvent::RestartRequired { keys: keys.clone() },
                        });
                    }
                }
            }
            index += 1;
        }
    }

    pub(crate) fn subscribe_published(&self) -> watch::Receiver<u64> {
        self.handle.subscribe()
    }
}

impl Root {
    /// Target- and compiled-registry-free bootstrap for failure-safe metadata,
    /// diagnostics, authored-value inspection, and immutable CAS reads. The
    /// metadata hub is a connection of its own.
    pub fn metadata(&self, protocol: u32) -> MetadataConnectOutcome {
        let handle = &self.handle;
        let expected = read_consistent(&handle.config, |reader| reader.rpc_fences())
            .map(|fences| fences.protocol_epoch.unwrap_or(PROTOCOL_VERSION))
            .unwrap_or_else(|error| panic!("cannot read the served store: {error}"));
        if protocol != expected {
            return MetadataConnectOutcome::ProtocolMismatch {
                expected,
                observed: protocol,
            };
        }
        let Some(admission) = handle.admit_connection() else {
            return MetadataConnectOutcome::Refused(connection_limit(handle));
        };
        MetadataConnectOutcome::Connected(MetadataConnected {
            hub: MetadataHub {
                binding: Rc::new(MetadataBinding {
                    id: handle.connection_id(),
                    protocol_epoch: expected,
                }),
                server: Server::connection(handle, admission),
            },
            instance: handle.instance,
            protocol_epoch: expected,
        })
    }

    /// Bind a target connection: its own front end, fences, and stream.
    pub fn connect(&self, request: ConnectRequest) -> ConnectOutcome {
        let handle = &self.handle;
        let target_name = request.target.nfc().collect::<String>();
        let read = read_consistent(&handle.config, |reader| {
            Ok((
                reader.rpc_fences()?,
                reader.rpc_target(&target_name)?,
                reader.configuration_state()?,
                reader.served_blob(SERVED_PIPELINE)?,
                reader.change_log_head()?,
            ))
        });
        let (fences, target, configuration, pipeline, head) = match read {
            Ok(read) => read,
            Err(error) => panic!("cannot read the served store: {error}"),
        };
        let protocol = fences.protocol_epoch.unwrap_or(PROTOCOL_VERSION);
        if request.protocol != protocol {
            return ConnectOutcome::Rejected(ConnectError::ProtocolMismatch {
                expected: protocol,
                got: request.protocol,
            });
        }
        let Some(target) = target else {
            return ConnectOutcome::Rejected(ConnectError::UnknownTarget {
                target: target_name,
            });
        };
        if TargetDefinitionHash(target.definition_hash) != request.target_definition_hash {
            return ConnectOutcome::Rejected(ConnectError::TargetDefinitionMismatch {
                expected: TargetDefinitionHash(target.definition_hash),
                got: request.target_definition_hash,
            });
        }
        if let ConfigurationStatus::Failed(error) = configuration_status(&configuration) {
            return ConnectOutcome::ConfigurationFailed(error);
        }
        match read_served_pipeline(pipeline).map(|(_, pipeline)| pipeline) {
            Ok(PipelineDiagnostic::Ready) => {}
            Ok(PipelineDiagnostic::Failed(failure)) => {
                return ConnectOutcome::PipelineUnavailable(
                    PipelineUnavailableDiagnostic::PipelineFailure(failure),
                );
            }
            Err(error) => panic!("cannot read the served pipeline: {error}"),
        }
        let Some(admission) = handle.admit_connection() else {
            return ConnectOutcome::Refused(connection_limit(handle));
        };
        let server = Server::connection(handle, admission);
        let connection = server
            .inner
            .open_connection(target_name, target.generation, &fences, head);
        ConnectOutcome::Connected(Connected {
            hub: Hub { connection, server },
            instance: handle.instance,
        })
    }
}

/// Why a connection past `max_connections` is refused.
fn connection_limit(handle: &ServerHandle) -> RpcFailure {
    RpcFailure::ResourceLimit {
        resource: "connections".to_owned(),
        limit: handle.snapshot_policy().max_connections,
    }
}

#[derive(Clone)]
pub(crate) struct MetadataBinding {
    pub(crate) id: u64,
    pub(crate) protocol_epoch: u32,
}

impl Inner {
    pub(crate) fn metadata_fence(
        &self,
        binding: &MetadataBinding,
    ) -> Option<MetadataReconnectReason> {
        (self.protocol_epoch() != binding.protocol_epoch)
            .then_some(MetadataReconnectReason::ProtocolEpochChanged)
    }
}

/// Publish an authoring-backend commit as the version after `base`. The
/// caller holds the input open across the backend's prepare step and
/// this.
pub(crate) fn publish_backend_commit(
    server: &Server,
    base: InputVersion,
    commit: Commit,
) -> Result<SnapshotStamp, RpcFailure> {
    server
        .publish_locked(base, commit, None)
        .map_err(|error| match error {
            CoordinatedCommitError::Stale { expected, observed } => {
                RpcFailure::StaleInputVersion {
                    expected: observed,
                    got: expected,
                }
            }
            CoordinatedCommitError::Invalid(error) => RpcFailure::InvalidAuthoringRequest {
                detail: format!("authoring backend produced an invalid commit: {error:?}"),
            },
            CoordinatedCommitError::Publication(detail) => RpcFailure::InvalidAuthoringRequest {
                detail,
            },
        })
}

pub(crate) fn is_embedded(server: &Server) -> bool {
    server.inner.handle.embedded()
}

#[cfg(test)]
mod bound_tests {
    use super::*;

    fn connect(server: &Server) -> ConnectOutcome {
        let request = ConnectRequest::new("dev", TargetDefinitionHash([7; 32]));
        server.root().connect(request)
    }

    #[test]
    fn capability_churn_keeps_bookkeeping_within_active_bounds() {
        let server = Server::new(
            StoreInstanceId([9; 16]),
            vec![TargetDefinition::new("dev", TargetDefinitionHash([7; 32]))],
        )
        .unwrap();
        server
            .install_snapshot_policy(SnapshotPolicy {
                ttl: Duration::from_secs(60 * 60),
                max_snapshots: 1,
                max_connections: 1,
            })
            .unwrap();

        let ConnectOutcome::Connected(first) = connect(&server) else {
            panic!("the first connection is admitted");
        };
        let first_hub = first.hub;
        let mut snapshots = Vec::new();
        for _ in 0..4096 {
            snapshots.push(match first_hub.snapshot() {
                RpcResult::Success(snapshot) => snapshot,
                other => panic!("expected snapshot, got {other:?}"),
            });
        }
        assert!(
            first_hub.server.inner.snapshots.borrow().len() <= 1,
            "released snapshots accumulated despite a one-snapshot bound"
        );
        assert_eq!(server.open_snapshots(), 1);

        for _ in 0..4096 {
            assert!(matches!(
                connect(&server),
                ConnectOutcome::Refused(RpcFailure::ResourceLimit { limit: 1, .. })
            ));
        }
        assert_eq!(server.handle().open_connections(), 1);

        drop(snapshots);
        assert_eq!(server.open_snapshots(), 0);
        drop(first_hub);
        assert_eq!(server.handle().open_connections(), 0);
        assert!(matches!(connect(&server), ConnectOutcome::Connected(_)));
    }
}
