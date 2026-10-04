//! The RPC server over the store's current-state tables (LOCKLESS.md §3).
//!
//! A [`ServerHandle`] is the shared, `Send + Sync` identity of one served
//! store: its configuration, snapshot policy, publication signal, backends,
//! two admission counters, and the store's [`StoreOpener`]. It is the only
//! state connections share, and none of it is per-connection: the counters
//! and the policy are atomics, and the opener is immutable but for the
//! operational configuration. Every owner writes through a writer of its
//! own, opened from the opener; SQLite's write lock orders them.
//!
//! Every client connection ([`Root::connect`], [`Root::metadata`]) owns a
//! private front end ([`Server`]): its own store reader, its own snapshots
//! (open read transactions), and for a target connection its subscription
//! queue and change-log cursor. Nothing of it is reachable from another
//! connection, so a connection lives on one thread and needs no locks; the
//! Cap'n Proto transport gives each one a thread of its own. A connection
//! learns of publications from the handle's watch signal and reads the
//! change log itself, from its own cursor. A connection opens its writer
//! on its first write and runs writes, imports and operations inline on
//! its own thread, so a call waiting on the write lock delays only its
//! own connection. Admin work (commits, installs) takes the caller's
//! writer explicitly ([`ServerHandle::coordinated_commit`] and friends);
//! [`Server::open`] wraps the handle with a writer of its own for callers
//! that have none.
//!
//! Embedded servers (tests, tools) own a private store in a temporary
//! directory. Daemon servers share the daemon's store: the daemon's
//! durable step and the served projection of each [`Commit`] commit as one
//! input ([`crate::apply_commit`]), and then the handle is signalled.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};
use unicode_normalization::UnicodeNormalization;

use distill_store::served::{Change, ChangeEntry};
use distill_store::{Store, StoreConfig, StoreError, StoreOpener, StoreReader, StoreWriter};

use crate::apply::{
    apply_commit, configuration_status, publish_pipeline_fence, publish_restart_required,
    publish_target_set, ApplyError,
};
use crate::persist::delta_state;
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

enum PublishError {
    Stale {
        expected: InputVersion,
        observed: InputVersion,
    },
    Invalid(AdminError),
    Store(StoreError),
}

/// The shared identity of one served store. Every connection opens a front
/// end of its own on it ([`Root::connect`]); writes go through a writer the
/// caller owns and passes in.
pub struct ServerHandle {
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
    /// Opens each owner's own reader and writer.
    opener: Arc<StoreOpener>,
}

impl fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerHandle")
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}

impl ServerHandle {
    fn new(
        instance: StoreInstanceId,
        config: StoreConfig,
        authoring_backend: Arc<dyn AuthoringBackend>,
        opener: Arc<StoreOpener>,
    ) -> Arc<Self> {
        Arc::new(Self {
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
            opener,
        })
    }

    /// Serve a daemon store. The target set must already be recorded
    /// ([`crate::publish_target_set`]).
    pub fn open(
        authoring_backend: Arc<dyn AuthoringBackend>,
        opener: Arc<StoreOpener>,
    ) -> Arc<Self> {
        let config = StoreConfig::clone(&opener.config());
        Self::new(opener.instance_id(), config, authoring_backend, opener)
    }

    /// Opens readers and writers on the served store.
    pub fn opener(&self) -> &Arc<StoreOpener> {
        &self.opener
    }

    pub fn instance(&self) -> StoreInstanceId {
        self.instance
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
    /// The caller's durable step has already committed the namespace as
    /// `base + 1` (or published nothing, and this transaction becomes that
    /// version); the served projection is applied on top.
    fn publish(
        &self,
        store: &mut Store,
        base: Option<InputVersion>,
        commit: Commit,
        targets: Option<BTreeMap<String, TargetDefinitionHash>>,
    ) -> Result<SnapshotStamp, PublishError> {
        let result = (|| {
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
            let observed = store.input_version().map_err(PublishError::Store)?;
            let base = base.unwrap_or(observed);
            let result = if observed == base {
                store
                    .input_transaction(|txn| {
                        let observed = txn.base_stamp().version;
                        if observed != base {
                            stale = Some(observed);
                            return Err(StoreError::Rejected {
                                detail: "stale publication base".to_owned(),
                            });
                        }
                        apply_commit(txn, &commit).map_err(&mut reject)?;
                        if let Some(targets) = &targets {
                            publish_target_set(txn, targets)?;
                        }
                        Ok(())
                    })
                    .map(|((), version)| version)
            } else if observed.0 == base.0 + 1 {
                store
                    .served_transaction(|txn| {
                        apply_commit(txn, &commit).map_err(&mut reject)?;
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
        })();
        // Inside an open input, readers are told once it commits.
        if result.is_ok() && !store.input_open() {
            self.notify_published();
        }
        result
    }

}

struct UnavailableBuildBackend;

#[cfg(test)]
struct UnavailableAuthoringBackend;

impl BuildBackend for UnavailableBuildBackend {
    fn start(&self, _view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        BuildStart::Answered(Ok(BuildAnswer::Drifted {
            input: request.drifted_input.clone(),
        }))
    }
}

#[cfg(test)]
impl AuthoringBackend for UnavailableAuthoringBackend {
    fn write_files(
        &self,
        _store: &mut Store,
        _base: InputVersion,
        _operations: &[AuthoringOp],
        _force_lossy: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "write".to_owned(),
        })
    }

    fn prepare_import(
        &self,
        _store: &mut Store,
        _base: InputVersion,
        _request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "import".to_owned(),
        })
    }

    fn prepare_reimport(
        &self,
        _store: &mut Store,
        _base: InputVersion,
        _bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "reimport".to_owned(),
        })
    }

    fn prepare_operation(
        &self,
        _store: &mut Store,
        _base: InputVersion,
        _operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "operation".to_owned(),
        })
    }
}

// ---------------------------------------------------------------------------
// Front end

/// A front end of one served store: one connection's, or an admin front end
/// ([`Server::open`]). Cheap to clone; not `Send`.
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
    /// This front end's writer, opened on its first write.
    writer: RefCell<Option<StoreWriter>>,
    /// This front end's snapshots of one version share a read transaction.
    current_txn: RefCell<Weak<SnapshotTxn>>,
    /// This front end's open snapshots, oldest first.
    snapshots: RefCell<VecDeque<Weak<SnapshotHold>>>,
    /// The connection its next read transaction begins on.
    spare: Spare,
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
    /// An admin front end of `handle` of the caller's own: commits,
    /// installs and current-state reads on its own reader and writer. It
    /// holds no connection state; every connection gets a front end of its
    /// own ([`Root::connect`]).
    pub fn open(handle: &Arc<ServerHandle>) -> Self {
        let reader = StoreReader::open(handle.config.clone())
            .unwrap_or_else(|error| panic!("cannot read the served store: {error}"));
        Self {
            inner: Rc::new(Inner::new(handle, reader, None)),
        }
    }

    /// A front end of its own for one admitted connection, on the reader
    /// its handshake read.
    fn connection(handle: &Arc<ServerHandle>, reader: StoreReader, admission: Admission) -> Self {
        Self {
            inner: Rc::new(Inner::new(handle, reader, Some(admission))),
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

    pub fn current_stamp(&self) -> Result<SnapshotStamp, StoreError> {
        self.inner.current_stamp()
    }

    /// A read snapshot of the current version, for a report.
    pub fn report_snapshot(&self) -> Result<ReportSnapshot<'_>, StoreError> {
        Ok(ReportSnapshot {
            server: self,
            txn: self.inner.current_snapshot()?,
        })
    }

}

/// One read snapshot a report runs on (see
/// [`crate::ReportOperation`]): the store at one version, and what the
/// server serves there.
pub struct ReportSnapshot<'a> {
    server: &'a Server,
    txn: Rc<SnapshotTxn>,
}

impl ReportSnapshot<'_> {
    pub fn stamp(&self) -> SnapshotStamp {
        self.txn.stamp
    }

    pub fn reader(&self) -> &StoreReader {
        self.txn.snapshot()
    }

    /// Batch requests for every runtime entry at this snapshot, for each
    /// target: what `doctor verify` rebuilds. This bypasses transport
    /// capabilities but not configuration or pipeline errors.
    pub fn verification_build_requests(&self) -> Result<Vec<BuildRequest>, RpcFailure> {
        let txn = &self.txn;
        if let ConfigurationStatus::Failed(error) = txn.configuration().map_err(store_failure)? {
            return Err(RpcFailure::InvalidQuery {
                detail: format!("cannot verify failed configuration: {error:?}"),
            });
        }
        if let Some(error) = pipeline_failure(self.server.inner.effective_pipeline(txn)) {
            return Err(error);
        }
        let snapshot = txn.snapshot();
        let targets = snapshot.rpc_targets().map_err(store_failure)?;
        let entries = snapshot
            .served_runtime_entry_types()
            .map_err(store_failure)?
            .into_iter()
            .map(|(uuid, type_uuid, terminal_type)| BuildEntry {
                uuid,
                type_uuid,
                terminal_type,
            })
            .collect::<Vec<_>>();
        let mut requests = Vec::new();
        for target in &targets {
            for entry in &entries {
                requests.push(BuildRequest {
                    work_class: BuildWorkClass::Batch,
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
}

impl ServerHandle {
    /// Fence every connection after the served pipeline epoch failed at
    /// runtime: a reconnect meets the failure, which the epoch holds in
    /// memory (see [`AuthoringBackend::pipeline_runtime_failure`]). Called
    /// inside an open input, the fence joins that input's version and
    /// readers learn of it when the input commits.
    pub fn coordinated_pipeline_fence(&self, store: &mut Store) -> Result<(), String> {
        store
            .served_transaction(|txn| publish_pipeline_fence(txn))
            .map_err(|error| error.to_string())?;
        // Inside an open input, readers are told once it commits.
        if !store.input_open() {
            self.notify_published();
        }
        Ok(())
    }

    /// The version `store` is at, as this server stamps it.
    pub fn stamp_of(&self, store: &StoreReader) -> Result<SnapshotStamp, StoreError> {
        Ok(SnapshotStamp {
            instance: self.instance,
            version: store.input_version()?,
        })
    }

    /// Publish one coordinator step against `base` on `store`. `publish`
    /// runs the daemon's durable step (if any) on that same writer, inside
    /// the input, and returns the RPC delta, which is then applied as the
    /// version after `base`. No other publication interleaves.
    pub fn coordinated_commit(
        &self,
        store: &mut Store,
        base: InputVersion,
        publish: impl FnOnce(&mut Store) -> Result<Commit, String>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        self.coordinated_maybe_commit(store, base, |store| publish(store).map(Some))
            .map(|stamp| stamp.expect("a coordinated commit always publishes"))
    }

    /// A coordinated publication that may terminate in durable memo state
    /// only; `None` leaves the input version untouched.
    pub fn coordinated_maybe_commit(
        &self,
        store: &mut Store,
        base: InputVersion,
        publish: impl FnOnce(&mut Store) -> Result<Option<Commit>, String>,
    ) -> Result<Option<SnapshotStamp>, CoordinatedCommitError> {
        self.coordinated_locked(store, base, publish, None)
    }

    fn coordinated_locked(
        &self,
        store: &mut Store,
        base: InputVersion,
        publish: impl FnOnce(&mut Store) -> Result<Option<Commit>, String>,
        targets: Option<BTreeMap<String, TargetDefinitionHash>>,
    ) -> Result<Option<SnapshotStamp>, CoordinatedCommitError> {
        // The step, its own writes and the served projection commit as one
        // input on this writer, begun here: the base is checked inside it,
        // and no reader sees the version before its rows.
        let observed = store
            .open_input()
            .map_err(|error| CoordinatedCommitError::Publication(error.to_string()))?;
        let step = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if observed != base {
                return Err(CoordinatedCommitError::Stale {
                    expected: base,
                    observed,
                });
            }
            match publish(store).map_err(CoordinatedCommitError::Publication)? {
                Some(commit) => self.publish_locked(store, base, commit, targets).map(Some),
                None => Ok(None),
            }
        }));
        let published = match step {
            Ok(Ok(published)) => published,
            Ok(Err(error)) => {
                let _ = store.finish_input(false);
                return Err(error);
            }
            Err(panic) => {
                let _ = store.finish_input(false);
                std::panic::resume_unwind(panic);
            }
        };
        store
            .finish_input(true)
            .map_err(|error| CoordinatedCommitError::Publication(error.to_string()))?;
        self.notify_published();
        Ok(published)
    }

    /// Apply `commit` as the version after `base`, inside the input open on
    /// `store`.
    pub(crate) fn publish_locked(
        &self,
        store: &mut Store,
        base: InputVersion,
        commit: Commit,
        targets: Option<BTreeMap<String, TargetDefinitionHash>>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        self.publish(store, Some(base), commit, targets)
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
        store: &mut Store,
        base: InputVersion,
        replacements: Vec<TargetDefinition>,
        publish: impl FnOnce(&mut Store) -> Result<Commit, String>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        let targets = target_map(replacements)
            .map_err(|error| CoordinatedCommitError::Publication(error.to_string()))?;
        self.coordinated_locked(store, base, |store| publish(store).map(Some), Some(targets))
            .map(|stamp| stamp.expect("a coordinated commit always publishes"))
    }

    /// Stage a restart-only edit (`stage` replaces the store's pending
    /// restart) and announce its RestartRequired keys, in one transaction.
    /// This does not advance the input version or mutate active
    /// configuration values.
    pub fn restart_required(
        &self,
        store: &mut Store,
        stage: impl FnOnce(&mut Store) -> Result<(), String>,
    ) -> Result<SnapshotStamp, String> {
        let pending_keys = |store: &Store| {
            store
                .pending_restart()
                .map(|pending| pending.map(|pending| pending.keys).unwrap_or_default())
                .map_err(|error| error.to_string())
        };
        let changed = store.write_transaction_with(
            |error| error.to_string(),
            |store| {
                let before = pending_keys(store)?;
                stage(store)?;
                let keys = pending_keys(store)?;
                store
                    .served_transaction(|txn| publish_restart_required(txn, &before, &keys))
                    .map_err(|error| error.to_string())
            },
        )?;
        // Inside an open input, readers are told once it commits.
        if changed && !store.input_open() {
            self.notify_published();
        }
        self.stamp_of(store).map_err(|error| error.to_string())
    }
}

impl Server {
    /// [`ServerHandle::coordinated_maybe_commit`] on this front end's own
    /// writer.
    pub(crate) fn coordinated_maybe_commit(
        &self,
        base: InputVersion,
        publish: impl FnOnce(&mut Store) -> Result<Option<Commit>, String>,
    ) -> Result<Option<SnapshotStamp>, CoordinatedCommitError> {
        self.inner.with_writer(|store| {
            self.inner
                .handle
                .coordinated_maybe_commit(store, base, publish)
        })
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

/// The RPC authoring carrier of entry `local_id` of a parsed bundle file:
/// its schema snapshot and value are the file's own.
pub fn bundle_authoring_entry(
    bundle: &distill_bundle::Bundle,
    local_id: &str,
    normalized_path: String,
    terminal_type: TypeUuid,
) -> Result<AuthoringEntry, String> {
    let entry = bundle
        .assets
        .get(local_id)
        .ok_or_else(|| format!("bundle has no entry {local_id:?}"))?;
    let schema = bundle
        .schemas
        .get(&entry.schema_hash)
        .ok_or_else(|| format!("bundle has no schema snapshot for entry {local_id:?}"))?;
    let logical_schema = distill_schema::ngp_schema::snapshot_to_json(schema)
        .map_err(|error| format!("cannot serialize verified schema: {error}"))?;
    Ok(AuthoringEntry {
        uuid: entry.uuid,
        bundle: bundle.uuid,
        local_id: local_id.to_owned(),
        normalized_path,
        type_uuid: entry.type_uuid,
        terminal_type,
        schema_hash: entry.schema_hash,
        logical_schema: Arc::from(logical_schema.into_bytes()),
        role: entry_role(entry.authoring_only),
        tags: BTreeMap::new(),
        value: authoring_value(&entry.data)?,
    })
}

/// An authored value as the RPC carries it: canonical JSON whose blobs are
/// `{"$distill_blob": index}` into the blob table.
fn authoring_value(value: &distill_json::AuthoredValue) -> Result<AuthoringValue, String> {
    use distill_json::AuthoredValue;
    fn rewrite(value: &AuthoredValue, blobs: &mut Vec<Arc<[u8]>>) -> AuthoredValue {
        match value {
            AuthoredValue::Blob(bytes) => {
                let index = blobs.len() as u128;
                blobs.push(Arc::from(bytes.clone()));
                AuthoredValue::Object(BTreeMap::from([(
                    "$distill_blob".to_owned(),
                    AuthoredValue::UInt(index),
                )]))
            }
            AuthoredValue::Array(values) => {
                AuthoredValue::Array(values.iter().map(|value| rewrite(value, blobs)).collect())
            }
            AuthoredValue::Object(values) => AuthoredValue::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), rewrite(value, blobs)))
                    .collect(),
            ),
            value => value.clone(),
        }
    }
    let mut blobs = Vec::new();
    let rewritten = rewrite(value, &mut blobs);
    let canonical = distill_json::write(&rewritten)
        .map_err(|error| format!("cannot serialize authored value: {error}"))?;
    Ok(AuthoringValue {
        canonical_value: Arc::from(canonical.into_bytes()),
        blobs,
    })
}

pub(crate) fn entry_role(authoring_only: bool) -> AuthoringEntryRole {
    if authoring_only {
        AuthoringEntryRole::AuthoringOnly
    } else {
        AuthoringEntryRole::Runtime
    }
}

/// The pipeline `snapshot` serves: its version's candidate failure, else
/// the runtime failure `backend` holds for the epoch it serves. A backend
/// that cannot see that epoch fails the read (retryably), never Ready.
fn pipeline_at(
    backend: &dyn AuthoringBackend,
    snapshot: &StoreReader,
) -> Result<PipelineDiagnostic, RpcFailure> {
    if let Some(failure) = snapshot.pipeline_failure().map_err(store_failure)? {
        return Ok(PipelineDiagnostic::Failed(failure));
    }
    Ok(backend
        .pipeline_runtime_failure(snapshot)?
        .map_or(PipelineDiagnostic::Ready, PipelineDiagnostic::Failed))
}

/// The failure a pipeline diagnostic gates a request with; a failure
/// reading it is one.
pub(crate) fn pipeline_failure(
    diagnostic: Result<PipelineDiagnostic, RpcFailure>,
) -> Option<RpcFailure> {
    match diagnostic {
        Err(error) => Some(error),
        Ok(PipelineDiagnostic::Ready) => None,
        Ok(PipelineDiagnostic::Failed(failure)) => Some(RpcFailure::PipelineUnavailable(Box::new(
            PipelineUnavailableDiagnostic::PipelineFailure(failure),
        ))),
    }
}

// ---------------------------------------------------------------------------
// Snapshots

/// One read transaction pinning one input version, on a connection of its
/// own, shared by every snapshot of that version on one front end. What it
/// pins is read through it when asked, each a primary-key read. Its
/// connection becomes its front end's spare when it ends.
pub(crate) struct SnapshotTxn {
    snapshot: Option<distill_store::served::StoreSnapshot>,
    spare: Spare,
    pub(crate) stamp: SnapshotStamp,
}

impl Drop for SnapshotTxn {
    fn drop(&mut self) {
        if let Some(snapshot) = self.snapshot.take() {
            keep_spare(&self.spare, snapshot);
        }
    }
}

impl SnapshotTxn {
    pub(crate) fn snapshot(&self) -> &StoreReader {
        self.snapshot
            .as_deref()
            .expect("a live snapshot transaction owns its snapshot")
    }

    /// The configuration status this snapshot pins.
    pub(crate) fn configuration(&self) -> Result<ConfigurationStatus, StoreError> {
        Ok(configuration_status(&self.snapshot().configuration_state()?))
    }

}

/// A front end's connection with no read transaction open, kept for its
/// next read transaction so that a new version does not open a connection.
/// It holds no state of the store's: a transaction on it reads the version
/// current when it begins. One is enough: a front end's transactions of
/// different versions overlap only while a client still holds an old
/// snapshot.
type Spare = Rc<RefCell<Option<StoreReader>>>;

/// End `snapshot`'s transaction and keep its connection as the spare, if
/// there is none.
fn keep_spare(spare: &RefCell<Option<StoreReader>>, snapshot: distill_store::served::StoreSnapshot) {
    match snapshot.into_reader() {
        Ok(reader) => {
            spare.borrow_mut().get_or_insert(reader);
        }
        Err(error) => tracing::error!(%error, "cannot end a read transaction"),
    }
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

/// A connection's handshake: several facts read from one committed
/// version, on the reader the connection then keeps.
fn handshake<T>(
    config: &StoreConfig,
    read: impl FnOnce(&StoreReader) -> Result<T, StoreError>,
) -> Result<(StoreReader, T), StoreError> {
    #[cfg(test)]
    bound_tests::HANDSHAKE_READERS.with(|opens| opens.set(opens.get() + 1));
    let snapshot = StoreReader::open(config.clone())?.begin_snapshot()?;
    let read = read(&snapshot)?;
    Ok((snapshot.into_reader()?, read))
}

impl Inner {
    fn new(handle: &Arc<ServerHandle>, reader: StoreReader, admission: Option<Admission>) -> Self {
        Self {
            reader,
            writer: RefCell::new(None),
            current_txn: RefCell::new(Weak::new()),
            snapshots: RefCell::new(VecDeque::new()),
            spare: Rc::new(RefCell::new(None)),
            handle: Arc::clone(handle),
            _admission: admission,
        }
    }

    /// Run `job` on this front end's writer, opening it on first use. Not
    /// reentrant: whatever `job` calls receives the writer explicitly.
    pub(crate) fn with_writer<T>(&self, job: impl FnOnce(&mut Store) -> T) -> T {
        let mut writer = self.writer.borrow_mut();
        let writer = match &mut *writer {
            Some(writer) => writer,
            empty => empty.insert(self.handle.opener.open_writer().unwrap_or_else(|error| {
                panic!("cannot open a writer on the served store: {error}")
            })),
        };
        job(writer)
    }

    pub(crate) fn current_stamp(&self) -> Result<SnapshotStamp, StoreError> {
        Ok(SnapshotStamp {
            instance: self.handle.instance,
            version: self.reader.input_version()?,
        })
    }

    /// Begin a read transaction on the spare connection, or on a new one
    /// while the spare is in use.
    fn begin_snapshot(&self) -> Result<distill_store::served::StoreSnapshot, StoreError> {
        let spare = self.spare.borrow_mut().take();
        let reader = match spare {
            Some(reader) => reader,
            None => {
                #[cfg(test)]
                bound_tests::READER_OPENS.with(|opens| opens.set(opens.get() + 1));
                StoreReader::open(self.handle.config.clone())?
            }
        };
        reader.begin_snapshot()
    }

    /// Read several facts from one committed version.
    pub(crate) fn read_consistent<T>(
        &self,
        read: impl FnOnce(&StoreReader) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let snapshot = self.begin_snapshot()?;
        let result = read(&snapshot);
        keep_spare(&self.spare, snapshot);
        result
    }

    /// The read transaction pinning the current version. Snapshots of one
    /// version on this front end share it; each new version begins a read
    /// transaction on the spare connection.
    pub(crate) fn current_snapshot(&self) -> Result<Rc<SnapshotTxn>, StoreError> {
        let current = self.reader.input_version()?;
        if let Some(txn) = self.current_txn.borrow().upgrade() {
            if txn.stamp.version == current {
                return Ok(txn);
            }
        }
        let snapshot = self.begin_snapshot()?;
        let txn = Rc::new(SnapshotTxn {
            stamp: snapshot.stamp(),
            snapshot: Some(snapshot),
            spare: Rc::clone(&self.spare),
        });
        *self.current_txn.borrow_mut() = Rc::downgrade(&txn);
        Ok(txn)
    }

    /// A snapshot's pipeline: its version's candidate failure, else the
    /// runtime failure of the epoch it serves, which the backend holds.
    pub(crate) fn effective_pipeline(&self, txn: &SnapshotTxn) -> Result<PipelineDiagnostic, RpcFailure> {
        pipeline_at(&*self.handle.authoring_backend(), txn.snapshot())
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
        pipeline_generation: u64,
        head: i64,
    ) -> Rc<RefCell<ConnectionState>> {
        Rc::new(RefCell::new(ConnectionState {
            id: self.handle.connection_id(),
            target,
            pipeline_generation,
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
        // A target definition changes only with a pipeline fence
        // ([`crate::publish_target_set`]): the one generation covers both.
        let pipeline_generation = match self.reader.rpc_pipeline_generation() {
            Ok(generation) => generation,
            Err(error) => {
                tracing::error!(%error, "cannot read the RPC fence");
                return Some(ReconnectReason::StoreInstanceChanged);
            }
        };
        (pipeline_generation != connection.pipeline_generation)
            .then_some(ReconnectReason::PipelineEpochChanged)
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
                Change::ReconnectAll => {
                    connection.fenced = true;
                    connection.enqueue(StreamEvent::Asset {
                        basis: RpcBasis { snapshot: stamp },
                        event: AssetEvent::ReconnectRequired {
                            reason: ReconnectReason::PipelineEpochChanged,
                        },
                    });
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
        if protocol != PROTOCOL_VERSION {
            return MetadataConnectOutcome::ProtocolMismatch {
                expected: PROTOCOL_VERSION,
                observed: protocol,
            };
        }
        let reader = match StoreReader::open(handle.config.clone()) {
            Ok(reader) => reader,
            Err(error) => return MetadataConnectOutcome::Refused(store_failure(error)),
        };
        let Some(admission) = handle.admit_connection() else {
            return MetadataConnectOutcome::Refused(connection_limit(handle));
        };
        MetadataConnectOutcome::Connected(MetadataConnected {
            hub: MetadataHub {
                binding: Rc::new(MetadataBinding {
                    id: handle.connection_id(),
                }),
                server: Server::connection(handle, reader, admission),
            },
            instance: handle.instance,
            protocol_epoch: PROTOCOL_VERSION,
        })
    }

    /// Bind a target connection: its own front end, fences, and stream.
    pub fn connect(&self, request: ConnectRequest) -> ConnectOutcome {
        let handle = &self.handle;
        let target_name = request.target.nfc().collect::<String>();
        let read = handshake(&handle.config, |reader| {
            Ok((
                reader.rpc_pipeline_generation()?,
                reader.rpc_target(&target_name)?,
                reader.configuration_state()?,
                pipeline_at(&*handle.authoring_backend(), reader),
                reader.change_log_head()?,
            ))
        });
        let (reader, (pipeline_generation, target, configuration, pipeline, head)) = match read {
            Ok(read) => read,
            Err(error) => return ConnectOutcome::Refused(store_failure(error)),
        };
        if request.protocol != PROTOCOL_VERSION {
            return ConnectOutcome::Rejected(ConnectError::ProtocolMismatch {
                expected: PROTOCOL_VERSION,
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
        let pipeline = match pipeline {
            Ok(pipeline) => pipeline,
            Err(failure) => return ConnectOutcome::Refused(failure),
        };
        if let PipelineDiagnostic::Failed(failure) = pipeline {
            return ConnectOutcome::PipelineUnavailable(
                PipelineUnavailableDiagnostic::PipelineFailure(failure),
            );
        }
        let Some(admission) = handle.admit_connection() else {
            return ConnectOutcome::Refused(connection_limit(handle));
        };
        let server = Server::connection(handle, reader, admission);
        let connection = server
            .inner
            .open_connection(target_name, pipeline_generation, head);
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
}

#[cfg(test)]
mod bound_tests {
    use super::*;

    thread_local! {
        /// Connections this thread's front ends opened for read
        /// transactions.
        pub(super) static READER_OPENS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        /// Connections handshakes opened.
        pub(super) static HANDSHAKE_READERS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }

    /// A target handshake reads on one connection, which its front end
    /// then keeps; a metadata handshake reads nothing and opens the one
    /// reader its front end keeps. A first snapshot opens none.
    #[test]
    fn a_connection_reads_its_handshake_on_the_reader_it_keeps() {
        let server = test_server();
        HANDSHAKE_READERS.with(|opens| opens.set(0));
        READER_OPENS.with(|opens| opens.set(0));
        let ConnectOutcome::Connected(connected) = connect(&server) else {
            panic!("the connection is admitted");
        };
        assert_eq!(HANDSHAKE_READERS.with(|opens| opens.get()), 1);
        let MetadataConnectOutcome::Connected(metadata) = server.root().metadata(PROTOCOL_VERSION) else {
            panic!("the metadata connection is admitted");
        };
        assert_eq!(HANDSHAKE_READERS.with(|opens| opens.get()), 1);
        assert_eq!(
            connected.hub.server.inner.reader.input_version().unwrap(),
            metadata.hub.server.inner.reader.input_version().unwrap()
        );
        assert_eq!(READER_OPENS.with(|opens| opens.get()), 0);
    }

    #[test]
    fn a_front_end_reads_each_version_on_the_connection_it_has() {
        let server = test_server();
        let ConnectOutcome::Connected(connected) = connect(&server) else {
            panic!("the connection is admitted");
        };
        let hub = connected.hub;
        let snapshot = |hub: &Hub| match hub.snapshot() {
            RpcResult::Success(snapshot) => snapshot,
            other => panic!("expected snapshot, got {other:?}"),
        };
        let publish = || {
            server.inner.with_writer(|store| store.input_transaction(|_| Ok(())).unwrap());
        };
        READER_OPENS.with(|opens| opens.set(0));
        for _ in 0..50 {
            drop(snapshot(&hub));
            publish();
        }
        assert_eq!(READER_OPENS.with(|opens| opens.get()), 1, "a connection per version");
        // A client holding an old version's snapshot while it takes the
        // new one needs a second connection, and only one.
        let mut held = snapshot(&hub);
        for _ in 0..50 {
            publish();
            held = snapshot(&hub);
        }
        drop(held);
        assert_eq!(READER_OPENS.with(|opens| opens.get()), 2);
    }

    /// A front end of a fresh daemon store serving target `dev`. The
    /// store directory lives as long as the returned server's handle.
    fn test_server() -> Server {
        let dir = tempfile::tempdir().unwrap().keep();
        let mut store = Store::open(StoreConfig::new(dir.join(".distill"))).unwrap();
        let targets = target_map(vec![TargetDefinition::new("dev", TargetDefinitionHash([7; 32]))]).unwrap();
        store
            .served_transaction(|txn| publish_target_set(txn, &targets))
            .unwrap();
        let (opener, writer) = StoreOpener::new(store);
        let handle = ServerHandle::open(Arc::new(UnavailableAuthoringBackend), opener);
        let server = Server::open(&handle);
        *server.inner.writer.borrow_mut() = Some(writer);
        server
    }

    fn connect(server: &Server) -> ConnectOutcome {
        let request = ConnectRequest::new("dev", TargetDefinitionHash([7; 32]));
        server.root().connect(request)
    }

    #[test]
    fn capability_churn_keeps_bookkeeping_within_active_bounds() {
        let server = test_server();
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
