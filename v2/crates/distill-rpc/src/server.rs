use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use unicode_normalization::UnicodeNormalization;

use distill_json::AuthoredValue;
use distill_schema::ngp_schema::{verify_snapshot, PrimitiveKind, SchemaNode};

use crate::*;

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;
const MAX_PENDING_STREAM_EVENTS: usize = 1024;
const MAX_RETAINED_HISTORY_DELTAS: usize = 4096;
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(5 * 60);
const DEFAULT_MAX_SNAPSHOT_LEASES: usize = 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 256;
pub const MAX_SUBSCRIBED_ASSETS: usize = 4096;
pub const MAX_SUBSCRIBED_PATHS: usize = 4096;

/// Resource bounds for target-bound RPC capabilities. Snapshot leases are
/// absolute: clients obtain a fresh capability through `refresh`. Hub
/// connections use the same deadline and reconnect after expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeasePolicy {
    pub ttl: Duration,
    pub max_snapshot_leases: usize,
    pub max_connections: usize,
}

impl Default for LeasePolicy {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_LEASE_TTL,
            max_snapshot_leases: DEFAULT_MAX_SNAPSHOT_LEASES,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

impl LeasePolicy {
    fn validate(self) -> Result<Self, &'static str> {
        if self.ttl.is_zero() {
            return Err("RPC lease TTL must be nonzero");
        }
        if Instant::now().checked_add(self.ttl).is_none() {
            return Err("RPC lease TTL is too large");
        }
        if self.max_snapshot_leases == 0 {
            return Err("RPC snapshot lease bound must be nonzero");
        }
        if self.max_connections == 0 {
            return Err("RPC connection bound must be nonzero");
        }
        Ok(self)
    }
}

#[derive(Clone)]
pub struct Server {
    inner: Arc<Mutex<ServerState>>,
    authoring_backend: Arc<dyn AuthoringBackend>,
    build_backend: Arc<RwLock<Arc<dyn BuildBackend>>>,
    lease_backend: Arc<RwLock<Arc<dyn ArtifactLeaseBackend>>>,
    payload_backend: Arc<RwLock<Arc<dyn ArtifactPayloadBackend>>>,
    next_lease_id: Arc<AtomicU64>,
    lease_policy: Arc<RwLock<LeasePolicy>>,
    snapshot_leases: Arc<Mutex<VecDeque<Weak<ViewLease>>>>,
}

struct UnavailableAuthoringBackend;

struct UnavailableBuildBackend;

struct InMemoryArtifactLeases;

#[derive(Default)]
struct InMemoryArtifactPayloads {
    artifacts: Mutex<HashMap<ContentHash, ArtifactPayload>>,
    wire_trees: Mutex<HashMap<LayoutHash, Arc<[u8]>>>,
}

impl ArtifactLeaseBackend for InMemoryArtifactLeases {
    fn pin_lease(&self, _holder: u64, _hashes: &[[u8; 32]]) -> Result<(), String> {
        Ok(())
    }

    fn release_lease(&self, _holder: u64) {}
}

impl ArtifactPayloadBackend for InMemoryArtifactPayloads {
    fn store_artifact(&self, hash: ContentHash, payload: &ArtifactPayload) -> Result<(), String> {
        let mut artifacts = self
            .artifacts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = artifacts.get(&hash) {
            if existing != payload {
                return Err("artifact hash already has a different payload".to_owned());
            }
            return Ok(());
        }
        artifacts.insert(hash, payload.clone());
        Ok(())
    }

    fn load_artifact(&self, hash: ContentHash) -> Result<Option<ArtifactPayload>, String> {
        Ok(self
            .artifacts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .cloned())
    }

    fn store_wire_tree(&self, hash: LayoutHash, bytes: &[u8]) -> Result<(), String> {
        let mut wire_trees = self
            .wire_trees
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = wire_trees.get(&hash) {
            if existing.as_ref() != bytes {
                return Err("wire-tree hash already has different bytes".to_owned());
            }
            return Ok(());
        }
        wire_trees.insert(hash, Arc::from(bytes));
        Ok(())
    }

    fn load_wire_tree(&self, hash: LayoutHash) -> Result<Option<Arc<[u8]>>, String> {
        Ok(self
            .wire_trees
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .cloned())
    }
}

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

struct ServerOperationCompletion {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
    base: InputVersion,
    publication: Mutex<Option<PreparedOperationPublication>>,
}

struct MetadataServerSchemaTransitionCompletion {
    server: Server,
    binding: Arc<MetadataBinding>,
    base: InputVersion,
    publication: Mutex<Option<PreparedOperationPublication>>,
}

impl ProgressCompletion for MetadataServerSchemaTransitionCompletion {
    fn complete(&self) -> Result<(), String> {
        let publication = self
            .publication
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
            .ok_or_else(|| "schema transition is already terminal".to_owned())?;
        let mut state = self.server.lock();
        if let Some(reason) = metadata_fence(&state, &self.binding) {
            return Err(format!(
                "schema transition lost its metadata binding: {reason:?}"
            ));
        }
        if state.current != self.base {
            return Err(format!(
                "schema transition lost its input basis: expected {:?}, observed {:?}",
                self.base, state.current
            ));
        }
        let (commit, terminal_error) = match publication {
            PreparedOperationPublication::Immediate(commit) => (*commit, None),
            PreparedOperationPublication::Deferred(operation) => {
                let completed = operation.complete(self.base)?;
                (completed.commit, completed.terminal_error)
            }
        };
        commit_locked(&mut state, commit)
            .map(|_| ())
            .map_err(|error| format!("schema-transition commit rejected: {error:?}"))?;
        terminal_error.map_or(Ok(()), Err)
    }

    fn cancel(&self) -> bool {
        self.publication
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
            .is_some()
    }
}

impl ProgressCompletion for ServerOperationCompletion {
    fn complete(&self) -> Result<(), String> {
        let publication = self
            .publication
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
            .ok_or_else(|| "long-running operation is already terminal".to_owned())?;
        let mut state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(gate) = authoring_gate(&state, &connection, self.base) {
            return Err(format!(
                "long-running operation lost its publication basis: {gate:?}"
            ));
        }
        drop(connection);
        let (commit, terminal_error) = match publication {
            PreparedOperationPublication::Immediate(commit) => (*commit, None),
            PreparedOperationPublication::Deferred(operation) => {
                let completed = operation.complete(self.base)?;
                (completed.commit, completed.terminal_error)
            }
        };
        commit_locked(&mut state, commit)
            .map(|_| ())
            .map_err(|error| format!("long-running operation commit rejected: {error:?}"))?;
        terminal_error.map_or(Ok(()), Err)
    }

    fn cancel(&self) -> bool {
        self.publication
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
            .is_some()
    }
}

#[derive(Clone)]
pub struct Root {
    server: Server,
}

#[derive(Clone)]
pub struct LineageRepair {
    server: Server,
    binding: Arc<MetadataBinding>,
}

#[derive(Clone)]
pub struct Hub {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
}

#[derive(Clone)]
pub struct MetadataHub {
    server: Server,
    binding: Arc<MetadataBinding>,
}

#[derive(Clone)]
pub struct MetadataSnapshot {
    server: Server,
    binding: Arc<MetadataBinding>,
    basis: MetadataBasis,
    lease: Arc<ViewLease>,
}

#[derive(Clone)]
pub struct MetadataAuthoringSnapshot {
    server: Server,
    binding: Arc<MetadataBinding>,
    basis: MetadataBasis,
    lease: Arc<ViewLease>,
}

#[derive(Clone)]
pub struct Snapshot {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
    connection_id: u64,
    basis: RpcBasis,
    lease: Arc<ViewLease>,
}

/// A single bounded pack-build scope attached to one snapshot lease. Calls
/// renew both the snapshot and its target connection; expiration or drop
/// releases every CAS pin held under the distinct pack-session pin class.
pub struct PackSession {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
    lease: Arc<ViewLease>,
    state: Arc<PackSessionLease>,
}

#[derive(Clone)]
pub struct AuthoringSnapshot {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
    connection_id: u64,
    basis: RpcBasis,
    lease: Arc<ViewLease>,
}

#[derive(Clone)]
pub struct DeltaStream {
    connection: Arc<Mutex<ConnectionState>>,
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

struct MetadataBinding {
    id: u64,
    store_instance: StoreInstanceId,
    protocol_epoch: u32,
}

struct ArtifactLease {
    holder: u64,
    backend: Arc<dyn ArtifactLeaseBackend>,
    alive: Mutex<bool>,
}

struct PackSessionLease {
    holder: u64,
    backend: Arc<dyn ArtifactLeaseBackend>,
    alive: Mutex<bool>,
}

/// Owns every resource retained by one snapshot capability. Expiration drops
/// the pinned version view as well as its CAS pins, so abandoned Cap'n Proto
/// capabilities cannot keep old daemon state alive after their lease ends.
struct ViewLease {
    deadline: Mutex<Instant>,
    expiry_active: Arc<AtomicBool>,
    view: Mutex<Option<Arc<VersionView>>>,
    artifact: Option<Arc<ArtifactLease>>,
    pack_session: Mutex<Option<Arc<PackSessionLease>>>,
}

impl ViewLease {
    fn view(&self) -> Option<Arc<VersionView>> {
        let deadline = *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.expiry_active.load(Ordering::Acquire) || Instant::now() >= deadline {
            return None;
        }
        self.view
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn alive(&self) -> bool {
        self.view().is_some()
    }

    fn expire(&self) {
        if !self.expiry_active.swap(false, Ordering::AcqRel) {
            return;
        }
        LeaseExpiryScheduler::cancel(&self.expiry_active);
        self.view
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(artifact) = &self.artifact {
            artifact.expire();
        }
        let pack_session = self
            .pack_session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(pack_session) = pack_session {
            pack_session.expire();
        }
    }

    fn renew(&self, deadline: Instant) -> bool {
        if !self.expiry_active.load(Ordering::Acquire) {
            return false;
        }
        let mut current = self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if Instant::now() >= *current || !self.expiry_active.load(Ordering::Acquire) {
            return false;
        }
        *current = deadline;
        true
    }

    fn expire_if_due(&self, scheduled_deadline: Instant) {
        let current = *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current == scheduled_deadline && Instant::now() >= current {
            self.expire();
        }
    }

    fn pin(&self, hashes: &[[u8; 32]]) -> Result<(), RpcFailure> {
        if self.view().is_none() {
            return Err(RpcFailure::LeaseExpired);
        }
        self.artifact
            .as_ref()
            .ok_or(RpcFailure::LeaseExpired)?
            .pin(hashes)
    }
}

impl Drop for ViewLease {
    fn drop(&mut self) {
        self.expire();
    }
}

impl ArtifactLease {
    fn pin(&self, hashes: &[[u8; 32]]) -> Result<(), RpcFailure> {
        let alive = self
            .alive
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*alive {
            return Err(RpcFailure::LeaseExpired);
        }
        self.backend
            .pin_lease(self.holder, hashes)
            .map_err(|detail| RpcFailure::InvalidQuery {
                detail: format!("cannot pin resolved artifact to snapshot lease: {detail}"),
            })
    }

    fn expire(&self) {
        let release = {
            let mut alive = self
                .alive
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::replace(&mut *alive, false)
        };
        if release {
            self.backend.release_lease(self.holder);
        }
    }
}

impl PackSessionLease {
    fn pin(&self, hashes: &[[u8; 32]]) -> Result<(), RpcFailure> {
        let alive = self
            .alive
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*alive {
            return Err(RpcFailure::LeaseExpired);
        }
        self.backend
            .pin_pack_session(self.holder, hashes)
            .map_err(|detail| RpcFailure::InvalidQuery {
                detail: format!("cannot pin artifact to pack-build session: {detail}"),
            })
    }

    fn expire(&self) {
        let release = {
            let mut alive = self
                .alive
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::replace(&mut *alive, false)
        };
        if release {
            self.backend.release_pack_session(self.holder);
        }
    }
}

impl Drop for PackSessionLease {
    fn drop(&mut self) {
        if *self
            .alive
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            self.backend.release_pack_session(self.holder);
        }
    }
}

impl Drop for PackSession {
    fn drop(&mut self) {
        self.state.expire();
        let mut slot = self
            .lease
            .pack_session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot
            .as_ref()
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            slot.take();
        }
    }
}

impl Drop for ArtifactLease {
    fn drop(&mut self) {
        if *self
            .alive
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            self.backend.release_lease(self.holder);
        }
    }
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server").finish_non_exhaustive()
    }
}

impl fmt::Debug for Root {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Root").finish_non_exhaustive()
    }
}

impl fmt::Debug for LineageRepair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LineageRepair")
            .field("connection_id", &self.binding.id)
            .finish()
    }
}

impl fmt::Debug for Hub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hub")
            .field("connection_id", &self.connection_id())
            .finish()
    }
}

impl fmt::Debug for MetadataHub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataHub")
            .field("connection_id", &self.connection_id())
            .finish()
    }
}

impl fmt::Debug for MetadataSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataSnapshot")
            .field("stamp", &self.basis.snapshot)
            .finish()
    }
}

impl fmt::Debug for MetadataAuthoringSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataAuthoringSnapshot")
            .field("stamp", &self.basis.snapshot)
            .finish()
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("connection_id", &self.connection_id)
            .field("stamp", &self.basis.snapshot)
            .finish()
    }
}

impl fmt::Debug for AuthoringSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthoringSnapshot")
            .field("connection_id", &self.connection_id)
            .field("stamp", &self.basis.snapshot)
            .finish()
    }
}

impl fmt::Debug for DeltaStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaStream").finish_non_exhaustive()
    }
}

struct ServerState {
    instance: StoreInstanceId,
    protocol_epoch: u32,
    pipeline_generation: u64,
    current: InputVersion,
    views: BTreeMap<InputVersion, Arc<VersionView>>,
    history: VecDeque<HistoryDelta>,
    oldest_available_cursor: InputVersion,
    targets: BTreeMap<String, TargetRuntime>,
    artifacts: HashMap<ContentHash, StoredArtifact>,
    wire_trees: HashMap<LayoutHash, StoredWireTree>,
    build_results: HashMap<BuildKey, BuildResolution>,
    connections: Vec<Weak<Mutex<ConnectionState>>>,
    next_connection_id: u64,
    chunk_size: usize,
    restart_required_keys: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BuildKey {
    basis: SnapshotStamp,
    target: String,
    asset: AssetUuid,
}

#[derive(Debug, Clone)]
enum BuildResolution {
    Built(ContentHash),
    Failed(String),
    Drifted(DriftedInput),
}

#[derive(Clone)]
struct VersionView {
    stamp: SnapshotStamp,
    configuration: ConfigurationStatus,
    pipeline: Arc<RwLock<PipelineDiagnostic>>,
    version_poison: Option<VersionPoison>,
    assets: BTreeMap<AssetUuid, VersionResolve>,
    authoring: BTreeMap<AssetUuid, AuthoringEntry>,
    paths: BTreeMap<String, BTreeSet<AssetUuid>>,
    derived_outputs: BTreeMap<AssetUuid, DerivedOutputEntry>,
    tag_poisons: BTreeMap<AssetUuid, BundleUuid>,
    lineage_repair: Option<LineageRepairState>,
}

#[derive(Clone, PartialEq, Eq)]
struct StoredArtifact {
    asset_uuid: AssetUuid,
    layout_hash: LayoutHash,
    load_edges: Vec<ServedLoadEdge>,
}

#[derive(Clone, PartialEq, Eq)]
struct StoredWireTree;

#[derive(Clone)]
enum VersionResolve {
    Built { content_hash: ContentHash },
    Drifted { input: DriftedInput },
    Failed { error: String },
    Deleted { at: SnapshotStamp },
}

struct TargetRuntime {
    definition: TargetDefinition,
    target_generation: u64,
}

struct ConnectionState {
    id: u64,
    target: String,
    target_generation: u64,
    store_instance: StoreInstanceId,
    protocol_epoch: u32,
    pipeline_generation: u64,
    deadline: Instant,
    expiry_active: Arc<AtomicBool>,
    active: bool,
    subscribed_assets: BTreeSet<AssetUuid>,
    subscribed_paths: BTreeSet<String>,
    queue: VecDeque<StreamEvent>,
    stream_installed: bool,
    notify: Arc<Notify>,
}

impl Drop for ConnectionState {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            LeaseExpiryScheduler::cancel(&self.expiry_active);
        }
    }
}

#[derive(Clone)]
enum LeaseExpiryTarget {
    View(Weak<ViewLease>),
    Connection(Weak<Mutex<ConnectionState>>),
}

impl LeaseExpiryTarget {
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::View(left), Self::View(right)) => Weak::ptr_eq(left, right),
            (Self::Connection(left), Self::Connection(right)) => Weak::ptr_eq(left, right),
            _ => false,
        }
    }

    fn expire_if_due(self, scheduled_deadline: Instant) {
        match self {
            Self::View(lease) => {
                if let Some(lease) = lease.upgrade() {
                    lease.expire_if_due(scheduled_deadline);
                }
            }
            Self::Connection(connection) => {
                if let Some(connection) = connection.upgrade() {
                    let mut connection = lock_connection(&connection);
                    if connection.deadline == scheduled_deadline
                        && Instant::now() >= connection.deadline
                    {
                        expire_connection(&mut connection);
                    }
                }
            }
        }
    }
}

struct ScheduledLeaseExpiry {
    deadline: Instant,
    active: Weak<AtomicBool>,
    target: LeaseExpiryTarget,
}

struct LeaseExpiryScheduler {
    entries: Mutex<Vec<ScheduledLeaseExpiry>>,
    wake: Condvar,
}

impl LeaseExpiryScheduler {
    fn global() -> &'static Arc<Self> {
        static SCHEDULER: OnceLock<Arc<LeaseExpiryScheduler>> = OnceLock::new();
        SCHEDULER.get_or_init(|| {
            let scheduler = Arc::new(Self {
                entries: Mutex::new(Vec::new()),
                wake: Condvar::new(),
            });
            let worker = Arc::clone(&scheduler);
            std::thread::Builder::new()
                .name("distill-rpc-lease-expiry".to_owned())
                .spawn(move || worker.run())
                .expect("failed to start RPC lease expiry worker");
            scheduler
        })
    }

    fn schedule(deadline: Instant, active: &Arc<AtomicBool>, target: LeaseExpiryTarget) {
        let scheduler = Self::global();
        let mut entries = scheduler
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::compact(&mut entries);
        entries.retain(|entry| !entry.target.same(&target));
        entries.push(ScheduledLeaseExpiry {
            deadline,
            active: Arc::downgrade(active),
            target,
        });
        drop(entries);
        scheduler.wake.notify_one();
    }

    fn cancel(active: &Arc<AtomicBool>) {
        active.store(false, Ordering::Release);
        let scheduler = Self::global();
        let mut entries = scheduler
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::compact(&mut entries);
        drop(entries);
        scheduler.wake.notify_one();
    }

    fn compact(entries: &mut Vec<ScheduledLeaseExpiry>) {
        entries.retain(|entry| {
            entry
                .active
                .upgrade()
                .is_some_and(|active| active.load(Ordering::Acquire))
        });
    }

    #[cfg(test)]
    fn entry_count() -> usize {
        let scheduler = Self::global();
        let mut entries = scheduler
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::compact(&mut entries);
        entries.len()
    }

    fn run(&self) {
        loop {
            let due = {
                let mut entries = self
                    .entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                loop {
                    Self::compact(&mut entries);
                    let now = Instant::now();
                    let mut due = Vec::new();
                    let mut index = 0;
                    while index < entries.len() {
                        if entries[index].deadline <= now {
                            let entry = entries.swap_remove(index);
                            due.push((entry.deadline, entry.target));
                        } else {
                            index += 1;
                        }
                    }
                    if !due.is_empty() {
                        break due;
                    }

                    if let Some(deadline) = entries.iter().map(|entry| entry.deadline).min() {
                        let timeout = deadline.saturating_duration_since(now);
                        let (next, _) = self
                            .wake
                            .wait_timeout(entries, timeout)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        entries = next;
                    } else {
                        entries = self
                            .wake
                            .wait(entries)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                }
            };
            for (deadline, target) in due {
                target.expire_if_due(deadline);
            }
        }
    }
}

#[derive(Clone)]
struct HistoryDelta {
    stamp: SnapshotStamp,
    assets: Vec<(AssetUuid, AssetDeltaState)>,
    paths: Vec<String>,
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

    /// Construct the RPC projection at an already-open durable store's
    /// current input version. Production performs startup reconciliation as
    /// the next coordinated commit; it must never replay `version` empty
    /// commits merely to make two counters agree.
    pub fn new_at_version_with_authoring_backend(
        instance: StoreInstanceId,
        version: InputVersion,
        targets: Vec<TargetDefinition>,
        authoring_backend: Arc<dyn AuthoringBackend>,
    ) -> Result<Self, TargetSetError> {
        let mut target_map = BTreeMap::new();
        for target in targets {
            let name = target.name().to_owned();
            if target_map
                .insert(
                    name.clone(),
                    TargetRuntime {
                        definition: target,
                        target_generation: 0,
                    },
                )
                .is_some()
            {
                return Err(TargetSetError::DuplicateTarget { target: name });
            }
        }
        let stamp = SnapshotStamp { instance, version };
        let view = Arc::new(VersionView {
            stamp,
            configuration: ConfigurationStatus::Ready,
            pipeline: Arc::new(RwLock::new(PipelineDiagnostic::Ready)),
            version_poison: None,
            assets: BTreeMap::new(),
            authoring: BTreeMap::new(),
            paths: BTreeMap::new(),
            derived_outputs: BTreeMap::new(),
            tag_poisons: BTreeMap::new(),
            lineage_repair: None,
        });
        let mut views = BTreeMap::new();
        views.insert(version, view);
        Ok(Self {
            inner: Arc::new(Mutex::new(ServerState {
                instance,
                protocol_epoch: PROTOCOL_VERSION,
                pipeline_generation: 0,
                current: version,
                views,
                history: VecDeque::new(),
                oldest_available_cursor: version,
                targets: target_map,
                artifacts: HashMap::new(),
                wire_trees: HashMap::new(),
                build_results: HashMap::new(),
                connections: Vec::new(),
                next_connection_id: 1,
                chunk_size: DEFAULT_CHUNK_SIZE,
                restart_required_keys: BTreeSet::new(),
            })),
            authoring_backend,
            build_backend: Arc::new(RwLock::new(Arc::new(UnavailableBuildBackend))),
            lease_backend: Arc::new(RwLock::new(Arc::new(InMemoryArtifactLeases))),
            payload_backend: Arc::new(RwLock::new(Arc::new(InMemoryArtifactPayloads::default()))),
            next_lease_id: Arc::new(AtomicU64::new(1)),
            lease_policy: Arc::new(RwLock::new(LeasePolicy::default())),
            snapshot_leases: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    /// Replace the capability resource policy. Production should install this
    /// before accepting clients; lowering either bound expires the oldest
    /// extant snapshot leases or target connections immediately.
    pub fn install_lease_policy(&self, policy: LeasePolicy) -> Result<(), &'static str> {
        let policy = policy.validate()?;
        let mut installed_policy = self
            .lease_policy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *installed_policy = policy;
        let evicted = {
            let mut leases = self
                .snapshot_leases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            prune_snapshot_leases(&mut leases);
            let mut evicted = Vec::new();
            while leases.len() > policy.max_snapshot_leases {
                if let Some(lease) = leases.pop_front().and_then(|lease| lease.upgrade()) {
                    evicted.push(lease);
                }
                prune_snapshot_leases(&mut leases);
            }
            evicted
        };
        drop(installed_policy);
        for lease in evicted {
            lease.expire();
        }
        enforce_connection_bound(&mut self.lock(), policy.max_connections, false);
        Ok(())
    }

    fn register_view_lease(
        &self,
        view: Arc<VersionView>,
        artifact: Option<Arc<ArtifactLease>>,
    ) -> Arc<ViewLease> {
        let policy_guard = self
            .lease_policy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let policy = *policy_guard;
        let expiry_active = Arc::new(AtomicBool::new(true));
        let deadline = Instant::now()
            .checked_add(policy.ttl)
            .expect("validated RPC lease TTL must fit in Instant");
        let lease = Arc::new(ViewLease {
            deadline: Mutex::new(deadline),
            expiry_active: Arc::clone(&expiry_active),
            view: Mutex::new(Some(view)),
            artifact,
            pack_session: Mutex::new(None),
        });
        let evicted = {
            let mut leases = self
                .snapshot_leases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            prune_snapshot_leases(&mut leases);
            let mut evicted = Vec::new();
            while leases.len() >= policy.max_snapshot_leases {
                if let Some(oldest) = leases.pop_front().and_then(|lease| lease.upgrade()) {
                    evicted.push(oldest);
                }
                prune_snapshot_leases(&mut leases);
            }
            leases.push_back(Arc::downgrade(&lease));
            evicted
        };
        drop(policy_guard);
        LeaseExpiryScheduler::schedule(
            deadline,
            &expiry_active,
            LeaseExpiryTarget::View(Arc::downgrade(&lease)),
        );
        for oldest in evicted {
            oldest.expire();
        }
        lease
    }

    /// Replace the lazy-build implementation used by subsequent drifted
    /// resolves. Existing snapshot capabilities remain valid because results
    /// are keyed by their complete snapshot/target/asset basis.
    pub fn install_build_backend(&self, backend: Arc<dyn BuildBackend>) {
        *self
            .build_backend
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = backend;
    }

    /// Install the daemon CAS lease ledger before accepting client snapshots.
    pub fn install_artifact_lease_backend(&self, backend: Arc<dyn ArtifactLeaseBackend>) {
        *self
            .lease_backend
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = backend;
    }

    /// Install the immutable daemon CAS reader before accepting runtime
    /// snapshots. The server keeps only small authenticated metadata.
    pub fn install_artifact_payload_backend(&self, backend: Arc<dyn ArtifactPayloadBackend>) {
        *self
            .payload_backend
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = backend;
    }

    pub fn root(&self) -> Root {
        Root {
            server: self.clone(),
        }
    }

    pub fn instance(&self) -> StoreInstanceId {
        self.lock().instance
    }

    pub fn current_stamp(&self) -> SnapshotStamp {
        let state = self.lock();
        stamp(&state)
    }

    /// Snapshot-pinned batch requests used by `doctor verify`. This bypasses
    /// transport capabilities but not configuration/pipeline/version poison;
    /// the daemon still executes each request through the ordinary build core.
    pub fn verification_build_requests(&self) -> Result<Vec<BuildRequest>, RpcFailure> {
        let state = self.lock();
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        if let Some(poison) = &view.version_poison {
            return Err(RpcFailure::InvalidQuery {
                detail: format!("cannot verify a poisoned input version: {poison:?}"),
            });
        }
        if let ConfigurationStatus::Poisoned(poison) = &view.configuration {
            return Err(RpcFailure::InvalidQuery {
                detail: format!("cannot verify poisoned configuration: {poison:?}"),
            });
        }
        if let Some(error) = pipeline_failure(view) {
            return Err(error);
        }
        let mut requests = Vec::new();
        for (target_name, target) in &state.targets {
            for entry in view
                .authoring
                .values()
                .filter(|entry| entry.role == AuthoringEntryRole::Runtime)
            {
                requests.push(BuildRequest {
                    work_class: BuildWorkClass::Batch,
                    basis: view.stamp,
                    target: target_name.clone(),
                    target_definition: target.definition.definition_hash(),
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

    /// Persist and publish a runtime failure for the pipeline epoch currently
    /// shared by every snapshot that pins it. The durable closure runs under
    /// the server publication lock, preserving the coordinator's
    /// pipeline -> RPC -> store lock order and preventing a successor epoch
    /// from being poisoned by a stale observation.
    pub fn coordinated_runtime_pipeline_poison(
        &self,
        poison: PipelinePoison,
        persist: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        poison
            .validate()
            .map_err(|error| format!("invalid runtime pipeline poison: {error:?}"))?;
        if poison.origin != PipelinePoisonOrigin::PublishedRuntime {
            return Err("runtime poison publication requires PublishedRuntime origin".to_owned());
        }

        let mut state = self.lock();
        let view = Arc::clone(
            state
                .views
                .get(&state.current)
                .expect("current view must exist"),
        );
        {
            let diagnostic = view
                .pipeline
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &*diagnostic {
                PipelineDiagnostic::Ready => {}
                PipelineDiagnostic::Poisoned(existing) if existing == &poison => return Ok(()),
                other => {
                    return Err(format!(
                        "current RPC pipeline is not the observed ready epoch: {other:?}"
                    ))
                }
            }
        }
        persist()?;
        *view
            .pipeline
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            PipelineDiagnostic::Poisoned(poison);
        state.pipeline_generation = state
            .pipeline_generation
            .checked_add(1)
            .expect("pipeline generation exhausted");
        notify_all_reconnect(&mut state, ReconnectReason::PipelineEpochChanged);
        Ok(())
    }

    /// Publish a replacement store instance and fence every extant
    /// capability. Production uses this when disposable daemon state is
    /// recreated; retaining it here makes the store component of the fence
    /// tuple explicit and testable.
    pub fn replace_store_instance(&self, instance: StoreInstanceId) -> SnapshotStamp {
        let mut state = self.lock();
        if state.instance == instance {
            return stamp(&state);
        }
        state.instance = instance;
        advance_empty_version(&mut state);
        notify_all_reconnect(&mut state, ReconnectReason::StoreInstanceChanged);
        stamp(&state)
    }

    /// Advance the protocol epoch and fence every existing connection.
    pub fn replace_protocol_epoch(&self, protocol_epoch: u32) -> SnapshotStamp {
        let mut state = self.lock();
        if state.protocol_epoch == protocol_epoch {
            return stamp(&state);
        }
        state.protocol_epoch = protocol_epoch;
        advance_empty_version(&mut state);
        notify_all_reconnect(&mut state, ReconnectReason::ProtocolEpochChanged);
        stamp(&state)
    }

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
        let layout_hash = parsed.layout_hash;
        let asset_uuid = parsed.asset_uuid;
        drop(parsed);
        let artifact = StoredArtifact {
            asset_uuid,
            layout_hash,
            load_edges: payload.load_edges.clone(),
        };
        {
            let state = self.lock();
            if let Some(existing) = state.artifacts.get(&hash) {
                return if existing == &artifact {
                    Ok(())
                } else {
                    Err(AdminError::ArtifactAlreadyExistsWithDifferentPayload { hash })
                };
            }
        }
        self.payload_backend
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .store_artifact(hash, &payload)
            .map_err(|detail| AdminError::InvalidArtifact {
                detail: format!("artifact payload backend rejected content: {detail}"),
            })?;
        let mut state = self.lock();
        if let Some(existing) = state.artifacts.get(&hash) {
            if existing != &artifact {
                return Err(AdminError::ArtifactAlreadyExistsWithDifferentPayload { hash });
            }
            return Ok(());
        }
        state.artifacts.insert(hash, artifact);
        Ok(())
    }

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
        {
            let state = self.lock();
            if state.wire_trees.contains_key(&hash) {
                return Ok(());
            }
        }
        self.payload_backend
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .store_wire_tree(hash, &bytes)
            .map_err(|detail| AdminError::InvalidWireTree {
                detail: format!("wire-tree payload backend rejected content: {detail}"),
            })?;
        let mut state = self.lock();
        let tree = StoredWireTree;
        if let Some(existing) = state.wire_trees.get(&hash) {
            if existing != &tree {
                return Err(AdminError::WireTreeAlreadyExistsWithDifferentPayload { hash });
            }
            return Ok(());
        }
        state.wire_trees.insert(hash, tree);
        Ok(())
    }

    fn install_build_publication(
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

    /// Publish an input-version commit and deliver its filtered live delta.
    pub fn commit(&self, commit: Commit) -> Result<SnapshotStamp, AdminError> {
        let mut state = self.lock();
        commit_locked(&mut state, commit)
    }

    /// Serialize one durable coordinator publication with the matching RPC
    /// projection. The closure runs while the server publication lock is held;
    /// watcher/configuration coordinators use the same lock order as RPC
    /// authoring backends, so a store version can never advance without its
    /// exact in-memory successor being installed before another publication.
    pub fn coordinated_commit(
        &self,
        base: InputVersion,
        publish: impl FnOnce() -> Result<Commit, String>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        let mut state = self.lock();
        if state.current != base {
            return Err(CoordinatedCommitError::Stale {
                expected: base,
                observed: state.current,
            });
        }
        let commit = publish().map_err(CoordinatedCommitError::Publication)?;
        commit_locked(&mut state, commit).map_err(CoordinatedCommitError::Invalid)
    }

    /// Serialize an attempted coordinator publication that may terminate in
    /// durable memo state only. `None` leaves the RPC input version untouched;
    /// this is used by failed watched imports whose outcome basis is recorded
    /// without pretending that source inputs changed.
    pub fn coordinated_maybe_commit(
        &self,
        base: InputVersion,
        publish: impl FnOnce() -> Result<Option<Commit>, String>,
    ) -> Result<Option<SnapshotStamp>, CoordinatedCommitError> {
        let mut state = self.lock();
        if state.current != base {
            return Err(CoordinatedCommitError::Stale {
                expected: base,
                observed: state.current,
            });
        }
        let Some(commit) = publish().map_err(CoordinatedCommitError::Publication)? else {
            return Ok(None);
        };
        commit_locked(&mut state, commit)
            .map(Some)
            .map_err(CoordinatedCommitError::Invalid)
    }

    /// Replace the complete configuration-bound target set in the same
    /// publication lock and input version as its durable coordinator commit.
    /// Validation finishes before the durable closure runs; installation is
    /// then infallible and connections are generation-fenced before unlock.
    pub fn coordinated_replace_target_set(
        &self,
        base: InputVersion,
        replacements: Vec<TargetDefinition>,
        publish: impl FnOnce() -> Result<Commit, String>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        let mut replacement_map = BTreeMap::new();
        for replacement in replacements {
            let name = replacement.name().to_owned();
            if replacement_map.insert(name.clone(), replacement).is_some() {
                return Err(CoordinatedCommitError::Publication(
                    TargetSetError::DuplicateTarget { target: name }.to_string(),
                ));
            }
        }

        let mut state = self.lock();
        if state.current != base {
            return Err(CoordinatedCommitError::Stale {
                expected: base,
                observed: state.current,
            });
        }
        let commit = publish().map_err(CoordinatedCommitError::Publication)?;
        let stamp = commit_locked(&mut state, commit).map_err(CoordinatedCommitError::Invalid)?;
        install_target_set(&mut state, replacement_map);
        Ok(stamp)
    }

    /// Discard cursor history strictly before `oldest_available`.
    pub fn discard_history_before(&self, oldest_available: InputVersion) {
        let mut state = self.lock();
        let clamped = InputVersion(oldest_available.0.min(state.current.0));
        state.oldest_available_cursor = clamped;
        while state
            .history
            .front()
            .is_some_and(|delta| delta.stamp.version <= clamped)
        {
            state.history.pop_front();
        }
    }

    /// Replace a staged target definition and fence every bound Hub.
    pub fn replace_target(
        &self,
        replacement: TargetDefinition,
    ) -> Result<SnapshotStamp, AdminError> {
        let mut state = self.lock();
        let name = replacement.name().to_owned();
        let runtime = state
            .targets
            .get_mut(&name)
            .ok_or_else(|| AdminError::UnknownTarget {
                target: name.clone(),
            })?;
        let target_changed = runtime.definition.definition_hash() != replacement.definition_hash();
        if !target_changed {
            return Ok(stamp(&state));
        }
        runtime.target_generation = runtime
            .target_generation
            .checked_add(1)
            .expect("target generation exhausted");
        runtime.definition = replacement;

        advance_empty_version(&mut state);
        notify_reconnect(&mut state, &name, ReconnectReason::TargetDefinitionChanged);
        Ok(stamp(&state))
    }

    /// Stage a valid restart-only edit. This deliberately does not advance
    /// the input version or mutate active configuration values.
    pub fn restart_required(&self, keys: Vec<String>) -> SnapshotStamp {
        let mut keys = keys;
        keys.sort();
        keys.dedup();
        let mut state = self.lock();
        let replacement = keys.iter().cloned().collect::<BTreeSet<_>>();
        if state.restart_required_keys == replacement {
            return stamp(&state);
        }
        state.restart_required_keys = replacement;
        if !keys.is_empty() {
            let keys = state
                .restart_required_keys
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            let current_stamp = stamp(&state);
            for connection in live_connections(&mut state) {
                let mut connection = lock_connection(&connection);
                if !connection.stream_installed {
                    continue;
                }
                let basis = basis_for(&connection, current_stamp);
                enqueue_event(
                    &mut connection,
                    StreamEvent::Asset {
                        basis,
                        event: AssetEvent::RestartRequired { keys: keys.clone() },
                    },
                );
            }
        }
        stamp(&state)
    }

    fn lock(&self) -> MutexGuard<'_, ServerState> {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

impl Root {
    /// Narrow, unbound recovery bootstrap. It is minted only while the
    /// authoritative configuration poison is exactly missing/duplicate
    /// lineage and exposes no metadata or target capability.
    pub fn lineage_repair(&self, protocol: u32) -> LineageRepairConnectOutcome {
        let mut state = self.server.lock();
        if protocol != state.protocol_epoch {
            return LineageRepairConnectOutcome::ProtocolMismatch {
                expected: state.protocol_epoch,
                observed: protocol,
            };
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        if let Err(outcome) = lineage_inspection(state.instance, view) {
            return match outcome {
                LineageInspectionFailure::Unavailable(unavailable) => {
                    LineageRepairConnectOutcome::Unavailable(unavailable)
                }
                LineageInspectionFailure::Invalid(_error) => {
                    // A same-code DSCP without its exact inspection is an
                    // invalid server publication, never authority to repair.
                    LineageRepairConnectOutcome::Unavailable(
                        LineageRepairUnavailable::OtherConfigurationPoison(
                            match &view.configuration {
                                ConfigurationStatus::Poisoned(poison) => poison.clone(),
                                ConfigurationStatus::Ready => {
                                    return LineageRepairConnectOutcome::Unavailable(
                                        LineageRepairUnavailable::ConfigurationReady,
                                    )
                                }
                            },
                        ),
                    )
                }
            };
        }
        let id = state.next_connection_id;
        state.next_connection_id = state
            .next_connection_id
            .checked_add(1)
            .expect("connection id exhausted");
        let binding = Arc::new(MetadataBinding {
            id,
            store_instance: state.instance,
            protocol_epoch: state.protocol_epoch,
        });
        LineageRepairConnectOutcome::Connected(LineageRepairConnected {
            repair: LineageRepair {
                server: self.server.clone(),
                binding,
            },
            instance: state.instance,
            protocol_epoch: state.protocol_epoch,
        })
    }

    /// Target- and compiled-registry-free bootstrap for poison-safe metadata,
    /// diagnostics, authored-value inspection, and immutable CAS reads.
    pub fn metadata(&self, protocol: u32) -> MetadataConnectOutcome {
        let mut state = self.server.lock();
        if protocol != state.protocol_epoch {
            return MetadataConnectOutcome::ProtocolMismatch {
                expected: state.protocol_epoch,
                observed: protocol,
            };
        }
        let id = state.next_connection_id;
        state.next_connection_id = state
            .next_connection_id
            .checked_add(1)
            .expect("connection id exhausted");
        let binding = Arc::new(MetadataBinding {
            id,
            store_instance: state.instance,
            protocol_epoch: state.protocol_epoch,
        });
        MetadataConnectOutcome::Connected(MetadataConnected {
            hub: MetadataHub {
                server: self.server.clone(),
                binding,
            },
            instance: state.instance,
            protocol_epoch: state.protocol_epoch,
        })
    }

    pub fn connect(&self, request: ConnectRequest) -> ConnectOutcome {
        let mut state = self.server.lock();
        if request.protocol != state.protocol_epoch {
            return ConnectOutcome::Rejected(ConnectError::ProtocolMismatch {
                expected: state.protocol_epoch,
                got: request.protocol,
            });
        }
        let target_name = request.target.nfc().collect::<String>();
        let runtime = match state.targets.get(&target_name) {
            Some(runtime) => runtime,
            None => {
                return ConnectOutcome::Rejected(ConnectError::UnknownTarget {
                    target: target_name,
                })
            }
        };
        if runtime.definition.definition_hash() != request.target_definition_hash {
            return ConnectOutcome::Rejected(ConnectError::TargetDefinitionMismatch {
                expected: runtime.definition.definition_hash(),
                got: request.target_definition_hash,
            });
        }
        let target_generation = runtime.target_generation;
        let current_view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        if let ConfigurationStatus::Poisoned(poison) = &current_view.configuration {
            return ConnectOutcome::ConfigurationPoisoned(poison.clone());
        }
        match pipeline_diagnostic(current_view) {
            PipelineDiagnostic::Ready => {}
            PipelineDiagnostic::Poisoned(poison) => {
                return ConnectOutcome::PipelineUnavailable(
                    PipelineUnavailableDiagnostic::PipelinePoison(poison),
                );
            }
            PipelineDiagnostic::SchemaAcceptanceRequired(required) => {
                return ConnectOutcome::PipelineUnavailable(
                    PipelineUnavailableDiagnostic::SchemaAcceptanceRequired(required),
                );
            }
            PipelineDiagnostic::RetiredTypeReferenced(retired) => {
                return ConnectOutcome::PipelineUnavailable(
                    PipelineUnavailableDiagnostic::RetiredTypeReferenced(retired),
                );
            }
        }

        let lease_policy = *self
            .server
            .lease_policy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        enforce_connection_bound(&mut state, lease_policy.max_connections, true);
        let id = state.next_connection_id;
        state.next_connection_id = state
            .next_connection_id
            .checked_add(1)
            .expect("connection id exhausted");
        let deadline = Instant::now()
            .checked_add(lease_policy.ttl)
            .expect("validated RPC lease TTL must fit in Instant");
        let expiry_active = Arc::new(AtomicBool::new(true));
        let connection = Arc::new(Mutex::new(ConnectionState {
            id,
            target: target_name,
            target_generation,
            store_instance: state.instance,
            protocol_epoch: state.protocol_epoch,
            pipeline_generation: state.pipeline_generation,
            deadline,
            expiry_active: Arc::clone(&expiry_active),
            active: true,
            subscribed_assets: BTreeSet::new(),
            subscribed_paths: BTreeSet::new(),
            queue: VecDeque::new(),
            stream_installed: false,
            notify: Arc::new(Notify::new()),
        }));
        state.connections.push(Arc::downgrade(&connection));
        LeaseExpiryScheduler::schedule(
            deadline,
            &expiry_active,
            LeaseExpiryTarget::Connection(Arc::downgrade(&connection)),
        );
        ConnectOutcome::Connected(Connected {
            hub: Hub {
                server: self.server.clone(),
                connection,
            },
            instance: state.instance,
        })
    }
}

enum LineageInspectionFailure {
    Unavailable(LineageRepairUnavailable),
    Invalid(RpcFailure),
}

fn lineage_inspection(
    instance: StoreInstanceId,
    view: &VersionView,
) -> Result<LineageRepairInspection, LineageInspectionFailure> {
    match &view.configuration {
        ConfigurationStatus::Ready => Err(LineageInspectionFailure::Unavailable(
            LineageRepairUnavailable::ConfigurationReady,
        )),
        ConfigurationStatus::Poisoned(poison) => match poison.detail.as_ref() {
            DscpV1::MissingLineageManifest => match &view.lineage_repair {
                Some(state @ LineageRepairState::Missing { .. }) => Ok(LineageRepairInspection {
                    instance,
                    stamp: view.stamp,
                    state: state.clone(),
                }),
                _ => Err(LineageInspectionFailure::Invalid(
                    RpcFailure::InvalidAuthoringRequest {
                        detail: "missing-lineage DSCP has no exact repair inspection".to_owned(),
                    },
                )),
            },
            DscpV1::DuplicateLineageManifest { entries } => match &view.lineage_repair {
                Some(state @ LineageRepairState::Duplicate { claimants })
                    if claimants == entries =>
                {
                    Ok(LineageRepairInspection {
                        instance,
                        stamp: view.stamp,
                        state: state.clone(),
                    })
                }
                _ => Err(LineageInspectionFailure::Invalid(
                    RpcFailure::InvalidAuthoringRequest {
                        detail: "duplicate-lineage DSCP disagrees with repair claimants".to_owned(),
                    },
                )),
            },
            _ => Err(LineageInspectionFailure::Unavailable(
                LineageRepairUnavailable::OtherConfigurationPoison(poison.clone()),
            )),
        },
    }
}

impl LineageRepair {
    pub fn inspect(&self) -> LineageRepairInspectOutcome {
        let state = self.server.lock();
        if let Some(reason) = metadata_fence(&state, &self.binding) {
            return LineageRepairInspectOutcome::ReconnectRequired { reason };
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        match lineage_inspection(state.instance, view) {
            Ok(inspection) => LineageRepairInspectOutcome::Success(inspection),
            Err(LineageInspectionFailure::Unavailable(unavailable)) => {
                LineageRepairInspectOutcome::Unavailable(unavailable)
            }
            Err(LineageInspectionFailure::Invalid(error)) => {
                LineageRepairInspectOutcome::Failure(error)
            }
        }
    }

    pub fn create_missing(
        &self,
        basis: LineageRepairInspection,
        canonical_manifest_bundle: Arc<[u8]>,
    ) -> LineageRepairMutationOutcome {
        let current = match self.mutation_preflight(&basis) {
            Ok(current) => current,
            Err(outcome) => return outcome,
        };
        if !matches!(basis.state, LineageRepairState::Missing { .. }) {
            return LineageRepairMutationOutcome::Invalid(LineageRepairInvalid {
                code: LineageRepairInvalidCode::WrongBasisState,
                message: "createMissing requires a Missing inspection".to_owned(),
            });
        }
        if let Err(invalid) = validate_manifest_repair_bundle(&canonical_manifest_bundle) {
            return LineageRepairMutationOutcome::Invalid(invalid);
        }
        debug_assert_eq!(current, basis);
        self.publish_lineage_repair(basis.clone(), || {
            self.server
                .authoring_backend
                .prepare_create_missing_lineage(&basis, &canonical_manifest_bundle)
        })
    }

    pub fn resolve_duplicate(
        &self,
        basis: LineageRepairInspection,
        survivor: LineageManifestClaimant,
    ) -> LineageRepairMutationOutcome {
        let current = match self.mutation_preflight(&basis) {
            Ok(current) => current,
            Err(outcome) => return outcome,
        };
        let LineageRepairState::Duplicate { claimants } = &basis.state else {
            return LineageRepairMutationOutcome::Invalid(LineageRepairInvalid {
                code: LineageRepairInvalidCode::WrongBasisState,
                message: "resolveDuplicate requires a Duplicate inspection".to_owned(),
            });
        };
        if claimants.binary_search(&survivor).is_err() {
            return LineageRepairMutationOutcome::Invalid(LineageRepairInvalid {
                code: LineageRepairInvalidCode::SurvivorNotClaimant,
                message: "selected survivor is not an exact current claimant".to_owned(),
            });
        }
        debug_assert_eq!(current, basis);
        self.publish_lineage_repair(basis.clone(), || {
            self.server
                .authoring_backend
                .prepare_resolve_duplicate_lineage(&basis, &survivor)
        })
    }

    fn mutation_preflight(
        &self,
        basis: &LineageRepairInspection,
    ) -> Result<LineageRepairInspection, LineageRepairMutationOutcome> {
        let state = self.server.lock();
        if let Some(reason) = metadata_fence(&state, &self.binding) {
            return Err(LineageRepairMutationOutcome::ReconnectRequired { reason });
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        let current = match lineage_inspection(state.instance, view) {
            Ok(current) => current,
            Err(LineageInspectionFailure::Unavailable(unavailable)) => {
                return Err(LineageRepairMutationOutcome::Unavailable(unavailable))
            }
            Err(LineageInspectionFailure::Invalid(error)) => {
                return Err(LineageRepairMutationOutcome::Failure(error))
            }
        };
        if &current != basis {
            return Err(LineageRepairMutationOutcome::StaleBasis(lineage_stale(
                basis, &current,
            )));
        }
        Ok(current)
    }

    fn publish_lineage_repair(
        &self,
        basis: LineageRepairInspection,
        prepare: impl FnOnce() -> Result<Commit, LineageRepairBackendError>,
    ) -> LineageRepairMutationOutcome {
        let mut state = self.server.lock();
        if let Some(reason) = metadata_fence(&state, &self.binding) {
            return LineageRepairMutationOutcome::ReconnectRequired { reason };
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        let current = match lineage_inspection(state.instance, view) {
            Ok(current) => current,
            Err(LineageInspectionFailure::Unavailable(unavailable)) => {
                return LineageRepairMutationOutcome::Unavailable(unavailable)
            }
            Err(LineageInspectionFailure::Invalid(error)) => {
                return LineageRepairMutationOutcome::Failure(error)
            }
        };
        if current != basis {
            return LineageRepairMutationOutcome::StaleBasis(lineage_stale(&basis, &current));
        }
        // This server mutex is the RPC coordinator lock: the durable backend
        // runs only after the exact destination/claimant basis CAS and no
        // competing RPC publication can interleave before the rescan-proven
        // Ready commit is installed below.
        let prepared = match prepare() {
            Ok(commit) => commit,
            Err(LineageRepairBackendError::Stale(stale)) => {
                return LineageRepairMutationOutcome::StaleBasis(stale)
            }
            Err(LineageRepairBackendError::Invalid(invalid)) => {
                return LineageRepairMutationOutcome::Invalid(invalid)
            }
            Err(LineageRepairBackendError::Failure(error)) => {
                return LineageRepairMutationOutcome::Failure(error)
            }
        };
        if !prepared.assets.is_empty()
            || !prepared.authoring.is_empty()
            || !prepared.paths.is_empty()
            || prepared.pipeline.is_some()
            || prepared.version_poison.is_some()
            || prepared.configuration != Some(ConfigurationStatus::Ready)
            || prepared.lineage_repair != Some(None)
        {
            return LineageRepairMutationOutcome::Invalid(LineageRepairInvalid {
                code: LineageRepairInvalidCode::WrongBasisState,
                message: "repair backend publication may only install a rescan-proven Ready state and clear its lineage inspection"
                    .to_owned(),
            });
        }
        match commit_locked(&mut state, prepared) {
            Ok(stamp) => LineageRepairMutationOutcome::Success(LineageRepairCommitted { stamp }),
            Err(error) => {
                LineageRepairMutationOutcome::Failure(RpcFailure::InvalidAuthoringRequest {
                    detail: format!("lineage repair publication rejected: {error:?}"),
                })
            }
        }
    }
}

fn lineage_stale(
    basis: &LineageRepairInspection,
    current: &LineageRepairInspection,
) -> LineageRepairStale {
    let code = if basis.state == current.state {
        LineageRepairStaleCode::StampChanged
    } else {
        match (&basis.state, &current.state) {
            (
                LineageRepairState::Missing {
                    destination: LineageRepairDestination::Absent,
                    ..
                },
                LineageRepairState::Missing {
                    destination: LineageRepairDestination::Occupied { .. },
                    ..
                },
            ) => LineageRepairStaleCode::DestinationAppeared,
            (LineageRepairState::Missing { .. }, LineageRepairState::Missing { .. }) => {
                LineageRepairStaleCode::PreimageChanged
            }
            (LineageRepairState::Duplicate { .. }, LineageRepairState::Duplicate { .. }) => {
                LineageRepairStaleCode::ClaimantChanged
            }
            _ => LineageRepairStaleCode::StateChanged,
        }
    };
    LineageRepairStale {
        code,
        observed_stamp: current.stamp,
    }
}

fn validate_manifest_repair_bundle(bytes: &[u8]) -> Result<(), LineageRepairInvalid> {
    let invalid = |code, message: &str| LineageRepairInvalid {
        code,
        message: message.to_owned(),
    };
    let bundle = distill_bundle::parse_bundle(bytes).map_err(|error| {
        invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            &format!("manifest bundle does not parse canonically: {error}"),
        )
    })?;
    let canonical = distill_bundle::write_bundle(&bundle).map_err(|error| {
        invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            &format!("manifest bundle cannot be rendered canonically: {error}"),
        )
    })?;
    if canonical != bytes {
        return Err(invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            "manifest bundle bytes are not the canonical encoding",
        ));
    }
    let mut manifests = bundle.assets.values().filter(|entry| {
        entry.type_uuid == distill_core::bootstrap::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID
    });
    let Some(manifest) = manifests.next() else {
        return Err(invalid(
            LineageRepairInvalidCode::MissingManifestEntry,
            "bundle contains no SchemaLineageManifest entry",
        ));
    };
    if manifests.next().is_some() {
        return Err(invalid(
            LineageRepairInvalidCode::MissingManifestEntry,
            "bundle must contain exactly one SchemaLineageManifest entry",
        ));
    }
    if !manifest.authoring_only {
        return Err(invalid(
            LineageRepairInvalidCode::NotAuthoringOnly,
            "SchemaLineageManifest entry must be authoring_only",
        ));
    }
    if !matches!(
        manifest.lineage,
        distill_bundle::EntryLineageV1::Bootstrap {
            bundle_format_version: 1
        }
    ) {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "SchemaLineageManifest entry must use format-v1 bootstrap lineage",
        ));
    }
    let authority =
        distill_core::bootstrap::bootstrap_control_logical_registry_v1().map_err(|error| {
            invalid(
                LineageRepairInvalidCode::InvalidLineage,
                &format!("bootstrap authority unavailable: {error}"),
            )
        })?;
    let expected = authority
        .get(&distill_core::bootstrap::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID)
        .expect("logical bootstrap authority contains lineage manifest");
    if manifest.schema_hash != *expected {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "SchemaLineageManifest entry has the wrong sealed logical schema",
        ));
    }
    let Some(type_keys) = lineage_manifest_type_keys(&manifest.data) else {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "SchemaLineageManifest data does not contain its canonical types map",
        ));
    };
    if type_keys
        .iter()
        .any(|type_uuid| distill_core::bootstrap::is_bootstrap_control_type(*type_uuid))
    {
        return Err(invalid(
            LineageRepairInvalidCode::BootstrapTypePresent,
            "SchemaLineageManifest types map contains a bootstrap-control TypeUuid",
        ));
    }
    Ok(())
}

fn lineage_manifest_type_keys(value: &AuthoredValue) -> Option<Vec<TypeUuid>> {
    let AuthoredValue::Object(fields) = value else {
        return None;
    };
    let AuthoredValue::Array(rows) = fields.get("types")? else {
        return None;
    };
    rows.iter()
        .map(|row| {
            let AuthoredValue::Array(pair) = row else {
                return None;
            };
            let [key, _value] = pair.as_slice() else {
                return None;
            };
            let AuthoredValue::Array(bytes) = key else {
                return None;
            };
            if bytes.len() != 16 {
                return None;
            }
            let mut uuid = [0; 16];
            for (output, byte) in uuid.iter_mut().zip(bytes) {
                let AuthoredValue::UInt(byte) = byte else {
                    return None;
                };
                *output = u8::try_from(*byte).ok()?;
            }
            Some(TypeUuid(uuid))
        })
        .collect()
}

impl MetadataHub {
    pub fn connection_id(&self) -> u64 {
        self.binding.id
    }

    pub fn snapshot(&self) -> MetadataCall<MetadataSnapshot> {
        let state = self.server.lock();
        if let Some(reason) = metadata_fence(&state, &self.binding) {
            return MetadataCall::ReconnectRequired { reason };
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist")
            .clone();
        let basis = metadata_basis(&self.binding, view.stamp);
        drop(state);
        MetadataCall::Success(MetadataSnapshot {
            server: self.server.clone(),
            binding: self.binding.clone(),
            basis,
            lease: self.server.register_view_lease(view, None),
        })
    }

    pub fn authoring_snapshot(&self) -> MetadataCall<MetadataAuthoringSnapshot> {
        let state = self.server.lock();
        if let Some(reason) = metadata_fence(&state, &self.binding) {
            return MetadataCall::ReconnectRequired { reason };
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist")
            .clone();
        let basis = metadata_basis(&self.binding, view.stamp);
        drop(state);
        MetadataCall::Success(MetadataAuthoringSnapshot {
            server: self.server.clone(),
            binding: self.binding.clone(),
            basis,
            lease: self.server.register_view_lease(view, None),
        })
    }

    pub fn diagnostics(&self) -> MetadataCall<MetadataDiagnostics> {
        let state = self.server.lock();
        if let Some(reason) = metadata_fence(&state, &self.binding) {
            return MetadataCall::ReconnectRequired { reason };
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        MetadataCall::Success(MetadataDiagnostics {
            stamp: view.stamp,
            configuration: view.configuration.clone(),
            pipeline: pipeline_diagnostic(view),
            version_poison: view.version_poison.clone(),
        })
    }

    pub fn fetch(&self, hash: ContentHash) -> MetadataCall<ChunkStream> {
        let state = self.server.lock();
        if let Some(reason) = metadata_fence(&state, &self.binding) {
            return MetadataCall::ReconnectRequired { reason };
        }
        let Some(metadata) = state.artifacts.get(&hash).cloned() else {
            return MetadataCall::Error(RpcFailure::ArtifactNotFound { hash });
        };
        let chunk_size = state.chunk_size;
        drop(state);
        match load_artifact_payload(&self.server, hash, &metadata) {
            Ok(payload) => MetadataCall::Success(chunk_payload(&payload, chunk_size)),
            Err(error) => MetadataCall::Error(error),
        }
    }

    /// Execute the one mutation needed to leave
    /// `SchemaAcceptanceRequired`. This intentionally lives on the unbound
    /// metadata capability because no target-bound Hub can be acquired while
    /// that diagnostic is current; it does not expose the general operation
    /// surface.
    pub fn schema_transition(
        &self,
        base: InputVersion,
        payload: Arc<[u8]>,
    ) -> MetadataCall<ProgressStream> {
        {
            let state = self.server.lock();
            if let Some(reason) = metadata_fence(&state, &self.binding) {
                return MetadataCall::ReconnectRequired { reason };
            }
            if state.current != base {
                return MetadataCall::Error(RpcFailure::StaleInputVersion {
                    expected: state.current,
                    got: base,
                });
            }
        }

        let prepared = match self
            .server
            .authoring_backend
            .prepare_operation(base, &LongRunningOp::SchemaTransition(payload))
        {
            Ok(prepared) => prepared,
            Err(error) => return MetadataCall::Error(error),
        };
        if let Err(detail) = validate_progress(&prepared.progress) {
            return MetadataCall::Error(RpcFailure::InvalidAuthoringRequest { detail });
        }

        {
            let state = self.server.lock();
            if let Some(reason) = metadata_fence(&state, &self.binding) {
                return MetadataCall::ReconnectRequired { reason };
            }
            if state.current != base {
                return MetadataCall::Error(RpcFailure::StaleInputVersion {
                    expected: state.current,
                    got: base,
                });
            }
        }

        MetadataCall::Success(ProgressStream {
            events: prepared.progress.into(),
            next_sequence: 0,
            terminal_seen: false,
            completion: Arc::new(MetadataServerSchemaTransitionCompletion {
                server: self.server.clone(),
                binding: self.binding.clone(),
                base,
                publication: Mutex::new(Some(prepared.publication)),
            }),
        })
    }
}

impl MetadataSnapshot {
    pub fn basis(&self) -> MetadataBasis {
        self.basis
    }

    pub fn version(&self) -> MetadataCall<InputVersion> {
        let state = self.server.lock();
        if let Some(result) = self.preflight(&state) {
            return result;
        }
        MetadataCall::Success(self.basis.snapshot.version)
    }

    pub fn diagnostics(&self) -> MetadataCall<MetadataDiagnostics> {
        let state = self.server.lock();
        if let Some(result) = self.preflight(&state) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return MetadataCall::LeaseFailure;
        };
        MetadataCall::Success(MetadataDiagnostics {
            stamp: self.basis.snapshot,
            configuration: view.configuration.clone(),
            pipeline: pipeline_diagnostic(&view),
            version_poison: view.version_poison.clone(),
        })
    }

    pub fn query(&self, query: &PureMetadataQuery) -> MetadataNamespaceCall<Vec<AssetUuid>> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return MetadataNamespaceCall::LeaseFailure;
        };
        if query
            .normalized_path_prefix
            .as_ref()
            .is_some_and(|path| !valid_logical_path_prefix(path))
        {
            return MetadataNamespaceCall::Error(RpcFailure::InvalidPath {
                path: query.normalized_path_prefix.clone().unwrap_or_default(),
            });
        }
        MetadataNamespaceCall::Success(query_pure_metadata(&view, query))
    }

    pub fn entry(&self, uuid: AssetUuid) -> MetadataNamespaceCall<PureMetadataEntry> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return MetadataNamespaceCall::LeaseFailure;
        };
        let Some(entry) = view.authoring.get(&uuid) else {
            return MetadataNamespaceCall::Error(RpcFailure::AssetNotFound { uuid });
        };
        MetadataNamespaceCall::Success(pure_metadata_entry(entry))
    }

    pub fn resolve_path(&self, path: &str) -> MetadataNamespaceCall<PathResolveResult> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return MetadataNamespaceCall::LeaseFailure;
        };
        if !valid_logical_path(path) {
            return MetadataNamespaceCall::Error(RpcFailure::InvalidPath {
                path: path.to_owned(),
            });
        }
        let value = match view.paths.get(path) {
            None => PathResolveResult::Missing,
            Some(candidates) if candidates.len() == 1 => {
                PathResolveResult::Resolved(*candidates.first().expect("one candidate"))
            }
            Some(candidates) => PathResolveResult::Failed(PathResolveFailure::Ambiguous {
                candidates: candidates.iter().copied().collect(),
            }),
        };
        MetadataNamespaceCall::Success(value)
    }

    pub fn refresh(&self) -> MetadataCall<MetadataSnapshot> {
        let state = self.server.lock();
        if let Some(result) = self.preflight(&state) {
            return result;
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist")
            .clone();
        let basis = metadata_basis(&self.binding, view.stamp);
        drop(state);
        MetadataCall::Success(Self {
            server: self.server.clone(),
            binding: self.binding.clone(),
            basis,
            lease: self.server.register_view_lease(view, None),
        })
    }

    pub fn expire_lease(&self) {
        self.lease.expire();
    }

    fn preflight<T>(&self, state: &ServerState) -> Option<MetadataCall<T>> {
        if let Some(reason) = metadata_fence(state, &self.binding) {
            return Some(MetadataCall::ReconnectRequired { reason });
        }
        if !self.lease.alive() {
            return Some(MetadataCall::LeaseFailure);
        }
        None
    }

    fn namespace_preflight<T>(&self, state: &ServerState) -> Option<MetadataNamespaceCall<T>> {
        if let Some(reason) = metadata_fence(state, &self.binding) {
            return Some(MetadataNamespaceCall::ReconnectRequired { reason });
        }
        let Some(view) = self.lease.view() else {
            return Some(MetadataNamespaceCall::LeaseFailure);
        };
        view.version_poison
            .clone()
            .map(MetadataNamespaceCall::VersionPoisoned)
    }
}

impl MetadataAuthoringSnapshot {
    pub fn basis(&self) -> MetadataBasis {
        self.basis
    }

    pub fn version(&self) -> MetadataCall<InputVersion> {
        let state = self.server.lock();
        if let Some(result) = self.preflight(&state) {
            return result;
        }
        MetadataCall::Success(self.basis.snapshot.version)
    }

    pub fn query(&self, query: &PureMetadataQuery) -> MetadataNamespaceCall<Vec<AssetUuid>> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return MetadataNamespaceCall::LeaseFailure;
        };
        if query
            .normalized_path_prefix
            .as_ref()
            .is_some_and(|path| !valid_logical_path_prefix(path))
        {
            return MetadataNamespaceCall::Error(RpcFailure::InvalidPath {
                path: query.normalized_path_prefix.clone().unwrap_or_default(),
            });
        }
        MetadataNamespaceCall::Success(query_pure_metadata(&view, query))
    }

    pub fn inspect(&self, uuid: AssetUuid) -> MetadataNamespaceCall<AuthoringInspectResult> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return MetadataNamespaceCall::LeaseFailure;
        };
        MetadataNamespaceCall::Success(inspect_authoring(&view, self.basis.snapshot, uuid))
    }

    pub fn refresh(&self) -> MetadataCall<MetadataAuthoringSnapshot> {
        let state = self.server.lock();
        if let Some(result) = self.preflight(&state) {
            return result;
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist")
            .clone();
        let basis = metadata_basis(&self.binding, view.stamp);
        drop(state);
        MetadataCall::Success(Self {
            server: self.server.clone(),
            binding: self.binding.clone(),
            basis,
            lease: self.server.register_view_lease(view, None),
        })
    }

    pub fn expire_lease(&self) {
        self.lease.expire();
    }

    fn preflight<T>(&self, state: &ServerState) -> Option<MetadataCall<T>> {
        if let Some(reason) = metadata_fence(state, &self.binding) {
            return Some(MetadataCall::ReconnectRequired { reason });
        }
        if !self.lease.alive() {
            return Some(MetadataCall::LeaseFailure);
        }
        None
    }

    fn namespace_preflight<T>(&self, state: &ServerState) -> Option<MetadataNamespaceCall<T>> {
        if let Some(reason) = metadata_fence(state, &self.binding) {
            return Some(MetadataNamespaceCall::ReconnectRequired { reason });
        }
        let Some(view) = self.lease.view() else {
            return Some(MetadataNamespaceCall::LeaseFailure);
        };
        view.version_poison
            .clone()
            .map(MetadataNamespaceCall::VersionPoisoned)
    }
}

impl Hub {
    pub fn connection_id(&self) -> u64 {
        lock_connection(&self.connection).id
    }

    /// Cheap transport gate used before decoding target-bound request
    /// parameters. A stale capability must reconnect even when its payload is
    /// malformed or the requested method is reserved.
    pub fn generation_reconnect(&self) -> Option<ReconnectReason> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        generation_fence(&state, &connection)
    }

    pub fn snapshot(&self) -> RpcResult<Snapshot> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if !connection_lease_alive(&connection) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist")
            .clone();
        let connection_id = connection.id;
        let basis = basis_for(&connection, view.stamp);
        drop(connection);
        drop(state);
        RpcResult::Success(snapshot_from(
            &self.server,
            view,
            connection_id,
            basis,
            self.connection.clone(),
        ))
    }

    /// Pin a tooling-only view. It shares the same immutable store stamp and
    /// complete connection-generation fence as the runtime snapshot, but has
    /// no resolve, fetch, dependency, or pack capability.
    pub fn authoring_snapshot(&self) -> RpcResult<AuthoringSnapshot> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if !connection_lease_alive(&connection) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist")
            .clone();
        let connection_id = connection.id;
        let basis = basis_for(&connection, view.stamp);
        drop(connection);
        drop(state);
        RpcResult::Success(authoring_snapshot_from(
            &self.server,
            view,
            connection_id,
            basis,
            self.connection.clone(),
        ))
    }

    fn authoring_gate<T>(&self, base: InputVersion) -> Option<RpcResult<T>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        authoring_gate(&state, &connection, base).map(AuthoringGate::into_result)
    }

    fn publish_prepared_import_locked(
        state: &mut ServerState,
        prepared: PreparedImportCommit,
    ) -> RpcResult<BundleUuid> {
        match commit_locked(state, prepared.commit) {
            Ok(_) => RpcResult::Success(prepared.bundle),
            Err(error) => authoring_admin_failure(error),
        }
    }

    pub fn write(&self, base: InputVersion, ops: Vec<AuthoringOp>) -> RpcResult<InputVersion> {
        let mut state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(gate) = authoring_gate(&state, &connection, base) {
            return gate.into_result();
        }
        if ops.is_empty() {
            return RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
                detail: "authoring operation batch must not be empty".to_owned(),
            });
        }
        if ops.iter().any(|operation| {
            matches!(operation, AuthoringOp::Set(entry) if entry.local_id.starts_with('$'))
        }) {
            return RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
                detail: "daemon-owned '$settings' and '$record' entries cannot be written directly"
                    .to_owned(),
            });
        }
        drop(connection);
        let backend = Arc::clone(&self.server.authoring_backend);
        match backend.prepare_write(base, &ops) {
            Ok(Some(commit)) => {
                return commit_locked(&mut state, commit)
                    .map(|stamp| RpcResult::Success(stamp.version))
                    .unwrap_or_else(authoring_admin_failure);
            }
            Ok(None) => {}
            Err(error) => return RpcResult::Failure(error),
        }
        let current_view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        let mut next_paths = current_view.paths.clone();
        let mut authoring = Vec::with_capacity(ops.len());
        let mut assets = Vec::with_capacity(ops.len());
        for op in ops {
            let uuid = match &op {
                AuthoringOp::Set(entry) => entry.uuid,
                AuthoringOp::Remove { uuid } => *uuid,
            };
            next_paths.retain(|_, candidates| {
                candidates.remove(&uuid);
                !candidates.is_empty()
            });
            match op {
                AuthoringOp::Set(entry) => {
                    next_paths
                        .entry(entry.normalized_path.clone())
                        .or_default()
                        .insert(entry.uuid);
                    authoring.push(AuthoringMutation::Set(entry));
                    assets.push(AssetMutation::Set {
                        uuid,
                        resolution: StoredResolve::Drifted {
                            input: DriftedInput::Asset(uuid),
                        },
                        delta: AssetDeltaState::Changed,
                    });
                }
                AuthoringOp::Remove { uuid } => {
                    authoring.push(AuthoringMutation::Remove { uuid });
                    assets.push(AssetMutation::Set {
                        uuid,
                        resolution: StoredResolve::Deleted,
                        delta: AssetDeltaState::Deleted,
                    });
                }
            }
        }
        let all_paths = current_view
            .paths
            .keys()
            .chain(next_paths.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let paths = all_paths
            .into_iter()
            .filter_map(
                |path| match (current_view.paths.get(&path), next_paths.get(&path)) {
                    (before, after) if before == after => None,
                    (_, Some(candidates)) => Some(PathMutation::Set {
                        path,
                        candidates: candidates.clone(),
                    }),
                    (_, None) => Some(PathMutation::Remove { path }),
                },
            )
            .collect();
        commit_locked(
            &mut state,
            Commit {
                assets,
                authoring,
                paths,
                ..Commit::default()
            },
        )
        .map(|stamp| RpcResult::Success(stamp.version))
        .unwrap_or_else(authoring_admin_failure)
    }

    pub fn import(&self, base: InputVersion, request: ImportRequest) -> RpcResult<BundleUuid> {
        if let Err(detail) = validate_import_request(&request) {
            return RpcResult::Failure(RpcFailure::InvalidAuthoringRequest { detail });
        }
        let backend = Arc::clone(&self.server.authoring_backend);
        let mut state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(gate) = authoring_gate(&state, &connection, base) {
            return gate.into_result();
        }
        drop(connection);
        let prepared = match backend.prepare_import(base, &request) {
            Ok(prepared) => prepared,
            Err(error) => return RpcResult::Failure(error),
        };
        Self::publish_prepared_import_locked(&mut state, prepared)
    }

    pub fn reimport(&self, base: InputVersion, bundle: BundleUuid) -> RpcResult<BundleUuid> {
        let backend = Arc::clone(&self.server.authoring_backend);
        let mut state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(gate) = authoring_gate(&state, &connection, base) {
            return gate.into_result();
        }
        drop(connection);
        let prepared = match backend.prepare_reimport(base, bundle) {
            Ok(prepared) => prepared,
            Err(error) => return RpcResult::Failure(error),
        };
        if prepared.bundle != bundle {
            return RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
                detail: "reimport backend changed the bundle identity".to_owned(),
            });
        }
        Self::publish_prepared_import_locked(&mut state, prepared)
    }

    pub fn operation(
        &self,
        base: InputVersion,
        operation: LongRunningOp,
    ) -> RpcResult<ProgressStream> {
        if let Some(result) = self.authoring_gate(base) {
            return result;
        }
        let prepared = match self
            .server
            .authoring_backend
            .prepare_operation(base, &operation)
        {
            Ok(prepared) => prepared,
            Err(error) => return RpcResult::Failure(error),
        };
        if let Err(detail) = validate_progress(&prepared.progress) {
            return RpcResult::Failure(RpcFailure::InvalidAuthoringRequest { detail });
        }
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(gate) = authoring_gate(&state, &connection, base) {
            return gate.into_result();
        }
        drop(connection);
        drop(state);
        RpcResult::Success(ProgressStream {
            events: prepared.progress.into(),
            next_sequence: 0,
            terminal_seen: false,
            completion: Arc::new(ServerOperationCompletion {
                server: self.server.clone(),
                connection: self.connection.clone(),
                base,
                publication: Mutex::new(Some(prepared.publication)),
            }),
        })
    }

    pub fn wire_tree(&self, hash: LayoutHash) -> RpcResult<Arc<[u8]>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if !connection_lease_alive(&connection) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        if !state.wire_trees.contains_key(&hash) {
            return RpcResult::Failure(RpcFailure::WireTreeNotFound { hash });
        }
        drop(connection);
        drop(state);
        match load_wire_tree(&self.server, hash) {
            Ok(bytes) => RpcResult::Success(bytes),
            Err(error) => RpcResult::Failure(error),
        }
    }

    pub fn fetch(
        &self,
        snapshot: &Snapshot,
        hash: ContentHash,
    ) -> RpcResult<TerminalEvent<ChunkStream>> {
        {
            let state = self.server.lock();
            let connection = lock_connection(&self.connection);
            if !connection_lease_alive(&connection) {
                return RpcResult::Failure(RpcFailure::LeaseExpired);
            }
            if let Some(reason) = generation_fence(&state, &connection) {
                return RpcResult::ReconnectRequired { reason };
            }
        }
        if !Arc::ptr_eq(&snapshot.server.inner, &self.server.inner)
            || !Arc::ptr_eq(&snapshot.connection, &self.connection)
        {
            return RpcResult::Failure(RpcFailure::ForeignSnapshot);
        }
        snapshot.fetch(hash)
    }

    pub fn subscribe(
        &self,
        since: InputVersion,
        assets: Vec<AssetUuid>,
        paths: Vec<String>,
    ) -> RpcResult<SubscriptionInstall> {
        let state = self.server.lock();
        let mut connection = lock_connection(&self.connection);
        if !connection_lease_alive(&connection) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        if since > state.current {
            return RpcResult::Failure(RpcFailure::InvalidCursor {
                since,
                current: state.current,
            });
        }
        if let Some(path) = paths.iter().find(|path| !valid_logical_path(path)) {
            return RpcResult::Failure(RpcFailure::InvalidPath { path: path.clone() });
        }

        let requested_assets: BTreeSet<_> = assets.into_iter().collect();
        let requested_paths: BTreeSet<_> = paths.into_iter().collect();
        let new_assets: BTreeSet<_> = requested_assets
            .difference(&connection.subscribed_assets)
            .copied()
            .collect();
        let new_paths: BTreeSet<_> = requested_paths
            .difference(&connection.subscribed_paths)
            .cloned()
            .collect();
        if connection
            .subscribed_assets
            .len()
            .checked_add(new_assets.len())
            .is_none_or(|count| count > MAX_SUBSCRIBED_ASSETS)
        {
            return RpcResult::Failure(RpcFailure::ResourceLimit {
                resource: "subscribed assets".to_owned(),
                limit: MAX_SUBSCRIBED_ASSETS,
            });
        }
        if connection
            .subscribed_paths
            .len()
            .checked_add(new_paths.len())
            .is_none_or(|count| count > MAX_SUBSCRIBED_PATHS)
        {
            return RpcResult::Failure(RpcFailure::ResourceLimit {
                resource: "subscribed paths".to_owned(),
                limit: MAX_SUBSCRIBED_PATHS,
            });
        }
        connection.subscribed_assets.extend(requested_assets);
        connection.subscribed_paths.extend(requested_paths);

        let installed = state.current;
        let installed_stamp = stamp(&state);
        let basis = basis_for(&connection, installed_stamp);
        let first_install = !connection.stream_installed;
        if since < state.oldest_available_cursor {
            enqueue_event(
                &mut connection,
                StreamEvent::ResyncRequired {
                    basis,
                    oldest_available: state.oldest_available_cursor,
                },
            );
        } else if first_install {
            let deltas = state
                .history
                .iter()
                .filter(|delta| delta.stamp.version > since && delta.stamp.version <= installed)
                .filter_map(|delta| filtered_delta(delta, &new_assets, &new_paths, &connection))
                .collect();
            enqueue_event(
                &mut connection,
                StreamEvent::InitialDelta {
                    basis,
                    since,
                    installed,
                    deltas,
                },
            );
        } else {
            let deltas = state
                .history
                .iter()
                .filter(|delta| delta.stamp.version > since && delta.stamp.version <= installed)
                .filter_map(|delta| filtered_delta(delta, &new_assets, &new_paths, &connection))
                .collect::<Vec<_>>();
            for delta in deltas {
                enqueue_event(&mut connection, StreamEvent::Delta(delta));
            }
        }
        connection.stream_installed = true;
        if first_install && !state.restart_required_keys.is_empty() {
            let restart_basis = basis_for(&connection, installed_stamp);
            enqueue_event(
                &mut connection,
                StreamEvent::Asset {
                    basis: restart_basis,
                    event: AssetEvent::RestartRequired {
                        keys: state.restart_required_keys.iter().cloned().collect(),
                    },
                },
            );
        }
        RpcResult::Success(SubscriptionInstall {
            deltas: DeltaStream {
                connection: self.connection.clone(),
            },
            installed,
        })
    }

    pub fn unsubscribe(&self, assets: Vec<AssetUuid>, paths: Vec<String>) -> RpcResult<()> {
        let state = self.server.lock();
        let mut connection = lock_connection(&self.connection);
        if !connection_lease_alive(&connection) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        if let Some(path) = paths.iter().find(|path| !valid_logical_path(path)) {
            return RpcResult::Failure(RpcFailure::InvalidPath { path: path.clone() });
        }
        for uuid in assets {
            connection.subscribed_assets.remove(&uuid);
        }
        for path in paths {
            connection.subscribed_paths.remove(&path);
        }
        RpcResult::Success(())
    }
}

impl Snapshot {
    pub fn stamp(&self) -> SnapshotStamp {
        self.basis.snapshot
    }

    pub fn generation_reconnect(&self) -> Option<ReconnectReason> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        generation_fence(&state, &connection)
    }

    pub fn version(&self) -> RpcResult<InputVersion> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        RpcResult::Success(self.basis.snapshot.version)
    }

    pub fn configuration(&self) -> RpcResult<ConfigurationStatus> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        };
        RpcResult::Success(view.configuration.clone())
    }

    pub fn basis(&self) -> &RpcBasis {
        &self.basis
    }

    pub fn expire_lease(&self) {
        self.lease.expire();
    }

    /// Open the sole renewable pack-build scope for this snapshot. The scope
    /// remains bounded by the snapshot/connection policy and owns a distinct
    /// durable pin holder that is released on drop or lease expiry.
    pub fn open_pack_session(&self) -> RpcResult<PackSession> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        let mut slot = self
            .lease
            .pack_session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_some() {
            return RpcResult::Failure(RpcFailure::ResourceLimit {
                resource: "pack sessions per snapshot".to_owned(),
                limit: 1,
            });
        }
        let session_state = Arc::new(PackSessionLease {
            holder: self.server.next_lease_id.fetch_add(1, Ordering::Relaxed),
            backend: self
                .server
                .lease_backend
                .read()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone(),
            alive: Mutex::new(true),
        });
        *slot = Some(Arc::clone(&session_state));
        drop(slot);
        drop(connection);
        drop(state);

        let session = PackSession {
            server: self.server.clone(),
            connection: Arc::clone(&self.connection),
            lease: Arc::clone(&self.lease),
            state: session_state,
        };
        match session.keep_alive() {
            RpcResult::Success(()) => RpcResult::Success(session),
            RpcResult::ReconnectRequired { reason } => RpcResult::ReconnectRequired { reason },
            RpcResult::ConfigurationPoisoned(poison) => RpcResult::ConfigurationPoisoned(poison),
            RpcResult::VersionPoisoned(poison) => RpcResult::VersionPoisoned(poison),
            RpcResult::Failure(error) => RpcResult::Failure(error),
        }
    }

    pub fn refresh(&self) -> RpcResult<Snapshot> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if !connection_lease_alive(&connection) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        if !self.lease.alive() {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist")
            .clone();
        let connection_id = connection.id;
        let basis = basis_for(&connection, view.stamp);
        drop(connection);
        drop(state);
        RpcResult::Success(snapshot_from(
            &self.server,
            view,
            connection_id,
            basis,
            self.connection.clone(),
        ))
    }

    pub fn query(&self, query: AssetQuery) -> RpcResult<Vec<AssetUuid>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        };
        if let Some(poison) = &view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        if query.terminal_type.is_some() {
            if let Some(error) = pipeline_failure(&view) {
                return RpcResult::Failure(error);
            }
        }
        if let Err(detail) = validate_asset_query(&query, false) {
            return RpcResult::Failure(RpcFailure::InvalidQuery { detail });
        }
        if let Some(error) = tag_query_poison(&view, &query, AuthoringEntryRole::Runtime) {
            return RpcResult::Failure(error);
        }
        RpcResult::Success(query_asset_entries(
            &view,
            &query,
            AuthoringEntryRole::Runtime,
        ))
    }

    pub fn entry(&self, uuid: AssetUuid) -> RpcResult<MetadataEntry> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        };
        if let Some(poison) = &view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        if let Some(error) = pipeline_failure(&view) {
            return RpcResult::Failure(error);
        }
        let Some(entry) = view.authoring.get(&uuid) else {
            return RpcResult::Failure(RpcFailure::AssetNotFound { uuid });
        };
        if entry.role != AuthoringEntryRole::Runtime {
            return RpcResult::Failure(RpcFailure::AssetNotFound { uuid });
        }
        RpcResult::Success(metadata_entry(entry))
    }

    /// Read the non-hashed runtime policy for one terminal type from the same
    /// target/pipeline epoch as this snapshot. The second fence prevents a
    /// concurrent epoch replacement from publishing a stale policy answer.
    pub fn runtime_type_policy(&self, type_uuid: TypeUuid) -> RpcResult<RuntimeTypePolicy> {
        let (request, backend) = {
            let state = self.server.lock();
            let connection = lock_connection(&self.connection);
            if let Some(result) = self.preflight(&state, &connection) {
                return result;
            }
            let target = state
                .targets
                .get(&connection.target)
                .expect("a generation-fenced connection has a target");
            (
                RuntimeTypePolicyRequest {
                    basis: self.basis.snapshot,
                    target: connection.target.clone(),
                    target_definition: target.definition.definition_hash(),
                    type_uuid,
                },
                self.server
                    .build_backend
                    .read()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clone(),
            )
        };
        let policy = match backend.runtime_type_policy(&request) {
            Ok(policy) => policy,
            Err(error) => return RpcResult::Failure(error),
        };
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        RpcResult::Success(policy)
    }

    pub fn resolve(&self, uuid: AssetUuid) -> RpcResult<TerminalEvent<ResolveResult>> {
        self.resolve_with_work_class(uuid, BuildWorkClass::Interactive)
    }

    /// Resolve work initiated by an offline pack/doctor traversal. The result
    /// is identical to [`Self::resolve`]; only scheduler admission differs.
    pub fn resolve_batch(&self, uuid: AssetUuid) -> RpcResult<TerminalEvent<ResolveResult>> {
        self.resolve_with_work_class(uuid, BuildWorkClass::Batch)
    }

    fn resolve_with_work_class(
        &self,
        uuid: AssetUuid,
        work_class: BuildWorkClass,
    ) -> RpcResult<TerminalEvent<ResolveResult>> {
        loop {
            let state = self.server.lock();
            let connection = lock_connection(&self.connection);
            if let Some(result) = self.preflight(&state, &connection) {
                return result;
            }
            let Some(view) = self.lease.view() else {
                return RpcResult::Failure(RpcFailure::LeaseExpired);
            };
            if let Some(poison) = &view.version_poison {
                return RpcResult::VersionPoisoned(poison.clone());
            }
            if let Some(error) = pipeline_failure(&view) {
                return RpcResult::Failure(error);
            }
            if let ConfigurationStatus::Poisoned(poison) = &view.configuration {
                return RpcResult::ConfigurationPoisoned(poison.clone());
            }
            let derived = view.derived_outputs.get(&uuid).cloned();
            let authoring_uuid = derived.as_ref().map_or(uuid, |output| output.parent);
            if view
                .authoring
                .get(&authoring_uuid)
                .is_some_and(|entry| entry.role == AuthoringEntryRole::AuthoringOnly)
            {
                return RpcResult::Success(TerminalEvent {
                    basis: self.basis.clone(),
                    value: ResolveResult::RoleIneligible {
                        observed: AuthoringEntryRole::AuthoringOnly,
                    },
                });
            }
            let runtime_entry = view
                .authoring
                .get(&authoring_uuid)
                .filter(|entry| entry.role == AuthoringEntryRole::Runtime)
                .cloned();
            let output_key = derived
                .as_ref()
                .map_or_else(String::new, |output| output.output_key.clone());
            let key = BuildKey {
                basis: self.basis.snapshot,
                target: connection.target.clone(),
                asset: uuid,
            };
            let build_resolution = state.build_results.get(&key).cloned();
            let version_resolution = derived.as_ref().map_or_else(
                || view.assets.get(&uuid).cloned(),
                |output| {
                    Some(VersionResolve::Drifted {
                        input: DriftedInput::Asset(output.parent),
                    })
                },
            );

            if build_resolution.is_none() {
                if let (Some(VersionResolve::Drifted { input }), Some(entry)) =
                    (&version_resolution, runtime_entry.clone())
                {
                    let target_definition = state
                        .targets
                        .get(&connection.target)
                        .expect("generation preflight guarantees the connected target")
                        .definition
                        .definition_hash();
                    let request = BuildRequest {
                        work_class,
                        basis: self.basis.snapshot,
                        target: connection.target.clone(),
                        target_definition,
                        requested_asset: uuid,
                        output_key: output_key.clone(),
                        requested_terminal_type: derived
                            .as_ref()
                            .map_or(entry.terminal_type, |output| output.terminal_type),
                        entry,
                        drifted_input: input.clone(),
                    };
                    drop(connection);
                    drop(state);

                    let backend = self
                        .server
                        .build_backend
                        .read()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .clone();
                    let mut outcome = backend.build(&request).and_then(|outcome| match outcome {
                        BuildBackendOutcome::Built(publication) => self
                            .server
                            .install_build_publication(uuid, publication)
                            .map(BuildResolution::Built),
                        BuildBackendOutcome::Failed { error } => Ok(BuildResolution::Failed(error)),
                        BuildBackendOutcome::Drifted { input } => {
                            Ok(BuildResolution::Drifted(input))
                        }
                    });
                    if let Ok(BuildResolution::Built(content_hash)) = &outcome {
                        let state = self.server.lock();
                        outcome = match state.artifacts.get(content_hash) {
                            Some(metadata) => pin_artifact(&self.lease, metadata, *content_hash)
                                .map(|()| BuildResolution::Built(*content_hash)),
                            None => Err(RpcFailure::ArtifactNotFound {
                                hash: *content_hash,
                            }),
                        };
                    }
                    if let Err(error) = backend.build_finished(&request) {
                        outcome = Err(error);
                    }
                    let mut state = self.server.lock();
                    match self.lease.view() {
                        Some(view) => {
                            if let Some(error) = pipeline_failure(&view) {
                                outcome = Err(error);
                            }
                        }
                        None => outcome = Err(RpcFailure::LeaseExpired),
                    }
                    if let Ok(BuildResolution::Built(content_hash)) = &outcome {
                        outcome = match state.artifacts.get(content_hash) {
                            Some(metadata) => pin_artifact(&self.lease, metadata, *content_hash)
                                .map(|()| BuildResolution::Built(*content_hash)),
                            None => Err(RpcFailure::ArtifactNotFound {
                                hash: *content_hash,
                            }),
                        };
                    }
                    if key.basis == stamp(&state) {
                        if let Ok(resolution) = &outcome {
                            state.build_results.insert(key, resolution.clone());
                        }
                    }
                    drop(state);
                    if let Err(error) = outcome {
                        return RpcResult::Failure(error);
                    }
                    continue;
                }
            }

            let resolution = build_resolution.map_or(version_resolution, |built| {
                Some(match built {
                    BuildResolution::Built(content_hash) => VersionResolve::Built { content_hash },
                    BuildResolution::Failed(error) => VersionResolve::Failed { error },
                    BuildResolution::Drifted(input) => VersionResolve::Drifted { input },
                })
            });
            let value = match resolution {
                Some(VersionResolve::Built { content_hash }) => {
                    let Some(metadata) = state.artifacts.get(&content_hash) else {
                        return RpcResult::Failure(RpcFailure::ArtifactNotFound {
                            hash: content_hash,
                        });
                    };
                    if let Err(error) = pin_artifact(&self.lease, metadata, content_hash) {
                        return RpcResult::Failure(error);
                    }
                    ResolveResult::Built { content_hash }
                }
                Some(VersionResolve::Drifted { input }) => ResolveResult::Drifted {
                    input,
                    current: stamp(&state),
                },
                Some(VersionResolve::Failed { error }) => ResolveResult::Failed { error },
                Some(VersionResolve::Deleted { at }) => ResolveResult::Deleted { at },
                None => ResolveResult::Missing,
            };
            return RpcResult::Success(TerminalEvent {
                basis: self.basis.clone(),
                value,
            });
        }
    }

    pub fn resolve_path(&self, path: &str) -> RpcResult<TerminalEvent<PathResolveResult>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        };
        if let Some(poison) = &view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        if !valid_logical_path(path) {
            return RpcResult::Failure(RpcFailure::InvalidPath {
                path: path.to_owned(),
            });
        }
        let value = match view.paths.get(path) {
            None => PathResolveResult::Missing,
            Some(candidates) if candidates.len() == 1 => {
                let uuid = *candidates.first().expect("one candidate");
                PathResolveResult::Resolved(uuid)
            }
            Some(candidates) => PathResolveResult::Failed(PathResolveFailure::Ambiguous {
                candidates: candidates.iter().copied().collect(),
            }),
        };
        RpcResult::Success(TerminalEvent {
            basis: self.basis.clone(),
            value,
        })
    }

    pub fn fetch(&self, hash: ContentHash) -> RpcResult<TerminalEvent<ChunkStream>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        let metadata = match state.artifacts.get(&hash).cloned() {
            Some(payload) => payload,
            None => return RpcResult::Failure(RpcFailure::ArtifactNotFound { hash }),
        };
        if let Err(error) = pin_artifact(&self.lease, &metadata, hash) {
            return RpcResult::Failure(error);
        }
        let chunk_size = state.chunk_size;
        drop(connection);
        drop(state);
        match load_artifact_payload(&self.server, hash, &metadata) {
            Ok(payload) => RpcResult::Success(TerminalEvent {
                basis: self.basis.clone(),
                value: chunk_payload(&payload, chunk_size),
            }),
            Err(error) => RpcResult::Failure(error),
        }
    }

    fn preflight<T>(
        &self,
        state: &ServerState,
        connection: &ConnectionState,
    ) -> Option<RpcResult<T>> {
        if let Some(reason) = generation_fence(state, connection) {
            return Some(RpcResult::ReconnectRequired { reason });
        }
        if !connection_lease_alive(connection) {
            return Some(RpcResult::Failure(RpcFailure::LeaseExpired));
        }
        if !self.lease.alive() {
            return Some(RpcResult::Failure(RpcFailure::LeaseExpired));
        }
        None
    }
}

impl PackSession {
    /// Extend the fixed snapshot and connection deadlines. Renewal never
    /// revives an expired capability and preserves every generation fence.
    pub fn keep_alive(&self) -> RpcResult<()> {
        let policy = *self
            .server
            .lease_policy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = Instant::now()
            .checked_add(policy.ttl)
            .expect("validated RPC lease TTL must fit in Instant");
        let state = self.server.lock();
        let mut connection = lock_connection(&self.connection);
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        if !connection_lease_alive(&connection) || !self.lease.renew(deadline) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        connection.deadline = deadline;
        let connection_active = Arc::clone(&connection.expiry_active);
        drop(connection);
        drop(state);
        LeaseExpiryScheduler::schedule(
            deadline,
            &self.lease.expiry_active,
            LeaseExpiryTarget::View(Arc::downgrade(&self.lease)),
        );
        LeaseExpiryScheduler::schedule(
            deadline,
            &connection_active,
            LeaseExpiryTarget::Connection(Arc::downgrade(&self.connection)),
        );
        RpcResult::Success(())
    }

    /// Add immutable CAS objects to this scope's PackSession pin holder.
    pub fn pin(&self, hashes: &[[u8; 32]]) -> RpcResult<()> {
        match self.keep_alive() {
            RpcResult::Success(()) => {}
            RpcResult::ReconnectRequired { reason } => {
                return RpcResult::ReconnectRequired { reason };
            }
            RpcResult::ConfigurationPoisoned(poison) => {
                return RpcResult::ConfigurationPoisoned(poison);
            }
            RpcResult::VersionPoisoned(poison) => {
                return RpcResult::VersionPoisoned(poison);
            }
            RpcResult::Failure(error) => return RpcResult::Failure(error),
        }
        match self.state.pin(hashes) {
            Ok(()) => RpcResult::Success(()),
            Err(error) => RpcResult::Failure(error),
        }
    }
}

impl AuthoringSnapshot {
    pub fn generation_reconnect(&self) -> Option<ReconnectReason> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        generation_fence(&state, &connection)
    }

    pub fn stamp(&self) -> SnapshotStamp {
        self.basis.snapshot
    }

    pub fn version(&self) -> RpcResult<InputVersion> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        RpcResult::Success(self.basis.snapshot.version)
    }

    pub fn basis(&self) -> &RpcBasis {
        &self.basis
    }

    pub fn expire_lease(&self) {
        self.lease.expire();
    }

    pub fn query(&self, query: AssetQuery) -> RpcResult<Vec<AssetUuid>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        };
        if let Some(poison) = &view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        if let Err(detail) = validate_asset_query(&query, true) {
            return RpcResult::Failure(RpcFailure::InvalidQuery { detail });
        }
        let role = if query.authoring_only.unwrap_or(false) {
            AuthoringEntryRole::AuthoringOnly
        } else {
            AuthoringEntryRole::Runtime
        };
        if let Some(error) = tag_query_poison(&view, &query, role) {
            return RpcResult::Failure(error);
        }
        let values = view
            .authoring
            .iter()
            .filter(|(uuid, entry)| query_asset_entry_matches(entry, **uuid, &query, role))
            .map(|(uuid, _)| *uuid)
            .collect();
        RpcResult::Success(values)
    }

    pub fn inspect(&self, uuid: AssetUuid) -> RpcResult<AuthoringInspectResult> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        let Some(view) = self.lease.view() else {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        };
        if let Some(poison) = &view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        let Some(entry) = view.authoring.get(&uuid) else {
            if view.assets.contains_key(&uuid) {
                return RpcResult::Success(AuthoringInspectResult::RoleIneligible {
                    observed: AuthoringEntryRole::Runtime,
                });
            }
            return RpcResult::Success(AuthoringInspectResult::Missing);
        };
        RpcResult::Success(AuthoringInspectResult::Inspection(AuthoringInspection {
            stamp: self.basis.snapshot,
            uuid: entry.uuid,
            bundle: entry.bundle,
            local_id: entry.local_id.clone(),
            normalized_path: entry.normalized_path.clone(),
            type_uuid: entry.type_uuid,
            schema_hash: entry.schema_hash,
            logical_schema: entry.logical_schema.clone(),
            role: entry.role,
            value: entry.value.clone(),
        }))
    }

    pub fn refresh(&self) -> RpcResult<AuthoringSnapshot> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if !connection_lease_alive(&connection) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        if !self.lease.alive() {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist")
            .clone();
        let connection_id = connection.id;
        let basis = basis_for(&connection, view.stamp);
        drop(connection);
        drop(state);
        RpcResult::Success(authoring_snapshot_from(
            &self.server,
            view,
            connection_id,
            basis,
            self.connection.clone(),
        ))
    }

    fn preflight<T>(
        &self,
        state: &ServerState,
        connection: &ConnectionState,
    ) -> Option<RpcResult<T>> {
        if let Some(reason) = generation_fence(state, connection) {
            return Some(RpcResult::ReconnectRequired { reason });
        }
        if !connection_lease_alive(connection) {
            return Some(RpcResult::Failure(RpcFailure::LeaseExpired));
        }
        if !self.lease.alive() {
            return Some(RpcResult::Failure(RpcFailure::LeaseExpired));
        }
        None
    }
}

impl DeltaStream {
    pub fn next(&self) -> Option<StreamEvent> {
        let mut connection = lock_connection(&self.connection);
        if !connection_lease_alive(&connection) {
            expire_connection(&mut connection);
            return None;
        }
        connection.queue.pop_front()
    }

    /// Wait for the next stream event without blocking the single-threaded
    /// Cap'n Proto `RpcSystem`. Registration happens while the queue mutex is
    /// held, closing the check/notify lost-wakeup window.
    pub async fn next_async(&self) -> Option<StreamEvent> {
        loop {
            let (notified, deadline) = {
                let mut connection = lock_connection(&self.connection);
                if !connection_lease_alive(&connection) {
                    expire_connection(&mut connection);
                    return None;
                }
                if let Some(event) = connection.queue.pop_front() {
                    return Some(event);
                }
                let mut notified = Box::pin(connection.notify.clone().notified_owned());
                notified.as_mut().enable();
                (
                    notified,
                    tokio::time::Instant::from_std(connection.deadline),
                )
            };
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep_until(deadline) => {
                    expire_connection(&mut lock_connection(&self.connection));
                    return None;
                }
            }
        }
    }
}

fn snapshot_from(
    server: &Server,
    view: Arc<VersionView>,
    connection_id: u64,
    basis: RpcBasis,
    connection_arc: Arc<Mutex<ConnectionState>>,
) -> Snapshot {
    Snapshot {
        server: server.clone(),
        connection: connection_arc,
        connection_id,
        basis,
        lease: server.register_view_lease(
            view,
            Some(Arc::new(ArtifactLease {
                holder: server.next_lease_id.fetch_add(1, Ordering::Relaxed),
                backend: server
                    .lease_backend
                    .read()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clone(),
                alive: Mutex::new(true),
            })),
        ),
    }
}

fn authoring_snapshot_from(
    server: &Server,
    view: Arc<VersionView>,
    connection_id: u64,
    basis: RpcBasis,
    connection_arc: Arc<Mutex<ConnectionState>>,
) -> AuthoringSnapshot {
    AuthoringSnapshot {
        server: server.clone(),
        connection: connection_arc,
        connection_id,
        basis,
        lease: server.register_view_lease(view, None),
    }
}

fn pipeline_diagnostic(view: &VersionView) -> PipelineDiagnostic {
    view.pipeline
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

fn pipeline_failure(view: &VersionView) -> Option<RpcFailure> {
    match pipeline_diagnostic(view) {
        PipelineDiagnostic::Ready => None,
        PipelineDiagnostic::Poisoned(poison) => Some(RpcFailure::PipelineUnavailable(Box::new(
            PipelineUnavailableDiagnostic::PipelinePoison(poison),
        ))),
        PipelineDiagnostic::SchemaAcceptanceRequired(required) => {
            Some(RpcFailure::PipelineUnavailable(Box::new(
                PipelineUnavailableDiagnostic::SchemaAcceptanceRequired(required),
            )))
        }
        PipelineDiagnostic::RetiredTypeReferenced(retired) => {
            Some(RpcFailure::PipelineUnavailable(Box::new(
                PipelineUnavailableDiagnostic::RetiredTypeReferenced(retired),
            )))
        }
    }
}

#[derive(Debug)]
enum AuthoringGate {
    Reconnect(ReconnectReason),
    ConfigurationPoisoned(ConfigurationPoison),
    Failure(RpcFailure),
}

impl AuthoringGate {
    fn into_result<T>(self) -> RpcResult<T> {
        match self {
            Self::Reconnect(reason) => RpcResult::ReconnectRequired { reason },
            Self::ConfigurationPoisoned(poison) => RpcResult::ConfigurationPoisoned(poison),
            Self::Failure(error) => RpcResult::Failure(error),
        }
    }
}

fn authoring_gate(
    state: &ServerState,
    connection: &ConnectionState,
    base: InputVersion,
) -> Option<AuthoringGate> {
    if !connection_lease_alive(connection) {
        return Some(AuthoringGate::Failure(RpcFailure::LeaseExpired));
    }
    if let Some(reason) = generation_fence(state, connection) {
        return Some(AuthoringGate::Reconnect(reason));
    }
    if base != state.current {
        return Some(AuthoringGate::Failure(RpcFailure::StaleInputVersion {
            expected: state.current,
            got: base,
        }));
    }
    let view = state
        .views
        .get(&state.current)
        .expect("current view must exist");
    if let ConfigurationStatus::Poisoned(poison) = &view.configuration {
        return Some(AuthoringGate::ConfigurationPoisoned(poison.clone()));
    }
    if let Some(error) = pipeline_failure(view) {
        return Some(AuthoringGate::Failure(error));
    }
    None
}

fn authoring_admin_failure<T>(error: AdminError) -> RpcResult<T> {
    RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
        detail: format!("authoring backend produced an invalid commit: {error:?}"),
    })
}

fn validate_import_request(request: &ImportRequest) -> Result<(), String> {
    if !valid_identifier(&request.importer) {
        return Err("importer ID is not canonical".to_owned());
    }
    if request.sources.is_empty() || request.sources.iter().any(|path| !valid_logical_path(path)) {
        return Err("import sources must be nonempty canonical logical paths".to_owned());
    }
    if !valid_logical_path(&request.dest) {
        return Err("import destination is not a canonical logical path".to_owned());
    }
    if !request.root.is_empty() && !valid_identifier(&request.root) {
        return Err("import root is not canonical".to_owned());
    }
    Ok(())
}

fn validate_progress(events: &[AuthoringProgressEvent]) -> Result<(), String> {
    if events.is_empty() {
        return Err("long-running operation returned no progress events".to_owned());
    }
    for (index, event) in events.iter().enumerate() {
        if event.sequence != index as u64 {
            return Err("progress event sequences must be contiguous from zero".to_owned());
        }
        if index == 0 && event.state != AuthoringProgressState::Started {
            return Err("progress stream must begin with Started".to_owned());
        }
        if event.state.is_terminal() != (index + 1 == events.len()) {
            return Err("progress stream must have exactly one final terminal event".to_owned());
        }
    }
    Ok(())
}

fn generation_fence(state: &ServerState, connection: &ConnectionState) -> Option<ReconnectReason> {
    if state.instance != connection.store_instance {
        return Some(ReconnectReason::StoreInstanceChanged);
    }
    if state.protocol_epoch != connection.protocol_epoch {
        return Some(ReconnectReason::ProtocolEpochChanged);
    }
    if state.pipeline_generation != connection.pipeline_generation {
        return Some(ReconnectReason::PipelineEpochChanged);
    }
    let Some(target) = state.targets.get(&connection.target) else {
        return Some(ReconnectReason::TargetDefinitionChanged);
    };
    if target.target_generation != connection.target_generation {
        return Some(ReconnectReason::TargetDefinitionChanged);
    }
    None
}

fn metadata_fence(
    state: &ServerState,
    binding: &MetadataBinding,
) -> Option<MetadataReconnectReason> {
    if state.instance != binding.store_instance {
        Some(MetadataReconnectReason::StoreInstanceChanged)
    } else if state.protocol_epoch != binding.protocol_epoch {
        Some(MetadataReconnectReason::ProtocolEpochChanged)
    } else {
        None
    }
}

fn query_pure_metadata(view: &VersionView, query: &PureMetadataQuery) -> Vec<AssetUuid> {
    view.authoring
        .iter()
        .filter(|(uuid, entry)| {
            query.uuid.is_none_or(|wanted| wanted == **uuid)
                && query.bundle.is_none_or(|wanted| wanted == entry.bundle)
                && query
                    .normalized_path_prefix
                    .as_ref()
                    .is_none_or(|prefix| entry.normalized_path.starts_with(prefix))
                && query
                    .authored_type
                    .is_none_or(|wanted| wanted == entry.type_uuid)
                && query.role.is_none_or(|wanted| wanted == entry.role)
        })
        .map(|(uuid, _)| *uuid)
        .collect()
}

fn validate_asset_query(query: &AssetQuery, allow_authoring: bool) -> Result<(), String> {
    let populated = query.uuid.is_some()
        || query.bundle_path.is_some()
        || query.local_id.is_some()
        || query.bundle_uuid.is_some()
        || query.authored_type.is_some()
        || query.terminal_type.is_some()
        || query.tag.is_some()
        || query.path_prefix.is_some()
        || query.path_glob.is_some()
        || query.authoring_only.is_some();
    if !populated {
        return Err("asset query must contain at least one selector".to_owned());
    }
    if query.authoring_only == Some(true) && !allow_authoring {
        return Err("authoringOnly=true is restricted to tooling snapshots".to_owned());
    }
    if query.local_id.is_some() && query.bundle_path.is_none() && query.bundle_uuid.is_none() {
        return Err("bundle-relative local_id query is not closed over a bundle".to_owned());
    }
    if query
        .bundle_path
        .as_deref()
        .is_some_and(|path| !valid_logical_path(path))
    {
        return Err("bundle path is not canonical".to_owned());
    }
    if query
        .path_prefix
        .as_deref()
        .is_some_and(|path| !valid_logical_path_prefix(path))
    {
        return Err("path prefix is not canonical".to_owned());
    }
    if query
        .local_id
        .as_deref()
        .is_some_and(|id| !valid_identifier(id))
    {
        return Err("local id is not canonical".to_owned());
    }
    if let Some(tag) = &query.tag {
        if !valid_identifier(&tag.tag)
            || tag
                .value
                .as_deref()
                .is_some_and(|value| !valid_identifier(value))
        {
            return Err("tag selector is not canonical".to_owned());
        }
    }
    if query
        .path_glob
        .as_deref()
        .is_some_and(|glob| glob.is_empty() || glob.contains('\0') || !glob.nfc().eq(glob.chars()))
    {
        return Err("path glob is not canonical".to_owned());
    }
    Ok(())
}

fn query_asset_entries(
    view: &VersionView,
    query: &AssetQuery,
    role: AuthoringEntryRole,
) -> Vec<AssetUuid> {
    view.authoring
        .iter()
        .filter(|(uuid, entry)| query_asset_entry_matches(entry, **uuid, query, role))
        .map(|(uuid, _)| *uuid)
        .collect()
}

fn tag_query_poison(
    view: &VersionView,
    query: &AssetQuery,
    role: AuthoringEntryRole,
) -> Option<RpcFailure> {
    query.tag.as_ref()?;
    let mut without_tag = query.clone();
    without_tag.tag = None;
    let bundles = view
        .tag_poisons
        .iter()
        .filter_map(|(asset, bundle)| {
            let entry = view.authoring.get(asset)?;
            query_asset_entry_matches(entry, *asset, &without_tag, role).then_some(*bundle)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    (!bundles.is_empty()).then_some(RpcFailure::TagIndexPoisoned { bundles })
}

fn query_asset_entry_matches(
    entry: &AuthoringEntry,
    uuid: AssetUuid,
    query: &AssetQuery,
    role: AuthoringEntryRole,
) -> bool {
    query.uuid.is_none_or(|wanted| wanted == uuid)
        && query
            .bundle_path
            .as_ref()
            .is_none_or(|path| path == &entry.normalized_path)
        && query
            .local_id
            .as_ref()
            .is_none_or(|local_id| local_id == &entry.local_id)
        && query
            .bundle_uuid
            .is_none_or(|bundle| bundle == entry.bundle)
        && query
            .authored_type
            .is_none_or(|type_uuid| type_uuid == entry.type_uuid)
        && query
            .terminal_type
            .is_none_or(|type_uuid| type_uuid == entry.terminal_type)
        && query.tag.as_ref().is_none_or(|tag| {
            entry.tags.get(&tag.tag).is_some_and(|value| {
                tag.value
                    .as_ref()
                    .is_none_or(|wanted| value.as_ref().is_some_and(|actual| actual == wanted))
            })
        })
        && query
            .path_prefix
            .as_ref()
            .is_none_or(|prefix| entry.normalized_path.starts_with(prefix))
        && query
            .path_glob
            .as_ref()
            .is_none_or(|glob| path_glob_matches(glob, &entry.normalized_path))
        && entry.role == role
}

fn metadata_entry(entry: &AuthoringEntry) -> MetadataEntry {
    MetadataEntry {
        uuid: entry.uuid,
        bundle: entry.bundle,
        local_id: entry.local_id.clone(),
        normalized_path: entry.normalized_path.clone(),
        authored_type: entry.type_uuid,
        terminal_type: entry.terminal_type,
        schema_hash: entry.schema_hash,
        role: entry.role,
        tags: entry.tags.clone(),
    }
}

fn pure_metadata_entry(entry: &AuthoringEntry) -> PureMetadataEntry {
    PureMetadataEntry {
        uuid: entry.uuid,
        bundle: entry.bundle,
        local_id: entry.local_id.clone(),
        normalized_path: entry.normalized_path.clone(),
        authored_type: entry.type_uuid,
        schema_hash: entry.schema_hash,
        role: entry.role,
    }
}

fn metadata_basis(binding: &MetadataBinding, snapshot: SnapshotStamp) -> MetadataBasis {
    MetadataBasis {
        snapshot,
        protocol_epoch: binding.protocol_epoch,
    }
}

fn basis_for(_connection: &ConnectionState, snapshot: SnapshotStamp) -> RpcBasis {
    RpcBasis { snapshot }
}

fn inspect_authoring(
    view: &VersionView,
    stamp: SnapshotStamp,
    uuid: AssetUuid,
) -> AuthoringInspectResult {
    let Some(entry) = view.authoring.get(&uuid) else {
        if view.assets.contains_key(&uuid) {
            return AuthoringInspectResult::RoleIneligible {
                observed: AuthoringEntryRole::Runtime,
            };
        }
        return AuthoringInspectResult::Missing;
    };
    AuthoringInspectResult::Inspection(AuthoringInspection {
        stamp,
        uuid: entry.uuid,
        bundle: entry.bundle,
        local_id: entry.local_id.clone(),
        normalized_path: entry.normalized_path.clone(),
        type_uuid: entry.type_uuid,
        schema_hash: entry.schema_hash,
        logical_schema: entry.logical_schema.clone(),
        role: entry.role,
        value: entry.value.clone(),
    })
}

fn stamp(state: &ServerState) -> SnapshotStamp {
    SnapshotStamp {
        instance: state.instance,
        version: state.current,
    }
}

fn advance_empty_version(state: &mut ServerState) {
    let next = InputVersion(
        state
            .current
            .0
            .checked_add(1)
            .expect("input version exhausted"),
    );
    let next_stamp = SnapshotStamp {
        instance: state.instance,
        version: next,
    };
    let mut view = (**state
        .views
        .get(&state.current)
        .expect("current view must exist"))
    .clone();
    view.stamp = next_stamp;
    state.current = next;
    state.views.insert(next, Arc::new(view));
    state.views.retain(|version, _| *version == next);
    state.build_results.clear();
    state.history.push_back(HistoryDelta {
        stamp: next_stamp,
        assets: Vec::new(),
        paths: Vec::new(),
    });
    prune_history(state);
}

fn prune_history(state: &mut ServerState) {
    while state.history.len() > MAX_RETAINED_HISTORY_DELTAS {
        let removed = state
            .history
            .pop_front()
            .expect("history is over its bound");
        state.oldest_available_cursor = removed.stamp.version;
    }
}

fn notify_live_delta(state: &mut ServerState, delta: &HistoryDelta) {
    for connection in live_connections(state) {
        let mut connection = lock_connection(&connection);
        if generation_fence(state, &connection).is_some() {
            continue;
        }
        let assets = delta
            .assets
            .iter()
            .filter(|(uuid, _)| connection.subscribed_assets.contains(uuid))
            .copied()
            .collect::<Vec<_>>();
        let paths = delta
            .paths
            .iter()
            .filter(|path| connection.subscribed_paths.contains(*path))
            .cloned()
            .collect::<Vec<_>>();
        if assets.is_empty() && paths.is_empty() {
            continue;
        }
        let basis = basis_for(&connection, delta.stamp);
        enqueue_event(
            &mut connection,
            StreamEvent::Delta(Delta {
                basis,
                assets,
                paths,
            }),
        );
    }
}

fn install_target_set(state: &mut ServerState, replacements: BTreeMap<String, TargetDefinition>) {
    let mut prior = std::mem::take(&mut state.targets);
    let mut installed = BTreeMap::new();
    let mut reconnect = Vec::new();
    for (name, definition) in replacements {
        let runtime = if let Some(mut runtime) = prior.remove(&name) {
            let target_changed =
                runtime.definition.definition_hash() != definition.definition_hash();
            if target_changed {
                runtime.target_generation = runtime
                    .target_generation
                    .checked_add(1)
                    .expect("target generation exhausted");
            }
            runtime.definition = definition;
            if target_changed {
                reconnect.push((name.clone(), ReconnectReason::TargetDefinitionChanged));
            }
            runtime
        } else {
            TargetRuntime {
                definition,
                target_generation: 0,
            }
        };
        installed.insert(name, runtime);
    }
    reconnect.extend(
        prior
            .into_keys()
            .map(|name| (name, ReconnectReason::TargetDefinitionChanged)),
    );
    state.targets = installed;
    for (name, reason) in reconnect {
        notify_reconnect(state, &name, reason);
    }
}

fn notify_reconnect(state: &mut ServerState, target: &str, reason: ReconnectReason) {
    let current = stamp(state);
    for connection in live_connections(state) {
        let mut connection = lock_connection(&connection);
        if connection.target != target {
            continue;
        }
        let basis = basis_for(&connection, current);
        enqueue_event(
            &mut connection,
            StreamEvent::Asset {
                basis,
                event: AssetEvent::ReconnectRequired { reason },
            },
        );
    }
}

fn notify_all_reconnect(state: &mut ServerState, reason: ReconnectReason) {
    let current = stamp(state);
    for connection in live_connections(state) {
        let mut connection = lock_connection(&connection);
        let basis = basis_for(&connection, current);
        enqueue_event(
            &mut connection,
            StreamEvent::Asset {
                basis,
                event: AssetEvent::ReconnectRequired { reason },
            },
        );
    }
}

fn filtered_delta(
    delta: &HistoryDelta,
    assets: &BTreeSet<AssetUuid>,
    paths: &BTreeSet<String>,
    connection: &ConnectionState,
) -> Option<Delta> {
    let filtered_assets = delta
        .assets
        .iter()
        .filter(|(uuid, _)| assets.contains(uuid))
        .copied()
        .collect::<Vec<_>>();
    let filtered_paths = delta
        .paths
        .iter()
        .filter(|path| paths.contains(*path))
        .cloned()
        .collect::<Vec<_>>();
    if filtered_assets.is_empty() && filtered_paths.is_empty() {
        return None;
    }
    Some(Delta {
        basis: basis_for(connection, delta.stamp),
        assets: filtered_assets,
        paths: filtered_paths,
    })
}

fn live_connections(state: &mut ServerState) -> Vec<Arc<Mutex<ConnectionState>>> {
    let mut live = Vec::new();
    state.connections.retain(|weak| {
        if let Some(connection) = weak.upgrade() {
            let mut guard = lock_connection(&connection);
            if !connection_lease_alive(&guard) {
                expire_connection(&mut guard);
                return false;
            }
            drop(guard);
            live.push(connection);
            true
        } else {
            false
        }
    });
    live
}

fn connection_lease_alive(connection: &ConnectionState) -> bool {
    connection.active && Instant::now() < connection.deadline
}

fn expire_connection(connection: &mut ConnectionState) {
    if !std::mem::replace(&mut connection.active, false) {
        return;
    }
    LeaseExpiryScheduler::cancel(&connection.expiry_active);
    connection.subscribed_assets.clear();
    connection.subscribed_paths.clear();
    connection.queue.clear();
    connection.notify.notify_waiters();
}

fn enforce_connection_bound(
    state: &mut ServerState,
    max_connections: usize,
    reserve_for_new_connection: bool,
) {
    let mut live = live_connections(state);
    let reserved = usize::from(reserve_for_new_connection);
    while live.len().saturating_add(reserved) > max_connections {
        if let Some(oldest) = live.first() {
            expire_connection(&mut lock_connection(oldest));
        }
        live.remove(0);
    }
    state.connections.retain(|weak| {
        weak.upgrade()
            .is_some_and(|connection| connection_lease_alive(&lock_connection(&connection)))
    });
}

fn commit_locked(state: &mut ServerState, commit: Commit) -> Result<SnapshotStamp, AdminError> {
    validate_commit(&commit)?;
    let pipeline_epoch_changed = commit.pipeline_epoch_changed
        || commit.pipeline.as_ref().is_some_and(|next| {
            next != &pipeline_diagnostic(
                state
                    .views
                    .get(&state.current)
                    .expect("current view must exist"),
            )
        });
    let next = InputVersion(
        state
            .current
            .0
            .checked_add(1)
            .expect("input version exhausted"),
    );
    let next_stamp = SnapshotStamp {
        instance: state.instance,
        version: next,
    };
    let mut view = (**state
        .views
        .get(&state.current)
        .expect("current view must exist"))
    .clone();
    view.stamp = next_stamp;

    let mut asset_deltas = Vec::with_capacity(commit.assets.len());
    for mutation in commit.assets {
        match mutation {
            AssetMutation::Set {
                uuid,
                resolution,
                delta,
            } => {
                let resolution = match resolution {
                    StoredResolve::Built { content_hash } => VersionResolve::Built { content_hash },
                    StoredResolve::Drifted { input } => VersionResolve::Drifted { input },
                    StoredResolve::Failed { error } => VersionResolve::Failed { error },
                    StoredResolve::Deleted => VersionResolve::Deleted { at: next_stamp },
                };
                view.assets.insert(uuid, resolution);
                asset_deltas.push((uuid, delta));
            }
            AssetMutation::Remove { uuid, delta } => {
                view.assets.remove(&uuid);
                asset_deltas.push((uuid, delta));
            }
        }
    }
    for mutation in commit.authoring {
        match mutation {
            AuthoringMutation::Set(entry) => {
                view.authoring.insert(entry.uuid, entry);
            }
            AuthoringMutation::Remove { uuid } => {
                view.authoring.remove(&uuid);
            }
        }
    }
    if let Some(tag_projection) = commit.tag_projection {
        for (uuid, entry) in &mut view.authoring {
            entry.tags = tag_projection.get(uuid).cloned().unwrap_or_default();
        }
    }
    let mut path_deltas = Vec::with_capacity(commit.paths.len());
    for mutation in commit.paths {
        match mutation {
            PathMutation::Set { path, candidates } => {
                view.paths.insert(path.clone(), candidates);
                path_deltas.push(path);
            }
            PathMutation::Remove { path } => {
                view.paths.remove(&path);
                path_deltas.push(path);
            }
        }
    }
    if let Some(derived_outputs) = commit.derived_outputs {
        view.derived_outputs = derived_outputs;
    }
    for mutation in commit.derived_output_mutations {
        match mutation {
            DerivedOutputMutation::Set { child, entry } => {
                view.derived_outputs.insert(child, entry);
            }
            DerivedOutputMutation::Remove { child } => {
                view.derived_outputs.remove(&child);
            }
        }
    }
    if let Some(tag_poisons) = commit.tag_poisons {
        view.tag_poisons = tag_poisons;
    }
    for mutation in commit.tag_poison_mutations {
        match mutation {
            TagPoisonMutation::Set { asset, bundle } => {
                view.tag_poisons.insert(asset, bundle);
            }
            TagPoisonMutation::Remove { asset } => {
                view.tag_poisons.remove(&asset);
            }
        }
    }
    for mutation in commit.tag_projection_mutations {
        match mutation {
            TagProjectionMutation::Set { asset, tags } => {
                if let Some(entry) = view.authoring.get_mut(&asset) {
                    entry.tags = tags;
                }
            }
            TagProjectionMutation::Remove { asset } => {
                if let Some(entry) = view.authoring.get_mut(&asset) {
                    entry.tags.clear();
                }
            }
        }
    }
    if let Some(configuration) = commit.configuration {
        view.configuration = configuration;
    }
    if let Some(pipeline) = commit.pipeline {
        view.pipeline = Arc::new(RwLock::new(pipeline));
    }
    if let Some(version_poison) = commit.version_poison {
        if let Some(poison) = &version_poison {
            poison
                .validate()
                .map_err(|error| AdminError::InvalidVersionPoison { error })?;
        }
        view.version_poison = version_poison;
    }
    if let Some(lineage_repair) = commit.lineage_repair {
        view.lineage_repair = lineage_repair;
    }
    validate_lineage_repair_configuration(&view.configuration, view.lineage_repair.as_ref())?;
    asset_deltas.sort_by_key(|(uuid, _)| *uuid);
    path_deltas.sort();

    state.current = next;
    state.views.insert(next, Arc::new(view));
    state.views.retain(|version, _| *version == next);
    state.build_results.clear();
    if pipeline_epoch_changed {
        state.pipeline_generation = state
            .pipeline_generation
            .checked_add(1)
            .expect("pipeline generation exhausted");
    }
    let delta = HistoryDelta {
        stamp: next_stamp,
        assets: asset_deltas,
        paths: path_deltas,
    };
    state.history.push_back(delta.clone());
    prune_history(state);
    if pipeline_epoch_changed {
        notify_all_reconnect(state, ReconnectReason::PipelineEpochChanged);
    } else {
        notify_live_delta(state, &delta);
    }
    Ok(next_stamp)
}

fn validate_commit(commit: &Commit) -> Result<(), AdminError> {
    if let Some(ConfigurationStatus::Poisoned(poison)) = &commit.configuration {
        poison
            .validate()
            .map_err(|error| AdminError::InvalidConfigurationPoison { error })?;
    }
    if let Some(pipeline) = &commit.pipeline {
        validate_pipeline_diagnostic(pipeline)?;
    }
    if let Some(Some(repair)) = &commit.lineage_repair {
        validate_lineage_repair_state(repair)?;
    }
    if let Some(derived_outputs) = &commit.derived_outputs {
        for (child, entry) in derived_outputs {
            if *child != AssetUuid::v5(entry.parent, &entry.output_key)
                || entry.output_key.is_empty()
                || !valid_identifier(&entry.output_key)
            {
                return Err(AdminError::InvalidAuthoringIdentity {
                    uuid: *child,
                    detail: "derived output does not match its canonical parent/key identity"
                        .to_owned(),
                });
            }
        }
    }
    let mut derived_children = BTreeSet::new();
    for mutation in &commit.derived_output_mutations {
        let (child, entry) = match mutation {
            DerivedOutputMutation::Set { child, entry } => (*child, Some(entry)),
            DerivedOutputMutation::Remove { child } => (*child, None),
        };
        if !derived_children.insert(child) {
            return Err(AdminError::InvalidAuthoringIdentity {
                uuid: child,
                detail: "duplicate derived-output mutation".to_owned(),
            });
        }
        if entry.is_some_and(|entry| {
            child != AssetUuid::v5(entry.parent, &entry.output_key)
                || entry.output_key.is_empty()
                || !valid_identifier(&entry.output_key)
        }) {
            return Err(AdminError::InvalidAuthoringIdentity {
                uuid: child,
                detail: "derived output does not match its canonical parent/key identity"
                    .to_owned(),
            });
        }
    }
    if let Some(tag_projection) = &commit.tag_projection {
        for (uuid, tags) in tag_projection {
            if tags.iter().any(|(tag, value)| {
                !valid_identifier(tag)
                    || value
                        .as_deref()
                        .is_some_and(|value| !valid_identifier(value))
            }) {
                return Err(AdminError::InvalidAuthoringIdentity {
                    uuid: *uuid,
                    detail: "tag projection contains a noncanonical name or value".to_owned(),
                });
            }
        }
    }
    let mut tag_assets = BTreeSet::new();
    for mutation in &commit.tag_projection_mutations {
        let (asset, tags) = match mutation {
            TagProjectionMutation::Set { asset, tags } => (*asset, Some(tags)),
            TagProjectionMutation::Remove { asset } => (*asset, None),
        };
        if !tag_assets.insert(asset) {
            return Err(AdminError::InvalidAuthoringIdentity {
                uuid: asset,
                detail: "duplicate tag-projection mutation".to_owned(),
            });
        }
        if tags.is_some_and(|tags| {
            tags.iter().any(|(tag, value)| {
                !valid_identifier(tag)
                    || value
                        .as_deref()
                        .is_some_and(|value| !valid_identifier(value))
            })
        }) {
            return Err(AdminError::InvalidAuthoringIdentity {
                uuid: asset,
                detail: "tag projection contains a noncanonical name or value".to_owned(),
            });
        }
    }
    let mut poison_assets = BTreeSet::new();
    for mutation in &commit.tag_poison_mutations {
        let asset = match mutation {
            TagPoisonMutation::Set { asset, .. } | TagPoisonMutation::Remove { asset } => *asset,
        };
        if !poison_assets.insert(asset) {
            return Err(AdminError::InvalidAuthoringIdentity {
                uuid: asset,
                detail: "duplicate tag-poison mutation".to_owned(),
            });
        }
    }
    let mut assets = BTreeSet::new();
    for mutation in &commit.assets {
        let uuid = match mutation {
            AssetMutation::Set { uuid, .. } | AssetMutation::Remove { uuid, .. } => *uuid,
        };
        if !assets.insert(uuid) {
            return Err(AdminError::DuplicateAssetMutation { uuid });
        }
    }
    let mut authoring = BTreeSet::new();
    for mutation in &commit.authoring {
        let uuid = match mutation {
            AuthoringMutation::Set(entry) => {
                if !valid_bundle_local_id(&entry.local_id) {
                    return Err(AdminError::InvalidAuthoringIdentity {
                        uuid: entry.uuid,
                        detail: "local ID is noncanonical or uses the reserved '$' namespace"
                            .to_owned(),
                    });
                }
                if !valid_logical_path(&entry.normalized_path) {
                    return Err(AdminError::InvalidAuthoringIdentity {
                        uuid: entry.uuid,
                        detail: "normalized path is not canonical".to_owned(),
                    });
                }
                if entry.tags.iter().any(|(tag, value)| {
                    !valid_identifier(tag)
                        || value
                            .as_deref()
                            .is_some_and(|value| !valid_identifier(value))
                }) {
                    return Err(AdminError::InvalidAuthoringIdentity {
                        uuid: entry.uuid,
                        detail: "tag name or value is not canonical".to_owned(),
                    });
                }
                validate_authoring_entry(entry).map_err(|error| {
                    AdminError::InvalidAuthoringValue {
                        uuid: entry.uuid,
                        error,
                    }
                })?;
                entry.uuid
            }
            AuthoringMutation::Remove { uuid } => *uuid,
        };
        if !authoring.insert(uuid) {
            return Err(AdminError::DuplicateAuthoringMutation { uuid });
        }
    }
    let mut paths = BTreeSet::new();
    for mutation in &commit.paths {
        let path = match mutation {
            PathMutation::Set { path, .. } | PathMutation::Remove { path } => path,
        };
        if !valid_logical_path(path) {
            return Err(AdminError::InvalidPath { path: path.clone() });
        }
        if !paths.insert(path.clone()) {
            return Err(AdminError::DuplicatePathMutation { path: path.clone() });
        }
        if let PathMutation::Set { candidates, .. } = mutation {
            if candidates.is_empty() {
                return Err(AdminError::EmptyPathCandidates { path: path.clone() });
            }
        }
    }
    Ok(())
}

fn validate_lineage_repair_state(state: &LineageRepairState) -> Result<(), AdminError> {
    let invalid = |detail: &str| AdminError::InvalidLineageRepairState {
        detail: detail.to_owned(),
    };
    match state {
        LineageRepairState::Missing {
            configured_root,
            configured_path,
            ..
        } => {
            if !valid_identifier(configured_root) || !valid_logical_path(configured_path) {
                return Err(invalid("missing-lineage destination is not canonical"));
            }
        }
        LineageRepairState::Duplicate { claimants } => {
            if claimants.len() < 2
                || claimants.windows(2).any(|pair| pair[0] >= pair[1])
                || claimants.iter().any(|claimant| {
                    !valid_identifier(&claimant.root_name)
                        || !valid_logical_path(&claimant.normalized_path)
                        || !valid_reference_local_id(&claimant.local_id)
                })
            {
                return Err(invalid(
                    "duplicate-lineage claimants must be canonical, strict, and contain at least two rows",
                ));
            }
        }
    }
    Ok(())
}

fn validate_lineage_repair_configuration(
    configuration: &ConfigurationStatus,
    repair: Option<&LineageRepairState>,
) -> Result<(), AdminError> {
    let matches = match (configuration, repair) {
        (ConfigurationStatus::Ready, None) => true,
        (
            ConfigurationStatus::Poisoned(ConfigurationPoison { detail, .. }),
            Some(LineageRepairState::Missing { .. }),
        ) => matches!(detail.as_ref(), DscpV1::MissingLineageManifest),
        (
            ConfigurationStatus::Poisoned(ConfigurationPoison { detail, .. }),
            Some(LineageRepairState::Duplicate { claimants }),
        ) => matches!(
            detail.as_ref(),
            DscpV1::DuplicateLineageManifest { entries } if entries == claimants
        ),
        (ConfigurationStatus::Poisoned(poison), None) => !matches!(
            poison.detail.as_ref(),
            DscpV1::MissingLineageManifest | DscpV1::DuplicateLineageManifest { .. }
        ),
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(AdminError::InvalidLineageRepairState {
            detail:
                "repair inspection must exactly match the current missing/duplicate DSCP detail"
                    .to_owned(),
        })
    }
}

fn validate_pipeline_diagnostic(diagnostic: &PipelineDiagnostic) -> Result<(), AdminError> {
    let invalid = |detail: &str| AdminError::InvalidPipelineDiagnostic {
        detail: detail.to_owned(),
    };
    match diagnostic {
        PipelineDiagnostic::Ready => Ok(()),
        PipelineDiagnostic::Poisoned(poison) => poison
            .validate()
            .map_err(|error| invalid(&format!("invalid DSPP record: {error:?}"))),
        PipelineDiagnostic::SchemaAcceptanceRequired(required) => {
            if required.mismatches.is_empty() {
                return Err(invalid("schema mismatch table must not be empty"));
            }
            let mut previous = None;
            for mismatch in &required.mismatches {
                if previous.is_some_and(|uuid| uuid >= mismatch.type_uuid)
                    || distill_core::bootstrap::is_bootstrap_control_type(mismatch.type_uuid)
                    || mismatch.candidate == mismatch.manifest
                {
                    return Err(invalid(
                        "schema mismatches must be strict, non-bootstrap disagreements",
                    ));
                }
                previous = Some(mismatch.type_uuid);
            }
            Ok(())
        }
        PipelineDiagnostic::RetiredTypeReferenced(retired) => {
            if retired.references.is_empty() {
                return Err(invalid("retired reference table must not be empty"));
            }
            let mut previous: Option<Vec<u8>> = None;
            for reference in &retired.references {
                let mut encoded = Vec::with_capacity(33);
                match reference {
                    RetiredTypeReference::Asset(uuid) => {
                        encoded.push(1);
                        encoded.extend_from_slice(&uuid.0);
                    }
                    RetiredTypeReference::MigrationEndpoint(hash) => {
                        encoded.push(2);
                        encoded.extend_from_slice(&hash.0);
                    }
                }
                if previous.as_ref().is_some_and(|prior| prior >= &encoded) {
                    return Err(invalid(
                        "retired references must be strictly canonical and deduplicated",
                    ));
                }
                previous = Some(encoded);
            }
            Ok(())
        }
    }
}

fn validate_authoring_entry(entry: &AuthoringEntry) -> Result<(), AuthoringValueError> {
    decode_authoring_payload(entry.schema_hash, &entry.logical_schema, &entry.value).map(drop)
}

/// Authenticate and decode an RPC authored-value carrier into the exact
/// schema-shaped value stored in a bundle. Blob tokens are interpreted only
/// while walking a `SchemaNode::Blob`, so an ordinary field named
/// `$distill_blob` can never collide with the transport escape.
pub fn decode_authoring_payload(
    schema_hash: LogicalHash,
    logical_schema: &[u8],
    authored_value: &AuthoringValue,
) -> Result<AuthoredValue, AuthoringValueError> {
    let schema_text =
        std::str::from_utf8(logical_schema).map_err(|_| AuthoringValueError::LogicalSchemaUtf8)?;
    let schema = verify_snapshot(schema_text, schema_hash)
        .map_err(|error| AuthoringValueError::LogicalSchemaInvalid(error.to_string()))?;
    let value_text = std::str::from_utf8(&authored_value.canonical_value)
        .map_err(|_| AuthoringValueError::ValueInvalid("value is not UTF-8".to_owned()))?;
    let mut value = distill_json::parse(value_text)
        .map_err(|error| AuthoringValueError::ValueInvalid(error.to_string()))?;
    let rewritten = distill_json::write(&value)
        .map_err(|error| AuthoringValueError::ValueInvalid(error.to_string()))?;
    if rewritten != value_text {
        return Err(AuthoringValueError::ValueNotCanonical);
    }
    let blob_count = u32::try_from(authored_value.blobs.len()).map_err(|_| {
        AuthoringValueError::SchemaValueShape {
            detail: "blob table exceeds UInt32".to_owned(),
        }
    })?;
    let mut used = BTreeSet::new();
    walk_schema_value(&schema.root, &value, &mut Vec::new(), &mut used, blob_count)?;
    for index in 0..blob_count {
        if !used.contains(&index) {
            return Err(AuthoringValueError::UnusedBlobIndex { index });
        }
    }
    materialize_blob_tokens(
        &schema.root,
        &mut value,
        &mut Vec::new(),
        &authored_value.blobs,
    );
    Ok(value)
}

fn materialize_blob_tokens(
    schema: &SchemaNode,
    value: &mut AuthoredValue,
    frames: &mut Vec<SchemaNode>,
    blobs: &[Arc<[u8]>],
) {
    match schema {
        SchemaNode::Blob => {
            let AuthoredValue::Object(object) = value else {
                unreachable!("validated Blob shape")
            };
            let Some(AuthoredValue::UInt(index)) = object.get("$distill_blob") else {
                unreachable!("validated Blob token")
            };
            *value = AuthoredValue::Blob(blobs[*index as usize].to_vec());
        }
        SchemaNode::Struct { fields, .. } => {
            let AuthoredValue::Object(object) = value else {
                unreachable!("validated struct shape")
            };
            frames.push(schema.clone());
            for (name, _, field_schema) in fields {
                materialize_blob_tokens(
                    field_schema,
                    object.get_mut(name).expect("validated struct field"),
                    frames,
                    blobs,
                );
            }
            frames.pop();
        }
        SchemaNode::Enum { variants, .. } => {
            let AuthoredValue::Object(object) = value else {
                unreachable!("validated enum shape")
            };
            let name = object
                .first_key_value()
                .expect("validated enum variant")
                .0
                .clone();
            let variant_schema = &variants
                .iter()
                .find(|(candidate, _, _)| candidate == &name)
                .expect("validated enum schema")
                .2;
            frames.push(schema.clone());
            materialize_blob_tokens(
                variant_schema,
                object.get_mut(&name).expect("validated enum value"),
                frames,
                blobs,
            );
            frames.pop();
        }
        SchemaNode::Vec(element) | SchemaNode::Set(element) => {
            let AuthoredValue::Array(values) = value else {
                unreachable!("validated sequence shape")
            };
            for value in values {
                materialize_blob_tokens(element, value, frames, blobs);
            }
        }
        SchemaNode::Array { elem, .. } => {
            let AuthoredValue::Array(values) = value else {
                unreachable!("validated array shape")
            };
            for value in values {
                materialize_blob_tokens(elem, value, frames, blobs);
            }
        }
        SchemaNode::Option(element) => {
            if !matches!(value, AuthoredValue::Null) {
                materialize_blob_tokens(element, value, frames, blobs);
            }
        }
        SchemaNode::Map { key, value: item } => match value {
            AuthoredValue::Array(entries) => {
                for entry in entries {
                    let AuthoredValue::Array(pair) = entry else {
                        unreachable!("validated map pair")
                    };
                    let (key_value, item_value) = pair.split_at_mut(1);
                    materialize_blob_tokens(key, &mut key_value[0], frames, blobs);
                    materialize_blob_tokens(item, &mut item_value[0], frames, blobs);
                }
            }
            AuthoredValue::Object(entries) => {
                for item_value in entries.values_mut() {
                    materialize_blob_tokens(item, item_value, frames, blobs);
                }
            }
            _ => unreachable!("validated map shape"),
        },
        SchemaNode::BackRef(distance) => {
            let target = frames[frames.len() - (*distance as usize + 1)].clone();
            materialize_blob_tokens(&target, value, frames, blobs);
        }
        SchemaNode::Primitive(_)
        | SchemaNode::AssetRef(_)
        | SchemaNode::WeakRef(_)
        | SchemaNode::String
        | SchemaNode::Unit => {}
    }
}

fn walk_schema_value(
    schema: &SchemaNode,
    value: &AuthoredValue,
    frames: &mut Vec<SchemaNode>,
    used: &mut BTreeSet<u32>,
    blob_count: u32,
) -> Result<(), AuthoringValueError> {
    let shape = |detail: &str| AuthoringValueError::SchemaValueShape {
        detail: detail.to_owned(),
    };
    match schema {
        SchemaNode::Blob => {
            let AuthoredValue::Object(object) = value else {
                return Err(shape("Blob value must be {$distill_blob:u32}"));
            };
            if object.len() != 1 {
                return Err(shape("Blob token must contain exactly one member"));
            }
            let Some(AuthoredValue::UInt(index)) = object.get("$distill_blob") else {
                return Err(shape("Blob token key/value is malformed"));
            };
            let index =
                u32::try_from(*index).map_err(|_| shape("Blob token index does not fit UInt32"))?;
            if index >= blob_count {
                return Err(AuthoringValueError::BlobIndexOutOfRange { index, blob_count });
            }
            if !used.insert(index) {
                return Err(AuthoringValueError::DuplicateBlobIndex { index });
            }
        }
        SchemaNode::Struct { fields, .. } => {
            let AuthoredValue::Object(object) = value else {
                return Err(shape("struct value must be an object"));
            };
            if object.len() != fields.len() {
                return Err(shape("struct value field set does not match schema"));
            }
            frames.push(schema.clone());
            for (name, _, field_schema) in fields {
                let field_value = object
                    .get(name)
                    .ok_or_else(|| shape("struct value is missing a schema field"))?;
                walk_schema_value(field_schema, field_value, frames, used, blob_count)?;
            }
            frames.pop();
        }
        SchemaNode::Enum { variants, .. } => {
            let AuthoredValue::Object(object) = value else {
                return Err(shape("enum value must be a one-member object"));
            };
            if object.len() != 1 {
                return Err(shape("enum value must select exactly one variant"));
            }
            let (name, variant_value) = object.first_key_value().expect("one variant");
            let (_, _, variant_schema) = variants
                .iter()
                .find(|(candidate, _, _)| candidate == name)
                .ok_or_else(|| shape("enum value names an unknown variant"))?;
            frames.push(schema.clone());
            walk_schema_value(variant_schema, variant_value, frames, used, blob_count)?;
            frames.pop();
        }
        SchemaNode::Vec(element) | SchemaNode::Set(element) => {
            let AuthoredValue::Array(values) = value else {
                return Err(shape("sequence value must be an array"));
            };
            for value in values {
                walk_schema_value(element, value, frames, used, blob_count)?;
            }
        }
        SchemaNode::Array { len, elem } => {
            let AuthoredValue::Array(values) = value else {
                return Err(shape("array value must be an array"));
            };
            if usize::try_from(*len).ok() != Some(values.len()) {
                return Err(shape("array value length does not match schema"));
            }
            for value in values {
                walk_schema_value(elem, value, frames, used, blob_count)?;
            }
        }
        SchemaNode::Option(element) => {
            if !matches!(value, AuthoredValue::Null) {
                walk_schema_value(element, value, frames, used, blob_count)?;
            }
        }
        SchemaNode::Map { key, value: item } => match value {
            AuthoredValue::Array(entries) => {
                for entry in entries {
                    let AuthoredValue::Array(pair) = entry else {
                        return Err(shape("map entry must be a key/value pair"));
                    };
                    if pair.len() != 2 {
                        return Err(shape("map entry must contain exactly two values"));
                    }
                    walk_schema_value(key, &pair[0], frames, used, blob_count)?;
                    walk_schema_value(item, &pair[1], frames, used, blob_count)?;
                }
            }
            AuthoredValue::Object(entries) if matches!(key.as_ref(), SchemaNode::String) => {
                for item_value in entries.values() {
                    walk_schema_value(item, item_value, frames, used, blob_count)?;
                }
            }
            _ => return Err(shape("map value does not match its key schema")),
        },
        SchemaNode::BackRef(distance) => {
            let distance = usize::try_from(*distance).expect("u32 fits usize");
            let Some(target) = frames
                .len()
                .checked_sub(distance + 1)
                .and_then(|index| frames.get(index))
                .cloned()
            else {
                return Err(shape("logical schema contains an invalid back-reference"));
            };
            walk_schema_value(&target, value, frames, used, blob_count)?;
        }
        SchemaNode::Unit if !matches!(value, AuthoredValue::Null) => {
            return Err(shape("unit value must be null"));
        }
        SchemaNode::String if !matches!(value, AuthoredValue::Str(_)) => {
            return Err(shape("string value must be a string"));
        }
        SchemaNode::Primitive(kind) => validate_primitive(*kind, value)?,
        SchemaNode::AssetRef(_) | SchemaNode::WeakRef(_) => validate_reference(value)?,
        SchemaNode::String | SchemaNode::Unit => {}
    }
    Ok(())
}

fn validate_primitive(
    kind: PrimitiveKind,
    value: &AuthoredValue,
) -> Result<(), AuthoringValueError> {
    let shape = |detail: &str| AuthoringValueError::SchemaValueShape {
        detail: detail.to_owned(),
    };
    let unsigned = |max: u128| match value {
        AuthoredValue::UInt(value) if *value <= max => Ok(()),
        _ => Err(shape(
            "unsigned integer value is out of range or has the wrong JSON kind",
        )),
    };
    let signed = |min: i128, max: i128| match value {
        AuthoredValue::Int(value) if (min..=max).contains(value) => Ok(()),
        AuthoredValue::UInt(value) if *value <= max as u128 => Ok(()),
        _ => Err(shape(
            "signed integer value is out of range or has the wrong JSON kind",
        )),
    };
    match kind {
        PrimitiveKind::Bool if matches!(value, AuthoredValue::Bool(_)) => Ok(()),
        PrimitiveKind::Bool => Err(shape("bool value must be a JSON boolean")),
        PrimitiveKind::U8 => unsigned(u8::MAX.into()),
        PrimitiveKind::U16 => unsigned(u16::MAX.into()),
        PrimitiveKind::U32 => unsigned(u32::MAX.into()),
        PrimitiveKind::U64 => unsigned(u64::MAX.into()),
        PrimitiveKind::U128 => unsigned(u128::MAX),
        PrimitiveKind::I8 => signed(i8::MIN.into(), i8::MAX.into()),
        PrimitiveKind::I16 => signed(i16::MIN.into(), i16::MAX.into()),
        PrimitiveKind::I32 => signed(i32::MIN.into(), i32::MAX.into()),
        PrimitiveKind::I64 => signed(i64::MIN.into(), i64::MAX.into()),
        PrimitiveKind::I128 => signed(i128::MIN, i128::MAX),
        PrimitiveKind::F32 => match value {
            AuthoredValue::Float(value) if (*value as f32).is_finite() => Ok(()),
            AuthoredValue::Int(value) if (*value as f32).is_finite() => Ok(()),
            AuthoredValue::UInt(value) if (*value as f32).is_finite() => Ok(()),
            _ => Err(shape(
                "f32 value must be a finite representable JSON number",
            )),
        },
        PrimitiveKind::F64
            if matches!(
                value,
                AuthoredValue::Float(_) | AuthoredValue::Int(_) | AuthoredValue::UInt(_)
            ) =>
        {
            Ok(())
        }
        PrimitiveKind::F64 => Err(shape("f64 value must be a JSON number")),
        PrimitiveKind::Char => match value {
            AuthoredValue::Str(value) if value.chars().count() == 1 => Ok(()),
            _ => Err(shape("char value must be a one-scalar string")),
        },
    }
}

fn validate_reference(value: &AuthoredValue) -> Result<(), AuthoringValueError> {
    decode_asset_reference_query(value).map(|_| ())
}

pub fn decode_asset_reference_query(
    value: &AuthoredValue,
) -> Result<AssetReferenceQuery, AuthoringValueError> {
    let invalid = |detail: &str| AuthoringValueError::SchemaValueShape {
        detail: format!("invalid asset reference query: {detail}"),
    };
    match value {
        AuthoredValue::Str(value) => match value.parse::<AssetUuid>() {
            Ok(uuid) if uuid.to_string() == *value => Ok(AssetReferenceQuery::Uuid(uuid)),
            Ok(_) => Err(invalid(
                "UUID spelling must be canonical lowercase RFC 4122",
            )),
            Err(_) if valid_logical_path(value) => Ok(AssetReferenceQuery::Path {
                normalized_path: value.clone(),
                local_id: None,
            }),
            Err(_) => Err(invalid("bare string is neither a canonical UUID nor path")),
        },
        AuthoredValue::Object(fields) => {
            if fields.is_empty()
                || fields.len() > 2
                || fields.keys().any(|key| key != "path" && key != "asset")
            {
                return Err(invalid("object keys must be path and/or asset exactly"));
            }
            let path = match fields.get("path") {
                Some(AuthoredValue::Str(path)) if valid_logical_path(path) => Some(path.clone()),
                Some(_) => return Err(invalid("path must use the canonical logical-path grammar")),
                None => None,
            };
            let local_id = match fields.get("asset") {
                Some(AuthoredValue::Str(asset)) if valid_reference_local_id(asset) => {
                    Some(asset.clone())
                }
                Some(_) => {
                    return Err(invalid(
                        "asset must be a canonical non-reserved bundle-local id",
                    ))
                }
                None => None,
            };
            match (path, local_id) {
                (Some(normalized_path), local_id) => Ok(AssetReferenceQuery::Path {
                    normalized_path,
                    local_id,
                }),
                (None, Some(local_id)) => Ok(AssetReferenceQuery::SameBundleLocalId(local_id)),
                (None, None) => Err(invalid("object must select a path or local id")),
            }
        }
        _ => Err(invalid("reference must be a string or selector object")),
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && !value.contains('\0')
        && value.nfc().eq(value.chars())
}

fn valid_reference_local_id(value: &str) -> bool {
    valid_identifier(value) && !value.starts_with('$')
}

fn valid_bundle_local_id(value: &str) -> bool {
    valid_reference_local_id(value) || matches!(value, "$settings" | "$record")
}

fn valid_logical_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.contains('\0')
        && path.split('/').all(|component| {
            !component.is_empty()
                && component != "."
                && component != ".."
                && component.nfc().eq(component.chars())
        })
}

fn valid_logical_path_prefix(path: &str) -> bool {
    path.is_empty()
        || (!path.starts_with('/')
            && !path.contains('\\')
            && !path.contains('\0')
            && path.split('/').all(|component| {
                !component.is_empty()
                    && component != "."
                    && component != ".."
                    && component.nfc().eq(component.chars())
            }))
}

fn path_glob_matches(pattern: &str, path: &str) -> bool {
    let pattern = pattern.as_bytes();
    let path = path.as_bytes();
    let mut previous = vec![false; path.len() + 1];
    previous[0] = true;
    for token in pattern {
        let mut current = vec![false; path.len() + 1];
        match token {
            b'*' => {
                current[0] = previous[0];
                for index in 1..=path.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            }
            b'?' => {
                current[1..].copy_from_slice(&previous[..path.len()]);
            }
            literal => {
                for index in 1..=path.len() {
                    current[index] = previous[index - 1] && path[index - 1] == *literal;
                }
            }
        }
        previous = current;
    }
    previous[path.len()]
}

fn chunk_payload(payload: &ArtifactPayload, chunk_size: usize) -> ChunkStream {
    let total_bytes = payload
        .blobs
        .iter()
        .try_fold(payload.structural.len() as u64, |total, blob| {
            total.checked_add(blob.len() as u64)
        })
        .expect("one process cannot hold more artifact bytes than u64");
    ChunkStream {
        structural: Arc::clone(&payload.structural),
        blobs: payload.blobs.clone(),
        chunk_size,
        section: 0,
        offset: 0,
        total_bytes,
        load_edges: payload.load_edges.clone(),
    }
}

fn pin_artifact(
    lease: &ViewLease,
    metadata: &StoredArtifact,
    hash: ContentHash,
) -> Result<(), RpcFailure> {
    lease.pin(&[hash.0, metadata.layout_hash.0])
}

fn load_artifact_payload(
    server: &Server,
    hash: ContentHash,
    metadata: &StoredArtifact,
) -> Result<ArtifactPayload, RpcFailure> {
    let backend = server
        .payload_backend
        .read()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    let mut payload = backend
        .load_artifact(hash)
        .map_err(|detail| RpcFailure::AuthoringBackendUnavailable {
            operation: format!("read artifact payload from CAS: {detail}"),
        })?
        .ok_or(RpcFailure::ArtifactNotFound { hash })?;
    if !payload.load_edges.is_empty() && payload.load_edges != metadata.load_edges {
        return Err(RpcFailure::InvalidQuery {
            detail: "artifact payload backend returned inconsistent typed load edges".to_owned(),
        });
    }
    payload.load_edges = metadata.load_edges.clone();
    let blobs = payload.blobs.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    let parsed = distill_wire::artifact::parse_artifact_parts(&payload.structural, &blobs)
        .map_err(|error| RpcFailure::InvalidQuery {
            detail: format!("artifact payload backend returned invalid DSTL: {error}"),
        })?;
    let edge_assets = payload
        .load_edges
        .iter()
        .map(|edge| edge.asset)
        .collect::<Vec<_>>();
    if parsed.content_hash != hash
        || parsed.asset_uuid != metadata.asset_uuid
        || parsed.layout_hash != metadata.layout_hash
        || edge_assets != parsed.load_deps
    {
        return Err(RpcFailure::InvalidQuery {
            detail: "artifact payload backend returned bytes inconsistent with installed metadata"
                .to_owned(),
        });
    }
    Ok(payload)
}

fn load_wire_tree(server: &Server, hash: LayoutHash) -> Result<Arc<[u8]>, RpcFailure> {
    let backend = server
        .payload_backend
        .read()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    let bytes = backend
        .load_wire_tree(hash)
        .map_err(|detail| RpcFailure::AuthoringBackendUnavailable {
            operation: format!("read wire tree from CAS: {detail}"),
        })?
        .ok_or(RpcFailure::WireTreeNotFound { hash })?;
    let wire =
        distill_wire::dswl::decode_dswl(&bytes).map_err(|error| RpcFailure::InvalidQuery {
            detail: format!("payload backend returned invalid DSWL: {error:?}"),
        })?;
    let observed =
        distill_wire::dswl::dswl_hash(&wire).map_err(|error| RpcFailure::InvalidQuery {
            detail: format!("cannot authenticate payload-backend DSWL: {error:?}"),
        })?;
    if observed != hash {
        return Err(RpcFailure::InvalidQuery {
            detail: "payload backend returned a wire tree under the wrong hash".to_owned(),
        });
    }
    Ok(bytes)
}

fn lock_connection(connection: &Arc<Mutex<ConnectionState>>) -> MutexGuard<'_, ConnectionState> {
    connection
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn prune_snapshot_leases(leases: &mut VecDeque<Weak<ViewLease>>) {
    leases.retain(|lease| lease.strong_count() != 0);
}

fn enqueue_event(connection: &mut ConnectionState, event: StreamEvent) {
    if connection.queue.len() >= MAX_PENDING_STREAM_EVENTS {
        let basis = event.basis().clone();
        let terminal = matches!(
            event,
            StreamEvent::Asset {
                event: AssetEvent::ReconnectRequired { .. },
                ..
            } | StreamEvent::ResyncRequired { .. }
        );
        connection.queue.clear();
        if !terminal {
            connection.queue.push_back(StreamEvent::ResyncRequired {
                oldest_available: basis.snapshot.version,
                basis,
            });
            connection.notify.notify_one();
            return;
        }
    }
    connection.queue.push_back(event);
    connection.notify.notify_one();
}

#[cfg(test)]
mod lease_tests {
    use super::*;

    fn connect(server: &Server) -> Hub {
        let request = ConnectRequest::new("dev", TargetDefinitionHash([7; 32]));
        match server.root().connect(request) {
            ConnectOutcome::Connected(connected) => connected.hub,
            other => panic!("expected connection, got {other:?}"),
        }
    }

    #[test]
    fn capability_churn_keeps_deadline_bookkeeping_within_active_bounds() {
        let server = Server::new(
            StoreInstanceId([9; 16]),
            vec![TargetDefinition::new("dev", TargetDefinitionHash([7; 32]))],
        )
        .unwrap();
        server
            .install_lease_policy(LeasePolicy {
                ttl: Duration::from_secs(60 * 60),
                max_snapshot_leases: 1,
                max_connections: 1,
            })
            .unwrap();

        let first_hub = connect(&server);
        let mut snapshots = Vec::new();
        for _ in 0..4096 {
            snapshots.push(match first_hub.snapshot() {
                RpcResult::Success(snapshot) => snapshot,
                other => panic!("expected snapshot, got {other:?}"),
            });
        }
        assert!(
            LeaseExpiryScheduler::entry_count() <= 2,
            "evicted snapshot deadlines accumulated despite a one-lease bound"
        );

        let mut hubs = vec![first_hub];
        for _ in 0..4096 {
            hubs.push(connect(&server));
        }
        assert!(
            LeaseExpiryScheduler::entry_count() <= 2,
            "evicted connection deadlines accumulated despite a one-connection bound"
        );

        assert_eq!(snapshots.len(), 4096);
        assert_eq!(hubs.len(), 4097);
    }
}
