use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, Weak};

use tokio::sync::Notify;
use unicode_normalization::UnicodeNormalization;

use distill_json::AuthoredValue;
use distill_schema::bootstrap_gen_v1::{
    consumer_bootstrap_authority_v1, ConsumerBootstrapAuthorityV1,
};
use distill_schema::ngp_schema::{verify_snapshot, PrimitiveKind, SchemaNode};

use crate::attestation::validate_attestation_shape;
use crate::*;

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Clone)]
pub struct Server {
    inner: Arc<Mutex<ServerState>>,
    authoring_backend: Arc<dyn AuthoringBackend>,
    build_backend: Arc<RwLock<Arc<dyn BuildBackend>>>,
}

struct UnavailableAuthoringBackend;

struct UnavailableBuildBackend;

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
    view: Arc<VersionView>,
    basis: MetadataBasis,
    lease_alive: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct MetadataAuthoringSnapshot {
    server: Server,
    binding: Arc<MetadataBinding>,
    view: Arc<VersionView>,
    basis: MetadataBasis,
    lease_alive: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct Snapshot {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
    connection_id: u64,
    epoch: GameModuleEpoch,
    attestation_generation: u64,
    view: Arc<VersionView>,
    basis: RpcBasis,
    lease_alive: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct AuthoringSnapshot {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
    connection_id: u64,
    epoch: GameModuleEpoch,
    attestation_generation: u64,
    view: Arc<VersionView>,
    basis: RpcBasis,
    lease_alive: Arc<AtomicBool>,
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
            .field("epoch", &self.epoch)
            .field("stamp", &self.basis.snapshot)
            .finish()
    }
}

impl fmt::Debug for AuthoringSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthoringSnapshot")
            .field("connection_id", &self.connection_id)
            .field("epoch", &self.epoch)
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
    bootstrap_authority: &'static ConsumerBootstrapAuthorityV1,
    instance: StoreInstanceId,
    protocol_epoch: u32,
    current: InputVersion,
    views: BTreeMap<InputVersion, Arc<VersionView>>,
    history: VecDeque<HistoryDelta>,
    oldest_available_cursor: InputVersion,
    targets: BTreeMap<String, TargetRuntime>,
    artifacts: HashMap<ContentHash, StoredArtifact>,
    wire_trees: HashMap<LayoutHash, StoredWireTree>,
    build_results: HashMap<BuildKey, BuildResolution>,
    build_flights: HashMap<BuildKey, Arc<BuildFlight>>,
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

struct BuildFlight {
    completed: Mutex<Option<Result<(), RpcFailure>>>,
    wake: Condvar,
}

impl BuildFlight {
    fn new() -> Self {
        Self {
            completed: Mutex::new(None),
            wake: Condvar::new(),
        }
    }

    fn wait(&self) -> Result<(), RpcFailure> {
        let mut completed = self
            .completed
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while completed.is_none() {
            completed = self
                .wake
                .wait(completed)
                .unwrap_or_else(|poison| poison.into_inner());
        }
        completed
            .as_ref()
            .expect("completed build flight has an outcome")
            .clone()
    }

    fn complete(&self, outcome: Result<(), RpcFailure>) {
        *self
            .completed
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(outcome);
        self.wake.notify_all();
    }
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
    payload: ArtifactPayload,
    asset_uuid: AssetUuid,
    layout_hash: LayoutHash,
}

#[derive(Clone, PartialEq, Eq)]
struct StoredWireTree {
    bytes: Arc<[u8]>,
}

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
    /// Full daemon-table generation used only as a reattestation validation /
    /// installation CAS fence. Live Hub generations are projection-sensitive.
    policy_authority_generation: u64,
    compiled_table_generation: u64,
}

struct ConnectionState {
    id: u64,
    target: String,
    target_generation: u64,
    policy_generation: u64,
    attestation_generation: u64,
    store_instance: StoreInstanceId,
    protocol_epoch: u32,
    epoch: GameModuleEpoch,
    load_policy: Arc<LoadPolicyAttestation>,
    accepted_compiled_rows: Vec<CompiledTypeRow>,
    accepted_type_uuids: BTreeSet<TypeUuid>,
    daemon_compiled_projection: CompiledAttestationDigest,
    subscribed_assets: BTreeSet<AssetUuid>,
    subscribed_paths: BTreeSet<String>,
    queue: VecDeque<StreamEvent>,
    stream_installed: bool,
    notify: Arc<Notify>,
}

/// Complete connection fence captured with the attestation validation
/// snapshot and compared again immediately before installation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReattestationFence {
    attestation_generation: u64,
    target_generation: u64,
    policy_generation: u64,
    store_instance: StoreInstanceId,
    protocol_epoch: u32,
    daemon_compiled_table_generation: u64,
    daemon_policy_authority_generation: u64,
    epoch: GameModuleEpoch,
    accepted_compiled_rows: Vec<CompiledTypeRow>,
    load_policy: Arc<LoadPolicyAttestation>,
    daemon_compiled_projection: CompiledAttestationDigest,
}

impl ReattestationFence {
    fn capture(
        connection: &ConnectionState,
        daemon_compiled_table_generation: u64,
        daemon_policy_authority_generation: u64,
    ) -> Self {
        Self {
            attestation_generation: connection.attestation_generation,
            target_generation: connection.target_generation,
            policy_generation: connection.policy_generation,
            store_instance: connection.store_instance,
            protocol_epoch: connection.protocol_epoch,
            daemon_compiled_table_generation,
            daemon_policy_authority_generation,
            epoch: connection.epoch,
            accepted_compiled_rows: connection.accepted_compiled_rows.clone(),
            load_policy: connection.load_policy.clone(),
            daemon_compiled_projection: connection.daemon_compiled_projection,
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
    ) -> Result<Self, AttestationShapeError> {
        Self::new_with_authoring_backend(instance, targets, Arc::new(UnavailableAuthoringBackend))
    }

    pub fn new_with_authoring_backend(
        instance: StoreInstanceId,
        targets: Vec<TargetDefinition>,
        authoring_backend: Arc<dyn AuthoringBackend>,
    ) -> Result<Self, AttestationShapeError> {
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
    ) -> Result<Self, AttestationShapeError> {
        let bootstrap_authority = consumer_bootstrap_authority_v1()
            .map_err(|error| AttestationShapeError::BootstrapAuthorityUnavailable(error.0))?;
        let mut target_map = BTreeMap::new();
        for target in targets {
            validate_attestation_shape(
                target.compiled_registry(),
                target.dsca(),
                target.load_policy(),
                target.policy_digest(),
            )?;
            bootstrap_authority
                .validate_boundary_rows(
                    target.compiled_registry(),
                    distill_core::attestation::BundleFormatVersion::V1,
                )
                .map_err(AttestationShapeError::Bootstrap)?;
            let name = target.name().to_owned();
            if target_map
                .insert(
                    name.clone(),
                    TargetRuntime {
                        definition: target,
                        target_generation: 0,
                        policy_authority_generation: 0,
                        compiled_table_generation: 0,
                    },
                )
                .is_some()
            {
                return Err(AttestationShapeError::DuplicateTarget { target: name });
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
                bootstrap_authority,
                instance,
                protocol_epoch: PROTOCOL_VERSION,
                current: version,
                views,
                history: VecDeque::new(),
                oldest_available_cursor: version,
                targets: target_map,
                artifacts: HashMap::new(),
                wire_trees: HashMap::new(),
                build_results: HashMap::new(),
                build_flights: HashMap::new(),
                connections: Vec::new(),
                next_connection_id: 1,
                chunk_size: DEFAULT_CHUNK_SIZE,
                restart_required_keys: BTreeSet::new(),
            })),
            authoring_backend,
            build_backend: Arc::new(RwLock::new(Arc::new(UnavailableBuildBackend))),
        })
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

        let state = self.lock();
        let view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
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
        if payload.encoded_type != parsed.encoded_type
            || payload.terminal_type != parsed.terminal_type
        {
            return Err(AdminError::InvalidArtifact {
                detail: "payload header types do not match the authenticated DSTL header"
                    .to_owned(),
            });
        }
        let required_runtime_types = validate_served_closure(&payload.closure_rows, true)?;
        let root_index = payload
            .closure_rows
            .binary_search_by_key(&parsed.asset_uuid, |row| row.asset)
            .map_err(|_| AdminError::InvalidArtifact {
                detail: "served closure omits the authenticated artifact root asset".to_owned(),
            })?;
        let root = &payload.closure_rows[root_index];
        if root.content_hash != hash
            || root.authored_type != parsed.authored_type
            || root.encoded_type != parsed.encoded_type
            || root.terminal_type != parsed.terminal_type
        {
            return Err(AdminError::InvalidArtifact {
                detail: "served closure root does not match the authenticated DSTL identity/header"
                    .to_owned(),
            });
        }
        let closure_deps = root
            .load_edges
            .iter()
            .map(|edge| edge.asset)
            .collect::<Vec<_>>();
        if closure_deps != parsed.load_deps {
            return Err(AdminError::InvalidArtifact {
                detail: "served closure root load edges do not match authenticated DSTL load_deps"
                    .to_owned(),
            });
        }
        let header_types = BTreeSet::from([
            parsed.authored_type,
            parsed.encoded_type,
            parsed.terminal_type,
        ]);
        let missing = header_types
            .difference(&required_runtime_types)
            .copied()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(AdminError::IncompleteArtifactTypeCoverage { missing });
        }
        let layout_hash = parsed.layout_hash;
        let asset_uuid = parsed.asset_uuid;
        drop(parsed);
        let mut state = self.lock();
        let artifact = StoredArtifact {
            payload,
            asset_uuid,
            layout_hash,
        };
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
        let mut state = self.lock();
        let tree = StoredWireTree { bytes };
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
        let bootstrap_authority = consumer_bootstrap_authority_v1().map_err(|error| {
            CoordinatedCommitError::Publication(format!(
                "bootstrap authority unavailable: {}",
                error.0
            ))
        })?;
        let mut replacement_map = BTreeMap::new();
        for replacement in replacements {
            validate_attestation_shape(
                replacement.compiled_registry(),
                replacement.dsca(),
                replacement.load_policy(),
                replacement.policy_digest(),
            )
            .map_err(|error| CoordinatedCommitError::Publication(error.to_string()))?;
            bootstrap_authority
                .validate_boundary_rows(
                    replacement.compiled_registry(),
                    distill_core::attestation::BundleFormatVersion::V1,
                )
                .map_err(AttestationShapeError::Bootstrap)
                .map_err(|error| CoordinatedCommitError::Publication(error.to_string()))?;
            let name = replacement.name().to_owned();
            if replacement_map.insert(name.clone(), replacement).is_some() {
                return Err(CoordinatedCommitError::Publication(
                    AttestationShapeError::DuplicateTarget { target: name }.to_string(),
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

    /// Replace a staged target. Any target-definition/layout change advances
    /// the target generation; policy projection changes advance its separate
    /// generation. The input version advances once for the combined commit.
    pub fn replace_target(
        &self,
        replacement: TargetDefinition,
    ) -> Result<SnapshotStamp, AdminError> {
        let mut state = self.lock();
        validate_attestation_shape(
            replacement.compiled_registry(),
            replacement.dsca(),
            replacement.load_policy(),
            replacement.policy_digest(),
        )
        .map_err(AdminError::InvalidTargetAttestation)?;
        state
            .bootstrap_authority
            .validate_boundary_rows(
                replacement.compiled_registry(),
                distill_core::attestation::BundleFormatVersion::V1,
            )
            .map_err(AttestationShapeError::Bootstrap)
            .map_err(AdminError::InvalidTargetAttestation)?;
        let name = replacement.name().to_owned();
        let runtime = state
            .targets
            .get_mut(&name)
            .ok_or_else(|| AdminError::UnknownTarget {
                target: name.clone(),
            })?;
        let target_changed = runtime.definition.definition_hash() != replacement.definition_hash();
        let compiled_changed =
            runtime.definition.compiled_registry() != replacement.compiled_registry();
        let policy_changed = runtime.definition.load_policy() != replacement.load_policy()
            || runtime.definition.policy_digest() != replacement.policy_digest();
        if !target_changed && !policy_changed && !compiled_changed {
            return Ok(stamp(&state));
        }
        if target_changed {
            runtime.target_generation = runtime
                .target_generation
                .checked_add(1)
                .expect("target generation exhausted");
        }
        if policy_changed {
            runtime.policy_authority_generation = runtime
                .policy_authority_generation
                .checked_add(1)
                .expect("policy authority generation exhausted");
        }
        if compiled_changed {
            runtime.compiled_table_generation = runtime
                .compiled_table_generation
                .checked_add(1)
                .expect("compiled table generation exhausted");
        }
        runtime.definition = replacement;

        advance_empty_version(&mut state);
        if target_changed {
            notify_reconnect(&mut state, &name, ReconnectReason::TargetDefinitionChanged);
        } else {
            if policy_changed {
                notify_projection_reconnect(&mut state, &name, ReconnectReason::LoadPolicyChanged);
            }
            if compiled_changed {
                notify_projection_reconnect(
                    &mut state,
                    &name,
                    ReconnectReason::CompiledAttestationChanged,
                );
            }
        }
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
        let runtime = match state.targets.get(&request.target) {
            Some(runtime) => runtime,
            None => {
                return ConnectOutcome::Rejected(ConnectError::UnknownTarget {
                    target: request.target,
                })
            }
        };
        let policy = match validate_complete_attestation(
            &runtime.definition,
            request.target_definition_hash,
            &request.compiled_registry,
            request.dsca,
            &request.load_policy,
            request.policy_digest,
        ) {
            Ok(policy) => policy,
            Err(error) => return ConnectOutcome::Rejected(error),
        };
        let target_generation = runtime.target_generation;
        // Policy generations belong to one Hub's accepted projection. A
        // fresh Hub starts at zero regardless of changes elsewhere in the
        // daemon's full policy table; reattestation advances it only when
        // that Hub's accepted-set projection changes.
        let policy_generation = 0;
        let accepted_type_uuids = request
            .compiled_registry
            .iter()
            .filter(|row| !distill_core::attestation::is_bootstrap_control_type(row.type_uuid))
            .map(|row| row.type_uuid)
            .collect::<BTreeSet<_>>();
        let daemon_compiled_projection = match daemon_projection_digest(
            runtime.definition.compiled_registry(),
            &accepted_type_uuids,
        ) {
            Ok(digest) => digest,
            Err(error) => {
                return ConnectOutcome::Rejected(projection_connect_error(error));
            }
        };
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

        let id = state.next_connection_id;
        state.next_connection_id = state
            .next_connection_id
            .checked_add(1)
            .expect("connection id exhausted");
        let connection = Arc::new(Mutex::new(ConnectionState {
            id,
            target: request.target,
            target_generation,
            policy_generation,
            attestation_generation: 0,
            store_instance: state.instance,
            protocol_epoch: state.protocol_epoch,
            epoch: request.epoch,
            load_policy: policy.clone(),
            accepted_compiled_rows: request.compiled_registry,
            accepted_type_uuids,
            daemon_compiled_projection,
            subscribed_assets: BTreeSet::new(),
            subscribed_paths: BTreeSet::new(),
            queue: VecDeque::new(),
            stream_installed: false,
            notify: Arc::new(Notify::new()),
        }));
        state.connections.push(Arc::downgrade(&connection));
        ConnectOutcome::Connected(Connected {
            hub: Hub {
                server: self.server.clone(),
                connection,
            },
            instance: state.instance,
            policy_generation,
            target_generation,
            attestation_generation: 0,
            load_policy: policy,
            daemon_compiled_projection,
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
        entry.type_uuid == distill_core::attestation::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID
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
    let authority = consumer_bootstrap_authority_v1().map_err(|error| {
        invalid(
            LineageRepairInvalidCode::InvalidLineage,
            &format!("bootstrap authority unavailable: {}", error.0),
        )
    })?;
    let expected = authority
        .rows()
        .iter()
        .find(|row| row.type_uuid == distill_core::attestation::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID)
        .expect("sealed bootstrap table contains lineage manifest");
    if manifest.schema_hash != expected.logical_hash {
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
        .any(|type_uuid| distill_core::attestation::is_bootstrap_control_type(*type_uuid))
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
        MetadataCall::Success(MetadataSnapshot {
            server: self.server.clone(),
            binding: self.binding.clone(),
            basis: metadata_basis(&self.binding, view.stamp),
            view,
            lease_alive: Arc::new(AtomicBool::new(true)),
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
        MetadataCall::Success(MetadataAuthoringSnapshot {
            server: self.server.clone(),
            binding: self.binding.clone(),
            basis: metadata_basis(&self.binding, view.stamp),
            view,
            lease_alive: Arc::new(AtomicBool::new(true)),
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
        let Some(payload) = state.artifacts.get(&hash) else {
            return MetadataCall::Error(RpcFailure::ArtifactNotFound { hash });
        };
        if let Err(error) = verified_artifact_closure(&state, hash) {
            return MetadataCall::Error(error);
        }
        MetadataCall::Success(chunk_payload(&payload.payload, state.chunk_size))
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
        MetadataCall::Success(MetadataDiagnostics {
            stamp: self.basis.snapshot,
            configuration: self.view.configuration.clone(),
            pipeline: pipeline_diagnostic(&self.view),
            version_poison: self.view.version_poison.clone(),
        })
    }

    pub fn query(&self, query: &PureMetadataQuery) -> MetadataNamespaceCall<Vec<AssetUuid>> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        if query
            .normalized_path_prefix
            .as_ref()
            .is_some_and(|path| !valid_logical_path_prefix(path))
        {
            return MetadataNamespaceCall::Error(RpcFailure::InvalidPath {
                path: query.normalized_path_prefix.clone().unwrap_or_default(),
            });
        }
        MetadataNamespaceCall::Success(query_pure_metadata(&self.view, query))
    }

    pub fn entry(&self, uuid: AssetUuid) -> MetadataNamespaceCall<PureMetadataEntry> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        let Some(entry) = self.view.authoring.get(&uuid) else {
            return MetadataNamespaceCall::Error(RpcFailure::AssetNotFound { uuid });
        };
        MetadataNamespaceCall::Success(pure_metadata_entry(entry))
    }

    pub fn resolve_path(&self, path: &str) -> MetadataNamespaceCall<PathResolveResult> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        if !valid_logical_path(path) {
            return MetadataNamespaceCall::Error(RpcFailure::InvalidPath {
                path: path.to_owned(),
            });
        }
        let value = match self.view.paths.get(path) {
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
        MetadataCall::Success(Self {
            server: self.server.clone(),
            binding: self.binding.clone(),
            basis: metadata_basis(&self.binding, view.stamp),
            view,
            lease_alive: Arc::new(AtomicBool::new(true)),
        })
    }

    pub fn expire_lease(&self) {
        self.lease_alive.store(false, Ordering::Release);
    }

    fn preflight<T>(&self, state: &ServerState) -> Option<MetadataCall<T>> {
        if let Some(reason) = metadata_fence(state, &self.binding) {
            return Some(MetadataCall::ReconnectRequired { reason });
        }
        if !self.lease_alive.load(Ordering::Acquire) {
            return Some(MetadataCall::LeaseFailure);
        }
        None
    }

    fn namespace_preflight<T>(&self, state: &ServerState) -> Option<MetadataNamespaceCall<T>> {
        if let Some(reason) = metadata_fence(state, &self.binding) {
            return Some(MetadataNamespaceCall::ReconnectRequired { reason });
        }
        if !self.lease_alive.load(Ordering::Acquire) {
            return Some(MetadataNamespaceCall::LeaseFailure);
        }
        self.view
            .version_poison
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
        if query
            .normalized_path_prefix
            .as_ref()
            .is_some_and(|path| !valid_logical_path_prefix(path))
        {
            return MetadataNamespaceCall::Error(RpcFailure::InvalidPath {
                path: query.normalized_path_prefix.clone().unwrap_or_default(),
            });
        }
        MetadataNamespaceCall::Success(query_pure_metadata(&self.view, query))
    }

    pub fn inspect(&self, uuid: AssetUuid) -> MetadataNamespaceCall<AuthoringInspectResult> {
        let state = self.server.lock();
        if let Some(result) = self.namespace_preflight(&state) {
            return result;
        }
        MetadataNamespaceCall::Success(inspect_authoring(&self.view, self.basis.snapshot, uuid))
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
        MetadataCall::Success(Self {
            server: self.server.clone(),
            binding: self.binding.clone(),
            basis: metadata_basis(&self.binding, view.stamp),
            view,
            lease_alive: Arc::new(AtomicBool::new(true)),
        })
    }

    pub fn expire_lease(&self) {
        self.lease_alive.store(false, Ordering::Release);
    }

    fn preflight<T>(&self, state: &ServerState) -> Option<MetadataCall<T>> {
        if let Some(reason) = metadata_fence(state, &self.binding) {
            return Some(MetadataCall::ReconnectRequired { reason });
        }
        if !self.lease_alive.load(Ordering::Acquire) {
            return Some(MetadataCall::LeaseFailure);
        }
        None
    }

    fn namespace_preflight<T>(&self, state: &ServerState) -> Option<MetadataNamespaceCall<T>> {
        if let Some(reason) = metadata_fence(state, &self.binding) {
            return Some(MetadataNamespaceCall::ReconnectRequired { reason });
        }
        if !self.lease_alive.load(Ordering::Acquire) {
            return Some(MetadataNamespaceCall::LeaseFailure);
        }
        self.view
            .version_poison
            .clone()
            .map(MetadataNamespaceCall::VersionPoisoned)
    }
}

impl Hub {
    pub fn connection_id(&self) -> u64 {
        lock_connection(&self.connection).id
    }

    /// The installed generation, exposed for loader basis rotation and
    /// diagnostics. Mutation is possible only through successful reattest.
    pub fn attestation_generation(&self) -> u64 {
        lock_connection(&self.connection).attestation_generation
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
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        RpcResult::Success(snapshot_from(
            &self.server,
            &state,
            &connection,
            self.connection.clone(),
        ))
    }

    /// Pin a tooling-only view. It shares the same immutable store stamp and
    /// complete connection-generation fence as the runtime snapshot, but has
    /// no resolve, fetch, dependency, or pack capability.
    pub fn authoring_snapshot(&self) -> RpcResult<AuthoringSnapshot> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        RpcResult::Success(authoring_snapshot_from(
            &self.server,
            &state,
            &connection,
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
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        let Some(tree) = state.wire_trees.get(&hash) else {
            return RpcResult::Failure(RpcFailure::WireTreeNotFound { hash });
        };
        let current_view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        let mut referencing_hashes = current_view
            .assets
            .values()
            .filter_map(|resolution| match resolution {
                VersionResolve::Built { content_hash } => state
                    .artifacts
                    .get(content_hash)
                    .is_some_and(|artifact| artifact.layout_hash == hash)
                    .then_some(*content_hash),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        if referencing_hashes.is_empty() {
            let historical = state
                .artifacts
                .iter()
                .filter_map(|(content_hash, artifact)| {
                    (artifact.layout_hash == hash).then_some(*content_hash)
                })
                .collect::<BTreeSet<_>>();
            if historical.is_empty() {
                return RpcResult::Failure(RpcFailure::WireTreeNotFound { hash });
            }
            for content_hash in &historical {
                let (required, _) = match verified_artifact_closure(&state, *content_hash) {
                    Ok(verified) => verified,
                    Err(error) => return RpcResult::Failure(error),
                };
                if required.is_subset(&connection.accepted_type_uuids) {
                    return RpcResult::Success(tree.bytes.clone());
                }
            }
            referencing_hashes.insert(
                *historical
                    .first()
                    .expect("nonempty historical reference set was checked"),
            );
        }
        let mut closure_by_asset = BTreeMap::new();
        for content_hash in referencing_hashes {
            let (_, rows) = match verified_artifact_closure(&state, content_hash) {
                Ok(verified) => verified,
                Err(error) => return RpcResult::Failure(error),
            };
            for row in rows {
                if closure_by_asset
                    .insert(row.asset, row.clone())
                    .is_some_and(|previous| previous != row)
                {
                    return RpcResult::Failure(RpcFailure::InvalidQuery {
                        detail: "current wire-tree references contain conflicting closure rows for one asset"
                            .to_owned(),
                    });
                }
            }
        }
        let closure_rows = closure_by_asset.into_values().collect::<Vec<_>>();
        let required_runtime_types = served_closure_types(&closure_rows);
        if let Some(expansion) = attestation_expansion(
            stamp(&state),
            4,
            &hash.0,
            &required_runtime_types,
            &connection.accepted_type_uuids,
            closure_rows,
        ) {
            return RpcResult::AttestationExpansionRequired(expansion);
        }
        RpcResult::Success(tree.bytes.clone())
    }

    pub fn fetch(
        &self,
        snapshot: &Snapshot,
        hash: ContentHash,
    ) -> RpcResult<TerminalEvent<ChunkStream>> {
        {
            let state = self.server.lock();
            let connection = lock_connection(&self.connection);
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

    /// Cap'n Proto `Hub.fetch` convenience. Loader integrations should prefer
    /// [`Hub::fetch`] with their adopted snapshot so the request basis is
    /// explicit; this method pins latest atomically and returns that basis.
    pub fn fetch_latest(&self, hash: ContentHash) -> RpcResult<TerminalEvent<ChunkStream>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        let payload = match state.artifacts.get(&hash) {
            Some(payload) => payload,
            None => return RpcResult::Failure(RpcFailure::ArtifactNotFound { hash }),
        };
        let (required_runtime_types, closure_rows) = match verified_artifact_closure(&state, hash) {
            Ok(verified) => verified,
            Err(error) => return RpcResult::Failure(error),
        };
        if let Some(expansion) = attestation_expansion(
            stamp(&state),
            3,
            &hash.0,
            &required_runtime_types,
            &connection.accepted_type_uuids,
            closure_rows,
        ) {
            return RpcResult::AttestationExpansionRequired(expansion);
        }
        let current = stamp(&state);
        RpcResult::Success(TerminalEvent {
            basis: basis_for(&connection, current),
            value: chunk_payload(&payload.payload, state.chunk_size),
        })
    }

    pub fn subscribe(
        &self,
        since: InputVersion,
        assets: Vec<AssetUuid>,
        paths: Vec<String>,
    ) -> RpcResult<SubscriptionInstall> {
        let state = self.server.lock();
        let mut connection = lock_connection(&self.connection);
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
        } else {
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

    pub fn reattest(&self, request: ReattestRequest) -> RpcResult<ReattestSuccess> {
        let state = self.server.lock();
        let mut connection = lock_connection(&self.connection);
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        let runtime = state
            .targets
            .get(&connection.target)
            .expect("connected target must exist");
        let validation_fence = ReattestationFence::capture(
            &connection,
            runtime.compiled_table_generation,
            runtime.policy_authority_generation,
        );
        let current_view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        if let ConfigurationStatus::Poisoned(poison) = &current_view.configuration {
            return RpcResult::ConfigurationPoisoned(poison.clone());
        }
        if let Some(error) = pipeline_failure(current_view) {
            return RpcResult::Failure(error);
        }
        let expected_successor = match request.base_attestation_generation.checked_add(1) {
            Some(successor) => successor,
            None => {
                return RpcResult::Failure(RpcFailure::AttestationGenerationOverflow {
                    base: request.base_attestation_generation,
                })
            }
        };
        if request.successor_attestation_generation != expected_successor {
            return RpcResult::Failure(RpcFailure::InvalidAttestationSuccessor {
                base: request.base_attestation_generation,
                successor: request.successor_attestation_generation,
            });
        }
        if request.base_attestation_generation != connection.attestation_generation {
            return RpcResult::Failure(RpcFailure::StaleAttestationBase {
                expected: connection.attestation_generation,
                got: request.base_attestation_generation,
            });
        }
        let proposed_type_uuids = request
            .compiled_registry
            .iter()
            .filter(|row| !distill_core::attestation::is_bootstrap_control_type(row.type_uuid))
            .map(|row| row.type_uuid)
            .collect::<BTreeSet<_>>();
        let same_epoch_expansion = request.epoch == connection.epoch
            && connection
                .accepted_type_uuids
                .is_subset(&proposed_type_uuids)
            && connection.accepted_type_uuids.len() < proposed_type_uuids.len()
            && connection.accepted_compiled_rows.iter().all(|bound| {
                request
                    .compiled_registry
                    .binary_search_by_key(&bound.type_uuid, |row| row.type_uuid)
                    .is_ok_and(|index| request.compiled_registry[index] == *bound)
            });
        if request.epoch < connection.epoch
            || (request.epoch == connection.epoch && !same_epoch_expansion)
        {
            return RpcResult::Failure(RpcFailure::EpochNotSuccessor {
                previous: connection.epoch,
                proposed: request.epoch,
            });
        }
        let policy = match validate_complete_attestation(
            &runtime.definition,
            request.target_definition_hash,
            &request.compiled_registry,
            request.dsca,
            &request.load_policy,
            request.policy_digest,
        ) {
            Ok(policy) => policy,
            Err(error) => return RpcResult::Failure(RpcFailure::Attestation(error)),
        };
        let accepted_type_uuids = proposed_type_uuids;
        let daemon_compiled_projection = match daemon_projection_digest(
            runtime.definition.compiled_registry(),
            &accepted_type_uuids,
        ) {
            Ok(digest) => digest,
            Err(error) => {
                return RpcResult::Failure(RpcFailure::Attestation(projection_connect_error(
                    error,
                )));
            }
        };
        let successor_policy_generation = if *policy != *connection.load_policy {
            match connection.policy_generation.checked_add(1) {
                Some(generation) => generation,
                None => {
                    return RpcResult::Failure(RpcFailure::PolicyGenerationOverflow {
                        base: connection.policy_generation,
                    })
                }
            }
        } else {
            connection.policy_generation
        };

        // Validation and installation currently share the server lock, but
        // the complete tuple is still compared explicitly. This preserves
        // the CAS contract if validation later moves off-lock.
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        let install_runtime = state
            .targets
            .get(&connection.target)
            .expect("connected target must exist");
        let install_fence = ReattestationFence::capture(
            &connection,
            install_runtime.compiled_table_generation,
            install_runtime.policy_authority_generation,
        );
        if install_fence.store_instance != validation_fence.store_instance {
            return RpcResult::ReconnectRequired {
                reason: ReconnectReason::StoreInstanceChanged,
            };
        }
        if install_fence.protocol_epoch != validation_fence.protocol_epoch {
            return RpcResult::ReconnectRequired {
                reason: ReconnectReason::ProtocolEpochChanged,
            };
        }
        if install_fence.target_generation != validation_fence.target_generation {
            return RpcResult::ReconnectRequired {
                reason: ReconnectReason::TargetDefinitionChanged,
            };
        }
        if install_fence.policy_generation != validation_fence.policy_generation {
            return RpcResult::ReconnectRequired {
                reason: ReconnectReason::LoadPolicyChanged,
            };
        }
        if install_fence.daemon_policy_authority_generation
            != validation_fence.daemon_policy_authority_generation
        {
            return RpcResult::ReconnectRequired {
                reason: ReconnectReason::LoadPolicyChanged,
            };
        }
        if install_fence.daemon_compiled_table_generation
            != validation_fence.daemon_compiled_table_generation
        {
            return RpcResult::ReconnectRequired {
                reason: ReconnectReason::CompiledAttestationChanged,
            };
        }
        if install_fence.attestation_generation != validation_fence.attestation_generation {
            return RpcResult::Failure(RpcFailure::StaleAttestationBase {
                expected: install_fence.attestation_generation,
                got: request.base_attestation_generation,
            });
        }
        if install_fence.epoch != validation_fence.epoch
            || install_fence.accepted_compiled_rows != validation_fence.accepted_compiled_rows
            || install_fence.load_policy != validation_fence.load_policy
            || install_fence.daemon_compiled_projection
                != validation_fence.daemon_compiled_projection
        {
            return RpcResult::Failure(RpcFailure::StaleAttestationBase {
                expected: install_fence.attestation_generation,
                got: request.base_attestation_generation,
            });
        }

        connection.epoch = request.epoch;
        connection.load_policy = policy;
        connection.policy_generation = successor_policy_generation;
        connection.accepted_compiled_rows = request.compiled_registry;
        connection.accepted_type_uuids = accepted_type_uuids;
        connection.daemon_compiled_projection = daemon_compiled_projection;
        connection.attestation_generation = request.successor_attestation_generation;
        if connection.stream_installed {
            connection.queue.clear();
            let basis = basis_for(&connection, stamp(&state));
            enqueue_event(
                &mut connection,
                StreamEvent::ResyncRequired {
                    basis,
                    oldest_available: state.current,
                },
            );
        }
        RpcResult::Success(ReattestSuccess {
            installed_attestation_generation: request.successor_attestation_generation,
            daemon_compiled_projection,
            load_policy: connection.load_policy.clone(),
            policy_generation: successor_policy_generation,
        })
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
        RpcResult::Success(self.view.configuration.clone())
    }

    pub fn basis(&self) -> &RpcBasis {
        &self.basis
    }

    pub fn expire_lease(&self) {
        self.lease_alive.store(false, Ordering::Release);
    }

    pub fn refresh(&self) -> RpcResult<Snapshot> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        if !self.lease_alive.load(Ordering::Acquire) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        RpcResult::Success(snapshot_from(
            &self.server,
            &state,
            &connection,
            self.connection.clone(),
        ))
    }

    pub fn query(&self, query: AssetQuery) -> RpcResult<Vec<AssetUuid>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        if let Some(poison) = &self.view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        if query.terminal_type.is_some() {
            if let Some(error) = pipeline_failure(&self.view) {
                return RpcResult::Failure(error);
            }
        }
        if let Err(detail) = validate_asset_query(&query, false) {
            return RpcResult::Failure(RpcFailure::InvalidQuery { detail });
        }
        if query
            .terminal_type
            .is_some_and(|type_uuid| !connection.accepted_type_uuids.contains(&type_uuid))
        {
            return RpcResult::Failure(RpcFailure::InvalidQuery {
                detail: "terminal type selector is outside this Hub's accepted attestation set"
                    .to_owned(),
            });
        }
        if let Some(error) = tag_query_poison(&self.view, &query, AuthoringEntryRole::Runtime) {
            return RpcResult::Failure(error);
        }
        RpcResult::Success(query_asset_entries(
            &self.view,
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
        if let Some(poison) = &self.view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        if let Some(error) = pipeline_failure(&self.view) {
            return RpcResult::Failure(error);
        }
        let Some(entry) = self.view.authoring.get(&uuid) else {
            return RpcResult::Failure(RpcFailure::AssetNotFound { uuid });
        };
        if entry.role != AuthoringEntryRole::Runtime {
            return RpcResult::Failure(RpcFailure::AssetNotFound { uuid });
        }
        let (required, closure_rows) = match served_rows_for_entry(&state, &self.view, entry) {
            Ok(verified) => verified,
            Err(error) => return RpcResult::Failure(error),
        };
        if let Some(expansion) = attestation_expansion(
            self.basis.snapshot,
            1,
            &uuid.0,
            &required,
            &connection.accepted_type_uuids,
            closure_rows,
        ) {
            return RpcResult::AttestationExpansionRequired(expansion);
        }
        RpcResult::Success(metadata_entry(entry))
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
            let mut state = self.server.lock();
            let connection = lock_connection(&self.connection);
            if let Some(result) = self.preflight(&state, &connection) {
                return result;
            }
            if let Some(poison) = &self.view.version_poison {
                return RpcResult::VersionPoisoned(poison.clone());
            }
            if let Some(error) = pipeline_failure(&self.view) {
                return RpcResult::Failure(error);
            }
            if let ConfigurationStatus::Poisoned(poison) = &self.view.configuration {
                return RpcResult::ConfigurationPoisoned(poison.clone());
            }
            let derived = self.view.derived_outputs.get(&uuid).cloned();
            let authoring_uuid = derived.as_ref().map_or(uuid, |output| output.parent);
            if self
                .view
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
            let runtime_entry = self
                .view
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
                || self.view.assets.get(&uuid).cloned(),
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
                    if let Some(flight) = state.build_flights.get(&key).cloned() {
                        drop(connection);
                        drop(state);
                        if let Err(error) = flight.wait() {
                            return RpcResult::Failure(error);
                        }
                        continue;
                    }
                    let flight = Arc::new(BuildFlight::new());
                    state.build_flights.insert(key.clone(), flight.clone());
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
                    let mut state = self.server.lock();
                    state.build_flights.remove(&key);
                    if let Some(error) = pipeline_failure(&self.view) {
                        outcome = Err(error);
                    }
                    if let Ok(resolution) = &outcome {
                        state.build_results.insert(key, resolution.clone());
                    }
                    drop(state);
                    let completed = outcome.as_ref().map(|_| ()).map_err(Clone::clone);
                    flight.complete(completed);
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
            let (required, closure_rows) = if let Some(VersionResolve::Built { content_hash }) =
                &resolution
            {
                match verified_artifact_closure(&state, *content_hash) {
                    Ok((mut required, rows)) => {
                        if let Some(output) = &derived {
                            let Some(root) = rows.iter().find(|row| row.asset == uuid) else {
                                return RpcResult::Failure(RpcFailure::InvalidQuery {
                                    detail:
                                        "built artifact closure omits the requested derived output"
                                            .to_owned(),
                                });
                            };
                            if root.authored_type != output.terminal_type
                                || root.encoded_type != output.terminal_type
                                || root.terminal_type != output.terminal_type
                            {
                                return RpcResult::Failure(RpcFailure::InvalidQuery {
                                    detail: "derived-output declaration does not match the authenticated artifact closure"
                                        .to_owned(),
                                });
                            }
                            required.insert(output.terminal_type);
                        } else if let Some(entry) = &runtime_entry {
                            let Some(root) = rows.iter().find(|row| row.asset == entry.uuid) else {
                                return RpcResult::Failure(RpcFailure::InvalidQuery {
                                    detail:
                                        "built artifact closure omits its runtime metadata asset"
                                            .to_owned(),
                                });
                            };
                            if root.authored_type != entry.type_uuid
                                || root.terminal_type != entry.terminal_type
                            {
                                return RpcResult::Failure(RpcFailure::InvalidQuery {
                                        detail: "runtime metadata types do not match the authenticated artifact closure"
                                            .to_owned(),
                                    });
                            }
                            required.extend([entry.type_uuid, entry.terminal_type]);
                        }
                        (required, rows)
                    }
                    Err(error) => return RpcResult::Failure(error),
                }
            } else if let Some(output) = &derived {
                (BTreeSet::from([output.terminal_type]), Vec::new())
            } else if let Some(entry) = &runtime_entry {
                match served_rows_for_entry(&state, &self.view, entry) {
                    Ok(verified) => verified,
                    Err(error) => return RpcResult::Failure(error),
                }
            } else {
                (BTreeSet::new(), Vec::new())
            };
            if let Some(expansion) = attestation_expansion(
                self.basis.snapshot,
                2,
                &uuid.0,
                &required,
                &connection.accepted_type_uuids,
                closure_rows,
            ) {
                return RpcResult::AttestationExpansionRequired(expansion);
            }
            let value = match resolution {
                Some(VersionResolve::Built { content_hash }) => {
                    let Some(_) = state.artifacts.get(&content_hash) else {
                        return RpcResult::Failure(RpcFailure::ArtifactNotFound {
                            hash: content_hash,
                        });
                    };
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
        if let Some(poison) = &self.view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        if !valid_logical_path(path) {
            return RpcResult::Failure(RpcFailure::InvalidPath {
                path: path.to_owned(),
            });
        }
        let value = match self.view.paths.get(path) {
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
        let payload = match state.artifacts.get(&hash) {
            Some(payload) => payload,
            None => return RpcResult::Failure(RpcFailure::ArtifactNotFound { hash }),
        };
        let (required_runtime_types, closure_rows) = match verified_artifact_closure(&state, hash) {
            Ok(verified) => verified,
            Err(error) => return RpcResult::Failure(error),
        };
        if let Some(expansion) = attestation_expansion(
            self.basis.snapshot,
            3,
            &hash.0,
            &required_runtime_types,
            &connection.accepted_type_uuids,
            closure_rows,
        ) {
            return RpcResult::AttestationExpansionRequired(expansion);
        }
        RpcResult::Success(TerminalEvent {
            basis: self.basis.clone(),
            value: chunk_payload(&payload.payload, state.chunk_size),
        })
    }

    fn preflight<T>(
        &self,
        state: &ServerState,
        connection: &ConnectionState,
    ) -> Option<RpcResult<T>> {
        if let Some(reason) = generation_fence(state, connection) {
            return Some(RpcResult::ReconnectRequired { reason });
        }
        if connection.epoch != self.epoch {
            return Some(RpcResult::Failure(RpcFailure::ClientEpochChanged {
                snapshot: self.epoch,
                current: connection.epoch,
            }));
        }
        if connection.attestation_generation != self.attestation_generation {
            return Some(RpcResult::Failure(RpcFailure::StaleAttestationBase {
                expected: connection.attestation_generation,
                got: self.attestation_generation,
            }));
        }
        if !self.lease_alive.load(Ordering::Acquire) {
            return Some(RpcResult::Failure(RpcFailure::LeaseExpired));
        }
        None
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
        self.lease_alive.store(false, Ordering::Release);
    }

    pub fn query(&self, query: AssetQuery) -> RpcResult<Vec<AssetUuid>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        if let Some(poison) = &self.view.version_poison {
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
        if let Some(error) = tag_query_poison(&self.view, &query, role) {
            return RpcResult::Failure(error);
        }
        let values = self
            .view
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
        if let Some(poison) = &self.view.version_poison {
            return RpcResult::VersionPoisoned(poison.clone());
        }
        let Some(entry) = self.view.authoring.get(&uuid) else {
            if self.view.assets.contains_key(&uuid) {
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
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        if !self.lease_alive.load(Ordering::Acquire) {
            return RpcResult::Failure(RpcFailure::LeaseExpired);
        }
        RpcResult::Success(authoring_snapshot_from(
            &self.server,
            &state,
            &connection,
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
        if connection.epoch != self.epoch {
            return Some(RpcResult::Failure(RpcFailure::ClientEpochChanged {
                snapshot: self.epoch,
                current: connection.epoch,
            }));
        }
        if connection.attestation_generation != self.attestation_generation {
            return Some(RpcResult::Failure(RpcFailure::StaleAttestationBase {
                expected: connection.attestation_generation,
                got: self.attestation_generation,
            }));
        }
        if !self.lease_alive.load(Ordering::Acquire) {
            return Some(RpcResult::Failure(RpcFailure::LeaseExpired));
        }
        None
    }
}

impl DeltaStream {
    pub fn next(&self) -> Option<StreamEvent> {
        let event = lock_connection(&self.connection).queue.pop_front();
        event
    }

    /// Wait for the next stream event without blocking the single-threaded
    /// Cap'n Proto `RpcSystem`. Registration happens while the queue mutex is
    /// held, closing the check/notify lost-wakeup window.
    pub async fn next_async(&self) -> StreamEvent {
        loop {
            let notified = {
                let mut connection = lock_connection(&self.connection);
                if let Some(event) = connection.queue.pop_front() {
                    return event;
                }
                connection.notify.clone().notified_owned()
            };
            notified.await;
        }
    }
}

fn validate_complete_attestation(
    target: &TargetDefinition,
    target_hash: TargetDefinitionHash,
    compiled_rows: &[CompiledTypeRow],
    dsca: CompiledAttestationDigest,
    policy_rows: &[LoadPolicyEntry],
    policy_digest: [u8; 32],
) -> Result<Arc<LoadPolicyAttestation>, ConnectError> {
    let expected_dsca =
        distill_core::attestation::compute_compiled_attestation_digest(compiled_rows)
            .map_err(|error| ConnectError::AttestationShape(error.into()))?;
    compute_policy_digest(policy_rows).map_err(ConnectError::AttestationShape)?;
    validate_cross_projection_rows(compiled_rows, policy_rows)?;
    if expected_dsca != dsca {
        return Err(ConnectError::CompiledRegistryAggregateMismatch {
            expected: expected_dsca.0,
            observed: dsca.0,
        });
    }
    validate_attestation_shape(compiled_rows, dsca, policy_rows, policy_digest)
        .map_err(ConnectError::AttestationShape)?;
    if target.definition_hash() != target_hash {
        return Err(ConnectError::TargetDefinitionMismatch {
            expected: target.definition_hash(),
            got: target_hash,
        });
    }
    let expected_bootstrap = target
        .compiled_registry()
        .iter()
        .filter(|row| distill_core::attestation::is_bootstrap_control_type(row.type_uuid))
        .cloned()
        .collect::<Vec<_>>();
    let observed_bootstrap = compiled_rows
        .iter()
        .filter(|row| distill_core::attestation::is_bootstrap_control_type(row.type_uuid))
        .cloned()
        .collect::<Vec<_>>();
    if let Some(missing) = expected_bootstrap.iter().find(|expected| {
        observed_bootstrap
            .binary_search_by_key(&expected.type_uuid, |row| row.type_uuid)
            .is_err()
    }) {
        return Err(ConnectError::MissingCompiledType {
            type_uuid: missing.type_uuid,
        });
    }
    if expected_bootstrap != observed_bootstrap {
        return Err(ConnectError::BootstrapAuthorityMismatch {
            expected: encode_compiled_rows(&expected_bootstrap),
            observed: encode_compiled_rows(&observed_bootstrap),
        });
    }

    if dsca != target.dsca() {
        for client in compiled_rows {
            let server = target
                .compiled_registry()
                .binary_search_by_key(&client.type_uuid, |row| row.type_uuid)
                .ok()
                .map(|index| &target.compiled_registry()[index]);
            match server {
                None => {
                    return Err(ConnectError::MissingCompiledType {
                        type_uuid: client.type_uuid,
                    })
                }
                Some(server) => {
                    if server.logical_hash != client.logical_hash {
                        return Err(ConnectError::LogicalHashMismatch {
                            type_uuid: client.type_uuid,
                            expected: server.logical_hash.0,
                            observed: client.logical_hash.0,
                        });
                    }
                    if server.native_layout_digest != client.native_layout_digest {
                        return Err(ConnectError::NativeLayoutMismatch {
                            type_uuid: client.type_uuid,
                            expected: server.native_layout_digest,
                            observed: client.native_layout_digest,
                        });
                    }
                    if server.build_only != client.build_only {
                        return Err(ConnectError::CompiledBuildOnlyMismatch {
                            type_uuid: client.type_uuid,
                            expected: server.build_only,
                            observed: client.build_only,
                        });
                    }
                    if server.registry_extras != client.registry_extras
                        || server.registry_extras_digest != client.registry_extras_digest
                    {
                        return Err(ConnectError::RegistryExtrasMismatch {
                            type_uuid: client.type_uuid,
                            expected: server
                                .registry_extras
                                .encode()
                                .expect("validated daemon extras encode"),
                            observed: client
                                .registry_extras
                                .encode()
                                .expect("validated client extras encode"),
                        });
                    }
                }
            }
        }
    }
    let accepted_runtime_types = compiled_rows
        .iter()
        .filter(|row| !distill_core::attestation::is_bootstrap_control_type(row.type_uuid))
        .map(|row| row.type_uuid)
        .collect::<BTreeSet<_>>();
    let server_policy = daemon_policy_projection(target.load_policy(), &accepted_runtime_types);
    for (expected, observed) in server_policy.rows.iter().zip(policy_rows) {
        if expected.type_uuid != observed.type_uuid {
            return Err(ConnectError::MissingLoadPolicy {
                type_uuid: observed.type_uuid,
            });
        }
        if expected.build_only != observed.build_only {
            return Err(ConnectError::LoadPolicyMismatch {
                type_uuid: observed.type_uuid,
                expected: expected.build_only,
                got: observed.build_only,
            });
        }
    }
    if server_policy.rows.len() != policy_rows.len() {
        let type_uuid = server_policy.rows.get(policy_rows.len()).map_or_else(
            || policy_rows[server_policy.rows.len()].type_uuid,
            |row| row.type_uuid,
        );
        return Err(ConnectError::MissingLoadPolicy { type_uuid });
    }
    Ok(Arc::new(server_policy))
}

fn validate_cross_projection_rows(
    compiled_rows: &[CompiledTypeRow],
    policy_rows: &[LoadPolicyEntry],
) -> Result<(), ConnectError> {
    let mut compiled = 0usize;
    let mut policy = 0usize;
    while compiled < compiled_rows.len() || policy < policy_rows.len() {
        match (compiled_rows.get(compiled), policy_rows.get(policy)) {
            (Some(compiled_row), Some(policy_row))
                if compiled_row.type_uuid == policy_row.type_uuid =>
            {
                if compiled_row.build_only != policy_row.build_only {
                    return Err(ConnectError::LoadPolicyMismatch {
                        type_uuid: compiled_row.type_uuid,
                        expected: compiled_row.build_only,
                        got: policy_row.build_only,
                    });
                }
                compiled += 1;
                policy += 1;
            }
            (Some(compiled_row), Some(policy_row))
                if compiled_row.type_uuid < policy_row.type_uuid =>
            {
                return Err(ConnectError::MissingLoadPolicy {
                    type_uuid: compiled_row.type_uuid,
                });
            }
            (Some(_), Some(policy_row)) => {
                return Err(ConnectError::MissingCompiledType {
                    type_uuid: policy_row.type_uuid,
                });
            }
            (Some(compiled_row), None) => {
                return Err(ConnectError::MissingLoadPolicy {
                    type_uuid: compiled_row.type_uuid,
                });
            }
            (None, Some(policy_row)) => {
                return Err(ConnectError::MissingCompiledType {
                    type_uuid: policy_row.type_uuid,
                });
            }
            (None, None) => break,
        }
    }
    Ok(())
}

fn encode_compiled_rows(rows: &[CompiledTypeRow]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        let row = row.encode().expect("validated compiled row encodes");
        bytes.extend_from_slice(&(row.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&row);
    }
    bytes
}

fn snapshot_from(
    server: &Server,
    state: &ServerState,
    connection: &ConnectionState,
    connection_arc: Arc<Mutex<ConnectionState>>,
) -> Snapshot {
    let view = state
        .views
        .get(&state.current)
        .expect("current view must exist")
        .clone();
    Snapshot {
        server: server.clone(),
        connection: connection_arc,
        connection_id: connection.id,
        epoch: connection.epoch,
        attestation_generation: connection.attestation_generation,
        basis: basis_for(connection, view.stamp),
        view,
        lease_alive: Arc::new(AtomicBool::new(true)),
    }
}

fn authoring_snapshot_from(
    server: &Server,
    state: &ServerState,
    connection: &ConnectionState,
    connection_arc: Arc<Mutex<ConnectionState>>,
) -> AuthoringSnapshot {
    let view = state
        .views
        .get(&state.current)
        .expect("current view must exist")
        .clone();
    AuthoringSnapshot {
        server: server.clone(),
        connection: connection_arc,
        connection_id: connection.id,
        epoch: connection.epoch,
        attestation_generation: connection.attestation_generation,
        basis: basis_for(connection, view.stamp),
        view,
        lease_alive: Arc::new(AtomicBool::new(true)),
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
    let Some(target) = state.targets.get(&connection.target) else {
        return Some(ReconnectReason::TargetDefinitionChanged);
    };
    if target.target_generation != connection.target_generation {
        return Some(ReconnectReason::TargetDefinitionChanged);
    }
    let compiled_projection = match daemon_projection_digest(
        target.definition.compiled_registry(),
        &connection.accepted_type_uuids,
    ) {
        Ok(digest) => digest,
        Err(_) => return Some(ReconnectReason::CompiledAttestationChanged),
    };
    if daemon_policy_projection(
        target.definition.load_policy(),
        &connection.accepted_type_uuids,
    ) != *connection.load_policy
    {
        Some(ReconnectReason::LoadPolicyChanged)
    } else if compiled_projection != connection.daemon_compiled_projection {
        Some(ReconnectReason::CompiledAttestationChanged)
    } else {
        None
    }
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

fn basis_for(connection: &ConnectionState, snapshot: SnapshotStamp) -> RpcBasis {
    RpcBasis {
        snapshot,
        load_policy: connection.load_policy.clone(),
        policy_generation: connection.policy_generation,
        target_generation: connection.target_generation,
        attestation_generation: connection.attestation_generation,
        daemon_compiled_projection: connection.daemon_compiled_projection,
    }
}

fn daemon_projection_digest(
    daemon_rows: &[CompiledTypeRow],
    accepted_type_uuids: &BTreeSet<TypeUuid>,
) -> Result<CompiledAttestationDigest, distill_core::attestation::AttestationError> {
    Ok(CompiledTypeTable::canonical(daemon_rows.to_vec())?
        .project_with_bootstrap(accepted_type_uuids)?
        .digest)
}

fn projection_connect_error(error: distill_core::attestation::AttestationError) -> ConnectError {
    match error {
        distill_core::attestation::AttestationError::ProjectionMissingType(type_uuid) => {
            ConnectError::MissingCompiledType { type_uuid }
        }
        error => ConnectError::AttestationShape(error.into()),
    }
}

fn daemon_policy_projection(
    daemon_rows: &[LoadPolicyEntry],
    accepted_type_uuids: &BTreeSet<TypeUuid>,
) -> LoadPolicyAttestation {
    let boundary = accepted_type_uuids
        .iter()
        .copied()
        .chain(distill_core::attestation::BOOTSTRAP_CONTROL_TYPE_UUIDS)
        .collect::<BTreeSet<_>>();
    let rows = daemon_rows
        .iter()
        .filter(|row| boundary.contains(&row.type_uuid))
        .copied()
        .collect::<Vec<_>>();
    let digest = compute_policy_digest(&rows)
        .expect("validated daemon policy remains canonical after ordered projection");
    LoadPolicyAttestation { rows, digest }
}

fn validate_served_closure(
    rows: &[ServedClosureRow],
    require_nonempty: bool,
) -> Result<BTreeSet<TypeUuid>, AdminError> {
    let invalid = |detail: &str| AdminError::InvalidServedClosure {
        detail: detail.to_owned(),
    };
    if require_nonempty && rows.is_empty() {
        return Err(invalid("artifact closure rows must not be empty"));
    }
    if rows.windows(2).any(|pair| pair[0].asset >= pair[1].asset) {
        return Err(invalid(
            "closure rows must be strictly raw-AssetUuid sorted and unique",
        ));
    }
    for row in rows {
        if row.load_edges.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(
                "closure load edges must be strictly sorted and unique",
            ));
        }
    }
    let required = served_closure_types(rows);
    if required
        .iter()
        .any(|uuid| distill_core::attestation::is_bootstrap_control_type(*uuid))
    {
        return Err(invalid(
            "served runtime closure must not use bootstrap-control types as runtime coverage",
        ));
    }
    Ok(required)
}

fn served_closure_types(rows: &[ServedClosureRow]) -> BTreeSet<TypeUuid> {
    let mut required = BTreeSet::new();
    for row in rows {
        required.extend([row.authored_type, row.encoded_type, row.terminal_type]);
        required.extend(row.load_edges.iter().map(|edge| edge.expected_terminal));
    }
    required
}

fn served_rows_for_entry(
    state: &ServerState,
    view: &VersionView,
    entry: &AuthoringEntry,
) -> Result<(BTreeSet<TypeUuid>, Vec<ServedClosureRow>), RpcFailure> {
    let content_hash = match view.assets.get(&entry.uuid) {
        Some(VersionResolve::Built { content_hash }) => *content_hash,
        _ => ContentHash([0; 32]),
    };
    if state.artifacts.contains_key(&content_hash) {
        let (mut required, rows) = verified_artifact_closure(state, content_hash)?;
        let Some(root) = rows.iter().find(|row| row.asset == entry.uuid) else {
            return Err(RpcFailure::InvalidQuery {
                detail: "built artifact closure omits its runtime metadata asset".to_owned(),
            });
        };
        if root.authored_type != entry.type_uuid || root.terminal_type != entry.terminal_type {
            return Err(RpcFailure::InvalidQuery {
                detail: "runtime metadata types do not match the authenticated artifact closure"
                    .to_owned(),
            });
        }
        required.extend([entry.type_uuid, entry.terminal_type]);
        return Ok((required, rows));
    }
    if matches!(
        view.assets.get(&entry.uuid),
        Some(VersionResolve::Built { .. })
    ) {
        return Err(RpcFailure::ArtifactNotFound { hash: content_hash });
    }
    let rows = vec![ServedClosureRow {
        asset: entry.uuid,
        content_hash,
        authored_type: entry.type_uuid,
        encoded_type: entry.type_uuid,
        terminal_type: entry.terminal_type,
        load_edges: Vec::new(),
    }];
    Ok((served_closure_types(&rows), rows))
}

fn verified_artifact_closure(
    state: &ServerState,
    root_hash: ContentHash,
) -> Result<(BTreeSet<TypeUuid>, Vec<ServedClosureRow>), RpcFailure> {
    let root_artifact = state
        .artifacts
        .get(&root_hash)
        .ok_or(RpcFailure::ArtifactNotFound { hash: root_hash })?;
    let rows = root_artifact.payload.closure_rows.clone();
    let by_asset = rows
        .iter()
        .map(|row| (row.asset, row))
        .collect::<BTreeMap<_, _>>();
    let mut pending = vec![root_artifact.asset_uuid];
    let mut visited = BTreeSet::new();
    while let Some(asset) = pending.pop() {
        if !visited.insert(asset) {
            continue;
        }
        let row = by_asset
            .get(&asset)
            .copied()
            .ok_or_else(|| RpcFailure::InvalidQuery {
                detail: "authenticated artifact closure is missing a reachable asset row"
                    .to_owned(),
            })?;
        let artifact =
            state
                .artifacts
                .get(&row.content_hash)
                .ok_or(RpcFailure::ArtifactNotFound {
                    hash: row.content_hash,
                })?;
        if artifact.asset_uuid != row.asset {
            return Err(RpcFailure::InvalidQuery {
                detail:
                    "artifact closure row asset does not match its ContentHash-authenticated header"
                        .to_owned(),
            });
        }
        let authenticated_root = artifact
            .payload
            .closure_rows
            .binary_search_by_key(&artifact.asset_uuid, |candidate| candidate.asset)
            .ok()
            .map(|index| &artifact.payload.closure_rows[index])
            .ok_or_else(|| RpcFailure::InvalidQuery {
                detail: "installed artifact is missing its authenticated root closure row"
                    .to_owned(),
            })?;
        if authenticated_root != row {
            return Err(RpcFailure::InvalidQuery {
                detail: "artifact closure row disagrees with the target artifact's authenticated header or load edges"
                    .to_owned(),
            });
        }
        for edge in &row.load_edges {
            let dependency =
                by_asset
                    .get(&edge.asset)
                    .copied()
                    .ok_or_else(|| RpcFailure::InvalidQuery {
                        detail: "artifact closure omits a typed load-dependency target".to_owned(),
                    })?;
            if dependency.terminal_type != edge.expected_terminal {
                return Err(RpcFailure::InvalidQuery {
                    detail: "typed load edge disagrees with its dependency terminal type"
                        .to_owned(),
                });
            }
            pending.push(edge.asset);
        }
    }
    if visited.len() != rows.len() {
        return Err(RpcFailure::InvalidQuery {
            detail: "artifact closure contains unreachable rows".to_owned(),
        });
    }
    Ok((served_closure_types(&rows), rows))
}

fn attestation_expansion(
    snapshot: SnapshotStamp,
    method_tag: u8,
    request_subject: &[u8],
    required_types: &BTreeSet<TypeUuid>,
    accepted_types: &BTreeSet<TypeUuid>,
    mut closure_rows: Vec<ServedClosureRow>,
) -> Option<AttestationExpansionRequired> {
    let required = required_types
        .difference(accepted_types)
        .copied()
        .collect::<Vec<_>>();
    if required.is_empty() {
        return None;
    }
    closure_rows.sort();
    closure_rows.dedup();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DSAE");
    hasher.update(&[1]);
    hasher.update(&snapshot.instance.0);
    hasher.update(&snapshot.version.0.to_le_bytes());
    hasher.update(&[method_tag]);
    hasher.update(&(request_subject.len() as u32).to_le_bytes());
    hasher.update(request_subject);
    hasher.update(&(closure_rows.len() as u32).to_le_bytes());
    for row in closure_rows {
        hasher.update(&row.asset.0);
        hasher.update(&row.content_hash.0);
        hasher.update(&row.authored_type.0);
        hasher.update(&row.encoded_type.0);
        hasher.update(&row.terminal_type.0);
        hasher.update(&(row.load_edges.len() as u32).to_le_bytes());
        for edge in row.load_edges {
            hasher.update(&edge.asset.0);
            hasher.update(&edge.expected_terminal.0);
        }
    }
    Some(AttestationExpansionRequired {
        snapshot,
        closure_identity: *hasher.finalize().as_bytes(),
        required,
    })
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
    state.history.push_back(HistoryDelta {
        stamp: next_stamp,
        assets: Vec::new(),
        paths: Vec::new(),
    });
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
            let policy_changed = runtime.definition.load_policy() != definition.load_policy()
                || runtime.definition.policy_digest() != definition.policy_digest();
            let compiled_changed =
                runtime.definition.compiled_registry() != definition.compiled_registry();
            if target_changed {
                runtime.target_generation = runtime
                    .target_generation
                    .checked_add(1)
                    .expect("target generation exhausted");
            }
            if policy_changed {
                runtime.policy_authority_generation = runtime
                    .policy_authority_generation
                    .checked_add(1)
                    .expect("policy authority generation exhausted");
            }
            if compiled_changed {
                runtime.compiled_table_generation = runtime
                    .compiled_table_generation
                    .checked_add(1)
                    .expect("compiled table generation exhausted");
            }
            runtime.definition = definition;
            if target_changed {
                reconnect.push((name.clone(), ReconnectReason::TargetDefinitionChanged));
            } else {
                if policy_changed {
                    reconnect.push((name.clone(), ReconnectReason::LoadPolicyChanged));
                }
                if compiled_changed {
                    reconnect.push((name.clone(), ReconnectReason::CompiledAttestationChanged));
                }
            }
            runtime
        } else {
            TargetRuntime {
                definition,
                target_generation: 0,
                policy_authority_generation: 0,
                compiled_table_generation: 0,
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
        match reason {
            ReconnectReason::TargetDefinitionChanged => notify_reconnect(state, &name, reason),
            ReconnectReason::LoadPolicyChanged | ReconnectReason::CompiledAttestationChanged => {
                notify_projection_reconnect(state, &name, reason)
            }
            ReconnectReason::StoreInstanceChanged | ReconnectReason::ProtocolEpochChanged => {
                unreachable!("target-set replacement cannot produce a global reconnect")
            }
        }
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

fn notify_projection_reconnect(state: &mut ServerState, target: &str, reason: ReconnectReason) {
    let current = stamp(state);
    for connection in live_connections(state) {
        let mut connection = lock_connection(&connection);
        if connection.target != target || generation_fence(state, &connection) != Some(reason) {
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
            live.push(connection);
            true
        } else {
            false
        }
    });
    live
}

fn commit_locked(state: &mut ServerState, commit: Commit) -> Result<SnapshotStamp, AdminError> {
    validate_commit(&commit)?;
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
    if let Some(tag_poisons) = commit.tag_poisons {
        view.tag_poisons = tag_poisons;
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
    let delta = HistoryDelta {
        stamp: next_stamp,
        assets: asset_deltas,
        paths: path_deltas,
    };
    state.history.push_back(delta.clone());
    notify_live_delta(state, &delta);
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
                    || distill_core::attestation::is_bootstrap_control_type(mismatch.type_uuid)
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
    }
}

fn lock_connection(connection: &Arc<Mutex<ConnectionState>>) -> MutexGuard<'_, ConnectionState> {
    connection
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn enqueue_event(connection: &mut ConnectionState, event: StreamEvent) {
    connection.queue.push_back(event);
    connection.notify.notify_one();
}
