use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tokio::sync::Notify;
use unicode_normalization::UnicodeNormalization;

use distill_core::attestation::RegistryExtraFact;

use crate::attestation::validate_attestation_shape;
use crate::*;

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Clone)]
pub struct Server {
    inner: Arc<Mutex<ServerState>>,
}

#[derive(Clone)]
pub struct Root {
    server: Server,
}

#[derive(Clone)]
pub struct Hub {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
}

#[derive(Clone)]
pub struct Snapshot {
    server: Server,
    connection: Arc<Mutex<ConnectionState>>,
    connection_id: u64,
    epoch: GameModuleEpoch,
    view: Arc<VersionView>,
    basis: RpcBasis,
    lease_alive: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct DeltaStream {
    connection: Arc<Mutex<ConnectionState>>,
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

impl fmt::Debug for Hub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hub")
            .field("connection_id", &self.connection_id())
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

impl fmt::Debug for DeltaStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaStream").finish_non_exhaustive()
    }
}

struct ServerState {
    instance: StoreInstanceId,
    current: InputVersion,
    views: BTreeMap<InputVersion, Arc<VersionView>>,
    history: VecDeque<HistoryDelta>,
    oldest_available_cursor: InputVersion,
    targets: BTreeMap<String, TargetRuntime>,
    artifacts: HashMap<ContentHash, ArtifactPayload>,
    connections: Vec<Weak<Mutex<ConnectionState>>>,
    next_connection_id: u64,
    chunk_size: usize,
    restart_required_keys: BTreeSet<String>,
}

#[derive(Clone)]
struct VersionView {
    stamp: SnapshotStamp,
    configuration: ConfigurationStatus,
    assets: BTreeMap<AssetUuid, VersionResolve>,
    paths: BTreeMap<String, BTreeSet<AssetUuid>>,
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
    policy_generation: u64,
}

struct ConnectionState {
    id: u64,
    target: String,
    target_generation: u64,
    policy_generation: u64,
    attestation_generation: u64,
    epoch: GameModuleEpoch,
    load_policy: Arc<LoadPolicyAttestation>,
    subscribed_assets: BTreeSet<AssetUuid>,
    subscribed_paths: BTreeSet<String>,
    queue: VecDeque<StreamEvent>,
    stream_installed: bool,
    notify: Arc<Notify>,
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
        let mut target_map = BTreeMap::new();
        for target in targets {
            validate_attestation_shape(
                &target.compiled_registry,
                target.dsca,
                &target.load_policy,
                target.policy_digest,
            )?;
            let name = target.name.clone();
            if target_map
                .insert(
                    name.clone(),
                    TargetRuntime {
                        definition: target,
                        target_generation: 0,
                        policy_generation: 0,
                    },
                )
                .is_some()
            {
                return Err(AttestationShapeError::DuplicateTarget { target: name });
            }
        }
        let stamp = SnapshotStamp {
            instance,
            version: InputVersion(0),
        };
        let view = Arc::new(VersionView {
            stamp,
            configuration: ConfigurationStatus::Ready,
            assets: BTreeMap::new(),
            paths: BTreeMap::new(),
        });
        let mut views = BTreeMap::new();
        views.insert(InputVersion(0), view);
        Ok(Self {
            inner: Arc::new(Mutex::new(ServerState {
                instance,
                current: InputVersion(0),
                views,
                history: VecDeque::new(),
                oldest_available_cursor: InputVersion(0),
                targets: target_map,
                artifacts: HashMap::new(),
                connections: Vec::new(),
                next_connection_id: 1,
                chunk_size: DEFAULT_CHUNK_SIZE,
                restart_required_keys: BTreeSet::new(),
            })),
        })
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

    pub fn install_artifact(
        &self,
        hash: ContentHash,
        payload: ArtifactPayload,
    ) -> Result<(), AdminError> {
        let mut state = self.lock();
        if let Some(existing) = state.artifacts.get(&hash) {
            if existing != &payload {
                return Err(AdminError::ArtifactAlreadyExistsWithDifferentPayload { hash });
            }
            return Ok(());
        }
        state.artifacts.insert(hash, payload);
        Ok(())
    }

    /// Publish an input-version commit and deliver its filtered live delta.
    pub fn commit(&self, commit: Commit) -> Result<SnapshotStamp, AdminError> {
        let mut state = self.lock();
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
                        StoredResolve::Built { content_hash } => {
                            VersionResolve::Built { content_hash }
                        }
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
        if let Some(configuration) = commit.configuration {
            view.configuration = configuration;
        }
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
        notify_live_delta(&mut state, &delta);
        Ok(next_stamp)
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
            &replacement.compiled_registry,
            replacement.dsca,
            &replacement.load_policy,
            replacement.policy_digest,
        )
        .map_err(AdminError::InvalidTargetAttestation)?;
        let name = replacement.name.clone();
        let runtime = state
            .targets
            .get_mut(&name)
            .ok_or_else(|| AdminError::UnknownTarget {
                target: name.clone(),
            })?;
        let target_changed = runtime.definition.definition_hash != replacement.definition_hash
            || !compiled_equal_ignoring_load_policy(
                &runtime.definition.compiled_registry,
                &replacement.compiled_registry,
            );
        let policy_changed = runtime.definition.load_policy != replacement.load_policy
            || runtime.definition.policy_digest != replacement.policy_digest;
        if !target_changed && !policy_changed {
            return Ok(stamp(&state));
        }
        if target_changed {
            runtime.target_generation = runtime
                .target_generation
                .checked_add(1)
                .expect("target generation exhausted");
        }
        if policy_changed {
            runtime.policy_generation = runtime
                .policy_generation
                .checked_add(1)
                .expect("policy generation exhausted");
        }
        runtime.definition = replacement;

        advance_empty_version(&mut state);
        let reason = if target_changed {
            ReconnectReason::TargetDefinitionChanged
        } else {
            ReconnectReason::LoadPolicyChanged
        };
        notify_reconnect(&mut state, &name, reason);
        Ok(stamp(&state))
    }

    /// Stage a valid restart-only edit. This deliberately does not advance
    /// the input version or mutate active configuration values.
    pub fn restart_required(&self, keys: Vec<String>) -> SnapshotStamp {
        let mut keys = keys;
        keys.sort();
        keys.dedup();
        let mut state = self.lock();
        if !keys.is_empty() {
            state.restart_required_keys.extend(keys);
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

fn compiled_equal_ignoring_load_policy(
    left: &[CompiledTypeRow],
    right: &[CompiledTypeRow],
) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.type_uuid == right.type_uuid
                && left.logical_hash == right.logical_hash
                && left.native_layout_digest == right.native_layout_digest
                && left
                    .registry_extras
                    .rows
                    .iter()
                    .filter(|row| !matches!(row.fact, RegistryExtraFact::BuildOnly(_)))
                    .eq(right
                        .registry_extras
                        .rows
                        .iter()
                        .filter(|row| !matches!(row.fact, RegistryExtraFact::BuildOnly(_))))
        })
}

impl Root {
    pub fn connect(&self, request: ConnectRequest) -> ConnectOutcome {
        let mut state = self.server.lock();
        if request.protocol != PROTOCOL_VERSION {
            return ConnectOutcome::Rejected(ConnectError::ProtocolMismatch {
                expected: PROTOCOL_VERSION,
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
        let policy_generation = runtime.policy_generation;
        let current_view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        if let ConfigurationStatus::Poisoned(poison) = &current_view.configuration {
            return ConnectOutcome::ConfigurationPoisoned(poison.clone());
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
            epoch: request.epoch,
            load_policy: policy,
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
        })
    }
}

impl Hub {
    pub fn connection_id(&self) -> u64 {
        lock_connection(&self.connection).id
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
        let current = stamp(&state);
        RpcResult::Success(TerminalEvent {
            basis: basis_for(&connection, current),
            value: chunk_payload(payload, state.chunk_size),
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

    pub fn reattest(&self, request: ReattestRequest) -> RpcResult<u64> {
        let state = self.server.lock();
        let mut connection = lock_connection(&self.connection);
        if let Some(reason) = generation_fence(&state, &connection) {
            return RpcResult::ReconnectRequired { reason };
        }
        let current_view = state
            .views
            .get(&state.current)
            .expect("current view must exist");
        if let ConfigurationStatus::Poisoned(poison) = &current_view.configuration {
            return RpcResult::ConfigurationPoisoned(poison.clone());
        }
        let expected_successor = match request.base_attestation_generation.checked_add(1) {
            Some(successor) => successor,
            None => {
                return RpcResult::Failure(RpcFailure::InvalidAttestationSuccessor {
                    base: request.base_attestation_generation,
                    successor: request.successor_attestation_generation,
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
        if request.epoch <= connection.epoch {
            return RpcResult::Failure(RpcFailure::EpochNotSuccessor {
                previous: connection.epoch,
                proposed: request.epoch,
            });
        }
        let runtime = state
            .targets
            .get(&connection.target)
            .expect("connected target must exist");
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
        connection.epoch = request.epoch;
        connection.load_policy = policy;
        connection.attestation_generation = request.successor_attestation_generation;
        RpcResult::Success(request.successor_attestation_generation)
    }
}

impl Snapshot {
    pub fn stamp(&self) -> SnapshotStamp {
        self.basis.snapshot
    }

    pub fn version(&self) -> InputVersion {
        self.basis.snapshot.version
    }

    pub fn configuration(&self) -> ConfigurationStatus {
        self.view.configuration.clone()
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

    pub fn resolve(&self, uuid: AssetUuid) -> RpcResult<TerminalEvent<ResolveResult>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        if let ConfigurationStatus::Poisoned(poison) = &self.view.configuration {
            return RpcResult::ConfigurationPoisoned(poison.clone());
        }
        let value = match self.view.assets.get(&uuid) {
            Some(VersionResolve::Built { content_hash }) => ResolveResult::Built {
                content_hash: *content_hash,
            },
            Some(VersionResolve::Drifted { input }) => ResolveResult::Drifted {
                input: input.clone(),
                current: stamp(&state),
            },
            Some(VersionResolve::Failed { error }) => ResolveResult::Failed {
                error: error.clone(),
            },
            Some(VersionResolve::Deleted { at }) => ResolveResult::Deleted { at: *at },
            None => ResolveResult::Missing,
        };
        RpcResult::Success(TerminalEvent {
            basis: self.basis.clone(),
            value,
        })
    }

    pub fn resolve_path(&self, path: &str) -> RpcResult<TerminalEvent<PathResolveResult>> {
        let state = self.server.lock();
        let connection = lock_connection(&self.connection);
        if let Some(result) = self.preflight(&state, &connection) {
            return result;
        }
        if !valid_logical_path(path) {
            return RpcResult::Failure(RpcFailure::InvalidPath {
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
        RpcResult::Success(TerminalEvent {
            basis: self.basis.clone(),
            value: chunk_payload(payload, state.chunk_size),
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
    validate_attestation_shape(compiled_rows, dsca, policy_rows, policy_digest)
        .map_err(ConnectError::AttestationShape)?;
    if target.definition_hash != target_hash {
        return Err(ConnectError::TargetDefinitionMismatch {
            expected: target.definition_hash,
            got: target_hash,
        });
    }

    if dsca != target.dsca {
        for client in compiled_rows {
            let server = target
                .compiled_registry
                .binary_search_by_key(&client.type_uuid, |row| row.type_uuid)
                .ok()
                .map(|index| &target.compiled_registry[index]);
            match server {
                None => {
                    return Err(ConnectError::MissingCompiledType {
                        type_uuid: client.type_uuid,
                    })
                }
                Some(server) if server != client => {
                    return Err(ConnectError::CompiledTypeMismatch {
                        type_uuid: client.type_uuid,
                    })
                }
                Some(_) => {}
            }
        }
    }
    if policy_digest != target.policy_digest {
        for client in policy_rows {
            let server = target
                .load_policy
                .binary_search_by_key(&client.type_uuid, |row| row.type_uuid)
                .ok()
                .map(|index| target.load_policy[index]);
            match server {
                None => {
                    return Err(ConnectError::MissingLoadPolicy {
                        type_uuid: client.type_uuid,
                    })
                }
                Some(server) if server.build_only != client.build_only => {
                    return Err(ConnectError::LoadPolicyMismatch {
                        type_uuid: client.type_uuid,
                        expected: server.build_only,
                        got: client.build_only,
                    })
                }
                Some(_) => {}
            }
        }
    }
    Ok(Arc::new(LoadPolicyAttestation {
        rows: policy_rows.to_vec(),
        digest: policy_digest,
    }))
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
        basis: basis_for(connection, view.stamp),
        view,
        lease_alive: Arc::new(AtomicBool::new(true)),
    }
}

fn generation_fence(state: &ServerState, connection: &ConnectionState) -> Option<ReconnectReason> {
    let target = state
        .targets
        .get(&connection.target)
        .expect("connected target must exist");
    if target.target_generation != connection.target_generation {
        Some(ReconnectReason::TargetDefinitionChanged)
    } else if target.policy_generation != connection.policy_generation {
        Some(ReconnectReason::LoadPolicyChanged)
    } else {
        None
    }
}

fn basis_for(connection: &ConnectionState, snapshot: SnapshotStamp) -> RpcBasis {
    RpcBasis {
        snapshot,
        load_policy: connection.load_policy.clone(),
        policy_generation: connection.policy_generation,
        target_generation: connection.target_generation,
        attestation_generation: connection.attestation_generation,
    }
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

fn validate_commit(commit: &Commit) -> Result<(), AdminError> {
    let mut assets = BTreeSet::new();
    for mutation in &commit.assets {
        let uuid = match mutation {
            AssetMutation::Set { uuid, .. } | AssetMutation::Remove { uuid, .. } => *uuid,
        };
        if !assets.insert(uuid) {
            return Err(AdminError::DuplicateAssetMutation { uuid });
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

fn chunk_payload(payload: &ArtifactPayload, chunk_size: usize) -> ChunkStream {
    let mut chunks = VecDeque::new();
    push_chunks(
        &mut chunks,
        ArtifactChunkKind::Structural,
        &payload.structural,
        chunk_size,
    );
    for (index, blob) in payload.blobs.iter().enumerate() {
        push_chunks(
            &mut chunks,
            ArtifactChunkKind::Blob {
                index: index as u32,
            },
            blob,
            chunk_size,
        );
    }
    ChunkStream { chunks }
}

fn push_chunks(
    out: &mut VecDeque<ArtifactChunk>,
    kind: ArtifactChunkKind,
    bytes: &[u8],
    chunk_size: usize,
) {
    for (index, chunk) in bytes.chunks(chunk_size).enumerate() {
        out.push_back(ArtifactChunk {
            kind: kind.clone(),
            offset: (index * chunk_size) as u64,
            bytes: Arc::from(chunk),
        });
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
