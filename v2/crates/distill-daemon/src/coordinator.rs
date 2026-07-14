//! Single-writer daemon coordinator.
//!
//! Filesystem scans are candidates. Publication happens only through one
//! server-serialized closure which rechecks the durable store basis, applies
//! the complete SQLite input transaction, and returns the exact immutable RPC
//! projection for the same successor version.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use distill_build::pipeline::Target;
use distill_bundle::{AssetEntry, Bundle};
use distill_core::attestation::{is_bootstrap_control_type, SCHEMA_LINEAGE_MANIFEST_TYPE_UUID};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::lineage::AcceptedSchemaEpoch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AssetDeltaState, AssetMutation, AuthoringEntry, AuthoringEntryRole, AuthoringMutation,
    AuthoringValue, Commit, ConfigurationPoison, ConfigurationStatus, CoordinatedCommitError,
    DerivedOutputEntry, DriftedInput, LineageManifestClaimant, LineageRepairState, PathMutation,
    PipelineDiagnostic, Server, SnapshotStamp, StoredResolve, TargetDefinition, VersionPoison,
    VersionPoisonV1,
};
use distill_schema::ProjectSchemaAuthority;
use distill_store::bundles::{AssetRecord, BundleMeta};
use distill_store::config::{PendingRestart, RestartOnlyChange};
use distill_store::files::{FileKind, FileState};
use distill_store::pipeline::{
    AcceptedTypeLineage, SchemaLineageManifest, TypeAuthorityState, ValidatedPipelineEpoch,
    VerifiedSchemaLineageManifest,
};
use distill_store::state::{
    AssetClaimant, CleanupDisposition, ConfigurationState, DirectoryAliasSide, DscpV1,
    InputVersion, PipelinePoison, PipelinePoisonCode, PipelinePoisonOrigin,
    PipelineState as StoredPipelineState, PlatformFileIdentity, ReadableBundleSource,
    ScanFailureCode, ScanSubject, SkeletonFailureCode,
};
use distill_store::{Store, StoreConfig, StoreError};

use crate::authoring::{AuthoringFilesystemCandidate, AuthoringService, AuthoringServiceInitError};
use crate::callbacks::EpochAuthoringImporter;
use crate::epoch::{
    stored_pipeline_epoch, CandidateRequirements, ModuleHost, PipelineEpoch, PipelineSnapshot,
    UnloadOutcome,
};
use crate::lineage_repair::LineageRepairBackendInitError;
use crate::module_loader::DynamicPipelineModuleLoader;
use crate::pipeline_map::PipelineProjection;
use crate::scanner::{
    AssetRoot, ObservedFileIdentity, RootedScanner, ScanError, ScanSnapshot, ScannedFileKind,
};
use crate::scheduler::{Scheduler, SchedulerConfig};
use crate::watcher::{GenerationReplay, WatcherQueue, WatcherQueueError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageDestination {
    pub root: String,
    pub path: String,
}

pub struct DaemonCoordinator {
    store: Arc<Mutex<Store>>,
    scanner: RootedScanner,
    server: Server,
    lineage_destination: RwLock<LineageDestination>,
    authoring: Arc<AuthoringService>,
    pipeline: Mutex<CoordinatedPipelineRuntime>,
    schema_authority: RwLock<Option<Arc<ProjectSchemaAuthority>>>,
    build_targets: RwLock<BTreeMap<String, Target>>,
    configuration_poison: Mutex<Option<ConfigurationPoison>>,
    operational: Mutex<OperationalRuntime>,
}

struct OperationalRuntime {
    scheduler: Scheduler,
}

struct CoordinatedPipelineRuntime {
    host: ModuleHost,
    loader: DynamicPipelineModuleLoader,
    pending: Option<PipelineEpoch>,
}

enum PipelinePublication {
    Ready([u8; 32]),
    SchemaAcceptanceRequired,
    Poisoned(PipelinePoison),
}

enum ConfigurationPipelinePublication {
    Epoch {
        epoch: ValidatedPipelineEpoch,
        tools: BTreeMap<String, distill_store::pipeline::ToolCapsuleRegistrationV1>,
    },
    Poison(PipelinePoison),
}

fn discard_pending(runtime: &mut CoordinatedPipelineRuntime) {
    if let Some(pending) = runtime.pending.take() {
        let _ = runtime.host.discard_unpublished(pending);
    }
}

impl DaemonCoordinator {
    pub fn open(
        store_config: StoreConfig,
        roots: Vec<AssetRoot>,
        lineage_destination: LineageDestination,
        targets: Vec<TargetDefinition>,
        max_dependency_depth: usize,
    ) -> Result<Self, CoordinatorInitError> {
        let module_state_path = store_config.state_path.join("pipeline-host");
        let store = Arc::new(Mutex::new(Store::open(store_config.clone())?));
        let scanner = RootedScanner::new(roots.clone())?;
        let backend = Arc::new(AuthoringService::new(
            Arc::clone(&store),
            roots,
            lineage_destination.clone(),
        )?);
        let (instance, version) = {
            let store = lock_store(&store);
            (store.instance_id(), store.input_version())
        };
        let server = Server::new_at_version_with_authoring_backend(
            instance,
            version,
            targets,
            backend.clone(),
        )?;
        let bootstrap = distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1()
            .map_err(|error| CoordinatorInitError::Module(error.to_string()))?;
        let pipeline = CoordinatedPipelineRuntime {
            host: ModuleHost::new_with_bootstrap_authority(module_state_path, bootstrap)
                .map_err(CoordinatorInitError::ModuleIo)?,
            loader: DynamicPipelineModuleLoader,
            pending: None,
        };
        let operational = OperationalRuntime {
            scheduler: Scheduler::new(SchedulerConfig {
                parallelism: store_config.parallelism,
                batch_reserved_workers: store_config.batch_reserved_workers,
                max_dependency_depth,
            })
            .map_err(|error| CoordinatorInitError::Operational(error.to_string()))?,
        };
        Ok(Self {
            store,
            scanner,
            server,
            lineage_destination: RwLock::new(lineage_destination),
            authoring: backend,
            pipeline: Mutex::new(pipeline),
            schema_authority: RwLock::new(None),
            build_targets: RwLock::new(BTreeMap::new()),
            configuration_poison: Mutex::new(None),
            operational: Mutex::new(operational),
        })
    }

    pub fn server(&self) -> &Server {
        &self.server
    }

    /// Attach this coordinator as the RPC server's production lazy-build
    /// authority after it has been placed in its final `Arc`.
    pub fn attach_build_backend(self: &Arc<Self>) {
        self.server
            .install_build_backend(Arc::new(crate::build::CoordinatorBuildBackend::new(self)));
    }

    pub fn store(&self) -> Arc<Mutex<Store>> {
        Arc::clone(&self.store)
    }

    pub fn scanner(&self) -> RootedScanner {
        self.scanner.clone()
    }

    pub fn authoring_service(&self) -> &Arc<AuthoringService> {
        &self.authoring
    }

    pub fn pipeline_snapshot(&self) -> PipelineSnapshot {
        lock_pipeline(&self.pipeline).host.snapshot()
    }

    /// Persist the first runtime poison latched by a published callback
    /// without minting a new input version. The in-memory epoch token fences
    /// work immediately; this closes the crash/restart durability side of the
    /// same monotonic transition.
    pub(crate) fn sync_runtime_pipeline_poison(
        &self,
    ) -> Result<Option<PipelinePoison>, CoordinatorError> {
        let observed = {
            let runtime = lock_pipeline(&self.pipeline);
            let Some(epoch) = runtime.host.published_ready_epoch() else {
                return Ok(None);
            };
            match runtime.host.snapshot().epoch() {
                Ok(_) => return Ok(None),
                Err(poison) if poison.origin == PipelinePoisonOrigin::PublishedRuntime => {
                    (epoch.dylib_hash(), poison)
                }
                Err(_) => return Ok(None),
            }
        };
        let mut store = lock_store(&self.store);
        match store.poison_published_pipeline_epoch(observed.0, &observed.1) {
            Ok(()) => Ok(Some(observed.1)),
            Err(StoreError::StalePublishedPipeline {
                actual: Some(actual),
                already_unavailable: true,
                ..
            }) if actual == observed.0 => Ok(Some(observed.1)),
            Err(error) => Err(CoordinatorError::RuntimePipeline(error.to_string())),
        }
    }

    pub fn schema_authority(&self) -> Option<Arc<ProjectSchemaAuthority>> {
        self.schema_authority
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn build_target(&self, name: &str) -> Option<Target> {
        self.build_targets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn install_schema_authority_for_test(&self, authority: Arc<ProjectSchemaAuthority>) {
        *self
            .schema_authority
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(authority);
    }

    #[cfg(test)]
    pub(crate) fn install_build_target_for_test(&self, name: &str, target: Target) {
        self.build_targets
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_owned(), target);
    }

    #[cfg(test)]
    pub(crate) fn install_pipeline_epoch_for_test(&self, epoch: PipelineEpoch) {
        lock_pipeline(&self.pipeline).host.install_ready(epoch);
    }

    pub fn operational_configuration(&self) -> SchedulerConfig {
        self.operational
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .scheduler
            .config()
    }

    pub fn apply_operational_configuration(
        &self,
        store_config: &StoreConfig,
        max_dependency_depth: usize,
    ) -> Result<(), CoordinatorError> {
        let scheduler = Scheduler::new(SchedulerConfig {
            parallelism: store_config.parallelism,
            batch_reserved_workers: store_config.batch_reserved_workers,
            max_dependency_depth,
        })
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        lock_store(&self.store)
            .apply_operational_config(store_config)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        self.operational
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .scheduler = scheduler;
        Ok(())
    }

    pub fn stage_restart_configuration(
        &self,
        changes: &[RestartOnlyChange],
    ) -> Result<PendingRestart, CoordinatorError> {
        let pending = lock_store(&self.store)
            .stage_pending_restart(changes)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        self.server.restart_required(pending.keys.clone());
        Ok(pending)
    }

    fn configuration_poison(&self) -> Option<ConfigurationPoison> {
        self.configuration_poison
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn lineage_destination(&self) -> LineageDestination {
        self.lineage_destination
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Publish a rejected configuration source as an ordinary input version.
    /// The overlay participates in every concurrent scan classification, so a
    /// scan failure cannot erase the candidate's typed configuration reason.
    pub fn publish_configuration_rejection(
        &self,
        reason: DscpV1,
        message: impl Into<String>,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let poison = ConfigurationPoison::from_reason(&reason, message);
        let previous = {
            let mut current = self
                .configuration_poison
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            current.replace(poison)
        };
        match self.reconcile_full_scan() {
            Ok(stamp) => Ok(stamp),
            Err(error) => {
                *self
                    .configuration_poison
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = previous;
                Err(error)
            }
        }
    }

    pub fn heal_configuration_rejection(&self) -> Result<SnapshotStamp, CoordinatorError> {
        let previous = self
            .configuration_poison
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        match self.reconcile_full_scan() {
            Ok(stamp) => Ok(stamp),
            Err(error) => {
                *self
                    .configuration_poison
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = previous;
                Err(error)
            }
        }
    }

    pub fn publish_configuration_candidate(
        &self,
        roots: Vec<AssetRoot>,
        lineage_destination: LineageDestination,
        targets: Vec<TargetDefinition>,
        build_targets: BTreeMap<String, Target>,
        pipeline_source: &std::path::Path,
        mut requirements: CandidateRequirements,
        schema_authority: Arc<ProjectSchemaAuthority>,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let filesystem = self
            .authoring
            .prepare_filesystem_candidate(roots, lineage_destination)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let scan = filesystem.scanner().scan()?;
        let destination = filesystem.lineage_destination().clone();
        let candidate = ScanCandidate::build(filesystem.scanner(), &destination, scan, None)?;
        let mut runtime = lock_pipeline(&self.pipeline);
        let prepared_epoch = {
            let CoordinatedPipelineRuntime { host, loader, .. } = &mut *runtime;
            host.prepare_candidate(pipeline_source, &mut requirements, loader)
        };
        let authored_types = requirements
            .compiled_types
            .rows
            .iter()
            .map(|row| row.type_uuid)
            .collect::<Vec<_>>();
        let (pipeline, mut prepared_epoch, projection) = match prepared_epoch {
            Ok(prepared) => {
                match PipelineProjection::build(
                    prepared.processor_descriptors(),
                    prepared.dylib_hash(),
                    &build_targets,
                    authored_types,
                ) {
                    Err(error) => {
                        let cleanup = runtime.host.discard_unpublished(prepared);
                        let poison = cleanup.unwrap_or_else(|| {
                            PipelinePoison::new(
                                PipelinePoisonCode::CandidateRegistration,
                                PipelinePoisonOrigin::CandidateOpen,
                                CleanupDisposition::CleanedAndClosed,
                                format!("invalid target-resolved pipeline map: {error:?}"),
                            )
                            .expect("candidate-registration cleanup tuple is valid")
                        });
                        (
                            ConfigurationPipelinePublication::Poison(poison),
                            None,
                            PipelineProjection::default(),
                        )
                    }
                    Ok(projection) => {
                        let stored = match stored_pipeline_epoch(&prepared, &requirements) {
                            Ok(stored) => stored,
                            Err(error) => {
                                let _ = runtime.host.discard_unpublished(prepared);
                                return Err(CoordinatorError::InvalidManifest(error.to_string()));
                            }
                        };
                        if let Err(error) = self.authoring.prepare_pipeline_importers(
                            EpochAuthoringImporter::metadata_only(prepared.importer_descriptors()),
                        ) {
                            let _ = runtime.host.discard_unpublished(prepared);
                            return Err(CoordinatorError::InvalidManifest(format!("{error:?}")));
                        }
                        let tools = prepared.tool_epoch();
                        (
                            ConfigurationPipelinePublication::Epoch {
                                epoch: stored,
                                tools,
                            },
                            Some(prepared),
                            projection,
                        )
                    }
                }
            }
            Err(poison) => (
                ConfigurationPipelinePublication::Poison(poison),
                None,
                PipelineProjection::default(),
            ),
        };
        let filesystem = Arc::new(Mutex::new(Some(filesystem)));
        let captured = Arc::clone(&filesystem);
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        let authoring = Arc::clone(&self.authoring);
        self.server
            .coordinated_replace_target_set(base, targets, || {
                let commit =
                    publish_scan(&store, base, candidate, true, Some(&pipeline), &projection)
                        .map_err(|error| error.to_string())?;
                let stored_pipeline = lock_store(&store)
                    .pipeline_state()
                    .map_err(|error| error.to_string())?
                    .expect("configuration candidate published pipeline state");
                let filesystem: AuthoringFilesystemCandidate = captured
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .expect("configuration candidate installs once");
                self.scanner.replace_from(filesystem.scanner());
                authoring.install_filesystem_candidate(filesystem);
                *self
                    .lineage_destination
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = destination;
                *self
                    .schema_authority
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(Arc::clone(&schema_authority));
                *self
                    .build_targets
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = build_targets.clone();
                authoring.install_pipeline_projection(projection.clone());
                self.configuration_poison
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                match (&pipeline, prepared_epoch.take(), stored_pipeline) {
                    (
                        ConfigurationPipelinePublication::Epoch { .. },
                        Some(prepared),
                        StoredPipelineState::Ready(epoch),
                    ) if epoch.dylib_hash == prepared.dylib_hash() => {
                        let importers = EpochAuthoringImporter::all(&prepared);
                        discard_pending(&mut runtime);
                        runtime.host.install_ready(prepared);
                        authoring
                            .replace_pipeline_importers(importers)
                            .expect("candidate importer metadata was prevalidated");
                    }
                    (
                        ConfigurationPipelinePublication::Epoch { .. },
                        Some(prepared),
                        StoredPipelineState::SchemaAcceptanceRequired { .. },
                    ) => {
                        discard_pending(&mut runtime);
                        let fence = PipelinePoison::new(
                            PipelinePoisonCode::CandidateValidation,
                            PipelinePoisonOrigin::CandidateOpen,
                            CleanupDisposition::None,
                            "pipeline candidate requires explicit schema acceptance",
                        )
                        .expect("schema-acceptance fence is valid");
                        runtime.host.install_poison(fence);
                        runtime.pending = Some(prepared);
                        authoring.install_pipeline_importers(BTreeMap::new());
                    }
                    (
                        ConfigurationPipelinePublication::Epoch { .. },
                        Some(prepared),
                        StoredPipelineState::Poisoned { error, .. },
                    ) => {
                        let _ = runtime.host.discard_unpublished(prepared);
                        discard_pending(&mut runtime);
                        runtime.host.install_poison(error);
                        authoring.install_pipeline_importers(BTreeMap::new());
                    }
                    (
                        ConfigurationPipelinePublication::Poison(poison),
                        None,
                        StoredPipelineState::Poisoned { .. },
                    ) => {
                        discard_pending(&mut runtime);
                        runtime.host.install_poison(poison.clone());
                        authoring.install_pipeline_importers(BTreeMap::new());
                    }
                    _ => unreachable!(
                        "durable configuration pipeline state must match its prepared candidate"
                    ),
                }
                Ok(commit)
            })
            .map_err(CoordinatorError::Coordinated)
    }

    /// Stage, attest, durably publish, and only then expose one pipeline
    /// candidate. The store transaction and RPC commit share the exact base
    /// version, so scanner or authoring work cannot split the epoch.
    pub fn publish_pipeline_candidate(
        &self,
        source: &std::path::Path,
        mut requirements: CandidateRequirements,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let base = self.server.current_stamp().version;
        let mut runtime = lock_pipeline(&self.pipeline);
        let prepared = {
            let CoordinatedPipelineRuntime { host, loader, .. } = &mut *runtime;
            host.prepare_candidate(source, &mut requirements, loader)
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(poison) => {
                drop(runtime);
                return self.publish_pipeline_rejection(poison);
            }
        };

        let stored = match stored_pipeline_epoch(&prepared, &requirements) {
            Ok(stored) => stored,
            Err(error) => {
                let _ = runtime.host.discard_unpublished(prepared);
                return Err(CoordinatorError::InvalidManifest(error.to_string()));
            }
        };
        if let Err(error) =
            self.authoring
                .prepare_pipeline_importers(EpochAuthoringImporter::metadata_only(
                    prepared.importer_descriptors(),
                ))
        {
            let _ = runtime.host.discard_unpublished(prepared);
            return Err(CoordinatorError::InvalidManifest(format!("{error:?}")));
        }
        let publication = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&publication);
        let store = Arc::clone(&self.store);
        let tools = prepared.tool_epoch();
        let result = self.server.coordinated_commit(base, || {
            let mut store = lock_store(&store);
            if store.input_version() != base {
                return Err(format!(
                    "durable pipeline basis is {:?}, expected {base:?}",
                    store.input_version()
                ));
            }
            store
                .input_transaction(|transaction| {
                    if transaction.publish_pipeline_epoch(&stored)? {
                        transaction.publish_tool_epoch(&tools)?;
                    }
                    Ok(())
                })
                .map_err(|error| error.to_string())?;
            let state = store
                .pipeline_state()
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "pipeline publication produced no durable state".to_owned())?;
            let (diagnostic, result) = match state {
                StoredPipelineState::Ready(epoch) => (
                    PipelineDiagnostic::Ready,
                    PipelinePublication::Ready(epoch.dylib_hash),
                ),
                StoredPipelineState::SchemaAcceptanceRequired { required, .. } => (
                    PipelineDiagnostic::SchemaAcceptanceRequired(required.clone()),
                    PipelinePublication::SchemaAcceptanceRequired,
                ),
                StoredPipelineState::Poisoned { error, .. } => (
                    PipelineDiagnostic::Poisoned(error.clone()),
                    PipelinePublication::Poisoned(error),
                ),
            };
            *lock_publication(&captured) = Some(result);
            Ok(Commit {
                pipeline: Some(diagnostic),
                ..Commit::default()
            })
        });
        let stamp = match result {
            Ok(stamp) => stamp,
            Err(error) => {
                let _ = runtime.host.discard_unpublished(prepared);
                return Err(CoordinatorError::Coordinated(error));
            }
        };
        match lock_publication(&publication)
            .take()
            .expect("coordinated pipeline publication captured its durable state")
        {
            PipelinePublication::Ready(hash) if hash == prepared.dylib_hash() => {
                let importers = EpochAuthoringImporter::all(&prepared);
                discard_pending(&mut runtime);
                runtime.host.install_ready(prepared);
                self.authoring
                    .replace_pipeline_importers(importers)
                    .expect("candidate importer metadata was prevalidated");
            }
            PipelinePublication::SchemaAcceptanceRequired => {
                discard_pending(&mut runtime);
                let fence = PipelinePoison::new(
                    PipelinePoisonCode::CandidateValidation,
                    PipelinePoisonOrigin::CandidateOpen,
                    CleanupDisposition::None,
                    "pipeline candidate requires explicit schema acceptance",
                )
                .expect("schema-acceptance fence is a valid candidate poison");
                runtime.host.install_poison(fence);
                runtime.pending = Some(prepared);
                self.authoring.install_pipeline_importers(BTreeMap::new());
            }
            PipelinePublication::Poisoned(poison) => {
                let _ = runtime.host.discard_unpublished(prepared);
                discard_pending(&mut runtime);
                runtime.host.install_poison(poison);
                self.authoring.install_pipeline_importers(BTreeMap::new());
            }
            PipelinePublication::Ready(_) => {
                let _ = runtime.host.discard_unpublished(prepared);
                return Err(CoordinatorError::InvalidManifest(
                    "durable pipeline hash differs from the prepared module".to_owned(),
                ));
            }
        }
        Ok(stamp)
    }

    /// Publish a candidate-input failure which occurs before a module can be
    /// opened (for example, malformed watched schema JSON). The last durable
    /// schema/target projection remains intact, while the new input version is
    /// explicitly pipeline-poisoned and the live module is fenced.
    pub fn publish_pipeline_rejection(
        &self,
        poison: PipelinePoison,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        let diagnostic = poison.clone();
        let result = self.server.coordinated_commit(base, || {
            let mut store = lock_store(&store);
            if store.input_version() != base {
                return Err(format!(
                    "durable pipeline-poison basis is {:?}, expected {base:?}",
                    store.input_version()
                ));
            }
            store
                .input_transaction(|transaction| transaction.publish_pipeline_poison(&diagnostic))
                .map_err(|error| error.to_string())?;
            Ok(Commit {
                pipeline: Some(PipelineDiagnostic::Poisoned(diagnostic.clone())),
                ..Commit::default()
            })
        });
        match result {
            Ok(stamp) => {
                let mut runtime = lock_pipeline(&self.pipeline);
                discard_pending(&mut runtime);
                runtime.host.install_poison(poison);
                self.authoring.install_pipeline_importers(BTreeMap::new());
                Ok(stamp)
            }
            Err(error) => Err(CoordinatorError::Coordinated(error)),
        }
    }

    pub fn reap_retired_pipeline_epochs(&self) -> Vec<UnloadOutcome> {
        lock_pipeline(&self.pipeline).host.reap_retired()
    }

    /// Reconcile one complete scan. A watcher generation is armed before the
    /// caller starts this method; queued events are unioned through
    /// [`Self::apply_watcher_batch`] after this transaction.
    pub fn reconcile_full_scan(&self) -> Result<SnapshotStamp, CoordinatorError> {
        match self.scanner.scan() {
            Ok(scan) => self.publish_scan(scan),
            Err(error) => self.publish_scan_rejection(&error),
        }
    }

    /// Arm a watcher generation before traversal, publish the startup scan,
    /// then replay the exact event union. Overflow repeats a fully armed scan;
    /// it can never degrade into a partial event set.
    pub fn reconcile_startup(
        &self,
        watcher: &Mutex<WatcherQueue>,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let mut stamp;
        loop {
            let generation = lock_watcher(watcher).arm_scan()?;
            stamp = self.reconcile_full_scan()?;
            match lock_watcher(watcher).finish_scan(generation)? {
                GenerationReplay::Events(events) => {
                    if !events.is_empty() {
                        stamp = self.apply_watcher_batch(events)?;
                    }
                    return Ok(stamp);
                }
                GenerationReplay::FullRescan => continue,
            }
        }
    }

    fn publish_scan(&self, scan: ScanSnapshot) -> Result<SnapshotStamp, CoordinatorError> {
        let candidate = ScanCandidate::build(
            &self.scanner,
            &self.lineage_destination(),
            scan,
            self.configuration_poison(),
        )?;
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        let projection = self.authoring.pipeline_projection();
        self.server
            .coordinated_commit(base, || {
                publish_scan(&store, base, candidate, false, None, &projection)
                    .map_err(|error| error.to_string())
            })
            .map_err(CoordinatorError::Coordinated)
    }

    fn publish_scan_rejection(&self, error: &ScanError) -> Result<SnapshotStamp, CoordinatorError> {
        let rejection = classify_scan_rejection(&self.scanner, error)?;
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        self.server
            .coordinated_commit(base, || {
                let mut store = lock_store(&store);
                if store.input_version() != base {
                    return Err(format!(
                        "durable rejected-scan basis is {:?}, expected {base:?}",
                        store.input_version()
                    ));
                }
                let commit = match &rejection {
                    ScanRejection::Version(poison) => {
                        let configuration = self.configuration_poison();
                        store
                            .input_transaction(|transaction| {
                                transaction.set_version_poisons([poison.clone()])?;
                                if let Some(configuration) = &configuration {
                                    transaction.publish_configuration_poison(
                                        &configuration.detail,
                                        &configuration.message,
                                    )?;
                                }
                                Ok(())
                            })
                            .map_err(|error| error.to_string())?;
                        Commit {
                            configuration: configuration.map(ConfigurationStatus::Poisoned),
                            version_poison: Some(Some(poison.clone())),
                            ..Commit::default()
                        }
                    }
                    ScanRejection::Configuration { reason, message } => {
                        let observed = ConfigurationPoison::from_reason(reason, message);
                        let poison = ConfigurationPoison::select_canonical(
                            self.configuration_poison().into_iter().chain([observed]),
                        )
                        .map_err(|error| error.to_string())?
                        .expect("one scan configuration poison");
                        store
                            .input_transaction(|transaction| {
                                transaction.set_version_poisons([])?;
                                transaction
                                    .publish_configuration_poison(&poison.detail, &poison.message)
                            })
                            .map_err(|error| error.to_string())?;
                        Commit {
                            configuration: Some(ConfigurationStatus::Poisoned(poison)),
                            version_poison: Some(None),
                            ..Commit::default()
                        }
                    }
                };
                Ok(commit)
            })
            .map_err(CoordinatorError::Coordinated)
    }

    /// Reconcile a watcher batch as one durable input event. Event paths are a
    /// trigger/union, never trusted as a complete namespace; the identity-
    /// checked scan supplies current bytes and the single coordinated commit
    /// consumes the change without an empty queue-only version in front of it.
    pub fn apply_watcher_batch(
        &self,
        events: impl IntoIterator<Item = WatcherPathEvent>,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let mut events = events.into_iter().collect::<Vec<_>>();
        events.sort();
        events.dedup();
        if events.is_empty() {
            return Ok(self.server.current_stamp());
        }
        let scan = self.scanner.scan()?;
        self.publish_scan(scan)
    }

    /// Rerun watched imports whose complete outcome-bearing basis drifted.
    /// Each bundle publishes as its own version so a later conflict cannot
    /// roll back an earlier per-file success.
    pub fn reconcile_watched_imports(&self) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let pending = self
            .authoring
            .watched_imports_needing_reimport()
            .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        let mut imported = Vec::with_capacity(pending.len());
        for bundle in pending {
            let base = self.server.current_stamp().version;
            let authoring = Arc::clone(&self.authoring);
            let publication = self
                .server
                .coordinated_maybe_commit(base, || {
                    authoring
                        .prepare_watched_reimport(base, bundle)
                        .map(|prepared| prepared.map(|prepared| prepared.commit))
                        .map_err(|error| format!("{error:?}"))
                })
                .map_err(CoordinatorError::Coordinated)?;
            if publication.is_some() {
                imported.push(bundle);
            }
        }
        Ok(imported)
    }

    /// Discover and apply authored directory-import rules. Every generated
    /// bundle is a separate journaled/versioned fold; orphaned prior outputs
    /// are deliberately retained and therefore never appear as deletion work.
    pub fn reconcile_directory_imports(&self) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let tasks = self
            .authoring
            .directory_import_tasks()
            .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        let mut imported = Vec::with_capacity(tasks.len());
        for task in tasks {
            let base = self.server.current_stamp().version;
            let authoring = Arc::clone(&self.authoring);
            let bundle = Arc::new(Mutex::new(None));
            let captured = Arc::clone(&bundle);
            let publication = self
                .server
                .coordinated_maybe_commit(base, || {
                    let prepared = authoring
                        .prepare_watched_directory_import(base, &task)
                        .map_err(|error| format!("{error:?}"))?;
                    let Some(prepared) = prepared else {
                        return Ok(None);
                    };
                    *captured
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(prepared.bundle);
                    Ok(Some(prepared.commit))
                })
                .map_err(CoordinatorError::Coordinated)?;
            if publication.is_some() {
                imported.push(
                    bundle
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .expect("coordinated directory import captured its bundle"),
                );
            }
        }
        Ok(imported)
    }
}

enum ScanRejection {
    Version(VersionPoison),
    Configuration {
        reason: Box<DscpV1>,
        message: String,
    },
}

fn classify_scan_rejection(
    scanner: &RootedScanner,
    error: &ScanError,
) -> Result<ScanRejection, CoordinatorError> {
    if let ScanError::DirectoryAlias {
        first_root,
        first,
        second_root,
        second,
        identity,
    } = error
    {
        let identity = platform_identity(*identity);
        return Ok(ScanRejection::Configuration {
            reason: Box::new(DscpV1::DirectoryAlias {
                first: DirectoryAliasSide {
                    normalized_path: scanner.normalized_observed_path(first_root, first),
                    identity,
                },
                second: DirectoryAliasSide {
                    normalized_path: scanner.normalized_observed_path(second_root, second),
                    identity,
                },
            }),
            message: error.to_string(),
        });
    }
    let (path, failure) = match error {
        ScanError::Io { path, source } => (
            Some(path.as_path()),
            match source.kind() {
                std::io::ErrorKind::PermissionDenied => ScanFailureCode::PermissionDenied,
                std::io::ErrorKind::NotFound => ScanFailureCode::NotFound,
                _ => ScanFailureCode::IoDataLoss,
            },
        ),
        ScanError::RootUnavailable { .. } => (None, ScanFailureCode::NotFound),
        ScanError::NonRegularFile { path } => {
            (Some(path.as_path()), ScanFailureCode::InvalidFileType)
        }
        ScanError::FileIdentityChanged { path }
        | ScanError::DirectoryCycle { path }
        | ScanError::SymlinkEscape { path, .. } => (
            Some(path.as_path()),
            ScanFailureCode::SymlinkIdentityChanged,
        ),
        ScanError::InvalidLogicalPath(_) => (None, ScanFailureCode::InvalidFileType),
        ScanError::InvalidRootName(_)
        | ScanError::DuplicateRootName(_)
        | ScanError::UnknownRoot(_)
        | ScanError::DirectoryAlias { .. } => (None, ScanFailureCode::IoDataLoss),
    };
    let subject = path
        .and_then(|path| scanner.scan_subject(path))
        .or_else(|| scanner.first_root_subject())
        .unwrap_or_else(|| ScanSubject::Root {
            root_name: "unconfigured".to_owned(),
        });
    let detail = VersionPoisonV1::UnreadableScanSubtree { subject, failure };
    let poison = VersionPoison::new(detail, error.to_string())
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
    Ok(ScanRejection::Version(poison))
}

fn platform_identity(identity: ObservedFileIdentity) -> PlatformFileIdentity {
    match identity {
        #[cfg(unix)]
        ObservedFileIdentity::Unix { device, inode } => {
            PlatformFileIdentity::Unix { device, inode }
        }
        #[cfg(windows)]
        ObservedFileIdentity::Windows {
            volume_serial,
            file_id,
        } => PlatformFileIdentity::Windows {
            volume_serial,
            file_id,
        },
        #[cfg(not(any(unix, windows)))]
        ObservedFileIdentity::Portable => PlatformFileIdentity::Unix {
            device: 0,
            inode: 0,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WatcherPathEvent {
    pub root: String,
    pub path: String,
    pub exists: bool,
}

#[derive(Debug)]
pub enum CoordinatorInitError {
    Store(StoreError),
    Scan(ScanError),
    Repair(LineageRepairBackendInitError),
    Authoring(AuthoringServiceInitError),
    Rpc(distill_rpc::AttestationShapeError),
    Module(String),
    ModuleIo(std::io::Error),
    Operational(String),
}

impl std::fmt::Display for CoordinatorInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "daemon coordinator initialization: {self:?}")
    }
}

impl std::error::Error for CoordinatorInitError {}

impl From<StoreError> for CoordinatorInitError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}
impl From<ScanError> for CoordinatorInitError {
    fn from(error: ScanError) -> Self {
        Self::Scan(error)
    }
}
impl From<LineageRepairBackendInitError> for CoordinatorInitError {
    fn from(error: LineageRepairBackendInitError) -> Self {
        Self::Repair(error)
    }
}
impl From<AuthoringServiceInitError> for CoordinatorInitError {
    fn from(error: AuthoringServiceInitError) -> Self {
        Self::Authoring(error)
    }
}
impl From<distill_rpc::AttestationShapeError> for CoordinatorInitError {
    fn from(error: distill_rpc::AttestationShapeError) -> Self {
        Self::Rpc(error)
    }
}

#[derive(Debug)]
pub enum CoordinatorError {
    Scan(ScanError),
    Watcher(WatcherQueueError),
    InvalidManifest(String),
    Coordinated(CoordinatedCommitError),
    RuntimePipeline(String),
}

impl std::fmt::Display for CoordinatorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "daemon coordinator: {self:?}")
    }
}

impl std::error::Error for CoordinatorError {}

impl From<ScanError> for CoordinatorError {
    fn from(error: ScanError) -> Self {
        Self::Scan(error)
    }
}

impl From<WatcherQueueError> for CoordinatorError {
    fn from(error: WatcherQueueError) -> Self {
        Self::Watcher(error)
    }
}

struct ScanCandidate {
    scan: ScanSnapshot,
    version_poison: Option<VersionPoison>,
    configuration: ConfigurationStatus,
    lineage_repair: Option<LineageRepairState>,
    lineage_manifest: Option<VerifiedSchemaLineageManifest>,
}

impl ScanCandidate {
    fn build(
        scanner: &RootedScanner,
        destination: &LineageDestination,
        scan: ScanSnapshot,
        external_poison: Option<ConfigurationPoison>,
    ) -> Result<Self, CoordinatorError> {
        let mut poisons = Vec::new();
        for bundle in &scan.bundles {
            if let Err(error) = &bundle.parsed {
                let source = readable_source(bundle);
                let detail = VersionPoisonV1::IncompleteSkeleton {
                    source,
                    failure: skeleton_failure(error),
                };
                poisons.push(
                    VersionPoison::new(detail, error.to_string())
                        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?,
                );
            }
        }

        let parsed = scan
            .bundles
            .iter()
            .filter_map(|source| source.parsed.as_ref().ok().map(|bundle| (source, bundle)))
            .collect::<Vec<_>>();
        let mut bundles = BTreeMap::<BundleUuid, Vec<ReadableBundleSource>>::new();
        let mut assets = BTreeMap::<AssetUuid, Vec<AssetClaimant>>::new();
        for (source, bundle) in &parsed {
            bundles
                .entry(bundle.uuid)
                .or_default()
                .push(readable_source(source));
            for (local_id, entry) in &bundle.assets {
                assets
                    .entry(entry.uuid)
                    .or_default()
                    .push(AssetClaimant::Authored {
                        source: readable_source(source),
                        bundle: bundle.uuid,
                        local_id: local_id.clone(),
                    });
            }
        }
        for (bundle, mut sources) in bundles {
            if sources.len() > 1 {
                sources.sort();
                poisons.push(
                    VersionPoison::new(
                        VersionPoisonV1::DuplicateBundleUuid { bundle, sources },
                        format!("duplicate bundle UUID {bundle}"),
                    )
                    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?,
                );
            }
        }
        for (asset, mut claimants) in assets {
            if claimants.len() > 1 {
                claimants.sort();
                poisons.push(
                    VersionPoison::new(
                        VersionPoisonV1::DuplicateAssetUuid { asset, claimants },
                        format!("duplicate asset UUID {asset}"),
                    )
                    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?,
                );
            }
        }
        let version_poison = VersionPoison::select_canonical(poisons)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;

        let mut claimants = lineage_claimants(&parsed);
        claimants.sort();
        claimants.dedup();
        let (lineage_configuration, mut lineage_repair, lineage_manifest) =
            match claimants.as_slice() {
                [] => {
                    let reason = DscpV1::MissingLineageManifest;
                    let poison = distill_rpc::ConfigurationPoison::from_reason(
                        &reason,
                        "the unique SchemaLineageManifest is missing",
                    );
                    let repair = LineageRepairState::Missing {
                        configured_root: destination.root.clone(),
                        configured_path: destination.path.clone(),
                        destination: scanner
                            .inspect_destination(&destination.root, &destination.path)?,
                    };
                    (ConfigurationStatus::Poisoned(poison), Some(repair), None)
                }
                [claimant] => {
                    let (source, bundle) = parsed
                        .iter()
                        .find(|(source, bundle)| {
                            source.root_name == claimant.root_name
                                && source.normalized_path == claimant.normalized_path
                                && bundle.uuid == claimant.bundle
                        })
                        .expect("claimant came from parsed scan");
                    let entry = &bundle.assets[&claimant.local_id];
                    let manifest =
                        decode_lineage_manifest(&entry.data, ContentHash(source.file_hash.0))?;
                    (ConfigurationStatus::Ready, None, Some(manifest))
                }
                _ => {
                    let reason = DscpV1::DuplicateLineageManifest {
                        entries: claimants.clone(),
                    };
                    let poison = distill_rpc::ConfigurationPoison::from_reason(
                        &reason,
                        "multiple SchemaLineageManifest entries claim authority",
                    );
                    (
                        ConfigurationStatus::Poisoned(poison),
                        Some(LineageRepairState::Duplicate { claimants }),
                        None,
                    )
                }
            };

        let configuration = match lineage_configuration {
            ConfigurationStatus::Ready => {
                external_poison.map_or(ConfigurationStatus::Ready, ConfigurationStatus::Poisoned)
            }
            ConfigurationStatus::Poisoned(lineage_poison) => {
                let selected = ConfigurationPoison::select_canonical(
                    external_poison.into_iter().chain([lineage_poison.clone()]),
                )
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?
                .expect("lineage supplied one configuration poison");
                if selected.reason_hash != lineage_poison.reason_hash {
                    lineage_repair = None;
                }
                ConfigurationStatus::Poisoned(selected)
            }
        };

        Ok(Self {
            scan,
            version_poison,
            configuration,
            lineage_repair,
            lineage_manifest,
        })
    }
}

fn projected_derived_outputs(
    candidate: &ScanCandidate,
    projection: &PipelineProjection,
) -> Result<
    (
        BTreeMap<AssetUuid, DerivedOutputEntry>,
        Option<VersionPoison>,
    ),
    StoreError,
> {
    let mut claims = BTreeMap::<AssetUuid, Vec<AssetClaimant>>::new();
    let mut outputs = BTreeMap::<AssetUuid, DerivedOutputEntry>::new();
    for source in &candidate.scan.bundles {
        let Ok(bundle) = &source.parsed else {
            continue;
        };
        for (local_id, entry) in &bundle.assets {
            claims
                .entry(entry.uuid)
                .or_default()
                .push(AssetClaimant::Authored {
                    source: readable_source(source),
                    bundle: bundle.uuid,
                    local_id: local_id.clone(),
                });
            for (child, output) in projection.derived_outputs([(entry.uuid, entry.type_uuid)]) {
                let claimant = AssetClaimant::Derived {
                    parent: entry.uuid,
                    output_key: output.output_key.clone(),
                };
                claims.entry(child).or_default().push(claimant);
                if let Some(existing) = outputs.insert(child, output.clone()) {
                    if existing != output {
                        // The claimant table below publishes the closed typed
                        // collision; keeping either row here is harmless
                        // because poisoned versions expose no namespace.
                    }
                }
            }
        }
    }

    let mut poisons = Vec::new();
    for (asset, mut claimants) in claims {
        claimants.sort();
        claimants.dedup();
        if claimants.len() > 1 {
            poisons.push(
                VersionPoison::new(
                    VersionPoisonV1::DuplicateAssetUuid { asset, claimants },
                    format!("authored/derived asset UUID collision at {asset}"),
                )
                .map_err(|error| StoreError::InvalidConfiguration {
                    error: format!("invalid derived-output collision: {error}"),
                })?,
            );
        }
    }
    let poison = VersionPoison::select_canonical(poisons).map_err(|error| {
        StoreError::InvalidConfiguration {
            error: format!("invalid derived-output collision set: {error}"),
        }
    })?;
    Ok((outputs, poison))
}

fn publish_scan(
    store: &Arc<Mutex<Store>>,
    base: InputVersion,
    mut candidate: ScanCandidate,
    advance_configuration: bool,
    pipeline: Option<&ConfigurationPipelinePublication>,
    projection: &PipelineProjection,
) -> Result<Commit, StoreError> {
    let (derived_outputs, derived_poison) = projected_derived_outputs(&candidate, projection)?;
    candidate.version_poison = VersionPoison::select_canonical(
        candidate
            .version_poison
            .take()
            .into_iter()
            .chain(derived_poison),
    )
    .map_err(|error| StoreError::InvalidConfiguration {
        error: format!("invalid derived-output collision poison: {error}"),
    })?;
    let mut store = lock_store(store);
    if store.input_version() != base {
        return Err(StoreError::InvalidConfiguration {
            error: format!(
                "durable scan basis is {:?}, expected {base:?}",
                store.input_version()
            ),
        });
    }
    let old_files = store.all_files()?;
    let old_bundles = store.all_bundles()?;
    let old_assets = store.all_asset_ids()?;
    let old_paths = store.all_path_entries()?;
    let mut generation = match store.configuration_state()? {
        ConfigurationState::Ready(epoch) => epoch.generation,
        ConfigurationState::Poisoned { last_good, .. } => {
            last_good.map_or(0, |epoch| epoch.generation)
        }
    };
    if advance_configuration {
        generation = generation
            .checked_add(1)
            .ok_or_else(|| StoreError::InvalidConfiguration {
                error: "configuration generation exhausted".to_owned(),
            })?;
    }

    let mut commit = rpc_commit(
        &candidate,
        &old_assets,
        &old_paths,
        projection,
        derived_outputs.clone(),
    )?;
    store.input_transaction(|transaction| {
        let mut root_ids = BTreeMap::new();
        let mut scanned_keys = BTreeSet::new();
        let mut newest_mtime = 0;
        for file in &candidate.scan.files {
            let root = *root_ids
                .entry(file.root_name.clone())
                .or_insert(transaction.intern_root(&file.root_name)?);
            let state = file_state(file);
            scanned_keys.insert((root, file.normalized_path.clone()));
            newest_mtime = newest_mtime.max(file.modified_nanos);
            let changed = old_files.iter().find_map(|(old_root, path, old)| {
                (*old_root == root && path == &file.normalized_path).then_some(old != &state)
            });
            transaction.upsert_file(root, &file.normalized_path, &state)?;
            if changed.unwrap_or(true) {
                transaction.push_dirty(root, &file.normalized_path, true)?;
            }
        }
        for (root, path, _) in &old_files {
            if !scanned_keys.contains(&(*root, path.clone())) {
                transaction.remove_file(*root, path)?;
                transaction.push_dirty(*root, path, false)?;
            }
        }
        transaction.set_clean_watermark(newest_mtime)?;
        transaction.set_version_poisons(candidate.version_poison.clone())?;
        transaction.clear_derived_outputs()?;
        if candidate.version_poison.is_none() {
            for (child, output) in &derived_outputs {
                transaction.set_derived_output(*child, output.parent, &output.output_key)?;
            }
        }

        match &candidate.configuration {
            ConfigurationStatus::Ready => transaction.publish_configuration_ready(generation)?,
            ConfigurationStatus::Poisoned(poison) => {
                transaction.publish_configuration_poison(&poison.detail, &poison.message)?
            }
        }
        if let Some(manifest) = &candidate.lineage_manifest {
            transaction.project_verified_lineage_manifest(manifest)?;
        }
        match pipeline {
            Some(ConfigurationPipelinePublication::Epoch { epoch, tools }) => {
                if transaction.publish_pipeline_epoch(epoch)? {
                    transaction.publish_tool_epoch(tools)?;
                }
            }
            Some(ConfigurationPipelinePublication::Poison(poison)) => {
                transaction.publish_pipeline_poison(poison)?;
            }
            None => {}
        }

        if candidate.version_poison.is_none() {
            for bundle in &old_bundles {
                transaction.remove_bundle(bundle.bundle)?;
            }
            for source in &candidate.scan.bundles {
                let Ok(bundle) = &source.parsed else {
                    continue;
                };
                let root = *root_ids
                    .entry(source.root_name.clone())
                    .or_insert(transaction.intern_root(&source.root_name)?);
                transaction.upsert_bundle(&BundleMeta {
                    bundle: bundle.uuid,
                    root,
                    path: source.normalized_path.clone(),
                    format_version: bundle.format_version,
                    content_hash: ContentHash(source.file_hash.0),
                    origin: crate::importer::decoded_directory_origin(bundle).map_err(|error| {
                        StoreError::InvalidConfiguration {
                            error: format!(
                                "invalid import record in {}: {error:?}",
                                source.normalized_path
                            ),
                        }
                    })?,
                })?;
                for (hash, schema) in &bundle.schemas {
                    let snapshot =
                        distill_schema::ngp_schema::snapshot_to_json(schema).map_err(|error| {
                            StoreError::InvalidConfiguration {
                                error: format!("cannot serialize verified schema {hash}: {error}"),
                            }
                        })?;
                    transaction.put_schema(*hash, &snapshot)?;
                }
                for (local_id, entry) in &bundle.assets {
                    transaction.upsert_asset(&AssetRecord {
                        asset: entry.uuid,
                        bundle: bundle.uuid,
                        local_id: local_id.clone(),
                        type_uuid: entry.type_uuid,
                        logical_hash: entry.schema_hash,
                        authoring_only: entry.authoring_only,
                        tags: Vec::new(),
                    })?;
                }
                if let Some(primary) = &bundle.primary {
                    transaction.set_path_entry(
                        &source.normalized_path,
                        root,
                        bundle.assets[primary].uuid,
                    )?;
                }
            }
        }
        Ok(())
    })?;
    commit.pipeline = Some(pipeline_diagnostic(store.pipeline_state()?));
    Ok(commit)
}

fn pipeline_diagnostic(state: Option<StoredPipelineState>) -> PipelineDiagnostic {
    match state {
        Some(StoredPipelineState::Ready(_)) | None => PipelineDiagnostic::Ready,
        Some(StoredPipelineState::SchemaAcceptanceRequired { required, .. }) => {
            PipelineDiagnostic::SchemaAcceptanceRequired(required)
        }
        Some(StoredPipelineState::Poisoned { error, .. }) => PipelineDiagnostic::Poisoned(error),
    }
}

/// Rescan the complete namespace and advance the durable projection from an
/// authoring backend while the RPC server holds its publication lock.
pub(crate) fn publish_current_scan(
    scanner: &RootedScanner,
    lineage_destination: &LineageDestination,
    store: &Arc<Mutex<Store>>,
    base: InputVersion,
    projection: &PipelineProjection,
) -> Result<Commit, String> {
    let scan = scanner.scan().map_err(|error| error.to_string())?;
    let candidate = ScanCandidate::build(scanner, lineage_destination, scan, None)
        .map_err(|error| error.to_string())?;
    publish_scan(store, base, candidate, false, None, projection).map_err(|error| error.to_string())
}

fn rpc_commit(
    candidate: &ScanCandidate,
    old_assets: &[AssetUuid],
    old_paths: &[(String, distill_store::files::RootId, AssetUuid)],
    projection: &PipelineProjection,
    derived_outputs: BTreeMap<AssetUuid, DerivedOutputEntry>,
) -> Result<Commit, StoreError> {
    let mut commit = Commit {
        configuration: Some(candidate.configuration.clone()),
        pipeline: Some(PipelineDiagnostic::Ready),
        version_poison: Some(candidate.version_poison.clone()),
        lineage_repair: Some(candidate.lineage_repair.clone()),
        derived_outputs: Some(derived_outputs),
        ..Commit::default()
    };
    if candidate.version_poison.is_some() {
        return Ok(commit);
    }

    let mut current_assets = BTreeSet::new();
    let mut paths = BTreeMap::<String, BTreeSet<AssetUuid>>::new();
    for source in &candidate.scan.bundles {
        let Ok(bundle) = &source.parsed else {
            continue;
        };
        for (local_id, entry) in &bundle.assets {
            current_assets.insert(entry.uuid);
            commit.assets.push(AssetMutation::Set {
                uuid: entry.uuid,
                resolution: StoredResolve::Drifted {
                    input: DriftedInput::Asset(entry.uuid),
                },
                delta: AssetDeltaState::Changed,
            });
            commit.authoring.push(AuthoringMutation::Set(rpc_entry(
                source.normalized_path.clone(),
                bundle,
                local_id,
                entry,
                projection.interface(entry.type_uuid).terminal,
            )?));
        }
        if let Some(primary) = &bundle.primary {
            paths
                .entry(source.normalized_path.clone())
                .or_default()
                .insert(bundle.assets[primary].uuid);
        }
    }
    for asset in old_assets {
        if !current_assets.contains(asset) {
            commit.assets.push(AssetMutation::Set {
                uuid: *asset,
                resolution: StoredResolve::Deleted,
                delta: AssetDeltaState::Deleted,
            });
            commit
                .authoring
                .push(AuthoringMutation::Remove { uuid: *asset });
        }
    }
    let old_path_names = old_paths
        .iter()
        .map(|(path, _, _)| path.clone())
        .collect::<BTreeSet<_>>();
    for (path, candidates) in paths {
        commit.paths.push(PathMutation::Set { path, candidates });
    }
    let current_path_names = commit
        .paths
        .iter()
        .filter_map(|mutation| match mutation {
            PathMutation::Set { path, .. } => Some(path.clone()),
            PathMutation::Remove { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    for path in old_path_names.difference(&current_path_names) {
        commit
            .paths
            .push(PathMutation::Remove { path: path.clone() });
    }
    Ok(commit)
}

fn rpc_entry(
    normalized_path: String,
    bundle: &Bundle,
    local_id: &str,
    entry: &AssetEntry,
    terminal_type: TypeUuid,
) -> Result<AuthoringEntry, StoreError> {
    let schema = &bundle.schemas[&entry.schema_hash];
    let logical_schema = distill_schema::ngp_schema::snapshot_to_json(schema).map_err(|error| {
        StoreError::InvalidConfiguration {
            error: format!("cannot serialize verified schema: {error}"),
        }
    })?;
    let value = split_authoring_value(&entry.data)?;
    Ok(AuthoringEntry {
        uuid: entry.uuid,
        bundle: bundle.uuid,
        local_id: local_id.to_owned(),
        normalized_path,
        type_uuid: entry.type_uuid,
        terminal_type,
        schema_hash: entry.schema_hash,
        logical_schema: Arc::from(logical_schema.into_bytes()),
        role: if entry.authoring_only {
            AuthoringEntryRole::AuthoringOnly
        } else {
            AuthoringEntryRole::Runtime
        },
        tags: BTreeMap::new(),
        value,
    })
}

fn split_authoring_value(value: &AuthoredValue) -> Result<AuthoringValue, StoreError> {
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
    let canonical =
        distill_json::write(&rewritten).map_err(|error| StoreError::InvalidConfiguration {
            error: format!("cannot serialize authored value: {error}"),
        })?;
    Ok(AuthoringValue {
        canonical_value: Arc::from(canonical.into_bytes()),
        blobs,
    })
}

fn file_state(file: &crate::scanner::ScannedFile) -> FileState {
    FileState {
        mtime: file.modified_nanos,
        size: file.size,
        kind: match file.kind {
            ScannedFileKind::File => FileKind::File,
            ScannedFileKind::Directory => FileKind::Directory,
            ScannedFileKind::Symlink => FileKind::Symlink,
        },
        content_hash: file.content_hash,
    }
}

fn readable_source(source: &crate::scanner::ScannedBundle) -> ReadableBundleSource {
    ReadableBundleSource {
        root_name: source.root_name.clone(),
        normalized_path: source.normalized_path.clone(),
        file_hash: source.file_hash,
    }
}

fn lineage_claimants(
    parsed: &[(&crate::scanner::ScannedBundle, &Bundle)],
) -> Vec<LineageManifestClaimant> {
    parsed
        .iter()
        .flat_map(|(source, bundle)| {
            bundle.assets.iter().filter_map(move |(local_id, entry)| {
                (entry.type_uuid == SCHEMA_LINEAGE_MANIFEST_TYPE_UUID).then_some(
                    LineageManifestClaimant {
                        root_name: source.root_name.clone(),
                        normalized_path: source.normalized_path.clone(),
                        bundle: bundle.uuid,
                        local_id: local_id.clone(),
                        asset: entry.uuid,
                        file_hash: source.file_hash,
                    },
                )
            })
        })
        .collect()
}

fn skeleton_failure(error: &distill_bundle::BundleError) -> SkeletonFailureCode {
    use distill_bundle::BundleError;
    match error {
        BundleError::MissingEnvelopeKey {
            key: "format_version",
        }
        | BundleError::FormatVersionNotUInt { .. }
        | BundleError::UnsupportedFormatVersion { .. } => SkeletonFailureCode::MissingFormatVersion,
        BundleError::BadBundleUuid { .. } => SkeletonFailureCode::InvalidBundleUuid,
        BundleError::BadEntryId { field: "uuid", .. }
        | BundleError::MissingEntryKey { key: "uuid", .. } => {
            SkeletonFailureCode::IncompleteAssetIdentity
        }
        BundleError::BadEntryId {
            field: "type_uuid", ..
        }
        | BundleError::MissingEntryKey {
            key: "type_uuid", ..
        } => SkeletonFailureCode::IncompleteTypeIdentity,
        _ => SkeletonFailureCode::EnvelopeMalformed,
    }
}

fn decode_lineage_manifest(
    value: &AuthoredValue,
    hash: ContentHash,
) -> Result<VerifiedSchemaLineageManifest, CoordinatorError> {
    let object = exact_object(value, &["types"])?;
    let AuthoredValue::Array(rows) = &object["types"] else {
        return invalid_manifest("lineage types must be a canonical non-string map array");
    };
    let mut types = BTreeMap::new();
    for row in rows {
        let AuthoredValue::Array(pair) = row else {
            return invalid_manifest("lineage type map row must be a pair");
        };
        if pair.len() != 2 {
            return invalid_manifest("lineage type map row must contain two values");
        }
        let type_uuid = TypeUuid(fixed_bytes::<16>(&pair[0], "type UUID")?);
        if is_bootstrap_control_type(type_uuid) || types.contains_key(&type_uuid) {
            return invalid_manifest("lineage type is bootstrap-controlled or duplicated");
        }
        let lineage = exact_object(&pair[1], &["authority", "current", "epochs"])?;
        let AuthoredValue::Array(epoch_values) = &lineage["epochs"] else {
            return invalid_manifest("lineage epochs must be an array");
        };
        let mut epochs = Vec::with_capacity(epoch_values.len());
        for epoch in epoch_values {
            let epoch = exact_object(epoch, &["digest", "forward_parent"])?;
            let digest = LogicalHash(fixed_bytes::<32>(&epoch["digest"], "schema digest")?);
            let forward_parent = match &epoch["forward_parent"] {
                AuthoredValue::Null => None,
                AuthoredValue::UInt(value) => Some(u32::try_from(*value).map_err(|_| {
                    CoordinatorError::InvalidManifest(
                        "lineage forward parent exceeds u32".to_owned(),
                    )
                })?),
                _ => return invalid_manifest("lineage forward parent must be null or u32"),
            };
            epochs.push(AcceptedSchemaEpoch {
                digest,
                forward_parent,
            });
        }
        let current = match &lineage["current"] {
            AuthoredValue::UInt(value) => u32::try_from(*value).map_err(|_| {
                CoordinatorError::InvalidManifest("lineage cursor exceeds u32".to_owned())
            })?,
            _ => return invalid_manifest("lineage current cursor must be u32"),
        };
        let authority_object = exact_one_variant(&lineage["authority"])?;
        let authority = match authority_object.0 {
            "Active" => {
                exact_object(authority_object.1, &[])?;
                TypeAuthorityState::Active
            }
            "Retired" => {
                let payload = exact_object(authority_object.1, &["retired_from"])?;
                let AuthoredValue::UInt(retired_from) = payload["retired_from"] else {
                    return invalid_manifest("retired_from must be u32");
                };
                TypeAuthorityState::Retired {
                    retired_from: u32::try_from(retired_from).map_err(|_| {
                        CoordinatorError::InvalidManifest("retired_from exceeds u32".to_owned())
                    })?,
                }
            }
            _ => return invalid_manifest("unknown lineage authority variant"),
        };
        types.insert(
            type_uuid,
            AcceptedTypeLineage {
                epochs,
                current,
                authority,
            },
        );
    }
    Ok(VerifiedSchemaLineageManifest::from_verified_source(
        hash,
        SchemaLineageManifest { types },
    ))
}

fn exact_object<'a>(
    value: &'a AuthoredValue,
    keys: &[&str],
) -> Result<&'a BTreeMap<String, AuthoredValue>, CoordinatorError> {
    let AuthoredValue::Object(object) = value else {
        return invalid_manifest("expected an object");
    };
    if object.len() != keys.len() || keys.iter().any(|key| !object.contains_key(*key)) {
        return invalid_manifest("object field set is not exact");
    }
    Ok(object)
}

fn exact_one_variant(value: &AuthoredValue) -> Result<(&str, &AuthoredValue), CoordinatorError> {
    let AuthoredValue::Object(object) = value else {
        return invalid_manifest("enum must be an object");
    };
    if object.len() != 1 {
        return invalid_manifest("enum must select exactly one variant");
    }
    let (name, payload) = object.first_key_value().expect("one row");
    Ok((name, payload))
}

fn fixed_bytes<const N: usize>(
    value: &AuthoredValue,
    subject: &str,
) -> Result<[u8; N], CoordinatorError> {
    let AuthoredValue::Array(values) = value else {
        return invalid_manifest(&format!("{subject} must be a byte array"));
    };
    if values.len() != N {
        return invalid_manifest(&format!("{subject} must contain {N} bytes"));
    }
    let mut bytes = [0; N];
    for (output, value) in bytes.iter_mut().zip(values) {
        let AuthoredValue::UInt(value) = value else {
            return invalid_manifest(&format!("{subject} byte must be unsigned"));
        };
        *output = u8::try_from(*value)
            .map_err(|_| CoordinatorError::InvalidManifest(format!("{subject} byte exceeds u8")))?;
    }
    Ok(bytes)
}

fn invalid_manifest<T>(detail: &str) -> Result<T, CoordinatorError> {
    Err(CoordinatorError::InvalidManifest(detail.to_owned()))
}

fn lock_store(store: &Arc<Mutex<Store>>) -> MutexGuard<'_, Store> {
    store.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn lock_watcher(watcher: &Mutex<WatcherQueue>) -> MutexGuard<'_, WatcherQueue> {
    watcher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_pipeline(
    pipeline: &Mutex<CoordinatedPipelineRuntime>,
) -> MutexGuard<'_, CoordinatedPipelineRuntime> {
    pipeline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_publication(
    publication: &Mutex<Option<PipelinePublication>>,
) -> MutexGuard<'_, Option<PipelinePublication>> {
    publication
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
