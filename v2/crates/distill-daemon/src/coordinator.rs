//! Single-writer daemon coordinator.
//!
//! Filesystem scans are candidates. Publication happens only through one
//! server-serialized closure which rechecks the durable store basis, applies
//! the complete SQLite input transaction, and returns the exact immutable RPC
//! projection for the same successor version.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard, RwLock};

use rayon::ThreadPool;

use distill_build::pipeline::Target;
use distill_bundle::{AssetEntry, Bundle};
use distill_core::bootstrap::{is_bootstrap_control_type, SCHEMA_LINEAGE_MANIFEST_TYPE_UUID};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::lineage::AcceptedSchemaEpoch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AssetDeltaState, AssetMutation, AuthoringEntry, AuthoringEntryRole, AuthoringMutation,
    AuthoringValue, Commit, ConfigurationPoison, ConfigurationStatus, CoordinatedCommitError,
    DerivedOutputEntry, DerivedOutputMutation, DriftedInput, LineageManifestClaimant,
    LineageRepairState, PathMutation, PipelineCandidateIdentity, PipelineDiagnostic,
    SchemaTransitionAction, Server, SnapshotStamp, StoredResolve, TargetDefinition, VersionPoison,
    VersionPoisonV1,
};
use distill_schema::ProjectSchemaAuthority;
use distill_store::bundles::{
    AssetRecord, BundleMeta, NamespaceSkeleton as StoreNamespaceSkeleton, SkeletonEntry,
};
use distill_store::config::{PendingRestart, RestartOnlyChange};
use distill_store::files::{FileKind, FileState, PendingFileWork};
use distill_store::journal::{JournalIntentPlan, PublicationGroupKind, RenameAsideOutcome};
use distill_store::pipeline::{
    AcceptedTypeLineage, SchemaLineageManifest, SchemaReactivationRequest, SchemaRollbackRequest,
    TypeAuthorityState, ValidatedPipelineEpoch, VerifiedSchemaLineageManifest,
};
use distill_store::state::{
    AssetClaimant, CleanupDisposition, ConfigurationState, DirectoryAliasSide, DscpV1,
    InputVersion, PipelinePoison, PipelinePoisonCode, PipelinePoisonOrigin,
    PipelineState as StoredPipelineState, ReadableBundleSource,
    RetiredTypeReferenced as StoredRetiredTypeReferenced, ScanFailureCode, ScanSubject,
    SkeletonFailureCode,
};
use distill_store::{RetiredTypeReference, Store, StoreConfig, StoreError};

use crate::authoring::{AuthoringFilesystemCandidate, AuthoringService, AuthoringServiceInitError};
use crate::callbacks::EpochAuthoringImporter;
use crate::epoch::{
    stored_pipeline_epoch, CandidateRequirements, ModuleHost, PipelineEpoch, PipelineSnapshot,
    UnloadOutcome,
};
use crate::lineage_repair::{
    plan_same_dir_temp, unique_sibling, write_planned_temp, LineageRepairBackendInitError,
};
use crate::module_loader::DynamicPipelineModuleLoader;
use crate::operations::{PlannedSchemaTransition, SchemaTransitionJournalBasis};
use crate::pipeline_map::PipelineProjection;
use crate::quarantine::QuarantineDriver;
use crate::scanner::{
    AssetRoot, DaemonOwnedDirectoryKind, RootedScanner, ScanDelta, ScanDiagnostic, ScanError,
    ScanSnapshot, ScannedBundle, ScannedFileKind,
};
use crate::scheduler::{Scheduler, SchedulerConfig, WorkClass};
use crate::watcher::{WatcherAction, WatcherBatch, WatcherQueue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageDestination {
    pub root: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogicalRename {
    root_name: String,
    from_path: String,
    to_path: String,
}

pub(crate) struct ConfigurationCandidate {
    pub roots: Vec<AssetRoot>,
    pub lineage_destination: LineageDestination,
    pub targets: Vec<TargetDefinition>,
    pub build_targets: BTreeMap<String, Target>,
    pub pipeline_source: PathBuf,
    pub requirements: CandidateRequirements,
    pub schema_authority: Arc<ProjectSchemaAuthority>,
}

pub struct DaemonCoordinator {
    store: Arc<Mutex<Store>>,
    scanner: RootedScanner,
    scan_snapshot: Arc<Mutex<ScanSnapshot>>,
    scan_projection: Mutex<ScanProjectionIndex>,
    scan_initialized: AtomicBool,
    scan_healthy: AtomicBool,
    scan_rejection: Mutex<Option<PendingScanRejection>>,
    server: Server,
    lineage_destination: RwLock<LineageDestination>,
    authoring: Arc<AuthoringService>,
    pipeline: Mutex<CoordinatedPipelineRuntime>,
    schema_authority: RwLock<Option<Arc<ProjectSchemaAuthority>>>,
    build_targets: RwLock<BTreeMap<String, Target>>,
    configuration_poison: Mutex<Option<ConfigurationPoison>>,
    operational: Mutex<OperationalRuntime>,
    operational_wake: Condvar,
}

struct OperationalRuntime {
    scheduler: Scheduler,
    worker_pool: Arc<ThreadPool>,
    next_job_id: u64,
}

struct CoordinatedPipelineRuntime {
    host: ModuleHost,
    loader: DynamicPipelineModuleLoader,
    pending: Option<PendingPipelineEpoch>,
}

struct PendingPipelineEpoch {
    loaded: PipelineEpoch,
    stored: ValidatedPipelineEpoch,
}

enum ConfigurationPipelinePublication {
    Epoch {
        epoch: ValidatedPipelineEpoch,
        tools: BTreeMap<String, distill_store::pipeline::ToolRegistrationV2>,
    },
    Poison(PipelinePoison),
}

fn discard_pending(runtime: &mut CoordinatedPipelineRuntime) -> Option<PipelinePoison> {
    if let Some(pending) = runtime.pending.take() {
        runtime.host.discard_unpublished(pending.loaded)
    } else {
        None
    }
}

fn discard_prepared(
    runtime: &mut CoordinatedPipelineRuntime,
    prepared: &mut Option<PipelineEpoch>,
) -> Option<PipelinePoison> {
    if let Some(prepared) = prepared.take() {
        runtime.host.discard_unpublished(prepared)
    } else {
        None
    }
}

fn record_cleanup_failure(slot: &Mutex<Option<PipelinePoison>>, failure: Option<PipelinePoison>) {
    if let Some(failure) = failure {
        let mut slot = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(failure);
        }
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
        let state_path = store_config.state_path.clone();
        let mut opened_store = Store::open(store_config.clone())?;
        if opened_store.pending_restart()?.is_some() {
            opened_store
                .input_transaction(|transaction| transaction.adopt_pending_restart().map(|_| ()))?;
        }
        let store = Arc::new(Mutex::new(opened_store));
        let host = ModuleHost::new(&module_state_path).map_err(CoordinatorInitError::ModuleIo)?;
        let scanner = RootedScanner::new(roots.clone())?;
        scanner.retain_daemon_owned_directory(DaemonOwnedDirectoryKind::State, &state_path)?;
        scanner.retain_daemon_owned_directory(
            DaemonOwnedDirectoryKind::ModuleStaging,
            &module_state_path,
        )?;
        scanner.retain_daemon_owned_directory(
            DaemonOwnedDirectoryKind::ModuleStaging,
            module_state_path.join("modules"),
        )?;
        let scan_snapshot = Arc::new(Mutex::new(ScanSnapshot::default()));
        let backend = Arc::new(AuthoringService::new(
            Arc::clone(&store),
            roots,
            scanner.clone(),
            lineage_destination.clone(),
            Arc::clone(&scan_snapshot),
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
        let pipeline = CoordinatedPipelineRuntime {
            host,
            loader: DynamicPipelineModuleLoader,
            pending: None,
        };
        let scheduler_config = SchedulerConfig {
            parallelism: store_config.parallelism,
            batch_reserved_workers: store_config.batch_reserved_workers,
            max_dependency_depth,
        };
        let operational = OperationalRuntime {
            scheduler: Scheduler::new(scheduler_config)
                .map_err(|error| CoordinatorInitError::Operational(error.to_string()))?,
            worker_pool: build_worker_pool(scheduler_config.parallelism)
                .map_err(CoordinatorInitError::Operational)?,
            next_job_id: 1,
        };
        Ok(Self {
            store,
            scanner,
            scan_snapshot,
            scan_projection: Mutex::new(ScanProjectionIndex::default()),
            scan_initialized: AtomicBool::new(false),
            scan_healthy: AtomicBool::new(true),
            scan_rejection: Mutex::new(None),
            server,
            lineage_destination: RwLock::new(lineage_destination),
            authoring: backend,
            pipeline: Mutex::new(pipeline),
            schema_authority: RwLock::new(None),
            build_targets: RwLock::new(BTreeMap::new()),
            configuration_poison: Mutex::new(None),
            operational: Mutex::new(operational),
            operational_wake: Condvar::new(),
        })
    }

    pub fn server(&self) -> &Server {
        &self.server
    }

    /// Attach this coordinator as the RPC server's production lazy-build
    /// authority after it has been placed in its final `Arc`.
    pub fn attach_build_backend(self: &Arc<Self>) {
        let backend = Arc::new(crate::build::CoordinatorBuildBackend::new(self));
        self.server.install_build_backend(backend.clone());
        self.server.install_artifact_lease_backend(backend.clone());
        self.server.install_artifact_payload_backend(backend);
        self.authoring.attach_tag_index_coordinator(self);
    }

    pub fn store(&self) -> Arc<Mutex<Store>> {
        Arc::clone(&self.store)
    }

    pub fn scanner(&self) -> RootedScanner {
        self.scanner.clone()
    }

    /// Current non-fatal filesystem exclusions in canonical rooted-path
    /// order. These rows are also reported by `doctor verify`.
    pub fn scan_diagnostics(&self) -> Vec<ScanDiagnostic> {
        lock_scan_snapshot(&self.scan_snapshot)
            .diagnostic_rows()
            .cloned()
            .collect()
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
        let runtime = lock_pipeline(&self.pipeline);
        let Some(epoch) = runtime.host.published_ready_epoch() else {
            return Ok(None);
        };
        let observed = match runtime.host.snapshot().epoch() {
            Ok(_) => return Ok(None),
            Err(poison) if poison.origin == PipelinePoisonOrigin::PublishedRuntime => {
                (epoch.dylib_hash(), poison)
            }
            Err(_) => return Ok(None),
        };
        let diagnostic = observed.1.clone();
        self.server
            .coordinated_runtime_pipeline_poison(diagnostic, || {
                let mut store = lock_store(&self.store);
                match store.poison_published_pipeline_epoch(observed.0, &observed.1) {
                    Ok(()) => Ok(()),
                    Err(StoreError::StalePublishedPipeline {
                        actual: Some(actual),
                        already_unavailable: true,
                        ..
                    }) if actual == observed.0 => Ok(()),
                    Err(error) => Err(error.to_string()),
                }
            })
            .map_err(CoordinatorError::RuntimePipeline)?;
        Ok(Some(observed.1))
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

    pub(crate) fn run_scheduled<R>(
        self: &Arc<Self>,
        class: WorkClass,
        run: impl FnOnce() -> R + Send + 'static,
    ) -> R
    where
        R: Send + 'static,
    {
        let mut operational = self
            .operational
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = operational.next_job_id;
        operational.next_job_id = operational
            .next_job_id
            .checked_add(1)
            .expect("scheduler job identity exhausted");
        operational
            .scheduler
            .try_enqueue(id, class)
            .expect("fresh scheduler job identity is unique");
        operational.scheduler.admit();
        self.operational_wake.notify_all();
        while !operational.scheduler.is_active(id) {
            operational = self
                .operational_wake
                .wait(operational)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        let worker_pool = Arc::clone(&operational.worker_pool);
        drop(operational);

        let coordinator = Arc::clone(self);
        let (sender, receiver) = mpsc::sync_channel(1);
        worker_pool.spawn_fifo(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
            let mut operational = coordinator
                .operational
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            operational
                .scheduler
                .complete(id)
                .expect("scheduled job remains active until its worker returns");
            operational.scheduler.admit();
            coordinator.operational_wake.notify_all();
            drop(operational);
            let _ = sender.send(outcome);
        });
        match receiver.recv() {
            Ok(Ok(result)) => result,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => panic!("distill build worker stopped before returning job {id}"),
        }
    }

    pub fn apply_operational_configuration(
        &self,
        store_config: &StoreConfig,
        max_dependency_depth: usize,
    ) -> Result<(), CoordinatorError> {
        let scheduler = SchedulerConfig {
            parallelism: store_config.parallelism,
            batch_reserved_workers: store_config.batch_reserved_workers,
            max_dependency_depth,
        };
        scheduler
            .validate()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let replacement_pool = {
            let operational = self
                .operational
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (operational.scheduler.config().parallelism != scheduler.parallelism)
                .then(|| build_worker_pool(scheduler.parallelism))
                .transpose()
                .map_err(CoordinatorError::InvalidManifest)?
        };
        lock_store(&self.store)
            .apply_operational_config(store_config)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let mut operational = self
            .operational
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        operational
            .scheduler
            .reconfigure(scheduler)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        if let Some(worker_pool) = replacement_pool {
            operational.worker_pool = worker_pool;
        }
        operational.scheduler.admit();
        self.operational_wake.notify_all();
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

    pub fn clear_restart_configuration(&self) -> Result<(), CoordinatorError> {
        lock_store(&self.store)
            .clear_pending_restart()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        self.server.restart_required(Vec::new());
        Ok(())
    }

    /// Run §14's journaled displaced-inode retention sweep using the current
    /// operational-live retention window.
    pub fn sweep_displaced_retention(&self, now_secs: i64) -> Result<usize, CoordinatorError> {
        lock_store(&self.store)
            .sweep_displaced(now_secs)
            .map_err(|error| CoordinatorError::Maintenance(error.to_string()))
    }

    fn configuration_poison(&self) -> Option<ConfigurationPoison> {
        let source = self
            .configuration_poison
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let scan = self
            .scan_rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|pending| pending.rejection.configuration.clone());
        ConfigurationPoison::select_canonical(source.into_iter().chain(scan))
            .ok()
            .flatten()
    }

    fn pending_scan_version_poison(&self) -> Option<VersionPoison> {
        self.scan_rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|pending| pending.rejection.version.clone())
    }

    fn configuration_without_source_poison(
        &self,
    ) -> Result<(ConfigurationStatus, Option<LineageRepairState>), CoordinatorError> {
        let scan_poison = self
            .scan_rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|pending| pending.rejection.configuration.clone());
        let index = lock_scan_projection(&self.scan_projection);
        indexed_lineage_projection(
            &index,
            &self.scanner,
            &self.lineage_destination(),
            scan_poison,
        )
        .map(|(configuration, repair, _)| (configuration, repair))
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
        match self.publish_cached_scan() {
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
        match self.publish_cached_scan() {
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

    pub(crate) fn publish_configuration_candidate(
        &self,
        candidate: ConfigurationCandidate,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let ConfigurationCandidate {
            roots,
            lineage_destination,
            targets,
            build_targets,
            pipeline_source,
            mut requirements,
            schema_authority,
        } = candidate;
        let filesystem = self
            .authoring
            .prepare_filesystem_candidate(roots, lineage_destination)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let filesystem_changed = !self.scanner.has_same_roots(filesystem.scanner());
        // Schema, target, or module-only candidates reuse the already
        // observed asset snapshot. A physical complete scan is reserved for
        // an actual configured-root replacement; the lineage destination is
        // projection authority, not filesystem watch coverage.
        let candidate_scan_heals =
            filesystem_changed || !self.scan_initialized.load(Ordering::Acquire);
        let scan = if candidate_scan_heals {
            filesystem.scanner().scan()?
        } else {
            lock_scan_snapshot(&self.scan_snapshot).clone()
        };
        let installed_snapshot = scan.clone();
        let destination = filesystem.lineage_destination().clone();
        let mut candidate = ScanCandidate::build(
            filesystem.scanner(),
            &destination,
            scan,
            None,
            Some(&schema_authority),
        )?;
        if !candidate_scan_heals {
            candidate.version_poison = VersionPoison::select_canonical(
                candidate
                    .version_poison
                    .take()
                    .into_iter()
                    .chain(self.pending_scan_version_poison()),
            )
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        }
        let mut runtime = lock_pipeline(&self.pipeline);
        let prepared_epoch = {
            let CoordinatedPipelineRuntime { host, loader, .. } = &mut *runtime;
            host.prepare_candidate(&pipeline_source, &mut requirements, loader)
        };
        let authored_types = requirements
            .schema_registry
            .keys()
            .copied()
            .filter(|type_uuid| !distill_core::bootstrap::is_bootstrap_control_type(*type_uuid))
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
                                if let Some(poison) = runtime.host.discard_unpublished(prepared) {
                                    drop(runtime);
                                    return self.publish_pipeline_rejection(poison);
                                }
                                return Err(CoordinatorError::InvalidManifest(error.to_string()));
                            }
                        };
                        if let Err(error) = self.authoring.prepare_pipeline_importers(
                            EpochAuthoringImporter::metadata_only(prepared.importer_descriptors()),
                        ) {
                            if let Some(poison) = runtime.host.discard_unpublished(prepared) {
                                drop(runtime);
                                return self.publish_pipeline_rejection(poison);
                            }
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
        let installed_projection = match ScanProjectionIndex::build(
            &installed_snapshot,
            &projection,
            Some(&schema_authority),
        ) {
            Ok(index) => index,
            Err(error) => {
                if let Some(poison) = discard_prepared(&mut runtime, &mut prepared_epoch) {
                    drop(runtime);
                    return self.publish_pipeline_rejection(poison);
                }
                return Err(error);
            }
        };
        let filesystem = Arc::new(Mutex::new(Some(filesystem)));
        let captured = Arc::clone(&filesystem);
        let tag_epoch = schema_authority.source_hash();
        let max_dependency_depth = self.operational_configuration().max_dependency_depth;
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        let fallback_bundles = match lock_store(&store).all_asset_bundles() {
            Ok(bundles) => bundles,
            Err(error) => {
                if let Some(poison) = discard_prepared(&mut runtime, &mut prepared_epoch) {
                    drop(runtime);
                    return self.publish_pipeline_rejection(poison);
                }
                return Err(CoordinatorError::InvalidManifest(error.to_string()));
            }
        };
        let authoring = Arc::clone(&self.authoring);
        let cleanup_failure = Arc::new(Mutex::new(None));
        let captured_cleanup_failure = Arc::clone(&cleanup_failure);
        let result = self
            .server
            .coordinated_replace_target_set(base, targets, || {
                let mut commit = publish_scan(
                    &store,
                    base,
                    candidate,
                    true,
                    Some(&pipeline),
                    &projection,
                    tag_epoch,
                )
                .map_err(|error| error.to_string())?;
                let published_pipeline = commit
                    .pipeline
                    .clone()
                    .expect("scan commit always carries pipeline diagnostics");
                let filesystem: AuthoringFilesystemCandidate = captured
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .expect("configuration candidate installs once");
                self.scanner.replace_from(filesystem.scanner());
                authoring.install_filesystem_candidate(filesystem);
                *lock_scan_snapshot(&self.scan_snapshot) = installed_snapshot;
                *lock_scan_projection(&self.scan_projection) = installed_projection;
                self.scan_initialized.store(true, Ordering::Release);
                if candidate_scan_heals {
                    self.scan_rejection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take();
                    self.scan_healthy.store(true, Ordering::Release);
                }
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
                match (&pipeline, prepared_epoch.take(), published_pipeline) {
                    (
                        ConfigurationPipelinePublication::Epoch { .. },
                        Some(prepared),
                        PipelineDiagnostic::Ready,
                    ) => {
                        let importers = EpochAuthoringImporter::all(&prepared);
                        record_cleanup_failure(
                            &captured_cleanup_failure,
                            discard_pending(&mut runtime),
                        );
                        runtime.host.install_ready(prepared);
                        authoring
                            .replace_pipeline_importers(importers)
                            .expect("candidate importer metadata was prevalidated");
                    }
                    (
                        ConfigurationPipelinePublication::Epoch { epoch, .. },
                        Some(prepared),
                        PipelineDiagnostic::SchemaAcceptanceRequired(_)
                        | PipelineDiagnostic::RetiredTypeReferenced(_),
                    ) => {
                        record_cleanup_failure(
                            &captured_cleanup_failure,
                            discard_pending(&mut runtime),
                        );
                        let fence = PipelinePoison::new(
                            PipelinePoisonCode::CandidateValidation,
                            PipelinePoisonOrigin::CandidateOpen,
                            CleanupDisposition::None,
                            "pipeline candidate requires explicit schema acceptance",
                        )
                        .expect("schema-acceptance fence is valid");
                        runtime.host.install_poison(fence);
                        runtime.pending = Some(PendingPipelineEpoch {
                            loaded: prepared,
                            stored: epoch.clone(),
                        });
                        authoring.install_pipeline_importers(BTreeMap::new());
                    }
                    (
                        ConfigurationPipelinePublication::Epoch { .. },
                        Some(prepared),
                        PipelineDiagnostic::Poisoned(error),
                    ) => {
                        record_cleanup_failure(
                            &captured_cleanup_failure,
                            runtime.host.discard_unpublished(prepared),
                        );
                        record_cleanup_failure(
                            &captured_cleanup_failure,
                            discard_pending(&mut runtime),
                        );
                        runtime.host.install_poison(error);
                        authoring.install_pipeline_importers(BTreeMap::new());
                    }
                    (
                        ConfigurationPipelinePublication::Poison(poison),
                        None,
                        PipelineDiagnostic::Poisoned(_)
                        | PipelineDiagnostic::RetiredTypeReferenced(_),
                    ) => {
                        record_cleanup_failure(
                            &captured_cleanup_failure,
                            discard_pending(&mut runtime),
                        );
                        runtime.host.install_poison(poison.clone());
                        authoring.install_pipeline_importers(BTreeMap::new());
                    }
                    _ => unreachable!(
                        "durable configuration pipeline state must match its prepared candidate"
                    ),
                }
                crate::build::refine_published_tag_index(
                    Arc::clone(&store),
                    self.scanner.clone(),
                    Arc::clone(&schema_authority),
                    runtime.host.snapshot(),
                    &build_targets,
                    max_dependency_depth,
                    &commit_asset_bundles(&commit, &fallback_bundles),
                )
                .apply(&mut commit);
                commit.pipeline_epoch_changed = true;
                Ok(commit)
            });
        match result {
            Ok(stamp) => {
                let poison = cleanup_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(poison) = poison {
                    drop(runtime);
                    self.publish_pipeline_rejection(poison)
                } else {
                    Ok(stamp)
                }
            }
            Err(error) => {
                if let Some(poison) = discard_prepared(&mut runtime, &mut prepared_epoch) {
                    drop(runtime);
                    return self.publish_pipeline_rejection(poison);
                }
                Err(CoordinatorError::Coordinated(error))
            }
        }
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
                if let Some(poison) = runtime.host.discard_unpublished(prepared) {
                    drop(runtime);
                    return self.publish_pipeline_rejection(poison);
                }
                return Err(CoordinatorError::InvalidManifest(error.to_string()));
            }
        };
        if let Err(error) =
            self.authoring
                .prepare_pipeline_importers(EpochAuthoringImporter::metadata_only(
                    prepared.importer_descriptors(),
                ))
        {
            if let Some(poison) = runtime.host.discard_unpublished(prepared) {
                drop(runtime);
                return self.publish_pipeline_rejection(poison);
            }
            return Err(CoordinatorError::InvalidManifest(format!("{error:?}")));
        }
        let store = Arc::clone(&self.store);
        let tools = prepared.tool_epoch();
        let authority = match self.schema_authority() {
            Some(authority) => authority,
            None => {
                if let Some(poison) = runtime.host.discard_unpublished(prepared) {
                    drop(runtime);
                    return self.publish_pipeline_rejection(poison);
                }
                return Err(CoordinatorError::InvalidManifest(
                    "pipeline publication requires project schema authority".to_owned(),
                ));
            }
        };
        let tag_epoch = authority.source_hash();
        let asset_bundles = match lock_store(&store).all_asset_bundles() {
            Ok(bundles) => bundles,
            Err(error) => {
                if let Some(poison) = runtime.host.discard_unpublished(prepared) {
                    drop(runtime);
                    return self.publish_pipeline_rejection(poison);
                }
                return Err(CoordinatorError::InvalidManifest(error.to_string()));
            }
        };
        let assets = asset_bundles.keys().copied().collect::<Vec<_>>();
        let targets = self
            .build_targets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let scanner = self.scanner.clone();
        let max_dependency_depth = self.operational_configuration().max_dependency_depth;
        let mut prepared = Some(prepared);
        let cleanup_failure = Arc::new(Mutex::new(None));
        let captured_cleanup_failure = Arc::clone(&cleanup_failure);
        let result = self.server.coordinated_commit(base, || {
            let mut durable = lock_store(&store);
            if durable.input_version() != base {
                return Err(format!(
                    "durable pipeline basis is {:?}, expected {base:?}",
                    durable.input_version()
                ));
            }
            let mut acceptance = None;
            durable
                .input_transaction(|transaction| {
                    acceptance = Some(transaction.pipeline_acceptance_requirement(&stored)?);
                    if transaction.publish_pipeline_epoch(&stored)? {
                        transaction.publish_tool_epoch(&tools)?;
                    }
                    for asset in &assets {
                        transaction.set_tag_index_pending(*asset, tag_epoch)?;
                    }
                    Ok(())
                })
                .map_err(|error| error.to_string())?;
            drop(durable);
            let candidate = prepared
                .take()
                .expect("pipeline candidate is installed once");
            let diagnostic = match acceptance.expect("pipeline outcome is captured in-transaction")
            {
                None => {
                    let importers = EpochAuthoringImporter::all(&candidate);
                    record_cleanup_failure(
                        &captured_cleanup_failure,
                        discard_pending(&mut runtime),
                    );
                    runtime.host.install_ready(candidate);
                    self.authoring
                        .replace_pipeline_importers(importers)
                        .expect("candidate importer metadata was prevalidated");
                    PipelineDiagnostic::Ready
                }
                Some(required) => {
                    record_cleanup_failure(
                        &captured_cleanup_failure,
                        discard_pending(&mut runtime),
                    );
                    let fence = PipelinePoison::new(
                        PipelinePoisonCode::CandidateValidation,
                        PipelinePoisonOrigin::CandidateOpen,
                        CleanupDisposition::None,
                        "pipeline candidate requires explicit schema acceptance",
                    )
                    .expect("schema-acceptance fence is a valid candidate poison");
                    runtime.host.install_poison(fence);
                    runtime.pending = Some(PendingPipelineEpoch {
                        loaded: candidate,
                        stored: stored.clone(),
                    });
                    self.authoring.install_pipeline_importers(BTreeMap::new());
                    PipelineDiagnostic::SchemaAcceptanceRequired(required)
                }
            };
            let mut commit = Commit {
                pipeline: Some(diagnostic),
                pipeline_epoch_changed: true,
                ..Commit::default()
            };
            crate::build::refine_published_tag_index(
                Arc::clone(&store),
                scanner,
                Arc::clone(&authority),
                runtime.host.snapshot(),
                &targets,
                max_dependency_depth,
                &asset_bundles,
            )
            .apply(&mut commit);
            Ok(commit)
        });
        match result {
            Ok(stamp) => {
                let poison = cleanup_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(poison) = poison {
                    drop(runtime);
                    self.publish_pipeline_rejection(poison)
                } else {
                    Ok(stamp)
                }
            }
            Err(error) => {
                if let Some(candidate) = prepared.take() {
                    if let Some(poison) = runtime.host.discard_unpublished(candidate) {
                        drop(runtime);
                        return self.publish_pipeline_rejection(poison);
                    }
                }
                Err(CoordinatorError::Coordinated(error))
            }
        }
    }

    /// Publish a candidate-input failure which occurs before a module can be
    /// opened (for example, malformed watched schema JSON). The last durable
    /// schema/target projection remains intact, while the new input version is
    /// explicitly pipeline-poisoned and the live module is fenced.
    pub fn publish_pipeline_rejection(
        &self,
        poison: PipelinePoison,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        self.publish_pipeline_rejection_inner(poison, false)
    }

    pub(crate) fn publish_pipeline_rejection_healing_configuration(
        &self,
        poison: PipelinePoison,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        self.publish_pipeline_rejection_inner(poison, true)
    }

    pub(crate) fn pending_schema_transition_context(
        &self,
        candidate: &PipelineCandidateIdentity,
        type_uuid: TypeUuid,
    ) -> Result<(Option<LogicalHash>, BTreeSet<String>), String> {
        let runtime = lock_pipeline(&self.pipeline);
        let pending = runtime
            .pending
            .as_ref()
            .ok_or_else(|| "no loaded pipeline candidate awaits schema acceptance".to_owned())?;
        let actual = PipelineCandidateIdentity::try_from(pending.stored.epoch())
            .map_err(|error| error.to_string())?;
        if &actual != candidate {
            return Err("loaded schema candidate identity is stale".to_owned());
        }
        Ok((
            pending.stored.schema_registry.get(&type_uuid).copied(),
            pending
                .loaded
                .migration_function_keys()
                .into_iter()
                .collect(),
        ))
    }

    pub(crate) fn publish_schema_transition(
        &self,
        base: InputVersion,
        planned: &PlannedSchemaTransition,
        quarantine: &QuarantineDriver,
    ) -> Result<distill_rpc::DeferredOperationResult, String> {
        // Keep the exact unpublished candidate reserved through durable
        // manifest replacement, rescan, and promotion. Ordinary pipeline
        // staging takes the same mutex, so it cannot replace the candidate
        // between the store CAS and live installation.
        let mut runtime = lock_pipeline(&self.pipeline);
        let (candidate, tools, prepared_importers) = {
            let pending = runtime.pending.as_ref().ok_or_else(|| {
                "no loaded pipeline candidate awaits schema acceptance".to_owned()
            })?;
            let actual = PipelineCandidateIdentity::try_from(pending.stored.epoch())
                .map_err(|error| error.to_string())?;
            if actual != planned.request.candidate {
                return Err("loaded schema candidate identity is stale".to_owned());
            }
            let importers = self
                .authoring
                .prepare_pipeline_importers(EpochAuthoringImporter::all(&pending.loaded))
                .map_err(|error| format!("candidate importer metadata is invalid: {error:?}"))?;
            (
                pending.stored.clone(),
                pending.loaded.tool_epoch(),
                importers,
            )
        };
        let transition = IncrementalSchemaTransition {
            planned,
            candidate: &candidate,
            tools: &tools,
        };

        let temp = plan_same_dir_temp(&planned.target)?;
        let plan = JournalIntentPlan {
            target_path: planned
                .target
                .to_str()
                .ok_or_else(|| "schema manifest path is not lossless UTF-8".to_owned())?
                .to_owned(),
            temp_path: temp
                .to_str()
                .ok_or_else(|| "schema manifest temp path is not lossless UTF-8".to_owned())?
                .to_owned(),
            conflict_path: unique_sibling(&planned.target, "conflict")
                .to_str()
                .ok_or_else(|| "schema manifest conflict path is not lossless UTF-8".to_owned())?
                .to_owned(),
            pre_image_hash: Some(planned.preimage),
            proposed_hash: planned.proposed.manifest_hash(),
        };
        let group_id = {
            let mut store = lock_store(&self.store);
            if store.input_version() != base
                || store
                    .schema_manifest_basis()
                    .map_err(|error| error.to_string())?
                    .as_ref()
                    != Some(&planned.request.manifest)
            {
                return Err("schema transition durable basis is stale".to_owned());
            }
            store
                .preview_input_transaction(|transaction| {
                    apply_schema_transition(transaction, &transition).map(|_| ())
                })
                .map_err(|error| error.to_string())?;
            let mut publication = quarantine
                .admit_publication(&mut store)
                .map_err(|error| error.to_string())?;
            let basis = SchemaTransitionJournalBasis {
                base,
                target: planned.target.clone(),
                old_manifest_hash: planned.preimage,
                proposed_manifest_hash: planned.proposed.manifest_hash(),
            }
            .encode()?;
            let group = publication
                .record_group(PublicationGroupKind::SchemaTransition, &basis, &[plan])
                .map_err(|error| error.to_string())?;
            write_planned_temp(&temp, &planned.proposed_bytes)?;
            publication
                .arm_group(group.group_id)
                .map_err(|error| error.to_string())?;
            let outcome = publication
                .resume_group_replace(group.child_intents[0], &planned.target)
                .map_err(|error| error.to_string())?;
            if outcome != RenameAsideOutcome::Installed {
                publication
                    .retire_group(group.group_id)
                    .map_err(|error| error.to_string())?;
                return Err(
                    "schema manifest changed concurrently; its bytes were preserved".to_owned(),
                );
            }
            group.group_id
        };

        let projection = self.authoring.pipeline_projection();
        let mut transition_paths = planned.waiting_paths.clone();
        transition_paths.push(planned.target.clone());
        transition_paths.sort();
        transition_paths.dedup();
        let commit = publish_incremental_paths_with_schema_transition(
            &self.scanner,
            &self.scan_snapshot,
            &transition_paths,
            &self
                .lineage_destination
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            &self.store,
            base,
            &projection,
            Some(self),
            Some(&transition),
        )?;

        let mut terminal_errors = Vec::new();
        if let Err(error) = lock_store(&self.store).retire_publication_group(group_id) {
            terminal_errors.push(format!(
                "schema transition committed but journal retirement failed: {error}"
            ));
        }

        if matches!(commit.pipeline, Some(PipelineDiagnostic::Ready)) {
            match runtime.pending.take() {
                Some(pending) => {
                    match PipelineCandidateIdentity::try_from(pending.stored.epoch()) {
                        Ok(actual) if actual == planned.request.candidate => {
                            runtime.host.install_ready(pending.loaded);
                            self.authoring
                                .install_pipeline_importers(prepared_importers);
                        }
                        Ok(_) => {
                            runtime.pending = Some(pending);
                            terminal_errors.push(
                                "schema transition committed but the reserved candidate identity changed"
                                    .to_owned(),
                            );
                        }
                        Err(error) => {
                            runtime.pending = Some(pending);
                            terminal_errors.push(format!(
                                "schema transition committed but the reserved candidate identity became invalid: {error}"
                            ));
                        }
                    }
                }
                None => terminal_errors.push(
                    "schema transition committed but the reserved candidate disappeared".to_owned(),
                ),
            }
        }

        Ok(distill_rpc::DeferredOperationResult {
            commit,
            terminal_error: (!terminal_errors.is_empty()).then(|| terminal_errors.join("; ")),
        })
    }

    fn publish_pipeline_rejection_inner(
        &self,
        poison: PipelinePoison,
        heal_configuration: bool,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let healed_configuration = heal_configuration
            .then(|| self.configuration_without_source_poison())
            .transpose()?;
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
            let generation = match store
                .configuration_state()
                .map_err(|error| error.to_string())?
            {
                ConfigurationState::Ready(epoch) => epoch.generation,
                ConfigurationState::Poisoned { last_good, .. } => {
                    last_good.map_or(0, |epoch| epoch.generation)
                }
            };
            store
                .input_transaction(|transaction| {
                    transaction.publish_pipeline_poison(&diagnostic)?;
                    match healed_configuration
                        .as_ref()
                        .map(|(configuration, _)| configuration)
                    {
                        Some(ConfigurationStatus::Ready) => {
                            transaction.publish_configuration_ready(generation)?
                        }
                        Some(ConfigurationStatus::Poisoned(poison)) => transaction
                            .publish_configuration_poison(&poison.detail, &poison.message)?,
                        None => {}
                    }
                    Ok(())
                })
                .map_err(|error| error.to_string())?;
            Ok(Commit {
                configuration: healed_configuration
                    .as_ref()
                    .map(|(configuration, _)| configuration.clone()),
                lineage_repair: healed_configuration
                    .as_ref()
                    .map(|(_, repair)| repair.clone()),
                pipeline: Some(PipelineDiagnostic::Poisoned(diagnostic.clone())),
                pipeline_epoch_changed: true,
                ..Commit::default()
            })
        });
        match result {
            Ok(stamp) => {
                let mut runtime = lock_pipeline(&self.pipeline);
                if let Some(cleanup) = discard_pending(&mut runtime) {
                    drop(runtime);
                    return self.publish_pipeline_rejection_inner(cleanup, false);
                }
                runtime.host.install_poison(poison);
                self.authoring.install_pipeline_importers(BTreeMap::new());
                if heal_configuration {
                    self.configuration_poison
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take();
                }
                Ok(stamp)
            }
            Err(error) => Err(CoordinatorError::Coordinated(error)),
        }
    }

    pub fn reap_retired_pipeline_epochs(&self) -> Vec<UnloadOutcome> {
        lock_pipeline(&self.pipeline).host.reap_retired()
    }

    /// Reconcile one complete identity-checked namespace scan.
    pub fn reconcile_full_scan(&self) -> Result<SnapshotStamp, CoordinatorError> {
        match self.scanner.scan() {
            Ok(scan) if self.scan_healthy.load(Ordering::Acquire) => {
                let mut baseline = lock_scan_snapshot(&self.scan_snapshot);
                if scan.same_namespace_observation(&baseline) {
                    // Warning-grade exclusions are scanner state, not authored
                    // input: refresh them without minting an input version.
                    *baseline = scan;
                    Ok(self.server.current_stamp())
                } else {
                    drop(baseline);
                    self.publish_scan(scan)
                }
            }
            Ok(scan) => self.publish_scan(scan),
            Err(error) => {
                self.scan_healthy.store(false, Ordering::Release);
                self.publish_scan_rejection(&error, true)
            }
        }
    }

    /// Arm the watcher before startup/recovery traversal. Precise events are
    /// replayed incrementally after the scan commits; only admitted overflow
    /// repeats the complete scan.
    pub fn reconcile_startup(
        &self,
        watcher: &Mutex<WatcherQueue>,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        loop {
            lock_watcher(watcher).arm_scan();
            let scan = self.reconcile_full_scan();
            let action = lock_watcher(watcher).finish_scan();
            let stamp = match scan {
                Ok(stamp) => stamp,
                Err(error) => {
                    // Always release scan ownership. Preserve anything that
                    // arrived while traversal was active so a caller that
                    // retries can still reconcile the complete event union.
                    lock_watcher(watcher).requeue_action(action);
                    return Err(error);
                }
            };
            match action {
                WatcherAction::None => return Ok(stamp),
                WatcherAction::Batch(batch) => return self.reconcile_incremental(&batch),
                WatcherAction::FullRescan => continue,
                WatcherAction::Failed(message) => {
                    return Err(CoordinatorError::InvalidManifest(message))
                }
            }
        }
    }

    /// Apply one native watcher batch by reopening only its affected paths or
    /// directory subtrees and merging those observations into startup state.
    pub fn reconcile_incremental(
        &self,
        batch: &WatcherBatch,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let pending_subjects = self
            .scan_rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|pending| pending.subjects.clone())
            .unwrap_or_default();
        let event_keys = batch
            .paths
            .iter()
            .filter_map(|event| self.scanner.event_path_key(event).ok().flatten())
            .collect::<Vec<_>>();
        let logical_contains = |prefix: &(String, String), candidate: &(String, String)| {
            prefix.0 == candidate.0
                && (prefix.1.is_empty()
                    || candidate.1 == prefix.1
                    || candidate
                        .1
                        .strip_prefix(&prefix.1)
                        .is_some_and(|suffix| suffix.starts_with('/')))
        };
        let heals_pending_rejection = pending_subjects.iter().any(|subject| {
            let physical_overlap = batch.paths.iter().any(|event| {
                event == subject || subject.starts_with(event) || event.starts_with(subject)
            });
            physical_overlap
                || self
                    .scanner
                    .event_path_key(subject)
                    .ok()
                    .flatten()
                    .is_some_and(|subject_key| {
                        event_keys.iter().any(|event_key| {
                            logical_contains(&subject_key, event_key)
                                || logical_contains(event_key, &subject_key)
                        })
                    })
        });
        let mut scan_paths = batch.paths.clone();
        if heals_pending_rejection {
            // Revalidate the complete rejected subject set, but no unrelated
            // root or subtree. This lets independently repaired defects heal
            // across separate native batches without falling back to a full
            // scan.
            scan_paths.extend(pending_subjects);
            scan_paths.sort_unstable();
            scan_paths.dedup();
        }
        let mut renames = Vec::new();
        for rename in &batch.renames {
            let from = self.scanner.event_path_key(&rename.from)?;
            let to = self.scanner.event_path_key(&rename.to)?;
            if let (Some((from_root, from_path)), Some((to_root, to_path))) = (from, to) {
                if from_root == to_root {
                    renames.push(LogicalRename {
                        root_name: from_root,
                        from_path,
                        to_path,
                    });
                }
            }
        }
        let mut baseline = lock_scan_snapshot(&self.scan_snapshot);
        let delta = match self.scanner.scan_incremental_delta(&baseline, &scan_paths) {
            Ok(None) => return Ok(self.server.current_stamp()),
            Ok(Some(delta)) => delta,
            Err(error) => {
                drop(baseline);
                self.scan_healthy.store(false, Ordering::Release);
                return self.publish_scan_rejection(&error, heals_pending_rejection);
            }
        };
        let healed_rejection = heals_pending_rejection
            .then(|| {
                self.scan_rejection
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
            })
            .flatten();
        if delta.is_same_namespace_observation(&baseline)
            && renames.is_empty()
            && self.scan_healthy.load(Ordering::Acquire)
        {
            // Diagnostics are replaced with their affected subtree even when
            // the authored namespace itself did not change.
            baseline.apply_delta(delta);
            return Ok(self.server.current_stamp());
        }
        let projection = self.authoring.pipeline_projection();
        let authority = self.schema_authority();
        let mut index = lock_scan_projection(&self.scan_projection);
        let checkpoint = index.checkpoint(delta.affected_prefixes());
        let prepared = index
            .replace_delta(&delta, &projection, authority.as_deref())
            .and_then(|_| {
                index.incremental_plan(
                    &self.scanner,
                    &self.lineage_destination(),
                    self.configuration_poison(),
                )
            });
        let mut plan = match prepared {
            Ok(plan) => plan,
            Err(error) => {
                index.restore(checkpoint)?;
                if let Some(rejection) = healed_rejection {
                    *self
                        .scan_rejection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(rejection);
                }
                return Err(error);
            }
        };
        plan.version_poison = VersionPoison::select_canonical(
            plan.version_poison
                .take()
                .into_iter()
                .chain(self.pending_scan_version_poison()),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        let tag_epoch = authority
            .as_ref()
            .map_or([0; 32], |authority| authority.source_hash());
        let pipeline = self.pipeline_snapshot();
        let targets = self
            .build_targets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let max_dependency_depth = self.operational_configuration().max_dependency_depth;
        let scanner = self.scanner.clone();
        let result = self.server.coordinated_commit(base, || {
            let mut commit = publish_incremental_scan(
                &store,
                base,
                &baseline,
                &delta,
                &plan,
                &renames,
                &projection,
                tag_epoch,
                None,
            )
            .map_err(|error| error.to_string())?;
            if let Some(authority) = authority {
                let affected = commit_affected_asset_bundles(&commit);
                if !affected.is_empty() {
                    crate::build::refine_published_tag_index_incremental(
                        Arc::clone(&store),
                        scanner,
                        authority,
                        pipeline,
                        &targets,
                        max_dependency_depth,
                        &affected,
                    )
                    .apply_incremental(&mut commit);
                }
            }
            Ok(commit)
        });
        match result {
            Ok(stamp) => {
                baseline.apply_delta(delta);
                if plan.version_poison.is_none() {
                    index.clear_published_pending();
                }
                self.scan_healthy.store(
                    self.scan_rejection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .is_none(),
                    Ordering::Release,
                );
                Ok(stamp)
            }
            Err(error) => {
                index.restore(checkpoint)?;
                if let Some(rejection) = healed_rejection {
                    *self
                        .scan_rejection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(rejection);
                }
                Err(CoordinatorError::Coordinated(error))
            }
        }
    }

    fn publish_cached_scan(&self) -> Result<SnapshotStamp, CoordinatorError> {
        let scan = lock_scan_snapshot(&self.scan_snapshot).clone();
        self.publish_scan_with_renames(scan, &[], false)
    }

    fn publish_scan(&self, scan: ScanSnapshot) -> Result<SnapshotStamp, CoordinatorError> {
        self.publish_scan_with_renames(scan, &[], true)
    }

    fn publish_scan_with_renames(
        &self,
        scan: ScanSnapshot,
        renames: &[LogicalRename],
        heals_scan_rejection: bool,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let published_snapshot = scan.clone();
        let authority = self.schema_authority();
        let mut candidate = ScanCandidate::build(
            &self.scanner,
            &self.lineage_destination(),
            scan,
            self.configuration_poison(),
            authority.as_deref(),
        )?;
        if !heals_scan_rejection {
            candidate.version_poison = VersionPoison::select_canonical(
                candidate
                    .version_poison
                    .take()
                    .into_iter()
                    .chain(self.pending_scan_version_poison()),
            )
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        }
        candidate.renames.extend_from_slice(renames);
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        let projection = self.authoring.pipeline_projection();
        let published_projection =
            ScanProjectionIndex::build(&published_snapshot, &projection, authority.as_deref())?;
        let tag_epoch = authority
            .as_ref()
            .map_or([0; 32], |authority| authority.source_hash());
        let pipeline = self.pipeline_snapshot();
        let build_targets = self
            .build_targets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let max_dependency_depth = self.operational_configuration().max_dependency_depth;
        let scanner = self.scanner.clone();
        let fallback_bundles = lock_store(&store)
            .all_asset_bundles()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let stamp = self
            .server
            .coordinated_commit(base, || {
                let mut commit =
                    publish_scan(&store, base, candidate, false, None, &projection, tag_epoch)
                        .map_err(|error| error.to_string())?;
                if let Some(authority) = authority {
                    crate::build::refine_published_tag_index(
                        Arc::clone(&store),
                        scanner,
                        authority,
                        pipeline,
                        &build_targets,
                        max_dependency_depth,
                        &commit_asset_bundles(&commit, &fallback_bundles),
                    )
                    .apply(&mut commit);
                }
                Ok(commit)
            })
            .map_err(CoordinatorError::Coordinated)?;
        *lock_scan_snapshot(&self.scan_snapshot) = published_snapshot;
        *lock_scan_projection(&self.scan_projection) = published_projection;
        self.scan_initialized.store(true, Ordering::Release);
        if heals_scan_rejection {
            self.scan_rejection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
        self.scan_healthy.store(
            self.scan_rejection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none(),
            Ordering::Release,
        );
        Ok(stamp)
    }

    fn publish_scan_rejection(
        &self,
        error: &ScanError,
        replaces_pending: bool,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let observed_rejection = classify_scan_rejection(&self.scanner, error)?;
        let previous_pending = self
            .scan_rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let rejection = if replaces_pending {
            observed_rejection
        } else {
            select_scan_rejection(
                previous_pending
                    .as_ref()
                    .map(|pending| pending.rejection.clone())
                    .into_iter()
                    .chain([observed_rejection]),
            )?
        };
        let mut subjects = self.scanner.rejection_subjects(error);
        if !replaces_pending {
            if let Some(previous) = &previous_pending {
                subjects.extend(previous.subjects.iter().cloned());
            }
        }
        subjects.sort_unstable();
        subjects.dedup();
        let pending = PendingScanRejection {
            rejection: rejection.clone(),
            subjects,
        };
        let indexed_version = lock_scan_projection(&self.scan_projection).version_poison()?;
        let durable_version = lock_store(&self.store)
            .version_poison()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let baseline_version = match (indexed_version, durable_version) {
            (Some(indexed), Some(durable)) if indexed.identity == durable.identity => Some(durable),
            (indexed, _) => indexed,
        };
        let version = VersionPoison::select_canonical(
            baseline_version
                .into_iter()
                .chain(rejection.version.clone()),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let source_configuration = self
            .configuration_poison
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let external_configuration = ConfigurationPoison::select_canonical(
            source_configuration
                .into_iter()
                .chain(rejection.configuration.clone()),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let (configuration, lineage_repair) = if self.scan_initialized.load(Ordering::Acquire) {
            let index = lock_scan_projection(&self.scan_projection);
            let (configuration, repair, _) = indexed_lineage_projection(
                &index,
                &self.scanner,
                &self.lineage_destination(),
                external_configuration,
            )?;
            (configuration, repair)
        } else {
            (
                external_configuration
                    .map_or(ConfigurationStatus::Ready, ConfigurationStatus::Poisoned),
                None,
            )
        };
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        let stamp = self
            .server
            .coordinated_commit(base, || {
                let mut store = lock_store(&store);
                if store.input_version() != base {
                    return Err(format!(
                        "durable rejected-scan basis is {:?}, expected {base:?}",
                        store.input_version()
                    ));
                }
                let generation = match store
                    .configuration_state()
                    .map_err(|error| error.to_string())?
                {
                    ConfigurationState::Ready(epoch) => epoch.generation,
                    ConfigurationState::Poisoned { last_good, .. } => {
                        last_good.map_or(0, |epoch| epoch.generation)
                    }
                };
                store
                    .input_transaction(|transaction| {
                        transaction.set_version_poisons(version.clone())?;
                        match &configuration {
                            ConfigurationStatus::Ready => {
                                transaction.publish_configuration_ready(generation)?
                            }
                            ConfigurationStatus::Poisoned(poison) => transaction
                                .publish_configuration_poison(&poison.detail, &poison.message)?,
                        }
                        Ok(())
                    })
                    .map_err(|error| error.to_string())?;
                let commit = Commit {
                    configuration: Some(configuration.clone()),
                    version_poison: Some(version.clone()),
                    lineage_repair: Some(lineage_repair.clone()),
                    ..Commit::default()
                };
                Ok(commit)
            })
            .map_err(CoordinatorError::Coordinated)?;
        *self
            .scan_rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(pending);
        Ok(stamp)
    }

    /// Rerun watched imports whose complete outcome-bearing basis drifted.
    /// Each bundle publishes as its own version so a later conflict cannot
    /// roll back an earlier per-file success.
    pub fn reconcile_watched_imports(&self) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let pending = self
            .authoring
            .watched_imports_needing_reimport()
            .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        self.reconcile_watched_import_bundles(pending)
    }

    /// Watcher-batch variant that revalidates only read sets capable of
    /// observing one of the transactionally queued dirty paths.
    pub fn reconcile_watched_imports_affected(
        &self,
        work: &PendingFileWork,
        capabilities_changed: bool,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let pending = if capabilities_changed {
            self.authoring
                .watched_imports_affected_by_capabilities(&work.dirty, &work.renames)
        } else {
            self.authoring
                .watched_imports_affected_by(&work.dirty, &work.renames)
        }
        .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        self.reconcile_watched_import_bundles(pending)
    }

    fn reconcile_watched_import_bundles(
        &self,
        pending: Vec<BundleUuid>,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
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

    pub fn pending_file_work(&self) -> Result<PendingFileWork, CoordinatorError> {
        lock_store(&self.store)
            .pending_file_work()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    pub fn acknowledge_file_work(&self, work: &PendingFileWork) -> Result<(), CoordinatorError> {
        if work.is_empty() {
            return Ok(());
        }
        match lock_store(&self.store).acknowledge_file_work(work) {
            Ok(true) => Ok(()),
            Ok(false) => Err(CoordinatorError::InvalidManifest(
                "watcher work observation changed before acknowledgement".to_owned(),
            )),
            Err(error) => Err(CoordinatorError::InvalidManifest(error.to_string())),
        }
    }

    /// Discover and apply authored directory-import rules. Every generated
    /// bundle is a separate journaled/versioned fold; orphaned prior outputs
    /// are deliberately retained and therefore never appear as deletion work.
    pub fn reconcile_directory_imports(&self) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let tasks = self
            .authoring
            .directory_import_tasks()
            .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        self.reconcile_directory_import_tasks(tasks)
    }

    pub fn reconcile_directory_imports_affected(
        &self,
        work: &PendingFileWork,
        capabilities_changed: bool,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let tasks = if capabilities_changed {
            self.authoring
                .directory_import_tasks_affected_by_capabilities(&work.dirty, &work.renames)
        } else {
            self.authoring
                .directory_import_tasks_affected_by(&work.dirty, &work.renames)
        }
        .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        self.reconcile_directory_import_tasks(tasks)
    }

    fn reconcile_directory_import_tasks(
        &self,
        tasks: Vec<crate::importer::DirectoryImportTask>,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
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

#[derive(Clone)]
struct ScanRejection {
    version: Option<VersionPoison>,
    configuration: Option<ConfigurationPoison>,
}

#[derive(Clone)]
struct PendingScanRejection {
    rejection: ScanRejection,
    subjects: Vec<PathBuf>,
}

fn select_scan_rejection(
    rejections: impl IntoIterator<Item = ScanRejection>,
) -> Result<ScanRejection, CoordinatorError> {
    let rejections = rejections.into_iter().collect::<Vec<_>>();
    let version =
        VersionPoison::select_canonical(rejections.iter().filter_map(|item| item.version.clone()))
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
    let configuration = ConfigurationPoison::select_canonical(
        rejections.into_iter().filter_map(|item| item.configuration),
    )
    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
    Ok(ScanRejection {
        version,
        configuration,
    })
}

fn classify_scan_rejection(
    scanner: &RootedScanner,
    error: &ScanError,
) -> Result<ScanRejection, CoordinatorError> {
    if let ScanError::Multiple(errors) = error {
        let classified = errors
            .iter()
            .map(|error| classify_scan_rejection(scanner, error))
            .collect::<Result<Vec<_>, _>>()?;
        return select_scan_rejection(classified);
    }
    if let ScanError::InvalidPhysicalPath {
        root_name,
        raw_relative_path,
        failure,
    } = error
    {
        let poison = VersionPoison::new(
            VersionPoisonV1::InvalidPhysicalPath {
                root_name: root_name.clone(),
                raw_relative_path: raw_relative_path.clone(),
                failure: *failure,
            },
            error.to_string(),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        return Ok(ScanRejection {
            version: Some(poison),
            configuration: None,
        });
    }
    if let ScanError::SameRootNormalizedPathCollision {
        root_name,
        normalized_path,
        claims,
    } = error
    {
        let poison = VersionPoison::new(
            VersionPoisonV1::SameRootNormalizedPathCollision {
                root_name: root_name.clone(),
                normalized_path: normalized_path.clone(),
                claims: claims.clone(),
            },
            error.to_string(),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        return Ok(ScanRejection {
            version: Some(poison),
            configuration: None,
        });
    }
    if let ScanError::DirectoryAlias {
        first_root,
        first,
        second_root,
        second,
    } = error
    {
        let reason = DscpV1::DirectoryAlias {
            first: DirectoryAliasSide {
                normalized_path: scanner.normalized_observed_path(first_root, first),
            },
            second: DirectoryAliasSide {
                normalized_path: scanner.normalized_observed_path(second_root, second),
            },
        };
        return Ok(ScanRejection {
            version: None,
            configuration: Some(ConfigurationPoison::from_reason(&reason, error.to_string())),
        });
    }
    if let ScanError::DaemonOwnedDirectoryAlias {
        path,
        owned_path,
        kind,
    } = error
    {
        use unicode_normalization::UnicodeNormalization;

        let reason = DscpV1::DirectoryAlias {
            first: DirectoryAliasSide {
                normalized_path: scanner.normalized_observed_subject(path),
            },
            second: DirectoryAliasSide {
                normalized_path: format!(
                    "daemon-owned:{kind}:{}",
                    owned_path.to_string_lossy().nfc().collect::<String>()
                ),
            },
        };
        return Ok(ScanRejection {
            version: None,
            configuration: Some(ConfigurationPoison::from_reason(&reason, error.to_string())),
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
        ScanError::RootUnavailable { path, .. } => {
            (Some(path.as_path()), ScanFailureCode::NotFound)
        }
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
        | ScanError::Multiple(_)
        | ScanError::DuplicateRootName(_)
        | ScanError::UnknownRoot(_)
        | ScanError::InvalidPhysicalPath { .. }
        | ScanError::SameRootNormalizedPathCollision { .. }
        | ScanError::DirectoryAlias { .. }
        | ScanError::DaemonOwnedDirectoryAlias { .. } => (None, ScanFailureCode::IoDataLoss),
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
    Ok(ScanRejection {
        version: Some(poison),
        configuration: None,
    })
}

#[derive(Debug)]
pub enum CoordinatorInitError {
    Store(StoreError),
    Scan(ScanError),
    Repair(LineageRepairBackendInitError),
    Authoring(AuthoringServiceInitError),
    Rpc(distill_rpc::TargetSetError),
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
impl From<distill_rpc::TargetSetError> for CoordinatorInitError {
    fn from(error: distill_rpc::TargetSetError) -> Self {
        Self::Rpc(error)
    }
}

#[derive(Debug)]
pub enum CoordinatorError {
    Scan(ScanError),
    InvalidManifest(String),
    Coordinated(CoordinatedCommitError),
    RuntimePipeline(String),
    Maintenance(String),
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

type ScanKey = (String, String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CollisionKey {
    Bundle(BundleUuid),
    Asset(AssetUuid),
}

#[derive(Debug, Clone)]
struct IndexedSourceClaims {
    source: Arc<ScannedBundle>,
    bundle: Option<BundleUuid>,
    authored: Vec<(AssetUuid, AssetClaimant)>,
    derived: Vec<(AssetUuid, AssetClaimant, DerivedOutputEntry)>,
    lineage: Vec<(LineageManifestClaimant, VerifiedSchemaLineageManifest)>,
    primary_path: Option<(String, AssetUuid)>,
    malformed: Option<VersionPoison>,
    scoped_poison: Option<ScopedBundlePoison>,
}

#[derive(Debug, Clone)]
struct ScopedBundlePoison {
    bundle: BundleUuid,
    root_name: String,
    normalized_path: String,
    content_hash: ContentHash,
    format_version: u32,
    entries: Vec<SkeletonEntry>,
    message: String,
}

fn scoped_bundle_poison(
    source: &ScannedBundle,
    authority: Option<&ProjectSchemaAuthority>,
) -> Option<ScopedBundlePoison> {
    let skeleton = source.namespace_skeleton.as_ref()?;
    let error = source.parsed.as_ref().err()?;
    let mut entries = Vec::with_capacity(skeleton.assets.len());
    for (local_id, entry) in &skeleton.assets {
        let tags = if is_bootstrap_control_type(entry.type_uuid) {
            BTreeMap::new()
        } else {
            let authority = authority?;
            let project = authority.project_type(entry.type_uuid)?;
            if project.logical_hash != entry.schema_hash {
                return None;
            }
            distill_schema::extract_search_tags(
                authority.schema(),
                project.schema_type,
                &entry.data,
            )
            .ok()?
            .into_iter()
            .map(|(name, value)| (name, Some(value)))
            .collect()
        };
        entries.push(SkeletonEntry {
            asset: entry.uuid,
            local_id: local_id.clone(),
            type_uuid: entry.type_uuid,
            authoring_only: entry.authoring_only,
            tags,
        });
    }
    Some(ScopedBundlePoison {
        bundle: skeleton.uuid,
        root_name: source.root_name.clone(),
        normalized_path: source.normalized_path.clone(),
        content_hash: ContentHash(source.file_hash.0),
        format_version: skeleton.format_version,
        entries,
        message: error.to_string(),
    })
}

impl IndexedSourceClaims {
    fn build(
        source: Arc<ScannedBundle>,
        projection: &PipelineProjection,
        authority: Option<&ProjectSchemaAuthority>,
    ) -> Result<Self, CoordinatorError> {
        let readable = readable_source(&source);
        let bundle = match &source.parsed {
            Ok(bundle) => bundle,
            Err(error) => {
                if let Some(scoped_poison) = scoped_bundle_poison(&source, authority) {
                    let mut authored = Vec::with_capacity(scoped_poison.entries.len());
                    let mut derived = Vec::new();
                    for entry in &scoped_poison.entries {
                        authored.push((
                            entry.asset,
                            AssetClaimant::Authored {
                                source: readable.clone(),
                                bundle: scoped_poison.bundle,
                                local_id: entry.local_id.clone(),
                            },
                        ));
                        for (child, output) in
                            projection.derived_outputs([(entry.asset, entry.type_uuid)])
                        {
                            derived.push((
                                child,
                                AssetClaimant::Derived {
                                    parent: entry.asset,
                                    output_key: output.output_key.clone(),
                                },
                                output,
                            ));
                        }
                    }
                    return Ok(Self {
                        source,
                        bundle: Some(scoped_poison.bundle),
                        authored,
                        derived,
                        lineage: Vec::new(),
                        primary_path: None,
                        malformed: None,
                        scoped_poison: Some(scoped_poison),
                    });
                }
                let poison = VersionPoison::new(
                    VersionPoisonV1::IncompleteSkeleton {
                        source: readable,
                        failure: skeleton_failure(error),
                    },
                    error.to_string(),
                )
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                return Ok(Self {
                    source,
                    bundle: None,
                    authored: Vec::new(),
                    derived: Vec::new(),
                    lineage: Vec::new(),
                    primary_path: None,
                    malformed: Some(poison),
                    scoped_poison: None,
                });
            }
        };
        crate::importer::decoded_directory_origin(bundle).map_err(|error| {
            CoordinatorError::InvalidManifest(format!(
                "invalid import record in {}: {error:?}",
                source.normalized_path
            ))
        })?;
        let mut authored = Vec::with_capacity(bundle.assets.len());
        let mut derived = Vec::new();
        let mut lineage = Vec::new();
        for (local_id, entry) in &bundle.assets {
            authored.push((
                entry.uuid,
                AssetClaimant::Authored {
                    source: readable.clone(),
                    bundle: bundle.uuid,
                    local_id: local_id.clone(),
                },
            ));
            for (child, output) in projection.derived_outputs([(entry.uuid, entry.type_uuid)]) {
                derived.push((
                    child,
                    AssetClaimant::Derived {
                        parent: entry.uuid,
                        output_key: output.output_key.clone(),
                    },
                    output,
                ));
            }
            if entry.type_uuid == SCHEMA_LINEAGE_MANIFEST_TYPE_UUID {
                let claimant = LineageManifestClaimant {
                    root_name: source.root_name.clone(),
                    normalized_path: source.normalized_path.clone(),
                    bundle: bundle.uuid,
                    local_id: local_id.clone(),
                    asset: entry.uuid,
                    file_hash: source.file_hash,
                };
                let manifest =
                    decode_lineage_manifest(&entry.data, ContentHash(source.file_hash.0))?;
                lineage.push((claimant, manifest));
            }
        }
        let primary_path = bundle
            .primary
            .as_ref()
            .map(|primary| (source.normalized_path.clone(), bundle.assets[primary].uuid));
        Ok(Self {
            source: Arc::clone(&source),
            bundle: Some(bundle.uuid),
            authored,
            derived,
            lineage,
            primary_path,
            malformed: None,
            scoped_poison: None,
        })
    }
}

#[derive(Debug, Default)]
struct ProjectionTouches {
    bundles: BTreeSet<BundleUuid>,
    assets: BTreeSet<AssetUuid>,
    derived: BTreeSet<AssetUuid>,
    paths: BTreeSet<String>,
}

impl ProjectionTouches {
    fn extend(&mut self, other: Self) {
        self.bundles.extend(other.bundles);
        self.assets.extend(other.assets);
        self.derived.extend(other.derived);
        self.paths.extend(other.paths);
    }
}

#[derive(Debug, Default)]
struct ScanProjectionIndex {
    sources: BTreeMap<ScanKey, IndexedSourceClaims>,
    bundle_claimants: BTreeMap<BundleUuid, BTreeSet<ReadableBundleSource>>,
    asset_claimants: BTreeMap<AssetUuid, BTreeSet<AssetClaimant>>,
    derived_outputs: BTreeMap<AssetUuid, BTreeMap<AssetClaimant, DerivedOutputEntry>>,
    lineage: BTreeMap<LineageManifestClaimant, VerifiedSchemaLineageManifest>,
    malformed: BTreeMap<ScanKey, VersionPoison>,
    collisions: BTreeMap<CollisionKey, VersionPoison>,
    path_claimants: BTreeMap<String, BTreeSet<AssetUuid>>,
    pending_bundles: BTreeSet<BundleUuid>,
    pending_assets: BTreeSet<AssetUuid>,
    pending_derived: BTreeSet<AssetUuid>,
    pending_paths: BTreeSet<String>,
}

struct ProjectionCheckpoint {
    prefixes: Vec<ScanKey>,
    sources: Vec<(ScanKey, IndexedSourceClaims)>,
    pending_bundles: BTreeSet<BundleUuid>,
    pending_assets: BTreeSet<AssetUuid>,
    pending_derived: BTreeSet<AssetUuid>,
    pending_paths: BTreeSet<String>,
}

impl ScanProjectionIndex {
    fn checkpoint(&self, prefixes: &[ScanKey]) -> ProjectionCheckpoint {
        ProjectionCheckpoint {
            prefixes: prefixes.to_vec(),
            sources: self
                .sources
                .iter()
                .filter(|(key, _)| prefixes.iter().any(|prefix| scan_key_matches(prefix, key)))
                .map(|(key, claims)| (key.clone(), claims.clone()))
                .collect(),
            pending_bundles: self.pending_bundles.clone(),
            pending_assets: self.pending_assets.clone(),
            pending_derived: self.pending_derived.clone(),
            pending_paths: self.pending_paths.clone(),
        }
    }

    fn restore(&mut self, checkpoint: ProjectionCheckpoint) -> Result<(), CoordinatorError> {
        let current = self
            .sources
            .keys()
            .filter(|key| {
                checkpoint
                    .prefixes
                    .iter()
                    .any(|prefix| scan_key_matches(prefix, key))
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut touched = ProjectionTouches::default();
        for key in current {
            touched.extend(self.remove_source(&key));
        }
        for (key, claims) in checkpoint.sources {
            touched.extend(self.insert_source(key, claims)?);
        }
        self.refresh_collisions(&touched)?;
        self.pending_bundles = checkpoint.pending_bundles;
        self.pending_assets = checkpoint.pending_assets;
        self.pending_derived = checkpoint.pending_derived;
        self.pending_paths = checkpoint.pending_paths;
        Ok(())
    }

    fn build(
        scan: &ScanSnapshot,
        projection: &PipelineProjection,
        authority: Option<&ProjectSchemaAuthority>,
    ) -> Result<Self, CoordinatorError> {
        let mut index = Self::default();
        let mut touched = ProjectionTouches::default();
        for (key, source) in scan.bundle_entries() {
            let claims = IndexedSourceClaims::build(Arc::clone(source), projection, authority)?;
            touched.extend(index.insert_source(key.clone(), claims)?);
        }
        index.refresh_collisions(&touched)?;
        index.pending_bundles.clear();
        index.pending_assets.clear();
        index.pending_derived.clear();
        index.pending_paths.clear();
        Ok(index)
    }

    fn replace_delta(
        &mut self,
        delta: &ScanDelta,
        projection: &PipelineProjection,
        authority: Option<&ProjectSchemaAuthority>,
    ) -> Result<ProjectionTouches, CoordinatorError> {
        let mut touched = ProjectionTouches::default();
        let removed = self
            .sources
            .keys()
            .filter(|key| {
                delta
                    .affected_prefixes()
                    .iter()
                    .any(|prefix| scan_key_matches(prefix, key))
            })
            .cloned()
            .collect::<Vec<_>>();
        for key in removed {
            touched.extend(self.remove_source(&key));
        }
        for (key, source) in delta.observed_bundle_entries() {
            let claims = IndexedSourceClaims::build(Arc::clone(source), projection, authority)?;
            touched.extend(self.insert_source(key.clone(), claims)?);
        }
        self.refresh_collisions(&touched)?;
        self.pending_bundles.extend(&touched.bundles);
        self.pending_assets.extend(&touched.assets);
        self.pending_derived.extend(&touched.derived);
        self.pending_paths.extend(touched.paths.iter().cloned());
        Ok(touched)
    }

    fn insert_source(
        &mut self,
        key: ScanKey,
        claims: IndexedSourceClaims,
    ) -> Result<ProjectionTouches, CoordinatorError> {
        debug_assert!(!self.sources.contains_key(&key));
        let mut touched = ProjectionTouches::default();
        if let Some(poison) = &claims.malformed {
            self.malformed.insert(key.clone(), poison.clone());
        }
        if let Some(bundle) = claims.bundle {
            touched.bundles.insert(bundle);
            self.bundle_claimants
                .entry(bundle)
                .or_default()
                .insert(readable_source(&claims.source));
        }
        for (asset, claimant) in &claims.authored {
            touched.assets.insert(*asset);
            self.asset_claimants
                .entry(*asset)
                .or_default()
                .insert(claimant.clone());
        }
        for (asset, claimant, output) in &claims.derived {
            touched.derived.insert(*asset);
            self.asset_claimants
                .entry(*asset)
                .or_default()
                .insert(claimant.clone());
            self.derived_outputs
                .entry(*asset)
                .or_default()
                .insert(claimant.clone(), output.clone());
        }
        for (claimant, manifest) in &claims.lineage {
            self.lineage.insert(claimant.clone(), manifest.clone());
        }
        if let Some((path, asset)) = &claims.primary_path {
            touched.paths.insert(path.clone());
            self.path_claimants
                .entry(path.clone())
                .or_default()
                .insert(*asset);
        }
        self.sources.insert(key, claims);
        Ok(touched)
    }

    fn remove_source(&mut self, key: &ScanKey) -> ProjectionTouches {
        let Some(claims) = self.sources.remove(key) else {
            return ProjectionTouches::default();
        };
        let mut touched = ProjectionTouches::default();
        self.malformed.remove(key);
        if let Some(bundle) = claims.bundle {
            touched.bundles.insert(bundle);
            remove_set_value(
                &mut self.bundle_claimants,
                &bundle,
                &readable_source(&claims.source),
            );
        }
        for (asset, claimant) in claims.authored {
            touched.assets.insert(asset);
            remove_set_value(&mut self.asset_claimants, &asset, &claimant);
        }
        for (asset, claimant, _) in claims.derived {
            touched.derived.insert(asset);
            remove_set_value(&mut self.asset_claimants, &asset, &claimant);
            remove_map_value(&mut self.derived_outputs, &asset, &claimant);
        }
        for (claimant, _) in claims.lineage {
            self.lineage.remove(&claimant);
        }
        if let Some((path, asset)) = claims.primary_path {
            touched.paths.insert(path.clone());
            remove_set_value(&mut self.path_claimants, &path, &asset);
        }
        touched
    }

    fn refresh_collisions(&mut self, touched: &ProjectionTouches) -> Result<(), CoordinatorError> {
        for bundle in &touched.bundles {
            let key = CollisionKey::Bundle(*bundle);
            self.collisions.remove(&key);
            if let Some(sources) = self
                .bundle_claimants
                .get(bundle)
                .filter(|set| set.len() > 1)
            {
                let sources = sources.iter().cloned().collect::<Vec<_>>();
                let poison = VersionPoison::new(
                    VersionPoisonV1::DuplicateBundleUuid {
                        bundle: *bundle,
                        sources,
                    },
                    format!("duplicate bundle UUID {bundle}"),
                )
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                self.collisions.insert(key, poison);
            }
        }
        for asset in touched.assets.iter().chain(&touched.derived) {
            let key = CollisionKey::Asset(*asset);
            self.collisions.remove(&key);
            if let Some(claimants) = self.asset_claimants.get(asset).filter(|set| set.len() > 1) {
                let claimants = claimants.iter().cloned().collect::<Vec<_>>();
                let poison = VersionPoison::new(
                    VersionPoisonV1::DuplicateAssetUuid {
                        asset: *asset,
                        claimants,
                    },
                    format!("duplicate asset UUID {asset}"),
                )
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                self.collisions.insert(key, poison);
            }
        }
        Ok(())
    }

    fn version_poison(&self) -> Result<Option<VersionPoison>, CoordinatorError> {
        VersionPoison::select_canonical(
            self.malformed
                .values()
                .chain(self.collisions.values())
                .cloned(),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    fn derived_output(&self, child: AssetUuid) -> Option<DerivedOutputEntry> {
        self.derived_outputs
            .get(&child)
            .and_then(|entries| (entries.len() == 1).then(|| entries.values().next().cloned()))
            .flatten()
    }

    fn source_for_bundle(&self, bundle: BundleUuid) -> Option<&IndexedSourceClaims> {
        let source = self.bundle_claimants.get(&bundle)?;
        if source.len() != 1 {
            return None;
        }
        let source = source.iter().next().expect("one bundle claimant");
        self.sources
            .get(&(source.root_name.clone(), source.normalized_path.clone()))
    }

    fn incremental_plan(
        &self,
        scanner: &RootedScanner,
        destination: &LineageDestination,
        external_poison: Option<ConfigurationPoison>,
    ) -> Result<IncrementalScanPlan, CoordinatorError> {
        let version_poison = self.version_poison()?;
        let (configuration, lineage_repair, lineage_manifest) =
            indexed_lineage_projection(self, scanner, destination, external_poison)?;
        let bundles = self
            .pending_bundles
            .iter()
            .copied()
            .map(|bundle| {
                (
                    bundle,
                    self.source_for_bundle(bundle)
                        .map(|claims| Arc::clone(&claims.source)),
                )
            })
            .collect();
        let bundle_poisons = self
            .pending_bundles
            .iter()
            .filter_map(|bundle| {
                self.source_for_bundle(*bundle)
                    .and_then(|claims| claims.scoped_poison.clone())
                    .map(|poison| (*bundle, poison))
            })
            .collect();
        let derived_outputs = self
            .pending_derived
            .iter()
            .copied()
            .map(|child| (child, self.derived_output(child)))
            .collect();
        let paths = self
            .pending_paths
            .iter()
            .cloned()
            .map(|path| {
                let candidates = self.path_claimants.get(&path).cloned().unwrap_or_default();
                (path, candidates)
            })
            .collect();
        Ok(IncrementalScanPlan {
            version_poison,
            configuration,
            lineage_repair,
            lineage_manifest,
            bundles,
            bundle_poisons,
            derived_outputs,
            paths,
        })
    }

    fn clear_published_pending(&mut self) {
        self.pending_bundles.clear();
        self.pending_assets.clear();
        self.pending_derived.clear();
        self.pending_paths.clear();
    }
}

struct IncrementalScanPlan {
    version_poison: Option<VersionPoison>,
    configuration: ConfigurationStatus,
    lineage_repair: Option<LineageRepairState>,
    lineage_manifest: Option<VerifiedSchemaLineageManifest>,
    bundles: BTreeMap<BundleUuid, Option<Arc<ScannedBundle>>>,
    bundle_poisons: BTreeMap<BundleUuid, ScopedBundlePoison>,
    derived_outputs: BTreeMap<AssetUuid, Option<DerivedOutputEntry>>,
    paths: BTreeMap<String, BTreeSet<AssetUuid>>,
}

fn indexed_lineage_projection(
    index: &ScanProjectionIndex,
    scanner: &RootedScanner,
    destination: &LineageDestination,
    external_poison: Option<ConfigurationPoison>,
) -> Result<
    (
        ConfigurationStatus,
        Option<LineageRepairState>,
        Option<VerifiedSchemaLineageManifest>,
    ),
    CoordinatorError,
> {
    let (lineage_configuration, mut repair, manifest) = match index.lineage.len() {
        0 => {
            let reason = DscpV1::MissingLineageManifest;
            let poison = ConfigurationPoison::from_reason(
                &reason,
                "the unique SchemaLineageManifest is missing",
            );
            (
                ConfigurationStatus::Poisoned(poison),
                Some(LineageRepairState::Missing {
                    configured_root: destination.root.clone(),
                    configured_path: destination.path.clone(),
                    destination: scanner
                        .inspect_destination(&destination.root, &destination.path)?,
                }),
                None,
            )
        }
        1 => {
            let manifest = index
                .lineage
                .values()
                .next()
                .expect("one lineage claimant has one manifest")
                .clone();
            (ConfigurationStatus::Ready, None, Some(manifest))
        }
        _ => {
            let claimants = index.lineage.keys().cloned().collect::<Vec<_>>();
            let reason = DscpV1::DuplicateLineageManifest {
                entries: claimants.clone(),
            };
            let poison = ConfigurationPoison::from_reason(
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
                repair = None;
            }
            ConfigurationStatus::Poisoned(selected)
        }
    };
    Ok((configuration, repair, manifest))
}

fn scan_key_matches(prefix: &ScanKey, key: &ScanKey) -> bool {
    prefix.0 == key.0
        && (prefix.1.is_empty()
            || prefix.1 == key.1
            || key
                .1
                .strip_prefix(&prefix.1)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

fn remove_set_value<K: Ord + Clone, V: Ord>(
    map: &mut BTreeMap<K, BTreeSet<V>>,
    key: &K,
    value: &V,
) {
    let empty = map.get_mut(key).is_some_and(|values| {
        values.remove(value);
        values.is_empty()
    });
    if empty {
        map.remove(key);
    }
}

fn remove_map_value<K: Ord + Clone, V: Ord, T>(
    map: &mut BTreeMap<K, BTreeMap<V, T>>,
    key: &K,
    value: &V,
) {
    let empty = map.get_mut(key).is_some_and(|values| {
        values.remove(value);
        values.is_empty()
    });
    if empty {
        map.remove(key);
    }
}

struct ScanCandidate {
    scan: ScanSnapshot,
    renames: Vec<LogicalRename>,
    version_poison: Option<VersionPoison>,
    bundle_poisons: BTreeMap<BundleUuid, ScopedBundlePoison>,
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
        authority: Option<&ProjectSchemaAuthority>,
    ) -> Result<Self, CoordinatorError> {
        let mut poisons = Vec::new();
        let mut bundle_poisons = BTreeMap::new();
        for bundle in scan.bundle_rows() {
            if let Err(error) = &bundle.parsed {
                if let Some(scoped) = scoped_bundle_poison(bundle, authority) {
                    bundle_poisons.insert(scoped.bundle, scoped);
                    continue;
                }
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
            .bundle_rows()
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
        for scoped in bundle_poisons.values() {
            bundles
                .entry(scoped.bundle)
                .or_default()
                .push(ReadableBundleSource {
                    root_name: scoped.root_name.clone(),
                    normalized_path: scoped.normalized_path.clone(),
                    file_hash: BundleFileHash(scoped.content_hash.0),
                });
            for entry in &scoped.entries {
                assets
                    .entry(entry.asset)
                    .or_default()
                    .push(AssetClaimant::Authored {
                        source: ReadableBundleSource {
                            root_name: scoped.root_name.clone(),
                            normalized_path: scoped.normalized_path.clone(),
                            file_hash: BundleFileHash(scoped.content_hash.0),
                        },
                        bundle: scoped.bundle,
                        local_id: entry.local_id.clone(),
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
            renames: Vec::new(),
            version_poison,
            bundle_poisons,
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
    for source in candidate.scan.bundle_rows() {
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
    for poison in candidate.bundle_poisons.values() {
        let source = ReadableBundleSource {
            root_name: poison.root_name.clone(),
            normalized_path: poison.normalized_path.clone(),
            file_hash: BundleFileHash(poison.content_hash.0),
        };
        for entry in &poison.entries {
            claims
                .entry(entry.asset)
                .or_default()
                .push(AssetClaimant::Authored {
                    source: source.clone(),
                    bundle: poison.bundle,
                    local_id: entry.local_id.clone(),
                });
            for (child, output) in projection.derived_outputs([(entry.asset, entry.type_uuid)]) {
                let claimant = AssetClaimant::Derived {
                    parent: entry.asset,
                    output_key: output.output_key.clone(),
                };
                claims.entry(child).or_default().push(claimant);
                outputs.insert(child, output);
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct BundleSummary {
    root_name: String,
    path: String,
    format_version: u32,
    content_hash: ContentHash,
    origin: Option<distill_store::bundles::DirectoryOrigin>,
}

struct RetiredWaitingProjection {
    error: StoredRetiredTypeReferenced,
    bundles: BTreeSet<BundleUuid>,
    paths: BTreeSet<String>,
    assets: BTreeSet<AssetUuid>,
}

fn retired_waiting_projection<'a>(
    sources: impl IntoIterator<Item = &'a ScannedBundle>,
    retired_types: &BTreeSet<TypeUuid>,
    manifest_hash: ContentHash,
    basis: SnapshotStamp,
) -> Result<Option<RetiredWaitingProjection>, StoreError> {
    if retired_types.is_empty() {
        return Ok(None);
    }
    let mut references = BTreeMap::<TypeUuid, BTreeSet<RetiredTypeReference>>::new();
    let mut bundles = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut assets = BTreeSet::new();
    for source in sources {
        let Ok(bundle) = &source.parsed else {
            continue;
        };
        let mut bundle_waits = false;
        for entry in bundle.assets.values() {
            if retired_types.contains(&entry.type_uuid) {
                references
                    .entry(entry.type_uuid)
                    .or_default()
                    .insert(RetiredTypeReference::Asset(entry.uuid));
                bundle_waits = true;
            }
            if entry.type_uuid == distill_core::bootstrap::MIGRATION_TYPE_UUID {
                let header =
                    crate::migration_control::decode_header(&entry.data).map_err(|error| {
                        StoreError::InvalidConfiguration {
                            error: format!(
                                "cannot classify retired Migration reference {}: {error}",
                                entry.uuid
                            ),
                        }
                    })?;
                if retired_types.contains(&header.target_type_uuid) {
                    let target = references.entry(header.target_type_uuid).or_default();
                    target.insert(RetiredTypeReference::MigrationEndpoint(entry.uuid));
                    bundle_waits = true;
                }
            }
        }
        if bundle_waits {
            bundles.insert(bundle.uuid);
            paths.insert(source.normalized_path.clone());
            assets.extend(bundle.assets.values().map(|entry| entry.uuid));
        }
    }
    let Some((type_uuid, references)) = references.into_iter().next() else {
        return Ok(None);
    };
    Ok(Some(RetiredWaitingProjection {
        error: StoredRetiredTypeReferenced {
            manifest_hash,
            basis,
            type_uuid,
            references: references.into_iter().collect(),
        },
        bundles,
        paths,
        assets,
    }))
}

impl BundleSummary {
    fn rpc_equivalent(&self, other: &Self) -> bool {
        self.path == other.path
            && self.format_version == other.format_version
            && self.content_hash == other.content_hash
    }
}

fn candidate_bundle_summaries(
    candidate: &ScanCandidate,
) -> Result<BTreeMap<BundleUuid, BundleSummary>, StoreError> {
    let mut summaries = BTreeMap::new();
    for source in candidate.scan.bundle_rows() {
        let Ok(bundle) = &source.parsed else {
            continue;
        };
        let origin = crate::importer::decoded_directory_origin(bundle).map_err(|error| {
            StoreError::InvalidConfiguration {
                error: format!(
                    "invalid import record in {}: {error:?}",
                    source.normalized_path
                ),
            }
        })?;
        summaries.insert(
            bundle.uuid,
            BundleSummary {
                root_name: source.root_name.clone(),
                path: source.normalized_path.clone(),
                format_version: bundle.format_version,
                content_hash: ContentHash(source.file_hash.0),
                origin,
            },
        );
    }
    for poison in candidate.bundle_poisons.values() {
        summaries.insert(
            poison.bundle,
            BundleSummary {
                root_name: poison.root_name.clone(),
                path: poison.normalized_path.clone(),
                format_version: poison.format_version,
                content_hash: poison.content_hash,
                origin: None,
            },
        );
    }
    Ok(summaries)
}

fn publish_scan(
    store: &Arc<Mutex<Store>>,
    base: InputVersion,
    mut candidate: ScanCandidate,
    advance_configuration: bool,
    pipeline: Option<&ConfigurationPipelinePublication>,
    projection: &PipelineProjection,
    tag_epoch: [u8; 32],
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
    let old_asset_bundles = store.all_asset_bundles()?;
    let old_paths = store.all_path_entries()?;
    let mut old_bundle_summaries = BTreeMap::new();
    for bundle in &old_bundles {
        let root_name =
            store
                .root_name(bundle.root)?
                .ok_or_else(|| StoreError::InvalidConfiguration {
                    error: format!("bundle {} has an unknown root id", bundle.bundle),
                })?;
        old_bundle_summaries.insert(
            bundle.bundle,
            BundleSummary {
                root_name,
                path: bundle.path.clone(),
                format_version: bundle.format_version,
                content_hash: bundle.content_hash,
                origin: bundle.origin.clone(),
            },
        );
    }
    let current_bundle_summaries = if candidate.version_poison.is_none() {
        candidate_bundle_summaries(&candidate)?
    } else {
        BTreeMap::new()
    };
    let changed_bundles = current_bundle_summaries
        .iter()
        .filter_map(|(bundle, current)| {
            (old_bundle_summaries.get(bundle) != Some(current)).then_some(*bundle)
        })
        .collect::<BTreeSet<_>>();
    let rpc_changed_bundles = current_bundle_summaries
        .iter()
        .filter_map(|(bundle, current)| {
            old_bundle_summaries
                .get(bundle)
                .is_none_or(|old| !current.rpc_equivalent(old))
                .then_some(*bundle)
        })
        .collect::<BTreeSet<_>>();
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

    let observation =
        InputVersion(
            base.0
                .checked_add(1)
                .ok_or_else(|| StoreError::InvalidConfiguration {
                    error: "input version exhausted".to_owned(),
                })?,
        );
    let manifest_hash = candidate
        .lineage_manifest
        .as_ref()
        .map(VerifiedSchemaLineageManifest::manifest_hash)
        .or(store
            .schema_manifest_basis()?
            .map(|basis| basis.manifest_hash));
    let waiting = match manifest_hash {
        Some(manifest_hash) => retired_waiting_projection(
            candidate.scan.bundle_rows(),
            &store.retired_type_uuids()?,
            manifest_hash,
            SnapshotStamp {
                instance: store.instance_id(),
                version: observation,
            },
        )?,
        None => None,
    };
    let waiting_bundles = waiting
        .as_ref()
        .map_or_else(BTreeSet::new, |waiting| waiting.bundles.clone());
    let publishable_changed_bundles = changed_bundles
        .difference(&waiting_bundles)
        .copied()
        .collect::<BTreeSet<_>>();
    let rpc_publishable_bundles = rpc_changed_bundles
        .difference(&waiting_bundles)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut commit = rpc_commit(
        &candidate,
        &old_asset_bundles,
        &old_paths,
        projection,
        derived_outputs.clone(),
        &rpc_publishable_bundles,
        &waiting_bundles,
    )?;
    if waiting.is_some() {
        commit.derived_outputs = None;
    }
    let mut next_pipeline = pipeline_diagnostic(store.pipeline_state()?);
    let mut healed_retired = false;
    store.input_transaction(|transaction| {
        let mut root_ids = BTreeMap::new();
        let mut scanned_keys = BTreeSet::new();
        let mut newest_mtime = 0;
        for file in candidate.scan.file_rows() {
            let root = *root_ids
                .entry(file.root_name.clone())
                .or_insert(transaction.intern_root(&file.root_name)?);
            let state = file_state(file);
            scanned_keys.insert((root, file.normalized_path.clone()));
            newest_mtime = newest_mtime.max(file.modified_nanos);
            let changed = old_files.iter().find_map(|(old_root, path, old)| {
                (*old_root == root && path == &file.normalized_path).then_some(old != &state)
            });
            if changed.unwrap_or(true) {
                transaction.upsert_file(root, &file.normalized_path, &state, observation)?;
                transaction.push_dirty(root, &file.normalized_path, true, observation)?;
            }
        }
        for (root, path, _) in &old_files {
            if !scanned_keys.contains(&(*root, path.clone())) {
                transaction.remove_file(*root, path)?;
                transaction.push_dirty(*root, path, false, observation)?;
            }
        }
        transaction.set_clean_watermark(newest_mtime)?;
        transaction.set_version_poisons(candidate.version_poison.clone())?;
        if waiting.is_none() {
            transaction.clear_derived_outputs()?;
        }
        if candidate.version_poison.is_none() && waiting.is_none() {
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
                next_pipeline = transaction
                    .pipeline_acceptance_requirement(epoch)?
                    .map_or(PipelineDiagnostic::Ready, |required| {
                        PipelineDiagnostic::SchemaAcceptanceRequired(required)
                    });
                if transaction.publish_pipeline_epoch(epoch)? {
                    transaction.publish_tool_epoch(tools)?;
                }
            }
            Some(ConfigurationPipelinePublication::Poison(poison)) => {
                next_pipeline = PipelineDiagnostic::Poisoned(poison.clone());
                transaction.publish_pipeline_poison(poison)?;
            }
            None => {}
        }
        if let Some(waiting) = &waiting {
            transaction.publish_retired_type_referenced(&waiting.error)?;
            next_pipeline =
                PipelineDiagnostic::RetiredTypeReferenced(distill_rpc::RetiredTypeReferenced {
                    manifest_hash: BundleFileHash(waiting.error.manifest_hash.0),
                    basis: waiting.error.basis,
                    type_uuid: waiting.error.type_uuid,
                    references: waiting.error.references.clone(),
                });
            commit.pipeline_epoch_changed = true;
        } else if pipeline.is_none() {
            healed_retired = transaction.clear_retired_type_referenced()?;
        }

        if candidate.version_poison.is_none() {
            for bundle in old_bundle_summaries.keys() {
                if !current_bundle_summaries.contains_key(bundle)
                    || publishable_changed_bundles.contains(bundle)
                {
                    transaction.remove_bundle(*bundle)?;
                }
            }
            for source in candidate.scan.bundle_rows() {
                let Ok(bundle) = &source.parsed else {
                    continue;
                };
                if !publishable_changed_bundles.contains(&bundle.uuid) {
                    continue;
                }
                let root = *root_ids
                    .entry(source.root_name.clone())
                    .or_insert(transaction.intern_root(&source.root_name)?);
                let summary = &current_bundle_summaries[&bundle.uuid];
                transaction.upsert_bundle(&BundleMeta {
                    bundle: bundle.uuid,
                    root,
                    path: source.normalized_path.clone(),
                    format_version: bundle.format_version,
                    content_hash: ContentHash(source.file_hash.0),
                    origin: summary.origin.clone(),
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
                        tags: BTreeMap::new(),
                    })?;
                    transaction.set_tag_index_pending(entry.uuid, tag_epoch)?;
                }
                if let Some(primary) = &bundle.primary {
                    transaction.set_path_entry(
                        &source.normalized_path,
                        root,
                        bundle.assets[primary].uuid,
                    )?;
                }
            }
            for poison in candidate.bundle_poisons.values() {
                if !publishable_changed_bundles.contains(&poison.bundle) {
                    continue;
                }
                let root = *root_ids
                    .entry(poison.root_name.clone())
                    .or_insert(transaction.intern_root(&poison.root_name)?);
                transaction.poison_bundle(
                    &StoreNamespaceSkeleton {
                        bundle: poison.bundle,
                        root,
                        path: poison.normalized_path.clone(),
                        format_version: poison.format_version,
                        content_hash: poison.content_hash,
                        entries: poison.entries.clone(),
                    },
                    &poison.message,
                )?;
            }
        }
        for rename in &candidate.renames {
            let root = *root_ids
                .entry(rename.root_name.clone())
                .or_insert(transaction.intern_root(&rename.root_name)?);
            transaction.push_rename(root, &rename.from_path, &rename.to_path)?;
        }
        Ok(())
    })?;
    if healed_retired {
        next_pipeline = pipeline_diagnostic(store.pipeline_state()?);
        commit.pipeline_epoch_changed = true;
    }
    commit.pipeline = Some(next_pipeline);
    Ok(commit)
}

#[derive(Debug)]
struct IncrementalFileMutation {
    root_name: String,
    path: String,
    state: Option<FileState>,
}

#[derive(Debug)]
struct DurableBundleBasis {
    summary: Option<BundleSummary>,
    assets: BTreeSet<AssetUuid>,
}

fn preserve_waiting_bundle_paths(
    waiting: &mut RetiredWaitingProjection,
    durable_bundles: &BTreeMap<BundleUuid, DurableBundleBasis>,
) {
    for bundle in &waiting.bundles {
        if let Some(path) = durable_bundles
            .get(bundle)
            .and_then(|basis| basis.summary.as_ref())
            .map(|summary| &summary.path)
        {
            waiting.paths.insert(path.clone());
        }
    }
}

fn include_reactivation_durable_paths(
    paths: &mut BTreeMap<String, BTreeSet<AssetUuid>>,
    scan: &ScanSnapshot,
    durable_bundles: &BTreeMap<BundleUuid, DurableBundleBasis>,
) {
    for path in durable_bundles
        .values()
        .filter_map(|basis| basis.summary.as_ref().map(|summary| summary.path.clone()))
    {
        paths.entry(path.clone()).or_insert_with(|| {
            scan.bundle_rows()
                .filter(|source| source.normalized_path == path)
                .filter_map(|source| source.parsed.as_ref().ok())
                .filter_map(|bundle| {
                    bundle
                        .primary
                        .as_ref()
                        .map(|primary| bundle.assets[primary].uuid)
                })
                .collect()
        });
    }
}

fn append_path_mutations(
    commit: &mut Commit,
    current_paths: &BTreeMap<String, BTreeSet<AssetUuid>>,
    old_paths: &BTreeMap<String, BTreeSet<AssetUuid>>,
    waiting: Option<&RetiredWaitingProjection>,
) {
    for (path, current) in current_paths {
        if waiting.is_some_and(|waiting| waiting.paths.contains(path)) {
            continue;
        }
        let old = &old_paths[path];
        if old == current {
            continue;
        }
        if current.is_empty() {
            commit
                .paths
                .push(PathMutation::Remove { path: path.clone() });
        } else {
            commit.paths.push(PathMutation::Set {
                path: path.clone(),
                candidates: current.clone(),
            });
        }
    }
}

struct IncrementalSchemaTransition<'a> {
    planned: &'a PlannedSchemaTransition,
    candidate: &'a ValidatedPipelineEpoch,
    tools: &'a BTreeMap<String, distill_store::pipeline::ToolRegistrationV2>,
}

fn apply_schema_transition(
    transaction: &mut distill_store::db::InputTxn<'_>,
    transition: &IncrementalSchemaTransition<'_>,
) -> Result<PipelineDiagnostic, StoreError> {
    let request = &transition.planned.request;
    match request.action {
        SchemaTransitionAction::Accept { requested } => {
            transaction.accept_schema_candidate(
                transition.candidate,
                &request.manifest,
                &transition.planned.proposed,
                request.type_uuid,
                requested,
            )?;
        }
        SchemaTransitionAction::Rollback { target } => {
            transaction.rollback_schema_candidate(
                transition.candidate,
                &request.manifest,
                &transition.planned.proposed,
                SchemaRollbackRequest {
                    type_uuid: request.type_uuid,
                    target,
                    live_schema_hashes: &transition.planned.live_schema_hashes,
                    reverse_edges: &transition.planned.reverse_edges,
                },
            )?;
        }
        SchemaTransitionAction::Retire { control_basis } => {
            transaction.retire_schema_candidate(
                transition.candidate,
                &request.manifest,
                control_basis,
                &transition.planned.proposed,
                request.type_uuid,
                &transition.planned.live_schema_hashes,
            )?;
        }
        SchemaTransitionAction::Reactivate => {
            transaction.reactivate_schema_candidate(
                transition.candidate,
                &request.manifest,
                &transition.planned.proposed,
                SchemaReactivationRequest {
                    type_uuid: request.type_uuid,
                    live_schema_hashes: &transition.planned.live_schema_hashes,
                    reverse_edges: &transition.planned.reverse_edges,
                },
            )?;
        }
    }
    transaction
        .pipeline_acceptance_requirement(transition.candidate)
        .map(|required| {
            required.map_or(PipelineDiagnostic::Ready, |required| {
                PipelineDiagnostic::SchemaAcceptanceRequired(required)
            })
        })
}

#[allow(clippy::too_many_arguments)] // The transaction receives each independently pinned publication authority.
fn publish_incremental_scan(
    store: &Arc<Mutex<Store>>,
    base: InputVersion,
    baseline: &ScanSnapshot,
    delta: &ScanDelta,
    plan: &IncrementalScanPlan,
    renames: &[LogicalRename],
    projection: &PipelineProjection,
    tag_epoch: [u8; 32],
    schema_transition: Option<&IncrementalSchemaTransition<'_>>,
) -> Result<Commit, StoreError> {
    let file_mutations = incremental_file_mutations(baseline, delta);
    let mut store = lock_store(store);
    if store.input_version() != base {
        return Err(StoreError::InvalidConfiguration {
            error: format!(
                "durable incremental-scan basis is {:?}, expected {base:?}",
                store.input_version()
            ),
        });
    }
    let watermark = store.clean_watermark()?.unwrap_or(0);
    let configuration_generation = match store.configuration_state()? {
        ConfigurationState::Ready(epoch) => epoch.generation,
        ConfigurationState::Poisoned { last_good, .. } => {
            last_good.map_or(0, |epoch| epoch.generation)
        }
    };
    let mut durable_bundles = BTreeMap::new();
    for bundle in plan.bundles.keys() {
        let meta = store.bundle(*bundle)?;
        let summary = match &meta {
            Some(meta) => {
                let root_name = store.root_name(meta.root)?.ok_or_else(|| {
                    StoreError::InvalidConfiguration {
                        error: format!("bundle {bundle} has an unknown root id"),
                    }
                })?;
                Some(BundleSummary {
                    root_name,
                    path: meta.path.clone(),
                    format_version: meta.format_version,
                    content_hash: meta.content_hash,
                    origin: meta.origin.clone(),
                })
            }
            None => None,
        };
        let assets = store.asset_ids_in_bundle(*bundle)?;
        durable_bundles.insert(*bundle, DurableBundleBasis { summary, assets });
    }
    let old_derived = plan
        .derived_outputs
        .keys()
        .map(|child| Ok((*child, store.derived_output_row(*child)?)))
        .collect::<Result<BTreeMap<_, _>, StoreError>>()?;
    let stored_pipeline = store.pipeline_state()?;
    let was_retired = matches!(
        stored_pipeline,
        Some(StoredPipelineState::RetiredTypeReferenced { .. })
    );
    let mut commit = Commit {
        configuration: Some(plan.configuration.clone()),
        pipeline: Some(pipeline_diagnostic(stored_pipeline)),
        version_poison: Some(plan.version_poison.clone()),
        lineage_repair: Some(plan.lineage_repair.clone()),
        tag_poisons: Some(BTreeMap::new()),
        ..Commit::default()
    };
    let observation =
        InputVersion(
            base.0
                .checked_add(1)
                .ok_or_else(|| StoreError::InvalidConfiguration {
                    error: "input version exhausted".to_owned(),
                })?,
        );
    let mut retired_types = store.retired_type_uuids()?;
    if schema_transition.is_some_and(|transition| {
        matches!(
            transition.planned.request.action,
            SchemaTransitionAction::Reactivate
        )
    }) {
        retired_types.remove(
            &schema_transition
                .expect("reactivation transition is present")
                .planned
                .request
                .type_uuid,
        );
    }
    let manifest_hash = plan
        .lineage_manifest
        .as_ref()
        .map(VerifiedSchemaLineageManifest::manifest_hash)
        .or(store
            .schema_manifest_basis()?
            .map(|basis| basis.manifest_hash));
    let mut complete_waiting_scan = None;
    if was_retired || schema_transition.is_some() {
        let mut next = baseline.clone();
        next.apply_delta(delta.clone());
        complete_waiting_scan = Some(next);
    }
    let mut waiting = match manifest_hash {
        Some(manifest_hash) => retired_waiting_projection(
            complete_waiting_scan.as_ref().map_or_else(
                || {
                    Box::new(plan.bundles.values().filter_map(Option::as_deref))
                        as Box<dyn Iterator<Item = &ScannedBundle>>
                },
                |scan| Box::new(scan.bundle_rows()) as Box<dyn Iterator<Item = &ScannedBundle>>,
            ),
            &retired_types,
            manifest_hash,
            SnapshotStamp {
                instance: store.instance_id(),
                version: observation,
            },
        )?,
        None => None,
    };
    if let Some(waiting) = &mut waiting {
        // A waiting bundle may have moved. Its new path is in the observed
        // projection, while the old path is part of the last published bundle
        // projection. Hold both so RPC and SQLite preserve the same complete
        // last-good view until reactivation admits the move atomically.
        preserve_waiting_bundle_paths(waiting, &durable_bundles);
    }
    let mut path_projections = plan.paths.clone();
    if schema_transition.is_some_and(|transition| {
        matches!(
            transition.planned.request.action,
            SchemaTransitionAction::Reactivate
        )
    }) {
        include_reactivation_durable_paths(
            &mut path_projections,
            complete_waiting_scan
                .as_ref()
                .expect("schema transition constructed a complete scan"),
            &durable_bundles,
        );
    }
    let old_paths = path_projections
        .keys()
        .map(|path| Ok((path.clone(), store.path_assets(path)?)))
        .collect::<Result<BTreeMap<_, _>, StoreError>>()?;
    let mut changed_bundles = BTreeSet::new();
    if plan.version_poison.is_none() {
        for (bundle_uuid, source) in &plan.bundles {
            if waiting
                .as_ref()
                .is_some_and(|waiting| waiting.bundles.contains(bundle_uuid))
            {
                continue;
            }
            let old = &durable_bundles[bundle_uuid];
            let current_summary = if let Some(poison) = plan.bundle_poisons.get(bundle_uuid) {
                Some(BundleSummary {
                    root_name: poison.root_name.clone(),
                    path: poison.normalized_path.clone(),
                    format_version: poison.format_version,
                    content_hash: poison.content_hash,
                    origin: None,
                })
            } else {
                source
                    .as_ref()
                    .map(|source| bundle_summary(source))
                    .transpose()?
            };
            if old.summary == current_summary {
                continue;
            }
            changed_bundles.insert(*bundle_uuid);
            let mut current_assets = BTreeSet::new();
            if let Some(poison) = plan.bundle_poisons.get(bundle_uuid) {
                for entry in &poison.entries {
                    current_assets.insert(entry.asset);
                    commit.assets.push(AssetMutation::Set {
                        uuid: entry.asset,
                        resolution: StoredResolve::Failed {
                            error: poison.message.clone(),
                        },
                        delta: AssetDeltaState::Changed,
                    });
                    commit
                        .tag_poisons
                        .as_mut()
                        .expect("incremental scan initializes tag poisons")
                        .insert(entry.asset, poison.bundle);
                    if old.assets.contains(&entry.asset) {
                        commit
                            .authoring
                            .push(AuthoringMutation::Remove { uuid: entry.asset });
                    }
                }
            } else if let Some(source) = source {
                let bundle = source
                    .parsed
                    .as_ref()
                    .expect("indexed current bundle parsed successfully");
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
            }
            for asset in old.assets.difference(&current_assets) {
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
        append_path_mutations(&mut commit, &path_projections, &old_paths, waiting.as_ref());
        for (child, current) in &plan.derived_outputs {
            let old = old_derived[child].as_ref();
            if waiting.as_ref().is_some_and(|waiting| {
                current
                    .as_ref()
                    .is_some_and(|entry| waiting.assets.contains(&entry.parent))
                    || old.is_some_and(|(parent, _)| waiting.assets.contains(parent))
            }) {
                continue;
            }
            let unchanged = match (old, current) {
                (None, None) => true,
                (Some((parent, key)), Some(entry)) => {
                    parent == &entry.parent && key == &entry.output_key
                }
                _ => false,
            };
            if unchanged {
                continue;
            }
            match current {
                Some(entry) => commit
                    .derived_output_mutations
                    .push(DerivedOutputMutation::Set {
                        child: *child,
                        entry: entry.clone(),
                    }),
                None => commit
                    .derived_output_mutations
                    .push(DerivedOutputMutation::Remove { child: *child }),
            }
        }
    }

    let mut healed_retired = false;
    store.input_transaction(|transaction| {
        let mut root_ids = BTreeMap::new();
        let mut newest_mtime = watermark;
        for mutation in &file_mutations {
            let root = *root_ids
                .entry(mutation.root_name.clone())
                .or_insert(transaction.intern_root(&mutation.root_name)?);
            match &mutation.state {
                Some(state) => {
                    transaction.upsert_file(root, &mutation.path, state, observation)?;
                    transaction.push_dirty(root, &mutation.path, true, observation)?;
                    newest_mtime = newest_mtime.max(state.mtime);
                }
                None => {
                    transaction.remove_file(root, &mutation.path)?;
                    transaction.push_dirty(root, &mutation.path, false, observation)?;
                }
            }
        }
        transaction.set_clean_watermark(newest_mtime)?;
        transaction.set_version_poisons(plan.version_poison.clone())?;
        match &plan.configuration {
            ConfigurationStatus::Ready => {
                transaction.publish_configuration_ready(configuration_generation)?;
            }
            ConfigurationStatus::Poisoned(poison) => {
                transaction.publish_configuration_poison(&poison.detail, &poison.message)?;
            }
        }
        if let Some(transition) = schema_transition {
            if plan.lineage_manifest.as_ref() != Some(&transition.planned.proposed) {
                return Err(StoreError::InvalidConfiguration {
                    error: "incremental scan did not reproduce the planned schema manifest"
                        .to_owned(),
                });
            }
            commit.pipeline = Some(apply_schema_transition(transaction, transition)?);
            if matches!(commit.pipeline, Some(PipelineDiagnostic::Ready)) {
                transaction.publish_tool_epoch(transition.tools)?;
            }
            commit.pipeline_epoch_changed = true;
        } else if let Some(manifest) = &plan.lineage_manifest {
            transaction.project_verified_lineage_manifest(manifest)?;
        }
        if let Some(waiting) = &waiting {
            transaction.publish_retired_type_referenced(&waiting.error)?;
            commit.pipeline = Some(PipelineDiagnostic::RetiredTypeReferenced(
                distill_rpc::RetiredTypeReferenced {
                    manifest_hash: BundleFileHash(waiting.error.manifest_hash.0),
                    basis: waiting.error.basis,
                    type_uuid: waiting.error.type_uuid,
                    references: waiting.error.references.clone(),
                },
            ));
            commit.pipeline_epoch_changed = true;
        } else if schema_transition.is_none() {
            healed_retired = transaction.clear_retired_type_referenced()?;
        }
        if plan.version_poison.is_none() {
            for bundle_uuid in &changed_bundles {
                transaction.remove_bundle(*bundle_uuid)?;
                let Some(source) = &plan.bundles[bundle_uuid] else {
                    continue;
                };
                if let Some(poison) = plan.bundle_poisons.get(bundle_uuid) {
                    let root = *root_ids
                        .entry(poison.root_name.clone())
                        .or_insert(transaction.intern_root(&poison.root_name)?);
                    transaction.poison_bundle(
                        &StoreNamespaceSkeleton {
                            bundle: poison.bundle,
                            root,
                            path: poison.normalized_path.clone(),
                            format_version: poison.format_version,
                            content_hash: poison.content_hash,
                            entries: poison.entries.clone(),
                        },
                        &poison.message,
                    )?;
                    continue;
                }
                let bundle = source
                    .parsed
                    .as_ref()
                    .expect("indexed current bundle parsed successfully");
                let summary = bundle_summary(source)?;
                let root = *root_ids
                    .entry(source.root_name.clone())
                    .or_insert(transaction.intern_root(&source.root_name)?);
                transaction.upsert_bundle(&BundleMeta {
                    bundle: bundle.uuid,
                    root,
                    path: source.normalized_path.clone(),
                    format_version: bundle.format_version,
                    content_hash: ContentHash(source.file_hash.0),
                    origin: summary.origin,
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
                        tags: BTreeMap::new(),
                    })?;
                    transaction.set_tag_index_pending(entry.uuid, tag_epoch)?;
                }
                if let Some(primary) = &bundle.primary {
                    transaction.set_path_entry(
                        &source.normalized_path,
                        root,
                        bundle.assets[primary].uuid,
                    )?;
                }
            }
            for (child, current) in &plan.derived_outputs {
                match current {
                    Some(entry) => {
                        transaction.set_derived_output(*child, entry.parent, &entry.output_key)?
                    }
                    None => {
                        transaction.remove_derived_output(*child)?;
                    }
                }
            }
        }
        for rename in renames {
            let root = *root_ids
                .entry(rename.root_name.clone())
                .or_insert(transaction.intern_root(&rename.root_name)?);
            transaction.push_rename(root, &rename.from_path, &rename.to_path)?;
        }
        Ok(())
    })?;
    if healed_retired {
        commit.pipeline = Some(pipeline_diagnostic(store.pipeline_state()?));
        commit.pipeline_epoch_changed = true;
    }
    Ok(commit)
}

fn incremental_file_mutations(
    baseline: &ScanSnapshot,
    delta: &ScanDelta,
) -> Vec<IncrementalFileMutation> {
    let mut old = BTreeMap::<ScanKey, FileState>::new();
    for prefix in delta.affected_prefixes() {
        for (key, file) in baseline.files.range(prefix.clone()..) {
            if !scan_key_matches(prefix, key) {
                break;
            }
            old.insert(key.clone(), file_state(file));
        }
    }
    let current = delta
        .observed_file_entries()
        .map(|(key, file)| (key.clone(), file_state(file)))
        .collect::<BTreeMap<_, _>>();
    old.keys()
        .chain(current.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|key| {
            let before = old.get(&key);
            let after = current.get(&key);
            (before != after).then(|| IncrementalFileMutation {
                root_name: key.0,
                path: key.1,
                state: after.cloned(),
            })
        })
        .collect()
}

fn bundle_summary(source: &ScannedBundle) -> Result<BundleSummary, StoreError> {
    let bundle = source
        .parsed
        .as_ref()
        .expect("indexed current bundle parsed successfully");
    let origin = crate::importer::decoded_directory_origin(bundle).map_err(|error| {
        StoreError::InvalidConfiguration {
            error: format!(
                "invalid import record in {}: {error:?}",
                source.normalized_path
            ),
        }
    })?;
    Ok(BundleSummary {
        root_name: source.root_name.clone(),
        path: source.normalized_path.clone(),
        format_version: bundle.format_version,
        content_hash: ContentHash(source.file_hash.0),
        origin,
    })
}

fn pipeline_diagnostic(state: Option<StoredPipelineState>) -> PipelineDiagnostic {
    match state {
        Some(StoredPipelineState::Ready(_)) | None => PipelineDiagnostic::Ready,
        Some(StoredPipelineState::SchemaAcceptanceRequired { required, .. }) => {
            PipelineDiagnostic::SchemaAcceptanceRequired(required)
        }
        Some(StoredPipelineState::RetiredTypeReferenced { error, .. }) => {
            PipelineDiagnostic::RetiredTypeReferenced(distill_rpc::RetiredTypeReferenced {
                manifest_hash: BundleFileHash(error.manifest_hash.0),
                basis: error.basis,
                type_uuid: error.type_uuid,
                references: error.references,
            })
        }
        Some(StoredPipelineState::Poisoned { error, .. }) => PipelineDiagnostic::Poisoned(error),
    }
}

/// Reobserve authored paths and advance the durable projection while the RPC
/// server holds its publication lock.
#[allow(clippy::too_many_arguments)] // The authoring boundary passes each publication authority explicitly.
pub(crate) fn publish_incremental_paths(
    scanner: &RootedScanner,
    scan_snapshot: &Mutex<ScanSnapshot>,
    paths: &[PathBuf],
    lineage_destination: &LineageDestination,
    store: &Arc<Mutex<Store>>,
    base: InputVersion,
    projection: &PipelineProjection,
    coordinator: Option<&DaemonCoordinator>,
) -> Result<Commit, String> {
    publish_incremental_paths_with_schema_transition(
        scanner,
        scan_snapshot,
        paths,
        lineage_destination,
        store,
        base,
        projection,
        coordinator,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn publish_incremental_paths_with_schema_transition(
    scanner: &RootedScanner,
    scan_snapshot: &Mutex<ScanSnapshot>,
    paths: &[PathBuf],
    lineage_destination: &LineageDestination,
    store: &Arc<Mutex<Store>>,
    base: InputVersion,
    projection: &PipelineProjection,
    coordinator: Option<&DaemonCoordinator>,
    schema_transition: Option<&IncrementalSchemaTransition<'_>>,
) -> Result<Commit, String> {
    let mut baseline = scan_snapshot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let delta = scanner
        .scan_incremental_delta(&baseline, paths)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "authored path is outside every configured root".to_owned())?;
    let authority = coordinator.and_then(DaemonCoordinator::schema_authority);
    let tag_epoch = authority
        .as_ref()
        .map_or([0; 32], |authority| authority.source_hash());

    if let Some(coordinator) = coordinator {
        let mut index = lock_scan_projection(&coordinator.scan_projection);
        let checkpoint = index.checkpoint(delta.affected_prefixes());
        let plan = match index
            .replace_delta(&delta, projection, authority.as_deref())
            .and_then(|_| {
                index.incremental_plan(
                    scanner,
                    lineage_destination,
                    coordinator.configuration_poison(),
                )
            }) {
            Ok(plan) => plan,
            Err(error) => {
                index
                    .restore(checkpoint)
                    .map_err(|error| error.to_string())?;
                return Err(error.to_string());
            }
        };
        let mut commit = match publish_incremental_scan(
            store,
            base,
            &baseline,
            &delta,
            &plan,
            &[],
            projection,
            tag_epoch,
            schema_transition,
        ) {
            Ok(commit) => commit,
            Err(error) => {
                index
                    .restore(checkpoint)
                    .map_err(|error| error.to_string())?;
                return Err(error.to_string());
            }
        };
        if schema_transition.is_none() {
            if let Some(authority) = authority {
                let affected = commit_affected_asset_bundles(&commit);
                if !affected.is_empty() {
                    let targets = coordinator
                        .build_targets
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    crate::build::refine_published_tag_index_incremental(
                        Arc::clone(store),
                        scanner.clone(),
                        authority,
                        coordinator.pipeline_snapshot(),
                        &targets,
                        coordinator.operational_configuration().max_dependency_depth,
                        &affected,
                    )
                    .apply_incremental(&mut commit);
                }
            }
        }
        baseline.apply_delta(delta);
        if plan.version_poison.is_none() {
            index.clear_published_pending();
        }
        return Ok(commit);
    }

    if schema_transition.is_some() {
        return Err("schema transition requires the live daemon coordinator".to_owned());
    }

    let mut scan = baseline.clone();
    scan.apply_delta(delta);
    let candidate = ScanCandidate::build(
        scanner,
        lineage_destination,
        scan.clone(),
        None,
        authority.as_deref(),
    )
    .map_err(|error| error.to_string())?;
    let commit = publish_scan(store, base, candidate, false, None, projection, tag_epoch)
        .map_err(|error| error.to_string())?;
    *baseline = scan;
    Ok(commit)
}

fn commit_asset_bundles(
    commit: &Commit,
    previous: &BTreeMap<AssetUuid, BundleUuid>,
) -> BTreeMap<AssetUuid, BundleUuid> {
    let mut bundles = previous.clone();
    for mutation in &commit.authoring {
        match mutation {
            AuthoringMutation::Set(entry) => {
                bundles.insert(entry.uuid, entry.bundle);
            }
            AuthoringMutation::Remove { uuid } => {
                bundles.remove(uuid);
            }
        }
    }
    bundles
}

fn commit_affected_asset_bundles(commit: &Commit) -> BTreeMap<AssetUuid, Option<BundleUuid>> {
    commit
        .authoring
        .iter()
        .map(|mutation| match mutation {
            AuthoringMutation::Set(entry) => (entry.uuid, Some(entry.bundle)),
            AuthoringMutation::Remove { uuid } => (*uuid, None),
        })
        .collect()
}

fn rpc_commit(
    candidate: &ScanCandidate,
    old_asset_bundles: &BTreeMap<AssetUuid, BundleUuid>,
    old_paths: &[(String, distill_store::files::RootId, AssetUuid)],
    projection: &PipelineProjection,
    derived_outputs: BTreeMap<AssetUuid, DerivedOutputEntry>,
    changed_bundles: &BTreeSet<BundleUuid>,
    waiting_bundles: &BTreeSet<BundleUuid>,
) -> Result<Commit, StoreError> {
    let mut commit = Commit {
        configuration: Some(candidate.configuration.clone()),
        pipeline: Some(PipelineDiagnostic::Ready),
        version_poison: Some(candidate.version_poison.clone()),
        lineage_repair: Some(candidate.lineage_repair.clone()),
        derived_outputs: Some(derived_outputs),
        tag_poisons: Some(BTreeMap::new()),
        ..Commit::default()
    };
    if candidate.version_poison.is_some() {
        return Ok(commit);
    }

    let mut current_assets = old_asset_bundles
        .iter()
        .filter_map(|(asset, bundle)| waiting_bundles.contains(bundle).then_some(*asset))
        .collect::<BTreeSet<_>>();
    let mut paths = BTreeMap::<String, BTreeSet<AssetUuid>>::new();
    for (path, _, asset) in old_paths {
        if old_asset_bundles
            .get(asset)
            .is_some_and(|bundle| waiting_bundles.contains(bundle))
        {
            paths.entry(path.clone()).or_default().insert(*asset);
        }
    }
    for source in candidate.scan.bundle_rows() {
        let Ok(bundle) = &source.parsed else {
            continue;
        };
        if waiting_bundles.contains(&bundle.uuid) {
            continue;
        }
        for (local_id, entry) in &bundle.assets {
            current_assets.insert(entry.uuid);
            if !changed_bundles.contains(&bundle.uuid) {
                continue;
            }
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
            commit
                .tag_poisons
                .as_mut()
                .expect("scan commit initializes tag poisons")
                .insert(entry.uuid, bundle.uuid);
        }
        if let Some(primary) = &bundle.primary {
            paths
                .entry(source.normalized_path.clone())
                .or_default()
                .insert(bundle.assets[primary].uuid);
        }
    }
    for poison in candidate.bundle_poisons.values() {
        for entry in &poison.entries {
            current_assets.insert(entry.asset);
            if !changed_bundles.contains(&poison.bundle) {
                continue;
            }
            commit.assets.push(AssetMutation::Set {
                uuid: entry.asset,
                resolution: StoredResolve::Failed {
                    error: poison.message.clone(),
                },
                delta: AssetDeltaState::Changed,
            });
            commit
                .tag_poisons
                .as_mut()
                .expect("scan commit initializes tag poisons")
                .insert(entry.asset, poison.bundle);
            if old_asset_bundles.contains_key(&entry.asset) {
                commit
                    .authoring
                    .push(AuthoringMutation::Remove { uuid: entry.asset });
            }
        }
    }
    for asset in old_asset_bundles.keys() {
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
    let mut old_path_candidates = BTreeMap::<String, BTreeSet<AssetUuid>>::new();
    for (path, _, asset) in old_paths {
        old_path_candidates
            .entry(path.clone())
            .or_default()
            .insert(*asset);
    }
    let path_names = old_path_candidates
        .keys()
        .chain(paths.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for path in path_names {
        match (old_path_candidates.get(&path), paths.get(&path)) {
            (old, current) if old == current => {}
            (_, Some(candidates)) => commit.paths.push(PathMutation::Set {
                path,
                candidates: candidates.clone(),
            }),
            (Some(_), None) => commit.paths.push(PathMutation::Remove { path }),
            (None, None) => unreachable!("path name came from one projection"),
        }
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

pub(crate) fn decode_lineage_manifest(
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

fn lock_scan_snapshot(snapshot: &Mutex<ScanSnapshot>) -> MutexGuard<'_, ScanSnapshot> {
    snapshot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_scan_projection(
    projection: &Mutex<ScanProjectionIndex>,
) -> MutexGuard<'_, ScanProjectionIndex> {
    projection
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

fn build_worker_pool(parallelism: usize) -> Result<Arc<ThreadPool>, String> {
    const BUILD_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;

    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism)
        .stack_size(BUILD_WORKER_STACK_SIZE)
        .thread_name(|index| format!("distill-build-{index}"))
        .build()
        .map(Arc::new)
        .map_err(|error| format!("create {parallelism}-thread build pool: {error}"))
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn waiting_bundle_move_holds_both_old_and_new_paths() {
        let bundle = BundleUuid([41; 16]);
        let mut waiting = RetiredWaitingProjection {
            error: StoredRetiredTypeReferenced {
                manifest_hash: ContentHash([42; 32]),
                basis: SnapshotStamp {
                    instance: distill_store::state::StoreInstanceId([43; 16]),
                    version: InputVersion(7),
                },
                type_uuid: TypeUuid([44; 16]),
                references: vec![RetiredTypeReference::Asset(AssetUuid([45; 16]))],
            },
            bundles: BTreeSet::from([bundle]),
            paths: BTreeSet::from(["new/location.bundle".to_owned()]),
            assets: BTreeSet::new(),
        };
        let durable = BTreeMap::from([(
            bundle,
            DurableBundleBasis {
                summary: Some(BundleSummary {
                    root_name: "main".to_owned(),
                    path: "old/location.bundle".to_owned(),
                    format_version: 1,
                    content_hash: ContentHash([46; 32]),
                    origin: None,
                }),
                assets: BTreeSet::new(),
            },
        )]);

        preserve_waiting_bundle_paths(&mut waiting, &durable);

        assert_eq!(
            waiting.paths,
            BTreeSet::from([
                "new/location.bundle".to_owned(),
                "old/location.bundle".to_owned(),
            ])
        );
    }

    #[test]
    fn reactivation_removes_durable_old_path_and_sets_observed_new_path() {
        let bundle_uuid = BundleUuid([61; 16]);
        let asset = AssetUuid([62; 16]);
        let old_path = "old/location.bundle".to_owned();
        let new_path = "new/location.bundle".to_owned();
        let bundle = Bundle {
            format_version: 1,
            uuid: bundle_uuid,
            primary: Some("primary".to_owned()),
            schemas: BTreeMap::new(),
            assets: BTreeMap::from([(
                "primary".to_owned(),
                AssetEntry {
                    uuid: asset,
                    type_uuid: TypeUuid([63; 16]),
                    schema_hash: LogicalHash([64; 32]),
                    lineage: distill_bundle::EntryLineageV1::Bootstrap {
                        bundle_format_version: 1,
                    },
                    authoring_only: false,
                    data: AuthoredValue::Null,
                },
            )]),
        };
        let mut scan = ScanSnapshot::default();
        scan.bundles.insert(
            ("main".to_owned(), new_path.clone()),
            Arc::new(ScannedBundle {
                root_name: "main".to_owned(),
                normalized_path: new_path.clone(),
                file_hash: BundleFileHash([65; 32]),
                bytes: Vec::new(),
                parsed: Ok(bundle),
                namespace_skeleton: None,
            }),
        );
        let durable = BTreeMap::from([(
            bundle_uuid,
            DurableBundleBasis {
                summary: Some(BundleSummary {
                    root_name: "main".to_owned(),
                    path: old_path.clone(),
                    format_version: 1,
                    content_hash: ContentHash([66; 32]),
                    origin: None,
                }),
                assets: BTreeSet::from([asset]),
            },
        )]);
        let mut current_paths = BTreeMap::from([(new_path.clone(), BTreeSet::from([asset]))]);

        include_reactivation_durable_paths(&mut current_paths, &scan, &durable);
        let old_paths = BTreeMap::from([
            (old_path.clone(), BTreeSet::from([asset])),
            (new_path.clone(), BTreeSet::new()),
        ]);
        let mut commit = Commit::default();
        append_path_mutations(&mut commit, &current_paths, &old_paths, None);

        assert!(commit.paths.contains(&PathMutation::Remove {
            path: old_path.clone(),
        }));
        assert!(commit.paths.contains(&PathMutation::Set {
            path: new_path,
            candidates: BTreeSet::from([asset]),
        }));
    }

    #[test]
    fn retired_migration_diagnostics_preserve_control_asset_multiplicity() {
        fn bytes<const N: usize>(value: [u8; N]) -> AuthoredValue {
            AuthoredValue::Array(
                value
                    .into_iter()
                    .map(|byte| AuthoredValue::UInt(u128::from(byte)))
                    .collect(),
            )
        }

        let retired_type = TypeUuid([51; 16]);
        let migration_a = AssetUuid([52; 16]);
        let migration_b = AssetUuid([53; 16]);
        let from = LogicalHash([54; 32]);
        let to = LogicalHash([55; 32]);
        let migration_value = || {
            AuthoredValue::Object(BTreeMap::from([
                ("from_hash".to_owned(), bytes(from.0)),
                ("from_lineage".to_owned(), AuthoredValue::Null),
                ("kind".to_owned(), AuthoredValue::Null),
                ("target_type_uuid".to_owned(), bytes(retired_type.0)),
                ("to_hash".to_owned(), bytes(to.0)),
                ("to_lineage".to_owned(), AuthoredValue::Null),
            ]))
        };
        let migration_entry = |uuid| AssetEntry {
            uuid,
            type_uuid: distill_core::bootstrap::MIGRATION_TYPE_UUID,
            schema_hash: LogicalHash([56; 32]),
            lineage: distill_bundle::EntryLineageV1::Bootstrap {
                bundle_format_version: 1,
            },
            authoring_only: true,
            data: migration_value(),
        };
        let bundle_uuid = BundleUuid([57; 16]);
        let source = ScannedBundle {
            root_name: "main".to_owned(),
            normalized_path: "migrations.bundle".to_owned(),
            file_hash: BundleFileHash([58; 32]),
            bytes: Vec::new(),
            parsed: Ok(Bundle {
                format_version: 1,
                uuid: bundle_uuid,
                primary: None,
                schemas: BTreeMap::new(),
                assets: BTreeMap::from([
                    ("a".to_owned(), migration_entry(migration_a)),
                    ("b".to_owned(), migration_entry(migration_b)),
                ]),
            }),
            namespace_skeleton: None,
        };

        let waiting = retired_waiting_projection(
            [&source],
            &BTreeSet::from([retired_type]),
            ContentHash([59; 32]),
            SnapshotStamp {
                instance: distill_store::state::StoreInstanceId([60; 16]),
                version: InputVersion(8),
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            waiting.error.references,
            [
                RetiredTypeReference::MigrationEndpoint(migration_a),
                RetiredTypeReference::MigrationEndpoint(migration_b),
            ]
        );
        assert_eq!(waiting.bundles, BTreeSet::from([bundle_uuid]));
    }

    #[test]
    fn prepared_candidate_error_cleanup_uses_the_explicit_unload_path() {
        let temp = tempfile::tempdir().unwrap();
        let mut runtime = CoordinatedPipelineRuntime {
            host: ModuleHost::new(temp.path().join("module-host")).unwrap(),
            loader: DynamicPipelineModuleLoader,
            pending: None,
        };
        let epoch = crate::epoch::empty_test_epoch();
        let observed = epoch.clone();
        let mut prepared = Some(epoch);

        discard_prepared(&mut runtime, &mut prepared);

        assert!(prepared.is_none());
        assert!(observed.status().unloaded);
    }

    #[test]
    fn production_admission_limits_concurrent_build_closures() {
        let temp = tempfile::tempdir().unwrap();
        let assets = temp.path().join("assets");
        std::fs::create_dir_all(&assets).unwrap();
        let mut config = StoreConfig::new(temp.path().join("state"));
        config.parallelism = 1;
        config.batch_reserved_workers = 1;
        let coordinator = Arc::new(
            DaemonCoordinator::open(
                config,
                vec![AssetRoot::new(
                    "main",
                    &assets,
                    assets.join(".distill-displaced"),
                )],
                LineageDestination {
                    root: "main".to_owned(),
                    path: "schema/lineage.bundle".to_owned(),
                },
                Vec::new(),
                8,
            )
            .unwrap(),
        );
        let (first_entered_tx, first_entered_rx) = mpsc::sync_channel(1);
        let (release_first_tx, release_first_rx) = mpsc::sync_channel(1);
        let first = {
            let coordinator = Arc::clone(&coordinator);
            thread::spawn(move || {
                coordinator.run_scheduled(WorkClass::Interactive, move || {
                    assert!(thread::current()
                        .name()
                        .is_some_and(|name| name.starts_with("distill-build-")));
                    first_entered_tx.send(()).unwrap();
                    release_first_rx.recv().unwrap();
                });
            })
        };
        first_entered_rx.recv().unwrap();

        let (second_entered_tx, second_entered_rx) = mpsc::sync_channel(1);
        let second = {
            let coordinator = Arc::clone(&coordinator);
            thread::spawn(move || {
                coordinator.run_scheduled(WorkClass::Interactive, move || {
                    assert!(thread::current()
                        .name()
                        .is_some_and(|name| name.starts_with("distill-build-")));
                    second_entered_tx.send(()).unwrap();
                });
            })
        };
        assert!(second_entered_rx
            .recv_timeout(Duration::from_millis(50))
            .is_err());
        release_first_tx.send(()).unwrap();
        second_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        first.join().unwrap();
        second.join().unwrap();
    }

    #[test]
    fn operational_resize_replaces_the_live_worker_pool() {
        let temp = tempfile::tempdir().unwrap();
        let assets = temp.path().join("assets");
        std::fs::create_dir(&assets).unwrap();
        let mut config = StoreConfig::new(temp.path().join("state"));
        config.parallelism = 1;
        config.batch_reserved_workers = 1;
        let coordinator = Arc::new(
            DaemonCoordinator::open(
                config.clone(),
                vec![AssetRoot::new(
                    "main",
                    &assets,
                    assets.join(".distill-displaced"),
                )],
                LineageDestination {
                    root: "main".to_owned(),
                    path: "schema/lineage.bundle".to_owned(),
                },
                Vec::new(),
                8,
            )
            .unwrap(),
        );
        config.parallelism = 2;
        coordinator
            .apply_operational_configuration(&config, 8)
            .unwrap();

        let (entered_tx, entered_rx) = mpsc::sync_channel(2);
        let (release_first_tx, release_first_rx) = mpsc::sync_channel(1);
        let (release_second_tx, release_second_rx) = mpsc::sync_channel(1);
        let first = {
            let coordinator = Arc::clone(&coordinator);
            let entered_tx = entered_tx.clone();
            thread::spawn(move || {
                coordinator.run_scheduled(WorkClass::Interactive, move || {
                    entered_tx.send(()).unwrap();
                    release_first_rx.recv().unwrap();
                });
            })
        };
        let second = {
            let coordinator = Arc::clone(&coordinator);
            thread::spawn(move || {
                coordinator.run_scheduled(WorkClass::Interactive, move || {
                    entered_tx.send(()).unwrap();
                    release_second_rx.recv().unwrap();
                });
            })
        };
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        release_first_tx.send(()).unwrap();
        release_second_tx.send(()).unwrap();
        first.join().unwrap();
        second.join().unwrap();
    }
}
