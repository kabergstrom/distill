//! Single-writer daemon coordinator.
//!
//! Filesystem scans are candidates. Publication happens only on the
//! authority thread ([`crate::authority`]), through one closure which
//! rechecks the durable store basis, applies the complete SQLite input
//! transaction, and returns the exact RPC projection for the same successor
//! version.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};

use arc_swap::{ArcSwap, ArcSwapOption};

use rayon::ThreadPool;

use distill_build::pipeline::Target;
use distill_bundle::{AssetEntry, Bundle};
use distill_core::bootstrap::is_bootstrap_control_type;
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, TypeUuid};
use distill_json::AuthoredValue;
use distill_rpc::{
    AssetDeltaState, AssetMutation, AuthoringEntry, AuthoringEntryRole, AuthoringMutation,
    AuthoringValue, Commit, ConfigurationError, ConfigurationStatus, CoordinatedCommitError,
    DerivedOutputEntry, DerivedOutputMutation, DriftedInput, PathMutation, PipelineDiagnostic,
    Server, ServerHandle, SnapshotStamp, StoredResolve, TargetDefinition, NamespaceError,
    PreparedImportCommit, RpcFailure, NamespaceErrorV1,
};
use distill_schema::ProjectSchemaAuthority;
use distill_store::bundles::{
    AssetRecord, BundleMeta, NamespaceSkeleton as StoreNamespaceSkeleton, ServedAuthoring,
    SkeletonEntry,
};
use distill_store::claims::{DerivedOutputClaim, SourceClaim, SourceClaims};
use distill_store::config::{PendingRestart, RestartOnlyChange};
use distill_store::files::{FileObservation, PendingFileWork};
use distill_store::pipeline::ValidatedPipelineEpoch;
use distill_store::served::{encode_authored_value, ResolutionRow};
use distill_store::state::{
    AssetClaimant, CleanupDisposition, ConfigurationState, DirectoryAliasSide, DscpV1,
    InputVersion, PipelineFailure, PipelineFailureCode, PipelineFailureOrigin,
    PipelineState as StoredPipelineState, ReadableBundleSource, ScanFailureCode, ScanSubject,
    SkeletonFailureCode,
};
use distill_store::{Store, StoreConfig, StoreError, StoreReader};

use crate::store_cell::AuthorityStore;
use crate::authority::{Authority, AuthorityCell, AuthorityRef, AuthoritySender};
use crate::authoring::{AuthoringService, AuthoringServiceInitError};
use crate::callbacks::EpochAuthoringImporter;
use crate::epoch::{
    stored_pipeline_epoch, CandidateRequirements, ModuleHost, PipelineEpoch, PipelineSnapshot,
};
use crate::importer::ImportRun;
use crate::module_loader::DynamicPipelineModuleLoader;
use crate::pipeline_map::PipelineProjection;
use crate::scanner::{
    AssetRoot, DaemonOwnedDirectoryKind, RootedScanner, ScanDelta, ScanDiagnostic, ScanError,
    ScanSnapshot, ScannedBundle, StoredBaseline,
};
use crate::scheduler::{ScheduledPool, Scheduler, SchedulerConfig, WorkClass};
use crate::watcher::{WatcherAction, WatcherBatch, WatcherQueue};

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogicalRename {
    root_name: String,
    from_path: String,
    to_path: String,
}

pub(crate) struct ConfigurationCandidate {
    pub roots: Vec<AssetRoot>,
    pub targets: Vec<TargetDefinition>,
    pub build_targets: BTreeMap<String, Target>,
    pub pipeline_source: PathBuf,
    pub requirements: CandidateRequirements,
    pub schema_authority: Arc<ProjectSchemaAuthority>,
}

pub struct DaemonCoordinator {
    store: Arc<AuthorityStore>,
    scanner: RootedScanner,
    scan_initialized: AtomicBool,
    scan_healthy: AtomicBool,
    scan_rejection: AuthorityCell<Option<PendingScanRejection>>,
    server: Arc<ServerHandle>,
    authoring: Arc<AuthoringService>,
    pipeline: PipelineState,
    schema_authority: ArcSwapOption<ProjectSchemaAuthority>,
    build_targets: ArcSwap<BTreeMap<String, Target>>,
    configuration_error: AuthorityCell<Option<ConfigurationError>>,
    operational: ScheduledPool,
    /// Last: stopped (and joined) after everything else is gone.
    authority: Authority,
}

struct CoordinatedPipelineRuntime {
    host: ModuleHost,
    loader: DynamicPipelineModuleLoader,
}

enum ConfigurationPipelinePublication {
    Epoch {
        epoch: ValidatedPipelineEpoch,
        tools: BTreeMap<String, distill_store::pipeline::ToolRegistrationV2>,
    },
    Failed(PipelineFailure),
}

fn discard_prepared(
    runtime: &mut CoordinatedPipelineRuntime,
    prepared: &mut Option<PipelineEpoch>,
) -> Option<PipelineFailure> {
    if let Some(prepared) = prepared.take() {
        runtime.host.discard_unpublished(prepared)
    } else {
        None
    }
}

fn record_cleanup_failure(slot: &mut Option<PipelineFailure>, failure: Option<PipelineFailure>) {
    if let Some(failure) = failure {
        if slot.is_none() {
            *slot = Some(failure);
        }
    }
}

impl DaemonCoordinator {
    pub fn open(
        store_config: StoreConfig,
        roots: Vec<AssetRoot>,
        targets: Vec<TargetDefinition>,
        max_dependency_depth: usize,
    ) -> Result<Self, CoordinatorInitError> {
        let module_state_path = store_config.state_path.join("pipeline-host");
        let state_path = store_config.state_path.clone();
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
        let target_set = distill_rpc::target_map(targets)?;
        // The store belongs to the authority from the start: it is opened
        // and first written there.
        let authority = Authority::start();
        let sender = authority.sender().clone();
        let (store, backend) = authority
            .sender()
            .run(|| -> Result<_, CoordinatorInitError> {
                let mut opened_store = Store::open(store_config.clone())?;
                // Claims follow the pipeline projection; the first full
                // publication rewrites them.
                opened_store.clear_source_claims()?;
                if opened_store.pending_restart()?.is_some() {
                    opened_store.input_transaction(|transaction| {
                        transaction.adopt_pending_restart().map(|_| ())
                    })?;
                }
                let version = opened_store.input_version();
                opened_store.served_transaction(|transaction| {
                    use distill_store::served::ServedWrite;
                    transaction.init_change_log_oldest(version)?;
                    distill_rpc::publish_target_set(transaction, &target_set)?;
                    Ok(())
                })?;
                let store = Arc::new(AuthorityStore::new(opened_store, sender));
                let backend = Arc::new(AuthoringService::new(
                    Arc::clone(&store),
                    roots,
                    scanner.clone(),
                ));
                Ok((store, backend))
            })
            .expect("the authority runs while its coordinator opens")?;
        let server = ServerHandle::open(
            store_config.clone(),
            backend.clone(),
            Arc::new(DaemonStore {
                store: Arc::clone(&store),
                authority: authority.sender().clone(),
            }),
        )?;
        let pipeline = CoordinatedPipelineRuntime {
            host,
            loader: DynamicPipelineModuleLoader,
        };
        let scheduler_config = SchedulerConfig {
            parallelism: store_config.parallelism,
            batch_reserved_workers: store_config.batch_reserved_workers,
            max_dependency_depth,
        };
        let operational = ScheduledPool::start(
            Scheduler::new(scheduler_config)
                .map_err(|error| CoordinatorInitError::Operational(error.to_string()))?,
            build_worker_pool(scheduler_config.parallelism)
                .map_err(CoordinatorInitError::Operational)?,
        )
        .map_err(|error| CoordinatorInitError::Operational(error.to_string()))?;
        Ok(Self {
            store,
            scanner,
            scan_initialized: AtomicBool::new(false),
            scan_healthy: AtomicBool::new(true),
            scan_rejection: AuthorityCell::new(None, authority.sender()),
            server,
            authoring: backend,
            pipeline: PipelineState::new(pipeline, authority.sender()),
            schema_authority: ArcSwapOption::empty(),
            build_targets: ArcSwap::from_pointee(BTreeMap::new()),
            configuration_error: AuthorityCell::new(None, authority.sender()),
            operational,
            authority,
        })
    }

    /// The authority thread's inbox.
    pub(crate) fn authority_sender(&self) -> &AuthoritySender {
        self.authority.sender()
    }

    /// Run `step` on the authority (inline when already on it).
    pub fn on_authority<T: Send>(&self, step: impl FnOnce() -> T + Send) -> T {
        self.server
            .on_authority(step)
            .expect("the authority runs while its coordinator is alive")
    }

    /// Publish one coordinated step against `base` on the authority.
    pub fn coordinated_commit(
        &self,
        base: InputVersion,
        publish: impl FnOnce() -> Result<Commit, String> + Send,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        self.on_authority(|| self.server().coordinated_commit(base, publish))
    }

    /// This thread's RPC front end.
    pub fn server(&self) -> Server {
        Server::attach(&self.server)
    }

    pub fn server_handle(&self) -> &Arc<ServerHandle> {
        &self.server
    }

    /// Attach this coordinator as the RPC server's production lazy-build
    /// authority after it has been placed in its final `Arc`.
    pub fn attach_build_backend(self: &Arc<Self>) {
        let backend = Arc::new(crate::build::CoordinatorBuildBackend::new(self));
        let server = self.server();
        server.install_build_backend(backend.clone());
        server.install_artifact_lease_backend(backend);
        self.authoring.attach_tag_index_coordinator(self);
    }

    pub fn store(&self) -> Arc<AuthorityStore> {
        Arc::clone(&self.store)
    }

    pub fn scanner(&self) -> RootedScanner {
        self.scanner.clone()
    }

    /// Current non-fatal filesystem exclusions in canonical rooted-path
    /// order. These rows are also reported by `doctor verify`.
    pub fn scan_diagnostics(&self) -> Result<Vec<ScanDiagnostic>, CoordinatorError> {
        ScanSnapshot::load_diagnostics(&self.store.read())
            .map(|scan| scan.diagnostic_rows().cloned().collect())
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    /// The published observation, from the store's scan tables.
    fn published_scan(&self) -> Result<ScanSnapshot, CoordinatorError> {
        ScanSnapshot::load(&self.store.read())
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    pub fn authoring_service(&self) -> &Arc<AuthoringService> {
        &self.authoring
    }

    pub fn pipeline_snapshot(&self) -> PipelineSnapshot {
        PipelineSnapshot::clone(&self.pipeline.published.load())
    }

    /// Persist the first runtime failure latched by a published callback
    /// without minting a new input version. The in-memory epoch token fences
    /// work immediately; this closes the crash/restart durability side of the
    /// same monotonic transition.
    pub(crate) fn sync_runtime_pipeline_failure(
        &self,
    ) -> Result<Option<PipelineFailure>, CoordinatorError> {
        self.on_authority(|| {
            let runtime = lock_pipeline(&self.pipeline);
            let Some(epoch) = runtime.host.published_ready_epoch() else {
                return Ok(None);
            };
            let observed = match runtime.host.snapshot().epoch() {
                Ok(_) => return Ok(None),
                Err(failure) if failure.origin == PipelineFailureOrigin::PublishedRuntime => {
                    (epoch.dylib_hash(), failure)
                }
                Err(_) => return Ok(None),
            };
            let diagnostic = observed.1.clone();
            self.server()
                .coordinated_runtime_pipeline_failure(diagnostic, || {
                    let mut store = self.store.write();
                    match store.fail_published_pipeline_epoch(observed.0, &observed.1) {
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
        })
    }

    pub fn schema_authority(&self) -> Option<Arc<ProjectSchemaAuthority>> {
        self.schema_authority.load_full()
    }

    pub fn build_target(&self, name: &str) -> Option<Target> {
        self.build_targets.load()
            .get(name)
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn install_schema_authority_for_test(&self, authority: Arc<ProjectSchemaAuthority>) {
        self.schema_authority.store(Some(authority));
    }

    #[cfg(test)]
    pub(crate) fn install_build_target_for_test(&self, name: &str, target: Target) {
        self.build_targets.rcu(|current| {
            let mut targets = BTreeMap::clone(current);
            targets.insert(name.to_owned(), target.clone());
            targets
        });
    }

    #[cfg(test)]
    pub(crate) fn install_pipeline_epoch_for_test(&self, epoch: PipelineEpoch) {
        self.on_authority(|| lock_pipeline(&self.pipeline).host.install_ready(epoch));
    }

    pub fn operational_configuration(&self) -> SchedulerConfig {
        self.operational.config()
    }

    pub(crate) fn run_scheduled<R>(
        self: &Arc<Self>,
        class: WorkClass,
        run: impl FnOnce() -> R + Send + 'static,
    ) -> R
    where
        R: Send + 'static,
    {
        // An authority that waits here lends itself to the worker, whose
        // store writes would otherwise wait on it.
        let lent = self.authority_sender().lend();
        let (sender, receiver) = mpsc::sync_channel(1);
        self.operational.submit(class, move || {
            let authority = lent.map(crate::authority::Lent::enter);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
            drop(authority);
            let _ = sender.send(outcome);
        });
        match receiver.recv() {
            Ok(Ok(result)) => result,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => panic!("distill build worker stopped before returning its job"),
        }
    }

    pub fn apply_operational_configuration(
        &self,
        store_config: &StoreConfig,
        max_dependency_depth: usize,
    ) -> Result<(), CoordinatorError> {
        self.on_authority(|| {
            let scheduler = SchedulerConfig {
                parallelism: store_config.parallelism,
                batch_reserved_workers: store_config.batch_reserved_workers,
                max_dependency_depth,
            };
            scheduler
                .validate()
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
            let replacement_pool = {
                (self.operational.config().parallelism != scheduler.parallelism)
                    .then(|| build_worker_pool(scheduler.parallelism))
                    .transpose()
                    .map_err(CoordinatorError::InvalidManifest)?
            };
            self.store.write()
                .apply_operational_config(store_config)
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
            self.operational
                .reconfigure(scheduler, replacement_pool)
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
        })
    }

    pub fn stage_restart_configuration(
        &self,
        changes: &[RestartOnlyChange],
    ) -> Result<PendingRestart, CoordinatorError> {
        self.on_authority(|| {
            let pending = self.store.write()
                .stage_pending_restart(changes)
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
            self.server().restart_required(pending.keys.clone());
            Ok(pending)
        })
    }

    pub fn clear_restart_configuration(&self) -> Result<(), CoordinatorError> {
        self.on_authority(|| {
            self.store.write()
                .clear_pending_restart()
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
            self.server().restart_required(Vec::new());
            Ok(())
        })
    }

    fn configuration_error(&self) -> Option<ConfigurationError> {
        let source = self
            .configuration_error
            .borrow_mut()
            .clone();
        let scan = self
            .scan_rejection
            .borrow_mut()
            .as_ref()
            .and_then(|pending| pending.rejection.configuration.clone());
        ConfigurationError::select_canonical(source.into_iter().chain(scan))
            .ok()
            .flatten()
    }

    /// The namespace errors of the pending scan rejection.
    fn pending_scan_errors(&self) -> Vec<NamespaceError> {
        self.scan_rejection
            .borrow_mut()
            .as_ref()
            .map_or_else(Vec::new, |pending| pending.rejection.version.clone())
    }

    fn configuration_without_source_error(&self) -> ConfigurationStatus {
        self.scan_rejection
            .borrow_mut()
            .as_ref()
            .and_then(|pending| pending.rejection.configuration.clone())
            .map_or(ConfigurationStatus::Ready, ConfigurationStatus::Failed)
    }

    /// Publish a rejected configuration source as an ordinary input version.
    /// The overlay participates in every concurrent scan classification, so a
    /// scan failure cannot erase the candidate's typed configuration reason.
    pub fn publish_configuration_rejection(
        &self,
        reason: DscpV1,
        message: impl Into<String>,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let message: String = message.into();
        self.on_authority(|| {
            let error = ConfigurationError::from_reason(&reason, message);
            let previous = {
                let mut current = self
                    .configuration_error
                    .borrow_mut();
                current.replace(error)
            };
            match self.publish_cached_scan() {
                Ok(stamp) => Ok(stamp),
                Err(error) => {
                    *self
                        .configuration_error
                        .borrow_mut() = previous;
                    Err(error)
                }
            }
        })
    }

    pub fn heal_configuration_rejection(&self) -> Result<SnapshotStamp, CoordinatorError> {
        self.on_authority(|| {
            let previous = self
                .configuration_error
                .borrow_mut()
                .take();
            match self.publish_cached_scan() {
                Ok(stamp) => Ok(stamp),
                Err(error) => {
                    *self
                        .configuration_error
                        .borrow_mut() = previous;
                    Err(error)
                }
            }
        })
    }

    pub(crate) fn publish_configuration_candidate(
        &self,
        candidate: ConfigurationCandidate,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        self.on_authority(|| {
            let ConfigurationCandidate {
                roots,
                targets,
                build_targets,
                pipeline_source,
                mut requirements,
                schema_authority,
            } = candidate;
            let filesystem = self
                .authoring
                .prepare_filesystem_candidate(roots)
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
            let filesystem_changed = !self.scanner.has_same_roots(filesystem.scanner());
            // Schema, target, or module-only candidates reuse the already
            // observed asset snapshot. A physical complete scan is reserved for
            // an actual configured-root replacement.
            let candidate_scan_heals =
                filesystem_changed || !self.scan_initialized.load(Ordering::Acquire);
            let scan = if candidate_scan_heals {
                filesystem.scanner().scan()?
            } else {
                self.published_scan()?
            };
            let mut candidate = ScanCandidate::build(
                scan,
                None,
                Some(&schema_authority),
            )?;
            if !candidate_scan_heals {
                candidate
                        .namespace_errors
                        .extend(self.pending_scan_errors());
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
                            let failure = cleanup.unwrap_or_else(|| {
                                PipelineFailure::new(
                                    PipelineFailureCode::CandidateRegistration,
                                    PipelineFailureOrigin::CandidateOpen,
                                    CleanupDisposition::CleanedAndClosed,
                                    format!("invalid target-resolved pipeline map: {error:?}"),
                                )
                                .expect("candidate-registration cleanup tuple is valid")
                            });
                            (
                                ConfigurationPipelinePublication::Failed(failure),
                                None,
                                PipelineProjection::default(),
                            )
                        }
                        Ok(projection) => {
                            let stored = match stored_pipeline_epoch(&prepared, &requirements) {
                                Ok(stored) => stored,
                                Err(error) => {
                                    if let Some(failure) = runtime.host.discard_unpublished(prepared) {
                                        drop(runtime);
                                        return self.publish_pipeline_rejection(failure);
                                    }
                                    return Err(CoordinatorError::InvalidManifest(error.to_string()));
                                }
                            };
                            if let Err(error) = self.authoring.prepare_pipeline_importers(
                                EpochAuthoringImporter::metadata_only(prepared.importer_descriptors()),
                            ) {
                                if let Some(failure) = runtime.host.discard_unpublished(prepared) {
                                    drop(runtime);
                                    return self.publish_pipeline_rejection(failure);
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
                Err(failure) => (
                    ConfigurationPipelinePublication::Failed(failure),
                    None,
                    PipelineProjection::default(),
                ),
            };
            let installed_claims = match bundle_claims(
                candidate.scan.bundle_rows(),
                &projection,
                Some(&schema_authority),
            ) {
                Ok(claims) => claims,
                Err(error) => {
                    if let Some(failure) = discard_prepared(&mut runtime, &mut prepared_epoch) {
                        drop(runtime);
                        return self.publish_pipeline_rejection(failure);
                    }
                    return Err(error);
                }
            };
            let tag_epoch = schema_authority.source_hash();
            let max_dependency_depth = self.operational_configuration().max_dependency_depth;
            let base = self.server().current_stamp().version;
            let store = Arc::clone(&self.store);
            let fallback_bundles = match store.read().all_asset_bundles() {
                Ok(bundles) => bundles,
                Err(error) => {
                    if let Some(failure) = discard_prepared(&mut runtime, &mut prepared_epoch) {
                        drop(runtime);
                        return self.publish_pipeline_rejection(failure);
                    }
                    return Err(CoordinatorError::InvalidManifest(error.to_string()));
                }
            };
            let authoring = Arc::clone(&self.authoring);
            let mut cleanup_failure = None;
            let result = self
                .server()
                .coordinated_replace_target_set(base, targets, || {
                    let mut commit = publish_scan(
                        &store,
                        base,
                        candidate,
                        true,
                        Some(&pipeline),
                        &projection,
                        tag_epoch,
                        &installed_claims,
                    )
                    .map_err(|error| error.to_string())?;
                    let published_pipeline = commit
                        .pipeline
                        .clone()
                        .expect("scan commit always carries pipeline diagnostics");
                    self.scanner.replace_from(filesystem.scanner());
                    authoring.install_filesystem_candidate(filesystem);
                    self.scan_initialized.store(true, Ordering::Release);
                    if candidate_scan_heals {
                        self.scan_rejection
                            .borrow_mut()
                            .take();
                        self.scan_healthy.store(true, Ordering::Release);
                    }
                    self.schema_authority
                        .store(Some(Arc::clone(&schema_authority)));
                    self.build_targets.store(Arc::new(build_targets.clone()));
                    authoring.install_pipeline_projection(projection.clone());
                    self.configuration_error
                        .borrow_mut()
                        .take();
                    match (&pipeline, prepared_epoch.take(), published_pipeline) {
                        (
                            ConfigurationPipelinePublication::Epoch { .. },
                            Some(prepared),
                            PipelineDiagnostic::Ready,
                        ) => {
                            let importers = EpochAuthoringImporter::all(&prepared);
                            runtime.host.install_ready(prepared);
                            authoring
                                .replace_pipeline_importers(importers)
                                .expect("candidate importer metadata was prevalidated");
                        }
                        (
                            ConfigurationPipelinePublication::Epoch { .. },
                            Some(prepared),
                            PipelineDiagnostic::Failed(error),
                        ) => {
                            record_cleanup_failure(
                                &mut cleanup_failure,
                                runtime.host.discard_unpublished(prepared),
                            );
                            runtime.host.install_failure(error);
                            authoring.install_pipeline_importers(BTreeMap::new());
                        }
                        (
                            ConfigurationPipelinePublication::Failed(failure),
                            None,
                            PipelineDiagnostic::Failed(_),
                        ) => {
                            runtime.host.install_failure(failure.clone());
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
                    let failure = cleanup_failure.take();
                    if let Some(failure) = failure {
                        drop(runtime);
                        self.publish_pipeline_rejection(failure)
                    } else {
                        Ok(stamp)
                    }
                }
                Err(error) => {
                    if let Some(failure) = discard_prepared(&mut runtime, &mut prepared_epoch) {
                        drop(runtime);
                        return self.publish_pipeline_rejection(failure);
                    }
                    Err(CoordinatorError::Coordinated(error))
                }
            }
        })
    }

    /// Stage, attest, durably publish, and only then expose one pipeline
    /// candidate. The store transaction and RPC commit share the exact base
    /// version, so scanner or authoring work cannot split the epoch.
    pub fn publish_pipeline_candidate(
        &self,
        source: &std::path::Path,
        mut requirements: CandidateRequirements,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        self.on_authority(|| {
            let base = self.server().current_stamp().version;
            let mut runtime = lock_pipeline(&self.pipeline);
            let prepared = {
                let CoordinatedPipelineRuntime { host, loader, .. } = &mut *runtime;
                host.prepare_candidate(source, &mut requirements, loader)
            };
            let prepared = match prepared {
                Ok(prepared) => prepared,
                Err(failure) => {
                    drop(runtime);
                    return self.publish_pipeline_rejection(failure);
                }
            };

            let stored = match stored_pipeline_epoch(&prepared, &requirements) {
                Ok(stored) => stored,
                Err(error) => {
                    if let Some(failure) = runtime.host.discard_unpublished(prepared) {
                        drop(runtime);
                        return self.publish_pipeline_rejection(failure);
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
                if let Some(failure) = runtime.host.discard_unpublished(prepared) {
                    drop(runtime);
                    return self.publish_pipeline_rejection(failure);
                }
                return Err(CoordinatorError::InvalidManifest(format!("{error:?}")));
            }
            let store = Arc::clone(&self.store);
            let tools = prepared.tool_epoch();
            let authority = match self.schema_authority() {
                Some(authority) => authority,
                None => {
                    if let Some(failure) = runtime.host.discard_unpublished(prepared) {
                        drop(runtime);
                        return self.publish_pipeline_rejection(failure);
                    }
                    return Err(CoordinatorError::InvalidManifest(
                        "pipeline publication requires project schema authority".to_owned(),
                    ));
                }
            };
            let tag_epoch = authority.source_hash();
            let asset_bundles = match store.read().all_asset_bundles() {
                Ok(bundles) => bundles,
                Err(error) => {
                    if let Some(failure) = runtime.host.discard_unpublished(prepared) {
                        drop(runtime);
                        return self.publish_pipeline_rejection(failure);
                    }
                    return Err(CoordinatorError::InvalidManifest(error.to_string()));
                }
            };
            let assets = asset_bundles.keys().copied().collect::<Vec<_>>();
            let targets = self
                .build_targets.load_full();
            let scanner = self.scanner.clone();
            let max_dependency_depth = self.operational_configuration().max_dependency_depth;
            let mut prepared = Some(prepared);
            let result = self.server().coordinated_commit(base, || {
                let mut durable = store.write();
                if durable.input_version() != base {
                    return Err(format!(
                        "durable pipeline basis is {:?}, expected {base:?}",
                        durable.input_version()
                    ));
                }
                durable
                    .input_transaction(|transaction| {
                        transaction.publish_pipeline_epoch(&stored)?;
                        transaction.publish_tool_epoch(&tools)?;
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
                let importers = EpochAuthoringImporter::all(&candidate);
                runtime.host.install_ready(candidate);
                self.authoring
                    .replace_pipeline_importers(importers)
                    .expect("candidate importer metadata was prevalidated");
                let mut commit = Commit {
                    pipeline: Some(PipelineDiagnostic::Ready),
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
                Ok(stamp) => Ok(stamp),
                Err(error) => {
                    if let Some(candidate) = prepared.take() {
                        if let Some(failure) = runtime.host.discard_unpublished(candidate) {
                            drop(runtime);
                            return self.publish_pipeline_rejection(failure);
                        }
                    }
                    Err(CoordinatorError::Coordinated(error))
                }
            }
        })
    }

    /// Publish a candidate-input failure which occurs before a module can be
    /// opened (for example, malformed watched schema JSON). The last durable
    /// schema/target projection remains intact, while the new input version is
    /// explicitly pipeline-failed and the live module is fenced.
    pub fn publish_pipeline_rejection(
        &self,
        failure: PipelineFailure,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        self.on_authority(|| {
            self.publish_pipeline_rejection_inner(failure, false)
        })
    }

    pub(crate) fn publish_pipeline_rejection_healing_configuration(
        &self,
        failure: PipelineFailure,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        self.on_authority(|| {
            self.publish_pipeline_rejection_inner(failure, true)
        })
    }

    fn publish_pipeline_rejection_inner(
        &self,
        failure: PipelineFailure,
        heal_configuration: bool,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let healed_configuration = heal_configuration
            .then(|| self.configuration_without_source_error());
        let base = self.server().current_stamp().version;
        let store = Arc::clone(&self.store);
        let diagnostic = failure.clone();
        let result = self.server().coordinated_commit(base, || {
            let mut store = store.write();
            if store.input_version() != base {
                return Err(format!(
                    "durable pipeline-failure basis is {:?}, expected {base:?}",
                    store.input_version()
                ));
            }
            let generation = match store
                .configuration_state()
                .map_err(|error| error.to_string())?
            {
                ConfigurationState::Ready(epoch) => epoch.generation,
                ConfigurationState::Failed { last_good, .. } => {
                    last_good.map_or(0, |epoch| epoch.generation)
                }
            };
            store
                .input_transaction(|transaction| {
                    transaction.publish_pipeline_failure(&diagnostic)?;
                    match healed_configuration.as_ref() {
                        Some(ConfigurationStatus::Ready) => {
                            transaction.publish_configuration_ready(generation)?
                        }
                        Some(ConfigurationStatus::Failed(error)) => transaction
                            .publish_configuration_error(&error.detail, &error.message)?,
                        None => {}
                    }
                    Ok(())
                })
                .map_err(|error| error.to_string())?;
            Ok(Commit {
                configuration: healed_configuration.clone(),
                pipeline: Some(PipelineDiagnostic::Failed(diagnostic.clone())),
                pipeline_epoch_changed: true,
                ..Commit::default()
            })
        });
        match result {
            Ok(stamp) => {
                let mut runtime = lock_pipeline(&self.pipeline);
                runtime.host.install_failure(failure);
                self.authoring.install_pipeline_importers(BTreeMap::new());
                if heal_configuration {
                    self.configuration_error
                        .borrow_mut()
                        .take();
                }
                Ok(stamp)
            }
            Err(error) => Err(CoordinatorError::Coordinated(error)),
        }
    }

    /// The loop's CAS pass: evict to the cache limit, compact, and delete
    /// the dead segments whose grace has passed.
    pub(crate) fn maintain_cas(
        &self,
        sweeper: &mut distill_store::cas::SegmentSweeper,
    ) -> Result<(), distill_store::StoreError> {
        let mut store = self.store.write();
        store.enforce_cache_limit()?;
        store.compact()?;
        sweeper.sweep(&mut store)?;
        Ok(())
    }

    /// Reconcile one complete identity-checked namespace scan.
    pub fn reconcile_full_scan(&self) -> Result<SnapshotStamp, CoordinatorError> {
        self.on_authority(|| {
            match self.scanner.scan() {
                Ok(scan)
                    if self.scan_healthy.load(Ordering::Acquire)
                        && self.scan_initialized.load(Ordering::Acquire) =>
                {
                    let mut store = self.store.write();
                    if scan.same_namespace_observation(&ScanSnapshot::load(&store)?) {
                        // Warning-grade exclusions are scanner state, not authored
                        // input: refresh them without minting an input version.
                        store.replace_scan_diagnostics(None, &scan.encoded_diagnostic_rows())?;
                        drop(store);
                        Ok(self.server().current_stamp())
                    } else {
                        drop(store);
                        self.publish_scan(scan)
                    }
                }
                Ok(scan) => self.publish_scan(scan),
                Err(error) => {
                    self.scan_healthy.store(false, Ordering::Release);
                    self.publish_scan_rejection(&error, true)
                }
            }
        })
    }

    /// Full startup/recovery scan. Events already queued are replayed
    /// incrementally after the scan commits; only admitted overflow repeats
    /// the complete scan. Later events wait in the authority inbox.
    pub fn reconcile_startup(
        &self,
        queue: &mut WatcherQueue,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        loop {
            queue.arm_scan();
            let scan = self.reconcile_full_scan();
            let action = queue.finish_scan();
            let stamp = match scan {
                Ok(stamp) => stamp,
                Err(error) => {
                    // Always release scan ownership. Preserve anything that
                    // arrived while traversal was active so a caller that
                    // retries can still reconcile the complete event union.
                    queue.requeue_action(action);
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
        self.on_authority(|| {
            let pending_subjects = self
                .scan_rejection
                .borrow_mut()
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
            // On the authority: no other publication interleaves with this step.
            let server = self.server();
            let (delta, baseline) = {
                let store = self.store.read();
                let stored = StoredBaseline::new(&store);
                let delta = self.scanner.scan_incremental_delta(&stored, &scan_paths);
                stored.finish()?;
                match delta {
                    Ok(None) => return Ok(server.current_stamp()),
                    // The published rows the delta replaces.
                    Ok(Some(delta)) => {
                        let baseline = ScanSnapshot::load_under(&store, delta.affected_prefixes())?;
                        (delta, baseline)
                    }
                    Err(error) => {
                        drop(store);
                        self.scan_healthy.store(false, Ordering::Release);
                        return self.publish_scan_rejection(&error, heals_pending_rejection);
                    }
                }
            };
            let healed_rejection = heals_pending_rejection
                .then(|| {
                    self.scan_rejection
                        .borrow_mut()
                        .take()
                })
                .flatten();
            if delta.is_same_namespace_observation(&baseline)
                && renames.is_empty()
                && self.scan_healthy.load(Ordering::Acquire)
            {
                // Diagnostics are replaced with their affected subtree even when
                // the authored namespace itself did not change.
                self.store.write().replace_scan_diagnostics(
                    Some(delta.affected_prefixes()),
                    &delta.observed().encoded_diagnostic_rows(),
                )?;
                return Ok(server.current_stamp());
            }
            let projection = self.authoring.pipeline_projection();
            let authority = self.schema_authority();
            let claims = match bundle_claims(
                delta
                    .observed_bundle_entries()
                    .map(|(_, source)| source.as_ref()),
                &projection,
                authority.as_deref(),
            ) {
                Ok(claims) => claims,
                Err(error) => {
                    if let Some(rejection) = healed_rejection {
                        *self
                            .scan_rejection
                            .borrow_mut() = Some(rejection);
                    }
                    return Err(error);
                }
            };
            let inputs = PlanInputs {
                configuration_error: self.configuration_error(),
                namespace_errors: self.pending_scan_errors(),
                authority: authority.as_deref(),
                fresh: fresh_bundles(&delta),
            };
            let base = server.current_stamp().version;
            let store = Arc::clone(&self.store);
            let tag_epoch = authority
                .as_ref()
                .map_or([0; 32], |authority| authority.source_hash());
            let pipeline = self.pipeline_snapshot();
            let targets = self
                .build_targets.load_full();
            let max_dependency_depth = self.operational_configuration().max_dependency_depth;
            let scanner = self.scanner.clone();
            let result = server.coordinated_commit(base, || {
                let mut commit = publish_incremental_scan(
                    &store,
                    base,
                    &baseline,
                    &delta,
                    &claims,
                    &inputs,
                    &renames,
                    &projection,
                    tag_epoch,
                )
                .map_err(|error| error.to_string())?;
                if let Some(authority) = authority.clone() {
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
                    self.scan_healthy.store(
                        self.scan_rejection
                            .borrow_mut()
                            .is_none(),
                        Ordering::Release,
                    );
                    Ok(stamp)
                }
                Err(error) => {
                    if let Some(rejection) = healed_rejection {
                        *self
                            .scan_rejection
                            .borrow_mut() = Some(rejection);
                    }
                    Err(CoordinatorError::Coordinated(error))
                }
            }
        })
    }

    fn publish_cached_scan(&self) -> Result<SnapshotStamp, CoordinatorError> {
        let scan = self.published_scan()?;
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
        let authority = self.schema_authority();
        let mut candidate = ScanCandidate::build(
            scan,
            self.configuration_error(),
            authority.as_deref(),
        )?;
        if !heals_scan_rejection {
            candidate
                    .namespace_errors
                    .extend(self.pending_scan_errors());
        }
        candidate.renames.extend_from_slice(renames);
        let base = self.server().current_stamp().version;
        let store = Arc::clone(&self.store);
        let projection = self.authoring.pipeline_projection();
        let claims = bundle_claims(
            candidate.scan.bundle_rows(),
            &projection,
            authority.as_deref(),
        )?;
        let tag_epoch = authority
            .as_ref()
            .map_or([0; 32], |authority| authority.source_hash());
        let pipeline = self.pipeline_snapshot();
        let build_targets = self
            .build_targets.load_full();
        let max_dependency_depth = self.operational_configuration().max_dependency_depth;
        let scanner = self.scanner.clone();
        let fallback_bundles = store.read()
            .all_asset_bundles()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let stamp = self
            .server()
            .coordinated_commit(base, || {
                let mut commit = publish_scan(
                    &store,
                    base,
                    candidate,
                    false,
                    None,
                    &projection,
                    tag_epoch,
                    &claims,
                )
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
        self.scan_initialized.store(true, Ordering::Release);
        if heals_scan_rejection {
            self.scan_rejection
                .borrow_mut()
                .take();
        }
        self.scan_healthy.store(
            self.scan_rejection
                .borrow_mut()
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
            .borrow_mut()
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
        // The scan could not observe the rejected subjects, so the namespace
        // keeps what it published; only the errors change.
        let version = self
            .store
            .read()
            .claims_namespace_errors()?
            .into_iter()
            .chain(rejection.version.iter().cloned())
            .collect::<Vec<_>>();
        let source_configuration = self
            .configuration_error
            .borrow_mut()
            .clone();
        let external_configuration = ConfigurationError::select_canonical(
            source_configuration
                .into_iter()
                .chain(rejection.configuration.clone()),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let configuration =
            external_configuration.map_or(ConfigurationStatus::Ready, ConfigurationStatus::Failed);
        let base = self.server().current_stamp().version;
        let store = Arc::clone(&self.store);
        let stamp = self
            .server()
            .coordinated_commit(base, || {
                let mut store = store.write();
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
                    ConfigurationState::Failed { last_good, .. } => {
                        last_good.map_or(0, |epoch| epoch.generation)
                    }
                };
                store
                    .input_transaction(|transaction| {
                        transaction.set_namespace_errors(version.iter().cloned())?;
                        match &configuration {
                            ConfigurationStatus::Ready => {
                                transaction.publish_configuration_ready(generation)?
                            }
                            ConfigurationStatus::Failed(error) => transaction
                                .publish_configuration_error(&error.detail, &error.message)?,
                        }
                        Ok(())
                    })
                    .map_err(|error| error.to_string())?;
                let commit = Commit {
                    configuration: Some(configuration.clone()),
                    namespace_errors: Some(version.clone()),
                    ..Commit::default()
                };
                Ok(commit)
            })
            .map_err(CoordinatorError::Coordinated)?;
        *self
            .scan_rejection
            .borrow_mut() = Some(pending);
        Ok(stamp)
    }

    /// Rerun watched imports whose complete outcome-bearing basis drifted.
    /// Each bundle publishes as its own version so a later conflict cannot
    /// roll back an earlier per-file success.
    pub fn reconcile_watched_imports(&self) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let pending = self
            .on_authority(|| self.authoring.watched_imports_needing_reimport())
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
        let pending = self
            .on_authority(|| {
                if capabilities_changed {
                    self.authoring
                        .watched_imports_affected_by_capabilities(&work.dirty, &work.renames)
                } else {
                    self.authoring
                        .watched_imports_affected_by(&work.dirty, &work.renames)
                }
            })
            .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        self.reconcile_watched_import_bundles(pending)
    }

    fn reconcile_watched_import_bundles(
        &self,
        pending: Vec<BundleUuid>,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        self.reconcile_watched(
            &pending,
            |base, bundle| self.authoring.run_reimport_bundle(base, *bundle),
            |base, bundle| self.authoring.prepare_watched_reimport(base, *bundle),
        )
    }

    /// Run each watched import in parallel, off the authority, at the
    /// current version; then publish them in order on the authority, each as
    /// its own version. A run that an earlier publication made stale reruns
    /// there, at the version it publishes after.
    fn reconcile_watched<T: Sync>(
        &self,
        items: &[T],
        run: impl Fn(InputVersion, &T) -> Result<ImportRun, RpcFailure> + Sync,
        rerun: impl Fn(InputVersion, &T) -> Result<Option<PreparedImportCommit>, RpcFailure> + Sync,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        use rayon::prelude::*;

        if items.is_empty() {
            return Ok(Vec::new());
        }
        let base = self.server().current_stamp().version;
        let runs = items
            .par_iter()
            .map(|item| run(base, item))
            .collect::<Vec<_>>();
        self.on_authority(|| {
            let mut imported = Vec::with_capacity(items.len());
            for (item, run) in items.iter().zip(runs) {
                let current = self.server().current_stamp().version;
                let drifted = current != base;
                let mut bundle = None;
                let publication = self
                    .server()
                    .coordinated_maybe_commit(current, || {
                        let prepared = match run {
                            Ok(run) => match self.authoring.publish_watched_import(current, run) {
                                Err(_) if drifted => rerun(current, item),
                                published => published,
                            },
                            Err(_) if drifted => rerun(current, item),
                            Err(error) => Err(error),
                        }
                        .map_err(|error| format!("{error:?}"))?;
                        Ok(prepared.map(|prepared| {
                            bundle = Some(prepared.bundle);
                            prepared.commit
                        }))
                    })
                    .map_err(CoordinatorError::Coordinated)?;
                if publication.is_some() {
                    imported.push(bundle.expect("a published watched import names its bundle"));
                }
            }
            Ok(imported)
        })
    }


    pub fn pending_file_work(&self) -> Result<PendingFileWork, CoordinatorError> {
        self.store.read()
            .pending_file_work()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    pub fn acknowledge_file_work(&self, work: &PendingFileWork) -> Result<(), CoordinatorError> {
        self.on_authority(|| {
            if work.is_empty() {
                return Ok(());
            }
            match self.store.write().acknowledge_file_work(work) {
                Ok(true) => Ok(()),
                Ok(false) => Err(CoordinatorError::InvalidManifest(
                    "watcher work observation changed before acknowledgement".to_owned(),
                )),
                Err(error) => Err(CoordinatorError::InvalidManifest(error.to_string())),
            }
        })
    }

    /// Discover and apply authored directory-import rules. Every generated
    /// bundle is a separate versioned fold; orphaned prior outputs
    /// are deliberately retained and therefore never appear as deletion work.
    pub fn reconcile_directory_imports(&self) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let tasks = self
            .on_authority(|| self.authoring.directory_import_tasks())
            .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        self.reconcile_directory_import_tasks(tasks)
    }

    pub fn reconcile_directory_imports_affected(
        &self,
        work: &PendingFileWork,
        capabilities_changed: bool,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        let tasks = self
            .on_authority(|| {
                if capabilities_changed {
                    self.authoring
                        .directory_import_tasks_affected_by_capabilities(&work.dirty, &work.renames)
                } else {
                    self.authoring
                        .directory_import_tasks_affected_by(&work.dirty, &work.renames)
                }
            })
            .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
        self.reconcile_directory_import_tasks(tasks)
    }

    fn reconcile_directory_import_tasks(
        &self,
        tasks: Vec<crate::importer::DirectoryImportTask>,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        self.reconcile_watched(
            &tasks,
            |base, task| self.authoring.run_watched_directory_import(base, task),
            |base, task| self.authoring.prepare_watched_directory_import(base, task),
        )
    }
}

#[derive(Clone)]
struct ScanRejection {
    version: Vec<NamespaceError>,
    configuration: Option<ConfigurationError>,
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
        NamespaceError::canonical_set(rejections.iter().flat_map(|item| item.version.clone()))
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
    let configuration = ConfigurationError::select_canonical(
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
        let namespace_error = NamespaceError::new(
            NamespaceErrorV1::InvalidPhysicalPath {
                root_name: root_name.clone(),
                raw_relative_path: raw_relative_path.clone(),
                failure: *failure,
            },
            error.to_string(),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        return Ok(ScanRejection {
            version: vec![namespace_error],
            configuration: None,
        });
    }
    if let ScanError::SameRootNormalizedPathCollision {
        root_name,
        normalized_path,
        claims,
    } = error
    {
        let namespace_error = NamespaceError::new(
            NamespaceErrorV1::SameRootNormalizedPathCollision {
                root_name: root_name.clone(),
                normalized_path: normalized_path.clone(),
                claims: claims.clone(),
            },
            error.to_string(),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        return Ok(ScanRejection {
            version: vec![namespace_error],
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
            version: Vec::new(),
            configuration: Some(ConfigurationError::from_reason(&reason, error.to_string())),
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
            version: Vec::new(),
            configuration: Some(ConfigurationError::from_reason(&reason, error.to_string())),
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
    let detail = NamespaceErrorV1::UnreadableScanSubtree { subject, failure };
    let namespace_error = NamespaceError::new(detail, error.to_string())
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
    Ok(ScanRejection {
        version: vec![namespace_error],
        configuration: None,
    })
}

#[derive(Debug)]
pub enum CoordinatorInitError {
    Store(StoreError),
    Scan(ScanError),
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

impl From<StoreError> for CoordinatorError {
    fn from(error: StoreError) -> Self {
        Self::InvalidManifest(error.to_string())
    }
}

type ScanKey = (String, String);

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

/// What collides is withheld from publication (LOCKLESS.md §4): a bundle
/// UUID more than one file claims, with every asset it holds, and an asset
/// UUID claimed more than once. Everything else publishes. A withheld asset
/// resolves `Failed` with its error until the collision heals.
#[derive(Debug, Default)]
struct Withheld {
    bundles: BTreeSet<BundleUuid>,
    assets: BTreeMap<AssetUuid, String>,
}

impl Withheld {
    /// What `errors` withhold. `sources` name the assets of withheld
    /// bundles.
    fn new<'a>(
        errors: &[NamespaceError],
        sources: impl IntoIterator<Item = &'a ScannedBundle>,
    ) -> Self {
        let mut withheld = Self::default();
        let mut bundle_errors = BTreeMap::new();
        for error in errors {
            match &error.detail {
                NamespaceErrorV1::DuplicateBundleUuid { bundle, .. } => {
                    withheld.bundles.insert(*bundle);
                    bundle_errors.insert(*bundle, error.message.clone());
                }
                NamespaceErrorV1::DuplicateAssetUuid { asset, .. } => {
                    withheld.assets.insert(*asset, error.message.clone());
                }
                _ => {}
            }
        }
        for source in sources {
            let uuids = match (&source.parsed, &source.namespace_skeleton) {
                (Ok(bundle), _) => bundle_errors
                    .get(&bundle.uuid)
                    .map(|message| (message, bundle.assets.values().map(|entry| entry.uuid).collect::<Vec<_>>())),
                (Err(_), Some(skeleton)) => bundle_errors
                    .get(&skeleton.uuid)
                    .map(|message| (message, skeleton.assets.values().map(|entry| entry.uuid).collect())),
                (Err(_), None) => None,
            };
            if let Some((message, uuids)) = uuids {
                for uuid in uuids {
                    withheld
                        .assets
                        .entry(uuid)
                        .or_insert_with(|| message.clone());
                }
            }
        }
        withheld
    }

    fn asset(&self, asset: &AssetUuid) -> bool {
        self.assets.contains_key(asset)
    }

    /// `source` as it publishes: `None` when its bundle is withheld, else
    /// without its withheld assets (a withheld primary leaves no primary).
    fn publishable(&self, source: &Arc<ScannedBundle>) -> Option<Arc<ScannedBundle>> {
        let Ok(bundle) = &source.parsed else {
            return Some(Arc::clone(source));
        };
        if self.bundles.contains(&bundle.uuid) {
            return None;
        }
        if !bundle.assets.values().any(|entry| self.asset(&entry.uuid)) {
            return Some(Arc::clone(source));
        }
        let mut bundle = bundle.clone();
        bundle.assets.retain(|_, entry| !self.asset(&entry.uuid));
        if bundle
            .primary
            .as_ref()
            .is_some_and(|primary| !bundle.assets.contains_key(primary))
        {
            bundle.primary = None;
        }
        Some(Arc::new(ScannedBundle {
            parsed: Ok(bundle),
            ..ScannedBundle::clone(source)
        }))
    }

    /// `poison` without its withheld entries; `None` when its bundle is
    /// withheld.
    fn publishable_skeleton(&self, mut poison: ScopedBundlePoison) -> Option<ScopedBundlePoison> {
        if self.bundles.contains(&poison.bundle) {
            return None;
        }
        poison.entries.retain(|entry| !self.asset(&entry.asset));
        Some(poison)
    }

    /// The withheld assets whose stored resolution is not yet their
    /// error: each publishes `Failed` once.
    fn newly_failed(&self, reader: &StoreReader) -> Result<BTreeMap<AssetUuid, String>, StoreError> {
        let mut failed = BTreeMap::new();
        for (asset, message) in &self.assets {
            let current = reader.asset_resolution(*asset)?;
            if !matches!(&current, Some(ResolutionRow::Failed(error)) if error == message) {
                failed.insert(*asset, message.clone());
            }
        }
        Ok(failed)
    }
}

/// What `source` claims under `projection` (see [`distill_store::claims`]).
fn source_claims(
    source: &ScannedBundle,
    projection: &PipelineProjection,
    authority: Option<&ProjectSchemaAuthority>,
) -> Result<SourceClaims, CoordinatorError> {
    let readable = readable_source(source);
    let mut claims = Vec::new();
    let authored_claims =
        |claims: &mut Vec<SourceClaim>, bundle: BundleUuid, local_id: &str, asset, type_uuid| {
            claims.push(SourceClaim::Authored {
                asset,
                claimant: AssetClaimant::Authored {
                    source: readable.clone(),
                    bundle,
                    local_id: local_id.to_owned(),
                },
            });
            for (child, output) in projection.derived_outputs([(asset, type_uuid)]) {
                claims.push(SourceClaim::DerivedOutput {
                    child,
                    output: DerivedOutputClaim {
                        parent: output.parent,
                        output_key: output.output_key,
                        terminal_type: output.terminal_type,
                    },
                });
            }
        };
    match &source.parsed {
        Err(error) => {
            if let Some(scoped_poison) = scoped_bundle_poison(source, authority) {
                claims.push(SourceClaim::Bundle {
                    bundle: scoped_poison.bundle,
                    source: readable.clone(),
                });
                for entry in &scoped_poison.entries {
                    authored_claims(
                        &mut claims,
                        scoped_poison.bundle,
                        &entry.local_id,
                        entry.asset,
                        entry.type_uuid,
                    );
                }
            } else {
                let namespace_error = NamespaceError::new(
                    NamespaceErrorV1::IncompleteSkeleton {
                        source: readable.clone(),
                        failure: skeleton_failure(error),
                    },
                    error.to_string(),
                )
                .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
                claims.push(SourceClaim::Malformed(namespace_error));
            }
        }
        Ok(bundle) => {
            crate::importer::decoded_directory_origin(bundle).map_err(|error| {
                CoordinatorError::InvalidManifest(format!(
                    "invalid import record in {}: {error:?}",
                    source.normalized_path
                ))
            })?;
            claims.push(SourceClaim::Bundle {
                bundle: bundle.uuid,
                source: readable.clone(),
            });
            for (local_id, entry) in &bundle.assets {
                authored_claims(&mut claims, bundle.uuid, local_id, entry.uuid, entry.type_uuid);
            }
            if let Some(primary) = &bundle.primary {
                claims.push(SourceClaim::PrimaryPath {
                    path: source.normalized_path.clone(),
                    asset: bundle.assets[primary].uuid,
                });
            }
        }
    }
    Ok(SourceClaims {
        root_name: source.root_name.clone(),
        path: source.normalized_path.clone(),
        claims,
    })
}

/// The claims of every bundle `sources` yields.
fn bundle_claims<'a>(
    sources: impl IntoIterator<Item = &'a ScannedBundle>,
    projection: &PipelineProjection,
    authority: Option<&ProjectSchemaAuthority>,
) -> Result<Vec<SourceClaims>, CoordinatorError> {
    sources
        .into_iter()
        .map(|source| source_claims(source, projection, authority))
        .collect()
}

/// What the incremental plan reads besides the claims.
struct PlanInputs<'a> {
    configuration_error: Option<ConfigurationError>,
    /// Namespace errors outside the claims (a pending scan rejection's).
    namespace_errors: Vec<NamespaceError>,
    authority: Option<&'a ProjectSchemaAuthority>,
    /// The bundles this scan read, by (root, path); other claimed sources
    /// are parsed from their stored bytes.
    fresh: BTreeMap<ScanKey, Arc<ScannedBundle>>,
}

fn fresh_bundles(delta: &ScanDelta) -> BTreeMap<ScanKey, Arc<ScannedBundle>> {
    delta
        .observed_bundle_entries()
        .map(|(key, source)| (key.clone(), Arc::clone(source)))
        .collect()
}

fn claimed_source(
    reader: &StoreReader,
    fresh: &BTreeMap<ScanKey, Arc<ScannedBundle>>,
    root_name: &str,
    path: &str,
) -> Result<Arc<ScannedBundle>, StoreError> {
    if let Some(source) = fresh.get(&(root_name.to_owned(), path.to_owned())) {
        return Ok(Arc::clone(source));
    }
    let bytes = reader
        .bundle_file(root_name, path)?
        .ok_or_else(|| StoreError::InvalidConfiguration {
            error: format!("claimed bundle {root_name}:{path} has no stored bytes"),
        })?;
    Ok(Arc::new(crate::scanner::scanned_bundle(root_name, path, bytes)))
}

fn incremental_plan(
    reader: &StoreReader,
    inputs: &PlanInputs<'_>,
) -> Result<IncrementalScanPlan, CoordinatorError> {
    let namespace_errors = NamespaceError::canonical_set(
        reader
            .claims_namespace_errors()?
            .into_iter()
            .chain(inputs.namespace_errors.iter().cloned()),
    )
    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
    let configuration = inputs
        .configuration_error
        .clone()
        .map_or(ConfigurationStatus::Ready, ConfigurationStatus::Failed);
    let pending = reader.pending_claims()?;
    // Every source of each pending bundle: a colliding bundle withholds the
    // assets of all of them.
    let mut claimed = BTreeMap::new();
    for bundle in pending.bundles {
        let sources = reader
            .bundle_claim_sources(bundle)?
            .iter()
            .map(|source| {
                claimed_source(
                    reader,
                    &inputs.fresh,
                    &source.root_name,
                    &source.normalized_path,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        claimed.insert(bundle, sources);
    }
    let withheld = Withheld::new(
        &namespace_errors,
        claimed.values().flatten().map(AsRef::as_ref),
    );
    let mut bundles = BTreeMap::new();
    let mut bundle_poisons = BTreeMap::new();
    for (bundle, sources) in claimed {
        let source = match sources.as_slice() {
            [source] => withheld.publishable(source),
            _ => None,
        };
        if let Some(poison) = source
            .as_deref()
            .and_then(|source| scoped_bundle_poison(source, inputs.authority))
            .and_then(|poison| withheld.publishable_skeleton(poison))
        {
            bundle_poisons.insert(bundle, poison);
        }
        bundles.insert(bundle, source);
    }
    let mut derived_outputs = BTreeMap::new();
    for child in pending.derived {
        let output = match reader.derived_output_claims(child)?.as_slice() {
            [output] if !withheld.asset(&child) && !withheld.asset(&output.parent) => Some(DerivedOutputEntry {
                parent: output.parent,
                output_key: output.output_key.clone(),
                terminal_type: output.terminal_type,
            }),
            _ => None,
        };
        derived_outputs.insert(child, output);
    }
    let paths = pending
        .paths
        .into_iter()
        .map(|path| {
            let mut assets = reader.path_claims(&path)?;
            assets.retain(|asset| !withheld.asset(asset));
            Ok((path, assets))
        })
        .collect::<Result<BTreeMap<_, _>, StoreError>>()?;
    Ok(IncrementalScanPlan {
        namespace_errors,
        withheld,
        configuration,
        bundles,
        bundle_poisons,
        derived_outputs,
        paths,
    })
}

struct IncrementalScanPlan {
    namespace_errors: Vec<NamespaceError>,
    withheld: Withheld,
    configuration: ConfigurationStatus,
    bundles: BTreeMap<BundleUuid, Option<Arc<ScannedBundle>>>,
    bundle_poisons: BTreeMap<BundleUuid, ScopedBundlePoison>,
    derived_outputs: BTreeMap<AssetUuid, Option<DerivedOutputEntry>>,
    paths: BTreeMap<String, BTreeSet<AssetUuid>>,
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
struct ScanCandidate {
    scan: ScanSnapshot,
    renames: Vec<LogicalRename>,
    namespace_errors: Vec<NamespaceError>,
    bundle_poisons: BTreeMap<BundleUuid, ScopedBundlePoison>,
    configuration: ConfigurationStatus,
}

impl ScanCandidate {
    fn build(
        scan: ScanSnapshot,
        external_error: Option<ConfigurationError>,
        authority: Option<&ProjectSchemaAuthority>,
    ) -> Result<Self, CoordinatorError> {
        let mut errors = Vec::new();
        let mut bundle_poisons = BTreeMap::new();
        for bundle in scan.bundle_rows() {
            if let Err(error) = &bundle.parsed {
                if let Some(scoped) = scoped_bundle_poison(bundle, authority) {
                    bundle_poisons.insert(scoped.bundle, scoped);
                    continue;
                }
                let source = readable_source(bundle);
                let detail = NamespaceErrorV1::IncompleteSkeleton {
                    source,
                    failure: skeleton_failure(error),
                };
                errors.push(
                    NamespaceError::new(detail, error.to_string())
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
                errors.push(
                    NamespaceError::new(
                        NamespaceErrorV1::DuplicateBundleUuid { bundle, sources },
                        format!("duplicate bundle UUID {bundle}"),
                    )
                    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?,
                );
            }
        }
        for (asset, mut claimants) in assets {
            if claimants.len() > 1 {
                claimants.sort();
                errors.push(
                    NamespaceError::new(
                        NamespaceErrorV1::DuplicateAssetUuid { asset, claimants },
                        format!("duplicate asset UUID {asset}"),
                    )
                    .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?,
                );
            }
        }
        let namespace_errors = NamespaceError::canonical_set(errors)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;

        let configuration =
            external_error.map_or(ConfigurationStatus::Ready, ConfigurationStatus::Failed);
        Ok(Self {
            scan,
            renames: Vec::new(),
            namespace_errors,
            bundle_poisons,
            configuration,
        })
    }
}

fn projected_derived_outputs(
    candidate: &ScanCandidate,
    projection: &PipelineProjection,
) -> Result<
    (
        BTreeMap<AssetUuid, DerivedOutputEntry>,
        Vec<NamespaceError>,
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
                        // The claimant table below publishes the collision,
                        // which withholds the child.
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

    let mut errors = Vec::new();
    for (asset, mut claimants) in claims {
        claimants.sort();
        claimants.dedup();
        if claimants.len() > 1 {
            errors.push(
                NamespaceError::new(
                    NamespaceErrorV1::DuplicateAssetUuid { asset, claimants },
                    format!("duplicate asset UUID {asset}"),
                )
                .map_err(|error| StoreError::InvalidConfiguration {
                    error: format!("invalid derived-output collision: {error}"),
                })?,
            );
        }
    }
    Ok((outputs, errors))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BundleSummary {
    root_name: String,
    path: String,
    format_version: u32,
    content_hash: ContentHash,
    origin: Option<distill_store::bundles::DirectoryOrigin>,
}

impl BundleSummary {
    fn rpc_equivalent(&self, other: &Self) -> bool {
        self.path == other.path
            && self.format_version == other.format_version
            && self.content_hash == other.content_hash
    }
}

fn candidate_bundle_summaries(
    published: &[Arc<ScannedBundle>],
    bundle_poisons: &BTreeMap<BundleUuid, ScopedBundlePoison>,
) -> Result<BTreeMap<BundleUuid, BundleSummary>, StoreError> {
    let mut summaries = BTreeMap::new();
    for source in published {
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
    for poison in bundle_poisons.values() {
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

/// The assets each published bundle holds.
fn published_bundle_assets(
    published: &[Arc<ScannedBundle>],
    bundle_poisons: &BTreeMap<BundleUuid, ScopedBundlePoison>,
) -> BTreeMap<BundleUuid, BTreeSet<AssetUuid>> {
    let mut assets = BTreeMap::<BundleUuid, BTreeSet<AssetUuid>>::new();
    for source in published {
        if let Ok(bundle) = &source.parsed {
            assets
                .entry(bundle.uuid)
                .or_default()
                .extend(bundle.assets.values().map(|entry| entry.uuid));
        }
    }
    for poison in bundle_poisons.values() {
        assets
            .entry(poison.bundle)
            .or_default()
            .extend(poison.entries.iter().map(|entry| entry.asset));
    }
    assets
}

#[allow(clippy::too_many_arguments)] // The scan transaction receives each publication input explicitly.
fn publish_scan(
    store: &Arc<AuthorityStore>,
    base: InputVersion,
    mut candidate: ScanCandidate,
    advance_configuration: bool,
    pipeline: Option<&ConfigurationPipelinePublication>,
    projection: &PipelineProjection,
    tag_epoch: [u8; 32],
    claims: &[SourceClaims],
) -> Result<Commit, StoreError> {
    let (derived_outputs, derived_errors) = projected_derived_outputs(&candidate, projection)?;
    candidate.namespace_errors = NamespaceError::canonical_set(
        std::mem::take(&mut candidate.namespace_errors)
            .into_iter()
            .chain(derived_errors),
    )
    .map_err(|error| StoreError::InvalidConfiguration {
        error: format!("invalid namespace error: {error}"),
    })?;
    let withheld = Withheld::new(&candidate.namespace_errors, candidate.scan.bundle_rows());
    let derived_outputs = derived_outputs
        .into_iter()
        .filter(|(child, output)| !withheld.asset(child) && !withheld.asset(&output.parent))
        .collect::<BTreeMap<_, _>>();
    let published = candidate
        .scan
        .bundles
        .values()
        .filter_map(|source| withheld.publishable(source))
        .collect::<Vec<_>>();
    candidate.bundle_poisons = std::mem::take(&mut candidate.bundle_poisons)
        .into_iter()
        .filter_map(|(bundle, poison)| {
            withheld
                .publishable_skeleton(poison)
                .map(|poison| (bundle, poison))
        })
        .collect();
    let mut store = store.write();
    if store.input_version() != base {
        return Err(StoreError::InvalidConfiguration {
            error: format!(
                "durable scan basis is {:?}, expected {base:?}",
                store.input_version()
            ),
        });
    }
    let old_files = store
        .observed_files()?
        .into_iter()
        .map(|row| ((row.root_name, row.path), row.file))
        .collect::<BTreeMap<_, _>>();
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
    let current_bundle_summaries = candidate_bundle_summaries(&published, &candidate.bundle_poisons)?;
    // A bundle whose assets changed without its bytes (an asset started or
    // stopped colliding) republishes too.
    let mut old_bundle_assets = BTreeMap::<BundleUuid, BTreeSet<AssetUuid>>::new();
    for (asset, bundle) in &old_asset_bundles {
        old_bundle_assets.entry(*bundle).or_default().insert(*asset);
    }
    let current_bundle_assets = published_bundle_assets(&published, &candidate.bundle_poisons);
    let assets_changed =
        |bundle: &BundleUuid| old_bundle_assets.get(bundle) != current_bundle_assets.get(bundle);
    let changed_bundles = current_bundle_summaries
        .iter()
        .filter_map(|(bundle, current)| {
            (old_bundle_summaries.get(bundle) != Some(current) || assets_changed(bundle))
                .then_some(*bundle)
        })
        .collect::<BTreeSet<_>>();
    let rpc_changed_bundles = current_bundle_summaries
        .iter()
        .filter_map(|(bundle, current)| {
            (old_bundle_summaries
                .get(bundle)
                .is_none_or(|old| !current.rpc_equivalent(old))
                || assets_changed(bundle))
            .then_some(*bundle)
        })
        .collect::<BTreeSet<_>>();
    let newly_failed = withheld.newly_failed(&store)?;
    let mut generation = match store.configuration_state()? {
        ConfigurationState::Ready(epoch) => epoch.generation,
        ConfigurationState::Failed { last_good, .. } => {
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
    let publishable_changed_bundles = changed_bundles;
    let rpc_publishable_bundles = rpc_changed_bundles;
    let mut commit = rpc_commit(
        &candidate,
        &published,
        &withheld,
        &newly_failed,
        &old_asset_bundles,
        &old_paths,
        projection,
        derived_outputs.clone(),
        &rpc_publishable_bundles,
    )?;
    let mut next_pipeline = pipeline_diagnostic(store.pipeline_state()?);
    store.input_transaction(|transaction| {
        transaction.replace_source_claims(None, claims)?;
        let mut root_ids = BTreeMap::new();
        let mut newest_mtime = 0;
        for (key, file) in candidate.scan.file_observations() {
            let root = *root_ids
                .entry(key.0.clone())
                .or_insert(transaction.intern_root(&key.0)?);
            newest_mtime = newest_mtime.max(file.state.mtime);
            let old = old_files.get(key);
            if old == Some(&file) {
                continue;
            }
            transaction.upsert_file(root, &key.1, &file, observation)?;
            if let Some(bundle) = candidate.scan.bundles.get(key) {
                transaction.set_bundle_file(root, &key.1, &bundle.bytes)?;
            }
            if old.is_none_or(|old| old.state != file.state) {
                transaction.push_dirty(root, &key.1, true, observation)?;
            }
        }
        for (root_name, path) in old_files.keys() {
            if !candidate
                .scan
                .files
                .contains_key(&(root_name.clone(), path.clone()))
            {
                let root = transaction.intern_root(root_name)?;
                transaction.remove_file(root, path)?;
                transaction.push_dirty(root, path, false, observation)?;
            }
        }
        transaction.replace_scan_structure(
            None,
            &candidate.scan.directory_rows(),
            &candidate.scan.encoded_diagnostic_rows(),
        )?;
        transaction.set_clean_watermark(newest_mtime)?;
        transaction.set_namespace_errors(candidate.namespace_errors.iter().cloned())?;
        transaction.clear_derived_outputs()?;
        for (child, output) in &derived_outputs {
            transaction.set_derived_output(
                *child,
                output.parent,
                &output.output_key,
                output.terminal_type,
            )?;
        }

        match &candidate.configuration {
            ConfigurationStatus::Ready => transaction.publish_configuration_ready(generation)?,
            ConfigurationStatus::Failed(error) => {
                transaction.publish_configuration_error(&error.detail, &error.message)?
            }
        }
        match pipeline {
            Some(ConfigurationPipelinePublication::Epoch { epoch, tools }) => {
                next_pipeline = PipelineDiagnostic::Ready;
                transaction.publish_pipeline_epoch(epoch)?;
                transaction.publish_tool_epoch(tools)?;
            }
            Some(ConfigurationPipelinePublication::Failed(failure)) => {
                next_pipeline = PipelineDiagnostic::Failed(failure.clone());
                transaction.publish_pipeline_failure(failure)?;
            }
            None => {}
        }

        for bundle in old_bundle_summaries.keys() {
            if !current_bundle_summaries.contains_key(bundle)
                || publishable_changed_bundles.contains(bundle)
            {
                transaction.remove_bundle(*bundle)?;
            }
        }
        for source in &published {
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
                    served: Some(served_authoring(entry, projection)?),
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
        for rename in &candidate.renames {
            let root = *root_ids
                .entry(rename.root_name.clone())
                .or_insert(transaction.intern_root(&rename.root_name)?);
            transaction.push_rename(root, &rename.from_path, &rename.to_path)?;
        }
        Ok(())
    })?;
    commit.pipeline = Some(next_pipeline);
    Ok(commit)
}

#[derive(Debug)]
struct IncrementalFileMutation {
    root_name: String,
    path: String,
    /// The new row; `None` removes it.
    file: Option<FileObservation>,
    /// The bytes of a new or changed `.bundle` row.
    bundle: Option<Arc<ScannedBundle>>,
    /// Whether the tree state changed (watcher work for importers).
    dirty: bool,
}

#[derive(Debug)]
struct DurableBundleBasis {
    summary: Option<BundleSummary>,
    assets: BTreeSet<AssetUuid>,
}

fn append_path_mutations(
    commit: &mut Commit,
    current_paths: &BTreeMap<String, BTreeSet<AssetUuid>>,
    old_paths: &BTreeMap<String, BTreeSet<AssetUuid>>,
) {
    for (path, current) in current_paths {
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

/// Publish one incremental scan in one input transaction: its file rows and
/// claims are written first, and the plan is read back from them, so a
/// failed publication rolls both back.
#[allow(clippy::too_many_arguments)] // The transaction receives each independently pinned publication authority.
fn publish_incremental_scan(
    store: &Arc<AuthorityStore>,
    base: InputVersion,
    baseline: &ScanSnapshot,
    delta: &ScanDelta,
    claims: &[SourceClaims],
    inputs: &PlanInputs<'_>,
    renames: &[LogicalRename],
    projection: &PipelineProjection,
    tag_epoch: [u8; 32],
) -> Result<Commit, StoreError> {
    let file_mutations = incremental_file_mutations(baseline, delta);
    let mut store = store.write();
    if store.input_version() != base {
        return Err(StoreError::InvalidConfiguration {
            error: format!(
                "durable incremental-scan basis is {:?}, expected {base:?}",
                store.input_version()
            ),
        });
    }
    let observation =
        InputVersion(
            base.0
                .checked_add(1)
                .ok_or_else(|| StoreError::InvalidConfiguration {
                    error: "input version exhausted".to_owned(),
                })?,
        );
    let (commit, _) = store.input_transaction(|transaction| {
        let watermark = transaction.reader().clean_watermark()?.unwrap_or(0);
        let mut root_ids = BTreeMap::new();
        let mut newest_mtime = watermark;
        for mutation in &file_mutations {
            let root = *root_ids
                .entry(mutation.root_name.clone())
                .or_insert(transaction.intern_root(&mutation.root_name)?);
            match &mutation.file {
                Some(file) => {
                    transaction.upsert_file(root, &mutation.path, file, observation)?;
                    if let Some(bundle) = &mutation.bundle {
                        transaction.set_bundle_file(root, &mutation.path, &bundle.bytes)?;
                    }
                    if mutation.dirty {
                        transaction.push_dirty(root, &mutation.path, true, observation)?;
                    }
                    newest_mtime = newest_mtime.max(file.state.mtime);
                }
                None => {
                    transaction.remove_file(root, &mutation.path)?;
                    transaction.push_dirty(root, &mutation.path, false, observation)?;
                }
            }
        }
        transaction.replace_scan_structure(
            Some(delta.affected_prefixes()),
            &delta.observed().directory_rows(),
            &delta.observed().encoded_diagnostic_rows(),
        )?;
        transaction.set_clean_watermark(newest_mtime)?;
        transaction.replace_source_claims(Some(delta.affected_prefixes()), claims)?;
        let IncrementalPublication {
            plan,
            commit,
            changed_bundles,
            configuration_generation,
        } = prepare_incremental_publication(
            &transaction.reader(),
            inputs,
            projection,
        )?;
        transaction.set_namespace_errors(plan.namespace_errors.iter().cloned())?;
        match &plan.configuration {
            ConfigurationStatus::Ready => {
                transaction.publish_configuration_ready(configuration_generation)?;
            }
            ConfigurationStatus::Failed(error) => {
                transaction.publish_configuration_error(&error.detail, &error.message)?;
            }
        }
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
                    served: Some(served_authoring(entry, projection)?),
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
                    transaction.set_derived_output(
                        *child,
                        entry.parent,
                        &entry.output_key,
                        entry.terminal_type,
                    )?
                }
                None => {
                    transaction.remove_derived_output(*child)?;
                }
            }
        }
        for rename in renames {
            let root = *root_ids
                .entry(rename.root_name.clone())
                .or_insert(transaction.intern_root(&rename.root_name)?);
            transaction.push_rename(root, &rename.from_path, &rename.to_path)?;
        }
        transaction.clear_pending_claims()?;
        Ok(commit)
    })?;
    Ok(commit)
}

struct IncrementalPublication {
    plan: IncrementalScanPlan,
    commit: Commit,
    changed_bundles: BTreeSet<BundleUuid>,
    configuration_generation: u64,
}

/// Plan an incremental publication from the transaction's claims and read
/// the published rows it replaces.
fn prepare_incremental_publication(
    store: &StoreReader,
    inputs: &PlanInputs<'_>,
    projection: &PipelineProjection,
) -> Result<IncrementalPublication, StoreError> {
    let plan = incremental_plan(store, inputs).map_err(|error| StoreError::InvalidConfiguration {
        error: error.to_string(),
    })?;
    let configuration_generation = match store.configuration_state()? {
        ConfigurationState::Ready(epoch) => epoch.generation,
        ConfigurationState::Failed { last_good, .. } => {
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
    let mut commit = Commit {
        configuration: Some(plan.configuration.clone()),
        pipeline: Some(pipeline_diagnostic(stored_pipeline)),
        namespace_errors: Some(plan.namespace_errors.clone()),
        tag_poisons: Some(BTreeMap::new()),
        ..Commit::default()
    };
    let path_projections = &plan.paths;
    let old_paths = path_projections
        .keys()
        .map(|path| Ok((path.clone(), store.path_assets(path)?)))
        .collect::<Result<BTreeMap<_, _>, StoreError>>()?;
    let mut changed_bundles = BTreeSet::new();
    for (bundle_uuid, source) in &plan.bundles {
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
        let published_assets = match (plan.bundle_poisons.get(bundle_uuid), source) {
            (Some(poison), _) => poison.entries.iter().map(|entry| entry.asset).collect(),
            (None, Some(source)) => source
                .parsed
                .as_ref()
                .map(|bundle| bundle.assets.values().map(|entry| entry.uuid).collect())
                .unwrap_or_default(),
            (None, None) => BTreeSet::new(),
        };
        if old.summary == current_summary && old.assets == published_assets {
            continue;
        }
        changed_bundles.insert(*bundle_uuid);
        // A withheld asset is not deleted: it resolves to its error.
        let mut current_assets = old
            .assets
            .iter()
            .filter(|asset| plan.withheld.asset(asset))
            .copied()
            .collect::<BTreeSet<_>>();
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
    append_path_mutations(&mut commit, path_projections, &old_paths);
    for (child, current) in &plan.derived_outputs {
        let old = old_derived[child].as_ref();
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
    for (asset, error) in plan.withheld.newly_failed(store)? {
        commit.assets.push(AssetMutation::Set {
            uuid: asset,
            resolution: StoredResolve::Failed { error },
            delta: AssetDeltaState::Changed,
        });
        if store.served_entry_meta(asset)?.is_some() {
            commit
                .authoring
                .push(AuthoringMutation::Remove { uuid: asset });
        }
    }
    Ok(IncrementalPublication {
        plan,
        commit,
        changed_bundles,
        configuration_generation,
    })
}

fn incremental_file_mutations(
    baseline: &ScanSnapshot,
    delta: &ScanDelta,
) -> Vec<IncrementalFileMutation> {
    let old = baseline
        .file_observations()
        .filter(|(key, _)| {
            delta
                .affected_prefixes()
                .iter()
                .any(|prefix| scan_key_matches(prefix, key))
        })
        .map(|(key, file)| (key.clone(), file))
        .collect::<BTreeMap<_, _>>();
    let observed = delta.observed();
    let current = observed
        .file_observations()
        .map(|(key, file)| (key.clone(), file))
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
                dirty: before.map(|file| &file.state) != after.map(|file| &file.state),
                bundle: after.and_then(|_| observed.bundles.get(&key).cloned()),
                file: after.cloned(),
                root_name: key.0,
                path: key.1,
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
        Some(StoredPipelineState::Failed { error, .. }) => PipelineDiagnostic::Failed(error),
    }
}

/// Reobserve authored paths and advance the durable projection, on the
/// authority.
#[allow(clippy::too_many_arguments)] // The authoring boundary passes each publication authority explicitly.
pub(crate) fn publish_incremental_paths(
    scanner: &RootedScanner,
    paths: &[PathBuf],
    store: &Arc<AuthorityStore>,
    base: InputVersion,
    projection: &PipelineProjection,
    coordinator: Option<&DaemonCoordinator>,
) -> Result<Commit, String> {
    let (delta, baseline) = {
        let store = store.read();
        let stored = StoredBaseline::new(&store);
        let delta = scanner.scan_incremental_delta(&stored, paths);
        stored.finish().map_err(|error| error.to_string())?;
        let delta = delta
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "authored path is outside every configured root".to_owned())?;
        let baseline = ScanSnapshot::load_under(&store, delta.affected_prefixes())
            .map_err(|error| error.to_string())?;
        (delta, baseline)
    };
    let authority = coordinator.and_then(DaemonCoordinator::schema_authority);
    let tag_epoch = authority
        .as_ref()
        .map_or([0; 32], |authority| authority.source_hash());

    if let Some(coordinator) = coordinator {
        let claims = bundle_claims(
            delta
                .observed_bundle_entries()
                .map(|(_, source)| source.as_ref()),
            projection,
            authority.as_deref(),
        )
        .map_err(|error| error.to_string())?;
        let inputs = PlanInputs {
            configuration_error: coordinator.configuration_error(),
            namespace_errors: Vec::new(),
            authority: authority.as_deref(),
            fresh: fresh_bundles(&delta),
        };
        let mut commit = publish_incremental_scan(
            store,
            base,
            &baseline,
            &delta,
            &claims,
            &inputs,
            &[],
            projection,
            tag_epoch,
        )
        .map_err(|error| error.to_string())?;
        if let Some(authority) = authority.clone() {
            let affected = commit_affected_asset_bundles(&commit);
            if !affected.is_empty() {
                let targets = coordinator
                    .build_targets.load_full();
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
        return Ok(commit);
    }

    drop(baseline);
    let mut scan = ScanSnapshot::load(&store.read()).map_err(|error| error.to_string())?;
    scan.apply_delta(delta);
    let claims = bundle_claims(scan.bundle_rows(), projection, authority.as_deref())
        .map_err(|error| error.to_string())?;
    let candidate = ScanCandidate::build(scan, None, authority.as_deref())
    .map_err(|error| error.to_string())?;
    let commit = publish_scan(
        store,
        base,
        candidate,
        false,
        None,
        projection,
        tag_epoch,
        &claims,
    )
    .map_err(|error| error.to_string())?;
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

#[allow(clippy::too_many_arguments)]
fn rpc_commit(
    candidate: &ScanCandidate,
    published: &[Arc<ScannedBundle>],
    withheld: &Withheld,
    newly_failed: &BTreeMap<AssetUuid, String>,
    old_asset_bundles: &BTreeMap<AssetUuid, BundleUuid>,
    old_paths: &[(String, distill_store::files::RootId, AssetUuid)],
    projection: &PipelineProjection,
    derived_outputs: BTreeMap<AssetUuid, DerivedOutputEntry>,
    changed_bundles: &BTreeSet<BundleUuid>,
) -> Result<Commit, StoreError> {
    let mut commit = Commit {
        configuration: Some(candidate.configuration.clone()),
        pipeline: Some(PipelineDiagnostic::Ready),
        namespace_errors: Some(candidate.namespace_errors.clone()),
        derived_outputs: Some(derived_outputs),
        tag_poisons: Some(BTreeMap::new()),
        ..Commit::default()
    };

    // A withheld asset is not deleted: it resolves to its error.
    let mut current_assets = withheld
        .assets
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    for (asset, error) in newly_failed {
        commit.assets.push(AssetMutation::Set {
            uuid: *asset,
            resolution: StoredResolve::Failed {
                error: error.clone(),
            },
            delta: AssetDeltaState::Changed,
        });
        if old_asset_bundles.contains_key(asset) {
            commit
                .authoring
                .push(AuthoringMutation::Remove { uuid: *asset });
        }
    }
    let mut paths = BTreeMap::<String, BTreeSet<AssetUuid>>::new();
    for source in published {
        let Ok(bundle) = &source.parsed else {
            continue;
        };
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

/// The served authored value and terminal type an asset row carries (LOCKLESS.md
/// §2.2): the same bytes the RPC authoring entry serves.
fn served_authoring(
    entry: &AssetEntry,
    projection: &PipelineProjection,
) -> Result<ServedAuthoring, StoreError> {
    let value = split_authoring_value(&entry.data)?;
    let blobs = value.blobs.iter().map(|blob| &blob[..]).collect::<Vec<_>>();
    Ok(ServedAuthoring {
        authored_value: encode_authored_value(&value.canonical_value, &blobs),
        terminal_type: projection.interface(entry.type_uuid).terminal,
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

fn readable_source(source: &crate::scanner::ScannedBundle) -> ReadableBundleSource {
    ReadableBundleSource {
        root_name: source.root_name.clone(),
        normalized_path: source.normalized_path.clone(),
        file_hash: source.file_hash,
    }
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


/// The daemon side of the RPC server's store: publications run on the
/// authority, writes go through the shared store mutex.
struct DaemonStore {
    store: Arc<AuthorityStore>,
    authority: AuthoritySender,
}

impl distill_rpc::ExternalStore for DaemonStore {
    fn on_authority(&self) -> bool {
        self.authority.on_authority()
    }

    fn execute(&self, job: distill_rpc::AuthorityJob) {
        self.authority.execute(job);
    }

    fn with_store(&self, job: &mut (dyn FnMut(&mut Store) + Send)) {
        self.store
            .write_with(|store| job(store))
            .expect("the authority runs while its store is served");
    }
}

/// The pipeline runtime, which only the authority touches, and the
/// snapshot of it that other threads read.
struct PipelineState {
    runtime: AuthorityCell<CoordinatedPipelineRuntime>,
    published: ArcSwap<PipelineSnapshot>,
}

impl PipelineState {
    fn new(runtime: CoordinatedPipelineRuntime, authority: &AuthoritySender) -> Self {
        Self {
            published: ArcSwap::from_pointee(runtime.host.snapshot()),
            runtime: AuthorityCell::new(runtime, authority),
        }
    }
}

/// The pipeline runtime, borrowed on the authority. Dropping it publishes
/// the host's snapshot.
struct PipelineGuard<'a> {
    runtime: AuthorityRef<'a, CoordinatedPipelineRuntime>,
    published: &'a ArcSwap<PipelineSnapshot>,
}

impl std::ops::Deref for PipelineGuard<'_> {
    type Target = CoordinatedPipelineRuntime;

    fn deref(&self) -> &CoordinatedPipelineRuntime {
        &self.runtime
    }
}

impl std::ops::DerefMut for PipelineGuard<'_> {
    fn deref_mut(&mut self) -> &mut CoordinatedPipelineRuntime {
        &mut self.runtime
    }
}

impl Drop for PipelineGuard<'_> {
    fn drop(&mut self) {
        self.published.store(Arc::new(self.runtime.host.snapshot()));
    }
}

fn lock_pipeline(pipeline: &PipelineState) -> PipelineGuard<'_> {
    PipelineGuard {
        runtime: pipeline.runtime.borrow_mut(),
        published: &pipeline.published,
    }
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
    fn prepared_candidate_error_cleanup_uses_the_explicit_unload_path() {
        let temp = tempfile::tempdir().unwrap();
        let mut runtime = CoordinatedPipelineRuntime {
            host: ModuleHost::new(temp.path().join("module-host")).unwrap(),
            loader: DynamicPipelineModuleLoader,
        };
        let mut prepared = Some(crate::epoch::empty_test_epoch());

        // Unloaded: a failed unload would poison and leak the epoch.
        assert_eq!(discard_prepared(&mut runtime, &mut prepared), None);
        assert!(prepared.is_none());
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
                vec![AssetRoot::new("main", &assets)],
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
                vec![AssetRoot::new("main", &assets)],
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
