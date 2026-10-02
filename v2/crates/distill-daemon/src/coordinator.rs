//! The daemon coordinator.
//!
//! Filesystem scans are candidates. A publication is one input
//! transaction, begun with SQLite's write lock, which rechecks the durable
//! store basis, applies the complete input, and commits the exact RPC
//! projection for the same successor version. Publications run on the
//! calling thread; the process loop runs the scan-driven ones, each pass
//! of which publishes exactly one input version (see `pass`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
#[cfg(test)]
use std::sync::mpsc;


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
    RpcFailure, NamespaceErrorV1,
};
use distill_schema::ProjectSchemaAuthority;
use distill_store::bundles::{
    AssetRecord, BundleMeta, NamespaceSkeleton as StoreNamespaceSkeleton, ServedAuthoring,
    SkeletonEntry,
};
use distill_store::claims::{DerivedOutputClaim, SourceClaim, SourceClaims};
use distill_store::config::{PendingRestart, RestartOnlyChange};
use distill_store::errors::ScanRejectionRecord;
use distill_store::files::{FileObservation, PendingFileWork};
use distill_store::pipeline::ValidatedPipelineEpoch;
use distill_store::served::{encode_authored_value, ResolutionRow};
use distill_store::state::{
    AssetClaimant, CleanupDisposition, ConfigurationState, DirectoryAliasSide, DscpV1,
    InputVersion, PipelineFailure, PipelineFailureCode, PipelineFailureOrigin,
    PipelineState as StoredPipelineState, ReadableBundleSource, ScanFailureCode, ScanSubject,
    SkeletonFailureCode,
};
use distill_store::{Store, StoreConfig, StoreError, StoreOpener, StoreReader, StoreWriter};

use crate::authoring::{AuthoringService, AuthoringServiceInitError};
use crate::callbacks::EpochAuthoringImporter;
use crate::compiled::{Compiled, CompiledLookupError, CompiledRegistry, StagedCompiled};
use crate::epoch::{
    stored_pipeline_epoch, CandidateRejection, CandidateRequirements, ModuleHost, PipelineEpoch,
    PipelineSnapshot,
};
use crate::module_loader::DynamicPipelineModuleLoader;
use crate::pipeline_map::PipelineProjection;
use crate::scanner::{
    AssetRoot, DaemonOwnedDirectoryKind, RootedScanner, ScanDelta, ScanDiagnostic, ScanError,
    ScanSnapshot, ScannedBundle, StoredBaseline,
};
use crate::scheduler::{ScheduledPool, Scheduler, SchedulerConfig, WorkClass};
use crate::watcher::{WatcherAction, WatcherBatch, WatcherQueue};

mod pass;
#[cfg(test)]
mod compiled_tests;

use pass::ImportScope;
pub use pass::PassOutcome;

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
    /// Opens each owner's own reader and writer; nothing here holds one.
    opener: Arc<StoreOpener>,
    /// The watcher's scanner: its roots follow the last committed
    /// configuration. Publications read the roots of their own compiled
    /// state ([`Compiled::scanner`]).
    scanner: RootedScanner,
    /// Set once this process has published a scan of its own.
    scan_initialized: OnceLock<()>,
    server: Arc<ServerHandle>,
    authoring: Arc<AuthoringService>,
    pipeline: PipelineState,
    /// The compiled configuration state of each live store version (see
    /// `crate::compiled`).
    compiled: Arc<CompiledRegistry>,
    /// The state before this process published any: what a publication
    /// that changes only the pipeline builds on when nothing is loaded.
    boot: Compiled,
    operational: ScheduledPool<crate::build::BuildOutcome>,
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

/// Every path that publishes or installs a pipeline failure reports it here:
/// a rejected candidate leaves its importers unregistered, which is otherwise
/// only visible to RPC clients.
fn warn_pipeline_failure(failure: &PipelineFailure, context: &str) {
    tracing::warn!(
        code = ?failure.code,
        origin = ?failure.origin,
        cleanup = ?failure.cleanup,
        message = %failure.message,
        "{context}"
    );
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
        // Recovery ran on the store that took the state lock; every owner
        // opens its own writer from here on.
        let compiled_version = opened_store.compiled_version()?;
        let (opener, recovered) = StoreOpener::new(opened_store);
        drop(recovered);
        // A store no publication compiled anything for serves the boot state
        // under its absent key. A store an earlier process compiled for has
        // no entry until this process publishes one (`loop_compiled`).
        let boot = Compiled::boot(None, scanner.candidate_with_roots(roots.clone())?, roots);
        let compiled = Arc::new(CompiledRegistry::new());
        if compiled_version.is_none() {
            compiled.install(boot.clone());
        }
        let backend = Arc::new(AuthoringService::new(Arc::clone(&opener), Arc::clone(&compiled)));
        let server = ServerHandle::open(backend.clone(), Arc::clone(&opener));
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
            Arc::clone(&opener),
        )
        .map_err(|error| CoordinatorInitError::Operational(error.to_string()))?;
        Ok(Self {
            opener,
            scanner,
            scan_initialized: OnceLock::new(),
            server,
            authoring: backend,
            pipeline: PipelineState::new(pipeline),
            compiled,
            boot,
            operational,
        })
    }

    /// Publish one coordinated step against `base` on `store`; `publish`
    /// runs inside its input, on the same writer.
    pub fn coordinated_commit(
        &self,
        store: &mut Store,
        base: InputVersion,
        publish: impl FnOnce(&mut Store) -> Result<Commit, String>,
    ) -> Result<SnapshotStamp, CoordinatedCommitError> {
        self.server.coordinated_commit(store, base, publish)
    }

    /// An RPC front end of the caller's own (reads, `root`, embedded-style
    /// admin calls on a writer of its own).
    pub fn server(&self) -> Server {
        Server::open(&self.server)
    }

    pub fn server_handle(&self) -> &Arc<ServerHandle> {
        &self.server
    }

    /// Attach this coordinator as the RPC server's production lazy-build
    /// authority after it has been placed in its final `Arc`.
    pub fn attach_build_backend(self: &Arc<Self>) {
        let backend = Arc::new(crate::build::CoordinatorBuildBackend::new(self));
        let server = self.server();
        server.install_build_backend(backend);
        self.authoring.attach_tag_index_coordinator(self);
    }

    pub fn opener(&self) -> &Arc<StoreOpener> {
        &self.opener
    }

    /// A writer of the caller's own on the daemon's store.
    pub fn open_writer(&self) -> Result<StoreWriter, StoreError> {
        self.opener.open_writer()
    }

    /// A reader of the caller's own on the daemon's store.
    pub fn open_reader(&self) -> Result<StoreReader, StoreError> {
        self.opener.open_reader()
    }

    pub fn scanner(&self) -> RootedScanner {
        self.scanner.clone()
    }

    /// Current non-fatal filesystem exclusions in canonical rooted-path
    /// order. These rows are also reported by `doctor verify`.
    pub fn scan_diagnostics(&self, store: &StoreReader) -> Result<Vec<ScanDiagnostic>, CoordinatorError> {
        ScanSnapshot::load_diagnostics(store)
            .map(|scan| scan.diagnostic_rows().cloned().collect())
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    /// The published observation, from the store's scan tables.
    fn published_scan(&self, store: &mut Store) -> Result<ScanSnapshot, CoordinatorError> {
        ScanSnapshot::load(store)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    pub fn authoring_service(&self) -> &Arc<AuthoringService> {
        &self.authoring
    }

    /// The compiled state of the version `reader`'s transaction sees (see
    /// `crate::compiled`). A version this process has no state for is an
    /// error, never another version's state.
    pub fn compiled_at(&self, reader: &StoreReader) -> Result<Arc<Compiled>, CompiledLookupError> {
        self.compiled.at(reader)
    }

    /// The compiled state at `store`'s version, for the process loop: when
    /// an earlier process compiled it, this one first publishes what it
    /// serves instead (no pipeline epoch loaded yet), so the store says what
    /// the process holds.
    fn loop_compiled(&self, store: &mut Store) -> Result<Arc<Compiled>, CoordinatorError> {
        match self.compiled.at(store) {
            Ok(compiled) => return Ok(compiled),
            Err(CompiledLookupError::NotLoaded { .. }) => {}
            Err(error) => return Err(CoordinatorError::Compiled(error)),
        }
        self.publish_pipeline_rejection(store, crate::epoch::unpublished_failure())?;
        self.compiled.at(store).map_err(CoordinatorError::Compiled)
    }

    /// Persist the first runtime failure latched by a published callback
    /// without minting a new input version. The in-memory epoch token fences
    /// work immediately; this closes the crash/restart durability side of the
    /// same monotonic transition.
    pub(crate) fn sync_runtime_pipeline_failure(
        &self,
        store: &mut Store,
    ) -> Result<Option<PipelineFailure>, CoordinatorError> {
        // Only a latched runtime failure of the epoch this writer's version
        // serves needs the runtime.
        let Ok(compiled) = self.compiled.at(store) else {
            return Ok(None);
        };
        match compiled.pipeline_epoch() {
            Err(failure) if failure.origin == PipelineFailureOrigin::PublishedRuntime => {}
            _ => return Ok(None),
        }
        drop(compiled);
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
        self.server
            .coordinated_runtime_pipeline_failure(store, diagnostic, |store| {
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
        warn_pipeline_failure(
            &observed.1,
            "published pipeline latched a runtime failure; importers from it are unavailable",
        );
        Ok(Some(observed.1))
    }

    /// Whether a Ready pipeline epoch serves.
    pub(crate) fn has_ready_pipeline(&self) -> bool {
        lock_pipeline(&self.pipeline).host.published_ready_epoch().is_some()
    }

    /// Whether publishing `candidate` with the current configuration and the
    /// pipeline module whose bytes hash to `dylib_hash` would install what
    /// already serves: the Ready epoch was staged from those bytes, and its
    /// version key (`ngp_module_host::ModuleReloadIdentity::version_key`,
    /// which uses the module's own source hash, plus the rest of the schema)
    /// is the same against `candidate` as against the installed schema.
    ///
    /// This is source-walk catching up after an `AheadOfWalk` adoption: it
    /// rewrites only the pipeline crate's source hash. It is also a pipeline
    /// event for bytes the epoch already serves: the pass that adopted them
    /// may have observed the file mid-replacement (cargo removes and
    /// re-links it) and staged it a moment later. Neither is republished, so
    /// the epoch, the RPC generation, and the imports stay.
    pub(crate) fn ready_pipeline_serves(
        &self,
        candidate: &ProjectSchemaAuthority,
        dylib_hash: [u8; 32],
    ) -> bool {
        let Some(installed) = self
            .compiled
            .latest()
            .and_then(|compiled| compiled.schema_authority())
        else {
            tracing::debug!("no installed schema to compare with");
            return false;
        };
        let Some(epoch) = lock_pipeline(&self.pipeline).host.published_ready_epoch() else {
            tracing::debug!("no Ready pipeline epoch");
            return false;
        };
        if epoch.runtime_failure().is_some() {
            return false;
        }
        if epoch.dylib_hash() != dylib_hash {
            tracing::debug!("the watched pipeline is not the Ready epoch's");
            return false;
        }
        let Some(identity) = epoch.reload_identity() else {
            tracing::debug!("Ready pipeline epoch has no reload identity");
            return false;
        };
        let key = |authority: &ProjectSchemaAuthority| {
            let schema = authority.schema();
            let mut rest = schema.clone();
            rest.source_hashes.clear();
            // `Schema` has no `PartialEq`; its derived `Debug` covers every
            // field, in order.
            (
                identity.version_key(&schema.source_hashes, &schema.layout_hashes),
                format!("{rest:?}"),
            )
        };
        let (installed, candidate) = (key(&installed), key(candidate));
        if installed != candidate {
            tracing::debug!(
                installed_key = %installed.0,
                candidate_key = %candidate.0,
                rest_equal = installed.1 == candidate.1,
                "schema changes what the Ready pipeline epoch would install"
            );
        }
        installed == candidate
    }

    /// Replace the compiled state the store's current version serves with
    /// `with` of it: tests install state no publication compiled.
    #[cfg(test)]
    fn replace_compiled_for_test(&self, with: impl FnOnce(&Compiled) -> Compiled) {
        let reader = self.open_reader().expect("test reader");
        let current = self.compiled.at(&reader).expect("test compiled state");
        self.compiled.replace_for_test(with(&current));
    }

    #[cfg(test)]
    pub(crate) fn install_schema_authority_for_test(&self, authority: Arc<ProjectSchemaAuthority>) {
        self.replace_compiled_for_test(|current| current.with_test_state(Some(authority), None, None));
    }

    #[cfg(test)]
    pub(crate) fn install_build_target_for_test(&self, name: &str, target: Target) {
        self.replace_compiled_for_test(|current| {
            current.with_test_state(None, Some((name, target)), None)
        });
    }

    #[cfg(test)]
    pub(crate) fn install_pipeline_epoch_for_test(&self, epoch: PipelineEpoch) {
        lock_pipeline(&self.pipeline).host.install_ready(epoch.clone());
        self.replace_compiled_for_test(|current| {
            current.with_test_state(None, None, Some(PipelineSnapshot::ready(epoch)))
        });
    }

    pub fn operational_configuration(&self) -> SchedulerConfig {
        self.operational.config()
    }

    /// Register interest in the build cell `key` (`crate::scheduler`): the
    /// cell in flight for it, or a new one running `job`. The ticket
    /// resolves to the cell's shared outcome; dropping it withdraws the
    /// interest.
    pub(crate) fn request_build(
        &self,
        key: crate::scheduler::CellKey,
        class: WorkClass,
        job: impl FnOnce(
                &mut Store,
                &crate::scheduler::CellWorker<crate::build::BuildOutcome>,
            ) -> crate::build::BuildOutcome
            + Send
            + 'static,
    ) -> crate::scheduler::CellTicket<crate::build::BuildOutcome> {
        self.operational.request(key, class, job)
    }

    /// How many build cells are queued or running.
    #[cfg(test)]
    pub(crate) fn build_cells_in_flight(&self) -> usize {
        self.operational.cells_in_flight()
    }

    /// Run `run` on a build worker, on the writer the scheduler lends the
    /// job, and wait for it. Test-only: builds are requested through their
    /// cells ([`Self::request_build`]), never waited on like this.
    #[cfg(test)]
    pub(crate) fn run_scheduled<R>(
        self: &Arc<Self>,
        class: WorkClass,
        run: impl FnOnce(&mut Store) -> R + Send + 'static,
    ) -> R
    where
        R: Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.operational.submit(class, move |store| {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(store)));
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
        self.opener
            .apply_operational_config(store_config)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        self.operational
            .reconfigure(scheduler, replacement_pool)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    pub fn stage_restart_configuration(
        &self,
        store: &mut Store,
        changes: &[RestartOnlyChange],
    ) -> Result<PendingRestart, CoordinatorError> {
        let pending = store
            .stage_pending_restart(changes)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        self.server.restart_required(store, pending.keys.clone());
        Ok(pending)
    }

    pub fn clear_restart_configuration(&self, store: &mut Store) -> Result<(), CoordinatorError> {
        store
            .clear_pending_restart()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        self.server.restart_required(store, Vec::new());
        Ok(())
    }

    /// Publish a rejected configuration source as an ordinary input version.
    /// The source's error is stored beside the pending scan rejection's, and
    /// every publication selects the configuration status from both, so a
    /// scan failure cannot erase the candidate's typed configuration reason.
    pub fn publish_configuration_rejection(
        &self,
        store: &mut Store,
        reason: DscpV1,
        message: impl Into<String>,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let error = ConfigurationError::from_reason(&reason, message.into());
        self.publish_cached_scan(store, pass::SourceError::Set(error))
    }

    pub fn heal_configuration_rejection(&self, store: &mut Store) -> Result<SnapshotStamp, CoordinatorError> {
        self.publish_cached_scan(store, pass::SourceError::Heal)
    }

    /// Publish a validated configuration candidate as one input version: its
    /// scan, its pipeline epoch (or the epoch's failure) and its compiled
    /// state. The compiled state is staged under the version the input
    /// publishes, so a reader finds it the moment its snapshot sees that
    /// version and not before; the module host, the watcher's roots and the
    /// rest of this process's memory change only once the input committed.
    /// A publication that fails changes none of them.
    pub(crate) fn publish_configuration_candidate(
        &self,
        store: &mut Store,
        candidate: ConfigurationCandidate,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let ConfigurationCandidate {
            roots,
            targets,
            build_targets,
            pipeline_source,
            mut requirements,
            schema_authority,
        } = candidate;
        // The candidate's own scanner: its roots never change, and it becomes
        // the candidate's compiled state.
        let scanner = self
            .scanner
            .candidate_with_roots(roots.clone())
            .map_err(|error| {
                CoordinatorError::InvalidManifest(AuthoringServiceInitError::from(error).to_string())
            })?;
        let filesystem_changed = !self.scanner.has_same_roots(&scanner);
        // Schema, target, or module-only candidates reuse the already
        // observed asset snapshot. A physical complete scan is reserved for
        // an actual configured-root replacement.
        let candidate_scan_heals =
            filesystem_changed || self.scan_initialized.get().is_none();
        let scan = if candidate_scan_heals {
            scanner.scan()?
        } else {
            self.published_scan(store)?
        };
        let candidate = ScanCandidate::build(scan, Some(&schema_authority))?;
        let mut runtime = lock_pipeline(&self.pipeline);
        let prepared_epoch = {
            let CoordinatedPipelineRuntime { host, loader, .. } = &mut *runtime;
            host.prepare_candidate(&pipeline_source, &mut requirements, loader)
        };
        // Nothing of this candidate is observable yet (the filesystem and
        // scan candidates are staged, not installed), so a module waiting for
        // source-walk leaves the whole configuration candidate pending.
        let prepared_epoch = match prepared_epoch {
            Err(CandidateRejection::AwaitingSchema(detail)) => {
                return Err(CoordinatorError::PipelineAwaitingSchema(detail))
            }
            Err(CandidateRejection::Failed(failure)) => Err(failure),
            Ok(prepared) => Ok(prepared),
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
                                    return self.publish_pipeline_rejection(store, failure);
                                }
                                return Err(CoordinatorError::InvalidManifest(error.to_string()));
                            }
                        };
                        if let Err(error) = self.authoring.prepare_pipeline_importers(
                            EpochAuthoringImporter::metadata_only(prepared.importer_descriptors()),
                        ) {
                            if let Some(failure) = runtime.host.discard_unpublished(prepared) {
                                drop(runtime);
                                return self.publish_pipeline_rejection(store, failure);
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
                    return self.publish_pipeline_rejection(store, failure);
                }
                return Err(error);
            }
        };
        let tag_epoch = schema_authority.source_hash();
        let max_dependency_depth = self.operational_configuration().max_dependency_depth;
        let base = self.server.stamp_of(store).version;
        let mut staged: Option<StagedCompiled<'_>> = None;
        let mut published_pipeline = None;
        let result = self
            .server
            .coordinated_replace_target_set(store, base, targets, |store| {
                let (version, _) = store
                    .input_transaction(|transaction| {
                        // A valid candidate heals the source's error; a
                        // physical scan of its roots replaces the pending
                        // scan rejection.
                        transaction.set_configuration_source_error(None)?;
                        if candidate_scan_heals {
                            transaction.set_scan_rejection(None)?;
                        }
                        transaction.mark_compiled()?;
                        Ok(transaction.version())
                    })
                    .map_err(|error| error.to_string())?;
                let mut commit = publish_scan(
                    store,
                    store.input_version(),
                    candidate,
                    true,
                    Some(&pipeline),
                    &projection,
                    tag_epoch,
                    &installed_claims,
                )
                .map_err(|error| error.to_string())?;
                let diagnostic = commit
                    .pipeline
                    .clone()
                    .expect("scan commit always carries pipeline diagnostics");
                let (snapshot, importers) = match (&pipeline, prepared_epoch.as_ref(), &diagnostic) {
                    (
                        ConfigurationPipelinePublication::Epoch { .. },
                        Some(prepared),
                        PipelineDiagnostic::Ready,
                    ) => (
                        PipelineSnapshot::ready(prepared.clone()),
                        self.authoring
                            .prepare_pipeline_importers(EpochAuthoringImporter::all(prepared))
                            .map_err(|error| format!("{error:?}"))?,
                    ),
                    (_, _, PipelineDiagnostic::Failed(error)) => {
                        (PipelineSnapshot::failed(error.clone()), Default::default())
                    }
                    _ => unreachable!(
                        "durable configuration pipeline state must match its prepared candidate"
                    ),
                };
                let entry = self.compiled.stage(Compiled::candidate(
                    version,
                    Arc::clone(&schema_authority),
                    build_targets.clone(),
                    projection.clone(),
                    importers,
                    snapshot,
                    scanner.clone(),
                    roots.clone(),
                ));
                let compiled = Arc::clone(entry.entry());
                staged = Some(entry);
                published_pipeline = Some(diagnostic);
                crate::build::refine_published_tag_index(
                    crate::build::OpenInput::new(store).expect("tag-index refinement runs inside its input"),
                    compiled.scanner().clone(),
                    Arc::clone(&schema_authority),
                    compiled.pipeline_snapshot(),
                    compiled.build_targets(),
                    max_dependency_depth,
                )?
                .apply(&mut commit);
                commit.pipeline_epoch_changed = true;
                #[cfg(test)]
                compiled_tests::candidate_staged()?;
                Ok(commit)
            });
        let stamp = match result {
            Ok(stamp) => stamp,
            Err(error) => {
                // The staged state holds the prepared epoch: release it first.
                drop(staged.take());
                if let Some(failure) = discard_prepared(&mut runtime, &mut prepared_epoch) {
                    drop(runtime);
                    return self.publish_pipeline_rejection(store, failure);
                }
                return Err(CoordinatorError::Coordinated(error));
            }
        };
        // Committed: install what the version holds.
        staged
            .take()
            .expect("a committed candidate staged its compiled state")
            .confirm();
        let mut cleanup_failure = None;
        match published_pipeline.expect("a committed candidate published its pipeline") {
            PipelineDiagnostic::Ready => runtime.host.install_ready(
                prepared_epoch
                    .take()
                    .expect("a Ready publication installs its prepared epoch"),
            ),
            PipelineDiagnostic::Failed(error) => {
                match &pipeline {
                    ConfigurationPipelinePublication::Failed(_) => warn_pipeline_failure(
                        &error,
                        "configuration pipeline candidate rejected; importers from it are unavailable",
                    ),
                    ConfigurationPipelinePublication::Epoch { .. } => warn_pipeline_failure(
                        &error,
                        "configuration pipeline epoch failed to publish; importers from it are unavailable",
                    ),
                }
                record_cleanup_failure(
                    &mut cleanup_failure,
                    discard_prepared(&mut runtime, &mut prepared_epoch),
                );
                runtime.host.install_failure(error);
            }
        }
        self.scanner.replace_from(&scanner);
        let _ = self.scan_initialized.set(());
        if let Some(failure) = cleanup_failure {
            drop(runtime);
            self.publish_pipeline_rejection(store, failure)
        } else {
            Ok(stamp)
        }
    }

    /// Publish a candidate-input failure which occurs before a module can be
    /// opened (for example, malformed watched schema JSON). The last durable
    /// schema/target projection remains intact, while the new input version is
    /// explicitly pipeline-failed and the live module is fenced.
    pub fn publish_pipeline_rejection(
        &self,
        store: &mut Store,
        failure: PipelineFailure,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        self.publish_pipeline_rejection_inner(store, failure, false)
    }

    pub(crate) fn publish_pipeline_rejection_healing_configuration(
        &self,
        store: &mut Store,
        failure: PipelineFailure,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        self.publish_pipeline_rejection_inner(store, failure, true)
    }

    fn publish_pipeline_rejection_inner(
        &self,
        store: &mut Store,
        failure: PipelineFailure,
        heal_configuration: bool,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        warn_pipeline_failure(
            &failure,
            "pipeline candidate rejected; importers from it are unavailable",
        );
        let base = self.server.stamp_of(store).version;
        let diagnostic = failure.clone();
        let mut staged: Option<StagedCompiled<'_>> = None;
        let result = self.server.coordinated_commit(store, base, |store| {
            if store.input_version() != base {
                return Err(format!(
                    "durable pipeline-failure basis is {:?}, expected {base:?}",
                    store.input_version()
                ));
            }
            // The compiled state this version keeps but for its pipeline: the
            // base's, or, when this process has compiled nothing for the
            // store yet, what it holds since it opened.
            let previous = match self.compiled.at(store) {
                Ok(previous) => previous,
                Err(CompiledLookupError::NotLoaded { .. }) => Arc::new(self.boot.clone()),
                Err(error) => return Err(error.to_string()),
            };
            let generation = match store
                .configuration_state()
                .map_err(|error| error.to_string())?
            {
                ConfigurationState::Ready(epoch) => epoch.generation,
                ConfigurationState::Failed { last_good, .. } => {
                    last_good.map_or(0, |epoch| epoch.generation)
                }
            };
            let ((configuration, version), _) = store
                .input_transaction(|transaction| {
                    transaction.publish_pipeline_failure(&diagnostic)?;
                    transaction.mark_compiled()?;
                    let configuration = if heal_configuration {
                        transaction.set_configuration_source_error(None)?;
                        Some(transaction.publish_configuration_status(generation)?)
                    } else {
                        None
                    };
                    Ok((configuration, transaction.version()))
                })
                .map_err(|error| error.to_string())?;
            staged = Some(
                self.compiled
                    .stage(previous.with_pipeline_failure(version, diagnostic.clone())),
            );
            Ok(Commit {
                configuration: configuration.map(configuration_status),
                pipeline: Some(PipelineDiagnostic::Failed(diagnostic.clone())),
                pipeline_epoch_changed: true,
                ..Commit::default()
            })
        });
        match result {
            Ok(stamp) => {
                staged
                    .take()
                    .expect("a committed pipeline failure staged its compiled state")
                    .confirm();
                lock_pipeline(&self.pipeline).host.install_failure(failure);
                Ok(stamp)
            }
            Err(error) => Err(CoordinatorError::Coordinated(error)),
        }
    }

    /// Reconcile one complete identity-checked namespace scan, as a pass
    /// with no imports.
    pub fn reconcile_full_scan(&self, store: &mut Store) -> Result<SnapshotStamp, CoordinatorError> {
        let compiled = self.loop_compiled(store)?;
        let base = self.server.stamp_of(store).version;
        let step = self.full_step(&compiled, store)?;
        self.pass(store, base, step, ImportScope::NONE)
            .map(|outcome| outcome.stamp)
    }

    /// Apply one native watcher batch by reopening only its affected paths or
    /// directory subtrees and merging those observations into the published
    /// state, as a pass with no imports.
    pub fn reconcile_incremental(
        &self,
        store: &mut Store,
        batch: &WatcherBatch,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let compiled = self.loop_compiled(store)?;
        let base = self.server.stamp_of(store).version;
        let step = self.incremental_step(&compiled, store, batch)?;
        self.pass(store, base, step, ImportScope::NONE)
            .map(|outcome| outcome.stamp)
    }

    /// Republish the observed namespace as a pass, changing the stored
    /// configuration source error as `source_error` says.
    fn publish_cached_scan(
        &self,
        store: &mut Store,
        source_error: pass::SourceError,
    ) -> Result<SnapshotStamp, CoordinatorError> {
        let compiled = self.loop_compiled(store)?;
        let base = self.server.stamp_of(store).version;
        let scan = self.published_scan(store)?;
        let step = self.candidate_step(&compiled, scan, &[], false, source_error)?;
        self.pass(store, base, step, ImportScope::NONE)
            .map(|outcome| outcome.stamp)
    }

    /// Rerun every watched import whose complete outcome-bearing basis
    /// drifted, as one pass: all of them publish as one version.
    pub fn reconcile_watched_imports(&self, store: &mut Store) -> Result<Vec<BundleUuid>, CoordinatorError> {
        self.import_pass(store, ImportScope::watched(None))
    }

    /// Watcher-work variant that revalidates only read sets capable of
    /// observing one of `work`'s dirty paths.
    pub fn reconcile_watched_imports_affected(
        &self,
        store: &mut Store,
        work: &PendingFileWork,
        capabilities_changed: bool,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        self.import_pass(store, ImportScope::watched(Some((work, capabilities_changed))))
    }

    /// Discover and apply authored directory-import rules, as one pass.
    /// Orphaned prior outputs are deliberately retained and therefore never
    /// appear as deletion work.
    pub fn reconcile_directory_imports(&self, store: &mut Store) -> Result<Vec<BundleUuid>, CoordinatorError> {
        self.import_pass(store, ImportScope::directories(None))
    }

    pub fn reconcile_directory_imports_affected(
        &self,
        store: &mut Store,
        work: &PendingFileWork,
        capabilities_changed: bool,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        self.import_pass(store, ImportScope::directories(Some((work, capabilities_changed))))
    }

    /// An imports-only pass. An importer failure with no bundle to hold its
    /// memo is this call's error, after the rest of the pass published.
    fn import_pass(
        &self,
        store: &mut Store,
        scope: ImportScope<'_>,
    ) -> Result<Vec<BundleUuid>, CoordinatorError> {
        self.loop_compiled(store)?;
        let base = self.server.stamp_of(store).version;
        let outcome = self.pass(store, base, pass::ScanStep::Unchanged, scope)?;
        if !outcome.failures.is_empty() {
            return Err(CoordinatorError::InvalidManifest(outcome.failures.join("; ")));
        }
        Ok(outcome.imported)
    }

    pub fn pending_file_work(&self, store: &mut Store) -> Result<PendingFileWork, CoordinatorError> {
        store
            .pending_file_work()
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }

    /// Clear the watcher work whose observation still stands. A path whose
    /// observation moved on has more work queued behind it, not an error: it
    /// stays pending, and this returns `false`.
    pub fn acknowledge_file_work(
        &self,
        store: &mut Store,
        work: &PendingFileWork,
    ) -> Result<bool, CoordinatorError> {
        if work.is_empty() {
            return Ok(true);
        }
        store
            .acknowledge_file_work(work)
            .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))
    }
}

/// The loop's CAS pass: evict to the cache limit and compact, unless neither
/// the CAS (its write count) nor the limit changed since `swept`, the last
/// pass that finished within the limit; then delete the dead segment files
/// no read can reach any more.
pub(crate) fn maintain_cas(
    store: &mut Store,
    sweeper: &mut distill_store::cas::SegmentSweeper,
    swept: &mut Option<(u64, u64)>,
) -> Result<(), distill_store::StoreError> {
    let state = (store.cas_writes()?, store.config().cache_limit);
    if *swept != Some(state) {
        *swept = None;
        let sweep = store.enforce_cache_limit()?;
        store.compact()?;
        if sweep.live_bytes <= state.1 {
            *swept = Some((store.cas_writes()?, state.1));
        }
    }
    sweeper.sweep(store)?;
    Ok(())
}

#[cfg(test)]
mod cas_pass_tests {
    use super::*;
    use distill_store::cas::record::KeyKind;
    use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec, PayloadKind, SegmentSweeper};

    static STATEMENTS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    fn record_statement(sql: &str) {
        STATEMENTS.lock().unwrap().push(sql.to_owned());
    }

    fn commit(store: &mut Store, key: u8) {
        store
            .commit_build(BuildCommit {
                wire_trees: Vec::new(),
                key_kind: KeyKind::Processor,
                static_input_key: [key; 32],
                asset_uuid: AssetUuid([7; 16]),
                static_inputs_canonical: vec![],
                trace: vec![key],
                outcome: CommitOutcome::Success {
                    payload_kind: PayloadKind::ProcessorOutput,
                    outputs: vec![OutputSpec {
                        output_key: String::new(),
                        type_uuids: vec![],
                        bytes: vec![key; 64],
                    }],
                    aux: vec![],
                },
            })
            .unwrap();
    }

    /// The statements one pass runs.
    fn pass(store: &mut Store, swept: &mut Option<(u64, u64)>) -> Vec<String> {
        let mut sweeper = SegmentSweeper::new(std::time::Duration::ZERO);
        store.trace_statements(Some(record_statement));
        STATEMENTS.lock().unwrap().clear();
        maintain_cas(store, &mut sweeper, swept).unwrap();
        store.trace_statements(None);
        std::mem::take(&mut *STATEMENTS.lock().unwrap())
    }

    /// A pass after no CAS write sums nothing and evicts nothing: it reads
    /// the write count and the dead segments only, however large the CAS.
    #[test]
    fn a_pass_with_no_cas_write_skips_the_sweeps() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
        let mut swept = None;
        let mut idle = Vec::new();
        for writes in [2u8, 40] {
            for key in 0..writes {
                commit(&mut store, key.wrapping_add(idle.len() as u8 * 100));
            }
            let first = pass(&mut store, &mut swept);
            assert!(first.iter().any(|sql| sql.contains("SUM(len)")), "{first:?}");
            let again = pass(&mut store, &mut swept);
            assert!(!again.iter().any(|sql| sql.contains("SUM(len)")), "{again:?}");
            idle.push(again.len());
        }
        assert_eq!(idle[0], idle[1], "{idle:?}");
        commit(&mut store, 250);
        let after_write = pass(&mut store, &mut swept);
        assert!(after_write.iter().any(|sql| sql.contains("SUM(len)")), "{after_write:?}");
    }
}

#[derive(Clone)]
struct ScanRejection {
    version: Vec<NamespaceError>,
    configuration: Option<ConfigurationError>,
}

/// A scan rejection and the physical subjects whose revalidation heals it,
/// as the store holds it (`distill_store::errors`).
#[derive(Clone)]
struct PendingScanRejection {
    rejection: ScanRejection,
    subjects: Vec<PathBuf>,
}

impl PendingScanRejection {
    /// The rejection `store` holds, if a scan left one.
    fn stored(store: &StoreReader) -> Result<Option<Self>, StoreError> {
        Ok(store.scan_rejection()?.map(|record| Self {
            rejection: ScanRejection {
                version: record.errors,
                configuration: record.configuration,
            },
            subjects: record
                .subjects
                .iter()
                .map(|subject| crate::scanner::decode_path(subject))
                .collect(),
        }))
    }

    fn record(&self) -> ScanRejectionRecord {
        ScanRejectionRecord {
            errors: self.rejection.version.clone(),
            configuration: self.rejection.configuration.clone(),
            subjects: self
                .subjects
                .iter()
                .map(|subject| crate::scanner::encode_path(subject))
                .collect(),
        }
    }
}

/// The status `error`, the configuration error the stored errors select,
/// publishes.
fn configuration_status(error: Option<ConfigurationError>) -> ConfigurationStatus {
    error.map_or(ConfigurationStatus::Ready, ConfigurationStatus::Failed)
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
    /// The pipeline candidate's source is ahead of the watched schema
    /// (`CandidateRejection::AwaitingSchema`). Nothing was published: the
    /// Ready epoch keeps serving until a schema write retries the candidate.
    PipelineAwaitingSchema(String),
    /// No compiled state serves the store's version (`crate::compiled`).
    Compiled(CompiledLookupError),
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

/// What the incremental plan reads besides the claims. The pending scan
/// rejection and the configuration errors are the store's own rows, which
/// the plan's publication leaves as they are.
struct PlanInputs<'a> {
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
    let namespace_errors = NamespaceError::canonical_set(reader.claims_namespace_errors()?)
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
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
        bundles,
        bundle_poisons,
        derived_outputs,
        paths,
    })
}

struct IncrementalScanPlan {
    namespace_errors: Vec<NamespaceError>,
    withheld: Withheld,
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
#[derive(Clone)]
struct ScanCandidate {
    scan: ScanSnapshot,
    renames: Vec<LogicalRename>,
    namespace_errors: Vec<NamespaceError>,
    bundle_poisons: BTreeMap<BundleUuid, ScopedBundlePoison>,
}

impl ScanCandidate {
    fn build(
        scan: ScanSnapshot,
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

        Ok(Self {
            scan,
            renames: Vec::new(),
            namespace_errors,
            bundle_poisons,
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

/// The candidate scan's `files` rows to write, each with whether it is
/// dirty, and the stored rows it no longer has: one ordered merge of the
/// stored rows against the candidate's, holding only the differences.
fn scan_file_changes(
    store: &StoreReader,
    scan: &ScanSnapshot,
) -> Result<(BTreeMap<(String, String), bool>, Vec<(String, String)>), StoreError> {
    let (mut writes, mut removed) = (BTreeMap::new(), Vec::new());
    let mut current = scan.file_observations().peekable();
    store.for_each_observed_file(|row| {
        let key = (row.root_name, row.path);
        while let Some((added, _)) = current.next_if(|(next, _)| **next < key) {
            writes.insert(added.clone(), true);
        }
        match current.next_if(|(next, _)| **next == key) {
            Some((_, file)) if file == row.file => {}
            Some((_, file)) => {
                let dirty = file.state != row.file.state;
                writes.insert(key, dirty);
            }
            None => removed.push(key),
        }
        Ok(())
    })?;
    writes.extend(current.map(|(added, _)| (added.clone(), true)));
    Ok((writes, removed))
}

/// Which bundles' stored asset rows differ from a candidate's. The rows
/// arrive grouped by bundle; each group is compared and dropped, so only
/// bundle identities are held.
struct AssetGroupChanges {
    stored: BTreeSet<BundleUuid>,
    differ: BTreeSet<BundleUuid>,
}

impl AssetGroupChanges {
    fn read(
        store: &StoreReader,
        current: &BTreeMap<BundleUuid, BTreeSet<AssetUuid>>,
    ) -> Result<Self, StoreError> {
        let mut changes = Self {
            stored: BTreeSet::new(),
            differ: BTreeSet::new(),
        };
        let mut close = |group: Option<(BundleUuid, BTreeSet<AssetUuid>)>| {
            if let Some((bundle, assets)) = group {
                if current.get(&bundle) != Some(&assets) {
                    changes.differ.insert(bundle);
                }
                changes.stored.insert(bundle);
            }
        };
        let mut group = None::<(BundleUuid, BTreeSet<AssetUuid>)>;
        store.for_each_bundle_asset(|bundle, asset| {
            match &mut group {
                Some((open, assets)) if *open == bundle => {
                    assets.insert(asset);
                }
                _ => close(group.replace((bundle, BTreeSet::from([asset])))),
            }
            Ok(())
        })?;
        close(group);
        Ok(changes)
    }

    /// Whether `bundle`'s stored asset set differs from `current`'s: a
    /// bundle with no stored rows differs exactly when the candidate has it.
    fn changed(
        &self,
        bundle: &BundleUuid,
        current: &BTreeMap<BundleUuid, BTreeSet<AssetUuid>>,
    ) -> bool {
        if self.stored.contains(bundle) {
            self.differ.contains(bundle)
        } else {
            current.contains_key(bundle)
        }
    }
}

#[allow(clippy::too_many_arguments)] // The scan transaction receives each publication input explicitly.
fn publish_scan(
    store: &mut Store,
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
    if store.input_version() != base {
        return Err(StoreError::InvalidConfiguration {
            error: format!(
                "durable scan basis is {:?}, expected {base:?}",
                store.input_version()
            ),
        });
    }
    let (file_writes, removed_files) = scan_file_changes(store, &candidate.scan)?;
    let old_bundles = store.all_bundles()?;
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
    let current_bundle_assets = published_bundle_assets(&published, &candidate.bundle_poisons);
    let asset_changes = AssetGroupChanges::read(store, &current_bundle_assets)?;
    let assets_changed = |bundle: &BundleUuid| asset_changes.changed(bundle, &current_bundle_assets);
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
    let newly_failed = withheld.newly_failed(store)?;
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

    let publishable_changed_bundles = changed_bundles;
    let rpc_publishable_bundles = rpc_changed_bundles;
    let mut commit = rpc_commit(
        &candidate,
        &published,
        &withheld,
        &newly_failed,
        store,
        projection,
        derived_outputs.clone(),
        &rpc_publishable_bundles,
    )?;
    let mut next_pipeline = pipeline_diagnostic(store.pipeline_state()?);
    let mut configuration = None;
    store.input_transaction(|transaction| {
        // Rows are labelled with the version the input publishes, which a
        // pass's earlier step may already have advanced to.
        let observation = transaction.version();
        transaction.replace_source_claims(None, claims)?;
        let mut root_ids = BTreeMap::new();
        let mut newest_mtime = 0;
        for (key, file) in candidate.scan.file_observations() {
            let root = *root_ids
                .entry(key.0.clone())
                .or_insert(transaction.intern_root(&key.0)?);
            newest_mtime = newest_mtime.max(file.state.mtime);
            let Some(&dirty) = file_writes.get(key) else {
                continue;
            };
            transaction.upsert_file(root, &key.1, &file, observation)?;
            if let Some(bundle) = candidate.scan.bundles.get(key) {
                transaction.set_bundle_file(root, &key.1, &bundle.bytes)?;
            }
            if dirty {
                transaction.push_dirty(root, &key.1, true, observation)?;
            }
        }
        for (root_name, path) in &removed_files {
            let root = transaction.intern_root(root_name)?;
            transaction.remove_file(root, path)?;
            transaction.push_dirty(root, path, false, observation)?;
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

        configuration = transaction.publish_configuration_status(generation)?;
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
            transaction.set_bundle_path_refs(
                bundle.uuid,
                crate::operations::bundle_path_references(bundle)
                    .iter()
                    .map(String::as_str),
            )?;
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
    commit.configuration = Some(configuration_status(configuration));
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
    store: &mut Store,
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
    if store.input_version() != base {
        return Err(StoreError::InvalidConfiguration {
            error: format!(
                "durable incremental-scan basis is {:?}, expected {base:?}",
                store.input_version()
            ),
        });
    }
    let (commit, _) = store.input_transaction(|transaction| {
        // Rows are labelled with the version the input publishes: inside a
        // pass, an earlier step has already advanced it past `base`.
        let observation = transaction.version();
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
            mut commit,
            changed_bundles,
            configuration_generation,
        } = prepare_incremental_publication(
            &transaction.reader(),
            inputs,
            projection,
        )?;
        transaction.set_namespace_errors(plan.namespace_errors.iter().cloned())?;
        let configuration = transaction.publish_configuration_status(configuration_generation)?;
        commit.configuration = Some(configuration_status(configuration));
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
            transaction.set_bundle_path_refs(
                bundle.uuid,
                crate::operations::bundle_path_references(bundle)
                    .iter()
                    .map(String::as_str),
            )?;
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
        // Selected from the store's errors in the publishing input.
        configuration: None,
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
/// authority, under `compiled`, the compiled state of the writer's version.
/// The pending scan rejection and the configuration errors are the store's
/// own rows: this publication keeps them.
pub(crate) fn publish_incremental_paths(
    paths: &[PathBuf],
    store: &mut Store,
    base: InputVersion,
    compiled: &Compiled,
    coordinator: Option<&DaemonCoordinator>,
) -> Result<Commit, String> {
    let scanner = compiled.scanner();
    let projection = compiled.projection();
    let (delta, baseline) = {
        let store: &StoreReader = store;
        let stored = StoredBaseline::new(store);
        let delta = scanner.scan_incremental_delta(&stored, paths);
        stored.finish().map_err(|error| error.to_string())?;
        let delta = delta
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "authored path is outside every configured root".to_owned())?;
        let baseline = ScanSnapshot::load_under(store, delta.affected_prefixes())
            .map_err(|error| error.to_string())?;
        (delta, baseline)
    };
    let authority = coordinator.and_then(|_| compiled.schema_authority());
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
                crate::build::refine_published_tag_index_incremental(
                    crate::build::OpenInput::new(store).expect("tag-index refinement runs inside its input"),
                    scanner.clone(),
                    authority,
                    compiled.pipeline_snapshot(),
                    compiled.build_targets(),
                    coordinator.operational_configuration().max_dependency_depth,
                    &affected,
                )
                .apply_incremental(&mut commit);
            }
        }
        return Ok(commit);
    }

    drop(baseline);
    let mut scan = ScanSnapshot::load(store).map_err(|error| error.to_string())?;
    scan.apply_delta(delta);
    let claims = bundle_claims(scan.bundle_rows(), projection, authority.as_deref())
        .map_err(|error| error.to_string())?;
    let candidate = ScanCandidate::build(scan, authority.as_deref())
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
    old: &StoreReader,
    projection: &PipelineProjection,
    derived_outputs: BTreeMap<AssetUuid, DerivedOutputEntry>,
    changed_bundles: &BTreeSet<BundleUuid>,
) -> Result<Commit, StoreError> {
    let mut commit = Commit {
        // Selected from the store's errors in the publishing input.
        configuration: None,
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
        if old.asset_exists(*asset)? {
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
            if old.asset_exists(entry.asset)? {
                commit
                    .authoring
                    .push(AuthoringMutation::Remove { uuid: entry.asset });
            }
        }
    }
    old.for_each_asset_bundle(|asset, _| {
        if !current_assets.contains(&asset) {
            commit.assets.push(AssetMutation::Set {
                uuid: asset,
                resolution: StoredResolve::Deleted,
                delta: AssetDeltaState::Deleted,
            });
            commit
                .authoring
                .push(AuthoringMutation::Remove { uuid: asset });
        }
        Ok(())
    })?;
    path_mutations(old, &paths, &mut commit.paths)?;
    Ok(commit)
}

/// The path mutations taking the stored path index to `paths`. The index
/// arrives grouped by path and merges, group by group, with `paths` in
/// path order.
fn path_mutations(
    old: &StoreReader,
    paths: &BTreeMap<String, BTreeSet<AssetUuid>>,
    mutations: &mut Vec<PathMutation>,
) -> Result<(), StoreError> {
    let mut current_paths = paths.iter().peekable();
    let mut group = None::<(String, BTreeSet<AssetUuid>)>;
    old.for_each_path_entry(|path, _, asset| {
        match &mut group {
            Some((open, assets)) if *open == path => {
                assets.insert(asset);
            }
            _ => {
                if let Some(closed) = group.replace((path, BTreeSet::from([asset]))) {
                    merge_path_group(&mut current_paths, Some(closed), mutations);
                }
            }
        }
        Ok(())
    })?;
    if let Some(closed) = group {
        merge_path_group(&mut current_paths, Some(closed), mutations);
    }
    merge_path_group(&mut current_paths, None, mutations);
    Ok(())
}

/// Merge one stored path-index group, or the end of the index (`None`),
/// into `mutations`: each candidate path ordered before it is new, and the
/// group's own path is kept, replaced or removed.
fn merge_path_group(
    current: &mut std::iter::Peekable<
        std::collections::btree_map::Iter<'_, String, BTreeSet<AssetUuid>>,
    >,
    stored: Option<(String, BTreeSet<AssetUuid>)>,
    mutations: &mut Vec<PathMutation>,
) {
    while let Some((path, candidates)) = current.next_if(|(next, _)| {
        stored
            .as_ref()
            .is_none_or(|(stored, _)| next.as_str() < stored.as_str())
    }) {
        mutations.push(PathMutation::Set {
            path: path.clone(),
            candidates: candidates.clone(),
        });
    }
    let Some((path, old)) = stored else {
        return;
    };
    match current.next_if(|(next, _)| **next == path) {
        Some((_, candidates)) if *candidates == old => {}
        Some((_, candidates)) => mutations.push(PathMutation::Set {
            path,
            candidates: candidates.clone(),
        }),
        None => mutations.push(PathMutation::Remove { path }),
    }
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


/// The pipeline runtime. Other threads read the epoch of their own version
/// through its compiled state (`crate::compiled`), never this.
struct PipelineState {
    /// Taken to prepare, install or fail an epoch; never while waiting on
    /// SQLite's write lock with a transaction open.
    runtime: Mutex<CoordinatedPipelineRuntime>,
}

impl PipelineState {
    fn new(runtime: CoordinatedPipelineRuntime) -> Self {
        Self {
            runtime: Mutex::new(runtime),
        }
    }
}

/// The pipeline runtime, locked.
struct PipelineGuard<'a> {
    runtime: MutexGuard<'a, CoordinatedPipelineRuntime>,
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

fn lock_pipeline(pipeline: &PipelineState) -> PipelineGuard<'_> {
    PipelineGuard {
        runtime: locked(&pipeline.runtime),
    }
}

/// `mutex`, locked; a panic while it was held leaves the state as it was.
fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
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

    /// An operational configuration change reaches writers already open: the
    /// loop's, and the one the scheduler keeps idle between build jobs.
    #[test]
    fn a_configuration_change_reaches_open_writers() {
        let temp = tempfile::tempdir().unwrap();
        let assets = temp.path().join("assets");
        std::fs::create_dir_all(&assets).unwrap();
        let mut config = StoreConfig::new(temp.path().join("state"));
        config.parallelism = 1;
        let coordinator = Arc::new(
            DaemonCoordinator::open(
                config.clone(),
                vec![AssetRoot::new("main", &assets)],
                Vec::new(),
                8,
            )
            .unwrap(),
        );
        let cache_limit = |store: &mut Store| {
            store
                .write_transaction(|store| Ok(store.config().cache_limit))
                .unwrap()
        };
        let mut loop_writer = coordinator.open_writer().unwrap();
        let before = cache_limit(&mut loop_writer);
        // The scheduler opens a writer for this job and keeps it idle.
        assert_eq!(
            coordinator.run_scheduled(WorkClass::Batch, move |store| cache_limit(store)),
            before
        );

        config.cache_limit = before * 2;
        coordinator
            .apply_operational_configuration(&config, 8)
            .unwrap();
        assert_eq!(cache_limit(&mut loop_writer), before * 2);
        assert_eq!(
            coordinator.run_scheduled(WorkClass::Batch, move |store| cache_limit(store)),
            before * 2
        );
    }

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
                coordinator.run_scheduled(WorkClass::Interactive, move |_store| {
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
                coordinator.run_scheduled(WorkClass::Interactive, move |_store| {
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
                coordinator.run_scheduled(WorkClass::Interactive, move |_store| {
                    entered_tx.send(()).unwrap();
                    release_first_rx.recv().unwrap();
                });
            })
        };
        let second = {
            let coordinator = Arc::clone(&coordinator);
            thread::spawn(move || {
                coordinator.run_scheduled(WorkClass::Interactive, move |_store| {
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

#[cfg(test)]
mod publish_diff_tests {
    //! A complete publication's streamed diffs against the stored rows give
    //! what the whole-table maps they replaced gave.

    use super::*;
    use crate::scanner::{AssetRoot, RootedScanner};
    use distill_core::id::LogicalHash;
    use distill_store::files::RootId;

    /// A deterministic generator: the cases vary, the runs do not.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
        (dir, store)
    }

    const NAMES: [&str; 8] = ["dir", "dir.txt", "dir-old", "dir0", "é", "a", "z", "b c"];

    fn write_tree(root: &std::path::Path, random: &mut Lcg) {
        for name in NAMES {
            match random.next(4) {
                0 => {}
                1 => std::fs::write(root.join(name), [random.next(3) as u8]).unwrap(),
                _ => {
                    std::fs::create_dir_all(root.join(name)).unwrap();
                    for child in ["child", "x.bin"] {
                        if random.next(2) == 0 {
                            std::fs::write(root.join(name).join(child), [random.next(3) as u8])
                                .unwrap();
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn file_changes_match_the_stored_file_map() {
        let mut random = Lcg(7);
        for _ in 0..12 {
            let (dir, mut store) = store();
            let root = dir.path().join("root");
            std::fs::create_dir(&root).unwrap();
            let scanner = RootedScanner::new([AssetRoot::new("main", &root)]).unwrap();
            write_tree(&root, &mut random);
            let published = scanner.scan().unwrap();
            store
                .input_transaction(|txn| {
                    let version = txn.version();
                    for (key, file) in published.file_observations() {
                        let root = txn.intern_root(&key.0)?;
                        txn.upsert_file(root, &key.1, &file, version)?;
                    }
                    Ok(())
                })
                .unwrap();
            std::fs::remove_dir_all(&root).unwrap();
            std::fs::create_dir(&root).unwrap();
            write_tree(&root, &mut random);
            let candidate = scanner.scan().unwrap();

            let old_files = store
                .observed_files()
                .unwrap()
                .into_iter()
                .map(|row| ((row.root_name, row.path), row.file))
                .collect::<BTreeMap<_, _>>();
            let mut writes = BTreeMap::new();
            for (key, file) in candidate.file_observations() {
                let old = old_files.get(key);
                if old != Some(&file) {
                    writes.insert(key.clone(), old.is_none_or(|old| old.state != file.state));
                }
            }
            let removed = old_files
                .keys()
                .filter(|key| !candidate.files.contains_key(*key))
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(scan_file_changes(&store, &candidate).unwrap(), (writes, removed));
        }
    }

    /// Stored assets for random bundles, then random candidate groups.
    #[test]
    fn asset_and_path_changes_match_the_stored_maps() {
        let mut random = Lcg(11);
        for _ in 0..20 {
            let (_dir, mut store) = store();
            let bundle = |index: u64| BundleUuid([index as u8 + 1; 16]);
            let asset = |index: u64| AssetUuid([index as u8 + 1; 16]);
            let path = |index: u64| NAMES[index as usize % NAMES.len()].to_owned();
            store
                .input_transaction(|txn| {
                    let roots = [txn.intern_root("main")?, txn.intern_root("alt")?];
                    for index in 0..6 {
                        if random.next(3) == 0 {
                            continue;
                        }
                        txn.upsert_bundle(&BundleMeta {
                            bundle: bundle(index),
                            root: roots[0],
                            path: path(index),
                            format_version: 1,
                            content_hash: ContentHash([0; 32]),
                            origin: None,
                        })?;
                        for entry in 0..random.next(3) {
                            txn.upsert_asset(&AssetRecord {
                                asset: asset(index * 8 + entry),
                                bundle: bundle(index),
                                local_id: format!("e{entry}"),
                                type_uuid: TypeUuid([1; 16]),
                                logical_hash: LogicalHash([1; 32]),
                                authoring_only: false,
                                tags: BTreeMap::new(),
                                served: None,
                            })?;
                        }
                    }
                    for index in 0..10 {
                        if random.next(2) == 0 {
                            let root = roots[random.next(2) as usize];
                            txn.set_path_entry(&path(index), root, asset(random.next(40)))?;
                        }
                    }
                    Ok(())
                })
                .unwrap();
            let mut current = BTreeMap::<BundleUuid, BTreeSet<AssetUuid>>::new();
            let mut paths = BTreeMap::<String, BTreeSet<AssetUuid>>::new();
            for index in 0..6 {
                if random.next(3) != 0 {
                    current.insert(
                        bundle(index),
                        (0..random.next(3)).map(|entry| asset(index * 8 + entry)).collect(),
                    );
                }
            }
            for index in 0..10 {
                if random.next(2) == 0 {
                    paths
                        .entry(path(index))
                        .or_default()
                        .extend((0..1 + random.next(2)).map(|_| asset(random.next(40))));
                }
            }

            let mut old_bundle_assets = BTreeMap::<BundleUuid, BTreeSet<AssetUuid>>::new();
            for (asset, bundle) in store.all_asset_bundles().unwrap() {
                old_bundle_assets.entry(bundle).or_default().insert(asset);
            }
            let changes = AssetGroupChanges::read(&store, &current).unwrap();
            for index in 0..8 {
                assert_eq!(
                    changes.changed(&bundle(index), &current),
                    old_bundle_assets.get(&bundle(index)) != current.get(&bundle(index)),
                    "{index}"
                );
            }

            let old_paths: Vec<(String, RootId, AssetUuid)> = store.all_path_entries().unwrap();
            let mut old_path_candidates = BTreeMap::<String, BTreeSet<AssetUuid>>::new();
            for (path, _, asset) in &old_paths {
                old_path_candidates.entry(path.clone()).or_default().insert(*asset);
            }
            let mut expected = Vec::new();
            let names = old_path_candidates
                .keys()
                .chain(paths.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            for path in names {
                match (old_path_candidates.get(&path), paths.get(&path)) {
                    (old, current) if old == current => {}
                    (_, Some(candidates)) => expected.push(PathMutation::Set {
                        path,
                        candidates: candidates.clone(),
                    }),
                    (Some(_), None) => expected.push(PathMutation::Remove { path }),
                    (None, None) => unreachable!(),
                }
            }
            let mut merged = Vec::new();
            path_mutations(&store, &paths, &mut merged).unwrap();
            assert_eq!(merged, expected);
        }
    }
}
