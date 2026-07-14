//! Single-writer daemon coordinator.
//!
//! Filesystem scans are candidates. Publication happens only through one
//! server-serialized closure which rechecks the durable store basis, applies
//! the complete SQLite input transaction, and returns the exact immutable RPC
//! projection for the same successor version.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use distill_bundle::{AssetEntry, Bundle};
use distill_core::attestation::{is_bootstrap_control_type, SCHEMA_LINEAGE_MANIFEST_TYPE_UUID};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::lineage::AcceptedSchemaEpoch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AssetDeltaState, AssetMutation, AuthoringEntry, AuthoringEntryRole, AuthoringMutation,
    AuthoringValue, Commit, ConfigurationStatus, CoordinatedCommitError, DriftedInput,
    LineageManifestClaimant, LineageRepairState, PathMutation, PipelineDiagnostic, Server,
    SnapshotStamp, StoredResolve, TargetDefinition, VersionPoison, VersionPoisonV1,
};
use distill_store::bundles::{AssetRecord, BundleMeta};
use distill_store::files::{FileKind, FileState};
use distill_store::pipeline::{
    AcceptedTypeLineage, SchemaLineageManifest, TypeAuthorityState, VerifiedSchemaLineageManifest,
};
use distill_store::state::{
    AssetClaimant, CleanupDisposition, ConfigurationState, DirectoryAliasSide, DscpV1,
    InputVersion, PipelinePoison, PipelinePoisonCode, PipelinePoisonOrigin,
    PipelineState as StoredPipelineState, PlatformFileIdentity, ReadableBundleSource,
    ScanFailureCode, ScanSubject, SkeletonFailureCode,
};
use distill_store::{Store, StoreConfig, StoreError};

use crate::authoring::{AuthoringService, AuthoringServiceInitError};
use crate::epoch::{
    stored_pipeline_epoch, CandidateRequirements, ModuleHost, PipelineEpoch, PipelineSnapshot,
    UnloadOutcome,
};
use crate::lineage_repair::LineageRepairBackendInitError;
use crate::module_loader::DynamicPipelineModuleLoader;
use crate::scanner::{
    AssetRoot, ObservedFileIdentity, RootedScanner, ScanError, ScanSnapshot, ScannedFileKind,
};
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
    lineage_destination: LineageDestination,
    authoring: Arc<AuthoringService>,
    pipeline: Mutex<CoordinatedPipelineRuntime>,
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
    ) -> Result<Self, CoordinatorInitError> {
        let module_state_path = store_config.state_path.join("pipeline-host");
        let store = Arc::new(Mutex::new(Store::open(store_config)?));
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
        Ok(Self {
            store,
            scanner,
            server,
            lineage_destination,
            authoring: backend,
            pipeline: Mutex::new(pipeline),
        })
    }

    pub fn server(&self) -> &Server {
        &self.server
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
                        .input_transaction(|transaction| {
                            transaction.publish_pipeline_poison(&diagnostic)
                        })
                        .map_err(|error| error.to_string())?;
                    Ok(Commit {
                        pipeline: Some(PipelineDiagnostic::Poisoned(diagnostic.clone())),
                        ..Commit::default()
                    })
                });
                match result {
                    Ok(stamp) => {
                        discard_pending(&mut runtime);
                        runtime.host.install_poison(poison);
                        return Ok(stamp);
                    }
                    Err(error) => return Err(CoordinatorError::Coordinated(error)),
                }
            }
        };

        let stored = match stored_pipeline_epoch(&prepared, &requirements) {
            Ok(stored) => stored,
            Err(error) => {
                let _ = runtime.host.discard_unpublished(prepared);
                return Err(CoordinatorError::InvalidManifest(error.to_string()));
            }
        };
        let publication = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&publication);
        let store = Arc::clone(&self.store);
        let result = self.server.coordinated_commit(base, || {
            let mut store = lock_store(&store);
            if store.input_version() != base {
                return Err(format!(
                    "durable pipeline basis is {:?}, expected {base:?}",
                    store.input_version()
                ));
            }
            store
                .input_transaction(|transaction| transaction.publish_pipeline_epoch(&stored))
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
                discard_pending(&mut runtime);
                runtime.host.install_ready(prepared);
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
            }
            PipelinePublication::Poisoned(poison) => {
                let _ = runtime.host.discard_unpublished(prepared);
                discard_pending(&mut runtime);
                runtime.host.install_poison(poison);
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
        let candidate = ScanCandidate::build(&self.scanner, &self.lineage_destination, scan)?;
        let base = self.server.current_stamp().version;
        let store = Arc::clone(&self.store);
        self.server
            .coordinated_commit(base, || {
                publish_scan(&store, base, candidate).map_err(|error| error.to_string())
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
                        store
                            .input_transaction(|transaction| {
                                transaction.set_version_poisons([poison.clone()])
                            })
                            .map_err(|error| error.to_string())?;
                        Commit {
                            version_poison: Some(Some(poison.clone())),
                            ..Commit::default()
                        }
                    }
                    ScanRejection::Configuration { reason, message } => {
                        let poison = distill_rpc::ConfigurationPoison::from_reason(reason, message);
                        store
                            .input_transaction(|transaction| {
                                transaction.set_version_poisons([])?;
                                transaction.publish_configuration_poison(reason, message)
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
            self.server
                .coordinated_commit(base, || {
                    authoring
                        .prepare_reimport_bundle(base, bundle)
                        .map(|prepared| prepared.commit)
                        .map_err(|error| format!("{error:?}"))
                })
                .map_err(CoordinatorError::Coordinated)?;
            imported.push(bundle);
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
            self.server
                .coordinated_commit(base, || {
                    let prepared = authoring
                        .prepare_directory_import(base, &task)
                        .map_err(|error| format!("{error:?}"))?;
                    *captured
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(prepared.bundle);
                    Ok(prepared.commit)
                })
                .map_err(CoordinatorError::Coordinated)?;
            imported.push(
                bundle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .expect("coordinated directory import captured its bundle"),
            );
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
        let (configuration, lineage_repair, lineage_manifest) = match claimants.as_slice() {
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

        Ok(Self {
            scan,
            version_poison,
            configuration,
            lineage_repair,
            lineage_manifest,
        })
    }
}

fn publish_scan(
    store: &Arc<Mutex<Store>>,
    base: InputVersion,
    candidate: ScanCandidate,
) -> Result<Commit, StoreError> {
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
    let generation = match store.configuration_state()? {
        ConfigurationState::Ready(epoch) => epoch.generation,
        ConfigurationState::Poisoned { last_good, .. } => {
            last_good.map_or(0, |epoch| epoch.generation)
        }
    };

    let mut commit = rpc_commit(&candidate, &old_assets, &old_paths)?;
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

        match &candidate.configuration {
            ConfigurationStatus::Ready => transaction.publish_configuration_ready(generation)?,
            ConfigurationStatus::Poisoned(poison) => {
                transaction.publish_configuration_poison(&poison.detail, &poison.message)?
            }
        }
        if let Some(manifest) = &candidate.lineage_manifest {
            transaction.project_verified_lineage_manifest(manifest)?;
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
) -> Result<Commit, String> {
    let scan = scanner.scan().map_err(|error| error.to_string())?;
    let candidate = ScanCandidate::build(scanner, lineage_destination, scan)
        .map_err(|error| error.to_string())?;
    publish_scan(store, base, candidate).map_err(|error| error.to_string())
}

fn rpc_commit(
    candidate: &ScanCandidate,
    old_assets: &[AssetUuid],
    old_paths: &[(String, distill_store::files::RootId, AssetUuid)],
) -> Result<Commit, StoreError> {
    let mut commit = Commit {
        configuration: Some(candidate.configuration.clone()),
        pipeline: Some(PipelineDiagnostic::Ready),
        version_poison: Some(candidate.version_poison.clone()),
        lineage_repair: Some(candidate.lineage_repair.clone()),
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
        terminal_type: entry.type_uuid,
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
