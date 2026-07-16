//! Snapshot-pinned lazy build execution and durable build-import caching.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use distill_build::artifact_encode::{
    encode_artifact_value, ArtifactEncodeError, ArtifactValueSpec, EncodedArtifact,
};
use distill_build::dslf::{
    ArtifactEncodingFailureV1, DslfV1, LocalFailureClass, MigrationPlanFailureV1,
    OutputBindingFailureV1, OutputBindingSlotV1,
};
use distill_build::keys::{
    build_import_digest, static_inputs_canonical_bytes, static_inputs_digest, AppliedMigration,
    AutomaticMigration, BuildImportInputs, OutputHash, StaticInputs,
};
use distill_build::persist::{
    lookup_persisted_candidate, persisted_candidate_traces, PersistedOutcome,
};
use distill_build::pipeline::{
    PipelineChain, PipelineRegistry, PipelineStage, ProcessorRegistration, Target,
};
use distill_build::query::{asset_query_result_hash, normalize_path, AssetQuery};
use distill_build::tool::{ProcessContext, ToolEpochSnapshot, ToolRuntimeBinding};
use distill_build::trace::{
    control_failure_fingerprint, revalidate, trace_payload_bytes, CapabilityKey,
    ControlFailureCode, ControlFailureSubject, ControlQuery, ControlSubject, ControlValueHash,
    EntryRole, MigrationControlKind, MigrationControlValue, Observed, StableFailureFingerprint,
    TraceOp, TraceSource,
};
use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::bootstrap::MIGRATION_TYPE_UUID;
use distill_core::id::{
    AssetUuid, BundleFileHash, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid,
};
#[cfg(test)]
use distill_core::lineage::lineage_chain_digest;
use distill_json::AuthoredValue;
use distill_migrate::{
    conforms, execute_ops, plan_automatic, validate_plan, DefaultProvider, EdgeKind, FieldPath,
    MigrationError, MigrationOp,
};
use distill_rpc::{
    decode_asset_reference_query, decode_authoring_payload, ArtifactLeaseBackend, ArtifactPayload,
    ArtifactPayloadBackend, AssetReferenceQuery, AuthoringMutation, BuildArtifactPublication,
    BuildBackend, BuildBackendOutcome, BuildPublication, BuildRequest, BuildWireTree,
    BuildWorkClass, Commit, DriftedInput, PipelineUnavailableDiagnostic, RpcFailure,
    RuntimeTypePolicy, RuntimeTypePolicyRequest, ServedLoadEdge, TagPoisonMutation,
    TagProjectionMutation,
};
use distill_schema::{ProjectSchemaAuthority, ProjectTypeAuthority};
use distill_store::artifacts::PinKind;
use distill_store::bundles::{BundleMeta, EntryMeta, TagIndexUpdate};
use distill_store::cas::record::{
    FailureCause as StoreFailureCause, FailureFingerprint as StoreFailureFingerprint, KeyKind,
    LocalFailureClass as StoreLocalFailureClass,
};
use distill_store::cas::{AuxSpec, BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
use distill_store::pipeline::RegisteredTool;
use distill_store::{Store, StoreError};
use distill_wire::artifact::{parse_artifact, ArtifactError, ARTIFACT_FORMAT_VERSION};
use distill_wire::encode::EncodeError;

use crate::callbacks::{
    CallbackInvokeError, DiagnosticSeverity, PipelineProcessContext, ProcessArtifact,
    ProcessContextError, ProcessOutputs,
};
use crate::coordinator::DaemonCoordinator;
use crate::epoch::{PipelineEpoch, PipelineSnapshot};
use crate::migration_control::{self, MigrationDecodeError, MigrationHeader};
use crate::scanner::RootedScanner;
use crate::scheduler::WorkClass;

const MIGRATION_PLANNER_VERSION: u32 = 1;

pub(crate) struct PublishedTagIndex {
    tags: BTreeMap<AssetUuid, BTreeMap<String, Option<String>>>,
    poisons: BTreeMap<AssetUuid, BundleUuid>,
    removed: BTreeSet<AssetUuid>,
}

impl PublishedTagIndex {
    fn conservatively_poisoned(assets: &BTreeMap<AssetUuid, BundleUuid>) -> Self {
        Self {
            tags: assets
                .keys()
                .copied()
                .map(|asset| (asset, BTreeMap::new()))
                .collect(),
            poisons: assets.clone(),
            removed: BTreeSet::new(),
        }
    }

    pub(crate) fn apply(self, commit: &mut Commit) {
        for mutation in &mut commit.authoring {
            if let AuthoringMutation::Set(entry) = mutation {
                entry.tags = self.tags.get(&entry.uuid).cloned().unwrap_or_default();
            }
        }
        commit.tag_projection = Some(self.tags);
        commit.tag_poisons = Some(self.poisons);
    }

    pub(crate) fn apply_incremental(self, commit: &mut Commit) {
        for mutation in &mut commit.authoring {
            if let AuthoringMutation::Set(entry) = mutation {
                if let Some(tags) = self.tags.get(&entry.uuid) {
                    entry.tags = tags.clone();
                }
            }
        }
        for (asset, tags) in self.tags {
            commit
                .tag_projection_mutations
                .push(TagProjectionMutation::Set { asset, tags });
        }
        for asset in &self.removed {
            commit
                .tag_projection_mutations
                .push(TagProjectionMutation::Remove { asset: *asset });
        }
        let affected = commit
            .tag_projection_mutations
            .iter()
            .map(|mutation| match mutation {
                TagProjectionMutation::Set { asset, .. }
                | TagProjectionMutation::Remove { asset } => *asset,
            })
            .collect::<BTreeSet<_>>();
        for asset in affected {
            match self.poisons.get(&asset) {
                Some(bundle) => commit.tag_poison_mutations.push(TagPoisonMutation::Set {
                    asset,
                    bundle: *bundle,
                }),
                None => commit
                    .tag_poison_mutations
                    .push(TagPoisonMutation::Remove { asset }),
            }
        }
    }
}

pub(crate) struct CoordinatorBuildBackend {
    coordinator: Weak<DaemonCoordinator>,
    active_builds: Mutex<usize>,
}

impl CoordinatorBuildBackend {
    pub(crate) fn new(coordinator: &Arc<DaemonCoordinator>) -> Self {
        Self {
            coordinator: Arc::downgrade(coordinator),
            active_builds: Mutex::new(0),
        }
    }
}

impl BuildBackend for CoordinatorBuildBackend {
    fn build(&self, request: &BuildRequest) -> Result<BuildBackendOutcome, RpcFailure> {
        {
            let mut active = self
                .active_builds
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *active = active.checked_add(1).expect("active build count exhausted");
        }
        let coordinator =
            self.coordinator
                .upgrade()
                .ok_or_else(|| RpcFailure::AuthoringBackendUnavailable {
                    operation: "build coordinator stopped".to_owned(),
                })?;
        let class = match request.work_class {
            BuildWorkClass::Interactive => WorkClass::Interactive,
            BuildWorkClass::Batch => WorkClass::Batch,
        };
        let scheduled_request = request.clone();
        let job_coordinator = Arc::clone(&coordinator);
        let (result, poison) = coordinator.run_scheduled(class, move || {
            let result = build_with_runtime(&job_coordinator, &scheduled_request);
            let poison = job_coordinator.sync_runtime_pipeline_poison();
            (result, poison)
        });
        match poison {
            Ok(Some(poison)) => {
                return Err(RpcFailure::PipelineUnavailable(Box::new(
                    PipelineUnavailableDiagnostic::PipelinePoison(poison),
                )))
            }
            Ok(None) => {}
            Err(error) => {
                return Err(RpcFailure::AuthoringBackendUnavailable {
                    operation: format!("persist runtime pipeline poison: {error}"),
                })
            }
        }
        match result {
            Ok(publication) => {
                let mut hashes = publication
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.content_hash.0)
                    .collect::<Vec<_>>();
                hashes.extend(publication.wire_trees.iter().map(|tree| tree.layout_hash.0));
                coordinator
                    .store()
                    .lock()
                    .map_err(|_| RpcFailure::AuthoringBackendUnavailable {
                        operation: "pin completed build: durable store mutex is poisoned"
                            .to_owned(),
                    })?
                    .pin(PinKind::InFlight, &build_pin_holder(request), &hashes)
                    .map_err(|error| RpcFailure::AuthoringBackendUnavailable {
                        operation: format!("pin completed build: {error}"),
                    })?;
                Ok(BuildBackendOutcome::Built(publication))
            }
            Err(BuildError::Drifted(input)) => Ok(BuildBackendOutcome::Drifted { input }),
            Err(BuildError::DepthExceeded { limit, chain }) => {
                Err(RpcFailure::BuildDepthExceeded { limit, chain })
            }
            Err(BuildError::Failed(error)) => Ok(BuildBackendOutcome::Failed { error }),
            Err(BuildError::Deterministic { message, .. }) => {
                Ok(BuildBackendOutcome::Failed { error: message })
            }
            Err(BuildError::Infrastructure(error)) => {
                Err(RpcFailure::AuthoringBackendUnavailable {
                    operation: format!("build: {error}"),
                })
            }
        }
    }

    fn runtime_type_policy(
        &self,
        request: &RuntimeTypePolicyRequest,
    ) -> Result<RuntimeTypePolicy, RpcFailure> {
        let coordinator =
            self.coordinator
                .upgrade()
                .ok_or_else(|| RpcFailure::AuthoringBackendUnavailable {
                    operation: "runtime type-policy coordinator stopped".to_owned(),
                })?;
        let target =
            coordinator
                .build_target(&request.target)
                .ok_or_else(|| RpcFailure::InvalidQuery {
                    detail: format!(
                        "runtime type-policy target {} is not published",
                        request.target
                    ),
                })?;
        if distill_build::keys::target_definition_hash(&target) != request.target_definition.0 {
            return Err(RpcFailure::InvalidQuery {
                detail: format!(
                    "runtime type-policy target definition changed for {}",
                    request.target
                ),
            });
        }
        let authority = coordinator.schema_authority().ok_or_else(|| {
            RpcFailure::AuthoringBackendUnavailable {
                operation: "runtime type-policy schema authority is not published".to_owned(),
            }
        })?;
        let project =
            authority
                .project_type(request.type_uuid)
                .ok_or_else(|| RpcFailure::InvalidQuery {
                    detail: format!(
                        "runtime type {} has no published schema authority",
                        request.type_uuid
                    ),
                })?;
        Ok(RuntimeTypePolicy {
            build_only: project.build_only,
        })
    }

    fn build_finished(&self, request: &BuildRequest) -> Result<(), RpcFailure> {
        let mut active = self
            .active_builds
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active = active
            .checked_sub(1)
            .expect("build_finished called without a matching build");
        let run_maintenance = *active == 0;
        let Some(coordinator) = self.coordinator.upgrade() else {
            return Ok(());
        };
        let store_handle = coordinator.store();
        let mut store =
            store_handle
                .lock()
                .map_err(|_| RpcFailure::AuthoringBackendUnavailable {
                    operation: "finish build: durable store mutex is poisoned".to_owned(),
                })?;
        store
            .unpin_holder(PinKind::InFlight, &build_pin_holder(request))
            .map_err(|error| RpcFailure::AuthoringBackendUnavailable {
                operation: format!("release completed build pin: {error}"),
            })?;
        if !run_maintenance {
            return Ok(());
        }
        // Keep the zero-count gate locked through maintenance. A new build
        // cannot read a candidate between the last activity check and an
        // eviction that would otherwise treat it as unreferenced.
        let sweep = store.enforce_cache_limit().map_err(|error| {
            RpcFailure::AuthoringBackendUnavailable {
                operation: format!("enforce CAS cache limit: {error}"),
            }
        })?;
        if sweep.evicted != 0 {
            store
                .compact()
                .map_err(|error| RpcFailure::AuthoringBackendUnavailable {
                    operation: format!("compact CAS after eviction: {error}"),
                })?;
        }
        Ok(())
    }
}

impl ArtifactLeaseBackend for CoordinatorBuildBackend {
    fn pin_lease(&self, holder: u64, hashes: &[[u8; 32]]) -> Result<(), String> {
        let coordinator = self
            .coordinator
            .upgrade()
            .ok_or_else(|| "build coordinator stopped".to_owned())?;
        coordinator
            .store()
            .lock()
            .map_err(|_| "durable store mutex is poisoned".to_owned())?
            .pin(PinKind::Lease, &format!("rpc-lease-{holder}"), hashes)
            .map_err(|error| error.to_string())
    }

    fn release_lease(&self, holder: u64) {
        let Some(coordinator) = self.coordinator.upgrade() else {
            return;
        };
        let store_handle = coordinator.store();
        let Ok(mut store) = store_handle.lock() else {
            return;
        };
        let _ = store.unpin_holder(PinKind::Lease, &format!("rpc-lease-{holder}"));
    }

    fn pin_pack_session(&self, holder: u64, hashes: &[[u8; 32]]) -> Result<(), String> {
        let coordinator = self
            .coordinator
            .upgrade()
            .ok_or_else(|| "build coordinator stopped".to_owned())?;
        coordinator
            .store()
            .lock()
            .map_err(|_| "durable store mutex is poisoned".to_owned())?
            .pin(
                PinKind::PackSession,
                &format!("rpc-pack-session-{holder}"),
                hashes,
            )
            .map_err(|error| error.to_string())
    }

    fn release_pack_session(&self, holder: u64) {
        let Some(coordinator) = self.coordinator.upgrade() else {
            return;
        };
        let store_handle = coordinator.store();
        let Ok(mut store) = store_handle.lock() else {
            return;
        };
        let _ = store.unpin_holder(PinKind::PackSession, &format!("rpc-pack-session-{holder}"));
    }
}

impl ArtifactPayloadBackend for CoordinatorBuildBackend {
    fn store_artifact(&self, hash: ContentHash, _payload: &ArtifactPayload) -> Result<(), String> {
        let coordinator = self
            .coordinator
            .upgrade()
            .ok_or_else(|| "build coordinator stopped".to_owned())?;
        coordinator
            .store()
            .lock()
            .map_err(|_| "durable store mutex is poisoned".to_owned())?
            .cas_read(&hash.0)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn load_artifact(&self, hash: ContentHash) -> Result<Option<ArtifactPayload>, String> {
        let coordinator = self
            .coordinator
            .upgrade()
            .ok_or_else(|| "build coordinator stopped".to_owned())?;
        let bytes = match coordinator
            .store()
            .lock()
            .map_err(|_| "durable store mutex is poisoned".to_owned())?
            .cas_read(&hash.0)
        {
            Ok(bytes) => bytes,
            Err(StoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        let view = parse_artifact(&bytes).map_err(|error| error.to_string())?;
        let structural_len = bytes
            .len()
            .checked_sub(view.blob_section.len())
            .ok_or_else(|| "artifact structural length underflow".to_owned())?;
        let blobs = (0..view.blob_table.len())
            .map(|index| {
                view.blob(index as u32)
                    .map(|blob| Arc::<[u8]>::from(blob.to_vec()))
                    .ok_or_else(|| "artifact blob table is invalid".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(ArtifactPayload {
            structural: Arc::from(bytes[..structural_len].to_vec()),
            blobs,
            load_edges: Vec::new(),
        }))
    }

    fn store_wire_tree(&self, hash: LayoutHash, bytes: &[u8]) -> Result<(), String> {
        let coordinator = self
            .coordinator
            .upgrade()
            .ok_or_else(|| "build coordinator stopped".to_owned())?;
        let stored = coordinator
            .store()
            .lock()
            .map_err(|_| "durable store mutex is poisoned".to_owned())?
            .wire_tree_read(hash)
            .map_err(|error| error.to_string())?;
        if stored != bytes {
            return Err("CAS wire tree bytes disagree with publication".to_owned());
        }
        Ok(())
    }

    fn load_wire_tree(&self, hash: LayoutHash) -> Result<Option<Arc<[u8]>>, String> {
        let coordinator = self
            .coordinator
            .upgrade()
            .ok_or_else(|| "build coordinator stopped".to_owned())?;
        match coordinator
            .store()
            .lock()
            .map_err(|_| "durable store mutex is poisoned".to_owned())?
            .wire_tree_read(hash)
        {
            Ok(bytes) => Ok(Some(Arc::from(bytes))),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }
}

fn build_pin_holder(request: &BuildRequest) -> String {
    format!(
        "rpc-build-{:02x?}-{}-{}-{:02x?}",
        request.basis.instance.0,
        request.basis.version.0,
        request.target,
        request.requested_asset.0
    )
}

#[derive(Debug, Clone)]
enum BuildError {
    Drifted(DriftedInput),
    DepthExceeded { limit: usize, chain: Vec<AssetUuid> },
    Failed(String),
    Deterministic { message: String, facts: Box<DslfV1> },
    Infrastructure(String),
}

impl BuildError {
    fn failed(error: impl std::fmt::Debug) -> Self {
        Self::Failed(format!("{error:?}"))
    }

    fn infrastructure(error: impl std::fmt::Debug) -> Self {
        Self::Infrastructure(format!("{error:?}"))
    }

    fn deterministic(message: impl Into<String>, facts: DslfV1) -> Self {
        Self::Deterministic {
            message: message.into(),
            facts: Box::new(facts),
        }
    }

    fn migration(message: impl Into<String>, facts: DslfV1) -> Self {
        Self::deterministic(message, facts)
    }
}

#[derive(Clone)]
struct LoadedAsset {
    meta: EntryMeta,
    bundle_meta: BundleMeta,
    bundle_bytes: Vec<u8>,
    entry: AssetEntry,
}

#[derive(Clone)]
struct NodePublication {
    primary: BuildOutputRow,
    outputs: BTreeMap<String, BuildOutputRow>,
    artifacts: BTreeMap<ContentHash, BuildArtifactPublication>,
    wire_trees: BTreeMap<LayoutHash, BuildWireTree>,
}

struct BuildContext {
    store: Arc<Mutex<Store>>,
    store_instance: distill_store::state::StoreInstanceId,
    drifted_input: DriftedInput,
    scanner: RootedScanner,
    authority: Arc<ProjectSchemaAuthority>,
    pipeline: PipelineSnapshot,
    registry: PipelineRegistry,
    target: Target,
    target_definition: [u8; 32],
    dylib_hash: [u8; 32],
    basis: distill_store::state::InputVersion,
    tools: PinnedToolEpoch,
    execution_root: std::path::PathBuf,
    max_depth: usize,
    visiting: BTreeSet<AssetUuid>,
    callback_chain: Vec<AssetUuid>,
    memo: BTreeMap<AssetUuid, NodePublication>,
    verify_fresh: bool,
}

struct CurrentLoadRuntime<'a> {
    store: &'a Arc<Mutex<Store>>,
    store_instance: distill_store::state::StoreInstanceId,
    drifted_input: &'a DriftedInput,
    pipeline: &'a PipelineSnapshot,
    basis: distill_store::state::InputVersion,
}

impl<'a> CurrentLoadRuntime<'a> {
    fn from_build(context: &'a BuildContext) -> Self {
        Self {
            store: &context.store,
            store_instance: context.store_instance,
            drifted_input: &context.drifted_input,
            pipeline: &context.pipeline,
            basis: context.basis,
        }
    }
}

fn lock_current_load_store<'a>(
    runtime: &'a CurrentLoadRuntime<'_>,
) -> Result<MutexGuard<'a, Store>, BuildError> {
    let store = runtime
        .store
        .lock()
        .map_err(|_| BuildError::Infrastructure("durable store mutex is poisoned".to_owned()))?;
    if store.instance_id() != runtime.store_instance || store.input_version() != runtime.basis {
        return Err(BuildError::Drifted(runtime.drifted_input.clone()));
    }
    Ok(store)
}

pub(crate) struct CurrentDiskValue {
    pub(crate) value: AuthoredValue,
    pub(crate) schema: distill_schema::ngp_schema::LogicalSchema,
    pub(crate) schema_hash: LogicalHash,
    pub(crate) lineage: distill_bundle::LineageStamp,
}

pub(crate) struct CurrentLoadService {
    store: Arc<Mutex<Store>>,
    store_instance: distill_store::state::StoreInstanceId,
    basis: distill_store::state::InputVersion,
    pipeline: PipelineSnapshot,
    source: CurrentLoadSource,
}

impl CurrentLoadService {
    pub(crate) fn capture(
        coordinator: &DaemonCoordinator,
        basis: distill_store::state::InputVersion,
    ) -> Result<Self, String> {
        let store = coordinator.store();
        let scanner = coordinator.scanner();
        let pipeline = coordinator.pipeline_snapshot();
        let (store_instance, source) = {
            let durable = store
                .lock()
                .map_err(|_| "durable store mutex is poisoned".to_owned())?;
            if durable.input_version() != basis {
                return Err(format!(
                    "disk-migration basis drifted: expected {basis:?}, observed {:?}",
                    durable.input_version()
                ));
            }
            let ready = pipeline.epoch().ok();
            let dylib_hash = ready.map(PipelineEpoch::dylib_hash);
            let source = CurrentLoadSource::capture(&durable, &scanner, ready, dylib_hash)
                .map_err(|error| format!("capture current-load inputs: {error:?}"))?;
            (durable.instance_id(), source)
        };
        Ok(Self {
            store,
            store_instance,
            basis,
            pipeline,
            source,
        })
    }

    pub(crate) fn load(
        &self,
        entry: &AssetEntry,
        bundle: &Bundle,
        bundle_path: &str,
    ) -> Result<CurrentDiskValue, String> {
        let drifted_input = DriftedInput::File(bundle_path.to_owned());
        let runtime = CurrentLoadRuntime {
            store: &self.store,
            store_instance: self.store_instance,
            drifted_input: &drifted_input,
            pipeline: &self.pipeline,
            basis: self.basis,
        };
        let (lineage, schema_hash, schema) = {
            let durable = lock_current_load_store(&runtime)
                .map_err(|error| format!("read current schema: {error:?}"))?;
            let lineage = durable
                .current_lineage_stamp(entry.type_uuid)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("type {} has no accepted lineage", entry.type_uuid))?;
            let schema_hash = lineage
                .selected_digest()
                .ok_or_else(|| "accepted lineage cursor is out of range".to_owned())?;
            let snapshot = durable
                .schema(schema_hash)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "store omitted the accepted current schema snapshot".to_owned())?;
            let schema = distill_schema::ngp_schema::verify_snapshot(&snapshot, schema_hash)
                .map_err(|error| error.to_string())?;
            (lineage, schema_hash, schema)
        };
        let mut trace = Vec::new();
        let value = load_current_entry(
            &runtime,
            entry,
            bundle,
            &schema,
            schema_hash,
            &self.source,
            &mut trace,
        )
        .map_err(|error| format!("load asset {} at current schema: {error:?}", entry.uuid))?;
        Ok(CurrentDiskValue {
            value,
            schema,
            schema_hash,
            lineage,
        })
    }
}

#[derive(Clone)]
struct PinnedToolEpoch {
    tools: BTreeMap<String, RegisteredTool>,
}

impl PinnedToolEpoch {
    fn capture(
        store: &Store,
        basis: distill_store::state::InputVersion,
    ) -> Result<Self, BuildError> {
        let hashes = store
            .tool_hashes_at(basis)
            .map_err(BuildError::infrastructure)?;
        let mut tools = BTreeMap::new();
        for (key, expected_hash) in hashes {
            let tool = store
                .tool_at(&key, basis)
                .map_err(BuildError::infrastructure)?
                .ok_or_else(|| {
                    BuildError::Infrastructure(format!(
                        "published tool {key:?} disappeared while pinning its epoch"
                    ))
                })?;
            if tool.tool_hash != expected_hash {
                return Err(BuildError::Infrastructure(format!(
                    "published tool {key:?} changed while pinning its epoch"
                )));
            }
            tools.insert(key, tool);
        }
        Ok(Self { tools })
    }
}

impl ToolEpochSnapshot for PinnedToolEpoch {
    fn tool(&self, id: &str) -> Result<Option<RegisteredTool>, StoreError> {
        Ok(self.tools.get(id).cloned())
    }
}

fn lock_build_store(context: &BuildContext) -> Result<MutexGuard<'_, Store>, BuildError> {
    let store = context
        .store
        .lock()
        .map_err(|_| BuildError::Infrastructure("durable store mutex is poisoned".to_owned()))?;
    if store.instance_id() != context.store_instance || store.input_version() != context.basis {
        return Err(BuildError::Drifted(context.drifted_input.clone()));
    }
    Ok(store)
}

fn ensure_build_basis(context: &BuildContext) -> Result<(), BuildError> {
    drop(lock_build_store(context)?);
    Ok(())
}

struct BuildProcessContext<'a> {
    context: &'a mut BuildContext,
    origin_bundle: BundleUuid,
    outputs: ProcessOutputs,
    trace: Vec<TraceOp>,
    stopped: bool,
    discarded: bool,
    cacheable: bool,
    fatal: Option<BuildError>,
}

impl<'a> BuildProcessContext<'a> {
    fn new(
        context: &'a mut BuildContext,
        origin_bundle: BundleUuid,
        parent: AssetUuid,
        declarations: distill_build::outputs::OutputDecls,
    ) -> Self {
        Self {
            context,
            origin_bundle,
            outputs: ProcessOutputs::new(parent, declarations),
            trace: Vec::new(),
            stopped: false,
            discarded: false,
            cacheable: true,
            fatal: None,
        }
    }

    fn ensure_active(&self) -> Result<(), ProcessContextError> {
        if self.stopped || self.discarded {
            Err(ProcessContextError::AttemptStopped)
        } else {
            Ok(())
        }
    }

    fn abort(
        &mut self,
        error: BuildError,
        callback_error: ProcessContextError,
    ) -> ProcessContextError {
        self.stopped = true;
        self.fatal = Some(error);
        callback_error
    }

    fn abort_observed(&mut self, failure: StableFailureFingerprint) -> ProcessContextError {
        self.abort(
            BuildError::Failed(format!("processor dependency failed: {failure:?}")),
            ProcessContextError::Observed(failure),
        )
    }

    fn abort_build(&mut self, error: BuildError) -> ProcessContextError {
        let detail = format!("{error:?}");
        self.abort(error, ProcessContextError::Failed(detail))
    }

    fn finish(self) -> (Vec<TraceOp>, bool, Option<BuildError>) {
        let error = self.fatal.or_else(|| {
            self.discarded.then(|| {
                BuildError::Infrastructure("processor attempted basis was discarded".to_owned())
            })
        });
        (self.trace, self.cacheable, error)
    }
}

impl PipelineProcessContext for BuildProcessContext<'_> {
    fn read(
        &mut self,
        asset: AssetUuid,
        expected_terminal: TypeUuid,
    ) -> Result<ProcessArtifact, ProcessContextError> {
        self.ensure_active()?;
        let source = capture_trace_source(self.context).map_err(|error| self.abort_build(error))?;

        let role = source.role_check(asset);
        self.trace.push(TraceOp::RoleCheck {
            asset,
            observed: role.clone(),
        });
        match role {
            Observed::Ok(Some(EntryRole::Runtime)) => {}
            Observed::Ok(Some(observed_role)) => {
                let failure = StableFailureFingerprint::RoleIneligible {
                    asset,
                    observed_role,
                };
                self.trace.push(TraceOp::Read {
                    asset,
                    observed: Observed::Err(failure.clone()),
                });
                return Err(self.abort_observed(failure));
            }
            Observed::Ok(None) => {
                let failure = StableFailureFingerprint::MissingRef {
                    query: Box::new(AssetQuery {
                        uuid: Some(asset),
                        ..AssetQuery::default()
                    }),
                    expected_terminal,
                };
                self.trace.push(TraceOp::Read {
                    asset,
                    observed: Observed::Err(failure.clone()),
                });
                return Err(self.abort_observed(failure));
            }
            Observed::Err(failure) => return Err(self.abort_observed(failure)),
        }

        let terminal = source.ref_check(asset, expected_terminal);
        self.trace.push(TraceOp::RefCheck {
            asset,
            expected_terminal,
            observed: terminal.clone(),
        });
        match terminal {
            Observed::Ok(Some(observed)) if observed == expected_terminal => {}
            Observed::Ok(Some(observed)) => {
                let error = ProcessContextError::WrongTerminal {
                    asset,
                    expected: expected_terminal,
                    observed,
                };
                return Err(self.abort(BuildError::Failed(error.to_string()), error));
            }
            Observed::Ok(None) => {
                let failure = StableFailureFingerprint::MissingRef {
                    query: Box::new(AssetQuery {
                        uuid: Some(asset),
                        ..AssetQuery::default()
                    }),
                    expected_terminal,
                };
                self.trace.push(TraceOp::Read {
                    asset,
                    observed: Observed::Err(failure.clone()),
                });
                return Err(self.abort_observed(failure));
            }
            Observed::Err(failure) => return Err(self.abort_observed(failure)),
        }

        let artifact =
            build_process_artifact(self.context, asset).map_err(|error| self.abort_build(error))?;
        self.trace.push(TraceOp::Read {
            asset,
            observed: Observed::Ok(artifact.content_hash),
        });
        Ok(artifact)
    }

    fn read_path(
        &mut self,
        path: &str,
        expected_terminal: TypeUuid,
    ) -> Result<ProcessArtifact, ProcessContextError> {
        self.ensure_active()?;
        let path = match normalize_path(path) {
            Ok(path) => path,
            Err(detail) => {
                let callback_error = ProcessContextError::InvalidQuery(detail.clone());
                return Err(self.abort(BuildError::Failed(detail.to_string()), callback_error));
            }
        };
        let source = capture_trace_source(self.context).map_err(|error| self.abort_build(error))?;
        let observed = source.resolve(&path);
        self.trace.push(TraceOp::Resolve {
            path: path.clone(),
            observed: observed.clone(),
        });
        match observed {
            Observed::Ok(Some(asset)) => self.read(asset, expected_terminal),
            Observed::Ok(None) => {
                let failure = StableFailureFingerprint::MissingRef {
                    query: Box::new(AssetQuery {
                        bundle_path: Some(path),
                        ..AssetQuery::default()
                    }),
                    expected_terminal,
                };
                Err(self.abort_observed(failure))
            }
            Observed::Err(failure) => Err(self.abort_observed(failure)),
        }
    }

    fn query(&mut self, query: &AssetQuery) -> Result<Vec<AssetUuid>, ProcessContextError> {
        self.ensure_active()?;
        let query = match query.clone().close(Some(self.origin_bundle)) {
            Ok(query) => query,
            Err(detail) => {
                let callback_error = ProcessContextError::InvalidQuery(detail.clone());
                return Err(self.abort(BuildError::Failed(detail.to_string()), callback_error));
            }
        };
        let source = capture_trace_source(self.context).map_err(|error| self.abort_build(error))?;
        let observed = source.query(&query);
        self.trace.push(TraceOp::Query {
            query: Box::new(query.clone()),
            observed: observed.clone(),
        });
        match observed {
            Observed::Ok(_) => Ok(source.query_results(&query)),
            Observed::Err(failure) => Err(self.abort_observed(failure)),
        }
    }

    fn target(&self) -> Result<&Target, ProcessContextError> {
        self.ensure_active()?;
        Ok(&self.context.target)
    }

    fn outputs(&self) -> Result<ProcessOutputs, ProcessContextError> {
        self.ensure_active()?;
        Ok(self.outputs.clone())
    }

    fn run_tool(
        &mut self,
        id: &str,
        args: &[String],
        stdin: &[u8],
    ) -> Result<distill_build::tool::ToolOutput, distill_build::tool::ToolRunError> {
        if self.ensure_active().is_err() {
            return Err(distill_build::tool::ToolRunError::AttemptStopped);
        }
        let mut tool = ProcessContext::new(
            &self.context.tools,
            ToolRuntimeBinding {
                execution_root: &self.context.execution_root,
            },
        );
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();
        let result = tool.run_tool(id, &args, stdin);
        match tool.into_trace() {
            Ok((trace, cacheable)) => {
                self.trace.extend(trace);
                self.cacheable &= cacheable;
            }
            Err(_) => {
                self.discarded = true;
                self.fatal = Some(BuildError::Infrastructure(
                    "tool launch discarded the processor attempted basis".to_owned(),
                ));
            }
        }
        if let Err(error) = &result {
            self.stopped = true;
            if self.fatal.is_none() {
                self.fatal = Some(match error {
                    distill_build::tool::ToolRunError::Stable(failure) => {
                        BuildError::Failed(format!("tool dependency failed: {failure:?}"))
                    }
                    _ => BuildError::Infrastructure(format!("tool launch failed: {error:?}")),
                });
            }
        }
        result
    }
}

fn build_process_artifact(
    context: &mut BuildContext,
    asset: AssetUuid,
) -> Result<ProcessArtifact, BuildError> {
    let derived = lock_build_store(context)?
        .resolve_child(asset)
        .map_err(BuildError::failed)?;
    let (publication, output_key) = match derived {
        Some((parent, output_key)) => (build_asset(context, parent)?, output_key),
        None => (build_asset(context, asset)?, String::new()),
    };
    let selected = publication.outputs.get(&output_key).ok_or_else(|| {
        BuildError::Failed(format!(
            "built dependency {asset} omitted output {output_key:?}"
        ))
    })?;
    let artifact = publication
        .artifacts
        .get(&selected.content_hash)
        .ok_or_else(|| {
            BuildError::Infrastructure("built dependency payload is absent".to_owned())
        })?;
    Ok(ProcessArtifact {
        asset,
        content_hash: selected.content_hash,
        encoded_type: selected.encoded_type,
        terminal_type: selected.terminal_type,
        structural: Arc::clone(&artifact.payload.structural),
        blobs: artifact.payload.blobs.clone(),
    })
}

fn build_with_runtime(
    coordinator: &DaemonCoordinator,
    request: &BuildRequest,
) -> Result<BuildPublication, BuildError> {
    build_with_runtime_mode(coordinator, request, false)
}

fn build_with_runtime_mode(
    coordinator: &DaemonCoordinator,
    request: &BuildRequest,
    verify_fresh: bool,
) -> Result<BuildPublication, BuildError> {
    let authority = coordinator.schema_authority().ok_or_else(|| {
        BuildError::Failed("project schema authority is not published".to_owned())
    })?;
    let target = coordinator.build_target(&request.target).ok_or_else(|| {
        BuildError::Failed(format!(
            "build target {:?} is not published",
            request.target
        ))
    })?;
    let observed_target =
        distill_rpc::TargetDefinitionHash(distill_build::keys::target_definition_hash(&target));
    if observed_target != request.target_definition {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    let pipeline = coordinator.pipeline_snapshot();
    let epoch = pipeline
        .epoch()
        .map_err(|poison| BuildError::Failed(poison.to_string()))?;
    let dylib_hash = epoch.dylib_hash();
    let registry = PipelineRegistry::new(
        epoch
            .processor_descriptors()
            .into_iter()
            .map(|descriptor| {
                ProcessorRegistration::new(
                    &descriptor.id,
                    descriptor.version,
                    descriptor.input,
                    descriptor.selector,
                    descriptor.outputs,
                    dylib_hash,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(BuildError::failed)?,
    )
    .map_err(BuildError::failed)?;
    let requested_chain = registry
        .chain(request.entry.type_uuid, &target)
        .map_err(BuildError::failed)?;
    let expected_requested_type = if request.output_key.is_empty() {
        requested_chain.terminal
    } else {
        requested_chain
            .extras
            .get(&request.output_key)
            .copied()
            .ok_or(BuildError::Drifted(DriftedInput::Asset(
                request.requested_asset,
            )))?
    };
    if request.entry.terminal_type != requested_chain.terminal
        || request.requested_terminal_type != expected_requested_type
    {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    let store_handle = coordinator.store();
    let scanner = coordinator.scanner();
    let (root, tools, execution_root) = {
        let store = store_handle.lock().map_err(|_| {
            BuildError::Infrastructure("durable store mutex is poisoned".to_owned())
        })?;
        if store.instance_id() != request.basis.instance
            || store.input_version() != request.basis.version
        {
            return Err(BuildError::Drifted(request.drifted_input.clone()));
        }
        let root = load_asset(&store, &scanner, request.entry.uuid)?;
        let tools = PinnedToolEpoch::capture(&store, request.basis.version)?;
        let execution_root = store.state_path().join("tool-runs");
        (root, tools, execution_root)
    };
    verify_request_entry(request, &root, &authority)?;

    let mut context = BuildContext {
        store: store_handle,
        store_instance: request.basis.instance,
        drifted_input: request.drifted_input.clone(),
        scanner,
        authority,
        pipeline,
        registry,
        target,
        target_definition: request.target_definition.0,
        dylib_hash,
        basis: request.basis.version,
        tools,
        execution_root,
        max_depth: coordinator.operational_configuration().max_dependency_depth,
        visiting: BTreeSet::new(),
        callback_chain: Vec::new(),
        memo: BTreeMap::new(),
        verify_fresh,
    };
    let root = build_asset(&mut context, request.entry.uuid)?;
    ensure_build_basis(&context)?;
    let selected = root
        .outputs
        .get(&request.output_key)
        .ok_or(BuildError::Drifted(DriftedInput::Asset(
            request.requested_asset,
        )))?;
    Ok(BuildPublication {
        root_content_hash: selected.content_hash,
        artifacts: root.artifacts.into_values().collect(),
        wire_trees: root.wire_trees.into_values().collect(),
    })
}

#[cfg(test)]
fn build(
    coordinator: &Arc<DaemonCoordinator>,
    request: &BuildRequest,
) -> Result<BuildPublication, BuildError> {
    let request = request.clone();
    let job_coordinator = Arc::clone(coordinator);
    coordinator.run_scheduled(WorkClass::Interactive, move || {
        build_with_runtime(&job_coordinator, &request)
    })
}

pub(crate) fn doctor_verify_builds(
    coordinator: &Arc<DaemonCoordinator>,
    requests: &[BuildRequest],
) -> Result<Vec<String>, String> {
    let mut defects = Vec::new();
    for request in requests {
        let run = |request: BuildRequest, verify_fresh| {
            let job_coordinator = Arc::clone(coordinator);
            coordinator.run_scheduled(WorkClass::Batch, move || {
                build_with_runtime_mode(&job_coordinator, &request, verify_fresh)
            })
        };
        let published = run(request.clone(), false);
        let first = run(request.clone(), true);
        let second = run(request.clone(), true);
        match (published, first, second) {
            (Ok(published), Ok(first), Ok(second))
                if published == first && first == second => {}
            (Ok(published), Ok(first), Ok(second)) if first == second => defects.push(format!(
                "asset {} target {:?} fresh rebuild differs from the published artifact set: published root {}, rebuilt root {}",
                request.requested_asset,
                request.target,
                published.root_content_hash,
                first.root_content_hash
            )),
            (Ok(_), Ok(_), Ok(_)) => defects.push(format!(
                "asset {} target {:?} produced different fresh rebuild publications",
                request.requested_asset, request.target
            )),
            (Err(published), Err(first), Err(second)) => {
                let published = format!("{published:?}");
                let first = format!("{first:?}");
                let second = format!("{second:?}");
                if published == first && first == second {
                    defects.push(format!(
                        "asset {} target {:?} failed reproducibly: {first}",
                        request.requested_asset, request.target
                    ));
                } else {
                    defects.push(format!(
                        "asset {} target {:?} produced different rebuild failures: {first}; {second}",
                        request.requested_asset, request.target
                    ));
                }
            }
            (published, first, second) => defects.push(format!(
                "asset {} target {:?} changed published/fresh rebuild outcome: {published:?}; {first:?}; {second:?}",
                request.requested_asset, request.target
            )),
        }
    }
    Ok(defects)
}

/// Finish §10 tag indexing against a namespace that has advanced durably but
/// is still hidden behind the coordinator's RPC publication lock.
pub(crate) fn refine_published_tag_index(
    store_handle: Arc<Mutex<Store>>,
    scanner: RootedScanner,
    authority: Arc<ProjectSchemaAuthority>,
    pipeline: PipelineSnapshot,
    targets: &BTreeMap<String, Target>,
    max_depth: usize,
    fallback_assets: &BTreeMap<AssetUuid, BundleUuid>,
) -> PublishedTagIndex {
    try_refine_published_tag_index(
        store_handle,
        scanner,
        authority,
        pipeline,
        targets,
        max_depth,
        None,
    )
    .unwrap_or_else(|_| PublishedTagIndex::conservatively_poisoned(fallback_assets))
}

/// Reindex only identities whose authored rows changed in the same input
/// publication. Deleted identities become bounded removals; unrelated tag
/// rows and cached traces are not enumerated.
pub(crate) fn refine_published_tag_index_incremental(
    store_handle: Arc<Mutex<Store>>,
    scanner: RootedScanner,
    authority: Arc<ProjectSchemaAuthority>,
    pipeline: PipelineSnapshot,
    targets: &BTreeMap<String, Target>,
    max_depth: usize,
    affected: &BTreeMap<AssetUuid, Option<BundleUuid>>,
) -> PublishedTagIndex {
    let current = affected
        .iter()
        .filter_map(|(asset, bundle)| bundle.map(|bundle| (*asset, bundle)))
        .collect::<BTreeMap<_, _>>();
    let assets = current.keys().copied().collect::<Vec<_>>();
    let mut indexed = try_refine_published_tag_index(
        store_handle,
        scanner,
        authority,
        pipeline,
        targets,
        max_depth,
        Some(assets),
    )
    .unwrap_or_else(|_| PublishedTagIndex::conservatively_poisoned(&current));
    indexed.removed = affected
        .iter()
        .filter_map(|(asset, bundle)| bundle.is_none().then_some(*asset))
        .collect();
    indexed
}

fn try_refine_published_tag_index(
    store_handle: Arc<Mutex<Store>>,
    scanner: RootedScanner,
    authority: Arc<ProjectSchemaAuthority>,
    pipeline: PipelineSnapshot,
    targets: &BTreeMap<String, Target>,
    max_depth: usize,
    requested_assets: Option<Vec<AssetUuid>>,
) -> Result<PublishedTagIndex, String> {
    let tag_epoch = authority.source_hash();
    let (store_instance, basis, assets, tools, execution_root) = {
        let store = store_handle
            .lock()
            .map_err(|_| "durable store mutex is poisoned".to_owned())?;
        (
            store.instance_id(),
            store.input_version(),
            match requested_assets {
                Some(assets) => assets,
                None => store
                    .all_asset_ids()
                    .map_err(|error| format!("enumerate tag-index assets: {error}"))?,
            },
            PinnedToolEpoch::capture(&store, store.input_version())
                .map_err(|error| format!("pin tag-index tools: {error:?}"))?,
            store.state_path().join("tag-index-runs"),
        )
    };
    let epoch = pipeline.epoch();
    let ready = epoch.ok();
    let target = targets.values().next().cloned();
    let registry = match (ready, target.as_ref()) {
        (Some(epoch), Some(_)) => Some(
            PipelineRegistry::new(
                epoch
                    .processor_descriptors()
                    .into_iter()
                    .map(|descriptor| {
                        ProcessorRegistration::new(
                            &descriptor.id,
                            descriptor.version,
                            descriptor.input,
                            descriptor.selector,
                            descriptor.outputs,
                            epoch.dylib_hash(),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| format!("tag-index pipeline map: {error:?}"))?,
            )
            .map_err(|error| format!("tag-index pipeline map: {error:?}"))?,
        ),
        _ => None,
    };

    let mut tags = BTreeMap::new();
    let mut poisons = BTreeMap::new();
    let mut updates = Vec::with_capacity(assets.len());
    if let (Some(epoch), Some(target), Some(registry)) = (ready, target, registry) {
        let dylib_hash = epoch.dylib_hash();
        let target_definition = distill_build::keys::target_definition_hash(&target);
        let mut context = BuildContext {
            store: Arc::clone(&store_handle),
            store_instance,
            drifted_input: DriftedInput::Dylib,
            scanner: scanner.clone(),
            authority: Arc::clone(&authority),
            pipeline: pipeline.clone(),
            registry,
            target,
            target_definition,
            dylib_hash,
            basis,
            tools,
            execution_root,
            max_depth,
            visiting: BTreeSet::new(),
            callback_chain: Vec::new(),
            memo: BTreeMap::new(),
            verify_fresh: false,
        };
        for asset in assets {
            let indexed = index_one_tag_entry(&mut context, asset, tag_epoch);
            match indexed {
                Ok(update) => {
                    tags.insert(asset, update.tags.clone());
                    updates.push(update);
                }
                Err((bundle, error, trace, migrated)) => {
                    poisons.insert(asset, bundle);
                    updates.push(TagIndexUpdate {
                        asset,
                        tags: BTreeMap::new(),
                        tag_epoch,
                        planner_version: migrated.then_some(MIGRATION_PLANNER_VERSION),
                        dylib_hash: migrated.then_some(dylib_hash),
                        trace,
                        poison: Some(error),
                    });
                }
            }
        }
    } else {
        let unavailable = pipeline.epoch().err().map_or_else(
            || "no build target is published".to_owned(),
            |error| error.to_string(),
        );
        let store = store_handle
            .lock()
            .map_err(|_| "durable store mutex is poisoned".to_owned())?;
        for asset in assets {
            let direct = (|| {
                let loaded = load_asset(&store, &scanner, asset)
                    .map_err(|error| (BundleUuid([0; 16]), format!("{error:?}"), false))?;
                let bundle = loaded.meta.bundle;
                let project = authority
                    .project_type(loaded.entry.type_uuid)
                    .ok_or_else(|| {
                        (
                            bundle,
                            format!(
                                "type {} has no project schema authority",
                                loaded.entry.type_uuid
                            ),
                            false,
                        )
                    })?;
                let migrated = loaded.entry.schema_hash != project.logical_hash;
                if migrated {
                    return Err((bundle, format!("tag load unavailable: {unavailable}"), true));
                }
                distill_schema::extract_search_tags(
                    authority.schema(),
                    project.schema_type,
                    &loaded.entry.data,
                )
                .map_err(|error| (bundle, error.to_string(), false))
            })();
            match direct {
                Ok(extracted) => {
                    let extracted = extracted
                        .into_iter()
                        .map(|(name, value)| (name, Some(value)))
                        .collect::<BTreeMap<_, _>>();
                    tags.insert(asset, extracted.clone());
                    updates.push(TagIndexUpdate {
                        asset,
                        tags: extracted,
                        tag_epoch,
                        planner_version: None,
                        dylib_hash: None,
                        trace: Vec::new(),
                        poison: None,
                    });
                }
                Err((bundle, error, migrated)) => {
                    poisons.insert(asset, bundle);
                    updates.push(TagIndexUpdate {
                        asset,
                        tags: BTreeMap::new(),
                        tag_epoch,
                        planner_version: migrated.then_some(MIGRATION_PLANNER_VERSION),
                        dylib_hash: None,
                        trace: Vec::new(),
                        poison: Some(error),
                    });
                }
            }
        }
    }
    store_handle
        .lock()
        .map_err(|_| "durable store mutex is poisoned".to_owned())?
        .refine_unpublished_tag_index(basis, &updates)
        .map_err(|error| format!("publish tag index: {error}"))?;
    Ok(PublishedTagIndex {
        tags,
        poisons,
        removed: BTreeSet::new(),
    })
}

fn index_one_tag_entry(
    context: &mut BuildContext,
    asset: AssetUuid,
    tag_epoch: [u8; 32],
) -> Result<TagIndexUpdate, (BundleUuid, String, Vec<u8>, bool)> {
    let loaded = {
        let store = lock_build_store(context)
            .map_err(|error| (BundleUuid([0; 16]), format!("{error:?}"), Vec::new(), false))?;
        load_asset(&store, &context.scanner, asset)
            .map_err(|error| (BundleUuid([0; 16]), format!("{error:?}"), Vec::new(), false))?
    };
    let bundle = loaded.meta.bundle;
    let Some(project) = context
        .authority
        .project_type(loaded.entry.type_uuid)
        .cloned()
    else {
        return Err((
            bundle,
            format!(
                "type {} has no project schema authority",
                loaded.entry.type_uuid
            ),
            Vec::new(),
            false,
        ));
    };
    let migrated = loaded.entry.schema_hash != project.logical_hash;
    let source = capture_trace_source(context)
        .map_err(|error| (bundle, format!("{error:?}"), Vec::new(), migrated))?;
    let mut trace = Vec::new();
    let current =
        load_current_value(context, &loaded, &project, &source, &mut trace).map_err(|error| {
            (
                bundle,
                format!("{error:?}"),
                trace_payload_bytes(&trace),
                migrated,
            )
        })?;
    let extracted = distill_schema::extract_search_tags(
        context.authority.schema(),
        project.schema_type,
        &current,
    )
    .map_err(|error| {
        (
            bundle,
            error.to_string(),
            trace_payload_bytes(&trace),
            migrated,
        )
    })?
    .into_iter()
    .map(|(name, value)| (name, Some(value)))
    .collect();
    Ok(TagIndexUpdate {
        asset,
        tags: extracted,
        tag_epoch,
        planner_version: migrated.then_some(MIGRATION_PLANNER_VERSION),
        dylib_hash: migrated.then_some(context.dylib_hash),
        trace: trace_payload_bytes(&trace),
        poison: None,
    })
}

fn verify_request_entry(
    request: &BuildRequest,
    loaded: &LoadedAsset,
    authority: &ProjectSchemaAuthority,
) -> Result<(), BuildError> {
    let requested = &request.entry;
    if request.output_key.is_empty() {
        if request.requested_asset != requested.uuid {
            return Err(BuildError::Drifted(request.drifted_input.clone()));
        }
    } else if request.requested_asset != AssetUuid::v5(requested.uuid, &request.output_key) {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    if requested.bundle != loaded.meta.bundle
        || requested.local_id != loaded.meta.local_id
        || requested.normalized_path != loaded.bundle_meta.path
        || requested.type_uuid != loaded.meta.type_uuid
        || requested.schema_hash != loaded.meta.logical_hash
    {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    let decoded = decode_authoring_payload(
        requested.schema_hash,
        &requested.logical_schema,
        &requested.value,
    )
    .map_err(BuildError::failed)?;
    if decoded != loaded.entry.data {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    authority.project_type(requested.type_uuid).ok_or_else(|| {
        BuildError::Failed(format!(
            "type {} has no project schema authority",
            requested.type_uuid
        ))
    })?;
    Ok(())
}

fn build_asset(
    context: &mut BuildContext,
    asset: AssetUuid,
) -> Result<NodePublication, BuildError> {
    enum Frame {
        Enter {
            asset: AssetUuid,
            chain: Vec<AssetUuid>,
        },
        Assemble(PendingNodePublication),
    }

    let mut chain = context.callback_chain.clone();
    if chain.last().copied() != Some(asset) {
        chain.push(asset);
    }
    let mut stack = vec![Frame::Enter { asset, chain }];
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter { asset, chain } => {
                if chain.len() > context.max_depth {
                    return Err(BuildError::DepthExceeded {
                        limit: context.max_depth,
                        chain,
                    });
                }
                if context.memo.contains_key(&asset) {
                    continue;
                }
                if !context.visiting.insert(asset) {
                    return Err(BuildError::Failed(format!(
                        "strong-reference cycle reaches assets {chain:?}"
                    )));
                }
                let previous_chain = std::mem::replace(&mut context.callback_chain, chain.clone());
                let pending = match build_asset_inner(context, asset) {
                    Ok(pending) => pending,
                    Err(error) => {
                        context.callback_chain = previous_chain;
                        context.visiting.remove(&asset);
                        return Err(error);
                    }
                };
                context.callback_chain = previous_chain;
                let dependencies = pending_dependency_parents(context, &pending)?;
                stack.push(Frame::Assemble(pending));
                for dependency in dependencies.into_iter().rev() {
                    let mut dependency_chain = chain.clone();
                    dependency_chain.push(dependency);
                    stack.push(Frame::Enter {
                        asset: dependency,
                        chain: dependency_chain,
                    });
                }
            }
            Frame::Assemble(pending) => {
                let asset = pending.asset;
                let publication = match assemble_pending(context, pending) {
                    Ok(publication) => publication,
                    Err(error) => {
                        context.visiting.remove(&asset);
                        return Err(error);
                    }
                };
                context.visiting.remove(&asset);
                context.memo.insert(asset, publication);
            }
        }
    }
    context.memo.get(&asset).cloned().ok_or_else(|| {
        BuildError::Infrastructure("iterative build lost its root result".to_owned())
    })
}

fn build_asset_inner(
    context: &mut BuildContext,
    asset: AssetUuid,
) -> Result<PendingNodePublication, BuildError> {
    let loaded = {
        let store = lock_build_store(context)?;
        load_asset(&store, &context.scanner, asset)?
    };
    if loaded.meta.authoring_only {
        return Err(BuildError::Failed(format!(
            "authoring-only asset {asset} cannot enter a runtime build closure"
        )));
    }
    let project = context
        .authority
        .project_type(loaded.entry.type_uuid)
        .cloned()
        .ok_or_else(|| {
            BuildError::Failed(format!(
                "type {} has no project schema authority",
                loaded.entry.type_uuid
            ))
        })?;
    let validators_registered = context
        .pipeline
        .epoch()
        .map_err(|poison| BuildError::Failed(poison.to_string()))?
        .validator_descriptors()
        .iter()
        .any(|descriptor| descriptor.asset_type == loaded.entry.type_uuid);
    let chain = context
        .registry
        .chain(loaded.entry.type_uuid, &context.target)
        .map_err(BuildError::failed)?;
    validate_runtime_chain(&context.authority, &chain)?;
    let (bytes, references, current_value) = encode_or_hydrate(
        context,
        &loaded,
        &project,
        chain.terminal,
        validators_registered.then_some(context.dylib_hash),
    )?;
    let imported = EncodedNodeOutput {
        output_key: String::new(),
        asset,
        authored_type: loaded.entry.type_uuid,
        encoded_type: loaded.entry.type_uuid,
        terminal_type: chain.terminal,
        project: project.clone(),
        bytes,
        references,
    };
    let outputs = if chain.stages.is_empty() {
        vec![imported]
    } else {
        process_chain(context, &loaded, &project, &chain, imported, current_value)?
    };
    prepare_outputs(context, asset, outputs)
}

fn validate_runtime_chain(
    authority: &ProjectSchemaAuthority,
    chain: &PipelineChain,
) -> Result<(), BuildError> {
    let types = std::iter::once(chain.authored)
        .chain(chain.stages.iter().flat_map(|stage| {
            std::iter::once(stage.registration.outputs.primary)
                .chain(stage.registration.outputs.extras.values().copied())
        }))
        .chain(std::iter::once(chain.terminal))
        .chain(chain.extras.values().copied());
    for type_uuid in types {
        let project = authority.project_type(type_uuid).ok_or_else(|| {
            BuildError::Failed(format!("runtime type {type_uuid} has no schema authority"))
        })?;
        if project.build_only {
            return Err(BuildError::Failed(format!(
                "build-only type {type_uuid} cannot enter a runtime build closure"
            )));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct EncodedNodeOutput {
    output_key: String,
    asset: AssetUuid,
    authored_type: TypeUuid,
    encoded_type: TypeUuid,
    terminal_type: TypeUuid,
    project: ProjectTypeAuthority,
    bytes: Vec<u8>,
    references: Vec<distill_wire::encode::EncodedReference>,
}

struct PendingArtifact {
    content_hash: ContentHash,
    structural: Arc<[u8]>,
    blobs: Vec<Arc<[u8]>>,
    load_edges: Vec<ServedLoadEdge>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BuildOutputRow {
    asset: AssetUuid,
    content_hash: ContentHash,
    authored_type: TypeUuid,
    encoded_type: TypeUuid,
    terminal_type: TypeUuid,
    load_edges: Vec<ServedLoadEdge>,
}

struct PendingNodePublication {
    asset: AssetUuid,
    local_assets: BTreeSet<AssetUuid>,
    rows: BTreeMap<String, BuildOutputRow>,
    pending: Vec<PendingArtifact>,
    wire_trees: BTreeMap<LayoutHash, BuildWireTree>,
}

type HydratedProcessorStage = (Vec<EncodedNodeOutput>, BTreeMap<String, Vec<u8>>);
type EncodedBuildImport = (
    Vec<u8>,
    Vec<distill_wire::encode::EncodedReference>,
    Option<AuthoredValue>,
);

fn process_chain(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    chain: &PipelineChain,
    imported: EncodedNodeOutput,
    current_value: Option<AuthoredValue>,
) -> Result<Vec<EncodedNodeOutput>, BuildError> {
    if !context.verify_fresh {
        if let Some(outputs) = hydrate_complete_chain(context, loaded, chain, &imported)? {
            return Ok(outputs);
        }
    }

    let mut current_value = match current_value {
        Some(value) => value,
        None => {
            let trace_source = capture_trace_source(context)?;
            let mut trace = Vec::new();
            load_current_value(context, loaded, project, &trace_source, &mut trace)?
        }
    };
    let mut current_hash = ContentHash(*blake3::hash(&imported.bytes).as_bytes());
    let mut extras = BTreeMap::<String, EncodedNodeOutput>::new();
    let mut final_primary = None;
    for stage in &chain.stages {
        let static_inputs = processor_static_inputs(context, loaded, stage, current_hash)?;
        let cached = hydrate_processor_stage(context, loaded, chain, stage, &static_inputs)?;
        let (next_value, encoded) = run_processor_stage(
            context,
            loaded,
            chain,
            stage,
            &static_inputs,
            current_value,
            cached,
        )?;
        let primary = encoded
            .iter()
            .find(|output| output.output_key.is_empty())
            .cloned()
            .expect("closed processor output always has a primary");
        current_hash = ContentHash(*blake3::hash(&primary.bytes).as_bytes());
        current_value = next_value;
        for output in encoded {
            if output.output_key.is_empty() {
                final_primary = Some(output);
            } else if extras.insert(output.output_key.clone(), output).is_some() {
                return Err(BuildError::Failed(
                    "processor chain produced a duplicate extra key".to_owned(),
                ));
            }
        }
    }
    let mut outputs = vec![final_primary.expect("nonempty chain has a final primary")];
    outputs.extend(extras.into_values());
    Ok(outputs)
}

fn run_processor_stage(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    chain: &PipelineChain,
    stage: &PipelineStage,
    static_inputs: &StaticInputs,
    current_value: AuthoredValue,
    cached: Option<HydratedProcessorStage>,
) -> Result<(AuthoredValue, Vec<EncodedNodeOutput>), BuildError> {
    std::fs::create_dir_all(&context.execution_root).map_err(BuildError::infrastructure)?;
    let epoch = context
        .pipeline
        .epoch()
        .map_err(|poison| BuildError::Failed(poison.to_string()))?
        .clone();
    let verify_fresh = context.verify_fresh;
    let mut process_context = BuildProcessContext::new(
        context,
        loaded.meta.bundle,
        loaded.entry.uuid,
        stage.registration.outputs.clone(),
    );
    if verify_fresh {
        process_context.cacheable = false;
    }
    let outcome =
        epoch.invoke_processor(&stage.registration.id, current_value, &mut process_context);
    let (mut trace, cacheable, context_error) = process_context.finish();
    if let Some(error) = context_error {
        return Err(error);
    }
    let products = match outcome {
        Ok(products) => products,
        Err(CallbackInvokeError::Rejected(error)) => {
            let facts = DslfV1::Processor {
                asset: loaded.entry.uuid,
                processor_id: stage.registration.id.clone(),
                processor_version: stage.registration.version,
                stage: stage.index,
                build_error_code: error.code,
            };
            if cacheable {
                commit_processor_failure(context, loaded, static_inputs, &trace, Some(&facts))?;
            }
            return Err(BuildError::Failed(format!(
                "processor {:?} rejected asset {} with code {}: {}",
                stage.registration.id, loaded.entry.uuid, error.code, error.message
            )));
        }
        Err(CallbackInvokeError::OutputBinding(failure)) => {
            let facts = DslfV1::OutputBinding {
                asset: loaded.entry.uuid,
                processor_id: stage.registration.id.clone(),
                processor_version: stage.registration.version,
                stage: stage.index,
                failure,
            };
            if cacheable {
                commit_processor_failure(context, loaded, static_inputs, &trace, Some(&facts))?;
            }
            return Err(BuildError::deterministic(
                "processor output binding failed",
                facts,
            ));
        }
        Err(error) => return Err(BuildError::failed(error)),
    };
    if products.primary.is_none() {
        return Err(BuildError::Failed(format!(
            "processor {:?} omitted its primary output",
            stage.registration.id
        )));
    }
    let encoded =
        match encode_processor_products(context, loaded, chain, stage, &products, &mut trace) {
            Ok(encoded) => encoded,
            Err(error) => {
                let facts = match &error {
                    BuildError::Deterministic { facts, .. } => Some(facts.as_ref()),
                    _ => None,
                };
                if cacheable && (trace.last().is_some_and(TraceOp::failed) || facts.is_some()) {
                    commit_processor_failure(context, loaded, static_inputs, &trace, facts)?;
                }
                return Err(error);
            }
        };
    if let Some((cached_outputs, cached_debug)) = cached.as_ref() {
        ensure_cached_stage_matches(cached_outputs, cached_debug, &encoded, &products.debug)?;
    } else if cacheable {
        commit_processor_stage(
            context,
            loaded,
            static_inputs,
            &trace,
            &encoded,
            &products.debug,
        )?;
    }
    let next = products
        .primary
        .map(|product| product.value)
        .ok_or_else(|| {
            BuildError::Infrastructure("processor result omitted its primary".to_owned())
        })?;
    Ok((next, encoded))
}

fn hydrate_complete_chain(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    chain: &PipelineChain,
    imported: &EncodedNodeOutput,
) -> Result<Option<Vec<EncodedNodeOutput>>, BuildError> {
    let mut input_hash = ContentHash(*blake3::hash(&imported.bytes).as_bytes());
    let mut extras = BTreeMap::<String, EncodedNodeOutput>::new();
    let mut final_primary = None;
    for stage in &chain.stages {
        let static_inputs = processor_static_inputs(context, loaded, stage, input_hash)?;
        let Some((outputs, _debug)) =
            hydrate_processor_stage(context, loaded, chain, stage, &static_inputs)?
        else {
            return Ok(None);
        };
        let primary = outputs
            .iter()
            .find(|output| output.output_key.is_empty())
            .cloned()
            .expect("hydrated stage was shape checked");
        input_hash = ContentHash(*blake3::hash(&primary.bytes).as_bytes());
        for output in outputs {
            if output.output_key.is_empty() {
                final_primary = Some(output);
            } else {
                extras.insert(output.output_key.clone(), output);
            }
        }
    }
    let mut outputs = vec![final_primary.expect("nonempty chain has a cached final primary")];
    outputs.extend(extras.into_values());
    Ok(Some(outputs))
}

fn processor_static_inputs(
    context: &BuildContext,
    loaded: &LoadedAsset,
    stage: &PipelineStage,
    input_hash: ContentHash,
) -> Result<StaticInputs, BuildError> {
    let mut output_hashes = Vec::with_capacity(stage.registration.outputs.extras.len() + 1);
    for (key, type_uuid) in std::iter::once((String::new(), stage.registration.outputs.primary))
        .chain(
            stage
                .registration
                .outputs
                .extras
                .iter()
                .map(|(key, ty)| (key.clone(), *ty)),
        )
    {
        let authority = context.authority.project_type(type_uuid).ok_or_else(|| {
            BuildError::Failed(format!(
                "processor output type {type_uuid} has no schema authority"
            ))
        })?;
        output_hashes.push(OutputHash {
            key,
            logical: authority.logical_hash,
            layout: authority.layout_hash,
        });
    }
    Ok(StaticInputs {
        asset: loaded.entry.uuid,
        stage: stage.index,
        input_hash,
        target_def_hash: context.target_definition,
        processor_id: stage.registration.id.clone(),
        processor_version: stage.registration.version,
        dylib_hash: context.dylib_hash,
        output_hashes,
        artifact_format_version: ARTIFACT_FORMAT_VERSION,
    })
}

fn hydrate_processor_stage(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    chain: &PipelineChain,
    stage: &PipelineStage,
    static_inputs: &StaticInputs,
) -> Result<Option<HydratedProcessorStage>, BuildError> {
    let key = static_inputs_digest(static_inputs);
    preload_persisted_reads(context, KeyKind::Processor, &key, loaded.entry.uuid)?;
    let trace_source = capture_trace_source(context)?;
    let hit = if context.verify_fresh {
        None
    } else {
        let mut store = lock_build_store(context)?;
        lookup_persisted_candidate(
            &mut store,
            KeyKind::Processor,
            &key,
            loaded.entry.uuid,
            &trace_source,
        )
        .map_err(BuildError::infrastructure)?
    };
    let Some(hit) = hit else {
        return Ok(None);
    };
    match hit.outcome {
        PersistedOutcome::Failure { cause } => Err(BuildError::Failed(format!(
            "cached processor failure: {cause:?}"
        ))),
        PersistedOutcome::Success { outputs, aux } => {
            let expected_keys = std::iter::once(String::new())
                .chain(stage.registration.outputs.extras.keys().cloned())
                .collect::<BTreeSet<_>>();
            let observed_keys = outputs
                .iter()
                .map(|output| output.output_key.clone())
                .collect::<BTreeSet<_>>();
            if outputs.len() != expected_keys.len() || observed_keys != expected_keys {
                return Err(BuildError::Failed(
                    "cached processor result has an invalid closed output table".to_owned(),
                ));
            }
            let mut hydrated = Vec::with_capacity(outputs.len());
            for output in outputs {
                let persisted_type_uuids = output.type_uuids;
                let (asset, authored_type, encoded_type, terminal_type) =
                    expected_output_identity(loaded, chain, stage, &output.output_key)?;
                let project = context
                    .authority
                    .project_type(encoded_type)
                    .cloned()
                    .ok_or_else(|| {
                        BuildError::Failed(format!(
                            "cached processor output type {encoded_type} has no schema authority"
                        ))
                    })?;
                let encoded = EncodedNodeOutput {
                    output_key: output.output_key,
                    asset,
                    authored_type,
                    encoded_type,
                    terminal_type,
                    project,
                    bytes: output.bytes,
                    references: Vec::new(),
                };
                verify_encoded_output(&encoded)?;
                validate_cached_output_type_set(
                    &persisted_type_uuids,
                    authored_type,
                    encoded_type,
                    terminal_type,
                )?;
                hydrated.push(encoded);
            }
            let debug = aux
                .into_iter()
                .map(|row| (row.debug_key, row.bytes))
                .collect();
            Ok(Some((hydrated, debug)))
        }
    }
}

fn preload_persisted_reads(
    context: &mut BuildContext,
    key_kind: KeyKind,
    static_key: &[u8; 32],
    asset: AssetUuid,
) -> Result<(), BuildError> {
    let traces = {
        let mut store = lock_build_store(context)?;
        persisted_candidate_traces(&mut store, key_kind, static_key, asset)
            .map_err(BuildError::infrastructure)?
    };
    // Candidates are newest-first. Materialize only until one complete trace
    // revalidates; a stale candidate's now-unavailable successful read is a
    // cache miss, never authority to fail the current build.
    for trace in &traces {
        match preload_trace_reads(context, trace) {
            Ok(()) => {
                let source = capture_trace_source(context)?;
                if revalidate(trace, &source) {
                    break;
                }
            }
            Err(error) if cache_candidate_miss(&error) => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn cache_candidate_miss(error: &BuildError) -> bool {
    matches!(
        error,
        BuildError::DepthExceeded { .. } | BuildError::Failed(_) | BuildError::Deterministic { .. }
    )
}

fn preload_trace_reads(context: &mut BuildContext, trace: &[TraceOp]) -> Result<(), BuildError> {
    let dependencies = trace
        .iter()
        .filter_map(|operation| match operation {
            TraceOp::Read {
                asset,
                observed: Observed::Ok(_),
            } => Some(*asset),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for dependency in dependencies {
        let _ = build_process_artifact(context, dependency)?;
    }
    Ok(())
}

fn expected_output_identity(
    loaded: &LoadedAsset,
    chain: &PipelineChain,
    stage: &PipelineStage,
    output_key: &str,
) -> Result<(AssetUuid, TypeUuid, TypeUuid, TypeUuid), BuildError> {
    if output_key.is_empty() {
        Ok((
            loaded.entry.uuid,
            loaded.entry.type_uuid,
            stage.registration.outputs.primary,
            chain.terminal,
        ))
    } else {
        let type_uuid = stage
            .registration
            .outputs
            .extras
            .get(output_key)
            .copied()
            .ok_or_else(|| BuildError::Failed("cached undeclared processor extra".to_owned()))?;
        Ok((
            AssetUuid::v5(loaded.entry.uuid, output_key),
            type_uuid,
            type_uuid,
            type_uuid,
        ))
    }
}

fn encode_processor_products(
    context: &BuildContext,
    loaded: &LoadedAsset,
    chain: &PipelineChain,
    stage: &PipelineStage,
    products: &crate::callbacks::ProcessorProducts,
    trace: &mut Vec<TraceOp>,
) -> Result<Vec<EncodedNodeOutput>, BuildError> {
    let trace_source = capture_trace_source(context)?;
    let mut values = Vec::with_capacity(products.extras.len() + 1);
    values.push((
        String::new(),
        products
            .primary
            .as_ref()
            .map(|product| &product.value)
            .expect("epoch validates processor primary"),
    ));
    values.extend(
        products
            .extras
            .iter()
            .map(|(key, product)| (key.clone(), &product.value)),
    );
    let mut outputs = Vec::with_capacity(values.len());
    for (output_key, value) in values {
        let (asset, authored_type, encoded_type, terminal_type) =
            expected_output_identity(loaded, chain, stage, &output_key)?;
        let project = context
            .authority
            .project_type(encoded_type)
            .cloned()
            .ok_or_else(|| {
                BuildError::Failed(format!(
                    "processor output type {encoded_type} has no schema authority"
                ))
            })?;
        let source_bundle = loaded.meta.bundle;
        let mut resolver = |query: &distill_json::AuthoredValue,
                            expected: TypeUuid,
                            strong: bool,
                            _path: &[distill_bundle::PathComponent]| {
            resolve_reference(&trace_source, source_bundle, query, expected, strong, trace)
        };
        let encoded = encode_artifact_value(
            ArtifactValueSpec {
                asset_uuid: asset,
                authored_type,
                terminal_type,
                encoded_type,
                logical_hash: project.logical_hash,
                layout_hash: project.layout_hash,
                schema: &project.logical_schema.root,
                wire: &project.wire,
                value,
            },
            &mut resolver,
        )
        .map_err(|error| {
            let message = error.to_string();
            match artifact_encoding_failure(&error) {
                Some(failure) => BuildError::deterministic(
                    message,
                    DslfV1::OutputBinding {
                        asset: loaded.entry.uuid,
                        processor_id: stage.registration.id.clone(),
                        processor_version: stage.registration.version,
                        stage: stage.index,
                        failure: OutputBindingFailureV1::EncodeRejected {
                            slot: if output_key.is_empty() {
                                OutputBindingSlotV1::Primary
                            } else {
                                OutputBindingSlotV1::Extra {
                                    output_key: output_key.clone(),
                                }
                            },
                            encoded_type,
                            failure,
                        },
                    },
                ),
                None => BuildError::Failed(message),
            }
        })?;
        outputs.push(EncodedNodeOutput {
            output_key,
            asset,
            authored_type,
            encoded_type,
            terminal_type,
            project,
            bytes: encoded.bytes,
            references: encoded.references,
        });
    }
    Ok(outputs)
}

fn ensure_cached_stage_matches(
    cached: &[EncodedNodeOutput],
    cached_debug: &BTreeMap<String, Vec<u8>>,
    fresh: &[EncodedNodeOutput],
    fresh_debug: &BTreeMap<String, Vec<u8>>,
) -> Result<(), BuildError> {
    let cached = cached
        .iter()
        .map(|output| (&output.output_key, &output.bytes))
        .collect::<BTreeMap<_, _>>();
    let fresh = fresh
        .iter()
        .map(|output| (&output.output_key, &output.bytes))
        .collect::<BTreeMap<_, _>>();
    if cached != fresh || cached_debug != fresh_debug {
        return Err(BuildError::Failed(
            "processor output disagrees with its revalidated cached result".to_owned(),
        ));
    }
    Ok(())
}

fn commit_processor_stage(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    static_inputs: &StaticInputs,
    trace: &[TraceOp],
    outputs: &[EncodedNodeOutput],
    debug: &BTreeMap<String, Vec<u8>>,
) -> Result<(), BuildError> {
    for output in outputs {
        lock_build_store(context)?
            .put_wire_tree(&output.project.dswl_bytes)
            .map_err(BuildError::infrastructure)?;
    }
    lock_build_store(context)?
        .commit_build(BuildCommit {
            key_kind: KeyKind::Processor,
            static_input_key: static_inputs_digest(static_inputs),
            asset_uuid: loaded.entry.uuid,
            static_inputs_canonical: static_inputs_canonical_bytes(static_inputs),
            trace: trace_payload_bytes(trace),
            outcome: CommitOutcome::Success {
                payload_kind: PayloadKind::ProcessorOutput,
                outputs: outputs
                    .iter()
                    .map(|output| OutputSpec {
                        output_key: output.output_key.clone(),
                        type_uuids: output_type_set(output),
                        bytes: output.bytes.clone(),
                    })
                    .collect(),
                aux: debug
                    .iter()
                    .map(|(debug_key, bytes)| AuxSpec {
                        debug_key: debug_key.clone(),
                        bytes: bytes.clone(),
                    })
                    .collect(),
            },
        })
        .map_err(BuildError::infrastructure)?;
    Ok(())
}

fn commit_processor_failure(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    static_inputs: &StaticInputs,
    trace: &[TraceOp],
    facts: Option<&DslfV1>,
) -> Result<(), BuildError> {
    let cause = build_failure_cause(trace, facts)?;
    lock_build_store(context)?
        .commit_build(BuildCommit {
            key_kind: KeyKind::Processor,
            static_input_key: static_inputs_digest(static_inputs),
            asset_uuid: loaded.entry.uuid,
            static_inputs_canonical: static_inputs_canonical_bytes(static_inputs),
            trace: trace_payload_bytes(trace),
            outcome: CommitOutcome::Failure { cause },
        })
        .map_err(BuildError::infrastructure)?;
    Ok(())
}

fn build_failure_cause(
    trace: &[TraceOp],
    facts: Option<&DslfV1>,
) -> Result<StoreFailureCause, BuildError> {
    if trace.last().is_some_and(TraceOp::failed) {
        return Ok(StoreFailureCause::Op);
    }
    let facts = facts.ok_or_else(|| {
        BuildError::Infrastructure(
            "build failure is neither trace-caused nor locally fingerprinted".to_owned(),
        )
    })?;
    let detail = facts.digest().map_err(BuildError::failed)?;
    Ok(StoreFailureCause::Local(StoreFailureFingerprint::Local {
        class: store_local_failure_class(facts.class()),
        detail,
    }))
}

fn store_local_failure_class(class: LocalFailureClass) -> StoreLocalFailureClass {
    match class {
        LocalFailureClass::Validator => StoreLocalFailureClass::Validator,
        LocalFailureClass::MigrationPlan => StoreLocalFailureClass::MigrationPlan,
        LocalFailureClass::Processor => StoreLocalFailureClass::Processor,
        LocalFailureClass::MigrationFunction => StoreLocalFailureClass::MigrationFunction,
        LocalFailureClass::OutputBinding => StoreLocalFailureClass::OutputBinding,
        LocalFailureClass::Importer => StoreLocalFailureClass::Importer,
        LocalFailureClass::ImportIntake => StoreLocalFailureClass::ImportIntake,
        LocalFailureClass::ArtifactEncoding => StoreLocalFailureClass::ArtifactEncoding,
    }
}

fn output_type_set(output: &EncodedNodeOutput) -> Vec<TypeUuid> {
    canonical_output_type_set(
        output.authored_type,
        output.encoded_type,
        output.terminal_type,
    )
}

fn canonical_output_type_set(
    authored_type: TypeUuid,
    encoded_type: TypeUuid,
    terminal_type: TypeUuid,
) -> Vec<TypeUuid> {
    let mut types = vec![authored_type, encoded_type, terminal_type];
    types.sort();
    types.dedup();
    types
}

fn validate_cached_output_type_set(
    observed: &[TypeUuid],
    authored_type: TypeUuid,
    encoded_type: TypeUuid,
    terminal_type: TypeUuid,
) -> Result<(), BuildError> {
    let expected = canonical_output_type_set(authored_type, encoded_type, terminal_type);
    if observed != expected {
        return Err(BuildError::Failed(format!(
            "cached processor output type set is invalid: expected {expected:?}, observed {observed:?}"
        )));
    }
    Ok(())
}

fn prepare_outputs(
    context: &mut BuildContext,
    asset: AssetUuid,
    outputs: Vec<EncodedNodeOutput>,
) -> Result<PendingNodePublication, BuildError> {
    let local_assets = outputs
        .iter()
        .map(|output| output.asset)
        .collect::<BTreeSet<_>>();
    let mut rows = BTreeMap::<String, BuildOutputRow>::new();
    let mut pending = Vec::with_capacity(outputs.len());
    let mut wire_trees = BTreeMap::new();
    for output in &outputs {
        verify_encoded_output(output)?;
        lock_build_store(context)?
            .put_wire_tree(&output.project.dswl_bytes)
            .map_err(BuildError::infrastructure)?;
        wire_trees.insert(
            output.project.layout_hash,
            BuildWireTree {
                layout_hash: output.project.layout_hash,
                bytes: Arc::from(output.project.dswl_bytes.clone()),
            },
        );
        let view = parse_artifact(&output.bytes).map_err(BuildError::failed)?;
        let structural_len = output
            .bytes
            .len()
            .checked_sub(view.blob_section.len())
            .ok_or_else(|| BuildError::Failed("artifact structural length underflow".to_owned()))?;
        let blobs = (0..view.blob_table.len())
            .map(|index| {
                view.blob(index as u32)
                    .map(|blob| Arc::<[u8]>::from(blob.to_vec()))
                    .ok_or_else(|| BuildError::Failed("artifact blob table is invalid".to_owned()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let load_edges =
            load_edges_for_output(context, output.asset, &view.load_deps, &output.references)?;
        let content_hash = ContentHash(*blake3::hash(&output.bytes).as_bytes());
        rows.insert(
            output.output_key.clone(),
            BuildOutputRow {
                asset: output.asset,
                content_hash,
                authored_type: output.authored_type,
                encoded_type: output.encoded_type,
                terminal_type: output.terminal_type,
                load_edges,
            },
        );
        pending.push(PendingArtifact {
            content_hash,
            structural: Arc::from(output.bytes[..structural_len].to_vec()),
            blobs,
            load_edges: rows
                .get(&output.output_key)
                .expect("row was inserted immediately above")
                .load_edges
                .clone(),
        });
    }

    Ok(PendingNodePublication {
        asset,
        local_assets,
        rows,
        pending,
        wire_trees,
    })
}

fn pending_dependency_parents(
    context: &BuildContext,
    pending: &PendingNodePublication,
) -> Result<Vec<AssetUuid>, BuildError> {
    let mut dependencies = BTreeSet::new();
    let store = lock_build_store(context)?;
    for row in pending.rows.values() {
        for edge in &row.load_edges {
            if pending.local_assets.contains(&edge.asset) {
                continue;
            }
            let parent = store
                .resolve_child(edge.asset)
                .map_err(BuildError::failed)?
                .map_or(edge.asset, |(parent, _)| parent);
            dependencies.insert(parent);
        }
    }
    Ok(dependencies.into_iter().collect())
}

fn assemble_pending(
    context: &BuildContext,
    pending_node: PendingNodePublication,
) -> Result<NodePublication, BuildError> {
    let PendingNodePublication {
        asset: _,
        local_assets,
        rows,
        pending,
        mut wire_trees,
    } = pending_node;

    let mut artifacts = BTreeMap::new();
    for row in rows.values() {
        for edge in &row.load_edges {
            if local_assets.contains(&edge.asset) {
                continue;
            }
            let (dependency, _) = dependency_from_memo(context, edge.asset)?;
            merge_artifacts(&mut artifacts, dependency.artifacts)?;
            merge_wire_trees(&mut wire_trees, dependency.wire_trees)?;
        }
    }
    for artifact in pending {
        artifacts.insert(
            artifact.content_hash,
            BuildArtifactPublication {
                content_hash: artifact.content_hash,
                payload: ArtifactPayload {
                    structural: artifact.structural,
                    blobs: artifact.blobs,
                    load_edges: artifact.load_edges,
                },
            },
        );
    }
    let primary = rows
        .get("")
        .cloned()
        .ok_or_else(|| BuildError::Failed("built chain has no primary output".to_owned()))?;
    Ok(NodePublication {
        primary,
        outputs: rows,
        artifacts,
        wire_trees,
    })
}

fn dependency_from_memo(
    context: &BuildContext,
    asset: AssetUuid,
) -> Result<(NodePublication, BuildOutputRow), BuildError> {
    let derived = lock_build_store(context)?
        .resolve_child(asset)
        .map_err(BuildError::failed)?;
    if let Some((parent, output_key)) = derived {
        let publication = context.memo.get(&parent).cloned().ok_or_else(|| {
            BuildError::Infrastructure(format!(
                "iterative build assembled dependent {asset} before parent {parent}"
            ))
        })?;
        let selected = publication
            .outputs
            .get(&output_key)
            .cloned()
            .ok_or_else(|| {
                BuildError::Failed(format!(
                    "derived child {asset} is absent from parent {parent}'s chain result"
                ))
            })?;
        Ok((publication, selected))
    } else {
        let publication = context.memo.get(&asset).cloned().ok_or_else(|| {
            BuildError::Infrastructure(format!(
                "iterative build assembled a node before dependency {asset}"
            ))
        })?;
        let selected = publication.primary.clone();
        Ok((publication, selected))
    }
}

fn verify_encoded_output(output: &EncodedNodeOutput) -> Result<(), BuildError> {
    let view = parse_artifact(&output.bytes).map_err(BuildError::failed)?;
    if view.asset_uuid != output.asset
        || view.authored_type != output.authored_type
        || view.encoded_type != output.encoded_type
        || view.terminal_type != output.terminal_type
        || view.logical_hash != output.project.logical_hash
        || view.layout_hash != output.project.layout_hash
    {
        return Err(BuildError::Failed(
            "cached artifact identity does not match its build inputs".to_owned(),
        ));
    }
    Ok(())
}

fn load_edges_for_output(
    context: &BuildContext,
    source_asset: AssetUuid,
    load_deps: &[AssetUuid],
    references: &[distill_wire::encode::EncodedReference],
) -> Result<Vec<ServedLoadEdge>, BuildError> {
    let fresh = references
        .iter()
        .filter(|reference| reference.strong)
        .map(|reference| (reference.asset, reference.expected_terminal))
        .collect::<BTreeMap<_, _>>();
    let mut edges = Vec::with_capacity(load_deps.len());
    for asset in load_deps {
        let observed = resolved_terminal_type(context, *asset)?;
        let expected = fresh.get(asset).copied().unwrap_or(observed);
        if observed != expected {
            return Err(BuildError::Failed(format!(
                "reference from {source_asset} expects terminal type {expected}, but {asset} has {observed}"
            )));
        }
        edges.push(ServedLoadEdge {
            asset: *asset,
            expected_terminal: expected,
        });
    }
    edges.sort();
    edges.dedup();
    Ok(edges)
}

fn resolved_terminal_type(
    context: &BuildContext,
    asset: AssetUuid,
) -> Result<TypeUuid, BuildError> {
    let store = lock_build_store(context)?;
    if let Some(entry) = store.entry(asset).map_err(BuildError::failed)? {
        return context
            .registry
            .chain(entry.type_uuid, &context.target)
            .map(|chain| chain.terminal)
            .map_err(BuildError::failed);
    }
    let (parent, output_key) = store
        .resolve_child(asset)
        .map_err(BuildError::failed)?
        .ok_or_else(|| BuildError::Failed(format!("missing strong reference {asset}")))?;
    let parent = store
        .entry(parent)
        .map_err(BuildError::failed)?
        .ok_or_else(|| BuildError::Failed("derived parent is missing".to_owned()))?;
    context
        .registry
        .chain(parent.type_uuid, &context.target)
        .map_err(BuildError::failed)?
        .extras
        .get(&output_key)
        .copied()
        .ok_or_else(|| BuildError::Failed("derived output is absent from pipeline map".to_owned()))
}

fn capture_trace_source(context: &BuildContext) -> Result<StoreTraceSource, BuildError> {
    let epoch = context
        .pipeline
        .epoch()
        .map_err(|poison| BuildError::Failed(poison.to_string()))?;
    let store = lock_build_store(context)?;
    StoreTraceSource::capture(
        &store,
        TraceCaptureBasis {
            scanner: &context.scanner,
            registry: &context.registry,
            target: &context.target,
            input_version: context.basis,
            epoch,
            dylib_hash: context.dylib_hash,
            memo: &context.memo,
        },
    )
}

struct EpochDefaults<'a> {
    epoch: &'a PipelineEpoch,
    type_uuid: TypeUuid,
    callback_error: std::cell::RefCell<Option<String>>,
}

impl EpochDefaults<'_> {
    fn new(epoch: &PipelineEpoch, type_uuid: TypeUuid) -> EpochDefaults<'_> {
        EpochDefaults {
            epoch,
            type_uuid,
            callback_error: std::cell::RefCell::new(None),
        }
    }

    fn record_error(&self, error: impl std::fmt::Debug) {
        *self.callback_error.borrow_mut() = Some(format!("{error:?}"));
    }

    fn take_error(&self) -> Option<String> {
        self.callback_error.borrow_mut().take()
    }
}

impl DefaultProvider for EpochDefaults<'_> {
    fn field_default(
        &self,
        to_schema: &distill_schema::ngp_schema::SchemaNode,
        at: &FieldPath,
    ) -> Option<AuthoredValue> {
        match self
            .epoch
            .invoke_field_default(self.type_uuid, to_schema, at)
        {
            Ok(value) => value,
            Err(error) => {
                self.record_error(error);
                None
            }
        }
    }

    fn parent_default(
        &self,
        to_schema: &distill_schema::ngp_schema::SchemaNode,
        at: &FieldPath,
    ) -> Option<AuthoredValue> {
        match self
            .epoch
            .invoke_parent_default(self.type_uuid, to_schema, at)
        {
            Ok(value) => value,
            Err(error) => {
                self.record_error(error);
                None
            }
        }
    }
}

struct NoMigrationDefaults;

impl DefaultProvider for NoMigrationDefaults {
    fn field_default(
        &self,
        _to_schema: &distill_schema::ngp_schema::SchemaNode,
        _at: &FieldPath,
    ) -> Option<AuthoredValue> {
        None
    }

    fn parent_default(
        &self,
        _to_schema: &distill_schema::ngp_schema::SchemaNode,
        _at: &FieldPath,
    ) -> Option<AuthoredValue> {
        None
    }
}

fn migration_ops_use_defaults(ops: &[MigrationOp]) -> bool {
    ops.iter().any(|op| match op {
        MigrationOp::WriteFieldDefault { .. } | MigrationOp::WriteParentDefault { .. } => true,
        MigrationOp::MapVariant { payload, .. } => migration_ops_use_defaults(payload),
        MigrationOp::MigrateElements { element, .. } => migration_ops_use_defaults(element),
        MigrationOp::MigrateMapKeys { key, .. } => migration_ops_use_defaults(key),
        MigrationOp::MigrateInline { ops, .. } => migration_ops_use_defaults(ops),
        MigrationOp::CopyField { .. }
        | MigrationOp::Widen { .. }
        | MigrationOp::WriteValue { .. }
        | MigrationOp::WriteNone { .. }
        | MigrationOp::DropField { .. } => false,
    })
}

fn migration_key_inputs(
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    source: &StoreTraceSource,
    dylib_hash: [u8; 32],
) -> Result<(Vec<AppliedMigration>, Option<AutomaticMigration>), BuildError> {
    if loaded.entry.schema_hash == project.logical_hash {
        return Ok((Vec::new(), None));
    }
    let bundle = distill_bundle::parse_bundle(&loaded.bundle_bytes).map_err(BuildError::failed)?;
    let mut schema = bundle
        .schemas
        .get(&loaded.entry.schema_hash)
        .ok_or_else(|| {
            BuildError::Failed("bundle omitted the entry's old schema snapshot".to_owned())
        })?
        .clone();
    let mut node = loaded.entry.schema_hash;
    let mut visited = BTreeSet::from([node]);
    let mut migrations = Vec::<AppliedMigration>::new();
    loop {
        if node == project.logical_hash {
            return Ok((migrations, None));
        }
        let records = source
            .current_load
            .migration_controls
            .values()
            .filter(|record| {
                record.header.target_type_uuid == loaded.entry.type_uuid
                    && record.header.from_hash == node
            })
            .collect::<Vec<_>>();
        match records.as_slice() {
            [] => {
                let uses_pipeline_code = plan_automatic(&schema.root, &project.logical_schema.root)
                    .is_ok_and(|plan| migration_ops_use_defaults(&plan));
                return Ok((
                    migrations,
                    Some(AutomaticMigration {
                        from: node,
                        to: project.logical_hash,
                        planner_version: MIGRATION_PLANNER_VERSION,
                        dylib_hash: uses_pipeline_code.then_some(dylib_hash),
                    }),
                ));
            }
            [record] => {
                let (Observed::Ok(bundle_hash), Some(edge)) = (&record.observed, &record.value)
                else {
                    return Ok((migrations, None));
                };
                if edge.target_type_uuid != loaded.entry.type_uuid
                    || edge.from_hash != node
                    || edge.from_schema != schema
                    || !visited.insert(edge.to_hash)
                {
                    return Ok((migrations, None));
                }
                let edge_dylib = matches!(edge.kind, MigrationControlKind::Function { .. })
                    .then_some(dylib_hash);
                if let Some(existing) = migrations
                    .iter_mut()
                    .find(|migration| migration.bundle_hash == bundle_hash.0)
                {
                    if edge_dylib.is_some() {
                        existing.dylib_hash = edge_dylib;
                    }
                } else {
                    migrations.push(AppliedMigration {
                        bundle_hash: bundle_hash.0,
                        planner_version: MIGRATION_PLANNER_VERSION,
                        dylib_hash: edge_dylib,
                    });
                }
                node = edge.to_hash;
                schema = edge.to_schema.clone();
            }
            _ => return Ok((migrations, None)),
        }
    }
}

fn migration_plan_error(
    type_uuid: TypeUuid,
    from: LogicalHash,
    to: LogicalHash,
    failure: MigrationPlanFailureV1,
    message: impl Into<String>,
) -> BuildError {
    BuildError::migration(
        message,
        DslfV1::MigrationPlan {
            type_uuid,
            from,
            to,
            failure,
        },
    )
}

fn migration_execution_failure(error: &MigrationError) -> MigrationPlanFailureV1 {
    let path = match error {
        MigrationError::MissingDefault { path, .. }
        | MigrationError::InputPathMissing { path }
        | MigrationError::InputShape { path, .. }
        | MigrationError::WidenOutOfRange { path, .. }
        | MigrationError::DuplicateSetElement { path }
        | MigrationError::MapKeyCollision { path }
        | MigrationError::MapKeyNotString { path }
        | MigrationError::DuplicateWrite { path }
        | MigrationError::MissingWrite { path }
        | MigrationError::PlanShape { path, .. }
        | MigrationError::Unencodable { path } => field_path_from_display(path),
        MigrationError::DuplicateMapVariantFrom { at, .. } => field_path_from_display(at),
        MigrationError::NonConforming { .. }
        | MigrationError::PlanInvalid(_)
        | MigrationError::FunctionFailed { .. } => FieldPath::root(),
    };
    match error {
        MigrationError::MissingDefault { .. } => MigrationPlanFailureV1::MissingDefault { path },
        MigrationError::MapKeyCollision { .. } => MigrationPlanFailureV1::MapKeyCollision { path },
        MigrationError::DuplicateSetElement { .. } => {
            MigrationPlanFailureV1::SetElementCollision { path }
        }
        MigrationError::DuplicateWrite { .. } => {
            MigrationPlanFailureV1::DuplicateDestination { path }
        }
        MigrationError::MissingWrite { .. } => {
            MigrationPlanFailureV1::UnwrittenDestination { path }
        }
        _ => MigrationPlanFailureV1::MissingPath { path },
    }
}

fn field_path_from_display(path: &str) -> FieldPath {
    FieldPath(
        path.strip_prefix('$')
            .unwrap_or(path)
            .split('.')
            .filter(|segment| !segment.is_empty())
            .map(str::to_owned)
            .collect(),
    )
}

fn artifact_encoding_failure(error: &ArtifactEncodeError) -> Option<ArtifactEncodingFailureV1> {
    match error {
        ArtifactEncodeError::Value(
            EncodeError::DuplicateOrderedValue { path } | EncodeError::DuplicateBlobPath { path },
        ) => Some(ArtifactEncodingFailureV1::NonCanonicalOrder {
            path: field_path_from_display(path),
        }),
        ArtifactEncodeError::Value(
            EncodeError::InvalidScalar { path, .. } | EncodeError::KeyNotEncodable { path },
        ) => Some(ArtifactEncodingFailureV1::InvalidScalar {
            path: field_path_from_display(path),
        }),
        ArtifactEncodeError::Value(
            EncodeError::InvalidWireShape { .. }
            | EncodeError::MissingWireField { .. }
            | EncodeError::MissingWireVariant { .. }
            | EncodeError::BackRefMismatch { .. },
        ) => Some(ArtifactEncodingFailureV1::LayoutUnavailable),
        ArtifactEncodeError::Artifact(ArtifactError::VarLenTooLarge { var_len }) => {
            Some(ArtifactEncodingFailureV1::SizeLimit {
                limit: u32::MAX as u64,
                observed: *var_len,
            })
        }
        ArtifactEncodeError::Artifact(ArtifactError::CountExceedsU32 { count, .. }) => {
            Some(ArtifactEncodingFailureV1::SizeLimit {
                limit: u32::MAX as u64,
                observed: *count,
            })
        }
        ArtifactEncodeError::Artifact(ArtifactError::DuplicateBlobPath { path }) => {
            Some(ArtifactEncodingFailureV1::NonCanonicalOrder {
                path: field_path_from_display(path),
            })
        }
        // The v1 DSLF grammar has no exact fact payload for these failures.
        // Keep them deterministic but non-memoized rather than inventing an
        // observation or collapsing distinct failures into a false fact.
        ArtifactEncodeError::Value(
            EncodeError::Shape { .. }
            | EncodeError::InvalidReference { .. }
            | EncodeError::Bounds { .. }
            | EncodeError::DepthExceeded,
        )
        | ArtifactEncodeError::Artifact(
            ArtifactError::BadMagic
            | ArtifactError::UnsupportedVersion { .. }
            | ArtifactError::Truncated { .. }
            | ArtifactError::TrailingBytes { .. }
            | ArtifactError::Overflow
            | ArtifactError::DepsNotCanonical { .. }
            | ArtifactError::NonzeroPadding { .. }
            | ArtifactError::BlobTableInvalid { .. }
            | ArtifactError::BlobCountMismatch { .. }
            | ArtifactError::BlobLengthMismatch { .. },
        ) => None,
    }
}

fn commit_build_import_failure(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    key: [u8; 32],
    trace: &[TraceOp],
    facts: Option<&DslfV1>,
) -> Result<(), BuildError> {
    let cause = build_failure_cause(trace, facts)?;
    lock_build_store(context)?
        .commit_build(BuildCommit {
            key_kind: KeyKind::BuildImport,
            static_input_key: key,
            asset_uuid: loaded.entry.uuid,
            static_inputs_canonical: Vec::new(),
            trace: trace_payload_bytes(trace),
            outcome: CommitOutcome::Failure { cause },
        })
        .map_err(BuildError::infrastructure)?;
    Ok(())
}

fn load_current_value(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    trace_source: &StoreTraceSource,
    trace: &mut Vec<TraceOp>,
) -> Result<AuthoredValue, BuildError> {
    let bundle = distill_bundle::parse_bundle(&loaded.bundle_bytes).map_err(BuildError::failed)?;
    load_current_entry(
        &CurrentLoadRuntime::from_build(context),
        &loaded.entry,
        &bundle,
        &project.logical_schema,
        project.logical_hash,
        &trace_source.current_load,
        trace,
    )
}

fn load_current_entry(
    runtime: &CurrentLoadRuntime<'_>,
    entry: &AssetEntry,
    bundle: &Bundle,
    current_schema: &distill_schema::ngp_schema::LogicalSchema,
    current_hash: LogicalHash,
    trace_source: &CurrentLoadSource,
    trace: &mut Vec<TraceOp>,
) -> Result<AuthoredValue, BuildError> {
    if entry.schema_hash == current_hash {
        return Ok(entry.data.clone());
    }
    let old = bundle
        .schemas
        .get(&entry.schema_hash)
        .ok_or_else(|| {
            BuildError::Failed("bundle omitted the entry's old schema snapshot".to_owned())
        })?
        .clone();
    let selected = select_migration_chain(
        trace_source,
        entry.type_uuid,
        entry.schema_hash,
        current_hash,
        trace,
    )?;
    let mut value = entry.data.clone();
    let mut schema = old;
    let mut node = entry.schema_hash;
    let mut tail_stamp = match &entry.lineage {
        EntryLineageV1::Manifest(stamp) => Some(stamp.clone()),
        EntryLineageV1::Bootstrap { .. } => None,
    };
    let last_custom_edge = selected.edges.last().map(|edge| edge.asset);
    for edge in selected.edges {
        if edge.target_type_uuid != entry.type_uuid
            || edge.from_hash != node
            || edge.from_schema != schema
        {
            return Err(migration_plan_error(
                entry.type_uuid,
                node,
                edge.to_hash,
                MigrationPlanFailureV1::NonConformingOutput {
                    edge: edge.asset,
                    path: FieldPath::root(),
                },
                format!(
                    "Migration control {} does not continue the selected schema chain",
                    edge.asset
                ),
            ));
        }
        value = execute_custom_migration(runtime, trace_source, trace, entry.uuid, &edge, value)?;
        schema = edge.to_schema.clone();
        node = edge.to_hash;
        tail_stamp = Some(edge.to_lineage.clone());
    }
    if node == current_hash {
        if schema != *current_schema {
            let edge = last_custom_edge.unwrap_or(entry.uuid);
            return Err(migration_plan_error(
                entry.type_uuid,
                entry.schema_hash,
                current_hash,
                MigrationPlanFailureV1::NonConformingOutput {
                    edge,
                    path: FieldPath::root(),
                },
                "Migration chain reached the current hash with a different schema",
            ));
        }
        return Ok(value);
    }
    if !selected.needs_automatic_tail {
        return Err(migration_plan_error(
            entry.type_uuid,
            node,
            current_hash,
            MigrationPlanFailureV1::MissingReverseEdge {
                missing_from: node,
                missing_to: current_hash,
            },
            "Migration chain stopped before the current schema",
        ));
    }
    let placement = lock_current_load_store(runtime)?
        .classify_lineage(entry.type_uuid, node, tail_stamp, current_hash)
        .map_err(BuildError::infrastructure)?;
    if !placement.permits_automatic_diff() {
        return Err(migration_plan_error(
            entry.type_uuid,
            node,
            current_hash,
            MigrationPlanFailureV1::MissingReverseEdge {
                missing_from: node,
                missing_to: current_hash,
            },
            format!(
                "asset {} cannot automatically migrate the custom-chain tail from {} to {}: {placement:?}",
                entry.uuid, node, current_hash
            ),
        ));
    }
    let plan = plan_automatic(&schema.root, &current_schema.root).map_err(|error| {
        migration_plan_error(
            entry.type_uuid,
            node,
            current_hash,
            MigrationPlanFailureV1::MissingPath {
                path: FieldPath::root(),
            },
            error.to_string(),
        )
    })?;
    validate_plan(
        &plan,
        &schema.root,
        &current_schema.root,
        EdgeKind::Automatic,
    )
    .map_err(|errors| {
        migration_plan_error(
            entry.type_uuid,
            node,
            current_hash,
            MigrationPlanFailureV1::MissingPath {
                path: FieldPath::root(),
            },
            format!("automatic migration plan rejected: {errors:?}"),
        )
    })?;

    let uses_defaults = migration_ops_use_defaults(&plan);
    if uses_defaults {
        let key = CapabilityKey::DefaultTable(entry.type_uuid);
        let observed = trace_source.capability(&key);
        trace.push(TraceOp::Capability {
            key,
            observed: observed.clone(),
        });
        if let Observed::Err(error) = observed {
            return Err(BuildError::Failed(format!(
                "automatic migration default capability is unavailable: {error:?}"
            )));
        }
    }

    let migrated = if uses_defaults {
        let epoch = runtime
            .pipeline
            .epoch()
            .map_err(|poison| BuildError::Failed(poison.to_string()))?;
        let defaults = EpochDefaults::new(epoch, entry.type_uuid);
        execute_ops(&plan, &value, &schema.root, &current_schema.root, &defaults)
            .map_err(|error| {
                if let Some(callback) = defaults.take_error() {
                    BuildError::Failed(format!("default callback failed: {callback}"))
                } else {
                    migration_plan_error(
                        entry.type_uuid,
                        node,
                        current_hash,
                        migration_execution_failure(&error),
                        error.to_string(),
                    )
                }
            })?
            .value
    } else {
        execute_ops(
            &plan,
            &value,
            &schema.root,
            &current_schema.root,
            &NoMigrationDefaults,
        )
        .map_err(|error| {
            migration_plan_error(
                entry.type_uuid,
                node,
                current_hash,
                migration_execution_failure(&error),
                error.to_string(),
            )
        })?
        .value
    };
    conforms(&migrated, &current_schema.root).map_err(|error| {
        migration_plan_error(
            entry.type_uuid,
            node,
            current_hash,
            MigrationPlanFailureV1::NonConformingOutput {
                edge: last_custom_edge.unwrap_or(entry.uuid),
                path: FieldPath::root(),
            },
            format!("migrated value is non-conforming: {error}"),
        )
    })?;
    Ok(migrated)
}

struct SelectedMigrationChain {
    edges: Vec<MigrationControlValue>,
    needs_automatic_tail: bool,
}

fn select_migration_chain(
    source: &CurrentLoadSource,
    type_uuid: TypeUuid,
    start: LogicalHash,
    target: LogicalHash,
    trace: &mut Vec<TraceOp>,
) -> Result<SelectedMigrationChain, BuildError> {
    let mut selected = SelectedMigrationChain {
        edges: Vec::new(),
        needs_automatic_tail: false,
    };
    let mut visited = BTreeSet::from([start]);
    let mut node = start;
    loop {
        if node == target {
            return Ok(selected);
        }
        let query = ControlQuery::MigrationEdges {
            type_uuid,
            from_hash: node,
        };
        let observed = source.control(&query);
        trace.push(TraceOp::Control {
            query,
            observed: observed.clone(),
        });
        if let Observed::Err(error) = observed {
            return Err(BuildError::Failed(format!(
                "Migration control query failed: {error:?}"
            )));
        }
        let assets = source.migration_assets(type_uuid, node);
        let mut outgoing = Vec::with_capacity(assets.len());
        for asset in assets {
            let subject = ControlSubject::Migration(asset);
            let observed = source.control_read(&subject);
            trace.push(TraceOp::ControlRead {
                subject,
                observed: observed.clone(),
            });
            if let Observed::Err(error) = observed {
                return Err(BuildError::Failed(format!(
                    "Migration control {asset} failed validation: {error:?}"
                )));
            }
            outgoing.push(
                source
                    .migration_controls
                    .get(&asset)
                    .and_then(|record| record.value.clone())
                    .ok_or_else(|| {
                        BuildError::Infrastructure(
                            "successful Migration control read has no decoded value".to_owned(),
                        )
                    })?,
            );
        }
        match outgoing.len() {
            0 => {
                selected.needs_automatic_tail = true;
                return Ok(selected);
            }
            1 => {
                let edge = outgoing.pop().expect("one outgoing edge");
                if !visited.insert(edge.to_hash) {
                    let mut cycle_edges = selected
                        .edges
                        .iter()
                        .map(|selected| selected.asset)
                        .collect::<Vec<_>>();
                    cycle_edges.push(edge.asset);
                    return Err(migration_plan_error(
                        type_uuid,
                        start,
                        target,
                        MigrationPlanFailureV1::Cycle { cycle_edges },
                        format!("Migration graph cycle revisits {}", edge.to_hash),
                    ));
                }
                node = edge.to_hash;
                selected.edges.push(edge);
            }
            _ => {
                let conflicting_edges = outgoing.iter().map(|edge| edge.asset).collect::<Vec<_>>();
                return Err(migration_plan_error(
                    type_uuid,
                    start,
                    target,
                    MigrationPlanFailureV1::AmbiguousEdge {
                        conflicting_edges: conflicting_edges.clone(),
                    },
                    format!("ambiguous Migration graph at {node}: {conflicting_edges:?}"),
                ));
            }
        }
    }
}

fn execute_custom_migration(
    runtime: &CurrentLoadRuntime<'_>,
    source: &CurrentLoadSource,
    trace: &mut Vec<TraceOp>,
    asset: AssetUuid,
    edge: &MigrationControlValue,
    input: AuthoredValue,
) -> Result<AuthoredValue, BuildError> {
    let output = match &edge.kind {
        MigrationControlKind::Ops(ops) => {
            validate_plan(
                ops,
                &edge.from_schema.root,
                &edge.to_schema.root,
                EdgeKind::Custom,
            )
            .map_err(|errors| {
                migration_plan_error(
                    edge.target_type_uuid,
                    edge.from_hash,
                    edge.to_hash,
                    MigrationPlanFailureV1::MissingPath {
                        path: FieldPath::root(),
                    },
                    format!(
                        "Migration control {} has an invalid custom plan: {errors:?}",
                        edge.asset
                    ),
                )
            })?;
            execute_ops(
                ops,
                &input,
                &edge.from_schema.root,
                &edge.to_schema.root,
                &NoMigrationDefaults,
            )
            .map_err(|error| {
                migration_plan_error(
                    edge.target_type_uuid,
                    edge.from_hash,
                    edge.to_hash,
                    migration_execution_failure(&error),
                    error.to_string(),
                )
            })?
            .value
        }
        MigrationControlKind::Function { key } => {
            let capability = CapabilityKey::MigrationFn(key.clone());
            let observed = source.capability(&capability);
            trace.push(TraceOp::Capability {
                key: capability,
                observed: observed.clone(),
            });
            if let Observed::Err(error) = observed {
                return Err(BuildError::Failed(format!(
                    "Migration function capability {key:?} is unavailable: {error:?}"
                )));
            }
            match runtime
                .pipeline
                .epoch()
                .map_err(|poison| BuildError::Failed(poison.to_string()))?
                .invoke_migration(key, input)
            {
                Ok(output) => output,
                Err(CallbackInvokeError::Rejected(error)) => {
                    return Err(BuildError::migration(
                        format!(
                            "Migration function {key:?} rejected asset {asset} with code {}: {}",
                            error.code, error.message
                        ),
                        DslfV1::MigrationFunction {
                            asset,
                            type_uuid: edge.target_type_uuid,
                            from: edge.from_hash,
                            to: edge.to_hash,
                            function_key: key.clone(),
                            migration_error_code: error.code,
                        },
                    ));
                }
                Err(error) => return Err(BuildError::failed(error)),
            }
        }
    };
    conforms(&output, &edge.to_schema.root).map_err(|error| {
        migration_plan_error(
            edge.target_type_uuid,
            edge.from_hash,
            edge.to_hash,
            MigrationPlanFailureV1::NonConformingOutput {
                edge: edge.asset,
                path: FieldPath::root(),
            },
            format!(
                "Migration control {} produced a non-conforming value: {error}",
                edge.asset
            ),
        )
    })?;
    Ok(output)
}

fn encode_or_hydrate(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    terminal_type: TypeUuid,
    validator_dylib_hash: Option<[u8; 32]>,
) -> Result<EncodedBuildImport, BuildError> {
    let trace_source = capture_trace_source(context)?;
    let (migrations, automatic_migration) =
        migration_key_inputs(loaded, project, &trace_source, context.dylib_hash)?;
    let key = build_import_digest(&BuildImportInputs {
        asset: loaded.entry.uuid,
        bundle: loaded.meta.bundle,
        local_id: loaded.meta.local_id.clone(),
        authored_type: loaded.entry.type_uuid,
        terminal_type,
        canonical_bundle_bytes: loaded.bundle_bytes.clone(),
        logical: project.logical_hash,
        layout: project.layout_hash,
        migrations,
        automatic_migration,
        validator_dylib_hash,
        artifact_format_version: ARTIFACT_FORMAT_VERSION,
    });
    let hit = if context.verify_fresh {
        None
    } else {
        let mut store = lock_build_store(context)?;
        lookup_persisted_candidate(
            &mut store,
            KeyKind::BuildImport,
            &key,
            loaded.entry.uuid,
            &trace_source,
        )
        .map_err(BuildError::infrastructure)?
    };
    if let Some(hit) = hit {
        return match hit.outcome {
            PersistedOutcome::Success { outputs, aux }
                if outputs.len() == 1 && outputs[0].output_key.is_empty() && aux.is_empty() =>
            {
                let output = outputs.into_iter().next().unwrap();
                validate_cached_output_type_set(
                    &output.type_uuids,
                    loaded.entry.type_uuid,
                    loaded.entry.type_uuid,
                    terminal_type,
                )?;
                let encoded = EncodedNodeOutput {
                    output_key: String::new(),
                    asset: loaded.entry.uuid,
                    authored_type: loaded.entry.type_uuid,
                    encoded_type: loaded.entry.type_uuid,
                    terminal_type,
                    project: project.clone(),
                    bytes: output.bytes,
                    references: Vec::new(),
                };
                verify_encoded_output(&encoded)?;
                Ok((encoded.bytes, Vec::new(), None))
            }
            PersistedOutcome::Success { .. } => Err(BuildError::Failed(
                "cached build-import result has an invalid output shape".to_owned(),
            )),
            PersistedOutcome::Failure { cause } => Err(BuildError::Failed(format!(
                "cached build-import failure: {cause:?}"
            ))),
        };
    }

    let mut trace = Vec::new();
    let current_value =
        match load_current_value(context, loaded, project, &trace_source, &mut trace) {
            Ok(value) => value,
            Err(error) => {
                let facts = match &error {
                    BuildError::Deterministic { facts, .. } => Some(facts.as_ref()),
                    _ => None,
                };
                if !context.verify_fresh
                    && (trace.last().is_some_and(TraceOp::failed) || facts.is_some())
                {
                    commit_build_import_failure(context, loaded, key, &trace, facts)?;
                }
                return Err(error);
            }
        };

    if validator_dylib_hash.is_some() {
        let diagnostics = context
            .pipeline
            .epoch()
            .map_err(|poison| BuildError::Failed(poison.to_string()))?
            .invoke_validators(loaded.entry.type_uuid, &current_value)
            .map_err(BuildError::failed)?;
        let error_paths = diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
            .map(|diagnostic| diagnostic.path.clone())
            .collect::<Vec<_>>();
        if !error_paths.is_empty() {
            let facts = DslfV1::Validator {
                asset: loaded.entry.uuid,
                type_uuid: loaded.entry.type_uuid,
                error_paths,
            };
            let detail = facts.digest().map_err(BuildError::failed)?;
            if !context.verify_fresh {
                lock_build_store(context)?
                    .commit_build(BuildCommit {
                        key_kind: KeyKind::BuildImport,
                        static_input_key: key,
                        asset_uuid: loaded.entry.uuid,
                        static_inputs_canonical: Vec::new(),
                        trace: trace_payload_bytes(&trace),
                        outcome: CommitOutcome::Failure {
                            cause: StoreFailureCause::Local(StoreFailureFingerprint::Local {
                                class: StoreLocalFailureClass::Validator,
                                detail,
                            }),
                        },
                    })
                    .map_err(BuildError::infrastructure)?;
            }
            return Err(BuildError::Failed(format!(
                "asset {} failed validation: {diagnostics:?}",
                loaded.entry.uuid
            )));
        }
    }

    let source_bundle = loaded.meta.bundle;
    let mut resolver = |query: &distill_json::AuthoredValue,
                        expected: TypeUuid,
                        strong: bool,
                        _path: &[distill_bundle::PathComponent]| {
        resolve_reference(
            &trace_source,
            source_bundle,
            query,
            expected,
            strong,
            &mut trace,
        )
    };
    let encoded = match encode_artifact_value(
        ArtifactValueSpec {
            asset_uuid: loaded.entry.uuid,
            authored_type: loaded.entry.type_uuid,
            terminal_type,
            encoded_type: loaded.entry.type_uuid,
            logical_hash: project.logical_hash,
            layout_hash: project.layout_hash,
            schema: &project.logical_schema.root,
            wire: &project.wire,
            value: &current_value,
        },
        &mut resolver,
    ) {
        Ok(encoded) => encoded,
        Err(error) => {
            let message = error.to_string();
            let facts = artifact_encoding_failure(&error).map(|failure| DslfV1::ArtifactEncoding {
                asset: loaded.entry.uuid,
                encoded_type: loaded.entry.type_uuid,
                failure,
            });
            if !context.verify_fresh
                && (trace.last().is_some_and(TraceOp::failed) || facts.is_some())
            {
                commit_build_import_failure(context, loaded, key, &trace, facts.as_ref())?;
            }
            return Err(match facts {
                Some(facts) => BuildError::deterministic(message, facts),
                None => BuildError::Failed(message),
            });
        }
    };
    let EncodedArtifact {
        bytes, references, ..
    } = encoded;
    let types = canonical_output_type_set(
        loaded.entry.type_uuid,
        loaded.entry.type_uuid,
        terminal_type,
    );
    if !context.verify_fresh {
        lock_build_store(context)?
            .put_wire_tree(&project.dswl_bytes)
            .map_err(BuildError::infrastructure)?;
        lock_build_store(context)?
            .commit_build(BuildCommit {
                key_kind: KeyKind::BuildImport,
                static_input_key: key,
                asset_uuid: loaded.entry.uuid,
                static_inputs_canonical: Vec::new(),
                trace: trace_payload_bytes(&trace),
                outcome: CommitOutcome::Success {
                    payload_kind: PayloadKind::ImportEncoding,
                    outputs: vec![OutputSpec {
                        output_key: String::new(),
                        type_uuids: types,
                        bytes: bytes.clone(),
                    }],
                    aux: Vec::new(),
                },
            })
            .map_err(BuildError::infrastructure)?;
    }
    Ok((bytes, references, Some(current_value)))
}

fn resolve_reference(
    source: &StoreTraceSource,
    source_bundle: BundleUuid,
    value: &distill_json::AuthoredValue,
    expected: TypeUuid,
    strong: bool,
    trace: &mut Vec<TraceOp>,
) -> Result<AssetUuid, String> {
    let query = decode_asset_reference_query(value).map_err(|error| format!("{error:?}"))?;
    let asset = match query {
        AssetReferenceQuery::Uuid(asset) => Some(asset),
        AssetReferenceQuery::SameBundleLocalId(local_id) => resolve_query(
            source,
            AssetQuery {
                local_id: Some(local_id),
                bundle_uuid: Some(source_bundle),
                ..AssetQuery::default()
            },
            trace,
        )?,
        AssetReferenceQuery::Path {
            normalized_path,
            local_id: None,
        } => {
            let observed = source.resolve(&normalized_path);
            trace.push(TraceOp::Resolve {
                path: normalized_path,
                observed: observed.clone(),
            });
            match observed {
                Observed::Ok(asset) => asset,
                Observed::Err(error) => return Err(format!("{error:?}")),
            }
        }
        AssetReferenceQuery::Path {
            normalized_path,
            local_id: Some(local_id),
        } => resolve_query(
            source,
            AssetQuery {
                bundle_path: Some(normalized_path),
                local_id: Some(local_id),
                ..AssetQuery::default()
            },
            trace,
        )?,
    }
    .ok_or_else(|| "asset reference did not resolve".to_owned())?;
    let role = source.role_check(asset);
    trace.push(TraceOp::RoleCheck {
        asset,
        observed: role.clone(),
    });
    if !strong && role == Observed::Ok(None) {
        let terminal = source.ref_check(asset, expected);
        trace.push(TraceOp::RefCheck {
            asset,
            expected_terminal: expected,
            observed: terminal.clone(),
        });
        if terminal == Observed::Ok(None) {
            return Ok(asset);
        }
        return Err(format!(
            "absent weak asset {asset} has an inconsistent terminal-type observation"
        ));
    }
    if role != Observed::Ok(Some(EntryRole::Runtime)) {
        return Err(format!("referenced asset {asset} is not runtime eligible"));
    }
    let terminal = source.ref_check(asset, expected);
    trace.push(TraceOp::RefCheck {
        asset,
        expected_terminal: expected,
        observed: terminal.clone(),
    });
    if terminal != Observed::Ok(Some(expected)) {
        return Err(format!(
            "referenced asset {asset} does not have expected terminal type {expected}"
        ));
    }
    Ok(asset)
}

fn resolve_query(
    source: &StoreTraceSource,
    query: AssetQuery,
    trace: &mut Vec<TraceOp>,
) -> Result<Option<AssetUuid>, String> {
    let results = source.query_results(&query);
    trace.push(TraceOp::Query {
        query: Box::new(query),
        observed: Observed::Ok(asset_query_result_hash(&results)),
    });
    match results.as_slice() {
        [] => Ok(None),
        [asset] => Ok(Some(*asset)),
        _ => Err(format!("asset reference query is ambiguous: {results:?}")),
    }
}

fn load_asset(
    store: &Store,
    scanner: &RootedScanner,
    asset: AssetUuid,
) -> Result<LoadedAsset, BuildError> {
    let meta = store
        .entry(asset)
        .map_err(BuildError::failed)?
        .ok_or_else(|| BuildError::Failed(format!("asset {asset} is missing")))?;
    let bundle_meta = store
        .bundle(meta.bundle)
        .map_err(BuildError::infrastructure)?
        .ok_or_else(|| BuildError::Infrastructure("asset owner bundle is missing".to_owned()))?;
    let root = store
        .root_name(bundle_meta.root)
        .map_err(BuildError::infrastructure)?
        .ok_or_else(|| BuildError::Infrastructure("bundle root identity is missing".to_owned()))?;
    let path = scanner
        .physical_path(&root, &bundle_meta.path)
        .map_err(BuildError::infrastructure)?;
    let bundle_bytes = scanner
        .read_identity_checked(&path)
        .map_err(|_| BuildError::Drifted(DriftedInput::File(bundle_meta.path.clone())))?;
    if ContentHash(*blake3::hash(&bundle_bytes).as_bytes()) != bundle_meta.content_hash {
        return Err(BuildError::Drifted(DriftedInput::File(
            bundle_meta.path.clone(),
        )));
    }
    let bundle = distill_bundle::parse_bundle(&bundle_bytes).map_err(BuildError::failed)?;
    let canonical = distill_bundle::write_bundle(&bundle).map_err(BuildError::failed)?;
    if bundle.uuid != bundle_meta.bundle {
        return Err(BuildError::Drifted(DriftedInput::File(
            bundle_meta.path.clone(),
        )));
    }
    let (local_id, entry) =
        find_bundle_asset(&bundle, asset).ok_or(BuildError::Drifted(DriftedInput::Asset(asset)))?;
    if local_id != meta.local_id
        || entry.type_uuid != meta.type_uuid
        || entry.schema_hash != meta.logical_hash
    {
        return Err(BuildError::Drifted(DriftedInput::Asset(asset)));
    }
    Ok(LoadedAsset {
        meta,
        bundle_meta,
        bundle_bytes: canonical,
        entry: entry.clone(),
    })
}

fn find_bundle_asset(bundle: &Bundle, asset: AssetUuid) -> Option<(&str, &AssetEntry)> {
    bundle
        .assets
        .iter()
        .find_map(|(local_id, entry)| (entry.uuid == asset).then_some((local_id.as_str(), entry)))
}

fn merge_artifacts(
    target: &mut BTreeMap<ContentHash, BuildArtifactPublication>,
    source: BTreeMap<ContentHash, BuildArtifactPublication>,
) -> Result<(), BuildError> {
    for (hash, artifact) in source {
        if let Some(existing) = target.get(&hash) {
            if existing != &artifact {
                return Err(BuildError::Failed(
                    "equal content hashes carry different RPC artifact metadata".to_owned(),
                ));
            }
        } else {
            target.insert(hash, artifact);
        }
    }
    Ok(())
}

fn merge_wire_trees(
    target: &mut BTreeMap<LayoutHash, BuildWireTree>,
    source: BTreeMap<LayoutHash, BuildWireTree>,
) -> Result<(), BuildError> {
    for (hash, tree) in source {
        if let Some(existing) = target.get(&hash) {
            if existing != &tree {
                return Err(BuildError::Failed(
                    "equal layout hashes carry different DSWL bodies".to_owned(),
                ));
            }
        } else {
            target.insert(hash, tree);
        }
    }
    Ok(())
}

#[derive(Clone)]
struct TraceEntry {
    asset: AssetUuid,
    bundle: BundleUuid,
    bundle_path: String,
    local_id: String,
    authored_type: TypeUuid,
    terminal_type: TypeUuid,
    role: EntryRole,
    tags: BTreeMap<String, Option<String>>,
}

#[derive(Clone)]
struct StoreTraceSource {
    authoring_hashes: BTreeMap<AssetUuid, BundleFileHash>,
    entries: BTreeMap<AssetUuid, TraceEntry>,
    terminal_types: BTreeMap<AssetUuid, TypeUuid>,
    roles: BTreeMap<AssetUuid, EntryRole>,
    paths: BTreeMap<String, Vec<AssetUuid>>,
    tools: BTreeMap<String, [u8; 32]>,
    current_load: CurrentLoadSource,
    content_hashes: BTreeMap<AssetUuid, ContentHash>,
    tag_poisons: BTreeMap<AssetUuid, BundleUuid>,
}

#[derive(Clone)]
struct CurrentLoadSource {
    capabilities: Vec<(CapabilityKey, [u8; 32])>,
    migration_controls: BTreeMap<AssetUuid, MigrationControlRecord>,
}

struct TraceCaptureBasis<'a> {
    scanner: &'a RootedScanner,
    registry: &'a PipelineRegistry,
    target: &'a Target,
    input_version: distill_store::state::InputVersion,
    epoch: &'a PipelineEpoch,
    dylib_hash: [u8; 32],
    memo: &'a BTreeMap<AssetUuid, NodePublication>,
}

#[derive(Clone)]
struct MigrationControlRecord {
    header: MigrationHeader,
    observed: Observed<ControlValueHash>,
    value: Option<MigrationControlValue>,
}

fn capture_migration_controls(
    store: &Store,
    scanner: &RootedScanner,
) -> Result<BTreeMap<AssetUuid, MigrationControlRecord>, BuildError> {
    let mut records = BTreeMap::new();
    for asset in store.all_asset_ids().map_err(BuildError::infrastructure)? {
        let Some(meta) = store.entry(asset).map_err(BuildError::failed)? else {
            continue;
        };
        if meta.type_uuid != MIGRATION_TYPE_UUID {
            continue;
        }
        let loaded = load_asset(store, scanner, asset)?;
        let bundle =
            distill_bundle::parse_bundle(&loaded.bundle_bytes).map_err(BuildError::failed)?;
        let header = migration_control::decode_header(&loaded.entry.data)
            .map_err(|error| BuildError::Failed(error.to_string()))?;
        let decoded = if !loaded.entry.authoring_only {
            Err((
                ControlFailureCode::WrongRole,
                "Migration control is not authoring-only".to_owned(),
            ))
        } else {
            migration_control::decode(asset, &bundle, &loaded.entry, header)
                .map_err(|error| match error {
                    MigrationDecodeError::Malformed(detail) => {
                        (ControlFailureCode::Malformed, detail)
                    }
                    MigrationDecodeError::SchemaClosure(detail) => {
                        (ControlFailureCode::SchemaClosure, detail)
                    }
                    MigrationDecodeError::Invalid(detail) => {
                        (ControlFailureCode::Malformed, detail)
                    }
                })
                .and_then(|value| {
                    let accepted = store
                        .current_lineage_stamp(value.target_type_uuid)
                        .map_err(|error| (ControlFailureCode::Malformed, error.to_string()))?
                        .ok_or_else(|| {
                            (
                                ControlFailureCode::Malformed,
                                "Migration target type has no accepted lineage authority"
                                    .to_owned(),
                            )
                        })?;
                    migration_control::validate_lineage(&value, &accepted)
                        .map(|()| value)
                        .map_err(|error| (ControlFailureCode::Malformed, error.to_string()))
                })
        };
        let (observed, value) = match decoded {
            Ok(value) => (
                Observed::Ok(ControlValueHash(
                    *blake3::hash(&loaded.bundle_bytes).as_bytes(),
                )),
                Some(value),
            ),
            Err((code, _detail)) => (
                Observed::Err(
                    control_failure_fingerprint(
                        ControlFailureSubject::Read(ControlSubject::Migration(asset)),
                        code,
                        Vec::<AssetUuid>::new(),
                    )
                    .expect("migration control decode failures have valid cardinality"),
                ),
                None,
            ),
        };
        records.insert(
            asset,
            MigrationControlRecord {
                header,
                observed,
                value,
            },
        );
    }
    Ok(records)
}

impl StoreTraceSource {
    fn capture(store: &Store, basis: TraceCaptureBasis<'_>) -> Result<Self, BuildError> {
        let bundle_rows = store.all_bundles().map_err(BuildError::infrastructure)?;
        let bundles = bundle_rows
            .iter()
            .map(|bundle| (bundle.bundle, bundle.path.clone()))
            .collect::<BTreeMap<_, _>>();
        let bundle_hashes = bundle_rows
            .into_iter()
            .map(|bundle| (bundle.bundle, BundleFileHash(bundle.content_hash.0)))
            .collect::<BTreeMap<_, _>>();
        let mut entries = BTreeMap::new();
        let mut authoring_hashes = BTreeMap::new();
        let mut tag_poisons = BTreeMap::new();
        for asset in store.all_asset_ids().map_err(BuildError::infrastructure)? {
            let Some(entry) = store.entry(asset).map_err(BuildError::failed)? else {
                continue;
            };
            let bundle = entry.bundle;
            let bundle_hash = bundle_hashes.get(&bundle).copied().ok_or_else(|| {
                BuildError::Infrastructure("trace entry owner bundle hash is missing".to_owned())
            })?;
            let bundle_path = bundles.get(&bundle).cloned().ok_or_else(|| {
                BuildError::Infrastructure("trace entry owner bundle is missing".to_owned())
            })?;
            entries.insert(
                asset,
                TraceEntry {
                    asset,
                    bundle,
                    bundle_path,
                    local_id: entry.local_id,
                    authored_type: entry.type_uuid,
                    terminal_type: basis
                        .registry
                        .chain(entry.type_uuid, basis.target)
                        .map_err(BuildError::failed)?
                        .terminal,
                    role: if entry.authoring_only {
                        EntryRole::AuthoringOnly
                    } else {
                        EntryRole::Runtime
                    },
                    tags: entry.tags,
                },
            );
            authoring_hashes.insert(asset, bundle_hash);
            if store
                .tag_index_state(asset)
                .map_err(BuildError::infrastructure)?
                .is_some_and(|state| state.poison.is_some())
            {
                tag_poisons.insert(asset, bundle);
            }
        }
        let mut paths = BTreeMap::<String, Vec<AssetUuid>>::new();
        for (path, _, asset) in store
            .all_path_entries()
            .map_err(BuildError::infrastructure)?
        {
            paths.entry(path).or_default().push(asset);
        }
        for assets in paths.values_mut() {
            assets.sort();
            assets.dedup();
        }
        let mut terminal_types = entries
            .iter()
            .map(|(asset, entry)| (*asset, entry.terminal_type))
            .collect::<BTreeMap<_, _>>();
        let mut roles = entries
            .iter()
            .map(|(asset, entry)| (*asset, entry.role))
            .collect::<BTreeMap<_, _>>();
        for (child, parent, output_key) in store
            .all_derived_outputs()
            .map_err(BuildError::infrastructure)?
        {
            let parent_type = entries.get(&parent).ok_or_else(|| {
                BuildError::Infrastructure("derived parent is absent from trace index".to_owned())
            })?;
            let terminal = basis
                .registry
                .chain(parent_type.authored_type, basis.target)
                .map_err(BuildError::failed)?
                .extras
                .get(&output_key)
                .copied()
                .ok_or_else(|| {
                    BuildError::Infrastructure(
                        "derived output is absent from the pinned pipeline map".to_owned(),
                    )
                })?;
            terminal_types.insert(child, terminal);
            roles.insert(child, EntryRole::Runtime);
        }
        let tools = store
            .tool_hashes_at(basis.input_version)
            .map_err(BuildError::infrastructure)?;
        let current_load = CurrentLoadSource::capture(
            store,
            basis.scanner,
            Some(basis.epoch),
            Some(basis.dylib_hash),
        )?;
        let mut content_hashes = BTreeMap::new();
        for publication in basis.memo.values() {
            for output in publication.outputs.values() {
                if content_hashes
                    .insert(output.asset, output.content_hash)
                    .is_some_and(|existing| existing != output.content_hash)
                {
                    return Err(BuildError::Infrastructure(
                        "one build context observed two contents for an asset".to_owned(),
                    ));
                }
            }
        }
        Ok(Self {
            authoring_hashes,
            entries,
            terminal_types,
            roles,
            paths,
            tools,
            current_load,
            content_hashes,
            tag_poisons,
        })
    }

    fn query_results(&self, query: &AssetQuery) -> Vec<AssetUuid> {
        let glob = query
            .path_glob
            .as_ref()
            .and_then(|pattern| globset::Glob::new(pattern).ok())
            .map(|glob| glob.compile_matcher());
        self.entries
            .values()
            .filter(|entry| entry.role == EntryRole::Runtime)
            .filter(|entry| query.uuid.is_none_or(|uuid| uuid == entry.asset))
            .filter(|entry| {
                query
                    .bundle_path
                    .as_ref()
                    .is_none_or(|path| path == &entry.bundle_path)
            })
            .filter(|entry| {
                query
                    .local_id
                    .as_ref()
                    .is_none_or(|local_id| local_id == &entry.local_id)
            })
            .filter(|entry| {
                query
                    .bundle_uuid
                    .is_none_or(|bundle| bundle == entry.bundle)
            })
            .filter(|entry| {
                query
                    .authored_type
                    .is_none_or(|authored| authored == entry.authored_type)
            })
            .filter(|entry| {
                query
                    .terminal_type
                    .is_none_or(|terminal| terminal == entry.terminal_type)
            })
            .filter(|entry| {
                query.tag.as_ref().is_none_or(|tag| {
                    entry.tags.get(&tag.tag).is_some_and(|actual| {
                        tag.value
                            .as_ref()
                            .is_none_or(|wanted| actual.as_ref() == Some(wanted))
                    })
                })
            })
            .filter(|entry| {
                query
                    .path_prefix
                    .as_ref()
                    .is_none_or(|prefix| entry.bundle_path.starts_with(prefix))
            })
            .filter(|entry| {
                glob.as_ref()
                    .is_none_or(|glob| glob.is_match(&entry.bundle_path))
            })
            .map(|entry| entry.asset)
            .collect()
    }
}

impl CurrentLoadSource {
    fn capture(
        store: &Store,
        scanner: &RootedScanner,
        epoch: Option<&PipelineEpoch>,
        dylib_hash: Option<[u8; 32]>,
    ) -> Result<Self, BuildError> {
        let capabilities = match (epoch, dylib_hash) {
            (Some(epoch), Some(dylib_hash)) => epoch
                .default_table_types()
                .into_iter()
                .map(CapabilityKey::DefaultTable)
                .chain(
                    epoch
                        .migration_function_keys()
                        .into_iter()
                        .map(CapabilityKey::MigrationFn),
                )
                .map(|key| (key, dylib_hash))
                .collect(),
            _ => Vec::new(),
        };
        Ok(Self {
            capabilities,
            migration_controls: capture_migration_controls(store, scanner)?,
        })
    }

    fn migration_assets(&self, type_uuid: TypeUuid, from_hash: LogicalHash) -> Vec<AssetUuid> {
        self.migration_controls
            .iter()
            .filter(|(_, record)| {
                record.header.target_type_uuid == type_uuid && record.header.from_hash == from_hash
            })
            .map(|(asset, _)| *asset)
            .collect()
    }

    fn capability(&self, key: &CapabilityKey) -> Observed<[u8; 32]> {
        self.capabilities
            .iter()
            .find_map(|(registered, hash)| (registered == key).then_some(*hash))
            .map_or_else(
                || Observed::Err(StableFailureFingerprint::MissingCapability { key: key.clone() }),
                Observed::Ok,
            )
    }

    fn control(&self, query: &ControlQuery) -> Observed<[u8; 32]> {
        match query {
            ControlQuery::MigrationEdges {
                type_uuid,
                from_hash,
            } => Observed::Ok(asset_query_result_hash(
                &self.migration_assets(*type_uuid, *from_hash),
            )),
            ControlQuery::DirectoryImportRuleSet => no_trace(),
        }
    }

    fn control_read(&self, subject: &ControlSubject) -> Observed<ControlValueHash> {
        match subject {
            ControlSubject::Migration(asset) => self
                .migration_controls
                .get(asset)
                .map(|record| record.observed.clone())
                .unwrap_or_else(|| {
                    Observed::Err(
                        control_failure_fingerprint(
                            ControlFailureSubject::Read(subject.clone()),
                            ControlFailureCode::Missing,
                            Vec::<AssetUuid>::new(),
                        )
                        .expect("missing control read has valid cardinality"),
                    )
                }),
            _ => no_trace(),
        }
    }
}

fn no_trace<T>() -> Observed<T> {
    Observed::Err(StableFailureFingerprint::Local {
        class: LocalFailureClass::ArtifactEncoding,
        detail: [0; 32],
    })
}

impl TraceSource for StoreTraceSource {
    fn authoring_read(&self, asset: AssetUuid) -> Observed<Option<BundleFileHash>> {
        Observed::Ok(self.authoring_hashes.get(&asset).copied())
    }

    fn read(&self, asset: AssetUuid) -> Observed<ContentHash> {
        self.content_hashes.get(&asset).copied().map_or_else(
            || {
                Observed::Err(StableFailureFingerprint::MissingRef {
                    query: Box::new(AssetQuery {
                        uuid: Some(asset),
                        ..AssetQuery::default()
                    }),
                    expected_terminal: self
                        .terminal_types
                        .get(&asset)
                        .copied()
                        .unwrap_or(TypeUuid([0; 16])),
                })
            },
            Observed::Ok,
        )
    }

    fn resolve(&self, path: &str) -> Observed<Option<AssetUuid>> {
        match self.paths.get(path).map(Vec::as_slice).unwrap_or_default() {
            [] => Observed::Ok(None),
            [asset] => Observed::Ok(Some(*asset)),
            conflicting => Observed::Err(StableFailureFingerprint::Ambiguous {
                conflicting: conflicting.to_vec(),
            }),
        }
    }

    fn query(&self, query: &AssetQuery) -> Observed<[u8; 32]> {
        if query.tag.is_some() {
            let mut without_tag = query.clone();
            without_tag.tag = None;
            let candidates = self.query_results(&without_tag);
            if let Some(bundle) = candidates
                .iter()
                .filter_map(|asset| self.tag_poisons.get(asset))
                .min()
            {
                return Observed::Err(StableFailureFingerprint::Poisoned { bundle: *bundle });
            }
        }
        Observed::Ok(asset_query_result_hash(&self.query_results(query)))
    }

    fn tool(&self, id: &str) -> Observed<[u8; 32]> {
        self.tools.get(id).copied().map_or_else(
            || {
                Observed::Err(StableFailureFingerprint::MissingCapability {
                    key: CapabilityKey::Tool(id.to_owned()),
                })
            },
            Observed::Ok,
        )
    }

    fn capability(&self, key: &CapabilityKey) -> Observed<[u8; 32]> {
        self.current_load.capability(key)
    }

    fn ref_check(&self, asset: AssetUuid, _expected: TypeUuid) -> Observed<Option<TypeUuid>> {
        Observed::Ok(self.terminal_types.get(&asset).copied())
    }

    fn role_check(&self, asset: AssetUuid) -> Observed<Option<EntryRole>> {
        Observed::Ok(self.roles.get(&asset).copied())
    }

    fn control(&self, query: &ControlQuery) -> Observed<[u8; 32]> {
        self.current_load.control(query)
    }

    fn control_read(&self, subject: &ControlSubject) -> Observed<ControlValueHash> {
        self.current_load.control_read(subject)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::callbacks::{
        Diagnostics, PipelineProcessContext, PipelineProcessor, PipelineValidator, ProcessorError,
        ProcessorProduct, ProcessorProducts, ValidatorDescriptor,
    };
    use distill_build::outputs::OutputDecls;
    use distill_build::pipeline::{GraphicsApi, TargetArch, TargetOs, TargetSelector};
    use distill_bundle::{EntryLineageV1, LineageStamp};
    use distill_core::lineage::AcceptedSchemaEpoch;
    use distill_json::AuthoredValue;
    use distill_rpc::{
        AuthoringEntry, AuthoringEntryRole, AuthoringValue, Commit, InputVersion, TargetDefinition,
        TargetDefinitionHash,
    };
    use distill_schema::ngp_schema::{
        node_hash, snapshot_to_json, Field, FieldAttrs, FieldIdentifier, FieldLayout,
        LayoutIdentity, LogicalSchema, PrimitiveKind, PrimitiveType, Schema, SchemaLayouts,
        SchemaNode, SchemaTypeId, TypeAttrs, TypeDef, TypeLayout, TypePath,
    };
    use distill_store::pipeline::{
        AcceptedTypeLineage, SchemaLineageManifest, TypeAuthorityState,
        VerifiedSchemaLineageManifest,
    };
    use distill_store::StoreConfig;

    use crate::coordinator::LineageDestination;
    use crate::scanner::AssetRoot;

    const TYPE: TypeUuid = TypeUuid([71; 16]);
    const TERMINAL: TypeUuid = TypeUuid([74; 16]);
    const EXTRA: TypeUuid = TypeUuid([75; 16]);
    const ASSET: AssetUuid = AssetUuid([72; 16]);
    const BUNDLE: BundleUuid = BundleUuid([73; 16]);
    const DEPENDENCY_ASSET: AssetUuid = AssetUuid([78; 16]);
    const DEPENDENCY_BUNDLE: BundleUuid = BundleUuid([79; 16]);
    const MIGRATION_ASSET: AssetUuid = AssetUuid([76; 16]);
    const MIGRATION_BUNDLE: BundleUuid = BundleUuid([77; 16]);

    fn test_layout_identity() -> LayoutIdentity {
        LayoutIdentity {
            target_triple: "x86_64-unknown-linux-gnu".into(),
            rustc: "rustc test".into(),
            algorithm_version: 1,
        }
    }

    #[test]
    fn artifact_encoding_failures_are_memoized_only_with_exact_dslf_facts() {
        assert_eq!(
            artifact_encoding_failure(&ArtifactEncodeError::Value(
                EncodeError::DuplicateOrderedValue {
                    path: "$.items".to_owned(),
                },
            )),
            Some(ArtifactEncodingFailureV1::NonCanonicalOrder {
                path: FieldPath::of(&["items"]),
            })
        );
        assert_eq!(
            artifact_encoding_failure(&ArtifactEncodeError::Artifact(
                ArtifactError::VarLenTooLarge {
                    var_len: u32::MAX as u64 + 1,
                },
            )),
            Some(ArtifactEncodingFailureV1::SizeLimit {
                limit: u32::MAX as u64,
                observed: u32::MAX as u64 + 1,
            })
        );
        assert_eq!(
            artifact_encoding_failure(&ArtifactEncodeError::Value(EncodeError::Shape {
                path: "$.value".to_owned(),
                expected: "record",
            })),
            None
        );
    }

    #[test]
    fn runtime_build_closures_reject_schema_authoritative_build_only_types() {
        let normal = authority();
        let mut schema = normal.schema().clone();
        schema.types[0].attrs.build_only = true;
        let build_only = ProjectSchemaAuthority::from_schema(schema, [9; 32]).unwrap();
        let chain = PipelineChain {
            authored: TYPE,
            terminal: TYPE,
            stages: Vec::new(),
            extras: BTreeMap::new(),
        };
        assert!(matches!(
            validate_runtime_chain(&build_only, &chain),
            Err(BuildError::Failed(message)) if message.contains("build-only type")
        ));
    }

    #[test]
    fn conservative_tag_index_preserves_exact_bundle_identity() {
        let other_asset = AssetUuid([80; 16]);
        let other_bundle = BundleUuid([81; 16]);
        let assets = BTreeMap::from([(ASSET, BUNDLE), (other_asset, other_bundle)]);

        let index = PublishedTagIndex::conservatively_poisoned(&assets);

        assert_eq!(index.poisons, assets);
        assert_eq!(
            index.tags.keys().copied().collect::<Vec<_>>(),
            vec![ASSET, other_asset]
        );
        assert!(index.tags.values().all(BTreeMap::is_empty));
    }

    fn path(krate: &str, name: &str) -> TypePath {
        TypePath {
            name: Some(name.to_owned()),
            containing_type: None,
            modules: Vec::new(),
            krate: krate.to_owned(),
        }
    }

    fn authority() -> ProjectSchemaAuthority {
        ProjectSchemaAuthority::from_schema(
            Schema {
                source_hashes: BTreeMap::new(),
                types: vec![
                    TypeDef {
                        id: SchemaTypeId(0),
                        kind: PrimitiveType::Struct,
                        path: path("game", "ByteAsset"),
                        uuid: Some(TYPE),
                        attrs: TypeAttrs::default(),
                        fields: vec![Field {
                            id: FieldIdentifier::Name("value".to_owned()),
                            type_id: SchemaTypeId(1),
                            attrs: FieldAttrs::default(),
                        }],
                        generic_parameters: Vec::new(),
                        generic_argument_ids: Vec::new(),
                        has_default: false,
                    },
                    TypeDef {
                        id: SchemaTypeId(1),
                        kind: PrimitiveType::U8,
                        path: path("core", "u8"),
                        uuid: None,
                        attrs: TypeAttrs::default(),
                        fields: Vec::new(),
                        generic_parameters: Vec::new(),
                        generic_argument_ids: Vec::new(),
                        has_default: true,
                    },
                    TypeDef {
                        id: SchemaTypeId(2),
                        kind: PrimitiveType::Struct,
                        path: path("game", "CookedByteAsset"),
                        uuid: Some(TERMINAL),
                        attrs: TypeAttrs::default(),
                        fields: vec![Field {
                            id: FieldIdentifier::Name("value".to_owned()),
                            type_id: SchemaTypeId(1),
                            attrs: FieldAttrs::default(),
                        }],
                        generic_parameters: Vec::new(),
                        generic_argument_ids: Vec::new(),
                        has_default: false,
                    },
                    TypeDef {
                        id: SchemaTypeId(3),
                        kind: PrimitiveType::Struct,
                        path: path("game", "ByteMetadata"),
                        uuid: Some(EXTRA),
                        attrs: TypeAttrs::default(),
                        fields: vec![Field {
                            id: FieldIdentifier::Name("value".to_owned()),
                            type_id: SchemaTypeId(1),
                            attrs: FieldAttrs::default(),
                        }],
                        generic_parameters: Vec::new(),
                        generic_argument_ids: Vec::new(),
                        has_default: false,
                    },
                ],
                layouts: vec![SchemaLayouts {
                    identity: test_layout_identity(),
                    layouts: vec![
                        TypeLayout {
                            size: Some(1),
                            align: Some(1),
                            layout_complete: true,
                            tag_encoding: None,
                            fields: vec![FieldLayout {
                                offset: Some(0),
                                field_size: Some(1),
                            }],
                        },
                        TypeLayout {
                            size: Some(1),
                            align: Some(1),
                            layout_complete: true,
                            tag_encoding: None,
                            fields: Vec::new(),
                        },
                        TypeLayout {
                            size: Some(1),
                            align: Some(1),
                            layout_complete: true,
                            tag_encoding: None,
                            fields: vec![FieldLayout {
                                offset: Some(0),
                                field_size: Some(1),
                            }],
                        },
                        TypeLayout {
                            size: Some(1),
                            align: Some(1),
                            layout_complete: true,
                            tag_encoding: None,
                            fields: vec![FieldLayout {
                                offset: Some(0),
                                field_size: Some(1),
                            }],
                        },
                    ],
                }],
            },
            [9; 32],
        )
        .unwrap()
    }

    fn primitive_authority() -> ProjectSchemaAuthority {
        ProjectSchemaAuthority::from_schema(
            Schema {
                source_hashes: BTreeMap::new(),
                types: vec![
                    TypeDef {
                        id: SchemaTypeId(0),
                        kind: PrimitiveType::Struct,
                        path: path("game", "WidenedAsset"),
                        uuid: Some(TYPE),
                        attrs: TypeAttrs::default(),
                        fields: vec![Field {
                            id: FieldIdentifier::Name("value".to_owned()),
                            type_id: SchemaTypeId(1),
                            attrs: FieldAttrs::default(),
                        }],
                        generic_parameters: Vec::new(),
                        generic_argument_ids: Vec::new(),
                        has_default: false,
                    },
                    TypeDef {
                        id: SchemaTypeId(1),
                        kind: PrimitiveType::U16,
                        path: path("core", "u16"),
                        uuid: None,
                        attrs: TypeAttrs::default(),
                        fields: Vec::new(),
                        generic_parameters: Vec::new(),
                        generic_argument_ids: Vec::new(),
                        has_default: true,
                    },
                    TypeDef {
                        id: SchemaTypeId(2),
                        kind: PrimitiveType::Struct,
                        path: path("game", "WidenedTerminal"),
                        uuid: Some(TERMINAL),
                        attrs: TypeAttrs::default(),
                        fields: vec![Field {
                            id: FieldIdentifier::Name("value".to_owned()),
                            type_id: SchemaTypeId(1),
                            attrs: FieldAttrs::default(),
                        }],
                        generic_parameters: Vec::new(),
                        generic_argument_ids: Vec::new(),
                        has_default: false,
                    },
                ],
                layouts: vec![SchemaLayouts {
                    identity: test_layout_identity(),
                    layouts: vec![
                        TypeLayout {
                            size: Some(2),
                            align: Some(2),
                            layout_complete: true,
                            tag_encoding: None,
                            fields: vec![FieldLayout {
                                offset: Some(0),
                                field_size: Some(2),
                            }],
                        },
                        TypeLayout {
                            size: Some(2),
                            align: Some(2),
                            layout_complete: true,
                            tag_encoding: None,
                            fields: Vec::new(),
                        },
                        TypeLayout {
                            size: Some(2),
                            align: Some(2),
                            layout_complete: true,
                            tag_encoding: None,
                            fields: vec![FieldLayout {
                                offset: Some(0),
                                field_size: Some(2),
                            }],
                        },
                    ],
                }],
            },
            [10; 32],
        )
        .unwrap()
    }

    fn rpc_target(hash: TargetDefinitionHash) -> TargetDefinition {
        TargetDefinition::new("dev", hash)
    }

    fn authored_object(
        fields: impl IntoIterator<Item = (impl Into<String>, AuthoredValue)>,
    ) -> AuthoredValue {
        AuthoredValue::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        )
    }

    fn authored_variant(name: &str, fields: Vec<(&str, AuthoredValue)>) -> AuthoredValue {
        authored_object([(name, authored_object(fields))])
    }

    fn authored_bytes(bytes: &[u8]) -> AuthoredValue {
        AuthoredValue::Array(
            bytes
                .iter()
                .map(|byte| AuthoredValue::UInt(u128::from(*byte)))
                .collect(),
        )
    }

    fn authored_path(path: &FieldPath) -> AuthoredValue {
        authored_object([(
            "segments",
            AuthoredValue::Array(
                path.0
                    .iter()
                    .map(|segment| AuthoredValue::Str(segment.clone()))
                    .collect(),
            ),
        )])
    }

    fn authored_neutral_value(value: &AuthoredValue) -> AuthoredValue {
        match value {
            AuthoredValue::Null => authored_variant("Null", vec![]),
            AuthoredValue::Bool(value) => {
                authored_variant("Bool", vec![("value", AuthoredValue::Bool(*value))])
            }
            AuthoredValue::Int(value) => {
                authored_variant("Int", vec![("value", AuthoredValue::Int(*value))])
            }
            AuthoredValue::UInt(value) => {
                authored_variant("UInt", vec![("value", AuthoredValue::UInt(*value))])
            }
            AuthoredValue::Float(value) => {
                authored_variant("Float", vec![("value", AuthoredValue::Float(*value))])
            }
            AuthoredValue::Str(value) => {
                authored_variant("Str", vec![("value", AuthoredValue::Str(value.clone()))])
            }
            AuthoredValue::Array(values) => authored_variant(
                "Array",
                vec![(
                    "value",
                    AuthoredValue::Array(values.iter().map(authored_neutral_value).collect()),
                )],
            ),
            AuthoredValue::Object(values) => authored_variant(
                "Object",
                vec![(
                    "value",
                    AuthoredValue::Object(
                        values
                            .iter()
                            .map(|(key, value)| (key.clone(), authored_neutral_value(value)))
                            .collect(),
                    ),
                )],
            ),
            AuthoredValue::Blob(value) => {
                authored_variant("Blob", vec![("value", AuthoredValue::Blob(value.clone()))])
            }
        }
    }

    fn authored_migration_op(op: &MigrationOp) -> AuthoredValue {
        match op {
            MigrationOp::DropField { at } => {
                authored_variant("DropField", vec![("at", authored_path(at))])
            }
            MigrationOp::WriteValue { to, value } => authored_variant(
                "WriteValue",
                vec![
                    ("to", authored_path(to)),
                    ("value", authored_neutral_value(value)),
                ],
            ),
            _ => panic!("test helper only encodes the migration operations used here"),
        }
    }

    fn authored_lineage(epochs: &[AcceptedSchemaEpoch], cursor: u32) -> AuthoredValue {
        authored_object([
            (
                "chain",
                authored_bytes(&lineage_chain_digest(TYPE, epochs, cursor)),
            ),
            ("cursor", AuthoredValue::UInt(u128::from(cursor))),
            (
                "epochs",
                AuthoredValue::Array(
                    epochs
                        .iter()
                        .map(|epoch| {
                            authored_object([
                                ("digest", authored_bytes(&epoch.digest.0)),
                                (
                                    "forward_parent",
                                    epoch.forward_parent.map_or(AuthoredValue::Null, |parent| {
                                        AuthoredValue::UInt(u128::from(parent))
                                    }),
                                ),
                            ])
                        })
                        .collect(),
                ),
            ),
        ])
    }

    fn migration_value(
        old_hash: LogicalHash,
        current_hash: LogicalHash,
        old_epochs: &[AcceptedSchemaEpoch],
        current_epochs: &[AcceptedSchemaEpoch],
        kind: MigrationControlKind,
    ) -> AuthoredValue {
        let kind = match kind {
            MigrationControlKind::Ops(ops) => authored_variant(
                "Ops",
                vec![(
                    "ops",
                    AuthoredValue::Array(ops.iter().map(authored_migration_op).collect()),
                )],
            ),
            MigrationControlKind::Function { key } => {
                authored_variant("Function", vec![("key", AuthoredValue::Str(key))])
            }
        };
        authored_object([
            ("from_hash", authored_bytes(&old_hash.0)),
            ("from_lineage", authored_lineage(old_epochs, 0)),
            ("kind", kind),
            ("target_type_uuid", authored_bytes(&TYPE.0)),
            ("to_hash", authored_bytes(&current_hash.0)),
            ("to_lineage", authored_lineage(current_epochs, 1)),
        ])
    }

    fn custom_write_migration_bundle(
        old_hash: LogicalHash,
        old_schema: &LogicalSchema,
        current_hash: LogicalHash,
        current_schema: &LogicalSchema,
    ) -> Bundle {
        migration_bundle_with_kind(
            old_hash,
            old_schema,
            current_hash,
            current_schema,
            MigrationControlKind::Ops(vec![
                MigrationOp::DropField {
                    at: FieldPath(vec!["value".to_owned()]),
                },
                MigrationOp::WriteValue {
                    to: FieldPath(vec!["value".to_owned()]),
                    value: AuthoredValue::UInt(42),
                },
            ]),
        )
    }

    fn migration_bundle_with_kind(
        old_hash: LogicalHash,
        old_schema: &LogicalSchema,
        current_hash: LogicalHash,
        current_schema: &LogicalSchema,
        kind: MigrationControlKind,
    ) -> Bundle {
        let old_epochs = vec![AcceptedSchemaEpoch {
            digest: old_hash,
            forward_parent: None,
        }];
        let current_epochs = vec![
            AcceptedSchemaEpoch {
                digest: old_hash,
                forward_parent: None,
            },
            AcceptedSchemaEpoch {
                digest: current_hash,
                forward_parent: Some(0),
            },
        ];
        let migration = migration_value(old_hash, current_hash, &old_epochs, &current_epochs, kind);
        let row = distill_core::bootstrap::BootstrapControlSpecV1::embedded()
            .unwrap()
            .0
            .into_iter()
            .find(|row| row.symbol == distill_core::bootstrap::BootstrapControlSymbol::Migration)
            .unwrap();
        let migration_schema =
            distill_schema::ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
        let migration_hash = row.logical_hash;
        Bundle {
            format_version: 1,
            uuid: MIGRATION_BUNDLE,
            primary: None,
            schemas: BTreeMap::from([
                (old_hash, old_schema.clone()),
                (current_hash, current_schema.clone()),
                (migration_hash, migration_schema),
            ]),
            assets: BTreeMap::from([(
                "migration".to_owned(),
                AssetEntry {
                    uuid: MIGRATION_ASSET,
                    type_uuid: row.type_uuid,
                    schema_hash: migration_hash,
                    lineage: EntryLineageV1::Bootstrap {
                        bundle_format_version: 1,
                    },
                    authoring_only: true,
                    data: migration,
                },
            )]),
        }
    }

    struct PrimaryCountingProcessor(Arc<AtomicUsize>);

    impl PipelineProcessor for PrimaryCountingProcessor {
        fn process(
            &self,
            input: AuthoredValue,
            _context: &mut dyn PipelineProcessContext,
        ) -> Result<ProcessorProducts, ProcessorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ProcessorProducts {
                primary: Some(ProcessorProduct::new(TERMINAL, input)),
                extras: BTreeMap::new(),
                debug: BTreeMap::new(),
            })
        }
    }

    struct CountingValidator(Arc<AtomicUsize>);

    impl PipelineValidator for CountingValidator {
        fn validate(
            &self,
            _asset: &AuthoredValue,
            _diagnostics: &mut Diagnostics,
        ) -> Result<(), distill_asset::CallbackPanic> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct StoreLockProbeProcessor {
        calls: Arc<AtomicUsize>,
        store: Arc<Mutex<Store>>,
        observed_unlocked: Arc<AtomicBool>,
    }

    impl PipelineProcessor for StoreLockProbeProcessor {
        fn process(
            &self,
            input: AuthoredValue,
            context: &mut dyn PipelineProcessContext,
        ) -> Result<ProcessorProducts, ProcessorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.observed_unlocked
                .store(self.store.try_lock().is_ok(), Ordering::SeqCst);
            let AuthoredValue::Object(fields) = &input else {
                panic!("processor input is a struct");
            };
            if fields.get("value") == Some(&AuthoredValue::UInt(7)) {
                assert_eq!(context.target().unwrap().os, TargetOs::Linux);
                let outputs = context.outputs().unwrap();
                assert_eq!(outputs.parent(), ASSET);
                assert_eq!(
                    outputs.child("metadata").unwrap(),
                    AssetUuid::v5(ASSET, "metadata")
                );
                let query = AssetQuery {
                    authored_type: Some(TYPE),
                    ..AssetQuery::default()
                };
                assert_eq!(
                    context.query(&query).unwrap(),
                    vec![ASSET, DEPENDENCY_ASSET]
                );
                let read = context.read(DEPENDENCY_ASSET, TERMINAL).unwrap();
                assert_eq!(read.asset, DEPENDENCY_ASSET);
                assert_eq!(read.terminal_type, TERMINAL);
                let resolved = context.read_path("dependency.bundle", TERMINAL).unwrap();
                assert_eq!(resolved.content_hash, read.content_hash);
            }
            Ok(ProcessorProducts {
                primary: Some(ProcessorProduct::new(TERMINAL, input.clone())),
                extras: BTreeMap::from([(
                    "metadata".to_owned(),
                    ProcessorProduct::new(EXTRA, input),
                )]),
                debug: BTreeMap::new(),
            })
        }
    }

    #[test]
    fn cached_processor_type_set_must_be_exact_and_canonical() {
        let expected = canonical_output_type_set(TYPE, TERMINAL, TERMINAL);
        assert_eq!(expected, vec![TYPE, TERMINAL]);
        assert!(validate_cached_output_type_set(&expected, TYPE, TERMINAL, TERMINAL).is_ok());
        assert!(validate_cached_output_type_set(&[TYPE], TYPE, TERMINAL, TERMINAL).is_err());
        assert!(
            validate_cached_output_type_set(&[TERMINAL, TYPE], TYPE, TERMINAL, TERMINAL).is_err()
        );
        assert!(validate_cached_output_type_set(
            &[TYPE, TERMINAL, TERMINAL],
            TYPE,
            TERMINAL,
            TERMINAL
        )
        .is_err());
    }

    #[test]
    fn production_backend_replans_when_a_custom_migration_shadows_the_automatic_tail() {
        let temp = tempfile::tempdir().unwrap();
        let assets = temp.path().join("assets");
        std::fs::create_dir_all(&assets).unwrap();
        let authority = Arc::new(primitive_authority());
        let project = authority.project_type(TYPE).unwrap();
        let old_schema = LogicalSchema {
            root: SchemaNode::Struct {
                rev: 0,
                fields: vec![(
                    "value".to_owned(),
                    0,
                    SchemaNode::Primitive(PrimitiveKind::U8),
                )],
            },
        };
        let old_hash = node_hash(&old_schema.root).unwrap();
        let epochs = vec![AcceptedSchemaEpoch {
            digest: old_hash,
            forward_parent: None,
        }];
        let bundle = Bundle {
            format_version: 1,
            uuid: BUNDLE,
            primary: Some("entry".to_owned()),
            schemas: BTreeMap::from([(old_hash, old_schema.clone())]),
            assets: BTreeMap::from([(
                "entry".to_owned(),
                AssetEntry {
                    uuid: ASSET,
                    type_uuid: TYPE,
                    schema_hash: old_hash,
                    lineage: EntryLineageV1::Manifest(LineageStamp {
                        chain: lineage_chain_digest(TYPE, &epochs, 0),
                        epochs: epochs.clone(),
                        cursor: 0,
                    }),
                    authoring_only: false,
                    data: AuthoredValue::Object(BTreeMap::from([(
                        "value".to_owned(),
                        AuthoredValue::UInt(7),
                    )])),
                },
            )]),
        };
        std::fs::write(
            assets.join("widen.bundle"),
            distill_bundle::write_bundle(&bundle).unwrap(),
        )
        .unwrap();
        let build_target = Target::new(
            TargetOs::Linux,
            TargetArch::X86_64,
            BTreeSet::from([GraphicsApi::new("vulkan").unwrap()]),
            false,
            true,
            authority.identity().clone(),
        )
        .unwrap();
        let target_hash =
            TargetDefinitionHash(distill_build::keys::target_definition_hash(&build_target));
        let coordinator = Arc::new(
            DaemonCoordinator::open(
                StoreConfig::new(temp.path().join("state")),
                vec![AssetRoot::new(
                    "main",
                    &assets,
                    assets.join(".distill-displaced"),
                )],
                LineageDestination {
                    root: "main".to_owned(),
                    path: "schema/lineage.bundle".to_owned(),
                },
                vec![rpc_target(target_hash)],
                64,
            )
            .unwrap(),
        );
        coordinator.reconcile_full_scan().unwrap();
        let manifest = VerifiedSchemaLineageManifest::from_verified_source(
            ContentHash([3; 32]),
            SchemaLineageManifest {
                types: BTreeMap::from([(
                    TYPE,
                    AcceptedTypeLineage {
                        epochs: vec![
                            AcceptedSchemaEpoch {
                                digest: old_hash,
                                forward_parent: None,
                            },
                            AcceptedSchemaEpoch {
                                digest: project.logical_hash,
                                forward_parent: Some(0),
                            },
                        ],
                        current: 1,
                        authority: TypeAuthorityState::Active,
                    },
                )]),
            },
        );
        let store = coordinator.store();
        let current_snapshot = snapshot_to_json(&project.logical_schema).unwrap();
        coordinator
            .server()
            .coordinated_commit(InputVersion(1), || {
                store
                    .lock()
                    .unwrap()
                    .input_transaction(|transaction| {
                        transaction.put_schema(project.logical_hash, &current_snapshot)?;
                        transaction.project_verified_lineage_manifest(&manifest)
                    })
                    .map_err(|error| error.to_string())?;
                Ok(Commit::default())
            })
            .unwrap();
        coordinator.install_schema_authority_for_test(authority.clone());
        coordinator.install_build_target_for_test("dev", build_target.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        coordinator.install_pipeline_epoch_for_test(crate::epoch::processor_test_epoch(
            "dev",
            target_hash.0,
            crate::callbacks::ProcessorDescriptor {
                id: "identity".to_owned(),
                version: 1,
                input: TYPE,
                selector: TargetSelector::new(None, None).unwrap(),
                outputs: OutputDecls::new(TERMINAL, Vec::<(String, TypeUuid)>::new()).unwrap(),
            },
            PrimaryCountingProcessor(Arc::clone(&calls)),
        ));
        refine_published_tag_index(
            coordinator.store(),
            coordinator.scanner(),
            Arc::clone(&authority),
            coordinator.pipeline_snapshot(),
            &BTreeMap::from([("dev".to_owned(), build_target)]),
            64,
            &BTreeMap::from([(ASSET, BUNDLE)]),
        );
        let indexed = coordinator
            .store()
            .lock()
            .unwrap()
            .tag_index_state(ASSET)
            .unwrap()
            .unwrap();
        assert_eq!(indexed.planner_version, Some(MIGRATION_PLANNER_VERSION));
        assert!(indexed.dylib_hash.is_some());
        assert!(indexed.poison.is_none());
        let mut request = BuildRequest {
            work_class: BuildWorkClass::Interactive,
            basis: coordinator.server().current_stamp(),
            target: "dev".to_owned(),
            target_definition: target_hash,
            requested_asset: ASSET,
            output_key: String::new(),
            requested_terminal_type: TERMINAL,
            entry: AuthoringEntry {
                uuid: ASSET,
                bundle: BUNDLE,
                local_id: "entry".to_owned(),
                normalized_path: "widen.bundle".to_owned(),
                type_uuid: TYPE,
                terminal_type: TERMINAL,
                schema_hash: old_hash,
                logical_schema: Arc::from(snapshot_to_json(&old_schema).unwrap().into_bytes()),
                role: AuthoringEntryRole::Runtime,
                tags: BTreeMap::new(),
                value: AuthoringValue {
                    canonical_value: Arc::from(&b"{\"value\":7}"[..]),
                    blobs: Vec::new(),
                },
            },
            drifted_input: DriftedInput::Asset(ASSET),
        };

        let first = build(&coordinator, &request).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let root = first
            .artifacts
            .iter()
            .find(|artifact| artifact.content_hash == first.root_content_hash)
            .unwrap();
        let blobs = root
            .payload
            .blobs
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>();
        let parsed =
            distill_wire::artifact::parse_artifact_parts(&root.payload.structural, &blobs).unwrap();
        assert_eq!(parsed.fixed, [7, 0]);

        let hydrated = build(&coordinator, &request).unwrap();
        assert_eq!(hydrated, first);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let migration = custom_write_migration_bundle(
            old_hash,
            &old_schema,
            project.logical_hash,
            &project.logical_schema,
        );
        std::fs::write(
            assets.join("migration.bundle"),
            distill_bundle::write_bundle(&migration).unwrap(),
        )
        .unwrap();
        coordinator.reconcile_full_scan().unwrap();
        request.basis = coordinator.server().current_stamp();

        let custom = build(&coordinator, &request).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let root = custom
            .artifacts
            .iter()
            .find(|artifact| artifact.content_hash == custom.root_content_hash)
            .unwrap();
        let blobs = root
            .payload
            .blobs
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>();
        let parsed =
            distill_wire::artifact::parse_artifact_parts(&root.payload.structural, &blobs).unwrap();
        assert_eq!(parsed.fixed, [42, 0]);

        let function_migration = migration_bundle_with_kind(
            old_hash,
            &old_schema,
            project.logical_hash,
            &project.logical_schema,
            MigrationControlKind::Function {
                key: "upgrade".to_owned(),
            },
        );
        std::fs::write(
            assets.join("migration.bundle"),
            distill_bundle::write_bundle(&function_migration).unwrap(),
        )
        .unwrap();
        coordinator.reconcile_full_scan().unwrap();
        let migration_calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::clone(&migration_calls);
        coordinator.install_pipeline_epoch_for_test(crate::epoch::processor_test_epoch_with(
            "dev",
            target_hash.0,
            crate::callbacks::ProcessorDescriptor {
                id: "identity".to_owned(),
                version: 1,
                input: TYPE,
                selector: TargetSelector::new(None, None).unwrap(),
                outputs: OutputDecls::new(TERMINAL, Vec::<(String, TypeUuid)>::new()).unwrap(),
            },
            PrimaryCountingProcessor(Arc::clone(&calls)),
            move |arena| {
                arena
                    .register_migration(
                        "upgrade",
                        move |mut value: AuthoredValue| -> Result<
                            AuthoredValue,
                            crate::callbacks::MigrationFunctionError,
                        > {
                            callback_calls.fetch_add(1, Ordering::SeqCst);
                            let AuthoredValue::Object(fields) = &mut value else {
                                panic!("test migration input is a struct");
                            };
                            fields.insert("value".to_owned(), AuthoredValue::UInt(55));
                            Ok(value)
                        },
                    )
                    .into_result()
                    .unwrap();
            },
        ));
        request.basis = coordinator.server().current_stamp();

        let function = build(&coordinator, &request).unwrap();
        assert_eq!(migration_calls.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let root = function
            .artifacts
            .iter()
            .find(|artifact| artifact.content_hash == function.root_content_hash)
            .unwrap();
        let blobs = root
            .payload
            .blobs
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>();
        let parsed =
            distill_wire::artifact::parse_artifact_parts(&root.payload.structural, &blobs).unwrap();
        assert_eq!(parsed.fixed, [55, 0]);

        assert_eq!(build(&coordinator, &request).unwrap(), function);
        assert_eq!(migration_calls.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn production_backend_persists_and_hydrates_the_complete_processor_chain() {
        let temp = tempfile::tempdir().unwrap();
        let assets = temp.path().join("assets");
        std::fs::create_dir_all(&assets).unwrap();
        let authority = Arc::new(authority());
        let project = authority.project_type(TYPE).unwrap();
        let build_target = Target::new(
            TargetOs::Linux,
            TargetArch::X86_64,
            BTreeSet::from([GraphicsApi::new("vulkan").unwrap()]),
            false,
            true,
            authority.identity().clone(),
        )
        .unwrap();
        let target_hash =
            TargetDefinitionHash(distill_build::keys::target_definition_hash(&build_target));
        let epochs = vec![AcceptedSchemaEpoch {
            digest: project.logical_hash,
            forward_parent: None,
        }];
        let bundle = Bundle {
            format_version: 1,
            uuid: BUNDLE,
            primary: Some("entry".to_owned()),
            schemas: BTreeMap::from([(project.logical_hash, project.logical_schema.clone())]),
            assets: BTreeMap::from([(
                "entry".to_owned(),
                AssetEntry {
                    uuid: ASSET,
                    type_uuid: TYPE,
                    schema_hash: project.logical_hash,
                    lineage: EntryLineageV1::Manifest(LineageStamp {
                        chain: lineage_chain_digest(TYPE, &epochs, 0),
                        epochs: epochs.clone(),
                        cursor: 0,
                    }),
                    authoring_only: false,
                    data: AuthoredValue::Object(BTreeMap::from([(
                        "value".to_owned(),
                        AuthoredValue::UInt(7),
                    )])),
                },
            )]),
        };
        let bundle_bytes = distill_bundle::write_bundle(&bundle).unwrap();
        std::fs::write(assets.join("byte.bundle"), &bundle_bytes).unwrap();
        let dependency_bundle = Bundle {
            format_version: 1,
            uuid: DEPENDENCY_BUNDLE,
            primary: Some("dependency".to_owned()),
            schemas: BTreeMap::from([(project.logical_hash, project.logical_schema.clone())]),
            assets: BTreeMap::from([(
                "dependency".to_owned(),
                AssetEntry {
                    uuid: DEPENDENCY_ASSET,
                    type_uuid: TYPE,
                    schema_hash: project.logical_hash,
                    lineage: EntryLineageV1::Manifest(LineageStamp {
                        chain: lineage_chain_digest(TYPE, &epochs, 0),
                        epochs: epochs.clone(),
                        cursor: 0,
                    }),
                    authoring_only: false,
                    data: AuthoredValue::Object(BTreeMap::from([(
                        "value".to_owned(),
                        AuthoredValue::UInt(8),
                    )])),
                },
            )]),
        };
        std::fs::write(
            assets.join("dependency.bundle"),
            distill_bundle::write_bundle(&dependency_bundle).unwrap(),
        )
        .unwrap();
        let coordinator = Arc::new(
            DaemonCoordinator::open(
                StoreConfig::new(temp.path().join("state")),
                vec![AssetRoot::new(
                    "main",
                    &assets,
                    assets.join(".distill-displaced"),
                )],
                LineageDestination {
                    root: "main".to_owned(),
                    path: "schema/lineage.bundle".to_owned(),
                },
                vec![rpc_target(target_hash)],
                64,
            )
            .unwrap(),
        );
        coordinator.reconcile_full_scan().unwrap();
        coordinator.install_schema_authority_for_test(authority.clone());
        coordinator.install_build_target_for_test("dev", build_target);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_unlocked = Arc::new(AtomicBool::new(false));
        let validator_calls = Arc::new(AtomicUsize::new(0));
        let registered_validator_calls = Arc::clone(&validator_calls);
        coordinator.install_pipeline_epoch_for_test(crate::epoch::processor_test_epoch_with(
            "dev",
            target_hash.0,
            crate::callbacks::ProcessorDescriptor {
                id: "cook".to_owned(),
                version: 4,
                input: TYPE,
                selector: TargetSelector::new(None, None).unwrap(),
                outputs: OutputDecls::new(TERMINAL, vec![("metadata".to_owned(), EXTRA)]).unwrap(),
            },
            StoreLockProbeProcessor {
                calls: Arc::clone(&calls),
                store: coordinator.store(),
                observed_unlocked: Arc::clone(&observed_unlocked),
            },
            move |arena| {
                arena
                    .register_validator(
                        ValidatorDescriptor {
                            id: "validate-import".to_owned(),
                            asset_type: TYPE,
                        },
                        CountingValidator(registered_validator_calls),
                    )
                    .into_result()
                    .unwrap();
            },
        ));
        let mut request = BuildRequest {
            work_class: BuildWorkClass::Interactive,
            basis: coordinator.server().current_stamp(),
            target: "dev".to_owned(),
            target_definition: target_hash,
            requested_asset: ASSET,
            output_key: String::new(),
            requested_terminal_type: TERMINAL,
            entry: AuthoringEntry {
                uuid: ASSET,
                bundle: BUNDLE,
                local_id: "entry".to_owned(),
                normalized_path: "byte.bundle".to_owned(),
                type_uuid: TYPE,
                terminal_type: TERMINAL,
                schema_hash: project.logical_hash,
                logical_schema: Arc::from(
                    snapshot_to_json(&project.logical_schema)
                        .unwrap()
                        .into_bytes(),
                ),
                role: AuthoringEntryRole::Runtime,
                tags: BTreeMap::new(),
                value: AuthoringValue {
                    canonical_value: Arc::from(&b"{\"value\":7}"[..]),
                    blobs: Vec::new(),
                },
            },
            drifted_input: DriftedInput::Asset(ASSET),
        };

        let first = build(&coordinator, &request).unwrap();
        let payload_backend = CoordinatorBuildBackend::new(&coordinator);
        assert_eq!(
            payload_backend
                .runtime_type_policy(&RuntimeTypePolicyRequest {
                    basis: request.basis,
                    target: request.target.clone(),
                    target_definition: request.target_definition,
                    type_uuid: TYPE,
                })
                .unwrap(),
            RuntimeTypePolicy { build_only: false }
        );
        for artifact in &first.artifacts {
            payload_backend
                .store_artifact(artifact.content_hash, &artifact.payload)
                .unwrap();
            let loaded = payload_backend
                .load_artifact(artifact.content_hash)
                .unwrap()
                .expect("committed artifact remains CAS-readable");
            assert_eq!(loaded.structural, artifact.payload.structural);
            assert_eq!(loaded.blobs, artifact.payload.blobs);
            assert!(loaded.load_edges.is_empty());
        }
        for wire_tree in &first.wire_trees {
            payload_backend
                .store_wire_tree(wire_tree.layout_hash, &wire_tree.bytes)
                .unwrap();
            assert_eq!(
                payload_backend
                    .load_wire_tree(wire_tree.layout_hash)
                    .unwrap()
                    .as_deref(),
                Some(wire_tree.bytes.as_ref()),
            );
        }
        let first_memo = coordinator.store().lock().unwrap().memo_seq();
        let import_key = build_import_digest(&BuildImportInputs {
            asset: ASSET,
            bundle: BUNDLE,
            local_id: "entry".to_owned(),
            authored_type: TYPE,
            terminal_type: TERMINAL,
            canonical_bundle_bytes: bundle_bytes.clone(),
            logical: project.logical_hash,
            layout: project.layout_hash,
            migrations: Vec::new(),
            automatic_migration: None,
            validator_dylib_hash: Some([9; 32]),
            artifact_format_version: ARTIFACT_FORMAT_VERSION,
        });
        let import_candidates = coordinator
            .store()
            .lock()
            .unwrap()
            .lookup_candidates(KeyKind::BuildImport, &import_key)
            .unwrap();
        let distill_store::cas::record::ResultOutcome::Success { outputs, .. } =
            &import_candidates[0].payload.outcome
        else {
            panic!("build import succeeded");
        };
        assert_eq!(outputs[0].type_uuids, vec![TYPE, TERMINAL]);
        assert_eq!(first.artifacts.len(), 2);
        assert_eq!(first.wire_trees.len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(validator_calls.load(Ordering::SeqCst), 2);
        assert!(observed_unlocked.load(Ordering::SeqCst));
        let root = first
            .artifacts
            .iter()
            .find(|artifact| artifact.content_hash == first.root_content_hash)
            .unwrap();
        let blobs = root
            .payload
            .blobs
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>();
        let parsed =
            distill_wire::artifact::parse_artifact_parts(&root.payload.structural, &blobs).unwrap();
        assert_eq!(parsed.asset_uuid, ASSET);
        assert_eq!(parsed.authored_type, TYPE);
        assert_eq!(parsed.encoded_type, TERMINAL);
        assert_eq!(parsed.terminal_type, TERMINAL);
        assert_eq!(parsed.fixed, [7]);
        let child = AssetUuid::v5(ASSET, "metadata");
        let extra = first
            .artifacts
            .iter()
            .find(|artifact| {
                let blobs = artifact
                    .payload
                    .blobs
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<_>>();
                distill_wire::artifact::parse_artifact_parts(&artifact.payload.structural, &blobs)
                    .unwrap()
                    .asset_uuid
                    == child
            })
            .expect("declared extra artifact is published");
        let blobs = extra
            .payload
            .blobs
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>();
        let parsed_extra =
            distill_wire::artifact::parse_artifact_parts(&extra.payload.structural, &blobs)
                .unwrap();
        assert_eq!(parsed_extra.encoded_type, EXTRA);
        assert_eq!(parsed_extra.terminal_type, EXTRA);

        let hydrated = build(&coordinator, &request).unwrap();
        assert_eq!(hydrated, first);
        assert_eq!(coordinator.store().lock().unwrap().memo_seq(), first_memo);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(validator_calls.load(Ordering::SeqCst), 2);

        let noncanonical = [b"\n  ".as_slice(), bundle_bytes.as_slice()].concat();
        std::fs::write(assets.join("byte.bundle"), noncanonical).unwrap();
        coordinator.reconcile_full_scan().unwrap();
        request.basis = coordinator.server().current_stamp();
        assert_eq!(build(&coordinator, &request).unwrap(), first);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(validator_calls.load(Ordering::SeqCst), 2);

        assert_eq!(
            build_with_runtime_mode(&coordinator, &request, true).unwrap(),
            first
        );
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(validator_calls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn reference_trace_invalidates_on_resolution_role_or_terminal_type_drift() {
        let source = StoreTraceSource {
            authoring_hashes: BTreeMap::new(),
            entries: BTreeMap::from([(
                ASSET,
                TraceEntry {
                    asset: ASSET,
                    bundle: BUNDLE,
                    bundle_path: "target.bundle".to_owned(),
                    local_id: "entry".to_owned(),
                    authored_type: TYPE,
                    terminal_type: TYPE,
                    role: EntryRole::Runtime,
                    tags: BTreeMap::new(),
                },
            )]),
            terminal_types: BTreeMap::from([(ASSET, TYPE)]),
            roles: BTreeMap::from([(ASSET, EntryRole::Runtime)]),
            paths: BTreeMap::from([("target.bundle".to_owned(), vec![ASSET])]),
            tools: BTreeMap::new(),
            current_load: CurrentLoadSource {
                capabilities: Vec::new(),
                migration_controls: BTreeMap::new(),
            },
            content_hashes: BTreeMap::new(),
            tag_poisons: BTreeMap::new(),
        };
        let mut trace = Vec::new();
        assert_eq!(
            resolve_reference(
                &source,
                BUNDLE,
                &AuthoredValue::Str("target.bundle".to_owned()),
                TYPE,
                true,
                &mut trace,
            )
            .unwrap(),
            ASSET
        );
        assert_eq!(trace.len(), 3);
        assert!(distill_build::trace::revalidate(&trace, &source));

        let mut moved = source.clone();
        moved
            .paths
            .insert("target.bundle".to_owned(), vec![AssetUuid([99; 16])]);
        assert!(!distill_build::trace::revalidate(&trace, &moved));

        let mut role_changed = source.clone();
        role_changed.roles.insert(ASSET, EntryRole::AuthoringOnly);
        assert!(!distill_build::trace::revalidate(&trace, &role_changed));

        let mut retyped = source;
        retyped.terminal_types.insert(ASSET, TypeUuid([88; 16]));
        assert!(!distill_build::trace::revalidate(&trace, &retyped));
    }

    #[test]
    fn absent_weak_uuid_reference_is_legal_and_trace_invalidates_when_it_appears() {
        let missing = AssetUuid([99; 16]);
        let mut source = StoreTraceSource {
            authoring_hashes: BTreeMap::new(),
            entries: BTreeMap::new(),
            terminal_types: BTreeMap::new(),
            roles: BTreeMap::new(),
            paths: BTreeMap::new(),
            tools: BTreeMap::new(),
            current_load: CurrentLoadSource {
                capabilities: Vec::new(),
                migration_controls: BTreeMap::new(),
            },
            content_hashes: BTreeMap::new(),
            tag_poisons: BTreeMap::new(),
        };
        let value = AuthoredValue::Str(missing.to_string());
        let mut trace = Vec::new();
        assert_eq!(
            resolve_reference(&source, BUNDLE, &value, TYPE, false, &mut trace).unwrap(),
            missing
        );
        assert_eq!(trace.len(), 2);
        assert!(distill_build::trace::revalidate(&trace, &source));

        let mut strong_trace = Vec::new();
        assert!(resolve_reference(&source, BUNDLE, &value, TYPE, true, &mut strong_trace).is_err());

        source.roles.insert(missing, EntryRole::Runtime);
        source.terminal_types.insert(missing, TYPE);
        assert!(!distill_build::trace::revalidate(&trace, &source));
    }
}
