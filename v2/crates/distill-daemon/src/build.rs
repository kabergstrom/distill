//! Lazy builds: node keys, the node cache, and the build cells requesters
//! share.
//!
//! A build is a pure function of its inputs. Its static inputs are digested
//! into its key (DSNK for an asset node, DSSI per processor stage, DSBI per
//! import) and never name an input version; what it observes of the
//! project (queries, reads, resolves, tools, capabilities) is its trace. A
//! cached result serves a snapshot exactly when its trace holds there, so
//! one result serves every snapshot whose answers agree.
//!
//! A requester resolves at its own snapshot: it keys the node, looks the
//! cache up, and on a miss submits the key's build cell
//! ([`crate::scheduler`]). The cell's worker builds at one consistent read
//! view opened when it starts, publishes the node in one transaction, and
//! notifies every waiter; each waiter then checks the trace at its own
//! snapshot (Built, or Drifted when an observed input differs there).

use std::cell::{Ref, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::{Arc, Weak};

use distill_build::artifact_encode::{
    encode_artifact_value, ArtifactEncodeError, ArtifactValueSpec, EncodedArtifact,
};
use distill_build::dslf::{
    ArtifactEncodingFailureV1, DslfV1, LocalFailureClass, MigrationPlanFailureV1,
    OutputBindingFailureV1, OutputBindingSlotV1,
};
use distill_build::keys::{
    build_import_digest, node_canonical_bytes, node_digest, static_inputs_canonical_bytes,
    static_inputs_digest, AppliedMigration, AutomaticMigration, BuildImportInputs, NodeInputs,
    NodeStage, NodeType, OutputHash, StaticInputs,
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
    decode_trace_payload_bytes, revalidate, trace_digest, trace_payload_bytes, CapabilityKey,
    EntryRole, Observed,
    StableFailureFingerprint, TraceOp, TraceSource,
};
use distill_bundle::{AssetEntry, Bundle};
use distill_core::id::{
    AssetUuid, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid,
};
use distill_json::AuthoredValue;
use distill_migrate::{
    conforms, execute_ops, plan_automatic_renamed, validate_plan, DefaultProvider, EdgeKind,
    FieldPath, MigrationError, MigrationOp,
};
use distill_pipeline_api::callbacks::MigrationKey;
use distill_rpc::{
    decode_asset_reference_query, AssetReferenceQuery, AuthoringMutation, BuildAnswer,
    BuildBackend, BuildCompletion, BuildRequest, BuildStart, BuildTicket, BuildView,
    BuildWorkClass, Commit, DriftedInput, PipelineUnavailableDiagnostic, RpcFailure,
    RuntimeTypePolicy, RuntimeTypePolicyRequest, ServedLoadEdge, TagPoisonMutation,
    TagProjectionMutation,
};
use distill_schema::{ProjectSchemaAuthority, ProjectTypeAuthority};
use distill_store::bundles::{EntryMeta, TagIndexUpdate};
use distill_store::cas::record::{
    FailureCause as StoreFailureCause, FailureFingerprint as StoreFailureFingerprint, KeyKind,
    LocalFailureClass as StoreLocalFailureClass, ResultOutcome,
};
use distill_store::cas::{AuxSpec, BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
use distill_store::pipeline::RegisteredTool;
use distill_store::state::SnapshotStamp;
use distill_store::served::StoreSnapshot;
use distill_store::{Store, StoreError, StoreReader};
use distill_wire::artifact::{parse_artifact, ArtifactError, ARTIFACT_FORMAT_VERSION};
use distill_wire::encode::EncodeError;

use crate::callbacks::{
    CallbackInvokeError, DiagnosticSeverity, PipelineProcessContext, ProcessArtifact,
    ProcessContextError, ProcessOutputs,
};
use crate::coordinator::DaemonCoordinator;
use crate::epoch::{PipelineEpoch, PipelineSnapshot};
use crate::scanner::RootedScanner;
use crate::scheduler::{CellOutcome, CellRun, CellWorker, Claim, WorkClass};

mod trace_source;

use trace_source::{BuiltNodes, StoreTraceSource, TraceAnswers, TraceBasis, TraceQueries};

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

/// The production lazy-build authority. A request resolves at the
/// requester's snapshot: its DSNK node key is a digest of the node's static
/// inputs only, the node cache answers when a cached result's trace holds
/// at that snapshot, and a miss submits the key's build cell, which every
/// requester of the key shares (`crate::scheduler`).
pub(crate) struct CoordinatorBuildBackend {
    coordinator: Weak<DaemonCoordinator>,
}

impl CoordinatorBuildBackend {
    pub(crate) fn new(coordinator: &Arc<DaemonCoordinator>) -> Self {
        Self {
            coordinator: Arc::downgrade(coordinator),
        }
    }
}

fn coordinator_stopped() -> RpcFailure {
    RpcFailure::AuthoringBackendUnavailable {
        operation: "build coordinator stopped".to_owned(),
    }
}

impl BuildBackend for CoordinatorBuildBackend {
    fn start(&self, view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        let Some(coordinator) = self.coordinator.upgrade() else {
            return BuildStart::Answered(Err(coordinator_stopped()));
        };
        let env = match NodeEnv::capture(&coordinator, &request.target)
            .and_then(|env| check_request(&env, request).map(|()| env))
        {
            Ok(env) => env,
            Err(error) => return BuildStart::Answered(error.answer()),
        };
        let asset = request.entry.uuid;
        let mut lookup = NodeLookup::new(&env, view.snapshot, view.latest, view.stamp.version);
        let key = match lookup.key(asset) {
            Ok(Some(key)) => key,
            Ok(None) => {
                return BuildStart::Answered(Ok(BuildAnswer::Drifted {
                    input: request.drifted_input.clone(),
                }))
            }
            Err(error) => return BuildStart::Answered(error.answer()),
        };
        match lookup.node(asset, 0) {
            Ok(Some(hit)) => {
                return BuildStart::Answered(Ok(select_output(
                    &hit.outputs,
                    request,
                )))
            }
            Ok(None) => {}
            Err(error) => return BuildStart::Answered(error.answer()),
        }
        drop(lookup);
        let class = match request.work_class {
            BuildWorkClass::Interactive => WorkClass::Interactive,
            BuildWorkClass::Batch => WorkClass::Batch,
        };
        let job_coordinator = Arc::clone(&coordinator);
        let target = request.target.clone();
        let cell_key = key.digest;
        let ticket = coordinator.request_build(cell_key, class, move |store, worker| {
            run_cell(&job_coordinator, store, worker, &target, cell_key, asset)
        });
        let request = request.clone();
        BuildStart::Submitted(BuildTicket::new(async move {
            let outcome = ticket.await;
            Box::new(NodeCompletion {
                outcome,
                env,
                request,
            }) as Box<dyn BuildCompletion>
        }))
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
}

/// The requested output of a node's outputs: absent, the requester's
/// derived child no longer exists under these inputs.
fn select_output(outputs: &BTreeMap<String, ContentHash>, request: &BuildRequest) -> BuildAnswer {
    match outputs.get(&request.output_key) {
        Some(content_hash) => BuildAnswer::Built {
            content_hash: *content_hash,
        },
        None => BuildAnswer::Drifted {
            input: DriftedInput::Asset(request.requested_asset),
        },
    }
}

/// What a build cell produces, shared by every requester of its key. Each
/// requester answers it at its own snapshot ([`NodeCompletion`]).
pub(crate) enum BuildOutcome {
    /// The node built, or was found cached, at the worker's view. Its trace
    /// decides, per requester, whether the outputs serve that requester.
    Built {
        trace: Vec<TraceOp>,
        outputs: BTreeMap<String, ContentHash>,
    },
    /// The build failed at the worker's view `at`. A failure keeps no
    /// complete trace, so it answers only requesters at that same snapshot.
    Failed { error: String, at: SnapshotStamp },
    /// The worker's view, or the files on disk, disagree with the key the
    /// cell was requested under: every requester of the key is behind.
    Drifted(DriftedInput),
    Unavailable(RpcFailure),
}

impl CellOutcome for BuildOutcome {
    fn lost() -> Self {
        Self::Unavailable(RpcFailure::AuthoringBackendUnavailable {
            operation: "build worker stopped before it finished".to_owned(),
        })
    }
}

/// One requester's view of a finished cell.
struct NodeCompletion {
    outcome: Arc<BuildOutcome>,
    env: NodeEnv,
    request: BuildRequest,
}

impl BuildCompletion for NodeCompletion {
    fn answer(self: Box<Self>, view: BuildView<'_>) -> Result<BuildAnswer, RpcFailure> {
        match &*self.outcome {
            BuildOutcome::Built { trace, outputs } => {
                let mut lookup =
                    NodeLookup::new(&self.env, view.snapshot, view.latest, view.stamp.version);
                match lookup.first_drift(trace, 0) {
                    Ok(None) => Ok(select_output(outputs, &self.request)),
                    Ok(Some(op)) => Ok(BuildAnswer::Drifted {
                        input: drifted_input(op),
                    }),
                    Err(error) => error.answer(),
                }
            }
            BuildOutcome::Failed { error, at } if *at == view.stamp => Ok(BuildAnswer::Failed {
                error: error.clone(),
            }),
            // Built elsewhere, the failure may not hold here: a fresh
            // snapshot builds again.
            BuildOutcome::Failed { .. } => Ok(BuildAnswer::Drifted {
                input: self.request.drifted_input.clone(),
            }),
            BuildOutcome::Drifted(input) => Ok(BuildAnswer::Drifted {
                input: input.clone(),
            }),
            BuildOutcome::Unavailable(failure) => Err(failure.clone()),
        }
    }
}

/// The input a requester names when `op` no longer holds at its snapshot.
fn drifted_input(op: &TraceOp) -> DriftedInput {
    match op {
        TraceOp::AuthoringRead { asset, .. }
        | TraceOp::Read { asset, .. }
        | TraceOp::RefCheck { asset, .. }
        | TraceOp::RoleCheck { asset, .. } => DriftedInput::Asset(*asset),
        TraceOp::Resolve { path, .. } => DriftedInput::File(path.clone()),
        TraceOp::Query { query, .. } => DriftedInput::Query(format!("{query:?}")),
        TraceOp::Tool { id, .. } => DriftedInput::Tool(id.clone()),
        TraceOp::Capability { .. } | TraceOp::Control { .. } | TraceOp::ControlRead { .. } => {
            DriftedInput::Dylib
        }
    }
}

/// The requester's own consistency checks: the target and chain it names
/// are the ones this pipeline serves.
fn check_request(env: &NodeEnv, request: &BuildRequest) -> Result<(), BuildError> {
    if env.target_definition != request.target_definition.0 {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    let chain = env
        .registry
        .chain(request.entry.type_uuid, &env.target)
        .map_err(BuildError::failed)?;
    let expected_requested_type = if request.output_key.is_empty() {
        chain.terminal
    } else {
        chain
            .extras
            .get(&request.output_key)
            .copied()
            .ok_or(BuildError::Drifted(DriftedInput::Asset(
                request.requested_asset,
            )))?
    };
    if request.entry.terminal_type != chain.terminal
        || request.requested_terminal_type != expected_requested_type
    {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    let named = if request.output_key.is_empty() {
        request.entry.uuid
    } else {
        AssetUuid::v5(request.entry.uuid, &request.output_key)
    };
    if request.requested_asset != named {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    Ok(())
}

/// A build cell's job, on a pool worker and the writer it was lent: build
/// `asset`'s node at a read view opened now, publish it, and report the
/// outcome every waiter shares.
fn run_cell(
    coordinator: &DaemonCoordinator,
    store: &mut Store,
    worker: &CellWorker<BuildOutcome>,
    target: &str,
    key: [u8; 32],
    asset: AssetUuid,
) -> BuildOutcome {
    let opened = coordinator
        .open_reader()
        .and_then(StoreReader::begin_snapshot)
        .and_then(|view| Ok((view, coordinator.open_reader()?)));
    let (view, latest) = match opened {
        Ok(opened) => opened,
        Err(error) => {
            return BuildOutcome::Unavailable(RpcFailure::AuthoringBackendUnavailable {
                operation: format!("open a build view: {error}"),
            })
        }
    };
    let at = view.stamp();
    let outcome = match build_cell(coordinator, store, worker, target, key, asset, view, latest) {
        Ok(node) => BuildOutcome::Built {
            trace: node.trace,
            outputs: node.outputs,
        },
        Err(error) => error.outcome(at),
    };
    match coordinator.sync_runtime_pipeline_failure(store) {
        Ok(Some(failure)) => BuildOutcome::Unavailable(RpcFailure::PipelineUnavailable(Box::new(
            PipelineUnavailableDiagnostic::PipelineFailure(failure),
        ))),
        Ok(None) => outcome,
        Err(error) => BuildOutcome::Unavailable(RpcFailure::AuthoringBackendUnavailable {
            operation: format!("persist runtime pipeline failure: {error}"),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_cell(
    coordinator: &DaemonCoordinator,
    store: &mut Store,
    worker: &CellWorker<BuildOutcome>,
    target: &str,
    key: [u8; 32],
    asset: AssetUuid,
    view: StoreSnapshot,
    latest: StoreReader,
) -> Result<NodeResult, BuildError> {
    let env = NodeEnv::capture(coordinator, target)?;
    // The requester keyed the cell at its snapshot; this view may be newer.
    // Inputs that moved since make every requester of the key stale.
    match node_key(&env, &view, asset)? {
        Some(observed) if observed.digest == key => {}
        _ => return Err(BuildError::Drifted(DriftedInput::Asset(asset))),
    }
    let at = view.stamp();
    let mut context = BuildContext::new(
        env,
        BuildStores::Worker {
            view,
            latest,
            writer: RefCell::new(store),
        },
        coordinator.scanner(),
        Some(CellScope {
            worker,
            root: asset,
            at,
        }),
        false,
        "tool-runs",
    )?;
    build_asset(&mut context, asset)
}

/// The pipeline one answer or one cell runs under, captured from the
/// coordinator once and never mixed with another.
struct NodeEnv {
    authority: Arc<ProjectSchemaAuthority>,
    pipeline: PipelineSnapshot,
    registry: PipelineRegistry,
    target: Target,
    target_definition: [u8; 32],
    dylib_hash: [u8; 32],
    max_depth: usize,
}

impl NodeEnv {
    fn capture(coordinator: &DaemonCoordinator, target: &str) -> Result<Self, BuildError> {
        let authority = coordinator.schema_authority().ok_or_else(|| {
            BuildError::Failed("project schema authority is not published".to_owned())
        })?;
        let target = coordinator.build_target(target).ok_or_else(|| {
            BuildError::Failed(format!("build target {target:?} is not published"))
        })?;
        Self::new(
            authority,
            coordinator.pipeline_snapshot(),
            target,
            coordinator.operational_configuration().max_dependency_depth,
        )
    }

    fn new(
        authority: Arc<ProjectSchemaAuthority>,
        pipeline: PipelineSnapshot,
        target: Target,
        max_depth: usize,
    ) -> Result<Self, BuildError> {
        let epoch = pipeline
            .epoch()
            .map_err(|failure| BuildError::Failed(failure.to_string()))?;
        let dylib_hash = epoch.dylib_hash();
        let registry = pipeline_registry(epoch).map_err(BuildError::Failed)?;
        Ok(Self {
            target_definition: distill_build::keys::target_definition_hash(&target),
            authority,
            registry,
            target,
            dylib_hash,
            max_depth,
            pipeline,
        })
    }

    fn epoch(&self) -> Result<&PipelineEpoch, BuildError> {
        self.pipeline
            .epoch()
            .map_err(|failure| BuildError::Failed(failure.to_string()))
    }
}

fn pipeline_registry(epoch: &PipelineEpoch) -> Result<PipelineRegistry, String> {
    let dylib_hash = epoch.dylib_hash();
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
                    dylib_hash,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("{error:?}"))?,
    )
    .map_err(|error| format!("{error:?}"))
}

/// A node's DSNK key and the canonical bytes its result record keeps.
struct NodeKey {
    digest: [u8; 32],
    canonical: Vec<u8>,
}

/// The key of `asset`'s node at `reader`'s state under `env`, or `None`
/// when `reader` has no such entry. It reads rows only: the bundle is keyed
/// by its recorded hash, which the build checks the file against.
fn node_key(
    env: &NodeEnv,
    reader: &StoreReader,
    asset: AssetUuid,
) -> Result<Option<NodeKey>, BuildError> {
    let Some(meta) = reader.entry(asset).map_err(BuildError::infrastructure)? else {
        return Ok(None);
    };
    let bundle = reader
        .bundle(meta.bundle)
        .map_err(BuildError::infrastructure)?
        .ok_or_else(|| BuildError::Infrastructure("asset owner bundle is missing".to_owned()))?;
    let chain = env
        .registry
        .chain(meta.type_uuid, &env.target)
        .map_err(BuildError::failed)?;
    let validated = env
        .epoch()?
        .validator_descriptors()
        .iter()
        .any(|descriptor| descriptor.asset_type == meta.type_uuid);
    let stages = chain
        .stages
        .iter()
        .map(|stage| NodeStage {
            processor_id: stage.registration.id.clone(),
            processor_version: stage.registration.version,
            primary: stage.registration.outputs.primary,
            extras: stage
                .registration
                .outputs
                .extras
                .iter()
                .map(|(key, type_uuid)| (key.clone(), *type_uuid))
                .collect(),
        })
        .collect::<Vec<_>>();
    let named = std::iter::once(meta.type_uuid)
        .chain(stages.iter().flat_map(|stage| {
            std::iter::once(stage.primary).chain(stage.extras.iter().map(|(_, ty)| *ty))
        }))
        .chain(std::iter::once(chain.terminal))
        .chain(chain.extras.values().copied())
        .collect::<Vec<_>>();
    let mut types = Vec::with_capacity(named.len());
    for type_uuid in named {
        let project = env.authority.project_type(type_uuid).ok_or_else(|| {
            BuildError::Failed(format!("type {type_uuid} has no project schema authority"))
        })?;
        types.push(NodeType {
            type_uuid,
            logical: project.logical_hash,
            layout: project.layout_hash,
        });
    }
    let inputs = NodeInputs {
        asset,
        bundle: meta.bundle,
        local_id: meta.local_id,
        bundle_hash: bundle.content_hash,
        authored_type: meta.type_uuid,
        authored_logical: meta.logical_hash,
        target_def_hash: env.target_definition,
        dylib_hash: env.dylib_hash,
        validated,
        terminal_type: chain.terminal,
        extras: chain
            .extras
            .iter()
            .map(|(key, type_uuid)| (key.clone(), *type_uuid))
            .collect(),
        stages,
        types,
        migration_planner_version: MIGRATION_PLANNER_VERSION,
        artifact_format_version: ARTIFACT_FORMAT_VERSION,
    };
    Ok(Some(NodeKey {
        digest: node_digest(&inputs),
        canonical: node_canonical_bytes(&inputs),
    }))
}

/// A node result: its outputs by key, and the trace that decides at which
/// snapshots they serve.
#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeResult {
    trace: Vec<TraceOp>,
    outputs: BTreeMap<String, ContentHash>,
}

/// Read-only node-cache lookups at one snapshot: a requester's before it
/// submits a build, a waiter's answer to a finished one, and a worker's
/// before it builds a node. A cached trace's `Read`s are answered by
/// looking their nodes up in turn, bounded by the dependency depth and
/// cut at a cycle (a miss).
struct NodeLookup<'a> {
    env: &'a NodeEnv,
    view: &'a StoreReader,
    latest: &'a StoreReader,
    tool_version: distill_store::state::InputVersion,
    answers: Rc<TraceAnswers>,
    nodes: BTreeMap<AssetUuid, Option<NodeResult>>,
    visiting: BTreeSet<AssetUuid>,
}

impl<'a> NodeLookup<'a> {
    fn new(
        env: &'a NodeEnv,
        view: &'a StoreReader,
        latest: &'a StoreReader,
        tool_version: distill_store::state::InputVersion,
    ) -> Self {
        Self {
            env,
            view,
            latest,
            tool_version,
            answers: Rc::default(),
            nodes: BTreeMap::new(),
            visiting: BTreeSet::new(),
        }
    }

    fn key(&self, asset: AssetUuid) -> Result<Option<NodeKey>, BuildError> {
        node_key(self.env, self.view, asset)
    }

    /// The newest cached result of `asset`'s node whose trace holds here.
    fn node(&mut self, asset: AssetUuid, depth: usize) -> Result<Option<NodeResult>, BuildError> {
        if let Some(known) = self.nodes.get(&asset) {
            return Ok(known.clone());
        }
        if depth > self.env.max_depth || !self.visiting.insert(asset) {
            return Ok(None);
        }
        let found = self.lookup(asset, depth);
        self.visiting.remove(&asset);
        let found = found?;
        self.nodes.insert(asset, found.clone());
        Ok(found)
    }

    fn lookup(&mut self, asset: AssetUuid, depth: usize) -> Result<Option<NodeResult>, BuildError> {
        let Some(key) = self.key(asset)? else {
            return Ok(None);
        };
        let candidates = self
            .latest
            .lookup_candidates(KeyKind::Node, &key.digest)
            .map_err(BuildError::infrastructure)?;
        for candidate in candidates {
            if candidate.payload.key_kind != KeyKind::Node || candidate.asset_uuid != asset {
                return Err(BuildError::Infrastructure(
                    "a node candidate names another key kind or asset".to_owned(),
                ));
            }
            let ResultOutcome::Success { outputs, .. } = &candidate.payload.outcome else {
                continue;
            };
            let trace = decode_trace_payload_bytes(&candidate.payload.trace)
                .map_err(BuildError::infrastructure)?;
            if trace_digest(&trace) != candidate.trace_digest {
                return Err(BuildError::Infrastructure(
                    "a node candidate's trace does not match its digest".to_owned(),
                ));
            }
            if self.first_drift(&trace, depth)?.is_none() {
                return Ok(Some(NodeResult {
                    trace,
                    outputs: outputs
                        .iter()
                        .map(|row| (row.output_key.clone(), row.content_hash))
                        .collect(),
                }));
            }
        }
        Ok(None)
    }

    /// The first operation of `trace` that does not hold here.
    fn first_drift<'t>(
        &mut self,
        trace: &'t [TraceOp],
        depth: usize,
    ) -> Result<Option<&'t TraceOp>, BuildError> {
        for op in trace {
            if let TraceOp::Read {
                asset,
                observed: Observed::Ok(_),
            } = op
            {
                let parent = self
                    .view
                    .resolve_child(*asset)
                    .map_err(BuildError::infrastructure)?
                    .map_or(*asset, |(parent, _)| parent);
                self.node(parent, depth + 1)?;
            }
        }
        let source = self.source()?;
        let drift = trace
            .iter()
            .find(|op| !revalidate(std::slice::from_ref(*op), &source));
        source.check()?;
        Ok(drift)
    }

    /// The trace answers at this lookup's snapshot, read as they are asked;
    /// a `read` sees the nodes looked up so far.
    fn source(&self) -> Result<StoreTraceSource<'_>, BuildError> {
        let env = self.env;
        let current_load = self.answers.current_load(|| {
            Ok(CurrentLoadSource::capture(env.epoch()?, env.dylib_hash))
        })?;
        Ok(StoreTraceSource::new(
            self.view,
            TraceBasis {
                registry: &env.registry,
                target: &env.target,
                tool_version: self.tool_version,
            },
            &self.answers,
            current_load,
            BuiltNodes::Lookup(&self.nodes),
        ))
    }
}

/// The content each asset (a node's primary or derived child) has among
/// `nodes`.
fn node_content_hashes<'n>(
    nodes: impl Iterator<Item = (AssetUuid, &'n NodeResult)>,
) -> Result<BTreeMap<AssetUuid, ContentHash>, BuildError> {
    let mut content_hashes = BTreeMap::new();
    for (parent, node) in nodes {
        for (output_key, hash) in &node.outputs {
            let asset = if output_key.is_empty() {
                parent
            } else {
                AssetUuid::v5(parent, output_key)
            };
            if content_hashes
                .insert(asset, *hash)
                .is_some_and(|existing| existing != *hash)
            {
                return Err(BuildError::Infrastructure(
                    "one build context observed two contents for an asset".to_owned(),
                ));
            }
        }
    }
    Ok(content_hashes)
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

    /// The outcome a cell reports for this error, built at `at`.
    fn outcome(self, at: SnapshotStamp) -> BuildOutcome {
        match self {
            Self::Drifted(input) => BuildOutcome::Drifted(input),
            Self::DepthExceeded { limit, chain } => {
                BuildOutcome::Unavailable(RpcFailure::BuildDepthExceeded { limit, chain })
            }
            Self::Failed(error) | Self::Deterministic { message: error, .. } => {
                BuildOutcome::Failed { error, at }
            }
            Self::Infrastructure(error) => {
                BuildOutcome::Unavailable(RpcFailure::AuthoringBackendUnavailable {
                    operation: format!("build: {error}"),
                })
            }
        }
    }

    /// The answer a requester gets for this error, found before any build
    /// at its own snapshot.
    fn answer(self) -> Result<BuildAnswer, RpcFailure> {
        match self {
            Self::Drifted(input) => Ok(BuildAnswer::Drifted { input }),
            Self::Failed(error) | Self::Deterministic { message: error, .. } => {
                Ok(BuildAnswer::Failed { error })
            }
            Self::DepthExceeded { limit, chain } => {
                Err(RpcFailure::BuildDepthExceeded { limit, chain })
            }
            Self::Infrastructure(error) => Err(RpcFailure::AuthoringBackendUnavailable {
                operation: format!("build: {error}"),
            }),
        }
    }
}

#[derive(Clone)]
struct LoadedAsset {
    meta: EntryMeta,
    bundle_bytes: Vec<u8>,
    entry: AssetEntry,
}

/// The ToolEpoch visible at a build's view, read one key at a time as
/// tools are run (one primary-key read each) and kept for the rest of the
/// build.
impl ToolEpochSnapshot for BuildContext<'_> {
    fn tool(&self, id: &str) -> Result<Option<RegisteredTool>, StoreError> {
        if let Some(known) = self.tools.borrow().get(id) {
            return Ok(known.clone());
        }
        let tool = match &self.stores {
            BuildStores::Worker { view, .. } => view.tool_at(id, self.tool_version),
            BuildStores::Inline(store) => store.borrow().tool_at(id, self.tool_version),
        }?;
        self.tools.borrow_mut().insert(id.to_owned(), tool.clone());
        Ok(tool)
    }
}

/// Proof that a writer holds an open input. A build that must see the
/// input's uncommitted rows (tag-index refinement, doctor verification)
/// runs inline on it, and only with this proof; nothing holding one submits
/// or waits on a build cell, whose worker would wait on the input's write
/// lock.
pub(crate) struct OpenInput<'s>(&'s mut Store);

impl<'s> OpenInput<'s> {
    pub(crate) fn new(store: &'s mut Store) -> Option<Self> {
        store.input_open().then_some(Self(store))
    }
}

/// Where a build reads and writes.
enum BuildStores<'s> {
    /// A build cell's worker. It reads one consistent view opened when the
    /// cell started, looks cached results and CAS bytes up at the latest
    /// committed state, and publishes on the writer it was lent.
    Worker {
        view: StoreSnapshot,
        latest: StoreReader,
        writer: RefCell<&'s mut Store>,
    },
    /// Inline in an open input: reads and writes see its uncommitted rows.
    Inline(RefCell<&'s mut Store>),
}

/// A read of a build's store; no write is made while one is held.
enum StoreRead<'c> {
    Borrowed(Ref<'c, &'c mut Store>),
    Reader(&'c StoreReader),
}

impl std::ops::Deref for StoreRead<'_> {
    type Target = StoreReader;

    fn deref(&self) -> &StoreReader {
        match self {
            Self::Borrowed(store) => store,
            Self::Reader(reader) => reader,
        }
    }
}

/// The cell a worker's build runs for, and through which its dependencies'
/// cells are claimed.
struct CellScope<'w> {
    worker: &'w CellWorker<BuildOutcome>,
    /// The node of the job's own cell, which the job finishes.
    root: AssetUuid,
    at: SnapshotStamp,
}

/// A write a node's build makes. They are buffered and published in one
/// transaction once the node is assembled.
enum BuildWrite {
    WireTree(Vec<u8>),
    Commit(BuildCommit),
    Artifact {
        asset: AssetUuid,
        bytes: Arc<[u8]>,
        load_edges: Vec<(AssetUuid, TypeUuid)>,
    },
}

/// One build: a cell's on its worker, or an inline one in an open input.
struct BuildContext<'s> {
    stores: BuildStores<'s>,
    env: NodeEnv,
    scanner: RootedScanner,
    tool_version: distill_store::state::InputVersion,
    /// The ToolEpoch rows read at the view so far (see its `ToolEpochSnapshot`).
    tools: RefCell<BTreeMap<String, Option<RegisteredTool>>>,
    execution_root: std::path::PathBuf,
    visiting: BTreeSet<AssetUuid>,
    callback_chain: Vec<AssetUuid>,
    memo: BTreeMap<AssetUuid, NodeResult>,
    verify_fresh: bool,
    cells: Option<CellScope<'s>>,
    /// The trace answers read at the view so far; content hashes come from
    /// the memo at each use.
    trace_answers: Rc<TraceAnswers>,
    writes: RefCell<Vec<BuildWrite>>,
    /// Artifact bytes this build produced, readable before (or, verifying,
    /// without) their publication.
    artifacts: RefCell<BTreeMap<ContentHash, Arc<[u8]>>>,
}

impl<'s> BuildContext<'s> {
    fn new(
        env: NodeEnv,
        stores: BuildStores<'s>,
        scanner: RootedScanner,
        cells: Option<CellScope<'s>>,
        verify_fresh: bool,
        runs: &str,
    ) -> Result<Self, BuildError> {
        let (tool_version, execution_root) = {
            let read = match &stores {
                BuildStores::Worker { view, .. } => StoreRead::Reader(view),
                BuildStores::Inline(store) => StoreRead::Borrowed(store.borrow()),
            };
            let version = read.input_version();
            (version, read.state_path().join(runs))
        };
        Ok(Self {
            stores,
            env,
            scanner,
            tool_version,
            tools: RefCell::default(),
            execution_root,
            visiting: BTreeSet::new(),
            callback_chain: Vec::new(),
            memo: BTreeMap::new(),
            verify_fresh,
            cells,
            trace_answers: Rc::default(),
            writes: RefCell::new(Vec::new()),
            artifacts: RefCell::new(BTreeMap::new()),
        })
    }
}

/// Read the build's own view.
fn lock_build_store<'c>(context: &'c BuildContext<'_>) -> Result<StoreRead<'c>, BuildError> {
    Ok(match &context.stores {
        BuildStores::Worker { view, .. } => StoreRead::Reader(view),
        BuildStores::Inline(store) => StoreRead::Borrowed(store.borrow()),
    })
}

/// Read the latest committed state: cached results and CAS bytes.
fn latest_store<'c>(context: &'c BuildContext<'_>) -> StoreRead<'c> {
    match &context.stores {
        BuildStores::Worker { latest, .. } => StoreRead::Reader(latest),
        BuildStores::Inline(store) => StoreRead::Borrowed(store.borrow()),
    }
}

fn record_write(context: &BuildContext, write: BuildWrite) {
    context.writes.borrow_mut().push(write);
}

/// Publish the buffered writes in one transaction on the build's writer.
/// A verifying build publishes nothing.
fn flush_writes(context: &BuildContext) -> Result<(), BuildError> {
    let writes = std::mem::take(&mut *context.writes.borrow_mut());
    if writes.is_empty() || context.verify_fresh {
        return Ok(());
    }
    let publish = |store: &mut Store| {
        for write in writes {
            match write {
                BuildWrite::WireTree(bytes) => {
                    store.put_wire_tree(&bytes)?;
                }
                BuildWrite::Commit(commit) => {
                    store.commit_build(commit)?;
                }
                BuildWrite::Artifact {
                    asset,
                    bytes,
                    load_edges,
                } => {
                    store.put_artifact(asset, &bytes, &load_edges)?;
                }
            }
        }
        Ok(())
    };
    match &context.stores {
        BuildStores::Worker { writer, .. } => writer.borrow_mut().write_transaction(publish),
        BuildStores::Inline(store) => store.borrow_mut().write_transaction(publish),
    }
    .map_err(BuildError::infrastructure)
}

/// The bytes of an artifact this build produced or found cached.
fn artifact_bytes(context: &BuildContext, hash: ContentHash) -> Result<Arc<[u8]>, BuildError> {
    if let Some(bytes) = context.artifacts.borrow().get(&hash) {
        return Ok(Arc::clone(bytes));
    }
    let bytes = latest_store(context)
        .cas_read(&hash.0)
        .map_err(BuildError::infrastructure)?;
    Ok(Arc::from(bytes))
}

struct BuildProcessContext<'a, 's> {
    context: &'a mut BuildContext<'s>,
    origin_bundle: BundleUuid,
    outputs: ProcessOutputs,
    trace: Vec<TraceOp>,
    stopped: bool,
    discarded: bool,
    cacheable: bool,
    fatal: Option<BuildError>,
}

impl<'a, 's> BuildProcessContext<'a, 's> {
    fn new(
        context: &'a mut BuildContext<'s>,
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

    /// Ask trace questions at the build's view; a store failure aborts.
    fn ask<T>(
        &mut self,
        ask: impl FnOnce(&StoreTraceSource<'_>) -> T,
    ) -> Result<T, ProcessContextError> {
        ask_trace(self.context, ask).map_err(|error| self.abort_build(error))
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

impl PipelineProcessContext for BuildProcessContext<'_, '_> {
    fn read(
        &mut self,
        asset: AssetUuid,
        expected_terminal: TypeUuid,
    ) -> Result<ProcessArtifact, ProcessContextError> {
        self.ensure_active()?;
        let role = self.ask(|source| source.role_check(asset))?;
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

        let terminal = self.ask(|source| source.ref_check(asset, expected_terminal))?;
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
        let observed = self.ask(|source| source.resolve(&path))?;
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
        let answer = self.ask(|source| match source.query(&query) {
            Observed::Ok(hash) => Ok((hash, source.query_results(&query))),
            Observed::Err(failure) => Err(failure),
        })?;
        self.trace.push(TraceOp::Query {
            query: Box::new(query.clone()),
            observed: match &answer {
                Ok((hash, _)) => Observed::Ok(*hash),
                Err(failure) => Observed::Err(failure.clone()),
            },
        });
        match answer {
            Ok((_, results)) => Ok(results),
            Err(failure) => Err(self.abort_observed(failure)),
        }
    }

    fn target(&self) -> Result<&Target, ProcessContextError> {
        self.ensure_active()?;
        Ok(&self.context.env.target)
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
            &*self.context,
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
    let (parent, output_key) = derived.unwrap_or((asset, String::new()));
    let node = build_asset(context, parent)?;
    let content_hash = node.outputs.get(&output_key).copied().ok_or_else(|| {
        BuildError::Failed(format!(
            "built dependency {asset} omitted output {output_key:?}"
        ))
    })?;
    let bytes = artifact_bytes(context, content_hash)?;
    let (view, structural, blobs) = split_artifact(&bytes)?;
    Ok(ProcessArtifact {
        asset,
        content_hash,
        encoded_type: view.encoded_type,
        terminal_type: view.terminal_type,
        structural,
        blobs,
    })
}

/// An artifact's parsed header, structural section and blobs.
fn split_artifact(
    bytes: &[u8],
) -> Result<(distill_wire::artifact::ArtifactView<'_>, Arc<[u8]>, Vec<Arc<[u8]>>), BuildError> {
    let view = parse_artifact(bytes).map_err(BuildError::failed)?;
    let structural_len = bytes
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
    Ok((view, Arc::from(bytes[..structural_len].to_vec()), blobs))
}

/// Build `request`'s node inline in an open input, at the input's own
/// rows: a doctor's verification. `verify_fresh` builds without any cache
/// and publishes nothing.
fn build_inline(
    coordinator: &DaemonCoordinator,
    input: &mut OpenInput<'_>,
    request: &BuildRequest,
    verify_fresh: bool,
) -> Result<NodeResult, BuildError> {
    let env = NodeEnv::capture(coordinator, &request.target)?;
    check_request(&env, request)?;
    let mut context = BuildContext::new(
        env,
        BuildStores::Inline(RefCell::new(&mut *input.0)),
        coordinator.scanner(),
        None,
        verify_fresh,
        "tool-runs",
    )?;
    let node = build_asset(&mut context, request.entry.uuid)?;
    if !node.outputs.contains_key(&request.output_key) {
        return Err(BuildError::Drifted(DriftedInput::Asset(
            request.requested_asset,
        )));
    }
    Ok(node)
}

/// Rebuild each request three times inside `input`: doctor verification
/// runs inside the input it completes in, so its builds see that input's
/// rows, run inline on its writer, and never wait on a build cell.
pub(crate) fn doctor_verify_builds(
    coordinator: &Arc<DaemonCoordinator>,
    mut input: OpenInput<'_>,
    requests: &[BuildRequest],
) -> Result<Vec<String>, String> {
    let mut defects = Vec::new();
    for request in requests {
        let mut run =
            |verify_fresh| build_inline(coordinator, &mut input, request, verify_fresh);
        let published = run(false);
        let first = run(true);
        let second = run(true);
        let root = |node: &NodeResult| node.outputs.get(&request.output_key).copied();
        match (published, first, second) {
            (Ok(published), Ok(first), Ok(second))
                if published.outputs == first.outputs && first.outputs == second.outputs => {}
            (Ok(published), Ok(first), Ok(second)) if first.outputs == second.outputs => defects.push(format!(
                "asset {} target {:?} fresh rebuild differs from the published artifact set: published root {:?}, rebuilt root {:?}",
                request.requested_asset,
                request.target,
                root(&published),
                root(&first)
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
                "asset {} target {:?} changed published/fresh rebuild outcome: {:?}; {:?}; {:?}",
                request.requested_asset,
                request.target,
                published.map(|node| node.outputs),
                first.map(|node| node.outputs),
                second.map(|node| node.outputs),
            )),
        }
    }
    Ok(defects)
}

/// Finish §10 tag indexing against a namespace that has advanced durably but
/// is not yet served: the open input applies its RPC delta afterwards.
pub(crate) fn refine_published_tag_index(
    input: OpenInput<'_>,
    scanner: RootedScanner,
    authority: Arc<ProjectSchemaAuthority>,
    pipeline: PipelineSnapshot,
    targets: &BTreeMap<String, Target>,
    max_depth: usize,
    fallback_assets: &BTreeMap<AssetUuid, BundleUuid>,
) -> PublishedTagIndex {
    try_refine_published_tag_index(
        input.0,
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
    input: OpenInput<'_>,
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
        input.0,
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
    store: &mut Store,
    scanner: RootedScanner,
    authority: Arc<ProjectSchemaAuthority>,
    pipeline: PipelineSnapshot,
    targets: &BTreeMap<String, Target>,
    max_depth: usize,
    requested_assets: Option<Vec<AssetUuid>>,
) -> Result<PublishedTagIndex, String> {
    let tag_epoch = authority.source_hash();
    let basis = store.input_version();
    let assets = match requested_assets {
        Some(assets) => assets,
        None => store
            .all_asset_ids()
            .map_err(|error| format!("enumerate tag-index assets: {error}"))?,
    };
    let env = match (pipeline.epoch(), targets.values().next()) {
        (Ok(_), Some(target)) => Some(
            NodeEnv::new(
                Arc::clone(&authority),
                pipeline.clone(),
                target.clone(),
                max_depth,
            )
            .map_err(|error| format!("tag-index pipeline map: {error:?}"))?,
        ),
        _ => None,
    };

    let mut tags = BTreeMap::new();
    let mut poisons = BTreeMap::new();
    let mut updates = Vec::with_capacity(assets.len());
    if let Some(env) = env {
        let dylib_hash = env.dylib_hash;
        let mut context = BuildContext::new(
            env,
            BuildStores::Inline(RefCell::new(&mut *store)),
            scanner.clone(),
            None,
            false,
            "tag-index-runs",
        )
        .map_err(|error| format!("pin tag-index tools: {error:?}"))?;
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
        for asset in assets {
            let direct = (|| {
                let loaded = load_asset(store, &scanner, asset)
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
    store
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
        .env.authority
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
    let current_load = current_load(context)
        .map_err(|error| (bundle, format!("{error:?}"), Vec::new(), migrated))?;
    let mut trace = Vec::new();
    let current =
        load_current_value(context, &loaded, &project, current_load, &mut trace).map_err(|error| {
            (
                bundle,
                format!("{error:?}"),
                trace_payload_bytes(&trace),
                migrated,
            )
        })?;
    let extracted = distill_schema::extract_search_tags(
        context.env.authority.schema(),
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
        dylib_hash: migrated.then_some(context.env.dylib_hash),
        trace: trace_payload_bytes(&trace),
        poison: None,
    })
}

/// One step of a build's walk over its strong-reference closure.
enum BuildFrame {
    Enter {
        asset: AssetUuid,
        chain: Vec<AssetUuid>,
    },
    /// A node whose own outputs are built, assembled once its dependencies
    /// are; with the cell this worker claimed for it, if any.
    Assemble(Box<PendingNodePublication>, Option<CellRun<BuildOutcome>>),
}

/// How a node entered the walk.
enum Entered {
    Done(NodeResult),
    Pending(PendingNodePublication, Option<CellRun<BuildOutcome>>),
}

/// What a worker does about a node it needs and the cache does not hold.
enum NodeClaim {
    /// Build it: for the cell this worker now runs (`Some`), or privately.
    Build(Option<CellRun<BuildOutcome>>),
    /// Another worker's finished cell serves it at this build's view.
    Done(NodeResult),
}

/// Build `asset`'s node and the nodes its outputs strongly reference, or
/// find them cached. The walk is iterative; its depth is bounded by the
/// dependency depth and a strong-reference cycle fails it.
fn build_asset(context: &mut BuildContext, asset: AssetUuid) -> Result<NodeResult, BuildError> {
    let mut chain = context.callback_chain.clone();
    if chain.last().copied() != Some(asset) {
        chain.push(asset);
    }
    let mut stack = vec![BuildFrame::Enter { asset, chain }];
    if let Err(error) = drive_build(context, &mut stack) {
        // Stage results the failed walk cached stay valid; the cells it
        // claimed report its failure to their waiters.
        if let Err(flush) = flush_writes(context) {
            tracing::warn!(?flush, "a failed build could not publish its cached stages");
        }
        for frame in stack.drain(..) {
            if let BuildFrame::Assemble(pending, run) = frame {
                context.visiting.remove(&pending.asset);
                if let Some(run) = run {
                    run.finish(Arc::new(error.clone().outcome(cell_at(context))));
                }
            }
        }
        return Err(error);
    }
    context.memo.get(&asset).cloned().ok_or_else(|| {
        BuildError::Infrastructure("iterative build lost its root result".to_owned())
    })
}

fn drive_build(context: &mut BuildContext, stack: &mut Vec<BuildFrame>) -> Result<(), BuildError> {
    while let Some(frame) = stack.pop() {
        match frame {
            BuildFrame::Enter { asset, chain } => {
                if chain.len() > context.env.max_depth {
                    return Err(BuildError::DepthExceeded {
                        limit: context.env.max_depth,
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
                let entered = enter_node(context, asset);
                context.callback_chain = previous_chain;
                let (pending, run) = match entered {
                    Ok(Entered::Done(result)) => {
                        context.visiting.remove(&asset);
                        context.memo.insert(asset, result);
                        continue;
                    }
                    Ok(Entered::Pending(pending, run)) => (pending, run),
                    Err(error) => {
                        context.visiting.remove(&asset);
                        return Err(error);
                    }
                };
                let dependencies = pending_dependency_parents(context, &pending);
                stack.push(BuildFrame::Assemble(Box::new(pending), run));
                for dependency in dependencies?.into_iter().rev() {
                    let mut dependency_chain = chain.clone();
                    dependency_chain.push(dependency);
                    stack.push(BuildFrame::Enter {
                        asset: dependency,
                        chain: dependency_chain,
                    });
                }
            }
            BuildFrame::Assemble(pending, run) => {
                let asset = pending.asset;
                let assembled = assemble_pending(context, *pending);
                context.visiting.remove(&asset);
                let result = match assembled {
                    Ok(result) => result,
                    Err(error) => {
                        if let Some(run) = run {
                            run.finish(Arc::new(error.clone().outcome(cell_at(context))));
                        }
                        return Err(error);
                    }
                };
                if let Some(run) = run {
                    run.finish(Arc::new(BuildOutcome::Built {
                        trace: result.trace.clone(),
                        outputs: result.outputs.clone(),
                    }));
                }
                context.memo.insert(asset, result);
            }
        }
    }
    Ok(())
}

/// The snapshot a cell's worker builds at. Only a cell's worker claims
/// cells, so only it finishes one.
fn cell_at(context: &BuildContext) -> SnapshotStamp {
    context
        .cells
        .as_ref()
        .map(|scope| scope.at)
        .expect("only a cell's worker holds a claimed cell")
}

/// Find `asset`'s node cached, take it from a finished cell, or build its
/// own outputs.
fn enter_node(context: &mut BuildContext, asset: AssetUuid) -> Result<Entered, BuildError> {
    if let Some(found) = cached_node(context, asset)? {
        return Ok(Entered::Done(found));
    }
    let run = match claim_node(context, asset)? {
        NodeClaim::Done(result) => return Ok(Entered::Done(result)),
        NodeClaim::Build(run) => run,
    };
    match build_asset_inner(context, asset) {
        Ok(pending) => Ok(Entered::Pending(pending, run)),
        Err(error) => {
            if let Some(run) = run {
                run.finish(Arc::new(error.clone().outcome(cell_at(context))));
            }
            Err(error)
        }
    }
}

/// A node-cache lookup at this build's view, answering `Read`s from the
/// nodes this build already has.
fn node_lookup<'a>(
    context: &'a BuildContext,
    view: &'a StoreReader,
    latest: &'a StoreReader,
) -> NodeLookup<'a> {
    let mut lookup = NodeLookup::new(&context.env, view, latest, context.tool_version);
    lookup.answers = Rc::clone(&context.trace_answers);
    lookup.nodes = context
        .memo
        .iter()
        .map(|(asset, node)| {
            (
                *asset,
                Some(NodeResult {
                    trace: Vec::new(),
                    outputs: node.outputs.clone(),
                }),
            )
        })
        .collect();
    lookup
}

/// Keep what a lookup found: the nodes it looked up hold at this view.
fn absorb_lookup(
    context: &mut BuildContext,
    nodes: BTreeMap<AssetUuid, Option<NodeResult>>,
) {
    for (asset, node) in nodes {
        if let Some(node) = node {
            context.memo.entry(asset).or_insert(node);
        }
    }
}

/// `asset`'s node from the cache, when a cached result's trace holds at
/// this build's view. A verifying build finds nothing cached.
fn cached_node(
    context: &mut BuildContext,
    asset: AssetUuid,
) -> Result<Option<NodeResult>, BuildError> {
    if context.verify_fresh {
        return Ok(None);
    }
    let depth = context.callback_chain.len();
    let (found, nodes) = {
        let view = lock_build_store(context)?;
        let latest = latest_store(context);
        let mut lookup = node_lookup(context, &view, &latest);
        let found = lookup.node(asset, depth)?;
        (found, lookup.nodes)
    };
    absorb_lookup(context, nodes);
    Ok(found)
}

/// Whether a finished cell's `trace` holds at this build's view.
fn holds_here(context: &mut BuildContext, trace: &[TraceOp]) -> Result<bool, BuildError> {
    let depth = context.callback_chain.len();
    let (holds, nodes) = {
        let view = lock_build_store(context)?;
        let latest = latest_store(context);
        let mut lookup = node_lookup(context, &view, &latest);
        let holds = lookup.first_drift(trace, depth)?.is_none();
        (holds, lookup.nodes)
    };
    absorb_lookup(context, nodes);
    Ok(holds)
}

/// Claim the cell of a node this worker's build needs (§ build cells): run
/// an absent or queued one inline, wait on one another worker runs unless
/// that could close a cycle of waiting workers, and otherwise build the
/// node privately. Inline builds and the job's own root claim nothing.
fn claim_node(context: &mut BuildContext, asset: AssetUuid) -> Result<NodeClaim, BuildError> {
    let worker = match &context.cells {
        Some(scope) if scope.root != asset => scope.worker,
        _ => return Ok(NodeClaim::Build(None)),
    };
    let key = {
        let view = lock_build_store(context)?;
        node_key(&context.env, &view, asset)?
    };
    let Some(key) = key else {
        return Ok(NodeClaim::Build(None));
    };
    match worker.claim(key.digest) {
        Claim::Run(run) => {
            // A worker that finished the cell since this one looked has
            // published it.
            if let Some(found) = cached_node(context, asset)? {
                run.finish(Arc::new(BuildOutcome::Built {
                    trace: found.trace.clone(),
                    outputs: found.outputs.clone(),
                }));
                return Ok(NodeClaim::Done(found));
            }
            Ok(NodeClaim::Build(Some(run)))
        }
        Claim::Private => Ok(NodeClaim::Build(None)),
        Claim::Wait(wait) => {
            let outcome = wait.wait();
            if let BuildOutcome::Built { trace, outputs } = &*outcome {
                if holds_here(context, trace)? {
                    return Ok(NodeClaim::Done(NodeResult {
                        trace: trace.clone(),
                        outputs: outputs.clone(),
                    }));
                }
            }
            // A result that does not hold at this view, or no result: this
            // build decides the node for itself.
            Ok(NodeClaim::Build(None))
        }
    }
}

/// Build one node's own outputs: its import and processor chain, each
/// stage from its cache when a candidate holds. Its strong dependencies
/// are entered after it and assembled before it.
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
        .env
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
        .env
        .epoch()?
        .validator_descriptors()
        .iter()
        .any(|descriptor| descriptor.asset_type == loaded.entry.type_uuid);
    let chain = context
        .env
        .registry
        .chain(loaded.entry.type_uuid, &context.env.target)
        .map_err(BuildError::failed)?;
    validate_runtime_chain(&context.env.authority, &chain)?;
    let mut trace = Vec::new();
    let mut cacheable = !context.verify_fresh;
    let (bytes, references, current_value) = encode_or_hydrate(
        context,
        &loaded,
        &project,
        chain.terminal,
        validators_registered.then_some(context.env.dylib_hash),
        &mut trace,
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
        process_chain(
            context,
            &loaded,
            &project,
            &chain,
            imported,
            current_value,
            &mut trace,
            &mut cacheable,
        )?
    };
    prepare_outputs(context, asset, outputs, trace, cacheable)
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

/// One output's artifact, until its node is published.
struct PendingArtifact {
    asset: AssetUuid,
    output_key: String,
    type_uuids: Vec<TypeUuid>,
    bytes: Arc<[u8]>,
    load_edges: Vec<ServedLoadEdge>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BuildOutputRow {
    asset: AssetUuid,
    content_hash: ContentHash,
    load_edges: Vec<ServedLoadEdge>,
}

/// A node whose own outputs are built: published once its dependencies
/// are assembled.
struct PendingNodePublication {
    asset: AssetUuid,
    local_assets: BTreeSet<AssetUuid>,
    rows: BTreeMap<String, BuildOutputRow>,
    pending: Vec<PendingArtifact>,
    wire_trees: BTreeMap<LayoutHash, Vec<u8>>,
    /// The import's and every stage's trace, cached or fresh.
    trace: Vec<TraceOp>,
    /// False when a stage ran an uncacheable tool, or the build verifies:
    /// the node then gets no cache entry.
    cacheable: bool,
}

type HydratedProcessorStage = (
    Vec<EncodedNodeOutput>,
    BTreeMap<String, Vec<u8>>,
    Vec<TraceOp>,
);
type EncodedBuildImport = (
    Vec<u8>,
    Vec<distill_wire::encode::EncodedReference>,
    Option<AuthoredValue>,
);

#[allow(clippy::too_many_arguments)]
fn process_chain(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    chain: &PipelineChain,
    imported: EncodedNodeOutput,
    current_value: Option<AuthoredValue>,
    node_trace: &mut Vec<TraceOp>,
    cacheable: &mut bool,
) -> Result<Vec<EncodedNodeOutput>, BuildError> {
    if !context.verify_fresh {
        if let Some((outputs, traces)) = hydrate_complete_chain(context, loaded, chain, &imported)? {
            node_trace.extend(traces);
            return Ok(outputs);
        }
    }

    let mut current_value = match current_value {
        Some(value) => value,
        None => {
            let current_load = current_load(context)?;
            let mut trace = Vec::new();
            load_current_value(context, loaded, project, current_load, &mut trace)?
        }
    };
    let mut current_hash = ContentHash(*blake3::hash(&imported.bytes).as_bytes());
    let mut extras = BTreeMap::<String, EncodedNodeOutput>::new();
    let mut final_primary = None;
    for stage in &chain.stages {
        let static_inputs = processor_static_inputs(context, loaded, stage, current_hash)?;
        let cached = hydrate_processor_stage(context, loaded, chain, stage, &static_inputs)?;
        let (next_value, encoded, trace, stage_cacheable) = run_processor_stage(
            context,
            loaded,
            chain,
            stage,
            &static_inputs,
            current_value,
            cached,
        )?;
        node_trace.extend(trace);
        *cacheable &= stage_cacheable;
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
) -> Result<(AuthoredValue, Vec<EncodedNodeOutput>, Vec<TraceOp>, bool), BuildError> {
    std::fs::create_dir_all(&context.execution_root).map_err(BuildError::infrastructure)?;
    let epoch = context
        .env.pipeline
        .epoch()
        .map_err(|failure| BuildError::Failed(failure.to_string()))?
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
    if let Some((cached_outputs, cached_debug, _)) = cached.as_ref() {
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
    Ok((next, encoded, trace, cacheable))
}

fn hydrate_complete_chain(
    context: &mut BuildContext,
    loaded: &LoadedAsset,
    chain: &PipelineChain,
    imported: &EncodedNodeOutput,
) -> Result<Option<(Vec<EncodedNodeOutput>, Vec<TraceOp>)>, BuildError> {
    let mut input_hash = ContentHash(*blake3::hash(&imported.bytes).as_bytes());
    let mut extras = BTreeMap::<String, EncodedNodeOutput>::new();
    let mut final_primary = None;
    let mut traces = Vec::new();
    for stage in &chain.stages {
        let static_inputs = processor_static_inputs(context, loaded, stage, input_hash)?;
        let Some((outputs, _debug, trace)) =
            hydrate_processor_stage(context, loaded, chain, stage, &static_inputs)?
        else {
            return Ok(None);
        };
        traces.extend(trace);
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
    Ok(Some((outputs, traces)))
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
        let authority = context.env.authority.project_type(type_uuid).ok_or_else(|| {
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
        target_def_hash: context.env.target_definition,
        processor_id: stage.registration.id.clone(),
        processor_version: stage.registration.version,
        dylib_hash: context.env.dylib_hash,
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
    let hit = if context.verify_fresh {
        None
    } else {
        ask_trace(context, |source| {
            let store = latest_store(context);
            lookup_persisted_candidate(&store, KeyKind::Processor, &key, loaded.entry.uuid, source)
        })?
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
                    .env.authority
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
            Ok(Some((hydrated, debug, hit.trace)))
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
        let store = latest_store(context);
        persisted_candidate_traces(&store, key_kind, static_key, asset)
            .map_err(BuildError::infrastructure)?
    };
    // Candidates are newest-first. Materialize only until one complete trace
    // revalidates; a stale candidate's now-unavailable successful read is a
    // cache miss, never authority to fail the current build.
    for trace in &traces {
        match preload_trace_reads(context, trace) {
            Ok(()) => {
                if ask_trace(context, |source| revalidate(trace, source))? {
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
            .env.authority
            .project_type(encoded_type)
            .cloned()
            .ok_or_else(|| {
                BuildError::Failed(format!(
                    "processor output type {encoded_type} has no schema authority"
                ))
            })?;
        let source_bundle = loaded.meta.bundle;
        let encoded = ask_trace(context, |trace_source| {
            let mut resolver = |query: &distill_json::AuthoredValue,
                                expected: TypeUuid,
                                strong: bool,
                                _path: &[distill_bundle::PathComponent]| {
                resolve_reference(trace_source, source_bundle, query, expected, strong, trace)
            };
            encode_artifact_value(
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
        })?
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
    record_write(context, BuildWrite::Commit(BuildCommit {
            wire_trees: outputs
                .iter()
                .map(|output| output.project.dswl_bytes.clone())
                .collect(),
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
        }));
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
    record_write(context, BuildWrite::Commit(BuildCommit {
            key_kind: KeyKind::Processor,
            static_input_key: static_inputs_digest(static_inputs),
            asset_uuid: loaded.entry.uuid,
            static_inputs_canonical: static_inputs_canonical_bytes(static_inputs),
            trace: trace_payload_bytes(trace),
            outcome: CommitOutcome::Failure { cause },
            wire_trees: Vec::new(),
        }));
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
    trace: Vec<TraceOp>,
    cacheable: bool,
) -> Result<PendingNodePublication, BuildError> {
    let local_assets = outputs
        .iter()
        .map(|output| output.asset)
        .collect::<BTreeSet<_>>();
    let mut rows = BTreeMap::<String, BuildOutputRow>::new();
    let mut pending = Vec::with_capacity(outputs.len());
    let mut wire_trees = BTreeMap::new();
    for output in outputs {
        verify_encoded_output(&output)?;
        wire_trees.insert(
            output.project.layout_hash,
            output.project.dswl_bytes.clone(),
        );
        let view = parse_artifact(&output.bytes).map_err(BuildError::failed)?;
        let load_edges =
            load_edges_for_output(context, output.asset, &view.load_deps, &output.references)?;
        let content_hash = ContentHash(*blake3::hash(&output.bytes).as_bytes());
        rows.insert(
            output.output_key.clone(),
            BuildOutputRow {
                asset: output.asset,
                content_hash,
                load_edges: load_edges.clone(),
            },
        );
        pending.push(PendingArtifact {
            asset: output.asset,
            type_uuids: output_type_set(&output),
            output_key: output.output_key,
            bytes: Arc::from(output.bytes),
            load_edges,
        });
    }

    Ok(PendingNodePublication {
        asset,
        local_assets,
        rows,
        pending,
        wire_trees,
        trace,
        cacheable,
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

/// Publish a node whose strong dependencies are assembled, in one
/// transaction on the build's writer: its stages' cache rows, its wire
/// trees, its artifacts with their load edges, and (when every stage was
/// cacheable) its node result. A node's trace is its import's and stages'
/// traces and a `Read` of each strong dependency: a node serves only where
/// its dependencies build to the same content.
fn assemble_pending(
    context: &mut BuildContext,
    pending_node: PendingNodePublication,
) -> Result<NodeResult, BuildError> {
    let PendingNodePublication {
        asset,
        local_assets,
        rows,
        pending,
        wire_trees,
        mut trace,
        cacheable,
    } = pending_node;

    let mut reads = BTreeMap::new();
    for row in rows.values() {
        for edge in &row.load_edges {
            if local_assets.contains(&edge.asset) {
                continue;
            }
            reads.insert(edge.asset, dependency_hash(context, edge.asset)?);
        }
    }
    trace.extend(reads.into_iter().map(|(asset, hash)| TraceOp::Read {
        asset,
        observed: Observed::Ok(hash),
    }));
    let outputs = rows
        .iter()
        .map(|(output_key, row)| (output_key.clone(), row.content_hash))
        .collect::<BTreeMap<_, _>>();

    for tree in wire_trees.values() {
        record_write(context, BuildWrite::WireTree(tree.clone()));
    }
    if cacheable {
        let key = {
            let view = lock_build_store(context)?;
            node_key(&context.env, &view, asset)?
        }
        .ok_or_else(|| BuildError::Drifted(DriftedInput::Asset(asset)))?;
        record_write(
            context,
            BuildWrite::Commit(BuildCommit {
                wire_trees: wire_trees.into_values().collect(),
                key_kind: KeyKind::Node,
                static_input_key: key.digest,
                asset_uuid: asset,
                static_inputs_canonical: key.canonical,
                trace: trace_payload_bytes(&trace),
                outcome: CommitOutcome::Success {
                    payload_kind: PayloadKind::ProcessorOutput,
                    outputs: pending
                        .iter()
                        .map(|artifact| OutputSpec {
                            output_key: artifact.output_key.clone(),
                            type_uuids: artifact.type_uuids.clone(),
                            bytes: artifact.bytes.to_vec(),
                        })
                        .collect(),
                    aux: Vec::new(),
                },
            }),
        );
    }
    for artifact in pending {
        let content_hash = ContentHash(*blake3::hash(&artifact.bytes).as_bytes());
        context
            .artifacts
            .borrow_mut()
            .insert(content_hash, Arc::clone(&artifact.bytes));
        record_write(
            context,
            BuildWrite::Artifact {
                asset: artifact.asset,
                bytes: artifact.bytes,
                load_edges: artifact
                    .load_edges
                    .iter()
                    .map(|edge| (edge.asset, edge.expected_terminal))
                    .collect(),
            },
        );
    }
    flush_writes(context)?;
    Ok(NodeResult { trace, outputs })
}

/// The content a strong dependency, assembled before its dependent, has.
fn dependency_hash(context: &BuildContext, asset: AssetUuid) -> Result<ContentHash, BuildError> {
    let derived = lock_build_store(context)?
        .resolve_child(asset)
        .map_err(BuildError::failed)?;
    let (parent, output_key) = derived.unwrap_or((asset, String::new()));
    let node = context.memo.get(&parent).ok_or_else(|| {
        BuildError::Infrastructure(format!(
            "iterative build assembled a node before dependency {asset}"
        ))
    })?;
    node.outputs.get(&output_key).copied().ok_or_else(|| {
        BuildError::Failed(format!(
            "derived child {asset} is absent from parent {parent}'s chain result"
        ))
    })
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
            .env.registry
            .chain(entry.type_uuid, &context.env.target)
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
        .env.registry
        .chain(parent.type_uuid, &context.env.target)
        .map_err(BuildError::failed)?
        .extras
        .get(&output_key)
        .copied()
        .ok_or_else(|| BuildError::Failed("derived output is absent from pipeline map".to_owned()))
}

/// Ask trace questions at this build's view. Each is answered by reading
/// the view when first asked (see `trace_source`), and kept for the rest of the
/// build, whose view does not move; a `read` sees the contents of the nodes
/// the build has so far. A store failure met while answering fails the
/// whole ask.
fn ask_trace<T>(
    context: &BuildContext,
    ask: impl FnOnce(&StoreTraceSource<'_>) -> T,
) -> Result<T, BuildError> {
    let store = lock_build_store(context)?;
    let source = StoreTraceSource::new(
        &store,
        TraceBasis {
            registry: &context.env.registry,
            target: &context.env.target,
            tool_version: context.tool_version,
        },
        &context.trace_answers,
        current_load(context)?,
        BuiltNodes::Memo(&context.memo),
    );
    let answer = ask(&source);
    source.check()?;
    Ok(answer)
}

/// The loaded pipeline's capabilities, captured once per build.
fn current_load<'c>(context: &'c BuildContext) -> Result<&'c CurrentLoadSource, BuildError> {
    context.trace_answers.current_load(|| {
        Ok(CurrentLoadSource::capture(
            context.env.epoch()?,
            context.env.dylib_hash,
        ))
    })
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

/// The build-key migration input. Any schema step keys on the pipeline
/// dylib: it decides whether a migration function exists and supplies
/// defaults.
fn migration_key_inputs(
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    dylib_hash: [u8; 32],
) -> (Vec<AppliedMigration>, Option<AutomaticMigration>) {
    if loaded.entry.schema_hash == project.logical_hash {
        return (Vec::new(), None);
    }
    (
        Vec::new(),
        Some(AutomaticMigration {
            from: loaded.entry.schema_hash,
            to: project.logical_hash,
            planner_version: MIGRATION_PLANNER_VERSION,
            dylib_hash: Some(dylib_hash),
        }),
    )
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
    context: &BuildContext,
    loaded: &LoadedAsset,
    key: [u8; 32],
    trace: &[TraceOp],
    facts: Option<&DslfV1>,
) -> Result<(), BuildError> {
    let cause = build_failure_cause(trace, facts)?;
    record_write(context, BuildWrite::Commit(BuildCommit {
            key_kind: KeyKind::BuildImport,
            static_input_key: key,
            asset_uuid: loaded.entry.uuid,
            static_inputs_canonical: Vec::new(),
            trace: trace_payload_bytes(trace),
            outcome: CommitOutcome::Failure { cause },
            wire_trees: Vec::new(),
        }));
    Ok(())
}

fn load_current_value(
    context: &BuildContext,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    current_load: &CurrentLoadSource,
    trace: &mut Vec<TraceOp>,
) -> Result<AuthoredValue, BuildError> {
    let bundle = distill_bundle::parse_bundle(&loaded.bundle_bytes).map_err(BuildError::failed)?;
    load_current_entry(
        &context.env.pipeline,
        &loaded.entry,
        &bundle,
        &project.logical_schema,
        project.logical_hash,
        &project.renamed_from,
        current_load,
        trace,
    )
}

fn load_current_entry(
    pipeline: &PipelineSnapshot,
    entry: &AssetEntry,
    bundle: &Bundle,
    current_schema: &distill_schema::ngp_schema::LogicalSchema,
    current_hash: LogicalHash,
    renames: &distill_schema::ngp_schema::Renames,
    trace_source: &CurrentLoadSource,
    trace: &mut Vec<TraceOp>,
) -> Result<AuthoredValue, BuildError> {
    if entry.schema_hash == current_hash {
        return Ok(entry.data.clone());
    }
    let schema = bundle
        .schemas
        .get(&entry.schema_hash)
        .ok_or_else(|| {
            BuildError::Failed("bundle omitted the entry's old schema snapshot".to_owned())
        })?
        .clone();
    let node = entry.schema_hash;
    let value = entry.data.clone();

    // A registered function for this exact (type, from, to) wins over the
    // automatic plan.
    let function = MigrationKey {
        type_uuid: entry.type_uuid,
        from: node,
        to: current_hash,
    }
    .id();
    let capability = CapabilityKey::MigrationFn(function.clone());
    if let observed @ Observed::Ok(_) = trace_source.capability(&capability) {
        trace.push(TraceOp::Capability {
            key: capability,
            observed,
        });
        return execute_migration_function(
            pipeline,
            entry,
            current_schema,
            current_hash,
            &function,
            value,
        );
    }

    let plan = plan_automatic_renamed(&schema.root, &current_schema.root, renames).map_err(
        |error| {
            migration_plan_error(
                entry.type_uuid,
                node,
                current_hash,
                MigrationPlanFailureV1::MissingPath {
                    path: FieldPath::root(),
                },
                format!(
                    "asset {} has no registered migration function and the automatic plan was refused: {error}",
                    entry.uuid
                ),
            )
        },
    )?;
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
        let epoch = pipeline
            .epoch()
            .map_err(|failure| BuildError::Failed(failure.to_string()))?;
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
                edge: entry.uuid,
                path: FieldPath::root(),
            },
            format!("migrated value is non-conforming: {error}"),
        )
    })?;
    Ok(migrated)
}

fn execute_migration_function(
    pipeline: &PipelineSnapshot,
    entry: &AssetEntry,
    current_schema: &distill_schema::ngp_schema::LogicalSchema,
    current_hash: LogicalHash,
    key: &str,
    input: AuthoredValue,
) -> Result<AuthoredValue, BuildError> {
    let output = match pipeline
        .epoch()
        .map_err(|failure| BuildError::Failed(failure.to_string()))?
        .invoke_migration(key, input)
    {
        Ok(output) => output,
        Err(CallbackInvokeError::Rejected(error)) => {
            return Err(BuildError::migration(
                format!(
                    "Migration function {key:?} rejected asset {} with code {}: {}",
                    entry.uuid, error.code, error.message
                ),
                DslfV1::MigrationFunction {
                    asset: entry.uuid,
                    type_uuid: entry.type_uuid,
                    from: entry.schema_hash,
                    to: current_hash,
                    function_key: key.to_owned(),
                    migration_error_code: error.code,
                },
            ));
        }
        Err(error) => return Err(BuildError::failed(error)),
    };
    conforms(&output, &current_schema.root).map_err(|error| {
        migration_plan_error(
            entry.type_uuid,
            entry.schema_hash,
            current_hash,
            MigrationPlanFailureV1::NonConformingOutput {
                edge: entry.uuid,
                path: FieldPath::root(),
            },
            format!("Migration function {key:?} produced a non-conforming value: {error}"),
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
    trace_out: &mut Vec<TraceOp>,
) -> Result<EncodedBuildImport, BuildError> {
    let current_load = current_load(context)?;
    let (migrations, automatic_migration) =
        migration_key_inputs(loaded, project, context.env.dylib_hash);
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
        ask_trace(context, |source| {
            let store = latest_store(context);
            lookup_persisted_candidate(
                &store,
                KeyKind::BuildImport,
                &key,
                loaded.entry.uuid,
                source,
            )
        })?
        .map_err(BuildError::infrastructure)?
    };
    if let Some(hit) = hit {
        return match hit.outcome {
            PersistedOutcome::Success { outputs, aux }
                if outputs.len() == 1 && outputs[0].output_key.is_empty() && aux.is_empty() =>
            {
                let output = outputs.into_iter().next().unwrap();
                trace_out.extend(hit.trace);
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
        match load_current_value(context, loaded, project, current_load, &mut trace) {
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
            .env.pipeline
            .epoch()
            .map_err(|failure| BuildError::Failed(failure.to_string()))?
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
                record_write(context, BuildWrite::Commit(BuildCommit {
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
                        wire_trees: Vec::new(),
                    }));
            }
            return Err(BuildError::Failed(format!(
                "asset {} failed validation: {diagnostics:?}",
                loaded.entry.uuid
            )));
        }
    }

    let source_bundle = loaded.meta.bundle;
    let encoded = ask_trace(context, |trace_source| {
        let mut resolver = |query: &distill_json::AuthoredValue,
                            expected: TypeUuid,
                            strong: bool,
                            _path: &[distill_bundle::PathComponent]| {
            resolve_reference(
                trace_source,
                source_bundle,
                query,
                expected,
                strong,
                &mut trace,
            )
        };
        encode_artifact_value(
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
        )
    })?;
    let encoded = match encoded {
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
        record_write(context, BuildWrite::Commit(BuildCommit {
                wire_trees: vec![project.dswl_bytes.clone()],
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
            }));
    }
    trace_out.extend(trace);
    Ok((bytes, references, Some(current_value)))
}

fn resolve_reference(
    source: &impl TraceQueries,
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
    source: &impl TraceQueries,
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
    store: &StoreReader,
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

/// The capabilities the loaded pipeline provides (default tables and
/// migration functions), each answering with the pipeline's dylib hash.
#[derive(Clone)]
struct CurrentLoadSource {
    capabilities: Vec<(CapabilityKey, [u8; 32])>,
}

impl CurrentLoadSource {
    fn capture(epoch: &PipelineEpoch, dylib_hash: [u8; 32]) -> Self {
        let capabilities = epoch
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
            .collect();
        Self { capabilities }
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
}

fn no_trace<T>() -> Observed<T> {
    Observed::Err(StableFailureFingerprint::Local {
        class: LocalFailureClass::ArtifactEncoding,
        detail: [0; 32],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    mod cells;
    mod eager_trace;
    mod lazy_trace;
    use eager_trace::{EagerEntry, EagerTraceSource};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::callbacks::{
        Diagnostics, PipelineProcessContext, PipelineProcessor, PipelineValidator, ProcessorError,
        ProcessorProduct, ProcessorProducts, ValidatorDescriptor,
    };
    use distill_build::outputs::OutputDecls;
    use distill_build::pipeline::{GraphicsApi, TargetArch, TargetOs, TargetSelector};
    use distill_json::AuthoredValue;
    use distill_rpc::{
        ArtifactPayload, AuthoringEntry, AuthoringEntryRole, AuthoringValue,
        BuildArtifactPublication, BuildPublication, BuildWireTree, TargetDefinition,
        TargetDefinitionHash,
    };
    use distill_schema::ngp_schema::{
        node_hash, snapshot_to_json, Field, FieldAttrs, FieldIdentifier, FieldLayout,
        LayoutIdentity, LogicalSchema, PrimitiveKind, PrimitiveType, Schema, SchemaLayouts,
        SchemaNode, SchemaTypeId, TypeAttrs, TypeDef, TypeLayout, TypePath,
    };
    use distill_store::StoreConfig;

    use crate::scanner::AssetRoot;

    const TYPE: TypeUuid = TypeUuid([71; 16]);
    const TERMINAL: TypeUuid = TypeUuid([74; 16]);
    const EXTRA: TypeUuid = TypeUuid([75; 16]);
    const ASSET: AssetUuid = AssetUuid([72; 16]);
    const BUNDLE: BundleUuid = BundleUuid([73; 16]);
    const DEPENDENCY_ASSET: AssetUuid = AssetUuid([78; 16]);
    const DEPENDENCY_BUNDLE: BundleUuid = BundleUuid([79; 16]);

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
                type_ops_hash: String::new(),
                layout_hashes: Default::default(),
                rustc_version: String::new(),
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
                        generic_const_arguments: Vec::new(),
                        has_explicit_discriminants: false,
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
                        generic_const_arguments: Vec::new(),
                        has_explicit_discriminants: false,
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
                        generic_const_arguments: Vec::new(),
                        has_explicit_discriminants: false,
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
                        generic_const_arguments: Vec::new(),
                        has_explicit_discriminants: false,
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
                type_ops_hash: String::new(),
                layout_hashes: Default::default(),
                rustc_version: String::new(),
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
                        generic_const_arguments: Vec::new(),
                        has_explicit_discriminants: false,
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
                        generic_const_arguments: Vec::new(),
                        has_explicit_discriminants: false,
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
                        generic_const_arguments: Vec::new(),
                        has_explicit_discriminants: false,
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

    const WATCHDOG: std::time::Duration = std::time::Duration::from_secs(30);

    fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async { tokio::time::timeout(WATCHDOG, future).await })
            .expect("watchdog: the build never finished")
    }

    /// A requester's snapshot and latest reader.
    struct Requester {
        snapshot: StoreSnapshot,
        latest: StoreReader,
    }

    impl Requester {
        fn at_current(coordinator: &DaemonCoordinator) -> Self {
            Self {
                snapshot: coordinator.open_reader().unwrap().begin_snapshot().unwrap(),
                latest: coordinator.open_reader().unwrap(),
            }
        }

        fn view(&self) -> BuildView<'_> {
            BuildView {
                snapshot: &self.snapshot,
                stamp: self.snapshot.stamp(),
                latest: &self.latest,
            }
        }
    }

    /// Answer `request` at `requester`'s snapshot through the production
    /// backend, waiting for its cell when it submits one.
    fn resolve(
        coordinator: &Arc<DaemonCoordinator>,
        requester: &Requester,
        request: &BuildRequest,
    ) -> Result<BuildAnswer, RpcFailure> {
        match CoordinatorBuildBackend::new(coordinator).start(requester.view(), request) {
            BuildStart::Answered(answer) => answer,
            BuildStart::Submitted(ticket) => block_on(ticket).answer(requester.view()),
        }
    }

    /// Build `request` at the current snapshot, and read its node's
    /// published artifacts and wire trees back from the CAS.
    fn build(
        coordinator: &Arc<DaemonCoordinator>,
        request: &BuildRequest,
    ) -> Result<BuildPublication, BuildError> {
        let requester = Requester::at_current(coordinator);
        let root = match resolve(coordinator, &requester, request) {
            Ok(BuildAnswer::Built { content_hash }) => content_hash,
            Ok(BuildAnswer::Failed { error }) => return Err(BuildError::Failed(error)),
            Ok(BuildAnswer::Drifted { input }) => return Err(BuildError::Drifted(input)),
            Err(failure) => return Err(BuildError::Infrastructure(format!("{failure:?}"))),
        };
        let env = NodeEnv::capture(coordinator, &request.target)?;
        let node = NodeLookup::new(
            &env,
            &requester.snapshot,
            &requester.latest,
            requester.snapshot.stamp().version,
        )
        .node(request.entry.uuid, 0)?
        .expect("a built node is cached at its requester's snapshot");
        Ok(published(&requester.latest, root, &node))
    }

    fn published(latest: &StoreReader, root: ContentHash, node: &NodeResult) -> BuildPublication {
        let mut artifacts = BTreeMap::new();
        let mut wire_trees = BTreeMap::new();
        for hash in node.outputs.values() {
            let bytes = latest.cas_read(&hash.0).unwrap();
            let (view, structural, blobs) = split_artifact(&bytes).unwrap();
            let layout_hash = view.layout_hash;
            wire_trees.insert(
                layout_hash,
                BuildWireTree {
                    layout_hash,
                    bytes: Arc::from(latest.wire_tree_read(layout_hash).unwrap()),
                },
            );
            let load_edges = latest
                .artifact_load_edges(*hash)
                .unwrap()
                .into_iter()
                .map(|(asset, expected_terminal)| ServedLoadEdge {
                    asset,
                    expected_terminal,
                })
                .collect();
            artifacts.insert(
                *hash,
                BuildArtifactPublication {
                    content_hash: *hash,
                    payload: ArtifactPayload {
                        structural,
                        blobs,
                        load_edges,
                    },
                },
            );
        }
        BuildPublication {
            root_content_hash: root,
            artifacts: artifacts.into_values().collect(),
            wire_trees: wire_trees.into_values().collect(),
        }
    }

    /// Build `request` inline without any cache, as a doctor's verification
    /// does, and read the outputs it would publish (identical content is
    /// already in the CAS).
    fn build_fresh(
        coordinator: &Arc<DaemonCoordinator>,
        writer: &mut Store,
        request: &BuildRequest,
    ) -> BuildPublication {
        writer.open_input().unwrap();
        let node = build_inline(
            coordinator,
            &mut OpenInput::new(writer).unwrap(),
            request,
            true,
        )
        .unwrap();
        writer.finish_input(false).unwrap();
        let root = node.outputs[&request.output_key];
        published(&coordinator.open_reader().unwrap(), root, &node)
    }

    fn rpc_target(hash: TargetDefinitionHash) -> TargetDefinition {
        TargetDefinition::new("dev", hash)
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
        opener: Arc<distill_store::StoreOpener>,
        observed_unlocked: Arc<AtomicBool>,
    }

    impl PipelineProcessor for StoreLockProbeProcessor {
        fn process(
            &self,
            input: AuthoredValue,
            context: &mut dyn PipelineProcessContext,
        ) -> Result<ProcessorProducts, ProcessorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // A cell's worker holds no write transaction while a processor
            // runs: another writer takes the write lock at once. (Probed
            // until seen once; an inline verification runs inside its input.)
            if !self.observed_unlocked.load(Ordering::SeqCst) {
                let unlocked = self
                    .opener
                    .open_writer()
                    .and_then(|mut writer| writer.write_transaction(|_| Ok(())))
                    .is_ok();
                self.observed_unlocked.store(unlocked, Ordering::SeqCst);
            }
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
    fn production_backend_prefers_a_registered_migration_function_over_the_automatic_plan() {
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
                vec![AssetRoot::new("main", &assets)],
                vec![rpc_target(target_hash)],
                64,
            )
            .unwrap(),
        );
        let mut writer = coordinator.open_writer().unwrap();
        coordinator.reconcile_full_scan(&mut writer).unwrap();
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
        writer.open_input().unwrap();
        refine_published_tag_index(
            OpenInput::new(&mut writer).unwrap(),
            coordinator.scanner(),
            Arc::clone(&authority),
            coordinator.pipeline_snapshot(),
            &BTreeMap::from([("dev".to_owned(), build_target)]),
            64,
            &BTreeMap::from([(ASSET, BUNDLE)]),
        );
        writer.finish_input(true).unwrap();
        let indexed = coordinator
            .open_reader()
            .unwrap()
            .tag_index_state(ASSET)
            .unwrap()
            .unwrap();
        assert_eq!(indexed.planner_version, Some(MIGRATION_PLANNER_VERSION));
        assert!(indexed.dylib_hash.is_some());
        assert!(indexed.poison.is_none());
        let request = BuildRequest {
            work_class: BuildWorkClass::Interactive,
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

        let migration_calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::clone(&migration_calls);
        coordinator.install_pipeline_epoch_for_test(crate::epoch::processor_test_epoch_with_dylib(
            [10; 32],
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
                    .registrar()
                    .register_migration(
                        crate::callbacks::MigrationKey {
                            type_uuid: TYPE,
                            from: old_hash,
                            to: project.logical_hash,
                        },
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

        let function = build(&coordinator, &request).unwrap();
        assert_eq!(migration_calls.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
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
        assert_eq!(calls.load(Ordering::SeqCst), 2);
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
                vec![AssetRoot::new("main", &assets)],
                vec![rpc_target(target_hash)],
                64,
            )
            .unwrap(),
        );
        let mut writer = coordinator.open_writer().unwrap();
        coordinator.reconcile_full_scan(&mut writer).unwrap();
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
                opener: Arc::clone(coordinator.opener()),
                observed_unlocked: Arc::clone(&observed_unlocked),
            },
            move |arena| {
                arena
                    .registrar()
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
        let request = BuildRequest {
            work_class: BuildWorkClass::Interactive,
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
                    basis: coordinator.server().current_stamp(),
                    target: request.target.clone(),
                    target_definition: request.target_definition,
                    type_uuid: TYPE,
                })
                .unwrap(),
            RuntimeTypePolicy { build_only: false }
        );
        for artifact in &first.artifacts {
            coordinator
                .server()
                .install_artifact(artifact.content_hash, artifact.payload.clone())
                .unwrap();
            let blobs = artifact
                .payload
                .blobs
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&[u8]>>();
            assert_eq!(
                coordinator
                    .open_reader()
                    .unwrap()
                    .cas_read(&artifact.content_hash.0)
                    .unwrap(),
                distill_wire::artifact::assemble_artifact(&artifact.payload.structural, &blobs),
                "committed artifact remains CAS-readable"
            );
        }
        for wire_tree in &first.wire_trees {
            coordinator
                .server()
                .install_wire_tree(wire_tree.layout_hash, wire_tree.bytes.clone())
                .unwrap();
            assert_eq!(
                coordinator
                    .open_reader()
                    .unwrap()
                    .wire_tree_read(wire_tree.layout_hash)
                    .unwrap(),
                wire_tree.bytes.to_vec(),
            );
        }
        let first_memo = coordinator.open_reader().unwrap().memo_seq();
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
            .open_reader()
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
        assert_eq!(coordinator.open_reader().unwrap().memo_seq(), first_memo);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(validator_calls.load(Ordering::SeqCst), 2);

        let noncanonical = [b"\n  ".as_slice(), bundle_bytes.as_slice()].concat();
        std::fs::write(assets.join("byte.bundle"), noncanonical).unwrap();
        coordinator.reconcile_full_scan(&mut writer).unwrap();
        assert_eq!(build(&coordinator, &request).unwrap(), first);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(validator_calls.load(Ordering::SeqCst), 2);

        assert_eq!(build_fresh(&coordinator, &mut writer, &request), first);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(validator_calls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn reference_trace_invalidates_on_resolution_role_or_terminal_type_drift() {
        let source = EagerTraceSource {
            authoring_hashes: BTreeMap::new(),
            entries: BTreeMap::from([(
                ASSET,
                EagerEntry {
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
        let mut source = EagerTraceSource {
            authoring_hashes: BTreeMap::new(),
            entries: BTreeMap::new(),
            terminal_types: BTreeMap::new(),
            roles: BTreeMap::new(),
            paths: BTreeMap::new(),
            tools: BTreeMap::new(),
            current_load: CurrentLoadSource {
                capabilities: Vec::new(),
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
