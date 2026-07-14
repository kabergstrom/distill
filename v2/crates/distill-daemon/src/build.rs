//! Snapshot-pinned lazy build execution and durable build-import caching.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};

use distill_build::artifact_encode::{encode_artifact_value, ArtifactValueSpec, EncodedArtifact};
use distill_build::dslf::{DslfV1, LocalFailureClass, MigrationPlanFailureV1};
use distill_build::keys::{
    build_import_digest, static_inputs_canonical_bytes, static_inputs_digest, BuildImportInputs,
    OutputHash, StaticInputs,
};
use distill_build::persist::{lookup_persisted_candidate, PersistedOutcome};
use distill_build::pipeline::{
    PipelineChain, PipelineRegistry, PipelineStage, ProcessorRegistration, Target,
};
use distill_build::query::{asset_query_result_hash, AssetQuery};
use distill_build::tool::{ProcessContext, StoreToolEpochSnapshot, ToolRuntimeBinding};
use distill_build::trace::{
    trace_payload_bytes, CapabilityKey, ControlQuery, ControlSubject, ControlValueHash, EntryRole,
    Observed, StableFailureFingerprint, TraceOp, TraceSource,
};
use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, TypeUuid};
use distill_json::AuthoredValue;
use distill_migrate::{
    conforms, execute_ops, plan_automatic, validate_plan, DefaultProvider, EdgeKind, FieldPath,
    MigrationOp,
};
use distill_rpc::{
    decode_asset_reference_query, decode_authoring_payload, ArtifactPayload, AssetReferenceQuery,
    BuildArtifactPublication, BuildBackend, BuildBackendOutcome, BuildPublication, BuildRequest,
    BuildWireTree, DriftedInput, RpcFailure, ServedClosureRow, ServedLoadEdge,
};
use distill_schema::{ProjectSchemaAuthority, ProjectTypeAuthority};
use distill_store::bundles::{BundleMeta, EntryMeta};
use distill_store::cas::record::{
    FailureCause as StoreFailureCause, FailureFingerprint as StoreFailureFingerprint, KeyKind,
    LocalFailureClass as StoreLocalFailureClass,
};
use distill_store::cas::{AuxSpec, BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
use distill_store::Store;
use distill_wire::artifact::{parse_artifact, ARTIFACT_FORMAT_VERSION};

use crate::callbacks::{CallbackInvokeError, DiagnosticSeverity};
use crate::coordinator::DaemonCoordinator;
use crate::epoch::{PipelineEpoch, PipelineSnapshot};
use crate::scanner::RootedScanner;

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

impl BuildBackend for CoordinatorBuildBackend {
    fn build(&self, request: &BuildRequest) -> Result<BuildBackendOutcome, RpcFailure> {
        let coordinator =
            self.coordinator
                .upgrade()
                .ok_or_else(|| RpcFailure::AuthoringBackendUnavailable {
                    operation: "build coordinator stopped".to_owned(),
                })?;
        match build(&coordinator, request) {
            Ok(publication) => Ok(BuildBackendOutcome::Built(publication)),
            Err(BuildError::Drifted(input)) => Ok(BuildBackendOutcome::Drifted { input }),
            Err(BuildError::Failed(error)) => Ok(BuildBackendOutcome::Failed { error }),
            Err(BuildError::Infrastructure(error)) => {
                Err(RpcFailure::AuthoringBackendUnavailable {
                    operation: format!("build: {error}"),
                })
            }
        }
    }
}

#[derive(Debug)]
enum BuildError {
    Drifted(DriftedInput),
    Failed(String),
    Infrastructure(String),
}

impl BuildError {
    fn failed(error: impl std::fmt::Debug) -> Self {
        Self::Failed(format!("{error:?}"))
    }

    fn infrastructure(error: impl std::fmt::Debug) -> Self {
        Self::Infrastructure(format!("{error:?}"))
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
    primary: ServedClosureRow,
    outputs: BTreeMap<String, ServedClosureRow>,
    artifacts: BTreeMap<ContentHash, BuildArtifactPublication>,
    wire_trees: BTreeMap<LayoutHash, BuildWireTree>,
}

struct BuildContext<'a> {
    store: &'a mut Store,
    scanner: RootedScanner,
    authority: Arc<ProjectSchemaAuthority>,
    pipeline: PipelineSnapshot,
    registry: PipelineRegistry,
    target: Target,
    target_definition: [u8; 32],
    dylib_hash: [u8; 32],
    basis: distill_store::state::InputVersion,
    max_depth: usize,
    visiting: BTreeSet<AssetUuid>,
    memo: BTreeMap<AssetUuid, NodePublication>,
}

fn build(
    coordinator: &DaemonCoordinator,
    request: &BuildRequest,
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
    let observed_target = distill_rpc::TargetDefinitionHash(
        distill_build::keys::target_definition_hash(&target, &[]),
    );
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
            .ok_or_else(|| BuildError::Drifted(DriftedInput::Asset(request.requested_asset)))?
    };
    if request.entry.terminal_type != requested_chain.terminal
        || request.requested_terminal_type != expected_requested_type
    {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    let store_handle = coordinator.store();
    let mut store = store_handle
        .lock()
        .map_err(|_| BuildError::Infrastructure("durable store mutex is poisoned".to_owned()))?;
    if store.instance_id() != request.basis.instance
        || store.input_version() != request.basis.version
    {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    let root = load_asset(&store, &coordinator.scanner(), request.entry.uuid)?;
    verify_request_entry(request, &root, &authority)?;

    let mut context = BuildContext {
        store: &mut store,
        scanner: coordinator.scanner(),
        authority,
        pipeline,
        registry,
        target,
        target_definition: request.target_definition.0,
        dylib_hash,
        basis: request.basis.version,
        max_depth: coordinator.operational_configuration().max_dependency_depth,
        visiting: BTreeSet::new(),
        memo: BTreeMap::new(),
    };
    let root = build_asset(&mut context, request.entry.uuid, 0)?;
    let selected = root
        .outputs
        .get(&request.output_key)
        .ok_or_else(|| BuildError::Drifted(DriftedInput::Asset(request.requested_asset)))?;
    Ok(BuildPublication {
        root_content_hash: selected.content_hash,
        artifacts: root.artifacts.into_values().collect(),
        wire_trees: root.wire_trees.into_values().collect(),
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
    context: &mut BuildContext<'_>,
    asset: AssetUuid,
    depth: usize,
) -> Result<NodePublication, BuildError> {
    if let Some(cached) = context.memo.get(&asset) {
        return Ok(cached.clone());
    }
    if depth > context.max_depth {
        return Err(BuildError::Failed(format!(
            "dependency depth exceeds configured limit {}",
            context.max_depth
        )));
    }
    if !context.visiting.insert(asset) {
        return Err(BuildError::Failed(format!(
            "strong-reference cycle reaches asset {asset}"
        )));
    }
    let result = build_asset_inner(context, asset, depth);
    context.visiting.remove(&asset);
    let publication = result?;
    context.memo.insert(asset, publication.clone());
    Ok(publication)
}

fn build_asset_inner(
    context: &mut BuildContext<'_>,
    asset: AssetUuid,
    depth: usize,
) -> Result<NodePublication, BuildError> {
    let loaded = load_asset(context.store, &context.scanner, asset)?;
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
    assemble_outputs(context, outputs, depth)
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

fn process_chain(
    context: &mut BuildContext<'_>,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    chain: &PipelineChain,
    imported: EncodedNodeOutput,
    current_value: Option<AuthoredValue>,
) -> Result<Vec<EncodedNodeOutput>, BuildError> {
    if let Some(outputs) = hydrate_complete_chain(context, loaded, chain, &imported)? {
        return Ok(outputs);
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
        let execution_root = context.store.state_path().join("tool-runs");
        std::fs::create_dir_all(&execution_root).map_err(BuildError::infrastructure)?;
        let platform_id = context.target.compilation_identity.target_triple.clone();
        let system_runtime_id = context.target.compilation_identity.rustc.clone();
        let tool_snapshot = StoreToolEpochSnapshot::new(context.store, context.basis);
        let mut process_context = ProcessContext::new(
            &tool_snapshot,
            ToolRuntimeBinding {
                platform_id: &platform_id,
                system_runtime_id: &system_runtime_id,
                execution_root: &execution_root,
            },
        );
        let outcome = context
            .pipeline
            .epoch()
            .map_err(|poison| BuildError::Failed(poison.to_string()))?
            .invoke_processor(&stage.registration.id, current_value, &mut process_context);
        let mut trace = process_context
            .into_trace()
            .map_err(|_| BuildError::Infrastructure("processor trace was discarded".to_owned()))?;
        let products = match outcome {
            Ok(products) => products,
            Err(CallbackInvokeError::Rejected(error)) => {
                commit_processor_failure(
                    context,
                    loaded,
                    stage,
                    &static_inputs,
                    &trace,
                    error.code,
                )?;
                return Err(BuildError::Failed(format!(
                    "processor {:?} rejected asset {} with code {}: {}",
                    stage.registration.id, loaded.entry.uuid, error.code, error.message
                )));
            }
            Err(error) => return Err(BuildError::failed(error)),
        };
        let next_value = products.primary.clone().ok_or_else(|| {
            BuildError::Failed(format!(
                "processor {:?} omitted its primary output",
                stage.registration.id
            ))
        })?;
        let encoded =
            encode_processor_products(context, loaded, chain, stage, &products, &mut trace)?;
        if let Some((cached_outputs, cached_debug)) = cached {
            ensure_cached_stage_matches(&cached_outputs, &cached_debug, &encoded, &products.debug)?;
        } else {
            commit_processor_stage(
                context,
                loaded,
                &static_inputs,
                &trace,
                &encoded,
                &products.debug,
            )?;
        }
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

fn hydrate_complete_chain(
    context: &mut BuildContext<'_>,
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
    context: &BuildContext<'_>,
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
    context: &mut BuildContext<'_>,
    loaded: &LoadedAsset,
    chain: &PipelineChain,
    stage: &PipelineStage,
    static_inputs: &StaticInputs,
) -> Result<Option<(Vec<EncodedNodeOutput>, BTreeMap<String, Vec<u8>>)>, BuildError> {
    let key = static_inputs_digest(static_inputs);
    let trace_source = capture_trace_source(context)?;
    let Some(hit) = lookup_persisted_candidate(
        context.store,
        KeyKind::Processor,
        &key,
        loaded.entry.uuid,
        &trace_source,
    )
    .map_err(BuildError::infrastructure)?
    else {
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
    context: &BuildContext<'_>,
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
            .expect("epoch validates processor primary"),
    ));
    values.extend(
        products
            .extras
            .iter()
            .map(|(key, value)| (key.clone(), value)),
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
        .map_err(BuildError::failed)?;
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
    context: &mut BuildContext<'_>,
    loaded: &LoadedAsset,
    static_inputs: &StaticInputs,
    trace: &[TraceOp],
    outputs: &[EncodedNodeOutput],
    debug: &BTreeMap<String, Vec<u8>>,
) -> Result<(), BuildError> {
    context
        .store
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
    context: &mut BuildContext<'_>,
    loaded: &LoadedAsset,
    stage: &PipelineStage,
    static_inputs: &StaticInputs,
    trace: &[TraceOp],
    build_error_code: u32,
) -> Result<(), BuildError> {
    let cause = if trace.last().is_some_and(TraceOp::failed) {
        StoreFailureCause::Op
    } else {
        let detail = DslfV1::Processor {
            asset: loaded.entry.uuid,
            processor_id: stage.registration.id.clone(),
            processor_version: stage.registration.version,
            stage: stage.index,
            build_error_code,
        }
        .digest()
        .map_err(BuildError::failed)?;
        StoreFailureCause::Local(StoreFailureFingerprint::Local {
            class: StoreLocalFailureClass::Processor,
            detail,
        })
    };
    context
        .store
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

fn output_type_set(output: &EncodedNodeOutput) -> Vec<TypeUuid> {
    let mut types = vec![
        output.authored_type,
        output.encoded_type,
        output.terminal_type,
    ];
    types.sort();
    types.dedup();
    types
}

fn assemble_outputs(
    context: &mut BuildContext<'_>,
    outputs: Vec<EncodedNodeOutput>,
    depth: usize,
) -> Result<NodePublication, BuildError> {
    struct PendingArtifact {
        content_hash: ContentHash,
        structural: Arc<[u8]>,
        blobs: Vec<Arc<[u8]>>,
        encoded_type: TypeUuid,
        terminal_type: TypeUuid,
    }

    let local_assets = outputs
        .iter()
        .map(|output| output.asset)
        .collect::<BTreeSet<_>>();
    let mut rows = BTreeMap::<String, ServedClosureRow>::new();
    let mut pending = Vec::with_capacity(outputs.len());
    let mut wire_trees = BTreeMap::new();
    for output in &outputs {
        verify_encoded_output(output)?;
        context
            .store
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
            ServedClosureRow {
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
            encoded_type: output.encoded_type,
            terminal_type: output.terminal_type,
        });
    }

    let mut closure = BTreeMap::new();
    for row in rows.values() {
        merge_closure_row(&mut closure, row.clone())?;
    }
    let mut artifacts = BTreeMap::new();
    for row in rows.values() {
        for edge in &row.load_edges {
            if local_assets.contains(&edge.asset) {
                continue;
            }
            let (dependency, selected) = build_dependency(context, edge.asset, depth + 1)?;
            merge_closure_row(&mut closure, selected.clone())?;
            let selected_artifact = dependency
                .artifacts
                .get(&selected.content_hash)
                .expect("selected dependency row has an artifact");
            for nested in &selected_artifact.payload.closure_rows {
                merge_closure_row(&mut closure, nested.clone())?;
            }
            merge_artifacts(&mut artifacts, dependency.artifacts)?;
            merge_wire_trees(&mut wire_trees, dependency.wire_trees)?;
        }
    }
    let closure_rows = closure.into_values().collect::<Vec<_>>();
    for artifact in pending {
        artifacts.insert(
            artifact.content_hash,
            BuildArtifactPublication {
                content_hash: artifact.content_hash,
                payload: ArtifactPayload {
                    structural: artifact.structural,
                    blobs: artifact.blobs,
                    encoded_type: artifact.encoded_type,
                    terminal_type: artifact.terminal_type,
                    closure_rows: closure_rows.clone(),
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

fn build_dependency(
    context: &mut BuildContext<'_>,
    asset: AssetUuid,
    depth: usize,
) -> Result<(NodePublication, ServedClosureRow), BuildError> {
    let derived = context
        .store
        .resolve_child(asset)
        .map_err(BuildError::failed)?;
    if let Some((parent, output_key)) = derived {
        let publication = build_asset(context, parent, depth)?;
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
        let publication = build_asset(context, asset, depth)?;
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
    context: &BuildContext<'_>,
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
    context: &BuildContext<'_>,
    asset: AssetUuid,
) -> Result<TypeUuid, BuildError> {
    if let Some(entry) = context.store.entry(asset).map_err(BuildError::failed)? {
        return context
            .registry
            .chain(entry.type_uuid, &context.target)
            .map(|chain| chain.terminal)
            .map_err(BuildError::failed);
    }
    let (parent, output_key) = context
        .store
        .resolve_child(asset)
        .map_err(BuildError::failed)?
        .ok_or_else(|| BuildError::Failed(format!("missing strong reference {asset}")))?;
    let parent = context
        .store
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

fn capture_trace_source(context: &BuildContext<'_>) -> Result<StoreTraceSource, BuildError> {
    let epoch = context
        .pipeline
        .epoch()
        .map_err(|poison| BuildError::Failed(poison.to_string()))?;
    StoreTraceSource::capture(
        context.store,
        &context.registry,
        &context.target,
        context.basis,
        epoch,
        context.dylib_hash,
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

fn commit_migration_failure(
    context: &mut BuildContext<'_>,
    loaded: &LoadedAsset,
    key: [u8; 32],
    trace: &[TraceOp],
    from: distill_core::id::LogicalHash,
    to: distill_core::id::LogicalHash,
    failure: MigrationPlanFailureV1,
) -> Result<(), BuildError> {
    let cause = if trace.last().is_some_and(TraceOp::failed) {
        StoreFailureCause::Op
    } else {
        let detail = DslfV1::MigrationPlan {
            type_uuid: loaded.entry.type_uuid,
            from,
            to,
            failure,
        }
        .digest()
        .map_err(BuildError::failed)?;
        StoreFailureCause::Local(StoreFailureFingerprint::Local {
            class: StoreLocalFailureClass::MigrationPlan,
            detail,
        })
    };
    context
        .store
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
    context: &mut BuildContext<'_>,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    trace_source: &StoreTraceSource,
    trace: &mut Vec<TraceOp>,
) -> Result<AuthoredValue, BuildError> {
    if loaded.entry.schema_hash == project.logical_hash {
        return Ok(loaded.entry.data.clone());
    }
    let stamp = match &loaded.entry.lineage {
        EntryLineageV1::Manifest(stamp) => Some(stamp.clone()),
        EntryLineageV1::Bootstrap { .. } => None,
    };
    let placement = context
        .store
        .classify_lineage(
            loaded.entry.type_uuid,
            loaded.entry.schema_hash,
            stamp,
            project.logical_hash,
        )
        .map_err(BuildError::infrastructure)?;
    if !placement.permits_automatic_diff() {
        return Err(BuildError::Failed(format!(
            "asset {} cannot migrate from {} to {}: {placement:?}",
            loaded.entry.uuid, loaded.entry.schema_hash, project.logical_hash
        )));
    }
    let bundle = distill_bundle::parse_bundle(&loaded.bundle_bytes).map_err(BuildError::failed)?;
    let old = bundle
        .schemas
        .get(&loaded.entry.schema_hash)
        .ok_or_else(|| {
            BuildError::Failed("bundle omitted the entry's old schema snapshot".to_owned())
        })?;
    let plan = plan_automatic(&old.root, &project.logical_schema.root)
        .map_err(|error| BuildError::Failed(error.to_string()))?;
    validate_plan(
        &plan,
        &old.root,
        &project.logical_schema.root,
        EdgeKind::Automatic,
    )
    .map_err(|errors| {
        BuildError::Failed(format!("automatic migration plan rejected: {errors:?}"))
    })?;

    if migration_ops_use_defaults(&plan) {
        let key = CapabilityKey::DefaultTable(loaded.entry.type_uuid);
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

    let epoch = context
        .pipeline
        .epoch()
        .map_err(|poison| BuildError::Failed(poison.to_string()))?;
    let defaults = EpochDefaults::new(epoch, loaded.entry.type_uuid);
    let migrated = execute_ops(
        &plan,
        &loaded.entry.data,
        &old.root,
        &project.logical_schema.root,
        &defaults,
    )
    .map_err(|error| {
        if let Some(callback) = defaults.take_error() {
            BuildError::Failed(format!("default callback failed: {callback}"))
        } else {
            BuildError::Failed(error.to_string())
        }
    })?
    .value;
    conforms(&migrated, &project.logical_schema.root).map_err(|error| {
        BuildError::Failed(format!("migrated value is non-conforming: {error}"))
    })?;
    Ok(migrated)
}

fn encode_or_hydrate(
    context: &mut BuildContext<'_>,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
    terminal_type: TypeUuid,
    validator_dylib_hash: Option<[u8; 32]>,
) -> Result<
    (
        Vec<u8>,
        Vec<distill_wire::encode::EncodedReference>,
        Option<AuthoredValue>,
    ),
    BuildError,
> {
    let key = build_import_digest(&BuildImportInputs {
        asset: loaded.entry.uuid,
        bundle: loaded.meta.bundle,
        local_id: loaded.meta.local_id.clone(),
        authored_type: loaded.entry.type_uuid,
        terminal_type,
        canonical_bundle_bytes: loaded.bundle_bytes.clone(),
        logical: project.logical_hash,
        layout: project.layout_hash,
        migrations: Vec::new(),
        validator_dylib_hash,
        artifact_format_version: ARTIFACT_FORMAT_VERSION,
    });
    let trace_source = capture_trace_source(context)?;
    if let Some(hit) = lookup_persisted_candidate(
        context.store,
        KeyKind::BuildImport,
        &key,
        loaded.entry.uuid,
        &trace_source,
    )
    .map_err(BuildError::infrastructure)?
    {
        return match hit.outcome {
            PersistedOutcome::Success { outputs, aux }
                if outputs.len() == 1 && outputs[0].output_key.is_empty() && aux.is_empty() =>
            {
                Ok((outputs.into_iter().next().unwrap().bytes, Vec::new(), None))
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
                commit_migration_failure(
                    context,
                    loaded,
                    key,
                    &trace,
                    loaded.entry.schema_hash,
                    project.logical_hash,
                    MigrationPlanFailureV1::MissingPath {
                        path: FieldPath::root(),
                    },
                )?;
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
            context
                .store
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
    let encoded = encode_artifact_value(
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
    .map_err(BuildError::failed)?;
    let EncodedArtifact {
        bytes, references, ..
    } = encoded;
    let mut types = vec![loaded.entry.type_uuid];
    types.sort();
    types.dedup();
    context
        .store
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
    Ok((bytes, references, Some(current_value)))
}

fn resolve_reference(
    source: &StoreTraceSource,
    source_bundle: BundleUuid,
    value: &distill_json::AuthoredValue,
    expected: TypeUuid,
    _strong: bool,
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
    if canonical != bundle_bytes || bundle.uuid != bundle_meta.bundle {
        return Err(BuildError::Drifted(DriftedInput::File(
            bundle_meta.path.clone(),
        )));
    }
    let (local_id, entry) = find_bundle_asset(&bundle, asset)
        .ok_or_else(|| BuildError::Drifted(DriftedInput::Asset(asset)))?;
    if local_id != meta.local_id
        || entry.type_uuid != meta.type_uuid
        || entry.schema_hash != meta.logical_hash
    {
        return Err(BuildError::Drifted(DriftedInput::Asset(asset)));
    }
    Ok(LoadedAsset {
        meta,
        bundle_meta,
        bundle_bytes,
        entry: entry.clone(),
    })
}

fn find_bundle_asset(bundle: &Bundle, asset: AssetUuid) -> Option<(&str, &AssetEntry)> {
    bundle
        .assets
        .iter()
        .find_map(|(local_id, entry)| (entry.uuid == asset).then_some((local_id.as_str(), entry)))
}

fn merge_closure_row(
    closure: &mut BTreeMap<AssetUuid, ServedClosureRow>,
    row: ServedClosureRow,
) -> Result<(), BuildError> {
    if let Some(existing) = closure.get(&row.asset) {
        if existing != &row {
            return Err(BuildError::Failed(format!(
                "dependency closure disagrees for asset {}",
                row.asset
            )));
        }
    } else {
        closure.insert(row.asset, row);
    }
    Ok(())
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
    tags: Vec<String>,
}

#[derive(Clone)]
struct StoreTraceSource {
    entries: BTreeMap<AssetUuid, TraceEntry>,
    terminal_types: BTreeMap<AssetUuid, TypeUuid>,
    roles: BTreeMap<AssetUuid, EntryRole>,
    paths: BTreeMap<String, Vec<AssetUuid>>,
    tools: BTreeMap<String, [u8; 32]>,
    capabilities: Vec<(CapabilityKey, [u8; 32])>,
}

impl StoreTraceSource {
    fn capture(
        store: &Store,
        registry: &PipelineRegistry,
        target: &Target,
        basis: distill_store::state::InputVersion,
        epoch: &PipelineEpoch,
        dylib_hash: [u8; 32],
    ) -> Result<Self, BuildError> {
        let bundles = store
            .all_bundles()
            .map_err(BuildError::infrastructure)?
            .into_iter()
            .map(|bundle| (bundle.bundle, bundle.path))
            .collect::<BTreeMap<_, _>>();
        let mut entries = BTreeMap::new();
        for asset in store.all_asset_ids().map_err(BuildError::infrastructure)? {
            let Some(entry) = store.entry(asset).map_err(BuildError::failed)? else {
                continue;
            };
            let bundle_path = bundles.get(&entry.bundle).cloned().ok_or_else(|| {
                BuildError::Infrastructure("trace entry owner bundle is missing".to_owned())
            })?;
            entries.insert(
                asset,
                TraceEntry {
                    asset,
                    bundle: entry.bundle,
                    bundle_path,
                    local_id: entry.local_id,
                    authored_type: entry.type_uuid,
                    terminal_type: registry
                        .chain(entry.type_uuid, target)
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
            let terminal = registry
                .chain(parent_type.authored_type, target)
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
            .tool_hashes_at(basis)
            .map_err(BuildError::infrastructure)?;
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
        Ok(Self {
            entries,
            terminal_types,
            roles,
            paths,
            tools,
            capabilities,
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
                    tag.value.is_none() && entry.tags.iter().any(|actual| actual == &tag.tag)
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

fn no_trace<T>() -> Observed<T> {
    Observed::Err(StableFailureFingerprint::Local {
        class: LocalFailureClass::ArtifactEncoding,
        detail: [0; 32],
    })
}

impl TraceSource for StoreTraceSource {
    fn read(&self, _asset: AssetUuid) -> Observed<ContentHash> {
        no_trace()
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
        self.capabilities
            .iter()
            .find_map(|(registered, hash)| (registered == key).then_some(*hash))
            .map_or_else(
                || Observed::Err(StableFailureFingerprint::MissingCapability { key: key.clone() }),
                Observed::Ok,
            )
    }

    fn ref_check(&self, asset: AssetUuid, _expected: TypeUuid) -> Observed<Option<TypeUuid>> {
        Observed::Ok(self.terminal_types.get(&asset).copied())
    }

    fn role_check(&self, asset: AssetUuid) -> Observed<Option<EntryRole>> {
        Observed::Ok(self.roles.get(&asset).copied())
    }

    fn control(&self, _query: &ControlQuery) -> Observed<[u8; 32]> {
        no_trace()
    }

    fn control_read(&self, _subject: &ControlSubject) -> Observed<ControlValueHash> {
        no_trace()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::callbacks::{
        PipelineProcessContext, PipelineProcessor, ProcessorError, ProcessorProducts,
    };
    use distill_build::outputs::OutputDecls;
    use distill_build::pipeline::{GraphicsApi, TargetArch, TargetOs, TargetSelector};
    use distill_bundle::{EntryLineageV1, LineageStamp};
    use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch};
    use distill_json::AuthoredValue;
    use distill_rpc::{
        AuthoringEntry, AuthoringEntryRole, AuthoringValue, Commit, InputVersion, LoadPolicyEntry,
        TargetDefinition, TargetDefinitionHash,
    };
    use distill_schema::ngp_schema::{
        node_hash, snapshot_to_json, Field, FieldAttrs, FieldIdentifier, FieldLayout,
        LogicalSchema, PrimitiveKind, PrimitiveType, Schema, SchemaLayouts, SchemaNode,
        SchemaTypeId, TypeAttrs, TypeDef, TypeLayout, TypePath,
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
                    identity: distill_schema::bootstrap_gen_v1::consumer_compilation_identity_v1()
                        .clone(),
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
                    identity: distill_schema::bootstrap_gen_v1::consumer_compilation_identity_v1()
                        .clone(),
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
        let rows = distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1()
            .unwrap()
            .rows()
            .to_vec();
        let policy = rows
            .iter()
            .map(|row| LoadPolicyEntry {
                type_uuid: row.type_uuid,
                build_only: row.build_only,
            })
            .collect();
        TargetDefinition::canonical("dev", hash, rows, policy).unwrap()
    }

    struct CountingProcessor(Arc<AtomicUsize>);

    impl PipelineProcessor for CountingProcessor {
        fn process(
            &self,
            input: AuthoredValue,
            _context: &mut dyn PipelineProcessContext,
        ) -> Result<ProcessorProducts, ProcessorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ProcessorProducts {
                primary: Some(input.clone()),
                extras: BTreeMap::from([("metadata".to_owned(), input)]),
                debug: BTreeMap::from([("processor-log".to_owned(), b"ok".to_vec())]),
            })
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
                primary: Some(input),
                extras: BTreeMap::new(),
                debug: BTreeMap::new(),
            })
        }
    }

    #[test]
    fn production_backend_migrates_forward_lineage_before_processing_and_caches_it() {
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
                        epochs,
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
        let target_hash = TargetDefinitionHash(distill_build::keys::target_definition_hash(
            &build_target,
            &[],
        ));
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
        coordinator.install_build_target_for_test("dev", build_target);
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
        let request = BuildRequest {
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
        let target_hash = TargetDefinitionHash(distill_build::keys::target_definition_hash(
            &build_target,
            &[],
        ));
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
                        epochs,
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
            assets.join("byte.bundle"),
            distill_bundle::write_bundle(&bundle).unwrap(),
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
        coordinator.install_pipeline_epoch_for_test(crate::epoch::processor_test_epoch(
            "dev",
            target_hash.0,
            crate::callbacks::ProcessorDescriptor {
                id: "cook".to_owned(),
                version: 4,
                input: TYPE,
                selector: TargetSelector::new(None, None).unwrap(),
                outputs: OutputDecls::new(TERMINAL, vec![("metadata".to_owned(), EXTRA)]).unwrap(),
            },
            CountingProcessor(Arc::clone(&calls)),
        ));
        let request = BuildRequest {
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
        let first_memo = coordinator.store().lock().unwrap().memo_seq();
        assert_eq!(first.artifacts.len(), 2);
        assert_eq!(first.wire_trees.len(), 1);
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
        assert_eq!(extra.payload.encoded_type, EXTRA);
        assert_eq!(extra.payload.terminal_type, EXTRA);

        let hydrated = build(&coordinator, &request).unwrap();
        assert_eq!(hydrated, first);
        assert_eq!(coordinator.store().lock().unwrap().memo_seq(), first_memo);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn reference_trace_invalidates_on_resolution_role_or_terminal_type_drift() {
        let source = StoreTraceSource {
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
                    tags: Vec::new(),
                },
            )]),
            terminal_types: BTreeMap::from([(ASSET, TYPE)]),
            roles: BTreeMap::from([(ASSET, EntryRole::Runtime)]),
            paths: BTreeMap::from([("target.bundle".to_owned(), vec![ASSET])]),
            tools: BTreeMap::new(),
            capabilities: Vec::new(),
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
}
