//! Snapshot-pinned lazy build execution and durable build-import caching.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};

use distill_build::artifact_encode::{encode_artifact_value, ArtifactValueSpec, EncodedArtifact};
use distill_build::dslf::LocalFailureClass;
use distill_build::keys::{build_import_digest, BuildImportInputs};
use distill_build::persist::{lookup_persisted_candidate, PersistedOutcome};
use distill_build::query::{asset_query_result_hash, AssetQuery};
use distill_build::trace::{
    trace_payload_bytes, CapabilityKey, ControlQuery, ControlSubject, ControlValueHash, EntryRole,
    Observed, StableFailureFingerprint, TraceOp, TraceSource,
};
use distill_bundle::{AssetEntry, Bundle};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, TypeUuid};
use distill_rpc::{
    decode_asset_reference_query, decode_authoring_payload, ArtifactPayload, AssetReferenceQuery,
    BuildArtifactPublication, BuildBackend, BuildBackendOutcome, BuildPublication, BuildRequest,
    BuildWireTree, DriftedInput, RpcFailure, ServedClosureRow, ServedLoadEdge,
};
use distill_schema::{ProjectSchemaAuthority, ProjectTypeAuthority};
use distill_store::bundles::{BundleMeta, EntryMeta};
use distill_store::cas::record::KeyKind;
use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
use distill_store::Store;
use distill_wire::artifact::{parse_artifact, ARTIFACT_FORMAT_VERSION};

use crate::coordinator::DaemonCoordinator;
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
    row: ServedClosureRow,
    artifacts: BTreeMap<ContentHash, BuildArtifactPublication>,
    wire_trees: BTreeMap<LayoutHash, BuildWireTree>,
}

struct BuildContext<'a> {
    store: &'a mut Store,
    scanner: RootedScanner,
    authority: Arc<ProjectSchemaAuthority>,
    max_depth: usize,
    visiting: BTreeSet<AssetUuid>,
    memo: BTreeMap<AssetUuid, NodePublication>,
}

fn build(
    coordinator: &DaemonCoordinator,
    request: &BuildRequest,
) -> Result<BuildPublication, BuildError> {
    let store_handle = coordinator.store();
    let mut store = store_handle
        .lock()
        .map_err(|_| BuildError::Infrastructure("durable store mutex is poisoned".to_owned()))?;
    if store.instance_id() != request.basis.instance
        || store.input_version() != request.basis.version
    {
        return Err(BuildError::Drifted(request.drifted_input.clone()));
    }
    let authority = coordinator.schema_authority().ok_or_else(|| {
        BuildError::Failed("project schema authority is not published".to_owned())
    })?;
    let root = load_asset(&store, &coordinator.scanner(), request.entry.uuid)?;
    verify_request_entry(request, &root, &authority)?;

    let mut context = BuildContext {
        store: &mut store,
        scanner: coordinator.scanner(),
        authority,
        max_depth: coordinator.operational_configuration().max_dependency_depth,
        visiting: BTreeSet::new(),
        memo: BTreeMap::new(),
    };
    let root = build_asset(&mut context, request.entry.uuid, 0)?;
    Ok(BuildPublication {
        root_content_hash: root.row.content_hash,
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
        || requested.type_uuid != requested.terminal_type
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
    let project = authority.project_type(requested.type_uuid).ok_or_else(|| {
        BuildError::Failed(format!(
            "type {} has no project schema authority",
            requested.type_uuid
        ))
    })?;
    if loaded.entry.schema_hash != project.logical_hash {
        return Err(BuildError::Failed(format!(
            "asset {} requires schema migration before build",
            requested.uuid
        )));
    }
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
    if loaded.entry.schema_hash != project.logical_hash {
        return Err(BuildError::Failed(format!(
            "asset {asset} requires schema migration before build"
        )));
    }
    context
        .store
        .put_wire_tree(&project.dswl_bytes)
        .map_err(BuildError::infrastructure)?;

    let (bytes, references) = encode_or_hydrate(context, &loaded, &project)?;
    let view = parse_artifact(&bytes).map_err(BuildError::failed)?;
    verify_artifact_identity(&view, &loaded, &project)?;
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

    let load_edges = load_edges(context.store, &loaded, &view.load_deps, &references)?;
    let mut closure = BTreeMap::new();
    let mut artifacts = BTreeMap::new();
    let mut wire_trees = BTreeMap::from([(
        project.layout_hash,
        BuildWireTree {
            layout_hash: project.layout_hash,
            bytes: Arc::from(project.dswl_bytes.clone()),
        },
    )]);
    for edge in &load_edges {
        let child = build_asset(context, edge.asset, depth + 1)?;
        merge_closure_row(&mut closure, child.row.clone())?;
        for row in &child.artifacts[&child.row.content_hash]
            .payload
            .closure_rows
        {
            merge_closure_row(&mut closure, row.clone())?;
        }
        merge_artifacts(&mut artifacts, child.artifacts)?;
        merge_wire_trees(&mut wire_trees, child.wire_trees)?;
    }
    let content_hash = ContentHash(*blake3::hash(&bytes).as_bytes());
    let row = ServedClosureRow {
        asset,
        content_hash,
        authored_type: loaded.entry.type_uuid,
        encoded_type: loaded.entry.type_uuid,
        terminal_type: loaded.entry.type_uuid,
        load_edges,
    };
    merge_closure_row(&mut closure, row.clone())?;
    let closure_rows = closure.into_values().collect::<Vec<_>>();
    artifacts.insert(
        content_hash,
        BuildArtifactPublication {
            content_hash,
            payload: ArtifactPayload {
                structural: Arc::from(bytes[..structural_len].to_vec()),
                blobs,
                encoded_type: loaded.entry.type_uuid,
                terminal_type: loaded.entry.type_uuid,
                closure_rows,
            },
        },
    );
    Ok(NodePublication {
        row,
        artifacts,
        wire_trees,
    })
}

fn encode_or_hydrate(
    context: &mut BuildContext<'_>,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
) -> Result<(Vec<u8>, Vec<distill_wire::encode::EncodedReference>), BuildError> {
    let key = build_import_digest(&BuildImportInputs {
        asset: loaded.entry.uuid,
        bundle: loaded.meta.bundle,
        local_id: loaded.meta.local_id.clone(),
        authored_type: loaded.entry.type_uuid,
        terminal_type: loaded.entry.type_uuid,
        canonical_bundle_bytes: loaded.bundle_bytes.clone(),
        logical: project.logical_hash,
        layout: project.layout_hash,
        migrations: Vec::new(),
        artifact_format_version: ARTIFACT_FORMAT_VERSION,
    });
    let trace_source = StoreTraceSource::capture(context.store)?;
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
                Ok((outputs.into_iter().next().unwrap().bytes, Vec::new()))
            }
            PersistedOutcome::Success { .. } => Err(BuildError::Failed(
                "cached build-import result has an invalid output shape".to_owned(),
            )),
            PersistedOutcome::Failure { cause } => Err(BuildError::Failed(format!(
                "cached build-import failure: {cause:?}"
            ))),
        };
    }

    let source_bundle = loaded.meta.bundle;
    let mut trace = Vec::new();
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
            terminal_type: loaded.entry.type_uuid,
            encoded_type: loaded.entry.type_uuid,
            logical_hash: project.logical_hash,
            layout_hash: project.layout_hash,
            schema: &project.logical_schema.root,
            wire: &project.wire,
            value: &loaded.entry.data,
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
    Ok((bytes, references))
}

fn load_edges(
    store: &Store,
    loaded: &LoadedAsset,
    load_deps: &[AssetUuid],
    references: &[distill_wire::encode::EncodedReference],
) -> Result<Vec<ServedLoadEdge>, BuildError> {
    let fresh = references
        .iter()
        .filter(|reference| reference.strong)
        .map(|reference| {
            (
                reference.asset,
                ServedLoadEdge {
                    asset: reference.asset,
                    expected_terminal: reference.expected_terminal,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut edges = Vec::with_capacity(load_deps.len());
    for asset in load_deps {
        let target = store
            .runtime_entry(*asset)
            .map_err(BuildError::failed)?
            .ok_or_else(|| BuildError::Failed(format!("missing strong reference {asset}")))?;
        let expected = fresh
            .get(asset)
            .map_or(target.type_uuid, |edge| edge.expected_terminal);
        if target.type_uuid != expected {
            return Err(BuildError::Failed(format!(
                "reference from {} expects terminal type {}, but {} has {}",
                loaded.entry.uuid, expected, asset, target.type_uuid
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

fn verify_artifact_identity(
    view: &distill_wire::artifact::ArtifactView<'_>,
    loaded: &LoadedAsset,
    project: &ProjectTypeAuthority,
) -> Result<(), BuildError> {
    if view.asset_uuid != loaded.entry.uuid
        || view.authored_type != loaded.entry.type_uuid
        || view.encoded_type != loaded.entry.type_uuid
        || view.terminal_type != loaded.entry.type_uuid
        || view.logical_hash != project.logical_hash
        || view.layout_hash != project.layout_hash
    {
        return Err(BuildError::Failed(
            "cached build-import artifact identity does not match its DSBI inputs".to_owned(),
        ));
    }
    Ok(())
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
    role: EntryRole,
    tags: Vec<String>,
}

#[derive(Clone)]
struct StoreTraceSource {
    entries: BTreeMap<AssetUuid, TraceEntry>,
    paths: BTreeMap<String, Vec<AssetUuid>>,
}

impl StoreTraceSource {
    fn capture(store: &Store) -> Result<Self, BuildError> {
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
        Ok(Self { entries, paths })
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
                    .is_none_or(|terminal| terminal == entry.authored_type)
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

    fn tool(&self, _id: &str) -> Observed<[u8; 32]> {
        no_trace()
    }

    fn capability(&self, _key: &CapabilityKey) -> Observed<[u8; 32]> {
        no_trace()
    }

    fn ref_check(&self, asset: AssetUuid, _expected: TypeUuid) -> Observed<Option<TypeUuid>> {
        Observed::Ok(self.entries.get(&asset).map(|entry| entry.authored_type))
    }

    fn role_check(&self, asset: AssetUuid) -> Observed<Option<EntryRole>> {
        Observed::Ok(self.entries.get(&asset).map(|entry| entry.role))
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

    use distill_bundle::{EntryLineageV1, LineageStamp};
    use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch};
    use distill_json::AuthoredValue;
    use distill_rpc::{
        AuthoringEntry, AuthoringEntryRole, AuthoringValue, LoadPolicyEntry, TargetDefinition,
        TargetDefinitionHash,
    };
    use distill_schema::ngp_schema::{
        snapshot_to_json, Field, FieldAttrs, FieldIdentifier, FieldLayout, PrimitiveType, Schema,
        SchemaLayouts, SchemaTypeId, TypeAttrs, TypeDef, TypeLayout, TypePath,
    };
    use distill_store::StoreConfig;

    use crate::coordinator::LineageDestination;
    use crate::scanner::AssetRoot;

    const TYPE: TypeUuid = TypeUuid([71; 16]);
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
                    ],
                }],
            },
            [9; 32],
        )
        .unwrap()
    }

    fn target() -> TargetDefinition {
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
        TargetDefinition::canonical("dev", TargetDefinitionHash([4; 32]), rows, policy).unwrap()
    }

    #[test]
    fn production_backend_persists_and_hydrates_the_exact_build_import() {
        let temp = tempfile::tempdir().unwrap();
        let assets = temp.path().join("assets");
        std::fs::create_dir_all(&assets).unwrap();
        let authority = Arc::new(authority());
        let project = authority.project_type(TYPE).unwrap();
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
                vec![target()],
                64,
            )
            .unwrap(),
        );
        coordinator.reconcile_full_scan().unwrap();
        coordinator.install_schema_authority_for_test(authority.clone());
        let request = BuildRequest {
            basis: coordinator.server().current_stamp(),
            target: "dev".to_owned(),
            target_definition: TargetDefinitionHash([4; 32]),
            requested_asset: ASSET,
            output_key: String::new(),
            requested_terminal_type: TYPE,
            entry: AuthoringEntry {
                uuid: ASSET,
                bundle: BUNDLE,
                local_id: "entry".to_owned(),
                normalized_path: "byte.bundle".to_owned(),
                type_uuid: TYPE,
                terminal_type: TYPE,
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
        assert_eq!(first.artifacts.len(), 1);
        assert_eq!(first.wire_trees.len(), 1);
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
        assert_eq!(parsed.fixed, [7]);

        let hydrated = build(&coordinator, &request).unwrap();
        assert_eq!(hydrated, first);
        assert_eq!(coordinator.store().lock().unwrap().memo_seq(), first_memo);
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
                    role: EntryRole::Runtime,
                    tags: Vec::new(),
                },
            )]),
            paths: BTreeMap::from([("target.bundle".to_owned(), vec![ASSET])]),
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
        role_changed.entries.get_mut(&ASSET).unwrap().role = EntryRole::AuthoringOnly;
        assert!(!distill_build::trace::revalidate(&trace, &role_changed));

        let mut retyped = source;
        retyped.entries.get_mut(&ASSET).unwrap().authored_type = TypeUuid([88; 16]);
        assert!(!distill_build::trace::revalidate(&trace, &retyped));
    }
}
