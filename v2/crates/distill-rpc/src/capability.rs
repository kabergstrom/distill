//! Client capabilities over one connection's front end ([`Server`]):
//! target-bound hubs and snapshots, metadata bootstraps, delta streams and
//! progress completions. The capabilities of one connection share its front
//! end and nothing else. Every call reads the store through the
//! capability's own read transaction (snapshots) or the connection's
//! current-state reader (fences, CAS, the change log). A snapshot holds
//! nothing else: an artifact removed from the CAS after it resolved is a
//! cache miss for the client to retry.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use unicode_normalization::UnicodeNormalization;

use distill_store::bundles::AssetFilter;
use distill_store::served::{ResolutionRow, ServedEntryMeta, SERVED_RESTART_KEYS};
use distill_store::{Store, StoreError, StoreReader};

use crate::persist::decode_drifted_input;
use crate::server::{
    authoring_entry, entry_role, history_deltas, is_embedded, pipeline_failure,
    publish_backend_commit, store_failure, ConnectionState,
    MetadataBinding, SnapshotHold, SnapshotTxn, DEFAULT_CHUNK_SIZE,
};
use crate::validate::{
    path_glob_matches, valid_identifier, valid_logical_path, valid_logical_path_prefix,
};
use crate::*;

// ---------------------------------------------------------------------------
// Capability types

#[derive(Clone)]
pub struct Hub {
    pub(crate) server: Server,
    pub(crate) connection: Rc<RefCell<ConnectionState>>,
}

#[derive(Clone)]
pub struct MetadataHub {
    pub(crate) server: Server,
    pub(crate) binding: Rc<MetadataBinding>,
}

#[derive(Clone)]
pub struct MetadataSnapshot {
    server: Server,
    binding: Rc<MetadataBinding>,
    basis: MetadataBasis,
    hold: Rc<SnapshotHold>,
}

#[derive(Clone)]
pub struct MetadataAuthoringSnapshot {
    server: Server,
    binding: Rc<MetadataBinding>,
    basis: MetadataBasis,
    hold: Rc<SnapshotHold>,
}

#[derive(Clone)]
pub struct Snapshot {
    server: Server,
    connection: Rc<RefCell<ConnectionState>>,
    basis: RpcBasis,
    hold: Rc<SnapshotHold>,
}

#[derive(Clone)]
pub struct AuthoringSnapshot {
    server: Server,
    connection: Rc<RefCell<ConnectionState>>,
    basis: RpcBasis,
    hold: Rc<SnapshotHold>,
}

#[derive(Clone)]
pub struct DeltaStream {
    server: Server,
    connection: Rc<RefCell<ConnectionState>>,
}

impl fmt::Debug for Hub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hub")
            .field("connection", &self.connection.borrow().id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for MetadataHub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataHub")
            .field("binding", &self.binding.id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for MetadataSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataSnapshot")
            .field("basis", &self.basis)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for MetadataAuthoringSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataAuthoringSnapshot")
            .field("basis", &self.basis)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("basis", &self.basis)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for AuthoringSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthoringSnapshot")
            .field("basis", &self.basis)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for DeltaStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaStream").finish_non_exhaustive()
    }
}

fn metadata_basis(binding: &MetadataBinding, snapshot: SnapshotStamp) -> MetadataBasis {
    MetadataBasis {
        snapshot,
        protocol_epoch: binding.protocol_epoch,
    }
}

/// Unwrap a store read inside an `RpcResult` method.
macro_rules! rpc_try {
    ($expr:expr) => {
        match $expr {
            Ok(value) => value,
            Err(error) => return RpcResult::Failure(store_failure(error)),
        }
    };
}

macro_rules! metadata_try {
    ($expr:expr) => {
        match $expr {
            Ok(value) => value,
            Err(error) => return MetadataCall::Error(store_failure(error)),
        }
    };
}

macro_rules! namespace_try {
    ($expr:expr) => {
        match $expr {
            Ok(value) => value,
            Err(error) => return MetadataNamespaceCall::Error(store_failure(error)),
        }
    };
}

// ---------------------------------------------------------------------------
// Artifacts

/// Load, authenticate and split one artifact from the CAS, with its
/// recorded typed load edges.
pub(crate) fn load_artifact(
    reader: &StoreReader,
    hash: ContentHash,
) -> Result<(ArtifactPayload, LayoutHash), RpcFailure> {
    let bytes = match reader.cas_read(&hash.0) {
        Ok(bytes) => bytes,
        Err(StoreError::NotFound { .. }) => return Err(RpcFailure::ArtifactNotFound { hash }),
        Err(error) => return Err(store_failure(error)),
    };
    let (structural, blobs) = distill_wire::artifact::split_artifact(&bytes)
        .map_err(|_| RpcFailure::ArtifactNotFound { hash })?;
    let parsed = distill_wire::artifact::parse_artifact_parts(structural, &blobs)
        .map_err(|_| RpcFailure::ArtifactNotFound { hash })?;
    let edges = reader.artifact_load_edges(hash).map_err(store_failure)?;
    let edge_assets = edges.iter().map(|(asset, _)| *asset).collect::<Vec<_>>();
    if parsed.content_hash != hash || edge_assets != parsed.load_deps {
        return Err(RpcFailure::ArtifactNotFound { hash });
    }
    let layout_hash = parsed.layout_hash;
    let payload = ArtifactPayload {
        structural: Arc::from(structural),
        blobs: blobs.into_iter().map(Arc::from).collect(),
        load_edges: edges
            .into_iter()
            .map(|(asset, expected_terminal)| ServedLoadEdge {
                asset,
                expected_terminal,
            })
            .collect(),
    };
    Ok((payload, layout_hash))
}

fn load_wire_tree(reader: &StoreReader, hash: LayoutHash) -> Result<Arc<[u8]>, RpcFailure> {
    match reader.wire_tree_read(hash) {
        Ok(bytes) => Ok(Arc::from(bytes)),
        Err(StoreError::NotFound { .. }) => Err(RpcFailure::WireTreeNotFound { hash }),
        Err(error) => Err(store_failure(error)),
    }
}

pub(crate) fn chunk_payload(payload: &ArtifactPayload, chunk_size: usize) -> ChunkStream {
    let total_bytes = payload
        .blobs
        .iter()
        .try_fold(payload.structural.len() as u64, |total, blob| {
            total.checked_add(blob.len() as u64)
        })
        .expect("one process cannot hold more artifact bytes than u64");
    ChunkStream {
        structural: Arc::clone(&payload.structural),
        blobs: payload.blobs.clone(),
        chunk_size,
        section: 0,
        offset: 0,
        total_bytes,
        load_edges: payload.load_edges.clone(),
    }
}

// ---------------------------------------------------------------------------
// Queries

/// Run a pure metadata query as one indexed SQL query on the snapshot.
fn query_pure_metadata(
    snapshot: &StoreReader,
    query: &PureMetadataQuery,
) -> Result<Vec<AssetUuid>, StoreError> {
    let filter = AssetFilter {
        asset: query.uuid,
        bundle: query.bundle,
        authored_type: query.authored_type,
        path_prefixes: query.normalized_path_prefix.iter().cloned().collect(),
        authoring_only: query
            .role
            .map(|role| role == AuthoringEntryRole::AuthoringOnly),
        ..AssetFilter::default()
    };
    Ok(snapshot
        .served_assets_matching(&filter)?
        .into_iter()
        .map(|matched| matched.asset)
        .collect())
}

/// The whole-table scan [`query_pure_metadata`] replaced, kept to pin its
/// results.
#[cfg(test)]
fn query_pure_metadata_scan(entries: &[ServedEntryMeta], query: &PureMetadataQuery) -> Vec<AssetUuid> {
    entries
        .iter()
        .filter(|entry| {
            query.uuid.is_none_or(|wanted| wanted == entry.asset)
                && query.bundle.is_none_or(|wanted| wanted == entry.bundle)
                && query
                    .normalized_path_prefix
                    .as_ref()
                    .is_none_or(|prefix| entry.normalized_path.starts_with(prefix))
                && query
                    .authored_type
                    .is_none_or(|wanted| wanted == entry.type_uuid)
                && query
                    .role
                    .is_none_or(|wanted| wanted == entry_role(entry.authoring_only))
        })
        .map(|entry| entry.asset)
        .collect()
}

fn validate_asset_query(query: &AssetQuery, allow_authoring: bool) -> Result<(), String> {
    let populated = query.uuid.is_some()
        || query.bundle_path.is_some()
        || query.local_id.is_some()
        || query.bundle_uuid.is_some()
        || query.authored_type.is_some()
        || query.terminal_type.is_some()
        || query.tag.is_some()
        || query.path_prefix.is_some()
        || query.path_glob.is_some()
        || query.authoring_only.is_some();
    if !populated {
        return Err("asset query must contain at least one selector".to_owned());
    }
    if query.authoring_only == Some(true) && !allow_authoring {
        return Err("authoringOnly=true is restricted to tooling snapshots".to_owned());
    }
    if query.local_id.is_some() && query.bundle_path.is_none() && query.bundle_uuid.is_none() {
        return Err("bundle-relative local_id query is not closed over a bundle".to_owned());
    }
    if query
        .bundle_path
        .as_deref()
        .is_some_and(|path| !valid_logical_path(path))
    {
        return Err("bundle path is not canonical".to_owned());
    }
    if query
        .path_prefix
        .as_deref()
        .is_some_and(|path| !valid_logical_path_prefix(path))
    {
        return Err("path prefix is not canonical".to_owned());
    }
    if query
        .local_id
        .as_deref()
        .is_some_and(|id| !valid_identifier(id))
    {
        return Err("local id is not canonical".to_owned());
    }
    if let Some(tag) = &query.tag {
        if !valid_identifier(&tag.tag)
            || tag
                .value
                .as_deref()
                .is_some_and(|value| !valid_identifier(value))
        {
            return Err("tag selector is not canonical".to_owned());
        }
    }
    if query
        .path_glob
        .as_deref()
        .is_some_and(|glob| glob.is_empty() || glob.contains('\0') || !glob.nfc().eq(glob.chars()))
    {
        return Err("path glob is not canonical".to_owned());
    }
    Ok(())
}

/// The SQL half of an asset query: every selector but the glob, which
/// contributes only its literal prefix (the text before its first `*` or
/// `?`, which every match starts with) and is matched on the rows.
fn asset_filter(query: &AssetQuery, role: AuthoringEntryRole) -> AssetFilter {
    let mut path_prefixes = query.path_prefix.iter().cloned().collect::<Vec<_>>();
    if let Some(glob) = &query.path_glob {
        let literal = &glob[..glob.find(['*', '?']).unwrap_or(glob.len())];
        if !literal.is_empty() {
            path_prefixes.push(literal.to_owned());
        }
    }
    AssetFilter {
        asset: query.uuid,
        bundle: query.bundle_uuid,
        bundle_path: query.bundle_path.clone(),
        local_id: query.local_id.clone(),
        authored_type: query.authored_type,
        terminal_type: query.terminal_type,
        tag: query
            .tag
            .as_ref()
            .map(|tag| (tag.tag.clone(), tag.value.clone())),
        path_prefixes,
        authoring_only: Some(role == AuthoringEntryRole::AuthoringOnly),
        tag_index_poisoned: false,
    }
}

#[cfg(test)]
fn query_matches(entry: &ServedEntryMeta, query: &AssetQuery, role: AuthoringEntryRole) -> bool {
    query.uuid.is_none_or(|wanted| wanted == entry.asset)
        && query
            .bundle_path
            .as_ref()
            .is_none_or(|path| path == &entry.normalized_path)
        && query
            .local_id
            .as_ref()
            .is_none_or(|local_id| local_id == &entry.local_id)
        && query
            .bundle_uuid
            .is_none_or(|bundle| bundle == entry.bundle)
        && query
            .authored_type
            .is_none_or(|type_uuid| type_uuid == entry.type_uuid)
        && query
            .terminal_type
            .is_none_or(|type_uuid| type_uuid == entry.terminal_type)
        && query.tag.as_ref().is_none_or(|tag| {
            entry.tags.get(&tag.tag).is_some_and(|value| {
                tag.value
                    .as_ref()
                    .is_none_or(|wanted| value.as_ref().is_some_and(|actual| actual == wanted))
            })
        })
        && query
            .path_prefix
            .as_ref()
            .is_none_or(|prefix| entry.normalized_path.starts_with(prefix))
        && query
            .path_glob
            .as_ref()
            .is_none_or(|glob| path_glob_matches(glob, &entry.normalized_path))
        && entry_role(entry.authoring_only) == role
}

/// Run an asset query over a pinned snapshot, failing when a tag query
/// would consult a poisoned tag index: the entries the query selects without
/// its tag whose tag index is poisoned name the failing bundles. Both reads
/// are indexed SQL queries; only the glob is matched here.
fn query_assets(
    snapshot: &StoreReader,
    query: &AssetQuery,
    role: AuthoringEntryRole,
) -> Result<Result<Vec<AssetUuid>, RpcFailure>, StoreError> {
    let glob_matches = |path: &str| {
        query
            .path_glob
            .as_ref()
            .is_none_or(|glob| path_glob_matches(glob, path))
    };
    if query.tag.is_some() {
        let poisoned = AssetFilter {
            tag: None,
            tag_index_poisoned: true,
            ..asset_filter(query, role)
        };
        let bundles = snapshot
            .served_assets_matching(&poisoned)?
            .into_iter()
            .filter(|matched| glob_matches(&matched.path))
            .map(|matched| matched.bundle)
            .collect::<BTreeSet<_>>();
        if !bundles.is_empty() {
            return Ok(Err(RpcFailure::TagIndexPoisoned {
                bundles: bundles.into_iter().collect(),
            }));
        }
    }
    Ok(Ok(snapshot
        .served_assets_matching(&asset_filter(query, role))?
        .into_iter()
        .filter(|matched| glob_matches(&matched.path))
        .map(|matched| matched.asset)
        .collect()))
}

/// The whole-table scan [`query_assets`] replaced, kept to pin its results.
#[cfg(test)]
fn query_assets_scan(
    snapshot: &StoreReader,
    query: &AssetQuery,
    role: AuthoringEntryRole,
) -> Result<Result<Vec<AssetUuid>, RpcFailure>, StoreError> {
    let entries = snapshot.served_entries()?;
    if query.tag.is_some() {
        let mut without_tag = query.clone();
        without_tag.tag = None;
        let by_asset = entries
            .iter()
            .map(|entry| (entry.asset, entry))
            .collect::<BTreeMap<_, _>>();
        let bundles = snapshot
            .tag_poisoned_assets()?
            .into_iter()
            .filter_map(|(asset, bundle)| {
                let entry = by_asset.get(&asset)?;
                query_matches(entry, &without_tag, role).then_some(bundle)
            })
            .collect::<BTreeSet<_>>();
        if !bundles.is_empty() {
            return Ok(Err(RpcFailure::TagIndexPoisoned {
                bundles: bundles.into_iter().collect(),
            }));
        }
    }
    Ok(Ok(entries
        .iter()
        .filter(|entry| query_matches(entry, query, role))
        .map(|entry| entry.asset)
        .collect()))
}

fn metadata_entry(entry: &ServedEntryMeta) -> MetadataEntry {
    MetadataEntry {
        uuid: entry.asset,
        bundle: entry.bundle,
        local_id: entry.local_id.clone(),
        normalized_path: entry.normalized_path.clone(),
        authored_type: entry.type_uuid,
        terminal_type: entry.terminal_type,
        schema_hash: entry.logical_hash,
        role: entry_role(entry.authoring_only),
        tags: entry.tags.clone(),
    }
}

fn pure_metadata_entry(entry: &ServedEntryMeta) -> PureMetadataEntry {
    PureMetadataEntry {
        uuid: entry.asset,
        bundle: entry.bundle,
        local_id: entry.local_id.clone(),
        normalized_path: entry.normalized_path.clone(),
        authored_type: entry.type_uuid,
        schema_hash: entry.logical_hash,
        role: entry_role(entry.authoring_only),
    }
}

fn resolve_path_in(snapshot: &StoreReader, path: &str) -> Result<PathResolveResult, StoreError> {
    let candidates = snapshot.served_path_candidates(path)?;
    Ok(match candidates.len() {
        0 => PathResolveResult::Missing,
        1 => PathResolveResult::Resolved(*candidates.first().expect("one candidate")),
        _ => PathResolveResult::Failed(PathResolveFailure::Ambiguous {
            candidates: candidates.into_iter().collect(),
        }),
    })
}

fn resolve_named_in(
    snapshot: &StoreReader,
    path: &str,
    name: &str,
) -> Result<PathResolveResult, StoreError> {
    let candidates = snapshot.served_named_candidates(path, name)?;
    Ok(match candidates.len() {
        0 => PathResolveResult::Missing,
        1 => PathResolveResult::Resolved(*candidates.first().expect("one candidate")),
        _ => PathResolveResult::Failed(PathResolveFailure::Ambiguous {
            candidates: candidates.into_iter().collect(),
        }),
    })
}

fn inspect_authoring(
    snapshot: &StoreReader,
    stamp: SnapshotStamp,
    uuid: AssetUuid,
) -> Result<AuthoringInspectResult, StoreError> {
    let Some(entry) = snapshot.served_entry(uuid)? else {
        if snapshot.asset_resolution(uuid)?.is_some() {
            return Ok(AuthoringInspectResult::RoleIneligible {
                observed: AuthoringEntryRole::Runtime,
            });
        }
        return Ok(AuthoringInspectResult::Missing);
    };
    let entry = authoring_entry(entry)?;
    Ok(AuthoringInspectResult::Inspection(AuthoringInspection {
        stamp,
        uuid: entry.uuid,
        bundle: entry.bundle,
        local_id: entry.local_id,
        normalized_path: entry.normalized_path,
        type_uuid: entry.type_uuid,
        schema_hash: entry.schema_hash,
        logical_schema: entry.logical_schema,
        role: entry.role,
        value: entry.value,
    }))
}

// ---------------------------------------------------------------------------
// Authoring gates and completions

#[derive(Debug)]
enum AuthoringGate {
    Reconnect(ReconnectReason),
    ConfigurationFailed(ConfigurationError),
    Failure(RpcFailure),
}

impl AuthoringGate {
    fn into_result<T>(self) -> RpcResult<T> {
        match self {
            Self::Reconnect(reason) => RpcResult::ReconnectRequired { reason },
            Self::ConfigurationFailed(error) => RpcResult::ConfigurationFailed(error),
            Self::Failure(error) => RpcResult::Failure(error),
        }
    }
}

fn authoring_gate(
    server: &Server,
    connection: &ConnectionState,
    base: InputVersion,
) -> Option<AuthoringGate> {
    if let Some(reason) = server.inner.generation_fence(connection) {
        return Some(AuthoringGate::Reconnect(reason));
    }
    let current = server.inner.current_stamp().version;
    if base != current {
        return Some(AuthoringGate::Failure(RpcFailure::StaleInputVersion {
            expected: current,
            got: base,
        }));
    }
    let txn = match server.inner.current_snapshot() {
        Ok(txn) => txn,
        Err(error) => return Some(AuthoringGate::Failure(store_failure(error))),
    };
    if let ConfigurationStatus::Failed(error) = &txn.configuration {
        return Some(AuthoringGate::ConfigurationFailed(error.clone()));
    }
    if let Some(error) = pipeline_failure(&server.inner.effective_pipeline(&txn)) {
        return Some(AuthoringGate::Failure(error));
    }
    None
}

fn validate_import_request(request: &ImportRequest) -> Result<(), String> {
    if !valid_identifier(&request.importer) {
        return Err("importer ID is not canonical".to_owned());
    }
    if request.sources.is_empty() || request.sources.iter().any(|path| !valid_logical_path(path)) {
        return Err("import sources must be nonempty canonical logical paths".to_owned());
    }
    if !valid_logical_path(&request.dest) {
        return Err("import destination is not a canonical logical path".to_owned());
    }
    if !request.root.is_empty() && !valid_identifier(&request.root) {
        return Err("import root is not canonical".to_owned());
    }
    Ok(())
}

fn validate_progress(events: &[AuthoringProgressEvent]) -> Result<(), String> {
    if events.is_empty() {
        return Err("long-running operation returned no progress events".to_owned());
    }
    for (index, event) in events.iter().enumerate() {
        if event.sequence != index as u64 {
            return Err("progress event sequences must be contiguous from zero".to_owned());
        }
        if index == 0 && event.state != AuthoringProgressState::Started {
            return Err("progress stream must begin with Started".to_owned());
        }
        if event.state.is_terminal() != (index + 1 == events.len()) {
            return Err("progress stream must have exactly one final terminal event".to_owned());
        }
    }
    Ok(())
}

/// What a write answered: `None` when the backend declined.
fn write_outcome(outcome: Option<RpcResult<InputVersion>>) -> RpcResult<InputVersion> {
    outcome.unwrap_or_else(|| {
        RpcResult::Failure(RpcFailure::AuthoringBackendUnavailable {
            operation: "write".to_owned(),
        })
    })
}

/// Complete a prepared operation as one input on `server`'s writer, still
/// at `base`.
fn complete_publication(
    server: &Server,
    base: InputVersion,
    publication: PreparedOperationPublication,
    what: &'static str,
) -> Result<(), String> {
    {
        let mut failed = None;
        let mut terminal = None;
        let published = server.coordinated_maybe_commit(base, |store| {
            let (commit, terminal_error) = match publication {
                PreparedOperationPublication::Immediate(commit) => (*commit, None),
                PreparedOperationPublication::Deferred(operation) => {
                    match operation.complete(store, base) {
                        Ok(completed) => (completed.commit, completed.terminal_error),
                        Err(error) => {
                            failed = Some(error);
                            return Ok(None);
                        }
                    }
                }
            };
            terminal = terminal_error;
            Ok(Some(commit))
        });
        match published {
            Ok(_) if failed.is_some() => Err(failed.expect("checked")),
            Ok(_) => terminal.map_or(Ok(()), Err),
            Err(CoordinatedCommitError::Stale { expected, observed }) => Err(format!(
                "{what} lost its input basis: expected {expected:?}, observed {observed:?}"
            )),
            Err(error) => Err(format!(
                "{what} commit rejected: {:?}",
                coordinated_failure(error)
            )),
        }
    }
}

/// A coordinated publication's failure as the RPC reports it.
fn coordinated_failure(error: CoordinatedCommitError) -> RpcFailure {
    match error {
        CoordinatedCommitError::Stale { expected, observed } => RpcFailure::StaleInputVersion {
            expected: observed,
            got: expected,
        },
        CoordinatedCommitError::Invalid(error) => RpcFailure::InvalidAuthoringRequest {
            detail: format!("authoring backend produced an invalid commit: {error:?}"),
        },
        CoordinatedCommitError::Publication(detail) => {
            RpcFailure::InvalidAuthoringRequest { detail }
        }
    }
}

struct ServerOperationCompletion {
    server: Server,
    connection: Rc<RefCell<ConnectionState>>,
    base: InputVersion,
    publication: RefCell<Option<PreparedOperationPublication>>,
}

impl ProgressCompletion for ServerOperationCompletion {
    fn complete(&self) -> Result<(), String> {
        let publication = self
            .publication
            .borrow_mut()
            .take()
            .ok_or_else(|| "long-running operation is already terminal".to_owned())?;
        if let Some(gate) = authoring_gate(&self.server, &self.connection.borrow(), self.base) {
            return Err(format!(
                "long-running operation lost its publication basis: {gate:?}"
            ));
        }
        complete_publication(&self.server, self.base, publication, "long-running operation")
    }

    fn cancel(&self) -> bool {
        self.publication.borrow_mut().take().is_some()
    }
}

// ---------------------------------------------------------------------------
// Metadata capabilities

impl MetadataHub {
    pub fn connection_id(&self) -> u64 {
        self.binding.id
    }

    fn open(&self) -> Result<(MetadataBasis, Rc<SnapshotHold>), MetadataCall<()>> {
        let txn = self
            .server
            .inner
            .current_snapshot()
            .map_err(|error| MetadataCall::Error(store_failure(error)))?;
        let basis = metadata_basis(&self.binding, txn.stamp);
        let hold = self
            .server
            .inner
            .register_snapshot(txn)
            .map_err(MetadataCall::Error)?;
        Ok((basis, hold))
    }

    pub fn snapshot(&self) -> MetadataCall<MetadataSnapshot> {
        if let Some(reason) = self.server.inner.metadata_fence(&self.binding) {
            return MetadataCall::ReconnectRequired { reason };
        }
        match self.open() {
            Ok((basis, hold)) => MetadataCall::Success(MetadataSnapshot {
                server: self.server.clone(),
                binding: self.binding.clone(),
                basis,
                hold,
            }),
            Err(failure) => failure.retype(),
        }
    }

    pub fn authoring_snapshot(&self) -> MetadataCall<MetadataAuthoringSnapshot> {
        if let Some(reason) = self.server.inner.metadata_fence(&self.binding) {
            return MetadataCall::ReconnectRequired { reason };
        }
        match self.open() {
            Ok((basis, hold)) => MetadataCall::Success(MetadataAuthoringSnapshot {
                server: self.server.clone(),
                binding: self.binding.clone(),
                basis,
                hold,
            }),
            Err(failure) => failure.retype(),
        }
    }

    pub fn diagnostics(&self) -> MetadataCall<MetadataDiagnostics> {
        if let Some(reason) = self.server.inner.metadata_fence(&self.binding) {
            return MetadataCall::ReconnectRequired { reason };
        }
        let txn = metadata_try!(self.server.inner.current_snapshot());
        MetadataCall::Success(MetadataDiagnostics {
            stamp: txn.stamp,
            configuration: txn.configuration.clone(),
            pipeline: self.server.inner.effective_pipeline(&txn),
            namespace_errors: metadata_try!(txn.snapshot().namespace_errors()),
        })
    }

    pub fn fetch(&self, hash: ContentHash) -> MetadataCall<ChunkStream> {
        if let Some(reason) = self.server.inner.metadata_fence(&self.binding) {
            return MetadataCall::ReconnectRequired { reason };
        }
        match load_artifact(&self.server.inner.reader, hash) {
            Ok((payload, _)) => MetadataCall::Success(chunk_payload(&payload, DEFAULT_CHUNK_SIZE)),
            Err(error) => MetadataCall::Error(error),
        }
    }

}

impl<T> MetadataCall<T> {
    fn retype<U>(self) -> MetadataCall<U> {
        match self {
            Self::Success(_) => unreachable!("only failures are retyped"),
            Self::ReconnectRequired { reason } => MetadataCall::ReconnectRequired { reason },
            Self::SnapshotExpired => MetadataCall::SnapshotExpired,
            Self::Error(error) => MetadataCall::Error(error),
        }
    }
}

/// The shared body of the two metadata snapshot kinds.
struct MetadataView<'a> {
    server: &'a Server,
    binding: &'a MetadataBinding,
    hold: &'a SnapshotHold,
}

impl MetadataView<'_> {
    fn preflight<T>(&self) -> Option<MetadataCall<T>> {
        if let Some(reason) = self.server.inner.metadata_fence(self.binding) {
            return Some(MetadataCall::ReconnectRequired { reason });
        }
        if !self.hold.alive() {
            return Some(MetadataCall::SnapshotExpired);
        }
        None
    }

    fn namespace<T>(&self) -> Result<Rc<SnapshotTxn>, MetadataNamespaceCall<T>> {
        if let Some(reason) = self.server.inner.metadata_fence(self.binding) {
            return Err(MetadataNamespaceCall::ReconnectRequired { reason });
        }
        let Some(txn) = self.hold.txn() else {
            return Err(MetadataNamespaceCall::SnapshotExpired);
        };
        Ok(txn)
    }

    fn query(&self, query: &PureMetadataQuery) -> MetadataNamespaceCall<Vec<AssetUuid>> {
        let txn = match self.namespace() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if query
            .normalized_path_prefix
            .as_ref()
            .is_some_and(|path| !valid_logical_path_prefix(path))
        {
            return MetadataNamespaceCall::Error(RpcFailure::InvalidPath {
                path: query.normalized_path_prefix.clone().unwrap_or_default(),
            });
        }
        MetadataNamespaceCall::Success(namespace_try!(query_pure_metadata(
            txn.snapshot(),
            query
        )))
    }

    fn refresh(&self) -> Result<(MetadataBasis, Rc<SnapshotHold>), MetadataCall<()>> {
        if let Some(result) = self.preflight() {
            return Err(result);
        }
        let txn = self
            .server
            .inner
            .current_snapshot()
            .map_err(|error| MetadataCall::Error(store_failure(error)))?;
        let basis = metadata_basis(self.binding, txn.stamp);
        let hold = self
            .server
            .inner
            .register_snapshot(txn)
            .map_err(MetadataCall::Error)?;
        Ok((basis, hold))
    }
}

impl MetadataSnapshot {
    fn view(&self) -> MetadataView<'_> {
        MetadataView {
            server: &self.server,
            binding: &self.binding,
            hold: &self.hold,
        }
    }

    pub fn basis(&self) -> MetadataBasis {
        self.basis
    }

    pub fn version(&self) -> MetadataCall<InputVersion> {
        if let Some(result) = self.view().preflight() {
            return result;
        }
        MetadataCall::Success(self.basis.snapshot.version)
    }

    pub fn diagnostics(&self) -> MetadataCall<MetadataDiagnostics> {
        if let Some(result) = self.view().preflight() {
            return result;
        }
        let Some(txn) = self.hold.txn() else {
            return MetadataCall::SnapshotExpired;
        };
        MetadataCall::Success(MetadataDiagnostics {
            stamp: self.basis.snapshot,
            configuration: txn.configuration.clone(),
            pipeline: self.server.inner.effective_pipeline(&txn),
            namespace_errors: metadata_try!(txn.snapshot().namespace_errors()),
        })
    }

    pub fn query(&self, query: &PureMetadataQuery) -> MetadataNamespaceCall<Vec<AssetUuid>> {
        self.view().query(query)
    }

    pub fn entry(&self, uuid: AssetUuid) -> MetadataNamespaceCall<PureMetadataEntry> {
        let txn = match self.view().namespace() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        match namespace_try!(txn.snapshot().served_entry_meta(uuid)) {
            Some(entry) => MetadataNamespaceCall::Success(pure_metadata_entry(&entry)),
            None => MetadataNamespaceCall::Error(RpcFailure::AssetNotFound { uuid }),
        }
    }

    pub fn resolve_path(&self, path: &str) -> MetadataNamespaceCall<PathResolveResult> {
        let txn = match self.view().namespace() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if !valid_logical_path(path) {
            return MetadataNamespaceCall::Error(RpcFailure::InvalidPath {
                path: path.to_owned(),
            });
        }
        MetadataNamespaceCall::Success(namespace_try!(resolve_path_in(txn.snapshot(), path)))
    }

    pub fn refresh(&self) -> MetadataCall<MetadataSnapshot> {
        match self.view().refresh() {
            Ok((basis, hold)) => MetadataCall::Success(Self {
                server: self.server.clone(),
                binding: self.binding.clone(),
                basis,
                hold,
            }),
            Err(failure) => failure.retype(),
        }
    }

    pub fn expire(&self) {
        self.hold.expire();
    }

    /// Release this snapshot once its TTL passes (on the RPC `LocalSet`).
    pub(crate) fn expire_later(&self) {
        self.hold.expire_later();
    }
}

impl MetadataAuthoringSnapshot {
    fn view(&self) -> MetadataView<'_> {
        MetadataView {
            server: &self.server,
            binding: &self.binding,
            hold: &self.hold,
        }
    }

    pub fn basis(&self) -> MetadataBasis {
        self.basis
    }

    pub fn version(&self) -> MetadataCall<InputVersion> {
        if let Some(result) = self.view().preflight() {
            return result;
        }
        MetadataCall::Success(self.basis.snapshot.version)
    }

    pub fn query(&self, query: &PureMetadataQuery) -> MetadataNamespaceCall<Vec<AssetUuid>> {
        self.view().query(query)
    }

    pub fn inspect(&self, uuid: AssetUuid) -> MetadataNamespaceCall<AuthoringInspectResult> {
        let txn = match self.view().namespace() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        MetadataNamespaceCall::Success(namespace_try!(inspect_authoring(
            txn.snapshot(),
            self.basis.snapshot,
            uuid
        )))
    }

    pub fn refresh(&self) -> MetadataCall<MetadataAuthoringSnapshot> {
        match self.view().refresh() {
            Ok((basis, hold)) => MetadataCall::Success(Self {
                server: self.server.clone(),
                binding: self.binding.clone(),
                basis,
                hold,
            }),
            Err(failure) => failure.retype(),
        }
    }

    pub fn expire(&self) {
        self.hold.expire();
    }

    /// Release this snapshot once its TTL passes (on the RPC `LocalSet`).
    pub(crate) fn expire_later(&self) {
        self.hold.expire_later();
    }
}

// ---------------------------------------------------------------------------
// Hub

impl Hub {
    pub fn connection_id(&self) -> u64 {
        self.connection.borrow().id
    }

    /// Cheap transport gate used before decoding target-bound request
    /// parameters. A stale capability must reconnect even when its payload is
    /// malformed or the requested method is reserved.
    pub fn generation_reconnect(&self) -> Option<ReconnectReason> {
        self.server.inner.generation_fence(&self.connection.borrow())
    }

    fn live<T>(&self) -> Option<RpcResult<T>> {
        self.server.inner.pump(&self.connection);
        let connection = self.connection.borrow();
        self.server
            .inner
            .generation_fence(&connection)
            .map(|reason| RpcResult::ReconnectRequired { reason })
    }

    pub fn snapshot(&self) -> RpcResult<Snapshot> {
        if let Some(result) = self.live() {
            return result;
        }
        let txn = rpc_try!(self.server.inner.current_snapshot());
        snapshot_from(&self.server, &self.connection, txn)
    }

    /// Pin a tooling-only view. It shares the same immutable store stamp and
    /// complete connection-generation fence as the runtime snapshot, but has
    /// no resolve, fetch, dependency, or pack capability.
    pub fn authoring_snapshot(&self) -> RpcResult<AuthoringSnapshot> {
        if let Some(result) = self.live() {
            return result;
        }
        let txn = rpc_try!(self.server.inner.current_snapshot());
        authoring_snapshot_from(&self.server, &self.connection, txn)
    }

    fn authoring_gate<T>(&self, base: InputVersion) -> Option<RpcResult<T>> {
        self.server.inner.pump(&self.connection);
        authoring_gate(&self.server, &self.connection.borrow(), base)
            .map(AuthoringGate::into_result)
    }

    fn publish<T>(&self, base: InputVersion, commit: Commit, value: T) -> RpcResult<T> {
        match publish_backend_commit(&self.server, base, commit) {
            Ok(_) => RpcResult::Success(value),
            Err(error) => RpcResult::Failure(error),
        }
    }

    /// Run the backend's `prepare` on this connection's writer and publish
    /// its commit as one input, still at `base`. `None` when the backend
    /// declined.
    fn prepared<T>(
        &self,
        base: InputVersion,
        prepare: impl FnOnce(&dyn AuthoringBackend, &mut Store) -> Result<Option<(Commit, T)>, RpcFailure>,
    ) -> Option<RpcResult<T>> {
        let handle = &self.server.inner.handle;
        let backend = handle.authoring_backend();
        self.server.with_writer(|store| {
            let mut value = None;
            let mut failed = None;
            let published =
                handle.coordinated_maybe_commit(store, base, |store| match prepare(&*backend, store) {
                    Ok(Some((commit, prepared))) => {
                        value = Some(prepared);
                        Ok(Some(commit))
                    }
                    Ok(None) => Ok(None),
                    // What the backend made durable before it failed (a
                    // memoized failure) still commits; nothing is published.
                    Err(error) => {
                        failed = Some(error);
                        Ok(None)
                    }
                });
            match published {
                Ok(_) if failed.is_some() => Some(RpcResult::Failure(failed.expect("checked"))),
                Ok(Some(_)) => Some(RpcResult::Success(value.expect("a publication has a value"))),
                Ok(None) => None,
                Err(error) => Some(RpcResult::Failure(coordinated_failure(error))),
            }
        })
    }

    /// `force_lossy` writes even when data held under the on-disk schema
    /// would be dropped (see [`RpcFailure::LossyWrite`]). The write
    /// publishes on this connection's own writer: waiting on SQLite's write
    /// lock holds up only this connection.
    pub fn write(
        &self,
        base: InputVersion,
        ops: Vec<AuthoringOp>,
        force_lossy: bool,
    ) -> RpcResult<InputVersion> {
        if let Some(result) = self.authoring_gate(base) {
            return result;
        }
        if ops.is_empty() {
            return RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
                detail: "authoring operation batch must not be empty".to_owned(),
            });
        }
        if ops.iter().any(|operation| {
            matches!(operation, AuthoringOp::Set(entry) if entry.local_id.starts_with('$'))
        }) {
            return RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
                detail: "daemon-owned '$settings' and '$record' entries cannot be written directly"
                    .to_owned(),
            });
        }
        let next = InputVersion(base.0 + 1);
        if is_embedded(&self.server) {
            return self.embedded_write(base, ops, force_lossy, next);
        }
        write_outcome(self.prepared(base, move |backend, store| {
            Ok(backend
                .prepare_write(store, base, &ops, force_lossy)?
                .map(|commit| (commit, next)))
        }))
    }

    fn embedded_write(
        &self,
        base: InputVersion,
        ops: Vec<AuthoringOp>,
        force_lossy: bool,
        next: InputVersion,
    ) -> RpcResult<InputVersion> {
        let backend_ops = ops.clone();
        if let Some(result) = self.prepared(base, move |backend, store| {
            Ok(backend
                .prepare_write(store, base, &backend_ops, force_lossy)?
                .map(|commit| (commit, next)))
        }) {
            return result;
        }
        let commit = rpc_try!(self.embedded_write_commit(ops));
        self.publish(base, commit, next)
    }

    /// The commit an embedded server publishes for a raw authoring batch.
    fn embedded_write_commit(&self, ops: Vec<AuthoringOp>) -> Result<Commit, StoreError> {
        let reader = &self.server.inner.reader;
        let mut touched = BTreeSet::new();
        let mut final_paths = BTreeMap::new();
        let mut affected = BTreeSet::new();
        for op in &ops {
            let uuid = match op {
                AuthoringOp::Set(entry) => entry.uuid,
                AuthoringOp::Remove { uuid } => *uuid,
            };
            if touched.insert(uuid) {
                affected.extend(reader.served_paths_of(uuid)?);
            }
            match op {
                AuthoringOp::Set(entry) => {
                    affected.insert(entry.normalized_path.clone());
                    final_paths.insert(uuid, Some(entry.normalized_path.clone()));
                }
                AuthoringOp::Remove { .. } => {
                    final_paths.insert(uuid, None);
                }
            }
        }
        let mut paths = Vec::new();
        for path in affected {
            let before = reader.served_path_candidates(&path)?;
            let mut after = before
                .iter()
                .filter(|uuid| !touched.contains(uuid))
                .copied()
                .collect::<BTreeSet<_>>();
            after.extend(
                final_paths
                    .iter()
                    .filter(|(_, final_path)| final_path.as_deref() == Some(path.as_str()))
                    .map(|(uuid, _)| *uuid),
            );
            if before == after {
                continue;
            }
            paths.push(if after.is_empty() {
                PathMutation::Remove { path }
            } else {
                PathMutation::Set {
                    path,
                    candidates: after,
                }
            });
        }
        let mut authoring = Vec::with_capacity(ops.len());
        let mut assets = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                AuthoringOp::Set(entry) => {
                    let uuid = entry.uuid;
                    authoring.push(AuthoringMutation::Set(entry));
                    assets.push(AssetMutation::Set {
                        uuid,
                        resolution: StoredResolve::Drifted {
                            input: DriftedInput::Asset(uuid),
                        },
                        delta: AssetDeltaState::Changed,
                    });
                }
                AuthoringOp::Remove { uuid } => {
                    authoring.push(AuthoringMutation::Remove { uuid });
                    assets.push(AssetMutation::Set {
                        uuid,
                        resolution: StoredResolve::Deleted,
                        delta: AssetDeltaState::Deleted,
                    });
                }
            }
        }
        Ok(Commit {
            assets,
            authoring,
            paths,
            ..Commit::default()
        })
    }

    pub fn import(&self, base: InputVersion, request: ImportRequest) -> RpcResult<BundleUuid> {
        match self.import_prepare(base, request) {
            Ok(pending) => self.import_finish(pending.run()),
            Err(result) => result,
        }
    }

    pub fn reimport(&self, base: InputVersion, bundle: BundleUuid) -> RpcResult<BundleUuid> {
        match self.reimport_prepare(base, bundle) {
            Ok(pending) => self.import_finish(pending.run()),
            Err(result) => result,
        }
    }

    /// Check an import request. The returned [`PendingImport`] runs the
    /// importer; transports run it off the RPC thread and hand the result
    /// to [`Hub::import_finish`].
    pub fn import_prepare(
        &self,
        base: InputVersion,
        request: ImportRequest,
    ) -> Result<PendingImport, RpcResult<BundleUuid>> {
        if let Err(detail) = validate_import_request(&request) {
            return Err(RpcResult::Failure(RpcFailure::InvalidAuthoringRequest { detail }));
        }
        if let Some(result) = self.authoring_gate(base) {
            return Err(result);
        }
        Ok(PendingImport {
            base,
            backend: self.server.inner.handle.authoring_backend(),
            work: ImportWork::Import(request),
        })
    }

    /// [`Hub::import_prepare`] for a reimport.
    pub fn reimport_prepare(
        &self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PendingImport, RpcResult<BundleUuid>> {
        if let Some(result) = self.authoring_gate(base) {
            return Err(result);
        }
        Ok(PendingImport {
            base,
            backend: self.server.inner.handle.authoring_backend(),
            work: ImportWork::Reimport(bundle),
        })
    }

    /// Publish a finished import on this connection's writer, only while
    /// still at its base.
    pub fn import_finish(&self, finished: FinishedImport) -> RpcResult<BundleUuid> {
        let FinishedImport {
            base,
            reimport,
            job,
        } = finished;
        let job = match job {
            Ok(job) => job,
            Err(error) => return RpcResult::Failure(error),
        };
        let published = self.prepared(base, move |_, store| {
            let prepared = job(store)?;
            if reimport.is_some_and(|bundle| bundle != prepared.bundle) {
                return Err(RpcFailure::InvalidAuthoringRequest {
                    detail: "reimport backend changed the bundle identity".to_owned(),
                });
            }
            Ok(Some((prepared.commit, prepared.bundle)))
        });
        published.expect("an import always publishes")
    }

    pub fn operation(
        &self,
        base: InputVersion,
        operation: LongRunningOp,
    ) -> RpcResult<ProgressStream> {
        if let Some(result) = self.authoring_gate(base) {
            return result;
        }
        let backend = self.server.inner.handle.authoring_backend();
        let prepared = match self
            .server
            .with_writer(|store| backend.prepare_operation(store, base, &operation))
        {
            Ok(prepared) => prepared,
            Err(error) => return RpcResult::Failure(error),
        };
        if let Err(detail) = validate_progress(&prepared.progress) {
            return RpcResult::Failure(RpcFailure::InvalidAuthoringRequest { detail });
        }
        if let Some(result) = self.authoring_gate(base) {
            return result;
        }
        RpcResult::Success(ProgressStream {
            events: prepared.progress.into(),
            next_sequence: 0,
            terminal_seen: false,
            completion: Rc::new(ServerOperationCompletion {
                server: self.server.clone(),
                connection: self.connection.clone(),
                base,
                publication: RefCell::new(Some(prepared.publication)),
            }),
        })
    }

    pub fn wire_tree(&self, hash: LayoutHash) -> RpcResult<Arc<[u8]>> {
        if let Some(result) = self.live() {
            return result;
        }
        match load_wire_tree(&self.server.inner.reader, hash) {
            Ok(bytes) => RpcResult::Success(bytes),
            Err(error) => RpcResult::Failure(error),
        }
    }

    pub fn fetch(
        &self,
        snapshot: &Snapshot,
        hash: ContentHash,
    ) -> RpcResult<TerminalEvent<ChunkStream>> {
        if let Some(result) = self.live() {
            return result;
        }
        if !Rc::ptr_eq(&snapshot.server.inner, &self.server.inner)
            || !Rc::ptr_eq(&snapshot.connection, &self.connection)
        {
            return RpcResult::Failure(RpcFailure::ForeignSnapshot);
        }
        snapshot.fetch(hash)
    }

    pub fn subscribe(
        &self,
        since: InputVersion,
        assets: Vec<AssetUuid>,
        paths: Vec<String>,
    ) -> RpcResult<SubscriptionInstall> {
        if let Some(result) = self.live() {
            return result;
        }
        if let Some(path) = paths.iter().find(|path| !valid_logical_path(path)) {
            return RpcResult::Failure(RpcFailure::InvalidPath { path: path.clone() });
        }
        let inner = &self.server.inner;
        let (stamp, head, oldest, history, restart) = rpc_try!(inner.read_consistent(|reader| {
            let stamp = reader.stamp();
            let history = if since < stamp.version {
                reader.change_log_history(since, stamp.version)?
            } else {
                Vec::new()
            };
            Ok((
                stamp,
                reader.change_log_head()?,
                reader.change_log_oldest()?,
                history,
                reader.served_blob(SERVED_RESTART_KEYS)?,
            ))
        }));
        let installed = stamp.version;
        if since > installed {
            return RpcResult::Failure(RpcFailure::InvalidCursor {
                since,
                current: installed,
            });
        }
        // Deliver everything up to the read version to the existing
        // subscriptions; the history below covers the new ones.
        inner.pump_until(&self.connection, Some(head));

        let mut connection = self.connection.borrow_mut();
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
        if connection
            .subscribed_assets
            .len()
            .checked_add(new_assets.len())
            .is_none_or(|count| count > MAX_SUBSCRIBED_ASSETS)
        {
            return RpcResult::Failure(RpcFailure::ResourceLimit {
                resource: "subscribed assets".to_owned(),
                limit: MAX_SUBSCRIBED_ASSETS,
            });
        }
        if connection
            .subscribed_paths
            .len()
            .checked_add(new_paths.len())
            .is_none_or(|count| count > MAX_SUBSCRIBED_PATHS)
        {
            return RpcResult::Failure(RpcFailure::ResourceLimit {
                resource: "subscribed paths".to_owned(),
                limit: MAX_SUBSCRIBED_PATHS,
            });
        }
        connection.subscribed_assets.extend(requested_assets);
        connection.subscribed_paths.extend(requested_paths);

        let basis = RpcBasis { snapshot: stamp };
        let first_install = !connection.stream_installed;
        if since < oldest {
            connection.enqueue(StreamEvent::ResyncRequired {
                basis,
                oldest_available: oldest,
            });
        } else {
            let deltas = history_deltas(stamp.instance, &history)
                .iter()
                .filter_map(|delta| delta.filtered(&new_assets, &new_paths))
                .collect::<Vec<_>>();
            if first_install {
                connection.enqueue(StreamEvent::InitialDelta {
                    basis,
                    since,
                    installed,
                    deltas,
                });
            } else {
                for delta in deltas {
                    connection.enqueue(StreamEvent::Delta(delta));
                }
            }
        }
        connection.stream_installed = true;
        let restart = restart
            .and_then(|bytes| distill_store::served::decode_keys(&bytes))
            .unwrap_or_default();
        if first_install && !restart.is_empty() {
            connection.enqueue(StreamEvent::Asset {
                basis: RpcBasis { snapshot: stamp },
                event: AssetEvent::RestartRequired { keys: restart },
            });
        }
        drop(connection);
        RpcResult::Success(SubscriptionInstall {
            deltas: DeltaStream {
                server: self.server.clone(),
                connection: self.connection.clone(),
            },
            installed,
        })
    }

    /// The current watched-import failures. They are memo state, not input:
    /// recording or clearing one publishes no version, so clients poll.
    pub fn import_failures(&self) -> RpcResult<Vec<ImportFailure>> {
        if let Some(result) = self.live() {
            return result;
        }
        match self.server.inner.reader.watched_import_failure_summaries() {
            Ok(rows) => RpcResult::Success(
                rows.into_iter()
                    .map(|row| ImportFailure {
                        bundle: row.bundle,
                        root: row.root,
                        path: row.path,
                        message: row.message,
                    })
                    .collect(),
            ),
            Err(error) => RpcResult::Failure(store_failure(error)),
        }
    }

    pub fn unsubscribe(&self, assets: Vec<AssetUuid>, paths: Vec<String>) -> RpcResult<()> {
        if let Some(result) = self.live() {
            return result;
        }
        if let Some(path) = paths.iter().find(|path| !valid_logical_path(path)) {
            return RpcResult::Failure(RpcFailure::InvalidPath { path: path.clone() });
        }
        let mut connection = self.connection.borrow_mut();
        for uuid in assets {
            connection.subscribed_assets.remove(&uuid);
        }
        for path in paths {
            connection.subscribed_paths.remove(&path);
        }
        RpcResult::Success(())
    }
}

fn snapshot_from(
    server: &Server,
    connection: &Rc<RefCell<ConnectionState>>,
    txn: Rc<SnapshotTxn>,
) -> RpcResult<Snapshot> {
    let basis = RpcBasis {
        snapshot: txn.stamp,
    };
    match server.inner.register_snapshot(txn) {
        Ok(hold) => RpcResult::Success(Snapshot {
            server: server.clone(),
            connection: connection.clone(),
            basis,
            hold,
        }),
        Err(failure) => RpcResult::Failure(failure),
    }
}

fn authoring_snapshot_from(
    server: &Server,
    connection: &Rc<RefCell<ConnectionState>>,
    txn: Rc<SnapshotTxn>,
) -> RpcResult<AuthoringSnapshot> {
    let basis = RpcBasis {
        snapshot: txn.stamp,
    };
    match server.inner.register_snapshot(txn) {
        Ok(hold) => RpcResult::Success(AuthoringSnapshot {
            server: server.clone(),
            connection: connection.clone(),
            basis,
            hold,
        }),
        Err(failure) => RpcResult::Failure(failure),
    }
}

// ---------------------------------------------------------------------------
/// An import the hub accepted. It is `Send`: transports run it off the RPC
/// thread and hand the result back to [`Hub::import_finish`].
pub struct PendingImport {
    base: InputVersion,
    backend: Arc<dyn AuthoringBackend>,
    work: ImportWork,
}

enum ImportWork {
    Import(ImportRequest),
    Reimport(BundleUuid),
}

impl fmt::Debug for PendingImport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingImport")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl PendingImport {
    /// Run the importer.
    pub fn run(self) -> FinishedImport {
        let (reimport, job) = match self.work {
            ImportWork::Import(request) => (None, self.backend.run_import(self.base, request)),
            ImportWork::Reimport(bundle) => {
                (Some(bundle), self.backend.run_reimport(self.base, bundle))
            }
        };
        FinishedImport {
            base: self.base,
            reimport,
            job,
        }
    }
}

/// A [`PendingImport`]'s result: the step that publishes it.
pub struct FinishedImport {
    base: InputVersion,
    reimport: Option<BundleUuid>,
    job: Result<ImportJob, RpcFailure>,
}

// Snapshot

/// A lazy build a resolve waits on: a `Send` future over the backend's
/// ticket. A transport awaits it on the connection's own task, so the
/// connection keeps serving its other calls, and hands the result to
/// [`Snapshot::resolve_finish`]. Dropping it withdraws the resolve's
/// interest in the build.
pub struct PendingBuild {
    asset: AssetUuid,
    ticket: BuildTicket,
    started: Instant,
}

impl fmt::Debug for PendingBuild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingBuild")
            .field("asset", &self.asset)
            .finish_non_exhaustive()
    }
}

impl std::future::Future for PendingBuild {
    type Output = FinishedBuild;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<FinishedBuild> {
        let completion = std::task::ready!(std::pin::Pin::new(&mut self.ticket).poll(cx));
        tracing::debug!(
            asset = %self.asset,
            elapsed = ?self.started.elapsed(),
            "build finished"
        );
        std::task::Poll::Ready(FinishedBuild { completion })
    }
}

/// A [`PendingBuild`]'s result, still to be answered at the snapshot.
pub struct FinishedBuild {
    completion: Box<dyn BuildCompletion>,
}

/// One step of a resolve.
pub enum ResolveStep {
    Done(RpcResult<TerminalEvent<ResolveResult>>),
    Build(PendingBuild),
}

enum VersionResolve {
    Built(ContentHash),
    Drifted(DriftedInput),
    Failed(String),
    Deleted(InputVersion),
}

impl Snapshot {
    pub fn stamp(&self) -> SnapshotStamp {
        self.basis.snapshot
    }

    pub fn generation_reconnect(&self) -> Option<ReconnectReason> {
        self.server.inner.generation_fence(&self.connection.borrow())
    }

    fn preflight<T>(&self) -> Result<Rc<SnapshotTxn>, RpcResult<T>> {
        let connection = self.connection.borrow();
        if let Some(reason) = self.server.inner.generation_fence(&connection) {
            return Err(RpcResult::ReconnectRequired { reason });
        }
        self.hold
            .txn()
            .ok_or(RpcResult::Failure(RpcFailure::SnapshotExpired))
    }

    pub fn version(&self) -> RpcResult<InputVersion> {
        match self.preflight() {
            Ok(_) => RpcResult::Success(self.basis.snapshot.version),
            Err(result) => result,
        }
    }

    pub fn configuration(&self) -> RpcResult<ConfigurationStatus> {
        match self.preflight() {
            Ok(txn) => RpcResult::Success(txn.configuration.clone()),
            Err(result) => result,
        }
    }

    pub fn basis(&self) -> &RpcBasis {
        &self.basis
    }

    pub fn expire(&self) {
        self.hold.expire();
    }

    /// Release this snapshot once its TTL passes (on the RPC `LocalSet`).
    pub(crate) fn expire_later(&self) {
        self.hold.expire_later();
    }

    pub fn refresh(&self) -> RpcResult<Snapshot> {
        {
            let connection = self.connection.borrow();
            if let Some(reason) = self.server.inner.generation_fence(&connection) {
                return RpcResult::ReconnectRequired { reason };
            }
        }
        if !self.hold.alive() {
            return RpcResult::Failure(RpcFailure::SnapshotExpired);
        }
        let txn = rpc_try!(self.server.inner.current_snapshot());
        snapshot_from(&self.server, &self.connection, txn)
    }

    pub fn query(&self, query: AssetQuery) -> RpcResult<Vec<AssetUuid>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if query.terminal_type.is_some() {
            if let Some(error) = pipeline_failure(&self.server.inner.effective_pipeline(&txn)) {
                return RpcResult::Failure(error);
            }
        }
        if let Err(detail) = validate_asset_query(&query, false) {
            return RpcResult::Failure(RpcFailure::InvalidQuery { detail });
        }
        match rpc_try!(query_assets(
            txn.snapshot(),
            &query,
            AuthoringEntryRole::Runtime
        )) {
            Ok(assets) => RpcResult::Success(assets),
            Err(error) => RpcResult::Failure(error),
        }
    }

    pub fn entry(&self, uuid: AssetUuid) -> RpcResult<MetadataEntry> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if let Some(error) = pipeline_failure(&self.server.inner.effective_pipeline(&txn)) {
            return RpcResult::Failure(error);
        }
        match rpc_try!(txn.snapshot().served_entry_meta(uuid)) {
            Some(entry) if !entry.authoring_only => RpcResult::Success(metadata_entry(&entry)),
            _ => RpcResult::Failure(RpcFailure::AssetNotFound { uuid }),
        }
    }

    /// Read the non-hashed runtime policy for one terminal type from the same
    /// target/pipeline epoch as this snapshot. The second fence prevents a
    /// concurrent epoch replacement from publishing a stale policy answer.
    pub fn runtime_type_policy(&self, type_uuid: TypeUuid) -> RpcResult<RuntimeTypePolicy> {
        if let Err(result) = self.preflight::<RuntimeTypePolicy>() {
            return result;
        }
        let target = self.connection.borrow().target.clone();
        let Some(row) = rpc_try!(self.server.inner.reader.rpc_target(&target)) else {
            return RpcResult::ReconnectRequired {
                reason: ReconnectReason::TargetDefinitionChanged,
            };
        };
        let request = RuntimeTypePolicyRequest {
            basis: self.basis.snapshot,
            target,
            target_definition: TargetDefinitionHash(row.definition_hash),
            type_uuid,
        };
        let policy = match self
            .server
            .inner
            .handle
            .build_backend()
            .runtime_type_policy(&request)
        {
            Ok(policy) => policy,
            Err(error) => return RpcResult::Failure(error),
        };
        match self.preflight() {
            Ok(_) => RpcResult::Success(policy),
            Err(result) => result,
        }
    }

    /// Resolve `uuid`, blocking this thread on a build it needs. Transports
    /// await [`Self::resolve_prepare`]'s build instead.
    pub fn resolve(&self, uuid: AssetUuid) -> RpcResult<TerminalEvent<ResolveResult>> {
        match self.resolve_prepare(uuid, BuildWorkClass::Interactive) {
            ResolveStep::Done(result) => result,
            ResolveStep::Build(build) => {
                self.resolve_finish(uuid, futures::executor::block_on(build))
            }
        }
    }

    /// The first half of a resolve: answer it, or name the build it waits
    /// on.
    pub fn resolve_prepare(&self, uuid: AssetUuid, work_class: BuildWorkClass) -> ResolveStep {
        match self.resolve_step(uuid, work_class) {
            Ok(step) => step,
            Err(error) => ResolveStep::Done(RpcResult::Failure(store_failure(error))),
        }
    }

    fn resolve_step(
        &self,
        uuid: AssetUuid,
        work_class: BuildWorkClass,
    ) -> Result<ResolveStep, StoreError> {
        let done = |result| Ok(ResolveStep::Done(result));
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return done(result),
        };
        if let Some(error) = pipeline_failure(&self.server.inner.effective_pipeline(&txn)) {
            return done(RpcResult::Failure(error));
        }
        if let ConfigurationStatus::Failed(error) = &txn.configuration {
            return done(RpcResult::ConfigurationFailed(error.clone()));
        }
        let snapshot = txn.snapshot();
        let derived = snapshot.served_derived_output(uuid)?;
        let authoring_uuid = derived.as_ref().map_or(uuid, |output| output.parent);
        let meta = snapshot.served_entry_meta(authoring_uuid)?;
        if meta.as_ref().is_some_and(|meta| meta.authoring_only) {
            return done(RpcResult::Success(TerminalEvent {
                basis: self.basis.clone(),
                value: ResolveResult::RoleIneligible {
                    observed: AuthoringEntryRole::AuthoringOnly,
                },
            }));
        }
        let target = self.connection.borrow().target.clone();
        let resolution = match &derived {
            Some(output) => Some(VersionResolve::Drifted(DriftedInput::Asset(output.parent))),
            None => match snapshot.asset_resolution(uuid)? {
                None | Some(ResolutionRow::Missing) => None,
                Some(ResolutionRow::Built(hash)) => Some(VersionResolve::Built(hash)),
                Some(ResolutionRow::Drifted(bytes)) => Some(VersionResolve::Drifted(
                    decode_drifted_input(&bytes).map_err(|error| StoreError::Rejected {
                        detail: format!("corrupt drift input: {error}"),
                    })?,
                )),
                Some(ResolutionRow::Failed(error)) => Some(VersionResolve::Failed(error)),
                Some(ResolutionRow::Deleted(at)) => Some(VersionResolve::Deleted(at)),
            },
        };
        // A drifted asset is built on demand. The backend keys the build by
        // its static inputs: a cached result whose traced inputs hold at
        // this snapshot answers at once, and requesters of one key share
        // one build.
        if let (Some(VersionResolve::Drifted(input)), Some(_)) = (&resolution, &meta) {
            let entry = snapshot
                .served_entry(authoring_uuid)?
                .map(authoring_entry)
                .transpose()?
                .expect("a served entry's metadata implies the entry");
            let Some(row) = self.server.inner.reader.rpc_target(&target)? else {
                return done(RpcResult::ReconnectRequired {
                    reason: ReconnectReason::TargetDefinitionChanged,
                });
            };
            let request = BuildRequest {
                work_class,
                target,
                target_definition: TargetDefinitionHash(row.definition_hash),
                requested_asset: uuid,
                output_key: derived
                    .as_ref()
                    .map_or_else(String::new, |output| output.output_key.clone()),
                requested_terminal_type: derived
                    .as_ref()
                    .map_or(entry.terminal_type, |output| output.terminal_type),
                entry,
                drifted_input: input.clone(),
            };
            let view = BuildView {
                snapshot,
                stamp: self.basis.snapshot,
                latest: &self.server.inner.reader,
            };
            let started = Instant::now();
            return match self.server.inner.handle.build_backend().start(view, &request) {
                BuildStart::Answered(answer) => {
                    drop(txn);
                    done(self.build_answer(answer))
                }
                BuildStart::Submitted(ticket) => Ok(ResolveStep::Build(PendingBuild {
                    asset: uuid,
                    ticket,
                    started,
                })),
            };
        }
        let value = match resolution {
            Some(VersionResolve::Built(content_hash)) => {
                if let Err(error) =
                    load_artifact(&self.server.inner.reader, content_hash)
                {
                    return done(RpcResult::Failure(error));
                }
                ResolveResult::Built { content_hash }
            }
            Some(VersionResolve::Drifted(input)) => ResolveResult::Drifted {
                input,
                current: self.server.inner.current_stamp(),
            },
            Some(VersionResolve::Failed(error)) => ResolveResult::Failed { error },
            Some(VersionResolve::Deleted(at)) => ResolveResult::Deleted {
                at: SnapshotStamp {
                    instance: self.basis.snapshot.instance,
                    version: at,
                },
            },
            None => ResolveResult::Missing,
        };
        done(RpcResult::Success(TerminalEvent {
            basis: self.basis.clone(),
            value,
        }))
    }

    /// The second half of a resolve: answer the finished build at this
    /// snapshot. The build may have run at another snapshot; its traced
    /// inputs decide whether its result serves this one.
    pub fn resolve_finish(
        &self,
        _uuid: AssetUuid,
        build: FinishedBuild,
    ) -> RpcResult<TerminalEvent<ResolveResult>> {
        let txn = match self.preflight::<TerminalEvent<ResolveResult>>() {
            Ok(txn) => txn,
            Err(RpcResult::ReconnectRequired { reason }) => {
                return RpcResult::ReconnectRequired { reason };
            }
            Err(_) => return RpcResult::Failure(RpcFailure::SnapshotExpired),
        };
        let answer = build.completion.answer(BuildView {
            snapshot: txn.snapshot(),
            stamp: self.basis.snapshot,
            latest: &self.server.inner.reader,
        });
        drop(txn);
        self.build_answer(answer)
    }

    /// Serve a build answer from this snapshot: a built artifact must be
    /// readable, and the snapshot still live under a working pipeline.
    fn build_answer(
        &self,
        answer: Result<BuildAnswer, RpcFailure>,
    ) -> RpcResult<TerminalEvent<ResolveResult>> {
        let mut outcome = answer;
        if let Ok(BuildAnswer::Built { content_hash }) = &outcome {
            if let Err(error) = load_artifact(&self.server.inner.reader, *content_hash) {
                outcome = Err(error);
            }
        }
        match self.preflight::<()>() {
            Ok(txn) => {
                if let Some(error) = pipeline_failure(&self.server.inner.effective_pipeline(&txn))
                {
                    outcome = Err(error);
                }
            }
            Err(RpcResult::ReconnectRequired { reason }) => {
                return RpcResult::ReconnectRequired { reason };
            }
            Err(_) => outcome = Err(RpcFailure::SnapshotExpired),
        }
        match outcome {
            Ok(answer) => RpcResult::Success(TerminalEvent {
                basis: self.basis.clone(),
                value: match answer {
                    BuildAnswer::Built { content_hash } => ResolveResult::Built { content_hash },
                    BuildAnswer::Failed { error } => ResolveResult::Failed { error },
                    BuildAnswer::Drifted { input } => ResolveResult::Drifted {
                        input,
                        current: self.server.inner.current_stamp(),
                    },
                },
            }),
            Err(error) => RpcResult::Failure(error),
        }
    }

    pub fn resolve_path(&self, path: &str) -> RpcResult<TerminalEvent<PathResolveResult>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if !valid_logical_path(path) {
            return RpcResult::Failure(RpcFailure::InvalidPath {
                path: path.to_owned(),
            });
        }
        RpcResult::Success(TerminalEvent {
            basis: self.basis.clone(),
            value: rpc_try!(resolve_path_in(txn.snapshot(), path)),
        })
    }

    /// The runtime asset named `name` (its local id) among those imported
    /// at `path`. Protocol 12.
    pub fn resolve_named(
        &self,
        path: &str,
        name: &str,
    ) -> RpcResult<TerminalEvent<PathResolveResult>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if !valid_logical_path(path) {
            return RpcResult::Failure(RpcFailure::InvalidPath {
                path: path.to_owned(),
            });
        }
        if !valid_identifier(name) {
            return RpcResult::Failure(RpcFailure::InvalidQuery {
                detail: "asset name is not a canonical local id".to_owned(),
            });
        }
        RpcResult::Success(TerminalEvent {
            basis: self.basis.clone(),
            value: rpc_try!(resolve_named_in(txn.snapshot(), path, name)),
        })
    }

    pub fn fetch(&self, hash: ContentHash) -> RpcResult<TerminalEvent<ChunkStream>> {
        if let Err(result) = self.preflight::<TerminalEvent<ChunkStream>>() {
            return result;
        }
        match load_artifact(&self.server.inner.reader, hash) {
            Ok((payload, _)) => RpcResult::Success(TerminalEvent {
                basis: self.basis.clone(),
                value: chunk_payload(&payload, DEFAULT_CHUNK_SIZE),
            }),
            Err(error) => RpcResult::Failure(error),
        }
    }
}

// ---------------------------------------------------------------------------
// AuthoringSnapshot

impl AuthoringSnapshot {
    pub fn generation_reconnect(&self) -> Option<ReconnectReason> {
        self.server.inner.generation_fence(&self.connection.borrow())
    }

    pub fn stamp(&self) -> SnapshotStamp {
        self.basis.snapshot
    }

    fn preflight<T>(&self) -> Result<Rc<SnapshotTxn>, RpcResult<T>> {
        let connection = self.connection.borrow();
        if let Some(reason) = self.server.inner.generation_fence(&connection) {
            return Err(RpcResult::ReconnectRequired { reason });
        }
        self.hold
            .txn()
            .ok_or(RpcResult::Failure(RpcFailure::SnapshotExpired))
    }

    pub fn version(&self) -> RpcResult<InputVersion> {
        match self.preflight() {
            Ok(_) => RpcResult::Success(self.basis.snapshot.version),
            Err(result) => result,
        }
    }

    pub fn basis(&self) -> &RpcBasis {
        &self.basis
    }

    pub fn expire(&self) {
        self.hold.expire();
    }

    /// Release this snapshot once its TTL passes (on the RPC `LocalSet`).
    pub(crate) fn expire_later(&self) {
        self.hold.expire_later();
    }

    pub fn query(&self, query: AssetQuery) -> RpcResult<Vec<AssetUuid>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if let Err(detail) = validate_asset_query(&query, true) {
            return RpcResult::Failure(RpcFailure::InvalidQuery { detail });
        }
        let role = if query.authoring_only.unwrap_or(false) {
            AuthoringEntryRole::AuthoringOnly
        } else {
            AuthoringEntryRole::Runtime
        };
        match rpc_try!(query_assets(txn.snapshot(), &query, role)) {
            Ok(assets) => RpcResult::Success(assets),
            Err(error) => RpcResult::Failure(error),
        }
    }

    pub fn inspect(&self, uuid: AssetUuid) -> RpcResult<AuthoringInspectResult> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        RpcResult::Success(rpc_try!(inspect_authoring(
            txn.snapshot(),
            self.basis.snapshot,
            uuid
        )))
    }

    pub fn refresh(&self) -> RpcResult<AuthoringSnapshot> {
        {
            let connection = self.connection.borrow();
            if let Some(reason) = self.server.inner.generation_fence(&connection) {
                return RpcResult::ReconnectRequired { reason };
            }
        }
        if !self.hold.alive() {
            return RpcResult::Failure(RpcFailure::SnapshotExpired);
        }
        let txn = rpc_try!(self.server.inner.current_snapshot());
        authoring_snapshot_from(&self.server, &self.connection, txn)
    }
}

// ---------------------------------------------------------------------------
// DeltaStream

impl DeltaStream {
    pub fn next(&self) -> Option<StreamEvent> {
        self.server.inner.pump(&self.connection);
        self.connection.borrow_mut().queue.pop_front()
    }

    /// Wait for the next stream event without blocking the connection's
    /// thread. The publication watch is subscribed before the pump, so a
    /// publication between the two still wakes the wait; the wait itself
    /// costs the publisher nothing but the signal.
    pub async fn next_async(&self) -> Option<StreamEvent> {
        loop {
            let mut published = self.server.inner.subscribe_published();
            self.server.inner.pump(&self.connection);
            let notify = {
                let mut connection = self.connection.borrow_mut();
                if let Some(event) = connection.queue.pop_front() {
                    return Some(event);
                }
                Rc::clone(&connection.notify)
            };
            tokio::select! {
                _ = notify.notified() => {}
                changed = published.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod query_tests {
    //! The indexed queries answer exactly what the whole-table scans they
    //! replaced answered, errors included, and read a small part of a large
    //! namespace.

    use std::collections::BTreeMap;

    use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
    use distill_store::bundles::{AssetRecord, BundleMeta, ServedAuthoring};
    use distill_store::{Store, StoreConfig};

    use super::*;

    const SCHEMA: LogicalHash = LogicalHash([0x5c; 32]);
    const MESH: TypeUuid = TypeUuid([0x71; 16]);
    const TEXTURE: TypeUuid = TypeUuid([0x72; 16]);
    const GPU_MESH: TypeUuid = TypeUuid([0x73; 16]);

    /// Paths that sort between a directory and its children, non-ASCII
    /// ones, and glob metacharacters spelled literally.
    const PATHS: [&str; 12] = [
        "dir",
        "dir.txt",
        "dir-old",
        "dir/child",
        "dir/child/leaf",
        "dir0",
        "dirt/x",
        "é/ü.bundle",
        "éa",
        "a*b",
        "a?b/c",
        "z",
    ];

    fn uuid(kind: u8, index: u32) -> [u8; 16] {
        let mut bytes = [kind; 16];
        bytes[12..].copy_from_slice(&index.to_be_bytes());
        bytes
    }

    /// Two entries per path (a runtime one and an authoring-only one, with
    /// varied types and tags), one poisoned bundle, one pending tag index,
    /// and `filler` more runtime bundles under `bulk/`.
    fn namespace(filler: u32) -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
        store
            .input_transaction(|txn| {
                let root = txn.intern_root("main")?;
                txn.put_schema(SCHEMA, "{}")?;
                let paths = PATHS
                    .iter()
                    .map(|path| (*path).to_owned())
                    .chain((0..filler).map(|index| format!("bulk/b{index:05}")));
                for (index, path) in paths.enumerate() {
                    let index = index as u32;
                    let bundle = BundleUuid(uuid(0x10, index));
                    txn.upsert_bundle(&BundleMeta {
                        bundle,
                        root,
                        path: path.clone(),
                        format_version: 1,
                        content_hash: ContentHash([index as u8; 32]),
                        origin: None,
                    })?;
                    for (entry, authoring_only) in [(1u8, false), (2, true)] {
                        let mut tags = BTreeMap::new();
                        tags.insert(
                            "kind".to_owned(),
                            (index % 3 != 0).then(|| ["mesh", "rock"][index as usize % 2].to_owned()),
                        );
                        if index % 4 == entry as u32 {
                            tags.insert("hero".into(), None);
                        }
                        txn.upsert_asset(&AssetRecord {
                            asset: AssetUuid(uuid(0x20 + entry, index)),
                            bundle,
                            local_id: if entry == 1 { "main" } else { "settings" }.into(),
                            type_uuid: if index % 2 == 0 { MESH } else { TEXTURE },
                            logical_hash: SCHEMA,
                            authoring_only,
                            tags,
                            served: Some(ServedAuthoring {
                                authored_value: Vec::new(),
                                terminal_type: if index % 2 == 0 { GPU_MESH } else { TEXTURE },
                            }),
                        })?;
                    }
                }
                // `z`'s tag index is pending: its tag queries must fail.
                let z = PATHS.iter().position(|path| *path == "z").unwrap() as u32;
                txn.set_tag_index_pending(AssetUuid(uuid(0x21, z)), [7; 32])?;
                // `dir0` is poisoned: it serves nothing.
                let dir0 = PATHS.iter().position(|path| *path == "dir0").unwrap() as u32;
                txn.poison_bundle(
                    &distill_store::bundles::NamespaceSkeleton {
                        bundle: BundleUuid(uuid(0x10, dir0)),
                        root,
                        path: "dir0".into(),
                        format_version: 1,
                        content_hash: ContentHash([0xee; 32]),
                        entries: Vec::new(),
                    },
                    "malformed",
                )
            })
            .unwrap();
        (dir, store)
    }

    fn selectors() -> Vec<AssetQuery> {
        let mut queries = Vec::new();
        let none = AssetQuery {
            uuid: None,
            bundle_path: None,
            local_id: None,
            bundle_uuid: None,
            authored_type: None,
            terminal_type: None,
            tag: None,
            path_prefix: None,
            path_glob: None,
            authoring_only: None,
        };
        for index in [0, 3, 7, 11] {
            queries.push(AssetQuery {
                uuid: Some(AssetUuid(uuid(0x21, index))),
                ..none.clone()
            });
            queries.push(AssetQuery {
                bundle_uuid: Some(BundleUuid(uuid(0x10, index))),
                local_id: Some("main".into()),
                ..none.clone()
            });
        }
        for path in PATHS {
            queries.push(AssetQuery {
                bundle_path: Some(path.into()),
                ..none.clone()
            });
            queries.push(AssetQuery {
                bundle_path: Some(path.into()),
                local_id: Some("settings".into()),
                ..none.clone()
            });
            queries.push(AssetQuery {
                path_prefix: Some(path.into()),
                ..none.clone()
            });
        }
        for prefix in ["", "d", "dir/", "é", "a", "bulk/b0001"] {
            queries.push(AssetQuery {
                path_prefix: Some(prefix.into()),
                ..none.clone()
            });
        }
        for glob in [
            "*", "dir*", "dir/*", "*child*", "d?r*", "é/*", "a*b", "a?b/*", "bulk/b000?", "?",
            "dir", "*.txt",
        ] {
            queries.push(AssetQuery {
                path_glob: Some(glob.into()),
                ..none.clone()
            });
            queries.push(AssetQuery {
                path_glob: Some(glob.into()),
                path_prefix: Some("dir".into()),
                ..none.clone()
            });
        }
        for type_uuid in [MESH, TEXTURE, GPU_MESH] {
            queries.push(AssetQuery {
                authored_type: Some(type_uuid),
                ..none.clone()
            });
            queries.push(AssetQuery {
                terminal_type: Some(type_uuid),
                ..none.clone()
            });
        }
        for (tag, value) in [
            ("kind", None),
            ("kind", Some("mesh")),
            ("kind", Some("rock")),
            ("hero", None),
            ("hero", Some("x")),
            ("absent", None),
        ] {
            let tag = Some(TagSelector {
                tag: tag.into(),
                value: value.map(str::to_owned),
            });
            queries.push(AssetQuery {
                tag: tag.clone(),
                ..none.clone()
            });
            // Narrowed away from `z`'s pending index, and not.
            queries.push(AssetQuery {
                tag: tag.clone(),
                path_prefix: Some("dir".into()),
                ..none.clone()
            });
            queries.push(AssetQuery {
                tag,
                path_glob: Some("z*".into()),
                ..none.clone()
            });
        }
        for authoring_only in [false, true] {
            queries.push(AssetQuery {
                authoring_only: Some(authoring_only),
                ..none.clone()
            });
        }
        queries
    }

    #[test]
    fn asset_queries_answer_what_the_scan_answered() {
        let (_dir, store) = namespace(30);
        let reader = store.reader().unwrap();
        let mut nonempty = 0;
        for query in selectors() {
            for role in [AuthoringEntryRole::Runtime, AuthoringEntryRole::AuthoringOnly] {
                let indexed = query_assets(&reader, &query, role).unwrap();
                let scanned = query_assets_scan(&reader, &query, role).unwrap();
                assert_eq!(
                    format!("{indexed:?}"),
                    format!("{scanned:?}"),
                    "{query:?} as {role:?}"
                );
                nonempty += usize::from(indexed.is_ok_and(|assets| !assets.is_empty()));
            }
        }
        // The selectors exercise matches, not only empty answers.
        assert!(nonempty > 60, "{nonempty}");
    }

    #[test]
    fn pure_metadata_queries_answer_what_the_scan_answered() {
        let (_dir, store) = namespace(30);
        let reader = store.reader().unwrap();
        let entries = reader.served_entries().unwrap();
        let roles = [None, Some(AuthoringEntryRole::Runtime), Some(AuthoringEntryRole::AuthoringOnly)];
        let prefixes = [None, Some(""), Some("dir"), Some("dir/"), Some("é"), Some("bulk/b001")];
        for role in roles {
            for prefix in prefixes {
                for (uuid, bundle, authored_type) in [
                    (None, None, None),
                    (Some(AssetUuid(uuid(0x22, 3))), None, None),
                    (None, Some(BundleUuid(uuid(0x10, 4))), None),
                    (None, None, Some(TEXTURE)),
                ] {
                    let query = PureMetadataQuery {
                        uuid,
                        bundle,
                        normalized_path_prefix: prefix.map(str::to_owned),
                        authored_type,
                        role,
                    };
                    assert_eq!(
                        query_pure_metadata(&reader, &query).unwrap(),
                        query_pure_metadata_scan(&entries, &query),
                        "{query:?}"
                    );
                }
            }
        }
    }

    /// The runtime entries build verification reads in one query are the
    /// per-entry reads it replaced, each failing where that read failed.
    #[test]
    fn runtime_entries_are_the_per_entry_reads() {
        let (_dir, mut store) = namespace(30);
        // An entry whose schema snapshot is missing fails in its place.
        let unknown = LogicalHash([0x99; 32]);
        store
            .input_transaction(|txn| {
                txn.upsert_asset(&AssetRecord {
                    asset: AssetUuid(uuid(0x21, 4)),
                    bundle: BundleUuid(uuid(0x10, 4)),
                    local_id: "main".into(),
                    type_uuid: MESH,
                    logical_hash: unknown,
                    authoring_only: false,
                    tags: BTreeMap::from([("kind".to_owned(), None)]),
                    served: Some(ServedAuthoring {
                        authored_value: vec![1, 2, 3],
                        terminal_type: GPU_MESH,
                    }),
                })
            })
            .unwrap();
        let reader = store.reader().unwrap();
        let per_entry = reader
            .served_entries()
            .unwrap()
            .into_iter()
            .filter(|meta| !meta.authoring_only)
            .map(|meta| {
                reader
                    .served_entry(meta.asset)
                    .map(|entry| entry.expect("a served entry reads"))
                    .map_err(|error| error.to_string())
            })
            .collect::<Vec<_>>();
        let batched = reader
            .served_runtime_entries()
            .unwrap()
            .into_iter()
            .map(|entry| entry.map_err(|error| error.to_string()))
            .collect::<Vec<_>>();
        assert_eq!(batched, per_entry);
        assert!(batched.iter().filter(|entry| entry.is_err()).count() == 1);
        assert!(batched.len() > 30);
    }

    /// Pages fetched by `read`.
    fn pages<T>(reader: &StoreReader, read: impl FnOnce() -> T) -> (T, u64) {
        let before = reader.pages_fetched().unwrap();
        let value = read();
        (value, reader.pages_fetched().unwrap() - before)
    }

    #[test]
    fn a_narrow_asset_query_reads_little_of_a_large_namespace() {
        let (_dir, store) = namespace(20_000);
        let reader = store.reader().unwrap();
        let narrow = [
            AssetQuery {
                bundle_path: Some("bulk/b12345".into()),
                uuid: None,
                local_id: None,
                bundle_uuid: None,
                authored_type: None,
                terminal_type: None,
                tag: None,
                path_prefix: None,
                path_glob: None,
                authoring_only: None,
            },
            AssetQuery {
                bundle_path: None,
                path_glob: Some("bulk/b1234?".into()),
                tag: Some(TagSelector {
                    tag: "hero".into(),
                    value: None,
                }),
                uuid: None,
                local_id: None,
                bundle_uuid: None,
                authored_type: None,
                terminal_type: None,
                path_prefix: None,
                authoring_only: None,
            },
        ];
        for query in narrow {
            let role = AuthoringEntryRole::Runtime;
            let (indexed, indexed_pages) = pages(&reader, || query_assets(&reader, &query, role));
            let (scanned, scanned_pages) =
                pages(&reader, || query_assets_scan(&reader, &query, role));
            let indexed = indexed.unwrap().unwrap();
            assert_eq!(indexed, scanned.unwrap().unwrap());
            assert!(!indexed.is_empty());
            println!("{query:?}: {indexed_pages} pages (scan: {scanned_pages})");
            assert!(indexed_pages <= 128, "{indexed_pages} pages");
            assert!(scanned_pages >= 100 * indexed_pages, "{scanned_pages} pages");
        }
        let query = PureMetadataQuery {
            uuid: None,
            bundle: None,
            normalized_path_prefix: Some("bulk/b1234".into()),
            authored_type: None,
            role: Some(AuthoringEntryRole::Runtime),
        };
        let (indexed, indexed_pages) = pages(&reader, || query_pure_metadata(&reader, &query));
        let (scanned, scanned_pages) = pages(&reader, || {
            query_pure_metadata_scan(&reader.served_entries().unwrap(), &query)
        });
        assert_eq!(indexed.unwrap(), scanned);
        println!("pure metadata prefix: {indexed_pages} pages (scan: {scanned_pages})");
        assert!(indexed_pages <= 64, "{indexed_pages} pages");
        assert!(scanned_pages >= 100 * indexed_pages, "{scanned_pages} pages");
    }
}
