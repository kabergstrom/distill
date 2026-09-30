//! Client capabilities over one front end ([`Server`]): target-bound hubs
//! and snapshots, metadata bootstraps, delta streams and progress
//! completions. Every call reads the store through the capability's own
//! read transaction (snapshots) or the front end's current-state reader
//! (fences, CAS). A snapshot holds nothing else: an artifact removed from
//! the CAS after it resolved is a cache miss for the client to retry.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use unicode_normalization::UnicodeNormalization;

use distill_store::served::{ResolutionRow, ServedEntryMeta, SERVED_RESTART_KEYS};
use distill_store::{StoreError, StoreReader};

use crate::persist::decode_drifted_input;
use crate::server::{
    authoring_entry, entry_role, history_deltas, is_embedded, pipeline_failure,
    publish_backend_commit, store_failure, BuildKey, BuildResolution, ConnectionState,
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

fn query_pure_metadata(entries: &[ServedEntryMeta], query: &PureMetadataQuery) -> Vec<AssetUuid> {
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
/// would consult a poisoned tag index.
fn query_assets(
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
    if !connection.alive() {
        return Some(AuthoringGate::Failure(RpcFailure::ConnectionClosed));
    }
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

/// What a [`Hub::write_call`] answered.
pub fn write_call_outcome(outcome: Option<RpcResult<InputVersion>>) -> RpcResult<InputVersion> {
    outcome.unwrap_or_else(|| {
        RpcResult::Failure(RpcFailure::AuthoringBackendUnavailable {
            operation: "write".to_owned(),
        })
    })
}

/// What a [`Hub::import_publish_call`] answered.
pub fn import_call_outcome(outcome: Option<RpcResult<BundleUuid>>) -> RpcResult<BundleUuid> {
    outcome.expect("an import always publishes")
}

/// Complete a prepared operation as one input, still at `base`.
fn complete_publication_call(
    server: &Server,
    base: InputVersion,
    publication: PreparedOperationPublication,
    what: &'static str,
) -> WriteCall<Result<(), String>> {
    server.write_call(move |server| {
        let mut failed = None;
        let mut terminal = None;
        let published = server.coordinated_maybe_commit(base, || {
            let (commit, terminal_error) = match publication {
                PreparedOperationPublication::Immediate(commit) => (*commit, None),
                PreparedOperationPublication::Deferred(operation) => {
                    match operation.complete(base) {
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
    })
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
    fn complete_call(&self) -> Result<WriteCall<Result<(), String>>, String> {
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
        Ok(complete_publication_call(
            &self.server,
            self.base,
            publication,
            "long-running operation",
        ))
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
        Ok((basis, self.server.inner.register_snapshot(txn)))
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
        let entries = namespace_try!(txn.snapshot().served_entries());
        MetadataNamespaceCall::Success(query_pure_metadata(&entries, query))
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
        Ok((basis, self.server.inner.register_snapshot(txn)))
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
        self.server.inner.pump();
        let connection = self.connection.borrow();
        if !connection.alive() {
            return Some(RpcResult::Failure(RpcFailure::ConnectionClosed));
        }
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
        RpcResult::Success(snapshot_from(&self.server, &self.connection, txn))
    }

    /// Pin a tooling-only view. It shares the same immutable store stamp and
    /// complete connection-generation fence as the runtime snapshot, but has
    /// no resolve, fetch, dependency, or pack capability.
    pub fn authoring_snapshot(&self) -> RpcResult<AuthoringSnapshot> {
        if let Some(result) = self.live() {
            return result;
        }
        let txn = rpc_try!(self.server.inner.current_snapshot());
        RpcResult::Success(authoring_snapshot_from(&self.server, &self.connection, txn))
    }

    fn authoring_gate<T>(&self, base: InputVersion) -> Option<RpcResult<T>> {
        self.server.inner.pump();
        authoring_gate(&self.server, &self.connection.borrow(), base)
            .map(AuthoringGate::into_result)
    }

    fn publish<T>(&self, base: InputVersion, commit: Commit, value: T) -> RpcResult<T> {
        match publish_backend_commit(&self.server, base, commit) {
            Ok(_) => RpcResult::Success(value),
            Err(error) => RpcResult::Failure(error),
        }
    }

    /// Run the backend's `prepare` and publish its commit as one input,
    /// still at `base`. `None` when the backend declined.
    fn prepared<T: Send + 'static>(
        &self,
        base: InputVersion,
        prepare: impl FnOnce(&dyn AuthoringBackend) -> Result<Option<(Commit, T)>, RpcFailure>
            + Send
            + 'static,
    ) -> Option<RpcResult<T>> {
        self.prepared_call(base, prepare).run()
    }

    /// [`Hub::prepared`] as a job for a blocking thread.
    fn prepared_call<T: Send + 'static>(
        &self,
        base: InputVersion,
        prepare: impl FnOnce(&dyn AuthoringBackend) -> Result<Option<(Commit, T)>, RpcFailure>
            + Send
            + 'static,
    ) -> WriteCall<Option<RpcResult<T>>> {
        let backend = self.server.inner.handle.authoring_backend();
        self.server.write_call(move |server| {
            let mut value = None;
            let mut failed = None;
            let published = server.coordinated_maybe_commit(base, || match prepare(&*backend) {
                Ok(Some((commit, prepared))) => {
                    value = Some(prepared);
                    Ok(Some(commit))
                }
                Ok(None) => Ok(None),
                // What the backend made durable before it failed (a memoized
                // failure) still commits; nothing is published.
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
    /// would be dropped (see [`RpcFailure::LossyWrite`]).
    pub fn write(
        &self,
        base: InputVersion,
        ops: Vec<AuthoringOp>,
        force_lossy: bool,
    ) -> RpcResult<InputVersion> {
        match self.write_call(base, ops, force_lossy) {
            Ok(call) => write_call_outcome(call.run()),
            Err(result) => result,
        }
    }

    /// Check a write. Against a daemon, the returned job publishes it
    /// (`None`: the backend declined); transports run it off the RPC
    /// thread. An embedded server answers here.
    #[allow(clippy::type_complexity)]
    pub fn write_call(
        &self,
        base: InputVersion,
        ops: Vec<AuthoringOp>,
        force_lossy: bool,
    ) -> Result<WriteCall<Option<RpcResult<InputVersion>>>, RpcResult<InputVersion>> {
        if let Some(result) = self.authoring_gate(base) {
            return Err(result);
        }
        if ops.is_empty() {
            return Err(RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
                detail: "authoring operation batch must not be empty".to_owned(),
            }));
        }
        if ops.iter().any(|operation| {
            matches!(operation, AuthoringOp::Set(entry) if entry.local_id.starts_with('$'))
        }) {
            return Err(RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
                detail: "daemon-owned '$settings' and '$record' entries cannot be written directly"
                    .to_owned(),
            }));
        }
        let next = InputVersion(base.0 + 1);
        if is_embedded(&self.server) {
            return Err(self.embedded_write(base, ops, force_lossy, next));
        }
        Ok(self.prepared_call(base, move |backend| {
            Ok(backend
                .prepare_write(base, &ops, force_lossy)?
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
        if let Some(result) = self.prepared(base, move |backend| {
            Ok(backend
                .prepare_write(base, &backend_ops, force_lossy)?
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

    /// Publish a finished import, only while still at its base.
    pub fn import_finish(&self, finished: FinishedImport) -> RpcResult<BundleUuid> {
        match self.import_publish_call(finished) {
            Ok(call) => import_call_outcome(call.run()),
            Err(result) => result,
        }
    }

    /// [`Hub::import_finish`] as a job for a blocking thread.
    #[allow(clippy::type_complexity)]
    pub fn import_publish_call(
        &self,
        finished: FinishedImport,
    ) -> Result<WriteCall<Option<RpcResult<BundleUuid>>>, RpcResult<BundleUuid>> {
        let FinishedImport {
            base,
            reimport,
            job,
        } = finished;
        let job = match job {
            Ok(job) => job,
            Err(error) => return Err(RpcResult::Failure(error)),
        };
        Ok(self.prepared_call(base, move |_| {
            let prepared = job()?;
            if reimport.is_some_and(|bundle| bundle != prepared.bundle) {
                return Err(RpcFailure::InvalidAuthoringRequest {
                    detail: "reimport backend changed the bundle identity".to_owned(),
                });
            }
            Ok(Some((prepared.commit, prepared.bundle)))
        }))
    }

    pub fn operation(
        &self,
        base: InputVersion,
        operation: LongRunningOp,
    ) -> RpcResult<ProgressStream> {
        if let Some(result) = self.authoring_gate(base) {
            return result;
        }
        let prepared = match self
            .server
            .inner
            .handle
            .authoring_backend()
            .prepare_operation(base, &operation)
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
        inner.pump_until(Some(head));

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
) -> Snapshot {
    Snapshot {
        server: server.clone(),
        connection: connection.clone(),
        basis: RpcBasis {
            snapshot: txn.stamp,
        },
        hold: server.inner.register_snapshot(txn),
    }
}

fn authoring_snapshot_from(
    server: &Server,
    connection: &Rc<RefCell<ConnectionState>>,
    txn: Rc<SnapshotTxn>,
) -> AuthoringSnapshot {
    AuthoringSnapshot {
        server: server.clone(),
        connection: connection.clone(),
        basis: RpcBasis {
            snapshot: txn.stamp,
        },
        hold: server.inner.register_snapshot(txn),
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

/// A lazy build the server needs before it can answer a resolve. It is
/// `Send`: transports run it off the RPC thread and hand the result back to
/// [`Snapshot::resolve_finish`].
pub struct PendingBuild {
    request: BuildRequest,
    backend: Arc<dyn BuildBackend>,
}

impl fmt::Debug for PendingBuild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingBuild")
            .field("asset", &self.request.requested_asset)
            .finish_non_exhaustive()
    }
}

impl PendingBuild {
    /// Run the build backend.
    pub fn run(self) -> FinishedBuild {
        let started = Instant::now();
        let outcome = self.backend.build(&self.request);
        tracing::debug!(
            asset = %self.request.requested_asset,
            target = %self.request.target,
            elapsed = ?started.elapsed(),
            built = matches!(outcome, Ok(BuildBackendOutcome::Built(_))),
            "build finished"
        );
        FinishedBuild {
            request: self.request,
            backend: self.backend,
            outcome,
        }
    }
}

/// A [`PendingBuild`]'s result.
pub struct FinishedBuild {
    request: BuildRequest,
    backend: Arc<dyn BuildBackend>,
    outcome: Result<BuildBackendOutcome, RpcFailure>,
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
        if !connection.alive() {
            return Err(RpcResult::Failure(RpcFailure::ConnectionClosed));
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
            if !connection.alive() {
                return RpcResult::Failure(RpcFailure::ConnectionClosed);
            }
            if let Some(reason) = self.server.inner.generation_fence(&connection) {
                return RpcResult::ReconnectRequired { reason };
            }
        }
        if !self.hold.alive() {
            return RpcResult::Failure(RpcFailure::SnapshotExpired);
        }
        let txn = rpc_try!(self.server.inner.current_snapshot());
        RpcResult::Success(snapshot_from(&self.server, &self.connection, txn))
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

    pub fn resolve(&self, uuid: AssetUuid) -> RpcResult<TerminalEvent<ResolveResult>> {
        self.resolve_with_work_class(uuid, BuildWorkClass::Interactive)
    }

    /// Resolve work initiated by an offline pack/doctor traversal. The result
    /// is identical to [`Self::resolve`]; only scheduler admission differs.
    pub fn resolve_batch(&self, uuid: AssetUuid) -> RpcResult<TerminalEvent<ResolveResult>> {
        self.resolve_with_work_class(uuid, BuildWorkClass::Batch)
    }

    fn resolve_with_work_class(
        &self,
        uuid: AssetUuid,
        work_class: BuildWorkClass,
    ) -> RpcResult<TerminalEvent<ResolveResult>> {
        loop {
            match self.resolve_prepare(uuid, work_class) {
                ResolveStep::Done(result) => return result,
                ResolveStep::Build(build) => {
                    if let Some(result) = self.resolve_finish(uuid, build.run()) {
                        return result;
                    }
                }
            }
        }
    }

    /// The first half of a resolve: answer it, or name the build it needs.
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
        let key = BuildKey {
            basis: self.basis.snapshot,
            target: target.clone(),
            asset: uuid,
        };
        let built = self.server.inner.build_results.borrow().get(&key).cloned();
        let resolution = match built {
            Some(BuildResolution::Built(hash)) => Some(VersionResolve::Built(hash)),
            Some(BuildResolution::Failed(error)) => Some(VersionResolve::Failed(error)),
            Some(BuildResolution::Drifted(input)) => Some(VersionResolve::Drifted(input)),
            None => {
                let resolution = match &derived {
                    Some(output) => Some(VersionResolve::Drifted(DriftedInput::Asset(
                        output.parent,
                    ))),
                    None => match snapshot.asset_resolution(uuid)? {
                        None | Some(ResolutionRow::Missing) => None,
                        Some(ResolutionRow::Built(hash)) => Some(VersionResolve::Built(hash)),
                        Some(ResolutionRow::Drifted(bytes)) => {
                            Some(VersionResolve::Drifted(decode_drifted_input(&bytes).map_err(
                                |error| StoreError::Rejected {
                                    detail: format!("corrupt drift input: {error}"),
                                },
                            )?))
                        }
                        Some(ResolutionRow::Failed(error)) => Some(VersionResolve::Failed(error)),
                        Some(ResolutionRow::Deleted(at)) => Some(VersionResolve::Deleted(at)),
                    },
                };
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
                        basis: self.basis.snapshot,
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
                    return Ok(ResolveStep::Build(PendingBuild {
                        request,
                        backend: self.server.inner.handle.build_backend(),
                    }));
                }
                resolution
            }
        };
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

    /// The second half of a resolve: publish the build's result. `None`
    /// means the caller prepares again (the result is cached for it).
    pub fn resolve_finish(
        &self,
        uuid: AssetUuid,
        build: FinishedBuild,
    ) -> Option<RpcResult<TerminalEvent<ResolveResult>>> {
        let FinishedBuild {
            request,
            backend,
            outcome,
        } = build;
        let mut outcome = outcome.and_then(|outcome| match outcome {
            BuildBackendOutcome::Built(publication) => self
                .server
                .install_build_publication(uuid, publication)
                .map(BuildResolution::Built),
            BuildBackendOutcome::Failed { error } => Ok(BuildResolution::Failed(error)),
            BuildBackendOutcome::Drifted { input } => Ok(BuildResolution::Drifted(input)),
        });
        if let Ok(BuildResolution::Built(content_hash)) = &outcome {
            if let Err(error) =
                load_artifact(&self.server.inner.reader, *content_hash)
            {
                outcome = Err(error);
            }
        }
        if let Err(error) = backend.build_finished(&request) {
            outcome = Err(error);
        }
        match self.preflight::<()>() {
            Ok(txn) => {
                if let Some(error) = pipeline_failure(&self.server.inner.effective_pipeline(&txn))
                {
                    outcome = Err(error);
                }
            }
            Err(RpcResult::ReconnectRequired { reason }) => {
                return Some(RpcResult::ReconnectRequired { reason });
            }
            Err(_) => outcome = Err(RpcFailure::SnapshotExpired),
        }
        match outcome {
            Ok(resolution) => {
                let key = BuildKey {
                    basis: self.basis.snapshot,
                    target: request.target,
                    asset: uuid,
                };
                if key.basis == self.server.inner.current_stamp() {
                    self.server.inner.cache_build(key, resolution);
                    None
                } else {
                    // The snapshot is no longer current: answer from the
                    // outcome directly rather than loop on an uncacheable
                    // build.
                    Some(RpcResult::Success(TerminalEvent {
                        basis: self.basis.clone(),
                        value: match resolution {
                            BuildResolution::Built(content_hash) => {
                                ResolveResult::Built { content_hash }
                            }
                            BuildResolution::Failed(error) => ResolveResult::Failed { error },
                            BuildResolution::Drifted(input) => ResolveResult::Drifted {
                                input,
                                current: self.server.inner.current_stamp(),
                            },
                        },
                    }))
                }
            }
            Err(error) => Some(RpcResult::Failure(error)),
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
        if !connection.alive() {
            return Err(RpcResult::Failure(RpcFailure::ConnectionClosed));
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
            if !connection.alive() {
                return RpcResult::Failure(RpcFailure::ConnectionClosed);
            }
            if let Some(reason) = self.server.inner.generation_fence(&connection) {
                return RpcResult::ReconnectRequired { reason };
            }
        }
        if !self.hold.alive() {
            return RpcResult::Failure(RpcFailure::SnapshotExpired);
        }
        let txn = rpc_try!(self.server.inner.current_snapshot());
        RpcResult::Success(authoring_snapshot_from(&self.server, &self.connection, txn))
    }
}

// ---------------------------------------------------------------------------
// DeltaStream

impl DeltaStream {
    pub fn next(&self) -> Option<StreamEvent> {
        self.server.inner.pump();
        let mut connection = self.connection.borrow_mut();
        if !connection.alive() {
            connection.expire();
            return None;
        }
        connection.queue.pop_front()
    }

    /// Wait for the next stream event without blocking the single-threaded
    /// Cap'n Proto `RpcSystem`. The publication watch is subscribed before
    /// the pump, so a publication between the two still wakes the wait.
    pub async fn next_async(&self) -> Option<StreamEvent> {
        loop {
            let mut published = self.server.inner.subscribe_published();
            self.server.inner.pump();
            let notify = {
                let mut connection = self.connection.borrow_mut();
                if !connection.alive() {
                    connection.expire();
                    return None;
                }
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
