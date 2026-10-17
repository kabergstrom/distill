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

use distill_schema::ngp_schema::prefab_types;
use distill_store::bundles::{AssetAnswer, AssetFilter};
use distill_store::files::GlobKeys;
use distill_store::served::{AssetResolution, ServedEntryMeta};
use distill_store::{Store, StoreError, StoreReader};

use crate::server::{
    entry_role, has_file_changes, history_deltas, pipeline_failure, store_failure, types_root,
    ConnectionState, HistoryDelta, MetadataBinding, SnapshotHold, SnapshotTxn, DEFAULT_CHUNK_SIZE,
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
    basis: MetadataBasis,
    hold: Rc<SnapshotHold>,
}

#[derive(Clone)]
pub struct MetadataAuthoringSnapshot {
    server: Server,
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

fn metadata_basis(snapshot: SnapshotStamp) -> MetadataBasis {
    MetadataBasis {
        snapshot,
        protocol_epoch: PROTOCOL_VERSION,
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

/// A metadata call's read that fails as an [`RpcFailure`]: an expired
/// snapshot is the call's own retryable result.
macro_rules! metadata_rpc_try {
    ($expr:expr) => {
        match $expr {
            Ok(value) => value,
            Err(RpcFailure::SnapshotExpired) => return MetadataCall::SnapshotExpired,
            Err(error) => return MetadataCall::Error(error),
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
) -> Result<Result<Vec<AssetUuid>, RpcFailure>, StoreError> {
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
    Ok(assets(snapshot.served_assets_matching(&filter, |_| true)?))
}

/// The whole-table scan [`query_pure_metadata`] replaced, kept to pin its
/// results.
#[cfg(test)]
fn query_pure_metadata_scan(
    entries: &[ServedEntryMeta],
    query: &PureMetadataQuery,
) -> Vec<AssetUuid> {
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
/// contributes only its keys (its literal prefix, and the final segment its
/// literal tail names; its only metacharacters are `*` and `?`) and is
/// matched on the rows.
fn asset_filter(query: &AssetQuery, role: AuthoringEntryRole) -> AssetFilter {
    let filter = AssetFilter {
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
        path_prefixes: query.path_prefix.iter().cloned().collect(),
        authoring_only: Some(role == AuthoringEntryRole::AuthoringOnly),
        ..AssetFilter::default()
    };
    match &query.path_glob {
        Some(pattern) => filter.with_glob_keys(GlobKeys::of(pattern, &['*', '?'])),
        None => filter,
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

/// Run an asset query over a pinned snapshot as one asset query
/// (DESIGN.md §13, asset queries): it fails naming the poisoned bundles it
/// reaches, through a poisoned bundle's skeleton or a poisoned tag index.
/// Only the glob is matched here.
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
    Ok(assets(snapshot.served_assets_matching(
        &asset_filter(query, role),
        glob_matches,
    )?))
}

/// The assets an asset query answered, or the failure naming the poisoned
/// bundles it reached.
fn assets(answer: AssetAnswer) -> Result<Vec<AssetUuid>, RpcFailure> {
    match answer {
        Ok(matched) => Ok(matched.into_iter().map(|matched| matched.asset).collect()),
        Err(bundles) => Err(RpcFailure::TagIndexPoisoned { bundles }),
    }
}

/// The whole-table scan [`query_assets`] replaced, kept to pin its results
/// over namespaces whose poisoned bundles have no skeleton entries.
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
            .collect::<std::collections::BTreeMap<_, _>>();
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

/// A served entry's bundle file, read once, verified against the hash the
/// snapshot published and parsed.
struct ReadBundle {
    parsed: distill_bundle::Bundle,
    root: String,
    origin: BundleOrigin,
}

/// What reading an entry's bundle found: the bundle, or the answer that
/// stands in for the entry (missing, runtime-only, drifted).
enum EntryBundle {
    Read(ServedEntryMeta, Rc<ReadBundle>),
    Answer(AuthoringInspectResult),
}

/// The entries' bundles a call has read, by bundle: each is read and
/// parsed once however many of its entries the call names.
struct BundleReads<'s> {
    server: &'s Server,
    snapshot: &'s StoreReader,
    read: BTreeMap<BundleUuid, Result<Rc<ReadBundle>, DriftedInput>>,
}

impl<'s> BundleReads<'s> {
    fn new(server: &'s Server, snapshot: &'s StoreReader) -> Self {
        Self { server, snapshot, read: BTreeMap::new() }
    }

    /// `uuid`'s served metadata and its bundle.
    fn entry(&mut self, uuid: AssetUuid) -> Result<EntryBundle, RpcFailure> {
        let Some(meta) = self.snapshot.served_entry_meta(uuid).map_err(store_failure)? else {
            // A served derived output is a runtime asset with no authored
            // entry; any other UUID without a served entry is missing here.
            let answer = if self.snapshot.derived_output(uuid).map_err(store_failure)?.is_some() {
                AuthoringInspectResult::RoleIneligible { observed: AuthoringEntryRole::Runtime }
            } else {
                AuthoringInspectResult::Missing
            };
            return Ok(EntryBundle::Answer(answer));
        };
        let read = match self.read.get(&meta.bundle) {
            Some(read) => read.clone(),
            None => {
                let read = self.read_bundle(uuid, meta.bundle)?;
                self.read.insert(meta.bundle, read.clone());
                read
            }
        };
        Ok(match read {
            Ok(read) => EntryBundle::Read(meta, read),
            Err(input) => EntryBundle::Answer(AuthoringInspectResult::Drifted {
                input,
                current: self.server.inner.current_stamp().map_err(store_failure)?,
            }),
        })
    }

    fn read_bundle(
        &self,
        uuid: AssetUuid,
        bundle_uuid: BundleUuid,
    ) -> Result<Result<Rc<ReadBundle>, DriftedInput>, RpcFailure> {
        let unpublished = || RpcFailure::InvalidQuery {
            detail: format!("served entry {uuid} has no published bundle"),
        };
        let bundle = self
            .snapshot
            .bundle(bundle_uuid)
            .map_err(store_failure)?
            .ok_or_else(unpublished)?;
        let root = self
            .snapshot
            .root_name(bundle.root)
            .map_err(store_failure)?
            .ok_or_else(unpublished)?;
        let read = self
            .server
            .inner
            .handle
            .authoring_backend()
            .read_file(self.snapshot, &root, &bundle.path);
        let bytes = match read {
            Ok(bytes) if ContentHash(*blake3::hash(&bytes).as_bytes()) == bundle.content_hash => bytes,
            _ => return Ok(Err(DriftedInput::File(bundle.path))),
        };
        let origin = bundle_origin(self.snapshot, bundle_uuid)
            .map_err(store_failure)?
            .ok_or_else(unpublished)?
            .1;
        self.server
            .inner
            .handle
            .authoring_bundle_parses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let parsed = distill_bundle::parse_bundle(&bytes)
            .map_err(|error| RpcFailure::InvalidQuery { detail: error.to_string() })?;
        Ok(Ok(Rc::new(ReadBundle { parsed, root, origin })))
    }
}

/// Inspect `uuid` at `snapshot`: its metadata from the snapshot, its
/// schema and value from its bundle file, verified against the hash the
/// snapshot published. A file that no longer holds those bytes is
/// [`AuthoringInspectResult::Drifted`].
fn inspect_authoring(
    server: &Server,
    snapshot: &StoreReader,
    stamp: SnapshotStamp,
    uuid: AssetUuid,
) -> Result<AuthoringInspectResult, RpcFailure> {
    inspect_entry(&mut BundleReads::new(server, snapshot), stamp, uuid)
}

/// [`inspect_authoring`] of each of `uuids`, in order, reading each bundle
/// once.
fn inspect_authoring_many(
    server: &Server,
    snapshot: &StoreReader,
    stamp: SnapshotStamp,
    uuids: &[AssetUuid],
) -> Result<Vec<AuthoringInspectResult>, RpcFailure> {
    let mut reads = BundleReads::new(server, snapshot);
    uuids.iter().map(|&uuid| inspect_entry(&mut reads, stamp, uuid)).collect()
}

fn inspect_entry(
    reads: &mut BundleReads<'_>,
    stamp: SnapshotStamp,
    uuid: AssetUuid,
) -> Result<AuthoringInspectResult, RpcFailure> {
    let (meta, read) = match reads.entry(uuid)? {
        EntryBundle::Read(meta, read) => (meta, read),
        EntryBundle::Answer(answer) => return Ok(answer),
    };
    let entry = crate::server::bundle_authoring_entry_by_ref(
        &read.parsed,
        &meta.local_id,
        meta.normalized_path,
        meta.terminal_type,
    )
    .map_err(|detail| RpcFailure::InvalidQuery { detail })?;
    let type_name = type_name(
        &*reads.server.inner.handle.authoring_backend(),
        reads.snapshot,
        entry.type_uuid,
    );
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
        root: read.root.clone(),
        origin: read.origin,
        type_name,
        content: meta.content,
    }))
}

/// Blob `index` of `uuid`'s value at `snapshot` as one structural chunk
/// section; the blob table's order is the order an inspection lists its
/// refs in.
fn authoring_blob(
    server: &Server,
    snapshot: &StoreReader,
    stamp: SnapshotStamp,
    uuid: AssetUuid,
    index: u32,
) -> Result<AuthoringBlobResult, RpcFailure> {
    let (meta, read) = match BundleReads::new(server, snapshot).entry(uuid)? {
        EntryBundle::Read(meta, read) => (meta, read),
        EntryBundle::Answer(AuthoringInspectResult::Drifted { input, current }) => {
            return Ok(AuthoringBlobResult::Drifted { input, current })
        }
        EntryBundle::Answer(_) => return Ok(AuthoringBlobResult::Missing),
    };
    let invalid = |detail: String| RpcFailure::InvalidQuery { detail };
    let entry = read
        .parsed
        .assets
        .get(&meta.local_id)
        .ok_or_else(|| invalid(format!("bundle has no entry {:?}", meta.local_id)))?;
    let schema = read
        .parsed
        .schemas
        .get(&entry.schema_hash)
        .ok_or_else(|| invalid(format!("bundle has no schema snapshot for entry {:?}", meta.local_id)))?;
    let types = read.parsed.type_table().map_err(|error| invalid(error.to_string()))?;
    let Some(blob) = crate::validate::value_blob(&schema.root, &entry.data, &types, index).map_err(invalid)?
    else {
        return Ok(AuthoringBlobResult::Missing);
    };
    let payload = ArtifactPayload { structural: blob, blobs: Vec::new(), load_edges: Vec::new() };
    Ok(AuthoringBlobResult::Blob { stamp, chunks: chunk_payload(&payload, DEFAULT_CHUNK_SIZE) })
}

// ---------------------------------------------------------------------------
// Browsing (§17 "Browsing", protocol 23)

/// The rows of `query` at `snapshot`: as [`query_assets`], but
/// `authoring_only: None` selects both roles; each row names its type and
/// its bundle's origin. Sorted by root, path and local id.
fn list_rows(
    server: &Server,
    snapshot: &StoreReader,
    query: &AssetQuery,
) -> Result<Result<Vec<AuthoringRow>, RpcFailure>, StoreError> {
    let mut filter = asset_filter(query, AuthoringEntryRole::Runtime);
    filter.authoring_only = query.authoring_only;
    let glob = |path: &str| {
        query
            .path_glob
            .as_ref()
            .is_none_or(|pattern| path_glob_matches(pattern, path))
    };
    let matched = match snapshot.served_assets_matching(&filter, glob)? {
        Ok(matched) => matched,
        Err(bundles) => return Ok(Err(RpcFailure::TagIndexPoisoned { bundles })),
    };
    rows_of(server, snapshot, matched.into_iter().map(|matched| matched.asset)).map(Ok)
}

/// The rows of `uuids` at `snapshot`, sorted by root, path and local id;
/// an entry the snapshot does not serve has none.
fn rows_of(
    server: &Server,
    snapshot: &StoreReader,
    uuids: impl Iterator<Item = AssetUuid>,
) -> Result<Vec<AuthoringRow>, StoreError> {
    let backend = server.inner.handle.authoring_backend();
    let mut bundles = BTreeMap::<BundleUuid, Option<(String, BundleOrigin)>>::new();
    let mut names = BTreeMap::<TypeUuid, String>::new();
    // The primaries of the bundles at each path (several roots may share one).
    let mut primaries = BTreeMap::<String, std::collections::BTreeSet<AssetUuid>>::new();
    let mut rows = Vec::new();
    for uuid in uuids {
        let Some(entry) = snapshot.served_entry_meta(uuid)? else {
            continue;
        };
        let bundle = match bundles.entry(entry.bundle) {
            std::collections::btree_map::Entry::Occupied(slot) => slot.into_mut(),
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(bundle_origin(snapshot, entry.bundle)?)
            }
        };
        let Some((root, origin)) = bundle.clone() else {
            continue;
        };
        let type_name = names
            .entry(entry.type_uuid)
            .or_insert_with(|| type_name(&*backend, snapshot, entry.type_uuid))
            .clone();
        let primary = match primaries.get(&entry.normalized_path) {
            Some(set) => set.contains(&entry.asset),
            None => {
                let set = snapshot.served_path_candidates(&entry.normalized_path)?;
                let primary = set.contains(&entry.asset);
                primaries.insert(entry.normalized_path.clone(), set);
                primary
            }
        };
        rows.push(AuthoringRow {
            uuid: entry.asset,
            bundle: entry.bundle,
            root,
            normalized_path: entry.normalized_path,
            local_id: entry.local_id,
            type_uuid: entry.type_uuid,
            type_name,
            role: entry_role(entry.authoring_only),
            origin,
            primary,
            content: entry.content,
        });
    }
    rows.sort_by(|a, b| {
        (&a.root, &a.normalized_path, &a.local_id).cmp(&(&b.root, &b.normalized_path, &b.local_id))
    });
    Ok(rows)
}

/// `bundle`'s root and origin at `snapshot`; `None` without a published
/// row.
fn bundle_origin(
    snapshot: &StoreReader,
    bundle: BundleUuid,
) -> Result<Option<(String, BundleOrigin)>, StoreError> {
    let Some(meta) = snapshot.bundle(bundle)? else {
        return Ok(None);
    };
    let Some(root) = snapshot.root_name(meta.root)? else {
        return Ok(None);
    };
    let origin = match meta.origin {
        Some(origin) if origin.rules_bundle == distill_core::id::default_rules_bundle(&root) => {
            BundleOrigin::Default { rule: origin.rule.0 }
        }
        Some(origin) => BundleOrigin::Rule {
            rules_bundle: origin.rules_bundle,
            rule: origin.rule.0,
        },
        None => {
            let record = AssetFilter {
                bundle: Some(bundle),
                local_id: Some("$record".to_owned()),
                authoring_only: Some(true),
                ..AssetFilter::default()
            };
            match snapshot.served_assets_matching(&record, |_| true)? {
                Ok(matched) if !matched.is_empty() => BundleOrigin::Explicit,
                _ => BundleOrigin::Authored,
            }
        }
    };
    Ok(Some((root, origin)))
}

/// A type's path: a bootstrap control type's Distill name, else the served
/// schema's; empty when unknown.
fn type_name(backend: &dyn AuthoringBackend, snapshot: &StoreReader, type_uuid: TypeUuid) -> String {
    use distill_core::bootstrap::{
        DIRECTORY_IMPORT_RULES_TYPE_UUID, IMPORT_RECORD_TYPE_UUID, PACK_DEFINITION_TYPE_UUID,
    };
    match type_uuid {
        IMPORT_RECORD_TYPE_UUID => "distill::ImportRecord".to_owned(),
        DIRECTORY_IMPORT_RULES_TYPE_UUID => "distill::DirectoryImportRules".to_owned(),
        PACK_DEFINITION_TYPE_UUID => "distill::PackDefinition".to_owned(),
        _ => backend.type_name(snapshot, type_uuid).unwrap_or_default(),
    }
}

/// `bundle`'s reference edges at `snapshot`; `None` without the bundle.
fn bundle_references(
    snapshot: &StoreReader,
    bundle: BundleUuid,
) -> Result<Option<BundleReferences>, StoreError> {
    let Some(meta) = snapshot.bundle(bundle)? else {
        return Ok(None);
    };
    let mut referenced_by = Vec::new();
    for referrer in snapshot.bundles_referencing_path(&meta.path)? {
        let Some(row) = snapshot.bundle(referrer)? else {
            continue;
        };
        let Some(root) = snapshot.root_name(row.root)? else {
            continue;
        };
        referenced_by.push((referrer, root, row.path));
    }
    referenced_by.sort_by(|a, b| (&a.1, &a.2).cmp(&(&b.1, &b.2)));
    Ok(Some(BundleReferences {
        referenced_by,
        references: snapshot.bundle_path_refs(bundle)?.into_iter().collect(),
    }))
}

// ---------------------------------------------------------------------------
// Type descriptions (§17)

enum TypeFileError {
    /// The file no longer holds the bytes the snapshot observed.
    Drifted(String),
    Failure(RpcFailure),
}

impl From<RpcFailure> for TypeFileError {
    fn from(error: RpcFailure) -> Self {
        Self::Failure(error)
    }
}

/// The type files (`ngp_schema::prefab_types`) as a snapshot observed
/// them: under the one root holding the marker file, each read verified
/// against the snapshot's content hash.
struct TypeFiles<'a> {
    server: &'a Server,
    snapshot: &'a StoreReader,
}

impl TypeFiles<'_> {
    /// The root holding [`prefab_types::MARKER`]; `None` without one. Two
    /// such roots are an error: types would be ambiguous.
    fn root(&self) -> Result<Option<String>, RpcFailure> {
        let rows = self.snapshot.observed_files_at(prefab_types::MARKER).map_err(store_failure)?;
        let mut roots = rows.into_iter().map(|row| row.root_name);
        let Some(root) = roots.next() else {
            return Ok(None);
        };
        if let Some(other) = roots.next() {
            return Err(RpcFailure::InvalidQuery {
                detail: format!("roots {root:?} and {other:?} both hold type files ({})", prefab_types::MARKER),
            });
        }
        Ok(Some(root))
    }

    /// The bytes of `path` and their hash as the snapshot observed them;
    /// `None` for no file there.
    fn read(&self, root: &str, path: &str) -> Result<Option<(ContentHash, Vec<u8>)>, TypeFileError> {
        let Some(hash) = self.snapshot.file_content_hash(root, path).map_err(store_failure)? else {
            return Ok(None);
        };
        match self.server.inner.handle.authoring_backend().read_file(self.snapshot, root, path) {
            Ok(bytes) if ContentHash(*blake3::hash(&bytes).as_bytes()) == hash => Ok(Some((hash, bytes))),
            _ => Err(TypeFileError::Drifted(path.to_owned())),
        }
    }

    /// The index's hash and rows; `None` without type files.
    fn index(&self) -> Result<Option<(ContentHash, TypeIndex)>, TypeFileError> {
        let Some(root) = self.root()? else {
            return Ok(None);
        };
        let Some((hash, bytes)) = self.read(&root, prefab_types::INDEX)? else {
            return Ok(None);
        };
        let index = prefab_types::read_index(&bytes).map_err(|error| invalid_type_file(prefab_types::INDEX, error))?;
        let mut types = Vec::with_capacity(index.types.len());
        for row in index.types {
            let type_uuid = row
                .uuid
                .parse()
                .map_err(|error| invalid_type_file(prefab_types::INDEX, format!("uuid {:?}: {error:?}", row.uuid)))?;
            types.push(DescribableType { type_uuid, path: row.path, component: row.component });
        }
        Ok(Some((hash, TypeIndex { root, types })))
    }
}

struct TypeIndex {
    root: String,
    types: Vec<DescribableType>,
}

fn invalid_type_file(path: &str, detail: impl fmt::Display) -> TypeFileError {
    TypeFileError::Failure(RpcFailure::InvalidQuery {
        detail: format!("type file {path}: {detail}"),
    })
}

fn describe_type_in(
    server: &Server,
    snapshot: &StoreReader,
    stamp: SnapshotStamp,
    type_uuid: TypeUuid,
) -> Result<DescribeTypeResult, RpcFailure> {
    let files = TypeFiles { server, snapshot };
    let described = (|| -> Result<Option<TypeDescription>, TypeFileError> {
        let Some((_, index)) = files.index()? else {
            return Ok(None);
        };
        let Some(entry) = index.types.into_iter().find(|t| t.type_uuid == type_uuid) else {
            return Ok(None);
        };
        let schema_path = prefab_types::schema_file(&entry.path);
        let Some((_, bytes)) = files.read(&index.root, &schema_path)? else {
            return Ok(None);
        };
        let (schema, renames) = match prefab_types::read_file(&bytes).map_err(|error| invalid_type_file(&schema_path, error))? {
            prefab_types::Contents::Type { schema, renames } => (schema, renames),
            prefab_types::Contents::RenamedTo(_) => return Ok(None),
        };
        let description_path = prefab_types::description_file(&entry.path);
        let members = match files.read(&index.root, &description_path)? {
            Some((_, bytes)) => {
                prefab_types::read_description(&bytes).map_err(|error| invalid_type_file(&description_path, error))?.members
            }
            None => Default::default(),
        };
        let schema_hash = distill_schema::ngp_schema::node_hash(&schema.root)
            .map_err(|error| invalid_type_file(&schema_path, format!("{error:?}")))?;
        let logical_schema = distill_schema::ngp_schema::snapshot_to_json(&schema)
            .map_err(|error| invalid_type_file(&schema_path, error))?;
        let default = match files.read(&index.root, &prefab_types::default_file(&entry.path))? {
            None => TypeDefault::Absent,
            Some((_, bytes)) => match prefab_types::read_default(&bytes, &schema.root) {
                Ok(prefab_types::DefaultFile::OtherSchema(written)) => TypeDefault::SchemaMismatch { written },
                Ok(prefab_types::DefaultFile::Unavailable(reason)) => TypeDefault::Invalid(reason),
                Ok(prefab_types::DefaultFile::Value(value)) => {
                    // A default file is read without a type table (`read_default`),
                    // so it holds no dynamic value and carries no types.
                    match crate::validate::authoring_value(&schema.root, &value, &Default::default()) {
                        Ok(value) => TypeDefault::Value(value),
                        Err(error) => TypeDefault::Invalid(error),
                    }
                }
                Err(error) => TypeDefault::Invalid(error),
            },
        };
        let members = members
            .into_iter()
            .map(|(path, member)| TypeMember {
                path,
                doc: member.doc,
                type_doc: member.type_doc,
                editor: member.editor.to_json(),
            })
            .collect();
        Ok(Some(TypeDescription {
            stamp,
            type_uuid,
            path: entry.path,
            schema_hash,
            logical_schema: Arc::from(logical_schema.into_bytes()),
            default,
            members,
            renames,
        }))
    })();
    match described {
        Ok(Some(description)) => Ok(DescribeTypeResult::Description(description)),
        Ok(None) => Ok(DescribeTypeResult::Missing),
        Err(TypeFileError::Drifted(path)) => Ok(DescribeTypeResult::Drifted {
            input: DriftedInput::File(path),
            current: server.inner.current_stamp().map_err(store_failure)?,
        }),
        Err(TypeFileError::Failure(error)) => Err(error),
    }
}

/// What a generation fence answers in place of the call, if anything: a
/// reconnect, or the failure to read the fence.
fn fenced<T>(fence: Result<Option<ReconnectReason>, RpcFailure>) -> Option<RpcResult<T>> {
    match fence {
        Ok(reason) => reason.map(|reason| RpcResult::ReconnectRequired { reason }),
        Err(error) => Some(RpcResult::Failure(error)),
    }
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
    match server.inner.generation_fence(connection) {
        Ok(None) => {}
        Ok(Some(reason)) => return Some(AuthoringGate::Reconnect(reason)),
        Err(error) => return Some(AuthoringGate::Failure(error)),
    }
    let current = match server.inner.current_stamp() {
        Ok(stamp) => stamp.version,
        Err(error) => return Some(AuthoringGate::Failure(store_failure(error))),
    };
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
    match server.inner.configuration() {
        Ok(ConfigurationStatus::Failed(error)) => {
            return Some(AuthoringGate::ConfigurationFailed(error));
        }
        Ok(ConfigurationStatus::Ready) => {}
        Err(error) => return Some(AuthoringGate::Failure(store_failure(error))),
    }
    if let Some(error) = pipeline_failure(server.inner.effective_pipeline(&txn)) {
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
/// Complete a prepared operation as one input on `server`'s writer, still
/// at `base`.
fn complete_publication(
    server: &Server,
    base: InputVersion,
    publication: PreparedOperationPublication,
    what: &'static str,
) -> Result<(), String> {
    let publication = match publication {
        // A report reads one snapshot of its base and writes nothing.
        PreparedOperationPublication::Report(operation) => {
            let snapshot = server
                .report_snapshot()
                .map_err(|error| format!("{what} cannot read the store: {error}"))?;
            if snapshot.stamp().version != base {
                return Err(format!(
                    "{what} lost its input basis: expected {base:?}, observed {:?}",
                    snapshot.stamp().version
                ));
            }
            return operation.report(&snapshot)?.map_or(Ok(()), Err);
        }
        publication => publication,
    };
    {
        let mut failed = None;
        let mut terminal = None;
        let published = server.coordinated_maybe_commit(base, |store| {
            let (commit, terminal_error) = match publication {
                PreparedOperationPublication::Immediate(commit) => (Some(*commit), None),
                PreparedOperationPublication::Report(_) => {
                    unreachable!("a report publishes nothing")
                }
                PreparedOperationPublication::Deferred(operation) => {
                    match operation.complete(store, base) {
                        Ok(completed) => (completed.commit, completed.terminal_error),
                        // Nothing a failed operation wrote commits.
                        Err(error) => {
                            failed = Some(error.clone());
                            return Err(error);
                        }
                    }
                }
            };
            terminal = terminal_error;
            Ok(commit)
        });
        if let Some(error) = failed {
            return Err(error);
        }
        match published {
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
        complete_publication(
            &self.server,
            self.base,
            publication,
            "long-running operation",
        )
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
        let basis = metadata_basis(txn.stamp);
        let hold = self
            .server
            .inner
            .register_snapshot(txn)
            .map_err(MetadataCall::Error)?;
        Ok((basis, hold))
    }

    pub fn snapshot(&self) -> MetadataCall<MetadataSnapshot> {
        match self.open() {
            Ok((basis, hold)) => MetadataCall::Success(MetadataSnapshot {
                server: self.server.clone(),
                basis,
                hold,
            }),
            Err(failure) => failure.retype(),
        }
    }

    pub fn authoring_snapshot(&self) -> MetadataCall<MetadataAuthoringSnapshot> {
        match self.open() {
            Ok((basis, hold)) => MetadataCall::Success(MetadataAuthoringSnapshot {
                server: self.server.clone(),
                basis,
                hold,
            }),
            Err(failure) => failure.retype(),
        }
    }

    pub fn diagnostics(&self) -> MetadataCall<MetadataDiagnostics> {
        let txn = metadata_try!(self.server.inner.current_snapshot());
        MetadataCall::Success(MetadataDiagnostics {
            stamp: txn.stamp,
            configuration: metadata_try!(self.server.inner.configuration()),
            pipeline: metadata_rpc_try!(self.server.inner.effective_pipeline(&txn)),
            namespace_errors: metadata_try!(self
                .server
                .inner
                .handle
                .namespace_errors(txn.snapshot())),
        })
    }

    pub fn fetch(&self, hash: ContentHash) -> MetadataCall<ChunkStream> {
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
            Self::SnapshotExpired => MetadataCall::SnapshotExpired,
            Self::Error(error) => MetadataCall::Error(error),
        }
    }
}

/// The shared body of the two metadata snapshot kinds.
struct MetadataView<'a> {
    server: &'a Server,
    hold: &'a SnapshotHold,
}

impl MetadataView<'_> {
    fn preflight<T>(&self) -> Option<MetadataCall<T>> {
        if !self.hold.alive() {
            return Some(MetadataCall::SnapshotExpired);
        }
        None
    }

    fn namespace<T>(&self) -> Result<Rc<SnapshotTxn>, MetadataNamespaceCall<T>> {
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
        match namespace_try!(query_pure_metadata(txn.snapshot(), query)) {
            Ok(assets) => MetadataNamespaceCall::Success(assets),
            Err(failure) => MetadataNamespaceCall::Error(failure),
        }
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
        let basis = metadata_basis(txn.stamp);
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
            configuration: metadata_try!(self.server.inner.configuration()),
            pipeline: metadata_rpc_try!(self.server.inner.effective_pipeline(&txn)),
            namespace_errors: metadata_try!(self
                .server
                .inner
                .handle
                .namespace_errors(txn.snapshot())),
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
        match inspect_authoring(&self.server, txn.snapshot(), self.basis.snapshot, uuid) {
            Ok(result) => MetadataNamespaceCall::Success(result),
            Err(error) => MetadataNamespaceCall::Error(error),
        }
    }

    pub fn refresh(&self) -> MetadataCall<MetadataAuthoringSnapshot> {
        match self.view().refresh() {
            Ok((basis, hold)) => MetadataCall::Success(Self {
                server: self.server.clone(),
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
    /// malformed or the requested method is reserved. `Err`: the fence
    /// could not be read, the call's failure.
    pub fn generation_reconnect(&self) -> Result<Option<ReconnectReason>, RpcFailure> {
        self.server
            .inner
            .generation_fence(&self.connection.borrow())
    }

    fn live<T>(&self) -> Option<RpcResult<T>> {
        self.server.inner.pump(&self.connection);
        let connection = self.connection.borrow();
        fenced(self.server.inner.generation_fence(&connection))
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

    /// Run the backend's `prepare` on this connection's writer and publish
    /// its commit as one input, still at `base`. `None` when the backend
    /// declined: what it wrote then commits as durable state with no new
    /// version. When it fails, nothing it wrote commits.
    fn prepared<T>(
        &self,
        base: InputVersion,
        prepare: impl FnOnce(
            &dyn AuthoringBackend,
            &mut Store,
        ) -> Result<Option<(Commit, T)>, RpcFailure>,
    ) -> Option<RpcResult<T>> {
        let handle = &self.server.inner.handle;
        let backend = handle.authoring_backend();
        self.server.inner.with_writer(|store| {
            let mut value = None;
            let mut failed = None;
            let published = handle.coordinated_maybe_commit(store, base, |store| {
                match prepare(&*backend, store) {
                    Ok(Some((commit, prepared))) => {
                        value = Some(prepared);
                        Ok(Some(commit))
                    }
                    Ok(None) => Ok(None),
                    Err(error) => {
                        let detail = format!("{error:?}");
                        failed = Some(error);
                        Err(detail)
                    }
                }
            });
            if let Some(error) = failed {
                return Some(RpcResult::Failure(error));
            }
            match published {
                Ok(Some(_)) => Some(RpcResult::Success(
                    value.expect("a publication has a value"),
                )),
                Ok(None) => None,
                Err(error) => Some(RpcResult::Failure(coordinated_failure(error))),
            }
        })
    }

    /// `force_lossy` writes even when data held under the on-disk schema
    /// would be dropped (see [`RpcFailure::LossyWrite`]). The reply comes
    /// once the changed files are atomically on disk; the store publishes
    /// them through the watcher (see [`WriteReceipt`]). A failure means no
    /// file changed.
    pub fn write(
        &self,
        base: InputVersion,
        ops: Vec<AuthoringOp>,
        force_lossy: bool,
    ) -> RpcResult<WriteReceipt> {
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
        self.write_files(base, &ops, force_lossy)
    }

    /// Run the backend's file write on this connection's writer, inside an
    /// input at `base` that is rolled back: it holds the write lock while
    /// the backend plans and writes, and commits nothing.
    fn write_files(
        &self,
        base: InputVersion,
        ops: &[AuthoringOp],
        force_lossy: bool,
    ) -> RpcResult<WriteReceipt> {
        let backend = self.server.inner.handle.authoring_backend();
        self.server.inner.with_writer(|store| {
            let observed = match store.open_input() {
                Ok(observed) => observed,
                Err(error) => return RpcResult::Failure(store_failure(error)),
            };
            let written = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if observed != base {
                    return Err(RpcFailure::StaleInputVersion {
                        expected: observed,
                        got: base,
                    });
                }
                backend.write_files(store, base, ops, force_lossy)
            }));
            let _ = store.finish_input(false);
            match written {
                Ok(Ok(receipt)) => RpcResult::Success(receipt),
                Ok(Err(error)) => RpcResult::Failure(error),
                Err(panic) => std::panic::resume_unwind(panic),
            }
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

    /// Make explicit import `bundle` follow the rule claiming its sources
    /// again (§17 "Release"): re-imported under the rule's settings and
    /// origin, keeping its bundle and entry UUIDs.
    pub fn release(&self, base: InputVersion, bundle: BundleUuid) -> RpcResult<BundleUuid> {
        match self.release_prepare(base, bundle) {
            Ok(pending) => self.import_finish(pending.run()),
            Err(result) => result,
        }
    }

    /// [`Hub::import_prepare`] for a release.
    pub fn release_prepare(
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
            work: ImportWork::Release(bundle),
        })
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
            return Err(RpcResult::Failure(RpcFailure::InvalidAuthoringRequest {
                detail,
            }));
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
        // A failure the job memoized commits that memo and publishes
        // nothing.
        let mut memoized = None;
        let published = self.prepared(base, |_, store| {
            let prepared = match job(store)? {
                Ok(prepared) => prepared,
                Err(failure) => {
                    memoized = Some(failure);
                    return Ok(None);
                }
            };
            if reimport.is_some_and(|bundle| bundle != prepared.bundle) {
                return Err(RpcFailure::InvalidAuthoringRequest {
                    detail: "reimport backend changed the bundle identity".to_owned(),
                });
            }
            Ok(Some((prepared.commit, prepared.bundle)))
        });
        match (published, memoized) {
            (Some(result), _) => result,
            (None, Some(failure)) => RpcResult::Failure(failure),
            (None, None) => unreachable!("an import publishes or fails"),
        }
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
            .inner
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
        let requested_assets: BTreeSet<_> = assets.into_iter().collect();
        let requested_paths: BTreeSet<_> = paths.into_iter().collect();
        // Only `subscribe` grows a connection's subjects, so these are the
        // ones the history below is for.
        let (new_assets, new_paths) = {
            let connection = self.connection.borrow();
            let assets: BTreeSet<_> = requested_assets
                .difference(&connection.subscribed_assets)
                .copied()
                .collect();
            let paths: BTreeSet<_> = requested_paths
                .difference(&connection.subscribed_paths)
                .cloned()
                .collect();
            (assets, paths)
        };
        let inner = &self.server.inner;
        let (stamp, head, oldest, history) = rpc_try!(inner.read_consistent(|reader| {
            let stamp = reader.stamp()?;
            let oldest = reader.change_log_oldest()?;
            let history = if oldest <= since && since < stamp.version {
                reader.change_log_history(since, stamp.version, &new_assets, &new_paths)?
            } else {
                Vec::new()
            };
            Ok((stamp, reader.change_log_head()?, oldest, history))
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

        let deltas = history_deltas(stamp.instance, &history, None)
            .iter()
            .filter_map(|delta| delta.filtered(&new_assets, &new_paths))
            .collect::<Vec<_>>();
        self.install_stream(connection, stamp, since, oldest, deltas)
    }

    /// Index subscription (protocol 30): from now on this connection's
    /// stream carries every published version's delta whole, with its
    /// source files and type-file flag, besides the asset and path
    /// subscriptions it already serves. The history from `since` is the
    /// whole change log after it; past retention it is `ResyncRequired`.
    pub fn subscribe_index(&self, since: InputVersion) -> RpcResult<SubscriptionInstall> {
        if let Some(result) = self.live() {
            return result;
        }
        let inner = &self.server.inner;
        let (stamp, head, oldest, history, types_root) = rpc_try!(inner.read_consistent(|reader| {
            let stamp = reader.stamp()?;
            let oldest = reader.change_log_oldest()?;
            let history = if oldest <= since && since < stamp.version {
                reader.change_log_window(since, stamp.version)?
            } else {
                Vec::new()
            };
            let types_root = if has_file_changes(&history) { types_root(reader)? } else { None };
            Ok((stamp, reader.change_log_head()?, oldest, history, types_root))
        }));
        if since > stamp.version {
            return RpcResult::Failure(RpcFailure::InvalidCursor {
                since,
                current: stamp.version,
            });
        }
        // Deliver everything up to the read version as subscribed before;
        // the history below covers the index.
        inner.pump_until(&self.connection, Some(head));
        let mut connection = self.connection.borrow_mut();
        connection.index = true;
        let deltas = history_deltas(stamp.instance, &history, types_root.as_deref())
            .iter()
            .filter_map(HistoryDelta::whole)
            .collect::<Vec<_>>();
        self.install_stream(connection, stamp, since, oldest, deltas)
    }

    /// Queue a subscription's history on the connection's stream (or
    /// `ResyncRequired` when `since` is past retention), installing the
    /// stream on its first subscription.
    fn install_stream(
        &self,
        mut connection: std::cell::RefMut<'_, ConnectionState>,
        stamp: SnapshotStamp,
        since: InputVersion,
        oldest: InputVersion,
        deltas: Vec<Delta>,
    ) -> RpcResult<SubscriptionInstall> {
        let installed = stamp.version;
        let basis = RpcBasis { snapshot: stamp };
        let first_install = !connection.stream_installed;
        if since < oldest {
            connection.enqueue(StreamEvent::ResyncRequired {
                basis,
                oldest_available: oldest,
            });
        } else if first_install {
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
        connection.stream_installed = true;
        if first_install {
            // The keys as they are now; a later change reaches the stream
            // through `pump`.
            let (serial, keys) = self.server.inner.handle.restart_required();
            connection.restart_serial = serial;
            if !keys.is_empty() {
                connection.enqueue(StreamEvent::Asset {
                    basis: RpcBasis { snapshot: stamp },
                    event: AssetEvent::RestartRequired { keys },
                });
            }
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
    Release(BundleUuid),
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
            ImportWork::Release(bundle) => {
                (Some(bundle), self.backend.run_release(self.base, bundle))
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
    Drifted(DriftedInput),
    Failed(String),
}

impl Snapshot {
    pub fn stamp(&self) -> SnapshotStamp {
        self.basis.snapshot
    }

    pub fn generation_reconnect(&self) -> Result<Option<ReconnectReason>, RpcFailure> {
        self.server
            .inner
            .generation_fence(&self.connection.borrow())
    }

    fn preflight<T>(&self) -> Result<Rc<SnapshotTxn>, RpcResult<T>> {
        let connection = self.connection.borrow();
        if let Some(result) = fenced(self.server.inner.generation_fence(&connection)) {
            return Err(result);
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
            Ok(_) => RpcResult::Success(rpc_try!(self.server.inner.configuration())),
            Err(result) => result,
        }
    }

    /// The watched-import failures recorded now, each named by its bundle's
    /// root and path at this snapshot. A failure is memo state, not input:
    /// recording or clearing one publishes no version, so a client polls its
    /// snapshot, and the failures are read as they stand, not as of the
    /// version (an idle version's shared transaction would hide every
    /// failure recorded after it began). A failure whose bundle this
    /// snapshot does not have is left for the snapshot that does.
    /// A directory import that failed before any bundle existed at its
    /// output is named by that output, with no bundle. So is each of the
    /// snapshot's namespace errors (a bundle file that does not read, …),
    /// by the file it names.
    pub fn import_failures(&self) -> RpcResult<Vec<ImportFailure>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        let reader = txn.snapshot();
        let mut failures = Vec::new();
        for (bundle, message) in
            rpc_try!(self.server.inner.reader.watched_import_failure_messages())
        {
            let Some(meta) = rpc_try!(reader.bundle(bundle)) else {
                continue;
            };
            let Some(root) = rpc_try!(reader.root_name(meta.root)) else {
                continue;
            };
            failures.push(ImportFailure {
                bundle: Some(bundle),
                root,
                path: meta.path,
                message,
            });
        }
        // Directory imports that failed before any bundle existed at their
        // output, named by that output: listed while the snapshot has their
        // rules bundle (a default import's is its root's reserved one, which
        // no file holds) and no bundle at the output.
        for failure in rpc_try!(self
            .server
            .inner
            .reader
            .directory_import_failure_messages())
        {
            let default_layer = failure.rules_bundle
                == distill_core::id::default_rules_bundle(&failure.destination_root);
            if (!default_layer && rpc_try!(reader.bundle(failure.rules_bundle)).is_none())
                || rpc_try!(reader.bundle_at(&failure.destination_root, &failure.destination_path))
                    .is_some()
            {
                continue;
            }
            failures.push(ImportFailure {
                bundle: None,
                root: failure.destination_root,
                path: failure.destination_path,
                message: failure.message,
            });
        }
        // Namespace errors (a bundle file that does not read, duplicate
        // claims, path collisions): the snapshot serves nothing from the
        // files they name, so a client polling failures learns why,
        // instead of finding no asset there.
        for error in rpc_try!(self.server.inner.handle.namespace_errors(reader)) {
            let (root, path) = namespace_error_location(&error.detail);
            failures.push(ImportFailure {
                bundle: None,
                root,
                path,
                message: error.message,
            });
        }
        failures.sort_by(|a, b| (&a.root, &a.path).cmp(&(&b.root, &b.path)));
        RpcResult::Success(failures)
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
            if let Some(result) = fenced(self.server.inner.generation_fence(&connection)) {
                return result;
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
            if let Some(error) = pipeline_failure(self.server.inner.effective_pipeline(&txn)) {
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
        if let Some(error) = pipeline_failure(self.server.inner.effective_pipeline(&txn)) {
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
        let txn = match self.preflight::<RuntimeTypePolicy>() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        let target = self.connection.borrow().target.clone();
        let Some(row) = rpc_try!(self.server.inner.reader.rpc_target(&target)) else {
            return RpcResult::ReconnectRequired {
                // A target goes only with a pipeline fence.
                reason: ReconnectReason::PipelineEpochChanged,
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
            .runtime_type_policy(txn.snapshot(), &request)
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
        if let Some(error) = pipeline_failure(self.server.inner.effective_pipeline(&txn)) {
            return done(RpcResult::Failure(error));
        }
        if let ConfigurationStatus::Failed(error) = self.server.inner.configuration()? {
            return done(RpcResult::ConfigurationFailed(error));
        }
        let snapshot = txn.snapshot();
        let derived = snapshot.derived_output(uuid)?;
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
            // A served entry is published; anything else resolves by its
            // rows (a deleted asset is missing).
            None if meta.is_some() => Some(VersionResolve::Drifted(DriftedInput::Asset(uuid))),
            None => match snapshot.asset_resolution(uuid)? {
                None => None,
                Some(AssetResolution::Published) => {
                    Some(VersionResolve::Drifted(DriftedInput::Asset(uuid)))
                }
                Some(AssetResolution::Failed(error)) => Some(VersionResolve::Failed(error)),
            },
        };
        // A drifted asset is built on demand. The backend keys the build by
        // its static inputs: a cached result whose traced inputs hold at
        // this snapshot answers at once, and requesters of one key share
        // one build.
        if let (Some(VersionResolve::Drifted(input)), Some(meta)) = (&resolution, &meta) {
            let entry = BuildEntry {
                uuid: meta.asset,
                type_uuid: meta.type_uuid,
                terminal_type: meta.terminal_type,
            };
            let Some(row) = self.server.inner.reader.rpc_target(&target)? else {
                return done(RpcResult::ReconnectRequired {
                    // A target goes only with a pipeline fence.
                    reason: ReconnectReason::PipelineEpochChanged,
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
            return match self
                .server
                .inner
                .handle
                .build_backend()
                .start(view, &request)
            {
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
            Some(VersionResolve::Drifted(input)) => match self.server.inner.current_stamp() {
                Ok(current) => ResolveResult::Drifted { input, current },
                Err(error) => return done(RpcResult::Failure(store_failure(error))),
            },
            Some(VersionResolve::Failed(error)) => ResolveResult::Failed { error },
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
                if let Some(error) = pipeline_failure(self.server.inner.effective_pipeline(&txn)) {
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
                    BuildAnswer::Drifted { input } => match self.server.inner.current_stamp() {
                        Ok(current) => ResolveResult::Drifted { input, current },
                        Err(error) => return RpcResult::Failure(store_failure(error)),
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
    pub fn generation_reconnect(&self) -> Result<Option<ReconnectReason>, RpcFailure> {
        self.server
            .inner
            .generation_fence(&self.connection.borrow())
    }

    pub fn stamp(&self) -> SnapshotStamp {
        self.basis.snapshot
    }

    fn preflight<T>(&self) -> Result<Rc<SnapshotTxn>, RpcResult<T>> {
        let connection = self.connection.borrow();
        if let Some(result) = fenced(self.server.inner.generation_fence(&connection)) {
            return Err(result);
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

    /// Each of `uuids` inspected, in order (§17 "Authoring snapshots"): a
    /// bundle holding several of them is read and parsed once. Values carry
    /// blob refs; [`Self::blob`] reads the bytes.
    pub fn inspect(&self, uuids: &[AssetUuid]) -> RpcResult<Vec<AuthoringInspectResult>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        match inspect_authoring_many(&self.server, txn.snapshot(), self.basis.snapshot, uuids) {
            Ok(result) => RpcResult::Success(result),
            Err(error) => RpcResult::Failure(error),
        }
    }

    /// Blob `index` of `uuid`'s value table (the order of an inspection's
    /// refs), as a chunk stream of one structural section.
    pub fn blob(&self, uuid: AssetUuid, index: u32) -> RpcResult<AuthoringBlobResult> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        match authoring_blob(&self.server, txn.snapshot(), self.basis.snapshot, uuid, index) {
            Ok(result) => RpcResult::Success(result),
            Err(error) => RpcResult::Failure(error),
        }
    }

    /// The content hash of the file this snapshot observed at `path` in
    /// root `root`; `None` for no file there (absent, or a directory). A
    /// [`WriteReceipt`] is reflected at the first version whose answers
    /// match it.
    pub fn file(&self, root: &str, path: &str) -> RpcResult<Option<ContentHash>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        RpcResult::Success(rpc_try!(txn.snapshot().file_content_hash(root, path)))
    }

    /// What the snapshot's type files say about `type_uuid`, with the
    /// default the serving pipeline registered (§17 "Type descriptions").
    pub fn describe_type(&self, type_uuid: TypeUuid) -> RpcResult<DescribeTypeResult> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        match describe_type_in(&self.server, txn.snapshot(), self.basis.snapshot, type_uuid) {
            Ok(result) => RpcResult::Success(result),
            Err(error) => RpcResult::Failure(error),
        }
    }

    /// The entries `query` selects, as rows with their type names and their
    /// bundles' origins (§17 "Browsing"). Unlike [`Self::query`],
    /// `authoring_only: None` selects both roles.
    pub fn list(&self, query: AssetQuery) -> RpcResult<Vec<AuthoringRow>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if let Err(detail) = validate_asset_query(&query, true) {
            return RpcResult::Failure(RpcFailure::InvalidQuery { detail });
        }
        match rpc_try!(list_rows(&self.server, txn.snapshot(), &query)) {
            Ok(rows) => RpcResult::Success(rows),
            Err(error) => RpcResult::Failure(error),
        }
    }

    /// The rows of the named entries (protocol 30), as [`Self::list`]
    /// answers them; an entry the snapshot does not serve has none. An
    /// index subscriber reads the entries a delta names with it.
    pub fn list_entries(&self, uuids: &[AssetUuid]) -> RpcResult<Vec<AuthoringRow>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        let uuids = uuids.iter().copied().collect::<BTreeSet<_>>();
        RpcResult::Success(rpc_try!(rows_of(&self.server, txn.snapshot(), uuids.into_iter())))
    }

    /// The files under `root` that `glob` matches (all without one), bundle
    /// files excepted, each with the layer claiming it (§17 "Browsing").
    pub fn source_files(&self, root: &str, glob: Option<&str>) -> RpcResult<Vec<SourceFile>> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        if root.is_empty() || glob.is_some_and(|glob| glob.is_empty() || glob.contains('\0')) {
            return RpcResult::Failure(RpcFailure::InvalidQuery {
                detail: "source files need a root and a nonempty glob, if any".to_owned(),
            });
        }
        match self
            .server
            .inner
            .handle
            .authoring_backend()
            .source_files(txn.snapshot(), root, glob)
        {
            Ok(files) => RpcResult::Success(files),
            Err(error) => RpcResult::Failure(error),
        }
    }

    /// `bundle`'s reference edges (§17 "Browsing").
    pub fn references(&self, bundle: BundleUuid) -> RpcResult<BundleReferences> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        match rpc_try!(bundle_references(txn.snapshot(), bundle)) {
            Some(references) => RpcResult::Success(references),
            None => RpcResult::Failure(RpcFailure::InvalidQuery {
                detail: format!("no bundle {bundle} at this snapshot"),
            }),
        }
    }

    /// What can be created at this snapshot (§17 "Creating").
    pub fn authorable(&self) -> RpcResult<Authorable> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        match self.server.inner.handle.authoring_backend().authorable(txn.snapshot()) {
            Ok(authorable) => RpcResult::Success(authorable),
            Err(error) => RpcResult::Failure(error),
        }
    }

    /// The types the snapshot's type index lists (§17 "Type descriptions").
    pub fn describable_types(&self) -> RpcResult<DescribableTypesResult> {
        let txn = match self.preflight() {
            Ok(txn) => txn,
            Err(result) => return result,
        };
        let types = TypeFiles { server: &self.server, snapshot: txn.snapshot() };
        let result = match types.index() {
            Ok(Some((index_hash, index))) => DescribableTypesResult::Types {
                stamp: self.basis.snapshot,
                index_hash: Some(index_hash),
                types: index.types,
            },
            Ok(None) => DescribableTypesResult::Types {
                stamp: self.basis.snapshot,
                index_hash: None,
                types: Vec::new(),
            },
            Err(TypeFileError::Drifted(path)) => DescribableTypesResult::Drifted {
                input: DriftedInput::File(path),
                current: rpc_try!(self.server.inner.current_stamp()),
            },
            Err(TypeFileError::Failure(error)) => return RpcResult::Failure(error),
        };
        RpcResult::Success(result)
    }

    pub fn refresh(&self) -> RpcResult<AuthoringSnapshot> {
        {
            let connection = self.connection.borrow();
            if let Some(result) = fenced(self.server.inner.generation_fence(&connection)) {
                return result;
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
    use distill_store::bundles::{AssetRecord, BundleMeta};
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
                        import_watched: false,
                    })?;
                    for (entry, authoring_only) in [(1u8, false), (2, true)] {
                        let mut tags = BTreeMap::new();
                        tags.insert(
                            "kind".to_owned(),
                            (index % 3 != 0)
                                .then(|| ["mesh", "rock"][index as usize % 2].to_owned()),
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
                            terminal_type: Some(if index % 2 == 0 { GPU_MESH } else { TEXTURE }),
                            content: None,
                        })?;
                    }
                }
                // `z`'s tag index is pending: its tag queries must fail.
                let z = PATHS.iter().position(|path| *path == "z").unwrap() as u32;
                txn.set_tag_index_pending(AssetUuid(uuid(0x21, z)))?;
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
            "*",
            "dir*",
            "dir/*",
            "*child*",
            "d?r*",
            "é/*",
            "a*b",
            "a?b/*",
            "bulk/b000?",
            "?",
            "dir",
            "*.txt",
            "*/child",
            "*/leaf",
            "*/ü.bundle",
            "*/c",
            "*.bundle",
            "*b/c",
            "a?b/c",
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
            // A narrow selector beside a broad one.
            for glob in ["*/child", "*/ü.bundle", "*.txt"] {
                queries.push(AssetQuery {
                    path_glob: Some(glob.into()),
                    authored_type: Some(type_uuid),
                    ..none.clone()
                });
            }
            queries.push(AssetQuery {
                local_id: Some("settings".into()),
                authored_type: Some(type_uuid),
                ..none.clone()
            });
            queries.push(AssetQuery {
                authoring_only: Some(true),
                terminal_type: Some(type_uuid),
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
        for local_id in ["main", "settings", "absent"] {
            queries.push(AssetQuery {
                local_id: Some(local_id.into()),
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

    /// Every path an RPC glob matches starts with its literal prefix and has
    /// the final segment and extension its literal tail names.
    #[test]
    fn an_rpc_glob_matches_only_paths_with_its_keys() {
        let paths = PATHS.iter().copied().chain([
            "",
            "x/child",
            "a/b/c",
            "q.txt",
            "x.y/z",
            "bulk/b12345",
            "dir/child/x",
        ]);
        let mut matched = 0;
        for query in selectors() {
            let Some(pattern) = query.path_glob.as_deref() else {
                continue;
            };
            let keys = GlobKeys::of(pattern, &['*', '?']);
            for path in paths.clone() {
                if path_glob_matches(pattern, path) {
                    matched += 1;
                    let name = path.rsplit_once('/').map_or(path, |(_, name)| name);
                    assert!(
                        path.starts_with(keys.prefix),
                        "{pattern:?} matched {path:?}"
                    );
                    assert!(
                        keys.name.is_none_or(|key| key == name),
                        "{pattern:?} matched {path:?}"
                    );
                    assert!(
                        keys.extension
                            .is_none_or(|key| name.rsplit_once('.').map(|(_, e)| e) == Some(key)),
                        "{pattern:?} matched {path:?}"
                    );
                }
            }
        }
        assert!(matched > 20, "{matched}");
    }

    #[test]
    fn asset_queries_answer_what_the_scan_answered() {
        let (_dir, store) = namespace(30);
        let reader = store.reader().unwrap();
        let mut nonempty = 0;
        for query in selectors() {
            for role in [
                AuthoringEntryRole::Runtime,
                AuthoringEntryRole::AuthoringOnly,
            ] {
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
        let roles = [
            None,
            Some(AuthoringEntryRole::Runtime),
            Some(AuthoringEntryRole::AuthoringOnly),
        ];
        let prefixes = [
            None,
            Some(""),
            Some("dir"),
            Some("dir/"),
            Some("é"),
            Some("bulk/b001"),
        ];
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
                        query_pure_metadata(&reader, &query).unwrap().unwrap(),
                        query_pure_metadata_scan(&entries, &query),
                        "{query:?}"
                    );
                }
            }
        }
    }

    /// A query whose selectors match a poisoned bundle's skeleton entry fails
    /// naming that bundle; one that cannot reach it answers.
    #[test]
    fn a_query_reaching_a_poisoned_bundle_fails_naming_it() {
        let (_dir, mut store) = namespace(0);
        let dir0 = PATHS.iter().position(|path| *path == "dir0").unwrap() as u32;
        let bundle = BundleUuid(uuid(0x10, dir0));
        store
            .input_transaction(|txn| {
                let root = txn.intern_root("main")?;
                txn.poison_bundle(
                    &distill_store::bundles::NamespaceSkeleton {
                        bundle,
                        root,
                        path: "dir0".into(),
                        format_version: 1,
                        content_hash: ContentHash([0xee; 32]),
                        entries: vec![distill_store::bundles::SkeletonEntry {
                            asset: AssetUuid(uuid(0x21, dir0)),
                            local_id: "main".into(),
                            type_uuid: MESH,
                            authoring_only: false,
                            tags: BTreeMap::from([("hero".to_owned(), None)]),
                        }],
                    },
                    "malformed",
                )
            })
            .unwrap();
        let reader = store.reader().unwrap();
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
        let role = AuthoringEntryRole::Runtime;
        let hero = Some(TagSelector {
            tag: "hero".into(),
            value: None,
        });
        for reaching in [
            AssetQuery {
                path_prefix: Some("dir".into()),
                ..none.clone()
            },
            AssetQuery {
                tag: hero.clone(),
                path_prefix: Some("dir".into()),
                ..none.clone()
            },
            AssetQuery {
                path_glob: Some("dir?".into()),
                local_id: Some("main".into()),
                ..none.clone()
            },
        ] {
            assert_eq!(
                query_assets(&reader, &reaching, role).unwrap(),
                Err(RpcFailure::TagIndexPoisoned {
                    bundles: vec![bundle]
                }),
                "{reaching:?}"
            );
        }
        for missing in [
            AssetQuery {
                tag: hero,
                path_prefix: Some("dir/".into()),
                ..none.clone()
            },
            AssetQuery {
                local_id: Some("settings".into()),
                ..none.clone()
            },
            AssetQuery {
                authored_type: Some(TEXTURE),
                ..none.clone()
            },
        ] {
            assert!(
                query_assets(&reader, &missing, role).unwrap().is_ok(),
                "{missing:?}"
            );
        }
        let pure = PureMetadataQuery {
            uuid: None,
            bundle: Some(bundle),
            normalized_path_prefix: None,
            authored_type: None,
            role: None,
        };
        assert_eq!(
            query_pure_metadata(&reader, &pure).unwrap(),
            Err(RpcFailure::TagIndexPoisoned {
                bundles: vec![bundle]
            })
        );
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
            AssetQuery {
                bundle_path: None,
                path_glob: Some("*/b12345".into()),
                tag: None,
                uuid: None,
                local_id: None,
                bundle_uuid: None,
                authored_type: Some(TEXTURE),
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
            assert!(
                scanned_pages >= 100 * indexed_pages,
                "{scanned_pages} pages"
            );
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
        assert_eq!(indexed.unwrap().unwrap(), scanned);
        println!("pure metadata prefix: {indexed_pages} pages (scan: {scanned_pages})");
        assert!(indexed_pages <= 64, "{indexed_pages} pages");
        assert!(
            scanned_pages >= 100 * indexed_pages,
            "{scanned_pages} pages"
        );
    }
}

/// The root and path a namespace error names, for listing it beside import
/// failures (`Snapshot::import_failures`): its first source's, or `""` for
/// one that names none (a derived claimant, a whole root).
fn namespace_error_location(detail: &NamespaceErrorV1) -> (String, String) {
    let source = |s: &ReadableBundleSource| (s.root_name.clone(), s.normalized_path.clone());
    let raw = |path: &PlatformPathBytes| match path {
        PlatformPathBytes::Unix(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        PlatformPathBytes::Windows(units) => String::from_utf16_lossy(units),
    };
    match detail {
        NamespaceErrorV1::DuplicateAssetUuid { claimants, .. } => claimants
            .iter()
            .find_map(|claimant| match claimant {
                AssetClaimant::Authored { source: s, .. } => Some(source(s)),
                AssetClaimant::Derived { .. } => None,
            })
            .unwrap_or_default(),
        NamespaceErrorV1::DuplicateBundleUuid { sources, .. } => sources.first().map(source).unwrap_or_default(),
        NamespaceErrorV1::SameRootNormalizedPathCollision { root_name, normalized_path, .. }
        | NamespaceErrorV1::UnreadableGlobalBundlePath { root_name, normalized_path, .. } => {
            (root_name.clone(), normalized_path.clone())
        }
        NamespaceErrorV1::IncompleteSkeleton { source: s, .. } => source(s),
        NamespaceErrorV1::InvalidPhysicalPath { root_name, raw_relative_path, .. } => {
            (root_name.clone(), raw(raw_relative_path))
        }
        NamespaceErrorV1::UnreadableScanSubtree { subject, .. } => match subject {
            ScanSubject::Root { root_name } => (root_name.clone(), String::new()),
            ScanSubject::Subtree { root_name, raw_relative_path } => (root_name.clone(), raw(raw_relative_path)),
        },
    }
}
