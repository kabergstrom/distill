//! Durable production authoring backend.
//!
//! Direct CRUD is a filesystem publication, never an RPC-only projection:
//! validate the exact store basis, mutate one canonical bundle, write it
//! atomically, reobserve only the authored paths, and return the commit
//! for that same durable successor version.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, Weak};

use distill_bundle::{AssetEntry, Bundle, BUNDLE_FORMAT_VERSION};
use distill_core::bootstrap::{is_bootstrap_control_type, DIRECTORY_IMPORT_RULES_TYPE_UUID};
use distill_core::id::{BundleUuid, ContentHash, LogicalHash};
use distill_schema::ngp_schema::{prefab_types, Patch, Value};
use ngp_migrate::{lossy_drops, plan_with_edges, uses_edge, Edges};
use distill_rpc::{
    names_blob_ref, resolve_blob_refs, stored_value, value_blobs, AuthoringBackend, AuthoringEntry, AuthoringEntryRole, AuthoringOp,
    Commit, ImportJob, ImportRequest, InputVersion, LongRunningOp, PreparedImportCommit,
    PreparedOperationCommit, RpcFailure, WriteReceipt, WrittenFile,
};
use distill_store::state::PipelineFailure;
use distill_store::{Store, StoreOpener, StoreReader};

use crate::compiled::{Compiled, CompiledRegistry};
use crate::coordinator::publish_incremental_paths;
use crate::importer::{ImportControlsCache, RegisteredImporter, RegisteredImporters};
use crate::scanner::ScanError;
use distill_store::atomic_file;

pub struct AuthoringService {
    /// Opens the reader an import run reads through when it runs outside
    /// any write.
    pub(crate) opener: Arc<StoreOpener>,
    /// The compiled configuration state of each live store version: every
    /// operation reads the roots, importers, schema and pipeline of the
    /// version its own transaction sees (`crate::compiled`).
    compiled: Arc<CompiledRegistry>,
    tag_index_coordinator: OnceLock<Weak<crate::coordinator::DaemonCoordinator>>,
    /// Published import bundles' decoded `$record` and `$settings`, by
    /// file content hash (`crate::importer::ImportControls`).
    pub(crate) import_controls: ImportControlsCache,
}

impl AuthoringService {
    pub(crate) fn new(opener: Arc<StoreOpener>, compiled: Arc<CompiledRegistry>) -> Self {
        Self {
            opener,
            compiled,
            tag_index_coordinator: OnceLock::new(),
            import_controls: ImportControlsCache::default(),
        }
    }

    pub(crate) fn attach_tag_index_coordinator(
        &self,
        coordinator: &Arc<crate::coordinator::DaemonCoordinator>,
    ) {
        if self
            .tag_index_coordinator
            .set(Arc::downgrade(coordinator))
            .is_err()
        {
            panic!("the tag index coordinator is attached once");
        }
    }

    pub(crate) fn tag_index_coordinator(
        &self,
    ) -> Option<Arc<crate::coordinator::DaemonCoordinator>> {
        self.tag_index_coordinator.get().and_then(Weak::upgrade)
    }

    /// The compiled state of the version `store`'s transaction sees.
    pub(crate) fn compiled(&self, store: &StoreReader) -> Result<Arc<Compiled>, RpcFailure> {
        self.compiled.at(store).map_err(RpcFailure::from)
    }

    pub(crate) fn compiled_registry(&self) -> Arc<CompiledRegistry> {
        Arc::clone(&self.compiled)
    }

    /// Validate a pipeline epoch's importers as its compiled state holds
    /// them.
    pub(crate) fn prepare_pipeline_importers(
        &self,
        importers: Vec<Arc<dyn crate::importer::AuthoringImporter>>,
    ) -> Result<RegisteredImporters, RpcFailure> {
        let mut next = BTreeMap::new();
        for importer in importers {
            let registered = RegisteredImporter::validate(importer)?;
            if next.insert(registered.id.clone(), registered).is_some() {
                return Err(invalid("pipeline epoch contains a duplicate importer id"));
            }
        }
        Ok(next)
    }

    /// Write the one bundle file a direct authoring batch changes. The
    /// write is complete once the file is atomically on disk: the watcher
    /// publishes it like any other edit. An error means the file did not
    /// change.
    fn write_direct(
        &self,
        store: &StoreReader,
        base: InputVersion,
        operations: &[AuthoringOp],
        force_lossy: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        require_base(store, base)?;
        let planned = self.plan_bundle_mutation(store, operations, force_lossy)?;
        let compiled = self.compiled(store)?;
        let content_hash = match planned.proposed.as_deref() {
            Some(bytes) => {
                let root = compiled
                    .scanner()
                    .root_containing(&planned.target)
                    .map_err(invalid)?;
                atomic_file::write(&root, &planned.target, bytes, planned.preimage.into())
                    .map_err(invalid)?;
                Some(atomic_file::content_hash(bytes))
            }
            None => {
                atomic_file::remove(&planned.target, planned.preimage.into()).map_err(invalid)?;
                None
            }
        };
        Ok(WriteReceipt {
            files: vec![WrittenFile {
                root: planned.root,
                path: planned.path,
                content_hash,
            }],
        })
    }

    /// Write or delete one bundle file, then publish the store rows for
    /// it. The target is re-hashed right before the change; if it no
    /// longer holds `preimage` the publication fails with a conflict.
    pub(crate) fn publish_file(
        &self,
        store: &mut Store,
        base: InputVersion,
        target: PathBuf,
        preimage: Option<ContentHash>,
        proposed: Option<Vec<u8>>,
    ) -> Result<Commit, RpcFailure> {
        require_base(store, base)?;
        let compiled = self.compiled(store)?;
        match proposed.as_deref() {
            Some(bytes) => {
                let root = compiled
                    .scanner()
                    .root_containing(&target)
                    .map_err(invalid)?;
                atomic_file::write(&root, &target, bytes, preimage.into())
            }
            None => atomic_file::remove(&target, preimage.into()),
        }
        .map_err(invalid)?;

        publish_incremental_paths(
            std::slice::from_ref(&target),
            store,
            base,
            &compiled,
            self.tag_index_coordinator().as_deref(),
        )
        .map_err(invalid)
    }

    fn plan_bundle_mutation(
        &self,
        store: &StoreReader,
        operations: &[AuthoringOp],
        force_lossy: bool,
    ) -> Result<PlannedBundleMutation, RpcFailure> {
        let mut seen = BTreeSet::new();
        let mut bundle_ids = BTreeSet::new();
        for operation in operations {
            let uuid = match operation {
                AuthoringOp::Set(entry) => {
                    bundle_ids.insert(entry.bundle);
                    entry.uuid
                }
                AuthoringOp::Remove { uuid } => {
                    let existing = store
                        .entry(*uuid)
                        .map_err(invalid)?
                        .ok_or_else(|| invalid(format!("cannot remove unknown asset {uuid}")))?;
                    bundle_ids.insert(existing.bundle);
                    *uuid
                }
            };
            if !seen.insert(uuid) {
                return Err(invalid(format!(
                    "authoring batch mutates asset {uuid} more than once"
                )));
            }
        }
        if bundle_ids.len() != 1 {
            return Err(invalid(
                "one direct authoring batch must mutate exactly one physical bundle",
            ));
        }
        let bundle_id = *bundle_ids.first().expect("one bundle ID");
        let compiled = self.compiled(store)?;
        let scanner = compiled.scanner();

        let (root, path, target, preimage, mut bundle) = if let Some(meta) =
            store.bundle(bundle_id).map_err(invalid)?
        {
            let root = store
                .root_name(meta.root)
                .map_err(invalid)?
                .ok_or_else(|| invalid("bundle root identity is missing"))?;
            let target = scanner.physical_path(&root, &meta.path).map_err(invalid)?;
            let bytes = scanner.read_identity_checked(&target).map_err(invalid)?;
            let observed = ContentHash(distill_core::hash::hash(&bytes));
            if observed != meta.content_hash {
                return Err(invalid(format!(
                    "bundle {} changed since durable version {}",
                    meta.path,
                    store.input_version().map_err(crate::authoring::invalid)?.0
                )));
            }
            let bundle = distill_bundle::parse_bundle(&bytes).map_err(invalid)?;
            if bundle.uuid != bundle_id {
                return Err(invalid(
                    "bundle UUID no longer matches the durable namespace",
                ));
            }
            for operation in operations {
                if let AuthoringOp::Set(entry) = operation {
                    if entry.normalized_path != meta.path {
                        return Err(invalid(
                            "direct write cannot relocate a bundle; use rename-with-fixups",
                        ));
                    }
                    if !entry.root.is_empty() && entry.root != root {
                        return Err(invalid(
                            "direct write cannot relocate a bundle; use rename-with-fixups",
                        ));
                    }
                }
            }
            (root, meta.path, target, Some(observed), bundle)
        } else {
            if operations
                .iter()
                .any(|operation| matches!(operation, AuthoringOp::Remove { .. }))
            {
                return Err(invalid("cannot remove from a bundle that does not exist"));
            }
            let paths = operations
                .iter()
                .filter_map(|operation| match operation {
                    AuthoringOp::Set(entry) => Some(entry.normalized_path.as_str()),
                    AuthoringOp::Remove { .. } => None,
                })
                .collect::<BTreeSet<_>>();
            if paths.len() != 1 {
                return Err(invalid("new bundle entries must name one destination path"));
            }
            let path = *paths.first().expect("one destination path");
            let named = operations
                .iter()
                .filter_map(|operation| match operation {
                    AuthoringOp::Set(entry) if !entry.root.is_empty() => Some(entry.root.as_str()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let root = match (named.len(), compiled.roots()) {
                (0, [root]) => root,
                (0, _) => {
                    return Err(invalid(
                        "direct bundle creation names its root when more than one asset root is configured",
                    ))
                }
                (1, roots) => {
                    let name = *named.first().expect("one named root");
                    roots
                        .iter()
                        .find(|root| root.name == name)
                        .ok_or_else(|| invalid(format!("no configured asset root {name:?}")))?
                }
                _ => return Err(invalid("new bundle entries must name one root")),
            };
            let target = scanner.physical_path(&root.name, path).map_err(invalid)?;
            if fs::symlink_metadata(&target).is_ok() {
                return Err(invalid("new bundle destination already exists"));
            }
            (
                root.name.clone(),
                path.to_owned(),
                target,
                None,
                Bundle {
                    format_version: BUNDLE_FORMAT_VERSION,
                    uuid: bundle_id,
                    primary: None,
                    schemas: BTreeMap::new(),
                    types: Default::default(),
                    assets: BTreeMap::new(),
                },
            )
        };

        for operation in operations {
            if let AuthoringOp::Remove { uuid } = operation {
                let local_id = bundle
                    .assets
                    .iter()
                    .find_map(|(local_id, entry)| (entry.uuid == *uuid).then(|| local_id.clone()))
                    .ok_or_else(|| invalid(format!("asset {uuid} is not present in its bundle")))?;
                bundle.assets.remove(&local_id);
                if bundle.primary.as_deref() == Some(&local_id) {
                    bundle.primary = None;
                }
            }
        }
        for operation in operations {
            let AuthoringOp::Set(entry) = operation else {
                continue;
            };
            self.apply_set(store, &compiled, &mut bundle, entry, force_lossy)?;
        }
        infer_primary(&mut bundle)?;
        let used_types = distill_bundle::referenced_types(&bundle);
        bundle
            .types
            .retain(|_, ty| used_types.contains(&ty.type_uuid));
        let used_schemas = distill_bundle::referenced_schema_hashes(&bundle);
        bundle.schemas.retain(|hash, _| used_schemas.contains(hash));
        bundle.format_version = distill_bundle::format_version_for(&bundle.assets);
        let proposed = if bundle.assets.is_empty() {
            None
        } else {
            Some(distill_bundle::write_bundle(&bundle).map_err(invalid)?)
        };
        Ok(PlannedBundleMutation {
            root,
            path,
            target,
            preimage,
            proposed,
        })
    }

    fn apply_set(
        &self,
        store: &StoreReader,
        compiled: &Compiled,
        bundle: &mut Bundle,
        entry: &AuthoringEntry,
        force_lossy: bool,
    ) -> Result<(), RpcFailure> {
        // Directory import rules are the one human-owned control type
        // (newgameplus GAP 23 §12.1): authoring-only, under the sealed
        // schema (the bundle writer checks it), a value the importer reads.
        let rules = entry.type_uuid == DIRECTORY_IMPORT_RULES_TYPE_UUID;
        if entry.local_id.starts_with('$') || (is_bootstrap_control_type(entry.type_uuid) && !rules)
        {
            return Err(invalid(
                "daemon-owned control entries cannot be written through direct CRUD",
            ));
        }
        if rules && entry.role != AuthoringEntryRole::AuthoringOnly {
            return Err(invalid("DirectoryImportRules entries are authoring-only"));
        }
        if entry.terminal_type != entry.type_uuid {
            return Err(invalid(
                "direct authored entries must carry their authored type as terminal_type",
            ));
        }
        if let Some(existing) = store.entry(entry.uuid).map_err(invalid)? {
            if existing.bundle != entry.bundle || existing.local_id != entry.local_id {
                return Err(invalid(
                    "direct write cannot move an existing asset identity; use rename-with-fixups",
                ));
            }
        }
        if bundle
            .assets
            .get(&entry.local_id)
            .is_some_and(|existing| existing.uuid != entry.uuid)
        {
            return Err(invalid(format!(
                "local ID {:?} is already owned by another asset",
                entry.local_id
            )));
        }

        // The capnp transport verified the schema against its hash; an
        // in-process caller hands it over as built.
        if distill_schema::ngp_schema::node_hash(&entry.schema.root).map_err(invalid)? != entry.schema_hash {
            return Err(invalid("the entry's schema does not hash to its schema hash"));
        }
        let schema = &*entry.schema;
        // A blob named by reference keeps the bytes the bundle holds under it
        // (§17 "Authoring snapshots"): an edit of a value with a blob
        // field re-sends no bytes.
        let resolved;
        let value = if names_blob_ref(&entry.value.value) {
            let mut held = BTreeMap::new();
            for existing in bundle.assets.values() {
                value_blobs(&existing.data, &mut held);
            }
            resolved = resolve_blob_refs(&entry.value, &held)
                .map_err(|error| invalid(format!("invalid authored value: {error:?}")))?;
            &resolved
        } else {
            &entry.value
        };
        let (data, types) = stored_value(schema, value)
            .map_err(|error| invalid(format!("invalid authored value: {error:?}")))?;
        if !force_lossy {
            if let Some(existing) = bundle.assets.get(&entry.local_id) {
                let edges = self.type_root_edges(store)?;
                self.check_lossless(compiled, bundle, existing, schema, entry, &edges)?;
                check_converted_types(bundle, existing, &types, entry, &edges)?;
            }
        }
        bundle.schemas.insert(entry.schema_hash, schema.clone());
        // The written types replace the bundle's by path and UUID; an entry
        // this write leaves naming one must still conform (the bundle write
        // checks it).
        for ty in types.types() {
            let schema_hash = distill_schema::ngp_schema::node_hash(&ty.schema.root).map_err(invalid)?;
            bundle
                .types
                .retain(|path, existing| *path != ty.path && existing.type_uuid != ty.uuid);
            bundle.types.insert(
                ty.path.clone(),
                distill_bundle::BundleType {
                    type_uuid: ty.uuid,
                    schema_hash,
                },
            );
            bundle.schemas.insert(schema_hash, (*ty.schema).clone());
        }
        if rules {
            crate::importer::validate_directory_rules(&data, &types)?;
        }
        bundle.assets.insert(
            entry.local_id.clone(),
            AssetEntry {
                uuid: entry.uuid,
                type_uuid: entry.type_uuid,
                schema_hash: entry.schema_hash,
                authoring_only: entry.role == AuthoringEntryRole::AuthoringOnly,
                data,
            },
        );
        Ok(())
    }

    /// A write replacing an entry stored under another schema must not drop
    /// its data: every field the automatic plan from the stored schema to
    /// the written one drops must hold its default. A schema change the
    /// planner refuses needs an edge of the game module (newgameplus GAP 29
    /// §8) covering it, which the type-files root lists ([`TypeRootEdges`]).
    fn check_lossless(
        &self,
        compiled: &Compiled,
        bundle: &Bundle,
        existing: &AssetEntry,
        written: &distill_schema::ngp_schema::LogicalSchema,
        entry: &AuthoringEntry,
        edges: &TypeRootEdges,
    ) -> Result<(), RpcFailure> {
        let written_hash = entry.schema_hash;
        if existing.schema_hash == written_hash {
            return Ok(());
        }
        let stored = bundle
            .schemas
            .get(&existing.schema_hash)
            .ok_or_else(|| invalid("the stored entry's schema snapshot is missing"))?;
        let lossy = |fields: Vec<String>, detail: String| RpcFailure::LossyWrite {
            type_uuid: existing.type_uuid,
            asset: existing.uuid,
            fields,
            detail,
        };
        let renames = current_renames(compiled, existing.type_uuid, written_hash);
        match plan_with_edges(&stored.root, &written.root, &renames, edges) {
            Ok(ops) => {
                if uses_edge(&ops) {
                    require_converted_from(bundle, existing, entry)?;
                }
                let fields = lossy_drops(&ops, &stored.root, &existing.data);
                if fields.is_empty() {
                    Ok(())
                } else {
                    Err(lossy(
                        fields,
                        "the written schema drops fields that hold data".to_owned(),
                    ))
                }
            }
            Err(refusal) => {
                Err(lossy(
                    refusal
                        .reasons
                        .iter()
                        .map(|(path, _)| path.clone())
                        .collect(),
                    format!("no edge covers the change and {refusal}"),
                ))
            }
        }
    }
}

impl AuthoringService {
    /// The edges file of the root holding the type files; none without one.
    fn type_root_edges(&self, store: &StoreReader) -> Result<TypeRootEdges, RpcFailure> {
        let rows = store.observed_files_at(prefab_types::MARKER).map_err(invalid)?;
        let Some(root) = rows.into_iter().next().map(|row| row.root_name) else {
            return Ok(TypeRootEdges::default());
        };
        if store.file_content_hash(&root, prefab_types::EDGES).map_err(invalid)?.is_none() {
            return Ok(TypeRootEdges::default());
        }
        let bytes = AuthoringBackend::read_file(self, store, &root, prefab_types::EDGES).map_err(invalid)?;
        let rows = prefab_types::read_edges(&bytes)
            .map_err(|error| invalid(format!("type file {}: {error}", prefab_types::EDGES)))?;
        Ok(TypeRootEdges(rows.into_iter().map(|row| (row.from_hash, row.to_hash)).collect()))
    }
}

/// The renamed fields of the type's schema at `compiled`, when the write is
/// under it.
fn current_renames(
    compiled: &Compiled,
    type_uuid: distill_core::id::TypeUuid,
    written_hash: LogicalHash,
) -> distill_schema::ngp_schema::Renames {
    compiled
        .schema_authority()
        .and_then(|authority| {
            let project = authority.project_type(type_uuid)?;
            (project.logical_hash == written_hash).then(|| project.renamed_from.clone())
        })
        .unwrap_or_default()
}

/// The game module's edges as the type-files root lists them
/// (`prefab_types::EDGES`): pairs of value-projection hashes. The daemon
/// checks that an edge covers a change; the engine runs it.
/// A write that replaces stored values through a game edge must name the
/// stored entry's value it converted (`AuthoringEntry::converted_from`, its
/// content key): the daemon cannot run the edge, so it refuses a write that
/// did not start from what is stored (a blind write, a reset). The
/// conversion itself is trusted: edges are game code (newgameplus GAP 29
/// §8).
fn require_converted_from(bundle: &Bundle, existing: &AssetEntry, entry: &AuthoringEntry) -> Result<(), RpcFailure> {
    let stored = bundle
        .schemas
        .get(&existing.schema_hash)
        .ok_or_else(|| invalid("the stored entry's schema snapshot is missing"))?;
    let types = bundle.type_table().map_err(|error| invalid(error.to_string()))?;
    let key = distill_rpc::content_key(stored, &existing.data, &types).map_err(invalid)?;
    if entry.converted_from == Some(key) {
        return Ok(());
    }
    Err(RpcFailure::LossyWrite {
        type_uuid: existing.type_uuid,
        asset: existing.uuid,
        fields: vec!["$".to_owned()],
        detail: match entry.converted_from {
            None => "the write replaces stored values through a game edge and names no converted value (converted_from)".to_owned(),
            Some(_) => "the write converted another value than the one stored (converted_from does not name it)".to_owned(),
        },
    })
}

/// [`require_converted_from`] for the dynamic types a write carries (a
/// prefab's components): a type the stored value names under another
/// schema than the written one, whose migration runs an edge.
fn check_converted_types(
    bundle: &Bundle,
    existing: &AssetEntry,
    written: &distill_schema::ngp_schema::TypeTable,
    entry: &AuthoringEntry,
    edges: &TypeRootEdges,
) -> Result<(), RpcFailure> {
    let mut named = BTreeSet::new();
    distill_schema::ngp_schema::dynamic_type_uuids(&existing.data, &mut named);
    for ty in written.types() {
        if !named.contains(&ty.uuid) {
            continue;
        }
        let Some(old) = bundle.types.values().find(|t| t.type_uuid == ty.uuid) else { continue };
        let new_hash = distill_schema::ngp_schema::node_hash(&ty.schema.root).map_err(invalid)?;
        if old.schema_hash == new_hash {
            continue;
        }
        let Some(old_schema) = bundle.schemas.get(&old.schema_hash) else { continue };
        if plan_with_edges(&old_schema.root, &ty.schema.root, &ty.renames, edges).is_ok_and(|ops| uses_edge(&ops)) {
            require_converted_from(bundle, existing, entry)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct TypeRootEdges(BTreeSet<(LogicalHash, LogicalHash)>);

impl Edges for TypeRootEdges {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn has(&self, from: &LogicalHash, to: &LogicalHash) -> bool {
        self.0.contains(&(*from, *to))
    }

    fn convert(&self, _: &LogicalHash, _: &LogicalHash, _: &Patch) -> Result<Value, String> {
        Err("the daemon checks edges; the game module runs them".to_owned())
    }
}

impl AuthoringBackend for AuthoringService {
    fn read_file(&self, snapshot: &StoreReader, root: &str, path: &str) -> Result<Vec<u8>, String> {
        let compiled = self
            .compiled(snapshot)
            .map_err(|error| format!("{error:?}"))?;
        let scanner = compiled.scanner();
        let physical = scanner
            .physical_path(root, path)
            .map_err(|error| error.to_string())?;
        scanner
            .read_identity_checked(&physical)
            .map_err(|error| error.to_string())
    }

    fn pipeline_failure(
        &self,
        snapshot: &StoreReader,
    ) -> Result<Option<PipelineFailure>, RpcFailure> {
        // The version's compiled state holds its candidate's failure, and
        // its epoch the runtime failure it latched.
        Ok(self.compiled(snapshot)?.pipeline_failure())
    }

    fn write_files(
        &self,
        store: &mut Store,
        base: InputVersion,
        operations: &[AuthoringOp],
        force_lossy: bool,
    ) -> Result<WriteReceipt, RpcFailure> {
        self.write_direct(store, base, operations, force_lossy)
    }

    fn prepare_import(
        &self,
        store: &mut Store,
        base: InputVersion,
        request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        if let Some(bundle) = self.unchanged_import_request(store, base, request)? {
            return Ok(PreparedImportCommit {
                bundle,
                commit: Commit::default(),
            });
        }
        self.prepare_import_request(store, base, request)
    }

    fn prepare_reimport(
        &self,
        store: &mut Store,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        self.prepare_reimport_bundle(store, base, bundle)
    }

    fn run_import(
        self: Arc<Self>,
        base: InputVersion,
        request: ImportRequest,
    ) -> Result<ImportJob, RpcFailure> {
        let unchanged = {
            let snapshot = self.snapshot()?;
            self.unchanged_import_request(&snapshot, base, &request)?
        };
        if let Some(bundle) = unchanged {
            return Ok(Box::new(move |_| {
                Ok(Ok(PreparedImportCommit {
                    bundle,
                    commit: Commit::default(),
                }))
            }));
        }
        let run = self.run_import_request(base, &request)?;
        Ok(Box::new(move |store| {
            self.publish_import_run(store, base, run)
        }))
    }

    fn run_reimport(
        self: Arc<Self>,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<ImportJob, RpcFailure> {
        let run = self.run_reimport_bundle(base, bundle)?;
        Ok(Box::new(move |store| {
            self.publish_import_run(store, base, run)
        }))
    }

    fn prepare_operation(
        &self,
        store: &mut Store,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        self.prepare_long_operation(store, base, operation)
    }

    fn type_name(&self, snapshot: &StoreReader, type_uuid: distill_core::id::TypeUuid) -> Option<String> {
        let authority = self.compiled(snapshot).ok()?.schema_authority()?;
        let project = authority.project_type(type_uuid)?;
        let def = authority.schema().types.get(project.schema_type.0)?;
        Some(def.path.display_path())
    }

    fn source_files(
        &self,
        snapshot: &StoreReader,
        root: &str,
        glob: Option<&str>,
    ) -> Result<Vec<distill_rpc::SourceFile>, RpcFailure> {
        AuthoringService::source_files(self, snapshot, root, glob)
    }

    fn run_release(
        self: Arc<Self>,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<ImportJob, RpcFailure> {
        let run = self.run_release_bundle(base, bundle)?;
        Ok(Box::new(move |store| {
            self.publish_import_run(store, base, run)
        }))
    }

    fn authorable(&self, snapshot: &StoreReader) -> Result<distill_rpc::Authorable, RpcFailure> {
        authorable(&*self.compiled(snapshot)?)
    }
}

/// What can be created under `compiled` (§17 "Creating"): the pipeline's
/// authored types under the served schema, `DirectoryImportRules`, and
/// the registered importers.
fn authorable(compiled: &Compiled) -> Result<distill_rpc::Authorable, RpcFailure> {
    use distill_rpc::{AuthorableImporter, AuthorableType, TypeDefault};
    use distill_schema::ngp_schema::{node_from_bytes, node_hash, TypeTable};

    let epoch = compiled.pipeline_epoch().map_err(|failure| {
        RpcFailure::PipelineUnavailable(Box::new(distill_rpc::PipelineUnavailableDiagnostic::PipelineFailure(
            failure.clone(),
        )))
    })?;
    let authority = compiled.schema_authority();
    let path_of = |type_uuid| {
        let authority = authority.as_ref()?;
        let project = authority.project_type(type_uuid)?;
        Some(authority.schema().types.get(project.schema_type.0)?.path.display_path())
    };
    let no_types = TypeTable::default();

    let mut types = Vec::new();
    for descriptor in epoch.authored_types() {
        // The epoch's bound check put every authored type in the schema.
        let project = authority
            .as_ref()
            .and_then(|authority| authority.project_type(descriptor.type_uuid))
            .ok_or_else(|| invalid(format!("authored type {} is not in the served schema", descriptor.type_uuid)))?;
        let default = if descriptor.logical_hash != project.logical_hash {
            TypeDefault::SchemaMismatch { written: descriptor.logical_hash }
        } else {
            match distill_rpc::authoring_value(&descriptor.default, &no_types).and_then(|value| {
                distill_rpc::check_authoring_value(&project.logical_schema, &value).map_err(|error| error.to_string())?;
                Ok(value)
            }) {
                Ok(value) => TypeDefault::Value(value),
                Err(error) => TypeDefault::Invalid(error),
            }
        };
        types.push(AuthorableType {
            type_uuid: descriptor.type_uuid,
            path: path_of(descriptor.type_uuid).unwrap_or_default(),
            extension: descriptor.extension,
            local_id: descriptor.local_id,
            doc: descriptor.doc,
            authoring_only: false,
            schema_hash: project.logical_hash,
            schema: Arc::new(project.logical_schema.clone()),
            default,
        });
    }
    let rules = distill_core::bootstrap::BootstrapControlSpecV1::embedded()
        .map_err(|error| invalid(format!("{error:?}")))?
        .0
        .into_iter()
        .find(|row| row.symbol == distill_core::bootstrap::BootstrapControlSymbol::DirectoryImportRules)
        .ok_or_else(|| invalid("the bootstrap spec has no DirectoryImportRules"))?;
    let rules_schema = node_from_bytes(&rules.logical_schema).map_err(|error| invalid(format!("{error:?}")))?;
    types.push(AuthorableType {
        type_uuid: DIRECTORY_IMPORT_RULES_TYPE_UUID,
        path: "distill::DirectoryImportRules".to_owned(),
        extension: "rules".to_owned(),
        local_id: "rules".to_owned(),
        doc: "Directory import rules: import the files a glob matches, each rule with an importer and settings"
            .to_owned(),
        authoring_only: true,
        schema_hash: node_hash(&rules_schema.root).map_err(|error| invalid(format!("{error:?}")))?,
        schema: Arc::new(rules_schema),
        default: TypeDefault::Absent,
    });
    types.sort_by(|a, b| a.extension.cmp(&b.extension));

    let default_rules = epoch.default_import_rules();
    let mut importers = Vec::new();
    for (id, importer) in compiled.pipeline_importers() {
        importers.push(AuthorableImporter {
            id: id.clone(),
            settings_type: importer.settings_type_uuid,
            settings_path: path_of(importer.settings_type_uuid).unwrap_or_default(),
            settings_hash: importer.settings_hash,
            settings_schema: Arc::new(importer.settings_schema.clone()),
            default_settings: distill_rpc::authoring_value(
                &importer.default_settings,
                &no_types,
            )
            .and_then(|value| {
                distill_rpc::check_authoring_value(&importer.settings_schema, &value).map_err(|error| error.to_string())?;
                Ok(value)
            })
            .map_err(|error| invalid(format!("importer {id:?} default settings: {error}")))?,
            default_globs: default_rules
                .iter()
                .filter(|rule| rule.importer == *id)
                .filter_map(|rule| rule.matches.path_glob.clone())
                .collect(),
        });
    }
    Ok(distill_rpc::Authorable { types, importers })
}

struct PlannedBundleMutation {
    /// The bundle's root name and normalized path, and its physical path.
    root: String,
    path: String,
    target: PathBuf,
    preimage: Option<ContentHash>,
    proposed: Option<Vec<u8>>,
}

pub(crate) fn require_base(store: &StoreReader, base: InputVersion) -> Result<(), RpcFailure> {
    let expected = store.input_version().map_err(crate::authoring::invalid)?;
    if expected != base {
        return Err(RpcFailure::StaleInputVersion {
            expected,
            got: base,
        });
    }
    Ok(())
}

fn infer_primary(bundle: &mut Bundle) -> Result<(), RpcFailure> {
    if bundle
        .primary
        .as_ref()
        .is_some_and(|primary| bundle.assets.contains_key(primary))
    {
        return Ok(());
    }
    let eligible = bundle
        .assets
        .iter()
        .filter_map(|(local_id, entry)| (!entry.authoring_only).then_some(local_id.clone()))
        .collect::<Vec<_>>();
    bundle.primary = match eligible.as_slice() {
        [] => None,
        [only] => Some(only.clone()),
        _ => {
            return Err(invalid(
                "removing the primary leaves multiple runtime entries; choose a replacement before removal",
            ))
        }
    };
    Ok(())
}

pub(crate) fn invalid(error: impl std::fmt::Display) -> RpcFailure {
    RpcFailure::InvalidAuthoringRequest {
        detail: error.to_string(),
    }
}

#[derive(Debug)]
pub enum AuthoringServiceInitError {
    Scan(ScanError),
}

impl std::fmt::Display for AuthoringServiceInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "authoring service initialization: {self:?}")
    }
}

impl std::error::Error for AuthoringServiceInitError {}

impl From<ScanError> for AuthoringServiceInitError {
    fn from(error: ScanError) -> Self {
        Self::Scan(error)
    }
}
