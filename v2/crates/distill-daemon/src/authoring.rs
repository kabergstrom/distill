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
use distill_core::bootstrap::is_bootstrap_control_type;
use distill_core::id::{BundleUuid, ContentHash, LogicalHash};
use distill_migrate::{lossy_drops, plan_automatic_renamed};
use distill_pipeline_api::callbacks::MigrationKey;
use distill_rpc::{
    decode_authoring_payload, AuthoringBackend, AuthoringEntry, AuthoringEntryRole, AuthoringOp,
    Commit, ImportJob, ImportRequest, InputVersion, LongRunningOp, PreparedImportCommit,
    PreparedOperationCommit, RpcFailure, WriteReceipt, WrittenFile,
};
use distill_store::state::{PipelineFailure, PipelineFailureOrigin};
use distill_store::{Current, Store, StoreOpener, StoreReader};

use distill_store::atomic_file;
use crate::compiled::{Compiled, CompiledRegistry};
use crate::coordinator::publish_incremental_paths;
use crate::importer::{RegisteredImporter, RegisteredImporters};
use crate::scanner::ScanError;

pub struct AuthoringService {
    /// Opens the reader an import run reads through when it runs outside
    /// any write.
    pub(crate) opener: Arc<StoreOpener>,
    /// The daemon's own importers. The pipeline's are compiled state.
    builtin_importers: Current<RegisteredImporters>,
    /// The compiled configuration state of each live store version: every
    /// operation reads the roots, importers, schema and pipeline of the
    /// version its own transaction sees (`crate::compiled`).
    compiled: Arc<CompiledRegistry>,
    tag_index_coordinator: OnceLock<Weak<crate::coordinator::DaemonCoordinator>>,
}

impl AuthoringService {
    pub(crate) fn new(
        opener: Arc<StoreOpener>,
        compiled: Arc<CompiledRegistry>,
    ) -> Self {
        Self {
            opener,
            builtin_importers: Current::new(RegisteredImporters::new()),
            compiled,
            tag_index_coordinator: OnceLock::new(),
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

    /// The daemon's own importers.
    pub(crate) fn builtin_importers(&self) -> Arc<RegisteredImporters> {
        self.builtin_importers.load()
    }

    pub fn register_importer(
        &self,
        importer: Arc<dyn crate::importer::AuthoringImporter>,
    ) -> Result<(), RpcFailure> {
        let registered = RegisteredImporter::validate(importer)?;
        let pipeline_has = self
            .compiled
            .latest()
            .is_some_and(|compiled| compiled.pipeline_importers().contains_key(&registered.id));
        let mut duplicate = pipeline_has;
        self.builtin_importers.update(|current| {
            duplicate |= current.contains_key(&registered.id);
            let mut next = RegisteredImporters::clone(current);
            if !duplicate {
                next.insert(registered.id.clone(), registered.clone());
            }
            next
        });
        if duplicate {
            return Err(invalid(format!(
                "importer {:?} is already registered",
                registered.id
            )));
        }
        Ok(())
    }

    /// Validate a pipeline epoch's importers as its compiled state holds
    /// them: a built-in id is the built-in's.
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
        let builtins = self.builtin_importers.load();
        if let Some(id) = next.keys().find(|id| builtins.contains_key(*id)) {
            return Err(invalid(format!(
                "pipeline importer {id:?} conflicts with a built-in importer"
            )));
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
                let root = compiled.scanner().root_containing(&target).map_err(invalid)?;
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
            let target = scanner
                .physical_path(&root, &meta.path)
                .map_err(invalid)?;
            let bytes = scanner
                .read_identity_checked(&target)
                .map_err(invalid)?;
            let observed = ContentHash(*blake3::hash(&bytes).as_bytes());
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
            let [root] = compiled.roots() else {
                return Err(invalid(
                    "direct bundle creation requires exactly one configured asset root; import supplies an explicit root",
                ));
            };
            let target = scanner
                .physical_path(&root.name, path)
                .map_err(invalid)?;
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
        let used_schemas = distill_bundle::referenced_schema_hashes(&bundle);
        bundle.schemas.retain(|hash, _| used_schemas.contains(hash));
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
        if entry.local_id.starts_with('$') || is_bootstrap_control_type(entry.type_uuid) {
            return Err(invalid(
                "daemon-owned control entries cannot be written through direct CRUD",
            ));
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

        let schema_text = std::str::from_utf8(&entry.logical_schema)
            .map_err(|_| invalid("logical schema is not UTF-8"))?;
        let schema = distill_schema::ngp_schema::verify_snapshot(schema_text, entry.schema_hash)
            .map_err(invalid)?;
        let data = decode_authoring_payload(entry.schema_hash, &entry.logical_schema, &entry.value)
            .map_err(|error| invalid(format!("invalid authored value: {error:?}")))?;
        if !force_lossy {
            if let Some(existing) = bundle.assets.get(&entry.local_id) {
                self.check_lossless(compiled, bundle, existing, &schema, entry.schema_hash)?;
            }
        }
        bundle.schemas.insert(entry.schema_hash, schema);
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
    /// planner refuses needs a registered migration function.
    fn check_lossless(
        &self,
        compiled: &Compiled,
        bundle: &Bundle,
        existing: &AssetEntry,
        written: &distill_schema::ngp_schema::LogicalSchema,
        written_hash: LogicalHash,
    ) -> Result<(), RpcFailure> {
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
        match plan_automatic_renamed(&stored.root, &written.root, &renames) {
            Ok(ops) => {
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
                let key = MigrationKey {
                    type_uuid: existing.type_uuid,
                    from: existing.schema_hash,
                    to: written_hash,
                };
                if migration_function_registered(compiled, &key) {
                    return Ok(());
                }
                Err(lossy(
                    refusal.reasons.iter().map(|(path, _)| path.clone()).collect(),
                    format!("no registered migration function and {refusal}"),
                ))
            }
        }
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

fn migration_function_registered(compiled: &Compiled, key: &MigrationKey) -> bool {
    compiled
        .pipeline_epoch()
        .is_ok_and(|epoch| epoch.migration_function_keys().contains(&key.id()))
}

impl AuthoringBackend for AuthoringService {
    fn read_file(
        &self,
        snapshot: &StoreReader,
        root: &str,
        path: &str,
    ) -> Result<Vec<u8>, String> {
        let compiled = self.compiled(snapshot).map_err(|error| format!("{error:?}"))?;
        let scanner = compiled.scanner();
        let physical = scanner.physical_path(root, path).map_err(|error| error.to_string())?;
        scanner
            .read_identity_checked(&physical)
            .map_err(|error| error.to_string())
    }

    fn pipeline_runtime_failure(
        &self,
        snapshot: &StoreReader,
    ) -> Result<Option<PipelineFailure>, RpcFailure> {
        // A candidate failure is the version's errors row; only the epoch
        // the snapshot serves can fail at runtime. A snapshot whose compiled
        // state is not loaded sees no epoch to report on.
        Ok(self
            .compiled(snapshot)?
            .pipeline_epoch()
            .err()
            .filter(|failure| failure.origin == PipelineFailureOrigin::PublishedRuntime))
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
        let run = self.run_import_request(base, &request)?;
        Ok(Box::new(move |store| self.publish_import_run(store, base, run)))
    }

    fn run_reimport(
        self: Arc<Self>,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<ImportJob, RpcFailure> {
        let run = self.run_reimport_bundle(base, bundle)?;
        Ok(Box::new(move |store| self.publish_import_run(store, base, run)))
    }

    fn prepare_operation(
        &self,
        store: &mut Store,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        self.prepare_long_operation(store, base, operation)
    }

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
