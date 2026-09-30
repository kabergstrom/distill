//! Durable production authoring backend.
//!
//! Direct CRUD is a filesystem publication, never an RPC-only projection:
//! validate the exact store basis, mutate one canonical bundle, write it
//! atomically, reobserve only the authored paths, and return the commit
//! for that same durable successor version.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock, Weak};

use arc_swap::ArcSwap;

use distill_bundle::{AssetEntry, Bundle, BUNDLE_FORMAT_VERSION};
use distill_core::bootstrap::is_bootstrap_control_type;
use distill_core::id::{BundleUuid, ContentHash};
use distill_rpc::{
    decode_authoring_payload, AuthoringBackend, AuthoringEntry, AuthoringEntryRole, AuthoringOp,
    Commit, ImportJob, ImportRequest, InputVersion, LongRunningOp, PreparedImportCommit,
    PreparedOperationCommit, RpcFailure,
};
use distill_store::StoreReader;

use crate::store_cell::{AuthorityStore, WriteGuard};
use crate::coordinator::publish_incremental_paths;
use crate::importer::{RegisteredImporter, RegisteredImporters};
use crate::atomic::{atomic_write_expecting, remove_expecting};
use crate::pipeline_map::PipelineProjection;
use crate::scanner::{AssetRoot, RootedScanner, ScanError};

pub struct AuthoringService {
    pub(crate) store: Arc<AuthorityStore>,
    pub(crate) scanner: RootedScanner,
    /// The asset roots and what hangs off them, replaced together when the
    /// configuration changes.
    filesystem: ArcSwap<AuthoringFilesystem>,
    importers: ArcSwap<Importers>,
    pipeline_projection: ArcSwap<PipelineProjection>,
    /// Whether the store's import index was built since it was last
    /// invalidated (see `importer`).
    pub(crate) import_index_ready: AtomicBool,
    tag_index_coordinator: OnceLock<Weak<crate::coordinator::DaemonCoordinator>>,
}

struct AuthoringFilesystem {
    roots: Vec<AssetRoot>,
}

/// Registered importers: the daemon's own, and the loaded pipeline's.
#[derive(Default, Clone)]
pub(crate) struct Importers {
    pub(crate) builtin: RegisteredImporters,
    pub(crate) pipeline: RegisteredImporters,
}

pub(crate) struct AuthoringFilesystemCandidate {
    scanner: RootedScanner,
    filesystem: AuthoringFilesystem,
}

impl AuthoringFilesystemCandidate {
    pub(crate) fn scanner(&self) -> &RootedScanner {
        &self.scanner
    }
}

impl AuthoringService {
    pub fn new(
        store: Arc<AuthorityStore>,
        roots: Vec<AssetRoot>,
        scanner: RootedScanner,
    ) -> Self {
        Self {
            store,
            scanner,
            filesystem: ArcSwap::from_pointee(AuthoringFilesystem { roots }),
            importers: ArcSwap::from_pointee(Importers::default()),
            pipeline_projection: ArcSwap::from_pointee(PipelineProjection::default()),
            import_index_ready: AtomicBool::new(false),
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

    pub(crate) fn pipeline_projection(&self) -> PipelineProjection {
        (**self.pipeline_projection.load()).clone()
    }

    pub(crate) fn install_pipeline_projection(&self, projection: PipelineProjection) {
        self.pipeline_projection.store(Arc::new(projection));
    }

    pub(crate) fn prepare_filesystem_candidate(
        &self,
        roots: Vec<AssetRoot>,
    ) -> Result<AuthoringFilesystemCandidate, AuthoringServiceInitError> {
        let scanner = self.scanner.candidate_with_roots(roots.clone())?;
        Ok(AuthoringFilesystemCandidate {
            scanner,
            filesystem: AuthoringFilesystem { roots },
        })
    }

    pub(crate) fn install_filesystem_candidate(&self, candidate: AuthoringFilesystemCandidate) {
        self.scanner.replace_from(&candidate.scanner);
        self.filesystem.store(Arc::new(candidate.filesystem));
    }

    pub(crate) fn roots_snapshot(&self) -> Vec<AssetRoot> {
        self.filesystem.load().roots.clone()
    }

    pub(crate) fn importers(&self) -> Arc<Importers> {
        self.importers.load_full()
    }

    pub fn register_importer(
        &self,
        importer: Arc<dyn crate::importer::AuthoringImporter>,
    ) -> Result<(), RpcFailure> {
        let registered = RegisteredImporter::validate(importer)?;
        let mut duplicate = false;
        self.importers.rcu(|current| {
            duplicate = current.builtin.contains_key(&registered.id)
                || current.pipeline.contains_key(&registered.id);
            let mut next = Importers::clone(current);
            if !duplicate {
                next.builtin
                    .insert(registered.id.clone(), registered.clone());
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

    pub(crate) fn replace_pipeline_importers(
        &self,
        importers: Vec<Arc<dyn crate::importer::AuthoringImporter>>,
    ) -> Result<(), RpcFailure> {
        let next = self.prepare_pipeline_importers(importers)?;
        self.install_pipeline_importers(next);
        Ok(())
    }

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
        let builtins = &self.importers.load().builtin;
        if let Some(id) = next.keys().find(|id| builtins.contains_key(*id)) {
            return Err(invalid(format!(
                "pipeline importer {id:?} conflicts with a built-in importer"
            )));
        }
        Ok(next)
    }

    /// Install the pipeline's importers. A built-in registered since they
    /// were prepared keeps its id: the pipeline's importer of that id is
    /// left out.
    pub(crate) fn install_pipeline_importers(&self, next: RegisteredImporters) {
        self.importers.rcu(|current| {
            let mut pipeline = next.clone();
            pipeline.retain(|id, _| !current.builtin.contains_key(id));
            Importers {
                builtin: current.builtin.clone(),
                pipeline,
            }
        });
    }


    fn prepare_direct_write(
        &self,
        base: InputVersion,
        operations: &[AuthoringOp],
    ) -> Result<Commit, RpcFailure> {
        let store = self.write_store()?;
        require_base(&store, base)?;
        let planned = self.plan_bundle_mutation(&store, operations)?;
        drop(store);
        self.publish_file(
            base,
            planned.target,
            planned.preimage,
            planned.proposed,
        )
    }

    /// Write or delete one bundle file, then publish the store rows for
    /// it. The target is re-hashed right before the change; if it no
    /// longer holds `preimage` the publication fails with a conflict.
    pub(crate) fn publish_file(
        &self,
        base: InputVersion,
        target: PathBuf,
        preimage: Option<ContentHash>,
        proposed: Option<Vec<u8>>,
    ) -> Result<Commit, RpcFailure> {
        let store = self.write_store()?;
        require_base(&store, base)?;
        match proposed.as_deref() {
            Some(bytes) => atomic_write_expecting(&target, bytes, preimage.into()),
            None => remove_expecting(&target, preimage.into()),
        }
        .map_err(invalid)?;
        drop(store);

        publish_incremental_paths(
            &self.scanner,
            std::slice::from_ref(&target),
            &self.store,
            base,
            &self.pipeline_projection(),
            self.tag_index_coordinator().as_deref(),
        )
        .map_err(invalid)
    }

    fn plan_bundle_mutation(
        &self,
        store: &StoreReader,
        operations: &[AuthoringOp],
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

        let (target, preimage, mut bundle) = if let Some(meta) =
            store.bundle(bundle_id).map_err(invalid)?
        {
            let root = store
                .root_name(meta.root)
                .map_err(invalid)?
                .ok_or_else(|| invalid("bundle root identity is missing"))?;
            let target = self
                .scanner
                .physical_path(&root, &meta.path)
                .map_err(invalid)?;
            let bytes = self
                .scanner
                .read_identity_checked(&target)
                .map_err(invalid)?;
            let observed = ContentHash(*blake3::hash(&bytes).as_bytes());
            if observed != meta.content_hash {
                return Err(invalid(format!(
                    "bundle {} changed since durable version {}",
                    meta.path,
                    store.input_version().0
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
            (target, Some(observed), bundle)
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
            let roots = self.roots_snapshot();
            let [root] = roots.as_slice() else {
                return Err(invalid(
                    "direct bundle creation requires exactly one configured asset root; import supplies an explicit root",
                ));
            };
            let target = self
                .scanner
                .physical_path(&root.name, path)
                .map_err(invalid)?;
            if fs::symlink_metadata(&target).is_ok() {
                return Err(invalid("new bundle destination already exists"));
            }
            (
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
            self.apply_set(store, &mut bundle, entry)?;
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
            target,
            preimage,
            proposed,
        })
    }

    fn apply_set(
        &self,
        store: &StoreReader,
        bundle: &mut Bundle,
        entry: &AuthoringEntry,
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

    fn write_store(&self) -> Result<WriteGuard<'_>, RpcFailure> {
        Ok(self.store.write())
    }
}

impl AuthoringBackend for AuthoringService {
    fn prepare_write(
        &self,
        base: InputVersion,
        operations: &[AuthoringOp],
    ) -> Result<Option<Commit>, RpcFailure> {
        self.prepare_direct_write(base, operations).map(Some)
    }

    fn prepare_import(
        &self,
        base: InputVersion,
        request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        self.prepare_import_request(base, request)
    }

    fn prepare_reimport(
        &self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        self.prepare_reimport_bundle(base, bundle)
    }

    fn run_import(
        self: Arc<Self>,
        base: InputVersion,
        request: ImportRequest,
    ) -> Result<ImportJob, RpcFailure> {
        let run = self.run_import_request(base, &request)?;
        Ok(Box::new(move || self.publish_import_run(base, run)))
    }

    fn run_reimport(
        self: Arc<Self>,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<ImportJob, RpcFailure> {
        let run = self.run_reimport_bundle(base, bundle)?;
        Ok(Box::new(move || self.publish_import_run(base, run)))
    }

    fn prepare_operation(
        &self,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        self.prepare_long_operation(base, operation)
    }

}

struct PlannedBundleMutation {
    target: PathBuf,
    preimage: Option<ContentHash>,
    proposed: Option<Vec<u8>>,
}

pub(crate) fn require_base(store: &StoreReader, base: InputVersion) -> Result<(), RpcFailure> {
    let expected = store.input_version();
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
