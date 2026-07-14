//! Durable production authoring backend.
//!
//! Direct CRUD is a filesystem publication, never an RPC-only projection:
//! validate the exact store basis, mutate one canonical bundle, journal the
//! inode transition, rescan the complete namespace, and return the commit for
//! that same durable successor version.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1, BUNDLE_FORMAT_VERSION};
use distill_core::attestation::is_bootstrap_control_type;
use distill_core::canonical::CanonicalEncoder;
use distill_core::id::{BundleUuid, ContentHash};
use distill_rpc::{
    decode_authoring_payload, AuthoringBackend, AuthoringEntry, AuthoringEntryRole, AuthoringOp,
    Commit, ImportRequest, InputVersion, LineageManifestClaimant, LineageRepairBackendError,
    LineageRepairInspection, LongRunningOp, PreparedImportCommit, PreparedOperationCommit,
    RpcFailure,
};
use distill_store::journal::{
    CreationRecoveryOutcome, DeletionRecoveryOutcome, JournalIntentPlan, PublicationGroupKind,
    RenameAsideOutcome,
};
use distill_store::Store;

use crate::coordinator::{publish_current_scan, LineageDestination};
use crate::importer::{RegisteredImporter, RegisteredImporters};
use crate::lineage_repair::{
    unique_sibling, write_same_dir_temp, LineageRepairBackend, LineageRepairBackendInitError,
};
use crate::pipeline_map::PipelineProjection;
use crate::quarantine::{QuarantineDriver, QuarantineError, QuarantineRoot};
use crate::scanner::{AssetRoot, RootedScanner, ScanError};

pub struct AuthoringService {
    pub(crate) store: Arc<Mutex<Store>>,
    pub(crate) scanner: RootedScanner,
    roots: RwLock<Vec<AssetRoot>>,
    quarantine: RwLock<QuarantineDriver>,
    lineage_destination: RwLock<LineageDestination>,
    lineage: RwLock<LineageRepairBackend>,
    pub(crate) builtin_importers: RwLock<RegisteredImporters>,
    pub(crate) pipeline_importers: RwLock<RegisteredImporters>,
    pipeline_projection: RwLock<PipelineProjection>,
}

pub(crate) struct AuthoringFilesystemCandidate {
    roots: Vec<AssetRoot>,
    scanner: RootedScanner,
    quarantine: QuarantineDriver,
    lineage_destination: LineageDestination,
    lineage: LineageRepairBackend,
}

impl AuthoringFilesystemCandidate {
    pub(crate) fn scanner(&self) -> &RootedScanner {
        &self.scanner
    }

    pub(crate) fn lineage_destination(&self) -> &LineageDestination {
        &self.lineage_destination
    }
}

impl AuthoringService {
    pub fn new(
        store: Arc<Mutex<Store>>,
        roots: Vec<AssetRoot>,
        lineage_destination: LineageDestination,
    ) -> Result<Self, AuthoringServiceInitError> {
        let scanner = RootedScanner::new(roots.clone())?;
        let quarantine = QuarantineDriver::new(
            roots
                .iter()
                .map(|root| QuarantineRoot::new(&root.path, &root.quarantine_dir)),
        )?;
        let lineage = LineageRepairBackend::new(Arc::clone(&store), roots.clone())?;
        Ok(Self {
            store,
            scanner,
            roots: RwLock::new(roots),
            quarantine: RwLock::new(quarantine),
            lineage_destination: RwLock::new(lineage_destination),
            lineage: RwLock::new(lineage),
            builtin_importers: RwLock::new(BTreeMap::new()),
            pipeline_importers: RwLock::new(BTreeMap::new()),
            pipeline_projection: RwLock::new(PipelineProjection::default()),
        })
    }

    pub(crate) fn pipeline_projection(&self) -> PipelineProjection {
        self.pipeline_projection
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn install_pipeline_projection(&self, projection: PipelineProjection) {
        *self
            .pipeline_projection
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = projection;
    }

    pub(crate) fn prepare_filesystem_candidate(
        &self,
        roots: Vec<AssetRoot>,
        lineage_destination: LineageDestination,
    ) -> Result<AuthoringFilesystemCandidate, AuthoringServiceInitError> {
        let scanner = RootedScanner::new(roots.clone())?;
        let quarantine = QuarantineDriver::new(
            roots
                .iter()
                .map(|root| QuarantineRoot::new(&root.path, &root.quarantine_dir)),
        )?;
        let lineage = LineageRepairBackend::new(Arc::clone(&self.store), roots.clone())?;
        Ok(AuthoringFilesystemCandidate {
            roots,
            scanner,
            quarantine,
            lineage_destination,
            lineage,
        })
    }

    pub(crate) fn install_filesystem_candidate(&self, candidate: AuthoringFilesystemCandidate) {
        self.scanner.replace_from(&candidate.scanner);
        *self
            .roots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = candidate.roots;
        *self
            .quarantine
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = candidate.quarantine;
        *self
            .lineage_destination
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = candidate.lineage_destination;
        *self
            .lineage
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = candidate.lineage;
    }

    pub(crate) fn roots_snapshot(&self) -> Vec<AssetRoot> {
        self.roots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn quarantine_snapshot(&self) -> QuarantineDriver {
        self.quarantine
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn lineage_destination_snapshot(&self) -> LineageDestination {
        self.lineage_destination
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn register_importer(
        &self,
        importer: Arc<dyn crate::importer::AuthoringImporter>,
    ) -> Result<(), RpcFailure> {
        let registered = RegisteredImporter::validate(importer)?;
        let mut importers = self
            .builtin_importers
            .write()
            .map_err(|_| invalid("importer registry lock is poisoned"))?;
        let pipeline = self
            .pipeline_importers
            .read()
            .map_err(|_| invalid("pipeline importer registry lock is poisoned"))?;
        if importers.contains_key(&registered.id) || pipeline.contains_key(&registered.id) {
            return Err(invalid(format!(
                "importer {:?} is already registered",
                registered.id
            )));
        }
        importers.insert(registered.id.clone(), registered);
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
        let builtins = self
            .builtin_importers
            .read()
            .map_err(|_| invalid("built-in importer registry lock is poisoned"))?;
        if let Some(id) = next.keys().find(|id| builtins.contains_key(*id)) {
            return Err(invalid(format!(
                "pipeline importer {id:?} conflicts with a built-in importer"
            )));
        }
        drop(builtins);
        Ok(next)
    }

    pub(crate) fn install_pipeline_importers(&self, next: RegisteredImporters) {
        *self
            .pipeline_importers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
    }

    fn prepare_direct_write(
        &self,
        base: InputVersion,
        operations: &[AuthoringOp],
    ) -> Result<Commit, RpcFailure> {
        let store = self.lock_store()?;
        require_base(&store, base)?;
        let planned = self.plan_bundle_mutation(&store, operations)?;
        drop(store);
        self.publish_file(
            base,
            PublicationGroupKind::AuthoringWrite,
            &encode_write_basis(base, operations),
            planned.target,
            planned.preimage,
            planned.proposed,
        )
    }

    pub(crate) fn publish_file(
        &self,
        base: InputVersion,
        kind: PublicationGroupKind,
        basis: &[u8],
        target: PathBuf,
        preimage: Option<ContentHash>,
        proposed: Option<Vec<u8>>,
    ) -> Result<Commit, RpcFailure> {
        let mut store = self.lock_store()?;
        require_base(&store, base)?;
        let temp = proposed
            .as_ref()
            .map(|bytes| write_same_dir_temp(&target, bytes).map_err(invalid))
            .transpose()?;
        let proposed_hash = proposed.as_ref().map_or_else(empty_hash, |bytes| {
            ContentHash(*blake3::hash(bytes).as_bytes())
        });
        let plan = JournalIntentPlan {
            target_path: path_text(&target)?,
            temp_path: temp
                .as_deref()
                .map(path_text)
                .transpose()?
                .unwrap_or_default(),
            conflict_path: path_text(&unique_sibling(&target, "conflict"))?,
            pre_image_hash: preimage,
            proposed_hash,
        };

        let quarantine = self.quarantine_snapshot();
        let mut publication = match quarantine.admit_publication(&mut store) {
            Ok(publication) => publication,
            Err(error) => {
                remove_unjournaled_temp(temp.as_deref());
                return Err(invalid(error));
            }
        };
        let group = match publication.record_group(kind, basis, &[plan]) {
            Ok(group) => group,
            Err(error) => {
                remove_unjournaled_temp(temp.as_deref());
                return Err(invalid(error));
            }
        };
        let intent = group.child_intents[0];
        let installed = match (preimage, proposed.as_ref()) {
            (Some(_), Some(_)) => publication
                .resume_group_replace(intent, &target)
                .map(|outcome| outcome == RenameAsideOutcome::Installed),
            (Some(_), None) => publication
                .resume_group_delete(intent, &target)
                .map(|outcome| outcome == DeletionRecoveryOutcome::Deleted),
            (None, Some(_)) => publication
                .resume_group_create(intent)
                .map(|outcome| outcome == CreationRecoveryOutcome::Installed),
            (None, None) => unreachable!("a new empty bundle is never planned"),
        }
        .map_err(invalid)?;
        publication.retire_group(group.group_id).map_err(invalid)?;
        if !installed {
            return Err(invalid(
                "authoring publication raced with an external edit; every observed inode was preserved",
            ));
        }
        drop(publication);
        drop(store);

        publish_current_scan(
            &self.scanner,
            &self.lineage_destination_snapshot(),
            &self.store,
            base,
            &self.pipeline_projection(),
        )
        .map_err(invalid)
    }

    fn plan_bundle_mutation(
        &self,
        store: &Store,
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
        let used_schemas = bundle
            .assets
            .values()
            .map(|entry| entry.schema_hash)
            .collect::<BTreeSet<_>>();
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
        store: &Store,
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
        let lineage = match bundle.assets.get(&entry.local_id) {
            Some(existing)
                if existing.uuid == entry.uuid
                    && existing.type_uuid == entry.type_uuid
                    && existing.schema_hash == entry.schema_hash =>
            {
                existing.lineage.clone()
            }
            _ => EntryLineageV1::Manifest(
                store
                    .current_lineage_stamp(entry.type_uuid)
                    .map_err(invalid)?
                    .filter(|stamp| stamp.selected_digest() == Some(entry.schema_hash))
                    .ok_or_else(|| {
                        invalid(
                            "authored schema is not the accepted current lineage cursor for its type",
                        )
                    })?,
            ),
        };
        bundle.schemas.insert(entry.schema_hash, schema);
        bundle.assets.insert(
            entry.local_id.clone(),
            AssetEntry {
                uuid: entry.uuid,
                type_uuid: entry.type_uuid,
                schema_hash: entry.schema_hash,
                lineage,
                authoring_only: entry.role == AuthoringEntryRole::AuthoringOnly,
                data,
            },
        );
        Ok(())
    }

    fn lock_store(&self) -> Result<MutexGuard<'_, Store>, RpcFailure> {
        self.store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))
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

    fn prepare_operation(
        &self,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        self.prepare_long_operation(base, operation)
    }

    fn prepare_create_missing_lineage(
        &self,
        basis: &LineageRepairInspection,
        canonical_manifest_bundle: &[u8],
    ) -> Result<Commit, LineageRepairBackendError> {
        self.lineage
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare_create_missing_lineage(basis, canonical_manifest_bundle)
    }

    fn prepare_resolve_duplicate_lineage(
        &self,
        basis: &LineageRepairInspection,
        survivor: &LineageManifestClaimant,
    ) -> Result<Commit, LineageRepairBackendError> {
        self.lineage
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare_resolve_duplicate_lineage(basis, survivor)
    }
}

struct PlannedBundleMutation {
    target: PathBuf,
    preimage: Option<ContentHash>,
    proposed: Option<Vec<u8>>,
}

pub(crate) fn require_base(store: &Store, base: InputVersion) -> Result<(), RpcFailure> {
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

fn encode_write_basis(base: InputVersion, operations: &[AuthoringOp]) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.u64(base.0);
    encoder.u32(operations.len() as u32);
    for operation in operations {
        match operation {
            AuthoringOp::Set(entry) => {
                encoder.u8(1);
                encoder.raw(&entry.uuid.0);
                encoder.raw(&entry.bundle.0);
                encoder.str(&entry.local_id);
                encoder.raw(&entry.schema_hash.0);
            }
            AuthoringOp::Remove { uuid } => {
                encoder.u8(2);
                encoder.raw(&uuid.0);
            }
        }
    }
    encoder.into_bytes()
}

fn path_text(path: &Path) -> Result<String, RpcFailure> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        invalid(format!(
            "journal path is not lossless UTF-8: {}",
            path.display()
        ))
    })
}

fn remove_unjournaled_temp(temp: Option<&Path>) {
    if let Some(temp) = temp {
        let _ = fs::remove_file(temp);
    }
}

fn empty_hash() -> ContentHash {
    ContentHash(*blake3::hash(b"").as_bytes())
}

pub(crate) fn invalid(error: impl std::fmt::Display) -> RpcFailure {
    RpcFailure::InvalidAuthoringRequest {
        detail: error.to_string(),
    }
}

#[derive(Debug)]
pub enum AuthoringServiceInitError {
    Scan(ScanError),
    Quarantine(QuarantineError),
    Lineage(LineageRepairBackendInitError),
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

impl From<QuarantineError> for AuthoringServiceInitError {
    fn from(error: QuarantineError) -> Self {
        Self::Quarantine(error)
    }
}

impl From<LineageRepairBackendInitError> for AuthoringServiceInitError {
    fn from(error: LineageRepairBackendInitError) -> Self {
        Self::Lineage(error)
    }
}
