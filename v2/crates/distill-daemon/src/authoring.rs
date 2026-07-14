//! Durable production authoring backend.
//!
//! Direct CRUD is a filesystem publication, never an RPC-only projection:
//! validate the exact store basis, mutate one canonical bundle, journal the
//! inode transition, rescan the complete namespace, and return the commit for
//! that same durable successor version.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

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
use crate::lineage_repair::{
    unique_sibling, write_same_dir_temp, LineageRepairBackend, LineageRepairBackendInitError,
};
use crate::quarantine::{QuarantineDriver, QuarantineError, QuarantineRoot};
use crate::scanner::{AssetRoot, RootedScanner, ScanError};

pub struct AuthoringService {
    store: Arc<Mutex<Store>>,
    roots: Vec<AssetRoot>,
    scanner: RootedScanner,
    quarantine: QuarantineDriver,
    lineage_destination: LineageDestination,
    lineage: LineageRepairBackend,
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
            roots,
            scanner,
            quarantine,
            lineage_destination,
            lineage,
        })
    }

    fn prepare_direct_write(
        &self,
        base: InputVersion,
        operations: &[AuthoringOp],
    ) -> Result<Commit, RpcFailure> {
        let mut store = self.lock_store()?;
        require_base(&store, base)?;
        let planned = self.plan_bundle_mutation(&store, operations)?;

        let temp = planned
            .proposed
            .as_ref()
            .map(|bytes| write_same_dir_temp(&planned.target, bytes).map_err(invalid))
            .transpose()?;
        let proposed_hash = planned.proposed.as_ref().map_or_else(empty_hash, |bytes| {
            ContentHash(*blake3::hash(bytes).as_bytes())
        });
        let plan = JournalIntentPlan {
            target_path: path_text(&planned.target)?,
            temp_path: temp
                .as_deref()
                .map(path_text)
                .transpose()?
                .unwrap_or_default(),
            conflict_path: path_text(&unique_sibling(&planned.target, "conflict"))?,
            pre_image_hash: planned.preimage,
            proposed_hash,
        };

        let mut publication = match self.quarantine.admit_publication(&mut store) {
            Ok(publication) => publication,
            Err(error) => {
                remove_unjournaled_temp(temp.as_deref());
                return Err(invalid(error));
            }
        };
        let group = match publication.record_group(
            PublicationGroupKind::AuthoringWrite,
            &encode_write_basis(base, operations),
            &[plan],
        ) {
            Ok(group) => group,
            Err(error) => {
                remove_unjournaled_temp(temp.as_deref());
                return Err(invalid(error));
            }
        };
        let intent = group.child_intents[0];
        let installed = match (planned.preimage, planned.proposed.as_ref()) {
            (Some(_), Some(_)) => publication
                .resume_group_replace(intent, &planned.target)
                .map(|outcome| outcome == RenameAsideOutcome::Installed),
            (Some(_), None) => publication
                .resume_group_delete(intent, &planned.target)
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

        publish_current_scan(&self.scanner, &self.lineage_destination, &self.store, base)
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
            let [root] = self.roots.as_slice() else {
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
        _base: InputVersion,
        _request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "import".into(),
        })
    }

    fn prepare_reimport(
        &self,
        _base: InputVersion,
        _bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "reimport".into(),
        })
    }

    fn prepare_operation(
        &self,
        _base: InputVersion,
        _operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "operation".into(),
        })
    }

    fn prepare_create_missing_lineage(
        &self,
        basis: &LineageRepairInspection,
        canonical_manifest_bundle: &[u8],
    ) -> Result<Commit, LineageRepairBackendError> {
        self.lineage
            .prepare_create_missing_lineage(basis, canonical_manifest_bundle)
    }

    fn prepare_resolve_duplicate_lineage(
        &self,
        basis: &LineageRepairInspection,
        survivor: &LineageManifestClaimant,
    ) -> Result<Commit, LineageRepairBackendError> {
        self.lineage
            .prepare_resolve_duplicate_lineage(basis, survivor)
    }
}

struct PlannedBundleMutation {
    target: PathBuf,
    preimage: Option<ContentHash>,
    proposed: Option<Vec<u8>>,
}

fn require_base(store: &Store, base: InputVersion) -> Result<(), RpcFailure> {
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

fn invalid(error: impl std::fmt::Display) -> RpcFailure {
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
