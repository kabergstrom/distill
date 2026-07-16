//! Production implementation of §6's deliberately narrow lineage repair.
//!
//! RPC owns the in-memory coordinator CAS. This backend repeats the exact
//! filesystem basis CAS, records a durable parent plus all child mutations,
//! executes only no-replace journal state machines, rescans, and advances the
//! durable store version before returning the Ready-only RPC commit.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::bootstrap::{is_bootstrap_control_type, SCHEMA_LINEAGE_MANIFEST_TYPE_UUID};
use distill_core::canonical::CanonicalEncoder;
use distill_core::id::{BundleFileHash, ContentHash};
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, Commit, ConfigurationStatus, ImportRequest, InputVersion,
    LineageManifestClaimant, LineageRepairBackendError, LineageRepairDestination,
    LineageRepairInspection, LineageRepairInvalid, LineageRepairInvalidCode, LineageRepairStale,
    LineageRepairStaleCode, LineageRepairState, LongRunningOp, PreparedImportCommit,
    PreparedOperationCommit, RpcFailure,
};
use distill_store::journal::{
    CreationRecoveryOutcome, DeletionRecoveryOutcome, JournalIntentPlan, PublicationGroupKind,
    RenameAsideOutcome,
};
use distill_store::Store;

use crate::quarantine::{PublicationDriver, QuarantineDriver, QuarantineError, QuarantineRoot};
use crate::scanner::{AssetRoot, RootedScanner, ScanError, ScanSnapshot};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub struct LineageRepairBackend {
    store: Arc<Mutex<Store>>,
    scanner: RootedScanner,
    quarantine: QuarantineDriver,
}

impl LineageRepairBackend {
    pub fn new(
        store: Arc<Mutex<Store>>,
        roots: impl IntoIterator<Item = AssetRoot>,
    ) -> Result<Self, LineageRepairBackendInitError> {
        let roots = roots.into_iter().collect::<Vec<_>>();
        let scanner = RootedScanner::new(roots.clone())?;
        let quarantine = QuarantineDriver::new(
            roots
                .iter()
                .map(|root| QuarantineRoot::new(&root.path, &root.quarantine_dir)),
        )?;
        Ok(Self {
            store,
            scanner,
            quarantine,
        })
    }

    fn reattest_lineage_claimants(
        &self,
        paths: &[PathBuf],
    ) -> Result<Vec<LineageManifestClaimant>, LineageRepairBackendError> {
        let baseline = ScanSnapshot::default();
        let observed = self
            .scanner
            .scan_incremental(&baseline, paths)
            .map_err(failure)?
            .unwrap_or(baseline);
        Ok(observed.lineage_claimants())
    }

    fn create_missing(
        &self,
        basis: &LineageRepairInspection,
        proposed_bytes: &[u8],
    ) -> Result<Commit, LineageRepairBackendError> {
        let LineageRepairState::Missing {
            configured_root,
            configured_path,
            destination,
        } = &basis.state
        else {
            return Err(invalid(
                LineageRepairInvalidCode::WrongBasisState,
                "createMissing requires a Missing basis",
            ));
        };
        let proposed = validate_manifest_bundle(proposed_bytes)?;
        let mut store = lock_store(&self.store)?;
        require_store_basis(&store, basis)?;
        let mut publication = self
            .quarantine
            .admit_publication(&mut store)
            .map_err(failure)?;
        let observed = self
            .scanner
            .inspect_destination(configured_root, configured_path)
            .map_err(failure)?;
        if &observed != destination {
            return Err(stale(basis, destination_stale_code(destination, &observed)));
        }
        let target = self
            .scanner
            .physical_path(configured_root, configured_path)
            .map_err(failure)?;
        if !self
            .reattest_lineage_claimants(std::slice::from_ref(&target))?
            .is_empty()
        {
            return Err(stale(basis, LineageRepairStaleCode::ClaimantChanged));
        }
        if let LineageRepairDestination::Occupied { kind, .. } = destination {
            if *kind == distill_rpc::OccupiedLineageDestinationKind::CanonicalBundle {
                let old_bytes = self
                    .scanner
                    .read_identity_checked(&target)
                    .map_err(failure)?;
                let old = distill_bundle::parse_bundle(&old_bytes).map_err(|error| {
                    failure(format!(
                        "occupied canonical destination no longer parses: {error}"
                    ))
                })?;
                validate_add_only_replacement(&old, &proposed.bundle, &proposed.local_id)?;
            }
        }

        let temp = plan_same_dir_temp(&target).map_err(failure)?;
        let proposed_hash = BundleFileHash::of_observed_bytes(proposed_bytes);
        let preimage = match destination {
            LineageRepairDestination::Absent => None,
            LineageRepairDestination::Occupied { file_hash, .. } => {
                let current = self
                    .scanner
                    .read_identity_checked(&target)
                    .map_err(failure)?;
                if BundleFileHash::of_observed_bytes(&current) != *file_hash {
                    let _ = fs::remove_file(&temp);
                    return Err(stale(basis, LineageRepairStaleCode::PreimageChanged));
                }
                Some(ContentHash(file_hash.0))
            }
        };
        let plan = JournalIntentPlan {
            target_path: path_text(&target)?,
            temp_path: path_text(&temp)?,
            conflict_path: path_text(&unique_sibling(&target, "conflict"))?,
            pre_image_hash: preimage,
            proposed_hash: ContentHash(proposed_hash.0),
        };

        let group = publication
            .record_group(
                PublicationGroupKind::LineageCreate,
                &encode_basis(basis),
                &[plan],
            )
            .map_err(failure)?;
        write_planned_temp(&temp, proposed_bytes).map_err(failure)?;
        let outcome = if preimage.is_none() {
            publication
                .resume_group_create(group.child_intents[0])
                .map(|outcome| outcome == CreationRecoveryOutcome::Installed)
        } else {
            publication
                .resume_group_replace(group.child_intents[0], &target)
                .map(|outcome| outcome == RenameAsideOutcome::Installed)
        }
        .map_err(failure)?;
        if !outcome {
            retire_if_terminal(&mut publication, group.group_id)?;
            return Err(stale(basis, LineageRepairStaleCode::PreimageChanged));
        }

        let expected = LineageManifestClaimant {
            root_name: configured_root.clone(),
            normalized_path: configured_path.clone(),
            bundle: proposed.bundle.uuid,
            local_id: proposed.local_id,
            asset: proposed.entry.uuid,
            file_hash: proposed_hash,
        };
        if self.reattest_lineage_claimants(std::slice::from_ref(&target))? != [expected] {
            retire_if_terminal(&mut publication, group.group_id)?;
            return Err(stale(basis, LineageRepairStaleCode::ClaimantChanged));
        }
        publication.retire_group(group.group_id).map_err(failure)?;
        drop(publication);
        publish_store_version(&mut store, basis)?;
        Ok(ready_commit())
    }

    fn resolve_duplicate(
        &self,
        basis: &LineageRepairInspection,
        survivor: &LineageManifestClaimant,
    ) -> Result<Commit, LineageRepairBackendError> {
        let LineageRepairState::Duplicate { claimants } = &basis.state else {
            return Err(invalid(
                LineageRepairInvalidCode::WrongBasisState,
                "resolveDuplicate requires a Duplicate basis",
            ));
        };
        if claimants.binary_search(survivor).is_err() {
            return Err(invalid(
                LineageRepairInvalidCode::SurvivorNotClaimant,
                "selected survivor is not an exact claimant",
            ));
        }
        let mut store = lock_store(&self.store)?;
        require_store_basis(&store, basis)?;
        let mut publication = self
            .quarantine
            .admit_publication(&mut store)
            .map_err(failure)?;
        let claimant_paths = claimants
            .iter()
            .map(|claimant| {
                self.scanner
                    .physical_path(&claimant.root_name, &claimant.normalized_path)
                    .map_err(failure)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if self.reattest_lineage_claimants(&claimant_paths)? != *claimants {
            return Err(stale(basis, LineageRepairStaleCode::ClaimantChanged));
        }

        let grouped = group_claimants(claimants)?;
        let mut mutations = Vec::new();
        let mut expected_survivor = survivor.clone();
        for ((root, path), physical_claimants) in grouped {
            let target = self.scanner.physical_path(&root, &path).map_err(failure)?;
            let bytes = self
                .scanner
                .read_identity_checked(&target)
                .map_err(failure)?;
            let file_hash = BundleFileHash::of_observed_bytes(&bytes);
            if physical_claimants
                .iter()
                .any(|claimant| claimant.file_hash != file_hash)
            {
                return Err(stale(basis, LineageRepairStaleCode::PreimageChanged));
            }
            let mut bundle = distill_bundle::parse_bundle(&bytes)
                .map_err(|error| failure(format!("claimant bundle no longer parses: {error}")))?;
            if physical_claimants
                .iter()
                .any(|claimant| claimant.bundle != bundle.uuid)
            {
                return Err(stale(basis, LineageRepairStaleCode::ClaimantChanged));
            }
            for claimant in &physical_claimants {
                let entry = bundle
                    .assets
                    .get(&claimant.local_id)
                    .ok_or_else(|| stale(basis, LineageRepairStaleCode::ClaimantChanged))?;
                if entry.uuid != claimant.asset
                    || entry.type_uuid != SCHEMA_LINEAGE_MANIFEST_TYPE_UUID
                {
                    return Err(stale(basis, LineageRepairStaleCode::ClaimantChanged));
                }
                validate_manifest_entry(entry)?;
            }

            let removed = physical_claimants
                .iter()
                .filter(|claimant| *claimant != survivor)
                .map(|claimant| claimant.local_id.clone())
                .collect::<BTreeSet<_>>();
            if removed.is_empty() {
                continue;
            }
            for local_id in &removed {
                bundle.assets.remove(local_id);
            }
            if bundle.assets.is_empty() {
                mutations.push(PlannedMutation::Delete {
                    target: target.clone(),
                    plan: JournalIntentPlan {
                        target_path: path_text(&target)?,
                        temp_path: String::new(),
                        conflict_path: path_text(&unique_sibling(&target, "conflict"))?,
                        pre_image_hash: Some(ContentHash(file_hash.0)),
                        proposed_hash: ContentHash(*blake3::hash(b"").as_bytes()),
                    },
                });
            } else {
                let replacement = distill_bundle::write_bundle(&bundle).map_err(|error| {
                    failure(format!(
                        "cannot render duplicate repair replacement: {error}"
                    ))
                })?;
                let replacement_hash = BundleFileHash::of_observed_bytes(&replacement);
                let temp = plan_same_dir_temp(&target).map_err(failure)?;
                if physical_claimants
                    .iter()
                    .any(|claimant| claimant == survivor)
                {
                    expected_survivor.file_hash = replacement_hash;
                }
                mutations.push(PlannedMutation::Replace {
                    target: target.clone(),
                    proposed: replacement,
                    plan: JournalIntentPlan {
                        target_path: path_text(&target)?,
                        temp_path: path_text(&temp)?,
                        conflict_path: path_text(&unique_sibling(&target, "conflict"))?,
                        pre_image_hash: Some(ContentHash(file_hash.0)),
                        proposed_hash: ContentHash(replacement_hash.0),
                    },
                });
            }
        }
        if mutations.is_empty() {
            return Err(stale(basis, LineageRepairStaleCode::ClaimantChanged));
        }

        let plans = mutations
            .iter()
            .map(PlannedMutation::plan)
            .cloned()
            .collect::<Vec<_>>();
        let group = publication
            .record_group(
                PublicationGroupKind::LineageDuplicate,
                &encode_basis(basis),
                &plans,
            )
            .map_err(failure)?;
        for mutation in &mutations {
            if let PlannedMutation::Replace { proposed, plan, .. } = mutation {
                write_planned_temp(Path::new(&plan.temp_path), proposed).map_err(failure)?;
            }
        }
        let mut all_installed = true;
        for (mutation, intent_id) in mutations.iter().zip(&group.child_intents) {
            let terminal_success = match mutation {
                PlannedMutation::Replace { target, .. } => publication
                    .resume_group_replace(*intent_id, target)
                    .map(|outcome| outcome == RenameAsideOutcome::Installed),
                PlannedMutation::Delete { target, .. } => publication
                    .resume_group_delete(*intent_id, target)
                    .map(|outcome| outcome == DeletionRecoveryOutcome::Deleted),
            }
            .map_err(failure)?;
            all_installed &= terminal_success;
        }
        if !all_installed {
            retire_if_terminal(&mut publication, group.group_id)?;
            return Err(stale(basis, LineageRepairStaleCode::PreimageChanged));
        }
        if self.reattest_lineage_claimants(&claimant_paths)? != [expected_survivor] {
            retire_if_terminal(&mut publication, group.group_id)?;
            return Err(stale(basis, LineageRepairStaleCode::ClaimantChanged));
        }
        publication.retire_group(group.group_id).map_err(failure)?;
        drop(publication);
        publish_store_version(&mut store, basis)?;
        Ok(ready_commit())
    }
}

impl AuthoringBackend for LineageRepairBackend {
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
        _bundle: distill_core::id::BundleUuid,
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
        self.create_missing(basis, canonical_manifest_bundle)
    }

    fn prepare_resolve_duplicate_lineage(
        &self,
        basis: &LineageRepairInspection,
        survivor: &LineageManifestClaimant,
    ) -> Result<Commit, LineageRepairBackendError> {
        self.resolve_duplicate(basis, survivor)
    }
}

#[derive(Debug)]
pub enum LineageRepairBackendInitError {
    Scan(ScanError),
    Quarantine(QuarantineError),
}

impl std::fmt::Display for LineageRepairBackendInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan(error) => error.fmt(f),
            Self::Quarantine(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for LineageRepairBackendInitError {}

impl From<ScanError> for LineageRepairBackendInitError {
    fn from(value: ScanError) -> Self {
        Self::Scan(value)
    }
}

impl From<QuarantineError> for LineageRepairBackendInitError {
    fn from(value: QuarantineError) -> Self {
        Self::Quarantine(value)
    }
}

struct ValidManifestBundle {
    bundle: Bundle,
    local_id: String,
    entry: AssetEntry,
}

fn validate_manifest_bundle(
    bytes: &[u8],
) -> Result<ValidManifestBundle, LineageRepairBackendError> {
    let bundle = distill_bundle::parse_bundle(bytes).map_err(|error| {
        invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            format!("manifest bundle does not parse: {error}"),
        )
    })?;
    let rendered = distill_bundle::write_bundle(&bundle).map_err(|error| {
        invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            format!("manifest bundle cannot be rendered: {error}"),
        )
    })?;
    if rendered != bytes {
        return Err(invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            "manifest bundle bytes are not canonical",
        ));
    }
    let manifests = bundle
        .assets
        .iter()
        .filter(|(_, entry)| entry.type_uuid == SCHEMA_LINEAGE_MANIFEST_TYPE_UUID)
        .collect::<Vec<_>>();
    let [(local_id, entry)] = manifests.as_slice() else {
        return Err(invalid(
            LineageRepairInvalidCode::MissingManifestEntry,
            "manifest bundle must contain exactly one SchemaLineageManifest entry",
        ));
    };
    validate_manifest_entry(entry)?;
    Ok(ValidManifestBundle {
        bundle: bundle.clone(),
        local_id: (*local_id).clone(),
        entry: (*entry).clone(),
    })
}

fn validate_manifest_entry(entry: &AssetEntry) -> Result<(), LineageRepairBackendError> {
    if !entry.authoring_only {
        return Err(invalid(
            LineageRepairInvalidCode::NotAuthoringOnly,
            "SchemaLineageManifest must be authoring_only",
        ));
    }
    if !matches!(
        entry.lineage,
        EntryLineageV1::Bootstrap {
            bundle_format_version: 1
        }
    ) {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "SchemaLineageManifest must use format-v1 bootstrap lineage",
        ));
    }
    validate_manifest_data(&entry.data)
}

fn validate_manifest_data(value: &AuthoredValue) -> Result<(), LineageRepairBackendError> {
    let fields = object(value, "manifest")?;
    let rows = array(field(fields, "types")?, "manifest.types")?;
    let mut seen_types = BTreeSet::new();
    for row in rows {
        let pair = array(row, "manifest.types row")?;
        if pair.len() != 2 {
            return Err(invalid(
                LineageRepairInvalidCode::InvalidLineage,
                "manifest type row must be a key/value pair",
            ));
        }
        let type_uuid = fixed_bytes::<16>(&pair[0], "manifest type UUID")?;
        let type_uuid = distill_core::id::TypeUuid(type_uuid);
        if is_bootstrap_control_type(type_uuid) {
            return Err(invalid(
                LineageRepairInvalidCode::BootstrapTypePresent,
                "manifest types contain a bootstrap-control TypeUuid",
            ));
        }
        if !seen_types.insert(type_uuid) {
            return Err(invalid(
                LineageRepairInvalidCode::InvalidLineage,
                "manifest contains a duplicate TypeUuid",
            ));
        }
        validate_type_lineage(&pair[1])?;
    }
    Ok(())
}

fn validate_type_lineage(value: &AuthoredValue) -> Result<(), LineageRepairBackendError> {
    let fields = object(value, "type lineage")?;
    let epochs = array(field(fields, "epochs")?, "type lineage epochs")?;
    if epochs.is_empty() || epochs.len() > u32::MAX as usize {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "type lineage requires one to u32::MAX epochs",
        ));
    }
    let current = u32_value(field(fields, "current")?, "type lineage current")?;
    if current as usize >= epochs.len() {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "type lineage current cursor is out of range",
        ));
    }
    let mut digests = BTreeSet::new();
    for (index, epoch) in epochs.iter().enumerate() {
        let epoch = object(epoch, "accepted epoch")?;
        let digest = fixed_bytes::<32>(field(epoch, "digest")?, "accepted epoch digest")?;
        if !digests.insert(digest) {
            return Err(invalid(
                LineageRepairInvalidCode::InvalidLineage,
                "type lineage repeats an accepted digest",
            ));
        }
        let parent = field(epoch, "forward_parent")?;
        match (index, parent) {
            (0, AuthoredValue::Null) => {}
            (0, _) => {
                return Err(invalid(
                    LineageRepairInvalidCode::InvalidLineage,
                    "first accepted epoch must not have a parent",
                ))
            }
            (_, AuthoredValue::UInt(parent)) if *parent < index as u128 => {}
            _ => {
                return Err(invalid(
                    LineageRepairInvalidCode::InvalidLineage,
                    "accepted epoch parent must name an earlier epoch",
                ))
            }
        }
    }
    let authority = object(field(fields, "authority")?, "type authority")?;
    if authority.len() != 1 {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "type authority must contain exactly one enum arm",
        ));
    }
    match authority.iter().next().expect("one authority arm") {
        (name, AuthoredValue::Object(payload)) if name == "Active" && payload.is_empty() => Ok(()),
        (name, payload) if name == "Retired" => {
            let payload = object(payload, "Retired authority")?;
            let retired = u32_value(field(payload, "retired_from")?, "Retired authority cursor")?;
            if retired == current {
                Ok(())
            } else {
                Err(invalid(
                    LineageRepairInvalidCode::InvalidLineage,
                    "Retired authority cursor must equal current",
                ))
            }
        }
        _ => Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "unknown or malformed type authority arm",
        )),
    }
}

fn validate_add_only_replacement(
    old: &Bundle,
    proposed: &Bundle,
    manifest_local_id: &str,
) -> Result<(), LineageRepairBackendError> {
    if old.uuid != proposed.uuid || old.primary != proposed.primary {
        return Err(invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            "occupied canonical replacement changed bundle identity or primary",
        ));
    }
    if old
        .assets
        .values()
        .any(|entry| entry.type_uuid == SCHEMA_LINEAGE_MANIFEST_TYPE_UUID)
    {
        return Err(invalid(
            LineageRepairInvalidCode::WrongBasisState,
            "missing-lineage destination already contains a manifest entry",
        ));
    }
    let mut non_manifest = proposed.assets.clone();
    non_manifest.remove(manifest_local_id);
    if non_manifest != old.assets {
        return Err(invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            "occupied canonical replacement must preserve every non-manifest entry",
        ));
    }
    let manifest_schema = proposed.assets[manifest_local_id].schema_hash;
    let mut expected_schemas = old.schemas.clone();
    let manifest_graph = proposed.schemas.get(&manifest_schema).ok_or_else(|| {
        invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "proposed manifest schema closure is missing",
        )
    })?;
    expected_schemas.insert(manifest_schema, manifest_graph.clone());
    if expected_schemas != proposed.schemas {
        return Err(invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            "occupied canonical replacement changed unrelated schema closure",
        ));
    }
    Ok(())
}

enum PlannedMutation {
    Replace {
        target: PathBuf,
        proposed: Vec<u8>,
        plan: JournalIntentPlan,
    },
    Delete {
        target: PathBuf,
        plan: JournalIntentPlan,
    },
}

impl PlannedMutation {
    fn plan(&self) -> &JournalIntentPlan {
        match self {
            Self::Replace { plan, .. } | Self::Delete { plan, .. } => plan,
        }
    }
}

fn group_claimants(
    claimants: &[LineageManifestClaimant],
) -> Result<BTreeMap<(String, String), Vec<LineageManifestClaimant>>, LineageRepairBackendError> {
    let mut grouped = BTreeMap::<_, Vec<_>>::new();
    for claimant in claimants {
        grouped
            .entry((claimant.root_name.clone(), claimant.normalized_path.clone()))
            .or_default()
            .push(claimant.clone());
    }
    for physical in grouped.values() {
        let first = physical[0].file_hash;
        if physical.iter().any(|claimant| claimant.file_hash != first) {
            return Err(invalid(
                LineageRepairInvalidCode::WrongBasisState,
                "one physical claimant path carries inconsistent preimage hashes",
            ));
        }
    }
    Ok(grouped)
}

pub(crate) fn plan_same_dir_temp(target: &Path) -> Result<PathBuf, String> {
    let parent = target
        .parent()
        .ok_or_else(|| "publication target has no parent directory".to_owned())?;
    let stem = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("bundle");
    for _ in 0..64 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp = parent.join(format!(
            ".{stem}.distill-{}-{sequence}.tmp",
            std::process::id()
        ));
        match fs::symlink_metadata(&temp) {
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(temp),
            Err(error) => return Err(format!("inspect proposed temp path: {error}")),
        }
    }
    Err("could not allocate a unique same-directory proposal temp".into())
}

pub(crate) fn write_planned_temp(temp: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = temp
        .parent()
        .ok_or_else(|| "publication temp has no parent directory".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| format!("create target directory: {error}"))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp)
        .map_err(|error| format!("create proposed temp: {error}"))?;
    let result = file
        .write_all(bytes)
        .map_err(|error| format!("write proposed temp: {error}"))
        .and_then(|()| {
            file.sync_all()
                .map_err(|error| format!("sync proposed temp: {error}"))
        })
        .and_then(|()| FileDirSync::sync(parent));
    if let Err(error) = result {
        drop(file);
        let _ = fs::remove_file(temp);
        let _ = FileDirSync::sync(parent);
        return Err(error);
    }
    Ok(())
}

struct FileDirSync;

impl FileDirSync {
    fn sync(path: &Path) -> Result<(), String> {
        let directory = fs::File::open(path)
            .map_err(|error| format!("open proposal directory for sync: {error}"))?;
        directory
            .sync_all()
            .map_err(|error| format!("sync proposal directory: {error}"))
    }
}

pub(crate) fn unique_sibling(target: &Path, role: &str) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("bundle");
    target.with_file_name(format!(
        ".{name}.distill-{role}-{}-{sequence}",
        std::process::id()
    ))
}

fn path_text(path: &Path) -> Result<String, LineageRepairBackendError> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        failure(format!(
            "journal path is not lossless UTF-8 on this platform: {}",
            path.display()
        ))
    })
}

fn encode_basis(basis: &LineageRepairInspection) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.raw(&basis.instance.0);
    encoder.raw(&basis.stamp.instance.0);
    encoder.u64(basis.stamp.version.0);
    match &basis.state {
        LineageRepairState::Missing {
            configured_root,
            configured_path,
            destination,
        } => {
            encoder.u8(1);
            encoder.str(configured_root);
            encoder.str(configured_path);
            match destination {
                LineageRepairDestination::Absent => encoder.u8(1),
                LineageRepairDestination::Occupied { file_hash, kind } => {
                    encoder.u8(2);
                    encoder.raw(&file_hash.0);
                    encoder.u8(match kind {
                        distill_rpc::OccupiedLineageDestinationKind::CanonicalBundle => 1,
                        distill_rpc::OccupiedLineageDestinationKind::Opaque => 2,
                    });
                }
            }
        }
        LineageRepairState::Duplicate { claimants } => {
            encoder.u8(2);
            encoder.u32(claimants.len() as u32);
            for claimant in claimants {
                encoder.str(&claimant.root_name);
                encoder.str(&claimant.normalized_path);
                encoder.raw(&claimant.bundle.0);
                encoder.str(&claimant.local_id);
                encoder.raw(&claimant.asset.0);
                encoder.raw(&claimant.file_hash.0);
            }
        }
    }
    encoder.into_bytes()
}

fn ready_commit() -> Commit {
    Commit {
        configuration: Some(ConfigurationStatus::Ready),
        lineage_repair: Some(None),
        ..Commit::default()
    }
}

fn publish_store_version(
    store: &mut Store,
    basis: &LineageRepairInspection,
) -> Result<(), LineageRepairBackendError> {
    require_store_basis(store, basis)?;
    store.input_transaction(|_| Ok(())).map_err(failure)?;
    Ok(())
}

fn require_store_basis(
    store: &Store,
    basis: &LineageRepairInspection,
) -> Result<(), LineageRepairBackendError> {
    if store.stamp() != basis.stamp || store.instance_id() != basis.instance {
        return Err(stale(basis, LineageRepairStaleCode::StampChanged));
    }
    Ok(())
}

fn retire_if_terminal(
    publication: &mut PublicationDriver<'_>,
    group_id: i64,
) -> Result<(), LineageRepairBackendError> {
    match publication.retire_group(group_id) {
        Ok(()) => Ok(()),
        Err(QuarantineError::Store(error))
            if matches!(error.as_ref(), distill_store::StoreError::BadIntent { .. }) =>
        {
            Ok(())
        }
        Err(error) => Err(failure(error)),
    }
}

fn lock_store(
    store: &Arc<Mutex<Store>>,
) -> Result<std::sync::MutexGuard<'_, Store>, LineageRepairBackendError> {
    store
        .lock()
        .map_err(|_| failure("durable store coordinator mutex is poisoned"))
}

fn destination_stale_code(
    expected: &LineageRepairDestination,
    observed: &LineageRepairDestination,
) -> LineageRepairStaleCode {
    match (expected, observed) {
        (LineageRepairDestination::Absent, LineageRepairDestination::Occupied { .. }) => {
            LineageRepairStaleCode::DestinationAppeared
        }
        _ => LineageRepairStaleCode::PreimageChanged,
    }
}

fn stale(
    basis: &LineageRepairInspection,
    code: LineageRepairStaleCode,
) -> LineageRepairBackendError {
    LineageRepairBackendError::Stale(LineageRepairStale {
        code,
        observed_stamp: basis.stamp,
    })
}

fn invalid(
    code: LineageRepairInvalidCode,
    message: impl Into<String>,
) -> LineageRepairBackendError {
    LineageRepairBackendError::Invalid(LineageRepairInvalid {
        code,
        message: message.into(),
    })
}

fn failure(error: impl std::fmt::Display) -> LineageRepairBackendError {
    LineageRepairBackendError::Failure(RpcFailure::InvalidAuthoringRequest {
        detail: error.to_string(),
    })
}

fn object<'a>(
    value: &'a AuthoredValue,
    context: &str,
) -> Result<&'a BTreeMap<String, AuthoredValue>, LineageRepairBackendError> {
    match value {
        AuthoredValue::Object(fields) => Ok(fields),
        _ => Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            format!("{context} must be an object"),
        )),
    }
}

fn array<'a>(
    value: &'a AuthoredValue,
    context: &str,
) -> Result<&'a [AuthoredValue], LineageRepairBackendError> {
    match value {
        AuthoredValue::Array(values) => Ok(values),
        _ => Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            format!("{context} must be an array"),
        )),
    }
}

fn field<'a>(
    fields: &'a BTreeMap<String, AuthoredValue>,
    name: &str,
) -> Result<&'a AuthoredValue, LineageRepairBackendError> {
    fields.get(name).ok_or_else(|| {
        invalid(
            LineageRepairInvalidCode::InvalidLineage,
            format!("manifest value is missing {name:?}"),
        )
    })
}

fn fixed_bytes<const N: usize>(
    value: &AuthoredValue,
    context: &str,
) -> Result<[u8; N], LineageRepairBackendError> {
    let values = array(value, context)?;
    if values.len() != N {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            format!("{context} must contain exactly {N} bytes"),
        ));
    }
    let mut bytes = [0u8; N];
    for (destination, value) in bytes.iter_mut().zip(values) {
        let AuthoredValue::UInt(value) = value else {
            return Err(invalid(
                LineageRepairInvalidCode::InvalidLineage,
                format!("{context} contains a non-byte value"),
            ));
        };
        *destination = u8::try_from(*value).map_err(|_| {
            invalid(
                LineageRepairInvalidCode::InvalidLineage,
                format!("{context} contains a value outside u8"),
            )
        })?;
    }
    Ok(bytes)
}

fn u32_value(value: &AuthoredValue, context: &str) -> Result<u32, LineageRepairBackendError> {
    let AuthoredValue::UInt(value) = value else {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            format!("{context} must be an unsigned integer"),
        ));
    };
    u32::try_from(*value).map_err(|_| {
        invalid(
            LineageRepairInvalidCode::InvalidLineage,
            format!("{context} is outside u32"),
        )
    })
}
