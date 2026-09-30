//! Durable long-running authoring operations.
//!
//! Preparation is read-only.  The filesystem/store publication is deferred
//! until the RPC progress stream reaches its terminal event, so cancellation
//! cannot strand the durable store ahead of the RPC coordinator.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use distill_bundle::{Bundle, EntryLineageV1, LineageStamp};
use distill_core::bootstrap::{
    is_bootstrap_control_type, MIGRATION_TYPE_UUID, SCHEMA_LINEAGE_MANIFEST_TYPE_UUID,
};
use distill_core::canonical::CanonicalEncoder;
use distill_core::id::{ContentHash, LogicalHash};
use distill_core::lineage::AcceptedSchemaEpoch;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringProgressEvent, AuthoringProgressState, BuildRequest, Commit, DeferredOperation,
    DeferredOperationResult, DiskMigrationRequest, DoctorRequest, InputVersion, LongRunningOp,
    PreparedOperationCommit, RenameWithFixupsRequest, RpcFailure, SchemaTransitionAction,
    SchemaTransitionRequest,
};
use distill_schema::ngp_schema::SchemaNode;
use distill_store::journal::{
    CreationRecoveryOutcome, DeletionRecoveryOutcome, JournalIntentPlan, PublicationGroupKind,
    RenameAsideOutcome,
};
use distill_store::pipeline::{
    AcceptedTypeLineage, ReverseMigrationEdge, SchemaLineageManifest, TypeAuthorityState,
    VerifiedSchemaLineageManifest,
};
use distill_store::state::PipelineState;
use distill_store::Store;

use crate::store_cell::AuthorityStore;
use crate::authoring::{invalid, require_base, AuthoringService};
use crate::build::CurrentLoadService;
use crate::coordinator::{publish_incremental_paths, LineageDestination};
use crate::lineage_repair::{plan_same_dir_temp, unique_sibling, write_planned_temp};
use crate::pipeline_map::PipelineProjection;
use crate::quarantine::{material_recovery_diagnostic, QuarantineDriver};
use crate::scanner::RootedScanner;

impl AuthoringService {
    pub(crate) fn prepare_long_operation(
        &self,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        let runtime = OperationRuntime {
            store: Arc::clone(&self.store),
            scanner: self.scanner.clone(),
            quarantine: self.quarantine_snapshot(),
            lineage_destination: self.lineage_destination_snapshot(),
            pipeline_projection: self.pipeline_projection(),
            tag_index_coordinator: self
                .tag_index_coordinator()
                .as_ref()
                .map_or_else(Weak::new, Arc::downgrade),
        };
        let planned = match operation {
            LongRunningOp::RenameWithFixups(payload) => {
                let request = RenameWithFixupsRequest::decode(payload)
                    .map_err(|error| invalid(error.to_string()))?;
                validate_rooted_destination(
                    &self.scanner,
                    &request.destination_root,
                    &request.destination_path,
                )?;
                PlannedOperation::Files {
                    kind: PublicationGroupKind::AuthoringWrite,
                    basis: encode_rename_basis(base, &request),
                    files: self.plan_rename_with_fixups(base, &request)?,
                    failures: Vec::new(),
                }
            }
            LongRunningOp::DiskMigration(payload) => {
                let request = DiskMigrationRequest::decode(payload)
                    .map_err(|error| invalid(error.to_string()))?;
                let (files, failures) = self.plan_disk_migration(base, &request)?;
                PlannedOperation::Files {
                    kind: PublicationGroupKind::DiskMigration,
                    basis: encode_migration_basis(base, &request),
                    files,
                    failures,
                }
            }
            LongRunningOp::Doctor(payload) => {
                let request =
                    DoctorRequest::decode(payload).map_err(|error| invalid(error.to_string()))?;
                // Snapshot the served request set now, on the RPC thread; the
                // deferred completion runs later on the authority.
                let build_requests = if request == DoctorRequest::Verify {
                    match runtime.tag_index_coordinator.upgrade() {
                        Some(coordinator) => coordinator
                            .server()
                            .verification_build_requests()
                            .map_err(|error| {
                                format!(
                                    "build verification is unavailable for the current authority state: {error:?}"
                                )
                            }),
                        None => Err("build coordinator stopped during doctor verify".to_owned()),
                    }
                } else {
                    Ok(Vec::new())
                };
                PlannedOperation::Doctor {
                    request,
                    build_requests,
                }
            }
            LongRunningOp::SchemaTransition(payload) => {
                let request = SchemaTransitionRequest::decode(payload)
                    .map_err(|error| invalid(error.to_string()))?;
                PlannedOperation::SchemaTransition(Box::new(
                    self.plan_schema_transition(base, request)?,
                ))
            }
        };
        let running_payload = operation_summary(&planned);
        Ok(PreparedOperationCommit::deferred(
            Arc::new(DeferredAuthoringOperation { runtime, planned }),
            vec![
                AuthoringProgressEvent {
                    sequence: 0,
                    state: AuthoringProgressState::Started,
                    payload: Arc::from([]),
                },
                AuthoringProgressEvent {
                    sequence: 1,
                    state: AuthoringProgressState::Running,
                    payload: Arc::from(running_payload.into_bytes()),
                },
                AuthoringProgressEvent {
                    sequence: 2,
                    state: AuthoringProgressState::Completed,
                    payload: Arc::from([]),
                },
            ],
        ))
    }

    fn plan_schema_repairs(
        &self,
        base: InputVersion,
    ) -> Result<(Vec<OperationFile>, Vec<String>), RpcFailure> {
        let (cached_schemas, snapshot) = {
            let store = self
                .store
                .read();
            require_base(&store, base)?;
            (
                store.all_schemas().map_err(invalid)?,
                crate::scanner::ScanSnapshot::load_bundles(&store).map_err(invalid)?,
            )
        };
        let mut holders = BTreeMap::new();
        let mut failures = Vec::new();
        for (hash, json) in cached_schemas {
            match distill_schema::ngp_schema::verify_snapshot(&json, hash) {
                Ok(schema) => {
                    holders.insert(hash, schema);
                }
                Err(error) => failures.push(format!(
                    "cached schema {hash} does not authenticate and was ignored: {error}"
                )),
            }
        }
        for row in snapshot.bundle_rows() {
            let schemas = match &row.parsed {
                Ok(bundle) => Some(&bundle.schemas),
                Err(_) => row
                    .namespace_skeleton
                    .as_ref()
                    .map(|skeleton| &skeleton.schemas),
            };
            if let Some(schemas) = schemas {
                holders.extend(schemas.iter().map(|(hash, schema)| (*hash, schema.clone())));
            }
        }
        let mut repairs = Vec::new();
        for row in snapshot.bundle_rows().filter(|row| row.parsed.is_err()) {
            match distill_bundle::repair_missing_schemas(&row.bytes, &holders) {
                Ok(Some(proposed)) => {
                    let target = self
                        .scanner
                        .physical_path(&row.root_name, &row.normalized_path)
                        .map_err(invalid)?;
                    repairs.push(OperationFile::replace(
                        target,
                        ContentHash(*blake3::hash(&row.bytes).as_bytes()),
                        proposed,
                    ));
                }
                Ok(None) => {}
                Err(distill_bundle::BundleError::MissingSchema {
                    local_id,
                    schema_hash,
                }) => failures.push(format!(
                    "{}:{} entry {local_id:?} references unavailable schema {schema_hash}",
                    row.root_name, row.normalized_path
                )),
                Err(_) => {}
            }
        }
        Ok((repairs, failures))
    }

    fn plan_rename_with_fixups(
        &self,
        base: InputVersion,
        request: &RenameWithFixupsRequest,
    ) -> Result<Vec<OperationFile>, RpcFailure> {
        let store = self
            .store
            .read();
        require_base(&store, base)?;
        let moving = store
            .bundle(request.bundle)
            .map_err(invalid)?
            .ok_or_else(|| invalid(format!("cannot rename unknown bundle {}", request.bundle)))?;
        let old_root = store
            .root_name(moving.root)
            .map_err(invalid)?
            .ok_or_else(|| invalid("bundle root identity is missing"))?;
        let source = self
            .scanner
            .physical_path(&old_root, &moving.path)
            .map_err(invalid)?;
        let destination = self
            .scanner
            .physical_path(&request.destination_root, &request.destination_path)
            .map_err(invalid)?;
        if source == destination {
            return Err(invalid("rename destination is the current bundle path"));
        }
        if fs::symlink_metadata(&destination).is_ok() {
            return Err(invalid("rename destination already exists"));
        }

        let mut files = Vec::new();
        for meta in store.all_bundles().map_err(invalid)? {
            let root = store
                .root_name(meta.root)
                .map_err(invalid)?
                .ok_or_else(|| invalid("bundle root identity is missing"))?;
            let path = self
                .scanner
                .physical_path(&root, &meta.path)
                .map_err(invalid)?;
            let bytes = self.scanner.read_identity_checked(&path).map_err(invalid)?;
            let observed = ContentHash(*blake3::hash(&bytes).as_bytes());
            if observed != meta.content_hash {
                return Err(invalid(format!(
                    "bundle {} changed since durable version {}",
                    meta.path, base.0
                )));
            }
            let mut bundle = distill_bundle::parse_bundle(&bytes).map_err(invalid)?;
            let changed = rewrite_bundle_path_references(
                &mut bundle,
                &moving.path,
                &request.destination_path,
            )?;
            if meta.bundle == request.bundle {
                let proposed = if changed {
                    distill_bundle::write_bundle(&bundle).map_err(invalid)?
                } else {
                    bytes
                };
                files.push(OperationFile::create(destination.clone(), proposed));
                files.push(OperationFile::delete(path, observed));
            } else if changed {
                files.push(OperationFile::replace(
                    path,
                    observed,
                    distill_bundle::write_bundle(&bundle).map_err(invalid)?,
                ));
            }
        }
        Ok(files)
    }

    fn plan_disk_migration(
        &self,
        base: InputVersion,
        request: &DiskMigrationRequest,
    ) -> Result<(Vec<OperationFile>, Vec<String>), RpcFailure> {
        let coordinator = self
            .tag_index_coordinator()
            .ok_or_else(|| invalid("disk migration coordinator is unavailable"))?;
        let loader = CurrentLoadService::capture(&coordinator, base).map_err(invalid)?;
        let requested = request.bundles.iter().copied().collect::<BTreeSet<_>>();
        let all = {
            let store = self
                .store
                .read();
            require_base(&store, base)?;
            store
                .all_bundles()
                .map_err(invalid)?
                .into_iter()
                .map(|meta| {
                    let root = store
                        .root_name(meta.root)
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| "bundle root identity is missing".to_owned());
                    Ok::<_, String>((meta, root))
                })
                .collect::<Result<Vec<_>, String>>()
                .map_err(invalid)?
        };
        let mut failures = Vec::new();
        if !requested.is_empty() {
            let present = all
                .iter()
                .map(|(meta, _)| meta.bundle)
                .collect::<BTreeSet<_>>();
            for missing in requested.difference(&present) {
                failures.push(format!(
                    "bundle {missing}: cannot migrate an unknown bundle"
                ));
            }
        }
        let mut files = Vec::new();
        for (meta, root) in all {
            if !requested.is_empty() && !requested.contains(&meta.bundle) {
                continue;
            }
            let result =
                root.and_then(|root| self.plan_disk_migration_bundle(base, &loader, &meta, &root));
            match result {
                Ok(Some(file)) => files.push(file),
                Ok(None) => {}
                Err(error) => {
                    failures.push(format!("bundle {} at {}: {error}", meta.bundle, meta.path))
                }
            }
        }
        Ok((files, failures))
    }

    fn plan_disk_migration_bundle(
        &self,
        base: InputVersion,
        loader: &CurrentLoadService,
        meta: &distill_store::bundles::BundleMeta,
        root: &str,
    ) -> Result<Option<OperationFile>, String> {
        let target = self
            .scanner
            .physical_path(root, &meta.path)
            .map_err(|error| error.to_string())?;
        let bytes = self
            .scanner
            .read_identity_checked(&target)
            .map_err(|error| error.to_string())?;
        let observed = ContentHash(*blake3::hash(&bytes).as_bytes());
        if observed != meta.content_hash {
            return Err(format!("changed since durable version {}", base.0));
        }
        let mut bundle = distill_bundle::parse_bundle(&bytes).map_err(|error| error.to_string())?;
        let load_bundle = bundle.clone();
        let mut changed = false;
        let mut migrated_schemas = BTreeMap::new();
        for entry in bundle.assets.values_mut() {
            if is_bootstrap_control_type(entry.type_uuid) {
                continue;
            }
            let current = loader.load(entry, &load_bundle, &meta.path)?;
            if entry.schema_hash == current.schema_hash {
                continue;
            }
            entry.data = current.value;
            entry.schema_hash = current.schema_hash;
            entry.lineage = EntryLineageV1::Manifest(current.lineage);
            migrated_schemas.insert(current.schema_hash, current.schema);
            changed = true;
        }
        if !changed {
            return Ok(None);
        }
        bundle.schemas.extend(migrated_schemas);
        let used = distill_bundle::referenced_schema_hashes(&bundle);
        bundle.schemas.retain(|hash, _| used.contains(hash));
        Ok(Some(OperationFile::replace(
            target,
            observed,
            distill_bundle::write_bundle(&bundle).map_err(|error| error.to_string())?,
        )))
    }

    fn plan_schema_transition(
        &self,
        base: InputVersion,
        request: SchemaTransitionRequest,
    ) -> Result<PlannedSchemaTransition, RpcFailure> {
        let coordinator = self
            .tag_index_coordinator()
            .ok_or_else(|| invalid("schema-transition coordinator is unavailable"))?;
        let (candidate_digest, migration_function_keys) = coordinator
            .pending_schema_transition_context(&request.candidate, request.type_uuid)
            .map_err(invalid)?;
        let accepted_lineage = {
            let store = self
                .store
                .read();
            require_base(&store, base)?;
            if store.schema_manifest_basis().map_err(invalid)?.as_ref() != Some(&request.manifest) {
                return Err(invalid("schema transition manifest basis is stale"));
            }
            match store.pipeline_state().map_err(invalid)? {
                Some(PipelineState::SchemaAcceptanceRequired { required, .. }) => {
                    if required.manifest != request.manifest
                        || required.candidate != request.candidate
                    {
                        return Err(invalid("schema transition candidate basis is stale"));
                    }
                }
                Some(PipelineState::RetiredTypeReferenced { error, .. })
                    if matches!(request.action, SchemaTransitionAction::Reactivate)
                        && error.type_uuid == request.type_uuid
                        && store
                            .pending_schema_candidate_identity()
                            .map_err(invalid)?
                            .as_ref()
                            == Some(&request.candidate) => {}
                _ => return Err(invalid("no pipeline candidate awaits schema acceptance")),
            }
            if let SchemaTransitionAction::Retire { control_basis } = request.action {
                if control_basis != store.stamp() {
                    return Err(invalid("schema retirement control snapshot is stale"));
                }
            }
            store
                .current_lineage_stamp(request.type_uuid)
                .map_err(invalid)?
        };

        let snapshot = crate::scanner::ScanSnapshot::load_bundles(&self.store.read())
        .map_err(invalid)?;
        let claimants = snapshot.lineage_claimants();
        let [claimant] = claimants.as_slice() else {
            return Err(invalid(
                "schema transition requires exactly one lineage-manifest claimant",
            ));
        };
        if ContentHash(claimant.file_hash.0) != request.manifest.manifest_hash {
            return Err(invalid("lineage-manifest file hash is stale"));
        }
        let source = snapshot
            .bundle_at(&claimant.root_name, &claimant.normalized_path)
            .ok_or_else(|| invalid("lineage-manifest bundle is absent from the pinned scan"))?;
        let mut bundle = source
            .parsed
            .as_ref()
            .map_err(|error| invalid(format!("lineage-manifest bundle is invalid: {error}")))?
            .clone();
        if bundle.uuid != claimant.bundle {
            return Err(invalid("lineage-manifest bundle identity changed"));
        }
        let entry = bundle
            .assets
            .get_mut(&claimant.local_id)
            .ok_or_else(|| invalid("lineage-manifest entry disappeared"))?;
        if entry.uuid != claimant.asset || entry.type_uuid != SCHEMA_LINEAGE_MANIFEST_TYPE_UUID {
            return Err(invalid("lineage-manifest claimant identity changed"));
        }
        let current = crate::coordinator::decode_lineage_manifest(
            &entry.data,
            request.manifest.manifest_hash,
        )
        .map_err(|error| invalid(error.to_string()))?;
        let next = proposed_schema_manifest(&current, &request, candidate_digest)?;
        let proposed_data = encode_schema_manifest(&next);
        entry.data = proposed_data.clone();
        let proposed_bytes = distill_bundle::write_bundle(&bundle).map_err(invalid)?;
        let proposed_hash = ContentHash(*blake3::hash(&proposed_bytes).as_bytes());
        let proposed = crate::coordinator::decode_lineage_manifest(&proposed_data, proposed_hash)
            .map_err(|error| invalid(error.to_string()))?;
        let target = self
            .scanner
            .physical_path(&claimant.root_name, &claimant.normalized_path)
            .map_err(invalid)?;

        let mut live_schema_hashes = BTreeSet::new();
        let mut reverse_edges = Vec::new();
        let mut waiting_paths = BTreeSet::new();
        let require_full_migration_proof = transition_requires_reverse_proof(
            request.action,
            candidate_digest,
            accepted_lineage.as_ref(),
        );
        if !matches!(request.action, SchemaTransitionAction::Accept { .. }) {
            for source in snapshot.bundle_rows() {
                let Ok(bundle) = &source.parsed else {
                    continue;
                };
                let mut waits_for_type = bundle
                    .assets
                    .values()
                    .any(|entry| entry.type_uuid == request.type_uuid);
                for migration in bundle
                    .assets
                    .values()
                    .filter(|entry| entry.type_uuid == MIGRATION_TYPE_UUID)
                {
                    let header = crate::migration_control::decode_header(&migration.data)
                        .map_err(|error| invalid(error.to_string()))?;
                    if header.target_type_uuid != request.type_uuid {
                        continue;
                    }
                    waits_for_type = true;
                    live_schema_hashes.insert(header.from_hash);
                    live_schema_hashes.insert(header.to_hash);
                    if require_full_migration_proof {
                        let accepted = accepted_lineage.as_ref().ok_or_else(|| {
                            invalid(
                                "rollback/reactivation proof target has no accepted lineage authority",
                            )
                        })?;
                        crate::migration_control::validate_transition_edge(
                            migration.uuid,
                            bundle,
                            migration,
                            header,
                            accepted,
                            &migration_function_keys,
                        )
                        .map_err(|error| invalid(error.to_string()))?;
                        reverse_edges.push(ReverseMigrationEdge {
                            asset: migration.uuid,
                            from: header.from_hash,
                            to: header.to_hash,
                        });
                    }
                }
                if waits_for_type && matches!(request.action, SchemaTransitionAction::Reactivate) {
                    waiting_paths.insert(
                        self.scanner
                            .physical_path(&source.root_name, &source.normalized_path)
                            .map_err(invalid)?,
                    );
                }
            }
        }
        drop(snapshot);

        Ok(PlannedSchemaTransition {
            request,
            target,
            preimage: ContentHash(claimant.file_hash.0),
            proposed_bytes,
            proposed,
            live_schema_hashes: live_schema_hashes.into_iter().collect(),
            reverse_edges,
            waiting_paths: waiting_paths.into_iter().collect(),
        })
    }
}

fn transition_requires_reverse_proof(
    action: SchemaTransitionAction,
    candidate_digest: Option<LogicalHash>,
    accepted: Option<&LineageStamp>,
) -> bool {
    match action {
        SchemaTransitionAction::Rollback { .. } => true,
        SchemaTransitionAction::Reactivate => {
            let (Some(candidate_digest), Some(accepted)) = (candidate_digest, accepted) else {
                return false;
            };
            let Some(target) = accepted
                .epochs
                .iter()
                .position(|epoch| epoch.digest == candidate_digest)
                .and_then(|position| u32::try_from(position).ok())
            else {
                // A genuinely new digest appends through the ordinary forward
                // rule and has no reverse-edge proof to establish.
                return false;
            };
            target != accepted.cursor
                && !lineage_is_ancestor(&accepted.epochs, accepted.cursor, target)
        }
        SchemaTransitionAction::Accept { .. } | SchemaTransitionAction::Retire { .. } => false,
    }
}

fn lineage_is_ancestor(epochs: &[AcceptedSchemaEpoch], ancestor: u32, descendant: u32) -> bool {
    let mut cursor = Some(descendant);
    while let Some(index) = cursor {
        if index == ancestor {
            return true;
        }
        cursor = epochs
            .get(index as usize)
            .and_then(|epoch| epoch.forward_parent);
    }
    false
}

#[cfg(test)]
mod transition_proof_tests {
    use super::*;

    fn hash(byte: u8) -> LogicalHash {
        LogicalHash([byte; 32])
    }

    fn accepted(rows: &[(u8, Option<u32>)], cursor: u32) -> LineageStamp {
        LineageStamp {
            epochs: rows
                .iter()
                .map(|(digest, forward_parent)| AcceptedSchemaEpoch {
                    digest: hash(*digest),
                    forward_parent: *forward_parent,
                })
                .collect(),
            cursor,
            chain: [0; 32],
        }
    }

    #[test]
    fn only_rollback_and_divergent_reactivation_require_reverse_proof() {
        let forward = accepted(&[(1, None), (2, Some(0)), (3, Some(1))], 0);
        assert!(!transition_requires_reverse_proof(
            SchemaTransitionAction::Reactivate,
            Some(hash(9)),
            Some(&forward),
        ));
        assert!(!transition_requires_reverse_proof(
            SchemaTransitionAction::Reactivate,
            Some(hash(3)),
            Some(&forward),
        ));

        let divergent = accepted(&[(1, None), (2, Some(0)), (3, Some(0))], 2);
        assert!(transition_requires_reverse_proof(
            SchemaTransitionAction::Reactivate,
            Some(hash(2)),
            Some(&divergent),
        ));
        assert!(transition_requires_reverse_proof(
            SchemaTransitionAction::Rollback { target: hash(1) },
            Some(hash(1)),
            Some(&divergent),
        ));
    }
}

#[derive(Clone)]
struct OperationRuntime {
    store: Arc<AuthorityStore>,
    scanner: RootedScanner,
    quarantine: QuarantineDriver,
    lineage_destination: LineageDestination,
    pipeline_projection: PipelineProjection,
    tag_index_coordinator: Weak<crate::coordinator::DaemonCoordinator>,
}

enum PlannedOperation {
    Files {
        kind: PublicationGroupKind,
        basis: Vec<u8>,
        files: Vec<OperationFile>,
        failures: Vec<String>,
    },
    Doctor {
        request: DoctorRequest,
        build_requests: Result<Vec<BuildRequest>, String>,
    },
    SchemaTransition(Box<PlannedSchemaTransition>),
}

pub(crate) struct PlannedSchemaTransition {
    pub(crate) request: SchemaTransitionRequest,
    pub(crate) target: PathBuf,
    pub(crate) preimage: ContentHash,
    pub(crate) proposed_bytes: Vec<u8>,
    pub(crate) proposed: VerifiedSchemaLineageManifest,
    pub(crate) live_schema_hashes: Vec<LogicalHash>,
    pub(crate) reverse_edges: Vec<ReverseMigrationEdge>,
    pub(crate) waiting_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaTransitionJournalBasis {
    pub(crate) base: InputVersion,
    pub(crate) target: PathBuf,
    pub(crate) old_manifest_hash: ContentHash,
    pub(crate) proposed_manifest_hash: ContentHash,
}

impl SchemaTransitionJournalBasis {
    const MAGIC: [u8; 4] = *b"DSST";
    const VERSION: u8 = 1;

    pub(crate) fn encode(&self) -> Result<Vec<u8>, String> {
        let target = self
            .target
            .to_str()
            .ok_or_else(|| "schema-transition journal path is not lossless UTF-8".to_owned())?;
        let target_len = u32::try_from(target.len())
            .map_err(|_| "schema-transition journal path exceeds u32".to_owned())?;
        let mut bytes = Vec::with_capacity(4 + 1 + 8 + 32 + 32 + 4 + target.len());
        bytes.extend_from_slice(&Self::MAGIC);
        bytes.push(Self::VERSION);
        bytes.extend_from_slice(&self.base.0.to_le_bytes());
        bytes.extend_from_slice(&self.old_manifest_hash.0);
        bytes.extend_from_slice(&self.proposed_manifest_hash.0);
        bytes.extend_from_slice(&target_len.to_le_bytes());
        bytes.extend_from_slice(target.as_bytes());
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        const PREFIX: usize = 4 + 1 + 8 + 32 + 32 + 4;
        if bytes.len() < PREFIX || bytes[..4] != Self::MAGIC || bytes[4] != Self::VERSION {
            return Err("schema-transition journal basis has invalid framing".to_owned());
        }
        let base = InputVersion(u64::from_le_bytes(
            bytes[5..13]
                .try_into()
                .expect("schema-transition base is fixed width"),
        ));
        let old_manifest_hash = ContentHash(
            bytes[13..45]
                .try_into()
                .expect("schema-transition old hash is fixed width"),
        );
        let proposed_manifest_hash = ContentHash(
            bytes[45..77]
                .try_into()
                .expect("schema-transition proposed hash is fixed width"),
        );
        let target_len = usize::try_from(u32::from_le_bytes(
            bytes[77..81]
                .try_into()
                .expect("schema-transition target length is fixed width"),
        ))
        .map_err(|_| "schema-transition target length overflows usize".to_owned())?;
        if bytes.len() != PREFIX + target_len {
            return Err(
                "schema-transition journal basis has trailing or truncated bytes".to_owned(),
            );
        }
        let target = std::str::from_utf8(&bytes[PREFIX..])
            .map_err(|_| "schema-transition journal path is not UTF-8".to_owned())?;
        Ok(Self {
            base,
            target: PathBuf::from(target),
            old_manifest_hash,
            proposed_manifest_hash,
        })
    }
}

struct DeferredAuthoringOperation {
    runtime: OperationRuntime,
    planned: PlannedOperation,
}

impl DeferredOperation for DeferredAuthoringOperation {
    fn complete(&self, base: InputVersion) -> Result<DeferredOperationResult, String> {
        match &self.planned {
            PlannedOperation::Files {
                kind,
                basis,
                files,
                failures,
            } => self
                .runtime
                .publish_files(base, *kind, basis, files, failures),
            PlannedOperation::Doctor {
                request,
                build_requests,
            } => self.runtime.run_doctor(base, *request, build_requests),
            PlannedOperation::SchemaTransition(planned) => {
                self.runtime.publish_schema_transition(base, planned)
            }
        }
    }
}

impl OperationRuntime {
    fn publish_schema_transition(
        &self,
        base: InputVersion,
        planned: &PlannedSchemaTransition,
    ) -> Result<DeferredOperationResult, String> {
        self.tag_index_coordinator
            .upgrade()
            .ok_or_else(|| "schema-transition coordinator stopped".to_owned())?
            .publish_schema_transition(base, planned, &self.quarantine)
    }

    fn publish_files(
        &self,
        base: InputVersion,
        kind: PublicationGroupKind,
        basis: &[u8],
        files: &[OperationFile],
        initial_failures: &[String],
    ) -> Result<DeferredOperationResult, String> {
        if kind == PublicationGroupKind::DiskMigration {
            return self.publish_disk_migration_files(base, basis, files, initial_failures);
        }
        if files.is_empty() {
            return self.advance_empty(
                base,
                (!initial_failures.is_empty()).then(|| initial_failures.join("; ")),
            );
        }
        let mut store = self
            .store
            .write();
        require_base(&store, base).map_err(|error| format!("{error:?}"))?;
        let mut staged = Vec::with_capacity(files.len());
        for file in files {
            let temp = file
                .proposed
                .as_ref()
                .map(|_| plan_same_dir_temp(&file.target))
                .transpose()?;
            staged.push(temp);
        }
        let plans = files
            .iter()
            .zip(&staged)
            .map(|(file, temp)| {
                Ok(JournalIntentPlan {
                    target_path: path_text(&file.target)?,
                    temp_path: temp
                        .as_deref()
                        .map(path_text)
                        .transpose()?
                        .unwrap_or_default(),
                    conflict_path: path_text(&unique_sibling(&file.target, "conflict"))?,
                    pre_image_hash: file.preimage,
                    proposed_hash: file.proposed_hash(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut publication = self
            .quarantine
            .admit_publication(&mut store)
            .map_err(|error| error.to_string())?;
        let group = publication
            .record_group(kind, basis, &plans)
            .map_err(|error| error.to_string())?;
        for (file, temp) in files.iter().zip(&staged) {
            if let (Some(bytes), Some(temp)) = (file.proposed.as_deref(), temp.as_deref()) {
                write_planned_temp(temp, bytes)?;
            }
        }
        publication
            .arm_group(group.group_id)
            .map_err(|error| error.to_string())?;
        let mut failures = initial_failures.to_vec();
        for (file, intent) in files.iter().zip(&group.child_intents) {
            let installed = match (&file.preimage, &file.proposed) {
                (Some(_), Some(_)) => publication
                    .resume_group_replace(*intent, &file.target)
                    .map(|outcome| outcome == RenameAsideOutcome::Installed),
                (Some(_), None) => publication
                    .resume_group_delete(*intent, &file.target)
                    .map(|outcome| outcome == DeletionRecoveryOutcome::Deleted),
                (None, Some(_)) => publication
                    .resume_group_create(*intent)
                    .map(|outcome| outcome == CreationRecoveryOutcome::Installed),
                (None, None) => unreachable!("operation file always changes an inode"),
            }
            .map_err(|error| error.to_string())?;
            if !installed {
                failures.push(format!(
                    "{} changed concurrently; its observed inode was preserved",
                    file.target.display()
                ));
            }
        }
        publication
            .retire_group(group.group_id)
            .map_err(|error| error.to_string())?;
        drop(publication);
        drop(store);
        let changed_paths = files
            .iter()
            .map(|file| file.target.clone())
            .collect::<Vec<_>>();
        let commit = publish_incremental_paths(
            &self.scanner,
            &changed_paths,
            &self.lineage_destination,
            &self.store,
            base,
            &self.pipeline_projection,
            self.tag_index_coordinator.upgrade().as_deref(),
        )?;
        Ok(DeferredOperationResult {
            commit,
            terminal_error: (!failures.is_empty()).then(|| failures.join("; ")),
        })
    }

    /// Disk migration is explicitly best-effort per bundle. Each planned
    /// replacement receives its own journal group so a conflict or local
    /// publication failure cannot strand later bundles behind an
    /// operation-wide parent. Every attempted path is rescanned together at
    /// the original base: conflicts may expose a user edit even when our
    /// proposal was not installed, while successful replacements still land
    /// in the one input-version commit returned to RPC.
    fn publish_disk_migration_files(
        &self,
        base: InputVersion,
        basis: &[u8],
        files: &[OperationFile],
        initial_failures: &[String],
    ) -> Result<DeferredOperationResult, String> {
        if files.is_empty() {
            return self.advance_empty(
                base,
                (!initial_failures.is_empty()).then(|| initial_failures.join("; ")),
            );
        }

        let mut store = self
            .store
            .write();
        require_base(&store, base).map_err(|error| format!("{error:?}"))?;
        let mut failures = initial_failures.to_vec();
        let mut attempted_paths = Vec::with_capacity(files.len());
        for file in files {
            attempted_paths.push(file.target.clone());
            match self.publish_one_migration_file(&mut store, basis, file) {
                Ok(true) => {}
                Ok(false) => failures.push(format!(
                    "{} changed concurrently; its observed inode was preserved",
                    file.target.display()
                )),
                Err(error) => {
                    failures.push(format!(
                        "{} could not be published: {error}",
                        file.target.display()
                    ));
                    // The failed attempt may have stopped at any journal or
                    // filesystem state. Reconcile that single-child group to
                    // a terminal state before admitting the next bundle.
                    match self.quarantine.admit_publication(&mut store) {
                        Ok(recovery) => {
                            if let Some(diagnostic) =
                                material_recovery_diagnostic(recovery.recovered())
                            {
                                failures.push(diagnostic);
                            }
                        }
                        Err(recovery_error) => {
                            failures.push(format!(
                                "publication recovery stopped later bundles: {recovery_error}"
                            ));
                            break;
                        }
                    }
                }
            }
        }
        drop(store);

        let commit = publish_incremental_paths(
            &self.scanner,
            &attempted_paths,
            &self.lineage_destination,
            &self.store,
            base,
            &self.pipeline_projection,
            self.tag_index_coordinator.upgrade().as_deref(),
        )?;
        Ok(DeferredOperationResult {
            commit,
            terminal_error: (!failures.is_empty()).then(|| failures.join("; ")),
        })
    }

    fn publish_one_migration_file(
        &self,
        store: &mut Store,
        basis: &[u8],
        file: &OperationFile,
    ) -> Result<bool, String> {
        let temp = file
            .proposed
            .as_ref()
            .map(|_| plan_same_dir_temp(&file.target))
            .transpose()?;
        let plan = JournalIntentPlan {
            target_path: path_text(&file.target)?,
            temp_path: temp
                .as_deref()
                .map(path_text)
                .transpose()?
                .unwrap_or_default(),
            conflict_path: path_text(&unique_sibling(&file.target, "conflict"))?,
            pre_image_hash: file.preimage,
            proposed_hash: file.proposed_hash(),
        };
        let mut publication = self
            .quarantine
            .admit_publication(store)
            .map_err(|error| error.to_string())?;
        let group = publication
            .record_group(PublicationGroupKind::DiskMigration, basis, &[plan])
            .map_err(|error| error.to_string())?;
        if let (Some(bytes), Some(temp)) = (file.proposed.as_deref(), temp.as_deref()) {
            let parent = temp
                .parent()
                .ok_or_else(|| "migration proposal temp has no parent directory".to_owned())?;
            let metadata = fs::symlink_metadata(parent)
                .map_err(|error| format!("migration proposal directory changed: {error}"))?;
            if !metadata.file_type().is_dir() {
                return Err("migration proposal parent is no longer a directory".to_owned());
            }
            write_planned_temp(temp, bytes)?;
        }
        publication
            .arm_group(group.group_id)
            .map_err(|error| error.to_string())?;
        let installed = match (&file.preimage, &file.proposed) {
            (Some(_), Some(_)) => publication
                .resume_group_replace(group.child_intents[0], &file.target)
                .map(|outcome| outcome == RenameAsideOutcome::Installed),
            (Some(_), None) => publication
                .resume_group_delete(group.child_intents[0], &file.target)
                .map(|outcome| outcome == DeletionRecoveryOutcome::Deleted),
            (None, Some(_)) => publication
                .resume_group_create(group.child_intents[0])
                .map(|outcome| outcome == CreationRecoveryOutcome::Installed),
            (None, None) => unreachable!("operation file always changes an inode"),
        }
        .map_err(|error| error.to_string())?;
        publication
            .retire_group(group.group_id)
            .map_err(|error| error.to_string())?;
        Ok(installed)
    }

    fn run_doctor(
        &self,
        base: InputVersion,
        request: DoctorRequest,
        build_requests: &Result<Vec<BuildRequest>, String>,
    ) -> Result<DeferredOperationResult, String> {
        // Keep the repository-wide reimport and schema walks deferred until
        // the client consumes Completed. Until then the progress stream can
        // be cancelled without starting either workload.
        let coordinator = if request == DoctorRequest::Verify {
            Some(
                self.tag_index_coordinator
                    .upgrade()
                    .ok_or_else(|| "build coordinator stopped during doctor verify".to_owned())?,
            )
        } else {
            None
        };
        let (import_failures, schema_repairs, schema_repair_failures) =
            if request == DoctorRequest::Verify {
                let authoring = coordinator
                    .as_ref()
                    .expect("verify coordinator was required")
                    .authoring_service();
                let (repairs, failures) = authoring
                    .plan_schema_repairs(base)
                    .map_err(|error| format!("{error:?}"))?;
                (
                    authoring
                        .verify_watched_import_fixpoints(base)
                        .map_err(|error| format!("{error:?}"))?,
                    repairs,
                    failures,
                )
            } else {
                (Vec::new(), Vec::new(), Vec::new())
            };
        let (filesystem_mismatch, scan_diagnostics) = if request == DoctorRequest::Verify {
            let observed = self.scanner.scan().map_err(|error| error.to_string())?;
            let published = crate::scanner::ScanSnapshot::load(
                &**self
                    .store
                    .write(),
            )
            .map_err(|error| error.to_string())?;
            let mismatch = !observed.same_observation(&published);
            let diagnostics = observed
                .diagnostic_rows()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            (mismatch, diagnostics)
        } else {
            (false, Vec::new())
        };
        let build_defects = if request == DoctorRequest::Verify {
            match build_requests {
                Ok(requests) => crate::build::doctor_verify_builds(
                    coordinator
                        .as_ref()
                        .expect("verify coordinator was required"),
                    requests,
                )?,
                Err(defect) => vec![defect.clone()],
            }
        } else {
            Vec::new()
        };
        let mut store = self
            .store
            .write();
        require_base(&store, base).map_err(|error| format!("{error:?}"))?;
        let terminal_error = match request {
            DoctorRequest::Verify => {
                store
                    .verify_all_cas_extents()
                    .map_err(|error| error.to_string())?;
                let recovered = self
                    .quarantine
                    .doctor_verify(&store)
                    .map_err(|error| error.to_string())?;
                let mut defects = Vec::new();
                if filesystem_mismatch {
                    defects.push(
                        "full filesystem rehash differs from the published input snapshot"
                            .to_owned(),
                    );
                }
                defects.extend(scan_diagnostics);
                if !import_failures.is_empty() {
                    defects.push(format!(
                        "{} watched import bundle(s) are not byte-identical fixpoints: {}",
                        import_failures.len(),
                        import_failures
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if !schema_repair_failures.is_empty() {
                    defects.push(format!(
                        "{} exact schema repair defect(s): {}",
                        schema_repair_failures.len(),
                        schema_repair_failures.join(", ")
                    ));
                }
                defects.extend(build_defects);
                if !recovered.is_empty() {
                    defects.push(format!(
                        "{} recovered edit(s) require attention",
                        recovered.len()
                    ));
                }
                (!defects.is_empty()).then(|| defects.join("; "))
            }
            DoctorRequest::Clean => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|error| error.to_string())?
                    .as_secs() as i64;
                store
                    .clean_all_displaced(now)
                    .map_err(|error| error.to_string())?;
                None
            }
            DoctorRequest::RebuildIndexes => {
                store.rebuild_indexes().map_err(|error| error.to_string())?;
                None
            }
        };
        drop(store);
        if schema_repairs.is_empty() {
            return self.advance_empty(base, terminal_error);
        }
        let basis = encode_schema_repair_basis(base, &schema_repairs);
        let mut result = self.publish_files(
            base,
            PublicationGroupKind::SchemaRepair,
            &basis,
            &schema_repairs,
            &[],
        )?;
        result.terminal_error = match (terminal_error, result.terminal_error) {
            (Some(left), Some(right)) => Some(format!("{left}; {right}")),
            (left, right) => left.or(right),
        };
        Ok(result)
    }

    fn advance_empty(
        &self,
        base: InputVersion,
        terminal_error: Option<String>,
    ) -> Result<DeferredOperationResult, String> {
        let mut store = self
            .store
            .write();
        require_base(&store, base).map_err(|error| format!("{error:?}"))?;
        store
            .input_transaction(|_| Ok(()))
            .map_err(|error| error.to_string())?;
        Ok(DeferredOperationResult {
            commit: Commit::default(),
            terminal_error,
        })
    }
}

struct OperationFile {
    target: PathBuf,
    preimage: Option<ContentHash>,
    proposed: Option<Vec<u8>>,
}

impl OperationFile {
    fn create(target: PathBuf, proposed: Vec<u8>) -> Self {
        Self {
            target,
            preimage: None,
            proposed: Some(proposed),
        }
    }

    fn replace(target: PathBuf, preimage: ContentHash, proposed: Vec<u8>) -> Self {
        Self {
            target,
            preimage: Some(preimage),
            proposed: Some(proposed),
        }
    }

    fn delete(target: PathBuf, preimage: ContentHash) -> Self {
        Self {
            target,
            preimage: Some(preimage),
            proposed: None,
        }
    }

    fn proposed_hash(&self) -> ContentHash {
        ContentHash(*blake3::hash(self.proposed.as_deref().unwrap_or_default()).as_bytes())
    }
}

fn rewrite_bundle_path_references(
    bundle: &mut Bundle,
    from: &str,
    to: &str,
) -> Result<bool, RpcFailure> {
    let schemas = bundle.schemas.clone();
    let mut changed = false;
    for entry in bundle.assets.values_mut() {
        let schema = schemas
            .get(&entry.schema_hash)
            .ok_or_else(|| invalid("bundle entry schema snapshot is missing"))?;
        changed |= rewrite_value(&schema.root, &mut entry.data, from, to)?;
    }
    Ok(changed)
}

fn proposed_schema_manifest(
    current: &VerifiedSchemaLineageManifest,
    request: &SchemaTransitionRequest,
    candidate_digest: Option<LogicalHash>,
) -> Result<SchemaLineageManifest, RpcFailure> {
    let mut next = current.manifest().clone();
    match request.action {
        SchemaTransitionAction::Accept { requested } => {
            if candidate_digest != Some(requested) {
                return Err(invalid(
                    "acceptance digest does not match the pending candidate",
                ));
            }
            match next.types.get_mut(&request.type_uuid) {
                Some(lineage) => {
                    if !matches!(lineage.authority, TypeAuthorityState::Active) {
                        return Err(invalid(
                            "ordinary acceptance cannot reactivate a retired type",
                        ));
                    }
                    if lineage.epochs.iter().any(|epoch| epoch.digest == requested) {
                        return Err(invalid("an accepted digest requires explicit rollback"));
                    }
                    let parent = lineage.current;
                    lineage.epochs.push(AcceptedSchemaEpoch {
                        digest: requested,
                        forward_parent: Some(parent),
                    });
                    lineage.current = u32::try_from(lineage.epochs.len() - 1)
                        .map_err(|_| invalid("accepted schema history exceeds u32"))?;
                }
                None => {
                    next.types.insert(
                        request.type_uuid,
                        AcceptedTypeLineage {
                            epochs: vec![AcceptedSchemaEpoch {
                                digest: requested,
                                forward_parent: None,
                            }],
                            current: 0,
                            authority: TypeAuthorityState::Active,
                        },
                    );
                }
            }
        }
        SchemaTransitionAction::Rollback { target } => {
            if candidate_digest != Some(target) {
                return Err(invalid(
                    "rollback digest does not match the pending candidate",
                ));
            }
            let lineage = next
                .types
                .get_mut(&request.type_uuid)
                .ok_or_else(|| invalid("rollback type has no accepted lineage"))?;
            let position = lineage
                .epochs
                .iter()
                .position(|epoch| epoch.digest == target)
                .ok_or_else(|| invalid("rollback target is not accepted"))?;
            lineage.current =
                u32::try_from(position).map_err(|_| invalid("rollback target exceeds u32"))?;
        }
        SchemaTransitionAction::Retire { .. } => {
            if candidate_digest.is_some() {
                return Err(invalid("retirement candidate still includes the type"));
            }
            let lineage = next
                .types
                .get_mut(&request.type_uuid)
                .ok_or_else(|| invalid("retirement type has no accepted lineage"))?;
            lineage.authority = TypeAuthorityState::Retired {
                retired_from: lineage.current,
            };
        }
        SchemaTransitionAction::Reactivate => {
            let requested =
                candidate_digest.ok_or_else(|| invalid("reactivation candidate omits the type"))?;
            let lineage = next
                .types
                .get_mut(&request.type_uuid)
                .ok_or_else(|| invalid("reactivation type has no retained lineage"))?;
            if !matches!(lineage.authority, TypeAuthorityState::Retired { .. }) {
                return Err(invalid("reactivation type is already active"));
            }
            match lineage
                .epochs
                .iter()
                .position(|epoch| epoch.digest == requested)
            {
                Some(position) => {
                    lineage.current = u32::try_from(position)
                        .map_err(|_| invalid("reactivation cursor exceeds u32"))?;
                }
                None => {
                    let parent = lineage.current;
                    lineage.epochs.push(AcceptedSchemaEpoch {
                        digest: requested,
                        forward_parent: Some(parent),
                    });
                    lineage.current = u32::try_from(lineage.epochs.len() - 1)
                        .map_err(|_| invalid("reactivated schema history exceeds u32"))?;
                }
            }
            lineage.authority = TypeAuthorityState::Active;
        }
    }
    Ok(next)
}

fn encode_schema_manifest(manifest: &SchemaLineageManifest) -> AuthoredValue {
    AuthoredValue::Object(BTreeMap::from([(
        "types".to_owned(),
        AuthoredValue::Array(
            manifest
                .types
                .iter()
                .map(|(type_uuid, lineage)| {
                    let authority = match lineage.authority {
                        TypeAuthorityState::Active => AuthoredValue::Object(BTreeMap::from([(
                            "Active".to_owned(),
                            AuthoredValue::Object(BTreeMap::new()),
                        )])),
                        TypeAuthorityState::Retired { retired_from } => {
                            AuthoredValue::Object(BTreeMap::from([(
                                "Retired".to_owned(),
                                AuthoredValue::Object(BTreeMap::from([(
                                    "retired_from".to_owned(),
                                    AuthoredValue::UInt(u128::from(retired_from)),
                                )])),
                            )]))
                        }
                    };
                    AuthoredValue::Array(vec![
                        authored_bytes(&type_uuid.0),
                        AuthoredValue::Object(BTreeMap::from([
                            ("authority".to_owned(), authority),
                            (
                                "current".to_owned(),
                                AuthoredValue::UInt(u128::from(lineage.current)),
                            ),
                            (
                                "epochs".to_owned(),
                                AuthoredValue::Array(
                                    lineage
                                        .epochs
                                        .iter()
                                        .map(|epoch| {
                                            AuthoredValue::Object(BTreeMap::from([
                                                (
                                                    "digest".to_owned(),
                                                    authored_bytes(&epoch.digest.0),
                                                ),
                                                (
                                                    "forward_parent".to_owned(),
                                                    epoch.forward_parent.map_or(
                                                        AuthoredValue::Null,
                                                        |parent| {
                                                            AuthoredValue::UInt(u128::from(parent))
                                                        },
                                                    ),
                                                ),
                                            ]))
                                        })
                                        .collect(),
                                ),
                            ),
                        ])),
                    ])
                })
                .collect(),
        ),
    )]))
}

fn authored_bytes(bytes: &[u8]) -> AuthoredValue {
    AuthoredValue::Array(
        bytes
            .iter()
            .map(|byte| AuthoredValue::UInt(u128::from(*byte)))
            .collect(),
    )
}

fn rewrite_value(
    schema: &SchemaNode,
    value: &mut AuthoredValue,
    from: &str,
    to: &str,
) -> Result<bool, RpcFailure> {
    match schema {
        SchemaNode::AssetRef(_) | SchemaNode::WeakRef(_) => Ok(rewrite_reference(value, from, to)),
        SchemaNode::Struct { fields, .. } => {
            let AuthoredValue::Object(values) = value else {
                return Ok(false);
            };
            let mut changed = false;
            for (name, _, field) in fields {
                if let Some(value) = values.get_mut(name) {
                    changed |= rewrite_value(field, value, from, to)?;
                }
            }
            Ok(changed)
        }
        SchemaNode::Enum { variants, .. } => {
            let AuthoredValue::Object(values) = value else {
                return Ok(false);
            };
            let Some((name, payload)) = values.iter_mut().next() else {
                return Ok(false);
            };
            let Some((_, _, variant)) = variants.iter().find(|(candidate, _, _)| candidate == name)
            else {
                return Ok(false);
            };
            rewrite_value(variant, payload, from, to)
        }
        SchemaNode::Vec(inner) | SchemaNode::Set(inner) | SchemaNode::Array { elem: inner, .. } => {
            let AuthoredValue::Array(values) = value else {
                return Ok(false);
            };
            let mut changed = false;
            for value in values {
                changed |= rewrite_value(inner, value, from, to)?;
            }
            Ok(changed)
        }
        SchemaNode::Option(inner) => {
            if matches!(value, AuthoredValue::Null) {
                Ok(false)
            } else {
                rewrite_value(inner, value, from, to)
            }
        }
        SchemaNode::Map { key, value: item } => {
            let AuthoredValue::Object(values) = value else {
                return Ok(false);
            };
            let mut changed = false;
            if matches!(key.as_ref(), SchemaNode::String) {
                for value in values.values_mut() {
                    changed |= rewrite_value(item, value, from, to)?;
                }
            }
            Ok(changed)
        }
        SchemaNode::Primitive(_)
        | SchemaNode::String
        | SchemaNode::Blob
        | SchemaNode::Unit
        | SchemaNode::BackRef(_) => Ok(false),
    }
}

fn rewrite_reference(value: &mut AuthoredValue, from: &str, to: &str) -> bool {
    match value {
        AuthoredValue::Str(path) if path == from => {
            *path = to.to_owned();
            true
        }
        AuthoredValue::Object(fields) => fields.get_mut("path").is_some_and(|path| {
            if let AuthoredValue::Str(path) = path {
                if path == from {
                    *path = to.to_owned();
                    return true;
                }
            }
            false
        }),
        _ => false,
    }
}

fn validate_rooted_destination(
    scanner: &RootedScanner,
    root: &str,
    path: &str,
) -> Result<(), RpcFailure> {
    scanner.physical_path(root, path).map(drop).map_err(invalid)
}

fn encode_rename_basis(base: InputVersion, request: &RenameWithFixupsRequest) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.u64(base.0);
    encoder.raw(&request.bundle.0);
    encoder.str(&request.destination_root);
    encoder.str(&request.destination_path);
    encoder.into_bytes()
}

fn encode_migration_basis(base: InputVersion, request: &DiskMigrationRequest) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.u64(base.0);
    encoder.u32(request.bundles.len() as u32);
    for bundle in &request.bundles {
        encoder.raw(&bundle.0);
    }
    encoder.into_bytes()
}

fn encode_schema_repair_basis(base: InputVersion, files: &[OperationFile]) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.u64(base.0);
    encoder.u32(files.len() as u32);
    for file in files {
        encoder.raw(
            &file
                .preimage
                .expect("schema repair replaces an observed file")
                .0,
        );
        encoder.raw(&file.proposed_hash().0);
    }
    encoder.into_bytes()
}

fn operation_summary(operation: &PlannedOperation) -> String {
    match operation {
        PlannedOperation::Files {
            kind,
            files,
            failures,
            ..
        } => format!(
            "{kind:?}: {} file(s), {} per-file failure(s)",
            files.len(),
            failures.len()
        ),
        PlannedOperation::Doctor { request, .. } => format!("doctor {request:?}"),
        PlannedOperation::SchemaTransition(planned) => {
            format!("schema transition {:?}", planned.request.action)
        }
    }
}

fn path_text(path: &Path) -> Result<String, String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("path is not lossless UTF-8: {}", path.display()))
}
