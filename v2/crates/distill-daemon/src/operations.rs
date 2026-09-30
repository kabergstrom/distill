//! Durable long-running authoring operations.
//!
//! Preparation is read-only.  The filesystem/store publication is deferred
//! until the RPC progress stream reaches its terminal event, so cancellation
//! cannot strand the durable store ahead of the RPC coordinator.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use distill_bundle::Bundle;
use distill_core::canonical::CanonicalEncoder;
use distill_core::id::ContentHash;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringProgressEvent, AuthoringProgressState, BuildRequest, Commit, DeferredOperation,
    DeferredOperationResult, DoctorRequest, InputVersion, LongRunningOp, PreparedOperationCommit,
    RenameWithFixupsRequest, RpcFailure,
};
use distill_schema::ngp_schema::SchemaNode;
use distill_store::journal::{
    CreationRecoveryOutcome, DeletionRecoveryOutcome, JournalIntentPlan, PublicationGroupKind,
    RenameAsideOutcome,
};

use crate::store_cell::AuthorityStore;
use crate::authoring::{invalid, require_base, AuthoringService};
use crate::coordinator::publish_incremental_paths;
use crate::atomic::{plan_same_dir_temp, unique_sibling, write_planned_temp};
use crate::pipeline_map::PipelineProjection;
use crate::quarantine::QuarantineDriver;
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

}

#[derive(Clone)]
struct OperationRuntime {
    store: Arc<AuthorityStore>,
    scanner: RootedScanner,
    quarantine: QuarantineDriver,
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
        }
    }
}

impl OperationRuntime {
    fn publish_files(
        &self,
        base: InputVersion,
        kind: PublicationGroupKind,
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
        let import_failures = if request == DoctorRequest::Verify {
            coordinator
                .as_ref()
                .expect("verify coordinator was required")
                .authoring_service()
                .verify_watched_import_fixpoints(base)
                .map_err(|error| format!("{error:?}"))?
        } else {
            Vec::new()
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
        self.advance_empty(base, terminal_error)
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
    }
}

fn path_text(path: &Path) -> Result<String, String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("path is not lossless UTF-8: {}", path.display()))
}
