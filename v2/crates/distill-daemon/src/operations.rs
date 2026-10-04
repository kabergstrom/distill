//! Durable long-running authoring operations.
//!
//! Preparation is read-only.  The filesystem/store publication is deferred
//! until the RPC progress stream reaches its terminal event, so cancellation
//! cannot strand the durable store ahead of the RPC coordinator.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use distill_bundle::Bundle;
use distill_core::id::ContentHash;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringProgressEvent, AuthoringProgressState, DeferredOperation, DeferredOperationResult,
    DoctorRequest, InputVersion, LongRunningOp, PreparedOperationCommit, RenameWithFixupsRequest,
    ReportOperation, ReportSnapshot, RpcFailure, WriteReceipt, WrittenFile,
};
use distill_schema::ngp_schema::SchemaNode;
use distill_store::{Store, StoreReader};

use crate::authoring::{invalid, require_base, AuthoringService};
use crate::compiled::CompiledRegistry;
use crate::scanner::RootedScanner;
use distill_store::atomic_file::{self, Expected};

impl AuthoringService {
    pub(crate) fn prepare_long_operation(
        &self,
        store: &mut Store,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        let runtime = OperationRuntime {
            compiled: self.compiled_registry(),
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
                    self.compiled(store)?.scanner(),
                    &request.destination_root,
                    &request.destination_path,
                )?;
                PlannedOperation::Rename(self.plan_rename_with_fixups(store, base, &request)?)
            }
            LongRunningOp::Doctor(payload) => {
                let request =
                    DoctorRequest::decode(payload).map_err(|error| invalid(error.to_string()))?;
                // The report runs when the client consumes Completed, on a
                // read snapshot of `base`; until then the progress stream
                // can be cancelled without starting any of its work.
                return Ok(PreparedOperationCommit::report(
                    Arc::new(DoctorReport { runtime, request }),
                    progress_events(&format!("doctor {request:?}"), Arc::from([])),
                ));
            }
        };
        let running_payload = operation_summary(&planned);
        // A rename's Completed event carries its receipt: the files it will
        // have changed when Completed is delivered.
        let PlannedOperation::Rename(rename) = &planned;
        let completed_payload = rename.receipt.encode();
        Ok(PreparedOperationCommit::deferred(
            Arc::new(DeferredAuthoringOperation { runtime, planned }),
            progress_events(&running_payload, completed_payload),
        ))
    }

    fn plan_rename_with_fixups(
        &self,
        store: &StoreReader,
        base: InputVersion,
        request: &RenameWithFixupsRequest,
    ) -> Result<PlannedRename, RpcFailure> {
        require_base(store, base)?;
        let compiled = self.compiled(store)?;
        let scanner = compiled.scanner();
        let moving = store
            .bundle(request.bundle)
            .map_err(invalid)?
            .ok_or_else(|| invalid(format!("cannot rename unknown bundle {}", request.bundle)))?;
        let old_root = store
            .root_name(moving.root)
            .map_err(invalid)?
            .ok_or_else(|| invalid("bundle root identity is missing"))?;
        let source = scanner
            .physical_path(&old_root, &moving.path)
            .map_err(invalid)?;
        let destination = scanner
            .physical_path(&request.destination_root, &request.destination_path)
            .map_err(invalid)?;
        if source == destination {
            return Err(invalid("rename destination is the current bundle path"));
        }
        if fs::symlink_metadata(&destination).is_ok() {
            return Err(invalid("rename destination already exists"));
        }

        // The bundles a rewrite can change: the moving one, those whose
        // published bytes reference its path, and the poisoned ones, whose
        // references are unknown because their bytes did not index.
        let mut candidates = store
            .bundles_referencing_path(&moving.path)
            .map_err(invalid)?;
        candidates.extend(store.poisoned_bundles().map_err(invalid)?);
        candidates.push(moving.bundle);
        candidates.sort();
        candidates.dedup();
        let mut referencers = Vec::new();
        let mut moved = None;
        // In the order the files change: referencers, then the move.
        let mut receipt = Vec::new();
        let mut moved_receipt = Vec::new();
        for bundle in candidates {
            let meta = store
                .bundle(bundle)
                .map_err(invalid)?
                .ok_or_else(|| invalid(format!("bundle {bundle} has references but no row")))?;
            let root = store
                .root_name(meta.root)
                .map_err(invalid)?
                .ok_or_else(|| invalid("bundle root identity is missing"))?;
            let path = scanner.physical_path(&root, &meta.path).map_err(invalid)?;
            let bytes = scanner.read_identity_checked(&path).map_err(invalid)?;
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
                let rewritten = if changed {
                    Some(distill_bundle::write_bundle(&bundle).map_err(invalid)?)
                } else {
                    None
                };
                moved_receipt.push(WrittenFile {
                    root,
                    path: meta.path,
                    content_hash: None,
                });
                moved_receipt.push(WrittenFile {
                    root: request.destination_root.clone(),
                    path: request.destination_path.clone(),
                    content_hash: Some(
                        rewritten
                            .as_deref()
                            .map_or(observed, atomic_file::content_hash),
                    ),
                });
                moved = Some((path, observed, rewritten));
            } else if changed {
                let proposed = distill_bundle::write_bundle(&bundle).map_err(invalid)?;
                receipt.push(WrittenFile {
                    root,
                    path: meta.path,
                    content_hash: Some(atomic_file::content_hash(&proposed)),
                });
                referencers.push(Rewrite {
                    target: path,
                    preimage: observed,
                    proposed,
                });
            }
        }
        let (source, source_preimage, source_rewritten) =
            moved.expect("the moving bundle is a candidate");
        receipt.extend(moved_receipt);
        Ok(PlannedRename {
            referencers,
            source,
            source_preimage,
            source_rewritten,
            destination,
            receipt: WriteReceipt { files: receipt },
        })
    }
}

/// What a deferred operation completes with: it reads the compiled state of
/// the version its own input sees, not the state it was prepared under.
#[derive(Clone)]
struct OperationRuntime {
    compiled: Arc<CompiledRegistry>,
    tag_index_coordinator: Weak<crate::coordinator::DaemonCoordinator>,
}

enum PlannedOperation {
    Rename(PlannedRename),
}

struct DeferredAuthoringOperation {
    runtime: OperationRuntime,
    planned: PlannedOperation,
}

impl DeferredOperation for DeferredAuthoringOperation {
    fn complete(
        &self,
        store: &mut Store,
        base: InputVersion,
    ) -> Result<DeferredOperationResult, String> {
        match &self.planned {
            PlannedOperation::Rename(rename) => self.runtime.complete_rename(store, base, rename),
        }
    }
}

/// `doctor verify`: a read-only report on one read snapshot. It holds no
/// write lock, writes nothing, publishes no version and repairs nothing;
/// every finding fails the operation with its description.
struct DoctorReport {
    runtime: OperationRuntime,
    request: DoctorRequest,
}

impl ReportOperation for DoctorReport {
    fn report(&self, snapshot: &ReportSnapshot<'_>) -> Result<Option<String>, String> {
        match self.request {
            DoctorRequest::Verify => self.runtime.verify(snapshot),
        }
    }
}

/// The progress of an operation that completes when the client consumes
/// Completed.
fn progress_events(running: &str, completed: Arc<[u8]>) -> Vec<AuthoringProgressEvent> {
    vec![
        AuthoringProgressEvent {
            sequence: 0,
            state: AuthoringProgressState::Started,
            payload: Arc::from([]),
        },
        AuthoringProgressEvent {
            sequence: 1,
            state: AuthoringProgressState::Running,
            payload: Arc::from(running.as_bytes()),
        },
        AuthoringProgressEvent {
            sequence: 2,
            state: AuthoringProgressState::Completed,
            payload: completed,
        },
    ]
}

impl OperationRuntime {
    /// Apply a planned rename as file writes, in the order that keeps every
    /// intermediate state ordinary authored input:
    ///
    /// 1. each referencing bundle is rewritten to the destination path (its
    ///    reference dangles until step 3, which a build reports as an
    ///    unresolved reference);
    /// 2. the moving bundle's own references are rewritten in place;
    /// 3. the bundle moves, by one rename, so it is never at both paths.
    ///
    /// Every temp is staged and every pre-image and the destination checked
    /// before the first rename, so a conflict found then changes nothing.
    /// A failure or crash part-way leaves a prefix of the steps; the same
    /// request, retried, plans exactly what remains (the published
    /// references to the old path are the referencers not yet rewritten).
    /// Nothing is published here: the watcher publishes the files.
    fn complete_rename(
        &self,
        store: &Store,
        base: InputVersion,
        rename: &PlannedRename,
    ) -> Result<DeferredOperationResult, String> {
        require_base(store, base).map_err(|error| format!("{error:?}"))?;
        let compiled = self.compiled.at(store).map_err(|error| error.to_string())?;
        let scanner = compiled.scanner();
        let stage = |target: &Path, bytes: &[u8]| {
            let root = scanner
                .root_containing(target)
                .map_err(|error| error.to_string())?;
            atomic_file::stage(&root, target, bytes).map_err(|error| error.to_string())
        };
        let mut steps = Vec::new();
        for rewrite in &rename.referencers {
            let staged = stage(&rewrite.target, &rewrite.proposed)?;
            steps.push((&rewrite.target, rewrite.preimage, staged));
        }
        if let Some(bytes) = &rename.source_rewritten {
            let staged = stage(&rename.source, bytes)?;
            steps.push((&rename.source, rename.source_preimage, staged));
        }
        for (target, preimage, _) in &steps {
            atomic_file::check(target, Expected::Hash(*preimage))
                .map_err(|error| error.to_string())?;
        }
        atomic_file::check(&rename.source, Expected::Hash(rename.source_preimage))
            .and_then(|()| atomic_file::check(&rename.destination, Expected::Absent))
            .map_err(|error| error.to_string())?;

        let moved = rename
            .source_rewritten
            .as_deref()
            .map_or(rename.source_preimage, atomic_file::content_hash);
        let total = steps.len() + 1;
        let mut applied = 0;
        let apply = || {
            for (_, preimage, staged) in steps {
                staged.commit(Expected::Hash(preimage))?;
                applied += 1;
            }
            atomic_file::move_file(&rename.source, Expected::Hash(moved), &rename.destination)
        };
        let terminal_error = apply().err().map(|error| {
            format!(
                "rename applied {applied} of {total} file steps, then failed: {error}; \
                 retry the same request to finish it"
            )
        });
        Ok(DeferredOperationResult {
            commit: None,
            terminal_error,
        })
    }

    /// Everything `doctor verify` checks, at `snapshot`: the scan of every
    /// root against the published input, each watched import's fixpoint,
    /// fresh rebuilds of every runtime entry against each other and the
    /// cached result, and every CAS extent's bytes against its hash.
    fn verify(&self, snapshot: &ReportSnapshot<'_>) -> Result<Option<String>, String> {
        let coordinator = self
            .tag_index_coordinator
            .upgrade()
            .ok_or_else(|| "build coordinator stopped during doctor verify".to_owned())?;
        let reader = snapshot.reader();
        let base = snapshot.stamp().version;
        let import_failures = coordinator
            .authoring_service()
            .verify_watched_import_fixpoints(reader, base)
            .map_err(|error| format!("{error:?}"))?;
        let compiled = self
            .compiled
            .at(reader)
            .map_err(|error| error.to_string())?;
        let observed = compiled
            .scanner()
            .scan()
            .map_err(|error| error.to_string())?;
        let filesystem_mismatch = !observed
            .matches_published(reader)
            .map_err(|error| error.to_string())?;
        let build_defects = match snapshot.verification_build_requests() {
            Ok(requests) => crate::build::doctor_verify_builds(&coordinator, reader, &requests),
            Err(error) => vec![format!(
                "build verification is unavailable for the current store state: {error:?}"
            )],
        };
        reader
            .verify_all_cas_extents()
            .map_err(|error| error.to_string())?;
        let mut defects = Vec::new();
        if filesystem_mismatch {
            defects.push(
                "full filesystem rehash differs from the published input snapshot".to_owned(),
            );
        }
        defects.extend(observed.diagnostic_rows().map(|row| row.to_string()));
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
        Ok((!defects.is_empty()).then(|| defects.join("; ")))
    }
}

/// One bundle file rewritten in place: it must hold `preimage` right before
/// it is replaced.
struct Rewrite {
    target: PathBuf,
    preimage: ContentHash,
    proposed: Vec<u8>,
}

/// A rename-with-fixups, planned against one version
/// ([`OperationRuntime::complete_rename`] applies it).
struct PlannedRename {
    /// The bundles that reference the moving one, rewritten to its
    /// destination path.
    referencers: Vec<Rewrite>,
    /// The moving bundle, its bytes, and its bytes with its own references
    /// rewritten when any changed.
    source: PathBuf,
    source_preimage: ContentHash,
    source_rewritten: Option<Vec<u8>>,
    destination: PathBuf,
    /// The files the rename changes, for its Completed event.
    receipt: WriteReceipt,
}

/// Every logical path `bundle`'s reference fields name: the strings
/// [`rewrite_bundle_path_references`] would rewrite if they were its `from`.
/// Publication stores them as `bundle_path_refs` so a rename reads only the
/// bundles it can change.
pub(crate) fn bundle_path_references(bundle: &Bundle) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for entry in bundle.assets.values() {
        // A parsed bundle holds every entry's schema snapshot.
        if let Some(schema) = bundle.schemas.get(&entry.schema_hash) {
            collect_references(&schema.root, &entry.data, &mut paths);
        }
    }
    paths
}

/// [`rewrite_value`]'s traversal, collecting each reference it would test.
fn collect_references(schema: &SchemaNode, value: &AuthoredValue, paths: &mut BTreeSet<String>) {
    match schema {
        SchemaNode::AssetRef(_) | SchemaNode::WeakRef(_) => match value {
            AuthoredValue::Str(path) => {
                paths.insert(path.clone());
            }
            AuthoredValue::Object(fields) => {
                if let Some(AuthoredValue::Str(path)) = fields.get("path") {
                    paths.insert(path.clone());
                }
            }
            _ => {}
        },
        SchemaNode::Struct { fields, .. } => {
            let AuthoredValue::Object(values) = value else {
                return;
            };
            for (name, _, field) in fields {
                if let Some(value) = values.get(name) {
                    collect_references(field, value, paths);
                }
            }
        }
        SchemaNode::Enum { variants, .. } => {
            let AuthoredValue::Object(values) = value else {
                return;
            };
            let Some((name, payload)) = values.iter().next() else {
                return;
            };
            if let Some((_, _, variant)) =
                variants.iter().find(|(candidate, _, _)| candidate == name)
            {
                collect_references(variant, payload, paths);
            }
        }
        SchemaNode::Vec(inner) | SchemaNode::Set(inner) | SchemaNode::Array { elem: inner, .. } => {
            if let AuthoredValue::Array(values) = value {
                for value in values {
                    collect_references(inner, value, paths);
                }
            }
        }
        SchemaNode::Option(inner) => {
            if !matches!(value, AuthoredValue::Null) {
                collect_references(inner, value, paths);
            }
        }
        SchemaNode::Map { key, value: item } => {
            if let AuthoredValue::Object(values) = value {
                if matches!(key.as_ref(), SchemaNode::String) {
                    for value in values.values() {
                        collect_references(item, value, paths);
                    }
                }
            }
        }
        SchemaNode::Primitive(_)
        | SchemaNode::String
        | SchemaNode::Blob
        | SchemaNode::Unit
        | SchemaNode::BackRef(_) => {}
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

fn operation_summary(operation: &PlannedOperation) -> String {
    match operation {
        PlannedOperation::Rename(rename) => {
            format!("rename: {} file(s)", rename.receipt.files.len())
        }
    }
}

#[cfg(test)]
mod reference_tests {
    //! The references publication stores for a bundle are exactly the
    //! paths a rename rewrite would change in it.

    use std::collections::BTreeMap;

    use distill_bundle::AssetEntry;
    use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
    use distill_schema::ngp_schema::{LogicalSchema, PrimitiveKind};

    use super::*;

    fn object(fields: &[(&str, AuthoredValue)]) -> AuthoredValue {
        AuthoredValue::Object(
            fields
                .iter()
                .map(|(name, value)| ((*name).to_owned(), value.clone()))
                .collect(),
        )
    }

    fn text(value: &str) -> AuthoredValue {
        AuthoredValue::Str(value.to_owned())
    }

    /// A bundle whose one entry holds references under every container the
    /// traversal descends, beside strings it must not count: plain string
    /// fields, non-string-keyed maps, inactive enum variants, and fields the
    /// schema does not name.
    fn bundle() -> Bundle {
        let target = TypeUuid([3; 16]);
        let reference = SchemaNode::AssetRef(target);
        let schema = LogicalSchema {
            root: SchemaNode::Struct {
                rev: 0,
                fields: vec![
                    ("direct".into(), 0, reference.clone()),
                    ("weak".into(), 0, SchemaNode::WeakRef(target)),
                    ("label".into(), 0, SchemaNode::String),
                    (
                        "list".into(),
                        0,
                        SchemaNode::Vec(Box::new(reference.clone())),
                    ),
                    (
                        "set".into(),
                        0,
                        SchemaNode::Set(Box::new(reference.clone())),
                    ),
                    (
                        "fixed".into(),
                        0,
                        SchemaNode::Array {
                            len: 1,
                            elem: Box::new(reference.clone()),
                        },
                    ),
                    (
                        "maybe".into(),
                        0,
                        SchemaNode::Option(Box::new(reference.clone())),
                    ),
                    (
                        "absent".into(),
                        0,
                        SchemaNode::Option(Box::new(reference.clone())),
                    ),
                    (
                        "by_name".into(),
                        0,
                        SchemaNode::Map {
                            key: Box::new(SchemaNode::String),
                            value: Box::new(reference.clone()),
                        },
                    ),
                    (
                        "by_number".into(),
                        0,
                        SchemaNode::Map {
                            key: Box::new(SchemaNode::Primitive(PrimitiveKind::U8)),
                            value: Box::new(reference.clone()),
                        },
                    ),
                    (
                        "choice".into(),
                        0,
                        SchemaNode::Enum {
                            rev: 0,
                            variants: vec![
                                ("Linked".into(), 0, reference.clone()),
                                ("Named".into(), 0, SchemaNode::String),
                            ],
                        },
                    ),
                    (
                        "other_choice".into(),
                        0,
                        SchemaNode::Enum {
                            rev: 0,
                            variants: vec![("Linked".into(), 0, reference.clone())],
                        },
                    ),
                ],
            },
        };
        let data = object(&[
            ("direct", text("a.bundle")),
            (
                "weak",
                object(&[("path", text("b.bundle")), ("local_id", text("x"))]),
            ),
            ("label", text("label.bundle")),
            (
                "list",
                AuthoredValue::Array(vec![text("c.bundle"), text("a.bundle")]),
            ),
            (
                "set",
                AuthoredValue::Array(vec![object(&[("path", text("d.bundle"))])]),
            ),
            ("fixed", AuthoredValue::Array(vec![text("e.bundle")])),
            ("maybe", text("f.bundle")),
            ("absent", AuthoredValue::Null),
            ("by_name", object(&[("k", text("g.bundle"))])),
            (
                "by_number",
                AuthoredValue::Array(vec![AuthoredValue::Array(vec![
                    AuthoredValue::UInt(1),
                    text("number.bundle"),
                ])]),
            ),
            ("choice", object(&[("Linked", text("h.bundle"))])),
            (
                "other_choice",
                object(&[("Unknown", text("unknown.bundle"))]),
            ),
            ("unnamed", text("unnamed.bundle")),
        ]);
        let hash = LogicalHash([1; 32]);
        Bundle {
            format_version: 1,
            uuid: BundleUuid([2; 16]),
            primary: None,
            schemas: BTreeMap::from([(hash, schema)]),
            assets: BTreeMap::from([(
                "main".to_owned(),
                AssetEntry {
                    uuid: AssetUuid([4; 16]),
                    type_uuid: TypeUuid([5; 16]),
                    schema_hash: hash,
                    authoring_only: false,
                    data,
                },
            )]),
        }
    }

    #[test]
    fn stored_references_are_the_paths_a_rename_rewrites() {
        let bundle = bundle();
        let references = bundle_path_references(&bundle);
        assert_eq!(
            references,
            ["a", "b", "c", "d", "e", "f", "g", "h"]
                .map(|name| format!("{name}.bundle"))
                .into()
        );
        let candidates = references.iter().cloned().chain(
            ["label", "number", "unknown", "unnamed", "x", "absent"]
                .map(|name| format!("{name}.bundle")),
        );
        for from in candidates {
            let mut rewritten = bundle.clone();
            let changed =
                rewrite_bundle_path_references(&mut rewritten, &from, "moved.bundle").unwrap();
            assert_eq!(changed, references.contains(&from), "{from}");
        }
    }
}
