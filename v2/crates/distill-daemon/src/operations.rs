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
use distill_rpc::{
    AuthoringProgressEvent, AuthoringProgressState, DeferredOperation, DeferredOperationResult,
    DoctorRequest, InputVersion, LongRunningOp, PreparedOperationCommit, RenameWithFixupsRequest,
    ReportOperation, ReportSnapshot, RpcFailure, WriteReceipt, WrittenFile,
};
use distill_schema::ngp_schema::{AssetRefValue, DynamicPayload, Patch, Value};
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
        each_reference(&entry.data, &mut |reference| {
            if let Some(path) = reference.path() {
                paths.insert(path.to_owned());
            }
        });
    }
    paths
}

fn rewrite_bundle_path_references(
    bundle: &mut Bundle,
    from: &str,
    to: &str,
) -> Result<bool, RpcFailure> {
    let mut changed = false;
    for entry in bundle.assets.values_mut() {
        changed |= rewrite_references(&mut entry.data, &mut |reference| {
            match reference.path_mut() {
                Some(path) if path == from => {
                    *path = to.to_owned();
                    true
                }
                _ => false,
            }
        });
    }
    Ok(changed)
}

/// Every asset/weak reference query in `value`, wherever the bundle parser
/// reads one and the build's wire encoder resolves one (§5, §6): under
/// options, the active variant, sequences, sets, both the keys and the
/// values of maps, and dynamic values and their patches.
fn each_reference(value: &Value, visit: &mut dyn FnMut(&AssetRefValue)) {
    match value {
        Value::AssetRef(reference) | Value::WeakRef(reference) => visit(reference),
        Value::Struct(items) | Value::Enum { payload: items, .. } => {
            items.iter().for_each(|item| each_reference(item, visit))
        }
        Value::Seq(items) => items.iter().for_each(|item| each_reference(item, visit)),
        Value::Option(Some(inner)) => each_reference(inner, visit),
        Value::Map(entries) => entries.iter().for_each(|(key, item)| {
            each_reference(key, visit);
            each_reference(item, visit);
        }),
        Value::Dynamic(dynamic) => match &dynamic.value {
            DynamicPayload::Full(value) => each_reference(value, visit),
            DynamicPayload::Sparse(patch) => each_patch_reference(patch, visit),
        },
        _ => {}
    }
}

fn each_patch_reference(patch: &Patch, visit: &mut dyn FnMut(&AssetRefValue)) {
    match patch {
        Patch::Replace(value) => each_reference(value, visit),
        Patch::Struct(slots) | Patch::Enum { payload: slots, .. } => slots
            .iter()
            .flatten()
            .for_each(|slot| each_patch_reference(slot, visit)),
        Patch::Some(inner) => each_patch_reference(inner, visit),
        Patch::Seq(items) => items.iter().for_each(|item| each_patch_reference(item, visit)),
        Patch::Map { set, remove } => {
            for (key, item) in set {
                each_reference(key, visit);
                each_patch_reference(item, visit);
            }
            remove.iter().for_each(|key| each_reference(key, visit));
        }
    }
}

/// [`each_reference`]'s walk, rewriting: `visit` returns whether it
/// changed the query. Sets and maps hold their entries in any order (the
/// writer canonicalizes), so a rewritten key needs no re-sort; a rewrite
/// that makes two elements or keys equal fails when the bundle is written.
fn rewrite_references(value: &mut Value, visit: &mut dyn FnMut(&mut AssetRefValue) -> bool) -> bool {
    let mut changed = false;
    match value {
        Value::AssetRef(reference) | Value::WeakRef(reference) => changed = visit(reference),
        Value::Struct(items) | Value::Enum { payload: items, .. } => {
            for item in items.iter_mut() {
                changed |= rewrite_references(item, visit);
            }
        }
        Value::Seq(items) => {
            for item in items {
                changed |= rewrite_references(item, visit);
            }
        }
        Value::Option(Some(inner)) => changed = rewrite_references(inner, visit),
        Value::Map(entries) => {
            for (key, item) in entries {
                changed |= rewrite_references(key, visit);
                changed |= rewrite_references(item, visit);
            }
        }
        Value::Dynamic(dynamic) => {
            changed = match &mut dynamic.value {
                DynamicPayload::Full(value) => rewrite_references(value, visit),
                DynamicPayload::Sparse(patch) => rewrite_patch_references(patch, visit),
            }
        }
        _ => {}
    }
    changed
}

fn rewrite_patch_references(
    patch: &mut Patch,
    visit: &mut dyn FnMut(&mut AssetRefValue) -> bool,
) -> bool {
    let mut changed = false;
    match patch {
        Patch::Replace(value) => changed = rewrite_references(value, visit),
        Patch::Struct(slots) | Patch::Enum { payload: slots, .. } => {
            for slot in slots.iter_mut().flatten() {
                changed |= rewrite_patch_references(slot, visit);
            }
        }
        Patch::Some(inner) => changed = rewrite_patch_references(inner, visit),
        Patch::Seq(items) => {
            for item in items {
                changed |= rewrite_patch_references(item, visit);
            }
        }
        Patch::Map { set, remove } => {
            for (key, item) in set {
                changed |= rewrite_references(key, visit);
                changed |= rewrite_patch_references(item, visit);
            }
            for key in remove {
                changed |= rewrite_references(key, visit);
            }
        }
    }
    changed
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
    use distill_schema::ngp_schema::{
        parse_value, LogicalSchema, Options, PrimitiveKind, SchemaNode,
    };

    use super::*;

    fn record(fields: Vec<(&str, SchemaNode)>) -> SchemaNode {
        let mut fields: Vec<_> = fields
            .into_iter()
            .map(|(name, node)| (name.to_owned(), 0, node))
            .collect();
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        SchemaNode::Struct { rev: 0, fields }
    }

    fn value(text: &str, schema: &SchemaNode) -> Value {
        parse_value(text, schema, Options::canonical())
            .unwrap_or_else(|error| panic!("{text}: {error}"))
    }

    fn one_entry(schema: LogicalSchema, hash: LogicalHash, data: Value) -> Bundle {
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

    /// A bundle whose one entry holds references under every container the
    /// traversal descends, beside strings it must not count: plain string
    /// fields, string map keys, a query's local id, and inactive enum
    /// variants.
    fn bundle() -> Bundle {
        let target = TypeUuid([3; 16]);
        let reference = SchemaNode::AssetRef(target);
        let root = record(vec![
            ("direct", reference.clone()),
            ("weak", SchemaNode::WeakRef(target)),
            ("label", SchemaNode::String),
            ("list", SchemaNode::Vec(Box::new(reference.clone()))),
            ("set", SchemaNode::Set(Box::new(reference.clone()))),
            (
                "fixed",
                SchemaNode::Array {
                    len: 1,
                    elem: Box::new(reference.clone()),
                },
            ),
            ("maybe", SchemaNode::Option(Box::new(reference.clone()))),
            ("absent", SchemaNode::Option(Box::new(reference.clone()))),
            (
                "by_name",
                SchemaNode::Map {
                    key: Box::new(SchemaNode::String),
                    value: Box::new(reference.clone()),
                },
            ),
            (
                "by_number",
                SchemaNode::Map {
                    key: Box::new(SchemaNode::Primitive(PrimitiveKind::U8)),
                    value: Box::new(reference.clone()),
                },
            ),
            (
                "choice",
                SchemaNode::Enum {
                    rev: 0,
                    variants: vec![
                        ("Linked".into(), 0, record(vec![("0", reference.clone())])),
                        ("Named".into(), 0, record(vec![("0", SchemaNode::String)])),
                    ],
                },
            ),
        ]);
        let data = value(
            concat!(
                r#"{"absent":null,"by_name":{"k":"g.bundle"},"by_number":[[1,"number.bundle"]],"#,
                r#""choice":{"Linked":{"0":"h.bundle"}},"direct":"a.bundle","fixed":["e.bundle"],"#,
                r#""label":"label.bundle","list":["c.bundle","a.bundle"],"maybe":"f.bundle","#,
                r#""set":[{"path":"d.bundle"}],"weak":{"asset":"x","path":"b.bundle"}}"#
            ),
            &root,
        );
        one_entry(LogicalSchema { root }, LogicalHash([1; 32]), data)
    }

    #[test]
    fn stored_references_are_the_paths_a_rename_rewrites() {
        let bundle = bundle();
        let references = bundle_path_references(&bundle);
        assert_eq!(
            references,
            ["a", "b", "c", "d", "e", "f", "g", "h", "number"]
                .map(|name| format!("{name}.bundle"))
                .into()
        );
        let candidates = references
            .iter()
            .cloned()
            .chain(["label", "k", "x", "absent"].map(|name| format!("{name}.bundle")))
            .chain(["x".to_owned(), "k".to_owned()]);
        for from in candidates {
            let mut rewritten = bundle.clone();
            let changed =
                rewrite_bundle_path_references(&mut rewritten, &from, "moved.bundle").unwrap();
            assert_eq!(changed, references.contains(&from), "{from}");
        }
    }

    #[test]
    fn references_reached_through_back_references_are_stored_and_rewritten() {
        // A { b: Option<B>, link: AssetRef },
        // B { a: Option<A>, b: Option<B>, link: AssetRef }: the inner A is
        // reached through B.b's BackRef(0), then that B's BackRef(1).
        let reference = SchemaNode::AssetRef(TypeUuid([3; 16]));
        let option = |inner| SchemaNode::Option(Box::new(inner));
        let root = record(vec![
            (
                "b",
                option(record(vec![
                    ("a", option(SchemaNode::BackRef(1))),
                    ("b", option(SchemaNode::BackRef(0))),
                    ("link", reference.clone()),
                ])),
            ),
            ("link", reference),
        ]);
        let data = value(
            concat!(
                r#"{"b":{"a":null,"b":{"a":{"b":null,"link":"inner-a.bundle"},"b":null,"#,
                r#""link":"inner-b.bundle"},"link":"b.bundle"},"link":"a.bundle"}"#
            ),
            &root,
        );
        let bundle = one_entry(LogicalSchema { root }, LogicalHash([1; 32]), data);
        let references = bundle_path_references(&bundle);
        assert_eq!(
            references,
            ["a", "b", "inner-a", "inner-b"]
                .map(|name| format!("{name}.bundle"))
                .into()
        );
        let mut rewritten = bundle.clone();
        assert!(
            rewrite_bundle_path_references(&mut rewritten, "inner-a.bundle", "moved.bundle")
                .unwrap()
        );
        assert!(bundle_path_references(&rewritten).contains("moved.bundle"));
    }

    /// One entry under a schema whose root is `{ label: String, v: shape }`,
    /// with a hash `write_bundle` accepts, holding `v` (canonical text).
    /// `label` is a decoy: a plain string naming a path, which no walker
    /// counts.
    fn shaped_bundle(shape: SchemaNode, v: &str) -> Bundle {
        let root = record(vec![("label", SchemaNode::String), ("v", shape)]);
        let data = value(&format!(r#"{{"label":"decoy.bundle","v":{v}}}"#), &root);
        let hash = distill_schema::ngp_schema::node_hash(&root).unwrap();
        one_entry(LogicalSchema { root }, hash, data)
    }

    type ShapeCase = (&'static str, SchemaNode, &'static str, &'static [&'static str]);

    /// Every container shape a reference can sit under (§5), with the
    /// paths it names. Two-reference cases name `a` and `b`, so a rename
    /// of `a` to `z.bundle` reorders a set or a map's keys.
    fn shape_cases() -> Vec<ShapeCase> {
        let reference = || SchemaNode::AssetRef(TypeUuid([3; 16]));
        let byte = || SchemaNode::Primitive(PrimitiveKind::U8);
        let map = |key, value| SchemaNode::Map {
            key: Box::new(key),
            value: Box::new(value),
        };
        let choice = || SchemaNode::Enum {
            rev: 0,
            variants: vec![
                ("Named".into(), 0, record(vec![("0", SchemaNode::String)])),
                ("Record".into(), 0, record(vec![("r", reference())])),
            ],
        };
        vec![
            ("asset reference", reference(), r#""a.bundle""#, &["a"]),
            (
                "weak reference by path and local id",
                SchemaNode::WeakRef(TypeUuid([3; 16])),
                r#"{"asset":"x","path":"a.bundle"}"#,
                &["a"],
            ),
            ("local id only", reference(), r#"{"asset":"a.bundle"}"#, &[]),
            (
                "option, some",
                SchemaNode::Option(Box::new(reference())),
                r#""a.bundle""#,
                &["a"],
            ),
            (
                "option, none",
                SchemaNode::Option(Box::new(reference())),
                "null",
                &[],
            ),
            (
                "vec",
                SchemaNode::Vec(Box::new(reference())),
                r#"["b.bundle","a.bundle"]"#,
                &["a", "b"],
            ),
            (
                "nested vec",
                SchemaNode::Vec(Box::new(SchemaNode::Vec(Box::new(reference())))),
                r#"[["a.bundle"],["b.bundle"]]"#,
                &["a", "b"],
            ),
            (
                "fixed array",
                SchemaNode::Array {
                    len: 2,
                    elem: Box::new(reference()),
                },
                r#"["b.bundle","a.bundle"]"#,
                &["a", "b"],
            ),
            (
                "set",
                SchemaNode::Set(Box::new(reference())),
                r#"["a.bundle","b.bundle"]"#,
                &["a", "b"],
            ),
            (
                "string-keyed map: values, never keys",
                map(SchemaNode::String, reference()),
                r#"{"k.bundle":"a.bundle"}"#,
                &["a"],
            ),
            (
                "integer-keyed map values",
                map(byte(), reference()),
                r#"[[1,"b.bundle"],[2,"a.bundle"]]"#,
                &["a", "b"],
            ),
            (
                "reference-keyed map keys",
                map(reference(), byte()),
                r#"[["a.bundle",1],["b.bundle",2]]"#,
                &["a", "b"],
            ),
            (
                "struct-keyed map keys",
                map(record(vec![("r", reference())]), SchemaNode::String),
                r#"[[{"r":"a.bundle"},"k.bundle"],[{"r":"b.bundle"},"k.bundle"]]"#,
                &["a", "b"],
            ),
            (
                "enum struct variant",
                choice(),
                r#"{"Record":{"r":"a.bundle"}}"#,
                &["a"],
            ),
            (
                "enum string variant",
                choice(),
                r#"{"Named":{"0":"a.bundle"}}"#,
                &[],
            ),
            (
                // Node { children: Map<u8, Node>, link: AssetRef }
                "integer-keyed map values through a back-reference",
                record(vec![
                    ("children", map(byte(), SchemaNode::BackRef(0))),
                    ("link", reference()),
                ]),
                r#"{"children":[[1,{"children":[],"link":"b.bundle"}]],"link":"a.bundle"}"#,
                &["a", "b"],
            ),
        ]
    }

    #[test]
    fn every_container_shape_stores_and_rewrites_the_same_references() {
        for (case, shape, value, names) in shape_cases() {
            let bundle = shaped_bundle(shape, value);
            let bytes = distill_bundle::write_bundle(&bundle)
                .unwrap_or_else(|error| panic!("{case}: fixture does not write: {error}"));
            assert_eq!(
                distill_bundle::parse_bundle(&bytes).unwrap(),
                bundle,
                "{case}"
            );
            let expected: BTreeSet<String> =
                names.iter().map(|name| format!("{name}.bundle")).collect();
            assert_eq!(bundle_path_references(&bundle), expected, "{case}");
            for from in expected
                .iter()
                .map(String::as_str)
                .chain(["decoy.bundle", "k.bundle", "x"])
            {
                let mut rewritten = bundle.clone();
                let changed =
                    rewrite_bundle_path_references(&mut rewritten, from, "z.bundle").unwrap();
                assert_eq!(changed, expected.contains(from), "{case}: {from}");
                // Still writable, canonically: a set or map whose order the
                // rewrite changed is sorted by the writer; what it wrote
                // reads back to the same bytes and references.
                let bytes = distill_bundle::write_bundle(&rewritten)
                    .unwrap_or_else(|error| panic!("{case}: {from}: {error}"));
                let reread = distill_bundle::parse_bundle(&bytes).unwrap();
                assert_eq!(
                    distill_bundle::write_bundle(&reread).unwrap(),
                    bytes,
                    "{case}: {from}"
                );
                let mut moved = expected.clone();
                if moved.remove(from) {
                    moved.insert("z.bundle".to_owned());
                }
                assert_eq!(bundle_path_references(&rewritten), moved, "{case}: {from}");
                assert_eq!(bundle_path_references(&reread), moved, "{case}: {from}");
            }
        }
    }

    /// The walk rewrites; a rewrite making two set elements equal is
    /// refused when the bundle is written.
    #[test]
    fn a_rewrite_that_merges_two_set_elements_is_refused() {
        let bundle = shaped_bundle(
            SchemaNode::Set(Box::new(SchemaNode::AssetRef(TypeUuid([3; 16])))),
            r#"["a.bundle","b.bundle"]"#,
        );
        let mut rewritten = bundle.clone();
        assert!(rewrite_bundle_path_references(&mut rewritten, "a.bundle", "b.bundle").unwrap());
        assert!(distill_bundle::write_bundle(&rewritten).is_err());
    }
}
