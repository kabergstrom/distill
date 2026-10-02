//! A reconciliation pass: everything one pass changes publishes as one input
//! version.
//!
//! A pass takes a scan step (an incremental rescan of a watcher batch, a full
//! rescan, or a scan rejection), then the imports it makes due, then the
//! acknowledgement of the watcher work it consumed, and commits all of them
//! as one input at the base it started from: one new input version, one
//! publication notification, one change-log version. It runs in three
//! phases:
//!
//! 1. **Plan.** An input opened at the base applies the scan step, discovers
//!    the directory imports and watched reimports due under it, plans their
//!    invocations, captures the `files` rows the step observed, and rolls
//!    back. Nothing it wrote is ever visible.
//! 2. **Run.** The imports run in parallel outside any write, each reading
//!    the committed `files` rows with the step's uncommitted ones over them
//!    (`FileOverlay`): what the input will hold when it publishes them.
//! 3. **Apply.** One coordinated input at the same base applies the scan
//!    step again, rediscovers the imports, publishes each run whose import is
//!    still due, acknowledges the work, and merges every step's RPC delta
//!    into the one commit served for the new version.
//!
//! The write lock is held for the plan and the apply, never across an
//! import run. A publication from elsewhere (an RPC write) that lands
//! between the plan and the apply makes the apply's base check fail
//! `Stale`: nothing of the pass is published, and the caller retries it from
//! the new base, recomputing everything. Nothing is applied twice.
//!
//! A run whose read set moved before the apply, or an import the apply finds
//! due that the plan did not, is left for another pass (`more_work`), with
//! the pass's watcher work kept pending so that pass finds it again. One
//! import reading another's output in the same pass is such a drift: it
//! publishes in the next pass's version.

use std::collections::BTreeSet;

use distill_store::files::ObservedDiagnostic;

use super::*;
use crate::importer::{FileOverlay, PassImport, PassPublication, PlannedImport};

/// What a reconciliation pass did.
#[derive(Debug, Clone)]
pub struct PassOutcome {
    /// The version the store is at afterwards: the pass's own, or the base
    /// when it published nothing.
    pub stamp: SnapshotStamp,
    /// Work remains that this pass could not finish: a run drifted, an import
    /// became due after the plan, or a path's watcher work moved on. The
    /// caller runs another pass.
    pub more_work: bool,
    /// Importers that failed with no bundle yet to hold the failure memo.
    /// The rest of the pass published; these rerun when their sources
    /// change.
    pub failures: Vec<String>,
    /// The bundles whose imports published.
    pub imported: Vec<BundleUuid>,
}

/// The scan half of a pass, prepared outside any write. Applying it is
/// deterministic at its base, so the plan and the apply write the same rows.
pub(super) enum ScanStep {
    /// The scan observed nothing new.
    Unchanged,
    /// Only warning-grade exclusions changed: scanner state, not authored
    /// input, so no input version.
    Diagnostics {
        under: Option<Vec<(String, String)>>,
        rows: Vec<ObservedDiagnostic>,
    },
    Incremental(Box<IncrementalStep>),
    Full(Box<FullStep>),
    /// The scan could not observe some subjects: the namespace keeps what it
    /// published and only its errors change.
    Rejection(Box<RejectionStep>),
}

pub(super) struct IncrementalStep {
    baseline: ScanSnapshot,
    delta: ScanDelta,
    claims: Vec<SourceClaims>,
    renames: Vec<LogicalRename>,
    tags: TagInputs,
    /// The batch revalidated a pending scan rejection's subjects: the
    /// publication heals it.
    heals: bool,
}

pub(super) struct FullStep {
    candidate: ScanCandidate,
    claims: Vec<SourceClaims>,
    tags: TagInputs,
    /// The scan observed every root: the publication heals the pending scan
    /// rejection.
    heals: bool,
    source_error: SourceError,
}

pub(super) struct RejectionStep {
    observed: PendingScanRejection,
    /// The scan observed every root: its rejection replaces the pending
    /// one instead of joining it.
    replaces_pending: bool,
}

/// What a step does to the configuration source's stored error.
#[derive(Clone)]
pub(super) enum SourceError {
    Keep,
    Set(ConfigurationError),
    Heal,
}

/// What a scan publication pins for its projection and tag index: the
/// compiled state of the version the pass starts from.
struct TagInputs {
    compiled: Arc<Compiled>,
    tag_epoch: [u8; 32],
    max_dependency_depth: usize,
}

impl TagInputs {
    fn authority(&self) -> Option<Arc<ProjectSchemaAuthority>> {
        self.compiled.schema_authority()
    }
}

/// Which imports a pass reconciles.
#[derive(Clone, Copy)]
pub(super) struct ImportScope<'a> {
    directories: bool,
    watched: bool,
    /// `None` revalidates every import; `Some` only those the watcher work
    /// can affect.
    affected: Option<Affected<'a>>,
    /// The process loop's pass: it acknowledges the watcher work it
    /// consumed and persists a latched runtime pipeline failure.
    loop_pass: bool,
}

#[derive(Clone, Copy)]
struct Affected<'a> {
    /// The work to reconcile; `None` reads it inside the pass.
    work: Option<&'a PendingFileWork>,
    capabilities_changed: bool,
}

impl<'a> ImportScope<'a> {
    /// A scan only.
    pub(super) const NONE: Self = Self {
        directories: false,
        watched: false,
        affected: None,
        loop_pass: false,
    };

    fn loop_pass(affected: Option<Affected<'a>>) -> Self {
        Self {
            directories: true,
            watched: true,
            affected,
            loop_pass: true,
        }
    }

    pub(super) fn directories(affected: Option<(&'a PendingFileWork, bool)>) -> Self {
        Self {
            directories: true,
            affected: affected.map(Affected::supplied),
            ..Self::NONE
        }
    }

    pub(super) fn watched(affected: Option<(&'a PendingFileWork, bool)>) -> Self {
        Self {
            watched: true,
            affected: affected.map(Affected::supplied),
            ..Self::NONE
        }
    }

    fn imports(&self) -> bool {
        self.directories || self.watched
    }
}

impl<'a> Affected<'a> {
    fn supplied((work, capabilities_changed): (&'a PendingFileWork, bool)) -> Self {
        Self {
            work: Some(work),
            capabilities_changed,
        }
    }
}

type PlannedImports = Vec<(PassImport, Option<PlannedImport>)>;

impl DaemonCoordinator {
    /// Reconcile one watcher batch: rescan its paths and run the imports its
    /// work affects, as one pass.
    pub fn reconcile_batch(
        &self,
        store: &mut Store,
        batch: &WatcherBatch,
        capabilities_changed: bool,
    ) -> Result<PassOutcome, CoordinatorError> {
        let compiled = self.loop_compiled(store)?;
        let base = self.server.stamp_of(store).version;
        let step = self.incremental_step(&compiled, store, batch)?;
        self.pass(
            store,
            base,
            step,
            ImportScope::loop_pass(Some(Affected {
                work: None,
                capabilities_changed,
            })),
        )
    }

    /// Startup or recovery: a complete scan and every import revalidated,
    /// as one pass. Events that arrive during the scan stay queued for the
    /// next pass; an overflow during it discards the scan and repeats it, so
    /// the pass publishes a settled tree.
    pub fn reconcile_rescan(
        &self,
        store: &mut Store,
        queue: &mut WatcherQueue,
    ) -> Result<PassOutcome, CoordinatorError> {
        loop {
            let compiled = self.loop_compiled(store)?;
            queue.arm_scan();
            let base = self.server.stamp_of(store).version;
            let step = self.full_step(&compiled, store);
            let action = queue.finish_scan();
            if let WatcherAction::FullRescan = action {
                continue;
            }
            if let WatcherAction::Failed(message) = action {
                return Err(CoordinatorError::InvalidManifest(message));
            }
            // Keep anything that arrived while traversal was active for the
            // next pass, whatever this one does.
            queue.requeue_action(action);
            return self.pass(store, base, step?, ImportScope::loop_pass(None));
        }
    }

    /// Run one pass at `base` (see the module docs).
    pub(super) fn pass(
        &self,
        store: &mut Store,
        base: InputVersion,
        step: ScanStep,
        scope: ImportScope<'_>,
    ) -> Result<PassOutcome, CoordinatorError> {
        use rayon::prelude::*;

        // The import index is built in an input; one that rolls back takes
        // the index rows and their built marker with it.
        let (planned, overlay) = if self.needs_plan(store, &step, scope)? {
            self.plan(store, base, &step, scope)?
        } else {
            (Vec::new(), None)
        };
        let mut runs = Vec::with_capacity(planned.len());
        for (import, run) in planned
            .into_par_iter()
            .map(|(import, planned)| {
                let run = match planned {
                    Some(planned) => self.authoring.run_pass_import(planned, overlay.as_ref()),
                    None => Ok(None),
                };
                (import, run)
            })
            .collect::<Vec<_>>()
        {
            let run = run.map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))?;
            runs.push((import, run));
        }

        let mut more_work = false;
        let mut failures = Vec::new();
        let mut imported = Vec::new();
        let published = self.server.coordinated_maybe_commit(store, base, |store| {
            if scope.loop_pass {
                // First: the scan's commit carries the pipeline diagnostic
                // as the store then holds it.
                self.sync_runtime_pipeline_failure(store)
                    .map_err(|error| error.to_string())?;
            }
            let mut commit = self.apply_scan_step(store, &step, true)?;
            if !scope.imports() {
                return Ok(commit);
            }
            let work = match scope.affected.and_then(|affected| affected.work) {
                Some(work) => work.clone(),
                None => store.pending_file_work().map_err(|error| error.to_string())?,
            };
            for import in self.discover(store, &work, scope)? {
                let Some(index) = runs.iter().position(|(planned, _)| *planned == import) else {
                    // Due now, but not when the pass planned.
                    more_work = true;
                    continue;
                };
                let (_, run) = runs.swap_remove(index);
                let Some(run) = run else {
                    // Deferred until its importer is registered.
                    continue;
                };
                match self
                    .authoring
                    .publish_pass_import(store, run)
                    .map_err(|error| format!("{error:?}"))?
                {
                    PassPublication::Published(prepared) => {
                        imported.push(prepared.bundle);
                        absorb(&mut commit, prepared.commit);
                    }
                    PassPublication::Memoized => {}
                    PassPublication::Drifted => more_work = true,
                    PassPublication::Failed(error) => {
                        tracing::warn!(?import, ?error, "import failed with no bundle to hold its failure");
                        failures.push(format!("{error:?}"));
                    }
                }
            }
            // Work whose imports did not all publish stays pending, so the
            // next pass finds them again.
            if scope.loop_pass && !more_work && !work.is_empty() {
                // A path whose observation moved on has more work queued
                // behind it: it stays pending too.
                more_work = !store
                    .acknowledge_file_work(&work)
                    .map_err(|error| error.to_string())?;
            }
            Ok(commit)
        });
        let published = published.map_err(CoordinatorError::Coordinated)?;
        self.finish_scan_step(&step);
        Ok(PassOutcome {
            stamp: published.unwrap_or_else(|| self.server.stamp_of(store)),
            more_work,
            failures,
            imported,
        })
    }

    /// Whether a pass needs its plan phase: a pass with no imports to
    /// consider has nothing to run.
    fn needs_plan(
        &self,
        store: &mut Store,
        step: &ScanStep,
        scope: ImportScope<'_>,
    ) -> Result<bool, CoordinatorError> {
        if !scope.imports() {
            return Ok(false);
        }
        let Some(affected) = scope.affected else {
            return Ok(true);
        };
        if affected.capabilities_changed
            || matches!(step, ScanStep::Incremental(_) | ScanStep::Full(_))
        {
            return Ok(true);
        }
        Ok(match affected.work {
            Some(work) => !work.is_empty(),
            None => !store.pending_file_work()?.is_empty(),
        })
    }

    /// The plan phase: apply `step` and discover and plan the imports due
    /// under it, in an input at `base` that always rolls back.
    fn plan(
        &self,
        store: &mut Store,
        base: InputVersion,
        step: &ScanStep,
        scope: ImportScope<'_>,
    ) -> Result<(PlannedImports, Option<FileOverlay>), CoordinatorError> {
        let publication =
            |error: String| CoordinatorError::Coordinated(CoordinatedCommitError::Publication(error));
        let observed = store
            .open_input()
            .map_err(|error| publication(error.to_string()))?;
        let plan = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if observed != base {
                return Err(CoordinatorError::Coordinated(CoordinatedCommitError::Stale {
                    expected: base,
                    observed,
                }));
            }
            self.apply_scan_step(store, step, false).map_err(publication)?;
            let overlay = match step {
                ScanStep::Incremental(step) => {
                    Some(FileOverlay::capture(store, Some(step.delta.affected_prefixes()))?)
                }
                ScanStep::Full(_) => Some(FileOverlay::capture(store, None)?),
                _ => None,
            };
            let work = match scope.affected.and_then(|affected| affected.work) {
                Some(work) => work.clone(),
                None => store.pending_file_work()?,
            };
            let mut planned = Vec::new();
            for import in self.discover(store, &work, scope).map_err(publication)? {
                let plan = self
                    .authoring
                    .plan_pass_import(store, &import)
                    .map_err(|error| publication(format!("{error:?}")))?;
                planned.push((import, plan));
            }
            Ok((planned, overlay))
        }));
        let rolled_back = store.finish_input(false);
        let plan = match plan {
            Ok(plan) => plan,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        rolled_back.map_err(|error| publication(error.to_string()))?;
        plan
    }

    /// The imports due under `work`: directory imports first, then watched
    /// reimports. A directory output the rules regenerate is not also
    /// reimported from its stale record.
    fn discover(
        &self,
        store: &mut Store,
        work: &PendingFileWork,
        scope: ImportScope<'_>,
    ) -> Result<Vec<PassImport>, String> {
        let failure = |error: RpcFailure| format!("{error:?}");
        let mut imports = Vec::new();
        let mut regenerated = BTreeSet::new();
        if scope.directories {
            let tasks = match scope.affected {
                None => self.authoring.directory_import_tasks(store),
                Some(affected) if affected.capabilities_changed => self
                    .authoring
                    .directory_import_tasks_affected_by_capabilities(store, &work.dirty, &work.renames),
                Some(_) => self
                    .authoring
                    .directory_import_tasks_affected_by(store, &work.dirty, &work.renames),
            }
            .map_err(failure)?;
            for task in tasks {
                if let Some(bundle) = self
                    .authoring
                    .directory_task_destination(store, &task)
                    .map_err(failure)?
                {
                    regenerated.insert(bundle);
                }
                imports.push(PassImport::Directory(task));
            }
        }
        if scope.watched {
            let bundles = match scope.affected {
                None => self.authoring.watched_imports_needing_reimport(store),
                Some(affected) if affected.capabilities_changed => self
                    .authoring
                    .watched_imports_affected_by_capabilities(store, &work.dirty, &work.renames),
                Some(_) => self
                    .authoring
                    .watched_imports_affected_by(store, &work.dirty, &work.renames),
            }
            .map_err(failure)?;
            imports.extend(
                bundles
                    .into_iter()
                    .filter(|bundle| !regenerated.contains(bundle))
                    .map(PassImport::Watched),
            );
        }
        Ok(imports)
    }

    // ---- scan steps ----

    /// The step for one watcher batch: reopen only its affected paths or
    /// directory subtrees and merge those observations into the published
    /// state, under `compiled`, the compiled state of the pass's base.
    pub(super) fn incremental_step(
        &self,
        compiled: &Arc<Compiled>,
        store: &mut Store,
        batch: &WatcherBatch,
    ) -> Result<ScanStep, CoordinatorError> {
        let scanner = compiled.scanner();
        let pending = PendingScanRejection::stored(store)?;
        let healthy = pending.is_none();
        let pending_subjects = pending.map(|pending| pending.subjects).unwrap_or_default();
        let event_keys = batch
            .paths
            .iter()
            .filter_map(|event| scanner.event_path_key(event).ok().flatten())
            .collect::<Vec<_>>();
        let logical_contains = |prefix: &(String, String), candidate: &(String, String)| {
            prefix.0 == candidate.0
                && (prefix.1.is_empty()
                    || candidate.1 == prefix.1
                    || candidate
                        .1
                        .strip_prefix(&prefix.1)
                        .is_some_and(|suffix| suffix.starts_with('/')))
        };
        let heals = pending_subjects.iter().any(|subject| {
            let physical_overlap = batch.paths.iter().any(|event| {
                event == subject || subject.starts_with(event) || event.starts_with(subject)
            });
            physical_overlap
                || scanner
                    .event_path_key(subject)
                    .ok()
                    .flatten()
                    .is_some_and(|subject_key| {
                        event_keys.iter().any(|event_key| {
                            logical_contains(&subject_key, event_key)
                                || logical_contains(event_key, &subject_key)
                        })
                    })
        });
        let mut scan_paths = batch.paths.clone();
        if heals {
            // Revalidate the complete rejected subject set, but no unrelated
            // root or subtree. This lets independently repaired defects heal
            // across separate native batches without falling back to a full
            // scan.
            scan_paths.extend(pending_subjects);
            scan_paths.sort_unstable();
            scan_paths.dedup();
        }
        let mut renames = Vec::new();
        for rename in &batch.renames {
            let from = scanner.event_path_key(&rename.from)?;
            let to = scanner.event_path_key(&rename.to)?;
            if let (Some((from_root, from_path)), Some((to_root, to_path))) = (from, to) {
                if from_root == to_root {
                    renames.push(LogicalRename {
                        root_name: from_root,
                        from_path,
                        to_path,
                    });
                }
            }
        }
        let (delta, baseline) = {
            let stored = StoredBaseline::new(store);
            let delta = scanner.scan_incremental_delta(&stored, &scan_paths);
            stored.finish()?;
            match delta {
                Ok(None) => return Ok(ScanStep::Unchanged),
                // The published rows the delta replaces.
                Ok(Some(delta)) => {
                    let baseline = ScanSnapshot::load_under(store, delta.affected_prefixes())?;
                    (delta, baseline)
                }
                Err(error) => return self.rejection_step(compiled, &error, heals),
            }
        };
        // A rename from a path never observed (the temporary file of an
        // atomic write, the daemon's own included) moves no identity: the
        // echo of an import's bundle write publishes nothing.
        if delta.is_same_namespace_observation(&baseline)
            && renames.iter().all(|rename| {
                delta.rename_moves_nothing(&baseline, &rename.root_name, &rename.from_path)
            })
            && healthy
        {
            // Diagnostics are replaced with their affected subtree even when
            // the authored namespace itself did not change.
            return Ok(ScanStep::Diagnostics {
                under: Some(delta.affected_prefixes().to_vec()),
                rows: delta.observed().encoded_diagnostic_rows(),
            });
        }
        let tags = self.tag_inputs(compiled);
        let claims = bundle_claims(
            delta
                .observed_bundle_entries()
                .map(|(_, source)| source.as_ref()),
            compiled.projection(),
            tags.authority().as_deref(),
        )?;
        Ok(ScanStep::Incremental(Box::new(IncrementalStep {
            baseline,
            delta,
            claims,
            renames,
            tags,
            heals,
        })))
    }

    /// The step for one complete identity-checked namespace scan.
    pub(super) fn full_step(
        &self,
        compiled: &Arc<Compiled>,
        store: &mut Store,
    ) -> Result<ScanStep, CoordinatorError> {
        match compiled.scanner().scan() {
            Ok(scan)
                if PendingScanRejection::stored(store)?.is_none()
                    && self.scan_initialized.get().is_some() =>
            {
                if scan.matches_published(store, false)? {
                    // Warning-grade exclusions are scanner state, not authored
                    // input: refresh them without minting an input version.
                    Ok(ScanStep::Diagnostics {
                        under: None,
                        rows: scan.encoded_diagnostic_rows(),
                    })
                } else {
                    self.candidate_step(compiled, scan, &[], true, SourceError::Keep)
                }
            }
            Ok(scan) => self.candidate_step(compiled, scan, &[], true, SourceError::Keep),
            Err(error) => self.rejection_step(compiled, &error, true),
        }
    }

    /// The step publishing `scan` as the complete namespace. `heals` when
    /// it observed every root.
    pub(super) fn candidate_step(
        &self,
        compiled: &Arc<Compiled>,
        scan: ScanSnapshot,
        renames: &[LogicalRename],
        heals: bool,
        source_error: SourceError,
    ) -> Result<ScanStep, CoordinatorError> {
        let tags = self.tag_inputs(compiled);
        let authority = tags.authority();
        let mut candidate = ScanCandidate::build(scan, authority.as_deref())?;
        candidate.renames.extend_from_slice(renames);
        let claims = bundle_claims(
            candidate.scan.bundle_rows(),
            compiled.projection(),
            authority.as_deref(),
        )?;
        Ok(ScanStep::Full(Box::new(FullStep {
            candidate,
            claims,
            tags,
            heals,
            source_error,
        })))
    }

    /// The step recording a scan that could not observe some subjects. Its
    /// rejection replaces the pending one when `replaces_pending`, and joins
    /// it otherwise.
    fn rejection_step(
        &self,
        compiled: &Compiled,
        error: &ScanError,
        replaces_pending: bool,
    ) -> Result<ScanStep, CoordinatorError> {
        let scanner = compiled.scanner();
        let rejection = classify_scan_rejection(scanner, error)?;
        let mut subjects = scanner.rejection_subjects(error);
        subjects.sort_unstable();
        subjects.dedup();
        Ok(ScanStep::Rejection(Box::new(RejectionStep {
            observed: PendingScanRejection {
                rejection,
                subjects,
            },
            replaces_pending,
        })))
    }

    fn tag_inputs(&self, compiled: &Arc<Compiled>) -> TagInputs {
        TagInputs {
            tag_epoch: compiled
                .schema_authority()
                .map_or([0; 32], |authority| authority.source_hash()),
            compiled: Arc::clone(compiled),
            max_dependency_depth: self.operational_configuration().max_dependency_depth,
        }
    }

    /// Apply `step` inside the input open on `store`, refining the published
    /// tag index when `refine_tags`. Returns its RPC delta, if it publishes.
    fn apply_scan_step(
        &self,
        store: &mut Store,
        step: &ScanStep,
        refine_tags: bool,
    ) -> Result<Option<Commit>, String> {
        match step {
            ScanStep::Unchanged => Ok(None),
            ScanStep::Diagnostics { under, rows } => {
                store
                    .replace_scan_diagnostics(under.as_deref(), rows)
                    .map_err(|error| error.to_string())?;
                Ok(None)
            }
            ScanStep::Incremental(step) => {
                let tags = &step.tags;
                if step.heals {
                    // The batch revalidated every rejected subject.
                    store
                        .input_transaction(|transaction| transaction.set_scan_rejection(None))
                        .map_err(|error| error.to_string())?;
                }
                let authority = tags.authority();
                let inputs = PlanInputs {
                    authority: authority.as_deref(),
                    fresh: fresh_bundles(&step.delta),
                };
                let mut commit = publish_incremental_scan(
                    store,
                    store.input_version(),
                    &step.baseline,
                    &step.delta,
                    &step.claims,
                    &inputs,
                    &step.renames,
                    tags.compiled.projection(),
                    tags.tag_epoch,
                )
                .map_err(|error| error.to_string())?;
                if let Some(authority) = authority.filter(|_| refine_tags) {
                    let affected = commit_affected_asset_bundles(&commit);
                    if !affected.is_empty() {
                        crate::build::refine_published_tag_index_incremental(
                            crate::build::OpenInput::new(store)
                                .expect("tag-index refinement runs inside its input"),
                            tags.compiled.scanner().clone(),
                            authority,
                            tags.compiled.pipeline_snapshot(),
                            tags.compiled.build_targets(),
                            tags.max_dependency_depth,
                            &affected,
                        )
                        .apply_incremental(&mut commit);
                    }
                }
                Ok(Some(commit))
            }
            ScanStep::Full(step) => {
                let tags = &step.tags;
                if step.heals || !matches!(step.source_error, SourceError::Keep) {
                    store
                        .input_transaction(|transaction| {
                            if step.heals {
                                transaction.set_scan_rejection(None)?;
                            }
                            match &step.source_error {
                                SourceError::Keep => {}
                                SourceError::Set(error) => {
                                    transaction.set_configuration_source_error(Some(error))?
                                }
                                SourceError::Heal => {
                                    transaction.set_configuration_source_error(None)?
                                }
                            }
                            Ok(())
                        })
                        .map_err(|error| error.to_string())?;
                }
                let authority = tags.authority().filter(|_| refine_tags);
                let fallback_bundles = match authority {
                    Some(_) => store
                        .all_asset_bundles()
                        .map_err(|error| error.to_string())?,
                    None => BTreeMap::new(),
                };
                let mut commit = publish_scan(
                    store,
                    store.input_version(),
                    step.candidate.clone(),
                    false,
                    None,
                    tags.compiled.projection(),
                    tags.tag_epoch,
                    &step.claims,
                )
                .map_err(|error| error.to_string())?;
                if let Some(authority) = authority {
                    crate::build::refine_published_tag_index(
                        crate::build::OpenInput::new(store)
                            .expect("tag-index refinement runs inside its input"),
                        tags.compiled.scanner().clone(),
                        authority,
                        tags.compiled.pipeline_snapshot(),
                        tags.compiled.build_targets(),
                        tags.max_dependency_depth,
                        &commit_asset_bundles(&commit, &fallback_bundles),
                    )
                    .apply(&mut commit);
                }
                Ok(Some(commit))
            }
            ScanStep::Rejection(step) => {
                let failed = |error: CoordinatorError| error.to_string();
                // The rejection joins the one the store holds, unless this
                // scan observed every root.
                let pending = if step.replaces_pending {
                    step.observed.clone()
                } else {
                    let previous = PendingScanRejection::stored(store)
                        .map_err(|error| error.to_string())?;
                    let rejection = select_scan_rejection(
                        previous
                            .as_ref()
                            .map(|previous| previous.rejection.clone())
                            .into_iter()
                            .chain([step.observed.rejection.clone()]),
                    )
                    .map_err(failed)?;
                    let mut subjects = step.observed.subjects.clone();
                    subjects.extend(previous.into_iter().flat_map(|previous| previous.subjects));
                    subjects.sort_unstable();
                    subjects.dedup();
                    PendingScanRejection {
                        rejection,
                        subjects,
                    }
                };
                let claims_errors = store
                    .claims_namespace_errors()
                    .map_err(|error| error.to_string())?;
                let generation = match store
                    .configuration_state()
                    .map_err(|error| error.to_string())?
                {
                    ConfigurationState::Ready(epoch) => epoch.generation,
                    ConfigurationState::Failed { last_good, .. } => {
                        last_good.map_or(0, |epoch| epoch.generation)
                    }
                };
                let (configuration, _) = store
                    .input_transaction(|transaction| {
                        transaction.set_namespace_errors(claims_errors)?;
                        transaction.set_scan_rejection(Some(&pending.record()))?;
                        transaction.publish_configuration_status(generation)
                    })
                    .map_err(|error| error.to_string())?;
                let namespace_errors = store
                    .namespace_errors()
                    .map_err(|error| error.to_string())?;
                Ok(Some(Commit {
                    configuration: Some(configuration_status(configuration)),
                    namespace_errors: Some(namespace_errors),
                    ..Commit::default()
                }))
            }
        }
    }

    /// What a committed `step` leaves behind in this process: a full scan
    /// is this process's own observation.
    fn finish_scan_step(&self, step: &ScanStep) {
        if let ScanStep::Full(_) = step {
            let _ = self.scan_initialized.set(());
        }
    }
}

/// Fold `later`, the RPC delta of a step applied after `commit`'s, into
/// `commit`: one delta from the base to the state after both. A later
/// mutation of the same asset, entry, path or output replaces the earlier
/// one; a later complete replacement replaces everything before it.
fn absorb(commit: &mut Option<Commit>, later: Commit) {
    match commit {
        Some(commit) => absorb_into(commit, later),
        None => *commit = Some(later),
    }
}

fn absorb_into(commit: &mut Commit, later: Commit) {
    fn keyed<T, K: Ord>(earlier: &mut Vec<T>, later: Vec<T>, key: impl Fn(&T) -> K) {
        let replaced = later.iter().map(&key).collect::<BTreeSet<_>>();
        earlier.retain(|item| !replaced.contains(&key(item)));
        earlier.extend(later);
    }
    keyed(&mut commit.assets, later.assets, |mutation| match mutation {
        AssetMutation::Set { uuid, .. } | AssetMutation::Remove { uuid, .. } => *uuid,
    });
    keyed(&mut commit.authoring, later.authoring, |mutation| match mutation {
        AuthoringMutation::Set(entry) => entry.uuid,
        AuthoringMutation::Remove { uuid } => *uuid,
    });
    keyed(&mut commit.paths, later.paths, |mutation| match mutation {
        PathMutation::Set { path, .. } | PathMutation::Remove { path } => path.clone(),
    });
    if later.derived_outputs.is_some() {
        commit.derived_outputs = later.derived_outputs;
        commit.derived_output_mutations.clear();
    }
    keyed(
        &mut commit.derived_output_mutations,
        later.derived_output_mutations,
        |mutation| match mutation {
            DerivedOutputMutation::Set { child, .. } | DerivedOutputMutation::Remove { child } => {
                *child
            }
        },
    );
    if later.tag_poisons.is_some() {
        commit.tag_poisons = later.tag_poisons;
        commit.tag_poison_mutations.clear();
    }
    keyed(
        &mut commit.tag_poison_mutations,
        later.tag_poison_mutations,
        |mutation| match mutation {
            distill_rpc::TagPoisonMutation::Set { asset, .. }
            | distill_rpc::TagPoisonMutation::Remove { asset } => *asset,
        },
    );
    if later.tag_projection.is_some() {
        commit.tag_projection = later.tag_projection;
        commit.tag_projection_mutations.clear();
    }
    keyed(
        &mut commit.tag_projection_mutations,
        later.tag_projection_mutations,
        |mutation| match mutation {
            distill_rpc::TagProjectionMutation::Set { asset, .. }
            | distill_rpc::TagProjectionMutation::Remove { asset } => *asset,
        },
    );
    if later.configuration.is_some() {
        commit.configuration = later.configuration;
    }
    if later.pipeline.is_some() {
        commit.pipeline = later.pipeline;
    }
    commit.pipeline_epoch_changed |= later.pipeline_epoch_changed;
    if later.namespace_errors.is_some() {
        commit.namespace_errors = later.namespace_errors;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(path: &str) -> PathMutation {
        PathMutation::Remove {
            path: path.to_owned(),
        }
    }

    #[test]
    fn absorbing_a_later_delta_keeps_one_mutation_per_key_and_the_later_replacements() {
        let mut commit = None;
        absorb(
            &mut commit,
            Commit {
                paths: vec![path("a"), path("b")],
                namespace_errors: Some(Vec::new()),
                pipeline_epoch_changed: true,
                ..Commit::default()
            },
        );
        absorb(
            &mut commit,
            Commit {
                paths: vec![
                    PathMutation::Set {
                        path: "b".to_owned(),
                        candidates: BTreeSet::new(),
                    },
                    path("c"),
                ],
                configuration: Some(ConfigurationStatus::Ready),
                ..Commit::default()
            },
        );
        let commit = commit.unwrap();
        assert_eq!(
            commit.paths,
            vec![
                path("a"),
                PathMutation::Set {
                    path: "b".to_owned(),
                    candidates: BTreeSet::new(),
                },
                path("c"),
            ]
        );
        assert_eq!(commit.namespace_errors, Some(Vec::new()));
        assert_eq!(commit.configuration, Some(ConfigurationStatus::Ready));
        assert!(commit.pipeline_epoch_changed);
    }
}
