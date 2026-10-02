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
use std::sync::atomic::Ordering;

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
        heals: bool,
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
    configuration_error: Option<ConfigurationError>,
    namespace_errors: Vec<NamespaceError>,
    renames: Vec<LogicalRename>,
    tags: TagInputs,
    /// The batch revalidated a pending scan rejection's subjects.
    heals: bool,
}

pub(super) struct FullStep {
    candidate: ScanCandidate,
    claims: Vec<SourceClaims>,
    tags: TagInputs,
    heals: bool,
}

pub(super) struct RejectionStep {
    pending: PendingScanRejection,
    configuration: ConfigurationStatus,
}

/// What a scan publication pins for its projection and tag index.
struct TagInputs {
    projection: PipelineProjection,
    authority: Option<Arc<ProjectSchemaAuthority>>,
    tag_epoch: [u8; 32],
    pipeline: PipelineSnapshot,
    targets: Arc<BTreeMap<String, Target>>,
    max_dependency_depth: usize,
    scanner: RootedScanner,
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
        let base = self.server.stamp_of(store).version;
        let step = self.incremental_step(store, batch)?;
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
            queue.arm_scan();
            let base = self.server.stamp_of(store).version;
            let step = self.full_step(store);
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
        // the index rows with it.
        let index_built = self.authoring.import_index_built.load(Ordering::Acquire);
        let restore_index = || {
            self.authoring
                .import_index_built
                .store(index_built, Ordering::Release)
        };

        let (planned, overlay) = if self.needs_plan(store, &step, scope)? {
            let plan = self.plan(store, base, &step, scope);
            restore_index();
            plan?
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
        let published = match published {
            Ok(published) => published,
            Err(error) => {
                restore_index();
                return Err(CoordinatorError::Coordinated(error));
            }
        };
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
    /// state.
    pub(super) fn incremental_step(
        &self,
        store: &mut Store,
        batch: &WatcherBatch,
    ) -> Result<ScanStep, CoordinatorError> {
        let pending_subjects = locked(&self.scan)
            .rejection
            .as_ref()
            .map(|pending| pending.subjects.clone())
            .unwrap_or_default();
        let event_keys = batch
            .paths
            .iter()
            .filter_map(|event| self.scanner.event_path_key(event).ok().flatten())
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
                || self
                    .scanner
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
            let from = self.scanner.event_path_key(&rename.from)?;
            let to = self.scanner.event_path_key(&rename.to)?;
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
            let delta = self.scanner.scan_incremental_delta(&stored, &scan_paths);
            stored.finish()?;
            match delta {
                Ok(None) => return Ok(ScanStep::Unchanged),
                // The published rows the delta replaces.
                Ok(Some(delta)) => {
                    let baseline = ScanSnapshot::load_under(store, delta.affected_prefixes())?;
                    (delta, baseline)
                }
                Err(error) => {
                    locked(&self.scan).healthy = false;
                    return self.rejection_step(&error, heals);
                }
            }
        };
        if delta.is_same_namespace_observation(&baseline)
            && renames.is_empty()
            && locked(&self.scan).healthy
        {
            // Diagnostics are replaced with their affected subtree even when
            // the authored namespace itself did not change.
            return Ok(ScanStep::Diagnostics {
                under: Some(delta.affected_prefixes().to_vec()),
                rows: delta.observed().encoded_diagnostic_rows(),
                heals,
            });
        }
        let tags = self.tag_inputs();
        let claims = bundle_claims(
            delta
                .observed_bundle_entries()
                .map(|(_, source)| source.as_ref()),
            &tags.projection,
            tags.authority.as_deref(),
        )?;
        // A healed rejection's errors are not this publication's.
        let (configuration_error, namespace_errors) = if heals {
            (locked(&self.configuration_error).clone(), Vec::new())
        } else {
            (self.configuration_error(), self.pending_scan_errors())
        };
        Ok(ScanStep::Incremental(Box::new(IncrementalStep {
            baseline,
            delta,
            claims,
            configuration_error,
            namespace_errors,
            renames,
            tags,
            heals,
        })))
    }

    /// The step for one complete identity-checked namespace scan.
    pub(super) fn full_step(&self, store: &mut Store) -> Result<ScanStep, CoordinatorError> {
        match self.scanner.scan() {
            Ok(scan)
                if locked(&self.scan).healthy && self.scan_initialized.get().is_some() =>
            {
                if scan.same_namespace_observation(&ScanSnapshot::load(store)?) {
                    // Warning-grade exclusions are scanner state, not authored
                    // input: refresh them without minting an input version.
                    Ok(ScanStep::Diagnostics {
                        under: None,
                        rows: scan.encoded_diagnostic_rows(),
                        heals: false,
                    })
                } else {
                    self.candidate_step(scan, &[], true)
                }
            }
            Ok(scan) => self.candidate_step(scan, &[], true),
            Err(error) => {
                locked(&self.scan).healthy = false;
                self.rejection_step(&error, true)
            }
        }
    }

    /// The step publishing `scan` as the complete namespace.
    pub(super) fn candidate_step(
        &self,
        scan: ScanSnapshot,
        renames: &[LogicalRename],
        heals: bool,
    ) -> Result<ScanStep, CoordinatorError> {
        let tags = self.tag_inputs();
        let mut candidate =
            ScanCandidate::build(scan, self.configuration_error(), tags.authority.as_deref())?;
        if !heals {
            candidate
                .namespace_errors
                .extend(self.pending_scan_errors());
        }
        candidate.renames.extend_from_slice(renames);
        let claims = bundle_claims(
            candidate.scan.bundle_rows(),
            &tags.projection,
            tags.authority.as_deref(),
        )?;
        Ok(ScanStep::Full(Box::new(FullStep {
            candidate,
            claims,
            tags,
            heals,
        })))
    }

    /// The step recording a scan that could not observe some subjects.
    fn rejection_step(
        &self,
        error: &ScanError,
        replaces_pending: bool,
    ) -> Result<ScanStep, CoordinatorError> {
        let observed_rejection = classify_scan_rejection(&self.scanner, error)?;
        let previous_pending = locked(&self.scan).rejection.clone();
        let rejection = if replaces_pending {
            observed_rejection
        } else {
            select_scan_rejection(
                previous_pending
                    .as_ref()
                    .map(|pending| pending.rejection.clone())
                    .into_iter()
                    .chain([observed_rejection]),
            )?
        };
        let mut subjects = self.scanner.rejection_subjects(error);
        if !replaces_pending {
            if let Some(previous) = &previous_pending {
                subjects.extend(previous.subjects.iter().cloned());
            }
        }
        subjects.sort_unstable();
        subjects.dedup();
        let source_configuration = locked(&self.configuration_error).clone();
        let external_configuration = ConfigurationError::select_canonical(
            source_configuration
                .into_iter()
                .chain(rejection.configuration.clone()),
        )
        .map_err(|error| CoordinatorError::InvalidManifest(error.to_string()))?;
        let configuration =
            external_configuration.map_or(ConfigurationStatus::Ready, ConfigurationStatus::Failed);
        Ok(ScanStep::Rejection(Box::new(RejectionStep {
            pending: PendingScanRejection {
                rejection,
                subjects,
            },
            configuration,
        })))
    }

    fn tag_inputs(&self) -> TagInputs {
        let authority = self.schema_authority();
        TagInputs {
            projection: self.authoring.pipeline_projection(),
            tag_epoch: authority
                .as_ref()
                .map_or([0; 32], |authority| authority.source_hash()),
            authority,
            pipeline: self.pipeline_snapshot(),
            targets: self.build_targets.load(),
            max_dependency_depth: self.operational_configuration().max_dependency_depth,
            scanner: self.scanner.clone(),
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
            ScanStep::Diagnostics { under, rows, .. } => {
                store
                    .replace_scan_diagnostics(under.as_deref(), rows)
                    .map_err(|error| error.to_string())?;
                Ok(None)
            }
            ScanStep::Incremental(step) => {
                let tags = &step.tags;
                let inputs = PlanInputs {
                    configuration_error: step.configuration_error.clone(),
                    namespace_errors: step.namespace_errors.clone(),
                    authority: tags.authority.as_deref(),
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
                    &tags.projection,
                    tags.tag_epoch,
                )
                .map_err(|error| error.to_string())?;
                if let Some(authority) = tags.authority.clone().filter(|_| refine_tags) {
                    let affected = commit_affected_asset_bundles(&commit);
                    if !affected.is_empty() {
                        crate::build::refine_published_tag_index_incremental(
                            store,
                            tags.scanner.clone(),
                            authority,
                            tags.pipeline.clone(),
                            &tags.targets,
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
                let authority = tags.authority.clone().filter(|_| refine_tags);
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
                    &tags.projection,
                    tags.tag_epoch,
                    &step.claims,
                )
                .map_err(|error| error.to_string())?;
                if let Some(authority) = authority {
                    crate::build::refine_published_tag_index(
                        store,
                        tags.scanner.clone(),
                        authority,
                        tags.pipeline.clone(),
                        &tags.targets,
                        tags.max_dependency_depth,
                        &commit_asset_bundles(&commit, &fallback_bundles),
                    )
                    .apply(&mut commit);
                }
                Ok(Some(commit))
            }
            ScanStep::Rejection(step) => {
                let rejection = &step.pending.rejection;
                let configuration = &step.configuration;
                let version = store
                    .claims_namespace_errors()
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .chain(rejection.version.iter().cloned())
                    .collect::<Vec<_>>();
                let generation = match store
                    .configuration_state()
                    .map_err(|error| error.to_string())?
                {
                    ConfigurationState::Ready(epoch) => epoch.generation,
                    ConfigurationState::Failed { last_good, .. } => {
                        last_good.map_or(0, |epoch| epoch.generation)
                    }
                };
                store
                    .input_transaction(|transaction| {
                        transaction.set_namespace_errors(version.iter().cloned())?;
                        match configuration {
                            ConfigurationStatus::Ready => {
                                transaction.publish_configuration_ready(generation)?
                            }
                            ConfigurationStatus::Failed(error) => transaction
                                .publish_configuration_error(&error.detail, &error.message)?,
                        }
                        Ok(())
                    })
                    .map_err(|error| error.to_string())?;
                Ok(Some(Commit {
                    configuration: Some(configuration.clone()),
                    namespace_errors: Some(version),
                    ..Commit::default()
                }))
            }
        }
    }

    /// The scan health a committed `step` leaves behind.
    fn finish_scan_step(&self, step: &ScanStep) {
        match step {
            ScanStep::Unchanged => {}
            ScanStep::Diagnostics { heals, .. } => {
                if *heals {
                    locked(&self.scan).rejection.take();
                }
            }
            ScanStep::Incremental(step) => {
                let mut scan = locked(&self.scan);
                if step.heals {
                    scan.rejection.take();
                }
                scan.healthy = scan.rejection.is_none();
            }
            ScanStep::Full(step) => {
                let _ = self.scan_initialized.set(());
                let mut scan = locked(&self.scan);
                if step.heals {
                    scan.rejection.take();
                }
                scan.healthy = scan.rejection.is_none();
            }
            ScanStep::Rejection(step) => {
                locked(&self.scan).rejection = Some(step.pending.clone());
            }
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
