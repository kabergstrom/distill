//! A reconciliation pass: everything one pass changes publishes as one input
//! version.
//!
//! A pass takes a scan step (an incremental rescan of a watcher batch, a full
//! rescan, or a scan rejection), then the imports it makes due, then the
//! acknowledgement of the watcher work it consumed, and commits all of them
//! as one input at the base it started from: one new input version, one
//! publication notification, one change-log version.
//!
//! An input opened at the base applies the scan step, brings the import
//! index up to date for the pending watcher work, and discovers the
//! directory imports and watched reimports due under it. When none is due
//! (every pass but those that import), it refines the tag index,
//! acknowledges the work and commits: the step is applied once. When some
//! are, the pass plans their invocations, rolls the input back (nothing it
//! wrote is ever visible) and runs in three more phases:
//!
//! 1. **Run.** The imports run in parallel outside any write, each reading
//!    the committed `files` rows with the step's changed ones over them
//!    (`FileOverlay`, which holds only the rows that differ): what the
//!    input will hold when it publishes them.
//! 2. **Chain.** Imports that read the outputs of imports the pass runs
//!    (an imported bundle that is another import's source) run in levels
//!    after them. For each level, a plan input at the base (rolled back like
//!    the first) folds the last level's runs to the bundles they will
//!    publish, discovers the imports due under the outputs that change,
//!    reading those bytes in place of the files, and plans them; they then
//!    run against the same outputs over the `FileOverlay`. An import runs
//!    once, after every import whose output it reads: one planned earlier
//!    moves to the later level. One that would read its own output again is
//!    a cycle: it is cut, reported in `failures`, and its next run is the
//!    next pass's. A chain deeper than `MAX_IMPORT_CHAIN_LEVELS` is
//!    reported too, and the rest runs in the next pass (`more_work`). A
//!    level runs only when the committed import index has an import that
//!    may read one of the outputs.
//! 3. **Apply.** One coordinated input at the same base applies the scan
//!    step again (the write lock is released while imports run, so the
//!    first input cannot stay open), rediscovers the imports level by level
//!    (each level under the paths the level before published), publishes
//!    each run whose import is still due, acknowledges the work, and merges
//!    every step's RPC delta into the one commit served for the new
//!    version. Each chained run is revalidated against the outputs the input
//!    now holds, so a run whose upstream published other bytes than planned
//!    drifts.
//!
//! The write lock is held for the inputs, never across an import run. A
//! publication from elsewhere (an RPC write) that lands between the first
//! input and the apply makes the apply's base check fail `Stale`: nothing
//! of the pass is published, and the caller retries it from the new base,
//! recomputing everything.
//!
//! A run whose read set moved before the apply, or an import the apply finds
//! due that the plan did not, is left for another pass (`more_work`), with
//! the pass's watcher work kept pending so that pass finds it again.

use std::collections::BTreeSet;

use distill_store::files::ObservedFile;

use super::*;
use crate::importer::{
    FileOverlay, ImportRun, PassImport, PassOutput, PassPublication, PlannedImport,
};

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
    Incremental(Box<IncrementalStep>),
    Full(Box<FullStep>),
    /// The scan could not observe some subjects: the namespace keeps what it
    /// published, and the rejection becomes the pending one.
    Rejection(Box<RejectionStep>),
}

/// Why applying a scan step failed: a file its publication read changed
/// since the step observed it (retried once the watcher reports the
/// change), or anything else.
enum StepFailure {
    Drifted { root: String, path: String },
    Failed(String),
}

impl From<String> for StepFailure {
    fn from(error: String) -> Self {
        StepFailure::Failed(error)
    }
}

impl From<StoreError> for StepFailure {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Drifted { root, path } => StepFailure::Drifted { root, path },
            error => StepFailure::Failed(error.to_string()),
        }
    }
}

impl StepFailure {
    /// The failure as a pass input's error message, remembering a drift in
    /// `drift` (scoped to the one pass) so the pass reports it typed.
    fn noted(self, drift: &std::cell::Cell<Option<ScanKey>>) -> String {
        match self {
            StepFailure::Drifted { root, path } => {
                let message = format!("{root}:{path} changed on disk since it was published");
                drift.set(Some((root, path)));
                message
            }
            StepFailure::Failed(error) => error,
        }
    }
}

/// The pass's error: a drift its input noted, else `error`.
fn pass_error(
    drift: &std::cell::Cell<Option<ScanKey>>,
    error: CoordinatedCommitError,
) -> CoordinatorError {
    match drift.take() {
        Some((root, path)) => CoordinatorError::Drifted { root, path },
        None => CoordinatorError::Coordinated(error),
    }
}

impl ScanStep {
    /// The bundle this step's scan read at (`root`, `path`): the bytes its
    /// publication observes, which a reader of that path in the step's
    /// input uses instead of reading the file again.
    fn fresh_bundle(&self, root: &str, path: &str) -> Option<Arc<ScannedBundle>> {
        let key = (root.to_owned(), path.to_owned());
        match self {
            ScanStep::Incremental(step) => step.delta.observed_bundle(&key).cloned(),
            ScanStep::Full(step) => step.candidate.scan.bundles.get(&key).cloned(),
            _ => None,
        }
    }
}

pub(super) struct IncrementalStep {
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
}

pub(super) struct RejectionStep {
    /// The pending rejection once the step commits: the scan's own, joined
    /// with the one pending before unless the scan observed every root.
    pending: PendingScanRejection,
}

/// What a scan publication pins for its projection and tag index: the
/// compiled state of the version the pass starts from.
struct TagInputs {
    compiled: Arc<Compiled>,
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
    #[cfg(any(test, feature = "test-hooks"))]
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

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn directories(affected: Option<(&'a PendingFileWork, bool)>) -> Self {
        Self {
            directories: true,
            affected: affected.map(Affected::supplied),
            ..Self::NONE
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
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

    /// The imports that read outputs this scope's imports publish: of the
    /// same kinds, due under the outputs' paths alone.
    fn chained(self) -> Self {
        Self {
            affected: Some(Affected {
                work: None,
                capabilities_changed: false,
            }),
            loop_pass: false,
            ..self
        }
    }
}

impl<'a> Affected<'a> {
    #[cfg(any(test, feature = "test-hooks"))]
    fn supplied((work, capabilities_changed): (&'a PendingFileWork, bool)) -> Self {
        Self {
            work: Some(work),
            capabilities_changed,
        }
    }
}

type PlannedImports = Vec<(PassImport, Option<PlannedImport>)>;

/// The first input's error when it planned imports: it rolls back, and the
/// pass runs them (`DaemonCoordinator::pass_with_imports`).
const PLANNED: &str = "the pass planned imports";

/// One level of a pass's imports and their runs (`None`: deferred).
type Level = Vec<(PassImport, Option<ImportRun>)>;

/// The most levels of chained imports one pass runs after its first.
const MAX_IMPORT_CHAIN_LEVELS: usize = 8;

/// What planning a pass's import chain decided beyond its levels.
#[derive(Default)]
struct Chain {
    /// Imports that would have read their own output again: they run once
    /// in this pass and are left due for the next.
    cut: Vec<PassImport>,
    /// The cycles and the depth bound met, for [`PassOutcome::failures`].
    failures: Vec<String>,
    /// The chain went deeper than [`MAX_IMPORT_CHAIN_LEVELS`]: the rest is
    /// the next pass's.
    truncated: bool,
}

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
        let base = self.server.stamp_of(store)?.version;
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
            let base = self.server.stamp_of(store)?.version;
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
        // The first input applies the step and discovers the imports due
        // under it. With none to run (the common case) it is the pass's
        // input: it refines the tag index, acknowledges the work and
        // commits, and the step is applied once. Otherwise it plans them and
        // rolls back (taking the index rows it built with it), and the
        // imports run outside any write.
        let mut planned = None;
        let drift = std::cell::Cell::new(None);
        let mut more_work = false;
        let published = self.server.coordinated_maybe_commit(store, base, |store| {
            if scope.loop_pass {
                // First: a runtime failure of the served epoch fences the
                // connections in this version.
                self.sync_runtime_pipeline_failure(store)
                    .map_err(|error| error.to_string())?;
            }
            let (mut commit, written) = self
                .apply_scan_step(store, &step)
                .map_err(|failure| failure.noted(&drift))?;
            if scope.imports() {
                let work = match scope.affected.and_then(|affected| affected.work) {
                    Some(work) => work.clone(),
                    None => store
                        .pending_file_work()
                        .map_err(|error| error.to_string())?,
                };
                let imports = self.discover(store, &step, &work, &written, scope, None)?;
                if !imports.is_empty() {
                    let mut plans = Vec::with_capacity(imports.len());
                    for import in imports {
                        let plan = self
                            .authoring
                            .plan_pass_import(store, &import)
                            .map_err(|error| format!("{error:?}"))?;
                        plans.push((import, plan));
                    }
                    planned = Some(plans);
                    return Err(PLANNED.to_owned());
                }
                self.refine_scan_tags(store, &step, &mut commit)?;
                if scope.loop_pass && !work.is_empty() {
                    // A path whose observation moved on has more work queued
                    // behind it: it stays pending.
                    more_work = !store
                        .acknowledge_file_work(&work)
                        .map_err(|error| error.to_string())?;
                }
            } else {
                self.refine_scan_tags(store, &step, &mut commit)?;
            }
            Ok(commit)
        });
        if let Some(planned) = planned {
            return self.pass_with_imports(store, base, step, scope, planned);
        }
        let published = published.map_err(|error| pass_error(&drift, error))?;
        self.finish_scan_step(&step);
        Ok(PassOutcome {
            stamp: match published {
                Some(stamp) => stamp,
                None => self.server.stamp_of(store)?,
            },
            more_work,
            failures: Vec::new(),
            imported: Vec::new(),
        })
    }

    /// The rest of a pass whose first input planned imports (see the module
    /// docs): run them and the imports chained to them, then apply the step
    /// again in the publishing input, with the runs still due.
    fn pass_with_imports(
        &self,
        store: &mut Store,
        base: InputVersion,
        step: ScanStep,
        scope: ImportScope<'_>,
        planned: PlannedImports,
    ) -> Result<PassOutcome, CoordinatorError> {
        let overlay = self.step_overlay(store, &step)?;
        let mut levels = vec![self.run_level(planned, Some(&overlay))?];
        let mut chain = Chain::default();
        self.plan_chain(store, base, &step, scope, overlay, &mut levels, &mut chain)?;

        let mut more_work = chain.truncated;
        let mut failures = chain.failures;
        let drift = std::cell::Cell::new(None);
        let mut imported = Vec::new();
        let published = self.server.coordinated_maybe_commit(store, base, |store| {
            if scope.loop_pass {
                self.sync_runtime_pipeline_failure(store)
                    .map_err(|error| error.to_string())?;
            }
            let (mut commit, written) = self
                .apply_scan_step(store, &step)
                .map_err(|failure| failure.noted(&drift))?;
            self.refine_scan_tags(store, &step, &mut commit)?;
            let work = match scope.affected.and_then(|affected| affected.work) {
                Some(work) => work.clone(),
                None => store
                    .pending_file_work()
                    .map_err(|error| error.to_string())?,
            };
            // Each level's imports are those due under the work the level
            // before published; level 0's, under the pass's own work.
            let mut level_work = work.clone();
            let mut level_scope = scope;
            let mut published_imports = Vec::new();
            let mut waiting = Vec::new();
            for level in 0..levels.len() {
                let mut outputs = Vec::new();
                // Only the step wrote bundle rows before level 0's imports.
                let written = if level == 0 {
                    written.clone()
                } else {
                    BTreeSet::new()
                };
                for import in
                    self.discover(store, &step, &level_work, &written, level_scope, None)?
                {
                    let Some(index) = levels[level]
                        .iter()
                        .position(|(planned, _)| *planned == import)
                    else {
                        if levels[level + 1..]
                            .iter()
                            .any(|later| later.iter().any(|(planned, _)| *planned == import))
                        {
                            // It runs after an import it reads.
                            waiting.push(import);
                        } else if !(published_imports.contains(&import)
                            && chain.cut.contains(&import))
                        {
                            // Due now, but not when the pass planned (an
                            // import cut from a cycle stays for the next
                            // pass, reported once).
                            more_work = true;
                        }
                        continue;
                    };
                    let (_, run) = levels[level].swap_remove(index);
                    let Some(run) = run else {
                        // Deferred until its importer is registered.
                        continue;
                    };
                    published_imports.push(import.clone());
                    match self
                        .authoring
                        .publish_pass_import(store, run)
                        .map_err(|error| format!("{error:?}"))?
                    {
                        PassPublication::Published(prepared) => {
                            outputs.push(prepared.bundle);
                            imported.push(prepared.bundle);
                            absorb(&mut commit, prepared.commit);
                        }
                        PassPublication::Memoized => {}
                        PassPublication::Drifted => more_work = true,
                        PassPublication::Failed(error) => {
                            tracing::warn!(
                                ?import,
                                ?error,
                                "import failed with no bundle to hold its failure"
                            );
                            failures.push(format!("{error:?}"));
                        }
                    }
                }
                if level + 1 == levels.len() || outputs.is_empty() {
                    break;
                }
                level_work = published_work(store, &outputs)?;
                level_scope = scope.chained();
            }
            // An import that waited for a level that never found it due
            // still is.
            more_work |= waiting
                .iter()
                .any(|import| !published_imports.contains(import));
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
        let published = published.map_err(|error| pass_error(&drift, error))?;
        self.finish_scan_step(&step);
        Ok(PassOutcome {
            stamp: match published {
                Some(stamp) => stamp,
                None => self.server.stamp_of(store)?,
            },
            more_work,
            failures,
            imported,
        })
    }

    /// The `files` rows `step` changes, as the overlay its imports run
    /// under: only the rows that differ from the committed ones, read
    /// outside any input (the step's input rolled back).
    fn step_overlay(
        &self,
        store: &StoreReader,
        step: &ScanStep,
    ) -> Result<FileOverlay, CoordinatorError> {
        Ok(match step {
            ScanStep::Full(step) => FileOverlay::differences(
                store,
                step.candidate
                    .scan
                    .file_observations()
                    .map(|((root_name, path), file)| ObservedFile {
                        root_name: root_name.clone(),
                        path: path.clone(),
                        file,
                    }),
            )?,
            ScanStep::Incremental(step) => incremental_overlay(store, &step.delta)?,
            _ => FileOverlay::empty(),
        })
    }

    /// Run one level's planned imports in parallel, outside any write, under
    /// `overlay`.
    fn run_level(
        &self,
        planned: PlannedImports,
        overlay: Option<&FileOverlay>,
    ) -> Result<Level, CoordinatorError> {
        use rayon::prelude::*;

        planned
            .into_par_iter()
            .map(|(import, planned)| {
                let run = match planned {
                    Some(planned) => self.authoring.run_pass_import(planned, overlay),
                    None => Ok(None),
                };
                (import, run)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|(import, run)| {
                run.map(|run| (import, run))
                    .map_err(|error| CoordinatorError::InvalidManifest(format!("{error:?}")))
            })
            .collect()
    }

    /// Plan and run the imports that read the outputs of the pass's last
    /// level, level by level, each in a plan input at `base` that rolls
    /// back: an import runs once, after every import of the pass whose
    /// output it reads, against those outputs (`FileOverlay::with_outputs`).
    /// Stops when a level changes no output any import reads; an import
    /// that would read its own output again is a cycle, cut and reported,
    /// and a chain deeper than [`MAX_IMPORT_CHAIN_LEVELS`] is left for the
    /// next pass, reported.
    #[allow(clippy::too_many_arguments)]
    fn plan_chain(
        &self,
        store: &mut Store,
        base: InputVersion,
        step: &ScanStep,
        scope: ImportScope<'_>,
        scan_overlay: FileOverlay,
        levels: &mut Vec<Level>,
        chain: &mut Chain,
    ) -> Result<(), CoordinatorError> {
        let publication = |error: String| {
            CoordinatorError::Coordinated(CoordinatedCommitError::Publication(error))
        };
        let rpc = |error: RpcFailure| publication(format!("{error:?}"));
        // What the input will hold at each output path, once every level
        // so far publishes.
        let mut outputs = BTreeMap::<(String, String), PassOutput>::new();
        // The outputs each import's output derives from in this pass.
        let mut lineages = BTreeMap::<(String, String), BTreeSet<(String, String)>>::new();
        loop {
            let upstream = levels.last().expect("a pass has a first level");
            let destinations = upstream
                .iter()
                .filter_map(|(_, run)| run.as_ref()?.output_destination())
                .collect::<Vec<_>>();
            // Most passes chain nothing: the committed index says so without
            // another plan input. (An import this pass's own scan adds is not
            // indexed yet: it reads the output in the next pass, through the
            // output's watcher work.)
            if destinations.is_empty()
                || !self
                    .authoring
                    .may_read_outputs(&self.open_reader()?, &destinations)
                    .map_err(rpc)?
            {
                return Ok(());
            }
            let planned = self.in_plan_input(store, base, step, |store| {
                // The last level's outputs that change what the input holds.
                let mut changed = Vec::new();
                for run in upstream.iter().filter_map(|(_, run)| run.as_ref()) {
                    let Some(output) = self.authoring.pass_output(store, run).map_err(rpc)? else {
                        continue;
                    };
                    let key = (output.root.clone(), output.path.clone());
                    let hash = ContentHash(*blake3::hash(&output.bytes).as_bytes());
                    let held = store
                        .observed_files_at(&output.path)?
                        .into_iter()
                        .find(|row| row.root_name == output.root)
                        .and_then(|row| row.file.state.content_hash);
                    if held == Some(hash) {
                        outputs.remove(&key);
                    } else {
                        changed.push(key.clone());
                        outputs.insert(key, output);
                    }
                }
                if changed.is_empty() {
                    return Ok(Vec::new());
                }
                // The plan input holds the observed rows; only the outputs
                // are over it.
                let overlay = FileOverlay::empty().with_outputs(outputs.values());
                let mut downstream = Vec::<(PassImport, BTreeSet<(String, String)>)>::new();
                for key in &changed {
                    let mut lineage = lineages.get(key).cloned().unwrap_or_default();
                    lineage.insert(key.clone());
                    let work = published_paths_work(store, std::slice::from_ref(key))
                        .map_err(publication)?;
                    for import in self
                        .discover(store, step, &work, &BTreeSet::new(), scope.chained(), Some(&overlay))
                        .map_err(publication)?
                    {
                        let Some(destination) = self
                            .authoring
                            .pass_import_destination(store, &import)
                            .map_err(rpc)?
                        else {
                            continue;
                        };
                        if lineage.contains(&destination) {
                            if !chain.cut.contains(&import) {
                                chain.failures.push(format!(
                                    "import cycle: {}:{} reads its own output through {}; it reruns in the next pass",
                                    destination.0,
                                    destination.1,
                                    describe_lineage(&lineage),
                                ));
                                tracing::warn!(?import, "import chain cycle cut");
                                chain.cut.push(import);
                            }
                            continue;
                        }
                        match downstream.iter_mut().find(|(planned, _)| *planned == import) {
                            Some((_, reads)) => reads.extend(lineage.iter().cloned()),
                            None => downstream.push((import, lineage.clone())),
                        }
                    }
                }
                let mut planned = Vec::with_capacity(downstream.len());
                for (import, lineage) in downstream {
                    let plan = self.authoring.plan_pass_import(store, &import).map_err(rpc)?;
                    let destination = self
                        .authoring
                        .pass_import_destination(store, &import)
                        .map_err(rpc)?;
                    planned.push((import, plan, destination, lineage));
                }
                Ok(planned)
            })?;
            if planned.is_empty() {
                return Ok(());
            }
            if levels.len() > MAX_IMPORT_CHAIN_LEVELS {
                let destinations = planned
                    .iter()
                    .filter_map(|(_, _, destination, _)| destination.as_ref())
                    .map(|(root, path)| format!("{root}:{path}"))
                    .collect::<Vec<_>>();
                chain.failures.push(format!(
                    "import chain deeper than {MAX_IMPORT_CHAIN_LEVELS} levels; {} run in the next pass",
                    destinations.join(", ")
                ));
                tracing::warn!(?destinations, "import chain truncated at its depth bound");
                chain.truncated = true;
                return Ok(());
            }
            // An import planned at an earlier level read an output this
            // level changes: it runs here instead, once.
            for level in levels.iter_mut() {
                level.retain(|(import, _)| !planned.iter().any(|(later, ..)| later == import));
            }
            let mut next = Vec::with_capacity(planned.len());
            for (import, plan, destination, lineage) in planned {
                if let Some(destination) = destination {
                    lineages.entry(destination).or_default().extend(lineage);
                }
                next.push((import, plan));
            }
            let overlay = scan_overlay.with_outputs(outputs.values());
            levels.push(self.run_level(next, Some(&overlay))?);
        }
    }

    /// Run `plan` in an input at `base` with `step` applied, which always
    /// rolls back.
    fn in_plan_input<T>(
        &self,
        store: &mut Store,
        base: InputVersion,
        step: &ScanStep,
        plan: impl FnOnce(&mut Store) -> Result<T, CoordinatorError>,
    ) -> Result<T, CoordinatorError> {
        let publication = |error: String| {
            CoordinatorError::Coordinated(CoordinatedCommitError::Publication(error))
        };
        let observed = store
            .open_input()
            .map_err(|error| publication(error.to_string()))?;
        let planned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if observed != base {
                return Err(CoordinatorError::Coordinated(
                    CoordinatedCommitError::Stale {
                        expected: base,
                        observed,
                    },
                ));
            }
            self.apply_scan_step(store, step)
                .map_err(|failure| match failure {
                    StepFailure::Drifted { root, path } => CoordinatorError::Drifted { root, path },
                    StepFailure::Failed(error) => publication(error),
                })?;
            plan(store)
        }));
        let rolled_back = store.finish_input(false);
        let planned = match planned {
            Ok(planned) => planned,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        rolled_back.map_err(|error| publication(error.to_string()))?;
        planned
    }

    /// The imports due under `work`: directory imports first, then watched
    /// reimports. A directory output the rules regenerate is not also
    /// reimported from its stale record. `outputs` holds the import outputs
    /// the pass will publish before these run, which they read instead.
    /// `written` names the sources whose bundle rows the input just wrote.
    fn discover(
        &self,
        store: &mut Store,
        step: &ScanStep,
        work: &PendingFileWork,
        written: &BTreeSet<ScanKey>,
        scope: ImportScope<'_>,
        outputs: Option<&FileOverlay>,
    ) -> Result<Vec<PassImport>, String> {
        let failure = |error: RpcFailure| format!("{error:?}");
        // The index is refreshed once, for both kinds: the bundle sources
        // `work` names, parsed once each.
        let refreshed = self
            .authoring
            .refresh_import_index(store, &work.dirty, written, |root, path| {
                step.fresh_bundle(root, path)
            })
            .map_err(failure)?;
        // `None` revalidates every import.
        let affected = scope.affected.map(|affected| {
            (
                (&work.dirty[..], &work.renames[..]),
                affected.capabilities_changed,
            )
        });
        let mut imports = Vec::new();
        let mut regenerated = BTreeSet::new();
        if scope.directories {
            let tasks = self
                .authoring
                .directory_import_tasks(store, &refreshed, affected, outputs)
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
            let bundles = self
                .authoring
                .watched_imports_due(store, affected, outputs)
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
        let pending = self.server.scan_rejection();
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
        let delta = {
            let stored = StoredBaseline::new(store, &scanner);
            let delta = scanner.scan_incremental_delta(&stored, &scan_paths);
            stored.finish()?;
            match delta {
                Ok(None) => return Ok(ScanStep::Unchanged),
                Ok(Some(delta)) => delta,
                Err(error) => return self.rejection_step(compiled, &error, heals),
            }
        };
        // A rename from a path never observed (the temporary file of an
        // atomic write, the daemon's own included) moves no identity: the
        // echo of an import's bundle write publishes nothing.
        let mut echo = healthy && delta.matches_published(store)?;
        for rename in &renames {
            echo =
                echo && delta.rename_moves_nothing(store, &rename.root_name, &rename.from_path)?;
        }
        if echo {
            return Ok(ScanStep::Unchanged);
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
                if self.server.scan_rejection().is_none()
                    && self.scan_initialized.get().is_some() =>
            {
                if scan.matches_published(store)? {
                    // Warning-grade exclusions are scanner state, not
                    // authored input: nothing to publish.
                    Ok(ScanStep::Unchanged)
                } else {
                    self.candidate_step(compiled, scan)
                }
            }
            Ok(scan) => self.candidate_step(compiled, scan),
            Err(error) => self.rejection_step(compiled, &error, true),
        }
    }

    /// The step publishing `scan`, which observed every root, as the
    /// complete namespace: it heals the pending scan rejection.
    fn candidate_step(
        &self,
        compiled: &Arc<Compiled>,
        scan: ScanSnapshot,
    ) -> Result<ScanStep, CoordinatorError> {
        let tags = self.tag_inputs(compiled);
        let authority = tags.authority();
        let candidate = ScanCandidate::build(scan, authority.as_deref())?;
        let claims = bundle_claims(
            candidate.scan.bundle_rows(),
            compiled.projection(),
            authority.as_deref(),
        )?;
        Ok(ScanStep::Full(Box::new(FullStep {
            candidate,
            claims,
            tags,
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
        let mut rejection = classify_scan_rejection(scanner, error)?;
        let mut subjects = scanner.rejection_subjects(error);
        if let Some(previous) = self
            .server
            .scan_rejection()
            .filter(|_| !replaces_pending)
        {
            rejection = select_scan_rejection([
                ScanRejection {
                    version: previous.errors,
                    configuration: previous.configuration,
                },
                rejection,
            ])?;
            subjects.extend(previous.subjects);
        }
        subjects.sort_unstable();
        subjects.dedup();
        Ok(ScanStep::Rejection(Box::new(RejectionStep {
            pending: PendingScanRejection {
                errors: rejection.version,
                configuration: rejection.configuration,
                subjects,
            },
        })))
    }

    fn tag_inputs(&self, compiled: &Arc<Compiled>) -> TagInputs {
        TagInputs {
            compiled: Arc::clone(compiled),
            max_dependency_depth: self.operational_configuration().max_dependency_depth,
        }
    }

    /// Refine the tag index of the assets `commit`, the RPC delta of
    /// `step` applied inside the input open on `store`, publishes (a full
    /// step's every stale row).
    fn refine_scan_tags(
        &self,
        store: &mut Store,
        step: &ScanStep,
        commit: &mut Option<Commit>,
    ) -> Result<(), String> {
        let (tags, full) = match step {
            ScanStep::Incremental(step) => (&step.tags, false),
            ScanStep::Full(step) => (&step.tags, true),
            _ => return Ok(()),
        };
        let (Some(authority), Some(commit)) = (tags.authority(), commit.as_mut()) else {
            return Ok(());
        };
        crate::build::refine_tag_index(
            crate::build::OpenInput::new(store)
                .expect("tag-index refinement runs inside its input"),
            tags.compiled.scanner().clone(),
            authority,
            tags.compiled.pipeline_snapshot(),
            tags.compiled.build_targets(),
            tags.max_dependency_depth,
            commit,
            full,
        )
    }

    /// Apply `step` inside the input open on `store`. Returns its RPC
    /// delta, if it publishes, and the sources whose bundle rows it wrote.
    fn apply_scan_step(
        &self,
        store: &mut Store,
        step: &ScanStep,
    ) -> Result<(Option<Commit>, BTreeSet<ScanKey>), StepFailure> {
        match step {
            ScanStep::Unchanged => Ok((None, BTreeSet::new())),
            ScanStep::Incremental(step) => {
                let tags = &step.tags;
                let authority = tags.authority();
                let inputs = PlanInputs {
                    authority: authority.as_deref(),
                    fresh: fresh_bundles(&step.delta),
                    scanner: tags.compiled.scanner(),
                };
                let (commit, written) = publish_incremental_scan(
                    store,
                    store.input_version().map_err(|error| error.to_string())?,
                    &step.delta,
                    &step.claims,
                    &inputs,
                    &step.renames,
                    tags.compiled.projection(),
                )?;
                Ok((Some(commit), written))
            }
            ScanStep::Full(step) => {
                let tags = &step.tags;
                let (commit, written) = publish_scan(
                    store,
                    store.input_version().map_err(|error| error.to_string())?,
                    step.candidate.clone(),
                    None,
                    tags.compiled.projection(),
                    &BTreeSet::new(),
                    tags.authority().as_deref(),
                    &step.claims,
                )
                .map_err(|error| error.to_string())?;
                Ok((Some(commit), written))
            }
            // The rejection changes no published row: it is pending once the
            // pass commits (see `finish_scan_step`).
            ScanStep::Rejection(_) => Ok((None, BTreeSet::new())),
        }
    }

    /// What a committed `step` leaves behind in this process: a full scan
    /// is this process's own observation, and it heals the pending scan
    /// rejection, as an incremental one that revalidated its subjects does;
    /// a rejection becomes the pending one.
    fn finish_scan_step(&self, step: &ScanStep) {
        match step {
            ScanStep::Full(_) => {
                let _ = self.scan_initialized.set(());
                self.server.set_scan_rejection(None);
            }
            ScanStep::Incremental(step) if step.heals => self.server.set_scan_rejection(None),
            ScanStep::Rejection(step) => self.server.set_scan_rejection(Some(step.pending.clone())),
            _ => {}
        }
    }
}

/// Fold `later`, the RPC delta of a step applied after `commit`'s, into
/// `commit`: one delta from the base to the state after both. A later
/// mutation of the same asset, entry, path or output replaces the earlier
/// one; a later complete replacement replaces everything before it.
/// The watcher work of the bundles a pass level published, as the input
/// holds them: what the next level's imports are due under.
fn published_work(store: &StoreReader, bundles: &[BundleUuid]) -> Result<PendingFileWork, String> {
    let mut paths = Vec::with_capacity(bundles.len());
    for bundle in bundles {
        let meta = store
            .bundle(*bundle)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("published import bundle {bundle} has no row"))?;
        let root = store
            .root_name(meta.root)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("published import bundle {bundle} has no root"))?;
        paths.push((root, meta.path));
    }
    published_paths_work(store, &paths)
}

/// Watcher work naming each (root, path) as changed.
fn published_paths_work(
    store: &StoreReader,
    paths: &[(String, String)],
) -> Result<PendingFileWork, String> {
    let mut dirty = Vec::with_capacity(paths.len());
    for (root_name, path) in paths {
        let root = store
            .root_id(root_name)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("import output root {root_name:?} is not interned"))?;
        dirty.push(distill_store::files::DirtyEntry {
            root,
            root_name: root_name.clone(),
            path: path.clone(),
            exists: true,
            observation: store.input_version().map_err(|error| error.to_string())?,
        });
    }
    Ok(PendingFileWork::unqueued(dirty, Vec::new()))
}

/// `root:path` of each output in a lineage, in order.
fn describe_lineage(lineage: &BTreeSet<(String, String)>) -> String {
    lineage
        .iter()
        .map(|(root, path)| format!("{root}:{path}"))
        .collect::<Vec<_>>()
        .join(", ")
}

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
    keyed(&mut commit.assets, later.assets, |mutation| mutation.uuid);
    keyed(
        &mut commit.authoring,
        later.authoring,
        |mutation| match mutation {
            AuthoringMutation::Set(entry) => entry.uuid,
            AuthoringMutation::Remove { uuid } => *uuid,
        },
    );
    keyed(&mut commit.paths, later.paths, |mutation| match mutation {
        PathMutation::Set { path, .. } | PathMutation::Remove { path } => path.clone(),
    });
    commit.new_entry_paths.extend(later.new_entry_paths);
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
    keyed(
        &mut commit.tag_projection_mutations,
        later.tag_projection_mutations,
        |mutation| match mutation {
            distill_rpc::TagProjectionMutation::Set { asset, .. }
            | distill_rpc::TagProjectionMutation::Remove { asset } => *asset,
        },
    );
    commit.pipeline_epoch_changed |= later.pipeline_epoch_changed;
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
        assert!(commit.pipeline_epoch_changed);
    }
}
