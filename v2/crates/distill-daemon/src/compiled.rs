//! The daemon's compiled configuration state, keyed by the store version
//! that published it.
//!
//! The schema authority, build targets, pipeline projection, the pipeline's
//! importers, its epoch and the asset roots are compiled from a
//! configuration candidate. SQLite is their source of truth: a publication
//! that changes them marks its input (`store_meta.compiled_version`), and the
//! state compiled for it is one immutable [`Compiled`] entry under that key.
//! A reader looks its entry up by the key its own transaction sees
//! ([`CompiledRegistry::at`]), so it never mixes its snapshot with state
//! compiled for another version, whichever writer it is: the process loop, an
//! RPC connection or a build worker.
//!
//! The publication stages its entry before it commits: nothing can look the
//! entry up until the key is committed, and a publication that fails removes
//! it, so a failed publication changes nothing. Superseded entries stay while
//! anything holds them; a bounded number of the newest stay held by the
//! registry for a while, for readers whose snapshot still sees them. A key
//! with no entry is a typed, retryable error, never another version's state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use distill_build::pipeline::Target;
use distill_rpc::RpcFailure;
use distill_schema::ProjectSchemaAuthority;
use distill_store::state::{InputVersion, PipelineFailure};
use distill_store::{StoreError, StoreReader};

use crate::epoch::{PipelineEpoch, PipelineSnapshot};
use crate::importer::RegisteredImporters;
use crate::pipeline_map::PipelineProjection;
use crate::scanner::{AssetRoot, RootedScanner};

/// The store version whose compiled state an entry is; `None` before any
/// publication records one.
pub type CompiledKey = Option<InputVersion>;

/// How many superseded entries the registry itself keeps, and for how long.
const RETAINED: usize = 4;
const RETAIN_FOR: Duration = Duration::from_secs(120);

/// The compiled state of one store version. Immutable: a publication that
/// changes any of it is a new entry.
#[derive(Clone)]
pub struct Compiled {
    key: CompiledKey,
    authority: Option<Arc<ProjectSchemaAuthority>>,
    targets: Arc<BTreeMap<String, Target>>,
    projection: Arc<PipelineProjection>,
    /// The pipeline epoch's importers (the daemon's built-in ones are not
    /// compiled state).
    importers: Arc<RegisteredImporters>,
    pipeline: PipelineSnapshot,
    /// Resolves rooted paths against `roots`; its roots never change.
    scanner: RootedScanner,
    roots: Arc<Vec<AssetRoot>>,
}

impl Compiled {
    /// What a process serves before it publishes compiled state: no schema
    /// authority, targets or pipeline, and the roots it was opened with.
    pub(crate) fn boot(key: CompiledKey, scanner: RootedScanner, roots: Vec<AssetRoot>) -> Self {
        Self {
            key,
            authority: None,
            targets: Arc::new(BTreeMap::new()),
            projection: Arc::new(PipelineProjection::default()),
            importers: Arc::new(RegisteredImporters::new()),
            pipeline: PipelineSnapshot::unpublished(),
            scanner,
            roots: Arc::new(roots),
        }
    }

    /// The state a configuration candidate compiles, for the version `key`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn candidate(
        key: InputVersion,
        authority: Arc<ProjectSchemaAuthority>,
        targets: BTreeMap<String, Target>,
        projection: PipelineProjection,
        importers: RegisteredImporters,
        pipeline: PipelineSnapshot,
        scanner: RootedScanner,
        roots: Vec<AssetRoot>,
    ) -> Self {
        Self {
            key: Some(key),
            authority: Some(authority),
            targets: Arc::new(targets),
            projection: Arc::new(projection),
            importers: Arc::new(importers),
            pipeline,
            scanner,
            roots: Arc::new(roots),
        }
    }

    /// This state with its pipeline failed, for the version `key`: the
    /// schema, targets and projection stay, the pipeline's importers go.
    pub(crate) fn with_pipeline_failure(&self, key: InputVersion, failure: PipelineFailure) -> Self {
        Self {
            key: Some(key),
            importers: Arc::new(RegisteredImporters::new()),
            pipeline: PipelineSnapshot::failed(failure),
            ..self.clone()
        }
    }

    pub fn key(&self) -> CompiledKey {
        self.key
    }

    pub fn schema_authority(&self) -> Option<Arc<ProjectSchemaAuthority>> {
        self.authority.clone()
    }

    pub fn build_target(&self, name: &str) -> Option<Target> {
        self.targets.get(name).cloned()
    }

    pub(crate) fn build_targets(&self) -> &Arc<BTreeMap<String, Target>> {
        &self.targets
    }

    pub(crate) fn projection(&self) -> &PipelineProjection {
        &self.projection
    }

    pub(crate) fn pipeline_importers(&self) -> &RegisteredImporters {
        &self.importers
    }

    pub fn pipeline_snapshot(&self) -> PipelineSnapshot {
        self.pipeline.clone()
    }

    pub(crate) fn pipeline_epoch(&self) -> Result<&PipelineEpoch, PipelineFailure> {
        self.pipeline.epoch()
    }

    pub fn scanner(&self) -> &RootedScanner {
        &self.scanner
    }

    pub(crate) fn roots(&self) -> &[AssetRoot] {
        &self.roots
    }

    #[cfg(test)]
    pub(crate) fn with_test_state(
        &self,
        authority: Option<Arc<ProjectSchemaAuthority>>,
        target: Option<(&str, Target)>,
        pipeline: Option<PipelineSnapshot>,
    ) -> Self {
        let mut next = self.clone();
        if let Some(authority) = authority {
            next.authority = Some(authority);
        }
        if let Some((name, target)) = target {
            let mut targets = BTreeMap::clone(&next.targets);
            targets.insert(name.to_owned(), target);
            next.targets = Arc::new(targets);
        }
        if let Some(pipeline) = pipeline {
            next.pipeline = pipeline;
        }
        next
    }
}

/// Why no entry serves a reader's key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledLookupError {
    /// Nothing is compiled for the key yet: the process has not published
    /// compiled state since it opened a store that names an earlier
    /// process's version.
    NotLoaded { key: CompiledKey },
    /// The key's entry was superseded and released; a newer snapshot serves.
    Superseded { key: CompiledKey, latest: CompiledKey },
    /// The key itself could not be read.
    Store(String),
}

impl std::fmt::Display for CompiledLookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLoaded { key } => {
                write!(f, "no compiled state is loaded for store version {key:?}")
            }
            Self::Superseded { key, latest } => write!(
                f,
                "the compiled state of store version {key:?} was superseded by {latest:?}"
            ),
            Self::Store(error) => write!(f, "reading the compiled version: {error}"),
        }
    }
}

impl std::error::Error for CompiledLookupError {}

impl From<StoreError> for CompiledLookupError {
    fn from(error: StoreError) -> Self {
        Self::Store(error.to_string())
    }
}

impl From<CompiledLookupError> for RpcFailure {
    fn from(error: CompiledLookupError) -> Self {
        match error {
            // A newer snapshot serves: the client reopens one.
            CompiledLookupError::Superseded { .. } => RpcFailure::SnapshotExpired,
            error => RpcFailure::AuthoringBackendUnavailable {
                operation: error.to_string(),
            },
        }
    }
}

/// Every live [`Compiled`] entry, by key.
#[derive(Default)]
pub(crate) struct CompiledRegistry {
    inner: Mutex<Entries>,
}

#[derive(Default)]
struct Entries {
    slots: BTreeMap<CompiledKey, Slot>,
    /// The last confirmed key.
    latest: Option<CompiledKey>,
}

struct Slot {
    entry: Weak<Compiled>,
    /// The registry's own hold: the latest entry, a staged one, or a
    /// recently superseded one.
    held: Option<Arc<Compiled>>,
    superseded_at: Option<Instant>,
}

impl CompiledRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The entry for the compiled version `reader`'s transaction sees.
    pub(crate) fn at(&self, reader: &StoreReader) -> Result<Arc<Compiled>, CompiledLookupError> {
        let key = reader.compiled_version()?;
        self.get(key)
    }

    /// The entry for `key` exactly.
    pub(crate) fn get(&self, key: CompiledKey) -> Result<Arc<Compiled>, CompiledLookupError> {
        let entries = self.entries();
        if let Some(entry) = entries.slots.get(&key).and_then(|slot| slot.entry.upgrade()) {
            return Ok(entry);
        }
        match entries.latest {
            Some(latest) if key < latest => Err(CompiledLookupError::Superseded { key, latest }),
            _ => Err(CompiledLookupError::NotLoaded { key }),
        }
    }

    /// The last confirmed entry: what the process loop, the only publisher
    /// of compiled state, last published.
    pub(crate) fn latest(&self) -> Option<Arc<Compiled>> {
        let entries = self.entries();
        let key = entries.latest?;
        entries.slots.get(&key).and_then(|slot| slot.entry.upgrade())
    }

    /// Register `entry` as the confirmed state of its key: the boot state,
    /// whose key the store already holds.
    pub(crate) fn install(&self, entry: Compiled) -> Arc<Compiled> {
        let staged = self.stage(entry);
        let entry = Arc::clone(staged.entry());
        staged.confirm();
        entry
    }

    /// Stage `entry` under its key, before the input that publishes the key
    /// commits. Until then no reader can see the key, so none finds the
    /// entry; confirm it once the input commits. Dropped unconfirmed, it is
    /// removed.
    pub(crate) fn stage(&self, entry: Compiled) -> StagedCompiled<'_> {
        let entry = Arc::new(entry);
        self.entries().slots.insert(
            entry.key,
            Slot {
                entry: Arc::downgrade(&entry),
                held: Some(Arc::clone(&entry)),
                superseded_at: None,
            },
        );
        StagedCompiled {
            registry: self,
            entry: Some(entry),
        }
    }

    /// Replace the entry of `entry`'s key in place: tests install state no
    /// publication compiled.
    #[cfg(test)]
    pub(crate) fn replace_for_test(&self, entry: Compiled) {
        let entry = Arc::new(entry);
        self.entries().slots.insert(
            entry.key,
            Slot {
                entry: Arc::downgrade(&entry),
                held: Some(entry),
                superseded_at: None,
            },
        );
    }

    fn confirm(&self, key: CompiledKey) {
        let mut entries = self.entries();
        let now = Instant::now();
        if let Some(previous) = entries.latest.filter(|previous| *previous != key) {
            if let Some(slot) = entries.slots.get_mut(&previous) {
                slot.superseded_at = Some(now);
            }
        }
        if entries.latest.is_none_or(|latest| latest <= key) {
            entries.latest = Some(key);
        }
        entries.prune(now);
    }

    fn remove(&self, entry: &Arc<Compiled>) {
        let mut entries = self.entries();
        if entries
            .slots
            .get(&entry.key)
            .and_then(|slot| slot.held.as_ref())
            .is_some_and(|held| Arc::ptr_eq(held, entry))
        {
            entries.slots.remove(&entry.key);
        }
    }

    fn entries(&self) -> MutexGuard<'_, Entries> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Entries {
    /// Release the registry's hold on superseded entries past the newest
    /// [`RETAINED`] or older than [`RETAIN_FOR`], and forget released
    /// entries nothing holds.
    fn prune(&mut self, now: Instant) {
        let mut superseded = self
            .slots
            .iter()
            .filter_map(|(key, slot)| slot.superseded_at.map(|at| (*key, at)))
            .collect::<Vec<_>>();
        superseded.sort_by(|left, right| right.0.cmp(&left.0));
        for (index, (key, at)) in superseded.into_iter().enumerate() {
            if index >= RETAINED || now.duration_since(at) > RETAIN_FOR {
                if let Some(slot) = self.slots.get_mut(&key) {
                    slot.held = None;
                }
            }
        }
        self.slots
            .retain(|_, slot| slot.held.is_some() || slot.entry.strong_count() > 0);
    }
}

/// A staged entry (see [`CompiledRegistry::stage`]).
pub(crate) struct StagedCompiled<'a> {
    registry: &'a CompiledRegistry,
    entry: Option<Arc<Compiled>>,
}

impl StagedCompiled<'_> {
    pub(crate) fn entry(&self) -> &Arc<Compiled> {
        self.entry.as_ref().expect("a staged entry is held until it resolves")
    }

    /// The input that publishes the entry's key committed.
    pub(crate) fn confirm(mut self) {
        let entry = self.entry.take().expect("a staged entry resolves once");
        self.registry.confirm(entry.key);
    }
}

impl Drop for StagedCompiled<'_> {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            self.registry.remove(&entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: u64) -> Compiled {
        let scanner = RootedScanner::new(Vec::new()).unwrap();
        Compiled::boot(Some(InputVersion(key)), scanner, Vec::new())
    }

    #[test]
    fn a_staged_entry_serves_its_key_and_an_unconfirmed_one_is_removed() {
        let registry = CompiledRegistry::new();
        registry.install(entry(1));
        {
            let staged = registry.stage(entry(2));
            assert_eq!(registry.get(Some(InputVersion(2))).unwrap().key(), Some(InputVersion(2)));
            assert_eq!(registry.latest().unwrap().key(), Some(InputVersion(1)));
            drop(staged);
        }
        assert_eq!(
            registry.get(Some(InputVersion(2))).err(),
            Some(CompiledLookupError::NotLoaded {
                key: Some(InputVersion(2))
            })
        );
        assert_eq!(registry.latest().unwrap().key(), Some(InputVersion(1)));
        registry.stage(entry(2)).confirm();
        assert_eq!(registry.latest().unwrap().key(), Some(InputVersion(2)));
        // The superseded entry is still retained for older snapshots.
        assert_eq!(registry.get(Some(InputVersion(1))).unwrap().key(), Some(InputVersion(1)));
    }

    #[test]
    fn superseded_entries_are_released_past_the_retained_count_unless_held() {
        let registry = CompiledRegistry::new();
        let held = registry.install(entry(1));
        for key in 2..=(RETAINED as u64 + 3) {
            registry.stage(entry(key)).confirm();
        }
        // Key 2 is past the retained count and nothing holds it.
        assert_eq!(
            registry.get(Some(InputVersion(2))).err(),
            Some(CompiledLookupError::Superseded {
                key: Some(InputVersion(2)),
                latest: Some(InputVersion(RETAINED as u64 + 3)),
            })
        );
        // Key 1 is held by a reader: it still serves.
        assert!(Arc::ptr_eq(&registry.get(Some(InputVersion(1))).unwrap(), &held));
        // A key no publication reached is not another version's state.
        assert_eq!(
            registry.get(Some(InputVersion(99))).err(),
            Some(CompiledLookupError::NotLoaded {
                key: Some(InputVersion(99))
            })
        );
    }
}
