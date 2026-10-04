//! Loader timing (doc 22 phase 0): per-asset timelines from request to
//! resident, split into the stages of today's path, and their aggregates.
//!
//! Always on and allocation-free after construction:
//! - in-flight timelines live in a fixed table indexed through a map sized
//!   up front; a table that is full drops new timelines and counts them;
//! - finished timelines go to a ring of the last [`TIMELINE_HISTORY`];
//! - aggregates are per-stage count, sum and max.
//!
//! Marks are nanoseconds since the stats origin; 0 means "not reached".
//! The loader marks what it sees (request, resolve, fetch issue, delivery,
//! accept, staging, commit); [`FetchTiming`] carries what the IO saw inside
//! a fetch; storage reports its upload part through
//! [`crate::AssetStorage::take_upload_ns`].

use std::collections::HashMap;
use std::time::Instant;

use distill_core::id::{AssetUuid, TypeUuid};

/// Finished timelines kept for inspection, newest last.
pub const TIMELINE_HISTORY: usize = 64;
/// In-flight timelines tracked at once; more are dropped (counted).
pub const ACTIVE_CAPACITY: usize = 1024;

/// A point on an asset's path from request to resident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Mark {
    /// A handle asked for it: `add_ref`, a path request, a dependency, a delta.
    Request,
    /// Its path resolve answered (path requests only).
    PathResolved,
    /// Its resolve answered (the loader handled the event).
    Resolved,
    /// The loader issued its fetch.
    FetchIssued,
    /// The IO started the fetch request (admitted as a task).
    FetchStarted,
    /// The daemon answered the fetch call: the payload size is known.
    FetchAnswered,
    /// The payload's memory reservation was granted.
    FetchAdmitted,
    /// The last chunk arrived.
    ChunksReceived,
    /// The IO queued the fetched event (after the layout tree round trip).
    FetchCompleted,
    /// The loader took the fetched event from `poll`.
    Delivered,
    /// `accept_fetched` finished: parsed, verified, constructed.
    Accepted,
    /// Its component was handed to storage (first `update`).
    Staged,
    /// Its component committed: the asset is resident.
    Resident,
}

pub const MARK_COUNT: usize = Mark::Resident as usize + 1;

impl Mark {
    pub const ALL: [Mark; MARK_COUNT] = [
        Mark::Request,
        Mark::PathResolved,
        Mark::Resolved,
        Mark::FetchIssued,
        Mark::FetchStarted,
        Mark::FetchAnswered,
        Mark::FetchAdmitted,
        Mark::ChunksReceived,
        Mark::FetchCompleted,
        Mark::Delivered,
        Mark::Accepted,
        Mark::Staged,
        Mark::Resident,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Mark::Request => "request",
            Mark::PathResolved => "path_resolved",
            Mark::Resolved => "resolved",
            Mark::FetchIssued => "fetch_issued",
            Mark::FetchStarted => "fetch_started",
            Mark::FetchAnswered => "fetch_answered",
            Mark::FetchAdmitted => "fetch_admitted",
            Mark::ChunksReceived => "chunks_received",
            Mark::FetchCompleted => "fetch_completed",
            Mark::Delivered => "delivered",
            Mark::Accepted => "accepted",
            Mark::Staged => "staged",
            Mark::Resident => "resident",
        }
    }
}

/// A duration on the path, derived from marks and measured parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Stage {
    /// Request → resolved: path resolve and resolve hops, frame waits included.
    Resolve,
    /// Fetch issued → started: queued in the IO until its next step.
    FetchQueue,
    /// Fetch started → answered: the daemon's fetch call.
    FetchAnswer,
    /// Answered → admitted: waiting for the fetch memory budget.
    Admission,
    /// Admitted → last chunk.
    Chunks,
    /// Last chunk → event queued: the layout tree round trip.
    WireTree,
    /// Event queued → taken by the loader: waiting for a loader step.
    Deliver,
    /// `accept_fetched`, whole.
    Accept,
    /// Accept part: header parse and content hash.
    AcceptParse,
    /// Accept part: DSWL decode and hash.
    AcceptDswl,
    /// Accept part: fixup plan compile.
    AcceptPlan,
    /// Accept part: value construction, every handle.
    AcceptConstruct,
    /// Accepted → staged: waiting for the sweep gate (§1.5).
    SweepWait,
    /// `AssetStorage::update`, every handle.
    StorageUpdate,
    /// The part of `update` storage reports as staging writes.
    StagingWrite,
    /// Update done → resident: GPU readiness and commit.
    Commit,
    /// Request → resident.
    Total,
}

pub const STAGE_COUNT: usize = Stage::Total as usize + 1;

impl Stage {
    pub const ALL: [Stage; STAGE_COUNT] = [
        Stage::Resolve,
        Stage::FetchQueue,
        Stage::FetchAnswer,
        Stage::Admission,
        Stage::Chunks,
        Stage::WireTree,
        Stage::Deliver,
        Stage::Accept,
        Stage::AcceptParse,
        Stage::AcceptDswl,
        Stage::AcceptPlan,
        Stage::AcceptConstruct,
        Stage::SweepWait,
        Stage::StorageUpdate,
        Stage::StagingWrite,
        Stage::Commit,
        Stage::Total,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Stage::Resolve => "resolve",
            Stage::FetchQueue => "fetch_queue",
            Stage::FetchAnswer => "fetch_answer",
            Stage::Admission => "admission",
            Stage::Chunks => "chunks",
            Stage::WireTree => "wire_tree",
            Stage::Deliver => "deliver",
            Stage::Accept => "accept",
            Stage::AcceptParse => "accept_parse",
            Stage::AcceptDswl => "accept_dswl",
            Stage::AcceptPlan => "accept_plan",
            Stage::AcceptConstruct => "accept_construct",
            Stage::SweepWait => "sweep_wait",
            Stage::StorageUpdate => "storage_update",
            Stage::StagingWrite => "staging_write",
            Stage::Commit => "commit",
            Stage::Total => "total",
        }
    }
}

/// Parts of `accept_fetched`, in nanoseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AcceptParts {
    pub parse_ns: u64,
    pub dswl_ns: u64,
    pub plan_ns: u64,
    pub construct_ns: u64,
}

/// What the IO saw inside one fetch; travels with the fetched artifact.
/// Every instant is `None` for an IO that does not measure it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FetchTiming {
    pub started: Option<Instant>,
    pub answered: Option<Instant>,
    pub admitted: Option<Instant>,
    pub received: Option<Instant>,
    pub completed: Option<Instant>,
    /// Payload chunks received.
    pub chunks: u32,
    /// Payload bytes received (structural section and blobs).
    pub bytes: u64,
    /// Time spent copying chunk bytes into the collected payload.
    pub copy_ns: u64,
}

/// How a timeline ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Still in flight.
    Active,
    /// Committed with a new value.
    Resident,
    /// Resolved to the content it already held; nothing fetched.
    Unchanged,
    /// Its resolve, fetch, accept or component failed (or it is missing).
    Failed,
    /// Its last handle was released before it finished.
    Abandoned,
}

impl Outcome {
    pub fn name(self) -> &'static str {
        match self {
            Outcome::Active => "active",
            Outcome::Resident => "resident",
            Outcome::Unchanged => "unchanged",
            Outcome::Failed => "failed",
            Outcome::Abandoned => "abandoned",
        }
    }
}

/// One asset's path from request to resident.
#[derive(Debug, Clone, Copy)]
pub struct AssetTimeline {
    pub uuid: AssetUuid,
    /// Known once fetched.
    pub type_uuid: Option<TypeUuid>,
    pub outcome: Outcome,
    /// Nanoseconds since the stats origin per [`Mark`]; 0 = not reached.
    pub marks: [u64; MARK_COUNT],
    pub accept: AcceptParts,
    pub update_ns: u64,
    pub staging_ns: u64,
    pub bytes: u64,
    pub chunks: u32,
    pub copy_ns: u64,
    /// Loader steps and host wakeups at request, then (once finished) the
    /// number spent from request to the end.
    pub steps: u64,
    pub wakeups: u64,
}

impl AssetTimeline {
    fn new(uuid: AssetUuid, at: u64, steps: u64, wakeups: u64) -> Self {
        let mut marks = [0; MARK_COUNT];
        marks[Mark::Request as usize] = at;
        Self {
            uuid,
            type_uuid: None,
            outcome: Outcome::Active,
            marks,
            accept: AcceptParts::default(),
            update_ns: 0,
            staging_ns: 0,
            bytes: 0,
            chunks: 0,
            copy_ns: 0,
            steps,
            wakeups,
        }
    }

    pub fn mark(&self, mark: Mark) -> Option<u64> {
        let at = self.marks[mark as usize];
        (at != 0).then_some(at)
    }

    fn between(&self, from: Mark, to: Mark) -> Option<u64> {
        Some(self.mark(to)?.saturating_sub(self.mark(from)?))
    }

    /// The stage's duration in nanoseconds, when its marks were reached.
    pub fn stage(&self, stage: Stage) -> Option<u64> {
        let fetched = self.mark(Mark::Delivered).is_some();
        match stage {
            Stage::Resolve => self.between(Mark::Request, Mark::Resolved),
            Stage::FetchQueue => self.between(Mark::FetchIssued, Mark::FetchStarted),
            Stage::FetchAnswer => self.between(Mark::FetchStarted, Mark::FetchAnswered),
            Stage::Admission => self.between(Mark::FetchAnswered, Mark::FetchAdmitted),
            Stage::Chunks => self.between(Mark::FetchAdmitted, Mark::ChunksReceived),
            Stage::WireTree => self.between(Mark::ChunksReceived, Mark::FetchCompleted),
            Stage::Deliver => self.between(Mark::FetchCompleted, Mark::Delivered),
            Stage::Accept => self.between(Mark::Delivered, Mark::Accepted),
            Stage::AcceptParse => fetched.then_some(self.accept.parse_ns),
            Stage::AcceptDswl => fetched.then_some(self.accept.dswl_ns),
            Stage::AcceptPlan => fetched.then_some(self.accept.plan_ns),
            Stage::AcceptConstruct => fetched.then_some(self.accept.construct_ns),
            Stage::SweepWait => self.between(Mark::Accepted, Mark::Staged),
            Stage::StorageUpdate => self.mark(Mark::Staged).map(|_| self.update_ns),
            Stage::StagingWrite => self.mark(Mark::Staged).map(|_| self.staging_ns),
            Stage::Commit => {
                let updated = self.mark(Mark::Staged)? + self.update_ns;
                Some(self.mark(Mark::Resident)?.saturating_sub(updated))
            }
            Stage::Total => self.between(Mark::Request, Mark::Resident),
        }
    }
}

/// Count, sum and max of one stage over finished timelines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StageAggregate {
    pub count: u64,
    pub sum_ns: u64,
    pub max_ns: u64,
}

impl StageAggregate {
    fn add(&mut self, ns: u64) {
        self.count += 1;
        self.sum_ns = self.sum_ns.saturating_add(ns);
        self.max_ns = self.max_ns.max(ns);
    }
}

/// Totals since the last reset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoaderTotals {
    /// `Loader::process` calls.
    pub steps: u64,
    /// Host wakeups reported through [`LoaderStats::note_wakeup`].
    pub wakeups: u64,
    /// Payload bytes and chunks of accepted fetches.
    pub fetched_bytes: u64,
    pub fetched_chunks: u64,
    pub resident: u64,
    pub unchanged: u64,
    pub failed: u64,
    pub abandoned: u64,
    /// Timelines not tracked because the active table was full.
    pub dropped: u64,
}

pub struct LoaderStats {
    origin: Instant,
    reset_at: u64,
    active: Vec<AssetTimeline>,
    index: HashMap<AssetUuid, u32>,
    history: [Option<AssetTimeline>; TIMELINE_HISTORY],
    /// Next history slot to write.
    history_next: usize,
    stages: [StageAggregate; STAGE_COUNT],
    totals: LoaderTotals,
    /// Steps and wakeups ever (timelines store these at request).
    steps: u64,
    wakeups: u64,
}

impl Default for LoaderStats {
    fn default() -> Self {
        Self::new()
    }
}

impl LoaderStats {
    pub fn new() -> Self {
        Self::with_origin(Instant::now())
    }

    /// Stats whose marks count from `origin` (tests pass a fixed one).
    pub fn with_origin(origin: Instant) -> Self {
        Self {
            origin,
            reset_at: 0,
            active: Vec::with_capacity(ACTIVE_CAPACITY),
            index: HashMap::with_capacity(ACTIVE_CAPACITY),
            history: [None; TIMELINE_HISTORY],
            history_next: 0,
            stages: [StageAggregate::default(); STAGE_COUNT],
            totals: LoaderTotals::default(),
            steps: 0,
            wakeups: 0,
        }
    }

    pub fn origin(&self) -> Instant {
        self.origin
    }

    /// Nanoseconds since the origin, never 0 (0 marks "not reached").
    pub fn ns(&self, at: Instant) -> u64 {
        (at.saturating_duration_since(self.origin).as_nanos() as u64).max(1)
    }

    pub fn now(&self) -> u64 {
        self.ns(Instant::now())
    }

    /// Clear aggregates, totals and history. In-flight timelines continue.
    pub fn reset(&mut self) {
        self.reset_at = self.now();
        self.history = [None; TIMELINE_HISTORY];
        self.history_next = 0;
        self.stages = [StageAggregate::default(); STAGE_COUNT];
        self.totals = LoaderTotals::default();
    }

    /// Nanoseconds since the origin of the last reset (0: never reset).
    pub fn reset_at(&self) -> u64 {
        self.reset_at
    }

    pub fn note_step(&mut self) {
        self.steps += 1;
        self.totals.steps += 1;
    }

    /// The host's event loop woke (for frame dependence: wakeups versus
    /// loader steps per asset).
    pub fn note_wakeup(&mut self) {
        self.wakeups += 1;
        self.totals.wakeups += 1;
    }

    /// Start `uuid`'s timeline at `at` unless one is in flight.
    pub fn begin(&mut self, uuid: AssetUuid, at: u64) {
        if self.index.contains_key(&uuid) {
            return;
        }
        if self.active.len() >= ACTIVE_CAPACITY {
            self.totals.dropped += 1;
            return;
        }
        self.index.insert(uuid, self.active.len() as u32);
        self.active
            .push(AssetTimeline::new(uuid, at, self.steps, self.wakeups));
    }

    pub fn is_active(&self, uuid: AssetUuid) -> bool {
        self.index.contains_key(&uuid)
    }

    fn get_mut(&mut self, uuid: AssetUuid) -> Option<&mut AssetTimeline> {
        let slot = *self.index.get(&uuid)? as usize;
        self.active.get_mut(slot)
    }

    /// Set `mark` of an in-flight timeline (a later round overwrites).
    pub fn mark(&mut self, uuid: AssetUuid, mark: Mark, at: u64) {
        if let Some(timeline) = self.get_mut(uuid) {
            timeline.marks[mark as usize] = at;
        }
    }

    /// The IO's view of a fetch, then the loader's delivery mark.
    pub fn fetched(&mut self, uuid: AssetUuid, timing: &FetchTiming, delivered: u64) {
        let marks = [
            (Mark::FetchStarted, timing.started),
            (Mark::FetchAnswered, timing.answered),
            (Mark::FetchAdmitted, timing.admitted),
            (Mark::ChunksReceived, timing.received),
            (Mark::FetchCompleted, timing.completed),
        ]
        .map(|(mark, at)| (mark, at.map_or(0, |at| self.ns(at))));
        if let Some(timeline) = self.get_mut(uuid) {
            for (mark, at) in marks {
                timeline.marks[mark as usize] = at;
            }
            timeline.marks[Mark::Delivered as usize] = delivered;
            timeline.bytes = timing.bytes;
            timeline.chunks = timing.chunks;
            timeline.copy_ns = timing.copy_ns;
        }
    }

    pub fn accepted(
        &mut self,
        uuid: AssetUuid,
        type_uuid: TypeUuid,
        parts: AcceptParts,
        at: u64,
    ) {
        if let Some(timeline) = self.get_mut(uuid) {
            timeline.type_uuid = Some(type_uuid);
            timeline.accept = parts;
            timeline.marks[Mark::Accepted as usize] = at;
            // A restaged round measures its own updates.
            timeline.marks[Mark::Staged as usize] = 0;
            timeline.update_ns = 0;
            timeline.staging_ns = 0;
        }
    }

    /// One handle's `update` took `update_ns`, `staging_ns` of it staging
    /// writes. The first update of a round sets the staged mark at `began`.
    pub fn updated(&mut self, uuid: AssetUuid, began: u64, update_ns: u64, staging_ns: u64) {
        if let Some(timeline) = self.get_mut(uuid) {
            if timeline.marks[Mark::Staged as usize] == 0 {
                timeline.marks[Mark::Staged as usize] = began;
                timeline.update_ns = 0;
                timeline.staging_ns = 0;
            }
            timeline.update_ns += update_ns;
            timeline.staging_ns += staging_ns;
        }
    }

    /// End `uuid`'s timeline; aggregates take its stages.
    pub fn finish(&mut self, uuid: AssetUuid, outcome: Outcome, at: u64) {
        let Some(slot) = self.index.remove(&uuid) else {
            return;
        };
        let slot = slot as usize;
        let mut timeline = self.active.swap_remove(slot);
        if let Some(moved) = self.active.get(slot) {
            self.index.insert(moved.uuid, slot as u32);
        }
        timeline.outcome = outcome;
        if outcome == Outcome::Resident {
            timeline.marks[Mark::Resident as usize] = at;
        }
        timeline.steps = self.steps - timeline.steps;
        timeline.wakeups = self.wakeups - timeline.wakeups;
        let started_before_reset = timeline.marks[Mark::Request as usize] < self.reset_at;
        if !started_before_reset {
            match outcome {
                Outcome::Resident => self.totals.resident += 1,
                Outcome::Unchanged => self.totals.unchanged += 1,
                Outcome::Failed => self.totals.failed += 1,
                Outcome::Abandoned => self.totals.abandoned += 1,
                Outcome::Active => {}
            }
            if matches!(outcome, Outcome::Resident | Outcome::Unchanged) {
                self.totals.fetched_bytes += timeline.bytes;
                self.totals.fetched_chunks += u64::from(timeline.chunks);
                for stage in Stage::ALL {
                    if let Some(ns) = timeline.stage(stage) {
                        self.stages[stage as usize].add(ns);
                    }
                }
            }
        }
        self.history[self.history_next] = Some(timeline);
        self.history_next = (self.history_next + 1) % TIMELINE_HISTORY;
    }

    pub fn active(&self) -> &[AssetTimeline] {
        &self.active
    }

    /// Finished timelines, oldest first.
    pub fn history(&self) -> impl Iterator<Item = &AssetTimeline> {
        let (newer, older) = self.history.split_at(self.history_next);
        older.iter().chain(newer).flatten()
    }

    pub fn stage(&self, stage: Stage) -> StageAggregate {
        self.stages[stage as usize]
    }

    pub fn totals(&self) -> LoaderTotals {
        self.totals
    }

    /// Loader steps and host wakeups ever: an in-flight timeline has spent
    /// these minus its own `steps` and `wakeups`.
    pub fn counters(&self) -> (u64, u64) {
        (self.steps, self.wakeups)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn uuid(n: u8) -> AssetUuid {
        AssetUuid([n; 16])
    }

    #[test]
    fn stages_derive_from_marks_and_parts() {
        let origin = Instant::now();
        let mut stats = LoaderStats::with_origin(origin);
        let at = |ms: u64| origin + Duration::from_millis(ms);
        let ns = |ms: u64| ms * 1_000_000;
        stats.note_step();
        stats.begin(uuid(1), ns(1));
        stats.mark(uuid(1), Mark::Resolved, ns(3));
        stats.mark(uuid(1), Mark::FetchIssued, ns(3));
        stats.note_step();
        stats.note_wakeup();
        let timing = FetchTiming {
            started: Some(at(4)),
            answered: Some(at(5)),
            admitted: Some(at(5)),
            received: Some(at(9)),
            completed: Some(at(10)),
            chunks: 4,
            bytes: 4096,
            copy_ns: 1000,
        };
        stats.fetched(uuid(1), &timing, ns(12));
        let parts = AcceptParts {
            parse_ns: 100,
            dswl_ns: 200,
            plan_ns: 300,
            construct_ns: 400,
        };
        stats.accepted(uuid(1), TypeUuid([9; 16]), parts, ns(13));
        stats.updated(uuid(1), ns(15), ns(2), ns(1));
        stats.note_step();
        stats.finish(uuid(1), Outcome::Resident, ns(20));

        let timeline = *stats.history().next().expect("finished");
        assert!(!stats.is_active(uuid(1)));
        assert_eq!(timeline.outcome, Outcome::Resident);
        assert_eq!(timeline.steps, 2);
        assert_eq!(timeline.wakeups, 1);
        let expect = [
            (Stage::Resolve, ns(2)),
            (Stage::FetchQueue, ns(1)),
            (Stage::FetchAnswer, ns(1)),
            (Stage::Admission, 0),
            (Stage::Chunks, ns(4)),
            (Stage::WireTree, ns(1)),
            (Stage::Deliver, ns(2)),
            (Stage::Accept, ns(1)),
            (Stage::AcceptParse, 100),
            (Stage::AcceptConstruct, 400),
            (Stage::SweepWait, ns(2)),
            (Stage::StorageUpdate, ns(2)),
            (Stage::StagingWrite, ns(1)),
            (Stage::Commit, ns(3)),
            (Stage::Total, ns(19)),
        ];
        for (stage, value) in expect {
            assert_eq!(timeline.stage(stage), Some(value), "{}", stage.name());
            assert_eq!(stats.stage(stage).count, 1, "{}", stage.name());
            assert_eq!(stats.stage(stage).sum_ns, value, "{}", stage.name());
        }
        let totals = stats.totals();
        assert_eq!((totals.resident, totals.fetched_bytes, totals.fetched_chunks), (1, 4096, 4));
        assert_eq!((totals.steps, totals.wakeups), (3, 1));
    }

    #[test]
    fn unfetched_timelines_aggregate_only_their_stages() {
        let mut stats = LoaderStats::new();
        stats.begin(uuid(2), 10);
        stats.mark(uuid(2), Mark::Resolved, 30);
        stats.finish(uuid(2), Outcome::Unchanged, 40);
        assert_eq!(stats.stage(Stage::Resolve).sum_ns, 20);
        assert_eq!(stats.stage(Stage::Total).count, 0);
        assert_eq!(stats.stage(Stage::AcceptParse).count, 0);
        assert_eq!(stats.stage(Stage::StorageUpdate).count, 0);
        stats.begin(uuid(3), 10);
        stats.finish(uuid(3), Outcome::Failed, 50);
        assert_eq!(stats.stage(Stage::Resolve).count, 1);
        assert_eq!(stats.totals().failed, 1);
    }

    #[test]
    fn begin_keeps_the_first_request_and_finish_reindexes() {
        let mut stats = LoaderStats::new();
        for n in 1..=3 {
            stats.begin(uuid(n), u64::from(n));
        }
        stats.begin(uuid(1), 99);
        stats.finish(uuid(1), Outcome::Abandoned, 100);
        // uuid(3) moved into slot 0; marks still land on it.
        stats.mark(uuid(3), Mark::Resolved, 7);
        let three = stats.active().iter().find(|t| t.uuid == uuid(3)).unwrap();
        assert_eq!(three.mark(Mark::Request), Some(3));
        assert_eq!(three.mark(Mark::Resolved), Some(7));
        assert_eq!(stats.active().len(), 2);
        assert_eq!(stats.history().next().unwrap().mark(Mark::Request), Some(1));
    }

    #[test]
    fn a_full_table_drops_and_history_keeps_the_newest() {
        let mut stats = LoaderStats::new();
        let capacity = stats.active.capacity();
        for n in 0..ACTIVE_CAPACITY as u32 + 5 {
            let mut id = [0; 16];
            id[..4].copy_from_slice(&n.to_le_bytes());
            stats.begin(AssetUuid(id), 1);
        }
        assert_eq!(stats.active().len(), ACTIVE_CAPACITY);
        assert_eq!(stats.totals().dropped, 5);
        assert_eq!(stats.active.capacity(), capacity, "no growth past the reserve");
        let ids: Vec<_> = stats.active().iter().map(|t| t.uuid).collect();
        for (n, id) in ids.iter().enumerate() {
            stats.finish(*id, Outcome::Resident, 2 + n as u64);
        }
        let history: Vec<_> = stats.history().collect();
        assert_eq!(history.len(), TIMELINE_HISTORY);
        assert_eq!(history.last().unwrap().uuid, *ids.last().unwrap());
        assert_eq!(history[0].uuid, ids[ids.len() - TIMELINE_HISTORY]);
    }

    #[test]
    fn reset_clears_aggregates_and_skips_older_requests() {
        let mut stats = LoaderStats::new();
        stats.begin(uuid(1), 1);
        stats.begin(uuid(2), 1);
        stats.finish(uuid(1), Outcome::Resident, 5);
        assert_eq!(stats.totals().resident, 1);
        stats.reset();
        assert_eq!(stats.totals(), LoaderTotals::default());
        assert_eq!(stats.history().count(), 0);
        // Requested before the reset: kept in history, not aggregated.
        stats.finish(uuid(2), Outcome::Resident, stats.now());
        assert_eq!(stats.totals().resident, 0);
        assert_eq!(stats.stage(Stage::Total).count, 0);
        assert_eq!(stats.history().count(), 1);
    }
}
