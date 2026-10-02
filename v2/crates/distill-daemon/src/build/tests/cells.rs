//! Build cells end to end (DESIGN.md § Build cells): requesters at their own
//! snapshots share one build per node key, through the production backend
//! and the daemon's build workers. Every wait is watchdog-bounded; ordering
//! comes from gates in a scripted processor, never from sleeps.

use super::*;
use std::collections::BTreeSet;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use distill_bundle::{AssetEntry, Bundle};
use distill_rpc::BuildWorkClass;

/// A node built from `value`; its asset and bundle uuids derive from it.
fn asset(value: u8) -> AssetUuid {
    AssetUuid([value; 16])
}

fn bundle(value: u8) -> BundleUuid {
    let mut uuid = [value; 16];
    uuid[0] = 0xb0;
    BundleUuid(uuid)
}

/// What the scripted processor does per authored value.
#[derive(Default)]
struct Script {
    /// Built artifacts a value's processor reads (strong dynamic inputs).
    reads: BTreeMap<u64, Vec<AssetUuid>>,
    /// Values whose processor blocks until the test opens their gate:
    /// before its reads (`true`) or after them (`false`).
    gated: BTreeMap<u64, bool>,
    /// Values whose processor fails.
    failing: BTreeSet<u64>,
}

#[derive(Default)]
struct GateState {
    entered: BTreeSet<u64>,
    open: BTreeSet<u64>,
}

#[derive(Default)]
struct Shared {
    script: Mutex<Script>,
    calls: Mutex<Vec<u64>>,
    gate: Mutex<GateState>,
    changed: Condvar,
}

impl Shared {
    fn calls_of(&self, value: u8) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| **call == u64::from(value))
            .count()
    }

    fn pass_gate(&self, value: u64) {
        let mut gate = self.gate.lock().unwrap();
        gate.entered.insert(value);
        self.changed.notify_all();
        let deadline = Instant::now() + WATCHDOG;
        while !gate.open.contains(&value) {
            let left = deadline
                .checked_duration_since(Instant::now())
                .expect("watchdog: a gate was never opened");
            gate = self.changed.wait_timeout(gate, left).unwrap().0;
        }
    }

    fn wait_entered(&self, value: u8) {
        let mut gate = self.gate.lock().unwrap();
        let deadline = Instant::now() + WATCHDOG;
        while !gate.entered.contains(&u64::from(value)) {
            let left = deadline
                .checked_duration_since(Instant::now())
                .expect("watchdog: a gated processor never ran");
            gate = self.changed.wait_timeout(gate, left).unwrap().0;
        }
    }

    fn open(&self, value: u8) {
        self.gate.lock().unwrap().open.insert(u64::from(value));
        self.changed.notify_all();
    }
}

struct ScriptedProcessor(Arc<Shared>);

impl PipelineProcessor for ScriptedProcessor {
    fn process(
        &self,
        input: AuthoredValue,
        context: &mut dyn PipelineProcessContext,
    ) -> Result<ProcessorProducts, ProcessorError> {
        let AuthoredValue::Object(fields) = &input else {
            panic!("processor input is a struct");
        };
        let Some(AuthoredValue::UInt(value)) = fields.get("value") else {
            panic!("processor input has a value");
        };
        let value = u64::try_from(*value).unwrap();
        self.0.calls.lock().unwrap().push(value);
        let (reads, gate, fails) = {
            let script = self.0.script.lock().unwrap();
            (
                script.reads.get(&value).cloned().unwrap_or_default(),
                script.gated.get(&value).copied(),
                script.failing.contains(&value),
            )
        };
        if gate == Some(true) {
            self.0.pass_gate(value);
        }
        for dependency in reads {
            context
                .read(dependency, TERMINAL)
                .map_err(|error| ProcessorError::new(8, format!("{error:?}")))?;
        }
        if gate == Some(false) {
            self.0.pass_gate(value);
        }
        if fails {
            return Err(ProcessorError::new(7, format!("value {value} does not cook")));
        }
        Ok(ProcessorProducts {
            primary: Some(ProcessorProduct::new(TERMINAL, input)),
            extras: BTreeMap::new(),
            debug: BTreeMap::new(),
        })
    }
}

/// A daemon over one bundle per authored value, cooking every value with
/// the scripted processor.
struct Cells {
    _temp: tempfile::TempDir,
    assets: std::path::PathBuf,
    authority: Arc<ProjectSchemaAuthority>,
    target_hash: TargetDefinitionHash,
    coordinator: Arc<DaemonCoordinator>,
    writer: distill_store::StoreWriter,
    shared: Arc<Shared>,
    /// Each asset's current value and file.
    files: BTreeMap<AssetUuid, (u8, String)>,
}

impl Cells {
    fn new(parallelism: usize, values: &[u8], script: Script) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let assets = temp.path().join("assets");
        std::fs::create_dir_all(&assets).unwrap();
        let authority = Arc::new(authority());
        let build_target = Target::new(
            TargetOs::Linux,
            TargetArch::X86_64,
            BTreeSet::from([GraphicsApi::new("vulkan").unwrap()]),
            false,
            true,
            authority.identity().clone(),
        )
        .unwrap();
        let target_hash =
            TargetDefinitionHash(distill_build::keys::target_definition_hash(&build_target));
        let mut cells_files = BTreeMap::new();
        for value in values {
            cells_files.insert(asset(*value), (*value, format!("node-{value}.bundle")));
        }
        let mut config = StoreConfig::new(temp.path().join("state"));
        config.parallelism = parallelism;
        config.batch_reserved_workers = 1;
        let coordinator = Arc::new(
            DaemonCoordinator::open(
                config,
                vec![AssetRoot::new("main", &assets)],
                vec![rpc_target(target_hash)],
                64,
            )
            .unwrap(),
        );
        let writer = coordinator.open_writer().unwrap();
        let shared = Arc::new(Shared {
            script: Mutex::new(script),
            ..Shared::default()
        });
        let mut cells = Self {
            _temp: temp,
            assets,
            authority,
            target_hash,
            coordinator,
            writer,
            shared,
            files: cells_files,
        };
        for (uuid, (value, _)) in cells.files.clone() {
            cells.write_bundle(uuid, value);
        }
        cells
            .coordinator
            .reconcile_full_scan(&mut cells.writer)
            .unwrap();
        cells
            .coordinator
            .install_schema_authority_for_test(Arc::clone(&cells.authority));
        cells
            .coordinator
            .install_build_target_for_test("dev", build_target);
        cells
            .coordinator
            .install_pipeline_epoch_for_test(crate::epoch::processor_test_epoch(
                "dev",
                target_hash.0,
                crate::callbacks::ProcessorDescriptor {
                    id: "scripted".to_owned(),
                    version: 1,
                    input: TYPE,
                    selector: TargetSelector::new(None, None).unwrap(),
                    outputs: OutputDecls::new(TERMINAL, Vec::<(String, TypeUuid)>::new())
                        .unwrap(),
                },
                ScriptedProcessor(Arc::clone(&cells.shared)),
            ));
        cells
    }

    fn write_bundle(&mut self, uuid: AssetUuid, value: u8) {
        let project = self.authority.project_type(TYPE).unwrap();
        let file = self.files.get_mut(&uuid).unwrap();
        file.0 = value;
        let written = Bundle {
            format_version: 1,
            uuid: bundle(uuid.0[0]),
            primary: Some("entry".to_owned()),
            schemas: BTreeMap::from([(project.logical_hash, project.logical_schema.clone())]),
            assets: BTreeMap::from([(
                "entry".to_owned(),
                AssetEntry {
                    uuid,
                    type_uuid: TYPE,
                    schema_hash: project.logical_hash,
                    authoring_only: false,
                    data: AuthoredValue::Object(BTreeMap::from([(
                        "value".to_owned(),
                        AuthoredValue::UInt(u128::from(value)),
                    )])),
                },
            )]),
        };
        let path = self.assets.join(&file.1);
        std::fs::write(&path, distill_bundle::write_bundle(&written).unwrap()).unwrap();
    }

    /// Commit `uuid`'s new authored `value`: a new store version.
    fn commit_value(&mut self, uuid: AssetUuid, value: u8) {
        let before = self.coordinator.server().current_stamp();
        self.write_bundle(uuid, value);
        self.coordinator
            .reconcile_full_scan(&mut self.writer)
            .unwrap();
        assert_ne!(self.coordinator.server().current_stamp(), before);
    }

    fn request(&self, uuid: AssetUuid, class: BuildWorkClass) -> BuildRequest {
        let project = self.authority.project_type(TYPE).unwrap();
        let (value, file) = &self.files[&uuid];
        BuildRequest {
            work_class: class,
            target: "dev".to_owned(),
            target_definition: self.target_hash,
            requested_asset: uuid,
            output_key: String::new(),
            requested_terminal_type: TERMINAL,
            entry: AuthoringEntry {
                uuid,
                bundle: bundle(uuid.0[0]),
                local_id: "entry".to_owned(),
                normalized_path: file.clone(),
                type_uuid: TYPE,
                terminal_type: TERMINAL,
                schema_hash: project.logical_hash,
                logical_schema: Arc::from(
                    snapshot_to_json(&project.logical_schema)
                        .unwrap()
                        .into_bytes(),
                ),
                role: AuthoringEntryRole::Runtime,
                tags: BTreeMap::new(),
                value: AuthoringValue {
                    canonical_value: Arc::from(format!("{{\"value\":{value}}}").into_bytes()),
                    blobs: Vec::new(),
                },
            },
            drifted_input: DriftedInput::Asset(uuid),
        }
    }

    fn requester(&self) -> Requester {
        Requester::at_current(&self.coordinator)
    }

    /// Start `uuid`'s build at `requester`'s snapshot; it must miss the
    /// cache and submit (or join) a cell.
    fn submit(&self, requester: &Requester, uuid: AssetUuid, class: BuildWorkClass) -> BuildTicket {
        match CoordinatorBuildBackend::new(&self.coordinator)
            .start(requester.view(), &self.request(uuid, class))
        {
            BuildStart::Submitted(ticket) => ticket,
            BuildStart::Answered(answer) => panic!("expected a submitted build, got {answer:?}"),
        }
    }

    fn wait_cells(&self, count: usize) {
        let deadline = Instant::now() + WATCHDOG;
        while self.coordinator.build_cells_in_flight() != count {
            assert!(
                Instant::now() < deadline,
                "watchdog: {} cells in flight, expected {count}",
                self.coordinator.build_cells_in_flight()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

fn finish(ticket: BuildTicket, requester: &Requester) -> BuildAnswer {
    block_on(ticket).answer(requester.view()).unwrap()
}

fn built(answer: BuildAnswer) -> ContentHash {
    match answer {
        BuildAnswer::Built { content_hash } => content_hash,
        other => panic!("expected a built answer, got {other:?}"),
    }
}

const A: u8 = 1;
const B: u8 = 2;
const C: u8 = 3;
const SHARED: u8 = 100;
const UNRELATED: u8 = 50;

#[test]
fn concurrent_requests_for_one_node_run_one_build() {
    let cells = Cells::new(
        4,
        &[A],
        Script {
            gated: BTreeMap::from([(u64::from(A), true)]),
            ..Script::default()
        },
    );
    let requesters = (0..6).map(|_| cells.requester()).collect::<Vec<_>>();
    let mut tickets = Vec::new();
    for requester in &requesters {
        tickets.push(cells.submit(requester, asset(A), BuildWorkClass::Interactive));
        if tickets.len() == 1 {
            cells.shared.wait_entered(A);
        }
    }
    assert_eq!(cells.coordinator.build_cells_in_flight(), 1);
    cells.shared.open(A);
    let hashes = tickets
        .into_iter()
        .zip(&requesters)
        .map(|(ticket, requester)| built(finish(ticket, requester)))
        .collect::<BTreeSet<_>>();
    assert_eq!(hashes.len(), 1);
    assert_eq!(cells.shared.calls_of(A), 1);
    // Finished and published: a later request is a cache hit.
    let later = cells.requester();
    match CoordinatorBuildBackend::new(&cells.coordinator).start(
        later.view(),
        &cells.request(asset(A), BuildWorkClass::Interactive),
    ) {
        BuildStart::Answered(Ok(BuildAnswer::Built { content_hash })) => {
            assert!(hashes.contains(&content_hash))
        }
        _ => panic!("a published node answers from the cache"),
    }
    assert_eq!(cells.shared.calls_of(A), 1);
}

#[test]
fn a_dependency_two_cells_share_is_built_once() {
    let cells = Cells::new(
        4,
        &[A, B, SHARED],
        Script {
            reads: BTreeMap::from([
                (u64::from(A), vec![asset(SHARED)]),
                (u64::from(B), vec![asset(SHARED)]),
            ]),
            gated: BTreeMap::from([(u64::from(SHARED), true)]),
            ..Script::default()
        },
    );
    let requester = cells.requester();
    let a = cells.submit(&requester, asset(A), BuildWorkClass::Interactive);
    let b = cells.submit(&requester, asset(B), BuildWorkClass::Interactive);
    cells.shared.wait_entered(SHARED);
    // Both dependents are past their own processor's start: one runs the
    // dependency's cell, the other waits on it.
    let deadline = Instant::now() + WATCHDOG;
    while cells.shared.calls_of(A) + cells.shared.calls_of(B) < 2 {
        assert!(Instant::now() < deadline, "watchdog: a dependent never ran");
        std::thread::sleep(Duration::from_millis(1));
    }
    cells.shared.open(SHARED);
    built(finish(a, &requester));
    built(finish(b, &requester));
    assert_eq!(cells.shared.calls_of(SHARED), 1);
    assert_eq!(cells.shared.calls_of(A), 1);
    assert_eq!(cells.shared.calls_of(B), 1);
}

#[test]
fn requesters_at_two_snapshots_with_the_same_inputs_share_one_build() {
    let mut cells = Cells::new(
        4,
        &[A, UNRELATED],
        Script {
            gated: BTreeMap::from([(u64::from(A), true)]),
            ..Script::default()
        },
    );
    let first = cells.requester();
    let first_ticket = cells.submit(&first, asset(A), BuildWorkClass::Interactive);
    cells.shared.wait_entered(A);
    cells.commit_value(asset(UNRELATED), UNRELATED + 1);
    let second = cells.requester();
    assert_ne!(first.snapshot.stamp(), second.snapshot.stamp());
    let second_ticket = cells.submit(&second, asset(A), BuildWorkClass::Batch);
    assert_eq!(cells.coordinator.build_cells_in_flight(), 1);
    cells.shared.open(A);
    let first_hash = built(finish(first_ticket, &first));
    assert_eq!(built(finish(second_ticket, &second)), first_hash);
    assert_eq!(cells.shared.calls_of(A), 1);
}

#[test]
fn an_unrelated_commit_during_a_build_does_not_fail_it() {
    let mut cells = Cells::new(
        4,
        &[A, B, UNRELATED],
        Script {
            reads: BTreeMap::from([(u64::from(A), vec![asset(B)])]),
            gated: BTreeMap::from([(u64::from(A), false)]),
            ..Script::default()
        },
    );
    let requester = cells.requester();
    let ticket = cells.submit(&requester, asset(A), BuildWorkClass::Interactive);
    cells.shared.wait_entered(A);
    cells.commit_value(asset(UNRELATED), UNRELATED + 1);
    cells.shared.open(A);
    let hash = built(finish(ticket, &requester));
    // The published node holds at the newer snapshot too: a cache hit.
    let later = cells.requester();
    match CoordinatorBuildBackend::new(&cells.coordinator).start(
        later.view(),
        &cells.request(asset(A), BuildWorkClass::Interactive),
    ) {
        BuildStart::Answered(Ok(BuildAnswer::Built { content_hash })) => {
            assert_eq!(content_hash, hash)
        }
        _ => panic!("the node holds after an unrelated commit"),
    }
    assert_eq!(cells.shared.calls_of(A), 1);
    assert_eq!(cells.shared.calls_of(B), 1);
}

#[test]
fn a_waiter_whose_snapshot_drifted_gets_drifted_while_a_matching_one_gets_built() {
    let mut cells = Cells::new(
        4,
        &[A, B],
        Script {
            reads: BTreeMap::from([(u64::from(A), vec![asset(B)])]),
            gated: BTreeMap::from([(u64::from(A), false)]),
            ..Script::default()
        },
    );
    let matching = cells.requester();
    let matching_ticket = cells.submit(&matching, asset(A), BuildWorkClass::Interactive);
    // A has read B's build at the cell's view; B then changes.
    cells.shared.wait_entered(A);
    cells.commit_value(asset(B), B + 10);
    let drifted = cells.requester();
    // A's own static inputs are unchanged, so the newer requester joins the
    // running cell.
    let drifted_ticket = cells.submit(&drifted, asset(A), BuildWorkClass::Interactive);
    assert_eq!(cells.coordinator.build_cells_in_flight(), 1);
    cells.shared.open(A);
    built(finish(matching_ticket, &matching));
    assert_eq!(
        finish(drifted_ticket, &drifted),
        BuildAnswer::Drifted {
            input: DriftedInput::Asset(asset(B))
        }
    );
    assert_eq!(cells.shared.calls_of(A), 1);
    // A fresh snapshot builds A over the new B.
    let fresh = cells.requester();
    let fresh_ticket = cells.submit(&fresh, asset(A), BuildWorkClass::Interactive);
    cells.shared.open(A);
    built(finish(fresh_ticket, &fresh));
    assert_eq!(cells.shared.calls_of(A), 2);
}

#[test]
fn a_dropped_ticket_cancels_a_queued_cell_but_not_one_another_requester_wants() {
    let cells = Cells::new(
        1,
        &[A, B, C],
        Script {
            gated: BTreeMap::from([(u64::from(A), true)]),
            ..Script::default()
        },
    );
    let requester = cells.requester();
    let a = cells.submit(&requester, asset(A), BuildWorkClass::Interactive);
    cells.shared.wait_entered(A);
    let b = cells.submit(&requester, asset(B), BuildWorkClass::Interactive);
    let c = cells.submit(&requester, asset(C), BuildWorkClass::Interactive);
    let c_again = cells.submit(&requester, asset(C), BuildWorkClass::Interactive);
    cells.wait_cells(3);
    drop(b);
    cells.wait_cells(2);
    drop(c);
    cells.shared.open(A);
    built(finish(a, &requester));
    built(finish(c_again, &requester));
    cells.wait_cells(0);
    assert_eq!(cells.shared.calls_of(B), 0);
    assert_eq!(cells.shared.calls_of(C), 1);
}

#[test]
fn a_dependency_chain_completes_on_one_build_worker() {
    let cells = Cells::new(
        1,
        &[A, B, C],
        Script {
            reads: BTreeMap::from([
                (u64::from(A), vec![asset(B)]),
                (u64::from(B), vec![asset(C)]),
            ]),
            gated: BTreeMap::from([(u64::from(A), true)]),
            ..Script::default()
        },
    );
    let requester = cells.requester();
    let a = cells.submit(&requester, asset(A), BuildWorkClass::Interactive);
    cells.shared.wait_entered(A);
    // The only worker runs A; B's and C's cells queue behind it, and A's
    // build steals them rather than waiting on a queue it occupies.
    let c = cells.submit(&requester, asset(C), BuildWorkClass::Batch);
    let b = cells.submit(&requester, asset(B), BuildWorkClass::Interactive);
    cells.wait_cells(3);
    cells.shared.open(A);
    built(finish(a, &requester));
    built(finish(b, &requester));
    built(finish(c, &requester));
    cells.wait_cells(0);
    for value in [A, B, C] {
        assert_eq!(cells.shared.calls_of(value), 1);
    }
}

#[test]
fn a_failed_build_reaches_every_waiter() {
    let cells = Cells::new(
        4,
        &[A],
        Script {
            gated: BTreeMap::from([(u64::from(A), true)]),
            failing: BTreeSet::from([u64::from(A)]),
            ..Script::default()
        },
    );
    let requesters = (0..4).map(|_| cells.requester()).collect::<Vec<_>>();
    let mut tickets = Vec::new();
    for requester in &requesters {
        tickets.push(cells.submit(requester, asset(A), BuildWorkClass::Interactive));
        if tickets.len() == 1 {
            cells.shared.wait_entered(A);
        }
    }
    cells.shared.open(A);
    let errors = tickets
        .into_iter()
        .zip(&requesters)
        .map(|(ticket, requester)| match finish(ticket, requester) {
            BuildAnswer::Failed { error } => error,
            other => panic!("expected the failure, got {other:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(errors.len(), 1);
    assert!(
        errors.first().unwrap().contains("does not cook"),
        "{errors:?}"
    );
    assert_eq!(cells.shared.calls_of(A), 1);
}
