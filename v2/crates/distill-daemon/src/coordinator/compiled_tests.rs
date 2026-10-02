//! The compiled state under the process loop's configuration publications:
//! a candidate's state is invisible to every other writer and reader until
//! its input commits, a failed one changes nothing, and a snapshot keeps the
//! state of the version it sees.

use std::cell::RefCell;
use std::rc::Rc;

use distill_build::pipeline::{GraphicsApi, TargetArch, TargetOs};
use distill_rpc::TargetDefinitionHash;
use distill_schema::ngp_schema::{LayoutIdentity, Schema, SchemaLayouts};
use distill_store::state::InputVersion;

use super::*;
use crate::epoch::ModuleAbiIdentity;

type Hook = Box<dyn FnMut() -> Result<(), String>>;

thread_local! {
    static CANDIDATE_STAGED: RefCell<Option<Hook>> = const { RefCell::new(None) };
}

/// Called on the publishing thread once a configuration candidate staged
/// its compiled state, its input still open and the write lock held.
pub(super) fn candidate_staged() -> Result<(), String> {
    match CANDIDATE_STAGED.with(|hook| hook.borrow_mut().take()) {
        Some(mut hook) => hook(),
        None => Ok(()),
    }
}

/// Run `hook` in the next configuration publication on this thread.
fn on_candidate_staged(hook: impl FnMut() -> Result<(), String> + 'static) {
    CANDIDATE_STAGED.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

fn layout_identity() -> LayoutIdentity {
    LayoutIdentity {
        target_triple: "x86_64-unknown-linux-gnu".into(),
        rustc: "rustc test".into(),
        algorithm_version: 1,
    }
}

fn authority(source: u8) -> Arc<ProjectSchemaAuthority> {
    Arc::new(
        ProjectSchemaAuthority::from_schema(
            Schema {
                source_hashes: BTreeMap::new(),
                type_ops_hash: String::new(),
                layout_hashes: Default::default(),
                rustc_version: String::new(),
                types: Vec::new(),
                layouts: vec![SchemaLayouts {
                    identity: layout_identity(),
                    layouts: Vec::new(),
                }],
            },
            [source; 32],
        )
        .unwrap(),
    )
}

fn build_target(optimize: bool) -> Target {
    Target::new(
        TargetOs::Linux,
        TargetArch::X86_64,
        BTreeSet::from([GraphicsApi::new("vulkan").unwrap()]),
        optimize,
        true,
        layout_identity(),
    )
    .unwrap()
}

fn rpc_target(optimize: bool) -> TargetDefinition {
    TargetDefinition::new(
        "dev",
        TargetDefinitionHash(distill_build::keys::target_definition_hash(&build_target(
            optimize,
        ))),
    )
}

struct Fixture {
    temp: tempfile::TempDir,
    coordinator: Arc<DaemonCoordinator>,
    writer: StoreWriter,
}

impl Fixture {
    /// A coordinator whose loop published one configuration: the `main`
    /// root and an unoptimized `dev` target. Its pipeline module does not
    /// exist, so the configuration publishes the pipeline's failure.
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("assets")).unwrap();
        std::fs::create_dir_all(temp.path().join("extra")).unwrap();
        let mut config = StoreConfig::new(temp.path().join("state"));
        config.parallelism = 1;
        let coordinator = Arc::new(
            DaemonCoordinator::open(
                config,
                vec![AssetRoot::new("main", temp.path().join("assets"))],
                vec![rpc_target(false)],
                8,
            )
            .unwrap(),
        );
        let mut writer = coordinator.open_writer().unwrap();
        coordinator.reconcile_full_scan(&mut writer).unwrap();
        let mut fixture = Self {
            temp,
            coordinator,
            writer,
        };
        let candidate = fixture.candidate(false, false);
        fixture
            .coordinator
            .publish_configuration_candidate(&mut fixture.writer, candidate)
            .unwrap();
        fixture
    }

    /// A configuration whose `dev` target is optimized when `optimize`, with
    /// a second root when `extra_root`.
    fn candidate(&self, optimize: bool, extra_root: bool) -> ConfigurationCandidate {
        let mut roots = vec![AssetRoot::new("main", self.temp.path().join("assets"))];
        if extra_root {
            roots.push(AssetRoot::new("extra", self.temp.path().join("extra")));
        }
        ConfigurationCandidate {
            roots,
            targets: vec![rpc_target(optimize)],
            build_targets: BTreeMap::from([("dev".to_owned(), build_target(optimize))]),
            pipeline_source: self.temp.path().join("missing-pipeline.so"),
            requirements: CandidateRequirements {
                module_abi: ModuleAbiIdentity {
                    rustc: "rustc test".into(),
                    interface_fingerprint: [1; 32],
                    measured_interface: [1; 32],
                    panic_strategy: "unwind".into(),
                    allocator: "system".into(),
                },
                source_hashes: BTreeMap::new(),
                layout_hashes: BTreeMap::new(),
                schema_registry: BTreeMap::new(),
                targets: Vec::new(),
            },
            schema_authority: authority(if optimize { 2 } else { 1 }),
        }
    }
}

fn optimized(compiled: &Compiled) -> bool {
    compiled
        .build_target("dev")
        .expect("the configuration publishes a dev target")
        .optimize
}

fn root_names(compiled: &Compiled) -> Vec<String> {
    compiled.roots().iter().map(|root| root.name.clone()).collect()
}

#[test]
fn a_failed_configuration_commit_changes_no_compiled_state() {
    let mut fixture = Fixture::new();
    let coordinator = Arc::clone(&fixture.coordinator);
    let reader = coordinator.open_reader().unwrap();
    let version = reader.input_version();
    let snapshot = coordinator.open_reader().unwrap().begin_snapshot().unwrap();
    let before = coordinator.compiled_at(&reader).unwrap();
    assert_eq!(before.key(), Some(version));
    assert!(!optimized(&before));
    assert_eq!(root_names(&before), ["main"]);

    // The candidate stages its state under the version it would publish,
    // then its input fails before the commit.
    let staged = Rc::new(RefCell::new(None));
    {
        let staged = Rc::clone(&staged);
        let coordinator = Arc::clone(&coordinator);
        on_candidate_staged(move || {
            *staged.borrow_mut() = Some(coordinator.compiled.get(Some(InputVersion(version.0 + 1))));
            Err("injected commit failure".to_owned())
        });
    }
    let candidate = fixture.candidate(true, true);
    assert!(coordinator
        .publish_configuration_candidate(&mut fixture.writer, candidate)
        .is_err());
    assert!(
        matches!(staged.borrow().as_ref(), Some(Ok(compiled)) if optimized(compiled)),
        "the hook ran with the candidate staged"
    );

    // Nothing changed: the version, every reader's compiled state, the
    // registry's latest entry and the watcher's roots.
    let reader = coordinator.open_reader().unwrap();
    assert_eq!(reader.input_version(), version);
    assert!(Arc::ptr_eq(&coordinator.compiled_at(&reader).unwrap(), &before));
    assert!(Arc::ptr_eq(&coordinator.compiled_at(&snapshot).unwrap(), &before));
    assert!(Arc::ptr_eq(&coordinator.compiled.latest().unwrap(), &before));
    assert!(matches!(
        coordinator.compiled.get(Some(InputVersion(version.0 + 1))),
        Err(CompiledLookupError::NotLoaded { .. })
    ));
    assert!(coordinator.scanner().has_same_roots(before.scanner()));
    assert_eq!(
        coordinator.authoring_service().compiled(&reader).unwrap().key(),
        before.key()
    );

    // The same candidate publishes once its input commits.
    let candidate = fixture.candidate(true, true);
    let stamp = coordinator
        .publish_configuration_candidate(&mut fixture.writer, candidate)
        .unwrap();
    assert_eq!(stamp.version, InputVersion(version.0 + 1));
    let after = coordinator
        .compiled_at(&coordinator.open_reader().unwrap())
        .unwrap();
    assert_eq!(after.key(), Some(stamp.version));
    assert!(optimized(&after));
    assert_eq!(root_names(&after), ["main", "extra"]);
    assert!(coordinator.scanner().has_same_roots(after.scanner()));
}

#[test]
fn readers_writers_and_build_workers_see_a_configuration_only_once_it_commits() {
    let mut fixture = Fixture::new();
    let coordinator = Arc::clone(&fixture.coordinator);
    // An RPC connection's reader and writer, and an older held snapshot.
    let rpc_reader = Rc::new(coordinator.open_reader().unwrap());
    let rpc_writer = Rc::new(coordinator.open_writer().unwrap());
    let old_snapshot = coordinator.open_reader().unwrap().begin_snapshot().unwrap();
    let old = coordinator.compiled_at(&old_snapshot).unwrap();
    // The build worker's writer is open before the loop takes the lock.
    let worker_view = |coordinator: &Arc<DaemonCoordinator>| {
        let compiled_at = Arc::clone(coordinator);
        coordinator.run_scheduled(WorkClass::Interactive, move |store| {
            compiled_at.compiled_at(store).map(|compiled| optimized(&compiled))
        })
    };
    assert_eq!(worker_view(&coordinator), Ok(false));

    // While the loop holds the write lock with the candidate staged.
    let during = Rc::new(RefCell::new(None));
    {
        let during = Rc::clone(&during);
        let coordinator = Arc::clone(&coordinator);
        let rpc_reader = Rc::clone(&rpc_reader);
        let rpc_writer = Rc::clone(&rpc_writer);
        on_candidate_staged(move || {
            let reader = coordinator.compiled_at(&rpc_reader).map(|c| optimized(&c));
            let writer = coordinator
                .authoring_service()
                .compiled(&rpc_writer)
                .map(|c| optimized(&c));
            let worker = worker_view(&coordinator);
            let fresh = coordinator
                .open_reader()
                .map_err(|error| error.to_string())?
                .begin_snapshot()
                .map_err(|error| error.to_string())
                .map(|snapshot| coordinator.compiled_at(&snapshot).map(|c| optimized(&c)))?;
            *during.borrow_mut() = Some((reader, writer, worker, fresh));
            Ok(())
        });
    }
    let candidate = fixture.candidate(true, false);
    let stamp = coordinator
        .publish_configuration_candidate(&mut fixture.writer, candidate)
        .unwrap();
    let (reader, writer, worker, fresh) = during.borrow_mut().take().expect("the hook ran");
    assert_eq!(reader, Ok(false), "an RPC reader saw the uncommitted state");
    assert_eq!(writer.map_err(|_| ()), Ok(false), "an RPC writer saw the uncommitted state");
    assert_eq!(worker, Ok(false), "a build worker saw the uncommitted state");
    assert_eq!(fresh, Ok(false), "a new snapshot saw the uncommitted state");

    // Committed: every new view sees all of it.
    let after = coordinator.compiled_at(&rpc_reader).unwrap();
    assert_eq!(after.key(), Some(stamp.version));
    assert!(optimized(&after));
    assert!(Arc::ptr_eq(
        &coordinator.authoring_service().compiled(&rpc_writer).unwrap(),
        &after
    ));
    assert_eq!(worker_view(&coordinator), Ok(true));
    assert_eq!(
        coordinator.open_reader().unwrap().rpc_target("dev").unwrap().unwrap().definition_hash,
        rpc_target(true).definition_hash().0
    );
    // The older snapshot keeps the state of the version it sees.
    assert!(Arc::ptr_eq(&coordinator.compiled_at(&old_snapshot).unwrap(), &old));
    assert!(!optimized(&coordinator.compiled_at(&old_snapshot).unwrap()));
}
