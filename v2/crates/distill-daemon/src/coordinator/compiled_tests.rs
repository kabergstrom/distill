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
use distill_bundle::AssetEntry;
use distill_json::AuthoredValue;
use crate::epoch::ModuleAbiIdentity;

type Hook = Box<dyn FnMut() -> Result<(), String>>;

thread_local! {
    static CANDIDATE_STAGED: RefCell<Option<Hook>> = const { RefCell::new(None) };
    static CANDIDATE_SCANNED: RefCell<Option<Hook>> = const { RefCell::new(None) };
}

/// Called on the publishing thread once a configuration candidate has
/// scanned its roots, before it takes the write lock.
pub(super) fn candidate_scanned() -> Result<(), String> {
    match CANDIDATE_SCANNED.with(|hook| hook.borrow_mut().take()) {
        Some(mut hook) => hook(),
        None => Ok(()),
    }
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
    let version = reader.input_version().unwrap();
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
    assert_eq!(reader.input_version().unwrap(), version);
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

/// A publication that commits while a configuration candidate scans makes
/// the candidate stale: it publishes nothing over that write (its scan may
/// predate it), and the loop retries it.
#[test]
fn a_write_during_a_configuration_scan_makes_the_candidate_stale() {
    let mut fixture = Fixture::new();
    let coordinator = Arc::clone(&fixture.coordinator);
    let assets = fixture.temp.path().join("assets");
    {
        let coordinator = Arc::clone(&coordinator);
        CANDIDATE_SCANNED.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                std::fs::write(assets.join("written.txt"), b"x").unwrap();
                let mut other = coordinator.open_writer().unwrap();
                coordinator.reconcile_full_scan(&mut other).unwrap();
                Ok(())
            }))
        });
    }
    let written = coordinator.open_reader().unwrap().input_version().unwrap();
    let candidate = fixture.candidate(true, true);
    let published = coordinator.publish_configuration_candidate(&mut fixture.writer, candidate);
    assert!(
        matches!(
            published,
            Err(CoordinatorError::Coordinated(
                distill_rpc::CoordinatedCommitError::Stale { .. }
            ))
        ),
        "{published:?}"
    );
    assert_eq!(
        coordinator.open_reader().unwrap().input_version().unwrap(),
        InputVersion(written.0 + 1),
        "only the write published"
    );
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

const BYTES_TYPE: TypeUuid = TypeUuid([0x61; 16]);
const EDITED_TYPE: TypeUuid = TypeUuid([0x62; 16]);

/// An authority over two asset types, each a struct of one `u8`; the
/// second's field names a previous name when `edited` (an edit its
/// logical hash does not see, but its tag epoch does).
fn typed_authority(edited: bool) -> Arc<ProjectSchemaAuthority> {
    use distill_schema::ngp_schema::{
        Field, FieldAttrs, FieldIdentifier, FieldLayout, PrimitiveType, SchemaTypeId, TypeAttrs,
        TypeDef, TypeLayout, TypePath,
    };
    let path = |krate: &str, name: &str| TypePath {
        name: Some(name.to_owned()),
        containing_type: None,
        modules: Vec::new(),
        krate: krate.to_owned(),
    };
    let def = |id: usize, kind, path, uuid, fields| TypeDef {
        id: SchemaTypeId(id),
        kind,
        path,
        uuid,
        attrs: TypeAttrs::default(),
        fields,
        generic_parameters: Vec::new(),
        generic_argument_ids: Vec::new(),
        generic_const_arguments: Vec::new(),
        has_default: id == 1,
        has_explicit_discriminants: false,
    };
    let field = |edited: bool| {
        vec![Field {
            id: FieldIdentifier::Name("value".to_owned()),
            type_id: SchemaTypeId(1),
            attrs: FieldAttrs {
                renamed_from: edited.then(|| "previous".to_owned()),
                ..FieldAttrs::default()
            },
        }]
    };
    let layout = |fields| TypeLayout {
        size: Some(1),
        align: Some(1),
        layout_complete: true,
        tag_encoding: None,
        fields,
    };
    let slot = || {
        vec![FieldLayout {
            offset: Some(0),
            field_size: Some(1),
        }]
    };
    Arc::new(
        ProjectSchemaAuthority::from_schema(
            Schema {
                source_hashes: BTreeMap::new(),
                type_ops_hash: String::new(),
                layout_hashes: Default::default(),
                rustc_version: String::new(),
                types: vec![
                    def(0, PrimitiveType::Struct, path("game", "Bytes"), Some(BYTES_TYPE), field(false)),
                    def(1, PrimitiveType::U8, path("core", "u8"), None, Vec::new()),
                    def(2, PrimitiveType::Struct, path("game", "Tagged"), Some(EDITED_TYPE), field(edited)),
                ],
                layouts: vec![SchemaLayouts {
                    identity: layout_identity(),
                    layouts: vec![layout(slot()), layout(Vec::new()), layout(slot())],
                }],
            },
            [u8::from(edited) + 10; 32],
        )
        .unwrap(),
    )
}

/// A bundle of one `type_uuid` asset at the authority's current schema.
fn typed_bundle(authority: &ProjectSchemaAuthority, type_uuid: TypeUuid, index: u32) -> Vec<u8> {
    let project = authority.project_type(type_uuid).unwrap();
    let mut uuid = [type_uuid.0[0]; 16];
    uuid[..4].copy_from_slice(&index.to_le_bytes());
    distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: BundleUuid(uuid),
        primary: Some("entry".into()),
        schemas: BTreeMap::from([(project.logical_hash, project.logical_schema.clone())]),
        assets: BTreeMap::from([(
            "entry".into(),
            AssetEntry {
                uuid: AssetUuid(uuid),
                type_uuid,
                schema_hash: project.logical_hash,
                authoring_only: false,
                data: AuthoredValue::Object(BTreeMap::from([(
                    "value".to_owned(),
                    AuthoredValue::UInt(u128::from(index % 7)),
                )])),
            },
        )]),
    })
    .unwrap()
}

/// Pages the configuration publication of a schema edit to the edited type
/// reads and writes beside `filler` bundles of the other type, and the
/// pages a candidate changing nothing costs there.
fn schema_edit_pages(filler: u32) -> (u64, u64) {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::create_dir_all(temp.path().join("extra")).unwrap();
    let authority = typed_authority(false);
    for index in 0..filler {
        let directory = assets.join(format!("bytes/d{}", index % 40));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join(format!("b{index}.bundle")),
            typed_bundle(&authority, BYTES_TYPE, index),
        )
        .unwrap();
    }
    for index in 0..3 {
        std::fs::write(
            assets.join(format!("edited{index}.bundle")),
            typed_bundle(&authority, EDITED_TYPE, index),
        )
        .unwrap();
    }
    let mut config = StoreConfig::new(temp.path().join("state"));
    config.parallelism = 1;
    let coordinator = Arc::new(
        DaemonCoordinator::open(config, vec![AssetRoot::new("main", &assets)], vec![rpc_target(false)], 8)
            .unwrap(),
    );
    let writer = coordinator.open_writer().unwrap();
    let mut fixture = Fixture {
        temp,
        coordinator: Arc::clone(&coordinator),
        writer,
    };
    coordinator.reconcile_full_scan(&mut fixture.writer).unwrap();
    let publish = |fixture: &mut Fixture, edited| {
        let mut candidate = fixture.candidate(false, false);
        candidate.schema_authority = typed_authority(edited);
        let before = fixture.writer.pages_fetched().unwrap();
        REFINED.lock().unwrap().clear();
        fixture.writer.trace_statements(Some(record_refined));
        coordinator
            .publish_configuration_candidate(&mut fixture.writer, candidate)
            .unwrap();
        fixture.writer.trace_statements(None);
        fixture.writer.pages_fetched().unwrap() - before
    };
    publish(&mut fixture, false);
    let asset = |type_uuid: TypeUuid, index: u32| {
        let mut uuid = [type_uuid.0[0]; 16];
        uuid[..4].copy_from_slice(&index.to_le_bytes());
        AssetUuid(uuid)
    };
    // Whether the last publication refined `asset`'s tags (wrote its tag
    // state), leaving no row stale.
    let refined = |asset: AssetUuid| {
        let reader = coordinator.open_reader().unwrap();
        assert!(reader.stale_tag_index_assets(None).unwrap().is_empty());
        let key: String = asset.0.iter().map(|byte| format!("{byte:02X}")).collect();
        REFINED
            .lock()
            .unwrap()
            .iter()
            .any(|sql| sql.starts_with("UPDATE assets SET tag_poison") && sql.contains(&key))
    };
    let edit = publish(&mut fixture, true);
    assert!(refined(asset(EDITED_TYPE, 2)), "the edited type's rows were refined again");
    assert!(!refined(asset(BYTES_TYPE, 0)), "the other type's rows were kept");
    let unchanged = publish(&mut fixture, true);
    (edit, unchanged)
}

/// The statements of the publication `schema_edit_pages` traces.
static REFINED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

fn record_refined(sql: &str) {
    REFINED.lock().unwrap().push(sql.to_owned());
}

/// A schema edit to one type republishes that type's rows; a candidate
/// changing nothing republishes nothing. Neither reads the bundles of the
/// other type.
#[test]
fn a_schema_edit_reads_independent_of_other_types() {
    // Both namespaces are large enough that their indexes are as deep: the
    // comparison sees the rows read, not a level of B-tree.
    let (small_edit, small_unchanged) = schema_edit_pages(1000);
    let (large_edit, large_unchanged) = schema_edit_pages(8000);
    println!(
        "schema edit: {small_edit} pages beside 1000 bundles, {large_edit} beside 8000; \
         unchanged candidate: {small_unchanged} and {large_unchanged}"
    );
    assert!(large_edit <= small_edit + 16, "{small_edit} beside 1000, {large_edit} beside 8000");
    assert!(
        large_unchanged <= small_unchanged + 16,
        "{small_unchanged} beside 1000, {large_unchanged} beside 8000"
    );
}

/// An authority knowing only the bytes type: the edited type's bundles
/// parse, but a malformed one has no validated skeleton.
fn bytes_only_authority() -> Arc<ProjectSchemaAuthority> {
    let full = typed_authority(false);
    let mut schema = full.schema().clone();
    schema.types.truncate(2);
    schema.layouts[0].layouts.truncate(2);
    Arc::new(ProjectSchemaAuthority::from_schema(schema, [20; 32]).unwrap())
}

/// The projection under which the bytes type cooks to `terminal`, or to
/// itself.
fn bytes_projection(terminal: Option<TypeUuid>) -> PipelineProjection {
    use distill_build::outputs::OutputDecls;
    use distill_build::pipeline::TargetSelector;
    let descriptors = terminal
        .map(|terminal| crate::callbacks::ProcessorDescriptor {
            id: "processor".to_owned(),
            version: 1,
            input: BYTES_TYPE,
            selector: TargetSelector::new(None, None).unwrap(),
            outputs: OutputDecls::new(terminal, Vec::new()).unwrap(),
        })
        .into_iter()
        .collect();
    PipelineProjection::build(
        descriptors,
        [9; 32],
        &BTreeMap::from([("dev".to_owned(), build_target(false))]),
        [BYTES_TYPE, EDITED_TYPE],
    )
    .unwrap()
}

/// A pipeline epoch of module `dylib` over `authority`'s types.
fn pipeline_publication(authority: &ProjectSchemaAuthority, dylib: u8) -> ConfigurationPipelinePublication {
    use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
    ConfigurationPipelinePublication::Epoch {
        epoch: ValidatedPipelineEpoch::validate(distill_store::state::PipelineEpoch {
            dylib_hash: [dylib; 32],
            target_set: CanonicalTargetSet::canonical(vec![TargetSetRow {
                name: "dev".into(),
                target_definition_hash: distill_build::keys::target_definition_hash(&build_target(false)),
            }])
            .unwrap(),
            schema_registry: authority.logical_registry().unwrap(),
        })
        .unwrap(),
        tools: BTreeMap::new(),
    }
}

struct Configuration {
    authority: Arc<ProjectSchemaAuthority>,
    projection: PipelineProjection,
    pipeline: ConfigurationPipelinePublication,
}

/// Publish the scan of `scanner` completely under `configuration`, as a
/// root replacement does, with the tag epochs its refinement records.
fn publish_completely(
    store: &mut Store,
    scanner: &RootedScanner,
    configuration: &Configuration,
    retyped: &BTreeSet<TypeUuid>,
) {
    let authority = &*configuration.authority;
    let candidate = ScanCandidate::build(scanner.scan().unwrap(), Some(authority)).unwrap();
    let claims = bundle_claims(candidate.scan.bundle_rows(), &configuration.projection, Some(authority)).unwrap();
    let base = store.input_version().unwrap();
    publish_scan(
        store,
        base,
        candidate,
        true,
        Some(&configuration.pipeline),
        &configuration.projection,
        retyped,
        Some(authority),
        &claims,
    )
    .unwrap();
    store
        .replace_tag_epochs(&crate::build::type_tag_epochs(authority))
        .unwrap();
}

/// The tables a configuration publication writes, as rows.
fn published_tables(store: &Store) -> BTreeMap<&'static str, Vec<String>> {
    [
        "files",
        "directories",
        "source_claims",
        "claim_collisions",
        "claim_pending",
        "file_work",
        "bundles",
        "bundle_path_refs",
        "assets",
        "asset_tags",
        "tag_epochs",
        "path_index",
        "derived_outputs",
        "errors",
    ]
    .into_iter()
    .map(|table| (table, store.table_rows(table).unwrap()))
    .chain([
        (
            "configuration_generation",
            vec![store.configuration_generation().unwrap().to_string()],
        ),
        (
            "pipeline_module_hash",
            vec![format!("{:?}", store.pipeline_module_hash().unwrap())],
        ),
    ])
    .collect()
}

/// Publish `before` completely into two stores, then `after` into one
/// completely (the oracle: every source parsed and claimed again) and into
/// the other as a candidate with unchanged roots does. Both must hold the
/// same rows, and the second must have refined at least the tag rows the
/// oracle would. Returns the sources the incremental publication claimed.
fn assert_reconfiguration_matches_oracle(before: &Configuration, after: &Configuration) -> usize {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let authority = typed_authority(false);
    for index in 0..3 {
        std::fs::write(assets.join(format!("bytes{index}.bundle")), typed_bundle(&authority, BYTES_TYPE, index))
            .unwrap();
    }
    for index in 0..2 {
        std::fs::write(assets.join(format!("edited{index}.bundle")), typed_bundle(&authority, EDITED_TYPE, index))
            .unwrap();
    }
    // A malformed bundle whose skeleton validates only under an authority
    // knowing its type.
    let malformed = typed_bundle(&authority, EDITED_TYPE, 7);
    let mut value = distill_json::parse(std::str::from_utf8(&malformed).unwrap()).unwrap();
    let AuthoredValue::Object(envelope) = &mut value else {
        panic!("a bundle envelope is an object");
    };
    envelope.insert("future-extension".to_owned(), AuthoredValue::UInt(1));
    std::fs::write(assets.join("malformed.bundle"), distill_json::write(&value).unwrap()).unwrap();
    // Two bundles claiming one asset.
    let mut shared = distill_bundle::parse_bundle(&typed_bundle(&authority, BYTES_TYPE, 8)).unwrap();
    std::fs::write(assets.join("shared-a.bundle"), distill_bundle::write_bundle(&shared).unwrap()).unwrap();
    shared.uuid = BundleUuid([0x99; 16]);
    std::fs::write(assets.join("shared-b.bundle"), distill_bundle::write_bundle(&shared).unwrap()).unwrap();

    let scanner = RootedScanner::new([AssetRoot::new("main", &assets)]).unwrap();
    let mut oracle = Store::open(StoreConfig::new(temp.path().join("oracle"))).unwrap();
    let mut store = Store::open(StoreConfig::new(temp.path().join("store"))).unwrap();
    let retyped = after.projection.retyped(&before.projection);
    publish_completely(&mut oracle, &scanner, before, &BTreeSet::new());
    publish_completely(&mut store, &scanner, before, &BTreeSet::new());
    assert_eq!(published_tables(&oracle), published_tables(&store));

    publish_completely(&mut oracle, &scanner, after, &retyped);
    BUNDLE_READS.with(|reads| reads.set(0));
    publish_reconfiguration(
        &mut store,
        &scanner,
        &after.pipeline,
        &after.projection,
        &retyped,
        &after.authority,
    )
    .unwrap();
    store
        .replace_tag_epochs(&crate::build::type_tag_epochs(&after.authority))
        .unwrap();
    let (oracle_tables, tables) = (published_tables(&oracle), published_tables(&store));
    for (table, rows) in &oracle_tables {
        assert_eq!(&tables[table], rows, "{table} differs from the oracle's");
    }
    let refined = store.stale_tag_index_assets(None).unwrap();
    for asset in oracle.stale_tag_index_assets(None).unwrap().keys() {
        assert!(refined.contains_key(asset), "{asset} is pending in the oracle only");
    }
    BUNDLE_READS.with(Cell::get)
}

thread_local! {
    static BUNDLE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
use std::cell::Cell;

/// Called once per source a configuration publication reads and claims
/// again.
pub(super) fn source_reclaimed() {
    BUNDLE_READS.with(|reads| reads.set(reads.get() + 1));
}

/// A candidate with unchanged roots publishes exactly what a complete
/// publication of the same configuration would, for each kind of change,
/// reading only the sources the change reaches.
#[test]
fn a_reconfiguration_publishes_what_a_complete_publication_does() {
    let configuration = |authority: Arc<ProjectSchemaAuthority>, terminal, dylib| Configuration {
        projection: bytes_projection(terminal),
        pipeline: pipeline_publication(&authority, dylib),
        authority,
    };
    const TERMINAL: TypeUuid = TypeUuid([0x63; 16]);
    // No change: nothing is read.
    let reads = assert_reconfiguration_matches_oracle(
        &configuration(typed_authority(false), None, 1),
        &configuration(typed_authority(false), None, 1),
    );
    assert_eq!(reads, 0, "an unchanged configuration read a bundle");
    // A module change: its migrated tag rows are the refinement's.
    let reads = assert_reconfiguration_matches_oracle(
        &configuration(typed_authority(false), None, 1),
        &configuration(typed_authority(false), None, 2),
    );
    assert_eq!(reads, 0, "a module change read a bundle");
    // A projection change: the retyped type's three bundles and the two
    // colliding sources (the malformed bundle's skeleton is valid here,
    // its asset the edited type's).
    let reads = assert_reconfiguration_matches_oracle(
        &configuration(typed_authority(false), None, 1),
        &configuration(typed_authority(false), Some(TERMINAL), 1),
    );
    assert_eq!(reads, 5);
    // A schema change that gives the malformed bundle a skeleton: it, the
    // colliding sources; the edited type's healthy bundles keep their rows.
    let reads = assert_reconfiguration_matches_oracle(
        &configuration(bytes_only_authority(), None, 1),
        &configuration(typed_authority(true), None, 1),
    );
    assert_eq!(reads, 3);
    // A schema edit of one type: its one poisoned bundle and the colliding
    // sources; its healthy bundles keep their rows.
    let reads = assert_reconfiguration_matches_oracle(
        &configuration(typed_authority(false), None, 1),
        &configuration(typed_authority(true), None, 1),
    );
    assert_eq!(reads, 3);
}

const LABEL_TYPE: TypeUuid = TypeUuid([0x64; 16]);

/// An authority over one asset type, a struct of one string `category`,
/// which is a search tag when `tagged` (an edit its logical hash does not
/// see, but its tag epoch does).
fn labeled_authority(tagged: bool) -> Arc<ProjectSchemaAuthority> {
    use distill_schema::ngp_schema::{
        Field, FieldAttrs, FieldIdentifier, FieldLayout, PrimitiveType, SchemaTypeId, TypeAttrs,
        TypeDef, TypeLayout, TypePath,
    };
    let path = |krate: &str, name: &str| TypePath {
        name: Some(name.to_owned()),
        containing_type: None,
        modules: Vec::new(),
        krate: krate.to_owned(),
    };
    let def = |id: usize, kind, path, uuid, fields| TypeDef {
        id: SchemaTypeId(id),
        kind,
        path,
        uuid,
        attrs: TypeAttrs::default(),
        fields,
        generic_parameters: Vec::new(),
        generic_argument_ids: Vec::new(),
        generic_const_arguments: Vec::new(),
        has_default: id == 1,
        has_explicit_discriminants: false,
    };
    let string = std::mem::size_of::<String>() as u64;
    let layout = |fields| TypeLayout {
        size: Some(string),
        align: Some(std::mem::align_of::<String>() as u64),
        layout_complete: true,
        tag_encoding: None,
        fields,
    };
    Arc::new(
        ProjectSchemaAuthority::from_schema(
            Schema {
                source_hashes: BTreeMap::new(),
                type_ops_hash: String::new(),
                layout_hashes: Default::default(),
                rustc_version: String::new(),
                types: vec![
                    def(
                        0,
                        PrimitiveType::Struct,
                        path("game", "Labeled"),
                        Some(LABEL_TYPE),
                        vec![Field {
                            id: FieldIdentifier::Name("category".to_owned()),
                            type_id: SchemaTypeId(1),
                            attrs: FieldAttrs {
                                tag: tagged,
                                ..FieldAttrs::default()
                            },
                        }],
                    ),
                    def(1, PrimitiveType::String, path("alloc", "String"), None, Vec::new()),
                ],
                layouts: vec![SchemaLayouts {
                    identity: layout_identity(),
                    layouts: vec![
                        layout(vec![FieldLayout {
                            offset: Some(0),
                            field_size: Some(string),
                        }]),
                        layout(Vec::new()),
                    ],
                }],
            },
            [u8::from(tagged) + 30; 32],
        )
        .unwrap(),
    )
}

/// After a schema edit that makes a field a search tag, a complete
/// publication (a root replacement) validates again the skeleton of a
/// poisoned bundle whose bytes, and so its summary and asset set, are
/// unchanged, as a reconfiguration does: the skeleton carries the new tag,
/// and a query for that tag fails naming the bundle instead of answering
/// nothing.
#[test]
fn a_complete_publication_revalidates_a_poisoned_skeleton_after_a_schema_edit() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let authority = labeled_authority(false);
    let project = authority.project_type(LABEL_TYPE).unwrap();
    let bundle = BundleUuid([0x64; 16]);
    let malformed = distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: bundle,
        primary: Some("entry".into()),
        schemas: BTreeMap::from([(project.logical_hash, project.logical_schema.clone())]),
        assets: BTreeMap::from([(
            "entry".into(),
            AssetEntry {
                uuid: AssetUuid([0x64; 16]),
                type_uuid: LABEL_TYPE,
                schema_hash: project.logical_hash,
                authoring_only: false,
                data: AuthoredValue::Object(BTreeMap::from([(
                    "category".to_owned(),
                    AuthoredValue::Str("enemy".to_owned()),
                )])),
            },
        )]),
    })
    .unwrap();
    let mut value = distill_json::parse(std::str::from_utf8(&malformed).unwrap()).unwrap();
    let AuthoredValue::Object(envelope) = &mut value else {
        panic!("a bundle envelope is an object");
    };
    envelope.insert("future-extension".to_owned(), AuthoredValue::UInt(1));
    std::fs::write(assets.join("malformed.bundle"), distill_json::write(&value).unwrap()).unwrap();

    let scanner = RootedScanner::new([AssetRoot::new("main", &assets)]).unwrap();
    let configuration = |tagged| {
        let authority = labeled_authority(tagged);
        Configuration {
            projection: PipelineProjection::build(
                Vec::new(),
                [9; 32],
                &BTreeMap::from([("dev".to_owned(), build_target(false))]),
                [LABEL_TYPE],
            )
            .unwrap(),
            pipeline: pipeline_publication(&authority, 1),
            authority,
        }
    };
    let (before, after) = (configuration(false), configuration(true));
    let query = |store: &Store| {
        let filter = distill_store::bundles::AssetFilter {
            tag: Some(("category".to_owned(), Some("enemy".to_owned()))),
            ..Default::default()
        };
        store
            .namespace_assets_matching(&filter, |_| true)
            .unwrap()
            .map(|rows| rows.len())
    };
    let mut complete = Store::open(StoreConfig::new(temp.path().join("complete"))).unwrap();
    let mut reconfigured = Store::open(StoreConfig::new(temp.path().join("reconfigured"))).unwrap();
    for store in [&mut complete, &mut reconfigured] {
        publish_completely(store, &scanner, &before, &BTreeSet::new());
        assert_eq!(query(store), Ok(0), "untagged, the skeleton answers no tag query");
    }

    publish_completely(&mut complete, &scanner, &after, &BTreeSet::new());
    publish_reconfiguration(
        &mut reconfigured,
        &scanner,
        &after.pipeline,
        &after.projection,
        &BTreeSet::new(),
        &after.authority,
    )
    .unwrap();
    reconfigured
        .replace_tag_epochs(&crate::build::type_tag_epochs(&after.authority))
        .unwrap();
    assert_eq!(query(&reconfigured), Err(vec![bundle]));
    assert_eq!(query(&complete), Err(vec![bundle]), "the complete publication kept the old skeleton");
    let (complete, reconfigured) = (published_tables(&complete), published_tables(&reconfigured));
    for table in ["bundles", "assets", "asset_tags"] {
        assert_eq!(complete[table], reconfigured[table], "{table}");
    }
}
