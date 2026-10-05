//! A project on disk served by a real [`DaemonCoordinator`]: tests author
//! bundle files, publish them through the daemon's own scan, and talk to
//! the daemon's RPC server. Nothing here writes store rows.
//!
//! - [`TestProject`]: the project's roots, its daemon and the daemon's
//!   writer. Files written through it are published by
//!   [`TestProject::publish`], the watcher batch for them.
//! - [`bundle_bytes`] / [`Asset`]: an authored bundle file.
//! - [`TestBuilds`]: a build backend that builds a canonical artifact for
//!   each request, or answers what a test set for an asset.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use distill_bundle::{AssetEntry, Bundle, BUNDLE_FORMAT_VERSION};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};
use distill_daemon::config::DaemonConfig;
use distill_daemon::coordinator::DaemonCoordinator;
use distill_daemon::scanner::AssetRoot;
use distill_daemon::watcher::WatcherBatch;
use distill_json::AuthoredValue;
use distill_rpc::{
    ArtifactPayload, BuildAnswer, BuildBackend, BuildCompletion, BuildRequest, BuildStart,
    BuildTicket, BuildView, RpcFailure, Server, ServerHandle, SnapshotStamp, TargetDefinition,
};
use distill_schema::ngp_schema::{
    node_hash, Field, FieldAttrs, FieldIdentifier, FieldLayout, LayoutIdentity, LogicalSchema,
    PrimitiveType, Schema, SchemaLayouts, SchemaNode, SchemaTypeId, StaticArray, TypeAttrs,
    TypeDef, TypeLayout, TypePath,
};
use distill_schema::ProjectSchemaAuthority;
use distill_store::{StoreConfig, StoreWriter};

/// The root [`TestProject::write`] writes under.
pub const ROOT: &str = "main";

/// One authored asset of a bundle.
#[derive(Debug, Clone)]
pub struct Asset {
    pub local_id: String,
    pub uuid: AssetUuid,
    pub type_uuid: TypeUuid,
    pub schema: SchemaNode,
    pub value: AuthoredValue,
    pub authoring_only: bool,
}

impl Asset {
    /// A runtime asset holding one blob.
    pub fn blob(local_id: &str, uuid: AssetUuid, type_uuid: TypeUuid, bytes: &[u8]) -> Self {
        Asset {
            local_id: local_id.to_owned(),
            uuid,
            type_uuid,
            schema: SchemaNode::Blob,
            value: AuthoredValue::Blob(bytes.to_vec()),
            authoring_only: false,
        }
    }

    /// A runtime asset holding one `u8`.
    pub fn uint(local_id: &str, uuid: AssetUuid, type_uuid: TypeUuid, value: u8) -> Self {
        Asset {
            local_id: local_id.to_owned(),
            uuid,
            type_uuid,
            schema: SchemaNode::Primitive(distill_schema::ngp_schema::PrimitiveKind::U8),
            value: AuthoredValue::UInt(value.into()),
            authoring_only: false,
        }
    }

    /// This asset as authoring-only.
    pub fn authoring_only(mut self) -> Self {
        self.authoring_only = true;
        self
    }
}

/// The bytes of a bundle `uuid` holding `assets`; `primary` names the
/// asset its path resolves to.
pub fn bundle_bytes(uuid: BundleUuid, primary: Option<&str>, assets: &[Asset]) -> Vec<u8> {
    let mut schemas = BTreeMap::new();
    let mut entries = BTreeMap::new();
    for asset in assets {
        let schema_hash = node_hash(&asset.schema).unwrap();
        schemas.insert(
            schema_hash,
            LogicalSchema {
                root: asset.schema.clone(),
            },
        );
        entries.insert(
            asset.local_id.clone(),
            AssetEntry {
                uuid: asset.uuid,
                type_uuid: asset.type_uuid,
                schema_hash,
                authoring_only: asset.authoring_only,
                data: asset.value.clone(),
            },
        );
    }
    distill_bundle::write_bundle(&Bundle {
        format_version: BUNDLE_FORMAT_VERSION,
        uuid,
        primary: primary.map(str::to_owned),
        schemas,
        assets: entries,
    })
    .unwrap()
}

/// A project directory with asset roots, served by a daemon coordinator
/// over its own store.
pub struct TestProject {
    coordinator: DaemonCoordinator,
    writer: StoreWriter,
    targets: Vec<TargetDefinition>,
    roots: Vec<String>,
    /// The configuration's schema authority, once [`TestProject::configured`].
    authority: Option<Arc<ProjectSchemaAuthority>>,
    /// Files written or removed since the last publication.
    touched: BTreeSet<PathBuf>,
    // Dropped last: the daemon holds files under it.
    dir: tempfile::TempDir,
}

impl TestProject {
    /// An empty project with the one root [`ROOT`], serving `targets`.
    pub fn new(targets: Vec<TargetDefinition>) -> Self {
        Self::with_roots(targets, &[ROOT])
    }

    /// An empty project with the roots `roots`, serving `targets`.
    pub fn with_roots(targets: Vec<TargetDefinition>, roots: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let roots = roots
            .iter()
            .map(|root| (*root).to_owned())
            .collect::<Vec<_>>();
        for root in &roots {
            std::fs::create_dir_all(dir.path().join(root)).unwrap();
        }
        Self::open(dir, targets, roots)
    }

    fn open(dir: tempfile::TempDir, targets: Vec<TargetDefinition>, roots: Vec<String>) -> Self {
        let coordinator = DaemonCoordinator::open(
            StoreConfig::new(dir.path().join(".distill")),
            roots
                .iter()
                .map(|root| AssetRoot::new(root, dir.path().join(root)))
                .collect(),
            targets.clone(),
            64,
        )
        .unwrap();
        let writer = coordinator.open_writer().unwrap();
        TestProject {
            coordinator,
            writer,
            targets,
            roots,
            authority: None,
            touched: BTreeSet::new(),
            dir,
        }
    }

    /// An empty project with the one root [`ROOT`] and a configuration:
    /// the target "dev" (`optimize` as given), the project schema
    /// [`project_schema`] and the pipeline module `distill-pipeline-fixture`,
    /// published as the daemon's process loop publishes a configuration
    /// it observes. Its pipeline is Ready.
    pub fn configured(optimize: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(ROOT);
        std::fs::create_dir_all(&root).unwrap();
        let config = write_configuration(dir.path(), &root, optimize);
        let targets = config
            .target_definitions(configuration_authority(&config).identity())
            .unwrap();
        let mut project = Self::open(dir, targets, vec![ROOT.to_owned()]);
        project.publish_configuration(&config);
        project
    }

    /// Edit the configuration so its target "dev" optimizes or not, and
    /// publish it as the process loop does: one version whose target
    /// definition changed, fencing every hub bound to the old one.
    pub fn reconfigure(&mut self, optimize: bool) -> SnapshotStamp {
        assert!(self.authority.is_some(), "the project is not configured");
        let config = write_configuration(self.dir.path(), &self.root(ROOT), optimize);
        self.publish_configuration(&config)
    }

    fn publish_configuration(&mut self, config: &DaemonConfig) -> SnapshotStamp {
        let (stamp, authority) = publish_configuration(&self.coordinator, &mut self.writer, config);
        self.targets = config.target_definitions(authority.identity()).unwrap();
        self.authority = Some(authority);
        stamp
    }

    /// The target the daemon serves (the first, for a project with more).
    pub fn target(&self) -> &TargetDefinition {
        &self.targets[0]
    }

    /// A runtime asset of the configured project type `type_uuid` holding
    /// `value`, authored under the type's current schema.
    pub fn asset(
        &self,
        local_id: &str,
        uuid: AssetUuid,
        type_uuid: TypeUuid,
        value: AuthoredValue,
    ) -> Asset {
        let authority = self.authority.as_ref().expect("the project is configured");
        Asset {
            local_id: local_id.to_owned(),
            uuid,
            type_uuid,
            schema: authority
                .project_type(type_uuid)
                .expect("a project type")
                .logical_schema
                .root
                .clone(),
            value,
            authoring_only: false,
        }
    }

    /// The same project after a daemon restart: its files and its store,
    /// served by a new coordinator, which publishes its configuration
    /// again as the process loop's startup does.
    pub fn restart(self) -> Self {
        let TestProject {
            coordinator,
            writer,
            targets,
            roots,
            authority,
            dir,
            ..
        } = self;
        drop(writer);
        drop(coordinator);
        let mut project = Self::open(dir, targets, roots);
        if authority.is_some() {
            let config = DaemonConfig::load(project.path().join("distill.toml")).unwrap();
            project.publish_configuration(&config);
        }
        project
    }

    /// The project's directory.
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// The directory of the root `root`.
    pub fn root(&self, root: &str) -> PathBuf {
        assert!(self.roots.iter().any(|name| name == root), "no root {root}");
        self.dir.path().join(root)
    }

    /// The daemon's RPC server.
    pub fn server(&self) -> Server {
        self.coordinator.server()
    }

    pub fn coordinator(&self) -> &DaemonCoordinator {
        &self.coordinator
    }

    /// The daemon's writer, for the coordinator's own publications.
    pub fn writer(&mut self) -> &mut StoreWriter {
        &mut self.writer
    }

    /// Write `bytes` at `path` under [`ROOT`].
    pub fn write(&mut self, path: &str, bytes: impl AsRef<[u8]>) {
        self.write_in(ROOT, path, bytes);
    }

    /// Write `bytes` at `path` under `root`; the next
    /// [`TestProject::publish`] publishes it.
    pub fn write_in(&mut self, root: &str, path: &str, bytes: impl AsRef<[u8]>) {
        let file = self.root(root).join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, bytes).unwrap();
        self.touched.insert(file);
    }

    /// Write a bundle file at `path` under [`ROOT`] (see [`bundle_bytes`]).
    pub fn write_bundle(
        &mut self,
        path: &str,
        uuid: BundleUuid,
        primary: Option<&str>,
        assets: &[Asset],
    ) {
        self.write(path, bundle_bytes(uuid, primary, assets));
    }

    /// Remove the file at `path` under [`ROOT`].
    pub fn remove(&mut self, path: &str) {
        self.remove_in(ROOT, path);
    }

    /// Remove the file at `path` under `root`; the next
    /// [`TestProject::publish`] publishes it.
    pub fn remove_in(&mut self, root: &str, path: &str) {
        let file = self.root(root).join(path);
        std::fs::remove_file(&file).unwrap();
        self.touched.insert(file);
    }

    /// Publish the files changed since the last publication, as the
    /// watcher's batch for them: one input version when they change what
    /// the daemon serves. Returns the current stamp either way.
    pub fn publish(&mut self) -> SnapshotStamp {
        // The watcher reports events under the canonicalized directory it
        // watches, as a configuration spells its roots (on Windows a `\\?\`
        // verbatim path, which a plain path under the same directory does
        // not match).
        let dir = std::fs::canonicalize(self.dir.path()).unwrap();
        let paths = std::mem::take(&mut self.touched)
            .into_iter()
            .map(|path| dir.join(path.strip_prefix(self.dir.path()).unwrap()))
            .collect();
        self.coordinator
            .reconcile_incremental(
                &mut self.writer,
                &WatcherBatch {
                    paths,
                    renames: Vec::new(),
                },
            )
            .unwrap()
    }
}

/// A build backend for a [`TestProject`]'s server. Each request is
/// recorded with its requester's stamp. An asset given an answer
/// ([`TestBuilds::answer`]) gets it; any other build publishes a canonical
/// artifact for the requested asset (a unit layout, no data), as the
/// daemon's build workers publish theirs, and answers it built.
pub struct TestBuilds {
    server: Weak<ServerHandle>,
    answers: Mutex<BTreeMap<AssetUuid, Result<BuildAnswer, RpcFailure>>>,
    requests: Mutex<Vec<(BuildRequest, SnapshotStamp)>>,
}

struct Finished(Result<BuildAnswer, RpcFailure>);

impl BuildCompletion for Finished {
    fn answer(self: Box<Self>, _view: BuildView<'_>) -> Result<BuildAnswer, RpcFailure> {
        self.0
    }
}

impl TestBuilds {
    /// A backend that publishes its builds on `server`; not installed.
    pub fn new(server: &Server) -> Arc<Self> {
        Arc::new(TestBuilds {
            server: Arc::downgrade(&server.handle()),
            answers: Mutex::new(BTreeMap::new()),
            requests: Mutex::new(Vec::new()),
        })
    }

    /// A backend installed on `server`.
    pub fn install(server: &Server) -> Arc<Self> {
        let builds = Self::new(server);
        server.install_build_backend(builds.clone());
        builds
    }

    /// Answer every build of `asset` with `answer`.
    pub fn answer(&self, asset: AssetUuid, answer: Result<BuildAnswer, RpcFailure>) {
        self.answers.lock().unwrap().insert(asset, answer);
    }

    /// The requests so far, with their requesters' stamps.
    pub fn requests(&self) -> Vec<(BuildRequest, SnapshotStamp)> {
        self.requests.lock().unwrap().clone()
    }

    fn build(&self, request: &BuildRequest) -> Result<BuildAnswer, RpcFailure> {
        let wire = distill_wire::wire::WireNode::Unit { offset: 0 };
        let wire_bytes: Arc<[u8]> = Arc::from(distill_wire::dswl::dswl_bytes(&wire).unwrap());
        let layout_hash = distill_wire::dswl::dswl_hash(&wire).unwrap();
        let authored_type = if request.output_key.is_empty() {
            request.entry.type_uuid
        } else {
            request.requested_terminal_type
        };
        let (content_hash, payload) = canonical_artifact(
            &distill_wire::artifact::ArtifactHeader {
                asset_uuid: request.requested_asset,
                authored_type,
                terminal_type: request.requested_terminal_type,
                encoded_type: request.requested_terminal_type,
                logical_hash: LogicalHash([77; 32]),
                layout_hash,
            },
            &[],
            &[],
        );
        let server = self
            .server
            .upgrade()
            .expect("the server outlives its backend");
        put_wire_tree(&server, &wire_bytes);
        assert_eq!(put_artifact(&server, &payload), content_hash);
        Ok(BuildAnswer::Built { content_hash })
    }
}

impl BuildBackend for TestBuilds {
    fn start(&self, view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        self.requests
            .lock()
            .unwrap()
            .push((request.clone(), view.stamp));
        let fixed = self
            .answers
            .lock()
            .unwrap()
            .get(&request.requested_asset)
            .cloned();
        let answer = fixed.unwrap_or_else(|| self.build(request));
        BuildStart::Submitted(BuildTicket::new(async move {
            Box::new(Finished(answer)) as Box<dyn BuildCompletion>
        }))
    }
}

/// A canonical artifact with `header`, `fixed` structural bytes and
/// `blobs`, as the server installs it: its content hash and its payload
/// (structural part, blobs, no load edges).
pub fn canonical_artifact(
    header: &distill_wire::artifact::ArtifactHeader,
    fixed: &[u8],
    blobs: &[Arc<[u8]>],
) -> (ContentHash, ArtifactPayload) {
    let blob_inputs = blobs
        .iter()
        .map(|blob| (Vec::new(), blob.as_ref()))
        .collect::<Vec<_>>();
    let complete =
        distill_wire::artifact::write_artifact(header, &[], fixed, &[], &blob_inputs).unwrap();
    let parsed = distill_wire::artifact::parse_artifact(&complete).unwrap();
    let structural_len = complete.len() - parsed.blob_section.len();
    (
        distill_wire::artifact::content_hash(&complete),
        ArtifactPayload {
            structural: Arc::from(complete[..structural_len].to_vec()),
            blobs: blobs.to_vec(),
            load_edges: Vec::new(),
        },
    )
}

/// A project type of the configured project: a struct of one `String`
/// field `group`, a search tag.
pub const TAGGED_TYPE: TypeUuid = TypeUuid([0xa4; 16]);
pub use distill_pipeline_fixture::{
    COOKED_TYPE, FLOAT_SETTINGS_TYPE, PARENT_TYPE, REFLECT, REFLECTION, REFLECTION_TYPE,
    SETTINGS_TYPE, VALUE_TYPE,
};

/// The configured project's schema: [`TAGGED_TYPE`], and the pipeline
/// module's [`PARENT_TYPE`], [`COOKED_TYPE`], [`REFLECTION_TYPE`] and
/// [`VALUE_TYPE`] (each of these a struct of one `u8` field `value`) and
/// [`SETTINGS_TYPE`] (`{ add: [u8; 2], scale: { by: u8 } }`) and
/// [`FLOAT_SETTINGS_TYPE`] (`{ rate: f32 }`), laid out for
/// this host. Its source hashes are the pipeline module's.
pub fn project_schema() -> Schema {
    let type_def = |id: usize, kind, krate: &str, name: &str, uuid, fields| TypeDef {
        id: SchemaTypeId(id),
        kind,
        path: TypePath {
            name: Some(name.to_owned()),
            containing_type: None,
            modules: Vec::new(),
            krate: krate.to_owned(),
        },
        uuid,
        attrs: TypeAttrs::default(),
        fields,
        generic_parameters: Vec::new(),
        generic_argument_ids: Vec::new(),
        has_default: true,
        generic_const_arguments: Vec::new(),
        has_explicit_discriminants: false,
    };
    let field = |name: &str, type_id: usize, tag: bool| Field {
        id: FieldIdentifier::Name(name.to_owned()),
        type_id: SchemaTypeId(type_id),
        attrs: FieldAttrs {
            tag,
            ..FieldAttrs::default()
        },
    };
    let layout = |size: usize, align: usize, fields: Vec<FieldLayout>| TypeLayout {
        size: Some(size as u64),
        align: Some(align as u64),
        layout_complete: true,
        tag_encoding: None,
        fields,
    };
    let at_zero = |size: usize| FieldLayout {
        offset: Some(0),
        field_size: Some(size as u64),
    };
    let string = std::mem::size_of::<String>();
    let string_align = std::mem::align_of::<String>();
    let mut types = vec![
        type_def(0, PrimitiveType::U8, "core", "u8", None, Vec::new()),
        type_def(
            1,
            PrimitiveType::String,
            "alloc",
            "String",
            None,
            Vec::new(),
        ),
        type_def(
            2,
            PrimitiveType::Struct,
            "fixture",
            "Tagged",
            Some(TAGGED_TYPE),
            vec![field("group", 1, true)],
        ),
        type_def(
            3,
            PrimitiveType::Struct,
            "fixture",
            "Settings",
            Some(SETTINGS_TYPE),
            vec![field("add", 4, false), field("scale", 5, false)],
        ),
        TypeDef {
            generic_argument_ids: vec![SchemaTypeId(0)],
            ..type_def(
                4,
                PrimitiveType::StaticArray(StaticArray { length: 2 }),
                "core",
                "array",
                None,
                Vec::new(),
            )
        },
        type_def(
            5,
            PrimitiveType::Struct,
            "fixture",
            "Scale",
            None,
            vec![field("by", 0, false)],
        ),
    ];
    let mut layouts = vec![
        layout(1, 1, Vec::new()),
        layout(string, string_align, Vec::new()),
        layout(string, string_align, vec![at_zero(string)]),
        layout(
            3,
            1,
            vec![
                at_zero(2),
                FieldLayout {
                    offset: Some(2),
                    field_size: Some(1),
                },
            ],
        ),
        layout(2, 1, Vec::new()),
        layout(1, 1, vec![at_zero(1)]),
    ];
    let f32_id = types.len();
    types.push(type_def(f32_id, PrimitiveType::F32, "core", "f32", None, Vec::new()));
    layouts.push(layout(4, 4, Vec::new()));
    types.push(type_def(
        types.len(),
        PrimitiveType::Struct,
        "fixture",
        "FloatSettings",
        Some(FLOAT_SETTINGS_TYPE),
        vec![field("rate", f32_id, false)],
    ));
    layouts.push(layout(4, 4, vec![at_zero(4)]));
    for (name, uuid) in [
        ("Parent", PARENT_TYPE),
        ("Cooked", COOKED_TYPE),
        ("Reflection", REFLECTION_TYPE),
        ("Value", VALUE_TYPE),
    ] {
        types.push(type_def(
            types.len(),
            PrimitiveType::Struct,
            "fixture",
            name,
            Some(uuid),
            vec![field("value", 0, false)],
        ));
        layouts.push(layout(1, 1, vec![at_zero(1)]));
    }
    Schema {
        source_hashes: BTreeMap::from([pipeline_module().1.clone()]),
        type_ops_hash: String::new(),
        layout_hashes: Default::default(),
        rustc_version: String::new(),
        types,
        layouts: vec![SchemaLayouts {
            identity: LayoutIdentity {
                target_triple: host_triple(),
                rustc: "rustc test-project".to_owned(),
                algorithm_version: 1,
            },
            layouts,
        }],
    }
}

fn host_triple() -> String {
    let arch = std::env::consts::ARCH;
    match std::env::consts::OS {
        "macos" => format!("{arch}-apple-darwin"),
        "linux" => format!("{arch}-unknown-linux-gnu"),
        "windows" => format!("{arch}-pc-windows-msvc"),
        other => panic!("unsupported test OS {other}"),
    }
}

/// Write the configuration of the project at `dir` whose root [`ROOT`] is
/// `root`: `dir/distill.toml` (state under `dir/.distill`, the target
/// "dev", `optimize` as given, the pipeline module
/// `distill-pipeline-fixture`) and `dir/schema.json` ([`project_schema`]).
/// Returns it loaded.
pub fn write_configuration(dir: &Path, root: &Path, optimize: bool) -> DaemonConfig {
    std::fs::write(
        dir.join("schema.json"),
        serde_json::to_vec(&project_schema()).unwrap(),
    )
    .unwrap();
    let toml_path = |path: PathBuf| path.display().to_string().replace('\\', "/");
    let config = format!(
        r#"
[daemon]
address = "127.0.0.1:0"
state_path = '{state}'
[assets]
roots = {{ {ROOT} = '{root}' }}
schema_path = '{schema}'
[modules]
pipeline_dylib = '{module}'
[targets.dev]
os = "{os}"
arch = "{arch}"
apis = ["vulkan"]
optimize = {optimize}
[codegen]
rs_mod_path = '{generated}'
auto_codegen = false
[pipeline]
parallelism = 2
max_dependency_depth = 64
batch_reserved_workers = 1
[cas]
segment_size = "1MiB"
cache_limit = "16MiB"
"#,
        state = toml_path(dir.join(".distill")),
        root = toml_path(root.to_owned()),
        schema = toml_path(dir.join("schema.json")),
        module = toml_path(pipeline_module().0.clone()),
        os = std::env::consts::OS,
        arch = std::env::consts::ARCH,
        generated = toml_path(dir.join("generated")),
    );
    let path = dir.join("distill.toml");
    std::fs::write(&path, config).unwrap();
    DaemonConfig::load(path).unwrap()
}

/// Publish `config` (see [`write_configuration`]) on `coordinator`'s
/// store, as the daemon's process loop publishes a configuration it
/// observes; returns the version and the configuration's schema
/// authority. Its pipeline is Ready.
pub fn publish_configuration(
    coordinator: &DaemonCoordinator,
    writer: &mut StoreWriter,
    config: &DaemonConfig,
) -> (SnapshotStamp, Arc<ProjectSchemaAuthority>) {
    let authority = configuration_authority(config);
    let candidate = config.configuration_candidate(&authority).unwrap();
    let stamp = coordinator
        .publish_configuration_candidate(writer, candidate)
        .unwrap();
    (stamp, authority)
}

/// The schema authority of `config`'s schema file, as the process loop
/// reads it.
fn configuration_authority(config: &DaemonConfig) -> Arc<ProjectSchemaAuthority> {
    let bytes = std::fs::read(&config.assets.schema_path).unwrap();
    Arc::new(ProjectSchemaAuthority::from_json(&bytes).unwrap())
}

/// The pipeline module `distill-pipeline-fixture`, built once per test
/// process, and its source identity (crate name, source hash).
fn pipeline_module() -> &'static (PathBuf, (String, String)) {
    static MODULE: OnceLock<(PathBuf, (String, String))> = OnceLock::new();
    MODULE.get_or_init(|| {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(5)
            .unwrap();
        let target_dir = workspace.join("target/pipeline-module-fixture");
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let output = std::process::Command::new(cargo)
            .current_dir(workspace)
            .env("CARGO_TARGET_DIR", &target_dir)
            .args(["build", "--offline", "-p", "distill-pipeline-fixture"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "pipeline fixture build failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let module = target_dir
            .join("debug")
            .join(if cfg!(target_os = "windows") {
                "distill_pipeline_fixture.dll"
            } else if cfg!(target_os = "macos") {
                "libdistill_pipeline_fixture.dylib"
            } else {
                "libdistill_pipeline_fixture.so"
            });
        let staging = tempfile::tempdir().unwrap();
        let staged = ngp_module_host::stage_copy_to(
            &module,
            &staging.path().join(module.file_name().unwrap()),
        )
        .unwrap();
        // SAFETY: the fixture was just built from this workspace and is
        // opened only to read its bounded source-identity export.
        let library = unsafe { ngp_module_host::HostedLibrary::open(staged) }.unwrap();
        // SAFETY: the fixture derives the shared source-identity export.
        let identity = unsafe { ngp_module_host::read_source_identity(&library) }.unwrap();
        library.close();
        (module, (identity.crate_name, identity.source_hash))
    })
}

/// Put the artifact `payload` (with its load edges) into `server`'s store,
/// as the daemon's build workers put theirs; returns its content hash.
pub fn put_artifact(server: &ServerHandle, payload: &ArtifactPayload) -> ContentHash {
    let blobs = payload
        .blobs
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<&[u8]>>();
    let bytes = distill_wire::artifact::assemble_artifact(&payload.structural, &blobs);
    let edges = payload
        .load_edges
        .iter()
        .map(|edge| (edge.asset, edge.expected_terminal))
        .collect::<Vec<_>>();
    server
        .opener()
        .open_writer()
        .unwrap()
        .write_transaction(|store| store.put_artifact(&bytes, &edges))
        .unwrap()
}

/// Put the wire tree `bytes` (canonical DSWL) into `server`'s store, as the
/// daemon's build workers put theirs; returns its layout hash.
pub fn put_wire_tree(server: &ServerHandle, bytes: &[u8]) -> LayoutHash {
    server
        .opener()
        .open_writer()
        .unwrap()
        .write_transaction(|store| store.put_wire_tree(bytes))
        .unwrap()
}

/// The schema authority of [`project_schema`].
pub fn project_authority() -> Arc<ProjectSchemaAuthority> {
    Arc::new(ProjectSchemaAuthority::from_schema(project_schema(), [0; 32]).unwrap())
}
