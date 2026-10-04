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
use std::sync::{Arc, Mutex, Weak};

use distill_bundle::{AssetEntry, Bundle, BUNDLE_FORMAT_VERSION};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_daemon::coordinator::DaemonCoordinator;
use distill_daemon::scanner::AssetRoot;
use distill_daemon::watcher::WatcherBatch;
use distill_json::AuthoredValue;
use distill_rpc::{
    ArtifactPayload, BuildAnswer, BuildArtifactPublication, BuildBackend, BuildCompletion,
    BuildPublication, BuildRequest, BuildStart, BuildTicket, BuildView, BuildWireTree, RpcFailure,
    Server, ServerHandle, SnapshotStamp, TargetDefinition,
};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, SchemaNode};
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
        let roots = roots.iter().map(|root| (*root).to_owned()).collect::<Vec<_>>();
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
            touched: BTreeSet::new(),
            dir,
        }
    }

    /// The same project after a daemon restart: its files and its store,
    /// served by a new coordinator.
    pub fn restart(self) -> Self {
        let TestProject {
            coordinator,
            writer,
            targets,
            roots,
            dir,
            ..
        } = self;
        drop(writer);
        drop(coordinator);
        Self::open(dir, targets, roots)
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
        let paths = std::mem::take(&mut self.touched).into_iter().collect();
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
        let server = self.server.upgrade().expect("the server outlives its backend");
        let mut writer = server.opener().open_writer().unwrap();
        let content_hash = server.install_build_publication(
            &mut writer,
            request.requested_asset,
            BuildPublication {
                root_content_hash: content_hash,
                artifacts: vec![BuildArtifactPublication {
                    content_hash,
                    payload,
                }],
                wire_trees: vec![BuildWireTree {
                    layout_hash,
                    bytes: wire_bytes,
                }],
            },
        )?;
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
