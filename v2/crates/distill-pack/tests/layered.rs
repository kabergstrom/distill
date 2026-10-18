//! `LayeredIO` over packs and a daemon (DESIGN.md §16 "Layer stack";
//! newgameplus GAP 28 §12 phase 2), driven by the real `Loader`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use distill_asset::{AssetType, ErasedValue};
use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_loader::{
    AdoptionId, AssetDeltaState, AssetPath, AssetStorage, FetchedArtifact, GameModuleEpoch,
    HandleId, IoBasis, IoEvent, Layer, LayeredIO, LoadStatus, Loader, LoaderIO, ManifestState,
    PathResolveResult, PendingState, PendingToken, ReqId, ResolveResult, RpcIo, RuntimeTarget,
    StorageError, TargetBindingState, UpdateResult,
};
use distill_pack::archive::{encode_archive, ArtifactPayload};
use distill_pack::manifest::{
    encode_manifest, ArchiveRef, EncodingRow, IndexRow, ManifestAssetRow, ManifestLoadEdge,
    PackManifest, PackTarget, PathRow, WireTreeRow,
};
use distill_pack::{PackfileIO, RuntimeTarget as PackTarget_};
use distill_protocol::ServedLoadEdge;
use distill_protocol::state::{InputVersion, SnapshotStamp, StoreInstanceId};
use distill_wire::artifact::{content_hash, parse_artifact, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::native::CallbackPanic;
use distill_wire::wire::WireNode;

#[distill_asset::asset(uuid = "61112233-4455-6677-8899-aabbccddeeff")]
struct A;

/// The game's target: the loader's, the daemon's, the game pack's.
const GAME: [u8; 16] = [7; 16];
/// The engine pack's own target.
const ENGINE: [u8; 16] = [5; 16];

fn uuid(seed: u8) -> AssetUuid {
    AssetUuid([seed; 16])
}

fn unit_wire() -> WireNode {
    WireNode::Struct {
        offset: 0,
        size: 0,
        align: 1,
        fields: Vec::new(),
    }
}

/// One built asset: an `A` whose blob `[version; 8]` tells its versions
/// apart, with load edges to `deps` (all `A`).
struct Built {
    asset: AssetUuid,
    hash: ContentHash,
    structural: Vec<u8>,
    blob: Vec<u8>,
    deps: Vec<AssetUuid>,
}

fn built(asset: AssetUuid, version: u8, deps: &[AssetUuid]) -> Built {
    let layout_hash = dswl_hash(&unit_wire()).unwrap();
    let blob = vec![version; 8];
    let bytes = write_artifact(
        &ArtifactHeader {
            asset_uuid: asset,
            authored_type: A::TYPE_UUID,
            terminal_type: A::TYPE_UUID,
            encoded_type: A::TYPE_UUID,
            logical_hash: A::descriptor().logical_hash,
            layout_hash: LayoutHash(layout_hash.0),
        },
        deps,
        &[],
        &[],
        &[(Vec::new(), blob.as_slice())],
    )
    .unwrap();
    let parsed = parse_artifact(&bytes).unwrap();
    let structural_len = bytes.len() - parsed.blob_section.len();
    Built {
        asset,
        hash: content_hash(&bytes),
        structural: bytes[..structural_len].to_vec(),
        blob,
        deps: deps.to_vec(),
    }
}

impl Built {
    fn fetched(&self) -> FetchedArtifact {
        let wire = dswl_bytes(&unit_wire()).unwrap();
        FetchedArtifact {
            structural: Arc::from(self.structural.clone()),
            blobs: vec![memory_blob(self.blob.clone())],
            load_edges: self
                .deps
                .iter()
                .map(|asset| ServedLoadEdge {
                    asset: *asset,
                    expected_terminal: A::TYPE_UUID,
                })
                .collect(),
            wire_layout: memory_blob(wire),
            timing: Default::default(),
        }
    }
}

fn memory_blob(bytes: Vec<u8>) -> distill_wire::exec::Blob {
    let len = bytes.len();
    let backing: Arc<dyn AsRef<[u8]> + Send + Sync> = Arc::new(bytes);
    distill_wire::exec::Blob::new(backing, 0, len)
}

/// A pack for `target` holding `assets`, with a path table of `paths`,
/// mounted from memory as an embedded pack is.
fn pack(target: [u8; 16], assets: &[&Built], paths: &[(&str, AssetUuid)]) -> PackfileIO {
    let wire = unit_wire();
    let archive = encode_archive(
        1,
        "zstd-test",
        3,
        &assets
            .iter()
            .map(|built| ArtifactPayload {
                content_hash: built.hash,
                structural: built.structural.clone(),
                blobs: vec![built.blob.clone()],
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut rows = assets
        .iter()
        .map(|built| ManifestAssetRow {
            asset_uuid: built.asset,
            content_hash: built.hash,
            load_deps: built
                .deps
                .iter()
                .map(|asset| ManifestLoadEdge {
                    asset_uuid: *asset,
                    expected_terminal: A::TYPE_UUID,
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.asset_uuid);
    let mut encodings = assets
        .iter()
        .map(|built| {
            let encoding = &archive.encodings[&built.hash];
            EncodingRow {
                content_hash: built.hash,
                blocks: encoding.blocks.clone(),
                blobs: encoding.blobs.clone(),
            }
        })
        .collect::<Vec<_>>();
    encodings.sort_by_key(|row| row.content_hash);
    let mut path_rows = paths
        .iter()
        .map(|(path, asset)| PathRow {
            path: (*path).to_owned(),
            name: None,
            asset_uuid: *asset,
        })
        .collect::<Vec<_>>();
    path_rows.sort_by(|a, b| a.path.cmp(&b.path));
    let manifest = PackManifest {
        target: PackTarget { name: "dev".into() },
        target_def_hash: target,
        archives: vec![ArchiveRef {
            generation: 1,
            file_hash: distill_core::hash::hash(&archive.bytes),
        }],
        assets: rows,
        encodings,
        index: archive
            .index
            .iter()
            .map(|(ekey, location)| IndexRow {
                ekey: *ekey,
                location: *location,
            })
            .collect(),
        wire_trees: vec![WireTreeRow {
            layout_hash: dswl_hash(&wire).unwrap(),
            bytes: dswl_bytes(&wire).unwrap(),
        }],
        paths: Some(path_rows),
    };
    let manifest: &'static [u8] = Box::leak(encode_manifest(&manifest).unwrap().into_boxed_slice());
    let archive: &'static [u8] = Box::leak(archive.bytes.into_boxed_slice());
    PackfileIO::mount_static(
        manifest,
        &[archive],
        &PackTarget_ {
            target: "dev".into(),
            target_def_hash: target,
        },
    )
    .unwrap()
}

fn stamp(version: u64) -> SnapshotStamp {
    SnapshotStamp {
        instance: StoreInstanceId([9; 16]),
        version: InputVersion(version),
    }
}

/// An in-process daemon layer: answers at once from what it names, at a
/// snapshot that advances with every change it publishes. Offline, it binds
/// nothing and names nothing, as a disconnected `RpcIo` does.
struct FakeDaemon {
    online: bool,
    version: u64,
    named: BTreeMap<AssetUuid, Built>,
    paths: BTreeMap<String, AssetUuid>,
    target: Option<RuntimeTarget>,
    events: VecDeque<IoEvent>,
}

impl FakeDaemon {
    fn new(online: bool, named: Vec<Built>) -> Self {
        Self {
            online,
            version: 1,
            named: named.into_iter().map(|built| (built.asset, built)).collect(),
            paths: BTreeMap::new(),
            target: None,
            events: VecDeque::new(),
        }
    }

    fn basis(&self) -> IoBasis {
        IoBasis::Rpc {
            snapshot: stamp(self.version),
        }
    }

    /// The daemon connects.
    fn connect(&mut self) {
        self.online = true;
        self.version += 1;
        let target = self.target.clone().expect("bound before connecting");
        self.events.push_back(IoEvent::TargetBound {
            target,
            basis: self.basis(),
        });
    }

    /// The asset's bundle is deleted: published as a deletion.
    fn delete(&mut self, asset: AssetUuid) {
        self.named.remove(&asset);
        self.version += 1;
        self.events.push_back(IoEvent::Delta {
            stamp: stamp(self.version),
            assets: vec![(asset, AssetDeltaState::Deleted)],
            paths: Vec::new(),
        });
    }
}

impl LoaderIO for FakeDaemon {
    fn bind_target(&mut self, target: RuntimeTarget) {
        self.target = Some(target.clone());
        if self.online {
            self.events.push_back(IoEvent::TargetBound {
                target,
                basis: self.basis(),
            });
        }
    }

    fn begin_sweep(&mut self) -> IoBasis {
        self.basis()
    }

    fn end_sweep(&mut self, _basis: &IoBasis) {}

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        let result = match self.named.get(&uuid) {
            Some(built) if self.online => ResolveResult::Built {
                content_hash: built.hash,
            },
            _ => ResolveResult::Missing,
        };
        self.events.push_back(IoEvent::Resolved {
            req,
            uuid,
            result,
            basis: basis.clone(),
        });
    }

    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis) {
        let event = match self.named.values().find(|built| built.hash == content_hash) {
            Some(built) if self.online => IoEvent::Fetched {
                req,
                content_hash,
                artifact: built.fetched(),
                basis: basis.clone(),
            },
            _ => IoEvent::RequestError {
                req,
                message: "not in the daemon's cache".into(),
                basis: basis.clone(),
            },
        };
        self.events.push_back(event);
    }

    fn resolve_path(&mut self, req: ReqId, path: &AssetPath, basis: &IoBasis) {
        let result = match self.paths.get(&path.path) {
            Some(asset) if self.online && path.name.is_none() => {
                PathResolveResult::Resolved(*asset)
            }
            _ => PathResolveResult::Missing,
        };
        self.events.push_back(IoEvent::PathResolved {
            req,
            path: path.clone(),
            result,
            basis: basis.clone(),
        });
    }

    fn subscribe(&mut self, _uuid: AssetUuid) {}
    fn unsubscribe(&mut self, _uuid: AssetUuid) {}
    fn subscribe_path(&mut self, _path: &str) {}
    fn unsubscribe_path(&mut self, _path: &str) {}

    fn poll(&mut self) -> Vec<IoEvent> {
        self.events.drain(..).collect()
    }
}

#[derive(Default)]
struct Storage {
    values: BTreeMap<(HandleId, AdoptionId), ErasedValue>,
}

impl AssetStorage for Storage {
    fn update(
        &mut self,
        _type_uuid: TypeUuid,
        handle: HandleId,
        value: ErasedValue,
        adoption: AdoptionId,
    ) -> Result<UpdateResult, StorageError> {
        self.values.insert((handle, adoption), value);
        Ok(UpdateResult::Ready)
    }

    fn poll(&mut self, _token: PendingToken) -> PendingState {
        PendingState::Ready
    }

    fn commit(&mut self, _type_uuid: TypeUuid, _handle: HandleId, _adoption: AdoptionId) {}

    fn free(
        &mut self,
        _type_uuid: TypeUuid,
        handle: HandleId,
        adoption: AdoptionId,
    ) -> Result<(), CallbackPanic> {
        match self.values.remove(&(handle, adoption)) {
            Some(value) => value.destroy(),
            None => Ok(()),
        }
    }
}

fn loader(layers: Vec<Layer>) -> Loader<LayeredIO> {
    let mut loader = Loader::new(LayeredIO::new(layers));
    loader
        .register_types(
            GameModuleEpoch(1),
            distill_asset::ModuleEpochToken::new(1),
            GAME,
            &[A::descriptor()],
        )
        .unwrap();
    loader
}

/// Process until `done` holds, or fail after a while (an `RpcIo` layer
/// connects in real time).
fn process_until(
    loader: &mut Loader<LayeredIO>,
    storage: &mut Storage,
    mut done: impl FnMut(&Loader<LayeredIO>) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done(loader) {
        loader.process(storage).unwrap();
        assert!(
            Instant::now() < deadline,
            "the loader did not get there: {:?}",
            loader.take_diagnostics()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn current(loader: &Loader<LayeredIO>, asset: AssetUuid) -> Option<ContentHash> {
    match loader.manifest_entry(asset)?.state {
        ManifestState::Current { content_hash } => Some(content_hash),
        _ => None,
    }
}

#[test]
fn a_uuid_two_layers_name_resolves_to_the_upper_one() {
    let engine = built(uuid(1), 1, &[]);
    let game = built(uuid(1), 2, &[]);
    let mut loader = loader(vec![
        Layer::new(pack(ENGINE, &[&engine], &[])).with_target(ENGINE),
        Layer::new(pack(GAME, &[&game], &[])),
    ]);
    let mut storage = Storage::default();
    let handle = loader.add_ref::<A>(uuid(1)).unwrap();
    process_until(&mut loader, &mut storage, |loader| {
        loader.status(&handle) == LoadStatus::Loaded
    });
    assert_eq!(loader.target_binding_state(), TargetBindingState::Bound);
    assert_eq!(current(&loader, uuid(1)), Some(game.hash));
    assert_ne!(engine.hash, game.hash);
}

#[test]
fn a_uuid_resolves_to_the_lower_layer_once_the_upper_drops_it() {
    let engine = built(uuid(1), 1, &[]);
    let edited = built(uuid(1), 2, &[]);
    let edited_hash = edited.hash;
    let mut loader = loader(vec![
        Layer::new(pack(ENGINE, &[&engine], &[])).with_target(ENGINE),
        Layer::new(FakeDaemon::new(true, vec![edited])).optional(),
    ]);
    let mut storage = Storage::default();
    let handle = loader.add_ref::<A>(uuid(1)).unwrap();
    process_until(&mut loader, &mut storage, |loader| {
        current(loader, uuid(1)) == Some(edited_hash)
    });

    // The daemon's bundle goes: the engine pack's version is a new
    // revision, never a deletion.
    loader
        .io_mut()
        .layer_mut::<FakeDaemon>()
        .unwrap()
        .delete(uuid(1));
    process_until(&mut loader, &mut storage, |loader| {
        current(loader, uuid(1)) == Some(engine.hash)
    });
    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
    assert_eq!(storage.values.len(), 1, "one adoption is resident");
}

#[test]
fn a_daemon_layer_that_never_connects_leaves_the_lower_layers_loading() {
    let engine = built(uuid(1), 1, &[]);
    let game = built(uuid(2), 2, &[]);
    // A port nothing listens on.
    let address = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let daemon = RpcIo::connect_in_background(
        address,
        distill_protocol::ConnectRequest::new("dev", distill_protocol::TargetDefinitionHash(GAME)),
    )
    .unwrap();
    let mut loader = loader(vec![
        Layer::new(pack(ENGINE, &[&engine], &[])).with_target(ENGINE),
        Layer::new(pack(GAME, &[&game], &[])),
        Layer::new(daemon).optional(),
    ]);
    let mut storage = Storage::default();
    let engine_handle = loader.add_ref::<A>(uuid(1)).unwrap();
    let game_handle = loader.add_ref::<A>(uuid(2)).unwrap();
    process_until(&mut loader, &mut storage, |loader| {
        loader.status(&engine_handle) == LoadStatus::Loaded
            && loader.status(&game_handle) == LoadStatus::Loaded
    });
    assert_eq!(current(&loader, uuid(1)), Some(engine.hash));
    assert_eq!(current(&loader, uuid(2)), Some(game.hash));

    // The engine reads the daemon's status from its layer.
    process_until(&mut loader, &mut storage, |loader| {
        loader
            .io()
            .layer::<RpcIo>()
            .is_some_and(|daemon| daemon.status().last_error.is_some())
    });
    let status = loader.io().layer::<RpcIo>().unwrap().status();
    assert!(!status.connected);
    assert!(!loader.io().layer_bound(2));
}

#[test]
fn a_path_resolves_through_the_stack() {
    let engine = built(uuid(1), 1, &[]);
    let shadowed = built(uuid(3), 3, &[]);
    let game = built(uuid(2), 2, &[]);
    let mut loader = loader(vec![
        Layer::new(pack(
            ENGINE,
            &[&engine, &shadowed],
            &[("engine/a.bundle", uuid(1)), ("shared/c.bundle", uuid(3))],
        ))
        .with_target(ENGINE),
        Layer::new(pack(
            GAME,
            &[&game],
            &[("game/b.bundle", uuid(2)), ("shared/c.bundle", uuid(2))],
        )),
        Layer::new(FakeDaemon::new(false, Vec::new())).optional(),
    ]);
    let mut storage = Storage::default();
    let lower = loader.add_ref_indirect::<A>("engine/a.bundle").unwrap();
    let upper = loader.add_ref_indirect::<A>("game/b.bundle").unwrap();
    let both = loader.add_ref_indirect::<A>("shared/c.bundle").unwrap();
    let neither = loader.add_ref_indirect::<A>("nowhere/d.bundle").unwrap();
    process_until(&mut loader, &mut storage, |loader| {
        [&lower, &upper, &both]
            .iter()
            .all(|handle| loader.status(handle) == LoadStatus::Loaded)
    });
    assert_eq!(current(&loader, uuid(1)), Some(engine.hash));
    assert_eq!(current(&loader, uuid(2)), Some(game.hash));
    assert_eq!(
        current(&loader, uuid(3)),
        None,
        "a path both packs name is the upper one's"
    );
    assert_ne!(loader.status(&neither), LoadStatus::Loaded);
}

#[test]
fn a_closure_edge_from_an_upper_layer_asset_to_a_lower_layer_asset_loads() {
    let shader = built(uuid(1), 1, &[]);
    let material = built(uuid(2), 2, &[uuid(1)]);
    let material_hash = material.hash;
    let mut loader = loader(vec![
        Layer::new(pack(ENGINE, &[&shader], &[])).with_target(ENGINE),
        Layer::new(FakeDaemon::new(true, vec![material])).optional(),
    ]);
    let mut storage = Storage::default();
    let handle = loader.add_ref::<A>(uuid(2)).unwrap();
    process_until(&mut loader, &mut storage, |loader| {
        loader.status(&handle) == LoadStatus::Loaded
    });
    assert_eq!(current(&loader, uuid(2)), Some(material_hash));
    assert_eq!(current(&loader, uuid(1)), Some(shader.hash));
}

#[test]
fn a_daemon_that_connects_late_replaces_what_the_pack_answered() {
    let engine = built(uuid(1), 1, &[]);
    let edited = built(uuid(1), 2, &[]);
    let edited_hash = edited.hash;
    let mut loader = loader(vec![
        Layer::new(pack(ENGINE, &[&engine], &[])).with_target(ENGINE),
        Layer::new(FakeDaemon::new(false, vec![edited])).optional(),
    ]);
    let mut storage = Storage::default();
    let handle = loader.add_ref::<A>(uuid(1)).unwrap();
    process_until(&mut loader, &mut storage, |loader| {
        loader.status(&handle) == LoadStatus::Loaded
    });
    assert_eq!(current(&loader, uuid(1)), Some(engine.hash));

    loader
        .io_mut()
        .layer_mut::<FakeDaemon>()
        .unwrap()
        .connect();
    process_until(&mut loader, &mut storage, |loader| {
        current(loader, uuid(1)) == Some(edited_hash)
    });
    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
    assert!(loader.io().layer_bound(1));
}

/// Each request gets one terminal event, under the stack basis, from the
/// topmost layer naming it; a layer basis is ended only once no live stack
/// basis shares it.
#[test]
fn layer_answers_settle_top_down_under_the_stack_basis() {
    let engine = built(uuid(1), 1, &[]);
    let game = built(uuid(1), 2, &[]);
    let mut io = LayeredIO::new(vec![
        Layer::new(pack(ENGINE, &[&engine], &[])).with_target(ENGINE),
        Layer::new(FakeDaemon::new(true, vec![game])).optional(),
    ]);
    let target = RuntimeTarget::new(GameModuleEpoch(1), GAME);
    io.bind_target(target.clone());
    let bound = io.poll();
    assert!(
        matches!(bound.as_slice(), [IoEvent::TargetBound { target: got, .. }] if *got == target),
        "{bound:?}"
    );

    let basis = io.begin_sweep();
    assert!(matches!(&basis, IoBasis::Layers(layers) if layers.len() == 2));
    io.resolve(ReqId(1), uuid(1), &basis);
    io.resolve(ReqId(2), uuid(9), &basis);
    io.resolve_path(ReqId(3), &AssetPath::from("nowhere"), &basis);
    let events = io.poll();
    assert_eq!(events.len(), 3, "{events:?}");
    let upper = match &events[0] {
        IoEvent::Resolved {
            req: ReqId(1),
            result: ResolveResult::Built { content_hash },
            basis: got,
            ..
        } if *got == basis => *content_hash,
        other => panic!("{other:?}"),
    };
    assert!(matches!(&events[1], IoEvent::Resolved {
        req: ReqId(2), result: ResolveResult::Missing, basis: got, ..
    } if *got == basis));
    assert!(matches!(&events[2], IoEvent::PathResolved {
        req: ReqId(3), result: PathResolveResult::Missing, basis: got, ..
    } if *got == basis));

    io.fetch(ReqId(4), upper, &basis);
    let fetched = io.poll();
    assert!(
        matches!(fetched.as_slice(), [IoEvent::Fetched { req: ReqId(4), basis: got, .. }] if *got == basis),
        "fetched from the layer that answered: {fetched:?}"
    );

    // Ended: nothing answers it.
    io.resolve(ReqId(5), uuid(1), &basis);
    io.end_sweep(&basis);
    assert!(io.poll().is_empty());
}

#[test]
fn a_path_only_the_daemon_names_resolves_once_the_daemon_connects() {
    let engine = built(uuid(1), 1, &[]);
    let game = built(uuid(2), 2, &[]);
    let game_hash = game.hash;
    let mut daemon = FakeDaemon::new(false, vec![game]);
    daemon.paths.insert("prefabs/game.prefab".into(), uuid(2));
    let mut loader = loader(vec![
        Layer::new(pack(ENGINE, &[&engine], &[])).with_target(ENGINE),
        Layer::new(daemon).optional(),
    ]);
    let mut storage = Storage::default();
    let shader = loader.add_ref::<A>(uuid(1)).unwrap();
    let root = loader.add_ref_indirect::<A>("prefabs/game.prefab").unwrap();
    // The engine pack loads with the daemon down; the game's path waits.
    process_until(&mut loader, &mut storage, |loader| {
        loader.status(&shader) == LoadStatus::Loaded
    });
    for _ in 0..10 {
        loader.process(&mut storage).unwrap();
    }
    assert_ne!(loader.status(&root), LoadStatus::Loaded);
    assert_ne!(loader.status(&root), LoadStatus::Dead);
    assert_eq!(current(&loader, uuid(2)), None);

    loader
        .io_mut()
        .layer_mut::<FakeDaemon>()
        .unwrap()
        .connect();
    process_until(&mut loader, &mut storage, |loader| {
        loader.status(&root) == LoadStatus::Loaded
    });
    assert_eq!(current(&loader, uuid(2)), Some(game_hash));
    assert_eq!(loader.status(&shader), LoadStatus::Loaded);
}

#[test]
fn what_nobody_names_stays_unanswered_while_the_daemon_is_down() {
    let engine = built(uuid(1), 1, &[]);
    let mut io = LayeredIO::new(vec![
        Layer::new(pack(ENGINE, &[&engine], &[("engine/a.bundle", uuid(1))])).with_target(ENGINE),
        Layer::new(FakeDaemon::new(false, Vec::new())).optional(),
    ]);
    let target = RuntimeTarget::new(GameModuleEpoch(1), GAME);
    io.bind_target(target);
    assert!(matches!(io.poll().as_slice(), [IoEvent::TargetBound { .. }]));

    let basis = io.begin_sweep();
    io.resolve_path(ReqId(1), &AssetPath::from("nowhere/b.bundle"), &basis);
    io.resolve(ReqId(2), uuid(9), &basis);
    // What the pack names answers at once.
    io.resolve_path(ReqId(3), &AssetPath::from("engine/a.bundle"), &basis);
    let events = io.poll();
    assert!(
        matches!(events.as_slice(), [IoEvent::PathResolved {
            req: ReqId(3), result: PathResolveResult::Resolved(found), basis: got, ..
        }] if *found == uuid(1) && *got == basis),
        "{events:?}"
    );
    for _ in 0..10 {
        let events = io.poll();
        assert!(events.is_empty(), "answered while the daemon is down: {events:?}");
    }

    io.layer_mut::<FakeDaemon>().unwrap().connect();
    // Asked again once it binds; it answers at the next poll.
    let mut events = io.poll();
    events.extend(io.poll());
    assert_eq!(events.len(), 2, "{events:?}");
    assert!(events.iter().any(|event| matches!(event, IoEvent::PathResolved {
        req: ReqId(1), result: PathResolveResult::Missing, basis: got, ..
    } if *got == basis)));
    assert!(events.iter().any(|event| matches!(event, IoEvent::Resolved {
        req: ReqId(2), result: ResolveResult::Missing, basis: got, ..
    } if *got == basis)));
}
