use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use distill_asset::{AssetType, ErasedValue, ModuleEpochToken};
use distill_core::id::{AssetUuid, ContentHash, LayoutHash};
use distill_loader::{
    AdoptionId, AssetStorage, FetchedArtifact, GameModuleEpoch, HandleId, IoBasis, IoEvent,
    LoadPolicyAttestation, LoadPolicyRow, LoadStatus, Loader, LoaderIO, ManifestHash,
    PathResolveResult, PendingState, PendingToken, PreparedValue, ReattestationState, ReqId,
    ResolveResult, StorageError, UpdateResult,
};
use distill_wire::artifact::{content_hash, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::native::CallbackPanic;
use distill_wire::wire::WireNode;

#[distill_asset::asset(uuid = "41112233-4455-6677-8899-aabbccddeeff")]
struct A;

#[distill_asset::asset(uuid = "42112233-4455-6677-8899-aabbccddeeff")]
struct B;

#[derive(Debug, Clone)]
enum Command {
    Resolve(ReqId, AssetUuid, IoBasis),
    Fetch(ReqId, ContentHash, IoBasis),
    ResolvePath(ReqId, String, IoBasis),
    Subscribe(AssetUuid),
    SubscribePath(String),
    Unsubscribe,
    UnsubscribePath,
}

struct MockIo {
    basis: IoBasis,
    commands: Vec<Command>,
    events: VecDeque<IoEvent>,
    sweeps: usize,
}

impl MockIo {
    fn push(&mut self, event: IoEvent) {
        self.events.push_back(event);
    }

    fn resolve_for(&self, uuid: AssetUuid) -> (ReqId, IoBasis) {
        self.commands
            .iter()
            .rev()
            .find_map(|command| match command {
                Command::Resolve(req, candidate, basis) if *candidate == uuid => {
                    Some((*req, basis.clone()))
                }
                _ => None,
            })
            .unwrap()
    }

    fn fetch_for(&self, hash: ContentHash) -> (ReqId, IoBasis) {
        self.commands
            .iter()
            .rev()
            .find_map(|command| match command {
                Command::Fetch(req, candidate, basis) if *candidate == hash => {
                    Some((*req, basis.clone()))
                }
                _ => None,
            })
            .unwrap()
    }

    fn path_for(&self, path: &str) -> (ReqId, IoBasis) {
        self.commands
            .iter()
            .rev()
            .find_map(|command| match command {
                Command::ResolvePath(req, candidate, basis) if candidate == path => {
                    Some((*req, basis.clone()))
                }
                _ => None,
            })
            .unwrap()
    }
}

impl LoaderIO for MockIo {
    fn begin_sweep(&mut self) -> IoBasis {
        self.sweeps += 1;
        self.basis.clone()
    }

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        self.commands
            .push(Command::Resolve(req, uuid, basis.clone()));
    }

    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis) {
        self.commands
            .push(Command::Fetch(req, content_hash, basis.clone()));
    }

    fn resolve_path(&mut self, req: ReqId, path: &str, basis: &IoBasis) {
        self.commands
            .push(Command::ResolvePath(req, path.to_owned(), basis.clone()));
    }

    fn subscribe(&mut self, uuid: AssetUuid) {
        self.commands.push(Command::Subscribe(uuid));
    }

    fn unsubscribe(&mut self, uuid: AssetUuid) {
        let _ = uuid;
        self.commands.push(Command::Unsubscribe);
    }

    fn subscribe_path(&mut self, path: &str) {
        self.commands.push(Command::SubscribePath(path.to_owned()));
    }

    fn unsubscribe_path(&mut self, path: &str) {
        let _ = path;
        self.commands.push(Command::UnsubscribePath);
    }

    fn poll(&mut self) -> Vec<IoEvent> {
        self.events.drain(..).collect()
    }
}

#[derive(Default)]
struct Storage {
    values: BTreeMap<(HandleId, AdoptionId), ErasedValue>,
    updates: Vec<(HandleId, AdoptionId)>,
    commits: Vec<(HandleId, AdoptionId)>,
    pending_handles: BTreeSet<HandleId>,
    pending_ready: bool,
    tokens: BTreeMap<PendingToken, (HandleId, AdoptionId)>,
    next_token: u64,
    fail_update: Option<(HandleId, GameModuleEpoch)>,
}

impl AssetStorage for Storage {
    fn update(
        &mut self,
        _type_uuid: distill_core::id::TypeUuid,
        handle: HandleId,
        value: ErasedValue,
        adoption: AdoptionId,
    ) -> Result<UpdateResult, StorageError> {
        self.updates.push((handle, adoption));
        if let Some((failed, owner_epoch)) = self.fail_update {
            if failed == handle {
                value.destroy().unwrap();
                return Err(StorageError::Callback {
                    panic: CallbackPanic,
                    owner_epoch,
                });
            }
        }
        self.values.insert((handle, adoption), value);
        if self.pending_handles.contains(&handle) {
            self.next_token += 1;
            let token = PendingToken(self.next_token);
            self.tokens.insert(token, (handle, adoption));
            Ok(UpdateResult::Pending(token))
        } else {
            Ok(UpdateResult::Ready)
        }
    }

    fn poll(&mut self, _token: PendingToken) -> PendingState {
        if self.pending_ready {
            PendingState::Ready
        } else {
            PendingState::Pending
        }
    }

    fn commit(
        &mut self,
        _type_uuid: distill_core::id::TypeUuid,
        handle: HandleId,
        adoption: AdoptionId,
    ) {
        self.commits.push((handle, adoption));
    }

    fn free(
        &mut self,
        _type_uuid: distill_core::id::TypeUuid,
        handle: HandleId,
        adoption: AdoptionId,
    ) -> Result<(), CallbackPanic> {
        if let Some(value) = self.values.remove(&(handle, adoption)) {
            value.destroy()?;
        }
        Ok(())
    }
}

fn uuid(seed: u8) -> AssetUuid {
    AssetUuid([seed; 16])
}

fn basis() -> IoBasis {
    let rows = vec![
        LoadPolicyRow {
            type_uuid: A::TYPE_UUID,
            build_only: false,
        },
        LoadPolicyRow {
            type_uuid: B::TYPE_UUID,
            build_only: false,
        },
    ];
    IoBasis::Pack {
        manifest: ManifestHash([9; 32]),
        load_policy: Arc::new(LoadPolicyAttestation::from_rows(rows).unwrap()),
    }
}

fn mock_io() -> MockIo {
    MockIo {
        basis: basis(),
        commands: Vec::new(),
        events: VecDeque::new(),
        sweeps: 0,
    }
}

fn unit_wire() -> WireNode {
    WireNode::Struct {
        offset: 0,
        size: 0,
        align: 1,
        fields: Vec::new(),
    }
}

fn artifact<T: AssetType>(
    asset_uuid: AssetUuid,
    deps: &[AssetUuid],
) -> (ContentHash, FetchedArtifact) {
    let wire = unit_wire();
    let layout_hash = dswl_hash(&wire).unwrap();
    let bytes = write_artifact(
        &ArtifactHeader {
            asset_uuid,
            authored_type: T::TYPE_UUID,
            terminal_type: T::TYPE_UUID,
            encoded_type: T::TYPE_UUID,
            logical_hash: T::descriptor().logical_hash,
            layout_hash: LayoutHash(layout_hash.0),
        },
        deps,
        &[],
        &[],
        &[],
    )
    .unwrap();
    (
        content_hash(&bytes),
        FetchedArtifact {
            structural: Arc::from(bytes),
            blobs: Vec::new(),
            wire_layout: Arc::from(dswl_bytes(&wire).unwrap()),
        },
    )
}

fn register(loader: &mut Loader<MockIo>, epoch: u64, token: &ModuleEpochToken) {
    loader
        .register_types(
            GameModuleEpoch(epoch),
            token.clone(),
            &[A::descriptor(), B::descriptor()],
        )
        .unwrap();
    assert_eq!(loader.reattestation_state(), ReattestationState::Required);
    loader.confirm_reattested();
}

fn resolve(loader: &mut Loader<MockIo>, asset_uuid: AssetUuid, hash: ContentHash) {
    let (req, request_basis) = loader.io().resolve_for(asset_uuid);
    loader.io_mut().push(IoEvent::Resolved {
        req,
        uuid: asset_uuid,
        result: ResolveResult::Built {
            content_hash: hash,
            basis: request_basis.clone(),
        },
        basis: request_basis,
    });
}

fn fetched(loader: &mut Loader<MockIo>, hash: ContentHash, artifact: FetchedArtifact) {
    let (req, request_basis) = loader.io().fetch_for(hash);
    loader.io_mut().push(IoEvent::Fetched {
        req,
        content_hash: hash,
        artifact,
        basis: request_basis,
    });
}

#[test]
fn one_basis_resolve_fetch_and_preconstructed_injection_commit_at_process_boundary() {
    let token = ModuleEpochToken::new(1);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 1, &token);
    let asset_uuid = uuid(1);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (hash, fetched_artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    let (fetch_req, fetch_basis) = loader.io().fetch_for(hash);
    assert_eq!(fetch_basis, basis());

    loader
        .inject_value(PreparedValue {
            handle: handle.id(),
            uuid: asset_uuid,
            content_hash: hash,
            type_uuid: A::TYPE_UUID,
            load_deps: Vec::new(),
            value: ErasedValue::new_in(A, token.clone()),
        })
        .unwrap();
    loader.io_mut().push(IoEvent::Fetched {
        req: fetch_req,
        content_hash: hash,
        artifact: fetched_artifact,
        basis: fetch_basis,
    });
    loader.process(&mut storage).unwrap();

    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
    assert_eq!(storage.updates.len(), 1);
    assert_eq!(storage.commits, storage.updates);
}

#[test]
fn pending_member_defers_the_whole_dependency_component() {
    let token = ModuleEpochToken::new(2);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 2, &token);
    let a_uuid = uuid(2);
    let b_uuid = uuid(3);
    let a = loader.add_ref::<A>(a_uuid).unwrap();
    let b = loader.add_ref::<B>(b_uuid).unwrap();
    let mut storage = Storage::default();
    storage.pending_handles.insert(b.id());
    loader.process(&mut storage).unwrap();

    let (a_hash, a_artifact) = artifact::<A>(a_uuid, &[b_uuid]);
    let (b_hash, b_artifact) = artifact::<B>(b_uuid, &[]);
    resolve(&mut loader, a_uuid, a_hash);
    resolve(&mut loader, b_uuid, b_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, a_hash, a_artifact);
    fetched(&mut loader, b_hash, b_artifact);
    loader.process(&mut storage).unwrap();

    assert_eq!(storage.updates.len(), 2);
    assert!(storage.commits.is_empty());
    storage.pending_ready = true;
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.commits.len(), 2);
    assert_eq!(loader.status(&a), LoadStatus::Loaded);
    assert_eq!(loader.status(&b), LoadStatus::Loaded);
}

#[test]
fn indirect_handle_rebinds_only_through_io_and_reconnect_blocks_old_completion() {
    let token = ModuleEpochToken::new(3);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 3, &token);
    let handle = loader.add_ref_indirect::<A>("textures/main").unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();
    let (old_req, old_basis) = loader.io().path_for("textures/main");

    loader.io_mut().push(IoEvent::ReconnectRequired {
        reason: distill_loader::ReconnectReason::LoadPolicyChanged,
    });
    loader.io_mut().push(IoEvent::PathResolved {
        req: old_req,
        path: "textures/main".into(),
        result: PathResolveResult::Resolved(uuid(4)),
        basis: old_basis,
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.reattestation_state(), ReattestationState::Required);
    assert_eq!(loader.status(&handle), LoadStatus::Unloaded);

    loader.confirm_reattested();
    loader.process(&mut storage).unwrap();
    let (new_req, new_basis) = loader.io().path_for("textures/main");
    assert_ne!(new_req, old_req);
    loader.io_mut().push(IoEvent::PathResolved {
        req: new_req,
        path: "textures/main".into(),
        result: PathResolveResult::Resolved(uuid(4)),
        basis: new_basis,
    });
    loader.process(&mut storage).unwrap();
    assert!(loader.io().commands.iter().any(
        |command| matches!(command, Command::Resolve(_, candidate, _) if *candidate == uuid(4))
    ));
}

#[test]
fn update_callback_failure_poisons_only_the_reported_module_epoch() {
    let token = ModuleEpochToken::new(4);
    let other = ModuleEpochToken::new(5);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 4, &token);
    let asset_uuid = uuid(5);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage {
        fail_update: Some((handle.id(), GameModuleEpoch(4))),
        ..Storage::default()
    };
    loader.process(&mut storage).unwrap();
    let (hash, artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, artifact);
    loader.process(&mut storage).unwrap();

    assert!(token.is_poisoned());
    assert!(!other.is_poisoned());
    assert!(storage.commits.is_empty());
}

#[test]
fn mock_io_records_subscriptions_through_the_declared_boundary() {
    let token = ModuleEpochToken::new(6);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 6, &token);
    let _direct = loader.add_ref::<A>(uuid(6)).unwrap();
    let _indirect = loader.add_ref_indirect::<B>("models/hero").unwrap();
    loader.process(&mut Storage::default()).unwrap();
    assert!(loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::Subscribe(candidate) if *candidate == uuid(6))));
    assert!(loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::SubscribePath(path) if path == "models/hero")));
}
