use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use distill_asset::{AssetRuntimeDescriptor, AssetType, EncodeSink, ErasedValue, ModuleEpochToken};
use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_loader::{
    AdoptionId, AssetPath, AssetStorage, CompletionDisposition, FetchedArtifact, GameModuleEpoch, HandleId,
    IoBasis, IoEvent, LoadStatus, Loader, LoaderDiagnostic, LoaderIO, ManifestHash, ManifestState,
    PathResolveResult, PendingState, PendingToken, RegistrationError, ReqId, ResolveResult,
    RuntimeTarget, StorageError, TargetBindingState, UpdateResult,
};
use distill_rpc::ServedLoadEdge;
use distill_store::state::{InputVersion, StoreInstanceId};
use distill_wire::artifact::{content_hash, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::native::CallbackPanic;
use distill_wire::wire::WireNode;

#[distill_asset::asset(uuid = "41112233-4455-6677-8899-aabbccddeeff")]
struct A;

#[distill_asset::asset(uuid = "42112233-4455-6677-8899-aabbccddeeff")]
struct B;

#[distill_asset::asset(uuid = "43112233-4455-6677-8899-aabbccddeeff", build_only)]
struct BuildOnly;

static COUNTED_DROPS: AtomicUsize = AtomicUsize::new(0);

#[distill_asset::asset(uuid = "44112233-4455-6677-8899-aabbccddeeff")]
struct Counted;

impl Drop for Counted {
    fn drop(&mut self) {
        COUNTED_DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

const PLACEHOLDER_TYPE: TypeUuid = TypeUuid([0x43; 16]);

fn blob(bytes: Vec<u8>) -> distill_wire::exec::Blob {
    let len = bytes.len();
    let backing: Arc<dyn AsRef<[u8]> + Send + Sync> = Arc::new(bytes);
    distill_wire::exec::Blob::new(backing, 0, len)
}

struct RefPlaceholder;

#[derive(Clone, Default)]
struct PlaceholderConfig {
    references: Vec<(bool, AssetUuid, TypeUuid)>,
    panic_while_visiting: bool,
}

thread_local! {
    static PLACEHOLDER_CONFIG: RefCell<PlaceholderConfig> = RefCell::default();
}

fn configure_placeholder(references: Vec<(bool, AssetUuid, TypeUuid)>, panic_while_visiting: bool) {
    PLACEHOLDER_CONFIG.with(|config| {
        *config.borrow_mut() = PlaceholderConfig {
            references,
            panic_while_visiting,
        };
    });
}

unsafe fn encode_ref_placeholder(
    value_ptr: *const u8,
    sink: &mut dyn EncodeSink,
) -> Result<(), CallbackPanic> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = value_ptr;
        PLACEHOLDER_CONFIG.with(|config| {
            let config = config.borrow();
            assert!(!config.panic_while_visiting, "test visitor panic");
            for (strong, target, expected) in &config.references {
                sink.reference(*strong, *target, *expected)?;
            }
            Ok(())
        })
    })) {
        Ok(result) => result,
        Err(_) => Err(CallbackPanic),
    }
}

unsafe impl AssetType for RefPlaceholder {
    const TYPE_UUID: TypeUuid = PLACEHOLDER_TYPE;

    fn descriptor() -> &'static AssetRuntimeDescriptor {
        static DESCRIPTOR: OnceLock<AssetRuntimeDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| {
            let base = A::descriptor();
            AssetRuntimeDescriptor {
                type_uuid: PLACEHOLDER_TYPE,
                logical_hash: base.logical_hash,
                build_only: false,
                native_layout: base.native_layout,
                size: base.size,
                align: base.align,
                ctors: base.ctors,
                drops: base.drops,
                skip_writers: base.skip_writers,
                finalize: base.finalize,
                encode: encode_ref_placeholder,
            }
        })
    }
}

#[derive(Debug, Clone)]
enum Command {
    BindTarget,
    Resolve(ReqId, AssetUuid, IoBasis),
    Fetch(ReqId, ContentHash, IoBasis),
    ResolvePath(ReqId, AssetPath, IoBasis),
    Subscribe(AssetUuid),
    SubscribePath(String),
    Unsubscribe(AssetUuid),
    UnsubscribePath,
    EndSweep(IoBasis),
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
                Command::ResolvePath(req, candidate, basis)
                    if *candidate == AssetPath::from(path) =>
                {
                    Some((*req, basis.clone()))
                }
                _ => None,
            })
            .unwrap()
    }

    fn named_for(&self, path: &str, name: &str) -> Option<(ReqId, IoBasis)> {
        self.commands.iter().rev().find_map(|command| match command {
            Command::ResolvePath(req, candidate, basis)
                if *candidate == AssetPath::named(path, name) =>
            {
                Some((*req, basis.clone()))
            }
            _ => None,
        })
    }
}

impl LoaderIO for MockIo {
    fn bind_target(&mut self, target: RuntimeTarget) {
        self.commands.push(Command::BindTarget);
        self.events.push_back(IoEvent::TargetBound {
            target,
            basis: self.basis.clone(),
        });
    }

    fn begin_sweep(&mut self) -> IoBasis {
        self.sweeps += 1;
        self.basis.clone()
    }

    fn end_sweep(&mut self, basis: &IoBasis) {
        self.commands.push(Command::EndSweep(basis.clone()));
    }

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        self.commands
            .push(Command::Resolve(req, uuid, basis.clone()));
    }

    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis) {
        self.commands
            .push(Command::Fetch(req, content_hash, basis.clone()));
    }

    fn resolve_path(&mut self, req: ReqId, path: &AssetPath, basis: &IoBasis) {
        self.commands
            .push(Command::ResolvePath(req, path.clone(), basis.clone()));
    }

    fn subscribe(&mut self, uuid: AssetUuid) {
        self.commands.push(Command::Subscribe(uuid));
    }

    fn unsubscribe(&mut self, uuid: AssetUuid) {
        self.commands.push(Command::Unsubscribe(uuid));
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
    pending_failure: Option<StorageError>,
    tokens: BTreeMap<PendingToken, (HandleId, AdoptionId)>,
    polls: usize,
    next_token: u64,
    fail_update: Option<(HandleId, GameModuleEpoch)>,
    fail_free: BTreeSet<HandleId>,
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
        self.polls += 1;
        if let Some(error) = self.pending_failure.clone() {
            PendingState::Failed(error)
        } else if self.pending_ready {
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
        if self.fail_free.contains(&handle) {
            return Err(CallbackPanic);
        }
        self.tokens
            .retain(|_, pending| *pending != (handle, adoption));
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
    basis_with(9)
}

fn basis_with(manifest_byte: u8) -> IoBasis {
    IoBasis::Pack {
        manifest: ManifestHash([manifest_byte; 32]),
    }
}

fn stamp(version: u64) -> distill_store::state::SnapshotStamp {
    distill_store::state::SnapshotStamp {
        instance: StoreInstanceId([7; 16]),
        version: InputVersion(version),
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
    assert!(deps.is_empty(), "use artifact_with_edges for dependencies");
    artifact_with_edges::<T>(asset_uuid, &[])
}

fn artifact_with_edges<T: AssetType>(
    asset_uuid: AssetUuid,
    edges: &[(AssetUuid, TypeUuid)],
) -> (ContentHash, FetchedArtifact) {
    let wire = unit_wire();
    let layout_hash = dswl_hash(&wire).unwrap();
    let deps = edges.iter().map(|(uuid, _)| *uuid).collect::<Vec<_>>();
    let bytes = write_artifact(
        &ArtifactHeader {
            asset_uuid,
            authored_type: T::TYPE_UUID,
            terminal_type: T::TYPE_UUID,
            encoded_type: T::TYPE_UUID,
            logical_hash: T::descriptor().logical_hash,
            layout_hash: LayoutHash(layout_hash.0),
        },
        &deps,
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
            load_edges: edges
                .iter()
                .map(|(asset_uuid, expected_terminal)| ServedLoadEdge {
                    asset: *asset_uuid,
                    expected_terminal: *expected_terminal,
                })
                .collect(),
            wire_layout: blob(dswl_bytes(&wire).unwrap()),
        },
    )
}

fn register(loader: &mut Loader<MockIo>, epoch: u64, token: &ModuleEpochToken) {
    loader
        .register_types(
            GameModuleEpoch(epoch),
            token.clone(),
            [7; 32],
            &[
                A::descriptor(),
                B::descriptor(),
                RefPlaceholder::descriptor(),
                BuildOnly::descriptor(),
                Counted::descriptor(),
            ],
        )
        .unwrap();
    assert_eq!(loader.target_binding_state(), TargetBindingState::Required);
}

fn register_ref_placeholder(loader: &mut Loader<MockIo>, epoch: u64) {
    loader
        .register_placeholder(
            GameModuleEpoch(epoch),
            distill_asset::placeholder!(RefPlaceholder, RefPlaceholder),
        )
        .unwrap();
}

#[test]
fn successor_registration_cannot_overwrite_a_live_descriptor() {
    let mut loader = Loader::new(mock_io());
    let first = ModuleEpochToken::new(1);
    register(&mut loader, 1, &first);
    let second = ModuleEpochToken::new(2);

    assert_eq!(
        loader.register_types(GameModuleEpoch(2), second, [7; 32], &[A::descriptor()],),
        Err(RegistrationError::DuplicateType(A::TYPE_UUID)),
    );
}

fn resolve(loader: &mut Loader<MockIo>, asset_uuid: AssetUuid, hash: ContentHash) {
    let (req, request_basis) = loader.io().resolve_for(asset_uuid);
    loader.io_mut().push(IoEvent::Resolved {
        req,
        uuid: asset_uuid,
        result: ResolveResult::Built { content_hash: hash },
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

fn load_placeholder_root(
    loader: &mut Loader<MockIo>,
    storage: &mut Storage,
    root: AssetUuid,
) -> distill_loader::Handle<RefPlaceholder> {
    let handle = loader.add_ref::<RefPlaceholder>(root).unwrap();
    loader.process(storage).unwrap();
    let (hash, root_artifact) = artifact::<RefPlaceholder>(root, &[]);
    resolve(loader, root, hash);
    loader.process(storage).unwrap();
    fetched(loader, hash, root_artifact);
    loader.process(storage).unwrap();
    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
    handle
}

fn begin_deletion(loader: &mut Loader<MockIo>, storage: &mut Storage, root: AssetUuid) {
    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(1),
        assets: vec![(root, distill_loader::AssetDeltaState::Deleted)],
        paths: Vec::new(),
    });
    loader.process(storage).unwrap();
    let (req, request_basis) = loader.io().resolve_for(root);
    loader.io_mut().push(IoEvent::Resolved {
        req,
        uuid: root,
        result: ResolveResult::Deleted { at: stamp(1) },
        basis: request_basis,
    });
    loader.process(storage).unwrap();
}

/// A deleted asset that comes back is published as `Changed`: the loader
/// takes it as a valid transition (no diagnostic) and re-resolves it.
#[test]
fn a_returning_asset_is_a_valid_transition_and_reloads() {
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
    fetched(&mut loader, hash, fetched_artifact.clone());
    loader.process(&mut storage).unwrap();
    begin_deletion(&mut loader, &mut storage, asset_uuid);
    assert_eq!(loader.status(&handle), LoadStatus::Dead);
    let (deleted_req, _) = loader.io().resolve_for(asset_uuid);

    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(2),
        assets: vec![(asset_uuid, distill_loader::AssetDeltaState::Changed)],
        paths: Vec::new(),
    });
    loader.process(&mut storage).unwrap();
    assert!(!loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ManifestTransition { .. }
    )));
    let (req, _) = loader.io().resolve_for(asset_uuid);
    assert_ne!(req, deleted_req, "the returning asset is resolved again");
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, fetched_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
}

#[test]
fn one_basis_resolve_fetch_and_fixup_commit_at_process_boundary() {
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
fn a_sweep_is_ended_at_the_io_when_abandoned_and_when_complete() {
    let token = ModuleEpochToken::new(1);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 1, &token);
    let asset_uuid = uuid(1);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();
    let (abandoned_req, _) = loader.io().resolve_for(asset_uuid);
    let ended = |loader: &Loader<MockIo>| {
        loader
            .io()
            .commands
            .iter()
            .filter(|command| matches!(command, Command::EndSweep(ended) if *ended == basis()))
            .count()
    };

    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(1),
        assets: vec![(uuid(2), distill_loader::AssetDeltaState::Changed)],
        paths: Vec::new(),
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(ended(&loader), 1, "the abandoned sweep was not ended");
    let (req, _) = loader.io().resolve_for(asset_uuid);
    assert_ne!(req, abandoned_req, "the next sweep resolves again");

    let (hash, fetched_artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, fetched_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
    assert_eq!(ended(&loader), 2, "the completed sweep was not ended");
}

#[test]
fn add_ref_during_a_live_sweep_joins_that_sweep() {
    let token = ModuleEpochToken::new(63);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 63, &token);
    let first_uuid = uuid(63);
    let second_uuid = uuid(64);
    let first = loader.add_ref::<A>(first_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let second = loader.add_ref::<B>(second_uuid).unwrap();
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.io().resolve_for(second_uuid).1, basis());

    let (first_hash, first_artifact) = artifact::<A>(first_uuid, &[]);
    let (second_hash, second_artifact) = artifact::<B>(second_uuid, &[]);
    resolve(&mut loader, first_uuid, first_hash);
    resolve(&mut loader, second_uuid, second_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, first_hash, first_artifact);
    fetched(&mut loader, second_hash, second_artifact);
    loader.process(&mut storage).unwrap();

    assert_eq!(loader.status(&first), LoadStatus::Loaded);
    assert_eq!(loader.status(&second), LoadStatus::Loaded);
    assert_eq!(storage.commits.len(), 2);
}

#[test]
fn equal_fetch_hashes_are_owned_by_asset_and_cannot_wedge_a_sweep() {
    let token = ModuleEpochToken::new(65);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 65, &token);
    let first_uuid = uuid(65);
    let second_uuid = uuid(66);
    let first = loader.add_ref::<A>(first_uuid).unwrap();
    let second = loader.add_ref::<B>(second_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let shared_hash = ContentHash([0xaa; 32]);
    resolve(&mut loader, first_uuid, shared_hash);
    resolve(&mut loader, second_uuid, shared_hash);
    loader.process(&mut storage).unwrap();
    let fetches = loader
        .io()
        .commands
        .iter()
        .filter_map(|command| match command {
            Command::Fetch(req, hash, request_basis) if *hash == shared_hash => {
                Some((*req, request_basis.clone()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(fetches.len(), 2);

    let (_, first_artifact) = artifact::<A>(first_uuid, &[]);
    let (_, second_artifact) = artifact::<B>(second_uuid, &[]);
    loader.io_mut().push(IoEvent::Fetched {
        req: fetches[0].0,
        content_hash: shared_hash,
        artifact: first_artifact,
        basis: fetches[0].1.clone(),
    });
    loader.io_mut().push(IoEvent::Fetched {
        req: fetches[1].0,
        content_hash: shared_hash,
        artifact: second_artifact,
        basis: fetches[1].1.clone(),
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.status(&first), LoadStatus::Unloaded);
    assert_eq!(loader.status(&second), LoadStatus::Unloaded);
    assert!(!loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::StaleCompletion(CompletionDisposition::Superseded)
    )));

    let later_uuid = uuid(67);
    let _later = loader.add_ref::<A>(later_uuid).unwrap();
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.io().resolve_for(later_uuid).1, basis());
}

#[test]
fn releasing_the_last_handle_removes_its_live_sweep_candidate() {
    let token = ModuleEpochToken::new(68);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 68, &token);
    let abandoned_uuid = uuid(68);
    let abandoned = loader.add_ref::<A>(abandoned_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    drop(abandoned);
    loader.process(&mut storage).unwrap();

    let later_uuid = uuid(69);
    let _later = loader.add_ref::<B>(later_uuid).unwrap();
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.io().resolve_for(later_uuid).1, basis());
}

#[test]
fn storage_repopulation_refetches_unchanged_content_with_stable_handle() {
    let token = ModuleEpochToken::new(60);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 60, &token);
    let asset_uuid = uuid(60);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let handle_id = handle.id();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (hash, first_artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, first_artifact);
    loader.process(&mut storage).unwrap();
    let first_adoption = storage.commits[0].1;
    let fetches_before = loader
        .io()
        .commands
        .iter()
        .filter(|command| matches!(command, Command::Fetch(..)))
        .count();

    loader.begin_storage_repopulation(&mut storage);
    assert_eq!(handle.id(), handle_id);
    assert_eq!(loader.status(&handle), LoadStatus::Unloaded);
    assert!(storage.values.is_empty());
    assert_eq!(
        loader.manifest_entry(asset_uuid).unwrap().state,
        ManifestState::Current { content_hash: hash }
    );

    loader.process(&mut storage).unwrap();
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(|command| matches!(command, Command::Fetch(..)))
            .count(),
        fetches_before + 1,
        "empty storage must bypass the unchanged-content fetch cutoff"
    );
    let (replayed_hash, replayed_artifact) = artifact::<A>(asset_uuid, &[]);
    assert_eq!(replayed_hash, hash);
    fetched(&mut loader, replayed_hash, replayed_artifact);
    loader.process(&mut storage).unwrap();

    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
    assert_eq!(storage.commits.len(), 2);
    assert!(storage.commits[1].1 > first_adoption);
    assert_eq!(storage.values.len(), 1);
}

#[test]
fn released_repopulation_handle_is_visible_to_engine_storage_cleanup() {
    let token = ModuleEpochToken::new(63);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 63, &token);
    let asset_uuid = uuid(63);
    let mut storage = Storage::default();
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    loader.process(&mut storage).unwrap();
    let (hash, first_artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, first_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
    let id = handle.id();
    assert!(loader.handle_is_live(id));
    loader.begin_storage_repopulation(&mut storage);
    assert!(
        loader.handle_is_live(id),
        "repopulation does not release a live handle"
    );
    drop(handle);
    loader.process(&mut storage).unwrap();
    assert!(
        !loader.handle_is_live(id),
        "storage can now retire the adoption-less GPU slot"
    );
}

#[test]
fn repeated_storage_repopulation_abandons_old_device_pending_uploads() {
    let token = ModuleEpochToken::new(61);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 61, &token);
    let asset_uuid = uuid(61);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (hash, first_artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, first_artifact);
    loader.process(&mut storage).unwrap();

    loader.begin_storage_repopulation(&mut storage);
    storage.pending_handles.insert(handle.id());
    loader.process(&mut storage).unwrap();
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    let (_, replayed_artifact) = artifact::<A>(asset_uuid, &[]);
    fetched(&mut loader, hash, replayed_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.tokens.len(), 1);
    assert_eq!(storage.values.len(), 1);
    assert_eq!(storage.commits.len(), 1);

    loader.begin_storage_repopulation(&mut storage);
    assert!(storage.tokens.is_empty());
    assert!(storage.values.is_empty());
    assert_eq!(loader.status(&handle), LoadStatus::Unloaded);
}

#[test]
fn storage_repopulation_requeues_a_first_load_abandoned_while_pending() {
    let token = ModuleEpochToken::new(62);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 62, &token);
    let asset_uuid = uuid(62);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    storage.pending_handles.insert(handle.id());
    loader.process(&mut storage).unwrap();

    let (hash, first_artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, first_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.tokens.len(), 1);
    assert!(storage.commits.is_empty());
    let resolves_before = loader
        .io()
        .commands
        .iter()
        .filter(|command| matches!(command, Command::Resolve(_, uuid, _) if *uuid == asset_uuid))
        .count();

    loader.begin_storage_repopulation(&mut storage);
    assert!(storage.tokens.is_empty());
    assert!(storage.values.is_empty());
    assert_eq!(loader.status(&handle), LoadStatus::Unloaded);
    loader.process(&mut storage).unwrap();
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(
                |command| matches!(command, Command::Resolve(_, uuid, _) if *uuid == asset_uuid)
            )
            .count(),
        resolves_before + 1
    );
}

#[test]
fn unchanged_delta_cuts_off_before_fetch_and_does_not_sweep_unrelated_assets() {
    let token = ModuleEpochToken::new(2);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 2, &token);
    let changed = uuid(2);
    let unrelated = uuid(3);
    let changed_handle = loader.add_ref::<A>(changed).unwrap();
    let unrelated_handle = loader.add_ref::<B>(unrelated).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (changed_hash, changed_artifact) = artifact::<A>(changed, &[]);
    let (unrelated_hash, unrelated_artifact) = artifact::<B>(unrelated, &[]);
    resolve(&mut loader, changed, changed_hash);
    resolve(&mut loader, unrelated, unrelated_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, changed_hash, changed_artifact);
    fetched(&mut loader, unrelated_hash, unrelated_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.updates.len(), 2);

    let resolve_count = |loader: &Loader<MockIo>, asset| {
        loader
            .io()
            .commands
            .iter()
            .filter(|command| matches!(command, Command::Resolve(_, uuid, _) if *uuid == asset))
            .count()
    };
    let fetches_before = loader
        .io()
        .commands
        .iter()
        .filter(|command| matches!(command, Command::Fetch(..)))
        .count();
    let changed_resolves_before = resolve_count(&loader, changed);
    let unrelated_resolves_before = resolve_count(&loader, unrelated);

    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(1),
        assets: vec![(changed, distill_loader::AssetDeltaState::Changed)],
        paths: Vec::new(),
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(resolve_count(&loader, changed), changed_resolves_before + 1);
    assert_eq!(
        resolve_count(&loader, unrelated),
        unrelated_resolves_before,
        "a disconnected held asset must not enter the affected sweep"
    );

    resolve(&mut loader, changed, changed_hash);
    loader.process(&mut storage).unwrap();
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(|command| matches!(command, Command::Fetch(..)))
            .count(),
        fetches_before,
        "a held content hash is an early cutoff before fetch"
    );
    assert_eq!(storage.updates.len(), 2);
    assert_eq!(storage.commits.len(), 2);
    assert_eq!(loader.status(&changed_handle), LoadStatus::Loaded);
    assert_eq!(loader.status(&unrelated_handle), LoadStatus::Loaded);

    let changed_resolves_before_drift = resolve_count(&loader, changed);
    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(2),
        assets: vec![(changed, distill_loader::AssetDeltaState::Changed)],
        paths: Vec::new(),
    });
    loader.process(&mut storage).unwrap();
    let (stale_req, stale_basis) = loader.io().resolve_for(changed);
    let fresh_basis = basis_with(10);
    loader.io_mut().basis = fresh_basis.clone();
    loader.io_mut().push(IoEvent::Resolved {
        req: stale_req,
        uuid: changed,
        result: ResolveResult::Drifted {
            input: distill_loader::DriftedInput::Asset(changed),
            current: stamp(3),
        },
        basis: stale_basis,
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(
        resolve_count(&loader, changed),
        changed_resolves_before_drift + 2
    );
    assert_eq!(loader.io().resolve_for(changed).1, fresh_basis);
    assert_eq!(
        resolve_count(&loader, unrelated),
        unrelated_resolves_before,
        "drift retry must preserve the affected sweep instead of dirtying every held asset"
    );
    assert!(!loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ComponentPoisoned { members, .. } if members.contains(&unrelated)
    )));
}

#[test]
fn strong_reference_cycle_is_rejected_before_any_member_is_adopted() {
    let token = ModuleEpochToken::new(40);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 40, &token);
    let a_uuid = uuid(40);
    let b_uuid = uuid(41);
    let _a = loader.add_ref::<A>(a_uuid).unwrap();
    let _b = loader.add_ref::<B>(b_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (a_hash, a_artifact) = artifact_with_edges::<A>(a_uuid, &[(b_uuid, B::TYPE_UUID)]);
    let (b_hash, b_artifact) = artifact_with_edges::<B>(b_uuid, &[(a_uuid, A::TYPE_UUID)]);
    resolve(&mut loader, a_uuid, a_hash);
    resolve(&mut loader, b_uuid, b_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, a_hash, a_artifact);
    fetched(&mut loader, b_hash, b_artifact);
    loader.process(&mut storage).unwrap();

    assert!(storage.updates.is_empty());
    assert!(loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::LoadCycle { cycle } if cycle == &vec![a_uuid, b_uuid, a_uuid]
    )));
}

#[test]
fn malformed_fetch_terminally_fails_its_candidate() {
    let token = ModuleEpochToken::new(31);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 31, &token);
    let asset_uuid = uuid(33);
    let _handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (hash, _) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(
        &mut loader,
        hash,
        FetchedArtifact {
            structural: Arc::from([0_u8]),
            blobs: Vec::new(),
            load_edges: Vec::new(),
            wire_layout: blob(Vec::new()),
        },
    );
    loader.process(&mut storage).unwrap();

    let diagnostics = loader.take_diagnostics();
    assert!(diagnostics
        .iter()
        .any(|diagnostic| matches!(diagnostic, LoaderDiagnostic::Artifact(_))));
    assert!(diagnostics.iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ComponentPoisoned { failures, .. }
            if failures.iter().any(|(_, failure)| format!("{failure:?}").contains("artifact"))
    )));
}

#[test]
fn typed_handle_rejects_an_artifact_with_another_terminal_type() {
    let token = ModuleEpochToken::new(36);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 36, &token);
    let asset_uuid = uuid(36);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (hash, artifact) = artifact::<B>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, artifact);
    loader.process(&mut storage).unwrap();

    assert_eq!(loader.status(&handle), LoadStatus::Unloaded);
    assert!(storage.updates.is_empty());
    assert!(loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::Artifact(message) if message.contains("handle terminal type mismatch")
    )));
}

#[test]
fn local_build_only_descriptor_rejects_runtime_artifact() {
    let token = ModuleEpochToken::new(35);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 35, &token);
    let asset_uuid = uuid(35);
    let _handle = loader.add_ref::<BuildOnly>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (hash, artifact) = artifact::<BuildOnly>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, artifact);
    loader.process(&mut storage).unwrap();

    assert!(loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::Artifact(message)
            if message.contains("local descriptor marks") && message.contains("build-only")
    )));
}

#[test]
fn fetch_request_error_terminally_fails_its_candidate() {
    let token = ModuleEpochToken::new(32);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 32, &token);
    let asset_uuid = uuid(34);
    let _handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (hash, _) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    let (req, request_basis) = loader.io().fetch_for(hash);
    loader.io_mut().push(IoEvent::RequestError {
        req,
        message: "fetch failed".into(),
        basis: request_basis,
    });
    loader.process(&mut storage).unwrap();

    let diagnostics = loader.take_diagnostics();
    assert!(diagnostics
        .iter()
        .any(|diagnostic| matches!(diagnostic, LoaderDiagnostic::Io(message) if message == "fetch failed")));
    assert!(diagnostics.iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ComponentPoisoned { failures, .. }
            if failures.iter().any(|(_, failure)| format!("{failure:?}").contains("fetch failed"))
    )));
}

#[test]
fn an_expired_snapshot_retries_the_round_a_bounded_number_of_times() {
    let token = ModuleEpochToken::new(61);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 61, &token);
    let asset_uuid = uuid(61);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();
    let (hash, _) = artifact::<A>(asset_uuid, &[]);

    // An expired snapshot at resolve, then a missing blob at fetch: each
    // restarts the round at a new snapshot.
    let sweeps = loader.io().sweeps;
    let (req, basis) = loader.io().resolve_for(asset_uuid);
    loader.io_mut().push(IoEvent::SnapshotExpired { req, basis });
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.io().sweeps, sweeps + 1);
    let (retry, _) = loader.io().resolve_for(asset_uuid);
    assert_ne!(retry, req);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    let (req, basis) = loader.io().fetch_for(hash);
    loader.io_mut().push(IoEvent::SnapshotExpired { req, basis });
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.io().sweeps, sweeps + 2);
    assert_eq!(loader.status(&handle), LoadStatus::Resolving);

    let (req, basis) = loader.io().resolve_for(asset_uuid);
    loader.io_mut().push(IoEvent::SnapshotExpired { req, basis });
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.io().sweeps, sweeps + 3);
    assert!(!loader.take_diagnostics().iter().any(
        |diagnostic| matches!(diagnostic, LoaderDiagnostic::ComponentPoisoned { .. })
    ));
    let (req, basis) = loader.io().resolve_for(asset_uuid);
    loader.io_mut().push(IoEvent::SnapshotExpired { req, basis });
    loader.process(&mut storage).unwrap();
    assert!(loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ComponentPoisoned { failures, .. }
            if failures.iter().any(|(_, failure)| format!("{failure:?}").contains("kept expiring"))
    )));
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

    let (a_hash, a_artifact) = artifact_with_edges::<A>(a_uuid, &[(b_uuid, B::TYPE_UUID)]);
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
fn failed_pending_storage_preserves_last_good_and_freezes_the_component() {
    let token = ModuleEpochToken::new(48);
    let mut io = mock_io();
    io.basis = IoBasis::Rpc { snapshot: stamp(1) };
    let mut loader = Loader::new(io);
    register(&mut loader, 48, &token);
    let asset_uuid = uuid(48);
    let dependency_uuid = uuid(49);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (old_hash, old_artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, old_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, old_hash, old_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.commits.len(), 1);

    storage.pending_handles.insert(handle.id());
    loader.io_mut().basis = IoBasis::Rpc { snapshot: stamp(2) };
    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(2),
        assets: vec![(asset_uuid, distill_loader::AssetDeltaState::Changed)],
        paths: Vec::new(),
    });
    loader.process(&mut storage).unwrap();
    let (new_hash, new_artifact) =
        artifact_with_edges::<A>(asset_uuid, &[(dependency_uuid, B::TYPE_UUID)]);
    resolve(&mut loader, asset_uuid, new_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, new_hash, new_artifact);
    loader.process(&mut storage).unwrap();
    loader.process(&mut storage).unwrap();
    let (dependency_hash, dependency_artifact) = artifact::<B>(dependency_uuid, &[]);
    resolve(&mut loader, dependency_uuid, dependency_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, dependency_hash, dependency_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.commits.len(), 1, "candidate component is pending");

    storage.pending_failure = Some(StorageError::Engine("upload failed".into()));
    loader.process(&mut storage).unwrap();

    assert_eq!(storage.commits.len(), 1);
    assert_eq!(loader.status(&handle), LoadStatus::Loaded);
    assert!(matches!(
        loader.manifest_entry(asset_uuid).map(|entry| &entry.state),
        Some(ManifestState::StaleLastGood {
            content_hash,
            built_from,
            ..
        }) if *content_hash == old_hash && *built_from == stamp(2)
    ));
    assert!(loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ComponentPoisoned { members, .. }
            if members.contains(&asset_uuid) && members.contains(&dependency_uuid)
    )));
}

#[test]
fn module_drain_cancels_pending_storage_before_reporting_complete() {
    let token = ModuleEpochToken::new(42);
    let epoch = GameModuleEpoch(42);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 42, &token);
    let asset_uuid = uuid(42);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    storage.pending_handles.insert(handle.id());
    loader.process(&mut storage).unwrap();

    let (hash, artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.values.len(), 1);
    assert_eq!(storage.tokens.len(), 1);

    loader.begin_module_drain(epoch).unwrap();
    assert!(!loader.drain_complete(epoch));

    loader.process(&mut storage).unwrap();
    assert!(loader.drain_complete(epoch));
    assert!(storage.values.is_empty());
    assert!(storage.tokens.is_empty());

    let polls_after_cancel = storage.polls;
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.polls, polls_after_cancel);
}

#[test]
fn failed_pending_cancellation_poisons_and_prevents_module_unload() {
    let token = ModuleEpochToken::new(43);
    let epoch = GameModuleEpoch(43);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 43, &token);
    let asset_uuid = uuid(43);
    let handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    storage.pending_handles.insert(handle.id());
    loader.process(&mut storage).unwrap();

    let (hash, artifact) = artifact::<A>(asset_uuid, &[]);
    resolve(&mut loader, asset_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, artifact);
    loader.process(&mut storage).unwrap();
    storage.fail_free.insert(handle.id());

    loader.begin_module_drain(epoch).unwrap();
    loader.process(&mut storage).unwrap();

    assert!(token.is_poisoned());
    assert!(!loader.drain_complete(epoch));
    assert_eq!(storage.values.len(), 1);
    assert_eq!(storage.tokens.len(), 1);
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
        reason: distill_loader::ReconnectReason::TargetDefinitionChanged,
    });
    loader.io_mut().push(IoEvent::PathResolved {
        req: old_req,
        path: "textures/main".into(),
        result: PathResolveResult::Resolved(uuid(4)),
        basis: old_basis,
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.target_binding_state(), TargetBindingState::Required);
    assert_eq!(loader.status(&handle), LoadStatus::Unloaded);

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
fn reconnect_missing_detaches_loaded_indirect_uuid_and_subscription() {
    let token = ModuleEpochToken::new(44);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 44, &token);
    let path = "textures/rebound";
    let old_uuid = uuid(44);
    let handle = loader.add_ref_indirect::<A>(path).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (path_req, path_basis) = loader.io().path_for(path);
    loader.io_mut().push(IoEvent::PathResolved {
        req: path_req,
        path: path.into(),
        result: PathResolveResult::Resolved(old_uuid),
        basis: path_basis,
    });
    loader.process(&mut storage).unwrap();
    let (hash, bytes) = artifact::<A>(old_uuid, &[]);
    resolve(&mut loader, old_uuid, hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, hash, bytes);
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.status(&handle), LoadStatus::Loaded);

    let old_resolve_count = loader
        .io()
        .commands
        .iter()
        .filter(|command| matches!(command, Command::Resolve(_, uuid, _) if *uuid == old_uuid))
        .count();
    loader.io_mut().push(IoEvent::ReconnectRequired {
        reason: distill_loader::ReconnectReason::StoreInstanceChanged,
    });
    loader.process(&mut storage).unwrap();
    loader.process(&mut storage).unwrap();
    let (new_path_req, new_path_basis) = loader.io().path_for(path);
    loader.io_mut().push(IoEvent::PathResolved {
        req: new_path_req,
        path: path.into(),
        result: PathResolveResult::Missing,
        basis: new_path_basis,
    });
    loader.process(&mut storage).unwrap();

    assert!(loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::Unsubscribe(uuid) if *uuid == old_uuid)));
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(|command| matches!(command, Command::Resolve(_, uuid, _) if *uuid == old_uuid))
            .count(),
        old_resolve_count,
        "the stale UUID must not be resolved again while its path says Missing"
    );
    assert_eq!(loader.status(&handle), LoadStatus::Unloaded);
}

#[test]
fn path_rebind_unsubscribes_old_uuid_before_subscribing_new_uuid() {
    let token = ModuleEpochToken::new(45);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 45, &token);
    let path = "textures/switch";
    let first = uuid(45);
    let second = uuid(46);
    let _handle = loader.add_ref_indirect::<A>(path).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (first_req, first_basis) = loader.io().path_for(path);
    loader.io_mut().push(IoEvent::PathResolved {
        req: first_req,
        path: path.into(),
        result: PathResolveResult::Resolved(first),
        basis: first_basis,
    });
    loader.process(&mut storage).unwrap();
    assert!(loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::Subscribe(uuid) if *uuid == first)));

    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(1),
        assets: Vec::new(),
        paths: vec![path.to_owned()],
    });
    loader.process(&mut storage).unwrap();
    let (second_req, second_basis) = loader.io().path_for(path);
    loader.io_mut().push(IoEvent::PathResolved {
        req: second_req,
        path: path.into(),
        result: PathResolveResult::Resolved(second),
        basis: second_basis,
    });
    loader.process(&mut storage).unwrap();

    let unsubscribe = loader
        .io()
        .commands
        .iter()
        .position(|command| matches!(command, Command::Unsubscribe(uuid) if *uuid == first))
        .expect("old UUID must be unsubscribed");
    let subscribe = loader
        .io()
        .commands
        .iter()
        .rposition(|command| matches!(command, Command::Subscribe(uuid) if *uuid == second))
        .expect("new UUID must be subscribed");
    assert!(unsubscribe < subscribe);
}

#[test]
fn path_delta_resolves_only_the_rebound_component() {
    let token = ModuleEpochToken::new(50);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 50, &token);
    let path = "textures/scoped";
    let path_uuid = uuid(50);
    let unrelated_uuid = uuid(51);
    let path_handle = loader.add_ref_indirect::<A>(path).unwrap();
    let unrelated_handle = loader.add_ref::<B>(unrelated_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    let (path_req, path_basis) = loader.io().path_for(path);
    loader.io_mut().push(IoEvent::PathResolved {
        req: path_req,
        path: path.into(),
        result: PathResolveResult::Resolved(path_uuid),
        basis: path_basis,
    });
    let (unrelated_hash, unrelated_artifact) = artifact::<B>(unrelated_uuid, &[]);
    resolve(&mut loader, unrelated_uuid, unrelated_hash);
    loader.process(&mut storage).unwrap();

    let (path_hash, path_artifact) = artifact::<A>(path_uuid, &[]);
    resolve(&mut loader, path_uuid, path_hash);
    fetched(&mut loader, unrelated_hash, unrelated_artifact);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, path_hash, path_artifact);
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.status(&path_handle), LoadStatus::Loaded);
    assert_eq!(loader.status(&unrelated_handle), LoadStatus::Loaded);

    let unrelated_resolves = loader
        .io()
        .commands
        .iter()
        .filter(
            |command| matches!(command, Command::Resolve(_, uuid, _) if *uuid == unrelated_uuid),
        )
        .count();
    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(3),
        assets: Vec::new(),
        paths: vec![path.to_owned()],
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(|command| {
                matches!(command, Command::Resolve(_, uuid, _) if *uuid == unrelated_uuid)
            })
            .count(),
        unrelated_resolves,
        "a path invalidation must not resolve a disconnected held asset"
    );

    let (rebind_req, rebind_basis) = loader.io().path_for(path);
    loader.io_mut().push(IoEvent::PathResolved {
        req: rebind_req,
        path: path.into(),
        result: PathResolveResult::Resolved(path_uuid),
        basis: rebind_basis,
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(|command| {
                matches!(command, Command::Resolve(_, uuid, _) if *uuid == unrelated_uuid)
            })
            .count(),
        unrelated_resolves
    );
}

#[test]
fn protocol_epoch_reconnect_fences_old_resolve_and_requires_target_binding() {
    let token = ModuleEpochToken::new(30);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 30, &token);
    let asset_uuid = uuid(32);
    let _handle = loader.add_ref::<A>(asset_uuid).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();
    let (old_req, old_basis) = loader.io().resolve_for(asset_uuid);

    loader.io_mut().push(IoEvent::ReconnectRequired {
        reason: distill_loader::ReconnectReason::ProtocolEpochChanged,
    });
    loader.io_mut().push(IoEvent::Resolved {
        req: old_req,
        uuid: asset_uuid,
        result: ResolveResult::Built {
            content_hash: ContentHash([3; 32]),
        },
        basis: old_basis,
    });
    loader.process(&mut storage).unwrap();

    assert_eq!(loader.target_binding_state(), TargetBindingState::Required);
    assert!(loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ReconnectRequired(distill_loader::ReconnectReason::ProtocolEpochChanged)
    )));
    loader.process(&mut storage).unwrap();
    assert_ne!(loader.io().resolve_for(asset_uuid).0, old_req);
}

#[test]
fn a_pipeline_epoch_change_is_reported_once_per_rebind() {
    let token = ModuleEpochToken::new(31);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 31, &token);
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();
    let reconnects = |loader: &mut Loader<MockIo>| {
        loader
            .take_diagnostics()
            .iter()
            .filter(|diagnostic| {
                matches!(
                    diagnostic,
                    LoaderDiagnostic::ReconnectRequired(
                        distill_loader::ReconnectReason::PipelineEpochChanged
                    )
                )
            })
            .count()
    };
    let _ = loader.take_diagnostics();

    // Every request in flight on the old binding fails with the same reason;
    // the mock binds the target again after them.
    for _ in 0..8 {
        loader.io_mut().push(IoEvent::ReconnectRequired {
            reason: distill_loader::ReconnectReason::PipelineEpochChanged,
        });
    }
    loader.process(&mut storage).unwrap();
    assert_eq!(reconnects(&mut loader), 1);
    // The rebind completes on a later pass.
    loader.process(&mut storage).unwrap();
    assert_eq!(loader.target_binding_state(), TargetBindingState::Bound);

    // The next epoch change, after the rebind, is reported again.
    loader.io_mut().push(IoEvent::ReconnectRequired {
        reason: distill_loader::ReconnectReason::PipelineEpochChanged,
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(reconnects(&mut loader), 1);
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
fn storage_update_failure_freezes_component_and_destroys_unsubmitted_values() {
    COUNTED_DROPS.store(0, Ordering::SeqCst);
    let token = ModuleEpochToken::new(46);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 46, &token);
    let first_uuid = uuid(46);
    let second_uuid = uuid(47);
    let first = loader.add_ref::<Counted>(first_uuid).unwrap();
    let second = loader.add_ref::<Counted>(second_uuid).unwrap();
    let mut storage = Storage {
        fail_update: Some((first.id(), GameModuleEpoch(46))),
        ..Storage::default()
    };
    loader.process(&mut storage).unwrap();

    let (first_hash, first_artifact) =
        artifact_with_edges::<Counted>(first_uuid, &[(second_uuid, Counted::TYPE_UUID)]);
    let (second_hash, second_artifact) = artifact::<Counted>(second_uuid, &[]);
    resolve(&mut loader, first_uuid, first_hash);
    resolve(&mut loader, second_uuid, second_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, first_hash, first_artifact);
    fetched(&mut loader, second_hash, second_artifact);
    loader.process(&mut storage).unwrap();

    assert_eq!(COUNTED_DROPS.load(Ordering::SeqCst), 2);
    assert!(storage.values.is_empty());
    assert!(storage.commits.is_empty());
    assert_eq!(loader.status(&first), LoadStatus::Unloaded);
    assert_eq!(loader.status(&second), LoadStatus::Unloaded);
    assert!(loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ComponentPoisoned { members, .. }
            if members == &vec![first_uuid, second_uuid]
    )));
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

#[test]
fn shared_subscription_owners_release_only_after_the_last_slot() {
    let token = ModuleEpochToken::new(7);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 7, &token);
    let asset = uuid(7);
    let path = "models/shared";
    let direct = loader.add_ref::<A>(asset).unwrap();
    let indirect_a = loader.add_ref_indirect::<A>(path).unwrap();
    let indirect_b = loader.add_ref_indirect::<B>(path).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();

    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(|command| matches!(command, Command::SubscribePath(value) if value == path))
            .count(),
        1,
        "one transport subscription serves both typed path slots"
    );
    let (req, request_basis) = loader.io().path_for(path);
    loader.io_mut().push(IoEvent::PathResolved {
        req,
        path: path.into(),
        result: PathResolveResult::Resolved(asset),
        basis: request_basis,
    });
    loader.process(&mut storage).unwrap();
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(
                |command| matches!(command, Command::Subscribe(candidate) if *candidate == asset)
            )
            .count(),
        1,
        "one transport subscription serves the direct and indirect slots"
    );

    drop(indirect_a);
    loader.process(&mut storage).unwrap();
    assert!(!loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::UnsubscribePath)));
    assert!(!loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::Unsubscribe(candidate) if *candidate == asset)));

    drop(indirect_b);
    loader.process(&mut storage).unwrap();
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(|command| matches!(command, Command::UnsubscribePath))
            .count(),
        1
    );
    assert!(!loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::Unsubscribe(candidate) if *candidate == asset)));

    drop(direct);
    loader.process(&mut storage).unwrap();
    assert_eq!(
        loader
            .io()
            .commands
            .iter()
            .filter(
                |command| matches!(command, Command::Unsubscribe(candidate) if *candidate == asset)
            )
            .count(),
        1
    );
}

#[test]
fn placeholder_strong_reference_expands_and_gates_the_component() {
    let token = ModuleEpochToken::new(20);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 20, &token);
    let root = uuid(20);
    let target = uuid(21);
    configure_placeholder(vec![(true, target, B::TYPE_UUID)], false);
    register_ref_placeholder(&mut loader, 20);
    let mut storage = Storage::default();
    let handle = load_placeholder_root(&mut loader, &mut storage, root);
    assert_eq!(storage.updates.len(), 1);
    begin_deletion(&mut loader, &mut storage, root);

    assert_eq!(storage.updates.len(), 1, "placeholder must not half-adopt");
    loader.process(&mut storage).unwrap();
    let (_, target_basis) = loader.io().resolve_for(target);
    assert_eq!(target_basis, basis());

    let (target_hash, target_artifact) = artifact::<B>(target, &[]);
    resolve(&mut loader, target, target_hash);
    loader.process(&mut storage).unwrap();
    assert_eq!(storage.updates.len(), 1, "fetch is still part of the gate");
    fetched(&mut loader, target_hash, target_artifact);
    loader.process(&mut storage).unwrap();

    assert_eq!(storage.updates.len(), 3);
    assert_eq!(storage.commits.len(), 3);
    assert_eq!(loader.status(&handle), LoadStatus::Dead);
}

#[test]
fn placeholder_weak_reference_does_not_expand_the_component() {
    let token = ModuleEpochToken::new(21);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 21, &token);
    let root = uuid(22);
    let weak_target = uuid(23);
    configure_placeholder(vec![(false, weak_target, B::TYPE_UUID)], false);
    register_ref_placeholder(&mut loader, 21);
    let mut storage = Storage::default();
    let _handle = load_placeholder_root(&mut loader, &mut storage, root);
    begin_deletion(&mut loader, &mut storage, root);

    assert_eq!(storage.updates.len(), 2);
    assert!(!loader.io().commands.iter().any(
        |command| matches!(command, Command::Resolve(_, candidate, _) if *candidate == weak_target)
    ));
}

#[test]
fn placeholder_terminal_type_mismatch_poisons_without_partial_swap() {
    let token = ModuleEpochToken::new(22);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 22, &token);
    let root = uuid(24);
    let target = uuid(25);
    configure_placeholder(vec![(true, target, A::TYPE_UUID)], false);
    register_ref_placeholder(&mut loader, 22);
    let mut storage = Storage::default();
    let _handle = load_placeholder_root(&mut loader, &mut storage, root);
    begin_deletion(&mut loader, &mut storage, root);
    loader.process(&mut storage).unwrap();
    let (target_hash, target_artifact) = artifact::<B>(target, &[]);
    resolve(&mut loader, target, target_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, target_hash, target_artifact);
    loader.process(&mut storage).unwrap();

    assert_eq!(storage.updates.len(), 1);
    assert!(loader.take_diagnostics().iter().any(|diagnostic| {
        matches!(diagnostic, LoaderDiagnostic::ComponentPoisoned { failures, .. }
            if failures.iter().any(|(_, failure)| format!("{failure:?}").contains("terminal type")))
    }));
}

#[test]
fn placeholder_resolution_failure_poisons_without_partial_swap() {
    let token = ModuleEpochToken::new(23);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 23, &token);
    let root = uuid(26);
    let target = uuid(27);
    configure_placeholder(vec![(true, target, B::TYPE_UUID)], false);
    register_ref_placeholder(&mut loader, 23);
    let mut storage = Storage::default();
    let _handle = load_placeholder_root(&mut loader, &mut storage, root);
    begin_deletion(&mut loader, &mut storage, root);
    loader.process(&mut storage).unwrap();
    let (req, request_basis) = loader.io().resolve_for(target);
    loader.io_mut().push(IoEvent::Resolved {
        req,
        uuid: target,
        result: ResolveResult::Failed {
            error: "placeholder target failed".into(),
        },
        basis: request_basis,
    });
    loader.process(&mut storage).unwrap();

    assert_eq!(storage.updates.len(), 1);
    let diagnostics = loader.take_diagnostics();
    assert!(diagnostics.iter().any(|diagnostic| {
        matches!(diagnostic, LoaderDiagnostic::ComponentPoisoned { failures, .. }
            if failures.iter().any(|(_, failure)| format!("{failure:?}").contains("placeholder target failed")))
    }), "{diagnostics:?}");
}

#[test]
fn role_ineligible_is_a_typed_failure_not_a_missing_asset() {
    let token = ModuleEpochToken::new(26);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 26, &token);
    let asset = uuid(30);
    let _handle = loader.add_ref::<A>(asset).unwrap();
    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();
    let (req, request_basis) = loader.io().resolve_for(asset);
    loader.io_mut().push(IoEvent::Resolved {
        req,
        uuid: asset,
        result: ResolveResult::RoleIneligible {
            uuid: asset,
            role: distill_build::trace::EntryRole::AuthoringOnly,
        },
        basis: request_basis,
    });

    loader.process(&mut storage).unwrap();

    assert!(loader.take_diagnostics().iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::RoleIneligible {
            uuid,
            role: distill_build::trace::EntryRole::AuthoringOnly,
        } if *uuid == asset
    )));
    assert!(storage.updates.is_empty());
}

#[test]
fn minted_placeholder_visitor_failure_poisons_the_component() {
    let token = ModuleEpochToken::new(26);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 26, &token);
    configure_placeholder(Vec::new(), true);
    register_ref_placeholder(&mut loader, 26);
    let root = uuid(31);
    let mut storage = Storage::default();
    let _handle = load_placeholder_root(&mut loader, &mut storage, root);

    begin_deletion(&mut loader, &mut storage, root);

    assert_eq!(storage.updates.len(), 1);
    assert!(token.is_poisoned());
    assert!(loader.take_diagnostics().iter().any(|diagnostic| {
        matches!(diagnostic, LoaderDiagnostic::ComponentPoisoned { failures, .. }
            if failures.iter().any(|(_, failure)| format!("{failure:?}").contains("placeholder visitor failed")))
    }));
}

#[test]
fn placeholder_edges_restart_and_resolve_as_one_fresh_basis() {
    const TARGET: AssetUuid = AssetUuid([30; 16]);
    let token = ModuleEpochToken::new(25);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 25, &token);
    configure_placeholder(vec![(true, TARGET, B::TYPE_UUID)], false);
    register_ref_placeholder(&mut loader, 25);
    let root = uuid(29);
    let mut storage = Storage::default();
    let _handle = load_placeholder_root(&mut loader, &mut storage, root);
    begin_deletion(&mut loader, &mut storage, root);
    loader.process(&mut storage).unwrap();
    let (old_req, old_basis) = loader.io().resolve_for(TARGET);

    let fresh_basis = basis_with(10);
    loader.io_mut().basis = fresh_basis.clone();
    loader.io_mut().push(IoEvent::Resolved {
        req: old_req,
        uuid: TARGET,
        result: ResolveResult::Drifted {
            input: distill_loader::DriftedInput::Asset(TARGET),
            current: stamp(2),
        },
        basis: old_basis,
    });
    loader.process(&mut storage).unwrap();

    assert_eq!(storage.updates.len(), 1);
    assert_eq!(loader.io().resolve_for(root).1, fresh_basis);
    assert_eq!(loader.io().resolve_for(TARGET).1, fresh_basis);

    let (root_req, root_basis) = loader.io().resolve_for(root);
    loader.io_mut().push(IoEvent::Resolved {
        req: root_req,
        uuid: root,
        result: ResolveResult::Deleted { at: stamp(2) },
        basis: root_basis,
    });
    let (target_hash, target_artifact) = artifact::<B>(TARGET, &[]);
    resolve(&mut loader, TARGET, target_hash);
    loader.process(&mut storage).unwrap();
    fetched(&mut loader, target_hash, target_artifact);
    loader.process(&mut storage).unwrap();

    assert_eq!(storage.updates.len(), 3);
    assert_eq!(storage.commits.len(), 3);
}

#[test]
fn named_refs_resolve_by_path_and_name_and_rebind_when_the_path_changes() {
    let token = ModuleEpochToken::new(47);
    let mut loader = Loader::new(mock_io());
    register(&mut loader, 47, &token);
    let path = "characters/fox.gltf.bundle";
    let walk = loader.add_ref_named::<A>(path, "Walk").unwrap();
    let run = loader.add_ref_named::<A>(path, "Run").unwrap();
    let skeleton = loader.add_ref_indirect::<A>(path).unwrap();
    assert_eq!(loader.add_ref_named::<A>(path, "Walk").unwrap().id(), walk.id());
    assert_ne!(walk.id(), run.id());
    assert_ne!(walk.id(), skeleton.id());

    let mut storage = Storage::default();
    loader.process(&mut storage).unwrap();
    // Every reference into the path shares one path subscription owner set.
    assert!(loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::SubscribePath(p) if p == path)));
    let (walk_req, walk_basis) = loader.io().named_for(path, "Walk").unwrap();
    let (run_req, run_basis) = loader.io().named_for(path, "Run").unwrap();
    let (skeleton_req, skeleton_basis) = loader.io().path_for(path);
    loader.io_mut().push(IoEvent::PathResolved {
        req: walk_req,
        path: AssetPath::named(path, "Walk"),
        result: PathResolveResult::Resolved(uuid(48)),
        basis: walk_basis,
    });
    loader.io_mut().push(IoEvent::PathResolved {
        req: run_req,
        path: AssetPath::named(path, "Run"),
        result: PathResolveResult::Missing,
        basis: run_basis,
    });
    loader.io_mut().push(IoEvent::PathResolved {
        req: skeleton_req,
        path: path.into(),
        result: PathResolveResult::Resolved(uuid(47)),
        basis: skeleton_basis,
    });
    loader.process(&mut storage).unwrap();
    for asset in [uuid(47), uuid(48)] {
        let (hash, bytes) = artifact::<A>(asset, &[]);
        resolve(&mut loader, asset, hash);
        loader.process(&mut storage).unwrap();
        fetched(&mut loader, hash, bytes);
        loader.process(&mut storage).unwrap();
    }
    assert_eq!(loader.status(&walk), LoadStatus::Loaded);
    assert_eq!(loader.status(&skeleton), LoadStatus::Loaded);
    assert_eq!(loader.status(&run), LoadStatus::Unloaded);

    // "Run" is imported later: the daemon announces the path, and the
    // missing name resolves again.
    loader.io_mut().push(IoEvent::Delta {
        stamp: stamp(1),
        assets: Vec::new(),
        paths: vec![path.to_owned()],
    });
    loader.process(&mut storage).unwrap();
    let (new_run_req, new_run_basis) = loader.io().named_for(path, "Run").unwrap();
    assert_ne!(new_run_req, run_req);
    loader.io_mut().push(IoEvent::PathResolved {
        req: new_run_req,
        path: AssetPath::named(path, "Run"),
        result: PathResolveResult::Resolved(uuid(49)),
        basis: new_run_basis,
    });
    loader.process(&mut storage).unwrap();
    assert!(loader
        .io()
        .commands
        .iter()
        .any(|command| matches!(command, Command::Resolve(_, asset, _) if *asset == uuid(49))));
}
