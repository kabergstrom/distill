use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, OnceLock};

use distill_asset::{AssetRuntimeDescriptor, AssetType, EncodeSink, ErasedValue, ModuleEpochToken};
use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_loader::{
    AdoptionId, AssetStorage, FetchedArtifact, GameModuleEpoch, HandleId, IoBasis, IoEvent,
    LoadStatus, Loader, LoaderDiagnostic, LoaderIO, ManifestHash, PathResolveResult, PendingState,
    PendingToken, RegistrationError, ReqId, ResolveResult, RuntimeTarget, StorageError,
    TargetBindingState, UpdateResult,
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
    ResolvePath(ReqId, String, IoBasis),
    Subscribe(AssetUuid),
    SubscribePath(String),
    Unsubscribe(AssetUuid),
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
        path: path.to_owned(),
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
        path: path.to_owned(),
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
        path: path.to_owned(),
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
        path: path.to_owned(),
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
