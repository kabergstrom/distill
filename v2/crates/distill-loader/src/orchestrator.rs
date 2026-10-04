//! Engine-thread loader orchestration.
//!
//! IO remains behind [`LoaderIO`]; this module owns request generations, the
//! client manifest, dependency-component decisions, and the frame-boundary
//! `update`/`poll`/`commit` protocol. Fetched bytes are parsed, validated, and
//! fixed up into owned [`ErasedValue`] instances here before atomic adoption.

use std::alloc::{alloc, dealloc, Layout};
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Weak};

use distill_asset::{
    AssetRuntimeDescriptor, AssetType, CallbackPanic, EncodeContainer, EncodeSink, ErasedValue,
    ModuleEpochPoisonCause, ModuleEpochToken, PlaceholderThunk,
};
use distill_core::id::{AssetUuid, ContentHash, TypeUuid};
use distill_store::state::SnapshotStamp;
use distill_wire::artifact::parse_artifact_parts;
use distill_wire::dswl::{decode_dswl, dswl_hash};
use distill_wire::exec::{execute_fixup, ExecEnv, ExecError, ExecLimits};
use distill_wire::native::validate_native_descriptor;
use distill_wire::plan::{compile_plans, CompiledPlans, PlanId};

use crate::component::{
    load_cycles, AdoptionDecision, CandidateAsset, CandidateOutcome, ComponentPlanner,
    MemberFailure,
};
use crate::io::{
    AssetPath, FetchedArtifact, IoEvent, LoaderIO, PathResolveResult, ReconnectReason,
    ResolveResult, RuntimeTarget,
};
use crate::runtime::{
    AdoptionId, CompletionDisposition, HandleId, ManifestEntry, ManifestState, OutstandingPurpose,
    OutstandingRequest, RequestOwner, RequestTracker,
};
use crate::stats::{AcceptParts, LoaderStats, Mark, Outcome};
use crate::storage::{
    AssetStorage, GameModuleEpoch, PendingState, PendingToken, RuntimeEpochError, RuntimeEpochs,
    StorageError, StoredAdoption, UpdateResult,
};
use crate::IoBasis;

/// Rounds retried at a new snapshot before the request fails.
const MAX_SWEEP_RETRIES: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadStatus {
    Unloaded,
    Resolving,
    LoadingDeps,
    Fetching,
    Loaded,
    Dead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetBindingState {
    Required,
    Bound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationError {
    DuplicateType(TypeUuid),
    DuplicatePlaceholder(TypeUuid),
    PlaceholderTypeMismatch,
    InvalidDescriptor { type_uuid: TypeUuid, detail: String },
    Epoch(RuntimeEpochError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoaderError {
    RequestIdsExhausted,
    AdoptionIdsExhausted,
    UnknownHandle(HandleId),
    TypeMismatch {
        expected: TypeUuid,
        actual: TypeUuid,
    },
    MissingDescriptor(TypeUuid),
    EpochFenced(GameModuleEpoch),
    OwnerEpochMismatch,
    PlaceholderHostCallbackFailed(TypeUuid),
    PlaceholderVisitorFailed(TypeUuid),
    RuntimeEpoch(RuntimeEpochError),
    Artifact(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoaderDiagnostic {
    StaleCompletion(CompletionDisposition),
    EventMismatch,
    Io(String),
    Artifact(String),
    ResolveFailed {
        uuid: AssetUuid,
        error: String,
    },
    RoleIneligible {
        uuid: AssetUuid,
        role: distill_build::trace::EntryRole,
    },
    ComponentPoisoned {
        members: Vec<AssetUuid>,
        failures: Vec<(AssetUuid, MemberFailure)>,
    },
    LoadCycle {
        cycle: Vec<AssetUuid>,
    },
    Storage {
        handle: HandleId,
        error: StorageError,
    },
    ReconnectRequired(ReconnectReason),
    /// Configuration keys the daemon needs a restart to apply; empty when a
    /// restart is no longer needed.
    RestartRequired(Vec<String>),
}

struct HandleLease;

pub struct Handle<T: AssetType> {
    id: HandleId,
    lease: Arc<HandleLease>,
    marker: PhantomData<fn() -> T>,
}

impl<T: AssetType> Handle<T> {
    pub fn id(&self) -> HandleId {
        self.id
    }
}

impl<T: AssetType> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            lease: Arc::clone(&self.lease),
            marker: PhantomData,
        }
    }
}

impl<T: AssetType> std::fmt::Debug for Handle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle").field("id", &self.id).finish()
    }
}

#[derive(Debug, Clone)]
enum Binding {
    Direct(AssetUuid),
    Indirect {
        path: AssetPath,
        resolved: Option<AssetUuid>,
    },
}

impl Binding {
    fn uuid(&self) -> Option<AssetUuid> {
        match self {
            Self::Direct(uuid) => Some(*uuid),
            Self::Indirect { resolved, .. } => *resolved,
        }
    }
}

#[derive(Debug, Clone)]
struct CurrentValue {
    type_uuid: TypeUuid,
    load_deps: Vec<AssetUuid>,
    adoption: Option<AdoptionId>,
    epoch: GameModuleEpoch,
}

struct Slot {
    lease: Weak<HandleLease>,
    internal_lease: Option<Arc<HandleLease>>,
    expected_type: Option<TypeUuid>,
    binding: Binding,
    subscribed_uuid: Option<AssetUuid>,
    path_subscribed: bool,
    status: LoadStatus,
    current: Option<CurrentValue>,
    /// When a path slot's resolve was asked for ([`LoaderStats`] ns; 0 once
    /// its timeline took it).
    requested_at: u64,
}

struct DescriptorRecord {
    descriptor: &'static AssetRuntimeDescriptor,
    epoch: GameModuleEpoch,
    token: ModuleEpochToken,
}

struct PlaceholderRecord {
    thunk: &'static PlaceholderThunk,
    epoch: GameModuleEpoch,
    token: ModuleEpochToken,
}

type PlaceholderReferences = BTreeMap<AssetUuid, BTreeSet<TypeUuid>>;

struct InspectedPlaceholder {
    value: ErasedValue,
    strong_references: PlaceholderReferences,
}

#[derive(Default)]
struct ReferenceOnlySink {
    strong_references: PlaceholderReferences,
}

impl EncodeSink for ReferenceOnlySink {
    fn flat(&mut self, _bytes: &[u8]) -> Result<(), CallbackPanic> {
        Ok(())
    }

    fn begin(&mut self, _kind: EncodeContainer, _len: u32) -> Result<(), CallbackPanic> {
        Ok(())
    }

    fn push(&mut self) -> Result<(), CallbackPanic> {
        Ok(())
    }

    fn finish(&mut self) -> Result<(), CallbackPanic> {
        Ok(())
    }

    fn blob(&mut self, _bytes: &[u8]) -> Result<(), CallbackPanic> {
        Ok(())
    }

    fn reference(
        &mut self,
        strong: bool,
        target: AssetUuid,
        expected_terminal: TypeUuid,
    ) -> Result<(), CallbackPanic> {
        if strong {
            self.strong_references
                .entry(target)
                .or_default()
                .insert(expected_terminal);
        }
        Ok(())
    }
}

/// Host-owned reverse-ABI adapter for generated encode visitors. Each sink
/// entry catches a panic before returning through module code and latches the
/// failure so a visitor cannot hide it by ignoring the returned status.
struct ContainedEncodeSink<'a> {
    inner: &'a mut dyn EncodeSink,
    failed: bool,
}

impl<'a> ContainedEncodeSink<'a> {
    fn new(inner: &'a mut dyn EncodeSink) -> Self {
        Self {
            inner,
            failed: false,
        }
    }

    fn failed(&self) -> bool {
        self.failed
    }

    fn call(
        &mut self,
        callback: impl FnOnce(&mut dyn EncodeSink) -> Result<(), CallbackPanic>,
    ) -> Result<(), CallbackPanic> {
        if self.failed {
            return Err(CallbackPanic);
        }
        match catch_unwind(AssertUnwindSafe(|| callback(self.inner))) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.failed = true;
                Err(error)
            }
            Err(_) => {
                self.failed = true;
                Err(CallbackPanic)
            }
        }
    }
}

impl EncodeSink for ContainedEncodeSink<'_> {
    fn flat(&mut self, bytes: &[u8]) -> Result<(), CallbackPanic> {
        self.call(|sink| sink.flat(bytes))
    }

    fn begin(&mut self, kind: EncodeContainer, len: u32) -> Result<(), CallbackPanic> {
        self.call(|sink| sink.begin(kind, len))
    }

    fn push(&mut self) -> Result<(), CallbackPanic> {
        self.call(|sink| sink.push())
    }

    fn finish(&mut self) -> Result<(), CallbackPanic> {
        self.call(|sink| sink.finish())
    }

    fn blob(&mut self, bytes: &[u8]) -> Result<(), CallbackPanic> {
        self.call(|sink| sink.blob(bytes))
    }

    fn reference(
        &mut self,
        strong: bool,
        target: AssetUuid,
        expected_terminal: TypeUuid,
    ) -> Result<(), CallbackPanic> {
        self.call(|sink| sink.reference(strong, target, expected_terminal))
    }
}

enum CandidateTerminal {
    Pending,
    Unchanged {
        content_hash: ContentHash,
        type_uuid: TypeUuid,
        load_deps: Vec<AssetUuid>,
    },
    Built {
        content_hash: ContentHash,
        fetched: bool,
        type_uuid: Option<TypeUuid>,
        load_deps: Option<Vec<AssetUuid>>,
        values: BTreeMap<HandleId, ErasedValue>,
    },
    Failed(String),
    Missing,
    Deleted {
        values: BTreeMap<HandleId, ErasedValue>,
        strong_references: PlaceholderReferences,
    },
}

struct CandidateRecord {
    basis: IoBasis,
    resolve_issued: bool,
    expected_terminal_types: BTreeSet<TypeUuid>,
    load_expectations: PlaceholderReferences,
    terminal: CandidateTerminal,
}

struct Sweep {
    basis: IoBasis,
    candidates: BTreeMap<AssetUuid, CandidateRecord>,
    pending_paths: BTreeSet<AssetPath>,
    dirty_seeds: BTreeSet<AssetUuid>,
}

struct PendingUpdate {
    handle: HandleId,
    uuid: AssetUuid,
    type_uuid: TypeUuid,
    content_hash: Option<ContentHash>,
    load_deps: Vec<AssetUuid>,
    owner_epoch: GameModuleEpoch,
    token: Option<PendingToken>,
    dead: bool,
}

struct PendingComponent {
    adoption: AdoptionId,
    updates: Vec<PendingUpdate>,
    members: Vec<AssetUuid>,
    basis: IoBasis,
}

enum StageComponentOutcome {
    Staged,
    Failed { uuid: AssetUuid, error: String },
}

pub struct Loader<I: LoaderIO> {
    io: I,
    requests: RequestTracker,
    epochs: RuntimeEpochs,
    descriptors: BTreeMap<TypeUuid, DescriptorRecord>,
    placeholders: BTreeMap<TypeUuid, PlaceholderRecord>,
    slots: BTreeMap<HandleId, Slot>,
    direct_slots: BTreeMap<(AssetUuid, TypeUuid), HandleId>,
    path_slots: BTreeMap<(AssetPath, TypeUuid), HandleId>,
    asset_subscription_owners: BTreeMap<AssetUuid, usize>,
    path_subscription_owners: BTreeMap<String, usize>,
    manifest: BTreeMap<AssetUuid, ManifestEntry>,
    dirty: BTreeSet<AssetUuid>,
    dirty_paths: BTreeSet<AssetPath>,
    sweep: Option<Sweep>,
    /// Rounds restarted since a sweep last completed.
    sweep_retries: u32,
    pending: Vec<PendingComponent>,
    diagnostics: Vec<LoaderDiagnostic>,
    next_handle: u64,
    next_adoption: u64,
    target_binding: TargetBindingState,
    /// Reason last reported since the target was last bound: the requests
    /// in flight on the old binding each fail with the same reason, and one
    /// diagnostic per rebind is enough.
    reported_reconnect: Option<ReconnectReason>,
    registered_target: Option<RuntimeTarget>,
    draining: BTreeSet<GameModuleEpoch>,
    stats: LoaderStats,
    /// Reused by the sweep-end timeline cleanup.
    stats_scratch: Vec<AssetUuid>,
}

impl<I: LoaderIO> Loader<I> {
    pub fn new(io: I) -> Self {
        Self {
            io,
            requests: RequestTracker::new(),
            epochs: RuntimeEpochs::default(),
            descriptors: BTreeMap::new(),
            placeholders: BTreeMap::new(),
            slots: BTreeMap::new(),
            direct_slots: BTreeMap::new(),
            path_slots: BTreeMap::new(),
            asset_subscription_owners: BTreeMap::new(),
            path_subscription_owners: BTreeMap::new(),
            manifest: BTreeMap::new(),
            dirty: BTreeSet::new(),
            dirty_paths: BTreeSet::new(),
            sweep: None,
            sweep_retries: 0,
            pending: Vec::new(),
            diagnostics: Vec::new(),
            next_handle: 1,
            next_adoption: 1,
            target_binding: TargetBindingState::Required,
            reported_reconnect: None,
            registered_target: None,
            draining: BTreeSet::new(),
            stats: LoaderStats::new(),
            stats_scratch: Vec::new(),
        }
    }

    /// Request-to-resident timelines and their aggregates (doc 22 phase 0).
    pub fn stats(&self) -> &LoaderStats {
        &self.stats
    }

    /// For [`LoaderStats::reset`] and [`LoaderStats::note_wakeup`].
    pub fn stats_mut(&mut self) -> &mut LoaderStats {
        &mut self.stats
    }

    pub fn io(&self) -> &I {
        &self.io
    }

    pub fn io_mut(&mut self) -> &mut I {
        &mut self.io
    }

    pub fn target_binding_state(&self) -> TargetBindingState {
        self.target_binding
    }

    pub fn register_types(
        &mut self,
        epoch: GameModuleEpoch,
        token: ModuleEpochToken,
        target_definition_hash: [u8; 32],
        descriptors: &[&'static AssetRuntimeDescriptor],
    ) -> Result<(), RegistrationError> {
        let mut seen = BTreeSet::new();
        for descriptor in descriptors {
            if !seen.insert(descriptor.type_uuid) {
                return Err(RegistrationError::DuplicateType(descriptor.type_uuid));
            }
            if self.descriptors.contains_key(&descriptor.type_uuid) {
                return Err(RegistrationError::DuplicateType(descriptor.type_uuid));
            }
            let size = u32::try_from(descriptor.size).map_err(|_| {
                RegistrationError::InvalidDescriptor {
                    type_uuid: descriptor.type_uuid,
                    detail: "native size exceeds u32".to_owned(),
                }
            })?;
            let align = u32::try_from(descriptor.align).map_err(|_| {
                RegistrationError::InvalidDescriptor {
                    type_uuid: descriptor.type_uuid,
                    detail: "native alignment exceeds u32".to_owned(),
                }
            })?;
            validate_native_descriptor(
                descriptor.native_layout,
                size,
                align,
                descriptor.ctors.entries.len(),
                descriptor.drops.entries.len(),
                descriptor.skip_writers.entries.len(),
            )
            .map_err(|error| RegistrationError::InvalidDescriptor {
                type_uuid: descriptor.type_uuid,
                detail: error.to_string(),
            })?;
        }
        self.epochs
            .register(epoch, token.clone(), descriptors.len())
            .map_err(RegistrationError::Epoch)?;
        for descriptor in descriptors {
            self.descriptors.insert(
                descriptor.type_uuid,
                DescriptorRecord {
                    descriptor,
                    epoch,
                    token: token.clone(),
                },
            );
        }
        self.registered_target = Some(RuntimeTarget::new(epoch, target_definition_hash));
        self.block_for_target_binding();
        Ok(())
    }

    pub fn register_placeholder(
        &mut self,
        epoch: GameModuleEpoch,
        thunk: &'static PlaceholderThunk,
    ) -> Result<(), RegistrationError> {
        let Some(descriptor) = self.descriptors.get(&thunk.type_uuid) else {
            return Err(RegistrationError::PlaceholderTypeMismatch);
        };
        if descriptor.epoch != epoch {
            return Err(RegistrationError::PlaceholderTypeMismatch);
        }
        if self.placeholders.contains_key(&thunk.type_uuid) {
            return Err(RegistrationError::DuplicatePlaceholder(thunk.type_uuid));
        }
        self.epochs
            .record_placeholder(epoch)
            .map_err(RegistrationError::Epoch)?;
        self.placeholders.insert(
            thunk.type_uuid,
            PlaceholderRecord {
                thunk,
                epoch,
                token: descriptor.token.clone(),
            },
        );
        Ok(())
    }

    pub fn add_ref<T: AssetType>(&mut self, uuid: AssetUuid) -> Result<Handle<T>, LoaderError> {
        self.ensure_descriptor(T::TYPE_UUID)?;
        if let Some(&id) = self.direct_slots.get(&(uuid, T::TYPE_UUID)) {
            if let Some(lease) = self.slots.get(&id).and_then(|slot| slot.lease.upgrade()) {
                return Ok(Handle {
                    id,
                    lease,
                    marker: PhantomData,
                });
            }
        }
        // A load dependency of a held asset already has an internal slot
        // (and its storage value): hand that slot out instead of loading the
        // asset a second time. The internal lease stays, as for any other
        // dependency slot.
        let internal = self
            .slots
            .iter()
            .find(|(_, slot)| {
                slot.expected_type.is_none()
                    && slot.internal_lease.is_some()
                    && matches!(slot.binding, Binding::Direct(bound) if bound == uuid)
                    && slot
                        .current
                        .as_ref()
                        .is_none_or(|current| current.type_uuid == T::TYPE_UUID)
            })
            .map(|(id, _)| *id);
        if let Some(id) = internal {
            let slot = self.slots.get_mut(&id).expect("found above");
            slot.expected_type = Some(T::TYPE_UUID);
            let lease = slot.internal_lease.clone().expect("checked above");
            self.direct_slots.insert((uuid, T::TYPE_UUID), id);
            return Ok(Handle {
                id,
                lease,
                marker: PhantomData,
            });
        }
        let (id, lease) = self.new_slot(Some(T::TYPE_UUID), Binding::Direct(uuid))?;
        self.direct_slots.insert((uuid, T::TYPE_UUID), id);
        self.manifest.entry(uuid).or_insert(ManifestEntry {
            state: ManifestState::Missing,
            adopted_at: AdoptionId(0),
        });
        self.stats.begin(uuid, self.stats.now());
        self.dirty.insert(uuid);
        self.add_to_live_sweep(BTreeSet::from([uuid]));
        Ok(Handle {
            id,
            lease,
            marker: PhantomData,
        })
    }

    /// The primary asset at `path`, late-bound: a later import that moves
    /// the path's primary rebinds the handle.
    pub fn add_ref_indirect<T: AssetType>(&mut self, path: &str) -> Result<Handle<T>, LoaderError> {
        self.add_ref_path(AssetPath::primary(path))
    }

    /// The asset named `name` (its importer's local id) among the assets
    /// imported at `path`, late-bound like [`Self::add_ref_indirect`]: a
    /// name that appears later binds then. Its type must be `T`.
    pub fn add_ref_named<T: AssetType>(
        &mut self,
        path: &str,
        name: &str,
    ) -> Result<Handle<T>, LoaderError> {
        self.add_ref_path(AssetPath::named(path, name))
    }

    fn add_ref_path<T: AssetType>(&mut self, path: AssetPath) -> Result<Handle<T>, LoaderError> {
        self.ensure_descriptor(T::TYPE_UUID)?;
        let key = (path.clone(), T::TYPE_UUID);
        if let Some(&id) = self.path_slots.get(&key) {
            if let Some(lease) = self.slots.get(&id).and_then(|slot| slot.lease.upgrade()) {
                return Ok(Handle {
                    id,
                    lease,
                    marker: PhantomData,
                });
            }
        }
        let (id, lease) = self.new_slot(
            Some(T::TYPE_UUID),
            Binding::Indirect {
                path: path.clone(),
                resolved: None,
            },
        )?;
        self.path_slots.insert(key, id);
        self.dirty_paths.insert(path);
        Ok(Handle {
            id,
            lease,
            marker: PhantomData,
        })
    }

    pub fn status<T: AssetType>(&self, handle: &Handle<T>) -> LoadStatus {
        self.slots
            .get(&handle.id)
            .filter(|slot| slot.expected_type == Some(T::TYPE_UUID))
            .map_or(LoadStatus::Unloaded, |slot| slot.status)
    }

    /// Whether an engine-storage handle still belongs to this loader. Storage
    /// may retain stable slots without an adoption during device repopulation.
    /// A slot disappears only after the loader has processed its last release.
    pub fn handle_is_live(&self, handle: HandleId) -> bool {
        self.slots.contains_key(&handle)
    }

    pub fn manifest_entry(&self, uuid: AssetUuid) -> Option<&ManifestEntry> {
        self.manifest.get(&uuid)
    }

    pub fn take_diagnostics(&mut self) -> Vec<LoaderDiagnostic> {
        std::mem::take(&mut self.diagnostics)
    }

    pub fn process(&mut self, storage: &mut dyn AssetStorage) -> Result<(), LoaderError> {
        self.stats.note_step();
        self.prune_released(storage);
        self.poll_pending(storage);
        self.requests.forget_cancelled();
        for event in self.io.poll() {
            self.handle_event(event, storage)?;
        }
        self.drain_epochs(storage)?;
        if self.target_binding == TargetBindingState::Required {
            return Ok(());
        }
        self.ensure_sweep()?;
        self.issue_sweep_requests()?;
        self.expand_dependencies()?;
        self.plan_and_stage(storage)?;
        Ok(())
    }

    /// Reset engine-local storage residency without changing handle or
    /// manifest identity. Any old-device uploads are abandoned, committed
    /// adoptions are freed, and every previously materialized dependency
    /// component is forced through resolve/fetch/adoption again.
    pub fn begin_storage_repopulation(&mut self, storage: &mut dyn AssetStorage) {
        let pending = std::mem::take(&mut self.pending);
        let pending_members = pending
            .iter()
            .flat_map(|component| component.members.iter().copied())
            .collect::<BTreeSet<_>>();
        for pending in pending {
            self.rollback_updates(storage, pending.adoption, &pending.updates);
        }
        self.dirty.extend(pending_members);
        self.restart_sweep();

        let materialized = self
            .slots
            .values()
            .filter(|slot| slot.current.is_some())
            .filter_map(|slot| slot.binding.uuid())
            .collect::<BTreeSet<_>>();
        self.dirty
            .extend(self.current_components_from(&materialized));

        let committed = self
            .slots
            .iter()
            .filter_map(|(handle, slot)| {
                let current = slot.current.as_ref()?;
                Some((*handle, current.type_uuid, current.adoption?, current.epoch))
            })
            .collect::<Vec<_>>();
        for (handle, type_uuid, adoption, epoch) in committed {
            let stored = StoredAdoption {
                type_uuid,
                handle,
                adoption,
            };
            match storage.free(type_uuid, handle, adoption) {
                Ok(()) => {
                    let _ = self.epochs.release_adoption(epoch, stored);
                    if let Some(current) = self
                        .slots
                        .get_mut(&handle)
                        .and_then(|slot| slot.current.as_mut())
                        .filter(|current| current.adoption == Some(adoption))
                    {
                        current.adoption = None;
                    }
                }
                Err(_) => self.poison_epoch(epoch),
            }
        }
        for slot in self.slots.values_mut() {
            slot.status = LoadStatus::Unloaded;
        }
    }

    pub fn begin_module_drain(&mut self, epoch: GameModuleEpoch) -> Result<(), LoaderError> {
        self.epochs
            .begin_module_drain(epoch)
            .map_err(LoaderError::RuntimeEpoch)?;
        self.draining.insert(epoch);
        self.descriptors.retain(|_, record| record.epoch != epoch);
        self.placeholders.retain(|_, record| record.epoch != epoch);
        self.block_for_target_binding();
        Ok(())
    }

    pub fn drain_complete(&self, epoch: GameModuleEpoch) -> bool {
        self.epochs.drain_complete(epoch)
    }

    fn ensure_descriptor(&self, type_uuid: TypeUuid) -> Result<&DescriptorRecord, LoaderError> {
        let record = self
            .descriptors
            .get(&type_uuid)
            .ok_or(LoaderError::MissingDescriptor(type_uuid))?;
        if !self.epochs.can_issue_work(record.epoch) {
            return Err(LoaderError::EpochFenced(record.epoch));
        }
        Ok(record)
    }

    fn validate_value_owner(
        &self,
        type_uuid: TypeUuid,
        value: &ErasedValue,
    ) -> Result<(), LoaderError> {
        if value.type_uuid() != type_uuid {
            return Err(LoaderError::TypeMismatch {
                expected: type_uuid,
                actual: value.type_uuid(),
            });
        }
        let record = self.ensure_descriptor(type_uuid)?;
        if !record.token.same_epoch(value.owner_token()) {
            return Err(LoaderError::OwnerEpochMismatch);
        }
        Ok(())
    }

    fn inspect_placeholder_value(
        &self,
        type_uuid: TypeUuid,
        value: &ErasedValue,
    ) -> Result<PlaceholderReferences, LoaderError> {
        self.validate_value_owner(type_uuid, value)?;
        let descriptor = self.ensure_descriptor(type_uuid)?;
        let mut sink = ReferenceOnlySink::default();
        let (visited, host_callback_failed) = {
            let mut contained = ContainedEncodeSink::new(&mut sink);
            // Safety: value ownership/type were checked against this descriptor;
            // encode is the descriptor's generated no-unwind visitor.
            let visited = catch_unwind(AssertUnwindSafe(|| unsafe {
                (descriptor.descriptor.encode)(value.as_ptr(), &mut contained)
            }));
            (visited, contained.failed())
        };
        if host_callback_failed {
            return Err(LoaderError::PlaceholderHostCallbackFailed(type_uuid));
        }
        match visited {
            Ok(Ok(())) => Ok(sink.strong_references),
            Ok(Err(_)) | Err(_) => {
                descriptor
                    .token
                    .poison_with(ModuleEpochPoisonCause::CallbackPanic);
                Err(LoaderError::PlaceholderVisitorFailed(type_uuid))
            }
        }
    }

    fn new_slot(
        &mut self,
        expected_type: Option<TypeUuid>,
        binding: Binding,
    ) -> Result<(HandleId, Arc<HandleLease>), LoaderError> {
        let id = HandleId(self.next_handle);
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or(LoaderError::RequestIdsExhausted)?;
        let lease = Arc::new(HandleLease);
        self.slots.insert(
            id,
            Slot {
                lease: Arc::downgrade(&lease),
                internal_lease: None,
                expected_type,
                binding,
                subscribed_uuid: None,
                path_subscribed: false,
                status: LoadStatus::Unloaded,
                current: None,
                requested_at: self.stats.now(),
            },
        );
        Ok((id, lease))
    }

    fn block_for_target_binding(&mut self) {
        self.target_binding = TargetBindingState::Required;
        let _ = self.requests.reconnect();
        self.abandon_sweep();
        if let Some(target) = self.registered_target.clone() {
            self.io.bind_target(target);
        }
    }

    /// Drop the live sweep. Its requests are cancelled at the IO (no answer
    /// arrives for them) and retired here; its unanswered path resolves are
    /// owed again by the next sweep.
    fn abandon_sweep(&mut self) {
        if let Some(sweep) = self.sweep.take() {
            self.dirty_paths.extend(sweep.pending_paths.iter().cloned());
            self.end_sweep(&sweep.basis);
            for candidate in sweep.candidates.into_values() {
                destroy_candidate_values(candidate.terminal);
            }
        }
    }

    fn end_sweep(&mut self, basis: &IoBasis) {
        self.io.end_sweep(basis);
        self.requests.retire_basis(basis);
    }

    /// A completion the tracker rejected. Answers to requests of a sweep
    /// this `process` ended are expected (the IO had already delivered
    /// them in this batch) and are dropped without a diagnostic.
    fn note_stale(&mut self, disposition: CompletionDisposition) {
        if disposition != CompletionDisposition::Cancelled {
            self.diagnostics
                .push(LoaderDiagnostic::StaleCompletion(disposition));
        }
    }

    fn prune_released(&mut self, storage: &mut dyn AssetStorage) {
        let released = self
            .slots
            .iter()
            .filter(|(_, slot)| slot.internal_lease.is_none() && slot.lease.upgrade().is_none())
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        let released_set = released.iter().copied().collect::<BTreeSet<_>>();
        self.cancel_pending_for_handles(storage, &released_set);
        self.prune_released_slots(storage, released);
    }

    fn cancel_pending_for_handles(
        &mut self,
        storage: &mut dyn AssetStorage,
        handles: &BTreeSet<HandleId>,
    ) {
        let mut pending_index = 0;
        while pending_index < self.pending.len() {
            if self.pending[pending_index]
                .updates
                .iter()
                .any(|update| handles.contains(&update.handle))
            {
                let pending = self.pending.remove(pending_index);
                self.rollback_updates(storage, pending.adoption, &pending.updates);
            } else {
                pending_index += 1;
            }
        }
    }

    fn detach_indirect_slots(
        &mut self,
        storage: &mut dyn AssetStorage,
        paths: Option<&BTreeSet<AssetPath>>,
    ) {
        let ids = self
            .slots
            .iter()
            .filter_map(|(id, slot)| match &slot.binding {
                Binding::Indirect { path, .. }
                    if paths.is_none_or(|selected| selected.contains(path)) =>
                {
                    Some(*id)
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        self.cancel_pending_for_handles(storage, &ids);
        let mut detached = BTreeSet::new();
        let mut unsubscribe = Vec::new();
        for id in ids {
            let slot = self.slots.get_mut(&id).expect("collected existing slot");
            if let Some(uuid) = slot.subscribed_uuid.take() {
                unsubscribe.push(uuid);
            }
            if let Binding::Indirect { resolved, .. } = &mut slot.binding {
                if let Some(uuid) = resolved.take() {
                    detached.insert(uuid);
                }
            }
            slot.status = LoadStatus::Unloaded;
        }
        for uuid in unsubscribe {
            self.release_asset_subscription(uuid);
        }
        for uuid in detached.iter().copied().collect::<Vec<_>>() {
            if self.handles_for_uuid(uuid).is_empty() {
                self.dirty.remove(&uuid);
                self.stats.finish(uuid, Outcome::Abandoned, self.stats.now());
                if let Some(candidate) = self
                    .sweep
                    .as_mut()
                    .and_then(|sweep| sweep.candidates.remove(&uuid))
                {
                    destroy_candidate_values(candidate.terminal);
                }
            }
        }
    }

    fn prune_released_slots(&mut self, storage: &mut dyn AssetStorage, released: Vec<HandleId>) {
        for id in released {
            if let Some(slot) = self.slots.remove(&id) {
                let bound_uuid = slot.binding.uuid();
                if let Some(uuid) = slot.subscribed_uuid {
                    self.release_asset_subscription(uuid);
                }
                if slot.path_subscribed {
                    if let Binding::Indirect { path, .. } = &slot.binding {
                        self.release_path_subscription(&path.path);
                    }
                }
                if let Some(current) = slot.current {
                    if let Some(adoption) = current.adoption {
                        let stored = StoredAdoption {
                            type_uuid: current.type_uuid,
                            handle: id,
                            adoption,
                        };
                        match storage.free(current.type_uuid, id, adoption) {
                            Ok(()) => {
                                let _ = self.epochs.release_adoption(current.epoch, stored);
                            }
                            Err(_) => self.poison_epoch(current.epoch),
                        }
                    }
                }
                self.direct_slots.retain(|_, value| *value != id);
                self.path_slots.retain(|_, value| *value != id);
                if let Some(uuid) = bound_uuid {
                    if self.handles_for_uuid(uuid).is_empty() {
                        self.dirty.remove(&uuid);
                        self.stats.finish(uuid, Outcome::Abandoned, self.stats.now());
                        if let Some(sweep) = &mut self.sweep {
                            sweep.dirty_seeds.remove(&uuid);
                            if let Some(candidate) = sweep.candidates.remove(&uuid) {
                                destroy_candidate_values(candidate.terminal);
                            }
                        }
                    }
                }
            }
        }
    }

    fn retain_asset_subscription(&mut self, uuid: AssetUuid) {
        let owners = self.asset_subscription_owners.entry(uuid).or_default();
        if *owners == 0 {
            self.io.subscribe(uuid);
        }
        *owners += 1;
    }

    fn release_asset_subscription(&mut self, uuid: AssetUuid) {
        let Some(owners) = self.asset_subscription_owners.get_mut(&uuid) else {
            debug_assert!(false, "asset subscription owner underflow");
            return;
        };
        *owners -= 1;
        if *owners == 0 {
            self.asset_subscription_owners.remove(&uuid);
            self.io.unsubscribe(uuid);
        }
    }

    fn retain_path_subscription(&mut self, path: &str) {
        let owners = self
            .path_subscription_owners
            .entry(path.to_owned())
            .or_default();
        if *owners == 0 {
            self.io.subscribe_path(path);
        }
        *owners += 1;
    }

    fn release_path_subscription(&mut self, path: &str) {
        let Some(owners) = self.path_subscription_owners.get_mut(path) else {
            debug_assert!(false, "path subscription owner underflow");
            return;
        };
        *owners -= 1;
        if *owners == 0 {
            self.path_subscription_owners.remove(path);
            self.io.unsubscribe_path(path);
        }
    }

    fn ensure_sweep(&mut self) -> Result<(), LoaderError> {
        if self.sweep.is_some() || (self.dirty.is_empty() && self.dirty_paths.is_empty()) {
            return Ok(());
        }
        let basis = self.io.begin_sweep();
        let affected = self.current_components_from(&self.dirty);
        let mut candidates = BTreeMap::new();
        let now = self.stats.now();
        for uuid in affected {
            self.stats.begin(uuid, now);
            candidates.insert(
                uuid,
                CandidateRecord {
                    basis: basis.clone(),
                    resolve_issued: false,
                    expected_terminal_types: BTreeSet::new(),
                    load_expectations: PlaceholderReferences::new(),
                    terminal: CandidateTerminal::Pending,
                },
            );
        }
        self.sweep = Some(Sweep {
            basis,
            candidates,
            pending_paths: self.dirty_paths.clone(),
            dirty_seeds: self.dirty.clone(),
        });
        Ok(())
    }

    fn add_to_live_sweep(&mut self, seeds: BTreeSet<AssetUuid>) {
        if self.sweep.is_none() {
            return;
        }
        let affected = self.current_components_from(&seeds);
        let sweep = self.sweep.as_mut().expect("checked above");
        let basis = sweep.basis.clone();
        sweep.dirty_seeds.extend(seeds);
        let now = self.stats.now();
        for uuid in affected {
            self.stats.begin(uuid, now);
            sweep.candidates.entry(uuid).or_insert(CandidateRecord {
                basis: basis.clone(),
                resolve_issued: false,
                expected_terminal_types: BTreeSet::new(),
                load_expectations: PlaceholderReferences::new(),
                terminal: CandidateTerminal::Pending,
            });
        }
    }

    fn issue_sweep_requests(&mut self) -> Result<(), LoaderError> {
        let Some(sweep) = &self.sweep else {
            return Ok(());
        };
        let basis = sweep.basis.clone();
        let paths = self.dirty_paths.iter().cloned().collect::<Vec<_>>();
        for path in paths {
            let req = self
                .requests
                .issue(
                    RequestOwner::Path(path.clone()),
                    OutstandingPurpose::ResolvePath,
                    basis.clone(),
                )
                .map_err(|_| LoaderError::RequestIdsExhausted)?;
            self.io.resolve_path(req, &path, &basis);
            // A re-resolve (a delta, a reconnect) times from here.
            let now = self.stats.now();
            for slot in self.slots.values_mut() {
                if slot.requested_at == 0
                    && matches!(&slot.binding, Binding::Indirect { path: bound, .. } if *bound == path)
                {
                    slot.requested_at = now;
                }
            }
            self.dirty_paths.remove(&path);
            // The sweep completes only once this answers; ending it earlier
            // would cancel the request.
            if let Some(sweep) = &mut self.sweep {
                sweep.pending_paths.insert(path);
            }
        }
        let pending = self
            .sweep
            .as_ref()
            .expect("checked above")
            .candidates
            .iter()
            .filter(|(_, candidate)| matches!(candidate.terminal, CandidateTerminal::Pending))
            .map(|(uuid, _)| *uuid)
            .collect::<Vec<_>>();
        for uuid in pending {
            let Some(handle) = self.handles_for_uuid(uuid).into_iter().next() else {
                continue;
            };
            if self
                .sweep
                .as_ref()
                .and_then(|sweep| sweep.candidates.get(&uuid))
                .is_some_and(|candidate| candidate.resolve_issued)
            {
                continue;
            }
            let req = self
                .requests
                .issue(
                    RequestOwner::Handle(handle),
                    OutstandingPurpose::Resolve,
                    basis.clone(),
                )
                .map_err(|_| LoaderError::RequestIdsExhausted)?;
            self.io.resolve(req, uuid, &basis);
            if let Some(candidate) = self
                .sweep
                .as_mut()
                .and_then(|sweep| sweep.candidates.get_mut(&uuid))
            {
                candidate.resolve_issued = true;
            }
            for handle in self.handles_for_uuid(uuid) {
                let mut subscribe = false;
                if let Some(slot) = self.slots.get_mut(&handle) {
                    slot.status = LoadStatus::Resolving;
                    if slot.subscribed_uuid != Some(uuid) {
                        slot.subscribed_uuid = Some(uuid);
                        subscribe = true;
                    }
                }
                if subscribe {
                    self.retain_asset_subscription(uuid);
                }
            }
        }
        let mut subscribe_paths = Vec::new();
        for slot in self.slots.values_mut() {
            if let Binding::Indirect { path, .. } = &slot.binding {
                if !slot.path_subscribed {
                    // A named asset's path changes when names appear there.
                    subscribe_paths.push(path.path.clone());
                    slot.path_subscribed = true;
                }
            }
        }
        for path in subscribe_paths {
            self.retain_path_subscription(&path);
        }
        Ok(())
    }

    fn handle_event(
        &mut self,
        event: IoEvent,
        storage: &mut dyn AssetStorage,
    ) -> Result<(), LoaderError> {
        match event {
            IoEvent::ReconnectRequired { reason } => {
                if self.reported_reconnect != Some(reason) {
                    self.reported_reconnect = Some(reason);
                    self.diagnostics
                        .push(LoaderDiagnostic::ReconnectRequired(reason));
                }
                for entry in self.manifest.values_mut() {
                    if let ManifestState::Current { content_hash } = entry.state {
                        entry.state = ManifestState::Invalidated { last: content_hash };
                    }
                }
                let paths = self
                    .slots
                    .values()
                    .filter_map(|slot| match &slot.binding {
                        Binding::Indirect { path, .. } => Some(path.clone()),
                        Binding::Direct(_) => None,
                    })
                    .collect::<BTreeSet<_>>();
                let held = self.held_uuids();
                self.detach_indirect_slots(storage, Some(&paths));
                self.dirty.extend(held);
                self.dirty_paths.extend(paths);
                self.block_for_target_binding();
            }
            IoEvent::TargetBound { target, basis: _ } => {
                if self.registered_target.as_ref() != Some(&target) {
                    self.diagnostics.push(LoaderDiagnostic::Io(format!(
                        "ignored stale runtime target binding for epoch {:?}",
                        target.epoch
                    )));
                    return Ok(());
                }
                self.target_binding = TargetBindingState::Bound;
                self.reported_reconnect = None;
            }
            IoEvent::TargetRejected { message } => {
                self.diagnostics.push(LoaderDiagnostic::Io(format!(
                    "runtime target binding failed: {message}"
                )));
            }
            IoEvent::Delta { assets, paths, .. } => {
                self.abandon_sweep();
                for (uuid, delta) in assets {
                    let entry = self.manifest.entry(uuid).or_insert(ManifestEntry {
                        state: ManifestState::Missing,
                        adopted_at: AdoptionId(0),
                    });
                    entry.apply_delta(delta);
                    self.stats.begin(uuid, self.stats.now());
                    self.dirty.insert(uuid);
                    if delta == crate::AssetDeltaState::Deleted {
                        for handle in self.handles_for_uuid(uuid) {
                            if let Some(slot) = self.slots.get_mut(&handle) {
                                if slot.current.is_some() {
                                    slot.status = LoadStatus::Dead;
                                }
                            }
                        }
                    }
                }
                // A changed path re-resolves every reference into it: its
                // primary and the names imported there.
                let changed = paths.into_iter().collect::<BTreeSet<_>>();
                let paths = self
                    .slots
                    .values()
                    .filter_map(|slot| match &slot.binding {
                        Binding::Indirect { path, .. } if changed.contains(&path.path) => {
                            Some(path.clone())
                        }
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                let old_path_assets = self
                    .slots
                    .values()
                    .filter_map(|slot| match &slot.binding {
                        Binding::Indirect { path, resolved } if paths.contains(path) => *resolved,
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                let old_components = self.current_components_from(&old_path_assets);
                self.detach_indirect_slots(storage, Some(&paths));
                self.dirty.extend(old_components);
                self.dirty_paths.extend(paths);
            }
            IoEvent::Resolved {
                req,
                uuid,
                result,
                basis,
            } => {
                let record = self.requests.outstanding(req).cloned();
                let disposition = self.requests.complete(req, &basis);
                if disposition != CompletionDisposition::Accepted {
                    self.note_stale(disposition);
                    return Ok(());
                }
                if !record.is_some_and(|record| {
                    record.purpose == OutstandingPurpose::Resolve
                        && matches!(record.owner, RequestOwner::Handle(handle) if self.slots.get(&handle).and_then(|slot| slot.binding.uuid()) == Some(uuid))
                }) {
                    self.diagnostics.push(LoaderDiagnostic::EventMismatch);
                    return Ok(());
                }
                self.accept_resolve(uuid, result, basis)?;
            }
            IoEvent::PathResolved {
                req,
                path,
                result,
                basis,
            } => {
                let record = self.requests.outstanding(req).cloned();
                let disposition = self.requests.complete(req, &basis);
                if disposition != CompletionDisposition::Accepted {
                    self.note_stale(disposition);
                    return Ok(());
                }
                if !record.is_some_and(|record| {
                    record.purpose == OutstandingPurpose::ResolvePath
                        && record.owner == RequestOwner::Path(path.clone())
                }) {
                    self.diagnostics.push(LoaderDiagnostic::EventMismatch);
                    return Ok(());
                }
                if let Some(sweep) = &mut self.sweep {
                    sweep.pending_paths.remove(&path);
                }
                self.accept_path(&path, result, storage);
            }
            IoEvent::Fetched {
                req,
                content_hash,
                artifact,
                basis,
            } => {
                let record = self.requests.outstanding(req).cloned();
                let disposition = self.requests.complete(req, &basis);
                if disposition != CompletionDisposition::Accepted {
                    self.note_stale(disposition);
                    return Ok(());
                }
                let Some(asset_uuid) = record.and_then(|record| match record {
                    OutstandingRequest {
                        purpose: OutstandingPurpose::Fetch,
                        owner: RequestOwner::Content { asset, hash },
                        ..
                    } if hash == content_hash => Some(asset),
                    _ => None,
                }) else {
                    self.diagnostics.push(LoaderDiagnostic::EventMismatch);
                    return Ok(());
                };
                self.stats
                    .fetched(asset_uuid, &artifact.timing, self.stats.now());
                self.accept_fetched(asset_uuid, content_hash, artifact, basis);
            }
            IoEvent::SnapshotExpired { req, basis } => {
                if self.sweep_retries >= MAX_SWEEP_RETRIES {
                    let message = "the daemon snapshot kept expiring".to_owned();
                    return self.handle_event(
                        IoEvent::RequestError {
                            req,
                            message,
                            basis,
                        },
                        storage,
                    );
                }
                let disposition = self.requests.complete(req, &basis);
                if disposition != CompletionDisposition::Accepted {
                    self.note_stale(disposition);
                    return Ok(());
                }
                // Retry the round at a new snapshot.
                if self
                    .sweep
                    .as_ref()
                    .is_some_and(|sweep| sweep.basis == basis)
                {
                    self.sweep_retries += 1;
                    self.restart_sweep();
                }
            }
            IoEvent::RequestError {
                req,
                message,
                basis,
            } => {
                let record = self.requests.outstanding(req).cloned();
                let disposition = self.requests.complete(req, &basis);
                if disposition != CompletionDisposition::Accepted {
                    self.note_stale(disposition);
                    return Ok(());
                }
                match record.map(|record| (record.purpose, record.owner)) {
                    Some((OutstandingPurpose::Resolve, RequestOwner::Handle(handle))) => {
                        if let Some(uuid) =
                            self.slots.get(&handle).and_then(|slot| slot.binding.uuid())
                        {
                            self.fail_candidate(uuid, &basis, message.clone());
                        }
                    }
                    Some((OutstandingPurpose::Fetch, RequestOwner::Content { asset, .. })) => {
                        self.fail_candidate(asset, &basis, message.clone());
                    }
                    Some((OutstandingPurpose::ResolvePath, RequestOwner::Path(path))) => {
                        if let Some(sweep) = &mut self.sweep {
                            sweep.pending_paths.remove(&path);
                        }
                        self.accept_path(
                            &path,
                            PathResolveResult::Failed {
                                error: message.clone(),
                            },
                            storage,
                        );
                    }
                    _ => self.diagnostics.push(LoaderDiagnostic::EventMismatch),
                }
                self.diagnostics.push(LoaderDiagnostic::Io(message));
            }
            IoEvent::ConnectionError { message } => {
                self.diagnostics.push(LoaderDiagnostic::Io(message));
            }
            IoEvent::RestartRequired { keys } => {
                self.diagnostics
                    .push(LoaderDiagnostic::RestartRequired(keys));
            }
        }
        Ok(())
    }

    fn accept_resolve(
        &mut self,
        uuid: AssetUuid,
        result: ResolveResult,
        event_basis: IoBasis,
    ) -> Result<(), LoaderError> {
        let was_live = self.handles_for_uuid(uuid).iter().any(|handle| {
            self.slots
                .get(handle)
                .is_some_and(|slot| slot.current.is_some())
        });
        let unchanged = match &result {
            ResolveResult::Built { content_hash } => self
                .manifest
                .get(&uuid)
                .and_then(|entry| last_good_hash(&entry.state))
                .filter(|last| last == content_hash)
                .and_then(|_| self.current_asset_shape(uuid)),
            _ => None,
        };
        let Some(candidate) = self
            .sweep
            .as_mut()
            .and_then(|sweep| sweep.candidates.get_mut(&uuid))
        else {
            return Ok(());
        };
        self.stats.mark(uuid, Mark::Resolved, self.stats.now());
        match result {
            ResolveResult::Built { content_hash } => {
                if candidate.basis != event_basis {
                    self.restart_sweep();
                    return Ok(());
                }
                if let Some((type_uuid, load_deps)) = unchanged {
                    candidate.terminal = CandidateTerminal::Unchanged {
                        content_hash,
                        type_uuid,
                        load_deps,
                    };
                    if let Some(entry) = self.manifest.get_mut(&uuid) {
                        entry.state = ManifestState::Current { content_hash };
                    }
                    for handle in self.handles_for_uuid(uuid) {
                        if let Some(slot) = self.slots.get_mut(&handle) {
                            slot.status = LoadStatus::Loaded;
                        }
                    }
                    return Ok(());
                }
                candidate.terminal = CandidateTerminal::Built {
                    content_hash,
                    fetched: false,
                    type_uuid: None,
                    load_deps: None,
                    values: BTreeMap::new(),
                };
                let req = self
                    .requests
                    .issue(
                        RequestOwner::Content {
                            asset: uuid,
                            hash: content_hash,
                        },
                        OutstandingPurpose::Fetch,
                        event_basis.clone(),
                    )
                    .map_err(|_| LoaderError::RequestIdsExhausted)?;
                self.io.fetch(req, content_hash, &event_basis);
                self.stats.mark(uuid, Mark::FetchIssued, self.stats.now());
                for handle in self.handles_for_uuid(uuid) {
                    if let Some(slot) = self.slots.get_mut(&handle) {
                        slot.status = LoadStatus::Fetching;
                    }
                }
            }
            ResolveResult::Drifted { .. } => self.restart_sweep(),
            ResolveResult::Failed { error } => {
                candidate.terminal = CandidateTerminal::Failed(error.clone());
                self.diagnostics
                    .push(LoaderDiagnostic::ResolveFailed { uuid, error });
            }
            ResolveResult::RoleIneligible {
                uuid: observed,
                role,
            } => {
                let error = format!("asset {observed} has non-runtime role {role:?}");
                candidate.terminal = CandidateTerminal::Failed(error);
                self.diagnostics.push(LoaderDiagnostic::RoleIneligible {
                    uuid: observed,
                    role,
                });
            }
            ResolveResult::Missing => {
                candidate.terminal = if was_live {
                    CandidateTerminal::Deleted {
                        values: BTreeMap::new(),
                        strong_references: BTreeMap::new(),
                    }
                } else {
                    CandidateTerminal::Missing
                };
                self.mint_placeholders(uuid);
            }
            ResolveResult::Deleted { .. } => {
                candidate.terminal = if was_live {
                    CandidateTerminal::Deleted {
                        values: BTreeMap::new(),
                        strong_references: BTreeMap::new(),
                    }
                } else {
                    CandidateTerminal::Missing
                };
                self.mint_placeholders(uuid);
            }
        }
        Ok(())
    }

    fn accept_path(
        &mut self,
        path: &AssetPath,
        result: PathResolveResult,
        storage: &mut dyn AssetStorage,
    ) {
        let ids = self
            .slots
            .iter()
            .filter_map(|(id, slot)| match &slot.binding {
                Binding::Indirect { path: value, .. } if value == path => Some(*id),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        self.cancel_pending_for_handles(storage, &ids);
        let mut retired = BTreeSet::new();
        let mut rebound = BTreeSet::new();
        let mut unsubscribe = Vec::new();
        let now = self.stats.now();
        for id in ids {
            let slot = self.slots.get_mut(&id).expect("collected existing slot");
            let requested_at = std::mem::take(&mut slot.requested_at);
            if let PathResolveResult::Resolved(uuid) = &result {
                let at = if requested_at == 0 { now } else { requested_at };
                self.stats.begin(*uuid, at);
                self.stats.mark(*uuid, Mark::PathResolved, now);
            }
            if let Some(uuid) = slot.subscribed_uuid.take() {
                unsubscribe.push(uuid);
            }
            if let Binding::Indirect { resolved, .. } = &mut slot.binding {
                if let Some(uuid) = resolved.take() {
                    retired.insert(uuid);
                }
            }
            match &result {
                PathResolveResult::Resolved(uuid) => {
                    if let Binding::Indirect { resolved, .. } = &mut slot.binding {
                        *resolved = Some(*uuid);
                    }
                    self.manifest.entry(*uuid).or_insert(ManifestEntry {
                        state: ManifestState::Missing,
                        adopted_at: AdoptionId(0),
                    });
                    self.dirty.insert(*uuid);
                    rebound.insert(*uuid);
                }
                PathResolveResult::Missing
                | PathResolveResult::Unsupported
                | PathResolveResult::Failed { .. } => {
                    slot.status = LoadStatus::Unloaded;
                }
            }
        }
        self.add_to_live_sweep(rebound);
        for uuid in unsubscribe {
            self.release_asset_subscription(uuid);
        }
        for uuid in retired {
            if self.handles_for_uuid(uuid).is_empty() {
                self.dirty.remove(&uuid);
                self.stats.finish(uuid, Outcome::Abandoned, self.stats.now());
                if let Some(candidate) = self
                    .sweep
                    .as_mut()
                    .and_then(|sweep| sweep.candidates.remove(&uuid))
                {
                    destroy_candidate_values(candidate.terminal);
                }
            }
        }
    }

    fn accept_fetched(
        &mut self,
        uuid: AssetUuid,
        content_hash: ContentHash,
        artifact: FetchedArtifact,
        basis: IoBasis,
    ) {
        let candidate_matches = self
            .sweep
            .as_ref()
            .and_then(|sweep| sweep.candidates.get(&uuid))
            .is_some_and(|candidate| {
                candidate.basis == basis
                    && matches!(
                        candidate.terminal,
                        CandidateTerminal::Built {
                            content_hash: expected,
                            ..
                        } if expected == content_hash
                    )
            });
        if !candidate_matches {
            self.diagnostics.push(LoaderDiagnostic::EventMismatch);
            return;
        }
        let t_begin = self.stats.now();
        let blob_bytes = artifact
            .blobs
            .iter()
            .map(|blob| blob.as_bytes())
            .collect::<Vec<_>>();
        let parsed = match parse_artifact_parts(&artifact.structural, &blob_bytes) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.reject_fetched(uuid, &basis, error.to_string());
                return;
            }
        };
        if parsed.asset_uuid != uuid {
            self.reject_fetched(
                uuid,
                &basis,
                "fetched artifact asset UUID does not match resolve".to_owned(),
            );
            return;
        }
        let type_uuid = parsed.terminal_type;
        let mut expected_terminal_types = self
            .sweep
            .as_ref()
            .and_then(|sweep| sweep.candidates.get(&uuid))
            .map(|candidate| candidate.expected_terminal_types.clone())
            .unwrap_or_default();
        expected_terminal_types.extend(
            self.handles_for_uuid(uuid)
                .into_iter()
                .filter_map(|handle| self.slots.get(&handle)?.expected_type),
        );
        if expected_terminal_types
            .iter()
            .any(|expected| *expected != type_uuid)
        {
            self.reject_fetched(
                uuid,
                &basis,
                format!(
                    "handle terminal type mismatch: expected {expected_terminal_types:?}, got {type_uuid}"
                ),
            );
            return;
        }
        let load_deps = parsed.load_deps.clone();
        let edge_assets = artifact
            .load_edges
            .iter()
            .map(|edge| edge.asset)
            .collect::<Vec<_>>();
        if artifact
            .load_edges
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
            || edge_assets != load_deps
        {
            self.reject_fetched(
                uuid,
                &basis,
                "direct typed load edges disagree with the artifact header".to_owned(),
            );
            return;
        }
        let load_expectations = artifact
            .load_edges
            .iter()
            .map(|edge| (edge.asset, BTreeSet::from([edge.expected_terminal])))
            .collect::<PlaceholderReferences>();
        if parsed.content_hash != content_hash {
            self.reject_fetched(
                uuid,
                &basis,
                "fetched artifact content hash does not match resolve".to_owned(),
            );
            return;
        }
        let descriptor = match self.descriptors.get(&type_uuid) {
            Some(record) if self.epochs.can_issue_work(record.epoch) => record,
            _ => {
                self.reject_fetched(
                    uuid,
                    &basis,
                    format!("no live descriptor for fetched terminal type {type_uuid}"),
                );
                return;
            }
        };
        if parsed.logical_hash != descriptor.descriptor.logical_hash {
            self.reject_fetched(
                uuid,
                &basis,
                format!("logical hash disagrees with descriptor for {type_uuid}"),
            );
            return;
        }
        if descriptor.descriptor.build_only {
            self.reject_fetched(
                uuid,
                &basis,
                format!("local descriptor marks {type_uuid} as build-only"),
            );
            return;
        }
        let t_checked = self.stats.now();
        let wire = match decode_dswl(artifact.wire_layout.as_bytes()) {
            Ok(wire) => wire,
            Err(error) => {
                self.reject_fetched(
                    uuid,
                    &basis,
                    format!("invalid DSWL for {type_uuid}: {error:?}"),
                );
                return;
            }
        };
        match dswl_hash(&wire) {
            Ok(hash) if hash == parsed.layout_hash => {}
            Ok(_) => {
                self.reject_fetched(
                    uuid,
                    &basis,
                    format!("DSWL hash disagrees with artifact header for {type_uuid}"),
                );
                return;
            }
            Err(error) => {
                self.reject_fetched(
                    uuid,
                    &basis,
                    format!("cannot hash DSWL for {type_uuid}: {error:?}"),
                );
                return;
            }
        }
        let t_dswl = self.stats.now();
        let plans = match compile_plans(&wire, descriptor.descriptor.native_layout) {
            Ok(plans) => plans,
            Err(error) => {
                self.reject_fetched(
                    uuid,
                    &basis,
                    format!("cannot compile fixup plan for {type_uuid}: {error}"),
                );
                return;
            }
        };
        let t_planned = self.stats.now();
        let handles = self.handles_for_uuid(uuid);
        let existing = self
            .sweep
            .as_ref()
            .and_then(|sweep| sweep.candidates.get(&uuid))
            .and_then(|candidate| match &candidate.terminal {
                CandidateTerminal::Built { values, .. } => {
                    Some(values.keys().copied().collect::<BTreeSet<_>>())
                }
                _ => None,
            })
            .unwrap_or_default();
        let mut constructed = BTreeMap::new();
        for handle in &handles {
            if existing.contains(handle) {
                continue;
            }
            match construct_value(
                descriptor.descriptor,
                &descriptor.token,
                &plans,
                parsed.fixed,
                parsed.variable,
                &artifact,
            ) {
                Ok(value) => {
                    constructed.insert(*handle, value);
                }
                Err(error) => {
                    for value in constructed.into_values() {
                        let _ = value.destroy();
                    }
                    self.diagnostics
                        .push(LoaderDiagnostic::Artifact(error.clone()));
                    if let Some(candidate) = self
                        .sweep
                        .as_mut()
                        .and_then(|sweep| sweep.candidates.get_mut(&uuid))
                    {
                        candidate.terminal = CandidateTerminal::Failed(error);
                    }
                    return;
                }
            }
        }
        let t_constructed = self.stats.now();
        // Header checks between parse and DSWL count as parse.
        let parts = AcceptParts {
            parse_ns: t_checked - t_begin,
            dswl_ns: t_dswl - t_checked,
            plan_ns: t_planned - t_dswl,
            construct_ns: t_constructed - t_planned,
        };
        self.stats.accepted(uuid, type_uuid, parts, t_constructed);
        let candidate = self
            .sweep
            .as_mut()
            .and_then(|sweep| sweep.candidates.get_mut(&uuid))
            .expect("fetched candidate was verified before value construction");
        let CandidateTerminal::Built {
            fetched,
            type_uuid: candidate_type,
            load_deps: candidate_deps,
            values,
            ..
        } = &mut candidate.terminal
        else {
            unreachable!("fetched candidate terminal was verified before value construction");
        };
        *fetched = true;
        *candidate_type = Some(type_uuid);
        *candidate_deps = Some(load_deps.clone());
        values.extend(constructed);
        candidate.load_expectations = load_expectations;
    }

    fn reject_fetched(&mut self, uuid: AssetUuid, basis: &IoBasis, message: String) {
        self.diagnostics
            .push(LoaderDiagnostic::Artifact(message.clone()));
        self.fail_candidate(uuid, basis, message);
    }

    fn fail_candidate(&mut self, uuid: AssetUuid, basis: &IoBasis, message: String) {
        if let Some(candidate) = self
            .sweep
            .as_mut()
            .and_then(|sweep| sweep.candidates.get_mut(&uuid))
            .filter(|candidate| &candidate.basis == basis)
        {
            candidate.terminal = CandidateTerminal::Failed(message);
        }
    }

    fn mint_placeholders(&mut self, uuid: AssetUuid) {
        let handles = self.handles_for_uuid(uuid);
        for handle in handles {
            let type_uuid = self
                .slots
                .get(&handle)
                .and_then(|slot| slot.current.as_ref().map(|current| current.type_uuid))
                .or_else(|| self.slots.get(&handle).and_then(|slot| slot.expected_type));
            let Some(type_uuid) = type_uuid else {
                continue;
            };
            let inspected = self.placeholders.get(&type_uuid).map(|placeholder| {
                if !self.epochs.can_issue_work(placeholder.epoch) {
                    return Err("placeholder epoch is fenced".to_owned());
                }
                let value = (placeholder.thunk.make)(placeholder.token.clone()).map_err(|_| {
                    placeholder
                        .token
                        .poison_with(ModuleEpochPoisonCause::CallbackPanic);
                    "placeholder factory callback failed".to_owned()
                })?;
                match self.inspect_placeholder_value(type_uuid, &value) {
                    Ok(strong_references) => Ok(InspectedPlaceholder {
                        value,
                        strong_references,
                    }),
                    Err(error) => {
                        let _ = value.destroy();
                        Err(format!("placeholder visitor failed: {error:?}"))
                    }
                }
            });
            match inspected {
                Some(Ok(inspected)) => {
                    if let Some(candidate) = self
                        .sweep
                        .as_mut()
                        .and_then(|sweep| sweep.candidates.get_mut(&uuid))
                    {
                        if let CandidateTerminal::Deleted {
                            values,
                            strong_references,
                        } = &mut candidate.terminal
                        {
                            merge_placeholder_references(
                                strong_references,
                                inspected.strong_references,
                            );
                            values.insert(handle, inspected.value);
                        } else {
                            let _ = inspected.value.destroy();
                        }
                    } else {
                        let _ = inspected.value.destroy();
                    }
                }
                Some(Err(error)) => {
                    self.fail_placeholder_candidate(uuid, error);
                    break;
                }
                None => {}
            }
        }
    }

    fn fail_placeholder_candidate(&mut self, uuid: AssetUuid, error: String) {
        let Some(candidate) = self
            .sweep
            .as_mut()
            .and_then(|sweep| sweep.candidates.get_mut(&uuid))
        else {
            return;
        };
        let terminal = std::mem::replace(&mut candidate.terminal, CandidateTerminal::Failed(error));
        destroy_candidate_values(terminal);
    }

    fn expand_dependencies(&mut self) -> Result<(), LoaderError> {
        let Some(sweep) = &self.sweep else {
            return Ok(());
        };
        let basis = sweep.basis.clone();
        let mut dependencies = PlaceholderReferences::new();
        for candidate in sweep.candidates.values() {
            match &candidate.terminal {
                CandidateTerminal::Built { .. } => merge_placeholder_references(
                    &mut dependencies,
                    candidate.load_expectations.clone(),
                ),
                CandidateTerminal::Deleted {
                    strong_references, ..
                } => merge_placeholder_references(&mut dependencies, strong_references.clone()),
                _ => {}
            }
        }
        let current = self.current_graph();
        let held = self.held_uuids();
        let mut affected = dependencies.keys().copied().collect::<BTreeSet<_>>();
        loop {
            let before = affected.len();
            for (parent, children) in &current {
                if affected.contains(parent)
                    || children.iter().any(|child| affected.contains(child))
                {
                    if held.contains(parent) {
                        affected.insert(*parent);
                    }
                    affected.extend(children.intersection(&held).copied());
                }
            }
            if affected.len() == before {
                break;
            }
        }
        for uuid in affected {
            let expected_terminal_types = dependencies.remove(&uuid).unwrap_or_default();
            if self.handles_for_uuid(uuid).is_empty() {
                let (id, lease) = self.new_slot(None, Binding::Direct(uuid))?;
                if let Some(slot) = self.slots.get_mut(&id) {
                    slot.internal_lease = Some(lease);
                }
                self.manifest.entry(uuid).or_insert(ManifestEntry {
                    state: ManifestState::Missing,
                    adopted_at: AdoptionId(0),
                });
            }
            let candidate = self
                .sweep
                .as_mut()
                .expect("checked Some")
                .candidates
                .entry(uuid)
                .or_insert(CandidateRecord {
                    basis: basis.clone(),
                    resolve_issued: false,
                    expected_terminal_types: BTreeSet::new(),
                    load_expectations: PlaceholderReferences::new(),
                    terminal: CandidateTerminal::Pending,
                });
            candidate
                .expected_terminal_types
                .extend(expected_terminal_types);
            self.stats.begin(uuid, self.stats.now());
        }
        Ok(())
    }

    fn plan_and_stage(&mut self, storage: &mut dyn AssetStorage) -> Result<(), LoaderError> {
        if !self.pending.is_empty() {
            return Ok(());
        }
        let Some(sweep) = &self.sweep else {
            return Ok(());
        };
        if !sweep.pending_paths.is_empty()
            || sweep
                .candidates
                .values()
                .any(|candidate| !self.candidate_complete(candidate))
        {
            return Ok(());
        }
        let held = sweep.candidates.keys().copied().collect::<BTreeSet<_>>();
        let current = self.current_graph();
        let graph = sweep
            .candidates
            .iter()
            .map(|(uuid, candidate)| (*uuid, self.candidate_deps(candidate)))
            .collect::<BTreeMap<_, _>>();
        let cycles = load_cycles(&graph);
        for cycle in cycles {
            let failed = cycle[0];
            self.fail_placeholder_candidate(
                failed,
                format!("strong-reference load cycle: {cycle:?}"),
            );
            self.diagnostics.push(LoaderDiagnostic::LoadCycle { cycle });
        }
        let candidates = self
            .sweep
            .as_ref()
            .expect("cycle rejection retains the sweep")
            .candidates
            .iter()
            .map(|(uuid, candidate)| {
                (
                    *uuid,
                    CandidateAsset {
                        uuid: *uuid,
                        basis: candidate.basis.clone(),
                        load_deps: self.candidate_deps(candidate),
                        outcome: self.candidate_outcome(*uuid, candidate),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let decisions = ComponentPlanner::new(current).plan(&held, &candidates);
        if decisions
            .iter()
            .any(|decision| matches!(decision, AdoptionDecision::Reresolve { .. }))
        {
            self.restart_sweep();
            return Ok(());
        }
        let mut sweep = self.sweep.take().expect("checked Some");
        // Every candidate and path answered: release the sweep's basis.
        self.end_sweep(&sweep.basis);
        self.sweep_retries = 0;
        let processed_dirty = std::mem::take(&mut sweep.dirty_seeds);
        for decision in decisions {
            match decision {
                AdoptionDecision::Ready { members, .. } => {
                    match self.stage_component(&mut sweep, &members, storage) {
                        Ok(StageComponentOutcome::Staged) => {}
                        Ok(StageComponentOutcome::Failed { uuid, error }) => {
                            let failures = vec![(uuid, MemberFailure::Failed(error))];
                            self.freeze_component(&mut sweep, &members, &failures);
                            self.diagnostics
                                .push(LoaderDiagnostic::ComponentPoisoned { members, failures });
                        }
                        Err(error) => {
                            destroy_sweep_values(sweep);
                            return Err(error);
                        }
                    }
                }
                AdoptionDecision::Poisoned { members, failures } => {
                    self.freeze_component(&mut sweep, &members, &failures);
                    self.diagnostics
                        .push(LoaderDiagnostic::ComponentPoisoned { members, failures });
                }
                AdoptionDecision::Reresolve { .. } => unreachable!("handled above"),
            }
        }
        for uuid in processed_dirty {
            self.dirty.remove(&uuid);
        }
        self.finish_orphan_timelines();
        Ok(())
    }

    /// The sweep ended: a timeline that is neither awaiting storage nor
    /// queued for the next sweep will not reach a terminal mark.
    fn finish_orphan_timelines(&mut self) {
        let mut scratch = std::mem::take(&mut self.stats_scratch);
        scratch.clear();
        scratch.extend(
            self.stats
                .active()
                .iter()
                .map(|timeline| timeline.uuid)
                .filter(|uuid| {
                    !self.dirty.contains(uuid)
                        && !self
                            .pending
                            .iter()
                            .any(|pending| pending.members.contains(uuid))
                }),
        );
        let now = self.stats.now();
        for uuid in scratch.drain(..) {
            self.stats.finish(uuid, Outcome::Abandoned, now);
        }
        self.stats_scratch = scratch;
    }

    fn candidate_complete(&self, candidate: &CandidateRecord) -> bool {
        match &candidate.terminal {
            CandidateTerminal::Pending => false,
            CandidateTerminal::Unchanged { .. } => true,
            CandidateTerminal::Built {
                fetched,
                type_uuid,
                load_deps,
                values,
                ..
            } => {
                *fetched
                    && type_uuid.is_some()
                    && load_deps.is_some()
                    && self
                        .handles_for_candidate(candidate)
                        .iter()
                        .all(|handle| values.contains_key(handle))
            }
            CandidateTerminal::Failed(_) | CandidateTerminal::Missing => true,
            CandidateTerminal::Deleted { values, .. } => {
                let handles = self.handles_for_candidate(candidate);
                values.is_empty() || handles.iter().all(|handle| values.contains_key(handle))
            }
        }
    }

    fn handles_for_candidate(&self, candidate: &CandidateRecord) -> Vec<HandleId> {
        self.sweep
            .as_ref()
            .and_then(|sweep| {
                sweep
                    .candidates
                    .iter()
                    .find(|(_, value)| std::ptr::eq(*value, candidate))
                    .map(|(uuid, _)| self.handles_for_uuid(*uuid))
            })
            .unwrap_or_default()
    }

    fn candidate_deps(&self, candidate: &CandidateRecord) -> Vec<AssetUuid> {
        match &candidate.terminal {
            CandidateTerminal::Unchanged { load_deps, .. } => load_deps.clone(),
            CandidateTerminal::Built {
                load_deps: Some(load_deps),
                ..
            } => load_deps.clone(),
            CandidateTerminal::Deleted {
                strong_references, ..
            } => strong_references.keys().copied().collect(),
            _ => Vec::new(),
        }
    }

    fn candidate_outcome(&self, uuid: AssetUuid, candidate: &CandidateRecord) -> CandidateOutcome {
        match &candidate.terminal {
            CandidateTerminal::Unchanged {
                type_uuid: actual, ..
            }
            | CandidateTerminal::Built {
                type_uuid: Some(actual),
                ..
            } if candidate
                .expected_terminal_types
                .iter()
                .any(|expected| expected != actual) =>
            {
                CandidateOutcome::Failed {
                    error: format!(
                        "placeholder reference terminal type mismatch: expected {:?}, got {actual}",
                        candidate.expected_terminal_types
                    ),
                }
            }
            CandidateTerminal::Unchanged { content_hash, .. }
            | CandidateTerminal::Built { content_hash, .. } => CandidateOutcome::Ready {
                content_hash: *content_hash,
            },
            CandidateTerminal::Failed(error) => CandidateOutcome::Failed {
                error: error.clone(),
            },
            CandidateTerminal::Missing => CandidateOutcome::Missing,
            CandidateTerminal::Deleted { values, .. } => CandidateOutcome::Deleted {
                placeholder_ready: !values.is_empty()
                    && self
                        .handles_for_uuid(uuid)
                        .iter()
                        .all(|handle| values.contains_key(handle)),
            },
            CandidateTerminal::Pending => unreachable!("completeness checked"),
        }
    }

    fn stage_component(
        &mut self,
        sweep: &mut Sweep,
        members: &[AssetUuid],
        storage: &mut dyn AssetStorage,
    ) -> Result<StageComponentOutcome, LoaderError> {
        let now = self.stats.now();
        for uuid in members {
            if sweep.candidates.get(uuid).is_some_and(|candidate| {
                matches!(candidate.terminal, CandidateTerminal::Unchanged { .. })
            }) {
                self.stats.finish(*uuid, Outcome::Unchanged, now);
            }
        }
        if members.iter().all(|uuid| {
            sweep.candidates.get(uuid).is_some_and(|candidate| {
                matches!(candidate.terminal, CandidateTerminal::Unchanged { .. })
            })
        }) {
            return Ok(StageComponentOutcome::Staged);
        }
        let adoption = self.mint_adoption()?;
        let mut updates = Vec::new();
        for uuid in members.iter().copied() {
            let Some(candidate) = sweep.candidates.get_mut(&uuid) else {
                continue;
            };
            match &mut candidate.terminal {
                CandidateTerminal::Built {
                    content_hash,
                    type_uuid: Some(type_uuid),
                    load_deps: Some(load_deps),
                    values,
                    ..
                } => {
                    for handle in self.handles_for_uuid(uuid) {
                        let value = values
                            .remove(&handle)
                            .expect("component readiness checked every handle value");
                        let epoch = match self.ensure_descriptor(*type_uuid) {
                            Ok(descriptor) => descriptor.epoch,
                            Err(error) => {
                                let _ = value.destroy();
                                self.rollback_updates(storage, adoption, &updates);
                                return Err(error);
                            }
                        };
                        let stored = StoredAdoption {
                            type_uuid: *type_uuid,
                            handle,
                            adoption,
                        };
                        if let Err(error) = self.epochs.record_adoption(epoch, stored) {
                            let _ = value.destroy();
                            self.rollback_updates(storage, adoption, &updates);
                            return Err(LoaderError::RuntimeEpoch(error));
                        }
                        let update_began = self.stats.now();
                        let update_result = storage.update(*type_uuid, handle, value, adoption);
                        let staging_ns = storage.take_upload_ns();
                        let update_ns = self.stats.now() - update_began;
                        self.stats.updated(uuid, update_began, update_ns, staging_ns);
                        let token = match update_result {
                            Ok(UpdateResult::Ready) => None,
                            Ok(UpdateResult::Pending(token)) => Some(token),
                            Err(error) => {
                                self.observe_storage_error(&error, epoch);
                                if matches!(error, StorageError::Engine(_)) {
                                    let _ = self.epochs.release_adoption(epoch, stored);
                                }
                                self.rollback_updates(storage, adoption, &updates);
                                self.diagnostics.push(LoaderDiagnostic::Storage {
                                    handle,
                                    error: error.clone(),
                                });
                                return Ok(StageComponentOutcome::Failed {
                                    uuid,
                                    error: format!("asset storage update failed: {error:?}"),
                                });
                            }
                        };
                        updates.push(PendingUpdate {
                            handle,
                            uuid,
                            type_uuid: *type_uuid,
                            content_hash: Some(*content_hash),
                            load_deps: load_deps.clone(),
                            owner_epoch: epoch,
                            token,
                            dead: false,
                        });
                    }
                }
                CandidateTerminal::Deleted {
                    values,
                    strong_references,
                } => {
                    for handle in self.handles_for_uuid(uuid) {
                        let Some(value) = values.remove(&handle) else {
                            continue;
                        };
                        let type_uuid = self
                            .slots
                            .get(&handle)
                            .and_then(|slot| slot.current.as_ref())
                            .map(|current| current.type_uuid)
                            .expect("placeholder applies only to a prior live value");
                        let epoch = match self.ensure_descriptor(type_uuid) {
                            Ok(descriptor) => descriptor.epoch,
                            Err(error) => {
                                let _ = value.destroy();
                                self.rollback_updates(storage, adoption, &updates);
                                return Err(error);
                            }
                        };
                        let stored = StoredAdoption {
                            type_uuid,
                            handle,
                            adoption,
                        };
                        if let Err(error) = self.epochs.record_adoption(epoch, stored) {
                            let _ = value.destroy();
                            self.rollback_updates(storage, adoption, &updates);
                            return Err(LoaderError::RuntimeEpoch(error));
                        }
                        let update_began = self.stats.now();
                        let update_result = storage.update(type_uuid, handle, value, adoption);
                        let staging_ns = storage.take_upload_ns();
                        let update_ns = self.stats.now() - update_began;
                        self.stats.updated(uuid, update_began, update_ns, staging_ns);
                        let token = match update_result {
                            Ok(UpdateResult::Ready) => None,
                            Ok(UpdateResult::Pending(token)) => Some(token),
                            Err(error) => {
                                self.observe_storage_error(&error, epoch);
                                if matches!(error, StorageError::Engine(_)) {
                                    let _ = self.epochs.release_adoption(epoch, stored);
                                }
                                self.rollback_updates(storage, adoption, &updates);
                                self.diagnostics.push(LoaderDiagnostic::Storage {
                                    handle,
                                    error: error.clone(),
                                });
                                return Ok(StageComponentOutcome::Failed {
                                    uuid,
                                    error: format!("asset storage update failed: {error:?}"),
                                });
                            }
                        };
                        updates.push(PendingUpdate {
                            handle,
                            uuid,
                            type_uuid,
                            content_hash: None,
                            load_deps: strong_references.keys().copied().collect(),
                            owner_epoch: epoch,
                            token,
                            dead: true,
                        });
                    }
                }
                CandidateTerminal::Pending
                | CandidateTerminal::Unchanged { .. }
                | CandidateTerminal::Failed(_)
                | CandidateTerminal::Missing
                | CandidateTerminal::Built { .. } => {}
            }
        }
        if updates.iter().all(|update| update.token.is_none()) {
            self.commit_updates(storage, adoption, updates);
        } else {
            self.pending.push(PendingComponent {
                adoption,
                updates,
                members: members.to_vec(),
                basis: sweep.basis.clone(),
            });
        }
        Ok(StageComponentOutcome::Staged)
    }

    fn poll_pending(&mut self, storage: &mut dyn AssetStorage) {
        let mut index = 0;
        while index < self.pending.len() {
            let mut failed = None;
            let mut all_ready = true;
            for update in &mut self.pending[index].updates {
                if let Some(token) = update.token {
                    match storage.poll(token) {
                        PendingState::Ready => update.token = None,
                        PendingState::Pending => all_ready = false,
                        PendingState::Failed(error) => {
                            failed = Some((update.handle, update.owner_epoch, error));
                            break;
                        }
                    }
                }
            }
            if let Some((handle, epoch, error)) = failed {
                self.observe_storage_error(&error, epoch);
                let pending = self.pending.remove(index);
                let failed_uuid = pending
                    .updates
                    .iter()
                    .find(|update| update.handle == handle)
                    .map(|update| update.uuid)
                    .or_else(|| pending.members.first().copied())
                    .expect("pending component has at least one member");
                self.rollback_updates(storage, pending.adoption, &pending.updates);
                let failures = vec![(
                    failed_uuid,
                    MemberFailure::Failed(format!("asset storage readiness failed: {error:?}")),
                )];
                self.apply_component_failure(&pending.members, &failures, &pending.basis);
                self.diagnostics
                    .push(LoaderDiagnostic::Storage { handle, error });
                self.diagnostics.push(LoaderDiagnostic::ComponentPoisoned {
                    members: pending.members,
                    failures,
                });
            } else if all_ready {
                let pending = self.pending.remove(index);
                self.commit_updates(storage, pending.adoption, pending.updates);
            } else {
                index += 1;
            }
        }
    }

    fn commit_updates(
        &mut self,
        storage: &mut dyn AssetStorage,
        adoption: AdoptionId,
        updates: Vec<PendingUpdate>,
    ) {
        for update in &updates {
            storage.commit(update.type_uuid, update.handle, adoption);
        }
        let now = self.stats.now();
        for update in &updates {
            self.stats.finish(update.uuid, Outcome::Resident, now);
        }
        for update in updates {
            let old = self
                .slots
                .get_mut(&update.handle)
                .and_then(|slot| slot.current.take());
            if let Some(old) = old {
                if let Some(old_adoption) = old.adoption {
                    let stored = StoredAdoption {
                        type_uuid: old.type_uuid,
                        handle: update.handle,
                        adoption: old_adoption,
                    };
                    match storage.free(old.type_uuid, update.handle, old_adoption) {
                        Ok(()) => {
                            let _ = self.epochs.release_adoption(old.epoch, stored);
                        }
                        Err(_) => self.poison_epoch(old.epoch),
                    }
                }
            }
            if let Some(slot) = self.slots.get_mut(&update.handle) {
                slot.current = Some(CurrentValue {
                    type_uuid: update.type_uuid,
                    load_deps: update.load_deps,
                    adoption: Some(adoption),
                    epoch: update.owner_epoch,
                });
                slot.status = if update.dead {
                    LoadStatus::Dead
                } else {
                    LoadStatus::Loaded
                };
            }
            let entry = self.manifest.entry(update.uuid).or_insert(ManifestEntry {
                state: ManifestState::Missing,
                adopted_at: AdoptionId(0),
            });
            entry.adopted_at = adoption;
            entry.state = match update.content_hash {
                Some(content_hash) => ManifestState::Current { content_hash },
                None => ManifestState::Dead,
            };
        }
    }

    fn rollback_updates(
        &mut self,
        storage: &mut dyn AssetStorage,
        adoption: AdoptionId,
        updates: &[PendingUpdate],
    ) {
        for update in updates {
            let stored = StoredAdoption {
                type_uuid: update.type_uuid,
                handle: update.handle,
                adoption,
            };
            match storage.free(update.type_uuid, update.handle, adoption) {
                Ok(()) => {
                    let _ = self.epochs.release_adoption(update.owner_epoch, stored);
                }
                Err(_) => self.poison_epoch(update.owner_epoch),
            }
        }
    }

    fn freeze_component(
        &mut self,
        sweep: &mut Sweep,
        members: &[AssetUuid],
        failures: &[(AssetUuid, MemberFailure)],
    ) {
        let basis = sweep.basis.clone();
        for uuid in members {
            if let Some(candidate) = sweep.candidates.remove(uuid) {
                destroy_candidate_values(candidate.terminal);
            }
        }
        self.apply_component_failure(members, failures, &basis);
    }

    fn apply_component_failure(
        &mut self,
        members: &[AssetUuid],
        failures: &[(AssetUuid, MemberFailure)],
        basis: &IoBasis,
    ) {
        let now = self.stats.now();
        for uuid in members {
            self.stats.finish(*uuid, Outcome::Failed, now);
        }
        for uuid in members {
            for handle in self.handles_for_uuid(*uuid) {
                if let Some(slot) = self.slots.get_mut(&handle) {
                    slot.status = if slot
                        .current
                        .as_ref()
                        .is_some_and(|current| current.adoption.is_some())
                    {
                        LoadStatus::Loaded
                    } else {
                        LoadStatus::Unloaded
                    };
                }
            }
        }
        for (uuid, failure) in failures {
            let entry = self.manifest.entry(*uuid).or_insert(ManifestEntry {
                state: ManifestState::Missing,
                adopted_at: AdoptionId(0),
            });
            match failure {
                MemberFailure::Failed(error) => {
                    if let Some(hash) = last_good_hash(&entry.state) {
                        if let Some(stamp) = basis.rpc_snapshot() {
                            entry.state = ManifestState::StaleLastGood {
                                content_hash: hash,
                                error: error.clone(),
                                built_from: stamp,
                            };
                        }
                    }
                }
                MemberFailure::Missing | MemberFailure::DeletedWithoutPlaceholder => {
                    entry.observe_absence();
                    for handle in self.handles_for_uuid(*uuid) {
                        if let Some(slot) = self.slots.get_mut(&handle) {
                            if slot.current.is_some() {
                                slot.status = LoadStatus::Dead;
                            }
                        }
                    }
                }
                MemberFailure::Unresolved => {}
            }
        }
    }

    fn restart_sweep(&mut self) {
        if let Some(sweep) = &self.sweep {
            self.dirty.extend(sweep.candidates.keys().copied());
        }
        self.abandon_sweep();
    }

    fn current_graph(&self) -> BTreeMap<AssetUuid, BTreeSet<AssetUuid>> {
        let mut graph = BTreeMap::new();
        for slot in self.slots.values() {
            if let (Some(uuid), Some(current)) = (slot.binding.uuid(), &slot.current) {
                graph
                    .entry(uuid)
                    .or_insert_with(BTreeSet::new)
                    .extend(current.load_deps.iter().copied());
            }
        }
        graph
    }

    fn current_components_from(&self, seeds: &BTreeSet<AssetUuid>) -> BTreeSet<AssetUuid> {
        let current = self.current_graph();
        let held = self.held_uuids();
        let mut affected = seeds.intersection(&held).copied().collect::<BTreeSet<_>>();
        loop {
            let before = affected.len();
            for (parent, children) in &current {
                if affected.contains(parent)
                    || children.iter().any(|child| affected.contains(child))
                {
                    if held.contains(parent) {
                        affected.insert(*parent);
                    }
                    affected.extend(children.intersection(&held).copied());
                }
            }
            if affected.len() == before {
                return affected;
            }
        }
    }

    fn current_asset_shape(&self, uuid: AssetUuid) -> Option<(TypeUuid, Vec<AssetUuid>)> {
        let handles = self.handles_for_uuid(uuid);
        if handles.is_empty() {
            return None;
        }
        let mut type_uuid = None;
        let mut load_deps = BTreeSet::new();
        for handle in handles {
            let current = self.slots.get(&handle)?.current.as_ref()?;
            if current.adoption.is_none() || !self.epochs.can_issue_work(current.epoch) {
                return None;
            }
            if type_uuid.is_some_and(|observed| observed != current.type_uuid) {
                return None;
            }
            type_uuid = Some(current.type_uuid);
            load_deps.extend(current.load_deps.iter().copied());
        }
        Some((type_uuid?, load_deps.into_iter().collect()))
    }

    fn held_uuids(&self) -> BTreeSet<AssetUuid> {
        self.slots
            .values()
            .filter(|slot| slot.internal_lease.is_some() || slot.lease.upgrade().is_some())
            .filter_map(|slot| slot.binding.uuid())
            .collect()
    }

    fn handles_for_uuid(&self, uuid: AssetUuid) -> Vec<HandleId> {
        self.slots
            .iter()
            .filter(|(_, slot)| {
                slot.binding.uuid() == Some(uuid)
                    && (slot.internal_lease.is_some() || slot.lease.upgrade().is_some())
            })
            .map(|(id, _)| *id)
            .collect()
    }

    fn observe_storage_error(&mut self, error: &StorageError, fallback: GameModuleEpoch) {
        match error {
            StorageError::Callback { owner_epoch, .. } => self.poison_epoch(*owner_epoch),
            StorageError::Engine(_) => {
                let _ = fallback;
            }
        }
    }

    fn poison_epoch(&mut self, epoch: GameModuleEpoch) {
        let _ = self.epochs.poison_callback(epoch);
        if let Some(record) = self
            .descriptors
            .values()
            .find(|record| record.epoch == epoch)
        {
            record
                .token
                .poison_with(ModuleEpochPoisonCause::CallbackPanic);
        } else if let Some(record) = self
            .placeholders
            .values()
            .find(|record| record.epoch == epoch)
        {
            record
                .token
                .poison_with(ModuleEpochPoisonCause::CallbackPanic);
        }
    }

    fn drain_epochs(&mut self, storage: &mut dyn AssetStorage) -> Result<(), LoaderError> {
        let epochs = self.draining.iter().copied().collect::<Vec<_>>();
        for epoch in epochs {
            let mut index = 0;
            while index < self.pending.len() {
                if self.pending[index]
                    .updates
                    .iter()
                    .any(|update| update.owner_epoch == epoch)
                {
                    let pending = self.pending.remove(index);
                    self.rollback_updates(storage, pending.adoption, &pending.updates);
                } else {
                    index += 1;
                }
            }
            let result = self.epochs.drain(epoch, storage);
            for slot in self.slots.values_mut() {
                if slot
                    .current
                    .as_ref()
                    .is_some_and(|current| current.epoch == epoch)
                {
                    slot.current = None;
                    slot.status = LoadStatus::Dead;
                }
            }
            match result {
                Ok(()) => {
                    if self.epochs.drain_complete(epoch) {
                        self.draining.remove(&epoch);
                    }
                }
                Err(RuntimeEpochError::FreeFailed { .. }) => {}
                Err(error) => return Err(LoaderError::RuntimeEpoch(error)),
            }
        }
        Ok(())
    }

    fn mint_adoption(&mut self) -> Result<AdoptionId, LoaderError> {
        let adoption = AdoptionId(self.next_adoption);
        self.next_adoption = self
            .next_adoption
            .checked_add(1)
            .ok_or(LoaderError::AdoptionIdsExhausted)?;
        Ok(adoption)
    }
}

fn destroy_candidate_values(terminal: CandidateTerminal) {
    let values = match terminal {
        CandidateTerminal::Built { values, .. } | CandidateTerminal::Deleted { values, .. } => {
            values
        }
        CandidateTerminal::Pending
        | CandidateTerminal::Unchanged { .. }
        | CandidateTerminal::Failed(_)
        | CandidateTerminal::Missing => {
            return;
        }
    };
    for value in values.into_values() {
        let _ = value.destroy();
    }
}

fn destroy_sweep_values(sweep: Sweep) {
    for candidate in sweep.candidates.into_values() {
        destroy_candidate_values(candidate.terminal);
    }
}

fn merge_placeholder_references(
    into: &mut PlaceholderReferences,
    references: PlaceholderReferences,
) {
    for (target, expected_types) in references {
        into.entry(target).or_default().extend(expected_types);
    }
}

fn construct_value(
    descriptor: &AssetRuntimeDescriptor,
    owner: &ModuleEpochToken,
    plans: &CompiledPlans,
    fixed: &[u8],
    variable: &[u8],
    artifact: &FetchedArtifact,
) -> Result<ErasedValue, String> {
    let root_meta = plans
        .metas
        .first()
        .ok_or_else(|| "compiled fixup plans have no root".to_owned())?;
    if root_meta.native_size as usize != descriptor.size
        || root_meta.native_align as usize != descriptor.align
    {
        return Err(format!(
            "root fixup geometry {}/{} does not match descriptor allocation {}/{}",
            root_meta.native_size, root_meta.native_align, descriptor.size, descriptor.align
        ));
    }
    let layout = Layout::from_size_align(descriptor.size.max(1), descriptor.align)
        .map_err(|_| "descriptor has an invalid native allocation layout".to_owned())?;
    // Safety: `layout` is non-zero and validated above. The pointer is used
    // only with the descriptor and plan compiled from that descriptor's native
    // tree, then deallocated with the identical layout on every exit path.
    let allocation = unsafe { alloc(layout) };
    if allocation.is_null() {
        return Err("native asset allocation failed".to_owned());
    }
    let env = ExecEnv {
        ctors: descriptor.ctors,
        drops: descriptor.drops,
        skips: descriptor.skip_writers,
        blobs: &artifact.blobs,
        limits: ExecLimits::default(),
    };
    // Safety: plan 0 is the compiled root; `allocation` has the descriptor's
    // size/alignment; tables and native tree come from the same descriptor.
    let fixed_up = unsafe { execute_fixup(plans, PlanId(0), fixed, variable, &env, allocation) };
    if let Err(error) = fixed_up {
        if matches!(error, ExecError::Callback { .. }) {
            owner.poison_with(ModuleEpochPoisonCause::CallbackPanic);
        }
        // Safety: execute_fixup guarantees the destination is uninitialized
        // again on failure, so only the raw allocation remains.
        unsafe { dealloc(allocation, layout) };
        return Err(format!("asset fixup failed: {error}"));
    }
    // Safety: successful fixup initialized the descriptor's true native type;
    // its generated finalizer moves that value into ErasedValue and leaves the
    // allocation uninitialized.
    let finalized = unsafe { (descriptor.finalize)(allocation, owner.clone()) };
    // Safety: finalize consumes the initialized value on success. Its
    // no-unwind status contract reports failure; that is a poisoned epoch and
    // the raw backing allocation itself is still released.
    unsafe { dealloc(allocation, layout) };
    match finalized {
        Ok(value) => Ok(value),
        Err(_) => {
            owner.poison_with(ModuleEpochPoisonCause::CallbackPanic);
            Err("asset finalizer callback failed".to_owned())
        }
    }
}

fn last_good_hash(state: &ManifestState) -> Option<ContentHash> {
    match state {
        ManifestState::Current { content_hash }
        | ManifestState::StaleLastGood { content_hash, .. } => Some(*content_hash),
        ManifestState::Invalidated { last } => Some(*last),
        ManifestState::Missing | ManifestState::Dead => None,
    }
}

#[allow(dead_code)]
fn _snapshot_for_failure(basis: &IoBasis) -> Option<SnapshotStamp> {
    basis.rpc_snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PanickingEncodeSink;

    impl EncodeSink for PanickingEncodeSink {
        fn flat(&mut self, _bytes: &[u8]) -> Result<(), CallbackPanic> {
            panic!("flat panicked")
        }

        fn begin(&mut self, _kind: EncodeContainer, _len: u32) -> Result<(), CallbackPanic> {
            panic!("begin panicked")
        }

        fn push(&mut self) -> Result<(), CallbackPanic> {
            panic!("push panicked")
        }

        fn finish(&mut self) -> Result<(), CallbackPanic> {
            panic!("finish panicked")
        }

        fn blob(&mut self, _bytes: &[u8]) -> Result<(), CallbackPanic> {
            panic!("blob panicked")
        }

        fn reference(
            &mut self,
            _strong: bool,
            _target: AssetUuid,
            _expected_terminal: TypeUuid,
        ) -> Result<(), CallbackPanic> {
            panic!("reference panicked")
        }
    }

    #[test]
    fn contained_encode_sink_contains_every_host_callback_entry() {
        fn assert_contained(
            callback: impl FnOnce(&mut ContainedEncodeSink<'_>) -> Result<(), CallbackPanic>,
        ) {
            let mut inner = PanickingEncodeSink;
            let mut sink = ContainedEncodeSink::new(&mut inner);
            assert_eq!(callback(&mut sink), Err(CallbackPanic));
            assert!(sink.failed());
        }

        assert_contained(|sink| sink.flat(&[1]));
        assert_contained(|sink| sink.begin(EncodeContainer::Vec, 1));
        assert_contained(|sink| sink.push());
        assert_contained(|sink| sink.finish());
        assert_contained(|sink| sink.blob(&[2]));
        assert_contained(|sink| sink.reference(true, AssetUuid([3; 16]), TypeUuid([4; 16])));
    }
}
