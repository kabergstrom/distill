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
    AssetRuntimeDescriptor, AssetType, EncodeContainer, EncodeSink, ErasedValue,
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
    AdoptionDecision, CandidateAsset, CandidateOutcome, ComponentPlanner, MemberFailure,
};
use crate::io::{
    FetchedArtifact, IoEvent, LoaderIO, PathResolveResult, ReconnectReason, ResolveResult,
    RuntimeTarget,
};
use crate::runtime::{
    AdoptionId, CompletionDisposition, HandleId, ManifestEntry, ManifestState, OutstandingPurpose,
    RequestOwner, RequestTracker,
};
use crate::storage::{
    AssetStorage, GameModuleEpoch, PendingState, PendingToken, RuntimeEpochError, RuntimeEpochs,
    StorageError, StoredAdoption, UpdateResult,
};
use crate::IoBasis;

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
    Storage {
        handle: HandleId,
        error: StorageError,
    },
    ReconnectRequired(ReconnectReason),
    ManifestTransition {
        uuid: AssetUuid,
    },
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
        path: String,
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
    adoption: AdoptionId,
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
    fn flat(&mut self, _bytes: &[u8]) {}

    fn begin(&mut self, _kind: EncodeContainer, _len: u32) {}

    fn push(&mut self) {}

    fn finish(&mut self) {}

    fn blob(&mut self, _bytes: &[u8]) {}

    fn reference(&mut self, strong: bool, target: AssetUuid, expected_terminal: TypeUuid) {
        if strong {
            self.strong_references
                .entry(target)
                .or_default()
                .insert(expected_terminal);
        }
    }
}

enum CandidateTerminal {
    Pending,
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
}

pub struct Loader<I: LoaderIO> {
    io: I,
    requests: RequestTracker,
    epochs: RuntimeEpochs,
    descriptors: BTreeMap<TypeUuid, DescriptorRecord>,
    placeholders: BTreeMap<TypeUuid, PlaceholderRecord>,
    slots: BTreeMap<HandleId, Slot>,
    direct_slots: BTreeMap<(AssetUuid, TypeUuid), HandleId>,
    path_slots: BTreeMap<(String, TypeUuid), HandleId>,
    manifest: BTreeMap<AssetUuid, ManifestEntry>,
    dirty: BTreeSet<AssetUuid>,
    dirty_paths: BTreeSet<String>,
    sweep: Option<Sweep>,
    pending: Vec<PendingComponent>,
    diagnostics: Vec<LoaderDiagnostic>,
    next_handle: u64,
    next_adoption: u64,
    target_binding: TargetBindingState,
    registered_target: Option<RuntimeTarget>,
    draining: BTreeSet<GameModuleEpoch>,
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
            manifest: BTreeMap::new(),
            dirty: BTreeSet::new(),
            dirty_paths: BTreeSet::new(),
            sweep: None,
            pending: Vec::new(),
            diagnostics: Vec::new(),
            next_handle: 1,
            next_adoption: 1,
            target_binding: TargetBindingState::Required,
            registered_target: None,
            draining: BTreeSet::new(),
        }
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
        let (id, lease) = self.new_slot(Some(T::TYPE_UUID), Binding::Direct(uuid))?;
        self.direct_slots.insert((uuid, T::TYPE_UUID), id);
        self.manifest.entry(uuid).or_insert(ManifestEntry {
            state: ManifestState::Missing,
            adopted_at: AdoptionId(0),
        });
        self.dirty.insert(uuid);
        Ok(Handle {
            id,
            lease,
            marker: PhantomData,
        })
    }

    pub fn add_ref_indirect<T: AssetType>(&mut self, path: &str) -> Result<Handle<T>, LoaderError> {
        self.ensure_descriptor(T::TYPE_UUID)?;
        let key = (path.to_owned(), T::TYPE_UUID);
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
                path: path.to_owned(),
                resolved: None,
            },
        )?;
        self.path_slots.insert(key, id);
        self.dirty_paths.insert(path.to_owned());
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

    pub fn manifest_entry(&self, uuid: AssetUuid) -> Option<&ManifestEntry> {
        self.manifest.get(&uuid)
    }

    pub fn take_diagnostics(&mut self) -> Vec<LoaderDiagnostic> {
        std::mem::take(&mut self.diagnostics)
    }

    pub fn process(&mut self, storage: &mut dyn AssetStorage) -> Result<(), LoaderError> {
        self.prune_released(storage);
        self.poll_pending(storage);
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
        // Safety: value ownership/type were checked against this descriptor;
        // encode is the descriptor's generated no-unwind visitor.
        let visited = catch_unwind(AssertUnwindSafe(|| unsafe {
            (descriptor.descriptor.encode)(value.as_ptr(), &mut sink)
        }));
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

    fn abandon_sweep(&mut self) {
        if let Some(sweep) = self.sweep.take() {
            for candidate in sweep.candidates.into_values() {
                destroy_candidate_values(candidate.terminal);
            }
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
        paths: Option<&BTreeSet<String>>,
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
            self.io.unsubscribe(uuid);
        }
        for uuid in detached.iter().copied().collect::<Vec<_>>() {
            if self.handles_for_uuid(uuid).is_empty() {
                self.dirty.remove(&uuid);
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
                if let Some(uuid) = slot.subscribed_uuid {
                    self.io.unsubscribe(uuid);
                }
                if slot.path_subscribed {
                    if let Binding::Indirect { path, .. } = &slot.binding {
                        self.io.unsubscribe_path(path);
                    }
                }
                if let Some(current) = slot.current {
                    let stored = StoredAdoption {
                        type_uuid: current.type_uuid,
                        handle: id,
                        adoption: current.adoption,
                    };
                    match storage.free(current.type_uuid, id, current.adoption) {
                        Ok(()) => {
                            let _ = self.epochs.release_adoption(current.epoch, stored);
                        }
                        Err(_) => self.poison_epoch(current.epoch),
                    }
                }
                self.direct_slots.retain(|_, value| *value != id);
                self.path_slots.retain(|_, value| *value != id);
            }
        }
    }

    fn ensure_sweep(&mut self) -> Result<(), LoaderError> {
        if self.sweep.is_some() || (self.dirty.is_empty() && self.dirty_paths.is_empty()) {
            return Ok(());
        }
        let basis = self.io.begin_sweep();
        let mut candidates = BTreeMap::new();
        for uuid in self.held_uuids() {
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
        self.sweep = Some(Sweep { basis, candidates });
        Ok(())
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
            self.dirty_paths.remove(&path);
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
                if let Some(slot) = self.slots.get_mut(&handle) {
                    slot.status = LoadStatus::Resolving;
                    if slot.subscribed_uuid != Some(uuid) {
                        self.io.subscribe(uuid);
                        slot.subscribed_uuid = Some(uuid);
                    }
                }
            }
        }
        for slot in self.slots.values_mut() {
            if let Binding::Indirect { path, .. } = &slot.binding {
                if !slot.path_subscribed {
                    self.io.subscribe_path(path);
                    slot.path_subscribed = true;
                }
            }
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
                self.diagnostics
                    .push(LoaderDiagnostic::ReconnectRequired(reason));
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
                self.detach_indirect_slots(storage, Some(&paths));
                self.dirty.extend(self.held_uuids());
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
                    if entry.apply_delta(delta).is_err() {
                        self.diagnostics
                            .push(LoaderDiagnostic::ManifestTransition { uuid });
                    }
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
                let paths = paths.into_iter().collect::<BTreeSet<_>>();
                self.detach_indirect_slots(storage, Some(&paths));
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
                    self.diagnostics
                        .push(LoaderDiagnostic::StaleCompletion(disposition));
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
                    self.diagnostics
                        .push(LoaderDiagnostic::StaleCompletion(disposition));
                    return Ok(());
                }
                if !record.is_some_and(|record| {
                    record.purpose == OutstandingPurpose::ResolvePath
                        && record.owner == RequestOwner::Path(path.clone())
                }) {
                    self.diagnostics.push(LoaderDiagnostic::EventMismatch);
                    return Ok(());
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
                    self.diagnostics
                        .push(LoaderDiagnostic::StaleCompletion(disposition));
                    return Ok(());
                }
                if !record.is_some_and(|record| {
                    record.purpose == OutstandingPurpose::Fetch
                        && record.owner == RequestOwner::Content(content_hash)
                }) {
                    self.diagnostics.push(LoaderDiagnostic::EventMismatch);
                    return Ok(());
                }
                self.accept_fetched(content_hash, artifact, basis);
            }
            IoEvent::RequestError {
                req,
                message,
                basis,
            } => {
                let record = self.requests.outstanding(req).cloned();
                let disposition = self.requests.complete(req, &basis);
                if disposition != CompletionDisposition::Accepted {
                    self.diagnostics
                        .push(LoaderDiagnostic::StaleCompletion(disposition));
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
                    Some((OutstandingPurpose::Fetch, RequestOwner::Content(content_hash))) => {
                        self.fail_content_candidate(content_hash, &basis, message.clone());
                    }
                    Some((OutstandingPurpose::ResolvePath, RequestOwner::Path(path))) => {
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
        let Some(candidate) = self
            .sweep
            .as_mut()
            .and_then(|sweep| sweep.candidates.get_mut(&uuid))
        else {
            return Ok(());
        };
        match result {
            ResolveResult::Built { content_hash } => {
                if candidate.basis != event_basis {
                    self.restart_sweep();
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
                        RequestOwner::Content(content_hash),
                        OutstandingPurpose::Fetch,
                        event_basis.clone(),
                    )
                    .map_err(|_| LoaderError::RequestIdsExhausted)?;
                self.io.fetch(req, content_hash, &event_basis);
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
        path: &str,
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
        let mut unsubscribe = Vec::new();
        for id in ids {
            let slot = self.slots.get_mut(&id).expect("collected existing slot");
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
                    if let Some(sweep) = &mut self.sweep {
                        sweep.candidates.entry(*uuid).or_insert(CandidateRecord {
                            basis: sweep.basis.clone(),
                            resolve_issued: false,
                            expected_terminal_types: BTreeSet::new(),
                            load_expectations: PlaceholderReferences::new(),
                            terminal: CandidateTerminal::Pending,
                        });
                    }
                }
                PathResolveResult::Missing
                | PathResolveResult::Unsupported
                | PathResolveResult::Failed { .. } => {
                    slot.status = LoadStatus::Unloaded;
                }
            }
        }
        for uuid in unsubscribe {
            self.io.unsubscribe(uuid);
        }
        for uuid in retired {
            if self.handles_for_uuid(uuid).is_empty() {
                self.dirty.remove(&uuid);
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
        content_hash: ContentHash,
        artifact: FetchedArtifact,
        basis: IoBasis,
    ) {
        let Some(uuid) = self.content_candidate_uuid(content_hash, &basis) else {
            self.diagnostics.push(LoaderDiagnostic::EventMismatch);
            return;
        };
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
        let wire = match decode_dswl(&artifact.wire_layout) {
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
        let Some(candidate) = self
            .sweep
            .as_mut()
            .and_then(|sweep| sweep.candidates.get_mut(&uuid))
        else {
            return;
        };
        if candidate.basis != basis {
            self.restart_sweep();
            return;
        }
        let CandidateTerminal::Built {
            content_hash: expected,
            fetched,
            type_uuid: candidate_type,
            load_deps: candidate_deps,
            values,
        } = &mut candidate.terminal
        else {
            return;
        };
        if *expected != content_hash {
            self.diagnostics.push(LoaderDiagnostic::EventMismatch);
            return;
        }
        *fetched = true;
        *candidate_type = Some(type_uuid);
        *candidate_deps = Some(load_deps.clone());
        values.extend(constructed);
        candidate.load_expectations = load_expectations;
    }

    fn content_candidate_uuid(
        &self,
        content_hash: ContentHash,
        basis: &IoBasis,
    ) -> Option<AssetUuid> {
        self.sweep
            .as_ref()?
            .candidates
            .iter()
            .find_map(|(uuid, candidate)| {
                if &candidate.basis != basis {
                    return None;
                }
                match &candidate.terminal {
                    CandidateTerminal::Built {
                        content_hash: expected,
                        ..
                    } if *expected == content_hash => Some(*uuid),
                    _ => None,
                }
            })
    }

    fn reject_fetched(&mut self, uuid: AssetUuid, basis: &IoBasis, message: String) {
        self.diagnostics
            .push(LoaderDiagnostic::Artifact(message.clone()));
        self.fail_candidate(uuid, basis, message);
    }

    fn fail_content_candidate(
        &mut self,
        content_hash: ContentHash,
        basis: &IoBasis,
        message: String,
    ) {
        if let Some(uuid) = self.content_candidate_uuid(content_hash, basis) {
            self.fail_candidate(uuid, basis, message);
        }
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
        for (uuid, expected_terminal_types) in dependencies {
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
        if sweep
            .candidates
            .values()
            .any(|candidate| !self.candidate_complete(candidate))
        {
            return Ok(());
        }
        let held = self.held_uuids();
        let current = self.current_graph();
        let candidates = sweep
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
        for decision in decisions {
            match decision {
                AdoptionDecision::Ready { members, .. } => {
                    self.stage_component(&mut sweep, members, storage)?;
                }
                AdoptionDecision::Poisoned { members, failures } => {
                    self.freeze_component(&mut sweep, &members, &failures);
                    self.diagnostics
                        .push(LoaderDiagnostic::ComponentPoisoned { members, failures });
                }
                AdoptionDecision::Reresolve { .. } => unreachable!("handled above"),
            }
        }
        self.dirty.clear();
        Ok(())
    }

    fn candidate_complete(&self, candidate: &CandidateRecord) -> bool {
        match &candidate.terminal {
            CandidateTerminal::Pending => false,
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
            CandidateTerminal::Built {
                load_deps: Some(value),
                ..
            } => value.clone(),
            CandidateTerminal::Deleted {
                strong_references, ..
            } => strong_references.keys().copied().collect(),
            _ => Vec::new(),
        }
    }

    fn candidate_outcome(&self, uuid: AssetUuid, candidate: &CandidateRecord) -> CandidateOutcome {
        match &candidate.terminal {
            CandidateTerminal::Built {
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
            CandidateTerminal::Built { content_hash, .. } => CandidateOutcome::Ready {
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
        members: Vec<AssetUuid>,
        storage: &mut dyn AssetStorage,
    ) -> Result<(), LoaderError> {
        let adoption = self.mint_adoption()?;
        let mut updates = Vec::new();
        for uuid in members {
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
                        let epoch = self.ensure_descriptor(*type_uuid)?.epoch;
                        let stored = StoredAdoption {
                            type_uuid: *type_uuid,
                            handle,
                            adoption,
                        };
                        if let Err(error) = self.epochs.record_adoption(epoch, stored) {
                            let _ = value.destroy();
                            return Err(LoaderError::RuntimeEpoch(error));
                        }
                        let token = match storage.update(*type_uuid, handle, value, adoption) {
                            Ok(UpdateResult::Ready) => None,
                            Ok(UpdateResult::Pending(token)) => Some(token),
                            Err(error) => {
                                self.observe_storage_error(&error, epoch);
                                if matches!(error, StorageError::Engine(_)) {
                                    let _ = self.epochs.release_adoption(epoch, stored);
                                }
                                self.rollback_updates(storage, adoption, &updates);
                                self.diagnostics
                                    .push(LoaderDiagnostic::Storage { handle, error });
                                return Ok(());
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
                        let epoch = self.ensure_descriptor(type_uuid)?.epoch;
                        let stored = StoredAdoption {
                            type_uuid,
                            handle,
                            adoption,
                        };
                        if let Err(error) = self.epochs.record_adoption(epoch, stored) {
                            let _ = value.destroy();
                            return Err(LoaderError::RuntimeEpoch(error));
                        }
                        let token = match storage.update(type_uuid, handle, value, adoption) {
                            Ok(UpdateResult::Ready) => None,
                            Ok(UpdateResult::Pending(token)) => Some(token),
                            Err(error) => {
                                self.observe_storage_error(&error, epoch);
                                if matches!(error, StorageError::Engine(_)) {
                                    let _ = self.epochs.release_adoption(epoch, stored);
                                }
                                self.rollback_updates(storage, adoption, &updates);
                                self.diagnostics
                                    .push(LoaderDiagnostic::Storage { handle, error });
                                return Ok(());
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
                | CandidateTerminal::Failed(_)
                | CandidateTerminal::Missing
                | CandidateTerminal::Built { .. } => {}
            }
        }
        if updates.iter().all(|update| update.token.is_none()) {
            self.commit_updates(storage, adoption, updates);
        } else {
            self.pending.push(PendingComponent { adoption, updates });
        }
        Ok(())
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
                self.rollback_updates(storage, pending.adoption, &pending.updates);
                self.diagnostics
                    .push(LoaderDiagnostic::Storage { handle, error });
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
        for update in updates {
            let old = self
                .slots
                .get_mut(&update.handle)
                .and_then(|slot| slot.current.take());
            if let Some(old) = old {
                let stored = StoredAdoption {
                    type_uuid: old.type_uuid,
                    handle: update.handle,
                    adoption: old.adoption,
                };
                match storage.free(old.type_uuid, update.handle, old.adoption) {
                    Ok(()) => {
                        let _ = self.epochs.release_adoption(old.epoch, stored);
                    }
                    Err(_) => self.poison_epoch(old.epoch),
                }
            }
            if let Some(slot) = self.slots.get_mut(&update.handle) {
                slot.current = Some(CurrentValue {
                    type_uuid: update.type_uuid,
                    load_deps: update.load_deps,
                    adoption,
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
        for uuid in members {
            if let Some(candidate) = sweep.candidates.remove(uuid) {
                destroy_candidate_values(candidate.terminal);
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
                        if let Some(stamp) = self
                            .sweep
                            .as_ref()
                            .and_then(|sweep| sweep.basis.rpc_snapshot())
                        {
                            entry.state = ManifestState::StaleLastGood {
                                content_hash: hash,
                                error: error.clone(),
                                built_from: stamp,
                            };
                        }
                    }
                    for handle in self.handles_for_uuid(*uuid) {
                        if let Some(slot) = self.slots.get_mut(&handle) {
                            slot.status = if slot.current.is_some() {
                                LoadStatus::Loaded
                            } else {
                                LoadStatus::Unloaded
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
        self.dirty.extend(self.held_uuids());
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
        CandidateTerminal::Pending | CandidateTerminal::Failed(_) | CandidateTerminal::Missing => {
            return;
        }
    };
    for value in values.into_values() {
        let _ = value.destroy();
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
