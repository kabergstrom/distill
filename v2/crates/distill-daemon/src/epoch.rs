//! Pipeline module staging, publication, poison, and unload fencing.
//!
//! The dynamic-library mechanics are provided by the shared `ngp-module-host`
//! crate used by New Game Plus. This crate owns the Distill state machine and
//! audited pipeline table layered on that common staging/residency boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};

use distill_asset::{ModuleEpochPoisonCause, ModuleEpochToken};
use distill_core::id::TypeUuid;
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
use distill_pipeline_api::registration::{ErasedCallback, RegistrationArena, RegistrationHost};
use distill_store::pipeline::ValidatedPipelineEpoch;
pub use distill_store::state::{
    CleanupDisposition as CandidateCleanupDisposition, PipelineFailure, PipelineFailureCode,
    PipelineFailureOrigin,
};
use distill_store::state::{
    PipelineEpoch as StoredPipelineEpoch, Registration as StoredRegistration,
    RegistrationKind as StoredRegistrationKind,
};
use distill_store::StoreError;

use crate::callbacks::{
    CallbackHandle, CallbackInvokeError, CodegenDescriptor, ContainedCodegenContext,
    ContainedImportContext, ContainedProcessContext, Diagnostics, ImporterDescriptor,
    InfallibleCallbackError, MigrationFunctionError, PipelineCodegenContext,
    PipelineProcessContext, ProcessorDescriptor, ProcessorError, ProcessorProducts,
    ToolDescriptor, ValidatorDescriptor,
};
use crate::tool_resolver::resolve_tool_epoch;

pub use distill_pipeline_api::module::ModuleAbiIdentity;
pub use distill_pipeline_api::registration::{
    ModuleCallError, Registration, RegistrationDisposition, RegistrationKind, RegistrationStatus,
    TargetDefinition,
};

/// Audited reverse-call surfaces. New host-owned callback tables must add a
/// named surface here so the cross-boundary inventory stays explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostCallbackSurface {
    Registry,
    EncodeSink,
    AuthoringImportContext,
    ProcessContext,
    CodegenContext,
}

impl HostCallbackSurface {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registry => "Registry",
            Self::EncodeSink => "EncodeSink",
            Self::AuthoringImportContext => "AuthoringImportContext",
            Self::ProcessContext => "ProcessContext",
            Self::CodegenContext => "CodegenContext",
        }
    }
}

/// Host-owned no-unwind thunk passed to module code. Every invocation returns
/// an explicit status; a panic in daemon callback code is caught on the host
/// side before control can unwind through a module frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostCallbackBoundary {
    surface: HostCallbackSurface,
}

pub type HostCallbackStatus<T> = Result<T, ModuleCallError>;

impl HostCallbackBoundary {
    pub const fn new(surface: HostCallbackSurface) -> Self {
        Self { surface }
    }

    pub fn call<T, F>(&self, operation: &str, callback: F) -> HostCallbackStatus<T>
    where
        F: FnOnce() -> HostCallbackStatus<T>,
    {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).unwrap_or_else(|_| {
            Err(host_callback_panic(self.surface, operation))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RegistrationSet {
    /// Owned strings are intentional: no identifier points into unloadable
    /// module storage after registration returns.
    pub registrations: Vec<Registration>,
    pub pipeline_targets: BTreeSet<String>,
}

/// An erased callback owned by a candidate arena. Ownership transfers to the
/// arena on entry and the status thunk is the only operation permitted to
/// destroy `pointer`.
///
/// This has deliberately no `Drop` implementation: if its thunk reports an
/// error or panics, retaining or even dropping the host-side wrapper leaks the
/// module object instead of running unchecked module drop glue.
pub struct ErasedRegistrationCapsule(NonNull<RegistrationCapsuleNode>);

struct RegistrationCapsuleNode {
    pointer: *mut u8,
    owner: ModuleEpochToken,
    cleanup: unsafe fn(*mut u8) -> Result<(), ModuleCallError>,
    next: Option<NonNull<RegistrationCapsuleNode>>,
    installation_seq: u64,
    registration: Option<Registration>,
    callback: CallbackHandle,
}

// SAFETY: construction requires the caller to promise that the opaque object
// and its thunk may be transferred to the module host thread.
unsafe impl Send for ErasedRegistrationCapsule {}

impl std::fmt::Debug for ErasedRegistrationCapsule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedRegistrationCapsule")
            .field("node", &self.0)
            .finish_non_exhaustive()
    }
}

impl ErasedRegistrationCapsule {
    /// Construct the raw, status-bearing ownership record installed into a
    /// candidate arena.
    ///
    /// # Safety
    ///
    /// `pointer` must remain valid until `cleanup` returns `Ok`. The thunk must
    /// fully destroy/deallocate it on `Ok`, leave it valid and deliberately
    /// leaked on `Err`, contain all module panics, and be safe to call exactly
    /// once. The pointed-to object and thunk must be safe to transfer to the
    /// module-host thread.
    pub unsafe fn from_raw(
        pointer: *mut u8,
        owner: ModuleEpochToken,
        cleanup: unsafe fn(*mut u8) -> Result<(), ModuleCallError>,
    ) -> Self {
        let node = Box::new(RegistrationCapsuleNode {
            pointer,
            owner,
            cleanup,
            next: None,
            installation_seq: 0,
            registration: None,
            callback: CallbackHandle::None,
        });
        // SAFETY: Box never produces a null pointer. The no-Drop capsule is
        // linked into exactly one arena, which frees the node after successful
        // payload cleanup and retains it after failure.
        Self(unsafe { NonNull::new_unchecked(Box::into_raw(node)) })
    }

    fn from_erased(callback: ErasedCallback, owner: ModuleEpochToken) -> Self {
        let (pointer, cleanup, handle) = callback.into_parts();
        // SAFETY: the API's generic registration boxed the callback at a
        // stable address, with the matching cleanup thunk that destroys it
        // under containment and deallocates only on success.
        let capsule = unsafe { Self::from_raw(pointer, owner, cleanup) };
        unsafe { (*capsule.0.as_ptr()).callback = handle };
        capsule
    }
}

/// Compatibility spelling for callers generated before the capsule handoff
/// was made explicit. It has the same no-Drop, consumed-on-entry semantics.
pub type RegistrationResource = ErasedRegistrationCapsule;

/// A tracked residency capability for a module-owned value. Registration and
/// generated value constructors use this instead of manufacturing an
/// untracked token clone; the host can therefore prove that no candidate pin
/// remains before `dlclose`: every pin shares one `Arc`, whose count is the
/// number of pins.
#[derive(Clone)]
pub struct ModuleEpochPin {
    token: ModuleEpochToken,
    _residency: Arc<()>,
}

impl std::fmt::Debug for ModuleEpochPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleEpochPin")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl ModuleEpochPin {
    pub fn token(&self) -> &ModuleEpochToken {
        &self.token
    }
}

/// Host-owned state minted immediately after a candidate image opens and
/// before any registration call. Entries are installed in order and cleaned
/// exclusively through their status thunks in reverse order.
pub struct CandidateRegistrationArena {
    owner: ModuleEpochToken,
    residency: Arc<()>,
    head: Option<NonNull<RegistrationCapsuleNode>>,
    installed_len: usize,
    next_installation_seq: u64,
    rejected: Option<ModuleCallError>,
}

// SAFETY: every intrusive node originates in a `Send` capsule; the arena has
// exclusive access while unpublished and is only read after publication.
unsafe impl Send for CandidateRegistrationArena {}

impl std::fmt::Debug for CandidateRegistrationArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CandidateRegistrationArena")
            .field("owner", &self.owner)
            .field("entries", &self.installed_len)
            .field("pins", &self.pin_count())
            .finish()
    }
}

impl CandidateRegistrationArena {
    fn new(owner: ModuleEpochToken) -> Self {
        Self {
            owner,
            residency: Arc::new(()),
            head: None,
            installed_len: 0,
            next_installation_seq: 0,
            rejected: None,
        }
    }

    pub fn owner_token(&self) -> &ModuleEpochToken {
        &self.owner
    }

    pub fn owner_pin(&self) -> ModuleEpochPin {
        ModuleEpochPin {
            token: self.owner.clone(),
            _residency: Arc::clone(&self.residency),
        }
    }

    /// The module-facing view of this arena, as passed to a module's
    /// `register`.
    pub fn registrar(&mut self) -> RegistrationArena<'_> {
        RegistrationArena::new(self)
    }

    fn tool_descriptors(&self) -> Vec<ToolDescriptor> {
        let mut descriptors = Vec::new();
        let mut cursor = self.head;
        while let Some(node) = cursor {
            // SAFETY: an unpublished arena exclusively owns every linked node.
            let node = unsafe { node.as_ref() };
            if let CallbackHandle::Tool(descriptor) = &node.callback {
                descriptors.push(descriptor.clone());
            }
            cursor = node.next;
        }
        descriptors.reverse();
        descriptors
    }

    /// Transfer one module-owned object into the unpublished arena. Metadata
    /// validation is intentionally performed only after `register` returns so
    /// even a later duplicate is already owned and can be rolled back safely.
    pub fn install(
        &mut self,
        registration: Registration,
        capsule: ErasedRegistrationCapsule,
    ) -> RegistrationStatus {
        // The first host operation is a pure move into this callback-local
        // guard, already inside the host boundary. The capsule owns its
        // intrusive node, so linking it into the arena performs no allocation.
        let mut ingress = CallbackIngressGuard {
            capsule: Some(capsule),
        };
        let installation_seq = self.next_installation_seq;
        let mut node = ingress.capsule.as_ref().expect("ingress is armed").0;
        let prior_head = self.head;
        // SAFETY: the capsule exclusively owns this live boxed node until it
        // is linked here. All fields are assigned by pure moves/writes.
        unsafe {
            node.as_mut().installation_seq = installation_seq;
            node.as_mut().registration = Some(registration);
            node.as_mut().next = prior_head;
        }
        self.head = Some(node);
        self.installed_len = self.installed_len.saturating_add(1);
        ingress.capsule = None;

        // Only after the allocation-free link owns the capsule may any
        // validation reject it. A panic from here is still cleaned by the
        // arena because the node is reachable from `head`.
        let owner_matches = unsafe { node.as_ref().owner.same_epoch(&self.owner) };
        let installed = unsafe {
            node.as_ref()
                .registration
                .as_ref()
                .expect("linked registration node has metadata")
        };
        let duplicate = installed.kind != RegistrationKind::Validator
            && Self::contains_registration_from(prior_head, installed.kind, &installed.id);
        let result = if !owner_matches {
            Err(ModuleCallError::new(
                "registration capsule belongs to a different module epoch",
            ))
        } else if duplicate {
            Err(ModuleCallError::new("duplicate non-validator registration"))
        } else if let Err(error) =
            Self::validate_callback_from(prior_head, installed, unsafe { &node.as_ref().callback })
        {
            Err(error)
        } else {
            match self.next_installation_seq.checked_add(1) {
                Some(next) => {
                    self.next_installation_seq = next;
                    Ok(())
                }
                None => Err(ModuleCallError::new(
                    "candidate registration sequence exhausted",
                )),
            }
        };
        if let Err(error) = &result {
            if self.rejected.is_none() {
                self.rejected = Some(error.clone());
            }
        }
        RegistrationStatus {
            disposition: RegistrationDisposition::Consumed,
            result,
        }
    }

    fn validate_callback_from(
        mut prior: Option<NonNull<RegistrationCapsuleNode>>,
        registration: &Registration,
        callback: &CallbackHandle,
    ) -> Result<(), ModuleCallError> {
        let normalized = distill_build::query::normalize_identifier(&registration.id)
            .map_err(|error| ModuleCallError::new(format!("invalid registration id: {error:?}")))?;
        if normalized != registration.id {
            return Err(ModuleCallError::new(
                "registration id is not in canonical normalized form",
            ));
        }
        let matches = match (registration.kind, callback) {
            (_, CallbackHandle::None) => true,
            (RegistrationKind::Importer, CallbackHandle::Importer { descriptor, .. }) => {
                descriptor.id == registration.id && descriptor.version == registration.version
            }
            (RegistrationKind::Processor, CallbackHandle::Processor { descriptor, .. }) => {
                descriptor.id == registration.id && descriptor.version == registration.version
            }
            (RegistrationKind::Codegen, CallbackHandle::Codegen { descriptor, .. }) => {
                descriptor.id == registration.id && descriptor.version == registration.version
            }
            (RegistrationKind::Validator, CallbackHandle::Validator { descriptor, .. }) => {
                descriptor.id == registration.id
            }
            (RegistrationKind::Migration, CallbackHandle::Migration { key, .. }) => {
                key == &registration.id
            }
            (RegistrationKind::Defaults, CallbackHandle::Defaults { descriptor, .. }) => {
                descriptor.type_uuid.to_string() == registration.id
            }
            (RegistrationKind::Tool, CallbackHandle::Tool(descriptor)) => {
                descriptor.id == registration.id
            }
            _ => false,
        };
        if !matches {
            return Err(ModuleCallError::new(
                "registration metadata does not match its executable callback",
            ));
        }

        if let CallbackHandle::Processor { descriptor, .. } = callback {
            while let Some(node) = prior {
                // SAFETY: prior nodes are linked and live for the complete
                // candidate lifetime.
                let linked = unsafe { node.as_ref() };
                if let CallbackHandle::Processor {
                    descriptor: existing,
                    ..
                } = &linked.callback
                {
                    if existing.input == descriptor.input
                        && existing.selector.overlaps(&descriptor.selector)
                    {
                        return Err(ModuleCallError::new(format!(
                            "processors {:?} and {:?} overlap for input {}",
                            existing.id, descriptor.id, descriptor.input
                        )));
                    }
                }
                prior = linked.next;
            }
        }
        Ok(())
    }

    pub fn installed_len(&self) -> usize {
        self.installed_len
    }

    fn registration_set(&self, pipeline_targets: BTreeSet<String>) -> RegistrationSet {
        let mut registrations = Vec::with_capacity(self.installed_len);
        let mut cursor = self.head;
        while let Some(node) = cursor {
            // SAFETY: every linked node remains live until cleanup.
            let node = unsafe { node.as_ref() };
            registrations.push(
                node.registration
                    .as_ref()
                    .expect("linked registration node has metadata")
                    .clone(),
            );
            cursor = node.next;
        }
        registrations.reverse();
        RegistrationSet {
            registrations,
            pipeline_targets,
        }
    }

    fn contains_registration_from(
        mut cursor: Option<NonNull<RegistrationCapsuleNode>>,
        kind: RegistrationKind,
        id: &str,
    ) -> bool {
        while let Some(node) = cursor {
            // SAFETY: every linked node remains live until cleanup.
            let node = unsafe { node.as_ref() };
            if node
                .registration
                .as_ref()
                .is_some_and(|registration| registration.kind == kind && registration.id == id)
            {
                return true;
            }
            cursor = node.next;
        }
        false
    }

    fn rejected(&self) -> Option<&ModuleCallError> {
        self.rejected.as_ref()
    }

    fn pin_count(&self) -> usize {
        Arc::strong_count(&self.residency).saturating_sub(1)
    }

    fn cleanup_reverse(&mut self) -> Result<(), ModuleCallError> {
        let mut errors = Vec::new();
        let mut cursor = self.head.take();
        let mut retained_head: Option<NonNull<RegistrationCapsuleNode>> = None;
        let mut retained_tail: Option<NonNull<RegistrationCapsuleNode>> = None;
        while let Some(mut node) = cursor {
            // SAFETY: `node` is linked and live. Save the next pointer before
            // cleanup; success frees this node, failure relinks it below.
            let next = unsafe { node.as_ref().next };
            let sequence = unsafe { node.as_ref().installation_seq };
            let cleanup = boundary_call("registration cleanup", || {
                // SAFETY: upheld by `from_raw`; each node is called at most
                // once after success and retained without retry after error.
                unsafe { (node.as_ref().cleanup)(node.as_ref().pointer) }
            });
            match cleanup {
                Ok(()) => {
                    self.installed_len = self.installed_len.saturating_sub(1);
                    // SAFETY: successful payload cleanup disarms the capsule;
                    // this is the unique Box pointer allocated by from_raw.
                    drop(unsafe { Box::from_raw(node.as_ptr()) });
                }
                Err(error) => {
                    self.owner.poison();
                    errors.push(format!("registration cleanup #{sequence} failed: {error}"));
                    // SAFETY: retain failed nodes in cleanup-attempt order
                    // without allocating a side table.
                    unsafe { node.as_mut().next = None };
                    if let Some(mut tail) = retained_tail {
                        unsafe { tail.as_mut().next = Some(node) };
                    } else {
                        retained_head = Some(node);
                    }
                    retained_tail = Some(node);
                }
            }
            cursor = next;
        }
        self.head = retained_head;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ModuleCallError::new(errors.join("; ")))
        }
    }
}

/// A module registers through the API arena, which erases each callback on
/// the module side and hands it here. The callback is wrapped in a capsule
/// owned by this arena's own epoch and installed like any other capsule, so
/// it is consumed on entry; a host panic is contained and rejects the
/// candidate.
impl RegistrationHost for CandidateRegistrationArena {
    fn install_callback(
        &mut self,
        registration: Registration,
        callback: ErasedCallback,
    ) -> RegistrationStatus {
        let boundary = HostCallbackBoundary::new(HostCallbackSurface::Registry);
        let result = boundary.call("install", || {
            let capsule = ErasedRegistrationCapsule::from_erased(callback, self.owner.clone());
            self.install(registration, capsule).into_result()
        });
        if let Err(error) = &result {
            if self.rejected.is_none() {
                self.rejected = Some(error.clone());
            }
        }
        RegistrationStatus {
            disposition: RegistrationDisposition::Consumed,
            result,
        }
    }
}

/// Allocation-free callback-local ingress owner. It intentionally has no
/// Drop implementation: a panic before the pure pointer link leaks the node
/// and forces the candidate boundary to retain the library rather than calling
/// module destruction or allowing automatic drop.
struct CallbackIngressGuard {
    capsule: Option<ErasedRegistrationCapsule>,
}

impl Drop for CallbackIngressGuard {
    fn drop(&mut self) {
        // An armed guard deliberately leaks its no-Drop capsule node. The
        // surrounding boundary latches candidate failure and retains the
        // library; it must never run module cleanup through unwinding.
        let _ = self.capsule.take();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateRequirements {
    pub module_abi: ModuleAbiIdentity,
    pub source_hashes: BTreeMap<String, String>,
    /// The watched schema's `layout_hashes`: a module whose source is ahead
    /// of the schema is still adopted when its layout hash matches (only fn
    /// bodies changed; see `ngp_module_host::SourceGate`).
    pub layout_hashes: BTreeMap<String, String>,
    pub schema_registry: BTreeMap<TypeUuid, distill_core::id::LogicalHash>,
    pub targets: Vec<TargetDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedModule {
    pub path: PathBuf,
    pub content_hash: [u8; 32],
}

fn boundary_panic(operation: &str) -> ModuleCallError {
    ModuleCallError::new(format!(
        "{operation} panicked at the module boundary; exported thunks must return status"
    ))
}

fn host_callback_panic(surface: HostCallbackSurface, operation: &str) -> ModuleCallError {
    ModuleCallError::new(format!(
        "{} host callback `{operation}` panicked; host-owned thunk returned error status",
        surface.as_str()
    ))
}

/// Audited table after its repr(C) ABI prefix has been located. All three probe
/// calls must be C-ABI in a concrete loader; `register` is reached only after
/// their values compare equal.
pub trait LoadedPipelineModule: Send {
    fn source_identity(&mut self)
        -> Result<ngp_module_host::ModuleSourceIdentity, ModuleCallError>;
    /// Source identity plus the module's layout hash. Without an override
    /// the layout hash is `""`, which never matches a schema's.
    fn reload_identity(
        &mut self,
    ) -> Result<ngp_module_host::ModuleReloadIdentity, ModuleCallError> {
        Ok(ngp_module_host::ModuleReloadIdentity {
            source: self.source_identity()?,
            layout_hash: String::new(),
        })
    }
    fn module_abi(&mut self) -> Result<ModuleAbiIdentity, ModuleCallError>;
    fn register(
        &mut self,
        targets: &[TargetDefinition],
        arena: &mut CandidateRegistrationArena,
    ) -> Result<BTreeSet<String>, ModuleCallError>;
    fn unload(&mut self) -> Result<(), ModuleCallError>;
    fn dlclose(&mut self);
}

/// Shared host adapter. Implementations must open exactly `staged.path`, never
/// the live build artifact that was copied from.
pub trait PipelineModuleLoader {
    fn open_staged(
        &mut self,
        staged: &StagedModule,
    ) -> Result<Box<dyn LoadedPipelineModule>, ModuleCallError>;
}

/// One loaded pipeline module. Every holder (the host's published state, a
/// snapshot, a running job) owns a clone; a job clones it when it starts and
/// holds it to the end. Replacing the module drops the host's clone; the
/// last clone's drop cleans the registrations up in reverse order and closes
/// the library. That drop happens in host code after the last callback
/// returned, so no module code is on the stack. An epoch whose token was
/// poisoned is leaked instead: its library is never closed.
#[derive(Clone)]
pub struct PipelineEpoch(Arc<EpochInner>);

impl std::fmt::Debug for PipelineEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineEpoch")
            .field("id", &self.id())
            .field("dylib_hash", &self.dylib_hash())
            .field("failed", &self.runtime_failure().is_some())
            .finish()
    }
}

struct EpochInner {
    id: u64,
    staged: StagedModule,
    token: ModuleEpochToken,
    targets: Vec<TargetDefinition>,
    registration: RegistrationSet,
    tools: BTreeMap<String, distill_store::pipeline::ToolRegistrationV2>,
    /// Source and layout identity the module reported when it was validated;
    /// set once by `prepare_candidate`.
    reload_identity: OnceLock<ngp_module_host::ModuleReloadIdentity>,
    /// The first runtime failure; it fences the epoch for good.
    runtime_error: OnceLock<(PipelineFailureCode, String)>,
    /// Read through `&self` once published; torn down only with the epoch
    /// exclusively held (see `unload_epoch`) or in `Drop`.
    registration_arena: Option<CandidateRegistrationArena>,
    /// Touched only with the epoch exclusively held, or in `Drop`.
    module: Option<Box<dyn LoadedPipelineModule>>,
}

// SAFETY: the module is reached only through `&mut EpochInner`. The arena's
// published nodes are immutable until cleanup, which also needs `&mut`, so
// shared reads of them from several workers do not race; the callbacks they
// hold are already copied out and called from any worker.
unsafe impl Sync for EpochInner {}

struct PreparedEpochRegistration {
    target_set: CanonicalTargetSet,
    registration: RegistrationSet,
    tools: BTreeMap<String, distill_store::pipeline::ToolRegistrationV2>,
    arena: CandidateRegistrationArena,
}

impl PipelineEpoch {
    fn new(
        id: u64,
        staged: StagedModule,
        token: ModuleEpochToken,
        prepared: PreparedEpochRegistration,
        module: Box<dyn LoadedPipelineModule>,
    ) -> Self {
        let PreparedEpochRegistration {
            target_set,
            registration,
            tools,
            arena,
        } = prepared;
        Self(Arc::new(EpochInner {
            id,
            staged,
            token,
            targets: target_set
                .rows
                .into_iter()
                .map(|row| TargetDefinition {
                    name: row.name,
                    fingerprint: row.target_definition_hash,
                })
                .collect(),
            registration,
            tools,
            reload_identity: OnceLock::new(),
            runtime_error: OnceLock::new(),
            registration_arena: Some(arena),
            module: Some(module),
        }))
    }

    pub fn id(&self) -> u64 {
        self.0.id
    }

    pub fn dylib_hash(&self) -> [u8; 32] {
        self.0.staged.content_hash
    }

    /// The module's source and layout identity, as validated against the
    /// schema it was published with (`None` only for test-built epochs).
    pub fn reload_identity(&self) -> Option<&ngp_module_host::ModuleReloadIdentity> {
        self.0.reload_identity.get()
    }

    pub fn staged_path(&self) -> &Path {
        &self.0.staged.path
    }

    pub fn module_token(&self) -> &ModuleEpochToken {
        &self.0.token
    }

    /// Mint a residency-tracked clone of the epoch token for a module-owned
    /// value whose lifetime is not already represented by a `PipelineEpoch`
    /// `Arc` pin.
    pub fn module_pin(&self) -> ModuleEpochPin {
        self.0
            .registration_arena
            .as_ref()
            .expect("published epoch registration arena must be resident")
            .owner_pin()
    }

    pub fn targets(&self) -> &[TargetDefinition] {
        &self.0.targets
    }

    pub fn registrations(&self) -> &RegistrationSet {
        &self.0.registration
    }

    pub fn importer_descriptors(&self) -> Vec<ImporterDescriptor> {
        self.callback_rows()
            .into_iter()
            .filter_map(|(_, callback)| match callback {
                CallbackHandle::Importer { descriptor, .. } => Some(descriptor),
                _ => None,
            })
            .collect()
    }

    pub fn processor_descriptors(&self) -> Vec<ProcessorDescriptor> {
        self.callback_rows()
            .into_iter()
            .filter_map(|(_, callback)| match callback {
                CallbackHandle::Processor { descriptor, .. } => Some(descriptor),
                _ => None,
            })
            .collect()
    }

    pub fn codegen_descriptors(&self) -> Vec<CodegenDescriptor> {
        self.callback_rows()
            .into_iter()
            .filter_map(|(_, callback)| match callback {
                CallbackHandle::Codegen { descriptor, .. } => Some(descriptor),
                _ => None,
            })
            .collect()
    }

    pub fn validator_descriptors(&self) -> Vec<ValidatorDescriptor> {
        self.callback_rows()
            .into_iter()
            .filter_map(|(_, callback)| match callback {
                CallbackHandle::Validator { descriptor, .. } => Some(descriptor),
                _ => None,
            })
            .collect()
    }

    pub fn has_default_table(&self, type_uuid: TypeUuid) -> bool {
        self.callback_rows().into_iter().any(|(_, callback)| {
            matches!(callback, CallbackHandle::Defaults { descriptor, .. } if descriptor.type_uuid == type_uuid)
        })
    }

    pub fn default_table_types(&self) -> Vec<TypeUuid> {
        self.callback_rows()
            .into_iter()
            .filter_map(|(_, callback)| match callback {
                CallbackHandle::Defaults { descriptor, .. } => Some(descriptor.type_uuid),
                _ => None,
            })
            .collect()
    }

    pub fn has_migration_function(&self, key: &str) -> bool {
        self.callback_rows().into_iter().any(|(_, callback)| {
            matches!(callback, CallbackHandle::Migration { key: registered, .. } if registered == key)
        })
    }

    pub fn migration_function_keys(&self) -> Vec<String> {
        self.callback_rows()
            .into_iter()
            .filter_map(|(_, callback)| match callback {
                CallbackHandle::Migration { key, .. } => Some(key),
                _ => None,
            })
            .collect()
    }

    pub fn tool_descriptors(&self) -> Vec<ToolDescriptor> {
        self.callback_rows()
            .into_iter()
            .filter_map(|(_, callback)| match callback {
                CallbackHandle::Tool(descriptor) => Some(descriptor),
                _ => None,
            })
            .collect()
    }

    pub fn tool_epoch(&self) -> BTreeMap<String, distill_store::pipeline::ToolRegistrationV2> {
        self.0.tools.clone()
    }

    pub fn invoke_importer(
        &self,
        id: &str,
        context: &mut dyn crate::importer::AuthoringImportContext,
        settings: &distill_json::AuthoredValue,
    ) -> Result<
        distill_build::import::ImportOutput,
        CallbackInvokeError<crate::importer::AuthoringImporterError>,
    > {
        self.ensure_healthy()?;
        let Some((pointer, CallbackHandle::Importer { call, .. })) = self
            .callback_rows()
            .into_iter()
            .find(|(_, callback)| matches!(callback, CallbackHandle::Importer { descriptor, .. } if descriptor.id == id))
        else {
            return Err(CallbackInvokeError::Missing);
        };
        // SAFETY: the job pins the epoch and therefore the registration arena
        // and staged image for the complete call. The handle and allocation
        // were created by the same generic registration constructor.
        let mut contained = ContainedImportContext::new(context);
        let result = unsafe { call(pointer, &mut contained, settings) };
        if contained.panicked() {
            return Err(CallbackInvokeError::HostRejected(
                "daemon import-context callback panicked".to_owned(),
            ));
        }
        match result {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(error)) => Err(CallbackInvokeError::Rejected(error)),
            Err(_) => {
                self.callback_panicked("importer");
                Err(CallbackInvokeError::Panicked)
            }
        }
    }

    pub fn invoke_processor(
        &self,
        id: &str,
        input: distill_json::AuthoredValue,
        context: &mut dyn PipelineProcessContext,
    ) -> Result<ProcessorProducts, CallbackInvokeError<ProcessorError>> {
        self.ensure_healthy()?;
        let Some((pointer, CallbackHandle::Processor { descriptor, call })) = self
            .callback_rows()
            .into_iter()
            .find(|(_, callback)| matches!(callback, CallbackHandle::Processor { descriptor, .. } if descriptor.id == id))
        else {
            return Err(CallbackInvokeError::Missing);
        };
        let mut contained = ContainedProcessContext::new(context);
        let result = unsafe { call(pointer, input, &mut contained) };
        if contained.panicked() {
            return Err(CallbackInvokeError::HostRejected(
                "daemon process-context callback panicked".to_owned(),
            ));
        }
        match result {
            Ok(Ok(output)) => {
                let actual_extras = output.extras.keys().cloned().collect::<BTreeSet<_>>();
                let Some(primary) = &output.primary else {
                    return Err(CallbackInvokeError::OutputBinding(
                        distill_build::dslf::OutputBindingFailureV1::MissingPrimary,
                    ));
                };
                if primary.type_uuid != descriptor.outputs.primary {
                    return Err(CallbackInvokeError::OutputBinding(
                        distill_build::dslf::OutputBindingFailureV1::TypeMismatch {
                            slot: distill_build::dslf::OutputBindingSlotV1::Primary,
                            expected_type: descriptor.outputs.primary,
                            observed_type: primary.type_uuid,
                        },
                    ));
                }
                if let Some((output_key, expected_type)) = descriptor
                    .outputs
                    .extras
                    .iter()
                    .find(|(key, _)| !actual_extras.contains(*key))
                {
                    return Err(CallbackInvokeError::OutputBinding(
                        distill_build::dslf::OutputBindingFailureV1::MissingExtra {
                            output_key: output_key.clone(),
                            expected_type: *expected_type,
                        },
                    ));
                }
                if let Some((output_key, product)) = output
                    .extras
                    .iter()
                    .find(|(key, _)| !descriptor.outputs.extras.contains_key(*key))
                {
                    return Err(CallbackInvokeError::OutputBinding(
                        distill_build::dslf::OutputBindingFailureV1::UndeclaredExtra {
                            output_key: output_key.clone(),
                            observed_type: product.type_uuid,
                        },
                    ));
                }
                if let Some((output_key, product)) = output.extras.iter().find(|(key, product)| {
                    descriptor.outputs.extras.get(*key) != Some(&product.type_uuid)
                }) {
                    return Err(CallbackInvokeError::OutputBinding(
                        distill_build::dslf::OutputBindingFailureV1::TypeMismatch {
                            slot: distill_build::dslf::OutputBindingSlotV1::Extra {
                                output_key: output_key.clone(),
                            },
                            expected_type: descriptor.outputs.extras[output_key],
                            observed_type: product.type_uuid,
                        },
                    ));
                }
                if let Some(debug_key) =
                    output.debug.keys().find(
                        |key| match distill_build::query::normalize_identifier(key) {
                            Ok(normalized) => normalized.as_str() != key.as_str(),
                            Err(_) => true,
                        },
                    )
                {
                    return Err(CallbackInvokeError::OutputBinding(
                        distill_build::dslf::OutputBindingFailureV1::InvalidOutputKey {
                            output_key: debug_key.clone(),
                        },
                    ));
                }
                Ok(output)
            }
            Ok(Err(error)) if error.code == 0 => Err(CallbackInvokeError::HostRejected(
                "processor failure code zero is reserved".to_owned(),
            )),
            Ok(Err(error)) => Err(CallbackInvokeError::Rejected(error)),
            Err(_) => {
                self.callback_panicked("processor");
                Err(CallbackInvokeError::Panicked)
            }
        }
    }

    pub fn invoke_codegens(
        &self,
        context: &mut dyn PipelineCodegenContext,
    ) -> Result<
        Vec<distill_build::codegen::GeneratedFile>,
        CallbackInvokeError<distill_build::codegen::CodegenFailure>,
    > {
        self.ensure_healthy()?;
        let callbacks = self
            .callback_rows()
            .into_iter()
            .filter(|(_, callback)| matches!(callback, CallbackHandle::Codegen { .. }))
            .collect::<Vec<_>>();
        let mut files = Vec::new();
        for (pointer, callback) in callbacks {
            let CallbackHandle::Codegen { call, .. } = callback else {
                unreachable!("codegen predicate returned another kind")
            };
            let mut contained = ContainedCodegenContext::new(context);
            let result = unsafe { call(pointer, &mut contained) };
            if contained.panicked() {
                return Err(CallbackInvokeError::HostRejected(
                    "daemon codegen-context callback panicked".to_owned(),
                ));
            }
            match result {
                Ok(Ok(mut generated)) => files.append(&mut generated),
                Ok(Err(error)) => return Err(CallbackInvokeError::Rejected(error)),
                Err(_) => {
                    self.callback_panicked("codegen");
                    return Err(CallbackInvokeError::Panicked);
                }
            }
        }
        Ok(files)
    }

    pub fn invoke_validators(
        &self,
        asset_type: TypeUuid,
        asset: &distill_json::AuthoredValue,
    ) -> Result<Vec<crate::callbacks::Diagnostic>, InfallibleCallbackError> {
        self.ensure_healthy()?;
        let callbacks = self
            .callback_rows()
            .into_iter()
            .filter(|(_, callback)| matches!(callback, CallbackHandle::Validator { descriptor, .. } if descriptor.asset_type == asset_type))
            .collect::<Vec<_>>();
        let mut diagnostics = Diagnostics::default();
        for (pointer, callback) in callbacks {
            let CallbackHandle::Validator { call, .. } = callback else {
                unreachable!("validator predicate returned another kind")
            };
            match unsafe { call(pointer, asset, &mut diagnostics) } {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    self.callback_panicked("validator");
                    return Err(CallbackInvokeError::Panicked);
                }
            }
        }
        Ok(diagnostics.into_rows())
    }

    pub fn invoke_migration(
        &self,
        key: &str,
        value: distill_json::AuthoredValue,
    ) -> Result<distill_json::AuthoredValue, CallbackInvokeError<MigrationFunctionError>> {
        self.ensure_healthy()?;
        let Some((pointer, CallbackHandle::Migration { call, .. })) = self
            .callback_rows()
            .into_iter()
            .find(|(_, callback)| matches!(callback, CallbackHandle::Migration { key: registered, .. } if registered == key))
        else {
            return Err(CallbackInvokeError::Missing);
        };
        match unsafe { call(pointer, value) } {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(error)) if error.code == 0 => Err(CallbackInvokeError::HostRejected(
                "migration-function failure code zero is reserved".to_owned(),
            )),
            Ok(Err(error)) => Err(CallbackInvokeError::Rejected(error)),
            Err(_) => {
                self.callback_panicked("migration function");
                Err(CallbackInvokeError::Panicked)
            }
        }
    }

    pub fn invoke_field_default(
        &self,
        type_uuid: TypeUuid,
        schema: &distill_schema::ngp_schema::SchemaNode,
        at: &distill_migrate::FieldPath,
    ) -> Result<Option<distill_json::AuthoredValue>, InfallibleCallbackError> {
        self.invoke_default(type_uuid, schema, at, false)
    }

    pub fn invoke_parent_default(
        &self,
        type_uuid: TypeUuid,
        schema: &distill_schema::ngp_schema::SchemaNode,
        at: &distill_migrate::FieldPath,
    ) -> Result<Option<distill_json::AuthoredValue>, InfallibleCallbackError> {
        self.invoke_default(type_uuid, schema, at, true)
    }

    fn invoke_default(
        &self,
        type_uuid: TypeUuid,
        schema: &distill_schema::ngp_schema::SchemaNode,
        at: &distill_migrate::FieldPath,
        parent: bool,
    ) -> Result<Option<distill_json::AuthoredValue>, InfallibleCallbackError> {
        self.ensure_healthy()?;
        let Some((pointer, CallbackHandle::Defaults { field, parent: parent_call, .. })) = self
            .callback_rows()
            .into_iter()
            .find(|(_, callback)| matches!(callback, CallbackHandle::Defaults { descriptor, .. } if descriptor.type_uuid == type_uuid))
        else {
            return Err(CallbackInvokeError::Missing);
        };
        let call = if parent { parent_call } else { field };
        match unsafe { call(pointer, schema, at) } {
            Ok(output) => Ok(output),
            Err(_) => {
                self.callback_panicked("default materializer");
                Err(CallbackInvokeError::Panicked)
            }
        }
    }

    fn callback_rows(&self) -> Vec<(*const u8, CallbackHandle)> {
        let Some(arena) = self.0.registration_arena.as_ref() else {
            return Vec::new();
        };
        let mut rows = Vec::with_capacity(arena.installed_len);
        let mut cursor = arena.head;
        while let Some(node) = cursor {
            // SAFETY: the arena owns every linked node. `self` pins the epoch,
            // so cleanup cannot begin while these copied handles are used.
            let node = unsafe { node.as_ref() };
            rows.push((node.pointer.cast_const(), node.callback.clone()));
            cursor = node.next;
        }
        rows.reverse();
        rows
    }

    fn ensure_healthy<E>(&self) -> Result<(), CallbackInvokeError<E>> {
        match self.runtime_failure() {
            Some(failure) => Err(CallbackInvokeError::Unavailable(failure.message)),
            None => Ok(()),
        }
    }

    fn callback_panicked(&self, kind: &str) {
        self.report_runtime_panic(format!("registered {kind} callback panicked"));
    }

    /// The first runtime failure: once set, no callback of this epoch runs
    /// again and its library is never closed.
    pub fn runtime_failure(&self) -> Option<PipelineFailure> {
        self.0.observe_token_poison();
        self.0.runtime_error.get().map(|(code, detail)| {
            pipeline_failure(
                *code,
                PipelineFailureOrigin::PublishedRuntime,
                CandidateCleanupDisposition::PublishedEpochLeaked,
                detail.clone(),
            )
        })
    }

    /// Report any contained module callback failure, including drop/free/update
    /// thunks. The shared token makes the fence immediately visible to values
    /// and every snapshot that pins this epoch.
    pub fn report_runtime_failure(&self, error: impl Into<String>) {
        self.0
            .poison(PipelineFailureCode::PublishedCallbackRejected, error.into());
    }

    pub fn report_runtime_panic(&self, error: impl Into<String>) {
        self.0
            .poison(PipelineFailureCode::PublishedCallbackPanic, error.into());
    }
}

impl EpochInner {
    fn observe_token_poison(&self) {
        if let Some(cause) = self.token.poison_cause() {
            let (code, detail) = match cause {
                ModuleEpochPoisonCause::CallbackPanic => (
                    PipelineFailureCode::PublishedCallbackPanic,
                    "a module-owned no-unwind thunk contained a callback panic",
                ),
                ModuleEpochPoisonCause::CallbackRejected => (
                    PipelineFailureCode::PublishedCallbackRejected,
                    "a module-owned status thunk returned a rejected status",
                ),
            };
            self.poison(code, detail.to_owned());
        }
    }

    fn poison(&self, code: PipelineFailureCode, detail: String) {
        self.token.poison_with(match code {
            PipelineFailureCode::PublishedCallbackPanic => ModuleEpochPoisonCause::CallbackPanic,
            _ => ModuleEpochPoisonCause::CallbackRejected,
        });
        let _ = self.runtime_error.set((code, detail));
    }

    /// Clean the registrations up in reverse order, unload the module and
    /// close the library. On failure the module and arena stay in place.
    fn unload(&mut self) -> Result<(), EpochCleanupError> {
        let arena = self.registration_arena.as_mut().ok_or_else(|| EpochCleanupError {
            disposition: CandidateCleanupDisposition::RegistrationCleanupFailed,
            detail: "registration arena is absent".to_owned(),
        })?;
        arena.cleanup_reverse().map_err(|error| EpochCleanupError {
            disposition: CandidateCleanupDisposition::RegistrationCleanupFailed,
            detail: error.to_string(),
        })?;

        let module = self.module.as_mut().ok_or_else(|| EpochCleanupError {
            disposition: CandidateCleanupDisposition::ModuleUnloadFailed,
            detail: "module handle is absent".to_owned(),
        })?;
        boundary_call("unload", || module.unload()).map_err(|error| EpochCleanupError {
            disposition: CandidateCleanupDisposition::ModuleUnloadFailed,
            detail: error.to_string(),
        })?;
        if self.token.is_poisoned() {
            return Err(EpochCleanupError {
                disposition: CandidateCleanupDisposition::TokenPoisoned,
                detail: "epoch token was poisoned before dlclose".to_owned(),
            });
        }
        let pins = arena.pin_count();
        if pins != 0 {
            return Err(EpochCleanupError {
                disposition: CandidateCleanupDisposition::TokenPinned,
                detail: format!("epoch token retains {pins} residency pin(s)"),
            });
        }
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| module.dlclose())).map_err(|_| {
            EpochCleanupError {
                disposition: CandidateCleanupDisposition::DlcloseFailed,
                detail: boundary_panic("dlclose").to_string(),
            }
        })?;
        self.module.take();
        self.registration_arena.take();
        Ok(())
    }
}

impl Drop for EpochInner {
    fn drop(&mut self) {
        // The last holder is gone: nothing runs module code any more.
        if self.module.is_some() && !self.token.is_poisoned() {
            if let Err(error) = self.unload() {
                self.token.poison();
                tracing::error!(
                    epoch = self.id,
                    disposition = cleanup_disposition_name(error.disposition),
                    detail = %error.detail,
                    "pipeline epoch cleanup failed; its library stays loaded"
                );
            }
        }
        if self.token.is_poisoned() {
            let arena = self.registration_arena.take();
            let module = self.module.take();
            if let Some(module) = module {
                // A poisoned image may contain half-torn-down state. Its
                // library handle is intentionally leaked rather than allowing
                // a host wrapper's Drop to dlclose it.
                std::mem::forget(module);
            }
            if let Some(arena) = arena {
                std::mem::forget(arena);
            }
        }
        // A closed library's copy is loaded by no one: delete it. A leaked
        // (poisoned) image keeps its file until the next open empties the
        // staging directory.
        if self.module.is_none()
            && !self.token.is_poisoned()
            && distill_store::atomic_file::in_staging(&self.staged.path)
        {
            let _ = std::fs::remove_file(&self.staged.path);
        }
    }
}

#[derive(Clone)]
enum PublishedState {
    Ready(PipelineEpoch),
    Failed(PipelineFailure),
}

#[derive(Clone)]
pub struct PipelineSnapshot {
    state: PublishedState,
}

impl PipelineSnapshot {
    /// A Ready snapshot of `epoch`. Its runtime failure still latches
    /// through the epoch's token.
    pub(crate) fn ready(epoch: PipelineEpoch) -> Self {
        Self {
            state: PublishedState::Ready(epoch),
        }
    }

    pub(crate) fn failed(failure: PipelineFailure) -> Self {
        Self {
            state: PublishedState::Failed(failure),
        }
    }

    /// What serves before any epoch is published.
    pub(crate) fn unpublished() -> Self {
        Self::failed(unpublished_failure())
    }

    pub fn epoch(&self) -> Result<&PipelineEpoch, PipelineFailure> {
        match &self.state {
            PublishedState::Failed(error) => Err(error.clone()),
            PublishedState::Ready(epoch) => {
                epoch.0.observe_token_poison();
                if let Some((code, detail)) = epoch.0.runtime_error.get() {
                    Err(pipeline_failure(
                        *code,
                        PipelineFailureOrigin::PublishedRuntime,
                        CandidateCleanupDisposition::PublishedEpochLeaked,
                        detail.clone(),
                    ))
                } else {
                    Ok(epoch)
                }
            }
        }
    }
}

/// The failure that serves before any epoch is published.
pub(crate) fn unpublished_failure() -> PipelineFailure {
    pipeline_failure(
        PipelineFailureCode::CandidateOpen,
        PipelineFailureOrigin::CandidateOpen,
        CandidateCleanupDisposition::None,
        "no pipeline epoch has been published",
    )
}

/// Why [`ModuleHost::prepare_candidate`] produced no epoch.
#[derive(Debug, Clone)]
pub enum CandidateRejection {
    /// The candidate cannot serve; publishing this failure fences the
    /// pipeline.
    Failed(PipelineFailure),
    /// The module's source hash is ahead of the watched schema and its layout
    /// hash differs (`ngp_module_host::SourceGate::Wait`), while a Ready
    /// epoch is published. Nothing is published: the Ready epoch keeps
    /// serving, and the next schema write retries the candidate.
    AwaitingSchema(String),
}

pub struct ModuleHost {
    state_dir: PathBuf,
    next_epoch_id: u64,
    published: Option<PublishedState>,
}

impl ModuleHost {
    /// A host whose module copies live in `state_dir`'s staging directory.
    /// Opening it empties that directory: no one loads an earlier
    /// process's copies now. Call it with the state lock held.
    pub fn new(state_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let state_dir = state_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&state_dir)?;
        distill_store::atomic_file::open_staging(&state_dir)?;
        Ok(Self {
            state_dir,
            next_epoch_id: 1,
            published: None,
        })
    }

    pub fn snapshot(&self) -> PipelineSnapshot {
        let state = self
            .published
            .clone()
            .unwrap_or_else(|| PublishedState::Failed(unpublished_failure()));
        PipelineSnapshot { state }
    }

    /// Stage and hash the candidate copy, open it, perform both pre-Rust-ABI
    /// probes, then register and validate its target-bound pipeline map as one
    /// unit. No state becomes current before every step succeeds.
    ///
    /// A candidate awaiting source-walk (see [`CandidateRejection`]) leaves
    /// the Ready epoch published and returns it.
    pub fn publish_candidate(
        &mut self,
        source: &Path,
        mut requirements: CandidateRequirements,
        loader: &mut dyn PipelineModuleLoader,
    ) -> Result<PipelineEpoch, PipelineFailure> {
        match self.prepare_candidate(source, &mut requirements, loader) {
            Ok(epoch) => {
                self.install_ready(epoch.clone());
                Ok(epoch)
            }
            Err(CandidateRejection::AwaitingSchema(_)) => Ok(self
                .published_ready_epoch()
                .expect("a candidate waits only while a Ready epoch is published")),
            Err(CandidateRejection::Failed(failure)) => {
                self.install_failure(failure.clone());
                Err(failure)
            }
        }
    }

    pub(crate) fn published_ready_epoch(&self) -> Option<PipelineEpoch> {
        match &self.published {
            Some(PublishedState::Ready(epoch)) => Some(epoch.clone()),
            _ => None,
        }
    }

    pub(crate) fn prepare_candidate(
        &mut self,
        source: &Path,
        requirements: &mut CandidateRequirements,
        loader: &mut dyn PipelineModuleLoader,
    ) -> Result<PipelineEpoch, CandidateRejection> {
        let id = self.mint_epoch_id();
        let target_set = match validate_requirements(requirements) {
            Ok(target_set) => target_set,
            Err(error) => {
                return Err(CandidateRejection::Failed(candidate_failure_record(
                    PipelineFailureCode::CandidateValidation,
                    error,
                    CandidateCleanupDisposition::None,
                )))
            }
        };
        let staged = match self.stage_copy(id, source) {
            Ok(staged) => staged,
            Err(error) => {
                return Err(CandidateRejection::Failed(candidate_failure_record(
                    PipelineFailureCode::CandidateOpen,
                    error,
                    CandidateCleanupDisposition::None,
                )))
            }
        };
        let mut module = match boundary_call("open", || loader.open_staged(&staged)) {
            Ok(module) => module,
            Err(error) => {
                return Err(CandidateRejection::Failed(candidate_failure_record(
                    PipelineFailureCode::CandidateOpen,
                    error.to_string(),
                    CandidateCleanupDisposition::None,
                )))
            }
        };

        // Mint ownership immediately after open, before any probe can reach a
        // Rust-ABI registration surface. The exact token and arena either move
        // into the published epoch or remain paired with the leaked candidate.
        let token = ModuleEpochToken::new(id);
        let mut registration_arena = CandidateRegistrationArena::new(token.clone());

        let validation =
            validate_open_module(module.as_mut(), requirements, &mut registration_arena);
        let (registration, reload_identity) = match validation {
            Ok(validated) => validated,
            Err(error) => {
                let cleanup = discard_candidate(module, registration_arena);
                // A module ahead of the schema is the normal window between
                // cargo rewriting the dylib and source-walk rewriting the
                // schema. With a Ready epoch to keep serving, it is not a
                // failure: the next schema write retries the candidate.
                if error.awaiting_schema
                    && cleanup.disposition == CandidateCleanupDisposition::CleanedAndClosed
                    && self.published_ready_epoch().is_some()
                {
                    return Err(CandidateRejection::AwaitingSchema(error.detail));
                }
                return Err(CandidateRejection::Failed(candidate_failure_with_cleanup(
                    error.code,
                    error.detail,
                    cleanup,
                )));
            }
        };
        if token.is_poisoned() {
            let cleanup = discard_candidate(module, registration_arena);
            return Err(CandidateRejection::Failed(candidate_failure_with_cleanup(
                PipelineFailureCode::CandidateRegistration,
                "candidate token was poisoned during registration".to_owned(),
                cleanup,
            )));
        }

        if let Err(error) =
            validate_registered_schema_types(&registration_arena, &requirements.schema_registry)
        {
            let cleanup = discard_candidate(module, registration_arena);
            return Err(CandidateRejection::Failed(candidate_failure_with_cleanup(
                PipelineFailureCode::CandidateRegistration,
                error,
                cleanup,
            )));
        }

        let tools = match resolve_tool_epoch(registration_arena.tool_descriptors()) {
            Ok(tools) => tools,
            Err(error) => {
                let cleanup = discard_candidate(module, registration_arena);
                return Err(CandidateRejection::Failed(candidate_failure_with_cleanup(
                    PipelineFailureCode::CandidateRegistration,
                    format!("tool registration resolution failed: {error}"),
                    cleanup,
                )));
            }
        };

        let epoch = PipelineEpoch::new(
            id,
            staged,
            token,
            PreparedEpochRegistration {
                target_set,
                registration,
                tools,
                arena: registration_arena,
            },
            module,
        );
        let _ = epoch.0.reload_identity.set(reload_identity);
        Ok(epoch)
    }

    fn mint_epoch_id(&mut self) -> u64 {
        let id = self.next_epoch_id;
        self.next_epoch_id = self
            .next_epoch_id
            .checked_add(1)
            .expect("pipeline epoch id exhausted");
        id
    }

    fn stage_copy(&self, id: u64, source: &Path) -> Result<StagedModule, String> {
        let extension = source
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("module");
        let mut attempt = 0_u32;
        loop {
            let suffix = if attempt == 0 {
                String::new()
            } else {
                format!("-{attempt}")
            };
            let path = distill_store::atomic_file::staging_dir(&self.state_dir)
                .join(format!("pipeline-{id}{suffix}.{extension}"));
            match ngp_module_host::stage_copy_to(source, &path) {
                Ok(staged) => {
                    return Ok(StagedModule {
                        path,
                        content_hash: staged.content_hash(),
                    })
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    attempt = attempt
                        .checked_add(1)
                        .ok_or_else(|| "pipeline staging path namespace exhausted".to_owned())?;
                }
                Err(error) => {
                    return Err(format!(
                        "stage {} as {}: {error}",
                        source.display(),
                        path.display()
                    ))
                }
            }
        }
    }

    /// Replace the published state. The previous epoch unloads once its
    /// last other holder lets go.
    pub(crate) fn install_failure(&mut self, failure: PipelineFailure) {
        self.published = Some(PublishedState::Failed(failure));
    }

    pub(crate) fn install_ready(&mut self, epoch: PipelineEpoch) {
        self.published = Some(PublishedState::Ready(epoch));
    }

    pub(crate) fn discard_unpublished(&mut self, mut epoch: PipelineEpoch) -> Option<PipelineFailure> {
        match unload_epoch(&mut epoch) {
            Ok(()) => None,
            Err(error) => {
                // This image never became published runtime. Poison it so
                // its drop leaks it instead of running module-owned
                // destructors, and report the candidate-cleanup matrix
                // required at the publication boundary.
                epoch.0.token.poison();
                let failure = candidate_failure_record(
                    PipelineFailureCode::CandidateCleanup,
                    format!(
                        "unpublished candidate cleanup disposition={}: {}",
                        cleanup_disposition_name(error.disposition),
                        error.detail
                    ),
                    error.disposition,
                );
                Some(failure)
            }
        }
    }
}

fn validate_registered_schema_types(
    arena: &CandidateRegistrationArena,
    registry: &BTreeMap<TypeUuid, distill_core::id::LogicalHash>,
) -> Result<(), String> {
    let mut bound = Vec::<(String, TypeUuid)>::new();
    let mut cursor = arena.head;
    while let Some(node) = cursor {
        // SAFETY: linked candidate nodes remain owned by `arena` until this
        // validation completes or candidate cleanup begins.
        let node = unsafe { node.as_ref() };
        match &node.callback {
            CallbackHandle::Importer { descriptor, .. } => bound.push((
                format!("importer {:?} settings", descriptor.id),
                descriptor.settings_type_uuid,
            )),
            CallbackHandle::Processor { descriptor, .. } => {
                bound.push((
                    format!("processor {:?} input", descriptor.id),
                    descriptor.input,
                ));
                bound.push((
                    format!("processor {:?} primary output", descriptor.id),
                    descriptor.outputs.primary,
                ));
                bound.extend(descriptor.outputs.extras.iter().map(|(key, type_uuid)| {
                    (
                        format!("processor {:?} extra output {key:?}", descriptor.id),
                        *type_uuid,
                    )
                }));
            }
            CallbackHandle::Validator { descriptor, .. } => bound.push((
                format!("validator {:?} asset", descriptor.id),
                descriptor.asset_type,
            )),
            CallbackHandle::Defaults { descriptor, .. } => {
                bound.push(("default materializer".to_owned(), descriptor.type_uuid))
            }
            CallbackHandle::None
            | CallbackHandle::Codegen { .. }
            | CallbackHandle::Migration { .. }
            | CallbackHandle::Tool(_) => {}
        }
        cursor = node.next;
    }
    bound.sort_unstable();
    if let Some((surface, type_uuid)) = bound
        .into_iter()
        .find(|(_, type_uuid)| !registry.contains_key(type_uuid))
    {
        return Err(format!(
            "{surface} binds schema-unknown type UUID {type_uuid}"
        ));
    }
    Ok(())
}

fn candidate_failure_with_cleanup(
    initiating_code: PipelineFailureCode,
    detail: String,
    cleanup: CandidateCleanup,
) -> PipelineFailure {
    let detail = format!(
        "{detail}; candidate cleanup disposition={}: {}",
        cleanup_disposition_name(cleanup.disposition),
        cleanup.detail
    );
    let code = if cleanup.disposition == CandidateCleanupDisposition::CleanedAndClosed {
        initiating_code
    } else {
        PipelineFailureCode::CandidateCleanup
    };
    candidate_failure_record(code, detail, cleanup.disposition)
}

fn candidate_failure_record(
    code: PipelineFailureCode,
    detail: String,
    cleanup: CandidateCleanupDisposition,
) -> PipelineFailure {
    pipeline_failure(code, PipelineFailureOrigin::CandidateOpen, cleanup, detail)
}

pub(crate) fn stored_pipeline_epoch(
    prepared: &PipelineEpoch,
    requirements: &CandidateRequirements,
) -> Result<ValidatedPipelineEpoch, StoreError> {
    let target_set = CanonicalTargetSet::canonical(
        prepared
            .targets()
            .iter()
            .map(|target| TargetSetRow {
                name: target.name.clone(),
                target_definition_hash: target.fingerprint,
            })
            .collect(),
    )
    .map_err(StoreError::InvalidTargetSet)?;
    if target_set.rows
        != prepared
            .targets()
            .iter()
            .map(|target| TargetSetRow {
                name: target.name.clone(),
                target_definition_hash: target.fingerprint,
            })
            .collect::<Vec<_>>()
    {
        return Err(StoreError::InvalidPipelineEpoch {
            detail: "prepared module target rows are not canonical",
        });
    }
    let registrations = prepared
        .registrations()
        .registrations
        .iter()
        .filter_map(|registration| {
            let kind = match registration.kind {
                RegistrationKind::Importer => StoredRegistrationKind::Importer,
                RegistrationKind::Processor => StoredRegistrationKind::Processor,
                RegistrationKind::Codegen
                | RegistrationKind::Validator
                | RegistrationKind::Migration
                | RegistrationKind::Defaults
                | RegistrationKind::Tool => return None,
            };
            Some(StoredRegistration {
                kind,
                id: registration.id.clone(),
                version: registration.version,
            })
        })
        .collect();
    let epoch = StoredPipelineEpoch {
        dylib_hash: prepared.dylib_hash(),
        target_set,
        schema_registry: requirements.schema_registry.clone(),
        registrations,
    };
    ValidatedPipelineEpoch::validate(epoch)
}

fn validate_requirements(
    requirements: &mut CandidateRequirements,
) -> Result<CanonicalTargetSet, String> {
    if requirements.module_abi.panic_strategy != "unwind" {
        return Err("pipeline and daemon must both use panic = unwind".to_owned());
    }
    if requirements.module_abi.allocator != "system" {
        return Err("pipeline and daemon must both use the system allocator".to_owned());
    }
    let canonical_targets = CanonicalTargetSet::canonical(
        requirements
            .targets
            .iter()
            .map(|target| TargetSetRow {
                name: target.name.clone(),
                target_definition_hash: target.fingerprint,
            })
            .collect(),
    )
    .map_err(|error| format!("target configuration is not canonical: {error}"))?;
    requirements.targets = canonical_targets
        .rows
        .iter()
        .map(|row| TargetDefinition {
            name: row.name.clone(),
            fingerprint: row.target_definition_hash,
        })
        .collect();
    Ok(canonical_targets)
}

fn validate_open_module(
    module: &mut dyn LoadedPipelineModule,
    requirements: &CandidateRequirements,
    registration_arena: &mut CandidateRegistrationArena,
) -> Result<(RegistrationSet, ngp_module_host::ModuleReloadIdentity), CandidatePhaseError> {
    let identity = boundary_call("source_identity", || module.reload_identity())
        .map_err(|error| CandidatePhaseError::attestation(error.to_string()))?;
    let crate_name = &identity.source.crate_name;
    match identity.gate(&requirements.source_hashes, &requirements.layout_hashes) {
        ngp_module_host::SourceGate::Match => {}
        ngp_module_host::SourceGate::AheadOfWalk { schema_source_hash } => {
            tracing::info!(
                crate_name = %crate_name,
                module = %identity.source.source_hash,
                schema = %schema_source_hash,
                "pipeline source changed but its layout hash matches the schema; \
                 adopting it ahead of source-walk"
            );
        }
        ngp_module_host::SourceGate::Wait {
            schema_source_hash: None,
        } => {
            return Err(CandidatePhaseError::attestation(format!(
                "module crate `{crate_name}` is absent from the watched schema"
            )));
        }
        ngp_module_host::SourceGate::Wait {
            schema_source_hash: Some(schema_source_hash),
        } => {
            return Err(CandidatePhaseError::awaiting_schema(format!(
                "module source hash mismatch for crate `{crate_name}` (module {}, schema \
                 {schema_source_hash}); waiting for source-walk",
                identity.source.source_hash
            )));
        }
    }
    let module_abi = boundary_call("module_abi", || module.module_abi())
        .map_err(|error| CandidatePhaseError::attestation(error.to_string()))?;
    if module_abi != requirements.module_abi {
        return Err(CandidatePhaseError::attestation(
            "module host-interface ABI identity mismatch",
        ));
    }
    let registration_result = boundary_call("register", || {
        module.register(&requirements.targets, registration_arena)
    });
    if let Some(rejected) = registration_arena.rejected() {
        return Err(CandidatePhaseError::registration(format!(
            "host latched rejected registration even though module registration returned: {rejected}"
        )));
    }
    let pipeline_targets = registration_result
        .map_err(|error| CandidatePhaseError::registration(error.to_string()))?;
    let registration = registration_arena.registration_set(pipeline_targets);
    validate_registration(&registration, &requirements.targets)
        .map_err(CandidatePhaseError::registration)?;
    Ok((registration, identity))
}

struct CandidatePhaseError {
    code: PipelineFailureCode,
    detail: String,
    /// The module's source is ahead of the watched schema and its layout
    /// hash differs: not a failure while a Ready epoch keeps serving.
    awaiting_schema: bool,
}

impl CandidatePhaseError {
    fn attestation(detail: impl Into<String>) -> Self {
        Self {
            code: PipelineFailureCode::CandidateAttestation,
            detail: detail.into(),
            awaiting_schema: false,
        }
    }

    fn awaiting_schema(detail: impl Into<String>) -> Self {
        Self {
            awaiting_schema: true,
            ..Self::attestation(detail)
        }
    }

    fn registration(detail: impl Into<String>) -> Self {
        Self {
            code: PipelineFailureCode::CandidateRegistration,
            detail: detail.into(),
            awaiting_schema: false,
        }
    }
}

fn validate_registration(
    registration: &RegistrationSet,
    targets: &[TargetDefinition],
) -> Result<(), String> {
    let mut owners = BTreeSet::new();
    for entry in &registration.registrations {
        if entry.kind != RegistrationKind::Validator
            && !owners.insert((entry.kind, entry.id.as_str()))
        {
            return Err(format!(
                "duplicate {:?} registration `{}`",
                entry.kind, entry.id
            ));
        }
    }
    let configured = targets
        .iter()
        .map(|target| target.name.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(unknown) = registration
        .pipeline_targets
        .iter()
        .find(|target| !configured.contains(target.as_str()))
    {
        return Err(format!(
            "pipeline map names unconfigured target `{unknown}`"
        ));
    }
    Ok(())
}

fn boundary_call<T>(
    operation: &str,
    call: impl FnOnce() -> Result<T, ModuleCallError>,
) -> Result<T, ModuleCallError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(call))
        .map_err(|_| boundary_panic(operation))?
}

fn pipeline_failure(
    code: PipelineFailureCode,
    origin: PipelineFailureOrigin,
    cleanup: CandidateCleanupDisposition,
    message: impl Into<String>,
) -> PipelineFailure {
    PipelineFailure::new(code, origin, cleanup, message)
        .expect("daemon emits only the closed DSPP code/origin/cleanup matrix")
}

const fn cleanup_disposition_name(disposition: CandidateCleanupDisposition) -> &'static str {
    match disposition {
        CandidateCleanupDisposition::None => "None",
        CandidateCleanupDisposition::CleanedAndClosed => "CleanedAndClosed",
        CandidateCleanupDisposition::RegistrationCleanupFailed => "RegistrationCleanupFailed",
        CandidateCleanupDisposition::ModuleUnloadFailed => "ModuleUnloadFailed",
        CandidateCleanupDisposition::TokenPoisoned => "TokenPoisoned",
        CandidateCleanupDisposition::TokenPinned => "TokenPinned",
        CandidateCleanupDisposition::DlcloseFailed => "DlcloseFailed",
        CandidateCleanupDisposition::PublishedEpochLeaked => "PublishedEpochLeaked",
    }
}

struct CandidateCleanup {
    disposition: CandidateCleanupDisposition,
    detail: String,
}

struct EpochCleanupError {
    disposition: CandidateCleanupDisposition,
    detail: String,
}

fn retain_candidate(
    module: Box<dyn LoadedPipelineModule>,
    arena: CandidateRegistrationArena,
    disposition: CandidateCleanupDisposition,
    detail: String,
) -> CandidateCleanup {
    std::mem::forget(module);
    std::mem::forget(arena);
    CandidateCleanup {
        disposition,
        detail,
    }
}

fn discard_candidate(
    mut module: Box<dyn LoadedPipelineModule>,
    mut arena: CandidateRegistrationArena,
) -> CandidateCleanup {
    if let Err(error) = arena.cleanup_reverse() {
        return retain_candidate(
            module,
            arena,
            CandidateCleanupDisposition::RegistrationCleanupFailed,
            error.to_string(),
        );
    }

    if let Err(error) = boundary_call("candidate unload", || module.unload()) {
        arena.owner.poison();
        return retain_candidate(
            module,
            arena,
            CandidateCleanupDisposition::ModuleUnloadFailed,
            error.to_string(),
        );
    }
    if arena.owner.is_poisoned() {
        return retain_candidate(
            module,
            arena,
            CandidateCleanupDisposition::TokenPoisoned,
            "candidate token was poisoned before dlclose".to_owned(),
        );
    }
    let pins = arena.pin_count();
    if pins != 0 {
        return retain_candidate(
            module,
            arena,
            CandidateCleanupDisposition::TokenPinned,
            format!("candidate token retains {pins} residency pin(s)"),
        );
    }
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| module.dlclose())).is_err() {
        arena.owner.poison();
        return retain_candidate(
            module,
            arena,
            CandidateCleanupDisposition::DlcloseFailed,
            boundary_panic("candidate dlclose").to_string(),
        );
    }
    CandidateCleanup {
        disposition: CandidateCleanupDisposition::CleanedAndClosed,
        detail: "all registrations cleaned, module unloaded, and library closed".to_owned(),
    }
}

/// Tear down an epoch nothing else holds.
fn unload_epoch(epoch: &mut PipelineEpoch) -> Result<(), EpochCleanupError> {
    let epoch = Arc::get_mut(&mut epoch.0).ok_or_else(|| EpochCleanupError {
        disposition: CandidateCleanupDisposition::TokenPinned,
        detail: "the epoch is still shared".to_owned(),
    })?;
    epoch.unload()
}

#[cfg(test)]
struct TestNoopModule;

#[cfg(test)]
impl LoadedPipelineModule for TestNoopModule {
    fn source_identity(
        &mut self,
    ) -> Result<ngp_module_host::ModuleSourceIdentity, ModuleCallError> {
        Err(ModuleCallError::new("unused test module source identity"))
    }

    fn module_abi(&mut self) -> Result<ModuleAbiIdentity, ModuleCallError> {
        Err(ModuleCallError::new("unused test module ABI"))
    }

    fn register(
        &mut self,
        _targets: &[TargetDefinition],
        _arena: &mut CandidateRegistrationArena,
    ) -> Result<BTreeSet<String>, ModuleCallError> {
        Err(ModuleCallError::new("unused test module registration"))
    }

    fn unload(&mut self) -> Result<(), ModuleCallError> {
        Ok(())
    }

    fn dlclose(&mut self) {}
}

#[cfg(test)]
pub(crate) fn processor_test_epoch<P: crate::callbacks::PipelineProcessor>(
    target: &str,
    target_definition_hash: [u8; 32],
    descriptor: crate::callbacks::ProcessorDescriptor,
    processor: P,
) -> PipelineEpoch {
    processor_test_epoch_with(
        target,
        target_definition_hash,
        descriptor,
        processor,
        |_| {},
    )
}

#[cfg(test)]
pub(crate) fn empty_test_epoch() -> PipelineEpoch {
    let target = "test".to_owned();
    let target_definition_hash = [7; 32];
    let token = ModuleEpochToken::new(9001);
    let arena = CandidateRegistrationArena::new(token.clone());
    let target_set = CanonicalTargetSet::canonical(vec![TargetSetRow {
        name: target.clone(),
        target_definition_hash,
    }])
    .expect("test target set is canonical");
    PipelineEpoch::new(
        9001,
        StagedModule {
            path: PathBuf::from("pipeline-empty-test"),
            content_hash: [7; 32],
        },
        token,
        PreparedEpochRegistration {
            target_set,
            registration: RegistrationSet {
                registrations: Vec::new(),
                pipeline_targets: BTreeSet::from([target]),
            },
            tools: BTreeMap::new(),
            arena,
        },
        Box::new(TestNoopModule),
    )
}

#[cfg(test)]
pub(crate) fn processor_test_epoch_with<
    P: crate::callbacks::PipelineProcessor,
    F: FnOnce(&mut CandidateRegistrationArena),
>(
    target: &str,
    target_definition_hash: [u8; 32],
    descriptor: crate::callbacks::ProcessorDescriptor,
    processor: P,
    configure: F,
) -> PipelineEpoch {
    processor_test_epoch_with_dylib(
        [9; 32],
        target,
        target_definition_hash,
        descriptor,
        processor,
        configure,
    )
}

/// As `processor_test_epoch_with`, staged from a dylib with `dylib_hash`.
#[cfg(test)]
pub(crate) fn processor_test_epoch_with_dylib<
    P: crate::callbacks::PipelineProcessor,
    F: FnOnce(&mut CandidateRegistrationArena),
>(
    dylib_hash: [u8; 32],
    target: &str,
    target_definition_hash: [u8; 32],
    descriptor: crate::callbacks::ProcessorDescriptor,
    processor: P,
    configure: F,
) -> PipelineEpoch {
    let token = ModuleEpochToken::new(9002);
    let mut arena = CandidateRegistrationArena::new(token.clone());
    arena
        .registrar()
        .register_processor(descriptor, processor)
        .into_result()
        .expect("test processor registration is valid");
    configure(&mut arena);
    let registration = arena.registration_set(BTreeSet::from([target.to_owned()]));
    let target_set = CanonicalTargetSet::canonical(vec![TargetSetRow {
        name: target.to_owned(),
        target_definition_hash,
    }])
    .expect("test target set is canonical");
    PipelineEpoch::new(
        9002,
        StagedModule {
            path: PathBuf::from("pipeline-test"),
            content_hash: dylib_hash,
        },
        token,
        PreparedEpochRegistration {
            target_set,
            registration,
            tools: BTreeMap::new(),
            arena,
        },
        Box::new(TestNoopModule),
    )
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    struct FailingUnloadModule;

    impl LoadedPipelineModule for FailingUnloadModule {
        fn source_identity(
            &mut self,
        ) -> Result<ngp_module_host::ModuleSourceIdentity, ModuleCallError> {
            Err(ModuleCallError::new("unused"))
        }

        fn module_abi(&mut self) -> Result<ModuleAbiIdentity, ModuleCallError> {
            Err(ModuleCallError::new("unused"))
        }

        fn register(
            &mut self,
            _targets: &[TargetDefinition],
            _arena: &mut CandidateRegistrationArena,
        ) -> Result<BTreeSet<String>, ModuleCallError> {
            Err(ModuleCallError::new("unused"))
        }

        fn unload(&mut self) -> Result<(), ModuleCallError> {
            Err(ModuleCallError::new("injected unload failure"))
        }

        fn dlclose(&mut self) {}
    }

    #[test]
    fn unpublished_cleanup_failure_uses_candidate_cleanup_failure_matrix() {
        let temp = tempfile::tempdir().unwrap();
        let mut host = ModuleHost::new(temp.path()).unwrap();
        let token = ModuleEpochToken::new(9100);
        let target_set = CanonicalTargetSet::canonical(vec![TargetSetRow {
            name: "test".to_owned(),
            target_definition_hash: [1; 32],
        }])
        .unwrap();
        let epoch = PipelineEpoch::new(
            9100,
            StagedModule {
                path: PathBuf::from("pipeline-failing-unload-test"),
                content_hash: [1; 32],
            },
            token.clone(),
            PreparedEpochRegistration {
                target_set,
                registration: RegistrationSet {
                    registrations: Vec::new(),
                    pipeline_targets: BTreeSet::from(["test".to_owned()]),
                },
                tools: BTreeMap::new(),
                arena: CandidateRegistrationArena::new(token),
            },
            Box::new(FailingUnloadModule),
        );

        let failure = host.discard_unpublished(epoch).unwrap();
        assert_eq!(failure.code, PipelineFailureCode::CandidateCleanup);
        assert_eq!(failure.origin, PipelineFailureOrigin::CandidateOpen);
        assert_eq!(
            failure.cleanup,
            CandidateCleanupDisposition::ModuleUnloadFailed
        );
    }
}

#[cfg(test)]
mod callback_tests {
    use super::*;
    use crate::callbacks::{
        CodegenAsset, CodegenContextError, CodegenDescriptor, DefaultsDescriptor,
        DiagnosticSeverity, MigrationFunctionError, PipelineCodegen, PipelineCodegenContext, PipelineDefaults,
        PipelineImporter, PipelineProcessContext, PipelineProcessor, PipelineValidator,
        ProcessorProduct, ProcessorProducts, ToolRegistration, ToolSource,
    };
    use crate::importer::{AuthoringImportContext, AuthoringImporterError};
    use distill_build::codegen::{CodegenFailure, GeneratedFile};
    use distill_build::import::{ImportError, ImportOutput};
    use distill_build::outputs::OutputDecls;
    use distill_build::pipeline::TargetSelector;
    use distill_build::query::{AssetQuery, FileQuery, RootedPath};
    use distill_build::tool::{ToolOutput, ToolRunError};
    use distill_core::id::AssetUuid;
    use distill_core::tool::ToolCwdPolicy;
    use distill_migrate::FieldPath;
    use distill_schema::ngp_schema::{LogicalSchema, SchemaNode};

    struct NoopModule;

    impl LoadedPipelineModule for NoopModule {
        fn source_identity(
            &mut self,
        ) -> Result<ngp_module_host::ModuleSourceIdentity, ModuleCallError> {
            Err(ModuleCallError::new("unused"))
        }

        fn module_abi(&mut self) -> Result<ModuleAbiIdentity, ModuleCallError> {
            Err(ModuleCallError::new("unused"))
        }

        fn register(
            &mut self,
            _targets: &[TargetDefinition],
            _arena: &mut CandidateRegistrationArena,
        ) -> Result<BTreeSet<String>, ModuleCallError> {
            Err(ModuleCallError::new("unused"))
        }

        fn unload(&mut self) -> Result<(), ModuleCallError> {
            Ok(())
        }

        fn dlclose(&mut self) {}
    }

    fn callback_test_epoch(
        configure: impl FnOnce(&mut CandidateRegistrationArena),
    ) -> PipelineEpoch {
        let token = ModuleEpochToken::new(9003);
        let mut arena = CandidateRegistrationArena::new(token.clone());
        configure(&mut arena);
        let registration = arena.registration_set(BTreeSet::from(["desktop".to_owned()]));
        let target_set = CanonicalTargetSet::canonical(vec![TargetSetRow {
            name: "desktop".to_owned(),
            target_definition_hash: [1; 32],
        }])
        .unwrap();
        PipelineEpoch::new(
            9003,
            StagedModule {
                path: PathBuf::from("pipeline-test"),
                content_hash: [9; 32],
            },
            token,
            PreparedEpochRegistration {
                target_set,
                registration,
                tools: BTreeMap::new(),
                arena,
            },
            Box::new(NoopModule),
        )
    }

    struct Importer;

    impl PipelineImporter for Importer {
        fn import(
            &self,
            _context: &mut dyn AuthoringImportContext,
            settings: &distill_json::AuthoredValue,
        ) -> Result<ImportOutput, AuthoringImporterError> {
            let mut output = ImportOutput::new();
            output
                .entry("main", TypeUuid([2; 16]), settings.clone())
                .unwrap();
            Ok(output)
        }
    }

    struct ImportContext;

    impl AuthoringImportContext for ImportContext {
        fn sources(&self) -> &[RootedPath] {
            &[]
        }

        fn read(&mut self, _path: &str) -> Result<Vec<u8>, ImportError> {
            unreachable!()
        }

        fn probe(&mut self, _path: &str) -> Result<bool, ImportError> {
            unreachable!()
        }

        fn enumerate(&mut self, _query: &FileQuery) -> Result<Vec<RootedPath>, ImportError> {
            unreachable!()
        }

        fn importer_capability(&mut self, _id: &str) -> Result<[u8; 32], ImportError> {
            unreachable!()
        }
    }

    struct ReadsImportContext;

    impl PipelineImporter for ReadsImportContext {
        fn import(
            &self,
            context: &mut dyn AuthoringImportContext,
            _settings: &distill_json::AuthoredValue,
        ) -> Result<ImportOutput, AuthoringImporterError> {
            let _ = context.read("source.asset");
            Ok(ImportOutput::new())
        }
    }

    struct PanickingImportContext;

    impl AuthoringImportContext for PanickingImportContext {
        fn sources(&self) -> &[RootedPath] {
            &[]
        }

        fn read(&mut self, _path: &str) -> Result<Vec<u8>, ImportError> {
            panic!("daemon import read panicked")
        }

        fn probe(&mut self, _path: &str) -> Result<bool, ImportError> {
            unreachable!()
        }

        fn enumerate(&mut self, _query: &FileQuery) -> Result<Vec<RootedPath>, ImportError> {
            unreachable!()
        }

        fn importer_capability(&mut self, _id: &str) -> Result<[u8; 32], ImportError> {
            unreachable!()
        }
    }

    #[test]
    fn import_host_callback_panic_is_typed_without_poisoning_the_epoch() {
        let mut epoch = callback_test_epoch(|arena| {
            arena
                .registrar()
                .register_importer(
                    ImporterDescriptor {
                        id: "reads".into(),
                        version: 1,
                        settings_type_uuid: TypeUuid([1; 16]),
                        settings_schema: LogicalSchema {
                            root: SchemaNode::Unit,
                        },
                        default_settings: distill_json::AuthoredValue::Null,
                    },
                    ReadsImportContext,
                )
                .into_result()
                .unwrap();
        });

        assert!(matches!(
            epoch.invoke_importer(
                "reads",
                &mut PanickingImportContext,
                &distill_json::AuthoredValue::Null,
            ),
            Err(CallbackInvokeError::HostRejected(detail))
                if detail.contains("import-context")
        ));
        assert!(epoch.runtime_failure().is_none());
        assert!(!epoch.0.token.is_poisoned());

        assert!(unload_epoch(&mut epoch).is_ok());
    }

    struct Process;

    impl PipelineProcessor for Process {
        fn process(
            &self,
            input: distill_json::AuthoredValue,
            _context: &mut dyn PipelineProcessContext,
        ) -> Result<ProcessorProducts, crate::callbacks::ProcessorError> {
            Ok(ProcessorProducts {
                primary: Some(ProcessorProduct::new(TypeUuid([3; 16]), input)),
                ..ProcessorProducts::default()
            })
        }
    }

    struct MissingPrimary;

    impl PipelineProcessor for MissingPrimary {
        fn process(
            &self,
            _input: distill_json::AuthoredValue,
            _context: &mut dyn PipelineProcessContext,
        ) -> Result<ProcessorProducts, crate::callbacks::ProcessorError> {
            Ok(ProcessorProducts::default())
        }
    }

    struct ProcessContext;

    impl PipelineProcessContext for ProcessContext {
        fn run_tool(
            &mut self,
            _id: &str,
            _args: &[String],
            _stdin: &[u8],
        ) -> Result<ToolOutput, ToolRunError> {
            unreachable!()
        }
    }

    #[test]
    fn processor_output_shape_failures_remain_typed_at_the_epoch_boundary() {
        let epoch = processor_test_epoch(
            "desktop",
            [1; 32],
            ProcessorDescriptor {
                id: "broken".into(),
                version: 1,
                input: TypeUuid([2; 16]),
                selector: TargetSelector::new(None, None).unwrap(),
                outputs: OutputDecls::new(TypeUuid([3; 16]), vec![]).unwrap(),
            },
            MissingPrimary,
        );

        assert!(matches!(
            epoch.invoke_processor(
                "broken",
                distill_json::AuthoredValue::UInt(1),
                &mut ProcessContext,
            ),
            Err(CallbackInvokeError::OutputBinding(
                distill_build::dslf::OutputBindingFailureV1::MissingPrimary
            ))
        ));
    }

    struct Validator;

    impl PipelineValidator for Validator {
        fn validate(
            &self,
            _asset: &distill_json::AuthoredValue,
            diagnostics: &mut Diagnostics,
        ) -> Result<(), distill_asset::CallbackPanic> {
            diagnostics.warn(FieldPath::of(&["value"]), "checked");
            Ok(())
        }
    }

    struct Codegen;

    impl PipelineCodegen for Codegen {
        fn generate(
            &self,
            context: &mut dyn PipelineCodegenContext,
        ) -> Result<Vec<GeneratedFile>, CodegenFailure> {
            let asset = context
                .read(AssetUuid([7; 16]))
                .map_err(|error| CodegenFailure::Generation(error.to_string()))?
                .ok_or_else(|| CodegenFailure::Generation("source disappeared".into()))?;
            Ok(vec![GeneratedFile::new(
                asset.asset,
                format!("pub const VALUE: u64 = {:?};\n", asset.value).into_bytes(),
            )])
        }
    }

    struct CodegenContext;

    impl PipelineCodegenContext for CodegenContext {
        fn query(&mut self, _query: &AssetQuery) -> Result<Vec<AssetUuid>, CodegenContextError> {
            Ok(vec![AssetUuid([7; 16])])
        }

        fn read(&mut self, asset: AssetUuid) -> Result<Option<CodegenAsset>, CodegenContextError> {
            Ok(Some(CodegenAsset {
                asset,
                type_uuid: TypeUuid([2; 16]),
                value: distill_json::AuthoredValue::UInt(17),
            }))
        }
    }

    struct ReadsCodegenContext;

    impl PipelineCodegen for ReadsCodegenContext {
        fn generate(
            &self,
            context: &mut dyn PipelineCodegenContext,
        ) -> Result<Vec<GeneratedFile>, CodegenFailure> {
            let _ = context.read(AssetUuid([7; 16]));
            Ok(Vec::new())
        }
    }

    struct PanickingCodegenContext;

    impl PipelineCodegenContext for PanickingCodegenContext {
        fn read(&mut self, _asset: AssetUuid) -> Result<Option<CodegenAsset>, CodegenContextError> {
            panic!("daemon codegen read panicked")
        }
    }

    #[test]
    fn codegen_host_callback_panic_is_typed_without_poisoning_the_epoch() {
        let mut epoch = callback_test_epoch(|arena| {
            arena
                .registrar()
                .register_codegen(
                    CodegenDescriptor {
                        id: "reads".into(),
                        version: 1,
                    },
                    ReadsCodegenContext,
                )
                .into_result()
                .unwrap();
        });

        assert!(matches!(
            epoch.invoke_codegens(&mut PanickingCodegenContext),
            Err(CallbackInvokeError::HostRejected(detail))
                if detail.contains("codegen-context")
        ));
        assert!(epoch.runtime_failure().is_none());
        assert!(!epoch.0.token.is_poisoned());

        assert!(unload_epoch(&mut epoch).is_ok());
    }

    struct Defaults;

    impl PipelineDefaults for Defaults {
        fn field_default(
            &self,
            _to_schema: &SchemaNode,
            _at: &FieldPath,
        ) -> Option<distill_json::AuthoredValue> {
            Some(distill_json::AuthoredValue::UInt(7))
        }

        fn parent_default(
            &self,
            _to_schema: &SchemaNode,
            _at: &FieldPath,
        ) -> Option<distill_json::AuthoredValue> {
            Some(distill_json::AuthoredValue::UInt(8))
        }
    }

    #[test]
    fn every_registration_kind_retains_an_executable_epoch_owned_surface() {
        let token = ModuleEpochToken::new(91);
        let mut arena = CandidateRegistrationArena::new(token.clone());
        let schema = LogicalSchema {
            root: SchemaNode::Unit,
        };
        arena
            .registrar()
            .register_importer(
                ImporterDescriptor {
                    id: "source".into(),
                    version: 3,
                    settings_type_uuid: TypeUuid([1; 16]),
                    settings_schema: schema.clone(),
                    default_settings: distill_json::AuthoredValue::UInt(1),
                },
                Importer,
            )
            .into_result()
            .unwrap();
        arena
            .registrar()
            .register_processor(
                ProcessorDescriptor {
                    id: "cook".into(),
                    version: 4,
                    input: TypeUuid([2; 16]),
                    selector: TargetSelector::new(None, None).unwrap(),
                    outputs: OutputDecls::new(TypeUuid([3; 16]), vec![]).unwrap(),
                },
                Process,
            )
            .into_result()
            .unwrap();
        arena
            .registrar()
            .register_validator(
                ValidatorDescriptor {
                    id: "lint".into(),
                    asset_type: TypeUuid([2; 16]),
                },
                Validator,
            )
            .into_result()
            .unwrap();
        arena
            .registrar()
            .register_migration(
                crate::callbacks::MigrationKey {
                    type_uuid: TypeUuid([2; 16]),
                    from: distill_core::id::LogicalHash([3; 32]),
                    to: distill_core::id::LogicalHash([4; 32]),
                },
                |value| -> Result<_, MigrationFunctionError> { Ok(value) },
            )
            .into_result()
            .unwrap();
        arena
            .registrar()
            .register_defaults(
                DefaultsDescriptor {
                    type_uuid: TypeUuid([2; 16]),
                },
                Defaults,
            )
            .into_result()
            .unwrap();
        arena
            .registrar()
            .register_tool(ToolDescriptor {
                id: "compiler".into(),
                registration: ToolRegistration {
                    source: ToolSource::Ambient {
                        launcher: PathBuf::from("compiler"),
                        toolchain_id: "test-compiler".into(),
                        trusted_fingerprint: None,
                    },
                    environment: vec![],
                    cwd_policy: ToolCwdPolicy::EmptyScratch,
                },
            })
            .into_result()
            .unwrap();
        arena
            .registrar()
            .register_codegen(
                CodegenDescriptor {
                    id: "rust_bindings".into(),
                    version: 1,
                },
                Codegen,
            )
            .into_result()
            .unwrap();

        let complete_registry = BTreeMap::from([
            (TypeUuid([1; 16]), distill_core::id::LogicalHash([1; 32])),
            (TypeUuid([2; 16]), distill_core::id::LogicalHash([2; 32])),
            (TypeUuid([3; 16]), distill_core::id::LogicalHash([3; 32])),
        ]);
        validate_registered_schema_types(&arena, &complete_registry).unwrap();
        let error = validate_registered_schema_types(
            &arena,
            &BTreeMap::from([
                (TypeUuid([1; 16]), distill_core::id::LogicalHash([1; 32])),
                (TypeUuid([2; 16]), distill_core::id::LogicalHash([2; 32])),
            ]),
        )
        .unwrap_err();
        assert!(error.contains("primary output"));
        assert!(error.contains(&TypeUuid([3; 16]).to_string()));

        let registration = arena.registration_set(BTreeSet::from(["desktop".into()]));
        let target_set = CanonicalTargetSet::canonical(vec![TargetSetRow {
            name: "desktop".into(),
            target_definition_hash: [1; 32],
        }])
        .unwrap();
        let mut epoch = PipelineEpoch::new(
            91,
            StagedModule {
                path: PathBuf::from("pipeline-test"),
                content_hash: [9; 32],
            },
            token,
            PreparedEpochRegistration {
                target_set,
                registration,
                tools: BTreeMap::new(),
                arena,
            },
            Box::new(NoopModule),
        );

        let imported = epoch
            .invoke_importer(
                "source",
                &mut ImportContext,
                &distill_json::AuthoredValue::UInt(11),
            )
            .unwrap();
        assert_eq!(
            imported.entries()["main"].value,
            distill_json::AuthoredValue::UInt(11)
        );
        let processed = epoch
            .invoke_processor(
                "cook",
                distill_json::AuthoredValue::UInt(12),
                &mut ProcessContext,
            )
            .unwrap();
        assert_eq!(
            processed.primary.map(|product| product.value),
            Some(distill_json::AuthoredValue::UInt(12)),
        );
        let diagnostics = epoch
            .invoke_validators(TypeUuid([2; 16]), &distill_json::AuthoredValue::Null)
            .unwrap();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, DiagnosticSeverity::Warning);
        assert_eq!(
            epoch
                .invoke_migration(
                    &crate::callbacks::MigrationKey {
                        type_uuid: TypeUuid([2; 16]),
                        from: distill_core::id::LogicalHash([3; 32]),
                        to: distill_core::id::LogicalHash([4; 32]),
                    }
                    .id(),
                    distill_json::AuthoredValue::UInt(13),
                )
                .unwrap(),
            distill_json::AuthoredValue::UInt(13)
        );
        assert_eq!(
            epoch
                .invoke_field_default(TypeUuid([2; 16]), &schema.root, &FieldPath::root())
                .unwrap(),
            Some(distill_json::AuthoredValue::UInt(7))
        );
        assert_eq!(epoch.tool_descriptors()[0].id, "compiler");
        let generated = epoch.invoke_codegens(&mut CodegenContext).unwrap();
        assert_eq!(generated.len(), 1);
        assert_eq!(generated[0].asset(), AssetUuid([7; 16]));

        assert!(unload_epoch(&mut epoch).is_ok());
    }

    #[test]
    fn overlapping_processor_selectors_reject_the_complete_candidate() {
        let token = ModuleEpochToken::new(92);
        let mut arena = CandidateRegistrationArena::new(token);
        let descriptor = |id: &str| ProcessorDescriptor {
            id: id.into(),
            version: 1,
            input: TypeUuid([5; 16]),
            selector: TargetSelector::new(None, None).unwrap(),
            outputs: OutputDecls::new(TypeUuid([6; 16]), vec![]).unwrap(),
        };
        arena
            .registrar()
            .register_processor(descriptor("first"), Process)
            .into_result()
            .unwrap();
        let error = arena
            .registrar()
            .register_processor(descriptor("second"), Process)
            .into_result()
            .unwrap_err();
        assert!(error.detail().contains("overlap"));
        assert!(arena.rejected().is_some());
        arena.cleanup_reverse().unwrap();
    }
}
