//! Pipeline module staging, publication, poison, and unload fencing.
//!
//! The dynamic-library mechanics are provided by the shared `ngp-module-host`
//! crate used by New Game Plus. This crate owns the Distill state machine and
//! audited pipeline table layered on that common staging/residency boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use distill_asset::{ModuleEpochPoisonCause, ModuleEpochToken};
use distill_core::id::TypeUuid;
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
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
    erase_callback, CallbackHandle, CallbackInvokeError, CodegenDescriptor,
    ContainedCodegenContext, ContainedImportContext, ContainedProcessContext, DefaultsDescriptor,
    Diagnostics, ImporterDescriptor, InfallibleCallbackError, MigrationFunctionError,
    PipelineCodegen, PipelineCodegenContext, PipelineDefaults, PipelineImporter, PipelineMigration,
    PipelineProcessContext, PipelineProcessor, PipelineValidator, ProcessorDescriptor,
    ProcessorError, ProcessorProducts, ToolDescriptor, ValidatorDescriptor,
};
use crate::tool_resolver::resolve_tool_epoch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleAbiIdentity {
    pub rustc: String,
    pub interface_fingerprint: [u8; 32],
    pub measured_interface: [u8; 32],
    pub panic_strategy: String,
    pub allocator: String,
}

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
            Err(ModuleCallError::host_callback_panic(
                self.surface,
                operation,
            ))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetDefinition {
    pub name: String,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RegistrationKind {
    Importer,
    Processor,
    Codegen,
    Validator,
    Migration,
    Defaults,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub kind: RegistrationKind,
    pub id: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RegistrationSet {
    /// Owned strings are intentional: no identifier points into unloadable
    /// module storage after registration returns.
    pub registrations: Vec<Registration>,
    pub pipeline_targets: BTreeSet<String>,
}

/// A generated erased registration capsule. Module-side construction suppresses
/// automatic drop; ownership transfers to the host callback on entry and the
/// status thunk is the only operation permitted to destroy `pointer`.
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

    fn from_callback<T: Send + Sync + 'static>(
        callback: T,
        owner: ModuleEpochToken,
        handle: CallbackHandle,
    ) -> Self {
        let pointer = erase_callback(callback);
        // SAFETY: `erase_callback` allocates a `ManuallyDrop<T>` whose address
        // is stable. The matching generic cleanup thunk destroys T under
        // containment and deallocates only after successful destruction.
        let capsule =
            unsafe { Self::from_raw(pointer, owner, crate::callbacks::cleanup_callback::<T>) };
        unsafe { (*capsule.0.as_ptr()).callback = handle };
        capsule
    }
}

/// Compatibility spelling for callers generated before the capsule handoff
/// was made explicit. It has the same no-Drop, consumed-on-entry semantics.
pub type RegistrationResource = ErasedRegistrationCapsule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationDisposition {
    Consumed,
}

/// Every object-bearing host callback returns an explicit consumed status on
/// both success and failure. There is deliberately no returned-to-caller arm.
#[derive(Debug)]
#[must_use = "registration ownership was consumed; inspect the nested result"]
pub struct RegistrationStatus {
    pub disposition: RegistrationDisposition,
    pub result: Result<(), ModuleCallError>,
}

impl RegistrationStatus {
    pub fn into_result(self) -> Result<(), ModuleCallError> {
        debug_assert_eq!(self.disposition, RegistrationDisposition::Consumed);
        self.result
    }
}

/// A tracked residency capability for a module-owned value. Registration and
/// generated value constructors use this instead of manufacturing an
/// untracked token clone; the host can therefore prove that no candidate pin
/// remains before `dlclose`.
#[derive(Clone)]
pub struct ModuleEpochPin {
    token: ModuleEpochToken,
    residency: Arc<EpochResidency>,
}

struct EpochResidency {
    fenced: AtomicBool,
}

impl std::fmt::Debug for ModuleEpochPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleEpochPin")
            .field("token", &self.token)
            .field("fenced", &self.is_fenced())
            .finish_non_exhaustive()
    }
}

impl ModuleEpochPin {
    pub fn token(&self) -> &ModuleEpochToken {
        &self.token
    }

    pub fn is_fenced(&self) -> bool {
        self.residency.fenced.load(Ordering::Acquire)
    }
}

/// Host-owned state minted immediately after a candidate image opens and
/// before any registration call. Entries are installed in order and cleaned
/// exclusively through their status thunks in reverse order.
pub struct CandidateRegistrationArena {
    owner: ModuleEpochToken,
    residency: Arc<EpochResidency>,
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
            .field("fenced", &self.is_fenced())
            .field("pins", &self.pin_count())
            .finish()
    }
}

impl CandidateRegistrationArena {
    fn new(owner: ModuleEpochToken) -> Self {
        Self {
            owner,
            residency: Arc::new(EpochResidency {
                fenced: AtomicBool::new(false),
            }),
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
            residency: Arc::clone(&self.residency),
        }
    }

    pub fn register_importer<T: PipelineImporter>(
        &mut self,
        descriptor: ImporterDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Importer,
            id: descriptor.id.clone(),
            version: descriptor.version,
        };
        let capsule = ErasedRegistrationCapsule::from_callback(
            callback,
            self.owner.clone(),
            CallbackHandle::importer::<T>(descriptor),
        );
        self.install(registration, capsule)
    }

    pub fn register_processor<T: PipelineProcessor>(
        &mut self,
        descriptor: ProcessorDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Processor,
            id: descriptor.id.clone(),
            version: descriptor.version,
        };
        let capsule = ErasedRegistrationCapsule::from_callback(
            callback,
            self.owner.clone(),
            CallbackHandle::processor::<T>(descriptor),
        );
        self.install(registration, capsule)
    }

    pub fn register_codegen<T: PipelineCodegen>(
        &mut self,
        descriptor: CodegenDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Codegen,
            id: descriptor.id.clone(),
            version: descriptor.version,
        };
        let capsule = ErasedRegistrationCapsule::from_callback(
            callback,
            self.owner.clone(),
            CallbackHandle::codegen::<T>(descriptor),
        );
        self.install(registration, capsule)
    }

    pub fn register_validator<T: PipelineValidator>(
        &mut self,
        descriptor: ValidatorDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Validator,
            id: descriptor.id.clone(),
            version: 1,
        };
        let capsule = ErasedRegistrationCapsule::from_callback(
            callback,
            self.owner.clone(),
            CallbackHandle::validator::<T>(descriptor),
        );
        self.install(registration, capsule)
    }

    pub fn register_migration<T: PipelineMigration>(
        &mut self,
        key: impl Into<String>,
        callback: T,
    ) -> RegistrationStatus {
        let key = key.into();
        let registration = Registration {
            kind: RegistrationKind::Migration,
            id: key.clone(),
            version: 1,
        };
        let capsule = ErasedRegistrationCapsule::from_callback(
            callback,
            self.owner.clone(),
            CallbackHandle::migration::<T>(key),
        );
        self.install(registration, capsule)
    }

    pub fn register_defaults<T: PipelineDefaults>(
        &mut self,
        descriptor: DefaultsDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Defaults,
            id: descriptor.type_uuid.to_string(),
            version: 1,
        };
        let capsule = ErasedRegistrationCapsule::from_callback(
            callback,
            self.owner.clone(),
            CallbackHandle::defaults::<T>(descriptor),
        );
        self.install(registration, capsule)
    }

    pub fn register_tool(&mut self, descriptor: ToolDescriptor) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Tool,
            id: descriptor.id.clone(),
            version: 1,
        };
        let capsule = ErasedRegistrationCapsule::from_callback(
            descriptor.clone(),
            self.owner.clone(),
            CallbackHandle::Tool(descriptor),
        );
        self.install(registration, capsule)
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
        let was_fenced = self.is_fenced();
        let owner_matches = unsafe { node.as_ref().owner.same_epoch(&self.owner) };
        let installed = unsafe {
            node.as_ref()
                .registration
                .as_ref()
                .expect("linked registration node has metadata")
        };
        let duplicate = installed.kind != RegistrationKind::Validator
            && Self::contains_registration_from(prior_head, installed.kind, &installed.id);
        let result = if was_fenced {
            Err(ModuleCallError::new(
                "candidate registration arena is fenced",
            ))
        } else if !owner_matches {
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

    fn fence(&mut self) {
        self.residency.fenced.store(true, Ordering::Release);
    }

    fn is_fenced(&self) -> bool {
        self.residency.fenced.load(Ordering::Acquire)
    }

    fn pin_count(&self) -> usize {
        Arc::strong_count(&self.residency).saturating_sub(1)
    }

    fn cleanup_reverse(&mut self) -> Result<(), ModuleCallError> {
        self.fence();
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
    pub schema_registry: BTreeMap<TypeUuid, distill_core::id::LogicalHash>,
    pub targets: Vec<TargetDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedModule {
    pub path: PathBuf,
    pub content_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleCallError {
    detail: String,
}

impl ModuleCallError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    fn boundary_panic(operation: &str) -> Self {
        Self::new(format!(
            "{operation} panicked at the module boundary; exported thunks must return status"
        ))
    }

    fn host_callback_panic(surface: HostCallbackSurface, operation: &str) -> Self {
        Self::new(format!(
            "{} host callback `{operation}` panicked; host-owned thunk returned error status",
            surface.as_str()
        ))
    }
}

impl std::fmt::Display for ModuleCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for ModuleCallError {}

/// Audited table after its repr(C) ABI prefix has been located. All three probe
/// calls must be C-ABI in a concrete loader; `register` is reached only after
/// their values compare equal.
pub trait LoadedPipelineModule: Send {
    fn source_identity(&mut self)
        -> Result<ngp_module_host::ModuleSourceIdentity, ModuleCallError>;
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

/// Stable outcome of the explicit teardown attempted for an opened but
/// unpublished candidate. `CleanedAndClosed` means the candidate failed its
/// validation but left no resident module state; every other variant means the
/// complete arena + library bundle was deliberately retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochStatus {
    pub accepting_new_work: bool,
    pub active_jobs: usize,
    pub draining: bool,
    pub poisoned: bool,
    pub unloaded: bool,
}

/// Called when an epoch may need its host's attention: a runtime failure
/// to persist, or a retired epoch that may now unload.
pub type EpochWake = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone)]
pub struct PipelineEpoch(Arc<EpochInner>);

impl Drop for PipelineEpoch {
    fn drop(&mut self) {
        // A drained, healthy epoch may unload once its last other holder is
        // gone. (A poisoned one never unloads.)
        if self.0.draining.load(Ordering::Acquire) && self.0.runtime_error.get().is_none() {
            self.0.wake();
        }
    }
}

impl std::fmt::Debug for PipelineEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineEpoch")
            .field("id", &self.id())
            .field("dylib_hash", &self.dylib_hash())
            .field("status", &self.status())
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
    accepting: AtomicBool,
    active_jobs: AtomicUsize,
    draining: AtomicBool,
    unloaded: AtomicBool,
    /// The first runtime failure; it fences the epoch for good.
    runtime_error: OnceLock<(PipelineFailureCode, String)>,
    /// Read through `&self` once published; torn down only with the epoch
    /// exclusively held (see `unload_epoch`) or in `Drop`.
    registration_arena: Option<CandidateRegistrationArena>,
    /// Touched only with the epoch exclusively held, or in `Drop`.
    module: Option<Box<dyn LoadedPipelineModule>>,
    wake: OnceLock<EpochWake>,
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
            accepting: AtomicBool::new(true),
            active_jobs: AtomicUsize::new(0),
            draining: AtomicBool::new(false),
            unloaded: AtomicBool::new(false),
            runtime_error: OnceLock::new(),
            registration_arena: Some(arena),
            module: Some(module),
            wake: OnceLock::new(),
        }))
    }

    pub fn id(&self) -> u64 {
        self.0.id
    }

    pub fn dylib_hash(&self) -> [u8; 32] {
        self.0.staged.content_hash
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
        let _job = self.callback_job()?;
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
        let _job = self.callback_job()?;
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
        let _job = self.callback_job()?;
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
        let _job = self.callback_job()?;
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
        let _job = self.callback_job()?;
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
        let _job = self.callback_job()?;
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

    fn callback_job<E>(&self) -> Result<EpochJobGuard, CallbackInvokeError<E>> {
        self.try_start_job().map_err(|error| {
            CallbackInvokeError::Unavailable(match error {
                EpochWorkError::Failed(failure) => failure.message,
                EpochWorkError::Retired { epoch_id } => {
                    format!("pipeline epoch {epoch_id} is retired")
                }
            })
        })
    }

    fn callback_panicked(&self, kind: &str) {
        self.report_runtime_panic(format!("registered {kind} callback panicked"));
    }

    pub fn try_start_job(&self) -> Result<EpochJobGuard, EpochWorkError> {
        self.0.observe_token_poison();
        if !self.0.accepting.load(Ordering::Acquire) {
            return Err(self.0.rejection());
        }
        self.0.active_jobs.fetch_add(1, Ordering::AcqRel);
        self.0.observe_token_poison();
        if !self.0.accepting.load(Ordering::Acquire) {
            self.0.active_jobs.fetch_sub(1, Ordering::AcqRel);
            return Err(self.0.rejection());
        }
        Ok(EpochJobGuard {
            epoch: Arc::clone(&self.0),
        })
    }

    pub fn begin_drain(&self) {
        self.0.accepting.store(false, Ordering::Release);
        self.0.draining.store(true, Ordering::Release);
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

    pub fn drain_complete(&self) -> bool {
        self.0.observe_token_poison();
        self.0.draining.load(Ordering::Acquire)
            && self.0.runtime_error.get().is_none()
            && self.0.active_jobs.load(Ordering::Acquire) == 0
    }

    pub fn status(&self) -> EpochStatus {
        self.0.observe_token_poison();
        EpochStatus {
            accepting_new_work: self.0.accepting.load(Ordering::Acquire),
            active_jobs: self.0.active_jobs.load(Ordering::Acquire),
            draining: self.0.draining.load(Ordering::Acquire),
            poisoned: self.0.runtime_error.get().is_some(),
            unloaded: self.0.unloaded.load(Ordering::Acquire),
        }
    }
}

impl EpochInner {
    fn wake(&self) {
        if let Some(wake) = self.wake.get() {
            wake();
        }
    }

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
        self.accepting.store(false, Ordering::Release);
        if self.runtime_error.set((code, detail)).is_ok() {
            self.wake();
        }
    }

    fn rejection(&self) -> EpochWorkError {
        match self.runtime_error.get() {
            Some((code, detail)) => EpochWorkError::Failed(pipeline_failure(
                *code,
                PipelineFailureOrigin::PublishedRuntime,
                CandidateCleanupDisposition::PublishedEpochLeaked,
                detail.clone(),
            )),
            None => EpochWorkError::Retired { epoch_id: self.id },
        }
    }
}

impl Drop for EpochInner {
    fn drop(&mut self) {
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
    }
}

pub struct EpochJobGuard {
    epoch: Arc<EpochInner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochWorkError {
    Failed(PipelineFailure),
    Retired { epoch_id: u64 },
}

impl Drop for EpochJobGuard {
    fn drop(&mut self) {
        let previous = self.epoch.active_jobs.fetch_sub(1, Ordering::AcqRel);
        let drained = previous == 1 && !self.epoch.accepting.load(Ordering::Acquire);
        if drained || self.epoch.token.is_poisoned() {
            self.epoch.wake();
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnloadOutcome {
    Draining(u64),
    Pinned(u64),
    Unloaded(u64),
    LeakedPoisoned(u64),
}

pub struct ModuleHost {
    state_dir: PathBuf,
    next_epoch_id: u64,
    published: Option<PublishedState>,
    retired: Vec<PipelineEpoch>,
    wake: Option<EpochWake>,
}

impl ModuleHost {
    pub fn new(state_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let state_dir = state_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(state_dir.join("modules"))?;
        Ok(Self {
            state_dir,
            next_epoch_id: 1,
            published: None,
            retired: Vec::new(),
            wake: None,
        })
    }

    /// Call `wake` when one of this host's epochs needs attention
    /// ([`ModuleHost::reap_retired`], or persisting a runtime failure).
    pub(crate) fn set_wake(&mut self, wake: EpochWake) {
        self.wake = Some(wake);
    }

    fn attach_wake(&self, epoch: &PipelineEpoch) {
        if let Some(wake) = &self.wake {
            let _ = epoch.0.wake.set(Arc::clone(wake));
        }
    }

    pub fn snapshot(&self) -> PipelineSnapshot {
        let state = self.published.clone().unwrap_or_else(|| {
            PublishedState::Failed(pipeline_failure(
                PipelineFailureCode::CandidateOpen,
                PipelineFailureOrigin::CandidateOpen,
                CandidateCleanupDisposition::None,
                "no pipeline epoch has been published",
            ))
        });
        PipelineSnapshot { state }
    }

    /// Stage and hash the candidate copy, open it, perform both pre-Rust-ABI
    /// probes, then register and validate its target-bound pipeline map as one
    /// unit. No state becomes current before every step succeeds.
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
            Err(failure) => {
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
    ) -> Result<PipelineEpoch, PipelineFailure> {
        let id = self.mint_epoch_id();
        let target_set = match validate_requirements(requirements) {
            Ok(target_set) => target_set,
            Err(error) => {
                return Err(candidate_failure_record(
                    PipelineFailureCode::CandidateValidation,
                    error,
                    CandidateCleanupDisposition::None,
                ))
            }
        };
        let staged = match self.stage_copy(id, source) {
            Ok(staged) => staged,
            Err(error) => {
                return Err(candidate_failure_record(
                    PipelineFailureCode::CandidateOpen,
                    error,
                    CandidateCleanupDisposition::None,
                ))
            }
        };
        let mut module = match boundary_call("open", || loader.open_staged(&staged)) {
            Ok(module) => module,
            Err(error) => {
                return Err(candidate_failure_record(
                    PipelineFailureCode::CandidateOpen,
                    error.to_string(),
                    CandidateCleanupDisposition::None,
                ))
            }
        };

        // Mint ownership immediately after open, before any probe can reach a
        // Rust-ABI registration surface. The exact token and arena either move
        // into the published epoch or remain paired with the leaked candidate.
        let token = ModuleEpochToken::new(id);
        let mut registration_arena = CandidateRegistrationArena::new(token.clone());

        let validation =
            validate_open_module(module.as_mut(), requirements, &mut registration_arena);
        let registration = match validation {
            Ok(registration) => registration,
            Err(error) => {
                let cleanup = discard_candidate(module, registration_arena);
                return Err(candidate_failure_with_cleanup(
                    error.code,
                    error.detail,
                    cleanup,
                ));
            }
        };
        if token.is_poisoned() {
            let cleanup = discard_candidate(module, registration_arena);
            return Err(candidate_failure_with_cleanup(
                PipelineFailureCode::CandidateRegistration,
                "candidate token was poisoned during registration".to_owned(),
                cleanup,
            ));
        }

        if let Err(error) =
            validate_registered_schema_types(&registration_arena, &requirements.schema_registry)
        {
            let cleanup = discard_candidate(module, registration_arena);
            return Err(candidate_failure_with_cleanup(
                PipelineFailureCode::CandidateRegistration,
                error,
                cleanup,
            ));
        }

        let tools = match resolve_tool_epoch(registration_arena.tool_descriptors()) {
            Ok(tools) => tools,
            Err(error) => {
                let cleanup = discard_candidate(module, registration_arena);
                return Err(candidate_failure_with_cleanup(
                    PipelineFailureCode::CandidateRegistration,
                    format!("tool registration resolution failed: {error}"),
                    cleanup,
                ));
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
        Ok(epoch)
    }

    /// Attempt cleanup of every retired image. Runtime failure is checked before
    /// active jobs or Arc pins: it is a permanent fence, not a delayed unload.
    pub fn reap_retired(&mut self) -> Vec<UnloadOutcome> {
        let mut outcomes = Vec::with_capacity(self.retired.len());
        let mut index = 0;
        while index < self.retired.len() {
            let epoch = &mut self.retired[index];
            epoch.0.observe_token_poison();
            let id = epoch.id();
            if epoch.status().poisoned {
                outcomes.push(UnloadOutcome::LeakedPoisoned(id));
                index += 1;
                continue;
            }
            if epoch.0.active_jobs.load(Ordering::Acquire) != 0 {
                outcomes.push(UnloadOutcome::Draining(id));
                index += 1;
                continue;
            }
            if Arc::strong_count(&epoch.0) != 1 {
                outcomes.push(UnloadOutcome::Pinned(id));
                index += 1;
                continue;
            }

            match unload_epoch(epoch) {
                Ok(()) => {
                    outcomes.push(UnloadOutcome::Unloaded(id));
                    self.retired.remove(index);
                }
                Err(error) => {
                    epoch.0.poison(
                        PipelineFailureCode::PublishedCleanup,
                        format!(
                            "epoch cleanup disposition={}: {}",
                            cleanup_disposition_name(error.disposition),
                            error.detail
                        ),
                    );
                    outcomes.push(UnloadOutcome::LeakedPoisoned(id));
                    index += 1;
                }
            }
        }
        outcomes
    }

    pub fn retired_count(&self) -> usize {
        self.retired.len()
    }

    pub fn retired_failure(&self, id: u64) -> Option<PipelineFailure> {
        let epoch = self.retired.iter().find(|epoch| epoch.id() == id)?;
        let (code, message) = epoch.0.runtime_error.get()?;
        Some(pipeline_failure(
            *code,
            PipelineFailureOrigin::PublishedRuntime,
            CandidateCleanupDisposition::PublishedEpochLeaked,
            message.clone(),
        ))
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
            let path = self
                .state_dir
                .join("modules")
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

    pub(crate) fn install_failure(&mut self, failure: PipelineFailure) {
        self.retire_published_ready();
        self.published = Some(PublishedState::Failed(failure));
    }

    pub(crate) fn install_ready(&mut self, epoch: PipelineEpoch) {
        self.attach_wake(&epoch);
        self.retire_published_ready();
        self.published = Some(PublishedState::Ready(epoch));
    }

    pub(crate) fn discard_unpublished(&mut self, mut epoch: PipelineEpoch) -> Option<PipelineFailure> {
        epoch.begin_drain();
        match unload_epoch(&mut epoch) {
            Ok(()) => None,
            Err(error) => {
                // This image never became published runtime. Fence and retain
                // it so EpochInner::drop cannot run module-owned destructors,
                // but report the candidate-cleanup matrix required at the
                // publication boundary.
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
                self.retired.push(epoch);
                Some(failure)
            }
        }
    }

    fn retire_published_ready(&mut self) {
        if let Some(PublishedState::Ready(epoch)) = self.published.take() {
            epoch.begin_drain();
            self.retired.push(epoch);
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
) -> Result<RegistrationSet, CandidatePhaseError> {
    let source = boundary_call("source_identity", || module.source_identity())
        .map_err(|error| CandidatePhaseError::attestation(error.to_string()))?;
    let Some(expected_source_hash) = requirements.source_hashes.get(&source.crate_name) else {
        return Err(CandidatePhaseError::attestation(format!(
            "module crate `{}` is absent from the watched schema",
            source.crate_name
        )));
    };
    if expected_source_hash != &source.source_hash {
        return Err(CandidatePhaseError::attestation(format!(
            "module source hash mismatch for crate `{}`",
            source.crate_name
        )));
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
    Ok(registration)
}

struct CandidatePhaseError {
    code: PipelineFailureCode,
    detail: String,
}

impl CandidatePhaseError {
    fn attestation(detail: impl Into<String>) -> Self {
        Self {
            code: PipelineFailureCode::CandidateAttestation,
            detail: detail.into(),
        }
    }

    fn registration(detail: impl Into<String>) -> Self {
        Self {
            code: PipelineFailureCode::CandidateRegistration,
            detail: detail.into(),
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
        .map_err(|_| ModuleCallError::boundary_panic(operation))?
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
    arena.fence();
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
            ModuleCallError::boundary_panic("candidate dlclose").to_string(),
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
    let arena = epoch.registration_arena.as_mut().ok_or_else(|| EpochCleanupError {
        disposition: CandidateCleanupDisposition::RegistrationCleanupFailed,
        detail: "registration arena is absent".to_owned(),
    })?;
    arena.fence();
    arena.cleanup_reverse().map_err(|error| EpochCleanupError {
        disposition: CandidateCleanupDisposition::RegistrationCleanupFailed,
        detail: error.to_string(),
    })?;

    let module = epoch.module.as_mut().ok_or_else(|| EpochCleanupError {
        disposition: CandidateCleanupDisposition::ModuleUnloadFailed,
        detail: "module handle is absent".to_owned(),
    })?;
    boundary_call("unload", || module.unload()).map_err(|error| EpochCleanupError {
        disposition: CandidateCleanupDisposition::ModuleUnloadFailed,
        detail: error.to_string(),
    })?;
    if epoch.token.is_poisoned() {
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
            detail: ModuleCallError::boundary_panic("dlclose").to_string(),
        }
    })?;
    epoch.module.take();
    epoch.registration_arena.take();
    epoch.unloaded.store(true, Ordering::Release);
    Ok(())
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
    let token = ModuleEpochToken::new(9002);
    let mut arena = CandidateRegistrationArena::new(token.clone());
    arena
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
            content_hash: [9; 32],
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
        assert_eq!(host.retired_count(), 1);
    }
}

#[cfg(test)]
mod callback_tests {
    use super::*;
    use crate::callbacks::{
        CodegenAsset, CodegenContextError, CodegenDescriptor, DiagnosticSeverity,
        MigrationFunctionError, PipelineCodegen, PipelineCodegenContext, PipelineDefaults,
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
        let status = epoch.status();
        assert!(status.accepting_new_work);
        assert!(!status.poisoned);

        epoch.begin_drain();
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
        let status = epoch.status();
        assert!(status.accepting_new_work);
        assert!(!status.poisoned);

        epoch.begin_drain();
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
            .register_migration("upgrade", |value| -> Result<_, MigrationFunctionError> {
                Ok(value)
            })
            .into_result()
            .unwrap();
        arena
            .register_defaults(
                DefaultsDescriptor {
                    type_uuid: TypeUuid([2; 16]),
                },
                Defaults,
            )
            .into_result()
            .unwrap();
        arena
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
                .invoke_migration("upgrade", distill_json::AuthoredValue::UInt(13))
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

        epoch.begin_drain();
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
            .register_processor(descriptor("first"), Process)
            .into_result()
            .unwrap();
        let error = arena
            .register_processor(descriptor("second"), Process)
            .into_result()
            .unwrap_err();
        assert!(error.detail().contains("overlap"));
        assert!(arena.rejected().is_some());
        arena.cleanup_reverse().unwrap();
    }
}
