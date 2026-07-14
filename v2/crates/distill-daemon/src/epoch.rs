//! Pipeline module staging, publication, poison, and unload fencing.
//!
//! The dynamic-library mechanics are provided by the shared `ngp-module-host`
//! crate used by New Game Plus. This crate owns the Distill state machine and
//! audited pipeline table layered on that common staging/residency boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use distill_asset::{ModuleEpochPoisonCause, ModuleEpochToken};
use distill_core::attestation::{
    validate_bootstrap_authority, validate_bootstrap_logical_authority, BundleFormatVersion,
};
pub use distill_core::attestation::{
    BootstrapAuthorityMismatch, CompiledAttestationDigest,
    CompiledTypeRow as CompiledTypeAttestation, CompiledTypeTable,
};
pub use distill_core::target_set::TargetSetHash;
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
pub use distill_schema::bootstrap_gen_v1::ConsumerBootstrapAuthorityV1 as HostBootstrapAuthorityV1;
use distill_schema::ngp_schema::CompilationIdentity;
use distill_store::pipeline::ValidatedPipelineEpoch;
use distill_store::state::{
    load_policy_digest, PipelineEpoch as StoredPipelineEpoch, PipelineState as StoredPipelineState,
    Registration as StoredRegistration, RegistrationKind as StoredRegistrationKind,
};
pub use distill_store::state::{
    CleanupDisposition as CandidateCleanupDisposition, PipelinePoison, PipelinePoisonCode,
    PipelinePoisonOrigin,
};
use distill_store::{Store, StoreError};

use crate::policy::{
    validate_candidate_linkage, CodeLoadRequest, CodeLoadingPolicy, NativeDependency,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleAbiIdentity {
    pub rustc: String,
    pub interface_fingerprint: [u8; 32],
    pub measured_interface: [u8; 32],
    pub panic_strategy: String,
    pub allocator: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleIdentity {
    pub compilation: CompilationIdentity,
    pub module_abi: ModuleAbiIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MeasuredLayout {
    pub type_id: String,
    pub digest: [u8; 32],
}

/// Audited reverse-call surfaces. New host-owned callback tables must add a
/// named surface here so the cross-boundary inventory stays explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostCallbackSurface {
    Registry,
    EncodeSink,
    ProcessContext,
}

impl HostCallbackSurface {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registry => "Registry",
            Self::EncodeSink => "EncodeSink",
            Self::ProcessContext => "ProcessContext",
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
        });
        // SAFETY: Box never produces a null pointer. The no-Drop capsule is
        // linked into exactly one arena, which frees the node after successful
        // payload cleanup and retains it after failure.
        Self(unsafe { NonNull::new_unchecked(Box::into_raw(node)) })
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
// exclusive access while unpublished and is mutex-protected after publication.
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
    pub identity: ModuleIdentity,
    pub measured_layouts: Vec<MeasuredLayout>,
    pub compiled_types: CompiledTypeTable,
    pub targets: Vec<TargetDefinition>,
    pub native_dependencies: Vec<NativeDependency>,
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
    fn identity(&mut self) -> Result<ModuleIdentity, ModuleCallError>;
    fn measured_layouts(&mut self) -> Result<Vec<MeasuredLayout>, ModuleCallError>;
    fn compiled_types(&mut self) -> Result<CompiledTypeTable, ModuleCallError>;
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

#[derive(Clone)]
pub struct PipelineEpoch(Arc<EpochInner>);

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
    target_set_hash: TargetSetHash,
    registration: RegistrationSet,
    accepting: AtomicBool,
    active_jobs: AtomicUsize,
    lifecycle: Mutex<EpochLifecycle>,
    registration_arena: Mutex<Option<CandidateRegistrationArena>>,
    module: Mutex<Option<Box<dyn LoadedPipelineModule>>>,
}

#[derive(Default)]
struct EpochLifecycle {
    draining: bool,
    runtime_error: Option<(PipelinePoisonCode, String)>,
    unloaded: bool,
}

impl PipelineEpoch {
    fn new(
        id: u64,
        staged: StagedModule,
        token: ModuleEpochToken,
        target_set: CanonicalTargetSet,
        registration: RegistrationSet,
        registration_arena: CandidateRegistrationArena,
        module: Box<dyn LoadedPipelineModule>,
    ) -> Self {
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
            target_set_hash: target_set.digest,
            registration,
            accepting: AtomicBool::new(true),
            active_jobs: AtomicUsize::new(0),
            lifecycle: Mutex::new(EpochLifecycle::default()),
            registration_arena: Mutex::new(Some(registration_arena)),
            module: Mutex::new(Some(module)),
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
        lock_unpoisoned(&self.0.registration_arena)
            .as_ref()
            .expect("published epoch registration arena must be resident")
            .owner_pin()
    }

    pub fn targets(&self) -> &[TargetDefinition] {
        &self.0.targets
    }

    pub fn target_set_hash(&self) -> TargetSetHash {
        self.0.target_set_hash
    }

    pub fn registrations(&self) -> &RegistrationSet {
        &self.0.registration
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
        lock_unpoisoned(&self.0.lifecycle).draining = true;
    }

    /// Report any contained module callback failure, including drop/free/update
    /// thunks. The shared token makes the fence immediately visible to values
    /// and every snapshot that pins this epoch.
    pub fn report_runtime_failure(&self, error: impl Into<String>) {
        self.0
            .poison(PipelinePoisonCode::PublishedCallbackRejected, error.into());
    }

    pub fn report_runtime_panic(&self, error: impl Into<String>) {
        self.0
            .poison(PipelinePoisonCode::PublishedCallbackPanic, error.into());
    }

    pub fn drain_complete(&self) -> bool {
        self.0.observe_token_poison();
        let lifecycle = lock_unpoisoned(&self.0.lifecycle);
        lifecycle.draining
            && lifecycle.runtime_error.is_none()
            && self.0.active_jobs.load(Ordering::Acquire) == 0
    }

    pub fn status(&self) -> EpochStatus {
        self.0.observe_token_poison();
        let lifecycle = lock_unpoisoned(&self.0.lifecycle);
        EpochStatus {
            accepting_new_work: self.0.accepting.load(Ordering::Acquire),
            active_jobs: self.0.active_jobs.load(Ordering::Acquire),
            draining: lifecycle.draining,
            poisoned: lifecycle.runtime_error.is_some(),
            unloaded: lifecycle.unloaded,
        }
    }
}

impl EpochInner {
    fn observe_token_poison(&self) {
        if let Some(cause) = self.token.poison_cause() {
            let (code, detail) = match cause {
                ModuleEpochPoisonCause::CallbackPanic => (
                    PipelinePoisonCode::PublishedCallbackPanic,
                    "a module-owned no-unwind thunk contained a callback panic",
                ),
                ModuleEpochPoisonCause::CallbackRejected => (
                    PipelinePoisonCode::PublishedCallbackRejected,
                    "a module-owned status thunk returned a rejected status",
                ),
            };
            self.poison(code, detail.to_owned());
        }
    }

    fn poison(&self, code: PipelinePoisonCode, detail: String) {
        self.token.poison_with(match code {
            PipelinePoisonCode::PublishedCallbackPanic => ModuleEpochPoisonCause::CallbackPanic,
            _ => ModuleEpochPoisonCause::CallbackRejected,
        });
        self.accepting.store(false, Ordering::Release);
        let mut lifecycle = lock_unpoisoned(&self.lifecycle);
        if lifecycle.runtime_error.is_none() {
            lifecycle.runtime_error = Some((code, detail));
        }
    }

    fn rejection(&self) -> EpochWorkError {
        let lifecycle = lock_unpoisoned(&self.lifecycle);
        match &lifecycle.runtime_error {
            Some((code, detail)) => EpochWorkError::Poisoned(pipeline_poison(
                *code,
                PipelinePoisonOrigin::PublishedRuntime,
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
            let arena = self
                .registration_arena
                .get_mut()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            let module = self
                .module
                .get_mut()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
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
    Poisoned(PipelinePoison),
    Retired { epoch_id: u64 },
}

impl Drop for EpochJobGuard {
    fn drop(&mut self) {
        self.epoch.active_jobs.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
enum PublishedState {
    Ready(PipelineEpoch),
    Poisoned(PipelinePoison),
}

#[derive(Clone)]
pub struct PipelineSnapshot {
    state: PublishedState,
}

impl PipelineSnapshot {
    pub fn epoch(&self) -> Result<&PipelineEpoch, PipelinePoison> {
        match &self.state {
            PublishedState::Poisoned(error) => Err(error.clone()),
            PublishedState::Ready(epoch) => {
                epoch.0.observe_token_poison();
                let lifecycle = lock_unpoisoned(&epoch.0.lifecycle);
                if let Some((code, detail)) = &lifecycle.runtime_error {
                    Err(pipeline_poison(
                        *code,
                        PipelinePoisonOrigin::PublishedRuntime,
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
    bootstrap_authority: Option<&'static HostBootstrapAuthorityV1>,
    next_epoch_id: u64,
    published: Option<PublishedState>,
    retired: Vec<PipelineEpoch>,
}

impl ModuleHost {
    pub fn new(state_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let state_dir = state_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(state_dir.join("modules"))?;
        Ok(Self {
            state_dir,
            bootstrap_authority: None,
            next_epoch_id: 1,
            published: None,
            retired: Vec::new(),
        })
    }

    pub fn new_with_bootstrap_authority(
        state_dir: impl AsRef<Path>,
        bootstrap_authority: &'static HostBootstrapAuthorityV1,
    ) -> std::io::Result<Self> {
        let mut host = Self::new(state_dir)?;
        host.bootstrap_authority = Some(bootstrap_authority);
        Ok(host)
    }

    pub fn snapshot(&self) -> PipelineSnapshot {
        let state = self.published.clone().unwrap_or_else(|| {
            PublishedState::Poisoned(pipeline_poison(
                PipelinePoisonCode::CandidateOpen,
                PipelinePoisonOrigin::CandidateOpen,
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
    ) -> Result<PipelineEpoch, PipelinePoison> {
        match self.prepare_candidate(source, &mut requirements, loader) {
            Ok(epoch) => {
                self.install_ready(epoch.clone());
                Ok(epoch)
            }
            Err(poison) => {
                self.install_poison(poison.clone());
                Err(poison)
            }
        }
    }

    pub(crate) fn prepare_candidate(
        &mut self,
        source: &Path,
        requirements: &mut CandidateRequirements,
        loader: &mut dyn PipelineModuleLoader,
    ) -> Result<PipelineEpoch, PipelinePoison> {
        let id = self.mint_epoch_id();
        let Some(bootstrap_authority) = self.bootstrap_authority else {
            return Err(candidate_poison_record(
                PipelinePoisonCode::CandidateValidation,
                "host has no decoded, DSCI-keyed bootstrap-control authority".to_owned(),
                CandidateCleanupDisposition::None,
            ));
        };
        let target_set = match validate_requirements(requirements, bootstrap_authority) {
            Ok(target_set) => target_set,
            Err(error) => {
                return Err(candidate_poison_record(
                    PipelinePoisonCode::CandidateValidation,
                    error,
                    CandidateCleanupDisposition::None,
                ))
            }
        };
        let staged = match self.stage_copy(id, source) {
            Ok(staged) => staged,
            Err(error) => {
                return Err(candidate_poison_record(
                    PipelinePoisonCode::CandidateOpen,
                    error,
                    CandidateCleanupDisposition::None,
                ))
            }
        };
        if let Err(error) = CodeLoadingPolicy::authorize(CodeLoadRequest::HostPipelineModule {
            staged_copy: true,
            content_hash: Some(staged.content_hash),
        }) {
            return Err(candidate_poison_record(
                PipelinePoisonCode::CandidateOpen,
                error.to_string(),
                CandidateCleanupDisposition::None,
            ));
        }
        let mut module = match boundary_call("open", || loader.open_staged(&staged)) {
            Ok(module) => module,
            Err(error) => {
                return Err(candidate_poison_record(
                    PipelinePoisonCode::CandidateOpen,
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

        let validation = validate_open_module(
            module.as_mut(),
            requirements,
            bootstrap_authority,
            &mut registration_arena,
        );
        let registration = match validation {
            Ok(registration) => registration,
            Err(error) => {
                let cleanup = discard_candidate(module, registration_arena);
                return Err(candidate_poison_with_cleanup(
                    error.code,
                    error.detail,
                    cleanup,
                ));
            }
        };
        if token.is_poisoned() {
            let cleanup = discard_candidate(module, registration_arena);
            return Err(candidate_poison_with_cleanup(
                PipelinePoisonCode::CandidateRegistration,
                "candidate token was poisoned during registration".to_owned(),
                cleanup,
            ));
        }

        let epoch = PipelineEpoch::new(
            id,
            staged,
            token,
            target_set,
            registration,
            registration_arena,
            module,
        );
        Ok(epoch)
    }

    /// Attempt cleanup of every retired image. Runtime poison is checked before
    /// active jobs or Arc pins: it is a permanent fence, not a delayed unload.
    pub fn reap_retired(&mut self) -> Vec<UnloadOutcome> {
        let mut outcomes = Vec::with_capacity(self.retired.len());
        let mut index = 0;
        while index < self.retired.len() {
            let epoch = &self.retired[index];
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
                        PipelinePoisonCode::PublishedCleanup,
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

    pub fn retired_poison(&self, id: u64) -> Option<PipelinePoison> {
        let epoch = self.retired.iter().find(|epoch| epoch.id() == id)?;
        let lifecycle = lock_unpoisoned(&epoch.0.lifecycle);
        let (code, message) = lifecycle.runtime_error.as_ref()?;
        Some(pipeline_poison(
            *code,
            PipelinePoisonOrigin::PublishedRuntime,
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

    pub(crate) fn install_poison(&mut self, poison: PipelinePoison) {
        self.retire_published_ready();
        self.published = Some(PublishedState::Poisoned(poison));
    }

    pub(crate) fn install_ready(&mut self, epoch: PipelineEpoch) {
        self.retire_published_ready();
        self.published = Some(PublishedState::Ready(epoch));
    }

    pub(crate) fn discard_unpublished(&mut self, epoch: PipelineEpoch) -> Option<PipelinePoison> {
        epoch.begin_drain();
        match unload_epoch(&epoch) {
            Ok(()) => None,
            Err(error) => {
                epoch.0.poison(
                    PipelinePoisonCode::PublishedCleanup,
                    format!(
                        "unpublished candidate cleanup disposition={}: {}",
                        cleanup_disposition_name(error.disposition),
                        error.detail
                    ),
                );
                let poison = match (PipelineSnapshot {
                    state: PublishedState::Ready(epoch.clone()),
                })
                .epoch()
                {
                    Err(poison) => poison,
                    Ok(_) => unreachable!("discard cleanup poison must fence the epoch"),
                };
                self.retired.push(epoch);
                Some(poison)
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

fn candidate_poison_with_cleanup(
    initiating_code: PipelinePoisonCode,
    detail: String,
    cleanup: CandidateCleanup,
) -> PipelinePoison {
    let detail = format!(
        "{detail}; candidate cleanup disposition={}: {}",
        cleanup_disposition_name(cleanup.disposition),
        cleanup.detail
    );
    let code = if cleanup.disposition == CandidateCleanupDisposition::CleanedAndClosed {
        initiating_code
    } else {
        PipelinePoisonCode::CandidateCleanup
    };
    candidate_poison_record(code, detail, cleanup.disposition)
}

fn candidate_poison_record(
    code: PipelinePoisonCode,
    detail: String,
    cleanup: CandidateCleanupDisposition,
) -> PipelinePoison {
    pipeline_poison(code, PipelinePoisonOrigin::CandidateOpen, cleanup, detail)
}

#[derive(Debug)]
pub enum DurablePublishError {
    Candidate(PipelinePoison),
    Store {
        source: Box<StoreError>,
        cleanup_poison: Option<Box<PipelinePoison>>,
    },
    SchemaAcceptanceRequired,
}

/// Production publication coordinator. A prepared module remains invisible
/// to in-memory snapshots until the exact store epoch is durably committed.
pub struct DurableModuleHost {
    host: ModuleHost,
    store: Store,
    pending: Option<PipelineEpoch>,
}

impl DurableModuleHost {
    pub fn from_parts(host: ModuleHost, store: Store) -> Self {
        Self {
            host,
            store,
            pending: None,
        }
    }

    pub fn snapshot(&self) -> PipelineSnapshot {
        self.host.snapshot()
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    pub fn pending_candidate(&self) -> Option<&PipelineEpoch> {
        self.pending.as_ref()
    }

    pub fn publish_candidate(
        &mut self,
        source: &Path,
        mut requirements: CandidateRequirements,
        loader: &mut dyn PipelineModuleLoader,
    ) -> Result<PipelineEpoch, DurablePublishError> {
        let prepared = match self
            .host
            .prepare_candidate(source, &mut requirements, loader)
        {
            Ok(epoch) => epoch,
            Err(poison) => {
                self.store
                    .input_transaction(|transaction| transaction.publish_pipeline_poison(&poison))
                    .map_err(|source| DurablePublishError::Store {
                        source: Box::new(source),
                        cleanup_poison: None,
                    })?;
                self.host.install_poison(poison.clone());
                return Err(DurablePublishError::Candidate(poison));
            }
        };

        let stored = match stored_pipeline_epoch(&prepared, &requirements) {
            Ok(stored) => stored,
            Err(source) => {
                let cleanup_poison = self.host.discard_unpublished(prepared);
                return Err(DurablePublishError::Store {
                    source: Box::new(source),
                    cleanup_poison: cleanup_poison.map(Box::new),
                });
            }
        };
        if let Err(source) = self
            .store
            .input_transaction(|transaction| transaction.publish_pipeline_epoch(&stored))
        {
            let cleanup_poison = self.host.discard_unpublished(prepared);
            return Err(DurablePublishError::Store {
                source: Box::new(source),
                cleanup_poison: cleanup_poison.map(Box::new),
            });
        }

        match self.store.pipeline_state() {
            Ok(Some(StoredPipelineState::Ready(epoch)))
                if epoch.dylib_hash == prepared.dylib_hash() =>
            {
                self.discard_pending();
                self.host.install_ready(prepared.clone());
                Ok(prepared)
            }
            Ok(Some(StoredPipelineState::SchemaAcceptanceRequired { .. })) => {
                self.discard_pending();
                self.pending = Some(prepared);
                Err(DurablePublishError::SchemaAcceptanceRequired)
            }
            Ok(_) => {
                let cleanup_poison = self.host.discard_unpublished(prepared);
                Err(DurablePublishError::Store {
                    source: Box::new(StoreError::InvalidPipelineEpoch {
                        detail: "store did not publish the prepared candidate as Ready or acceptance-required",
                    }),
                    cleanup_poison: cleanup_poison.map(Box::new),
                })
            }
            Err(source) => {
                let cleanup_poison = self.host.discard_unpublished(prepared);
                Err(DurablePublishError::Store {
                    source: Box::new(source),
                    cleanup_poison: cleanup_poison.map(Box::new),
                })
            }
        }
    }

    /// Persist the runtime poison for the currently published dylib before
    /// exposing the explicit report through the in-memory epoch.
    pub fn report_runtime_failure(
        &mut self,
        detail: impl Into<String>,
    ) -> Result<PipelinePoison, StoreError> {
        let detail = detail.into();
        let epoch = match &self.host.published {
            Some(PublishedState::Ready(epoch)) => epoch.clone(),
            _ => {
                return Err(StoreError::InvalidPipelineEpoch {
                    detail: "no ready in-memory epoch exists for runtime poison",
                })
            }
        };
        let poison = pipeline_poison(
            PipelinePoisonCode::PublishedCallbackRejected,
            PipelinePoisonOrigin::PublishedRuntime,
            CandidateCleanupDisposition::PublishedEpochLeaked,
            detail.clone(),
        );
        self.store
            .poison_published_pipeline_epoch(epoch.dylib_hash(), &poison)?;
        epoch.report_runtime_failure(detail);
        Ok(poison)
    }

    /// Persist a poison already latched by a module-owned status token.
    pub fn sync_runtime_poison(&mut self) -> Result<Option<PipelinePoison>, StoreError> {
        let epoch = match &self.host.published {
            Some(PublishedState::Ready(epoch)) => epoch.clone(),
            _ => return Ok(None),
        };
        let poison = match self.host.snapshot().epoch() {
            Ok(_) => return Ok(None),
            Err(poison) if poison.origin == PipelinePoisonOrigin::PublishedRuntime => poison,
            Err(_) => return Ok(None),
        };
        self.store
            .poison_published_pipeline_epoch(epoch.dylib_hash(), &poison)?;
        Ok(Some(poison))
    }

    fn discard_pending(&mut self) {
        if let Some(epoch) = self.pending.take() {
            let _ = self.host.discard_unpublished(epoch);
        }
    }
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
    if target_set.digest != prepared.target_set_hash() {
        return Err(StoreError::InvalidPipelineEpoch {
            detail: "prepared module target set differs from its retained DSTS",
        });
    }
    let policy = requirements
        .compiled_types
        .rows
        .iter()
        .map(|row| (row.type_uuid, row.build_only))
        .collect::<Vec<_>>();
    let schema_registry = requirements
        .compiled_types
        .rows
        .iter()
        .map(|row| (row.type_uuid, row.logical_hash))
        .collect::<BTreeMap<_, _>>();
    let registrations = prepared
        .registrations()
        .registrations
        .iter()
        .filter_map(|registration| {
            let kind = match registration.kind {
                RegistrationKind::Importer => StoredRegistrationKind::Importer,
                RegistrationKind::Processor => StoredRegistrationKind::Processor,
                RegistrationKind::Validator
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
        load_policy_digest: load_policy_digest(&policy),
        compiled_types: requirements.compiled_types.digest,
        target_set,
        schema_registry,
        registrations,
    };
    let authority = consumer_authority(requirements)?;
    ValidatedPipelineEpoch::validate(epoch, &requirements.compiled_types, authority)
}

fn consumer_authority(
    requirements: &CandidateRequirements,
) -> Result<&'static HostBootstrapAuthorityV1, StoreError> {
    let authority =
        distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1().map_err(|_| {
            StoreError::InvalidPipelineEpoch {
                detail: "consumer bootstrap authority resource is invalid",
            }
        })?;
    if &requirements.identity.compilation != authority.compilation_identity() {
        return Err(StoreError::InvalidPipelineEpoch {
            detail: "candidate DSCI differs from the store bootstrap authority",
        });
    }
    Ok(authority)
}

fn validate_requirements(
    requirements: &mut CandidateRequirements,
    bootstrap_authority: &HostBootstrapAuthorityV1,
) -> Result<CanonicalTargetSet, String> {
    if &requirements.identity.compilation != bootstrap_authority.compilation_identity() {
        return Err(
            "candidate CompilationIdentity does not match the host bootstrap resource DSCI"
                .to_owned(),
        );
    }
    validate_candidate_linkage(&requirements.native_dependencies)
        .map_err(|error| error.to_string())?;
    if requirements.identity.module_abi.panic_strategy != "unwind" {
        return Err("pipeline and daemon must both use panic = unwind".to_owned());
    }
    if requirements.identity.module_abi.allocator != "system" {
        return Err("pipeline and daemon must both use the system allocator".to_owned());
    }
    requirements.measured_layouts.sort();
    if requirements
        .measured_layouts
        .windows(2)
        .any(|pair| pair[0].type_id == pair[1].type_id)
    {
        return Err("expected measured-layout table contains a duplicate type".to_owned());
    }
    validate_compiled_table(&requirements.compiled_types, "expected")?;
    validate_bootstrap_logical_authority(
        &requirements.compiled_types.rows,
        BundleFormatVersion::V1,
    )
    .map_err(|error| format!("expected bootstrap-control authority invalid: {error}"))?;
    validate_bootstrap_authority(
        &requirements.compiled_types.rows,
        bootstrap_authority.table(),
        BundleFormatVersion::V1,
    )
    .map_err(|error| format!("expected bootstrap-control authority mismatch: {error}"))?;
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
    bootstrap_authority: &HostBootstrapAuthorityV1,
    registration_arena: &mut CandidateRegistrationArena,
) -> Result<RegistrationSet, CandidatePhaseError> {
    let identity = boundary_call("identity", || module.identity())
        .map_err(|error| CandidatePhaseError::attestation(error.to_string()))?;
    if identity != requirements.identity {
        return Err(CandidatePhaseError::attestation(
            "module compilation/interface identity mismatch",
        ));
    }
    let mut layouts = boundary_call("measured_layouts", || module.measured_layouts())
        .map_err(|error| CandidatePhaseError::attestation(error.to_string()))?;
    layouts.sort();
    if layouts != requirements.measured_layouts {
        return Err(CandidatePhaseError::attestation(measured_layout_error(
            &requirements.measured_layouts,
            &layouts,
        )));
    }
    let compiled_types = boundary_call("compiled_types", || module.compiled_types())
        .map_err(|error| CandidatePhaseError::attestation(error.to_string()))?;
    validate_compiled_table(&compiled_types, "module").map_err(CandidatePhaseError::attestation)?;
    validate_bootstrap_logical_authority(&compiled_types.rows, BundleFormatVersion::V1).map_err(
        |error| {
            CandidatePhaseError::attestation(format!(
                "module bootstrap-control authority invalid: {error}"
            ))
        },
    )?;
    validate_bootstrap_authority(
        &compiled_types.rows,
        bootstrap_authority.table(),
        BundleFormatVersion::V1,
    )
    .map_err(|error| {
        CandidatePhaseError::attestation(format!(
            "module bootstrap-control authority mismatch: {error}"
        ))
    })?;
    if compiled_types != requirements.compiled_types {
        return Err(CandidatePhaseError::attestation(compiled_type_error(
            &requirements.compiled_types.rows,
            &compiled_types.rows,
        )));
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
    code: PipelinePoisonCode,
    detail: String,
}

impl CandidatePhaseError {
    fn attestation(detail: impl Into<String>) -> Self {
        Self {
            code: PipelinePoisonCode::CandidateAttestation,
            detail: detail.into(),
        }
    }

    fn registration(detail: impl Into<String>) -> Self {
        Self {
            code: PipelinePoisonCode::CandidateRegistration,
            detail: detail.into(),
        }
    }
}

fn measured_layout_error(expected: &[MeasuredLayout], actual: &[MeasuredLayout]) -> String {
    let expected = expected
        .iter()
        .map(|layout| (&layout.type_id, layout.digest))
        .collect::<BTreeMap<_, _>>();
    let actual = actual
        .iter()
        .map(|layout| (&layout.type_id, layout.digest))
        .collect::<BTreeMap<_, _>>();
    let differing = expected
        .keys()
        .chain(actual.keys())
        .find(|type_id| expected.get(*type_id) != actual.get(*type_id));
    match differing {
        Some(type_id) => format!("measured layout mismatch for type `{type_id}`"),
        None => "measured layout table mismatch".to_owned(),
    }
}

fn validate_compiled_table(table: &CompiledTypeTable, role: &str) -> Result<(), String> {
    table
        .validate()
        .map_err(|error| format!("{role} compiled-type table/DSCA invalid: {error}"))
}

fn compiled_type_error(
    expected: &[CompiledTypeAttestation],
    actual: &[CompiledTypeAttestation],
) -> String {
    let expected_by_type = expected
        .iter()
        .map(|row| (row.type_uuid, row))
        .collect::<BTreeMap<_, _>>();
    let actual_by_type = actual
        .iter()
        .map(|row| (row.type_uuid, row))
        .collect::<BTreeMap<_, _>>();
    let type_uuid = expected_by_type
        .keys()
        .chain(actual_by_type.keys())
        .find(|type_uuid| expected_by_type.get(type_uuid) != actual_by_type.get(type_uuid));
    let Some(type_uuid) = type_uuid else {
        return "compiled-type attestation table mismatch".to_owned();
    };
    let (Some(expected), Some(actual)) = (
        expected_by_type.get(type_uuid),
        actual_by_type.get(type_uuid),
    ) else {
        return format!("compiled-type coverage mismatch for type `{type_uuid}`");
    };
    let field = if expected.logical_hash != actual.logical_hash {
        "logical hash"
    } else if expected.native_layout_digest != actual.native_layout_digest {
        "native layout digest"
    } else if expected.build_only != actual.build_only {
        "build_only"
    } else if expected.registry_extras_digest != actual.registry_extras_digest
        || expected.registry_extras != actual.registry_extras
    {
        "registry extras"
    } else {
        "row"
    };
    format!("compiled-type {field} mismatch for type `{type_uuid}`")
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

fn pipeline_poison(
    code: PipelinePoisonCode,
    origin: PipelinePoisonOrigin,
    cleanup: CandidateCleanupDisposition,
    message: impl Into<String>,
) -> PipelinePoison {
    PipelinePoison::new(code, origin, cleanup, message)
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

fn unload_epoch(epoch: &PipelineEpoch) -> Result<(), EpochCleanupError> {
    let mut arena_guard = lock_unpoisoned(&epoch.0.registration_arena);
    let arena = arena_guard.as_mut().ok_or_else(|| EpochCleanupError {
        disposition: CandidateCleanupDisposition::RegistrationCleanupFailed,
        detail: "registration arena is absent".to_owned(),
    })?;
    arena.fence();
    arena.cleanup_reverse().map_err(|error| EpochCleanupError {
        disposition: CandidateCleanupDisposition::RegistrationCleanupFailed,
        detail: error.to_string(),
    })?;

    let mut module_guard = lock_unpoisoned(&epoch.0.module);
    let module = module_guard.as_mut().ok_or_else(|| EpochCleanupError {
        disposition: CandidateCleanupDisposition::ModuleUnloadFailed,
        detail: "module handle is absent".to_owned(),
    })?;
    boundary_call("unload", || module.unload()).map_err(|error| EpochCleanupError {
        disposition: CandidateCleanupDisposition::ModuleUnloadFailed,
        detail: error.to_string(),
    })?;
    if epoch.0.token.is_poisoned() {
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
    module_guard.take();
    arena_guard.take();
    lock_unpoisoned(&epoch.0.lifecycle).unloaded = true;
    Ok(())
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
