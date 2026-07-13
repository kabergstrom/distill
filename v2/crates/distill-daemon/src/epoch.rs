//! Pipeline module staging, publication, poison, and unload fencing.
//!
//! The concrete `libloading` implementation belongs in the shared audited
//! module-host layer. This crate owns the daemon state machine around that
//! boundary and represents the host through [`PipelineModuleLoader`]. Identity
//! and measured-layout probes are called before the Rust-ABI registration call.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use distill_asset::ModuleEpochToken;

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
    /// Canonical §5 compilation-identity record. Kept encoded so the daemon
    /// compares the exact attestation produced by source-walk.
    pub compilation: Vec<u8>,
    pub module_abi: ModuleAbiIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MeasuredLayout {
    pub type_id: String,
    pub digest: [u8; 32],
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateRequirements {
    pub identity: ModuleIdentity,
    pub measured_layouts: Vec<MeasuredLayout>,
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
}

impl std::fmt::Display for ModuleCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for ModuleCallError {}

/// Audited table after its repr(C) ABI prefix has been located. The first two
/// calls must be C-ABI probes in a concrete loader; `register` is reached only
/// after their values compare equal.
pub trait LoadedPipelineModule: Send {
    fn identity(&mut self) -> Result<ModuleIdentity, ModuleCallError>;
    fn measured_layouts(&mut self) -> Result<Vec<MeasuredLayout>, ModuleCallError>;
    fn register(
        &mut self,
        targets: &[TargetDefinition],
    ) -> Result<RegistrationSet, ModuleCallError>;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoisonEntryPoint {
    CandidateOpen,
    PublishedRuntime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelinePoison {
    pub entry_point: PoisonEntryPoint,
    pub detail: String,
}

impl std::fmt::Display for PipelinePoison {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pipeline {:?} poison: {}", self.entry_point, self.detail)
    }
}

impl std::error::Error for PipelinePoison {}

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
    registration: RegistrationSet,
    accepting: AtomicBool,
    active_jobs: AtomicUsize,
    lifecycle: Mutex<EpochLifecycle>,
    module: Mutex<Option<Box<dyn LoadedPipelineModule>>>,
}

#[derive(Default)]
struct EpochLifecycle {
    draining: bool,
    runtime_error: Option<String>,
    unloaded: bool,
}

impl PipelineEpoch {
    fn new(
        id: u64,
        staged: StagedModule,
        targets: Vec<TargetDefinition>,
        registration: RegistrationSet,
        module: Box<dyn LoadedPipelineModule>,
    ) -> Self {
        Self(Arc::new(EpochInner {
            id,
            staged,
            token: ModuleEpochToken::new(id),
            targets,
            registration,
            accepting: AtomicBool::new(true),
            active_jobs: AtomicUsize::new(0),
            lifecycle: Mutex::new(EpochLifecycle::default()),
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

    pub fn targets(&self) -> &[TargetDefinition] {
        &self.0.targets
    }

    pub fn registrations(&self) -> &RegistrationSet {
        &self.0.registration
    }

    pub fn try_start_job(&self) -> Result<EpochJobGuard, PipelinePoison> {
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
        self.0.poison(error.into());
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
        if self.token.is_poisoned() {
            self.poison("a module-owned status thunk reported failure".to_owned());
        }
    }

    fn poison(&self, detail: String) {
        self.token.poison();
        self.accepting.store(false, Ordering::Release);
        let mut lifecycle = lock_unpoisoned(&self.lifecycle);
        if lifecycle.runtime_error.is_none() {
            lifecycle.runtime_error = Some(detail);
        }
    }

    fn rejection(&self) -> PipelinePoison {
        let lifecycle = lock_unpoisoned(&self.lifecycle);
        match &lifecycle.runtime_error {
            Some(detail) => PipelinePoison {
                entry_point: PoisonEntryPoint::PublishedRuntime,
                detail: detail.clone(),
            },
            None => PipelinePoison {
                entry_point: PoisonEntryPoint::CandidateOpen,
                detail: "epoch is retired and no longer accepts new work".to_owned(),
            },
        }
    }
}

impl Drop for EpochInner {
    fn drop(&mut self) {
        if self.token.is_poisoned() {
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
        }
    }
}

pub struct EpochJobGuard {
    epoch: Arc<EpochInner>,
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
                if let Some(detail) = &lifecycle.runtime_error {
                    Err(PipelinePoison {
                        entry_point: PoisonEntryPoint::PublishedRuntime,
                        detail: detail.clone(),
                    })
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
        })
    }

    pub fn snapshot(&self) -> PipelineSnapshot {
        let state = self.published.clone().unwrap_or_else(|| {
            PublishedState::Poisoned(PipelinePoison {
                entry_point: PoisonEntryPoint::CandidateOpen,
                detail: "no pipeline epoch has been published".to_owned(),
            })
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
        let id = self.mint_epoch_id();
        if let Err(error) = validate_requirements(&mut requirements) {
            return Err(self.publish_candidate_poison(error));
        }
        let staged = match self.stage_copy(id, source) {
            Ok(staged) => staged,
            Err(error) => return Err(self.publish_candidate_poison(error)),
        };
        if let Err(error) = CodeLoadingPolicy::authorize(CodeLoadRequest::HostPipelineModule {
            staged_copy: true,
            content_hash: Some(staged.content_hash),
        }) {
            return Err(self.publish_candidate_poison(error.to_string()));
        }
        let mut module = match boundary_call("open", || loader.open_staged(&staged)) {
            Ok(module) => module,
            Err(error) => return Err(self.publish_candidate_poison(error.to_string())),
        };

        let validation = validate_open_module(module.as_mut(), &requirements);
        let registration = match validation {
            Ok(registration) => registration,
            Err(error) => {
                discard_candidate(module);
                return Err(self.publish_candidate_poison(error));
            }
        };

        let epoch = PipelineEpoch::new(id, staged, requirements.targets, registration, module);
        self.retire_published_ready();
        self.published = Some(PublishedState::Ready(epoch.clone()));
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
                    epoch.0.poison(format!("module unload failed: {error}"));
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
        let path = self
            .state_dir
            .join("modules")
            .join(format!("pipeline-{id}.{extension}"));
        std::fs::copy(source, &path).map_err(|error| {
            format!("stage {} as {}: {error}", source.display(), path.display())
        })?;
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("hash staged module {}: {error}", path.display()))?;
        Ok(StagedModule {
            path,
            content_hash: *blake3::hash(&bytes).as_bytes(),
        })
    }

    fn publish_candidate_poison(&mut self, detail: String) -> PipelinePoison {
        let poison = PipelinePoison {
            entry_point: PoisonEntryPoint::CandidateOpen,
            detail,
        };
        self.retire_published_ready();
        self.published = Some(PublishedState::Poisoned(poison.clone()));
        poison
    }

    fn retire_published_ready(&mut self) {
        if let Some(PublishedState::Ready(epoch)) = self.published.take() {
            epoch.begin_drain();
            self.retired.push(epoch);
        }
    }
}

fn validate_requirements(requirements: &mut CandidateRequirements) -> Result<(), String> {
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
    requirements
        .targets
        .sort_by(|left, right| left.name.cmp(&right.name));
    if requirements
        .targets
        .windows(2)
        .any(|pair| pair[0].name == pair[1].name)
    {
        return Err("target configuration contains a duplicate target name".to_owned());
    }
    Ok(())
}

fn validate_open_module(
    module: &mut dyn LoadedPipelineModule,
    requirements: &CandidateRequirements,
) -> Result<RegistrationSet, String> {
    let identity =
        boundary_call("identity", || module.identity()).map_err(|error| error.to_string())?;
    if identity != requirements.identity {
        return Err("module compilation/interface identity mismatch".to_owned());
    }
    let mut layouts = boundary_call("measured_layouts", || module.measured_layouts())
        .map_err(|error| error.to_string())?;
    layouts.sort();
    if layouts != requirements.measured_layouts {
        return Err(measured_layout_error(
            &requirements.measured_layouts,
            &layouts,
        ));
    }
    let registration = boundary_call("register", || module.register(&requirements.targets))
        .map_err(|error| error.to_string())?;
    validate_registration(&registration, &requirements.targets)?;
    Ok(registration)
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

fn discard_candidate(mut module: Box<dyn LoadedPipelineModule>) {
    if boundary_call("candidate unload", || module.unload()).is_err() {
        std::mem::forget(module);
        return;
    }
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| module.dlclose())).is_err() {
        std::mem::forget(module);
    }
}

fn unload_epoch(epoch: &PipelineEpoch) -> Result<(), ModuleCallError> {
    let mut module_guard = lock_unpoisoned(&epoch.0.module);
    let module = module_guard
        .as_mut()
        .ok_or_else(|| ModuleCallError::new("module handle is absent"))?;
    boundary_call("unload", || module.unload())?;
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| module.dlclose()))
        .map_err(|_| ModuleCallError::boundary_panic("dlclose"))?;
    module_guard.take();
    lock_unpoisoned(&epoch.0.lifecycle).unloaded = true;
    Ok(())
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
