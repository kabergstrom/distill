use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use distill_asset::{ErasedValue, ModuleEpochPoisonCause, ModuleEpochToken};
use distill_core::bootstrap::bootstrap_control_logical_registry_v1;
use distill_core::id::{ContentHash, LogicalHash, TypeUuid};
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
use distill_daemon::epoch::{
    CandidateCleanupDisposition, CandidateRegistrationArena, CandidateRequirements,
    DurableModuleHost, DurablePublishError, EpochWorkError, HostCallbackBoundary,
    HostCallbackSurface, LoadedPipelineModule, ModuleAbiIdentity, ModuleCallError, ModuleEpochPin,
    ModuleHost, PipelineModuleLoader, PipelinePoisonCode, PipelinePoisonOrigin, Registration,
    RegistrationDisposition, RegistrationKind, RegistrationResource, RegistrationSet, StagedModule,
    TargetDefinition, UnloadOutcome,
};
use distill_store::pipeline::{
    AcceptedSchemaEpoch, AcceptedTypeLineage, SchemaLineageManifest, TypeAuthorityState,
    VerifiedSchemaLineageManifest,
};
use distill_store::state::PipelineState as StoredPipelineState;
use distill_store::{Store, StoreConfig};

#[distill_asset::asset(uuid = "30112233-4455-6677-8899-aabbccddeeff")]
struct PanickingAssetDrop;

impl Drop for PanickingAssetDrop {
    fn drop(&mut self) {
        panic!("module asset drop failed")
    }
}

#[derive(Default, Debug)]
struct Calls {
    register: usize,
    unload: usize,
    dlclose: usize,
    cleanup_order: Vec<String>,
    target_order: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupBehavior {
    Ok,
    Error,
    Panic,
}

struct TestRegistrationResource {
    label: String,
    behavior: CleanupBehavior,
    calls: Arc<Mutex<Calls>>,
}

unsafe fn cleanup_test_registration(pointer: *mut u8) -> Result<(), ModuleCallError> {
    // SAFETY: every pointer passed here was produced by Box::into_raw for this
    // exact type and the arena invokes the thunk at most once after success.
    let resource = unsafe { &*pointer.cast::<TestRegistrationResource>() };
    resource
        .calls
        .lock()
        .unwrap()
        .cleanup_order
        .push(resource.label.clone());
    match resource.behavior {
        CleanupBehavior::Ok => {
            // SAFETY: this successful status consumes the still-owned box.
            drop(unsafe { Box::from_raw(pointer.cast::<TestRegistrationResource>()) });
            Ok(())
        }
        CleanupBehavior::Error => Err(ModuleCallError::new(format!(
            "{} cleanup failed",
            resource.label
        ))),
        CleanupBehavior::Panic => panic!("{} cleanup panicked", resource.label),
    }
}

struct FakeModule {
    source_identity: ngp_module_host::ModuleSourceIdentity,
    module_abi: ModuleAbiIdentity,
    registration: RegistrationSet,
    calls: Arc<Mutex<Calls>>,
    cleanup_behaviors: Vec<CleanupBehavior>,
    unload_error: Option<&'static str>,
    unload_panics: bool,
    poison_during_register: bool,
    capsule_owner_mismatch: bool,
    panic_after_registration: bool,
    ignore_registration_errors: bool,
    pin_sink: Option<Arc<Mutex<Option<ModuleEpochPin>>>>,
}

impl LoadedPipelineModule for FakeModule {
    fn source_identity(
        &mut self,
    ) -> Result<ngp_module_host::ModuleSourceIdentity, ModuleCallError> {
        Ok(self.source_identity.clone())
    }

    fn module_abi(&mut self) -> Result<ModuleAbiIdentity, ModuleCallError> {
        Ok(self.module_abi.clone())
    }

    fn register(
        &mut self,
        targets: &[TargetDefinition],
        arena: &mut CandidateRegistrationArena,
    ) -> Result<BTreeSet<String>, ModuleCallError> {
        {
            let mut calls = self.calls.lock().unwrap();
            calls.register += 1;
            calls.target_order = targets.iter().map(|target| target.name.clone()).collect();
        }
        if let Some(pin_sink) = &self.pin_sink {
            *pin_sink.lock().unwrap() = Some(arena.owner_pin());
        }
        if self.poison_during_register {
            arena.owner_token().poison();
        }
        for (index, registration) in self.registration.registrations.iter().enumerate() {
            let resource = Box::new(TestRegistrationResource {
                label: format!("{}#{index}", registration.id),
                behavior: self
                    .cleanup_behaviors
                    .get(index)
                    .copied()
                    .unwrap_or(CleanupBehavior::Ok),
                calls: Arc::clone(&self.calls),
            });
            // SAFETY: `cleanup_test_registration` has the exact pointee type,
            // owns the allocation on success, and preserves it on failure.
            let owner = if self.capsule_owner_mismatch {
                ModuleEpochToken::new(u64::MAX)
            } else {
                arena.owner_token().clone()
            };
            let resource = unsafe {
                RegistrationResource::from_raw(
                    Box::into_raw(resource).cast(),
                    owner,
                    cleanup_test_registration,
                )
            };
            let status = arena.install(registration.clone(), resource);
            assert_eq!(status.disposition, RegistrationDisposition::Consumed);
            let result = status.into_result();
            if !self.ignore_registration_errors {
                result?;
            }
        }
        assert!(
            !self.panic_after_registration,
            "configured panic after capsule transfer"
        );
        Ok(self.registration.pipeline_targets.clone())
    }

    fn unload(&mut self) -> Result<(), ModuleCallError> {
        self.calls.lock().unwrap().unload += 1;
        assert!(!self.unload_panics, "configured unload panic");
        if let Some(error) = self.unload_error {
            return Err(ModuleCallError::new(error));
        }
        Ok(())
    }

    fn dlclose(&mut self) {
        self.calls.lock().unwrap().dlclose += 1;
    }
}

struct FakeLoader {
    module: Option<FakeModule>,
    open_error: Option<&'static str>,
}

struct PanickingLoader;

impl PipelineModuleLoader for PanickingLoader {
    fn open_staged(
        &mut self,
        _staged: &StagedModule,
    ) -> Result<Box<dyn LoadedPipelineModule>, ModuleCallError> {
        panic!("bad module crossed its boundary")
    }
}

impl PipelineModuleLoader for FakeLoader {
    fn open_staged(
        &mut self,
        staged: &StagedModule,
    ) -> Result<Box<dyn LoadedPipelineModule>, ModuleCallError> {
        assert!(staged.path.exists());
        if let Some(error) = self.open_error {
            return Err(ModuleCallError::new(error));
        }
        Ok(Box::new(self.module.take().unwrap()))
    }
}

fn module_abi(tag: u8) -> ModuleAbiIdentity {
    ModuleAbiIdentity {
        rustc: format!("rustc-{tag}"),
        interface_fingerprint: [tag; 32],
        measured_interface: [tag; 32],
        panic_strategy: "unwind".into(),
        allocator: "system".into(),
    }
}

fn source_identity(tag: u8) -> ngp_module_host::ModuleSourceIdentity {
    ngp_module_host::ModuleSourceIdentity {
        crate_name: "pipeline".to_owned(),
        source_hash: format!("{tag:016x}"),
    }
}

fn module_host(state_dir: impl AsRef<Path>) -> std::io::Result<ModuleHost> {
    ModuleHost::new(state_dir)
}

fn durable_module_host(
    state_dir: impl AsRef<Path>,
    requirements: &CandidateRequirements,
) -> DurableModuleHost {
    let state_dir = state_dir.as_ref();
    let host = module_host(state_dir.join("modules")).unwrap();
    let mut store = Store::open(StoreConfig::new(state_dir.join("store"))).unwrap();
    let types = requirements
        .schema_registry
        .iter()
        .filter(|(type_uuid, _)| !distill_core::bootstrap::is_bootstrap_control_type(**type_uuid))
        .map(|(type_uuid, logical_hash)| {
            (
                *type_uuid,
                AcceptedTypeLineage {
                    epochs: vec![AcceptedSchemaEpoch {
                        digest: *logical_hash,
                        forward_parent: None,
                    }],
                    current: 0,
                    authority: TypeAuthorityState::Active,
                },
            )
        })
        .collect();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(
                &VerifiedSchemaLineageManifest::from_verified_source(
                    ContentHash([0x91; 32]),
                    SchemaLineageManifest { types },
                ),
            )
        })
        .unwrap();
    DurableModuleHost::from_parts(host, store)
}

fn requirements(tag: u8) -> CandidateRequirements {
    let mut schema_registry = bootstrap_control_logical_registry_v1().unwrap();
    schema_registry.insert(TypeUuid([tag; 16]), LogicalHash([tag.wrapping_add(1); 32]));
    CandidateRequirements {
        module_abi: module_abi(tag),
        source_hashes: [("pipeline".to_owned(), format!("{tag:016x}"))]
            .into_iter()
            .collect(),
        schema_registry,
        targets: vec![TargetDefinition {
            name: "desktop".into(),
            fingerprint: [tag; 32],
        }],
    }
}

fn fake_module(tag: u8, calls: Arc<Mutex<Calls>>) -> FakeModule {
    FakeModule {
        source_identity: source_identity(tag),
        module_abi: module_abi(tag),
        registration: RegistrationSet {
            registrations: vec![Registration {
                kind: RegistrationKind::Processor,
                id: "cook".into(),
                version: 1,
            }],
            pipeline_targets: BTreeSet::from(["desktop".into()]),
        },
        calls,
        cleanup_behaviors: vec![CleanupBehavior::Ok],
        unload_error: None,
        unload_panics: false,
        poison_during_register: false,
        capsule_owner_mismatch: false,
        panic_after_registration: false,
        ignore_registration_errors: false,
        pin_sink: None,
    }
}

fn write_module(path: &Path, byte: u8) {
    std::fs::write(path, [byte; 32]).unwrap();
}

fn assert_poison_fields(
    poison: &distill_daemon::epoch::PipelinePoison,
    code: PipelinePoisonCode,
    origin: PipelinePoisonOrigin,
    cleanup: CandidateCleanupDisposition,
) {
    assert_eq!(poison.code, code);
    assert_eq!(poison.origin, origin);
    assert_eq!(poison.cleanup, cleanup);
    poison.validate().unwrap();
    assert_ne!(poison.identity, [0; 32]);
}

#[test]
fn candidate_open_failure_publishes_poison_and_next_good_candidate_heals() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 1);
    let mut host = module_host(temp.path().join("state")).unwrap();

    let mut failed = FakeLoader {
        module: None,
        open_error: Some("loader rejected image"),
    };
    let poison = host
        .publish_candidate(&source, requirements(1), &mut failed)
        .unwrap_err();
    assert_poison_fields(
        &poison,
        PipelinePoisonCode::CandidateOpen,
        PipelinePoisonOrigin::CandidateOpen,
        CandidateCleanupDisposition::None,
    );
    assert!(host.snapshot().epoch().is_err());

    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut good = FakeLoader {
        module: Some(fake_module(1, calls)),
        open_error: None,
    };
    let epoch = host
        .publish_candidate(&source, requirements(1), &mut good)
        .unwrap();
    assert_eq!(host.snapshot().epoch().unwrap().id(), epoch.id());
}

#[test]
fn candidate_target_set_is_normalized_sorted_and_retained() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 19);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut module = fake_module(19, calls.clone());
    module.registration.pipeline_targets = BTreeSet::from(["aa".to_owned()]);
    let mut loader = FakeLoader {
        module: Some(module),
        open_error: None,
    };
    let mut candidate = requirements(19);
    candidate.targets = vec![
        TargetDefinition {
            name: "z".to_owned(),
            fingerprint: [9; 32],
        },
        TargetDefinition {
            name: "e\u{301}".to_owned(),
            fingerprint: [8; 32],
        },
        TargetDefinition {
            name: "aa".to_owned(),
            fingerprint: [7; 32],
        },
    ];
    let expected = CanonicalTargetSet::canonical(vec![
        TargetSetRow {
            name: "z".to_owned(),
            target_definition_hash: [9; 32],
        },
        TargetSetRow {
            name: "e\u{301}".to_owned(),
            target_definition_hash: [8; 32],
        },
        TargetSetRow {
            name: "aa".to_owned(),
            target_definition_hash: [7; 32],
        },
    ])
    .unwrap();
    let mut host = module_host(temp.path().join("state")).unwrap();

    let epoch = host
        .publish_candidate(&source, candidate, &mut loader)
        .unwrap();

    assert_eq!(
        epoch
            .targets()
            .iter()
            .map(|target| (target.name.as_str(), target.fingerprint))
            .collect::<Vec<_>>(),
        expected
            .rows
            .iter()
            .map(|target| (target.name.as_str(), target.target_definition_hash))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        epoch
            .targets()
            .iter()
            .map(|target| target.name.as_str())
            .collect::<Vec<_>>(),
        ["aa", "z", "é"]
    );
    assert_eq!(calls.lock().unwrap().target_order, ["aa", "z", "é"]);
}

#[test]
fn nfc_equivalent_target_names_are_rejected_before_open_or_register() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 20);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut loader = FakeLoader {
        module: Some(fake_module(20, calls.clone())),
        open_error: None,
    };
    let mut candidate = requirements(20);
    candidate.targets = vec![
        TargetDefinition {
            name: "é".to_owned(),
            fingerprint: [1; 32],
        },
        TargetDefinition {
            name: "e\u{301}".to_owned(),
            fingerprint: [2; 32],
        },
    ];
    let mut host = module_host(temp.path().join("state")).unwrap();

    let poison = host
        .publish_candidate(&source, candidate, &mut loader)
        .unwrap_err();

    assert!(
        poison.message.contains("DuplicateTarget"),
        "{}",
        poison.message
    );
    assert_poison_fields(
        &poison,
        PipelinePoisonCode::CandidateValidation,
        PipelinePoisonOrigin::CandidateOpen,
        CandidateCleanupDisposition::None,
    );
    assert_eq!(calls.lock().unwrap().register, 0);
    assert!(loader.module.is_some(), "module open must not be attempted");
}

#[test]
fn boundary_panic_is_converted_to_candidate_poison_instead_of_unwinding() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 9);
    let mut host = module_host(temp.path().join("state")).unwrap();
    let poison = host
        .publish_candidate(&source, requirements(9), &mut PanickingLoader)
        .unwrap_err();
    assert_poison_fields(
        &poison,
        PipelinePoisonCode::CandidateOpen,
        PipelinePoisonOrigin::CandidateOpen,
        CandidateCleanupDisposition::None,
    );
    assert!(poison.message.contains("must return status"));
}

fn duplicate_registration_module(
    tag: u8,
    calls: Arc<Mutex<Calls>>,
    cleanup_behaviors: Vec<CleanupBehavior>,
) -> FakeModule {
    let mut module = fake_module(tag, calls);
    module.registration.registrations = vec![
        Registration {
            kind: RegistrationKind::Processor,
            id: "first".into(),
            version: 1,
        },
        Registration {
            kind: RegistrationKind::Importer,
            id: "middle".into(),
            version: 1,
        },
        Registration {
            kind: RegistrationKind::Processor,
            id: "first".into(),
            version: 2,
        },
    ];
    module.cleanup_behaviors = cleanup_behaviors;
    module
}

#[test]
fn partial_duplicate_registration_cleans_the_complete_arena_in_reverse_order() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 10);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut loader = FakeLoader {
        module: Some(duplicate_registration_module(
            10,
            calls.clone(),
            vec![CleanupBehavior::Ok; 3],
        )),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();

    let poison = host
        .publish_candidate(&source, requirements(10), &mut loader)
        .unwrap_err();

    assert_eq!(
        poison.cleanup,
        CandidateCleanupDisposition::CleanedAndClosed
    );
    assert_poison_fields(
        &poison,
        PipelinePoisonCode::CandidateRegistration,
        PipelinePoisonOrigin::CandidateOpen,
        CandidateCleanupDisposition::CleanedAndClosed,
    );
    let calls = calls.lock().unwrap();
    assert_eq!(calls.cleanup_order, ["first#2", "middle#1", "first#0"]);
    assert_eq!(calls.unload, 1);
    assert_eq!(calls.dlclose, 1);
}

#[test]
fn host_latch_rejects_candidate_when_module_ignores_duplicate_status() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 21);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut module = duplicate_registration_module(21, calls.clone(), vec![CleanupBehavior::Ok; 3]);
    module.ignore_registration_errors = true;
    let mut loader = FakeLoader {
        module: Some(module),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();

    let poison = host
        .publish_candidate(&source, requirements(21), &mut loader)
        .unwrap_err();

    assert!(poison
        .message
        .contains("host latched rejected registration"));
    assert_eq!(
        poison.cleanup,
        CandidateCleanupDisposition::CleanedAndClosed
    );
    assert_poison_fields(
        &poison,
        PipelinePoisonCode::CandidateRegistration,
        PipelinePoisonOrigin::CandidateOpen,
        CandidateCleanupDisposition::CleanedAndClosed,
    );
    let calls = calls.lock().unwrap();
    assert_eq!(calls.cleanup_order, ["first#2", "middle#1", "first#0"]);
    assert_eq!(calls.unload, 1);
    assert_eq!(calls.dlclose, 1);
}

#[test]
fn rejected_or_panicking_registration_keeps_every_capsule_arena_owned() {
    for panics_after_transfer in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("pipeline.dylib");
        write_module(&source, 18);
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut module = fake_module(18, calls.clone());
        module.capsule_owner_mismatch = !panics_after_transfer;
        module.panic_after_registration = panics_after_transfer;
        let mut loader = FakeLoader {
            module: Some(module),
            open_error: None,
        };
        let mut host = module_host(temp.path().join("state")).unwrap();

        let poison = host
            .publish_candidate(&source, requirements(18), &mut loader)
            .unwrap_err();

        if panics_after_transfer {
            assert!(
                poison.message.contains("register panicked"),
                "{}",
                poison.message
            );
        } else {
            assert!(
                poison.message.contains("different module epoch"),
                "{}",
                poison.message
            );
        }
        assert_eq!(
            poison.cleanup,
            CandidateCleanupDisposition::CleanedAndClosed
        );
        assert_poison_fields(
            &poison,
            PipelinePoisonCode::CandidateRegistration,
            PipelinePoisonOrigin::CandidateOpen,
            CandidateCleanupDisposition::CleanedAndClosed,
        );
        let calls = calls.lock().unwrap();
        assert_eq!(calls.cleanup_order, ["cook#0"]);
        assert_eq!(calls.unload, 1);
        assert_eq!(calls.dlclose, 1);
    }
}

#[test]
fn cleanup_error_or_panic_leaks_candidate_without_unload_or_dlclose() {
    for behavior in [CleanupBehavior::Error, CleanupBehavior::Panic] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("pipeline.dylib");
        write_module(&source, 11);
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut loader = FakeLoader {
            module: Some(duplicate_registration_module(
                11,
                calls.clone(),
                vec![CleanupBehavior::Ok, behavior, CleanupBehavior::Ok],
            )),
            open_error: None,
        };
        let mut host = module_host(temp.path().join("state")).unwrap();

        let poison = host
            .publish_candidate(&source, requirements(11), &mut loader)
            .unwrap_err();

        assert_eq!(
            poison.cleanup,
            CandidateCleanupDisposition::RegistrationCleanupFailed
        );
        assert_poison_fields(
            &poison,
            PipelinePoisonCode::CandidateCleanup,
            PipelinePoisonOrigin::CandidateOpen,
            CandidateCleanupDisposition::RegistrationCleanupFailed,
        );
        let calls = calls.lock().unwrap();
        assert_eq!(calls.cleanup_order, ["first#2", "middle#1", "first#0"]);
        assert_eq!(calls.unload, 0);
        assert_eq!(calls.dlclose, 0);
    }
}

#[test]
fn unload_error_or_panic_leaks_candidate_without_dlclose() {
    for unload_panics in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("pipeline.dylib");
        write_module(&source, 12);
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut module = fake_module(12, calls.clone());
        module.module_abi = module_abi(99);
        module.unload_panics = unload_panics;
        module.unload_error = (!unload_panics).then_some("unload refused");
        let mut loader = FakeLoader {
            module: Some(module),
            open_error: None,
        };
        let mut host = module_host(temp.path().join("state")).unwrap();

        let poison = host
            .publish_candidate(&source, requirements(12), &mut loader)
            .unwrap_err();

        assert_eq!(
            poison.cleanup,
            CandidateCleanupDisposition::ModuleUnloadFailed
        );
        assert_poison_fields(
            &poison,
            PipelinePoisonCode::CandidateCleanup,
            PipelinePoisonOrigin::CandidateOpen,
            CandidateCleanupDisposition::ModuleUnloadFailed,
        );
        let calls = calls.lock().unwrap();
        assert_eq!(calls.register, 0);
        assert_eq!(calls.unload, 1);
        assert_eq!(calls.dlclose, 0);
    }
}

#[test]
fn leaked_unpublished_token_pin_leaks_candidate_after_unload() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 13);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let pin_sink = Arc::new(Mutex::new(None));
    let mut module = duplicate_registration_module(13, calls.clone(), vec![CleanupBehavior::Ok; 3]);
    module.pin_sink = Some(pin_sink.clone());
    let mut loader = FakeLoader {
        module: Some(module),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();

    let poison = host
        .publish_candidate(&source, requirements(13), &mut loader)
        .unwrap_err();

    assert_eq!(poison.cleanup, CandidateCleanupDisposition::TokenPinned);
    assert_poison_fields(
        &poison,
        PipelinePoisonCode::CandidateCleanup,
        PipelinePoisonOrigin::CandidateOpen,
        CandidateCleanupDisposition::TokenPinned,
    );
    let calls = calls.lock().unwrap();
    assert_eq!(calls.unload, 1);
    assert_eq!(calls.dlclose, 0);
    drop(calls);
    let pin = pin_sink.lock().unwrap().take().unwrap();
    assert!(pin.is_fenced());
    drop(pin);
}

#[test]
fn token_poisoned_during_registration_never_publishes_or_dlcloses() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 14);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut module = fake_module(14, calls.clone());
    module.poison_during_register = true;
    let mut loader = FakeLoader {
        module: Some(module),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();

    let poison = host
        .publish_candidate(&source, requirements(14), &mut loader)
        .unwrap_err();

    assert_eq!(poison.cleanup, CandidateCleanupDisposition::TokenPoisoned);
    assert_poison_fields(
        &poison,
        PipelinePoisonCode::CandidateCleanup,
        PipelinePoisonOrigin::CandidateOpen,
        CandidateCleanupDisposition::TokenPoisoned,
    );
    assert!(host.snapshot().epoch().is_err());
    let calls = calls.lock().unwrap();
    assert_eq!(calls.cleanup_order, ["cook#0"]);
    assert_eq!(calls.unload, 1);
    assert_eq!(calls.dlclose, 0);
}

#[test]
fn runtime_poison_fences_every_pin_and_permanently_forbids_dlclose() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 2);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut loader = FakeLoader {
        module: Some(fake_module(2, calls.clone())),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();
    let epoch = host
        .publish_candidate(&source, requirements(2), &mut loader)
        .unwrap();
    let old_snapshot = host.snapshot();
    let job = epoch.try_start_job().unwrap();

    let module_value = ErasedValue::new_in(PanickingAssetDrop, epoch.module_token().clone());
    assert!(module_value.destroy().is_err());
    epoch.report_runtime_panic("module asset drop panicked");
    let runtime_poison = old_snapshot.epoch().unwrap_err();
    assert_poison_fields(
        &runtime_poison,
        PipelinePoisonCode::PublishedCallbackPanic,
        PipelinePoisonOrigin::PublishedRuntime,
        CandidateCleanupDisposition::PublishedEpochLeaked,
    );
    assert!(epoch.try_start_job().is_err());
    epoch.begin_drain();
    drop(job);
    assert!(!epoch.drain_complete());

    write_module(&source, 3);
    let next_calls = Arc::new(Mutex::new(Calls::default()));
    let mut next = FakeLoader {
        module: Some(fake_module(3, next_calls)),
        open_error: None,
    };
    host.publish_candidate(&source, requirements(3), &mut next)
        .unwrap();
    drop(old_snapshot);
    let outcomes = host.reap_retired();
    assert!(outcomes.contains(&UnloadOutcome::LeakedPoisoned(epoch.id())));
    let calls = calls.lock().unwrap();
    assert_eq!(calls.unload, 0);
    assert_eq!(calls.dlclose, 0);
}

#[test]
fn token_panic_cause_maps_to_panic_poison_without_a_second_report() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 31);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut loader = FakeLoader {
        module: Some(fake_module(31, calls)),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();
    let epoch = host
        .publish_candidate(&source, requirements(31), &mut loader)
        .unwrap();
    epoch
        .module_token()
        .poison_with(ModuleEpochPoisonCause::CallbackPanic);

    let poison = host.snapshot().epoch().unwrap_err();
    assert_eq!(poison.code, PipelinePoisonCode::PublishedCallbackPanic);
    assert!(matches!(
        epoch.try_start_job(),
        Err(EpochWorkError::Poisoned(ref poison))
            if poison.code == PipelinePoisonCode::PublishedCallbackPanic
    ));
}

#[test]
fn ordinary_retirement_is_stale_work_not_candidate_poison() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 32);
    let mut first_loader = FakeLoader {
        module: Some(fake_module(32, Arc::new(Mutex::new(Calls::default())))),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();
    let first = host
        .publish_candidate(&source, requirements(32), &mut first_loader)
        .unwrap();

    write_module(&source, 33);
    let mut second_loader = FakeLoader {
        module: Some(fake_module(33, Arc::new(Mutex::new(Calls::default())))),
        open_error: None,
    };
    host.publish_candidate(&source, requirements(33), &mut second_loader)
        .unwrap();
    assert!(matches!(
        first.try_start_job(),
        Err(EpochWorkError::Retired { epoch_id }) if epoch_id == first.id()
    ));
}

#[test]
fn runtime_rejection_and_retirement_cleanup_emit_exact_dspp_records() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 22);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut rejecting_loader = FakeLoader {
        module: Some(fake_module(22, calls)),
        open_error: None,
    };
    let mut rejecting_host = module_host(temp.path().join("rejecting-state")).unwrap();
    let rejecting_epoch = rejecting_host
        .publish_candidate(&source, requirements(22), &mut rejecting_loader)
        .unwrap();
    rejecting_epoch.report_runtime_failure("callback returned rejected status");
    let rejected = rejecting_host.snapshot().epoch().unwrap_err();
    assert_poison_fields(
        &rejected,
        PipelinePoisonCode::PublishedCallbackRejected,
        PipelinePoisonOrigin::PublishedRuntime,
        CandidateCleanupDisposition::PublishedEpochLeaked,
    );

    write_module(&source, 23);
    let cleanup_calls = Arc::new(Mutex::new(Calls::default()));
    let mut cleanup_module = fake_module(23, cleanup_calls);
    cleanup_module.unload_error = Some("published unload refused");
    let mut cleanup_loader = FakeLoader {
        module: Some(cleanup_module),
        open_error: None,
    };
    let mut cleanup_host = module_host(temp.path().join("cleanup-state")).unwrap();
    let retiring = cleanup_host
        .publish_candidate(&source, requirements(23), &mut cleanup_loader)
        .unwrap();
    let retiring_id = retiring.id();
    drop(retiring);

    write_module(&source, 24);
    let next_calls = Arc::new(Mutex::new(Calls::default()));
    let mut next_loader = FakeLoader {
        module: Some(fake_module(24, next_calls)),
        open_error: None,
    };
    cleanup_host
        .publish_candidate(&source, requirements(24), &mut next_loader)
        .unwrap();
    assert!(cleanup_host
        .reap_retired()
        .contains(&UnloadOutcome::LeakedPoisoned(retiring_id)));
    let cleanup = cleanup_host.retired_poison(retiring_id).unwrap();
    assert_poison_fields(
        &cleanup,
        PipelinePoisonCode::PublishedCleanup,
        PipelinePoisonOrigin::PublishedRuntime,
        CandidateCleanupDisposition::PublishedEpochLeaked,
    );
}

#[test]
fn clean_epoch_waits_for_snapshot_pin_then_unloads_and_closes() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 4);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut loader = FakeLoader {
        module: Some(fake_module(4, calls.clone())),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();
    let epoch = host
        .publish_candidate(&source, requirements(4), &mut loader)
        .unwrap();
    let pin = host.snapshot();

    write_module(&source, 5);
    let mut next = FakeLoader {
        module: Some(fake_module(5, Arc::new(Mutex::new(Calls::default())))),
        open_error: None,
    };
    host.publish_candidate(&source, requirements(5), &mut next)
        .unwrap();
    drop(epoch);
    assert!(host
        .reap_retired()
        .iter()
        .any(|outcome| matches!(outcome, UnloadOutcome::Pinned(_))));
    drop(pin);
    assert!(host
        .reap_retired()
        .iter()
        .any(|outcome| matches!(outcome, UnloadOutcome::Unloaded(_))));
    let calls = calls.lock().unwrap();
    assert_eq!(calls.register, 1);
    assert_eq!(calls.cleanup_order, ["cook#0"]);
    assert_eq!(calls.unload, 1);
    assert_eq!(calls.dlclose, 1);
}

fn publish_with_module(
    candidate: CandidateRequirements,
    module: FakeModule,
    calls: Arc<Mutex<Calls>>,
) -> distill_daemon::epoch::PipelinePoison {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 7);
    let mut loader = FakeLoader {
        module: Some(module),
        open_error: None,
    };
    let mut host = module_host(temp.path().join("state")).unwrap();
    let poison = host
        .publish_candidate(&source, candidate, &mut loader)
        .unwrap_err();
    assert_eq!(calls.lock().unwrap().register, 0);
    poison
}

#[test]
fn missing_or_wrong_module_source_identity_is_rejected_before_abi_and_register() {
    for missing in [false, true] {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut candidate = requirements(7);
        let mut module = fake_module(7, calls.clone());
        if missing {
            candidate.source_hashes.clear();
        } else {
            module.source_identity.source_hash = "ffffffffffffffff".to_owned();
        }
        let poison = publish_with_module(candidate, module, calls);
        assert_poison_fields(
            &poison,
            PipelinePoisonCode::CandidateAttestation,
            PipelinePoisonOrigin::CandidateOpen,
            CandidateCleanupDisposition::CleanedAndClosed,
        );
        assert!(
            poison.message.contains("absent from the watched schema")
                || poison.message.contains("source hash mismatch"),
            "{}",
            poison.message
        );
    }
}

#[test]
fn wrong_module_abi_is_rejected_before_register() {
    let calls = Arc::new(Mutex::new(Calls::default()));
    let candidate = requirements(7);
    let mut module = fake_module(7, calls.clone());
    module.module_abi = module_abi(8);
    let poison = publish_with_module(candidate, module, calls);
    assert_poison_fields(
        &poison,
        PipelinePoisonCode::CandidateAttestation,
        PipelinePoisonOrigin::CandidateOpen,
        CandidateCleanupDisposition::CleanedAndClosed,
    );
    assert!(poison
        .message
        .contains("host-interface ABI identity mismatch"));
}

#[test]
fn host_reverse_callback_boundary_contains_panics_on_all_audited_surfaces() {
    for surface in [
        HostCallbackSurface::Registry,
        HostCallbackSurface::EncodeSink,
        HostCallbackSurface::ProcessContext,
    ] {
        let boundary = HostCallbackBoundary::new(surface);
        let crossed = std::panic::catch_unwind(|| {
            boundary.call::<(), _>("test callback", || panic!("host callback panic"))
        });
        let error = crossed
            .expect("host panic must not unwind into the simulated module frame")
            .unwrap_err();
        assert!(error.detail().contains("host callback"));
        assert!(error.detail().contains(surface.as_str()));
    }
}

#[test]
fn host_reverse_callback_boundary_preserves_explicit_status_failures() {
    let boundary = HostCallbackBoundary::new(HostCallbackSurface::Registry);
    let error = boundary
        .call::<(), _>("insert processor", || {
            Err(ModuleCallError::new("duplicate processor"))
        })
        .unwrap_err();
    assert_eq!(error.detail(), "duplicate processor");
}

#[test]
fn durable_publication_commits_store_before_memory_becomes_current() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 40);
    let requirements = requirements(40);
    let mut host = durable_module_host(temp.path().join("state"), &requirements);
    let mut loader = FakeLoader {
        module: Some(fake_module(40, Arc::new(Mutex::new(Calls::default())))),
        open_error: None,
    };

    let epoch = host
        .publish_candidate(&source, requirements, &mut loader)
        .unwrap();
    assert_eq!(host.snapshot().epoch().unwrap().id(), epoch.id());
    assert!(matches!(
        host.store().pipeline_state().unwrap(),
        Some(StoredPipelineState::Ready(stored)) if stored.dylib_hash == epoch.dylib_hash()
    ));
}

#[test]
fn durable_store_failure_never_exposes_the_prepared_module() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 44);
    let host = module_host(temp.path().join("state/modules")).unwrap();
    let store = Store::open(StoreConfig::new(temp.path().join("state/store"))).unwrap();
    let mut host = DurableModuleHost::from_parts(host, store);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut loader = FakeLoader {
        module: Some(fake_module(44, calls.clone())),
        open_error: None,
    };

    assert!(matches!(
        host.publish_candidate(&source, requirements(44), &mut loader),
        Err(DurablePublishError::Store { .. })
    ));
    assert!(host.snapshot().epoch().is_err());
    assert!(host.store().pipeline_state().unwrap().is_none());
    assert_eq!(calls.lock().unwrap().unload, 1);
}

#[test]
fn candidate_failure_poison_is_committed_before_memory_publication() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 45);
    let requirements = requirements(45);
    let mut host = durable_module_host(temp.path().join("state"), &requirements);
    let mut loader = FakeLoader {
        module: None,
        open_error: Some("bad image"),
    };

    assert!(matches!(
        host.publish_candidate(&source, requirements, &mut loader),
        Err(DurablePublishError::Candidate(_))
    ));
    assert!(host.snapshot().epoch().is_err());
    assert!(matches!(
        host.store().pipeline_state().unwrap(),
        Some(StoredPipelineState::Poisoned { error, .. })
            if error.origin == PipelinePoisonOrigin::CandidateOpen
    ));
}

#[test]
fn schema_acceptance_state_retains_candidate_without_memory_swap() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 42);
    let initial = requirements(41);
    let candidate = requirements(42);
    let mut host = durable_module_host(temp.path().join("state"), &initial);
    let mut loader = FakeLoader {
        module: Some(fake_module(42, Arc::new(Mutex::new(Calls::default())))),
        open_error: None,
    };

    assert!(matches!(
        host.publish_candidate(&source, candidate, &mut loader),
        Err(DurablePublishError::SchemaAcceptanceRequired)
    ));
    assert!(host.snapshot().epoch().is_err());
    assert!(host.pending_candidate().is_some());
    assert!(matches!(
        host.store().pipeline_state().unwrap(),
        Some(StoredPipelineState::SchemaAcceptanceRequired { .. })
    ));
}

#[test]
fn runtime_callback_poison_is_fenced_and_durable_for_the_same_epoch() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 43);
    let requirements = requirements(43);
    let mut host = durable_module_host(temp.path().join("state"), &requirements);
    let mut loader = FakeLoader {
        module: Some(fake_module(43, Arc::new(Mutex::new(Calls::default())))),
        open_error: None,
    };
    let epoch = host
        .publish_candidate(&source, requirements, &mut loader)
        .unwrap();

    let poison = host
        .report_runtime_failure("published drop callback rejected")
        .unwrap();
    assert_eq!(poison.origin, PipelinePoisonOrigin::PublishedRuntime);
    assert!(epoch.try_start_job().is_err());
    assert!(host.snapshot().epoch().is_err());
    assert!(matches!(
        host.store().pipeline_state().unwrap(),
        Some(StoredPipelineState::Poisoned { error, .. })
            if error.origin == PipelinePoisonOrigin::PublishedRuntime
    ));
}
