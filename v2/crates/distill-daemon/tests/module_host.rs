use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use distill_asset::ErasedValue;
use distill_core::id::{LogicalHash, TypeUuid};
use distill_daemon::epoch::{
    CandidateRequirements, CompiledAttestationDigest, CompiledTypeAttestation, CompiledTypeTable,
    HostCallbackBoundary, HostCallbackSurface, LoadedPipelineModule, MeasuredLayout,
    ModuleAbiIdentity, ModuleCallError, ModuleHost, ModuleIdentity, PipelineModuleLoader,
    PoisonEntryPoint, Registration, RegistrationKind, RegistrationSet, StagedModule,
    TargetDefinition, UnloadOutcome,
};

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
}

struct FakeModule {
    identity: ModuleIdentity,
    layouts: Vec<MeasuredLayout>,
    compiled_types: CompiledTypeTable,
    registration: RegistrationSet,
    calls: Arc<Mutex<Calls>>,
}

impl LoadedPipelineModule for FakeModule {
    fn identity(&mut self) -> Result<ModuleIdentity, ModuleCallError> {
        Ok(self.identity.clone())
    }

    fn measured_layouts(&mut self) -> Result<Vec<MeasuredLayout>, ModuleCallError> {
        Ok(self.layouts.clone())
    }

    fn compiled_types(&mut self) -> Result<CompiledTypeTable, ModuleCallError> {
        Ok(self.compiled_types.clone())
    }

    fn register(
        &mut self,
        _targets: &[TargetDefinition],
    ) -> Result<RegistrationSet, ModuleCallError> {
        self.calls.lock().unwrap().register += 1;
        Ok(self.registration.clone())
    }

    fn unload(&mut self) -> Result<(), ModuleCallError> {
        self.calls.lock().unwrap().unload += 1;
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

fn identity(tag: u8) -> ModuleIdentity {
    ModuleIdentity {
        compilation: vec![tag],
        module_abi: ModuleAbiIdentity {
            rustc: format!("rustc-{tag}"),
            interface_fingerprint: [tag; 32],
            measured_interface: [tag; 32],
            panic_strategy: "unwind".into(),
            allocator: "system".into(),
        },
    }
}

fn requirements(tag: u8) -> CandidateRequirements {
    CandidateRequirements {
        identity: identity(tag),
        measured_layouts: vec![MeasuredLayout {
            type_id: "asset".into(),
            digest: [tag; 32],
        }],
        compiled_types: CompiledTypeTable::canonical(vec![compiled_type(tag)]).unwrap(),
        targets: vec![TargetDefinition {
            name: "desktop".into(),
            fingerprint: [tag; 32],
        }],
        native_dependencies: vec![],
    }
}

fn fake_module(tag: u8, calls: Arc<Mutex<Calls>>) -> FakeModule {
    FakeModule {
        identity: identity(tag),
        layouts: requirements(tag).measured_layouts,
        compiled_types: requirements(tag).compiled_types,
        registration: RegistrationSet {
            registrations: vec![Registration {
                kind: RegistrationKind::Processor,
                id: "cook".into(),
                version: 1,
            }],
            pipeline_targets: BTreeSet::from(["desktop".into()]),
        },
        calls,
    }
}

fn compiled_type(tag: u8) -> CompiledTypeAttestation {
    CompiledTypeAttestation {
        type_uuid: TypeUuid([tag; 16]),
        logical_hash: LogicalHash([tag.wrapping_add(1); 32]),
        native_layout_digest: [tag; 32],
        build_only: false,
        registry_extras: vec![tag.wrapping_add(2)],
    }
}

fn write_module(path: &Path, byte: u8) {
    std::fs::write(path, [byte; 32]).unwrap();
}

#[test]
fn candidate_open_failure_publishes_poison_and_next_good_candidate_heals() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 1);
    let mut host = ModuleHost::new(temp.path().join("state")).unwrap();

    let mut failed = FakeLoader {
        module: None,
        open_error: Some("loader rejected image"),
    };
    let poison = host
        .publish_candidate(&source, requirements(1), &mut failed)
        .unwrap_err();
    assert_eq!(poison.entry_point, PoisonEntryPoint::CandidateOpen);
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
fn boundary_panic_is_converted_to_candidate_poison_instead_of_unwinding() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 9);
    let mut host = ModuleHost::new(temp.path().join("state")).unwrap();
    let poison = host
        .publish_candidate(&source, requirements(9), &mut PanickingLoader)
        .unwrap_err();
    assert_eq!(poison.entry_point, PoisonEntryPoint::CandidateOpen);
    assert!(poison.detail.contains("must return status"));
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
    let mut host = ModuleHost::new(temp.path().join("state")).unwrap();
    let epoch = host
        .publish_candidate(&source, requirements(2), &mut loader)
        .unwrap();
    let old_snapshot = host.snapshot();
    let job = epoch.try_start_job().unwrap();

    let module_value = ErasedValue::new_in(PanickingAssetDrop, epoch.module_token().clone());
    assert!(module_value.destroy().is_err());
    assert_eq!(
        old_snapshot.epoch().unwrap_err().entry_point,
        PoisonEntryPoint::PublishedRuntime
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
fn clean_epoch_waits_for_snapshot_pin_then_unloads_and_closes() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 4);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut loader = FakeLoader {
        module: Some(fake_module(4, calls.clone())),
        open_error: None,
    };
    let mut host = ModuleHost::new(temp.path().join("state")).unwrap();
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
    assert_eq!(calls.unload, 1);
    assert_eq!(calls.dlclose, 1);
}

fn publish_with_compiled_types(
    expected: CompiledTypeTable,
    actual: CompiledTypeTable,
) -> (distill_daemon::epoch::PipelinePoison, Arc<Mutex<Calls>>) {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("pipeline.dylib");
    write_module(&source, 7);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let mut module = fake_module(7, calls.clone());
    module.compiled_types = actual;
    let mut loader = FakeLoader {
        module: Some(module),
        open_error: None,
    };
    let mut candidate = requirements(7);
    candidate.compiled_types = expected;
    let mut host = ModuleHost::new(temp.path().join("state")).unwrap();
    let poison = host
        .publish_candidate(&source, candidate, &mut loader)
        .unwrap_err();
    (poison, calls)
}

#[test]
fn compiled_attestation_rejects_semantic_drift_before_register_when_dsnl_is_equal() {
    let expected = requirements(7).compiled_types;
    for (field, mutate) in [
        (
            "logical hash",
            (|row: &mut CompiledTypeAttestation| row.logical_hash = LogicalHash([91; 32]))
                as fn(&mut CompiledTypeAttestation),
        ),
        (
            "build_only",
            (|row: &mut CompiledTypeAttestation| row.build_only = true)
                as fn(&mut CompiledTypeAttestation),
        ),
        (
            "registry extras",
            (|row: &mut CompiledTypeAttestation| row.registry_extras = vec![92])
                as fn(&mut CompiledTypeAttestation),
        ),
    ] {
        let mut rows = expected.rows.clone();
        mutate(&mut rows[0]);
        let actual = CompiledTypeTable::canonical(rows).unwrap();
        assert_eq!(actual.rows[0].native_layout_digest, [7; 32]);
        let (poison, calls) = publish_with_compiled_types(expected.clone(), actual);
        assert!(poison.detail.contains(field), "{}", poison.detail);
        assert_eq!(calls.lock().unwrap().register, 0);
    }
}

#[test]
fn compiled_attestation_rejects_unsorted_and_duplicate_module_rows_before_register() {
    let expected = CompiledTypeTable::canonical(vec![compiled_type(1), compiled_type(2)]).unwrap();

    let mut unsorted = expected.clone();
    unsorted.rows.swap(0, 1);
    let (poison, calls) = publish_with_compiled_types(expected.clone(), unsorted);
    assert!(
        poison.detail.contains("strictly sorted"),
        "{}",
        poison.detail
    );
    assert_eq!(calls.lock().unwrap().register, 0);

    let mut duplicate = expected.clone();
    duplicate.rows[1] = duplicate.rows[0].clone();
    let (poison, calls) = publish_with_compiled_types(expected, duplicate);
    assert!(poison.detail.contains("duplicate"), "{}", poison.detail);
    assert_eq!(calls.lock().unwrap().register, 0);
}

#[test]
fn candidate_expectations_must_be_canonical_and_module_dsca_must_recompute() {
    let canonical = CompiledTypeTable::canonical(vec![compiled_type(1), compiled_type(2)]).unwrap();
    let mut unsorted_expected = canonical.clone();
    unsorted_expected.rows.swap(0, 1);
    let (poison, calls) = publish_with_compiled_types(unsorted_expected, canonical.clone());
    assert!(
        poison
            .detail
            .contains("expected compiled-type table is not strictly sorted"),
        "{}",
        poison.detail
    );
    assert_eq!(calls.lock().unwrap().register, 0);

    let mut bad_digest = canonical.clone();
    bad_digest.digest = CompiledAttestationDigest([255; 32]);
    let (poison, calls) = publish_with_compiled_types(canonical, bad_digest);
    assert!(
        poison.detail.contains("DSCA digest mismatch"),
        "{}",
        poison.detail
    );
    assert_eq!(calls.lock().unwrap().register, 0);
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
