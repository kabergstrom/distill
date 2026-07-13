use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use distill_asset::ErasedValue;
use distill_daemon::epoch::{
    CandidateRequirements, LoadedPipelineModule, MeasuredLayout, ModuleAbiIdentity,
    ModuleCallError, ModuleHost, ModuleIdentity, PipelineModuleLoader, PoisonEntryPoint,
    Registration, RegistrationKind, RegistrationSet, StagedModule, TargetDefinition, UnloadOutcome,
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
    unload: usize,
    dlclose: usize,
}

struct FakeModule {
    identity: ModuleIdentity,
    layouts: Vec<MeasuredLayout>,
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

    fn register(
        &mut self,
        _targets: &[TargetDefinition],
    ) -> Result<RegistrationSet, ModuleCallError> {
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
    assert_eq!(calls.unload, 1);
    assert_eq!(calls.dlclose, 1);
}
