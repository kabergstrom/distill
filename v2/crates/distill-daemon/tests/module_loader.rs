use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use distill_daemon::epoch::{
    CandidateRequirements, ModuleAbiIdentity, ModuleHost, TargetDefinition, UnloadOutcome,
};
use distill_daemon::module_loader::{
    decode_module_abi_identity, encode_module_abi_identity, host_interface_closure_manifest,
    host_module_abi_identity, host_rustc_identity, DynamicPipelineModuleLoader,
};

fn identity() -> ModuleAbiIdentity {
    ModuleAbiIdentity {
        rustc: "rustc 1.92.0".to_owned(),
        interface_fingerprint: [1; 32],
        measured_interface: [2; 32],
        panic_strategy: "unwind".to_owned(),
        allocator: "system".to_owned(),
    }
}

#[test]
fn module_abi_identity_covers_the_resolved_interface_closure() {
    let manifest = host_interface_closure_manifest();
    assert!(manifest
        .iter()
        .any(|(path, _)| *path == "distill-daemon/src/callbacks.rs"));
    assert!(manifest
        .iter()
        .any(|(path, _)| *path == "distill-asset/src/types.rs"));
    assert!(manifest
        .iter()
        .any(|(path, _)| *path == "ngp-schema/src/identity.rs"));
    assert!(manifest.iter().any(|(path, _)| *path == "v2/Cargo.lock"));

    let host = host_module_abi_identity();
    assert_ne!(host.interface_fingerprint, [0; 32]);
    assert_ne!(host.measured_interface, [0; 32]);
    assert_eq!(host.rustc, host_rustc_identity());
    assert!(!host_rustc_identity().is_empty());
}

#[test]
fn module_abi_identity_boundary_encoding_is_canonical_and_closed() {
    let expected = identity();
    let encoded = encode_module_abi_identity(&expected).unwrap();
    assert_eq!(decode_module_abi_identity(&encoded).unwrap(), expected);

    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_module_abi_identity(&trailing)
        .unwrap_err()
        .detail()
        .contains("trailing"));
}

#[test]
fn compiled_pipeline_cdylib_opens_registers_unloads_and_closes() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap();
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target/pipeline-module-fixture"));
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .current_dir(workspace)
        .env("CARGO_TARGET_DIR", &target_dir)
        .args(["build", "--offline", "-p", "distill-pipeline-fixture"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture build failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let module_path = fixture_library_path(&target_dir);
    assert!(
        module_path.is_file(),
        "fixture module is missing at {}",
        module_path.display()
    );

    let temp = tempfile::tempdir().unwrap();
    let preflight = temp.path().join(format!(
        "source-identity.{}",
        module_path.extension().unwrap().to_string_lossy()
    ));
    let staged = ngp_module_host::stage_copy_to(&module_path, &preflight).unwrap();
    // SAFETY: the fixture is compiled from the current checked source and is
    // opened only from the exact authenticated staged copy.
    let image = unsafe { ngp_module_host::HostedLibrary::open(staged) }.unwrap();
    // SAFETY: the fixture derives the shared bounded source-identity exports.
    let source_identity = unsafe { ngp_module_host::read_source_identity(&image) }.unwrap();
    image.close();

    let requirements = CandidateRequirements {
        module_abi: host_module_abi_identity(),
        source_hashes: BTreeMap::from([(source_identity.crate_name, source_identity.source_hash)]),
        schema_registry: BTreeMap::new(),
        targets: vec![TargetDefinition {
            name: "desktop".to_owned(),
            fingerprint: [0x42; 32],
        }],
    };
    let mut host = ModuleHost::new(temp.path().join("module-host")).unwrap();
    let mut loader = DynamicPipelineModuleLoader;

    let first = host
        .publish_candidate(&module_path, requirements.clone(), &mut loader)
        .unwrap();
    assert_eq!(
        first.registrations().pipeline_targets,
        ["desktop".to_owned()].into_iter().collect()
    );
    assert!(first.registrations().registrations.is_empty());

    let second = host
        .publish_candidate(&module_path, requirements.clone(), &mut loader)
        .unwrap();
    drop(first);
    assert_eq!(host.reap_retired(), vec![UnloadOutcome::Unloaded(1)]);

    let missing = temp.path().join("missing-module.dylib");
    assert!(host
        .publish_candidate(&missing, requirements, &mut loader)
        .is_err());
    drop(second);
    assert_eq!(host.reap_retired(), vec![UnloadOutcome::Unloaded(2)]);
}

fn fixture_library_path(target_dir: &Path) -> PathBuf {
    let filename = if cfg!(target_os = "windows") {
        "distill_pipeline_fixture.dll"
    } else if cfg!(target_os = "macos") {
        "libdistill_pipeline_fixture.dylib"
    } else {
        "libdistill_pipeline_fixture.so"
    };
    target_dir.join("debug").join(filename)
}
