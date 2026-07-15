use distill_daemon::epoch::ModuleAbiIdentity;
use distill_daemon::module_loader::{
    decode_module_abi_identity, encode_module_abi_identity, host_interface_closure_manifest,
    host_module_abi_identity, host_rustc_identity,
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
