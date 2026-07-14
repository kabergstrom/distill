use distill_daemon::epoch::{MeasuredLayout, ModuleAbiIdentity, ModuleIdentity};
use distill_daemon::module_loader::{
    decode_measured_layouts, decode_module_identity, encode_measured_layouts,
    encode_module_identity, host_interface_closure_manifest, host_module_identity,
};
use distill_schema::bootstrap_gen_v1::consumer_compilation_identity_v1;

fn identity() -> ModuleIdentity {
    ModuleIdentity {
        compilation: consumer_compilation_identity_v1().clone(),
        module_abi: ModuleAbiIdentity {
            rustc: "rustc 1.92.0".to_owned(),
            interface_fingerprint: [1; 32],
            measured_interface: [2; 32],
            panic_strategy: "unwind".to_owned(),
            allocator: "system".to_owned(),
        },
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

    let host = host_module_identity(consumer_compilation_identity_v1().clone());
    assert_ne!(host.module_abi.interface_fingerprint, [0; 32]);
    assert_ne!(host.module_abi.measured_interface, [0; 32]);
}

#[test]
fn module_identity_boundary_encoding_is_canonical_and_closed() {
    let expected = identity();
    let encoded = encode_module_identity(&expected).unwrap();
    assert_eq!(decode_module_identity(&encoded).unwrap(), expected);

    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_module_identity(&trailing)
        .unwrap_err()
        .detail()
        .contains("trailing"));
}

#[test]
fn measured_layout_boundary_requires_strict_canonical_order() {
    let layouts = vec![
        MeasuredLayout {
            type_id: "asset::A".to_owned(),
            digest: [1; 32],
        },
        MeasuredLayout {
            type_id: "asset::B".to_owned(),
            digest: [2; 32],
        },
    ];
    let encoded = encode_measured_layouts(&layouts).unwrap();
    assert_eq!(decode_measured_layouts(&encoded).unwrap(), layouts);

    let duplicate = vec![layouts[0].clone(), layouts[0].clone()];
    assert!(encode_measured_layouts(&duplicate)
        .unwrap_err()
        .detail()
        .contains("strictly sorted"));
}
