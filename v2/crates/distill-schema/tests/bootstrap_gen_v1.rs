use distill_core::attestation::BOOTSTRAP_CONTROL_SPEC_V1_BYTES;
use distill_schema::bootstrap_builtins_v1::generated_bootstrap_control_spec_v1;
use distill_schema::bootstrap_gen_v1::{
    check_bootstrap_generation_v1, consumer_bootstrap_authority_v1,
    consumer_compilation_identity_v1, decode_generator_input_v1,
    generate_bootstrap_table_artifact_v1, local_generator_input_bytes_v1,
    EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_NAME_V1,
};

#[test]
fn pack_definition_roots_carry_the_authoring_only_selector() {
    let needle = b"authoring_only";
    let generated = generated_bootstrap_control_spec_v1()
        .unwrap()
        .encode()
        .unwrap();
    assert!(generated.windows(needle.len()).any(|row| row == needle));
    assert!(BOOTSTRAP_CONTROL_SPEC_V1_BYTES
        .windows(needle.len())
        .any(|row| row == needle));
}

#[test]
fn real_generator_is_byte_deterministic_and_exactly_dsci_keyed() {
    let first_input = local_generator_input_bytes_v1().unwrap();
    let second_input = local_generator_input_bytes_v1().unwrap();
    assert_eq!(first_input, second_input);

    let decoded = decode_generator_input_v1(&first_input).unwrap();
    assert_eq!(
        decoded.compilation_identity(),
        consumer_compilation_identity_v1()
    );
    assert_eq!(decoded.measured_layouts().len(), 5);

    let first = generate_bootstrap_table_artifact_v1(&first_input).unwrap();
    let second = generate_bootstrap_table_artifact_v1(&second_input).unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first.resource_name(),
        EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_NAME_V1
    );
    assert_eq!(first.bytes().len(), 2_287);
    assert_eq!(
        *blake3::hash(first.bytes()).as_bytes(),
        [
            100, 140, 176, 94, 141, 222, 208, 226, 198, 202, 15, 146, 47, 1, 89, 249, 58, 160, 197,
            114, 250, 48, 135, 141, 179, 167, 123, 152, 124, 214, 180, 250,
        ]
    );
}

#[test]
fn sealed_authority_verifies_literal_against_local_concrete_measurements() {
    check_bootstrap_generation_v1().unwrap();
    let authority = consumer_bootstrap_authority_v1().unwrap();
    assert_eq!(authority.rows().len(), 5);
    assert_eq!(
        authority.resource_name(),
        EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_NAME_V1
    );
    assert_eq!(
        authority.compilation_identity(),
        consumer_compilation_identity_v1()
    );
    assert_eq!(
        authority.table().compiled_table().unwrap().digest,
        authority.dsca()
    );
}
