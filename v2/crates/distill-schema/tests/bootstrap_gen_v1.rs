use distill_schema::bootstrap_gen_v1::{
    check_bootstrap_generation_v1, consumer_bootstrap_authority_v1,
    consumer_compilation_identity_v1, decode_generator_input_v1,
    generate_bootstrap_table_artifact_v1, local_generator_input_bytes_v1,
    EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_NAME_V1,
};

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
            252, 80, 243, 181, 148, 226, 78, 81, 63, 85, 147, 25, 128, 151, 54, 129, 4, 212,
            99, 160, 106, 67, 84, 99, 190, 44, 219, 13, 50, 241, 21, 206,
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
