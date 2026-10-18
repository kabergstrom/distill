use distill_core::bootstrap::{
    bootstrap_control_logical_registry_v1, is_bootstrap_control_type, BootstrapControlSpecV1,
    BootstrapSpecError, BOOTSTRAP_CONTROL_SPEC_V1_BYTES, BOOTSTRAP_CONTROL_TYPE_UUIDS,
};
use distill_core::encoding::DecodeErrorKind;
use distill_core::header::HeaderError;

#[test]
fn checked_in_spec_is_exactly_three_sorted_logical_rows() {
    let spec = BootstrapControlSpecV1::embedded().unwrap();
    assert_eq!(spec.type_uuids(), BOOTSTRAP_CONTROL_TYPE_UUIDS);
    assert_eq!(spec.encode().unwrap(), BOOTSTRAP_CONTROL_SPEC_V1_BYTES);
    assert_eq!(bootstrap_control_logical_registry_v1().unwrap().len(), 3);
    assert!(BOOTSTRAP_CONTROL_TYPE_UUIDS
        .iter()
        .all(|uuid| is_bootstrap_control_type(*uuid)));
}

#[test]
fn parser_rejects_truncation_unknown_symbols_and_trailing_bytes() {
    assert!(matches!(
        BootstrapControlSpecV1::parse(&BOOTSTRAP_CONTROL_SPEC_V1_BYTES[..18]),
        Err(BootstrapSpecError::Decode(error)) if error.kind == DecodeErrorKind::Truncated
    ));

    // The header, count u32, then the first row's symbol u32.
    let mut unknown = BOOTSTRAP_CONTROL_SPEC_V1_BYTES.to_vec();
    unknown[16] = 0xff;
    assert_eq!(
        BootstrapControlSpecV1::parse(&unknown),
        Err(BootstrapSpecError::UnknownSymbol(0xff))
    );

    let mut newer = BOOTSTRAP_CONTROL_SPEC_V1_BYTES.to_vec();
    newer[8] = 3;
    assert!(matches!(
        BootstrapControlSpecV1::parse(&newer),
        Err(BootstrapSpecError::Header(HeaderError::UnsupportedVersion { found: 3, .. }))
    ));

    let mut trailing = BOOTSTRAP_CONTROL_SPEC_V1_BYTES.to_vec();
    trailing.push(0);
    assert!(matches!(
        BootstrapControlSpecV1::parse(&trailing),
        Err(BootstrapSpecError::Decode(error)) if error.kind == DecodeErrorKind::TrailingBytes
    ));
}
