use distill_pipeline_api::outputs::{OutputDecls, OutputError};
use distill_core::id::TypeUuid;

#[test]
fn declarations_normalize_keys_and_reject_duplicates() {
    assert!(matches!(
        OutputDecls::new(
            TypeUuid([1; 16]),
            vec![
                ("e\u{301}".into(), TypeUuid([2; 16])),
                ("\u{e9}".into(), TypeUuid([2; 16])),
            ],
        ),
        Err(OutputError::DuplicateKey(_))
    ));
}

#[test]
fn declarations_reject_invalid_keys_and_excess_cardinality() {
    assert!(matches!(
        OutputDecls::new(TypeUuid([1; 16]), vec![(String::new(), TypeUuid([2; 16]))]),
        Err(OutputError::InvalidKey(_))
    ));
    let extras = (0..256)
        .map(|index| (format!("output-{index}"), TypeUuid([2; 16])))
        .collect();
    assert_eq!(
        OutputDecls::new(TypeUuid([1; 16]), extras),
        Err(OutputError::TooManyOutputs)
    );
}
