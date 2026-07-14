use std::collections::BTreeMap;

use distill_core::id::ContentHash;
use distill_store::codegen::CodegenPublicationBasis;
use distill_store::state::InputVersion;
use distill_store::{Store, StoreConfig, StoreError};

fn outputs(rows: &[(&str, u8)]) -> BTreeMap<String, ContentHash> {
    rows.iter()
        .map(|(path, byte)| ((*path).to_owned(), ContentHash([*byte; 32])))
        .collect()
}

#[test]
fn publication_basis_is_canonical_and_recovery_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(temp.path().join("state"))).unwrap();
    let previous = BTreeMap::new();
    let proposed = outputs(&[("mod.rs", 1), ("sp_01010101010101010101010101010101.rs", 2)]);
    let basis =
        CodegenPublicationBasis::new(store.input_version(), previous.clone(), proposed.clone());
    let bytes = basis.encode();

    assert_eq!(CodegenPublicationBasis::decode(&bytes).unwrap(), basis);
    store.apply_codegen_publication_basis(&basis).unwrap();
    let memo = store.memo_seq();
    store.apply_codegen_publication_basis(&basis).unwrap();
    assert_eq!(store.memo_seq(), memo);
    assert_eq!(store.codegen_outputs().unwrap(), proposed);

    let mut noncanonical = bytes;
    noncanonical.push(0);
    assert!(CodegenPublicationBasis::decode(&noncanonical).is_err());
    assert!(CodegenPublicationBasis::decode(b"not a basis").is_err());
}

#[test]
fn publication_basis_rejects_a_different_input_version_or_preimage_map() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(temp.path().join("state"))).unwrap();
    let empty = BTreeMap::new();
    let first = outputs(&[("mod.rs", 1)]);
    store.commit_codegen_outputs(&empty, &first).unwrap();

    let wrong_map = CodegenPublicationBasis::new(
        store.input_version(),
        empty.clone(),
        outputs(&[("mod.rs", 2)]),
    );
    assert!(matches!(
        store.apply_codegen_publication_basis(&wrong_map),
        Err(StoreError::CodegenStateDrift)
    ));

    let wrong_version =
        CodegenPublicationBasis::new(InputVersion(store.input_version().0 + 1), first, empty);
    assert!(matches!(
        store.apply_codegen_publication_basis(&wrong_version),
        Err(StoreError::CodegenStateDrift)
    ));
}

#[test]
fn codegen_output_preimages_are_memo_side_and_compare_and_set() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(temp.path().join("state"))).unwrap();
    let empty = BTreeMap::new();
    let first = outputs(&[("mod.rs", 1), ("sp_01010101010101010101010101010101.rs", 2)]);
    let second = outputs(&[("mod.rs", 3)]);

    assert_eq!(store.codegen_outputs().unwrap(), empty);
    store.commit_codegen_outputs(&empty, &first).unwrap();
    assert_eq!(store.codegen_outputs().unwrap(), first);
    assert_eq!(store.input_version().0, 0);
    assert_eq!(store.memo_seq().0, 1);

    assert!(matches!(
        store.commit_codegen_outputs(&empty, &second),
        Err(StoreError::CodegenStateDrift)
    ));
    assert_eq!(store.codegen_outputs().unwrap(), first);
    assert_eq!(store.memo_seq().0, 1);

    store.commit_codegen_outputs(&first, &second).unwrap();
    assert_eq!(store.codegen_outputs().unwrap(), second);
    assert_eq!(store.input_version().0, 0);
    assert_eq!(store.memo_seq().0, 2);
}
