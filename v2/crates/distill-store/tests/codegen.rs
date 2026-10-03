use std::collections::BTreeMap;

use distill_core::id::ContentHash;
use distill_store::{Store, StoreConfig, StoreError};

fn outputs(rows: &[(&str, u8)]) -> BTreeMap<String, ContentHash> {
    rows.iter()
        .map(|(path, byte)| ((*path).to_owned(), ContentHash([*byte; 32])))
        .collect()
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
    assert_eq!(store.input_version().unwrap().0, 0);
    assert_eq!(store.memo_seq().unwrap().0, 1);

    assert!(matches!(
        store.commit_codegen_outputs(&empty, &second),
        Err(StoreError::CodegenStateDrift)
    ));
    assert_eq!(store.codegen_outputs().unwrap(), first);
    assert_eq!(store.memo_seq().unwrap().0, 1);

    store.commit_codegen_outputs(&first, &second).unwrap();
    assert_eq!(store.codegen_outputs().unwrap(), second);
    assert_eq!(store.input_version().unwrap().0, 0);
    assert_eq!(store.memo_seq().unwrap().0, 2);
}
