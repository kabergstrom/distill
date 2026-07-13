use distill_core::canonical::DSTS;
use distill_core::target_set::{CanonicalTargetSet, TargetSetError, TargetSetRow};

fn row(name: &str, byte: u8) -> TargetSetRow {
    TargetSetRow {
        name: name.to_owned(),
        target_definition_hash: [byte; 32],
    }
}

#[test]
fn target_set_is_nfc_normalized_name_sorted_and_domain_separated() {
    let set = CanonicalTargetSet::canonical(vec![row("b", 2), row("e\u{301}", 1)]).unwrap();
    assert_eq!(set.rows[0].name, "b");
    assert_eq!(set.rows[1].name, "é");

    let mut expected = blake3::Hasher::new();
    expected.update(&DSTS);
    expected.update(&[1]);
    expected.update(&2_u32.to_le_bytes());
    expected.update(&1_u32.to_le_bytes());
    expected.update(b"b");
    expected.update(&[2; 32]);
    expected.update(&2_u32.to_le_bytes());
    expected.update("é".as_bytes());
    expected.update(&[1; 32]);
    assert_eq!(set.digest.0, *expected.finalize().as_bytes());
}

#[test]
fn canonically_equivalent_target_names_are_duplicates() {
    assert_eq!(
        CanonicalTargetSet::canonical(vec![row("é", 1), row("e\u{301}", 2)]),
        Err(TargetSetError::DuplicateTarget {
            normalized_name: "é".to_owned(),
        })
    );
}

#[test]
fn sorting_uses_normalized_name_bytes_not_length_framed_record_bytes() {
    let set = CanonicalTargetSet::canonical(vec![row("aa", 1), row("b", 2)]).unwrap();
    assert_eq!(
        set.rows
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        ["aa", "b"]
    );
}
