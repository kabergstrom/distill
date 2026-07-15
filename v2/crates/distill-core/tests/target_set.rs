use distill_core::target_set::{CanonicalTargetSet, TargetSetError, TargetSetRow};

fn row(name: &str, byte: u8) -> TargetSetRow {
    TargetSetRow {
        name: name.to_owned(),
        target_definition_hash: [byte; 32],
    }
}

#[test]
fn target_set_is_nfc_normalized_and_name_sorted() {
    let set = CanonicalTargetSet::canonical(vec![row("b", 2), row("e\u{301}", 1)]).unwrap();
    assert_eq!(set.rows[0].name, "b");
    assert_eq!(set.rows[1].name, "é");
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
