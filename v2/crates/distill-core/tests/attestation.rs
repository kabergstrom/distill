use distill_core::attestation::*;
use distill_core::id::{LogicalHash, TypeUuid};

fn row(node: u32, field: &str, fact: RegistryExtraFact) -> RegistryExtraRow {
    RegistryExtraRow {
        node: SchemaNodeId(node),
        path: vec![RegistryPathStep::Field(field.to_owned())],
        fact,
    }
}

fn compiled(tag: u8, extras: RegistryExtrasV1) -> CompiledTypeRow {
    CompiledTypeRow::new(
        TypeUuid([tag; 16]),
        LogicalHash([tag.wrapping_add(1); 32]),
        [tag.wrapping_add(2); 32],
        tag.is_multiple_of(2),
        extras,
    )
    .unwrap()
}

#[test]
fn registry_extras_normalize_sort_roundtrip_and_hash_typed_facts() {
    let extras = RegistryExtrasV1::canonical(vec![
        row(1, "tag", RegistryExtraFact::Tag),
        row(
            0,
            "re\u{301}f",
            RegistryExtraFact::Reference {
                strength: ReferenceStrength::Strong,
                target: TypeUuid([9; 16]),
            },
        ),
        row(0, "blob", RegistryExtraFact::Blob),
    ])
    .unwrap();
    let encoded = extras.encode().unwrap();
    assert_eq!(RegistryExtrasV1::decode(&encoded).unwrap(), extras);

    let mut weak = extras.clone();
    let RegistryExtraFact::Reference { strength, .. } = &mut weak.rows[1].fact else {
        panic!("expected reference row")
    };
    *strength = ReferenceStrength::Weak;
    weak.rows.sort();
    assert_ne!(extras.digest().unwrap(), weak.digest().unwrap());
}

#[test]
fn decoder_rejects_unknown_noncanonical_duplicate_and_forged_extras() {
    assert_eq!(
        RegistryExtrasV1::decode(&[2, 0, 0, 0, 0]).unwrap_err(),
        AttestationError::UnsupportedVersion(2)
    );

    let one = RegistryExtrasV1::canonical(vec![row(0, "x", RegistryExtraFact::Tag)]).unwrap();
    let mut unknown = one.encode().unwrap();
    *unknown.last_mut().unwrap() = 99;
    assert_eq!(
        RegistryExtrasV1::decode(&unknown).unwrap_err(),
        AttestationError::UnknownFact(99)
    );

    let duplicate = RegistryExtrasV1 {
        rows: vec![one.rows[0].clone(), one.rows[0].clone()],
    };
    assert_eq!(
        duplicate.validate().unwrap_err(),
        AttestationError::DuplicateExtraRow
    );

    let mut forged = compiled(1, one);
    forged.registry_extras_digest.0[0] ^= 1;
    assert_eq!(
        forged.validate().unwrap_err(),
        AttestationError::ExtrasDigestMismatch
    );
}

#[test]
fn registry_extras_order_by_the_pinned_encoded_path_not_rust_string_order() {
    // Canonical path strings are length-framed. Therefore Field("b") sorts
    // before Field("aa") in the pinned bytes (length 1 before length 2),
    // even though Rust's String::cmp produces the opposite order.
    let short = row(0, "b", RegistryExtraFact::Tag);
    let long = row(0, "aa", RegistryExtraFact::Tag);
    let extras = RegistryExtrasV1::canonical(vec![long.clone(), short.clone()]).unwrap();
    assert_eq!(extras.rows, vec![short.clone(), long.clone()]);

    assert_eq!(
        RegistryExtrasV1 {
            rows: vec![long, short],
        }
        .validate()
        .unwrap_err(),
        AttestationError::ExtrasNotStrictlySorted
    );
}

#[test]
fn dsca_rejects_duplicate_unsorted_and_forged_rows() {
    let extras = RegistryExtrasV1::canonical(vec![row(0, "x", RegistryExtraFact::Tag)]).unwrap();
    let first = compiled(1, extras.clone());
    let second = compiled(2, extras);
    let table = CompiledTypeTable::canonical(vec![second.clone(), first.clone()]).unwrap();
    assert_eq!(table.rows, vec![first.clone(), second.clone()]);
    table.validate().unwrap();

    assert!(matches!(
        CompiledTypeTable::from_canonical(vec![second, first.clone()], table.digest,),
        Err(AttestationError::TypeRowsNotStrictlySorted)
    ));
    assert!(matches!(
        CompiledTypeTable::canonical(vec![first.clone(), first]),
        Err(AttestationError::DuplicateType(_))
    ));
    let mut forged = table;
    forged.digest.0[0] ^= 1;
    assert_eq!(
        forged.validate().unwrap_err(),
        AttestationError::CompiledDigestMismatch
    );
}

#[test]
fn equal_dsnl_does_not_hide_logical_policy_or_extras_drift() {
    let base_extras = RegistryExtrasV1::canonical(vec![row(
        0,
        "ref",
        RegistryExtraFact::Reference {
            strength: ReferenceStrength::Strong,
            target: TypeUuid([7; 16]),
        },
    )])
    .unwrap();
    let base = compiled(3, base_extras.clone());
    let base_digest = CompiledTypeTable::canonical(vec![base.clone()])
        .unwrap()
        .digest;
    for changed in [
        CompiledTypeRow::new(
            base.type_uuid,
            LogicalHash([99; 32]),
            base.native_layout_digest,
            base.build_only,
            base_extras.clone(),
        )
        .unwrap(),
        CompiledTypeRow::new(
            base.type_uuid,
            base.logical_hash,
            base.native_layout_digest,
            !base.build_only,
            base_extras.clone(),
        )
        .unwrap(),
        CompiledTypeRow::new(
            base.type_uuid,
            base.logical_hash,
            base.native_layout_digest,
            base.build_only,
            RegistryExtrasV1::canonical(vec![row(
                0,
                "ref",
                RegistryExtraFact::Reference {
                    strength: ReferenceStrength::Weak,
                    target: TypeUuid([8; 16]),
                },
            )])
            .unwrap(),
        )
        .unwrap(),
    ] {
        assert_eq!(changed.native_layout_digest, base.native_layout_digest);
        assert_ne!(
            CompiledTypeTable::canonical(vec![changed]).unwrap().digest,
            base_digest
        );
    }
}

#[test]
fn dsta_projects_all_types_and_pins_the_canonical_bytes() {
    let first = compiled(
        1,
        RegistryExtrasV1::canonical(vec![
            row(256, "z", RegistryExtraFact::Tag),
            row(1, "y", RegistryExtraFact::Tag),
            row(2, "re\u{301}f", RegistryExtraFact::Blob),
            row(0, "aa", RegistryExtraFact::Tag),
            row(0, "b", RegistryExtraFact::Tag),
        ])
        .unwrap(),
    );
    let second = compiled(
        2,
        RegistryExtrasV1::canonical(vec![row(0, "not-a-tag", RegistryExtraFact::Skip)]).unwrap(),
    );
    let table = CompiledTypeTable::canonical(vec![second, first]).unwrap();

    let bytes = encode_tag_annotation_projection(&table.rows).unwrap();
    let mut expected = vec![1, 2, 0, 0, 0];
    expected.extend_from_slice(&[1; 16]);
    expected.extend_from_slice(&4_u32.to_le_bytes());
    for (node, field) in [(0_u32, "b"), (0, "aa"), (1, "y"), (256, "z")] {
        expected.extend_from_slice(&node.to_le_bytes());
        expected.extend_from_slice(&1_u32.to_le_bytes());
        expected.push(1); // RegistryPathStep::Field
        expected.extend_from_slice(&(field.len() as u32).to_le_bytes());
        expected.extend_from_slice(field.as_bytes());
        expected.push(1); // TagAnnotationFact::SearchTag
    }
    expected.extend_from_slice(&[2; 16]);
    expected.extend_from_slice(&0_u32.to_le_bytes());
    assert_eq!(bytes, expected);

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DSTA");
    hasher.update(&expected);
    assert_eq!(
        compute_tag_annotation_epoch(&table.rows).unwrap(),
        TagAnnotationEpoch(*hasher.finalize().as_bytes())
    );
}

#[test]
fn dsta_recomputes_for_comparison_and_rejects_noncanonical_inputs() {
    let extras =
        RegistryExtrasV1::canonical(vec![row(0, "label", RegistryExtraFact::Tag)]).unwrap();
    let first = compiled(1, extras.clone());
    let second = compiled(2, extras);
    let table = CompiledTypeTable::canonical(vec![first.clone(), second.clone()]).unwrap();
    let epoch = compute_tag_annotation_epoch(&table.rows).unwrap();
    verify_tag_annotation_epoch(&table.rows, epoch).unwrap();

    let mut forged = epoch;
    forged.0[0] ^= 1;
    assert_eq!(
        verify_tag_annotation_epoch(&table.rows, forged).unwrap_err(),
        AttestationError::TagAnnotationEpochMismatch {
            expected: epoch,
            observed: forged,
        }
    );
    assert_eq!(
        compute_tag_annotation_epoch(&[second, first.clone()]).unwrap_err(),
        AttestationError::TypeRowsNotStrictlySorted
    );
    assert_eq!(
        compute_tag_annotation_epoch(&[first.clone(), first]).unwrap_err(),
        AttestationError::DuplicateType(TypeUuid([1; 16]))
    );

    let mut duplicate_tag = table.rows[0].clone();
    duplicate_tag
        .registry_extras
        .rows
        .push(duplicate_tag.registry_extras.rows[0].clone());
    assert_eq!(
        compute_tag_annotation_epoch(&[duplicate_tag]).unwrap_err(),
        AttestationError::DuplicateExtraRow
    );
}

#[test]
fn dsta_changes_only_with_the_exact_tag_projection() {
    let base = compiled(
        1,
        RegistryExtrasV1::canonical(vec![
            row(0, "label", RegistryExtraFact::Tag),
            row(0, "cache", RegistryExtraFact::Skip),
        ])
        .unwrap(),
    );
    let epoch = compute_tag_annotation_epoch(std::slice::from_ref(&base)).unwrap();

    let unrelated_fact_changed = compiled(
        1,
        RegistryExtrasV1::canonical(vec![
            row(0, "label", RegistryExtraFact::Tag),
            row(0, "cache", RegistryExtraFact::Blob),
        ])
        .unwrap(),
    );
    assert_eq!(
        compute_tag_annotation_epoch(&[unrelated_fact_changed]).unwrap(),
        epoch
    );

    let tag_path_changed = compiled(
        1,
        RegistryExtrasV1::canonical(vec![
            row(1, "label", RegistryExtraFact::Tag),
            row(0, "cache", RegistryExtraFact::Skip),
        ])
        .unwrap(),
    );
    assert_ne!(
        compute_tag_annotation_epoch(&[tag_path_changed]).unwrap(),
        epoch
    );
}
