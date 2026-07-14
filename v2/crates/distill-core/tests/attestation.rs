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
fn compiled_table_boundary_encoding_roundtrips_and_authenticates_every_row() {
    let extras = RegistryExtrasV1::canonical(vec![row(0, "x", RegistryExtraFact::Tag)]).unwrap();
    let first = compiled(1, extras.clone());
    let second = compiled(2, extras);
    let table = CompiledTypeTable::canonical(vec![second, first]).unwrap();
    let encoded = table.encode().unwrap();
    assert_eq!(CompiledTypeTable::decode(&encoded).unwrap(), table);

    let mut changed = encoded;
    let last = changed.len() - 1;
    changed[last] ^= 1;
    assert_eq!(
        CompiledTypeTable::decode(&changed),
        Err(AttestationError::CompiledDigestMismatch)
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

#[test]
fn embedded_bootstrap_spec_is_closed_canonical_and_byte_pinned() {
    let spec = BootstrapControlSpecV1::embedded().unwrap();
    assert_eq!(spec.type_uuids(), BOOTSTRAP_CONTROL_TYPE_UUIDS);
    assert_eq!(BOOTSTRAP_CONTROL_SPEC_V1_BYTES.len(), 6_665);
    assert_eq!(
        *blake3::hash(BOOTSTRAP_CONTROL_SPEC_V1_BYTES).as_bytes(),
        [
            218, 81, 157, 37, 74, 13, 180, 254, 227, 76, 81, 7, 128, 165, 194, 121, 236, 68,
            227, 76, 50, 219, 246, 41, 164, 225, 35, 213, 34, 229, 91, 230,
        ]
    );
    assert!(BOOTSTRAP_CONTROL_TYPE_UUIDS
        .iter()
        .all(|uuid| is_bootstrap_control_type(*uuid)));
}

#[test]
fn bootstrap_spec_requires_complete_blob_and_back_reference_facts() {
    let spec = BootstrapControlSpecV1::embedded().unwrap();
    for symbol in [
        BootstrapControlSymbol::Migration,
        BootstrapControlSymbol::DirectoryImportRules,
    ] {
        let row = spec.0.iter().find(|row| row.symbol == symbol).unwrap();
        assert!(row
            .registry_extras
            .rows
            .iter()
            .any(|extra| matches!(extra.fact, RegistryExtraFact::Blob)));
        assert!(row
            .registry_extras
            .rows
            .iter()
            .any(|extra| matches!(extra.fact, RegistryExtraFact::BackReference { .. })));
    }

    let mut without_blob = spec.clone();
    without_blob
        .0
        .iter_mut()
        .find(|row| row.symbol == BootstrapControlSymbol::Migration)
        .unwrap()
        .registry_extras
        .rows
        .retain(|extra| !matches!(extra.fact, RegistryExtraFact::Blob));
    assert_eq!(
        without_blob.encode(),
        Err(BootstrapSpecError::MissingRegistryFact("Blob"))
    );

    let mut without_backrefs = spec;
    without_backrefs
        .0
        .iter_mut()
        .find(|row| row.symbol == BootstrapControlSymbol::DirectoryImportRules)
        .unwrap()
        .registry_extras
        .rows
        .retain(|extra| !matches!(extra.fact, RegistryExtraFact::BackReference { .. }));
    assert_eq!(
        without_backrefs.encode(),
        Err(BootstrapSpecError::MissingRegistryFact("BackReference"))
    );
}

#[test]
fn bootstrap_spec_rejects_truncation_unknown_symbols_and_uuid_drift() {
    assert_eq!(
        BootstrapControlSpecV1::parse(&BOOTSTRAP_CONTROL_SPEC_V1_BYTES[..10]),
        Err(BootstrapSpecError::Truncated)
    );
    let mut unknown = BOOTSTRAP_CONTROL_SPEC_V1_BYTES.to_vec();
    unknown[5] = 99;
    assert_eq!(
        BootstrapControlSpecV1::parse(&unknown),
        Err(BootstrapSpecError::UnknownSymbol(99))
    );
    let mut wrong_uuid = BOOTSTRAP_CONTROL_SPEC_V1_BYTES.to_vec();
    wrong_uuid[6] ^= 1;
    assert!(matches!(
        BootstrapControlSpecV1::parse(&wrong_uuid),
        Err(BootstrapSpecError::SymbolUuidMismatch { .. })
    ));
}

#[test]
fn full_bootstrap_authority_compares_native_rows_after_dsb_facts() {
    let spec = BootstrapControlSpecV1::embedded().unwrap();
    let rows = spec
        .0
        .iter()
        .enumerate()
        .map(|(index, row)| {
            CompiledTypeRow::new(
                row.type_uuid,
                row.logical_hash,
                [index as u8; 32],
                true,
                row.registry_extras.clone(),
            )
            .unwrap()
        })
        .collect();
    let expected = BootstrapControlTableV1::canonical(rows).unwrap();
    validate_bootstrap_authority(expected.rows(), &expected, BundleFormatVersion::V1).unwrap();
    let mut changed = expected.rows().to_vec();
    changed[0].native_layout_digest[0] ^= 1;
    assert!(validate_bootstrap_authority(&changed, &expected, BundleFormatVersion::V1).is_err());

    let runtime = compiled(250, RegistryExtrasV1::default());
    let mut all_rows = expected.rows().to_vec();
    all_rows.push(runtime.clone());
    let all = CompiledTypeTable::canonical(all_rows).unwrap();
    let projected = all
        .project_with_bootstrap(&std::collections::BTreeSet::from([runtime.type_uuid]))
        .unwrap();
    assert_eq!(projected.rows.len(), BOOTSTRAP_CONTROL_COUNT + 1);
    assert!(projected
        .rows
        .iter()
        .all(|row| row.type_uuid == runtime.type_uuid || is_bootstrap_control_type(row.type_uuid)));
    assert_eq!(
        all.project_with_bootstrap(&std::collections::BTreeSet::from([TypeUuid([249; 16])]))
            .unwrap_err(),
        AttestationError::ProjectionMissingType(TypeUuid([249; 16]))
    );
}
