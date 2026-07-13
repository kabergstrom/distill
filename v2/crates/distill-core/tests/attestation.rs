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
