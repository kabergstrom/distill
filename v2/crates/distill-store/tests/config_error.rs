//! DSCP v1 configuration-error grammar pinning.

use distill_store::state::{
    ConfigurationError, ConfigurationErrorCode, ConfigurationPathKey,
    ConfigurationSourceFailureCode, ConfigurationSourcePath,
    DirectoryAliasSide, DscpV1, OwnedPathKind, OwnedPathSide,
};
use ngp_schema::identity::LayoutIdentity;

fn identity(target: &str, marker: u8) -> LayoutIdentity {
    LayoutIdentity {
        target_triple: target.to_owned(),
        rustc: format!("rustc 1.{marker}.0"),
        algorithm_version: u32::from(marker),
    }
}

#[test]
fn dscp_v1_discriminants_and_one_complete_preimage_are_byte_pinned() {
    assert_eq!(ConfigurationErrorCode::MalformedConfiguration as u16, 1);
    assert_eq!(ConfigurationErrorCode::NonLoopbackAddress as u16, 2);
    assert_eq!(ConfigurationErrorCode::DuplicateRootName as u16, 3);
    assert_eq!(ConfigurationErrorCode::InvalidPath as u16, 4);
    assert_eq!(ConfigurationErrorCode::OwnedPathOverlap as u16, 5);
    assert_eq!(ConfigurationErrorCode::EmptyTargetApis as u16, 6);
    assert_eq!(ConfigurationErrorCode::InvalidParallelism as u16, 7);
    assert_eq!(ConfigurationErrorCode::InvalidBatchReservation as u16, 8);
    assert_eq!(ConfigurationErrorCode::DirectoryAlias as u16, 9);
    assert_eq!(ConfigurationErrorCode::UnsupportedTargetIdentity as u16, 12);
    assert_eq!(ConfigurationErrorCode::DuplicateTargetName as u16, 13);
    assert_eq!(
        ConfigurationErrorCode::ConfigurationSourceUnavailable as u16,
        14
    );
    assert_eq!(ConfigurationPathKey::AssetRoot as u8, 1);
    assert_eq!(ConfigurationPathKey::StatePath as u8, 2);
    assert_eq!(ConfigurationPathKey::SchemaArtifact as u8, 3);
    assert_eq!(ConfigurationPathKey::PipelineModule as u8, 4);
    assert_eq!(ConfigurationPathKey::CodegenOutput as u8, 5);
    assert_eq!(ConfigurationPathKey::Quarantine as u8, 6);
    assert_eq!(ConfigurationPathKey::ImportDestination as u8, 7);
    assert_eq!(OwnedPathKind::AssetRoot as u8, 1);
    assert_eq!(OwnedPathKind::DaemonState as u8, 2);
    assert_eq!(OwnedPathKind::SchemaArtifact as u8, 3);
    assert_eq!(OwnedPathKind::PipelineModule as u8, 4);
    assert_eq!(OwnedPathKind::CodegenOutput as u8, 5);
    assert_eq!(OwnedPathKind::Quarantine as u8, 6);
    assert_eq!(OwnedPathKind::ImportDestination as u8, 7);

    let reason = DscpV1::InvalidPath {
        key: ConfigurationPathKey::CodegenOutput,
        normalized_or_raw_path: "out/shaders".to_owned(),
    };
    let mut preimage = Vec::new();
    preimage.extend_from_slice(b"DSCP");
    preimage.push(1);
    preimage.extend_from_slice(&4u16.to_le_bytes());
    preimage.push(5);
    preimage.extend_from_slice(&11u32.to_le_bytes());
    preimage.extend_from_slice(b"out/shaders");
    assert_eq!(reason.reason_hash(), *blake3::hash(&preimage).as_bytes());
}

#[test]
fn every_dscp_v1_arm_maps_to_its_fixed_code() {
    let owned = OwnedPathSide {
        kind: OwnedPathKind::AssetRoot,
        path: "assets".to_owned(),
    };
    let alias = DirectoryAliasSide {
        normalized_path: "assets".to_owned(),
    };
    let compilation = identity("aarch64-apple-darwin", 1);
    let cases = [
        (
            DscpV1::MalformedConfiguration { file_hash: [0; 32] },
            ConfigurationErrorCode::MalformedConfiguration,
        ),
        (
            DscpV1::NonLoopbackAddress {
                address: "10.0.0.5:9999".to_owned(),
            },
            ConfigurationErrorCode::NonLoopbackAddress,
        ),
        (
            DscpV1::DuplicateRootName {
                normalized_name: "main".to_owned(),
            },
            ConfigurationErrorCode::DuplicateRootName,
        ),
        (
            DscpV1::InvalidPath {
                key: ConfigurationPathKey::AssetRoot,
                normalized_or_raw_path: "../assets".to_owned(),
            },
            ConfigurationErrorCode::InvalidPath,
        ),
        (
            DscpV1::OwnedPathOverlap {
                first: owned.clone(),
                second: owned,
            },
            ConfigurationErrorCode::OwnedPathOverlap,
        ),
        (
            DscpV1::EmptyTargetApis {
                target: "ship".to_owned(),
            },
            ConfigurationErrorCode::EmptyTargetApis,
        ),
        (
            DscpV1::InvalidParallelism { value: 0 },
            ConfigurationErrorCode::InvalidParallelism,
        ),
        (
            DscpV1::InvalidBatchReservation {
                parallelism: 4,
                reservation: 4,
            },
            ConfigurationErrorCode::InvalidBatchReservation,
        ),
        (
            DscpV1::DirectoryAlias {
                first: alias.clone(),
                second: alias,
            },
            ConfigurationErrorCode::DirectoryAlias,
        ),
        (
            DscpV1::UnsupportedTargetIdentity {
                target: "ship".to_owned(),
                expected: compilation.clone(),
                observed: compilation,
            },
            ConfigurationErrorCode::UnsupportedTargetIdentity,
        ),
        (
            DscpV1::DuplicateTargetName {
                normalized_name: "ship".to_owned(),
            },
            ConfigurationErrorCode::DuplicateTargetName,
        ),
        (
            DscpV1::ConfigurationSourceUnavailable {
                path: ConfigurationSourcePath::Unix(vec![b'c', 0xff]),
                failure: ConfigurationSourceFailureCode::Missing,
            },
            ConfigurationErrorCode::ConfigurationSourceUnavailable,
        ),
    ];
    for (reason, expected) in cases {
        assert_eq!(reason.code(), expected);
        let bytes = reason.canonical_detail_bytes();
        assert_eq!(
            DscpV1::from_canonical_detail_bytes(expected, &bytes).unwrap(),
            reason
        );
    }
}

#[test]
fn dscp_detail_decoder_rejects_noncanonical_text() {
    let mut decomposed = Vec::new();
    decomposed.extend_from_slice(&6_u32.to_le_bytes());
    decomposed.extend_from_slice(b"cafe\xcc\x81");
    assert_eq!(
        DscpV1::from_canonical_detail_bytes(
            ConfigurationErrorCode::DuplicateRootName,
            &decomposed,
        )
        .unwrap_err(),
        distill_store::state::DscpError::InvalidText
    );
}

#[test]
fn symmetric_pairs_have_one_canonical_order() {
    let asset = OwnedPathSide {
        kind: OwnedPathKind::AssetRoot,
        path: "assets".to_owned(),
    };
    let generated = OwnedPathSide {
        kind: OwnedPathKind::CodegenOutput,
        path: "assets/generated".to_owned(),
    };
    let forward = DscpV1::OwnedPathOverlap {
        first: asset.clone(),
        second: generated.clone(),
    };
    let reverse = DscpV1::OwnedPathOverlap {
        first: generated,
        second: asset,
    };
    assert_eq!(forward.reason_hash(), reverse.reason_hash());

    let physical = DirectoryAliasSide {
        normalized_path: "assets/a".to_owned(),
    };
    let alias = DirectoryAliasSide {
        normalized_path: "linked-assets/a".to_owned(),
    };
    assert_eq!(
        DscpV1::DirectoryAlias {
            first: physical.clone(),
            second: alias.clone(),
        }
        .reason_hash(),
        DscpV1::DirectoryAlias {
            first: alias,
            second: physical,
        }
        .reason_hash()
    );
}

#[test]
fn directory_alias_paths_round_trip_losslessly() {
    let detail = DscpV1::DirectoryAlias {
        first: DirectoryAliasSide {
            normalized_path: "a".into(),
        },
        second: DirectoryAliasSide {
            normalized_path: "b".into(),
        },
    };

    let bytes = detail.canonical_detail_bytes();
    assert_eq!(
        DscpV1::from_canonical_detail_bytes(ConfigurationErrorCode::DirectoryAlias, &bytes)
            .unwrap(),
        detail
    );
}

#[test]
fn unavailable_configuration_source_preserves_raw_paths_and_closed_failures() {
    assert_eq!(ConfigurationSourceFailureCode::Missing as u16, 1);
    assert_eq!(ConfigurationSourceFailureCode::PermissionDenied as u16, 2);
    assert_eq!(ConfigurationSourceFailureCode::InvalidFileType as u16, 3);
    assert_eq!(ConfigurationSourceFailureCode::IoDataLoss as u16, 4);

    let details = [
        DscpV1::ConfigurationSourceUnavailable {
            path: ConfigurationSourcePath::Unix(vec![b'c', 0xff]),
            failure: ConfigurationSourceFailureCode::PermissionDenied,
        },
        DscpV1::ConfigurationSourceUnavailable {
            path: ConfigurationSourcePath::Windows(vec![b'C' as u16, b':' as u16, 0xd800]),
            failure: ConfigurationSourceFailureCode::IoDataLoss,
        },
    ];
    for detail in details {
        let bytes = detail.canonical_detail_bytes();
        assert_eq!(
            DscpV1::from_canonical_detail_bytes(
                ConfigurationErrorCode::ConfigurationSourceUnavailable,
                &bytes,
            )
            .unwrap(),
            detail
        );
    }
}

#[test]
fn configuration_defects_select_one_authority_and_retain_the_canonical_doctor_set() {
    let later =
        ConfigurationError::from_reason(&DscpV1::InvalidParallelism { value: 0 }, "parallelism");
    let first = ConfigurationError::from_reason(
        &DscpV1::NonLoopbackAddress {
            address: "192.0.2.1:5000".into(),
        },
        "network",
    );
    let duplicate = ConfigurationError::from_reason(
        &DscpV1::NonLoopbackAddress {
            address: "192.0.2.1:5000".into(),
        },
        "a network diagnostic",
    );

    let set = ConfigurationError::canonical_set([later.clone(), first.clone(), duplicate.clone()])
        .unwrap();
    assert_eq!(set.len(), 2);
    assert_eq!(set[0], duplicate);
    assert_eq!(set[1], later);
    assert_eq!(
        ConfigurationError::select_canonical([later, first, duplicate]).unwrap(),
        Some(set[0].clone())
    );
}

#[test]
fn layout_identity_fields_are_part_of_the_typed_reason() {
    let expected = identity("aarch64-apple-darwin", 1);
    let observed = identity("x86_64-unknown-linux-gnu", 2);
    let a = DscpV1::UnsupportedTargetIdentity {
        target: "ship".to_owned(),
        expected: expected.clone(),
        observed: observed.clone(),
    };
    let b = DscpV1::UnsupportedTargetIdentity {
        target: "ship".to_owned(),
        expected,
        observed: LayoutIdentity {
            algorithm_version: 99,
            ..observed
        },
    };
    assert_ne!(a.reason_hash(), b.reason_hash());
}

#[test]
fn typed_configuration_error_carries_its_reason_and_message_is_not_hashed() {
    let facts = DscpV1::NonLoopbackAddress {
        address: "10.0.0.5:9999".to_owned(),
    };
    let expected_hash = facts.reason_hash();
    assert_eq!(
        ConfigurationError::from_reason(&facts, "first diagnostic").reason_hash,
        ConfigurationError::from_reason(&facts, "completely different prose").reason_hash,
    );
    let error = ConfigurationError::from_reason(&facts, "daemon address is not loopback");
    assert_eq!(error.code, ConfigurationErrorCode::NonLoopbackAddress);
    assert_eq!(error.reason_hash, expected_hash);
    assert_eq!(error.detail.as_ref(), &facts);
    assert_eq!(error.message, "daemon address is not loopback");
    assert_eq!(
        facts.reason_hash(),
        DscpV1::NonLoopbackAddress {
            address: "10.0.0.5:9999".to_owned(),
        }
        .reason_hash(),
        "presentation prose never enters DSCP"
    );
}

