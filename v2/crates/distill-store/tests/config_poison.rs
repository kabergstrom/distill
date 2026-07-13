//! DSCP v1 configuration-poison grammar and persistence pinning.

use std::collections::BTreeSet;

use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid};
use distill_store::config::RestartOnlyChange;
use distill_store::state::{
    ConfigurationPathKey, ConfigurationPoison, ConfigurationPoisonCode,
    ConfigurationSourceFailureCode, ConfigurationSourcePath, ConfigurationState,
    DirectoryAliasSide, DscpV1, LineageManifestClaimant, OwnedPathKind, OwnedPathSide,
    PlatformFileIdentity,
};
use distill_store::{Store, StoreConfig, StoreError};
use ngp_schema::identity::CompilationIdentity;

fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, store)
}

fn identity(target: &str, marker: u8) -> CompilationIdentity {
    CompilationIdentity {
        target_triple: target.to_owned(),
        rustc: format!("rustc 1.{marker}.0"),
        source_fingerprint: [marker; 32],
        features: BTreeSet::from([
            ("z-package".to_owned(), "feat-b".to_owned()),
            ("a-package".to_owned(), "feat-a".to_owned()),
        ]),
        cfgs: BTreeSet::from(["target_pointer_width=\"64\"".to_owned(), "unix".to_owned()]),
        manifest_lock_hash: [marker.wrapping_add(1); 32],
        algorithm_version: u32::from(marker),
    }
}

fn lineage_claimant(marker: u8, asset: u8) -> LineageManifestClaimant {
    LineageManifestClaimant {
        root_name: "main".into(),
        normalized_path: format!("lineage-{marker}.bundle"),
        bundle: BundleUuid([marker; 16]),
        local_id: format!("manifest-{marker}"),
        asset: AssetUuid([asset; 16]),
        file_hash: BundleFileHash([marker; 32]),
    }
}

#[test]
fn dscp_v1_discriminants_and_one_complete_preimage_are_byte_pinned() {
    assert_eq!(ConfigurationPoisonCode::MalformedConfiguration as u16, 1);
    assert_eq!(ConfigurationPoisonCode::NonLoopbackAddress as u16, 2);
    assert_eq!(ConfigurationPoisonCode::DuplicateRootName as u16, 3);
    assert_eq!(ConfigurationPoisonCode::InvalidPath as u16, 4);
    assert_eq!(ConfigurationPoisonCode::OwnedPathOverlap as u16, 5);
    assert_eq!(ConfigurationPoisonCode::EmptyTargetApis as u16, 6);
    assert_eq!(ConfigurationPoisonCode::InvalidParallelism as u16, 7);
    assert_eq!(ConfigurationPoisonCode::InvalidBatchReservation as u16, 8);
    assert_eq!(ConfigurationPoisonCode::DirectoryAlias as u16, 9);
    assert_eq!(ConfigurationPoisonCode::MissingLineageManifest as u16, 10);
    assert_eq!(ConfigurationPoisonCode::DuplicateLineageManifest as u16, 11);
    assert_eq!(
        ConfigurationPoisonCode::UnsupportedTargetIdentity as u16,
        12
    );
    assert_eq!(ConfigurationPoisonCode::DuplicateTargetName as u16, 13);
    assert_eq!(
        ConfigurationPoisonCode::ConfigurationSourceUnavailable as u16,
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
        identity: PlatformFileIdentity::Unix {
            device: 1,
            inode: 2,
        },
    };
    let compilation = identity("aarch64-apple-darwin", 1);
    let cases = [
        (
            DscpV1::MalformedConfiguration { file_hash: [0; 32] },
            ConfigurationPoisonCode::MalformedConfiguration,
        ),
        (
            DscpV1::NonLoopbackAddress {
                address: "10.0.0.5:9999".to_owned(),
            },
            ConfigurationPoisonCode::NonLoopbackAddress,
        ),
        (
            DscpV1::DuplicateRootName {
                normalized_name: "main".to_owned(),
            },
            ConfigurationPoisonCode::DuplicateRootName,
        ),
        (
            DscpV1::InvalidPath {
                key: ConfigurationPathKey::AssetRoot,
                normalized_or_raw_path: "../assets".to_owned(),
            },
            ConfigurationPoisonCode::InvalidPath,
        ),
        (
            DscpV1::OwnedPathOverlap {
                first: owned.clone(),
                second: owned,
            },
            ConfigurationPoisonCode::OwnedPathOverlap,
        ),
        (
            DscpV1::EmptyTargetApis {
                target: "ship".to_owned(),
            },
            ConfigurationPoisonCode::EmptyTargetApis,
        ),
        (
            DscpV1::InvalidParallelism { value: 0 },
            ConfigurationPoisonCode::InvalidParallelism,
        ),
        (
            DscpV1::InvalidBatchReservation {
                parallelism: 4,
                reservation: 4,
            },
            ConfigurationPoisonCode::InvalidBatchReservation,
        ),
        (
            DscpV1::DirectoryAlias {
                first: alias.clone(),
                second: alias,
            },
            ConfigurationPoisonCode::DirectoryAlias,
        ),
        (
            DscpV1::MissingLineageManifest,
            ConfigurationPoisonCode::MissingLineageManifest,
        ),
        (
            DscpV1::DuplicateLineageManifest {
                entries: vec![lineage_claimant(1, 9), lineage_claimant(2, 9)],
            },
            ConfigurationPoisonCode::DuplicateLineageManifest,
        ),
        (
            DscpV1::UnsupportedTargetIdentity {
                target: "ship".to_owned(),
                expected: compilation.clone(),
                observed: compilation,
            },
            ConfigurationPoisonCode::UnsupportedTargetIdentity,
        ),
        (
            DscpV1::DuplicateTargetName {
                normalized_name: "ship".to_owned(),
            },
            ConfigurationPoisonCode::DuplicateTargetName,
        ),
        (
            DscpV1::ConfigurationSourceUnavailable {
                path: ConfigurationSourcePath::Unix(vec![b'c', 0xff]),
                failure: ConfigurationSourceFailureCode::Missing,
            },
            ConfigurationPoisonCode::ConfigurationSourceUnavailable,
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
fn dscp_detail_decoder_rejects_noncanonical_order_and_text() {
    let first = lineage_claimant(1, 9);
    let second = lineage_claimant(2, 9);
    let canonical = DscpV1::DuplicateLineageManifest {
        entries: vec![first.clone(), second.clone()],
    }
    .canonical_detail_bytes();
    let row_len = (canonical.len() - 4) / 2;
    let mut unsorted = Vec::with_capacity(canonical.len());
    unsorted.extend_from_slice(&2_u32.to_le_bytes());
    unsorted.extend_from_slice(&canonical[4 + row_len..]);
    unsorted.extend_from_slice(&canonical[4..4 + row_len]);
    assert!(matches!(
        DscpV1::from_canonical_detail_bytes(
            ConfigurationPoisonCode::DuplicateLineageManifest,
            &unsorted,
        ),
        Err(distill_store::state::DscpError::NonCanonical)
    ));

    let mut decomposed = Vec::new();
    decomposed.extend_from_slice(&6_u32.to_le_bytes());
    decomposed.extend_from_slice(b"cafe\xcc\x81");
    assert_eq!(
        DscpV1::from_canonical_detail_bytes(
            ConfigurationPoisonCode::DuplicateRootName,
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
        identity: PlatformFileIdentity::Unix {
            device: 7,
            inode: 9,
        },
    };
    let alias = DirectoryAliasSide {
        normalized_path: "linked-assets/a".to_owned(),
        identity: PlatformFileIdentity::Unix {
            device: 7,
            inode: 9,
        },
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
fn directory_alias_identity_is_closed_and_lossless_on_both_platforms() {
    let unix = DscpV1::DirectoryAlias {
        first: DirectoryAliasSide {
            normalized_path: "a".into(),
            identity: PlatformFileIdentity::Unix {
                device: 7,
                inode: 9,
            },
        },
        second: DirectoryAliasSide {
            normalized_path: "b".into(),
            identity: PlatformFileIdentity::Unix {
                device: 7,
                inode: 9,
            },
        },
    };
    let windows = DscpV1::DirectoryAlias {
        first: DirectoryAliasSide {
            normalized_path: "a".into(),
            identity: PlatformFileIdentity::Windows {
                volume_serial: 11,
                file_id: [0xaa; 16],
            },
        },
        second: DirectoryAliasSide {
            normalized_path: "b".into(),
            identity: PlatformFileIdentity::Windows {
                volume_serial: 11,
                file_id: [0xaa; 16],
            },
        },
    };

    for detail in [unix, windows] {
        let bytes = detail.canonical_detail_bytes();
        assert_eq!(
            DscpV1::from_canonical_detail_bytes(ConfigurationPoisonCode::DirectoryAlias, &bytes)
                .unwrap(),
            detail
        );
    }
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
                ConfigurationPoisonCode::ConfigurationSourceUnavailable,
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
        ConfigurationPoison::from_reason(&DscpV1::InvalidParallelism { value: 0 }, "parallelism");
    let first = ConfigurationPoison::from_reason(
        &DscpV1::NonLoopbackAddress {
            address: "192.0.2.1:5000".into(),
        },
        "network",
    );
    let duplicate = ConfigurationPoison::from_reason(
        &DscpV1::NonLoopbackAddress {
            address: "192.0.2.1:5000".into(),
        },
        "a network diagnostic",
    );

    let set = ConfigurationPoison::canonical_set([later.clone(), first.clone(), duplicate.clone()])
        .unwrap();
    assert_eq!(set.len(), 2);
    assert_eq!(set[0], duplicate);
    assert_eq!(set[1], later);
    assert_eq!(
        ConfigurationPoison::select_canonical([later, first, duplicate]).unwrap(),
        Some(set[0].clone())
    );
}

#[test]
fn duplicate_lineage_entries_sort_and_deduplicate_complete_claimant_rows() {
    let a = lineage_claimant(1, 9);
    let b = lineage_claimant(2, 9);
    let noisy = DscpV1::DuplicateLineageManifest {
        entries: vec![b.clone(), a.clone(), b, a.clone()],
    };
    let canonical = DscpV1::DuplicateLineageManifest {
        entries: vec![a.clone(), lineage_claimant(2, 9)],
    };
    assert_eq!(noisy.reason_hash(), canonical.reason_hash());

    let mut preimage = Vec::new();
    preimage.extend_from_slice(b"DSCP");
    preimage.push(1);
    preimage.extend_from_slice(&11u16.to_le_bytes());
    preimage.extend_from_slice(&2u32.to_le_bytes());
    preimage.extend_from_slice(&canonical.canonical_detail_bytes()[4..]);
    assert_eq!(canonical.reason_hash(), *blake3::hash(&preimage).as_bytes());
}

#[test]
fn compilation_identity_fields_are_part_of_the_typed_reason() {
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
        observed: CompilationIdentity {
            algorithm_version: 99,
            ..observed
        },
    };
    assert_ne!(a.reason_hash(), b.reason_hash());
}

#[test]
fn typed_configuration_poison_roundtrips_and_message_is_not_hashed() {
    let (_dir, mut store) = open();
    let facts = DscpV1::NonLoopbackAddress {
        address: "10.0.0.5:9999".to_owned(),
    };
    let expected_hash = facts.reason_hash();
    assert_eq!(
        ConfigurationPoison::from_reason(&facts, "first diagnostic").reason_hash,
        ConfigurationPoison::from_reason(&facts, "completely different prose").reason_hash,
    );
    store
        .input_transaction(|txn| {
            txn.publish_configuration_poison(&facts, "daemon address is not loopback")
        })
        .unwrap();

    let state = store.configuration_state().unwrap();
    let ConfigurationState::Poisoned { reason: poison, .. } = state else {
        panic!("typed poison must be persisted");
    };
    assert_eq!(poison.code, ConfigurationPoisonCode::NonLoopbackAddress);
    assert_eq!(poison.reason_hash, expected_hash);
    assert_eq!(poison.detail.as_ref(), &facts);
    assert_eq!(poison.message, "daemon address is not loopback");
    assert_eq!(
        facts.reason_hash(),
        DscpV1::NonLoopbackAddress {
            address: "10.0.0.5:9999".to_owned(),
        }
        .reason_hash(),
        "presentation prose never enters DSCP"
    );
}

#[test]
fn a_later_valid_generation_heals_all_persisted_poison_fields() {
    let (_dir, mut store) = open();
    store
        .input_transaction(|txn| {
            txn.publish_configuration_poison(
                &DscpV1::InvalidParallelism { value: 0 },
                "parallelism must be positive",
            )
        })
        .unwrap();
    store
        .stage_pending_restart(&[RestartOnlyChange::AutoCodegen(false)])
        .unwrap();
    store
        .input_transaction(|txn| txn.adopt_pending_restart())
        .unwrap();
    assert!(matches!(
        store.configuration_state().unwrap(),
        ConfigurationState::Ready(_)
    ));
}

#[test]
fn unknown_persisted_code_is_rejected_instead_of_becoming_an_other_variant() {
    let (dir, store) = open();
    let state_path = dir.path().join(".distill");
    drop(store);
    let conn = rusqlite::Connection::open(state_path.join("meta.sqlite")).unwrap();
    conn.pragma_update(None, "ignore_check_constraints", true)
        .unwrap();
    conn.execute(
        "INSERT INTO configuration_state(
             id, active_generation, input_version,
             poison_code, poison_detail_version, poison_detail,
             poison_reason_hash, poison_message
         ) VALUES (0, 0, 1, 65535, 1, X'', ?1, 'future')",
        [[7u8; 32].as_slice()],
    )
    .unwrap();
    drop(conn);

    let reopened = Store::open(StoreConfig::new(state_path)).unwrap();
    assert!(matches!(
        reopened.configuration_state(),
        Err(StoreError::InvalidConfiguration { .. })
    ));
}

#[test]
fn noncanonical_persisted_poison_shape_is_rejected() {
    let (dir, store) = open();
    let state_path = dir.path().join(".distill");
    drop(store);
    let conn = rusqlite::Connection::open(state_path.join("meta.sqlite")).unwrap();
    conn.pragma_update(None, "ignore_check_constraints", true)
        .unwrap();
    conn.execute(
        "INSERT INTO configuration_state(
             id, active_generation, input_version,
             poison_code, poison_detail_version, poison_detail,
             poison_reason_hash, poison_message
         ) VALUES (0, 0, 1, 2, 1, ?1, ?2, 'truncated hash')",
        rusqlite::params![
            DscpV1::NonLoopbackAddress {
                address: "127.0.0.1:1".to_owned(),
            }
            .canonical_detail_bytes(),
            [7u8; 31].as_slice(),
        ],
    )
    .unwrap();
    drop(conn);

    let reopened = Store::open(StoreConfig::new(state_path)).unwrap();
    assert!(matches!(
        reopened.configuration_state(),
        Err(StoreError::InvalidConfiguration { .. })
    ));
}

#[test]
fn persisted_configuration_poison_recomputes_detail_authority() {
    let cases = ["version", "trailing-detail", "wrong-code", "wrong-digest"];
    for case in cases {
        let (dir, mut store) = open();
        let state_path = dir.path().join(".distill");
        store
            .input_transaction(|txn| {
                txn.publish_configuration_poison(
                    &DscpV1::NonLoopbackAddress {
                        address: "10.0.0.5:9999".to_owned(),
                    },
                    "invalid address",
                )
            })
            .unwrap();
        drop(store);

        let conn = rusqlite::Connection::open(state_path.join("meta.sqlite")).unwrap();
        conn.pragma_update(None, "ignore_check_constraints", true)
            .unwrap();
        match case {
            "version" => {
                conn.execute(
                    "UPDATE configuration_state SET poison_detail_version = 2 WHERE id = 0",
                    [],
                )
                .unwrap();
            }
            "trailing-detail" => {
                let mut detail: Vec<u8> = conn
                    .query_row(
                        "SELECT poison_detail FROM configuration_state WHERE id = 0",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                detail.push(0);
                conn.execute(
                    "UPDATE configuration_state SET poison_detail = ?1 WHERE id = 0",
                    [detail],
                )
                .unwrap();
            }
            "wrong-code" => {
                conn.execute(
                    "UPDATE configuration_state SET poison_code = 7 WHERE id = 0",
                    [],
                )
                .unwrap();
            }
            "wrong-digest" => {
                conn.execute(
                    "UPDATE configuration_state SET poison_reason_hash = ?1 WHERE id = 0",
                    [[0xabu8; 32].as_slice()],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        drop(conn);

        let reopened = Store::open(StoreConfig::new(&state_path)).unwrap();
        assert!(matches!(
            reopened.configuration_state(),
            Err(StoreError::InvalidConfiguration { .. })
        ));
    }
}
