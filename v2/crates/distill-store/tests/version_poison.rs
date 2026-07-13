use distill_core::canonical::{CanonicalEncoder, DSVP};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid};
use distill_store::state::{
    AssetClaimant, PhysicalPathClaim, PhysicalPathFailureCode, PlatformPathBytes,
    ReadableBundleSource, ScanFailureCode, ScanSubject, VersionPoison, VersionPoisonCode,
    VersionPoisonError, VersionPoisonV1,
};

fn source(root: &str, path: &str, byte: u8) -> ReadableBundleSource {
    ReadableBundleSource {
        root_name: root.into(),
        normalized_path: path.into(),
        file_hash: BundleFileHash([byte; 32]),
    }
}

#[test]
fn version_poison_identity_pins_dsvp_v1_and_excludes_message() {
    let claimants = vec![
        AssetClaimant::Authored {
            source: source("main", "a.bundle", 1),
            bundle: BundleUuid([4; 16]),
            local_id: "entry".into(),
        },
        AssetClaimant::Derived {
            parent: AssetUuid([5; 16]),
            output_key: "extra".into(),
        },
    ];
    let detail = VersionPoisonV1::DuplicateAssetUuid {
        asset: AssetUuid([3; 16]),
        claimants: claimants.clone(),
    };
    let first = VersionPoison::new(detail.clone(), "first wording").unwrap();
    let second = VersionPoison::new(detail, "different wording").unwrap();
    assert_eq!(first.code, VersionPoisonCode::DuplicateAssetUuid);
    assert_eq!(first.identity, second.identity);

    let mut expected = CanonicalEncoder::new();
    expected.raw(&DSVP);
    expected.u8(1);
    expected.u16(VersionPoisonCode::DuplicateAssetUuid as u16);
    expected.raw(&[3; 16]);
    expected.u32(2);
    expected.u8(1); // AssetClaimant::Authored
    expected.str("main");
    expected.str("a.bundle");
    expected.raw(&[1; 32]);
    expected.raw(&[4; 16]);
    expected.str("entry");
    expected.u8(2); // AssetClaimant::Derived
    expected.raw(&[5; 16]);
    expected.str("extra");
    assert_eq!(
        first.identity,
        *blake3::hash(&expected.into_bytes()).as_bytes()
    );
    let encoded = first.persisted_bytes().unwrap();
    assert_eq!(
        VersionPoison::from_persisted_bytes(&encoded).unwrap(),
        first
    );
    assert_eq!(
        VersionPoison::from_persisted_bytes(&[encoded, vec![0]].concat()).unwrap_err(),
        VersionPoisonError::TrailingBytes
    );
}

#[test]
fn collision_sources_are_strictly_sorted_distinct_and_have_two_rows() {
    let one = AssetClaimant::Derived {
        parent: AssetUuid([5; 16]),
        output_key: "extra".into(),
    };
    let detail = |claimants| VersionPoisonV1::DuplicateAssetUuid {
        asset: AssetUuid([3; 16]),
        claimants,
    };
    assert_eq!(
        VersionPoison::new(detail(vec![one.clone()]), "bad").unwrap_err(),
        VersionPoisonError::InsufficientSources
    );
    assert_eq!(
        VersionPoison::new(detail(vec![one.clone(), one]), "bad").unwrap_err(),
        VersionPoisonError::NonCanonicalSources
    );
}

#[test]
fn same_root_path_collision_retains_lossless_distinct_physical_names() {
    let poison = VersionPoison::new(
        VersionPoisonV1::SameRootNormalizedPathCollision {
            root_name: "main".into(),
            normalized_path: "café.bundle".into(),
            claims: vec![
                PhysicalPathClaim {
                    raw_relative_path: PlatformPathBytes::Unix(b"cafe\xcc\x81.bundle".to_vec()),
                    file_hash: BundleFileHash([1; 32]),
                },
                PhysicalPathClaim {
                    raw_relative_path: PlatformPathBytes::Unix(b"caf\xc3\xa9.bundle".to_vec()),
                    file_hash: BundleFileHash([2; 32]),
                },
            ],
        },
        "NFC alias",
    )
    .unwrap();
    assert_eq!(
        poison.code,
        VersionPoisonCode::SameRootNormalizedPathCollision
    );
    assert_eq!(
        VersionPoison::from_persisted_bytes(&poison.persisted_bytes().unwrap()).unwrap(),
        poison
    );
}

#[test]
fn invalid_physical_path_retains_raw_bytes_and_fixed_failure_code() {
    assert_eq!(VersionPoisonCode::InvalidPhysicalPath as u16, 6);
    assert_eq!(PhysicalPathFailureCode::InvalidUnixUtf8 as u16, 1);
    assert_eq!(PhysicalPathFailureCode::UnpairedWindowsUtf16 as u16, 2);
    assert_eq!(PhysicalPathFailureCode::Absolute as u16, 3);
    assert_eq!(PhysicalPathFailureCode::EmptyComponent as u16, 4);
    assert_eq!(PhysicalPathFailureCode::DotComponent as u16, 5);
    assert_eq!(PhysicalPathFailureCode::ParentComponent as u16, 6);
    assert_eq!(PhysicalPathFailureCode::ForbiddenCharacter as u16, 7);

    let poison = VersionPoison::new(
        VersionPoisonV1::InvalidPhysicalPath {
            root_name: "main".into(),
            raw_relative_path: PlatformPathBytes::Unix(vec![b'b', 0xff]),
            failure: PhysicalPathFailureCode::InvalidUnixUtf8,
        },
        "invalid physical name",
    )
    .unwrap();
    assert_eq!(poison.code, VersionPoisonCode::InvalidPhysicalPath);

    let encoded = poison.persisted_bytes().unwrap();
    assert_eq!(
        VersionPoison::from_persisted_bytes(&encoded).unwrap(),
        poison
    );

    let mut expected_identity = CanonicalEncoder::new();
    expected_identity.raw(&DSVP);
    expected_identity.u8(1);
    expected_identity.u16(6);
    expected_identity.str("main");
    expected_identity.u8(1); // Unix
    expected_identity.u32(2);
    expected_identity.raw(&[b'b', 0xff]);
    expected_identity.u16(1); // InvalidUnixUtf8
    assert_eq!(
        poison.identity,
        *blake3::hash(&expected_identity.into_bytes()).as_bytes()
    );
}

#[test]
fn canonical_winner_is_independent_of_discovery_order() {
    let invalid_path = VersionPoison::new(
        VersionPoisonV1::InvalidPhysicalPath {
            root_name: "main".into(),
            raw_relative_path: PlatformPathBytes::Windows(vec![0xd800]),
            failure: PhysicalPathFailureCode::UnpairedWindowsUtf16,
        },
        "invalid path",
    )
    .unwrap();
    let incomplete = VersionPoison::new(
        VersionPoisonV1::IncompleteSkeleton {
            source: source("main", "broken.bundle", 7),
            failure: distill_store::state::SkeletonFailureCode::EnvelopeMalformed,
        },
        "incomplete",
    )
    .unwrap();

    let forward = VersionPoison::select_canonical(vec![invalid_path.clone(), incomplete.clone()])
        .unwrap()
        .unwrap();
    let reverse = VersionPoison::select_canonical(vec![incomplete.clone(), invalid_path])
        .unwrap()
        .unwrap();
    assert_eq!(forward, incomplete);
    assert_eq!(reverse, incomplete);

    let lexicographically_later = VersionPoison::new(
        VersionPoisonV1::IncompleteSkeleton {
            source: source("main", "z.bundle", 1),
            failure: distill_store::state::SkeletonFailureCode::EnvelopeMalformed,
        },
        "z",
    )
    .unwrap();
    let lexicographically_first = VersionPoison::new(
        VersionPoisonV1::IncompleteSkeleton {
            source: source("main", "a.bundle", 9),
            failure: distill_store::state::SkeletonFailureCode::EnvelopeMalformed,
        },
        "a",
    )
    .unwrap();
    assert_eq!(
        VersionPoison::select_canonical(vec![
            lexicographically_later,
            lexicographically_first.clone(),
        ])
        .unwrap(),
        Some(lexicographically_first)
    );

    let duplicate_a = VersionPoison::new(
        VersionPoisonV1::IncompleteSkeleton {
            source: source("main", "same.bundle", 3),
            failure: distill_store::state::SkeletonFailureCode::EnvelopeMalformed,
        },
        "z diagnostic",
    )
    .unwrap();
    let duplicate_b = VersionPoison::new(duplicate_a.detail.clone(), "a diagnostic").unwrap();
    let set = VersionPoison::canonical_set([duplicate_a, duplicate_b]).unwrap();
    assert_eq!(set.len(), 1);
    assert_eq!(set[0].message, "a diagnostic");
}

#[test]
fn invalid_physical_path_rejects_unknown_failure_code() {
    let mut encoded = CanonicalEncoder::new();
    encoded.raw(&DSVP);
    encoded.u8(1);
    encoded.u16(VersionPoisonCode::InvalidPhysicalPath as u16);
    encoded.str("main");
    encoded.u8(1); // Unix
    encoded.u32(1);
    encoded.raw(b"x");
    encoded.u16(8); // not a PhysicalPathFailureCode
    encoded.str("diagnostic");
    assert_eq!(
        VersionPoison::from_persisted_bytes(&encoded.into_bytes()).unwrap_err(),
        VersionPoisonError::UnknownFailureCode(8)
    );
}

#[test]
fn invalid_physical_path_recomputes_lowest_applicable_platform_failure() {
    let cases = [
        (
            PlatformPathBytes::Unix(vec![b'/', 0xff]),
            PhysicalPathFailureCode::InvalidUnixUtf8,
        ),
        (
            PlatformPathBytes::Unix(b"/absolute".to_vec()),
            PhysicalPathFailureCode::Absolute,
        ),
        (
            PlatformPathBytes::Unix(b"a//b".to_vec()),
            PhysicalPathFailureCode::EmptyComponent,
        ),
        (
            PlatformPathBytes::Unix(b"a/./b".to_vec()),
            PhysicalPathFailureCode::DotComponent,
        ),
        (
            PlatformPathBytes::Unix(b"a/../b".to_vec()),
            PhysicalPathFailureCode::ParentComponent,
        ),
        (
            PlatformPathBytes::Unix(b"a\\b".to_vec()),
            PhysicalPathFailureCode::ForbiddenCharacter,
        ),
        (
            PlatformPathBytes::Windows(vec![0xd800, b'\\' as u16]),
            PhysicalPathFailureCode::UnpairedWindowsUtf16,
        ),
        (
            PlatformPathBytes::Windows("C:relative".encode_utf16().collect()),
            PhysicalPathFailureCode::Absolute,
        ),
        (
            PlatformPathBytes::Windows("a\\\\b".encode_utf16().collect()),
            PhysicalPathFailureCode::EmptyComponent,
        ),
        (
            PlatformPathBytes::Windows("a/.\\b".encode_utf16().collect()),
            PhysicalPathFailureCode::DotComponent,
        ),
        (
            PlatformPathBytes::Windows("a/../b".encode_utf16().collect()),
            PhysicalPathFailureCode::ParentComponent,
        ),
        (
            PlatformPathBytes::Windows("ab:c".encode_utf16().collect()),
            PhysicalPathFailureCode::ForbiddenCharacter,
        ),
    ];

    for (raw_relative_path, failure) in cases {
        VersionPoison::new(
            VersionPoisonV1::InvalidPhysicalPath {
                root_name: "main".into(),
                raw_relative_path: raw_relative_path.clone(),
                failure,
            },
            "canonical classification",
        )
        .unwrap();

        let wrong = if failure == PhysicalPathFailureCode::ForbiddenCharacter {
            PhysicalPathFailureCode::ParentComponent
        } else {
            PhysicalPathFailureCode::ForbiddenCharacter
        };
        assert_eq!(
            VersionPoison::new(
                VersionPoisonV1::InvalidPhysicalPath {
                    root_name: "main".into(),
                    raw_relative_path,
                    failure: wrong,
                },
                "wrong classification",
            )
            .unwrap_err(),
            VersionPoisonError::InvalidRawPath
        );
    }

    assert_eq!(
        VersionPoison::new(
            VersionPoisonV1::InvalidPhysicalPath {
                root_name: "main".into(),
                raw_relative_path: PlatformPathBytes::Unix(b"valid/path".to_vec()),
                failure: PhysicalPathFailureCode::ForbiddenCharacter,
            },
            "valid path cannot be poisoned",
        )
        .unwrap_err(),
        VersionPoisonError::InvalidRawPath
    );
}

#[test]
fn unreadable_scan_subtree_pins_closed_subject_and_failure_encodings() {
    assert_eq!(VersionPoisonCode::UnreadableScanSubtree as u16, 7);
    assert_eq!(ScanFailureCode::PermissionDenied as u16, 1);
    assert_eq!(ScanFailureCode::NotFound as u16, 2);
    assert_eq!(ScanFailureCode::InvalidFileType as u16, 3);
    assert_eq!(ScanFailureCode::SymlinkIdentityChanged as u16, 4);
    assert_eq!(ScanFailureCode::IoDataLoss as u16, 5);

    let root = VersionPoison::new(
        VersionPoisonV1::UnreadableScanSubtree {
            subject: ScanSubject::Root {
                root_name: "main".into(),
            },
            failure: ScanFailureCode::PermissionDenied,
        },
        "cannot enumerate root",
    )
    .unwrap();
    let mut expected = CanonicalEncoder::new();
    expected.raw(&DSVP);
    expected.u8(1);
    expected.u16(7);
    expected.u8(1); // ScanSubject::Root
    expected.str("main");
    expected.u16(1); // PermissionDenied
    assert_eq!(
        root.identity,
        *blake3::hash(&expected.into_bytes()).as_bytes()
    );

    let failures = [
        ScanFailureCode::PermissionDenied,
        ScanFailureCode::NotFound,
        ScanFailureCode::InvalidFileType,
        ScanFailureCode::SymlinkIdentityChanged,
        ScanFailureCode::IoDataLoss,
    ];
    let subjects = [
        ScanSubject::Root {
            root_name: "main".into(),
        },
        ScanSubject::Subtree {
            root_name: "main".into(),
            raw_relative_path: PlatformPathBytes::Unix(b"assets/nested".to_vec()),
        },
        ScanSubject::Subtree {
            root_name: "main".into(),
            raw_relative_path: PlatformPathBytes::Windows(
                "assets\\nested".encode_utf16().collect(),
            ),
        },
    ];
    for subject in subjects {
        for failure in failures {
            let poison = VersionPoison::new(
                VersionPoisonV1::UnreadableScanSubtree {
                    subject: subject.clone(),
                    failure,
                },
                "scan failed",
            )
            .unwrap();
            assert_eq!(poison.code, VersionPoisonCode::UnreadableScanSubtree);
            assert_eq!(
                VersionPoison::from_persisted_bytes(&poison.persisted_bytes().unwrap()).unwrap(),
                poison
            );
        }
    }
}

#[test]
fn unreadable_scan_subtree_rejects_open_tags_and_failure_codes() {
    let mut unknown_subject = CanonicalEncoder::new();
    unknown_subject.raw(&DSVP);
    unknown_subject.u8(1);
    unknown_subject.u16(VersionPoisonCode::UnreadableScanSubtree as u16);
    unknown_subject.u8(3);
    assert_eq!(
        VersionPoison::from_persisted_bytes(&unknown_subject.into_bytes()).unwrap_err(),
        VersionPoisonError::UnknownScanSubjectTag(3)
    );

    let mut unknown_failure = CanonicalEncoder::new();
    unknown_failure.raw(&DSVP);
    unknown_failure.u8(1);
    unknown_failure.u16(VersionPoisonCode::UnreadableScanSubtree as u16);
    unknown_failure.u8(1);
    unknown_failure.str("main");
    unknown_failure.u16(6);
    unknown_failure.str("diagnostic");
    assert_eq!(
        VersionPoison::from_persisted_bytes(&unknown_failure.into_bytes()).unwrap_err(),
        VersionPoisonError::UnknownFailureCode(6)
    );
}

#[test]
fn unreadable_scan_subtree_accepts_only_valid_relative_raw_paths() {
    let invalid = [
        PlatformPathBytes::Unix(b"../outside".to_vec()),
        PlatformPathBytes::Unix(vec![b'x', 0xff]),
        PlatformPathBytes::Windows("C:\\absolute".encode_utf16().collect()),
        PlatformPathBytes::Windows(vec![0xd800]),
    ];
    for raw_relative_path in invalid {
        assert_eq!(
            VersionPoison::new(
                VersionPoisonV1::UnreadableScanSubtree {
                    subject: ScanSubject::Subtree {
                        root_name: "main".into(),
                        raw_relative_path,
                    },
                    failure: ScanFailureCode::IoDataLoss,
                },
                "invalid subject",
            )
            .unwrap_err(),
            VersionPoisonError::InvalidRawPath
        );
    }
}

#[test]
fn unreadable_scan_subtree_identity_and_dedup_include_exact_subject_and_failure() {
    let poison = |path: &[u8], failure| {
        VersionPoison::new(
            VersionPoisonV1::UnreadableScanSubtree {
                subject: ScanSubject::Subtree {
                    root_name: "main".into(),
                    raw_relative_path: PlatformPathBytes::Unix(path.to_vec()),
                },
                failure,
            },
            "scan failed",
        )
        .unwrap()
    };
    let first = poison(b"a", ScanFailureCode::NotFound);
    let duplicate = VersionPoison::new(first.detail.clone(), "different prose").unwrap();
    let different_path = poison(b"b", ScanFailureCode::NotFound);
    let different_failure = poison(b"a", ScanFailureCode::PermissionDenied);
    assert_ne!(first.identity, different_path.identity);
    assert_ne!(first.identity, different_failure.identity);
    let canonical =
        VersionPoison::canonical_set([different_path, duplicate, different_failure, first])
            .unwrap();
    assert_eq!(canonical.len(), 3);
}
