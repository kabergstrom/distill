use distill_core::canonical::{CanonicalEncoder, DSVP};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid};
use distill_store::state::{
    AssetClaimant, PhysicalPathClaim, PlatformPathBytes, ReadableBundleSource, VersionPoison,
    VersionPoisonCode, VersionPoisonError, VersionPoisonV1,
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
