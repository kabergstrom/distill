use distill_core::canonical::{CanonicalEncoder, DSVP};
use distill_core::id::{AssetUuid, BundleFileHash};
use distill_store::state::{
    ReadableBundleSource, VersionPoison, VersionPoisonCode, VersionPoisonError, VersionPoisonV1,
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
    let sources = vec![source("main", "a.bundle", 1), source("main", "b.bundle", 2)];
    let detail = VersionPoisonV1::DuplicateAssetUuid {
        asset: AssetUuid([3; 16]),
        sources: sources.clone(),
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
    for source in sources {
        expected.str(&source.root_name);
        expected.str(&source.normalized_path);
        expected.raw(&source.file_hash.0);
    }
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
    let one = source("main", "a.bundle", 1);
    let detail = |sources| VersionPoisonV1::DuplicateAssetUuid {
        asset: AssetUuid([3; 16]),
        sources,
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
