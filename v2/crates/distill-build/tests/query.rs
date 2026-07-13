use distill_build::query::*;
use distill_core::id::{AssetUuid, BundleUuid};

#[test]
fn paths_are_lexically_contained_and_nfc_normalized() {
    assert_eq!(
        normalize_path("te\u{301}x/a.png").unwrap(),
        "t\u{e9}x/a.png"
    );
    for bad in ["", "/a", "a/", "a//b", ".", "a/../b", "a\\b", "a\0b"] {
        assert!(normalize_path(bad).is_err(), "accepted {bad:?}");
    }
}

#[test]
fn bare_local_id_closes_over_its_origin_bundle() {
    let origin = BundleUuid([7; 16]);
    let q = AssetQuery {
        local_id: Some("e\u{301}lite".into()),
        ..Default::default()
    }
    .close(Some(origin))
    .unwrap();
    assert_eq!(q.local_id.as_deref(), Some("\u{e9}lite"));
    assert_eq!(q.bundle_uuid, Some(origin));
    assert!(AssetQuery {
        local_id: Some("x".into()),
        ..Default::default()
    }
    .close(None)
    .is_err());
}

#[test]
fn empty_queries_and_invalid_globs_are_rejected() {
    assert!(AssetQuery::default().close(None).is_err());
    assert!(FileQuery::new(None, None).is_err());
    assert!(FileQuery::new(None, Some("[".into())).is_err());
    assert!(FileQuery::new(Some("a/../b".into()), None).is_err());
}

#[test]
fn query_hashes_are_sorted_deduplicated_and_domain_separated() {
    let a = AssetUuid([1; 16]);
    let b = AssetUuid([2; 16]);
    assert_eq!(
        asset_query_result_hash(&[b, a, a]),
        asset_query_result_hash(&[a, b])
    );

    let files = vec![
        RootedPath::new("z", "a").unwrap(),
        RootedPath::new("a", "z").unwrap(),
    ];
    assert_eq!(
        file_query_result_hash(&files),
        file_query_result_hash(&[files[1].clone(), files[0].clone()])
    );
    assert_ne!(asset_query_result_hash(&[]), file_query_result_hash(&[]));
}

#[test]
fn file_hash_frames_root_names_not_ordinals() {
    let a = RootedPath::new("root-a", "x/y").unwrap();
    let b = RootedPath::new("root-b", "x/y").unwrap();
    assert_ne!(file_query_result_hash(&[a]), file_query_result_hash(&[b]));
}
