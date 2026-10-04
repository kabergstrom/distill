//! Scan claims: per-source rows, collisions kept for the touched subjects
//! (as namespace error rows), the pending subjects a replacement returns,
//! and rollback with the owning input transaction.

use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, TypeUuid};
use distill_store::claims::{DerivedOutputClaim, SourceClaim, SourceClaims};
use distill_store::state::{AssetClaimant, NamespaceErrorV1, ReadableBundleSource};
use distill_store::{Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

fn source(path: &str, bundle: u8, asset: u8) -> SourceClaims {
    let readable = ReadableBundleSource {
        root_name: "main".to_owned(),
        normalized_path: path.to_owned(),
        file_hash: BundleFileHash([asset; 32]),
    };
    SourceClaims {
        root_name: "main".to_owned(),
        path: path.to_owned(),
        claims: vec![
            SourceClaim::Bundle {
                bundle: BundleUuid([bundle; 16]),
                source: readable.clone(),
            },
            SourceClaim::Authored {
                asset: AssetUuid([asset; 16]),
                claimant: AssetClaimant::Authored {
                    source: readable,
                    bundle: BundleUuid([bundle; 16]),
                    local_id: "main".to_owned(),
                },
            },
            SourceClaim::DerivedOutput {
                child: AssetUuid([asset + 100; 16]),
                output: DerivedOutputClaim {
                    parent: AssetUuid([asset; 16]),
                    output_key: "thumb".to_owned(),
                    terminal_type: TypeUuid([9; 16]),
                },
            },
            SourceClaim::PrimaryPath {
                path: path.to_owned(),
                asset: AssetUuid([asset; 16]),
            },
        ],
    }
}

fn under(path: &str) -> Vec<(String, String)> {
    vec![("main".to_owned(), path.to_owned())]
}

#[test]
fn a_shared_bundle_uuid_collides_until_one_claimant_leaves() {
    let (_d, mut store) = store();
    let (pending, _) = store
        .input_transaction(|txn| {
            txn.replace_source_claims(
                None,
                &[source("a.bundle", 1, 10), source("b.bundle", 1, 20)],
            )
        })
        .unwrap();
    let [error] = <[_; 1]>::try_from(store.namespace_errors().unwrap()).unwrap();
    let NamespaceErrorV1::DuplicateBundleUuid { bundle, sources } = &error.detail else {
        panic!("expected a bundle collision, got {error:?}");
    };
    assert_eq!(*bundle, BundleUuid([1; 16]));
    assert_eq!(sources.len(), 2);
    // A full replacement leaves nothing pending.
    assert_eq!(pending, Default::default());

    let (pending, _) = store
        .input_transaction(|txn| {
            txn.replace_source_claims(Some(&under("b.bundle")), &[source("b.bundle", 2, 20)])
        })
        .unwrap();
    assert!(store.namespace_errors().unwrap().is_empty());
    assert_eq!(
        pending.bundles.keys().copied().collect::<Vec<_>>(),
        [BundleUuid([1; 16]), BundleUuid([2; 16])]
    );
    assert_eq!(pending.paths.into_iter().collect::<Vec<_>>(), ["b.bundle"]);
    assert_eq!(
        pending.bundles[&BundleUuid([2; 16])][0].normalized_path,
        "b.bundle"
    );
    assert_eq!(
        store.derived_output_claims(AssetUuid([120; 16])).unwrap(),
        [DerivedOutputClaim {
            parent: AssetUuid([20; 16]),
            output_key: "thumb".to_owned(),
            terminal_type: TypeUuid([9; 16]),
        }]
    );
    assert_eq!(
        store
            .path_claims("b.bundle")
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        [AssetUuid([20; 16])]
    );
}

#[test]
fn an_asset_uuid_authored_twice_collides() {
    let (_d, mut store) = store();
    store
        .input_transaction(|txn| {
            txn.replace_source_claims(Some(&under("")), &[source("a.bundle", 1, 10)])?;
            txn.replace_source_claims(Some(&under("b.bundle")), &[source("b.bundle", 2, 10)])
        })
        .unwrap();
    let [error] = <[_; 1]>::try_from(store.namespace_errors().unwrap()).unwrap();
    assert!(
        matches!(
            &error.detail,
            NamespaceErrorV1::DuplicateAssetUuid { asset, claimants }
                if *asset == AssetUuid([10; 16]) && claimants.len() == 2
        ),
        "{error:?}"
    );
    // Both sources derive the same child from the same parent: one claimant.
    assert_eq!(
        store
            .derived_output_claims(AssetUuid([110; 16]))
            .unwrap()
            .len(),
        1
    );
    // The withheld parent withholds its child.
    assert_eq!(store.resolve_child(AssetUuid([110; 16])).unwrap(), None);

    // The collision ending makes the survivor's bundle pending, though its
    // own claims did not change: it publishes the asset again.
    let (pending, _) = store
        .input_transaction(|txn| txn.replace_source_claims(Some(&under("b.bundle")), &[]))
        .unwrap();
    assert!(store.namespace_errors().unwrap().is_empty());
    assert!(pending.bundles.contains_key(&BundleUuid([1; 16])));
    assert_eq!(
        store.resolve_child(AssetUuid([110; 16])).unwrap(),
        Some((AssetUuid([10; 16]), "thumb".to_owned()))
    );
    assert!(pending.bundles[&BundleUuid([2; 16])].is_empty());
}

#[test]
fn claims_roll_back_with_their_transaction() {
    let (_d, mut store) = store();
    let failed = store.input_transaction::<(), _>(|txn| {
        let pending = txn.replace_source_claims(
            Some(&under("")),
            &[source("a.bundle", 1, 10), source("b.bundle", 1, 20)],
        )?;
        assert_eq!(pending.bundles.len(), 1);
        // Both sources claim the bundle.
        assert_eq!(pending.bundles[&BundleUuid([1; 16])].len(), 2);
        assert_eq!(txn.reader().namespace_errors()?.len(), 1);
        Err(StoreError::Rejected {
            detail: "roll back".to_owned(),
        })
    });
    assert!(failed.is_err());
    // No claim was left to replace.
    let (pending, _) = store
        .input_transaction(|txn| txn.replace_source_claims(Some(&under("")), &[]))
        .unwrap();
    assert!(pending.bundles.is_empty());
    assert!(store.namespace_errors().unwrap().is_empty());
}

/// The claims' namespace errors publish as the scan's family: a malformed
/// source's error joins the collision rows the claims keep, a stale row
/// goes, and the family holds exactly what the publication returns.
#[test]
fn the_claims_namespace_errors_publish_as_the_scan_family() {
    use distill_store::state::{NamespaceError, SkeletonFailureCode};
    let (_d, mut store) = store();
    let malformed = NamespaceError::new(
        NamespaceErrorV1::IncompleteSkeleton {
            source: ReadableBundleSource {
                root_name: "main".to_owned(),
                normalized_path: "broken.bundle".to_owned(),
                file_hash: BundleFileHash([9; 32]),
            },
            failure: SkeletonFailureCode::IncompleteAssetIdentity,
        },
        "incomplete skeleton",
    )
    .unwrap();
    let broken = SourceClaims {
        root_name: "main".to_owned(),
        path: "broken.bundle".to_owned(),
        claims: vec![SourceClaim::Malformed(malformed.clone())],
    };
    let (published, _) = store
        .input_transaction(|txn| {
            txn.replace_source_claims(
                Some(&under("")),
                &[source("a.bundle", 1, 10), source("b.bundle", 1, 20), broken],
            )?;
            txn.publish_claims_namespace_errors()
        })
        .unwrap();
    assert_eq!(published.len(), 2);
    assert!(published.contains(&malformed));
    assert_eq!(store.namespace_errors().unwrap(), published);

    let (published, _) = store
        .input_transaction(|txn| {
            txn.replace_source_claims(Some(&under("broken.bundle")), &[])?;
            txn.publish_claims_namespace_errors()
        })
        .unwrap();
    let [collision] = <[_; 1]>::try_from(published).unwrap();
    assert!(matches!(
        collision.detail,
        NamespaceErrorV1::DuplicateBundleUuid { .. }
    ));
    assert_eq!(store.namespace_errors().unwrap(), [collision]);
}
