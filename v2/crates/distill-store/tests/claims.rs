//! Scan claims: per-source rows, collisions kept for the touched subjects,
//! pending subjects, and rollback with the owning input transaction.

use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, TypeUuid};
use distill_store::claims::{DerivedOutputClaim, SourceClaim, SourceClaims};
use distill_store::state::{AssetClaimant, ReadableBundleSource, VersionPoisonV1};
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
    store
        .input_transaction(|txn| {
            txn.replace_source_claims(None, &[source("a.bundle", 1, 10), source("b.bundle", 1, 20)])
        })
        .unwrap();
    let [poison] = <[_; 1]>::try_from(store.claims_namespace_errors().unwrap()).unwrap();
    let VersionPoisonV1::DuplicateBundleUuid { bundle, sources } = poison.detail else {
        panic!("expected a bundle collision, got {poison:?}");
    };
    assert_eq!(bundle, BundleUuid([1; 16]));
    assert_eq!(sources.len(), 2);
    // A full replacement leaves nothing pending.
    assert!(store.pending_claims().unwrap().bundles.is_empty());

    store
        .input_transaction(|txn| {
            txn.replace_source_claims(Some(&under("b.bundle")), &[source("b.bundle", 2, 20)])
        })
        .unwrap();
    assert!(store.claims_namespace_errors().unwrap().is_empty());
    let pending = store.pending_claims().unwrap();
    assert_eq!(
        pending.bundles.into_iter().collect::<Vec<_>>(),
        [BundleUuid([1; 16]), BundleUuid([2; 16])]
    );
    assert_eq!(
        pending.derived.into_iter().collect::<Vec<_>>(),
        [AssetUuid([120; 16])]
    );
    assert_eq!(
        pending.paths.into_iter().collect::<Vec<_>>(),
        ["b.bundle"]
    );
    assert_eq!(
        store.bundle_claim_sources(BundleUuid([2; 16])).unwrap()[0].normalized_path,
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
        store.path_claims("b.bundle").unwrap().into_iter().collect::<Vec<_>>(),
        [AssetUuid([20; 16])]
    );

    store
        .input_transaction(|txn| txn.clear_pending_claims())
        .unwrap();
    assert_eq!(store.pending_claims().unwrap(), Default::default());
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
    let [poison] = <[_; 1]>::try_from(store.claims_namespace_errors().unwrap()).unwrap();
    assert!(
        matches!(
            &poison.detail,
            VersionPoisonV1::DuplicateAssetUuid { asset, claimants }
                if *asset == AssetUuid([10; 16]) && claimants.len() == 2
        ),
        "{poison:?}"
    );
    // Both sources derive the same child from the same parent: one claimant.
    assert_eq!(store.derived_output_claims(AssetUuid([110; 16])).unwrap().len(), 1);

    // The collision ending makes the survivor's bundle pending, though its
    // own claims did not change: it publishes the asset again.
    store
        .input_transaction(|txn| {
            txn.clear_pending_claims()?;
            txn.replace_source_claims(Some(&under("b.bundle")), &[])
        })
        .unwrap();
    assert!(store.claims_namespace_errors().unwrap().is_empty());
    assert!(store
        .pending_claims()
        .unwrap()
        .bundles
        .contains(&BundleUuid([1; 16])));
    assert!(store.bundle_claim_sources(BundleUuid([2; 16])).unwrap().is_empty());
}

#[test]
fn claims_roll_back_with_their_transaction() {
    let (_d, mut store) = store();
    let failed = store.input_transaction::<(), _>(|txn| {
        txn.replace_source_claims(Some(&under("")), &[source("a.bundle", 1, 10)])?;
        let view = txn.reader();
        assert_eq!(view.pending_claims()?.bundles.len(), 1);
        Err(StoreError::Rejected {
            detail: "roll back".to_owned(),
        })
    });
    assert!(failed.is_err());
    assert!(store.bundle_claim_sources(BundleUuid([1; 16])).unwrap().is_empty());
    assert_eq!(store.pending_claims().unwrap(), Default::default());
}
