//! The import index: watched read sets joined by path, directory rule
//! sources, and per-source replacement.

use distill_core::id::{AssetUuid, BundleUuid};
use distill_store::imports::{ImportIndexSource, ImportReadKey, WatchedImport};
use distill_store::{Store, StoreConfig};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

fn source(path: &str, bundle: u8, reads: Vec<ImportReadKey>) -> ImportIndexSource {
    ImportIndexSource {
        root_name: "main".to_owned(),
        path: path.to_owned(),
        watched: Some(WatchedImport {
            bundle: BundleUuid([bundle; 16]),
            basis: vec![bundle],
            reads,
        }),
        directory_rules: vec![(BundleUuid([bundle; 16]), AssetUuid([bundle; 16]))],
    }
}

fn bundles(rows: Vec<WatchedImport>) -> Vec<u8> {
    rows.into_iter().map(|row| row.bundle.0[0]).collect()
}

#[test]
fn dirty_paths_join_the_read_sets_that_observed_them() {
    let (_d, mut store) = store();
    store
        .replace_import_index(
            None,
            &[
                source("a.bundle", 1, vec![ImportReadKey::Path("tex/a.png".to_owned())]),
                source("b.bundle", 2, vec![ImportReadKey::Listing]),
                source("c.bundle", 3, vec![ImportReadKey::Capability]),
            ],
        )
        .unwrap();
    assert_eq!(bundles(store.watched_imports().unwrap()), [1, 2, 3]);
    // Listings match in the daemon, so every listing read set is a candidate.
    assert_eq!(
        bundles(store.watched_imports_reading(["tex/a.png"], false).unwrap()),
        [1, 2]
    );
    assert_eq!(
        bundles(store.watched_imports_reading(["other.png"], true).unwrap()),
        [2, 3]
    );
    assert_eq!(store.directory_rule_sources().unwrap().len(), 3);
    assert_eq!(
        store.directory_rule_sources_at("main", "b.bundle").unwrap()[0].rules_bundle,
        BundleUuid([2; 16])
    );
}

#[test]
fn a_source_replacement_drops_only_that_source() {
    let (_d, mut store) = store();
    store
        .replace_import_index(
            None,
            &[
                source("a.bundle", 1, vec![ImportReadKey::Path("x".to_owned())]),
                source("b.bundle", 2, vec![ImportReadKey::Path("x".to_owned())]),
            ],
        )
        .unwrap();
    store
        .replace_import_index(Some(&[("main".to_owned(), "a.bundle".to_owned())]), &[])
        .unwrap();
    assert_eq!(bundles(store.watched_imports_reading(["x"], false).unwrap()), [2]);
    assert!(store.directory_rule_sources_at("main", "a.bundle").unwrap().is_empty());
    // A bundle that moved replaces its old row.
    store
        .replace_import_index(
            Some(&[("main".to_owned(), "c.bundle".to_owned())]),
            &[source("c.bundle", 2, vec![ImportReadKey::Path("y".to_owned())])],
        )
        .unwrap();
    assert!(store.watched_imports_reading(["x"], false).unwrap().is_empty());
    assert_eq!(bundles(store.watched_imports_reading(["y"], false).unwrap()), [2]);
}
