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
        directory_rules: vec![(
            BundleUuid([bundle; 16]),
            AssetUuid([bundle; 16]),
            format!("dir{bundle}/"),
        )],
    }
}

fn sources(paths: &[&str]) -> Vec<(String, String)> {
    paths
        .iter()
        .map(|path| ("main".to_owned(), (*path).to_owned()))
        .collect()
}

fn bundles(rows: Vec<WatchedImport>) -> Vec<u8> {
    rows.into_iter().map(|row| row.bundle.0[0]).collect()
}

#[test]
fn dirty_paths_join_the_read_sets_that_observed_them() {
    let (_d, mut store) = store();
    store
        .replace_import_index(
            &sources(&["a.bundle", "b.bundle", "c.bundle"]),
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
            &sources(&["a.bundle", "b.bundle"]),
            &[
                source("a.bundle", 1, vec![ImportReadKey::Path("x".to_owned())]),
                source("b.bundle", 2, vec![ImportReadKey::Path("x".to_owned())]),
            ],
        )
        .unwrap();
    let previous = store
        .replace_import_index(&[("main".to_owned(), "a.bundle".to_owned())], &[])
        .unwrap();
    // The rules the replaced source held come back.
    assert_eq!(previous.len(), 1);
    assert_eq!(previous[0].rules_bundle, BundleUuid([1; 16]));
    assert_eq!(bundles(store.watched_imports_reading(["x"], false).unwrap()), [2]);
    assert!(store.directory_rule_sources_at("main", "a.bundle").unwrap().is_empty());
    // A bundle that moved replaces its old row.
    store
        .replace_import_index(
            &[("main".to_owned(), "c.bundle".to_owned())],
            &[source("c.bundle", 2, vec![ImportReadKey::Path("y".to_owned())])],
        )
        .unwrap();
    assert!(store.watched_imports_reading(["x"], false).unwrap().is_empty());
    assert_eq!(bundles(store.watched_imports_reading(["y"], false).unwrap()), [2]);
}

#[test]
fn rules_are_found_by_the_directories_a_path_is_under() {
    let (_d, mut store) = store();
    let mut whole_root = source("r.bundle", 3, Vec::new());
    whole_root.directory_rules[0].2 = String::new();
    store
        .replace_import_index(
            &sources(&["a.bundle", "b.bundle", "r.bundle"]),
            &[
                source("a.bundle", 1, Vec::new()),
                source("b.bundle", 2, Vec::new()),
                whole_root,
            ],
        )
        .unwrap();
    let found = |dirs: &[&str]| {
        store
            .directory_rule_sources_listing(dirs.iter().copied())
            .unwrap()
            .into_iter()
            .map(|rule| rule.rules_bundle.0[0])
            .collect::<Vec<_>>()
    };
    // `dir1/x.png` is under `""` and `dir1/`.
    assert_eq!(found(&["", "dir1/"]), [1, 3]);
    assert_eq!(found(&["", "elsewhere/"]), [3]);
}
