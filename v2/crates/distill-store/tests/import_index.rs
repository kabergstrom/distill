//! The import index: watched read sets joined by path, directory rule
//! sources, and per-source replacement. Rows belong to published bundles.

use distill_core::id::{AssetUuid, BundleUuid, ContentHash};
use distill_store::bundles::BundleMeta;
use distill_store::imports::{ImportIndexSource, ImportReadKey, WatchedImport};
use distill_store::{Store, StoreConfig};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

/// Publish bundle `bundle` at `path` in the root "main".
fn publish(store: &mut Store, path: &str, bundle: u8) {
    store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.upsert_bundle(&BundleMeta {
                bundle: BundleUuid([bundle; 16]),
                root,
                path: path.to_owned(),
                format_version: 1,
                content_hash: ContentHash([bundle; 32]),
                origin: None,
                import_watched: true,
            })
        })
        .unwrap();
}

fn source(path: &str, bundle: u8, reads: Vec<ImportReadKey>) -> ImportIndexSource {
    ImportIndexSource {
        root_name: "main".to_owned(),
        path: path.to_owned(),
        bundle: BundleUuid([bundle; 16]),
        watched: (!reads.is_empty()).then(|| WatchedImport {
            record: AssetUuid([bundle + 100; 16]),
            reads,
        }),
        directory_rules: vec![(AssetUuid([bundle; 16]), format!("dir{bundle}/"))],
    }
}

fn sources(paths: &[&str]) -> Vec<(String, String)> {
    paths
        .iter()
        .map(|path| ("main".to_owned(), (*path).to_owned()))
        .collect()
}

fn bundles(rows: Vec<BundleUuid>) -> Vec<u8> {
    rows.into_iter().map(|row| row.0[0]).collect()
}

#[test]
fn dirty_paths_join_the_read_sets_that_observed_them() {
    let (_d, mut store) = store();
    for (path, bundle) in [("a.bundle", 1), ("b.bundle", 2), ("c.bundle", 3)] {
        publish(&mut store, path, bundle);
    }
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
        rules_at(&store, "b.bundle")[0].rules_bundle,
        BundleUuid([2; 16])
    );
}

#[test]
fn a_source_replacement_drops_only_that_source() {
    let (_d, mut store) = store();
    publish(&mut store, "a.bundle", 1);
    publish(&mut store, "b.bundle", 2);
    store
        .replace_import_index(
            &sources(&["a.bundle", "b.bundle"]),
            &[
                source("a.bundle", 1, vec![ImportReadKey::Path("x".to_owned())]),
                source("b.bundle", 2, vec![ImportReadKey::Path("x".to_owned())]),
            ],
        )
        .unwrap();
    store
        .replace_import_index(&[("main".to_owned(), "a.bundle".to_owned())], &[])
        .unwrap();
    assert_eq!(bundles(store.watched_imports_reading(["x"], false).unwrap()), [2]);
    assert!(rules_at(&store, "a.bundle").is_empty());
    // A bundle that moved is reindexed at its new source; its old rows go.
    publish(&mut store, "c.bundle", 2);
    store
        .replace_import_index(
            &sources(&["b.bundle", "c.bundle"]),
            &[source("c.bundle", 2, vec![ImportReadKey::Path("y".to_owned())])],
        )
        .unwrap();
    assert!(store.watched_imports_reading(["x"], false).unwrap().is_empty());
    assert_eq!(bundles(store.watched_imports_reading(["y"], false).unwrap()), [2]);
    assert_eq!(
        rules_at(&store, "c.bundle")[0].rules_bundle,
        BundleUuid([2; 16])
    );
}

#[test]
fn a_removed_bundle_takes_its_rows_with_it() {
    let (_d, mut store) = store();
    publish(&mut store, "a.bundle", 1);
    store
        .replace_import_index(
            &sources(&["a.bundle"]),
            &[source("a.bundle", 1, vec![ImportReadKey::Path("x".to_owned())])],
        )
        .unwrap();
    store
        .input_transaction(|txn| txn.remove_bundle(BundleUuid([1; 16])))
        .unwrap();
    assert!(store.watched_imports().unwrap().is_empty());
    assert!(store.directory_rule_sources().unwrap().is_empty());
}

#[test]
fn rules_are_found_by_the_directories_a_path_is_under() {
    let (_d, mut store) = store();
    for (path, bundle) in [("a.bundle", 1), ("b.bundle", 2), ("r.bundle", 3)] {
        publish(&mut store, path, bundle);
    }
    let mut whole_root = source("r.bundle", 3, Vec::new());
    whole_root.directory_rules[0].1 = String::new();
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

/// The directory-import rules the source `path` of the root "main" holds.
fn rules_at(store: &distill_store::StoreReader, path: &str) -> Vec<distill_store::imports::DirectoryRuleSource> {
    store
        .directory_rule_sources()
        .unwrap()
        .into_iter()
        .filter(|source| source.root_name == "main" && source.path == path)
        .collect()
}
