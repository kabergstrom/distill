//! Served RPC state: snapshots are read transactions, the change log trims
//! and advances its oldest cursor, artifact load edges follow their artifact, and
//! the authored-value codec roundtrips.

use distill_core::id::{AssetUuid, TypeUuid};
use distill_store::served::{Change, ServedWrite};
use distill_store::state::InputVersion;
use distill_store::{Store, StoreConfig};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

const ASSET: AssetUuid = AssetUuid([3; 16]);

#[test]
fn snapshot_keeps_its_version_while_the_writer_publishes() {
    let (_dir, mut store) = store();
    store
        .input_transaction(|txn| {
            let version = txn.version();
            txn.append_change(version, &Change::Path { path: "a".into() })
        })
        .unwrap();
    let snapshot = store.reader().unwrap().begin_snapshot().unwrap();
    assert_eq!(snapshot.stamp().version, InputVersion(1));
    store
        .input_transaction(|txn| {
            let version = txn.version();
            txn.append_change(
                version,
                &Change::Asset {
                    asset: ASSET,
                    state: 3,
                },
            )
        })
        .unwrap();
    assert_eq!(snapshot.input_version().unwrap(), InputVersion(1));
    assert_eq!(snapshot.change_log_after(0).unwrap().len(), 1);
    let reader = snapshot.into_reader().unwrap();
    let changes = reader.change_log_after(0).unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[1].version, InputVersion(2));
    assert_eq!(
        changes[1].change,
        Change::Asset {
            asset: ASSET,
            state: 3
        }
    );
}

#[test]
fn change_log_trim_advances_the_oldest_cursor() {
    let (_dir, mut store) = store();
    for _ in 0..5 {
        store
            .input_transaction(|txn| {
                let version = txn.version();
                txn.append_change(version, &Change::Path { path: "a".into() })?;
                txn.append_change(version, &Change::ReconnectAll)?;
                txn.trim_change_log(version, 2)
            })
            .unwrap();
    }
    assert_eq!(store.change_log_oldest().unwrap(), InputVersion(3));
    let history = store
        .change_log_history(
            InputVersion(0),
            InputVersion(5),
            &std::collections::BTreeSet::new(),
            &std::collections::BTreeSet::from(["a".to_owned()]),
        )
        .unwrap();
    assert_eq!(
        history
            .iter()
            .map(|entry| entry.version.0)
            .collect::<Vec<_>>(),
        [4, 5]
    );
    assert!(history
        .iter()
        .all(|entry| matches!(entry.change, Change::Path { .. })));
}

#[test]
fn a_target_is_installed_replaced_and_removed_without_a_fence() {
    let (_dir, mut store) = store();
    store
        .served_transaction(|txn| {
            txn.set_rpc_target("pc", [1; 16])?;
            txn.set_rpc_target("console", [3; 16])?;
            txn.set_rpc_target("pc", [2; 16])?;
            txn.remove_rpc_target("console")
        })
        .unwrap();
    let targets = store.rpc_targets().unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(
        (targets[0].name.as_str(), targets[0].definition_hash),
        ("pc", [2; 16])
    );
    assert_eq!(store.rpc_pipeline_generation().unwrap(), 0);
    assert_eq!(store.change_log_head().unwrap(), 0);
    store
        .served_transaction(|txn| txn.bump_rpc_pipeline_generation())
        .unwrap();
    assert_eq!(store.rpc_pipeline_generation().unwrap(), 1);
}

#[test]
fn an_artifacts_load_edges_are_its_latest_installs_and_go_with_it() {
    // The artifact image does not carry the edges' expected terminals, so a
    // dependency whose terminal type changed rebuilds the same bytes with
    // other edges. That publication succeeds and its edges are served; the
    // edges live exactly as long as the artifact's extent.
    let (_dir, mut store) = store();
    let old = [(AssetUuid([9; 16]), TypeUuid([1; 16]))];
    let new = [(AssetUuid([9; 16]), TypeUuid([2; 16]))];
    let hash = store.put_artifact(b"artifact bytes", &old).unwrap();
    store
        .put_artifact(b"artifact bytes", &new)
        .expect("a rebuild with changed load edges publishes");
    assert_eq!(store.artifact_load_edges(hash).unwrap(), new);
    store.evict_installed(&hash.0).unwrap();
    assert!(store.cas_read(&hash.0).is_err());
    assert!(
        store.artifact_load_edges(hash).unwrap().is_empty(),
        "load edges outlived their artifact"
    );
}

/// Commit a processor result for `parent` with outputs `keys` whose trace
/// read `reads`.
fn commit_reading(
    store: &mut Store,
    key: u8,
    parent: AssetUuid,
    keys: &[&str],
    reads: Vec<distill_store::cas::BuildRead>,
) {
    use distill_store::cas::record::KeyKind;
    use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec};
    store
        .commit_build(BuildCommit {
            key_kind: KeyKind::Processor,
            static_input_key: [key; 16],
            asset_uuid: parent,
            trace: vec![key],
            reads,
            outcome: CommitOutcome::Success {
                outputs: keys
                    .iter()
                    .map(|output_key| OutputSpec {
                        output_key: (*output_key).to_owned(),
                        type_uuids: vec![],
                        bytes: vec![key; 64],
                    })
                    .collect(),
                aux: vec![],
            },
            wire_trees: vec![],
        })
        .unwrap();
}

/// A result's reads name its parent and derived outputs as dependents of
/// what it read, transitively; a path read names them for that path, a
/// type file read for that type file.
#[test]
fn build_dependents_follow_result_reads_to_a_fixpoint() {
    use distill_store::cas::BuildRead;
    let (_dir, mut store) = store();
    let leaf = AssetUuid([10; 16]);
    let middle = AssetUuid([11; 16]);
    let top = AssetUuid([12; 16]);
    let by_path = AssetUuid([13; 16]);
    let by_type_file = AssetUuid([14; 16]);
    // middle reads leaf and has an extra output; top reads middle's extra.
    commit_reading(&mut store, 1, middle, &["", "extra"], vec![BuildRead::Asset(leaf)]);
    let extra = AssetUuid::v5(middle, "extra");
    commit_reading(&mut store, 2, top, &[""], vec![BuildRead::Asset(extra)]);
    commit_reading(&mut store, 3, by_path, &[""], vec![BuildRead::Path("a/b.bundle".into())]);
    // A path and a type file of the same text are different subjects.
    commit_reading(&mut store, 4, by_type_file, &[""], vec![BuildRead::TypeFile("a/b.bundle".into())]);

    let dependents = |store: &mut Store, assets: &[AssetUuid], paths: &[String], type_files: &[String]| {
        let (assets, paths, type_files) = (assets.to_vec(), paths.to_vec(), type_files.to_vec());
        store
            .served_transaction(move |txn| txn.build_dependents(&assets, &paths, &type_files))
            .unwrap()
    };
    let mut expected = vec![middle, extra, top];
    expected.sort();
    assert_eq!(dependents(&mut store, &[leaf], &[], &[]), expected);
    assert_eq!(dependents(&mut store, &[middle], &[], &[]), Vec::<AssetUuid>::new());
    assert_eq!(dependents(&mut store, &[], &["a/b.bundle".into()], &[]), [by_path]);
    assert_eq!(dependents(&mut store, &[], &["a/c.bundle".into()], &[]), Vec::<AssetUuid>::new());
    assert_eq!(dependents(&mut store, &[], &[], &["a/b.bundle".into()]), [by_type_file]);
    assert_eq!(dependents(&mut store, &[], &[], &["a/c.bundle".into()]), Vec::<AssetUuid>::new());
}

/// The type files a version changed are the file changes the scan logged
/// at it under the root holding the marker; a marker change, in any root,
/// is a change of every type file a result read.
#[test]
fn changed_type_files_are_the_logged_file_changes_under_the_marker_root() {
    use distill_core::id::ContentHash;
    use distill_store::cas::BuildRead;
    use distill_store::files::{FileKind, FileObservation, FileState};
    use ngp_schema::prefab_types::MARKER;
    let (_dir, mut store) = store();
    commit_reading(&mut store, 1, AssetUuid([20; 16]), &[""], vec![BuildRead::TypeFile("kernels/a.bin".into())]);
    commit_reading(&mut store, 2, AssetUuid([21; 16]), &[""], vec![BuildRead::TypeFile("schemas/b.bin".into())]);
    // Version 1: the marker in `types` (the first publication logs nothing).
    store
        .input_transaction(|txn| {
            let version = txn.version();
            let types = txn.intern_root("types")?;
            let marker = FileObservation::from(FileState {
                mtime: 1,
                size: 1,
                kind: FileKind::File,
                content_hash: Some(ContentHash([1; 16])),
            });
            txn.upsert_file(types, MARKER, &marker, version)?;
            txn.push_dirty(types, "types", MARKER, true, version)
        })
        .unwrap();
    // Version 2: a type file changed and removed, and a file in another root.
    store
        .input_transaction(|txn| {
            let version = txn.version();
            let types = txn.intern_root("types")?;
            let main = txn.intern_root("main")?;
            txn.push_dirty(types, "types", "kernels/a.bin", true, version)?;
            txn.push_dirty(main, "main", "kernels/c.bin", true, version)?;
            txn.push_dirty(types, "types", "kernels/a.bin", false, version)
        })
        .unwrap();
    // Version 3: a type file's path in another root.
    store
        .input_transaction(|txn| {
            let version = txn.version();
            let main = txn.intern_root("main")?;
            txn.push_dirty(main, "main", "kernels/a.bin", true, version)
        })
        .unwrap();
    // Version 4: a marker in another root.
    store
        .input_transaction(|txn| {
            let version = txn.version();
            let main = txn.intern_root("main")?;
            txn.push_dirty(main, "main", MARKER, true, version)
        })
        .unwrap();
    let changed = |store: &mut Store, version: u64| {
        store
            .served_transaction(move |txn| txn.changed_type_files(InputVersion(version)))
            .unwrap()
    };
    assert_eq!(changed(&mut store, 1), Vec::<String>::new());
    assert_eq!(changed(&mut store, 2), ["kernels/a.bin"]);
    assert_eq!(changed(&mut store, 3), Vec::<String>::new());
    assert_eq!(changed(&mut store, 4), ["kernels/a.bin", "schemas/b.bin"]);
}
