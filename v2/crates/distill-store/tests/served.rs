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
/// what it read, transitively; a path read names them for that path.
#[test]
fn build_dependents_follow_result_reads_to_a_fixpoint() {
    use distill_store::cas::BuildRead;
    let (_dir, mut store) = store();
    let leaf = AssetUuid([10; 16]);
    let middle = AssetUuid([11; 16]);
    let top = AssetUuid([12; 16]);
    let by_path = AssetUuid([13; 16]);
    // middle reads leaf and has an extra output; top reads middle's extra.
    commit_reading(&mut store, 1, middle, &["", "extra"], vec![BuildRead::Asset(leaf)]);
    let extra = AssetUuid::v5(middle, "extra");
    commit_reading(&mut store, 2, top, &[""], vec![BuildRead::Asset(extra)]);
    commit_reading(&mut store, 3, by_path, &[""], vec![BuildRead::Path("a/b.bundle".into())]);

    let dependents = |store: &mut Store, assets: &[AssetUuid], paths: &[String]| {
        let (assets, paths) = (assets.to_vec(), paths.to_vec());
        store
            .served_transaction(move |txn| txn.build_dependents(&assets, &paths))
            .unwrap()
    };
    let mut expected = vec![middle, extra, top];
    expected.sort();
    assert_eq!(dependents(&mut store, &[leaf], &[]), expected);
    assert_eq!(dependents(&mut store, &[middle], &[]), Vec::<AssetUuid>::new());
    assert_eq!(dependents(&mut store, &[], &["a/b.bundle".into()]), [by_path]);
    assert_eq!(dependents(&mut store, &[], &["a/c.bundle".into()]), Vec::<AssetUuid>::new());
}
