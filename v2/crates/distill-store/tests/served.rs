//! Served RPC state: snapshots are read transactions, the change log trims
//! and advances its oldest cursor, artifact load edges follow their artifact, and
//! the authored-value codec roundtrips.

use distill_core::id::{AssetUuid, TypeUuid};
use distill_store::served::{
    Change, ResolutionRow, ServedWrite,
};
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
        .input_transaction(|txn| txn.set_asset_resolution(ASSET, Some(&ResolutionRow::Missing)))
        .unwrap();
    let snapshot = store.reader().unwrap().begin_snapshot().unwrap();
    assert_eq!(snapshot.stamp().version, InputVersion(1));
    store
        .input_transaction(|txn| {
            let version = txn.version();
            txn.set_asset_resolution(ASSET, Some(&ResolutionRow::Failed("boom".into())))?;
            txn.append_change(version, &Change::Asset { asset: ASSET, state: 3 })
        })
        .unwrap();
    assert_eq!(
        snapshot.asset_resolution(ASSET).unwrap(),
        Some(ResolutionRow::Missing)
    );
    assert_eq!(snapshot.input_version().unwrap(), InputVersion(1));
    assert!(snapshot.change_log_after(0).unwrap().is_empty());
    let reader = snapshot.into_reader().unwrap();
    assert_eq!(
        reader.asset_resolution(ASSET).unwrap(),
        Some(ResolutionRow::Failed("boom".into()))
    );
    let changes = reader.change_log_after(0).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].version, InputVersion(2));
    assert_eq!(changes[0].change, Change::Asset { asset: ASSET, state: 3 });
}

#[test]
fn change_log_trim_advances_the_oldest_cursor() {
    let (_dir, mut store) = store();
    for _ in 0..5 {
        store
            .input_transaction(|txn| {
                let version = txn.version();
                txn.append_change(version, &Change::Path { path: "a".into() })?;
                txn.append_change(version, &Change::ReconnectAll { reason: 1 })?;
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
        history.iter().map(|entry| entry.version.0).collect::<Vec<_>>(),
        [4, 5]
    );
    assert!(history
        .iter()
        .all(|entry| matches!(entry.change, Change::Path { .. })));
}

#[test]
fn rpc_targets_advance_generation_only_on_change() {
    let (_dir, mut store) = store();
    let changed = store
        .served_transaction(|txn| {
            Ok([
                txn.set_rpc_target("pc", [1; 32])?,
                txn.set_rpc_target("pc", [1; 32])?,
                txn.set_rpc_target("pc", [2; 32])?,
            ])
        })
        .unwrap();
    assert_eq!(changed, [false, false, true]);
    assert_eq!(store.rpc_target("pc").unwrap().unwrap().generation, 1);
    assert_eq!(store.rpc_fences().unwrap().pipeline_generation, 0);
    store
        .served_transaction(|txn| txn.bump_rpc_pipeline_generation())
        .unwrap();
    assert_eq!(store.rpc_fences().unwrap().pipeline_generation, 1);
}

#[test]
fn an_artifacts_load_edges_are_its_latest_installs_and_go_with_it() {
    // The DSTL bytes do not carry the edges' expected terminals, so a
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
