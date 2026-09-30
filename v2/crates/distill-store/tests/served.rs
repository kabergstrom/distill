//! Served RPC state: snapshots are read transactions, the change log trims
//! and advances its oldest cursor, artifact load edges are write-once, and
//! the authored-value codec roundtrips.

use distill_core::id::{AssetUuid, ContentHash, TypeUuid};
use distill_store::served::{
    decode_authored_value, encode_authored_value, Change, ResolutionRow, ServedWrite,
};
use distill_store::state::{InputVersion, StoreInstanceId};
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
    assert_eq!(snapshot.input_version(), InputVersion(1));
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
        .change_log_history(InputVersion(0), InputVersion(5))
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
fn artifact_load_edges_are_write_once() {
    let (_dir, mut store) = store();
    let edges = [(AssetUuid([9; 16]), TypeUuid([1; 16]))];
    let hash = store.put_artifact(ASSET, b"artifact bytes", &edges).unwrap();
    assert_eq!(hash, ContentHash(*blake3::hash(b"artifact bytes").as_bytes()));
    assert_eq!(store.cas_read(&hash.0).unwrap(), b"artifact bytes");
    assert_eq!(store.artifact_load_edges(hash).unwrap(), edges);
    assert_eq!(store.put_artifact(ASSET, b"artifact bytes", &edges).unwrap(), hash);
    assert!(store
        .put_artifact(ASSET, b"artifact bytes", &[(AssetUuid([9; 16]), TypeUuid([2; 16]))])
        .is_err());
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
fn embedded_identity_applies_only_to_fresh_stores() {
    let (_dir, mut store) = store();
    let instance = StoreInstanceId([5; 16]);
    store
        .adopt_embedded_identity(instance, InputVersion(40))
        .unwrap();
    assert_eq!(store.stamp().instance, instance);
    assert_eq!(store.reader().unwrap().stamp().version, InputVersion(40));
    assert!(store
        .adopt_embedded_identity(instance, InputVersion(41))
        .is_err());
}

#[test]
fn authored_value_codec_roundtrips() {
    let encoded = encode_authored_value(br#"{"a":1}"#, &[b"one", b""]);
    let (json, blobs) = decode_authored_value(&encoded).unwrap();
    assert_eq!(json, br#"{"a":1}"#);
    assert_eq!(blobs, [b"one".to_vec(), Vec::new()]);
    assert!(decode_authored_value(&encoded[..encoded.len() - 1]).is_err());
}
