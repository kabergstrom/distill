use distill_core::id::BundleUuid;
use distill_store::imports::{WatchedImportFailure, WatchedImportTerminal};
use distill_store::{Store, StoreConfig};

#[test]
fn watched_import_failure_is_memo_state_and_roundtrips_exact_basis() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    let bundle = BundleUuid([7; 16]);
    let input = store.input_version();
    let before_memo = store.memo_seq();
    let record = WatchedImportFailure {
        bundle,
        attempted_input_version: input,
        basis: vec![0, 1, 2, 255],
        terminal: WatchedImportTerminal::Importer { code: 41 },
        message: "presentation only".into(),
        memo_seq: before_memo,
    };

    let seq = store.record_watched_import_failure(&record).unwrap();
    assert_eq!(store.input_version(), input);
    assert!(seq.0 > before_memo.0);
    let loaded = store.watched_import_failure(bundle).unwrap().unwrap();
    assert_eq!(loaded.bundle, bundle);
    assert_eq!(loaded.attempted_input_version, input);
    assert_eq!(loaded.basis, record.basis);
    assert_eq!(loaded.terminal, record.terminal);
    assert_eq!(loaded.message, record.message);
    assert_eq!(loaded.memo_seq, seq);

    assert!(store.clear_watched_import_failure(bundle).unwrap());
    assert_eq!(store.input_version(), input);
    assert!(store.watched_import_failure(bundle).unwrap().is_none());
}

#[test]
fn dependency_terminal_forbids_an_importer_code_and_upsert_replaces_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    let bundle = BundleUuid([8; 16]);
    let mut record = WatchedImportFailure {
        bundle,
        attempted_input_version: store.input_version(),
        basis: vec![1],
        terminal: WatchedImportTerminal::Dependency,
        message: "missing".into(),
        memo_seq: store.memo_seq(),
    };
    let first = store.record_watched_import_failure(&record).unwrap();
    record.basis = vec![2];
    record.message = "still missing".into();
    let second = store.record_watched_import_failure(&record).unwrap();
    assert!(second.0 > first.0);
    let loaded = store.watched_import_failure(bundle).unwrap().unwrap();
    assert_eq!(loaded.basis, vec![2]);
    assert_eq!(loaded.terminal, WatchedImportTerminal::Dependency);
}

#[test]
fn directory_orphan_terminal_roundtrips_without_an_importer_code() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    let bundle = BundleUuid([9; 16]);
    store
        .record_watched_import_failure(&WatchedImportFailure {
            bundle,
            attempted_input_version: store.input_version(),
            basis: vec![3],
            terminal: WatchedImportTerminal::DirectoryOrphan,
            message: "orphaned".into(),
            memo_seq: store.memo_seq(),
        })
        .unwrap();

    assert_eq!(
        store
            .watched_import_failure(bundle)
            .unwrap()
            .unwrap()
            .terminal,
        WatchedImportTerminal::DirectoryOrphan
    );
}
