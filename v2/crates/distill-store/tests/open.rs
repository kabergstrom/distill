//! Store open/create lifecycle: on-disk layout under `state_path` (§18),
//! instance-id minting and re-minting (§13), the two version counters,
//! transactional input-version advancement, and schema versioning
//! (daemon state is disposable, §2 — a mismatch is a typed error, never
//! a silent adopt).

use distill_store::{parse_byte_size, Store, StoreConfig, StoreError};

fn cfg(dir: &tempfile::TempDir) -> StoreConfig {
    StoreConfig::new(dir.path().join(".distill"))
}

#[test]
fn open_creates_the_state_layout() {
    let dir = tempfile::tempdir().unwrap();
    let config = cfg(&dir);
    let _store = Store::open(config.clone()).unwrap();
    assert!(config.state_path.join("meta.sqlite").is_file());
    assert!(config.state_path.join("cas").is_dir());
    assert!(config.state_path.join("cas/CURRENT").is_file());
    assert!(
        !config.state_path.join("displaced").exists(),
        "quarantine is per watched/output filesystem, never centralized under state_path"
    );
}

#[test]
fn instance_id_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let first = Store::open(cfg(&dir)).unwrap().instance_id();
    let second = Store::open(cfg(&dir)).unwrap().instance_id();
    assert_eq!(first, second, "one instance, one identity");
}

#[test]
fn reopen_removes_orphaned_tool_stage_files() {
    let dir = tempfile::tempdir().unwrap();
    let config = cfg(&dir);
    drop(Store::open(config.clone()).unwrap());

    let objects = config.state_path.join("tools/objects");
    std::fs::create_dir_all(&objects).unwrap();
    let orphan = objects.join(".stage-abandoned");
    let immutable = objects.join("tool-object");
    std::fs::write(&orphan, b"partial").unwrap();
    std::fs::write(&immutable, b"complete").unwrap();

    drop(Store::open(config).unwrap());

    assert!(!orphan.exists());
    assert_eq!(std::fs::read(immutable).unwrap(), b"complete");
}

#[test]
fn recreate_re_mints_the_instance_id_and_resets_versions() {
    // §13: re-minted whenever daemon state is rebuilt from scratch —
    // InputVersion counters restart after state loss, so the id must
    // change or versions would alias across a client reconnect.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    store.input_transaction(|_txn| Ok(())).unwrap();
    let old_id = store.instance_id();
    assert_eq!(store.input_version().0, 1);
    drop(store);

    let store = Store::recreate(cfg(&dir)).unwrap();
    assert_ne!(store.instance_id(), old_id);
    assert_eq!(store.input_version().0, 0);
    assert_eq!(store.memo_seq().0, 0);
}

#[test]
fn input_transactions_advance_the_input_version_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    assert_eq!(store.input_version().0, 0);
    assert_eq!(store.memo_seq().0, 0);

    let (_, v1) = store.input_transaction(|_txn| Ok(())).unwrap();
    assert_eq!(v1.0, 1);
    let (_, v2) = store.input_transaction(|_txn| Ok(())).unwrap();
    assert_eq!(v2.0, 2);
    assert_eq!(store.input_version().0, 2);
    assert_eq!(
        store.memo_seq().0,
        0,
        "input events never advance the memo sequence"
    );
}

#[test]
fn a_failed_input_transaction_publishes_nothing() {
    // Readers only ever observe a complete input version — all metadata
    // for a given tree state, or none of it (§13).
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let err = store
        .input_transaction::<(), _>(|txn| {
            txn.set_clean_watermark(123)?;
            Err(StoreError::Poisoned {
                error: "boom".to_owned(),
            })
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::Poisoned { .. }));
    assert_eq!(store.input_version().0, 0, "the version was never advanced");
    assert_eq!(
        store.clean_watermark().unwrap(),
        None,
        "the write rolled back"
    );
}

#[test]
fn versions_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    store.input_transaction(|_| Ok(())).unwrap();
    store.input_transaction(|_| Ok(())).unwrap();
    store.input_transaction(|_| Ok(())).unwrap();
    drop(store);
    let store = Store::open(cfg(&dir)).unwrap();
    assert_eq!(store.input_version().0, 3);
}

#[test]
fn stamp_pairs_instance_and_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    store.input_transaction(|_| Ok(())).unwrap();
    let stamp = store.stamp();
    assert_eq!(stamp.instance, store.instance_id());
    assert_eq!(stamp.version, store.input_version());
}

#[test]
fn schema_version_mismatch_is_a_typed_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = cfg(&dir);
    drop(Store::open(config.clone()).unwrap());
    // Sabotage the recorded schema version.
    let conn = rusqlite::Connection::open(config.state_path.join("meta.sqlite")).unwrap();
    conn.pragma_update(None, "user_version", 99).unwrap();
    drop(conn);

    let err = Store::open(config).unwrap_err();
    match err {
        StoreError::SchemaVersionMismatch { found, supported } => {
            assert_eq!(found, 99);
            assert_eq!(supported, distill_store::SCHEMA_VERSION);
        }
        other => panic!("expected SchemaVersionMismatch, got {other:?}"),
    }
}

#[test]
fn sqlite_runs_in_wal_mode() {
    // §13 concurrency model: one writer, many snapshot readers — WAL mode.
    let dir = tempfile::tempdir().unwrap();
    let config = cfg(&dir);
    drop(Store::open(config.clone()).unwrap());
    let conn = rusqlite::Connection::open(config.state_path.join("meta.sqlite")).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal");
}

// ---- configuration (§18 keys the store consumes) ----

#[test]
fn config_defaults_match_section_18() {
    let config = StoreConfig::new("/tmp/x");
    assert_eq!(config.displaced_retention_days, 7);
    assert_eq!(config.segment_size, 256 * 1024 * 1024);
    assert_eq!(config.cache_limit, 20 * 1024 * 1024 * 1024);
    assert_eq!(config.parallelism, 8);
    assert_eq!(config.batch_reserved_workers, 1);
}

#[test]
fn byte_sizes_parse_in_section_18_form() {
    assert_eq!(parse_byte_size("256MiB").unwrap(), 256 * 1024 * 1024);
    assert_eq!(parse_byte_size("20GiB").unwrap(), 20 * 1024 * 1024 * 1024);
    assert_eq!(parse_byte_size("4KiB").unwrap(), 4096);
    assert_eq!(parse_byte_size("1024").unwrap(), 1024);
    assert!(
        parse_byte_size("256MB").is_err(),
        "only binary units are pinned"
    );
    assert!(parse_byte_size("").is_err());
    assert!(parse_byte_size("MiB").is_err());
    assert!(parse_byte_size("-1KiB").is_err());
}
