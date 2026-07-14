//! §13 file-tracking tables: per-root physical rows, the derived logical
//! path index with representable ambiguity, and the transactionally
//! consumed dirty queue and rename log (§14's discipline).

use distill_core::id::ContentHash;
use distill_store::files::{DirtyEntry, FileKind, FileState, LogicalPathState};
use distill_store::{Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

fn file_state(mtime: i64) -> FileState {
    FileState {
        mtime,
        size: 42,
        kind: FileKind::File,
        content_hash: Some(ContentHash([3u8; 32])),
    }
}

// ---- roots ----

#[test]
fn root_interning_is_stable_within_and_across_transactions() {
    let (_d, mut store) = store();
    let ((main_a, engine, main_b), _) = store
        .input_transaction(|txn| {
            Ok((
                txn.intern_root("main")?,
                txn.intern_root("engine")?,
                txn.intern_root("main")?,
            ))
        })
        .unwrap();
    assert_eq!(main_a, main_b, "same name, same id");
    assert_ne!(main_a, engine);

    let (main_c, _) = store
        .input_transaction(|txn| txn.intern_root("main"))
        .unwrap();
    assert_eq!(main_a, main_c);
    assert_eq!(store.root_name(main_a).unwrap().as_deref(), Some("main"));
    assert_eq!(store.root_name(engine).unwrap().as_deref(), Some("engine"));
}

// ---- files: per-root physical rows ----

#[test]
fn file_rows_roundtrip_and_are_keyed_per_root() {
    // §13: a single-path key could hold only one of two same-path
    // observations, silently choosing a root — so the key is (root, path).
    let (_d, mut store) = store();
    let ((main, engine), _) = store
        .input_transaction(|txn| {
            let main = txn.intern_root("main")?;
            let engine = txn.intern_root("engine")?;
            txn.upsert_file(main, "tex/rock.bundle", &file_state(100))?;
            txn.upsert_file(engine, "tex/rock.bundle", &file_state(200))?;
            Ok((main, engine))
        })
        .unwrap();

    let a = store.file(main, "tex/rock.bundle").unwrap().unwrap();
    let b = store.file(engine, "tex/rock.bundle").unwrap().unwrap();
    assert_eq!(a.mtime, 100);
    assert_eq!(b.mtime, 200);
    assert_eq!(a.kind, FileKind::File);
    assert_eq!(a.content_hash, Some(ContentHash([3u8; 32])));
    assert!(store.file(main, "absent").unwrap().is_none());
    assert_eq!(
        store
            .all_files()
            .unwrap()
            .into_iter()
            .map(|(root, path, state)| (root, path, state.mtime))
            .collect::<Vec<_>>(),
        [
            (main, "tex/rock.bundle".to_owned(), 100),
            (engine, "tex/rock.bundle".to_owned(), 200),
        ]
    );
}

#[test]
fn upsert_replaces_and_remove_deletes() {
    let (_d, mut store) = store();
    let (root, _) = store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.upsert_file(root, "a.bundle", &file_state(1))?;
            txn.upsert_file(root, "a.bundle", &file_state(2))?;
            Ok(root)
        })
        .unwrap();
    assert_eq!(store.file(root, "a.bundle").unwrap().unwrap().mtime, 2);

    store
        .input_transaction(|txn| {
            assert!(txn.remove_file(root, "a.bundle")?);
            assert!(
                !txn.remove_file(root, "a.bundle")?,
                "second remove is a no-op"
            );
            Ok(())
        })
        .unwrap();
    assert!(store.file(root, "a.bundle").unwrap().is_none());
}

// ---- the derived logical path index ----

#[test]
fn logical_path_index_has_three_states() {
    // §13: Missing, Unique(root), Ambiguous(roots) — ambiguity is
    // representable, not pre-collapsed.
    let (_d, mut store) = store();
    assert_eq!(
        store.logical_path("tex/rock.bundle").unwrap(),
        LogicalPathState::Missing
    );

    let (main, _) = store
        .input_transaction(|txn| {
            let main = txn.intern_root("main")?;
            txn.upsert_file(main, "tex/rock.bundle", &file_state(1))?;
            Ok(main)
        })
        .unwrap();
    assert_eq!(
        store.logical_path("tex/rock.bundle").unwrap(),
        LogicalPathState::Unique(main)
    );

    let (engine, _) = store
        .input_transaction(|txn| {
            let engine = txn.intern_root("engine")?;
            txn.upsert_file(engine, "tex/rock.bundle", &file_state(2))?;
            Ok(engine)
        })
        .unwrap();
    match store.logical_path("tex/rock.bundle").unwrap() {
        LogicalPathState::Ambiguous(mut roots) => {
            roots.sort();
            let mut expected = vec![main, engine];
            expected.sort();
            assert_eq!(roots, expected);
        }
        other => panic!("expected Ambiguous, got {other:?}"),
    }

    // Removing one observation collapses back to Unique.
    store
        .input_transaction(|txn| {
            txn.remove_file(main, "tex/rock.bundle")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store.logical_path("tex/rock.bundle").unwrap(),
        LogicalPathState::Unique(engine)
    );
}

// ---- dirty queue ----

#[test]
fn dirty_queue_is_fifo_and_consumed_transactionally() {
    let (_d, mut store) = store();
    let (root, _) = store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.push_dirty(root, "a.bundle", true)?;
            txn.push_dirty(root, "b.bundle", true)?;
            txn.push_dirty(root, "c.bundle", false)?;
            Ok(root)
        })
        .unwrap();

    // A failed consumer transaction leaves the queue intact (§14: read
    // dirty entries, do the work, clear entries in the same transaction).
    let err = store
        .input_transaction::<Vec<DirtyEntry>, _>(|txn| {
            let taken = txn.take_dirty(10)?;
            assert_eq!(taken.len(), 3);
            Err(StoreError::Poisoned {
                error: "consumer crashed".into(),
            })
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::Poisoned { .. }));

    let (taken, _) = store.input_transaction(|txn| txn.take_dirty(2)).unwrap();
    assert_eq!(taken.len(), 2, "limit respected");
    assert_eq!(taken[0].path, "a.bundle");
    assert_eq!(taken[0].root, root);
    assert!(taken[0].exists);
    assert_eq!(taken[1].path, "b.bundle");

    let (rest, _) = store.input_transaction(|txn| txn.take_dirty(10)).unwrap();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].path, "c.bundle");
    assert!(!rest[0].exists, "deletes carry exists = false");

    let (empty, _) = store.input_transaction(|txn| txn.take_dirty(10)).unwrap();
    assert!(empty.is_empty());
}

// ---- rename log ----

#[test]
fn rename_log_is_ordered_and_consumed_transactionally() {
    let (_d, mut store) = store();
    let (root, _) = store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.push_rename(root, "old.bundle", "mid.bundle")?;
            txn.push_rename(root, "mid.bundle", "new.bundle")?;
            Ok(root)
        })
        .unwrap();

    let (events, _) = store.input_transaction(|txn| txn.take_renames()).unwrap();
    assert_eq!(events.len(), 2);
    // Order is the log order — rename chains must replay in sequence (§14).
    assert_eq!(events[0].from_path, "old.bundle");
    assert_eq!(events[0].to_path, "mid.bundle");
    assert_eq!(events[1].from_path, "mid.bundle");
    assert_eq!(events[1].to_path, "new.bundle");
    assert_eq!(events[0].root, root);

    let (empty, _) = store.input_transaction(|txn| txn.take_renames()).unwrap();
    assert!(empty.is_empty(), "consumed");
}

// ---- clean watermark (§14) ----

#[test]
fn clean_watermark_roundtrips_durably() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join(".distill"));
    let mut store = Store::open(config.clone()).unwrap();
    assert_eq!(store.clean_watermark().unwrap(), None);
    store
        .input_transaction(|txn| txn.set_clean_watermark(1_720_000_000))
        .unwrap();
    drop(store);
    let store = Store::open(config).unwrap();
    assert_eq!(store.clean_watermark().unwrap(), Some(1_720_000_000));
}
