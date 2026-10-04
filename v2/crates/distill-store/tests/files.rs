//! §13 file-tracking tables: per-root physical rows and the transactionally
//! consumed dirty queue and rename log (§14's discipline).

use distill_core::id::ContentHash;
use distill_store::files::{FileKind, FileObservation, FileState};
use distill_store::state::InputVersion;
use distill_store::{Store, StoreConfig};

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
            txn.upsert_file(
                main,
                "tex/rock.bundle",
                &file_state(100).into(),
                InputVersion(1),
            )?;
            txn.upsert_file(
                engine,
                "tex/rock.bundle",
                &file_state(200).into(),
                InputVersion(1),
            )?;
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
    let _ = (main, engine);
    assert_eq!(
        store
            .observed_files()
            .unwrap()
            .into_iter()
            .map(|row| (row.root_name, row.path, row.file.state.mtime))
            .collect::<Vec<_>>(),
        [
            ("engine".to_owned(), "tex/rock.bundle".to_owned(), 200),
            ("main".to_owned(), "tex/rock.bundle".to_owned(), 100),
        ]
    );
}

#[test]
fn upsert_replaces_and_remove_deletes() {
    let (_d, mut store) = store();
    let (root, _) = store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.upsert_file(root, "a.bundle", &file_state(1).into(), InputVersion(1))?;
            txn.upsert_file(root, "a.bundle", &file_state(2).into(), InputVersion(1))?;
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

// ---- dirty queue ----

#[test]
fn pending_file_work_acknowledges_only_the_observed_sequence_prefix() {
    let (_directory, mut store) = store();
    let (root, _) = store
        .input_transaction(|transaction| {
            let root = transaction.intern_root("main")?;
            transaction.push_dirty(root, "main", "old.bundle", false, InputVersion(1))?;
            transaction.push_rename(root, "main", "old.bundle", "new.bundle")?;
            Ok(root)
        })
        .unwrap();
    let observed = store.pending_file_work().unwrap();
    assert_eq!(observed.dirty.len(), 1);
    assert_eq!(observed.renames.len(), 1);

    store
        .input_transaction(|transaction| {
            transaction.push_dirty(root, "main", "later.bundle", true, InputVersion(2))?;
            transaction.push_rename(root, "main", "later.bundle", "last.bundle")
        })
        .unwrap();
    let version = store.input_version().unwrap();
    let acknowledged = store.acknowledge_file_work(&observed).unwrap();
    assert!(acknowledged);
    assert_eq!(
        store.input_version().unwrap(),
        version,
        "internal queue acknowledgement is unversioned"
    );

    let remaining = store.pending_file_work().unwrap();
    assert_eq!(remaining.dirty.len(), 1);
    assert_eq!(remaining.dirty[0].path, "later.bundle");
    assert_eq!(remaining.renames.len(), 1);
    assert_eq!(remaining.renames[0].to_path, "last.bundle");
}

#[test]
fn stale_observation_cannot_acknowledge_newer_work_for_the_same_path() {
    let (_directory, mut store) = store();
    let (root, _) = store
        .input_transaction(|transaction| {
            let root = transaction.intern_root("main")?;
            transaction.upsert_file(root, "source.txt", &file_state(1).into(), InputVersion(1))?;
            transaction.push_dirty(root, "main", "source.txt", true, InputVersion(1))?;
            Ok(root)
        })
        .unwrap();
    let stale = store.pending_file_work().unwrap();
    store
        .input_transaction(|transaction| {
            transaction.upsert_file(root, "source.txt", &file_state(2).into(), InputVersion(2))?;
            transaction.push_dirty(root, "main", "source.txt", true, InputVersion(2))
        })
        .unwrap();

    // The captured row is consumed; the newer one is more work.
    let acknowledged = store.acknowledge_file_work(&stale).unwrap();
    assert!(!acknowledged);
    let remaining = store.pending_file_work().unwrap();
    assert_eq!(remaining.dirty.len(), 1);
    assert_eq!(remaining.dirty[0].observation, InputVersion(2));
}

// ---- scan observation tables ----

#[test]
fn a_transaction_view_reads_its_own_uncommitted_scan_rows() {
    let (_d, mut store) = store();
    let observation = FileObservation {
        state: file_state(7),
        raw_path: b"\0tex/Rock.bundle".to_vec(),
        symlink_target: Some(b"/project/tex/rock.bundle".to_vec()),
        canonical_path: None,
    };
    let failed = store.input_transaction::<(), _>(|txn| {
        let root = txn.intern_root("main")?;
        txn.upsert_file(root, "tex/rock.bundle", &observation, InputVersion(1))?;
        let view = txn.reader();
        let [row] =
            <[_; 1]>::try_from(view.observed_files_under("main", "tex/rock.bundle")?).unwrap();
        assert_eq!(row.file, observation);
        assert_eq!(
            view.symlinks_targeting(b"/project/tex")?
                .into_iter()
                .map(|row| row.path)
                .collect::<Vec<_>>(),
            ["tex/rock.bundle"]
        );
        Err(distill_store::StoreError::Rejected {
            detail: "roll back".to_owned(),
        })
    });
    assert!(failed.is_err());
    assert!(store
        .observed_files_under("main", "tex/rock.bundle")
        .unwrap()
        .is_empty());
}

#[test]
fn a_directory_row_is_found_by_its_unique_canonical_path() {
    let (_d, mut store) = store();
    let directory = |canonical: &str| FileObservation {
        canonical_path: Some(format!("/project/{canonical}").into_bytes()),
        ..FileObservation::from(FileState {
            kind: FileKind::Directory,
            content_hash: None,
            ..file_state(1)
        })
    };
    let write = |store: &mut Store, path: &str, file: FileObservation| {
        store.input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.upsert_file(root, path, &file, txn.version())
        })
    };
    write(&mut store, "a", directory("a")).unwrap();
    write(&mut store, "ab", directory("ab")).unwrap();
    let found = store
        .directory_by_canonical(b"/project/ab")
        .unwrap()
        .unwrap();
    assert_eq!((found.path, found.file), ("ab".to_owned(), directory("ab")));
    assert_eq!(store.directory_by_canonical(b"/project/c").unwrap(), None);
    // Two directories never share a canonical path.
    assert!(write(&mut store, "c", directory("ab")).is_err());
}

#[test]
fn acknowledgement_clears_settled_paths_and_keeps_paths_with_newer_work() {
    let (_directory, mut store) = store();
    let (root, _) = store
        .input_transaction(|transaction| {
            let root = transaction.intern_root("main")?;
            transaction.upsert_file(root, "settled.txt", &file_state(1).into(), InputVersion(1))?;
            transaction.push_dirty(root, "main", "settled.txt", true, InputVersion(1))?;
            transaction.upsert_file(root, "moving.txt", &file_state(1).into(), InputVersion(1))?;
            transaction.push_dirty(root, "main", "moving.txt", true, InputVersion(1))?;
            Ok(root)
        })
        .unwrap();
    let captured = store.pending_file_work().unwrap();
    store
        .input_transaction(|transaction| {
            transaction.upsert_file(root, "moving.txt", &file_state(2).into(), InputVersion(2))?;
            transaction.push_dirty(root, "main", "moving.txt", true, InputVersion(2))
        })
        .unwrap();

    // The newer observation of one path is more work, not a failure: the
    // captured rows clear and the moving path's newer row stays.
    assert!(!store.acknowledge_file_work(&captured).unwrap());
    let remaining = store.pending_file_work().unwrap();
    assert_eq!(remaining.dirty.len(), 1);
    assert_eq!(remaining.dirty[0].path, "moving.txt");
    assert_eq!(remaining.dirty[0].observation, InputVersion(2));

    // Once a pass captures the newest observation, it clears.
    assert!(store.acknowledge_file_work(&remaining).unwrap());
    assert!(store.pending_file_work().unwrap().dirty.is_empty());
}

#[test]
fn work_queued_and_acknowledged_in_one_transaction_is_never_written() {
    let (_directory, mut store) = store();
    store.open_input().unwrap();
    store
        .input_transaction(|transaction| {
            let root = transaction.intern_root("main")?;
            let version = transaction.version();
            transaction.push_dirty(root, "main", "a.bundle", true, version)
        })
        .unwrap();
    let work = store.pending_file_work().unwrap();
    assert_eq!(work.dirty.len(), 1);
    assert!(store.acknowledge_file_work(&work).unwrap());
    // A step that fails fails its input: the work it queued rolls back
    // with the input.
    store
        .input_transaction(|transaction| {
            let root = transaction.intern_root("main")?;
            let version = transaction.version();
            transaction.push_dirty(root, "main", "b.bundle", true, version)?;
            Err::<(), _>(distill_store::StoreError::Rejected {
                detail: "rolled back".to_owned(),
            })
        })
        .unwrap_err();
    store.finish_input(true).unwrap_err();
    assert!(store.committed_file_work().unwrap().is_empty());
    assert!(store.pending_file_work().unwrap().is_empty());

    // Work no pass consumed is written when its transaction commits.
    store
        .input_transaction(|transaction| {
            let root = transaction.intern_root("main")?;
            let version = transaction.version();
            transaction.push_dirty(root, "main", "c.bundle", true, version)
        })
        .unwrap();
    let committed = store.committed_file_work().unwrap();
    assert_eq!(committed.dirty.len(), 1);
    assert_eq!(committed.dirty[0].path, "c.bundle");
}
