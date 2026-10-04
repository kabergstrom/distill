//! Writers on connections of their own, one per thread (LOCKLESS.md §3):
//! SQLite's write lock orders them.

use std::sync::{Arc, Barrier};

use distill_store::served::{Change, ServedWrite};
use distill_store::state::InputVersion;
use distill_store::{Store, StoreConfig, StoreOpener};

const THREADS: usize = 8;
const EACH: usize = 25;

#[test]
fn concurrent_writers_lose_no_update_and_log_in_commit_order() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
    let start = Arc::new(Barrier::new(THREADS));
    let threads = (0..THREADS)
        .map(|thread| {
            let mut writer = store.open_writer().unwrap();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..EACH {
                    // Each input reads the version it advances inside its
                    // own transaction: a lost update would publish a
                    // version twice.
                    writer
                        .input_transaction(|txn| {
                            let version = txn.version();
                            txn.append_change(
                                version,
                                &Change::Path {
                                    path: format!("{thread}/{}", version.0),
                                },
                            )
                        })
                        .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap();
    }

    let total = (THREADS * EACH) as u64;
    assert_eq!(store.input_version().unwrap(), InputVersion(total));
    let log = store.change_log_after(0).unwrap();
    assert_eq!(log.len(), THREADS * EACH);
    // Sequence order is commit order: the n-th row belongs to version n.
    for (index, entry) in log.iter().enumerate() {
        assert_eq!(entry.version, InputVersion(index as u64 + 1));
        let Change::Path { path } = &entry.change else {
            panic!("unexpected change {:?}", entry.change);
        };
        assert!(path.ends_with(&format!("/{}", index + 1)));
    }
}

#[test]
fn an_open_input_is_seen_through_its_writer_and_by_no_other_connection() {
    let dir = tempfile::tempdir().unwrap();
    let (opener, store) =
        StoreOpener::new(Store::open(StoreConfig::new(dir.path().join("state"))).unwrap());
    let mut writer = opener.open_writer().unwrap();
    writer.arm_input();
    writer
        .input_transaction(|txn| txn.intern_root("main"))
        .unwrap();
    // The owner reads its open input through its writer.
    assert_eq!(writer.input_version().unwrap(), InputVersion(1));
    assert!(writer.root_id("main").unwrap().is_some());
    // Another thread's reader, and another writer, see none of it.
    let other = {
        let opener = Arc::clone(&opener);
        std::thread::spawn(move || {
            let read = opener.open_reader().unwrap();
            (read.input_version().unwrap(), read.root_id("main").unwrap())
        })
        .join()
        .unwrap()
    };
    assert_eq!(other, (InputVersion(0), None));
    assert_eq!(store.input_version().unwrap(), InputVersion(0));

    assert_eq!(writer.finish_input(true).unwrap(), InputVersion(1));
    let other = {
        let opener = Arc::clone(&opener);
        std::thread::spawn(move || opener.open_reader().unwrap().input_version().unwrap())
            .join()
            .unwrap()
    };
    assert_eq!(other, InputVersion(1));
}

#[test]
fn writers_follow_the_operational_configuration_from_their_next_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let (opener, mut store) =
        StoreOpener::new(Store::open(StoreConfig::new(dir.path().join("state"))).unwrap());
    let mut writer = opener.open_writer().unwrap();
    let before = writer.config().segment_size;
    let mut candidate = StoreConfig::clone(&opener.config());
    candidate.segment_size = before * 2;
    opener.apply_operational_config(&candidate).unwrap();
    // Nothing changes under a writer until it begins a transaction.
    assert_eq!(writer.config().segment_size, before);
    writer.write_transaction(|_| Ok(())).unwrap();
    assert_eq!(writer.config().segment_size, before * 2);
    store.write_transaction(|_| Ok(())).unwrap();
    assert_eq!(store.config().segment_size, before * 2);
    // A writer opened afterwards starts from it.
    assert_eq!(opener.open_writer().unwrap().config().segment_size, before * 2);
}

/// A write transaction nested in an open input is a savepoint: when it
/// fails, its own writes roll back and the input keeps the rest.
#[test]
fn a_failed_nested_write_rolls_back_only_its_own_writes() {
    use distill_store::files::{FileKind, FileObservation, FileState};
    use distill_store::StoreError;
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
    let observe = |store: &mut Store, path: &str| {
        let file = FileObservation::from(FileState {
            mtime: 0,
            size: 0,
            kind: FileKind::File,
            content_hash: None,
        });
        store
            .input_transaction(|txn| {
                let root = txn.intern_root("main")?;
                txn.upsert_file(root, path, &file, txn.version())
            })
            .map(drop)
    };
    store.open_input().unwrap();
    store
        .write_transaction(|store| {
            observe(store, "kept")
        })
        .unwrap();
    let failed = store.write_transaction(|store| {
        observe(store, "dropped")?;
        Err::<(), _>(StoreError::Rejected {
            detail: "the step failed".to_owned(),
        })
    });
    assert!(failed.is_err());
    let failed_with = store.write_transaction_with(
        |error| error.to_string(),
        |store| {
            observe(store, "dropped too").map_err(|error| error.to_string())?;
            Err::<(), _>("the step failed".to_owned())
        },
    );
    assert!(failed_with.is_err());
    store.finish_input(true).unwrap();
    let paths = store
        .observed_files()
        .unwrap()
        .into_iter()
        .map(|row| row.path)
        .collect::<Vec<_>>();
    assert_eq!(paths, ["kept"]);
}

/// An inline (tag-refinement) build's flush nested in an open input: when it fails
/// after its node row, the input that commits keeps none of the node, and
/// its rolled-back segment row is no OPEN segment left behind.
#[test]
fn a_failed_inline_build_flush_commits_no_partial_node() {
    use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
    use distill_store::cas::record::KeyKind;
    use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec};
    use distill_store::StoreError;
    use distill_wire::artifact::{write_artifact, ArtifactHeader};
    use distill_wire::dswl::{dswl_bytes, dswl_hash};
    use distill_wire::wire::WireNode;

    let node_commit = |key: u8| {
        let node = WireNode::Unit { offset: 0 };
        let artifact = write_artifact(
            &ArtifactHeader {
                asset_uuid: AssetUuid([key; 16]),
                authored_type: TypeUuid([1; 16]),
                terminal_type: TypeUuid([2; 16]),
                encoded_type: TypeUuid([3; 16]),
                logical_hash: LogicalHash([4; 32]),
                layout_hash: dswl_hash(&node).unwrap(),
            },
            &[AssetUuid([9; 16])],
            &[],
            &[],
            &[],
        )
        .unwrap();
        BuildCommit {
            wire_trees: vec![dswl_bytes(&node).unwrap()],
            key_kind: KeyKind::Node,
            static_input_key: [key; 32],
            asset_uuid: AssetUuid([key; 16]),
            trace: vec![key],
            outcome: CommitOutcome::Success {
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: artifact,
                }],
                aux: vec![],
            },
        }
    };
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
    store.open_input().unwrap();
    let flush = store.write_transaction(|store| {
        store.commit_build(node_commit(3))?;
        Err::<(), _>(StoreError::Rejected {
            detail: "a later write of the node failed".to_owned(),
        })
    });
    assert!(flush.is_err());
    store.finish_input(true).unwrap();
    assert!(store.candidate_rows(KeyKind::Node, &[3; 32]).unwrap().is_empty());

    store
        .write_transaction(|store| store.commit_build(node_commit(4)).map(drop))
        .unwrap();
    let conn = rusqlite::Connection::open(dir.path().join("state/meta.sqlite")).unwrap();
    let open: i64 = conn
        .query_row("SELECT COUNT(*) FROM cas_segments WHERE state = 0", [], |row| row.get(0))
        .unwrap();
    assert_eq!(open, 1, "only the live writer's segment is OPEN");
}
