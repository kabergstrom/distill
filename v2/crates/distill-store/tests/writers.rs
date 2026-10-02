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
    assert_eq!(store.input_version(), InputVersion(total));
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
        .input_transaction(|txn| txn.set_clean_watermark(7))
        .unwrap();
    // The owner reads its open input through its writer.
    assert_eq!(writer.input_version(), InputVersion(1));
    assert_eq!(writer.clean_watermark().unwrap(), Some(7));
    // Another thread's reader, and another writer, see none of it.
    let other = {
        let opener = Arc::clone(&opener);
        std::thread::spawn(move || {
            let read = opener.open_reader().unwrap();
            (read.input_version(), read.clean_watermark().unwrap())
        })
        .join()
        .unwrap()
    };
    assert_eq!(other, (InputVersion(0), None));
    assert_eq!(store.input_version(), InputVersion(0));

    assert_eq!(writer.finish_input(true).unwrap(), InputVersion(1));
    let other = {
        let opener = Arc::clone(&opener);
        std::thread::spawn(move || opener.open_reader().unwrap().input_version())
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
