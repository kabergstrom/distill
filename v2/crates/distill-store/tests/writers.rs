//! Writers on connections of their own, one per thread (LOCKLESS.md §3):
//! SQLite's write lock orders them.

use std::sync::{Arc, Barrier};

use distill_store::served::{Change, ServedWrite};
use distill_store::state::InputVersion;
use distill_store::{SharedStore, Store, StoreConfig};

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
fn a_thread_reads_its_open_input_and_no_other_thread_does() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SharedStore::new(
        Store::open(StoreConfig::new(dir.path().join("state"))).unwrap(),
    ));
    store.write().arm_input();
    store
        .write()
        .input_transaction(|txn| txn.set_clean_watermark(7))
        .unwrap();
    // The writer stays with this thread while its input is open, and reads
    // here go through it.
    assert_eq!(store.read().input_version(), InputVersion(1));
    assert_eq!(store.read().clean_watermark().unwrap(), Some(7));
    let other = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || {
            let read = store.read();
            (read.input_version(), read.clean_watermark().unwrap())
        })
        .join()
        .unwrap()
    };
    assert_eq!(other, (InputVersion(0), None));

    assert_eq!(store.write().finish_input(true).unwrap(), InputVersion(1));
    let other = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || store.read().input_version())
            .join()
            .unwrap()
    };
    assert_eq!(other, InputVersion(1));
}
