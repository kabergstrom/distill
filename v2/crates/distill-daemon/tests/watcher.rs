use std::sync::Mutex;

use distill_daemon::scanner::{AssetRoot, RootedScanner};
use distill_daemon::watcher::{PollingWatchSource, WatcherQueue};

#[test]
fn event_during_scan_stays_dirty_until_the_scan_finishes() {
    let mut queue = WatcherQueue::new();
    queue.arm_scan();
    queue.mark_dirty();

    assert!(!queue.take_live_dirty());
    assert!(queue.finish_scan());
    assert!(!queue.take_live_dirty());
}

#[test]
fn live_events_collapse_to_one_sticky_invalidation() {
    let mut queue = WatcherQueue::new();
    queue.mark_dirty();
    queue.mark_dirty();

    assert!(queue.take_live_dirty());
    assert!(!queue.take_live_dirty());
}

#[test]
fn portable_watch_source_detects_create_update_and_delete() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir(&root).unwrap();
    let scanner = RootedScanner::new([AssetRoot::new(
        "main",
        &root,
        root.join(".distill-displaced"),
    )])
    .unwrap();
    let mut source = PollingWatchSource::arm(scanner).unwrap();
    let queue = Mutex::new(WatcherQueue::new());
    let path = root.join("source.txt");

    std::fs::write(&path, b"first").unwrap();
    assert_eq!(source.poll_once(&queue).unwrap(), 1);
    assert!(queue.lock().unwrap().take_live_dirty());

    std::fs::write(&path, b"second").unwrap();
    assert_eq!(source.poll_once(&queue).unwrap(), 1);
    assert!(queue.lock().unwrap().take_live_dirty());

    std::fs::remove_file(path).unwrap();
    assert_eq!(source.poll_once(&queue).unwrap(), 1);
    assert!(queue.lock().unwrap().take_live_dirty());
}

#[test]
fn published_root_replacement_resets_watcher_basis_without_invalidation() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    std::fs::create_dir(&first).unwrap();
    std::fs::create_dir(&second).unwrap();
    std::fs::write(first.join("old.txt"), b"old").unwrap();
    std::fs::write(second.join("new.txt"), b"new").unwrap();
    let scanner = RootedScanner::new([AssetRoot::new(
        "main",
        &first,
        first.join(".distill-displaced"),
    )])
    .unwrap();
    let mut source = PollingWatchSource::arm(scanner.clone()).unwrap();
    let queue = Mutex::new(WatcherQueue::new());

    scanner
        .replace_roots([AssetRoot::new(
            "main",
            &second,
            second.join(".distill-displaced"),
        )])
        .unwrap();
    assert_eq!(source.poll_once(&queue).unwrap(), 0);
    assert!(!queue.lock().unwrap().take_live_dirty());

    std::fs::write(second.join("later.txt"), b"later").unwrap();
    assert_eq!(source.poll_once(&queue).unwrap(), 1);
    assert!(queue.lock().unwrap().take_live_dirty());
}
