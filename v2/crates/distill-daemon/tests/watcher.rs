use distill_daemon::coordinator::WatcherPathEvent;
use std::sync::Mutex;

use distill_daemon::scanner::{AssetRoot, RootedScanner};
use distill_daemon::watcher::{GenerationReplay, PollingWatchSource, WatcherQueue};

fn event(path: &str) -> WatcherPathEvent {
    WatcherPathEvent {
        root: "main".to_owned(),
        path: path.to_owned(),
        exists: true,
    }
}

#[test]
fn scan_generation_replays_the_sorted_union_of_events_arriving_during_scan() {
    let mut queue = WatcherQueue::new(16);
    let generation = queue.arm_scan().unwrap();
    queue.push(event("z.bundle")).unwrap();
    queue.push(event("a.bundle")).unwrap();
    queue.push(event("z.bundle")).unwrap();

    assert_eq!(
        queue.finish_scan(generation).unwrap(),
        GenerationReplay::Events(vec![event("a.bundle"), event("z.bundle")])
    );
    assert!(queue.take_live_batch().is_empty());
}

#[test]
fn overflow_during_scan_requires_a_full_rescan_and_discards_partial_events() {
    let mut queue = WatcherQueue::new(2);
    let generation = queue.arm_scan().unwrap();
    queue.push(event("a")).unwrap();
    queue.push(event("b")).unwrap();
    queue.push(event("c")).unwrap();

    assert_eq!(
        queue.finish_scan(generation).unwrap(),
        GenerationReplay::FullRescan
    );
    assert!(queue.take_live_batch().is_empty());
}

#[test]
fn stale_generation_cannot_finish_or_consume_a_new_scan() {
    let mut queue = WatcherQueue::new(8);
    let first = queue.arm_scan().unwrap();
    queue.finish_scan(first).unwrap();
    let second = queue.arm_scan().unwrap();
    assert_ne!(first, second);
    assert!(queue.finish_scan(first).is_err());
    queue.push(event("fresh")).unwrap();
    assert_eq!(
        queue.finish_scan(second).unwrap(),
        GenerationReplay::Events(vec![event("fresh")])
    );
}

#[test]
fn live_batches_collapse_atomic_save_chains_to_the_last_path_state() {
    let mut queue = WatcherQueue::new(8);
    queue
        .push(WatcherPathEvent {
            root: "main".to_owned(),
            path: "asset.bundle".to_owned(),
            exists: false,
        })
        .unwrap();
    queue.push(event("asset.bundle")).unwrap();
    queue.push(event("other.bundle")).unwrap();

    assert_eq!(
        queue.take_live_batch(),
        vec![event("asset.bundle"), event("other.bundle")]
    );
}

#[test]
fn portable_watch_source_detects_create_update_and_delete_from_the_rooted_scanner() {
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
    let queue = Mutex::new(WatcherQueue::new(16));
    let path = root.join("source.txt");

    std::fs::write(&path, b"first").unwrap();
    assert_eq!(source.poll_once(&queue).unwrap(), 1);
    assert_eq!(
        queue.lock().unwrap().take_live_batch(),
        vec![event("source.txt")]
    );

    std::fs::write(&path, b"second").unwrap();
    assert_eq!(source.poll_once(&queue).unwrap(), 1);
    assert_eq!(
        queue.lock().unwrap().take_live_batch(),
        vec![event("source.txt")]
    );

    std::fs::remove_file(path).unwrap();
    assert_eq!(source.poll_once(&queue).unwrap(), 1);
    assert_eq!(
        queue.lock().unwrap().take_live_batch(),
        vec![WatcherPathEvent {
            root: "main".to_owned(),
            path: "source.txt".to_owned(),
            exists: false,
        }]
    );
}
