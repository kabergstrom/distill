use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use distill_daemon::scanner::{AssetRoot, RootedScanner};
use distill_daemon::watcher::{WatcherAction, WatcherQueue, WatcherThread};
use notify::event::{CreateKind, ModifyKind, RenameMode};
use notify::{Event, EventKind};

fn create(path: impl Into<PathBuf>) -> Event {
    Event::new(EventKind::Create(CreateKind::File)).add_path(path.into())
}

fn wait_for_action(queue: &Mutex<WatcherQueue>) -> WatcherAction {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let action = queue.lock().unwrap().take_live_action();
        if action != WatcherAction::None {
            return action;
        }
        assert!(Instant::now() < deadline, "native watcher event timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn events_during_startup_remain_owned_until_scan_finishes() {
    let mut queue = WatcherQueue::new();
    let path = PathBuf::from("/assets/live.txt");
    queue.arm_scan();
    queue.push_native(create(&path));

    assert_eq!(queue.take_live_action(), WatcherAction::None);
    assert_eq!(
        queue.finish_scan(),
        WatcherAction::Batch(distill_daemon::watcher::WatcherBatch {
            paths: vec![path],
            renames: Vec::new(),
        })
    );
}

#[test]
fn native_rename_pairs_retain_order_and_coalesce_paths() {
    let mut queue = WatcherQueue::new();
    let from = PathBuf::from("/assets/a.txt");
    let to = PathBuf::from("/assets/b.txt");
    queue.push_native(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(from.clone())
            .add_path(to.clone()),
    );
    queue.push_native(create(&to));

    let WatcherAction::Batch(batch) = queue.take_live_action() else {
        panic!("expected precise watcher batch")
    };
    assert_eq!(batch.paths, [from.clone(), to.clone()]);
    assert_eq!(batch.renames.len(), 1);
    assert_eq!(batch.renames[0].from, from);
    assert_eq!(batch.renames[0].to, to);
}

#[test]
fn bounded_event_overflow_discards_partial_paths() {
    let mut queue = WatcherQueue::with_capacity(1);
    queue.push_native(create("/assets/a.txt"));
    queue.push_native(create("/assets/b.txt"));

    assert_eq!(queue.take_live_action(), WatcherAction::FullRescan);
    assert_eq!(queue.take_live_action(), WatcherAction::None);
}

#[test]
fn native_watcher_reports_create_without_scanning() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir(&root).unwrap();
    let scanner = RootedScanner::new([AssetRoot::new(
        "main",
        &root,
        root.join(".distill-displaced"),
    )])
    .unwrap();
    let queue = Arc::new(Mutex::new(WatcherQueue::new()));
    let _watcher = WatcherThread::start(scanner, Arc::clone(&queue)).unwrap();
    let path = root.join("source.txt");

    std::fs::write(&path, b"first").unwrap();
    let WatcherAction::Batch(batch) = wait_for_action(&queue) else {
        panic!("ordinary create must not become a full rescan")
    };
    assert!(batch
        .paths
        .iter()
        .any(|observed| observed.ends_with("source.txt")));
}

#[test]
fn root_replacement_requests_one_catch_up_scan_then_watches_new_root() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    std::fs::create_dir(&first).unwrap();
    std::fs::create_dir(&second).unwrap();
    let scanner = RootedScanner::new([AssetRoot::new(
        "main",
        &first,
        first.join(".distill-displaced"),
    )])
    .unwrap();
    let queue = Arc::new(Mutex::new(WatcherQueue::new()));
    let _watcher = WatcherThread::start(scanner.clone(), Arc::clone(&queue)).unwrap();

    scanner
        .replace_roots([AssetRoot::new(
            "main",
            &second,
            second.join(".distill-displaced"),
        )])
        .unwrap();
    assert_eq!(wait_for_action(&queue), WatcherAction::FullRescan);

    let path = second.join("later.txt");
    std::fs::write(&path, b"later").unwrap();
    let WatcherAction::Batch(batch) = wait_for_action(&queue) else {
        panic!("new root create must be incremental")
    };
    assert!(batch
        .paths
        .iter()
        .any(|observed| observed.ends_with("later.txt")));
}
