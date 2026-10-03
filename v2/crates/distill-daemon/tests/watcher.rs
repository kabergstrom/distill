use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use distill_daemon::scanner::{AssetRoot, RootedScanner};
use distill_daemon::watcher::{WatcherAction, WatcherQueue, WatcherSink, WatcherThread};
use notify::event::{CreateKind, ModifyKind, RenameMode};
use notify::{Event, EventKind};

fn create(path: impl Into<PathBuf>) -> Event {
    Event::new(EventKind::Create(CreateKind::File)).add_path(path.into())
}

fn sink(queue: &Arc<Mutex<WatcherQueue>>) -> WatcherSink {
    let queue = Arc::clone(queue);
    Arc::new(move |event| queue.lock().unwrap().push(event))
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
fn failed_scan_finalization_can_requeue_events_without_leaving_scan_armed() {
    let mut queue = WatcherQueue::new();
    let path = PathBuf::from("/assets/arrived-during-failed-scan.txt");
    queue.arm_scan();
    queue.push_native(create(&path));

    let retained = queue.finish_scan();
    queue.requeue_action(retained);

    assert_eq!(
        queue.take_live_action(),
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
fn split_native_rename_is_retained_across_debounce_boundaries() {
    let mut queue = WatcherQueue::new();
    let from = PathBuf::from("/assets/a.txt");
    let to = PathBuf::from("/assets/b.txt");
    queue.push_native(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
            .add_path(from.clone())
            .set_tracker(17),
    );
    assert_eq!(queue.take_live_action(), WatcherAction::None);
    queue.push_native(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::To)))
            .add_path(to.clone())
            .set_tracker(17),
    );
    let WatcherAction::Batch(batch) = queue.take_live_action() else {
        panic!("expected completed split rename")
    };
    assert_eq!(
        batch.renames,
        [distill_daemon::watcher::WatcherRename { from, to }]
    );
}

#[test]
fn unmatched_rename_destination_requires_recovery_scan() {
    let mut queue = WatcherQueue::new();
    queue.push_native(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::To)))
            .add_path("/assets/b.txt".into())
            .set_tracker(17),
    );
    assert_eq!(queue.take_live_action(), WatcherAction::FullRescan);
}

#[test]
fn requeued_older_rename_stays_before_newer_rename() {
    let mut queue = WatcherQueue::new();
    let older = Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
        .add_path("/assets/a.txt".into())
        .add_path("/assets/b.txt".into());
    queue.push_native(older);
    let failed = queue.take_live_action();
    queue.push_native(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path("/assets/b.txt".into())
            .add_path("/assets/c.txt".into()),
    );
    queue.requeue_action(failed);
    let WatcherAction::Batch(batch) = queue.take_live_action() else {
        panic!("expected retry batch")
    };
    assert_eq!(batch.renames[0].from, PathBuf::from("/assets/a.txt"));
    assert_eq!(batch.renames[1].from, PathBuf::from("/assets/b.txt"));
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
fn failed_watch_coverage_is_terminal_instead_of_becoming_a_scan_loop() {
    let mut queue = WatcherQueue::new();
    queue.push_native(create("/assets/pending.txt"));
    queue.fail("watch installation failed");

    assert_eq!(
        queue.take_live_action(),
        WatcherAction::Failed("watch installation failed".to_owned())
    );
    assert_eq!(queue.take_live_action(), WatcherAction::None);
}

#[test]
fn native_watcher_reports_create_without_scanning() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir(&root).unwrap();
    let scanner = RootedScanner::new([AssetRoot::new("main", &root)])
    .unwrap();
    let queue = Arc::new(Mutex::new(WatcherQueue::new()));
    let _watcher = WatcherThread::start(scanner, [], sink(&queue)).unwrap();
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
    let scanner = RootedScanner::new([AssetRoot::new("main", &first)])
    .unwrap();
    let queue = Arc::new(Mutex::new(WatcherQueue::new()));
    let watcher = WatcherThread::start(scanner.clone(), [], sink(&queue)).unwrap();

    scanner
        .replace_roots([AssetRoot::new("main", &second)])
        .unwrap();
    watcher.replace_roots(&scanner).unwrap();
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

#[test]
fn native_watcher_admits_exact_control_files_but_not_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    let controls = temp.path().join("controls");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&controls).unwrap();
    let control = controls.join("distill.toml");
    std::fs::write(&control, b"initial").unwrap();
    let scanner = RootedScanner::new([AssetRoot::new("main", &root)]).unwrap();
    let queue = Arc::new(Mutex::new(WatcherQueue::new()));
    let _watcher = WatcherThread::start(scanner, [control.clone()], sink(&queue)).unwrap();

    std::fs::write(controls.join("unrelated.txt"), b"noise").unwrap();
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        queue.lock().unwrap().take_live_action(),
        WatcherAction::None
    );

    std::fs::write(&control, b"changed").unwrap();
    let WatcherAction::Batch(batch) = wait_for_action(&queue) else {
        panic!("exact control-file edit must be admitted")
    };
    assert_eq!(batch.paths, [control]);
}

#[test]
fn native_watcher_maps_parent_introduction_to_missing_control_without_a_scan() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir(&root).unwrap();
    let control = temp.path().join("controls/generated/schema.json");
    let scanner = RootedScanner::new([AssetRoot::new("main", &root)])
    .unwrap();
    let queue = Arc::new(Mutex::new(WatcherQueue::new()));
    let _watcher = WatcherThread::start(scanner, [control.clone()], sink(&queue)).unwrap();

    let staging = temp.path().join("staging");
    std::fs::create_dir_all(staging.join("generated")).unwrap();
    std::fs::write(staging.join("generated/schema.json"), b"schema").unwrap();
    std::fs::rename(&staging, temp.path().join("controls")).unwrap();

    let WatcherAction::Batch(batch) = wait_for_action(&queue) else {
        panic!("control-parent introduction must remain a precise invalidation")
    };
    assert_eq!(batch.paths, [control]);
    assert!(batch.renames.is_empty());
}

/// A write staged in `.distill-staging` and renamed onto its target is a
/// change to the target alone: the temp is never queued, and its rename is
/// not a lost rename source.
#[test]
fn a_rename_out_of_the_staging_directory_is_a_change_to_its_target() {
    let mut queue = WatcherQueue::new();
    let temp = PathBuf::from("/assets/.distill-staging/7-1-b.txt");
    let target = PathBuf::from("/assets/b.txt");
    queue.push_native(create(&temp));
    assert_eq!(queue.take_live_action(), WatcherAction::None);
    queue.push_native(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(temp.clone())
            .add_path(target.clone()),
    );
    assert_eq!(
        queue.take_live_action(),
        WatcherAction::Batch(distill_daemon::watcher::WatcherBatch {
            paths: vec![target.clone()],
            renames: Vec::new(),
        })
    );

    // Split rename events: the staged source half is dropped, and the
    // target half arrives as a plain change rather than an unmatched
    // destination that would force a rescan.
    queue.push_native(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
            .add_path(temp)
            .set_tracker(9),
    );
    queue.push_native(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::To)))
            .add_path(target.clone())
            .set_tracker(9),
    );
    assert_eq!(
        queue.take_live_action(),
        WatcherAction::Batch(distill_daemon::watcher::WatcherBatch {
            paths: vec![target],
            renames: Vec::new(),
        })
    );
}
