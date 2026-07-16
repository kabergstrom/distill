//! §14's daemon-state pieces of journaled rename-aside publication:
//! the write-intent journal (fsynced before the first rename), the
//! per-filesystem displaced-inode quarantine under intent-ID names, the
//! recovered-edit check, journal-driven temp cleanup, and the retention
//! sweep (§18's `displaced_retention_days`).

use distill_core::id::ContentHash;
use distill_store::journal::{
    CreationRecoveryOutcome, JournalIntentPlan, PublicationGroup, PublicationGroupKind,
    RenameAsideOutcome, RenameAsideState,
};
use distill_store::{Store, StoreConfig, StoreError};

fn cfg(dir: &tempfile::TempDir) -> StoreConfig {
    StoreConfig::new(dir.path().join(".distill"))
}

fn hash(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

fn record(store: &mut Store, n: u8) -> i64 {
    let group = record_group(
        store,
        JournalIntentPlan {
            target_path: format!("assets/tex/{n}.bundle"),
            temp_path: format!("assets/tex/.{n}.bundle.tmp"),
            conflict_path: format!("assets/tex/{n}.bundle.conflict"),
            pre_image_hash: Some(hash(b"pre-image bytes")),
            proposed_hash: hash(b"proposed bytes"),
        },
        true,
    );
    group.child_intents[0]
}

fn record_group(store: &mut Store, plan: JournalIntentPlan, armed: bool) -> PublicationGroup {
    let group = store
        .record_publication_group(PublicationGroupKind::AuthoringWrite, b"test basis", &[plan])
        .unwrap();
    if armed {
        store.arm_publication_group(group.group_id).unwrap();
    }
    group
}

fn record_paths(
    store: &mut Store,
    target: &std::path::Path,
    temp: &std::path::Path,
    conflict: &std::path::Path,
    pre_image_hash: Option<ContentHash>,
    proposed_hash: ContentHash,
) -> PublicationGroup {
    record_group(
        store,
        JournalIntentPlan {
            target_path: target.to_string_lossy().into_owned(),
            temp_path: temp.to_string_lossy().into_owned(),
            conflict_path: conflict.to_string_lossy().into_owned(),
            pre_image_hash,
            proposed_hash,
        },
        true,
    )
}

fn publish_replacement(
    store: &mut Store,
    dir: &tempfile::TempDir,
    name: &str,
    old: &[u8],
    new: &[u8],
) -> (i64, std::path::PathBuf, std::path::PathBuf) {
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join(format!("{name}.bundle"));
    let temp = root.join(format!(".{name}.proposed"));
    let conflict = root.join(format!("{name}.conflict"));
    std::fs::write(&target, old).unwrap();
    std::fs::write(&temp, new).unwrap();
    let group = record_paths(store, &target, &temp, &conflict, Some(hash(old)), hash(new));
    let intent = group.child_intents[0];
    assert_eq!(
        store
            .publish_journaled_replacement(intent, &quarantine_dir(dir))
            .unwrap(),
        RenameAsideOutcome::Installed
    );
    (
        intent,
        quarantine_dir(dir).join(format!("intent-{intent}")),
        target,
    )
}

fn quarantine_dir(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("asset-root/.distill-quarantine")
}

#[test]
fn intents_persist_with_their_full_shape() {
    // §14: the target path, the temp path, the conflict path that would
    // be used, the expected pre-image hash, and the proposed content
    // hash — written to daemon state and fsynced before the first
    // rename.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let id = record(&mut store, 1);

    let intents = store.unretired_intents().unwrap();
    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(intent.intent_id, id);
    assert_eq!(intent.target_path, "assets/tex/1.bundle");
    assert_eq!(intent.temp_path, "assets/tex/.1.bundle.tmp");
    assert_eq!(intent.conflict_path, "assets/tex/1.bundle.conflict");
    assert_eq!(intent.pre_image_hash, Some(hash(b"pre-image bytes")));
    assert_eq!(intent.proposed_hash, hash(b"proposed bytes"));
    assert!(intent.quarantine_paths.is_empty());
    assert_eq!(intent.rename_aside_state, RenameAsideState::Prepared);
    assert_eq!(intent.terminal_success, None);
    assert!(!intent.retired);
}

#[test]
fn multi_path_parent_and_every_child_are_recorded_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let plans = [
        JournalIntentPlan {
            target_path: "root/a.bundle".into(),
            temp_path: "root/.a.proposed".into(),
            conflict_path: "root/a.conflict".into(),
            pre_image_hash: Some(hash(b"a")),
            proposed_hash: hash(b"new-a"),
        },
        JournalIntentPlan {
            target_path: "root/b.bundle".into(),
            temp_path: String::new(),
            conflict_path: "root/b.conflict".into(),
            pre_image_hash: Some(hash(b"b")),
            proposed_hash: hash(b""),
        },
    ];

    let group = store
        .record_publication_group(
            PublicationGroupKind::LineageDuplicate,
            b"complete canonical claimant basis",
            &plans,
        )
        .unwrap();
    drop(store);

    let store = Store::open(cfg(&dir)).unwrap();
    let recovered = store.unfinished_publication_groups().unwrap();
    assert_eq!(recovered, vec![group.clone()]);
    let intents = store.unretired_intents().unwrap();
    assert_eq!(
        intents
            .iter()
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>(),
        group.child_intents
    );
    assert_eq!(intents[0].pre_image_hash, plans[0].pre_image_hash);
    assert_eq!(intents[1].temp_path, "");
}

#[test]
fn parent_cannot_retire_until_every_child_is_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let group = store
        .record_publication_group(
            PublicationGroupKind::LineageCreate,
            b"missing destination basis",
            &[JournalIntentPlan {
                target_path: "root/lineage.bundle".into(),
                temp_path: "root/.lineage.proposed".into(),
                conflict_path: "root/lineage.conflict".into(),
                pre_image_hash: None,
                proposed_hash: hash(b"manifest"),
            }],
        )
        .unwrap();

    assert!(matches!(
        store.retire_publication_group(group.group_id),
        Err(StoreError::BadIntent { .. })
    ));
    store.arm_publication_group(group.group_id).unwrap();
    assert_eq!(
        store
            .reconcile_journaled_creation(group.child_intents[0])
            .unwrap(),
        CreationRecoveryOutcome::RetryRequired
    );
    store.retire_publication_group(group.group_id).unwrap();
    assert!(store.unfinished_publication_groups().unwrap().is_empty());
}

#[test]
fn creation_intents_have_no_pre_image() {
    // §14: creation has no target to move aside — no pre-image.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    record_group(
        &mut store,
        JournalIntentPlan {
            target_path: "a.bundle".into(),
            temp_path: ".a.tmp".into(),
            conflict_path: "a.conflict".into(),
            pre_image_hash: None,
            proposed_hash: hash(b"new"),
        },
        false,
    );
    assert_eq!(store.unretired_intents().unwrap()[0].pre_image_hash, None);
}

#[test]
fn unfinished_intents_survive_restart_for_crash_reconciliation() {
    // §14: crash recovery reconciles every unfinished intent at startup.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    record(&mut store, 1);
    drop(store);
    let store = Store::open(cfg(&dir)).unwrap();
    assert_eq!(store.unretired_intents().unwrap().len(), 1);
}

#[test]
fn quarantine_moves_the_displaced_inode_under_an_intent_id_on_its_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (id, qpath, _target) =
        publish_replacement(&mut store, &dir, "one", b"the users late edit", b"new");

    assert_eq!(
        qpath.file_name().unwrap().to_string_lossy(),
        format!("intent-{id}")
    );
    assert!(qpath.starts_with(quarantine_dir(&dir)));
    assert!(qpath.is_file());
    assert_eq!(std::fs::read(&qpath).unwrap(), b"the users late edit");
    assert!(store.unretired_intents().unwrap().is_empty());
}

#[test]
fn identical_bytes_from_distinct_intents_never_alias_inodes() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (_a, q1, _) =
        publish_replacement(&mut store, &dir, "one", b"same displaced bytes", b"new one");
    let (_b, q2, _) =
        publish_replacement(&mut store, &dir, "two", b"same displaced bytes", b"new two");
    assert_ne!(q1, q2, "intent identity preserves two equal-content inodes");
    assert_eq!(store.quarantined_entries().unwrap().len(), 2);
}

#[test]
fn journal_apis_reject_unknown_intents() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    assert!(matches!(
        store.publish_journaled_replacement(99, &quarantine_dir(&dir)),
        Err(StoreError::BadIntent { intent_id: 99, .. })
    ));
}

#[test]
fn temp_cleanup_is_journal_driven() {
    // §14: the daemon deletes only temp files some retired intent names
    // as its own, never "stray" files by pattern.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let a = record_group(
        &mut store,
        JournalIntentPlan {
            target_path: "assets/tex/1.bundle".into(),
            temp_path: "assets/tex/.1.bundle.tmp".into(),
            conflict_path: "assets/tex/1.bundle.conflict".into(),
            pre_image_hash: Some(hash(b"pre-image bytes")),
            proposed_hash: hash(b"proposed bytes"),
        },
        false,
    );
    let _b = record(&mut store, 2); // stays unretired
    store.abort_unarmed_publication_group(a.group_id).unwrap();

    assert_eq!(
        store.journal_owned_temp_paths().unwrap(),
        vec!["assets/tex/.1.bundle.tmp".to_owned()],
        "only the retired intent's temp path is deletable"
    );
}

#[test]
fn startup_verification_surfaces_recovered_edits() {
    // §14: startup reconciliation re-hashes every quarantined file and
    // surfaces any that no longer matches its journal entry as a
    // recovered-edit diagnostic naming the origin path — the in-place
    // writer's late bytes landed in the quarantined inode and survived.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (_id, qpath, target) = publish_replacement(
        &mut store,
        &dir,
        "verify",
        b"original displaced bytes",
        b"new bytes",
    );

    // Untampered: quiet.
    assert!(store.verify_quarantine().unwrap().is_empty());

    // An editor holding an open descriptor writes late bytes into the
    // quarantined inode.
    std::fs::write(&qpath, b"late bytes from an open descriptor").unwrap();
    let diags = store.verify_quarantine().unwrap();
    assert_eq!(diags.len(), 1);
    assert_eq!(diags[0].origin_path, target.to_string_lossy());
    assert_eq!(diags[0].expected, hash(b"original displaced bytes"));
    assert_eq!(
        diags[0].actual,
        Some(hash(b"late bytes from an open descriptor"))
    );

    // A missing quarantined file is also a diagnostic, never silent.
    std::fs::remove_file(&qpath).unwrap();
    let diags = store.verify_quarantine().unwrap();
    assert_eq!(diags.len(), 1);
    assert_eq!(diags[0].actual, None);
}

#[test]
fn the_retention_sweep_removes_only_expired_entries() {
    // §18: displaced_retention_days — removal is retention expiry or
    // explicit `doctor clean`.
    let dir = tempfile::tempdir().unwrap();
    let mut config = cfg(&dir);
    config.displaced_retention_days = 7;
    let mut store = Store::open(config).unwrap();
    let (a, q_old, _) = publish_replacement(&mut store, &dir, "old", b"old displaced", b"old new");
    let (_b, q_new, _) = publish_replacement(&mut store, &dir, "new", b"new displaced", b"new new");

    // Backdate the first entry beyond the window (directly in the DB —
    // the sweep trusts quarantined_at).
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let conn = rusqlite::Connection::open(dir.path().join(".distill/meta.sqlite")).unwrap();
    conn.execute(
        "UPDATE displaced SET quarantined_at = ?1 WHERE intent_id = ?2",
        rusqlite::params![now - 8 * 86_400, a],
    )
    .unwrap();
    drop(conn);

    let removed = store.sweep_displaced(now).unwrap();
    assert_eq!(removed, 1);
    assert!(!q_old.exists(), "expired entry removed");
    assert!(q_new.exists(), "fresh entry retained");
    assert_eq!(store.quarantined_entries().unwrap().len(), 1);

    // Destruction is itself retained as journal history, not erased.
    let history = store.displacement_history().unwrap();
    let old = history.iter().find(|e| e.intent_id == a).unwrap();
    assert_eq!(old.cleanup_reason.as_deref(), Some("retention-expired"));
}

#[test]
fn doctor_clean_removes_fresh_entries_but_keeps_named_audit_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (_intent, quarantined, _) = publish_replacement(
        &mut store,
        &dir,
        "fresh",
        b"fresh displacement",
        b"new bytes",
    );

    assert_eq!(store.clean_all_displaced(1234).unwrap(), 1);
    assert!(!quarantined.exists());
    assert!(store.quarantined_entries().unwrap().is_empty());
    let history = store.displacement_history().unwrap();
    assert_eq!(history[0].cleaned_at, Some(1234));
    assert_eq!(history[0].cleanup_reason.as_deref(), Some("doctor-clean"));
}

#[test]
fn native_journaled_replacement_installs_no_replace_and_retains_the_preimage() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("manifest.bundle");
    let temp = root.join(".manifest.proposed");
    let conflict = root.join("manifest.conflict");
    std::fs::write(&target, b"old manifest").unwrap();
    std::fs::write(&temp, b"new manifest").unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let group = record_paths(
        &mut store,
        &target,
        &temp,
        &conflict,
        Some(hash(b"old manifest")),
        hash(b"new manifest"),
    );
    let intent = group.child_intents[0];

    assert_eq!(
        store
            .publish_journaled_replacement(intent, &quarantine_dir(&dir))
            .unwrap(),
        RenameAsideOutcome::Installed
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"new manifest");
    assert!(!temp.exists());
    assert_eq!(
        std::fs::read(quarantine_dir(&dir).join(format!("intent-{intent}"))).unwrap(),
        b"old manifest"
    );
    assert!(store.unretired_intents().unwrap().is_empty());
}

#[test]
fn native_creation_recovery_is_no_replace_and_restart_resumable() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("manifest.bundle");
    let temp = root.join(".manifest.proposed");
    std::fs::write(&temp, b"first manifest").unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let group = record_paths(
        &mut store,
        &target,
        &temp,
        &root.join("manifest.conflict"),
        None,
        hash(b"first manifest"),
    );
    let intent = group.child_intents[0];
    drop(store);

    let mut store = Store::open(cfg(&dir)).unwrap();
    assert_eq!(
        store.reconcile_journaled_creation(intent).unwrap(),
        CreationRecoveryOutcome::Installed
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"first manifest");
    assert!(!temp.exists());
    assert!(store.unretired_intents().unwrap().is_empty());
}

#[test]
fn prepared_creation_without_a_temp_is_safely_abandoned() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("manifest.bundle");
    let temp = root.join(".manifest.proposed");
    let mut store = Store::open(cfg(&dir)).unwrap();
    let group = record_paths(
        &mut store,
        &target,
        &temp,
        &root.join("manifest.conflict"),
        None,
        hash(b"first manifest"),
    );
    let intent = group.child_intents[0];

    assert_eq!(
        store.reconcile_journaled_creation(intent).unwrap(),
        CreationRecoveryOutcome::RetryRequired
    );
    assert!(!target.exists());
    assert!(!temp.exists());
    assert!(store.unretired_intents().unwrap().is_empty());
}

#[test]
fn prepared_replacement_without_a_temp_leaves_the_target_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("manifest.bundle");
    let temp = root.join(".manifest.proposed");
    std::fs::write(&target, b"old manifest").unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let group = record_paths(
        &mut store,
        &target,
        &temp,
        &root.join("manifest.conflict"),
        Some(hash(b"old manifest")),
        hash(b"new manifest"),
    );
    let intent = group.child_intents[0];

    assert_eq!(
        store
            .publish_journaled_replacement(intent, &quarantine_dir(&dir))
            .unwrap(),
        RenameAsideOutcome::RetryRequired
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"old manifest");
    assert!(!temp.exists());
    assert!(!quarantine_dir(&dir).exists());
    assert!(store.unretired_intents().unwrap().is_empty());
}
