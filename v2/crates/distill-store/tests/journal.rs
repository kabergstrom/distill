//! §14's daemon-state pieces of swap-verify-or-swap-back publication:
//! the write-intent journal (fsynced before the first rename), the
//! per-filesystem displaced-inode quarantine under intent-ID names, the
//! recovered-edit check, journal-driven temp cleanup, and the retention
//! sweep (§18's `displaced_retention_days`).

use distill_core::id::ContentHash;
use distill_store::journal::{
    CreationRecoveryOutcome, JournalIntentPlan, PublicationGroupKind, RenameAsideOutcome,
    RenameAsideState,
};
use distill_store::{Store, StoreConfig, StoreError};

fn cfg(dir: &tempfile::TempDir) -> StoreConfig {
    StoreConfig::new(dir.path().join(".distill"))
}

fn hash(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

fn record(store: &mut Store, n: u8) -> i64 {
    store
        .record_intent(
            &format!("assets/tex/{n}.bundle"),
            &format!("assets/tex/.{n}.bundle.tmp"),
            &format!("assets/tex/{n}.bundle.conflict"),
            Some(hash(b"pre-image bytes")),
            hash(b"proposed bytes"),
        )
        .unwrap()
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
    store.retire_intent(group.child_intents[0]).unwrap();
    store.retire_publication_group(group.group_id).unwrap();
    assert!(store.unfinished_publication_groups().unwrap().is_empty());
}

#[test]
fn creation_intents_have_no_pre_image() {
    // §14: creation has no target to exchange — no pre-image.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    store
        .record_intent("a.bundle", ".a.tmp", "a.conflict", None, hash(b"new"))
        .unwrap();
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
    // §14: the displaced inode is never unlinked — it moves to
    // .distill/displaced/<content-hash>, recorded in the journal entry
    // before the intent retires.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let id = record(&mut store, 1);

    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let displaced_src = root.join("displaced-bytes.tmp");
    std::fs::write(&displaced_src, b"the users late edit").unwrap();
    let qdir = quarantine_dir(&dir);
    let qpath = store
        .quarantine_displaced(id, &displaced_src, &qdir)
        .unwrap();

    assert_eq!(
        qpath.file_name().unwrap().to_string_lossy(),
        format!("intent-{id}")
    );
    assert!(qpath.starts_with(&qdir));
    assert!(qpath.is_file());
    assert_eq!(std::fs::read(&qpath).unwrap(), b"the users late edit");
    assert!(!displaced_src.exists(), "moved, not copied");

    // Recorded on the intent before it retires.
    let intent = &store.unretired_intents().unwrap()[0];
    assert_eq!(intent.quarantine_paths, [qpath]);

    store.retire_intent(id).unwrap();
    assert!(store.unretired_intents().unwrap().is_empty());
}

#[test]
fn identical_bytes_from_distinct_intents_never_alias_inodes() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let a = record(&mut store, 1);
    let b = record(&mut store, 2);
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let src1 = root.join("one.tmp");
    let src2 = root.join("two.tmp");
    std::fs::write(&src1, b"same displaced bytes").unwrap();
    std::fs::write(&src2, b"same displaced bytes").unwrap();
    let qdir = quarantine_dir(&dir);
    let q1 = store.quarantine_displaced(a, &src1, &qdir).unwrap();
    let q2 = store.quarantine_displaced(b, &src2, &qdir).unwrap();
    assert_ne!(q1, q2, "intent identity preserves two equal-content inodes");
    assert!(!src2.exists());
    assert_eq!(store.quarantined_entries().unwrap().len(), 2);
}

#[test]
fn journal_apis_reject_unknown_intents() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    assert!(matches!(
        store.retire_intent(99),
        Err(StoreError::BadIntent { intent_id: 99, .. })
    ));
    let src = dir.path().join("x.tmp");
    std::fs::write(&src, b"bytes").unwrap();
    assert!(matches!(
        store.quarantine_displaced(99, &src, &quarantine_dir(&dir)),
        Err(StoreError::BadIntent { intent_id: 99, .. })
    ));
}

#[test]
fn temp_cleanup_is_journal_driven() {
    // §14: the daemon deletes only temp files some retired intent names
    // as its own, never "stray" files by pattern.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let a = record(&mut store, 1);
    let _b = record(&mut store, 2); // stays unretired
    store.retire_intent(a).unwrap();

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
    let id = record(&mut store, 1);
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let src = root.join("displaced.tmp");
    std::fs::write(&src, b"original displaced bytes").unwrap();
    let qpath = store
        .quarantine_displaced(id, &src, &quarantine_dir(&dir))
        .unwrap();

    // Untampered: quiet.
    assert!(store.verify_quarantine().unwrap().is_empty());

    // An editor holding an open descriptor writes late bytes into the
    // quarantined inode.
    std::fs::write(&qpath, b"late bytes from an open descriptor").unwrap();
    let diags = store.verify_quarantine().unwrap();
    assert_eq!(diags.len(), 1);
    assert_eq!(diags[0].origin_path, src.to_string_lossy());
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
    let a = record(&mut store, 1);
    let b = record(&mut store, 2);
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let src1 = root.join("old.tmp");
    let src2 = root.join("new.tmp");
    std::fs::write(&src1, b"old displaced").unwrap();
    std::fs::write(&src2, b"new displaced").unwrap();
    let qdir = quarantine_dir(&dir);
    let q_old = store.quarantine_displaced(a, &src1, &qdir).unwrap();
    let q_new = store.quarantine_displaced(b, &src2, &qdir).unwrap();

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
    let intent = record(&mut store, 7);
    let root = dir.path().join("asset-root");
    std::fs::create_dir_all(&root).unwrap();
    let source = root.join("fresh.tmp");
    std::fs::write(&source, b"fresh displacement").unwrap();
    let quarantined = store
        .quarantine_displaced(intent, &source, &quarantine_dir(&dir))
        .unwrap();

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
    let intent = store
        .record_intent(
            target.to_str().unwrap(),
            temp.to_str().unwrap(),
            conflict.to_str().unwrap(),
            Some(hash(b"old manifest")),
            hash(b"new manifest"),
        )
        .unwrap();

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
    let intent = store
        .record_intent(
            target.to_str().unwrap(),
            temp.to_str().unwrap(),
            root.join("manifest.conflict").to_str().unwrap(),
            None,
            hash(b"first manifest"),
        )
        .unwrap();
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
    let intent = store
        .record_intent(
            target.to_str().unwrap(),
            temp.to_str().unwrap(),
            root.join("manifest.conflict").to_str().unwrap(),
            None,
            hash(b"first manifest"),
        )
        .unwrap();

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
    let intent = store
        .record_intent(
            target.to_str().unwrap(),
            temp.to_str().unwrap(),
            root.join("manifest.conflict").to_str().unwrap(),
            Some(hash(b"old manifest")),
            hash(b"new manifest"),
        )
        .unwrap();

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
