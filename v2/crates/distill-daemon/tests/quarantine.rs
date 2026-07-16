use std::collections::BTreeMap;

use distill_core::id::ContentHash;
use distill_daemon::quarantine::{
    QuarantineDriver, QuarantineRoot, RecoveredPublication, RecoveryOutcome,
};
use distill_store::codegen::CodegenPublicationBasis;
use distill_store::journal::{
    CreationRecoveryOutcome, DeletionRecoveryOutcome, JournalIntentPlan, PublicationGroupKind,
    RenameAsideOutcome,
};
use distill_store::{Store, StoreConfig};

fn publish_delete(
    driver: &QuarantineDriver,
    store: &mut Store,
    target: &std::path::Path,
    expected: ContentHash,
) -> (DeletionRecoveryOutcome, i64) {
    let mut publication = driver.admit_publication(store).unwrap();
    let group = publication
        .record_group(
            PublicationGroupKind::AuthoringWrite,
            b"test deletion",
            &[JournalIntentPlan {
                target_path: target.to_string_lossy().into_owned(),
                temp_path: String::new(),
                conflict_path: target
                    .with_extension("conflict")
                    .to_string_lossy()
                    .into_owned(),
                pre_image_hash: Some(expected),
                proposed_hash: ContentHash(*blake3::hash(b"").as_bytes()),
            }],
        )
        .unwrap();
    publication.arm_group(group.group_id).unwrap();
    let outcome = publication
        .resume_group_delete(group.child_intents[0], target)
        .unwrap();
    publication.retire_group(group.group_id).unwrap();
    (outcome, group.child_intents[0])
}

#[test]
fn deletion_uses_intent_named_same_filesystem_quarantine_and_verifies_preimage() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let watched = temp.path().join("assets");
    std::fs::create_dir(&watched).unwrap();
    let quarantine = watched.join(".distill-displaced");
    let target = watched.join("obsolete.asset");
    std::fs::write(&target, b"expected bytes").unwrap();
    let expected = ContentHash(*blake3::hash(b"expected bytes").as_bytes());
    let mut store = Store::open(StoreConfig::new(&state)).unwrap();
    let driver = QuarantineDriver::new([QuarantineRoot::new(&watched, &quarantine)]).unwrap();

    let (outcome, intent) = publish_delete(&driver, &mut store, &target, expected);
    assert_eq!(outcome, DeletionRecoveryOutcome::Deleted);
    let quarantined = store
        .quarantined_entries()
        .unwrap()
        .into_iter()
        .find(|entry| entry.intent_id == intent)
        .unwrap()
        .path;
    assert!(!target.exists());
    assert_eq!(quarantined.parent(), Some(quarantine.as_path()));
    assert!(quarantined
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("intent-"));
    assert!(store.verify_quarantine().unwrap().is_empty());
}

#[test]
fn preimage_mismatch_restores_file_and_reports_conflict() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let watched = temp.path().join("assets");
    std::fs::create_dir(&watched).unwrap();
    let quarantine = watched.join(".distill-displaced");
    let target = watched.join("edited.asset");
    std::fs::write(&target, b"user edit").unwrap();
    let mut store = Store::open(StoreConfig::new(&state)).unwrap();
    let driver = QuarantineDriver::new([QuarantineRoot::new(&watched, &quarantine)]).unwrap();

    let (outcome, _) = publish_delete(&driver, &mut store, &target, ContentHash([7; 32]));
    assert_eq!(outcome, DeletionRecoveryOutcome::ConflictRestored);
    assert_eq!(std::fs::read(&target).unwrap(), b"user edit");
}

#[test]
fn doctor_clean_runs_the_store_retention_sweep() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let watched = temp.path().join("assets");
    std::fs::create_dir(&watched).unwrap();
    let quarantine = watched.join(".distill-displaced");
    let target = watched.join("old.asset");
    std::fs::write(&target, b"old").unwrap();
    let mut config = StoreConfig::new(&state);
    config.displaced_retention_days = 0;
    let mut store = Store::open(config).unwrap();
    let driver = QuarantineDriver::new([QuarantineRoot::new(&watched, &quarantine)]).unwrap();
    let hash = ContentHash(*blake3::hash(b"old").as_bytes());
    let (outcome, intent) = publish_delete(&driver, &mut store, &target, hash);
    assert_eq!(outcome, DeletionRecoveryOutcome::Deleted);
    let path = store
        .quarantined_entries()
        .unwrap()
        .into_iter()
        .find(|entry| entry.intent_id == intent)
        .unwrap()
        .path;

    assert_eq!(driver.doctor_clean(&mut store, i64::MAX).unwrap(), 1);
    assert!(!path.exists());
}

#[test]
fn publication_admission_recovers_every_prior_creation_before_returning() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let watched = temp.path().join("assets");
    std::fs::create_dir(&watched).unwrap();
    let quarantine = watched.join(".distill-displaced");
    let target = watched.join("lineage.bundle");
    let proposed = watched.join(".lineage.proposed");
    std::fs::write(&proposed, b"manifest").unwrap();
    let proposed_hash = ContentHash(*blake3::hash(b"manifest").as_bytes());
    let mut store = Store::open(StoreConfig::new(&state)).unwrap();
    let group = store
        .record_publication_group(
            PublicationGroupKind::LineageCreate,
            b"interrupted creation",
            &[JournalIntentPlan {
                target_path: target.to_string_lossy().into_owned(),
                temp_path: proposed.to_string_lossy().into_owned(),
                conflict_path: watched
                    .join("lineage.conflict")
                    .to_string_lossy()
                    .into_owned(),
                pre_image_hash: None,
                proposed_hash,
            }],
        )
        .unwrap();
    store.arm_publication_group(group.group_id).unwrap();
    let intent = group.child_intents[0];
    let driver = QuarantineDriver::new([QuarantineRoot::new(&watched, &quarantine)]).unwrap();

    let publication = driver.admit_publication(&mut store).unwrap();

    assert_eq!(
        publication.recovered(),
        &[RecoveredPublication {
            intent_id: intent,
            group_kind: Some(PublicationGroupKind::LineageCreate),
            target_path: target.clone(),
            outcome: RecoveryOutcome::Creation(CreationRecoveryOutcome::Installed),
        }]
    );
    assert_eq!(std::fs::read(target).unwrap(), b"manifest");
}

#[test]
fn admitted_replacement_uses_the_unix_rename_aside_state_machine() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let watched = temp.path().join("assets");
    std::fs::create_dir(&watched).unwrap();
    let quarantine = watched.join(".distill-displaced");
    let target = watched.join("lineage.bundle");
    let proposed = watched.join(".lineage.proposed");
    std::fs::write(&target, b"old").unwrap();
    std::fs::write(&proposed, b"new").unwrap();
    let old_hash = ContentHash(*blake3::hash(b"old").as_bytes());
    let new_hash = ContentHash(*blake3::hash(b"new").as_bytes());
    let mut store = Store::open(StoreConfig::new(&state)).unwrap();
    let driver = QuarantineDriver::new([QuarantineRoot::new(&watched, &quarantine)]).unwrap();
    let mut publication = driver.admit_publication(&mut store).unwrap();

    let group = publication
        .record_group(
            PublicationGroupKind::AuthoringWrite,
            b"test replacement",
            &[JournalIntentPlan {
                target_path: target.to_string_lossy().into_owned(),
                temp_path: proposed.to_string_lossy().into_owned(),
                conflict_path: watched
                    .join("lineage.conflict")
                    .to_string_lossy()
                    .into_owned(),
                pre_image_hash: Some(old_hash),
                proposed_hash: new_hash,
            }],
        )
        .unwrap();
    publication.arm_group(group.group_id).unwrap();
    assert_eq!(
        publication
            .resume_group_replace(group.child_intents[0], &target)
            .unwrap(),
        RenameAsideOutcome::Installed
    );
    publication.retire_group(group.group_id).unwrap();
    assert_eq!(std::fs::read(target).unwrap(), b"new");
}

#[test]
fn ordinary_admission_leaves_codegen_recovery_for_its_validated_filesystem() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let output = temp.path().join("generated");
    std::fs::create_dir(&output).unwrap();
    let quarantine = output.join(".distill-displaced");
    let target = output.join("mod.rs");
    let proposed = output.join(".mod.proposed");
    std::fs::write(&proposed, b"pub mod shader;\n").unwrap();
    let proposed_hash = ContentHash(*blake3::hash(b"pub mod shader;\n").as_bytes());
    let mut store = Store::open(StoreConfig::new(&state)).unwrap();
    let mut outputs = BTreeMap::new();
    outputs.insert("mod.rs".to_owned(), proposed_hash);
    let basis =
        CodegenPublicationBasis::new(store.input_version(), BTreeMap::new(), outputs.clone());
    store
        .record_publication_group(
            PublicationGroupKind::Codegen,
            &basis.encode(),
            &[JournalIntentPlan {
                target_path: target.to_string_lossy().into_owned(),
                temp_path: proposed.to_string_lossy().into_owned(),
                conflict_path: output.join("mod.conflict").to_string_lossy().into_owned(),
                pre_image_hash: None,
                proposed_hash,
            }],
        )
        .unwrap();
    let driver = QuarantineDriver::new([QuarantineRoot::new(&output, &quarantine)]).unwrap();

    let publication = driver.admit_publication(&mut store).unwrap();

    assert!(publication.recovered().is_empty());
    drop(publication);
    assert!(!target.exists());
    assert_eq!(std::fs::read(proposed).unwrap(), b"pub mod shader;\n");
    assert!(store.codegen_outputs().unwrap().is_empty());
    assert_eq!(store.unfinished_publication_groups().unwrap().len(), 1);
}

#[test]
fn unarmed_create_delete_group_is_abandoned_without_removing_the_source() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let watched = temp.path().join("assets");
    std::fs::create_dir(&watched).unwrap();
    let quarantine = watched.join(".distill-displaced");
    let source = watched.join("source.bundle");
    let destination = watched.join("destination.bundle");
    let proposal = watched.join(".destination.proposed");
    std::fs::write(&source, b"source bytes").unwrap();
    let source_hash = ContentHash(*blake3::hash(b"source bytes").as_bytes());
    let proposed_hash = ContentHash(*blake3::hash(b"destination bytes").as_bytes());
    let mut store = Store::open(StoreConfig::new(&state)).unwrap();
    let group = store
        .record_publication_group(
            PublicationGroupKind::AuthoringWrite,
            b"interrupted rename",
            &[
                JournalIntentPlan {
                    target_path: destination.to_string_lossy().into_owned(),
                    temp_path: proposal.to_string_lossy().into_owned(),
                    conflict_path: watched
                        .join("destination.conflict")
                        .to_string_lossy()
                        .into_owned(),
                    pre_image_hash: None,
                    proposed_hash,
                },
                JournalIntentPlan {
                    target_path: source.to_string_lossy().into_owned(),
                    temp_path: String::new(),
                    conflict_path: watched
                        .join("source.conflict")
                        .to_string_lossy()
                        .into_owned(),
                    pre_image_hash: Some(source_hash),
                    proposed_hash: ContentHash(*blake3::hash(b"").as_bytes()),
                },
            ],
        )
        .unwrap();
    let driver = QuarantineDriver::new([QuarantineRoot::new(&watched, &quarantine)]).unwrap();

    let publication = driver.admit_publication(&mut store).unwrap();

    assert_eq!(publication.recovered().len(), 2);
    assert!(publication.recovered().iter().all(|recovered| {
        recovered.group_kind == Some(PublicationGroupKind::AuthoringWrite)
            && recovered.outcome == RecoveryOutcome::AbandonedUnarmed
    }));
    drop(publication);
    assert_eq!(std::fs::read(&source).unwrap(), b"source bytes");
    assert!(!destination.exists());
    assert!(!proposal.exists());
    assert!(store.unfinished_publication_groups().unwrap().is_empty());
    assert_eq!(
        store
            .publication_group_child_results(group.group_id)
            .unwrap()
            .iter()
            .map(|child| child.terminal_success)
            .collect::<Vec<_>>(),
        [Some(false), Some(false)]
    );
}
