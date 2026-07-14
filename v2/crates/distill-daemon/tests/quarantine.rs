use distill_core::id::ContentHash;
use distill_daemon::quarantine::{QuarantineDriver, QuarantineRoot, RecoveryOutcome};
use distill_store::journal::{CreationRecoveryOutcome, RenameAsideOutcome};
use distill_store::{Store, StoreConfig, StoreError};

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

    let quarantined = driver
        .journaled_delete(&mut store, &target, expected)
        .unwrap();
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

    let error = driver
        .journaled_delete(&mut store, &target, ContentHash([7; 32]))
        .unwrap_err();
    assert!(matches!(
        error.store_error(),
        Some(StoreError::DeleteConflict { .. })
    ));
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
    let path = driver.journaled_delete(&mut store, &target, hash).unwrap();

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
    let intent = store
        .record_intent(
            &target.to_string_lossy(),
            &proposed.to_string_lossy(),
            &watched.join("lineage.conflict").to_string_lossy(),
            None,
            proposed_hash,
        )
        .unwrap();
    let driver = QuarantineDriver::new([QuarantineRoot::new(&watched, &quarantine)]).unwrap();

    let publication = driver.admit_publication(&mut store).unwrap();

    assert_eq!(
        publication.recovered(),
        &[(
            intent,
            RecoveryOutcome::Creation(CreationRecoveryOutcome::Installed)
        )]
    );
    assert_eq!(std::fs::read(target).unwrap(), b"manifest");
}

#[test]
fn admitted_replacement_uses_the_portable_no_replace_state_machine() {
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

    assert_eq!(
        publication
            .journaled_replace(&target, &proposed, old_hash, new_hash)
            .unwrap(),
        RenameAsideOutcome::Installed
    );
    assert_eq!(std::fs::read(target).unwrap(), b"new");
}
