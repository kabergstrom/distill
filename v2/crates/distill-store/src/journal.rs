//! §14 write-intent and displaced-inode journal.
//!
//! Quarantine is per filesystem, supplied by the file-tracking layer for
//! each watched root / daemon-owned output directory. Every displaced
//! inode gets an intent-derived unique name; content hashes are metadata
//! only and equal bytes never alias two live inodes.

use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::collections::BTreeMap;
#[cfg(unix)]
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use distill_core::id::ContentHash;
use rusqlite::OptionalExtension;

use crate::bundles::blob32;
use crate::db::{Store, StoreReader};
use crate::error::StoreError;

/// Durable lower bound for §14's rename-aside publication protocol.
///
/// Every filesystem transition is preceded by the journaled `*Journaled`
/// state and followed by a directory sync before its `*Durable` state is
/// stored. Recovery may therefore observe the filesystem one transition
/// ahead of this value, but never mistakes an unsynced move for durable work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum RenameAsideState {
    Prepared = 0,
    AsideJournaled = 1,
    TargetAsideDurable = 2,
    PreimageVerifiedDurable = 3,
    ConflictJournaled = 4,
    ConflictPreservedDurable = 5,
    PreimageRestoredDurable = 6,
    ProposedInstalledDurable = 7,
}

impl RenameAsideState {
    fn from_db(intent_id: i64, value: i64) -> Result<Self, StoreError> {
        match value {
            0 => Ok(Self::Prepared),
            1 => Ok(Self::AsideJournaled),
            2 => Ok(Self::TargetAsideDurable),
            3 => Ok(Self::PreimageVerifiedDurable),
            4 => Ok(Self::ConflictJournaled),
            5 => Ok(Self::ConflictPreservedDurable),
            6 => Ok(Self::PreimageRestoredDurable),
            7 => Ok(Self::ProposedInstalledDurable),
            _ => Err(StoreError::BadIntent {
                intent_id,
                detail: format!("unknown rename-aside state {value}"),
            }),
        }
    }
}

/// Terminal or deliberately stopped result of the no-overwrite rename-aside
/// protocol. `RetryRequired` means every observed object remains at a path
/// named by the unretired intent; no overwrite was attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameAsideOutcome {
    Installed,
    ConflictRestored,
    RetryRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionRecoveryOutcome {
    Deleted,
    ConflictRestored,
    RetryRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreationRecoveryOutcome {
    Installed,
    RetryRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteIntent {
    pub intent_id: i64,
    pub target_path: String,
    pub temp_path: String,
    pub conflict_path: String,
    pub pre_image_hash: Option<ContentHash>,
    pub proposed_hash: ContentHash,
    pub quarantine_paths: Vec<PathBuf>,
    pub rename_aside_state: RenameAsideState,
    pub terminal_success: Option<bool>,
    pub retired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplacedEntry {
    pub displacement_id: i64,
    pub intent_id: i64,
    pub ordinal: u32,
    pub content_hash: ContentHash,
    pub origin_path: String,
    pub quarantined_at: i64,
    pub path: PathBuf,
    pub restored: bool,
    pub cleaned_at: Option<i64>,
    pub cleanup_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredEdit {
    pub origin_path: String,
    pub expected: ContentHash,
    pub actual: Option<ContentHash>,
    pub quarantine_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum PublicationGroupKind {
    LineageCreate = 1,
    LineageDuplicate = 2,
    AuthoringWrite = 3,
    Import = 4,
    DiskMigration = 5,
    Codegen = 6,
    SchemaRepair = 7,
    SchemaTransition = 8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum PublicationGroupState {
    Unarmed = 0,
    Armed = 1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalIntentPlan {
    pub target_path: String,
    pub temp_path: String,
    pub conflict_path: String,
    pub pre_image_hash: Option<ContentHash>,
    pub proposed_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationGroup {
    pub group_id: i64,
    pub kind: PublicationGroupKind,
    /// Canonical operation-specific basis, retained in full rather than only
    /// by digest so startup diagnostics can name every promised pre-image.
    pub basis: Vec<u8>,
    pub state: PublicationGroupState,
    pub child_intents: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationChildResult {
    pub intent_id: i64,
    pub target_path: String,
    pub terminal_success: Option<bool>,
}

#[derive(Debug)]
struct RenameAsideIntent {
    target: PathBuf,
    temp: PathBuf,
    conflict: PathBuf,
    expected_preimage: ContentHash,
    proposed: ContentHash,
    state: RenameAsideState,
}

/// Result class for a journal filesystem mutation that may not clobber its
/// destination.
pub enum NoReplaceMoveError {
    DestinationExists,
    Other(StoreError),
}

/// Deliberately has no replace operation. Keeping the state machine generic
/// over this capability both prevents an overwrite implementation and lets
/// tests inject races at exact move boundaries on every supported host.
/// Filesystem authority used by the durable publication state machine.
///
/// Production implementations validate their configured canonical workspace
/// boundary before each mutation and reject symlinked publication entries.
/// The state machine assumes a trusted local workspace, while revalidation
/// catches ordinary concurrent path drift before state is published.
pub trait JournalFilesystem {
    fn read(&mut self, path: &Path) -> Result<Option<Vec<u8>>, StoreError>;
    fn create_dir_all(&mut self, path: &Path) -> Result<(), StoreError>;
    fn sync_file(&mut self, path: &Path) -> Result<(), StoreError>;
    fn sync_dir(&mut self, path: &Path) -> Result<(), StoreError>;
    fn ensure_same_filesystem(
        &mut self,
        source: &Path,
        destination_dir: &Path,
    ) -> Result<(), StoreError>;
    /// Atomically move a user-visible target to a journal-reserved unique
    /// aside or conflict path. The trusted-workspace contract makes the
    /// reserved destination private to this journal intent.
    fn rename_target_to_reserved(
        &mut self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), NoReplaceMoveError>;
    /// Install a daemon-owned proposal temp without clobbering a target that
    /// appeared after planning. The source is safe to unlink after linking.
    fn install_temp_no_replace(
        &mut self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), NoReplaceMoveError>;
    /// Remove a still-named daemon proposal after recovery proves that the
    /// same proposed bytes were already installed at the target.
    fn remove_daemon_temp(&mut self, path: &Path) -> Result<(), StoreError>;
    /// Restore a retained preimage without clobbering a reappeared target.
    /// The retained source remains named in quarantine after the link.
    fn restore_retained_no_replace(
        &mut self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), NoReplaceMoveError>;
}

impl Store {
    /// Atomically record a durable parent operation and every child mutation
    /// before the first filesystem change. This closes the crash window where
    /// a multi-path repair could otherwise expose an unparented partial plan.
    pub fn record_publication_group(
        &mut self,
        kind: PublicationGroupKind,
        basis: &[u8],
        plans: &[JournalIntentPlan],
    ) -> Result<PublicationGroup, StoreError> {
        if plans.is_empty() {
            return Err(StoreError::BadIntent {
                intent_id: 0,
                detail: "a publication group requires at least one child intent".into(),
            });
        }
        let txn = self.read.conn.transaction()?;
        txn.execute(
            "INSERT INTO publication_groups(kind, basis, state, retired) VALUES (?1, ?2, 0, 0)",
            rusqlite::params![kind as i64, basis],
        )?;
        let group_id = txn.last_insert_rowid();
        let mut child_intents = Vec::with_capacity(plans.len());
        for (ordinal, plan) in plans.iter().enumerate() {
            txn.execute(
                "INSERT INTO write_intents(target_path, temp_path, conflict_path,
                                           pre_image_hash, proposed_hash, retired)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                rusqlite::params![
                    plan.target_path,
                    plan.temp_path,
                    plan.conflict_path,
                    plan.pre_image_hash.as_ref().map(|hash| hash.0.as_slice()),
                    plan.proposed_hash.0.as_slice(),
                ],
            )?;
            let intent_id = txn.last_insert_rowid();
            txn.execute(
                "INSERT INTO publication_group_children(group_id, ordinal, intent_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![group_id, ordinal as i64, intent_id],
            )?;
            child_intents.push(intent_id);
        }
        txn.commit()?;
        Ok(PublicationGroup {
            group_id,
            kind,
            basis: basis.to_vec(),
            state: PublicationGroupState::Unarmed,
            child_intents,
        })
    }

    /// Make a complete proposal set executable only after every non-empty
    /// proposal temp has been written and fsynced by the caller.
    pub fn arm_publication_group(&mut self, group_id: i64) -> Result<(), StoreError> {
        let changed = self.conn.execute(
            "UPDATE publication_groups SET state = 1
             WHERE group_id = ?1 AND state = 0 AND retired = 0",
            [group_id],
        )?;
        if changed != 1 {
            return Err(StoreError::BadIntent {
                intent_id: group_id,
                detail: "publication group is missing, retired, or already armed".into(),
            });
        }
        Ok(())
    }

    /// An unarmed group has performed no child mutation. Retire every child
    /// as an abandoned attempt in one durable transaction, including children
    /// such as deletions that do not have a proposal temp.
    pub fn abort_unarmed_publication_group(&mut self, group_id: i64) -> Result<(), StoreError> {
        let transaction = self.read.conn.transaction()?;
        let state: Option<(i64, i64)> = transaction
            .query_row(
                "SELECT state, retired FROM publication_groups WHERE group_id = ?1",
                [group_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if state != Some((PublicationGroupState::Unarmed as i64, 0)) {
            return Err(StoreError::BadIntent {
                intent_id: group_id,
                detail: "only an unfinished unarmed publication group may be aborted".into(),
            });
        }
        transaction.execute(
            "UPDATE write_intents SET terminal_success = 0, retired = 1
             WHERE intent_id IN (
                 SELECT intent_id FROM publication_group_children WHERE group_id = ?1
             ) AND retired = 0",
            [group_id],
        )?;
        transaction.execute(
            "UPDATE publication_groups SET retired = 1 WHERE group_id = ?1",
            [group_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Retire a parent only after every named child reached a terminal durable
    /// state. A caller must perform its final rescan before publishing the
    /// healed input version; retirement merely proves no filesystem work is
    /// left implicit.
    pub fn retire_publication_group(&mut self, group_id: i64) -> Result<(), StoreError> {
        let transaction = self.read.conn.transaction()?;
        let state: Option<i64> = transaction
            .query_row(
                "SELECT state FROM publication_groups WHERE group_id = ?1 AND retired = 0",
                [group_id],
                |row| row.get(0),
            )
            .optional()?;
        if state != Some(PublicationGroupState::Armed as i64) {
            return Err(StoreError::BadIntent {
                intent_id: group_id,
                detail: "only an armed publication group may retire normally".into(),
            });
        }
        let pending: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM publication_group_children c
             JOIN write_intents w ON w.intent_id = c.intent_id
             WHERE c.group_id = ?1 AND w.retired = 0",
            [group_id],
            |row| row.get(0),
        )?;
        if pending != 0 {
            return Err(StoreError::BadIntent {
                intent_id: group_id,
                detail: format!("publication group still has {pending} unfinished children"),
            });
        }
        let changed = transaction.execute(
            "UPDATE publication_groups SET retired = 1
             WHERE group_id = ?1 AND retired = 0",
            [group_id],
        )?;
        if changed != 1 {
            return Err(StoreError::BadIntent {
                intent_id: group_id,
                detail: "no unfinished publication group with this id".into(),
            });
        }
        transaction.commit()?;
        Ok(())
    }

    /// Execute or resume the supported-Unix rename-aside replacement state machine.
    /// Every move preserves a destination that appeared after the basis CAS;
    /// the exact pre-image and any raced target remain journal-addressable.
    #[cfg(unix)]
    pub fn publish_journaled_replacement(
        &mut self,
        intent_id: i64,
        quarantine_dir: &Path,
    ) -> Result<RenameAsideOutcome, StoreError> {
        self.ensure_intent_group_armed(intent_id)?;
        let mut filesystem = self.native_filesystem_for_intent(intent_id, Some(quarantine_dir))?;
        self.publish_rename_aside_with(intent_id, quarantine_dir, &mut filesystem)
    }

    /// Resume a journaled deletion on every supported host.
    #[cfg(unix)]
    pub fn reconcile_journaled_deletion(
        &mut self,
        intent_id: i64,
        quarantine_dir: &Path,
    ) -> Result<DeletionRecoveryOutcome, StoreError> {
        self.ensure_intent_group_armed(intent_id)?;
        let mut filesystem = self.native_filesystem_for_intent(intent_id, Some(quarantine_dir))?;
        self.reconcile_deletion_with(intent_id, quarantine_dir, &mut filesystem)
    }

    /// Resume a journaled no-replace creation on every supported host.
    #[cfg(unix)]
    pub fn reconcile_journaled_creation(
        &mut self,
        intent_id: i64,
    ) -> Result<CreationRecoveryOutcome, StoreError> {
        self.ensure_intent_group_armed(intent_id)?;
        let mut filesystem = self.native_filesystem_for_intent(intent_id, None)?;
        self.reconcile_creation_with(intent_id, &mut filesystem)
    }

    /// Resume a replacement through caller-supplied filesystem authority.
    /// This is the same durable state machine as
    /// [`Self::publish_journaled_replacement`]; only path resolution and the
    /// no-replace primitive are supplied by the caller.
    pub fn publish_journaled_replacement_with_filesystem(
        &mut self,
        intent_id: i64,
        quarantine_dir: &Path,
        filesystem: &mut dyn JournalFilesystem,
    ) -> Result<RenameAsideOutcome, StoreError> {
        self.ensure_intent_group_armed(intent_id)?;
        self.publish_rename_aside_with(intent_id, quarantine_dir, filesystem)
    }

    /// Resume a deletion through caller-supplied filesystem authority.
    pub fn reconcile_journaled_deletion_with_filesystem(
        &mut self,
        intent_id: i64,
        quarantine_dir: &Path,
        filesystem: &mut dyn JournalFilesystem,
    ) -> Result<DeletionRecoveryOutcome, StoreError> {
        self.ensure_intent_group_armed(intent_id)?;
        self.reconcile_deletion_with(intent_id, quarantine_dir, filesystem)
    }

    /// Resume a creation through caller-supplied filesystem authority.
    pub fn reconcile_journaled_creation_with_filesystem(
        &mut self,
        intent_id: i64,
        filesystem: &mut dyn JournalFilesystem,
    ) -> Result<CreationRecoveryOutcome, StoreError> {
        self.ensure_intent_group_armed(intent_id)?;
        self.reconcile_creation_with(intent_id, filesystem)
    }

    fn reconcile_deletion_with<F: JournalFilesystem + ?Sized>(
        &mut self,
        intent_id: i64,
        quarantine_dir: &Path,
        fs: &mut F,
    ) -> Result<DeletionRecoveryOutcome, StoreError> {
        let (target, temp, expected) = self.load_intent_paths(intent_id)?;
        if !temp.as_os_str().is_empty() {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "deletion recovery requires an empty temp path".into(),
            });
        }
        let expected = expected.ok_or_else(|| StoreError::BadIntent {
            intent_id,
            detail: "deletion recovery requires an expected pre-image".into(),
        })?;
        fs.create_dir_all(quarantine_dir)?;
        if let Some(parent) = quarantine_dir.parent() {
            fs.sync_dir(parent)?;
        }
        fs.sync_dir(quarantine_dir)?;

        let mut aside = self
            .displacement_path_optional(intent_id, 0)?
            .unwrap_or_else(|| self.reserved_aside_path(quarantine_dir, intent_id));
        if self.displacement_path_optional(intent_id, 0)?.is_none() {
            let Some(observed) = read_hash(fs, &target)? else {
                self.retire_intent_with_outcome(intent_id, false)?;
                return Ok(DeletionRecoveryOutcome::RetryRequired);
            };
            self.journal_rename_aside_displacement(intent_id, 0, observed, &target, &aside)?;
        }

        for _ in 0..16 {
            if let Some(actual) = read_hash(fs, &aside)? {
                let journaled = self.displacement_hash(intent_id, 0)?;
                if actual != journaled && fs.read(&target)?.is_some() {
                    fs.sync_file(&aside)?;
                    fs.sync_dir(aside.parent().unwrap_or_else(|| Path::new(".")))?;
                    let replacement = self.available_conflict_path(fs, intent_id, &aside)?;
                    self.preserve_displacement_path_collision(
                        intent_id,
                        0,
                        &aside,
                        &replacement,
                        actual,
                    )?;
                    aside = replacement;
                    continue;
                }
                fs.sync_file(&aside)?;
                sync_move_dirs(fs, &target, &aside)?;
                self.finish_journaled_move(
                    intent_id,
                    0,
                    actual,
                    RenameAsideState::TargetAsideDurable,
                )?;
                if actual == expected {
                    if fs.read(&target)?.is_some() {
                        return Ok(DeletionRecoveryOutcome::RetryRequired);
                    }
                    self.retire_intent_with_outcome(intent_id, true)?;
                    return Ok(DeletionRecoveryOutcome::Deleted);
                }
                match read_hash(fs, &target)? {
                    Some(target_hash) if target_hash == actual => {
                        fs.sync_file(&target)?;
                        fs.sync_dir(target.parent().unwrap_or_else(|| Path::new(".")))?;
                        self.finish_rename_aside_terminal(
                            intent_id,
                            RenameAsideState::PreimageRestoredDurable,
                            true,
                        )?;
                        return Ok(DeletionRecoveryOutcome::ConflictRestored);
                    }
                    Some(_) => return Ok(DeletionRecoveryOutcome::RetryRequired),
                    None => {}
                }
                match fs.restore_retained_no_replace(&aside, &target) {
                    Ok(()) => {
                        fs.sync_file(&target)?;
                        sync_move_dirs(fs, &aside, &target)?;
                        self.finish_rename_aside_terminal(
                            intent_id,
                            RenameAsideState::PreimageRestoredDurable,
                            true,
                        )?;
                        return Ok(DeletionRecoveryOutcome::ConflictRestored);
                    }
                    Err(NoReplaceMoveError::DestinationExists) => {
                        return Ok(DeletionRecoveryOutcome::RetryRequired)
                    }
                    Err(NoReplaceMoveError::Other(error)) => return Err(error),
                }
            }

            let Some(observed) = read_hash(fs, &target)? else {
                self.retire_intent_with_outcome(intent_id, false)?;
                return Ok(DeletionRecoveryOutcome::RetryRequired);
            };
            match fs.rename_target_to_reserved(&target, &aside) {
                Ok(()) => {
                    fs.sync_file(&aside)?;
                    sync_move_dirs(fs, &target, &aside)?;
                    self.finish_journaled_move(
                        intent_id,
                        0,
                        observed,
                        RenameAsideState::TargetAsideDurable,
                    )?;
                }
                Err(NoReplaceMoveError::DestinationExists) => {
                    let collision =
                        existing_hash(fs, intent_id, &aside, "deletion quarantine collision")?;
                    fs.sync_file(&aside)?;
                    fs.sync_dir(aside.parent().unwrap_or_else(|| Path::new(".")))?;
                    let replacement = self.available_conflict_path(fs, intent_id, &aside)?;
                    self.preserve_displacement_path_collision(
                        intent_id,
                        0,
                        &aside,
                        &replacement,
                        collision,
                    )?;
                    aside = replacement;
                }
                Err(NoReplaceMoveError::Other(error)) => return Err(error),
            }
        }
        Err(StoreError::BadIntent {
            intent_id,
            detail: "deletion recovery exceeded its transition bound".into(),
        })
    }

    fn reconcile_creation_with<F: JournalFilesystem + ?Sized>(
        &mut self,
        intent_id: i64,
        fs: &mut F,
    ) -> Result<CreationRecoveryOutcome, StoreError> {
        let (target, temp, preimage) = self.load_intent_paths(intent_id)?;
        if preimage.is_some() || temp.as_os_str().is_empty() {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "creation recovery requires a temp and no pre-image".into(),
            });
        }
        let proposed = self.intent_proposed_hash(intent_id)?;
        let target_hash = read_hash(fs, &target)?;
        let temp_hash = read_hash(fs, &temp)?;
        if target_hash == Some(proposed) && temp_hash == Some(proposed) {
            fs.sync_file(&target)?;
            fs.remove_daemon_temp(&temp)?;
            sync_move_dirs(fs, &temp, &target)?;
            self.retire_intent_with_outcome(intent_id, true)?;
            return Ok(CreationRecoveryOutcome::Installed);
        }
        if target_hash == Some(proposed) && temp_hash.is_none() {
            fs.sync_file(&target)?;
            fs.sync_dir(target.parent().unwrap_or_else(|| Path::new(".")))?;
            self.retire_intent_with_outcome(intent_id, true)?;
            return Ok(CreationRecoveryOutcome::Installed);
        }
        // A publication group is durable before its proposal temp is
        // created. A crash in that narrow window leaves a Prepared intent
        // whose target was never touched; abandoning it is therefore safe.
        if temp_hash.is_none() {
            self.retire_intent_with_outcome(intent_id, false)?;
            return Ok(CreationRecoveryOutcome::RetryRequired);
        }
        if target_hash.is_some() {
            self.retire_intent_with_outcome(intent_id, false)?;
            return Ok(CreationRecoveryOutcome::RetryRequired);
        }
        if temp_hash != Some(proposed) {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "creation proposal temp changed".into(),
            });
        }
        fs.sync_file(&temp)?;
        match fs.install_temp_no_replace(&temp, &target) {
            Ok(()) => {
                fs.sync_file(&target)?;
                sync_move_dirs(fs, &temp, &target)?;
                require_hash(fs, intent_id, &target, proposed, "installed creation")?;
                self.retire_intent_with_outcome(intent_id, true)?;
                Ok(CreationRecoveryOutcome::Installed)
            }
            Err(NoReplaceMoveError::DestinationExists) => {
                self.retire_intent_with_outcome(intent_id, false)?;
                Ok(CreationRecoveryOutcome::RetryRequired)
            }
            Err(NoReplaceMoveError::Other(error)) => Err(error),
        }
    }

    fn publish_rename_aside_with<F: JournalFilesystem + ?Sized>(
        &mut self,
        intent_id: i64,
        quarantine_dir: &Path,
        fs: &mut F,
    ) -> Result<RenameAsideOutcome, StoreError> {
        let mut intent = self.load_rename_aside_intent(intent_id)?;
        let mut aside = if intent.state == RenameAsideState::Prepared {
            self.reserved_aside_path(quarantine_dir, intent_id)
        } else {
            self.displacement_path(intent_id, 0)?
        };

        for _ in 0..16 {
            match intent.state {
                RenameAsideState::Prepared => {
                    // Recording the intent precedes creation of the proposal
                    // temp. With no temp, this attempt cannot have performed
                    // its first filesystem transition and is safe to abandon.
                    if read_hash(fs, &intent.temp)?.is_none() {
                        self.retire_intent_with_outcome(intent_id, false)?;
                        return Ok(RenameAsideOutcome::RetryRequired);
                    }
                    fs.create_dir_all(quarantine_dir)?;
                    if let Some(parent) = quarantine_dir.parent() {
                        fs.sync_dir(parent)?;
                    }
                    fs.sync_dir(quarantine_dir)?;
                    fs.ensure_same_filesystem(&intent.target, quarantine_dir)?;
                    fs.ensure_same_filesystem(&intent.temp, quarantine_dir)?;
                    require_hash(
                        fs,
                        intent_id,
                        &intent.temp,
                        intent.proposed,
                        "proposed temp",
                    )?;
                    fs.sync_file(&intent.temp)?;
                    let Some(observed) = read_hash(fs, &intent.target)? else {
                        self.retire_intent_with_outcome(intent_id, false)?;
                        return Ok(RenameAsideOutcome::RetryRequired);
                    };
                    // The preimage CAS belongs inside the journaled state
                    // machine immediately before its first filesystem
                    // mutation. A user edit that landed after planning stays
                    // at the target; it must not be renamed aside and leave
                    // an unrecoverable armed child that blocks later
                    // independent publications.
                    if observed != intent.expected_preimage {
                        self.retire_intent_with_outcome(intent_id, false)?;
                        return Ok(RenameAsideOutcome::RetryRequired);
                    }
                    let mut reserved_collision = None;
                    if let Some(collision) = read_hash(fs, &aside)? {
                        fs.sync_file(&aside)?;
                        fs.sync_dir(aside.parent().unwrap_or_else(|| Path::new(".")))?;
                        let replacement = self.available_conflict_path(fs, intent_id, &aside)?;
                        reserved_collision = Some((aside, collision));
                        aside = replacement;
                    }
                    self.journal_rename_aside_displacement_with_collision(
                        intent_id,
                        0,
                        observed,
                        &intent.target,
                        &aside,
                        reserved_collision
                            .as_ref()
                            .map(|(path, hash)| (path.as_path(), *hash)),
                    )?;
                    intent.state = RenameAsideState::AsideJournaled;
                }
                RenameAsideState::AsideJournaled => {
                    if let Some(actual) = read_hash(fs, &aside)? {
                        let journaled = self.displacement_hash(intent_id, 0)?;
                        if actual != journaled && fs.read(&intent.target)?.is_some() {
                            fs.sync_file(&aside)?;
                            fs.sync_dir(aside.parent().unwrap_or_else(|| Path::new(".")))?;
                            let replacement =
                                self.available_conflict_path(fs, intent_id, &aside)?;
                            self.preserve_displacement_path_collision(
                                intent_id,
                                0,
                                &aside,
                                &replacement,
                                actual,
                            )?;
                            aside = replacement;
                            continue;
                        }
                        fs.sync_file(&aside)?;
                        sync_move_dirs(fs, &intent.target, &aside)?;
                        self.finish_journaled_move(
                            intent_id,
                            0,
                            actual,
                            RenameAsideState::TargetAsideDurable,
                        )?;
                        intent.state = RenameAsideState::TargetAsideDurable;
                        continue;
                    }
                    if fs.read(&intent.target)?.is_none() {
                        self.retire_intent_with_outcome(intent_id, false)?;
                        return Ok(RenameAsideOutcome::RetryRequired);
                    }
                    match fs.rename_target_to_reserved(&intent.target, &aside) {
                        Ok(()) => {}
                        Err(NoReplaceMoveError::DestinationExists) => continue,
                        Err(NoReplaceMoveError::Other(error)) => return Err(error),
                    }
                    fs.sync_file(&aside)?;
                    sync_move_dirs(fs, &intent.target, &aside)?;
                    let actual = existing_hash(fs, intent_id, &aside, "displaced pre-image")?;
                    self.finish_journaled_move(
                        intent_id,
                        0,
                        actual,
                        RenameAsideState::TargetAsideDurable,
                    )?;
                    intent.state = RenameAsideState::TargetAsideDurable;
                }
                RenameAsideState::TargetAsideDurable => {
                    let aside_hash =
                        read_hash(fs, &aside)?.ok_or_else(|| StoreError::BadIntent {
                            intent_id,
                            detail: "durable rename-aside state has no displaced pre-image"
                                .to_owned(),
                        })?;
                    if aside_hash != intent.expected_preimage {
                        if let Some(outcome) = self.restore_changed_aside(
                            fs,
                            intent_id,
                            &aside,
                            aside_hash,
                            &mut intent,
                        )? {
                            return Ok(outcome);
                        }
                        continue;
                    }

                    // Verification is itself durable before installation;
                    // recovery never infers permission to install solely
                    // from a path layout left by an earlier state.
                    self.set_rename_aside_state(
                        intent_id,
                        RenameAsideState::PreimageVerifiedDurable,
                    )?;
                    intent.state = RenameAsideState::PreimageVerifiedDurable;
                }
                RenameAsideState::PreimageVerifiedDurable => {
                    let aside_hash =
                        existing_hash(fs, intent_id, &aside, "verified displaced pre-image")?;
                    if aside_hash != intent.expected_preimage {
                        if let Some(outcome) = self.restore_changed_aside(
                            fs,
                            intent_id,
                            &aside,
                            aside_hash,
                            &mut intent,
                        )? {
                            return Ok(outcome);
                        }
                        continue;
                    }
                    let target_hash = read_hash(fs, &intent.target)?;
                    let temp_hash = read_hash(fs, &intent.temp)?;

                    // A crash may happen after the successful no-replace
                    // install and directory sync but before its state commit.
                    if target_hash == Some(intent.proposed) && temp_hash == Some(intent.proposed) {
                        fs.sync_file(&intent.target)?;
                        fs.remove_daemon_temp(&intent.temp)?;
                        sync_move_dirs(fs, &intent.temp, &intent.target)?;
                        self.finish_rename_aside_terminal(
                            intent_id,
                            RenameAsideState::ProposedInstalledDurable,
                            false,
                        )?;
                        return Ok(RenameAsideOutcome::Installed);
                    }
                    if target_hash == Some(intent.proposed) && temp_hash.is_none() {
                        fs.sync_file(&intent.target)?;
                        sync_move_dirs(fs, &intent.temp, &intent.target)?;
                        self.finish_rename_aside_terminal(
                            intent_id,
                            RenameAsideState::ProposedInstalledDurable,
                            false,
                        )?;
                        return Ok(RenameAsideOutcome::Installed);
                    }

                    match target_hash {
                        None => {
                            if temp_hash != Some(intent.proposed) {
                                return Err(StoreError::BadIntent {
                                    intent_id,
                                    detail:
                                        "proposed temp vanished or changed while target was aside"
                                            .to_owned(),
                                });
                            }
                            match fs.install_temp_no_replace(&intent.temp, &intent.target) {
                                Ok(()) => {
                                    fs.sync_file(&intent.target)?;
                                    sync_move_dirs(fs, &intent.temp, &intent.target)?;
                                    let installed = existing_hash(
                                        fs,
                                        intent_id,
                                        &intent.target,
                                        "installed proposal",
                                    )?;
                                    if installed == intent.proposed {
                                        self.finish_rename_aside_terminal(
                                            intent_id,
                                            RenameAsideState::ProposedInstalledDurable,
                                            false,
                                        )?;
                                        return Ok(RenameAsideOutcome::Installed);
                                    }
                                    self.begin_reappeared_target(
                                        fs,
                                        intent_id,
                                        installed,
                                        &mut intent,
                                    )?;
                                }
                                Err(NoReplaceMoveError::DestinationExists) => continue,
                                Err(NoReplaceMoveError::Other(error)) => return Err(error),
                            }
                        }
                        Some(reappeared) => {
                            self.begin_reappeared_target(fs, intent_id, reappeared, &mut intent)?;
                        }
                    }
                }
                RenameAsideState::ConflictJournaled => {
                    let conflict_hash = read_hash(fs, &intent.conflict)?;
                    if let Some(actual) = conflict_hash {
                        let journaled = self.displacement_hash(intent_id, 1)?;
                        if actual != journaled {
                            fs.sync_file(&intent.conflict)?;
                            fs.sync_dir(
                                intent.conflict.parent().unwrap_or_else(|| Path::new(".")),
                            )?;
                            let replacement =
                                self.available_conflict_path(fs, intent_id, &intent.conflict)?;
                            self.preserve_displacement_path_collision(
                                intent_id,
                                1,
                                &intent.conflict,
                                &replacement,
                                actual,
                            )?;
                            intent.conflict = replacement;
                            continue;
                        }
                        fs.sync_file(&intent.conflict)?;
                        sync_move_dirs(fs, &intent.target, &intent.conflict)?;
                        self.finish_journaled_move(
                            intent_id,
                            1,
                            actual,
                            RenameAsideState::ConflictPreservedDurable,
                        )?;
                        intent.state = RenameAsideState::ConflictPreservedDurable;
                        continue;
                    }
                    if fs.read(&intent.target)?.is_none() {
                        return Err(StoreError::BadIntent {
                            intent_id,
                            detail: "planned conflict has neither a target nor conflict file"
                                .to_owned(),
                        });
                    }
                    match fs.rename_target_to_reserved(&intent.target, &intent.conflict) {
                        Ok(()) => {}
                        Err(NoReplaceMoveError::DestinationExists) => {
                            let collision = existing_hash(
                                fs,
                                intent_id,
                                &intent.conflict,
                                "conflict-path collision",
                            )?;
                            fs.sync_file(&intent.conflict)?;
                            fs.sync_dir(
                                intent.conflict.parent().unwrap_or_else(|| Path::new(".")),
                            )?;
                            let replacement =
                                self.available_conflict_path(fs, intent_id, &intent.conflict)?;
                            self.preserve_displacement_path_collision(
                                intent_id,
                                1,
                                &intent.conflict,
                                &replacement,
                                collision,
                            )?;
                            intent.conflict = replacement;
                            continue;
                        }
                        Err(NoReplaceMoveError::Other(error)) => return Err(error),
                    }
                    fs.sync_file(&intent.conflict)?;
                    sync_move_dirs(fs, &intent.target, &intent.conflict)?;
                    let actual =
                        existing_hash(fs, intent_id, &intent.conflict, "preserved conflict")?;
                    self.finish_journaled_move(
                        intent_id,
                        1,
                        actual,
                        RenameAsideState::ConflictPreservedDurable,
                    )?;
                    intent.state = RenameAsideState::ConflictPreservedDurable;
                }
                RenameAsideState::ConflictPreservedDurable => {
                    let aside_hash = read_hash(fs, &aside)?;
                    if let Some(aside_hash) = aside_hash {
                        match read_hash(fs, &intent.target)? {
                            Some(target_hash) if target_hash == aside_hash => {
                                fs.sync_file(&intent.target)?;
                                fs.sync_dir(
                                    intent.target.parent().unwrap_or_else(|| Path::new(".")),
                                )?;
                                self.finish_rename_aside_terminal(
                                    intent_id,
                                    RenameAsideState::PreimageRestoredDurable,
                                    true,
                                )?;
                                return Ok(RenameAsideOutcome::ConflictRestored);
                            }
                            Some(_) => return Ok(RenameAsideOutcome::RetryRequired),
                            None => {}
                        }
                        match fs.restore_retained_no_replace(&aside, &intent.target) {
                            Ok(()) => {
                                fs.sync_file(&intent.target)?;
                                sync_move_dirs(fs, &aside, &intent.target)?;
                                require_hash(
                                    fs,
                                    intent_id,
                                    &intent.target,
                                    aside_hash,
                                    "restored displaced pre-image",
                                )?;
                                self.finish_rename_aside_terminal(
                                    intent_id,
                                    RenameAsideState::PreimageRestoredDurable,
                                    true,
                                )?;
                                return Ok(RenameAsideOutcome::ConflictRestored);
                            }
                            Err(NoReplaceMoveError::DestinationExists) => {
                                return Ok(RenameAsideOutcome::RetryRequired)
                            }
                            Err(NoReplaceMoveError::Other(error)) => return Err(error),
                        }
                    }

                    // Recovery after a completed restore but before the
                    // terminal state commit: only the exact journaled aside
                    // hash is accepted as the restored target.
                    let displaced_hash = self.displacement_hash(intent_id, 0)?;
                    if read_hash(fs, &intent.target)? == Some(displaced_hash) {
                        fs.sync_file(&intent.target)?;
                        sync_move_dirs(fs, &aside, &intent.target)?;
                        self.finish_rename_aside_terminal(
                            intent_id,
                            RenameAsideState::PreimageRestoredDurable,
                            true,
                        )?;
                        return Ok(RenameAsideOutcome::ConflictRestored);
                    }
                    return Ok(RenameAsideOutcome::RetryRequired);
                }
                RenameAsideState::PreimageRestoredDurable => {
                    return Ok(RenameAsideOutcome::ConflictRestored)
                }
                RenameAsideState::ProposedInstalledDurable => {
                    return Ok(RenameAsideOutcome::Installed)
                }
            }
        }

        Err(StoreError::BadIntent {
            intent_id,
            detail: "rename-aside recovery exceeded its transition bound".to_owned(),
        })
    }

    /// An in-place writer can change the displaced inode after the target was
    /// atomically renamed aside. The changed inode is the newest user object:
    /// restore it no-clobber (after preserving any reappeared target) and
    /// terminate the child as a conflict instead of wedging recovery.
    fn restore_changed_aside<F: JournalFilesystem + ?Sized>(
        &mut self,
        fs: &mut F,
        intent_id: i64,
        aside: &Path,
        actual: ContentHash,
        intent: &mut RenameAsideIntent,
    ) -> Result<Option<RenameAsideOutcome>, StoreError> {
        match read_hash(fs, &intent.target)? {
            Some(target) if target == actual => {
                fs.sync_file(&intent.target)?;
                fs.sync_dir(intent.target.parent().unwrap_or_else(|| Path::new(".")))?;
                self.finish_rename_aside_terminal(
                    intent_id,
                    RenameAsideState::PreimageRestoredDurable,
                    true,
                )?;
                Ok(Some(RenameAsideOutcome::ConflictRestored))
            }
            Some(reappeared) => {
                self.begin_reappeared_target(fs, intent_id, reappeared, intent)?;
                Ok(None)
            }
            None => match fs.restore_retained_no_replace(aside, &intent.target) {
                Ok(()) => {
                    fs.sync_file(&intent.target)?;
                    sync_move_dirs(fs, aside, &intent.target)?;
                    require_hash(
                        fs,
                        intent_id,
                        &intent.target,
                        actual,
                        "restored late-written displacement",
                    )?;
                    self.finish_rename_aside_terminal(
                        intent_id,
                        RenameAsideState::PreimageRestoredDurable,
                        true,
                    )?;
                    Ok(Some(RenameAsideOutcome::ConflictRestored))
                }
                Err(NoReplaceMoveError::DestinationExists) => Ok(None),
                Err(NoReplaceMoveError::Other(error)) => Err(error),
            },
        }
    }

    fn journal_rename_aside_displacement(
        &mut self,
        intent_id: i64,
        ordinal: u32,
        content_hash: ContentHash,
        origin: &Path,
        destination: &Path,
    ) -> Result<(), StoreError> {
        self.journal_rename_aside_displacement_with_collision(
            intent_id,
            ordinal,
            content_hash,
            origin,
            destination,
            None,
        )
    }

    /// A retained quarantine directory can outlive disposable daemon state.
    /// Persist its occupied reserved name in the same transaction that selects
    /// ordinal zero and advances the intent, so a crash can expose both rows or
    /// neither row but can never strand the reserved name between them.
    fn journal_rename_aside_displacement_with_collision(
        &mut self,
        intent_id: i64,
        ordinal: u32,
        content_hash: ContentHash,
        origin: &Path,
        destination: &Path,
        reserved_collision: Option<(&Path, ContentHash)>,
    ) -> Result<(), StoreError> {
        let state = match ordinal {
            0 => RenameAsideState::AsideJournaled,
            1 => RenameAsideState::ConflictJournaled,
            _ => {
                return Err(StoreError::BadIntent {
                    intent_id,
                    detail: format!("invalid no-replace displacement ordinal {ordinal}"),
                });
            }
        };
        let transaction = self.read.conn.transaction()?;
        if let Some((path, collision_hash)) = reserved_collision {
            let collision_ordinal: u32 = transaction.query_row(
                "SELECT COALESCE(MAX(CASE WHEN ordinal >= 2 THEN ordinal END) + 1, 2)
                 FROM displaced WHERE intent_id = ?1",
                [intent_id],
                |row| row.get(0),
            )?;
            transaction.execute(
                "INSERT INTO displaced(intent_id, ordinal, content_hash, origin_path,
                                        quarantine_path, quarantined_at, restored)
                 VALUES (?1, ?2, ?3, ?4, ?4, ?5, 0)",
                rusqlite::params![
                    intent_id,
                    collision_ordinal,
                    collision_hash.0.as_slice(),
                    path.to_string_lossy(),
                    now_secs(),
                ],
            )?;
        }
        transaction.execute(
            "INSERT INTO displaced(intent_id, ordinal, content_hash, origin_path,
                                   quarantine_path, quarantined_at, restored)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
            rusqlite::params![
                intent_id,
                ordinal,
                content_hash.0.as_slice(),
                origin.to_string_lossy(),
                destination.to_string_lossy(),
                now_secs(),
            ],
        )?;
        let updated = if ordinal == 1 {
            transaction.execute(
                "UPDATE write_intents
                 SET conflict_path = ?2, rename_aside_state = ?3
                 WHERE intent_id = ?1 AND retired = 0",
                rusqlite::params![intent_id, destination.to_string_lossy(), state as i64],
            )?
        } else {
            transaction.execute(
                "UPDATE write_intents SET rename_aside_state = ?2
                 WHERE intent_id = ?1 AND retired = 0",
                rusqlite::params![intent_id, state as i64],
            )?
        };
        if updated != 1 {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "intent retired while journaling a no-replace move".to_owned(),
            });
        }
        transaction.commit()?;
        Ok(())
    }

    fn begin_reappeared_target<F: JournalFilesystem + ?Sized>(
        &mut self,
        fs: &mut F,
        intent_id: i64,
        observed: ContentHash,
        intent: &mut RenameAsideIntent,
    ) -> Result<(), StoreError> {
        let destination = self.available_conflict_path(fs, intent_id, &intent.conflict)?;
        self.journal_rename_aside_displacement(
            intent_id,
            1,
            observed,
            &intent.target,
            &destination,
        )?;
        intent.conflict = destination;
        intent.state = RenameAsideState::ConflictJournaled;
        Ok(())
    }

    fn preserve_displacement_path_collision(
        &mut self,
        intent_id: i64,
        ordinal: u32,
        previous: &Path,
        replacement: &Path,
        collision_hash: ContentHash,
    ) -> Result<(), StoreError> {
        let transaction = self.read.conn.transaction()?;
        let updated = transaction.execute(
            "UPDATE displaced SET quarantine_path = ?4
             WHERE intent_id = ?1 AND ordinal = ?2 AND quarantine_path = ?3",
            rusqlite::params![
                intent_id,
                ordinal,
                previous.to_string_lossy(),
                replacement.to_string_lossy(),
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "planned conflict path changed during no-replace retry".to_owned(),
            });
        }
        let next_ordinal: u32 = transaction.query_row(
            "SELECT COALESCE(MAX(CASE WHEN ordinal >= 2 THEN ordinal END) + 1, 2)
             FROM displaced WHERE intent_id = ?1",
            [intent_id],
            |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO displaced(intent_id, ordinal, content_hash, origin_path,
                                   quarantine_path, quarantined_at, restored)
             VALUES (?1, ?2, ?3, ?4, ?4, ?5, 0)",
            rusqlite::params![
                intent_id,
                next_ordinal,
                collision_hash.0.as_slice(),
                previous.to_string_lossy(),
                now_secs(),
            ],
        )?;
        if ordinal == 1 {
            transaction.execute(
                "UPDATE write_intents SET conflict_path = ?2
                 WHERE intent_id = ?1 AND retired = 0",
                rusqlite::params![intent_id, replacement.to_string_lossy()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn finish_journaled_move(
        &mut self,
        intent_id: i64,
        ordinal: u32,
        actual: ContentHash,
        state: RenameAsideState,
    ) -> Result<(), StoreError> {
        let transaction = self.read.conn.transaction()?;
        let updated = transaction.execute(
            "UPDATE displaced SET content_hash = ?3
             WHERE intent_id = ?1 AND ordinal = ?2",
            rusqlite::params![intent_id, ordinal, actual.0.as_slice()],
        )?;
        if updated != 1 {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: format!("missing displacement {ordinal} after no-replace move"),
            });
        }
        transaction.execute(
            "UPDATE write_intents SET rename_aside_state = ?2
             WHERE intent_id = ?1 AND retired = 0",
            rusqlite::params![intent_id, state as i64],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn set_rename_aside_state(
        &mut self,
        intent_id: i64,
        state: RenameAsideState,
    ) -> Result<(), StoreError> {
        let updated = self.conn.execute(
            "UPDATE write_intents SET rename_aside_state = ?2
             WHERE intent_id = ?1 AND retired = 0",
            rusqlite::params![intent_id, state as i64],
        )?;
        if updated != 1 {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "intent retired during rename-aside transition".to_owned(),
            });
        }
        Ok(())
    }

    fn finish_rename_aside_terminal(
        &mut self,
        intent_id: i64,
        state: RenameAsideState,
        restored: bool,
    ) -> Result<(), StoreError> {
        let transaction = self.read.conn.transaction()?;
        if restored {
            transaction.execute(
                "UPDATE displaced SET restored = 1
                 WHERE intent_id = ?1 AND ordinal = 0",
                [intent_id],
            )?;
        }
        let retire = matches!(
            state,
            RenameAsideState::ProposedInstalledDurable | RenameAsideState::PreimageRestoredDurable
        );
        let terminal_success = retire.then_some(!restored).map(i64::from);
        let updated = transaction.execute(
            "UPDATE write_intents
             SET rename_aside_state = ?2, terminal_success = ?3, retired = ?4
             WHERE intent_id = ?1 AND retired = 0",
            rusqlite::params![intent_id, state as i64, terminal_success, i64::from(retire)],
        )?;
        if updated != 1 {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "intent retired during terminal rename-aside transition".to_owned(),
            });
        }
        transaction.commit()?;
        Ok(())
    }

    fn retire_intent_with_outcome(
        &mut self,
        intent_id: i64,
        success: bool,
    ) -> Result<(), StoreError> {
        let n = self.conn.execute(
            "UPDATE write_intents SET terminal_success = ?2, retired = 1
             WHERE intent_id = ?1 AND retired = 0",
            rusqlite::params![intent_id, i64::from(success)],
        )?;
        if n == 0 {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "no unretired intent with this id".to_owned(),
            });
        }
        Ok(())
    }

    /// Journaled retention expiry. Rows remain as audit history naming
    /// exactly what destruction removed.
    pub fn sweep_displaced(&mut self, now_secs: i64) -> Result<usize, StoreError> {
        let cutoff = now_secs - (self.config.displaced_retention_days as i64) * 86_400;
        let expired = self
            .quarantined_entries()?
            .into_iter()
            .filter(|e| e.quarantined_at < cutoff)
            .collect::<Vec<_>>();
        self.clean_displaced_entries(&expired, now_secs, "retention-expired")
    }

    /// Explicit `doctor clean`: destroy every retained displacement while
    /// keeping its row as permanent named audit history.
    pub fn clean_all_displaced(&mut self, now_secs: i64) -> Result<usize, StoreError> {
        let entries = self.quarantined_entries()?;
        self.clean_displaced_entries(&entries, now_secs, "doctor-clean")
    }

    fn clean_displaced_entries(
        &mut self,
        entries: &[DisplacedEntry],
        now_secs: i64,
        reason: &str,
    ) -> Result<usize, StoreError> {
        let mut touched_dirs = std::collections::BTreeSet::new();
        for entry in entries {
            match std::fs::remove_file(&entry.path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(StoreError::Io {
                        path: entry.path.clone(),
                        source,
                    })
                }
            }
            self.conn.execute(
                "UPDATE displaced SET cleaned_at = ?2, cleanup_reason = ?3
                 WHERE displacement_id = ?1",
                rusqlite::params![entry.displacement_id, now_secs, reason],
            )?;
            if let Some(parent) = entry.path.parent() {
                touched_dirs.insert(parent.to_path_buf());
            }
        }
        for dir in touched_dirs {
            crate::cas::manifest::fsync_dir(&dir)?;
        }
        Ok(entries.len())
    }
}

impl StoreReader {

    pub fn unfinished_publication_groups(&self) -> Result<Vec<PublicationGroup>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT group_id, kind, basis, state FROM publication_groups
             WHERE retired = 0 ORDER BY group_id",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut groups = Vec::with_capacity(rows.len());
        for (group_id, raw_kind, basis, raw_state) in rows {
            let kind = match raw_kind {
                1 => PublicationGroupKind::LineageCreate,
                2 => PublicationGroupKind::LineageDuplicate,
                3 => PublicationGroupKind::AuthoringWrite,
                4 => PublicationGroupKind::Import,
                5 => PublicationGroupKind::DiskMigration,
                6 => PublicationGroupKind::Codegen,
                7 => PublicationGroupKind::SchemaRepair,
                8 => PublicationGroupKind::SchemaTransition,
                _ => {
                    return Err(StoreError::BadIntent {
                        intent_id: group_id,
                        detail: format!("publication group has unknown kind {raw_kind}"),
                    })
                }
            };
            let state = match raw_state {
                0 => PublicationGroupState::Unarmed,
                1 => PublicationGroupState::Armed,
                _ => {
                    return Err(StoreError::BadIntent {
                        intent_id: group_id,
                        detail: format!("publication group has unknown state {raw_state}"),
                    })
                }
            };
            let mut children = self.conn.prepare(
                "SELECT intent_id FROM publication_group_children
                 WHERE group_id = ?1 ORDER BY ordinal",
            )?;
            let child_intents = children
                .query_map([group_id], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;
            if child_intents.is_empty() {
                return Err(StoreError::BadIntent {
                    intent_id: group_id,
                    detail: "publication group has no child intents".into(),
                });
            }
            groups.push(PublicationGroup {
                group_id,
                kind,
                basis,
                state,
                child_intents,
            });
        }
        Ok(groups)
    }

    pub fn publication_group_child_results(
        &self,
        group_id: i64,
    ) -> Result<Vec<PublicationChildResult>, StoreError> {
        let mut statement = self.conn.prepare(
            "SELECT w.intent_id, w.target_path, w.terminal_success
             FROM publication_group_children c
             JOIN write_intents w ON w.intent_id = c.intent_id
             WHERE c.group_id = ?1 ORDER BY c.ordinal",
        )?;
        let rows = statement
            .query_map([group_id], |row| {
                Ok(PublicationChildResult {
                    intent_id: row.get(0)?,
                    target_path: row.get(1)?,
                    terminal_success: row.get::<_, Option<i64>>(2)?.map(|value| value != 0),
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into);
        rows
    }

    fn ensure_intent_group_armed(&self, intent_id: i64) -> Result<(), StoreError> {
        let state: Option<i64> = self
            .conn
            .query_row(
                "SELECT g.state FROM publication_group_children c
                 JOIN publication_groups g ON g.group_id = c.group_id
                 WHERE c.intent_id = ?1 AND g.retired = 0",
                [intent_id],
                |row| row.get(0),
            )
            .optional()?;
        if state != Some(PublicationGroupState::Armed as i64) {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "publication child has no unfinished armed parent group".into(),
            });
        }
        Ok(())
    }

    #[cfg(unix)]
    fn native_filesystem_for_intent(
        &self,
        intent_id: i64,
        quarantine_dir: Option<&Path>,
    ) -> Result<NativeRenameAsideFs, StoreError> {
        let paths: (String, String, String) = self.conn.query_row(
            "SELECT target_path, temp_path, conflict_path
             FROM write_intents WHERE intent_id = ?1",
            [intent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        NativeRenameAsideFs::for_paths(
            [
                PathBuf::from(paths.0),
                PathBuf::from(paths.1),
                PathBuf::from(paths.2),
            ]
            .iter()
            .filter(|path| !path.as_os_str().is_empty()),
            quarantine_dir,
        )
    }

    fn load_rename_aside_intent(&self, intent_id: i64) -> Result<RenameAsideIntent, StoreError> {
        let raw = self
            .conn
            .query_row(
                "SELECT target_path, temp_path, conflict_path, pre_image_hash,
                        proposed_hash, rename_aside_state
                 FROM write_intents WHERE intent_id = ?1 AND retired = 0",
                [intent_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((target, temp, conflict, preimage, proposed, state)) = raw else {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "no unretired intent with this id".to_owned(),
            });
        };
        let expected_preimage = preimage.ok_or_else(|| StoreError::BadIntent {
            intent_id,
            detail: "rename-aside replacement requires an expected pre-image".to_owned(),
        })?;
        Ok(RenameAsideIntent {
            target: PathBuf::from(target),
            temp: PathBuf::from(temp),
            conflict: PathBuf::from(conflict),
            expected_preimage: ContentHash(blob32(expected_preimage)),
            proposed: ContentHash(blob32(proposed)),
            state: RenameAsideState::from_db(intent_id, state)?,
        })
    }

    fn reserved_aside_path(&self, quarantine_dir: &Path, intent_id: i64) -> PathBuf {
        quarantine_dir.join(format!("intent-{}-{intent_id}", self.instance_id()))
    }

    fn available_conflict_path<F: JournalFilesystem + ?Sized>(
        &self,
        fs: &mut F,
        intent_id: i64,
        base: &Path,
    ) -> Result<PathBuf, StoreError> {
        for suffix in 0..1024u32 {
            let candidate = if suffix == 0 {
                base.to_path_buf()
            } else {
                let mut name = base.as_os_str().to_os_string();
                name.push(format!(".intent-{intent_id}-{suffix}"));
                PathBuf::from(name)
            };
            let journaled: Option<i64> = self
                .conn
                .query_row(
                    "SELECT 1 FROM displaced WHERE quarantine_path = ?1",
                    [candidate.to_string_lossy().as_ref()],
                    |row| row.get(0),
                )
                .optional()?;
            if journaled.is_none() && fs.read(&candidate)?.is_none() {
                return Ok(candidate);
            }
        }
        Err(StoreError::BadIntent {
            intent_id,
            detail: "could not allocate a unique no-replace conflict path".to_owned(),
        })
    }

    fn displacement_hash(&self, intent_id: i64, ordinal: u32) -> Result<ContentHash, StoreError> {
        let bytes = self
            .conn
            .query_row(
                "SELECT content_hash FROM displaced WHERE intent_id = ?1 AND ordinal = ?2",
                rusqlite::params![intent_id, ordinal],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::BadIntent {
                intent_id,
                detail: format!("missing displacement {ordinal}"),
            })?;
        Ok(ContentHash(blob32(bytes)))
    }

    fn displacement_path(&self, intent_id: i64, ordinal: u32) -> Result<PathBuf, StoreError> {
        self.displacement_path_optional(intent_id, ordinal)?
            .ok_or_else(|| StoreError::BadIntent {
                intent_id,
                detail: format!("missing displacement {ordinal} path"),
            })
    }

    fn displacement_path_optional(
        &self,
        intent_id: i64,
        ordinal: u32,
    ) -> Result<Option<PathBuf>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT quarantine_path FROM displaced WHERE intent_id = ?1 AND ordinal = ?2",
                rusqlite::params![intent_id, ordinal],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(PathBuf::from))
    }

    fn load_intent_paths(
        &self,
        intent_id: i64,
    ) -> Result<(PathBuf, PathBuf, Option<ContentHash>), StoreError> {
        self.conn
            .query_row(
                "SELECT target_path, temp_path, pre_image_hash
                 FROM write_intents WHERE intent_id = ?1 AND retired = 0",
                [intent_id],
                |row| {
                    Ok((
                        PathBuf::from(row.get::<_, String>(0)?),
                        PathBuf::from(row.get::<_, String>(1)?),
                        row.get::<_, Option<Vec<u8>>>(2)?
                            .map(|bytes| ContentHash(blob32(bytes))),
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::BadIntent {
                intent_id,
                detail: "no unretired intent with this id".into(),
            })
    }

    fn intent_proposed_hash(&self, intent_id: i64) -> Result<ContentHash, StoreError> {
        self.conn
            .query_row(
                "SELECT proposed_hash FROM write_intents WHERE intent_id = ?1 AND retired = 0",
                [intent_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(|bytes| ContentHash(blob32(bytes)))
            .ok_or_else(|| StoreError::BadIntent {
                intent_id,
                detail: "no unretired intent with this id".into(),
            })
    }

    pub fn unretired_intents(&self) -> Result<Vec<WriteIntent>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT intent_id, target_path, temp_path, conflict_path,
                    pre_image_hash, proposed_hash, rename_aside_state,
                    terminal_success, retired
             FROM write_intents WHERE retired = 0 ORDER BY intent_id",
        )?;
        let raw = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, Option<Vec<u8>>>(4)?,
                    r.get::<_, Vec<u8>>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, Option<i64>>(7)?.map(|value| value != 0),
                    r.get::<_, i64>(8)? != 0,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::with_capacity(raw.len());
        for (id, target, temp, conflict, pre, proposed, state, terminal_success, retired) in raw {
            let mut paths = self.conn.prepare(
                "SELECT quarantine_path FROM displaced WHERE intent_id = ?1 ORDER BY ordinal",
            )?;
            let quarantine_paths = paths
                .query_map([id], |r| r.get::<_, String>(0))?
                .map(|r| r.map(PathBuf::from))
                .collect::<Result<Vec<_>, _>>()?;
            out.push(WriteIntent {
                intent_id: id,
                target_path: target,
                temp_path: temp,
                conflict_path: conflict,
                pre_image_hash: pre.map(|b| ContentHash(blob32(b))),
                proposed_hash: ContentHash(blob32(proposed)),
                quarantine_paths,
                rename_aside_state: RenameAsideState::from_db(id, state)?,
                terminal_success,
                retired,
            });
        }
        Ok(out)
    }

    /// Remove only a retired non-codegen proposal that still has exactly the
    /// journaled proposed bytes. Unknown or edited files are left untouched.
    #[cfg(unix)]
    pub fn cleanup_retired_non_codegen_proposal_temps(&self) -> Result<usize, StoreError> {
        let plans = self.retired_proposal_temps(false)?;
        let mut cleaned = 0;
        for (_, path, proposed) in plans {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            match std::fs::symlink_metadata(parent) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(StoreError::Io {
                        path: parent.to_path_buf(),
                        source,
                    })
                }
            }
            let mut filesystem = NativeRenameAsideFs::for_paths(std::iter::once(&path), None)?;
            cleaned += usize::from(clean_retired_proposal_temp(
                &mut filesystem,
                &path,
                proposed,
            )?);
        }
        Ok(cleaned)
    }

    /// Codegen proposal paths are relative to independently validated output
    /// authority, so startup cleanup uses that same filesystem capability.
    pub fn cleanup_retired_codegen_proposal_temps_with_filesystem(
        &self,
        filesystem: &mut dyn JournalFilesystem,
    ) -> Result<usize, StoreError> {
        let plans = self.retired_proposal_temps(true)?;
        let mut cleaned = 0;
        for (_, path, proposed) in plans {
            cleaned += usize::from(clean_retired_proposal_temp(filesystem, &path, proposed)?);
        }
        Ok(cleaned)
    }

    fn retired_proposal_temps(
        &self,
        codegen: bool,
    ) -> Result<Vec<(i64, PathBuf, ContentHash)>, StoreError> {
        let comparison = if codegen { "=" } else { "<>" };
        let sql = format!(
            "SELECT w.intent_id, w.temp_path, w.proposed_hash
             FROM write_intents w
             JOIN publication_group_children c ON c.intent_id = w.intent_id
             JOIN publication_groups g ON g.group_id = c.group_id
             WHERE w.retired = 1 AND w.temp_path <> '' AND g.kind {comparison} ?1
             ORDER BY w.intent_id"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let plans = stmt
            .query_map([PublicationGroupKind::Codegen as i64], |row| {
                Ok((
                    row.get(0)?,
                    PathBuf::from(row.get::<_, String>(1)?),
                    ContentHash(blob32(row.get(2)?)),
                ))
            })?
            .collect::<Result<_, _>>()?;
        Ok(plans)
    }

    pub fn displacement_history(&self) -> Result<Vec<DisplacedEntry>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT displacement_id, intent_id, ordinal, content_hash, origin_path,
                    quarantine_path, quarantined_at, restored, cleaned_at, cleanup_reason
             FROM displaced ORDER BY displacement_id",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(DisplacedEntry {
                    displacement_id: r.get(0)?,
                    intent_id: r.get(1)?,
                    ordinal: r.get(2)?,
                    content_hash: ContentHash(blob32(r.get(3)?)),
                    origin_path: r.get(4)?,
                    path: PathBuf::from(r.get::<_, String>(5)?),
                    quarantined_at: r.get(6)?,
                    restored: r.get::<_, i64>(7)? != 0,
                    cleaned_at: r.get(8)?,
                    cleanup_reason: r.get(9)?,
                })
            })?
            .collect::<Result<_, _>>()
            .map_err(Into::into);
        rows
    }

    pub fn quarantined_entries(&self) -> Result<Vec<DisplacedEntry>, StoreError> {
        Ok(self
            .displacement_history()?
            .into_iter()
            .filter(|entry| entry.cleaned_at.is_none())
            .collect())
    }

    pub fn verify_quarantine(&self) -> Result<Vec<RecoveredEdit>, StoreError> {
        let mut diagnostics = Vec::new();
        for entry in self.quarantined_entries()? {
            let expected = if entry.ordinal == 0 {
                self.conn
                    .query_row(
                        "SELECT pre_image_hash FROM write_intents WHERE intent_id = ?1",
                        [entry.intent_id],
                        |row| row.get::<_, Option<Vec<u8>>>(0),
                    )
                    .optional()?
                    .flatten()
                    .map(|bytes| ContentHash(blob32(bytes)))
                    .unwrap_or(entry.content_hash)
            } else {
                entry.content_hash
            };
            let actual = match std::fs::read(&entry.path) {
                Ok(bytes) => Some(ContentHash(*blake3::hash(&bytes).as_bytes())),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(source) => {
                    return Err(StoreError::Io {
                        path: entry.path.clone(),
                        source,
                    })
                }
            };
            if actual != Some(expected) {
                diagnostics.push(RecoveredEdit {
                    origin_path: entry.origin_path,
                    expected,
                    actual,
                    quarantine_path: entry.path,
                });
            }
        }
        Ok(diagnostics)
    }
}

fn clean_retired_proposal_temp<F: JournalFilesystem + ?Sized>(
    fs: &mut F,
    path: &Path,
    proposed: ContentHash,
) -> Result<bool, StoreError> {
    if read_hash(fs, path)? != Some(proposed) {
        return Ok(false);
    }
    fs.sync_file(path)?;
    fs.remove_daemon_temp(path)?;
    fs.sync_dir(path.parent().unwrap_or_else(|| Path::new(".")))?;
    Ok(true)
}

fn read_hash<F: JournalFilesystem + ?Sized>(
    fs: &mut F,
    path: &Path,
) -> Result<Option<ContentHash>, StoreError> {
    Ok(fs
        .read(path)?
        .map(|bytes| ContentHash(*blake3::hash(&bytes).as_bytes())))
}

fn existing_hash<F: JournalFilesystem + ?Sized>(
    fs: &mut F,
    intent_id: i64,
    path: &Path,
    description: &str,
) -> Result<ContentHash, StoreError> {
    read_hash(fs, path)?.ok_or_else(|| StoreError::BadIntent {
        intent_id,
        detail: format!("{description} is missing at {}", path.display()),
    })
}

fn require_hash<F: JournalFilesystem + ?Sized>(
    fs: &mut F,
    intent_id: i64,
    path: &Path,
    expected: ContentHash,
    description: &str,
) -> Result<(), StoreError> {
    let actual = existing_hash(fs, intent_id, path, description)?;
    if actual != expected {
        return Err(StoreError::BadIntent {
            intent_id,
            detail: format!(
                "{description} hash changed: expected {}, got {}",
                hash_hex(expected),
                hash_hex(actual)
            ),
        });
    }
    Ok(())
}

fn hash_hex(hash: ContentHash) -> String {
    hash.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sync_move_dirs<F: JournalFilesystem + ?Sized>(
    fs: &mut F,
    source: &Path,
    destination: &Path,
) -> Result<(), StoreError> {
    let source_parent = source.parent().unwrap_or_else(|| Path::new("."));
    let destination_parent = destination.parent().unwrap_or_else(|| Path::new("."));
    fs.sync_dir(source_parent)?;
    if destination_parent != source_parent {
        fs.sync_dir(destination_parent)?;
    }
    Ok(())
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectoryIdentity {
    canonical: PathBuf,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

/// Native authored-file authority. The journal records trusted workspace
/// paths, then this adapter pins every immediate directory used by the
/// intent. Each operation revalidates that directory and rejects symlinked
/// or non-regular entries immediately before touching it.
#[cfg(unix)]
struct NativeRenameAsideFs {
    directories: BTreeMap<PathBuf, DirectoryIdentity>,
}

#[cfg(unix)]
impl NativeRenameAsideFs {
    fn for_paths<'a>(
        paths: impl IntoIterator<Item = &'a PathBuf>,
        quarantine_dir: Option<&Path>,
    ) -> Result<Self, StoreError> {
        let mut this = Self {
            directories: BTreeMap::new(),
        };
        for path in paths {
            this.pin_dir(path.parent().unwrap_or_else(|| Path::new(".")))?;
        }
        if let Some(quarantine_dir) = quarantine_dir {
            this.pin_dir(quarantine_dir.parent().unwrap_or_else(|| Path::new(".")))?;
            if quarantine_dir.exists() {
                this.pin_dir(quarantine_dir)?;
            }
        }
        Ok(this)
    }

    fn pin_dir(&mut self, path: &Path) -> Result<(), StoreError> {
        let metadata = std::fs::symlink_metadata(path).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(invalid_journal_path(
                path,
                "journal directory is not a real directory",
            ));
        }
        let canonical = std::fs::canonicalize(path).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        self.directories.insert(
            path.to_path_buf(),
            DirectoryIdentity {
                canonical,
                device: metadata.dev(),
                inode: metadata.ino(),
            },
        );
        Ok(())
    }

    fn validate_dir(&self, path: &Path) -> Result<&DirectoryIdentity, StoreError> {
        let expected = self.directories.get(path).ok_or_else(|| {
            invalid_journal_path(path, "journal operation escaped its pinned directories")
        })?;
        let metadata = std::fs::symlink_metadata(path).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let canonical = std::fs::canonicalize(path).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if metadata.file_type().is_symlink()
            || !metadata.is_dir()
            || metadata.dev() != expected.device
            || metadata.ino() != expected.inode
            || canonical != expected.canonical
        {
            return Err(invalid_journal_path(
                path,
                "journal directory identity changed",
            ));
        }
        Ok(expected)
    }

    fn validate_parent(&self, path: &Path) -> Result<&DirectoryIdentity, StoreError> {
        self.validate_dir(path.parent().unwrap_or_else(|| Path::new(".")))
    }

    fn file_identity(&self, path: &Path) -> Result<Option<FileIdentity>, StoreError> {
        self.validate_parent(path)?;
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(StoreError::Io {
                    path: path.to_path_buf(),
                    source,
                })
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(invalid_journal_path(
                path,
                "journal entry is not a real regular file",
            ));
        }
        Ok(Some(FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        }))
    }

    fn required_file_identity(&self, path: &Path) -> Result<FileIdentity, StoreError> {
        self.file_identity(path)?.ok_or_else(|| StoreError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "journal file is missing"),
        })
    }

    fn verify_file_identity(&self, path: &Path, expected: FileIdentity) -> Result<(), StoreError> {
        if self.file_identity(path)? != Some(expected) {
            return Err(invalid_journal_path(
                path,
                "journal file identity changed during operation",
            ));
        }
        Ok(())
    }
}

#[cfg(unix)]
fn invalid_journal_path(path: &Path, detail: &'static str) -> StoreError {
    StoreError::Io {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, detail),
    }
}

#[cfg(unix)]
impl JournalFilesystem for NativeRenameAsideFs {
    fn read(&mut self, path: &Path) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(expected) = self.file_identity(path)? else {
            return Ok(None);
        };
        let mut file = std::fs::File::open(path).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let before = file.metadata().map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if !before.is_file() || before.dev() != expected.device || before.ino() != expected.inode {
            return Err(invalid_journal_path(
                path,
                "journal file changed while it was opened",
            ));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| StoreError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        let after = file.metadata().map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        self.verify_file_identity(path, expected)?;
        if before.len() != after.len()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || after.len() != bytes.len() as u64
        {
            return Err(invalid_journal_path(
                path,
                "journal file changed while it was read",
            ));
        }
        Ok(Some(bytes))
    }

    fn create_dir_all(&mut self, path: &Path) -> Result<(), StoreError> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        self.validate_dir(parent)?;
        std::fs::create_dir_all(path).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        self.validate_dir(parent)?;
        self.pin_dir(path)
    }

    fn sync_file(&mut self, path: &Path) -> Result<(), StoreError> {
        let expected = self.required_file_identity(path)?;
        let file = std::fs::File::open(path).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let metadata = file.metadata().map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if !metadata.is_file()
            || metadata.dev() != expected.device
            || metadata.ino() != expected.inode
        {
            return Err(invalid_journal_path(
                path,
                "journal file changed while it was opened for sync",
            ));
        }
        file.sync_all().map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        self.verify_file_identity(path, expected)
    }

    fn sync_dir(&mut self, path: &Path) -> Result<(), StoreError> {
        self.validate_dir(path)?;
        crate::cas::manifest::fsync_dir(path)
    }

    fn ensure_same_filesystem(
        &mut self,
        source: &Path,
        destination_dir: &Path,
    ) -> Result<(), StoreError> {
        let source_identity = self.required_file_identity(source)?;
        let destination_identity = self.validate_dir(destination_dir)?;
        if source_identity.device != destination_identity.device {
            return Err(StoreError::CrossFilesystemQuarantine {
                source: source.to_path_buf(),
                quarantine: destination_dir.to_path_buf(),
            });
        }
        Ok(())
    }

    fn rename_target_to_reserved(
        &mut self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), NoReplaceMoveError> {
        let source_identity = self
            .required_file_identity(source)
            .map_err(NoReplaceMoveError::Other)?;
        self.validate_parent(destination)
            .map_err(NoReplaceMoveError::Other)?;
        if self
            .file_identity(destination)
            .map_err(NoReplaceMoveError::Other)?
            .is_some()
        {
            return Err(NoReplaceMoveError::DestinationExists);
        }
        self.verify_file_identity(source, source_identity)
            .map_err(NoReplaceMoveError::Other)?;
        self.validate_parent(destination)
            .map_err(NoReplaceMoveError::Other)?;
        std::fs::rename(source, destination).map_err(|source| {
            NoReplaceMoveError::Other(StoreError::Io {
                path: destination.to_path_buf(),
                source,
            })
        })?;
        self.validate_parent(source)
            .map_err(NoReplaceMoveError::Other)?;
        self.validate_parent(destination)
            .map_err(NoReplaceMoveError::Other)?;
        // An editor may have atomically exchanged the target after the
        // pre-image hash was read. The move still preserved that newer real
        // file at the journaled aside, so let the state machine hash and
        // restore it instead of converting a recoverable edit into an I/O
        // wedge.
        self.required_file_identity(destination)
            .map_err(NoReplaceMoveError::Other)?;
        Ok(())
    }

    fn install_temp_no_replace(
        &mut self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), NoReplaceMoveError> {
        let source_identity = self
            .required_file_identity(source)
            .map_err(NoReplaceMoveError::Other)?;
        self.validate_parent(destination)
            .map_err(NoReplaceMoveError::Other)?;
        match std::fs::hard_link(source, destination) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(NoReplaceMoveError::DestinationExists)
            }
            Err(source) => {
                return Err(NoReplaceMoveError::Other(StoreError::Io {
                    path: destination.to_path_buf(),
                    source,
                }))
            }
        }
        self.validate_parent(destination)
            .map_err(NoReplaceMoveError::Other)?;
        self.verify_file_identity(destination, source_identity)
            .map_err(NoReplaceMoveError::Other)?;
        self.verify_file_identity(source, source_identity)
            .map_err(NoReplaceMoveError::Other)?;
        let source_path = source.to_path_buf();
        std::fs::remove_file(source).map_err(|source| {
            NoReplaceMoveError::Other(StoreError::Io {
                path: source_path,
                source,
            })
        })
    }

    fn remove_daemon_temp(&mut self, path: &Path) -> Result<(), StoreError> {
        let identity = self.required_file_identity(path)?;
        self.verify_file_identity(path, identity)?;
        std::fs::remove_file(path).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        self.validate_parent(path)?;
        Ok(())
    }

    fn restore_retained_no_replace(
        &mut self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), NoReplaceMoveError> {
        let source_identity = self
            .required_file_identity(source)
            .map_err(NoReplaceMoveError::Other)?;
        self.validate_parent(destination)
            .map_err(NoReplaceMoveError::Other)?;
        std::fs::hard_link(source, destination).map_err(|source| {
            if source.kind() == std::io::ErrorKind::AlreadyExists {
                NoReplaceMoveError::DestinationExists
            } else {
                NoReplaceMoveError::Other(StoreError::Io {
                    path: destination.to_path_buf(),
                    source,
                })
            }
        })?;
        self.validate_parent(destination)
            .map_err(NoReplaceMoveError::Other)?;
        self.verify_file_identity(source, source_identity)
            .map_err(NoReplaceMoveError::Other)?;
        self.verify_file_identity(destination, source_identity)
            .map_err(NoReplaceMoveError::Other)
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod rename_aside_tests {
    use std::collections::BTreeMap;
    use std::io;

    use super::*;
    use crate::config::StoreConfig;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum FsEvent {
        MoveNoReplace(PathBuf, PathBuf),
        SyncFile(PathBuf),
        SyncDir(PathBuf),
    }

    #[derive(Default)]
    struct FakeFs {
        files: BTreeMap<PathBuf, Vec<u8>>,
        before_move: BTreeMap<(PathBuf, PathBuf), (PathBuf, Vec<u8>)>,
        after_reserved_move: BTreeMap<(PathBuf, PathBuf), (PathBuf, Vec<u8>)>,
        fail_sync_after_move: Option<(PathBuf, PathBuf)>,
        fail_after_temp_link: Option<(PathBuf, PathBuf)>,
        fail_next_sync: bool,
        events: Vec<FsEvent>,
    }

    impl FakeFs {
        fn put(&mut self, path: impl Into<PathBuf>, bytes: &[u8]) {
            self.files.insert(path.into(), bytes.to_vec());
        }

        fn inject_before_move(
            &mut self,
            source: impl Into<PathBuf>,
            destination: impl Into<PathBuf>,
            appeared_path: impl Into<PathBuf>,
            bytes: &[u8],
        ) {
            self.before_move.insert(
                (source.into(), destination.into()),
                (appeared_path.into(), bytes.to_vec()),
            );
        }

        fn fail_sync_after(&mut self, source: impl Into<PathBuf>, destination: impl Into<PathBuf>) {
            self.fail_sync_after_move = Some((source.into(), destination.into()));
        }

        fn fail_after_temp_link(
            &mut self,
            source: impl Into<PathBuf>,
            destination: impl Into<PathBuf>,
        ) {
            self.fail_after_temp_link = Some((source.into(), destination.into()));
        }

        fn inject_after_reserved_move(
            &mut self,
            source: impl Into<PathBuf>,
            destination: impl Into<PathBuf>,
            appeared_path: impl Into<PathBuf>,
            bytes: &[u8],
        ) {
            self.after_reserved_move.insert(
                (source.into(), destination.into()),
                (appeared_path.into(), bytes.to_vec()),
            );
        }

        fn bytes(&self, path: impl AsRef<Path>) -> Option<&[u8]> {
            self.files.get(path.as_ref()).map(Vec::as_slice)
        }

        fn apply_move(
            &mut self,
            source: &Path,
            destination: &Path,
            retain_source: bool,
        ) -> Result<(), NoReplaceMoveError> {
            self.events.push(FsEvent::MoveNoReplace(
                source.to_path_buf(),
                destination.to_path_buf(),
            ));
            if let Some((appeared_path, bytes)) = self
                .before_move
                .remove(&(source.to_path_buf(), destination.to_path_buf()))
            {
                self.files.insert(appeared_path, bytes);
            }
            if self.files.contains_key(destination) {
                return Err(NoReplaceMoveError::DestinationExists);
            }
            let Some(bytes) = self.files.get(source).cloned() else {
                return Err(NoReplaceMoveError::Other(StoreError::Io {
                    path: source.to_path_buf(),
                    source: io::Error::new(io::ErrorKind::NotFound, "source missing"),
                }));
            };
            if !retain_source {
                self.files.remove(source);
            }
            self.files.insert(destination.to_path_buf(), bytes);
            if self.fail_sync_after_move.as_ref()
                == Some(&(source.to_path_buf(), destination.to_path_buf()))
            {
                self.fail_sync_after_move = None;
                self.fail_next_sync = true;
            }
            Ok(())
        }
    }

    impl JournalFilesystem for FakeFs {
        fn read(&mut self, path: &Path) -> Result<Option<Vec<u8>>, StoreError> {
            Ok(self.files.get(path).cloned())
        }

        fn create_dir_all(&mut self, _path: &Path) -> Result<(), StoreError> {
            Ok(())
        }

        fn sync_file(&mut self, path: &Path) -> Result<(), StoreError> {
            self.events.push(FsEvent::SyncFile(path.to_path_buf()));
            Ok(())
        }

        fn sync_dir(&mut self, path: &Path) -> Result<(), StoreError> {
            self.events.push(FsEvent::SyncDir(path.to_path_buf()));
            if self.fail_next_sync {
                self.fail_next_sync = false;
                return Err(StoreError::Io {
                    path: path.to_path_buf(),
                    source: io::Error::other("injected crash boundary"),
                });
            }
            Ok(())
        }

        fn ensure_same_filesystem(
            &mut self,
            _source: &Path,
            _destination_dir: &Path,
        ) -> Result<(), StoreError> {
            Ok(())
        }

        fn rename_target_to_reserved(
            &mut self,
            source: &Path,
            destination: &Path,
        ) -> Result<(), NoReplaceMoveError> {
            self.apply_move(source, destination, false)?;
            if let Some((appeared_path, bytes)) = self
                .after_reserved_move
                .remove(&(source.to_path_buf(), destination.to_path_buf()))
            {
                self.files.insert(appeared_path, bytes);
            }
            Ok(())
        }

        fn install_temp_no_replace(
            &mut self,
            source: &Path,
            destination: &Path,
        ) -> Result<(), NoReplaceMoveError> {
            if self.fail_after_temp_link.as_ref()
                == Some(&(source.to_path_buf(), destination.to_path_buf()))
            {
                self.fail_after_temp_link = None;
                self.apply_move(source, destination, true)?;
                return Err(NoReplaceMoveError::Other(StoreError::Io {
                    path: source.to_path_buf(),
                    source: io::Error::other("injected crash after temp link"),
                }));
            }
            self.apply_move(source, destination, false)
        }

        fn remove_daemon_temp(&mut self, path: &Path) -> Result<(), StoreError> {
            self.files
                .remove(path)
                .map(|_| ())
                .ok_or_else(|| StoreError::Io {
                    path: path.to_path_buf(),
                    source: io::Error::new(io::ErrorKind::NotFound, "temp missing"),
                })
        }

        fn restore_retained_no_replace(
            &mut self,
            source: &Path,
            destination: &Path,
        ) -> Result<(), NoReplaceMoveError> {
            self.apply_move(source, destination, true)
        }
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        store: Store,
        intent_id: i64,
        target: PathBuf,
        temp: PathBuf,
        conflict: PathBuf,
        quarantine: PathBuf,
        aside: PathBuf,
        fs: FakeFs,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        let target = PathBuf::from("root/asset.bundle");
        let temp = PathBuf::from("root/.asset.bundle.tmp");
        let conflict = PathBuf::from("root/asset.bundle.conflict");
        let quarantine = PathBuf::from("root/.quarantine");
        let group = store
            .record_publication_group(
                PublicationGroupKind::AuthoringWrite,
                b"test replacement",
                &[JournalIntentPlan {
                    target_path: target.to_string_lossy().into_owned(),
                    temp_path: temp.to_string_lossy().into_owned(),
                    conflict_path: conflict.to_string_lossy().into_owned(),
                    pre_image_hash: Some(ContentHash(*blake3::hash(b"preimage").as_bytes())),
                    proposed_hash: ContentHash(*blake3::hash(b"proposed").as_bytes()),
                }],
            )
            .unwrap();
        store.arm_publication_group(group.group_id).unwrap();
        let intent_id = group.child_intents[0];
        let aside = store.reserved_aside_path(&quarantine, intent_id);
        let mut fs = FakeFs::default();
        fs.put(&target, b"preimage");
        fs.put(&temp, b"proposed");
        Fixture {
            _dir: dir,
            store,
            intent_id,
            target,
            temp,
            conflict,
            quarantine,
            aside,
            fs,
        }
    }

    fn persisted_state(store: &Store, intent_id: i64) -> (RenameAsideState, bool) {
        let (state, retired): (i64, i64) = store
            .conn
            .query_row(
                "SELECT rename_aside_state, retired FROM write_intents WHERE intent_id = ?1",
                [intent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        (
            RenameAsideState::from_db(intent_id, state).unwrap(),
            retired != 0,
        )
    }

    #[test]
    fn reappeared_target_is_conflict_preserved_and_never_overwritten() {
        let mut fixture = fixture();
        fixture.fs.inject_before_move(
            &fixture.temp,
            &fixture.target,
            &fixture.target,
            b"external save",
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::ConflictRestored);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"external save".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::PreimageRestoredDurable, true)
        );
        assert_eq!(fixture.store.displacement_history().unwrap().len(), 2);
    }

    #[test]
    fn preimage_drift_before_the_first_move_retires_without_displacing_the_edit() {
        let mut fixture = fixture();
        fixture.fs.put(&fixture.target, b"user edit");

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::RetryRequired);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"user edit".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );
        assert_eq!(fixture.fs.bytes(&fixture.aside), None);
        assert!(!fixture
            .fs
            .events
            .iter()
            .any(|event| matches!(event, FsEvent::MoveNoReplace(_, _))));
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::Prepared, true)
        );
    }

    #[test]
    fn target_exchange_between_hash_and_reserved_rename_restores_the_new_bytes() {
        let mut fixture = fixture();
        fixture.fs.inject_before_move(
            &fixture.target,
            &fixture.aside,
            &fixture.target,
            b"atomic editor exchange",
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::ConflictRestored);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"atomic editor exchange".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"atomic editor exchange".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::PreimageRestoredDurable, true)
        );
    }

    #[test]
    fn missing_prepared_target_is_terminally_retired() {
        let mut fixture = fixture();
        fixture.fs.files.remove(&fixture.target);

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::RetryRequired);
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::Prepared, true)
        );
    }

    #[test]
    fn occupied_reserved_aside_is_accounted_and_replanned() {
        let mut fixture = fixture();
        fixture
            .fs
            .put(&fixture.aside, b"retained by an older store");

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::Installed);
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"retained by an older store".as_slice())
        );
        let history = fixture.store.displacement_history().unwrap();
        let collision = history.iter().find(|entry| entry.ordinal == 2).unwrap();
        let displaced = history.iter().find(|entry| entry.ordinal == 0).unwrap();
        assert_eq!(collision.path, fixture.aside);
        assert_ne!(displaced.path, fixture.aside);
        assert_eq!(
            fixture.fs.bytes(&displaced.path),
            Some(b"preimage".as_slice())
        );
    }

    #[test]
    fn reserved_collision_and_replacement_journal_roll_back_together() {
        let mut fixture = fixture();
        fixture
            .fs
            .put(&fixture.aside, b"retained by an older store");
        fixture
            .store
            .conn
            .execute_batch(
                "CREATE TRIGGER fail_ordinal_zero_insert
                 BEFORE INSERT ON displaced WHEN NEW.ordinal = 0
                 BEGIN
                     SELECT RAISE(ABORT, 'injected crash boundary');
                 END;",
            )
            .unwrap();

        assert!(fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .is_err());
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::Prepared, false)
        );
        let displacement_count: i64 = fixture
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM displaced WHERE intent_id = ?1",
                [fixture.intent_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(displacement_count, 0);

        fixture
            .store
            .conn
            .execute_batch("DROP TRIGGER fail_ordinal_zero_insert")
            .unwrap();
        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::Installed);
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"retained by an older store".as_slice())
        );
        let history = fixture.store.displacement_history().unwrap();
        assert_eq!(history.iter().filter(|entry| entry.ordinal == 0).count(), 1);
        assert_eq!(history.iter().filter(|entry| entry.ordinal == 2).count(), 1);
    }

    #[test]
    fn post_journal_ordinal_zero_collision_is_preserved_before_replanning() {
        let mut fixture = fixture();
        fixture.fs.inject_before_move(
            &fixture.target,
            &fixture.aside,
            &fixture.aside,
            b"foreign retained bytes",
        );
        fixture.fs.inject_before_move(
            &fixture.temp,
            &fixture.target,
            &fixture.target,
            b"external save",
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::ConflictRestored);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"external save".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"foreign retained bytes".as_slice())
        );
        let history = fixture.store.displacement_history().unwrap();
        let displaced = history.iter().find(|entry| entry.ordinal == 0).unwrap();
        let reappeared = history.iter().find(|entry| entry.ordinal == 1).unwrap();
        let collision = history.iter().find(|entry| entry.ordinal == 2).unwrap();
        assert_ne!(displaced.path, fixture.aside);
        assert_eq!(reappeared.path, fixture.conflict);
        assert_eq!(collision.path, fixture.aside);
        assert_eq!(
            fixture.fs.bytes(&displaced.path),
            Some(b"preimage".as_slice())
        );
    }

    #[test]
    fn atomic_save_after_reserved_rename_is_preserved_not_unlinked() {
        let mut fixture = fixture();
        fixture.fs.inject_after_reserved_move(
            &fixture.target,
            &fixture.aside,
            &fixture.target,
            b"atomic editor save",
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::ConflictRestored);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"atomic editor save".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );
    }

    #[test]
    fn second_reappearance_during_restore_stops_with_every_byte_recoverable() {
        let mut fixture = fixture();
        fixture.fs.inject_before_move(
            &fixture.temp,
            &fixture.target,
            &fixture.target,
            b"first external save",
        );
        fixture.fs.inject_before_move(
            &fixture.aside,
            &fixture.target,
            &fixture.target,
            b"second external save",
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::RetryRequired);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"second external save".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"first external save".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::ConflictPreservedDurable, false)
        );
    }

    #[test]
    fn conflict_path_collision_allocates_another_journaled_no_replace_name() {
        let mut fixture = fixture();
        fixture.fs.inject_before_move(
            &fixture.temp,
            &fixture.target,
            &fixture.target,
            b"external save",
        );
        fixture.fs.inject_before_move(
            &fixture.target,
            &fixture.conflict,
            &fixture.conflict,
            b"preexisting conflict bytes",
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();

        assert_eq!(outcome, RenameAsideOutcome::ConflictRestored);
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"preexisting conflict bytes".as_slice())
        );
        let retained = fixture.store.displacement_history().unwrap();
        let reappeared = retained.iter().find(|entry| entry.ordinal == 1).unwrap();
        let collision = retained.iter().find(|entry| entry.ordinal == 2).unwrap();
        assert_ne!(reappeared.path, fixture.conflict);
        assert_eq!(collision.path, fixture.conflict);
        assert_eq!(
            collision.content_hash,
            ContentHash(*blake3::hash(b"preexisting conflict bytes").as_bytes())
        );
        assert_eq!(
            fixture.fs.bytes(&reappeared.path),
            Some(b"external save".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );
        let collision_sync = fixture
            .fs
            .events
            .iter()
            .position(|event| event == &FsEvent::SyncFile(fixture.conflict.clone()))
            .unwrap();
        let replanned_move = fixture
            .fs
            .events
            .iter()
            .position(|event| {
                event == &FsEvent::MoveNoReplace(fixture.target.clone(), reappeared.path.clone())
            })
            .unwrap();
        assert!(collision_sync < replanned_move);
    }

    #[test]
    fn recovery_resumes_when_crash_precedes_aside_directory_sync() {
        let mut fixture = fixture();
        fixture.fs.fail_sync_after(&fixture.target, &fixture.aside);

        assert!(fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs,)
            .is_err());
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::AsideJournaled, false)
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();
        assert_eq!(outcome, RenameAsideOutcome::Installed);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"proposed".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::ProposedInstalledDurable, true)
        );
    }

    #[test]
    fn recovery_recognizes_install_after_preimage_verified_state() {
        let mut fixture = fixture();
        fixture.fs.fail_sync_after(&fixture.temp, &fixture.target);

        assert!(fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs,)
            .is_err());
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::PreimageVerifiedDurable, false)
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"proposed".as_slice())
        );
        assert_eq!(fixture.fs.bytes(&fixture.temp), None);
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();
        assert_eq!(outcome, RenameAsideOutcome::Installed);
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::ProposedInstalledDurable, true)
        );
    }

    #[test]
    fn recovery_cleans_a_temp_left_linked_after_proposal_install() {
        let mut fixture = fixture();
        fixture
            .fs
            .fail_after_temp_link(&fixture.temp, &fixture.target);

        assert!(fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .is_err());
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::PreimageVerifiedDurable, false)
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"proposed".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );

        assert_eq!(
            fixture
                .store
                .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs,)
                .unwrap(),
            RenameAsideOutcome::Installed
        );
        assert_eq!(fixture.fs.bytes(&fixture.temp), None);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"proposed".as_slice())
        );
    }

    #[test]
    fn recovery_resumes_when_crash_precedes_conflict_directory_sync() {
        let mut fixture = fixture();
        fixture.fs.inject_before_move(
            &fixture.temp,
            &fixture.target,
            &fixture.target,
            b"external save",
        );
        fixture
            .fs
            .fail_sync_after(&fixture.target, &fixture.conflict);

        assert!(fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs,)
            .is_err());
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::ConflictJournaled, false)
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"external save".as_slice())
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();
        assert_eq!(outcome, RenameAsideOutcome::ConflictRestored);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"external save".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );
    }

    #[test]
    fn recovery_recognizes_no_replace_restore_before_its_directory_sync() {
        let mut fixture = fixture();
        fixture.fs.inject_before_move(
            &fixture.temp,
            &fixture.target,
            &fixture.target,
            b"external save",
        );
        fixture.fs.fail_sync_after(&fixture.aside, &fixture.target);

        assert!(fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs,)
            .is_err());
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::ConflictPreservedDurable, false)
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.temp),
            Some(b"proposed".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"external save".as_slice())
        );

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();
        assert_eq!(outcome, RenameAsideOutcome::ConflictRestored);
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::PreimageRestoredDurable, true)
        );
    }

    #[test]
    fn recovery_uses_journaled_aside_path_not_a_new_quarantine_argument() {
        let mut fixture = fixture();
        fixture.fs.fail_sync_after(&fixture.target, &fixture.aside);
        assert!(fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs,)
            .is_err());

        let different_quarantine = PathBuf::from("root/.changed-quarantine");
        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &different_quarantine, &mut fixture.fs)
            .unwrap();
        assert_eq!(outcome, RenameAsideOutcome::Installed);
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );
        assert_eq!(
            fixture
                .fs
                .bytes(different_quarantine.join(format!("intent-{}", fixture.intent_id))),
            None
        );
    }

    #[test]
    fn recovery_resumes_when_aside_was_journaled_before_its_move() {
        let mut fixture = fixture();
        fixture
            .store
            .journal_rename_aside_displacement(
                fixture.intent_id,
                0,
                ContentHash(*blake3::hash(b"preimage").as_bytes()),
                &fixture.target,
                &fixture.aside,
            )
            .unwrap();

        let outcome = fixture
            .store
            .publish_rename_aside_with(
                fixture.intent_id,
                Path::new("root/.not-the-journaled-quarantine"),
                &mut fixture.fs,
            )
            .unwrap();
        assert_eq!(outcome, RenameAsideOutcome::Installed);
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"proposed".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.aside),
            Some(b"preimage".as_slice())
        );
    }

    #[test]
    fn conflict_move_is_flushed_and_classified_before_a_second_target_stops_recovery() {
        let mut fixture = fixture();
        fixture.fs.inject_before_move(
            &fixture.temp,
            &fixture.target,
            &fixture.target,
            b"first external save",
        );
        fixture
            .fs
            .fail_sync_after(&fixture.target, &fixture.conflict);
        assert!(fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs,)
            .is_err());
        fixture.fs.put(&fixture.target, b"second external save");
        fixture.fs.events.clear();

        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();
        assert_eq!(outcome, RenameAsideOutcome::RetryRequired);
        assert_eq!(
            persisted_state(&fixture.store, fixture.intent_id),
            (RenameAsideState::ConflictPreservedDurable, false)
        );
        assert!(fixture
            .fs
            .events
            .contains(&FsEvent::SyncFile(fixture.conflict.clone())));
        assert_eq!(
            fixture.fs.bytes(&fixture.target),
            Some(b"second external save".as_slice())
        );
        assert_eq!(
            fixture.fs.bytes(&fixture.conflict),
            Some(b"first external save".as_slice())
        );
    }

    #[test]
    fn moved_bytes_are_file_flushed_before_move_directories() {
        let mut fixture = fixture();
        let outcome = fixture
            .store
            .publish_rename_aside_with(fixture.intent_id, &fixture.quarantine, &mut fixture.fs)
            .unwrap();
        assert_eq!(outcome, RenameAsideOutcome::Installed);

        for (source, destination) in [
            (&fixture.target, &fixture.aside),
            (&fixture.temp, &fixture.target),
        ] {
            let moved = fixture
                .fs
                .events
                .iter()
                .position(|event| {
                    event == &FsEvent::MoveNoReplace(source.clone(), destination.clone())
                })
                .unwrap();
            let file_synced = fixture
                .fs
                .events
                .iter()
                .enumerate()
                .skip(moved + 1)
                .find(|(_, event)| event == &&FsEvent::SyncFile(destination.clone()))
                .map(|(index, _)| index)
                .unwrap();
            let dir_synced = fixture
                .fs
                .events
                .iter()
                .enumerate()
                .skip(moved + 1)
                .find(|(_, event)| {
                    event
                        == &&FsEvent::SyncDir(
                            destination
                                .parent()
                                .unwrap_or_else(|| Path::new("."))
                                .to_path_buf(),
                        )
                })
                .map(|(index, _)| index)
                .unwrap();
            assert!(file_synced < dir_synced);
        }
    }

    #[test]
    fn unfinished_deletion_recovery_accounts_for_match_and_mismatch() {
        let mut matching = fixture();
        matching
            .store
            .conn
            .execute(
                "UPDATE write_intents SET temp_path = '' WHERE intent_id = ?1",
                [matching.intent_id],
            )
            .unwrap();
        let outcome = matching
            .store
            .reconcile_deletion_with(matching.intent_id, &matching.quarantine, &mut matching.fs)
            .unwrap();
        assert_eq!(outcome, DeletionRecoveryOutcome::Deleted);
        assert_eq!(matching.fs.bytes(&matching.target), None);
        assert_eq!(
            matching.fs.bytes(&matching.aside),
            Some(b"preimage".as_slice())
        );
        assert!(persisted_state(&matching.store, matching.intent_id).1);

        let mut raced = fixture();
        raced
            .store
            .conn
            .execute(
                "UPDATE write_intents SET temp_path = '' WHERE intent_id = ?1",
                [raced.intent_id],
            )
            .unwrap();
        raced.fs.put(&raced.target, b"external changed bytes");
        let outcome = raced
            .store
            .reconcile_deletion_with(raced.intent_id, &raced.quarantine, &mut raced.fs)
            .unwrap();
        assert_eq!(outcome, DeletionRecoveryOutcome::ConflictRestored);
        assert_eq!(
            raced.fs.bytes(&raced.target),
            Some(b"external changed bytes".as_slice())
        );
        assert_eq!(
            raced.fs.bytes(&raced.aside),
            Some(b"external changed bytes".as_slice())
        );
        assert!(persisted_state(&raced.store, raced.intent_id).1);

        let mut reappeared = fixture();
        reappeared
            .store
            .conn
            .execute(
                "UPDATE write_intents SET temp_path = '' WHERE intent_id = ?1",
                [reappeared.intent_id],
            )
            .unwrap();
        reappeared
            .fs
            .fail_sync_after(&reappeared.target, &reappeared.aside);
        assert!(reappeared
            .store
            .reconcile_deletion_with(
                reappeared.intent_id,
                &reappeared.quarantine,
                &mut reappeared.fs,
            )
            .is_err());
        reappeared.fs.put(&reappeared.target, b"new external file");
        let outcome = reappeared
            .store
            .reconcile_deletion_with(
                reappeared.intent_id,
                &reappeared.quarantine,
                &mut reappeared.fs,
            )
            .unwrap();
        assert_eq!(outcome, DeletionRecoveryOutcome::RetryRequired);
        assert_eq!(
            reappeared.fs.bytes(&reappeared.target),
            Some(b"new external file".as_slice())
        );
        assert_eq!(
            reappeared.fs.bytes(&reappeared.aside),
            Some(b"preimage".as_slice())
        );
        assert!(!persisted_state(&reappeared.store, reappeared.intent_id).1);
    }

    #[test]
    fn unfinished_creation_recovers_only_by_no_replace() {
        let mut created = fixture();
        created.fs.files.remove(&created.target);
        created
            .store
            .conn
            .execute(
                "UPDATE write_intents SET pre_image_hash = NULL WHERE intent_id = ?1",
                [created.intent_id],
            )
            .unwrap();
        let outcome = created
            .store
            .reconcile_creation_with(created.intent_id, &mut created.fs)
            .unwrap();
        assert_eq!(outcome, CreationRecoveryOutcome::Installed);
        assert_eq!(
            created.fs.bytes(&created.target),
            Some(b"proposed".as_slice())
        );
        assert_eq!(created.fs.bytes(&created.temp), None);
        assert!(persisted_state(&created.store, created.intent_id).1);

        let mut linked = fixture();
        linked.fs.files.remove(&linked.target);
        linked
            .store
            .conn
            .execute(
                "UPDATE write_intents SET pre_image_hash = NULL WHERE intent_id = ?1",
                [linked.intent_id],
            )
            .unwrap();
        linked.fs.fail_after_temp_link(&linked.temp, &linked.target);
        assert!(linked
            .store
            .reconcile_creation_with(linked.intent_id, &mut linked.fs)
            .is_err());
        assert_eq!(
            linked.fs.bytes(&linked.target),
            Some(b"proposed".as_slice())
        );
        assert_eq!(linked.fs.bytes(&linked.temp), Some(b"proposed".as_slice()));
        assert_eq!(
            linked
                .store
                .reconcile_creation_with(linked.intent_id, &mut linked.fs)
                .unwrap(),
            CreationRecoveryOutcome::Installed
        );
        assert_eq!(linked.fs.bytes(&linked.temp), None);
        assert!(persisted_state(&linked.store, linked.intent_id).1);

        let mut collision = fixture();
        collision
            .store
            .conn
            .execute(
                "UPDATE write_intents SET pre_image_hash = NULL WHERE intent_id = ?1",
                [collision.intent_id],
            )
            .unwrap();
        collision.fs.put(&collision.target, b"external creation");
        let outcome = collision
            .store
            .reconcile_creation_with(collision.intent_id, &mut collision.fs)
            .unwrap();
        assert_eq!(outcome, CreationRecoveryOutcome::RetryRequired);
        assert_eq!(
            collision.fs.bytes(&collision.target),
            Some(b"external creation".as_slice())
        );
        assert_eq!(
            collision.fs.bytes(&collision.temp),
            Some(b"proposed".as_slice())
        );
        assert!(persisted_state(&collision.store, collision.intent_id).1);
    }
}
