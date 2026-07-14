//! Daemon orchestration over the store's durable displacement journal.

use std::path::{Path, PathBuf};

use distill_core::id::ContentHash;
use distill_store::codegen::CodegenPublicationBasis;
use distill_store::journal::{
    JournalIntentPlan, PublicationGroup, PublicationGroupKind, RecoveredEdit,
};
use distill_store::{Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineRoot {
    pub filesystem_root: PathBuf,
    pub quarantine_dir: PathBuf,
}

impl QuarantineRoot {
    pub fn new(filesystem_root: impl AsRef<Path>, quarantine_dir: impl AsRef<Path>) -> Self {
        Self {
            filesystem_root: filesystem_root.as_ref().to_path_buf(),
            quarantine_dir: quarantine_dir.as_ref().to_path_buf(),
        }
    }
}

#[derive(Debug)]
pub enum QuarantineError {
    NoFilesystemRoot { target: PathBuf },
    EmptyRoots,
    Store(Box<StoreError>),
}

impl QuarantineError {
    pub fn store_error(&self) -> Option<&StoreError> {
        match self {
            Self::Store(error) => Some(error.as_ref()),
            Self::NoFilesystemRoot { .. } | Self::EmptyRoots => None,
        }
    }
}

impl std::fmt::Display for QuarantineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoFilesystemRoot { target } => write!(
                f,
                "no same-filesystem quarantine is configured for {}",
                target.display()
            ),
            Self::EmptyRoots => f.write_str("at least one filesystem quarantine is required"),
            Self::Store(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for QuarantineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(error) => Some(error.as_ref()),
            Self::NoFilesystemRoot { .. } | Self::EmptyRoots => None,
        }
    }
}

impl From<StoreError> for QuarantineError {
    fn from(value: StoreError) -> Self {
        Self::Store(Box::new(value))
    }
}

/// Maps every daemon-owned/watched filesystem root to a quarantine directory
/// on that filesystem. Longest-root matching makes nested roots deterministic.
#[derive(Clone)]
pub struct QuarantineDriver {
    roots: Vec<QuarantineRoot>,
}

/// Admission token for authored-file publication. It can only be constructed
/// after every recoverable unfinished mutation has been resumed; publication
/// code therefore never receives a raw, unreconciled replacement API.
pub struct PublicationDriver<'a> {
    quarantine: &'a QuarantineDriver,
    store: &'a mut Store,
    recovered: Vec<(i64, RecoveryOutcome)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcome {
    Rewrite(distill_store::journal::RenameAsideOutcome),
    Deletion(distill_store::journal::DeletionRecoveryOutcome),
    Creation(distill_store::journal::CreationRecoveryOutcome),
}

#[cfg(windows)]
pub type WindowsPublicationDriver<'a> = PublicationDriver<'a>;

#[cfg(windows)]
pub type WindowsRecoveryOutcome = RecoveryOutcome;

impl QuarantineDriver {
    pub fn new(roots: impl IntoIterator<Item = QuarantineRoot>) -> Result<Self, QuarantineError> {
        let mut roots = roots.into_iter().collect::<Vec<_>>();
        if roots.is_empty() {
            return Err(QuarantineError::EmptyRoots);
        }
        roots.sort_by(|left, right| {
            right
                .filesystem_root
                .components()
                .count()
                .cmp(&left.filesystem_root.components().count())
                .then_with(|| left.filesystem_root.cmp(&right.filesystem_root))
        });
        Ok(Self { roots })
    }

    pub fn quarantine_for(&self, target: &Path) -> Result<&Path, QuarantineError> {
        self.roots
            .iter()
            .find(|root| target.starts_with(&root.filesystem_root))
            .map(|root| root.quarantine_dir.as_path())
            .ok_or_else(|| QuarantineError::NoFilesystemRoot {
                target: target.to_path_buf(),
            })
    }

    pub(crate) fn with_root(&self, root: QuarantineRoot) -> Result<Self, QuarantineError> {
        Self::new(self.roots.iter().cloned().chain(std::iter::once(root)))
    }

    /// Record a deletion intent, rename the inode to its intent-ID-derived
    /// quarantine name, verify the displaced bytes, then retire the intent.
    /// The store restores mismatched bytes and returns a typed conflict.
    pub fn journaled_delete(
        &self,
        store: &mut Store,
        target: &Path,
        expected_preimage: ContentHash,
    ) -> Result<PathBuf, QuarantineError> {
        let quarantine = self.quarantine_for(target)?;
        let conflict = target.with_extension("distill-conflict");
        let intent_id = store.record_intent(
            &target.to_string_lossy(),
            "",
            &conflict.to_string_lossy(),
            Some(expected_preimage),
            ContentHash(*blake3::hash(b"").as_bytes()),
        )?;
        match store.reconcile_journaled_deletion(intent_id, quarantine)? {
            distill_store::journal::DeletionRecoveryOutcome::Deleted => store
                .quarantined_entries()?
                .into_iter()
                .find(|entry| entry.intent_id == intent_id && !entry.restored)
                .map(|entry| entry.path)
                .ok_or_else(|| {
                    QuarantineError::Store(Box::new(StoreError::BadIntent {
                        intent_id,
                        detail: "completed deletion has no retained displacement".into(),
                    }))
                }),
            distill_store::journal::DeletionRecoveryOutcome::ConflictRestored => Err(
                QuarantineError::Store(Box::new(StoreError::DeleteConflict {
                    intent_id,
                    expected: expected_preimage.0,
                    actual: *blake3::hash(&std::fs::read(target).map_err(|source| {
                        StoreError::Io {
                            path: target.to_path_buf(),
                            source,
                        }
                    })?)
                    .as_bytes(),
                })),
            ),
            distill_store::journal::DeletionRecoveryOutcome::RetryRequired => {
                Err(QuarantineError::Store(Box::new(StoreError::BadIntent {
                    intent_id,
                    detail: "journaled deletion stopped after a concurrent target appeared".into(),
                })))
            }
        }
    }

    /// Attach a rewrite/swap displacement to an already journaled intent.
    pub fn quarantine_displaced(
        &self,
        store: &mut Store,
        intent_id: i64,
        displaced_file: &Path,
    ) -> Result<PathBuf, QuarantineError> {
        let quarantine = self.quarantine_for(displaced_file)?;
        store
            .quarantine_displaced(intent_id, displaced_file, quarantine)
            .map_err(Into::into)
    }

    /// Resume every unfinished mutation before admitting any new publication.
    /// Durable states after `Prepared` recover from their
    /// journaled physical paths; root mapping is consulted only for an intent
    /// that has not yet performed its first filesystem mutation.
    fn startup_reconcile(
        &self,
        store: &mut Store,
    ) -> Result<Vec<(i64, RecoveryOutcome)>, QuarantineError> {
        let intents = store.unretired_intents()?;
        let mut outcomes = Vec::new();
        for intent in intents {
            let target = PathBuf::from(&intent.target_path);
            if intent.pre_image_hash.is_none() {
                let outcome = store.reconcile_journaled_creation(intent.intent_id)?;
                outcomes.push((intent.intent_id, RecoveryOutcome::Creation(outcome)));
                continue;
            }
            let quarantine = if intent.rename_aside_state
                == distill_store::journal::RenameAsideState::Prepared
            {
                self.quarantine_for(&target)?
            } else {
                intent
                    .quarantine_paths
                    .first()
                    .and_then(|path| path.parent())
                    .ok_or_else(|| {
                        QuarantineError::Store(Box::new(StoreError::BadIntent {
                            intent_id: intent.intent_id,
                            detail: "post-Prepared Windows intent has no journaled aside path"
                                .into(),
                        }))
                    })?
            };
            if intent.temp_path.is_empty() {
                let outcome = store.reconcile_journaled_deletion(intent.intent_id, quarantine)?;
                outcomes.push((intent.intent_id, RecoveryOutcome::Deletion(outcome)));
            } else {
                let outcome = store.publish_journaled_replacement(intent.intent_id, quarantine)?;
                outcomes.push((intent.intent_id, RecoveryOutcome::Rewrite(outcome)));
            }
        }
        if let Some(pending) = store.unretired_intents()?.first() {
            return Err(QuarantineError::Store(Box::new(StoreError::BadIntent {
                intent_id: pending.intent_id,
                detail: "startup recovery stopped before the intent became terminal".into(),
            })));
        }
        for group in store.unfinished_publication_groups()? {
            if group.kind == PublicationGroupKind::Codegen {
                let basis = CodegenPublicationBasis::decode(&group.basis).map_err(|detail| {
                    QuarantineError::Store(Box::new(StoreError::BadIntent {
                        intent_id: group.group_id,
                        detail,
                    }))
                })?;
                store.apply_codegen_publication_basis(&basis)?;
            }
            store.retire_publication_group(group.group_id)?;
        }
        Ok(outcomes)
    }

    /// Reconcile unfinished work and mint the sole daemon mutation
    /// publication capability. Failure leaves publication unadmitted.
    pub fn admit_publication<'a>(
        &'a self,
        store: &'a mut Store,
    ) -> Result<PublicationDriver<'a>, QuarantineError> {
        let recovered = self.startup_reconcile(store)?;
        Ok(PublicationDriver {
            quarantine: self,
            store,
            recovered,
        })
    }

    #[cfg(windows)]
    pub fn admit_windows_publication<'a>(
        &'a self,
        store: &'a mut Store,
    ) -> Result<WindowsPublicationDriver<'a>, QuarantineError> {
        self.admit_publication(store)
    }

    pub fn doctor_verify(&self, store: &Store) -> Result<Vec<RecoveredEdit>, QuarantineError> {
        store.verify_quarantine().map_err(Into::into)
    }

    pub fn doctor_clean(&self, store: &mut Store, now_secs: i64) -> Result<usize, QuarantineError> {
        store.sweep_displaced(now_secs).map_err(Into::into)
    }
}

impl PublicationDriver<'_> {
    pub fn recovered(&self) -> &[(i64, RecoveryOutcome)] {
        &self.recovered
    }

    pub fn record_group(
        &mut self,
        kind: PublicationGroupKind,
        basis: &[u8],
        plans: &[JournalIntentPlan],
    ) -> Result<PublicationGroup, QuarantineError> {
        self.store
            .record_publication_group(kind, basis, plans)
            .map_err(Into::into)
    }

    pub fn retire_group(&mut self, group_id: i64) -> Result<(), QuarantineError> {
        self.store
            .retire_publication_group(group_id)
            .map_err(Into::into)
    }

    /// Install the durable preimage map after every Codegen child mutation is
    /// terminal, then retire the parent. Startup recovery performs the same
    /// idempotent sequence for a crash between these two steps.
    pub fn complete_codegen_group(
        &mut self,
        group: &PublicationGroup,
        basis: &CodegenPublicationBasis,
    ) -> Result<(), QuarantineError> {
        if group.kind != PublicationGroupKind::Codegen || group.basis != basis.encode() {
            return Err(QuarantineError::Store(Box::new(StoreError::BadIntent {
                intent_id: group.group_id,
                detail: "codegen completion basis does not match its publication group".into(),
            })));
        }
        self.store.apply_codegen_publication_basis(basis)?;
        self.store.retire_publication_group(group.group_id)?;
        Ok(())
    }

    pub fn resume_group_replace(
        &mut self,
        intent_id: i64,
        target: &Path,
    ) -> Result<distill_store::journal::RenameAsideOutcome, QuarantineError> {
        let quarantine = self.quarantine.quarantine_for(target)?;
        self.store
            .publish_journaled_replacement(intent_id, quarantine)
            .map_err(Into::into)
    }

    pub fn resume_group_delete(
        &mut self,
        intent_id: i64,
        target: &Path,
    ) -> Result<distill_store::journal::DeletionRecoveryOutcome, QuarantineError> {
        let quarantine = self.quarantine.quarantine_for(target)?;
        self.store
            .reconcile_journaled_deletion(intent_id, quarantine)
            .map_err(Into::into)
    }

    pub fn resume_group_create(
        &mut self,
        intent_id: i64,
    ) -> Result<distill_store::journal::CreationRecoveryOutcome, QuarantineError> {
        self.store
            .reconcile_journaled_creation(intent_id)
            .map_err(Into::into)
    }

    pub fn journaled_replace(
        &mut self,
        target: &Path,
        proposed_temp: &Path,
        expected_preimage: ContentHash,
        proposed_hash: ContentHash,
    ) -> Result<distill_store::journal::RenameAsideOutcome, QuarantineError> {
        let quarantine = self.quarantine.quarantine_for(target)?;
        let conflict = target.with_extension("distill-conflict");
        let intent_id = self.store.record_intent(
            &target.to_string_lossy(),
            &proposed_temp.to_string_lossy(),
            &conflict.to_string_lossy(),
            Some(expected_preimage),
            proposed_hash,
        )?;
        self.store
            .publish_journaled_replacement(intent_id, quarantine)
            .map_err(Into::into)
    }

    pub fn journaled_delete(
        &mut self,
        target: &Path,
        expected_preimage: ContentHash,
    ) -> Result<distill_store::journal::DeletionRecoveryOutcome, QuarantineError> {
        let quarantine = self.quarantine.quarantine_for(target)?;
        let conflict = target.with_extension("distill-conflict");
        let intent_id = self.store.record_intent(
            &target.to_string_lossy(),
            "",
            &conflict.to_string_lossy(),
            Some(expected_preimage),
            ContentHash(*blake3::hash(b"").as_bytes()),
        )?;
        self.store
            .reconcile_journaled_deletion(intent_id, quarantine)
            .map_err(Into::into)
    }

    pub fn journaled_create(
        &mut self,
        target: &Path,
        proposed_temp: &Path,
        proposed_hash: ContentHash,
    ) -> Result<distill_store::journal::CreationRecoveryOutcome, QuarantineError> {
        let conflict = target.with_extension("distill-conflict");
        let intent_id = self.store.record_intent(
            &target.to_string_lossy(),
            &proposed_temp.to_string_lossy(),
            &conflict.to_string_lossy(),
            None,
            proposed_hash,
        )?;
        self.store
            .reconcile_journaled_creation(intent_id)
            .map_err(Into::into)
    }
}
