//! Daemon orchestration over the store's durable displacement journal.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use distill_store::codegen::CodegenPublicationBasis;
use distill_store::journal::{
    JournalFilesystem, JournalIntentPlan, PublicationGroup, PublicationGroupKind,
    PublicationGroupState, RecoveredEdit,
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
    filesystem: Option<&'a mut dyn JournalFilesystem>,
    recovered: Vec<RecoveredPublication>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcome {
    AbandonedUnarmed,
    TerminalFailure,
    Rewrite(distill_store::journal::RenameAsideOutcome),
    Deletion(distill_store::journal::DeletionRecoveryOutcome),
    Creation(distill_store::journal::CreationRecoveryOutcome),
}

impl RecoveryOutcome {
    pub fn is_material_failure(self) -> bool {
        match self {
            Self::AbandonedUnarmed | Self::TerminalFailure => true,
            Self::Rewrite(outcome) => {
                outcome != distill_store::journal::RenameAsideOutcome::Installed
            }
            Self::Deletion(outcome) => {
                outcome != distill_store::journal::DeletionRecoveryOutcome::Deleted
            }
            Self::Creation(outcome) => {
                outcome != distill_store::journal::CreationRecoveryOutcome::Installed
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredPublication {
    pub intent_id: i64,
    pub group_kind: Option<PublicationGroupKind>,
    pub target_path: PathBuf,
    pub outcome: RecoveryOutcome,
}

pub(crate) fn material_recovery_diagnostic(recovered: &[RecoveredPublication]) -> Option<String> {
    let failures = recovered
        .iter()
        .filter(|recovered| recovered.outcome.is_material_failure())
        .map(|recovered| {
            format!(
                "recovered {:?} publication for {} (intent {}): {:?}",
                recovered.group_kind,
                recovered.target_path.display(),
                recovered.intent_id,
                recovered.outcome
            )
        })
        .collect::<Vec<_>>();
    (!failures.is_empty()).then(|| failures.join("; "))
}

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

    /// Resume every unfinished mutation before admitting any new publication.
    /// Durable states after `Prepared` recover from their
    /// journaled physical paths; root mapping is consulted only for an intent
    /// that has not yet performed its first filesystem mutation.
    fn startup_reconcile(
        &self,
        store: &mut Store,
        mut filesystem: Option<&mut dyn JournalFilesystem>,
        recover_codegen: bool,
    ) -> Result<Vec<RecoveredPublication>, QuarantineError> {
        let groups = store.unfinished_publication_groups()?;
        let group_by_child = groups
            .iter()
            .flat_map(|group| {
                group
                    .child_intents
                    .iter()
                    .map(move |intent| (*intent, group.kind))
            })
            .collect::<BTreeMap<_, _>>();
        let codegen_children = groups
            .iter()
            .filter(|group| group.kind == PublicationGroupKind::Codegen)
            .flat_map(|group| group.child_intents.iter().copied())
            .collect::<BTreeSet<_>>();
        let mut outcomes = Vec::new();
        let mut recorded = BTreeSet::new();
        for group in &groups {
            if group.state != PublicationGroupState::Unarmed
                || (group.kind == PublicationGroupKind::Codegen && !recover_codegen)
            {
                continue;
            }
            for child in store.publication_group_child_results(group.group_id)? {
                recorded.insert(child.intent_id);
                outcomes.push(RecoveredPublication {
                    intent_id: child.intent_id,
                    group_kind: Some(group.kind),
                    target_path: PathBuf::from(child.target_path),
                    outcome: RecoveryOutcome::AbandonedUnarmed,
                });
            }
            store.abort_unarmed_publication_group(group.group_id)?;
        }
        let intents = store.unretired_intents()?;
        for intent in intents {
            let target = PathBuf::from(&intent.target_path);
            let codegen_owned = codegen_children.contains(&intent.intent_id);
            if codegen_owned && !recover_codegen {
                continue;
            }
            if codegen_owned && filesystem.is_none() {
                return Err(QuarantineError::Store(Box::new(StoreError::BadIntent {
                    intent_id: intent.intent_id,
                    detail: "codegen recovery requires its validated output filesystem".into(),
                })));
            }
            if intent.pre_image_hash.is_none() {
                let outcome = if codegen_owned {
                    store.reconcile_journaled_creation_with_filesystem(
                        intent.intent_id,
                        filesystem.as_deref_mut().expect("checked above"),
                    )?
                } else {
                    store.reconcile_journaled_creation(intent.intent_id)?
                };
                recorded.insert(intent.intent_id);
                outcomes.push(RecoveredPublication {
                    intent_id: intent.intent_id,
                    group_kind: group_by_child.get(&intent.intent_id).copied(),
                    target_path: target,
                    outcome: RecoveryOutcome::Creation(outcome),
                });
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
                            detail: "post-Prepared intent has no journaled aside path".into(),
                        }))
                    })?
            };
            if intent.temp_path.is_empty() {
                let outcome = if codegen_owned {
                    store.reconcile_journaled_deletion_with_filesystem(
                        intent.intent_id,
                        quarantine,
                        filesystem.as_deref_mut().expect("checked above"),
                    )?
                } else {
                    store.reconcile_journaled_deletion(intent.intent_id, quarantine)?
                };
                recorded.insert(intent.intent_id);
                outcomes.push(RecoveredPublication {
                    intent_id: intent.intent_id,
                    group_kind: group_by_child.get(&intent.intent_id).copied(),
                    target_path: target,
                    outcome: RecoveryOutcome::Deletion(outcome),
                });
            } else {
                let outcome = if codegen_owned {
                    store.publish_journaled_replacement_with_filesystem(
                        intent.intent_id,
                        quarantine,
                        filesystem.as_deref_mut().expect("checked above"),
                    )?
                } else {
                    store.publish_journaled_replacement(intent.intent_id, quarantine)?
                };
                recorded.insert(intent.intent_id);
                outcomes.push(RecoveredPublication {
                    intent_id: intent.intent_id,
                    group_kind: group_by_child.get(&intent.intent_id).copied(),
                    target_path: target,
                    outcome: RecoveryOutcome::Rewrite(outcome),
                });
            }
        }
        if let Some(pending) = store
            .unretired_intents()?
            .into_iter()
            .find(|intent| recover_codegen || !codegen_children.contains(&intent.intent_id))
        {
            return Err(QuarantineError::Store(Box::new(StoreError::BadIntent {
                intent_id: pending.intent_id,
                detail: "startup recovery stopped before the intent became terminal".into(),
            })));
        }
        for group in groups {
            if group.state == PublicationGroupState::Unarmed {
                continue;
            }
            if group.kind == PublicationGroupKind::Codegen && !recover_codegen {
                continue;
            }
            let children = store.publication_group_child_results(group.group_id)?;
            for child in children
                .iter()
                .filter(|child| child.terminal_success != Some(true))
            {
                if recorded.insert(child.intent_id) {
                    outcomes.push(RecoveredPublication {
                        intent_id: child.intent_id,
                        group_kind: Some(group.kind),
                        target_path: PathBuf::from(&child.target_path),
                        outcome: RecoveryOutcome::TerminalFailure,
                    });
                }
            }
            if group.kind == PublicationGroupKind::Codegen
                && children
                    .iter()
                    .all(|child| child.terminal_success == Some(true))
            {
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
        store.cleanup_retired_non_codegen_proposal_temps()?;
        if recover_codegen {
            store.cleanup_retired_codegen_proposal_temps_with_filesystem(
                filesystem.expect("codegen recovery checked output filesystem authority"),
            )?;
        }
        Ok(outcomes)
    }

    /// Reconcile startup work that can be authorized by watched-root
    /// ownership alone. Codegen groups remain pending until their output
    /// directory has been independently validated.
    pub(crate) fn reconcile_non_codegen(
        &self,
        store: &mut Store,
    ) -> Result<Vec<RecoveredPublication>, QuarantineError> {
        self.startup_reconcile(store, None, false)
    }

    /// Reconcile unfinished work and mint the sole daemon mutation
    /// publication capability. Failure leaves publication unadmitted.
    pub fn admit_publication<'a>(
        &'a self,
        store: &'a mut Store,
    ) -> Result<PublicationDriver<'a>, QuarantineError> {
        let recovered = self.startup_reconcile(store, None, false)?;
        Ok(PublicationDriver {
            quarantine: self,
            store,
            filesystem: None,
            recovered,
        })
    }

    /// Reconcile and publish codegen groups through the output filesystem,
    /// which revalidates the configured canonical workspace before mutation.
    pub fn admit_codegen_publication<'a>(
        &'a self,
        store: &'a mut Store,
        filesystem: &'a mut dyn JournalFilesystem,
    ) -> Result<PublicationDriver<'a>, QuarantineError> {
        let recovered = self.startup_reconcile(store, Some(filesystem), true)?;
        Ok(PublicationDriver {
            quarantine: self,
            store,
            filesystem: Some(filesystem),
            recovered,
        })
    }

    pub fn doctor_verify(&self, store: &Store) -> Result<Vec<RecoveredEdit>, QuarantineError> {
        store.verify_quarantine().map_err(Into::into)
    }
}

impl PublicationDriver<'_> {
    pub fn recovered(&self) -> &[RecoveredPublication] {
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

    pub fn arm_group(&mut self, group_id: i64) -> Result<(), QuarantineError> {
        self.store
            .arm_publication_group(group_id)
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
        if !self
            .store
            .publication_group_child_results(group.group_id)?
            .iter()
            .all(|child| child.terminal_success == Some(true))
        {
            return Err(QuarantineError::Store(Box::new(StoreError::BadIntent {
                intent_id: group.group_id,
                detail: "codegen publication has an unsuccessful child".into(),
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
        match self.filesystem.as_deref_mut() {
            Some(filesystem) => self
                .store
                .publish_journaled_replacement_with_filesystem(intent_id, quarantine, filesystem),
            None => self
                .store
                .publish_journaled_replacement(intent_id, quarantine),
        }
        .map_err(Into::into)
    }

    pub fn resume_group_delete(
        &mut self,
        intent_id: i64,
        target: &Path,
    ) -> Result<distill_store::journal::DeletionRecoveryOutcome, QuarantineError> {
        let quarantine = self.quarantine.quarantine_for(target)?;
        match self.filesystem.as_deref_mut() {
            Some(filesystem) => self
                .store
                .reconcile_journaled_deletion_with_filesystem(intent_id, quarantine, filesystem),
            None => self
                .store
                .reconcile_journaled_deletion(intent_id, quarantine),
        }
        .map_err(Into::into)
    }

    pub fn resume_group_create(
        &mut self,
        intent_id: i64,
    ) -> Result<distill_store::journal::CreationRecoveryOutcome, QuarantineError> {
        match self.filesystem.as_deref_mut() {
            Some(filesystem) => self
                .store
                .reconcile_journaled_creation_with_filesystem(intent_id, filesystem),
            None => self.store.reconcile_journaled_creation(intent_id),
        }
        .map_err(Into::into)
    }
}

