//! Daemon orchestration over the store's durable displacement journal.

use std::path::{Path, PathBuf};

use distill_core::id::ContentHash;
use distill_store::journal::RecoveredEdit;
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
pub struct QuarantineDriver {
    roots: Vec<QuarantineRoot>,
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
        store
            .delete_with_intent(intent_id, target, quarantine)
            .map_err(Into::into)
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

    pub fn doctor_verify(&self, store: &Store) -> Result<Vec<RecoveredEdit>, QuarantineError> {
        store.verify_quarantine().map_err(Into::into)
    }

    pub fn doctor_clean(&self, store: &mut Store, now_secs: i64) -> Result<usize, QuarantineError> {
        store.sweep_displaced(now_secs).map_err(Into::into)
    }
}
