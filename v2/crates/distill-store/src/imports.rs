//! Durable memo state for failed watched authoring imports (§8).
//!
//! A failed run never rewrites the committed bundle and never advances the
//! input version. Its complete outcome-bearing attempted basis is retained so
//! an unchanged failure does not spin and the exact healing observation wakes
//! the import. The daemon owns the versioned basis codec; the store preserves
//! those bytes exactly and owns the memo-sequence transaction.

use rusqlite::OptionalExtension;

use distill_core::id::BundleUuid;

use crate::state::{InputVersion, MemoSeq};
use crate::{Store, StoreError, StoreReader};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchedImportTerminal {
    /// The last outcome-bearing dependency is the failing operation.
    Dependency,
    /// The importer rejected otherwise successful observations with its
    /// stable, non-zero module-defined code.
    Importer { code: u32 },
    /// A directory-generated bundle whose exact persisted origin is no
    /// longer produced by the current rules and listing projection.
    DirectoryOrphan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedImportFailure {
    pub bundle: BundleUuid,
    pub attempted_input_version: InputVersion,
    pub basis: Vec<u8>,
    pub terminal: WatchedImportTerminal,
    pub message: String,
    /// Filled by reads. Callers may leave any value when recording.
    pub memo_seq: MemoSeq,
}

impl Store {
    pub fn record_watched_import_failure(
        &mut self,
        failure: &WatchedImportFailure,
    ) -> Result<MemoSeq, StoreError> {
        let (terminal_kind, terminal_code) = match failure.terminal {
            WatchedImportTerminal::Dependency => (1_i64, None),
            WatchedImportTerminal::Importer { code: 0 } => {
                return Err(StoreError::InvalidConfiguration {
                    error: "watched importer failure code zero is reserved".to_owned(),
                });
            }
            WatchedImportTerminal::Importer { code } => (2_i64, Some(i64::from(code))),
            WatchedImportTerminal::DirectoryOrphan => (3_i64, None),
        };
        if failure.basis.is_empty() {
            return Err(StoreError::InvalidConfiguration {
                error: "watched importer failure basis is empty".to_owned(),
            });
        }
        let bundle = failure.bundle;
        let version = failure.attempted_input_version;
        let basis = failure.basis.clone();
        let message = failure.message.clone();
        let (_, sequence) = self.memo_transaction(|transaction, sequence| {
            transaction.execute(
                "INSERT INTO watched_import_failures(
                     bundle_uuid, attempted_input_version, basis,
                     terminal_kind, terminal_code, message, memo_seq
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(bundle_uuid) DO UPDATE SET
                   attempted_input_version = excluded.attempted_input_version,
                   basis = excluded.basis,
                   terminal_kind = excluded.terminal_kind,
                   terminal_code = excluded.terminal_code,
                   message = excluded.message,
                   memo_seq = excluded.memo_seq",
                rusqlite::params![
                    bundle.0.as_slice(),
                    version.0 as i64,
                    basis,
                    terminal_kind,
                    terminal_code,
                    message,
                    sequence.0 as i64,
                ],
            )?;
            Ok(())
        })?;
        Ok(sequence)
    }

    pub fn clear_watched_import_failure(&mut self, bundle: BundleUuid) -> Result<bool, StoreError> {
        if self.watched_import_failure(bundle)?.is_none() {
            return Ok(false);
        }
        let (removed, _) = self.memo_transaction(|transaction, _| {
            Ok(transaction.execute(
                "DELETE FROM watched_import_failures WHERE bundle_uuid = ?1",
                [bundle.0.as_slice()],
            )? > 0)
        })?;
        Ok(removed)
    }
}

impl StoreReader {

    pub fn watched_import_failure(
        &self,
        bundle: BundleUuid,
    ) -> Result<Option<WatchedImportFailure>, StoreError> {
        self.conn
            .query_row(
                "SELECT attempted_input_version, basis, terminal_kind,
                        terminal_code, message, memo_seq
                 FROM watched_import_failures WHERE bundle_uuid = ?1",
                [bundle.0.as_slice()],
                |row| {
                    let kind = row.get::<_, i64>(2)?;
                    let code = row.get::<_, Option<i64>>(3)?;
                    let terminal = match (kind, code) {
                        (1, None) => WatchedImportTerminal::Dependency,
                        (2, Some(code)) if (1..=i64::from(u32::MAX)).contains(&code) => {
                            WatchedImportTerminal::Importer { code: code as u32 }
                        }
                        (3, None) => WatchedImportTerminal::DirectoryOrphan,
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    };
                    Ok(WatchedImportFailure {
                        bundle,
                        attempted_input_version: InputVersion(row.get::<_, i64>(0)? as u64),
                        basis: row.get(1)?,
                        terminal,
                        message: row.get(4)?,
                        memo_seq: MemoSeq(row.get::<_, i64>(5)? as u64),
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
    }
}
