//! Durable memo state for failed watched authoring imports (§8).
//!
//! A failed run never rewrites the committed bundle and never advances the
//! input version. Its complete outcome-bearing attempted basis is retained so
//! an unchanged failure does not spin and the exact healing observation wakes
//! the import. The daemon owns the versioned basis codec; the store preserves
//! those bytes exactly and owns the memo-sequence transaction.

use rusqlite::OptionalExtension;

use distill_core::id::{AssetUuid, BundleUuid};

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
        self.write_txn(|store| {
            if store.watched_import_failure(bundle)?.is_none() {
                return Ok(false);
            }
            let (removed, _) = store.memo_transaction(|transaction, _| {
                Ok(transaction.execute(
                    "DELETE FROM watched_import_failures WHERE bundle_uuid = ?1",
                    [bundle.0.as_slice()],
                )? > 0)
            })?;
            Ok(removed)
        })
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

/// What a watched import's read set observed, for joining dirty paths.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImportReadKey {
    Path(String),
    Listing,
    Capability,
}

impl ImportReadKey {
    fn row(&self) -> (i64, &str) {
        match self {
            Self::Path(path) => (0, path),
            Self::Listing => (1, ""),
            Self::Capability => (2, ""),
        }
    }
}

/// A watched import: its bundle and the read set it is revalidated against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedImport {
    pub bundle: BundleUuid,
    pub basis: Vec<u8>,
    pub reads: Vec<ImportReadKey>,
}

/// A directory-import rules asset and the bundle source holding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryRuleSource {
    pub root_name: String,
    pub path: String,
    pub rules_bundle: BundleUuid,
    pub rules_asset: AssetUuid,
}

/// The import index rows of one bundle source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportIndexSource {
    pub root_name: String,
    pub path: String,
    pub watched: Option<WatchedImport>,
    pub directory_rules: Vec<(BundleUuid, AssetUuid)>,
}

impl Store {
    /// Replace the import index rows of the bundle sources at `sources`
    /// (every source when `None`) with `rows`. The index is derived state,
    /// so this publishes no input version.
    pub fn replace_import_index(
        &mut self,
        sources: Option<&[(String, String)]>,
        rows: &[ImportIndexSource],
    ) -> Result<(), StoreError> {
        self.write_txn(|store| {
            let transaction = store.read.conn.savepoint()?;
            match sources {
                None => transaction.execute_batch(
                    "DELETE FROM import_reads; DELETE FROM import_records;
                     DELETE FROM directory_rule_sources;",
                )?,
                Some(sources) => {
                    for (root, path) in sources {
                        let params = rusqlite::params![root, path];
                        transaction.execute(
                            "DELETE FROM import_reads WHERE bundle_uuid IN (
                               SELECT t.bundle_uuid FROM import_records t JOIN roots r USING (root_id)
                               WHERE r.name = ?1 AND t.path = ?2)",
                            params,
                        )?;
                        for table in ["import_records", "directory_rule_sources"] {
                            transaction.execute(
                                &format!(
                                    "DELETE FROM {table} WHERE rowid IN (
                                       SELECT t.rowid FROM {table} t JOIN roots r USING (root_id)
                                       WHERE r.name = ?1 AND t.path = ?2)"
                                ),
                                params,
                            )?;
                        }
                    }
                }
            }
            for row in rows {
                let root = crate::files::intern_root(&transaction, &row.root_name)?;
                if let Some(watched) = &row.watched {
                    let bundle = watched.bundle.0.as_slice();
                    transaction.execute("DELETE FROM import_reads WHERE bundle_uuid = ?1", [bundle])?;
                    transaction.execute(
                        "INSERT OR REPLACE INTO import_records(bundle_uuid, root_id, path, basis)
                         VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![bundle, root.0, row.path, watched.basis],
                    )?;
                    for read in &watched.reads {
                        let (kind, key) = read.row();
                        transaction.execute(
                            "INSERT OR IGNORE INTO import_reads(bundle_uuid, kind, key)
                             VALUES (?1, ?2, ?3)",
                            rusqlite::params![bundle, kind, key],
                        )?;
                    }
                }
                for (bundle, asset) in &row.directory_rules {
                    transaction.execute(
                        "INSERT OR REPLACE INTO directory_rule_sources(rules_bundle, rules_asset, root_id, path)
                         VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![bundle.0.as_slice(), asset.0.as_slice(), root.0, row.path],
                    )?;
                }
            }
            transaction.commit()?;
            Ok(())
        })
    }
}

fn uuid16(bytes: Vec<u8>) -> rusqlite::Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)
}

impl StoreReader {
    /// Every watched import, by bundle.
    pub fn watched_imports(&self) -> Result<Vec<WatchedImport>, StoreError> {
        self.watched_import_rows("SELECT bundle_uuid, basis FROM import_records ORDER BY bundle_uuid", [])
    }

    /// The watched imports that read one of `paths`, list a directory, or
    /// (when `capabilities`) observe an importer capability.
    pub fn watched_imports_reading<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a str>,
        capabilities: bool,
    ) -> Result<Vec<WatchedImport>, StoreError> {
        let mut found = std::collections::BTreeMap::new();
        let mut add = |rows: Vec<WatchedImport>| {
            for row in rows {
                found.insert(row.bundle, row);
            }
        };
        for path in paths {
            add(self.watched_import_rows(
                "SELECT DISTINCT i.bundle_uuid, i.basis FROM import_reads t
                 JOIN import_records i USING (bundle_uuid) WHERE t.kind = 0 AND t.key = ?1",
                [path],
            )?);
        }
        add(self.watched_import_rows(
            "SELECT DISTINCT i.bundle_uuid, i.basis FROM import_reads t
             JOIN import_records i USING (bundle_uuid)
             WHERE t.kind = 1 OR (t.kind = 2 AND ?1)",
            [capabilities],
        )?);
        Ok(found.into_values().collect())
    }

    fn watched_import_rows<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Vec<WatchedImport>, StoreError> {
        let rows = self.query_rows(sql, params, |row| {
            Ok((BundleUuid(uuid16(row.get(0)?)?), row.get::<_, Vec<u8>>(1)?))
        })?;
        rows.into_iter()
            .map(|(bundle, basis)| {
                let reads = self.query_rows(
                    "SELECT kind, key FROM import_reads WHERE bundle_uuid = ?1 ORDER BY kind, key",
                    [bundle.0.as_slice()],
                    |row| {
                        Ok(match row.get::<_, i64>(0)? {
                            0 => ImportReadKey::Path(row.get(1)?),
                            1 => ImportReadKey::Listing,
                            _ => ImportReadKey::Capability,
                        })
                    },
                )?;
                Ok(WatchedImport {
                    bundle,
                    basis,
                    reads,
                })
            })
            .collect()
    }

    /// Every directory-import rules asset, by (root, path).
    pub fn directory_rule_sources(&self) -> Result<Vec<DirectoryRuleSource>, StoreError> {
        self.directory_rule_rows(
            "SELECT r.name, t.path, t.rules_bundle, t.rules_asset
             FROM directory_rule_sources t JOIN roots r USING (root_id)
             ORDER BY r.name, t.path, t.rules_bundle, t.rules_asset",
            rusqlite::params![],
        )
    }

    /// The directory-import rules assets held by the source at (root, path).
    pub fn directory_rule_sources_at(
        &self,
        root_name: &str,
        path: &str,
    ) -> Result<Vec<DirectoryRuleSource>, StoreError> {
        self.directory_rule_rows(
            "SELECT r.name, t.path, t.rules_bundle, t.rules_asset
             FROM directory_rule_sources t JOIN roots r USING (root_id)
             WHERE r.name = ?1 AND t.path = ?2 ORDER BY t.rules_bundle, t.rules_asset",
            rusqlite::params![root_name, path],
        )
    }

    fn directory_rule_rows<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Vec<DirectoryRuleSource>, StoreError> {
        self.query_rows(sql, params, |row| {
            Ok(DirectoryRuleSource {
                root_name: row.get(0)?,
                path: row.get(1)?,
                rules_bundle: BundleUuid(uuid16(row.get(2)?)?),
                rules_asset: AssetUuid(uuid16(row.get(3)?)?),
            })
        })
    }
}
