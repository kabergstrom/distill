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

/// A current watched-import failure by its bundle's rooted path; what runtime
/// clients show authors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedImportFailureSummary {
    pub bundle: BundleUuid,
    pub root: String,
    pub path: String,
    pub message: String,
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
            transaction
                .prepare_cached(
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
                )?
                .execute(
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
                Ok(transaction
                    .prepare_cached("DELETE FROM watched_import_failures WHERE bundle_uuid = ?1")?
                    .execute([bundle.0.as_slice()])? > 0)
            })?;
            Ok(removed)
        })
    }
}

impl StoreReader {
    /// Every recorded watched-import failure, ordered by root and path.
    pub fn watched_import_failure_summaries(
        &self,
    ) -> Result<Vec<WatchedImportFailureSummary>, StoreError> {
        let mut statement = self.conn.prepare(
            "SELECT f.bundle_uuid, r.name, b.path, f.message
               FROM watched_import_failures AS f
               JOIN bundles AS b USING (bundle_uuid)
               JOIN roots AS r ON r.root_id = b.root_id
              ORDER BY r.name, b.path",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(WatchedImportFailureSummary {
                bundle: BundleUuid(crate::bundles::blob16(row.get(0)?)),
                root: row.get(1)?,
                path: row.get(2)?,
                message: row.get(3)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
    }


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

/// What a watched import's basis observed, for joining dirty paths.
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

/// `import_keys.kind` of a directory-import rules asset.
const DIRECTORY_RULES: i64 = 3;

/// A watched import's index rows: its `$record` entry and what the basis
/// it is revalidated against reads. The basis itself is the failure memo's
/// or the record's, read when needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedImport {
    pub record: AssetUuid,
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

/// The import index rows of one published bundle, at its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportIndexSource {
    pub root_name: String,
    pub path: String,
    pub bundle: BundleUuid,
    pub watched: Option<WatchedImport>,
    /// Each directory-import rules asset, with its listing directory: the
    /// directory every path its listing matches is under (`""` for the
    /// whole root), ending in `/`. See
    /// [`StoreReader::directory_rule_sources_listing`].
    pub directory_rules: Vec<(AssetUuid, String)>,
}

impl Store {
    /// Replace the import index rows of the bundles at `sources` with
    /// `rows`, each a published bundle's. The index is derived state, so
    /// this publishes no input version. It is kept by source: every bundle
    /// publication queues its paths as dirty work, and the import pass
    /// reindexes the dirty bundle sources before it acknowledges that work.
    /// A removed bundle's rows go with its `bundles` row.
    pub fn replace_import_index(
        &mut self,
        sources: &[(String, String)],
        rows: &[ImportIndexSource],
    ) -> Result<(), StoreError> {
        self.write_txn(|store| {
            let transaction: &rusqlite::Connection = &store.read.conn;
            for (root_name, path) in sources {
                transaction
                    .prepare_cached(
                        "DELETE FROM import_keys WHERE bundle_uuid IN (
                           SELECT b.bundle_uuid FROM bundles b JOIN roots r USING (root_id)
                           WHERE r.name = ?1 AND b.path = ?2)",
                    )?
                    .execute(rusqlite::params![root_name, path])?;
            }
            for row in rows {
                let bundle = row.bundle.0.as_slice();
                // A row of a bundle at another source than the ones cleared.
                if !sources
                    .iter()
                    .any(|(root, path)| *root == row.root_name && *path == row.path)
                {
                    transaction
                        .prepare_cached("DELETE FROM import_keys WHERE bundle_uuid = ?1")?
                        .execute([bundle])?;
                }
                let mut insert = transaction.prepare_cached(
                    "INSERT OR IGNORE INTO import_keys(bundle_uuid, asset_uuid, kind, key)
                     VALUES (?1, ?2, ?3, ?4)",
                )?;
                if let Some(watched) = &row.watched {
                    for read in &watched.reads {
                        let (kind, key) = read.row();
                        insert.execute(rusqlite::params![
                            bundle,
                            watched.record.0.as_slice(),
                            kind,
                            key
                        ])?;
                    }
                }
                for (asset, listing_dir) in &row.directory_rules {
                    insert.execute(rusqlite::params![
                        bundle,
                        asset.0.as_slice(),
                        DIRECTORY_RULES,
                        listing_dir
                    ])?;
                }
            }
            Ok(())
        })
    }
}

fn uuid16(bytes: Vec<u8>) -> rusqlite::Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)
}

/// The watched imports, by bundle.
const WATCHED_IMPORTS: &str =
    "SELECT DISTINCT bundle_uuid FROM import_keys WHERE kind < 3 ORDER BY bundle_uuid";
/// The watched imports whose basis reads a path.
const WATCHED_READING_PATH: &str = "SELECT bundle_uuid FROM import_keys WHERE kind = 0 AND key = ?1";
/// The watched imports whose basis lists a directory or, with `?1`,
/// observes an importer capability.
const WATCHED_LISTING: &str =
    "SELECT bundle_uuid FROM import_keys WHERE kind = 1 OR (kind = 2 AND ?1)";
/// The rules rows' columns, joined to their bundles' sources.
const RULE_COLUMNS: &str = "SELECT r.name, b.path, k.bundle_uuid, k.asset_uuid
     FROM import_keys k JOIN bundles b USING (bundle_uuid) JOIN roots r ON r.root_id = b.root_id";

impl StoreReader {
    /// Every watched import's bundle, in order.
    pub fn watched_imports(&self) -> Result<Vec<BundleUuid>, StoreError> {
        self.query_rows(WATCHED_IMPORTS, [], |row| Ok(BundleUuid(uuid16(row.get(0)?)?)))
    }

    /// The bundles of the watched imports that read one of `paths`, list a
    /// directory, or (when `capabilities`) observe an importer capability,
    /// in order.
    pub fn watched_imports_reading<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a str>,
        capabilities: bool,
    ) -> Result<Vec<BundleUuid>, StoreError> {
        let mut found = std::collections::BTreeSet::new();
        let bundle = |row: &rusqlite::Row<'_>| Ok(BundleUuid(uuid16(row.get(0)?)?));
        for path in paths {
            found.extend(self.query_rows(WATCHED_READING_PATH, [path], bundle)?);
        }
        found.extend(self.query_rows(WATCHED_LISTING, [capabilities], bundle)?);
        Ok(found.into_iter().collect())
    }

    /// Every directory-import rules asset, by (root, path).
    pub fn directory_rule_sources(&self) -> Result<Vec<DirectoryRuleSource>, StoreError> {
        self.directory_rule_rows(
            &format!(
                "{RULE_COLUMNS} WHERE k.kind = 3
                 ORDER BY r.name, b.path, k.bundle_uuid, k.asset_uuid"
            ),
            rusqlite::params![],
        )
    }

    /// The directory-import rules assets whose listing directory is one of
    /// `dirs`: given the ancestor directories of a path (`""`, `a/`,
    /// `a/b/`, ...), every rule whose listing may match it. One indexed
    /// lookup per directory.
    pub fn directory_rule_sources_listing<'a>(
        &self,
        dirs: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<DirectoryRuleSource>, StoreError> {
        let mut found = Vec::new();
        for dir in dirs {
            found.extend(self.directory_rule_rows(
                &format!("{RULE_COLUMNS} WHERE k.kind = 3 AND k.key = ?1"),
                [dir],
            )?);
        }
        found.sort_by_key(|rule| (rule.rules_bundle, rule.rules_asset));
        found.dedup();
        Ok(found)
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
