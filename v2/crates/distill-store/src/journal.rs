//! §14 write-intent and displaced-inode journal.
//!
//! Quarantine is per filesystem, supplied by the file-tracking layer for
//! each watched root / daemon-owned output directory. Every displaced
//! inode gets an intent-derived unique name; content hashes are metadata
//! only and equal bytes never alias two live inodes.

use std::path::{Path, PathBuf};

use distill_core::id::ContentHash;
use rusqlite::OptionalExtension;

use crate::bundles::blob32;
use crate::db::Store;
use crate::error::StoreError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteIntent {
    pub intent_id: i64,
    pub target_path: String,
    pub temp_path: String,
    pub conflict_path: String,
    pub pre_image_hash: Option<ContentHash>,
    pub proposed_hash: ContentHash,
    pub quarantine_paths: Vec<PathBuf>,
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

impl Store {
    pub fn record_intent(
        &mut self,
        target_path: &str,
        temp_path: &str,
        conflict_path: &str,
        pre_image_hash: Option<ContentHash>,
        proposed_hash: ContentHash,
    ) -> Result<i64, StoreError> {
        self.conn.execute(
            "INSERT INTO write_intents(target_path, temp_path, conflict_path,
                                       pre_image_hash, proposed_hash, retired)
             VALUES (?1, ?2, ?3, ?4, ?5, 0)",
            rusqlite::params![
                target_path,
                temp_path,
                conflict_path,
                pre_image_hash.as_ref().map(|h| h.0.as_slice()),
                proposed_hash.0.as_slice(),
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn ensure_unretired_intent(&self, intent_id: i64) -> Result<(), StoreError> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM write_intents WHERE intent_id = ?1 AND retired = 0",
                [intent_id],
                |r| r.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "no unretired intent with this id".to_owned(),
            });
        }
        Ok(())
    }

    /// Journal then move one displaced inode into the caller's
    /// same-filesystem quarantine directory. A second displacement under
    /// one intent receives a numeric suffix (swap-back's proposed inode).
    pub fn quarantine_displaced(
        &mut self,
        intent_id: i64,
        displaced_file: &Path,
        quarantine_dir: &Path,
    ) -> Result<PathBuf, StoreError> {
        self.ensure_unretired_intent(intent_id)?;
        let io = |path: &Path| {
            let path = path.to_path_buf();
            move |source: std::io::Error| StoreError::Io { path, source }
        };
        std::fs::create_dir_all(quarantine_dir).map_err(io(quarantine_dir))?;
        ensure_same_filesystem(displaced_file, quarantine_dir)?;
        let bytes = std::fs::read(displaced_file).map_err(io(displaced_file))?;
        let hash = ContentHash(*blake3::hash(&bytes).as_bytes());
        let ordinal: u32 = self.conn.query_row(
            "SELECT COALESCE(MAX(ordinal) + 1, 0) FROM displaced WHERE intent_id = ?1",
            [intent_id],
            |r| r.get(0),
        )?;
        let name = if ordinal == 0 {
            format!("intent-{intent_id}")
        } else {
            format!("intent-{intent_id}-{ordinal}")
        };
        let qpath = quarantine_dir.join(name);
        if qpath.exists() {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: format!("quarantine destination already exists: {}", qpath.display()),
            });
        }
        let now = now_secs();
        // The physical destination is durable journal state before the
        // rename. A crash after this row is exactly an unfinished intent
        // for startup reconciliation to classify.
        self.conn.execute(
            "INSERT INTO displaced(intent_id, ordinal, content_hash, origin_path,
                                   quarantine_path, quarantined_at, restored)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
            rusqlite::params![
                intent_id,
                ordinal,
                hash.0.as_slice(),
                displaced_file.to_string_lossy(),
                qpath.to_string_lossy(),
                now,
            ],
        )?;
        std::fs::rename(displaced_file, &qpath).map_err(io(&qpath))?;
        crate::cas::manifest::fsync_dir(quarantine_dir)?;
        Ok(qpath)
    }

    /// Whole-file deletion uses the same journaled displacement family as
    /// rewrites: rename into quarantine, hash/compare the displaced inode,
    /// retain on success, restore and fail on mismatch.
    pub fn delete_with_intent(
        &mut self,
        intent_id: i64,
        target: &Path,
        quarantine_dir: &Path,
    ) -> Result<PathBuf, StoreError> {
        self.ensure_unretired_intent(intent_id)?;
        let (target_path, expected): (String, Option<Vec<u8>>) = self.conn.query_row(
            "SELECT target_path, pre_image_hash FROM write_intents WHERE intent_id = ?1",
            [intent_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if Path::new(&target_path) != target {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "delete target differs from journaled target".to_owned(),
            });
        }
        let expected = expected.ok_or_else(|| StoreError::BadIntent {
            intent_id,
            detail: "deletion intent has no expected pre-image".to_owned(),
        })?;
        let expected = blob32(expected);
        let qpath = self.quarantine_displaced(intent_id, target, quarantine_dir)?;
        let actual = *blake3::hash(&std::fs::read(&qpath).map_err(|source| StoreError::Io {
            path: qpath.clone(),
            source,
        })?)
        .as_bytes();
        if actual != expected {
            if target.exists() {
                return Err(StoreError::DeleteConflict {
                    intent_id,
                    expected,
                    actual,
                });
            }
            std::fs::rename(&qpath, target).map_err(|source| StoreError::Io {
                path: target.to_path_buf(),
                source,
            })?;
            self.conn.execute(
                "UPDATE displaced SET restored = 1 WHERE intent_id = ?1 AND quarantine_path = ?2",
                rusqlite::params![intent_id, qpath.to_string_lossy()],
            )?;
            if let Some(parent) = target.parent() {
                crate::cas::manifest::fsync_dir(parent)?;
            }
            return Err(StoreError::DeleteConflict {
                intent_id,
                expected,
                actual,
            });
        }
        self.retire_intent(intent_id)?;
        Ok(qpath)
    }

    pub fn retire_intent(&mut self, intent_id: i64) -> Result<(), StoreError> {
        let n = self.conn.execute(
            "UPDATE write_intents SET retired = 1 WHERE intent_id = ?1 AND retired = 0",
            [intent_id],
        )?;
        if n == 0 {
            return Err(StoreError::BadIntent {
                intent_id,
                detail: "no unretired intent with this id".to_owned(),
            });
        }
        Ok(())
    }

    pub fn unretired_intents(&self) -> Result<Vec<WriteIntent>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT intent_id, target_path, temp_path, conflict_path,
                    pre_image_hash, proposed_hash, retired
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
                    r.get::<_, i64>(6)? != 0,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::with_capacity(raw.len());
        for (id, target, temp, conflict, pre, proposed, retired) in raw {
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
                retired,
            });
        }
        Ok(out)
    }

    pub fn journal_owned_temp_paths(&self) -> Result<Vec<String>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT temp_path FROM write_intents WHERE retired = 1 ORDER BY intent_id")?;
        let rows = stmt
            .query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()
            .map_err(Into::into);
        rows
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
            .filter(|e| !e.restored && e.cleaned_at.is_none())
            .collect())
    }

    pub fn verify_quarantine(&self) -> Result<Vec<RecoveredEdit>, StoreError> {
        let mut diagnostics = Vec::new();
        for entry in self.quarantined_entries()? {
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
            if actual != Some(entry.content_hash) {
                diagnostics.push(RecoveredEdit {
                    origin_path: entry.origin_path,
                    expected: entry.content_hash,
                    actual,
                    quarantine_path: entry.path,
                });
            }
        }
        Ok(diagnostics)
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
        let mut touched_dirs = std::collections::BTreeSet::new();
        for entry in &expired {
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
                "UPDATE displaced SET cleaned_at = ?2, cleanup_reason = 'retention-expired'
                 WHERE displacement_id = ?1",
                rusqlite::params![entry.displacement_id, now_secs],
            )?;
            if let Some(parent) = entry.path.parent() {
                touched_dirs.insert(parent.to_path_buf());
            }
        }
        for dir in touched_dirs {
            crate::cas::manifest::fsync_dir(&dir)?;
        }
        Ok(expired.len())
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn ensure_same_filesystem(source: &Path, quarantine: &Path) -> Result<(), StoreError> {
    use std::os::unix::fs::MetadataExt;
    let source_dev = std::fs::metadata(source)
        .map_err(|error| StoreError::Io {
            path: source.to_path_buf(),
            source: error,
        })?
        .dev();
    let quarantine_dev = std::fs::metadata(quarantine)
        .map_err(|error| StoreError::Io {
            path: quarantine.to_path_buf(),
            source: error,
        })?
        .dev();
    if source_dev != quarantine_dev {
        return Err(StoreError::CrossFilesystemQuarantine {
            source: source.to_path_buf(),
            quarantine: quarantine.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_same_filesystem(_source: &Path, _quarantine: &Path) -> Result<(), StoreError> {
    // Windows' rename below remains the definitive same-volume check;
    // the platform fallback preserves the file as a named conflict.
    Ok(())
}
