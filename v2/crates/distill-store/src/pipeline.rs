//! Pipeline-side metadata (§13): the `pipeline_state` row and
//! `registrations`, the `tools` ToolEpoch table, and the
//! `schema_lineage` chain — full ordered schema-digest lists with
//! `"DSSL"` commitments gating automatic migration diffs (§6, §11).

use std::path::PathBuf;

use distill_core::id::{LogicalHash, TypeUuid};
use rusqlite::OptionalExtension;

use crate::bundles::blob32;
use crate::db::{InputTxn, Store};
use crate::error::StoreError;
use crate::state::{
    InputVersion, PipelineEpoch, PipelinePoison, PipelineState, Registration, RegistrationKind,
};

/// One published tool mapping (§13's `tools` row): registration staged a
/// content-addressed copy under daemon state, and jobs execute exactly
/// the staged copy the published hash names — never the live registered
/// path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedTool {
    pub key: String,
    pub path: PathBuf,
    pub content_hash: [u8; 32],
    pub input_version: InputVersion,
}

/// The direction marker beside every `schema_hash` (§6, §11), in the
/// superseding R22/C1 shape. The full ordered digest list is the ancestry
/// proof and complete predecessor record; the DSSL digest is its compact
/// commitment. Generation is derived from `digests.len()`, never trusted
/// as an independent authored number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageStamp {
    pub digests: Vec<LogicalHash>,
    pub chain_digest: [u8; 32],
}

impl LineageStamp {
    pub fn generation(&self) -> u64 {
        self.digests.len() as u64
    }

    fn is_valid_for(&self, type_uuid: TypeUuid, head: LogicalHash) -> bool {
        self.digests.last() == Some(&head)
            && self.chain_digest == lineage_chain_digest(type_uuid, &self.digests)
    }
}

/// One recorded `schema_lineage` row (§13): a chain position with the
/// hash it adopted and the `"DSSL"` digest assigned to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineageEntry {
    pub generation: u64,
    pub schema_hash: LogicalHash,
    pub chain_digest: [u8; 32],
}

/// The §6 chain-digest formula, pinned: `blake3("DSSL" ‖ version:u8 ‖
/// type_uuid ‖ count:u32 ‖ the ordered schema digests, oldest first)` —
/// §5's canonical sequence encoding under the registered `"DSSL"`
/// domain.
pub fn lineage_chain_digest(type_uuid: TypeUuid, digests: &[LogicalHash]) -> [u8; 32] {
    distill_core::canonical::domain_digest(distill_core::canonical::DSSL, 1, |e| {
        e.raw(&type_uuid.0);
        e.seq(digests, |e, d| e.raw(&d.0));
    })
}

/// Where the data's schema hash sits relative to the registry's current
/// on the recorded chain — or, where the chain does not cover it (state
/// loss), relative to the registry's position by lineage stamp (§6,
/// §11, §13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageClass {
    /// The data is at the registry's current — the walk terminates, no
    /// diff runs.
    AtCurrent,
    /// The data's hash sits strictly earlier on the recorded chain than
    /// the registry's current: the single automatic diff is legal.
    ForwardOnChain,
    /// The data is recorded — by chain position or by stamp — *ahead*
    /// of the registry's current: the registry is behind the data
    /// (§5's staleness window) — schema-dependent builds refuse with a
    /// staleness error naming both hashes; a deliberate rollback needs
    /// an explicit reverse custom edge.
    RegistryBehindData,
    /// No legal direction judgment exists: an explicit edge is required
    /// (the rev-rule hard stop, §11).
    HardStop(HardStopReason),
}

/// Why a placement hard-stopped (§11): unknown, divergent, or
/// positionless — never a heuristic tiebreak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardStopReason {
    /// The hash is on no recorded chain entry and the data carries no
    /// stamp: an unknown or unstamped schema.
    Unstamped,
    /// A stamp whose explicit ordered list is not prefix-comparable with
    /// the registry list, or whose `"DSSL"` commitment is invalid — a
    /// foreign branch, never a forward ancestor.
    Divergent,
    /// The registry's own current is on no recorded chain entry: no
    /// position to judge direction from (re-establish first, §11).
    UnknownPosition,
}

impl LineageClass {
    /// Whether §11's single trailing automatic diff is legal from this
    /// placement. `AtCurrent` is excluded not as a refusal but because
    /// the walk already terminated — no diff runs at all.
    pub fn permits_automatic_diff(self) -> bool {
        matches!(self, LineageClass::ForwardOnChain)
    }
}

impl InputTxn<'_> {
    /// Publish a validated pipeline epoch (§3, §13): the `pipeline_state`
    /// row plus the registration list, poison cleared.
    pub fn publish_pipeline_epoch(&mut self, epoch: &PipelineEpoch) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO pipeline_state(id, dylib_hash, load_policy_digest, input_version, poison)
             VALUES (0, ?1, ?2, ?3, NULL)
             ON CONFLICT(id) DO UPDATE SET
               dylib_hash = excluded.dylib_hash,
               load_policy_digest = excluded.load_policy_digest,
               input_version = excluded.input_version, poison = NULL",
            rusqlite::params![
                epoch.dylib_hash.as_slice(),
                epoch.load_policy_digest.as_slice(),
                self.version().0 as i64,
            ],
        )?;
        self.txn.execute("DELETE FROM registrations", [])?;
        for reg in &epoch.registrations {
            let kind = match reg.kind {
                RegistrationKind::Importer => 0i64,
                RegistrationKind::Processor => 1i64,
            };
            self.txn.execute(
                "INSERT INTO registrations(kind, reg_id, version) VALUES (?1, ?2, ?3)",
                rusqlite::params![kind, reg.id, reg.version],
            )?;
        }
        Ok(())
    }

    /// A rejected candidate still publishes (§13): the version carries a
    /// pipeline poison naming the error. The prior epoch's identity
    /// columns are retained as `last_good` residency bookkeeping — never
    /// served as this version's code.
    pub fn publish_pipeline_poison(&mut self, error: &str) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO pipeline_state(id, dylib_hash, load_policy_digest, input_version, poison)
             VALUES (0, NULL, NULL, ?1, ?2)
             ON CONFLICT(id) DO UPDATE SET
               input_version = excluded.input_version, poison = excluded.poison",
            rusqlite::params![self.version().0 as i64, error],
        )?;
        Ok(())
    }

    /// Stage a tool registration (§9, §13): write a content-addressed
    /// copy under `state_path/tools/<blake3-hex>` (fsynced — the copy
    /// must be durable before the mapping row can be read) and publish
    /// the mapping at this input version. Staged copies for different
    /// bytes coexist; the row rolls back with the transaction, leaving
    /// at worst an inert content-addressed orphan file.
    pub fn stage_tool(&mut self, key: &str, binary: &[u8]) -> Result<StagedTool, StoreError> {
        let content_hash = *blake3::hash(binary).as_bytes();
        let hex: String = content_hash.iter().map(|b| format!("{b:02x}")).collect();
        let tools_dir = self.state_path.join("tools");
        let path = tools_dir.join(&hex);
        let io = |p: &std::path::Path, source: std::io::Error| StoreError::Io {
            path: p.to_path_buf(),
            source,
        };
        if !path.exists() {
            let tmp = tools_dir.join(format!("{hex}.tmp"));
            std::fs::write(&tmp, binary).map_err(|e| io(&tmp, e))?;
            let f = std::fs::File::open(&tmp).map_err(|e| io(&tmp, e))?;
            f.sync_all().map_err(|e| io(&tmp, e))?;
            std::fs::rename(&tmp, &path).map_err(|e| io(&path, e))?;
            crate::cas::manifest::fsync_dir(&tools_dir)?;
        }
        self.txn.execute(
            "INSERT INTO tools(tool_key, staged_path, content_hash, input_version)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(tool_key) DO UPDATE SET
               staged_path = excluded.staged_path,
               content_hash = excluded.content_hash,
               input_version = excluded.input_version",
            rusqlite::params![
                key,
                path.to_string_lossy(),
                content_hash.as_slice(),
                self.version().0 as i64,
            ],
        )?;
        Ok(StagedTool {
            key: key.to_owned(),
            path,
            content_hash,
            input_version: self.version(),
        })
    }

    /// Adopt a candidate schema as a type's new current (§11, §13): each
    /// successfully staged module epoch appends the new current hash,
    /// assigning the next **generation** and the extended `"DSSL"`
    /// **chain digest** — the returned [`LineageStamp`] is what every
    /// schema-writing service stamps beside `schema_hash` (§6).
    ///
    /// Direction-checked at staging: a candidate whose digest already
    /// appears as a **non-head** entry is a *rollback* —
    /// [`StoreError::LineageRollback`], a hard stop with nothing
    /// appended. A candidate equal to the head is the unchanged schema:
    /// idempotent, returning the head's existing stamp.
    ///
    /// The digest extends over the recorded history in generation order
    /// — for a chain contiguous from generation 1 that is exactly §6's
    /// pinned formula; after a state-loss re-establishment it extends
    /// over the observed entries (position, not history, is what state
    /// loss costs — comparisons only ever run against this same chain).
    pub fn append_lineage(
        &mut self,
        type_uuid: TypeUuid,
        new: LogicalHash,
    ) -> Result<LineageStamp, StoreError> {
        let chain = lineage_rows(&self.txn, type_uuid)?;
        if let Some(entry) = chain.iter().find(|e| e.schema_hash == new) {
            let head = chain.last().expect("non-empty: entry found");
            if entry.generation == head.generation {
                // Unchanged schema: idempotent, no duplicate entry.
                let digests = chain.iter().map(|e| e.schema_hash).collect::<Vec<_>>();
                return Ok(LineageStamp {
                    chain_digest: lineage_chain_digest(type_uuid, &digests),
                    digests,
                });
            }
            return Err(StoreError::LineageRollback {
                type_uuid,
                candidate: new,
                head: head.schema_hash,
            });
        }
        let generation = chain.last().map(|e| e.generation + 1).unwrap_or(1);
        let mut digests: Vec<LogicalHash> = chain.iter().map(|e| e.schema_hash).collect();
        digests.push(new);
        let chain_digest = lineage_chain_digest(type_uuid, &digests);
        self.txn.execute(
            "INSERT INTO schema_lineage(type_uuid, generation, schema_hash, chain_digest, input_version)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                type_uuid.0.as_slice(),
                generation as i64,
                new.0.as_slice(),
                chain_digest.as_slice(),
                self.version().0 as i64,
            ],
        )?;
        Ok(LineageStamp {
            digests,
            chain_digest,
        })
    }

    /// Re-establish from one authored complete predecessor record. The
    /// observed lists are unioned only when prefix-consistent; one current
    /// stamp therefore reconstructs every missing intermediate position.
    pub fn reestablish_lineage(
        &mut self,
        type_uuid: TypeUuid,
        hash: LogicalHash,
        stamp: LineageStamp,
    ) -> Result<(), StoreError> {
        let generation = stamp.generation();
        if !stamp.is_valid_for(type_uuid, hash) {
            return Err(StoreError::LineageStampConflict {
                type_uuid,
                generation,
                detail: "stamp head or DSSL commitment does not match its explicit list".to_owned(),
            });
        }
        let chain = lineage_rows(&self.txn, type_uuid)?;
        let recorded: Vec<LogicalHash> = chain.iter().map(|e| e.schema_hash).collect();
        if !recorded.is_empty()
            && !is_prefix(&recorded, &stamp.digests)
            && !is_prefix(&stamp.digests, &recorded)
        {
            let mismatch = recorded
                .iter()
                .zip(&stamp.digests)
                .position(|(a, b)| a != b)
                .unwrap_or(recorded.len().min(stamp.digests.len()));
            return Err(StoreError::LineageStampConflict {
                type_uuid,
                generation: mismatch as u64 + 1,
                detail: "observed ordered list diverges from the reconstructed lineage".to_owned(),
            });
        }
        for (index, schema_hash) in stamp.digests.iter().enumerate().skip(recorded.len()) {
            let prefix = &stamp.digests[..=index];
            let digest = lineage_chain_digest(type_uuid, prefix);
            self.txn.execute(
                "INSERT INTO schema_lineage(type_uuid, generation, schema_hash, chain_digest, input_version)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    type_uuid.0.as_slice(),
                    (index + 1) as i64,
                    schema_hash.0.as_slice(),
                    digest.as_slice(),
                    self.version().0 as i64,
                ],
            )?;
        }
        Ok(())
    }
}

fn lineage_rows(
    conn: &rusqlite::Connection,
    type_uuid: TypeUuid,
) -> Result<Vec<LineageEntry>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT generation, schema_hash, chain_digest FROM schema_lineage
         WHERE type_uuid = ?1 ORDER BY generation",
    )?;
    let rows = stmt.query_map([type_uuid.0.as_slice()], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, Vec<u8>>(1)?,
            r.get::<_, Vec<u8>>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (generation, hash, digest) = row?;
        out.push(LineageEntry {
            generation: generation as u64,
            schema_hash: LogicalHash(blob32(hash)),
            chain_digest: blob32(digest),
        });
    }
    Ok(out)
}

fn is_prefix(a: &[LogicalHash], b: &[LogicalHash]) -> bool {
    a.len() <= b.len() && a == &b[..a.len()]
}

impl Store {
    /// The published pipeline state, or `None` before any publication.
    pub fn pipeline_state(&self) -> Result<Option<PipelineState>, StoreError> {
        // (dylib_hash, load_policy_digest, poison).
        type StateRow = (Option<Vec<u8>>, Option<Vec<u8>>, Option<String>);
        let row: Option<StateRow> = self
            .conn
            .query_row(
                "SELECT dylib_hash, load_policy_digest, poison FROM pipeline_state WHERE id = 0",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((dylib, lpd, poison)) = row else {
            return Ok(None);
        };

        let epoch = match (dylib, lpd) {
            (Some(dylib), Some(lpd)) => {
                let mut stmt = self
                    .conn
                    .prepare("SELECT kind, reg_id, version FROM registrations")?;
                let registrations: Vec<Registration> = stmt
                    .query_map([], |r| {
                        Ok(Registration {
                            kind: if r.get::<_, i64>(0)? == 0 {
                                RegistrationKind::Importer
                            } else {
                                RegistrationKind::Processor
                            },
                            id: r.get(1)?,
                            version: r.get(2)?,
                        })
                    })?
                    .collect::<Result<_, _>>()?;
                Some(std::sync::Arc::new(PipelineEpoch {
                    dylib_hash: blob32(dylib),
                    load_policy_digest: blob32(lpd),
                    registrations,
                }))
            }
            _ => None,
        };

        Ok(Some(match poison {
            None => match epoch {
                Some(e) => PipelineState::Ready(e),
                // A row with no identity and no poison cannot be
                // published through this API; treat as unpublished.
                None => return Ok(None),
            },
            Some(error) => PipelineState::Poisoned {
                error: PipelinePoison { error },
                last_good: epoch,
            },
        }))
    }

    /// Resolve a tool key through the ToolEpoch table (§13).
    pub fn tool(&self, key: &str) -> Result<Option<StagedTool>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT staged_path, content_hash, input_version FROM tools WHERE tool_key = ?1",
                [key],
                |r| {
                    Ok(StagedTool {
                        key: key.to_owned(),
                        path: PathBuf::from(r.get::<_, String>(0)?),
                        content_hash: {
                            let b: Vec<u8> = r.get(1)?;
                            let mut h = [0u8; 32];
                            h.copy_from_slice(&b);
                            h
                        },
                        input_version: InputVersion(r.get::<_, i64>(2)? as u64),
                    })
                },
            )
            .optional()?)
    }

    /// A type's recorded chain, in generation order. Possibly sparse
    /// after a state-loss re-establishment — the covered positions are
    /// exactly what "consistent where comparable" compares (§11).
    pub fn lineage(&self, type_uuid: TypeUuid) -> Result<Vec<LineageEntry>, StoreError> {
        lineage_rows(&self.conn, type_uuid)
    }

    /// The recorded [`LineageStamp`] for a hash on a type's chain — what
    /// a schema-writing service stamps beside `schema_hash` (§6).
    pub fn lineage_stamp(
        &self,
        type_uuid: TypeUuid,
        hash: LogicalHash,
    ) -> Result<Option<LineageStamp>, StoreError> {
        let chain = self.lineage(type_uuid)?;
        let Some(index) = chain.iter().position(|e| e.schema_hash == hash) else {
            return Ok(None);
        };
        let digests: Vec<_> = chain[..=index].iter().map(|e| e.schema_hash).collect();
        Ok(Some(LineageStamp {
            chain_digest: lineage_chain_digest(type_uuid, &digests),
            digests,
        }))
    }

    /// Classify `data`'s placement against `registry_current` (§11's
    /// direction gate). The chain rules first; where the chain does not
    /// cover the hash (state loss), the entry's own `data_stamp` — the
    /// §6 lineage stamp riding beside its `schema_hash` — decides: a
    /// first-sight automatic diff is legal **only** when the stamp's
    /// explicit digest list is a strict prefix of the registry list; an
    /// unknown or unstamped schema, a divergent stamp, or a stamp whose
    /// list strictly extends the registry's never is.
    pub fn classify_lineage(
        &self,
        type_uuid: TypeUuid,
        data: LogicalHash,
        data_stamp: Option<LineageStamp>,
        registry_current: LogicalHash,
    ) -> Result<LineageClass, StoreError> {
        if data == registry_current {
            return Ok(LineageClass::AtCurrent);
        }
        let chain = self.lineage(type_uuid)?;
        let registry_digests: Vec<_> = chain.iter().map(|e| e.schema_hash).collect();
        if registry_digests.last() != Some(&registry_current) {
            return Ok(LineageClass::HardStop(HardStopReason::UnknownPosition));
        }
        let Some(stamp) = data_stamp else {
            return Ok(LineageClass::HardStop(HardStopReason::Unstamped));
        };
        if !stamp.is_valid_for(type_uuid, data) {
            return Ok(LineageClass::HardStop(HardStopReason::Divergent));
        }
        if is_prefix(&stamp.digests, &registry_digests) {
            return Ok(LineageClass::ForwardOnChain);
        }
        if is_prefix(&registry_digests, &stamp.digests) {
            return Ok(LineageClass::RegistryBehindData);
        }
        Ok(LineageClass::HardStop(HardStopReason::Divergent))
    }
}
