//! Pipeline-side metadata (§13): the `pipeline_state` row and
//! `registrations`, the `tools` ToolEpoch table, and the
//! source-controlled schema-lineage manifest projection: append-only
//! accepted epochs, explicit parent links, an independent current cursor,
//! and `"DSSL"` commitments gating automatic migration diffs (§6, §11).

use std::collections::{BTreeMap, BTreeSet};
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

/// One accepted schema epoch in the source-controlled lineage manifest.
/// History is append-only; the parent records which prior current this
/// epoch was accepted as a forward successor of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedSchemaEpoch {
    pub digest: LogicalHash,
    pub forward_parent: Option<u32>,
}

/// A type's append-only accepted history and independently movable current
/// cursor. A rollback changes `current`, never `epochs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedTypeLineage {
    pub epochs: Vec<AcceptedSchemaEpoch>,
    pub current: u32,
}

/// The already parsed, unique source-controlled lineage authority. The
/// metadata store holds only a disposable projection of this value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaLineageManifest {
    pub types: BTreeMap<TypeUuid, AcceptedTypeLineage>,
}

/// One authored custom migration edge supplied to explicit rollback
/// validation. Automatic diffs are deliberately absent from this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReverseMigrationEdge {
    pub from: LogicalHash,
    pub to: LogicalHash,
}

/// The direction marker beside every `schema_hash` (§6, §11). `epochs` is
/// an exact prefix of the durable manifest, including parent links; `cursor`
/// selects the writing epoch and `chain` commits to both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageStamp {
    pub epochs: Vec<AcceptedSchemaEpoch>,
    pub cursor: u32,
    pub chain: [u8; 32],
}

impl LineageStamp {
    pub fn generation(&self) -> u64 {
        self.epochs.len() as u64
    }

    fn selected_digest(&self) -> Option<LogicalHash> {
        self.epochs
            .get(usize::try_from(self.cursor).ok()?)
            .map(|epoch| epoch.digest)
    }
}

/// One recorded append-only `schema_lineage` row (§13). The independently
/// movable current cursor and full-vector DSSL commitment live in
/// `schema_lineage_current`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineageEntry {
    pub generation: u64,
    pub schema_hash: LogicalHash,
    pub forward_parent: Option<u32>,
}

/// The §6 chain-digest formula over an accepted epoch prefix and its current
/// cursor. Parent links make ancestry explicit; vector order alone is never
/// a direction proof.
pub fn lineage_chain_digest(
    type_uuid: TypeUuid,
    epochs: &[AcceptedSchemaEpoch],
    cursor: u32,
) -> [u8; 32] {
    distill_core::canonical::domain_digest(distill_core::canonical::DSSL, 1, |e| {
        e.raw(&type_uuid.0);
        e.seq(epochs, |e, epoch| {
            e.raw(&epoch.digest.0);
            e.option(epoch.forward_parent, |e, parent| e.u32(*parent));
        });
        e.u32(cursor);
    })
}

/// Where the data's selected accepted epoch sits relative to the registry's
/// current by explicit parent reachability (§6, §11, §13).
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

/// Why a placement hard-stopped (§11): missing authority, unknown,
/// divergent, or positionless — never a heuristic tiebreak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardStopReason {
    /// The unique source-controlled manifest has not been projected. Bundle
    /// stamps cannot substitute for it, including after state loss.
    MissingManifest,
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

    /// Replace the disposable lineage projection from the already parsed,
    /// unique source-controlled manifest. This is the only state-rebuild
    /// path: bundle and migration-endpoint stamps are never unioned into
    /// authority. Validation completes before the old projection is touched.
    pub fn project_lineage_manifest(
        &mut self,
        manifest: &SchemaLineageManifest,
    ) -> Result<(), StoreError> {
        for (type_uuid, lineage) in &manifest.types {
            validate_type_lineage(*type_uuid, lineage)?;
        }
        validate_projection_transition(&self.txn, manifest)?;

        self.txn.execute("DELETE FROM schema_lineage_current", [])?;
        self.txn.execute("DELETE FROM schema_lineage", [])?;
        self.txn.execute("DELETE FROM schema_lineage_state", [])?;

        for (type_uuid, lineage) in &manifest.types {
            for (index, epoch) in lineage.epochs.iter().enumerate() {
                self.txn.execute(
                    "INSERT INTO schema_lineage(
                         type_uuid, generation, schema_hash, forward_parent, input_version
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        type_uuid.0.as_slice(),
                        (index + 1) as i64,
                        epoch.digest.0.as_slice(),
                        epoch.forward_parent.map(i64::from),
                        self.version().0 as i64,
                    ],
                )?;
            }
            write_lineage_current(&self.txn, self.version(), *type_uuid, lineage)?;
        }
        self.txn.execute(
            "INSERT INTO schema_lineage_state(id, input_version) VALUES (0, ?1)",
            [self.version().0 as i64],
        )?;
        Ok(())
    }

    /// Move an accepted type's current cursor to an existing non-current
    /// digest after validating total custom reverse paths from the old
    /// current and every supplied live data/migration-endpoint schema that
    /// is not already a forward ancestor of the requested cursor.
    ///
    /// The caller obtains `live_schema_hashes` (including migration
    /// endpoints) from one pinned source-tree snapshot; indexed live asset
    /// schemas are added automatically. The coordinator publishes the same
    /// validated cursor move to the source manifest through the journaled
    /// authoring protocol. This method updates only the projection's cursor
    /// row; accepted history remains append-only.
    pub fn rollback_lineage(
        &mut self,
        type_uuid: TypeUuid,
        target: LogicalHash,
        live_schema_hashes: &[LogicalHash],
        reverse_edges: &[ReverseMigrationEdge],
    ) -> Result<LineageStamp, StoreError> {
        ensure_manifest_available(&self.txn)?;
        let lineage = type_lineage(&self.txn, type_uuid)?.ok_or_else(|| {
            StoreError::IncompleteRollbackCoverage {
                type_uuid,
                target,
                source: target,
                detail: "type is absent from the accepted lineage manifest".to_owned(),
            }
        })?;
        let target_index = lineage
            .epochs
            .iter()
            .position(|epoch| epoch.digest == target)
            .ok_or_else(|| StoreError::IncompleteRollbackCoverage {
                type_uuid,
                target,
                source: target,
                detail: "target digest is not an accepted epoch".to_owned(),
            })? as u32;
        let old_current = lineage.current;
        if old_current == target_index {
            return stamp_for(type_uuid, &lineage);
        }

        let mut required_live = live_schema_hashes.to_vec();
        required_live.extend(live_asset_schema_hashes(&self.txn, type_uuid)?);

        validate_rollback_coverage(
            type_uuid,
            target,
            &lineage,
            target_index,
            &required_live,
            reverse_edges,
        )?;

        let moved = AcceptedTypeLineage {
            epochs: lineage.epochs,
            current: target_index,
        };
        write_lineage_current(&self.txn, self.version(), type_uuid, &moved)?;
        self.txn.execute(
            "UPDATE schema_lineage_state SET input_version = ?1 WHERE id = 0",
            [self.version().0 as i64],
        )?;
        stamp_for(type_uuid, &moved)
    }
}

fn lineage_rows(
    conn: &rusqlite::Connection,
    type_uuid: TypeUuid,
) -> Result<Vec<LineageEntry>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT generation, schema_hash, forward_parent FROM schema_lineage
         WHERE type_uuid = ?1 ORDER BY generation",
    )?;
    let rows = stmt.query_map([type_uuid.0.as_slice()], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, Vec<u8>>(1)?,
            r.get::<_, Option<i64>>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (generation, hash, forward_parent) = row?;
        let expected_generation = out.len() as u64 + 1;
        let generation = u64::try_from(generation)
            .map_err(|_| invalid_manifest(Some(type_uuid), "a projected generation is negative"))?;
        if generation != expected_generation {
            return Err(invalid_manifest(
                Some(type_uuid),
                "projected accepted generations are not contiguous",
            ));
        }
        let schema_hash = LogicalHash(hash.try_into().map_err(|_| {
            invalid_manifest(
                Some(type_uuid),
                "a projected schema digest is not exactly 32 bytes",
            )
        })?);
        let forward_parent = forward_parent
            .map(|parent| {
                u32::try_from(parent).map_err(|_| {
                    invalid_manifest(Some(type_uuid), "a projected parent index is invalid")
                })
            })
            .transpose()?;
        out.push(LineageEntry {
            generation,
            schema_hash,
            forward_parent,
        });
    }
    Ok(out)
}

fn validate_type_lineage(
    type_uuid: TypeUuid,
    lineage: &AcceptedTypeLineage,
) -> Result<(), StoreError> {
    if lineage.epochs.is_empty() {
        return Err(invalid_manifest(
            Some(type_uuid),
            "a manifest type must contain at least one accepted epoch",
        ));
    }
    if lineage.epochs.len() > u32::MAX as usize {
        return Err(invalid_manifest(
            Some(type_uuid),
            "accepted epoch count exceeds the DSSL u32 sequence bound",
        ));
    }
    if usize::try_from(lineage.current)
        .ok()
        .filter(|current| *current < lineage.epochs.len())
        .is_none()
    {
        return Err(invalid_manifest(
            Some(type_uuid),
            "current cursor is outside the accepted epoch vector",
        ));
    }
    let mut digests = BTreeSet::new();
    for (index, epoch) in lineage.epochs.iter().enumerate() {
        if !digests.insert(epoch.digest) {
            return Err(invalid_manifest(
                Some(type_uuid),
                "one digest appears in more than one accepted epoch",
            ));
        }
        match (index, epoch.forward_parent) {
            (0, None) => {}
            (0, Some(_)) => {
                return Err(invalid_manifest(
                    Some(type_uuid),
                    "the first accepted epoch must not have a forward parent",
                ));
            }
            (_, Some(parent)) if (parent as usize) < index => {}
            (_, Some(_)) => {
                return Err(invalid_manifest(
                    Some(type_uuid),
                    "a forward parent must name an earlier accepted epoch",
                ));
            }
            (_, None) => {
                return Err(invalid_manifest(
                    Some(type_uuid),
                    "only the first accepted epoch may omit its forward parent",
                ));
            }
        }
    }
    Ok(())
}

fn validate_projection_transition(
    conn: &rusqlite::Connection,
    proposed: &SchemaLineageManifest,
) -> Result<(), StoreError> {
    if !manifest_available(conn)? {
        return Ok(());
    }
    let recorded = projected_manifest(conn)?;
    for (type_uuid, old) in &recorded.types {
        let Some(new) = proposed.types.get(type_uuid) else {
            return Err(invalid_manifest(
                Some(*type_uuid),
                "an accepted type cannot be removed from append-only history",
            ));
        };
        if new.epochs.len() < old.epochs.len() || new.epochs[..old.epochs.len()] != old.epochs {
            return Err(invalid_manifest(
                Some(*type_uuid),
                "accepted epoch history is not an append-only extension",
            ));
        }
        if new.epochs.len() == old.epochs.len() {
            if new.current != old.current {
                return Err(StoreError::LineageRollback {
                    type_uuid: *type_uuid,
                    candidate: new.epochs[new.current as usize].digest,
                    current: old.epochs[old.current as usize].digest,
                });
            }
        } else {
            validate_acceptance_extension(*type_uuid, old, new)?;
        }
    }
    for (type_uuid, new) in &proposed.types {
        if !recorded.types.contains_key(type_uuid) && new.current as usize != new.epochs.len() - 1 {
            return Err(invalid_manifest(
                Some(*type_uuid),
                "a newly accepted type must select its newly appended epoch",
            ));
        }
    }
    Ok(())
}

fn validate_acceptance_extension(
    type_uuid: TypeUuid,
    old: &AcceptedTypeLineage,
    new: &AcceptedTypeLineage,
) -> Result<(), StoreError> {
    if new.current as usize != new.epochs.len() - 1 {
        return Err(invalid_manifest(
            Some(type_uuid),
            "ordinary acceptance must advance to the newly appended epoch",
        ));
    }
    if new.epochs[old.epochs.len()].forward_parent != Some(old.current) {
        return Err(invalid_manifest(
            Some(type_uuid),
            "the first appended epoch must name the prior current as its forward parent",
        ));
    }
    Ok(())
}

fn invalid_manifest(type_uuid: Option<TypeUuid>, detail: &str) -> StoreError {
    StoreError::InvalidLineageManifest {
        type_uuid,
        detail: detail.to_owned(),
    }
}

fn ensure_manifest_available(conn: &rusqlite::Connection) -> Result<(), StoreError> {
    if manifest_available(conn)? {
        Ok(())
    } else {
        Err(StoreError::LineageManifestUnavailable)
    }
}

fn manifest_available(conn: &rusqlite::Connection) -> Result<bool, StoreError> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM schema_lineage_state WHERE id = 0",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn type_lineage(
    conn: &rusqlite::Connection,
    type_uuid: TypeUuid,
) -> Result<Option<AcceptedTypeLineage>, StoreError> {
    let rows = lineage_rows(conn, type_uuid)?;
    let current_row: Option<(i64, Vec<u8>)> = conn
        .query_row(
            "SELECT current_cursor, chain_digest FROM schema_lineage_current
             WHERE type_uuid = ?1",
            [type_uuid.0.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((current, chain)) = current_row else {
        if rows.is_empty() {
            return Ok(None);
        }
        return Err(invalid_manifest(
            Some(type_uuid),
            "accepted history has no current cursor",
        ));
    };
    let current = u32::try_from(current).map_err(|_| {
        invalid_manifest(Some(type_uuid), "the projected current cursor is invalid")
    })?;
    let lineage = AcceptedTypeLineage {
        epochs: rows
            .into_iter()
            .map(|entry| AcceptedSchemaEpoch {
                digest: entry.schema_hash,
                forward_parent: entry.forward_parent,
            })
            .collect(),
        current,
    };
    validate_type_lineage(type_uuid, &lineage)?;
    let chain: [u8; 32] = chain.try_into().map_err(|_| {
        invalid_manifest(
            Some(type_uuid),
            "the projected DSSL commitment is not exactly 32 bytes",
        )
    })?;
    if chain != lineage_chain_digest(type_uuid, &lineage.epochs, lineage.current) {
        return Err(invalid_manifest(
            Some(type_uuid),
            "the disposable projection's DSSL commitment does not verify",
        ));
    }
    Ok(Some(lineage))
}

fn projected_manifest(conn: &rusqlite::Connection) -> Result<SchemaLineageManifest, StoreError> {
    let mut stmt =
        conn.prepare("SELECT type_uuid FROM schema_lineage_current ORDER BY type_uuid")?;
    let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    let mut types = BTreeMap::new();
    for row in rows {
        let bytes = row?;
        let type_uuid = TypeUuid(bytes.try_into().map_err(|_| {
            invalid_manifest(
                None,
                "a projected lineage type UUID is not exactly 16 bytes",
            )
        })?);
        let lineage = type_lineage(conn, type_uuid)?.ok_or_else(|| {
            invalid_manifest(
                Some(type_uuid),
                "a projected current cursor has no accepted history",
            )
        })?;
        types.insert(type_uuid, lineage);
    }
    Ok(SchemaLineageManifest { types })
}

fn write_lineage_current(
    conn: &rusqlite::Connection,
    version: InputVersion,
    type_uuid: TypeUuid,
    lineage: &AcceptedTypeLineage,
) -> Result<(), StoreError> {
    let chain = lineage_chain_digest(type_uuid, &lineage.epochs, lineage.current);
    conn.execute(
        "INSERT INTO schema_lineage_current(
             type_uuid, current_cursor, chain_digest, input_version
         ) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(type_uuid) DO UPDATE SET
             current_cursor = excluded.current_cursor,
             chain_digest = excluded.chain_digest,
             input_version = excluded.input_version",
        rusqlite::params![
            type_uuid.0.as_slice(),
            i64::from(lineage.current),
            chain.as_slice(),
            version.0 as i64,
        ],
    )?;
    Ok(())
}

fn stamp_for(
    type_uuid: TypeUuid,
    lineage: &AcceptedTypeLineage,
) -> Result<LineageStamp, StoreError> {
    validate_type_lineage(type_uuid, lineage)?;
    Ok(LineageStamp {
        chain: lineage_chain_digest(type_uuid, &lineage.epochs, lineage.current),
        epochs: lineage.epochs.clone(),
        cursor: lineage.current,
    })
}

fn is_ancestor(lineage: &AcceptedTypeLineage, ancestor: u32, descendant: u32) -> bool {
    let mut cursor = Some(descendant);
    while let Some(index) = cursor {
        if index == ancestor {
            return true;
        }
        cursor = lineage.epochs[index as usize].forward_parent;
    }
    false
}

fn validate_rollback_coverage(
    type_uuid: TypeUuid,
    target: LogicalHash,
    lineage: &AcceptedTypeLineage,
    target_index: u32,
    live_schema_hashes: &[LogicalHash],
    reverse_edges: &[ReverseMigrationEdge],
) -> Result<(), StoreError> {
    let positions: BTreeMap<_, _> = lineage
        .epochs
        .iter()
        .enumerate()
        .map(|(index, epoch)| (epoch.digest, index as u32))
        .collect();
    let mut outgoing: BTreeMap<LogicalHash, Vec<LogicalHash>> = BTreeMap::new();
    for edge in reverse_edges {
        if !positions.contains_key(&edge.from) || !positions.contains_key(&edge.to) {
            return Err(coverage_error(
                type_uuid,
                target,
                edge.from,
                "a supplied reverse edge endpoint is not an accepted epoch",
            ));
        }
        outgoing.entry(edge.from).or_default().push(edge.to);
    }

    let old_current = lineage.epochs[lineage.current as usize].digest;
    let mut required = BTreeSet::from([old_current]);
    for source in live_schema_hashes {
        let Some(&position) = positions.get(source) else {
            return Err(coverage_error(
                type_uuid,
                target,
                *source,
                "a live data or migration-endpoint schema is not accepted",
            ));
        };
        if !is_ancestor(lineage, position, target_index) {
            required.insert(*source);
        }
    }

    for source in required {
        let mut cursor = source;
        let mut visited = BTreeSet::new();
        while cursor != target {
            if !visited.insert(cursor) {
                return Err(coverage_error(
                    type_uuid,
                    target,
                    source,
                    "custom reverse path is cyclic",
                ));
            }
            let Some(next) = outgoing.get(&cursor) else {
                return Err(coverage_error(
                    type_uuid,
                    target,
                    source,
                    "custom reverse path is incomplete",
                ));
            };
            if next.len() != 1 {
                return Err(coverage_error(
                    type_uuid,
                    target,
                    source,
                    "custom reverse path is ambiguous",
                ));
            }
            cursor = next[0];
        }
    }
    Ok(())
}

fn live_asset_schema_hashes(
    conn: &rusqlite::Connection,
    type_uuid: TypeUuid,
) -> Result<Vec<LogicalHash>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT logical_hash FROM assets
         WHERE type_uuid = ?1 AND logical_hash IS NOT NULL",
    )?;
    let rows = stmt.query_map([type_uuid.0.as_slice()], |row| row.get::<_, Vec<u8>>(0))?;
    rows.map(|row| {
        let bytes = row?;
        Ok(LogicalHash(bytes.try_into().map_err(|_| {
            invalid_manifest(
                Some(type_uuid),
                "a live asset's schema digest is not exactly 32 bytes",
            )
        })?))
    })
    .collect()
}

fn coverage_error(
    type_uuid: TypeUuid,
    target: LogicalHash,
    source: LogicalHash,
    detail: &str,
) -> StoreError {
    StoreError::IncompleteRollbackCoverage {
        type_uuid,
        target,
        source,
        detail: detail.to_owned(),
    }
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

    /// Whether the unique source-controlled lineage manifest has been
    /// validated and projected for this store instance.
    pub fn lineage_manifest_available(&self) -> Result<bool, StoreError> {
        manifest_available(&self.conn)
    }

    /// A type's append-only accepted epoch history, in manifest order.
    pub fn lineage(&self, type_uuid: TypeUuid) -> Result<Vec<LineageEntry>, StoreError> {
        lineage_rows(&self.conn, type_uuid)
    }

    /// The digest selected by the type's independent current cursor.
    pub fn lineage_current(&self, type_uuid: TypeUuid) -> Result<Option<LogicalHash>, StoreError> {
        Ok(type_lineage(&self.conn, type_uuid)?
            .map(|lineage| lineage.epochs[lineage.current as usize].digest))
    }

    /// The stamp for a value written at the accepted current cursor. It
    /// carries the full accepted vector, so a rollback cursor may select a
    /// non-final entry without erasing later history.
    pub fn current_lineage_stamp(
        &self,
        type_uuid: TypeUuid,
    ) -> Result<Option<LineageStamp>, StoreError> {
        type_lineage(&self.conn, type_uuid)?
            .map(|lineage| stamp_for(type_uuid, &lineage))
            .transpose()
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
        if !self.lineage_manifest_available()? {
            return Ok(LineageClass::HardStop(HardStopReason::MissingManifest));
        }
        let Some(lineage) = type_lineage(&self.conn, type_uuid)? else {
            return Ok(LineageClass::HardStop(HardStopReason::UnknownPosition));
        };
        let accepted_current = lineage.epochs[lineage.current as usize].digest;
        if registry_current != accepted_current {
            return Ok(LineageClass::HardStop(HardStopReason::UnknownPosition));
        }
        if data == registry_current {
            return Ok(LineageClass::AtCurrent);
        }
        let Some(stamp) = data_stamp else {
            return Ok(LineageClass::HardStop(HardStopReason::Unstamped));
        };
        let Some(data_position) =
            validate_stamp_against_manifest(type_uuid, data, &stamp, &lineage)
        else {
            return Ok(LineageClass::HardStop(HardStopReason::Divergent));
        };
        if is_ancestor(&lineage, data_position, lineage.current) {
            return Ok(LineageClass::ForwardOnChain);
        }
        if is_ancestor(&lineage, lineage.current, data_position) {
            return Ok(LineageClass::RegistryBehindData);
        }
        Ok(LineageClass::HardStop(HardStopReason::Divergent))
    }
}

fn validate_stamp_against_manifest(
    type_uuid: TypeUuid,
    data: LogicalHash,
    stamp: &LineageStamp,
    lineage: &AcceptedTypeLineage,
) -> Option<u32> {
    let cursor = usize::try_from(stamp.cursor).ok()?;
    if stamp.epochs.is_empty()
        || stamp.epochs.len() > lineage.epochs.len()
        || cursor >= stamp.epochs.len()
        || stamp.epochs != lineage.epochs[..stamp.epochs.len()]
        || stamp.selected_digest()? != data
        || stamp.chain != lineage_chain_digest(type_uuid, &stamp.epochs, stamp.cursor)
    {
        return None;
    }
    Some(stamp.cursor)
}
