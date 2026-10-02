//! Bounded reads that answer build-trace questions at one snapshot (§9).
//!
//! A build's trace records what it observed of the project: entry rows,
//! path resolutions, asset queries, tool epochs. Revalidating a trace at
//! another snapshot asks each recorded question again, so every read here
//! is an indexed point lookup whose cost follows the size of its answer,
//! never the size of the project. Together with the asset query
//! ([`StoreReader::namespace_assets_matching`]) and the existing point reads
//! ([`StoreReader::resolve_child`], [`StoreReader::path_assets`]) they are
//! everything the daemon's trace source reads.

use distill_core::id::{AssetUuid, BundleUuid, ContentHash, TypeUuid};
use rusqlite::OptionalExtension;

use crate::bundles::{blob16, blob32};
use crate::db::StoreReader;
use crate::error::StoreError;
use crate::state::InputVersion;

/// One asset row with its owning bundle's content hash and poison (two
/// primary keys).
pub(crate) const TRACE_ENTRY: &str =
    "SELECT a.bundle_uuid, b.content_hash, a.type_uuid, a.authoring_only, b.poison
     FROM assets a CROSS JOIN bundles b ON b.bundle_uuid = a.bundle_uuid
     WHERE a.asset_uuid = ?1";
/// The last ToolEpoch row of one key at a pinned version (primary key).
pub(crate) const TOOL_HASH_AT: &str = "SELECT present, tool_hash FROM tools
     WHERE tool_key = ?1 AND input_version <= ?2
     ORDER BY input_version DESC LIMIT 1";

/// What a build trace observes of one asset row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceEntry {
    /// The owning bundle file's content hash.
    pub bundle_hash: ContentHash,
    pub type_uuid: TypeUuid,
    pub authoring_only: bool,
}

impl StoreReader {
    /// The trace's view of `asset`'s row, in one statement: `Ok(None)` is a
    /// recordable miss; a poisoned owning bundle fails, as
    /// [`StoreReader::entry`] does.
    pub fn trace_entry(&self, asset: AssetUuid) -> Result<Option<TraceEntry>, StoreError> {
        let row = self
            .conn
            .prepare_cached(TRACE_ENTRY)?
            .query_row([asset.0.as_slice()], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .optional()?;
        let Some((bundle, bundle_hash, type_uuid, authoring_only, poison)) = row else {
            return Ok(None);
        };
        if let Some(error) = poison {
            return Err(StoreError::BundlePoisoned {
                bundle: BundleUuid(blob16(bundle)),
                error,
            });
        }
        Ok(Some(TraceEntry {
            bundle_hash: ContentHash(blob32(bundle_hash)),
            type_uuid: TypeUuid(blob16(type_uuid)),
            authoring_only,
        }))
    }

    /// The hash of the ToolEpoch registration of `key` visible at `basis`:
    /// its last row at or before `basis`, unless that row is a tombstone.
    /// Agrees with [`StoreReader::tool_hashes_at`] key by key.
    pub fn tool_hash_at(
        &self,
        key: &str,
        basis: InputVersion,
    ) -> Result<Option<[u8; 32]>, StoreError> {
        let row = self
            .conn
            .prepare_cached(TOOL_HASH_AT)?
            .query_row(
                rusqlite::params![key, i64::try_from(basis.0).unwrap_or(i64::MAX)],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?;
        match row {
            Some((true, hash)) => hash.try_into().map(Some).map_err(|_| {
                StoreError::InvalidToolIdentity(distill_core::tool::ToolIdentityError::Truncated)
            }),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Store, StoreConfig};

    /// Each trace read is planned on primary keys.
    #[test]
    fn trace_reads_are_planned_on_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        let cases: &[(&str, &[&str])] = &[
            (
                TRACE_ENTRY,
                &[
                    "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
                    "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
                ],
            ),
            (
                TOOL_HASH_AT,
                &["SEARCH tools USING INDEX sqlite_autoindex_tools_1 (tool_key=? AND input_version<?)"],
            ),
            (
                crate::pipeline::TOOL_AT,
                &["SEARCH tools USING INDEX sqlite_autoindex_tools_1 (tool_key=? AND input_version<?)"],
            ),
        ];
        for (sql, expected) in cases {
            let plan = store.query_plan_details(sql).unwrap();
            assert_eq!(&plan, expected, "{sql}");
        }
    }
}
