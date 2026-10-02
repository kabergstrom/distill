//! Bounded reads that answer build-trace questions at one snapshot (§9).
//!
//! A build's trace records what it observed of the project: entry rows,
//! path resolutions, asset queries, tool epochs. Revalidating a trace at
//! another snapshot asks each recorded question again, so every read here
//! is an indexed point lookup or an index range whose cost follows the
//! size of its answer, never the size of the project. Together with the
//! existing point reads ([`StoreReader::entry`], [`StoreReader::bundle`],
//! [`StoreReader::resolve_child`], [`StoreReader::path_assets`],
//! [`StoreReader::asset_ids_in_bundle`]) they are everything the daemon's
//! trace source reads.

use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use rusqlite::OptionalExtension;

use crate::bundles::blob16;
use crate::db::StoreReader;
use crate::error::StoreError;
use crate::state::InputVersion;

/// Assets of one authored type (`assets_by_type`).
pub(crate) const ASSETS_OF_TYPE: &str = "SELECT asset_uuid FROM assets WHERE type_uuid = ?1";
/// Assets carrying a tag (`asset_tags_by_tag`).
pub(crate) const ASSETS_WITH_TAG: &str = "SELECT asset_uuid FROM asset_tags WHERE tag = ?1";
/// Assets carrying a tag with one value (`asset_tags_by_tag`).
pub(crate) const ASSETS_WITH_TAG_VALUE: &str =
    "SELECT asset_uuid FROM asset_tags WHERE tag = ?1 AND value = ?2";
/// Assets of the bundles at one path (`bundles_by_path`, `assets_by_bundle`).
pub(crate) const ASSETS_AT_BUNDLE_PATH: &str = "SELECT a.asset_uuid FROM bundles b
     JOIN assets a ON a.bundle_uuid = b.bundle_uuid
     WHERE b.path = ?1";
/// The asset of one local id in one bundle (`assets_by_bundle`).
pub(crate) const LOCAL_ASSETS: &str =
    "SELECT asset_uuid FROM assets WHERE bundle_uuid = ?1 AND local_id = ?2";
/// The assets of one local id in the bundles at one path.
pub(crate) const LOCAL_ASSETS_AT_BUNDLE_PATH: &str = "SELECT a.asset_uuid FROM bundles b
     JOIN assets a ON a.bundle_uuid = b.bundle_uuid
     WHERE b.path = ?1 AND a.local_id = ?2";
/// Assets of the bundles whose paths lie in `[?1, ?2)`.
pub(crate) const ASSETS_IN_BUNDLE_PATH_RANGE: &str = "SELECT a.asset_uuid FROM bundles b
     JOIN assets a ON a.bundle_uuid = b.bundle_uuid
     WHERE b.path >= ?1 AND b.path < ?2";
/// Assets of the bundles whose paths are at least `?1`.
pub(crate) const ASSETS_FROM_BUNDLE_PATH: &str = "SELECT a.asset_uuid FROM bundles b
     JOIN assets a ON a.bundle_uuid = b.bundle_uuid
     WHERE b.path >= ?1";
/// Assets whose tag index is poisoned (the partial `asset_tag_index_poisoned`).
pub(crate) const TAG_POISONED_ASSETS: &str =
    "SELECT asset_uuid FROM asset_tag_index WHERE poison IS NOT NULL";
/// Whether one asset's tag index is poisoned (primary key).
pub(crate) const TAG_INDEX_POISONED: &str =
    "SELECT poison IS NOT NULL FROM asset_tag_index WHERE asset_uuid = ?1";
/// The last ToolEpoch row of one key at a pinned version (primary key).
pub(crate) const TOOL_HASH_AT: &str = "SELECT present, tool_hash FROM tools
     WHERE tool_key = ?1 AND input_version <= ?2
     ORDER BY input_version DESC LIMIT 1";

impl StoreReader {
    /// Every asset row of authored type `type_uuid`, in asset order.
    pub fn asset_ids_of_type(&self, type_uuid: TypeUuid) -> Result<Vec<AssetUuid>, StoreError> {
        self.asset_ids(ASSETS_OF_TYPE, rusqlite::params![type_uuid.0.as_slice()])
    }

    /// Every asset row carrying `tag` (with `value`, when given), in asset
    /// order.
    pub fn asset_ids_with_tag(
        &self,
        tag: &str,
        value: Option<&str>,
    ) -> Result<Vec<AssetUuid>, StoreError> {
        match value {
            None => self.asset_ids(ASSETS_WITH_TAG, rusqlite::params![tag]),
            Some(value) => self.asset_ids(ASSETS_WITH_TAG_VALUE, rusqlite::params![tag, value]),
        }
    }

    /// Every asset row of a bundle at exactly `path`, in asset order.
    pub fn asset_ids_at_bundle_path(&self, path: &str) -> Result<Vec<AssetUuid>, StoreError> {
        self.asset_ids(ASSETS_AT_BUNDLE_PATH, rusqlite::params![path])
    }

    /// The asset row of `local_id` in `bundle`, if any.
    pub fn local_asset_ids(
        &self,
        bundle: BundleUuid,
        local_id: &str,
    ) -> Result<Vec<AssetUuid>, StoreError> {
        self.asset_ids(
            LOCAL_ASSETS,
            rusqlite::params![bundle.0.as_slice(), local_id],
        )
    }

    /// The asset rows of `local_id` in the bundles at exactly `path`, in
    /// asset order.
    pub fn local_asset_ids_at_bundle_path(
        &self,
        path: &str,
        local_id: &str,
    ) -> Result<Vec<AssetUuid>, StoreError> {
        self.asset_ids(
            LOCAL_ASSETS_AT_BUNDLE_PATH,
            rusqlite::params![path, local_id],
        )
    }

    /// Every asset row of a bundle whose path starts with `prefix`, in
    /// asset order: one range of `bundles_by_path`.
    pub fn asset_ids_under_bundle_path(&self, prefix: &str) -> Result<Vec<AssetUuid>, StoreError> {
        // SQLite's BINARY collation compares UTF-8 byte-wise, as `str` does.
        match prefix_upper_bound(prefix) {
            Some(upper) => self.asset_ids(
                ASSETS_IN_BUNDLE_PATH_RANGE,
                rusqlite::params![prefix, upper],
            ),
            None => self.asset_ids(ASSETS_FROM_BUNDLE_PATH, rusqlite::params![prefix]),
        }
    }

    /// Every asset whose tag index is poisoned, in asset order.
    pub fn tag_poisoned_asset_ids(&self) -> Result<Vec<AssetUuid>, StoreError> {
        self.asset_ids(TAG_POISONED_ASSETS, rusqlite::params![])
    }

    /// Whether `asset` has a tag-index row and it is poisoned.
    pub fn tag_index_poisoned(&self, asset: AssetUuid) -> Result<bool, StoreError> {
        Ok(self
            .conn
            .prepare_cached(TAG_INDEX_POISONED)?
            .query_row([asset.0.as_slice()], |row| row.get::<_, bool>(0))
            .optional()?
            .unwrap_or(false))
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

    fn asset_ids(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Vec<AssetUuid>, StoreError> {
        let mut statement = self.conn.prepare_cached(sql)?;
        let rows = statement.query_map(params, |row| row.get::<_, Vec<u8>>(0))?;
        let mut assets = rows
            .map(|row| row.map(|bytes| AssetUuid(blob16(bytes))))
            .collect::<Result<Vec<_>, _>>()?;
        assets.sort_unstable();
        assets.dedup();
        Ok(assets)
    }
}

/// The least string greater than every string starting with `prefix`, or
/// `None` when there is none. UTF-8 orders byte-wise as code points do, so
/// bumping the last code point that can be bumped (and dropping what
/// follows it) bounds the prefix's strings from above.
pub(crate) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    while let Some(last) = chars.pop() {
        let next = match last {
            '\u{D7FF}' => Some('\u{E000}'),
            char::MAX => None,
            other => char::from_u32(other as u32 + 1),
        };
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Store, StoreConfig};

    #[test]
    fn prefix_upper_bounds_bound_exactly_the_prefixed_strings() {
        assert_eq!(prefix_upper_bound("ab").as_deref(), Some("ac"));
        assert_eq!(
            prefix_upper_bound("a\u{D7FF}").as_deref(),
            Some("a\u{E000}")
        );
        assert_eq!(prefix_upper_bound("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(prefix_upper_bound("\u{10FFFF}"), None);
        assert_eq!(prefix_upper_bound(""), None);
        for prefix in ["textures/", "a", "ö/x", "z\u{10FFFF}"] {
            let upper = prefix_upper_bound(prefix).unwrap();
            for candidate in [
                prefix.to_owned(),
                format!("{prefix}\u{10FFFF}\u{10FFFF}"),
                format!("{prefix}\0"),
            ] {
                assert!(
                    candidate.as_str() >= prefix && candidate < upper,
                    "{candidate:?}"
                );
            }
            assert!(upper.as_str() > prefix && !upper.starts_with(prefix));
        }
    }

    /// Each trace read is planned on an index: a search, or the scan of a
    /// partial index holding only the rows it answers.
    #[test]
    fn trace_reads_are_planned_on_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path().join("state"))).unwrap();
        let cases: &[(&str, &[&str])] = &[
            (ASSETS_OF_TYPE, &["SEARCH assets USING INDEX assets_by_type (type_uuid=?)"]),
            (
                ASSETS_WITH_TAG,
                &["SEARCH asset_tags USING INDEX asset_tags_by_tag (tag=?)"],
            ),
            (
                ASSETS_WITH_TAG_VALUE,
                &["SEARCH asset_tags USING INDEX asset_tags_by_tag (tag=? AND value=?)"],
            ),
            (
                ASSETS_AT_BUNDLE_PATH,
                &[
                    "SEARCH b USING INDEX bundles_by_path (path=?)",
                    "SEARCH a USING COVERING INDEX assets_by_bundle (bundle_uuid=?)",
                ],
            ),
            (
                LOCAL_ASSETS,
                &["SEARCH assets USING COVERING INDEX assets_by_bundle (bundle_uuid=? AND local_id=?)"],
            ),
            (
                LOCAL_ASSETS_AT_BUNDLE_PATH,
                &[
                    "SEARCH b USING INDEX bundles_by_path (path=?)",
                    "SEARCH a USING COVERING INDEX assets_by_bundle (bundle_uuid=? AND local_id=?)",
                ],
            ),
            (
                ASSETS_IN_BUNDLE_PATH_RANGE,
                &[
                    "SEARCH b USING INDEX bundles_by_path (path>? AND path<?)",
                    "SEARCH a USING COVERING INDEX assets_by_bundle (bundle_uuid=?)",
                ],
            ),
            (
                ASSETS_FROM_BUNDLE_PATH,
                &[
                    "SEARCH b USING INDEX bundles_by_path (path>?)",
                    "SEARCH a USING COVERING INDEX assets_by_bundle (bundle_uuid=?)",
                ],
            ),
            (
                TAG_POISONED_ASSETS,
                &["SCAN asset_tag_index USING INDEX asset_tag_index_poisoned"],
            ),
            (
                TAG_INDEX_POISONED,
                &["SEARCH asset_tag_index USING INDEX sqlite_autoindex_asset_tag_index_1 (asset_uuid=?)"],
            ),
            (
                TOOL_HASH_AT,
                &["SEARCH tools USING INDEX sqlite_autoindex_tools_1 (tool_key=? AND input_version<?)"],
            ),
        ];
        for (sql, expected) in cases {
            let plan = store.query_plan_details(sql).unwrap();
            assert_eq!(&plan, expected, "{sql}");
        }
    }
}
