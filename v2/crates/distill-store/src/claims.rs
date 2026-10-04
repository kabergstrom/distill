//! Scan claims: what each scanned `.bundle` source claims — its bundle
//! UUID, its authored asset UUIDs, the derived outputs its assets project,
//! its primary path, or (for an unreadable skeleton) a
//! namespace error. Rows are keyed by the claiming (root, path), so a scan
//! replaces exactly the claims of the subtree it observed.
//!
//! Two things follow from the claims in the same transaction: every bundle
//! or asset subject with more than one distinct claimant has its namespace
//! error row (`errors`, the scan's family, scoped to the subject), and a
//! subtree replacement returns the bundle, derived-output and path subjects
//! it changed, for the publication that follows it in that transaction.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::OptionalExtension;

use distill_core::canonical::CanonicalEncoder;
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};

use crate::db::{InputTxn, StoreReader};
use crate::error::StoreError;
use crate::errors::{write_namespace_error, NAMESPACE};
use crate::state::{
    encode_asset_claimant, encode_bundle_source, AssetClaimant, ReadableBundleSource, NamespaceError, NamespaceErrorDecoder,
    NamespaceErrorDecodeError, NamespaceErrorV1,
};

const BUNDLE: i64 = 0;
const AUTHORED: i64 = 1;
const DERIVED: i64 = 2;
const PRIMARY_PATH: i64 = 3;
const MALFORMED: i64 = 5;

/// Collision groups: bundle UUIDs collide among themselves, authored and
/// derived asset UUIDs with each other.
const BUNDLE_GROUP: i64 = 0;
const ASSET_GROUP: i64 = 1;

/// One claim of one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceClaim {
    Bundle {
        bundle: BundleUuid,
        source: ReadableBundleSource,
    },
    /// An authored asset; `claimant` is [`AssetClaimant::Authored`].
    Authored {
        asset: AssetUuid,
        claimant: AssetClaimant,
    },
    DerivedOutput {
        child: AssetUuid,
        output: DerivedOutputClaim,
    },
    PrimaryPath {
        path: String,
        asset: AssetUuid,
    },
    /// The source's skeleton is unreadable.
    Malformed(NamespaceError),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DerivedOutputClaim {
    pub parent: AssetUuid,
    pub output_key: String,
    pub terminal_type: TypeUuid,
}

/// Every claim of one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceClaims {
    pub root_name: String,
    pub path: String,
    pub claims: Vec<SourceClaim>,
}

/// Subjects a claims replacement changed: what the publication that
/// follows it in its transaction republishes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingClaims {
    /// Each bundle with the distinct sources claiming it, in claimant
    /// order: the claims the replacement read refreshing its collision.
    pub bundles: BTreeMap<BundleUuid, Vec<ReadableBundleSource>>,
    pub paths: BTreeSet<String>,
}

fn claim_row(claim: &SourceClaim) -> Result<(i64, Vec<u8>, Vec<u8>, Vec<u8>), StoreError> {
    let encoded = |encode: &dyn Fn(&mut CanonicalEncoder)| {
        let mut encoder = CanonicalEncoder::new();
        encode(&mut encoder);
        encoder.into_bytes()
    };
    Ok(match claim {
        SourceClaim::Bundle { bundle, source } => (
            BUNDLE,
            bundle.0.to_vec(),
            encoded(&|encoder| encode_bundle_source(encoder, source)),
            Vec::new(),
        ),
        SourceClaim::Authored { asset, claimant } => (
            AUTHORED,
            asset.0.to_vec(),
            encoded(&|encoder| encode_asset_claimant(encoder, claimant)),
            Vec::new(),
        ),
        SourceClaim::DerivedOutput { child, output } => (
            DERIVED,
            child.0.to_vec(),
            encoded(&|encoder| {
                encode_asset_claimant(
                    encoder,
                    &AssetClaimant::Derived {
                        parent: output.parent,
                        output_key: output.output_key.clone(),
                    },
                )
            }),
            output.terminal_type.0.to_vec(),
        ),
        SourceClaim::PrimaryPath { path, asset } => (
            PRIMARY_PATH,
            path.as_bytes().to_vec(),
            asset.0.to_vec(),
            Vec::new(),
        ),
        SourceClaim::Malformed(error) => (
            MALFORMED,
            Vec::new(),
            error
                .persisted_bytes()
                .map_err(StoreError::InvalidNamespaceError)?,
            Vec::new(),
        ),
    })
}

fn collision_group(kind: i64) -> Option<i64> {
    match kind {
        BUNDLE => Some(BUNDLE_GROUP),
        AUTHORED | DERIVED => Some(ASSET_GROUP),
        _ => None,
    }
}

fn group_kinds(group: i64) -> &'static str {
    if group == BUNDLE_GROUP {
        "(0)"
    } else {
        "(1, 2)"
    }
}

/// The `errors.scope_kind` of a collision group's rows: bundle, asset.
fn group_scope(group: i64) -> i64 {
    if group == BUNDLE_GROUP {
        2
    } else {
        3
    }
}

/// The distinct claimants of `subject` in `group`, in order.
fn group_claimants(
    conn: &rusqlite::Connection,
    group: i64,
    subject: &[u8],
) -> Result<Vec<Vec<u8>>, StoreError> {
    let mut select = conn.prepare_cached(&format!(
        "SELECT DISTINCT claimant FROM source_claims
         WHERE kind IN {} AND subject = ?1 ORDER BY claimant",
        group_kinds(group)
    ))?;
    let rows = select.query_map([subject], |row| row.get::<_, Vec<u8>>(0))?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

/// The namespace error of `subject` claimed by every one of `claimants`.
fn collision_error(
    group: i64,
    subject: &[u8],
    claimants: &[Vec<u8>],
) -> Result<NamespaceError, StoreError> {
    let error = if group == BUNDLE_GROUP {
        let bundle = BundleUuid(uuid16(subject)?);
        let mut sources = claimants
            .iter()
            .map(|bytes| decode_bundle_source(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        sources.sort();
        NamespaceError::new(
            NamespaceErrorV1::DuplicateBundleUuid { bundle, sources },
            format!("duplicate bundle UUID {bundle}"),
        )
    } else {
        let asset = AssetUuid(uuid16(subject)?);
        let mut claimants = claimants
            .iter()
            .map(|bytes| decode_asset_claimant(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        claimants.sort();
        NamespaceError::new(
            NamespaceErrorV1::DuplicateAssetUuid { asset, claimants },
            format!("duplicate asset UUID {asset}"),
        )
    };
    error.map_err(invalid_namespace_error)
}

/// Write `error`, a collision's, as a row of the scan's family.
fn write_collision(conn: &rusqlite::Connection, error: &NamespaceError) -> Result<(), StoreError> {
    let record = error
        .persisted_bytes()
        .map_err(StoreError::InvalidNamespaceError)?;
    write_namespace_error(conn, NAMESPACE, error, &record)
}

fn delete_collision(conn: &rusqlite::Connection, identity: &[u8]) -> Result<(), StoreError> {
    conn.prepare_cached("DELETE FROM errors WHERE family = ?1 AND identity = ?2")?
        .execute(rusqlite::params![NAMESPACE, identity])?;
    Ok(())
}

/// Bring `subject`'s collision row in `group` up to date with its claims.
/// Returns whether it collided before, whether it does now, and its
/// distinct claimants.
fn refresh_collision(
    conn: &rusqlite::Connection,
    group: i64,
    subject: &[u8],
) -> Result<(bool, bool, Vec<Vec<u8>>), StoreError> {
    let recorded: Option<Vec<u8>> = conn
        .prepare_cached(
            "SELECT identity FROM errors WHERE scope_kind = ?1 AND scope_id = ?2 AND family = ?3",
        )?
        .query_row(rusqlite::params![group_scope(group), subject, NAMESPACE], |row| row.get(0))
        .optional()?;
    let claimants = group_claimants(conn, group, subject)?;
    let current = if claimants.len() > 1 {
        Some(collision_error(group, subject, &claimants)?)
    } else {
        None
    };
    let unchanged = matches!(
        (&recorded, &current),
        (Some(identity), Some(error)) if identity.as_slice() == error.identity.as_slice()
    );
    if !unchanged {
        if let Some(identity) = &recorded {
            delete_collision(conn, identity)?;
        }
        if let Some(error) = &current {
            write_collision(conn, error)?;
        }
    }
    Ok((recorded.is_some(), current.is_some(), claimants))
}

/// An asset UUID started or stopped colliding: what publishes it (its
/// claimant bundles, its derived output, the paths naming it) is pending,
/// since the asset is withheld from publication while it collides.
fn asset_dependents_pending(
    conn: &rusqlite::Connection,
    asset: &[u8],
    bundles: &mut BTreeSet<BundleUuid>,
    paths: &mut BTreeSet<String>,
) -> Result<(), StoreError> {
    let claimants = {
        let mut select = conn.prepare_cached(
            "SELECT claimant FROM source_claims WHERE kind = ?1 AND subject = ?2",
        )?;
        let rows = select.query_map(rusqlite::params![AUTHORED, asset], |row| {
            row.get::<_, Vec<u8>>(0)
        })?;
        rows.collect::<Result<BTreeSet<_>, _>>()?
    };
    for claimant in claimants {
        if let AssetClaimant::Authored { bundle, .. } = decode_asset_claimant(&claimant)? {
            bundles.insert(bundle);
        }
    }
    let mut naming = conn.prepare_cached(
        "SELECT subject FROM source_claims WHERE kind = ?1 AND claimant = ?2",
    )?;
    for path in naming.query_map(rusqlite::params![PRIMARY_PATH, asset], |row| {
        row.get::<_, Vec<u8>>(0)
    })? {
        paths.insert(path_subject(path?)?);
    }
    Ok(())
}

fn path_subject(subject: Vec<u8>) -> Result<String, StoreError> {
    String::from_utf8(subject)
        .map_err(|_| invalid_namespace_error(NamespaceErrorDecodeError::InvalidUtf8))
}

impl InputTxn<'_> {
    /// Replace the claims of every source under `under` (every source when
    /// `None`) with `sources`. A subtree replacement refreshes the
    /// collisions of the subjects it touched and returns them pending; a
    /// full replacement recomputes every collision and returns nothing
    /// pending.
    pub fn replace_source_claims(
        &mut self,
        under: Option<&[(String, String)]>,
        sources: &[SourceClaims],
    ) -> Result<PendingClaims, StoreError> {
        let Some(prefixes) = under else {
            self.replace_all_source_claims(sources)?;
            return Ok(PendingClaims::default());
        };
        let conn = &*self.txn;
        let mut touched = BTreeSet::<(i64, Vec<u8>)>::new();
        for (root, prefix) in prefixes {
            let mut select = conn.prepare_cached(&format!(
                "SELECT t.kind, t.subject FROM source_claims t JOIN roots r USING (root_id)
                 WHERE {}",
                crate::files::under_sql(prefix)
            ))?;
            let rows = select.query_map(rusqlite::params![root, prefix], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
            for row in rows {
                touched.insert(row?);
            }
            conn.execute(
                &format!(
                    "DELETE FROM source_claims WHERE rowid IN (
                       SELECT t.rowid FROM source_claims t JOIN roots r USING (root_id)
                       WHERE {})",
                    crate::files::under_sql(prefix)
                ),
                rusqlite::params![root, prefix],
            )?;
        }
        for source in sources {
            let root = self.intern_root(&source.root_name)?;
            for claim in &source.claims {
                let (kind, subject, claimant, detail) = claim_row(claim)?;
                self.txn
                    .prepare_cached(
                        "INSERT OR REPLACE INTO source_claims(root_id, path, kind, subject, claimant, detail)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    )?
                    .execute(rusqlite::params![root.0, source.path, kind, subject, claimant, detail])?;
                touched.insert((kind, subject));
            }
        }
        let conn = &*self.txn;
        let mut refreshed = BTreeSet::new();
        let mut pending = PendingClaims::default();
        let mut bundles = BTreeSet::new();
        let mut bundle_claimants = BTreeMap::new();
        for (kind, subject) in &touched {
            if let Some(group) = collision_group(*kind) {
                if refreshed.insert((group, subject.clone())) {
                    let (collided, collides, claimants) = refresh_collision(conn, group, subject)?;
                    if group == ASSET_GROUP && collided != collides {
                        asset_dependents_pending(conn, subject, &mut bundles, &mut pending.paths)?;
                    }
                    if group == BUNDLE_GROUP {
                        bundle_claimants.insert(BundleUuid(uuid16(subject)?), claimants);
                    }
                }
            }
            match *kind {
                BUNDLE => {
                    bundles.insert(BundleUuid(uuid16(subject)?));
                }
                PRIMARY_PATH => {
                    pending.paths.insert(path_subject(subject.clone())?);
                }
                _ => {}
            }
        }
        // A bundle only an asset's collision made pending has not had its
        // claimants read.
        for bundle in bundles {
            let claimants = match bundle_claimants.remove(&bundle) {
                Some(claimants) => claimants,
                None => group_claimants(conn, BUNDLE_GROUP, bundle.0.as_slice())?,
            };
            let sources = claimants
                .iter()
                .map(|bytes| decode_bundle_source(bytes))
                .collect::<Result<Vec<_>, _>>()?;
            pending.bundles.insert(bundle, sources);
        }
        Ok(pending)
    }


    /// Replace every source's claims with `sources`, writing only the rows
    /// that change: a full rescan republishes every source, and most keep
    /// their claims. When any changed, every collision is recomputed.
    /// Nothing is left pending.
    fn replace_all_source_claims(&mut self, sources: &[SourceClaims]) -> Result<(), StoreError> {
        type Key = (i64, String, i64, Vec<u8>, Vec<u8>);
        let mut wanted = std::collections::BTreeMap::<Key, Vec<u8>>::new();
        for source in sources {
            let root = self.intern_root(&source.root_name)?;
            for claim in &source.claims {
                let (kind, subject, claimant, detail) = claim_row(claim)?;
                wanted.insert((root.0, source.path.clone(), kind, subject, claimant), detail);
            }
        }
        let conn = &*self.txn;
        let mut stale = Vec::new();
        {
            let mut select = conn.prepare_cached(
                "SELECT root_id, path, kind, subject, claimant, detail FROM source_claims",
            )?;
            let mut rows = select.query([])?;
            while let Some(row) = rows.next()? {
                let key: Key = (row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?);
                let detail: Vec<u8> = row.get(5)?;
                match wanted.remove(&key) {
                    Some(same) if same == detail => {}
                    Some(changed) => {
                        wanted.insert(key, changed);
                    }
                    None => stale.push(key),
                }
            }
        }
        for (root, path, kind, subject, claimant) in &stale {
            conn.prepare_cached(
                "DELETE FROM source_claims
                 WHERE root_id = ?1 AND path = ?2 AND kind = ?3 AND subject = ?4 AND claimant = ?5",
            )?
            .execute(rusqlite::params![root, path, kind, subject, claimant])?;
        }
        for ((root, path, kind, subject, claimant), detail) in &wanted {
            conn.prepare_cached(
                "INSERT OR REPLACE INTO source_claims(root_id, path, kind, subject, claimant, detail)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?
            .execute(rusqlite::params![root, path, kind, subject, claimant, detail])?;
        }
        if stale.is_empty() && wanted.is_empty() {
            return Ok(());
        }
        // Every collision, against the rows recording them.
        let colliding = {
            let mut select = conn.prepare_cached(
                "SELECT CASE kind WHEN 0 THEN 0 ELSE 1 END AS g, subject FROM source_claims
                 WHERE kind IN (0, 1, 2)
                 GROUP BY g, subject HAVING COUNT(DISTINCT claimant) > 1",
            )?;
            let rows = select.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let mut recorded = {
            let mut select = conn.prepare_cached(
                "SELECT identity FROM errors WHERE family = ?1 AND scope_kind IN (2, 3)",
            )?;
            let rows = select.query_map([NAMESPACE], |row| row.get::<_, Vec<u8>>(0))?;
            rows.collect::<Result<BTreeSet<_>, _>>()?
        };
        for (group, subject) in colliding {
            let error = collision_error(group, &subject, &group_claimants(conn, group, &subject)?)?;
            if !recorded.remove(error.identity.as_slice()) {
                write_collision(conn, &error)?;
            }
        }
        for identity in recorded {
            delete_collision(conn, &identity)?;
        }
        Ok(())
    }
}

fn invalid_namespace_error(error: NamespaceErrorDecodeError) -> StoreError {
    StoreError::InvalidNamespaceError(error)
}

fn uuid16(bytes: &[u8]) -> Result<[u8; 16], StoreError> {
    bytes
        .try_into()
        .map_err(|_| invalid_namespace_error(NamespaceErrorDecodeError::Truncated))
}

fn decode_bundle_source(bytes: &[u8]) -> Result<ReadableBundleSource, StoreError> {
    let mut decoder = NamespaceErrorDecoder::new(bytes);
    let source = decoder.source().map_err(invalid_namespace_error)?;
    decoder.finish().map_err(invalid_namespace_error)?;
    Ok(source)
}

fn decode_asset_claimant(bytes: &[u8]) -> Result<AssetClaimant, StoreError> {
    let mut decoder = NamespaceErrorDecoder::new(bytes);
    let claimant = decoder.claimant().map_err(invalid_namespace_error)?;
    decoder.finish().map_err(invalid_namespace_error)?;
    Ok(claimant)
}

impl InputTxn<'_> {
    /// Publish the namespace errors of the claims as the scan's family, and
    /// return them in canonical order: every malformed skeleton and every
    /// bundle or asset UUID with more than one claimant. Each collision's
    /// row is the claims' own (see `refresh_collision`), so one read of the
    /// family serves both as those errors and as the rows the write
    /// compares with.
    pub fn publish_claims_namespace_errors(&mut self) -> Result<Vec<NamespaceError>, StoreError> {
        let decode = |bytes: &[u8]| {
            NamespaceError::from_persisted_bytes(bytes).map_err(invalid_namespace_error)
        };
        let mut errors = self
            .txn
            .prepare_cached("SELECT claimant FROM source_claims WHERE kind = 5")?
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .map(|bytes| decode(&bytes?))
            .collect::<Result<Vec<_>, _>>()?;
        let collision = [group_scope(BUNDLE_GROUP), group_scope(ASSET_GROUP)];
        let mut held = BTreeMap::new();
        let mut rows = self
            .txn
            .prepare_cached("SELECT identity, record, scope_kind FROM errors WHERE family = ?1")?;
        let mut rows = rows.query([NAMESPACE])?;
        while let Some(row) = rows.next()? {
            let (identity, record) = (row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?);
            if collision.contains(&row.get::<_, i64>(2)?) {
                errors.push(decode(&record)?);
            }
            held.insert(identity, record);
        }
        drop(rows);
        self.write_namespace_family(NAMESPACE, errors, held)
    }
}

impl StoreReader {
    /// The (root, path) of every source whose claims do not publish as they
    /// stand: a malformed source (no complete skeleton) and every source
    /// claiming a colliding bundle or asset. Searches of
    /// `source_claims_by_subject`, so it costs the defects, not the
    /// namespace.
    pub fn unpublished_claim_sources(&self) -> Result<BTreeSet<(String, String)>, StoreError> {
        Ok(self
            .query_rows(
                "SELECT r.name, t.path FROM source_claims t JOIN roots r USING (root_id)
                 WHERE t.kind = 5
                 UNION SELECT r.name, t.path FROM errors c
                 CROSS JOIN source_claims t ON t.kind IN (0, 1, 2) AND t.subject = c.scope_id
                 JOIN roots r ON r.root_id = t.root_id
                 WHERE c.family = ?1 AND c.scope_kind IN (2, 3)",
                [NAMESPACE],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .into_iter()
            .collect())
    }

    /// The distinct derived outputs claiming `child`.
    pub fn derived_output_claims(
        &self,
        child: AssetUuid,
    ) -> Result<Vec<DerivedOutputClaim>, StoreError> {
        let rows = self.query_rows(
            "SELECT DISTINCT claimant, detail FROM source_claims WHERE kind = 2 AND subject = ?1",
            [child.0.as_slice()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?;
        let mut outputs = BTreeSet::new();
        for (claimant, detail) in rows {
            let AssetClaimant::Derived { parent, output_key } = decode_asset_claimant(&claimant)?
            else {
                return Err(invalid_namespace_error(NamespaceErrorDecodeError::InvalidClaimant));
            };
            outputs.insert(DerivedOutputClaim {
                parent,
                output_key,
                terminal_type: TypeUuid(uuid16(&detail)?),
            });
        }
        Ok(outputs.into_iter().collect())
    }

    /// The assets claiming `path` as their bundle's primary.
    pub fn path_claims(&self, path: &str) -> Result<BTreeSet<AssetUuid>, StoreError> {
        self.query_rows(
            "SELECT DISTINCT claimant FROM source_claims WHERE kind = 3 AND subject = ?1",
            [path.as_bytes()],
            |row| row.get::<_, Vec<u8>>(0),
        )?
        .iter()
        .map(|bytes| Ok(AssetUuid(uuid16(bytes)?)))
        .collect()
    }

}
