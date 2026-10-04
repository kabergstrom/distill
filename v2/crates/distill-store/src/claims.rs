//! Scan claims: what each scanned `.bundle` source claims — its bundle
//! UUID, its authored asset UUIDs, the derived outputs its assets project,
//! its primary path, or (for an unreadable skeleton) a
//! namespace error. Rows are keyed by the claiming (root, path), so a scan
//! replaces exactly the claims of the subtree it observed.
//!
//! Two things follow from the claims. A bundle or asset subject with more
//! than one distinct claimant collides: its namespace error is read from
//! the claims, never stored. And a subtree replacement returns the subjects
//! it changed, for the publication that follows it in that transaction.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::canonical::CanonicalEncoder;
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};

use crate::db::{InputTxn, StoreReader};
use crate::error::StoreError;
use crate::state::{
    encode_asset_claimant, encode_bundle_source, AssetClaimant, NamespaceError,
    NamespaceErrorDecodeError, NamespaceErrorDecoder, NamespaceErrorV1, ReadableBundleSource,
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
    /// Each asset UUID the replacement touched, and whether more than one
    /// claimant claims it now.
    pub assets: BTreeMap<AssetUuid, bool>,
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

/// Every colliding subject and its group (0 bundle, 1 asset): each bundle
/// UUID, and each asset UUID, with more than one distinct claimant. A
/// GROUP BY over the bundle and asset claims, so it costs the claims: it
/// serves full publication and diagnostics, never an edit.
pub(crate) const COLLIDING: &str =
    "SELECT CASE kind WHEN 0 THEN 0 ELSE 1 END AS g, subject FROM source_claims
     WHERE kind IN (0, 1, 2)
     GROUP BY g, subject HAVING COUNT(DISTINCT claimant) > 1";

/// The message of a bundle UUID's collision.
pub fn bundle_collision_message(bundle: BundleUuid) -> String {
    format!("duplicate bundle UUID {bundle}")
}

/// The message of an asset UUID's collision.
pub fn asset_collision_message(asset: AssetUuid) -> String {
    format!("duplicate asset UUID {asset}")
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

/// Whether more than one distinct claimant claims asset UUID `subject`.
fn asset_collides(conn: &rusqlite::Connection, subject: &[u8]) -> Result<bool, StoreError> {
    Ok(conn
        .prepare_cached(ASSET_COLLIDES)?
        .query_row([subject], |row| row.get(0))?)
}

/// Whether asset UUID `?1` has more than one distinct claimant.
pub(crate) const ASSET_COLLIDES: &str =
    "SELECT COUNT(DISTINCT claimant) > 1 FROM source_claims WHERE kind IN (1, 2) AND subject = ?1";

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
            bundle_collision_message(bundle),
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
            asset_collision_message(asset),
        )
    };
    error.map_err(invalid_namespace_error)
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
    let mut naming =
        conn.prepare_cached("SELECT subject FROM source_claims WHERE kind = ?1 AND claimant = ?2")?;
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
    /// `None`) with `sources`. A subtree replacement returns the subjects it
    /// touched pending; a full replacement returns nothing pending.
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
        }
        let rows = sources
            .iter()
            .map(|source| {
                let rows = source
                    .claims
                    .iter()
                    .map(claim_row)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((source, rows))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        for (_, rows) in &rows {
            for (kind, subject, _, _) in rows {
                touched.insert((*kind, subject.clone()));
            }
        }
        // Whether each touched asset UUID collided before: one that starts
        // or stops colliding changes what publishes it.
        let mut collided = BTreeMap::new();
        for (kind, subject) in &touched {
            if collision_group(*kind) == Some(ASSET_GROUP) && !collided.contains_key(subject) {
                collided.insert(subject.clone(), asset_collides(conn, subject)?);
            }
        }
        for (root, prefix) in prefixes {
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
        for (source, rows) in &rows {
            let root = self.intern_root(&source.root_name)?;
            for (kind, subject, claimant, detail) in rows {
                self.txn
                    .prepare_cached(
                        "INSERT OR REPLACE INTO source_claims(root_id, path, kind, subject, claimant, detail)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    )?
                    .execute(rusqlite::params![root.0, source.path, kind, subject, claimant, detail])?;
            }
        }
        let conn = &*self.txn;
        let mut pending = PendingClaims::default();
        let mut bundles = BTreeSet::new();
        let mut bundle_claimants = BTreeMap::new();
        for (kind, subject) in &touched {
            match collision_group(*kind) {
                Some(BUNDLE_GROUP) => {
                    bundle_claimants.insert(
                        BundleUuid(uuid16(subject)?),
                        group_claimants(conn, BUNDLE_GROUP, subject)?,
                    );
                }
                Some(_) => {
                    let asset = AssetUuid(uuid16(subject)?);
                    if !pending.assets.contains_key(&asset) {
                        let collides = asset_collides(conn, subject)?;
                        if collides != collided[subject] {
                            asset_dependents_pending(
                                conn,
                                subject,
                                &mut bundles,
                                &mut pending.paths,
                            )?;
                        }
                        pending.assets.insert(asset, collides);
                    }
                }
                None => {}
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
    /// their claims. Nothing is left pending.
    fn replace_all_source_claims(&mut self, sources: &[SourceClaims]) -> Result<(), StoreError> {
        type Key = (i64, String, i64, Vec<u8>, Vec<u8>);
        let mut wanted = std::collections::BTreeMap::<Key, Vec<u8>>::new();
        for source in sources {
            let root = self.intern_root(&source.root_name)?;
            for claim in &source.claims {
                let (kind, subject, claimant, detail) = claim_row(claim)?;
                wanted.insert(
                    (root.0, source.path.clone(), kind, subject, claimant),
                    detail,
                );
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
                let key: Key = (
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                );
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

impl StoreReader {
    /// Every namespace error of the claims, in canonical order: each
    /// malformed skeleton (its claim holds the error) and each bundle or
    /// asset UUID with more than one claimant. Costs the claims (see
    /// [`COLLIDING`]).
    pub fn namespace_errors(&self) -> Result<Vec<NamespaceError>, StoreError> {
        let mut errors = self
            .query_rows(
                "SELECT claimant FROM source_claims WHERE kind = 5",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )?
            .iter()
            .map(|bytes| {
                NamespaceError::from_persisted_bytes(bytes).map_err(invalid_namespace_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let colliding = self.query_rows(COLLIDING, [], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        for (group, subject) in colliding {
            let claimants = group_claimants(&self.conn, group, &subject)?;
            errors.push(collision_error(group, &subject, &claimants)?);
        }
        NamespaceError::canonical_set(errors).map_err(invalid_namespace_error)
    }

    /// The (root, path) of every source whose claims do not publish as they
    /// stand: a malformed source (no complete skeleton) and every source
    /// claiming a colliding bundle or asset. Costs the claims (see
    /// [`COLLIDING`]): only a reconfiguration that reclaims sources asks.
    pub fn unpublished_claim_sources(&self) -> Result<BTreeSet<(String, String)>, StoreError> {
        Ok(self
            .query_rows(
                &format!(
                    "SELECT r.name, t.path FROM source_claims t JOIN roots r USING (root_id)
                     WHERE t.kind = 5
                     UNION SELECT r.name, t.path FROM ({COLLIDING}) c
                     CROSS JOIN source_claims t ON t.kind IN (0, 1, 2) AND t.subject = c.subject
                     JOIN roots r ON r.root_id = t.root_id"
                ),
                [],
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
                return Err(invalid_namespace_error(
                    NamespaceErrorDecodeError::InvalidClaimant,
                ));
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
