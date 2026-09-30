//! Scan claims: what each scanned `.bundle` source claims — its bundle
//! UUID, its authored asset UUIDs, the derived outputs its assets project,
//! its primary path, a lineage manifest, or (for an unreadable skeleton) a
//! version poison. Rows are keyed by the claiming (root, path), so a scan
//! replaces exactly the claims of the subtree it observed.
//!
//! Two tables follow from the claims in the same transaction:
//! `claim_collisions` holds every bundle or asset subject with more than
//! one distinct claimant, and `claim_pending` every bundle, derived-output
//! and path subject changed since the last clean publication.

use std::collections::BTreeSet;

use distill_core::canonical::CanonicalEncoder;
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, TypeUuid};

use crate::db::{InputTxn, Store, StoreReader};
use crate::error::StoreError;
use crate::state::{
    encode_asset_claimant, encode_bundle_source, encode_lineage_manifest_claimant, AssetClaimant,
    LineageManifestClaimant, ReadableBundleSource, VersionPoison, VersionPoisonDecoder,
    VersionPoisonError, VersionPoisonV1,
};

const BUNDLE: i64 = 0;
const AUTHORED: i64 = 1;
const DERIVED: i64 = 2;
const PRIMARY_PATH: i64 = 3;
const LINEAGE: i64 = 4;
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
    /// A SchemaLineageManifest entry.
    Lineage(LineageManifestClaimant),
    /// The source's skeleton is unreadable.
    Malformed(VersionPoison),
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

/// Subjects changed since the last clean publication.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingClaims {
    pub bundles: BTreeSet<BundleUuid>,
    pub derived: BTreeSet<AssetUuid>,
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
        SourceClaim::Lineage(claimant) => (
            LINEAGE,
            Vec::new(),
            encoded(&|encoder| encode_lineage_manifest_claimant(encoder, claimant)),
            Vec::new(),
        ),
        SourceClaim::Malformed(poison) => (
            MALFORMED,
            Vec::new(),
            poison
                .persisted_bytes()
                .map_err(StoreError::InvalidVersionPoison)?,
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

fn clear_claims(conn: &rusqlite::Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "DELETE FROM source_claims; DELETE FROM claim_collisions; DELETE FROM claim_pending;",
    )?;
    Ok(())
}

impl InputTxn<'_> {
    /// Replace the claims of every source under `under` (every source when
    /// `None`) with `sources`. A subtree replacement refreshes the
    /// collisions of the subjects it touched and marks them pending; a full
    /// replacement recomputes every collision and leaves nothing pending.
    pub fn replace_source_claims(
        &mut self,
        under: Option<&[(String, String)]>,
        sources: &[SourceClaims],
    ) -> Result<(), StoreError> {
        let conn = &*self.txn;
        let mut touched = BTreeSet::<(i64, Vec<u8>)>::new();
        match under {
            None => clear_claims(conn)?,
            Some(prefixes) => {
                for (root, prefix) in prefixes {
                    let mut select = conn.prepare_cached(&format!(
                        "SELECT t.kind, t.subject FROM source_claims t JOIN roots r USING (root_id)
                         WHERE {}",
                        crate::files::UNDER
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
                            crate::files::UNDER
                        ),
                        rusqlite::params![root, prefix],
                    )?;
                }
            }
        }
        for source in sources {
            let root = self.intern_root(&source.root_name)?;
            for claim in &source.claims {
                let (kind, subject, claimant, detail) = claim_row(claim)?;
                self.txn.execute(
                    "INSERT OR REPLACE INTO source_claims(root_id, path, kind, subject, claimant, detail)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![root.0, source.path, kind, subject, claimant, detail],
                )?;
                touched.insert((kind, subject));
            }
        }
        let conn = &*self.txn;
        if under.is_none() {
            conn.execute(
                "INSERT INTO claim_collisions(grp, subject)
                 SELECT CASE kind WHEN 0 THEN 0 ELSE 1 END AS g, subject FROM source_claims
                 WHERE kind IN (0, 1, 2)
                 GROUP BY g, subject HAVING COUNT(DISTINCT claimant) > 1",
                [],
            )?;
            return Ok(());
        }
        let mut refreshed = BTreeSet::new();
        for (kind, subject) in &touched {
            if let Some(group) = collision_group(*kind) {
                if refreshed.insert((group, subject.clone())) {
                    conn.execute(
                        "DELETE FROM claim_collisions WHERE grp = ?1 AND subject = ?2",
                        rusqlite::params![group, subject],
                    )?;
                    let claimants: i64 = conn.query_row(
                        &format!(
                            "SELECT COUNT(DISTINCT claimant) FROM source_claims
                             WHERE kind IN {} AND subject = ?1",
                            group_kinds(group)
                        ),
                        [subject],
                        |row| row.get(0),
                    )?;
                    if claimants > 1 {
                        conn.execute(
                            "INSERT INTO claim_collisions(grp, subject) VALUES (?1, ?2)",
                            rusqlite::params![group, subject],
                        )?;
                    }
                }
            }
            if matches!(*kind, BUNDLE | DERIVED | PRIMARY_PATH) {
                conn.execute(
                    "INSERT OR IGNORE INTO claim_pending(kind, subject) VALUES (?1, ?2)",
                    rusqlite::params![kind, subject],
                )?;
            }
        }
        Ok(())
    }

    /// The pending subjects were published.
    pub fn clear_pending_claims(&mut self) -> Result<(), StoreError> {
        self.txn.execute("DELETE FROM claim_pending", [])?;
        Ok(())
    }
}

impl Store {
    /// Drop every claim, collision and pending subject without publishing
    /// an input version: claims are derived from the scan and the pipeline
    /// projection, and the next full publication rewrites them.
    pub fn clear_source_claims(&mut self) -> Result<(), StoreError> {
        let transaction = self.read.conn.savepoint()?;
        clear_claims(&transaction)?;
        transaction.commit()?;
        Ok(())
    }
}

fn poison_error(error: VersionPoisonError) -> StoreError {
    StoreError::InvalidVersionPoison(error)
}

fn uuid16(bytes: &[u8]) -> Result<[u8; 16], StoreError> {
    bytes
        .try_into()
        .map_err(|_| poison_error(VersionPoisonError::Truncated))
}

fn decode_bundle_source(bytes: &[u8]) -> Result<ReadableBundleSource, StoreError> {
    let mut decoder = VersionPoisonDecoder::new(bytes);
    let source = decoder.source().map_err(poison_error)?;
    decoder.finish().map_err(poison_error)?;
    Ok(source)
}

fn decode_asset_claimant(bytes: &[u8]) -> Result<AssetClaimant, StoreError> {
    let mut decoder = VersionPoisonDecoder::new(bytes);
    let claimant = decoder.claimant().map_err(poison_error)?;
    decoder.finish().map_err(poison_error)?;
    Ok(claimant)
}

fn decode_lineage_claimant(bytes: &[u8]) -> Result<LineageManifestClaimant, StoreError> {
    let mut decoder = VersionPoisonDecoder::new(bytes);
    let claimant = (|| {
        Ok(LineageManifestClaimant {
            root_name: decoder.string()?,
            normalized_path: decoder.string()?,
            bundle: BundleUuid(decoder.array()?),
            local_id: decoder.string()?,
            asset: AssetUuid(decoder.array()?),
            file_hash: BundleFileHash(decoder.array()?),
        })
    })()
    .map_err(poison_error)?;
    decoder.finish().map_err(poison_error)?;
    Ok(claimant)
}

impl StoreReader {
    /// The canonical version poison of the claims: a malformed skeleton or
    /// a bundle or asset UUID with more than one claimant.
    pub fn claims_version_poison(&self) -> Result<Option<VersionPoison>, StoreError> {
        let mut poisons = self
            .query_rows(
                "SELECT claimant FROM source_claims WHERE kind = 5",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )?
            .iter()
            .map(|bytes| VersionPoison::from_persisted_bytes(bytes).map_err(poison_error))
            .collect::<Result<Vec<_>, _>>()?;
        let collisions = self.query_rows(
            "SELECT grp, subject FROM claim_collisions ORDER BY grp, subject",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?;
        for (group, subject) in collisions {
            let claimants = self.query_rows(
                &format!(
                    "SELECT DISTINCT claimant FROM source_claims
                     WHERE kind IN {} AND subject = ?1 ORDER BY claimant",
                    group_kinds(group)
                ),
                [&subject],
                |row| row.get::<_, Vec<u8>>(0),
            )?;
            let poison = if group == BUNDLE_GROUP {
                let bundle = BundleUuid(uuid16(&subject)?);
                let mut sources = claimants
                    .iter()
                    .map(|bytes| decode_bundle_source(bytes))
                    .collect::<Result<Vec<_>, _>>()?;
                sources.sort();
                VersionPoison::new(
                    VersionPoisonV1::DuplicateBundleUuid { bundle, sources },
                    format!("duplicate bundle UUID {bundle}"),
                )
            } else {
                let asset = AssetUuid(uuid16(&subject)?);
                let mut claimants = claimants
                    .iter()
                    .map(|bytes| decode_asset_claimant(bytes))
                    .collect::<Result<Vec<_>, _>>()?;
                claimants.sort();
                VersionPoison::new(
                    VersionPoisonV1::DuplicateAssetUuid { asset, claimants },
                    format!("duplicate asset UUID {asset}"),
                )
            };
            poisons.push(poison.map_err(poison_error)?);
        }
        VersionPoison::select_canonical(poisons).map_err(poison_error)
    }

    /// The distinct sources claiming `bundle`.
    pub fn bundle_claim_sources(
        &self,
        bundle: BundleUuid,
    ) -> Result<Vec<ReadableBundleSource>, StoreError> {
        self.query_rows(
            "SELECT DISTINCT claimant FROM source_claims WHERE kind = 0 AND subject = ?1
             ORDER BY claimant",
            [bundle.0.as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )?
        .iter()
        .map(|bytes| decode_bundle_source(bytes))
        .collect()
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
                return Err(poison_error(VersionPoisonError::InvalidClaimant));
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

    /// The distinct lineage manifest claimants.
    pub fn lineage_claims(&self) -> Result<BTreeSet<LineageManifestClaimant>, StoreError> {
        self.query_rows(
            "SELECT DISTINCT claimant FROM source_claims WHERE kind = 4",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )?
        .iter()
        .map(|claimant| decode_lineage_claimant(claimant))
        .collect()
    }

    pub fn pending_claims(&self) -> Result<PendingClaims, StoreError> {
        let rows = self.query_rows(
            "SELECT kind, subject FROM claim_pending",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?;
        let mut pending = PendingClaims::default();
        for (kind, subject) in rows {
            match kind {
                BUNDLE => {
                    pending.bundles.insert(BundleUuid(uuid16(&subject)?));
                }
                DERIVED => {
                    pending.derived.insert(AssetUuid(uuid16(&subject)?));
                }
                PRIMARY_PATH => {
                    pending.paths.insert(String::from_utf8(subject).map_err(|_| {
                        poison_error(VersionPoisonError::InvalidUtf8)
                    })?);
                }
                _ => {}
            }
        }
        Ok(pending)
    }
}
