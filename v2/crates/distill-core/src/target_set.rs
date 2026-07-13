//! Canonical candidate target-set identity (§5, §13, §18).

use std::fmt;

use unicode_normalization::UnicodeNormalization;

use crate::canonical::{domain_digest, DSTS};

/// One target-definition identity included in a staged pipeline candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSetRow {
    pub name: String,
    pub target_definition_hash: [u8; 32],
}

/// The recomputed DSTS commitment to the candidate's complete target set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetSetHash(pub [u8; 32]);

/// A canonical, NFC-normalized, name-sorted target set and its DSTS digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalTargetSet {
    pub rows: Vec<TargetSetRow>,
    pub digest: TargetSetHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetSetError {
    DuplicateTarget {
        normalized_name: String,
    },
    RowsNotCanonical,
    DigestMismatch {
        expected: TargetSetHash,
        got: TargetSetHash,
    },
    TooManyTargets,
}

impl fmt::Display for TargetSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TargetSetError {}

impl CanonicalTargetSet {
    /// Normalize names to NFC, sort by normalized UTF-8 bytes, reject
    /// duplicates, and recompute `blake3("DSTS" || 1 || rows)`.
    pub fn canonical(mut rows: Vec<TargetSetRow>) -> Result<Self, TargetSetError> {
        if rows.len() > u32::MAX as usize {
            return Err(TargetSetError::TooManyTargets);
        }
        for row in &mut rows {
            row.name = row.name.nfc().collect();
        }
        rows.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
        if let Some(pair) = rows
            .windows(2)
            .find(|pair| pair[0].name.as_bytes() == pair[1].name.as_bytes())
        {
            return Err(TargetSetError::DuplicateTarget {
                normalized_name: pair[0].name.clone(),
            });
        }
        let digest = compute_target_set_hash(&rows)?;
        Ok(Self { rows, digest })
    }

    /// Verify a caller-provided canonical row table and digest without
    /// accepting sorting or normalization as proof of canonicality.
    pub fn from_canonical(
        rows: Vec<TargetSetRow>,
        digest: TargetSetHash,
    ) -> Result<Self, TargetSetError> {
        let canonical = Self::canonical(rows.clone())?;
        if canonical.rows != rows {
            return Err(TargetSetError::RowsNotCanonical);
        }
        if canonical.digest != digest {
            return Err(TargetSetError::DigestMismatch {
                expected: canonical.digest,
                got: digest,
            });
        }
        Ok(Self { rows, digest })
    }
}

pub fn compute_target_set_hash(rows: &[TargetSetRow]) -> Result<TargetSetHash, TargetSetError> {
    let count = u32::try_from(rows.len()).map_err(|_| TargetSetError::TooManyTargets)?;
    Ok(TargetSetHash(domain_digest(DSTS, 1, |encoder| {
        encoder.u32(count);
        for row in rows {
            encoder.str(&row.name);
            encoder.raw(&row.target_definition_hash);
        }
    })))
}
