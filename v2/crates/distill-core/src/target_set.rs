//! Canonical candidate target rows (§5, §13, §18).

use std::fmt;

use unicode_normalization::UnicodeNormalization;

/// One target-definition identity included in a staged pipeline candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSetRow {
    pub name: String,
    pub target_definition_hash: [u8; 32],
}

/// A canonical, NFC-normalized, name-sorted target set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalTargetSet {
    pub rows: Vec<TargetSetRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetSetError {
    DuplicateTarget { normalized_name: String },
    RowsNotCanonical,
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
    /// duplicates, and retain the exact canonical rows.
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
        Ok(Self { rows })
    }

    /// Verify caller-provided rows without accepting sorting or normalization
    /// as proof of canonicality.
    pub fn from_canonical(rows: Vec<TargetSetRow>) -> Result<Self, TargetSetError> {
        let canonical = Self::canonical(rows.clone())?;
        if canonical.rows != rows {
            return Err(TargetSetError::RowsNotCanonical);
        }
        Ok(Self { rows })
    }
}
