use std::collections::BTreeSet;
use std::fmt;

use crate::{LayoutEntry, LoadPolicyEntry, TargetDefinitionHash, TypeUuid};

const DSLA: [u8; 4] = *b"DSLA";
const DSLP: [u8; 4] = *b"DSLP";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestationShapeError {
    LayoutRowsNotStrictlySorted {
        previous: TypeUuid,
        current: TypeUuid,
    },
    PolicyRowsNotStrictlySorted {
        previous: TypeUuid,
        current: TypeUuid,
    },
    LayoutAggregateMismatch {
        expected: [u8; 32],
        got: [u8; 32],
    },
    PolicyDigestMismatch {
        expected: [u8; 32],
        got: [u8; 32],
    },
    RegisteredTypeSetMismatch {
        only_layout: Vec<TypeUuid>,
        only_policy: Vec<TypeUuid>,
    },
    DuplicateTarget {
        target: String,
    },
}

impl fmt::Display for AttestationShapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for AttestationShapeError {}

fn ensure_layout_order(rows: &[LayoutEntry]) -> Result<(), AttestationShapeError> {
    for pair in rows.windows(2) {
        if pair[0].type_uuid >= pair[1].type_uuid {
            return Err(AttestationShapeError::LayoutRowsNotStrictlySorted {
                previous: pair[0].type_uuid,
                current: pair[1].type_uuid,
            });
        }
    }
    Ok(())
}

fn ensure_policy_order(rows: &[LoadPolicyEntry]) -> Result<(), AttestationShapeError> {
    for pair in rows.windows(2) {
        if pair[0].type_uuid >= pair[1].type_uuid {
            return Err(AttestationShapeError::PolicyRowsNotStrictlySorted {
                previous: pair[0].type_uuid,
                current: pair[1].type_uuid,
            });
        }
    }
    Ok(())
}

fn ensure_same_type_set(
    layout_rows: &[LayoutEntry],
    policy_rows: &[LoadPolicyEntry],
) -> Result<(), AttestationShapeError> {
    let layouts: BTreeSet<_> = layout_rows.iter().map(|row| row.type_uuid).collect();
    let policies: BTreeSet<_> = policy_rows.iter().map(|row| row.type_uuid).collect();
    if layouts == policies {
        return Ok(());
    }
    Err(AttestationShapeError::RegisteredTypeSetMismatch {
        only_layout: layouts.difference(&policies).copied().collect(),
        only_policy: policies.difference(&layouts).copied().collect(),
    })
}

/// `blake3("DSLA" || version:u8 || count:u32 LE || rows)`.
pub fn compute_layout_aggregate(rows: &[LayoutEntry]) -> Result<[u8; 32], AttestationShapeError> {
    ensure_layout_order(rows)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&DSLA);
    hasher.update(&[1]);
    hasher.update(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        hasher.update(&row.type_uuid.0);
        hasher.update(&row.layout_digest);
    }
    Ok(*hasher.finalize().as_bytes())
}

/// `blake3("DSLP" || version:u8 || count:u32 LE || rows)`.
pub fn compute_policy_digest(rows: &[LoadPolicyEntry]) -> Result<[u8; 32], AttestationShapeError> {
    ensure_policy_order(rows)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&DSLP);
    hasher.update(&[1]);
    hasher.update(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        hasher.update(&row.type_uuid.0);
        hasher.update(&[u8::from(row.build_only)]);
    }
    Ok(*hasher.finalize().as_bytes())
}

pub(crate) fn validate_attestation_shape(
    layout_rows: &[LayoutEntry],
    layout_aggregate: [u8; 32],
    policy_rows: &[LoadPolicyEntry],
    policy_digest: [u8; 32],
) -> Result<(), AttestationShapeError> {
    ensure_same_type_set(layout_rows, policy_rows)?;
    let expected_layout = compute_layout_aggregate(layout_rows)?;
    if expected_layout != layout_aggregate {
        return Err(AttestationShapeError::LayoutAggregateMismatch {
            expected: expected_layout,
            got: layout_aggregate,
        });
    }
    let expected_policy = compute_policy_digest(policy_rows)?;
    if expected_policy != policy_digest {
        return Err(AttestationShapeError::PolicyDigestMismatch {
            expected: expected_policy,
            got: policy_digest,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetDefinition {
    pub name: String,
    pub definition_hash: TargetDefinitionHash,
    pub layout_registry: Vec<LayoutEntry>,
    pub layout_aggregate: [u8; 32],
    pub load_policy: Vec<LoadPolicyEntry>,
    pub policy_digest: [u8; 32],
}

impl TargetDefinition {
    pub fn canonical(
        name: impl Into<String>,
        definition_hash: TargetDefinitionHash,
        mut layout_registry: Vec<LayoutEntry>,
        mut load_policy: Vec<LoadPolicyEntry>,
    ) -> Result<Self, AttestationShapeError> {
        layout_registry.sort_by_key(|row| row.type_uuid);
        load_policy.sort_by_key(|row| row.type_uuid);
        ensure_same_type_set(&layout_registry, &load_policy)?;
        let layout_aggregate = compute_layout_aggregate(&layout_registry)?;
        let policy_digest = compute_policy_digest(&load_policy)?;
        Ok(Self {
            name: name.into(),
            definition_hash,
            layout_registry,
            layout_aggregate,
            load_policy,
            policy_digest,
        })
    }
}
