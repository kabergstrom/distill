use std::collections::BTreeSet;
use std::fmt;

use distill_core::attestation::{
    AttestationError, BootstrapAuthorityMismatch, CompiledAttestationDigest, CompiledTypeRow,
    CompiledTypeTable,
};

use crate::{LoadPolicyEntry, TargetDefinitionHash, TypeUuid};

const DSLP: [u8; 4] = *b"DSLP";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestationShapeError {
    Compiled(AttestationError),
    PolicyRowsNotStrictlySorted {
        previous: TypeUuid,
        current: TypeUuid,
    },
    PolicyDigestMismatch {
        expected: [u8; 32],
        got: [u8; 32],
    },
    RegisteredTypeSetMismatch {
        only_compiled: Vec<TypeUuid>,
        only_policy: Vec<TypeUuid>,
    },
    DuplicateTarget {
        target: String,
    },
    Bootstrap(BootstrapAuthorityMismatch),
    BootstrapAuthorityUnavailable(String),
}

impl From<AttestationError> for AttestationShapeError {
    fn from(value: AttestationError) -> Self {
        Self::Compiled(value)
    }
}

impl fmt::Display for AttestationShapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for AttestationShapeError {}

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
    compiled_rows: &[CompiledTypeRow],
    policy_rows: &[LoadPolicyEntry],
) -> Result<(), AttestationShapeError> {
    let compiled: BTreeSet<_> = compiled_rows.iter().map(|row| row.type_uuid).collect();
    let policies: BTreeSet<_> = policy_rows.iter().map(|row| row.type_uuid).collect();
    if compiled == policies {
        return Ok(());
    }
    Err(AttestationShapeError::RegisteredTypeSetMismatch {
        only_compiled: compiled.difference(&policies).copied().collect(),
        only_policy: policies.difference(&compiled).copied().collect(),
    })
}

/// `blake3("DSLP" || version:u8 || count:u32 LE || rows)` remains an
/// independently basis-bound policy digest even though DSCA repeats the bit.
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
    compiled_rows: &[CompiledTypeRow],
    dsca: CompiledAttestationDigest,
    policy_rows: &[LoadPolicyEntry],
    policy_digest: [u8; 32],
) -> Result<(), AttestationShapeError> {
    ensure_same_type_set(compiled_rows, policy_rows)?;
    CompiledTypeTable::from_canonical(compiled_rows.to_vec(), dsca)?;
    let expected_policy = compute_policy_digest(policy_rows)?;
    if expected_policy != policy_digest {
        return Err(AttestationShapeError::PolicyDigestMismatch {
            expected: expected_policy,
            got: policy_digest,
        });
    }
    for (compiled, policy) in compiled_rows.iter().zip(policy_rows) {
        if compiled.type_uuid != policy.type_uuid || compiled.build_only != policy.build_only {
            return Err(AttestationShapeError::RegisteredTypeSetMismatch {
                only_compiled: vec![compiled.type_uuid],
                only_policy: vec![policy.type_uuid],
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetDefinition {
    name: String,
    definition_hash: TargetDefinitionHash,
    compiled_registry: Vec<CompiledTypeRow>,
    dsca: CompiledAttestationDigest,
    load_policy: Vec<LoadPolicyEntry>,
    policy_digest: [u8; 32],
}

impl TargetDefinition {
    pub fn canonical(
        name: impl Into<String>,
        definition_hash: TargetDefinitionHash,
        compiled_registry: Vec<CompiledTypeRow>,
        mut load_policy: Vec<LoadPolicyEntry>,
    ) -> Result<Self, AttestationShapeError> {
        let compiled = CompiledTypeTable::canonical(compiled_registry)?;
        load_policy.sort_by_key(|row| row.type_uuid);
        ensure_same_type_set(&compiled.rows, &load_policy)?;
        let policy_digest = compute_policy_digest(&load_policy)?;
        validate_attestation_shape(&compiled.rows, compiled.digest, &load_policy, policy_digest)?;
        Ok(Self {
            name: name.into(),
            definition_hash,
            compiled_registry: compiled.rows,
            dsca: compiled.digest,
            load_policy,
            policy_digest,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn definition_hash(&self) -> TargetDefinitionHash {
        self.definition_hash
    }

    pub fn compiled_registry(&self) -> &[CompiledTypeRow] {
        &self.compiled_registry
    }

    pub fn dsca(&self) -> CompiledAttestationDigest {
        self.dsca
    }

    pub fn load_policy(&self) -> &[LoadPolicyEntry] {
        &self.load_policy
    }

    pub fn policy_digest(&self) -> [u8; 32] {
        self.policy_digest
    }
}
