//! IO-neutral sweep bases and the basis-bound load-policy attestation.

use std::sync::Arc;

use distill_asset::AssetRuntimeDescriptor;
use distill_core::id::TypeUuid;
use distill_store::state::SnapshotStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ManifestHash(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LoadPolicyRow {
    pub type_uuid: TypeUuid,
    pub build_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadPolicyAttestation {
    rows: Vec<LoadPolicyRow>,
    digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadPolicyError {
    Unsorted,
    Duplicate(TypeUuid),
    DigestMismatch,
    MissingType(TypeUuid),
    BitMismatch(TypeUuid),
    BuildOnly(TypeUuid),
}

impl LoadPolicyAttestation {
    pub fn from_rows(mut rows: Vec<LoadPolicyRow>) -> Result<Self, LoadPolicyError> {
        rows.sort_by_key(|row| row.type_uuid);
        reject_duplicates(&rows)?;
        let digest = digest_rows(&rows);
        Ok(Self { rows, digest })
    }

    /// Validate a carried attestation without normalizing it. Wire order is
    /// normative; accepting an unsorted equivalent would admit two encodings.
    pub fn try_from_parts(
        rows: Vec<LoadPolicyRow>,
        digest: [u8; 32],
    ) -> Result<Self, LoadPolicyError> {
        if rows
            .windows(2)
            .any(|pair| pair[0].type_uuid > pair[1].type_uuid)
        {
            return Err(LoadPolicyError::Unsorted);
        }
        reject_duplicates(&rows)?;
        if digest_rows(&rows) != digest {
            return Err(LoadPolicyError::DigestMismatch);
        }
        Ok(Self { rows, digest })
    }

    pub fn rows(&self) -> &[LoadPolicyRow] {
        &self.rows
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn row(&self, type_uuid: TypeUuid) -> Option<LoadPolicyRow> {
        self.rows
            .binary_search_by_key(&type_uuid, |row| row.type_uuid)
            .ok()
            .map(|index| self.rows[index])
    }

    pub fn require_runtime(&self, type_uuid: TypeUuid) -> Result<(), LoadPolicyError> {
        let row = self
            .row(type_uuid)
            .ok_or(LoadPolicyError::MissingType(type_uuid))?;
        if row.build_only {
            return Err(LoadPolicyError::BuildOnly(type_uuid));
        }
        Ok(())
    }

    /// Verify every carried projection row against the runtime registry.
    /// Extra runtime descriptors are ignored: pack coverage is the pack
    /// closure, never the consuming binary's whole workspace registry.
    pub fn verify_descriptors(
        &self,
        descriptors: &[&AssetRuntimeDescriptor],
    ) -> Result<(), LoadPolicyError> {
        for row in &self.rows {
            let descriptor = descriptors
                .iter()
                .find(|descriptor| descriptor.type_uuid == row.type_uuid)
                .ok_or(LoadPolicyError::MissingType(row.type_uuid))?;
            if row.build_only != descriptor.build_only {
                return Err(LoadPolicyError::BitMismatch(row.type_uuid));
            }
        }
        Ok(())
    }
}

fn reject_duplicates(rows: &[LoadPolicyRow]) -> Result<(), LoadPolicyError> {
    if let Some(pair) = rows
        .windows(2)
        .find(|pair| pair[0].type_uuid == pair[1].type_uuid)
    {
        return Err(LoadPolicyError::Duplicate(pair[0].type_uuid));
    }
    Ok(())
}

pub fn digest_rows(rows: &[LoadPolicyRow]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DSLP");
    hasher.update(&[1]);
    hasher.update(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        hasher.update(&row.type_uuid.0);
        hasher.update(&[u8::from(row.build_only)]);
    }
    *hasher.finalize().as_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoBasis {
    Rpc {
        snapshot: SnapshotStamp,
        load_policy: Arc<LoadPolicyAttestation>,
        policy_generation: u64,
    },
    Pack {
        manifest: ManifestHash,
        load_policy: Arc<LoadPolicyAttestation>,
    },
}

impl IoBasis {
    pub fn load_policy(&self) -> &LoadPolicyAttestation {
        match self {
            Self::Rpc { load_policy, .. } | Self::Pack { load_policy, .. } => load_policy,
        }
    }

    pub fn rpc_snapshot(&self) -> Option<SnapshotStamp> {
        match self {
            Self::Rpc { snapshot, .. } => Some(*snapshot),
            Self::Pack { .. } => None,
        }
    }
}
