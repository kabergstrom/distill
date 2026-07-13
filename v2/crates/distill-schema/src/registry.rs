//! The daemon's live schema registry (§5, §11): current logical schema +
//! hash per stable type UUID, plus the archived-snapshot accelerator
//! (rebuildable from bundle snapshots).

use std::collections::BTreeMap;

use distill_core::id::{LogicalHash, TypeUuid};
use ngp_schema::{
    node_hash, project, ExtractionError, LogicalSchema, NodeError, Schema, ASSET_REF_UUID,
    FIXED_STATE_UUID, WEAK_ASSET_REF_UUID,
};

#[derive(Debug, Default)]
pub struct SchemaRegistry {
    current: BTreeMap<TypeUuid, (LogicalSchema, LogicalHash)>,
    archived: BTreeMap<LogicalHash, LogicalSchema>,
}

impl SchemaRegistry {
    /// Build from an extracted schema: every type carrying a stable UUID
    /// registers (framework types — AssetRef/WeakAssetRef/the fixed-seed
    /// hasher state — never project standalone and are excluded). A
    /// duplicate UUID is an error naming both types (§7).
    pub fn from_schema(schema: &Schema) -> Result<Self, ExtractionError> {
        let mut current: BTreeMap<TypeUuid, (LogicalSchema, LogicalHash)> = BTreeMap::new();
        let mut owners: BTreeMap<TypeUuid, String> = BTreeMap::new();

        for ty in &schema.types {
            let Some(uuid) = ty.uuid else { continue };
            // Framework types are recognized by identity and skipped BEFORE
            // the duplicate check — every AssetRef<T> instantiation carries
            // the same framework UUID by construction.
            if uuid == ASSET_REF_UUID || uuid == WEAK_ASSET_REF_UUID || uuid == FIXED_STATE_UUID {
                continue;
            }
            if let Some(first) = owners.get(&uuid) {
                return Err(ExtractionError::DuplicateTypeUuid {
                    uuid,
                    first: first.clone(),
                    second: ty.path.display_path(),
                });
            }

            let snap = project(schema, ty.id)?;
            // The projection emits a canonical AST; re-hashing it cannot
            // fail unless the model is internally inconsistent.
            let hash = node_hash(&snap.root).map_err(|e| ExtractionError::MalformedModel {
                detail: format!("projected AST failed to hash: {e:?}"),
            })?;

            owners.insert(uuid, ty.path.display_path());
            current.insert(uuid, (snap, hash));
        }

        Ok(Self {
            current,
            archived: BTreeMap::new(),
        })
    }

    pub fn current(&self, t: TypeUuid) -> Option<(&LogicalSchema, LogicalHash)> {
        self.current.get(&t).map(|(snap, hash)| (snap, *hash))
    }

    /// Accelerator, rebuildable: snapshots decoded from bundles archive
    /// here by their own hash.
    pub fn archived(&self, h: LogicalHash) -> Option<&LogicalSchema> {
        self.archived.get(&h)
    }

    pub fn archive(&mut self, snap: LogicalSchema) -> Result<LogicalHash, NodeError> {
        let hash = node_hash(&snap.root)?;
        self.archived.entry(hash).or_insert(snap);
        Ok(hash)
    }
}
