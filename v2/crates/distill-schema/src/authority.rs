//! Validated daemon authority derived from one watched shared-schema artifact.
//!
//! The schema model and classifier remain in `ngp-schema`. This layer performs
//! Distill's logical projection and authenticated DSWL trees. Native layout
//! compatibility is checked locally when the loader compiles a DSWL plan.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::bootstrap::BOOTSTRAP_CONTROL_TYPE_UUIDS;
use distill_core::id::{LayoutHash, LogicalHash, TypeUuid};
use distill_wire::derive::derive_wire;
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::WireNode;
use ngp_schema::classify::{classify, Class};
use ngp_schema::{
    ExtractionError, LayoutIdentity, Schema, SchemaTypeId, ASSET_REF_UUID, FIXED_STATE_UUID,
    WEAK_ASSET_REF_UUID,
};

use crate::SchemaRegistry;

#[derive(Debug)]
pub struct ProjectSchemaAuthority {
    schema: Schema,
    registry: SchemaRegistry,
    identity: LayoutIdentity,
    project_types: BTreeMap<TypeUuid, ProjectTypeAuthority>,
    source_hash: [u8; 32],
}

/// All schema and target-layout material needed to encode one project asset
/// type and publish its authenticated DSWL tree.
#[derive(Debug, Clone)]
pub struct ProjectTypeAuthority {
    pub schema_type: SchemaTypeId,
    /// Runtime load-closure policy from the shared schema. This is kept out
    /// of logical hashes, but remains authoritative for daemon builds.
    pub build_only: bool,
    pub logical_schema: ngp_schema::LogicalSchema,
    pub logical_hash: LogicalHash,
    /// Renamed fields by planner display path, for migration planning.
    pub renamed_from: ngp_schema::Renames,
    pub wire: WireNode,
    pub layout_hash: LayoutHash,
    pub dswl_bytes: Vec<u8>,
}

impl ProjectSchemaAuthority {
    pub fn from_json(bytes: &[u8]) -> Result<Self, SchemaAuthorityError> {
        let schema = Schema::from_json(bytes)
            .map_err(|error| SchemaAuthorityError::Json(error.to_string()))?;
        Self::from_schema(schema, *blake3::hash(bytes).as_bytes())
    }

    pub fn from_schema(
        schema: Schema,
        source_hash: [u8; 32],
    ) -> Result<Self, SchemaAuthorityError> {
        validate_model(&schema)?;
        let view = schema
            .single_layout()
            .ok_or(SchemaAuthorityError::LayoutTableCount {
                got: schema.layouts.len(),
            })?;
        let identity = view.table.identity.clone();
        let registry = SchemaRegistry::from_schema(&schema)?;
        let mut project_types = BTreeMap::new();
        let bootstrap = BOOTSTRAP_CONTROL_TYPE_UUIDS
            .into_iter()
            .collect::<BTreeSet<_>>();
        for ty in &schema.types {
            let Some(type_uuid) = ty.uuid else { continue };
            if framework_uuid(type_uuid) {
                continue;
            }
            if bootstrap.contains(&type_uuid) {
                return Err(SchemaAuthorityError::BootstrapTypeCollision {
                    type_uuid,
                    type_path: ty.path.display_path(),
                });
            }
            if !matches!(classify(&schema, ty.id), Class::Struct | Class::Enum) {
                return Err(SchemaAuthorityError::InvalidAssetRoot {
                    type_path: ty.path.display_path(),
                });
            }
            if !ty.generic_parameters.is_empty() {
                return Err(SchemaAuthorityError::GenericAssetRoot {
                    type_path: ty.path.display_path(),
                });
            }
            let (logical_schema, logical_hash) = registry.current(type_uuid).ok_or_else(|| {
                SchemaAuthorityError::MissingProjection {
                    type_path: ty.path.display_path(),
                }
            })?;
            let (_, renamed_from) = ngp_schema::project_with_renames(&schema, ty.id)?;
            let wire = derive_wire(view, ty.id)
                .map_err(|error| SchemaAuthorityError::Wire(error.to_string()))?;
            let layout_hash =
                dswl_hash(&wire).map_err(|error| SchemaAuthorityError::Dswl(error.to_string()))?;
            let dswl_bytes =
                dswl_bytes(&wire).map_err(|error| SchemaAuthorityError::Dswl(error.to_string()))?;
            project_types.insert(
                type_uuid,
                ProjectTypeAuthority {
                    schema_type: ty.id,
                    build_only: ty.attrs.build_only,
                    logical_schema: logical_schema.clone(),
                    logical_hash,
                    renamed_from,
                    wire,
                    layout_hash,
                    dswl_bytes,
                },
            );
        }
        Ok(Self {
            schema,
            registry,
            identity,
            project_types,
            source_hash,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn registry(&self) -> &SchemaRegistry {
        &self.registry
    }

    pub fn identity(&self) -> &LayoutIdentity {
        &self.identity
    }

    pub fn project_type(&self, type_uuid: TypeUuid) -> Option<&ProjectTypeAuthority> {
        self.project_types.get(&type_uuid)
    }

    pub fn project_types(&self) -> &BTreeMap<TypeUuid, ProjectTypeAuthority> {
        &self.project_types
    }

    pub fn logical_registry(
        &self,
    ) -> Result<BTreeMap<TypeUuid, LogicalHash>, SchemaAuthorityError> {
        let mut registry = distill_core::bootstrap::bootstrap_control_logical_registry_v1()
            .map_err(|error| SchemaAuthorityError::Bootstrap(error.to_string()))?;
        registry.extend(
            self.project_types
                .iter()
                .map(|(type_uuid, authority)| (*type_uuid, authority.logical_hash)),
        );
        Ok(registry)
    }

    pub fn source_hash(&self) -> [u8; 32] {
        self.source_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaAuthorityError {
    Json(String),
    NonContiguousTypeId {
        index: usize,
        observed: SchemaTypeId,
    },
    DanglingTypeId {
        owner: String,
        target: SchemaTypeId,
    },
    LayoutTableCount {
        got: usize,
    },
    LayoutTableLength {
        expected: usize,
        got: usize,
    },
    FieldLayoutLength {
        type_path: String,
        expected: usize,
        got: usize,
    },
    DuplicateLayoutIdentity,
    BootstrapTypeCollision {
        type_uuid: TypeUuid,
        type_path: String,
    },
    InvalidAssetRoot {
        type_path: String,
    },
    GenericAssetRoot {
        type_path: String,
    },
    MissingProjection {
        type_path: String,
    },
    Extraction(String),
    Wire(String),
    Dswl(String),
    Bootstrap(String),
}

impl std::fmt::Display for SchemaAuthorityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "schema authority: {self:?}")
    }
}

impl std::error::Error for SchemaAuthorityError {}

impl From<ExtractionError> for SchemaAuthorityError {
    fn from(error: ExtractionError) -> Self {
        Self::Extraction(error.to_string())
    }
}

fn framework_uuid(type_uuid: TypeUuid) -> bool {
    matches!(
        type_uuid,
        ASSET_REF_UUID | WEAK_ASSET_REF_UUID | FIXED_STATE_UUID
    )
}

fn validate_model(schema: &Schema) -> Result<(), SchemaAuthorityError> {
    for (index, ty) in schema.types.iter().enumerate() {
        if ty.id != SchemaTypeId(index) {
            return Err(SchemaAuthorityError::NonContiguousTypeId {
                index,
                observed: ty.id,
            });
        }
        for target in ty
            .fields
            .iter()
            .map(|field| field.type_id)
            .chain(ty.generic_argument_ids.iter().copied())
        {
            if target.0 >= schema.types.len() {
                return Err(SchemaAuthorityError::DanglingTypeId {
                    owner: ty.path.display_path(),
                    target,
                });
            }
        }
    }
    let mut identities = BTreeSet::new();
    for table in &schema.layouts {
        if !identities.insert(ngp_schema::identity_digest(&table.identity)) {
            return Err(SchemaAuthorityError::DuplicateLayoutIdentity);
        }
        if table.layouts.len() != schema.types.len() {
            return Err(SchemaAuthorityError::LayoutTableLength {
                expected: schema.types.len(),
                got: table.layouts.len(),
            });
        }
        for (ty, layout) in schema.types.iter().zip(&table.layouts) {
            if layout.fields.len() != ty.fields.len() {
                return Err(SchemaAuthorityError::FieldLayoutLength {
                    type_path: ty.path.display_path(),
                    expected: ty.fields.len(),
                    got: layout.fields.len(),
                });
            }
        }
    }
    Ok(())
}
