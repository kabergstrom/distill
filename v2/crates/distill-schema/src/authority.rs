//! Validated daemon authority derived from one watched shared-schema artifact.
//!
//! The schema model and classifier remain in `ngp-schema`. This layer performs
//! Distill's independent consumer projection: DSLH from the shared logical
//! graph, DSNL from the measured layout table, and DSRE from the attribute and
//! reference walk. The module must later attest the exact resulting rows.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::attestation::{
    AttestationError, CompiledTypeRow, CompiledTypeTable, ReferenceStrength, RegistryExtraFact,
    RegistryExtraRow, RegistryExtrasV1, RegistryPathStep, SchemaNodeId,
    BOOTSTRAP_CONTROL_TYPE_UUIDS,
};
use distill_core::id::{LayoutHash, LogicalHash, TypeUuid};
use distill_wire::derive::derive_wire;
use distill_wire::dsnl::measured_dsnl_hash;
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::measured::{derive_measured_native, MeasuredLayoutError};
use distill_wire::wire::WireNode;
use ngp_schema::classify::{classify, Class};
use ngp_schema::{
    CompilationIdentity, ExtractionError, Field, FieldIdentifier, LayoutView, Schema, SchemaTypeId,
    ASSET_REF_UUID, FIXED_STATE_UUID, WEAK_ASSET_REF_UUID,
};
use unicode_normalization::UnicodeNormalization;

use crate::bootstrap_gen_v1::{consumer_bootstrap_authority_v1, BootstrapGenError};
use crate::SchemaRegistry;

#[derive(Debug)]
pub struct ProjectSchemaAuthority {
    schema: Schema,
    registry: SchemaRegistry,
    identity: CompilationIdentity,
    compiled: CompiledTypeTable,
    project_types: BTreeMap<TypeUuid, ProjectTypeAuthority>,
    source_hash: [u8; 32],
}

/// All schema and target-layout material needed to encode one project asset
/// type and publish its authenticated DSWL tree.
#[derive(Debug, Clone)]
pub struct ProjectTypeAuthority {
    pub schema_type: SchemaTypeId,
    pub logical_schema: ngp_schema::LogicalSchema,
    pub logical_hash: LogicalHash,
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
        let mut rows = Vec::new();
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
            let native = derive_measured_native(view, ty.id)?;
            let native_layout_digest = measured_dsnl_hash(&native)
                .map_err(|error| SchemaAuthorityError::Dsnl(error.to_string()))?;
            let registry_extras = registry_extras(view, ty.id, ty.attrs.build_only)?;
            rows.push(CompiledTypeRow::new(
                type_uuid,
                logical_hash,
                native_layout_digest,
                ty.attrs.build_only,
                registry_extras,
            )?);
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
                    logical_schema: logical_schema.clone(),
                    logical_hash,
                    wire,
                    layout_hash,
                    dswl_bytes,
                },
            );
        }
        let bootstrap_authority = consumer_bootstrap_authority_v1()?;
        rows.extend(bootstrap_authority.rows().iter().cloned());
        let compiled = CompiledTypeTable::canonical(rows)?;
        bootstrap_authority
            .validate_boundary_rows(
                &compiled.rows,
                distill_core::attestation::BundleFormatVersion::V1,
            )
            .map_err(|error| SchemaAuthorityError::Bootstrap(error.to_string()))?;
        Ok(Self {
            schema,
            registry,
            identity,
            compiled,
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

    pub fn identity(&self) -> &CompilationIdentity {
        &self.identity
    }

    pub fn compiled_table(&self) -> &CompiledTypeTable {
        &self.compiled
    }

    pub fn project_type(&self, type_uuid: TypeUuid) -> Option<&ProjectTypeAuthority> {
        self.project_types.get(&type_uuid)
    }

    pub fn project_types(&self) -> &BTreeMap<TypeUuid, ProjectTypeAuthority> {
        &self.project_types
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
    MissingReferenceTarget {
        type_path: String,
    },
    UnsupportedRegistryType {
        type_path: String,
        reason: String,
    },
    Extraction(String),
    Measured(String),
    Dsnl(String),
    Wire(String),
    Dswl(String),
    Attestation(String),
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

impl From<MeasuredLayoutError> for SchemaAuthorityError {
    fn from(error: MeasuredLayoutError) -> Self {
        Self::Measured(error.to_string())
    }
}

impl From<AttestationError> for SchemaAuthorityError {
    fn from(error: AttestationError) -> Self {
        Self::Attestation(error.to_string())
    }
}

impl From<BootstrapGenError> for SchemaAuthorityError {
    fn from(error: BootstrapGenError) -> Self {
        Self::Bootstrap(error.to_string())
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

#[derive(Default)]
struct ExtrasBuilder {
    nodes: BTreeMap<SchemaTypeId, SchemaNodeId>,
    rows: Vec<RegistryExtraRow>,
}

impl ExtrasBuilder {
    fn fact(&mut self, node: SchemaNodeId, path: Vec<RegistryPathStep>, fact: RegistryExtraFact) {
        self.rows.push(RegistryExtraRow { node, path, fact });
    }

    fn enter(
        &mut self,
        id: SchemaTypeId,
        owner: SchemaNodeId,
        path: Vec<RegistryPathStep>,
    ) -> Option<SchemaNodeId> {
        if let Some(target) = self.nodes.get(&id).copied() {
            self.fact(owner, path, RegistryExtraFact::BackReference { target });
            return None;
        }
        let node = SchemaNodeId(self.nodes.len() as u32);
        self.nodes.insert(id, node);
        Some(node)
    }
}

fn registry_extras(
    view: LayoutView<'_>,
    root: SchemaTypeId,
    build_only: bool,
) -> Result<RegistryExtrasV1, SchemaAuthorityError> {
    let mut builder = ExtrasBuilder::default();
    walk_extras(&mut builder, view, root, SchemaNodeId(0), Vec::new())?;
    if builder.nodes.get(&root) != Some(&SchemaNodeId(0)) {
        return Err(SchemaAuthorityError::InvalidAssetRoot {
            type_path: view.schema.types[root.0].path.display_path(),
        });
    }
    builder.fact(
        SchemaNodeId(0),
        Vec::new(),
        RegistryExtraFact::BuildOnly(build_only),
    );
    Ok(RegistryExtrasV1::canonical(builder.rows)?)
}

fn walk_extras(
    builder: &mut ExtrasBuilder,
    view: LayoutView<'_>,
    id: SchemaTypeId,
    owner: SchemaNodeId,
    path: Vec<RegistryPathStep>,
) -> Result<(), SchemaAuthorityError> {
    match classify(view.schema, id) {
        Class::Primitive(_) | Class::Unit | Class::StringNode => Ok(()),
        Class::Boxed(child) | Class::ArcOf(child) => walk_extras(builder, view, child, owner, path),
        Class::Vec(child) | Class::Array { elem: child, .. } | Class::Option(child) => walk_extras(
            builder,
            view,
            child,
            owner,
            appended(path, RegistryPathStep::Elem),
        ),
        Class::Set { elem, .. } => walk_extras(
            builder,
            view,
            elem,
            owner,
            appended(path, RegistryPathStep::Elem),
        ),
        Class::Map { key, value, .. } => {
            walk_extras(
                builder,
                view,
                key,
                owner,
                appended(path.clone(), RegistryPathStep::MapKey),
            )?;
            walk_extras(
                builder,
                view,
                value,
                owner,
                appended(path, RegistryPathStep::MapValue),
            )
        }
        Class::AssetRef(target) => reference_fact(
            builder,
            view,
            target,
            owner,
            path,
            ReferenceStrength::Strong,
        ),
        Class::WeakRef(target) => {
            reference_fact(builder, view, target, owner, path, ReferenceStrength::Weak)
        }
        Class::Struct | Class::Tuple => walk_record(builder, view, id, owner, path),
        Class::Enum => walk_enum(builder, view, id, owner, path),
        Class::EnumVariant => unsupported_registry(view, id, "enum variant outside its enum"),
        Class::FixedState => unsupported_registry(view, id, "fixed-seed BuildHasher as a value"),
        Class::SlotMap { .. } => unsupported_registry(view, id, "engine SlotMap container"),
        Class::Opaque(reason) => unsupported_registry(view, id, &format!("{reason:?}")),
        Class::Malformed(reason) => {
            unsupported_registry(view, id, &format!("malformed schema: {reason:?}"))
        }
    }
}

fn walk_record(
    builder: &mut ExtrasBuilder,
    view: LayoutView<'_>,
    id: SchemaTypeId,
    owner: SchemaNodeId,
    path: Vec<RegistryPathStep>,
) -> Result<(), SchemaAuthorityError> {
    let Some(node) = builder.enter(id, owner, path) else {
        return Ok(());
    };
    let mut fields = view.schema.types[id.0].fields.iter().collect::<Vec<_>>();
    fields.sort_by_key(|field| {
        registry_name(&field.id)
            .unwrap_or_default()
            .nfc()
            .collect::<String>()
            .into_bytes()
    });
    for field in fields {
        let name = registry_name(&field.id).ok_or_else(|| {
            SchemaAuthorityError::UnsupportedRegistryType {
                type_path: view.schema.types[id.0].path.display_path(),
                reason: "variant field in a record".to_owned(),
            }
        })?;
        walk_field(
            builder,
            view,
            field,
            node,
            vec![RegistryPathStep::Field(name)],
        )?;
    }
    Ok(())
}

fn walk_enum(
    builder: &mut ExtrasBuilder,
    view: LayoutView<'_>,
    id: SchemaTypeId,
    owner: SchemaNodeId,
    path: Vec<RegistryPathStep>,
) -> Result<(), SchemaAuthorityError> {
    let Some(node) = builder.enter(id, owner, path) else {
        return Ok(());
    };
    let mut variants = view.schema.types[id.0].fields.iter().collect::<Vec<_>>();
    variants.sort_by_key(|field| match &field.id {
        FieldIdentifier::Variant(name) => name.nfc().collect::<String>().into_bytes(),
        _ => Vec::new(),
    });
    for variant in variants {
        let FieldIdentifier::Variant(variant_name) = &variant.id else {
            return unsupported_registry(view, id, "non-variant field in enum");
        };
        if !matches!(classify(view.schema, variant.type_id), Class::EnumVariant) {
            return unsupported_registry(view, id, "enum payload is not EnumVariant");
        }
        let mut fields = view.schema.types[variant.type_id.0]
            .fields
            .iter()
            .collect::<Vec<_>>();
        fields.sort_by_key(|field| {
            registry_name(&field.id)
                .unwrap_or_default()
                .nfc()
                .collect::<String>()
                .into_bytes()
        });
        for field in fields {
            let field_name = registry_name(&field.id).ok_or_else(|| {
                SchemaAuthorityError::UnsupportedRegistryType {
                    type_path: view.schema.types[id.0].path.display_path(),
                    reason: "nested variant field".to_owned(),
                }
            })?;
            walk_field(
                builder,
                view,
                field,
                node,
                vec![
                    RegistryPathStep::Variant(variant_name.clone()),
                    RegistryPathStep::Field(field_name),
                ],
            )?;
        }
    }
    Ok(())
}

fn walk_field(
    builder: &mut ExtrasBuilder,
    view: LayoutView<'_>,
    field: &Field,
    owner: SchemaNodeId,
    path: Vec<RegistryPathStep>,
) -> Result<(), SchemaAuthorityError> {
    if field.attrs.skip {
        builder.fact(owner, path, RegistryExtraFact::Skip);
        return Ok(());
    }
    if field.attrs.tag {
        builder.fact(owner, path.clone(), RegistryExtraFact::Tag);
    }
    if field.attrs.blob {
        builder.fact(owner, path, RegistryExtraFact::Blob);
        return Ok(());
    }
    walk_extras(builder, view, field.type_id, owner, path)
}

fn reference_fact(
    builder: &mut ExtrasBuilder,
    view: LayoutView<'_>,
    target: SchemaTypeId,
    owner: SchemaNodeId,
    path: Vec<RegistryPathStep>,
    strength: ReferenceStrength,
) -> Result<(), SchemaAuthorityError> {
    let target_type = view.schema.types.get(target.0).ok_or_else(|| {
        SchemaAuthorityError::MissingReferenceTarget {
            type_path: format!("type #{}", target.0),
        }
    })?;
    let target = target_type
        .uuid
        .ok_or_else(|| SchemaAuthorityError::MissingReferenceTarget {
            type_path: target_type.path.display_path(),
        })?;
    builder.fact(
        owner,
        path,
        RegistryExtraFact::Reference { strength, target },
    );
    Ok(())
}

fn registry_name(id: &FieldIdentifier) -> Option<String> {
    match id {
        FieldIdentifier::Name(name) => Some(name.clone()),
        FieldIdentifier::Number(index) => Some(index.to_string()),
        FieldIdentifier::Variant(_) => None,
    }
}

fn appended(mut path: Vec<RegistryPathStep>, step: RegistryPathStep) -> Vec<RegistryPathStep> {
    path.push(step);
    path
}

fn unsupported_registry<T>(
    view: LayoutView<'_>,
    id: SchemaTypeId,
    reason: &str,
) -> Result<T, SchemaAuthorityError> {
    Err(SchemaAuthorityError::UnsupportedRegistryType {
        type_path: view.schema.types[id.0].path.display_path(),
        reason: reason.to_owned(),
    })
}
