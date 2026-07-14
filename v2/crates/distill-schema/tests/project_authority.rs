use distill_asset::AssetType;
use distill_core::attestation::BOOTSTRAP_CONTROL_COUNT;
use distill_core::id::TypeUuid;
use distill_schema::ngp_schema::{
    Field, FieldAttrs, FieldIdentifier, FieldLayout, PrimitiveType, Schema, SchemaLayouts,
    SchemaTypeId, TypeAttrs, TypeDef, TypeLayout, TypePath,
};
use distill_schema::{ProjectSchemaAuthority, SchemaAuthorityError};
use distill_wire::dswl::{decode_dswl, dswl_hash};

const PROJECT_UUID: TypeUuid = TypeUuid([
    0x91, 0x11, 0x22, 0x33, 0x44, 0x55, 0x46, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
]);

#[repr(C)]
#[distill_asset::asset(uuid = "91112233-4455-4677-8899-aabbccddeeff")]
struct ProjectRow {
    alpha: u32,
    beta: u16,
}

fn path(krate: &str, name: &str) -> TypePath {
    TypePath {
        name: Some(name.to_owned()),
        containing_type: None,
        modules: Vec::new(),
        krate: krate.to_owned(),
    }
}

fn project_schema() -> Schema {
    let types = vec![
        TypeDef {
            id: SchemaTypeId(0),
            kind: PrimitiveType::Struct,
            path: path("game", "ProjectRow"),
            uuid: Some(PROJECT_UUID),
            attrs: TypeAttrs::default(),
            fields: vec![
                Field {
                    id: FieldIdentifier::Name("alpha".to_owned()),
                    type_id: SchemaTypeId(1),
                    attrs: FieldAttrs::default(),
                },
                Field {
                    id: FieldIdentifier::Name("beta".to_owned()),
                    type_id: SchemaTypeId(2),
                    attrs: FieldAttrs::default(),
                },
            ],
            generic_parameters: Vec::new(),
            generic_argument_ids: Vec::new(),
            has_default: false,
        },
        TypeDef {
            id: SchemaTypeId(1),
            kind: PrimitiveType::U32,
            path: path("core", "u32"),
            uuid: None,
            attrs: TypeAttrs::default(),
            fields: Vec::new(),
            generic_parameters: Vec::new(),
            generic_argument_ids: Vec::new(),
            has_default: true,
        },
        TypeDef {
            id: SchemaTypeId(2),
            kind: PrimitiveType::U16,
            path: path("core", "u16"),
            uuid: None,
            attrs: TypeAttrs::default(),
            fields: Vec::new(),
            generic_parameters: Vec::new(),
            generic_argument_ids: Vec::new(),
            has_default: true,
        },
    ];
    let layouts = vec![
        TypeLayout {
            size: Some(std::mem::size_of::<ProjectRow>() as u64),
            align: Some(std::mem::align_of::<ProjectRow>() as u64),
            layout_complete: true,
            tag_encoding: None,
            fields: vec![
                FieldLayout {
                    offset: Some(std::mem::offset_of!(ProjectRow, alpha) as u64),
                    field_size: Some(std::mem::size_of::<u32>() as u64),
                },
                FieldLayout {
                    offset: Some(std::mem::offset_of!(ProjectRow, beta) as u64),
                    field_size: Some(std::mem::size_of::<u16>() as u64),
                },
            ],
        },
        TypeLayout {
            size: Some(4),
            align: Some(4),
            layout_complete: true,
            tag_encoding: None,
            fields: Vec::new(),
        },
        TypeLayout {
            size: Some(2),
            align: Some(2),
            layout_complete: true,
            tag_encoding: None,
            fields: Vec::new(),
        },
    ];
    Schema {
        source_hashes: Default::default(),
        types,
        layouts: vec![SchemaLayouts {
            identity: distill_schema::bootstrap_gen_v1::consumer_compilation_identity_v1().clone(),
            layouts,
        }],
    }
}

#[test]
fn schema_projection_matches_the_independently_compiled_descriptor() {
    let authority = ProjectSchemaAuthority::from_schema(project_schema(), [7; 32]).unwrap();
    let observed = authority
        .compiled_table()
        .rows
        .iter()
        .find(|row| row.type_uuid == PROJECT_UUID)
        .unwrap();

    assert_eq!(observed, ProjectRow::descriptor().compiled_type);
    assert_eq!(
        authority.compiled_table().rows.len(),
        BOOTSTRAP_CONTROL_COUNT + 1
    );
    assert_eq!(authority.source_hash(), [7; 32]);

    let project = authority.project_type(PROJECT_UUID).unwrap();
    assert_eq!(project.schema_type, SchemaTypeId(0));
    assert_eq!(project.logical_hash, observed.logical_hash);
    assert_eq!(
        project.logical_schema,
        *authority.registry().current(PROJECT_UUID).unwrap().0
    );
    assert_eq!(dswl_hash(&project.wire).unwrap(), project.layout_hash);
    let decoded = decode_dswl(&project.dswl_bytes).unwrap();
    assert_eq!(dswl_hash(&decoded).unwrap(), project.layout_hash);
    assert_eq!(authority.project_types().len(), 1);
}

#[test]
fn layout_tables_must_be_positionally_parallel_to_the_shared_model() {
    let mut schema = project_schema();
    schema.layouts[0].layouts[0].fields.pop();
    assert!(matches!(
        ProjectSchemaAuthority::from_schema(schema, [0; 32]),
        Err(SchemaAuthorityError::FieldLayoutLength { .. })
    ));
}

#[test]
fn project_types_cannot_override_a_sealed_bootstrap_uuid() {
    let mut schema = project_schema();
    schema.types[0].uuid = Some(distill_core::attestation::PACK_DEFINITION_TYPE_UUID);
    assert!(matches!(
        ProjectSchemaAuthority::from_schema(schema, [0; 32]),
        Err(SchemaAuthorityError::BootstrapTypeCollision { .. })
    ));
}
