use distill_core::bootstrap::BOOTSTRAP_CONTROL_COUNT;
use distill_core::id::TypeUuid;
use distill_json::AuthoredValue;
use distill_schema::ngp_schema::{
    Field, FieldAttrs, FieldIdentifier, FieldLayout, LayoutIdentity, PrimitiveType, Schema,
    SchemaLayouts, SchemaTypeId, TypeAttrs, TypeDef, TypeLayout, TypePath,
};
use distill_schema::{extract_search_tags, ProjectSchemaAuthority, SchemaAuthorityError};
use distill_wire::dswl::{decode_dswl, dswl_hash};
use std::collections::BTreeMap;

const PROJECT_UUID: TypeUuid = TypeUuid([
    0x91, 0x11, 0x22, 0x33, 0x44, 0x55, 0x46, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
]);

fn test_layout_identity() -> LayoutIdentity {
    LayoutIdentity {
        target_triple: "x86_64-unknown-linux-gnu".into(),
        rustc: "rustc test".into(),
        algorithm_version: 1,
    }
}

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
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
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
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
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
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
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
        type_ops_hash: String::new(),
        layout_hashes: Default::default(),
        rustc_version: String::new(),
        types,
        layouts: vec![SchemaLayouts {
            identity: test_layout_identity(),
            layouts,
        }],
    }
}

fn tagged_schema() -> Schema {
    Schema {
        source_hashes: Default::default(),
        type_ops_hash: String::new(),
        layout_hashes: Default::default(),
        rustc_version: String::new(),
        types: vec![
            TypeDef {
                id: SchemaTypeId(0),
                kind: PrimitiveType::Struct,
                path: path("game", "TaggedRow"),
                uuid: Some(TypeUuid([0x42; 16])),
                attrs: TypeAttrs::default(),
                fields: vec![Field {
                    id: FieldIdentifier::Name("category".to_owned()),
                    type_id: SchemaTypeId(1),
                    attrs: FieldAttrs {
                        tag: true,
                        ..FieldAttrs::default()
                    },
                }],
                generic_parameters: Vec::new(),
                generic_argument_ids: Vec::new(),
                has_default: false,
                generic_const_arguments: Vec::new(),
                has_explicit_discriminants: false,
            },
            TypeDef {
                id: SchemaTypeId(1),
                kind: PrimitiveType::String,
                path: path("alloc", "String"),
                uuid: None,
                attrs: TypeAttrs::default(),
                fields: Vec::new(),
                generic_parameters: Vec::new(),
                generic_argument_ids: Vec::new(),
                has_default: true,
                generic_const_arguments: Vec::new(),
                has_explicit_discriminants: false,
            },
        ],
        layouts: vec![SchemaLayouts {
            identity: test_layout_identity(),
            layouts: vec![
                TypeLayout {
                    size: Some(std::mem::size_of::<String>() as u64),
                    align: Some(std::mem::align_of::<String>() as u64),
                    layout_complete: true,
                    tag_encoding: None,
                    fields: vec![FieldLayout {
                        offset: Some(0),
                        field_size: Some(std::mem::size_of::<String>() as u64),
                    }],
                },
                TypeLayout {
                    size: Some(std::mem::size_of::<String>() as u64),
                    align: Some(std::mem::align_of::<String>() as u64),
                    layout_complete: true,
                    tag_encoding: None,
                    fields: Vec::new(),
                },
            ],
        }],
    }
}

#[test]
fn schema_projection_produces_logical_registry_and_authenticated_wire() {
    let authority = ProjectSchemaAuthority::from_schema(project_schema(), [7; 32]).unwrap();
    assert_eq!(authority.source_hash(), [7; 32]);

    let project = authority.project_type(PROJECT_UUID).unwrap();
    assert_eq!(project.schema_type, SchemaTypeId(0));
    assert!(!project.build_only);
    assert_eq!(
        project.logical_schema,
        *authority.registry().current(PROJECT_UUID).unwrap().0
    );
    let logical_registry = authority.logical_registry().unwrap();
    assert_eq!(logical_registry[&PROJECT_UUID], project.logical_hash);
    assert_eq!(logical_registry.len(), BOOTSTRAP_CONTROL_COUNT + 1);
    assert_eq!(dswl_hash(&project.wire).unwrap(), project.layout_hash);
    let decoded = decode_dswl(&project.dswl_bytes).unwrap();
    assert_eq!(dswl_hash(&decoded).unwrap(), project.layout_hash);
    assert_eq!(authority.project_types().len(), 1);
}

#[test]
fn project_authority_preserves_build_only_policy() {
    let mut schema = project_schema();
    schema.types[0].attrs.build_only = true;
    let authority = ProjectSchemaAuthority::from_schema(schema, [7; 32]).unwrap();
    assert!(authority.project_type(PROJECT_UUID).unwrap().build_only);
}

#[test]
fn project_authority_carries_renamed_fields_outside_the_hash() {
    let plain = ProjectSchemaAuthority::from_schema(project_schema(), [7; 32]).unwrap();
    let mut schema = project_schema();
    schema.types[0].fields[1].attrs.renamed_from = Some("gamma".to_owned());
    let renamed = ProjectSchemaAuthority::from_schema(schema, [7; 32]).unwrap();
    let (plain, renamed) = (
        plain.project_type(PROJECT_UUID).unwrap(),
        renamed.project_type(PROJECT_UUID).unwrap(),
    );
    assert_eq!(plain.logical_hash, renamed.logical_hash);
    assert!(plain.renamed_from.is_empty());
    assert_eq!(
        renamed.renamed_from,
        BTreeMap::from([("$.beta".to_owned(), "gamma".to_owned())])
    );
}

#[test]
fn current_schema_tag_extraction_reads_and_validates_annotated_string_values() {
    let schema = tagged_schema();
    let tags = extract_search_tags(
        &schema,
        SchemaTypeId(0),
        &AuthoredValue::Object(BTreeMap::from([(
            "category".to_owned(),
            AuthoredValue::Str("enemy".to_owned()),
        )])),
    )
    .unwrap();
    assert_eq!(
        tags,
        BTreeMap::from([("category".to_owned(), "enemy".to_owned())])
    );

    let invalid = extract_search_tags(
        &schema,
        SchemaTypeId(0),
        &AuthoredValue::Object(BTreeMap::from([(
            "category".to_owned(),
            AuthoredValue::Str("enemy\0hidden".to_owned()),
        )])),
    );
    assert!(invalid.is_err());
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
    schema.types[0].uuid = Some(distill_core::bootstrap::PACK_DEFINITION_TYPE_UUID);
    assert!(matches!(
        ProjectSchemaAuthority::from_schema(schema, [0; 32]),
        Err(SchemaAuthorityError::BootstrapTypeCollision { .. })
    ));
}
