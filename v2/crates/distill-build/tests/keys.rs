use distill_build::keys::*;
use distill_build::pipeline::{GraphicsApi, Target, TargetArch, TargetOs};
use distill_build::trace::{Observed, TraceOp};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};
use distill_schema::ngp_schema::LayoutIdentity;

fn test_layout_identity() -> LayoutIdentity {
    LayoutIdentity {
        target_triple: "x86_64-unknown-linux-gnu".into(),
        rustc: "rustc test".into(),
        algorithm_version: 1,
    }
}

fn statics() -> StaticInputs {
    StaticInputs {
        asset: AssetUuid([1; 16]),
        stage: 2,
        input_hash: ContentHash([3; 32]),
        target_def_hash: [4; 32],
        processor_id: "proc".into(),
        processor_version: 5,
        dylib_hash: [6; 32],
        output_hashes: vec![OutputHash {
            key: "".into(),
            logical: LogicalHash([7; 32]),
            layout: LayoutHash([8; 32]),
        }],
        artifact_format_version: 9,
    }
}

#[test]
fn static_key_changes_for_every_static_determinant() {
    let base = statics();
    let digest = static_inputs_digest(&base);
    let mut changed = base.clone();
    changed.stage += 1;
    assert_ne!(digest, static_inputs_digest(&changed));
    let mut changed = base.clone();
    changed.output_hashes[0].layout = LayoutHash([0; 32]);
    assert_ne!(digest, static_inputs_digest(&changed));
    let mut changed = base.clone();
    changed.dylib_hash[0] ^= 1;
    assert_ne!(digest, static_inputs_digest(&changed));
}

#[test]
fn build_import_key_names_the_sibling_entry_and_bundle_bytes() {
    let base = BuildImportInputs {
        asset: AssetUuid([1; 16]),
        bundle: BundleUuid([2; 16]),
        local_id: "a".into(),
        authored_type: TypeUuid([3; 16]),
        terminal_type: TypeUuid([4; 16]),
        canonical_bundle_bytes: b"bundle".to_vec(),
        logical: LogicalHash([5; 32]),
        layout: LayoutHash([6; 32]),
        migrations: vec![],
        automatic_migration: None,
        validator_dylib_hash: Some([7; 32]),
        artifact_format_version: 1,
    };
    let mut sibling = base.clone();
    sibling.local_id = "b".into();
    assert_ne!(build_import_digest(&base), build_import_digest(&sibling));
    let mut edited = base.clone();
    edited.canonical_bundle_bytes.push(0);
    assert_ne!(build_import_digest(&base), build_import_digest(&edited));
    let mut validator = base.clone();
    validator.validator_dylib_hash = Some([8; 32]);
    assert_ne!(build_import_digest(&base), build_import_digest(&validator));

    let mut automatic = base.clone();
    automatic.automatic_migration = Some(AutomaticMigration {
        from: LogicalHash([9; 32]),
        to: LogicalHash([10; 32]),
        planner_version: 1,
        dylib_hash: None,
    });
    assert_ne!(build_import_digest(&base), build_import_digest(&automatic));
    let digest = build_import_digest(&automatic);
    automatic
        .automatic_migration
        .as_mut()
        .unwrap()
        .planner_version = 2;
    assert_ne!(digest, build_import_digest(&automatic));
}

#[test]
fn full_input_hash_includes_labeled_trace() {
    let base = statics();
    let a = vec![TraceOp::Resolve {
        path: "a".into(),
        observed: Observed::Ok(None),
    }];
    let b = vec![TraceOp::Resolve {
        path: "b".into(),
        observed: Observed::Ok(None),
    }];
    assert_ne!(full_input_hash(&base, &a), full_input_hash(&base, &b));
}

#[test]
fn target_definition_hash_includes_full_api_set() {
    let vk = GraphicsApi::new("vulkan").unwrap();
    let gl = GraphicsApi::new("opengl").unwrap();
    let a = Target::new(
        TargetOs::Linux,
        TargetArch::X86_64,
        [vk.clone()].into_iter().collect(),
        false,
        true,
        test_layout_identity(),
    )
    .unwrap();
    let b = Target::new(
        TargetOs::Linux,
        TargetArch::X86_64,
        [vk, gl].into_iter().collect(),
        false,
        true,
        test_layout_identity(),
    )
    .unwrap();
    assert_ne!(target_definition_hash(&a), target_definition_hash(&b));
}

#[test]
fn target_definition_hash_binds_target_fields_and_layout_identity() {
    let api = [GraphicsApi::new("vulkan").unwrap()].into_iter().collect();
    let identity = test_layout_identity();
    let base = Target::new(
        TargetOs::Linux,
        TargetArch::X86_64,
        api,
        false,
        true,
        identity.clone(),
    )
    .unwrap();
    let mut changed_identity = identity;
    changed_identity.algorithm_version += 1;
    let variants = [
        Target::new(
            TargetOs::Linux,
            TargetArch::Aarch64,
            base.apis.clone(),
            false,
            true,
            base.layout_identity.clone(),
        )
        .unwrap(),
        Target::new(
            TargetOs::Linux,
            TargetArch::X86_64,
            base.apis.clone(),
            true,
            true,
            base.layout_identity.clone(),
        )
        .unwrap(),
        Target::new(
            TargetOs::Linux,
            TargetArch::X86_64,
            base.apis.clone(),
            false,
            false,
            base.layout_identity.clone(),
        )
        .unwrap(),
        Target::new(
            TargetOs::Linux,
            TargetArch::X86_64,
            base.apis.clone(),
            false,
            true,
            changed_identity,
        )
        .unwrap(),
    ];
    let base_hash = target_definition_hash(&base);
    for variant in variants {
        assert_ne!(base_hash, target_definition_hash(&variant));
    }
}

fn node_inputs() -> distill_build::keys::NodeInputs {
    use distill_build::keys::{NodeInputs, NodeStage, NodeType};
    NodeInputs {
        asset: AssetUuid([1; 16]),
        bundle: distill_core::id::BundleUuid([2; 16]),
        local_id: "entry".to_owned(),
        bundle_hash: ContentHash([3; 32]),
        authored_type: TypeUuid([4; 16]),
        authored_logical: LogicalHash([5; 32]),
        target_def_hash: [6; 32],
        dylib_hash: [7; 32],
        validated: false,
        terminal_type: TypeUuid([8; 16]),
        extras: vec![("b".to_owned(), TypeUuid([9; 16])), ("a".to_owned(), TypeUuid([10; 16]))],
        stages: vec![NodeStage {
            processor_id: "cook".to_owned(),
            processor_version: 1,
            primary: TypeUuid([8; 16]),
            extras: vec![("b".to_owned(), TypeUuid([9; 16]))],
        }],
        types: vec![
            NodeType {
                type_uuid: TypeUuid([8; 16]),
                logical: LogicalHash([11; 32]),
                layout: LayoutHash([12; 32]),
            },
            NodeType {
                type_uuid: TypeUuid([4; 16]),
                logical: LogicalHash([5; 32]),
                layout: LayoutHash([13; 32]),
            },
        ],
        migration_planner_version: 1,
        artifact_format_version: 1,
    }
}

#[test]
fn the_node_key_covers_static_inputs_only_and_ignores_declaration_order() {
    use distill_build::keys::{node_canonical_bytes, node_digest};
    let inputs = node_inputs();
    let key = node_digest(&inputs);
    assert_eq!(key, node_digest(&inputs.clone()));

    let mut reordered = inputs.clone();
    reordered.types.reverse();
    reordered.extras.reverse();
    assert_eq!(node_digest(&reordered), key);

    let mut other_asset = inputs.clone();
    other_asset.asset = AssetUuid([99; 16]);
    assert_ne!(node_digest(&other_asset), key, "the asset uuid is observable");

    let mut edited = inputs.clone();
    edited.bundle_hash = ContentHash([99; 32]);
    assert_ne!(node_digest(&edited), key);

    let mut rebuilt_pipeline = inputs.clone();
    rebuilt_pipeline.dylib_hash = [99; 32];
    assert_ne!(node_digest(&rebuilt_pipeline), key);

    let mut relaid = inputs.clone();
    relaid.types[0].layout = LayoutHash([99; 32]);
    assert_ne!(node_digest(&relaid), key);

    assert!(!node_canonical_bytes(&inputs).is_empty());
}
