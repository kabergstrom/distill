use std::collections::BTreeMap;
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::process::Command;

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::bootstrap::{BootstrapControlSpecV1, BootstrapControlSymbol};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_daemon::config::DaemonConfig;
use distill_daemon::process::DaemonProcess;
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringProgressState, MetadataCall, SchemaTransitionAction, SchemaTransitionRequest,
    PROTOCOL_VERSION,
};
use distill_schema::ngp_schema::{
    Field, FieldAttrs, FieldIdentifier, FieldLayout, LayoutIdentity, PrimitiveType, Schema,
    SchemaLayouts, SchemaTypeId, TypeAttrs, TypeDef, TypeLayout, TypePath,
};
use distill_schema::ProjectSchemaAuthority;
use distill_store::state::{InputVersion, PipelineState};

const PROJECT_TYPE: TypeUuid = TypeUuid([0x71; 16]);

#[test]
fn accept_schema_transition_promotes_the_real_pending_candidate_atomically() {
    let module = build_pipeline_fixture();
    let source_identity = fixture_source_identity(&module);
    let schema = project_schema(source_identity);
    let authority = ProjectSchemaAuthority::from_schema(schema.clone(), [0x51; 32]).unwrap();
    let accepted = authority.project_type(PROJECT_TYPE).unwrap().logical_hash;
    let old = LogicalHash([0x33; 32]);
    assert_ne!(old, accepted);

    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    let manifest_path = assets.join("schema/schema-lineage.bundle");
    std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
    std::fs::write(&manifest_path, lineage_manifest_bundle(old)).unwrap();
    std::fs::write(
        temp.path().join("schema.json"),
        serde_json::to_vec(&schema).unwrap(),
    )
    .unwrap();

    let process = DaemonProcess::start(write_config(&temp, &module, authority.identity())).unwrap();
    let coordinator = process.coordinator();
    let server = coordinator.server();
    let required = {
        let store = coordinator.store();
        let store = store.read();
        match store.pipeline_state().unwrap().unwrap() {
            PipelineState::SchemaAcceptanceRequired { required, .. } => required,
            state => panic!("real module staging did not retain a pending candidate: {state:?}"),
        }
    };
    assert!(required.mismatches.iter().any(|mismatch| {
        mismatch.type_uuid == PROJECT_TYPE
            && mismatch.candidate == Some(accepted)
            && mismatch.manifest == Some(old)
    }));
    let base = server.current_stamp().version;
    let request = SchemaTransitionRequest {
        manifest: required.manifest.clone(),
        candidate: required.candidate.clone(),
        type_uuid: PROJECT_TYPE,
        action: SchemaTransitionAction::Accept {
            requested: accepted,
        },
    };
    let metadata = server
        .root()
        .metadata(PROTOCOL_VERSION)
        .connected()
        .unwrap()
        .hub;
    let events = match metadata.schema_transition(base, request.encode().unwrap()) {
        MetadataCall::Success(progress) => progress.collect::<Vec<_>>(),
        other => panic!("metadata schema-transition command was not reachable: {other:?}"),
    };
    assert_eq!(
        events.iter().map(|event| event.state).collect::<Vec<_>>(),
        vec![
            AuthoringProgressState::Started,
            AuthoringProgressState::Running,
            AuthoringProgressState::Completed,
        ]
    );

    let next = InputVersion(base.0 + 1);
    assert_eq!(server.current_stamp().version, next);
    let store = coordinator.store();
    let store = store.read();
    assert_eq!(store.input_version(), next);
    assert_eq!(store.lineage_current(PROJECT_TYPE).unwrap(), Some(accepted));
    let lineage = store.lineage(PROJECT_TYPE).unwrap();
    assert_eq!(lineage.len(), 2);
    assert_eq!(lineage[0].schema_hash, old);
    assert_eq!(lineage[1].schema_hash, accepted);
    assert_eq!(lineage[1].forward_parent, Some(0));
    assert!(matches!(
        store.pipeline_state().unwrap(),
        Some(PipelineState::Ready(_))
    ));
    let basis = store.schema_manifest_basis().unwrap().unwrap();
    drop(store);

    let published_bytes = std::fs::read(&manifest_path).unwrap();
    assert_eq!(
        ContentHash(*blake3::hash(&published_bytes).as_bytes()),
        basis.manifest_hash
    );
    assert_ne!(basis.manifest_hash, required.manifest.manifest_hash);
    let published = distill_bundle::parse_bundle(&published_bytes).unwrap();
    assert_eq!(
        published.assets["manifest"].data,
        lineage_manifest_value(old, Some(accepted))
    );
    assert_eq!(
        coordinator
            .pipeline_snapshot()
            .epoch()
            .expect("accepted candidate must be live")
            .dylib_hash(),
        required.candidate.dylib_hash
    );
}

fn project_schema(source_identity: (String, String)) -> Schema {
    Schema {
        source_hashes: BTreeMap::from([source_identity]),
        type_ops_hash: String::new(),
        layout_hashes: Default::default(),
        rustc_version: String::new(),
        types: vec![
            TypeDef {
                id: SchemaTypeId(0),
                kind: PrimitiveType::Struct,
                path: type_path("schema_transition_fixture", "ProjectRow"),
                uuid: Some(PROJECT_TYPE),
                attrs: TypeAttrs::default(),
                fields: vec![Field {
                    id: FieldIdentifier::Name("value".into()),
                    type_id: SchemaTypeId(1),
                    attrs: FieldAttrs::default(),
                }],
                generic_parameters: Vec::new(),
                generic_argument_ids: Vec::new(),
                has_default: true,
                generic_const_arguments: Vec::new(),
                has_explicit_discriminants: false,
            },
            TypeDef {
                id: SchemaTypeId(1),
                kind: PrimitiveType::U8,
                path: type_path("core", "u8"),
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
            identity: host_layout_identity(),
            layouts: vec![
                TypeLayout {
                    size: Some(1),
                    align: Some(1),
                    layout_complete: true,
                    tag_encoding: None,
                    fields: vec![FieldLayout {
                        offset: Some(0),
                        field_size: Some(1),
                    }],
                },
                TypeLayout {
                    size: Some(size_of::<u8>() as u64),
                    align: Some(align_of::<u8>() as u64),
                    layout_complete: true,
                    tag_encoding: None,
                    fields: Vec::new(),
                },
            ],
        }],
    }
}

fn lineage_manifest_bundle(old: LogicalHash) -> Vec<u8> {
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::SchemaLineageManifest)
        .unwrap();
    let schema = distill_schema::ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
    distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: BundleUuid([0xa1; 16]),
        primary: None,
        schemas: BTreeMap::from([(row.logical_hash, schema)]),
        assets: BTreeMap::from([(
            "manifest".into(),
            AssetEntry {
                uuid: AssetUuid([0xa2; 16]),
                type_uuid: row.type_uuid,
                schema_hash: row.logical_hash,
                lineage: EntryLineageV1::Bootstrap {
                    bundle_format_version: 1,
                },
                authoring_only: true,
                data: lineage_manifest_value(old, None),
            },
        )]),
    })
    .unwrap()
}

fn lineage_manifest_value(old: LogicalHash, accepted: Option<LogicalHash>) -> AuthoredValue {
    let mut epochs = vec![AuthoredValue::Object(BTreeMap::from([
        ("digest".into(), bytes(&old.0)),
        ("forward_parent".into(), AuthoredValue::Null),
    ]))];
    if let Some(accepted) = accepted {
        epochs.push(AuthoredValue::Object(BTreeMap::from([
            ("digest".into(), bytes(&accepted.0)),
            ("forward_parent".into(), AuthoredValue::UInt(0)),
        ])));
    }
    AuthoredValue::Object(BTreeMap::from([(
        "types".into(),
        AuthoredValue::Array(vec![AuthoredValue::Array(vec![
            bytes(&PROJECT_TYPE.0),
            AuthoredValue::Object(BTreeMap::from([
                (
                    "authority".into(),
                    AuthoredValue::Object(BTreeMap::from([(
                        "Active".into(),
                        AuthoredValue::Object(BTreeMap::new()),
                    )])),
                ),
                (
                    "current".into(),
                    AuthoredValue::UInt(if accepted.is_some() { 1 } else { 0 }),
                ),
                ("epochs".into(), AuthoredValue::Array(epochs)),
            ])),
        ])]),
    )]))
}

fn bytes(value: &[u8]) -> AuthoredValue {
    AuthoredValue::Array(
        value
            .iter()
            .map(|byte| AuthoredValue::UInt(u128::from(*byte)))
            .collect(),
    )
}

fn type_path(krate: &str, name: &str) -> TypePath {
    TypePath {
        name: Some(name.into()),
        containing_type: None,
        modules: Vec::new(),
        krate: krate.into(),
    }
}

fn host_layout_identity() -> LayoutIdentity {
    let arch = match std::env::consts::ARCH {
        "aarch64" => "aarch64",
        "x86_64" => "x86_64",
        other => panic!("unsupported test architecture {other}"),
    };
    let target_triple = match std::env::consts::OS {
        "macos" => format!("{arch}-apple-darwin"),
        "linux" => format!("{arch}-unknown-linux-gnu"),
        "windows" => format!("{arch}-pc-windows-msvc"),
        other => panic!("unsupported test OS {other}"),
    };
    LayoutIdentity {
        target_triple,
        rustc: "rustc schema-transition-e2e".into(),
        algorithm_version: 1,
    }
}

fn build_pipeline_fixture() -> PathBuf {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap();
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target/schema-transition-pipeline-fixture"));
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .current_dir(workspace)
        .env("CARGO_TARGET_DIR", &target_dir)
        .args(["build", "--offline", "-p", "distill-pipeline-fixture"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture build failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    target_dir
        .join("debug")
        .join(if cfg!(target_os = "windows") {
            "distill_pipeline_fixture.dll"
        } else if cfg!(target_os = "macos") {
            "libdistill_pipeline_fixture.dylib"
        } else {
            "libdistill_pipeline_fixture.so"
        })
}

fn fixture_source_identity(module: &Path) -> (String, String) {
    let temp = tempfile::tempdir().unwrap();
    let staged =
        ngp_module_host::stage_copy_to(module, &temp.path().join(module.file_name().unwrap()))
            .unwrap();
    // SAFETY: this exact fixture was compiled immediately above and is opened
    // only to read its bounded source-identity export.
    let library = unsafe { ngp_module_host::HostedLibrary::open(staged) }.unwrap();
    // SAFETY: the fixture derives the shared source-identity contract.
    let identity = unsafe { ngp_module_host::read_source_identity(&library) }.unwrap();
    library.close();
    (identity.crate_name, identity.source_hash)
}

fn write_config(
    temp: &tempfile::TempDir,
    module: &Path,
    identity: &LayoutIdentity,
) -> DaemonConfig {
    assert_eq!(&host_layout_identity(), identity);
    let os = match std::env::consts::OS {
        "macos" => "macos",
        "linux" => "linux",
        "windows" => "windows",
        other => panic!("unsupported test OS {other}"),
    };
    let config = format!(
        r#"
[daemon]
address = "127.0.0.1:0"
state_path = "{}"
[assets]
roots = {{ main = "{}" }}
schema_path = "{}"
lineage_manifest = {{ root = "main", path = "schema/schema-lineage.bundle" }}
[modules]
pipeline_dylib = "{}"
[targets.dev]
os = "{}"
arch = "{}"
apis = ["vulkan"]
optimize = false
[codegen]
rs_mod_path = "{}"
auto_codegen = false
[pipeline]
parallelism = 2
max_dependency_depth = 64
batch_reserved_workers = 1
[cas]
segment_size = "1MiB"
cache_limit = "16MiB"
"#,
        temp.path().join("state").display(),
        temp.path().join("assets").display(),
        temp.path().join("schema.json").display(),
        module.display(),
        os,
        std::env::consts::ARCH,
        temp.path().join("generated").display(),
    );
    let path = temp.path().join("distill.toml");
    std::fs::write(&path, config).unwrap();
    DaemonConfig::load(path).unwrap()
}
