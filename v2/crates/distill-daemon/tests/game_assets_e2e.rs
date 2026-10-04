use std::collections::BTreeMap;
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distill_asset::{AssetType, ErasedValue, ModuleEpochToken};
use distill_build::keys::target_definition_hash;
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, TypeUuid};
use distill_daemon::config::DaemonConfig;
use distill_daemon::pack_command::{build_configured_pack, PackCommandError};
use distill_daemon::process::DaemonProcess;
use distill_json::AuthoredValue;
use distill_loader::{
    AdoptionId, AssetStorage, GameModuleEpoch, HandleId, LoadStatus, Loader, LoaderDiagnostic,
    PendingState, PendingToken, RpcIo, StorageError, UpdateResult,
};
use distill_pack::{PackfileIO, RuntimeTarget as PackRuntimeTarget};
use distill_rpc::{
    AuthoringValue, ConnectOutcome, ConnectRequest, ImportRequest, ResolveResult,
    TargetDefinitionHash,
};
use distill_schema::ngp_schema::{
    Field, FieldAttrs, FieldIdentifier, FieldLayout, LayoutIdentity, PrimitiveType, Schema,
    SchemaLayouts, SchemaTypeId, TypeAttrs, TypeDef, TypeLayout, TypePath,
};
use distill_schema::ProjectSchemaAuthority;
use distill_wire::native::CallbackPanic;

const SETTINGS_TYPE: TypeUuid = TypeUuid([0x90; 16]);
const TEXTURE_SOURCE_TYPE: TypeUuid = TypeUuid([0x91; 16]);
const MESH_SOURCE_TYPE: TypeUuid = TypeUuid([0x92; 16]);
const SHADER_SOURCE_TYPE: TypeUuid = TypeUuid([0x93; 16]);

use newgameplus_assets::{CookedPipeline, MeshAsset, TextureAsset};

#[path = "support/pack_definition.rs"]
mod pack_definition;

#[repr(C)]
struct FixtureSettings {
    value: u8,
}

#[repr(C)]
struct SourceValue {
    value: String,
}

#[derive(Default)]
struct Storage {
    values: BTreeMap<(HandleId, AdoptionId), ErasedValue>,
    updates: Vec<(HandleId, AdoptionId)>,
    commits: Vec<(HandleId, AdoptionId)>,
}

impl AssetStorage for Storage {
    fn update(
        &mut self,
        _type_uuid: TypeUuid,
        handle: HandleId,
        value: ErasedValue,
        adoption: AdoptionId,
    ) -> Result<UpdateResult, StorageError> {
        self.updates.push((handle, adoption));
        self.values.insert((handle, adoption), value);
        Ok(UpdateResult::Ready)
    }

    fn poll(&mut self, _token: PendingToken) -> PendingState {
        PendingState::Ready
    }

    fn commit(&mut self, _type_uuid: TypeUuid, handle: HandleId, adoption: AdoptionId) {
        self.commits.push((handle, adoption));
    }

    fn free(
        &mut self,
        _type_uuid: TypeUuid,
        handle: HandleId,
        adoption: AdoptionId,
    ) -> Result<(), CallbackPanic> {
        if let Some(value) = self.values.remove(&(handle, adoption)) {
            value.destroy()?;
        }
        Ok(())
    }
}

#[test]
fn imports_cooks_hot_reloads_packs_mounts_and_adopts_basic_game_assets() {
    let module = build_pipeline_fixture();
    let source_identity = fixture_source_identity(&module);
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(assets.join("shaders")).unwrap();
    std::fs::write(assets.join("pixel.ppm"), b"P6\n1 1\n255\n\xff\x00\x00").unwrap();
    std::fs::write(
        assets.join("triangle.obj"),
        b"v 0 0 0\nv 1 0 0\nv 0 1 0\nf 1 2 3\n",
    )
    .unwrap();
    std::fs::write(
        assets.join("shaders/basic.comp"),
        b"#version 450\n#include \"shaders/common.inc\"\nlayout(local_size_x=1, local_size_y=1, local_size_z=1) in;\nvoid main() { uint value = VALUE; }\n",
    )
    .unwrap();
    std::fs::write(
        assets.join("shaders/common.inc"),
        b"const uint VALUE = 1;\n",
    )
    .unwrap();

    let schema = fixture_schema(source_identity);
    let authority = ProjectSchemaAuthority::from_schema(schema.clone(), [0x51; 32]).unwrap();
    for descriptor in [
        TextureAsset::descriptor(),
        MeshAsset::descriptor(),
        CookedPipeline::descriptor(),
    ] {
        assert_eq!(
            authority
                .project_type(descriptor.type_uuid)
                .unwrap()
                .logical_hash,
            descriptor.logical_hash,
            "the pipeline schema and typed game runtime must describe the same terminal value",
        );
    }
    std::fs::write(
        temp.path().join("schema.json"),
        serde_json::to_vec(&schema).unwrap(),
    )
    .unwrap();
    let config = write_config(&temp, &module, authority.identity());
    let process = DaemonProcess::start(config.clone()).unwrap();
    let reader = process.coordinator().open_reader().unwrap();
    let failure = process.coordinator().pipeline_failure(&reader).unwrap();
    assert!(
        failure.is_none() && process.coordinator().ready_dylib_hash().is_some(),
        "fixture pipeline did not become ready: {failure:?}"
    );
    drop(reader);

    let [texture, mesh, shader] = import_assets(&process, &config, &assets);

    let coordinator = process.coordinator();
    let reader = coordinator.open_reader().unwrap();
    let target = coordinator
        .compiled_at(&reader)
        .unwrap()
        .build_target("dev")
        .unwrap();
    let target_hash = target_definition_hash(&target);
    let request = ConnectRequest::new("dev", TargetDefinitionHash(target_hash));

    let mut live_loader =
        Loader::new(RpcIo::connect(process.rpc_address(), request.clone()).unwrap());
    register_runtime(&mut live_loader, target_hash, 1);
    let texture_handle = live_loader.add_ref::<TextureAsset>(texture).unwrap();
    let mesh_handle = live_loader.add_ref::<MeshAsset>(mesh).unwrap();
    let shader_handle = live_loader.add_ref::<CookedPipeline>(shader).unwrap();
    let mut live_storage = Storage::default();
    wait_for_loads(
        &mut live_loader,
        &mut live_storage,
        &texture_handle,
        &mesh_handle,
        &shader_handle,
    );
    let baseline_updates = settle_loader(&mut live_loader, &mut live_storage);
    let baseline_commits = live_storage.commits.len();
    assert_eq!(baseline_updates, 3);
    assert_eq!(baseline_commits, 3);
    assert_loaded_game_assets(
        &live_storage,
        texture_handle.id(),
        mesh_handle.id(),
        shader_handle.id(),
    );

    let old_texture_hash = resolved_hash(process.coordinator().server().root(), &request, texture);
    let old_mesh_hash = resolved_hash(process.coordinator().server().root(), &request, mesh);
    let old_shader_hash = resolved_hash(process.coordinator().server().root(), &request, shader);
    let include_path = assets.join("shaders/common.inc");
    std::fs::write(&include_path, b"const uint VALUE = 2;\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let updated = std::fs::read(assets.join("game-assets.bundle"))
            .ok()
            .and_then(|bytes| distill_bundle::parse_bundle(&bytes).ok())
            .is_some_and(|reimported| {
                matches!(
                    &reimported.assets["shader"].data,
                    AuthoredValue::Object(fields)
                        if matches!(fields.get("value"), Some(AuthoredValue::Str(value)) if value.contains("VALUE = 2"))
                )
            });
        if updated {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "native watcher did not trigger shader-include reimport"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // The reimport rewrites the bundle file before the watcher's reconcile
    // publishes it; a snapshot taken in between resolves the old shader, as
    // it should.
    let deadline = Instant::now() + Duration::from_secs(10);
    while resolved_hash(process.coordinator().server().root(), &request, shader) == old_shader_hash
    {
        assert!(
            Instant::now() < deadline,
            "the reimported shader never resolved to new content"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let new_texture_hash = resolved_hash(process.coordinator().server().root(), &request, texture);
    let new_mesh_hash = resolved_hash(process.coordinator().server().root(), &request, mesh);
    assert_eq!(old_texture_hash, new_texture_hash);
    assert_eq!(old_mesh_hash, new_mesh_hash);
    let deadline = Instant::now() + Duration::from_secs(10);
    while live_storage.commits.len() == baseline_commits {
        live_loader.process(&mut live_storage).unwrap();
        if Instant::now() >= deadline {
            panic!(
                "live typed loader did not adopt the shader-only update: diagnostics={:?}",
                live_loader.take_diagnostics()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(live_storage.updates.len(), 4);
    assert_eq!(live_storage.commits.len(), 4);
    assert_eq!(live_storage.updates[3].0, shader_handle.id());
    assert_eq!(live_storage.commits[3].0, shader_handle.id());
    assert_ne!(live_storage.updates[3].0, texture_handle.id());
    assert_ne!(live_storage.updates[3].0, mesh_handle.id());
    assert_eq!(live_loader.status(&texture_handle), LoadStatus::Loaded);
    assert_eq!(live_loader.status(&mesh_handle), LoadStatus::Loaded);
    assert_eq!(live_loader.status(&shader_handle), LoadStatus::Loaded);
    let diagnostics = live_loader.take_diagnostics();
    assert!(!diagnostics.iter().any(|diagnostic| matches!(
        diagnostic,
        LoaderDiagnostic::ComponentPoisoned { members, .. }
            if members.contains(&texture) || members.contains(&mesh)
    )));

    // Named assets: the bundle holds two meshes, each loaded by its path and
    // the name its importer gave it; an unknown name resolves to nothing.
    let primary_named = live_loader
        .add_ref_named::<MeshAsset>("game-assets.bundle", "mesh")
        .unwrap();
    let reversed_named = live_loader
        .add_ref_named::<MeshAsset>("game-assets.bundle", "mesh_reversed")
        .unwrap();
    let unknown_named = live_loader
        .add_ref_named::<MeshAsset>("game-assets.bundle", "mesh_unknown")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while live_loader.status(&primary_named) != LoadStatus::Loaded
        || live_loader.status(&reversed_named) != LoadStatus::Loaded
    {
        live_loader.process(&mut live_storage).unwrap();
        assert!(
            Instant::now() < deadline,
            "named meshes did not load: diagnostics={:?}",
            live_loader.take_diagnostics()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    settle_loader(&mut live_loader, &mut live_storage);
    assert_eq!(live_loader.status(&unknown_named), LoadStatus::Unloaded);
    let indices_of = |handle: HandleId| {
        live_storage
            .values
            .iter()
            .filter(|((candidate, _), _)| *candidate == handle)
            .max_by_key(|((_, adoption), _)| *adoption)
            .and_then(|(_, value)| value.downcast_ref::<MeshAsset>())
            .map(|mesh| mesh.indices.clone())
            .unwrap_or_else(|| panic!("named mesh {handle:?} has no resident value"))
    };
    assert_eq!(indices_of(primary_named.id()), [0, 0, 1, 0, 2, 0]);
    assert_eq!(indices_of(reversed_named.id()), [0, 0, 2, 0, 1, 0]);

    // `distilld pack` runs as a client of this serving daemon, from a
    // PackDefinition authored in the asset root.
    let definition = AssetUuid([0xd1; 16]);
    std::fs::write(
        assets.join("game.pack.bundle"),
        pack_definition::pack_definition_bundle(
            BundleUuid([0xd0; 16]),
            definition,
            "dev",
            &[texture, mesh, shader],
            false,
        ),
    )
    .unwrap();
    let mut client_config = config.clone();
    client_config.daemon.address = process.rpc_address();
    let pack_dir = tempfile::tempdir().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let output = loop {
        match build_configured_pack(&client_config, definition, pack_dir.path()) {
            Err(PackCommandError::MissingDefinition(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            result => break result.unwrap(),
        }
    };
    assert_eq!(output.manifest.assets.len(), 3);
    assert!(
        output.manifest.paths.is_none(),
        "production UUID loading must not depend on the optional path table"
    );

    let pack_io = PackfileIO::mount_current(
        pack_dir.path(),
        &PackRuntimeTarget {
            target: "dev".into(),
            target_def_hash: target_hash,
        },
    )
    .unwrap();
    let mut pack_loader = Loader::new(pack_io);
    register_runtime(&mut pack_loader, target_hash, 2);
    let packed_texture = pack_loader.add_ref::<TextureAsset>(texture).unwrap();
    let packed_mesh = pack_loader.add_ref::<MeshAsset>(mesh).unwrap();
    let packed_shader = pack_loader.add_ref::<CookedPipeline>(shader).unwrap();
    let mut pack_storage = Storage::default();
    wait_for_loads(
        &mut pack_loader,
        &mut pack_storage,
        &packed_texture,
        &packed_mesh,
        &packed_shader,
    );
    assert_eq!(pack_storage.commits.len(), 3);
    assert_loaded_game_assets(
        &pack_storage,
        packed_texture.id(),
        packed_mesh.id(),
        packed_shader.id(),
    );
}

fn assert_loaded_game_assets(
    storage: &Storage,
    texture_handle: HandleId,
    mesh_handle: HandleId,
    shader_handle: HandleId,
) {
    let value_for = |handle| {
        storage
            .values
            .iter()
            .filter(|((candidate, _), _)| *candidate == handle)
            .max_by_key(|((_, adoption), _)| *adoption)
            .map(|(_, value)| value)
            .unwrap_or_else(|| panic!("handle {handle:?} has no resident value"))
    };
    let texture = value_for(texture_handle)
        .downcast_ref::<TextureAsset>()
        .expect("texture terminal value used the wrong native type");
    // The fixture cooks the pixel into a 2x2 texture with its 1x1 mip.
    assert_eq!((texture.width, texture.height, texture.depth), (2, 2, 1));
    assert_eq!((texture.array_layers, texture.mip_count), (1, 2));
    assert_eq!(texture.dimension, newgameplus_assets::TEXTURE_DIMENSION_2D);
    assert_eq!(texture.format, newgameplus_assets::FORMAT_R8G8B8A8_UNORM);
    assert_eq!(texture.data.as_bytes(), [255, 0, 0, 255].repeat(5));
    let layout = texture
        .layout(newgameplus_assets::format::block(texture.format).unwrap())
        .unwrap();
    assert_eq!(layout.total_len, texture.data.len() as u64);
    assert_eq!(
        (layout.subresources[1].mip, layout.subresources[1].offset),
        (1, 16)
    );

    let mesh = value_for(mesh_handle)
        .downcast_ref::<MeshAsset>()
        .expect("mesh terminal value used the wrong native type");
    assert_eq!(
        mesh.vertex_channels,
        newgameplus_assets::VERTEX_CHANNEL_POSITION
    );
    assert_eq!(mesh.index_stride, 2);
    assert_eq!(mesh.vertices.len(), 3 * 16);
    assert_eq!(mesh.indices, [0, 0, 1, 0, 2, 0]);

    let shader = value_for(shader_handle)
        .downcast_ref::<CookedPipeline>()
        .expect("shader terminal value used the wrong native type");
    let package: rafx_api::RafxPipelinePackage = bincode::deserialize(shader.cooked.as_bytes())
        .expect("shader cooker did not emit a real Rafx pipeline package");
    assert_eq!(package.shaders.len(), 1);
    let stage = package.shaders[0].shader_package();
    assert!(stage.vk.is_some(), "cooked package has no Vulkan shader");
    assert!(
        stage.vk_reflection.is_some(),
        "cooked package has no Vulkan reflection"
    );
    assert!(
        stage.metal.is_none(),
        "Vulkan-only cook unexpectedly packaged Metal"
    );
}

fn register_runtime<I: distill_loader::LoaderIO>(
    loader: &mut Loader<I>,
    target_hash: [u8; 32],
    epoch: u64,
) {
    loader
        .register_types(
            GameModuleEpoch(epoch),
            ModuleEpochToken::new(epoch),
            target_hash,
            &[
                TextureAsset::descriptor(),
                MeshAsset::descriptor(),
                CookedPipeline::descriptor(),
            ],
        )
        .unwrap();
}

fn wait_for_loads<I: distill_loader::LoaderIO>(
    loader: &mut Loader<I>,
    storage: &mut Storage,
    texture: &distill_loader::Handle<TextureAsset>,
    mesh: &distill_loader::Handle<MeshAsset>,
    shader: &distill_loader::Handle<CookedPipeline>,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        loader.process(storage).unwrap();
        if loader.status(texture) == LoadStatus::Loaded
            && loader.status(mesh) == LoadStatus::Loaded
            && loader.status(shader) == LoadStatus::Loaded
        {
            return;
        }
        if Instant::now() >= deadline {
            let statuses = [
                loader.status(texture),
                loader.status(mesh),
                loader.status(shader),
            ];
            panic!(
                "assets did not reach typed adoption: statuses={statuses:?}, target={:?}, diagnostics={:?}",
                loader.target_binding_state(),
                loader.take_diagnostics()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn settle_loader<I: distill_loader::LoaderIO>(
    loader: &mut Loader<I>,
    storage: &mut Storage,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut stable_since = Instant::now();
    let mut observed = storage.updates.len();
    loop {
        loader.process(storage).unwrap();
        if storage.updates.len() != observed {
            observed = storage.updates.len();
            stable_since = Instant::now();
        }
        if Instant::now().duration_since(stable_since) >= Duration::from_millis(100) {
            return observed;
        }
        assert!(
            Instant::now() < deadline,
            "live loader did not reach a quiescent baseline"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn resolved_hash(
    root: distill_rpc::Root,
    request: &ConnectRequest,
    asset: AssetUuid,
) -> ContentHash {
    let hub = match root.connect(request.clone()) {
        ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("resolve connection failed: {other:?}"),
    };
    // A snapshot races background publications (watched reimports), so a
    // build against a superseded version drifts; retry on a fresh snapshot.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = hub.snapshot().success().unwrap();
        match snapshot.resolve(asset).success().unwrap().value {
            ResolveResult::Built { content_hash } => return content_hash,
            ResolveResult::Drifted { .. } if Instant::now() < deadline => continue,
            other => panic!("asset did not build: {other:?}"),
        }
    }
}

fn import_assets(process: &DaemonProcess, config: &DaemonConfig, assets: &Path) -> [AssetUuid; 3] {
    // Through the RPC hub, as `distilld import` does.
    distill_daemon::bootstrap::import(
        config,
        process.rpc_address(),
        None,
        &ImportRequest {
            importer: "fixture-game-assets".into(),
            sources: vec![
                "pixel.ppm".into(),
                "triangle.obj".into(),
                "shaders/basic.comp".into(),
            ],
            dest: "game-assets.bundle".into(),
            settings: AuthoringValue {
                canonical_value: Arc::from(&b"{\"value\":0}"[..]),
                blobs: Vec::new(),
            },
            watch: true,
            root: "main".into(),
        },
        Duration::from_secs(30),
    )
    .unwrap();
    let bundle =
        distill_bundle::parse_bundle(&std::fs::read(assets.join("game-assets.bundle")).unwrap())
            .unwrap();
    [
        bundle.assets["texture"].uuid,
        bundle.assets["mesh"].uuid,
        bundle.assets["shader"].uuid,
    ]
}

fn build_pipeline_fixture() -> PathBuf {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap();
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target/game-assets-pipeline-fixture"));
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .current_dir(workspace)
        .env("CARGO_TARGET_DIR", &target_dir)
        .args([
            "build",
            "--offline",
            "-p",
            "distill-game-assets-pipeline-fixture",
        ])
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
            "distill_game_assets_pipeline_fixture.dll"
        } else if cfg!(target_os = "macos") {
            "libdistill_game_assets_pipeline_fixture.dylib"
        } else {
            "libdistill_game_assets_pipeline_fixture.so"
        })
}

fn fixture_source_identity(module: &Path) -> (String, String) {
    let temp = tempfile::tempdir().unwrap();
    let staged =
        ngp_module_host::stage_copy_to(module, &temp.path().join(module.file_name().unwrap()))
            .unwrap();
    // SAFETY: this exact test fixture was compiled immediately above from the
    // current workspace and is opened only to read its bounded identity export.
    let library = unsafe { ngp_module_host::HostedLibrary::open(staged) }.unwrap();
    // SAFETY: the fixture derives the shared source-identity export contract.
    let identity = unsafe { ngp_module_host::read_source_identity(&library) }.unwrap();
    library.close();
    (identity.crate_name, identity.source_hash)
}

fn fixture_schema(source_identity: (String, String)) -> Schema {
    let leaf = |id, kind, krate, name| TypeDef {
        id: SchemaTypeId(id),
        kind,
        path: type_path(krate, name),
        uuid: None,
        attrs: TypeAttrs::default(),
        fields: Vec::new(),
        generic_parameters: Vec::new(),
        generic_argument_ids: Vec::new(),
        has_default: true,
        generic_const_arguments: Vec::new(),
        has_explicit_discriminants: false,
    };
    let field = |name: &str, type_id: usize| Field {
        id: FieldIdentifier::Name(name.into()),
        type_id: SchemaTypeId(type_id),
        attrs: FieldAttrs::default(),
    };
    let mut types = vec![
        leaf(0, PrimitiveType::U8, "core", "u8"),
        TypeDef {
            id: SchemaTypeId(1),
            kind: PrimitiveType::Struct,
            path: type_path("game_assets_fixture", "FixtureSettings"),
            uuid: Some(SETTINGS_TYPE),
            attrs: TypeAttrs {
                build_only: true,
                ..TypeAttrs::default()
            },
            fields: vec![field("value", 0)],
            generic_parameters: Vec::new(),
            generic_argument_ids: Vec::new(),
            has_default: true,
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
        },
        leaf(2, PrimitiveType::String, "alloc", "String"),
        leaf(3, PrimitiveType::U32, "core", "u32"),
        TypeDef {
            id: SchemaTypeId(4),
            kind: PrimitiveType::Struct,
            path: TypePath {
                name: Some("Vec".into()),
                containing_type: None,
                modules: vec!["vec".into()],
                krate: "alloc".into(),
            },
            uuid: None,
            attrs: TypeAttrs::default(),
            fields: Vec::new(),
            generic_parameters: Vec::new(),
            generic_argument_ids: vec![SchemaTypeId(0)],
            has_default: true,
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
        },
        leaf(5, PrimitiveType::Struct, "distill_asset", "Blob"),
    ];
    for (index, (name, uuid)) in [
        ("TextureSource", TEXTURE_SOURCE_TYPE),
        ("MeshSource", MESH_SOURCE_TYPE),
        ("ShaderSource", SHADER_SOURCE_TYPE),
    ]
    .into_iter()
    .enumerate()
    {
        types.push(TypeDef {
            id: SchemaTypeId(index + 6),
            kind: PrimitiveType::Struct,
            path: type_path("game_assets_fixture", name),
            uuid: Some(uuid),
            attrs: TypeAttrs::default(),
            fields: vec![field("value", 2)],
            generic_parameters: Vec::new(),
            generic_argument_ids: Vec::new(),
            has_default: true,
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
        });
    }
    types.extend([
        TypeDef {
            id: SchemaTypeId(9),
            kind: PrimitiveType::Struct,
            path: type_path("newgameplus_assets", "TextureAsset"),
            uuid: Some(TextureAsset::TYPE_UUID),
            attrs: TypeAttrs::default(),
            fields: vec![
                field("width", 3),
                field("height", 3),
                field("depth", 3),
                field("array_layers", 3),
                field("mip_count", 3),
                field("dimension", 0),
                field("format", 0),
                Field {
                    attrs: FieldAttrs {
                        blob: true,
                        ..FieldAttrs::default()
                    },
                    ..field("data", 5)
                },
            ],
            generic_parameters: Vec::new(),
            generic_argument_ids: Vec::new(),
            has_default: true,
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
        },
        TypeDef {
            id: SchemaTypeId(10),
            kind: PrimitiveType::Struct,
            path: type_path("newgameplus_assets", "MeshAsset"),
            uuid: Some(MeshAsset::TYPE_UUID),
            attrs: TypeAttrs::default(),
            fields: vec![
                field("vertices", 4),
                field("indices", 4),
                field("vertex_channels", 3),
                field("index_stride", 3),
            ],
            generic_parameters: Vec::new(),
            generic_argument_ids: Vec::new(),
            has_default: true,
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
        },
        TypeDef {
            id: SchemaTypeId(11),
            kind: PrimitiveType::Struct,
            path: type_path("newgameplus_assets", "CookedPipeline"),
            uuid: Some(CookedPipeline::TYPE_UUID),
            attrs: TypeAttrs::default(),
            fields: vec![Field {
                attrs: FieldAttrs {
                    blob: true,
                    ..FieldAttrs::default()
                },
                ..field("cooked", 5)
            }],
            generic_parameters: Vec::new(),
            generic_argument_ids: Vec::new(),
            has_default: false,
            generic_const_arguments: Vec::new(),
            has_explicit_discriminants: false,
        },
    ]);
    let string_layout = TypeLayout {
        size: Some(size_of::<String>() as u64),
        align: Some(align_of::<String>() as u64),
        layout_complete: true,
        tag_encoding: None,
        fields: Vec::new(),
    };
    let source_layout = TypeLayout {
        size: Some(size_of::<SourceValue>() as u64),
        align: Some(align_of::<SourceValue>() as u64),
        layout_complete: true,
        tag_encoding: None,
        fields: vec![FieldLayout {
            offset: Some(std::mem::offset_of!(SourceValue, value) as u64),
            field_size: Some(size_of::<String>() as u64),
        }],
    };
    let terminal_layout = |size, align, fields| TypeLayout {
        size: Some(size),
        align: Some(align),
        layout_complete: true,
        tag_encoding: None,
        fields,
    };
    Schema {
        source_hashes: BTreeMap::from([source_identity]),
        type_ops_hash: String::new(),
        layout_hashes: Default::default(),
        rustc_version: String::new(),
        types,
        layouts: vec![SchemaLayouts {
            identity: host_layout_identity(),
            layouts: vec![
                scalar_layout(1, 1),
                terminal_layout(
                    size_of::<FixtureSettings>() as u64,
                    align_of::<FixtureSettings>() as u64,
                    vec![FieldLayout {
                        offset: Some(std::mem::offset_of!(FixtureSettings, value) as u64),
                        field_size: Some(1),
                    }],
                ),
                string_layout,
                scalar_layout(4, 4),
                terminal_layout(
                    size_of::<Vec<u8>>() as u64,
                    align_of::<Vec<u8>>() as u64,
                    Vec::new(),
                ),
                terminal_layout(
                    size_of::<distill_asset::Blob>() as u64,
                    align_of::<distill_asset::Blob>() as u64,
                    Vec::new(),
                ),
                source_layout.clone(),
                source_layout.clone(),
                source_layout,
                terminal_layout(
                    size_of::<TextureAsset>() as u64,
                    align_of::<TextureAsset>() as u64,
                    vec![
                        layout_field::<TextureAsset, u32>(std::mem::offset_of!(
                            TextureAsset,
                            width
                        )),
                        layout_field::<TextureAsset, u32>(std::mem::offset_of!(
                            TextureAsset,
                            height
                        )),
                        layout_field::<TextureAsset, u32>(std::mem::offset_of!(
                            TextureAsset,
                            depth
                        )),
                        layout_field::<TextureAsset, u32>(std::mem::offset_of!(
                            TextureAsset,
                            array_layers
                        )),
                        layout_field::<TextureAsset, u32>(std::mem::offset_of!(
                            TextureAsset,
                            mip_count
                        )),
                        layout_field::<TextureAsset, u8>(std::mem::offset_of!(
                            TextureAsset,
                            dimension
                        )),
                        layout_field::<TextureAsset, u8>(std::mem::offset_of!(
                            TextureAsset,
                            format
                        )),
                        layout_field::<TextureAsset, distill_asset::Blob>(std::mem::offset_of!(
                            TextureAsset,
                            data
                        )),
                    ],
                ),
                terminal_layout(
                    size_of::<MeshAsset>() as u64,
                    align_of::<MeshAsset>() as u64,
                    vec![
                        layout_field::<MeshAsset, Vec<u8>>(std::mem::offset_of!(
                            MeshAsset, vertices
                        )),
                        layout_field::<MeshAsset, Vec<u8>>(std::mem::offset_of!(
                            MeshAsset, indices
                        )),
                        layout_field::<MeshAsset, u32>(std::mem::offset_of!(
                            MeshAsset,
                            vertex_channels
                        )),
                        layout_field::<MeshAsset, u32>(std::mem::offset_of!(
                            MeshAsset,
                            index_stride
                        )),
                    ],
                ),
                terminal_layout(
                    size_of::<CookedPipeline>() as u64,
                    align_of::<CookedPipeline>() as u64,
                    vec![layout_field::<CookedPipeline, distill_asset::Blob>(
                        std::mem::offset_of!(CookedPipeline, cooked),
                    )],
                ),
            ],
        }],
    }
}

fn layout_field<T, F>(offset: usize) -> FieldLayout {
    let _ = std::marker::PhantomData::<T>;
    FieldLayout {
        offset: Some(offset as u64),
        field_size: Some(size_of::<F>() as u64),
    }
}

fn scalar_layout(size: u64, align: u64) -> TypeLayout {
    TypeLayout {
        size: Some(size),
        align: Some(align),
        layout_complete: true,
        tag_encoding: None,
        fields: Vec::new(),
    }
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
        rustc: "rustc game-assets-e2e".into(),
        algorithm_version: 1,
    }
}

fn write_config(
    temp: &tempfile::TempDir,
    module: &Path,
    identity: &LayoutIdentity,
) -> DaemonConfig {
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
state_path = '{}'
[assets]
roots = {{ main = '{}' }}
schema_path = '{}'
[modules]
pipeline_dylib = '{}'
[targets.dev]
os = "{}"
arch = "{}"
apis = ["vulkan"]
optimize = false
[codegen]
rs_mod_path = '{}'
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
    assert_eq!(&host_layout_identity(), identity);
    let path = temp.path().join("distill.toml");
    std::fs::write(&path, config).unwrap();
    DaemonConfig::load(path).unwrap()
}
