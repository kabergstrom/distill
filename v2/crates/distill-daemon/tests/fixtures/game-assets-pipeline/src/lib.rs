use std::collections::{BTreeMap, BTreeSet};

use distill_build::import::ImportOutput;
use distill_build::outputs::OutputDecls;
use distill_build::pipeline::TargetSelector;
use distill_core::id::TypeUuid;
use distill_daemon::callbacks::{
    ImporterDescriptor, PipelineProcessContext, PipelineProcessor, ProcessorDescriptor,
    ProcessorError, ProcessorProduct, ProcessorProducts,
};
use distill_daemon::epoch::{CandidateRegistrationArena, ModuleCallError, TargetDefinition};
use distill_daemon::importer::{AuthoringImportContext, AuthoringImporter, AuthoringImporterError};
use distill_json::AuthoredValue;
use distill_schema::ngp_schema::{LogicalSchema, PrimitiveKind, SchemaNode};

pub const SETTINGS_TYPE: TypeUuid = TypeUuid([0x90; 16]);
pub const TEXTURE_SOURCE_TYPE: TypeUuid = TypeUuid([0x91; 16]);
pub const MESH_SOURCE_TYPE: TypeUuid = TypeUuid([0x92; 16]);
pub const SHADER_SOURCE_TYPE: TypeUuid = TypeUuid([0x93; 16]);
pub const COOKED_ASSET_TYPE: TypeUuid = TypeUuid([0x94; 16]);

#[derive(newgameplus_api_macros::NgpSourceIdentity)]
pub struct SourceIdentity;

#[derive(Clone, Copy)]
enum Kind {
    Texture,
    Mesh,
    Shader,
}

impl Kind {
    fn processor_id(self) -> &'static str {
        match self {
            Self::Texture => "fixture-texture-cook",
            Self::Mesh => "fixture-mesh-cook",
            Self::Shader => "fixture-shader-cook",
        }
    }

    fn source_type(self) -> TypeUuid {
        match self {
            Self::Texture => TEXTURE_SOURCE_TYPE,
            Self::Mesh => MESH_SOURCE_TYPE,
            Self::Shader => SHADER_SOURCE_TYPE,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Texture => "texture",
            Self::Mesh => "mesh",
            Self::Shader => "shader",
        }
    }
}

struct GameAssetsImporter;

impl AuthoringImporter for GameAssetsImporter {
    fn id(&self) -> &str {
        "fixture-game-assets"
    }

    fn version(&self) -> u32 {
        1
    }

    fn settings_type_uuid(&self) -> TypeUuid {
        SETTINGS_TYPE
    }

    fn settings_schema(&self) -> &LogicalSchema {
        static SETTINGS: std::sync::OnceLock<LogicalSchema> = std::sync::OnceLock::new();
        SETTINGS.get_or_init(|| LogicalSchema {
            root: settings_schema(),
        })
    }

    fn default_settings(&self) -> AuthoredValue {
        settings_value()
    }

    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        _settings: &AuthoredValue,
    ) -> Result<ImportOutput, AuthoringImporterError> {
        let texture = source_with_suffix(context, "pixel.ppm")?;
        let mesh = source_with_suffix(context, "triangle.obj")?;
        let shader = source_with_suffix(context, "shaders/basic.glsl")?;
        let texture = parse_ppm(&context.read(&texture)?)?;
        let mesh = parse_triangle(&context.read(&mesh)?)?;
        let shader_bytes = context.read(&shader)?;
        let shader = parse_shader(context, &shader_bytes)?;
        let mut output = ImportOutput::new();
        for (id, type_uuid, value) in [
            ("texture", TEXTURE_SOURCE_TYPE, texture),
            ("mesh", MESH_SOURCE_TYPE, mesh),
            ("shader", SHADER_SOURCE_TYPE, shader),
        ] {
            output
                .entry(
                    id,
                    type_uuid,
                    AuthoredValue::Object(BTreeMap::from([(
                        "value".to_owned(),
                        AuthoredValue::Str(value),
                    )])),
                )
                .map_err(|error| AuthoringImporterError::rejected(1, format!("{error:?}")))?;
        }
        output
            .primary("texture")
            .map_err(|error| AuthoringImporterError::rejected(2, format!("{error:?}")))?;
        Ok(output)
    }
}

fn source_with_suffix(
    context: &dyn AuthoringImportContext,
    suffix: &str,
) -> Result<String, AuthoringImporterError> {
    context
        .sources()
        .iter()
        .find(|source| source.path.ends_with(suffix))
        .map(|source| source.path.clone())
        .ok_or_else(|| AuthoringImporterError::rejected(3, format!("missing source {suffix}")))
}

fn parse_ppm(bytes: &[u8]) -> Result<String, AuthoringImporterError> {
    const HEADER: &[u8] = b"P6\n1 1\n255\n";
    if bytes.len() != HEADER.len() + 3 || !bytes.starts_with(HEADER) {
        return Err(AuthoringImporterError::rejected(
            10,
            "fixture texture must be a 1x1 binary PPM",
        ));
    }
    Ok(format!(
        "1x1:{:02x}{:02x}{:02x}",
        bytes[HEADER.len()],
        bytes[HEADER.len() + 1],
        bytes[HEADER.len() + 2]
    ))
}

fn parse_triangle(bytes: &[u8]) -> Result<String, AuthoringImporterError> {
    let source = std::str::from_utf8(bytes)
        .map_err(|error| AuthoringImporterError::rejected(20, error.to_string()))?;
    let vertices = source.lines().filter(|line| line.starts_with("v ")).count();
    let faces = source.lines().filter(|line| line.starts_with("f ")).count();
    if vertices != 3 || faces != 1 {
        return Err(AuthoringImporterError::rejected(
            21,
            "fixture mesh must contain three OBJ vertices and one face",
        ));
    }
    Ok(format!("vertices={vertices};faces={faces}"))
}

fn parse_shader(
    context: &mut dyn AuthoringImportContext,
    bytes: &[u8],
) -> Result<String, AuthoringImporterError> {
    let source = std::str::from_utf8(bytes)
        .map_err(|error| AuthoringImporterError::rejected(30, error.to_string()))?;
    let include_path = source
        .lines()
        .find_map(|line| line.strip_prefix("#include \"")?.strip_suffix('"'))
        .ok_or_else(|| AuthoringImporterError::rejected(31, "shader include is missing"))?;
    let include = context.read(include_path)?;
    let include = std::str::from_utf8(&include)
        .map_err(|error| AuthoringImporterError::rejected(32, error.to_string()))?;
    Ok(format!("{}|{}", source.trim(), include.trim()))
}

struct FixtureCook(Kind);

impl PipelineProcessor for FixtureCook {
    fn process(
        &self,
        input: AuthoredValue,
        _context: &mut dyn PipelineProcessContext,
    ) -> Result<ProcessorProducts, ProcessorError> {
        let AuthoredValue::Object(mut fields) = input else {
            return Err(ProcessorError::new(1, "fixture source must be an object"));
        };
        let Some(AuthoredValue::Str(value)) = fields.remove("value") else {
            return Err(ProcessorError::new(
                2,
                "fixture source value must be a string",
            ));
        };
        Ok(ProcessorProducts {
            primary: Some(ProcessorProduct::new(
                COOKED_ASSET_TYPE,
                AuthoredValue::Object(BTreeMap::from([(
                    "value".to_owned(),
                    AuthoredValue::Str(format!("cooked:{}:{value}", self.0.label())),
                )])),
            )),
            ..ProcessorProducts::default()
        })
    }
}

fn register(
    targets: &[TargetDefinition],
    arena: &mut CandidateRegistrationArena,
) -> Result<BTreeSet<String>, ModuleCallError> {
    arena
        .register_importer(
            ImporterDescriptor {
                id: "fixture-game-assets".to_owned(),
                version: 1,
                settings_type_uuid: SETTINGS_TYPE,
                settings_schema: LogicalSchema {
                    root: settings_schema(),
                },
                default_settings: settings_value(),
            },
            GameAssetsImporter,
        )
        .into_result()?;
    for kind in [Kind::Texture, Kind::Mesh, Kind::Shader] {
        arena
            .register_processor(
                ProcessorDescriptor {
                    id: kind.processor_id().to_owned(),
                    version: 1,
                    input: kind.source_type(),
                    selector: TargetSelector::new(None, None)
                        .map_err(|error| ModuleCallError::new(format!("{error:?}")))?,
                    outputs: OutputDecls::new(COOKED_ASSET_TYPE, Vec::new())
                        .map_err(|error| ModuleCallError::new(format!("{error:?}")))?,
                },
                FixtureCook(kind),
            )
            .into_result()?;
    }
    Ok(targets.iter().map(|target| target.name.clone()).collect())
}

fn settings_schema() -> SchemaNode {
    SchemaNode::Struct {
        rev: 0,
        fields: vec![(
            "value".to_owned(),
            0,
            SchemaNode::Primitive(PrimitiveKind::U8),
        )],
    }
}

fn settings_value() -> AuthoredValue {
    AuthoredValue::Object(BTreeMap::from([(
        "value".to_owned(),
        AuthoredValue::UInt(0),
    )]))
}

fn unload() -> Result<(), ModuleCallError> {
    Ok(())
}

distill_daemon::export_pipeline_module_v2!(register = register, unload = unload);
