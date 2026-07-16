use std::collections::{BTreeMap, BTreeSet};

use distill_asset::AssetType;
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

    fn terminal_type(self) -> TypeUuid {
        match self {
            Self::Texture => newgameplus_assets::TextureAsset::TYPE_UUID,
            Self::Mesh => newgameplus_assets::MeshAsset::TYPE_UUID,
            Self::Shader => newgameplus_assets::CookedPipeline::TYPE_UUID,
        }
    }
}

fn cook_texture(value: &str) -> Result<AuthoredValue, ProcessorError> {
    let Some(hex) = value.strip_prefix("1x1:") else {
        return Err(ProcessorError::new(10, "invalid imported PPM value"));
    };
    if hex.len() != 6 {
        return Err(ProcessorError::new(11, "invalid imported PPM pixel"));
    }
    let mut data = Vec::with_capacity(4);
    for offset in [0, 2, 4] {
        data.push(
            u8::from_str_radix(&hex[offset..offset + 2], 16)
                .map_err(|error| ProcessorError::new(12, error.to_string()))?,
        );
    }
    data.push(255);
    Ok(AuthoredValue::Object(BTreeMap::from([
        ("width".to_owned(), AuthoredValue::UInt(1)),
        ("height".to_owned(), AuthoredValue::UInt(1)),
        (
            "format".to_owned(),
            AuthoredValue::UInt(u128::from(
                newgameplus_assets::FORMAT_R8G8B8A8_UNORM,
            )),
        ),
        ("data".to_owned(), bytes_array(&data)),
    ])))
}

fn cook_mesh(value: &str) -> Result<AuthoredValue, ProcessorError> {
    let mut positions = Vec::<[f32; 3]>::new();
    let mut indices = Vec::<u16>::new();
    for line in value.lines() {
        let mut words = line.split_whitespace();
        match words.next() {
            Some("v") => {
                let mut position = [0.0; 3];
                for component in &mut position {
                    *component = words
                        .next()
                        .ok_or_else(|| ProcessorError::new(20, "OBJ vertex is incomplete"))?
                        .parse()
                        .map_err(|error: std::num::ParseFloatError| {
                            ProcessorError::new(21, error.to_string())
                        })?;
                }
                if words.next().is_some() {
                    return Err(ProcessorError::new(22, "OBJ vertex has extra fields"));
                }
                positions.push(position);
            }
            Some("f") => {
                let face = words
                    .map(|word| {
                        let position = word.split('/').next().unwrap_or_default();
                        let one_based: usize = position.parse().map_err(
                            |error: std::num::ParseIntError| {
                                ProcessorError::new(23, error.to_string())
                            },
                        )?;
                        let zero_based = one_based
                            .checked_sub(1)
                            .ok_or_else(|| ProcessorError::new(24, "OBJ indices are one-based"))?;
                        u16::try_from(zero_based)
                            .map_err(|_| ProcessorError::new(25, "OBJ index exceeds u16"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if face.len() != 3 {
                    return Err(ProcessorError::new(26, "fixture OBJ faces must be triangles"));
                }
                indices.extend(face);
            }
            Some(_) | None => {}
        }
    }
    if positions.is_empty()
        || indices.is_empty()
        || indices
            .iter()
            .any(|index| usize::from(*index) >= positions.len())
    {
        return Err(ProcessorError::new(27, "OBJ geometry is empty or out of range"));
    }
    let mut vertices = Vec::with_capacity(positions.len() * 16);
    for position in positions {
        for component in position {
            vertices.extend_from_slice(&component.to_le_bytes());
        }
        vertices.extend_from_slice(&0_u32.to_le_bytes());
    }
    let indices = indices
        .into_iter()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    Ok(AuthoredValue::Object(BTreeMap::from([
        ("vertices".to_owned(), bytes_array(&vertices)),
        ("indices".to_owned(), bytes_array(&indices)),
        (
            "vertex_channels".to_owned(),
            AuthoredValue::UInt(u128::from(
                newgameplus_assets::VERTEX_CHANNEL_POSITION,
            )),
        ),
        ("index_stride".to_owned(), AuthoredValue::UInt(2)),
    ])))
}

fn cook_shader(value: &str) -> Result<AuthoredValue, ProcessorError> {
    let cooked = rafx_shader_processor::compile_vulkan_pipeline(
        &[rafx_shader_processor::VulkanShaderStageSource {
            virtual_path: "assets/basic.comp",
            source: value,
        }],
        false,
    )
    .map_err(|error| ProcessorError::new(30, error.to_string()))?;
    Ok(AuthoredValue::Object(BTreeMap::from([(
        "cooked".to_owned(),
        AuthoredValue::Blob(cooked),
    )])))
}

fn bytes_array(bytes: &[u8]) -> AuthoredValue {
    AuthoredValue::Array(
        bytes
            .iter()
            .map(|byte| AuthoredValue::UInt(u128::from(*byte)))
            .collect(),
    )
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
        let shader = source_with_suffix(context, "shaders/basic.comp")?;
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
    Ok(source.to_owned())
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
    Ok(source.replace(
        &format!("#include \"{include_path}\""),
        include.trim(),
    ))
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
        let value = match self.0 {
            Kind::Texture => cook_texture(&value)?,
            Kind::Mesh => cook_mesh(&value)?,
            Kind::Shader => cook_shader(&value)?,
        };
        Ok(ProcessorProducts {
            primary: Some(ProcessorProduct::new(self.0.terminal_type(), value)),
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
                    outputs: OutputDecls::new(kind.terminal_type(), Vec::new())
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
