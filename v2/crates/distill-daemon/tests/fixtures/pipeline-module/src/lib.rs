//! The pipeline module the daemon tests load: it serves every target and
//! registers one processor, [`REFLECT`], which cooks a [`PARENT_TYPE`]
//! asset to a [`COOKED_TYPE`] primary and a declared [`REFLECTION`] output
//! of [`REFLECTION_TYPE`]: a derived child asset; four importers,
//! [`BYTE_IMPORTER`], [`CHAIN_IMPORTER`], [`SETTINGS_IMPORTER`] and
//! [`REQUIRE_IMPORTER`],
//! producing [`VALUE_TYPE`] from settings of [`SETTINGS_TYPE`]; and
//! [`FLOAT_IMPORTER`], producing [`VALUE_TYPE`] from settings of
//! [`FLOAT_SETTINGS_TYPE`]; and [`OPTIONAL_IMPORTER`], producing
//! [`VALUE_TYPE`] from settings of [`OPTIONAL_SETTINGS_TYPE`].

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::TypeUuid;
use distill_pipeline_api::callbacks::{
    AuthoredDescriptor,
    ImporterDescriptor, PipelineImporter, PipelineProcessContext, PipelineProcessor,
    ProcessorDescriptor, ProcessorError, ProcessorProduct, ProcessorProducts,
};
use distill_pipeline_api::import::{
    DefaultImportRule, DirectoryGrouping, ImportOutput, ImportRuleId,
};
use distill_pipeline_api::importer::{AuthoringImportContext, AuthoringImporterError};
use distill_pipeline_api::outputs::OutputDecls;
use distill_pipeline_api::query::FileQuery;
use distill_pipeline_api::registration::{ModuleCallError, RegistrationArena, TargetDefinition};
use distill_pipeline_api::target::TargetSelector;
use distill_schema::ngp_schema::{LogicalSchema, PrimitiveKind, SchemaNode, Value};

/// The processor's input: a struct of one `u8` field `value`.
pub const PARENT_TYPE: TypeUuid = TypeUuid([0xa1; 16]);
/// The processor's primary output, shaped as its input.
pub const COOKED_TYPE: TypeUuid = TypeUuid([0xa2; 16]);
/// The type of the processor's declared output [`REFLECTION`].
pub const REFLECTION_TYPE: TypeUuid = TypeUuid([0xa3; 16]);
/// The processor's id.
pub const REFLECT: &str = "fixture-reflect";
/// The output key of the processor's declared output.
pub const REFLECTION: &str = "reflection";

/// The importers' settings type, a project type of the daemon tests'
/// configuration: `{ add: [u8; 2], scale: { by: u8 } }`. An importer's
/// value `n` imports as `(n + add[0] + add[1]) * scale.by`.
pub const SETTINGS_TYPE: TypeUuid = TypeUuid([0xa6; 16]);
/// [`FLOAT_IMPORTER`]'s settings type, a project type of the daemon tests'
/// configuration: `{ rate: f32 }`.
pub const FLOAT_SETTINGS_TYPE: TypeUuid = TypeUuid([0xa7; 16]);
/// [`OPTIONAL_IMPORTER`]'s settings type, a project type of the daemon
/// tests' configuration: `{ skeleton: Option<SkeletonRef> }`, where
/// `SkeletonRef` is `{ path: String, name: Option<String> }` (the shape of
/// `GltfImportSettings.animation.skeleton` in newgameplus).
pub const OPTIONAL_SETTINGS_TYPE: TypeUuid = TypeUuid([0xa8; 16]);
/// The importers' output type, a project type of the daemon tests'
/// configuration no processor cooks: a struct of one `u8` field `value`.
pub const VALUE_TYPE: TypeUuid = TypeUuid([0xa5; 16]);
/// Imports the number its one text source holds, under its settings, taking
/// that many times 10 ms. A source `"{value} {dir}"` is gated: the run
/// writes `dir/started` and waits for `dir/release` (at most a minute).
pub const BYTE_IMPORTER: &str = "byte-importer";
/// Imports one more than the largest value among its sources that exist,
/// under its settings: a text source's number, or a bundle source's `asset`
/// entry, so its sources may be other imports' outputs.
pub const CHAIN_IMPORTER: &str = "chain-importer";
/// Imports the sum of `scale.by` over the settings of its sources' imports
/// (`read_settings`; a source no import names adds 0), under its settings.
pub const SETTINGS_IMPORTER: &str = "settings-importer";
/// Each text source names another path; imports the sum of `scale.by`
/// over the settings of those paths' imports (`read_settings`), under its
/// settings. A named path no import names is rejection 9.
pub const REQUIRE_IMPORTER: &str = "require-importer";
/// Imports the number its one text source holds, whatever its settings
/// ([`FLOAT_SETTINGS_TYPE`], defaults [`float_default_settings`]).
pub const FLOAT_IMPORTER: &str = "float-importer";
/// Imports the number its one text source holds, whatever its settings
/// ([`OPTIONAL_SETTINGS_TYPE`], defaults [`optional_default_settings`]).
pub const OPTIONAL_IMPORTER: &str = "optional-importer";
/// The default import rule (§8 "Default imports"): every `*.num` file,
/// per file, with [`BYTE_IMPORTER`] at its default settings, to
/// `{name}.bundle`.
pub const DEFAULT_RULE: [u8; 16] = [0xd5; 16];
/// The extension [`DEFAULT_RULE`] matches.
pub const DEFAULT_EXTENSION: &str = "num";

/// The extension [`PARENT_TYPE`] is authored under (§17 "Creating"); its
/// default is `value(7)`.
pub const PARENT_EXTENSION: &str = "parent";
/// The extension [`VALUE_TYPE`] is authored under, declared with a logical
/// hash that is not the type's: its default is a schema mismatch.
pub const STALE_EXTENSION: &str = "stale";

/// The logical schema of [`PARENT_TYPE`] and [`VALUE_TYPE`]: `{ value: u8 }`.
pub fn value_schema() -> SchemaNode {
    SchemaNode::Struct {
        rev: 0,
        fields: vec![("value".to_owned(), 0, SchemaNode::Primitive(PrimitiveKind::U8))],
    }
}

/// A [`VALUE_TYPE`] value (`value` truncated to its `u8`).
pub fn value(value: u128) -> Value {
    Value::Struct(Box::new([Value::U8(value as u8)]))
}

/// The `u8` of a [`VALUE_TYPE`] value.
pub fn value_of(value: &Value) -> Option<u128> {
    match value {
        Value::Struct(fields) => match fields.as_ref() {
            [Value::U8(value)] => Some(u128::from(*value)),
            _ => None,
        },
        _ => None,
    }
}

/// The [`SETTINGS_TYPE`] schema: `add` a `[u8; 2]`, `scale` a `{ by: u8 }`.
pub fn settings_schema() -> SchemaNode {
    SchemaNode::Struct {
        rev: 0,
        fields: vec![
            (
                "add".to_owned(),
                0,
                SchemaNode::Array {
                    len: 2,
                    elem: Box::new(SchemaNode::Primitive(PrimitiveKind::U8)),
                },
            ),
            (
                "scale".to_owned(),
                0,
                SchemaNode::Struct {
                    rev: 0,
                    fields: vec![(
                        "by".to_owned(),
                        0,
                        SchemaNode::Primitive(PrimitiveKind::U8),
                    )],
                },
            ),
        ],
    }
}

/// The [`FLOAT_SETTINGS_TYPE`] schema: `rate` an `f32`.
pub fn float_settings_schema() -> SchemaNode {
    SchemaNode::Struct {
        rev: 0,
        fields: vec![(
            "rate".to_owned(),
            0,
            SchemaNode::Primitive(PrimitiveKind::F32),
        )],
    }
}

/// A [`SETTINGS_TYPE`] value (fields in node order: `add`, `scale`).
pub fn settings(add: [u8; 2], by: u8) -> Value {
    Value::Struct(Box::new([
        Value::Seq(add.into_iter().map(Value::U8).collect()),
        Value::Struct(Box::new([Value::U8(by)])),
    ]))
}

/// The importers' default settings, under which a value imports as itself.
pub fn default_settings() -> Value {
    settings([0, 0], 1)
}

/// A [`FLOAT_SETTINGS_TYPE`] value.
pub fn float_settings(rate: f32) -> Value {
    Value::Struct(Box::new([Value::F32(rate)]))
}

/// [`FLOAT_IMPORTER`]'s default settings: `rate` 30.0, an integral float.
pub fn float_default_settings() -> Value {
    float_settings(30.0)
}

/// [`OPTIONAL_IMPORTER`]'s default settings: `skeleton` `None`.
pub fn optional_default_settings() -> Value {
    Value::Struct(Box::new([Value::Option(None)]))
}

/// `scale.by` of a [`SETTINGS_TYPE`]-shaped value of `schema`.
fn scale_by(schema: &SchemaNode, value: &Value) -> Option<u128> {
    let SchemaNode::Struct { fields, .. } = schema else {
        return None;
    };
    let (_, _, scale_node) = fields.iter().find(|(name, _, _)| name == "scale")?;
    value.field(schema, "scale")?.field(scale_node, "by")?.as_u128()
}

/// `value` under `settings` (see [`SETTINGS_TYPE`]).
fn apply(settings: &Value, value: u128) -> Result<u128, AuthoringImporterError> {
    let malformed = || AuthoringImporterError::rejected(7, format!("settings {settings:?}"));
    let Value::Struct(fields) = settings else {
        return Err(malformed());
    };
    let [Value::Seq(add), Value::Struct(scale)] = fields.as_ref() else {
        return Err(malformed());
    };
    let [Value::U8(by)] = scale.as_ref() else {
        return Err(malformed());
    };
    let mut sum = value;
    for item in add {
        let Value::U8(item) = item else {
            return Err(malformed());
        };
        sum += u128::from(*item);
    }
    Ok(sum * u128::from(*by))
}

/// One `asset` entry holding `value`.
fn output(value: u128) -> Result<ImportOutput, AuthoringImporterError> {
    let mut output = ImportOutput::new();
    output
        .entry("asset", VALUE_TYPE, self::value(value))
        .map_err(|error| AuthoringImporterError::rejected(4, format!("{error:?}")))?;
    Ok(output)
}

struct ByteImporter;

impl PipelineImporter for ByteImporter {
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &Value,
    ) -> Result<ImportOutput, AuthoringImporterError> {
        let source = context
            .sources()
            .first()
            .ok_or_else(|| AuthoringImporterError::rejected(1, "one source is required"))?
            .path
            .clone();
        let bytes = context.read(&source)?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|error| AuthoringImporterError::rejected(2, error.to_string()))?;
        let (value, gate) = match text.split_once(' ') {
            Some((value, gate)) => (value, Some(std::path::Path::new(gate))),
            None => (text, None),
        };
        let value = value
            .parse::<u8>()
            .map_err(|error| AuthoringImporterError::rejected(3, error.to_string()))?;
        std::thread::sleep(std::time::Duration::from_millis(u64::from(value) * 10));
        if let Some(gate) = gate {
            let _ = std::fs::write(gate.join("started"), b"");
            for _ in 0..12_000 {
                if gate.join("release").exists() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
        output(apply(settings, value.into())?)
    }
}

struct ChainImporter;

impl PipelineImporter for ChainImporter {
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &Value,
    ) -> Result<ImportOutput, AuthoringImporterError> {
        let mut value = 0;
        for source in context.sources().to_vec() {
            if !context.probe(&source.path)? {
                continue;
            }
            let bytes = context.read(&source.path)?;
            let source_value = if source.path.ends_with(".bundle") {
                let bundle = distill_bundle::parse_bundle(&bytes)
                    .map_err(|error| AuthoringImporterError::rejected(5, format!("{error:?}")))?;
                let data = &bundle.assets["asset"].data;
                value_of(data)
                    .ok_or_else(|| AuthoringImporterError::rejected(6, format!("{data:?}")))?
            } else {
                std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|text| text.parse::<u128>().ok())
                    .ok_or_else(|| AuthoringImporterError::rejected(3, "not a number"))?
            };
            value = value.max(source_value);
        }
        output(apply(settings, value + 1)?)
    }
}

struct SettingsImporter;

impl PipelineImporter for SettingsImporter {
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &Value,
    ) -> Result<ImportOutput, AuthoringImporterError> {
        let mut value = 0;
        for source in context.sources().to_vec() {
            let Some((schema, imported)) = context.read_settings(&source.path)? else {
                continue;
            };
            let by = scale_by(&schema.root, &imported);
            value += by
                .ok_or_else(|| AuthoringImporterError::rejected(8, format!("{imported:?}")))?;
        }
        output(apply(settings, value)?)
    }
}

struct RequireImporter;

impl PipelineImporter for RequireImporter {
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &Value,
    ) -> Result<ImportOutput, AuthoringImporterError> {
        let mut value = 0;
        for source in context.sources().to_vec() {
            let bytes = context.read(&source.path)?;
            let named = String::from_utf8(bytes)
                .map_err(|_| AuthoringImporterError::rejected(3, "not text"))?;
            let named = named.trim();
            let (schema, imported) = context.read_settings(named)?.ok_or_else(|| {
                AuthoringImporterError::rejected(9, format!("{named} has no import"))
            })?;
            let by = scale_by(&schema.root, &imported);
            value += by
                .ok_or_else(|| AuthoringImporterError::rejected(8, format!("{imported:?}")))?;
        }
        output(apply(settings, value)?)
    }
}

struct FloatImporter;

impl PipelineImporter for FloatImporter {
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        _settings: &Value,
    ) -> Result<ImportOutput, AuthoringImporterError> {
        let source = context
            .sources()
            .first()
            .ok_or_else(|| AuthoringImporterError::rejected(1, "one source is required"))?
            .path
            .clone();
        let bytes = context.read(&source)?;
        let value = std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| text.parse::<u8>().ok())
            .ok_or_else(|| AuthoringImporterError::rejected(3, "not a number"))?;
        output(value.into())
    }
}

/// [`FLOAT_IMPORTER`]'s descriptor: version 1, settings a
/// [`FLOAT_SETTINGS_TYPE`].
fn float_importer() -> ImporterDescriptor {
    ImporterDescriptor {
        id: FLOAT_IMPORTER.to_owned(),
        version: 1,
        settings_type_uuid: FLOAT_SETTINGS_TYPE,
        settings_schema: LogicalSchema {
            root: float_settings_schema(),
        },
        default_settings: float_default_settings(),
    }
}

/// [`OPTIONAL_IMPORTER`]'s descriptor: version 1, settings an
/// [`OPTIONAL_SETTINGS_TYPE`].
fn optional_importer() -> ImporterDescriptor {
    let option = |node| SchemaNode::Option(Box::new(node));
    ImporterDescriptor {
        id: OPTIONAL_IMPORTER.to_owned(),
        version: 1,
        settings_type_uuid: OPTIONAL_SETTINGS_TYPE,
        settings_schema: LogicalSchema {
            root: SchemaNode::Struct {
                rev: 0,
                fields: vec![(
                    "skeleton".to_owned(),
                    0,
                    option(SchemaNode::Struct {
                        rev: 0,
                        fields: vec![
                            ("name".to_owned(), 0, option(SchemaNode::String)),
                            ("path".to_owned(), 0, SchemaNode::String),
                        ],
                    }),
                )],
            },
        },
        default_settings: optional_default_settings(),
    }
}

/// An importer's descriptor: version 1, settings a [`SETTINGS_TYPE`].
fn importer(id: &str) -> ImporterDescriptor {
    ImporterDescriptor {
        id: id.to_owned(),
        version: 1,
        settings_type_uuid: SETTINGS_TYPE,
        settings_schema: LogicalSchema {
            root: settings_schema(),
        },
        default_settings: default_settings(),
    }
}

#[derive(newgameplus_api_macros::NgpSourceIdentity)]
pub struct SourceIdentity;

struct Reflect;

impl PipelineProcessor for Reflect {
    fn process(
        &self,
        input: Value,
        _context: &mut dyn PipelineProcessContext,
    ) -> Result<ProcessorProducts, ProcessorError> {
        Ok(ProcessorProducts {
            primary: Some(ProcessorProduct::new(COOKED_TYPE, input.clone())),
            extras: BTreeMap::from([(
                REFLECTION.to_owned(),
                ProcessorProduct::new(REFLECTION_TYPE, input),
            )]),
            ..ProcessorProducts::default()
        })
    }
}

fn register(
    targets: &[TargetDefinition],
    arena: &mut RegistrationArena,
) -> Result<BTreeSet<String>, ModuleCallError> {
    let invalid = |error| ModuleCallError::new(format!("{error:?}"));
    arena
        .register_processor(
            ProcessorDescriptor {
                id: REFLECT.to_owned(),
                version: 1,
                input: PARENT_TYPE,
                selector: TargetSelector::new(None, None).map_err(invalid)?,
                outputs: OutputDecls::new(
                    COOKED_TYPE,
                    vec![(REFLECTION.to_owned(), REFLECTION_TYPE)],
                )
                .map_err(|error| ModuleCallError::new(format!("{error:?}")))?,
            },
            Reflect,
        )
        .into_result()?;
    arena
        .register_importer(importer(BYTE_IMPORTER), ByteImporter)
        .into_result()?;
    arena
        .register_importer(importer(CHAIN_IMPORTER), ChainImporter)
        .into_result()?;
    arena
        .register_importer(importer(SETTINGS_IMPORTER), SettingsImporter)
        .into_result()?;
    arena
        .register_importer(importer(REQUIRE_IMPORTER), RequireImporter)
        .into_result()?;
    arena
        .register_importer(float_importer(), FloatImporter)
        .into_result()?;
    arena
        .register_importer(optional_importer(), FloatImporter)
        .into_result()?;
    arena
        .register_default_import(DefaultImportRule {
            id: ImportRuleId(DEFAULT_RULE),
            matches: FileQuery::new(None, Some(format!("**/*.{DEFAULT_EXTENSION}")))
                .map_err(|error| ModuleCallError::new(format!("{error:?}")))?,
            group: DirectoryGrouping::PerFile,
            importer: BYTE_IMPORTER.to_owned(),
            output: "{name}.bundle".to_owned(),
        })
        .into_result()?;
    arena
        .register_authored(AuthoredDescriptor {
            type_uuid: PARENT_TYPE,
            logical_hash: distill_schema::ngp_schema::node_hash(&value_schema()).map_err(|error| ModuleCallError::new(format!("{error:?}")))?,
            extension: PARENT_EXTENSION.to_owned(),
            local_id: "asset".to_owned(),
            doc: "A fixture parent.".to_owned(),
            default: value(7),
        })
        .into_result()?;
    arena
        .register_authored(AuthoredDescriptor {
            type_uuid: VALUE_TYPE,
            logical_hash: distill_core::id::LogicalHash([0; 16]),
            extension: STALE_EXTENSION.to_owned(),
            local_id: "asset".to_owned(),
            doc: "Declared under another schema.".to_owned(),
            default: value(1),
        })
        .into_result()?;
    Ok(targets.iter().map(|target| target.name.clone()).collect())
}

fn unload() -> Result<(), ModuleCallError> {
    Ok(())
}

distill_pipeline_api::export_pipeline_module_v2!(register = register, unload = unload);
