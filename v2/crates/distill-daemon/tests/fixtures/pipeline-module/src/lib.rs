//! The pipeline module the daemon tests load: it serves every target and
//! registers one processor, [`REFLECT`], which cooks a [`PARENT_TYPE`]
//! asset to a [`COOKED_TYPE`] primary and a declared [`REFLECTION`] output
//! of [`REFLECTION_TYPE`]: a derived child asset; three importers,
//! [`BYTE_IMPORTER`], [`CHAIN_IMPORTER`] and [`SETTINGS_IMPORTER`],
//! producing [`VALUE_TYPE`] from settings of [`SETTINGS_TYPE`]; and
//! [`FLOAT_IMPORTER`], producing [`VALUE_TYPE`] from settings of
//! [`FLOAT_SETTINGS_TYPE`].

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::TypeUuid;
use distill_json::AuthoredValue;
use distill_pipeline_api::callbacks::{
    ImporterDescriptor, PipelineImporter, PipelineProcessContext, PipelineProcessor,
    ProcessorDescriptor, ProcessorError, ProcessorProduct, ProcessorProducts,
};
use distill_pipeline_api::import::ImportOutput;
use distill_pipeline_api::importer::{AuthoringImportContext, AuthoringImporterError};
use distill_pipeline_api::outputs::OutputDecls;
use distill_pipeline_api::registration::{ModuleCallError, RegistrationArena, TargetDefinition};
use distill_pipeline_api::target::TargetSelector;
use distill_schema::ngp_schema::{LogicalSchema, PrimitiveKind, SchemaNode};

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
/// Imports the number its one text source holds, whatever its settings
/// ([`FLOAT_SETTINGS_TYPE`], defaults [`float_default_settings`]).
pub const FLOAT_IMPORTER: &str = "float-importer";

/// A [`VALUE_TYPE`] value.
pub fn value(value: u128) -> AuthoredValue {
    AuthoredValue::Object(BTreeMap::from([(
        "value".to_owned(),
        AuthoredValue::UInt(value),
    )]))
}

/// The `u8` of a [`VALUE_TYPE`] value.
pub fn value_of(value: &AuthoredValue) -> Option<u128> {
    match value {
        AuthoredValue::Object(fields) => match fields.get("value") {
            Some(AuthoredValue::UInt(value)) => Some(*value),
            _ => None,
        },
        _ => None,
    }
}

/// A [`SETTINGS_TYPE`] value.
pub fn settings(add: [u8; 2], by: u8) -> AuthoredValue {
    let uint = |value: u8| AuthoredValue::UInt(value.into());
    AuthoredValue::Object(BTreeMap::from([
        (
            "add".to_owned(),
            AuthoredValue::Array(add.into_iter().map(uint).collect()),
        ),
        (
            "scale".to_owned(),
            AuthoredValue::Object(BTreeMap::from([("by".to_owned(), uint(by))])),
        ),
    ]))
}

/// The importers' default settings, under which a value imports as itself.
pub fn default_settings() -> AuthoredValue {
    settings([0, 0], 1)
}

/// A [`FLOAT_SETTINGS_TYPE`] value.
pub fn float_settings(rate: f64) -> AuthoredValue {
    AuthoredValue::Object(BTreeMap::from([(
        "rate".to_owned(),
        AuthoredValue::Float(rate),
    )]))
}

/// [`FLOAT_IMPORTER`]'s default settings: `rate` 30.0, an integral float
/// (canonical JSON writes it `30`, which parses back as an integer).
pub fn float_default_settings() -> AuthoredValue {
    float_settings(30.0)
}

/// `value` under `settings` (see [`SETTINGS_TYPE`]).
fn apply(settings: &AuthoredValue, value: u128) -> Result<u128, AuthoringImporterError> {
    let malformed = || AuthoringImporterError::rejected(7, format!("settings {settings:?}"));
    let AuthoredValue::Object(fields) = settings else {
        return Err(malformed());
    };
    let Some(AuthoredValue::Array(add)) = fields.get("add") else {
        return Err(malformed());
    };
    let Some(AuthoredValue::Object(scale)) = fields.get("scale") else {
        return Err(malformed());
    };
    let Some(AuthoredValue::UInt(by)) = scale.get("by") else {
        return Err(malformed());
    };
    let mut sum = value;
    for item in add {
        let AuthoredValue::UInt(item) = item else {
            return Err(malformed());
        };
        sum += item;
    }
    Ok(sum * by)
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
        settings: &AuthoredValue,
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
        settings: &AuthoredValue,
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
        settings: &AuthoredValue,
    ) -> Result<ImportOutput, AuthoringImporterError> {
        let mut value = 0;
        for source in context.sources().to_vec() {
            let Some(imported) = context.read_settings(&source.path)? else {
                continue;
            };
            let by = match &imported {
                AuthoredValue::Object(fields) => match fields.get("scale") {
                    Some(AuthoredValue::Object(scale)) => match scale.get("by") {
                        Some(AuthoredValue::UInt(by)) => Some(*by),
                        _ => None,
                    },
                    _ => None,
                },
                _ => None,
            };
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
        _settings: &AuthoredValue,
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
            root: SchemaNode::Struct {
                rev: 0,
                fields: vec![(
                    "rate".to_owned(),
                    0,
                    SchemaNode::Primitive(PrimitiveKind::F32),
                )],
            },
        },
        default_settings: float_default_settings(),
    }
}

/// An importer's descriptor: version 1, settings a [`SETTINGS_TYPE`].
fn importer(id: &str) -> ImporterDescriptor {
    ImporterDescriptor {
        id: id.to_owned(),
        version: 1,
        settings_type_uuid: SETTINGS_TYPE,
        settings_schema: LogicalSchema {
            root: SchemaNode::Struct {
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
            },
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
        input: AuthoredValue,
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
        .register_importer(float_importer(), FloatImporter)
        .into_result()?;
    Ok(targets.iter().map(|target| target.name.clone()).collect())
}

fn unload() -> Result<(), ModuleCallError> {
    Ok(())
}

distill_pipeline_api::export_pipeline_module_v2!(register = register, unload = unload);
