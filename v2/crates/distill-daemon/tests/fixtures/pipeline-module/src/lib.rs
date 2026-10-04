//! The pipeline module the daemon tests load: it serves every target and
//! registers one processor, [`REFLECT`], which cooks a [`PARENT_TYPE`]
//! asset to a [`COOKED_TYPE`] primary and a declared [`REFLECTION`] output
//! of [`REFLECTION_TYPE`]: a derived child asset; and two importers,
//! [`BYTE_IMPORTER`] and [`CHAIN_IMPORTER`], producing [`VALUE_TYPE`].

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
use distill_schema::ngp_schema::{LogicalSchema, SchemaNode};

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
/// configuration: a struct of no fields.
pub const SETTINGS_TYPE: TypeUuid = TypeUuid([0xa6; 16]);
/// The importers' output type, a project type of the daemon tests'
/// configuration no processor cooks: a struct of one `u8` field `value`.
pub const VALUE_TYPE: TypeUuid = TypeUuid([0xa5; 16]);
/// Imports the number its one text source holds, taking that many times
/// 10 ms. A source `"{value} {dir}"` is gated: the run writes `dir/started`
/// and waits for `dir/release` (at most a minute).
pub const BYTE_IMPORTER: &str = "byte-importer";
/// Imports one more than the largest value among its sources that exist: a
/// text source's number, or a bundle source's `asset` entry, so its sources
/// may be other imports' outputs.
pub const CHAIN_IMPORTER: &str = "chain-importer";

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
        _settings: &AuthoredValue,
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
        output(value.into())
    }
}

struct ChainImporter;

impl PipelineImporter for ChainImporter {
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        _settings: &AuthoredValue,
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
        output(value + 1)
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
                fields: Vec::new(),
            },
        },
        default_settings: AuthoredValue::Object(BTreeMap::new()),
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
    Ok(targets.iter().map(|target| target.name.clone()).collect())
}

fn unload() -> Result<(), ModuleCallError> {
    Ok(())
}

distill_pipeline_api::export_pipeline_module_v2!(register = register, unload = unload);
