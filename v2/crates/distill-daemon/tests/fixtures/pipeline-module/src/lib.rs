//! The pipeline module the daemon tests load: it serves every target and
//! registers one processor, [`REFLECT`], which cooks a [`PARENT_TYPE`]
//! asset to a [`COOKED_TYPE`] primary and a declared [`REFLECTION`] output
//! of [`REFLECTION_TYPE`]: a derived child asset.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::TypeUuid;
use distill_json::AuthoredValue;
use distill_pipeline_api::callbacks::{
    PipelineProcessContext, PipelineProcessor, ProcessorDescriptor, ProcessorError,
    ProcessorProduct, ProcessorProducts,
};
use distill_pipeline_api::outputs::OutputDecls;
use distill_pipeline_api::registration::{ModuleCallError, RegistrationArena, TargetDefinition};
use distill_pipeline_api::target::TargetSelector;

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
    Ok(targets.iter().map(|target| target.name.clone()).collect())
}

fn unload() -> Result<(), ModuleCallError> {
    Ok(())
}

distill_pipeline_api::export_pipeline_module_v2!(register = register, unload = unload);
