use std::collections::BTreeSet;

use distill_pipeline_api::registration::{ModuleCallError, RegistrationArena, TargetDefinition};

#[derive(newgameplus_api_macros::NgpSourceIdentity)]
pub struct SourceIdentity;

fn register(
    targets: &[TargetDefinition],
    _arena: &mut RegistrationArena,
) -> Result<BTreeSet<String>, ModuleCallError> {
    Ok(targets.iter().map(|target| target.name.clone()).collect())
}

fn unload() -> Result<(), ModuleCallError> {
    Ok(())
}

distill_pipeline_api::export_pipeline_module_v2!(register = register, unload = unload);
