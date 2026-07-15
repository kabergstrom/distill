use std::collections::BTreeSet;

use distill_daemon::epoch::{CandidateRegistrationArena, ModuleCallError, TargetDefinition};

#[derive(newgameplus_api_macros::NgpSourceIdentity)]
pub struct SourceIdentity;

fn register(
    targets: &[TargetDefinition],
    _arena: &mut CandidateRegistrationArena,
) -> Result<BTreeSet<String>, ModuleCallError> {
    Ok(targets.iter().map(|target| target.name.clone()).collect())
}

fn unload() -> Result<(), ModuleCallError> {
    Ok(())
}

distill_daemon::export_pipeline_module_v2!(register = register, unload = unload);
