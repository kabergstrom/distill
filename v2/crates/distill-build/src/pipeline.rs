//! Processor registration and target-resolved pipeline maps (§9).

use std::collections::BTreeMap;

use distill_core::id::TypeUuid;
pub use distill_pipeline_api::target::{
    GraphicsApi, PipelineError, Target, TargetArch, TargetOs, TargetSelector,
};

use crate::outputs::OutputDecls;
use crate::query::normalize_identifier;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorRegistration {
    pub id: String,
    pub version: u32,
    pub input: TypeUuid,
    pub selector: TargetSelector,
    pub outputs: OutputDecls,
    pub dylib_hash: [u8; 32],
}

impl ProcessorRegistration {
    pub fn new(
        id: &str,
        version: u32,
        input: TypeUuid,
        selector: TargetSelector,
        outputs: OutputDecls,
        dylib_hash: [u8; 32],
    ) -> Result<Self, PipelineError> {
        Ok(Self {
            id: normalize_identifier(id).map_err(PipelineError::InvalidId)?,
            version,
            input,
            selector,
            outputs,
            dylib_hash,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineStage {
    pub index: u16,
    pub registration: ProcessorRegistration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineChain {
    pub authored: TypeUuid,
    pub terminal: TypeUuid,
    pub stages: Vec<PipelineStage>,
    pub extras: BTreeMap<String, TypeUuid>,
}

pub struct PipelineRegistry {
    by_input: BTreeMap<TypeUuid, Vec<ProcessorRegistration>>,
}

impl PipelineRegistry {
    pub fn new(registrations: Vec<ProcessorRegistration>) -> Result<Self, PipelineError> {
        let mut by_input: BTreeMap<TypeUuid, Vec<ProcessorRegistration>> = BTreeMap::new();
        for registration in registrations {
            let existing = by_input.entry(registration.input).or_default();
            if let Some(other) = existing
                .iter()
                .find(|other| other.selector.overlaps(&registration.selector))
            {
                return Err(PipelineError::OverlappingProcessors {
                    input: registration.input,
                    first: other.id.clone(),
                    second: registration.id,
                });
            }
            existing.push(registration);
        }
        Ok(Self { by_input })
    }

    /// Every type some registration takes as input, in type order: the
    /// only types whose chains can end anywhere but where they start.
    pub fn input_types(&self) -> impl Iterator<Item = TypeUuid> + '_ {
        self.by_input.keys().copied()
    }

    pub fn chain(
        &self,
        authored: TypeUuid,
        target: &Target,
    ) -> Result<PipelineChain, PipelineError> {
        let mut current = authored;
        let mut seen = Vec::new();
        let mut stages = Vec::new();
        let mut extras = BTreeMap::new();
        loop {
            if let Some(at) = seen.iter().position(|ty| *ty == current) {
                let mut types = seen[at..].to_vec();
                types.push(current);
                return Err(PipelineError::Cycle { types });
            }
            seen.push(current);
            let Some(registration) = self
                .by_input
                .get(&current)
                .and_then(|v| v.iter().find(|r| r.selector.matches(target)))
                .cloned()
            else {
                break;
            };
            let index = u16::try_from(stages.len()).map_err(|_| PipelineError::TooManyStages)?;
            for (key, ty) in &registration.outputs.extras {
                if extras.insert(key.clone(), *ty).is_some() {
                    return Err(PipelineError::DuplicateExtraKey { key: key.clone() });
                }
            }
            current = registration.outputs.primary;
            stages.push(PipelineStage {
                index,
                registration,
            });
        }
        Ok(PipelineChain {
            authored,
            terminal: current,
            stages,
            extras,
        })
    }

    pub fn validate_target_invariance(
        &self,
        authored_types: &[TypeUuid],
        targets: &[Target],
    ) -> Result<(), PipelineError> {
        for authored in authored_types {
            let mut expected: Option<(TypeUuid, BTreeMap<String, TypeUuid>)> = None;
            for target in targets {
                let chain = self.chain(*authored, target)?;
                let interface = (chain.terminal, chain.extras);
                if expected.as_ref().is_some_and(|v| v != &interface) {
                    return Err(PipelineError::TargetVariantInterface {
                        authored: *authored,
                    });
                }
                expected.get_or_insert(interface);
            }
        }
        Ok(())
    }
}
