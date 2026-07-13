//! Processor registration and target-resolved pipeline maps (§9).

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::TypeUuid;

use crate::outputs::OutputDecls;
use crate::query::{normalize_identifier, IntakeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TargetOs {
    Linux,
    MacOs,
    Windows,
    Ios,
    Android,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GraphicsApi(pub String);

impl GraphicsApi {
    pub fn new(value: &str) -> Result<Self, IntakeError> {
        normalize_identifier(value).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub os: TargetOs,
    pub apis: BTreeSet<GraphicsApi>,
}

impl Target {
    pub fn new(os: TargetOs, apis: BTreeSet<GraphicsApi>) -> Result<Self, PipelineError> {
        if apis.is_empty() {
            return Err(PipelineError::EmptyTargetApis);
        }
        Ok(Self { os, apis })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSelector {
    pub os: Option<BTreeSet<TargetOs>>,
    pub apis: Option<BTreeSet<GraphicsApi>>,
}

impl TargetSelector {
    pub fn new(
        os: Option<BTreeSet<TargetOs>>,
        apis: Option<BTreeSet<GraphicsApi>>,
    ) -> Result<Self, PipelineError> {
        if os.as_ref().is_some_and(BTreeSet::is_empty)
            || apis.as_ref().is_some_and(BTreeSet::is_empty)
        {
            return Err(PipelineError::EmptySelectorSet);
        }
        Ok(Self { os, apis })
    }

    pub fn matches(&self, target: &Target) -> bool {
        self.os.as_ref().is_none_or(|set| set.contains(&target.os))
            && self
                .apis
                .as_ref()
                .is_none_or(|set| target.apis.is_subset(set))
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        option_sets_intersect(self.os.as_ref(), other.os.as_ref())
            && option_sets_intersect(self.apis.as_ref(), other.apis.as_ref())
    }
}

fn option_sets_intersect<T: Ord>(a: Option<&BTreeSet<T>>, b: Option<&BTreeSet<T>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.iter().any(|v| b.contains(v)),
        _ => true,
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineError {
    InvalidId(IntakeError),
    EmptySelectorSet,
    EmptyTargetApis,
    OverlappingProcessors {
        input: TypeUuid,
        first: String,
        second: String,
    },
    Cycle {
        types: Vec<TypeUuid>,
    },
    TooManyStages,
    DuplicateExtraKey {
        key: String,
    },
    TargetVariantInterface {
        authored: TypeUuid,
    },
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
