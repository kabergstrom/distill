//! Build targets and processor target selectors (§9).

use std::collections::BTreeSet;

use distill_core::id::TypeUuid;
use ngp_schema::LayoutIdentity;

use crate::query::{normalize_identifier, IntakeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TargetOs {
    Linux,
    MacOs,
    Windows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TargetArch {
    Aarch64,
    X86_64,
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
    pub arch: TargetArch,
    pub apis: BTreeSet<GraphicsApi>,
    pub optimize: bool,
    pub debug_info: bool,
    pub layout_identity: LayoutIdentity,
}

impl Target {
    pub fn new(
        os: TargetOs,
        arch: TargetArch,
        apis: BTreeSet<GraphicsApi>,
        optimize: bool,
        debug_info: bool,
        layout_identity: LayoutIdentity,
    ) -> Result<Self, PipelineError> {
        if apis.is_empty() {
            return Err(PipelineError::EmptyTargetApis);
        }
        Ok(Self {
            os,
            arch,
            apis,
            optimize,
            debug_info,
            layout_identity,
        })
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
