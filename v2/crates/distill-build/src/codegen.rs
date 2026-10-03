//! Authoring-side binding-code generation (§20).
//!
//! Generation happens against a pinned basis and returns its complete
//! outcome-bearing dependency trace.  This coordinator is deliberately
//! independent of filesystem mechanics: the daemon publisher owns the atomic
//! file writes, while this module keeps a stale attempt or a colliding
//! namespace from reaching them.

use std::collections::BTreeMap;

use distill_core::id::AssetUuid;

pub use distill_pipeline_api::codegen::{generated_module_name, CodegenFailure, GeneratedFile};

use crate::trace::TraceOp;

#[derive(Debug, Clone, PartialEq, Eq)]
enum AttemptOutcome {
    Files(Vec<GeneratedFile>),
    Failure(CodegenFailure),
}

/// Everything observed by one asynchronous generation run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodegenAttempt<B> {
    basis: B,
    trace: Vec<TraceOp>,
    outcome: AttemptOutcome,
}

impl<B> CodegenAttempt<B> {
    pub fn success(basis: B, trace: Vec<TraceOp>, files: Vec<GeneratedFile>) -> Self {
        Self {
            basis,
            trace,
            outcome: AttemptOutcome::Files(files),
        }
    }

    pub fn failure(basis: B, trace: Vec<TraceOp>, failure: CodegenFailure) -> Self {
        Self {
            basis,
            trace,
            outcome: AttemptOutcome::Failure(failure),
        }
    }
}

/// Read-only view used for the last-moment basis/trace check.
pub trait CodegenSnapshot<B> {
    /// The current basis; `None` when it cannot be read, which requeues.
    fn current_basis(&self) -> Option<B>;
    fn observe(&self, op: &TraceOp) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationError {
    detail: String,
}

impl PublicationError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for PublicationError {}

/// The implementation must publish the complete slice. It is passed the
/// attempted basis so the daemon can repeat the basis fence while it holds
/// the store and writes the files.
pub trait CodegenPublisher<B> {
    fn publish(
        &mut self,
        attempted_basis: &B,
        files: &[GeneratedFile],
    ) -> Result<(), PublicationError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodegenPublication {
    Published { files: usize },
    Failed(CodegenFailure),
    Requeued,
    PublicationFailed(PublicationError),
}

/// Keeps the trace of the last attempt whose outcome stands: its files were
/// published, or it failed deterministically.
#[derive(Debug, Default)]
pub struct CodegenCoordinator {
    installed: Option<Vec<TraceOp>>,
}

impl CodegenCoordinator {
    /// Finish one attempt. Trace and outcome are installed only after the
    /// complete basis is still current. Successful file traces are installed
    /// only after the all-or-nothing publisher succeeds.
    pub fn finish<B, W>(&mut self, attempt: CodegenAttempt<B>, world: &mut W) -> CodegenPublication
    where
        B: PartialEq,
        W: CodegenSnapshot<B> + CodegenPublisher<B>,
    {
        if world.current_basis().as_ref() != Some(&attempt.basis)
            || !attempt.trace.iter().all(|op| world.observe(op))
        {
            return CodegenPublication::Requeued;
        }

        match attempt.outcome {
            AttemptOutcome::Failure(failure) => {
                self.installed = Some(attempt.trace);
                CodegenPublication::Failed(failure)
            }
            AttemptOutcome::Files(mut files) => {
                if let Err(collision) = validate_namespace(&files) {
                    return CodegenPublication::Failed(collision);
                }
                files.sort_unstable_by(|left, right| left.relative_path().cmp(right.relative_path()));
                let count = files.len();
                if let Err(error) = world.publish(&attempt.basis, &files) {
                    return CodegenPublication::PublicationFailed(error);
                }
                self.installed = Some(attempt.trace);
                CodegenPublication::Published { files: count }
            }
        }
    }

    /// Whether the installed attempt's every observation still holds in
    /// `world`: a run there would generate what stands, so it need not run.
    /// `false` when nothing is installed.
    pub fn holds<B, W: CodegenSnapshot<B>>(&self, world: &W) -> bool {
        self.installed
            .as_ref()
            .is_some_and(|trace| trace.iter().all(|op| world.observe(op)))
    }
}

fn validate_namespace(files: &[GeneratedFile]) -> Result<(), CodegenFailure> {
    let mut claims = BTreeMap::<&str, Vec<AssetUuid>>::new();
    for file in files {
        claims
            .entry(file.relative_path())
            .or_default()
            .push(file.asset());
    }
    if let Some((name, claimants)) = claims
        .into_iter()
        .find(|(_, claimants)| claimants.len() > 1)
    {
        return Err(CodegenFailure::NamespaceCollision {
            name: name.to_owned(),
            claimants,
        });
    }
    Ok(())
}
