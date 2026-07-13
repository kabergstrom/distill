//! The code-identity boundary from §3/§9.
//!
//! The host may `dlopen` exactly one staged and hashed pipeline image. Pipeline
//! code gets no dynamic-library opening capability; changing native code must
//! therefore change the one module image. Dynamic tools are separate, staged,
//! hashed subprocesses.

use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Linkage {
    Static,
    RuntimeDynamic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeDependency {
    pub name: String,
    pub linkage: Linkage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeLoadRequest {
    HostPipelineModule {
        staged_copy: bool,
        content_hash: Option<[u8; 32]>,
    },
    PipelineDlopen {
        path: PathBuf,
    },
    ToolSubprocess {
        staged_copy: bool,
        content_hash: Option<[u8; 32]>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeLoadPermit {
    HostPipelineModule,
    ToolSubprocess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    RuntimeDynamicDependency { name: String },
    PipelineDlopenBanned,
    UnstagedCode,
    MissingContentHash,
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuntimeDynamicDependency { name } => {
                write!(f, "pipeline dependency `{name}` must be statically linked")
            }
            Self::PipelineDlopenBanned => {
                f.write_str("runtime dlopen from pipeline/module code is banned")
            }
            Self::UnstagedCode => f.write_str("code must execute from a staged copy"),
            Self::MissingContentHash => {
                f.write_str("staged code must carry the hash of the staged bytes")
            }
        }
    }
}

impl std::error::Error for PolicyError {}

pub fn validate_candidate_linkage(dependencies: &[NativeDependency]) -> Result<(), PolicyError> {
    if let Some(dependency) = dependencies
        .iter()
        .find(|dependency| dependency.linkage == Linkage::RuntimeDynamic)
    {
        return Err(PolicyError::RuntimeDynamicDependency {
            name: dependency.name.clone(),
        });
    }
    Ok(())
}

pub struct CodeLoadingPolicy;

impl CodeLoadingPolicy {
    pub fn authorize(request: CodeLoadRequest) -> Result<CodeLoadPermit, PolicyError> {
        match request {
            CodeLoadRequest::PipelineDlopen { .. } => Err(PolicyError::PipelineDlopenBanned),
            CodeLoadRequest::HostPipelineModule {
                staged_copy,
                content_hash,
            } => {
                validate_staging(staged_copy, content_hash)?;
                Ok(CodeLoadPermit::HostPipelineModule)
            }
            CodeLoadRequest::ToolSubprocess {
                staged_copy,
                content_hash,
            } => {
                validate_staging(staged_copy, content_hash)?;
                Ok(CodeLoadPermit::ToolSubprocess)
            }
        }
    }
}

fn validate_staging(staged_copy: bool, content_hash: Option<[u8; 32]>) -> Result<(), PolicyError> {
    if !staged_copy {
        return Err(PolicyError::UnstagedCode);
    }
    if content_hash.is_none() {
        return Err(PolicyError::MissingContentHash);
    }
    Ok(())
}
