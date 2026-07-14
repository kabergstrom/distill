//! The code-identity boundary from §3/§9.
//!
//! The host may `dlopen` exactly one staged and hashed pipeline image. Pipeline
//! code gets no dynamic-library opening capability; changing native code must
//! therefore change the one module image. Dynamic tools are separate, staged,
//! hashed subprocesses.

use std::path::PathBuf;

use object::Object;

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
    InvalidNativeImage { detail: String },
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
            Self::InvalidNativeImage { detail } => {
                write!(f, "pipeline image dependency table is invalid: {detail}")
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

/// Inspect the dependency table encoded in the authenticated native image.
/// This is deliberately performed before platform loader code runs: a module
/// may depend only on the target's acknowledged system runtime.
pub fn validate_pipeline_image_linkage(bytes: &[u8]) -> Result<(), PolicyError> {
    let image = object::File::parse(bytes).map_err(|error| PolicyError::InvalidNativeImage {
        detail: error.to_string(),
    })?;
    let imports = image
        .imports()
        .map_err(|error| PolicyError::InvalidNativeImage {
            detail: error.to_string(),
        })?;
    let mut libraries = imports
        .iter()
        .map(|import| {
            std::str::from_utf8(import.library())
                .map(str::to_owned)
                .map_err(|_| PolicyError::InvalidNativeImage {
                    detail: "dependency name is not UTF-8".to_owned(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    libraries.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    libraries.dedup();
    validate_native_library_names(libraries)
}

pub fn validate_native_library_names<I, S>(libraries: I) -> Result<(), PolicyError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    for library in libraries {
        let library = library.as_ref();
        if !is_system_runtime_library(library) {
            return Err(PolicyError::RuntimeDynamicDependency {
                name: library.to_owned(),
            });
        }
    }
    Ok(())
}

fn is_system_runtime_library(library: &str) -> bool {
    if library.is_empty() {
        return false;
    }
    if library.starts_with("/usr/lib/")
        || library.starts_with("/System/Library/Frameworks/")
        || library.starts_with("/System/Library/PrivateFrameworks/")
    {
        return true;
    }

    let name = library
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(library)
        .to_ascii_lowercase();
    let unix_system_stems = [
        "ld-linux",
        "ld-musl",
        "libc.so",
        "libdl.so",
        "libgcc_s.so",
        "libm.so",
        "libpthread.so",
        "libresolv.so",
        "librt.so",
        "libunwind.so",
        "libutil.so",
        "linux-vdso.so",
    ];
    if unix_system_stems
        .iter()
        .any(|stem| name == *stem || name.starts_with(&format!("{stem}.")))
    {
        return true;
    }

    let windows_system = [
        "advapi32.dll",
        "bcrypt.dll",
        "kernel32.dll",
        "msvcrt.dll",
        "ntdll.dll",
        "ole32.dll",
        "shell32.dll",
        "ucrtbase.dll",
        "user32.dll",
        "userenv.dll",
        "vcruntime140.dll",
        "vcruntime140_1.dll",
        "ws2_32.dll",
    ];
    windows_system.contains(&name.as_str())
        || name.starts_with("api-ms-win-")
        || name.starts_with("ext-ms-win-")
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
