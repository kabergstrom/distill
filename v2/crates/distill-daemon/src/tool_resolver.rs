//! Host-owned resolution of package and ambient tool registrations.

use std::collections::BTreeMap;
use std::path::{Component, Path};

use distill_store::pipeline::{ResolvedToolPackageFile, ResolvedToolSourceV2, ToolRegistrationV2};

use crate::callbacks::{ToolDescriptor, ToolRegistration, ToolSource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolResolutionError {
    detail: String,
}

impl ToolResolutionError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for ToolResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ToolResolutionError {}

pub(crate) fn resolve_tool_epoch(
    descriptors: Vec<ToolDescriptor>,
) -> Result<BTreeMap<String, ToolRegistrationV2>, ToolResolutionError> {
    descriptors
        .into_iter()
        .map(|descriptor| {
            let key = descriptor.id;
            let registration = resolve_registration(descriptor.registration)
                .map_err(|error| ToolResolutionError::new(format!("tool `{key}`: {error}")))?;
            Ok((key, registration))
        })
        .collect()
}

fn resolve_registration(
    registration: ToolRegistration,
) -> Result<ToolRegistrationV2, ToolResolutionError> {
    let source = match registration.source {
        ToolSource::Package { root, launcher } => resolve_package(&root, &launcher)?,
        ToolSource::Ambient {
            launcher,
            toolchain_id,
            trusted_fingerprint,
        } => ResolvedToolSourceV2::Ambient {
            launcher: resolve_ambient_launcher(&launcher)?,
            toolchain_id,
            trusted_fingerprint,
        },
    };
    Ok(ToolRegistrationV2 {
        source,
        environment: registration.environment,
        cwd_policy: registration.cwd_policy,
    })
}

fn resolve_package(
    root: &Path,
    launcher: &str,
) -> Result<ResolvedToolSourceV2, ToolResolutionError> {
    let metadata = std::fs::symlink_metadata(root).map_err(|error| {
        ToolResolutionError::new(format!(
            "cannot inspect package `{}`: {error}",
            root.display()
        ))
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ToolResolutionError::new(format!(
            "package `{}` is not a real directory",
            root.display()
        )));
    }
    let root = std::fs::canonicalize(root).map_err(|error| {
        ToolResolutionError::new(format!(
            "cannot resolve package `{}`: {error}",
            root.display()
        ))
    })?;
    let launcher = normalize_package_path(Path::new(launcher))?;
    let mut files = BTreeMap::new();
    collect_package_files(&root, &root, &mut files)?;
    let files = files.into_values().collect::<Vec<_>>();
    if !files
        .iter()
        .any(|file| file.path == launcher && file.executable)
    {
        return Err(ToolResolutionError::new(
            "package launcher does not name an executable package member",
        ));
    }
    Ok(ResolvedToolSourceV2::Package { launcher, files })
}

fn collect_package_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<String, ResolvedToolPackageFile>,
) -> Result<(), ToolResolutionError> {
    let entries = std::fs::read_dir(directory).map_err(|error| {
        ToolResolutionError::new(format!(
            "cannot enumerate package directory `{}`: {error}",
            directory.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            ToolResolutionError::new(format!(
                "cannot enumerate package directory `{}`: {error}",
                directory.display()
            ))
        })?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
            ToolResolutionError::new(format!(
                "cannot inspect package member `{}`: {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(ToolResolutionError::new(format!(
                "package member `{}` is a symlink",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_package_files(root, &path, files)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(ToolResolutionError::new(format!(
                "package member `{}` is not a regular file",
                path.display()
            )));
        }
        let relative = path.strip_prefix(root).map_err(|_| {
            ToolResolutionError::new(format!(
                "package member `{}` escaped its root",
                path.display()
            ))
        })?;
        let package_path = normalize_package_path(relative)?;
        let bytes = std::fs::read(&path).map_err(|error| {
            ToolResolutionError::new(format!(
                "cannot read package member `{}`: {error}",
                path.display()
            ))
        })?;
        files.insert(
            package_path.clone(),
            ResolvedToolPackageFile {
                path: package_path,
                executable: executable_mode(&metadata),
                bytes,
            },
        );
    }
    Ok(())
}

fn resolve_ambient_launcher(path: &Path) -> Result<String, ToolResolutionError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        ToolResolutionError::new(format!(
            "cannot inspect ambient launcher `{}`: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || !executable_mode(&metadata) {
        return Err(ToolResolutionError::new(format!(
            "ambient launcher `{}` is not a real executable file",
            path.display()
        )));
    }
    let path = std::fs::canonicalize(path).map_err(|error| {
        ToolResolutionError::new(format!(
            "cannot resolve ambient launcher `{}`: {error}",
            path.display()
        ))
    })?;
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| ToolResolutionError::new("ambient launcher path is not UTF-8"))
}

fn normalize_package_path(path: &Path) -> Result<String, ToolResolutionError> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    ToolResolutionError::new("package path component is not UTF-8")
                })?;
                if part.is_empty() || part.contains('\0') || part.contains('\\') {
                    return Err(ToolResolutionError::new(
                        "package path contains an invalid component",
                    ));
                }
                parts.push(part.to_owned());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.pop().is_none() {
                    return Err(ToolResolutionError::new("package path escapes its root"));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(ToolResolutionError::new("package path is absolute"));
            }
        }
    }
    if parts.is_empty() {
        return Err(ToolResolutionError::new("package path is empty"));
    }
    Ok(parts.join("/"))
}

#[cfg(unix)]
fn executable_mode(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable_mode(_metadata: &std::fs::Metadata) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use distill_core::tool::ToolCwdPolicy;

    fn executable(path: &Path, bytes: &[u8]) {
        std::fs::write(path, bytes).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[test]
    fn package_registration_snapshots_the_complete_directory() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(directory.path().join("bin")).unwrap();
        std::fs::create_dir_all(directory.path().join("share")).unwrap();
        executable(&directory.path().join("bin/tool"), b"tool");
        std::fs::write(directory.path().join("share/rules"), b"rules").unwrap();

        let resolved = resolve_registration(ToolRegistration {
            source: ToolSource::Package {
                root: directory.path().to_path_buf(),
                launcher: "bin/tool".to_owned(),
            },
            environment: Vec::new(),
            cwd_policy: ToolCwdPolicy::ReadOnlyPackageRoot,
        })
        .unwrap();

        let ResolvedToolSourceV2::Package { launcher, files } = resolved.source else {
            panic!("expected package source");
        };
        assert_eq!(launcher, "bin/tool");
        assert_eq!(
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            ["bin/tool", "share/rules"]
        );
    }

    #[test]
    fn ambient_registration_records_identity_without_inspecting_adjacent_files() {
        let directory = tempfile::tempdir().unwrap();
        let launcher = directory.path().join("tool");
        executable(&launcher, b"tool");
        std::fs::write(directory.path().join("untracked-library"), b"ambient").unwrap();

        let resolved = resolve_registration(ToolRegistration {
            source: ToolSource::Ambient {
                launcher: launcher.clone(),
                toolchain_id: "local-sdk".to_owned(),
                trusted_fingerprint: None,
            },
            environment: Vec::new(),
            cwd_policy: ToolCwdPolicy::EmptyScratch,
        })
        .unwrap();

        assert_eq!(
            resolved.source,
            ResolvedToolSourceV2::Ambient {
                launcher: std::fs::canonicalize(launcher)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
                toolchain_id: "local-sdk".to_owned(),
                trusted_fingerprint: None,
            }
        );
    }

    #[test]
    fn package_launcher_must_be_an_executable_member() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("data"), b"data").unwrap();

        let error = resolve_package(directory.path(), "data").unwrap_err();

        assert!(error.to_string().contains("executable package member"));
    }

    #[cfg(unix)]
    #[test]
    fn package_symlinks_are_rejected_without_following_them() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        symlink(outside.path(), directory.path().join("linked")).unwrap();

        let error = resolve_package(directory.path(), "linked").unwrap_err();

        assert!(error.to_string().contains("is a symlink"));
    }
}
