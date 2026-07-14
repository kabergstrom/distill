//! Snapshot-bound hermetic tool execution for `ProcessContext` (§9).

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use distill_core::tool::{ToolCwdPolicy, ToolPackageFile, ToolSourceIdentityV2};
use distill_store::pipeline::RegisteredTool;
use distill_store::state::InputVersion;
use distill_store::{Store, StoreError};

use crate::query::{normalize_identifier, IntakeError};
use crate::trace::{
    CapabilityKey, Observed, StableFailureFingerprint, ToolLaunchDiagnostic,
    ToolLaunchFailureClass, TraceOp,
};

/// A snapshot-pinned ToolEpoch projection. Implementations must return the
/// mapping visible at that snapshot, not a live registration path.
pub trait ToolEpochSnapshot {
    fn tool(&self, id: &str) -> Result<Option<RegisteredTool>, StoreError>;
}

/// Store-backed projection of the ToolEpoch visible at one exact input
/// version. The basis is explicit so a job cannot accidentally consult the
/// live mapping after a replacement.
pub struct StoreToolEpochSnapshot<'a> {
    store: &'a Store,
    basis: InputVersion,
}

impl<'a> StoreToolEpochSnapshot<'a> {
    pub fn new(store: &'a Store, basis: InputVersion) -> Self {
        Self { store, basis }
    }

    pub fn basis(&self) -> InputVersion {
        self.basis
    }
}

impl ToolEpochSnapshot for StoreToolEpochSnapshot<'_> {
    fn tool(&self, id: &str) -> Result<Option<RegisteredTool>, StoreError> {
        self.store.tool_at(id, self.basis)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ToolRuntimeBinding<'a> {
    /// Daemon-owned directory in which private execution trees are created.
    pub execution_root: &'a Path,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolOutput {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRunError {
    /// Stable ToolEpoch miss. The matching failing trace operation is retained.
    Stable(StableFailureFingerprint),
    /// Post-lookup launch failure. The entire attempted trace is discarded.
    Transient(ToolLaunchDiagnostic),
    /// Invalid caller input never becomes canonical trace data.
    InvalidId(IntakeError),
    /// Store/runtime infrastructure failed before a closed launch outcome
    /// could be established. The attempted trace is discarded.
    Infrastructure {
        id: String,
        detail: String,
    },
    AttemptStopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceDiscarded;

/// The process-facing subset of the build context. Other context operations
/// may append their observations with [`Self::record`]; a transient tool
/// outcome invalidates the whole sequence so it cannot enter a memo bucket.
pub struct ProcessContext<'a, S: ToolEpochSnapshot + ?Sized> {
    snapshot: &'a S,
    runtime: ToolRuntimeBinding<'a>,
    trace: Vec<TraceOp>,
    stopped: bool,
    discarded: bool,
    cacheable: bool,
}

impl<'a, S: ToolEpochSnapshot + ?Sized> ProcessContext<'a, S> {
    pub fn new(snapshot: &'a S, runtime: ToolRuntimeBinding<'a>) -> Self {
        Self {
            snapshot,
            runtime,
            trace: Vec::new(),
            stopped: false,
            discarded: false,
            cacheable: true,
        }
    }

    pub fn record(&mut self, op: TraceOp) -> Result<(), ToolRunError> {
        if self.stopped || self.discarded {
            return Err(ToolRunError::AttemptStopped);
        }
        self.stopped = op.failed();
        self.trace.push(op);
        Ok(())
    }

    pub fn trace(&self) -> Option<&[TraceOp]> {
        (!self.discarded).then_some(self.trace.as_slice())
    }

    pub fn into_trace(self) -> Result<(Vec<TraceOp>, bool), TraceDiscarded> {
        if self.discarded {
            Err(TraceDiscarded)
        } else {
            Ok((self.trace, self.cacheable))
        }
    }

    pub fn run_tool(
        &mut self,
        id: &str,
        args: &[&str],
        stdin: &[u8],
    ) -> Result<ToolOutput, ToolRunError> {
        if self.stopped || self.discarded {
            return Err(ToolRunError::AttemptStopped);
        }
        let id = match normalize_identifier(id) {
            Ok(id) => id,
            Err(error) => {
                self.discard();
                return Err(ToolRunError::InvalidId(error));
            }
        };
        let registered = match self.snapshot.tool(&id) {
            Ok(Some(registered)) => registered,
            Ok(None) => {
                let failure = StableFailureFingerprint::MissingCapability {
                    key: CapabilityKey::Tool(id.clone()),
                };
                self.trace.push(TraceOp::Tool {
                    id,
                    observed: Observed::Err(failure.clone()),
                });
                self.stopped = true;
                return Err(ToolRunError::Stable(failure));
            }
            Err(error) => {
                self.discard();
                return Err(ToolRunError::Infrastructure {
                    id,
                    detail: error.to_string(),
                });
            }
        };

        self.trace.push(TraceOp::Tool {
            id: id.clone(),
            observed: Observed::Ok(registered.tool_hash),
        });
        self.cacheable &= registered.is_cacheable();
        if registered.revalidate().is_err() {
            let class = match &registered.identity.source {
                ToolSourceIdentityV2::Package { .. } => ToolLaunchFailureClass::PackageUnavailable,
                ToolSourceIdentityV2::Ambient { .. } => ToolLaunchFailureClass::AmbientUnavailable,
            };
            return Err(self.transient(id, registered.tool_hash, class));
        }

        let execution = match ExecutionTree::new(&registered, self.runtime.execution_root) {
            Ok(execution) => execution,
            Err(class) => return Err(self.transient(id, registered.tool_hash, class)),
        };
        let result = launch(&registered, &execution, args, stdin);
        let output = match result {
            Ok(output) => output,
            Err(LaunchError::Closed(class)) => {
                return Err(self.transient(id, registered.tool_hash, class));
            }
            Err(LaunchError::Infrastructure(detail)) => {
                self.discard();
                return Err(ToolRunError::Infrastructure { id, detail });
            }
        };
        if let (Some(package_root), Some(files)) = (
            execution.package_root.as_deref(),
            registered.identity.package_files(),
        ) {
            if verify_execution_root(package_root, files).is_err() {
                return Err(self.transient(
                    id,
                    registered.tool_hash,
                    ToolLaunchFailureClass::PackageUnavailable,
                ));
            }
        }
        Ok(output)
    }

    fn transient(
        &mut self,
        id: String,
        tool_hash: [u8; 32],
        class: ToolLaunchFailureClass,
    ) -> ToolRunError {
        self.discard();
        ToolRunError::Transient(ToolLaunchDiagnostic {
            id,
            tool_hash,
            class,
        })
    }

    fn discard(&mut self) {
        self.trace.clear();
        self.stopped = true;
        self.discarded = true;
    }
}

struct ExecutionTree {
    directory: tempfile::TempDir,
    package_root: Option<PathBuf>,
    scratch_root: PathBuf,
}

impl ExecutionTree {
    fn new(tool: &RegisteredTool, execution_root: &Path) -> Result<Self, ToolLaunchFailureClass> {
        let directory = tempfile::Builder::new()
            .prefix(".distill-tool-")
            .tempdir_in(execution_root)
            .map_err(|_| ToolLaunchFailureClass::SpawnDenied)?;
        let scratch_root = directory.path().join("scratch");
        std::fs::create_dir(&scratch_root).map_err(|_| ToolLaunchFailureClass::SpawnDenied)?;

        let package_root = if let ToolSourceIdentityV2::Package { files, .. } =
            &tool.identity.source
        {
            let source_root = tool
                .root
                .as_deref()
                .ok_or(ToolLaunchFailureClass::PackageUnavailable)?;
            let package_root = directory.path().join("package");
            std::fs::create_dir(&package_root).map_err(|_| ToolLaunchFailureClass::SpawnDenied)?;
            for file in files {
                let source = source_root.join(&file.path);
                let destination = package_root.join(&file.path);
                let bytes = std::fs::read(&source)
                    .map_err(|_| ToolLaunchFailureClass::PackageUnavailable)?;
                if bytes.len() as u64 != file.len
                    || blake3::hash(&bytes).as_bytes() != &file.bytes_hash
                {
                    return Err(ToolLaunchFailureClass::PackageUnavailable);
                }
                let parent = destination
                    .parent()
                    .ok_or(ToolLaunchFailureClass::PackageUnavailable)?;
                std::fs::create_dir_all(parent).map_err(|_| ToolLaunchFailureClass::SpawnDenied)?;
                let mut output = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&destination)
                    .map_err(|_| ToolLaunchFailureClass::SpawnDenied)?;
                output
                    .write_all(&bytes)
                    .map_err(|_| ToolLaunchFailureClass::SpawnDenied)?;
                set_file_permissions(&output, file.executable)
                    .map_err(|_| ToolLaunchFailureClass::SpawnDenied)?;
            }
            seal_directories(&package_root).map_err(|_| ToolLaunchFailureClass::SpawnDenied)?;
            verify_execution_root(&package_root, files)
                .map_err(|_| ToolLaunchFailureClass::PackageUnavailable)?;
            Some(package_root)
        } else {
            None
        };

        Ok(Self {
            directory,
            package_root,
            scratch_root,
        })
    }
}

impl Drop for ExecutionTree {
    fn drop(&mut self) {
        make_directories_writable(self.directory.path());
    }
}

enum LaunchError {
    Closed(ToolLaunchFailureClass),
    Infrastructure(String),
}

fn launch(
    tool: &RegisteredTool,
    execution: &ExecutionTree,
    args: &[&str],
    stdin: &[u8],
) -> Result<ToolOutput, LaunchError> {
    let (program, source_is_ambient) = match &tool.identity.source {
        ToolSourceIdentityV2::Package { launcher, .. } => (
            execution
                .package_root
                .as_deref()
                .ok_or(LaunchError::Closed(
                    ToolLaunchFailureClass::PackageUnavailable,
                ))?
                .join(launcher),
            false,
        ),
        ToolSourceIdentityV2::Ambient { launcher, .. } => (PathBuf::from(launcher), true),
    };

    let mut command = Command::new(program);
    command.args(args);
    command.env_clear();
    command.envs(
        tool.identity
            .environment
            .iter()
            .map(|(key, value)| (key, value)),
    );
    let cwd = match &tool.identity.cwd_policy {
        ToolCwdPolicy::EmptyScratch => execution.scratch_root.clone(),
        ToolCwdPolicy::ReadOnlyPackageRoot => execution.package_root.clone().ok_or(
            LaunchError::Closed(ToolLaunchFailureClass::PackageUnavailable),
        )?,
        ToolCwdPolicy::ReadOnlyPackageSubdir(path) => execution
            .package_root
            .as_deref()
            .ok_or(LaunchError::Closed(
                ToolLaunchFailureClass::PackageUnavailable,
            ))?
            .join(path),
    };
    command.current_dir(cwd);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| {
        let class = match error.kind() {
            std::io::ErrorKind::NotFound if source_is_ambient => {
                ToolLaunchFailureClass::AmbientUnavailable
            }
            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
                ToolLaunchFailureClass::NotExecutable
            }
            _ => ToolLaunchFailureClass::SpawnDenied,
        };
        LaunchError::Closed(class)
    })?;
    let mut child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| LaunchError::Infrastructure("spawned tool has no stdin pipe".to_owned()))?;
    if let Err(error) = child_stdin.write_all(stdin) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(LaunchError::Infrastructure(format!(
            "failed to write tool stdin: {error}"
        )));
    }
    drop(child_stdin);
    let output = child.wait_with_output().map_err(|error| {
        LaunchError::Infrastructure(format!("failed to collect tool output: {error}"))
    })?;
    Ok(ToolOutput {
        status: output.status.code().unwrap_or(-1),
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

fn verify_execution_root(root: &Path, expected: &[ToolPackageFile]) -> Result<(), ()> {
    let expected_paths = expected
        .iter()
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    collect_files(root, root, &mut actual)?;
    if actual != expected_paths {
        return Err(());
    }
    for file in expected {
        let path = root.join(&file.path);
        let metadata = std::fs::symlink_metadata(&path).map_err(|_| ())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != file.len {
            return Err(());
        }
        let bytes = std::fs::read(&path).map_err(|_| ())?;
        if blake3::hash(&bytes).as_bytes() != &file.bytes_hash
            || !file_permissions_match(&metadata, file.executable)
        {
            return Err(());
        }
    }
    Ok(())
}

fn collect_files(root: &Path, directory: &Path, files: &mut BTreeSet<String>) -> Result<(), ()> {
    let metadata = std::fs::symlink_metadata(directory).map_err(|_| ())?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || !directory_permissions_are_read_only(&metadata)
    {
        return Err(());
    }
    for entry in std::fs::read_dir(directory).map_err(|_| ())? {
        let entry = entry.map_err(|_| ())?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path).map_err(|_| ())?;
        if metadata.file_type().is_symlink() {
            return Err(());
        }
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path.strip_prefix(root).map_err(|_| ())?;
            let relative = relative
                .components()
                .map(|component| component.as_os_str().to_str())
                .collect::<Option<Vec<_>>>()
                .ok_or(())?
                .join("/");
            files.insert(relative);
        } else {
            return Err(());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_file_permissions(file: &std::fs::File, executable: bool) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(if executable {
        0o555
    } else {
        0o444
    }))
}

#[cfg(not(unix))]
fn set_file_permissions(file: &std::fs::File, _executable: bool) -> std::io::Result<()> {
    let mut permissions = file.metadata()?.permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)
}

#[cfg(unix)]
fn file_permissions_match(metadata: &std::fs::Metadata, executable: bool) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o777 == if executable { 0o555 } else { 0o444 }
}

#[cfg(not(unix))]
fn file_permissions_match(metadata: &std::fs::Metadata, _executable: bool) -> bool {
    metadata.permissions().readonly()
}

#[cfg(unix)]
fn directory_permissions_are_read_only(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o222 == 0
}

#[cfg(not(unix))]
fn directory_permissions_are_read_only(metadata: &std::fs::Metadata) -> bool {
    metadata.permissions().readonly()
}

fn seal_directories(root: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if std::fs::symlink_metadata(&path)?.is_dir() {
            seal_directories(&path)?;
        }
    }
    set_directory_read_only(root)
}

#[cfg(unix)]
fn set_directory_read_only(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o555))
}

#[cfg(not(unix))]
fn set_directory_read_only(path: &Path) -> std::io::Result<()> {
    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(path, permissions)
}

fn make_directories_writable(root: &Path) {
    set_directory_writable(root);
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if std::fs::symlink_metadata(&path)
            .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
        {
            make_directories_writable(&path);
        }
    }
}

#[cfg(unix)]
fn set_directory_writable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755));
}

#[cfg(not(unix))]
fn set_directory_writable(path: &Path) {
    if let Ok(metadata) = std::fs::metadata(path) {
        let mut permissions = metadata.permissions();
        permissions.set_readonly(false);
        let _ = std::fs::set_permissions(path, permissions);
    }
}
