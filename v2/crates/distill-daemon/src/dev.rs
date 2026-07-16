//! Startup-only development process supervision.
//!
//! This owns the long-lived `source-walk` and Cargo-watch processes without
//! taking over either producer's incremental invalidation logic. The serving
//! daemon still observes only atomically published schema/module artifacts.

use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::config::DaemonConfig;
use crate::process::{DaemonProcess, DaemonProcessError};

const MIN_HEALTHY_UPTIME: Duration = Duration::from_secs(10);
const MAX_RAPID_FAILURES: u32 = 5;
const BASE_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
const REQUESTED_RESTART_EXIT_CODE: i32 = 75;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevLaunchConfig {
    pub source_path: PathBuf,
    pub workspace_root: PathBuf,
    pub target_directory: PathBuf,
    pub source_walk: LaunchCommand,
    pub cargo: CargoWatchLaunch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CargoWatchLaunch {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub pipeline_packages: Vec<String>,
    pub gameplay_packages: Vec<String>,
    pub features: Vec<String>,
    pub profile: BuildProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildProfile {
    Debug,
    Release,
}

#[derive(Debug)]
pub enum DevLaunchConfigError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Toml(String),
    CurrentDirectory(std::io::Error),
    WorkspaceUnavailable(PathBuf),
    EmptyProgram(&'static str),
    EmptyPipelinePackages,
    InvalidPackage(String),
    InvalidFeature(String),
    InvalidProfile(String),
    ReservedSourceWalkArgument(&'static str),
}

impl std::fmt::Display for DevLaunchConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "development launch configuration: {self:?}")
    }
}

impl std::error::Error for DevLaunchConfigError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDevLaunchConfig {
    workspace_root: PathBuf,
    target_directory: PathBuf,
    source_walk: RawLaunchCommand,
    cargo: RawCargoWatchLaunch,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLaunchCommand {
    program: PathBuf,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCargoWatchLaunch {
    program: PathBuf,
    #[serde(default)]
    args: Vec<String>,
    pipeline_packages: Vec<String>,
    #[serde(default)]
    gameplay_packages: Vec<String>,
    #[serde(default)]
    features: Vec<String>,
    profile: String,
}

impl DevLaunchConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, DevLaunchConfigError> {
        let path = path.as_ref();
        let source =
            std::fs::read_to_string(path).map_err(|source| DevLaunchConfigError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        Self::parse(path, &source)
    }

    pub fn parse(
        source_path: impl AsRef<Path>,
        source: &str,
    ) -> Result<Self, DevLaunchConfigError> {
        let source_path = absolute_path(source_path.as_ref())?;
        let base = source_path.parent().unwrap_or_else(|| Path::new("/"));
        let raw: RawDevLaunchConfig = toml::from_str(source)
            .map_err(|error| DevLaunchConfigError::Toml(error.to_string()))?;
        let workspace_root = resolve_path(base, &raw.workspace_root);
        if !workspace_root.is_dir() {
            return Err(DevLaunchConfigError::WorkspaceUnavailable(workspace_root));
        }
        let workspace_root = std::fs::canonicalize(&workspace_root)
            .map_err(|_| DevLaunchConfigError::WorkspaceUnavailable(workspace_root))?;
        let target_directory = resolve_path(&workspace_root, &raw.target_directory);
        let source_program = resolve_program(&workspace_root, raw.source_walk.program);
        let cargo_program = resolve_program(&workspace_root, raw.cargo.program);
        if source_program.as_os_str().is_empty() {
            return Err(DevLaunchConfigError::EmptyProgram("source_walk"));
        }
        if cargo_program.as_os_str().is_empty() {
            return Err(DevLaunchConfigError::EmptyProgram("cargo"));
        }
        for reserved in ["--target-dir", "--target-directory"] {
            if raw.source_walk.args.iter().any(|arg| arg == reserved) {
                return Err(DevLaunchConfigError::ReservedSourceWalkArgument(reserved));
            }
        }
        if raw.cargo.pipeline_packages.is_empty() {
            return Err(DevLaunchConfigError::EmptyPipelinePackages);
        }
        for package in raw
            .cargo
            .pipeline_packages
            .iter()
            .chain(&raw.cargo.gameplay_packages)
        {
            if !is_cargo_name(package) {
                return Err(DevLaunchConfigError::InvalidPackage(package.clone()));
            }
        }
        for feature in &raw.cargo.features {
            if !is_cargo_name(feature) {
                return Err(DevLaunchConfigError::InvalidFeature(feature.clone()));
            }
        }
        let profile = match raw.cargo.profile.as_str() {
            "debug" => BuildProfile::Debug,
            "release" => BuildProfile::Release,
            other => return Err(DevLaunchConfigError::InvalidProfile(other.to_owned())),
        };
        Ok(Self {
            source_path,
            workspace_root,
            target_directory,
            source_walk: LaunchCommand {
                program: source_program,
                args: raw.source_walk.args,
            },
            cargo: CargoWatchLaunch {
                program: cargo_program,
                args: raw.cargo.args,
                pipeline_packages: raw.cargo.pipeline_packages,
                gameplay_packages: raw.cargo.gameplay_packages,
                features: raw.cargo.features,
                profile,
            },
        })
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf, DevLaunchConfigError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(DevLaunchConfigError::CurrentDirectory)
    }
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn resolve_program(workspace_root: &Path, program: PathBuf) -> PathBuf {
    if program.is_absolute() || program.components().count() == 1 {
        program
    } else {
        workspace_root.join(program)
    }
}

fn is_cargo_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticStream {
    Supervisor,
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevDiagnostic {
    pub source: &'static str,
    pub stream: DiagnosticStream,
    pub text: String,
}

#[derive(Debug)]
pub enum DevSupervisorError {
    Daemon(DaemonProcessError),
    DaemonStopped(Option<String>),
}

impl std::fmt::Display for DevSupervisorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "development supervisor: {self:?}")
    }
}

impl std::error::Error for DevSupervisorError {}

impl From<DaemonProcessError> for DevSupervisorError {
    fn from(error: DaemonProcessError) -> Self {
        Self::Daemon(error)
    }
}

pub struct DevSupervisor {
    launch: DevLaunchConfig,
    daemon_config: Option<DaemonConfig>,
    daemon: Option<DaemonProcess>,
    source_walk: ManagedChild,
    cargo_watch: ManagedChild,
    diagnostics_rx: mpsc::Receiver<DevDiagnostic>,
}

impl DevSupervisor {
    pub fn start(daemon_config: DaemonConfig, launch: DevLaunchConfig) -> Self {
        let (diagnostics_tx, diagnostics_rx) = mpsc::channel();
        let source_walk = ManagedChild::start(
            ChildKind::SourceWalk,
            source_walk_command(&launch),
            diagnostics_tx.clone(),
        );
        let cargo_watch = ManagedChild::start(
            ChildKind::CargoWatch,
            cargo_watch_command(&launch),
            diagnostics_tx,
        );
        Self {
            launch,
            daemon_config: Some(daemon_config),
            daemon: None,
            source_walk,
            cargo_watch,
            diagnostics_rx,
        }
    }

    /// Advance producer restart state and start the serving daemon once both
    /// first-generation artifacts are readable. Existing artifacts make this
    /// happen on the first poll while producers catch up in parallel.
    pub fn poll(&mut self) -> Result<Vec<DevDiagnostic>, DevSupervisorError> {
        self.source_walk.poll();
        self.cargo_watch.poll();
        if self.daemon.is_none() {
            let config = self
                .daemon_config
                .as_ref()
                .expect("daemon config exists until daemon startup");
            if artifacts_ready(config) {
                let config = self.daemon_config.take().expect("checked above");
                self.daemon = Some(DaemonProcess::start(config)?);
            }
        }
        if let Some(daemon) = &self.daemon {
            if daemon.has_stopped() {
                return Err(DevSupervisorError::DaemonStopped(
                    daemon.last_background_error(),
                ));
            }
        }
        Ok(self.diagnostics_rx.try_iter().collect())
    }

    pub fn rpc_address(&self) -> Option<std::net::SocketAddr> {
        self.daemon.as_ref().map(DaemonProcess::rpc_address)
    }

    pub fn waiting_for_artifacts(&self) -> bool {
        self.daemon.is_none()
    }

    pub fn run(mut self) -> Result<(), DevSupervisorError> {
        install_shutdown_handlers();
        let mut announced_address = None;
        let mut announced_wait = false;
        while !SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            let diagnostics = self.poll()?;
            for diagnostic in diagnostics {
                eprintln!("[{}] {}", diagnostic.source, diagnostic.text);
            }
            if let Some(address) = self.rpc_address() {
                if announced_address != Some(address) {
                    eprintln!("distill daemon listening on {address}");
                    announced_address = Some(address);
                }
            } else if !announced_wait {
                let config = self.daemon_config.as_ref().expect("daemon has not started");
                eprintln!(
                    "waiting for schema {} and pipeline module {}",
                    config.assets.schema_path.display(),
                    config.modules.pipeline_dylib.display()
                );
                announced_wait = true;
            }
            std::thread::sleep(POLL_INTERVAL);
        }
        Ok(())
    }

    pub fn launch_config(&self) -> &DevLaunchConfig {
        &self.launch
    }
}

fn artifacts_ready(config: &DaemonConfig) -> bool {
    readable_nonempty_file(&config.assets.schema_path)
        && readable_nonempty_file(&config.modules.pipeline_dylib)
}

fn readable_nonempty_file(path: &Path) -> bool {
    std::fs::File::open(path)
        .and_then(|file| file.metadata())
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
}

#[derive(Debug, Clone)]
struct ChildCommand {
    program: PathBuf,
    args: Vec<String>,
    cwd: PathBuf,
    target_directory: Option<PathBuf>,
}

fn source_walk_command(config: &DevLaunchConfig) -> ChildCommand {
    let mut args = config.source_walk.args.clone();
    args.push("--target-dir".to_owned());
    args.push(config.target_directory.to_string_lossy().into_owned());
    ChildCommand {
        program: config.source_walk.program.clone(),
        args,
        cwd: config.workspace_root.clone(),
        target_directory: None,
    }
}

fn cargo_watch_command(config: &DevLaunchConfig) -> ChildCommand {
    let mut build = vec!["build".to_owned()];
    for package in config
        .cargo
        .pipeline_packages
        .iter()
        .chain(&config.cargo.gameplay_packages)
    {
        build.push("-p".to_owned());
        build.push(package.clone());
    }
    if !config.cargo.features.is_empty() {
        build.push("--features".to_owned());
        build.push(config.cargo.features.join(","));
    }
    if config.cargo.profile == BuildProfile::Release {
        build.push("--release".to_owned());
    }
    let mut args = config.cargo.args.clone();
    args.extend(["watch".to_owned(), "-x".to_owned(), build.join(" ")]);
    ChildCommand {
        program: config.cargo.program.clone(),
        args,
        cwd: config.workspace_root.clone(),
        target_directory: Some(config.target_directory.clone()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildKind {
    SourceWalk,
    CargoWatch,
}

impl ChildKind {
    fn name(self) -> &'static str {
        match self {
            Self::SourceWalk => "source-walk",
            Self::CargoWatch => "cargo-watch",
        }
    }
}

struct ManagedChild {
    kind: ChildKind,
    command: ChildCommand,
    child: Option<Child>,
    readers: Vec<JoinHandle<()>>,
    diagnostics: mpsc::Sender<DevDiagnostic>,
    last_spawn: Option<Instant>,
    consecutive_failures: u32,
    retry_at: Option<Instant>,
    gave_up: bool,
}

impl ManagedChild {
    fn start(
        kind: ChildKind,
        command: ChildCommand,
        diagnostics: mpsc::Sender<DevDiagnostic>,
    ) -> Self {
        let mut managed = Self {
            kind,
            command,
            child: None,
            readers: Vec::new(),
            diagnostics,
            last_spawn: None,
            consecutive_failures: 0,
            retry_at: None,
            gave_up: false,
        };
        managed.spawn_or_back_off();
        managed
    }

    fn poll(&mut self) {
        if self.gave_up {
            return;
        }
        if let Some(retry_at) = self.retry_at {
            if Instant::now() >= retry_at {
                self.join_readers();
                self.retry_at = None;
                self.spawn_or_back_off();
            }
            return;
        }
        let Some(child) = &mut self.child else {
            self.record_failure("producer is not running".to_owned());
            return;
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                self.child = None;
                if self
                    .last_spawn
                    .is_some_and(|spawned| spawned.elapsed() >= MIN_HEALTHY_UPTIME)
                {
                    self.consecutive_failures = 0;
                }
                self.record_exit(status);
            }
            Ok(None) => {}
            Err(error) => {
                self.child = None;
                self.record_failure(format!("failed to poll producer: {error}"));
            }
        }
    }

    fn spawn_or_back_off(&mut self) {
        match spawn_child(self.kind, &self.command, &self.diagnostics) {
            Ok((child, readers)) => {
                self.child = Some(child);
                self.readers = readers;
                self.last_spawn = Some(Instant::now());
                let _ = self.diagnostics.send(DevDiagnostic {
                    source: self.kind.name(),
                    stream: DiagnosticStream::Supervisor,
                    text: "started".to_owned(),
                });
            }
            Err(error) => self.record_failure(format!("failed to spawn producer: {error}")),
        }
    }

    fn record_exit(&mut self, status: ExitStatus) {
        if is_requested_restart(self.kind, status) {
            self.consecutive_failures = 0;
            self.retry_at = Some(Instant::now());
            let _ = self.diagnostics.send(DevDiagnostic {
                source: self.kind.name(),
                stream: DiagnosticStream::Supervisor,
                text: "workspace changed; restarting with a fresh crate graph".to_owned(),
            });
            return;
        }
        self.record_failure(format!("exited with {status}"));
    }

    fn record_failure(&mut self, reason: String) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures > MAX_RAPID_FAILURES {
            self.gave_up = true;
            let _ = self.diagnostics.send(DevDiagnostic {
                source: self.kind.name(),
                stream: DiagnosticStream::Supervisor,
                text: format!(
                    "{reason}; crash-looped {} times, giving up until distilld is restarted",
                    self.consecutive_failures - 1
                ),
            });
            return;
        }
        let backoff = backoff(self.consecutive_failures);
        self.retry_at = Some(Instant::now() + backoff);
        let _ = self.diagnostics.send(DevDiagnostic {
            source: self.kind.name(),
            stream: DiagnosticStream::Supervisor,
            text: format!(
                "{reason}; retrying in {:.1}s (failure {}/{MAX_RAPID_FAILURES})",
                backoff.as_secs_f32(),
                self.consecutive_failures
            ),
        });
    }

    fn join_readers(&mut self) {
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }

    fn terminate(&mut self) {
        if let Some(mut child) = self.child.take() {
            terminate_process_group(&mut child, SHUTDOWN_GRACE);
        }
        self.join_readers();
    }
}

fn is_requested_restart(kind: ChildKind, status: ExitStatus) -> bool {
    is_requested_restart_code(kind, status.code())
}

fn is_requested_restart_code(kind: ChildKind, code: Option<i32>) -> bool {
    kind == ChildKind::SourceWalk && code == Some(REQUESTED_RESTART_EXIT_CODE)
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        self.terminate();
    }
}

fn backoff(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(16);
    BASE_BACKOFF
        .checked_mul(1u32 << shift)
        .unwrap_or(MAX_BACKOFF)
        .min(MAX_BACKOFF)
}

fn spawn_child(
    kind: ChildKind,
    command: &ChildCommand,
    diagnostics: &mpsc::Sender<DevDiagnostic>,
) -> Result<(Child, Vec<JoinHandle<()>>), std::io::Error> {
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .current_dir(&command.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(target_directory) = &command.target_directory {
        process.env("CARGO_TARGET_DIR", target_directory);
    }
    configure_process_group(&mut process);
    let mut child = process.spawn()?;
    let mut readers = Vec::with_capacity(2);
    if let Some(stdout) = child.stdout.take() {
        readers.push(spawn_reader(
            kind.name(),
            DiagnosticStream::Stdout,
            stdout,
            diagnostics.clone(),
        ));
    }
    if let Some(stderr) = child.stderr.take() {
        readers.push(spawn_reader(
            kind.name(),
            DiagnosticStream::Stderr,
            stderr,
            diagnostics.clone(),
        ));
    }
    Ok((child, readers))
}

fn spawn_reader(
    source: &'static str,
    stream: DiagnosticStream,
    reader: impl std::io::Read + Send + 'static,
    diagnostics: mpsc::Sender<DevDiagnostic>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(reader).lines() {
            let Ok(text) = line else { break };
            if diagnostics
                .send(DevDiagnostic {
                    source,
                    stream,
                    text,
                })
                .is_err()
            {
                break;
            }
        }
    })
}

fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

fn terminate_process_group(child: &mut Child, grace: Duration) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }
    let process_group = -(child.id() as i32);
    // SAFETY: this child was created as the leader of a fresh process group.
    unsafe {
        libc::kill(process_group, libc::SIGTERM);
    }
    let deadline = Instant::now() + grace;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => break,
        }
    }
    // SAFETY: the direct child has not been reaped, so its process-group id
    // cannot have been recycled to an unrelated process group.
    unsafe {
        libc::kill(process_group, libc::SIGKILL);
    }
    let _ = child.wait();
}

extern "C" fn request_shutdown(_signal: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

fn install_shutdown_handlers() {
    SHUTDOWN_REQUESTED.store(false, Ordering::Release);
    // SAFETY: the handler performs only a lock-free atomic store, and these are
    // the process-wide termination signals owned by this standalone CLI.
    unsafe {
        libc::signal(libc::SIGINT, request_shutdown as libc::sighandler_t);
        libc::signal(libc::SIGTERM, request_shutdown as libc::sighandler_t);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_source() -> &'static str {
        r#"
workspace_root = "."
target_directory = "target/distill-dev"

[source_walk]
program = "cargo"
args = ["run", "--release", "-p", "source-walk", "--", "--module-crate", "game"]

[cargo]
program = "cargo"
args = []
pipeline_packages = ["game-assets-pipeline"]
gameplay_packages = ["game"]
features = ["rafx-vulkan"]
profile = "debug"
"#
    }

    #[test]
    fn parses_the_startup_only_launch_surface() {
        let temp = tempfile::tempdir().unwrap();
        let config =
            DevLaunchConfig::parse(temp.path().join("distill-dev.toml"), config_source()).unwrap();
        assert_eq!(
            config.workspace_root,
            std::fs::canonicalize(temp.path()).unwrap()
        );
        assert_eq!(
            config.target_directory,
            config.workspace_root.join("target/distill-dev")
        );
        assert_eq!(config.cargo.profile, BuildProfile::Debug);
        assert_eq!(config.cargo.gameplay_packages, ["game"]);
        let source = source_walk_command(&config);
        assert_eq!(
            &source.args[source.args.len() - 2..],
            [
                "--target-dir".to_owned(),
                config.target_directory.to_string_lossy().into_owned()
            ]
        );
        let cargo = cargo_watch_command(&config);
        assert_eq!(cargo.target_directory, Some(config.target_directory));
        assert_eq!(
            cargo.args.last().unwrap(),
            "build -p game-assets-pipeline -p game --features rafx-vulkan"
        );
    }

    #[test]
    fn rejects_generic_or_ambiguous_launch_inputs() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("distill-dev.toml");
        let no_pipeline = config_source().replace(
            "pipeline_packages = [\"game-assets-pipeline\"]",
            "pipeline_packages = []",
        );
        assert!(matches!(
            DevLaunchConfig::parse(&path, &no_pipeline),
            Err(DevLaunchConfigError::EmptyPipelinePackages)
        ));
        let shell_package = config_source().replace(
            "pipeline_packages = [\"game-assets-pipeline\"]",
            "pipeline_packages = [\"game; rm -rf target\"]",
        );
        assert!(matches!(
            DevLaunchConfig::parse(&path, &shell_package),
            Err(DevLaunchConfigError::InvalidPackage(_))
        ));
        let custom_profile = config_source().replace("profile = \"debug\"", "profile = \"fast\"");
        assert!(matches!(
            DevLaunchConfig::parse(&path, &custom_profile),
            Err(DevLaunchConfigError::InvalidProfile(_))
        ));
        let duplicate_target = config_source().replace(
            "\"--module-crate\", \"game\"",
            "\"--module-crate\", \"game\", \"--target-dir\", \"elsewhere\"",
        );
        assert!(matches!(
            DevLaunchConfig::parse(&path, &duplicate_target),
            Err(DevLaunchConfigError::ReservedSourceWalkArgument(
                "--target-dir"
            ))
        ));
    }

    #[test]
    fn only_source_walk_tempfail_requests_a_clean_restart() {
        assert!(is_requested_restart_code(
            ChildKind::SourceWalk,
            Some(REQUESTED_RESTART_EXIT_CODE)
        ));
        assert!(!is_requested_restart_code(
            ChildKind::CargoWatch,
            Some(REQUESTED_RESTART_EXIT_CODE)
        ));
        assert!(!is_requested_restart_code(ChildKind::SourceWalk, Some(1)));
        assert!(!is_requested_restart_code(ChildKind::SourceWalk, None));
    }

    #[test]
    fn process_group_teardown_reaps_a_long_lived_producer() {
        let (diagnostics, _rx) = mpsc::channel();
        let command = ChildCommand {
            program: PathBuf::from("sh"),
            args: vec!["-c".to_owned(), "sleep 30 & wait".to_owned()],
            cwd: std::env::current_dir().unwrap(),
            target_directory: None,
        };
        let mut child = ManagedChild::start(ChildKind::SourceWalk, command, diagnostics);
        assert!(child.child.is_some());
        child.terminate();
        assert!(child.child.is_none());
        assert!(child.readers.is_empty());
    }

    #[test]
    fn backoff_is_bounded_and_exponential() {
        assert_eq!(backoff(1), BASE_BACKOFF);
        assert_eq!(backoff(2), BASE_BACKOFF * 2);
        assert_eq!(backoff(32), MAX_BACKOFF);
    }

    #[test]
    fn path_like_programs_are_workspace_relative_but_path_names_are_not() {
        let workspace = Path::new("/workspace");
        assert_eq!(
            resolve_program(workspace, PathBuf::from("cargo")),
            Path::new("cargo")
        );
        assert_eq!(
            resolve_program(workspace, PathBuf::from("target/release/source-walk")),
            Path::new("/workspace/target/release/source-walk")
        );
        assert_eq!(
            resolve_program(workspace, PathBuf::from("/opt/bin/source-walk")),
            Path::new("/opt/bin/source-walk")
        );
    }

    #[test]
    fn os_string_program_names_remain_opaque() {
        let program = PathBuf::from(std::ffi::OsStr::new("cargo"));
        assert_eq!(
            resolve_program(Path::new("/workspace"), program),
            Path::new("cargo")
        );
    }
}
