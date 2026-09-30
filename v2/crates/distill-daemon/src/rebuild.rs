//! Rebuild on save, for development.
//!
//! Each `[[rebuild]]` job runs its steps (typically `cargo build`, then
//! `source-walk`) whenever a file its last build read changes. The build's
//! dep-info (cargo's `<artifact>.d`) names those files: the crate's sources,
//! its path dependencies, `include_bytes!` inputs, manifests and lockfile.
//! The set follows each build, so a new `mod` is covered once it has been
//! built. The daemon adopts what the steps publish (the pipeline module, the
//! schema) as it adopts any other change to those files, and the engine
//! adopts a rebuilt game module the same way.
//!
//! A change is judged by content. A job hashes its inputs as it starts, and
//! only an input whose bytes then differ from that snapshot queues it again:
//! a step that rewrites one of its own job's inputs (source-walk's type-ops
//! table) queues one more round, and an identical rewrite queues none. Files
//! under the cargo target directory that holds the dep-info are never
//! inputs.
//!
//! One thread runs the jobs in configuration order, one step at a time. A
//! change to a running job's inputs ends its round (terminating the step)
//! and queues it again. A job whose dep-info is missing runs once at
//! startup. Steps run in the configuration file's directory with the
//! daemon's stdout and stderr; a failed step ends its round, and the next
//! change to an input starts another.
//!
//! The jobs follow the configuration: [`RebuildJobs::replace`] (the daemon
//! calls it for each accepted configuration) keeps an unchanged job as it
//! is, runs an added or changed one once, and drops a removed one. A removed
//! or changed job's running step is terminated.
//!
//! A step and everything it starts end with its round, and with the daemon.
//! On Unix the step leads a process group of its own, terminated as a
//! whole, and (on Linux) gets `SIGTERM` when the rebuild thread goes away.
//! On Windows the step starts suspended, joins a job object that kills its
//! processes when terminated or when its last handle closes (the daemon's),
//! and only then runs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use notify::{RecommendedWatcher, RecursiveMode, Watcher};

pub use crate::config::RebuildJob;

/// Editors save in bursts (write, rename, chmod); a job starts once its
/// inputs have been quiet this long.
const SETTLE: Duration = Duration::from_millis(30);

enum Message {
    Changed(Vec<PathBuf>),
    WatchFailed(String),
    /// A step's process exited (not yet reaped).
    StepExited(u32),
    /// The configuration's jobs, replacing the current ones.
    Jobs(Vec<RebuildJob>),
    Stop,
}

/// The rebuild thread. Dropping it stops the thread, terminating a running
/// step and everything it started.
pub struct Rebuilder {
    inbox: mpsc::Sender<Message>,
    thread: Option<JoinHandle<()>>,
}

/// Replaces a [`Rebuilder`]'s jobs; it may outlive the rebuilder.
#[derive(Clone)]
pub struct RebuildJobs {
    inbox: mpsc::Sender<Message>,
}

impl RebuildJobs {
    /// Adopt `jobs`, matched to the current ones by name.
    pub fn replace(&self, jobs: Vec<RebuildJob>) {
        let _ = self.inbox.send(Message::Jobs(jobs));
    }
}

impl Rebuilder {
    pub fn start(jobs: Vec<RebuildJob>) -> Self {
        let (inbox, messages) = mpsc::channel();
        let worker = Worker {
            jobs: jobs.into_iter().map(JobState::new).collect(),
            watcher: None,
            watched: BTreeSet::new(),
            queued: BTreeSet::new(),
            inbox: inbox.clone(),
            messages,
        };
        let thread = thread::Builder::new()
            .name("distill-rebuild".to_owned())
            .spawn(move || worker.run())
            .expect("spawn the rebuild thread");
        Self {
            inbox,
            thread: Some(thread),
        }
    }

    pub fn jobs(&self) -> RebuildJobs {
        RebuildJobs {
            inbox: self.inbox.clone(),
        }
    }
}

impl Drop for Rebuilder {
    fn drop(&mut self) {
        let _ = self.inbox.send(Message::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct JobState {
    job: RebuildJob,
    /// Where cargo writes; nothing under it is an input.
    target_dir: Option<PathBuf>,
    /// Each input's content as of the job's last start (`None`: missing).
    inputs: BTreeMap<PathBuf, Option<blake3::Hash>>,
}

impl JobState {
    fn new(job: RebuildJob) -> Self {
        let target_dir = job
            .dep_info
            .parent()
            .and_then(Path::parent)
            .map(simplified);
        let mut state = Self {
            job,
            target_dir,
            inputs: BTreeMap::new(),
        };
        state.snapshot();
        state
    }

    /// Re-read the dep-info and hash every input.
    fn snapshot(&mut self) {
        self.inputs = self
            .read_inputs()
            .unwrap_or_default()
            .into_iter()
            .map(|path| {
                let hash = content_hash(&path);
                (path, hash)
            })
            .collect();
    }

    fn read_inputs(&self) -> Option<Vec<PathBuf>> {
        let text = std::fs::read_to_string(&self.job.dep_info).ok()?;
        Some(
            parse_dep_info(&text)
                .into_iter()
                .map(|path| simplified(&path))
                .filter(|path| {
                    !self
                        .target_dir
                        .as_ref()
                        .is_some_and(|target| path.starts_with(target))
                })
                .collect(),
        )
    }

    /// Whether `path` is an input whose content differs from the snapshot.
    fn changed(&self, path: &Path) -> bool {
        self.inputs
            .get(path)
            .is_some_and(|recorded| *recorded != content_hash(path))
    }
}

struct Worker {
    jobs: Vec<JobState>,
    /// Made once some job has an input to watch.
    watcher: Option<RecommendedWatcher>,
    /// Directories watched (non-recursively) for the inputs they hold.
    watched: BTreeSet<PathBuf>,
    /// Jobs to run, by index.
    queued: BTreeSet<usize>,
    inbox: mpsc::Sender<Message>,
    messages: mpsc::Receiver<Message>,
}

/// The rebuild thread stops.
struct Stopped;

enum Step {
    Exited(std::io::Result<ExitStatus>),
    /// A change to the job's inputs queued it again.
    Superseded,
    /// The configuration removed or changed the job.
    Replaced,
}

impl Worker {
    fn run(mut self) {
        for (index, state) in self.jobs.iter().enumerate() {
            if !state.job.dep_info.exists() {
                self.queued.insert(index);
            }
        }
        self.watch_inputs();
        let _ = self.serve();
    }

    fn serve(&mut self) -> Result<(), Stopped> {
        loop {
            if self.queued.is_empty() {
                let message = self.messages.recv().map_err(|_| Stopped)?;
                self.receive(message)?;
                continue;
            }
            // Let the burst of events one save makes settle.
            loop {
                match self.messages.recv_timeout(SETTLE) {
                    Ok(message) => self.receive(message)?,
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => return Err(Stopped),
                }
            }
            let Some(index) = self.queued.pop_first() else {
                // A replacement dropped the queued jobs.
                continue;
            };
            self.run_job(index)?;
            self.watch_inputs();
        }
    }

    fn receive(&mut self, message: Message) -> Result<(), Stopped> {
        match message {
            Message::Changed(paths) => {
                for path in paths {
                    for (index, state) in self.jobs.iter().enumerate() {
                        if !self.queued.contains(&index) && state.changed(&path) {
                            tracing::info!(job = %state.job.name, path = %path.display(), "rebuild queued");
                            self.queued.insert(index);
                        }
                    }
                }
                Ok(())
            }
            Message::WatchFailed(error) => {
                tracing::warn!(%error, "rebuild watcher");
                Ok(())
            }
            // A step killed early reports after its reap.
            Message::StepExited(_) => Ok(()),
            Message::Jobs(jobs) => {
                self.replace_jobs(jobs);
                Ok(())
            }
            Message::Stop => Err(Stopped),
        }
    }

    /// Adopt a configuration's jobs: an unchanged job keeps its snapshot and
    /// its place in the queue; an added or changed one is queued to run once.
    fn replace_jobs(&mut self, jobs: Vec<RebuildJob>) {
        let queued = std::mem::take(&mut self.queued);
        let mut previous: BTreeMap<String, (usize, JobState)> = std::mem::take(&mut self.jobs)
            .into_iter()
            .enumerate()
            .map(|(index, state)| (state.job.name.clone(), (index, state)))
            .collect();
        for job in jobs {
            let index = self.jobs.len();
            match previous.remove(&job.name) {
                Some((old_index, state)) if state.job == job => {
                    if queued.contains(&old_index) {
                        self.queued.insert(index);
                    }
                    self.jobs.push(state);
                }
                replaced => {
                    let change = if replaced.is_some() { "changed" } else { "added" };
                    tracing::info!(job = %job.name, change, "rebuild job configured");
                    self.queued.insert(index);
                    self.jobs.push(JobState::new(job));
                }
            }
        }
        for name in previous.keys() {
            tracing::info!(job = %name, "rebuild job removed");
        }
        self.watch_inputs();
    }

    /// The index of `job` as configured now, if the configuration still has
    /// it unchanged.
    fn position(&self, job: &RebuildJob) -> Option<usize> {
        self.jobs.iter().position(|state| state.job == *job)
    }

    fn run_job(&mut self, index: usize) -> Result<(), Stopped> {
        self.jobs[index].snapshot();
        let job = self.jobs[index].job.clone();
        let started = Instant::now();
        tracing::info!(job = %job.name, "rebuild started");
        for step in &job.steps {
            let status = match self.run_step(&job, step)? {
                Step::Exited(Ok(status)) => status,
                Step::Superseded => {
                    tracing::info!(job = %job.name, step = %step.join(" "), "rebuild superseded by a newer change");
                    return Ok(());
                }
                Step::Replaced => {
                    tracing::info!(job = %job.name, step = %step.join(" "), "rebuild ended: the configuration replaced its job");
                    return Ok(());
                }
                Step::Exited(Err(error)) => {
                    tracing::warn!(job = %job.name, step = %step.join(" "), %error, "rebuild step did not start");
                    return Ok(());
                }
            };
            if !status.success() {
                tracing::warn!(job = %job.name, step = %step.join(" "), %status, "rebuild step failed");
                return Ok(());
            }
        }
        // The build may have read files the snapshot lacked (a new module).
        if let Some(index) = self.position(&job) {
            let state = &mut self.jobs[index];
            for path in state.read_inputs().unwrap_or_default() {
                state
                    .inputs
                    .entry(path)
                    .or_insert_with_key(|path| content_hash(path));
            }
        }
        tracing::info!(job = %job.name, elapsed_ms = started.elapsed().as_millis() as u64, "rebuild finished");
        Ok(())
    }

    /// Run one step of `job`, handling messages while it runs. A change that
    /// queues the job again, or a configuration without it, ends the step:
    /// its round is stale.
    fn run_step(&mut self, job: &RebuildJob, step: &[String]) -> Result<Step, Stopped> {
        let mut process = match StepProcess::spawn(job, step) {
            Ok(process) => process,
            Err(error) => return Ok(Step::Exited(Err(error))),
        };
        let id = process.id();
        let waiter = match process.exit_waiter() {
            Ok(waiter) => waiter,
            Err(error) => {
                process.terminate();
                return Ok(Step::Exited(Err(error)));
            }
        };
        let exited = self.inbox.clone();
        thread::Builder::new()
            .name("distill-rebuild-wait".to_owned())
            .spawn(move || {
                waiter();
                let _ = exited.send(Message::StepExited(id));
            })
            .expect("spawn a rebuild step waiter");
        loop {
            let message = match self.messages.recv() {
                Ok(message) => message,
                Err(_) => Message::Stop,
            };
            match message {
                Message::StepExited(exited) if exited == id => {
                    return Ok(Step::Exited(process.wait()));
                }
                Message::Stop => {
                    process.terminate();
                    return Err(Stopped);
                }
                message => {
                    self.receive(message)?;
                    match self.position(job) {
                        None => {
                            process.terminate();
                            return Ok(Step::Replaced);
                        }
                        Some(index) if self.queued.contains(&index) => {
                            process.terminate();
                            return Ok(Step::Superseded);
                        }
                        Some(_) => {}
                    }
                }
            }
        }
    }

    /// Watch the directory of every input (editors replace files by rename,
    /// which a watch on the file itself would lose), and only those.
    fn watch_inputs(&mut self) {
        let directories: BTreeSet<PathBuf> = self
            .jobs
            .iter()
            .flat_map(|state| state.inputs.keys())
            .filter_map(|path| path.parent().map(Path::to_path_buf))
            .collect();
        let stale: Vec<PathBuf> = self.watched.difference(&directories).cloned().collect();
        for directory in stale {
            if let Some(watcher) = &mut self.watcher {
                let _ = watcher.unwatch(&directory);
            }
            self.watched.remove(&directory);
        }
        if directories.is_empty() {
            return;
        }
        let watcher = match &mut self.watcher {
            Some(watcher) => watcher,
            None => {
                let events = self.inbox.clone();
                let watcher =
                    notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                        let _ = events.send(match event {
                            // Opens and reads change nothing, and hashing
                            // an input opens it.
                            Ok(event) if matches!(event.kind, notify::EventKind::Access(_)) => {
                                return
                            }
                            Ok(event) => Message::Changed(
                                event.paths.iter().map(|path| simplified(path)).collect(),
                            ),
                            Err(error) => Message::WatchFailed(error.to_string()),
                        });
                    });
                match watcher {
                    Ok(watcher) => self.watcher.insert(watcher),
                    Err(error) => {
                        tracing::warn!(%error, "rebuild watcher did not start");
                        return;
                    }
                }
            }
        };
        for directory in directories {
            if self.watched.contains(&directory) || !directory.is_dir() {
                continue;
            }
            match watcher.watch(&directory, RecursiveMode::NonRecursive) {
                Ok(()) => {
                    self.watched.insert(directory);
                }
                Err(error) => {
                    tracing::warn!(directory = %directory.display(), %error, "rebuild watch")
                }
            }
        }
    }
}

/// A running step.
struct StepProcess {
    child: Child,
    /// Holds the step's processes; closing it kills them.
    #[cfg(windows)]
    job: std::os::windows::io::OwnedHandle,
}

impl StepProcess {
    fn command(job: &RebuildJob, step: &[String]) -> Command {
        let (program, args) = step.split_first().expect("steps are validated non-empty");
        let has_separator = program.contains('/') || (cfg!(windows) && program.contains('\\'));
        let program = if has_separator {
            job.working_dir.join(program)
        } else {
            PathBuf::from(program)
        };
        let mut command = Command::new(program);
        command.args(args).current_dir(&job.working_dir);
        command
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.child.wait()
    }
}

#[cfg(unix)]
impl StepProcess {
    fn spawn(job: &RebuildJob, step: &[String]) -> std::io::Result<Self> {
        use std::os::unix::process::CommandExt;

        let mut command = Self::command(job, step);
        command.process_group(0);
        #[cfg(target_os = "linux")]
        // SAFETY: prctl is async-signal-safe; the closure allocates nothing.
        unsafe {
            // A step outlives neither the daemon nor this thread.
            command.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                Ok(())
            });
        }
        Ok(Self {
            child: command.spawn()?,
        })
    }

    /// Blocks until the step has exited, leaving it unreaped: until the
    /// reap, its process group id cannot be reused, so terminating the group
    /// cannot race its exit.
    fn exit_waiter(&self) -> std::io::Result<impl FnOnce() + Send + 'static> {
        let pid = self.child.id();
        Ok(move || loop {
            // SAFETY: `info` is a valid out-parameter for the call.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    pid as libc::id_t,
                    &mut info,
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            if result == 0
                || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
            {
                return;
            }
        })
    }

    /// End the step's process group and reap the step.
    fn terminate(&mut self) {
        // Not yet reaped, so the group id cannot have been reused.
        // SAFETY: kill takes no pointers.
        unsafe { libc::kill(-(self.child.id() as libc::pid_t), libc::SIGTERM) };
        let _ = self.child.wait();
    }
}

#[cfg(windows)]
impl StepProcess {
    fn spawn(job: &RebuildJob, step: &[String]) -> std::io::Result<Self> {
        use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
            SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        };
        use windows_sys::Win32::System::Threading::CREATE_SUSPENDED;

        // SAFETY: null attributes and name ask for an unnamed job whose
        // handle is not inherited.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: a fresh handle this function alone owns.
        let job_handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        // SAFETY: an all-zero limit structure is valid; only the flag is set.
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: `limits` is the structure the information class names, and
        // lives for the call.
        let set = unsafe {
            SetInformationJobObject(
                job_handle.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&limits).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        };
        if set == 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Suspended, so the step starts nothing outside the job.
        let mut command = Self::command(job, step);
        command.creation_flags(CREATE_SUSPENDED);
        let mut child = command.spawn()?;
        // SAFETY: both handles are open for the call.
        let assigned =
            unsafe { AssignProcessToJobObject(job_handle.as_raw_handle(), child.as_raw_handle()) };
        let started = if assigned == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            resume_threads(child.id())
        };
        if let Err(error) = started {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        Ok(Self {
            child,
            job: job_handle,
        })
    }

    /// Blocks until the step has exited. It waits on a handle of its own:
    /// the process object stays until every handle closes, and the job, not
    /// a process id, is what terminating addresses, so nothing races the
    /// exit.
    fn exit_waiter(&self) -> std::io::Result<impl FnOnce() + Send + 'static> {
        use std::os::windows::io::{AsHandle, AsRawHandle};
        use windows_sys::Win32::System::Threading::{WaitForSingleObject, INFINITE};

        let process = self.child.as_handle().try_clone_to_owned()?;
        Ok(move || {
            // SAFETY: `process` is open until the closure returns.
            unsafe { WaitForSingleObject(process.as_raw_handle(), INFINITE) };
        })
    }

    /// End every process in the step's job and reap the step.
    fn terminate(&mut self) {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;

        // SAFETY: the job handle is open.
        unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) };
        let _ = self.child.wait();
    }
}

/// Resume the threads of a process started suspended (just its main one).
#[cfg(windows)]
fn resume_threads(pid: u32) -> std::io::Result<()> {
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

    // SAFETY: a thread snapshot takes no pointers.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a fresh handle this function alone owns.
    let snapshot = unsafe { OwnedHandle::from_raw_handle(snapshot) };
    // SAFETY: an all-zero entry is valid once its size is set.
    let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
    let mut resumed = 0;
    // SAFETY: `entry` is a valid, sized out-parameter for each call.
    let mut more = unsafe { Thread32First(snapshot.as_raw_handle(), &mut entry) } != 0;
    while more {
        if entry.th32OwnerProcessID == pid {
            // SAFETY: plain handle-returning call.
            let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if thread.is_null() {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: a fresh handle this function alone owns.
            let thread = unsafe { OwnedHandle::from_raw_handle(thread) };
            // SAFETY: the thread handle is open with resume access.
            if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
                return Err(std::io::Error::last_os_error());
            }
            resumed += 1;
        }
        // SAFETY: as for Thread32First.
        more = unsafe { Thread32Next(snapshot.as_raw_handle(), &mut entry) } != 0;
    }
    if resumed == 0 {
        return Err(std::io::Error::other("the suspended step has no thread to resume"));
    }
    Ok(())
}

/// `path` without a Windows verbatim prefix (`\\?\C:\…` as `C:\…`), as
/// cargo's dep-info and the watcher's events spell it; unchanged elsewhere.
fn simplified(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};
        let mut components = path.components();
        if let Some(Component::Prefix(prefix)) = components.next() {
            let simple = match prefix.kind() {
                Prefix::VerbatimDisk(disk) => Some(format!("{}:", disk as char)),
                Prefix::VerbatimUNC(server, share) => Some(format!(
                    r"\\{}\{}",
                    server.to_string_lossy(),
                    share.to_string_lossy()
                )),
                _ => None,
            };
            if let Some(simple) = simple {
                let mut result = PathBuf::from(simple);
                result.push(std::path::MAIN_SEPARATOR_STR);
                for component in components {
                    if !matches!(component, Component::RootDir) {
                        result.push(component);
                    }
                }
                return result;
            }
        }
    }
    path.to_path_buf()
}

fn content_hash(path: &Path) -> Option<blake3::Hash> {
    std::fs::read(path).ok().map(|bytes| blake3::hash(&bytes))
}

/// The prerequisites of every rule in a make-style dep-info file. Cargo
/// escapes a space in a path as `\ ` and writes every other character,
/// Windows separators included, as is.
fn parse_dep_info(text: &str) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for line in text.lines() {
        let Some(prerequisites) = split_rule(line) else {
            continue;
        };
        let mut current = String::new();
        let mut chars = prerequisites.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\\' if chars.peek() == Some(&' ') => current.extend(chars.next()),
                c if c.is_whitespace() => {
                    if !current.is_empty() {
                        paths.push(PathBuf::from(std::mem::take(&mut current)));
                    }
                }
                c => current.push(c),
            }
        }
        if !current.is_empty() {
            paths.push(PathBuf::from(current));
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

/// The text after a rule's unescaped `: `.
fn split_rule(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    (0..bytes.len().saturating_sub(1)).find_map(|index| {
        let escaped = index > 0 && bytes[index - 1] == b'\\';
        (bytes[index] == b':' && bytes[index + 1] == b' ' && !escaped)
            .then(|| &line[index + 2..])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A step running `script` in the platform's shell.
    fn shell(script: &str) -> Vec<String> {
        if cfg!(windows) {
            vec!["cmd".to_owned(), "/C".to_owned(), script.to_owned()]
        } else {
            vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()]
        }
    }

    /// A script appending one line to `file`.
    fn append_line(file: &str) -> Vec<String> {
        shell(&format!("echo x>> {file}"))
    }

    /// A script waiting a second, then appending `source` to `file`.
    fn slow_copy(source: &str, file: &str) -> Vec<String> {
        if cfg!(windows) {
            shell(&format!("ping -n 2 127.0.0.1 >nul & type {source} >> {file}"))
        } else {
            shell(&format!("sleep 1; cat {source} >> {file}"))
        }
    }

    fn lines(path: &Path) -> usize {
        std::fs::read_to_string(path).map_or(0, |text| text.lines().count())
    }

    fn wait_for(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + deadline;
        while !done() {
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(20));
        }
        true
    }

    /// A job over `<name>.rs` in `dir`, its dep-info in `dir/target/debug`.
    fn job(dir: &Path, name: &str, step: Vec<String>) -> RebuildJob {
        let source = dir.join(format!("{name}.rs"));
        if !source.exists() {
            std::fs::write(&source, "1").unwrap();
        }
        let dep_info = dir.join(format!("target/debug/lib{name}.d"));
        std::fs::create_dir_all(dep_info.parent().unwrap()).unwrap();
        std::fs::write(&dep_info, dep_info_line(&format!("lib{name}.rlib"), &source)).unwrap();
        RebuildJob {
            name: name.to_owned(),
            dep_info,
            steps: vec![step],
            working_dir: dir.to_path_buf(),
        }
    }

    /// A dep-info rule as cargo writes it (spaces escaped).
    fn dep_info_line(target: &str, source: &Path) -> String {
        format!(
            "{target}: {}\n",
            source.display().to_string().replace(' ', "\\ ")
        )
    }

    #[test]
    fn dep_info_prerequisites_unescape_spaces() {
        let text = "/t/debug/liba.rlib: /s/a.rs /s/with\\ space.rs\n\n/s/a.rs:\n";
        assert_eq!(
            parse_dep_info(text),
            vec![PathBuf::from("/s/a.rs"), PathBuf::from("/s/with space.rs")],
        );
    }

    #[test]
    fn dep_info_keeps_windows_separators() {
        let text = "D:\\t\\debug\\a.dll: D:\\s\\a.rs D:\\s\\with\\ space.rs\n";
        assert_eq!(
            parse_dep_info(text),
            vec![
                PathBuf::from("D:\\s\\a.rs"),
                PathBuf::from("D:\\s\\with space.rs")
            ],
        );
    }

    #[cfg(windows)]
    #[test]
    fn verbatim_paths_are_simplified() {
        assert_eq!(
            simplified(Path::new(r"\\?\D:\Projects\a.rs")),
            PathBuf::from(r"D:\Projects\a.rs")
        );
        assert_eq!(
            simplified(Path::new(r"D:\Projects\a.rs")),
            PathBuf::from(r"D:\Projects\a.rs")
        );
    }

    #[test]
    fn a_rewrite_with_the_same_content_is_no_change() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("lib.rs");
        std::fs::write(&source, "fn a() {}").unwrap();
        let target = temp.path().join("target");
        std::fs::create_dir_all(target.join("debug")).unwrap();
        let generated = target.join("debug/out.rs");
        std::fs::write(&generated, "").unwrap();
        let dep_info = target.join("debug/liba.d");
        std::fs::write(
            &dep_info,
            format!(
                "{}: {} {}\n",
                target.join("debug/liba.rlib").display(),
                source.display(),
                generated.display()
            ),
        )
        .unwrap();
        let state = JobState::new(RebuildJob {
            name: "a".to_owned(),
            dep_info,
            steps: vec![vec!["true".to_owned()]],
            working_dir: temp.path().to_path_buf(),
        });
        assert_eq!(state.inputs.len(), 1, "target-dir files are not inputs");
        std::fs::write(&source, "fn a() {}").unwrap();
        assert!(!state.changed(&source));
        std::fs::write(&source, "fn b() {}").unwrap();
        assert!(state.changed(&source));
        assert!(!state.changed(&generated));
    }

    #[test]
    fn saving_an_input_runs_the_steps() {
        let temp = tempfile::tempdir().unwrap();
        let ran = temp.path().join("ran");
        let _rebuilder = Rebuilder::start(vec![job(temp.path(), "a", append_line("ran"))]);
        // The watch is installed on the rebuild thread; give it a moment.
        thread::sleep(Duration::from_millis(200));
        assert!(!ran.exists(), "nothing ran before a change");
        std::fs::write(temp.path().join("a.rs"), "2").unwrap();
        assert!(wait_for(Duration::from_secs(10), || ran.exists()));
        thread::sleep(Duration::from_millis(200));
        assert_eq!(lines(&ran), 1, "one save, one run");
    }

    #[test]
    fn reading_an_input_does_not_hold_off_other_jobs() {
        // Hashing an input opens it; were opens changes, reading b.rs would
        // keep job b hashing it forever and job a would never settle.
        let temp = tempfile::tempdir().unwrap();
        let _rebuilder = Rebuilder::start(vec![
            job(temp.path(), "a", append_line("ran-a")),
            job(temp.path(), "b", append_line("ran-b")),
        ]);
        thread::sleep(Duration::from_millis(200));
        std::fs::read(temp.path().join("b.rs")).unwrap();
        thread::sleep(Duration::from_millis(100));
        std::fs::write(temp.path().join("a.rs"), "2").unwrap();
        let ran = temp.path().join("ran-a");
        assert!(wait_for(Duration::from_secs(10), || ran.exists()), "job a ran");
        assert!(!temp.path().join("ran-b").exists(), "a read is no change");
    }

    #[test]
    fn a_change_during_a_round_supersedes_it() {
        let temp = tempfile::tempdir().unwrap();
        let ran = temp.path().join("ran");
        let _rebuilder = Rebuilder::start(vec![job(temp.path(), "a", slow_copy("a.rs", "ran"))]);
        thread::sleep(Duration::from_millis(200));
        std::fs::write(temp.path().join("a.rs"), "2").unwrap();
        thread::sleep(Duration::from_millis(400));
        std::fs::write(temp.path().join("a.rs"), "3").unwrap();
        assert!(wait_for(Duration::from_secs(10), || ran.exists()));
        thread::sleep(Duration::from_millis(1500));
        assert_eq!(
            std::fs::read_to_string(&ran).unwrap().trim(),
            "3",
            "the first round was terminated before it wrote",
        );
    }

    #[test]
    fn replaced_jobs_are_added_changed_and_removed_by_name() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        let a = job(dir, "a", append_line("ran-a"));
        let rebuilder = Rebuilder::start(vec![a.clone()]);
        thread::sleep(Duration::from_millis(200));

        // Unchanged a stays idle; added b runs once.
        rebuilder
            .jobs()
            .replace(vec![a.clone(), job(dir, "b", append_line("ran-b"))]);
        assert!(wait_for(Duration::from_secs(10), || dir.join("ran-b").exists()));
        thread::sleep(Duration::from_millis(200));
        assert!(!dir.join("ran-a").exists(), "an unchanged job does not run");
        assert_eq!(lines(&dir.join("ran-b")), 1);

        // A save to b.rs runs b, now watched.
        std::fs::write(dir.join("b.rs"), "2").unwrap();
        assert!(wait_for(Duration::from_secs(10), || lines(&dir.join("ran-b")) == 2));

        // a removed, b changed: b runs its new step once; a.rs is no input.
        rebuilder
            .jobs()
            .replace(vec![job(dir, "b", append_line("ran-b2"))]);
        assert!(wait_for(Duration::from_secs(10), || dir.join("ran-b2").exists()));
        std::fs::write(dir.join("a.rs"), "2").unwrap();
        thread::sleep(Duration::from_millis(500));
        assert!(!dir.join("ran-a").exists(), "a removed job does not run");
        assert_eq!(lines(&dir.join("ran-b")), 2, "b's old step is gone");
        assert_eq!(lines(&dir.join("ran-b2")), 1);
    }

    #[test]
    fn removing_a_running_job_ends_its_step() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        let a = job(dir, "a", slow_copy("a.rs", "ran"));
        let rebuilder = Rebuilder::start(vec![a]);
        thread::sleep(Duration::from_millis(200));
        std::fs::write(dir.join("a.rs"), "2").unwrap();
        thread::sleep(Duration::from_millis(400));
        rebuilder.jobs().replace(Vec::new());
        thread::sleep(Duration::from_millis(2000));
        assert!(!dir.join("ran").exists(), "the step was terminated");
    }
}
