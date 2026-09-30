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
//! startup. Steps run in the
//! configuration file's directory, in a process group of their own, with the
//! daemon's stdout and stderr; a failed step ends its round, and the next
//! change to an input starts another.

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
    Stop,
}

/// The rebuild thread. Dropping it stops the thread, terminating a running
/// step's process group.
pub struct Rebuilder {
    inbox: mpsc::Sender<Message>,
    thread: Option<JoinHandle<()>>,
}

impl Rebuilder {
    pub fn start(jobs: Vec<RebuildJob>) -> Result<Self, notify::Error> {
        let (inbox, messages) = mpsc::channel();
        let events = inbox.clone();
        let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            let _ = events.send(match event {
                // Opens and reads change nothing, and hashing an input opens it.
                Ok(event) if matches!(event.kind, notify::EventKind::Access(_)) => return,
                Ok(event) => Message::Changed(event.paths),
                Err(error) => Message::WatchFailed(error.to_string()),
            });
        })?;
        let worker = Worker {
            jobs: jobs.into_iter().map(JobState::new).collect(),
            watcher,
            watched: BTreeSet::new(),
            queued: BTreeSet::new(),
            inbox: inbox.clone(),
            messages,
        };
        let thread = thread::Builder::new()
            .name("distill-rebuild".to_owned())
            .spawn(move || worker.run())
            .expect("spawn the rebuild thread");
        Ok(Self {
            inbox,
            thread: Some(thread),
        })
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
            .map(Path::to_path_buf);
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
    watcher: RecommendedWatcher,
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
    Superseded,
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
            let index = self.queued.pop_first().expect("queued is not empty");
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
            Message::Stop => Err(Stopped),
        }
    }

    fn run_job(&mut self, index: usize) -> Result<(), Stopped> {
        self.jobs[index].snapshot();
        let job = self.jobs[index].job.clone();
        let started = Instant::now();
        tracing::info!(job = %job.name, "rebuild started");
        for step in &job.steps {
            let status = match self.run_step(index, &job, step)? {
                Step::Exited(Ok(status)) => status,
                Step::Superseded => {
                    tracing::info!(job = %job.name, step = %step.join(" "), "rebuild superseded by a newer change");
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
        let state = &mut self.jobs[index];
        for path in state.read_inputs().unwrap_or_default() {
            state.inputs.entry(path).or_insert_with_key(|path| content_hash(path));
        }
        tracing::info!(job = %job.name, elapsed_ms = started.elapsed().as_millis() as u64, "rebuild finished");
        Ok(())
    }

    /// Run one step of job `index`, handling messages while it runs. A change
    /// that queues the job again ends the step: its round is stale.
    fn run_step(
        &mut self,
        index: usize,
        job: &RebuildJob,
        step: &[String],
    ) -> Result<Step, Stopped> {
        let mut child = match spawn(job, step) {
            Ok(child) => child,
            Err(error) => return Ok(Step::Exited(Err(error))),
        };
        let pid = child.id();
        let exited = self.inbox.clone();
        thread::Builder::new()
            .name("distill-rebuild-wait".to_owned())
            .spawn(move || {
                wait_exited(pid);
                let _ = exited.send(Message::StepExited(pid));
            })
            .expect("spawn a rebuild step waiter");
        loop {
            let message = match self.messages.recv() {
                Ok(message) => message,
                Err(_) => Message::Stop,
            };
            match message {
                Message::StepExited(exited) if exited == pid => {
                    return Ok(Step::Exited(child.wait()));
                }
                Message::Stop => {
                    terminate(pid, &mut child);
                    return Err(Stopped);
                }
                message => {
                    self.receive(message)?;
                    if self.queued.contains(&index) {
                        terminate(pid, &mut child);
                        return Ok(Step::Superseded);
                    }
                }
            }
        }
    }

    /// Watch the directory of every input (editors replace files by rename,
    /// which a watch on the file itself would lose).
    fn watch_inputs(&mut self) {
        let directories: BTreeSet<PathBuf> = self
            .jobs
            .iter()
            .flat_map(|state| state.inputs.keys())
            .filter_map(|path| path.parent().map(Path::to_path_buf))
            .collect();
        for directory in directories {
            if self.watched.contains(&directory) || !directory.is_dir() {
                continue;
            }
            match self.watcher.watch(&directory, RecursiveMode::NonRecursive) {
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

/// End a step's process group and reap the step.
fn terminate(pid: u32, child: &mut Child) {
    // Not yet reaped, so the group id cannot have been reused.
    // SAFETY: kill takes no pointers.
    unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGTERM) };
    let _ = child.wait();
}

fn spawn(job: &RebuildJob, step: &[String]) -> std::io::Result<Child> {
    use std::os::unix::process::CommandExt;

    let (program, args) = step.split_first().expect("steps are validated non-empty");
    let program = if program.contains('/') {
        job.working_dir.join(program)
    } else {
        PathBuf::from(program)
    };
    let mut command = Command::new(program);
    command.args(args).current_dir(&job.working_dir).process_group(0);
    #[cfg(target_os = "linux")]
    // SAFETY: prctl is async-signal-safe; the closure allocates nothing.
    unsafe {
        // A step outlives neither the daemon nor this thread.
        command.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
    command.spawn()
}

/// Block until `pid` has exited, leaving it unreaped.
fn wait_exited(pid: u32) {
    loop {
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
        if result == 0 || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
        {
            return;
        }
    }
}

fn content_hash(path: &Path) -> Option<blake3::Hash> {
    std::fs::read(path).ok().map(|bytes| blake3::hash(&bytes))
}

/// The prerequisites of every rule in a make-style dep-info file. A
/// backslash escapes the next character (cargo escapes spaces).
fn parse_dep_info(text: &str) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for line in text.lines() {
        let Some(prerequisites) = split_rule(line) else {
            continue;
        };
        let mut current = String::new();
        let mut chars = prerequisites.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => current.extend(chars.next()),
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

    #[test]
    fn dep_info_prerequisites_unescape_spaces() {
        let text = "/t/debug/liba.rlib: /s/a.rs /s/with\\ space.rs\n\n/s/a.rs:\n";
        assert_eq!(
            parse_dep_info(text),
            vec![PathBuf::from("/s/a.rs"), PathBuf::from("/s/with space.rs")],
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
        let source = temp.path().join("lib.rs");
        std::fs::write(&source, "fn a() {}").unwrap();
        let dep_info = temp.path().join("target/debug/liba.d");
        std::fs::create_dir_all(dep_info.parent().unwrap()).unwrap();
        std::fs::write(&dep_info, format!("liba.rlib: {}\n", source.display())).unwrap();
        let ran = temp.path().join("ran");
        let _rebuilder = Rebuilder::start(vec![RebuildJob {
            name: "a".to_owned(),
            dep_info,
            steps: vec![vec!["sh".to_owned(), "-c".to_owned(), "echo >> ran".to_owned()]],
            working_dir: temp.path().to_path_buf(),
        }])
        .unwrap();
        // The watch is installed on the rebuild thread; give it a moment.
        thread::sleep(Duration::from_millis(200));
        assert!(!ran.exists(), "nothing ran before a change");
        std::fs::write(&source, "fn b() {}").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ran.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(std::fs::read_to_string(&ran).unwrap(), "\n", "one save, one run");
    }

    #[test]
    fn reading_an_input_does_not_hold_off_other_jobs() {
        // Hashing an input opens it; were opens changes, reading b.rs would
        // keep job b hashing it forever and job a would never settle.
        let temp = tempfile::tempdir().unwrap();
        let job = |name: &str| {
            let source = temp.path().join(format!("{name}.rs"));
            std::fs::write(&source, "1").unwrap();
            let dep_info = temp.path().join(format!("target/debug/lib{name}.d"));
            std::fs::create_dir_all(dep_info.parent().unwrap()).unwrap();
            std::fs::write(&dep_info, format!("lib{name}.rlib: {}\n", source.display())).unwrap();
            RebuildJob {
                name: name.to_owned(),
                dep_info,
                steps: vec![vec!["sh".to_owned(), "-c".to_owned(), format!("echo >> ran-{name}")]],
                working_dir: temp.path().to_path_buf(),
            }
        };
        let _rebuilder = Rebuilder::start(vec![job("a"), job("b")]).unwrap();
        thread::sleep(Duration::from_millis(200));
        std::fs::read(temp.path().join("b.rs")).unwrap();
        thread::sleep(Duration::from_millis(100));
        std::fs::write(temp.path().join("a.rs"), "2").unwrap();
        let ran = temp.path().join("ran-a");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ran.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(ran.exists(), "job a ran");
        assert!(!temp.path().join("ran-b").exists(), "a read is no change");
    }

    #[test]
    fn a_change_during_a_round_supersedes_it() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("lib.rs");
        std::fs::write(&source, "1").unwrap();
        let dep_info = temp.path().join("target/debug/liba.d");
        std::fs::create_dir_all(dep_info.parent().unwrap()).unwrap();
        std::fs::write(&dep_info, format!("liba.rlib: {}\n", source.display())).unwrap();
        let ran = temp.path().join("ran");
        let _rebuilder = Rebuilder::start(vec![RebuildJob {
            name: "a".to_owned(),
            dep_info,
            steps: vec![vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "sleep 1; cat lib.rs >> ran".to_owned(),
            ]],
            working_dir: temp.path().to_path_buf(),
        }])
        .unwrap();
        thread::sleep(Duration::from_millis(200));
        std::fs::write(&source, "2").unwrap();
        thread::sleep(Duration::from_millis(400));
        std::fs::write(&source, "3").unwrap();
        thread::sleep(Duration::from_millis(2500));
        assert_eq!(
            std::fs::read_to_string(&ran).unwrap(),
            "3",
            "the first round was terminated before it wrote",
        );
    }
}
