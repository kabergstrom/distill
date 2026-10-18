//! `distilld codegen <config> [--check]` end to end: the fixture pipeline
//! module's codegen (`distill_pipeline_fixture::BINDINGS`) over two roots,
//! each with its own `[[codegen.output]]`, without a running daemon.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::SystemTime;

use distill_daemon::config::DaemonConfig;
use distill_daemon::process::DaemonProcess;

struct Project {
    _temp: tempfile::TempDir,
    dir: PathBuf,
    config: PathBuf,
}

impl Project {
    /// Roots `main` (`a.num` = 12) and `extra` (`b.num` = 34), each
    /// generated into `gen/<root>` by the fixture codegen.
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().canonicalize().unwrap();
        let (main, extra) = (dir.join("main"), dir.join("extra"));
        std::fs::create_dir_all(&main).unwrap();
        std::fs::create_dir_all(&extra).unwrap();
        // The schema and the built fixture module, as the test project
        // configures them.
        let base = distill_test_project::write_configuration(&dir, &main, false);
        let toml_path = |path: &Path| path.display().to_string().replace('\\', "/");
        let source = format!(
            r#"
[daemon]
address = "127.0.0.1:0"
state_path = '{state}'
[assets]
roots = {{ main = '{main}', extra = '{extra}' }}
schema_path = '{schema}'
[modules]
pipeline_dylib = '{module}'
[targets.dev]
os = "{os}"
arch = "{arch}"
apis = ["vulkan"]
[[codegen.output]]
root = "main"
rs_mod_path = "gen/main"
codegen = "{codegen}"
[[codegen.output]]
root = "extra"
rs_mod_path = "gen/extra"
codegen = "{codegen}"
[pipeline]
parallelism = 2
batch_reserved_workers = 1
[cas]
segment_size = "1MiB"
cache_limit = "16MiB"
"#,
            state = toml_path(&dir.join(".distill")),
            main = toml_path(&main),
            extra = toml_path(&extra),
            schema = toml_path(&base.assets.schema_path),
            module = toml_path(&base.modules.pipeline_dylib),
            os = std::env::consts::OS,
            arch = std::env::consts::ARCH,
            codegen = distill_pipeline_fixture::BINDINGS,
        );
        let config = dir.join("distill.toml");
        std::fs::write(&config, source).unwrap();
        std::fs::write(main.join("a.num"), "12").unwrap();
        std::fs::write(extra.join("b.num"), "34").unwrap();
        Self {
            _temp: temp,
            dir,
            config,
        }
    }

    fn codegen(&self, check: bool) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_distilld"));
        command.arg("codegen").arg(&self.config);
        if check {
            command.arg("--check");
        }
        command.output().unwrap()
    }

    fn generated(&self, root: &str) -> PathBuf {
        self.dir.join("gen").join(root)
    }

    /// Every generated file, by output and name, with its modification
    /// time.
    fn files(&self) -> Vec<(String, SystemTime)> {
        let mut files = Vec::new();
        for root in ["main", "extra"] {
            let Ok(entries) = std::fs::read_dir(self.generated(root)) else {
                continue;
            };
            for entry in entries {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_file() {
                    files.push((
                        format!("{root}/{}", entry.file_name().to_str().unwrap()),
                        entry.metadata().unwrap().modified().unwrap(),
                    ));
                }
            }
        }
        files.sort();
        files
    }

    /// `mod.rs` of `root`, without its mark line.
    fn module(&self, root: &str) -> String {
        let text = std::fs::read_to_string(self.generated(root).join("mod.rs")).unwrap();
        text.split_once('\n').unwrap().1.to_owned()
    }
}

fn lines(output: &Output) -> Vec<String> {
    let mut lines = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    lines.sort();
    lines
}

fn describe(output: &Output) -> String {
    format!(
        "status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn codegen_command_generates_each_roots_namespace_and_check_reports_drift() {
    let project = Project::new();

    // Nothing generated yet: --check names every file it would create and
    // writes none.
    let check = project.codegen(true);
    assert_eq!(check.status.code(), Some(1), "{}", describe(&check));
    let would = lines(&check);
    assert_eq!(would.len(), 4, "{}", describe(&check));
    assert!(would.iter().all(|line| line.starts_with("create ")), "{would:?}");
    assert!(project.files().is_empty());

    // A run writes both namespaces, each from its own root only.
    let run = project.codegen(false);
    assert_eq!(run.status.code(), Some(0), "{}", describe(&run));
    assert_eq!(lines(&run), would);
    let main = project.module("main");
    assert!(main.starts_with("#![allow(dead_code)]\n\n#[doc(hidden)]\npub mod sp_"), "{main}");
    assert!(main.contains(" as tens_1;\n"), "{main}");
    assert!(main.ends_with("pub const ROOT: &str = \"main\";\npub const COUNT: usize = 1;\n"), "{main}");
    let extra = project.module("extra");
    assert!(extra.contains(" as tens_3;\n"), "{extra}");
    assert!(!extra.contains("tens_1"), "{extra}");
    assert!(extra.ends_with("pub const ROOT: &str = \"extra\";\npub const COUNT: usize = 1;\n"), "{extra}");

    // Current: --check is quiet and succeeds.
    let check = project.codegen(true);
    assert_eq!(check.status.code(), Some(0), "{}", describe(&check));
    assert!(lines(&check).is_empty());

    // A body-only edit (12 -> 13, the same tens) re-imports a.num but
    // generates the same bytes: nothing is written.
    let before = project.files();
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(project.dir.join("main/a.num"), "13").unwrap();
    let run = project.codegen(false);
    assert_eq!(run.status.code(), Some(0), "{}", describe(&run));
    assert!(lines(&run).is_empty(), "{}", describe(&run));
    assert_eq!(project.files(), before);

    // A binding edit (13 -> 25): the unit and mod.rs of main, not extra.
    std::fs::write(project.dir.join("main/a.num"), "25").unwrap();
    let check = project.codegen(true);
    assert_eq!(check.status.code(), Some(1), "{}", describe(&check));
    let would = lines(&check);
    assert_eq!(would.len(), 2, "{would:?}");
    assert!(
        would
            .iter()
            .all(|line| line.starts_with("update ") && line.contains("/gen/main/")),
        "{would:?}"
    );
    assert_eq!(project.files(), before);

    // A second asset of main aliased tens_2 collides: the run fails naming
    // both claimants and writes nothing, in either namespace.
    std::fs::write(project.dir.join("main/c.num"), "27").unwrap();
    let run = project.codegen(false);
    assert_eq!(run.status.code(), Some(2), "{}", describe(&run));
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(stderr.contains("NamespaceCollision"), "{stderr}");
    assert!(stderr.contains("tens_2"), "{stderr}");
    assert!(stderr.matches("AssetUuid").count() >= 2, "{stderr}");
    assert_eq!(project.files(), before);

    // While a daemon serves the configuration, the command is refused.
    std::fs::remove_file(project.dir.join("main/c.num")).unwrap();
    let daemon = DaemonProcess::start(DaemonConfig::load(&project.config).unwrap()).unwrap();
    let refused = project.codegen(true);
    assert_eq!(refused.status.code(), Some(2), "{}", describe(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("a daemon is running"),
        "{}",
        describe(&refused)
    );
    drop(daemon);
}
