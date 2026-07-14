use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use distill_daemon::config::DaemonConfig;
use distill_daemon::process::DaemonProcess;
use distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1;
use distill_store::state::PipelineState;

fn config(temp: &tempfile::TempDir) -> DaemonConfig {
    let assets = temp.path().join("assets");
    std::fs::create_dir(&assets).unwrap();
    let source = format!(
        r#"
[daemon]
address = "127.0.0.1:0"
state_path = "{}"
[assets]
roots = {{ main = "{}" }}
schema_path = "{}"
lineage_manifest = {{ root = "main", path = "schema/lineage.bundle" }}
[modules]
pipeline_dylib = "{}"
[targets.dev]
os = "linux"
arch = "x86_64"
apis = ["vulkan"]
[codegen]
rs_mod_path = "{}"
auto_codegen = false
[pipeline]
parallelism = 2
max_dependency_depth = 64
batch_reserved_workers = 1
[cas]
segment_size = "1MiB"
cache_limit = "8MiB"
"#,
        temp.path().join("state").display(),
        assets.display(),
        temp.path().join("schema.json").display(),
        temp.path().join("pipeline.so").display(),
        temp.path().join("generated").display(),
    );
    DaemonConfig::parse(temp.path().join("distill.toml"), &source).unwrap()
}

#[test]
fn process_serves_rpc_and_consumes_watcher_changes_until_drop() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = consumer_bootstrap_authority_v1()
        .unwrap()
        .table()
        .compiled_table()
        .unwrap();
    let process = DaemonProcess::start(config(&temp), compiled).unwrap();
    let mut socket = TcpStream::connect(process.rpc_address()).unwrap();
    socket.write_all(&[]).unwrap();

    let before = process.coordinator().server().current_stamp().version.0;
    assert!(matches!(
        process
            .coordinator()
            .store()
            .lock()
            .unwrap()
            .pipeline_state()
            .unwrap(),
        Some(PipelineState::Poisoned { .. })
    ));

    std::fs::write(temp.path().join("assets/source.txt"), b"source").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while process.coordinator().server().current_stamp().version.0 <= before {
        assert!(Instant::now() < deadline, "watcher batch did not publish");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(matches!(
        process
            .coordinator()
            .store()
            .lock()
            .unwrap()
            .pipeline_state()
            .unwrap(),
        Some(PipelineState::Poisoned { .. })
    ));
    assert!(process.last_background_error().is_none());
    drop(process);
}
