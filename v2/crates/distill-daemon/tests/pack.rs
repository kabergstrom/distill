//! `distilld pack` is a client of the running daemon. The complete pack of
//! real assets runs in game_assets_e2e; these cover the command's edges
//! without a pipeline.

use std::time::{Duration, Instant};

use distill_core::id::{AssetUuid, BundleUuid};
use distill_daemon::config::DaemonConfig;
use distill_daemon::pack_command::{build_configured_pack, PackCommandError};
use distill_daemon::process::DaemonProcess;
use distill_schema::ngp_schema::{LayoutIdentity, Schema, SchemaLayouts};

#[path = "support/pack_definition.rs"]
mod pack_definition;

fn config(temp: &tempfile::TempDir) -> DaemonConfig {
    let assets = temp.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let schema = Schema {
        source_hashes: Default::default(),
        type_ops_hash: String::new(),
        layout_hashes: Default::default(),
        rustc_version: String::new(),
        types: Vec::new(),
        layouts: vec![SchemaLayouts {
            identity: LayoutIdentity {
                target_triple: "aarch64-apple-darwin".into(),
                rustc: "rustc test".into(),
                algorithm_version: 1,
            },
            layouts: Vec::new(),
        }],
    };
    std::fs::write(
        temp.path().join("schema.json"),
        serde_json::to_vec(&schema).unwrap(),
    )
    .unwrap();
    let path = temp.path().join("distill.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[daemon]
address = "127.0.0.1:0"
state_path = '{}'
[assets]
roots = {{ main = '{}' }}
schema_path = '{}'
[modules]
pipeline_dylib = '{}'
[targets.dev]
os = "macos"
arch = "aarch64"
apis = ["vulkan"]
optimize = false
[codegen]
rs_mod_path = '{}'
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
        ),
    )
    .unwrap();
    DaemonConfig::load(path).unwrap()
}

#[test]
fn pack_runs_against_the_serving_daemon_instead_of_opening_its_state() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp);
    let process = DaemonProcess::start(config.clone()).unwrap();
    let mut client_config = config.clone();
    client_config.daemon.address = process.rpc_address();

    let definition = AssetUuid([0x61; 16]);
    std::fs::write(
        temp.path().join("assets/game.bundle"),
        pack_definition::pack_definition_bundle(
            BundleUuid([0x60; 16]),
            definition,
            "dev",
            &[AssetUuid([0x62; 16])],
            true,
        ),
    )
    .unwrap();
    let output = tempfile::tempdir().unwrap();

    // The definition is inspected over the metadata hub once the watcher
    // publishes it. This fixture has no pipeline, so the target connection
    // is where the command stops: past the point an in-process pack failed
    // with StateLocked.
    let deadline = Instant::now() + Duration::from_secs(10);
    let error = loop {
        match build_configured_pack(&client_config, definition, output.path()) {
            Err(PackCommandError::MissingDefinition(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(error) => break error,
            Ok(output) => panic!("a pack without a pipeline: {output:?}"),
        }
    };
    match &error {
        PackCommandError::Connect(message) => {
            assert!(message.contains("PipelineUnavailable"), "{message}")
        }
        other => panic!("expected the target connection to fail: {other:?}"),
    }
    assert!(!error.to_string().contains("StateLocked"));
    assert!(process.last_background_error().is_none());
    assert_eq!(std::fs::read_dir(output.path()).unwrap().count(), 0);
}

#[test]
fn pack_output_inside_an_asset_root_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp);
    let nested = temp.path().join("assets/packs");
    std::fs::create_dir_all(&nested).unwrap();
    for output in [nested, temp.path().join("assets")] {
        let error = build_configured_pack(&config, AssetUuid([1; 16]), &output).unwrap_err();
        assert!(
            matches!(&error, PackCommandError::OutputInsideAssetRoot { root, .. } if root == "main"),
            "{error:?}"
        );
        assert!(error.to_string().contains("inside asset root `main`"), "{error}");
    }
}

#[test]
fn pack_without_a_daemon_says_to_start_one() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(&temp);
    // A port nothing listens on: bind one, then release it.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    config.daemon.address = listener.local_addr().unwrap();
    drop(listener);
    let output = tempfile::tempdir().unwrap();
    let error = build_configured_pack(&config, AssetUuid([1; 16]), output.path()).unwrap_err();
    assert!(
        matches!(error, PackCommandError::NoDaemon { .. }),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .starts_with(&format!("no distilld at {}; start it first", config.daemon.address)),
        "{error}"
    );
}
