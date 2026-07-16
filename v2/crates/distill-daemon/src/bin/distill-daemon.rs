use std::path::PathBuf;

use distill_core::id::AssetUuid;
use distill_daemon::config::DaemonConfig;
use distill_daemon::dev::{DevLaunchConfig, DevSupervisor};
use distill_daemon::pack_command::build_configured_pack;
use distill_daemon::process::DaemonProcess;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let Some(first) = args.next() else {
        return run_daemon(PathBuf::from("distill.toml"));
    };
    if first == "dev" {
        let daemon_config = args
            .next()
            .ok_or("usage: distilld dev <daemon-config> <development-launch-config>")?;
        let launch_config = args
            .next()
            .ok_or("usage: distilld dev <daemon-config> <development-launch-config>")?;
        if args.next().is_some() {
            return Err("usage: distilld dev <daemon-config> <development-launch-config>".into());
        }
        let daemon_config = DaemonConfig::load(daemon_config)?;
        let launch_config = DevLaunchConfig::load(launch_config)?;
        return DevSupervisor::start(daemon_config, launch_config)
            .run()
            .map_err(Into::into);
    }
    if first == "pack" {
        let config_path = args
            .next()
            .ok_or("usage: distilld pack <config-path> <definition-uuid> <output-directory>")?;
        let definition = args
            .next()
            .ok_or("usage: distilld pack <config-path> <definition-uuid> <output-directory>")?;
        let destination = args
            .next()
            .ok_or("usage: distilld pack <config-path> <definition-uuid> <output-directory>")?;
        if args.next().is_some() {
            return Err(
                "usage: distilld pack <config-path> <definition-uuid> <output-directory>".into(),
            );
        }
        let definition: AssetUuid = definition
            .to_str()
            .ok_or("definition UUID is not UTF-8")?
            .parse()?;
        let config = DaemonConfig::load(config_path)?;
        let output = build_configured_pack(config, definition, &PathBuf::from(destination))?;
        eprintln!(
            "activated pack manifest {} with archive {}",
            distill_pack::manifest_filename(distill_pack::manifest_hash(&output.manifest_bytes)),
            distill_pack::archive_filename(output.archive_file_hash)
        );
        return Ok(());
    }
    if args.next().is_some() {
        return Err("usage: distilld [config-path]".into());
    }
    run_daemon(PathBuf::from(first))
}

fn run_daemon(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let config = DaemonConfig::load(path)?;
    let process = DaemonProcess::start(config)?;
    eprintln!("distill daemon listening on {}", process.rpc_address());
    process.wait()
}
