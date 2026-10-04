use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use distill_core::id::AssetUuid;
use distill_daemon::bootstrap;
use distill_daemon::config::DaemonConfig;
use distill_daemon::pack_command::build_configured_pack;
use distill_daemon::process::DaemonProcess;
use distill_rpc::{AuthoringValue, ImportRequest};

// Pipeline modules allocate with System and hand those allocations to the
// daemon (see distill-pipeline-api).
#[global_allocator]
static SYSTEM: std::alloc::System = std::alloc::System;

const USAGE: &str = "usage:
  distilld [config-path]
  distilld import <config-path> <source> <dest-bundle> --importer <id> --settings <json>
                  [--root <name>] [--target <name>] [--no-watch] [--if-missing | --if-changed] [--wait <seconds>]
  distilld engine-args <config-path> [target]
  distilld pack <config-path> <definition-uuid> <output-directory>";

/// How long `import` waits for a starting daemon by default.
const DEFAULT_IMPORT_WAIT: Duration = Duration::from_secs(120);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let mut args = std::env::args_os().skip(1);
    let Some(first) = args.next() else {
        return run_daemon(PathBuf::from("distill.toml"));
    };
    let rest: Vec<OsString> = args.collect();
    match first.to_str() {
        Some("import") => import(rest),
        Some("engine-args") => engine_args(rest),
        Some("pack") => pack(rest),
        Some("-h" | "--help") => {
            println!("{USAGE}");
            Ok(())
        }
        _ if rest.is_empty() => run_daemon(PathBuf::from(first)),
        _ => Err(USAGE.into()),
    }
}

fn run_daemon(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let config = DaemonConfig::load(path)?;
    let process = DaemonProcess::start(config)?;
    eprintln!("distill daemon listening on {}", process.rpc_address());
    process.wait()
}

fn engine_args(args: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    let (config, target) = match args.as_slice() {
        [config] => (config, None),
        [config, target] => (config, Some(utf8(target)?)),
        _ => return Err(USAGE.into()),
    };
    let config = DaemonConfig::load(config)?;
    println!("{}", bootstrap::engine_args(&config, target)?.join(" "));
    Ok(())
}

fn import(args: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    let mut positional = Vec::new();
    let mut importer = None;
    let mut settings = None;
    let mut root = "main".to_owned();
    let mut target = None;
    let mut watch = true;
    let mut if_missing = false;
    let mut if_changed = false;
    let mut wait = DEFAULT_IMPORT_WAIT;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let mut value = || -> Result<String, Box<dyn std::error::Error>> {
            Ok(utf8(args.next().ok_or(USAGE)?)?.to_owned())
        };
        match arg.to_str() {
            Some("--importer") => importer = Some(value()?),
            Some("--settings") => settings = Some(value()?),
            Some("--root") => root = value()?,
            Some("--target") => target = Some(value()?),
            Some("--wait") => wait = Duration::from_secs_f64(value()?.parse()?),
            Some("--no-watch") => watch = false,
            Some("--if-missing") => if_missing = true,
            Some("--if-changed") => if_changed = true,
            _ => positional.push(arg),
        }
    }
    let [config, source, dest] = positional.as_slice() else {
        return Err(USAGE.into());
    };
    let (Some(importer), Some(settings)) = (importer, settings) else {
        return Err(USAGE.into());
    };
    let config = DaemonConfig::load(config)?;
    let (source, dest) = (utf8(source)?, utf8(dest)?);
    if if_missing {
        let root_path = config
            .assets
            .roots
            .get(&root)
            .ok_or_else(|| format!("unknown asset root {root}"))?;
        if root_path.join(dest).exists() {
            eprintln!("{dest}: already imported");
            return Ok(());
        }
    }
    // The hub takes canonical authored-value text.
    let settings = distill_json::write(&distill_json::parse(&settings)?)?;
    let request = ImportRequest {
        importer,
        sources: vec![source.to_owned()],
        dest: dest.to_owned(),
        settings: AuthoringValue {
            canonical_value: Arc::from(settings.as_bytes()),
            blobs: Vec::new(),
        },
        watch,
        root,
        if_changed,
    };
    let address = config.daemon.address;
    if address.port() == 0 {
        return Err("daemon.address needs a fixed port for import to find the daemon".into());
    }
    let bundle = bootstrap::import(&config, address, target.as_deref(), &request, wait)?;
    eprintln!("{dest}: imported as bundle {bundle}");
    Ok(())
}

fn pack(args: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    let [config_path, definition, destination] = args.as_slice() else {
        return Err(USAGE.into());
    };
    let definition: AssetUuid = utf8(definition)?.parse()?;
    let config = DaemonConfig::load(config_path)?;
    // A client of the running daemon, like import.
    let output = match build_configured_pack(&config, definition, &PathBuf::from(destination)) {
        Ok(output) => output,
        Err(error) => {
            eprintln!("distilld pack: {error}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "activated pack manifest {} with archive {}",
        distill_pack::manifest_filename(distill_pack::manifest_hash(&output.manifest_bytes)),
        distill_pack::archive_filename(output.archive_file_hash)
    );
    Ok(())
}

fn utf8(value: &OsString) -> Result<&str, Box<dyn std::error::Error>> {
    value
        .to_str()
        .ok_or_else(|| format!("argument {value:?} is not UTF-8").into())
}
