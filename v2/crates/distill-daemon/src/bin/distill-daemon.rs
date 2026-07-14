use distill_daemon::config::DaemonConfig;
use distill_daemon::process::DaemonProcess;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .unwrap_or_else(|| "distill.toml".into());
    let config = DaemonConfig::load(path)?;
    let compiled = distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1()?
        .table()
        .compiled_table()?;
    let process = DaemonProcess::start(config, compiled)?;
    eprintln!("distill daemon listening on {}", process.rpc_address());
    process.wait()
}
