use daimon_alerts::{model::Config, store::Store};
use std::{net::SocketAddr, path::Path, sync::Arc};

fn load(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = std::fs::metadata(path)?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err("configuration must be owner-private".into());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("daimon-alerts: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 3 {
        return Err("usage: daimon-alerts serve CONFIG | backup CONFIG DESTINATION".into());
    }
    let config = load(&args[2]).map_err(|_| "cannot load private configuration")?;
    let addr: SocketAddr = config.listen.parse().map_err(|_| "invalid listener")?;
    if !addr.ip().is_loopback() {
        return Err("local runtime requires loopback; use the authorized remote gateway".into());
    }
    // State files including WAL and backups inherit owner-only permissions.
    unsafe {
        libc_umask(0o077);
    }
    let store = Arc::new(Store::open(config).map_err(|_| "cannot open durable state")?);
    match args[1].as_str() {
        "serve" if args.len() == 3 => {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|_| "cannot bind listener")?;
            axum::serve(listener, daimon_alerts::router(store))
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await
                .map_err(|_| "listener stopped unexpectedly")?;
        }
        "backup" if args.len() == 4 => store
            .backup(Path::new(&args[3]))
            .map_err(|_| "backup failed; retain source and inspect destination")?,
        _ => return Err("invalid command".into()),
    }
    Ok(())
}
unsafe extern "C" {
    #[link_name = "umask"]
    fn libc_umask(mask: u32) -> u32;
}
