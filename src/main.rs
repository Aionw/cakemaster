mod logging;

use cakemaster::server::{DEFAULT_MOONCAKE_LISTEN_ADDR, MooncakeServerConfig};
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use logging::{LoggingConfig, LoggingOverrides};
use std::error::Error;
use std::future::Future;
use std::io;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Listen address for the Mooncake RPC server.
    #[arg(
        long,
        default_value_t = DEFAULT_MOONCAKE_LISTEN_ADDR,
        value_name = "ADDRESS",
        help_heading = "Server options"
    )]
    listen: std::net::SocketAddr,

    /// Emit one structured info log for each completed RPC request.
    #[arg(long, help_heading = "Server options")]
    access_log: bool,

    #[command(flatten, next_help_heading = "Logging options")]
    logging: LoggingOverrides,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let logging = match LoggingConfig::resolve(cli.logging) {
        Ok(logging) => logging,
        Err(error) => Cli::command().error(ErrorKind::InvalidValue, error).exit(),
    };
    if let Err(error) = logging::init(&logging) {
        eprintln!("{error}");
        std::process::exit(1);
    }
    let server = MooncakeServerConfig::default()
        .with_listen_addr(cli.listen)
        .with_access_log(cli.access_log);
    if let Err(error) = run(server).await {
        log::error!(error:% = error; "cakemaster exited with an error");
        eprintln!("error: {error}");
        log::logger().flush();
        std::process::exit(1);
    }
    log::logger().flush();
}

async fn run(config: MooncakeServerConfig) -> Result<(), Box<dyn Error>> {
    let bound = config.build()?.bind().await?;
    let local_addr = bound.local_addr()?;
    log::info!(listen_addr:% = local_addr; "cakemaster is ready");
    println!("cakemaster_ready={local_addr}");
    bound.run_until(shutdown_signal()?).await?;
    log::info!("cakemaster stopped");
    Ok(())
}

#[cfg(unix)]
fn shutdown_signal() -> io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    log::error!(error:% = error; "failed to listen for Ctrl-C");
                }
            }
            _ = terminate.recv() => {}
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> io::Result<impl Future<Output = ()>> {
    Ok(async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            log::error!(error:% = error; "failed to listen for Ctrl-C");
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_loopback_and_accepts_an_explicit_listener() {
        let default = Cli::try_parse_from(["cakemaster"]).unwrap();
        assert_eq!(default.listen, DEFAULT_MOONCAKE_LISTEN_ADDR);
        assert!(!default.access_log);
        assert_eq!(default.logging, LoggingOverrides::default());

        let explicit = Cli::try_parse_from(["cakemaster", "--listen", "127.0.0.1:0"]).unwrap();
        assert_eq!(explicit.listen, "127.0.0.1:0".parse().unwrap());

        let access_log = Cli::try_parse_from(["cakemaster", "--access-log"]).unwrap();
        assert!(access_log.access_log);
    }

    #[test]
    fn accepts_logging_options_and_module_filters() {
        let cli = Cli::try_parse_from([
            "cakemaster",
            "--log-filter=info,cakemaster::server=debug",
            "--log-dir",
            "/var/log/cakemaster",
            "--log-output=file",
        ])
        .unwrap();
        assert_eq!(
            cli.logging.log_filter.as_deref(),
            Some("info,cakemaster::server=debug")
        );
        assert_eq!(
            cli.logging.log_dir.as_deref(),
            Some(std::path::Path::new("/var/log/cakemaster"))
        );
        assert_eq!(cli.logging.log_output, Some(logging::LogOutput::File));
    }

    #[test]
    fn rejects_missing_invalid_duplicate_and_unknown_arguments() {
        assert!(Cli::try_parse_from(["cakemaster", "--listen"]).is_err());
        assert!(Cli::try_parse_from(["cakemaster", "--listen=localhost:50051"]).is_err());
        assert!(
            Cli::try_parse_from([
                "cakemaster",
                "--listen",
                "127.0.0.1:1",
                "--listen",
                "127.0.0.1:2"
            ])
            .is_err()
        );
        assert!(Cli::try_parse_from(["cakemaster", "--log-level=verbose"]).is_err());
        assert!(Cli::try_parse_from(["cakemaster", "--log-filter=cakemaster=verbose"]).is_err());
        assert!(Cli::try_parse_from(["cakemaster", "--log-output=stdout"]).is_err());
        assert!(
            Cli::try_parse_from([
                "cakemaster",
                "--log-level=info",
                "--log-filter=cakemaster=debug"
            ])
            .is_err()
        );
        assert!(Cli::try_parse_from(["cakemaster", "server"]).is_err());
    }
}
