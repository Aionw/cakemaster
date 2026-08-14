use cakemaster::server::{DEFAULT_MOONCAKE_LISTEN_ADDR, MooncakeServerConfig};
use std::error::Error;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Debug, Error)]
enum CliError {
    #[error("command-line argument is not valid UTF-8")]
    NonUtf8,
    #[error("--listen requires an address")]
    MissingListenAddress,
    #[error("--listen was specified more than once")]
    DuplicateListenAddress,
    #[error("invalid --listen address {value:?}: {source}")]
    InvalidListenAddress {
        value: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("unknown argument {0:?}")]
    UnknownArgument(String),
}

enum Command {
    Run(MooncakeServerConfig),
    Help,
}

#[tokio::main]
async fn main() {
    init_logging();
    if let Err(error) = run().await {
        log::error!(error:% = error; "cakemaster exited with an error");
        eprintln!("error: {error}");
        print_usage();
        log::logger().flush();
        std::process::exit(1);
    }
    log::logger().flush();
}

async fn run() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args_os()
        .skip(1)
        .map(|argument| argument.into_string().map_err(|_| CliError::NonUtf8))
        .collect::<Result<Vec<_>, _>>()?;
    let config = match parse_args(arguments)? {
        Command::Run(config) => config,
        Command::Help => {
            print_usage();
            return Ok(());
        }
    };

    let bound = config.build()?.bind().await?;
    let local_addr = bound.local_addr()?;
    log::info!(listen_addr:% = local_addr; "cakemaster is ready");
    println!("cakemaster_ready={local_addr}");
    bound.run_until(shutdown_signal()?).await?;
    log::info!("cakemaster stopped");
    Ok(())
}

fn parse_args(arguments: impl IntoIterator<Item = String>) -> Result<Command, CliError> {
    let mut listen_addr = DEFAULT_MOONCAKE_LISTEN_ADDR;
    let mut listen_seen = false;
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "--listen" => {
                let value = arguments.next().ok_or(CliError::MissingListenAddress)?;
                set_listen_addr(&mut listen_addr, &mut listen_seen, value)?;
            }
            _ if argument.starts_with("--listen=") => {
                let value = argument["--listen=".len()..].to_owned();
                if value.is_empty() {
                    return Err(CliError::MissingListenAddress);
                }
                set_listen_addr(&mut listen_addr, &mut listen_seen, value)?;
            }
            _ => return Err(CliError::UnknownArgument(argument)),
        }
    }
    Ok(Command::Run(
        MooncakeServerConfig::default().with_listen_addr(listen_addr),
    ))
}

fn set_listen_addr(
    listen_addr: &mut SocketAddr,
    listen_seen: &mut bool,
    value: String,
) -> Result<(), CliError> {
    if *listen_seen {
        return Err(CliError::DuplicateListenAddress);
    }
    *listen_addr = value
        .parse()
        .map_err(|source| CliError::InvalidListenAddress { value, source })?;
    *listen_seen = true;
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

fn init_logging() {
    let stderr = logforth::append::asynchronous::AsyncBuilder::new("cakemaster-log")
        .buffered_lines_limit(Some(8_192))
        .overflow_block()
        .append(logforth::append::Stderr::default())
        .build();
    let logger = logforth::core::builder()
        .dispatch(|dispatch| dispatch.append(stderr))
        .build();
    let bridge = logforth::bridge::log::LogBridge::new(logger);
    log::set_boxed_logger(Box::new(bridge)).expect("global logger must not already be installed");
    log::set_max_level(log::LevelFilter::Info);
}

fn print_usage() {
    eprintln!("usage: cakemaster [--listen ADDRESS]");
    eprintln!("default listen address: {DEFAULT_MOONCAKE_LISTEN_ADDR}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_loopback_and_accepts_an_explicit_listener() {
        let Command::Run(default) = parse_args(Vec::new()).unwrap() else {
            panic!("empty arguments must run the server");
        };
        assert_eq!(default.listen_addr(), DEFAULT_MOONCAKE_LISTEN_ADDR);

        let Command::Run(config) =
            parse_args(["--listen", "127.0.0.1:0"].map(str::to_owned)).unwrap()
        else {
            panic!("listen arguments must run the server");
        };
        assert_eq!(config.listen_addr(), "127.0.0.1:0".parse().unwrap());
    }

    #[test]
    fn rejects_missing_invalid_duplicate_and_unknown_arguments() {
        assert!(matches!(
            parse_args(["--listen".to_owned()]),
            Err(CliError::MissingListenAddress)
        ));
        assert!(matches!(
            parse_args(["--listen=localhost:50051".to_owned()]),
            Err(CliError::InvalidListenAddress { .. })
        ));
        assert!(matches!(
            parse_args(["--listen", "127.0.0.1:1", "--listen", "127.0.0.1:2"].map(str::to_owned)),
            Err(CliError::DuplicateListenAddress)
        ));
        assert!(matches!(
            parse_args(["server".to_owned()]),
            Err(CliError::UnknownArgument(_))
        ));
    }
}
