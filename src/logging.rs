use std::env;
use std::ffi::OsString;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::ValueEnum;
use thiserror::Error;

const DEFAULT_LOG_DIRECTORY: &str = "logs";
const LOG_FILE_BASENAME: &str = "cakemaster";
const LOG_FILE_SUFFIX: &str = "log";
const LOG_ROLLOVER_SIZE: usize = 100 * 1024 * 1024;
const LOG_FILE_LIMIT: usize = 14;

const RUST_LOG_ENV: &str = "RUST_LOG";
const LOG_LEVEL_ENV: &str = "CAKEMASTER_LOG_LEVEL";
const LOG_DIRECTORY_ENV: &str = "CAKEMASTER_LOG_DIR";
const LOG_OUTPUT_ENV: &str = "CAKEMASTER_LOG_OUTPUT";

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum LogOutput {
    Stderr,
    File,
    Both,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
    All,
}

impl LogLevel {
    fn as_filter(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
            Self::All => "all",
        }
    }
}

#[derive(clap::Args, Debug, Default, Eq, PartialEq)]
pub(crate) struct LoggingOverrides {
    /// Set one global log level.
    #[arg(long, value_enum, conflicts_with = "log_filter", value_name = "LEVEL")]
    pub(crate) log_level: Option<LogLevel>,

    /// Set RUST_LOG-style global and per-module directives.
    #[arg(
        long,
        conflicts_with = "log_level",
        value_name = "FILTER",
        value_parser = parse_filter_argument
    )]
    pub(crate) log_filter: Option<String>,

    /// Set the rolling log directory.
    #[arg(
        long,
        value_name = "PATH",
        value_parser = parse_directory_argument
    )]
    pub(crate) log_dir: Option<PathBuf>,

    /// Select stderr, rolling file, or both outputs.
    #[arg(long, value_enum, value_name = "OUTPUT")]
    pub(crate) log_output: Option<LogOutput>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct LoggingConfig {
    filter: String,
    directory: PathBuf,
    output: LogOutput,
}

#[derive(Debug, Error)]
pub(crate) enum LoggingConfigError {
    #[error("invalid log level {0:?}; expected off, error, warn, info, debug, trace, or all")]
    InvalidLevel(String),
    #[error("invalid log filter {value:?}: {reason}")]
    InvalidFilter { value: String, reason: String },
    #[error("log directory must not be empty")]
    EmptyDirectory,
    #[error("invalid log output {0:?}; expected stderr, file, or both")]
    InvalidOutput(String),
    #[error("environment variable {0} is not valid UTF-8")]
    NonUtf8Environment(&'static str),
    #[error("failed to initialize logging: {0}")]
    Initialize(#[from] logforth::Error),
}

impl LoggingConfig {
    pub(crate) fn resolve(overrides: LoggingOverrides) -> Result<Self, LoggingConfigError> {
        let LoggingOverrides {
            log_level,
            log_filter,
            log_dir,
            log_output,
        } = overrides;
        let override_filter =
            log_filter.or_else(|| log_level.map(|level| level.as_filter().to_owned()));
        let filter = match override_filter {
            Some(filter) => filter,
            None => {
                let rust_log = environment_string(RUST_LOG_ENV)?;
                let level = if rust_log.is_none() {
                    environment_string(LOG_LEVEL_ENV)?
                } else {
                    None
                };
                resolve_filter(None, rust_log, level)?
            }
        };

        let directory = match log_dir {
            Some(directory) => directory,
            None => match env::var_os(LOG_DIRECTORY_ENV) {
                Some(directory) => parse_directory(directory)?,
                None => PathBuf::from(DEFAULT_LOG_DIRECTORY),
            },
        };

        let output = match log_output {
            Some(output) => output,
            None => match environment_string(LOG_OUTPUT_ENV)? {
                Some(output) => parse_output(&output)?,
                None => LogOutput::Both,
            },
        };

        Ok(Self {
            filter,
            directory,
            output,
        })
    }
}

pub(crate) fn parse_level(value: &str) -> Result<String, LoggingConfigError> {
    LogLevel::from_str(value.trim(), true)
        .map(|level| level.as_filter().to_owned())
        .map_err(|_| LoggingConfigError::InvalidLevel(value.to_owned()))
}

pub(crate) fn parse_filter(value: &str) -> Result<String, LoggingConfigError> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(LoggingConfigError::InvalidFilter {
            value: value.to_owned(),
            reason: "filter must not be empty".to_owned(),
        });
    }
    logforth::filter::rustlog::RustLogFilterBuilder::try_from_spec(normalized).map_err(
        |error| LoggingConfigError::InvalidFilter {
            value: value.to_owned(),
            reason: error.to_string(),
        },
    )?;
    Ok(normalized.to_owned())
}

pub(crate) fn parse_directory(value: OsString) -> Result<PathBuf, LoggingConfigError> {
    if value.is_empty() {
        return Err(LoggingConfigError::EmptyDirectory);
    }
    Ok(PathBuf::from(value))
}

pub(crate) fn parse_output(value: &str) -> Result<LogOutput, LoggingConfigError> {
    LogOutput::from_str(value.trim(), true)
        .map_err(|_| LoggingConfigError::InvalidOutput(value.to_owned()))
}

fn parse_filter_argument(value: &str) -> Result<String, String> {
    parse_filter(value).map_err(|error| error.to_string())
}

fn parse_directory_argument(value: &str) -> Result<PathBuf, String> {
    parse_directory(OsString::from(value)).map_err(|error| error.to_string())
}

pub(crate) fn init(config: &LoggingConfig) -> Result<(), LoggingConfigError> {
    let filter =
        logforth::filter::rustlog::RustLogFilterBuilder::try_from_spec(&config.filter)?.build();
    let asynchronous = logforth::append::asynchronous::AsyncBuilder::new("cakemaster-log")
        .buffered_lines_limit(Some(8_192))
        .overflow_block();
    let asynchronous = match config.output {
        LogOutput::Stderr => asynchronous.append(stderr_appender()),
        LogOutput::File => asynchronous.append(file_appender(config)?),
        LogOutput::Both => asynchronous
            .append(stderr_appender())
            .append(file_appender(config)?),
    }
    .build();
    let logger = logforth::core::builder()
        .dispatch(|dispatch| dispatch.filter(filter).append(asynchronous))
        .build();
    let bridge = logforth::bridge::log::LogBridge::new(logger);
    log::set_boxed_logger(Box::new(bridge)).expect("global logger must not already be installed");
    // Module-specific filters may enable any level, so filtering must happen in logforth.
    log::set_max_level(log::LevelFilter::Trace);
    Ok(())
}

fn stderr_appender() -> logforth::append::Stderr {
    logforth::append::Stderr::default().with_layout(logforth::layout::TextLayout::default())
}

fn file_appender(config: &LoggingConfig) -> Result<logforth::append::File, logforth::Error> {
    logforth::append::file::FileBuilder::new(config.directory.clone(), LOG_FILE_BASENAME)
        .filename_suffix(LOG_FILE_SUFFIX)
        .rollover_daily()
        .rollover_size(NonZeroUsize::new(LOG_ROLLOVER_SIZE).unwrap())
        .max_log_files(NonZeroUsize::new(LOG_FILE_LIMIT).unwrap())
        .layout(logforth::layout::TextLayout::default().no_color())
        .build()
}

fn environment_string(name: &'static str) -> Result<Option<String>, LoggingConfigError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(LoggingConfigError::NonUtf8Environment(name)),
    }
}

fn resolve_filter(
    override_filter: Option<String>,
    rust_log: Option<String>,
    level: Option<String>,
) -> Result<String, LoggingConfigError> {
    if let Some(filter) = override_filter {
        return parse_filter(&filter);
    }
    if let Some(filter) = rust_log {
        return parse_filter(&filter);
    }
    match level {
        Some(level) => parse_level(&level),
        None => Ok("info".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_sources_follow_cli_environment_and_default_precedence() {
        assert_eq!(
            resolve_filter(
                Some("warn".to_owned()),
                Some("trace".to_owned()),
                Some("debug".to_owned())
            )
            .unwrap(),
            "warn"
        );
        assert_eq!(
            resolve_filter(None, Some("info,cakemaster::server=debug".to_owned()), None).unwrap(),
            "info,cakemaster::server=debug"
        );
        assert_eq!(
            resolve_filter(None, None, Some("WARN".to_owned())).unwrap(),
            "warn"
        );
        assert_eq!(resolve_filter(None, None, None).unwrap(), "info");
    }

    #[test]
    fn validates_logging_values() {
        assert_eq!(parse_level("DEBUG").unwrap(), "debug");
        assert!(parse_level("verbose").is_err());
        assert!(parse_filter("info,cakemaster::server=trace").is_ok());
        assert!(parse_filter("info,cakemaster=verbose").is_err());
        assert_eq!(parse_output("STDERR").unwrap(), LogOutput::Stderr);
        assert!(parse_output("stdout").is_err());
        assert!(parse_directory(OsString::new()).is_err());
    }
}
