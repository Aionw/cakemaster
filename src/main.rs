mod logging;

use cakemaster::object::reclamation::CollectBudget;
use cakemaster::object::{DEFAULT_EXPECTED_OBJECTS, ObjectCatalogConfig};
use cakemaster::segment::SegmentPoolConfig;
use cakemaster::segment::config::DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT;
use cakemaster::server::{
    DEFAULT_MOONCAKE_LISTEN_ADDR, DEFAULT_OBJECT_COLLECTION_BUDGET, DEFAULT_RECONCILE_INTERVAL,
    MasterReconcileConfig, MooncakeServerConfig,
};
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use logging::{LoggingConfig, LoggingOverrides};
use mimalloc::MiMalloc;
use std::error::Error;
use std::num::NonZeroUsize;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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

    /// Maximum offset-allocator metadata nodes created for each mounted segment.
    ///
    /// Size this for the maximum number of simultaneously live slices, plus
    /// headroom for free regions. It does not limit the segment's byte capacity.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT,
        value_name = "COUNT",
        help_heading = "Server options"
    )]
    max_allocator_nodes_per_segment: u32,

    /// Expected maximum number of object slots retained in the catalog.
    ///
    /// This is a capacity hint, not a hard limit. Size it for peak indexed
    /// objects, including empty slots retained during their grace period, to
    /// avoid concurrent hash-table growth pauses on the request path.
    #[arg(
        long,
        default_value_t = NonZeroUsize::new(DEFAULT_EXPECTED_OBJECTS)
            .expect("the default expected object count is non-zero"),
        value_name = "COUNT",
        help_heading = "Server options"
    )]
    expected_objects: NonZeroUsize,

    /// Maximum candidates scanned and retired objects reclaimed per background step.
    ///
    /// Larger values let watermark eviction converge faster, but each step can
    /// occupy the collector for longer. The allocation-failure request path
    /// keeps its separate, smaller bounded budget.
    #[arg(
        long,
        default_value_t = NonZeroUsize::new(DEFAULT_OBJECT_COLLECTION_BUDGET.max_candidates())
            .expect("the default object collection budget is non-zero"),
        value_name = "COUNT",
        help_heading = "Server options"
    )]
    object_collection_budget_per_step: NonZeroUsize,

    /// Fixed owner shards for catalog, eviction state, and segment allocator arenas.
    #[arg(
        long,
        default_value_t = default_metadata_shards(),
        value_name = "COUNT",
        help_heading = "Server options"
    )]
    metadata_shards: NonZeroUsize,

    /// Emit one structured info log for each completed RPC request.
    #[arg(long, help_heading = "Server options")]
    access_log: bool,

    #[command(flatten, next_help_heading = "Logging options")]
    logging: LoggingOverrides,
}

impl Cli {
    fn server_config(&self) -> MooncakeServerConfig {
        let collection_budget = self.object_collection_budget_per_step.get();
        let reconcile = MasterReconcileConfig::new(
            DEFAULT_RECONCILE_INTERVAL,
            CollectBudget::new(
                collection_budget,
                collection_budget,
                DEFAULT_OBJECT_COLLECTION_BUDGET.max_empty_slots(),
            ),
        )
        .expect("the production reconcile interval is non-zero");
        MooncakeServerConfig::default()
            .with_listen_addr(self.listen)
            .with_segment_pool(SegmentPoolConfig::new(self.max_allocator_nodes_per_segment))
            .with_object_catalog(ObjectCatalogConfig::new(self.expected_objects.get()))
            .with_metadata_shards(self.metadata_shards.get())
            .with_reconcile(reconcile)
            .with_access_log(self.access_log)
    }
}

fn default_metadata_shards() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN)
}

fn main() {
    let cli = Cli::parse();
    let server = cli.server_config();
    let logging = match LoggingConfig::resolve(cli.logging) {
        Ok(logging) => logging,
        Err(error) => Cli::command().error(ErrorKind::InvalidValue, error).exit(),
    };
    if let Err(error) = logging::init(&logging) {
        eprintln!("{error}");
        std::process::exit(1);
    }
    let result = compio::runtime::Runtime::new()
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })
        .and_then(|runtime| runtime.block_on(run(server)));
    if let Err(error) = result {
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
    bound.run_until(shutdown_signal()).await?;
    log::info!("cakemaster stopped");
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    tokio::select! {
        result = compio::signal::ctrl_c() => {
            if let Err(error) = result {
                log::error!(error:% = error; "failed to listen for Ctrl-C");
            }
        }
        result = compio::signal::unix::signal(15) => {
            if let Err(error) = result {
                log::error!(error:% = error; "failed to listen for SIGTERM");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_tuning_options_use_production_defaults() {
        let cli = Cli::try_parse_from(["cakemaster"]).unwrap();

        assert_eq!(
            cli.max_allocator_nodes_per_segment,
            DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT
        );
        assert_eq!(
            cli.object_collection_budget_per_step.get(),
            DEFAULT_OBJECT_COLLECTION_BUDGET.max_candidates()
        );
        assert_eq!(cli.expected_objects.get(), DEFAULT_EXPECTED_OBJECTS);
        assert_eq!(cli.metadata_shards, default_metadata_shards());
    }

    #[test]
    fn allocator_node_limit_accepts_an_explicit_count() {
        let cli =
            Cli::try_parse_from(["cakemaster", "--max-allocator-nodes-per-segment", "1000000"])
                .unwrap();

        assert_eq!(cli.max_allocator_nodes_per_segment, 1_000_000);
        assert_eq!(
            cli.server_config()
                .segment_pool()
                .max_allocator_nodes_per_segment(),
            1_000_000
        );
    }

    #[test]
    fn object_collection_budget_configures_background_candidates_and_reclaims() {
        let cli =
            Cli::try_parse_from(["cakemaster", "--object-collection-budget-per-step", "2304"])
                .unwrap();
        let budget = cli.server_config().reconcile().object_budget();

        assert_eq!(budget.max_candidates(), 2_304);
        assert_eq!(budget.max_reclaims(), 2_304);
        assert_eq!(
            budget.max_empty_slots(),
            DEFAULT_OBJECT_COLLECTION_BUDGET.max_empty_slots()
        );
    }

    #[test]
    fn object_collection_budget_rejects_zero() {
        assert!(
            Cli::try_parse_from(["cakemaster", "--object-collection-budget-per-step", "0"])
                .is_err()
        );
    }

    #[test]
    fn expected_objects_configures_the_catalog_capacity_hint() {
        let cli = Cli::try_parse_from(["cakemaster", "--expected-objects", "3000000"]).unwrap();

        assert_eq!(
            cli.server_config().object_catalog(),
            ObjectCatalogConfig::new(3_000_000)
        );
    }

    #[test]
    fn expected_objects_rejects_zero() {
        assert!(Cli::try_parse_from(["cakemaster", "--expected-objects", "0"]).is_err());
    }

    #[test]
    fn metadata_shards_configure_fixed_owner_workers() {
        let cli = Cli::try_parse_from(["cakemaster", "--metadata-shards", "4"]).unwrap();
        assert_eq!(cli.server_config().metadata_shards(), 4);
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    if let Err(error) = compio::signal::ctrl_c().await {
        log::error!(error:% = error; "failed to listen for Ctrl-C");
    }
}
