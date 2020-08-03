#![warn(rust_2018_idioms)]
#![recursion_limit = "256"]

use std::error::Error;

use clap::Clap;
use tracing::{info, Level};

use instaproxy::config::topology::Topology;
use tokio::runtime;
use tracing_subscriber;


#[derive(Clap)]
#[clap(version = "0.1", author = "Ben B. <ben.bromhead@gmail.com>")]
struct ConfigOpts {
    #[clap(short, long, default_value = "config/config.yaml")]
    pub config_file: String,
    #[clap(long, default_value = "4")]
    pub core_threads: usize,
    // 2,097,152 = 2 * 1024 * 1024 (2MiB)
    #[clap(long, default_value = "2097152")]
    pub stack_size: usize
}

#[cfg(not(feature = "no_index"))]
#[cfg(not(feature = "no_object"))]
fn main() -> Result<(), Box<dyn Error>> {
    let _subscriber = tracing_subscriber::fmt()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::INFO)
        // completes the builder and sets the constructed `Subscriber` as the default.
        .init();

    info!("Loading configuration");
    let configuration = ConfigOpts::parse();
    info!( "Starting loaded topology");
    let mut rt = runtime::Builder::new()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .thread_stack_size(configuration.stack_size)
        .threaded_scheduler()
        .core_threads(configuration.core_threads)
        .build()
        .unwrap();

    //todo: https://github.com/tokio-rs/mini-redis/blob/master/src/server.rs

    return rt.block_on(async move {
        if let Ok((_, mut shutdown_complete_rx)) = Topology::from_file(configuration.config_file)?
            .run_chains()
            .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
            info!("Goodbye!");
        }
        Ok(())
    });
}
