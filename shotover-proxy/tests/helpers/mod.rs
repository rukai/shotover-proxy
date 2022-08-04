use anyhow::Result;
use cassandra_cpp::{Cluster, Ssl};
use redis::aio::AsyncStream;
use redis::Client;
use shotover_proxy::runner::{ConfigOpts, Runner};
use shotover_proxy::tls::{TlsConfig, TlsConnector};
use std::fs::read_to_string;
use std::pin::Pin;
use std::sync::mpsc;
use std::time::Duration;
use tokio::runtime::{Handle as RuntimeHandle, Runtime};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_io_timeout::TimeoutStream;

pub mod cassandra;
use cassandra::CassandraConnection;

#[must_use]
pub struct ShotoverManager {
    pub runtime: Option<Runtime>,
    pub runtime_handle: RuntimeHandle,
    pub join_handle: Option<JoinHandle<Result<()>>>,
    pub trigger_shutdown_tx: watch::Sender<bool>,
    panic_occured_rx: mpsc::Receiver<()>,
}

// false unused warnings caused by https://github.com/rust-lang/rust/issues/46379
impl ShotoverManager {
    #[allow(dead_code)]
    pub fn from_topology_file(topology_path: &str) -> ShotoverManager {
        ShotoverManager::from_topology_file_inner(topology_path, true)
    }

    #[allow(dead_code)]
    pub fn from_topology_file_without_observability(topology_path: &str) -> ShotoverManager {
        ShotoverManager::from_topology_file_inner(topology_path, false)
    }

    fn from_topology_file_inner(topology_path: &str, observability: bool) -> ShotoverManager {
        // std does not yet support doing this without a race condition.
        // See: https://github.com/rust-lang/rust/issues/92649
        let prev_hook = std::panic::take_hook();
        let (tx, panic_occured_rx) = mpsc::sync_channel(10);
        std::panic::set_hook(Box::new(move |panic_info| {
            // This panic hook will outlive the Receiver.
            // so we should just ignore failures to send as that is expected once the Receiver drops.
            let _ = tx.try_send(());

            prev_hook(panic_info)
        }));

        let opts = ConfigOpts {
            topology_file: topology_path.into(),
            config_file: "tests/helpers/config.yaml".into(),
            ..ConfigOpts::default()
        };
        let spawn = if observability {
            Runner::new(opts)
                .unwrap_or_else(|x| panic!("{x} occurred processing {topology_path:?}"))
                .with_observability_interface()
                .unwrap_or_else(|x| panic!("{x} occurred processing {topology_path:?}"))
                .run_spawn()
        } else {
            Runner::new(opts)
                .unwrap_or_else(|x| panic!("{x} occurred processing {topology_path:?}"))
                .run_spawn()
        };

        // If we allow the tracing_guard to be dropped then the following tests in the same file will not get tracing so we mem::forget it.
        // This is because tracing can only be initialized once in the same execution, secondary attempts to initalize tracing will silently fail.
        std::mem::forget(spawn.tracing_guard);

        ShotoverManager {
            runtime: spawn.runtime,
            runtime_handle: spawn.runtime_handle,
            join_handle: Some(spawn.join_handle),
            trigger_shutdown_tx: spawn.trigger_shutdown_tx,
            panic_occured_rx,
        }
    }

    #[allow(unused)]
    pub fn redis_connection(&self, port: u16) -> redis::Connection {
        let address = "127.0.0.1";
        test_helpers::wait_for_socket_to_open(address, port);

        let connection = Client::open((address, port))
            .unwrap()
            .get_connection()
            .unwrap();
        connection.set_read_timeout(Some(Duration::from_secs(10)));
        connection
    }

    #[allow(unused)]
    pub async fn redis_connection_async(&self, port: u16) -> redis::aio::Connection {
        let address = "127.0.0.1";
        test_helpers::wait_for_socket_to_open(address, port);

        let stream = Box::pin(
            tokio::net::TcpStream::connect((address, port))
                .await
                .unwrap(),
        );
        ShotoverManager::redis_connection_async_inner(
            Box::pin(stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
        )
        .await
    }

    #[allow(unused)]
    pub async fn redis_connection_async_tls(
        &self,
        port: u16,
        config: TlsConfig,
    ) -> redis::aio::Connection {
        let address = "127.0.0.1";
        test_helpers::wait_for_socket_to_open(address, port);

        let tcp_stream = tokio::net::TcpStream::connect((address, port))
            .await
            .unwrap();
        let connector = TlsConnector::new(config).unwrap();
        let tls_stream = connector
            .connect_unverified_hostname(tcp_stream)
            .await
            .unwrap();
        ShotoverManager::redis_connection_async_inner(
            Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
        )
        .await
    }

    async fn redis_connection_async_inner(
        stream: Pin<Box<dyn AsyncStream + Send + Sync>>,
    ) -> redis::aio::Connection {
        let mut stream_with_timeout = TimeoutStream::new(stream);
        stream_with_timeout.set_read_timeout(Some(Duration::from_secs(10)));

        let connection_info = Default::default();
        redis::aio::Connection::new(
            &connection_info,
            Box::pin(stream_with_timeout) as Pin<Box<dyn AsyncStream + Send + Sync>>,
        )
        .await
        .unwrap()
    }

    #[allow(unused)]
    pub fn cassandra_connection(&self, contact_points: &str, port: u16) -> CassandraConnection {
        CassandraConnection::new(contact_points, port)
    }

    #[allow(unused)]
    pub fn cassandra_connection_tls(
        &self,
        contact_points: &str,
        port: u16,
        ca_cert_path: &str,
    ) -> CassandraConnection {
        let ca_cert = read_to_string(ca_cert_path).unwrap();
        let mut ssl = Ssl::default();
        Ssl::add_trusted_cert(&mut ssl, &ca_cert).unwrap();

        for contact_point in contact_points.split(',') {
            test_helpers::wait_for_socket_to_open(contact_point, port);
        }

        let mut cluster = Cluster::default();
        cluster.set_credentials("cassandra", "cassandra").unwrap();
        cluster.set_contact_points(contact_points).unwrap();
        cluster.set_port(port).ok();
        cluster.set_load_balance_round_robin();
        cluster.set_ssl(&mut ssl);

        CassandraConnection::Datastax(cluster.connect().unwrap())
    }

    fn shutdown_shotover(&mut self) -> Result<()> {
        self.trigger_shutdown_tx.send(true)?;
        let _enter_guard = self.runtime_handle.enter();
        futures::executor::block_on(self.join_handle.take().unwrap())?
    }
}

impl Drop for ShotoverManager {
    fn drop(&mut self) {
        // Must clear the recorder before skipping a shutdown on panic; if one test panics and the recorder is not cleared,
        // the following tests will panic because they will try to set another recorder
        // TODO: This is unsafe due to a possible race condition and we are doing nothing to prevent that race condition.
        // It is left as is because there are no reasonable solutions to this problem at the moment.
        // A possible way to trigger this issue would be creating a ShotoverManager instance in one thread while dropping a ShotoverManager instance in another thread.
        unsafe { metrics::clear_recorder() };

        if std::thread::panicking() {
            // If already panicking do not panic while attempting to shutdown shotover in order to avoid a double panic.
            if let Err(err) = self.shutdown_shotover() {
                println!("Failed to shutdown shotover: {err}")
            }
        } else {
            self.shutdown_shotover().unwrap();

            // When a panic occurs in a shotover tokio task that isnt joined on, tokio will catch the panic, print the panic message and shotover will continue running happily.
            // This behaviour is reasonable and makes shotover more robust but in our integration tests we want to ensure that panics never ever occur.
            // As a result we need the following assertion to detect panics that occur within tasks.
            if let Ok(()) = self.panic_occured_rx.try_recv() {
                panic!("Panic occured within a shotover task during integration test.\nPlease refer to the panic message that should have been already been logged to stdout by tokio when the panic occured.")
            }
        }
    }
}
