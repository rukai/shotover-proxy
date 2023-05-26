use criterion::{criterion_group, Criterion};
use redis::Cmd;
use std::path::Path;
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::{docker_compose, DockerCompose};
use test_helpers::lazy::new_lazy_shared;
use test_helpers::shotover_process::{BinProcess, ShotoverProcessBuilder};
use tokio::runtime::Runtime;

struct Query {
    name: &'static str,
    command: Cmd,
}

fn redis(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis");
    group.throughput(criterion::Throughput::Elements(1));
    group.noise_threshold(0.2);

    let queries = [
        Query {
            name: "set",
            command: redis::cmd("SET").arg("foo").arg(42).clone(),
        },
        Query {
            name: "get",
            command: redis::cmd("GET").arg("bench_test_data").clone(),
        },
    ];

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "tests/test-configs/redis-multi/topology.yaml",
                "tests/test-configs/redis-multi/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(
                format!("active_{}", query.name),
                &resources,
                move |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        query
                            .command
                            .execute(&mut resources.as_mut().unwrap().connection);
                    })
                },
            );
        }
    }

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "tests/test-configs/redis-cluster-hiding/topology.yaml",
                "tests/test-configs/redis-cluster-hiding/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(
                format!("cluster_{}", query.name),
                &resources,
                move |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        query
                            .command
                            .execute(&mut resources.as_mut().unwrap().connection);
                    })
                },
            );
        }
    }

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "tests/test-configs/redis-passthrough/topology.yaml",
                "tests/test-configs/redis-passthrough/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(
                format!("passthrough_{}", query.name),
                &resources,
                move |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        query
                            .command
                            .execute(&mut resources.as_mut().unwrap().connection);
                    })
                },
            );
        }
    }

    // Only need to test one case here as the type of command shouldnt affect TLS
    group.bench_with_input(
        "single_tls",
        || {
            test_helpers::cert::generate_test_certs(Path::new(
                "tests/test-configs/redis-tls/certs",
            ));
            BenchResources::new(
                "tests/test-configs/redis-tls/topology.yaml",
                "tests/test-configs/redis-tls/docker-compose.yaml",
            )
        },
        move |b, state| {
            b.iter(|| {
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        },
    );

    // Only need to test one case here as the type of command shouldnt affect TLS
    group.bench_with_input(
        "cluster_tls",
        || {
            test_helpers::cert::generate_test_certs(Path::new(
                "tests/test-configs/redis-tls/certs",
            ));
            BenchResources::new(
                "tests/test-configs/redis-cluster-tls/topology.yaml",
                "tests/test-configs/redis-cluster-tls/docker-compose.yaml",
            )
        },
        move |b, state| {
            b.iter(|| {
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        },
    );
}

criterion_group!(benches, redis);

struct BenchResources {
    connection: redis::Connection,
    shotover: Option<BinProcess>,
    _compose: DockerCompose,
    tokio: Runtime,
}

impl Drop for BenchResources {
    fn drop(&mut self) {
        self.tokio.block_on(
            self.shotover
                .take()
                .unwrap()
                .shutdown_and_then_consume_events(&[]),
        );
    }
}

impl BenchResources {
    fn new(shotover_topology: &str, compose_file: &str) -> Self {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let compose = docker_compose(compose_file);
        let shotover = Some(
            tokio.block_on(ShotoverProcessBuilder::new_with_topology(shotover_topology).start()),
        );

        let mut connection = redis_connection::new(6379);
        redis::cmd("SET")
            .arg("bench_test_data")
            .arg("A value with some length length to it to form a reasonable benchmark")
            .execute(&mut connection);

        Self {
            _compose: compose,
            shotover,
            connection,
            tokio,
        }
    }
}
