use test_helpers::docker_compose::DockerCompose;
use test_helpers::flamegraph::Perf;
use test_helpers::kafka_producer_perf_test::run_producer_bench;
use test_helpers::shotover_process::ShotoverProcessBuilder;

// To get useful results you will need to modify the Cargo.toml like:
// [profile.release]
// #lto = "fat"
// codegen-units = 1
// debug = true

#[tokio::main]
async fn main() {
    test_helpers::bench::init();
    let config_dir = "tests/test-configs/kafka/passthrough";
    {
        let _compose = DockerCompose::new(&format!("{}/docker-compose.yaml", config_dir));

        let shotover =
            ShotoverProcessBuilder::new_with_topology(&format!("{}/topology.yaml", config_dir))
                .start()
                .await;

        let perf = Perf::new(shotover.child.as_ref().unwrap().id().unwrap());

        println!("Benching Shotover ...");
        run_producer_bench("[localhost]:9192");

        shotover.shutdown_and_then_consume_events(&[]).await;
        perf.flamegraph();
    }
}