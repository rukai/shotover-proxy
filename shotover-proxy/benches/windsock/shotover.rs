use test_helpers::shotover_process::ShotoverProcessBuilder;
use tokio_bin_process::{bin_path, BinProcess};
use uuid::Uuid;

pub async fn shotover_process_custom_topology(topology_contents: &str) -> BinProcess {
    let topology_path = std::env::temp_dir().join(Uuid::new_v4().to_string());
    std::fs::write(&topology_path, topology_contents).unwrap();
    ShotoverProcessBuilder::new_with_topology(topology_path.to_str().unwrap())
        .with_config("config/config.yaml")
        .with_bin(bin_path!("shotover-proxy"))
        .start()
        .await
}
