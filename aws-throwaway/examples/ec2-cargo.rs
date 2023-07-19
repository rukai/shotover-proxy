use aws_throwaway::{ec2_instance::Ec2Instance, Aws, InstanceType};
use clap::Parser;
use rustyline::DefaultEditor;
use shellfish::{async_fn, handler::DefaultAsyncHandler, Command, Shell};
use std::error::Error;
use tracing_subscriber::EnvFilter;

/// Spins up an EC2 instance and then presents a shell from which you can run `cargo test` on the ec2 instance.
///
/// TODO: Every time the shell runs a cargo command, any local changes to the repo are reuploaded to the ec2 instance.
///
/// When the shell is exited all created EC2 instances are destroyed.
#[derive(Parser, Clone)]
#[clap()]
pub struct Args {
    /// Specify the instance type to use for building and running tests.
    #[clap(long, default_value = "c7g.2xlarge")]
    pub instance_type: String,

    /// Cleanup resources and then immediately terminate without opening the shell
    #[clap(long)]
    pub cleanup: bool,
}

#[tokio::main]
async fn main() {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(non_blocking)
        .init();

    let args = Args::parse();
    if args.cleanup {
        Aws::cleanup_resources_static().await;
        println!("All AWS throwaway resources have been deleted");
        return;
    }

    let aws = Aws::new().await;
    let instance_type = InstanceType::from(args.instance_type.as_str());
    let instance = aws.create_ec2_instance(instance_type, 40).await;

    println!(
        "If something goes wrong with setup you can ssh into the machine by:\n{}",
        instance.ssh_instructions()
    );

    let mut receiver = instance
        .ssh()
        .shell_stdout_lines(r#"
set -e
set -u
# Need to retry until succeeds as apt-get update may fail if run really quickly after starting the instance
until sudo apt-get update -qq
do
  sleep 1
done
sudo apt-get install -y cmake pkg-config g++ libssl-dev librdkafka-dev uidmap
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
curl -sSL https://get.docker.com/ | sudo sh
dockerd-rootless-setuptool.sh install
echo '#!/bin/bash
docker compose "$@"
' | sudo dd of=/bin/docker-compose
sudo chmod +x /bin/docker-compose

echo "export RUST_BACKTRACE=1" >> .profile
"#).await;
    while let Some(line) = receiver.recv().await {
        println!("{}", line)
    }

    println!("Finished creating instance.");

    let mut shell = Shell::new_with_async_handler(
        State { instance },
        "ec2-cargo$ ",
        DefaultAsyncHandler::default(),
        DefaultEditor::new().unwrap(),
    );
    shell.commands.insert(
        "test",
        Command::new_async(
            "Uploads changes and runs cargo test $args".to_owned(),
            async_fn!(State, test),
        ),
    );
    shell.commands.insert(
        "ssh-instructions",
        Command::new(
            "Print a bash snippet that can be used to ssh into the machine".to_owned(),
            ssh_instructions,
        ),
    );

    shell.run_async().await.unwrap();

    aws.cleanup_resources().await;
    println!("All AWS throwaway resources have been deleted")
}

async fn test(state: &mut State, mut args: Vec<String>) -> Result<(), Box<dyn Error>> {
    rsync_shotover(&state.instance).await;
    args.remove(0);
    let args = args.join(" ");
    let mut receiver = state
        .instance
        .ssh()
        .shell_stdout_lines(&format!(
            r#"
cd shotover-proxy
RUST_BACKTRACE=1 ~/.cargo/bin/cargo test --color always {} 2>&1
"#,
            args
        ))
        .await;
    while let Some(line) = receiver.recv().await {
        println!("{}", line)
    }

    Ok(())
}

async fn rsync_shotover(instance: &Ec2Instance) {
    let project_root = std::env::current_dir().unwrap();
    // TODO: proper

    let address = instance.public_ip();
    let key_path = project_root.join("target").join("ec2-cargo-privatekey");
    tokio::fs::write(&key_path, instance.client_private_key())
        .await
        .unwrap();

    let output = tokio::process::Command::new("chmod")
        .args(&[format!("{}", key_path.display()), "400".to_owned()])
        .output()
        .await
        .unwrap();
    if !output.status.success() {
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        panic!("chmod failed:\nstdout:\n{stdout}\nstderr:\n{stderr}")
    }

    let output = tokio::process::Command::new("rsync")
        .args(&[
            "--delete".to_owned(),
            "--exclude".to_owned(),
            "target".to_owned(),
            "-e".to_owned(),
            format!("'ssh -i {}'", key_path.display()),
            "-ravuh".to_owned(),
            ".".to_owned(),
            format!("ubuntu@{address}:/home/ubuntu/shotover-proxy"),
        ])
        .output()
        .await
        .unwrap();
    if !output.status.success() {
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        panic!("rsync failed:\nstdout:\n{stdout}\nstderr:\n{stderr}")
    }
}

fn ssh_instructions(state: &mut State, mut _args: Vec<String>) -> Result<(), Box<dyn Error>> {
    println!(
        "Run the following to ssh into the EC2 instance:\n{}",
        state.instance.ssh_instructions()
    );

    Ok(())
}

struct State {
    instance: Ec2Instance,
}
