use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};
use rfsync::rpc::{switch_client::SwitchClient, JoinRequest};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Join(JoinArgs),
}

#[derive(Args)]
struct JoinArgs {
    lhs: SocketAddr,
    rhs: SocketAddr,
}

fn httped(addr: SocketAddr) -> String {
    format!("http://{}", addr)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Join(args) => {
            let lhs = httped(args.lhs);
            let rhs = httped(args.rhs);
            let mut lhs_client = SwitchClient::connect(lhs.clone()).await.unwrap();
            let mut rhs_client = SwitchClient::connect(rhs.clone()).await.unwrap();
            lhs_client.join(JoinRequest { addr: rhs }).await.unwrap();
            rhs_client.join(JoinRequest { addr: lhs }).await.unwrap();
        }
    }
}
