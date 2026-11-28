use clap::Parser;
use std::net::SocketAddr;
use tonic::transport::Server;

mod server;

#[derive(Parser, Debug)]
#[command(author, version, about = "Bloom backend")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:50051")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let service = server::BloomServiceImpl::default();

    println!("Starting Bloom backend on {}", args.bind);
    Server::builder()
        .add_service(service.into_service())
        .serve(args.bind)
        .await?;

    Ok(())
}