use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "3050")]
    port: u16,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let app = rynamodb::router();
    tracing::info!(%args.port, "running server");
    rynamodb::run_server(app, args.port).await.unwrap();
}
