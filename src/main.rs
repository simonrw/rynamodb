#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = rynamodb::router();
    let port = 3050;
    tracing::info!(%port, "running server");
    rynamodb::run_server(app, port).await.unwrap();
}
