#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = rynamodb::router();
    rynamodb::run_server(app, 3050).await.unwrap();
}
