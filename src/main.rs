#[tokio::main]
async fn main() {
    let app = rynamodb::router();
    rynamodb::run_server(app, 3050).await.unwrap();
}
