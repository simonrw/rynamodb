#[tokio::main]
async fn main() {
    let app = rynamodb::router();

    let (_tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

    rynamodb::run_server(app, move |port| {
        Box::new(Box::pin(async move {
            eprintln!("running on port {port}");
            let _ = rx.recv().await;
            Ok(())
        }))
    })
    .await
    .unwrap();
}
