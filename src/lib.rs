use std::future::Future;

use axum::{
    http::{HeaderMap, Method, Uri},
    response::IntoResponse,
    routing::any,
    Router,
};

pub async fn run_server(router: Router, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let server = axum::Server::bind(&addr).serve(router.into_make_service());
    server.await.map_err(From::from)
}

pub async fn test_run_server<F>(router: Router, f: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(u16) -> Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Unpin>,
{
    let server =
        axum::Server::bind(&"127.0.0.1:0".parse().unwrap()).serve(router.into_make_service());
    let listening_port = server.local_addr().port();
    let handle = tokio::spawn(async { server.await.unwrap() });
    let result = f(listening_port).await;
    eprintln!("stopping server");
    drop(handle);
    result
}

pub async fn handler(
    uri: Uri,
    method: Method,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    println!("got request");
    dbg!(&uri);
    dbg!(&method);
    dbg!(&headers);
    dbg!(&body);

    "ok"
}

pub fn router() -> Router {
    Router::new().fallback(any(handler))
}
