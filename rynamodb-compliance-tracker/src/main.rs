use std::net::SocketAddr;

use axum::{routing::post, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::SqlitePool;

mod database;
mod handlers;

use crate::database::Database;

#[derive(Debug, Deserialize)]
pub struct ComplianceReport {
    branch: String,
    #[serde(rename = "commitSha")]
    commit_sha: String,
    committer: String,
    errors: i64,
    failed: i64,
    skipped: i64,
    passed: i64,
    duration: f64,
    uploaded: DateTime<Utc>,
}

#[derive(Clone)]
pub struct AppState {
    db: Database,
    auth_token: String,
}

#[tokio::main]
async fn main() {
    let _ = color_eyre::install();
    tracing_subscriber::fmt::init();

    let conn = SqlitePool::connect(concat!(env!("CARGO_MANIFEST_DIR"), "/db.db"))
        .await
        .expect("could not connect to database");
    let db = Database::new(conn);
    let state = AppState {
        db,
        auth_token: std::env::var("RYNAMODB_AUTH_TOKEN").expect("no auth token specified"),
    };

    let app = Router::new()
        .route("/submit", post(handlers::submit_compliance_report))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 9050));
    eprintln!("listening on {addr}");
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .expect("running server");
}
