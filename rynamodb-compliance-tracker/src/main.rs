use std::net::SocketAddr;

use axum::{
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::Deserialize;
use sqlx::SqlitePool;

mod database;
mod handlers;

use crate::database::Database;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    db_path: String,

    #[clap(short, long, default_value = "9050")]
    port: u16,
}

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

    let args = Args::parse();

    let conn = SqlitePool::connect(&args.db_path)
        .await
        .expect("could not connect to database");
    let db = Database::new(conn);
    let state = AppState {
        db,
        auth_token: std::env::var("RYNAMODB_AUTH_TOKEN").expect("no auth token specified"),
    };

    let app = Router::new()
        .route("/", get(handlers::index))
        .route("/branches/:branch", get(handlers::branch))
        .route("/submit", post(handlers::submit_compliance_report))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    eprintln!("listening on {addr}");
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .expect("running server");
}
