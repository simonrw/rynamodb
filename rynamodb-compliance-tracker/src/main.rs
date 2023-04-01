use std::net::SocketAddr;

use axum::{routing::post, Router};
use chrono::{DateTime, Utc};
use eyre::WrapErr;
use serde::Deserialize;
use sqlx::SqlitePool;

mod handlers;

#[derive(Debug, Deserialize)]
struct ComplianceReport {
    branch: String,
    #[serde(rename = "commitSha")]
    commit_sha: String,
    errors: i64,
    failed: i64,
    skipped: i64,
    passed: i64,
    duration: f64,
    uploaded: DateTime<Utc>,
}

#[derive(Clone)]
struct Database {
    conn: SqlitePool,
}

impl Database {
    pub fn new(conn: SqlitePool) -> Self {
        Self { conn }
    }

    pub async fn insert(&self, payload: ComplianceReport) -> eyre::Result<()> {
        sqlx::query("INSERT INTO compliance (branch, commitSha, errors, failed, skipped, passed, duration, uploaded) values ($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(payload.branch)
            .bind(payload.commit_sha)
            .bind(payload.errors)
            .bind(payload.failed)
            .bind(payload.skipped)
            .bind(payload.passed)
            .bind(payload.duration)
            .bind(payload.uploaded)
            .execute(&self.conn)
            .await
            .wrap_err("inserting data into compliance table")?;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let _ = color_eyre::install();
    tracing_subscriber::fmt::init();

    let conn = SqlitePool::connect(concat!(env!("CARGO_MANIFEST_DIR"), "/db.db"))
        .await
        .expect("could not connect to database");
    let state = Database::new(conn);

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
