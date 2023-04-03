use chrono::{DateTime, Utc};
use eyre::WrapErr;
use futures::TryStreamExt;
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};

use crate::ComplianceReport;

#[derive(Clone)]
pub struct Database {
    conn: SqlitePool,
}

impl Database {
    pub fn new(conn: SqlitePool) -> Self {
        Self { conn }
    }

    pub async fn insert(&self, payload: ComplianceReport) -> eyre::Result<()> {
        sqlx::query("INSERT INTO compliance (branch, commitSha, committer, errors, failed, skipped, passed, duration, uploaded) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(payload.branch)
            .bind(payload.commit_sha)
            .bind(payload.committer)
            .bind(payload.errors)
            .bind(payload.failed)
            .bind(payload.skipped)
            .bind(payload.passed)
            .bind(payload.duration)
            .bind(payload.uploaded)
            .execute(&self.conn)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "running SQL query");
                e
            }).wrap_err("running SQL query")?;
        Ok(())
    }

    pub(crate) async fn fetch_branches(&self) -> eyre::Result<Vec<String>> {
        let rows = sqlx::query("SELECT DISTINCT branch FROM compliance ORDER BY branch DESC")
            .map(|row: SqliteRow| row.get("branch"))
            .fetch_all(&self.conn)
            .await
            .wrap_err("fetching branches")?;
        Ok(rows)
    }

    pub(crate) async fn fetch_compliance_history(
        &self,
        branch: &str,
    ) -> eyre::Result<(Vec<DateTime<Utc>>, Vec<f64>)> {
        let mut stream = sqlx::query("SELECT uploaded, passed * 100.0 / (passed + errors + failed + skipped) FROM compliance WHERE branch = $1")
            .bind(&branch)
            .map(|row: SqliteRow| (row.get(0), row.get(1)))
            .fetch(&self.conn);

        let mut x = Vec::new();
        let mut y = Vec::new();

        while let Some(row) = stream.try_next().await? {
            x.push(row.0);
            y.push(row.1);
        }

        Ok((x, y))
    }
}
