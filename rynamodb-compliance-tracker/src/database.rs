use chrono::{DateTime, Utc};
use eyre::WrapErr;
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
            .wrap_err("inserting data into compliance table")?;
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
        branch: String,
    ) -> eyre::Result<Vec<(DateTime<Utc>, f64)>> {
        let rows = sqlx::query("SELECT uploaded, passed * 100.0 / (passed + errors + failed + skipped) FROM compliance WHERE branch = $1")
            .bind(&branch)
            .map(|row: SqliteRow| (row.get(0), row.get(1)))
            .fetch_all(&self.conn)
            .await
            .wrap_err("fetching compliance history")?;
        Ok(rows)
    }
}
