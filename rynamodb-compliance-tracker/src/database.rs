use eyre::WrapErr;
use sqlx::SqlitePool;

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
