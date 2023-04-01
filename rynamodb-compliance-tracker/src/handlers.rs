use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};

pub(crate) async fn submit_compliance_report(
    State(db): State<crate::Database>,
    Json(payload): Json<crate::ComplianceReport>,
) -> impl IntoResponse {
    tracing::debug!(report = ?payload, "submitting compliance report");
    db.insert(payload)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        .into_response()
}
