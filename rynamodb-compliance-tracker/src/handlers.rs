use axum::{
    async_trait,
    extract::{FromRequestParts, State},
    http::{request::Parts, HeaderName, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};

// extractor to get auth token
pub struct ExtractAuthToken(HeaderValue);

#[async_trait]
impl<S> FromRequestParts<S> for ExtractAuthToken
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(auth_token) = parts
            .headers
            .get(HeaderName::from_static("x-rynamodb-token"))
        {
            Ok(ExtractAuthToken(auth_token.clone()))
        } else {
            tracing::warn!("missing auth token");
            Err((StatusCode::UNAUTHORIZED, "no auth token present"))
        }
    }
}

pub(crate) async fn submit_compliance_report(
    State(crate::AppState { db, auth_token }): State<crate::AppState>,
    ExtractAuthToken(given_auth_token): ExtractAuthToken,
    Json(payload): Json<crate::ComplianceReport>,
) -> impl IntoResponse {
    if given_auth_token != auth_token {
        tracing::warn!(?auth_token, "invalid auth token");
        return (StatusCode::FORBIDDEN, "invalid auth token").into_response();
    }

    tracing::debug!(report = ?payload, "submitting compliance report");
    match db.insert(payload).await {
        Ok(_) => "submitted".into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
