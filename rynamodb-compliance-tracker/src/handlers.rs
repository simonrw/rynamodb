use askama::Template;
use axum::{
    async_trait,
    extract::{FromRequestParts, Path, State},
    http::{request::Parts, HeaderName, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};

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

// routes

// GET /

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    branches: Vec<String>,
}

pub(crate) async fn index(
    State(crate::AppState { db, .. }): State<crate::AppState>,
) -> impl IntoResponse {
    let branches = db.fetch_branches().await.unwrap();

    HtmlTemplate(IndexTemplate { branches }).into_response()
}

// GET /branches/:branch

#[derive(Template)]
#[template(path = "branch.html")]
struct History {
    history: Vec<(DateTime<Utc>, f64)>,
}

pub(crate) async fn branch(
    Path(branch): Path<String>,
    State(crate::AppState { db, .. }): State<crate::AppState>,
) -> impl IntoResponse {
    let compliance_history = db.fetch_compliance_history(branch).await.unwrap();

    HtmlTemplate(History {
        history: compliance_history,
    })
    .into_response()
}

// POST /submit
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

// helpers

struct HtmlTemplate<T>(T);

impl<T> IntoResponse for HtmlTemplate<T>
where
    T: Template,
{
    fn into_response(self) -> Response {
        match self.0.render() {
            Ok(html) => Html(html).into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to render template. Error: {}", err),
            )
                .into_response(),
        }
    }
}
