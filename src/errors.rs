//! Module containing API errors

use axum::{
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::ser::SerializeMap;

#[derive(Debug)]
pub enum ErrorResponse {
    ResourceNotFound { name: Option<String> },
    SerializationError,
    RynamodbError(Box<dyn std::error::Error>),
}

// How to encode the errors
impl serde::Serialize for ErrorResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::ResourceNotFound { name } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry(
                    "__type",
                    "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
                )?;
                if let Some(name) = name {
                    map.serialize_entry(
                        "message",
                        &format!("Requested resource not found: Table: {} not found", name),
                    )?;
                } else {
                    map.serialize_entry("message", "Requested resource not found")?;
                }
                map.end()
            }
            Self::SerializationError => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("__type", "com.amazon.coral.service#SerializationException")?;
                map.end()
            }
            Self::RynamodbError(_) => {
                todo!()
            }
        }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> axum::response::Response {
        match self {
            ErrorResponse::ResourceNotFound { .. } => {
                let request_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
                let mut headers = HeaderMap::new();
                headers.insert(
                    header::HeaderName::from_static("x-amzn-requestid"),
                    request_id.parse().unwrap(),
                );
                headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-amz-json-1.0"),
                );
                headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));

                (StatusCode::BAD_REQUEST, headers, Json(self)).into_response()
            }
            ErrorResponse::SerializationError => {
                (StatusCode::BAD_REQUEST, Json(self)).into_response()
            }
            ErrorResponse::RynamodbError(_) => {
                todo!()
            }
        }
    }
}
