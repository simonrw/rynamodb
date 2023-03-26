use axum::{
    async_trait,
    body::{Bytes, HttpBody},
    extract::{FromRequest, FromRequestParts},
    http::{request::Parts, HeaderName, HeaderValue, Request},
    BoxError,
};
use serde::de::DeserializeOwned;

// JSON type that accepts aws content-type
//
// Copied directly from the axum source code
#[derive(Debug, Clone, Copy, Default)]
pub struct AwsJson<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for AwsJson<T>
where
    T: DeserializeOwned,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = String;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        // TODO check content-type header

        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|e| format!("fetching body bytes: {e:?}"))?;
        let res =
            serde_json::from_slice(&bytes).map_err(|e| format!("deserializing body: {e:?}"))?;

        Ok(AwsJson(res))
    }
}

/// Extractor for dynamodb operation
#[derive(Debug)]
pub struct Operation {
    pub version: String,
    pub name: crate::OperationType,
}

impl TryFrom<&HeaderValue> for Operation {
    // error does not matter because we map it away anyway
    type Error = String;

    fn try_from(value: &HeaderValue) -> std::result::Result<Self, Self::Error> {
        let s = value
            .to_str()
            .map_err(|e| format!("converting to string: {e:?}"))?;
        let mut parts = s.splitn(2, '.');
        let version = parts.next().ok_or("invalid number of parts".to_string())?;
        let operation = parts.next().ok_or("invalid number of parts".to_string())?;

        Ok(Self {
            version: version.to_string(),
            name: operation
                .parse()
                .map_err(|e| format!("parsing operation: {e:?}"))?,
        })
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for Operation
where
    S: Send + Sync,
{
    type Rejection = String;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        if let Some(raw_target_string) = parts.headers.get(HeaderName::from_static("x-amz-target"))
        {
            raw_target_string.try_into()
        } else {
            Err("missing_target_header".to_string())
        }
    }
}
