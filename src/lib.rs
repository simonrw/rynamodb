use eyre::{Context, Result};
use std::{
    future::Future,
    str::FromStr,
    sync::{Arc, RwLock},
};

use axum::{
    async_trait,
    extract::{FromRequestParts, State},
    http::{request::Parts, HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri},
    response::IntoResponse,
    routing::any,
    Json, Router,
};

mod table;
mod table_manager;
mod types;

static ACCOUNT_ID: &'static str = "000000000000";

pub async fn run_server(router: Router, port: u16) -> Result<()> {
    let addr = format!("127.0.0.1:{port}").parse().unwrap();

    let server = axum::Server::bind(&addr).serve(router.into_make_service());
    server.await.wrap_err("server shutdown incorrectly")?;
    Ok(())
}

pub async fn test_run_server<F>(router: Router, f: F) -> Result<()>
where
    F: FnOnce(u16) -> Box<dyn Future<Output = Result<()>> + Unpin>,
{
    let server =
        axum::Server::bind(&"127.0.0.1:0".parse().unwrap()).serve(router.into_make_service());
    let listening_port = server.local_addr().port();
    tracing::debug!(?listening_port, "server listening");
    let handle = tokio::spawn(async { server.await });
    let result = f(listening_port).await;
    tracing::debug!("stopping server");
    drop(handle);
    result
}

#[derive(Debug)]
pub enum OperationType {
    CreateTable,
    PutItem,
    DescribeTable,
    DeleteTable,
    Query,
}

impl FromStr for OperationType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "CreateTable" => Ok(OperationType::CreateTable),
            "PutItem" => Ok(OperationType::PutItem),
            "DescribeTable" => Ok(OperationType::DescribeTable),
            "DeleteTable" => Ok(OperationType::DeleteTable),
            "Query" => Ok(OperationType::Query),
            _ => todo!("parsing operation {s}"),
        }
    }
}

/// Extractor for dynamodb operation
#[derive(Debug)]
pub struct Operation {
    pub version: String,
    pub name: OperationType,
}

impl TryFrom<&HeaderValue> for Operation {
    // error does not matter because we map it away anyway
    type Error = String;

    fn try_from(value: &HeaderValue) -> std::result::Result<Self, Self::Error> {
        let s = value
            .to_str()
            .map_err(|e| format!("converting to string: {e:?}"))?;
        let mut parts = s.splitn(2, '.');
        let version = parts.next().ok_or(format!("invalid number of parts"))?;
        let operation = parts.next().ok_or(format!("invalid number of parts"))?;

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
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        if let Some(raw_target_string) = parts.headers.get(HeaderName::from_static("x-amz-target"))
        {
            raw_target_string.try_into().map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("invalid target string: {e:?}"),
                )
            })
        } else {
            Err((StatusCode::BAD_REQUEST, "missing target header".to_string()))
        }
    }
}

pub async fn handler(
    uri: Uri,
    method: Method,
    headers: HeaderMap,
    Operation {
        version: _version,
        name: operation,
    }: Operation,
    State(manager): State<Arc<RwLock<table_manager::TableManager>>>,
    // we cannot use the Json extractor since it requires the `Content-Type: application/json`
    // header, which the SDK does not send.
    body: String,
) -> impl IntoResponse {
    tracing::debug!(?uri, ?method, ?operation, "handler invoked");
    tracing::trace!(?headers, "with headers");

    // parse the body
    let res = match operation {
        OperationType::CreateTable => handle_create_table(manager, body).await,
        OperationType::PutItem => handle_put_item(manager, body).await,
        OperationType::DescribeTable => handle_describe_table(manager, body).await,
        OperationType::DeleteTable => handle_delete_table(manager, body).await,
        OperationType::Query => handle_query(manager, body).await,
    };
    res.map_err(|e| (StatusCode::BAD_REQUEST, format!("{e:?}")))
}

async fn handle_query(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>> {
    tracing::debug!("handling query");
    Ok(Json(types::Response::Query(types::QueryOutput {})))
}

async fn handle_delete_table(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>> {
    tracing::debug!("handling delete table");
    Ok(Json(types::Response::DeleteTable(
        types::DeleteTableOutput {},
    )))
}

async fn handle_put_item(
    _manager: Arc<RwLock<table_manager::TableManager>>,
    _body: String,
) -> Result<Json<types::Response>> {
    tracing::debug!("handling put item");
    Ok(Json(types::Response::PutItem(types::PutItemOutput {})))
}

async fn handle_describe_table(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>> {
    tracing::debug!("handling describe table");

    let input: types::DescribeTableInput = serde_json::from_str(&body).wrap_err("invalid json")?;
    tracing::debug!(?input, "parsed input");

    let unlocked_manager = manager.read().unwrap();
    match unlocked_manager.get_table(&input.table_name) {
        Some(table) => Ok(Json(types::Response::DescribeTable(
            types::DescribeTableOutput {
                table: table.description(),
            },
        ))),
        None => todo!("no table error"),
    }
}

async fn handle_create_table(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>> {
    tracing::debug!("handling create table");
    // parse the input

    let input: types::CreateTableInput = serde_json::from_str(&body).wrap_err("invalid json")?;
    tracing::debug!(?input, "parsed input");

    // lock: not great, but probably ok for now
    let mut unlocked_manager = manager.write().unwrap();
    let table = unlocked_manager.new_table(ACCOUNT_ID, table_manager::Region::UsEast1, input)?;

    Ok(Json(types::Response::CreateTable(
        types::CreateTableOutput {
            table_description: table.description(),
        },
    )))
}

pub fn router() -> Router {
    let manager = table_manager::TableManager::default();
    Router::new()
        .fallback(any(handler))
        .with_state(Arc::new(RwLock::new(manager)))
}
