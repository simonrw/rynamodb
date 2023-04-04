use eyre::Context;
use std::{
    future::Future,
    str::FromStr,
    sync::{Arc, RwLock},
};
use tracing::Instrument;

use axum::{
    extract::State,
    http::{HeaderMap, Method, Uri},
    routing::{any, get},
    Json, Router,
};

use crate::{errors::ErrorResponse, types::ListTablesOutput};

mod errors;
mod extractors;
mod table;
mod table_manager;
pub mod types;

pub static DEFAULT_ACCOUNT_ID: &str = "000000000000";

pub async fn run_server(router: Router, port: u16) -> eyre::Result<()> {
    let addr = format!("127.0.0.1:{port}").parse().unwrap();

    let server = axum::Server::bind(&addr).serve(router.into_make_service());
    server.await.wrap_err("server shutdown incorrectly")?;
    Ok(())
}

pub async fn test_run_server<F>(router: Router, f: F) -> eyre::Result<()>
where
    F: FnOnce(u16) -> Box<dyn Future<Output = eyre::Result<()>> + Unpin>,
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
    GetItem,
    ListTables,
    Scan,
    BatchWriteItem,
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
            "GetItem" => Ok(OperationType::GetItem),
            "ListTables" => Ok(OperationType::ListTables),
            "Scan" => Ok(OperationType::Scan),
            "BatchWriteItem" => Ok(OperationType::BatchWriteItem),
            s => Err(format!("operation {s} not handled")),
        }
    }
}

pub async fn handler(
    uri: Uri,
    method: Method,
    headers: HeaderMap,
    operation_extractor: std::result::Result<extractors::Operation, String>,
    State(manager): State<Arc<RwLock<table_manager::TableManager>>>,
    // we cannot use the Json extractor since it requires the `Content-Type: application/json`
    // header, which the SDK does not send.
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::debug_span!("request", request_id = request_id);

    let extractors::Operation {
        name: operation, ..
    } = operation_extractor.map_err(|e| {
        tracing::error!(error = ?e, "operation unhandled");
        ErrorResponse::InvalidOperation(e)
    })?;

    async move {
        tracing::debug!(?uri, ?method, ?operation, "handler invoked");
        tracing::trace!(?headers, "with headers");

        // parse the body
        let res = match operation {
            OperationType::CreateTable => handle_create_table(manager, body).await,
            OperationType::PutItem => handle_put_item(manager, body).await,
            OperationType::DescribeTable => handle_describe_table(manager, body).await,
            OperationType::DeleteTable => handle_delete_table(manager, body).await,
            OperationType::Query => handle_query(manager, body).await,
            OperationType::GetItem => handle_get_item(manager, body).await,
            OperationType::ListTables => handle_list_tables(manager, body).await,
            OperationType::Scan => handle_scan(manager, body).await,
            OperationType::BatchWriteItem => handle_batch_write_item(manager, body).await,
        };
        tracing::info!(?res, "got result");
        res
    }
    .instrument(span)
    .await
}

async fn handle_batch_write_item(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!("handling batch write item");
    let input: types::BatchWriteInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;
    tracing::debug!(?input, "parsed input");

    let mut unlocked_manager = manager.write().map_err(|_| ErrorResponse::MutexUnlock)?;
    let unprocessed_items = unlocked_manager
        .batch_write_item(input)
        .expect("TODO: failed to batch write item");

    Ok(Json(types::Response::BatchWriteItem(
        types::BatchWriteItemOutput {
            unprocessed_items: Some(unprocessed_items),
        },
    )))
}

async fn handle_scan(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!("handling scan");
    let input: types::ScanInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;
    tracing::debug!(?input, "parsed input");

    let unlocked_manager = manager.read().map_err(|_| ErrorResponse::MutexUnlock)?;
    let table = unlocked_manager
        .get_table(&input.table_name)
        .ok_or_else(|| ErrorResponse::ResourceNotFound { name: None })?;
    tracing::debug!(table_name = ?input.table_name, "found table");

    let res = table
        .scan()
        .map_err(|e| ErrorResponse::RynamodbError(Box::new(e)))?;

    let count = res.len();
    Ok(Json(types::Response::Query(types::QueryOutput {
        items: res,
        count,
        // TODO
        scanned_count: count,
    })))
}

async fn handle_list_tables(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!("handling list_tables");
    let _input: types::ListTablesInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;

    // TODO: input handling
    let unlocked_manager = manager.read().map_err(|_| ErrorResponse::MutexUnlock)?;
    let table_names = unlocked_manager.table_names();
    tracing::debug!(?table_names, "found table names");

    Ok(Json(types::Response::ListTables(ListTablesOutput {
        table_names,
    })))
}

async fn handle_get_item(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!("handling get_item");
    let input: types::GetItemInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;
    tracing::debug!(?input, "parsed input");

    let unlocked_manager = manager.read().map_err(|_| ErrorResponse::MutexUnlock)?;
    let table = unlocked_manager
        .get_table(&input.table_name)
        .ok_or_else(|| ErrorResponse::ResourceNotFound { name: None })?;
    tracing::debug!(table_name = ?input.table_name, "found table");

    let res = table.get_item(input.key);
    tracing::debug!(result = ?res, "found result");

    Ok(Json(types::Response::GetItem(types::GetItemOutput {
        item: res,
    })))
}

async fn handle_query(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!("handling query");

    tracing::debug!(?body, "got body");
    let input: types::QueryInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;
    tracing::debug!(?input, "parsed input");

    let unlocked_manager = manager.read().map_err(|_| ErrorResponse::MutexUnlock)?;
    let table = unlocked_manager
        .get_table(&input.table_name)
        .ok_or_else(|| ErrorResponse::ResourceNotFound { name: None })?;
    // .ok_or_else(|| eyre::eyre!("no table found"))?;
    tracing::debug!(table_name = ?input.table_name, "found table");

    let res = table
        .query(
            &input.key_condition_expression,
            &input.expression_attribute_names,
            &input.expression_attribute_values,
        )
        .map_err(|e| ErrorResponse::RynamodbError(Box::new(e)))?;
    tracing::debug!(result = ?res, "found result");

    let count = res.len();
    Ok(Json(types::Response::Query(types::QueryOutput {
        items: res,
        count,
        // TODO
        scanned_count: count,
    })))
}

async fn handle_delete_table(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!(%body, "handling delete table");

    let input: types::DeleteTableInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;
    tracing::debug!(?input, "parsed input");

    let mut unlocked_manager = manager.write().map_err(|_| ErrorResponse::MutexUnlock)?;
    unlocked_manager
        .delete_table(&input.table_name)
        .map_err(|e| ErrorResponse::RynamodbError(format!("{e}").into()))?;

    Ok(Json(types::Response::DeleteTable(
        types::DeleteTableOutput {},
    )))
}

async fn handle_put_item(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!("handling put item");

    let input: types::PutItemInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;
    tracing::debug!(?input, "parsed input");

    // convert the item to our representation
    let attributes = input.item;

    let mut unlocked_manager = manager.write().map_err(|_| ErrorResponse::MutexUnlock)?;
    let table = unlocked_manager
        .get_table_mut(&input.table_name)
        .ok_or_else(|| ErrorResponse::ResourceNotFound { name: None })?;

    table
        .insert(attributes)
        .map_err(|e| ErrorResponse::RynamodbError(Box::new(e)))?;

    Ok(Json(types::Response::PutItem(types::PutItemOutput {})))
}

async fn handle_describe_table(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!("handling describe table");

    let input: types::DescribeTableInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;
    tracing::debug!(?input, "parsed input");

    let unlocked_manager = manager.read().map_err(|_| ErrorResponse::MutexUnlock)?;
    match unlocked_manager.get_table(&input.table_name) {
        Some(table) => Ok(Json(types::Response::DescribeTable(
            types::DescribeTableOutput {
                table: table.description(),
            },
        ))),
        None => Err(ErrorResponse::ResourceNotFound {
            name: Some(input.table_name),
        }),
    }
}

async fn handle_create_table(
    manager: Arc<RwLock<table_manager::TableManager>>,
    body: String,
) -> Result<Json<types::Response>, ErrorResponse> {
    tracing::debug!(?body, "handling create table");
    // parse the input

    let input: types::CreateTableInput =
        serde_json::from_str(&body).map_err(|_| ErrorResponse::SerializationError)?;
    tracing::debug!(?input, "parsed input");

    // lock: not great, but probably ok for now
    let mut unlocked_manager = manager.write().map_err(|_| ErrorResponse::MutexUnlock)?;
    let table = unlocked_manager
        .new_table(DEFAULT_ACCOUNT_ID, table_manager::Region::UsEast1, input)
        // .map_err(|e| ErrorResponse::RynamodbError(format!("{e}").into()))?;
        .map_err(|e| ErrorResponse::RynamodbError(format!("{e}").into()))?;

    Ok(Json(types::Response::CreateTable(
        types::CreateTableOutput {
            table_description: table.description(),
        },
    )))
}

pub fn router() -> Router {
    let manager = table_manager::TableManager::default();
    Router::new()
        .route("/_health", get(|| async { "ok" }))
        .fallback(any(handler))
        .with_state(Arc::new(RwLock::new(manager)))
}
