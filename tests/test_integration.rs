use std::{collections::HashMap, future::Future, time::Duration};

use aws_sdk_dynamodb::{
    model::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    },
    output::GetItemOutput,
    types::SdkError,
    Client,
};
use axum::{
    async_trait,
    http::{HeaderMap, HeaderName, HeaderValue},
};
use eyre::{Context, Result};
use reqwest::header::CONTENT_TYPE;

#[async_trait]
trait ToValue {
    async fn to_json_value(self) -> Option<serde_json::Value>;
}

#[async_trait]
impl<E> ToValue for aws_sdk_dynamodb::types::SdkError<E>
where
    E: Sync + Send,
{
    async fn to_json_value(self) -> Option<serde_json::Value> {
        match self {
            SdkError::ServiceError(e) => {
                let raw = e.raw();
                let response = raw.http();
                let body = response.body();
                let bytes = body.bytes().unwrap();
                let value = serde_json::from_slice(bytes).expect("invalid json body");
                value
            }
            _ => None,
        }
    }
}

#[async_trait]
impl<R, E> ToValue for Result<R, E>
where
    E: ToValue + Sync + Send,
    R: Sync + Send,
{
    async fn to_json_value(self) -> Option<serde_json::Value> {
        match self {
            Ok(_) => panic!("not implemented for Ok types"),
            Err(e) => e.to_json_value().await,
        }
    }
}

#[async_trait]
impl ToValue for Result<reqwest::Response, reqwest::Error> {
    async fn to_json_value(self) -> Option<serde_json::Value> {
        match self {
            Ok(r) => {
                let status = r.status().as_u16();
                let body: serde_json::Value = r.json().await.unwrap();

                dbg!(status, &body);

                Some(serde_json::json!({
                    "status": status,
                    "body": body,
                }))
            }

            Err(e) => {
                dbg!(&e);
                None
            }
        }
    }
}

fn test_init() {
    let _ = tracing_subscriber::fmt::try_init();

    // only create new snapshots when targeting AWS
    let insta_envar_value = if targeting_aws() { "always" } else { "no" };
    std::env::set_var("INSTA_UPDATE", insta_envar_value);

    // set some AWS envars
    if !targeting_aws() {
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }
}

fn targeting_aws() -> bool {
    std::env::var("TEST_TARGET").unwrap_or_else(|_| String::new()) == "AWS_CLOUD"
}

async fn test_client(port: u16) -> Client {
    if targeting_aws() {
        tracing::debug!("creating client against AWS");
        create_client(None).await
    } else {
        tracing::debug!("creating local client");
        let endpoint_url = format!("http://127.0.0.1:{port}");
        let client = create_client(Some(&endpoint_url)).await;
        client
    }
}

macro_rules! skip_aws_cloud {
    () => {{
        if targeting_aws() {
            tracing::warn!("skipping test as we are targeting AWS");
            return;
        }
    }};
}

async fn wait_for_table_creation(table_name: &str, client: &Client) -> Result<()> {
    tracing::debug!("waiting for table to be created");
    for _ in 0..30 {
        let res = client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .wrap_err("fetching table status")?;

        match res
            .table()
            .expect("could not get table")
            .table_status()
            .expect("could not retrieve table status")
        {
            aws_sdk_dynamodb::model::TableStatus::Active => {
                tracing::debug!("table created successfully");
                return Ok(());
            }
            status => tracing::trace!(?status, "incomplete status given"),
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    eyre::bail!("timeout waiting for table to have been created");
}

#[tracing::instrument(skip(client))]
async fn default_dynamodb_table(table_name: &str, client: &Client) -> Result<()> {
    let pk_ad = AttributeDefinition::builder()
        .attribute_name("pk")
        .attribute_type(ScalarAttributeType::S)
        .build();

    let sk_ad = AttributeDefinition::builder()
        .attribute_name("sk")
        .attribute_type(ScalarAttributeType::S)
        .build();

    let pk_ks = KeySchemaElement::builder()
        .attribute_name("pk")
        .key_type(KeyType::Hash)
        .build();

    let sk_ks = KeySchemaElement::builder()
        .attribute_name("sk")
        .key_type(KeyType::Range)
        .build();

    let pt = ProvisionedThroughput::builder()
        .read_capacity_units(10)
        .write_capacity_units(10)
        .build();

    client
        .create_table()
        .table_name(table_name)
        .key_schema(pk_ks)
        .attribute_definitions(pk_ad)
        .key_schema(sk_ks)
        .attribute_definitions(sk_ad)
        .provisioned_throughput(pt)
        .send()
        .await?;

    wait_for_table_creation(table_name, client).await
}

async fn with_table<'a, F>(f: F) -> Result<()>
where
    F: FnOnce(String, Client) -> Box<dyn Future<Output = Result<()>> + Unpin> + 'static,
{
    let router = rynamodb::router();
    rynamodb::test_run_server(router, |port| {
        let table_name = format!("table-{}", uuid::Uuid::new_v4());
        Box::new(Box::pin(async move {
            let client = test_client(port).await;

            // create the table
            default_dynamodb_table(&table_name, &client).await?;

            // run the test closure
            let res = f(table_name.clone(), client.clone()).await;

            match client.delete_table().table_name(&table_name).send().await {
                Ok(_) => {}
                Err(e) if targeting_aws() => {
                    return Err(eyre::eyre!("could not drop table {table_name}: {e:?}"));
                }
                _ => tracing::warn!(%table_name, "deleting table"),
            }

            res
        }))
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn create_table_invalid_input() {
    test_init();

    let router = rynamodb::router();
    rynamodb::test_run_server(router, |port| {
        Box::new(Box::pin(async move {
            let url = if targeting_aws() {
                format!("https://dynamodb.eu-west-2.amazonaws.com")
            } else {
                format!("http://localhost:{port}")
            };
            let client = reqwest::Client::new();
            let headers = {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HeaderName::from_static("x-amz-target"),
                    HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
                );
                headers.insert(
                    CONTENT_TYPE,
                    HeaderValue::from_static("application/x-amz-json-1.0"),
                );
                headers
            };
            let res = client
                .post(&url)
                .headers(headers)
                .body("invalid json")
                .send()
                .await;
            insta::assert_json_snapshot!(res.to_json_value().await);
            Ok(())
        }))
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn create_table() -> Result<()> {
    test_init();

    let router = rynamodb::router();
    rynamodb::test_run_server(router, |port| {
        Box::new(Box::pin(async move {
            let client = test_client(port).await;

            let pk_ad = AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build();

            let sk_ad = AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build();

            let pk_ks = KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build();

            let sk_ks = KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build();

            let pt = ProvisionedThroughput::builder()
                .read_capacity_units(10)
                .write_capacity_units(10)
                .build();

            let table_name = format!("table-{}", uuid::Uuid::new_v4());
            client
                .create_table()
                .table_name(&table_name)
                .key_schema(pk_ks)
                .attribute_definitions(pk_ad)
                .key_schema(sk_ks)
                .attribute_definitions(sk_ad)
                .provisioned_throughput(pt)
                .send()
                .await
                .wrap_err("sending request")?;

            wait_for_table_creation(&table_name, &client).await.wrap_err("waiting for table to be created")?;

            let res = client.describe_table().table_name(&table_name).send().await.wrap_err("describing table")?;

            let result = insta::with_settings!({ filters => vec![
                // table name
                (r"table-[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", "[table-name]"),
                // region
                (r"(eu-west-2|us-east-1)", "[region]"),
                // account id
                (r"[0-9]{12}", "[account]"),
                // table id
                (r"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", "[table-id]"),
                // datetime seconds
                (r"seconds:\s*\d+", "[seconds]"),
                // datetime nanoseconds
                (r"subsecond_nanos:\s*\d+", "[nanos]"),
            ] }, {
                std::panic::catch_unwind(|| {
                    insta::assert_debug_snapshot!(res);
                })
            });

            // delete the table
            match client.delete_table().table_name(&table_name).send().await {
                Ok(_) => {}
                Err(e) if targeting_aws() => {
                    return Err(eyre::eyre!("could not drop table {table_name}: {e:?}"));
                }
                _ => tracing::warn!(%table_name, "deleting table"),
            }

            result.map_err(|e| eyre::eyre!("snapshot did not match: {e:?}"))
        }))
    })
    .await
    .expect("running test server framework");
    Ok(())
}

#[tokio::test]
async fn delete_table() {
    test_init();

    skip_aws_cloud!();

    let router = rynamodb::router();
    rynamodb::test_run_server(router, |port| {
        let table_names = vec![
            format!("table-{}", uuid::Uuid::new_v4()),
            format!("table-{}", uuid::Uuid::new_v4()),
        ];
        Box::new(Box::pin(async move {
            let client = test_client(port).await;

            for table_name in &table_names {
                default_dynamodb_table(table_name, &client).await?;
            }

            let to_delete_table_name = &table_names[0];
            let to_keep_table_name = &table_names[1];

            client
                .delete_table()
                .table_name(to_delete_table_name)
                .send()
                .await?;

            let res = client.list_tables().send().await?;

            client
                .delete_table()
                .table_name(to_keep_table_name)
                .send()
                .await?;

            assert_eq!(res.table_names, Some(vec![to_keep_table_name.to_string()]));

            Ok(())
        }))
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn put_item() -> Result<()> {
    test_init();

    with_table(|table_name, client| {
        Box::new(Box::pin(async move {
            let res = client
                .put_item()
                .table_name(table_name)
                .item("pk", AttributeValue::S("abc".to_string()))
                .item("sk", AttributeValue::S("def".to_string()))
                .send()
                .await
                .wrap_err("inserting item")?;

            let result = std::panic::catch_unwind(|| {
                insta::assert_debug_snapshot!(res);
            });

            result.map_err(|e| eyre::eyre!("snapshot did not match: {e:?}"))
        }))
    })
    .await
}

#[tokio::test]
async fn list_tables() {
    test_init();

    with_table(|_table_name, client| {
        Box::new(Box::pin(async move {
            let res = client
                .list_tables()
                .send()
                .await
                .wrap_err("listing tables")?;

            insta::with_settings!({ filters => vec![
                // table name
                (r"table-[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", "[table-name]"),
            ]}, {
                std::panic::catch_unwind(|| {
                    insta::assert_debug_snapshot!(res);
                })
                .map_err(|e| eyre::eyre!("snapshot did not match: {e:?}"))
            })
        }))
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn round_trip() {
    test_init();

    // check that we can insert and fetch data from rynamodb
    with_table(|table_name, client| {
        Box::new(Box::pin(async move {
            client
                .put_item()
                .table_name(&table_name)
                .item("pk", AttributeValue::S("abc".to_string()))
                .item("sk", AttributeValue::S("def".to_string()))
                .send()
                .await
                .wrap_err("inserting item")?;

            let res = client
                .query()
                .table_name(&table_name)
                .key_condition_expression("pk = :a AND sk = :b")
                .expression_attribute_values(":a", AttributeValue::S("abc".to_string()))
                .expression_attribute_values(":b", AttributeValue::S("def".to_string()))
                .send()
                .await
                .wrap_err("performing query")?;

            // TODO: we have to sort the returned items somehow, or else the snapshotting won't
            // work.

            let expected_items = {
                let mut h = HashMap::new();
                h.insert("pk".to_string(), AttributeValue::S("abc".to_string()));
                h.insert("sk".to_string(), AttributeValue::S("def".to_string()));
                h
            };
            let expected_output = aws_sdk_dynamodb::output::QueryOutput::builder()
                .items(expected_items)
                .count(1)
                .scanned_count(1)
                .build();

            assert_eq!(res, expected_output);

            Ok(())

            //             let result = insta::with_settings!({ filters => vec![
            //                 // table name
            //                 (r"table-[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", "[table-name]"),
            //                 // region
            //                 (r"(eu-west-2|us-east-1)", "[region]"),
            //                 // account id
            //                 (r"[0-9]{12}", "[account]"),
            //                 // table id
            //                 (r"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", "[table-id]"),
            //                 // datetime seconds
            //                 (r"seconds:\s*\d+", "[seconds]"),
            //                 // datetime nanoseconds
            //                 (r"subsecond_nanos:\s*\d+", "[nanos]"),
            //             ] }, {
            //                 std::panic::catch_unwind(|| {
            //                     insta::assert_debug_snapshot!(res);
            //                 })
            //             });

            // result.map_err(|e| eyre::eyre!("snapshot did not match: {e:?}"))
        }))
    })
    .await
    .unwrap();
}

// TODO: sort the results so that they are stable
#[tokio::test]
#[ignore]
async fn scan_table() {
    test_init();

    with_table(|table_name, client| {
        Box::new(Box::pin(async move {
            // add two items
            client
                .put_item()
                .table_name(&table_name)
                .item("pk", AttributeValue::S("123".to_string()))
                .item("sk", AttributeValue::S("456".to_string()))
                .item("value", AttributeValue::S("789".to_string()))
                .send()
                .await
                .wrap_err("inserting item")?;

            client
                .put_item()
                .table_name(&table_name)
                .item("pk", AttributeValue::S("abc".to_string()))
                .item("sk", AttributeValue::S("def".to_string()))
                .item("value", AttributeValue::S("ghi".to_string()))
                .send()
                .await
                .wrap_err("inserting item")?;

            let res = client
                .scan()
                .table_name(&table_name)
                .send()
                .await
                .wrap_err("scanning the table")?;

            let expected_items1 = {
                let mut h = HashMap::new();
                h.insert("pk".to_string(), AttributeValue::S("123".to_string()));
                h.insert("sk".to_string(), AttributeValue::S("456".to_string()));
                h.insert("value".to_string(), AttributeValue::S("789".to_string()));
                h
            };

            let expected_items2 = {
                let mut h = HashMap::new();
                h.insert("pk".to_string(), AttributeValue::S("abc".to_string()));
                h.insert("sk".to_string(), AttributeValue::S("def".to_string()));
                h.insert("value".to_string(), AttributeValue::S("ghi".to_string()));
                h
            };

            // TODO: stable sort
            let expected_output = aws_sdk_dynamodb::output::ScanOutput::builder()
                .items(expected_items1)
                .items(expected_items2)
                .count(2)
                .scanned_count(2)
                .build();

            assert_eq!(res, expected_output);

            Ok(())
        }))
    })
    .await
    .unwrap();
}

#[derive(PartialEq, Debug)]
struct SortableItem {
    name: String,
    value: AttributeValue,
}

impl Eq for SortableItem {}

impl PartialOrd for SortableItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Ord for SortableItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

#[tokio::test]
async fn get_item() {
    test_init();

    // check that we can insert and fetch data from rynamodb
    with_table(|table_name, client| {
        Box::new(Box::pin(async move {
            client
                .put_item()
                .table_name(&table_name)
                .item("pk", AttributeValue::S("abc".to_string()))
                .item("sk", AttributeValue::S("def".to_string()))
                .item("value", AttributeValue::S("ghi".to_string()))
                .send()
                .await
                .wrap_err("inserting item")?;

            let res = client
                .get_item()
                .table_name(&table_name)
                .key("pk", AttributeValue::S("abc".to_string()))
                .key("sk", AttributeValue::S("def".to_string()))
                .send()
                .await
                .wrap_err("getting item that exists")?;

            // similar to queries, the sort is unstable so we have to compare the result without
            // using snapshots :(
            let expected = GetItemOutput::builder()
                .item("pk", AttributeValue::S("abc".to_string()))
                .item("sk", AttributeValue::S("def".to_string()))
                .item("value", AttributeValue::S("ghi".to_string()))
                .build();
            assert_eq!(res, expected);

            // get an item that doesnt exist
            let res = client
                .get_item()
                .table_name(&table_name)
                .key("pk", AttributeValue::S("something-else".to_string()))
                .key("sk", AttributeValue::S("def".to_string()))
                .send()
                .await
                .wrap_err("getting item that does not exists")?;

            let expected = GetItemOutput::builder().build();
            assert_eq!(res, expected);

            Ok(())
        }))
    })
    .await
    .unwrap();
}

// tables

// test describing a non-existent table
#[tokio::test]
async fn describe_nonexistent_table() {
    test_init();

    let router = rynamodb::router();
    rynamodb::test_run_server(router, |port| {
        Box::new(Box::pin(async move {
            let client = test_client(port).await;
            let res = client
                .describe_table()
                .table_name("non-existent-table")
                .send()
                .await;

            insta::assert_json_snapshot!(res.to_json_value().await);
            Ok(())
        }))
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn scan_missing_table() {
    test_init();

    with_table(|_table_name, client| {
        Box::new(Box::pin(async move {
            let res = client.scan().table_name("invalid table").send().await;
            insta::assert_json_snapshot!(res.to_json_value().await);
            Ok(())
        }))
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn get_item_missing_table() {
    test_init();

    with_table(|_table_name, client| {
        Box::new(Box::pin(async move {
            let res = client
                .get_item()
                .table_name("invalid-table")
                .key("pk", AttributeValue::S("abc".to_string()))
                .key("sk", AttributeValue::S("def".to_string()))
                .send()
                .await;
            insta::assert_json_snapshot!(res.to_json_value().await);
            Ok(())
        }))
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn put_item_missing_table() {
    test_init();

    with_table(|_table_name, client| {
        Box::new(Box::pin(async move {
            let res = client
                .put_item()
                .table_name("nonexistent-table")
                .item("pk", AttributeValue::S("abc".to_string()))
                .item("sk", AttributeValue::S("def".to_string()))
                .send()
                .await;

            insta::assert_json_snapshot!(res.to_json_value().await);
            Ok(())
        }))
    })
    .await
    .unwrap();
}

async fn create_client(endpoint_url: Option<&str>) -> aws_sdk_dynamodb::Client {
    match endpoint_url {
        Some(url) => {
            let config = aws_config::from_env().endpoint_url(url).load().await;
            Client::new(&config)
        }
        None => {
            let config = aws_config::load_from_env().await;
            Client::new(&config)
        }
    }
}
