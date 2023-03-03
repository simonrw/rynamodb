use std::{collections::HashMap, future::Future};

use aws_sdk_dynamodb::{
    model::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    },
    output::CreateTableOutput,
    Client,
};
use eyre::{Context, Result};

fn init_logging() {
    let _ = tracing_subscriber::fmt::try_init();
}

async fn test_client(port: u16) -> Client {
    if std::env::var("TEST_TARGET").unwrap_or_else(|_| String::new()) == "AWS_CLOUD" {
        eprintln!("creating client against AWS");
        create_client(None).await
    } else {
        println!("creating local client");
        let endpoint_url = format!("http://127.0.0.1:{port}");
        let client = create_client(Some(&endpoint_url)).await;
        client
    }
}

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
    Ok(())
}

async fn with_table<'a, F>(table_name: &'a str, f: F) -> Result<()>
where
    F: FnOnce(String, Client) -> Box<dyn Future<Output = Result<()>> + Unpin> + 'static,
{
    let router = rynamodb::router();
    rynamodb::test_run_server(router, |port| {
        let table_name = table_name.to_string();
        Box::new(Box::pin(async move {
            let endpoint_url = format!("http://127.0.0.1:{port}");
            let client = create_client(Some(&endpoint_url)).await;

            // create the table
            default_dynamodb_table(&table_name, &client).await?;

            // run the test closure
            let res = f(table_name.clone(), client.clone()).await;

            // TODO: drop table
            res
        }))
    })
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn create_table() -> Result<()> {
    init_logging();

    color_eyre::install().unwrap();
    tracing_subscriber::fmt::init();

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

            let res = client
                .create_table()
                .table_name("table")
                .key_schema(pk_ks)
                .attribute_definitions(pk_ad)
                .key_schema(sk_ks)
                .attribute_definitions(sk_ad)
                .provisioned_throughput(pt)
                .send()
                .await
                .wrap_err("sending request")?;

            // TODO: handle the arn
            insta::assert_debug_snapshot!(res);

            Ok(())
        }))
    })
    .await
    .expect("running test server framework");
    Ok(())
}

#[tokio::test]
async fn round_trip() {
    init_logging();

    // check that we can insert and fetch data from rynamodb
    with_table("table", |table_name, client| {
        Box::new(Box::pin(async move {
            client
                .put_item()
                .table_name(table_name)
                .item("pk", AttributeValue::S("abc".to_string()))
                .send()
                .await
                .wrap_err("inserting item")?;

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
