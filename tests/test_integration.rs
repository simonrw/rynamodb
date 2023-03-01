use std::future::Future;

use aws_sdk_dynamodb::{
    model::{
        AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType,
    },
    output::CreateTableOutput,
    Client,
};
use eyre::{Context, Result};

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

async fn default_dynamodb_table(table_name: &str, client: &Client) -> Result<CreateTableOutput> {
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

    match client
        .create_table()
        .table_name(table_name)
        .key_schema(pk_ks)
        .attribute_definitions(pk_ad)
        .key_schema(sk_ks)
        .attribute_definitions(sk_ad)
        .provisioned_throughput(pt)
        .send()
        .await
    {
        Ok(_) => todo!(),
        Err(e) => eyre::bail!("bad: {e:?}"),
    }

    todo!()
}

async fn with_table<F>(
    table_name: &str,
    client: &Client,
    f: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(&Client) -> Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Unpin>,
{
    default_dynamodb_table(table_name, client).await;
    let res = f(client).await;
    // TODO: drop table
    res
}

#[tokio::test]
#[ignore]
async fn create_table() -> Result<()> {
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

// #[tokio::test]
// async fn foo() {
//     let router = rynamodb::router();
//     rynamodb::test_run_server(router, |port| {
//         Box::new(Box::pin(async move {
//             let endpoint_url = format!("http://127.0.0.1:{port}");
//             let client = create_client(Some(&endpoint_url)).await;

//             with_table("table", &client, |_client| {
//                 Box::new(Box::pin(async { Ok(()) }))
//             })
//             .await
//             .unwrap();
//             Ok(())
//         }))
//     })
//     .await
//     .unwrap();
// }

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
