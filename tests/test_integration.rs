use aws_sdk_dynamodb::{
    model::{
        AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType,
    },
    Client,
};

#[tokio::test]
async fn create_table() {
    let router = rynamodb::router();
    rynamodb::test_run_server(router, |port| {
        Box::new(Box::pin(async move {
            let endpoint_url = format!("http://127.0.0.1:{port}");
            let client = create_client(&endpoint_url).await;

            let ad = AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build();

            let ks = KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build();

            let pt = ProvisionedThroughput::builder()
                .read_capacity_units(10)
                .write_capacity_units(10)
                .build();

            let _res = client
                .create_table()
                .table_name("table")
                .key_schema(ks)
                .attribute_definitions(ad)
                .provisioned_throughput(pt)
                .send()
                .await
                .unwrap();

            Ok(())
        }))
    })
    .await
    .unwrap();
}

async fn create_client(endpoint_url: &str) -> aws_sdk_dynamodb::Client {
    let config = aws_config::from_env()
        .endpoint_url(endpoint_url)
        .load()
        .await;
    Client::new(&config)
}
