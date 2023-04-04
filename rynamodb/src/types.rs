use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{de::Unexpected, Deserialize, Serialize};
use serde_dynamo::AttributeValue;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct AttributeDefinition {
    pub attribute_name: String,
    #[serde(deserialize_with = "deserialize_attribute_type")]
    pub attribute_type: AttributeType,
}

fn deserialize_attribute_type<'de, D>(deserializer: D) -> Result<AttributeType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;

    match buf.as_str() {
        "S" => Ok(AttributeType::S),
        "B" => Ok(AttributeType::B),
        "N" => Ok(AttributeType::N),
        // TODO
        s => Err(serde::de::Error::invalid_value(
            Unexpected::Str(s),
            &"a dynamodb definition of a type",
        )),
    }
}

/// The incoming payload for creating a table
#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTableInput {
    pub table_name: String,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub key_schema: Vec<KeySchema>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct KeySchema {
    pub attribute_name: String,
    pub key_type: KeyType,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum AttributeType {
    S,
    B,
    N,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub enum KeyType {
    HASH,
    RANGE,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeTableInput {
    pub table_name: String,
}

/// The resulting response payload for creating a table
#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTableOutput {
    pub table_description: TableDescription,
}

#[derive(Serialize, Debug, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TableDescription {
    pub table_name: Option<String>,
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    pub table_status: Option<String>,
    pub table_size_bytes: Option<usize>,
    pub item_count: Option<usize>,
    pub key_schema: Option<Vec<KeySchema>>,
    pub table_arn: Option<String>,
    pub table_id: Option<String>,
    pub creation_date_time: Option<i64>,
    pub provisioned_throughput: Option<ProvisionedThroughputDescription>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct PutItemInput {
    pub table_name: String,
    pub item: HashMap<String, AttributeValue>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct PutItemOutput {}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeTableOutput {
    pub table: TableDescription,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct QueryOutput {
    pub items: Vec<HashMap<String, AttributeValue>>,
    pub count: usize,
    pub scanned_count: usize,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteTableOutput {}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase", untagged)]
pub enum Response {
    CreateTable(CreateTableOutput),
    PutItem(PutItemOutput),
    DescribeTable(DescribeTableOutput),
    Query(QueryOutput),
    DeleteTable(DeleteTableOutput),
    GetItem(GetItemOutput),
    ListTables(ListTablesOutput),
    BatchWriteItem(BatchWriteItemOutput),
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct ProvisionedThroughputDescription {
    last_increase_date_time: Option<DateTime<Utc>>,
    last_decrease_date_time: Option<DateTime<Utc>>,
    number_of_decreases_today: Option<usize>,
    read_capacity_units: Option<u64>,
    write_capacity_units: Option<u64>,
}

impl Default for ProvisionedThroughputDescription {
    fn default() -> Self {
        Self {
            number_of_decreases_today: Some(0),
            read_capacity_units: Some(10),
            write_capacity_units: Some(10),
            last_increase_date_time: None,
            last_decrease_date_time: None,
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct QueryInput {
    pub table_name: String,
    pub key_condition_expression: String,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct GetItemInput {
    pub table_name: String,
    pub key: HashMap<String, AttributeValue>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct GetItemOutput {
    pub item: Option<HashMap<String, AttributeValue>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ListTablesInput {}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ListTablesOutput {
    pub table_names: Vec<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteTableInput {
    pub table_name: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ScanInput {
    pub table_name: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ScanOutput {
    pub items: Vec<HashMap<String, HashMap<String, String>>>,
    pub count: usize,
    pub scanned_count: usize,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct BatchWriteItemOutput {
    pub unprocessed_items: Option<HashMap<String, Vec<BatchPutRequest>>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct BatchPutRequest {
    pub put_request: BatchPutRequestItem,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct BatchPutRequestItem {
    pub item: HashMap<String, AttributeValue>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct BatchWriteInput {
    pub request_items: HashMap<String, Vec<BatchPutRequest>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // test parsing
    #[test]
    fn batch_write_item() {
        let input = r#"
        {
            "RequestItems": {
                "table-c8e7d653-20a2-4b24-9a62-bbae884a7e8c": [{
                    "PutRequest": {
                        "Item": {
                            "sk": {
                                "S": "def"
                            },
                            "pk": {
                                "S": "abc"
                            }
                        }
                    }
                }]
            }
        }
        "#;
        let _: BatchWriteInput = serde_json::from_str(input).unwrap();
    }
}
