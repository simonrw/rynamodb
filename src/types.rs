use serde::{Deserialize, Serialize};

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
pub struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: AttributeType,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct KeySchema {
    pub attribute_name: String,
    pub key_type: KeyType,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum KeyType {
    HASH,
    RANGE,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AttributeType {
    S,
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
}

/*
*
  26       │-            key_schema: Some(
  27       │-                [
  28       │-                    KeySchemaElement {
  29       │-                        attribute_name: Some(
  30       │-                            "pk",
  31       │-                        ),
  32       │-                        key_type: Some(
  33       │-                            Hash,
  34       │-                        ),
  35       │-                    },
  36       │-                    KeySchemaElement {
  37       │-                        attribute_name: Some(
  38       │-                            "sk",
  39       │-                        ),
  40       │-                        key_type: Some(
  41       │-                            Range,
  42       │-                        ),
  43       │-                    },
  44       │-                ],
  45       │-            ),
  46       │-            table_status: Some(
  47       │-                Creating,
  48       │-            ),
  49       │-            creation_date_time: Some(
  50       │-                DateTime {
  51       │-                    seconds: 1677604318,
  52       │-                    subsecond_nanos: 355999946,
  53       │-                },
  54       │-            ),
  55       │-            provisioned_throughput: Some(
  56       │-                ProvisionedThroughputDescription {
  57       │-                    last_increase_date_time: None,
  58       │-                    last_decrease_date_time: None,
  59       │-                    number_of_decreases_today: Some(
  60       │-                        0,
  61       │-                    ),
  62       │-                    read_capacity_units: Some(
  63       │-                        10,
  64       │-                    ),
  65       │-                    write_capacity_units: Some(
  66       │-                        10,
  67       │-                    ),
  68       │-                },
  69       │-            ),
  70       │-            table_size_bytes: Some(
  71       │-                0,
  72       │-            ),
  73       │-            item_count: Some(
  74       │-                0,
  75       │-            ),
  76       │-            table_arn: Some(
  77       │-                "arn:aws:dynamodb:eu-west-2:678133472802:table/table",
  78       │-            ),
  79       │-            table_id: Some(
  80       │-                "d3558043-d3a6-42a5-b3cb-feca1edb1ef4",
  81       │-            ),
*/

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct PutItemInput {}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct PutItemOutput {}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeTableOutput {
    pub table: TableDescription,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase", untagged)]
pub enum Response {
    CreateTable(CreateTableOutput),
    PutItem(PutItemOutput),
    DescribeTable(DescribeTableOutput),
}
