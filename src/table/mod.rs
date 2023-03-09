use chrono::{DateTime, Utc};
use serde_dynamo::AttributeValue;
use std::collections::HashMap;
use thiserror::Error;

use crate::{
    table_manager::Region,
    types::{self, AttributeDefinition, KeySchema, KeyType},
};

use self::queries::{Node, Operator};

mod queries;
mod visitor;

#[derive(Debug, Error)]
pub enum TableError {
    #[error("missing partition key")]
    MissingPartitionKey,
    #[error("parsing condition expression")]
    ParseError(#[from] queries::ParserError),
    #[error("partition key specified is not valid")]
    InvalidPartitionKey,
    #[error("attribute name {0} not supplied")]
    NoAttributeName(String),
    #[error("attribute value {0} not supplied")]
    NoAttributeValue(String),
    #[error("invalid attribute map, no types found")]
    InvalidAttributeMap,
}

pub type Result<T> = std::result::Result<T, TableError>;

#[derive(Default, Clone)]
pub struct Table {
    pub name: String,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub arn: String,
    pub table_id: String,
    pub created_at: DateTime<Utc>,
    pub provisioned_throughput: types::ProvisionedThroughputDescription,
    // internal information
    partition_key: String,
    sort_key: Option<String>,
    /// map partition key to partitions
    partitions: HashMap<String, Partition>,
}

impl Table {
    pub fn new(region: Region, account: impl Into<String>, options: TableOptions) -> Self {
        let table_name = options.name;
        Self {
            name: table_name.clone(),
            partition_key: options.partition_key,
            sort_key: options.sort_key,
            attribute_definitions: options.attribute_definitions,
            arn: format!(
                "arn:aws:dynamodb:{region}:{account}:table/{name}",
                account = account.into(),
                name = &table_name,
            ),
            table_id: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        }
    }

    pub fn insert(&mut self, attributes: HashMap<String, AttributeValue>) -> Result<()> {
        let partition_key_value = attributes
            .get(&self.partition_key)
            .ok_or(TableError::MissingPartitionKey)?;

        match partition_key_value {
            serde_dynamo::AttributeValue::S(partition_key_value) => {
                let partition = self
                    .partitions
                    .entry(partition_key_value.clone())
                    .or_insert_with(|| {
                        tracing::debug!(?partition_key_value, "creating new partition");
                        Default::default()
                    });
                partition.insert(attributes);
            }
            _ => todo!(),
        }

        Ok(())
    }

    pub fn statistics(&self) -> Statistics {
        Statistics {
            num_partitions: self.partitions.len(),
        }
    }

    pub fn description(&self) -> types::TableDescription {
        let mut key_schema = vec![KeySchema {
            attribute_name: self.partition_key.clone(),
            key_type: KeyType::HASH,
        }];

        if let Some(sk) = &self.sort_key {
            key_schema.push(KeySchema {
                attribute_name: sk.clone(),
                key_type: KeyType::RANGE,
            });
        }

        types::TableDescription {
            table_name: Some(self.name.clone()),
            table_status: Some("ACTIVE".to_string()),
            attribute_definitions: Some(self.attribute_definitions.clone()),
            table_size_bytes: Some(0),
            item_count: Some(self.len()),
            key_schema: Some(key_schema),
            table_arn: Some(self.arn.clone()),
            table_id: Some(self.table_id.clone()),
            // TODO
            creation_date_time: Some(self.created_at.timestamp_millis()),
            provisioned_throughput: Some(self.provisioned_throughput.clone()),
        }
    }

    fn len(&self) -> usize {
        self.partitions.values().map(|p| p.item_count()).sum()
    }

    pub fn query(
        &self,
        key_condition_expression: &str,
        expression_attribute_names: &Option<HashMap<String, String>>,
        expression_attribute_values: &Option<HashMap<String, AttributeValue>>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let ast = queries::parse(key_condition_expression)?;
        // remove placeholders
        let placeholder_remover =
            visitor::NodeVisitor::new(expression_attribute_names, expression_attribute_values);
        let ast = placeholder_remover.visit(ast);

        match ast {
            // simple equality check with the partition key
            Node::Binop { op, lhs, rhs } if op == queries::Operator::Eq => {
                match (lhs.as_ref(), rhs.as_ref()) {
                    (Node::Attribute(key), Node::Attribute(value)) => {
                        if key != &self.partition_key {
                            return Err(TableError::InvalidPartitionKey);
                        }

                        match self.partitions.get(value) {
                            Some(p) => Ok(p.rows.clone()),
                            None => Ok(Vec::new()),
                        }
                    }
                    (l, r) => unreachable!("lhs: {l:?} rhs: {r:?}"),
                }
            }
            Node::Binop { op, lhs, rhs } if op == queries::Operator::And => {
                // TODO: assume the lhs is the primary key for now
                let pk_query = lhs.as_ref();
                match pk_query {
                    Node::Binop {
                        lhs: pk_lhs,
                        rhs: pk_rhs,
                        // operator _must_ be =
                        ..
                    } => match (pk_lhs.as_ref(), pk_rhs.as_ref()) {
                        (Node::Attribute(_), Node::Attribute(value)) => {
                            let partition = self
                                .partitions
                                .get(value)
                                .ok_or(TableError::InvalidPartitionKey)?;

                            // delegate to the partition
                            // the rhs _must_ be the sk
                            partition.query(*rhs)
                        }
                        (l, r) => unreachable!("lhs: {l:?} rhs: {r:?}"),
                    },
                    n => unreachable!("node: {n:?}"),
                }
            }
            _ => todo!(),
        }
    }

    // key is something like {"pk": {"S": "def"}, "sk": {"S": "ghj"}}
    pub fn get_item(
        &self,
        key: HashMap<String, AttributeValue>,
    ) -> Option<HashMap<String, serde_dynamo::AttributeValue>> {
        assert!(key.len() >= 1);

        let partition_name = key.get(&self.partition_key).map(|k| match k {
            AttributeValue::S(s) => s,
            _ => unreachable!(),
        })?;
        let partition = self.partitions.get(partition_name)?;

        if let Some(sort_key) = &self.sort_key {
            let sort_key_value = key.get(sort_key).map(|k| match k {
                AttributeValue::S(s) => s,
                _ => unreachable!(),
            })?;
            partition.get_item(sort_key, sort_key_value.as_str())
        } else {
            partition.get_by_pk_only()
        }
    }
}

pub struct Statistics {
    pub num_partitions: usize,
}

#[derive(Clone)]
pub struct TableOptions {
    pub name: String,
    pub partition_key: String,
    pub sort_key: Option<String>,
    pub attribute_definitions: Vec<AttributeDefinition>,
}

impl From<types::CreateTableInput> for TableOptions {
    fn from(value: types::CreateTableInput) -> Self {
        let mut partition_key = String::new();
        let mut sort_key = None;

        for key_definition in value.key_schema {
            if key_definition.key_type == types::KeyType::HASH {
                partition_key = key_definition.attribute_name.clone();
            }

            if key_definition.key_type == types::KeyType::RANGE {
                sort_key = Some(key_definition.attribute_name.clone());
            }
        }

        if partition_key.is_empty() {
            // TODO
        }

        Self {
            name: value.table_name,
            partition_key,
            sort_key,
            attribute_definitions: value.attribute_definitions,
        }
    }
}

#[derive(Default, Clone)]
pub struct Partition {
    rows: Vec<HashMap<String, AttributeValue>>,
}

impl Partition {
    pub fn insert(&mut self, attributes: HashMap<String, AttributeValue>) {
        self.rows.push(attributes);
    }

    fn query(&self, ast: Node) -> Result<Vec<HashMap<String, AttributeValue>>> {
        match ast {
            Node::Binop { lhs, rhs, op } => match (lhs.as_ref(), rhs.as_ref(), op) {
                (Node::Attribute(key), Node::Attribute(value), Operator::Eq) => Ok(self
                    .rows
                    .iter()
                    .filter(|row| {
                        row.get(key.as_str())
                            .map(|v| match v {
                                serde_dynamo::AttributeValue::S(s) => value == s,
                                _ => todo!(),
                            })
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect()),
                (l, r, o) => todo!("lhs: {l:?}, rhs: {r:?}, op: {o:?}"),
            },
            _ => todo!("unhandled query for secondary: {ast:?}"),
        }
    }

    fn get_by_pk_only(&self) -> Option<HashMap<String, AttributeValue>> {
        self.rows.iter().cloned().next()
    }

    fn get_item(
        &self,
        sort_key_name: &str,
        sort_key_value: &str,
    ) -> Option<HashMap<String, AttributeValue>> {
        for row in &self.rows {
            let sk_value = row.get(sort_key_name)?;
            match sk_value {
                serde_dynamo::AttributeValue::S(sort_key) => {
                    if sort_key == sort_key_value {
                        return Some(row.clone());
                    }
                }
                _ => todo!("{sk_value:?}"),
            }
        }

        None
    }

    pub fn item_count(&self) -> usize {
        self.rows.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::types::AttributeType;

    use super::*;

    fn init_logging() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    fn default_table() -> Table {
        let table = Table::new(
            Region::UsEast1,
            crate::DEFAULT_ACCOUNT_ID,
            TableOptions {
                name: format!("table-{}", uuid::Uuid::new_v4()),
                partition_key: "pk".to_string(),
                sort_key: Some("sk".to_string()),
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".to_string(),
                        attribute_type: AttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".to_string(),
                        attribute_type: AttributeType::S,
                    },
                ],
            },
        );

        table
    }

    macro_rules! insert_into_table {
        ($table:ident, $($key:expr => $value:expr),+) => {{
            let mut attributes = HashMap::new();
            $(
                attributes.insert($key.to_string(), AttributeValue::S($value.to_string()));
            )+
            $table.insert(attributes.clone()).unwrap();
            attributes
        }};
    }

    #[test]
    fn pk_only() {
        init_logging();

        let queries = &["pk = abc", "#K = :val", "pk = :val", "#K = abc"];
        for query in queries {
            eprintln!("testing query {query}");
            let mut table = default_table();

            let attributes =
                insert_into_table!(table, "pk" => "abc", "sk" => "def", "value" => "great");

            let stats = table.statistics();
            assert_eq!(stats.num_partitions, 1);

            let expression_attribute_names: HashMap<_, _> =
                [("#K".to_string(), "pk".to_string())].into_iter().collect();
            let expression_attribute_values = {
                let mut res = HashMap::new();
                res.insert(":val".to_string(), AttributeValue::S("abc".to_string()));
                res
            };

            let rows = table
                .query(
                    query,
                    &Some(expression_attribute_names),
                    &Some(expression_attribute_values),
                )
                .unwrap();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows.into_iter().next().unwrap(), attributes);
        }
    }

    #[test]
    fn pk_and_sk_equality() {
        init_logging();

        let queries = &[
            "pk = abc AND sk = def",
            "pk = abc AND #S = def",
            "pk = abc AND sk = :other",
            "pk = abc AND #S = :other",
        ];
        for query in queries {
            eprintln!("testing query {query}");
            let mut table = default_table();

            let attributes =
                insert_into_table!(table, "pk" => "abc", "sk" => "def", "value" => "great");

            // insert an additional row to ensure that we don't return this value as well
            insert_into_table!(table, "pk" => "foobar", "sk" => "123", "another" => "something");

            let stats = table.statistics();
            assert_eq!(stats.num_partitions, 2);

            let expression_attribute_names: HashMap<_, _> = [
                ("#K".to_string(), "pk".to_string()),
                ("#S".to_string(), "sk".to_string()),
            ]
            .into_iter()
            .collect();
            let expression_attribute_values = {
                let mut res = HashMap::new();
                res.insert(":val".to_string(), AttributeValue::S("abc".to_string()));
                res.insert(":other".to_string(), AttributeValue::S("def".to_string()));
                res
            };

            let rows = table
                .query(
                    query,
                    &Some(expression_attribute_names),
                    &Some(expression_attribute_values),
                )
                .unwrap();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows.into_iter().next().unwrap(), attributes);
        }
    }
}
