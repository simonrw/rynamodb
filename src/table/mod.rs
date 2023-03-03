use std::collections::HashMap;
use thiserror::Error;

use self::queries::{Node, Operator};

mod queries;

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
}

pub type Result<T> = std::result::Result<T, TableError>;

#[derive(Default)]
pub struct Table {
    partition_key: String,
    sort_key: Option<String>,
    /// map partition key to partitions
    partitions: HashMap<String, Partition>,
}

impl Table {
    pub fn new(options: TableOptions) -> Self {
        Self {
            partition_key: options.partition_key,
            sort_key: options.sort_key,
            ..Default::default()
        }
    }

    pub fn insert(&mut self, attributes: HashMap<String, Attribute>) -> Result<()> {
        let partition_key_value = attributes
            .get(&self.partition_key)
            .ok_or(TableError::MissingPartitionKey)?;
        let partition = self
            .partitions
            .entry(
                partition_key_value
                    .to_string()
                    .expect("key attribute type is not convertible to a string"),
            )
            .or_insert_with(|| {
                tracing::debug!(?partition_key_value, "creating new partition");
                Default::default()
            });
        partition.insert(attributes);
        Ok(())
    }

    pub fn statistics(&self) -> Statistics {
        Statistics {
            num_partitions: self.partitions.len(),
        }
    }

    pub(crate) fn query(
        &self,
        key_condition_expression: &str,
        expression_attribute_names: HashMap<&str, &str>,
        expression_attribute_values: HashMap<&str, &str>,
    ) -> Result<Vec<HashMap<String, Attribute>>> {
        let ast = queries::parse(key_condition_expression)?;
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
                    (Node::Placeholder(key_name), Node::Placeholder(value_name)) => {
                        let key = expression_attribute_names
                            .get(format!("#{key_name}").as_str())
                            .ok_or_else(|| TableError::NoAttributeName(key_name.to_string()))?;

                        if key != &self.partition_key {
                            return Err(TableError::InvalidPartitionKey);
                        }

                        let value = expression_attribute_values
                            .get(format!(":{value_name}").as_str())
                            .ok_or_else(|| TableError::NoAttributeValue(value_name.to_string()))?;
                        match self.partitions.get(*value) {
                            Some(p) => Ok(p.rows.clone()),
                            None => Ok(Vec::new()),
                        }
                    }
                    (Node::Attribute(key), Node::Placeholder(value_name)) => {
                        if key != &self.partition_key {
                            return Err(TableError::InvalidPartitionKey);
                        }
                        let value = expression_attribute_values
                            .get(format!(":{value_name}").as_str())
                            .ok_or_else(|| TableError::NoAttributeValue(value_name.to_string()))?;
                        match self.partitions.get(*value) {
                            Some(p) => Ok(p.rows.clone()),
                            None => Ok(Vec::new()),
                        }
                    }
                    (Node::Placeholder(key_name), Node::Attribute(value)) => {
                        let key = expression_attribute_names
                            .get(format!("#{key_name}").as_str())
                            .ok_or_else(|| TableError::NoAttributeName(key_name.to_string()))?;
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
                                .ok_or_else(|| TableError::InvalidPartitionKey)?;

                            // delegate to the partition
                            // the rhs _must_ be the sk
                            partition.query(
                                *rhs,
                                &expression_attribute_names,
                                &expression_attribute_values,
                            )
                        }
                        (l, r) => unreachable!("lhs: {l:?} rhs: {r:?}"),
                    },
                    n => unreachable!("node: {n:?}"),
                }
            }
            _ => todo!(),
        }
    }
}

pub struct Statistics {
    pub num_partitions: usize,
}

#[derive(Clone)]
pub struct TableOptions {
    partition_key: String,
    sort_key: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Attribute {
    String(String),
}

impl Attribute {
    pub fn to_string(&self) -> Result<String> {
        match self {
            Attribute::String(s) => Ok(s.clone()),
        }
    }
}

#[derive(Default)]
pub struct Partition {
    rows: Vec<HashMap<String, Attribute>>,
}

impl Partition {
    pub fn insert(&mut self, attributes: HashMap<String, Attribute>) {
        self.rows.push(attributes);
    }

    fn query(
        &self,
        ast: Node,
        expression_attribute_names: &HashMap<&str, &str>,
        expression_attribute_values: &HashMap<&str, &str>,
    ) -> Result<Vec<HashMap<String, Attribute>>> {
        match ast {
            Node::Binop { lhs, rhs, op } => match (lhs.as_ref(), rhs.as_ref(), op) {
                (Node::Attribute(key), Node::Attribute(value), Operator::Eq) => Ok(self
                    .rows
                    .iter()
                    .filter(|row| {
                        row.get(key.as_str())
                            .map(|v| match v {
                                Attribute::String(s) => value == s,
                            })
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect()),
                (Node::Placeholder(key_name), Node::Attribute(value), Operator::Eq) => {
                    let key = expression_attribute_names
                        .get(format!("#{key_name}").as_str())
                        .ok_or_else(|| TableError::NoAttributeName(key_name.to_string()))?;

                    Ok(self
                        .rows
                        .iter()
                        .filter(|row| {
                            row.get(*key)
                                .map(|v| match v {
                                    Attribute::String(s) => value == s,
                                })
                                .unwrap_or(false)
                        })
                        .cloned()
                        .collect())
                }
                (Node::Attribute(key), Node::Placeholder(value_name), Operator::Eq) => {
                    let value = expression_attribute_values
                        .get(format!(":{value_name}").as_str())
                        .ok_or_else(|| TableError::NoAttributeValue(value_name.to_string()))?;

                    Ok(self
                        .rows
                        .iter()
                        .filter(|row| {
                            row.get(key.as_str())
                                .map(|v| match v {
                                    Attribute::String(s) => value == s,
                                })
                                .unwrap_or(false)
                        })
                        .cloned()
                        .collect())
                }
                (Node::Placeholder(key_name), Node::Placeholder(value_name), Operator::Eq) => {
                    let key = expression_attribute_names
                        .get(format!("#{key_name}").as_str())
                        .ok_or_else(|| TableError::NoAttributeName(key_name.to_string()))?;

                    let value = expression_attribute_values
                        .get(format!(":{value_name}").as_str())
                        .ok_or_else(|| TableError::NoAttributeValue(value_name.to_string()))?;

                    Ok(self
                        .rows
                        .iter()
                        .filter(|row| {
                            row.get(*key)
                                .map(|v| match v {
                                    Attribute::String(s) => value == s,
                                })
                                .unwrap_or(false)
                        })
                        .cloned()
                        .collect())
                }
                (l, r, o) => todo!("lhs: {l:?}, rhs: {r:?}, op: {o:?}"),
            },
            _ => todo!("unhandled query for secondary: {ast:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_logging() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    fn default_table() -> Table {
        let table = Table::new(TableOptions {
            partition_key: "pk".to_string(),
            sort_key: Some("sk".to_string()),
        });

        table
    }

    macro_rules! insert_into_table {
        ($table:ident, $($key:expr => $value:expr),+) => {{
            let mut attributes = HashMap::new();
            $(
                attributes.insert($key.to_string(), Attribute::String($value.to_string()));
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

            let expression_attribute_names: HashMap<_, _> = [("#K", "pk")].into_iter().collect();
            let expression_attribute_values: HashMap<_, _> =
                [(":val", "abc")].into_iter().collect();

            let rows = table
                .query(
                    query,
                    expression_attribute_names,
                    expression_attribute_values,
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

            let expression_attribute_names: HashMap<_, _> =
                [("#K", "pk"), ("#S", "sk")].into_iter().collect();
            let expression_attribute_values: HashMap<_, _> =
                [(":val", "abc"), (":other", "def")].into_iter().collect();

            let rows = table
                .query(
                    query,
                    expression_attribute_names,
                    expression_attribute_values,
                )
                .unwrap();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows.into_iter().next().unwrap(), attributes);
        }
    }
}
