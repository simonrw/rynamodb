use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TableError {
    #[error("missing partition key")]
    MissingPartitionKey,
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
}

pub struct Statistics {
    pub num_partitions: usize,
}

#[derive(Clone)]
pub struct TableOptions {
    partition_key: String,
    sort_key: Option<String>,
}

#[derive(Debug)]
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_logging() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    #[test]
    fn round_trip() {
        init_logging();

        let mut table = Table::new(TableOptions {
            partition_key: "pk".to_string(),
            sort_key: Some("sk".to_string()),
        });

        let mut attributes = HashMap::new();
        attributes.insert("pk".to_string(), Attribute::String("abc".to_string()));
        attributes.insert("sk".to_string(), Attribute::String("def".to_string()));
        attributes.insert("value".to_string(), Attribute::String("great".to_string()));
        table.insert(attributes).unwrap();

        let stats = table.statistics();

        assert_eq!(stats.num_partitions, 1);
    }
}
