use eyre::Result;
use std::collections::HashMap;
use std::fmt;

use crate::{table, types};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    UsEast1,
}

impl Default for Region {
    fn default() -> Self {
        Self::UsEast1
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Region::UsEast1 => write!(f, "us-east-1"),
        }
    }
}

/// Handle the creation and destruction of tables
#[derive(Default)]
pub struct TableManager {
    // map from account to the tables in that account broken down by region
    pub per_account: HashMap<String, TablesPerRegion>,
}

impl TableManager {
    pub fn new_table(
        &mut self,
        account: impl Into<String>,
        region: Region,
        input: types::CreateTableInput,
    ) -> Result<table::Table> {
        let account_id = account.into();
        let table = table::Table::new(region, &account_id, input.into());

        let entry = self.per_account.entry(account_id).or_default();
        entry.tables.insert(region, table.clone());
        Ok(table)
    }

    pub fn get_table(&self, table_name: &str) -> Option<&table::Table> {
        for account in self.per_account.values() {
            for table in account.tables.values() {
                tracing::trace!(created_table_name = %table.name, requested_table_name = %table_name, "checking table name");
                if table.name == table_name {
                    return Some(table);
                }
            }
        }

        tracing::debug!(%table_name, "could not find table");

        None
    }

    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut table::Table> {
        for account in self.per_account.values_mut() {
            for table in account.tables.values_mut() {
                if table.name == table_name {
                    return Some(table);
                }
            }
        }

        None
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut table_names = Vec::new();
        for account in self.per_account.values() {
            for table in account.tables.values() {
                table_names.push(table.name.clone());
            }
        }
        table_names
    }

    pub fn delete_table(&mut self, table_name: &str) -> Result<()> {
        for account in self.per_account.values_mut() {
            account.remove(table_name);
        }
        Ok(())
    }

    pub fn batch_write_item(
        &mut self,
        input: types::BatchWriteInput,
    ) -> HashMap<String, Vec<types::BatchPutRequest>> {
        let mut unprocessed_items: HashMap<String, Vec<_>> = HashMap::new();
        for (table_name, put_request) in input.request_items.into_iter() {
            match self.get_table_mut(&table_name) {
                Some(table) => {
                    for req in put_request {
                        let item = req.put_request.item.clone();
                        match table.insert(item.clone()) {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::warn!(error = %e, "could not insert item");
                                unprocessed_items
                                    .entry(table_name.clone())
                                    .or_default()
                                    .push(req.clone());
                            }
                        }
                    }
                }
                None => {
                    for req in put_request {
                        unprocessed_items
                            .entry(table_name.clone())
                            .or_default()
                            .push(req.clone());
                    }
                }
            }
        }
        unprocessed_items
    }
}

#[derive(Default)]
pub struct TablesPerRegion {
    // map from region to table
    pub tables: HashMap<Region, table::Table>,
}

impl TablesPerRegion {
    fn remove(&mut self, table_name: &str) {
        self.tables
            .retain(|_region, table| table.name != table_name);
    }
}
