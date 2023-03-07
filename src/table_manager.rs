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
                if table.name == table_name {
                    return Some(table);
                }
            }
        }

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
}

#[derive(Default)]
pub struct TablesPerRegion {
    // map from region to table
    pub tables: HashMap<Region, table::Table>,
}
