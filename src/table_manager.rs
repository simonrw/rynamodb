use eyre::Result;
use std::collections::HashMap;

use crate::{table, types};

#[derive(Clone, Copy)]
pub enum Region {
    UsEast1,
}

impl Default for Region {
    fn default() -> Self {
        Self::UsEast1
    }
}

/// Handle the creation and destruction of tables
#[derive(Default)]
pub struct TableManager {
    pub per_account: HashMap<String, TablesPerRegion>,
}

impl TableManager {
    pub fn new_table(
        &mut self,
        account: impl Into<String>,
        region: Region,
        input: types::CreateTableInput,
    ) -> Result<table::Table> {
        let table = table::Table::new(input.into());

        let account_id = account.into();
        let entry = self
            .per_account
            .entry(account_id.clone())
            .or_insert(TablesPerRegion {
                account: account_id,
                region,
                tables: HashMap::new(),
            });

        entry.tables.insert(table.name.clone(), table.clone());
        Ok(table)
    }

    pub fn get_table(&self, table_name: &str) -> Option<&table::Table> {
        for account in self.per_account.values() {
            for table in account.tables.values() {
                if table.name == table_name {
                    return Some(&table);
                }
            }
        }

        None
    }
}

#[derive(Default)]
pub struct TablesPerRegion {
    pub account: String,
    pub region: Region,
    pub tables: HashMap<String, table::Table>,
}
