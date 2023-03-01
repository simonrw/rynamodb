use std::collections::HashMap;

/// Handle the creation and destruction of tables
#[derive(Default)]
pub struct TableManager {
    pub per_account: TablesPerAccount,
}

#[derive(Default)]
pub struct TablesPerAccount(HashMap<String, TablesPerRegion>);

#[derive(Default)]
pub struct TablesPerRegion {
    pub account: String,
    pub region: String,
    pub tables: HashMap<String, crate::table::Table>,
}
