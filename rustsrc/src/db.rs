use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::column::Column;
use crate::command::create::CreateCommandIndexType;
use crate::consts::{DB_PERSIST_CATALOG_FILE, DB_PERSIST_DIR};
use crate::errors::DbError;
use crate::parse::{parse_column_var, parse_table_var};
use crate::table::Table;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Db {
    pub name: Option<String>,
    pub tables: HashMap<String, Table>,
}

impl Db {
    pub fn launch() -> Result<Self, DbError> {
        let catalog_path = Path::new(DB_PERSIST_DIR).join(DB_PERSIST_CATALOG_FILE);
        if !catalog_path.exists() {
            println!("Database launched freshly (no catalog found)");
            return Ok(Db::default());
        }

        let start = std::time::Instant::now();
        let catalog = File::open(&catalog_path).map_err(|e| DbError::Internal(e.to_string()))?;
        let mut db: Db =
            bincode::deserialize_from(catalog).map_err(|e| DbError::Internal(e.to_string()))?;
        for table in db.tables.values_mut() {
            table.recreate_indexes(true);
        }
        println!("Database launched in: {:?}", start.elapsed());
        Ok(db)
    }

    pub fn shutdown(&self) -> Result<(), DbError> {
        let catalog_dir = Path::new(DB_PERSIST_DIR);
        if !catalog_dir.exists() {
            create_dir_all(catalog_dir).map_err(|e| DbError::Internal(e.to_string()))?;
        }

        let start = std::time::Instant::now();
        let catalog_path = catalog_dir.join(DB_PERSIST_CATALOG_FILE);
        let catalog = File::create(&catalog_path).map_err(|e| DbError::Internal(e.to_string()))?;
        bincode::serialize_into(catalog, self).map_err(|e| DbError::Internal(e.to_string()))?;
        println!("Database shutdown in: {:?}", start.elapsed());
        Ok(())
    }

    pub fn activate(&mut self, db_name: &str) -> Result<(), DbError> {
        self.name = Some(db_name.to_string());
        Ok(())
    }

    fn check_db_exists(&self, db_name: &str) -> Result<(), DbError> {
        if self.name.as_deref() != Some(db_name) {
            return Err(DbError::DbNotExist);
        }
        Ok(())
    }

    pub fn create_table(
        &mut self,
        db_var: &str,
        table_name: &str,
        n_cols: usize,
    ) -> Result<(), DbError> {
        self.check_db_exists(db_var)?;
        if self.tables.contains_key(table_name) {
            return Err(DbError::TableAlreadyExist);
        }

        self.tables
            .insert(table_name.to_string(), Table::new(n_cols));
        Ok(())
    }

    pub fn create_column(&mut self, table_var: &str, column_name: &str) -> Result<(), DbError> {
        let (db_name, table_name) = parse_table_var(table_var)?;
        self.check_db_exists(db_name)?;
        if !self.tables.contains_key(table_name) {
            return Err(DbError::TableNotExist);
        }

        let column_name = column_name.to_string();
        let table = self.tables.get_mut(table_name).unwrap();
        if table.columns.iter().any(|c| c.name == column_name) {
            return Err(DbError::ColumnAlreadyExist);
        }

        if table.columns.len() >= table.n_cols {
            return Err(DbError::TableFull);
        }
        table.columns.push(Column::new(column_name));
        Ok(())
    }

    pub fn create_index(
        &mut self,
        column_var: &str,
        index_type: CreateCommandIndexType,
    ) -> Result<(), DbError> {
        let (db_name, table_name, column_name) = parse_column_var(column_var)?;
        self.check_db_exists(db_name)?;
        if !self.tables.contains_key(table_name) {
            return Err(DbError::TableNotExist);
        }

        let table = self.tables.get_mut(table_name).unwrap();
        let ith_column = table
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or(DbError::ColumnNotExist)?;
        table.create_index(ith_column, index_type);
        Ok(())
    }

    pub fn load_csv<R>(&mut self, reader: &mut csv::Reader<R>) -> Result<(), DbError>
    where
        R: std::io::Read,
    {
        let headers = reader
            .headers()
            .map_err(|e| DbError::Internal(e.to_string()))?;
        let first_column_var = headers
            .get(0)
            .ok_or(DbError::Internal("Header is empty".to_string()))?;

        let (db_name, table_name, _) = parse_column_var(first_column_var)?;
        self.check_db_exists(db_name)?;
        if !self.tables.contains_key(table_name) {
            return Err(DbError::TableNotExist);
        }

        let table = self.tables.get_mut(table_name).unwrap();
        table.load_csv(reader)
    }
}
