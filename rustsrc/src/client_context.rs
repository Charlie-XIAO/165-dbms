use std::collections::HashMap;
use std::fmt::Display;

use crate::db::Db;
use crate::parse::parse_column_var;

#[derive(Debug)]
pub struct Valvec {
    pub values: Vec<i32>,
}

#[derive(Debug)]
pub struct Posvec {
    pub positions: Vec<usize>,
}

#[derive(Debug)]
pub enum Numval {
    I32(i32),
    I64(i64),
    F64(f64),
}

impl Display for Numval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Numval::I32(val) => write!(f, "{}", val),
            Numval::I64(val) => write!(f, "{}", val),
            Numval::F64(val) => write!(f, "{:.2}", val),
        }
    }
}

#[derive(Debug, Default)]
pub struct ClientContext {
    pub valvecs: HashMap<String, Valvec>,
    pub posvecs: HashMap<String, Posvec>,
    pub numvals: HashMap<String, Numval>,
}

impl ClientContext {
    pub fn lookup_values_by_name<'a>(&'a self, name: &str, db: &'a Db) -> Option<&'a Vec<i32>> {
        match self.valvecs.get(name) {
            Some(valvec) => Some(&valvec.values),
            None => {
                let (_, table_name, column_name) = match parse_column_var(name) {
                    Ok(tup) => tup,
                    Err(_) => return None,
                };
                db.tables.get(table_name).and_then(|table| {
                    table
                        .columns
                        .iter()
                        .find(|col| col.name == column_name)
                        .map(|col| &col.data)
                })
            },
        }
    }
}
