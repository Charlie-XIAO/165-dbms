#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("Database does not exist")]
    DbNotExist,
    #[error("Database already exists")]
    DbAlreadyExist,
    #[error("Table does not exist")]
    TableNotExist,
    #[error("Table already exists")]
    TableAlreadyExist,
    #[error("Table cannot hold more columns")]
    TableFull,
    #[error("Column does not exist")]
    ColumnNotExist,
    #[error("Column already exists")]
    ColumnAlreadyExist,
    #[error("Variable does not include a database component")]
    VarNoDb,
    #[error("Variable does not include a table component")]
    VarNoTable,
    #[error("Variable does not include a column component")]
    VarNoColumn,
    #[error("Value vector does not exist")]
    ValvecNotExist,
    #[error("Position vector does not exist")]
    PosvecNotExist,
    #[error("Numeric value does not exist")]
    NumvalNotExist,
    #[error("Internal execution error: {0}")]
    Internal(String),
}
