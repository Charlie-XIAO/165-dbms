use serde::{Deserialize, Serialize};

use crate::errors::DbError;

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerMessage {
    Ok,
    OkTerminate,
    OkWithPayload(String),
    InvalidCommand,
    ParseError(String),
    BatchError(String),
    ExecutionError(String),
    UnknownExecutionError,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    RequestProcessCommand(String),
    RequestProcessCSV(u64),
}

impl From<Result<(), DbError>> for ServerMessage {
    fn from(value: Result<(), DbError>) -> Self {
        match value {
            Ok(_) => ServerMessage::Ok,
            Err(e) => ServerMessage::ExecutionError(e.to_string()),
        }
    }
}
