use super::Command;
use crate::client_context::ClientContext;
use crate::db::Db;
use crate::message::ServerMessage;
use crate::parse::{trim_quotes, ParserContext};

#[derive(Debug, Clone, Copy)]
pub enum CreateCommandIndexType {
    None,
    UnclusteredSorted,
    UnclusteredBTree,
    ClusteredSorted,
    ClusteredBTree,
}

#[derive(Debug)]
pub enum CreateCommand<'a> {
    Db {
        db_name: &'a str,
    },
    Table {
        db_var: &'a str,
        table_name: &'a str,
        n_cols: usize,
    },
    Column {
        table_var: &'a str,
        column_name: &'a str,
    },
    Index {
        column_var: &'a str,
        index_type: CreateCommandIndexType,
    },
}

impl Command for CreateCommand<'_> {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized,
    {
        let args = ctx.args;

        match args.first() {
            Some(&"db") if args.len() == 2 => (
                Some(Box::new(CreateCommand::Db {
                    db_name: match trim_quotes(args[1]) {
                        (db_name, ServerMessage::Ok) => db_name,
                        _ => return (None, ServerMessage::InvalidCommand),
                    },
                })),
                ServerMessage::Ok,
            ),
            Some(&"tbl") if args.len() == 4 => (
                Some(Box::new(CreateCommand::Table {
                    table_name: match trim_quotes(args[1]) {
                        (table_name, ServerMessage::Ok) => table_name,
                        _ => return (None, ServerMessage::InvalidCommand),
                    },
                    db_var: args[2],
                    n_cols: match args[3].parse() {
                        Ok(n_cols) => n_cols,
                        Err(_) => return (None, ServerMessage::InvalidCommand),
                    },
                })),
                ServerMessage::Ok,
            ),
            Some(&"col") if args.len() == 3 => (
                Some(Box::new(CreateCommand::Column {
                    column_name: match trim_quotes(args[1]) {
                        (column_name, ServerMessage::Ok) => column_name,
                        _ => return (None, ServerMessage::InvalidCommand),
                    },
                    table_var: args[2],
                })),
                ServerMessage::Ok,
            ),
            Some(&"idx") if args.len() == 4 => (
                Some(Box::new(CreateCommand::Index {
                    column_var: args[1],
                    index_type: match (args[2], args[3]) {
                        ("sorted", "unclustered") => CreateCommandIndexType::UnclusteredSorted,
                        ("btree", "unclustered") => CreateCommandIndexType::UnclusteredBTree,
                        ("sorted", "clustered") => CreateCommandIndexType::ClusteredSorted,
                        ("btree", "clustered") => CreateCommandIndexType::ClusteredBTree,
                        _ => return (None, ServerMessage::InvalidCommand),
                    },
                })),
                ServerMessage::Ok,
            ),
            _ => (None, ServerMessage::InvalidCommand),
        }
    }

    fn execute(&self, db: &mut Db, _: &mut ClientContext) -> ServerMessage {
        match *self {
            CreateCommand::Db { db_name } => db.activate(db_name).into(),
            CreateCommand::Table {
                db_var,
                table_name,
                n_cols,
            } => db.create_table(db_var, table_name, n_cols).into(),
            CreateCommand::Column {
                table_var,
                column_name,
            } => db.create_column(table_var, column_name).into(),
            CreateCommand::Index {
                column_var,
                index_type,
            } => db.create_index(column_var, index_type).into(),
        }
    }
}
