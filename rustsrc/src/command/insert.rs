use super::Command;
use crate::client_context::ClientContext;
use crate::db::Db;
use crate::errors::DbError;
use crate::message::ServerMessage;
use crate::parse::{parse_table_var, ParserContext};

#[derive(Debug)]
pub struct InsertCommand<'a> {
    table_var: &'a str,
    values: Vec<i32>,
}

impl Command for InsertCommand<'_> {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized,
    {
        let args = ctx.args;
        if args.is_empty() {
            return (None, ServerMessage::InvalidCommand);
        }

        (
            Some(Box::new(InsertCommand {
                table_var: args[0],
                values: match args[1..]
                    .iter()
                    .map(|&x| x.parse())
                    .collect::<Result<Vec<i32>, _>>()
                {
                    Ok(values) => values,
                    Err(_) => return (None, ServerMessage::InvalidCommand),
                },
            })),
            ServerMessage::Ok,
        )
    }

    fn execute(&self, db: &mut Db, _: &mut ClientContext) -> ServerMessage {
        let (_, table_name) = match parse_table_var(self.table_var) {
            Ok(tup) => tup,
            Err(_) => return ServerMessage::ExecutionError(DbError::TableNotExist.to_string()),
        };

        let table = match db.tables.get_mut(table_name) {
            Some(table) => table,
            None => return ServerMessage::ExecutionError(DbError::TableNotExist.to_string()),
        };

        match table.insert_row(&self.values) {
            Ok(_) => ServerMessage::Ok,
            Err(e) => ServerMessage::ExecutionError(e.to_string()),
        }
    }
}
