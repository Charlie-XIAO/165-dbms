use super::Command;
use crate::client_context::ClientContext;
use crate::db::Db;
use crate::errors::DbError;
use crate::message::ServerMessage;
use crate::parse::ParserContext;

#[derive(Debug)]
pub struct PrintCommand<'a> {
    items: Vec<&'a str>,
}

impl Command for PrintCommand<'_> {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized,
    {
        let args = ctx.args;
        if args.is_empty() {
            return (None, ServerMessage::InvalidCommand);
        }
        (
            Some(Box::new(PrintCommand { items: args })),
            ServerMessage::Ok,
        )
    }

    fn execute(&self, db: &mut Db, cc: &mut ClientContext) -> ServerMessage {
        let is_numvals = cc.numvals.contains_key(self.items[0]);

        let payload = if is_numvals {
            let reprs: Vec<String> = self
                .items
                .iter()
                .filter_map(|&item| cc.numvals.get(item))
                .map(|numval| numval.to_string())
                .collect();
            if reprs.len() != self.items.len() {
                return ServerMessage::ExecutionError(DbError::NumvalNotExist.to_string());
            }
            reprs.join(",")
        } else {
            let columns: Vec<&Vec<i32>> = self
                .items
                .iter()
                .filter_map(|&item| cc.lookup_values_by_name(item, db))
                .collect();
            if columns.len() != self.items.len() {
                println!(
                    "columns.len() = {}, self.items.len() = {}",
                    columns.len(),
                    self.items.len()
                );
                return ServerMessage::ExecutionError(DbError::ValvecNotExist.to_string());
            }
            (0..columns[0].len())
                .map(|row_idx| {
                    columns
                        .iter()
                        .map(|col| {
                            col.get(row_idx)
                                .map_or("".to_string(), |val| val.to_string())
                        })
                        .collect::<Vec<String>>()
                        .join(",")
                })
                .collect::<Vec<String>>()
                .join("\n")
        };

        ServerMessage::OkWithPayload(payload)
    }
}
