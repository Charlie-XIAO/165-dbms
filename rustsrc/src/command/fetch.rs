use super::Command;
use crate::client_context::{ClientContext, Valvec};
use crate::db::Db;
use crate::errors::DbError;
use crate::message::ServerMessage;
use crate::parse::ParserContext;

#[derive(Debug)]
pub struct FetchCommand<'a> {
    out: Option<&'a str>,
    valvec_name: &'a str,
    posvec_name: &'a str,
}

impl Command for FetchCommand<'_> {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized,
    {
        let args = ctx.args;
        if args.len() != 2 {
            return (None, ServerMessage::InvalidCommand);
        }

        (
            Some(Box::new(FetchCommand {
                out: ctx.handles_str,
                valvec_name: args[0],
                posvec_name: args[1],
            })),
            ServerMessage::Ok,
        )
    }

    fn execute(&self, db: &mut Db, cc: &mut ClientContext) -> ServerMessage {
        let out = match self.out {
            Some(out) => out,
            None => return ServerMessage::Ok,
        };

        let values = match cc.lookup_values_by_name(self.valvec_name, db) {
            Some(values) => values,
            None => return ServerMessage::ExecutionError(DbError::ValvecNotExist.to_string()),
        };
        let positions = match cc.posvecs.get(self.posvec_name) {
            Some(posvec) => &posvec.positions,
            None => return ServerMessage::ExecutionError(DbError::PosvecNotExist.to_string()),
        };

        let fetched_valvec = Valvec {
            values: positions
                .iter()
                .map(|&pos| unsafe { *values.get_unchecked(pos) })
                .collect(),
        };
        cc.valvecs.insert(out.to_string(), fetched_valvec);
        ServerMessage::Ok
    }
}
