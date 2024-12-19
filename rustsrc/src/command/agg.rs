use super::Command;
use crate::client_context::{ClientContext, Numval};
use crate::db::Db;
use crate::errors::DbError;
use crate::message::ServerMessage;
use crate::parse::ParserContext;

#[derive(Debug)]
pub enum AggCommandType {
    Min,
    Max,
    Sum,
    Avg,
}

impl AggCommandType {
    fn from_flag(flag: u8) -> Self {
        match flag {
            0 => AggCommandType::Min,
            1 => AggCommandType::Max,
            2 => AggCommandType::Sum,
            3 => AggCommandType::Avg,
            _ => panic!("Invalid flag for AggCommandType"),
        }
    }

    pub fn to_flag(&self) -> u8 {
        match self {
            AggCommandType::Min => 0,
            AggCommandType::Max => 1,
            AggCommandType::Sum => 2,
            AggCommandType::Avg => 3,
        }
    }
}

#[derive(Debug)]
pub struct AggCommand<'a> {
    out: Option<&'a str>,
    agg_type: AggCommandType,
    valvec_name: &'a str,
}

impl Command for AggCommand<'_> {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized,
    {
        let args = ctx.args;
        if args.len() != 1 {
            return (None, ServerMessage::InvalidCommand);
        }

        (
            Some(Box::new(AggCommand {
                out: ctx.handles_str,
                agg_type: AggCommandType::from_flag(ctx.flag),
                valvec_name: args[0],
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

        let computed_numval = match self.agg_type {
            AggCommandType::Min => Numval::I32(*values.iter().min().unwrap_or(&i32::MIN)),
            AggCommandType::Max => Numval::I32(*values.iter().max().unwrap_or(&i32::MAX)),
            AggCommandType::Sum => Numval::I64(values.iter().map(|&x| x as i64).sum()),
            AggCommandType::Avg => Numval::F64(if values.is_empty() {
                0.0
            } else {
                values.iter().map(|&x| x as i64).sum::<i64>() as f64 / values.len() as f64
            }),
        };
        cc.numvals.insert(out.to_string(), computed_numval);
        ServerMessage::Ok
    }
}
