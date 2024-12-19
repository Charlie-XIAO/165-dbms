use rayon::prelude::*;

use super::Command;
use crate::client_context::{ClientContext, Valvec};
use crate::db::Db;
use crate::errors::DbError;
use crate::message::ServerMessage;
use crate::parse::ParserContext;

#[derive(Debug)]
pub enum AddsubCommandType {
    Add,
    Sub,
}

impl AddsubCommandType {
    fn from_flag(flag: u8) -> Self {
        match flag {
            0 => AddsubCommandType::Add,
            1 => AddsubCommandType::Sub,
            _ => panic!("Invalid flag for AddsubCommandType"),
        }
    }

    pub fn to_flag(&self) -> u8 {
        match self {
            AddsubCommandType::Add => 0,
            AddsubCommandType::Sub => 1,
        }
    }
}

#[derive(Debug)]
pub struct AddsubCommand<'a> {
    out: Option<&'a str>,
    addsub_type: AddsubCommandType,
    valvec1_name: &'a str,
    valvec2_name: &'a str,
}

impl Command for AddsubCommand<'_> {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized,
    {
        let args = ctx.args;
        if args.len() != 2 {
            return (None, ServerMessage::InvalidCommand);
        }

        (
            Some(Box::new(AddsubCommand {
                out: ctx.handles_str,
                addsub_type: AddsubCommandType::from_flag(ctx.flag),
                valvec1_name: args[0],
                valvec2_name: args[1],
            })),
            ServerMessage::Ok,
        )
    }

    fn execute(&self, db: &mut Db, cc: &mut ClientContext) -> ServerMessage {
        let out = match self.out {
            Some(out) => out,
            None => return ServerMessage::Ok,
        };

        let values1 = match cc.lookup_values_by_name(self.valvec1_name, db) {
            Some(values) => values,
            None => return ServerMessage::ExecutionError(DbError::ValvecNotExist.to_string()),
        };
        let values2 = match cc.lookup_values_by_name(self.valvec2_name, db) {
            Some(values) => values,
            None => return ServerMessage::ExecutionError(DbError::ValvecNotExist.to_string()),
        };

        if values1.len() != values2.len() {
            return ServerMessage::ExecutionError(format!(
                "Value vectors have different lengths: {} and {}",
                values1.len(),
                values2.len()
            ));
        }

        let min_length = values1.len() / rayon::current_num_threads();
        let addsub_values: Vec<i32> = match self.addsub_type {
            AddsubCommandType::Add => (values1, values2)
                .into_par_iter()
                .with_min_len(min_length)
                .map(|(v1, v2)| v1 + v2)
                .collect(),
            AddsubCommandType::Sub => (values1, values2)
                .into_par_iter()
                .with_min_len(min_length)
                .map(|(v1, v2)| v1 - v2)
                .collect(),
        };

        cc.valvecs.insert(
            out.to_string(),
            Valvec {
                values: addsub_values,
            },
        );
        ServerMessage::Ok
    }
}
