use std::fmt::Debug;

use crate::client_context::ClientContext;
use crate::db::Db;
use crate::message::ServerMessage;
use crate::parse::ParserContext;

pub mod addsub;
pub mod agg;
pub mod create;
pub mod fetch;
pub mod insert;
pub mod join;
pub mod print;
pub mod select;

pub trait Command: Debug {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized;
    fn execute(&self, db: &mut Db, cc: &mut ClientContext) -> ServerMessage;
}
