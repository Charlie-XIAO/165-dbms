use crate::command::addsub::{AddsubCommand, AddsubCommandType};
use crate::command::agg::{AggCommand, AggCommandType};
use crate::command::create::CreateCommand;
use crate::command::fetch::FetchCommand;
use crate::command::insert::InsertCommand;
use crate::command::join::JoinCommand;
use crate::command::print::PrintCommand;
use crate::command::select::SelectCommand;
use crate::command::Command;
use crate::errors::DbError;
use crate::message::ServerMessage;

pub struct ParserContext<'a> {
    pub args: Vec<&'a str>,
    pub handles_str: Option<&'a str>,
    pub flag: u8,
}

pub fn parse(query: &str) -> (Option<Box<dyn Command + '_>>, ServerMessage) {
    let query = query.trim();
    if query.starts_with("--") {
        return (None, ServerMessage::Ok);
    }

    let (handles_str, args_str) = match query.split_once('=') {
        Some((handles_str, args_str)) => (Some(handles_str), args_str),
        None => (None, query),
    };

    let args_str = args_str.trim().trim_end_matches(')');
    let mut parts = args_str.splitn(2, '(');

    let command = match parts.next() {
        Some(command) => command,
        None => return (None, ServerMessage::InvalidCommand),
    };
    let args: Vec<&str> = match parts.next() {
        Some(args) => args.split(',').map(str::trim).collect(),
        None => return (None, ServerMessage::InvalidCommand),
    };

    let mut ctx = ParserContext {
        args,
        handles_str,
        flag: Default::default(),
    };
    match command {
        "add" => {
            ctx.flag = AddsubCommandType::Add.to_flag();
            AddsubCommand::parse(ctx)
        },
        "sub" => {
            ctx.flag = AddsubCommandType::Sub.to_flag();
            AddsubCommand::parse(ctx)
        },
        "min" => {
            ctx.flag = AggCommandType::Min.to_flag();
            AggCommand::parse(ctx)
        },
        "max" => {
            ctx.flag = AggCommandType::Max.to_flag();
            AggCommand::parse(ctx)
        },
        "sum" => {
            ctx.flag = AggCommandType::Sum.to_flag();
            AggCommand::parse(ctx)
        },
        "avg" => {
            ctx.flag = AggCommandType::Avg.to_flag();
            AggCommand::parse(ctx)
        },
        "create" => CreateCommand::parse(ctx),
        "relational_delete" => unimplemented!(), // TODO
        "fetch" => FetchCommand::parse(ctx),
        "relational_insert" => InsertCommand::parse(ctx),
        "join" => JoinCommand::parse(ctx),
        "print" => PrintCommand::parse(ctx),
        "select" => SelectCommand::parse(ctx),
        "relational_update" => unimplemented!(), // TODO
        // TODO: batch command
        _ => (None, ServerMessage::InvalidCommand),
    }
}

pub fn trim_quotes(s: &str) -> (&str, ServerMessage) {
    if s.starts_with('"') && s.ends_with('"') {
        (&s[1..s.len() - 1], ServerMessage::Ok)
    } else {
        ("", ServerMessage::InvalidCommand)
    }
}

pub fn parse_table_var(table_var: &str) -> Result<(&str, &str), DbError> {
    let mut parts = table_var.splitn(2, '.');
    let db_name = parts.next().ok_or(DbError::VarNoDb)?;
    let table_name = parts.next().ok_or(DbError::VarNoTable)?;
    Ok((db_name, table_name))
}

pub fn parse_column_var(column_var: &str) -> Result<(&str, &str, &str), DbError> {
    let mut parts = column_var.splitn(3, '.');
    let db_name = parts.next().ok_or(DbError::VarNoDb)?;
    let table_name = parts.next().ok_or(DbError::VarNoTable)?;
    let column_name = parts.next().ok_or(DbError::VarNoColumn)?;
    Ok((db_name, table_name, column_name))
}
