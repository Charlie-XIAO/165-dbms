use rayon::prelude::*;

use super::Command;
use crate::client_context::{ClientContext, Posvec};
use crate::column::{Column, ColumnIndex};
use crate::db::Db;
use crate::errors::DbError;
use crate::message::ServerMessage;
use crate::parse::{parse_column_var, ParserContext};
use crate::threads::is_multi_threaded;

#[derive(Debug)]
pub struct SelectCommand<'a> {
    out: Option<&'a str>,
    lower_bound: i32,
    upper_bound: i32,
    valvec_name: &'a str,
    posvec_name: Option<&'a str>,
}

impl Command for SelectCommand<'_> {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized,
    {
        let args = ctx.args;

        if args.len() == 3 {
            (
                Some(Box::new(SelectCommand {
                    out: ctx.handles_str,
                    posvec_name: None,
                    valvec_name: args[0],
                    lower_bound: match args[1].parse() {
                        Ok(val) => val,
                        Err(_) => {
                            if args[1] == "null" {
                                i32::MIN
                            } else {
                                return (None, ServerMessage::InvalidCommand);
                            }
                        },
                    },
                    upper_bound: match args[2].parse() {
                        Ok(val) => val,
                        Err(_) => {
                            if args[2] == "null" {
                                i32::MAX
                            } else {
                                return (None, ServerMessage::InvalidCommand);
                            }
                        },
                    },
                })),
                ServerMessage::Ok,
            )
        } else if args.len() == 4 {
            (
                Some(Box::new(SelectCommand {
                    out: ctx.handles_str,
                    posvec_name: Some(args[0]),
                    valvec_name: args[1],
                    lower_bound: match args[2].parse() {
                        Ok(val) => val,
                        Err(_) => return (None, ServerMessage::InvalidCommand),
                    },
                    upper_bound: match args[3].parse() {
                        Ok(val) => val,
                        Err(_) => return (None, ServerMessage::InvalidCommand),
                    },
                })),
                ServerMessage::Ok,
            )
        } else {
            (None, ServerMessage::InvalidCommand)
        }
    }

    fn execute(&self, db: &mut Db, cc: &mut ClientContext) -> ServerMessage {
        let out = match self.out {
            Some(out) => out,
            None => return ServerMessage::Ok,
        };

        let positions = match self.posvec_name {
            Some(posvec_name) => match cc.posvecs.get(posvec_name) {
                Some(posvec) => Some(&posvec.positions),
                None => return ServerMessage::ParseError(DbError::PosvecNotExist.to_string()),
            },
            None => None,
        };

        let selected_positions = if let Some(valvec) = cc.valvecs.get(self.valvec_name) {
            self.execute_with_no_index(&valvec.values, positions)
        } else {
            let (_, table_name, column_name) = match parse_column_var(self.valvec_name) {
                Ok(tup) => tup,
                Err(_) => return ServerMessage::ParseError(DbError::ValvecNotExist.to_string()),
            };

            let table = match db.tables.get(table_name) {
                Some(table) => table,
                None => return ServerMessage::ParseError(DbError::ValvecNotExist.to_string()),
            };
            let column = match table.columns.iter().find(|col| col.name == column_name) {
                Some(column) => column,
                None => return ServerMessage::ParseError(DbError::ValvecNotExist.to_string()),
            };

            if matches!(column.index, ColumnIndex::None) {
                self.execute_with_no_index(&column.data, positions)
            } else {
                self.execute_with_index(column, positions)
            }
        };

        cc.posvecs.insert(
            out.to_string(),
            Posvec {
                positions: selected_positions,
            },
        );
        ServerMessage::Ok
    }
}

impl SelectCommand<'_> {
    fn execute_with_no_index(
        &self,
        values: &Vec<i32>,
        positions: Option<&Vec<usize>>,
    ) -> Vec<usize> {
        if is_multi_threaded() {
            let min_length = values.len() / rayon::current_num_threads();
            if let Some(positions) = positions {
                values
                    .par_iter()
                    .with_min_len(min_length)
                    .enumerate()
                    .filter_map(|(idx, &val)| {
                        if val >= self.lower_bound && val < self.upper_bound {
                            Some(unsafe { *positions.get_unchecked(idx) })
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                values
                    .par_iter()
                    .with_min_len(min_length)
                    .enumerate()
                    .filter_map(|(idx, &val)| {
                        if val >= self.lower_bound && val < self.upper_bound {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        } else if let Some(positions) = positions {
            values
                .iter()
                .enumerate()
                .filter_map(|(idx, &val)| {
                    if val >= self.lower_bound && val < self.upper_bound {
                        Some(unsafe { *positions.get_unchecked(idx) })
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            values
                .iter()
                .enumerate()
                .filter_map(|(idx, &val)| {
                    if val >= self.lower_bound && val < self.upper_bound {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .collect()
        }
    }

    fn execute_with_index(&self, column: &Column, positions: Option<&Vec<usize>>) -> Vec<usize> {
        match &column.index {
            ColumnIndex::None => unreachable!(),
            ColumnIndex::UnclusteredSorted { sorter } => {
                let lower_ind = column.binsearch(self.lower_bound, Some(sorter), true);
                let upper_ind = column.binsearch(self.upper_bound, Some(sorter), true);
                if let Some(positions) = positions {
                    (lower_ind..upper_ind)
                        .map(|ind| unsafe { *positions.get_unchecked(*sorter.get_unchecked(ind)) })
                        .collect()
                } else {
                    sorter[lower_ind..upper_ind].to_vec()
                }
            },
            ColumnIndex::ClusteredSorted => {
                let lower_ind = column.binsearch(self.lower_bound, None, true);
                let upper_ind = column.binsearch(self.upper_bound, None, true);
                if let Some(positions) = positions {
                    (lower_ind..upper_ind)
                        .map(|ind| unsafe { *positions.get_unchecked(ind) })
                        .collect()
                } else {
                    (lower_ind..upper_ind).collect()
                }
            },
            ColumnIndex::UnclusteredBTree { btree } | ColumnIndex::ClusteredBTree { btree } => {
                let range = btree.range(self.lower_bound..self.upper_bound);
                if let Some(positions) = positions {
                    range
                        .flat_map(|(_, indices)| {
                            indices
                                .iter()
                                .map(|&ind| unsafe { *positions.get_unchecked(ind) })
                        })
                        .collect()
                } else {
                    range
                        .flat_map(|(_, indices)| indices.iter().copied())
                        .collect()
                }
            },
        }
    }
}
