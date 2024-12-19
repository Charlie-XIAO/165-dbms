use std::collections::HashMap;

use super::Command;
use crate::client_context::{ClientContext, Posvec};
use crate::db::Db;
use crate::errors::DbError;
use crate::message::ServerMessage;
use crate::parse::ParserContext;

#[derive(Debug)]
pub enum JoinCommandAlg {
    NestedLoop,
    NaiveHash,
    GraceHash,
    Hash,
}

#[derive(Debug)]
pub struct JoinCommand<'a> {
    outs: Option<(&'a str, &'a str)>,
    valvec1_name: &'a str,
    posvec1_name: &'a str,
    valvec2_name: &'a str,
    posvec2_name: &'a str,
    join_alg: JoinCommandAlg,
}

impl Command for JoinCommand<'_> {
    fn parse(ctx: ParserContext) -> (Option<Box<dyn Command + '_>>, ServerMessage)
    where
        Self: Sized,
    {
        let args = ctx.args;
        if args.len() != 5 {
            return (None, ServerMessage::InvalidCommand);
        }

        (
            Some(Box::new(JoinCommand {
                outs: match ctx.handles_str {
                    Some(handles_str) => {
                        let mut parts = handles_str.split(',');
                        match (parts.next(), parts.next()) {
                            (Some(out1), Some(out2)) => Some((out1.trim(), out2.trim())),
                            _ => None,
                        }
                    },
                    None => None,
                },
                valvec1_name: args[0],
                posvec1_name: args[1],
                valvec2_name: args[2],
                posvec2_name: args[3],
                join_alg: match args[4] {
                    "nested-loop" => JoinCommandAlg::NestedLoop,
                    "naive-hash" => JoinCommandAlg::NaiveHash,
                    "grace-hash" => JoinCommandAlg::GraceHash,
                    "hash" => JoinCommandAlg::Hash,
                    _ => return (None, ServerMessage::InvalidCommand),
                },
            })),
            ServerMessage::Ok,
        )
    }

    fn execute(&self, db: &mut Db, cc: &mut ClientContext) -> ServerMessage {
        let (out1, out2) = match self.outs {
            Some(outs) => outs,
            None => return ServerMessage::Ok,
        };

        let values1 = match cc.lookup_values_by_name(self.valvec1_name, db) {
            Some(values) => values,
            None => return ServerMessage::ExecutionError(DbError::ValvecNotExist.to_string()),
        };
        let positions1 = match cc.posvecs.get(self.posvec1_name) {
            Some(posvec) => &posvec.positions,
            None => return ServerMessage::ExecutionError(DbError::PosvecNotExist.to_string()),
        };

        let values2 = match cc.lookup_values_by_name(self.valvec2_name, db) {
            Some(values) => values,
            None => return ServerMessage::ExecutionError(DbError::ValvecNotExist.to_string()),
        };
        let positions2 = match cc.posvecs.get(self.posvec2_name) {
            Some(posvec) => &posvec.positions,
            None => return ServerMessage::ExecutionError(DbError::PosvecNotExist.to_string()),
        };

        let (joined1, joined2) = match self.join_alg {
            JoinCommandAlg::NestedLoop => {
                self.nested_loop_join(values1, positions1, values2, positions2)
            },
            JoinCommandAlg::NaiveHash => {
                self.naive_hash_join(values1, positions1, values2, positions2)
            },
            JoinCommandAlg::GraceHash => {
                self.grace_hash_join(values1, positions1, values2, positions2)
            },
            JoinCommandAlg::Hash => self.hash_join(values1, positions1, values2, positions2),
        };

        cc.posvecs
            .insert(out1.to_string(), Posvec { positions: joined1 });
        cc.posvecs
            .insert(out2.to_string(), Posvec { positions: joined2 });
        ServerMessage::Ok
    }
}

impl JoinCommand<'_> {
    fn nested_loop_join(
        &self,
        values1: &[i32],
        positions1: &[usize],
        values2: &[i32],
        positions2: &[usize],
    ) -> (Vec<usize>, Vec<usize>) {
        positions1
            .iter()
            .zip(values1.iter())
            .flat_map(|(pos1, &val1)| {
                positions2
                    .iter()
                    .zip(values2.iter())
                    .filter_map(move |(pos2, &val2)| {
                        if val1 == val2 {
                            Some((pos1, pos2))
                        } else {
                            None
                        }
                    })
            })
            .unzip()
    }

    fn naive_hash_join(
        &self,
        values1: &[i32],
        positions1: &[usize],
        values2: &[i32],
        positions2: &[usize],
    ) -> (Vec<usize>, Vec<usize>) {
        let no_swap = values1.len() < values2.len();
        let (build_values, build_positions, probe_values, probe_positions) = if no_swap {
            (values1, positions1, values2, positions2)
        } else {
            (values2, positions2, values1, positions1)
        };

        let mut mapping = HashMap::with_capacity(build_positions.len());
        build_positions
            .iter()
            .zip(build_values.iter())
            .for_each(|(&build_pos, &build_val)| {
                mapping
                    .entry(build_val)
                    .or_insert_with(Vec::new)
                    .push(build_pos);
            });

        let (build_joined, probe_joined) = probe_positions
            .iter()
            .zip(probe_values.iter())
            .flat_map(|(&probe_pos, &probe_val)| {
                mapping
                    .get(&probe_val)
                    .into_iter()
                    .flat_map(move |build_positions| {
                        build_positions
                            .iter()
                            .map(move |&build_pos| (build_pos, probe_pos))
                    })
            })
            .unzip();

        if no_swap {
            (build_joined, probe_joined)
        } else {
            (probe_joined, build_joined)
        }
    }

    fn grace_hash_join(
        &self,
        values1: &[i32],
        positions1: &[usize],
        values2: &[i32],
        positions2: &[usize],
    ) -> (Vec<usize>, Vec<usize>) {
        // TODO
        self.naive_hash_join(values1, positions1, values2, positions2)
    }

    fn hash_join(
        &self,
        values1: &[i32],
        positions1: &[usize],
        values2: &[i32],
        positions2: &[usize],
    ) -> (Vec<usize>, Vec<usize>) {
        // TODO
        self.naive_hash_join(values1, positions1, values2, positions2)
    }
}
