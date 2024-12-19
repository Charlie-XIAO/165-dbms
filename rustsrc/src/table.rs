use std::ops::Bound;

use itertools::Itertools;
use ndarray::Array1;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::column::{Column, ColumnIndex};
use crate::command::create::CreateCommandIndexType;
use crate::consts::NUM_ELEMS_PER_LOAD_BATCH;
use crate::errors::DbError;

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub columns: Vec<Column>,
    pub primary: Option<usize>,
    pub n_rows: usize,
    pub n_cols: usize,
}

impl Table {
    pub fn new(n_cols: usize) -> Self {
        Table {
            columns: Vec::with_capacity(n_cols),
            primary: None,
            n_rows: 0,
            n_cols,
        }
    }

    fn set_primary_column(&mut self, ith_column: usize, skip_sorting: bool) {
        let column = &mut self.columns[ith_column];
        self.primary = Some(ith_column);
        if skip_sorting {
            return;
        }

        let sorter = column.argsort();
        for col in &mut self.columns {
            let data = &col.data;
            col.data = sorter
                .par_iter()
                .map(|&i| unsafe { *data.get_unchecked(i) })
                .collect();
        }
        self.recreate_unclustered_indexes();
    }

    fn recreate_unclustered_indexes(&mut self) {
        for ith_column in 0..self.n_cols {
            match self.columns[ith_column].index {
                ColumnIndex::UnclusteredSorted { .. } => {
                    self.columns[ith_column].set_index_unclustered_sorted();
                },
                ColumnIndex::UnclusteredBTree { .. } => {
                    self.columns[ith_column].set_index_unclustered_btree();
                },
                _ => {},
            }
        }
    }

    pub fn recreate_indexes(&mut self, skip_sorting: bool) {
        if let Some(primary) = self.primary {
            match self.columns[primary].index {
                ColumnIndex::ClusteredSorted => {
                    self.set_primary_column(primary, skip_sorting);
                    self.columns[primary].set_index_clustered_sorted();
                },
                ColumnIndex::ClusteredBTree { .. } => {
                    self.set_primary_column(primary, skip_sorting);
                    self.columns[primary].set_index_clustered_btree();
                },
                _ => unreachable!(),
            }
        }
        self.recreate_unclustered_indexes();
    }

    pub fn create_index(&mut self, ith_column: usize, index_type: CreateCommandIndexType) {
        match index_type {
            CreateCommandIndexType::None => {},
            CreateCommandIndexType::UnclusteredSorted => {
                self.columns[ith_column].set_index_unclustered_sorted();
            },
            CreateCommandIndexType::UnclusteredBTree => {
                self.columns[ith_column].set_index_unclustered_btree();
            },
            CreateCommandIndexType::ClusteredSorted => {
                self.set_primary_column(ith_column, false);
                self.columns[ith_column].set_index_clustered_sorted();
            },
            CreateCommandIndexType::ClusteredBTree => {
                self.set_primary_column(ith_column, false);
                self.columns[ith_column].set_index_clustered_btree();
            },
        }
    }

    pub fn load_csv<R>(&mut self, reader: &mut csv::Reader<R>) -> Result<(), DbError>
    where
        R: std::io::Read,
    {
        let chunk_size = (NUM_ELEMS_PER_LOAD_BATCH / self.n_cols) * self.n_cols;
        for chunk in &reader
            .deserialize::<Vec<i32>>()
            .filter_map(Result::ok)
            .flatten()
            .chunks(chunk_size)
        {
            let chunk_flattened: Array1<i32> = chunk.collect();
            let n_rows = chunk_flattened.len() / self.n_cols;
            let chunk_view = chunk_flattened
                .to_shape((n_rows, self.n_cols))
                .map_err(|e| DbError::Internal(e.to_string()))?;
            for (col_idx, column) in chunk_view.columns().into_iter().enumerate() {
                self.columns[col_idx].data.reserve(n_rows);
                self.columns[col_idx].data.extend(column);
            }
            self.n_rows += n_rows;
        }

        self.recreate_indexes(false);
        Ok(())
    }

    pub fn insert_row(&mut self, values: &[i32]) -> Result<(), DbError> {
        if values.len() != self.n_cols {
            return Err(DbError::Internal(format!(
                "{} columns, but {} values given",
                self.n_cols,
                values.len()
            )));
        }

        if let Some(primary) = self.primary {
            let column = &mut self.columns[primary];
            let value = values[primary];

            let insert_pos = match &column.index {
                ColumnIndex::ClusteredSorted => column.binsearch(value, None, true),
                ColumnIndex::ClusteredBTree { btree } => {
                    let cursor = btree.upper_bound(Bound::Included(&value));
                    let insert_pos = if let Some((_, indices)) = cursor.peek_prev() {
                        *indices.last().ok_or(DbError::Internal(
                            "Broken clustered B+ tree index".to_string(),
                        ))?
                    } else {
                        0
                    };
                    if let ColumnIndex::ClusteredBTree { btree } = &mut column.index {
                        btree
                            .entry(value)
                            .or_insert_with(Vec::new)
                            .push(self.n_rows);
                    }
                    insert_pos
                },
                _ => unreachable!(),
            };

            for column in &mut self.columns {
                column.data.insert(insert_pos, value);
            }
            self.n_rows += 1;
            self.recreate_unclustered_indexes();
            return Ok(());
        }

        for (ith_column, value) in values.iter().enumerate() {
            let column = &mut self.columns[ith_column];
            column.data.push(*value);

            match &column.index {
                ColumnIndex::UnclusteredSorted { sorter } => {
                    let insert_pos = column.binsearch(*value, Some(sorter), false);
                    if let ColumnIndex::UnclusteredSorted { sorter } = &mut column.index {
                        sorter.insert(insert_pos, self.n_rows);
                    }
                },
                ColumnIndex::UnclusteredBTree { .. } => {
                    if let ColumnIndex::UnclusteredBTree { btree } = &mut column.index {
                        btree
                            .entry(*value)
                            .or_insert_with(Vec::new)
                            .push(self.n_rows);
                    }
                },
                _ => {},
            }
        }

        self.n_rows += 1;
        Ok(())
    }
}
