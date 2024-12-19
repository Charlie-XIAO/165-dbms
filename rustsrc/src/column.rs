use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

use bytemuck::cast_slice;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

pub enum ColumnIndex {
    None,
    UnclusteredSorted { sorter: Vec<usize> },
    UnclusteredBTree { btree: BTreeMap<i32, Vec<usize>> },
    ClusteredSorted,
    ClusteredBTree { btree: BTreeMap<i32, Vec<usize>> },
}

impl Serialize for ColumnIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            ColumnIndex::None => 0,
            ColumnIndex::UnclusteredSorted { .. } => 1,
            ColumnIndex::UnclusteredBTree { .. } => 2,
            ColumnIndex::ClusteredSorted => 3,
            ColumnIndex::ClusteredBTree { .. } => 4,
        };
        serializer.serialize_u8(variant)
    }
}

impl<'de> Deserialize<'de> for ColumnIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let variant: u8 = Deserialize::deserialize(deserializer)?;
        match variant {
            0 => Ok(ColumnIndex::None),
            1 => Ok(ColumnIndex::UnclusteredSorted {
                sorter: Default::default(),
            }),
            2 => Ok(ColumnIndex::UnclusteredBTree {
                btree: Default::default(),
            }),
            3 => Ok(ColumnIndex::ClusteredSorted),
            4 => Ok(ColumnIndex::ClusteredBTree {
                btree: Default::default(),
            }),
            _ => Err(serde::de::Error::unknown_variant(
                &format!("{variant}"),
                &["0", "1", "2", "3", "4"],
            )),
        }
    }
}

impl Debug for ColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnIndex::None => write!(f, "None"),
            ColumnIndex::UnclusteredSorted { sorter } => {
                write!(f, "UnclusteredSorted(len={})", sorter.len())
            },
            ColumnIndex::UnclusteredBTree { btree } => {
                write!(f, "UnclusteredBTree(n_keys={})", btree.len())
            },
            ColumnIndex::ClusteredSorted => write!(f, "ClusteredSorted"),
            ColumnIndex::ClusteredBTree { btree } => {
                write!(f, "ClusteredBTree(n_keys={})", btree.len())
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub index: ColumnIndex,
    #[serde(
        serialize_with = "serialize_column_data",
        deserialize_with = "deserialize_column_data"
    )]
    pub data: Vec<i32>,
}

fn serialize_column_data<S>(data: &[i32], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let bytes: &[u8] = cast_slice(data);
    serde_bytes::serialize(bytes, serializer)
}

fn deserialize_column_data<'de, D>(deserializer: D) -> Result<Vec<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
    Ok(cast_slice(&bytes).to_vec())
}

impl Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Column")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("data", &format!("i32[{}]", self.data.len()))
            .finish()
    }
}

impl Column {
    pub fn new(name: String) -> Self {
        Column {
            name,
            data: Vec::new(),
            index: ColumnIndex::None,
        }
    }

    pub fn argsort(&self) -> Vec<usize> {
        if self.data.is_empty() {
            return Default::default();
        }

        let data = &self.data;
        let mut indices: Vec<usize> = (0..self.data.len()).collect();
        indices.par_sort_unstable_by_key(|&i| unsafe { *data.get_unchecked(i) });
        indices
    }

    pub fn binsearch(&self, key: i32, sorter: Option<&[usize]>, align_left: bool) -> usize {
        if key == i32::MAX {
            return self.data.len();
        } else if key == i32::MIN {
            return 0;
        }

        let mut low = 0;
        let mut high = self.data.len();
        let data = &self.data;

        if let Some(sorter) = sorter {
            if align_left {
                while low < high {
                    let mid = (low + high) / 2;
                    if unsafe { *data.get_unchecked(*sorter.get_unchecked(mid)) } < key {
                        low = mid + 1;
                    } else {
                        high = mid;
                    }
                }
            } else {
                while low < high {
                    let mid = (low + high) / 2;
                    if unsafe { *data.get_unchecked(*sorter.get_unchecked(mid)) } <= key {
                        low = mid + 1;
                    } else {
                        high = mid;
                    }
                }
            }
        } else if align_left {
            while low < high {
                let mid = (low + high) / 2;
                if unsafe { *data.get_unchecked(mid) } < key {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
        } else {
            while low < high {
                let mid = (low + high) / 2;
                if unsafe { *data.get_unchecked(mid) } <= key {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
        }

        low
    }

    pub fn bulk_load_btree(&self) -> BTreeMap<i32, Vec<usize>> {
        if self.data.is_empty() {
            return Default::default();
        }

        let mut mapping = HashMap::new();
        self.data.iter().enumerate().for_each(|(idx, &value)| {
            mapping.entry(value).or_insert_with(Vec::new).push(idx);
        });
        BTreeMap::from_iter(mapping)
    }

    pub fn set_index_unclustered_sorted(&mut self) {
        self.index = ColumnIndex::UnclusteredSorted {
            sorter: self.argsort(),
        };
    }

    pub fn set_index_unclustered_btree(&mut self) {
        self.index = ColumnIndex::UnclusteredBTree {
            btree: self.bulk_load_btree(),
        };
    }

    pub fn set_index_clustered_sorted(&mut self) {
        self.index = ColumnIndex::ClusteredSorted;
    }

    pub fn set_index_clustered_btree(&mut self) {
        self.index = ColumnIndex::ClusteredBTree {
            btree: self.bulk_load_btree(),
        };
    }
}
