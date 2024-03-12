#![allow(missing_docs)]

use std::{collections::HashMap, fmt::Debug};

use serde::Serialize;
use serde_with::skip_serializing_none;

use crate::{
    results::{DeleteResult, InsertOneResult, UpdateResult},
    serde_util::serialize_indexed_map,
};

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BulkWriteResult {
    pub inserted_count: i64,
    pub upserted_count: i64,
    pub matched_count: i64,
    pub modified_count: i64,
    pub deleted_count: i64,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub insert_results: Option<HashMap<usize, InsertOneResult>>,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub update_results: Option<HashMap<usize, UpdateResult>>,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub delete_results: Option<HashMap<usize, DeleteResult>>,
}

impl BulkWriteResult {
    pub(crate) fn new(verbose: bool) -> Self {
        Self {
            inserted_count: 0,
            upserted_count: 0,
            matched_count: 0,
            modified_count: 0,
            deleted_count: 0,
            insert_results: verbose.then(HashMap::new),
            update_results: verbose.then(HashMap::new),
            delete_results: verbose.then(HashMap::new),
        }
    }

    pub(crate) fn add_insert_result(&mut self, index: usize, insert_result: InsertOneResult) {
        self.insert_results
            .get_or_insert_with(Default::default)
            .insert(index, insert_result);
    }

    pub(crate) fn add_update_result(&mut self, index: usize, update_result: UpdateResult) {
        self.update_results
            .get_or_insert_with(Default::default)
            .insert(index, update_result);
    }

    pub(crate) fn add_delete_result(&mut self, index: usize, delete_result: DeleteResult) {
        self.delete_results
            .get_or_insert_with(Default::default)
            .insert(index, delete_result);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.inserted_count += other.inserted_count;
        self.upserted_count += other.upserted_count;
        self.matched_count += other.matched_count;
        self.modified_count += other.modified_count;
        self.deleted_count += other.deleted_count;
        if let Some(insert_results) = other.insert_results {
            self.insert_results
                .get_or_insert_with(Default::default)
                .extend(insert_results);
        }
        if let Some(update_results) = other.update_results {
            self.update_results
                .get_or_insert_with(Default::default)
                .extend(update_results);
        }
        if let Some(delete_results) = other.delete_results {
            self.delete_results
                .get_or_insert_with(Default::default)
                .extend(delete_results);
        }
    }
}
