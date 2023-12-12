#![allow(missing_docs)]

use std::{collections::HashMap, fmt::Debug};

use serde::Serialize;

use crate::{
    results::{DeleteResult, InsertOneResult, UpdateResult},
    serde_util::serialize_indexed_map,
};

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BulkWriteResult {
    pub inserted_count: i64,
    pub upserted_count: i64,
    pub matched_count: i64,
    pub modified_count: i64,
    pub deleted_count: i64,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub insert_results: HashMap<usize, InsertOneResult>,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub update_results: HashMap<usize, UpdateResult>,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub delete_results: HashMap<usize, DeleteResult>,
}

impl BulkWriteResult {
    pub(crate) fn add_insert_result(&mut self, index: usize, insert_result: InsertOneResult) {
        self.insert_results.insert(index, insert_result);
    }

    pub(crate) fn add_update_result(&mut self, index: usize, update_result: UpdateResult) {
        self.update_results.insert(index, update_result);
    }

    pub(crate) fn add_delete_result(&mut self, index: usize, delete_result: DeleteResult) {
        self.delete_results.insert(index, delete_result);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.inserted_count += other.inserted_count;
        self.upserted_count += other.upserted_count;
        self.matched_count += other.matched_count;
        self.modified_count += other.modified_count;
        self.deleted_count += other.deleted_count;
        self.insert_results.extend(other.insert_results);
        self.update_results.extend(other.update_results);
        self.delete_results.extend(other.delete_results);
    }
}
