//! Contains the types of results returned by CRUD operations.

use std::collections::{HashMap, VecDeque};

use crate::bson::{Bson, Document};

use serde::Serialize;

/// The result of a [`Collection::insert_one`](../struct.Collection.html#method.insert_one)
/// operation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertOneResult {
    /// The `_id` field of the document inserted.
    pub inserted_id: Bson,
}

impl InsertOneResult {
    pub(crate) fn from_insert_many_result(result: InsertManyResult) -> Self {
        Self {
            inserted_id: result
                .inserted_ids
                .get(&0)
                .cloned()
                .unwrap_or_else(|| Bson::Null),
        }
    }
}

/// The result of a [`Collection::insert_many`](../struct.Collection.html#method.insert_many)
/// operation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertManyResult {
    /// The `_id` field of the documents inserted.
    pub inserted_ids: HashMap<usize, Bson>,
}

impl InsertManyResult {
    pub(crate) fn new() -> Self {
        InsertManyResult {
            inserted_ids: HashMap::new(),
        }
    }
}

/// The result of a [`Collection::update_one`](../struct.Collection.html#method.update_one) or
/// [`Collection::update_many`](../struct.Collection.html#method.update_many) operation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateResult {
    /// The number of documents that matched the filter.
    pub matched_count: i64,
    /// The number of documents that were modified by the operation.
    pub modified_count: i64,
    /// The `_id` field of the upserted document.
    pub upserted_id: Option<Bson>,
}

/// The result of a [`Collection::delete_one`](../struct.Collection.html#method.delete_one) or
/// [`Collection::delete_many`](../struct.Collection.html#method.delete_many) operation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteResult {
    /// The number of documents deleted by the operation.
    pub deleted_count: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct GetMoreResult {
    pub(crate) batch: VecDeque<Document>,
    pub(crate) exhausted: bool,
}
