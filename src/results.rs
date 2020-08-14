//! Contains the types of results returned by CRUD operations.

use std::collections::{HashMap, VecDeque};

use crate::bson::{Bson, Document};

use serde::{Serialize, Deserialize};

/// The result of a [`Collection::insert_one`](../struct.Collection.html#method.insert_one)
/// operation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct DeleteResult {
    /// The number of documents deleted by the operation.
    pub deleted_count: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct GetMoreResult {
    pub(crate) batch: VecDeque<Document>,
    pub(crate) exhausted: bool,
}

/// The result of a [`Collection::create_indexes`](../struct.Collection.html#method.create_indexes)
/// operation.
/// https://docs.mongodb.com/manual/reference/command/createIndexes/#output
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CreateIndexesResult {
    /// If true, then the collection didn’t exist and was created in the process of creating the index.
    pub created_collection_automatically: Option<bool>,
    /// The number of indexes at the start of the command.
    pub num_indexes_before: Option<u32>,
    /// The number of indexes at the end of the command.
    pub num_indexes_after: Option<u32>,
    /// A value of 1 indicates the indexes are in place. A value of 0 indicates an error.
    pub ok: Result<(), ()>,
    /// This note is returned if an existing index or indexes already exist. This indicates that the index was not created or changed.
    pub note: Option<String>,
    /// Returns information about any errors.
    pub errmsg: Option<String>,
    /// The error code representing the type of error.
    pub code: Option<u32>,
    // look like this exist but not documented
    pub code_name: Option<String>,
}