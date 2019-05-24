//! Contains the types of results returned by CRUD operations.

use std::collections::HashMap;

use bson::Bson;

/// The result of a `Collection::insert_one` operation.
#[derive(Debug)]
pub struct InsertOneResult {
    /// The `_id` field of the document inserted.
    pub inserted_id: Bson,
}

/// The result of a `Collection::insert_many` operation.
#[derive(Debug)]
pub struct InsertManyResult {
    /// The `_id` field of the documents inserted.
    pub inserted_ids: HashMap<usize, Bson>,
}

/// The result of a `Collection::update_one` or `Collection::update_many` operation.
#[derive(Debug)]
pub struct UpdateResult {
    /// The number of documents that matched the filter.
    pub matched_count: i64,
    /// The number of documents that were modified by the operation.
    pub modified_count: i64,
    /// The `_id` field of the upserted document.
    pub upserted_id: Option<Bson>,
}

/// The result of a `Collection::delete_one` or `Collection::delete_many` operation.
#[derive(Debug)]
pub struct DeleteResult {
    /// The number of documents deleted by the operation.
    pub deleted_count: i64,
}
