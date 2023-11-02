#![allow(missing_docs, unused_variables, dead_code)]

use crate::{
    bson::Document,
    error::{Error, WriteError},
    options::UpdateModifications,
    results::{DeleteResult, InsertOneResult, UpdateResult},
    Client,
    Namespace,
};
use bson::{Array, Bson};
use std::collections::HashMap;

impl Client {
    pub async fn bulk_write(
        models: impl IntoIterator<Item = BulkWriteModel>,
        options: impl Into<Option<BulkWriteOptions>>,
    ) -> Result<BulkWriteResult, BulkWriteError> {
        todo!()
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum BulkWriteModel {
    InsertOne {
        namespace: Namespace,
        document: Document,
    },
    DeleteOne {
        namespace: Namespace,
        filter: Document,
        collation: Option<Document>,
        hint: Option<Bson>,
    },
    DeleteMany {
        namespace: Namespace,
        filter: Document,
        collation: Option<Document>,
        hint: Option<Bson>,
    },
    ReplaceOne {
        namespace: Namespace,
        filter: Document,
        replacement: Document,
        collation: Option<Document>,
        hint: Option<Bson>,
        upsert: Option<bool>,
        let_vars: Option<Document>,
    },
    UpdateOne {
        namespace: Namespace,
        filter: Document,
        update: UpdateModifications,
        array_filters: Option<Array>,
        collation: Option<Document>,
        hint: Option<Bson>,
        upsert: Option<bool>,
        let_vars: Option<Document>,
    },
    UpdateMany {
        namespace: Namespace,
        filter: Document,
        update: UpdateModifications,
        array_filters: Option<Array>,
        collation: Option<Document>,
        hint: Option<Bson>,
        upsert: Option<bool>,
    },
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BulkWriteOptions {
    pub ordered: Option<bool>,
    pub bypass_document_validation: Option<bool>,
    pub comment: Option<Bson>,
    pub let_vars: Option<Document>,
    pub verbose_results: Option<bool>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BulkWriteResult {
    pub inserted_count: i32,
    pub upserted_count: i32,
    pub matched_count: i32,
    pub modified_count: i32,
    pub deleted_count: i32,
    pub insert_results: HashMap<usize, InsertOneResult>,
    pub update_results: HashMap<usize, UpdateResult>,
    pub delete_results: HashMap<usize, DeleteResult>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BulkWriteError {
    error: Option<Error>,
    write_errors: Vec<BulkWriteOperationError>,
    write_result: Option<BulkWriteResult>,
    processed_requests: Vec<BulkWriteModel>,
    unprocessed_requests: Vec<BulkWriteModel>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BulkWriteOperationError {
    index: i32,
    request: BulkWriteModel,
    error: WriteError,
}
