//! Contains the types of results returned by CRUD operations.

use std::collections::{HashMap, VecDeque};

use crate::{
    bson::{serde_helpers, Bson, Document},
    change_stream::event::ResumeToken,
    db::options::CreateCollectionOptions,
    serde_util,
    Namespace,
};

use bson::{Binary, RawDocumentBuf};
use serde::{Deserialize, Serialize};

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
            inserted_id: result.inserted_ids.get(&0).cloned().unwrap_or(Bson::Null),
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
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u64_as_i64")]
    pub matched_count: u64,

    /// The number of documents that were modified by the operation.
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u64_as_i64")]
    pub modified_count: u64,

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
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u64_as_i64")]
    pub deleted_count: u64,
}

/// Information about the index created as a result of a
/// [`Collection::create_index`](../struct.Collection.html#method.create_index).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CreateIndexResult {
    /// The name of the index created in the `createIndex` command.
    pub index_name: String,
}

/// Information about the indexes created as a result of a
/// [`Collection::create_indexes`](../struct.Collection.html#method.create_indexes).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CreateIndexesResult {
    /// The list containing the names of all indexes created in the `createIndexes` command.
    pub index_names: Vec<String>,
}

impl CreateIndexesResult {
    pub(crate) fn into_create_index_result(self) -> CreateIndexResult {
        CreateIndexResult {
            index_name: self.index_names.into_iter().next().unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GetMoreResult {
    pub(crate) batch: VecDeque<RawDocumentBuf>,
    pub(crate) exhausted: bool,
    pub(crate) post_batch_resume_token: Option<ResumeToken>,
    pub(crate) ns: Namespace,
    pub(crate) id: i64,
}

/// Describes the type of data store returned when executing
/// [`Database::list_collections`](../struct.Database.html#method.list_collections).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum CollectionType {
    /// Indicates that the data store is a view.
    View,

    /// Indicates that the data store is a collection.
    Collection,

    /// Indicates that the data store is a timeseries.
    Timeseries,
}

/// Info about the collection that is contained in the `CollectionSpecification::info` field of a
/// specification returned from
/// [`Database::list_collections`](../struct.Database.html#method.list_collections).
///
/// See the MongoDB [manual](https://www.mongodb.com/docs/manual/reference/command/listCollections/#listCollections.cursor)
/// for more information.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CollectionSpecificationInfo {
    /// Indicates whether or not the data store is read-only.
    pub read_only: bool,

    /// The collection's UUID - once established, this does not change and remains the same across
    /// replica set members and shards in a sharded cluster. If the data store is a view, this
    /// field is `None`.
    pub uuid: Option<Binary>,
}

/// Information about a collection as reported by
/// [`Database::list_collections`](../struct.Database.html#method.list_collections).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CollectionSpecification {
    /// The name of the collection.
    pub name: String,

    /// Type of the data store.
    #[serde(rename = "type")]
    pub collection_type: CollectionType,

    /// The options used to create the collection.
    pub options: CreateCollectionOptions,

    /// Additional info pertaining to the collection.
    pub info: CollectionSpecificationInfo,

    /// Provides information on the _id index for the collection
    /// For views, this is `None`.
    pub id_index: Option<Document>,
}

/// A struct modeling the information about an individual database returned from
/// [`Client::list_databases`](../struct.Client.html#method.list_databases).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DatabaseSpecification {
    /// The name of the database.
    pub name: String,

    /// The amount of disk space in bytes that is consumed by the database.
    #[serde(
        deserialize_with = "serde_util::deserialize_u64_from_bson_number",
        serialize_with = "serde_helpers::serialize_u64_as_i64"
    )]
    pub size_on_disk: u64,

    /// Whether the database has any data.
    pub empty: bool,

    /// For sharded clusters, this field includes a document which maps each shard to the size in
    /// bytes of the database on disk on that shard. For non sharded environments, this field
    /// is `None`.
    pub shards: Option<Document>,
}
