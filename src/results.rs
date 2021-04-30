//! Contains the types of results returned by CRUD operations.

use std::collections::{HashMap, VecDeque};

use crate::{bson::{Bson, Document}, db::options::CreateCollectionOptions};

use bson::Binary;
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
}

/// Info about the collection that is contained in the `CollectionSpecification::info` field of a specification returned from
/// [`Database::list_collections`](../struct.Database.html#method.list_collections).
///
/// See the MongoDB [manual](https://docs.mongodb.com/manual/reference/command/listCollections/#listCollections.cursor)
/// for more information.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CollectionSpecificationInfo {
    /// Indicates whether or not the data store is read-only.
    pub read_only: bool,

    /// The collection's UUID - once established, this does not change and remains the same across replica
    /// set members and shards in a sharded cluster. If the data store is a view, this field is `None`.
    pub uuid: Option<Binary>,
}

/// Information about a collection as reported by [`Database::list_collections`](../struct.Database.html#method.list_collections).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CollectionSpecification {
    /// The name of the collection.
    pub name: String,

    /// Type of the data store.
    #[serde(rename="type")]
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
    #[serde(deserialize_with = "crate::bson_util::deserialize_u64_from_bson_number")]
    pub size_on_disk: u64,

    /// Whether the database has any data.
    pub empty: bool,

    /// For sharded clusters, this field includes a document which maps each shard to the size in bytes of the database
    /// on disk on that shard. For non sharded environments, this field is `None`.
    pub shards: Option<Document>,
}
