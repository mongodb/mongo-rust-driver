//! Contains the types of results returned by CRUD operations.

use std::collections::{HashMap, VecDeque};

use crate::{bson::{Bson, Document}, db::options::CreateCollectionOptions};

use bson::Binary;
use serde::Serialize;
use serde::Deserialize;

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

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "lowercase", untagged)]
#[non_exhaustive]
/// Describes the type of data store returned when executing [`crate::Database::list_collections`].
pub enum CollectionType {
    /// Indicates that the data store is a view.
    View,

    /// Indicates that the data store is a collection.
    Collection,

    /// An unknown collection type. This is included for forwards compatibility.
    Other(String),
}

impl<'de> Deserialize<'de> for CollectionType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        let out = match s.as_str() {
            "collection" => CollectionType::Collection,
            "view" => CollectionType::View,
            _ => CollectionType::Other(s)
        };
        Ok(out)
    }
}

/// Info about the collection that is contained in the `CollectionSpecification::info` field of a specification returned from
/// [`crate::Database::list_collections`].
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
/// Information about a collection as reported by [`crate::Database::list_collections`].
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
    pub id_index: Document,
}
