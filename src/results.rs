//! Contains the types of results returned by CRUD operations.

use std::{fmt, collections::{HashMap, VecDeque}};

use crate::bson::{Bson, Document};

use serde::{Serialize, Deserialize, de::{self, Visitor, Deserializer}};

use num_traits::identities::One;

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
    #[serde(deserialize_with = "deserialize_ok")]
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

fn deserialize_ok<'de, D>(d: D) -> Result<Result<(), ()>, D::Error> where D: Deserializer<'de> {
    fn ok_from_one(value: impl One + std::cmp::PartialEq) -> Result<(), ()> {
        if value.is_one() {
            Ok(())
        }
        else {
            Err(())
        }
    }

    struct OkVisitor;

    impl<'de> Visitor<'de> for OkVisitor {
        type Value = Result<(), ()>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("0 or 1")
        }

        fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }

        fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }


        fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }


        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ok_from_one(value))
        }
    }

    d.deserialize_i32(OkVisitor)
}