pub mod options;

use crate::bson::Document;

use serde::{Deserialize, Serialize};
use self::options::*;

use typed_builder::TypedBuilder;

/// Specifies the options to a [`Collection::create_index`](../struct.Collection.html#method.create_index)
/// operation.
/// See the
/// [documentation](https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/#options-for-all-index-types)
/// for more information on how to use this option.
#[derive(Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct IndexModel {
    /// Specifies the index’s fields. For each field, specify a key-value pair in which the key is the name of the field to
    /// index and the value is index type.
    #[serde(rename = "key")]
    pub keys: Document,

    options: Option<IndexOptions>,
}