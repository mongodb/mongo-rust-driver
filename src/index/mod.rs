pub mod options;

use crate::bson::Document;

use self::options::*;
use serde::{Deserialize, Serialize};

use typed_builder::TypedBuilder;

/// Specifies the options to a
/// [`Collection::create_index`](../struct.Collection.html#method.create_index) operation.
/// See the
/// [documentation](https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/#options-for-all-index-types)
/// for more information on how to use this option.
#[derive(Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct IndexModel {
    /// Specifies the index’s fields. For each field, specify a key-value pair in which the key is
    /// the name of the field to index and the value is index type.
    #[serde(rename = "key")]
    keys: Document,

    #[serde(flatten)]
    options: Option<IndexOptions>,
}

impl IndexModel {
    /// If the client did not specify a name, generate and set it. Otherwise, do nothing.
    pub(crate) fn update_name(&mut self) {
        if self
            .options
            .as_ref()
            .and_then(|o| o.name.as_ref())
            .is_none()
        {
            let key_names: Vec<String> = self
                .keys
                .iter()
                .map(|(k, v)| format!("{}_{}", k, v.to_string()))
                .collect();
            self.options.get_or_insert(IndexOptions::default()).name = Some(key_names.join("_"));
        }
    }

    pub(crate) fn get_name(&self) -> Option<String> {
        self.options.as_ref().and_then(|o| o.name.as_ref()).cloned()
    }
}
