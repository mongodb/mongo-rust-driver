use std::time::Duration;

use crate::{
    bson::Document,
    bson_util,
    collation::Collation,
};

<<<<<<< HEAD
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
=======
use serde::{Deserialize, Deserializer, Serialize, Serializer};
>>>>>>> a521ae6 (fix)
use typed_builder::TypedBuilder;

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexOptions {
    /// Optional. Tells the server to build the index in the background and not block other tasks.
    ///
    /// Starting in MongoDB 4.2, this option is ignored by the server.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/command/createIndexes/) for more information on how to use this option.
    #[builder(default)]
    pub background: Option<bool>,

    /// Optional. Specifies a TTL to control how long MongoDB retains
    /// documents in this collection.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/tutorial/expire-data/)
    /// for more information on how to use this option.
    ///
    /// This applies only to [TTL](https://docs.mongodb.com/manual/reference/glossary/#term-ttl) indexes.
    #[builder(default)]
    #[serde(
        deserialize_with = "bson_util::deserialize_duration_option_from_u64_seconds",
        serialize_with = "bson_util::serialize_duration_option_as_int_secs"
    )]
    pub expire_after: Option<Duration>,

    /// Optional. Specifies a name outside the default generated name.
    ///
    /// If none is provided, the default name is generated in the format "[field]_[direction]".
    ///
    /// Not ethat if an index is created for the same key apttern with different collations, a name
    /// must be provided by the user to avoid ambiguity.
    pub name: Option<String>,

    /// Optional. If true, the index only references documents with the specified field.
    ///
    /// These indexes use less space but behave differently in some situations (particularly
    /// sorts).
    ///
    /// The default value is false.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/core/index-sparse/)
    /// for more information on how to use this option.
    #[builder(default)]
    pub sparse: Option<bool>,

    /// Optional. Allows users to configure the storage engine on a per-index basis when creating
    /// an index.
    ///
    /// The storageEngine option should take the following form:
    ///
    /// storageEngine: { <storage-engine-name>: <options> }
    #[builder(default)]
    pub storage_engine: Option<Document>,

    /// Optional. Creates a unique index so that the collection will not accept insertion or update
    /// of documents where the index key value matches an existing value in the index.
    ///
    /// Specify true to create a unique index. The default value is false.
    ///
    /// The option is unavailable for [hashed](https://docs.mongodb.com/manual/core/index-hashed/) indexes.
    #[builder(default)]
    pub unique: Option<bool>,

    /// Specify the version number of the index. Starting in MongoDB 3.2, this option is not
    /// allowed.
    pub version: Option<IndexVersion>,

    /// Optional. For text indexes, the language that determines the list of stop words and the
    /// rules for the stemmer and tokenizer.
    ///
    /// See [Text Search Languages](https://docs.mongodb.com/manual/reference/text-search-languages/#text-search-languages)
    /// for the available languages and
    /// [Specify a Language for Text Index](https://docs.mongodb.com/manual/tutorial/specify-language-for-text-index/) for
    /// more information and examples.
    ///
    /// The default value is english.
    #[builder(default)]
    pub default_language: Option<String>,

    /// Optional. For text indexes, the name of the field, in the collection’s documents, that
    /// contains the override language for the document. The default value is language. See
    /// [Use any Field to Specify the Language for a Document](https://docs.mongodb.com/manual/tutorial/specify-language-for-text-index/#specify-language-field-text-index-example) for an example.
    #[builder(default)]
    pub language_override: Option<String>,

    /// Optional. The text index version number. Users can use this option to override the default
    /// version number.
    #[builder(default)]
    pub text_index_version: Option<TextIndexVersion>,

    /// Optional. For text indexes, a document that contains field and weight pairs.
    ///
    /// The weight is an integer ranging from 1 to 99,999 and denotes the significance of the field
    /// relative to the other indexed fields in terms of the score.
    /// You can specify weights for some or all the indexed fields.
    /// See [Control Search Results with Weights](https://docs.mongodb.com/manual/tutorial/control-results-of-text-search/)
    /// to adjust the scores.
    ///
    /// The default value is 1.
    #[builder(default)]
    pub weights: Option<Document>,

    /// Optional. The 2dsphere index version number.
    /// Users can use this option to override the default version number.
    #[builder(default)]
    #[serde(rename = "2dsphereIndexVersion")]
    pub sphere_2d_index_version: Option<Sphere2DIndexVersion>,

    /// Optional. For 2d indexes, the number of precision of the stored geohash value of the
    /// location data. The bits value ranges from 1 to 32 inclusive.
    ///
    /// The default value is 26.
    #[builder(default)]
    #[serde(serialize_with = "bson_util::serialize_u32_option_as_i32")]
    pub bits: Option<u32>,

    /// Optional. For 2d indexes, the upper inclusive boundary for the longitude and latitude
    /// values.
    ///
    /// The default value is -180.0.
    #[builder(default)]
    pub max: Option<f64>,

    /// Optional. For 2d indexes, the lower inclusive boundary for the longitude and latitude
    /// values.
    ///
    /// The default value is -180.0.
    #[builder(default)]
    pub min: Option<f64>,

    /// For geoHaystack indexes, specify the number of units within which to group the location
    /// values; i.e. group in the same bucket those location values that are within the
    /// specified number of units to each other.
    ///
    /// The value must be greater than 0.
    #[builder(default)]
    #[serde(serialize_with = "bson_util::serialize_u32_option_as_i32")]
    bucket_size: Option<u32>,

    /// Optional. If specified, the index only references documents that match the filter
    /// expression. See Partial Indexes for more information.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/core/index-partial/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub partial_filter_expression: Option<Document>,

    /// Optional. Specifies the collation for the index.
    ///
    /// [Collation](https://docs.mongodb.com/manual/reference/collation/) allows users to specify language-specific rules for
    /// string comparison, such as rules for lettercase and accent marks.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// Optional. Allows users to include or exclude specific field paths from a
    /// [wildcard index](https://docs.mongodb.com/manual/core/index-wildcard/#wildcard-index-core)
    /// using the { "$**" : 1} key pattern.
    ///
    /// This is only used when you specific a wildcard index field
    #[builder(default)]
    pub wildcard_projection: Option<Document>,

    /// Optional. A flag that determines whether the index is hidden from the query planner. A
    /// hidden index is not evaluated as part of the query plan selection.
    ///
    /// Default is false.
    ///
    /// See the
    /// [documentation](https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/#method-createindex-hidden)
    /// for more information on how to use this option.
    #[builder(default)]
    pub hidden: Option<bool>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum IndexVersion {
    #[deprecated]
    V0,
    V1,
    V2,
    Custom(u32),
}

#[allow(deprecated)]
impl Serialize for IndexVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            IndexVersion::V0 => serializer.serialize_u32(0),
            IndexVersion::V1 => serializer.serialize_u32(1),
            IndexVersion::V2 => serializer.serialize_u32(2),
            IndexVersion::Custom(i) => serializer.serialize_u32(i),
        }
    }
}

#[allow(deprecated)]
impl<'de> Deserialize<'de> for IndexVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match u32::deserialize(deserializer)? {
            0 => Ok(IndexVersion::V0),
            1 => Ok(IndexVersion::V1),
            2 => Ok(IndexVersion::V2),
            i => Ok(IndexVersion::Custom(i)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum TextIndexVersion {
    V1,
    V2,
    V3,
    Custom(u32),
}

impl Serialize for TextIndexVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            TextIndexVersion::V1 => serializer.serialize_u32(1),
            TextIndexVersion::V2 => serializer.serialize_u32(2),
            TextIndexVersion::V3 => serializer.serialize_u32(3),
            TextIndexVersion::Custom(i) => serializer.serialize_u32(i),
        }
    }
}

impl<'de> Deserialize<'de> for TextIndexVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match u32::deserialize(deserializer)? {
            1 => Ok(TextIndexVersion::V1),
            2 => Ok(TextIndexVersion::V2),
            3 => Ok(TextIndexVersion::V3),
            i => Ok(TextIndexVersion::Custom(i)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Sphere2DIndexVersion {
    V2,
    V3,
    Custom(u32),
}

impl Serialize for Sphere2DIndexVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Sphere2DIndexVersion::V2 => serializer.serialize_u32(2),
            Sphere2DIndexVersion::V3 => serializer.serialize_u32(3),
            Sphere2DIndexVersion::Custom(i) => serializer.serialize_u32(i),
        }
    }
}

impl<'de> Deserialize<'de> for Sphere2DIndexVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match u32::deserialize(deserializer)? {
            2 => Ok(Sphere2DIndexVersion::V2),
            3 => Ok(Sphere2DIndexVersion::V3),
            i => Ok(Sphere2DIndexVersion::Custom(i)),
        }
    }
}
