use std::time::Duration;

use crate::{bson::Document, bson_util, collation::Collation};

<<<<<<< HEAD
use serde::{Deserialize, Serialize};
<<<<<<< HEAD
use serde_with::skip_serializing_none;
=======
use serde::{Deserialize, Deserializer, Serialize, Serializer};
>>>>>>> a521ae6 (fix)
=======
>>>>>>> 4a468a7 (Update documentation for Options)
use typed_builder::TypedBuilder;

/// These are the valid options for specifying an [`IndexModel`](../struct.IndexModel.html).
/// For more information on these properties, see the [documentation](https://docs.mongodb.com/manual/reference/command/createIndexes/#definition).
#[derive(Clone, Debug, Default, Deserialize, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[non_exhaustive]
pub struct IndexOptions {
    /// Tells the server to build the index in the background and not block other tasks. Starting
    /// in MongoDB 4.2, this option is deprecated and ignored by the server.
    pub background: Option<bool>,

    /// Specifies a TTL to control how long MongoDB retains
    /// documents in this collection.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/core/index-ttl/)
    /// for more information on how to use this option.
    #[serde(
        deserialize_with = "bson_util::deserialize_duration_option_from_u64_seconds",
        serialize_with = "bson_util::serialize_duration_option_as_int_secs"
    )]
    pub expire_after: Option<Duration>,

    /// Specifies a name outside the default generated name.
    pub name: Option<String>,

    /// If true, the index only references documents with the specified field. The
    /// default value is false.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/core/index-sparse/)
    /// for more information on how to use this option.
    pub sparse: Option<bool>,

    /// Allows users to configure the storage engine on a per-index basis when creating
    /// an index.
    pub storage_engine: Option<Document>,

    /// Forces the index to be unique. The default value is false.
    pub unique: Option<bool>,

    /// Specify the version number of the index. Starting in MongoDB 3.2, this option is not
    /// allowed.
    pub version: Option<IndexVersion>,

    /// For text indexes, the language that determines the list of stop words and the
    /// rules for the stemmer and tokenizer.
    pub default_language: Option<String>,

    /// For `text` indexes, the name of the field, in the collection’s documents, that
    /// contains the override language for the document.
    pub language_override: Option<String>,

    /// The `text` index version number. Users can use this option to override the default
    /// version number.
    pub text_index_version: Option<TextIndexVersion>,

    /// For `text` indexes, a document that contains field and weight pairs.
    pub weights: Option<Document>,

    /// The `2dsphere` index version number.
    /// As of MongoDB 3.2, version 3 is the default. Version 2 is the default in MongoDB 2.6 and
    /// 3.0 series.
    #[serde(rename = "2dsphereIndexVersion")]
    pub sphere_2d_index_version: Option<Sphere2DIndexVersion>,

    /// For `2dsphere` indexes, the number of precision of the stored geohash value of the
    /// location data. The bits value ranges from 1 to 32 inclusive.
    #[serde(serialize_with = "bson_util::serialize_u32_option_as_i32")]
    pub bits: Option<u32>,

    /// For `2dsphere` indexes, the upper inclusive boundary for the longitude and latitude
    /// values.
    pub max: Option<f64>,

    /// For `2dsphere` indexes, the lower inclusive boundary for the longitude and latitude
    /// values.
    pub min: Option<f64>,

    /// For `geoHaystack` indexes, specify the number of units within which to group the location
    /// values.
    #[serde(serialize_with = "bson_util::serialize_u32_option_as_i32")]
    bucket_size: Option<u32>,

    /// If specified, the index only references documents that match the filter
    /// expression. See Partial Indexes for more information.
    pub partial_filter_expression: Option<Document>,

    /// Specifies the collation for the index.
    pub collation: Option<Collation>,

    /// Allows users to include or exclude specific field paths from a wildcard index.
    pub wildcard_projection: Option<Document>,

    /// A flag that determines whether the index is hidden from the query planner. A
    /// hidden index is not evaluated as part of the query plan selection.
    pub hidden: Option<bool>,
}

/// The version of the index. Version 0 Indexes are disallowed as of MongoDB 3.2.
///
/// See [Version 0 Indexes](https://docs.mongodb.com/manual/release-notes/3.2-compatibility/#std-label-3.2-version-0-indexes) for more information.
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

/// Specify the version for a `text` index. For more information, see [Versions](https://docs.mongodb.com/manual/core/index-text/#versions).
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

/// Specify the version for a `2dsphere` index. For more information, see [Versions](https://docs.mongodb.com/manual/core/2dsphere/#versions).
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
