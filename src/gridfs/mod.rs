/// This follow officiel [specification](https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst)
/// If you found bug according to the specification, please report bug
use serde::{Deserialize, Serialize};

use crate::{
    bson::{oid::ObjectId, DateTime, Document},
    bson_util::{serialize_u32_as_i32, serialize_u64_as_i64},
};

/// This represent a chunk of a file
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Chunk {
    /// a unique ID for this document of type BSON ObjectId
    #[serde(rename = "_id")]
    id: ObjectId,
    /// the id for this file (the _id from the files collection document). This field takes the
    /// type of the corresponding _id in the files collection.
    files_id: ObjectId,
    /// the index number of this chunk, zero-based.
    #[serde(serialize_with = "serialize_u32_as_i32")]
    n: u32,
    /// a chunk of data from the user file
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetaChunk {
    /// a unique ID for this document. Usually this will be of type ObjectId, but a custom _id
    /// value provided by the application may be of any type.
    #[serde(rename = "_id")]
    id: ObjectId,
    /// the length of this stored file, in bytes
    #[serde(serialize_with = "serialize_u64_as_i64")]
    length: u64,
    /// the size, in bytes, of each data chunk of this file. This value is configurable by file.
    /// The default is 255 KiB.
    chunk_size: u32,
    /// the date and time this file was added to GridFS, stored as a BSON datetime value.
    /// The value of this field MUST be the datetime when the upload completed, not the datetime
    /// when it was begun.
    upload_date: DateTime,
    /// DEPRECATED, a hash of the contents of the stored file
    #[deprecated]
    md5: String,
    /// the name of this stored file; this does not need to be unique
    filename: String,
    /// DEPRECATED, any MIME type, for application use only
    #[deprecated]
    content_type: String,
    /// DEPRECATED, for application use only
    #[deprecated]
    aliases: Vec<String>,
    /// any additional application data the user wishes to store
    metadata: Document,
}
