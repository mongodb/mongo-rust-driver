use std::time::Duration;

use serde::Deserialize;
use typed_builder::TypedBuilder;

use crate::{
    bson::Document,
    options::{FindOptions, ReadConcern, SelectionCriteria, WriteConcern},
};

/// Contains the options for creating a [`GridFsBucket`].
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GridFsBucketOptions {
    /// The bucket name. Defaults to 'fs'.
    pub bucket_name: Option<String>,

    /// The chunk size in bytes used to break the user file into chunks. Defaults to 255 KiB.
    pub chunk_size_bytes: Option<u32>,

    /// The write concern. Defaults to the write concern of the database.
    pub write_concern: Option<WriteConcern>,

    /// The read concern. Defaults to the read concern of the database.
    pub read_concern: Option<ReadConcern>,

    /// The selection criteria. Defaults to the selection criteria of the database.
    pub selection_criteria: Option<SelectionCriteria>,
}

/// Contains the options for creating a [`GridFsUploadStream`] to upload a file to a
/// [`GridFsBucket`].
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GridFsUploadOptions {
    /// The number of bytes per chunk of this file. Defaults to the `chunk_size_bytes` specified
    /// in the [`GridFsBucketOptions`].
    #[serde(rename = "chunkSizeBytes")]
    pub chunk_size_bytes: Option<u32>,

    /// User data for the 'metadata' field of the files collection document.
    pub metadata: Option<Document>,
}

/// Contains the options for creating a [`GridFsDownloadStream`] to retrieve a stored file
/// from a [`GridFsBucket`].
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GridFsDownloadByNameOptions {
    /// Which revision (documents with the same filename and different `upload_date`)
    /// of the file to retrieve. Defaults to -1 (the most recent revision).
    ///
    /// Revision numbers are defined as follows:
    /// 0 = the original stored file
    /// 1 = the first revision
    /// 2 = the second revision
    /// etc...
    /// -2 = the second most recent revision
    /// -1 = the most recent revision
    pub revision: Option<i32>,
}

/// Contains the options for performing a find operation on a files collection.  
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GridFsFindOptions {
    /// Enables writing to temporary files on the server. When set to true, the
    /// server can write temporary data to disk while executing the find operation
    /// on the files collection.
    pub allow_disk_use: Option<bool>,

    /// The number of documents to return per batch.
    pub batch_size: Option<u32>,

    /// The maximum number of documents to return.
    pub limit: Option<i64>,

    /// The maximum amount of time to allow the query to run.
    pub max_time: Option<Duration>,

    /// The number of documents to skip before returning.
    pub skip: Option<u64>,

    /// The order by which to sort results. Defaults to not sorting.
    pub sort: Option<Document>,
}

impl From<GridFsFindOptions> for FindOptions {
    fn from(options: GridFsFindOptions) -> Self {
        Self {
            allow_disk_use: options.allow_disk_use,
            batch_size: options.batch_size,
            limit: options.limit,
            max_time: options.max_time,
            skip: options.skip,
            sort: options.sort,
            ..Default::default()
        }
    }
}
