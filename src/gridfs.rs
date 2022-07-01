use core::task::{Context, Poll};
use std::{
    io::{self, Result},
    marker::PhantomPinned,
    pin::Pin,
};

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    selection_criteria::ReadPreference,
    Database,
};

use bson::{oid::ObjectId, DateTime, Document};
use serde::Deserialize;
use typed_builder::TypedBuilder;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GridFSBucketOptions {
    bucket_name: Option<String>,
    chunk_size_bytes: Option<i32>,
    write_concern: Option<WriteConcern>,
    read_concern: Option<ReadConcern>,
    read_preference: Option<ReadPreference>,
}

pub struct GridFSUploadOptions {
    chunk_size_bytes: Option<i32>,
    metadata: Option<Document>,
}

pub struct GridFSDownloadByNameOptions {
    revision: Option<i32>,
}

#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GridFSFindOptions {
    allow_disk_use: Option<bool>,
    batch_size: Option<i32>,
    limit: Option<i32>,
    max_time_ms: Option<i64>,
    no_cursor_timeout: Option<bool>,
    skip: i32,
    sort: Option<Document>,
}

// Contained in a "chunks" collection for each user file
pub struct Chunk<T: Eq + Copy> {
    id: ObjectId,
    files_id: T,
    n: i32,
    // default size is 255 KiB
    data: Vec<u8>,
}

// A collection in which information about stored files is stored. There will be one files
// collection document per stored file.
pub struct FilesCollectionDocument<T: Eq + PartialEq + Copy> {
    id: T,
    length: i64,
    chunk_size: i32,
    upload_date: DateTime,
    filename: String,
    metadata: Document,
}

pub struct GridFSBucket {
    // Contains a "chunks" collection
    pub db: Database,
    pub options: Option<GridFSBucketOptions>,
}

pub struct GridFSStream {
    _pin: PhantomPinned,
}

impl AsyncRead for GridFSStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        todo!()
    }
}

impl AsyncWrite for GridFSStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        todo!()
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        todo!()
    }
}

impl GridFSBucket {
    pub fn open_upload_stream_with_id<T>(
        &self,
        id: T,
        filename: String,
        options: GridFSUploadOptions,
    ) -> GridFSStream {
        todo!()
    }

    pub fn upload_from_stream_with_id<T>(
        &self,
        id: T,
        filename: String,
        source: GridFSStream,
        option: GridFSUploadOptions,
    ) {
        todo!()
    }

    pub fn open_download_stream<T>(&self, id: T) {
        todo!()
    }

    pub fn download_to_stream<T>(&self, id: T, destination: GridFSStream) {
        todo!()
    }

    pub fn delete<T>(&self, id: T) {
        todo!()
    }

    pub fn find<T>(&self, filter: Document, options: GridFSBucketOptions) -> Result<Cursor<T>> {
        todo!()
    }

    pub fn open_download_stream_by_name(
        &self,
        filename: String,
        options: GridFSDownloadByNameOptions,
    ) -> GridFSStream {
        todo!()
    }

    pub fn download_to_stream_by_name(
        &self,
        filename: String,
        destination: GridFSStream,
        options: GridFSDownloadByNameOptions,
    ) {
        todo!()
    }

    pub fn rename<T>(&self, id: T, new_filename: String) {
        todo!()
    }

    pub fn drop(&self) {
        todo!()
    }
}
