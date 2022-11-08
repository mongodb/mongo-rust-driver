// TODO(RUST-1395) Remove these allows.
#![allow(dead_code, unused_variables)]

mod download;
pub mod options;
mod upload;

use core::task::{Context, Poll};
use std::{
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
};

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    bson::{doc, oid::ObjectId, Bson, DateTime, Document, RawBinaryRef},
    cursor::Cursor,
    error::{ErrorKind, GridFsErrorKind, GridFsFileIdentifier, Result},
    options::{CollectionOptions, FindOptions, ReadConcern, SelectionCriteria, WriteConcern},
    Collection,
    Database,
};

use options::*;

pub const DEFAULT_BUCKET_NAME: &str = "fs";
pub const DEFAULT_CHUNK_SIZE_BYTES: u32 = 255 * 1024;

// Contained in a "chunks" collection for each user file
#[derive(Debug, Deserialize, Serialize)]
struct Chunk<'a> {
    #[serde(rename = "_id")]
    id: ObjectId,
    files_id: Bson,
    n: u32,
    #[serde(borrow)]
    data: RawBinaryRef<'a>,
}

/// A collection in which information about stored files is stored. There will be one files
/// collection document per stored file.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[skip_serializing_none]
#[non_exhaustive]
pub struct FilesCollectionDocument {
    #[serde(rename = "_id")]
    pub id: Bson,
    pub length: u64,
    pub chunk_size: u32,
    pub upload_date: DateTime,
    pub filename: Option<String>,
    pub metadata: Option<Document>,
}

impl FilesCollectionDocument {
    /// Returns the total number of chunks expected to be in the file.
    fn n(&self) -> u32 {
        Self::n_from_vals(self.length, self.chunk_size)
    }

    fn n_from_vals(length: u64, chunk_size: u32) -> u32 {
        let chunk_size = chunk_size as u64;
        let n = length / chunk_size + u64::from(length % chunk_size != 0);
        n as u32
    }

    /// Returns the expected length of a chunk given its index.
    fn expected_chunk_length(&self, n: u32) -> u32 {
        Self::expected_chunk_length_from_vals(self.length, self.chunk_size, n)
    }

    fn expected_chunk_length_from_vals(length: u64, chunk_size: u32, n: u32) -> u32 {
        let remainder = length % (chunk_size as u64);
        if n == Self::n_from_vals(length, chunk_size) - 1 && remainder != 0 {
            remainder as u32
        } else {
            chunk_size
        }
    }
}

#[derive(Debug)]
struct GridFsBucketInner {
    db: Database,
    options: GridFsBucketOptions,
    files: Collection<FilesCollectionDocument>,
    chunks: Collection<Chunk<'static>>,
    created_indexes: AtomicBool,
}

/// Struct for storing GridFS managed files within a [`Database`].
#[derive(Debug, Clone)]
pub struct GridFsBucket {
    inner: Arc<GridFsBucketInner>,
}

// TODO: RUST-1395 Add documentation and example code for this struct.
pub struct GridFsUploadStream {
    files_id: Bson,
}

impl GridFsUploadStream {
    /// Gets the file `id` for the stream.
    pub fn files_id(&self) -> &Bson {
        &self.files_id
    }

    /// Consumes the stream and uploads data in the stream to the server.
    pub async fn finish(self) {
        todo!()
    }

    /// Aborts the upload and discards the upload stream.
    pub async fn abort(self) {
        todo!()
    }
}

impl tokio::io::AsyncWrite for GridFsUploadStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        todo!()
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        todo!()
    }
}

impl futures_util::AsyncWrite for GridFsUploadStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<core::result::Result<usize, futures_util::io::Error>> {
        todo!()
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<core::result::Result<(), futures_util::io::Error>> {
        todo!()
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<core::result::Result<(), futures_util::io::Error>> {
        todo!()
    }
}

impl GridFsBucket {
    pub(crate) fn new(db: Database, mut options: GridFsBucketOptions) -> GridFsBucket {
        if options.read_concern.is_none() {
            options.read_concern = db.read_concern().cloned();
        }
        if options.write_concern.is_none() {
            options.write_concern = db.write_concern().cloned();
        }
        if options.selection_criteria.is_none() {
            options.selection_criteria = db.selection_criteria().cloned();
        }

        let bucket_name = options
            .bucket_name
            .as_deref()
            .unwrap_or(DEFAULT_BUCKET_NAME);

        let collection_options = CollectionOptions::builder()
            .read_concern(options.read_concern.clone())
            .write_concern(options.write_concern.clone())
            .selection_criteria(options.selection_criteria.clone())
            .build();
        let files = db.collection_with_options::<FilesCollectionDocument>(
            &format!("{}.files", bucket_name),
            collection_options.clone(),
        );
        let chunks = db.collection_with_options::<Chunk>(
            &format!("{}.chunks", bucket_name),
            collection_options,
        );

        GridFsBucket {
            inner: Arc::new(GridFsBucketInner {
                db: db.clone(),
                options,
                files,
                chunks,
                created_indexes: AtomicBool::new(false),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn client(&self) -> &crate::Client {
        self.inner.files.client()
    }

    /// Gets the read concern of the [`GridFsBucket`].
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    /// Gets the write concern of the [`GridFsBucket`].
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.options.write_concern.as_ref()
    }

    /// Gets the selection criteria of the [`GridFsBucket`].
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    /// Gets the chunk size in bytes for the [`GridFsBucket`].
    fn chunk_size_bytes(&self) -> u32 {
        self.inner
            .options
            .chunk_size_bytes
            .unwrap_or(DEFAULT_CHUNK_SIZE_BYTES)
    }

    /// Gets a handle to the files collection for the [`GridFsBucket`].
    fn files(&self) -> &Collection<FilesCollectionDocument> {
        &self.inner.files
    }

    /// Gets a handle to the chunks collection for the [`GridFsBucket`].
    fn chunks(&self) -> &Collection<Chunk<'static>> {
        &self.inner.chunks
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The application provides a custom file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub async fn open_upload_stream_with_id(
        &self,
        id: Bson,
        filename: String,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<GridFsUploadStream> {
        todo!()
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The driver generates a unique [`Bson::ObjectId`] for the file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub async fn open_upload_stream(
        &self,
        filename: String,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<GridFsUploadStream> {
        self.open_upload_stream_with_id(Bson::ObjectId(ObjectId::new()), filename, options)
            .await
    }

    /// Deletes the [`FilesCollectionDocument`] with the given `id `and its associated chunks from
    /// this bucket.
    pub async fn delete(&self, id: Bson) -> Result<()> {
        let delete_result = self
            .files()
            .delete_one(doc! { "_id": id.clone() }, None)
            .await?;
        // Delete chunks regardless of whether a file was found. This will remove any possibly
        // orphaned chunks.
        self.chunks()
            .delete_many(doc! { "files_id": id.clone() }, None)
            .await?;

        if delete_result.deleted_count == 0 {
            return Err(ErrorKind::GridFs(GridFsErrorKind::FileNotFound {
                identifier: GridFsFileIdentifier::Id(id),
            })
            .into());
        }

        Ok(())
    }

    /// Finds and returns the [`FilesCollectionDocument`]s within this bucket that match the given
    /// filter.
    pub async fn find(
        &self,
        filter: Document,
        options: impl Into<Option<GridFsFindOptions>>,
    ) -> Result<Cursor<FilesCollectionDocument>> {
        let find_options = options.into().map(FindOptions::from);
        self.files().find(filter, find_options).await
    }

    /// Renames the file with the given 'id' to the provided `new_filename`.
    pub async fn rename(&self, id: Bson, new_filename: impl AsRef<str>) -> Result<()> {
        self.files()
            .update_one(
                doc! { "_id": id },
                doc! { "$set": { "filename": new_filename.as_ref() } },
                None,
            )
            .await?;

        Ok(())
    }

    /// Drops all of the files and their associated chunks in this bucket.
    pub async fn drop(&self) -> Result<()> {
        self.files().drop(None).await?;
        self.chunks().drop(None).await?;

        Ok(())
    }
}
