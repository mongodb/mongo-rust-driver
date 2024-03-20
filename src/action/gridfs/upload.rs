use bson::{oid::ObjectId, Bson, Document};

use crate::{
    action::{action_impl, option_setters},
    error::Result,
    gridfs::GridFsUploadOptions,
    GridFsBucket,
    GridFsUploadStream,
};

impl GridFsBucket {
    /// Creates and returns a [`GridFsUploadStream`] that the application can write the contents of
    /// the file to.
    ///
    /// `await` will return `Result<GridFsUploadStream>`.
    pub fn open_upload_stream(&self, filename: impl AsRef<str>) -> OpenUploadStream {
        OpenUploadStream {
            bucket: self,
            filename: filename.as_ref().to_owned(),
            id: None,
            options: None,
        }
    }
}

#[cfg(feature = "sync")]
impl crate::sync::gridfs::GridFsBucket {
    /// Creates and returns a [`GridFsUploadStream`] that the application can write the contents of
    /// the file to.
    ///
    /// [`run`](OpenUploadStream::run) will return `Result<GridFsUploadStream>`.
    pub fn open_upload_stream(&self, filename: impl AsRef<str>) -> OpenUploadStream {
        self.async_bucket.open_upload_stream(filename)
    }
}

/// Creates and returns a [`GridFsUploadStream`] that the application can write the contents of
/// a file to.  Construct with [`GridFsBucket::open_upload_stream`].
#[must_use]
pub struct OpenUploadStream<'a> {
    bucket: &'a GridFsBucket,
    filename: String,
    id: Option<Bson>,
    options: Option<GridFsUploadOptions>,
}

impl<'a> OpenUploadStream<'a> {
    /// Set the value to be used for the corresponding [`FilesCollectionDocument`]'s `id`
    /// field.  If not set, a unique [`ObjectId`] will be generated that can be accessed via the
    /// stream's [`id`](GridFsUploadStream::id) method.
    pub fn id(mut self, value: Bson) -> Self {
        self.id = Some(value);
        self
    }

    option_setters! { options: GridFsUploadOptions;
        chunk_size_bytes: u32,
        metadata: Document,
    }
}

action_impl! {
    impl<'a> Action for OpenUploadStream<'a> {
        type Future = OpenUploadStreamFuture;

        async fn execute(self) -> Result<GridFsUploadStream> {
            let id = self.id.unwrap_or_else(|| ObjectId::new().into());
            let chunk_size_bytes = self.options
                .as_ref()
                .and_then(|opts| opts.chunk_size_bytes)
                .unwrap_or_else(|| self.bucket.chunk_size_bytes());
            let metadata = self.options.and_then(|opts| opts.metadata);
            Ok(GridFsUploadStream::new(
                self.bucket.clone(),
                id,
                self.filename,
                chunk_size_bytes,
                metadata,
                self.bucket.client().register_async_drop(),
            ))
        }

        fn sync_wrap(out) -> Result<crate::sync::gridfs::GridFsUploadStream> {
            out.map(crate::sync::gridfs::GridFsUploadStream::new)
        }
    }
}
