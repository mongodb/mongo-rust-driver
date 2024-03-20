use bson::{doc, Bson};

use crate::{
    action::action_impl,
    error::{ErrorKind, GridFsErrorKind, GridFsFileIdentifier, Result},
    gridfs::FilesCollectionDocument,
    GridFsBucket,
    GridFsDownloadStream,
};

impl GridFsBucket {
    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `id`.
    ///
    /// `await` will return `Result<GridFsDownloadStream>`.
    pub fn open_download_stream_2(&self, id: Bson) -> OpenDownloadStream {
        OpenDownloadStream { bucket: self, id }
    }

    // Utility functions for finding files within the bucket.

    async fn find_file_by_id_2(&self, id: &Bson) -> Result<FilesCollectionDocument> {
        match self.find_one(doc! { "_id": id }).await? {
            Some(file) => Ok(file),
            None => Err(ErrorKind::GridFs(GridFsErrorKind::FileNotFound {
                identifier: GridFsFileIdentifier::Id(id.clone()),
            })
            .into()),
        }
    }
}

#[cfg(feature = "sync")]
impl crate::sync::gridfs::GridFsBucket {
    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `id`.
    ///
    /// [`run`](OpenDownloadStream::run) will return `Result<GridFsDownloadStream>`.
    pub fn open_download_stream_2(&self, id: Bson) -> OpenDownloadStream {
        self.async_bucket.open_download_stream_2(id)
    }
}

/// Opens and returns a [`GridFsDownloadStream`] from which the application can read
/// the contents of the stored file specified by an id.  Construct with
/// [`GridFsBucket::open_download_stream`].
#[must_use]
pub struct OpenDownloadStream<'a> {
    bucket: &'a GridFsBucket,
    id: Bson,
}

action_impl! {
    impl<'a> Action for OpenDownloadStream<'a> {
        type Future = OpenDownloadStreamFuture;

        async fn execute(self) -> Result<GridFsDownloadStream> {
            let file = self.bucket.find_file_by_id_2(&self.id).await?;
            GridFsDownloadStream::new(file, self.bucket.chunks()).await
        }

        fn sync_wrap(out) -> Result<crate::sync::gridfs::GridFsDownloadStream> {
            out.map(crate::sync::gridfs::GridFsDownloadStream::new)
        }
    }
}
