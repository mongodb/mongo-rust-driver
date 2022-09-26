use tokio::io::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use super::{options::GridFsDownloadByNameOptions, FilesCollectionDocument, GridFsBucket};
use crate::{
    bson::{doc, Bson},
    error::{ErrorKind, GridFsErrorKind, GridFsFileIdentifier, Result},
    options::{FindOneOptions, FindOptions},
};

impl GridFsBucket {
    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the `destination`.
    pub async fn download_to_tokio_writer<T>(&self, id: Bson, destination: T) -> Result<()>
    where
        T: tokio::io::AsyncWrite + std::marker::Unpin,
    {
        let options = FindOneOptions::builder()
            .read_concern(self.read_concern().cloned())
            .selection_criteria(self.selection_criteria().cloned())
            .build();

        let file = match self
            .inner
            .files
            .find_one(doc! { "_id": id.clone() }, options)
            .await?
        {
            Some(fcd) => fcd,
            None => {
                return Err(ErrorKind::GridFs(GridFsErrorKind::FileNotFound {
                    identifier: GridFsFileIdentifier::Id(id),
                })
                .into());
            }
        };
        self.download_to_tokio_writer_common(file, destination)
            .await
    }

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the `destination`.
    pub async fn download_to_futures_0_3_writer<T>(&self, id: Bson, destination: T) -> Result<()>
    where
        T: futures_util::io::AsyncWrite + std::marker::Unpin,
    {
        self.download_to_tokio_writer(id, &mut destination.compat_write())
            .await
    }

    /// Downloads the contents of the stored file specified by `filename` and writes the contents to
    /// the `destination`. If there are multiple files with the same filename, the `revision` in the
    /// options provided is used to determine which one to download. If no `revision` is specified,
    /// the most recent file with the given filename is chosen.
    pub async fn download_to_tokio_writer_by_name<T>(
        &self,
        filename: impl AsRef<str>,
        destination: T,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<()>
    where
        T: tokio::io::AsyncWrite + std::marker::Unpin,
    {
        let revision = options.into().and_then(|opts| opts.revision).unwrap_or(-1);
        let (sort, skip) = if revision >= 0 {
            (1, revision)
        } else {
            (-1, -revision - 1)
        };
        let options = FindOneOptions::builder()
            .sort(doc! { "uploadDate": sort })
            .skip(skip as u64)
            .read_concern(self.read_concern().cloned())
            .selection_criteria(self.selection_criteria().cloned())
            .build();

        let file = match self
            .files()
            .find_one(doc! { "filename": filename.as_ref() }, options)
            .await?
        {
            Some(fcd) => fcd,
            None => {
                if self
                    .files()
                    .find_one(doc! { "filename": filename.as_ref() }, None)
                    .await?
                    .is_some()
                {
                    return Err(
                        ErrorKind::GridFs(GridFsErrorKind::RevisionNotFound { revision }).into(),
                    );
                } else {
                    return Err(ErrorKind::GridFs(GridFsErrorKind::FileNotFound {
                        identifier: GridFsFileIdentifier::Filename(filename.as_ref().into()),
                    })
                    .into());
                }
            }
        };

        self.download_to_tokio_writer_common(file, destination)
            .await
    }

    /// Downloads the contents of the stored file specified by `filename` and writes the contents to
    /// the `destination`. If there are multiple files with the same filename, the `revision` in the
    /// options provided is used to determine which one to download. If no `revision` is specified,
    /// the most recent file with the given filename is chosen.
    pub async fn download_to_futures_0_3_writer_by_name<T>(
        &self,
        filename: impl AsRef<str>,
        destination: T,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<()>
    where
        T: futures_util::io::AsyncWrite + std::marker::Unpin,
    {
        self.download_to_tokio_writer_by_name(filename, &mut destination.compat_write(), options)
            .await
    }

    async fn download_to_tokio_writer_common<T>(
        &self,
        file: FilesCollectionDocument,
        mut destination: T,
    ) -> Result<()>
    where
        T: tokio::io::AsyncWrite + std::marker::Unpin,
    {
        let total_bytes_expected = file.length;
        let chunk_size = file.chunk_size as u64;

        if total_bytes_expected == 0 {
            return Ok(());
        }

        let options = FindOptions::builder()
            .sort(doc! { "n": 1 })
            .read_concern(self.read_concern().cloned())
            .selection_criteria(self.selection_criteria().cloned())
            .build();
        let mut cursor = self
            .chunks()
            .find(doc! { "files_id": &file.id }, options)
            .await?;

        let mut n = 0;
        while cursor.advance().await? {
            let chunk = cursor.deserialize_current()?;
            if chunk.n != n {
                return Err(ErrorKind::GridFs(GridFsErrorKind::MissingChunk { n }).into());
            }

            let chunk_length = chunk.data.bytes.len();
            let expected_length =
                std::cmp::min(total_bytes_expected - chunk_size * n as u64, chunk_size);
            if chunk_length as u64 != expected_length {
                return Err(ErrorKind::GridFs(GridFsErrorKind::WrongSizeChunk {
                    actual_size: chunk_length as u32,
                    expected_size: expected_length as u32,
                })
                .into());
            }

            destination.write_all(chunk.data.bytes).await?;
            n += 1;
        }

        let expected_n =
            total_bytes_expected / chunk_size + u64::from(total_bytes_expected % chunk_size != 0);
        if (n as u64) != expected_n {
            return Err(ErrorKind::GridFs(GridFsErrorKind::WrongNumberOfChunks {
                actual_number: n,
                expected_number: expected_n as u32,
            })
            .into());
        }

        Ok(())
    }
}
