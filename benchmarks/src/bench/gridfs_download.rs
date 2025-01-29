use std::path::PathBuf;

use anyhow::{Context, Result};
use futures::{AsyncReadExt, AsyncWriteExt};
use mongodb::{bson::Bson, gridfs::GridFsBucket, Client};

use crate::{
    bench::{drop_database, Benchmark, DATABASE_NAME},
    fs::open_async_read_compat,
};

pub struct GridFsDownloadBenchmark {
    uri: String,
    bucket: GridFsBucket,
    file_id: Bson,
}

pub struct Options {
    pub uri: String,
    pub path: PathBuf,
}

#[async_trait::async_trait]
impl Benchmark for GridFsDownloadBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(&options.uri, &DATABASE_NAME)
            .await
            .context("drop database setup")?;

        let bucket = db.gridfs_bucket(None);

        let file = open_async_read_compat(&options.path).await?;
        let mut upload = bucket.open_upload_stream("gridfstest").await?;
        let file_id = upload.id().clone();
        futures_util::io::copy(file, &mut upload).await?;
        upload.close().await?;

        Ok(Self {
            uri: options.uri,
            bucket,
            file_id,
        })
    }

    async fn do_task(&mut self) -> Result<()> {
        let mut buf = vec![];
        let mut download = self
            .bucket
            .open_download_stream(self.file_id.clone())
            .await?;
        download.read_to_end(&mut buf).await?;

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(&self.uri, &DATABASE_NAME)
            .await
            .context("drop database teardown")?;

        Ok(())
    }
}
