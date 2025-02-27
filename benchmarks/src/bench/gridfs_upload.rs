use std::path::PathBuf;

use anyhow::{Context, Result};
use futures::{AsyncReadExt, AsyncWriteExt};
use mongodb::{gridfs::GridFsBucket, Client};

use crate::{
    bench::{drop_database, Benchmark, DATABASE_NAME},
    fs::open_async_read_compat,
};

pub struct GridFsUploadBenchmark {
    uri: String,
    bucket: GridFsBucket,
    bytes: Vec<u8>,
}

pub struct Options {
    pub uri: String,
    pub path: PathBuf,
}

#[async_trait::async_trait]
impl Benchmark for GridFsUploadBenchmark {
    type Options = Options;
    type TaskState = ();

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(&options.uri, &DATABASE_NAME).await?;

        let bucket = db.gridfs_bucket(None);

        let mut file = open_async_read_compat(&options.path).await?;
        let mut bytes = vec![];
        file.read_to_end(&mut bytes).await?;

        Ok(Self {
            uri: options.uri,
            bucket,
            bytes,
        })
    }

    async fn before_task(&self) -> Result<Self::TaskState> {
        self.bucket.drop().await.context("bucket drop")?;
        let mut upload = self.bucket.open_upload_stream("beforetask").await?;
        upload.write_all(&[11u8][..]).await?;
        upload.close().await?;

        Ok(())
    }

    async fn do_task(&self, _state: Self::TaskState) -> Result<()> {
        let mut upload = self.bucket.open_upload_stream("gridfstest").await?;
        upload.write_all(&self.bytes[..]).await?;
        upload.close().await?;

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(&self.uri, &DATABASE_NAME)
            .await
            .context("drop database teardown")?;

        Ok(())
    }
}
