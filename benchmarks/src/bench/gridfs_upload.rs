use std::path::PathBuf;

use anyhow::{Context, Result};
use futures::AsyncReadExt;
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

    async fn before_task(&mut self) -> Result<()> {
        self.bucket.drop().await.context("bucket drop")?;

        self.bucket
            .upload_from_futures_0_3_reader("beforetask", &[11u8][..], None)
            .await
            .context("single byte upload")?;

        Ok(())
    }

    async fn do_task(&self) -> Result<()> {
        self.bucket
            .upload_from_futures_0_3_reader("gridfstest", &self.bytes[..], None)
            .await
            .context("upload bytes")?;

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(&self.uri, &DATABASE_NAME)
            .await
            .context("drop database teardown")?;

        Ok(())
    }
}
