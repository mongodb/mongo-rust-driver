use std::path::PathBuf;

use anyhow::{Context, Result};
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
        let file_id = bucket
            .upload_from_futures_0_3_reader("gridfstest", file, None)
            .await
            .context("upload file")?;

        Ok(Self {
            uri: options.uri,
            bucket,
            file_id: file_id.into(),
        })
    }

    async fn do_task(&self) -> Result<()> {
        let mut buf = vec![];
        self.bucket
            .download_to_futures_0_3_writer(self.file_id.clone(), &mut buf)
            .await
            .context("download file")?;

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(&self.uri, &DATABASE_NAME)
            .await
            .context("drop database teardown")?;

        Ok(())
    }
}
