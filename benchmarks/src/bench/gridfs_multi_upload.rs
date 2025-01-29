use std::{fs::read_dir, path::PathBuf};

use anyhow::{Context, Result};
use futures::AsyncWriteExt;
use mongodb::{gridfs::GridFsBucket, Client};

use crate::{
    bench::{drop_database, Benchmark, DATABASE_NAME},
    fs::open_async_read_compat,
};

pub struct GridFsMultiUploadBenchmark {
    uri: String,
    bucket: GridFsBucket,
    path: PathBuf,
}

pub struct Options {
    pub uri: String,
    pub path: PathBuf,
}

#[async_trait::async_trait]
impl Benchmark for GridFsMultiUploadBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(&options.uri, &DATABASE_NAME)
            .await
            .context("drop database setup")?;

        Ok(Self {
            uri: options.uri,
            bucket: db.gridfs_bucket(None),
            path: options.path,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        self.bucket.drop().await.context("bucket drop")?;
        let mut upload = self.bucket.open_upload_stream("beforetask").await?;
        upload.write_all(&[11u8][..]).await?;
        upload.close().await?;

        Ok(())
    }

    async fn do_task(&mut self) -> Result<()> {
        let mut tasks = vec![];

        for entry in read_dir(&self.path)? {
            let bucket = self.bucket.clone();

            tasks.push(crate::spawn(async move {
                let path = entry?.path();
                let file = open_async_read_compat(&path).await?;
                let mut upload = bucket
                    .open_upload_stream(path.display().to_string())
                    .await?;
                futures_util::io::copy(file, &mut upload).await?;
                upload.close().await?;

                let ok: anyhow::Result<()> = Ok(());
                ok
            }));
        }

        for task in tasks {
            task.await?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(&self.uri, &DATABASE_NAME)
            .await
            .context("drop database teardown")?;

        Ok(())
    }
}
