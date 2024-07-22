use std::{
    fs::{create_dir, read_dir, remove_file},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use futures::AsyncWriteExt;
use mongodb::{bson::Bson, gridfs::GridFsBucket, Client};
use once_cell::sync::Lazy;

use crate::{
    bench::{drop_database, Benchmark, DATABASE_NAME},
    fs::{open_async_read_compat, open_async_write_compat},
};

static DOWNLOAD_PATH: Lazy<PathBuf> =
    Lazy::new(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("gridfs_multi_download"));

pub struct GridFsMultiDownloadBenchmark {
    uri: String,
    bucket: GridFsBucket,
    ids: Vec<Bson>,
}

pub struct Options {
    pub uri: String,
    pub path: PathBuf,
}

#[async_trait::async_trait]
impl Benchmark for GridFsMultiDownloadBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(&options.uri, &DATABASE_NAME)
            .await
            .context("drop database setup")?;

        let bucket = db.gridfs_bucket(None);
        bucket.drop().await.context("drop bucket setup")?;

        let mut ids = vec![];
        for entry in read_dir(options.path)? {
            let path = entry?.path();

            let file = open_async_read_compat(&path).await?;
            let mut upload = bucket
                .open_upload_stream(path.display().to_string())
                .await?;
            let id = upload.id().clone();
            futures_util::io::copy(file, &mut upload).await?;
            upload.close().await?;
            ids.push(id);
        }

        create_dir(DOWNLOAD_PATH.as_path())?;

        Ok(Self {
            uri: options.uri,
            bucket,
            ids,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        for id in &self.ids {
            let path = get_filename(&id);
            if Path::try_exists(&path)? {
                remove_file(path)?;
            }
        }

        Ok(())
    }

    async fn do_task(&self) -> Result<()> {
        let mut tasks = vec![];

        for id in &self.ids {
            let bucket = self.bucket.clone();
            let id = id.clone();

            tasks.push(crate::spawn(async move {
                let download_path = get_filename(&id);
                let mut file = open_async_write_compat(&download_path)
                    .await
                    .context("open file")?;
                let download = bucket.open_download_stream(id).await?;
                futures_util::io::copy(download, &mut file).await?;

                file.flush().await?;

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

fn get_filename(id: &Bson) -> PathBuf {
    DOWNLOAD_PATH.join(format!("file{}.txt", id))
}
