use crate::{
    error::Result,
    gridfs::{GridFsDownloadByNameOptions, GridFsUploadOptions},
    test::spec::unified_runner::{operation::TestOperation, Entity, TestRunner},
};
use bson::{Bson, Document};
use futures::{future::BoxFuture, AsyncReadExt, AsyncWriteExt};
use futures_util::FutureExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Download {
    id: Bson,
}

impl TestOperation for Download {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;

            let mut buf: Vec<u8> = vec![];
            bucket
                .open_download_stream(self.id.clone())
                .await?
                .read_to_end(&mut buf)
                .await?;
            let writer_data = hex::encode(buf);

            Ok(Some(Entity::Bson(writer_data.into())))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DownloadByName {
    filename: String,
    #[serde(flatten)]
    options: GridFsDownloadByNameOptions,
}

impl TestOperation for DownloadByName {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;

            let mut buf: Vec<u8> = vec![];
            bucket
                .open_download_stream_by_name(&self.filename)
                .with_options(self.options.clone())
                .await?
                .read_to_end(&mut buf)
                .await?;
            let writer_data = hex::encode(buf);

            Ok(Some(Entity::Bson(writer_data.into())))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Delete {
    id: Bson,
}

impl TestOperation for Delete {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;
            bucket.delete(self.id.clone()).await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeleteByName {
    filename: String,
}

impl TestOperation for DeleteByName {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;
            bucket.delete_by_name(&self.filename).await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Upload {
    source: Document,
    filename: String,
    #[serde(flatten)]
    options: GridFsUploadOptions,
}

impl TestOperation for Upload {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;
            let hex_string = self.source.get("$$hexBytes").unwrap().as_str().unwrap();
            let bytes = hex::decode(hex_string).unwrap();

            let id = {
                let mut stream = bucket
                    .open_upload_stream(&self.filename)
                    .with_options(self.options.clone())
                    .await?;
                stream.write_all(&bytes[..]).await?;
                stream.close().await?;
                stream.id().clone()
            };

            Ok(Some(Entity::Bson(id)))
        }
        .boxed()
    }
}
