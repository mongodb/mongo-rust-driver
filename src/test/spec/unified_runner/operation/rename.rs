use bson::{doc, Bson, Document};
use futures::FutureExt;
use serde::Deserialize;

use crate::{
    error::Result,
    gridfs::GridFsBucket,
    test::spec::unified_runner::{Entity, TestRunner},
    BoxFuture,
    Collection,
};

use super::TestOperation;

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub(super) struct Rename(Document);

impl TestOperation for Rename {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            match test_runner.entities.read().await.get(id).unwrap() {
                Entity::Collection(c) => {
                    let args: RenameCollection = bson::from_document(self.0.clone()).unwrap();
                    args.run(c.clone(), test_runner).await
                }
                Entity::Bucket(b) => {
                    let args: RenameBucket = bson::from_document(self.0.clone()).unwrap();
                    args.run(b.clone()).await
                }
                other => panic!("cannot execute rename on {:?}", other),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RenameCollection {
    to: String,
}

impl RenameCollection {
    async fn run(
        &self,
        target: Collection<Document>,
        test_runner: &TestRunner,
    ) -> Result<Option<Entity>> {
        let ns = target.namespace();
        let mut to_ns = ns.clone();
        to_ns.coll.clone_from(&self.to);
        let cmd = doc! {
            "renameCollection": crate::bson::to_bson(&ns)?,
            "to": crate::bson::to_bson(&to_ns)?,
        };
        let admin = test_runner.internal_client.database("admin");
        admin.run_command(cmd).await?;
        Ok(None)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RenameBucket {
    id: Bson,
    new_filename: String,
}

impl RenameBucket {
    async fn run(&self, target: GridFsBucket) -> Result<Option<Entity>> {
        target.rename(self.id.clone(), &self.new_filename).await?;
        Ok(None)
    }
}
