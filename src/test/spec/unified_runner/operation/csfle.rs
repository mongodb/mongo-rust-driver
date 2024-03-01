use std::fmt::Debug;

use futures::{future::BoxFuture, stream::TryStreamExt, FutureExt};
use serde::Deserialize;

use super::{Entity, TestOperation, TestRunner};

use crate::{
    action::csfle::DataKeyOptions,
    bson::{doc, Bson},
    client_encryption::MasterKey,
    error::Result,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct GetKeyByAltName {
    key_alt_name: String,
}

impl TestOperation for GetKeyByAltName {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let ce = test_runner.get_client_encryption(id).await;
            let key = ce.get_key_by_alt_name(&self.key_alt_name).await?;
            let ent = match key {
                Some(rd) => Entity::Bson(Bson::Document(rd.to_document()?)),
                None => Entity::None,
            };
            Ok(Some(ent))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeleteKey {
    id: bson::Binary,
}

impl TestOperation for DeleteKey {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let ce = test_runner.get_client_encryption(id).await;
            let result = ce.delete_key(&self.id).await?;
            Ok(Some(Entity::Bson(Bson::Document(bson::to_document(
                &result,
            )?))))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct GetKey {
    id: bson::Binary,
}

impl TestOperation for GetKey {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let ce = test_runner.get_client_encryption(id).await;
            let entity = match ce.get_key(&self.id).await? {
                Some(key) => Entity::Bson(Bson::Document(key.to_document()?)),
                None => Entity::None,
            };
            Ok(Some(entity))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AddKeyAltName {
    id: bson::Binary,
    key_alt_name: String,
}

impl TestOperation for AddKeyAltName {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let ce = test_runner.get_client_encryption(id).await;
            let entity = match ce.add_key_alt_name(&self.id, &self.key_alt_name).await? {
                Some(key) => Entity::Bson(Bson::Document(key.to_document()?)),
                None => Entity::None,
            };
            Ok(Some(entity))
        }
        .boxed()
    }
}

#[derive(Debug)]
pub(super) struct CreateDataKey {
    kms_provider: mongocrypt::ctx::KmsProvider,
    master_key: MasterKey,
    opts: Option<DataKeyOptions>,
}

impl<'de> Deserialize<'de> for CreateDataKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct TestOptions {
            master_key: Option<MasterKey>,
            key_alt_names: Option<Vec<String>>,
            key_material: Option<bson::Binary>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct TestOp {
            kms_provider: mongocrypt::ctx::KmsProvider,
            opts: Option<TestOptions>,
        }
        let t_op = TestOp::deserialize(deserializer)?;
        Ok(CreateDataKey {
            kms_provider: t_op.kms_provider,
            master_key: t_op
                .opts
                .as_ref()
                .and_then(|o| o.master_key.as_ref())
                .cloned()
                .unwrap_or(MasterKey::Local),
            opts: t_op.opts.map(|to| DataKeyOptions {
                key_alt_names: to.key_alt_names,
                key_material: to.key_material.map(|bin| bin.bytes),
            }),
        })
    }
}

impl TestOperation for CreateDataKey {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let ce = test_runner.get_client_encryption(id).await;
            let key = ce
                .create_data_key(self.master_key.clone())
                .with_options(self.opts.clone())
                .test_kms_provider(self.kms_provider.clone())
                .await?;
            Ok(Some(Entity::Bson(Bson::Binary(key))))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct GetKeys {}

impl TestOperation for GetKeys {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let ce = test_runner.get_client_encryption(id).await;
            let mut cursor = ce.get_keys().await?;
            let mut keys = vec![];
            while let Some(key) = cursor.try_next().await? {
                keys.push(Bson::Document(key.to_document()?));
            }
            Ok(Some(Entity::Bson(Bson::Array(keys))))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RemoveKeyAltName {
    id: bson::Binary,
    key_alt_name: String,
}

impl TestOperation for RemoveKeyAltName {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let ce = test_runner.get_client_encryption(id).await;
            let entity = match ce.remove_key_alt_name(&self.id, &self.key_alt_name).await? {
                Some(key) => Entity::Bson(Bson::Document(key.to_document()?)),
                None => Entity::None,
            };
            Ok(Some(entity))
        }
        .boxed()
    }
}
