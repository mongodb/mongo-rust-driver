use std::future::IntoFuture;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;

use crate::{
    bson::{Bson, Document},
    error::{ErrorKind, Result},
    operation::BulkWrite,
    Client,
};

use super::{
    models::add_ids_to_insert_one_models,
    results::{SummaryBulkWriteResult, VerboseBulkWriteResult},
    BulkWriteOptions,
    WriteModel,
};

pub struct VerboseBulkWriteAction {
    client: Client,
    models: Vec<WriteModel>,
    options: BulkWriteOptions,
}

impl VerboseBulkWriteAction {
    pub fn ordered(mut self, ordered: bool) -> Self {
        self.options.ordered = Some(ordered);
        self
    }

    pub fn bypass_document_validation(mut self, bypass_document_validation: bool) -> Self {
        self.options.bypass_document_validation = Some(bypass_document_validation);
        self
    }

    pub fn comment(mut self, comment: impl Into<Bson>) -> Self {
        self.options.comment = Some(comment.into());
        self
    }

    pub fn let_vars(mut self, let_vars: Document) -> Self {
        self.options.let_vars = Some(let_vars);
        self
    }
}

impl IntoFuture for VerboseBulkWriteAction {
    type Output = Result<VerboseBulkWriteResult>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        async move {
            let inserted_ids = add_ids_to_insert_one_models(&mut self.models)?;

            let bulk_write = BulkWrite {
                models: &self.models,
                options: self.options,
                client: self.client.clone(),
            };
            let (mut cursor, summary_info) =
                self.client.execute_operation(bulk_write, None).await?;

            let mut results = VerboseBulkWriteResult::new(summary_info, inserted_ids);
            while cursor.advance().await? {
                let response = cursor.deserialize_current()?;
                let model =
                    self.models
                        .get(response.index)
                        .ok_or_else(|| ErrorKind::InvalidResponse {
                            message: format!(
                                "unknown index returned from bulkWrite: {}",
                                response.index
                            ),
                        })?;

                match model {
                    WriteModel::InsertOne { .. } => {
                        debug_assert!(!response.is_update_result());
                    }
                    WriteModel::UpdateOne { .. }
                    | WriteModel::UpdateMany { .. }
                    | WriteModel::ReplaceOne { .. } => {
                        results.add_update_result(response)?;
                    }
                    WriteModel::DeleteOne { .. } | WriteModel::DeleteMany { .. } => {
                        debug_assert!(!response.is_update_result());
                        results.add_delete_result(response);
                    }
                }
            }

            Ok(results)
        }
        .boxed()
    }
}

pub struct SummaryBulkWriteAction {
    inner: VerboseBulkWriteAction,
}

impl SummaryBulkWriteAction {
    pub(crate) fn new(client: Client, models: Vec<WriteModel>) -> Self {
        Self {
            inner: VerboseBulkWriteAction {
                client,
                models,
                options: Default::default(),
            },
        }
    }

    pub fn ordered(self, ordered: bool) -> Self {
        Self {
            inner: self.inner.ordered(ordered),
        }
    }

    pub fn bypass_document_validation(self, bypass_document_validation: bool) -> Self {
        Self {
            inner: self
                .inner
                .bypass_document_validation(bypass_document_validation),
        }
    }

    pub fn comment(self, comment: impl Into<Bson>) -> Self {
        Self {
            inner: self.inner.comment(comment),
        }
    }

    pub fn let_vars(self, let_vars: Document) -> Self {
        Self {
            inner: self.inner.let_vars(let_vars),
        }
    }

    pub fn verbose_results(mut self) -> VerboseBulkWriteAction {
        self.inner.options.verbose_results = Some(true);
        self.inner
    }
}

impl IntoFuture for SummaryBulkWriteAction {
    type Output = Result<SummaryBulkWriteResult>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move { self.inner.await.map(Into::into) }.boxed()
    }
}
