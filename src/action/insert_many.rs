use std::{borrow::Borrow, collections::HashSet, ops::Deref};

use bson::{Bson, RawDocumentBuf};
use serde::Serialize;

use crate::{
    coll::options::InsertManyOptions,
    error::{BulkWriteError, BulkWriteFailure, Error, ErrorKind, Result},
    operation::Insert as Op,
    options::WriteConcern,
    results::InsertManyResult,
    ClientSession,
    Collection,
};

use super::{action_impl, deeplink, option_setters, CollRef};

impl<T: Serialize + Send + Sync> Collection<T> {
    /// Inserts the data in `docs` into the collection.
    ///
    /// Note that this method accepts both owned and borrowed values, so the input documents
    /// do not need to be cloned in order to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return d[`Result<InsertManyResult>`].
    #[deeplink]
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> InsertMany {
        InsertMany {
            coll: CollRef::new(self),
            docs: docs
                .into_iter()
                .map(|v| bson::to_raw_document_buf(v.borrow()).map_err(Into::into))
                .collect(),
            options: None,
            session: None,
        }
    }
}

#[cfg(feature = "sync")]
impl<T: Serialize + Send + Sync> crate::sync::Collection<T> {
    /// Inserts the data in `docs` into the collection.
    ///
    /// Note that this method accepts both owned and borrowed values, so the input documents
    /// do not need to be cloned in order to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// [`run`](InsertMany::run) will return d[`Result<InsertManyResult>`].
    #[deeplink]
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> InsertMany {
        self.async_collection.insert_many(docs)
    }
}

/// Inserts documents into a collection.  Construct with [`Collection::insert_many`].
#[must_use]
pub struct InsertMany<'a> {
    coll: CollRef<'a>,
    docs: Result<Vec<RawDocumentBuf>>,
    options: Option<InsertManyOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> InsertMany<'a> {
    option_setters! { options: InsertManyOptions;
        bypass_document_validation: bool,
        ordered: bool,
        write_concern: WriteConcern,
        comment: Bson,
    }

    /// Use the provided session when running the operation.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

#[action_impl]
impl<'a> Action for InsertMany<'a> {
    type Future = InsertManyFuture;

    async fn execute(mut self) -> Result<InsertManyResult> {
        resolve_write_concern_with_session!(self.coll, self.options, self.session.as_ref())?;

        let ds = self.docs?;
        if ds.is_empty() {
            return Err(ErrorKind::InvalidArgument {
                message: "No documents provided to insert_many".to_string(),
            }
            .into());
        }
        let ordered = self
            .options
            .as_ref()
            .and_then(|o| o.ordered)
            .unwrap_or(true);
        #[cfg(feature = "in-use-encryption-unstable")]
        let encrypted = self.coll.client().auto_encryption_opts().await.is_some();
        #[cfg(not(feature = "in-use-encryption-unstable"))]
        let encrypted = false;

        let mut cumulative_failure: Option<BulkWriteFailure> = None;
        let mut error_labels: HashSet<String> = Default::default();
        let mut cumulative_result: Option<InsertManyResult> = None;

        let mut n_attempted = 0;

        while n_attempted < ds.len() {
            let docs: Vec<_> = ds.iter().skip(n_attempted).map(Deref::deref).collect();
            let insert = Op::new(self.coll.namespace(), docs, self.options.clone(), encrypted);

            match self
                .coll
                .client()
                .execute_operation(insert, self.session.as_deref_mut())
                .await
            {
                Ok(result) => {
                    let current_batch_size = result.inserted_ids.len();

                    let cumulative_result =
                        cumulative_result.get_or_insert_with(InsertManyResult::new);
                    for (index, id) in result.inserted_ids {
                        cumulative_result
                            .inserted_ids
                            .insert(index + n_attempted, id);
                    }

                    n_attempted += current_batch_size;
                }
                Err(e) => {
                    let labels = e.labels().clone();
                    match *e.kind {
                        ErrorKind::BulkWrite(bw) => {
                            // for ordered inserts this size will be incorrect, but knowing the
                            // batch size isn't needed for ordered
                            // failures since we return immediately from
                            // them anyways.
                            let current_batch_size = bw.inserted_ids.len()
                                + bw.write_errors.as_ref().map(|we| we.len()).unwrap_or(0);

                            let failure_ref =
                                cumulative_failure.get_or_insert_with(BulkWriteFailure::new);
                            if let Some(write_errors) = bw.write_errors {
                                for err in write_errors {
                                    let index = n_attempted + err.index;

                                    failure_ref
                                        .write_errors
                                        .get_or_insert_with(Default::default)
                                        .push(BulkWriteError { index, ..err });
                                }
                            }

                            if let Some(wc_error) = bw.write_concern_error {
                                failure_ref.write_concern_error = Some(wc_error);
                            }

                            error_labels.extend(labels);

                            if ordered {
                                // this will always be true since we invoked get_or_insert_with
                                // above.
                                if let Some(failure) = cumulative_failure {
                                    return Err(Error::new(
                                        ErrorKind::BulkWrite(failure),
                                        Some(error_labels),
                                    ));
                                }
                            }
                            n_attempted += current_batch_size;
                        }
                        _ => return Err(e),
                    }
                }
            }
        }

        match cumulative_failure {
            Some(failure) => Err(Error::new(
                ErrorKind::BulkWrite(failure),
                Some(error_labels),
            )),
            None => Ok(cumulative_result.unwrap_or_else(InsertManyResult::new)),
        }
    }
}
