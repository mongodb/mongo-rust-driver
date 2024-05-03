use std::{borrow::Borrow, ops::Deref};

use bson::{Bson, RawDocumentBuf};
use serde::Serialize;

use crate::{
    coll::options::{InsertManyOptions, InsertOneOptions},
    error::{convert_bulk_errors, Result},
    operation::Insert as Op,
    options::WriteConcern,
    results::InsertOneResult,
    ClientSession,
    Collection,
};

use super::{action_impl, deeplink, option_setters, CollRef};

impl<T: Serialize + Send + Sync> Collection<T> {
    /// Inserts `doc` into the collection.
    ///
    /// Note that either an owned or borrowed value can be inserted here, so the input document
    /// does not need to be cloned to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return d[`Result<InsertOneResult>`].
    #[deeplink]
    pub fn insert_one(&self, doc: impl Borrow<T>) -> InsertOne {
        InsertOne {
            coll: CollRef::new(self),
            doc: bson::to_raw_document_buf(doc.borrow()).map_err(Into::into),
            options: None,
            session: None,
        }
    }
}

#[cfg(feature = "sync")]
impl<T: Serialize + Send + Sync> crate::sync::Collection<T> {
    /// Inserts `doc` into the collection.
    ///
    /// Note that either an owned or borrowed value can be inserted here, so the input document
    /// does not need to be cloned to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// [`run`](InsertOne::run) will return d[`Result<InsertOneResult>`].
    #[deeplink]
    pub fn insert_one(&self, doc: impl Borrow<T>) -> InsertOne {
        self.async_collection.insert_one(doc)
    }
}

/// Inserts a document into a collection.  Construct with ['Collection::insert_one`].
#[must_use]
pub struct InsertOne<'a> {
    coll: CollRef<'a>,
    doc: Result<RawDocumentBuf>,
    options: Option<InsertOneOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> InsertOne<'a> {
    option_setters! { options: InsertOneOptions;
        bypass_document_validation: bool,
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
impl<'a> Action for InsertOne<'a> {
    type Future = InsertOneFuture;

    async fn execute(mut self) -> Result<InsertOneResult> {
        resolve_write_concern_with_session!(self.coll, self.options, self.session.as_ref())?;

        let doc = self.doc?;

        let insert = Op::new(
            self.coll.namespace(),
            vec![doc.deref()],
            self.options.map(InsertManyOptions::from_insert_one_options),
            self.coll.client().should_auto_encrypt().await,
        );
        self.coll
            .client()
            .execute_operation(insert, self.session)
            .await
            .map(InsertOneResult::from_insert_many_result)
            .map_err(convert_bulk_errors)
    }
}
