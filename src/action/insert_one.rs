use std::{borrow::Borrow, ops::Deref};

use bson::{Bson, RawDocumentBuf};
use serde::Serialize;

use crate::{
    coll::options::{InsertManyOptions, InsertOneOptions},
    error::{convert_bulk_errors, Result},
    operation::Insert as Op,
    options::WriteConcern,
    results::InsertOneResult,
    serde_util,
    ClientSession,
    Collection,
};

use super::{action_impl, option_setters, CollRef};

impl<T: Serialize> Collection<T> {
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
    /// `await` will return `Result<InsertOneResult>`.
    pub fn insert_one(&self, doc: impl Borrow<T>) -> InsertOne {
        InsertOne {
            coll: CollRef::new(self),
            doc: serde_util::to_raw_document_buf_with_options(
                doc.borrow(),
                self.human_readable_serialization(),
            ),
            options: None,
            session: None,
        }
    }
}

#[cfg(feature = "sync")]
impl<T: Serialize> crate::sync::Collection<T> {
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
    /// [`run`](InsertOne::run) will return `Result<InsertOneResult>`.
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

    /// Runs the operation using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
    impl<'a> Action for InsertOne<'a> {
        type Future = InsertOneFuture;

        async fn execute(mut self) -> Result<InsertOneResult> {
            resolve_write_concern_with_session!(self.coll, self.options, self.session.as_ref())?;

            #[cfg(feature = "in-use-encryption-unstable")]
            let encrypted = self.coll.client().auto_encryption_opts().await.is_some();
            #[cfg(not(feature = "in-use-encryption-unstable"))]
            let encrypted = false;

            let doc = self.doc?;

            let insert = Op::new(
                self.coll.namespace(),
                vec![doc.deref()],
                self.options.map(InsertManyOptions::from_insert_one_options),
                encrypted,
            );
            self.coll.client()
                .execute_operation(insert, self.session)
                .await
                .map(InsertOneResult::from_insert_many_result)
                .map_err(convert_bulk_errors)
        }
    }
}
