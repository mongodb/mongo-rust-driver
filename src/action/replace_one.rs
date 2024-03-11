use std::borrow::Borrow;

use bson::{Bson, Document, RawDocumentBuf};
use serde::Serialize;

use crate::{
    coll::options::{Hint, ReplaceOptions, UpdateOptions},
    collation::Collation,
    error::Result,
    operation::Update as Op,
    options::WriteConcern,
    results::UpdateResult,
    serde_util,
    ClientSession,
    Collection,
};

use super::{action_impl, option_setters, CollRef};

impl<T: Serialize + Send + Sync> Collection<T> {
    /// Replaces up to one document matching `query` in the collection with `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return `Result<UpdateResult>`.
    pub fn replace_one(&self, query: Document, replacement: impl Borrow<T>) -> ReplaceOne {
        ReplaceOne {
            coll: CollRef::new(self),
            query,
            replacement: serde_util::to_raw_document_buf_with_options(
                replacement.borrow(),
                self.human_readable_serialization(),
            ),
            options: None,
            session: None,
        }
    }
}

#[cfg(feature = "sync")]
impl<T: Serialize + Send + Sync> crate::sync::Collection<T> {
    /// Replaces up to one document matching `query` in the collection with `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// [`run`](ReplaceOne::run) will return `Result<UpdateResult>`.
    pub fn replace_one(&self, query: Document, replacement: impl Borrow<T>) -> ReplaceOne {
        self.async_collection.replace_one(query, replacement)
    }
}

/// Replace up to one document matching a query.  Construct with [`Collection::replace_one`].
#[must_use]
pub struct ReplaceOne<'a> {
    coll: CollRef<'a>,
    query: Document,
    replacement: Result<RawDocumentBuf>,
    options: Option<ReplaceOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> ReplaceOne<'a> {
    option_setters! { options: ReplaceOptions;
        bypass_document_validation: bool,
        upsert: bool,
        collation: Collation,
        hint: Hint,
        write_concern: WriteConcern,
        let_vars: Document,
        comment: Bson,
    }

    /// Use the provided session when running the operation.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
    impl<'a> Action for ReplaceOne<'a> {
        type Future = ReplaceOneFuture;

        async fn execute(mut self) -> Result<UpdateResult> {
            resolve_write_concern_with_session!(self.coll, self.options, self.session.as_ref())?;

            let update = Op::with_replace_raw(
                self.coll.namespace(),
                self.query,
                self.replacement?,
                false,
                self.options.map(UpdateOptions::from_replace_options),
            )?;
            self.coll.client().execute_operation(update, self.session).await
        }
    }
}
