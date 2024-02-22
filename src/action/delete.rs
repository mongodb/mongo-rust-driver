use bson::{Bson, Document};

use crate::{
    coll::options::{DeleteOptions, Hint},
    collation::Collation,
    error::Result,
    operation::Delete as Op,
    options::WriteConcern,
    results::DeleteResult,
    ClientSession,
    Collection,
};

use super::{action_impl, option_setters, CollRef};

impl<T> Collection<T> {
    /// Deletes up to one document found matching `query`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return `Result<DeleteResult>`.
    pub fn delete_one_2(&self, query: Document) -> Delete {
        Delete {
            coll: CollRef::new(self),
            query,
            options: None,
            session: None,
            limit: Some(1),
        }
    }

    /// Deletes all documents stored in the collection matching `query`.
    ///
    /// `await` will return `Result<DeleteResult>`.
    pub fn delete_many_2(&self, query: Document) -> Delete {
        Delete {
            coll: CollRef::new(self),
            query,
            options: None,
            session: None,
            limit: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<T> crate::sync::Collection<T> {
    /// Deletes up to one document found matching `query`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// [`run`](Delete::run) will return `Result<DeleteResult>`.
    pub fn delete_one_2(&self, query: Document) -> Delete {
        self.async_collection.delete_one_2(query)
    }

    /// Deletes all documents stored in the collection matching `query`.
    ///
    /// [`run`](Delete::run) will return `Result<DeleteResult>`.
    pub fn delete_many_2(&self, query: Document) -> Delete {
        self.async_collection.delete_many_2(query)
    }
}

/// Deletes documents matching a query.  Construct with [`Collection::delete_one`] or
/// [`Collection::delete_many`].
#[must_use]
pub struct Delete<'a> {
    coll: CollRef<'a>,
    query: Document,
    options: Option<DeleteOptions>,
    session: Option<&'a mut ClientSession>,
    limit: Option<u32>,
}

impl<'a> Delete<'a> {
    option_setters!(options: DeleteOptions;
        collation: Collation,
        write_concern: WriteConcern,
        hint: Hint,
        let_vars: Document,
        comment: Bson,
    );

    /// Runs the operation using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
    impl<'a> Action for Delete<'a> {
        type Future = DeleteFuture;

        async fn execute(mut self) -> Result<DeleteResult> {
            resolve_write_concern_with_session!(self.coll.inner, self.options, self.session.as_ref())?;

            let op = Op::new(self.coll.inner.namespace(), self.query, self.limit, self.options);
            self.coll.inner.client().execute_operation(op, self.session).await
        }
    }
}
