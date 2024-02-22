use std::{marker::PhantomData, time::Duration};

use bson::Bson;

use crate::{
    coll::options::{CommitQuorum, CreateIndexOptions},
    error::Result,
    operation::CreateIndexes as Op,
    options::WriteConcern,
    results::{CreateIndexResult, CreateIndexesResult},
    ClientSession,
    Collection,
    IndexModel,
};

use super::{action_impl, option_setters, CollRef, Multiple, Single};

impl<T> Collection<T> {
    /// Creates the given index on this collection.
    ///
    /// `await` will return `Result<CreateIndexResult>`.
    pub fn create_index(&self, index: IndexModel) -> CreateIndex {
        CreateIndex {
            coll: CollRef::new(self),
            indexes: vec![index],
            options: None,
            session: None,
            _mode: PhantomData,
        }
    }

    /// Creates the given indexes on this collection.
    ///
    /// `await` will return `Result<CreateIndexesResult>`.
    pub fn create_indexes(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
    ) -> CreateIndex<'_, Multiple> {
        CreateIndex {
            coll: CollRef::new(self),
            indexes: indexes.into_iter().collect(),
            options: None,
            session: None,
            _mode: PhantomData,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<T> crate::sync::Collection<T> {
    /// Creates the given index on this collection.
    ///
    /// [`run`](CreateIndex::run) will return `Result<CreateIndexResult>`.
    pub fn create_index(&self, index: IndexModel) -> CreateIndex {
        self.async_collection.create_index(index)
    }

    /// Creates the given indexes on this collection.
    ///
    /// [`run`](CreateIndex::run) will return `Result<CreateIndexesResult>`.
    pub fn create_indexes(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
    ) -> CreateIndex<'_, Multiple> {
        self.async_collection.create_indexes(indexes)
    }
}

/// Perform creation of an index or indexes.  Construct by calling [`Collection::create_index`] or
/// [`Collection::create_indexes`].
#[must_use]
pub struct CreateIndex<'a, M = Single> {
    coll: CollRef<'a>,
    indexes: Vec<IndexModel>,
    options: Option<CreateIndexOptions>,
    session: Option<&'a mut ClientSession>,
    _mode: PhantomData<M>,
}

impl<'a, M> CreateIndex<'a, M> {
    option_setters!(options: CreateIndexOptions;
        commit_quorum: CommitQuorum,
        max_time: Duration,
        write_concern: WriteConcern,
        comment: Bson,
    );

    /// Runs the operation using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
    impl<'a> Action for CreateIndex<'a, Single> {
        type Future = CreateIndexFuture;

        async fn execute(self) -> Result<CreateIndexResult> {
            let inner: CreateIndex<'a, Multiple> = CreateIndex {
                coll: self.coll,
                indexes: self.indexes,
                options: self.options,
                session: self.session,
                _mode: PhantomData,
            };
            let response = inner.await?;
            Ok(response.into_create_index_result())
        }
    }
}

action_impl! {
    impl<'a> Action for CreateIndex<'a, Multiple> {
        type Future = CreateIndexesFuture;

        async fn execute(mut self) -> Result<CreateIndexesResult> {
            resolve_write_concern_with_session!(self.coll.inner, self.options, self.session.as_ref())?;

            let op = Op::new(self.coll.inner.namespace(), self.indexes, self.options);
            self.coll.inner.client().execute_operation(op, self.session).await
        }
    }
}
