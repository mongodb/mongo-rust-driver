use std::{marker::PhantomData, time::Duration};

use bson::Bson;
use futures_util::stream::TryStreamExt;

use crate::{
    coll::options::ListIndexesOptions,
    error::Result,
    operation::ListIndexes as Op,
    ClientSession,
    Collection,
    Cursor,
    IndexModel,
    SessionCursor,
};

use super::{
    action_impl,
    deeplink,
    option_setters,
    CollRef,
    ExplicitSession,
    ImplicitSession,
    ListNames,
    ListSpecifications,
};

impl<T> Collection<T>
where
    T: Send + Sync,
{
    /// Lists all indexes on this collection.
    ///
    /// `await` will return d[`Result<Cursor<IndexModel>>`] (or
    /// d[`Result<SessionCursor<IndexModel>>`] if a `ClientSession` is provided).
    #[deeplink]
    pub fn list_indexes(&self) -> ListIndexes {
        ListIndexes {
            coll: CollRef::new(self),
            options: None,
            session: ImplicitSession,
            _mode: PhantomData,
        }
    }

    /// Gets the names of all indexes on the collection.
    ///
    /// `await` will return d[`Result<Vec<String>>`].
    #[deeplink]
    pub fn list_index_names(&self) -> ListIndexes<ListNames> {
        ListIndexes {
            coll: CollRef::new(self),
            options: None,
            session: ImplicitSession,
            _mode: PhantomData,
        }
    }
}

#[cfg(feature = "sync")]
impl<T> crate::sync::Collection<T>
where
    T: Send + Sync,
{
    /// Lists all indexes on this collection.
    ///
    /// [`run`](ListIndexes::run) will return d[`Result<crate::sync::Cursor<IndexModel>>`] (or
    /// d[`Result<crate::sync::SessionCursor<IndexModel>>`] if a `ClientSession` is provided).
    #[deeplink]
    pub fn list_indexes(&self) -> ListIndexes {
        self.async_collection.list_indexes()
    }

    /// Gets the names of all indexes on the collection.
    ///
    /// [`run`](ListIndexes::run) will return d[`Result<Vec<String>>`].
    #[deeplink]
    pub fn list_index_names(&self) -> ListIndexes<ListNames> {
        self.async_collection.list_index_names()
    }
}

/// List indexes on a collection.  Construct with [`Collection::list_indexes`] or
/// [`Collection::list_index_names`].
#[must_use]
pub struct ListIndexes<'a, Mode = ListSpecifications, Session = ImplicitSession> {
    coll: CollRef<'a>,
    options: Option<ListIndexesOptions>,
    session: Session,
    _mode: PhantomData<Mode>,
}

impl<'a, Mode, Session> ListIndexes<'a, Mode, Session> {
    option_setters!(options: ListIndexesOptions;
        max_time: Duration,
        batch_size: u32,
        comment: Bson,
    );
}

impl<'a, Mode> ListIndexes<'a, Mode, ImplicitSession> {
    /// Use the provided session when running the operation.
    pub fn session(
        self,
        value: impl Into<&'a mut ClientSession>,
    ) -> ListIndexes<'a, Mode, ExplicitSession<'a>> {
        ListIndexes {
            coll: self.coll,
            options: self.options,
            session: ExplicitSession(value.into()),
            _mode: PhantomData,
        }
    }
}

#[action_impl(sync = crate::sync::Cursor<IndexModel>)]
impl<'a> Action for ListIndexes<'a, ListSpecifications, ImplicitSession> {
    type Future = ListIndexesFuture;

    async fn execute(self) -> Result<Cursor<IndexModel>> {
        let op = Op::new(self.coll.namespace(), self.options);
        self.coll.client().execute_cursor_operation(op).await
    }
}

#[action_impl(sync = crate::sync::SessionCursor<IndexModel>)]
impl<'a> Action for ListIndexes<'a, ListSpecifications, ExplicitSession<'a>> {
    type Future = ListIndexesSessionFuture;

    async fn execute(self) -> Result<SessionCursor<IndexModel>> {
        let op = Op::new(self.coll.namespace(), self.options);
        self.coll
            .client()
            .execute_session_cursor_operation(op, self.session.0)
            .await
    }
}

#[action_impl]
impl<'a> Action for ListIndexes<'a, ListNames, ImplicitSession> {
    type Future = ListIndexNamesFuture;

    async fn execute(self) -> Result<Vec<String>> {
        let inner = ListIndexes {
            coll: self.coll,
            options: self.options,
            session: self.session,
            _mode: PhantomData::<ListSpecifications>,
        };
        let cursor = inner.await?;
        cursor
            .try_filter_map(|index| futures_util::future::ok(index.get_name()))
            .try_collect()
            .await
    }
}

#[action_impl]
impl<'a> Action for ListIndexes<'a, ListNames, ExplicitSession<'a>> {
    type Future = ListIndexNamesSessionFuture;

    async fn execute(self) -> Result<Vec<String>> {
        let session = self.session.0;
        let inner = ListIndexes {
            coll: self.coll,
            options: self.options,
            session: ExplicitSession(&mut *session),
            _mode: PhantomData::<ListSpecifications>,
        };
        let mut cursor = inner.await?;
        let stream = cursor.stream(session);
        stream
            .try_filter_map(|index| futures_util::future::ok(index.get_name()))
            .try_collect()
            .await
    }
}
