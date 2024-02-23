use std::marker::PhantomData;
use std::time::Duration;

use bson::Bson;
use futures_util::stream::TryStreamExt;

use crate::{ClientSession, Cursor, IndexModel, SessionCursor};
use crate::{coll::options::ListIndexesOptions, Collection};
use crate::operation::ListIndexes as Op;
use crate::error::Result;

use super::{CollRef, ExplicitSession, ImplicitSession, ListNames, ListSpecifications};
use super::{action_impl, option_setters};

impl<T> Collection<T> {
    /// Lists all indexes on this collection.
    ///
    /// `await` will return `Result<Cursor<IndexModel>>` (or `Result<SessionCursor<IndexModel>>` if a `ClientSession` is provided).
    pub fn list_indexes_2(&self) -> ListIndexes {
        ListIndexes {
            coll: CollRef::new(self),
            options: None,
            session: ImplicitSession,
            _mode: PhantomData,
        }
    }

    /// Gets the names of all indexes on the collection.
    ///
    /// `await` will return `Result<Vec<String>>`.
    pub fn list_index_names_2(&self) -> ListIndexes<ListNames> {
        ListIndexes {
            coll: CollRef::new(self),
            options: None,
            session: ImplicitSession,
            _mode: PhantomData,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<T> crate::sync::Collection<T> {
    /// Lists all indexes on this collection.
    ///
    /// [`run`](ListIndexes::run) will return `Result<Cursor<IndexModel>>` (or `Result<SessionCursor<IndexModel>>` if a `ClientSession` is provided).
    pub fn list_indexes_2(&self) -> ListIndexes {
        self.async_collection.list_indexes_2()
    }

    /// Gets the names of all indexes on the collection.
    ///
    /// [`run`](ListIndexes::run) will return `Result<Vec<String>>`.
    pub fn list_index_names_2(&self) -> ListIndexes<ListNames> {
        self.async_collection.list_index_names_2()
    }
}

/// List indexes on a collection.  Construct with [`Collection::list_indexes`] or [`Collection::list_index_names`].
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
    /// Runs the operation using the provided session.
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

action_impl! {
    impl<'a> Action for ListIndexes<'a, ListSpecifications, ImplicitSession> {
        type Future = ListIndexesFuture;

        async fn execute(self) -> Result<Cursor<IndexModel>> {
            let op = Op::new(self.coll.namespace(), self.options);
            self.coll.client().execute_cursor_operation(op).await
        }

        fn sync_wrap(out) -> Result<crate::sync::Cursor<IndexModel>> {
            out.map(crate::sync::Cursor::new)
        }
    }
}

action_impl! {
    impl<'a> Action for ListIndexes<'a, ListSpecifications, ExplicitSession<'a>> {
        type Future = ListIndexesSessionFuture;

        async fn execute(self) -> Result<SessionCursor<IndexModel>> {
            let op = Op::new(self.coll.namespace(), self.options);
            self.coll.client().execute_session_cursor_operation(op, self.session.0).await
        }

        fn sync_wrap(out) -> Result<crate::sync::SessionCursor<IndexModel>> {
            out.map(crate::sync::SessionCursor::new)
        }
    }
}

action_impl! {
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
}

action_impl! {
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
}