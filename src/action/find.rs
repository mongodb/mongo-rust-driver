use std::{marker::PhantomData, time::Duration};

use bson::{Bson, Document};
use serde::de::DeserializeOwned;

use crate::{
    coll::options::{CursorType, FindOptions, Hint},
    collation::Collation,
    error::Result,
    operation::Find as Op,
    options::ReadConcern,
    selection_criteria::SelectionCriteria,
    ClientSession,
    Collection,
    Cursor,
    SessionCursor,
};

use super::{action_impl, option_setters, ExplicitSession, ImplicitSession, Multiple, Single};

impl<T: Send + Sync> Collection<T> {
    /// Finds the documents in the collection matching `filter`.
    ///
    /// `await` will return `Result<Cursor<T>>` (or `Result<SessionCursor<T>>` if a session is
    /// provided).
    pub fn find(&self, filter: Document) -> Find<'_, T> {
        Find {
            coll: self,
            filter,
            options: None,
            session: ImplicitSession,
            _mode: PhantomData,
        }
    }
}

impl<T: DeserializeOwned + Send + Sync> Collection<T> {
    /// Finds a single document in the collection matching `filter`.
    ///
    /// `await` will return `Result<Option<T>>`.
    pub fn find_one(&self, filter: Document) -> Find<'_, T, Single> {
        Find {
            coll: self,
            filter,
            options: None,
            session: ImplicitSession,
            _mode: PhantomData,
        }
    }
}

#[cfg(feature = "sync")]
impl<T: Send + Sync> crate::sync::Collection<T> {
    /// Finds the documents in the collection matching `filter`.
    ///
    /// [`run`](Find::run) will return `Result<Cursor<T>>` (or `Result<SessionCursor<T>>` if a
    /// session is provided).
    pub fn find(&self, filter: Document) -> Find<'_, T> {
        self.async_collection.find(filter)
    }
}

#[cfg(feature = "sync")]
impl<T: DeserializeOwned + Send + Sync> crate::sync::Collection<T> {
    /// Finds a single document in the collection matching `filter`.
    ///
    /// [`run`](Find::run) will return `Result<Option<T>>`.
    pub fn find_one(&self, filter: Document) -> Find<'_, T, Single> {
        self.async_collection.find_one(filter)
    }
}

/// Finds the documents in a collection matching a filter.  Construct with [`Collection::find`] or
/// [`Collection::find_one`].
#[must_use]
pub struct Find<'a, T: Send + Sync, Mode = Multiple, Session = ImplicitSession> {
    coll: &'a Collection<T>,
    filter: Document,
    options: Option<FindOptions>,
    session: Session,
    _mode: PhantomData<Mode>,
}

impl<'a, T: Send + Sync, Mode, Session> Find<'a, T, Mode, Session> {
    option_setters!(options: FindOptions;
        allow_partial_results: bool,
        comment: String,
        comment_bson: Bson,
        hint: Hint,
        max: Document,
        max_scan: u64,
        max_time: Duration,
        min: Document,
        projection: Document,
        read_concern: ReadConcern,
        return_key: bool,
        selection_criteria: SelectionCriteria,
        show_record_id: bool,
        skip: u64,
        sort: Document,
        collation: Collation,
        let_vars: Document,
    );
}

// Some options don't make sense for `find_one`.
impl<'a, T: Send + Sync, Session> Find<'a, T, Multiple, Session> {
    option_setters!(FindOptions;
        allow_disk_use: bool,
        batch_size: u32,
        cursor_type: CursorType,
        limit: i64,
        max_await_time: Duration,
        no_cursor_timeout: bool,
    );
}

impl<'a, T: Send + Sync, Mode> Find<'a, T, Mode, ImplicitSession> {
    /// Runs the query using the provided session.
    pub fn session<'s>(
        self,
        value: impl Into<&'s mut ClientSession>,
    ) -> Find<'a, T, Mode, ExplicitSession<'s>> {
        Find {
            coll: self.coll,
            filter: self.filter,
            options: self.options,
            session: ExplicitSession(value.into()),
            _mode: PhantomData,
        }
    }
}

action_impl! {
    impl<'a, T: Send + Sync> Action for Find<'a, T, Multiple, ImplicitSession> {
        type Future = FindFuture;

        async fn execute(mut self) -> Result<Cursor<T>> {
            resolve_options!(self.coll, self.options, [read_concern, selection_criteria]);

            let find = Op::new(self.coll.namespace(), self.filter, self.options);
            self.coll.client().execute_cursor_operation(find).await
        }

        fn sync_wrap(out) -> Result<crate::sync::Cursor<T>> {
            out.map(crate::sync::Cursor::new)
        }
    }
}

action_impl! {
    impl<'a, T: Send + Sync> Action for Find<'a, T, Multiple, ExplicitSession<'a>> {
        type Future = FindSessionFuture;

        async fn execute(mut self) -> Result<SessionCursor<T>> {
            resolve_read_concern_with_session!(self.coll, self.options, Some(&mut *self.session.0))?;
            resolve_selection_criteria_with_session!(self.coll, self.options, Some(&mut *self.session.0))?;

            let find = Op::new(self.coll.namespace(), self.filter, self.options);
            self.coll.client().execute_session_cursor_operation(find, self.session.0).await
        }

        fn sync_wrap(out) -> Result<crate::sync::SessionCursor<T>> {
            out.map(crate::sync::SessionCursor::new)
        }
    }
}

action_impl! {
    impl<'a, T: DeserializeOwned + Send + Sync> Action for Find<'a, T, Single, ImplicitSession>
    {
        type Future = FindOneFuture;

        async fn execute(self) -> Result<Option<T>> {
            use futures_util::stream::StreamExt;
            let mut options = self.options.unwrap_or_default();
            options.limit = Some(-1);
            let mut cursor = self.coll.find(self.filter).with_options(options).await?;
            cursor.next().await.transpose()
        }
    }
}

action_impl! {
    impl<'a, T: DeserializeOwned + Send + Sync> Action for Find<'a, T, Single, ExplicitSession<'a>> {
        type Future = FindOneSessionFuture;

        async fn execute(self) -> Result<Option<T>> {
            use futures_util::stream::StreamExt;
            let mut options = self.options.unwrap_or_default();
            options.limit = Some(-1);
            let mut cursor = self.coll
                .find(self.filter)
                .with_options(options)
                .session(&mut *self.session.0)
                .await?;
            let mut stream = cursor.stream(self.session.0);
            stream.next().await.transpose()
        }
    }
}
