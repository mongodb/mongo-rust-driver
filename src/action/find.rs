use std::time::Duration;

use bson::{Bson, Document};
use serde::de::DeserializeOwned;

use crate::{
    coll::options::{CursorType, FindOneOptions, FindOptions, Hint},
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

use super::{action_impl, deeplink, option_setters, ExplicitSession, ImplicitSession};

impl<T: Send + Sync> Collection<T> {
    /// Finds the documents in the collection matching `filter`.
    ///
    /// `await` will return d[`Result<Cursor<T>>`] (or d[`Result<SessionCursor<T>>`] if a session is
    /// provided).
    #[deeplink]
    pub fn find(&self, filter: Document) -> Find<'_, T> {
        Find {
            coll: self,
            filter,
            options: None,
            session: ImplicitSession,
        }
    }
}

impl<T: DeserializeOwned + Send + Sync> Collection<T> {
    /// Finds a single document in the collection matching `filter`.
    ///
    /// `await` will return d[`Result<Option<T>>`].
    #[deeplink]
    pub fn find_one(&self, filter: Document) -> FindOne<'_, T> {
        FindOne {
            coll: self,
            filter,
            options: None,
            session: None,
        }
    }
}

#[cfg(feature = "sync")]
impl<T: Send + Sync> crate::sync::Collection<T> {
    /// Finds the documents in the collection matching `filter`.
    ///
    /// [`run`](Find::run) will return d[`Result<crate::sync::Cursor<T>>`] (or
    /// d[`Result<crate::sync::SessionCursor<T>>`] if a session is provided).
    #[deeplink]
    pub fn find(&self, filter: Document) -> Find<'_, T> {
        self.async_collection.find(filter)
    }
}

#[cfg(feature = "sync")]
impl<T: DeserializeOwned + Send + Sync> crate::sync::Collection<T> {
    /// Finds a single document in the collection matching `filter`.
    ///
    /// [`run`](Find::run) will return d[`Result<Option<T>>`].
    #[deeplink]
    pub fn find_one(&self, filter: Document) -> FindOne<'_, T> {
        self.async_collection.find_one(filter)
    }
}

/// Finds the documents in a collection matching a filter.  Construct with [`Collection::find`].
#[must_use]
pub struct Find<'a, T: Send + Sync, Session = ImplicitSession> {
    coll: &'a Collection<T>,
    filter: Document,
    options: Option<FindOptions>,
    session: Session,
}

impl<'a, T: Send + Sync, Session> Find<'a, T, Session> {
    option_setters!(options: FindOptions;
        allow_disk_use: bool,
        allow_partial_results: bool,
        batch_size: u32,
        comment: Bson,
        cursor_type: CursorType,
        hint: Hint,
        limit: i64,
        max: Document,
        max_await_time: Duration,
        max_scan: u64,
        max_time: Duration,
        min: Document,
        no_cursor_timeout: bool,
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

    /// Use the provided session when running the operation.
    pub fn session<'s>(
        self,
        value: impl Into<&'s mut ClientSession>,
    ) -> Find<'a, T, ExplicitSession<'s>> {
        Find {
            coll: self.coll,
            filter: self.filter,
            options: self.options,
            session: ExplicitSession(value.into()),
        }
    }
}

#[action_impl(sync = crate::sync::Cursor<T>)]
impl<'a, T: Send + Sync> Action for Find<'a, T, ImplicitSession> {
    type Future = FindFuture;

    async fn execute(mut self) -> Result<Cursor<T>> {
        resolve_options!(self.coll, self.options, [read_concern, selection_criteria]);

        let find = Op::new(self.coll.namespace(), self.filter, self.options);
        self.coll.client().execute_cursor_operation(find).await
    }
}

#[action_impl(sync = crate::sync::SessionCursor<T>)]
impl<'a, T: Send + Sync> Action for Find<'a, T, ExplicitSession<'a>> {
    type Future = FindSessionFuture;

    async fn execute(mut self) -> Result<SessionCursor<T>> {
        resolve_read_concern_with_session!(self.coll, self.options, Some(&mut *self.session.0))?;
        resolve_selection_criteria_with_session!(
            self.coll,
            self.options,
            Some(&mut *self.session.0)
        )?;

        let find = Op::new(self.coll.namespace(), self.filter, self.options);
        self.coll
            .client()
            .execute_session_cursor_operation(find, self.session.0)
            .await
    }
}

/// Finds a single document in a collection matching a filter.  Construct with
/// [`Collection::find_one`].
#[must_use]
pub struct FindOne<'a, T: Send + Sync> {
    coll: &'a Collection<T>,
    filter: Document,
    options: Option<FindOneOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a, T: Send + Sync> FindOne<'a, T> {
    option_setters! { options: FindOneOptions;
        allow_partial_results: bool,
        collation: Collation,
        comment: Bson,
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
        let_vars: Document,
    }

    /// Use the provided session when running the operation.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

#[action_impl]
impl<'a, T: DeserializeOwned + Send + Sync> Action for FindOne<'a, T> {
    type Future = FindOneFuture;

    async fn execute(self) -> Result<Option<T>> {
        use futures_util::stream::StreamExt;
        let mut options: FindOptions = self.options.unwrap_or_default().into();
        options.limit = Some(-1);
        let find = self.coll.find(self.filter).with_options(options);
        if let Some(session) = self.session {
            let mut cursor = find.session(&mut *session).await?;
            let mut stream = cursor.stream(session);
            stream.next().await.transpose()
        } else {
            let mut cursor = find.await?;
            cursor.next().await.transpose()
        }
    }
}
