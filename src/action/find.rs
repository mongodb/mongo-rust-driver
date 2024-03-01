use std::time::Duration;

use bson::{Bson, Document};

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
};

use super::{action_impl, option_setters, ExplicitSession, ImplicitSession};

impl<T> Collection<T> {
    /// Finds the documents in the collection matching `filter`.
    pub fn find_2(&self, filter: Document) -> Find<'_, T> {
        Find {
            coll: self,
            filter,
            options: None,
            session: ImplicitSession,
        }
    }
}

#[cfg(feature = "sync")]
impl<T> crate::sync::Collection<T> {
    /// Finds the documents in the collection matching `filter`.
    pub fn find_2(&self, filter: Document) -> Find<'_, T> {
        self.async_collection.find_2(filter)
    }
}

#[must_use]
pub struct Find<'a, T, Session = ImplicitSession> {
    coll: &'a Collection<T>,
    filter: Document,
    options: Option<FindOptions>,
    session: Session,
}

impl<'a, T, Session> Find<'a, T, Session> {
    option_setters!(options: FindOptions;
        allow_disk_use: bool,
        allow_partial_results: bool,
        batch_size: u32,
        comment: String,
        comment_bson: Bson,
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
}

impl<'a, T> Find<'a, T, ImplicitSession> {
    /// Runs the query using the provided session.
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

action_impl! {
    impl<'a, T> Action for Find<'a, T, ImplicitSession> {
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
