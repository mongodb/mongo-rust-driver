use std::time::Duration;

use bson::Document;

use crate::{
    coll::options::AggregateOptions,
    error::Result,
    ClientSession,
    Cursor,
    Database,
    SessionCursor,
};

use super::{action_impl, option_setters, ExplicitSession, ImplicitSession};

impl Database {
    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
    /// information on aggregations.
    ///
    /// `await` will return `Result<`[`Cursor`]`<Document>>` or `Result<SessionCursor<Document>>` if
    /// a `ClientSession` is provided.
    pub fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Aggregate {
        Aggregate {
            db: self,
            pipeline: pipeline.into_iter().collect(),
            options: None,
            session: ImplicitSession,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Database {
    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
    /// information on aggregations.
    ///
    /// [`run`](Aggregate::run) will return `Result<`[`Cursor`]`<Document>>` or
    /// `Result<SessionCursor<Document>>` if a `ClientSession` is provided.
    pub fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Aggregate {
        self.async_database.aggregate(pipeline)
    }
}

/// Run an aggregation operation.  Create by calling [`Database::aggregate`].
#[must_use]
pub struct Aggregate<'a, Session = ImplicitSession> {
    db: &'a Database,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
    session: Session,
}

impl<'a, Session> Aggregate<'a, Session> {
    option_setters!(options: AggregateOptions;
        allow_disk_use: bool,
        batch_size: u32,
        bypass_document_validation: bool,
        collation: crate::collation::Collation,
        comment: bson::Bson,
        hint: crate::coll::options::Hint,
        max_await_time: Duration,
        max_time: Duration,
        read_concern: crate::options::ReadConcern,
        selection_criteria: crate::selection_criteria::SelectionCriteria,
        write_concern: crate::options::WriteConcern,
        let_vars: Document,
    );
}

impl<'a> Aggregate<'a, ImplicitSession> {
    /// Runs the operation using the provided session.
    pub fn session(
        self,
        value: impl Into<&'a mut ClientSession>,
    ) -> Aggregate<'a, ExplicitSession<'a>> {
        Aggregate {
            db: self.db,
            pipeline: self.pipeline,
            options: self.options,
            session: ExplicitSession(value.into()),
        }
    }
}

action_impl! {
    impl<'a> Action for Aggregate<'a, ImplicitSession> {
        type Future = AggregateFuture;

        async fn execute(mut self) -> Result<Cursor<Document>> {
            resolve_options!(
                self.db,
                self.options,
                [read_concern, write_concern, selection_criteria]
            );

            let aggregate = crate::operation::aggregate::Aggregate::new(self.db.name().to_string(), self.pipeline, self.options);
            let client = self.db.client();
            client.execute_cursor_operation(aggregate).await
        }

        fn sync_wrap(out) -> Result<crate::sync::Cursor<Document>> {
            out.map(crate::sync::Cursor::new)
        }
    }
}

action_impl! {
    impl<'a> Action for Aggregate<'a, ExplicitSession<'a>> {
        type Future = AggregateSessionFuture;

        async fn execute(mut self) -> Result<SessionCursor<Document>> {
            resolve_options!(
                self.db,
                self.options,
                [read_concern, write_concern, selection_criteria]
            );

            let aggregate = crate::operation::aggregate::Aggregate::new(self.db.name().to_string(), self.pipeline, self.options);
            let client = self.db.client();
            client.execute_session_cursor_operation(aggregate, self.session.0).await
        }

        fn sync_wrap(out) -> Result<crate::sync::SessionCursor<Document>> {
            out.map(crate::sync::SessionCursor::new)
        }
    }
}
