use std::time::Duration;

use bson::Document;

use crate::{
    coll::options::AggregateOptions,
    error::Result,
    operation::aggregate::AggregateTarget,
    options::{ReadConcern, WriteConcern},
    selection_criteria::SelectionCriteria,
    Client,
    ClientSession,
    Collection,
    Cursor,
    Database,
    SessionCursor,
};

use super::{action_impl, option_setters, CollRef, ExplicitSession, ImplicitSession};

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
            target: AggregateTargetRef::Database(self),
            pipeline: pipeline.into_iter().collect(),
            options: None,
            session: ImplicitSession,
        }
    }
}

impl<T> Collection<T>
where
    T: Send + Sync,
{
    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
    /// information on aggregations.
    ///
    /// `await` will return `Result<Cursor<Document>>` or `Result<SessionCursor<Document>>` if
    /// a `ClientSession` is provided.
    pub fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Aggregate {
        Aggregate {
            target: AggregateTargetRef::Collection(CollRef::new(self)),
            pipeline: pipeline.into_iter().collect(),
            options: None,
            session: ImplicitSession,
        }
    }
}

#[cfg(feature = "sync")]
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

#[cfg(feature = "sync")]
impl<T> crate::sync::Collection<T>
where
    T: Send + Sync,
{
    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
    /// information on aggregations.
    ///
    /// [`run`](Aggregate::run) will return `Result<Cursor<Document>>` or
    /// `Result<SessionCursor<Document>>` if a `ClientSession` is provided.
    pub fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Aggregate {
        self.async_collection.aggregate(pipeline)
    }
}

/// Run an aggregation operation.  Create by calling [`Database::aggregate`] or
/// [`Collection::aggregate`].
#[must_use]
pub struct Aggregate<'a, Session = ImplicitSession> {
    target: AggregateTargetRef<'a>,
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
        read_concern: ReadConcern,
        selection_criteria: SelectionCriteria,
        write_concern: WriteConcern,
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
            target: self.target,
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
                self.target,
                self.options,
                [read_concern, write_concern, selection_criteria]
            );

            let aggregate = crate::operation::aggregate::Aggregate::new(self.target.target(), self.pipeline, self.options);
            let client = self.target.client();
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
            resolve_read_concern_with_session!(self.target, self.options, Some(&mut *self.session.0))?;
            resolve_write_concern_with_session!(self.target, self.options, Some(&mut *self.session.0))?;
            resolve_selection_criteria_with_session!(self.target, self.options, Some(&mut *self.session.0))?;

            let aggregate = crate::operation::aggregate::Aggregate::new(self.target.target(), self.pipeline, self.options);
            let client = self.target.client();
            client.execute_session_cursor_operation(aggregate, self.session.0).await
        }

        fn sync_wrap(out) -> Result<crate::sync::SessionCursor<Document>> {
            out.map(crate::sync::SessionCursor::new)
        }
    }
}

enum AggregateTargetRef<'a> {
    Database(&'a Database),
    Collection(CollRef<'a>),
}

impl<'a> AggregateTargetRef<'a> {
    fn target(&self) -> AggregateTarget {
        match self {
            Self::Collection(cr) => AggregateTarget::Collection(cr.namespace()),
            Self::Database(db) => AggregateTarget::Database(db.name().to_string()),
        }
    }

    fn client(&self) -> &Client {
        match self {
            Self::Collection(cr) => cr.client(),
            Self::Database(db) => db.client(),
        }
    }

    fn read_concern(&self) -> Option<&ReadConcern> {
        match self {
            Self::Collection(cr) => cr.read_concern(),
            Self::Database(db) => db.read_concern(),
        }
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        match self {
            Self::Collection(cr) => cr.write_concern(),
            Self::Database(db) => db.write_concern(),
        }
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        match self {
            Self::Collection(cr) => cr.selection_criteria(),
            Self::Database(db) => db.selection_criteria(),
        }
    }
}
