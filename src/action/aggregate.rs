use std::{marker::PhantomData, time::Duration};

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

use super::{action_impl, deeplink, option_setters, CollRef, ExplicitSession, ImplicitSession};

impl Database {
    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
    /// information on aggregations.
    ///
    /// `await` will return d[`Result<Cursor<Document>>`]. If a [`ClientSession`] was provided, the
    /// returned cursor will be a [`SessionCursor`]. If [`with_type`](Aggregate::with_type) was
    /// called, the returned cursor will be generic over the `T` specified.
    #[deeplink]
    pub fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Aggregate {
        Aggregate {
            target: AggregateTargetRef::Database(self),
            pipeline: pipeline.into_iter().collect(),
            options: None,
            session: ImplicitSession,
            _phantom: PhantomData,
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
    /// `await` will return d[`Result<Cursor<Document>>`]. If a [`ClientSession`] was provided, the
    /// returned cursor will be a [`SessionCursor`]. If [`with_type`](Aggregate::with_type) was
    /// called, the returned cursor will be generic over the `T` specified.
    #[deeplink]
    pub fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Aggregate {
        Aggregate {
            target: AggregateTargetRef::Collection(CollRef::new(self)),
            pipeline: pipeline.into_iter().collect(),
            options: None,
            session: ImplicitSession,
            _phantom: PhantomData,
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
    /// [`run`](Aggregate::run) will return d[Result<crate::sync::Cursor<Document>>`]. If a
    /// [`crate::sync::ClientSession`] was provided, the returned cursor will be a
    /// [`crate::sync::SessionCursor`]. If [`with_type`](Aggregate::with_type) was called, the
    /// returned cursor will be generic over the `T` specified.
    #[deeplink]
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
    /// [`run`](Aggregate::run) will return d[Result<crate::sync::Cursor<Document>>`]. If a
    /// `crate::sync::ClientSession` was provided, the returned cursor will be a
    /// `crate::sync::SessionCursor`. If [`with_type`](Aggregate::with_type) was called, the
    /// returned cursor will be generic over the `T` specified.
    #[deeplink]
    pub fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Aggregate {
        self.async_collection.aggregate(pipeline)
    }
}

/// Run an aggregation operation.  Construct with [`Database::aggregate`] or
/// [`Collection::aggregate`].
#[must_use]
pub struct Aggregate<'a, Session = ImplicitSession, T = Document> {
    target: AggregateTargetRef<'a>,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
    session: Session,
    _phantom: PhantomData<T>,
}

impl<'a, Session, T> Aggregate<'a, Session, T> {
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
    /// Use the provided session when running the operation.
    pub fn session(
        self,
        value: impl Into<&'a mut ClientSession>,
    ) -> Aggregate<'a, ExplicitSession<'a>> {
        Aggregate {
            target: self.target,
            pipeline: self.pipeline,
            options: self.options,
            session: ExplicitSession(value.into()),
            _phantom: PhantomData,
        }
    }
}

impl<'a, Session> Aggregate<'a, Session, Document> {
    /// Use the provided type for the returned cursor.
    ///
    /// ```rust
    /// # use futures_util::TryStreamExt;
    /// # use mongodb::{bson::Document, error::Result, Cursor, Database};
    /// # use serde::Deserialize;
    /// # async fn run() -> Result<()> {
    /// # let database: Database = todo!();
    /// # let pipeline: Vec<Document> = todo!();
    /// #[derive(Deserialize)]
    /// struct PipelineOutput {
    ///     len: usize,
    /// }
    ///
    /// let aggregate_cursor = database
    ///     .aggregate(pipeline)
    ///     .with_type::<PipelineOutput>()
    ///     .await?;
    /// let aggregate_results: Vec<PipelineOutput> = aggregate_cursor.try_collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_type<T>(self) -> Aggregate<'a, Session, T> {
        Aggregate {
            target: self.target,
            pipeline: self.pipeline,
            options: self.options,
            session: self.session,
            _phantom: PhantomData,
        }
    }
}

#[action_impl(sync = crate::sync::Cursor<T>)]
impl<'a, T> Action for Aggregate<'a, ImplicitSession, T> {
    type Future = AggregateFuture;

    async fn execute(mut self) -> Result<Cursor<T>> {
        resolve_options!(
            self.target,
            self.options,
            [read_concern, write_concern, selection_criteria]
        );

        let aggregate = crate::operation::aggregate::Aggregate::new(
            self.target.target(),
            self.pipeline,
            self.options,
        );
        let client = self.target.client();
        client.execute_cursor_operation(aggregate).await
    }
}

#[action_impl(sync = crate::sync::SessionCursor<Document>)]
impl<'a> Action for Aggregate<'a, ExplicitSession<'a>> {
    type Future = AggregateSessionFuture;

    async fn execute(mut self) -> Result<SessionCursor<Document>> {
        resolve_read_concern_with_session!(self.target, self.options, Some(&mut *self.session.0))?;
        resolve_write_concern_with_session!(self.target, self.options, Some(&mut *self.session.0))?;
        resolve_selection_criteria_with_session!(
            self.target,
            self.options,
            Some(&mut *self.session.0)
        )?;

        let aggregate = crate::operation::aggregate::Aggregate::new(
            self.target.target(),
            self.pipeline,
            self.options,
        );
        let client = self.target.client();
        client
            .execute_session_cursor_operation(aggregate, self.session.0)
            .await
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
