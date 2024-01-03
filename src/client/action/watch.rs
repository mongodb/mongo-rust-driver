use std::{future::IntoFuture, time::Duration};

use bson::{Document, Timestamp, Bson};
use futures_util::FutureExt;

use crate::{Client, change_stream::{options::{ChangeStreamOptions, FullDocumentType, FullDocumentBeforeChangeType}, ChangeStream, event::{ChangeStreamEvent, ResumeToken}, session::SessionChangeStream}, ClientSession, error::{Result, ErrorKind}, client::BoxFuture, operation::AggregateTarget, collation::Collation, options::ReadConcern, selection_criteria::SelectionCriteria};

impl Client {
    /// Starts a new [`ChangeStream`] that receives events for all changes in the cluster. The
    /// stream does not observe changes from system collections or the "config", "local" or
    /// "admin" databases. Note that this method (`watch` on a cluster) is only supported in
    /// MongoDB 4.0 or greater.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/changeStreams/) on change
    /// streams.
    ///
    /// Change streams require either a "majority" read concern or no read
    /// concern. Anything else will cause a server error.
    ///
    /// Note that using a `$project` stage to remove any of the `_id` `operationType` or `ns` fields
    /// will cause an error. The driver requires these fields to support resumability. For
    /// more information on resumability, see the documentation for
    /// [`ChangeStream`](change_stream/struct.ChangeStream.html)
    ///
    /// If the pipeline alters the structure of the returned events, the parsed type will need to be
    /// changed via [`ChangeStream::with_type`].
    pub fn watch_2(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
    ) -> Watch {
        Watch {
            client: &self,
            pipeline: pipeline.into_iter().collect(),
            options: None,
            session: Implicit,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Client {
    /// Starts a new [`ChangeStream`] that receives events for all changes in the cluster. The
    /// stream does not observe changes from system collections or the "config", "local" or
    /// "admin" databases. Note that this method (`watch` on a cluster) is only supported in
    /// MongoDB 4.0 or greater.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/changeStreams/) on change
    /// streams.
    ///
    /// Change streams require either a "majority" read concern or no read
    /// concern. Anything else will cause a server error.
    ///
    /// Note that using a `$project` stage to remove any of the `_id` `operationType` or `ns` fields
    /// will cause an error. The driver requires these fields to support resumability. For
    /// more information on resumability, see the documentation for
    /// [`ChangeStream`](change_stream/struct.ChangeStream.html)
    ///
    /// If the pipeline alters the structure of the returned events, the parsed type will need to be
    /// changed via [`ChangeStream::with_type`].
    pub fn watch_2(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
    ) -> Watch {
        self.async_client.watch_2(pipeline)
    }
}

mod private {
    pub trait Sealed {}
    impl Sealed for super::Implicit {}
    impl<'a> Sealed for super::Explicit<'a> {}
}

pub trait Session: private::Sealed {}
impl Session for Implicit {}
impl<'a> Session for Explicit<'a> {}

pub struct Implicit;
pub struct Explicit<'a>(&'a mut ClientSession);

/// Starts a new [`ChangeStream`] that receives events for all changes in the cluster.
#[must_use]
pub struct Watch<'a, S: Session = Implicit> {
    client: &'a Client,
    pipeline: Vec<Document>,
    options: Option<ChangeStreamOptions>,
    session: S,
}

macro_rules! option_setters {
    (
        $opt_field:ident: $opt_field_ty:ty;
        $(
            $(#[$($attrss:tt)*])*
            $opt_name:ident: $opt_ty:ty,
        )+
    ) => {
        fn options(&mut self) -> &mut $opt_field_ty {
            self.$opt_field.get_or_insert_with(<$opt_field_ty>::default)
        }

        $(
            $(#[$($attrss)*])*
            pub fn $opt_name(mut self, value: $opt_ty) -> Self {
                self.options().$opt_name = Some(value);
                self
            }
        )+
    };
}

impl<'a, S: Session> Watch<'a, S> {
    option_setters!(options: ChangeStreamOptions;
        /// Configures how the
        /// [`ChangeStreamEvent::full_document`](crate::change_stream::event::ChangeStreamEvent::full_document)
        /// field will be populated. By default, the field will be empty for updates.
        full_document: FullDocumentType,
        /// Configures how the
        /// [`ChangeStreamEvent::full_document_before_change`](
        /// crate::change_stream::event::ChangeStreamEvent::full_document_before_change) field will be
        /// populated.  By default, the field will be empty for updates.
        full_document_before_change: FullDocumentBeforeChangeType,

        /// Specifies the logical starting point for the new change stream. Note that if a watched
        /// collection is dropped and recreated or newly renamed, `start_after` should be set instead.
        /// `resume_after` and `start_after` cannot be set simultaneously.
        ///
        /// For more information on resuming a change stream see the documentation [here](https://www.mongodb.com/docs/manual/changeStreams/#change-stream-resume-after)
        resume_after: ResumeToken,

        /// The change stream will only provide changes that occurred at or after the specified
        /// timestamp. Any command run against the server will return an operation time that can be
        /// used here.
        start_at_operation_time: Timestamp,

        /// Takes a resume token and starts a new change stream returning the first notification after
        /// the token. This will allow users to watch collections that have been dropped and
        /// recreated or newly renamed collections without missing any notifications.
        ///
        /// This feature is only available on MongoDB 4.2+.
        ///
        /// See the documentation [here](https://www.mongodb.com/docs/master/changeStreams/#change-stream-start-after) for more
        /// information.
        start_after: ResumeToken,

        /// The maximum amount of time for the server to wait on new documents to satisfy a change
        /// stream query.        
        max_await_time: Duration,

        /// The number of documents to return per batch.
        batch_size: u32,

        /// Specifies a collation.
        collation: Collation,

        /// The read concern to use for the operation.
        ///
        /// If none is specified, the read concern defined on the object executing this operation will
        /// be used.
        read_concern: ReadConcern,

        /// The criteria used to select a server for this operation.
        ///
        /// If none is specified, the selection criteria defined on the object executing this operation
        /// will be used.
        selection_criteria: SelectionCriteria,

        /// Tags the query with an arbitrary [`Bson`] value to help trace the operation through the
        /// database profiler, currentOp and logs.
        ///
        /// The comment can be any [`Bson`] value on server versions 4.4+. On lower server versions,
        /// the comment must be a [`Bson::String`] value.
        comment: Bson,
    );
}

impl<'a> Watch<'a, Implicit> {
    /// Use the provided ['ClientSession'].
    pub fn with_session<'s>(self, session: &'s mut ClientSession) -> Watch<'a, Explicit<'s>> {
        Watch {
            client: self.client,
            pipeline: self.pipeline,
            options: self.options,
            session: Explicit(session),
        }
    }
}

impl<'a> IntoFuture for Watch<'a, Implicit> {
    type Output = Result<ChangeStream<ChangeStreamEvent<Document>>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        async {
            resolve_options!(self.client, self.options, [read_concern, selection_criteria]);
            self.options
                .get_or_insert_with(Default::default)
                .all_changes_for_cluster = Some(true);
            let target = AggregateTarget::Database("admin".to_string());
            self.client.execute_watch(self.pipeline, self.options, target, None).await
        }.boxed()
    }
}

impl<'a> IntoFuture for Watch<'a, Explicit<'a>> {
    type Output = Result<SessionChangeStream<ChangeStreamEvent<Document>>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        async {
            resolve_read_concern_with_session!(self.client, self.options, Some(&mut *self.session.0))?;
            resolve_selection_criteria_with_session!(self.client, self.options, Some(&mut *self.session.0))?;
            self.options
                .get_or_insert_with(Default::default)
                .all_changes_for_cluster = Some(true);
            let target = AggregateTarget::Database("admin".to_string());
            self.client.execute_watch_with_session(self.pipeline, self.options, target, None, self.session.0)
                .await
        }.boxed()
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<'a> Watch<'a, Implicit> {
    /// Synchronously execute this action.
    pub fn run(self) -> Result<crate::sync::ChangeStream<ChangeStreamEvent<Document>>> {
        crate::runtime::block_on(self.into_future()).map(crate::sync::ChangeStream::new)
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<'a> Watch<'a, Explicit<'a>> {
    /// Synchronously execute this action.
    pub fn run(self) -> Result<crate::sync::SessionChangeStream<ChangeStreamEvent<Document>>> {
        crate::runtime::block_on(self.into_future()).map(crate::sync::SessionChangeStream::new)
    }
}