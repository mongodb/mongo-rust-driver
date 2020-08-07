//! Contains the functionality for ChangeStreams.
pub mod document;
pub(crate) mod options;

use std::{marker::PhantomData, pin::Pin, task::Poll};

use bson::{Bson, Document};
use futures::{stream::Stream, task::Context};
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    change_stream::{
        document::{ChangeStreamEventDocument, ResumeToken},
        options::ChangeStreamOptions,
    },
    client::ClientSession,
    cursor::CursorSpecification,
    error::Result,
    options::AggregateOptions,
    selection_criteria::SelectionCriteria,
    Client,
    Collection,
    Cursor,
    Database,
    Namespace,
};

/// A `ChangeStream` streams the ongoing changes of its associated collection, database or
/// deployment. `ChangeStream` instances should be created with method `watch` against the relevant
/// target.
///
/// `ChangeStream`'s are "resumable", meaning that they can be restarted at a given place in the
/// stream of events. This is done automatically when the `ChangeStream` encounters certain
/// ["resumable"](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#resumable-error) errors, such as transient network failures. It can also be done manually by passing
/// a [`ResumeToken`](document/struct.ResumeToken.html) retrieved from a past event
/// into either the
/// [`resume_after`](option/struct.ChangeStreamOptions.html#structfield.resume_after)
/// or [`start_after`](option/struct.ChangeStreamOptions.html#structfield.start_after)
/// (4.2+) options used to create the `ChangeStream`. Issuing a raw change stream aggregation is
/// discouraged unless users wish to explicitly opt out of resumability.
///
/// A `ChangeStream` can be iterated to return batches of instances of any type that implements the
/// Deserialize trait. By default, it returns batches of
/// [`ChangeStreamEventDocument`](document/struct.ChangeStreamEventDocument.html).
/// These documents correspond to changes in the associated collection, database or deployment.
///
/// A `ChangeStream` can be iterated like any other `Iterator`:
///
/// ```rust
/// # #[cfg(not(feature = "sync"))]
/// # use futures::stream::StreamExt;
/// # use mongodb::{Client, error::Result, bson::doc,
/// # change_stream::document::ChangeStreamEventDocument};
/// # #[cfg(feature = "async-std-runtime")]
/// # use async_std::task;
/// # #[cfg(feature = "tokio-runtime")]
/// # use tokio::task;
/// #
/// # async fn func() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let coll = client.database("foo").collection("bar");
/// let mut change_stream = coll.watch(None, None).await?;
/// let coll_ref = coll.clone();
/// task::spawn(async move {
///     coll_ref.insert_one(doc! { "x": 1 }, None).await;
/// });
/// while let Some(document) = change_stream.next().await {
///     let document = document?;
///     println!("operation performed: {:?}, document: {:?}", document.operation_type, document.full_document);
///     // operation performed: Insert, document: Some(Document({"x": Int32(1)}))
/// }
/// #
/// # Ok(())
/// # }
/// ```
///
/// See the documentation [here](https://docs.mongodb.com/manual/changeStreams) for more
/// details. Also see the documentation on [usage recommendations](https://docs.mongodb.com/manual/administration/change-streams-production-recommendations/).
#[derive(Debug)]
pub struct ChangeStream {
    /// The cursor to iterate over `ChangeStreamEventDocument` instances.
    cursor: Cursor,

    /// The pipeline of stages to append to an initial `$changeStream` stage.
    pipeline: Vec<Document>,

    /// The client that was used for the initial `$changeStream` aggregation, used for server
    /// selection during an automatic resume.
    client: Client,

    /// The original target of the change stream, used for re-issuing the aggregation during
    /// an automatic resume.
    target: ChangeStreamTarget,

    /// The cached resume token.
    resume_token: Option<ResumeToken>,

    /// The options provided to the initial `$changeStream` stage.
    options: Option<ChangeStreamOptions>,

    /// The SelectionCriteria for the initial `$changeStream` aggregation, used for server
    /// selection during an automatic resume.
    selection_criteria: Option<SelectionCriteria>,

    /// Whether or not the change stream has attempted a resume, used to attempt a resume only
    /// once.
    resume_attempted: bool,

    /// Whether or not the change stream has returned a document, used to update resume token
    /// during an automatic resume.
    document_returned: bool,
}

impl ChangeStream {
    pub(crate) fn new(
        spec: ChangeStreamSpecification,
        session: Option<ClientSession>,
        client: Client,
    ) -> Self {
        let cursor = Cursor::new(client.clone(), spec.cursor_spec, session);
        Self {
            cursor,
            pipeline: spec.pipeline,
            client,
            target: spec.target,
            resume_token: None,
            options: spec.options.clone(),
            selection_criteria: spec.options.and_then(|opts| opts.selection_criteria),
            resume_attempted: false,
            document_returned: false,
        }
    }

    /// Returns the cached resume token that can be used to resume after the most recently returned
    /// change.
    ///
    /// See the documentation
    /// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
    /// information on change stream resume tokens.
    pub fn resume_token(&self) -> Option<ResumeToken> {
        todo!();
    }

    #[cfg(test)]
    pub fn options(&self) -> Option<ChangeStreamOptions> {
        self.options.clone()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ChangeStreamTarget {
    Collection(Namespace),
    Database(String),
    Cluster(String),
}

impl Stream for ChangeStream {
    type Item = Result<ChangeStreamEventDocument>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let poll = Pin::new(&mut self.cursor).poll_next(cx);
        // Using existing Poll instead of constructing a new one would lead to it having the wrong
        // generic type parameter.
        match poll {
            Poll::Ready(Some(Ok(doc))) => {
                Poll::Ready(Some(Ok(bson::from_bson(Bson::Document(doc))?)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Specification used to create a new `ChangeStream`.
#[derive(Debug)]
pub(crate) struct ChangeStreamSpecification {
    /// Specification used to create a cursor wrapped by the `ChangeStream`.
    pub(crate) cursor_spec: CursorSpecification,

    /// The pipeline of stages to append to an initial `$changeStream` stage.
    pub(crate) pipeline: Vec<Document>,

    /// The options provided to the initial `$changeStream` stage.
    pub(crate) options: Option<ChangeStreamOptions>,

    /// The original target of the change stream, used for re-issuing the aggregation during
    /// an automatic resume.
    pub(crate) target: ChangeStreamTarget,
}

impl ChangeStreamSpecification {
    pub(crate) fn new(
        cursor_spec: CursorSpecification,
        pipeline: Vec<Document>,
        options: Option<ChangeStreamOptions>,
        target: ChangeStreamTarget,
    ) -> Self {
        Self {
            cursor_spec,
            pipeline,
            options,
            target,
        }
    }
}
