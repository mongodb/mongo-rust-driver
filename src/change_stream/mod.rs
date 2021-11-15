//! Contains the functionality for ChangeStreams.
pub mod event;
pub(crate) mod options;

use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bson::Document;
use futures_core::Stream;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    change_stream::{
        event::{ChangeStreamEvent, ResumeToken},
        options::ChangeStreamOptions,
    },
    error::Result,
    options::AggregateOptions,
    selection_criteria::ReadPreference,
    Client,
    Collection,
    Cursor,
    Database,
};

/// A `ChangeStream` streams the ongoing changes of its associated collection, database or
/// deployment. `ChangeStream` instances should be created with method `watch` or
/// `watch_with_pipeline` against the relevant target.
///
/// `ChangeStream`'s are "resumable", meaning that they can be restarted at a given place in the
/// stream of events. This is done automatically when the `ChangeStream` encounters certain
/// ["resumable"](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#resumable-error)
/// errors, such as transient network failures. It can also be done manually by passing
/// a [`ResumeToken`] retrieved from a past event into either the
/// [`resume_after`](ChangeStreamOptions::resume_after) or
/// [`start_after`](ChangeStreamOptions::start_after) (4.2+) options used to create the
/// `ChangeStream`. Issuing a raw change stream aggregation is discouraged unless users wish to
/// explicitly opt out of resumability.
///
/// A `ChangeStream` can be iterated to return batches of instances of the result type.  For
/// `watch`, this will be [`ChangeStreamEvent`] wrapping the value type (for [`Collection::watch`])
/// or a [`Document`] (for [`Client::watch`] and [`Database::watch`]).  For `watch_with_pipeline`,
/// a deserializable type must be specified that matches the result of the aggregation pipeline; if
/// the pipeline does not alter the event structure it may be convenient to reuse
/// [`ChangeStreamEvent`] for that type.
///
/// A `ChangeStream` can be iterated like any other [`Stream`]:
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
/// let mut change_stream = coll.watch(None).await?;
/// let coll_ref = coll.clone();
/// task::spawn(async move {
///     coll_ref.insert_one(doc! { "x": 1 }, None).await;
/// });
/// while let Some(event) = change_stream.next().await {
///     println!("operation performed: {:?}, document: {:?}", event.operation_type, event.full_document);
///     // operation performed: Insert, document: Some(Document({"x": Int32(1)}))
/// }
/// #
/// # Ok(())
/// # }
/// ```
///
/// See the documentation [here](https://docs.mongodb.com/manual/changeStreams) for more
/// details. Also see the documentation on [usage recommendations](https://docs.mongodb.com/manual/administration/change-streams-production-recommendations/).
pub struct ChangeStream<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    /// The cursor to iterate over event instances.
    cursor: Cursor<T>,

    /// The pipeline of stages to append to an initial `$changeStream` stage.
    pipeline: Vec<Document>,

    /// The client that was used for the initial `$changeStream` aggregation, used for server
    /// selection during an automatic resume.
    client: Client,

    /// The original target of the change stream, used for re-issuing the aggregation during
    /// an automatic resume.
    target: ChangeStreamTarget<T>,

    /// The cached resume token.
    resume_token: Option<ResumeToken>,

    /// The options provided to the initial `$changeStream` stage.
    options: Option<ChangeStreamOptions>,

    /// The read preference for the initial `$changeStream` aggregation, used for server selection
    /// during an automatic resume.
    read_preference: Option<ReadPreference>,

    /// Whether or not the change stream has attempted a resume, used to attempt a resume only
    /// once.
    resume_attempted: bool,

    /// Whether or not the change stream has returned a document, used to update resume token
    /// during an automatic resume.
    document_returned: bool,
}

impl<T> ChangeStream<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    /// Returns the cached resume token that can be used to resume after the most recently returned
    /// change.
    ///
    /// See the documentation
    /// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
    /// information on change stream resume tokens.
    pub fn resume_token(&self) -> Option<&ResumeToken> {
        todo!()
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<D: DeserializeOwned + Unpin + Send + Sync>(self) -> ChangeStream<D> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ChangeStreamTarget<T> {
    Collection(Collection<T>),
    Database(Database),
    Cluster(Database),
}

impl<T> Stream for ChangeStream<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
