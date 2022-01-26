use futures_util::stream::StreamExt;
use serde::de::DeserializeOwned;

use crate::{
    change_stream::{self as async_cs, event::ResumeToken},
    error::Result, RUNTIME,
};

/// A `ChangeStream` streams the ongoing changes of its associated collection, database or
/// deployment. `ChangeStream` instances should be created with method `watch` against the relevant target.
///
/// `ChangeStream`s are "resumable", meaning that they can be restarted at a given place in the
/// stream of events. This is done automatically when the `ChangeStream` encounters certain
/// ["resumable"](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#resumable-error)
/// errors, such as transient network failures. It can also be done manually by passing
/// a [`ResumeToken`] retrieved from a past event into either the
/// [`resume_after`](ChangeStreamOptions::resume_after) or
/// [`start_after`](ChangeStreamOptions::start_after) (4.2+) options used to create the
/// `ChangeStream`. Issuing a raw change stream aggregation is discouraged unless users wish to
/// explicitly opt out of resumability.
///
/// A `ChangeStream` can be iterated like any other [`Iterator`]:
///
/// ```ignore
/// # use mongodb::{sync::Client, error::Result, bson::doc,
/// # change_stream::event::ChangeStreamEvent};
/// #
/// # fn func() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// # let coll = client.database("foo").collection("bar");
/// let mut change_stream = coll.watch(None, None)?;
/// coll.insert_one(doc! { "x": 1 }, None)?;
/// for event in change_stream {
///     let event = event?;
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
    async_stream: async_cs::ChangeStream<T>,
}

impl<T> ChangeStream<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub(crate) fn new(async_stream: async_cs::ChangeStream<T>) -> Self {
        Self { async_stream }
    }

    /// Returns the cached resume token that can be used to resume after the most recently returned
    /// change.
    ///
    /// See the documentation
    /// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
    /// information on change stream resume tokens.
    pub fn resume_token(&self) -> Option<ResumeToken> {
        self.async_stream.resume_token()
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<D: DeserializeOwned + Unpin + Send + Sync>(self) -> ChangeStream<D> {
        ChangeStream {
            async_stream: self.async_stream.with_type(),
        }
    }

    /// Returns whether the change stream will continue to receive events.
    pub fn is_alive(&self) -> bool {
        self.async_stream.is_alive()
    }

    /// Retrieves the next result from the change stream, if any.
    ///
    /// Where calling `Iterator::next` will internally loop until a change document is received,
    /// this will make at most one request and return `None` if the returned document batch is
    /// empty.  This method should be used when storing the resume token in order to ensure the
    /// most up to date token is received, e.g.
    ///
    /// ```ignore
    /// # use mongodb::{sync::Client, error::Result};
    /// # fn func() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com")?;
    /// # let coll = client.database("foo").collection("bar");
    /// let mut change_stream = coll.watch(None, None)?;
    /// let mut resume_token = None;
    /// while change_stream.is_alive() {
    ///     if let Some(event) = change_stream.next_if_any() {
    ///         // process event
    ///     }
    ///     resume_token = change_stream.resume_token().cloned();
    /// }
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_if_any(&mut self) -> Result<Option<T>> {
        RUNTIME.block_on(self.async_stream.next_if_any())
    }
}

impl<T> Iterator for ChangeStream<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        RUNTIME.block_on(self.async_stream.next())
    }
}