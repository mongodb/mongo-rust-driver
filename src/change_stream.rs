//! Contains the functionality for change streams.
pub mod event;
pub(crate) mod options;
pub mod session;

#[cfg(test)]
use std::collections::VecDeque;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(test)]
use bson::RawDocumentBuf;
use bson::{Document, Timestamp};
use derivative::Derivative;
use futures_core::{future::BoxFuture, Stream};
use serde::de::DeserializeOwned;
#[cfg(test)]
use tokio::sync::oneshot;

use crate::{
    change_stream::event::{ChangeStreamEvent, ResumeToken},
    cursor::{stream_poll_next, BatchValue, CursorStream, NextInBatchFuture},
    error::{ErrorKind, Result},
    operation::aggregate::AggregateTarget,
    ClientSession,
    Cursor,
};

/// A `ChangeStream` streams the ongoing changes of its associated collection, database or
/// deployment. `ChangeStream` instances should be created with method `watch` against the relevant
/// target.
///
/// `ChangeStream`s are "resumable", meaning that they can be restarted at a given place in the
/// stream of events. This is done automatically when the `ChangeStream` encounters certain
/// ["resumable"](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#resumable-error)
/// errors, such as transient network failures. It can also be done manually by passing
/// a [`ResumeToken`] retrieved from a past event into either the
/// [`resume_after`](crate::action::Watch::resume_after) or
/// [`start_after`](crate::action::Watch::start_after) (4.2+) options used to create the
/// `ChangeStream`. Issuing a raw change stream aggregation is discouraged unless users wish to
/// explicitly opt out of resumability.
///
/// A `ChangeStream` can be iterated like any other [`Stream`]:
///
/// ```
/// # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
/// # {
/// # #[cfg(feature = "tokio-runtime")]
/// # use futures::stream::StreamExt;
/// # use mongodb::{Client, error::Result, bson::doc,
/// # change_stream::event::ChangeStreamEvent};
/// # #[cfg(feature = "async-std-runtime")]
/// # use async_std::{task, stream::StreamExt};
/// # #[cfg(feature = "tokio-runtime")]
/// # use tokio::task;
/// #
/// # async fn func() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let coll = client.database("foo").collection("bar");
/// let mut change_stream = coll.watch().await?;
/// let coll_ref = coll.clone();
/// task::spawn(async move {
///     coll_ref.insert_one(doc! { "x": 1 }, None).await;
/// });
/// while let Some(event) = change_stream.next().await.transpose()? {
///     println!("operation performed: {:?}, document: {:?}", event.operation_type, event.full_document);
///     // operation performed: Insert, document: Some(Document({"x": Int32(1)}))
/// }
/// #
/// # Ok(())
/// # }
/// # }
/// ```
///
/// If a [`ChangeStream`] is still open when it goes out of scope, it will automatically be closed
/// via an asynchronous [killCursors](https://www.mongodb.com/docs/manual/reference/command/killCursors/) command executed
/// from its [`Drop`](https://doc.rust-lang.org/std/ops/trait.Drop.html) implementation.
///
/// See the documentation [here](https://www.mongodb.com/docs/manual/changeStreams) for more
/// details. Also see the documentation on [usage recommendations](https://www.mongodb.com/docs/manual/administration/change-streams-production-recommendations/).
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ChangeStream<T>
where
    T: DeserializeOwned,
{
    /// The cursor to iterate over event instances.
    cursor: Cursor<T>,

    /// Arguments to `watch` that created this change stream.
    args: WatchArgs,

    /// Dynamic information associated with this change stream.
    data: ChangeStreamData,

    /// A pending future for a resume.
    #[derivative(Debug = "ignore")]
    pending_resume: Option<BoxFuture<'static, Result<ChangeStream<T>>>>,
}

impl<T> ChangeStream<T>
where
    T: DeserializeOwned,
{
    pub(crate) fn new(cursor: Cursor<T>, args: WatchArgs, data: ChangeStreamData) -> Self {
        let pending_resume: Option<BoxFuture<'static, Result<ChangeStream<T>>>> = None;
        Self {
            cursor,
            args,
            data,
            pending_resume,
        }
    }

    /// Returns the cached resume token that can be used to resume after the most recently returned
    /// change.
    ///
    /// See the documentation
    /// [here](https://www.mongodb.com/docs/manual/changeStreams/#change-stream-resume-token) for more
    /// information on change stream resume tokens.
    pub fn resume_token(&self) -> Option<ResumeToken> {
        self.data.resume_token.clone()
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<D: DeserializeOwned>(self) -> ChangeStream<D> {
        ChangeStream {
            cursor: self.cursor.with_type(),
            args: self.args,
            data: self.data,
            pending_resume: None,
        }
    }

    /// Returns whether the change stream will continue to receive events.
    pub fn is_alive(&self) -> bool {
        !self.cursor.is_exhausted()
    }

    /// Retrieves the next result from the change stream, if any.
    ///
    /// Where calling `Stream::next` will internally loop until a change document is received,
    /// this will make at most one request and return `None` if the returned document batch is
    /// empty.  This method should be used when storing the resume token in order to ensure the
    /// most up to date token is received, e.g.
    ///
    /// ```
    /// # use mongodb::{Client, Collection, bson::Document, error::Result};
    /// # async fn func() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com").await?;
    /// # let coll: Collection<Document> = client.database("foo").collection("bar");
    /// let mut change_stream = coll.watch().await?;
    /// let mut resume_token = None;
    /// while change_stream.is_alive() {
    ///     if let Some(event) = change_stream.next_if_any().await? {
    ///         // process event
    ///     }
    ///     resume_token = change_stream.resume_token();
    /// }
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_if_any(&mut self) -> Result<Option<T>> {
        Ok(match NextInBatchFuture::new(self).await? {
            BatchValue::Some { doc, .. } => Some(bson::from_slice(doc.as_bytes())?),
            BatchValue::Empty | BatchValue::Exhausted => None,
        })
    }

    #[cfg(test)]
    pub(crate) fn set_kill_watcher(&mut self, tx: oneshot::Sender<()>) {
        self.cursor.set_kill_watcher(tx);
    }

    #[cfg(test)]
    pub(crate) fn current_batch(&self) -> &VecDeque<RawDocumentBuf> {
        self.cursor.current_batch()
    }

    #[cfg(test)]
    pub(crate) fn client(&self) -> &crate::Client {
        self.cursor.client()
    }
}

/// Arguments passed to a `watch` method, captured to allow resume.
#[derive(Debug, Clone)]
pub(crate) struct WatchArgs {
    /// The pipeline of stages to append to an initial `$changeStream` stage.
    pub(crate) pipeline: Vec<Document>,

    /// The original target of the change stream.
    pub(crate) target: AggregateTarget,

    /// The options provided to the initial `$changeStream` stage.
    pub(crate) options: Option<options::ChangeStreamOptions>,
}

/// Dynamic change stream data needed for resume.
#[derive(Debug, Default)]
pub(crate) struct ChangeStreamData {
    /// The `operationTime` returned by the initial `aggregate` command.
    pub(crate) initial_operation_time: Option<Timestamp>,

    /// The cached resume token.
    pub(crate) resume_token: Option<ResumeToken>,

    /// Whether or not the change stream has attempted a resume, used to attempt a resume only
    /// once.
    pub(crate) resume_attempted: bool,

    /// Whether or not the change stream has returned a document, used to update resume token
    /// during an automatic resume.
    pub(crate) document_returned: bool,

    /// The implicit session used to create the original cursor.
    pub(crate) implicit_session: Option<ClientSession>,
}

impl ChangeStreamData {
    fn take(&mut self) -> Self {
        Self {
            initial_operation_time: self.initial_operation_time,
            resume_token: self.resume_token.clone(),
            resume_attempted: self.resume_attempted,
            document_returned: self.document_returned,
            implicit_session: self.implicit_session.take(),
        }
    }
}

fn get_resume_token(
    batch_value: &BatchValue,
    batch_token: Option<&ResumeToken>,
) -> Result<Option<ResumeToken>> {
    Ok(match batch_value {
        BatchValue::Some { doc, is_last } => {
            let doc_token = match doc.get("_id")? {
                Some(val) => ResumeToken(val.to_raw_bson()),
                None => return Err(ErrorKind::MissingResumeToken.into()),
            };
            if *is_last && batch_token.is_some() {
                batch_token.cloned()
            } else {
                Some(doc_token)
            }
        }
        BatchValue::Empty => batch_token.cloned(),
        _ => None,
    })
}

impl<T> CursorStream for ChangeStream<T>
where
    T: DeserializeOwned,
{
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>> {
        loop {
            if let Some(mut pending) = self.pending_resume.take() {
                match Pin::new(&mut pending).poll(cx) {
                    Poll::Pending => {
                        self.pending_resume = Some(pending);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(new_stream)) => {
                        // Ensure that the old cursor is killed on the server selected for the new
                        // one.
                        self.cursor
                            .set_drop_address(new_stream.cursor.address().clone());
                        self.cursor = new_stream.cursor;
                        self.args = new_stream.args;
                        // After a successful resume, another resume must be allowed.
                        self.data.resume_attempted = false;
                        continue;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            }
            let out = self.cursor.poll_next_in_batch(cx);
            match &out {
                Poll::Ready(Ok(bv)) => {
                    if let Some(token) =
                        get_resume_token(bv, self.cursor.post_batch_resume_token())?
                    {
                        self.data.resume_token = Some(token);
                    }
                    if matches!(bv, BatchValue::Some { .. }) {
                        self.data.document_returned = true;
                    }
                }
                Poll::Ready(Err(e)) if e.is_resumable() && !self.data.resume_attempted => {
                    self.data.resume_attempted = true;
                    let client = self.cursor.client().clone();
                    let args = self.args.clone();
                    let mut data = self.data.take();
                    data.implicit_session = self.cursor.take_implicit_session();
                    self.pending_resume = Some(Box::pin(async move {
                        let new_stream: Result<ChangeStream<ChangeStreamEvent<()>>> = client
                            .execute_watch(args.pipeline, args.options, args.target, Some(data))
                            .await;
                        new_stream.map(|cs| cs.with_type::<T>())
                    }));
                    // Iterate the loop so the new future gets polled and can register wakers.
                    continue;
                }
                _ => {}
            }
            return out;
        }
    }
}

impl<T> Stream for ChangeStream<T>
where
    T: DeserializeOwned,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        stream_poll_next(Pin::into_inner(self), cx)
    }
}
