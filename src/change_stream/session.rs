//! Types for change streams using sessions.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bson::Document;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;

use crate::{error::Result, ClientSession, SessionCursor, SessionCursorStream, cursor::{CursorStream, BatchValue}};

use super::{event::ResumeToken, ChangeStreamData, stream_poll_next, get_resume_token};

/// A [`SessionChangeStream`] is a change stream that was created with a [`ClientSession`] that must
/// be iterated using one. To iterate, use [`SessionChangeStream::next`] or retrieve a
/// [`SessionCursorStream`] using [`SessionChangeStream::stream`]:
///
/// ```ignore
/// # use mongodb::{bson::Document, Client, error::Result, ClientSession, SessionCursor};
/// #
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let mut session = client.start_session(None).await?;
/// # let coll = client.database("foo").collection::<Document>("bar");
/// #
/// // iterate using next()
/// let mut cs = coll.watch_with_session(None, None, &mut session).await?;
/// while let Some(event) = cs.next(&mut session).await.transpose()? {
///     println!("{:?}", event)
/// }
///
/// // iterate using `Stream`:
/// use futures::stream::TryStreamExt;
///
/// let mut cs = coll.watch_with_session(None, None, &mut session).await?;
/// let results: Vec<_> = cs.values(&mut session).try_collect().await?;
/// #
/// # Ok(())
/// # }
/// ```
pub struct SessionChangeStream<T>
where
    T: DeserializeOwned + Unpin,
{
    cursor: SessionCursor<T>,
    data: ChangeStreamData,
    resume_token: Option<ResumeToken>,
}

impl<T> SessionChangeStream<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub(crate) fn new(cursor: SessionCursor<T>, data: ChangeStreamData, resume_token: Option<ResumeToken>) -> Self {
        Self { cursor, data, resume_token }
    }

    /// Returns the cached resume token that can be used to resume after the most recently returned
    /// change.
    ///
    /// See the documentation
    /// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
    /// information on change stream resume tokens.
    pub fn resume_token(&self) -> Option<&ResumeToken> {
        self.resume_token.as_ref()
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<D: DeserializeOwned + Unpin + Send + Sync>(self) -> SessionChangeStream<D> {
        SessionChangeStream {
            cursor: self.cursor.with_type(),
            data: self.data,
            resume_token: self.resume_token,
        }
    }

    /// Retrieves a [`SessionCursorStream`] to iterate this change stream. The session provided must
    /// be the same session used to create the cursor.
    ///
    /// Note that the borrow checker will not allow the session to be reused in between iterations
    /// of this stream. In order to do that, either use [`SessionChangeStream::next`] instead or
    /// drop the stream before using the session.
    ///
    /// ```ignore
    /// # use bson::{doc, Document};
    /// # use mongodb::{Client, change_stream::event::ChangeStreamEvent, error::Result};
    /// # fn main() {
    /// # async {
    /// # let client = Client::with_uri_str("foo").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let other_coll = coll.clone();
    /// # let mut session = client.start_session(None).await?;
    /// #
    /// use futures::stream::TryStreamExt;
    ///
    /// // iterate over the results
    /// let mut cs = coll.watch_with_session(None, None, &mut session).await?;
    /// while let Some(event) = cs.values(&mut session).try_next().await? {
    ///     println!("{:?}", event);
    /// }
    ///
    /// // collect the results
    /// let mut cs1 = coll.watch_with_session(None, None, &mut session).await?;
    /// let v: Vec<ChangeStreamEvent<Document>> = cs1.values(&mut session).try_collect().await?;
    ///
    /// // use session between iterations
    /// let mut cs2 = coll.watch_with_session(None, None, &mut session).await?;
    /// loop {
    ///     let event = match cs2.values(&mut session).try_next().await? {
    ///         Some(d) => d,
    ///         None => break,
    ///     };
    ///     let id = bson::to_bson(&event.id)?;
    ///     other_coll.insert_one_with_session(doc! { "id": id }, None, &mut session).await?;
    /// }
    /// # Ok::<(), mongodb::error::Error>(())
    /// # };
    /// # }
    /// ```
    pub fn values<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionChangeStreamValues<'_, 'session, T> {
        SessionChangeStreamValues {
            stream: self.cursor.stream(session),
            resume_token: &mut self.resume_token,
        }
    }

    /// Retrieve the next result from the change stream.
    /// The session provided must be the same session used to create the change stream.
    ///
    /// Use this method when the session needs to be used again between iterations or when the added
    /// functionality of `Stream` is not needed.
    ///
    /// ```ignore
    /// # use bson::{doc, Document};
    /// # use mongodb::Client;
    /// # fn main() {
    /// # async {
    /// # let client = Client::with_uri_str("foo").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let other_coll = coll.clone();
    /// # let mut session = client.start_session(None).await?;
    /// let mut cs = coll.watch_with_session(None, None, &mut session).await?;
    /// while let Some(event) = cs.next(&mut session).await.transpose()? {
    ///     let id = bson::to_bson(&event.id)?;
    ///     other_coll.insert_one_with_session(doc! { "id": id }, None, &mut session).await?;
    /// }
    /// # Ok::<(), mongodb::error::Error>(())
    /// # };
    /// # }
    /// ```
    pub async fn next(&mut self, session: &mut ClientSession) -> Option<Result<T>> {
        self.values(session).next().await
    }
}

/// A type that implements [`Stream`](https://docs.rs/futures/latest/futures/stream/index.html) which can be used to
/// stream the results of a [`SessionChangeStream`]. Returned from [`SessionChangeStream::values`].
///
/// This updates the buffer of the parent [`SessionChangeStream`] when dropped.
/// [`SessionChangeStream::next`] or any further streams created from
/// [`SessionChangeStream::values`] will pick up where this one left off.
pub struct SessionChangeStreamValues<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    stream: SessionCursorStream<'cursor, 'session, T>,
    resume_token: &'cursor mut Option<ResumeToken>,
}

impl<'cursor, 'session, T> SessionChangeStreamValues<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub fn resume_token(&self) -> Option<&ResumeToken> {
        self.resume_token.as_ref()
    }
}

impl<'cursor, 'session, T> CursorStream for SessionChangeStreamValues<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>> {
        let out = self.stream.poll_next_in_batch(cx);
        match &out {
            Poll::Ready(Ok(bv)) => {
                if let Some(token) = get_resume_token(bv, self.stream.post_batch_resume_token())? {
                    *self.resume_token = Some(token);
                }
            }
            _ => {}
        }
        out
    }
}

impl<'cursor, 'session, T> Stream for SessionChangeStreamValues<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        stream_poll_next(Pin::into_inner(self), cx)
    }
}