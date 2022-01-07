//! Types for change streams using sessions.
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll}, marker::PhantomData,
};

use bson::Document;
use futures_core::{future::BoxFuture, Stream};
use futures_util::{StreamExt};
use serde::de::DeserializeOwned;

use crate::{
    cursor::{BatchValue, CursorStream, NextInBatchFuture},
    error::Result,
    ClientSession,
    SessionCursor,
    SessionCursorStream,
};

use super::{event::{ResumeToken, ChangeStreamEvent}, get_resume_token, stream_poll_next, ChangeStreamData, WatchArgs};

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
    args: WatchArgs,
    data: ChangeStreamData,
}

impl<T> SessionChangeStream<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub(crate) fn new(cursor: SessionCursor<T>, args: WatchArgs, data: ChangeStreamData) -> Self {
        Self { cursor, args, data }
    }

    /// Returns the cached resume token that can be used to resume after the most recently returned
    /// change.
    ///
    /// See the documentation
    /// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
    /// information on change stream resume tokens.
    pub fn resume_token(&self) -> Option<ResumeToken> {
        self.data.resume_token.clone()
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<D: DeserializeOwned + Unpin + Send + Sync>(self) -> SessionChangeStream<D> {
        SessionChangeStream::new(
            self.cursor.with_type(),
            self.args,
            self.data,
        )
    }

    /*
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
        todo!()
        /*
        SessionChangeStreamValues {
            stream: self,
            state: StreamState::Idle(session),
        }
        */
    }
    */

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
    /// while let Some(event) = cs.next(&mut session).await? {
    ///     let id = bson::to_bson(&event.id)?;
    ///     other_coll.insert_one_with_session(doc! { "id": id }, None, &mut session).await?;
    /// }
    /// # Ok::<(), mongodb::error::Error>(())
    /// # };
    /// # }
    /// ```
    pub async fn next(&mut self, session: &mut ClientSession) -> Result<Option<T>> {
        loop {
            let maybe_next = self.next_if_any(session).await?;
            match maybe_next {
                Some(t) => return Ok(Some(t)),
                None if self.is_alive() => continue,
                None => return Ok(None),
            }
        }
    }

    /// Returns whether the change stream will continue to receive events.
    pub fn is_alive(&self) -> bool {
        !self.cursor.is_exhausted()
    }

    /// Retrieve the next result from the change stream, if any.
    ///
    /// Where calling `next` will internally loop until a change document is received,
    /// this will make at most one request and return `None` if the returned document batch is
    /// empty.  This method should be used when storing the resume token in order to ensure the
    /// most up to date token is received, e.g.
    ///
    /// ```ignore
    /// # use mongodb::{Client, error::Result};
    /// # async fn func() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com").await?;
    /// # let coll = client.database("foo").collection("bar");
    /// # let mut session = client.start_session(None).await?;
    /// let mut change_stream = coll.watch_with_session(None, None, &mut session).await?;
    /// let mut resume_token = None;
    /// while change_stream.is_alive() {
    ///     if let Some(event) = change_stream.next_if_any(&mut session) {
    ///         // process event
    ///     }
    ///     resume_token = change_stream.resume_token().cloned();
    /// }
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_if_any(&mut self, session: &mut ClientSession) -> Result<Option<T>> {
        loop {
            let (next, post_batch_token, client) = {
                let mut stream = self.cursor.stream(session);
                let next = NextInBatchFuture::new(&mut stream).await;
                let post_batch_token = stream.post_batch_resume_token().cloned();
                let client = stream.client().clone();
                (next, post_batch_token, client)
            };
            match next {
                Ok(bv) => {
                    if let Some(token) = get_resume_token(&bv, post_batch_token.as_ref())? {
                        self.data.resume_token = Some(token);
                    }
                    match bv {
                        BatchValue::Some { doc, .. } => {
                            self.data.document_returned = true;
                            return Ok(Some(bson::from_slice(doc.as_bytes())?));
                        }
                        BatchValue::Empty | BatchValue::Exhausted => return Ok(None),
                    }
                }
                Err(e) if e.is_resumable() && !self.data.resume_attempted => {
                    self.data.resume_attempted = true;
                    let args = self.args.clone();
                    let data = self.data.clone();
                    let new_stream: SessionChangeStream<ChangeStreamEvent<()>> = client.execute_watch_with_session(args.pipeline, args.options, args.target, Some(data), session).await?;
                    let new_stream = new_stream.with_type::<T>();
                    self.cursor.set_drop_address(new_stream.cursor.address().clone());
                    self.cursor = new_stream.cursor;
                    self.args = new_stream.args;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/*
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
    inner: Arc<Mutex<SessionChangeStreamState<T>>>,
    state: StreamState<'cursor, 'session, T>,
    phantom: PhantomData<&'cursor mut T>,
    //pending_resume: Option<BoxFuture<'static, Result<SessionChangeStream<T>>>>,
}

pub enum StreamState<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    Idle(&'session mut ClientSession),
    PollNext(SessionCursorStream<'cursor, 'session, T>),
    /*
    PendingResume(Option<BoxFuture<'static, Result<SessionChangeStream<T>>>>),
    */
}

impl<'cursor, 'session, T> SessionChangeStreamValues<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub fn resume_token(&self) -> Option<ResumeToken> {
        self.inner.lock().unwrap().data.resume_token.clone()
    }

    pub fn is_alive(&self) -> bool {
        !self.inner.lock().unwrap().cursor.is_exhausted()
    }

    pub async fn next_if_any<'a>(&'a mut self) -> Result<Option<T>> {
        Ok(match NextInBatchFuture::new(self).await? {
            BatchValue::Some { doc, .. } => Some(bson::from_slice(doc.as_bytes())?),
            BatchValue::Empty | BatchValue::Exhausted => None,
        })
    }
}

impl<'cursor, 'session, T> CursorStream for SessionChangeStreamValues<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>> {
        let mut out = Poll::Pending;
        take_mut::take(&mut self.state, |state| {
            match state {
                _ => todo!(),
            }
        });
        out
        /*
        if let Some(mut pending) = self.pending_resume.take() {
            match Pin::new(&mut pending).poll(cx) {
                Poll::Pending => {
                    self.pending_resume = Some(pending);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(new_stream)) => {
                    // Ensure that the old cursor is killed on the server selected for the new one.
                    todo!();
                    /*
                    self.stream.set_drop_address(new_values.stream.address().clone());
                    self.stream = new_values.stream;
                    self.args = new_values.args;
                    return Poll::Pending;
                    */
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
        let out = self.cursor_stream.poll_next_in_batch(cx);
        match &out {
            Poll::Ready(Ok(bv)) => {
                if let Some(token) = get_resume_token(bv, self.cursor_stream.post_batch_resume_token())? {
                    self.change_stream.data.resume_token = Some(token);
                }
                if matches!(bv, BatchValue::Some { .. }) {
                    self.change_stream.data.document_returned = true;
                }
            }
            Poll::Ready(Err(e)) if e.is_resumable() && !self.change_stream.data.resume_attempted => {
                self.change_stream.data.resume_attempted = true;
                let client = self.cursor_stream.client().clone();
                let args = self.change_stream.args.clone();
                let data = self.change_stream.data.clone();
                self.pending_resume = Some(Box::pin(async move {
                    let new_stream: Result<SessionChangeStream<ChangeStreamEvent<()>>> = client
                        .execute_watch_with_session(args.pipeline, args.options, args.target, Some(data), todo!())
                        .await;
                    new_stream.map(|cs| cs.with_type::<T>())
                }));
                return Poll::Pending;
            }
            _ => {}
        }
        out
        */
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
*/