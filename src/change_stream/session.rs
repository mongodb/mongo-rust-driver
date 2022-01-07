//! Types for change streams using sessions.
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bson::Document;
use serde::de::DeserializeOwned;

use crate::{
    cursor::{BatchValue, CursorStream, NextInBatchFuture},
    error::Result,
    ClientSession,
    SessionCursor,
    SessionCursorStream,
};

use super::{
    event::{ChangeStreamEvent, ResumeToken},
    get_resume_token,
    stream_poll_next,
    ChangeStreamData,
    WatchArgs,
};

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
        SessionChangeStream::new(self.cursor.with_type(), self.args, self.data)
    }

    /// Retrieve the next result from the change stream.
    /// The session provided must be the same session used to create the change stream.
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
    ///     resume_token = change_stream.resume_token();
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
                    let new_stream: SessionChangeStream<ChangeStreamEvent<()>> = client
                        .execute_watch_with_session(
                            args.pipeline,
                            args.options,
                            args.target,
                            Some(self.data.clone()),
                            session,
                        )
                        .await?;
                    let new_stream = new_stream.with_type::<T>();
                    self.cursor
                        .set_drop_address(new_stream.cursor.address().clone());
                    self.cursor = new_stream.cursor;
                    self.args = new_stream.args;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
