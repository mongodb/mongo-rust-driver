use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bson::{doc, Document};
use derivative::Derivative;
use futures::{future::BoxFuture, stream::Stream};

use crate::{
    error::Result,
    operation::GetMore,
    options::StreamAddress,
    results::GetMoreResult,
    Client,
    Namespace,
    RUNTIME,
};

/// A `Cursor` streams the result of a query. When a query is made, a `Cursor` will be returned with
/// the first batch of results from the server; the documents will be returned as the `Cursor` is
/// iterated. When the batch is exhausted and if there are more results, the `Cursor` will fetch the
/// next batch of documents, and so forth until the results are exhausted. Note that because of this
/// batching, additional network I/O may occur on any given call to `Cursor::next`. Because of this,
/// a `Cursor` iterates over `Result<Document>` items rather than simply `Document` items.
///
/// The batch size of the `Cursor` can be configured using the options to the method that returns
/// it. For example, setting the `batch_size` field of
/// [`FindOptions`](options/struct.FindOptions.html) will set the batch size of the
/// `Cursor` returned by [`Collection::find`](struct.Collection.html#method.find).
///
/// Note that the batch size determines both the number of documents stored in memory by the
/// `Cursor` at a given time as well as the total number of network round-trips needed to fetch all
/// results from the server; both of these factors should be taken into account when choosing the
/// optimal batch size.
///
/// A cursor can be used like any other [`Stream`](https://docs.rs/futures/0.3.4/futures/stream/trait.Stream.html). The simplest way is just to iterate over the
/// documents it yields:
///
/// ```rust
/// # use futures::stream::StreamExt;
/// # use mongodb::{Client, error::Result};
/// #
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let coll = client.database("foo").collection("bar");
/// # let mut cursor = coll.find(None, None)?;
/// #
/// while let Some(doc) = cursor.next().await {
///   println!("{}", doc?)
/// }
/// #
/// # Ok(())
/// # }
/// ```
///
/// Additionally, all the other methods that an [`Stream`](https://docs.rs/futures/0.3.4/futures/stream/trait.Stream.html) has are available on `Cursor` as well.
/// This includes all of the functionality provided by [`StreamExt`](https://docs.rs/futures/0.3.4/futures/stream/trait.StreamExt.html), which provides similar functionality to the standard library `Iterator` trait.
/// For instance, if the number of results from a query is known to be small, it might make sense
/// to collect them into a vector:
///
/// ```rust
/// # use bson::{doc, bson, Document};
/// # use futures::stream::StreamExt;
/// # use mongodb::{Client, error::Result};
/// #
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let coll = client.database("foo").collection("bar");
/// # let cursor = coll.find(Some(doc! { "x": 1 }), None)?;
/// #
/// let results: Vec<Result<Document>> = cursor.collect().await;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Cursor {
    client: Client,
    get_more: GetMore,
    exhausted: bool,

    /// This will be None when the cursor is exhausted.
    state: Option<PollState>,
}

/// Describes the current state of the Cursor. If the state is Executing, then a getMore operation
/// is in progress. If the state is Buffer, then there are documents available from the current
/// batch.
#[derive(Derivative)]
#[derivative(Debug)]
enum PollState {
    Executing(#[derivative(Debug = "ignore")] BoxFuture<'static, Result<GetMoreResult>>),
    Buffer(VecDeque<Document>),
}

impl Cursor {
    pub(crate) fn new(client: Client, spec: CursorSpecification) -> Self {
        let get_more = GetMore::new(
            spec.ns,
            spec.id,
            spec.address,
            spec.batch_size,
            spec.max_time,
        );

        Self {
            client,
            get_more,
            exhausted: spec.id == 0,
            state: Some(PollState::Buffer(spec.buffer)),
        }
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        if self.exhausted {
            return;
        }

        let namespace = self.get_more.namespace().clone();
        let client = self.client.clone();
        let cursor_id = self.get_more.cursor_id();

        RUNTIME.execute(async move {
            let _: Result<_> = client
                .database(&namespace.db)
                .run_command(
                    doc! {
                        "killCursors": &namespace.coll,
                        "cursors": [cursor_id]
                    },
                    None,
                )
                .await;
        })
    }
}

impl Stream for Cursor {
    type Item = Result<Document>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.state.take() {
            // If the current state is Executing, then we check the progress of the getMore
            // operation.
            Some(PollState::Executing(mut future)) => match Pin::new(&mut future).poll(cx) {
                // If the getMore is finished and successful, then we pop off the first document
                // from the batch, set the poll state to Buffer, record whether the cursor is
                // exhausted, and return the popped document.
                Poll::Ready(Ok(get_more_result)) => {
                    let mut buffer: VecDeque<_> = get_more_result.batch.into_iter().collect();
                    let next_doc = buffer.pop_front();

                    self.state = Some(PollState::Buffer(buffer));
                    self.exhausted = get_more_result.exhausted;
                    Poll::Ready(next_doc.map(Ok))
                }

                // If the getMore finished with an error, return that error.
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),

                // If the getMore has not finished, keep the state as Executing and return.
                Poll::Pending => {
                    self.state = Some(PollState::Executing(future));
                    Poll::Pending
                }
            },

            Some(PollState::Buffer(mut buffer)) => {
                // If there is a document ready, return it.
                let poll = if let Some(doc) = buffer.pop_front() {
                    Poll::Ready(Some(Ok(doc)))
                // If the cursor is exhausted, return None.
                } else if self.exhausted {
                    Poll::Ready(None)
                // Since no document is ready and the cursor isn't exhausted, we need to start a new
                // getMore operation, so return that the operation is pending.
                } else {
                    Poll::Pending
                };

                // If no documents are left and the batch and the cursor is exhausted, set the state
                // to None.
                self.state = if buffer.is_empty() && self.exhausted {
                    None
                // If the batch is empty and the cursor is not exhausted, start a new operation and
                // set the state to Executing.
                } else if buffer.is_empty() {
                    let future = Box::pin(
                        self.client
                            .clone()
                            .execute_operation_owned(self.get_more.clone()),
                    );

                    Some(PollState::Executing(future))
                // Otherwise, there are documents left in the batch, so save the buffer as the
                // state.
                } else {
                    Some(PollState::Buffer(buffer))
                };

                poll
            }

            // If the state is None, then the cursor has already exhausted all its results, so do
            // nothing.
            None => Poll::Ready(None),
        }
    }
}

#[derive(Debug)]
pub(crate) struct CursorSpecification {
    pub(crate) ns: Namespace,
    pub(crate) address: StreamAddress,
    pub(crate) id: i64,
    pub(crate) batch_size: Option<u32>,
    pub(crate) max_time: Option<Duration>,
    pub(crate) buffer: VecDeque<Document>,
}
