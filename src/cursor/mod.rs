mod common;
// TODO: RUST-52 use this
#[allow(dead_code)]
mod session;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bson::Document;
use futures::{future::BoxFuture, Stream};

use crate::{
    client::ClientSession,
    error::Result,
    operation::GetMore,
    results::GetMoreResult,
    Client,
    RUNTIME,
};
pub(crate) use common::{CursorInformation, CursorSpecification};
use common::{GenericCursor, GetMoreProvider, GetMoreProviderResult};

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
/// # let mut cursor = coll.find(None, None).await?;
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
/// # let cursor = coll.find(Some(doc! { "x": 1 }), None).await?;
/// #
/// let results: Vec<Result<Document>> = cursor.collect().await;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Cursor {
    client: Client,
    wrapped_cursor: GenericCursor<OwnedSessionGetMoreProvider>,
}

impl Cursor {
    pub(crate) fn new(client: Client, spec: CursorSpecification, session: ClientSession) -> Self {
        let provider = OwnedSessionGetMoreProvider::new(&spec, session);

        Self {
            client: client.clone(),
            wrapped_cursor: GenericCursor::new(client, spec, provider),
        }
    }
}

impl Stream for Cursor {
    type Item = Result<Document>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.wrapped_cursor).poll_next(cx)
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        if self.wrapped_cursor.is_exhausted() {
            return;
        }

        let ns = self.wrapped_cursor.namespace();
        let coll = self
            .client
            .database(ns.db.as_str())
            .collection(ns.coll.as_str());
        let cursor_id = self.wrapped_cursor.id();
        RUNTIME.execute(async move { coll.kill_cursor(cursor_id).await });
    }
}

struct OwnedSessionGetMoreResult {
    get_more_result: Result<GetMoreResult>,
    session: ClientSession,
}

impl GetMoreProviderResult for OwnedSessionGetMoreResult {
    fn as_mut(&mut self) -> Result<&mut GetMoreResult> {
        self.get_more_result.as_mut().map_err(|e| e.clone())
    }
    fn as_ref(&self) -> Result<&GetMoreResult> {
        self.get_more_result.as_ref().map_err(|e| e.clone())
    }
}

enum OwnedSessionGetMoreProvider {
    Executing(BoxFuture<'static, OwnedSessionGetMoreResult>),
    Idle(ClientSession),
    Done,
}

impl OwnedSessionGetMoreProvider {
    fn new(spec: &CursorSpecification, session: ClientSession) -> Self {
        if spec.id() == 0 {
            Self::Done
        } else {
            Self::Idle(session)
        }
    }
}

impl GetMoreProvider for OwnedSessionGetMoreProvider {
    type GetMoreResult = OwnedSessionGetMoreResult;
    type GetMoreFuture = BoxFuture<'static, OwnedSessionGetMoreResult>;

    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture> {
        match self {
            Self::Executing(ref mut future) => Some(future),
            Self::Idle(_) | Self::Done => None,
        }
    }

    fn clear_execution(&mut self, result: Self::GetMoreResult) {
        // If cursor is exhausted, immediately return implicit session to the pool.
        if result.get_more_result.map(|r| r.exhausted).unwrap_or(false) {
            *self = Self::Done;
        } else {
            *self = Self::Idle(result.session)
        }
    }

    fn start_execution(&mut self, info: CursorInformation, client: Client) {
        take_mut::take(self, |self_| match self_ {
            Self::Idle(mut session) => {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info);
                    let get_more_result = client
                        .execute_operation_with_session(get_more, &mut session)
                        .await;
                    OwnedSessionGetMoreResult {
                        get_more_result,
                        session,
                    }
                });
                Self::Executing(future)
            }
            Self::Executing(_) | Self::Done => self_,
        })
    }
}
