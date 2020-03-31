mod impatient;

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bson::Document;
use futures::stream::Stream;

use crate::{error::Result, options::StreamAddress, Client, Namespace};
pub use impatient::ImpatientCursor;

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
    inner: ImpatientCursor,
    tailable: bool,
}

impl Cursor {
    pub(crate) fn new(client: Client, spec: CursorSpecification, tailable: bool) -> Self {
        Self {
            inner: ImpatientCursor::new(client, spec),
            tailable,
        }
    }
}

impl Stream for Cursor {
    type Item = Result<Document>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.tailable {
            loop {
                if let Poll::Ready(Some(result)) = Pin::new(&mut self.inner).poll_next(cx) {
                    return Poll::Ready(Some(result));
                }
            }
        } else {
            Pin::new(&mut self.inner).poll_next(cx)
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
