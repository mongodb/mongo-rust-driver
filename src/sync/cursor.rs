use futures::StreamExt;
use serde::de::DeserializeOwned;

use crate::{
    bson::Document,
    error::Result,
    ClientSession,
    Cursor as AsyncCursor,
    SessionCursor as AsyncSessionCursor,
    SessionCursorHandle as AsyncSessionCursorHandle,
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
/// # use mongodb::{bson::Document, sync::Client, error::Result};
/// #
/// # fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// # let coll = client.database("foo").collection::<Document>("bar");
/// # let mut cursor = coll.find(None, None)?;
/// #
/// for doc in cursor {
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
/// # use mongodb::{
/// #     bson::{doc, Document},
/// #     error::Result,
/// #     sync::Client,
/// # };
/// #
/// # fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// # let coll = client.database("foo").collection("bar");
/// # let cursor = coll.find(Some(doc! { "x": 1 }), None)?;
/// #
/// let results: Vec<Result<Document>> = cursor.collect();
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Cursor<T>
where
    T: DeserializeOwned + Unpin + Send,
{
    async_cursor: AsyncCursor<T>,
}

impl<T> Cursor<T>
where
    T: DeserializeOwned + Unpin + Send,
{
    pub(crate) fn new(async_cursor: AsyncCursor<T>) -> Self {
        Self { async_cursor }
    }
}

impl<T> Iterator for Cursor<T>
where
    T: DeserializeOwned + Unpin + Send,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        RUNTIME.block_on(self.async_cursor.next())
    }
}

/// A `SessionCursor` is a cursor that was created with a `ClientSession` must be iterated using
/// one. To iterate, retrieve a `SessionCursorHandle` using `SessionCursor::with_session`:
///
/// ```rust
/// # use mongodb::{bson::Document, sync::Client, error::Result};
/// #
/// # fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// # let mut session = client.start_session(None)?;
/// # let coll = client.database("foo").collection::<Document>("bar");
/// # let mut cursor = coll.find_with_session(None, None, &mut session)?;
/// #
/// for doc in cursor.with_session(&mut session) {
///   println!("{}", doc?)
/// }
/// #
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SessionCursor<T>
where
    T: DeserializeOwned + Unpin + Send,
{
    async_cursor: AsyncSessionCursor<T>,
}

impl<T> SessionCursor<T>
where
    T: DeserializeOwned + Unpin + Send,
{
    pub(crate) fn new(async_cursor: AsyncSessionCursor<T>) -> Self {
        Self { async_cursor }
    }

    /// Retrieves a `SessionCursorHandle` to iterate this cursor. The session provided must be the
    /// same session used to create the cursor.
    pub fn with_session<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionCursorHandle<'_, 'session, T> {
        SessionCursorHandle {
            async_handle: self.async_cursor.with_session(session),
        }
    }
}

/// A handle that borrows a `ClientSession` temporarily for executing getMores or iterating through
/// the current buffer of a `SessionCursor`.
///
/// This updates the buffer of the parent `SessionCursor` when dropped.
pub struct SessionCursorHandle<'cursor, 'session, T = Document>
where
    T: DeserializeOwned + Unpin + Send,
{
    async_handle: AsyncSessionCursorHandle<'cursor, 'session, T>,
}

impl<T> Iterator for SessionCursorHandle<'_, '_, T>
where
    T: DeserializeOwned + Unpin + Send,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        RUNTIME.block_on(self.async_handle.next())
    }
}
