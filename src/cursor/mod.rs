mod common;
// TODO: RUST-52 use this
#[allow(dead_code)]
mod session;
#[cfg(test)]
mod test;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{future::BoxFuture, Stream};
use serde::de::DeserializeOwned;

use crate::{
    bson::{from_document, Document},
    client::{ClientSession, CursorResponse},
    cmap::Connection,
    error::Result,
    operation::GetMore,
    results::GetMoreResult,
    Client,
    RUNTIME,
};
use common::{read_exhaust_get_more, GenericCursor, GetMoreProvider, GetMoreProviderResult};
pub(crate) use common::{CursorInformation, CursorSpecification};

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
/// # use futures::stream::StreamExt;
/// # use mongodb::{
/// #     bson::{doc, Document},
/// #     error::Result,
/// #     Client,
/// # };
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
pub struct Cursor<T = Document>
where
    T: DeserializeOwned + Unpin,
{
    client: Client,
    wrapped_cursor: ImplicitSessionCursor,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Cursor<T>
where
    T: DeserializeOwned + Unpin,
{
    pub(crate) fn new(
        client: Client,
        spec: CursorResponse<CursorSpecification>,
        request_exhaust: bool,
    ) -> Self {
        let provider =
            ImplicitSessionGetMoreProvider::new(&spec.response, spec.session, request_exhaust);

        Self {
            client: client.clone(),

            wrapped_cursor: ImplicitSessionCursor::new(
                client,
                spec.response,
                spec.connection,
                provider,
            ),

            _phantom: Default::default(),
        }
    }
}

impl<T> Stream for Cursor<T>
where
    T: DeserializeOwned + Unpin,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = Pin::new(&mut self.wrapped_cursor).poll_next(cx);
        match next {
            Poll::Ready(opt) => Poll::Ready(
                opt.map(|result| result.and_then(|doc| from_document(doc).map_err(Into::into))),
            ),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for Cursor<T>
where
    T: DeserializeOwned + Unpin,
{
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

/// A `GenericCursor` that optionally owns its own sessions.
/// This is to be used by cursors associated with implicit sessions.
type ImplicitSessionCursor = GenericCursor<ImplicitSessionGetMoreProvider>;

#[derive(Debug)]
struct ImplicitSessionGetMoreResult {
    get_more_result: Result<GetMoreResult>,
    session: Option<ClientSession>,
    exhaust_cursor: Option<Connection>,
}

impl GetMoreProviderResult for ImplicitSessionGetMoreResult {
    fn as_mut(&mut self) -> Result<&mut GetMoreResult> {
        self.get_more_result.as_mut().map_err(|e| e.clone())
    }

    fn as_ref(&self) -> Result<&GetMoreResult> {
        self.get_more_result.as_ref().map_err(|e| e.clone())
    }

    fn take_exhaust_conn(&mut self) -> Option<Connection> {
        self.exhaust_cursor.take()
    }
}

/// A `GetMoreProvider` that optionally owns its own session.
/// This is to be used with cursors associated with implicit sessions.
struct ImplicitSessionGetMoreProvider {
    state: ImplicitSessionGetMoreProviderState,
    request_exhaust: bool,
}

enum ImplicitSessionGetMoreProviderState {
    Executing(BoxFuture<'static, ImplicitSessionGetMoreResult>),
    Idle(Option<ClientSession>),
    Done,
}

impl ImplicitSessionGetMoreProvider {
    fn new(
        spec: &CursorSpecification,
        session: Option<ClientSession>,
        request_exhaust: bool,
    ) -> Self {
        let state = if spec.id() == 0 {
            ImplicitSessionGetMoreProviderState::Done
        } else {
            ImplicitSessionGetMoreProviderState::Idle(session)
        };

        Self {
            state,
            request_exhaust,
        }
    }
}

impl GetMoreProvider for ImplicitSessionGetMoreProvider {
    type GetMoreResult = ImplicitSessionGetMoreResult;
    type GetMoreFuture = BoxFuture<'static, ImplicitSessionGetMoreResult>;

    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture> {
        match self.state {
            ImplicitSessionGetMoreProviderState::Executing(ref mut future) => Some(future),
            ImplicitSessionGetMoreProviderState::Idle(_)
            | ImplicitSessionGetMoreProviderState::Done => None,
        }
    }

    fn clear_execution(&mut self, result: Self::GetMoreResult) {
        // If cursor is exhausted, immediately return implicit session to the pool.
        if result.get_more_result.map(|r| r.exhausted).unwrap_or(false) {
            self.state = ImplicitSessionGetMoreProviderState::Done;
        } else {
            self.state = ImplicitSessionGetMoreProviderState::Idle(result.session)
        }
    }

    fn start_execution(
        &mut self,
        info: CursorInformation,
        exhaust_conn: Option<Connection>,
        client: Client,
    ) {
        let request_exhaust = self.request_exhaust;

        take_mut::take(&mut self.state, |state| match state {
            ImplicitSessionGetMoreProviderState::Idle(mut session) => {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info, request_exhaust);

                    let mut result = match (exhaust_conn, &mut session) {
                        // Since we don't send any commands in the case of an exhaust cursor, the
                        // presence or absence of a session is irrelevant.
                        (Some(conn), _) => read_exhaust_get_more(conn).await,
                        (None, Some(session)) => {
                            client
                                .execute_operation_with_session(get_more, session)
                                .await
                        }
                        (None, None) => client.execute_cursor_operation(get_more).await,
                    };

                    ImplicitSessionGetMoreResult {
                        exhaust_cursor: result.as_mut().ok().and_then(|r| r.connection.take()),
                        get_more_result: result.map(|r| r.response),
                        session,
                    }
                });
                ImplicitSessionGetMoreProviderState::Executing(future)
            }
            ImplicitSessionGetMoreProviderState::Executing(_)
            | ImplicitSessionGetMoreProviderState::Done => state,
        })
    }
}
