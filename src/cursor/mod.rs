mod common;
pub(crate) mod session;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{future::BoxFuture, Stream};
use serde::de::DeserializeOwned;

use crate::{
    cmap::conn::{PinHandle},
    error::{Error, Result},
    operation::GetMore,
    results::GetMoreResult,
    Client,
    ClientSession,
};
use common::{kill_cursor, GenericCursor, GetMoreProvider, GetMoreProviderResult};
pub(crate) use common::{CursorInformation, CursorSpecification, PinnedConnection};

/// A [`Cursor`] streams the result of a query. When a query is made, the returned [`Cursor`] will
/// contain the first batch of results from the server; the individual results will then be returned
/// as the [`Cursor`] is iterated. When the batch is exhausted and if there are more results, the
/// [`Cursor`] will fetch the next batch of documents, and so forth until the results are exhausted.
/// Note that because of this batching, additional network I/O may occur on any given call to
/// `next`. Because of this, a [`Cursor`] iterates over `Result<T>` items rather than
/// simply `T` items.
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
/// [`Cursor`] implements [`Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html), which means
/// it can be iterated over much in the same way that an `Iterator` can be in synchronous Rust. In
/// order to do so, the [`StreamExt`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html) trait must
/// be imported. Because a [`Cursor`] iterates over a `Result<T>`, it also has access to the
/// potentially more ergonomic functionality provided by
/// [`TryStreamExt`](https://docs.rs/futures/latest/futures/stream/trait.TryStreamExt.html), which can be
/// imported instead of or in addition to
/// [`StreamExt`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html). The methods from
/// [`TryStreamExt`](https://docs.rs/futures/latest/futures/stream/trait.TryStreamExt.html) are especially useful when
/// used in conjunction with the `?` operator.
///
/// ```rust
/// # use mongodb::{bson::Document, Client, error::Result};
/// #
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let coll = client.database("foo").collection::<Document>("bar");
/// #
/// use futures::stream::{StreamExt, TryStreamExt};
///
/// let mut cursor = coll.find(None, None).await?;
/// // regular Stream uses next() and iterates over Option<Result<T>>
/// while let Some(doc) = cursor.next().await {
///   println!("{}", doc?)
/// }
/// // regular Stream uses collect() and collects into a Vec<Result<T>>
/// let v: Vec<Result<_>> = cursor.collect().await;
///
/// let mut cursor = coll.find(None, None).await?;
/// // TryStream uses try_next() and iterates over Result<Option<T>>
/// while let Some(doc) = cursor.try_next().await? {
///   println!("{}", doc)
/// }
/// // TryStream uses try_collect() and collects into a Result<Vec<T>>
/// let v: Vec<_> = cursor.try_collect().await?;
/// #
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Cursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    client: Client,
    wrapped_cursor: ImplicitSessionCursor<T>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Cursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub(crate) fn new(
        client: Client,
        spec: CursorSpecification<T>,
        session: Option<ClientSession>,
        pin: Option<PinHandle>,
    ) -> Self {
        let provider = ImplicitSessionGetMoreProvider::new(&spec, session);

        Self {
            client: client.clone(),
            wrapped_cursor: ImplicitSessionCursor::new(
                client,
                spec,
                PinnedConnection::new(pin),
                provider,
            ),
            _phantom: Default::default(),
        }
    }
}

impl<T> Stream for Cursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.wrapped_cursor).poll_next(cx)
    }
}

impl<T> Drop for Cursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    fn drop(&mut self) {
        if self.wrapped_cursor.is_exhausted() {
            return;
        }

        kill_cursor(
            self.client.clone(),
            self.wrapped_cursor.namespace(),
            self.wrapped_cursor.id(),
            self.wrapped_cursor.pinned_connection().clone(),
        );
    }
}

/// A `GenericCursor` that optionally owns its own sessions.
/// This is to be used by cursors associated with implicit sessions.
type ImplicitSessionCursor<T> = GenericCursor<ImplicitSessionGetMoreProvider<T>, T>;

struct ImplicitSessionGetMoreResult<T> {
    get_more_result: Result<GetMoreResult<T>>,
    session: Option<Box<ClientSession>>,
}

impl<T> GetMoreProviderResult for ImplicitSessionGetMoreResult<T> {
    type Session = Option<Box<ClientSession>>;
    type DocumentType = T;

    fn as_ref(&self) -> std::result::Result<&GetMoreResult<T>, &Error> {
        self.get_more_result.as_ref()
    }

    fn into_parts(self) -> (Result<GetMoreResult<T>>, Self::Session) {
        (self.get_more_result, self.session)
    }
}

/// A `GetMoreProvider` that optionally owns its own session.
/// This is to be used with cursors associated with implicit sessions.
enum ImplicitSessionGetMoreProvider<T> {
    Executing(BoxFuture<'static, ImplicitSessionGetMoreResult<T>>),
    Idle(Option<Box<ClientSession>>),
    Done,
}

impl<T> ImplicitSessionGetMoreProvider<T> {
    fn new(spec: &CursorSpecification<T>, session: Option<ClientSession>) -> Self {
        if spec.id() == 0 {
            Self::Done
        } else {
            Self::Idle(session.map(Box::new))
        }
    }
}

impl<T: Send + Sync + DeserializeOwned> GetMoreProvider for ImplicitSessionGetMoreProvider<T> {
    type DocumentType = T;
    type ResultType = ImplicitSessionGetMoreResult<T>;
    type GetMoreFuture = BoxFuture<'static, ImplicitSessionGetMoreResult<T>>;

    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture> {
        match self {
            Self::Executing(ref mut future) => Some(future),
            Self::Idle { .. } | Self::Done => None,
        }
    }

    fn clear_execution(&mut self, session: Option<Box<ClientSession>>, exhausted: bool) {
        // If cursor is exhausted, immediately return implicit session to the pool.
        if exhausted {
            *self = Self::Done;
        } else {
            *self = Self::Idle(session);
        }
    }

    fn start_execution(
        &mut self,
        info: CursorInformation,
        client: Client,
        pinned_connection: Option<&PinHandle>,
    ) {
        take_mut::take(self, |self_| match self_ {
            Self::Idle(mut session) => {
                let pinned_connection = pinned_connection.cloned();
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info, pinned_connection.as_ref());
                    let get_more_result = client
                        .execute_operation(
                            get_more,
                            session.as_mut().map(|b| b.as_mut()),
                        )
                        .await;
                    ImplicitSessionGetMoreResult {
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
