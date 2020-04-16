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

#[derive(Debug)]
pub(crate) struct CursorSpecification {
    pub(crate) ns: Namespace,
    pub(crate) address: StreamAddress,
    pub(crate) id: i64,
    pub(crate) batch_size: Option<u32>,
    pub(crate) max_time: Option<Duration>,
    pub(crate) buffer: VecDeque<Document>,
}

#[derive(Debug)]
pub struct Cursor {
    client: Client,
    get_more: GetMore,
    exhausted: bool,
    state: State,
}

type GetMoreFuture = BoxFuture<'static, Result<GetMoreResult>>;

/// Describes the current state of the Cursor. If the state is Executing, then a getMore operation
/// is in progress. If the state is Buffer, then there are documents available from the current
/// batch.
#[derive(Derivative)]
#[derivative(Debug)]
enum State {
    Executing(#[derivative(Debug = "ignore")] GetMoreFuture),
    Buffer(VecDeque<Document>),
    Exhausted,
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
            state: State::Buffer(spec.buffer),
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
        loop {
            match self.state {
                // If the current state is Executing, then we check the progress of the getMore
                // operation.
                State::Executing(ref mut future) => {
                    match Pin::new(future).poll(cx) {
                        // If the getMore is finished and successful, then we pop off the first
                        // document from the batch, set the poll state to
                        // Buffer, record whether the cursor is exhausted,
                        // and return the popped document.
                        Poll::Ready(Ok(get_more_result)) => {
                            let mut buffer: VecDeque<_> =
                                get_more_result.batch.into_iter().collect();
                            let next_doc = buffer.pop_front();

                            self.state = State::Buffer(buffer);
                            self.exhausted = get_more_result.exhausted;

                            match next_doc {
                                Some(doc) => return Poll::Ready(Some(Ok(doc))),
                                None => continue,
                            }
                        }

                        // If the getMore finished with an error, return that error.
                        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),

                        // If the getMore has not finished, keep the state as Executing and return.
                        Poll::Pending => return Poll::Pending,
                    }
                }

                State::Buffer(ref mut buffer) => {
                    // If there is a document ready, return it.
                    if let Some(doc) = buffer.pop_front() {
                        return Poll::Ready(Some(Ok(doc)));
                    }

                    // If no documents are left and the batch and the cursor is exhausted, set the
                    // state to None.
                    if self.exhausted {
                        self.state = State::Exhausted;
                        return Poll::Ready(None);
                    // If the batch is empty and the cursor is not exhausted, start a new operation
                    // and set the state to Executing.
                    } else {
                        let future = Box::pin(
                            self.client
                                .clone()
                                .execute_operation_owned(self.get_more.clone()),
                        );

                        self.state = State::Executing(future as GetMoreFuture);
                        continue;
                    }
                }

                // If the state is None, then the cursor has already exhausted all its results, so
                // do nothing.
                State::Exhausted => return Poll::Ready(None),
            }
        }
    }
}
