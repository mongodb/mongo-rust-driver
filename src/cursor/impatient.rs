use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bson::{doc, Document};
use derivative::Derivative;
use futures::{future::BoxFuture, stream::Stream};

use super::CursorSpecification;
use crate::{error::Result, operation::GetMore, results::GetMoreResult, Client, RUNTIME};

#[derive(Debug)]
pub struct ImpatientCursor {
    client: Client,
    get_more: GetMore,
    exhausted: bool,
    state: State,
}

/// Describes the current state of the Cursor. If the state is Executing, then a getMore operation
/// is in progress. If the state is Buffer, then there are documents available from the current
/// batch.
#[derive(Derivative)]
#[derivative(Debug)]
enum State {
    Executing(#[derivative(Debug = "ignore")] BoxFuture<'static, Result<GetMoreResult>>),
    Buffer(VecDeque<Document>),
    Exhausted,
}

impl ImpatientCursor {
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

    pub(super) fn exhausted(&self) -> bool {
        self.exhausted
    }
}

impl Drop for ImpatientCursor {
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

impl Stream for ImpatientCursor {
    type Item = Result<Document>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let exhausted = self.exhausted;

        match self.state {
            // If the current state is Executing, then we check the progress of the getMore
            // operation.
            State::Executing(ref mut future) => match Pin::new(future).poll(cx) {
                // If the getMore is finished and successful, then we pop off the first document
                // from the batch, set the poll state to Buffer, record whether the cursor is
                // exhausted, and return the popped document.
                Poll::Ready(Ok(get_more_result)) => {
                    let mut buffer: VecDeque<_> = get_more_result.batch.into_iter().collect();
                    let next_doc = buffer.pop_front();

                    self.state = State::Buffer(buffer);
                    self.exhausted = get_more_result.exhausted;
                    Poll::Ready(next_doc.map(Ok))
                }

                // If the getMore finished with an error, return that error.
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),

                // If the getMore has not finished, keep the state as Executing and return.
                Poll::Pending => Poll::Pending,
            },

            State::Buffer(ref mut buffer) => {
                // If there is a document ready, return it.
                let poll = if let Some(doc) = buffer.pop_front() {
                    Poll::Ready(Some(Ok(doc)))
                // If the cursor is exhausted, return None.
                } else if exhausted {
                    Poll::Ready(None)
                // Since no document is ready and the cursor isn't exhausted, we need to start a new
                // getMore operation, so return that the operation is pending.
                } else {
                    Poll::Pending
                };

                // If no documents are left and the batch and the cursor is exhausted, set the state
                // to None.
                if buffer.is_empty() && exhausted {
                    self.state = State::Exhausted;
                // If the batch is empty and the cursor is not exhausted, start a new operation and
                // set the state to Executing.
                } else if buffer.is_empty() {
                    let future = Box::pin(
                        self.client
                            .clone()
                            .execute_operation_owned(self.get_more.clone()),
                    );

                    self.state = State::Executing(future);
                };

                poll
            }

            // If the state is None, then the cursor has already exhausted all its results, so do
            // nothing.
            State::Exhausted => Poll::Ready(None),
        }
    }
}
