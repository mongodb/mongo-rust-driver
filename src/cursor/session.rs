use super::common::{CursorInformation, GenericCursor, GetMoreProvider, GetMoreProviderResult};
use crate::{
    client::ClientSession,
    cursor::CursorSpecification,
    error::Result,
    operation::GetMore,
    Client,
    RUNTIME,
};
use bson::Document;
use futures::future::BoxFuture;
use std::collections::VecDeque;

/// A cursor that was started with a session and must be iterated using one.
#[derive(Debug)]
pub(crate) struct SessionCursor {
    exhausted: bool,
    client: Client,
    info: CursorInformation,
    buffer: VecDeque<Document>,
}

impl SessionCursor {
    fn new(client: Client, spec: CursorSpecification) -> Self {
        let exhausted = spec.id() == 0;

        Self {
            exhausted,
            client,
            info: spec.info,
            buffer: spec.initial_buffer,
        }
    }

    fn with_session<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionCursorHandle<'_, 'session> {
        let get_more_provider = ExplicitSessionGetMoreProvider::new(session);

        // Pass the buffer into this cursor handle for iteration.
        // It will be returned in the handle's `Drop` implementation.
        let spec = CursorSpecification {
            info: self.info.clone(),
            initial_buffer: std::mem::take(&mut self.buffer),
        };
        SessionCursorHandle {
            generic_cursor: GenericCursor::new(self.client.clone(), spec, get_more_provider),
            session_cursor: self,
        }
    }
}

impl Drop for SessionCursor {
    fn drop(&mut self) {
        if self.exhausted {
            return;
        }

        let ns = &self.info.ns;
        let coll = self
            .client
            .database(ns.db.as_str())
            .collection(ns.coll.as_str());
        let cursor_id = self.info.id;
        RUNTIME.execute(async move { coll.kill_cursor(cursor_id).await });
    }
}

struct SessionCursorHandle<'cursor, 'session> {
    session_cursor: &'cursor mut SessionCursor,
    generic_cursor: GenericCursor<ExplicitSessionGetMoreProvider<'session>>,
}

impl<'cursor, 'session> Drop for SessionCursorHandle<'cursor, 'session> {
    fn drop(&mut self) {
        // Update the parent cursor's state based on any iteration performed on this handle.
        self.session_cursor.buffer = self.generic_cursor.take_buffer();
        self.session_cursor.exhausted = self.generic_cursor.is_exhausted();
    }
}

/// Enum determining whether a `SessionCursorHandle` is excuting a getMore or not.
/// In charge of maintaining ownership of the session reference.
enum ExplicitSessionGetMoreProvider<'session> {
    /// The handle is currently executing a getMore via the future.
    ///
    /// This future owns the reference to the session and will return it on completion.
    Executing(BoxFuture<'session, ExecutionResult<'session>>),

    /// No future is being executed.
    ///
    /// This variant needs a `MutableSessionReference` struct that can be moved in order to
    /// transition to `Executing` via `take_mut`.
    Idle(MutableSessionReference<'session>),
}

impl<'session> ExplicitSessionGetMoreProvider<'session> {
    fn new(session: &'session mut ClientSession) -> Self {
        Self::Idle(MutableSessionReference { reference: session })
    }
}

impl<'session> GetMoreProvider for ExplicitSessionGetMoreProvider<'session> {
    type GetMoreResult = ExecutionResult<'session>;
    type GetMoreFuture = BoxFuture<'session, ExecutionResult<'session>>;

    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture> {
        match self {
            Self::Executing(future) => Some(future),
            Self::Idle(_) => None,
        }
    }

    fn clear_execution(&mut self, result: Self::GetMoreResult) {
        *self = Self::Idle(MutableSessionReference {
            reference: result.session,
        })
    }

    fn start_execution(&mut self, info: CursorInformation, client: Client) {
        take_mut::take(self, |self_| {
            if let ExplicitSessionGetMoreProvider::Idle(session) = self_ {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info);
                    let get_more_result = client
                        .execute_operation_with_session(get_more, session.reference)
                        .await;
                    ExecutionResult {
                        get_more_result,
                        session: session.reference,
                    }
                });
                return ExplicitSessionGetMoreProvider::Executing(future);
            }
            self_
        });
    }
}

/// Struct returned from awaiting on a `GetMoreFuture` containing the result of the getMore as
/// well as the reference to the `ClientSession` used for the getMore.
struct ExecutionResult<'session> {
    get_more_result: Result<crate::results::GetMoreResult>,
    session: &'session mut ClientSession,
}

impl<'session> GetMoreProviderResult for ExecutionResult<'session> {
    fn as_mut(&mut self) -> Result<&mut crate::results::GetMoreResult> {
        self.get_more_result.as_mut().map_err(|e| e.clone())
    }

    fn as_ref(&self) -> Result<&crate::results::GetMoreResult> {
        self.get_more_result.as_ref().map_err(|e| e.clone())
    }
}

/// Wrapper around a mutable reference to a `ClientSession` that provides move semantics.
/// This is required
struct MutableSessionReference<'a> {
    reference: &'a mut ClientSession,
}
