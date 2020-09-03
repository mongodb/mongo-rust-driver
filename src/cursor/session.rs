use std::collections::VecDeque;

use futures::future::BoxFuture;

use super::common::{
    read_exhaust_get_more,
    CursorInformation,
    GenericCursor,
    GetMoreProvider,
    GetMoreProviderResult,
};
use crate::{
    bson::Document,
    client::{ClientSession, OperationResult},
    cmap::Connection,
    cursor::CursorSpecification,
    error::Result,
    operation::GetMore,
    results::GetMoreResult,
    Client,
    RUNTIME,
};

/// A cursor that was started with a session and must be iterated using one.
#[derive(Debug)]
pub(crate) struct SessionCursor {
    exhausted: bool,
    client: Client,
    info: CursorInformation,
    buffer: VecDeque<Document>,
    exhaust_conn: Option<Connection>,
    request_exhaust: bool,
}

impl SessionCursor {
    fn new(
        client: Client,
        spec: OperationResult<CursorSpecification>,
        request_exhaust: bool,
    ) -> Self {
        let exhausted = spec.response.id() == 0;

        Self {
            exhausted,
            client,
            info: spec.response.info,
            buffer: spec.response.initial_buffer,
            exhaust_conn: spec.connection,
            request_exhaust,
        }
    }

    fn with_session<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionCursorHandle<'_, 'session> {
        let get_more_provider = ExplicitSessionGetMoreProvider::new(session, self.request_exhaust);

        // Pass the buffer into this cursor handle for iteration.
        // It will be returned in the handle's `Drop` implementation.
        let spec = CursorSpecification {
            info: self.info.clone(),
            initial_buffer: std::mem::take(&mut self.buffer),
        };
        SessionCursorHandle {
            generic_cursor: ExplicitSessionCursor::new(
                self.client.clone(),
                spec,
                self.exhaust_conn.take(),
                get_more_provider,
            ),
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

/// A `GenericCursor` that borrows its session.
/// This is to be used with cursors associated with explicit sessions borrowed from the user.
type ExplicitSessionCursor<'session> = GenericCursor<ExplicitSessionGetMoreProvider<'session>>;

/// A handle that borrows a `ClientSession` temporarily for executing getMores or iterating through
/// the current buffer.
///
/// This updates the buffer of the parent cursor when dropped.
struct SessionCursorHandle<'cursor, 'session> {
    session_cursor: &'cursor mut SessionCursor,
    generic_cursor: ExplicitSessionCursor<'session>,
}

impl<'cursor, 'session> Drop for SessionCursorHandle<'cursor, 'session> {
    fn drop(&mut self) {
        // Update the parent cursor's state based on any iteration performed on this handle.
        self.session_cursor.buffer = self.generic_cursor.take_buffer();
        self.session_cursor.exhausted = self.generic_cursor.is_exhausted();
        self.session_cursor.exhaust_conn = self.generic_cursor.take_exhaust_conn();
    }
}

/// Enum determining whether a `SessionCursorHandle` is excuting a getMore or not.
/// In charge of maintaining ownership of the session reference.
struct ExplicitSessionGetMoreProvider<'session> {
    state: ExplicitSessionGetMoreProviderState<'session>,
    request_exhaust: bool,
}

enum ExplicitSessionGetMoreProviderState<'session> {
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
    fn new(session: &'session mut ClientSession, request_exhaust: bool) -> Self {
        let state = ExplicitSessionGetMoreProviderState::Idle(MutableSessionReference {
            reference: session,
        });

        Self {
            state,
            request_exhaust,
        }
    }
}

impl<'session> GetMoreProvider for ExplicitSessionGetMoreProvider<'session> {
    type GetMoreResult = ExecutionResult<'session>;
    type GetMoreFuture = BoxFuture<'session, ExecutionResult<'session>>;

    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture> {
        match self.state {
            ExplicitSessionGetMoreProviderState::Executing(ref mut future) => Some(future),
            ExplicitSessionGetMoreProviderState::Idle(_) => None,
        }
    }

    fn clear_execution(&mut self, result: Self::GetMoreResult) {
        self.state = ExplicitSessionGetMoreProviderState::Idle(MutableSessionReference {
            reference: result.session,
        })
    }

    fn start_execution(
        &mut self,
        info: CursorInformation,
        exhaust_conn: Option<Connection>,
        client: Client,
    ) {
        let request_exhaust = self.request_exhaust;

        take_mut::take(&mut self.state, |state| {
            if let ExplicitSessionGetMoreProviderState::Idle(session) = state {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info, request_exhaust);

                    let mut result = if let Some(conn) = exhaust_conn {
                        read_exhaust_get_more(conn).await
                    } else {
                        client
                            .execute_operation_with_session(get_more, session.reference)
                            .await
                    };

                    ExecutionResult {
                        exhaust_conn: result.as_mut().ok().and_then(|r| r.connection.take()),
                        get_more_result: result.map(|r| r.response),
                        session: session.reference,
                    }
                });
                return ExplicitSessionGetMoreProviderState::Executing(future);
            }
            state
        });
    }
}

/// Struct returned from awaiting on a `GetMoreFuture` containing the result of the getMore as
/// well as the reference to the `ClientSession` used for the getMore.
#[derive(Debug)]
struct ExecutionResult<'session> {
    get_more_result: Result<GetMoreResult>,
    session: &'session mut ClientSession,
    exhaust_conn: Option<Connection>,
}

impl<'session> GetMoreProviderResult for ExecutionResult<'session> {
    fn as_mut(&mut self) -> Result<&mut GetMoreResult> {
        self.get_more_result.as_mut().map_err(|e| e.clone())
    }

    fn as_ref(&self) -> Result<&GetMoreResult> {
        self.get_more_result.as_ref().map_err(|e| e.clone())
    }

    fn take_exhaust_conn(&mut self) -> Option<Connection> {
        self.exhaust_conn.take()
    }
}

/// Wrapper around a mutable reference to a `ClientSession` that provides move semantics.
/// This is used to prevent re-borrowing of the session and forcing it to be moved instead
/// by moving the wrapping struct.
struct MutableSessionReference<'a> {
    reference: &'a mut ClientSession,
}
