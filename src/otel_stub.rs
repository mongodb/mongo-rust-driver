use crate::{operation::Operation, Client, ClientSession};

#[derive(Clone)]
pub(crate) struct Context;

impl Client {
    pub(crate) fn start_operation_span(
        &self,
        _op: &impl Operation,
        _session: Option<&ClientSession>,
    ) -> Context {
        Context
    }
}

pub(crate) trait OtelFutureStub: Sized {
    fn with_context(self, _ctx: Context) -> Self {
        self
    }

    fn with_current_context(self) -> Self {
        self
    }
}

impl<T: std::future::Future> OtelFutureStub for T {}
