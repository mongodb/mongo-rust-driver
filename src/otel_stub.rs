pub(crate) trait OtelFutureStub: Sized {
    fn with_current_context(self) -> Self {
        self
    }
}

impl<T: std::future::Future> OtelFutureStub for T {}
