use crate::{
    bson::rawdoc,
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{ExecutionContext, OperationWithDefaults},
    Client,
};

#[derive(Debug)]
pub(crate) struct DropStreamProcessor {
    client: Client,
    name: String,
}

impl DropStreamProcessor {
    pub(crate) fn new(client: Client, name: String) -> Self {
        Self { client, name }
    }
}

impl OperationWithDefaults for DropStreamProcessor {
    type O = ();

    const NAME: &'static CStr = cstr!("dropStreamProcessor");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        Ok(Command::from_operation(
            self,
            rawdoc! { Self::NAME: self.name.as_str() },
        ))
    }

    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(())
    }

    fn target(&self) -> super::super::OperationTarget {
        super::super::OperationTarget::admin(&self.client)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for DropStreamProcessor {}
