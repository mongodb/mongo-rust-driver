use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{ExecutionContext, OperationWithDefaults, Retryability},
    options::ClientOptions,
    stream_processing::types::StreamProcessorInfo,
    Client,
};

#[derive(Debug)]
pub(crate) struct GetStreamProcessor {
    client: Client,
    name: String,
}

impl GetStreamProcessor {
    pub(crate) fn new(client: Client, name: String) -> Self {
        Self { client, name }
    }
}

impl OperationWithDefaults for GetStreamProcessor {
    type O = StreamProcessorInfo;

    const NAME: &'static CStr = cstr!("getStreamProcessor");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        Ok(Command::from_operation(
            self,
            rawdoc! { Self::NAME: self.name.as_str() },
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        // Dev-server deviation: some server builds wrap the processor document
        // in a top-level "result" key. Decode the response as a Document first
        // so we can unwrap it before deserializing into StreamProcessorInfo.
        let mut doc: Document = response.body()?;
        if let Some(crate::bson::Bson::Document(inner)) = doc.remove("result") {
            doc = inner;
        }

        let info: StreamProcessorInfo = crate::bson_compat::deserialize_from_document(doc)?;
        Ok(info)
    }

    fn retryability(&self, options: &ClientOptions) -> Retryability {
        Retryability::read(options)
    }

    fn target(&self) -> super::super::OperationTarget {
        super::super::OperationTarget::admin(&self.client)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for GetStreamProcessor {}
