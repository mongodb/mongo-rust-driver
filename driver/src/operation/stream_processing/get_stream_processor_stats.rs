use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{ExecutionContext, OperationWithDefaults, Retryability},
    options::ClientOptions,
    stream_processing::options::GetStreamProcessorStatsOptions,
    Client,
};

#[derive(Debug)]
pub(crate) struct GetStreamProcessorStats {
    client: Client,
    name: String,
    options: Option<GetStreamProcessorStatsOptions>,
}

impl GetStreamProcessorStats {
    pub(crate) fn new(
        client: Client,
        name: String,
        options: Option<GetStreamProcessorStatsOptions>,
    ) -> Self {
        Self {
            client,
            name,
            options,
        }
    }
}

impl OperationWithDefaults for GetStreamProcessorStats {
    type O = Document;

    const NAME: &'static CStr = cstr!("getStreamProcessorStats");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! { Self::NAME: self.name.as_str() };

        if let Some(opts) = self.options.as_ref() {
            if let Some(verbose) = opts.verbose {
                body.append(cstr!("options"), rawdoc! { "verbose": verbose });
            }
        }

        Ok(Command::from_operation(self, body))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let doc: Document = response.body()?;
        Ok(doc)
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
impl crate::otel::OtelInfoDefaults for GetStreamProcessorStats {}
