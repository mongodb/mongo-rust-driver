use crate::{
    bson::{doc, Document},
    bson_compat::{cstr, cstr_to_str, CStr},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, ExecutionContext, OperationWithDefaults},
    stream_processing::options::CreateStreamProcessorOptions,
    Client,
};

#[derive(Debug)]
pub(crate) struct CreateStreamProcessor {
    client: Client,
    name: String,
    pipeline: Vec<Document>,
    options: Option<CreateStreamProcessorOptions>,
}

impl CreateStreamProcessor {
    pub(crate) fn new(
        client: Client,
        name: String,
        pipeline: Vec<Document>,
        options: Option<CreateStreamProcessorOptions>,
    ) -> Self {
        Self {
            client,
            name,
            pipeline,
            options,
        }
    }
}

impl OperationWithDefaults for CreateStreamProcessor {
    type O = ();

    const NAME: &'static CStr = cstr!("createStreamProcessor");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            cstr_to_str(Self::NAME): self.name.as_str(),
            "pipeline": bson_util::to_bson_array(&self.pipeline),
        };

        if let Some(opts) = self.options.as_ref() {
            let mut sub = Document::new();
            append_options(&mut sub, Some(opts))?;
            if !sub.is_empty() {
                body.insert("options", sub);
            }
        }

        Ok(Command::from_operation(self, (&body).try_into()?))
    }

    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        // { ok: 1 }
        Ok(())
    }

    fn target(&self) -> super::super::OperationTarget {
        super::super::OperationTarget::admin(&self.client)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for CreateStreamProcessor {}
