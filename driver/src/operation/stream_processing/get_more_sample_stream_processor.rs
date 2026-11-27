use crate::{
    bson::{rawdoc, Bson, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    operation::{ExecutionContext, OperationWithDefaults},
    stream_processing::types::StreamProcessorSamples,
    Client,
};

/// Retrieves the next batch from a sample cursor. MUST be called with the
/// `cursor_id` returned by `startSampleStreamProcessor` (or a prior call to
/// this command).
#[derive(Debug)]
pub(crate) struct GetMoreSampleStreamProcessor {
    client: Client,
    name: String,
    cursor_id: i64,
    batch_size: Option<i32>,
}

impl GetMoreSampleStreamProcessor {
    pub(crate) fn new(
        client: Client,
        name: String,
        cursor_id: i64,
        batch_size: Option<i32>,
    ) -> Self {
        Self {
            client,
            name,
            cursor_id,
            batch_size,
        }
    }
}

impl OperationWithDefaults for GetMoreSampleStreamProcessor {
    type O = StreamProcessorSamples;

    const NAME: &'static CStr = cstr!("getMoreSampleStreamProcessor");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.name.as_str(),
            "cursorId": self.cursor_id,
        };
        if let Some(bs) = self.batch_size {
            body.append(cstr!("batchSize"), bs);
        }
        Ok(Command::from_operation(self, body))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let mut doc: Document = response.body()?;

        let cursor_id = match doc.remove("cursorId") {
            Some(Bson::Int64(n)) => n,
            Some(Bson::Int32(n)) => n as i64,
            _ => {
                return Err(Error::custom(
                    "getMoreSampleStreamProcessor response missing cursorId",
                ))
            }
        };

        // Dev-server deviation: some server builds use "messages" instead of
        // "nextBatch". Prefer the spec-defined "nextBatch" but fall back to
        // "messages" when present.
        let batch_bson = doc.remove("nextBatch").or_else(|| doc.remove("messages"));
        let documents = match batch_bson {
            Some(Bson::Array(arr)) => arr
                .into_iter()
                .filter_map(|b| match b {
                    Bson::Document(d) => Some(d),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };

        Ok(StreamProcessorSamples {
            cursor_id,
            documents,
        })
    }

    fn target(&self) -> super::super::OperationTarget {
        super::super::OperationTarget::admin(&self.client)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for GetMoreSampleStreamProcessor {}
