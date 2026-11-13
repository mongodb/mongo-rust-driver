use serde::Deserialize;

use crate::{
    bson::rawdoc,
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{ExecutionContext, OperationWithDefaults},
    Client,
};

/// Returns the int64 `cursorId` used by `getMoreSampleStreamProcessor` to
/// retrieve the first batch of sampled documents. `startSampleStreamProcessor`
/// itself does not return any documents.
#[derive(Debug)]
pub(crate) struct StartSampleStreamProcessor {
    client: Client,
    name: String,
    limit: Option<i32>,
}

impl StartSampleStreamProcessor {
    pub(crate) fn new(client: Client, name: String, limit: Option<i32>) -> Self {
        Self {
            client,
            name,
            limit,
        }
    }
}

#[derive(Deserialize)]
struct Response {
    #[serde(rename = "cursorId")]
    cursor_id: i64,
}

impl OperationWithDefaults for StartSampleStreamProcessor {
    type O = i64;

    const NAME: &'static CStr = cstr!("startSampleStreamProcessor");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! { Self::NAME: self.name.as_str() };
        if let Some(limit) = self.limit {
            body.append(cstr!("limit"), limit);
        }
        Ok(Command::from_operation(self, body))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let r: Response = response.body()?;
        Ok(r.cursor_id)
    }

    fn target(&self) -> super::super::OperationTarget {
        super::super::OperationTarget::admin(&self.client)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for StartSampleStreamProcessor {}
