use std::time::Duration;

use crate::{
    bson::{rawdoc, RawBson},
    bson_compat::{cstr, CStr},
};

use crate::{
    bson::{Bson, RawDocumentBuf},
    change_stream::event::ResumeToken,
    checked::Checked,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    cursor::CursorInformation,
    error::Result,
    operation::OperationWithDefaults,
    options::SelectionCriteria,
    results::GetMoreResult,
    Namespace,
};

use super::ExecutionContext;

#[derive(Debug)]
pub(crate) struct GetMore<'conn> {
    ns: Namespace,
    cursor_id: i64,
    selection_criteria: SelectionCriteria,
    batch_size: Option<u32>,
    max_time: Option<Duration>,
    pinned_connection: Option<&'conn PinnedConnectionHandle>,
    comment: Option<Bson>,
}

impl<'conn> GetMore<'conn> {
    pub(crate) fn new(
        info: CursorInformation,
        pinned: Option<&'conn PinnedConnectionHandle>,
    ) -> Self {
        Self {
            ns: info.ns,
            cursor_id: info.id,
            selection_criteria: SelectionCriteria::from_address(info.address),
            batch_size: info.batch_size,
            max_time: info.max_time,
            pinned_connection: pinned,
            comment: info.comment,
        }
    }
}

impl OperationWithDefaults for GetMore<'_> {
    type O = GetMoreResult;

    const NAME: &'static CStr = cstr!("getMore");

    const ZERO_COPY: bool = true;

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.cursor_id,
            "collection": self.ns.coll.clone(),
        };

        if let Some(batch_size) = self.batch_size {
            let batch_size = Checked::from(batch_size).try_into::<i32>()?;
            if batch_size != 0 {
                body.append(cstr!("batchSize"), batch_size);
            }
        }

        if let Some(ref max_time) = self.max_time {
            body.append(
                cstr!("maxTimeMS"),
                max_time.as_millis().try_into().unwrap_or(i32::MAX),
            );
        }

        if let Some(comment) = &self.comment {
            let raw_comment: RawBson = comment.clone().try_into()?;
            body.append(cstr!("comment"), raw_comment);
        }

        Ok(Command::new(Self::NAME, &self.ns.db, body))
    }

    fn handle_response_owned<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        // Extract minimal fields directly from the raw reply to avoid walking the batch via serde.
        let root = response.raw_body();
        let cursor = root
            .get("cursor")?
            .and_then(crate::bson::RawBsonRef::as_document)
            .ok_or_else(|| crate::error::Error::invalid_response("missing cursor subdocument"))?;

        let id = cursor
            .get("id")?
            .and_then(crate::bson::RawBsonRef::as_i64)
            .ok_or_else(|| crate::error::Error::invalid_response("missing cursor id"))?;

        let ns_str = cursor
            .get("ns")?
            .and_then(crate::bson::RawBsonRef::as_str)
            .ok_or_else(|| crate::error::Error::invalid_response("missing cursor ns"))?;
        let ns = Namespace::from_str(ns_str)
            .ok_or_else(|| crate::error::Error::invalid_response("invalid cursor ns"))?;

        let token_raw = cursor
            .get("postBatchResumeToken")?
            .and_then(crate::bson::RawBsonRef::as_document)
            .map(|d| RawDocumentBuf::from_bytes(d.as_bytes().to_vec()))
            .transpose()?;
        let post_batch_resume_token = ResumeToken::from_raw(token_raw);

        // Take ownership of the raw bytes without copying.
        let raw = response.into_raw_document_buf();

        Ok(GetMoreResult {
            raw_reply: raw,
            exhausted: id == 0,
            post_batch_resume_token,
            ns,
            id,
        })
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(&self.selection_criteria)
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for GetMore<'_> {
    fn target(&self) -> crate::otel::OperationTarget<'_> {
        (&self.ns).into()
    }

    #[cfg(feature = "opentelemetry")]
    fn cursor_id(&self) -> Option<i64> {
        Some(self.cursor_id)
    }
}

/*
#[derive(Debug, Deserialize)]
pub(crate) struct GetMoreResponseBody {
    cursor: NextBatchBody,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NextBatchBody {
    id: i64,
    next_batch: VecDeque<RawDocumentBuf>,
    post_batch_resume_token: Option<RawDocumentBuf>,
    ns: String,
}
*/
