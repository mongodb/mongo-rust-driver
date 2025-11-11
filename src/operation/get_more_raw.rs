use crate::bson::RawDocumentBuf;
use std::time::Duration;

use crate::{
    bson::{rawdoc, RawBson},
    bson_compat::{cstr, CStr},
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    cursor::CursorInformation,
    error::Result,
    operation::OperationWithDefaults,
    options::SelectionCriteria,
    Namespace,
};

use super::ExecutionContext;

#[derive(Debug)]
pub(crate) struct GetMoreRaw<'conn> {
    ns: Namespace,
    cursor_id: i64,
    selection_criteria: SelectionCriteria,
    batch_size: Option<u32>,
    max_time: Option<Duration>,
    pinned_connection: Option<&'conn PinnedConnectionHandle>,
    comment: Option<crate::bson::Bson>,
}

impl<'conn> GetMoreRaw<'conn> {
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

#[derive(Debug, Clone)]
pub(crate) struct GetMoreRawResult {
    pub(crate) raw_reply: RawDocumentBuf,
    pub(crate) exhausted: bool,
    pub(crate) post_batch_resume_token: Option<crate::change_stream::event::ResumeToken>,
    pub(crate) ns: Namespace,
    pub(crate) id: i64,
}

impl OperationWithDefaults for GetMoreRaw<'_> {
    type O = GetMoreRawResult;

    const NAME: &'static CStr = cstr!("getMore");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.cursor_id,
            "collection": self.ns.coll.clone(),
        };

        if let Some(batch_size) = self.batch_size {
            let batch_size = crate::checked::Checked::from(batch_size).try_into::<i32>()?;
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

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        // Copy raw reply for the batch.
        let raw = RawDocumentBuf::from_bytes(response.as_bytes().to_vec())?;

        // Parse minimal cursor fields from the raw reply.
        #[derive(serde::Deserialize)]
        struct HelperNs {
            cursor: HelperCursor,
        }
        #[derive(serde::Deserialize)]
        struct HelperCursor {
            id: i64,
            ns: String,
            #[serde(rename = "postBatchResumeToken")]
            post_batch_resume_token: Option<RawDocumentBuf>,
        }
        let helper: HelperNs = crate::bson_compat::deserialize_from_slice(response.as_bytes())?;

        let exhausted = helper.cursor.id == 0;
        let ns = Namespace::from_str(helper.cursor.ns.as_str()).unwrap();
        let token = crate::change_stream::event::ResumeToken::from_raw(
            helper.cursor.post_batch_resume_token,
        );

        Ok(GetMoreRawResult {
            raw_reply: raw,
            exhausted,
            post_batch_resume_token: token,
            ns,
            id: helper.cursor.id,
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
