use std::{collections::VecDeque, time::Duration};

use bson::{rawdoc, RawBson};
use serde::Deserialize;

use crate::{
    bson::{doc, Bson, RawDocumentBuf},
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

impl<'conn> OperationWithDefaults for GetMore<'conn> {
    type O = GetMoreResult;

    const NAME: &'static str = "getMore";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.cursor_id,
            "collection": self.ns.coll.clone(),
        };

        if let Some(batch_size) = self.batch_size {
            let batch_size = Checked::from(batch_size).try_into::<i32>()?;
            if batch_size != 0 {
                body.append("batchSize", batch_size);
            }
        }

        if let Some(ref max_time) = self.max_time {
            body.append(
                "maxTimeMS",
                max_time.as_millis().try_into().unwrap_or(i32::MAX),
            );
        }

        if let Some(comment) = &self.comment {
            let raw_comment: RawBson = comment.clone().try_into()?;
            body.append("comment", raw_comment);
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: GetMoreResponseBody = response.body()?;

        Ok(GetMoreResult {
            batch: response.cursor.next_batch,
            exhausted: response.cursor.id == 0,
            post_batch_resume_token: ResumeToken::from_raw(response.cursor.post_batch_resume_token),
            id: response.cursor.id,
            ns: Namespace::from_str(response.cursor.ns.as_str()).unwrap(),
        })
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(&self.selection_criteria)
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection
    }
}

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
