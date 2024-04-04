use std::{collections::VecDeque, time::Duration};

use bson::{Document, RawDocumentBuf};
use serde::Deserialize;

use crate::{
    bson::{doc, Bson},
    change_stream::event::ResumeToken,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    cursor::CursorInformation,
    error::{ErrorKind, Result},
    operation::OperationWithDefaults,
    options::SelectionCriteria,
    results::GetMoreResult,
    ClientSession,
    Namespace,
};

use super::{handle_response_sync, OperationResponse};

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
    type Command = Document;

    const NAME: &'static str = "getMore";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.cursor_id,
            "collection": self.ns.coll.clone(),
        };

        if let Some(batch_size) = self.batch_size {
            if batch_size > std::i32::MAX as u32 {
                return Err(ErrorKind::InvalidArgument {
                    message: "The batch size must fit into a signed 32-bit integer".to_string(),
                }
                .into());
            } else if batch_size != 0 {
                body.insert("batchSize", batch_size);
            }
        }

        if let Some(ref max_time) = self.max_time {
            body.insert(
                "maxTimeMS",
                max_time.as_millis().try_into().unwrap_or(i32::MAX),
            );
        }

        if let Some(ref comment) = self.comment {
            body.insert("comment", comment);
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
        _session: Option<&mut ClientSession>,
    ) -> OperationResponse<'static, Self::O> {
        handle_response_sync! {{
            let response: GetMoreResponseBody = response.body()?;

            Ok(GetMoreResult {
                batch: response.cursor.next_batch,
                exhausted: response.cursor.id == 0,
                post_batch_resume_token: ResumeToken::from_raw(
                    response.cursor.post_batch_resume_token,
                ),
                id: response.cursor.id,
                ns: Namespace::from_str(response.cursor.ns.as_str()).unwrap(),
            })
        }}
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
