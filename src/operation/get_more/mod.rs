#[cfg(test)]
mod test;

use std::{collections::VecDeque, time::Duration};

use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    cmap::{Command, CommandResponse, StreamDescription},
    cursor::CursorInformation,
    error::{ErrorKind, Result},
    operation::Operation,
    options::SelectionCriteria,
    results::GetMoreResult,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct GetMore {
    ns: Namespace,
    cursor_id: i64,
    selection_criteria: SelectionCriteria,
    batch_size: Option<u32>,
    max_time: Option<Duration>,
    request_exhaust: bool,
}

impl GetMore {
    pub(crate) fn new(info: CursorInformation, request_exhaust: bool) -> Self {
        Self {
            ns: info.ns,
            cursor_id: info.id,
            selection_criteria: SelectionCriteria::from_address(info.address),
            batch_size: info.batch_size,
            max_time: info.max_time,
            request_exhaust,
        }
    }

    pub(crate) fn handle_response(response: CommandResponse) -> Result<GetMoreResult> {
        let body: GetMoreResponseBody = response.body()?;
        Ok(GetMoreResult {
            batch: body.cursor.next_batch,
            exhausted: body.cursor.id == 0,
        })
    }
}

impl Operation for GetMore {
    type O = GetMoreResult;
    const NAME: &'static str = "getMore";

    fn build(&self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.cursor_id,
            "collection": self.ns.coll.clone(),
        };

        if let Some(batch_size) = self.batch_size {
            if batch_size > std::i32::MAX as u32 {
                return Err(ErrorKind::ArgumentError {
                    message: "The batch size must fit into a signed 32-bit integer".to_string(),
                }
                .into());
            } else if batch_size != 0 {
                body.insert("batchSize", batch_size);
            }
        }

        if let Some(ref max_time) = self.max_time {
            body.insert("maxTimeMS", max_time.as_millis() as i32);
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        GetMore::handle_response(response)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(&self.selection_criteria)
    }

    fn request_exhaust(&self) -> bool {
        self.request_exhaust
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetMoreResponseBody {
    pub(crate) cursor: NextBatchBody,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NextBatchBody {
    id: i64,
    pub(crate) next_batch: VecDeque<Document>,
}
