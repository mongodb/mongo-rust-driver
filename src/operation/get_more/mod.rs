#[cfg(test)]
mod test;

use std::{collections::VecDeque, marker::PhantomData, time::Duration};

use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    bson::doc,
    cmap::{Command, StreamDescription},
    cursor::CursorInformation,
    error::{ErrorKind, Result},
    operation::Operation,
    options::SelectionCriteria,
    results::GetMoreResult,
    Namespace,
};

use super::CommandResponse;

#[derive(Debug)]
pub(crate) struct GetMore<T> {
    ns: Namespace,
    cursor_id: i64,
    selection_criteria: SelectionCriteria,
    batch_size: Option<u32>,
    max_time: Option<Duration>,
    _phantom: PhantomData<T>,
}

impl<T> GetMore<T> {
    pub(crate) fn new(info: CursorInformation) -> Self {
        Self {
            ns: info.ns,
            cursor_id: info.id,
            selection_criteria: SelectionCriteria::from_address(info.address),
            batch_size: info.batch_size,
            max_time: info.max_time,
            _phantom: Default::default(),
        }
    }
}

impl<T: DeserializeOwned> Operation for GetMore<T> {
    type O = GetMoreResult<T>;
    type Response = CommandResponse<GetMoreResponseBody<T>>;

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
            body.insert("maxTimeMS", max_time.as_millis() as i32);
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: GetMoreResponseBody<T>,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        Ok(GetMoreResult {
            batch: response.cursor.next_batch,
            exhausted: response.cursor.id == 0,
        })
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(&self.selection_criteria)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetMoreResponseBody<T> {
    cursor: NextBatchBody<T>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NextBatchBody<T> {
    id: i64,
    next_batch: VecDeque<T>,
}
