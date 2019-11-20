#[cfg(test)]
mod test;

use std::time::Duration;

use bson::{bson, doc, Document};
use serde::Deserialize;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    error::Result,
    operation::Operation,
    options::{SelectionCriteria, StreamAddress},
    results::GetMoreResult,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct GetMore {
    ns: Namespace,
    cursor_id: i64,
    selection_criteria: SelectionCriteria,
    batch_size: Option<i32>,
    max_time: Option<Duration>,
}

impl GetMore {
    #[allow(dead_code)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            0,
            StreamAddress {
                hostname: String::new(),
                port: None,
            },
            None,
            None,
        )
    }

    pub(crate) fn new(
        ns: Namespace,
        cursor_id: i64,
        address: StreamAddress,
        batch_size: Option<i32>,
        max_time: Option<Duration>,
    ) -> Self {
        Self {
            ns,
            cursor_id,
            selection_criteria: SelectionCriteria::from_address(address),
            batch_size,
            max_time,
        }
    }
}

impl Operation for GetMore {
    type O = GetMoreResult;
    const NAME: &'static str = "getMore";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.cursor_id,
            "collection": self.ns.coll.clone(),
        };

        if let Some(batch_size) = self.batch_size {
            if batch_size != 0 {
                body.insert("batchSize", batch_size.abs());
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
        let body: GetMoreResponseBody = response.body()?;
        Ok(GetMoreResult {
            batch: body.cursor.next_batch,
            exhausted: body.cursor.id == 0,
        })
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(&self.selection_criteria)
    }
}

#[derive(Debug, Deserialize)]
struct GetMoreResponseBody {
    cursor: NextBatchBody,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NextBatchBody {
    id: i64,
    next_batch: Vec<Document>,
}
