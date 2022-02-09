#[cfg(test)]
mod test;

use bson::Document;
use serde::Deserialize;

use crate::{
    bson::doc,
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::{options::EstimatedDocumentCountOptions, Namespace},
    error::{Error, ErrorKind, Result},
    operation::{append_options, CursorBody, Operation, Retryability},
    selection_criteria::SelectionCriteria,
};

use super::SERVER_4_9_0_WIRE_VERSION;

pub(crate) struct Count {
    ns: Namespace,
    options: Option<EstimatedDocumentCountOptions>,
}

impl Count {
    pub fn new(ns: Namespace, options: Option<EstimatedDocumentCountOptions>) -> Self {
        Count { ns, options }
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Count {
            ns: Namespace {
                db: String::new(),
                coll: String::new(),
            },
            options: None,
        }
    }
}

impl Operation for Count {
    type O = u64;
    type Command = Document;

    const NAME: &'static str = "count";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        let mut name = Self::NAME.to_string();
        let mut body = match description.max_wire_version {
            Some(v) if v >= SERVER_4_9_0_WIRE_VERSION => {
                name = "aggregate".to_string();
                doc! {
                    "aggregate": self.ns.coll.clone(),
                    "pipeline": [
                        {
                            "$collStats": { "count": {} },
                        },
                        {
                            "$group": {
                                "_id": 1,
                                "n": { "$sum": "$count" },
                            },
                        },
                    ],
                    "cursor": {},
                }
            }
            _ => {
                doc! {
                    Self::NAME: self.ns.coll.clone(),
                }
            }
        };

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new_read(
            name,
            self.ns.db.clone(),
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: Response = response.body()?;

        // let response_body: ResponseBody = match (description.max_wire_version, response) {
        //     (Some(v), Response::Aggregate(mut cursor_body)) if v >= SERVER_4_9_0_WIRE_VERSION =>
        // {         cursor_body.cursor.first_batch.pop_front().ok_or_else(|| {
        //             Error::from(ErrorKind::InvalidResponse {
        //                 message: "invalid server response to count operation".into(),
        //             })
        //         })?
        //     }
        //     (_, Response::Count(body)) => body,
        //     _ => {
        //         return Err(ErrorKind::InvalidResponse {
        //             message: "response from server did not match count command".to_string(),
        //         }
        //         .into())
        //     }
        // };
        todo!()

        // Ok(response_body.n)
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        if error.is_ns_not_found() {
            Ok(0)
        } else {
            Err(error)
        }
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        if let Some(ref options) = self.options {
            return options.selection_criteria.as_ref();
        }
        None
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        true
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Response {
    Aggregate(Box<CursorBody>),
    Count(ResponseBody),
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResponseBody {
    n: u64,
}
