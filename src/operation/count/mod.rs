#[cfg(test)]
mod test;

use serde::Deserialize;

use crate::{
    bson::doc,
    cmap::{Command, CommandResponse, StreamDescription},
    coll::{options::EstimatedDocumentCountOptions, Namespace},
    error::{Error, ErrorKind, Result},
    operation::{append_options, CursorBody, Operation, Retryability},
    selection_criteria::SelectionCriteria,
};

const SERVER_4_9_0_WIRE_VERSION: i32 = 12;

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
    type O = i64;
    const NAME: &'static str = "count";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = match description.max_wire_version {
            Some(v) if v >= SERVER_4_9_0_WIRE_VERSION => {
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

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: CommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        let response_body: ResponseBody = match description.max_wire_version {
            Some(v) if v >= SERVER_4_9_0_WIRE_VERSION => {
                let CursorBody { mut cursor } = response.body()?;

                cursor
                    .first_batch
                    .pop_front()
                    .and_then(|doc| bson::from_document(doc).ok())
                    .ok_or_else(|| {
                        Error::from(ErrorKind::ResponseError {
                            message: "invalid server response to count operation".into(),
                        })
                    })?
            }
            _ => response.body()?,
        };

        Ok(response_body.n)
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

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}

#[derive(Debug, Deserialize)]
struct ResponseBody {
    n: i64,
}
