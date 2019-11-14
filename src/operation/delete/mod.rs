#[cfg(test)]
mod test;

use bson::{bson, doc, Document};

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    coll::Namespace,
    error::{convert_bulk_errors, Result},
    operation::{append_options, Operation, WriteResponseBody},
    options::DeleteOptions,
    results::DeleteResult,
};

#[derive(Debug)]
pub(crate) struct Delete {
    ns: Namespace,
    filter: Document,
    limit: u32,
    options: Option<DeleteOptions>,
}

impl Delete {
    #[allow(dead_code)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            Document::new(),
            None,
            None,
        )
    }

    pub(crate) fn new(
        ns: Namespace,
        filter: Document,
        limit: Option<u32>,
        options: Option<DeleteOptions>,
    ) -> Self {
        Self {
            ns,
            filter,
            limit: limit.unwrap_or(0), // 0 = no limit
            options,
        }
    }
}

impl Operation for Delete {
    type O = DeleteResult;
    const NAME: &'static str = "delete";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
            "deletes": [
                {
                    "q": self.filter.clone(),
                    "limit": self.limit,
                }
            ]
        };
        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        let body: WriteResponseBody = response.body()?;
        body.validate().map_err(convert_bulk_errors)?;

        Ok(DeleteResult {
            deleted_count: body.n,
        })
    }
}
