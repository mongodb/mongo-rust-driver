#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::Namespace,
    collation::Collation,
    error::{convert_bulk_errors, Result},
    operation::{
        append_options,
        remove_empty_write_concern,
        OperationWithDefaults,
        Retryability,
        WriteResponseBody,
    },
    options::{DeleteOptions, Hint, WriteConcern},
    results::DeleteResult,
};

#[derive(Debug)]
pub(crate) struct Delete {
    ns: Namespace,
    filter: Document,
    limit: u32,
    options: Option<DeleteOptions>,
    collation: Option<Collation>,
    hint: Option<Hint>,
}

impl Delete {
    #[cfg(test)]
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
        mut options: Option<DeleteOptions>,
    ) -> Self {
        Self {
            ns,
            filter,
            limit: limit.unwrap_or(0), // 0 = no limit
            collation: options.as_mut().and_then(|opts| opts.collation.take()),
            hint: options.as_mut().and_then(|opts| opts.hint.take()),
            options,
        }
    }
}

impl OperationWithDefaults for Delete {
    type O = DeleteResult;
    type Command = Document;

    const NAME: &'static str = "delete";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut delete = doc! {
            "q": self.filter.clone(),
            "limit": self.limit,
        };

        if let Some(ref collation) = self.collation {
            delete.insert("collation", bson::to_bson(&collation)?);
        }

        if let Some(ref hint) = self.hint {
            delete.insert("hint", bson::to_bson(&hint)?);
        }

        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
            "deletes": [delete],
            "ordered": true, // command monitoring tests expect this (SPEC-1130)
        };

        remove_empty_write_concern!(self.options);
        append_options(&mut body, self.options.as_ref())?;

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
    ) -> Result<Self::O> {
        let response: WriteResponseBody = response.body()?;
        response.validate().map_err(convert_bulk_errors)?;

        Ok(DeleteResult {
            deleted_count: response.n,
        })
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }

    fn retryability(&self) -> Retryability {
        if self.limit == 1 {
            Retryability::Write
        } else {
            Retryability::None
        }
    }
}
