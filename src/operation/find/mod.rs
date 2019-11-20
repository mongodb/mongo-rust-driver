#[cfg(test)]
mod test;

use bson::{bson, doc, Document};

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, CursorBody, Operation},
    options::{CursorType, FindOptions, SelectionCriteria},
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Find {
    ns: Namespace,
    filter: Option<Document>,
    options: Option<FindOptions>,
}

impl Find {
    #[allow(dead_code)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            None,
            None,
        )
    }

    pub(crate) fn new(
        ns: Namespace,
        filter: Option<Document>,
        options: Option<FindOptions>,
    ) -> Self {
        Self {
            ns,
            filter,
            options,
        }
    }
}

impl Operation for Find {
    type O = CursorSpecification;
    const NAME: &'static str = "find";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };
        append_options(&mut body, self.options.as_ref())?;

        if let Some(ref filter) = self.filter {
            body.insert("filter", filter.clone());
        }

        match self.cursor_type() {
            CursorType::Tailable => {
                body.insert("tailable", true);
            }
            CursorType::TailableAwait => {
                body.insert("tailable", true);
                body.insert("awaitData", true);
            }
            _ => {}
        };

        // negative limits should be interpreted as request for single batch as per crud spec.
        if self
            .options
            .as_ref()
            .and_then(|opts| opts.limit)
            .map(|limit| limit < 0)
            == Some(true)
        {
            body.insert("singleBatch", true);
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        let body: CursorBody = response.body()?;

        let max_await_time = match self.cursor_type() {
            CursorType::TailableAwait => self.options.as_ref().and_then(|opts| opts.max_await_time),
            _ => None,
        };

        Ok(CursorSpecification {
            ns: self.ns.clone(),
            address: response.source_address().clone(),
            id: body.cursor.id,
            batch_size: self.options.as_ref().and_then(|opts| opts.batch_size),
            max_time: max_await_time,
            buffer: body.cursor.first_batch,
        })
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.options
            .as_ref()
            .and_then(|opts| opts.selection_criteria.as_ref())
    }
}

impl Find {
    fn cursor_type(&self) -> CursorType {
        self.options
            .as_ref()
            .and_then(|opts| opts.cursor_type)
            .unwrap_or(CursorType::NonTailable)
    }
}
